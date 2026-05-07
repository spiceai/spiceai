/*
Copyright 2026 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

//! Elasticsearch [`SearchIndex`] and [`VectorIndex`] implementations.
//!
//! This module bridges Elasticsearch's native kNN and full-text search capabilities
//! with the Spice search pipeline, enabling hybrid search via `vector_search`,
//! `text_search`, and `rrf` UDTFs.

mod write;

use std::any::Any;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use arrow::array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::TableProvider;
use datafusion::datasource::DefaultTableSource;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::LogicalPlan;
use datafusion_expr::LogicalPlanBuilder;
use elasticsearch::Elasticsearch;
use futures::future::try_join_all;
use llms::embeddings::Embed;
use runtime_datafusion_index::Index;
use tokio::sync::Mutex;

use crate::SEARCH_SCORE_COLUMN_NAME;
use crate::index::chunking::{CHUNKED_INDEX_CHUNK_KEY, ChunkedSearchIndex};
use crate::index::{SearchIndex, VectorIndex, embedding_col};
use crate::metadata::MetadataColumns;
use data_components::elasticsearch::search_table::{
    ElasticsearchKnnTable, ElasticsearchTextSearchTable, QueryEmbedder,
};

/// Adapter that implements [`QueryEmbedder`] using an [`Embed`] model.
#[derive(Debug)]
struct EmbedQueryAdapter(Arc<dyn Embed>);

#[async_trait]
impl QueryEmbedder for EmbedQueryAdapter {
    async fn embed_query(&self, query: &str) -> Result<Vec<f32>, DataFusionError> {
        let mut vectors = self
            .0
            .embed(llms::embeddings::EmbeddingInput::String(query.to_string()))
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        vectors.pop().ok_or_else(|| {
            DataFusionError::Execution("No embedding vector computed for query".to_string())
        })
    }
}

/// Elasticsearch-backed vector search index.
///
/// Stores embeddings in an Elasticsearch `dense_vector` field and uses kNN search
/// for vector similarity queries. Also supports full-text search on text fields.
#[derive(Debug, Clone)]
pub struct ElasticsearchIndex {
    /// The Elasticsearch client.
    pub client: Arc<dyn Elasticsearch>,

    /// The Elasticsearch index name.
    pub es_index: String,

    /// Name of the column in the underlying table that is embedded.
    pub embedded_column: String,

    /// The Elasticsearch field name for the `dense_vector` field.
    pub vector_field: String,

    /// Text fields for full-text search.
    pub text_fields: Vec<String>,

    /// Primary key fields that link search results back to the underlying table.
    pub primary_key: Vec<Field>,

    /// Embedding model for computing query vectors.
    pub compute_query: Arc<dyn Embed>,

    /// Dimensionality of the embedding vectors.
    pub dims: i32,

    /// Full source schema for extracting fields from Elasticsearch results.
    pub source_schema: SchemaRef,

    /// Filterable / non-filterable metadata columns as declared by the user.
    /// These influence the ES mapping (index: true vs index: false) but are
    /// otherwise written into `_source` like any other source column.
    pub metadata_columns: MetadataColumns,

    /// Maximum number of rows to send per Elasticsearch `_bulk` request.
    pub batch_write_rows: usize,

    /// External index maintenance to run around full/append writes.
    pub write_maintenance: Arc<ElasticsearchIndexWriteMaintenance>,
}

/// Optional Elasticsearch maintenance to run around full/append table-sink writes.
#[derive(Debug, Clone, Default)]
pub struct ElasticsearchIndexWriteOptions {
    /// Temporary `index.refresh_interval` to apply before a bounded write.
    pub refresh_interval_during_write: Option<String>,

    /// Target segment count for `_forcemerge` after a bounded write.
    pub force_merge_segments: Option<u32>,
}

/// Stateful helper shared by Elasticsearch-backed indexes that write to the same ES index.
#[derive(Debug)]
pub struct ElasticsearchIndexWriteMaintenance {
    options: ElasticsearchIndexWriteOptions,
    write_cycle_active: AtomicBool,
    refresh_interval_overridden: AtomicBool,
    previous_refresh_interval: Mutex<PreviousRefreshInterval>,
}

#[derive(Debug, Clone, Default)]
enum PreviousRefreshInterval {
    #[default]
    NotCaptured,
    Unset,
    Value(String),
}

impl Default for ElasticsearchIndexWriteMaintenance {
    fn default() -> Self {
        Self::new(ElasticsearchIndexWriteOptions::default())
    }
}

impl ElasticsearchIndexWriteMaintenance {
    #[must_use]
    pub fn new(options: ElasticsearchIndexWriteOptions) -> Self {
        Self {
            options,
            write_cycle_active: AtomicBool::new(false),
            refresh_interval_overridden: AtomicBool::new(false),
            previous_refresh_interval: Mutex::new(PreviousRefreshInterval::default()),
        }
    }

    async fn on_write_start(
        &self,
        client: &dyn Elasticsearch,
        es_index: &str,
    ) -> Result<(), DataFusionError> {
        let first_start = !self.write_cycle_active.swap(true, Ordering::AcqRel);
        if !first_start {
            return Ok(());
        }

        let Some(refresh_interval) = self.options.refresh_interval_during_write.as_deref() else {
            return Ok(());
        };

        if self
            .refresh_interval_overridden
            .swap(true, Ordering::AcqRel)
        {
            return Ok(());
        }

        let previous = client
            .get_index_refresh_interval(es_index)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let body = serde_json::json!({ "index": { "refresh_interval": refresh_interval } });

        if let Err(e) = client.put_index_settings(es_index, &body).await {
            self.refresh_interval_overridden
                .store(false, Ordering::Release);
            return Err(DataFusionError::External(Box::new(e)));
        }

        *self.previous_refresh_interval.lock().await = match previous {
            Some(refresh_interval) => PreviousRefreshInterval::Value(refresh_interval),
            None => PreviousRefreshInterval::Unset,
        };
        tracing::debug!(
            "Set Elasticsearch index '{es_index}' refresh_interval to '{refresh_interval}' for bulk write."
        );
        Ok(())
    }

    async fn on_write_failed(
        &self,
        client: &dyn Elasticsearch,
        es_index: &str,
    ) -> Result<(), DataFusionError> {
        if !self.write_cycle_active.swap(false, Ordering::AcqRel) {
            return Ok(());
        }
        self.restore_refresh_interval(client, es_index).await?;
        Ok(())
    }

    async fn on_write_complete(
        &self,
        client: &dyn Elasticsearch,
        es_index: &str,
    ) -> Result<(), DataFusionError> {
        if !self.write_cycle_active.swap(false, Ordering::AcqRel) {
            return Ok(());
        }

        let restored_refresh_interval = self.restore_refresh_interval(client, es_index).await?;
        if restored_refresh_interval || self.options.force_merge_segments.is_some() {
            client
                .refresh_index(es_index)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
        }

        if let Some(max_num_segments) = self.options.force_merge_segments {
            client
                .force_merge(es_index, max_num_segments)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            tracing::debug!(
                "Force-merged Elasticsearch index '{es_index}' to max_num_segments={max_num_segments}."
            );
        }

        Ok(())
    }

    async fn restore_refresh_interval(
        &self,
        client: &dyn Elasticsearch,
        es_index: &str,
    ) -> Result<bool, DataFusionError> {
        if !self.refresh_interval_overridden.load(Ordering::Acquire) {
            return Ok(false);
        }

        let previous = self.previous_refresh_interval.lock().await.clone();
        let refresh_interval = match previous {
            PreviousRefreshInterval::NotCaptured => {
                self.refresh_interval_overridden
                    .store(false, Ordering::Release);
                return Ok(false);
            }
            PreviousRefreshInterval::Unset => serde_json::Value::Null,
            PreviousRefreshInterval::Value(refresh_interval) => {
                serde_json::Value::String(refresh_interval)
            }
        };
        let body = serde_json::json!({ "index": { "refresh_interval": refresh_interval } });
        client
            .put_index_settings(es_index, &body)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        *self.previous_refresh_interval.lock().await = PreviousRefreshInterval::NotCaptured;
        self.refresh_interval_overridden
            .store(false, Ordering::Release);
        tracing::debug!("Restored Elasticsearch index '{es_index}' refresh_interval after write.");
        Ok(true)
    }
}

#[async_trait]
impl SearchIndex for ElasticsearchIndex {
    fn search_column(&self) -> String {
        self.embedded_column.clone()
    }

    fn primary_fields(&self) -> Vec<Field> {
        self.primary_key.clone()
    }

    async fn write(
        &self,
        record: RecordBatch,
    ) -> Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>> {
        write::write(self, record)
            .await
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })
    }

    fn query_table_provider(&self, query: &str) -> Result<Arc<LogicalPlan>, DataFusionError> {
        let schema = self.query_result_schema();
        let table: Arc<dyn TableProvider> = Arc::new(ElasticsearchKnnTable {
            client: Arc::clone(&self.client),
            index: self.es_index.clone(),
            vector_field: self.vector_field.clone(),
            query_vector: vec![],
            k: 10,
            schema: Arc::clone(&schema),
            source_schema: Arc::clone(&self.source_schema),
            query_text: Some(query.to_string()),
            embedder: Some(Arc::new(EmbedQueryAdapter(Arc::clone(&self.compute_query)))),
        });

        Ok(
            LogicalPlanBuilder::scan("tbl", Arc::new(DefaultTableSource::new(table)), None)?
                .build()?
                .into(),
        )
    }

    fn as_vector_index(self: Arc<Self>) -> Option<Arc<dyn VectorIndex>> {
        Some(Arc::clone(&self) as Arc<dyn VectorIndex>)
    }
}

impl ElasticsearchIndex {
    /// Wrap a [`TableProvider`] so that its schema matches what the Elasticsearch HTTP client
    /// actually returns, by casting columns whose types differ between the accelerated store
    /// (e.g. DuckDB uses `LargeUtf8`) and the ES JSON deserializer (always `Utf8`).
    ///
    /// This is used when building the `SearchQueryProvider` join so that join key types
    /// are consistent between the kNN exec (left) and the base table scan (right).
    ///
    /// # Errors
    ///
    /// Returns a `DataFusionError` if the `ViewTable` plan cannot be built.
    pub fn normalize_source_table(
        &self,
        table: Arc<dyn TableProvider>,
    ) -> Result<Arc<dyn TableProvider>, DataFusionError> {
        use datafusion::datasource::ViewTable;
        use datafusion::prelude::{Expr, cast, col};

        let raw_schema = table.schema();
        let normalized_schema = Arc::clone(&self.source_schema);

        // If schemas already match, no wrapping needed.
        if raw_schema == normalized_schema {
            return Ok(table);
        }

        // Build a projection that casts columns whose types differ.
        let projections: Vec<Expr> = raw_schema
            .fields()
            .iter()
            .map(|f| {
                let raw_type = f.data_type();
                // Find the normalized type from source_schema (same field name).
                let target_type = normalized_schema
                    .field_with_name(f.name())
                    .map_or_else(|_| raw_type.clone(), |nf| nf.data_type().clone());

                if &target_type == raw_type {
                    col(f.name())
                } else {
                    cast(col(f.name()), target_type).alias(f.name())
                }
            })
            .collect();

        let plan =
            LogicalPlanBuilder::scan("base", Arc::new(DefaultTableSource::new(table)), None)?
                .project(projections)?
                .build()?;

        Ok(Arc::new(ViewTable::new(plan, None)))
    }
}

impl VectorIndex for ElasticsearchIndex {
    fn list_table_provider(&self) -> Result<LogicalPlan, DataFusionError> {
        // Elasticsearch doesn't support listing all vectors efficiently.
        // Return an empty plan with the correct schema.
        let schema = self.list_result_schema();
        let empty_batch = RecordBatch::new_empty(Arc::clone(&schema));
        let mem_table =
            datafusion::catalog::MemTable::try_new(Arc::clone(&schema), vec![vec![empty_batch]])?;

        LogicalPlanBuilder::scan(
            "tbl",
            Arc::new(DefaultTableSource::new(Arc::new(mem_table))),
            None,
        )?
        .build()
    }

    fn dimension(&self) -> i32 {
        self.dims
    }
}

#[async_trait]
impl Index for ElasticsearchIndex {
    fn name(&self) -> &'static str {
        "elasticsearch_index"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn required_columns(&self) -> Vec<String> {
        let mut cols: Vec<_> = self.primary_key.iter().map(|f| f.name().clone()).collect();
        cols.push(self.embedded_column.clone());
        for t in &self.text_fields {
            if !cols.contains(t) {
                cols.push(t.clone());
            }
        }

        // Include user-declared metadata columns (`vectors: filterable | non-filterable`)
        // so they survive projection pruning and make it into the ES `_source` on
        // refresh/CDC indexing. Without this, the mapping hints would be ineffective
        // and expected metadata would be dropped. The derived embedding column is
        // excluded because the write path computes it itself.
        let derived_embedding_column = embedding_col(&self.embedded_column);
        for name in self.metadata_columns.all_names() {
            if name == derived_embedding_column {
                continue;
            }
            if !cols.contains(&name) {
                cols.push(name);
            }
        }

        cols
    }

    async fn compute_index(
        &self,
        batches: Vec<RecordBatch>,
    ) -> Result<Vec<RecordBatch>, DataFusionError> {
        let futs = batches.into_iter().map(|rb| async move {
            write::write(self, rb)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))
        });
        try_join_all(futs).await
    }

    async fn on_write_start(&self) -> Result<(), DataFusionError> {
        self.write_maintenance
            .on_write_start(self.client.as_ref(), &self.es_index)
            .await
    }

    async fn on_write_failed(&self) -> Result<(), DataFusionError> {
        self.write_maintenance
            .on_write_failed(self.client.as_ref(), &self.es_index)
            .await
    }

    async fn on_write_complete(&self) -> Result<(), DataFusionError> {
        self.write_maintenance
            .on_write_complete(self.client.as_ref(), &self.es_index)
            .await
    }
}

impl ElasticsearchIndex {
    /// Schema for `query_table_provider` results: primary keys + embedding + `_score`.
    ///
    /// `_spice.chunk_id` is excluded even when present in `self.primary_key` (added by
    /// [`ChunkedSearchIndex::augment_primary_key`]). It is an internal ordering key used
    /// only inside `list_table_provider`'s aggregation — it is never stored in ES `_source`
    /// as a retrievable field, so `knn_hits_to_batch` would fill it with nulls and violate
    /// the non-nullable declaration ("Column '_spice.chunk_id' is declared as non-nullable
    /// but contains null values").
    fn query_result_schema(&self) -> SchemaRef {
        let mut fields: Vec<Field> = self
            .primary_key
            .iter()
            .filter(|f| f.name() != CHUNKED_INDEX_CHUNK_KEY)
            .cloned()
            .collect();
        fields.push(Field::new(
            embedding_col(&self.embedded_column),
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, false)),
                self.dims,
            ),
            true,
        ));
        fields.push(Field::new(
            SEARCH_SCORE_COLUMN_NAME,
            DataType::Float64,
            true,
        ));
        Arc::new(Schema::new(fields))
    }

    /// Schema for `list_table_provider` results: primary keys + embedding (+ offset when chunked).
    ///
    /// The offset column (`{embedded_column}_offset`) is only included when this index is
    /// wrapped by [`ChunkedSearchIndex`] / [`super::chunking::ChunkedVectorIndex`] (detected
    /// by the presence of [`CHUNKED_INDEX_CHUNK_KEY`] in `self.primary_key`). The chunking
    /// write path always emits the offset column into the inner-index batch, so Elasticsearch
    /// stores it even though the schema was previously not advertising it—causing
    /// "Schema error: No field named content_offset". For non-chunked indexes the offset
    /// is never written, so advertising it here would produce null values and violate the
    /// non-nullable declaration.
    fn list_result_schema(&self) -> SchemaRef {
        let mut fields: Vec<Field> = self.primary_key.clone();
        fields.push(Field::new(
            embedding_col(&self.embedded_column),
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, false)),
                self.dims,
            ),
            true,
        ));
        if self.is_chunked() {
            fields.push(Field::new(
                ChunkedSearchIndex::chunking_offset_col(&self.embedded_column),
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Int32, false)), 2),
                false,
            ));
        }
        Arc::new(Schema::new(fields))
    }

    /// Whether this index is being used as the inner index of a [`ChunkedSearchIndex`],
    /// detected by the presence of [`CHUNKED_INDEX_CHUNK_KEY`] in `self.primary_key`
    /// (added by [`ChunkedSearchIndex::augment_primary_key`]).
    fn is_chunked(&self) -> bool {
        self.primary_key
            .iter()
            .any(|f| f.name() == CHUNKED_INDEX_CHUNK_KEY)
    }
}

/// Elasticsearch-backed full-text search index.
///
/// Uses Elasticsearch's native BM25 full-text search capabilities, querying live
/// at search time via the `text_search()` UDTF and `/v1/search` API.
#[derive(Debug, Clone)]
pub struct ElasticsearchTextIndex {
    /// The Elasticsearch client.
    pub client: Arc<dyn Elasticsearch>,

    /// The Elasticsearch index name.
    pub es_index: String,

    /// Name of the text column being searched.
    pub search_column_name: String,

    /// The Elasticsearch field names to search.
    pub search_fields: Vec<String>,

    /// Primary key fields.
    pub primary_key: Vec<Field>,

    /// Full source schema for extracting fields.
    pub source_schema: SchemaRef,

    /// Maximum number of rows to send per Elasticsearch `_bulk` request.
    pub batch_write_rows: usize,

    /// External index maintenance to run around full/append writes.
    pub write_maintenance: Arc<ElasticsearchIndexWriteMaintenance>,
}

impl ElasticsearchTextIndex {
    /// Wrap a [`TableProvider`] so that its schema matches the normalized source schema
    /// used by the ES text index: type-cast where needed and mark all fields nullable.
    /// ES text search results never include dense_vector columns, so the base table
    /// scan must allow nulls in those fields or Arrow will reject the result.
    pub fn normalize_source_table(
        &self,
        table: Arc<dyn TableProvider>,
    ) -> Result<Arc<dyn TableProvider>, DataFusionError> {
        use datafusion::datasource::ViewTable;
        use datafusion::prelude::col;

        let raw_schema = table.schema();

        // Project out list/embedding columns — ES text search does not return
        // them and SearchQueryProvider would include them in its advertised
        // schema from the base table, causing Arrow type mismatches at scan time.
        let projections: Vec<_> = raw_schema
            .fields()
            .iter()
            .filter(|f| {
                !matches!(
                    f.data_type(),
                    DataType::FixedSizeList(_, _) | DataType::LargeList(_) | DataType::List(_)
                )
            })
            .map(|f| col(f.name()))
            .collect();

        let plan =
            LogicalPlanBuilder::scan("base", Arc::new(DefaultTableSource::new(table)), None)?
                .project(projections)?
                .build()?;

        Ok(Arc::new(ViewTable::new(plan, None)))
    }
}

#[async_trait]
impl SearchIndex for ElasticsearchTextIndex {
    fn search_column(&self) -> String {
        self.search_column_name.clone()
    }

    fn primary_fields(&self) -> Vec<Field> {
        self.primary_key.clone()
    }

    async fn write(
        &self,
        record: RecordBatch,
    ) -> Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>> {
        write::write_text(
            self.client.as_ref(),
            &self.es_index,
            &self.primary_key,
            self.batch_write_rows,
            &record,
        )
        .await
        .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })
    }

    fn query_table_provider(&self, query: &str) -> Result<Arc<LogicalPlan>, DataFusionError> {
        let mut result_fields: Vec<Field> = self.primary_key.clone();
        result_fields.push(Field::new(
            SEARCH_SCORE_COLUMN_NAME,
            DataType::Float64,
            true,
        ));
        let schema = Arc::new(Schema::new(result_fields));

        let table: Arc<dyn TableProvider> = Arc::new(ElasticsearchTextSearchTable {
            client: Arc::clone(&self.client),
            index: self.es_index.clone(),
            search_fields: self.search_fields.clone(),
            query_text: query.to_string(),
            limit: 10_000,
            schema: Arc::clone(&schema),
            source_schema: Arc::clone(&self.source_schema),
        });

        Ok(
            LogicalPlanBuilder::scan("tbl", Arc::new(DefaultTableSource::new(table)), None)?
                .build()?
                .into(),
        )
    }
}

#[async_trait]
impl Index for ElasticsearchTextIndex {
    fn name(&self) -> &'static str {
        "elasticsearch_text_index"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn required_columns(&self) -> Vec<String> {
        let mut cols: Vec<_> = self.primary_key.iter().map(|f| f.name().clone()).collect();
        for field in &self.search_fields {
            if !cols.contains(field) {
                cols.push(field.clone());
            }
        }
        cols
    }

    async fn compute_index(
        &self,
        batches: Vec<RecordBatch>,
    ) -> Result<Vec<RecordBatch>, DataFusionError> {
        let futs = batches
            .into_iter()
            .map(|rb| async move { self.write(rb).await.map_err(DataFusionError::External) });
        try_join_all(futs).await
    }

    async fn on_write_start(&self) -> Result<(), DataFusionError> {
        self.write_maintenance
            .on_write_start(self.client.as_ref(), &self.es_index)
            .await
    }

    async fn on_write_failed(&self) -> Result<(), DataFusionError> {
        self.write_maintenance
            .on_write_failed(self.client.as_ref(), &self.es_index)
            .await
    }

    async fn on_write_complete(&self) -> Result<(), DataFusionError> {
        self.write_maintenance
            .on_write_complete(self.client.as_ref(), &self.es_index)
            .await
    }
}

#[cfg(test)]
mod write_maintenance_tests {
    use super::*;

    use std::sync::atomic::AtomicU32;

    use elasticsearch::{
        Elasticsearch as ElasticsearchTrait, MappingResponse, SearchRequest, SearchResponse,
    };

    /// Minimal mock that records calls to the maintenance-related Elasticsearch methods.
    /// All other methods are unused by `ElasticsearchIndexWriteMaintenance` and will panic.
    #[derive(Debug, Default)]
    struct MockElasticsearch {
        refresh_interval: std::sync::Mutex<Option<String>>,
        get_refresh_interval_calls: AtomicU32,
        put_settings_calls: AtomicU32,
        refresh_index_calls: AtomicU32,
        force_merge_calls: AtomicU32,
        last_force_merge_segments: std::sync::Mutex<Option<u32>>,
    }

    #[async_trait::async_trait]
    impl ElasticsearchTrait for MockElasticsearch {
        async fn get_index_refresh_interval(
            &self,
            _index: &str,
        ) -> elasticsearch::Result<Option<String>> {
            self.get_refresh_interval_calls
                .fetch_add(1, Ordering::Relaxed);
            Ok(self.refresh_interval.lock().expect("lock poisoned").clone())
        }

        async fn put_index_settings(
            &self,
            _index: &str,
            _body: &serde_json::Value,
        ) -> elasticsearch::Result<serde_json::Value> {
            self.put_settings_calls.fetch_add(1, Ordering::Relaxed);
            Ok(serde_json::json!({}))
        }

        async fn refresh_index(&self, _index: &str) -> elasticsearch::Result<serde_json::Value> {
            self.refresh_index_calls.fetch_add(1, Ordering::Relaxed);
            Ok(serde_json::json!({}))
        }

        async fn force_merge(
            &self,
            _index: &str,
            max_num_segments: u32,
        ) -> elasticsearch::Result<serde_json::Value> {
            self.force_merge_calls.fetch_add(1, Ordering::Relaxed);
            *self
                .last_force_merge_segments
                .lock()
                .expect("lock poisoned") = Some(max_num_segments);
            Ok(serde_json::json!({}))
        }

        // Remaining methods are unused by ElasticsearchIndexWriteMaintenance.
        async fn get_mapping(&self, _: &str) -> elasticsearch::Result<MappingResponse> {
            unimplemented!()
        }
        async fn search(
            &self,
            _: &str,
            _: &SearchRequest,
        ) -> elasticsearch::Result<SearchResponse> {
            unimplemented!()
        }
        async fn search_raw(
            &self,
            _: &str,
            _: &serde_json::Value,
        ) -> elasticsearch::Result<SearchResponse> {
            unimplemented!()
        }
        async fn open_point_in_time(&self, _: &str, _: &str) -> elasticsearch::Result<String> {
            unimplemented!()
        }
        async fn search_point_in_time(
            &self,
            _: &serde_json::Value,
        ) -> elasticsearch::Result<SearchResponse> {
            unimplemented!()
        }
        async fn close_point_in_time(&self, _: &str) -> elasticsearch::Result<()> {
            unimplemented!()
        }
        async fn index_exists(&self, _: &str) -> elasticsearch::Result<bool> {
            unimplemented!()
        }
        async fn create_index(
            &self,
            _: &str,
            _: &serde_json::Value,
        ) -> elasticsearch::Result<serde_json::Value> {
            unimplemented!()
        }
        async fn put_mapping(
            &self,
            _: &str,
            _: &serde_json::Value,
        ) -> elasticsearch::Result<serde_json::Value> {
            unimplemented!()
        }
        async fn index_document(
            &self,
            _: &str,
            _: &str,
            _: &serde_json::Value,
        ) -> elasticsearch::Result<serde_json::Value> {
            unimplemented!()
        }
        async fn bulk_index(
            &self,
            _: &str,
            _: &[(Option<String>, serde_json::Value)],
        ) -> elasticsearch::Result<serde_json::Value> {
            unimplemented!()
        }
    }

    fn make_maintenance(
        opts: ElasticsearchIndexWriteOptions,
    ) -> ElasticsearchIndexWriteMaintenance {
        ElasticsearchIndexWriteMaintenance::new(opts)
    }

    /// `on_write_start` with no options configured is a pure no-op: no ES calls.
    #[tokio::test]
    async fn write_start_no_options_is_noop() {
        let client = MockElasticsearch::default();
        let m = make_maintenance(ElasticsearchIndexWriteOptions::default());

        m.on_write_start(&client, "my-index")
            .await
            .expect("on_write_start should succeed");

        assert_eq!(client.get_refresh_interval_calls.load(Ordering::Relaxed), 0);
        assert_eq!(client.put_settings_calls.load(Ordering::Relaxed), 0);
    }

    /// Calling `on_write_start` multiple times — simulating multiple batches arriving in one
    /// refresh cycle — must only apply the `refresh_interval` override once.
    /// The second and subsequent calls must be no-ops due to `write_cycle_active`.
    /// This is the core correctness guarantee of the fix for issue #3.
    #[tokio::test]
    async fn write_start_fires_once_per_cycle_not_per_batch() {
        let client = MockElasticsearch::default();
        let m = make_maintenance(ElasticsearchIndexWriteOptions {
            refresh_interval_during_write: Some("-1".to_string()),
            force_merge_segments: None,
        });

        // Simulate three batches before on_write_complete.
        m.on_write_start(&client, "my-index")
            .await
            .expect("first start");
        m.on_write_start(&client, "my-index")
            .await
            .expect("second start — must be no-op");
        m.on_write_start(&client, "my-index")
            .await
            .expect("third start — must be no-op");

        assert_eq!(
            client.get_refresh_interval_calls.load(Ordering::Relaxed),
            1,
            "refresh_interval must be fetched exactly once per write cycle, not once per batch"
        );
        assert_eq!(
            client.put_settings_calls.load(Ordering::Relaxed),
            1,
            "refresh_interval must be overridden exactly once per write cycle, not once per batch"
        );
    }

    /// `on_write_complete` restores the refresh_interval and calls `_refresh`.
    /// `force_merge` must NOT be called when `force_merge_segments` is `None`.
    #[tokio::test]
    async fn write_complete_restores_interval_and_refreshes() {
        let client = MockElasticsearch {
            refresh_interval: std::sync::Mutex::new(Some("30s".to_string())),
            ..Default::default()
        };
        let m = make_maintenance(ElasticsearchIndexWriteOptions {
            refresh_interval_during_write: Some("-1".to_string()),
            force_merge_segments: None,
        });

        m.on_write_start(&client, "my-index").await.expect("start");
        m.on_write_complete(&client, "my-index")
            .await
            .expect("complete");

        // put_settings: once to override (set -1), once to restore (set 30s).
        assert_eq!(
            client.put_settings_calls.load(Ordering::Relaxed),
            2,
            "settings should be applied on start and restored on complete"
        );
        assert_eq!(
            client.refresh_index_calls.load(Ordering::Relaxed),
            1,
            "index should be refreshed once after write completes"
        );
        assert_eq!(
            client.force_merge_calls.load(Ordering::Relaxed),
            0,
            "force_merge must not be called when force_merge_segments is None"
        );
    }

    /// `on_write_complete` calls `force_merge` with the configured segment count.
    #[tokio::test]
    async fn write_complete_calls_force_merge_when_configured() {
        let client = MockElasticsearch::default();
        let m = make_maintenance(ElasticsearchIndexWriteOptions {
            refresh_interval_during_write: None,
            force_merge_segments: Some(1),
        });

        m.on_write_start(&client, "my-index").await.expect("start");
        m.on_write_complete(&client, "my-index")
            .await
            .expect("complete");

        assert_eq!(
            client.force_merge_calls.load(Ordering::Relaxed),
            1,
            "force_merge must be called once after write completes"
        );
        assert_eq!(
            *client
                .last_force_merge_segments
                .lock()
                .expect("lock poisoned"),
            Some(1),
            "force_merge must use the configured segment count"
        );
    }

    /// `on_write_failed` resets the write cycle so the next `on_write_start`
    /// correctly re-applies the refresh_interval override for a subsequent cycle.
    #[tokio::test]
    async fn write_failed_resets_cycle_so_next_start_reapplies_override() {
        let client = MockElasticsearch {
            refresh_interval: std::sync::Mutex::new(Some("30s".to_string())),
            ..Default::default()
        };
        let m = make_maintenance(ElasticsearchIndexWriteOptions {
            refresh_interval_during_write: Some("-1".to_string()),
            force_merge_segments: None,
        });

        // First write cycle fails.
        m.on_write_start(&client, "my-index")
            .await
            .expect("start cycle 1");
        m.on_write_failed(&client, "my-index")
            .await
            .expect("failed cycle 1");

        // Second write cycle — must re-apply the override.
        m.on_write_start(&client, "my-index")
            .await
            .expect("start cycle 2");

        // get_interval: once per cycle start → 2 total.
        assert_eq!(
            client.get_refresh_interval_calls.load(Ordering::Relaxed),
            2,
            "each write cycle must independently fetch the current refresh_interval"
        );
        // put_settings: set -1 (cycle 1) + restore on failure + set -1 (cycle 2) = 3.
        assert_eq!(
            client.put_settings_calls.load(Ordering::Relaxed),
            3,
            "settings must be applied on each start and restored on each failure"
        );
    }

    /// `on_write_complete` called without a preceding `on_write_start` must be a no-op.
    /// The `write_cycle_active` guard prevents spurious maintenance calls.
    #[tokio::test]
    async fn write_complete_without_start_is_noop() {
        let client = MockElasticsearch::default();
        let m = make_maintenance(ElasticsearchIndexWriteOptions {
            refresh_interval_during_write: Some("-1".to_string()),
            force_merge_segments: Some(1),
        });

        m.on_write_complete(&client, "my-index")
            .await
            .expect("complete without start must be no-op");

        assert_eq!(client.refresh_index_calls.load(Ordering::Relaxed), 0);
        assert_eq!(client.force_merge_calls.load(Ordering::Relaxed), 0);
        assert_eq!(client.put_settings_calls.load(Ordering::Relaxed), 0);
    }
}
