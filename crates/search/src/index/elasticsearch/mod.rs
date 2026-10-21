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

mod delete;
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
use elasticsearch::{DocumentVersion, Elasticsearch};
use futures::{StreamExt, TryStreamExt};
use llms::embeddings::Embed;
use spice_table::{GroupPruning, Index, WriteWindow};
use tokio::sync::Mutex;

use crate::SEARCH_SCORE_COLUMN_NAME;
use crate::index::{MAX_CONCURRENT_INDEX_WRITES, SearchIndex, VectorIndex, embedding_col};
use crate::metadata::MetadataColumns;
use data_components::elasticsearch::search_table::{
    ElasticsearchKnnTable, ElasticsearchTextSearchTable, QueryEmbedder,
};

/// Default kNN candidate-pool size used when a `LIMIT` cannot be pushed down to the
/// Elasticsearch index scan. Matches the DuckDB vector backend's default
/// (`DEFAULT_DUCKDB_VECTOR_SEARCH_LIMIT`).
const DEFAULT_ELASTICSEARCH_VECTOR_SEARCH_LIMIT: usize = 1000;

/// Adapter that implements [`QueryEmbedder`] using an [`Embed`] model.
#[derive(Debug)]
struct EmbedQueryAdapter(Arc<dyn Embed>);

#[async_trait]
impl QueryEmbedder for EmbedQueryAdapter {
    async fn embed_query(&self, query: &str) -> Result<Vec<f32>, DataFusionError> {
        let vectors = self
            .0
            .embed(llms::embeddings::EmbeddingInput::String(query.to_string()))
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        vectors.first().cloned().ok_or_else(|| {
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

    /// The Elasticsearch `dense_vector` similarity the index was created with
    /// (`cosine` | `l2_norm` | `dot_product` | `max_inner_product`).
    pub similarity: String,

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

/// A client whose every call is a test failure.
///
/// Some write-path helpers take the whole [`ElasticsearchIndex`] but touch neither the
/// cluster nor the embedder — they are handed the embeddings and the `_id`s. Those tests
/// still have to populate the struct's fields, and this makes an accidental call loud
/// rather than letting a stub quietly answer one.
#[cfg(test)]
pub(super) mod unused_client {
    use elasticsearch::{
        Elasticsearch, Error, MappingResponse, Result, SearchRequest, SearchResponse,
    };

    #[derive(Debug)]
    pub(crate) struct UnusedClient;

    impl UnusedClient {
        fn refuse<T>(method: &str) -> Result<T> {
            Err(Error::ElasticsearchError {
                status: 500,
                message: format!("unexpected call to {method}"),
            })
        }
    }

    #[async_trait::async_trait]
    impl Elasticsearch for UnusedClient {
        async fn get_mapping(&self, _: &str) -> Result<MappingResponse> {
            Self::refuse("get_mapping")
        }
        async fn search(&self, _: &str, _: &SearchRequest) -> Result<SearchResponse> {
            Self::refuse("search")
        }
        async fn search_raw(&self, _: &str, _: &serde_json::Value) -> Result<SearchResponse> {
            Self::refuse("search_raw")
        }
        async fn open_point_in_time(&self, _: &str, _: &str) -> Result<String> {
            Self::refuse("open_point_in_time")
        }
        async fn search_point_in_time(&self, _: &serde_json::Value) -> Result<SearchResponse> {
            Self::refuse("search_point_in_time")
        }
        async fn close_point_in_time(&self, _: &str) -> Result<()> {
            Self::refuse("close_point_in_time")
        }
        async fn index_exists(&self, _: &str) -> Result<bool> {
            Self::refuse("index_exists")
        }
        async fn create_index(&self, _: &str, _: &serde_json::Value) -> Result<serde_json::Value> {
            Self::refuse("create_index")
        }
        async fn put_mapping(&self, _: &str, _: &serde_json::Value) -> Result<serde_json::Value> {
            Self::refuse("put_mapping")
        }
        async fn get_index_refresh_interval(&self, _: &str) -> Result<Option<String>> {
            Self::refuse("get_index_refresh_interval")
        }
        async fn put_index_settings(
            &self,
            _: &str,
            _: &serde_json::Value,
        ) -> Result<serde_json::Value> {
            Self::refuse("put_index_settings")
        }
        async fn refresh_index(&self, _: &str) -> Result<serde_json::Value> {
            Self::refuse("refresh_index")
        }
        async fn force_merge(&self, _: &str, _: u32) -> Result<serde_json::Value> {
            Self::refuse("force_merge")
        }
        async fn index_document(
            &self,
            _: &str,
            _: &str,
            _: &serde_json::Value,
        ) -> Result<serde_json::Value> {
            Self::refuse("index_document")
        }
        async fn bulk_index(
            &self,
            _: &str,
            _: &[(Option<String>, serde_json::Value)],
        ) -> Result<serde_json::Value> {
            Self::refuse("bulk_index")
        }
        async fn delete_by_query(
            &self,
            _: &str,
            _: &serde_json::Value,
        ) -> Result<serde_json::Value> {
            Self::refuse("delete_by_query")
        }
        async fn bulk_delete_if_unchanged(
            &self,
            _: &str,
            _: &[elasticsearch::DocumentVersion],
        ) -> Result<serde_json::Value> {
            Self::refuse("bulk_delete_if_unchanged")
        }
    }
}

/// Optional Elasticsearch maintenance to run around full/append table-sink writes.
#[derive(Debug, Clone, Default)]
pub struct ElasticsearchIndexWriteOptions {
    /// Temporary `index.refresh_interval` to apply before a bounded write.
    pub refresh_interval_during_write: Option<String>,

    /// Target segment count for `_forcemerge` after a bounded write.
    pub force_merge_segments: Option<u32>,
}

/// How long Elasticsearch keeps a replacing write's snapshot open between two uses of it.
const REPLACE_SNAPSHOT_KEEP_ALIVE: &str = "5m";

/// How often a replacing write renews its snapshot's keep-alive while rows are still being
/// written, well inside [`REPLACE_SNAPSHOT_KEEP_ALIVE`] so one slow renewal does not lose it.
const REPLACE_SNAPSHOT_RENEW_INTERVAL: std::time::Duration = std::time::Duration::from_mins(1);

/// Documents read from the snapshot, and conditionally deleted, per request.
const REPLACE_RECONCILE_PAGE_SIZE: usize = 1000;

/// The documents an Elasticsearch index held when a replacing write (a `refresh_mode: full`
/// refresh) started, kept as a point-in-time reader.
///
/// A replacing write removes a row by not sending it again, so its document has to be found
/// and deleted once the write is done. Every document the write *did* send was rewritten and
/// carries a new `(seq_no, primary_term)`; a document still at the version this snapshot
/// recorded is one the write left out. Deleting each snapshot document conditionally on that
/// version removes exactly those, and leaves anything written since — by this write or a
/// concurrent one — in place, without a second copy of the index or an alias swap. Readers
/// keep seeing the dropped rows until the write completes, never an empty index.
#[derive(Debug)]
struct ReplaceSnapshot {
    /// Elasticsearch may hand back a new id on any search against the reader, so the renewal
    /// task and the reconcile both read and update the latest one here.
    pit_id: Arc<parking_lot::Mutex<String>>,
    renewal: tokio::task::JoinHandle<()>,
}

impl ReplaceSnapshot {
    /// `None` when the index does not exist yet: it holds no documents the write could drop.
    async fn open(
        client: &Arc<dyn Elasticsearch>,
        es_index: &str,
    ) -> Result<Option<Self>, DataFusionError> {
        // A point-in-time reader sees only searchable documents, so make every earlier write
        // searchable first; one still inside `refresh_interval` would otherwise be missing
        // from the snapshot and never reconciled.
        let snapshot = async {
            client.refresh_index(es_index).await?;
            client
                .open_point_in_time(es_index, REPLACE_SNAPSHOT_KEEP_ALIVE)
                .await
        };
        let pit_id = match snapshot.await {
            Ok(pit_id) => pit_id,
            Err(elasticsearch::Error::ElasticsearchError { status: 404, .. }) => return Ok(None),
            Err(e) => {
                return Err(DataFusionError::Execution(format!(
                    "Failed to snapshot the search index '{es_index}' (elasticsearch) before a full refresh, so documents for rows the refresh drops cannot be found afterwards and searches will keep returning those rows. Cause: {e}"
                )));
            }
        };
        let pit_id = Arc::new(parking_lot::Mutex::new(pit_id));
        let renewal = tokio::spawn(Self::renew(Arc::clone(client), Arc::clone(&pit_id)));
        Ok(Some(Self { pit_id, renewal }))
    }

    /// Renews the reader's keep-alive until the write finishes, however long it runs. Stops
    /// at the first failed renewal: the reader is gone, and the reconcile reports that.
    async fn renew(client: Arc<dyn Elasticsearch>, pit_id: Arc<parking_lot::Mutex<String>>) {
        loop {
            tokio::time::sleep(REPLACE_SNAPSHOT_RENEW_INTERVAL).await;
            let id = pit_id.lock().clone();
            let body = serde_json::json!({
                "pit": { "id": id, "keep_alive": REPLACE_SNAPSHOT_KEEP_ALIVE },
                "size": 0,
                "track_total_hits": false,
            });
            match client.search_point_in_time(&body).await {
                Ok(resp) => {
                    if let Some(new_id) = resp.pit_id {
                        *pit_id.lock() = new_id;
                    }
                }
                Err(e) => {
                    tracing::debug!("Stopped renewing a full-refresh snapshot: {e}");
                    return;
                }
            }
        }
    }

    /// Deletes every snapshot document the write did not rewrite, page by page, and closes
    /// the reader. Returns how many documents were deleted.
    async fn delete_unrewritten(
        self,
        client: &dyn Elasticsearch,
        es_index: &str,
    ) -> Result<u64, DataFusionError> {
        self.renewal.abort();
        let mut pit_id = self.pit_id.lock().clone();
        let result = Self::delete_pages(client, es_index, &mut pit_id).await;
        *self.pit_id.lock() = pit_id;
        self.close(client, es_index).await;
        result.map_err(|cause| {
            DataFusionError::Execution(format!(
                "Failed to remove the documents for rows a full refresh dropped from the search index '{es_index}' (elasticsearch), so searches can return rows the dataset no longer has until the next full refresh succeeds. Cause: {cause}"
            ))
        })
    }

    async fn delete_pages(
        client: &dyn Elasticsearch,
        es_index: &str,
        pit_id: &mut String,
    ) -> Result<u64, String> {
        let mut search_after: Option<Vec<serde_json::Value>> = None;
        let mut deleted = 0;
        loop {
            let mut body = serde_json::json!({
                "pit": { "id": *pit_id, "keep_alive": REPLACE_SNAPSHOT_KEEP_ALIVE },
                "size": REPLACE_RECONCILE_PAGE_SIZE,
                "_source": false,
                "seq_no_primary_term": true,
                "track_total_hits": false,
                "sort": [{ "_shard_doc": "asc" }],
            });
            if let Some(after) = search_after.take() {
                body["search_after"] = serde_json::Value::Array(after);
            }
            let page = client
                .search_point_in_time(&body)
                .await
                .map_err(|e| format!("reading the snapshot failed: {e}"))?;
            if let Some(new_id) = page.pit_id {
                *pit_id = new_id;
            }
            let hits = page.hits.hits;
            let Some(last) = hits.last() else {
                return Ok(deleted);
            };
            search_after = Some(last.sort.clone().ok_or_else(|| {
                "the snapshot returned a document without sort values, so it cannot be paged"
                    .to_string()
            })?);
            let full_page = hits.len() == REPLACE_RECONCILE_PAGE_SIZE;
            let versions = hits
                .into_iter()
                .map(|hit| match (hit.seq_no, hit.primary_term) {
                    (Some(seq_no), Some(primary_term)) => Ok(DocumentVersion {
                        id: hit.id,
                        seq_no,
                        primary_term,
                    }),
                    _ => Err(format!(
                        "the snapshot returned document '{}' without its sequence number",
                        hit.id
                    )),
                })
                .collect::<Result<Vec<_>, _>>()?;
            let response = client
                .bulk_delete_if_unchanged(es_index, &versions)
                .await
                .map_err(|e| format!("the delete request failed: {e}"))?;
            deleted += count_conditional_deletes(&response)?;
            if !full_page {
                return Ok(deleted);
            }
        }
    }
}

impl ReplaceSnapshot {
    /// Stops the renewal and releases the reader. A failure only delays the release until
    /// the keep-alive lapses, so it is not reported.
    async fn close(self, client: &dyn Elasticsearch, es_index: &str) {
        self.renewal.abort();
        let pit_id = self.pit_id.lock().clone();
        if let Err(e) = client.close_point_in_time(&pit_id).await {
            tracing::debug!("Could not close a full-refresh snapshot of '{es_index}': {e}");
        }
    }
}

impl Drop for ReplaceSnapshot {
    fn drop(&mut self) {
        self.renewal.abort();
    }
}

/// Counts the documents a conditional bulk delete removed. A `409` is a document rewritten
/// since the snapshot and a `404` one already deleted; both are expected and left alone. Any
/// other per-item outcome is an error, since that document may still be served. Failures are
/// described without the document id, which is the row's primary key.
fn count_conditional_deletes(response: &serde_json::Value) -> Result<u64, String> {
    let items = response
        .get("items")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| {
            format!(
                "the bulk response carries no `items`; got {}",
                write::describe_unexpected_response(response)
            )
        })?;
    let mut deleted = 0;
    for (position, item) in items.iter().enumerate() {
        let outcome = item.get("delete").unwrap_or(item);
        match outcome.get("status").and_then(serde_json::Value::as_u64) {
            Some(200) => deleted += 1,
            Some(404 | 409) => {}
            _ => return Err(write::describe_bulk_failure(position, outcome)),
        }
    }
    Ok(deleted)
}

/// Stateful helper shared by Elasticsearch-backed indexes that write to the same ES index.
#[derive(Debug)]
pub struct ElasticsearchIndexWriteMaintenance {
    options: ElasticsearchIndexWriteOptions,
    write_cycle_active: AtomicBool,
    refresh_interval_overridden: AtomicBool,
    previous_refresh_interval: Mutex<PreviousRefreshInterval>,
    /// Set for the length of a replacing write; see [`ReplaceSnapshot`].
    replace_snapshot: parking_lot::Mutex<Option<ReplaceSnapshot>>,
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
            replace_snapshot: parking_lot::Mutex::new(None),
        }
    }

    /// Starts a write cycle for `window`. The `refresh_interval` tuning is applied once per
    /// cycle; later calls in the same cycle skip it.
    ///
    /// A [`WriteWindow::ReplaceAll`] write snapshots the index (see [`ReplaceSnapshot`]) so
    /// [`Self::on_write_complete`] can delete the documents of every row the write dropped.
    /// Every call takes a fresh snapshot, replacing any it finds: the sinks run each index's
    /// `on_write_start` before the write sends any row, so a fresh snapshot never misses one
    /// of this write's rows, while a snapshot left by a write that was cancelled (dropped
    /// without `on_write_failed`) predates that write's rows and would keep a row it rewrote
    /// from being deleted.
    ///
    /// A failed start leaves no cycle open: the caller does not call `on_write_failed` on an
    /// index whose own start failed, so the next write would otherwise skip the tuning.
    pub async fn on_write_start(
        &self,
        client: &Arc<dyn Elasticsearch>,
        es_index: &str,
        window: WriteWindow,
    ) -> Result<(), DataFusionError> {
        let first_start = !self.write_cycle_active.swap(true, Ordering::AcqRel);
        let started = async {
            if window == WriteWindow::ReplaceAll {
                let snapshot = ReplaceSnapshot::open(client, es_index).await?;
                *self.replace_snapshot.lock() = snapshot;
            }
            if first_start {
                self.tune_refresh_interval(client.as_ref(), es_index)
                    .await?;
            }
            Ok(())
        };
        started.await.inspect_err(|_| self.abandon_write_cycle())
    }

    async fn tune_refresh_interval(
        &self,
        client: &dyn Elasticsearch,
        es_index: &str,
    ) -> Result<(), DataFusionError> {
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
        client
            .put_index_settings(es_index, &body)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        *self.previous_refresh_interval.lock().await = match previous {
            Some(refresh_interval) => PreviousRefreshInterval::Value(refresh_interval),
            None => PreviousRefreshInterval::Unset,
        };
        tracing::debug!(
            "Set Elasticsearch index '{es_index}' refresh_interval to '{refresh_interval}' for bulk write."
        );
        Ok(())
    }

    /// Undo everything [`Self::on_write_start`] set before it failed.
    fn abandon_write_cycle(&self) {
        // Dropping the snapshot stops its renewal; Elasticsearch releases the reader once
        // its keep-alive lapses.
        self.replace_snapshot.lock().take();
        self.refresh_interval_overridden
            .store(false, Ordering::Release);
        self.write_cycle_active.store(false, Ordering::Release);
    }

    async fn on_write_failed(
        &self,
        client: &dyn Elasticsearch,
        es_index: &str,
    ) -> Result<(), DataFusionError> {
        if !self.write_cycle_active.swap(false, Ordering::AcqRel) {
            return Ok(());
        }
        // A failed write proves nothing about which rows the source still has, so delete
        // nothing; the next full refresh reconciles the index.
        let snapshot = self.replace_snapshot.lock().take();
        if let Some(snapshot) = snapshot {
            snapshot.close(client, es_index).await;
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

        // Delete before the refresh below, so it publishes the deletions with the write.
        // A failure still restores the refresh interval before it is reported.
        let snapshot = self.replace_snapshot.lock().take();
        let reconciled = match snapshot {
            Some(snapshot) => snapshot.delete_unrewritten(client, es_index).await.map(|n| {
                tracing::debug!(
                    "Removed {n} document(s) for rows a full refresh dropped from Elasticsearch index '{es_index}'."
                );
            }),
            None => Ok(()),
        };

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

        reconciled
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
            k: DEFAULT_ELASTICSEARCH_VECTOR_SEARCH_LIMIT,
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
        use datafusion::prelude::{Expr, cast, ident};

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
                    ident(f.name())
                } else {
                    cast(ident(f.name()), target_type).alias(f.name())
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
        futures::stream::iter(futs)
            .buffered(MAX_CONCURRENT_INDEX_WRITES)
            .try_collect()
            .await
    }

    async fn on_write_start(&self, window: WriteWindow) -> Result<(), DataFusionError> {
        self.write_maintenance
            .on_write_start(&self.client, &self.es_index, window)
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

    async fn delete_by_keys(&self, keys: RecordBatch) -> Result<(), DataFusionError> {
        let key_columns = delete::document_key_columns(&self.primary_key);
        delete::delete_by_keys(
            self.client.as_ref(),
            &self.es_index,
            &self.primary_key,
            &key_columns,
            &keys,
        )
        .await
    }

    /// `_delete_by_query` filters by field value, so a key naming only some of the indexed fields
    /// removes every document matching it.
    fn deletes_by_partial_key(&self) -> bool {
        true
    }

    async fn delete_group_remainder(
        &self,
        group_columns: &[String],
        members: RecordBatch,
    ) -> Result<(), DataFusionError> {
        delete::delete_group_remainder(
            self.client.as_ref(),
            &self.es_index,
            &self.primary_key,
            group_columns,
            &members,
        )
        .await
    }

    /// The same field-value addressing [`Index::deletes_by_partial_key`] reports, applied to the
    /// rest of a key group: a `_delete_by_query` reaches every document sharing the group's key,
    /// and `must_not` on the surviving `_id`s keeps the members.
    ///
    /// That addressing needs every key column to have a `term` form. A key this index cannot
    /// render one for — a float, date or timestamp column — prunes nothing at all, because
    /// `collect_groups` drops the whole group rather than filter on part of its key, so it is
    /// reported as unsupported and the caller warns that superseded chunks stay in place.
    fn group_pruning(&self) -> GroupPruning {
        if delete::key_renders_terms(&self.primary_key) {
            GroupPruning::Complete
        } else {
            GroupPruning::Unsupported
        }
    }
}

impl ElasticsearchIndex {
    /// The metadata columns to advertise in the query/list schemas: every declared metadata
    /// column except any that collides with the derived embedding column (which is appended
    /// separately). When chunking is enabled this includes the `{col}_offset`
    /// `FixedSizeList(Int32, 2)` column, and — if the search column is itself metadata — the
    /// full-search-field column.
    fn metadata_fields(&self) -> Vec<Field> {
        let embedding_name = embedding_col(&self.embedded_column);
        self.metadata_columns
            .iter()
            .filter(|c| c.name() != embedding_name)
            .map(|c| Arc::unwrap_or_clone(c.field()))
            .collect()
    }

    /// Schema for `query_table_provider` results: primary keys + metadata + embedding +
    /// `_score`.
    ///
    /// This mirrors the S3 Vectors engine: both the query and list plans expose the full
    /// (augmented, when chunked) primary key and the metadata columns, so an in-memory warm
    /// index can fall back onto Elasticsearch — the fallback projection requires every warm
    /// column to be present by name in the Elasticsearch plan. `_spice.chunk_id` (a primary
    /// key when chunked) and `{col}_offset` (a metadata column when chunked) are both stored
    /// in `_source` and read back by `knn_hits_to_batch`.
    fn query_result_schema(&self) -> SchemaRef {
        let mut fields: Vec<Field> = self.primary_key.clone();
        fields.extend(self.metadata_fields());
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

    /// Schema for `list_table_provider` results: primary keys + metadata + embedding.
    ///
    /// See [`Self::query_result_schema`] for why metadata columns are projected uniformly.
    /// Elasticsearch cannot enumerate vectors, so this schema only shapes the empty
    /// `MemTable` returned by `list_table_provider`; matching column *names* is what lets the
    /// warm-index fallback plan build.
    fn list_result_schema(&self) -> SchemaRef {
        let mut fields: Vec<Field> = self.primary_key.clone();
        fields.extend(self.metadata_fields());
        fields.push(Field::new(
            embedding_col(&self.embedded_column),
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, false)),
                self.dims,
            ),
            true,
        ));
        Arc::new(Schema::new(fields))
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
        use datafusion::prelude::ident;

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
            .map(|f| ident(f.name()))
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
        futures::stream::iter(futs)
            .buffered(MAX_CONCURRENT_INDEX_WRITES)
            .try_collect()
            .await
    }

    async fn on_write_start(&self, window: WriteWindow) -> Result<(), DataFusionError> {
        self.write_maintenance
            .on_write_start(&self.client, &self.es_index, window)
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

    async fn delete_by_keys(&self, keys: RecordBatch) -> Result<(), DataFusionError> {
        let key_columns = delete::document_key_columns(&self.primary_key);
        delete::delete_by_keys(
            self.client.as_ref(),
            &self.es_index,
            &self.primary_key,
            &key_columns,
            &keys,
        )
        .await
    }

    /// Same `_delete_by_query` addressing as [`ElasticsearchIndex`].
    fn deletes_by_partial_key(&self) -> bool {
        true
    }

    async fn delete_group_remainder(
        &self,
        group_columns: &[String],
        members: RecordBatch,
    ) -> Result<(), DataFusionError> {
        delete::delete_group_remainder(
            self.client.as_ref(),
            &self.es_index,
            &self.primary_key,
            group_columns,
            &members,
        )
        .await
    }

    /// Same group addressing as [`ElasticsearchIndex`], including the key types it cannot
    /// address.
    fn group_pruning(&self) -> GroupPruning {
        if delete::key_renders_terms(&self.primary_key) {
            GroupPruning::Complete
        } else {
            GroupPruning::Unsupported
        }
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
        /// When set, every `put_index_settings` call fails — the shape of an ES cluster that
        /// rejects the `refresh_interval` override `on_write_start` applies.
        fail_put_settings: bool,
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
            if self.fail_put_settings {
                return Err(elasticsearch::Error::ElasticsearchError {
                    status: 400,
                    message: "settings rejected".to_string(),
                });
            }
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
        async fn delete_by_query(
            &self,
            _: &str,
            _: &serde_json::Value,
        ) -> elasticsearch::Result<serde_json::Value> {
            unimplemented!()
        }
        async fn bulk_delete_if_unchanged(
            &self,
            _: &str,
            _: &[elasticsearch::DocumentVersion],
        ) -> elasticsearch::Result<serde_json::Value> {
            unimplemented!()
        }
    }

    fn dyn_es(client: &Arc<MockElasticsearch>) -> Arc<dyn Elasticsearch> {
        Arc::clone(client) as Arc<dyn Elasticsearch>
    }

    fn make_maintenance(
        opts: ElasticsearchIndexWriteOptions,
    ) -> ElasticsearchIndexWriteMaintenance {
        ElasticsearchIndexWriteMaintenance::new(opts)
    }

    /// `on_write_start` with no options configured is a pure no-op: no ES calls.
    #[tokio::test]
    async fn write_start_no_options_is_noop() {
        let client = Arc::new(MockElasticsearch::default());
        let m = make_maintenance(ElasticsearchIndexWriteOptions::default());

        m.on_write_start(&dyn_es(&client), "my-index", WriteWindow::Append)
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
        let client = Arc::new(MockElasticsearch::default());
        let m = make_maintenance(ElasticsearchIndexWriteOptions {
            refresh_interval_during_write: Some("-1".to_string()),
            force_merge_segments: None,
        });

        // Simulate three batches before on_write_complete.
        m.on_write_start(&dyn_es(&client), "my-index", WriteWindow::Append)
            .await
            .expect("first start");
        m.on_write_start(&dyn_es(&client), "my-index", WriteWindow::Append)
            .await
            .expect("second start — must be no-op");
        m.on_write_start(&dyn_es(&client), "my-index", WriteWindow::Append)
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

    /// A start that fails leaves no write cycle open, so the *next* write still applies the
    /// `refresh_interval` override instead of short-circuiting as a second-batch no-op.
    ///
    /// The caller does not call `on_write_failed` on an index whose own start failed — a
    /// partway start owns its cleanup — so this has to be self-contained.
    #[tokio::test]
    async fn a_failed_write_start_leaves_no_write_cycle_open() {
        let failing = Arc::new(MockElasticsearch {
            fail_put_settings: true,
            ..Default::default()
        });
        let m = make_maintenance(ElasticsearchIndexWriteOptions {
            refresh_interval_during_write: Some("-1".to_string()),
            force_merge_segments: None,
        });

        m.on_write_start(&dyn_es(&failing), "my-index", WriteWindow::Append)
            .await
            .expect_err("the override was rejected");

        // A second write, against a healthy cluster, must apply the override rather than
        // treat itself as another batch of the abandoned cycle.
        let healthy = Arc::new(MockElasticsearch::default());
        m.on_write_start(&dyn_es(&healthy), "my-index", WriteWindow::Append)
            .await
            .expect("a later write starts a fresh cycle");

        assert_eq!(
            healthy.put_settings_calls.load(Ordering::Relaxed),
            1,
            "the write after a failed start must still override refresh_interval"
        );
    }

    /// `on_write_complete` restores the refresh_interval and calls `_refresh`.
    /// `force_merge` must NOT be called when `force_merge_segments` is `None`.
    #[tokio::test]
    async fn write_complete_restores_interval_and_refreshes() {
        let client = Arc::new(MockElasticsearch {
            refresh_interval: std::sync::Mutex::new(Some("30s".to_string())),
            ..Default::default()
        });
        let m = make_maintenance(ElasticsearchIndexWriteOptions {
            refresh_interval_during_write: Some("-1".to_string()),
            force_merge_segments: None,
        });

        m.on_write_start(&dyn_es(&client), "my-index", WriteWindow::Append)
            .await
            .expect("start");
        m.on_write_complete(client.as_ref(), "my-index")
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
        let client = Arc::new(MockElasticsearch::default());
        let m = make_maintenance(ElasticsearchIndexWriteOptions {
            refresh_interval_during_write: None,
            force_merge_segments: Some(1),
        });

        m.on_write_start(&dyn_es(&client), "my-index", WriteWindow::Append)
            .await
            .expect("start");
        m.on_write_complete(client.as_ref(), "my-index")
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
        let client = Arc::new(MockElasticsearch {
            refresh_interval: std::sync::Mutex::new(Some("30s".to_string())),
            ..Default::default()
        });
        let m = make_maintenance(ElasticsearchIndexWriteOptions {
            refresh_interval_during_write: Some("-1".to_string()),
            force_merge_segments: None,
        });

        // First write cycle fails.
        m.on_write_start(&dyn_es(&client), "my-index", WriteWindow::Append)
            .await
            .expect("start cycle 1");
        m.on_write_failed(client.as_ref(), "my-index")
            .await
            .expect("failed cycle 1");

        // Second write cycle — must re-apply the override.
        m.on_write_start(&dyn_es(&client), "my-index", WriteWindow::Append)
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
        let client = Arc::new(MockElasticsearch::default());
        let m = make_maintenance(ElasticsearchIndexWriteOptions {
            refresh_interval_during_write: Some("-1".to_string()),
            force_merge_segments: Some(1),
        });

        m.on_write_complete(client.as_ref(), "my-index")
            .await
            .expect("complete without start must be no-op");

        assert_eq!(client.refresh_index_calls.load(Ordering::Relaxed), 0);
        assert_eq!(client.force_merge_calls.load(Ordering::Relaxed), 0);
        assert_eq!(client.put_settings_calls.load(Ordering::Relaxed), 0);
    }

    // ── Chunked warm-index fallback contract ─────────────────────────────────────

    use crate::index::chunking::{CHUNKED_INDEX_CHUNK_KEY, ChunkedSearchIndex};
    use crate::index::compound::{CompoundReadMode, CompoundVectorIndex};
    use crate::index::memory::{MemoryDistanceMetric, MemoryVectorIndex};
    use crate::metadata::MetadataColumn;
    use llms::embeddings::EmbeddingInput;

    #[derive(Debug)]
    struct NoopEmbed;

    #[async_trait::async_trait]
    impl Embed for NoopEmbed {
        async fn embed(
            &self,
            _input: EmbeddingInput,
        ) -> llms::embeddings::Result<std::sync::Arc<Vec<Vec<f32>>>> {
            Ok(std::sync::Arc::new(vec![]))
        }
        fn size(&self) -> i32 {
            3
        }
    }

    fn noop_embed_udf() -> Arc<datafusion::logical_expr::ScalarUDF> {
        use datafusion::logical_expr::{Volatility, create_udf};
        Arc::new(create_udf(
            "embed",
            vec![],
            DataType::Null,
            Volatility::Volatile,
            Arc::new(|_| unimplemented!("not exercised by schema/plan tests")),
        ))
    }

    /// A chunked [`ElasticsearchIndex`] with an augmented primary key (`_spice.chunk_id`)
    /// and a `{col}_offset` non-filterable metadata column, mirroring what `try_from_table`
    /// produces for a chunked column.
    fn chunked_es_index() -> ElasticsearchIndex {
        let embedded_column = "content".to_string();
        let dims = 3;
        let primary_key =
            ChunkedSearchIndex::augment_primary_key(vec![Field::new("id", DataType::Int64, false)]);
        let offset_field = Field::new(
            ChunkedSearchIndex::chunking_offset_col(&embedded_column),
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Int32, false)), 2),
            false,
        );
        let metadata_columns: MetadataColumns = vec![MetadataColumn::NonFilterable(Arc::new(
            offset_field.clone(),
        ))]
        .into();

        let source_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("content", DataType::Utf8, true),
            offset_field,
            Field::new(CHUNKED_INDEX_CHUNK_KEY, DataType::UInt64, false),
            Field::new(
                embedding_col(&embedded_column),
                DataType::FixedSizeList(
                    Arc::new(Field::new("item", DataType::Float32, false)),
                    dims,
                ),
                true,
            ),
        ]));

        ElasticsearchIndex {
            client: Arc::new(MockElasticsearch::default()),
            es_index: "test-index".to_string(),
            embedded_column,
            vector_field: embedding_col("content"),
            text_fields: vec![],
            primary_key,
            compute_query: Arc::new(NoopEmbed),
            dims,
            similarity: "cosine".to_string(),
            source_schema,
            metadata_columns,
            batch_write_rows: 1000,
            write_maintenance: Arc::new(ElasticsearchIndexWriteMaintenance::default()),
        }
    }

    #[test]
    fn chunked_result_schemas_expose_chunk_id_and_offset() {
        let index = chunked_es_index();
        let offset_col = ChunkedSearchIndex::chunking_offset_col("content");
        let embedding = embedding_col("content");

        // Every column, in order, with its exact type and nullability: the augmented key
        // (the row's key, then the chunk key), the chunk offsets, the embedding, and — for a
        // query only — the score.
        let item = |data_type: DataType| Arc::new(Field::new("item", data_type, false));
        let mut fields = vec![
            Field::new("id", DataType::Int64, false),
            Field::new(CHUNKED_INDEX_CHUNK_KEY, DataType::UInt64, false),
            Field::new(
                &offset_col,
                DataType::FixedSizeList(item(DataType::Int32), 2),
                false,
            ),
            Field::new(
                &embedding,
                DataType::FixedSizeList(item(DataType::Float32), 3),
                true,
            ),
        ];
        assert_eq!(*index.list_result_schema(), Schema::new(fields.clone()));
        fields.push(Field::new(
            SEARCH_SCORE_COLUMN_NAME,
            DataType::Float64,
            true,
        ));
        assert_eq!(*index.query_result_schema(), Schema::new(fields));
    }

    /// The core fallback-contract guard: a `CompoundVectorIndex` pairing an in-memory warm
    /// index with the chunked Elasticsearch index must build both its query and list fallback
    /// plans without a plan error — i.e. every warm-index column exists by name in the
    /// Elasticsearch plan.
    #[test]
    fn chunked_warm_index_fallback_plans_build() {
        let es_index = chunked_es_index();
        let memory = MemoryVectorIndex::try_new(
            es_index.search_column(),
            es_index.primary_fields(),
            es_index.metadata_columns.clone(),
            Arc::new(NoopEmbed),
            noop_embed_udf(),
            "test-model".to_string(),
            MemoryDistanceMetric::Cosine,
        )
        .expect("memory warm index should build");

        // What each side answers with on its own.
        let warm_query = memory
            .query_table_provider("query")
            .expect("warm query plan")
            .schema()
            .as_arrow()
            .clone();
        let warm_list = memory
            .list_table_provider()
            .expect("warm list plan")
            .schema()
            .as_arrow()
            .clone();
        let es_query = es_index
            .query_table_provider("query")
            .expect("Elasticsearch query plan")
            .schema()
            .as_arrow()
            .clone();
        let es_list = es_index
            .list_table_provider()
            .expect("Elasticsearch list plan")
            .schema()
            .as_arrow()
            .clone();

        let compound = CompoundVectorIndex::try_new(
            Arc::new(memory) as Arc<dyn VectorIndex>,
            Arc::new(es_index) as Arc<dyn VectorIndex>,
            CompoundReadMode::FallbackToSecondary,
        )
        .expect("compound index should build");

        let query = compound
            .query_table_provider("query")
            .expect("query fallback plan should build over the augmented primary key + metadata");
        let list = compound
            .list_table_provider()
            .expect("list fallback plan should build over the augmented primary key + metadata");

        let offsets =
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Int32, false)), 2);
        for (label, fallback, warm, es) in [
            ("query", query.schema().as_arrow(), &warm_query, &es_query),
            ("list", list.schema().as_arrow(), &warm_list, &es_list),
        ] {
            // The fallback answers with exactly the warm index's columns...
            assert_eq!(fallback, warm, "{label} fallback schema");
            // ...each of which Elasticsearch provides by name...
            for field in warm.fields() {
                es.field_with_name(field.name()).unwrap_or_else(|_| {
                    panic!("the Elasticsearch {label} plan lacks '{}'", field.name())
                });
            }
            // ...and the row and chunk keys and the chunk offsets reach it with their own
            // types, so none of them is cast on the way.
            for (key, data_type) in [
                ("id", DataType::Int64),
                (CHUNKED_INDEX_CHUNK_KEY, DataType::UInt64),
                ("content_offset", offsets.clone()),
            ] {
                let warm_type = warm
                    .field_with_name(key)
                    .unwrap_or_else(|_| panic!("the warm {label} plan lacks '{key}'"))
                    .data_type();
                let es_type = es
                    .field_with_name(key)
                    .unwrap_or_else(|_| panic!("the Elasticsearch {label} plan lacks '{key}'"))
                    .data_type();
                assert_eq!(warm_type, &data_type, "warm {label} '{key}'");
                assert_eq!(es_type, &data_type, "Elasticsearch {label} '{key}'");
            }
        }
    }
}

#[cfg(test)]
mod chunked_group_pruning_tests {
    use super::*;

    use std::collections::{HashMap, HashSet};
    use std::sync::Mutex as StdMutex;

    use arrow::array::{Int64Array, StringArray};
    use elasticsearch::{
        Elasticsearch as ElasticsearchTrait, Error as EsError, FieldMapping, IndexMapping,
        MappingResponse, Mappings, Result as EsResult, SearchRequest, SearchResponse,
    };
    use llms::embeddings::EmbeddingInput;
    use serde_json::{Value, json};

    use crate::index::chunking::{CHUNKED_INDEX_CHUNK_KEY, ChunkedSearchIndex, DelimChunker};
    use crate::metadata::MetadataColumn;

    const DIMS: i32 = 3;

    /// One unit vector per input, so the write path's embedding count matches its row count.
    #[derive(Debug)]
    struct OnesEmbed;

    #[async_trait::async_trait]
    impl Embed for OnesEmbed {
        async fn embed(
            &self,
            input: EmbeddingInput,
        ) -> llms::embeddings::Result<Arc<Vec<Vec<f32>>>> {
            let n = match input {
                EmbeddingInput::String(_) => 1,
                EmbeddingInput::StringArray(v) => v.len(),
                _ => 0,
            };
            Ok(Arc::new(vec![vec![1.0, 0.0, 0.0]; n]))
        }
        fn size(&self) -> i32 {
            DIMS
        }
    }

    /// An Elasticsearch stand-in that holds documents by `_id` and answers `_delete_by_query`
    /// by evaluating the query against them.
    ///
    /// It is a model, not a cluster: it understands exactly the clause kinds this module emits
    /// (`term`, `ids`, and `bool` with `filter` / `should` / `must_not` /
    /// `minimum_should_match`), and a query naming anything else fails the test rather than
    /// matching nothing. Modelling the boolean semantics — rather than asserting a literal
    /// request body — is what makes the surviving document set, not the spelling of the query,
    /// the thing under test.
    #[derive(Debug, Default)]
    struct StoreClient {
        docs: StdMutex<Vec<(String, Value)>>,
    }

    impl StoreClient {
        /// The `_spice.chunk_id`s the store holds for source row `id`, ascending.
        fn stored_chunk_ids(&self, id: i64) -> Vec<u64> {
            let docs = self.docs.lock().expect("docs mutex should not be poisoned");
            let mut chunk_ids: Vec<u64> = docs
                .iter()
                .filter(|(_, doc)| doc["id"].as_i64() == Some(id))
                .filter_map(|(_, doc)| doc[CHUNKED_INDEX_CHUNK_KEY].as_u64())
                .collect();
            chunk_ids.sort_unstable();
            chunk_ids
        }
    }

    /// Does `doc` (stored under `_id`) satisfy `query`? `None` for a clause shape this model does
    /// not implement, which the caller turns into a failed request.
    fn matches(query: &Value, id: &str, doc: &Value) -> Option<bool> {
        if let Some(values) = query.get("ids").and_then(|ids| ids.get("values")) {
            let values = values.as_array()?;
            return Some(values.iter().any(|v| v.as_str() == Some(id)));
        }
        if let Some(term) = query.get("term").and_then(Value::as_object) {
            let (path, wanted) = term.iter().next()?;
            // A `keyword` multi-field matches on its parent field's stored value.
            let field = path.strip_suffix(".keyword").unwrap_or(path);
            return Some(doc.get(field) == Some(wanted));
        }
        // `filter` / `should` / `must_not` are always arrays in the bodies this module emits, so
        // any other spelling is a shape the model does not implement rather than an empty one.
        let b = query.get("bool")?.as_object()?;
        let all = |key: &str| -> Option<bool> {
            match b.get(key) {
                None => Some(true),
                Some(Value::Array(clauses)) => clauses
                    .iter()
                    .try_fold(true, |ok, clause| Some(ok & matches(clause, id, doc)?)),
                Some(_) => None,
            }
        };
        let mut ok = all("filter")?;
        if let Some(shoulds) = b.get("should") {
            let minimum = b.get("minimum_should_match")?.as_u64()?;
            let hits = shoulds.as_array()?.iter().try_fold(0u64, |hits, clause| {
                Some(hits + u64::from(matches(clause, id, doc)?))
            })?;
            ok &= hits >= minimum;
        }
        match b.get("must_not") {
            None => {}
            Some(Value::Array(clauses)) => {
                for clause in clauses {
                    ok &= !matches(clause, id, doc)?;
                }
            }
            Some(_) => return None,
        }
        Some(ok)
    }

    fn unexpected(method: &str) -> EsError {
        EsError::ElasticsearchError {
            status: 500,
            message: format!("unexpected call to {method}"),
        }
    }

    #[async_trait::async_trait]
    impl ElasticsearchTrait for StoreClient {
        async fn bulk_delete_if_unchanged(
            &self,
            _index: &str,
            _docs: &[elasticsearch::DocumentVersion],
        ) -> EsResult<serde_json::Value> {
            Err(unexpected("bulk_delete_if_unchanged"))
        }

        async fn bulk_index(
            &self,
            _index: &str,
            docs: &[(Option<String>, Value)],
        ) -> EsResult<Value> {
            let mut store = self.docs.lock().expect("docs mutex should not be poisoned");
            let mut items = Vec::with_capacity(docs.len());
            for (id, doc) in docs {
                let id = id.clone().ok_or_else(|| EsError::ElasticsearchError {
                    status: 400,
                    message: "a chunked write must name every document's _id".to_string(),
                })?;
                match store.iter_mut().find(|(existing, _)| *existing == id) {
                    Some(slot) => slot.1 = doc.clone(),
                    None => store.push((id.clone(), doc.clone())),
                }
                items.push(json!({"index": {"_id": id, "status": 200}}));
            }
            Ok(json!({"errors": false, "items": items}))
        }

        async fn delete_by_query(&self, _index: &str, query: &Value) -> EsResult<Value> {
            let mut store = self.docs.lock().expect("docs mutex should not be poisoned");
            let mut doomed = HashSet::new();
            for (id, doc) in store.iter() {
                let hit = matches(query, id, doc).ok_or_else(|| EsError::ElasticsearchError {
                    status: 400,
                    message: format!("query shape not modelled: {query}"),
                })?;
                if hit {
                    doomed.insert(id.clone());
                }
            }
            let deleted = doomed.len() as u64;
            store.retain(|(id, _)| !doomed.contains(id));

            Ok(json!({
                "took": 1, "timed_out": false, "total": deleted, "deleted": deleted,
                "batches": 1, "version_conflicts": 0, "noops": 0,
                "retries": {"bulk": 0, "search": 0}, "throttled_millis": 0, "failures": [],
            }))
        }

        async fn get_mapping(&self, index: &str) -> EsResult<MappingResponse> {
            let keyword = FieldMapping {
                field_type: Some("keyword".to_string()),
                properties: None,
                fields: None,
                ignore_above: None,
                index: None,
                normalizer: None,
                dims: None,
                similarity: None,
            };
            Ok(MappingResponse::from([(
                index.to_string(),
                IndexMapping {
                    mappings: Mappings {
                        properties: HashMap::from([("id".to_string(), keyword)]),
                    },
                },
            )]))
        }

        async fn search(&self, _index: &str, _body: &SearchRequest) -> EsResult<SearchResponse> {
            Err(unexpected("search"))
        }
        async fn search_raw(&self, _index: &str, _body: &Value) -> EsResult<SearchResponse> {
            Err(unexpected("search_raw"))
        }
        async fn open_point_in_time(&self, _index: &str, _keep_alive: &str) -> EsResult<String> {
            Err(unexpected("open_point_in_time"))
        }
        async fn search_point_in_time(&self, _body: &Value) -> EsResult<SearchResponse> {
            Err(unexpected("search_point_in_time"))
        }
        async fn close_point_in_time(&self, _pit_id: &str) -> EsResult<()> {
            Err(unexpected("close_point_in_time"))
        }
        async fn index_exists(&self, _index: &str) -> EsResult<bool> {
            Err(unexpected("index_exists"))
        }
        async fn create_index(&self, _index: &str, _body: &Value) -> EsResult<Value> {
            Err(unexpected("create_index"))
        }
        async fn put_mapping(&self, _index: &str, _body: &Value) -> EsResult<Value> {
            Err(unexpected("put_mapping"))
        }
        async fn get_index_refresh_interval(&self, _index: &str) -> EsResult<Option<String>> {
            Err(unexpected("get_index_refresh_interval"))
        }
        async fn put_index_settings(&self, _index: &str, _body: &Value) -> EsResult<Value> {
            Err(unexpected("put_index_settings"))
        }
        async fn refresh_index(&self, _index: &str) -> EsResult<Value> {
            Err(unexpected("refresh_index"))
        }
        async fn force_merge(&self, _index: &str, _max_num_segments: u32) -> EsResult<Value> {
            Err(unexpected("force_merge"))
        }
        async fn index_document(&self, _index: &str, _id: &str, _doc: &Value) -> EsResult<Value> {
            Err(unexpected("index_document"))
        }
    }

    /// An [`ElasticsearchIndex`] shaped as the inner index of a [`ChunkedSearchIndex`]: keyed by
    /// the source row's `id` plus the chunk id, one document per chunk.
    fn chunked_index(client: &Arc<StoreClient>) -> Arc<ElasticsearchIndex> {
        let embedded_column = "content".to_string();
        let primary_key =
            ChunkedSearchIndex::augment_primary_key(vec![Field::new("id", DataType::Int64, false)]);
        let offset_field = Field::new(
            ChunkedSearchIndex::chunking_offset_col(&embedded_column),
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Int32, false)), 2),
            false,
        );
        let metadata_columns: MetadataColumns = vec![MetadataColumn::NonFilterable(Arc::new(
            offset_field.clone(),
        ))]
        .into();
        let source_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("content", DataType::Utf8, true),
            offset_field,
            Field::new(CHUNKED_INDEX_CHUNK_KEY, DataType::UInt64, false),
            Field::new(
                embedding_col(&embedded_column),
                DataType::FixedSizeList(
                    Arc::new(Field::new("item", DataType::Float32, false)),
                    DIMS,
                ),
                true,
            ),
        ]));

        Arc::new(ElasticsearchIndex {
            client: Arc::clone(client) as Arc<dyn Elasticsearch>,
            es_index: "idx".to_string(),
            embedded_column,
            vector_field: embedding_col("content"),
            text_fields: vec![],
            primary_key,
            compute_query: Arc::new(OnesEmbed),
            dims: DIMS,
            similarity: "cosine".to_string(),
            source_schema,
            metadata_columns,
            batch_write_rows: 1000,
            write_maintenance: Arc::new(ElasticsearchIndexWriteMaintenance::default()),
        })
    }

    /// Source rows as the chunking layer receives them: the base key and the text to chunk.
    fn content_rows(rows: &[(i64, &str)]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("content", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter().map(|(_, text)| *text).collect::<Vec<_>>(),
                )),
            ],
        )
        .expect("valid test batch")
    }

    /// Regression test for #13717 over a real [`ElasticsearchIndex`]: a row rewritten to text
    /// that chunks into fewer pieces is upserted under chunk ids `0..n` and nothing above them,
    /// so every higher chunk id the previous text produced stays in the index — the row keeps
    /// answering searches for a word its current text does not contain.
    #[tokio::test]
    async fn a_shortened_row_drops_the_chunks_it_no_longer_produces() {
        let client = Arc::new(StoreClient::default());
        let idx = ChunkedSearchIndex::new(
            chunked_index(&client) as Arc<dyn SearchIndex>,
            Arc::new(DelimChunker { delim: ' ' }),
        );

        idx.write(content_rows(&[(1, "aaa bbb"), (2, "ddd eee")]))
            .await
            .expect("the first write lands");
        assert_eq!(client.stored_chunk_ids(1), vec![0, 1]);
        assert_eq!(client.stored_chunk_ids(2), vec![0, 1]);

        idx.write(content_rows(&[(1, "ccc")]))
            .await
            .expect("the rewrite lands");

        assert_eq!(
            client.stored_chunk_ids(1),
            vec![0],
            "the chunk 'bbb' produced goes with the text that produced it"
        );
        assert_eq!(
            client.stored_chunk_ids(2),
            vec![0, 1],
            "a row the write did not touch keeps every chunk"
        );
    }
}
