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

use std::any::Any;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::TableProvider;
use datafusion::datasource::DefaultTableSource;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::LogicalPlan;
use datafusion_expr::LogicalPlanBuilder;
use elasticsearch::Elasticsearch;
use llms::embeddings::Embed;
use runtime_datafusion_index::Index;

use crate::SEARCH_SCORE_COLUMN_NAME;
use crate::index::{SearchIndex, VectorIndex, embedding_col};
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
        // Elasticsearch indexes are typically populated by the source system;
        // for now, writes from the Spice pipeline are a pass-through.
        // A full implementation would bulk-index documents here.
        Ok(record)
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
        cols
    }
}

impl ElasticsearchIndex {
    /// Schema for `query_table_provider` results: primary keys + embedding + `_score`.
    fn query_result_schema(&self) -> SchemaRef {
        let mut fields: Vec<Field> = self.primary_key.clone();
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

    /// Schema for `list_table_provider` results: primary keys + embedding.
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
        Arc::new(Schema::new(fields))
    }
}

/// Elasticsearch-backed full-text search index.
///
/// Uses Elasticsearch's native BM25 full-text search capabilities,
/// compatible with the `text_search` UDTF and `rrf` hybrid search.
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
        Ok(record)
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
            limit: 100,
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
        cols.push(self.search_column_name.clone());
        cols
    }
}
