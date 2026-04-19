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

//! [`TableProvider`] implementations for Elasticsearch kNN vector search
//! and full-text search, used by the search index layer.

use std::any::Any;
use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::project_schema;
use datafusion::datasource::TableType;
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use datafusion::prelude::Expr;
use elasticsearch::{Elasticsearch, KnnQuery, SearchRequest};

use super::query_table::hits_to_record_batch;

/// The column name for the search relevance score.
pub static ES_SCORE_COLUMN_NAME: &str = "_score";

/// The column name for the document `_id`.
pub static ES_ID_COLUMN_NAME: &str = "_id";

/// Trait for lazily computing query embedding vectors.
///
/// This allows the kNN table provider to be constructed in a sync context
/// (e.g. `query_table_provider`) and defer the async embedding computation
/// to execution time.
#[async_trait]
pub trait QueryEmbedder: std::fmt::Debug + Send + Sync {
    async fn embed_query(&self, query: &str) -> Result<Vec<f32>, DataFusionError>;
}

// ── kNN Vector Search Table ────────────────────────────────────────────────

/// A [`TableProvider`] that executes an Elasticsearch kNN vector search.
///
/// Returns the primary key fields, embedding vector, and `_score` column.
///
/// Supports two modes:
/// - Pre-computed vector: supply `query_vector` directly
/// - Lazy embedding: supply `query_text` + `embedder` to compute at execution time
#[derive(Debug)]
pub struct ElasticsearchKnnTable {
    pub client: Arc<dyn Elasticsearch>,
    pub index: String,
    pub vector_field: String,
    pub query_vector: Vec<f32>,
    pub k: usize,
    /// Schema of the kNN result (primary keys + embedding + _score).
    pub schema: SchemaRef,
    /// Schema of the full source document (for extracting fields from _source).
    pub source_schema: SchemaRef,
    /// Query text for lazy embedding computation.
    pub query_text: Option<String>,
    /// Embedder for computing query vectors from text at execution time.
    pub embedder: Option<Arc<dyn QueryEmbedder>>,
}

#[async_trait]
impl TableProvider for ElasticsearchKnnTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let effective_k = limit.unwrap_or(self.k);
        if effective_k == 0 {
            let projected_schema = project_schema(&self.schema, projection)?;
            return Ok(Arc::new(datafusion::physical_plan::empty::EmptyExec::new(
                projected_schema,
            )));
        }
        let projected_schema = project_schema(&self.schema, projection)?;
        Ok(Arc::new(ElasticsearchKnnExec {
            client: Arc::clone(&self.client),
            index: self.index.clone(),
            vector_field: self.vector_field.clone(),
            query_vector: self.query_vector.clone(),
            k: effective_k,
            schema: Arc::clone(&self.schema),
            source_schema: Arc::clone(&self.source_schema),
            projected_schema,
            projection: projection.cloned(),
            query_text: self.query_text.clone(),
            embedder: self.embedder.clone(),
            properties: PlanProperties::new(
                EquivalenceProperties::new(project_schema(&self.schema, projection)?),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Final,
                Boundedness::Bounded,
            ),
        }))
    }
}

#[derive(Debug)]
struct ElasticsearchKnnExec {
    client: Arc<dyn Elasticsearch>,
    index: String,
    vector_field: String,
    query_vector: Vec<f32>,
    k: usize,
    schema: SchemaRef,
    source_schema: SchemaRef,
    projected_schema: SchemaRef,
    projection: Option<Vec<usize>>,
    query_text: Option<String>,
    embedder: Option<Arc<dyn QueryEmbedder>>,
    properties: PlanProperties,
}

impl DisplayAs for ElasticsearchKnnExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "ElasticsearchKnnExec: index={}, field={}, k={}",
            self.index, self.vector_field, self.k
        )
    }
}

impl ExecutionPlan for ElasticsearchKnnExec {
    fn name(&self) -> &'static str {
        "ElasticsearchKnnExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        let client = Arc::clone(&self.client);
        let index = self.index.clone();
        let vector_field = self.vector_field.clone();
        let query_vector = self.query_vector.clone();
        let k = self.k;
        let schema = Arc::clone(&self.schema);
        let source_schema = Arc::clone(&self.source_schema);
        let projected_schema = Arc::clone(&self.projected_schema);
        let projection = self.projection.clone();
        let query_text = self.query_text.clone();
        let embedder = self.embedder.clone();

        let stream = futures::stream::once(async move {
            // Compute the query vector: use embedder if available, otherwise use pre-set vector.
            let query_vector = if let (Some(embedder), Some(query_text)) = (&embedder, &query_text)
            {
                embedder.embed_query(query_text).await?
            } else {
                query_vector
            };

            if query_vector.is_empty() {
                return Err(DataFusionError::Execution(
                    "Elasticsearch kNN search requires a non-empty query vector".to_string(),
                ));
            }

            let vector_field_for_alias = vector_field.clone();
            let req = SearchRequest {
                knn: Some(KnnQuery {
                    field: vector_field,
                    query_vector,
                    k,
                    num_candidates: k.saturating_mul(2),
                }),
                size: Some(k),
                ..Default::default()
            };

            let response = client
                .search(&index, &req)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            // Build alias: find the embedding column in the output schema (FixedSizeList type)
            // and map it from the ES vector_field name.
            let alias_pair = schema
                .fields()
                .iter()
                .find(|f| matches!(f.data_type(), DataType::FixedSizeList(_, _)))
                .filter(|f| f.name() != &vector_field_for_alias)
                .map(|f| (vector_field_for_alias.clone(), f.name().clone()));

            let batch = knn_hits_to_batch(
                &response.hits.hits,
                &schema,
                &source_schema,
                alias_pair.as_ref().map(|(a, b)| (a.as_str(), b.as_str())),
            )?;

            if let Some(proj) = &projection {
                Ok(batch.project(proj)?)
            } else {
                Ok(batch)
            }
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            projected_schema,
            stream,
        )))
    }
}

/// Convert kNN search hits to a [`RecordBatch`] with primary key fields and `_score`.
///
/// `vector_field_alias` maps the ES `vector_field` name to the output schema's
/// embedding column name so extraction from `_source` succeeds even when names differ.
fn knn_hits_to_batch(
    hits: &[elasticsearch::Hit],
    output_schema: &SchemaRef,
    source_schema: &SchemaRef,
    vector_field_alias: Option<(&str, &str)>,
) -> Result<RecordBatch, DataFusionError> {
    let num_rows = hits.len();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(output_schema.fields().len());

    // Build the source batch once for all _source-derived fields.
    let source_batch = hits_to_record_batch(hits, source_schema)?;

    for field in output_schema.fields() {
        if field.name() == ES_SCORE_COLUMN_NAME {
            let scores: Vec<Option<f64>> = hits.iter().map(|h| h.score).collect();
            columns.push(Arc::new(Float64Array::from(scores)) as ArrayRef);
        } else if field.name() == ES_ID_COLUMN_NAME {
            let ids: Vec<Option<&str>> = hits.iter().map(|h| Some(h.id.as_str())).collect();
            columns.push(Arc::new(StringArray::from(ids)) as ArrayRef);
        } else if let Ok(col_idx) = source_batch.schema().index_of(field.name()) {
            columns.push(Arc::clone(source_batch.column(col_idx)));
        } else if let Some((es_field, output_name)) = vector_field_alias {
            // When the output schema expects a derived name (e.g. "col_embedding")
            // but ES stores it under the configured vector_field, alias it.
            if field.name() == output_name {
                if let Ok(col_idx) = source_batch.schema().index_of(es_field) {
                    columns.push(Arc::clone(source_batch.column(col_idx)));
                } else {
                    columns.push(arrow::array::new_null_array(field.data_type(), num_rows));
                }
            } else {
                columns.push(arrow::array::new_null_array(field.data_type(), num_rows));
            }
        } else {
            // Field not in source; fill with nulls.
            columns.push(arrow::array::new_null_array(field.data_type(), num_rows));
        }
    }

    RecordBatch::try_new(Arc::clone(output_schema), columns)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

// ── Full-Text Search Table ─────────────────────────────────────────────────

/// A [`TableProvider`] that executes an Elasticsearch full-text search query.
///
/// Returns primary key fields and a `_score` column with BM25 relevance scores.
#[derive(Debug)]
pub struct ElasticsearchTextSearchTable {
    pub client: Arc<dyn Elasticsearch>,
    pub index: String,
    pub search_fields: Vec<String>,
    pub query_text: String,
    pub limit: usize,
    /// Schema of the result (primary keys + `_score`).
    pub schema: SchemaRef,
    /// Schema of the full source document.
    pub source_schema: SchemaRef,
}

#[async_trait]
impl TableProvider for ElasticsearchTextSearchTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let effective_limit = limit.unwrap_or(self.limit);
        if effective_limit == 0 {
            let projected_schema = project_schema(&self.schema, projection)?;
            return Ok(Arc::new(datafusion::physical_plan::empty::EmptyExec::new(
                projected_schema,
            )));
        }
        let projected_schema = project_schema(&self.schema, projection)?;
        Ok(Arc::new(ElasticsearchTextSearchExec {
            client: Arc::clone(&self.client),
            index: self.index.clone(),
            search_fields: self.search_fields.clone(),
            query_text: self.query_text.clone(),
            limit: effective_limit,
            schema: Arc::clone(&self.schema),
            source_schema: Arc::clone(&self.source_schema),
            projected_schema,
            projection: projection.cloned(),
            properties: PlanProperties::new(
                EquivalenceProperties::new(project_schema(&self.schema, projection)?),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Final,
                Boundedness::Bounded,
            ),
        }))
    }
}

#[derive(Debug)]
struct ElasticsearchTextSearchExec {
    client: Arc<dyn Elasticsearch>,
    index: String,
    search_fields: Vec<String>,
    query_text: String,
    limit: usize,
    schema: SchemaRef,
    source_schema: SchemaRef,
    projected_schema: SchemaRef,
    projection: Option<Vec<usize>>,
    properties: PlanProperties,
}

impl DisplayAs for ElasticsearchTextSearchExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "ElasticsearchTextSearchExec: index={}, fields={:?}, limit={}",
            self.index, self.search_fields, self.limit
        )
    }
}

impl ExecutionPlan for ElasticsearchTextSearchExec {
    fn name(&self) -> &'static str {
        "ElasticsearchTextSearchExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        let client = Arc::clone(&self.client);
        let index = self.index.clone();
        let search_fields = self.search_fields.clone();
        let query_text = self.query_text.clone();
        let limit = self.limit;
        let schema = Arc::clone(&self.schema);
        let source_schema = Arc::clone(&self.source_schema);
        let projected_schema = Arc::clone(&self.projected_schema);
        let projection = self.projection.clone();

        let stream = futures::stream::once(async move {
            let query = if search_fields.len() == 1 {
                elasticsearch::match_query(&search_fields[0], &query_text)
            } else {
                let fields: Vec<&str> = search_fields.iter().map(String::as_str).collect();
                elasticsearch::multi_match_query(&fields, &query_text)
            };

            let req = SearchRequest {
                query: Some(query),
                size: Some(limit),
                ..Default::default()
            };

            let response = client
                .search(&index, &req)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let batch = knn_hits_to_batch(&response.hits.hits, &schema, &source_schema, None)?;

            if let Some(proj) = &projection {
                Ok(batch.project(proj)?)
            } else {
                Ok(batch)
            }
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            projected_schema,
            stream,
        )))
    }
}

/// Build a search result schema from primary key fields plus a `_score` column.
#[must_use]
pub fn search_result_schema(primary_fields: &[Field], extra_fields: &[Field]) -> SchemaRef {
    let mut fields: Vec<Field> = primary_fields.to_vec();
    fields.extend_from_slice(extra_fields);
    fields.push(Field::new(ES_SCORE_COLUMN_NAME, DataType::Float64, true));
    Arc::new(Schema::new(fields))
}
