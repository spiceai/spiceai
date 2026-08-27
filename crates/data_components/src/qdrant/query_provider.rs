/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

use std::sync::Arc;

use arrow::array::{ArrayRef, FixedSizeListArray, Float32Array, Float64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::project_schema;
use datafusion::datasource::TableType;
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use datafusion::prelude::Expr;
use futures::stream::{self, unfold};
use qdrant::QdrantStore;
use qdrant::SearchResult;
use qdrant::proto::PointId;

static QDRANT_SCORE_COLUMN_NAME: &str = "_score";

/// How raw Qdrant similarity scores map onto Spice `_score` semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum QdrantScoreSemantics {
    /// Raw inner product, passed through (`dot`).
    #[default]
    Similarity,
    /// `(similarity + 1) / 2` (`cosine`), matching `1 - cosine_distance`.
    NormalizedCosine,
    /// `-distance` (`euclid`, `manhattan`).
    NegatedDistance,
}

impl QdrantScoreSemantics {
    #[must_use]
    pub fn transform(self, score: f32) -> f64 {
        let score = f64::from(score);
        match self {
            Self::Similarity => score,
            Self::NormalizedCosine => score.midpoint(1.0),
            Self::NegatedDistance => -score,
        }
    }
}

#[async_trait]
pub trait QueryEmbedder: std::fmt::Debug + Send + Sync {
    async fn embed_query(&self, query: &str) -> Result<Vec<f32>, DataFusionError>;
}

#[derive(Debug)]
pub struct QdrantQueryTable {
    client: Arc<dyn QdrantStore>,
    collection: String,
    query_vector: Vec<f32>,
    pub k: usize,
    schema: SchemaRef,
    embedding_column: String,
    dims: i32,
    query_text: Option<String>,
    embedder: Option<Arc<dyn QueryEmbedder>>,
    score_semantics: QdrantScoreSemantics,
}

impl QdrantQueryTable {
    #[must_use]
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        client: Arc<dyn QdrantStore>,
        collection: String,
        query_vector: Vec<f32>,
        k: usize,
        schema: SchemaRef,
        embedding_column: String,
        dims: i32,
        query_text: Option<String>,
        embedder: Option<Arc<dyn QueryEmbedder>>,
        score_semantics: QdrantScoreSemantics,
    ) -> Self {
        Self {
            client,
            collection,
            query_vector,
            k,
            schema,
            embedding_column,
            dims,
            query_text,
            embedder,
            score_semantics,
        }
    }
}

#[derive(Debug)]
struct QdrantQueryExec {
    client: Arc<dyn QdrantStore>,
    collection: String,
    query_vector: Vec<f32>,
    k: usize,
    schema: SchemaRef,
    embedding_column: String,
    dims: i32,
    projected_schema: SchemaRef,
    projection: Option<Vec<usize>>,
    filter: Option<qdrant::proto::Filter>,
    query_text: Option<String>,
    embedder: Option<Arc<dyn QueryEmbedder>>,
    score_semantics: QdrantScoreSemantics,
    properties: Arc<PlanProperties>,
}

impl DisplayAs for QdrantQueryExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "QdrantQueryExec: collection={}, k={}",
            self.collection, self.k
        )
    }
}

impl ExecutionPlan for QdrantQueryExec {
    fn name(&self) -> &'static str {
        "QdrantQueryExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
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
        let collection = self.collection.clone();
        let query_vector = self.query_vector.clone();
        let k = self.k;
        let schema = Arc::clone(&self.schema);
        let embedding_column = self.embedding_column.clone();
        let dims = self.dims;
        let projected_schema = Arc::clone(&self.projected_schema);
        let projection = self.projection.clone();
        let filter = self.filter.clone();
        let query_text = self.query_text.clone();
        let embedder = self.embedder.clone();
        let score_semantics = self.score_semantics;

        let stream = stream::once(async move {
            let query_vector = if let (Some(embedder), Some(query_text)) = (&embedder, &query_text)
            {
                embedder.embed_query(query_text).await?
            } else {
                query_vector
            };

            if query_vector.is_empty() {
                return Err(DataFusionError::Execution(
                    "Qdrant kNN search requires a non-empty query vector".to_string(),
                ));
            }

            let results = client
                .search(&collection, query_vector, k as u64, filter)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            let batch = search_results_to_batch(
                &results,
                &schema,
                &embedding_column,
                dims,
                score_semantics,
            )?;
            if let Some(proj) = &projection {
                batch.project(proj).map_err(DataFusionError::from)
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

#[async_trait]
impl TableProvider for QdrantQueryTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::error::Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![
            TableProviderFilterPushDown::Unsupported;
            filters.len()
        ])
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
        Ok(Arc::new(QdrantQueryExec {
            client: Arc::clone(&self.client),
            collection: self.collection.clone(),
            query_vector: self.query_vector.clone(),
            k: effective_k,
            schema: Arc::clone(&self.schema),
            embedding_column: self.embedding_column.clone(),
            dims: self.dims,
            projected_schema: Arc::clone(&projected_schema),
            projection: projection.cloned(),
            filter: None,
            query_text: self.query_text.clone(),
            embedder: self.embedder.clone(),
            score_semantics: self.score_semantics,
            properties: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(projected_schema),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Final,
                Boundedness::Bounded,
            )),
        }))
    }
}

fn search_results_to_batch(
    results: &[SearchResult],
    schema: &SchemaRef,
    embedding_column: &str,
    dims: i32,
    score_semantics: QdrantScoreSemantics,
) -> Result<RecordBatch, DataFusionError> {
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

    for field in schema.fields() {
        if field.name() == QDRANT_SCORE_COLUMN_NAME {
            let scores: Vec<Option<f64>> = results
                .iter()
                .map(|r| Some(score_semantics.transform(r.score)))
                .collect();
            columns.push(Arc::new(Float64Array::from(scores)) as ArrayRef);
        } else if field.name() == embedding_column {
            columns.push(
                Arc::new(build_embedding_array(results, embedding_column, dims)?) as ArrayRef,
            );
        } else {
            let array = build_payload_array(results, field)?;
            columns.push(array);
        }
    }

    RecordBatch::try_new(Arc::clone(schema), columns)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

#[expect(clippy::cast_sign_loss)]
fn build_embedding_array(
    results: &[SearchResult],
    embedding_column: &str,
    dims: i32,
) -> Result<ArrayRef, DataFusionError> {
    let mut flat: Vec<f32> = Vec::new();
    let mut nulls: Vec<bool> = Vec::with_capacity(results.len());
    for result in results {
        match dense_vector(result, dims) {
            Ok(vector) => {
                flat.extend(vector);
                nulls.push(true);
            }
            Err(message) if result.payload.contains_key(embedding_column) => {
                return Err(DataFusionError::Execution(message));
            }
            Err(_) => {
                flat.extend(std::iter::repeat_n(0.0f32, dims as usize));
                nulls.push(false);
            }
        }
    }
    let values_array = Arc::new(Float32Array::from(flat)) as ArrayRef;
    let list_array = FixedSizeListArray::try_new(
        Arc::new(Field::new("item", DataType::Float32, false)),
        dims,
        values_array,
        Some(arrow::buffer::NullBuffer::from(nulls)),
    )
    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
    Ok(Arc::new(list_array) as ArrayRef)
}

fn build_payload_array(
    results: &[SearchResult],
    field: &Field,
) -> Result<ArrayRef, DataFusionError> {
    let columns: Vec<ArrayRef> = results
        .iter()
        .map(|result| {
            let value = result
                .payload
                .get(field.name())
                .unwrap_or(&qdrant::proto::Value {
                    kind: Some(qdrant::proto::value::Kind::NullValue(0)),
                });
            qdrant::payload::qdrant_value_to_arrow(value, field.data_type())
        })
        .collect();
    if columns.is_empty() {
        return Ok(arrow::array::new_null_array(field.data_type(), 0) as ArrayRef);
    }
    let refs: Vec<&dyn arrow::array::Array> = columns.iter().map(AsRef::as_ref).collect();
    let combined = arrow::compute::concat(&refs)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
    Ok(combined)
}

#[derive(Debug)]
pub struct QdrantListTable {
    client: Arc<dyn QdrantStore>,
    collection: String,
    schema: SchemaRef,
    embedding_column: String,
    dims: i32,
}

impl QdrantListTable {
    #[must_use]
    pub fn new(
        client: Arc<dyn QdrantStore>,
        collection: String,
        schema: SchemaRef,
        embedding_column: String,
        dims: i32,
    ) -> Self {
        Self {
            client,
            collection,
            schema,
            embedding_column,
            dims,
        }
    }
}

#[derive(Debug)]
struct QdrantListExec {
    client: Arc<dyn QdrantStore>,
    collection: String,
    schema: SchemaRef,
    embedding_column: String,
    dims: i32,
    projected_schema: SchemaRef,
    projection: Option<Vec<usize>>,
    properties: Arc<PlanProperties>,
}

impl DisplayAs for QdrantListExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "QdrantListExec: collection={}", self.collection)
    }
}

impl ExecutionPlan for QdrantListExec {
    fn name(&self) -> &'static str {
        "QdrantListExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
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
        struct ScrollState {
            client: Arc<dyn QdrantStore>,
            collection: String,
            schema: SchemaRef,
            embedding_column: String,
            dims: i32,
            projection: Option<Vec<usize>>,
            offset: Option<PointId>,
            finished: bool,
        }

        let projected_schema = Arc::clone(&self.projected_schema);
        let projection = self.projection.clone();

        let state = ScrollState {
            client: Arc::clone(&self.client),
            collection: self.collection.clone(),
            schema: Arc::clone(&self.schema),
            embedding_column: self.embedding_column.clone(),
            dims: self.dims,
            projection,
            offset: None,
            finished: false,
        };

        let stream = unfold(state, |mut s| async move {
            if s.finished {
                return None;
            }
            match s
                .client
                .scroll(
                    &s.collection,
                    qdrant::DEFAULT_SCROLL_PAGE_SIZE,
                    s.offset.clone(),
                )
                .await
            {
                Ok(page) => {
                    let points: Vec<SearchResult> = page
                        .points
                        .iter()
                        .map(|p| {
                            let vector = p
                                .vectors
                                .as_ref()
                                .and_then(qdrant::proto::VectorsOutput::get_vector)
                                .and_then(|v| match v {
                                    qdrant::proto::vector_output::Vector::Dense(d) => Some(d.data),
                                    _ => None,
                                });
                            SearchResult {
                                id: p.id.clone().unwrap_or_default(),
                                score: 0.0,
                                payload: p.payload.clone(),
                                vector,
                            }
                        })
                        .collect();
                    let batch = search_results_to_batch(
                        &points,
                        &s.schema,
                        &s.embedding_column,
                        s.dims,
                        QdrantScoreSemantics::Similarity,
                    )
                    .and_then(|b| match &s.projection {
                        Some(proj) => b.project(proj).map_err(DataFusionError::from),
                        None => Ok(b),
                    });
                    s.offset.clone_from(&page.next_page_offset);
                    s.finished = s.offset.is_none() || page.points.is_empty();
                    Some((batch, s))
                }
                Err(e) => {
                    s.finished = true;
                    Some((Err(DataFusionError::External(Box::new(e))), s))
                }
            }
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            projected_schema,
            stream,
        )))
    }
}

#[async_trait]
impl TableProvider for QdrantListTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::error::Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![
            TableProviderFilterPushDown::Unsupported;
            filters.len()
        ])
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let projected_schema = project_schema(&self.schema, projection)?;
        Ok(Arc::new(QdrantListExec {
            client: Arc::clone(&self.client),
            collection: self.collection.clone(),
            schema: Arc::clone(&self.schema),
            embedding_column: self.embedding_column.clone(),
            dims: self.dims,
            projected_schema: Arc::clone(&projected_schema),
            projection: projection.cloned(),
            properties: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(projected_schema),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            )),
        }))
    }
}

#[expect(clippy::cast_sign_loss)]
fn dense_vector(result: &SearchResult, dims: i32) -> Result<Vec<f32>, String> {
    let Some(vector) = &result.vector else {
        return Err("Qdrant search result did not include the vector".to_string());
    };
    if vector.len() != dims as usize {
        return Err(format!(
            "Qdrant point {:?} has vector length {}, expected {dims}",
            result.id,
            vector.len()
        ));
    }
    Ok(vector.clone())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use qdrant::payload::point_id_from_values;
    use qdrant::proto::value::Kind;

    use super::*;

    fn result_with_vector(vector: Option<Vec<f32>>, with_payload: bool) -> SearchResult {
        let mut payload = HashMap::new();
        if with_payload {
            payload.insert(
                "embedding".to_string(),
                qdrant::proto::Value {
                    kind: Some(Kind::NullValue(0)),
                },
            );
        }
        SearchResult {
            id: point_id_from_values(&["a".to_string()]),
            score: 0.5,
            payload,
            vector,
        }
    }

    #[test]
    fn build_embedding_array_handles_present_missing_wrong_dims_and_empty() {
        let results = vec![
            result_with_vector(Some(vec![1.0, 2.0]), true),
            result_with_vector(None, false),
        ];
        let array = build_embedding_array(&results, "embedding", 2).expect("array");
        assert!(array.is_valid(0), "present vector must be valid");
        assert!(array.is_null(1), "absent vector must be NULL");

        let wrong = vec![result_with_vector(Some(vec![1.0]), true)];
        let err = build_embedding_array(&wrong, "embedding", 2).expect_err("dims mismatch");
        assert!(err.to_string().contains("vector length 1, expected 2"));

        let empty = build_embedding_array(&[], "embedding", 4).expect("array");
        assert_eq!(empty.len(), 0);
    }

    #[test]
    #[expect(
        clippy::float_cmp,
        reason = "the transformed values are exact midpoints/negations of binary-representable inputs, so exact equality is the point"
    )]
    fn score_semantics_transform_as_documented() {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
            QDRANT_SCORE_COLUMN_NAME,
            DataType::Float64,
            true,
        )]));
        let scores = |semantics: QdrantScoreSemantics| {
            let results = vec![SearchResult {
                score: 0.5,
                ..result_with_vector(None, false)
            }];
            let batch = search_results_to_batch(&results, &schema, "embedding", 2, semantics)
                .expect("batch");
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("f64 scores")
                .value(0)
        };
        assert_eq!(scores(QdrantScoreSemantics::Similarity), 0.5);
        assert_eq!(scores(QdrantScoreSemantics::NormalizedCosine), 0.75);
        assert_eq!(scores(QdrantScoreSemantics::NegatedDistance), -0.5);
    }
}
