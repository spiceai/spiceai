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

//! [`TableProvider`]s over a [`MemoryVectorStore`].
//!
//! Both providers read the store lazily at `scan()` time, so rows written
//! after plan construction are visible — `VectorScanTableProvider` builds the
//! list plan once at construction, and `query_table_provider` is a sync fn
//! while query embedding is async (the embed happens inside `scan()`,
//! mirroring `S3VectorsQueryTable`).

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, FixedSizeListBuilder, Float32Array, Float32Builder, Float64Array, RecordBatch,
};
use arrow::compute::{SortOptions, concat_batches, sort_to_indices, take};
use arrow_schema::{DataType, Field, SchemaRef};
use async_trait::async_trait;
use datafusion::{
    catalog::{MemTable, Session},
    config::ConfigOptions,
    datasource::{TableProvider, TableType},
    error::{DataFusionError, Result as DataFusionResult},
    logical_expr::{ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDFImpl},
    physical_plan::ExecutionPlan,
    scalar::ScalarValue,
};
use llms::embeddings::{Embed, EmbeddingInput};
use parking_lot::RwLock;
use runtime_datafusion_udfs::{
    inner_product::InnerProduct, l2_distance::L2Distance, l2_norm::L2Norm,
};
use tokio::sync::OnceCell;

use crate::SEARCH_SCORE_COLUMN_NAME;
use crate::index::memory::{MemoryDistanceMetric, store::MemoryVectorStore};

/// Enumerates the store contents for [`crate::index::VectorIndex::list_table_provider`].
#[derive(Debug)]
pub(crate) struct MemoryVectorListTable {
    store: Arc<RwLock<MemoryVectorStore>>,
    schema: SchemaRef,
}

impl MemoryVectorListTable {
    pub(crate) fn new(store: Arc<RwLock<MemoryVectorStore>>) -> Self {
        let schema = store.read().stored_schema();
        Self { store, schema }
    }
}

#[async_trait]
impl TableProvider for MemoryVectorListTable {
    fn schema(&self) -> SchemaRef {
        SchemaRef::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let batches = self.store.read().batches();
        MemTable::try_new(self.schema(), vec![batches])?
            .scan(state, projection, filters, limit)
            .await
    }
}

/// Brute-force exact k-NN scan for [`crate::index::SearchIndex::query_table_provider`].
///
/// Embeds the query string once (lazily, at first `scan()`), scores every
/// stored row against it with the SIMD distance kernels, and returns rows
/// ordered by descending [`SEARCH_SCORE_COLUMN_NAME`].
#[derive(Debug)]
pub(crate) struct MemoryVectorQueryTable {
    index_name: String,
    store: Arc<RwLock<MemoryVectorStore>>,
    embedder: Arc<dyn Embed>,
    query: String,
    metric: MemoryDistanceMetric,
    dimension: i32,
    embedding_column_name: String,
    schema: SchemaRef,
    query_vector: OnceCell<Vec<f32>>,
}

impl MemoryVectorQueryTable {
    pub(crate) fn new(
        index_name: String,
        store: Arc<RwLock<MemoryVectorStore>>,
        embedder: Arc<dyn Embed>,
        query: String,
        metric: MemoryDistanceMetric,
        dimension: i32,
        embedding_column_name: String,
    ) -> Self {
        let stored_schema = store.read().stored_schema();
        let mut fields = stored_schema.fields().to_vec();
        fields.push(Arc::new(Field::new(
            SEARCH_SCORE_COLUMN_NAME,
            DataType::Float64,
            true,
        )));
        let schema = Arc::new(arrow_schema::Schema::new(fields));
        Self {
            index_name,
            store,
            embedder,
            query,
            metric,
            dimension,
            embedding_column_name,
            schema,
            query_vector: OnceCell::new(),
        }
    }

    /// Embed the query string, once per provider instance.
    async fn query_vector(&self) -> DataFusionResult<&Vec<f32>> {
        self.query_vector
            .get_or_try_init(|| async {
                let mut vectors = self
                    .embedder
                    .embed(EmbeddingInput::String(self.query.clone()))
                    .await
                    .map_err(|e| {
                        DataFusionError::Execution(format!(
                            "Failed to search index {} (memory): could not embed the search query: {e}",
                            self.index_name
                        ))
                    })?;
                let Some(vector) = (!vectors.is_empty()).then(|| vectors.swap_remove(0)) else {
                    return Err(DataFusionError::Execution(format!(
                        "Failed to search index {} (memory): the embedding model returned no vector for the search query",
                        self.index_name
                    )));
                };
                if vector.len() != usize::try_from(self.dimension).unwrap_or_default() {
                    return Err(DataFusionError::Execution(format!(
                        "Failed to search index {} (memory): the embedding model returned a vector of dimension {}, but the index has dimension {}",
                        self.index_name,
                        vector.len(),
                        self.dimension
                    )));
                }
                Ok(vector)
            })
            .await
    }

    /// Score one stored batch against the query vector, returning the batch
    /// with [`SEARCH_SCORE_COLUMN_NAME`] appended.
    fn score_batch(&self, batch: &RecordBatch, query: &[f32]) -> DataFusionResult<RecordBatch> {
        let (embedding_idx, _) = batch
            .schema()
            .column_with_name(&self.embedding_column_name)
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "memory vector store batch is missing embedding column '{}'",
                    self.embedding_column_name
                ))
            })?;
        let embeddings = Arc::clone(batch.column(embedding_idx));
        let scores = self.metric.score_column(&embeddings, query)?;

        let mut columns = batch.columns().to_vec();
        columns.push(Arc::new(scores) as ArrayRef);
        RecordBatch::try_new(self.schema(), columns)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
    }
}

#[async_trait]
impl TableProvider for MemoryVectorQueryTable {
    fn schema(&self) -> SchemaRef {
        SchemaRef::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        // Embed before touching the store: the lock is synchronous and must
        // never be held across an await.
        let query = self.query_vector().await?.clone();
        let batches = self.store.read().batches();

        let scored = batches
            .iter()
            .filter(|b| b.num_rows() > 0)
            .map(|b| self.score_batch(b, &query))
            .collect::<DataFusionResult<Vec<_>>>()?;
        let combined = concat_batches(&self.schema(), &scored)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

        // Order by score descending; NULL scores (undefined similarity) last.
        let (score_idx, _) = combined
            .schema()
            .column_with_name(SEARCH_SCORE_COLUMN_NAME)
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "memory vector query batch is missing '{SEARCH_SCORE_COLUMN_NAME}' column"
                ))
            })?;
        let indices = sort_to_indices(
            combined.column(score_idx),
            Some(SortOptions {
                descending: true,
                nulls_first: false,
            }),
            limit,
        )
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
        let sorted_columns = combined
            .columns()
            .iter()
            .map(|c| take(c, &indices, None))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
        let sorted = RecordBatch::try_new(self.schema(), sorted_columns)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

        MemTable::try_new(self.schema(), vec![vec![sorted]])?
            .scan(state, projection, filters, limit)
            .await
    }
}

impl MemoryDistanceMetric {
    /// Compute the search score of every row of `embeddings`
    /// (`FixedSizeList<Float32, N>`) against `query`, using the shared SIMD
    /// distance kernels. Rows with null embeddings — or an undefined
    /// similarity (e.g. cosine against a zero-magnitude vector) — score NULL.
    ///
    /// Score conventions match the other vector indexes: higher is better.
    /// Cosine → cosine similarity; L2 → negated distance; Dot → inner product.
    pub(crate) fn score_column(
        self,
        embeddings: &ArrayRef,
        query: &[f32],
    ) -> DataFusionResult<Float64Array> {
        let n = embeddings.len();
        let query_scalar = query_vector_scalar(query)?;
        match self {
            Self::Dot => {
                let dot = invoke_udf(
                    &InnerProduct::new(),
                    vec![
                        ColumnarValue::Array(Arc::clone(embeddings)),
                        ColumnarValue::Scalar(query_scalar),
                    ],
                    n,
                    &DataType::Float64,
                )?;
                as_float64(&dot).cloned()
            }
            Self::L2 => {
                let distance = invoke_udf(
                    &L2Distance::new(),
                    vec![
                        ColumnarValue::Array(Arc::clone(embeddings)),
                        ColumnarValue::Scalar(query_scalar),
                    ],
                    n,
                    &DataType::Float64,
                )?;
                Ok(as_float64(&distance)?
                    .iter()
                    .map(|d| d.map(std::ops::Neg::neg))
                    .collect())
            }
            Self::Cosine => {
                let dot = invoke_udf(
                    &InnerProduct::new(),
                    vec![
                        ColumnarValue::Array(Arc::clone(embeddings)),
                        ColumnarValue::Scalar(query_scalar),
                    ],
                    n,
                    &DataType::Float64,
                )?;
                let norms = invoke_udf(
                    &L2Norm::new(),
                    vec![ColumnarValue::Array(Arc::clone(embeddings))],
                    n,
                    &DataType::Float32,
                )?;
                let norms = norms
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .ok_or_else(|| {
                        DataFusionError::Internal(
                            "l2_norm returned a non-Float32 array".to_string(),
                        )
                    })?;
                let query_norm = query
                    .iter()
                    .map(|v| f64::from(*v) * f64::from(*v))
                    .sum::<f64>()
                    .sqrt();
                Ok(as_float64(&dot)?
                    .iter()
                    .zip(norms.iter())
                    .map(|(dot, norm)| {
                        let (Some(dot), Some(norm)) = (dot, norm) else {
                            return None;
                        };
                        let similarity = dot / (f64::from(norm) * query_norm);
                        // Zero-magnitude vectors make the similarity NaN; surface
                        // NULL rather than a NaN that would sort as a top match.
                        similarity.is_finite().then_some(similarity)
                    })
                    .collect())
            }
        }
    }
}

fn as_float64(array: &ArrayRef) -> DataFusionResult<&Float64Array> {
    array
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| {
            DataFusionError::Internal("distance kernel returned a non-Float64 array".to_string())
        })
}

/// Invoke a distance [`ScalarUDFImpl`] directly over concrete arrays,
/// bypassing plan-time coercion (the SIMD UDFs accept
/// `FixedSizeList<Float32, N>` natively).
fn invoke_udf(
    udf: &dyn ScalarUDFImpl,
    args: Vec<ColumnarValue>,
    number_rows: usize,
    return_type: &DataType,
) -> DataFusionResult<ArrayRef> {
    let result = udf.invoke_with_args(ScalarFunctionArgs {
        args,
        arg_fields: vec![],
        number_rows,
        return_field: Arc::new(Field::new("out", return_type.clone(), true)),
        config_options: Arc::new(ConfigOptions::new()),
    })?;
    match result {
        ColumnarValue::Array(array) => Ok(array),
        ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(number_rows),
    }
}

/// Build a one-row `FixedSizeList<Float32, N>` scalar from the query vector,
/// broadcastable against the stored embedding column.
fn query_vector_scalar(query: &[f32]) -> DataFusionResult<ScalarValue> {
    let dimension = i32::try_from(query.len()).map_err(|_| {
        DataFusionError::Execution(format!(
            "query vector dimension {} is too large to fit into an i32",
            query.len()
        ))
    })?;
    let mut builder = FixedSizeListBuilder::new(Float32Builder::new(), dimension)
        .with_field(Field::new_list_field(DataType::Float32, false));
    builder.values().append_slice(query);
    builder.append(true);
    Ok(ScalarValue::FixedSizeList(Arc::new(builder.finish())))
}
