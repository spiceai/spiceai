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
    Array, ArrayRef, BooleanArray, FixedSizeListBuilder, Float32Array, Float32Builder,
    Float64Array, RecordBatch,
};
use arrow::compute::{SortOptions, concat_batches, filter_record_batch, sort_to_indices, take};
use arrow_schema::{DataType, Field, SchemaRef};
use async_trait::async_trait;
use datafusion::{
    catalog::{MemTable, Session},
    common::{
        Column, DFSchema,
        tree_node::{Transformed, TreeNode},
    },
    config::ConfigOptions,
    datasource::{TableProvider, TableType},
    error::{DataFusionError, Result as DataFusionResult},
    execution::context::ExecutionProps,
    logical_expr::{
        ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDFImpl, TableProviderFilterPushDown,
    },
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

    /// Best-effort application of pushed-down ([`TableProviderFilterPushDown::Inexact`])
    /// filters, run *before* scoring and top-k truncation so the result is the top-k of
    /// the filtered rows — matching the filter-aware search of the engine-backed indexes.
    ///
    /// A filter that cannot be planned or evaluated here is skipped: `Inexact` pushdown
    /// keeps a `Filter` node above this scan, so skipping only loses prefiltering, never
    /// correctness.
    fn apply_filters(&self, filters: &[Expr], batch: RecordBatch) -> RecordBatch {
        if filters.is_empty() || batch.num_rows() == 0 {
            return batch;
        }
        let df_schema = match DFSchema::try_from(batch.schema()) {
            Ok(df_schema) => df_schema,
            Err(e) => {
                tracing::trace!(
                    "memory vector index '{}': not prefiltering pushed-down filters: {e}",
                    self.index_name
                );
                return batch;
            }
        };
        let execution_props = ExecutionProps::new();
        let mut current = batch;
        for filter in filters {
            let mask = unqualify_columns(filter.clone())
                .and_then(|filter| {
                    datafusion::physical_expr::create_physical_expr(
                        &filter,
                        &df_schema,
                        &execution_props,
                    )
                })
                .and_then(|physical| physical.evaluate(&current))
                .and_then(|value| value.into_array(current.num_rows()));
            let filtered = mask.and_then(|mask| {
                let mask = mask.as_any().downcast_ref::<BooleanArray>().ok_or_else(|| {
                    DataFusionError::Internal("filter did not evaluate to a boolean".to_string())
                })?;
                filter_record_batch(&current, mask)
                    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
            });
            match filtered {
                Ok(filtered) => current = filtered,
                Err(e) => {
                    tracing::trace!(
                        "memory vector index '{}': skipping a pushed-down filter it cannot evaluate: {e}",
                        self.index_name
                    );
                }
            }
        }
        current
    }
}

/// Rewrite every column reference in `expr` to be unqualified, so filters pushed down
/// with a scan qualifier resolve against the store's (unqualified) schema.
fn unqualify_columns(expr: Expr) -> DataFusionResult<Expr> {
    expr.transform(|e| {
        Ok(match e {
            Expr::Column(column) => {
                Transformed::yes(Expr::Column(Column::new_unqualified(column.name)))
            }
            other => Transformed::no(other),
        })
    })
    .map(|t| t.data)
}

#[async_trait]
impl TableProvider for MemoryVectorQueryTable {
    fn schema(&self) -> SchemaRef {
        SchemaRef::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    /// Filters over stored (primary-key / metadata) columns are applied inside [`Self::scan`]
    /// *before* the top-k truncation, so a filtered search returns the top-k of the rows
    /// matching the filter — not the filtered remainder of an unfiltered top-k. `Inexact`
    /// keeps a `Filter` node above the scan, so a filter the scan fails to evaluate is
    /// still enforced.
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        let stored_schema = self.store.read().stored_schema();
        Ok(filters
            .iter()
            .map(|filter| {
                let supported = filter.column_refs().iter().all(|column| {
                    column.name != self.embedding_column_name
                        && stored_schema.column_with_name(&column.name).is_some()
                });
                if supported {
                    TableProviderFilterPushDown::Inexact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
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
        let (stored_schema, batches) = {
            let store = self.store.read();
            (store.stored_schema(), store.batches())
        };

        let stored = concat_batches(
            &stored_schema,
            batches.iter().filter(|b| b.num_rows() > 0),
        )
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
        // Prefilter before scoring and truncation (see `supports_filters_pushdown`).
        let stored = self.apply_filters(filters, stored);
        let combined = if stored.num_rows() == 0 {
            RecordBatch::new_empty(self.schema())
        } else {
            self.score_batch(&stored, &query)?
        };

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

        // Filters were already applied above (best-effort) and `Inexact` pushdown keeps a
        // `Filter` above this scan — the inner `MemTable` ignores filters regardless.
        MemTable::try_new(self.schema(), vec![vec![sorted]])?
            .scan(state, projection, &[], limit)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::SearchIndex;
    use crate::index::memory::{MemoryDistanceMetric, MemoryVectorIndex};
    use crate::metadata::MetadataColumns;
    use arrow::array::{Int64Array, StringArray};
    use arrow_schema::Schema;
    use datafusion::prelude::{SessionContext, col, lit};
    use llms::embeddings::{Embed, EmbeddingInput};

    /// Maps known strings to fixed vectors so scores are deterministic under
    /// [`MemoryDistanceMetric::Dot`]: "a" scores 1, "b" scores 2, "c" scores 3
    /// against the query "q".
    #[derive(Debug)]
    struct MapEmbed;

    fn vector_for(text: &str) -> Vec<f32> {
        let magnitude = match text {
            "a" => 1.0,
            "b" => 2.0,
            "c" => 3.0,
            _ => 1.0, // the query "q"
        };
        vec![magnitude, 0.0, 0.0]
    }

    #[async_trait]
    impl Embed for MapEmbed {
        async fn embed(
            &self,
            input: EmbeddingInput,
        ) -> llms::embeddings::Result<Vec<Vec<f32>>> {
            match input {
                EmbeddingInput::String(s) => Ok(vec![vector_for(&s)]),
                EmbeddingInput::StringArray(v) => Ok(v.iter().map(|s| vector_for(s)).collect()),
                _ => Ok(vec![]),
            }
        }
    }

    async fn populated_index() -> MemoryVectorIndex {
        let index = MemoryVectorIndex::try_new(
            "content".to_string(),
            vec![Field::new("id", DataType::Int64, false)],
            MetadataColumns::none(),
            Arc::new(MapEmbed),
            3,
            MemoryDistanceMetric::Dot,
        )
        .expect("valid memory index");
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("content", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .expect("valid input batch");
        index.write(batch).await.expect("write succeeds");
        index
    }

    fn query_table(index: &MemoryVectorIndex) -> MemoryVectorQueryTable {
        MemoryVectorQueryTable::new(
            "memory_vector_index".to_string(),
            Arc::clone(&index.store),
            Arc::new(MapEmbed),
            "q".to_string(),
            MemoryDistanceMetric::Dot,
            3,
            "content_embedding".to_string(),
        )
    }

    async fn scan_ids(
        table: &MemoryVectorQueryTable,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Vec<i64> {
        let ctx = SessionContext::new();
        let state = ctx.state();
        let plan = table
            .scan(&state, None, filters, limit)
            .await
            .expect("scan builds");
        let batches = datafusion::physical_plan::collect(plan, ctx.task_ctx())
            .await
            .expect("scan executes");
        let mut ids = vec![];
        for batch in &batches {
            let id_col = batch
                .column_by_name("id")
                .expect("id column")
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("int64 ids");
            for i in 0..batch.num_rows() {
                ids.push(id_col.value(i));
            }
        }
        ids
    }

    #[tokio::test]
    async fn pushed_filters_apply_before_top_k_truncation() {
        let index = populated_index().await;
        let table = query_table(&index);

        // Unfiltered top-1 is the highest-scoring row.
        assert_eq!(scan_ids(&table, &[], Some(1)).await, vec![3]);

        // A pushed-down filter excludes the top row *before* truncation: the result is
        // the best of the matching rows, not an empty remainder of an unfiltered top-1.
        let filter = col("id").lt(lit(3_i64));
        assert_eq!(scan_ids(&table, &[filter], Some(1)).await, vec![2]);
    }

    #[tokio::test]
    async fn unevaluable_pushed_filters_are_skipped_not_fatal() {
        let index = populated_index().await;
        let table = query_table(&index);

        // A filter over a column the store does not hold cannot be evaluated; the scan
        // must skip it (DataFusion re-applies `Inexact` filters above) rather than fail.
        let filter = col("not_a_column").eq(lit("x"));
        assert_eq!(scan_ids(&table, &[filter], None).await, vec![3, 2, 1]);
    }

    #[tokio::test]
    async fn filter_pushdown_supports_only_stored_non_embedding_columns() {
        let index = populated_index().await;
        let table = query_table(&index);

        let on_pk = col("id").eq(lit(1_i64));
        let on_embedding = col("content_embedding").is_not_null();
        let on_unknown = col("elsewhere").eq(lit(1_i64));
        let support = table
            .supports_filters_pushdown(&[&on_pk, &on_embedding, &on_unknown])
            .expect("pushdown probe succeeds");
        assert_eq!(
            support,
            vec![
                TableProviderFilterPushDown::Inexact,
                TableProviderFilterPushDown::Unsupported,
                TableProviderFilterPushDown::Unsupported,
            ]
        );
    }
}
