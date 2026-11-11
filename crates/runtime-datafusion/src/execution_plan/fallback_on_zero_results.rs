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

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::TableProvider;
use datafusion::common::TableReference;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties,
};
use futures::{StreamExt, stream};
use opentelemetry::KeyValue;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;

use crate::execution_plan::schema_cast::SchemaCastScanExec;

use super::TableScanParams;

/// [`FallbackAsyncTableProvider`] is a generic function type that allows the deferred construction of a [`TableProvider`].
pub type FallbackAsyncTableProvider = Arc<
    dyn Fn() -> Pin<Box<dyn Future<Output = Arc<dyn TableProvider>> + Send + Sync + 'static>>
        + Send
        + Sync,
>;

/// Builds the retention keep filters only when a zero-results fallback runs.
pub type FallbackKeepFilters = Arc<dyn Fn() -> Result<Vec<Expr>> + Send + Sync>;

/// Already-planned keep filters. Tests use this; production builds the inverse
/// lazily so an accelerator hit does not coerce or simplify a predicate.
#[must_use]
pub fn static_keep_filters(filters: Vec<Expr>) -> FallbackKeepFilters {
    Arc::new(move || Ok(filters.clone()))
}

/// `FallbackOnZeroResultsScanExec` takes an input `ExecutionPlan` and a fallback `TableProvider`.
/// If the input `ExecutionPlan` returns 0 rows, the fallback `TableProvider.scan()` is executed.
///
/// The input and fallback `ExecutionPlan` must have the same schema, execution modes and equivalence properties.
pub struct FallbackOnZeroResultsScanExec {
    table_name: TableReference,
    /// The input execution plan.
    input: Arc<dyn ExecutionPlan>,
    fallback_table_provider: FallbackAsyncTableProvider,
    fallback_scan_params: TableScanParams,
    /// Inverse of the dataset's retention delete predicates. Invoked only on
    /// the federated fallback path so an accelerator hit does not plan them.
    fallback_keep_filters: FallbackKeepFilters,
    properties: Arc<PlanProperties>,
}

impl FallbackOnZeroResultsScanExec {
    /// Create a new `FallbackOnZeroResultsScanExec`.
    pub fn new(
        table_name: TableReference,
        mut input: Arc<dyn ExecutionPlan>,
        fallback_table_provider: FallbackAsyncTableProvider,
        fallback_scan_params: TableScanParams,
        fallback_keep_filters: FallbackKeepFilters,
    ) -> Self {
        let eq_properties = input.equivalence_properties().clone();
        let emission_type = input.pipeline_behavior();
        let boundedness = input.boundedness();

        // Ensure the input has a single partition
        if input.output_partitioning().partition_count() != 1 {
            input = Arc::new(CoalescePartitionsExec::new(input));
        }
        Self {
            table_name,
            input,
            fallback_table_provider,
            fallback_scan_params,
            fallback_keep_filters,
            properties: Arc::new(PlanProperties::new(
                eq_properties,
                Partitioning::UnknownPartitioning(1),
                emission_type,
                boundedness,
            )),
        }
    }
}

impl fmt::Debug for FallbackOnZeroResultsScanExec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "FallbackOnZeroResultsScanExec")
    }
}

impl DisplayAs for FallbackOnZeroResultsScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        write!(f, "FallbackOnZeroResultsScanExec")
    }
}

#[async_trait]
impl ExecutionPlan for FallbackOnZeroResultsScanExec {
    fn name(&self) -> &'static str {
        "FallbackOnZeroResultsScanExec"
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(
            &Arc<dyn datafusion::physical_plan::PhysicalExpr>,
        ) -> datafusion::error::Result<
            datafusion::common::tree_node::TreeNodeRecursion,
        >,
    ) -> datafusion::error::Result<datafusion::common::tree_node::TreeNodeRecursion> {
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() == 1 {
            Ok(Arc::new(FallbackOnZeroResultsScanExec::new(
                self.table_name.clone(),
                Arc::clone(&children[0]),
                Arc::clone(&self.fallback_table_provider),
                self.fallback_scan_params.clone(),
                Arc::clone(&self.fallback_keep_filters),
            )))
        } else {
            Err(DataFusionError::Execution(
                "FallbackOnZeroResultsScanExec expects exactly one input".to_string(),
            ))
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        tracing::trace!(
            "Executing FallbackOnZeroResultsScanExec: partition={}",
            partition
        );
        if partition > 0 {
            return Err(DataFusionError::Execution(format!(
                "FallbackOnZeroResultsScanExec only supports 1 partitions, but partition {partition} was requested",
            )));
        }

        // The input execution plan may not support all of the push down filters, so wrap it with a `FilterExec`.

        let schema_cast = SchemaCastScanExec::new(Arc::clone(&self.input), self.schema());
        let filtered_input = super::filter_plan(Arc::new(schema_cast), &self.fallback_scan_params)?;

        let mut input_stream = filtered_input.execute(0, Arc::clone(&context))?;
        let schema = input_stream.schema();
        let scan_params = self.fallback_scan_params.clone();
        let table_name = self.table_name.clone();
        let keep_filters_fn = Arc::clone(&self.fallback_keep_filters);

        let federated_provider_callback = Arc::clone(&self.fallback_table_provider);
        let potentially_fallback_stream = stream::once(async move {
            let context = Arc::clone(&context);
            let schema = input_stream.schema();
            // If the input_stream returns a value - then we don't need to fallback. Piece back together the input_stream.
            if let Some(input) = input_stream.next().await {
                tracing::trace!("FallbackOnZeroResultsScanExec input_stream.next() returned Some()");
                match &input {
                    Ok(batch) => {
                        tracing::trace!(
                            "FallbackOnZeroResultsScanExec input_stream.next() is Ok(): num_rows: {}",
                            batch.num_rows()
                        );
                    }
                    Err(e) => {
                        tracing::trace!("FallbackOnZeroResultsScanExec input_stream.next() is Err(): {e}");
                    }
                }
                // Add this input back to the stream
                let input_once = stream::once(async move { input });
                let stream_adapter =
                    RecordBatchStreamAdapter::new(schema, input_once.chain(input_stream));
                Box::pin(stream_adapter) as SendableRecordBatchStream
            } else {
                tracing::trace!("FallbackOnZeroResultsScanExec input_stream.next() returned None");
                // Build the log message only on the empty-stream fallback path.
                let fallback_msg = format!(
                    r#"Accelerated table "{table_name}" returned 0 results for query with filter [{}], sending query to federated table..."#,
                    scan_params
                        .filters
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<String>>()
                        .join(", ")
                );
                tracing::debug!("{fallback_msg}");
                metrics::FEDERATED_FALLBACK.add(1, &[KeyValue::new("dataset_name", table_name.to_string())]);
                let keep_filters = match keep_filters_fn() {
                    Ok(filters) => filters,
                    Err(e) => {
                        let error_stream = RecordBatchStreamAdapter::new(
                            schema,
                            stream::once(async move { Err(e) }),
                        );
                        return Box::pin(error_stream) as SendableRecordBatchStream;
                    }
                };
                let federated_provider = federated_provider_callback().await;
                let fallback_optimized_plan =
                    match scan_fallback_plan(
                        federated_provider.as_ref(),
                        scan_params,
                        &keep_filters,
                        Arc::clone(&schema),
                    )
                    .await
                    {
                        Ok(plan) => plan,
                        Err(e) => {
                            let error_stream = RecordBatchStreamAdapter::new(
                                schema,
                                stream::once(async move { Err(e) }),
                            );
                            return Box::pin(error_stream) as SendableRecordBatchStream;
                        }
                    };

                tracing::trace!(
                    "FallbackOnZeroResultsScanExec fallback plan for \"{}\":\n{}",
                    table_name,
                    datafusion::physical_plan::displayable(fallback_optimized_plan.as_ref()).indent(true),
                );

                match fallback_optimized_plan.execute(0, context) {
                    Ok(stream) => stream,
                    Err(e) => {
                        // If the fallback plan fails, return an error
                        let error_stream = stream::once(async move {
                            Err(DataFusionError::Execution(format!(
                                "Error executing fallback plan: {e}"
                            )))
                        });
                        let stream_adapter = RecordBatchStreamAdapter::new(schema, error_stream);
                        Box::pin(stream_adapter) as SendableRecordBatchStream
                    }
                }
            }
        })
        .flatten();

        let stream_adapter = RecordBatchStreamAdapter::new(schema, potentially_fallback_stream);

        Ok(Box::pin(stream_adapter))
    }
}

/// Scan the federated source for a zero-results fallback.
///
/// Query filters are always re-applied as residuals so a source that cannot
/// push them down still cannot return rows the query excluded. When retention
/// has an inverse, the source is scanned unprojected so the keep predicate can
/// see columns the caller did not ask for, and those keep filters are residuals
/// too. The source's scan receives only the filters it accepts for pushdown: a
/// source may reject any other filter it is handed, and the residuals apply it
/// anyway. The result is cast to the accelerated input schema so a projection
/// that omitted a filter column still matches the caller's output.
async fn scan_fallback_plan(
    federated_provider: &dyn TableProvider,
    scan_params: TableScanParams,
    keep_filters: &[Expr],
    output_schema: SchemaRef,
) -> Result<Arc<dyn ExecutionPlan>> {
    let mut fallback_scan_params = if keep_filters.is_empty() {
        scan_params
    } else {
        scan_params
            .without_projection()
            .with_additional_filters(keep_filters)
    };
    let residual = fallback_scan_params.filters.clone();
    fallback_scan_params.filters = pushdown_filters(federated_provider, &residual)?;
    let plan = fallback_scan_params
        .scan_and_optimize(federated_provider, &residual)
        .await?;
    Ok(Arc::new(SchemaCastScanExec::new(plan, output_schema)) as Arc<dyn ExecutionPlan>)
}

/// The filters `provider` accepts for pushdown, in their original order.
fn pushdown_filters(provider: &dyn TableProvider, filters: &[Expr]) -> Result<Vec<Expr>> {
    let support = provider.supports_filters_pushdown(&filters.iter().collect::<Vec<_>>())?;
    if support.len() != filters.len() {
        return Err(DataFusionError::Internal(format!(
            "The source answered pushdown support for {} of {} fallback filters",
            support.len(),
            filters.len()
        )));
    }
    Ok(filters
        .iter()
        .zip(support)
        .filter(|(_, support)| *support != TableProviderFilterPushDown::Unsupported)
        .map(|(filter, _)| filter.clone())
        .collect())
}

mod metrics {
    use std::sync::LazyLock;

    use opentelemetry::{
        global,
        metrics::{Counter, Meter},
    };

    static METER: LazyLock<Meter> = LazyLock::new(|| global::meter("accelerated_zero_results"));

    pub(super) static FEDERATED_FALLBACK: LazyLock<Counter<u64>> = LazyLock::new(|| {
        METER
            .u64_counter("accelerated_zero_results_federated_fallback")
            .with_description("Number of times the federated table was queried due to the accelerated table returning zero results.")
            .build()
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::execution::context::SessionContext;
    use std::sync::Arc;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, false),
        ]))
    }

    fn create_fallback_provider(
        table_provider: Arc<dyn TableProvider>,
    ) -> FallbackAsyncTableProvider {
        Arc::new(move || {
            let table_provider = Arc::clone(&table_provider);
            Box::pin(async move { Arc::clone(&table_provider) })
        })
    }

    mod empty_fallback {
        use datafusion::catalog::{MemTable, TableProvider};
        use datafusion_datasource::{memory::MemorySourceConfig, source::DataSourceExec};

        use super::*;

        fn batch() -> RecordBatch {
            RecordBatch::try_new(
                schema(),
                vec![
                    Arc::new(Int64Array::from(vec![1, 2, 3])),
                    Arc::new(StringArray::from(vec!["foo", "bar", "baz"])),
                ],
            )
            .expect("record batch should not panic")
        }

        fn empty_memory_exec() -> Arc<dyn ExecutionPlan> {
            Arc::new(DataSourceExec::new(Arc::new(
                MemorySourceConfig::try_new(&[vec![]], schema(), None)
                    .expect("memory exec should not panic"),
            )))
        }

        fn memory_table_provider() -> Arc<dyn TableProvider> {
            Arc::new(
                MemTable::try_new(schema(), vec![vec![batch()]])
                    .expect("memtable should not panic"),
            )
        }

        #[tokio::test]
        async fn test_fallback_on_empty_input() {
            let ctx = SessionContext::new();

            let exec = FallbackOnZeroResultsScanExec::new(
                TableReference::bare("test"),
                empty_memory_exec(),
                create_fallback_provider(memory_table_provider()),
                TableScanParams {
                    state: Arc::new(ctx.state()),
                    projection: None,
                    filters: vec![],
                    limit: None,
                },
                static_keep_filters(vec![]),
            );

            let result_stream = exec
                .execute(0, ctx.task_ctx())
                .expect("should create stream successfully");
            let collected_result = datafusion::physical_plan::common::collect(result_stream)
                .await
                .expect("should be able to collect results");

            assert_eq!(collected_result.len(), 1);
            assert_eq!(batch().num_rows(), collected_result[0].num_rows());
        }
    }

    mod optimize_physical_plan_tests {
        use super::*;
        use datafusion::datasource::listing::PartitionedFile;
        use datafusion::datasource::physical_plan::{
            FileGroup, FileScanConfigBuilder, ParquetSource,
        };
        use datafusion::execution::config::SessionConfig;
        use datafusion::execution::object_store::ObjectStoreUrl;
        use datafusion_datasource::source::DataSourceExec;
        use object_store::path::Path;

        /// Creates a `DataSourceExec` with a single `FileGroup` containing `n` fake parquet files.
        fn single_group_parquet_exec(n: usize) -> Arc<dyn ExecutionPlan> {
            let files: Vec<PartitionedFile> = (0..n)
                .map(|i| {
                    PartitionedFile::from(object_store::ObjectMeta {
                        location: Path::from(format!("file_{i}.parquet")),
                        last_modified: chrono::DateTime::UNIX_EPOCH,
                        size: 1024,
                        e_tag: None,
                        version: None,
                    })
                })
                .collect();

            let table_schema = datafusion_datasource::TableSchema::from(schema());
            let parquet_source = ParquetSource::new(table_schema);
            let config = FileScanConfigBuilder::new(
                ObjectStoreUrl::parse("file:///").expect("valid url"),
                Arc::new(parquet_source),
            )
            .with_file_group(FileGroup::new(files))
            .build();

            DataSourceExec::from_data_source(config)
        }

        #[test]
        fn test_optimizer_splits_single_file_group() {
            let plan = single_group_parquet_exec(16);
            assert_eq!(
                plan.output_partitioning().partition_count(),
                1,
                "pre-optimization plan should have 1 partition"
            );

            let config = SessionConfig::new()
                .with_target_partitions(4)
                .with_repartition_file_min_size(0);
            let ctx = SessionContext::new_with_config(config);
            let state = ctx.state();

            let optimized = crate::execution_plan::optimize_single_partition_plan(plan, &state)
                .expect("optimization should succeed");

            let plan_display = datafusion::physical_plan::displayable(optimized.as_ref())
                .indent(true)
                .to_string();
            insta::assert_snapshot!(plan_display);
        }
    }

    mod non_empty_filtered_fallback {
        use datafusion::{
            catalog::{MemTable, TableProvider},
            logical_expr::{Expr, Operator, binary_expr, col},
            scalar::ScalarValue,
        };
        use datafusion_datasource::{memory::MemorySourceConfig, source::DataSourceExec};

        use super::*;

        fn batch_input() -> RecordBatch {
            RecordBatch::try_new(
                schema(),
                vec![
                    Arc::new(Int64Array::from(vec![1, 2, 3])),
                    Arc::new(StringArray::from(vec!["foo", "bar", "baz"])),
                ],
            )
            .expect("record batch should not panic")
        }

        fn batch_fallback() -> RecordBatch {
            RecordBatch::try_new(
                schema(),
                vec![
                    Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6])),
                    Arc::new(StringArray::from(vec![
                        "foo", "bar", "baz", "four", "five", "six",
                    ])),
                ],
            )
            .expect("record batch should not panic")
        }

        fn memory_exec() -> Arc<dyn ExecutionPlan> {
            Arc::new(DataSourceExec::new(Arc::new(
                MemorySourceConfig::try_new(&[vec![batch_input()]], schema(), None)
                    .expect("memory exec should not panic"),
            )))
        }

        fn memory_table_provider() -> Arc<dyn TableProvider> {
            Arc::new(
                MemTable::try_new(schema(), vec![vec![batch_fallback()]])
                    .expect("memtable should not panic"),
            )
        }

        #[tokio::test]
        async fn test_fallback_on_non_empty_input() {
            let ctx = SessionContext::new();

            let input_plan = memory_exec();
            let fallback_scan_params = TableScanParams {
                state: Arc::new(ctx.state()),
                projection: None,
                filters: vec![binary_expr(
                    col("a"),
                    Operator::Gt,
                    Expr::Literal(ScalarValue::Int64(Some(3)), None),
                )],
                limit: None,
            };

            let exec = FallbackOnZeroResultsScanExec::new(
                TableReference::bare("test"),
                input_plan,
                create_fallback_provider(memory_table_provider()),
                fallback_scan_params,
                static_keep_filters(vec![]),
            );

            let result_stream = exec
                .execute(0, ctx.task_ctx())
                .expect("should create stream successfully");
            let collected_result = datafusion::physical_plan::common::collect(result_stream)
                .await
                .expect("should be able to collect results");

            assert_eq!(collected_result.len(), 1);
            let batch = &collected_result[0];
            let a = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("column a");
            let b = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("column b");
            assert_eq!(a.values(), &[4, 5, 6]);
            assert_eq!(
                b.iter().flatten().collect::<Vec<_>>(),
                vec!["four", "five", "six"]
            );
        }
    }

    mod retention_keep_filters {
        use datafusion::{
            catalog::{MemTable, TableProvider},
            logical_expr::{col, lit},
        };
        use datafusion_datasource::{memory::MemorySourceConfig, source::DataSourceExec};

        use super::*;
        use crate::retention_keep::keep_expr_for_retention_delete;

        fn events_schema() -> SchemaRef {
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, false),
                Field::new("deleted", DataType::Boolean, true),
            ]))
        }

        fn source_batch() -> RecordBatch {
            RecordBatch::try_new(
                events_schema(),
                vec![
                    Arc::new(Int64Array::from(vec![1, 2, 3])),
                    Arc::new(StringArray::from(vec!["keep", "gone", "also"])),
                    Arc::new(arrow::array::BooleanArray::from(vec![
                        Some(false),
                        Some(true),
                        Some(false),
                    ])),
                ],
            )
            .expect("source batch")
        }

        fn empty_memory_exec(projection: Option<Vec<usize>>) -> Arc<dyn ExecutionPlan> {
            Arc::new(DataSourceExec::new(Arc::new(
                MemorySourceConfig::try_new(&[vec![]], events_schema(), projection)
                    .expect("empty exec"),
            )))
        }

        fn source_table() -> Arc<dyn TableProvider> {
            Arc::new(
                MemTable::try_new(events_schema(), vec![vec![source_batch()]])
                    .expect("source table"),
            )
        }

        fn batch_ids(batches: &[RecordBatch]) -> Vec<i64> {
            batches
                .iter()
                .flat_map(|batch| {
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .expect("id")
                        .values()
                        .iter()
                        .copied()
                })
                .collect()
        }

        fn batch_names(batches: &[RecordBatch], column: usize) -> Vec<String> {
            batches
                .iter()
                .flat_map(|batch| {
                    batch
                        .column(column)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .expect("name")
                        .iter()
                        .map(|value| value.expect("name is non-null").to_string())
                        .collect::<Vec<_>>()
                })
                .collect()
        }

        async fn collect_fallback(
            keep_filters: Vec<datafusion::logical_expr::Expr>,
            query_filters: Vec<datafusion::logical_expr::Expr>,
            projection: Option<Vec<usize>>,
        ) -> (SchemaRef, Vec<RecordBatch>) {
            let ctx = SessionContext::new();
            let exec = FallbackOnZeroResultsScanExec::new(
                TableReference::bare("events"),
                empty_memory_exec(projection.clone()),
                create_fallback_provider(source_table()),
                TableScanParams {
                    state: Arc::new(ctx.state()),
                    projection,
                    filters: query_filters,
                    limit: None,
                },
                static_keep_filters(keep_filters),
            );
            let schema = exec.schema();
            let stream = exec.execute(0, ctx.task_ctx()).expect("stream");
            let batches = datafusion::physical_plan::common::collect(stream)
                .await
                .expect("collect");
            (schema, batches)
        }

        async fn collect_ids(
            keep_filters: Vec<datafusion::logical_expr::Expr>,
            query_filters: Vec<datafusion::logical_expr::Expr>,
        ) -> Vec<i64> {
            let (_schema, batches) = collect_fallback(keep_filters, query_filters, None).await;
            batch_ids(&batches)
        }

        #[tokio::test]
        async fn empty_keep_returns_the_source_row() {
            let ids = collect_ids(vec![], vec![col("id").eq(lit(2i64))]).await;
            assert_eq!(
                ids,
                vec![2],
                "without a retention inverse the source still serves the soft-deleted row"
            );
        }

        #[tokio::test]
        async fn keep_filter_hides_retention_deleted_row() {
            let keep = keep_expr_for_retention_delete(col("deleted").eq(lit(true)));
            let ids = collect_ids(vec![keep], vec![col("id").eq(lit(2i64))]).await;
            assert_eq!(
                ids,
                Vec::<i64>::new(),
                "the evicted soft-deleted row must not come back from the source"
            );
        }

        #[tokio::test]
        async fn keep_filter_still_returns_rows_retention_would_keep() {
            let keep = keep_expr_for_retention_delete(col("deleted").eq(lit(true)));
            let ids = collect_ids(vec![keep], vec![col("id").eq(lit(3i64))]).await;
            assert_eq!(
                ids,
                vec![3],
                "a source row retention would keep still falls back"
            );
        }

        #[tokio::test]
        async fn keep_filter_hides_deleted_row_when_deleted_is_not_projected() {
            let keep = keep_expr_for_retention_delete(col("deleted").eq(lit(true)));
            let (schema, batches) =
                collect_fallback(vec![keep], vec![col("id").eq(lit(2i64))], Some(vec![0])).await;
            assert_eq!(schema.fields().len(), 1);
            assert_eq!(schema.field(0).name(), "id");
            assert_eq!(schema.field(0).data_type(), &DataType::Int64);
            assert_eq!(
                batch_ids(&batches),
                Vec::<i64>::new(),
                "projecting away `deleted` must not resurrect the evicted row"
            );
        }

        #[tokio::test]
        async fn projected_fallback_keeps_schema_and_values_for_retained_row() {
            let keep = keep_expr_for_retention_delete(col("deleted").eq(lit(true)));
            let (schema, batches) =
                collect_fallback(vec![keep], vec![col("id").eq(lit(3i64))], Some(vec![0, 1])).await;
            assert_eq!(
                schema
                    .fields()
                    .iter()
                    .map(|field| field.name().as_str())
                    .collect::<Vec<_>>(),
                vec!["id", "name"]
            );
            assert_eq!(schema.field(0).data_type(), &DataType::Int64);
            assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
            assert_eq!(batch_ids(&batches), vec![3]);
            assert_eq!(batch_names(&batches, 1), vec!["also".to_string()]);
        }

        fn retained_memory_exec() -> Arc<dyn ExecutionPlan> {
            Arc::new(DataSourceExec::new(Arc::new(
                MemorySourceConfig::try_new(&[vec![source_batch()]], events_schema(), None)
                    .expect("retained exec"),
            )))
        }

        #[tokio::test]
        async fn keep_builder_runs_only_when_fallback_is_selected() {
            use std::sync::atomic::{AtomicUsize, Ordering};

            async fn run(input: Arc<dyn ExecutionPlan>) -> usize {
                let calls = Arc::new(AtomicUsize::new(0));
                let keep = keep_expr_for_retention_delete(col("deleted").eq(lit(true)));
                let builder = {
                    let calls = Arc::clone(&calls);
                    Arc::new(move || {
                        calls.fetch_add(1, Ordering::SeqCst);
                        Ok(vec![keep.clone()])
                    }) as FallbackKeepFilters
                };
                let ctx = SessionContext::new();
                let exec = FallbackOnZeroResultsScanExec::new(
                    TableReference::bare("events"),
                    input,
                    create_fallback_provider(source_table()),
                    TableScanParams {
                        state: Arc::new(ctx.state()),
                        projection: None,
                        filters: vec![],
                        limit: None,
                    },
                    builder,
                );
                let stream = exec.execute(0, ctx.task_ctx()).expect("stream");
                datafusion::physical_plan::common::collect(stream)
                    .await
                    .expect("collect");
                calls.load(Ordering::SeqCst)
            }

            assert_eq!(
                run(retained_memory_exec()).await,
                0,
                "an accelerator hit must not plan the retention inverse"
            );
            assert_eq!(
                run(empty_memory_exec(None)).await,
                1,
                "fallback must plan the retention inverse once"
            );
        }
    }
}
