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
    /// Inverse of the dataset's retention delete predicates. Applied only to
    /// the federated fallback scan so rows retention removed cannot come back
    /// from the source.
    fallback_keep_filters: Vec<datafusion::logical_expr::Expr>,
    properties: Arc<PlanProperties>,
}

impl FallbackOnZeroResultsScanExec {
    /// Create a new `FallbackOnZeroResultsScanExec`.
    pub fn new(
        table_name: TableReference,
        mut input: Arc<dyn ExecutionPlan>,
        fallback_table_provider: FallbackAsyncTableProvider,
        fallback_scan_params: TableScanParams,
        fallback_keep_filters: Vec<datafusion::logical_expr::Expr>,
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
                self.fallback_keep_filters.clone(),
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
        let keep_filters = self.fallback_keep_filters.clone();

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
                tracing::info!(
                    target: "task_history",
                    fallback = true,
                    on_zero_results = "use_source",
                    "labels"
                );
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
/// When `keep_filters` is empty the source is scanned with the caller's
/// projection and query filters only. When retention has an inverse, the
/// source is scanned unprojected so the keep predicate can see columns the
/// caller did not ask for, and every filter is re-applied as a residual so a
/// source that cannot push the keep predicate down still cannot resurrect
/// evicted rows.
async fn scan_fallback_plan(
    federated_provider: &dyn TableProvider,
    scan_params: TableScanParams,
    keep_filters: &[datafusion::logical_expr::Expr],
    output_schema: SchemaRef,
) -> Result<Arc<dyn ExecutionPlan>> {
    if keep_filters.is_empty() {
        return scan_params.scan_and_optimize(federated_provider, &[]).await;
    }

    let fallback_scan_params = scan_params
        .without_projection()
        .with_additional_filters(keep_filters);
    let residual = fallback_scan_params.filters.clone();
    let plan = fallback_scan_params
        .scan_and_optimize(federated_provider, &residual)
        .await?;
    Ok(Arc::new(SchemaCastScanExec::new(plan, output_schema)) as Arc<dyn ExecutionPlan>)
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
    use arrow::array::{Int64Array, StringArray};
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
                vec![],
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
                vec![],
            );

            let result_stream = exec
                .execute(0, ctx.task_ctx())
                .expect("should create stream successfully");
            let collected_result = datafusion::physical_plan::common::collect(result_stream)
                .await
                .expect("should be able to collect results");

            assert_eq!(collected_result.len(), 1);
            assert_eq!(batch_fallback().num_rows(), collected_result[0].num_rows());
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

        fn empty_memory_exec() -> Arc<dyn ExecutionPlan> {
            Arc::new(DataSourceExec::new(Arc::new(
                MemorySourceConfig::try_new(&[vec![]], events_schema(), None).expect("empty exec"),
            )))
        }

        fn source_table() -> Arc<dyn TableProvider> {
            Arc::new(
                MemTable::try_new(events_schema(), vec![vec![source_batch()]])
                    .expect("source table"),
            )
        }

        async fn collect_ids(
            keep_filters: Vec<datafusion::logical_expr::Expr>,
            query_filters: Vec<datafusion::logical_expr::Expr>,
        ) -> Vec<i64> {
            collect_projected_ids(keep_filters, query_filters, None).await
        }

        async fn collect_projected_ids(
            keep_filters: Vec<datafusion::logical_expr::Expr>,
            query_filters: Vec<datafusion::logical_expr::Expr>,
            projection: Option<Vec<usize>>,
        ) -> Vec<i64> {
            let ctx = SessionContext::new();
            let exec = FallbackOnZeroResultsScanExec::new(
                TableReference::bare("events"),
                empty_memory_exec(),
                create_fallback_provider(source_table()),
                TableScanParams {
                    state: Arc::new(ctx.state()),
                    projection,
                    filters: query_filters,
                    limit: None,
                },
                keep_filters,
            );
            let stream = exec.execute(0, ctx.task_ctx()).expect("stream");
            let batches = datafusion::physical_plan::common::collect(stream)
                .await
                .expect("collect");
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
            let ids =
                collect_projected_ids(vec![keep], vec![col("id").eq(lit(2i64))], Some(vec![0]))
                    .await;
            assert_eq!(
                ids,
                Vec::<i64>::new(),
                "projecting away `deleted` must not resurrect the evicted row"
            );
        }
    }
}
