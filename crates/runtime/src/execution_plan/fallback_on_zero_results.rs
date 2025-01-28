/*
Copyright 2024-2025 The Spice.ai OSS Authors

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
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties,
};
use datafusion::sql::TableReference;
use futures::{stream, StreamExt};
use opentelemetry::KeyValue;
use std::any::Any;
use std::fmt;
use std::sync::Arc;

use crate::execution_plan::schema_cast::SchemaCastScanExec;
use crate::federated_table::FederatedTable;

use super::TableScanParams;

/// `FallbackOnZeroResultsScanExec` takes an input `ExecutionPlan` and a fallback `TableProvider`.
/// If the input `ExecutionPlan` returns 0 rows, the fallback `TableProvider.scan()` is executed.
///
/// The input and fallback `ExecutionPlan` must have the same schema, execution modes and equivalence properties.
pub struct FallbackOnZeroResultsScanExec {
    table_name: TableReference,
    /// The input execution plan.
    input: Arc<dyn ExecutionPlan>,
    /// A closure to get the fallback execution plan if needed.
    fallback_table_provider: Arc<FederatedTable>,
    fallback_scan_params: TableScanParams,
    properties: PlanProperties,
}

impl FallbackOnZeroResultsScanExec {
    /// Create a new `FallbackOnZeroResultsScanExec`.
    pub fn new(
        table_name: TableReference,
        mut input: Arc<dyn ExecutionPlan>,
        fallback_table_provider: Arc<FederatedTable>,
        fallback_scan_params: TableScanParams,
    ) -> Self {
        let eq_properties = input.equivalence_properties().clone();
        let execution_mode = input.execution_mode();

        // Ensure the input has a single partition
        if input.output_partitioning().partition_count() != 1 {
            input = Arc::new(CoalescePartitionsExec::new(input));
        }
        Self {
            table_name,
            input,
            fallback_table_provider,
            fallback_scan_params,
            properties: PlanProperties::new(
                eq_properties,
                Partitioning::UnknownPartitioning(1),
                execution_mode,
            ),
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

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
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
        let fallback_provider = Arc::clone(&self.fallback_table_provider);
        let table_name = self.table_name.clone();
        let fallback_msg = format!(
            r#"Accelerated table "{}" returned 0 results for query with filter [{}], sending query to federated table..."#,
            self.table_name,
            self.fallback_scan_params
                .filters
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<String>>()
                .join(", ")
        );

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
                tracing::debug!("{fallback_msg}");
                metrics::FEDERATED_FALLBACK.add(1, &[KeyValue::new("dataset_name", table_name.to_string())]);
                let federated_provider = fallback_provider.table_provider().await;
                let fallback_plan = match federated_provider
                    .scan(
                        &scan_params.state,
                        scan_params.projection.as_ref(),
                        &scan_params.filters,
                        scan_params.limit,
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

                match fallback_plan.execute(0, context) {
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
    use data_components::arrow::write::MemTable;
    use datafusion::execution::context::SessionContext;
    use datafusion::physical_plan::memory::MemoryExec;
    use std::sync::Arc;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, false),
        ]))
    }

    mod empty_fallback {
        use datafusion::catalog::TableProvider;

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
            Arc::new(
                MemoryExec::try_new(&[vec![]], schema(), None)
                    .expect("memory exec should not panic"),
            )
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
                Arc::new(FederatedTable::new(memory_table_provider())),
                TableScanParams {
                    state: ctx.state(),
                    projection: None,
                    filters: vec![],
                    limit: None,
                },
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

    mod non_empty_filtered_fallback {
        use datafusion::{
            catalog::TableProvider,
            logical_expr::{binary_expr, col, Expr, Operator},
            scalar::ScalarValue,
        };

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
            Arc::new(
                MemoryExec::try_new(&[vec![batch_input()]], schema(), None)
                    .expect("memory exec should not panic"),
            )
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
            let fallback_provider = Arc::new(FederatedTable::new(memory_table_provider()));
            let fallback_scan_params = TableScanParams {
                state: ctx.state(),
                projection: None,
                filters: vec![binary_expr(
                    col("a"),
                    Operator::Gt,
                    Expr::Literal(ScalarValue::Int64(Some(3))),
                )],
                limit: None,
            };

            let exec = FallbackOnZeroResultsScanExec::new(
                TableReference::bare("test"),
                input_plan,
                fallback_provider,
                fallback_scan_params,
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
}
