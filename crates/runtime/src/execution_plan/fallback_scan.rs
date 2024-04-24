use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionState;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties,
};
use futures::{stream, StreamExt};
use std::any::Any;
use std::fmt;
use std::sync::Arc;

#[derive(Clone)]
pub struct FallbackScanParams {
    state: SessionState,
    projection: Option<Vec<usize>>,
    filters: Vec<Expr>,
    limit: Option<usize>,
}

impl FallbackScanParams {
    pub fn new(
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Self {
        Self {
            state: state.clone(),
            projection: projection.cloned(),
            filters: filters.to_vec(),
            limit,
        }
    }
}

/// `FallbackScanExec` takes an input `ExecutionPlan` and a fallback `TableProvider`.
/// If the input `ExecutionPlan` returns 0 rows, the fallback `TableProvider.scan()` is executed.
///
/// The input and fallback `ExecutionPlan` must have the same schema, execution modes and equivalence properties.
#[allow(clippy::module_name_repetitions)]
pub struct FallbackScanExec {
    /// The input execution plan.
    input: Arc<dyn ExecutionPlan>,
    /// A closure to get the fallback execution plan if needed.
    fallback_table_provider: Arc<dyn TableProvider>,
    fallback_scan_params: FallbackScanParams,
    properties: PlanProperties,
}

impl FallbackScanExec {
    /// Create a new `FallbackScanExec`.
    pub fn new(
        mut input: Arc<dyn ExecutionPlan>,
        fallback_table_provider: Arc<dyn TableProvider>,
        fallback_scan_params: FallbackScanParams,
    ) -> Self {
        let eq_properties = input.equivalence_properties().clone();
        let execution_mode = input.execution_mode();

        // Ensure the input has a single partition
        if input.output_partitioning().partition_count() != 1 {
            input = Arc::new(CoalescePartitionsExec::new(input));
        }
        Self {
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

impl fmt::Debug for FallbackScanExec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "FallbackScanExec")
    }
}

impl DisplayAs for FallbackScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        write!(f, "FallbackScanExec")
    }
}

#[async_trait]
impl ExecutionPlan for FallbackScanExec {
    fn name(&self) -> &'static str {
        "FallbackScanExec"
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

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![Arc::clone(&self.input)]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() == 1 {
            Ok(Arc::new(FallbackScanExec::new(
                Arc::clone(&children[0]),
                Arc::clone(&self.fallback_table_provider),
                self.fallback_scan_params.clone(),
            )))
        } else {
            Err(DataFusionError::Execution(
                "FallbackScanExec expects exactly one input".to_string(),
            ))
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition > 0 {
            return Err(DataFusionError::Execution(format!(
                "FallbackScanExec only supports 1 partitions, but partition {partition} was requested",
            )));
        }

        let mut input_stream = self.input.execute(0, Arc::clone(&context))?;
        let schema = input_stream.schema();

        let potentially_fallback_stream = stream::once(async move {
            let context = Arc::clone(&context);
            let schema = input_stream.schema();
            let scan_params = self.fallback_scan_params.clone();
            let fallback_provider = Arc::clone(&self.fallback_table_provider);
            // If the input_stream returns a value - then we don't need to fallback. Piece back together the input_stream.
            if let Some(input) = input_stream.next().await {
                // Add this input back to the stream
                let input_once = stream::once(async move { input });
                let stream_adapter =
                    RecordBatchStreamAdapter::new(schema, input_once.chain(input_stream));
                Box::pin(stream_adapter) as SendableRecordBatchStream
            } else {
                let fallback_plan = match fallback_provider
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

        Ok(Box::pin(stream_adapter) as SendableRecordBatchStream)
    }
}
