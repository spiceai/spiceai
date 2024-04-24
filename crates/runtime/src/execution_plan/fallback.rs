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
use futures::{stream, StreamExt};
use std::any::Any;
use std::fmt;
use std::sync::{Arc, Mutex};

pub type GetFallbackPlanFn = Box<dyn FnOnce() -> Arc<dyn ExecutionPlan> + Send + Sync>;

/// `FallbackExec` takes an input `ExecutionPlan` and a fallback `ExecutionPlan`.
/// If the input `ExecutionPlan` returns 0 rows, the fallback `ExecutionPlan` is executed.
///
/// The input and fallback `ExecutionPlan` must have the same schema, execution modes and equivalence properties.
#[allow(clippy::module_name_repetitions)]
pub struct FallbackExec {
    /// The input execution plan.
    input: Arc<dyn ExecutionPlan>,
    /// A closure to get the fallback execution plan if needed.
    fallback: Mutex<Option<GetFallbackPlanFn>>,
    properties: PlanProperties,
}

impl FallbackExec {
    /// Create a new `FallbackExec`.
    pub fn new(
        mut input: Arc<dyn ExecutionPlan>,
        fallback: Mutex<Option<GetFallbackPlanFn>>,
    ) -> Self {
        let eq_properties = input.equivalence_properties().clone();
        let execution_mode = input.execution_mode();

        // Ensure the input has a single partition
        if input.output_partitioning().partition_count() != 1 {
            input = Arc::new(CoalescePartitionsExec::new(input));
        }
        Self {
            input,
            fallback,
            properties: PlanProperties::new(
                eq_properties,
                Partitioning::UnknownPartitioning(1),
                execution_mode,
            ),
        }
    }

    fn fallback_plan_fn(&self) -> Result<GetFallbackPlanFn> {
        let backup_plan_opt_fn = match self.fallback.lock() {
            Ok(mut fallback) => fallback.take(),
            Err(poisoned) => poisoned.into_inner().take(),
        };

        let Some(backup_plan_fn) = backup_plan_opt_fn else {
            return Err(DataFusionError::Execution(
                "No backup plan provided for FallbackExec, or already consumed".to_string(),
            ));
        };

        Ok(backup_plan_fn)
    }
}

impl fmt::Debug for FallbackExec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "FallbackExec")
    }
}

impl DisplayAs for FallbackExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        write!(f, "FallbackExec")
    }
}

#[async_trait]
impl ExecutionPlan for FallbackExec {
    fn name(&self) -> &'static str {
        "FallbackExec"
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
            let fallback_plan_fn = self.fallback_plan_fn()?;
            Ok(Arc::new(FallbackExec::new(
                Arc::clone(&children[0]),
                Mutex::new(Some(fallback_plan_fn)),
            )))
        } else {
            Err(DataFusionError::Execution(
                "FallbackExec expects exactly one input".to_string(),
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
                "FallbackExec only supports 1 partitions, but partition {partition} was requested",
            )));
        }

        let fallback_plan_fn = self.fallback_plan_fn()?;

        let mut input_stream = self.input.execute(0, Arc::clone(&context))?;
        let schema = input_stream.schema();

        let potentially_fallback_stream = stream::once(async move {
            let context = Arc::clone(&context);
            // If the input_stream returns a value - then we don't need to fallback. Piece back together the input_stream.
            if let Some(input) = input_stream.next().await {
                // Add this input back to the stream
                let input_once = stream::once(async move { input });
                let schema = input_stream.schema();
                let stream_adapter =
                    RecordBatchStreamAdapter::new(schema, input_once.chain(input_stream));
                Box::pin(stream_adapter) as SendableRecordBatchStream
            } else {
                let fallback_plan = fallback_plan_fn();
                match fallback_plan.execute(0, context) {
                    Ok(stream) => stream,
                    Err(e) => {
                        // If the fallback plan fails, return an error
                        let error_stream = stream::once(async move {
                            Err(DataFusionError::Execution(format!(
                                "Error executing fallback plan: {e}"
                            )))
                        });
                        let schema = input_stream.schema();
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
