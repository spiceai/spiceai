use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::Partitioning::UnknownPartitioning;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning};
use std::any::Any;
use std::fmt;
use std::sync::Arc;

/// `SliceExec` slices an `ExecutionPlan` and returns output from a single partition.
#[allow(clippy::module_name_repetitions)]
pub struct SliceExec {
    /// The input execution plan.
    input: Arc<dyn ExecutionPlan>,
    /// The partition of the execution plan to return.
    partition: usize,
}

impl SliceExec {
    /// Create a new `SliceExec`.
    pub fn new(input: Arc<dyn ExecutionPlan>, partition: usize) -> Self {
        Self { input, partition }
    }
}

impl fmt::Debug for SliceExec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SliceExec partition: {:?}", self.partition)
    }
}

impl DisplayAs for SliceExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        write!(f, "SliceExec partition: {:?}", self.partition)
    }
}

#[async_trait]
impl ExecutionPlan for SliceExec {
    // Uncomment after upgrading to DataFusion 37
    // fn name(&self) -> &'static str {
    //     "SliceExec"
    // }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn output_partitioning(&self) -> Partitioning {
        UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() == 1 {
            Ok(Arc::new(SliceExec::new(
                children[0].clone(),
                self.partition,
            )))
        } else {
            Err(DataFusionError::Execution(
                "SliceExec expects exactly one input".to_string(),
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
                "SliceExec only supports 1 partitions, but partition {partition} was requested",
            )));
        }

        let sliced_input = self.input.execute(self.partition, context)?;

        Ok(sliced_input)
    }
}
