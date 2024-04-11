use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::Partitioning::UnknownPartitioning;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning};
use std::any::Any;
use std::fmt;
use std::sync::Arc;

/// `TeeExec` duplicates the output of an execution plan into N partitions.
#[allow(clippy::module_name_repetitions)]
pub struct TeeExec {
    /// The input execution plan.
    input: Arc<dyn ExecutionPlan>,
    /// The number of times to duplicate the output.
    n: usize,
}

impl TeeExec {
    /// Create a new `TeeExec`.
    pub fn new(input: Arc<dyn ExecutionPlan>, n: usize) -> Self {
        Self { input, n }
    }
}

impl fmt::Debug for TeeExec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TeeExec n_partitions: {:?}", self.n)
    }
}

impl DisplayAs for TeeExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        write!(f, "TeeExec n_partitions: {:?}", self.n)
    }
}

#[async_trait]
impl ExecutionPlan for TeeExec {
    // Uncomment after upgrading to DataFusion 37
    // fn name(&self) -> &'static str {
    //     "TeeExec"
    // }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn output_partitioning(&self) -> Partitioning {
        UnknownPartitioning(self.n)
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
            Ok(Arc::new(TeeExec::new(children[0].clone(), self.n)))
        } else {
            Err(DataFusionError::Execution(
                "TeeExec expects exactly one input".to_string(),
            ))
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition >= self.n {
            return Err(DataFusionError::Execution(format!(
                "TeeExec only supports {} partitions, but partition {} was requested",
                self.n, partition
            )));
        }

        // Coalesce the input into a single partition
        let coalesce_plan = CoalescePartitionsExec::new(self.input.clone());
        let single_partition = coalesce_plan.execute(0, context)?;

        Ok(single_partition)
    }
}
