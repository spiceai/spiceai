use crate::datafusion::DataFusion;
use ballista_core::serde::scheduler::ExecutorMetadata;
use ballista_scheduler::state::SchedulerState;
use datafusion::common::{DataFusionError, Result, exec_err};
use datafusion_proto::logical_plan::AsLogicalPlan;
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::{LogicalPlanNode, PhysicalPlanNode};
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Handle;
use tokio::task;

pub trait DataFusionSchedulerExtensions<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> {
    fn scheduler_state(&self) -> Result<Arc<SchedulerState<T, U>>>;

    fn executors(&self) -> Result<Vec<(ExecutorMetadata, Option<Duration>)>> {
        task::block_in_place(|| {
            Handle::current().block_on(
                self.scheduler_state()?
                    .executor_manager
                    .get_executor_state(),
            )
        })
        .map_err(|e| DataFusionError::External(Box::new(e)))
    }
}

impl DataFusionSchedulerExtensions<LogicalPlanNode, PhysicalPlanNode> for DataFusion {
    fn scheduler_state(&self) -> Result<Arc<SchedulerState<LogicalPlanNode, PhysicalPlanNode>>> {
        let state = self
            .scheduler_state
            .try_read()
            .map_err(|_| DataFusionError::External("Unable to read scheduler state".into()))?;

        if let Some(state) = state.as_ref() {
            Ok(Arc::clone(state))
        } else {
            exec_err!("Unable to read scheduler state")
        }
    }
}
