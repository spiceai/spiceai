use crate::datafusion::DataFusion;
use ballista_core::serde::scheduler::ExecutorMetadata;
use ballista_scheduler::state::SchedulerState;
use datafusion::common::{DataFusionError, Result};
use datafusion_proto::logical_plan::AsLogicalPlan;
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::{LogicalPlanNode, PhysicalPlanNode};
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Handle;
use tokio::task;

/// Some convenience methods for the `DataFusion` for accessing the scheduler state in clustered mode
pub trait DataFusionSchedulerExtensions<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> {
    fn scheduler_state(&self) -> Option<Arc<SchedulerState<T, U>>>;

    fn executors(&self) -> Result<Vec<(ExecutorMetadata, Option<Duration>)>> {
        if let Some(scheduler_state) = self.scheduler_state() {
            task::block_in_place(|| {
                Handle::current().block_on(scheduler_state.executor_manager.get_executor_state())
            })
            .map_err(|e| DataFusionError::External(Box::new(e)))
        } else {
            Ok(vec![])
        }
    }
}

impl DataFusionSchedulerExtensions<LogicalPlanNode, PhysicalPlanNode> for DataFusion {
    fn scheduler_state(&self) -> Option<Arc<SchedulerState<LogicalPlanNode, PhysicalPlanNode>>> {
        self.scheduler_server
            .try_read()
            .ok()
            .and_then(|maybe_server| maybe_server.clone().map(|s| Arc::clone(&s.state)))
    }
}
