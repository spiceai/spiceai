/*
Copyright 2025 The Spice.ai OSS Authors

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

use crate::datafusion::DataFusion;
use async_trait::async_trait;
use ballista_core::serde::scheduler::ExecutorMetadata;
use ballista_scheduler::state::SchedulerState;
use datafusion::common::{DataFusionError, Result};
use datafusion_proto::logical_plan::AsLogicalPlan;
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::{LogicalPlanNode, PhysicalPlanNode};
use std::sync::Arc;
use std::time::Duration;

/// Some convenience methods for the `DataFusion` for accessing the scheduler state in clustered mode
#[async_trait]
pub trait DataFusionSchedulerExtensions<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> {
    fn scheduler_state(&self) -> Option<Arc<SchedulerState<T, U>>>;

    async fn executors(&self) -> Result<Vec<(ExecutorMetadata, Option<Duration>)>> {
        if let Some(scheduler_state) = self.scheduler_state() {
            scheduler_state
                .executor_manager
                .get_executor_state()
                .await
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
