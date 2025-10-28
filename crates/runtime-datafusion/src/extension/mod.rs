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

use async_trait::async_trait;
use datafusion::{
    error::Result,
    execution::context::{QueryPlanner, SessionState},
    logical_expr::LogicalPlan,
    physical_plan::ExecutionPlan,
    physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner},
};
use std::sync::Arc;

pub mod bytes_processed;

/// [`ExtensionPlanQueryPlanner`] implements [`QueryPlanner`] with a set of [`ExtensionPlanner`].
///
/// It provides all [`ExtensionPlanner`]s to [`DefaultPhysicalPlanner`] during [`QueryPlanner::create_physical_plan`].
pub struct ExtensionPlanQueryPlanner {
    physical_planner: Arc<dyn PhysicalPlanner>,
}

impl std::fmt::Debug for ExtensionPlanQueryPlanner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExtensionPlanQueryPlanner").finish()
    }
}
impl Default for ExtensionPlanQueryPlanner {
    fn default() -> Self {
        Self {
            physical_planner: Arc::new(DefaultPhysicalPlanner::default()),
        }
    }
}

impl ExtensionPlanQueryPlanner {
    #[must_use]
    pub fn from_extension_planners(planners: Vec<Arc<dyn ExtensionPlanner + Send + Sync>>) -> Self {
        Self {
            physical_planner: Arc::new(DefaultPhysicalPlanner::with_extension_planners(planners)),
        }
    }
}

#[async_trait]
impl QueryPlanner for ExtensionPlanQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.physical_planner
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}
