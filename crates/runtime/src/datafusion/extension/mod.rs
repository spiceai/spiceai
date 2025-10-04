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
use bytes_processed::{BytesProcessedExec, BytesProcessedNode};
use datafusion::{
    arrow::datatypes::Schema as ArrowSchema,
    error::Result,
    execution::context::{QueryPlanner, SessionState},
    logical_expr::{LogicalPlan, UserDefinedLogicalNode},
    physical_plan::ExecutionPlan,
    physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner},
};
use partition_ai_by_provider::{AiSourcePartitionExec, AiSourcePartitionNode};
use std::sync::Arc;

pub mod bytes_processed;
pub mod partition_ai_by_provider;

#[derive(Default)]
pub struct SpiceQueryPlanner {
    extension_planners: Vec<Arc<dyn ExtensionPlanner + Send + Sync>>,
}

impl std::fmt::Debug for SpiceQueryPlanner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpiceQueryPlanner")
            .field("extension_planners", &self.extension_planners.len())
            .finish()
    }
}

impl SpiceQueryPlanner {
    #[must_use]
    pub fn new() -> Self {
        SpiceQueryPlanner {
            extension_planners: vec![],
        }
    }

    #[must_use]
    pub fn with_extension_planners(
        mut self,
        planners: Vec<Arc<dyn ExtensionPlanner + Send + Sync>>,
    ) -> Self {
        self.extension_planners = planners;
        self
    }
}

#[async_trait]
impl QueryPlanner for SpiceQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let physical_planner =
            DefaultPhysicalPlanner::with_extension_planners(self.extension_planners.clone());
        physical_planner
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}

#[derive(Default)]
pub struct SpiceExtensionPlanner {}

impl SpiceExtensionPlanner {
    #[must_use]
    pub fn new() -> Self {
        SpiceExtensionPlanner {}
    }
}

#[async_trait]
impl ExtensionPlanner for SpiceExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        // bytes_processed Extension
        let bytes_processed_node = node.as_any().downcast_ref::<BytesProcessedNode>();
        if bytes_processed_node.is_some() {
            assert_eq!(logical_inputs.len(), 1, "should have 1 input");
            assert_eq!(physical_inputs.len(), 1, "should have 1 input");
            let physical_input = &physical_inputs[0];

            let exec_plan = Arc::new(BytesProcessedExec::new(Arc::clone(physical_input)));
            return Ok(Some(exec_plan));
        }

        // AI source partition extension
        if let Some(ai_partition_node) = node.as_any().downcast_ref::<AiSourcePartitionNode>() {
            assert_eq!(logical_inputs.len(), 1, "should have 1 input");
            assert_eq!(physical_inputs.len(), 1, "should have 1 input");

            // The AiSourcePartitionNode contains the expressions but doesn't execute them.
            // We need to create a projection that actually evaluates the AI UDFs.
            // For now, create a logical projection and let DataFusion convert it to physical.

            // Build the projection expressions in the correct order
            use datafusion::logical_expr::Projection;
            let mut proj_exprs = Vec::new();

            // Add all expressions in the order specified by field_order
            for field_name in &ai_partition_node.field_order {
                // Find the expression with this alias
                let mut found = false;

                // Check AI expressions
                for group in &ai_partition_node.source_groups {
                    for (expr, _model_name, alias) in &group.ai_exprs {
                        if alias == field_name {
                            proj_exprs.push(expr.clone().alias(field_name.clone()));
                            found = true;
                            break;
                        }
                    }
                    if found {
                        break;
                    }
                }

                // Check passthrough expressions
                if !found {
                    for (expr, alias) in &ai_partition_node.passthrough_exprs {
                        if alias == field_name {
                            proj_exprs.push(expr.clone().alias(field_name.clone()));
                            break;
                        }
                    }
                }
            }

            // Create a logical projection
            let logical_projection = LogicalPlan::Projection(Projection::try_new(
                proj_exprs,
                Arc::clone(&ai_partition_node.input),
            )?);

            // Plan the projection to physical
            let physical_projection = _planner
                .create_physical_plan(&logical_projection, _session_state)
                .await?;

            return Ok(Some(physical_projection));
        }

        Ok(None)
    }
}
