/*
Copyright 2026, Spice AI, Inc.

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

//! Stateless extension planner for all Spice DML operations.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::error::Result as DFResult;
use datafusion::execution::SessionState;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};

use crate::node::{DmlExtensionNode, DmlNodeOp};

/// Stateless extension planner for all Spice DML operations.
///
/// Matches any [`DmlExtensionNode`] and delegates to the handler embedded in
/// that node. Because the handler is carried by the node itself, one planner
/// instance can serve all registered catalog handlers.
///
/// This planner expects a concrete DML execution plan from the selected
/// handler operation. Handlers can rely on trait defaults for operations they
/// do not override.
#[derive(Debug, Default)]
pub struct DmlExtensionPlanner;

#[async_trait]
impl ExtensionPlanner for DmlExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        state: &SessionState,
    ) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
        let Some(DmlExtensionNode { op, handler, .. }) =
            node.as_any().downcast_ref::<DmlExtensionNode>()
        else {
            return Ok(None);
        };

        match op.clone() {
            DmlNodeOp::Delete(p) => handler.delete_exec(p, physical_inputs.to_vec(), state),
            DmlNodeOp::Update(p) => handler.update_exec(p, physical_inputs.to_vec(), state),
            DmlNodeOp::Insert(p) => handler.insert_exec(p, physical_inputs.to_vec(), state),
            DmlNodeOp::Merge(p) => handler.merge_exec(*p, physical_inputs.to_vec(), state),
        }
        .await
        .map(Some)
    }
}
