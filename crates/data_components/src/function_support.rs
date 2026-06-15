/*
Copyright 2026 The Spice.ai OSS Authors

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

// Re-export the canonical types from datafusion-table-providers so that the
// rest of the codebase has a single definition of FunctionSupport.
pub use datafusion_table_providers::util::supported_functions::{
    FunctionRestriction, FunctionSupport, contains_unsupported_functions,
};

use datafusion::{common::Result, logical_expr::LogicalPlan};
use datafusion_federation::FederatedPlanNode;

/// If `plan` is a `FederatedPlanNode` whose inner plan contains functions that
/// are unsupported according to `function_support`, unwrap it back to the inner
/// plan so it is executed locally rather than being sent to the remote.
pub fn unfederate_plan_with_unsupported_functions(
    plan: LogicalPlan,
    function_support: &FunctionSupport,
) -> Result<LogicalPlan> {
    if let LogicalPlan::Extension(extension) = &plan
        && let Some(federated) = extension.node.as_any().downcast_ref::<FederatedPlanNode>()
        && contains_unsupported_functions(federated.plan(), function_support)?
    {
        return Ok(federated.plan().clone());
    }

    Ok(plan)
}
