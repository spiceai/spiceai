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

use datafusion::{
    common::{
        Result,
        tree_node::{TreeNode, TreeNodeRecursion},
    },
    logical_expr::{Expr, LogicalPlan},
};
use datafusion_federation::FederatedPlanNode;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FunctionRestriction {
    Allow(Vec<String>),
    Deny(Vec<String>),
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FunctionSupport {
    scalar_functions: Option<FunctionRestriction>,
    aggregate_functions: Option<FunctionRestriction>,
    window_functions: Option<FunctionRestriction>,
}

impl FunctionSupport {
    #[must_use]
    pub const fn new(
        scalar_functions: Option<FunctionRestriction>,
        aggregate_functions: Option<FunctionRestriction>,
        window_functions: Option<FunctionRestriction>,
    ) -> Self {
        Self {
            scalar_functions,
            aggregate_functions,
            window_functions,
        }
    }

    #[must_use]
    pub fn supports(&self, expr: &Expr) -> bool {
        let mut supported = true;
        let result = expr.apply(|expr| {
            if !self.supports_node(expr) {
                supported = false;
                return Ok(TreeNodeRecursion::Stop);
            }
            Ok(TreeNodeRecursion::Continue)
        });

        result.is_ok() && supported
    }

    fn supports_node(&self, expr: &Expr) -> bool {
        match expr {
            Expr::ScalarFunction(function) => {
                supports_name(self.scalar_functions.as_ref(), function.name())
            }
            Expr::AggregateFunction(function) => {
                supports_name(self.aggregate_functions.as_ref(), function.func.name())
            }
            Expr::WindowFunction(function) => {
                supports_name(self.window_functions.as_ref(), function.fun.name())
            }
            _ => true,
        }
    }
}

#[must_use]
fn supports_name(restriction: Option<&FunctionRestriction>, name: &str) -> bool {
    match restriction {
        Some(FunctionRestriction::Allow(names)) => {
            names.iter().any(|n| n.eq_ignore_ascii_case(name))
        }
        Some(FunctionRestriction::Deny(names)) => {
            !names.iter().any(|n| n.eq_ignore_ascii_case(name))
        }
        None => true,
    }
}

pub fn contains_unsupported_functions(
    plan: &LogicalPlan,
    function_support: &FunctionSupport,
) -> Result<bool> {
    let mut unsupported = false;
    for expr in plan.expressions() {
        expr.apply(|expr| {
            if !function_support.supports(expr) {
                unsupported = true;
                return Ok(TreeNodeRecursion::Stop);
            }
            Ok(TreeNodeRecursion::Continue)
        })?;

        if unsupported {
            return Ok(true);
        }
    }

    Ok(false)
}

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
