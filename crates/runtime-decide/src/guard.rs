/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! Keeps `DataFusion`'s leaf-expression pushdown away from async function calls.
//!
//! `extract_leaf_expressions` and `push_down_leaf_projections` move a field access such
//! as `get_field(col, 'a')` toward the scan, and to do so they inline the expression the
//! column comes from, without checking that it is volatile. When that expression is an
//! async call — a model request — it is moved below the plan's filters and runs on
//! every row the filters would have dropped. For a decision this turns ten model
//! requests into a thousand.

use std::sync::Arc;

use datafusion::common::Result;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::logical_expr::{Expr, LogicalPlan};
use datafusion::optimizer::optimizer::ApplyOrder;
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};

/// The rules that inline an expression to push a field access toward the scan.
const LEAF_PUSHDOWN_RULES: [&str; 2] = ["extract_leaf_expressions", "push_down_leaf_projections"];

/// Wraps the leaf-expression pushdown rules in `rules` so they leave alone any plan
/// that calls an async function; every other rule is returned as is. Adjacent pushdown
/// rules share one wrapper, so the plan is checked once for them per optimizer pass.
#[must_use]
pub fn guard_async_calls(
    rules: Vec<Arc<dyn OptimizerRule + Send + Sync>>,
) -> Vec<Arc<dyn OptimizerRule + Send + Sync>> {
    let mut guarded: Vec<Arc<dyn OptimizerRule + Send + Sync>> = Vec::with_capacity(rules.len());
    let mut pending: Vec<Arc<dyn OptimizerRule + Send + Sync>> = Vec::new();
    for rule in rules {
        if LEAF_PUSHDOWN_RULES.contains(&rule.name()) {
            pending.push(rule);
            continue;
        }
        if !pending.is_empty() {
            guarded.push(Arc::new(SkipWithAsyncCalls::new(std::mem::take(
                &mut pending,
            ))));
        }
        guarded.push(rule);
    }
    if !pending.is_empty() {
        guarded.push(Arc::new(SkipWithAsyncCalls::new(pending)));
    }
    guarded
}

/// Runs its rules, each in its own order, on plans without an async call.
#[derive(Debug)]
struct SkipWithAsyncCalls {
    name: String,
    rules: Vec<Arc<dyn OptimizerRule + Send + Sync>>,
}

impl SkipWithAsyncCalls {
    fn new(rules: Vec<Arc<dyn OptimizerRule + Send + Sync>>) -> Self {
        let name = rules
            .iter()
            .map(|rule| rule.name())
            .collect::<Vec<_>>()
            .join("+");
        Self { name, rules }
    }
}

impl OptimizerRule for SkipWithAsyncCalls {
    fn name(&self) -> &str {
        &self.name
    }

    /// The guard walks the plan itself: one check of the whole plan, then each rule in
    /// its own order. Letting the optimizer drive the walk would repeat the check at
    /// every node, once per subtree.
    fn apply_order(&self) -> Option<ApplyOrder> {
        None
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        if calls_async_function(&plan) {
            return Ok(Transformed::no(plan));
        }
        let mut result = Transformed::no(plan);
        for rule in &self.rules {
            result = result.transform_data(|plan| match rule.apply_order() {
                Some(ApplyOrder::TopDown) => {
                    plan.transform_down_with_subqueries(|node| rule.rewrite(node, config))
                }
                Some(ApplyOrder::BottomUp) => {
                    plan.transform_up_with_subqueries(|node| rule.rewrite(node, config))
                }
                None => rule.rewrite(plan, config),
            })?;
        }
        Ok(result)
    }
}

/// Whether `plan`, including its subqueries, calls an async function anywhere.
fn calls_async_function(plan: &LogicalPlan) -> bool {
    let mut found = false;
    let _ = plan.apply_with_subqueries(|node| {
        node.apply_expressions(|expr| {
            found = expr
                .exists(|e| {
                    Ok(matches!(e, Expr::ScalarFunction(function) if function.func.as_async().is_some()))
                })
                .unwrap_or(false);
            Ok(if found {
                TreeNodeRecursion::Stop
            } else {
                TreeNodeRecursion::Continue
            })
        })?;
        Ok(if found {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        })
    });
    found
}
