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

use std::{fmt::Debug, sync::Arc};

use datafusion::{
    common::tree_node::{Transformed, TransformedResult, TreeNode},
    config::ConfigOptions,
    error::DataFusionError,
    logical_expr::LogicalPlan,
    optimizer::AnalyzerRule,
};

/// Function type that returns true if the two [`LogicalPlan`]s are to be consider duplicates
pub type DuplicateNodeFn = Box<dyn Fn(&LogicalPlan, &LogicalPlan) -> bool + Send + Sync>;

/// An [`AnalyzerRule`] that can remove unnecessary, duplicate [`LogicalPlan`] nodes,
/// keeping only the bottom-most node of a given type in each subtree.
pub struct DuplicateLogicalPlanNode {
    is_duplicate: Arc<DuplicateNodeFn>,
}

impl DuplicateLogicalPlanNode {
    #[must_use] 
    pub fn extension_nodes(extension_name: &'static str) -> Self {
        Self {
            is_duplicate: Arc::new(make_duplicate_extension_checker(extension_name)),
        }
    }

    pub fn new(is_duplicate: impl Into<Arc<DuplicateNodeFn>>) -> Self {
        Self {
            is_duplicate: is_duplicate.into(),
        }
    }
}

impl Debug for DuplicateLogicalPlanNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DuplicateLogicalPlanNode")
            .finish_non_exhaustive()
    }
}

impl AnalyzerRule for DuplicateLogicalPlanNode {
    fn analyze(
        &self,
        plan: LogicalPlan,
        _config: &ConfigOptions,
    ) -> Result<LogicalPlan, DataFusionError> {
        plan.transform_up(|plan| {
            // Only process nodes with a single input
            if plan.inputs().len() != 1 {
                return Ok(Transformed::no(plan));
            }

            // Check if this node matches the duplicate criteria
            if let Some(child) = plan.inputs().first() {
                // Check if there's a matching node anywhere in the subtree below
                if has_matching_node_in_subtree(child, &plan, &self.is_duplicate) {
                    // If there's a matching node below, remove this one (keep the bottom-most)
                    Ok(Transformed::yes(plan.with_new_exprs(
                        plan.expressions(),
                        child.inputs().into_iter().cloned().collect(),
                    )?))
                } else {
                    Ok(Transformed::no(plan))
                }
            } else {
                Ok(Transformed::no(plan))
            }
        })
        .data()
    }

    fn name(&self) -> &'static str {
        "DuplicateLogicalPlanNode"
    }
}

/// Recursively checks if there's a node in the subtree that matches the given node
/// according to the duplicate function.
fn has_matching_node_in_subtree(
    subtree: &LogicalPlan,
    target: &LogicalPlan,
    is_duplicate: &DuplicateNodeFn,
) -> bool {
    // Check if the current subtree root matches
    if is_duplicate(subtree, target) {
        return true;
    }

    // Recursively check all children
    for child in subtree.inputs() {
        if has_matching_node_in_subtree(child, target, is_duplicate) {
            return true;
        }
    }

    false
}

/// Returns a [`DuplicateNodeFn`] that checks for [`datafusion::logical_expr::Extension`] nodes with the same `name`.
fn make_duplicate_extension_checker(name: &'static str) -> DuplicateNodeFn {
    Box::new(move |a: &LogicalPlan, b: &LogicalPlan| -> bool {
        match (a, b) {
            (LogicalPlan::Extension(ext_a), LogicalPlan::Extension(ext_b))
                if ext_a.node.name() == name && ext_b.node.name() == name =>
            {
                true
            }
            (_, _) => false,
        }
    })
}
