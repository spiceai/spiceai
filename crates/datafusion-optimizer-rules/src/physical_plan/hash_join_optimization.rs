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

//!  [`EmptyHashJoinExecPhysicalOptimization`] removes redundant (empty result) [`HashJoinExec`] from [`ExecutionPlan`]s.

use std::sync::Arc;

use datafusion::{
    common::{
        stats::Precision,
        tree_node::{Transformed, TransformedResult, TreeNode},
    },
    config::ConfigOptions,
    error::DataFusionError,
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{ExecutionPlan, empty::EmptyExec, joins::HashJoinExec},
};
use datafusion_expr::JoinType;

/// A [`PhysicalOptimizerRule`] that checks the [`JoinType`] and child [`ExecutionPlan`] of [`HashJoinExec`]s, and if applicable, replaces the entire [`HashJoinExec`] with a [`EmptyExec`].
///
/// A [`EmptyExec`] can be used if the associated child [`ExecutionPlan`] is guaranteed to have no rows (using [`ExecutionPlan::partition_statistics`]).
#[derive(Debug)]
pub struct EmptyHashJoinExecPhysicalOptimization {}

impl PhysicalOptimizerRule for EmptyHashJoinExecPhysicalOptimization {
    fn optimize(
        &self,
        plan: std::sync::Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        plan.transform_down(|plan| {
            let Some(join_exec) = plan.as_any().downcast_ref::<HashJoinExec>() else {
                return Ok(Transformed::no(plan));
            };

            let is_empty = match join_exec.join_type {
                JoinType::Left | JoinType::RightSemi | JoinType::LeftAnti | JoinType::LeftMark => {
                    guaranteed_empty(join_exec.left())
                }
                JoinType::Right
                | JoinType::LeftSemi
                | JoinType::RightAnti
                | JoinType::RightMark => guaranteed_empty(join_exec.right()),
                JoinType::Inner => {
                    guaranteed_empty(join_exec.left()) || guaranteed_empty(join_exec.right())
                }
                JoinType::Full => {
                    guaranteed_empty(join_exec.left()) && guaranteed_empty(join_exec.right())
                }
            };

            if !is_empty {
                return Ok(Transformed::no(plan));
            }

            Ok(Transformed::yes(Arc::new(EmptyExec::new(
                join_exec.schema(),
            ))))
        })
        .data()
    }

    fn name(&self) -> &'static str {
        "EmptyHashJoinExecPhysicalOptimization"
    }

    fn schema_check(&self) -> bool {
        false
    }
}

fn guaranteed_empty(plan: &Arc<dyn ExecutionPlan>) -> bool {
    let Ok(stats) = plan.partition_statistics(None) else {
        return false;
    };
    match stats.num_rows {
        Precision::Exact(n) => n == 0,
        _ => false,
    }
}
