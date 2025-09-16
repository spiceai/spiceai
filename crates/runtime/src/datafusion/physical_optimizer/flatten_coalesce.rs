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
use datafusion::common::DataFusionError;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use std::sync::Arc;

#[derive(Debug)]
pub struct FlattenCoalesce {}

impl FlattenCoalesce {
    fn coalesce_batches_exec_node(node: &Arc<dyn ExecutionPlan>) -> Option<CoalesceBatchesExec> {
        node.as_any().downcast_ref::<CoalesceBatchesExec>().cloned()
    }

    fn coalesce_batches_are_equivalent(a: &CoalesceBatchesExec, b: &CoalesceBatchesExec) -> bool {
        a.target_batch_size() == b.target_batch_size() && a.fetch() == b.fetch()
    }
}

impl PhysicalOptimizerRule for FlattenCoalesce {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<(dyn ExecutionPlan + 'static)>, DataFusionError> {
        let transformed = plan.transform_down(|plan| {
            if let Some(outer_coalesce) = Self::coalesce_batches_exec_node(&plan) {
                let mut current = outer_coalesce;
                let mut transformed = false;

                // Support arbitrarily nested CoalesceBatchesExec nodes, but only proceed if the
                // parameters for both nodes are the same
                while let Some(input_plan) = Self::coalesce_batches_exec_node(current.input()) {
                    if Self::coalesce_batches_are_equivalent(&current, &input_plan) {
                        transformed = true;
                        current = input_plan;
                    } else {
                        break;
                    }
                }

                if transformed {
                    return Ok(Transformed::yes(Arc::new(current)));
                }
            }

            Ok(Transformed::no(plan))
        })?;

        Ok(transformed.data)
    }

    fn name(&self) -> &'static str {
        "spice_flatten_coalesce"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use arrow_schema::DataType;
    use arrow::datatypes::Field;
    use datafusion::physical_plan::expressions::col;
    use crate::datafusion::physical_optimizer::flatten_coalesce::FlattenCoalesce;
    use arrow_schema::Schema;
    use datafusion::config::ConfigOptions;
    use datafusion::physical_optimizer::PhysicalOptimizerRule;
    use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::{ExecutionPlan, displayable};
    use std::sync::Arc;
    use datafusion::common::NullEquality;
    use datafusion::execution::SessionState;
    use datafusion::physical_optimizer::optimizer::PhysicalOptimizer;
    use datafusion::physical_plan::joins::{HashJoinExec, JoinOn, PartitionMode};
    use datafusion::physical_planner::DefaultPhysicalPlanner;
    use datafusion::prelude::SessionContext;
    use datafusion_expr::JoinType;
    use tracing::field::display;

    #[test]
    fn test_flatten_coalesce() {
        // Make a plan with 3x CoalesceBatchesExec
        let empty_exec = Arc::new(EmptyExec::new(Arc::new(Schema::empty())));
        let inner: Arc<dyn ExecutionPlan> = Arc::new(CoalesceBatchesExec::new(empty_exec, 2));
        let middle = Arc::new(CoalesceBatchesExec::new(Arc::clone(&inner), 2));
        let outer = Arc::new(CoalesceBatchesExec::new(middle, 2));

        let optimizer: Arc<dyn PhysicalOptimizerRule> = Arc::new(FlattenCoalesce {});
        let optimized_plan = optimizer
            .optimize(outer, &ConfigOptions::new())
            .expect("Must optimize plan");

        // Ensure that the post-optimize plan is equivalent to 1x CoalesceBatchesExec
        assert_eq!(
            displayable(optimized_plan.as_ref())
                .indent(true)
                .to_string(),
            displayable(inner.as_ref()).indent(true).to_string()
        );
    }

    #[test]
    fn test_do_not_flatten() {
        // Make a plan with 3x CoalesceBatchesExec, where one in the chain has a different parameter
        let empty_exec = Arc::new(EmptyExec::new(Arc::new(Schema::empty())));
        let inner = Arc::new(CoalesceBatchesExec::new(empty_exec, 2));
        let middle = Arc::new(CoalesceBatchesExec::new(inner, 2).with_fetch(Some(2)));
        let outer: Arc<dyn ExecutionPlan> = Arc::new(CoalesceBatchesExec::new(middle, 2));

        let optimizer: Arc<dyn PhysicalOptimizerRule> = Arc::new(FlattenCoalesce {});
        let optimized_plan = optimizer
            .optimize(Arc::clone(&outer), &ConfigOptions::new())
            .expect("Must optimize plan");

        // Ensure that the post-optimize plan is equivalent to the input plan, because we cannot optimize
        assert_eq!(
            displayable(optimized_plan.as_ref())
                .indent(true)
                .to_string(),
            displayable(outer.as_ref()).indent(true).to_string()
        );
    }

    #[test]
    fn test_repeat_optimize() {
        // Make a plan with 3x CoalesceBatchesExec, where one in the chain has a different parameter
        let schema = Arc::new(Schema::new(vec![
            Field::new("foo", DataType::Int32, false),
        ]));

        let empty_left = Arc::new(EmptyExec::new(Arc::clone(&schema)));
        let empty_right = Arc::new(EmptyExec::new(Arc::clone(&schema)));
        let hash_join_exec: Arc<dyn ExecutionPlan> = Arc::new(HashJoinExec::try_new(
            empty_left,
            empty_right,
            JoinOn::from(&[(
                col("foo", schema.as_ref()).expect("must foo"),
                col("foo", schema.as_ref()).expect("must foo")
                )]),
            None,
            &JoinType::Full,
            None,
            PartitionMode::Auto,
            NullEquality::NullEqualsNothing
        ).expect("Should be able to create hash join exec"));

        let planner = DefaultPhysicalPlanner::default();
        let context = SessionContext::new();

        (0..3).fold(hash_join_exec, |plan, i| {
            let optimized = planner.optimize_physical_plan(plan, &context.state(), |_a, _b| {}).unwrap();
            println!("Optimization pass {i}\n{}", displayable(optimized.as_ref()).indent(true));
            optimized
        });
    }
}
