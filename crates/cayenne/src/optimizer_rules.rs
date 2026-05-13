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

//! Physical optimizer rules for Cayenne execution plans.

use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::error::DataFusionError;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::joins::HashJoinExec;
use datafusion::{error::Result, physical_plan::projection::ProjectionExec};
use datafusion_physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion_physical_plan::repartition::RepartitionExec;
use runtime_datafusion::execution_plan::schema_cast::SchemaCastScanExec;
use runtime_datafusion::extension::bytes_processed::BytesProcessedExec;
use runtime_datafusion::join_accumulator::ExactLeftAccumulator;
use std::sync::Arc;

use crate::provider::CayenneAccelerationExec;
use crate::provider::scan::IsCayenneAccelerationExec;

/// Optimizer rule that rewrites `HashJoinExec` nodes to use `ExactLeftAccumulator`
/// when the probe side is a `CayenneAccelerationExec`.
#[derive(Default)]
pub struct CayenneJoinRewriter;

impl CayenneJoinRewriter {
    /// Create a new `CayenneJoinRewriter` optimizer rule.
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl std::fmt::Debug for CayenneJoinRewriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CayenneJoinRewriter").finish()
    }
}

/// Flatten transparent nodes (like `ProjectionExec` that just pass through columns)
/// to find the underlying plan node.
fn flatten_transparent_nodes(plan: &Arc<dyn ExecutionPlan>) -> &Arc<dyn ExecutionPlan> {
    // ProjectionExec is transparent if it just passes through columns
    if let Some(projection) = plan.as_any().downcast_ref::<ProjectionExec>() {
        return flatten_transparent_nodes(projection.input());
    }

    if let Some(bytes_processed_exec) = plan.as_any().downcast_ref::<BytesProcessedExec>() {
        let children = bytes_processed_exec.children();
        let Some(input) = children.first() else {
            return plan;
        };

        return flatten_transparent_nodes(input);
    }

    if let Some(repartitioned) = plan.as_any().downcast_ref::<RepartitionExec>() {
        return flatten_transparent_nodes(repartitioned.input());
    }

    if let Some(coalesce) = plan.as_any().downcast_ref::<CoalesceBatchesExec>() {
        return flatten_transparent_nodes(coalesce.input());
    }

    if let Some(schema_cast_scan) = plan.as_any().downcast_ref::<SchemaCastScanExec>() {
        let children = schema_cast_scan.children();
        let Some(input) = children.first() else {
            return plan;
        };

        return flatten_transparent_nodes(input);
    }

    plan
}

fn hash_join_build_side_is_cayenne(join: &HashJoinExec) -> bool {
    let build_side = flatten_transparent_nodes(join.left());

    if build_side.is_cayenne_acceleration_exec() {
        true
    } else if let Some(nested_join) = build_side.as_any().downcast_ref::<HashJoinExec>() {
        // Recursively check the build side of the nested join
        hash_join_build_side_is_cayenne(nested_join)
    } else {
        false
    }
}

/// Check if the probe side of the first input `HashJoinExec` is either `CayenneAccelerationExec` or another `HashJoinExec`.
///
/// For nested hash joins, the build side of the join must also be a `CayenneAccelerationExec` as the dynamic filter from this `HashJoinExec` will push into the build side of the next join.
///
/// This handles nested join patterns like:
/// ```text
///      HashJoinExec (top)
///         | - DataSourceExec (build)
///         | - HashJoinExec (probe/nested)
///               | - DataSourceExec (build of nested)
///               | - DataSourceExec (probe of nested)
/// ```
fn is_cayenne_backed_join(hash_join: &HashJoinExec) -> bool {
    // Check the probe side first (right child)
    let probe_side = flatten_transparent_nodes(hash_join.right());

    if probe_side
        .as_any()
        .downcast_ref::<CayenneAccelerationExec>()
        .is_some()
    {
        return true;
    }

    // If probe side is another `HashJoinExec`, check the build side of the nested join is Cayenne
    if let Some(nested_join) = probe_side.as_any().downcast_ref::<HashJoinExec>() {
        // The nested join's build side must also be Cayenne
        return hash_join_build_side_is_cayenne(nested_join);
    }

    // Unknown node type on probe side - not Cayenne-backed
    false
}

impl PhysicalOptimizerRule for CayenneJoinRewriter {
    fn name(&self) -> &'static str {
        "CayenneJoinRewriter"
    }

    fn schema_check(&self) -> bool {
        false
    }

    fn optimize(
        &self,
        plan: std::sync::Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        // For each `HashJoinExec`, determine if probe side is a `CayenneAccelerationExec` with a Cayenne accelerator
        // If so, that `HashJoinExec` can be replaced with one which uses a `ExactLeftAccumulator` so we can push down exact dynamic filter bounds into Cayenne
        // The build side is irrelevant for the collection, as we only push the filter down to the probe side
        //
        // This can become more complex for plans like:
        //      `HashJoinExec`
        //         | - `CayenneAccelerationExec`
        //         | - `HashJoinExec`
        //               | - `CayenneAccelerationExec`
        //               | - `CayenneAccelerationExec`
        //
        // In this scenario, the "build side" is the very first `CayenneAccelerationExec` - the probe side becomes the remaining `HashJoinExec`, which includes the other 2 `CayenneAccelerationExec`s.
        // The dynamic filter from the top `CayenneAccelerationExec` will push down into the build side of the second `HashJoinExec`.
        // After that, the dynamic filter from the second `HashJoinExec` will push down into its probe side `CayenneAccelerationExec` - sourced from its own build-side dynamic filter.
        //
        // Therefore, after we encounter a `HashJoinExec` we need to continue traversing down the build side of any subsequent `HashJoinExec`s to ensure it is a `CayenneAccelerationExec`.

        plan.transform_down(|node| {
            let Some(hash_join) = node.as_any().downcast_ref::<HashJoinExec>() else {
                return Ok(Transformed::no(node));
            };

            if !is_cayenne_backed_join(hash_join) {
                return Ok(Transformed::no(node));
            }

            tracing::debug!(
                "Replacing HashJoinExec with ExactLeftAccumulator for Cayenne acceleration"
            );

            let new_join = hash_join.recreate_with_accumulator::<ExactLeftAccumulator>();

            Ok(Transformed::yes(Arc::new(new_join)))
        })
        .data()
    }
}

#[cfg(test)]
mod tests {
    use super::CayenneJoinRewriter;
    use crate::provider::CayenneAccelerationExec;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::{JoinType, NullEquality};
    use datafusion::config::ConfigOptions;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::physical_optimizer::PhysicalOptimizerRule;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
    use datafusion_physical_expr::expressions::col;
    use runtime_datafusion::join_accumulator::ExactLeftAccumulator;
    use std::sync::Arc;

    fn memory_exec(column_name: &str) -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            column_name,
            DataType::Int32,
            false,
        )]));
        MemorySourceConfig::try_new_exec(&[vec![]], schema, None)
            .expect("memory exec should be valid")
    }

    fn join_with_right(right: Arc<dyn ExecutionPlan>) -> HashJoinExec {
        let left = memory_exec("left_id");
        HashJoinExec::try_new(
            Arc::clone(&left),
            right,
            vec![(
                col("left_id", &left.schema()).expect("left join key should exist"),
                col("right_id", &memory_exec("right_id").schema())
                    .expect("right join key should exist"),
            )],
            None,
            &JoinType::Inner,
            None,
            PartitionMode::Partitioned,
            NullEquality::NullEqualsNothing,
        )
        .expect("hash join should be valid")
    }

    #[test]
    fn rewrites_hash_join_with_cayenne_probe_side() {
        let right = Arc::new(CayenneAccelerationExec::new(memory_exec("right_id")));
        let join = Arc::new(join_with_right(right));

        let optimized = CayenneJoinRewriter::new()
            .optimize(join, &ConfigOptions::default())
            .expect("optimizer should succeed");

        assert!(
            optimized
                .as_any()
                .downcast_ref::<HashJoinExec<ExactLeftAccumulator>>()
                .is_some(),
            "Cayenne-backed joins should use ExactLeftAccumulator"
        );
    }

    #[test]
    fn leaves_hash_join_without_cayenne_probe_side_unchanged() {
        let right = memory_exec("right_id");
        let join = Arc::new(join_with_right(right));

        let optimized = CayenneJoinRewriter::new()
            .optimize(join, &ConfigOptions::default())
            .expect("optimizer should succeed");

        assert!(
            optimized.as_any().downcast_ref::<HashJoinExec>().is_some(),
            "Non-Cayenne joins should keep the default accumulator"
        );
    }
}
