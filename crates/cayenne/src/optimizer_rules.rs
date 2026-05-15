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

use datafusion::common::NullEquality;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::error::DataFusionError;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::joins::HashJoinExec;
use datafusion::{error::Result, physical_plan::projection::ProjectionExec};
use datafusion_physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
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

/// Flatten transparent nodes (like `ProjectionExec` that just pass through columns,
/// `CoalescePartitionsExec` for merging partitions without altering columns, etc.)
/// to find the underlying plan node.
///
/// This is used by the Cayenne join rewriter to reliably detect whether a join's
/// probe (or nested build) side is ultimately backed by a `CayenneAccelerationExec`,
/// even when DataFusion or the runtime inserts transparent wrapper nodes for
/// batch coalescing, repartitioning, schema casting, bytes tracking, or partition
/// coalescing.
///
/// # Correctness (ACID Consistency for Cayenne-backed queries)
///
/// The `CayenneJoinRewriter` *only* rewrites a `HashJoinExec` to use
/// `ExactLeftAccumulator` (enabling precise dynamic filter pushdown into Cayenne
/// scans) when this function successfully uncovers a `CayenneAccelerationExec`.
///
/// If a transparent wrapper is missed:
/// - The rewriter silently skips the rewrite.
/// - Default DataFusion accumulator (range/bloom approx) is used instead.
/// - For join types that rely on exact "not in" / "in" sets from the build side
///   (LeftAnti, LeftSemi, RightAnti, and Q21-style "suppliers with no lineitems"
///   anti-join patterns), this can produce **incorrect query results** (missing or
///   extraneous rows) or at minimum non-exact filters that violate the "exact"
///   contract the Cayenne probe expects.
///
/// **Devil's advocate review**: One could argue "our plans never insert unknown
/// wrappers today, and recursion is fine because plan trees have no cycles."
/// Counter-argument (to be *really sure*): 
/// - DataFusion versions or future runtime extensions *can* insert additional
///   passthrough nodes (e.g. new Coalesce*Exec, SortPreservingMerge in some paths,
///   custom telemetry wrappers).
/// - A single missed wrapper = silent correctness regression for any user query
///   that happens to hit that plan shape with Cayenne + qualifying join.
/// - Deep wrapper nesting (possible via generated SQL or many optimizer rules)
///   could in theory exhaust recursion stack; iterative loop removes that risk.
///
/// Therefore the list of handled wrappers **must** be extended when new transparent
/// execs are added anywhere in the physical plan pipeline. This function is a
/// critical correctness gate, not just a perf heuristic.
///
/// The implementation uses an iterative loop (not recursion) for robustness.
fn flatten_transparent_nodes(plan: &Arc<dyn ExecutionPlan>) -> &Arc<dyn ExecutionPlan> {
    let mut current = plan;
    loop {
        // ProjectionExec is transparent if it just passes through columns
        if let Some(projection) = current.as_any().downcast_ref::<ProjectionExec>() {
            current = projection.input();
            continue;
        }

        if let Some(bytes_processed_exec) = current.as_any().downcast_ref::<BytesProcessedExec>() {
            let children = bytes_processed_exec.children();
            let Some(input) = children.first() else {
                return current;
            };
            current = input;
            continue;
        }

        if let Some(repartitioned) = current.as_any().downcast_ref::<RepartitionExec>() {
            current = repartitioned.input();
            continue;
        }

        if let Some(coalesce) = current.as_any().downcast_ref::<CoalesceBatchesExec>() {
            current = coalesce.input();
            continue;
        }

        if let Some(schema_cast_scan) = current.as_any().downcast_ref::<SchemaCastScanExec>() {
            let children = schema_cast_scan.children();
            let Some(input) = children.first() else {
                return current;
            };
            current = input;
            continue;
        }

        if let Some(coalesce_parts) = current.as_any().downcast_ref::<CoalescePartitionsExec>() {
            let children = coalesce_parts.children();
            let Some(input) = children.first() else {
                return current;
            };
            current = input;
            continue;
        }

        break;
    }
    current
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

            if hash_join.null_equality() != NullEquality::NullEqualsNothing {
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
    use super::{CayenneJoinRewriter, CoalescePartitionsExec};
    use crate::provider::CayenneAccelerationExec;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::{JoinType, NullEquality};
    use datafusion::config::ConfigOptions;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::physical_optimizer::PhysicalOptimizerRule;
    use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
    use datafusion::physical_plan::projection::ProjectionExec;
    use datafusion::physical_plan::{ExecutionPlan, displayable};
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

    fn hash_join(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        left_column: &str,
        right_column: &str,
    ) -> HashJoinExec {
        hash_join_with_null_equality(
            left,
            right,
            left_column,
            right_column,
            NullEquality::NullEqualsNothing,
        )
    }

    fn hash_join_with_null_equality(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        left_column: &str,
        right_column: &str,
        null_equality: NullEquality,
    ) -> HashJoinExec {
        let left_key = col(left_column, &left.schema()).expect("left join key should exist");
        let right_key = col(right_column, &right.schema()).expect("right join key should exist");

        HashJoinExec::try_new(
            left,
            right,
            vec![(left_key, right_key)],
            None,
            &JoinType::Inner,
            None,
            PartitionMode::Partitioned,
            null_equality,
        )
        .expect("hash join should be valid")
    }

    fn join_with_right(right: Arc<dyn ExecutionPlan>) -> HashJoinExec {
        hash_join(memory_exec("left_id"), right, "left_id", "right_id")
    }

    fn optimize(plan: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        CayenneJoinRewriter::new()
            .optimize(plan, &ConfigOptions::default())
            .expect("optimizer should succeed")
    }

    fn plan_snapshot(plan: &Arc<dyn ExecutionPlan>) -> String {
        displayable(plan.as_ref()).indent(true).to_string()
    }

    #[test]
    fn rewrites_hash_join_with_cayenne_probe_side() {
        let right = Arc::new(CayenneAccelerationExec::new(memory_exec("right_id")));
        let join = Arc::new(join_with_right(right));

        let optimized = optimize(join);

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

        let optimized = optimize(join);

        assert!(
            optimized.as_any().downcast_ref::<HashJoinExec>().is_some(),
            "Non-Cayenne joins should keep the default accumulator"
        );
    }

    #[test]
    fn leaves_null_equal_hash_join_unchanged() {
        let left = memory_exec("left_id");
        let right = Arc::new(CayenneAccelerationExec::new(memory_exec("right_id")));
        let join = Arc::new(hash_join_with_null_equality(
            left,
            right,
            "left_id",
            "right_id",
            NullEquality::NullEqualsNull,
        ));

        let optimized = optimize(join);

        assert!(
            optimized.as_any().downcast_ref::<HashJoinExec>().is_some(),
            "Null-equal joins should keep the default accumulator to preserve probe NULL matches"
        );
    }

    #[test]
    fn rewrites_hash_join_through_transparent_projection() {
        let right_input = Arc::new(CayenneAccelerationExec::new(memory_exec("right_id")));
        let right_schema = right_input.schema();
        let right = Arc::new(
            ProjectionExec::try_new(
                vec![(
                    col("right_id", &right_schema).expect("projection column should exist"),
                    "right_id".to_string(),
                )],
                right_input,
            )
            .expect("projection should be valid"),
        );
        let join = Arc::new(join_with_right(right));

        let optimized = optimize(join);

        assert!(
            optimized
                .as_any()
                .downcast_ref::<HashJoinExec<ExactLeftAccumulator>>()
                .is_some(),
            "Transparent wrappers over Cayenne scans should still use ExactLeftAccumulator"
        );
    }

    #[test]
    fn rewrites_hash_join_through_coalesce_partitions() {
        // CoalescePartitionsExec is a transparent wrapper (used in Cayenne scan paths
        // for local limit + global limit plans) that must be stripped for correct
        // detection of Cayenne-backed probe sides. Missing it would silently skip
        // ExactLeftAccumulator rewrite for any plan containing a coalesced probe join,
        // risking inexact filters and incorrect results on LeftAnti / anti-join queries.
        let right_input = Arc::new(CayenneAccelerationExec::new(memory_exec("right_id")));
        let right = Arc::new(CoalescePartitionsExec::new(right_input));
        let join = Arc::new(join_with_right(right));

        let optimized = optimize(join);

        assert!(
            optimized
                .as_any()
                .downcast_ref::<HashJoinExec<ExactLeftAccumulator>>()
                .is_some(),
            "CoalescePartitionsExec over Cayenne must still trigger ExactLeftAccumulator (ACID consistency gate)"
        );
    }

    #[test]
    fn rewrites_hash_join_through_stacked_transparent_wrappers() {
        // Edge case: multiple stacked transparent nodes (projection + coalesce partitions)
        // as can occur in real plans with limits, projections, and partition-aware scans.
        let right_input = Arc::new(CayenneAccelerationExec::new(memory_exec("right_id")));
        let right_schema = right_input.schema();
        let projected = Arc::new(
            ProjectionExec::try_new(
                vec![(
                    col("right_id", &right_schema).expect("projection column should exist"),
                    "right_id".to_string(),
                )],
                right_input,
            )
            .expect("projection should be valid"),
        );
        let coalesced = Arc::new(CoalescePartitionsExec::new(projected));
        let join = Arc::new(join_with_right(coalesced));

        let optimized = optimize(join);

        assert!(
            optimized
                .as_any()
                .downcast_ref::<HashJoinExec<ExactLeftAccumulator>>()
                .is_some(),
            "Stacked transparent wrappers (Projection + CoalescePartitions) over Cayenne must be fully flattened"
        );
    }

    #[test]
    fn rewrites_nested_cayenne_probe_join_chain() {
        let nested_left = Arc::new(CayenneAccelerationExec::new(memory_exec("nested_left_id")));
        let nested_right = Arc::new(CayenneAccelerationExec::new(memory_exec("nested_right_id")));
        let nested_join = Arc::new(hash_join(
            nested_left,
            nested_right,
            "nested_left_id",
            "nested_right_id",
        ));
        let top_join = Arc::new(hash_join(
            memory_exec("top_id"),
            nested_join,
            "top_id",
            "nested_left_id",
        ));

        let optimized = optimize(top_join);
        let snapshot = plan_snapshot(&optimized);

        assert_eq!(
            2,
            snapshot.matches("accumulator=ExactLeftAccumulator").count(),
            "The top join and nested Cayenne probe join should both use ExactLeftAccumulator"
        );
    }

    #[test]
    fn snapshots_cayenne_probe_join_explain_plan() {
        let right = Arc::new(CayenneAccelerationExec::new(memory_exec("right_id")));
        let join = Arc::new(join_with_right(right));

        let optimized = optimize(join);

        insta::assert_snapshot!(
            "cayenne_probe_join_uses_exact_accumulator_explain",
            plan_snapshot(&optimized)
        );
    }

    #[test]
    fn snapshots_nested_cayenne_probe_join_explain_plan() {
        let nested_left = Arc::new(CayenneAccelerationExec::new(memory_exec("nested_left_id")));
        let nested_right = Arc::new(CayenneAccelerationExec::new(memory_exec("nested_right_id")));
        let nested_join = Arc::new(hash_join(
            nested_left,
            nested_right,
            "nested_left_id",
            "nested_right_id",
        ));
        let top_join = Arc::new(hash_join(
            memory_exec("top_id"),
            nested_join,
            "top_id",
            "nested_left_id",
        ));

        let optimized = optimize(top_join);

        insta::assert_snapshot!(
            "nested_cayenne_probe_join_uses_exact_accumulator_explain",
            plan_snapshot(&optimized)
        );
    }
}
