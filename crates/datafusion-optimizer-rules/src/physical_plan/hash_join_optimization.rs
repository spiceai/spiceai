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

            // Preserve the join's output partition count on the replacement
            // EmptyExec. Without this, downstream operators that expected
            // a `Partitioned` input (e.g. another `HashJoinExec` with
            // `mode=Partitioned`) would see a partition-count mismatch at
            // execution time and fail DataFusion's runtime sanity check.
            let partitions = join_exec
                .properties()
                .output_partitioning()
                .partition_count();

            Ok(Transformed::yes(Arc::new(
                EmptyExec::new(join_exec.schema()).with_partitions(partitions),
            )))
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

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::common::NullEquality;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_plan::joins::PartitionMode;
    use datafusion::physical_plan::repartition::RepartitionExec;
    use datafusion::physical_plan::{Partitioning, PhysicalExpr};
    use datafusion_datasource::memory::MemorySourceConfig;

    fn schema(col: &str) -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(col, DataType::Int32, true)]))
    }

    fn empty_memory_exec(col: &str) -> Arc<dyn ExecutionPlan> {
        let schema = schema(col);
        MemorySourceConfig::try_new_exec(&[vec![]], schema, None).expect("valid memory exec")
    }

    fn hash_repartition(
        input: Arc<dyn ExecutionPlan>,
        col: &str,
        partitions: usize,
    ) -> Arc<dyn ExecutionPlan> {
        let expr: Arc<dyn PhysicalExpr> = Arc::new(Column::new(col, 0));
        Arc::new(
            RepartitionExec::try_new(input, Partitioning::Hash(vec![expr], partitions))
                .expect("valid repartition"),
        )
    }

    #[test]
    fn empty_partitioned_hash_join_replacement_preserves_partition_count() {
        // Build a `HashJoinExec` with `mode=Partitioned` where both inputs
        // are hash-partitioned into 8 partitions and the left input is
        // statically empty. The rule must replace the join with an
        // `EmptyExec` that still advertises 8 output partitions so that
        // any downstream operator with a `Partitioned` distribution
        // requirement continues to see matching partition counts.
        let target_partitions = 8usize;

        let left = hash_repartition(empty_memory_exec("l"), "l", target_partitions);
        let right = hash_repartition(empty_memory_exec("r"), "r", target_partitions);

        let on: Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)> =
            vec![(Arc::new(Column::new("l", 0)), Arc::new(Column::new("r", 0)))];

        let join: Arc<dyn ExecutionPlan> = Arc::new(
            HashJoinExec::try_new(
                left,
                right,
                on,
                None,
                &JoinType::Inner,
                None,
                PartitionMode::Partitioned,
                NullEquality::NullEqualsNothing,
            )
            .expect("valid HashJoinExec"),
        );

        let original_partitions = join.properties().output_partitioning().partition_count();
        assert_eq!(
            original_partitions, target_partitions,
            "test precondition: partitioned hash join should output {target_partitions} partitions"
        );

        let rule = EmptyHashJoinExecPhysicalOptimization {};
        let optimized = rule
            .optimize(join, &ConfigOptions::default())
            .expect("optimize succeeds");

        let empty = optimized
            .as_any()
            .downcast_ref::<EmptyExec>()
            .expect("join replaced with EmptyExec");
        assert_eq!(
            empty.properties().output_partitioning().partition_count(),
            target_partitions,
            "replacement EmptyExec must preserve the join's output partition count"
        );
    }

    #[test]
    fn non_empty_inputs_leave_hash_join_untouched() {
        // Sanity check: when neither side is statically empty the rule
        // must not rewrite the plan.
        let schema = schema("l");
        let mk_one_row = || -> Arc<dyn ExecutionPlan> {
            use arrow::array::{Int32Array, RecordBatch};
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int32Array::from(vec![1]))],
            )
            .expect("valid batch");
            MemorySourceConfig::try_new_exec(&[vec![batch]], Arc::clone(&schema), None)
                .expect("valid memory exec")
        };

        let on: Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)> =
            vec![(Arc::new(Column::new("l", 0)), Arc::new(Column::new("l", 0)))];

        let join: Arc<dyn ExecutionPlan> = Arc::new(
            HashJoinExec::try_new(
                mk_one_row(),
                mk_one_row(),
                on,
                None,
                &JoinType::Inner,
                None,
                PartitionMode::CollectLeft,
                NullEquality::NullEqualsNothing,
            )
            .expect("valid HashJoinExec"),
        );

        let rule = EmptyHashJoinExecPhysicalOptimization {};
        let optimized = rule
            .optimize(Arc::clone(&join), &ConfigOptions::default())
            .expect("optimize succeeds");

        assert!(
            optimized.as_any().is::<HashJoinExec>(),
            "rule must leave non-empty joins as `HashJoinExec`"
        );
    }
}
