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

use std::sync::Arc;

use datafusion::physical_optimizer::{PhysicalOptimizerRule, optimizer::PhysicalOptimizer};
use datafusion_optimizer_rules::physical_plan::cluster::ensure_supported_file_scan::EnsureSupportedFileScan;

pub mod codec;
pub mod datafusion_scheduler_ext;

#[must_use]
pub fn datafusion_and_cluster_physical_optimizers()
-> Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> {
    let mut rules = PhysicalOptimizer::new().rules;
    rules.extend(cluster_physical_optimizers());
    rules
}

#[must_use]
fn cluster_physical_optimizers() -> Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> {
    vec![EnsureSupportedFileScan::new()]
}

/// Guards the null-aware anti-join fix the `spiceai/datafusion-ballista` fork
/// carries (fork PR #58).
///
/// `NOT IN (<subquery>)` decorrelates to a `LeftAnti` hash join carrying the
/// `null_aware` flag, and that flag is what gives the join SQL's three-valued
/// semantics: a NULL among the values being tested against makes every `NOT IN`
/// UNKNOWN, so the predicate selects no rows at all.
///
/// The scheduler runs its own `JoinSelection` over each stage as it resolves
/// (`ExecutionStage::to_resolved`), rebuilding hash joins to choose a collect side.
/// Before the fork patch that rebuild hard-coded `null_aware` to `false` and swapped
/// `LeftAnti` to `RightAnti` where the statistics favoured it. Either way the
/// rebuilt join is an ordinary anti join: it plans, it runs, and it returns the rows
/// a NULL should have excluded.
///
/// The tests below drive the scheduler's rule directly rather than a live cluster,
/// so they run in the unit-test gate and assert the rows the rebuilt join actually
/// produces, not just its shape. The fork branch is re-cut per `DataFusion` major
/// and distributed execution is largely Spice's own code, so this patch is among the
/// likeliest to be dropped in a re-cut and the least likely to be re-derived
/// upstream; `docs/dev/fork_patches.md` is the ledger this guard is named in.
#[cfg(test)]
mod null_aware_anti_join {
    use std::sync::Arc;

    use arrow::array::{Int64Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use ballista_scheduler::physical_optimizer::join_selection::JoinSelection;
    use datafusion::common::{JoinType, NullEquality};
    use datafusion::config::ConfigOptions;
    use datafusion::execution::TaskContext;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_optimizer::PhysicalOptimizerRule;
    use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
    use datafusion::physical_plan::repartition::RepartitionExec;
    use datafusion::physical_plan::{ExecutionPlan, Partitioning, PhysicalExpr, collect};
    use datafusion_datasource::memory::MemorySourceConfig;

    fn schema(column: &str) -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(column, DataType::Int64, true)]))
    }

    fn source(column: &str, values: &[Option<i64>]) -> Arc<dyn ExecutionPlan> {
        let schema = schema(column);
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(values.iter().copied().collect::<Int64Array>())],
        )
        .expect("batch matches schema");
        MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None).expect("memory source")
    }

    fn ascending(range: std::ops::Range<i64>) -> Vec<Option<i64>> {
        range.map(Some).collect()
    }

    /// Hash-partition `input` on its only column, so a fixture can enter
    /// `JoinSelection` with the partitioning a shuffled stage would have.
    fn hash_partitioned(
        input: Arc<dyn ExecutionPlan>,
        column: &str,
        partitions: usize,
    ) -> Arc<dyn ExecutionPlan> {
        let key: Arc<dyn PhysicalExpr> = Arc::new(Column::new(column, 0));
        Arc::new(
            RepartitionExec::try_new(input, Partitioning::Hash(vec![key], partitions))
                .expect("hash repartition"),
        )
    }

    /// `SELECT probe FROM probes WHERE probe NOT IN (SELECT value FROM values)`, as
    /// the scheduler sees it once the planner has decorrelated it: a null-aware
    /// `LeftAnti` hash join over the two sides.
    ///
    /// Three things about the fixture decide which part of the patch the rewrite
    /// exercises. Which side is larger: a larger build side makes a swap look
    /// profitable, so the rule has to decline it, while a larger probe side makes it
    /// rebuild the join in place and carry `null_aware` across that rebuild. And the
    /// mode it arrives in: a join that reaches the rule already `Partitioned` takes a
    /// different arm, which has to correct it to `CollectLeft` — partitioned
    /// null-aware state is only ever partition-local, so the NULL one partition sees
    /// would not be seen by the others.
    fn not_in_join_in_mode(
        probes: &[Option<i64>],
        values: &[Option<i64>],
        mode: PartitionMode,
        partitions: usize,
    ) -> Arc<dyn ExecutionPlan> {
        let on: Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)> = vec![(
            Arc::new(Column::new("probe", 0)),
            Arc::new(Column::new("value", 0)),
        )];
        let (left, right) = if partitions > 1 {
            (
                hash_partitioned(source("probe", probes), "probe", partitions),
                hash_partitioned(source("value", values), "value", partitions),
            )
        } else {
            (source("probe", probes), source("value", values))
        };
        Arc::new(
            HashJoinExec::try_new(
                left,
                right,
                on,
                None,
                &JoinType::LeftAnti,
                None,
                mode,
                NullEquality::NullEqualsNothing,
                true,
            )
            .expect("a null-aware LeftAnti join is valid"),
        )
    }

    fn not_in_join(probes: &[Option<i64>], values: &[Option<i64>]) -> Arc<dyn ExecutionPlan> {
        not_in_join_in_mode(probes, values, PartitionMode::CollectLeft, 1)
    }

    fn optimized(join: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        JoinSelection::new()
            .optimize(join, &ConfigOptions::new())
            .expect("join selection rewrites the join")
    }

    async fn probes_selected_by(plan: Arc<dyn ExecutionPlan>) -> Vec<i64> {
        let batches = collect(plan, Arc::new(TaskContext::default()))
            .await
            .expect("the rewritten join executes");
        let mut probes: Vec<i64> = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("probe column is Int64")
                    .iter()
                    .map(|value| value.expect("probe is not null"))
                    .collect::<Vec<_>>()
            })
            .collect();
        probes.sort_unstable();
        probes
    }

    /// A NULL among the values makes every `NOT IN` UNKNOWN, so the join selects
    /// nothing — here with the build side larger, the arrangement that makes a swap
    /// look profitable. A swapped `RightAnti` cannot carry `null_aware` at all, so
    /// losing that half of the patch fails the plan outright.
    #[tokio::test]
    async fn a_null_among_the_values_leaves_no_row_selected() {
        let plan = optimized(not_in_join(&ascending(0..512), &[None, Some(7)]));
        assert!(
            probes_selected_by(plan).await.is_empty(),
            "a NULL among the values tested against selects no rows"
        );
    }

    /// The same query with the values as the larger side, so no swap looks
    /// profitable and the rule rebuilds the join where it stands. This is the half
    /// of the patch that fails quietly: a rebuild that drops `null_aware` treats the
    /// NULL as "no match" and selects both probes, which is valid SQL for a plain
    /// anti join and wrong for this one.
    #[tokio::test]
    async fn a_null_among_the_values_leaves_no_row_selected_where_no_swap_is_profitable() {
        let mut values = vec![None];
        values.extend(ascending(7..519));
        let plan = optimized(not_in_join(&[Some(0), Some(1)], &values));
        assert!(
            probes_selected_by(plan).await.is_empty(),
            "the rebuilt join has to keep the NULL-aware semantics of the one it replaced"
        );
    }

    /// The control: with no NULL among the values the same join selects every probe
    /// absent from them, so a guard that only ever saw an empty result would notice
    /// nothing.
    #[tokio::test]
    async fn values_without_a_null_select_every_probe_absent_from_them() {
        let plan = optimized(not_in_join(&ascending(0..512), &[Some(7)]));
        let selected = probes_selected_by(plan).await;
        assert_eq!(selected.len(), 511);
        assert!(!selected.contains(&7));
    }

    /// A null-aware join that reaches the rule already `Partitioned` takes a
    /// different arm from the ones above, and PR #58's second commit is what makes
    /// that arm correct it to `CollectLeft`. It has to: the null-aware build state
    /// (`probe_side_has_null`) is per-partition, so under `Partitioned` the NULL one
    /// partition sees is invisible to the others and each answers its own slice as if
    /// no NULL existed.
    ///
    /// Asserted on the shape rather than the rows, because the rows cannot distinguish
    /// this arm today. The difference the correction makes only appears across more
    /// than one partition — with a single partition, per-partition NULL state *is* the
    /// global state, so `Partitioned` and `CollectLeft` answer identically and the
    /// assertion would hold with the patch reverted. More than one partition cannot be
    /// executed either: the rule forces `CollectLeft` without coalescing the left
    /// input, and `HashJoinExec` refuses `CollectLeft` with a multi-partition left
    /// side. That is the same defect a distributed `NOT IN` hits, and it is tracked
    /// separately; until it is fixed the mode is the only observable this arm has.
    #[test]
    fn a_partitioned_null_aware_join_is_corrected_to_collect_left() {
        let plan = optimized(not_in_join_in_mode(
            &ascending(0..512),
            &[None, Some(7)],
            PartitionMode::Partitioned,
            4,
        ));
        let join = plan
            .downcast_ref::<HashJoinExec>()
            .expect("the rewrite is still a hash join");

        assert_eq!(
            *join.partition_mode(),
            PartitionMode::CollectLeft,
            "a null-aware anti join left in Partitioned mode keeps its NULL state per-partition, \
             so each partition answers as though no NULL existed"
        );
        assert_eq!(*join.join_type(), JoinType::LeftAnti);
        assert!(join.null_aware);
    }

    /// The shape behind those results: a null-aware join is only valid as `LeftAnti`,
    /// so the rule must leave the sides alone however profitable a swap looks, and
    /// must carry the flag across the rebuild.
    #[test]
    fn the_rule_neither_swaps_the_sides_nor_drops_the_flag() {
        let optimized = optimized(not_in_join(&ascending(0..512), &[None, Some(7)]));
        let join = optimized
            .downcast_ref::<HashJoinExec>()
            .expect("the rewrite is still a hash join");

        assert_eq!(*join.join_type(), JoinType::LeftAnti);
        assert!(join.null_aware);
    }
}

/// Guards the stale-status rejection the `spiceai/datafusion-ballista` fork
/// carries (fork PR #53).
///
/// An executor that is lost — or merely heartbeat-timed-out — has its stages reset:
/// every task it was running is marked `Failed(ResultLost)` and the plan input
/// partitions that task held go back on the stage's pending queue for another
/// executor to pick up. The lost executor's own status updates are already on the
/// wire when that happens, so the scheduler receives a status for a task whose
/// partitions now belong to a later attempt. `RunningStage::update_task_info` has
/// to refuse it, and the scheduler's update path takes that return value as its
/// gate — `if !running_stage.update_task_info(..) { continue; }` — so accepting a
/// late `Successful` would overwrite the reset record and register shuffle output
/// locations on the executor that is gone, for a partition a live executor is at
/// that moment re-running.
///
/// Asserted against the patched function directly, on a stage driven into the state
/// a reset leaves behind. `RunningStage::task_infos` and `RunningStage::pending` are
/// public, so the test binds a task on each of two executors by hand — take a
/// partition slice off the pending queue, append the `TaskInfo`, as the real bind
/// path does — and then has `RunningStage::reset_tasks`, the function the
/// lost-executor path calls, reset the one belonging to the executor that is gone.
/// Driving the real `reset_stages_on_lost_executor` would be closer still, but the
/// scheduler's task-issuing API (`ExecutionGraph::pop_next_task`) is `#[cfg(test)]`
/// on the fork, so it is unreachable from here — and being `#[cfg(test)]` is also
/// why the fork's own coverage of this leaves with the branch that gets re-cut.
///
/// Both halves of the patched function are asserted: the stale status for the reset
/// task is refused, and an ordinary status for the task whose executor is still
/// there is accepted. A guard that checked only the refusal would pass just as well
/// on a regression that refused *every* status.
#[cfg(test)]
mod stale_status_for_a_reset_task {
    use std::collections::HashMap;
    use std::sync::Arc;

    use ballista_core::extension::SessionConfigExt;
    use ballista_core::serde::protobuf::{
        RunningTask, ShuffleWritePartition, SuccessfulTask, TaskStatus, task_status,
    };
    use ballista_scheduler::state::execution_stage::{RunningStage, TaskInfo};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::execution::context::SessionConfig;
    use datafusion::physical_plan::empty::EmptyExec;

    const LOST_EXECUTOR: &str = "executor-that-is-lost";
    const LIVE_EXECUTOR: &str = "executor-still-here";

    /// The status an executor sends when a task finishes: one shuffle partition
    /// written per plan input partition the task covered.
    fn completed(task_id: usize, partitions: &[usize], executor_id: &str) -> TaskStatus {
        let task_id = u64::try_from(task_id).expect("a small test task id fits in u64");
        TaskStatus {
            task_id: u32::try_from(task_id).expect("a small test task id fits in u32"),
            job_id: "job".to_string(),
            stage_id: 1,
            stage_attempt_num: 0,
            launch_time: 0,
            start_exec_time: 0,
            end_exec_time: 0,
            metrics: vec![],
            status: Some(task_status::Status::Successful(SuccessfulTask {
                executor_id: executor_id.to_owned(),
                partitions: partitions
                    .iter()
                    .map(|partition_id| ShuffleWritePartition {
                        partition_id: u64::try_from(*partition_id)
                            .expect("a small test partition id fits in u64"),
                        path: format!("/job/1/{partition_id}"),
                        num_batches: 1,
                        num_rows: 1,
                        num_bytes: 1,
                        file_id: Some(task_id),
                        is_sort_shuffle: false,
                    })
                    .collect(),
                runtime_stats: vec![],
                window_state: vec![],
            })),
        }
    }

    /// Bind a task covering the next pending partition to `executor_id`, as the
    /// scheduler's bind path does: take a slice off the stage's pending queue and
    /// append the `TaskInfo` at the next task slot. Returns the new task id.
    fn launch_one_partition_on(stage: &mut RunningStage, executor_id: &str) -> usize {
        let partitions = stage.pending.next_slice(1);
        assert_eq!(
            partitions.len(),
            1,
            "the stage must still have a partition left to hand out",
        );
        let task_id = stage.task_infos.len();
        stage.task_infos.push(TaskInfo {
            task_id,
            executor_id: executor_id.to_owned(),
            scheduled_time: 0,
            launch_time: 0,
            start_exec_time: 0,
            end_exec_time: 0,
            finish_time: 0,
            task_status: task_status::Status::Running(RunningTask {
                executor_id: executor_id.to_owned(),
            }),
            global_input_partition_ids: partitions,
            vcores_consumed: 1,
        });
        task_id
    }

    /// A two-partition stage with a task running on each of two executors, the
    /// first of which has just been lost: its task is reset and the partition it
    /// held is back on the pending queue, while the other executor's task keeps
    /// running. Returns the stage and the two task ids.
    fn stage_after_losing_an_executor() -> (RunningStage, usize, usize) {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));
        let mut stage = RunningStage::new(
            1,
            0,
            Arc::new(EmptyExec::new(schema)),
            2,
            vec![],
            HashMap::new(),
            Arc::new(SessionConfig::new_with_ballista()),
        );
        let lost_task = launch_one_partition_on(&mut stage, LOST_EXECUTOR);
        let live_task = launch_one_partition_on(&mut stage, LIVE_EXECUTOR);
        assert_eq!(
            stage.available_tasks(),
            0,
            "both of the stage's partitions were handed to a task",
        );

        assert_eq!(
            stage.reset_tasks(LOST_EXECUTOR),
            1,
            "the lost executor ran exactly one task on this stage",
        );
        assert_eq!(
            stage.available_tasks(),
            1,
            "the reset must put the lost executor's partition back on the pending queue",
        );
        assert_eq!(
            stage.running_tasks().len(),
            1,
            "the reset must leave the other executor's task running",
        );
        (stage, lost_task, live_task)
    }

    /// A status for a task the reset already failed must be refused, and the stage
    /// must be left as the reset left it.
    #[test]
    fn a_status_for_a_task_that_was_reset_is_refused() {
        let (mut stage, lost_task, _) = stage_after_losing_an_executor();
        let partitions = stage.task_infos[lost_task]
            .global_input_partition_ids
            .clone();

        let accepted =
            stage.update_task_info(lost_task, completed(lost_task, &partitions, LOST_EXECUTOR));

        assert!(
            !accepted,
            "a status for a task the lost-executor reset already failed must be refused; the \
             scheduler takes this return value as its gate, so accepting it registers shuffle \
             output on the executor that is gone, for a partition another executor is re-running"
        );
        assert!(
            matches!(
                stage.task_infos[lost_task].task_status,
                task_status::Status::Failed(_)
            ),
            "the refused status overwrote the reset record, so the partition now reads as \
             complete on an executor that is gone",
        );
        assert_eq!(
            stage.successful_tasks(),
            0,
            "the refused status was counted as a successful task",
        );
        assert_eq!(
            stage.available_tasks(),
            1,
            "the reset partition must stay pending for another executor to pick up",
        );
    }

    /// A status for a task that is still running must be accepted and recorded,
    /// exactly as before the patch.
    #[test]
    fn a_status_for_a_task_that_is_still_running_is_accepted() {
        let (mut stage, _, live_task) = stage_after_losing_an_executor();
        let partitions = stage.task_infos[live_task]
            .global_input_partition_ids
            .clone();

        let accepted =
            stage.update_task_info(live_task, completed(live_task, &partitions, LIVE_EXECUTOR));

        assert!(
            accepted,
            "an ordinary status for a task that is still running must be accepted; a guard that \
             stopped at the refusal could not tell the patch from one that refuses every status"
        );
        assert!(
            matches!(
                stage.task_infos[live_task].task_status,
                task_status::Status::Successful(_)
            ),
            "the accepted status was not recorded against its task",
        );
        assert_eq!(
            stage.successful_tasks(),
            1,
            "the accepted status was not counted as a successful task",
        );
        assert_eq!(
            stage.available_tasks(),
            1,
            "the reset partition must stay pending for rescheduling",
        );
    }
}
