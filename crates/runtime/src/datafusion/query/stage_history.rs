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

//! Decorates the parent `sql_query` `task_history` span and emits one child
//! `ballista_stage` span per stage at distributed job completion.
//!
//! Run from `QueryHandle::finish_tracker_*` after the Ballista job reaches
//! terminal state (success, failure, or cancellation). Walks the in-process
//! `ExecutionGraph` and produces child rows via the existing `OTel` pipeline
//! — child spans created inside `parent_span.in_scope(...)` inherit the
//! parent's `OTel` context, so the exporter at
//! `crate::task_history::otel_exporter` writes them with the correct
//! `parent_span_id`.
//!
//! Tracing spans use wall-clock time, so a child span's
//! `execution_duration_ms` reflects the brief window between creation and
//! drop in this module, not the stage's actual run. The historical times
//! are emitted as `stage_started_at` / `stage_ended_at` /
//! `stage_duration_ms` labels — queries against stage rows should use
//! those.

use std::collections::HashMap;

use ballista_scheduler::state::execution_graph::ExecutionGraph;
use ballista_scheduler::state::execution_stage::{ExecutionStage, TaskInfo};
use datafusion::common::format::ExplainFormat;
use tracing::Span;

/// Summary metrics extracted from a single stage's `TaskInfo`s.
struct StageSummary {
    partitions: usize,
    attempt_num: usize,
    task_count: usize,
    executor_count: usize,
    executor_histogram: String,
    slowest_task_ms: u128,
    slowest_task_executor: String,
    total_executor_ms: u128,
    stage_started_at: u128,
    stage_ended_at: u128,
    stage_duration_ms: u128,
    error_message: Option<String>,
}

/// Decorate the parent `sql_query` span with job-wide summary labels and
/// emit one child `ballista_stage` span per stage.
///
/// The parent span must be active (i.e., this function is called inside
/// `parent_span.in_scope(...)`). Child spans created via `info_span!` will
/// then inherit the `OTel` parent context automatically.
pub(crate) fn record_stage_history(
    parent_span: &Span,
    ballista_job_id: &str,
    graph: &dyn ExecutionGraph,
) {
    let stages = graph.stages();
    let mut total_tasks: usize = 0;
    let mut total_executor_ms: u128 = 0;
    let mut all_executors: std::collections::HashSet<String> = std::collections::HashSet::new();

    // First pass: aggregate parent-level counters and prepare per-stage
    // summaries so we can emit children once the parent labels are set.
    let mut per_stage_summaries: Vec<(usize, StageSummary)> = Vec::with_capacity(stages.len());
    let mut stage_ids: Vec<usize> = stages.keys().copied().collect();
    stage_ids.sort_unstable();
    for stage_id in stage_ids {
        if let Some(stage) = stages.get(&stage_id) {
            let summary = summarize_stage(stage);
            total_tasks = total_tasks.saturating_add(summary.task_count);
            total_executor_ms = total_executor_ms.saturating_add(summary.total_executor_ms);
            for exec in executor_ids_in_stage(stage) {
                all_executors.insert(exec);
            }
            per_stage_summaries.push((stage_id, summary));
        }
    }

    parent_span.record("ballista_job_id", ballista_job_id);
    parent_span.record("stage_count", stages.len() as u64);
    parent_span.record("executor_count", all_executors.len() as u64);
    parent_span.record("total_tasks", total_tasks as u64);
    parent_span.record("total_executor_ms", u128_to_u64_sat(total_executor_ms));

    for (stage_id, summary) in per_stage_summaries {
        if let Some(stage) = stages.get(&stage_id) {
            emit_stage_span(parent_span, stage_id, stage, &summary);
        }
    }
}

/// Create the child `ballista_stage` span and let it close to fire the
/// `task_history` row write. Called inside the parent span's scope.
fn emit_stage_span(
    parent_span: &Span,
    stage_id: usize,
    stage: &ExecutionStage,
    summary: &StageSummary,
) {
    // Render the stage plan in tree form via Ballista's per-variant
    // `format_with` so the row's `input` matches what `EXPLAIN FORMAT TREE`
    // would produce for the stage in isolation.
    let plan_str = stage.format_with(&ExplainFormat::Tree);

    parent_span.in_scope(|| {
        let stage_span = tracing::info_span!(
            target: "task_history",
            "ballista_stage",
            input = %plan_str,
            stage_id = stage_id as u64,
            stage_status = stage.variant_name(),
            partitions = summary.partitions as u64,
            attempt_num = summary.attempt_num as u64,
            task_count = summary.task_count as u64,
            executor_count = summary.executor_count as u64,
            executor_histogram = %summary.executor_histogram,
            slowest_task_ms = u128_to_u64_sat(summary.slowest_task_ms),
            slowest_task_executor = %summary.slowest_task_executor,
            total_executor_ms = u128_to_u64_sat(summary.total_executor_ms),
            stage_started_at = u128_to_u64_sat(summary.stage_started_at),
            stage_ended_at = u128_to_u64_sat(summary.stage_ended_at),
            stage_duration_ms = u128_to_u64_sat(summary.stage_duration_ms),
        );
        if let Some(err) = summary.error_message.as_deref() {
            tracing::error!(target: "task_history", parent: &stage_span, "{err}");
        }
        // stage_span drops here → OTel closes it → exporter writes the row.
    });
}

/// Returns the set of executor ids that have run (or are running) any
/// `TaskInfo` in the stage. Used to count distinct executors at the
/// job level.
fn executor_ids_in_stage(stage: &ExecutionStage) -> Vec<String> {
    let mut ids = Vec::new();
    for task_info in iter_task_infos(stage) {
        if !task_info.executor_id.is_empty() {
            ids.push(task_info.executor_id.clone());
        }
    }
    ids
}

/// Iterate over the concrete `TaskInfo`s in a stage regardless of variant.
/// Unresolved/Resolved stages yield no tasks (none have launched yet).
fn iter_task_infos(stage: &ExecutionStage) -> Box<dyn Iterator<Item = &TaskInfo> + '_> {
    match stage {
        ExecutionStage::UnResolved(_) | ExecutionStage::Resolved(_) => Box::new(std::iter::empty()),
        ExecutionStage::Running(s) => Box::new(s.task_infos.iter().filter_map(Option::as_ref)),
        ExecutionStage::Successful(s) => Box::new(s.task_infos.iter()),
        ExecutionStage::Failed(s) => Box::new(s.task_infos.iter().filter_map(Option::as_ref)),
    }
}

/// Compute per-stage summary metrics from its `TaskInfo`s.
fn summarize_stage(stage: &ExecutionStage) -> StageSummary {
    let (partitions, attempt_num, error_message) = match stage {
        ExecutionStage::UnResolved(s) => (0, s.stage_attempt_num, None),
        ExecutionStage::Resolved(s) => (s.partitions, s.stage_attempt_num, None),
        ExecutionStage::Running(s) => (s.partitions, s.stage_attempt_num, None),
        ExecutionStage::Successful(s) => (s.partitions, s.stage_attempt_num, None),
        ExecutionStage::Failed(s) => (
            s.partitions,
            s.stage_attempt_num,
            Some(s.error_message.clone()),
        ),
    };

    let mut task_count: usize = 0;
    let mut total_executor_ms: u128 = 0;
    let mut slowest_task_ms: u128 = 0;
    let mut slowest_task_executor = String::new();
    let mut stage_started_at: u128 = u128::MAX;
    let mut stage_ended_at: u128 = 0;
    let mut by_executor: HashMap<String, u64> = HashMap::new();

    for task in iter_task_infos(stage) {
        task_count = task_count.saturating_add(1);
        let dur = task.end_exec_time.saturating_sub(task.start_exec_time);
        total_executor_ms = total_executor_ms.saturating_add(dur);
        if task.launch_time != 0 && task.launch_time < stage_started_at {
            stage_started_at = task.launch_time;
        }
        if task.finish_time > stage_ended_at {
            stage_ended_at = task.finish_time;
        }
        // Skip tasks that haven't been placed yet (pending or failed without
        // executor assignment). Including their empty `executor_id` would
        // pollute the per-stage histogram (a `:N` bucket) and disagree with
        // the job-level `executor_count`, which already filters empty ids.
        if task.executor_id.is_empty() {
            continue;
        }
        if dur > slowest_task_ms {
            slowest_task_ms = dur;
            slowest_task_executor.clone_from(&task.executor_id);
        }
        *by_executor.entry(task.executor_id.clone()).or_insert(0) += 1;
    }
    if stage_started_at == u128::MAX {
        stage_started_at = 0;
    }
    let stage_duration_ms = stage_ended_at.saturating_sub(stage_started_at);

    let executor_count = by_executor.len();
    let executor_histogram = format_executor_histogram(&by_executor);

    StageSummary {
        partitions,
        attempt_num,
        task_count,
        executor_count,
        executor_histogram,
        slowest_task_ms,
        slowest_task_executor,
        total_executor_ms,
        stage_started_at,
        stage_ended_at,
        stage_duration_ms,
        error_message,
    }
}

/// Saturating cast from `u128` (Ballista's task timestamps and
/// duration accumulators) to `u64` (the `OTel` field type). `u64::MAX`
/// milliseconds is ~584 million years; this only saturates if a counter
/// has overflowed, in which case losing precision in the label is the
/// least of our problems.
fn u128_to_u64_sat(v: u128) -> u64 {
    u64::try_from(v).unwrap_or(u64::MAX)
}

/// Format `{executor_id -> count}` as `"executor-a:120,executor-b:80"`
/// sorted by executor id so the same shape always produces the same
/// string (stable for snapshot tests).
fn format_executor_histogram(by_executor: &HashMap<String, u64>) -> String {
    let mut entries: Vec<(&String, &u64)> = by_executor.iter().collect();
    entries.sort_by(|a, b| a.0.cmp(b.0));
    let mut s = String::new();
    for (i, (exec, count)) in entries.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        s.push_str(exec);
        s.push(':');
        s.push_str(&count.to_string());
    }
    s
}
