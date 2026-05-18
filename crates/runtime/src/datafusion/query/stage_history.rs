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
//! Tracing spans use wall-clock time, so without intervention the span
//! window would just be the brief moment in which this module emitted
//! the span at job completion. To make stage rows appear on the timeline
//! view with their real execution window, the spans carry
//! `stage_started_at` / `stage_ended_at` attributes (millis since UNIX
//! epoch, derived from `TaskInfo::launch_time` / `finish_time`), and the
//! `OTel` exporter overrides the row's `start_time` / `end_time` with
//! those values. `stage_duration_ms` is also kept as a separate label
//! for backwards-compatible label queries.

use std::collections::HashMap;
use std::time::{Duration, UNIX_EPOCH};

use ballista_scheduler::state::execution_graph::ExecutionGraph;
use ballista_scheduler::state::execution_stage::{ExecutionStage, TaskInfo};
use datafusion::common::format::ExplainFormat;
use opentelemetry_sdk::trace::SpanData;
use tracing::Span;

use crate::task_history::TaskSpan;
use crate::task_history::otel_exporter::{SpanRetention, SpanTransform};
use std::sync::Arc;

/// Name of the tracing span this module emits per stage. Also the value
/// of the `task` column in the resulting `task_history` row.
const BALLISTA_STAGE_SPAN_NAME: &str = "ballista_stage";

/// Attribute key carrying the stage's actual start time, in milliseconds
/// since the UNIX epoch.
const STAGE_STARTED_AT_ATTR: &str = "stage_started_at";

/// Attribute key carrying the stage's actual end time, in milliseconds
/// since the UNIX epoch.
const STAGE_ENDED_AT_ATTR: &str = "stage_ended_at";

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
        // Span name must match `BALLISTA_STAGE_SPAN_NAME` so
        // `BallistaStageMiddleware` can recognize the span at export
        // time. `info_span!` interprets a string-literal positional
        // argument as the span name, so the constant has to be inlined
        // here (verified by `stage_span_name_matches_constant`).
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

/// Task-history middleware for `ballista_stage` spans. Bundles two
/// hooks on a single type:
///
/// - [`SpanTransform`]: rewrites `start_time` / `end_time` to the
///   stage's actual execution window (read from the `stage_started_at`
///   / `stage_ended_at` attributes), so timeline visualizations show
///   the real per-stage runtime rather than the brief span-emission
///   window inside `record_stage_history`.
/// - [`SpanRetention`]: declares that a `ballista_stage` row depends
///   on its parent `sql_query` row — the stage row is written only
///   when the parent row is also being written, avoiding orphans.
///
/// Register on the exporter (both hooks):
/// ```ignore
/// let m: Arc<BallistaStageMiddleware> = Arc::new(BallistaStageMiddleware);
/// TaskHistoryExporter::new(...)
///     .with_transform(Arc::clone(&m) as _)
///     .with_retention(m as _)
/// ```
#[derive(Debug, Default)]
pub struct BallistaStageMiddleware;

impl BallistaStageMiddleware {
    /// Helper that constructs an `Arc<Self>` and returns it twice — once
    /// as each trait object — so a single instance can be registered for
    /// both hooks in a single chained builder call.
    #[must_use]
    pub fn pair() -> (Arc<dyn SpanTransform>, Arc<dyn SpanRetention>) {
        let m = Arc::new(Self);
        (
            Arc::clone(&m) as Arc<dyn SpanTransform>,
            m as Arc<dyn SpanRetention>,
        )
    }
}

impl SpanTransform for BallistaStageMiddleware {
    fn transform(&self, span: &mut SpanData) {
        if &*span.name != BALLISTA_STAGE_SPAN_NAME {
            return;
        }
        let Some(start_ms) = unix_ms_attr(span, STAGE_STARTED_AT_ATTR) else {
            return;
        };
        let Some(end_ms) = unix_ms_attr(span, STAGE_ENDED_AT_ATTR) else {
            return;
        };
        if start_ms == 0 || end_ms < start_ms {
            return;
        }
        span.start_time = UNIX_EPOCH + Duration::from_millis(start_ms);
        span.end_time = UNIX_EPOCH + Duration::from_millis(end_ms);
    }
}

impl SpanRetention for BallistaStageMiddleware {
    fn parent_dependency(&self, span: &TaskSpan) -> Option<Arc<str>> {
        if span.task.as_ref() != BALLISTA_STAGE_SPAN_NAME {
            return None;
        }
        span.parent_span_id.as_ref().map(Arc::clone)
    }
}

/// Read a non-negative `i64` attribute as `u64` milliseconds. Returns
/// `None` if the attribute is missing or not a non-negative integer.
fn unix_ms_attr(span: &SpanData, key: &str) -> Option<u64> {
    span.attributes.iter().find_map(|kv| {
        if kv.key.as_str() != key {
            return None;
        }
        match &kv.value {
            opentelemetry::Value::I64(v) if *v >= 0 => Some((*v).cast_unsigned()),
            _ => None,
        }
    })
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

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::trace::{
        SpanContext, SpanId, SpanKind, Status, TraceFlags, TraceId, TraceState,
    };
    use opentelemetry::{InstrumentationScope, KeyValue};
    use opentelemetry_sdk::trace::{SpanData, SpanEvents, SpanLinks};
    use std::borrow::Cow;
    use std::time::SystemTime;

    fn stage_span(start_at: SystemTime, end_at: SystemTime, attrs: Vec<KeyValue>) -> SpanData {
        SpanData {
            span_context: SpanContext::new(
                TraceId::from_bytes([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]),
                SpanId::from_bytes([0, 0, 0, 0, 0, 0, 0, 1]),
                TraceFlags::default(),
                false,
                TraceState::default(),
            ),
            parent_span_id: SpanId::INVALID,
            parent_span_is_remote: false,
            span_kind: SpanKind::Internal,
            name: Cow::Borrowed(BALLISTA_STAGE_SPAN_NAME),
            start_time: start_at,
            end_time: end_at,
            attributes: attrs,
            dropped_attributes_count: 0,
            events: SpanEvents::default(),
            links: SpanLinks::default(),
            status: Status::Unset,
            instrumentation_scope: InstrumentationScope::default(),
        }
    }

    #[test]
    fn override_replaces_timestamps_with_stage_window() {
        let emit_at = UNIX_EPOCH + Duration::from_millis(10_000_000);
        let mut span = stage_span(
            emit_at,
            emit_at + Duration::from_millis(2),
            vec![
                KeyValue::new(STAGE_STARTED_AT_ATTR, 1_000_000_i64),
                KeyValue::new(STAGE_ENDED_AT_ATTR, 1_005_000_i64),
            ],
        );
        BallistaStageMiddleware.transform(&mut span);
        assert_eq!(
            span.start_time,
            UNIX_EPOCH + Duration::from_millis(1_000_000)
        );
        assert_eq!(span.end_time, UNIX_EPOCH + Duration::from_millis(1_005_000));
    }

    #[test]
    fn override_leaves_span_unchanged_for_other_span_names() {
        let emit_at = UNIX_EPOCH + Duration::from_millis(10_000_000);
        let mut span = stage_span(
            emit_at,
            emit_at + Duration::from_millis(2),
            vec![
                KeyValue::new(STAGE_STARTED_AT_ATTR, 1_000_000_i64),
                KeyValue::new(STAGE_ENDED_AT_ATTR, 1_005_000_i64),
            ],
        );
        span.name = Cow::Borrowed("sql_query");
        BallistaStageMiddleware.transform(&mut span);
        assert_eq!(span.start_time, emit_at);
        assert_eq!(span.end_time, emit_at + Duration::from_millis(2));
    }

    #[test]
    fn override_skipped_when_attributes_missing() {
        let emit_at = UNIX_EPOCH + Duration::from_millis(10_000_000);
        let mut span = stage_span(emit_at, emit_at + Duration::from_millis(2), vec![]);
        BallistaStageMiddleware.transform(&mut span);
        assert_eq!(span.start_time, emit_at);
        assert_eq!(span.end_time, emit_at + Duration::from_millis(2));
    }

    #[test]
    fn override_skipped_when_window_is_inverted_or_zero() {
        let emit_at = UNIX_EPOCH + Duration::from_millis(10_000_000);
        // start > end is nonsense from a partly-recorded stage.
        let mut span = stage_span(
            emit_at,
            emit_at + Duration::from_millis(2),
            vec![
                KeyValue::new(STAGE_STARTED_AT_ATTR, 1_005_000_i64),
                KeyValue::new(STAGE_ENDED_AT_ATTR, 1_000_000_i64),
            ],
        );
        BallistaStageMiddleware.transform(&mut span);
        assert_eq!(span.start_time, emit_at);
        // start == 0 means no tasks recorded a launch time; leave alone.
        let mut span0 = stage_span(
            emit_at,
            emit_at + Duration::from_millis(2),
            vec![
                KeyValue::new(STAGE_STARTED_AT_ATTR, 0_i64),
                KeyValue::new(STAGE_ENDED_AT_ATTR, 1_000_000_i64),
            ],
        );
        BallistaStageMiddleware.transform(&mut span0);
        assert_eq!(span0.start_time, emit_at);
    }

    fn task_span(task: &str, parent: Option<&str>) -> TaskSpan {
        TaskSpan {
            trace_id: Arc::from("trace"),
            trace_id_override: None,
            span_id: Arc::from("span"),
            parent_span_id: parent.map(Arc::from),
            distributed_parent_id: None,
            task: Arc::from(task),
            input: Arc::from(""),
            captured_output: None,
            start_time: UNIX_EPOCH,
            end_time: UNIX_EPOCH,
            execution_duration_ms: 0.0,
            error_message: None,
            labels: HashMap::new(),
            node_id: None,
        }
    }

    #[test]
    fn retention_declares_parent_dependency_for_stage_spans() {
        let span = task_span(BALLISTA_STAGE_SPAN_NAME, Some("parent-id"));
        let dep = BallistaStageMiddleware.parent_dependency(&span);
        assert_eq!(dep.as_deref(), Some("parent-id"));
    }

    #[test]
    fn retention_returns_none_for_non_stage_spans() {
        let span = task_span("sql_query", Some("ignored"));
        assert!(BallistaStageMiddleware.parent_dependency(&span).is_none());
    }

    #[test]
    fn retention_returns_none_for_orphan_stage() {
        let span = task_span(BALLISTA_STAGE_SPAN_NAME, None);
        // An orphan stage has no parent to depend on; falling through
        // to base retention is the correct behavior.
        assert!(BallistaStageMiddleware.parent_dependency(&span).is_none());
    }
}
