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

use arrow::util::pretty::pretty_format_batches;
use futures::StreamExt;
use std::collections::HashMap;
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, LazyLock};
use std::time::SystemTime;

use opentelemetry::trace::SpanId;
use opentelemetry_sdk::{
    error::{OTelSdkError, OTelSdkResult},
    trace::{SpanData, SpanExporter},
};
use spicepod::component::runtime::{
    TaskHistoryCapturedContext, TaskHistoryCapturedOutput, TaskHistoryCapturedPlan,
};

use runtime_query_engine::query_engine::{QueryEngine, QueryRequest};

use super::TaskSpan;

/// Label key used to identify plan capture spans in OpenTelemetry traces.
/// This is used to override the default behavior of `captured_output` processing to ensure that
/// plan capture spans always retain their output.
const PLAN_CAPTURE_LABEL: &str = "plan_capture";
const REDACTED_TASK_HISTORY_VALUE: &str = "[redacted]";
static REDACTED_TASK_HISTORY_VALUE_ARC: LazyLock<Arc<str>> =
    LazyLock::new(|| REDACTED_TASK_HISTORY_VALUE.into());
const TRUNCATED_TASK_HISTORY_CONTEXT_CHARS: usize = 4096;
const TRUNCATED_TASK_HISTORY_CONTEXT_SUFFIX: &str = "...[truncated]";

macro_rules! extract_attr {
    ($span:expr, $key:expr) => {
        $span.events.iter().find_map(|event| {
            let event_attr_idx = event
                .attributes
                .iter()
                .position(|kv| kv.key.as_str() == $key)?;
            Some(event.attributes[event_attr_idx].value.as_str().into())
        })
    };
}

/// Hook for rewriting an `OTel` [`SpanData`] before the `task_history`
/// exporter converts it to a row. Implementors can adjust timestamps,
/// inject attributes, redact fields, etc. Transforms run in registration
/// order, so later transforms observe the effects of earlier ones.
///
/// Implementations should be cheap (each runs per span per export batch)
/// and should be no-ops for spans they don't recognize.
pub trait SpanTransform: Send + Sync {
    fn transform(&self, span: &mut SpanData);
}

/// Hook expressing a retention dependency between spans in a batch.
///
/// When a rule returns `Some(parent_id)` for a span, the span is kept
/// iff the span with `parent_id` was kept by the exporter's base rules
/// (the `PLAN_CAPTURE_LABEL` short-circuit and the `min_sql_duration_ms`
/// filter). Returning `None` leaves the span subject to base rules on
/// its own merits.
///
/// Rules are evaluated in registration order; the first rule returning
/// `Some` wins. This expresses cases like "a child summary span should
/// be written if and only if its parent query span is also written" —
/// without baking that policy into the exporter itself.
pub trait SpanRetention: Send + Sync {
    fn parent_dependency(&self, span: &TaskSpan) -> Option<Arc<str>>;
}

#[derive(Clone)]
pub struct TaskHistoryExporter {
    df: Arc<dyn QueryEngine>,
    captured_output: TaskHistoryCapturedOutput,
    captured_context: TaskHistoryCapturedContext,
    min_sql_duration_ms: Option<f64>,
    captured_plan: TaskHistoryCapturedPlan,
    min_plan_duration_ms: Option<f64>,
    /// The node ID (advertise address) for this node.
    /// Only populated in cluster mode.
    node_id: Option<Arc<str>>,
    /// Span transforms applied to each `SpanData` before it is converted
    /// to a row. See [`SpanTransform`]. Transforms run in registration
    /// order.
    transforms: Vec<Arc<dyn SpanTransform>>,
    /// Retention dependency rules consulted during the batch retention
    /// decision. See [`SpanRetention`].
    retentions: Vec<Arc<dyn SpanRetention>>,
}

impl Debug for TaskHistoryExporter {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("TaskHistoryExporter").finish()
    }
}

impl TaskHistoryExporter {
    pub fn new(
        df: Arc<dyn QueryEngine>,
        captured_output: TaskHistoryCapturedOutput,
        captured_context: TaskHistoryCapturedContext,
        min_sql_duration_ms: Option<f64>,
        captured_plan: TaskHistoryCapturedPlan,
        min_plan_duration_ms: Option<f64>,
        node_id: Option<Arc<str>>,
    ) -> Self {
        Self {
            df,
            captured_output,
            captured_context,
            min_sql_duration_ms,
            captured_plan,
            min_plan_duration_ms,
            node_id,
            transforms: Vec::new(),
            retentions: Vec::new(),
        }
    }

    /// Append a [`SpanTransform`] that will run on every `SpanData`
    /// processed by this exporter, before conversion to a row.
    ///
    /// Transforms run in the order they were registered. Returns `self`
    /// so the call is chainable on a freshly-built exporter.
    #[must_use]
    pub fn with_transform(mut self, transform: Arc<dyn SpanTransform>) -> Self {
        self.transforms.push(transform);
        self
    }

    /// Append a [`SpanRetention`] rule consulted when deciding which
    /// spans in a batch to write. Rules can declare that a span depends
    /// on another span being retained (e.g., a child summary on its
    /// parent query); they run in registration order, first match wins.
    #[must_use]
    pub fn with_retention(mut self, retention: Arc<dyn SpanRetention>) -> Self {
        self.retentions.push(retention);
        self
    }

    /// Exporter's intrinsic retention rule. A span is kept by base
    /// rules if it carries the plan-capture label (already filtered by
    /// `min_plan_duration_ms` at emission time) or if it passes the
    /// `min_sql_duration_ms` cutoff.
    fn passes_base_retention(span: &TaskSpan, min_sql_duration_ms: Option<f64>) -> bool {
        if span.labels.contains_key(PLAN_CAPTURE_LABEL) {
            return true;
        }
        min_sql_duration_ms.is_none_or(|min| span.execution_duration_ms >= min)
    }

    fn process_output(&self, output: Arc<str>, force_capture: bool) -> Arc<str> {
        if force_capture {
            return output;
        }

        match self.captured_output {
            TaskHistoryCapturedOutput::None => "".into(),
            TaskHistoryCapturedOutput::Truncated => output,
        }
    }

    fn is_context_task(task: &str) -> bool {
        matches!(
            task,
            "ai_chat"
                | "ai_completion"
                | "responses"
                | "text_embed"
                | "search"
                | "nsql"
                | "scheduled_worker"
        ) || task.starts_with("tool_use::")
    }

    fn process_context_payload(
        captured_context: &TaskHistoryCapturedContext,
        task: &str,
        value: Arc<str>,
    ) -> Arc<str> {
        if value.is_empty() || !Self::is_context_task(task) {
            return value;
        }

        match captured_context {
            TaskHistoryCapturedContext::Redacted => Arc::clone(&REDACTED_TASK_HISTORY_VALUE_ARC),
            TaskHistoryCapturedContext::Truncated => Self::truncate_context_payload(value),
            TaskHistoryCapturedContext::Full => value,
        }
    }

    fn truncate_context_payload(value: Arc<str>) -> Arc<str> {
        let Some((truncate_at, _)) = value
            .char_indices()
            .nth(TRUNCATED_TASK_HISTORY_CONTEXT_CHARS)
        else {
            return value;
        };

        let truncated_value = &value[..truncate_at];
        format!("{truncated_value}{TRUNCATED_TASK_HISTORY_CONTEXT_SUFFIX}").into()
    }

    fn is_valid_span_id(span_id: &Arc<str>) -> bool {
        span_id.len() == 16 && span_id.chars().all(|c| c.is_ascii_hexdigit())
    }

    /// Captures query plans for spans that meet the threshold.
    ///
    /// Only used for `TaskHistoryCapturedPlan::Explain` (plan-only, no body
    /// execution). `ExplainAnalyze` is captured at query completion from the
    /// executed plan — see `datafusion::query::plan_capture`.
    ///
    /// Writes the `plan` row via [`TaskSpan::write`] rather than tracing
    /// instrumentation. The async `BatchSpanProcessor` + `TokioCurrentThread`
    /// test harness runs export on a dedicated worker thread that does not
    /// see the test's thread-local subscriber, so a tracing-emitted `plan`
    /// span would never reach this exporter.
    ///
    /// Called from a `tokio::spawn` after the parent batch is written so
    /// `EXPLAIN` does not block span export throughput.
    async fn capture_plans_async(
        df: Arc<dyn QueryEngine>,
        spans: Vec<TaskSpan>,
        captured_plan: TaskHistoryCapturedPlan,
        _min_plan_duration_ms: Option<f64>,
    ) {
        for span in spans {
            let explain_query = match captured_plan {
                TaskHistoryCapturedPlan::None | TaskHistoryCapturedPlan::ExplainAnalyze => {
                    // ExplainAnalyze is handled at execution time in plan_capture.
                    continue;
                }
                TaskHistoryCapturedPlan::Explain => {
                    format!("EXPLAIN {}", span.input.as_ref())
                }
            };

            let start_time = SystemTime::now();
            match df.execute_query(QueryRequest::new(&explain_query)).await {
                Ok(mut result) => {
                    let mut batches = Vec::new();
                    let mut read_failed = false;
                    while let Some(batch) = result.next().await {
                        match batch {
                            Ok(b) => batches.push(b),
                            Err(e) => {
                                tracing::debug!(
                                    "Failed to read EXPLAIN result batch for span_id {}: {}",
                                    span.span_id,
                                    e
                                );
                                read_failed = true;
                                break;
                            }
                        }
                    }
                    if read_failed {
                        continue;
                    }

                    let output = match pretty_format_batches(&batches) {
                        Ok(formatted) => formatted.to_string(),
                        Err(e) => {
                            tracing::debug!(
                                "Failed to format EXPLAIN output for span_id {}: {}",
                                span.span_id,
                                e
                            );
                            continue;
                        }
                    };

                    let end_time = SystemTime::now();
                    let execution_duration_ms = end_time
                        .duration_since(start_time)
                        .map_or(0.0, |d| d.as_secs_f64() * 1000.0);

                    // Inherit parent correlation (labels, distributed parent,
                    // client trace override) so Explain re-plan rows stay on
                    // the same trace / job filters as the sql_query.
                    let mut labels = span.labels.clone();
                    labels.insert(Arc::from(PLAN_CAPTURE_LABEL), Arc::from("true"));
                    labels.insert(Arc::from("runtime_query"), Arc::from("true"));

                    let plan_task = TaskSpan {
                        trace_id: Arc::clone(&span.trace_id),
                        trace_id_override: span.trace_id_override.clone(),
                        span_id: Arc::from(format!("{:016x}", rand::random::<u64>())),
                        parent_span_id: Some(Arc::clone(&span.span_id)),
                        distributed_parent_id: span.distributed_parent_id.clone(),
                        task: Arc::from("plan"),
                        input: Arc::from(explain_query),
                        captured_output: Some(Arc::from(output)),
                        start_time,
                        end_time,
                        execution_duration_ms,
                        error_message: None,
                        labels,
                        node_id: span.node_id.clone(),
                    };

                    if let Err(e) = TaskSpan::write(Arc::clone(&df), vec![plan_task]).await {
                        tracing::debug!(
                            "Failed to write plan span for parent span_id {}: {e}",
                            span.span_id
                        );
                    }
                }
                Err(e) => {
                    tracing::debug!(
                        "Failed to run EXPLAIN query for span_id {}: {}",
                        span.span_id,
                        e
                    );
                }
            }
        }
    }

    fn span_to_task_span(&self, mut span: SpanData) -> TaskSpan {
        for transform in &self.transforms {
            transform.transform(&mut span);
        }
        let trace_id: Arc<str> = span.span_context.trace_id().to_string().into();
        let span_id: Arc<str> = span.span_context.span_id().to_string().into();
        let parent_span_id: Option<Arc<str>> = if span.parent_span_id == SpanId::INVALID {
            None
        } else {
            Some(span.parent_span_id.to_string().into())
        };
        let task: Arc<str> = extract_attr!(span, "task_override").unwrap_or(span.name.into());
        let input: Arc<str> = Self::process_context_payload(
            &self.captured_context,
            task.as_ref(),
            span.attributes
                .iter()
                .position(|kv| kv.key.as_str() == "input")
                .map_or_else(
                    || "".into(),
                    |idx| span.attributes[idx].value.as_str().into(),
                ),
        );

        // Validated the same way the header that supplies it is, so one id is
        // not accepted at the door and rejected here (or vice versa).
        let trace_id_override: Option<Arc<str>> =
            extract_attr!(span, "trace_id").and_then(|trace_id: Arc<str>| {
                let normalized = runtime_request_context::normalize_trace_id(&trace_id);
                if normalized.is_none() {
                    tracing::warn!("User provided 'trace_id'='{trace_id}' is invalid. Must be a 32 character hex string.");
                }
                normalized
            });

        let distributed_parent_id: Option<Arc<str>> = extract_attr!(span, "parent_id")
            .and_then(|parent_id| if Self::is_valid_span_id(&parent_id) {
                Some(parent_id)
            } else {
                tracing::warn!("User provided 'parent_id'='{}' is a invalid span id. Must be a 32 character hex string.", Arc::clone(&trace_id));
                None
            });

        let start_time = span.start_time;
        let end_time = span.end_time;
        let execution_duration_ms = end_time
            .duration_since(start_time)
            .map_or(0.0, |duration| duration.as_secs_f64() * 1000.0);
        let error_message: Option<Arc<str>> = span
            .events
            .iter()
            .position(|event| {
                event
                    .attributes
                    .iter()
                    .any(|kv| kv.key.as_str() == "level" && kv.value.as_str() == "ERROR")
            })
            .map(|idx| span.events[idx].name.clone().into());
        let mut labels: HashMap<Arc<str>, Arc<str>> = span
            .attributes
            .iter()
            .filter(|kv| filter_event_keys(kv.key.as_str()))
            .map(|kv| (kv.key.as_str().into(), kv.value.as_str().into()))
            .collect();

        let event_labels: HashMap<Arc<str>, Arc<str>> = span
            .events
            .iter()
            .filter(|event| event.name == "labels")
            .flat_map(|event| {
                event
                    .attributes
                    .iter()
                    .filter(|kv| filter_event_keys(kv.key.as_str()))
                    .map(|kv| (kv.key.as_str().into(), kv.value.as_str().into()))
            })
            .collect();

        labels.extend(event_labels);

        let runtime_query = span.attributes.iter().any(|kv| {
            kv.key.as_str() == "runtime_query"
                && matches!(kv.value, opentelemetry::Value::Bool(true))
        });
        if runtime_query {
            labels.insert("runtime_query".into(), "true".into());
        }

        let plan_capture = span.attributes.iter().any(|kv| {
            kv.key.as_str() == PLAN_CAPTURE_LABEL
                && matches!(kv.value, opentelemetry::Value::Bool(true))
        });
        if plan_capture {
            labels.insert(PLAN_CAPTURE_LABEL.into(), "true".into());
        }

        let captured_output: Option<Arc<str>> = extract_attr!(span, "captured_output")
            .map(|output| self.process_output(output, plan_capture))
            .map(|output| {
                Self::process_context_payload(&self.captured_context, task.as_ref(), output)
            });

        // Remove trace_id and parent_id from `labels`, if they exist (no issue if they don't).
        labels.remove(&Into::<Arc<str>>::into("trace_id"));
        labels.remove(&Into::<Arc<str>>::into("parent_id"));

        TaskSpan {
            trace_id,
            trace_id_override,
            span_id,
            parent_span_id,
            distributed_parent_id,
            task,
            input,
            captured_output,
            start_time,
            end_time,
            execution_duration_ms,
            error_message,
            labels,
            node_id: self.node_id.clone(),
        }
    }
}

impl SpanExporter for TaskHistoryExporter {
    fn export(
        &self,
        batch: Vec<SpanData>,
    ) -> impl std::future::Future<Output = OTelSdkResult> + Send {
        let min_sql_duration_ms = self.min_sql_duration_ms;
        let captured_plan = self.captured_plan.clone();
        let min_plan_duration_ms = self.min_plan_duration_ms;
        let df = Arc::clone(&self.df);

        let candidates: Vec<TaskSpan> = batch
            .into_iter()
            .map(|span| self.span_to_task_span(span))
            .collect();

        // Compute the set of spans that pass the exporter's base
        // retention rules: anything explicitly tagged for plan capture,
        // or anything passing the `min_sql_duration_ms` filter. Stored
        // by span id so dependency rules can ask "was my parent kept?".
        let base_retained_ids: std::collections::HashSet<Arc<str>> = candidates
            .iter()
            .filter(|task_span| Self::passes_base_retention(task_span, min_sql_duration_ms))
            .map(|task_span| Arc::clone(&task_span.span_id))
            .collect();

        let retentions = self.retentions.clone();
        let should_include = |task_span: &TaskSpan| {
            // Spans with an explicit parent dependency (e.g.,
            // `ballista_stage` rows on their parent query) inherit that
            // parent's decision; otherwise they would be orphans whose
            // `parent_span_id` references a row that was never written.
            for rule in &retentions {
                if let Some(parent_id) = rule.parent_dependency(task_span) {
                    return base_retained_ids.contains(&parent_id);
                }
            }
            base_retained_ids.contains(&task_span.span_id)
        };
        let spans: Vec<TaskSpan> = candidates.into_iter().filter(should_include).collect();

        async move {
            // ExplainAnalyze is captured at execution time (see
            // `datafusion::query::plan_capture`); only the Explain
            // (plan-only) path still re-plans in the background here.
            if !matches!(captured_plan, TaskHistoryCapturedPlan::Explain) {
                return TaskSpan::write(Arc::clone(&df), spans)
                    .await
                    .map_err(|e| OTelSdkError::InternalFailure(e.to_string()));
            }

            // Filter spans that need plan capture before cloning
            let should_capture_plan = |span: &TaskSpan| {
                crate::datafusion::query::plan_capture::should_capture_explain_plan(
                    span.task.as_ref(),
                    span.input.as_ref(),
                    span.execution_duration_ms,
                    min_plan_duration_ms,
                )
            };

            // Clone only the spans that need plan capture
            let spans_for_plan: Vec<TaskSpan> = spans
                .iter()
                .filter(|s| should_capture_plan(s))
                .cloned()
                .collect();

            // Write all spans first
            TaskSpan::write(Arc::clone(&df), spans)
                .await
                .map_err(|e| OTelSdkError::InternalFailure(e.to_string()))?;

            // Spawn Explain re-plan so export stays non-blocking. Plan rows are
            // written via `TaskSpan::write` (not tracing), so the worker thread
            // does not need the test's thread-local subscriber.
            if !spans_for_plan.is_empty() {
                let df_clone = Arc::clone(&df);
                let num_spans = spans_for_plan.len();
                tokio::spawn(async move {
                    Self::capture_plans_async(
                        df_clone,
                        spans_for_plan,
                        captured_plan,
                        min_plan_duration_ms,
                    )
                    .await;
                    tracing::trace!("Plan capture completed successfully for {num_spans} queries");
                });
            }

            Ok(())
        }
    }
}

const AUTOGENERATED_LABELS: [&str; 12] = [
    "thread.id",
    "code.namespace",
    "code.lineno",
    "idle_ns",
    "busy_ns",
    "runtime_query",
    "plan_capture",
    "target",
    "code.filepath",
    "level",
    "thread.name",
    "input",
];

const SENSITIVE_LABELS: [&str; 2] = ["prompt", "metadata"];

/// Filters out auto-generated attributes by the tracing/OpenTelemetry instrumentation appearing as labels
fn filter_event_keys(event_key: &str) -> bool {
    !AUTOGENERATED_LABELS.contains(&event_key) && !SENSITIVE_LABELS.contains(&event_key)
}

#[cfg(test)]
mod tests {
    use super::{
        REDACTED_TASK_HISTORY_VALUE, TRUNCATED_TASK_HISTORY_CONTEXT_CHARS,
        TRUNCATED_TASK_HISTORY_CONTEXT_SUFFIX, TaskHistoryExporter,
    };
    use spicepod::component::runtime::TaskHistoryCapturedContext;
    use std::sync::Arc;

    #[test]
    fn process_context_payload_truncates_context_by_default() {
        let payload: Arc<str> =
            format!("{}z", "a".repeat(TRUNCATED_TASK_HISTORY_CONTEXT_CHARS)).into();

        assert_eq!(
            TaskHistoryExporter::process_context_payload(
                &TaskHistoryCapturedContext::Truncated,
                "nsql",
                payload,
            ),
            Arc::<str>::from(format!(
                "{}{TRUNCATED_TASK_HISTORY_CONTEXT_SUFFIX}",
                "a".repeat(TRUNCATED_TASK_HISTORY_CONTEXT_CHARS)
            ))
        );
    }

    #[test]
    fn process_context_payload_preserves_full_context() {
        let payload: Arc<str> =
            format!("{}z", "a".repeat(TRUNCATED_TASK_HISTORY_CONTEXT_CHARS)).into();

        assert_eq!(
            TaskHistoryExporter::process_context_payload(
                &TaskHistoryCapturedContext::Full,
                "nsql",
                Arc::clone(&payload),
            ),
            payload
        );
    }

    #[test]
    fn process_context_payload_redacts_context_when_configured() {
        assert_eq!(
            TaskHistoryExporter::process_context_payload(
                &TaskHistoryCapturedContext::Redacted,
                "tool_use::table_schema",
                "{}".into(),
            ),
            Arc::<str>::from(REDACTED_TASK_HISTORY_VALUE)
        );
    }

    #[test]
    fn process_context_payload_preserves_non_context_tasks() {
        let payload: Arc<str> = "SELECT COUNT(*) FROM item".into();

        assert_eq!(
            TaskHistoryExporter::process_context_payload(
                &TaskHistoryCapturedContext::Redacted,
                "sql_query",
                Arc::clone(&payload),
            ),
            payload
        );
    }

    #[test]
    fn filter_event_keys_omits_sensitive_labels() {
        assert!(!super::filter_event_keys("prompt"));
        assert!(!super::filter_event_keys("metadata"));
    }
}
