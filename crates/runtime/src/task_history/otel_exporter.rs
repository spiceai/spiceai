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
use std::sync::Arc;
use tracing::Instrument;

use opentelemetry::trace::SpanId;
use opentelemetry_sdk::{
    error::{OTelSdkError, OTelSdkResult},
    trace::{SpanData, SpanExporter},
};
use spicepod::component::runtime::{TaskHistoryCapturedOutput, TaskHistoryCapturedPlan};

use crate::datafusion::DataFusion;

use super::TaskSpan;

/// Label key used to identify plan capture spans in OpenTelemetry traces.
/// This is used to override the default behavior of `captured_output` processing to ensure that
/// plan capture spans always retain their output.
const PLAN_CAPTURE_LABEL: &str = "plan_capture";

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

#[derive(Clone)]
pub struct TaskHistoryExporter {
    df: Arc<DataFusion>,
    captured_output: TaskHistoryCapturedOutput,
    min_sql_duration_ms: Option<f64>,
    captured_plan: TaskHistoryCapturedPlan,
    min_plan_duration_ms: Option<f64>,
}

impl Debug for TaskHistoryExporter {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("TaskHistoryExporter").finish()
    }
}

impl TaskHistoryExporter {
    pub fn new(
        df: Arc<DataFusion>,
        captured_output: TaskHistoryCapturedOutput,
        min_sql_duration_ms: Option<f64>,
        captured_plan: TaskHistoryCapturedPlan,
        min_plan_duration_ms: Option<f64>,
    ) -> Self {
        Self {
            df,
            captured_output,
            min_sql_duration_ms,
            captured_plan,
            min_plan_duration_ms,
        }
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

    fn is_valid_span_id(span_id: &Arc<str>) -> bool {
        span_id.len() == 16 && span_id.chars().all(|c| c.is_ascii_hexdigit())
    }

    fn is_valid_traceid(trace_id: &Arc<str>) -> bool {
        trace_id.len() == 32 && trace_id.chars().all(|c| c.is_ascii_hexdigit())
    }

    /// Asynchronously captures query plans for spans that meet the threshold.
    /// This runs on a separate tokio task to avoid blocking the original query.
    /// The spans passed to this method have already been filtered by the caller.
    ///
    /// For each span, this runs an EXPLAIN query which will create a new `task_history` entry
    /// with `task="sql_query"` and the original query's `span_id` as `parent_span_id`.
    /// The output is always captured in full regardless of the global `captured_output` setting.
    async fn capture_plans_async(
        df: Arc<DataFusion>,
        spans: Vec<TaskSpan>,
        captured_plan: TaskHistoryCapturedPlan,
        _min_plan_duration_ms: Option<f64>,
    ) {
        for span in spans {
            let explain_query = match captured_plan {
                TaskHistoryCapturedPlan::None => continue,
                TaskHistoryCapturedPlan::Explain => {
                    format!("EXPLAIN {}", span.input.as_ref())
                }
                TaskHistoryCapturedPlan::ExplainAnalyze => {
                    format!("EXPLAIN ANALYZE {}", span.input.as_ref())
                }
            };

            // Create a tracing span for the plan capture with "plan" task override
            // This will create a task_history entry as a child of the original query
            let plan_span = tracing::span!(
                target: "task_history",
                tracing::Level::INFO,
                "plan",
                input = %explain_query,
                runtime_query = true,
                plan_capture = true
            );
            plan_span.record("parent_id", span.span_id.as_ref());

            // Run EXPLAIN query within the span context so it appears as a child task
            async {
                match df.query_builder(&explain_query).build().run().await {
                    Ok(mut result) => {
                        // Collect all record batches from the result stream
                        let mut batches = Vec::new();
                        while let Some(batch) = result.data.next().await {
                            match batch {
                                Ok(b) => batches.push(b),
                                Err(e) => {
                                    tracing::debug!(
                                        "Failed to read EXPLAIN result batch for span_id {}: {}",
                                        span.span_id,
                                        e
                                    );
                                    return;
                                }
                            }
                        }

                        match pretty_format_batches(&batches) {
                            Ok(formatted) => {
                                let output = formatted.to_string();
                                tracing::info!(target: "task_history", captured_output = %output);
                            }
                            Err(e) => {
                                tracing::debug!(
                                    "Failed to format EXPLAIN output for span_id {}: {}",
                                    span.span_id,
                                    e
                                );
                            }
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
            .instrument(plan_span)
            .await;
        }
    }

    fn span_to_task_span(&self, span: SpanData) -> TaskSpan {
        let trace_id: Arc<str> = span.span_context.trace_id().to_string().into();
        let span_id: Arc<str> = span.span_context.span_id().to_string().into();
        let parent_span_id: Option<Arc<str>> = if span.parent_span_id == SpanId::INVALID {
            None
        } else {
            Some(span.parent_span_id.to_string().into())
        };
        let task: Arc<str> = extract_attr!(span, "task_override").unwrap_or(span.name.into());
        let input: Arc<str> = span
            .attributes
            .iter()
            .position(|kv| kv.key.as_str() == "input")
            .map_or_else(
                || "".into(),
                |idx| span.attributes[idx].value.as_str().into(),
            );

        let trace_id_override: Option<Arc<str>> = extract_attr!(span, "trace_id")
            .and_then(|trace_id| if Self::is_valid_traceid(&trace_id) {
                Some(trace_id)
            } else {
                tracing::warn!("User provided 'trace_id'='{}' is invalid. Must be a 32 character hex string.", Arc::clone(&trace_id));
                None
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
            .map(|output| self.process_output(output, plan_capture));

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

        let should_include = |task_span: &TaskSpan| {
            // Always include plan capture spans regardless of duration since they are already
            // filtered by min_plan_duration when created.
            if task_span.labels.contains_key(PLAN_CAPTURE_LABEL) {
                return true;
            }
            min_sql_duration_ms.is_none_or(|min| task_span.execution_duration_ms >= min)
        };
        let spans: Vec<TaskSpan> = batch
            .into_iter()
            .map(|span| self.span_to_task_span(span))
            .filter(should_include)
            .collect();

        async move {
            // Separate logic: if plan capture is disabled, write all spans directly
            if matches!(captured_plan, TaskHistoryCapturedPlan::None) {
                return TaskSpan::write(Arc::clone(&df), spans)
                    .await
                    .map_err(|e| OTelSdkError::InternalFailure(e.to_string()));
            }

            // Filter spans that need plan capture before cloning
            let should_capture_plan = |span: &TaskSpan| {
                // Check min_plan_duration threshold
                if !min_plan_duration_ms
                    .is_none_or(|min_duration| span.execution_duration_ms >= min_duration)
                {
                    return false;
                }

                // Only capture plans for sql_query tasks with non-empty input
                if span.task.as_ref() != "sql_query" || span.input.is_empty() {
                    return false;
                }

                // Don't capture plans for queries that are already EXPLAIN queries
                let input_trimmed = span.input.trim_start();
                !(input_trimmed.len() >= 7 && input_trimmed[..7].eq_ignore_ascii_case("explain"))
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

            // Spawn async task to capture plans for filtered spans
            // The task runs in the background without blocking the export operation
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

/// Filters out auto-generated attributes by the tracing/OpenTelemetry instrumentation appearing as labels
fn filter_event_keys(event_key: &str) -> bool {
    !AUTOGENERATED_LABELS.contains(&event_key)
}
