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

use std::collections::HashMap;
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use super::DEFAULT_TASK_HISTORY_TABLE;
use crate::datafusion::SPICE_RUNTIME_SCHEMA;

use arrow::array::Array;
use futures::future::BoxFuture;
use opentelemetry::trace::{SpanId, TraceError};
use opentelemetry_sdk::export::trace::{ExportResult, SpanData, SpanExporter};
use spicepod::component::runtime::{TaskHistoryCapturedOutput, TaskHistoryCapturedPlan};

use crate::datafusion::DataFusion;

use super::TaskSpan;

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

    fn process_output(&self, output: Arc<str>) -> Arc<str> {
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
    async fn capture_plans_async(
        df: Arc<DataFusion>,
        spans: Vec<TaskSpan>,
        captured_plan: TaskHistoryCapturedPlan,
        min_plan_duration_ms: Option<f64>,
    ) {
        for span in spans {
            // Skip if execution duration doesn't meet min_plan_duration threshold
            if let Some(min_duration) = min_plan_duration_ms {
                if span.execution_duration_ms < min_duration {
                    continue;
                }
            }

            // Only capture plans for tasks with exact name "sql_query"
            if span.task.as_ref() != "sql_query" && !span.input.is_empty() {
                continue;
            }

            let explain_query = match captured_plan {
                TaskHistoryCapturedPlan::None => continue,
                TaskHistoryCapturedPlan::Explain => {
                    format!("EXPLAIN {}", span.input.as_ref())
                }
                TaskHistoryCapturedPlan::ExplainAnalyze => {
                    format!("EXPLAIN ANALYZE {}", span.input.as_ref())
                }
            };

            // Run EXPLAIN query asynchronously
            match df.query_builder(&explain_query).build().run().await {
                Ok(mut stream) => {
                    // Collect all result rows
                    let mut plan_output = String::new();
                    match futures::TryStreamExt::try_collect::<Vec<_>>(stream.data).await {
                        Ok(batches) => {
                            for batch in batches {
                                // Format the RecordBatch into a readable string
                                for row_idx in 0..batch.num_rows() {
                                    for col_idx in 0..batch.num_columns() {
                                        let column = batch.column(col_idx);
                                        if let Some(str_array) = column
                                            .as_any()
                                            .downcast_ref::<arrow::array::StringArray>(
                                        ) {
                                            if !str_array.is_null(row_idx) {
                                                let value = str_array.value(row_idx);
                                                plan_output.push_str(value);
                                                plan_output.push('\n');
                                            }
                                        }
                                    }
                                }
                            }

                            // Update the task_history table with the captured plan
                            let update_query = format!(
                                "UPDATE {}.{} SET captured_plan = '{}' WHERE span_id = '{}'",
                                SPICE_RUNTIME_SCHEMA,
                                DEFAULT_TASK_HISTORY_TABLE,
                                plan_output.replace('\'', "''"), // Escape single quotes
                                span.span_id
                            );

                            if let Err(e) = df.query_builder(&update_query).build().run().await {
                                tracing::warn!(
                                    "Failed to update captured_plan for span_id {}: {}",
                                    span.span_id,
                                    e
                                );
                            }
                        }
                        Err(e) => {
                            tracing::debug!(
                                "Failed to collect EXPLAIN results for span_id {}: {}",
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

        let captured_output: Option<Arc<str>> =
            extract_attr!(span, "captured_output").map(|output| self.process_output(output));

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
            captured_plan: None, // Will be populated asynchronously if needed
            start_time,
            end_time,
            execution_duration_ms,
            error_message,
            labels,
        }
    }
}

impl SpanExporter for TaskHistoryExporter {
    fn export(&mut self, batch: Vec<SpanData>) -> BoxFuture<'static, ExportResult> {
        let min_sql_duration_ms = self.min_sql_duration_ms;
        let captured_plan = self.captured_plan.clone();
        let min_plan_duration_ms = self.min_plan_duration_ms;
        let df = Arc::clone(&self.df);

        let spans: Vec<TaskSpan> = batch
            .into_iter()
            .map(|span| self.span_to_task_span(span))
            .filter(|task_span| {
                min_sql_duration_ms.map_or(true, |min| task_span.execution_duration_ms >= min)
            })
            .collect();

        Box::pin(async move {
            // First, write the spans without captured_plan
            TaskSpan::write(Arc::clone(&df), spans.clone())
                .await
                .map_err(|e| TraceError::Other(Box::new(e)))?;

            // If captured_plan is enabled, spawn async tasks to capture plans
            if !matches!(captured_plan, TaskHistoryCapturedPlan::None) {
                let df_clone = Arc::clone(&df);
                tokio::spawn(async move {
                    Self::capture_plans_async(df_clone, spans, captured_plan, min_plan_duration_ms)
                        .await;
                });
            }

            Ok(())
        })
    }
}

const AUTOGENERATED_LABELS: [&str; 11] = [
    "thread.id",
    "code.namespace",
    "code.lineno",
    "idle_ns",
    "busy_ns",
    "runtime_query",
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
