/*
Copyright 2024 The Spice.ai OSS Authors

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

use futures::future::BoxFuture;
use opentelemetry::trace::{SpanId, TraceError};
use opentelemetry_sdk::export::trace::{ExportResult, SpanData, SpanExporter};

use crate::datafusion::DataFusion;

use super::TaskSpan;

#[derive(Clone)]
pub struct TaskHistoryExporter {
    df: Arc<DataFusion>,
}

impl Debug for TaskHistoryExporter {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("TaskHistoryExporter").finish()
    }
}

impl TaskHistoryExporter {
    pub fn new(df: Arc<DataFusion>) -> Self {
        Self { df }
    }
}

impl SpanExporter for TaskHistoryExporter {
    fn export(&mut self, batch: Vec<SpanData>) -> BoxFuture<'static, ExportResult> {
        let spans = batch.into_iter().map(span_to_task_span).collect();
        let df = Arc::clone(&self.df);
        Box::pin(async move {
            TaskSpan::write(df, spans)
                .await
                .map_err(|e| TraceError::Other(Box::new(e)))
        })
    }
}

fn span_to_task_span(span: SpanData) -> TaskSpan {
    let trace_id: Arc<str> = span.span_context.trace_id().to_string().into();
    let span_id: Arc<str> = span.span_context.span_id().to_string().into();
    let parent_span_id: Option<Arc<str>> = if span.parent_span_id == SpanId::INVALID {
        None
    } else {
        Some(span.parent_span_id.to_string().into())
    };
    let task: Arc<str> = span.name.into();
    let input: Arc<str> = span
        .attributes
        .iter()
        .position(|kv| kv.key.as_str() == "input")
        .map_or_else(
            || "".into(),
            |idx| &span.attributes[idx].value.as_str().into(),
        );
    let truncated_output: Option<Arc<str>> = span.events.iter().find_map(|event| {
        let event_attr_idx = event
            .attributes
            .iter()
            .position(|kv| kv.key.as_str() == "truncated_output")?;
        Some(event.attributes[event_attr_idx].value.as_str().into())
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
        .map(|idx| {
            span.events[idx]
                .attributes
                .iter()
                .find(|kv| kv.key.as_str() == "message")
                .map_or_else(|| "".into(), |kv| kv.value.as_str().into())
        });
    let labels: HashMap<Arc<str>, Arc<str>> = span
        .attributes
        .iter()
        .filter(|kv| kv.key.as_str() != "input")
        .map(|kv| (kv.key.as_str().into(), kv.value.as_str().into()))
        .collect();

    TaskSpan {
        trace_id,
        span_id,
        parent_span_id,
        task,
        input,
        truncated_output,
        start_time,
        end_time,
        execution_duration_ms,
        error_message,
        labels,
    }
}
