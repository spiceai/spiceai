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

use runtime_request_context::TraceParent;
use tracing::Span;

/// Use the span context from the traceparent header to override the `trace_id` & `parent_span_id` columns in the task history table.
///
/// This should not be used for any span within a HTTP API that has [HTTP Spans](https://opentelemetry.io/docs/specs/semconv/http/http-spans/) created, as they are incompatible (both the `span` input and the span created for the HTTP handler will have the same `parent_span_id`, even though the `input` span would become a child of the HTTP span)
pub fn override_task_history_with_trace_parent(span: &Span, value: &TraceParent) {
    tracing::info!(target: "task_history", parent: span, trace_id = %value.trace_id, parent_id = %value.span_id);
}
