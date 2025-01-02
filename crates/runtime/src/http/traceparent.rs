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

use axum::http::HeaderMap;
use opentelemetry::trace::{SpanId, TraceId};
use tracing::Span;

const MAX_VERSION: u8 = 254;
const TRACEPARENT_HEADER: &str = "traceparent";

/// Use the span context from the traceparent header to override the `trace_id` & `parent_span_id` columns in the task history table.
///
/// Errors are not returned, invalid traceparent headers must be ignored.
///
/// This should not be used for any span within a HTTP API that has [HTTP Spans](https://opentelemetry.io/docs/specs/semconv/http/http-spans/) created, as they are incompatible (both the `span` input and the span created for the HTTP handler will have the same `parent_span_id`, even though the `input` span would become a child of the HTTP span).
pub(super) fn override_task_history_with_traceparent(span: &Span, headers: &HeaderMap) {
    match extract_traceparent(headers) {
        Ok(Some((trace_id, span_id))) => {
            tracing::info!(target: "task_history", parent: span, trace_id = %trace_id, parent_id = %span_id);
        }
        Err(e) => {
            tracing::warn!("Recieved invalid `traceparent` HTTP header: {e}");
        }
        _ => {}
    }
}

fn extract_traceparent(headers: &HeaderMap) -> Result<Option<(TraceId, SpanId)>, String> {
    let Some(header_value) = headers.get(TRACEPARENT_HEADER).map(|v| v.to_str()) else {
        return Ok(None);
    };
    let header_value = header_value.map_err(|e| {
        format!("In traceparent header, invalid traceparent header value, expected string, got {e}")
    })?;
    let parts = header_value.split_terminator('-').collect::<Vec<&str>>();
    // Ensure parts are not out of range.
    if parts.len() < 4 {
        return Err(format!(
            "In traceparent header, invalid traceparent header, expected 4 parts, got {}",
            parts.len()
        ));
    }

    // Ensure version is within range, for version 0 there must be 4 parts.
    let version = u8::from_str_radix(parts[0], 16).map_err(|e| {
        format!("In traceparent header, invalid traceparent version, expected hex value, got {e}")
    })?;
    if version > MAX_VERSION || version == 0 && parts.len() != 4 {
        return Err(format!(
            "In traceparent header, invalid traceparent version {version}"
        ));
    }

    // Ensure trace id is lowercase
    if parts[1].chars().any(|c| c.is_ascii_uppercase()) {
        return Err(format!(
            "In traceparent header, invalid trace id. Expected lowercase hex value, got {}",
            parts[1]
        ));
    }

    // Parse trace id section
    let trace_id = TraceId::from_hex(parts[1]).map_err(|e| {
        format!("In traceparent header, invalid trace id. Expected 32 character hex value: {e}")
    })?;

    // Ensure span id is lowercase
    if parts[2].chars().any(|c| c.is_ascii_uppercase()) {
        return Err(format!(
            "In traceparent header, invalid span id. Expected lowercase hex value, got {}",
            parts[2]
        ));
    }

    // Parse span id section
    let span_id = SpanId::from_hex(parts[2]).map_err(|e| {
        format!("In traceparent header, invalid span id. Expected 16 character hex value: {e}")
    })?;

    // Parse trace flags section solely to ensure they're valid.
    let _ = u8::from_str_radix(parts[3], 16).map_err(|e| {
        format!("In traceparent header, invalid trace flags. Expected hex value, got {e}")
    })?;

    Ok(Some((trace_id, span_id)))
}
