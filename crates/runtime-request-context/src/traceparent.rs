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
#![allow(clippy::missing_errors_doc)]

use axum::http::HeaderMap;
use opentelemetry::trace::{SpanId, TraceId};

const MAX_VERSION: u8 = 254;
/// Name of the W3C trace context header.
pub const TRACEPARENT_HEADER: &str = "traceparent";

/// Represents W3C `traceparent` header values.
pub struct TraceParent {
    pub trace_id: TraceId,
    pub span_id: SpanId,
}

impl std::fmt::Display for TraceParent {
    /// Formats the value to send downstream: version `00`, with the sampled flag set.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "00-{}-{}-01", self.trace_id, self.span_id)
    }
}

pub fn extract_trace_parent(headers: &HeaderMap) -> Result<Option<TraceParent>, String> {
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

    Ok(Some(TraceParent { trace_id, span_id }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_round_trips_through_extract() {
        let value = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        let mut headers = HeaderMap::new();
        headers.insert(
            TRACEPARENT_HEADER,
            value.parse().expect("valid header value"),
        );
        let trace_parent = extract_trace_parent(&headers)
            .expect("valid traceparent")
            .expect("traceparent present");
        assert_eq!(trace_parent.to_string(), value);
    }
}
