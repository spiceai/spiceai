/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! The client-supplied trace id: the correlation id a caller pins so the work
//! its request produces can be found again, both in `runtime.task_history` and
//! in the runtime's own log.
//!
//! Two headers carry it. [`SPICE_TRACE_ID_HEADER`] takes a bare id and is the
//! one to reach for when the caller is not part of a distributed trace; W3C
//! `traceparent` (see [`crate::traceparent`]) carries the same id plus the span
//! the request is a child of. When a request sends both, the bare header wins —
//! it is set deliberately by the caller, whereas a `traceparent` is routinely
//! injected by a proxy or APM agent that knows nothing about this request.

use std::sync::Arc;

use axum::http::HeaderMap;
use opentelemetry::trace::TraceId;

/// Header a client sets to pin the trace id for a request without speaking W3C
/// trace context. The value is a 32-character hexadecimal (16-byte) id, the
/// same shape as the `trace_id` column of `runtime.task_history`.
pub const SPICE_TRACE_ID_HEADER: &str = "spice-trace-id";

/// Length of a trace id in hexadecimal characters (16 bytes).
pub const TRACE_ID_HEX_LEN: usize = 32;

/// Normalizes a client-supplied trace id to the form the runtime records:
/// 32 lowercase hexadecimal characters.
///
/// Returns `None` for anything else, including the all-zero id — W3C reserves
/// it as "invalid", and it correlates nothing because every request that sent
/// it would share the one id.
#[must_use]
pub fn normalize_trace_id(value: &str) -> Option<Arc<str>> {
    // The length and digit checks are not redundant with `from_hex`, which is
    // `u128::from_str_radix` underneath and so also accepts a short value or a
    // leading sign. `TraceId`'s `Display` is what renders the canonical 32
    // lowercase hex characters the `trace_id` column holds.
    if value.len() != TRACE_ID_HEX_LEN || !value.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    let trace_id = TraceId::from_hex(value).ok()?;
    (trace_id != TraceId::INVALID).then(|| Arc::from(trace_id.to_string()))
}

/// Reads [`SPICE_TRACE_ID_HEADER`] from `headers`. `Ok(None)` means the header
/// was absent.
///
/// # Errors
///
/// Describes a header that was present but unusable. Callers warn and fall back
/// to generating an id, so a malformed correlation id degrades correlation
/// rather than failing the request.
pub fn extract_trace_id(headers: &HeaderMap) -> Result<Option<Arc<str>>, String> {
    let Some(header_value) = headers.get(SPICE_TRACE_ID_HEADER) else {
        return Ok(None);
    };
    let header_value = header_value.to_str().map_err(|e| {
        format!("In {SPICE_TRACE_ID_HEADER} header, expected a string value, got {e}")
    })?;

    normalize_trace_id(header_value).map(Some).ok_or_else(|| {
        format!(
            "In {SPICE_TRACE_ID_HEADER} header, invalid trace id '{header_value}'. \
             Expected {TRACE_ID_HEX_LEN} hexadecimal characters, not all zero."
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    const VALID: &str = "4bf92f3577b34da6a3ce929d0e0e4736";

    #[test]
    fn normalizes_a_well_formed_id() {
        assert_eq!(normalize_trace_id(VALID).as_deref(), Some(VALID));
    }

    /// A caller that upper-cases its hex still gets the id it asked for: the
    /// runtime records the lowercase form, so its own log and the
    /// `task_history` row agree on one spelling.
    #[test]
    fn lowercases_uppercase_hex() {
        assert_eq!(
            normalize_trace_id(&VALID.to_ascii_uppercase()).as_deref(),
            Some(VALID)
        );
    }

    #[test]
    fn rejects_ids_that_cannot_correlate() {
        // Wrong length.
        assert!(normalize_trace_id("4bf92f35").is_none());
        assert!(normalize_trace_id(&format!("{VALID}00")).is_none());
        // Not hexadecimal.
        assert!(normalize_trace_id("4bf92f3577b34da6a3ce929d0e0e473z").is_none());
        // W3C's "invalid" id — shared by every request that sends it.
        assert!(normalize_trace_id(&"0".repeat(TRACE_ID_HEX_LEN)).is_none());
        assert!(normalize_trace_id("").is_none());
    }

    #[test]
    fn extracts_the_header_when_present() {
        let mut headers = HeaderMap::new();
        assert_eq!(extract_trace_id(&headers), Ok(None));

        headers.insert(SPICE_TRACE_ID_HEADER, HeaderValue::from_static(VALID));
        assert_eq!(
            extract_trace_id(&headers).expect("valid header").as_deref(),
            Some(VALID)
        );
    }

    #[test]
    fn reports_a_malformed_header_rather_than_ignoring_it() {
        let mut headers = HeaderMap::new();
        headers.insert(SPICE_TRACE_ID_HEADER, HeaderValue::from_static("nope"));

        let err = extract_trace_id(&headers).expect_err("malformed header should be reported");
        assert!(
            err.contains(SPICE_TRACE_ID_HEADER) && err.contains("nope"),
            "the warning must name the header and the offending value, got: {err}"
        );
    }
}
