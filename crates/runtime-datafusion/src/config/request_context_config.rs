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

//! `ConfigExtension` that carries `RequestContext` trace fields through
//! transport boundaries that round-trip a `DataFusion` `SessionConfig` as
//! opaque key/value pairs — primarily the Ballista `TaskDefinition` props
//! used to ship per-job session config to executors.
//!
//! The receiver reconstructs an `Arc<RequestContext>` via
//! [`SpiceRequestContextConfig::to_request_context`] so executor-side exec
//! nodes (`BytesProcessedExec` and similar) attribute metrics and any
//! emitted task-history rows to the originating request.
//!
//! The propagated `span_id` is the sender's *current* span; on the receiver
//! it becomes the parent of any new spans created from the reconstructed
//! context (standard W3C trace-context semantics).

use datafusion::common::extensions_options;
use datafusion::config::ConfigExtension;
use opentelemetry::trace::{SpanId, TraceId};
use runtime_request_context::{Protocol, RequestContext, RequestContextBuilder, TraceParent};
use std::sync::Arc;

extensions_options! {
    /// Trace fields propagated from a scheduler (or any upstream sender) to
    /// downstream executors via a `SessionConfig` round-trip.
    pub struct SpiceRequestContextConfig {
        /// Source request protocol, encoded as `Protocol as u8`. `None` if
        /// unset (executor falls back to `Protocol::Internal`).
        pub protocol: Option<u8>, default = None
        /// W3C trace id (32 lowercase hex chars) the upstream request pinned,
        /// from either `traceparent` or the bare trace-id header. `None` if
        /// the upstream request pinned no id — the executor then numbers its
        /// own tasks, as it does for any unpinned work.
        pub trace_id: Option<String>, default = None
        /// W3C span id of the *current* span on the sender (16 lowercase
        /// hex chars). On the receiver this is the parent of any new spans
        /// created from the reconstructed `RequestContext`.
        pub span_id: Option<String>, default = None
    }
}

impl ConfigExtension for SpiceRequestContextConfig {
    const PREFIX: &'static str = "spice_ctx";
}

impl SpiceRequestContextConfig {
    /// Populate from a `RequestContext` for shipping over a transport.
    ///
    /// The `span_id` in the resulting config is the request's current span
    /// (i.e. `request.trace_parent().span_id`), which becomes the parent of
    /// any spans created from the reconstructed context on the receiver.
    #[must_use]
    pub fn from_request_context(request: &RequestContext) -> Self {
        let protocol = request.protocol();
        // The pinned id, not the `traceparent` one: a caller that used the
        // bare trace-id header has no `traceparent`, and its id still has to
        // reach the executor or the executor's rows and logs land under a
        // different id than the scheduler's.
        let trace_id = request
            .client_trace_id()
            .map(std::string::ToString::to_string);
        // Only when the `traceparent` span belongs to the trace being sent. A
        // caller that pins a trace id while a proxy injects a `traceparent`
        // names a span in a different trace; shipping the pair would have the
        // executor reconstruct a `TraceParent` that never existed.
        let span_id = request
            .trace_parent()
            .as_ref()
            .filter(|tp| Some(tp.trace_id.to_string()) == trace_id)
            .map(|tp| tp.span_id.to_string());
        Self {
            protocol: Some(protocol as u8),
            trace_id,
            span_id,
        }
    }

    /// Build a fresh `RequestContext` from the propagated fields.
    ///
    /// `protocol` defaults to [`Protocol::Internal`] when missing or
    /// invalid. `trace_id` / `span_id` populate the `TraceParent` only if
    /// both parse as valid W3C ids.
    #[must_use]
    pub fn to_request_context(&self) -> Arc<RequestContext> {
        let protocol = self
            .protocol
            .map(Protocol::from)
            .filter(|p| !matches!(p, Protocol::Invalid))
            .unwrap_or(Protocol::Internal);

        let mut builder = RequestContextBuilder::new(protocol);

        if let (Some(trace_id_str), Some(span_id_str)) =
            (self.trace_id.as_deref(), self.span_id.as_deref())
            && let (Ok(trace_id), Ok(span_id)) = (
                TraceId::from_hex(trace_id_str),
                SpanId::from_hex(span_id_str),
            )
        {
            builder = builder.with_trace_parent(Some(TraceParent { trace_id, span_id }));
        }

        // Independent of the `TraceParent` above, which needs a span id the
        // bare trace-id header never supplies. `build()` would otherwise derive
        // the id from a `TraceParent` that was not reconstructed.
        builder = builder.with_client_trace_id(
            self.trace_id
                .as_deref()
                .and_then(runtime_request_context::normalize_trace_id),
        );

        Arc::new(builder.build())
    }

    /// Returns `true` if any field carries a value — useful to decide
    /// whether the receiver should prefer this extension over a fallback.
    #[must_use]
    pub fn is_populated(&self) -> bool {
        self.protocol.is_some() || self.trace_id.is_some() || self.span_id.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_to_request_context_roundtrip_with_trace_parent() {
        let trace_id = TraceId::from_hex("0123456789abcdef0123456789abcdef").expect("trace id");
        let span_id = SpanId::from_hex("0123456789abcdef").expect("span id");
        let original = Arc::new(
            RequestContextBuilder::new(Protocol::FlightSQL)
                .with_trace_parent(Some(TraceParent { trace_id, span_id }))
                .build(),
        );

        let cfg = SpiceRequestContextConfig::from_request_context(&original);
        assert_eq!(cfg.protocol, Some(Protocol::FlightSQL as u8));
        assert_eq!(
            cfg.trace_id.as_deref(),
            Some("0123456789abcdef0123456789abcdef")
        );
        assert_eq!(cfg.span_id.as_deref(), Some("0123456789abcdef"));

        let rebuilt = cfg.to_request_context();
        assert!(matches!(rebuilt.protocol(), Protocol::FlightSQL));
        let tp = rebuilt
            .trace_parent()
            .as_ref()
            .expect("trace parent should be present");
        assert_eq!(tp.trace_id, trace_id);
        assert_eq!(tp.span_id, span_id);
    }

    /// A caller that pinned its id with the bare header has no `traceparent`,
    /// so the id has to cross the scheduler → executor boundary on its own.
    #[test]
    fn client_trace_id_propagates_without_a_traceparent() {
        let pinned = "4bf92f3577b34da6a3ce929d0e0e4736";
        let original = Arc::new(
            RequestContextBuilder::new(Protocol::FlightSQL)
                .with_client_trace_id(Some(Arc::from(pinned)))
                .build(),
        );

        let cfg = SpiceRequestContextConfig::from_request_context(&original);
        assert_eq!(cfg.trace_id.as_deref(), Some(pinned));
        assert_eq!(cfg.span_id, None);

        let rebuilt = cfg.to_request_context();
        assert_eq!(rebuilt.client_trace_id().map(AsRef::as_ref), Some(pinned));
        assert!(
            rebuilt.trace_parent().is_none(),
            "no span id was sent, so there is no parent span to claim"
        );
    }

    /// A pinned id plus a proxy-injected `traceparent` name two different
    /// traces. The span id belongs to the one that lost, so sending it would
    /// have the executor claim a parent that never existed in the caller's
    /// graph.
    #[test]
    fn a_parent_span_from_another_trace_is_not_propagated() {
        let pinned = "4bf92f3577b34da6a3ce929d0e0e4736";
        let original = Arc::new(
            RequestContextBuilder::new(Protocol::Http)
                .with_client_trace_id(Some(Arc::from(pinned)))
                .with_trace_parent(Some(TraceParent {
                    trace_id: TraceId::from_hex("0af7651916cd43dd8448eb211c80319c")
                        .expect("trace id"),
                    span_id: SpanId::from_hex("b7ad6b7169203331").expect("span id"),
                }))
                .build(),
        );

        let cfg = SpiceRequestContextConfig::from_request_context(&original);
        assert_eq!(cfg.trace_id.as_deref(), Some(pinned));
        assert_eq!(
            cfg.span_id, None,
            "a span id from a different trace must not travel with the pinned id"
        );
        assert!(cfg.to_request_context().trace_parent().is_none());
    }

    /// The agreeing case still propagates: a `traceparent` on its own supplies
    /// both halves of one trace.
    #[test]
    fn a_parent_span_from_the_same_trace_is_propagated() {
        let trace_id = TraceId::from_hex("0af7651916cd43dd8448eb211c80319c").expect("trace id");
        let span_id = SpanId::from_hex("b7ad6b7169203331").expect("span id");
        let original = Arc::new(
            RequestContextBuilder::new(Protocol::Http)
                .with_trace_parent(Some(TraceParent { trace_id, span_id }))
                .build(),
        );

        let cfg = SpiceRequestContextConfig::from_request_context(&original);
        assert_eq!(cfg.span_id.as_deref(), Some("b7ad6b7169203331"));
        assert_eq!(
            cfg.to_request_context()
                .trace_parent()
                .as_ref()
                .map(|tp| tp.span_id),
            Some(span_id)
        );
    }

    #[test]
    fn to_request_context_without_trace_fields_defaults_to_internal() {
        let cfg = SpiceRequestContextConfig::default();
        assert!(!cfg.is_populated());

        let ctx = cfg.to_request_context();
        assert!(matches!(ctx.protocol(), Protocol::Internal));
        assert!(ctx.trace_parent().is_none());
        assert!(ctx.client_trace_id().is_none());
    }

    #[test]
    fn to_request_context_with_invalid_trace_id_drops_trace_parent() {
        let cfg = SpiceRequestContextConfig {
            protocol: Some(Protocol::Http as u8),
            trace_id: Some("not-hex".to_string()),
            span_id: Some("0123456789abcdef".to_string()),
        };
        let ctx = cfg.to_request_context();
        assert!(matches!(ctx.protocol(), Protocol::Http));
        assert!(ctx.trace_parent().is_none());
    }

    #[test]
    fn entries_use_prefix() {
        use datafusion::config::ExtensionOptions;

        let cfg = SpiceRequestContextConfig::from_request_context(&Arc::new(
            RequestContextBuilder::new(Protocol::FlightSQL)
                .with_trace_parent(Some(TraceParent {
                    trace_id: TraceId::from_hex("0123456789abcdef0123456789abcdef")
                        .expect("trace id"),
                    span_id: SpanId::from_hex("0123456789abcdef").expect("span id"),
                }))
                .build(),
        ));

        let entries = cfg.entries();
        let keys: Vec<&str> = entries.iter().map(|e| e.key.as_str()).collect();
        assert!(keys.contains(&"protocol"));
        assert!(keys.contains(&"trace_id"));
        assert!(keys.contains(&"span_id"));
    }
}
