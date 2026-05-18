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

//! Shared resolution for an `Arc<RequestContext>` from a `DataFusion`
//! [`TaskContext`].
//!
//! Establishes a canonical lookup order for any Spice exec node that needs
//! the originating request's context on an executor:
//!
//! 1. A typed extension on the [`SessionConfig`] (set on the
//!    scheduler-side in-process via
//!    `SessionConfig::with_extension::<RequestContext>`).
//! 2. The [`SpiceRequestContextConfig`] `ConfigExtension`, populated when
//!    the per-job `SessionConfig` round-trips over a transport (Ballista
//!    `TaskDefinition` props).
//! 3. A fresh `Protocol::Internal` context — only when the caller opts in
//!    via `fallback_to_internal = true`. Otherwise returns `None`.
//!
//! Source 1 is preferred so single-process queries continue to see the
//! exact same `Arc<RequestContext>` (including extensions and
//! cancellation token) that was installed by the query builder. Only when
//! crossing a transport boundary do we reconstruct a fresh
//! `RequestContext` from the config extension.

use crate::config::request_context_config::SpiceRequestContextConfig;
use datafusion::execution::TaskContext;
use runtime_request_context::{Protocol, RequestContext, RequestContextBuilder};
use std::sync::Arc;

/// Resolve an `Arc<RequestContext>` for executor-side telemetry.
///
/// Returns `None` if no context is available and `fallback_to_internal`
/// is `false` — callers can treat that as a missing-context bug to panic
/// on, matching the prior `BytesProcessedExec` behavior.
#[must_use]
pub fn resolve_request_context(
    context: &TaskContext,
    fallback_to_internal: bool,
) -> Option<Arc<RequestContext>> {
    if let Some(request_context) = context.session_config().get_extension::<RequestContext>() {
        return Some(request_context);
    }

    let config_ext = context
        .session_config()
        .options()
        .extensions
        .get::<SpiceRequestContextConfig>();
    if let Some(ext) = config_ext
        && ext.is_populated()
    {
        return Some(ext.to_request_context());
    }

    if fallback_to_internal {
        return Some(Arc::new(
            RequestContextBuilder::new(Protocol::Internal).build(),
        ));
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionConfig;
    use opentelemetry::trace::{SpanId, TraceId};
    use runtime_request_context::TraceParent;

    fn task_context(config: SessionConfig) -> TaskContext {
        TaskContext::default().with_session_config(config)
    }

    #[test]
    fn prefers_typed_extension_over_config_extension() {
        let typed = Arc::new(
            RequestContextBuilder::new(Protocol::FlightSQL)
                .with_trace_parent(Some(TraceParent {
                    trace_id: TraceId::from_hex("00000000000000000000000000000001")
                        .expect("hardcoded trace_id is valid hex"),
                    span_id: SpanId::from_hex("0000000000000001")
                        .expect("hardcoded span_id is valid hex"),
                }))
                .build(),
        );

        let config_ext = SpiceRequestContextConfig {
            protocol: Some(Protocol::Http as u8),
            trace_id: Some("00000000000000000000000000000002".to_string()),
            span_id: Some("0000000000000002".to_string()),
        };

        let config = SessionConfig::new()
            .with_extension(Arc::clone(&typed))
            .with_option_extension(config_ext);

        let resolved = resolve_request_context(&task_context(config), false)
            .expect("typed extension should resolve");
        assert!(matches!(resolved.protocol(), Protocol::FlightSQL));
        // Same Arc — typed extension wins.
        assert!(Arc::ptr_eq(&resolved, &typed));
    }

    #[test]
    fn falls_back_to_config_extension_when_typed_missing() {
        let config_ext = SpiceRequestContextConfig {
            protocol: Some(Protocol::Http as u8),
            trace_id: Some("0123456789abcdef0123456789abcdef".to_string()),
            span_id: Some("0123456789abcdef".to_string()),
        };
        let config = SessionConfig::new().with_option_extension(config_ext);

        let resolved = resolve_request_context(&task_context(config), false)
            .expect("config extension should resolve");
        assert!(matches!(resolved.protocol(), Protocol::Http));
        let tp = resolved
            .trace_parent()
            .as_ref()
            .expect("trace_parent should be populated from config");
        assert_eq!(
            tp.trace_id,
            TraceId::from_hex("0123456789abcdef0123456789abcdef")
                .expect("hardcoded trace_id is valid hex")
        );
        assert_eq!(
            tp.span_id,
            SpanId::from_hex("0123456789abcdef").expect("hardcoded span_id is valid hex")
        );
    }

    #[test]
    fn returns_none_when_unpopulated_and_no_fallback() {
        let config =
            SessionConfig::new().with_option_extension(SpiceRequestContextConfig::default());
        assert!(resolve_request_context(&task_context(config), false).is_none());
    }

    #[test]
    fn returns_internal_when_fallback_requested() {
        let config = SessionConfig::new();
        let resolved = resolve_request_context(&task_context(config), true)
            .expect("fallback should produce internal context");
        assert!(matches!(resolved.protocol(), Protocol::Internal));
    }
}
