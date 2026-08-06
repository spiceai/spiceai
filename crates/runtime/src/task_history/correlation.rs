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

//! The trace id every task carries, whether or not `runtime.task_history` is
//! recording.
//!
//! A failed query and the log records explaining it are only findable together
//! if they share an id. The `task_history` table has always had one — the
//! `trace_id` column — but nothing put it on the log, and with
//! `runtime.task_history.enabled: false` no id was minted at all: the
//! OpenTelemetry layer that numbers spans is not installed, so a failure and
//! the connector warning that caused it had nothing in common but a timestamp.
//!
//! [`begin_task_trace`] is the whole contract: given a task's `task_history`
//! span, it writes any client-pinned id to the row and returns the span that
//! puts the same id on the log. The two must not be done apart — the row and
//! the log would name different ids — which is why there is one function and
//! not three.
//!
//! There is no compiler check that a task-history span gets one: a task that
//! skips [`begin_task_trace`] still records a row, but its log records are not
//! correlatable. Today only the SQL query path calls it.

use std::sync::Arc;

use opentelemetry::trace::{TraceContextExt, TraceId};
use opentelemetry_sdk::trace::{IdGenerator, RandomIdGenerator};
use runtime_request_context::RequestContext;
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// Name of the span created by [`begin_task_trace`]. It is what prefixes each
/// console record the task produces: `query{trace_id=…}:`.
pub const TRACE_SPAN_NAME: &str = "query";

/// Target of that span.
///
/// Named explicitly rather than left to the module path so the `runtime` prefix
/// — which is what keeps the span enabled under the default log filter — cannot
/// be lost to a move. It is also how a nested task recognises that a trace span
/// is already in scope.
const TRACE_SPAN_TARGET: &str = "runtime::query::trace";

/// Records any client-pinned trace id on `task_span`, then returns the span
/// that carries the task's id onto the log.
///
/// Enter the returned span around the whole task — planning, execution, and
/// result streaming — so a failure raised at any point in that window is
/// attributable.
///
/// A nested task (a cache fill, NSQL's generated SQL, a sub-query) opens its own
/// trace, so its records read `query{outer}:query{inner}:`. That is one prefix
/// per nesting level rather than one, but it is honest — the ids differ when
/// nothing is pinned and task history is off — and suppressing it would take a
/// walk of the entered span stack that `tracing` does not expose here.
#[must_use]
pub fn begin_task_trace(task_span: &Span, request_context: &RequestContext) -> Span {
    record_task_history_trace_id(task_span, request_context);

    let trace_id = resolve_trace_id(request_context, task_span);

    // Deliberately NOT `target: "task_history"`: that target is what the
    // console layer filters out, and this span exists to be seen there. It is
    // equally excluded from the task-history exporter, which wants only spans
    // on that target — this span records no task. The default event format
    // renders an entered span as `name{fields}:` ahead of the target, so every
    // record emitted while the task runs carries the id.
    tracing::info_span!(target: TRACE_SPAN_TARGET, TRACE_SPAN_NAME, trace_id = %trace_id)
}

/// Records the trace id the client pinned — via `x-spice-trace-id` or W3C
/// `traceparent` — on `span`, overriding the `trace_id` column of the
/// `task_history` row, plus the `parent_span_id` column when the client sent a
/// `traceparent` naming the span this task is a child of.
///
/// Only a *pinned* id is recorded. An id the runtime resolves for itself
/// already is the span's trace id, and re-declaring it would make the exporter
/// rewrite every row of the trace to the value it already holds.
///
/// This should not be used for any span within a HTTP API that has [HTTP Spans](https://opentelemetry.io/docs/specs/semconv/http/http-spans/) created, as they are incompatible (both the `span` input and the span created for the HTTP handler will have the same `parent_span_id`, even though the `input` span would become a child of the HTTP span)
pub fn record_task_history_trace_id(span: &Span, request_context: &RequestContext) {
    let Some(trace_id) = request_context.client_trace_id() else {
        return;
    };

    match same_trace_parent_span(request_context, trace_id) {
        Some(parent_span_id) => {
            tracing::info!(target: "task_history", parent: span, trace_id = %trace_id, parent_id = %parent_span_id);
        }
        None => {
            tracing::info!(target: "task_history", parent: span, trace_id = %trace_id);
        }
    }
}

/// The `traceparent` span this task is a child of, but only when that span
/// belongs to `trace_id`.
///
/// A caller that pins `x-spice-trace-id` while a proxy injects a `traceparent`
/// sends two different traces. A span id is only meaningful inside its own
/// trace, so recording it under the pinned id would put an edge in the
/// task-history tree — and in the context shipped to executors — that exists in
/// no caller's graph. Anything joining on `(trace_id, parent_span_id)` would
/// follow it.
pub fn same_trace_parent_span(
    request_context: &RequestContext,
    trace_id: &str,
) -> Option<opentelemetry::trace::SpanId> {
    request_context
        .trace_parent()
        .as_ref()
        .filter(|trace_parent| trace_parent.trace_id.to_string() == trace_id)
        .map(|trace_parent| trace_parent.span_id)
}

/// The trace id for a task, given the `task_history` span recording it, from
/// the first of these that applies:
///
/// 1. the id the client pinned;
/// 2. the span's own OpenTelemetry trace id, when task history is recording.
///    Reusing it is what makes a log record and the row it belongs to name the
///    same id, at no cost — the alternative, minting an id here and rewriting
///    every row to match, is a full scan of the task-history table per query;
/// 3. a freshly generated id, when task history is disabled and there is no
///    span context to borrow.
fn resolve_trace_id(request_context: &RequestContext, task_span: &Span) -> Arc<str> {
    if let Some(pinned) = request_context.client_trace_id() {
        return Arc::clone(pinned);
    }

    // `TraceId::INVALID` is what an unnumbered span reports, which is every
    // span when the task-history layer is not installed.
    let span_trace_id = task_span.context().span().span_context().trace_id();
    if span_trace_id == TraceId::INVALID {
        // The exporter's own generator, so an id minted here is the same shape
        // and quality as one it would have written.
        return Arc::from(RandomIdGenerator::default().new_trace_id().to_string());
    }

    Arc::from(span_trace_id.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use runtime_request_context::{Protocol, RequestContextBuilder};

    const PINNED: &str = "4bf92f3577b34da6a3ce929d0e0e4736";

    fn unpinned() -> RequestContext {
        RequestContextBuilder::new(Protocol::Http).build()
    }

    fn pinned() -> RequestContext {
        RequestContextBuilder::new(Protocol::Http)
            .with_client_trace_id(Some(Arc::from(PINNED)))
            .build()
    }

    /// The console renders this span as `name{fields}:`, so the name, the
    /// target that keeps it enabled, and the id being its only field are all
    /// part of what an operator greps for.
    #[test]
    fn trace_span_renders_the_id_and_nothing_else() {
        let _guard = tracing::subscriber::set_default(tracing_subscriber::registry());

        let span = begin_task_trace(&Span::none(), &pinned());
        let metadata = span.metadata().expect("the span has metadata");

        assert_eq!(metadata.name(), TRACE_SPAN_NAME);
        assert_eq!(metadata.target(), TRACE_SPAN_TARGET);
        assert_eq!(
            metadata
                .fields()
                .iter()
                .map(|f| f.name())
                .collect::<Vec<_>>(),
            vec!["trace_id"],
            "the console renders every field of this span, so the id must be the only one"
        );
        assert!(
            TRACE_SPAN_TARGET.starts_with("runtime"),
            "the default log filter enables `runtime` targets; another prefix \
             would leave the span disabled and the id unlogged"
        );
    }

    /// A pinned id plus a proxy-injected `traceparent` name two different
    /// traces, so the `traceparent` span is not this task's parent under the
    /// pinned id. Recording it would put an edge in the task-history tree that
    /// exists in no caller's graph.
    #[test]
    fn a_parent_span_from_another_trace_is_not_recorded() {
        use opentelemetry::trace::{SpanId, TraceId as OtelTraceId};
        use runtime_request_context::TraceParent;

        let other_trace = OtelTraceId::from_hex("0af7651916cd43dd8448eb211c80319c")
            .expect("hardcoded trace id is valid hex");
        let span_id = SpanId::from_hex("b7ad6b7169203331").expect("hardcoded span id is valid hex");

        let conflicting = RequestContextBuilder::new(Protocol::Http)
            .with_client_trace_id(Some(Arc::from(PINNED)))
            .with_trace_parent(Some(TraceParent {
                trace_id: other_trace,
                span_id,
            }))
            .build();
        assert_eq!(same_trace_parent_span(&conflicting, PINNED), None);

        // The agreeing case is what the parent column is for, and still works.
        let agreeing = RequestContextBuilder::new(Protocol::Http)
            .with_trace_parent(Some(TraceParent {
                trace_id: other_trace,
                span_id,
            }))
            .build();
        assert_eq!(
            same_trace_parent_span(&agreeing, &other_trace.to_string()),
            Some(span_id)
        );
    }

    /// No client id and no task-history layer installed — the span carries no
    /// trace id to borrow, so one is minted rather than left empty.
    ///
    /// Two calls give two ids, which is right for two tasks and a trap for one:
    /// a caller that resolves again later, rather than holding the span it
    /// already has, logs the second half of a task under an id the first half
    /// never mentioned — see `QueryHandle::trace_span`.
    #[test]
    fn resolve_generates_a_distinct_id_per_call() {
        let ctx = unpinned();

        let first = resolve_trace_id(&ctx, &Span::none());
        assert_eq!(first.len(), 32);
        assert!(
            first
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
        );
        assert_ne!(first, resolve_trace_id(&ctx, &Span::none()));
    }

    /// The other half of that trap: a *pinned* id is stable across calls, so a
    /// re-resolve looks harmless in exactly the configuration most people run
    /// and forks the id only when task history is off and nothing is pinned.
    #[test]
    fn resolve_is_stable_across_calls_when_the_client_pinned_an_id() {
        let ctx = pinned();

        assert_eq!(&*resolve_trace_id(&ctx, &Span::none()), PINNED);
        assert_eq!(&*resolve_trace_id(&ctx, &Span::none()), PINNED);
    }
}
