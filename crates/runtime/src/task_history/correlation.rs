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

use opentelemetry::{
    Context,
    trace::{SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState},
};
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

/// Puts `task_span` on the task's trace id — recording a client-pinned one, or
/// joining a trace already returned to the client — then returns the span that
/// carries the same id onto the log.
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
    join_propagated_trace(task_span, request_context);

    let trace_id = resolve_trace_id(request_context, task_span);

    // Record it, so the response can name the id the work was recorded under.
    // A protocol that hands the id out before the query runs (Flight SQL's
    // `GetFlightInfo`) has already put it here and this is a no-op; one that
    // cannot — HTTP, MCP — has no id to return until a task resolves one, and
    // this is where that happens.
    request_context.propagate_trace_id(Arc::clone(&trace_id));

    // Deliberately NOT `target: "task_history"`: that target is what the
    // console layer filters out, and this span exists to be seen there. It is
    // equally excluded from the task-history exporter, which wants only spans
    // on that target — this span records no task. The default event format
    // renders an entered span as `name{fields}:` ahead of the target, so every
    // record emitted while the task runs carries the id.
    tracing::info_span!(target: TRACE_SPAN_TARGET, TRACE_SPAN_NAME, trace_id = %trace_id)
}

/// Records the trace id the client pinned — via `spice-trace-id` or W3C
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

/// The trace id to return to the client for work this request has not started
/// yet, recorded so everything the request goes on to do adopts it.
///
/// For the first RPC of an exchange that answers the client before the query
/// runs — Flight SQL's `GetFlightInfo`. A pinned id is returned unchanged; a
/// caller that pinned nothing gets a freshly minted one, and that id then
/// reaches the RPC which does run the query (see `flight::traced_ticket`), so
/// the two name one trace.
pub fn publish_trace_id(request_context: &RequestContext) -> Arc<str> {
    if let Some(pinned) = request_context.client_trace_id() {
        return Arc::clone(pinned);
    }
    Arc::clone(request_context.propagate_trace_id(mint_trace_id()))
}

/// A fresh trace id, from the exporter's own generator so one minted here is
/// the same shape and quality as one it would have written.
fn mint_trace_id() -> Arc<str> {
    Arc::from(RandomIdGenerator::default().new_trace_id().to_string())
}

/// The span [`join_propagated_trace`] anchors a joined trace on.
///
/// A span context is only valid with *both* ids, so joining a trace costs a
/// span id even though no span answers to it. A constant rather than a fresh
/// id per query: it names nothing either way, and a value the exporter already
/// knows needs no attribute to carry it across.
///
/// The exporter drops it from the `parent_span_id` column — left in place it
/// would point at a row that is never written, and `spice trace` roots its
/// tree on a null parent, so the query would not appear at all.
pub const TRACE_JOIN_ANCHOR: SpanId = SpanId::from_bytes(*b"spictrac");

/// Puts `task_span` in the trace whose id an earlier RPC of this exchange
/// already returned to the client, so the row it writes carries that id.
///
/// Joining the trace — rather than declaring the id as an override the way a
/// client-pinned one is — is what keeps this free. An override is reconciled
/// after the fact by `TaskSpan::write`, which scans `runtime.task_history` for
/// every row already written under the id being replaced and rewrites them;
/// that is a scan per query on a path every Flight SQL query takes. A joined
/// trace is simply the span's own id, correct at write time, and child spans
/// (`ballista_stage` rows) inherit it for free.
///
/// A client-pinned id wins over a propagated one and keeps the override path:
/// the id is then the caller's, and its `traceparent` may name a parent span
/// that the anchor would displace.
///
/// No-op when the task-history layer is not installed — there is no `OTel`
/// span to place, and [`resolve_trace_id`] falls back to the propagated id
/// directly for the log.
fn join_propagated_trace(task_span: &Span, request_context: &RequestContext) {
    if request_context.client_trace_id().is_some() {
        return;
    }
    // Only the request's first task anchors itself. A nested task — a cache
    // fill, NSQL's generated SQL, a sub-query — is already in this trace
    // through the task above it, and anchoring it too would replace its real
    // parent with one that names nothing, flattening the `task_history` tree.
    let Some(propagated) = request_context.claim_propagated_trace() else {
        return;
    };
    let Ok(trace_id) = TraceId::from_hex(propagated) else {
        // Unreachable for an id this runtime minted; a corrupted one costs
        // correlation, not the query.
        tracing::warn!("Ignoring malformed propagated trace id '{propagated}'");
        return;
    };

    let parent = Context::new().with_remote_span_context(SpanContext::new(
        trace_id,
        TRACE_JOIN_ANCHOR,
        TraceFlags::SAMPLED,
        true,
        TraceState::default(),
    ));

    if let Err(e) = task_span.set_parent(parent) {
        // Expected whenever there is no row to place: with
        // `runtime.task_history.enabled: false` the layer is absent, and the
        // span may be filtered out. Not a warning — it would be one per query
        // in a supported configuration, and the log still gets the id from
        // `resolve_trace_id`.
        tracing::debug!("Not recording task under trace {propagated}: {e}");
    }
}

/// The `traceparent` span this task is a child of, but only when that span
/// belongs to `trace_id`.
///
/// A caller that pins `spice-trace-id` while a proxy injects a `traceparent`
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
/// 2. the id an earlier RPC of this exchange already returned to the client —
///    [`join_propagated_trace`] has put the span in that trace, so this is the
///    span's own id too, but it is read from the request because the join is a
///    no-op when task history is off;
/// 3. the span's own OpenTelemetry trace id, when task history is recording.
///    Reusing it is what makes a log record and the row it belongs to name the
///    same id, at no cost — the alternative, minting an id here and rewriting
///    every row to match, is a full scan of the task-history table per query;
/// 4. a freshly generated id, when task history is disabled and there is no
///    span context to borrow.
fn resolve_trace_id(request_context: &RequestContext, task_span: &Span) -> Arc<str> {
    if let Some(settled) = request_context.settled_trace_id() {
        return Arc::clone(settled);
    }

    // `TraceId::INVALID` is what an unnumbered span reports, which is every
    // span when the task-history layer is not installed.
    let span_trace_id = task_span.context().span().span_context().trace_id();
    if span_trace_id == TraceId::INVALID {
        return mint_trace_id();
    }

    Arc::from(span_trace_id.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use runtime_request_context::{Protocol, RequestContextBuilder, TRACE_ID_HEX_LEN};

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

    const PROPAGATED: &str = "0af7651916cd43dd8448eb211c80319c";

    /// The whole point of propagating an id through a Flight ticket: the task
    /// that runs the query adopts the id an earlier RPC already returned,
    /// rather than generating one the client never saw.
    #[test]
    fn resolve_adopts_a_propagated_id_instead_of_generating_one() {
        let ctx = unpinned();
        ctx.propagate_trace_id(Arc::from(PROPAGATED));

        assert_eq!(&*resolve_trace_id(&ctx, &Span::none()), PROPAGATED);
        assert_eq!(
            &*resolve_trace_id(&ctx, &Span::none()),
            PROPAGATED,
            "an adopted id is stable, unlike a generated one"
        );
    }

    /// A client that pinned an id gets that id, even on a request whose ticket
    /// carries one: the pinned id is the caller's and is what it correlates on.
    #[test]
    fn a_pinned_id_beats_a_propagated_one() {
        let ctx = pinned();
        ctx.propagate_trace_id(Arc::from(PROPAGATED));

        assert_eq!(&*resolve_trace_id(&ctx, &Span::none()), PINNED);
    }

    /// Only the request's top task anchors itself on the trace. A nested task
    /// is already in it through the task above, and re-anchoring would trade
    /// that task's real parent for one that names nothing.
    #[test]
    fn only_the_first_task_of_a_request_joins_the_trace() {
        let ctx = unpinned();
        ctx.propagate_trace_id(Arc::from(PROPAGATED));

        assert_eq!(
            ctx.claim_propagated_trace().map(AsRef::as_ref),
            Some(PROPAGATED),
            "the top task joins"
        );
        assert!(
            ctx.claim_propagated_trace().is_none(),
            "a nested task must not re-anchor"
        );

        assert_eq!(
            &*resolve_trace_id(&ctx, &Span::none()),
            PROPAGATED,
            "a nested task still logs under the same id"
        );
    }

    /// A second id cannot move a request already numbered — the task that read
    /// the first has already logged under it.
    #[test]
    fn the_first_propagated_id_wins() {
        let ctx = unpinned();
        ctx.propagate_trace_id(Arc::from(PROPAGATED));
        ctx.propagate_trace_id(Arc::from(PINNED));

        assert_eq!(&*resolve_trace_id(&ctx, &Span::none()), PROPAGATED);
    }

    /// `GetFlightInfo` returns the id before the query runs, and `DoGet` has to
    /// end up on the same one, so the minting has to happen exactly once.
    #[test]
    fn publish_mints_one_id_and_records_it_for_the_request() {
        let ctx = unpinned();

        let published = publish_trace_id(&ctx);
        assert_eq!(published.len(), TRACE_ID_HEX_LEN);
        assert_eq!(
            publish_trace_id(&ctx),
            published,
            "a second call must return the id already handed to the client"
        );
        assert_eq!(
            ctx.propagated_trace_id().map(AsRef::as_ref),
            Some(&*published),
            "the id has to be on the request, or the task would resolve another"
        );
        assert_eq!(&*resolve_trace_id(&ctx, &Span::none()), &*published);
    }

    /// A pinned id is returned as-is and deliberately *not* recorded as
    /// propagated: it keeps the override path, which is what also writes the
    /// caller's `traceparent` span to the row as the task's parent.
    #[test]
    fn publish_returns_a_pinned_id_without_adopting_it() {
        let ctx = pinned();

        assert_eq!(&*publish_trace_id(&ctx), PINNED);
        assert!(ctx.propagated_trace_id().is_none());
    }

    /// Two requests get two ids — the id identifies a query, not a connection,
    /// which is the whole reason a pooled client cannot pin one up front.
    #[test]
    fn publish_mints_a_distinct_id_per_request() {
        assert_ne!(publish_trace_id(&unpinned()), publish_trace_id(&unpinned()));
    }
}
