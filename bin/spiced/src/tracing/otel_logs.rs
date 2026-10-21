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

//! Sends `spiced`'s log records to an OpenTelemetry endpoint over OTLP, when
//! `runtime.telemetry.otel_exporter.signals` includes `logs`.
//!
//! [`OtelLogsLayer`] turns each `tracing` event the console prints into an OTLP
//! log record. Records are batched and exported on a runtime of their own,
//! [`start`]ed beside the query runtime rather than on it: a busy query runtime
//! cannot hold back export, and export I/O never takes a query worker. That
//! runtime also outlives the query runtime. `main` logs the fatal error that
//! ends the process, and `Goodbye!`, after the query runtime is gone, then calls
//! [`shutdown`] last, so both still reach the endpoint.
//!
//! The layer is installed before it can send anything: a cluster executor learns
//! its settings from the scheduler after the subscriber exists. Until a
//! [`LogsSink`] is armed, the layer does nothing.

use std::borrow::Cow;
use std::cell::Cell;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant, SystemTime};

use opentelemetry::Key;
use opentelemetry::logs::{AnyValue, LogRecord as _, Logger as _, LoggerProvider as _, Severity};
use opentelemetry::trace::{SpanId, TraceContextExt as _, TraceId};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::logs::log_processor_with_async_runtime::BatchLogProcessor;
use opentelemetry_sdk::logs::{SdkLogRecord, SdkLogger, SdkLoggerProvider};
use runtime::otel_push_exporter::{self, OtelExporterConfig};
use runtime::task_history::correlation;
use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber, span};
use tracing_log::NormalizeEvent as _;
use tracing_subscriber::filter;
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::registry::LookupSpan;

/// How long [`shutdown`] waits for the queued records to reach the endpoint.
///
/// Bounded so that an endpoint that has stopped answering cannot hold the
/// process open past its own shutdown; whatever is still queued is on stdout.
const FLUSH_TIMEOUT: Duration = Duration::from_secs(5);

thread_local! {
    /// Whether this thread is one of the export runtime's.
    ///
    /// Everything logged there is the export's own doing: its gRPC or HTTP
    /// client, the connections and name lookups that client spawns, the TLS
    /// handshake, and the exporter's failure reports. None of it is exported —
    /// a sent batch would log events that fill the next one, and a failing
    /// endpoint would be sent the report of its own failure. It still prints to
    /// stdout.
    static EXPORT_THREAD: Cell<bool> = const { Cell::new(false) };
}

/// Where the layer sends records once [`start`] arms it.
#[derive(Clone, Default)]
pub struct LogsSink(Arc<OnceLock<SdkLogger>>);

impl LogsSink {
    /// Sends every record the layer sees from now on to `logger`.
    ///
    /// Returns `false`, and keeps the logger it has, when already armed.
    #[must_use]
    pub fn arm(&self, logger: SdkLogger) -> bool {
        self.0.set(logger).is_ok()
    }

    fn logger(&self) -> Option<&SdkLogger> {
        self.0.get()
    }
}

/// A `tracing` layer that exports each event as an OTLP log record; see
/// [`layer`].
struct OtelLogsLayer {
    sink: LogsSink,
}

/// The layer that exports through `sink` once it is armed, filtered the way
/// the console is: `task_history` spans and events, the task-history table's
/// bookkeeping, are neither printed nor exported.
///
/// The filter is also what finds a query's trace id. A query runs inside two
/// sibling spans — its `task_history` span, entered last, and the trace span
/// [`correlation::begin_task_trace`] returns — so the current span's ancestry
/// does not include the trace span. A per-layer filter that disables
/// `task_history` spans makes `tracing` resolve this layer's current span from
/// the span stack instead, past the task span to the trace span, exactly as it
/// does for the console's `query{trace_id=…}:` prefix.
#[must_use]
pub fn layer<S>(sink: LogsSink) -> impl Layer<S>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    OtelLogsLayer { sink }.with_filter(filter::filter_fn(|metadata| {
        metadata.target() != "task_history"
    }))
}

/// The trace id of a query, kept on the query's trace span (see
/// [`correlation::begin_task_trace`]) so every record logged under it can carry
/// the id the client was given.
struct QueryTraceId(TraceId);

impl<S> Layer<S> for OtelLogsLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &span::Attributes<'_>, id: &span::Id, ctx: Context<'_, S>) {
        if self.sink.logger().is_none() || !correlation::is_task_trace_span(attrs.metadata()) {
            return;
        }
        let mut visitor = TraceIdField(None);
        attrs.record(&mut visitor);
        if let (Some(trace_id), Some(span)) = (visitor.0, ctx.span(id)) {
            span.extensions_mut().insert(QueryTraceId(trace_id));
        }
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        let Some(logger) = self.sink.logger() else {
            return;
        };
        if EXPORT_THREAD.get() {
            return;
        }

        // A record bridged from the `log` crate arrives on a fixed `tracing`
        // callsite; its own target and level are in its fields.
        let normalized = event.normalized_metadata();
        let metadata = normalized.as_ref().unwrap_or_else(|| event.metadata());

        let mut record = logger.create_log_record();
        let now = SystemTime::now();
        record.set_timestamp(now);
        record.set_observed_timestamp(now);
        record.set_severity_number(severity(*metadata.level()));
        record.set_severity_text(metadata.level().as_str());
        record.set_target(if normalized.is_some() {
            Cow::Owned(metadata.target().to_owned())
        } else {
            Cow::Borrowed(event.metadata().target())
        });
        event.record(&mut RecordFields(&mut record));
        if let Some((trace_id, span_id)) = trace_context(event, &ctx) {
            record.set_trace_context(trace_id, span_id, None);
        }
        logger.emit(record);
    }
}

fn severity(level: Level) -> Severity {
    match level {
        Level::TRACE => Severity::Trace,
        Level::DEBUG => Severity::Debug,
        Level::INFO => Severity::Info,
        Level::WARN => Severity::Warn,
        Level::ERROR => Severity::Error,
    }
}

/// The trace id and span id a record logged by `event` belongs to.
///
/// Inside a query, the trace id is the query's — the id `spice-trace-id`
/// returned and the `runtime.task_history` row stores, including one the client
/// pinned — and the span id is that of the innermost task-history span, when
/// task history is recording. Inside any other recorded task, the span id is
/// that task's own and the trace id is its trace's, or the id a client pinned
/// for that trace (see [`correlation::task_history_trace_id`]). Otherwise there
/// is none.
///
/// A record logged while the query runtime polls a query's plan is inside the
/// query's task span but not its trace span, so its trace id comes from the
/// task: the id the client pinned, when it pinned one.
fn trace_context<S>(event: &Event<'_>, ctx: &Context<'_, S>) -> Option<(TraceId, SpanId)>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    let query = ctx.event_scope(event).and_then(|mut scope| {
        scope.find_map(|span| span.extensions().get::<QueryTraceId>().map(|id| id.0))
    });
    let current = opentelemetry::Context::current();
    let task = current.span().span_context().clone();
    match query {
        Some(trace_id) if task.is_valid() => Some((trace_id, task.span_id())),
        Some(trace_id) => Some((trace_id, SpanId::INVALID)),
        None if task.is_valid() => Some((
            correlation::task_history_trace_id(task.trace_id()),
            task.span_id(),
        )),
        None => None,
    }
}

/// Reads the trace id off a query's trace span.
struct TraceIdField(Option<TraceId>);

impl Visit for TraceIdField {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == correlation::TRACE_ID_FIELD {
            self.0 = TraceId::from_hex(value).ok();
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == correlation::TRACE_ID_FIELD {
            self.0 = TraceId::from_hex(&format!("{value:?}")).ok();
        }
    }
}

/// Writes an event's fields onto a record: `message` as the body, the rest as
/// attributes that keep their type.
struct RecordFields<'a>(&'a mut SdkLogRecord);

impl RecordFields<'_> {
    fn set(&mut self, field: &Field, value: AnyValue) {
        if field.name() == "message" {
            self.0.set_body(value);
        } else if !field.name().starts_with("log.") {
            // `log.*` fields carry a bridged `log` record's metadata, which the
            // record already has as its target and severity.
            self.0
                .add_attribute(Key::from_static_str(field.name()), value);
        }
    }
}

impl Visit for RecordFields<'_> {
    fn record_f64(&mut self, field: &Field, value: f64) {
        self.set(field, AnyValue::Double(value));
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.set(field, AnyValue::Int(value));
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.set(
            field,
            i64::try_from(value).map_or_else(|_| AnyValue::from(value.to_string()), AnyValue::Int),
        );
    }

    fn record_i128(&mut self, field: &Field, value: i128) {
        self.set(
            field,
            i64::try_from(value).map_or_else(|_| AnyValue::from(value.to_string()), AnyValue::Int),
        );
    }

    fn record_u128(&mut self, field: &Field, value: u128) {
        self.set(
            field,
            i64::try_from(value).map_or_else(|_| AnyValue::from(value.to_string()), AnyValue::Int),
        );
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.set(field, AnyValue::Boolean(value));
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.set(field, AnyValue::from(value.to_owned()));
    }

    fn record_error(&mut self, field: &Field, value: &(dyn std::error::Error + 'static)) {
        self.set(field, AnyValue::from(value.to_string()));
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.set(field, AnyValue::from(format!("{value:?}")));
    }
}

/// Why log export could not start.
#[derive(Debug)]
pub enum StartError {
    /// The runtime that runs the export could not be created.
    Runtime(std::io::Error),
    /// The exporter could not be created from the configuration.
    Exporter(otel_push_exporter::Error),
}

impl std::fmt::Display for StartError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Runtime(e) => write!(f, "Failed to start the log export runtime: {e}"),
            Self::Exporter(e) => e.fmt(f),
        }
    }
}

impl std::error::Error for StartError {}

/// The export runtime and the provider batching into it, kept until
/// [`shutdown`] flushes them.
struct Pipeline {
    provider: SdkLoggerProvider,
    runtime: tokio::runtime::Runtime,
}

static PIPELINE: parking_lot::Mutex<Option<Pipeline>> = parking_lot::Mutex::new(None);

/// Starts exporting to `config.endpoint` and arms `sink` with the result.
///
/// The export runs on a runtime this call creates, which [`shutdown`] stops.
/// `headers` must already have secret templates resolved. `resource` describes
/// this process to the endpoint; pass the one exported metrics carry, so a
/// backend can join the two.
///
/// # Errors
///
/// Returns an error, and leaves `sink` unarmed, if the runtime or the exporter
/// cannot be created.
#[expect(
    clippy::implicit_hasher,
    reason = "callers pass the std HashMap that secret resolution returns"
)]
pub fn start(
    sink: &LogsSink,
    config: &OtelExporterConfig,
    headers: HashMap<String, String>,
    resource: Resource,
) -> Result<(), StartError> {
    let mut pipeline = PIPELINE.lock();
    // One export per process. Replacing a running one would drop its runtime
    // here, which panics when `start` runs on another runtime's worker.
    if pipeline.is_some() {
        return Ok(());
    }

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .thread_name("otel-logs-export")
        .on_thread_start(|| EXPORT_THREAD.set(true))
        .enable_all()
        .build()
        .map_err(StartError::Runtime)?;

    // Everything the exporter spawns — the batching task, the gRPC channel's
    // connection worker — must land on the export runtime, not the caller's.
    let export_context = runtime.enter();
    let provider = otel_push_exporter::create_otel_log_exporter(config, headers).map(|exporter| {
        let processor =
            BatchLogProcessor::builder(exporter, opentelemetry_sdk::runtime::Tokio).build();
        SdkLoggerProvider::builder()
            .with_resource(resource)
            .with_log_processor(processor)
            .build()
    });
    drop(export_context);
    let provider = match provider {
        Ok(provider) => provider,
        Err(e) => {
            // `start` may run on another runtime's worker, where dropping a
            // runtime would panic.
            runtime.shutdown_background();
            return Err(StartError::Exporter(e));
        }
    };

    // `PIPELINE` admits one export per process, so a sink armed here was not
    // armed before; one that was keeps the logger it has.
    let _ = sink.arm(provider.logger("spiced"));
    *pipeline = Some(Pipeline { provider, runtime });
    Ok(())
}

/// Flushes the records still queued and stops log export.
///
/// Call it once, last, from outside any Tokio runtime: it blocks for up to
/// [`FLUSH_TIMEOUT`] while the queue drains. Records logged after it are
/// printed to stdout but not exported. Does nothing when export never started.
pub fn shutdown() {
    let Some(Pipeline { provider, runtime }) = PIPELINE.lock().take() else {
        return;
    };

    // The SDK hands its flush and shutdown requests to the export task through
    // the bounded record queue, and fails at once when that queue is full — as
    // it is while an export to a slow endpoint holds the task. So retry the
    // flush until the deadline, which lets the task drain the queue, and only
    // then shut down. Done beside this thread because the SDK's wait for an
    // export has no bound of its own.
    let deadline = Instant::now() + FLUSH_TIMEOUT;
    let (finished, wait) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let flushed = loop {
            match provider.force_flush() {
                Ok(()) => break Ok(()),
                Err(_) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(50));
                }
                Err(e) => break Err(e),
            }
        };
        let _ = finished.send(flushed.and_then(|()| provider.shutdown()));
    });

    let outcome = wait.recv_timeout(deadline.saturating_duration_since(Instant::now()));
    if let Some(cause) = match outcome {
        Ok(Ok(())) => None,
        Ok(Err(e)) => Some(e.to_string()),
        Err(_) => Some(format!(
            "it did not finish within {}s",
            FLUSH_TIMEOUT.as_secs()
        )),
    } {
        tracing::warn!(
            target: otel_push_exporter::DIAGNOSTICS_TARGET,
            "Failed to send the last log records to the OpenTelemetry endpoint before exiting, so they are on stdout only. Cause: {cause}"
        );
    }
    runtime.shutdown_timeout(Duration::from_secs(1));
}
