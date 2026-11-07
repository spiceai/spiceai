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

//! Log export to an OpenTelemetry endpoint: `runtime.telemetry.otel_exporter`
//! with `signals: [logs]`.
//!
//! Two halves. In process, the layer's events go through the real SDK pipeline
//! to a collecting exporter, where each record's mapping can be asserted
//! exactly, trace ids included. End to end, a built `spiced` sends to an OTLP
//! receiver this test serves — gRPC for one run, HTTP for another, and a
//! scheduler and its executor over cluster mTLS — the way it would send to an
//! OpenTelemetry Collector.

use std::collections::BTreeMap;
use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use opentelemetry::logs::{AnyValue, LoggerProvider as _, Severity};
use opentelemetry::trace::{SpanId, TraceContextExt as _, TraceId, TracerProvider as _};
use opentelemetry_proto::tonic::collector::logs::v1::logs_service_server::{
    LogsService, LogsServiceServer,
};
use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsServiceRequest, ExportLogsServiceResponse,
};
use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_server::{
    MetricsService, MetricsServiceServer,
};
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::logs::{
    LogBatch, LogExporter, SdkLogRecord, SdkLoggerProvider, SimpleLogProcessor,
};
use prost::Message as _;
use runtime::task_history::correlation;
use runtime_request_context::{Protocol, RequestContext};
use spiced::otel_logs::{self, LogsSink};
use tracing_opentelemetry::OpenTelemetrySpanExt as _;
use tracing_subscriber::{Layer as _, filter, layer::SubscriberExt as _};

/// A trace id a client pins with `spice-trace-id`.
const PINNED: &str = "4bf92f3577b34da6a3ce929d0e0e4736";

/// The export request header the receivers record with each record.
const TOKEN_HEADER: &str = "x-otel-token";

/// A log exporter that keeps every record it is sent.
#[derive(Clone, Debug, Default)]
struct Collected(Arc<Mutex<Vec<SdkLogRecord>>>);

impl LogExporter for Collected {
    fn export(&self, batch: LogBatch<'_>) -> impl Future<Output = OTelSdkResult> + Send {
        self.0
            .lock()
            .expect("collected records")
            .extend(batch.iter().map(|(record, _)| record.clone()));
        std::future::ready(Ok(()))
    }
}

/// The records exported while `f` runs under a subscriber assembled the way
/// `init_tracing` assembles it, with the log layer armed. `task_history` adds
/// the OpenTelemetry layer that numbers the task-history table's spans.
fn exported_by(task_history: bool, f: impl FnOnce()) -> Vec<SdkLogRecord> {
    let collected = Collected::default();
    let provider = SdkLoggerProvider::builder()
        .with_log_processor(SimpleLogProcessor::new(collected.clone()))
        .build();
    let sink = LogsSink::default();
    assert!(sink.arm(provider.logger("spiced")), "a fresh sink arms");

    let tracer_provider = opentelemetry_sdk::trace::SdkTracerProvider::builder().build();
    let task_history_layer = task_history.then(|| {
        tracing_opentelemetry::layer()
            .with_tracer(tracer_provider.tracer("task_history"))
            .with_filter(filter::filter_fn(|metadata| {
                metadata.target() == "task_history"
            }))
    });
    let subscriber = tracing_subscriber::registry()
        .with(task_history_layer)
        .with(otel_logs::layer(sink));
    tracing::subscriber::with_default(subscriber, f);

    collected.0.lock().expect("collected records").clone()
}

fn only(records: &[SdkLogRecord]) -> &SdkLogRecord {
    assert_eq!(records.len(), 1, "expected one record, got {records:#?}");
    &records[0]
}

fn bodies(records: &[SdkLogRecord]) -> Vec<String> {
    records
        .iter()
        .map(|record| match record.body() {
            Some(AnyValue::String(body)) => body.to_string(),
            other => panic!("every record has a string body, got {other:?}"),
        })
        .collect()
}

fn pinned_request() -> RequestContext {
    RequestContext::builder(Protocol::Http)
        .with_client_trace_id(Some(Arc::from(PINNED)))
        .build()
}

#[test]
fn an_event_becomes_a_record_with_its_severity_scope_body_and_typed_fields() {
    let records = exported_by(false, || {
        tracing::info!(
            target: "runtime::init::dataset",
            dataset = "trips",
            rows = 3_u64,
            ratio = 0.5,
            cached = true,
            "Loaded 3 rows for dataset trips"
        );
    });

    let record = only(&records);
    assert_eq!(record.severity_number(), Some(Severity::Info));
    assert_eq!(record.severity_text(), Some("INFO"));
    assert_eq!(
        record.target().map(AsRef::as_ref),
        Some("runtime::init::dataset")
    );
    assert_eq!(bodies(&records), vec!["Loaded 3 rows for dataset trips"]);
    let attributes: Vec<(String, AnyValue)> = record
        .attributes_iter()
        .map(|(key, value)| (key.to_string(), value.clone()))
        .collect();
    assert_eq!(
        attributes,
        vec![
            ("dataset".to_string(), AnyValue::from("trips".to_string())),
            ("rows".to_string(), AnyValue::Int(3)),
            ("ratio".to_string(), AnyValue::Double(0.5)),
            ("cached".to_string(), AnyValue::Boolean(true)),
        ]
    );
    assert!(
        record.timestamp().is_some(),
        "the record says when it was logged"
    );
    assert!(
        record.trace_context().is_none(),
        "a record logged outside any task belongs to no trace"
    );
}

#[test]
fn a_multi_line_message_is_one_record() {
    let records = exported_by(false, || {
        tracing::error!(target: "runtime", "Failed to plan query:\nSchema error: No field named foo");
    });

    assert_eq!(only(&records).severity_number(), Some(Severity::Error));
    assert_eq!(
        bodies(&records),
        vec!["Failed to plan query:\nSchema error: No field named foo"]
    );
}

/// The trace contexts of `records`, each as `(trace id, span id)`.
fn trace_contexts(records: &[SdkLogRecord]) -> Vec<Option<(TraceId, SpanId)>> {
    records
        .iter()
        .map(|record| {
            record
                .trace_context()
                .map(|trace| (trace.trace_id, trace.span_id))
        })
        .collect()
}

/// The id a client pinned is the one its query's records carry — not the
/// OpenTelemetry span's own trace id — so the records, the response's
/// `spice-trace-id`, and the `runtime.task_history` row all agree.
///
/// `query.rs` creates the task span and the trace span as siblings and
/// instruments the query with the task span inside the trace span, so the
/// current span is the task's and the trace span is not among its ancestors.
/// The record must find the id there too.
#[test]
fn a_record_in_a_query_carries_the_pinned_trace_id_and_the_task_span_id() {
    let mut task_span_id = SpanId::INVALID;
    let records = exported_by(true, || {
        let task = tracing::info_span!(target: "task_history", "sql_query");
        task_span_id = task.context().span().span_context().span_id();
        let trace = correlation::begin_task_trace(&task, &pinned_request());
        trace.in_scope(|| {
            task.in_scope(|| {
                tracing::warn!(target: "data_components", "connection reset by peer");
            });
        });
        task.in_scope(|| {
            trace.in_scope(|| {
                tracing::warn!(target: "data_components", "connection reset by peer");
            });
        });
    });

    assert_ne!(
        task_span_id,
        SpanId::INVALID,
        "task history numbered the task"
    );
    let expected = Some((
        TraceId::from_hex(PINNED).expect("a valid trace id"),
        task_span_id,
    ));
    assert_eq!(trace_contexts(&records), vec![expected, expected]);
}

#[test]
fn a_record_in_a_query_carries_the_trace_id_with_task_history_off() {
    let records = exported_by(false, || {
        let task = tracing::info_span!(target: "task_history", "sql_query");
        let trace = correlation::begin_task_trace(&task, &pinned_request());
        trace.in_scope(|| {
            task.in_scope(|| {
                tracing::warn!(target: "data_components", "connection reset by peer");
            });
        });
    });

    assert_eq!(
        trace_contexts(&records),
        vec![Some((
            TraceId::from_hex(PINNED).expect("a valid trace id"),
            SpanId::INVALID
        ))],
        "the query's trace id does not depend on the task-history table"
    );
}

/// Runs `log` on another thread inside `span` alone, the way the query runtime
/// polls a query's plan: `run_record_batch_stream_on_runtime` instruments its
/// driver with the task span and nothing else, so a connector logging during
/// the scan has no trace span in scope.
fn on_query_runtime(span: &tracing::Span, log: impl FnOnce() + Send) {
    let dispatch = tracing::dispatcher::get_default(Clone::clone);
    std::thread::scope(|scope| {
        scope.spawn(|| tracing::dispatcher::with_default(&dispatch, || span.in_scope(log)));
    });
}

fn pinned_trace_id() -> TraceId {
    TraceId::from_hex(PINNED).expect("a valid trace id")
}

/// A record logged while the query executes carries the id the client pinned,
/// like one logged while it is planned. Regression test for #15357, where it
/// carried the trace id the SDK generated for the task span, which the
/// `runtime.task_history` row replaces with the pinned one.
#[test]
fn a_record_logged_while_the_query_executes_carries_the_pinned_trace_id() {
    let mut task_span_id = SpanId::INVALID;
    let records = exported_by(true, || {
        let task = tracing::info_span!(target: "task_history", "sql_query");
        let _trace = correlation::begin_task_trace(&task, &pinned_request());
        task_span_id = task.context().span().span_context().span_id();
        on_query_runtime(&task, || {
            tracing::debug!(target: "data_components::http::provider", "Pagination page 0: 1 rows fetched, total so far: 1");
        });
    });

    assert_ne!(
        task_span_id,
        SpanId::INVALID,
        "task history numbered the task"
    );
    assert_eq!(
        trace_contexts(&records),
        vec![Some((pinned_trace_id(), task_span_id))]
    );
}

/// `runtime.task_history` writes every row of the query's trace under the
/// pinned id, so a record in a task the query started carries it too, with
/// that task's own span id.
#[test]
fn a_record_in_a_task_the_query_started_carries_the_pinned_trace_id() {
    let mut child_span_id = SpanId::INVALID;
    let records = exported_by(true, || {
        let task = tracing::info_span!(target: "task_history", "sql_query");
        let _trace = correlation::begin_task_trace(&task, &pinned_request());
        let child = task.in_scope(|| tracing::info_span!(target: "task_history", "ballista_stage"));
        child_span_id = child.context().span().span_context().span_id();
        on_query_runtime(&child, || {
            tracing::warn!(target: "data_components", "connection reset by peer");
        });
    });

    assert_ne!(
        child_span_id,
        SpanId::INVALID,
        "task history numbered the child"
    );
    assert_eq!(
        trace_contexts(&records),
        vec![Some((pinned_trace_id(), child_span_id))]
    );
}

/// A nested query declares the pinned id its parent declared. It finishing
/// first must not take the id from the parent's records.
#[test]
fn a_nested_query_finishing_first_leaves_the_parent_its_pinned_trace_id() {
    let mut task_span_id = SpanId::INVALID;
    let records = exported_by(true, || {
        let request = pinned_request();
        let task = tracing::info_span!(target: "task_history", "sql_query");
        let _trace = correlation::begin_task_trace(&task, &request);
        task_span_id = task.context().span().span_context().span_id();
        task.in_scope(|| {
            let nested = tracing::info_span!(target: "task_history", "sql_query");
            let _nested_trace = correlation::begin_task_trace(&nested, &request);
        });
        on_query_runtime(&task, || {
            tracing::warn!(target: "data_components", "connection reset by peer");
        });
    });

    assert_eq!(
        trace_contexts(&records),
        vec![Some((pinned_trace_id(), task_span_id))]
    );
}

/// The tasks other than a query that take a pinned id — chat, NSQL, responses,
/// decisions — declare it the same way, and their records carry it.
#[test]
fn a_record_in_another_task_that_took_a_pinned_id_carries_it() {
    let mut task_span_id = SpanId::INVALID;
    let records = exported_by(true, || {
        let task = tracing::info_span!(target: "task_history", "ai_chat");
        correlation::record_task_history_trace_id(&task, &pinned_request());
        task_span_id = task.context().span().span_context().span_id();
        task.in_scope(|| {
            tracing::warn!(target: "llms::chat", "the model returned no choices");
        });
    });

    assert_eq!(
        trace_contexts(&records),
        vec![Some((pinned_trace_id(), task_span_id))]
    );
}

/// A task's pinned id is kept as long as the task span is open, which can be
/// after its trace span closes, and no longer: the ids are kept process-wide,
/// so one that outlived its task would leak an entry per pinned query.
#[test]
fn a_pinned_trace_id_is_kept_while_its_task_is_open() {
    exported_by(true, || {
        let task = tracing::info_span!(target: "task_history", "sql_query");
        let trace = correlation::begin_task_trace(&task, &pinned_request());
        let task_trace_id = task.context().span().span_context().trace_id();
        assert_ne!(
            task_trace_id,
            TraceId::INVALID,
            "task history numbered the task"
        );
        assert_ne!(
            task_trace_id,
            pinned_trace_id(),
            "the SDK numbered the task"
        );
        assert_eq!(
            correlation::task_history_trace_id(task_trace_id),
            pinned_trace_id()
        );

        drop(trace);
        assert_eq!(
            correlation::task_history_trace_id(task_trace_id),
            pinned_trace_id(),
            "the task span alone keeps it"
        );

        drop(task);
        assert_eq!(
            correlation::task_history_trace_id(task_trace_id),
            task_trace_id,
            "a closed task keeps nothing"
        );
    });
}

#[test]
fn a_record_in_another_recorded_task_carries_that_tasks_ids() {
    let mut task = None;
    let records = exported_by(true, || {
        let span = tracing::info_span!(target: "task_history", "accelerated_refresh");
        let context = span.context().span().span_context().clone();
        task = Some((context.trace_id(), context.span_id()));
        span.in_scope(|| {
            tracing::info!(target: "runtime_table::accelerated::refresh_task", "Loading data for dataset trips");
        });
    });

    let trace = only(&records)
        .trace_context()
        .expect("a record logged in a recorded task belongs to its trace");
    assert_eq!(
        Some((trace.trace_id, trace.span_id)),
        task,
        "the record names the task's row in `runtime.task_history`"
    );
}

/// Task-history bookkeeping is not exported, as it is not printed. A
/// transport crate's events are, when they are not the export's own: the Flight
/// server's `h2` and a connector's `reqwest` log on query and server threads.
#[test]
fn task_history_bookkeeping_is_not_exported() {
    let records = exported_by(false, || {
        tracing::info!(target: "task_history", "labels");
        tracing::warn!(target: "h2::codec", "frame size exceeded on a Flight stream");
    });

    assert_eq!(
        bodies(&records),
        vec!["frame size exceeded on a Flight stream"]
    );
}

/// Every batch the export sends logs its HTTP client's, connection's, and
/// TLS's events on the export runtime. Exported, each batch would fill the
/// next: one record logged here would become a stream that never stops.
#[test]
fn the_exports_own_events_are_not_exported() {
    crate::require_process_per_test("it installs the global tracing subscriber");

    // The receiver stands in for a collector, which is another process: its
    // own server's events are kept from the layer, so only the export can
    // feed the export.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .thread_name("otlp-receiver")
        .enable_all()
        .build()
        .expect("receiver runtime");
    let inbox = Inbox::default();
    let receiver = serve_http(&rt, inbox.clone());

    // Global, at `trace`: the export runtime's threads must reach the layer,
    // with every event the transport logs.
    let sink = LogsSink::default();
    tracing::subscriber::set_global_default(
        tracing_subscriber::registry()
            .with(tracing_subscriber::EnvFilter::new("trace"))
            .with(
                otel_logs::layer(sink.clone()).with_filter(filter::filter_fn(|_| {
                    std::thread::current().name() != Some("otlp-receiver")
                })),
            ),
    )
    .expect("install the subscriber");
    let config = app::spicepod::component::runtime::OtelExporterConfig {
        enabled: true,
        endpoint: format!("http://{receiver}"),
        push_interval: "60s".to_string(),
        metrics: vec![],
        headers: std::collections::HashMap::new(),
        temporality: app::spicepod::component::runtime::OtelTemporality::default(),
        signals: vec![app::spicepod::component::runtime::OtelSignal::Logs],
    };
    otel_logs::start(
        &sink,
        &config,
        std::collections::HashMap::new(),
        opentelemetry_sdk::Resource::builder().build(),
    )
    .expect("log export starts");

    tracing::info!(target: "runtime", "the only record");
    inbox.wait_for("logged by the test", Duration::from_secs(10), |r| {
        r.body == "the only record"
    });
    // Time is what is under test: a loop shows as records that keep arriving,
    // so watch for three batch periods after the first export.
    std::thread::sleep(Duration::from_secs(3));
    otel_logs::shutdown();

    assert_eq!(
        inbox
            .all()
            .iter()
            .map(|r| r.body.as_str())
            .collect::<Vec<_>>(),
        vec!["the only record"]
    );
}

/// A slow endpoint holds the export task while records fill its queue, and the
/// SDK refuses a flush while the queue is full: `shutdown` retries until the
/// queue drains, so every record queued before it is still delivered.
#[test]
fn shutdown_delivers_a_full_queue_to_a_slow_endpoint() {
    crate::require_process_per_test(
        "it starts the process's one log export, sized by the environment",
    );
    // SAFETY: the test runs in a process of its own (checked above), and no
    // thread that reads the environment has started yet.
    unsafe {
        // Room for four queued records, and an export for each record.
        std::env::set_var("OTEL_BLRP_MAX_QUEUE_SIZE", "4");
        std::env::set_var("OTEL_BLRP_MAX_EXPORT_BATCH_SIZE", "1");
    }

    let rt = tokio::runtime::Runtime::new().expect("receiver runtime");
    let inbox = Inbox::default();
    let (receiver, _) = serve_grpc_answering_after(&rt, inbox.clone(), Duration::from_millis(300));
    let sink = LogsSink::default();
    let config = app::spicepod::component::runtime::OtelExporterConfig {
        enabled: true,
        endpoint: receiver.to_string(),
        push_interval: "60s".to_string(),
        metrics: vec![],
        headers: std::collections::HashMap::new(),
        temporality: app::spicepod::component::runtime::OtelTemporality::default(),
        signals: vec![app::spicepod::component::runtime::OtelSignal::Logs],
    };
    otel_logs::start(
        &sink,
        &config,
        std::collections::HashMap::new(),
        opentelemetry_sdk::Resource::builder().build(),
    )
    .expect("log export starts");

    tracing::subscriber::with_default(
        tracing_subscriber::registry().with(otel_logs::layer(sink)),
        || {
            tracing::info!(target: "runtime", "record 0");
            // The endpoint holds record 0's export, so the export task takes
            // nothing more from the queue until it answers: the next four
            // records fill the queue.
            inbox.wait_for("for the first record", Duration::from_secs(10), |r| {
                r.body == "record 0"
            });
            for n in 1..=4 {
                tracing::info!(target: "runtime", "record {n}");
            }
        },
    );
    otel_logs::shutdown();

    assert_eq!(
        inbox
            .all()
            .iter()
            .map(|r| r.body.as_str())
            .collect::<Vec<_>>(),
        vec!["record 0", "record 1", "record 2", "record 3", "record 4"]
    );
}

#[test]
fn a_log_crate_record_keeps_its_own_target_and_level() {
    let records = exported_by(false, || {
        tracing_log::format_trace(
            &tracing_log::log::Record::builder()
                .target("datafusion_optimizer::optimizer")
                .level(tracing_log::log::Level::Warn)
                .module_path(Some("datafusion_optimizer::optimizer"))
                .file(Some("optimizer.rs"))
                .line(Some(42))
                .args(format_args!("Skipping optimizer rule"))
                .build(),
        )
        .expect("the record is dispatched");
    });

    let record = only(&records);
    assert_eq!(
        record.target().map(AsRef::as_ref),
        Some("datafusion_optimizer::optimizer")
    );
    assert_eq!(record.severity_number(), Some(Severity::Warn));
    assert_eq!(bodies(&records), vec!["Skipping optimizer rule"]);
    assert_eq!(
        record.attributes_iter().count(),
        0,
        "the bridged record's `log.*` fields are its metadata, not attributes"
    );
}

/// A cluster executor installs the layer before the scheduler's settings
/// arrive, and arms it once they do.
#[test]
fn records_are_exported_only_once_the_sink_is_armed() {
    let collected = Collected::default();
    let provider = SdkLoggerProvider::builder()
        .with_log_processor(SimpleLogProcessor::new(collected.clone()))
        .build();
    let sink = LogsSink::default();
    let subscriber = tracing_subscriber::registry().with(otel_logs::layer(sink.clone()));

    tracing::subscriber::with_default(subscriber, || {
        tracing::info!(target: "runtime::cluster", "before the scheduler's settings arrive");
        assert!(sink.arm(provider.logger("spiced")), "a fresh sink arms");
        assert!(!sink.arm(provider.logger("spiced")), "a sink arms once");
        tracing::info!(target: "runtime::cluster", "after");
    });

    assert_eq!(
        bodies(&collected.0.lock().expect("collected records")),
        vec!["after"]
    );
}

// --- End to end ------------------------------------------------------------

/// One record as an OTLP receiver got it.
#[derive(Clone, Debug)]
struct Received {
    scope: String,
    severity_number: i32,
    severity_text: String,
    body: String,
    /// Each attribute as `<type>:<value>`, so a test can assert the type.
    attributes: BTreeMap<String, String>,
    trace_id: String,
    span_id: String,
    /// The `service.name` of the resource it was sent under.
    service: String,
    /// The [`TOKEN_HEADER`] of the request that carried it.
    token: Option<String>,
}

fn typed(value: Option<&Value>) -> String {
    match value {
        Some(Value::StringValue(s)) => format!("string:{s}"),
        Some(Value::IntValue(i)) => format!("int:{i}"),
        Some(Value::DoubleValue(d)) => format!("double:{d}"),
        Some(Value::BoolValue(b)) => format!("bool:{b}"),
        other => format!("{other:?}"),
    }
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut hex, byte| {
            let _ = write!(hex, "{byte:02x}");
            hex
        })
}

/// Every record an OTLP receiver has been sent.
#[derive(Clone, Default)]
struct Inbox(Arc<Mutex<Vec<Received>>>);

impl Inbox {
    fn push(&self, request: ExportLogsServiceRequest, token: Option<&str>) {
        let mut received = self.0.lock().expect("inbox");
        for resource in request.resource_logs {
            let service = resource
                .resource
                .iter()
                .flat_map(|resource| &resource.attributes)
                .find(|kv| kv.key == "service.name")
                .and_then(
                    |kv| match kv.value.as_ref().and_then(|v| v.value.as_ref()) {
                        Some(Value::StringValue(name)) => Some(name.clone()),
                        _ => None,
                    },
                )
                .unwrap_or_default();
            for scope_logs in resource.scope_logs {
                let scope = scope_logs.scope.map(|s| s.name).unwrap_or_default();
                for record in scope_logs.log_records {
                    received.push(Received {
                        scope: scope.clone(),
                        severity_number: record.severity_number,
                        severity_text: record.severity_text,
                        body: match record.body.and_then(|b| b.value) {
                            Some(Value::StringValue(body)) => body,
                            other => format!("{other:?}"),
                        },
                        attributes: record
                            .attributes
                            .into_iter()
                            .map(|kv| (kv.key, typed(kv.value.and_then(|v| v.value).as_ref())))
                            .collect(),
                        trace_id: hex(&record.trace_id),
                        span_id: hex(&record.span_id),
                        service: service.clone(),
                        token: token.map(str::to_string),
                    });
                }
            }
        }
    }

    fn all(&self) -> Vec<Received> {
        self.0.lock().expect("inbox").clone()
    }

    /// The first record that satisfies `matches`, waiting up to `within` for it.
    fn wait_for(
        &self,
        what: &str,
        within: Duration,
        matches: impl Fn(&Received) -> bool,
    ) -> Received {
        self.wait_for_spiced(None, what, within, matches)
    }

    /// [`Self::wait_for`] for records `spiced` sends: a timeout also reports
    /// whether it is still running and what it printed, which says why nothing
    /// came.
    fn wait_for_spiced(
        &self,
        spiced: Option<&mut Spiced>,
        what: &str,
        within: Duration,
        matches: impl Fn(&Received) -> bool,
    ) -> Received {
        let deadline = Instant::now() + within;
        loop {
            if let Some(found) = self.all().into_iter().find(|r| matches(r)) {
                return found;
            }
            assert!(
                Instant::now() < deadline,
                "no record {what} within {within:?}; received: {:#?}\n{}",
                self.all().iter().map(|r| &r.body).collect::<Vec<_>>(),
                spiced.map(Spiced::report).unwrap_or_default()
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}

struct GrpcReceiver {
    inbox: Inbox,
    /// How long it holds each export before answering, as a slow endpoint does.
    answer_after: Duration,
}

#[tonic::async_trait]
impl LogsService for GrpcReceiver {
    async fn export(
        &self,
        request: tonic::Request<ExportLogsServiceRequest>,
    ) -> Result<tonic::Response<ExportLogsServiceResponse>, tonic::Status> {
        let token = request
            .metadata()
            .get(TOKEN_HEADER)
            .and_then(|value| value.to_str().ok())
            .map(str::to_string);
        self.inbox.push(request.into_inner(), token.as_deref());
        tokio::time::sleep(self.answer_after).await;
        Ok(tonic::Response::new(ExportLogsServiceResponse::default()))
    }
}

/// Counts the metrics exports a receiver is sent.
struct MetricsCounter(Arc<std::sync::atomic::AtomicUsize>);

#[tonic::async_trait]
impl MetricsService for MetricsCounter {
    async fn export(
        &self,
        _request: tonic::Request<ExportMetricsServiceRequest>,
    ) -> Result<tonic::Response<ExportMetricsServiceResponse>, tonic::Status> {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(tonic::Response::new(ExportMetricsServiceResponse::default()))
    }
}

/// Serves the OTLP logs and metrics services over gRPC on a loopback port, and
/// returns the address with the count of metrics exports received.
fn serve_grpc(
    rt: &tokio::runtime::Runtime,
    inbox: Inbox,
) -> (SocketAddr, Arc<std::sync::atomic::AtomicUsize>) {
    serve_grpc_answering_after(rt, inbox, Duration::ZERO)
}

/// [`serve_grpc`], holding each logs export for `answer_after` before answering it.
fn serve_grpc_answering_after(
    rt: &tokio::runtime::Runtime,
    inbox: Inbox,
    answer_after: Duration,
) -> (SocketAddr, Arc<std::sync::atomic::AtomicUsize>) {
    let listener = rt
        .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
        .expect("bind the gRPC receiver");
    let address = listener.local_addr().expect("the receiver has an address");
    let metrics_exports = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    rt.spawn(
        tonic::transport::Server::builder()
            .add_service(LogsServiceServer::new(GrpcReceiver {
                inbox,
                answer_after,
            }))
            .add_service(MetricsServiceServer::new(MetricsCounter(Arc::clone(
                &metrics_exports,
            ))))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );
    (address, metrics_exports)
}

/// Serves OTLP/HTTP on a loopback port, at `/v1/logs` only: a request to any
/// other path is a 404, so a wrong logs URL loses every record.
fn serve_http(rt: &tokio::runtime::Runtime, inbox: Inbox) -> SocketAddr {
    let app = axum::Router::new().route(
        "/v1/logs",
        axum::routing::post(
            move |headers: axum::http::HeaderMap, body: axum::body::Bytes| {
                let inbox = inbox.clone();
                async move {
                    match ExportLogsServiceRequest::decode(body) {
                        Ok(request) => {
                            let token = headers
                                .get(TOKEN_HEADER)
                                .and_then(|value| value.to_str().ok());
                            inbox.push(request, token);
                            (
                                axum::http::StatusCode::OK,
                                [(axum::http::header::CONTENT_TYPE, "application/x-protobuf")],
                                ExportLogsServiceResponse::default().encode_to_vec(),
                            )
                        }
                        Err(_) => (
                            axum::http::StatusCode::BAD_REQUEST,
                            [(axum::http::header::CONTENT_TYPE, "text/plain")],
                            b"not an OTLP logs request".to_vec(),
                        ),
                    }
                }
            },
        ),
    );
    let listener = rt
        .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
        .expect("bind the HTTP receiver");
    let address = listener.local_addr().expect("the receiver has an address");
    rt.spawn(async move { axum::serve(listener, app).await });
    address
}

fn unused_local_addr() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind an ephemeral port");
    listener.local_addr().expect("read ephemeral address")
}

/// A `spiced` process, killed when the test ends if it has not stopped, so a
/// failing test cannot leave a runtime holding its ports and its instance
/// directory for the next one.
/// A running `spiced`, with the directory [`spiced_in`] keeps its output in.
struct Spiced {
    child: Child,
    dir: PathBuf,
}

impl Spiced {
    /// Whether `spiced` is still running (or the status or signal it ended
    /// with), then its stdout and stderr: what a failure message needs to say
    /// why `spiced` sent nothing.
    fn report(&mut self) -> String {
        let state = match self.child.try_wait() {
            Ok(Some(status)) => format!("exited: {status}"),
            Ok(None) => "still running".to_string(),
            Err(error) => format!("state unknown: {error}"),
        };
        let read = |name: &str| std::fs::read_to_string(self.dir.join(name)).unwrap_or_default();
        format!(
            "spiced {state}\nspiced stdout:\n{}\nspiced stderr:\n{}",
            read(STDOUT_LOG),
            read(STDERR_LOG)
        )
    }
}

impl Drop for Spiced {
    fn drop(&mut self) {
        if matches!(self.child.try_wait(), Ok(None)) {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

const STDOUT_LOG: &str = "spiced.log";
const STDERR_LOG: &str = "spiced.stderr.log";

/// A `spiced` command on the Spicepod in `dir`, with its stdout and stderr kept
/// in `dir` for the failure messages below.
///
/// It runs in `dir`, which is also its instance directory: two runtimes in one
/// directory refuse to start, and tests run side by side.
fn spiced_in(dir: &Path, http: SocketAddr) -> Command {
    let log = |name: &str| std::fs::File::create(dir.join(name)).expect("create a log file");
    let mut command = Command::new(env!("CARGO_BIN_EXE_spiced"));
    command
        .arg(dir)
        .args(["--http", &http.to_string()])
        .args(["--flight", &unused_local_addr().to_string()])
        .current_dir(dir)
        .stdout(Stdio::from(log(STDOUT_LOG)))
        .stderr(Stdio::from(log(STDERR_LOG)));
    command
}

fn start_spiced(dir: &Path, http: SocketAddr) -> Spiced {
    spawn(&mut spiced_in(dir, http))
}

fn spawn(command: &mut Command) -> Spiced {
    let dir = command
        .get_current_dir()
        .expect("spiced_in sets the working directory")
        .to_path_buf();
    Spiced {
        child: command.spawn().expect("start spiced"),
        dir,
    }
}

/// Waits for `spiced` to exit; the guard kills it if it has not after `within`.
fn exit_status(spiced: &mut Spiced, within: Duration) -> std::process::ExitStatus {
    let deadline = Instant::now() + within;
    loop {
        if let Some(status) = spiced.child.try_wait().expect("poll spiced") {
            return status;
        }
        assert!(
            Instant::now() < deadline,
            "spiced did not exit within {within:?}\n{}",
            spiced.report()
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Sends `spiced` the `SIGTERM` an orchestrator stops it with, and waits for
/// it to exit.
#[cfg(unix)]
fn terminate(spiced: &mut Spiced) -> std::process::ExitStatus {
    let pid = libc::pid_t::try_from(spiced.child.id()).expect("a pid fits pid_t");
    // SAFETY: `kill` only sends a signal to the child this test started.
    assert_eq!(
        unsafe { libc::kill(pid, libc::SIGTERM) },
        0,
        "signal spiced"
    );
    exit_status(spiced, Duration::from_mins(1))
}

/// A default-feature `spiced` is large; cold loading on a busy macOS builder can
/// take tens of seconds.
const STARTUP: Duration = Duration::from_mins(2);

/// gRPC end to end: a running runtime's records, a failed query correlated
/// with its `runtime.task_history` row, and the records of a graceful
/// shutdown up to `Goodbye!`.
#[cfg(unix)]
#[test]
fn spiced_exports_its_log_over_grpc_through_a_graceful_shutdown() {
    let rt = tokio::runtime::Runtime::new().expect("receiver runtime");
    let inbox = Inbox::default();
    let (receiver, metrics_exports) = serve_grpc(&rt, inbox.clone());

    let dir = tempfile::tempdir().expect("create the spicepod directory");
    std::fs::write(
        dir.path().join("trips.csv"),
        "id,city,amount\n1,Seattle,10.5\n2,Portland,7.25\n3,Boise,3.0\n",
    )
    .expect("write the dataset");
    std::fs::write(
        dir.path().join("spicepod.yaml"),
        format!(
            "version: v1
kind: Spicepod
name: otel-logs-grpc
runtime:
  telemetry:
    otel_exporter:
      endpoint: {receiver}
      push_interval: 1s
      signals: [logs]
  query:
    log_errors: all
datasets:
  - from: file:{data}
    name: trips
    params:
      file_format: csv
    acceleration:
      enabled: true
",
            data = dir.path().join("trips.csv").display()
        ),
    )
    .expect("write the spicepod");

    let http = unused_local_addr();
    let mut child = start_spiced(dir.path(), http);

    let listening = inbox.wait_for_spiced(
        Some(&mut child),
        "saying the HTTP server is listening",
        STARTUP,
        |r| r.body == format!("Spice Runtime HTTP listening on {http}"),
    );
    assert_eq!(
        (
            listening.scope.as_str(),
            listening.severity_number,
            listening.severity_text.as_str()
        ),
        ("runtime::http", 9, "INFO")
    );
    let registered =
        inbox.wait_for_spiced(Some(&mut child), "registering the dataset", STARTUP, |r| {
            r.body.starts_with("Dataset trips registered")
        });
    assert!(
        registered
            .attributes
            .get("duration_ms")
            .is_some_and(|value| value.starts_with("int:")),
        "a numeric field arrives as an integer attribute: {registered:?}"
    );

    // A query that fails while running, under an id the client pinned.
    let client = reqwest::Client::new();
    let response = rt
        .block_on(
            client
                .post(format!("http://{http}/v1/sql"))
                .header("spice-trace-id", PINNED)
                .header("Content-Type", "text/plain")
                .body("SELECT id / 0 FROM trips")
                .send(),
        )
        .expect("send the query");
    assert!(
        !response.status().is_success(),
        "dividing by zero fails the query"
    );

    let failed = inbox.wait_for_spiced(
        Some(&mut child),
        "for the failed query",
        Duration::from_secs(30),
        |r| r.body.starts_with("Query failed: ") && r.trace_id == PINNED,
    );
    assert_eq!(
        (failed.severity_number, failed.severity_text.as_str()),
        (13, "WARN"),
        "a query that failed while running is a warning: {failed:?}"
    );
    assert_eq!(
        failed.attributes.get("datasets").map(String::as_str),
        Some("string:trips")
    );
    assert!(
        failed.attributes.contains_key("error_code"),
        "the error code is an attribute an alert can filter on: {failed:?}"
    );

    // The record's ids are the failed query's row in `runtime.task_history`.
    let deadline = Instant::now() + Duration::from_secs(60);
    let row = loop {
        let rows: serde_json::Value = rt
            .block_on(async {
                client
                    .post(format!("http://{http}/v1/sql"))
                    .header("Content-Type", "text/plain")
                    .body(format!(
                        "SELECT trace_id, span_id FROM runtime.task_history WHERE trace_id = '{PINNED}' AND task = 'sql_query'"
                    ))
                    .send()
                    .await?
                    .json()
                    .await
            })
            .expect("query runtime.task_history");
        if let Some(row) = rows.as_array().and_then(|rows| rows.first()).cloned() {
            break row;
        }
        assert!(
            Instant::now() < deadline,
            "the failed query never reached runtime.task_history"
        );
        std::thread::sleep(Duration::from_millis(250));
    };
    assert_eq!(
        (row["trace_id"].as_str(), row["span_id"].as_str()),
        (
            Some(failed.trace_id.as_str()),
            Some(failed.span_id.as_str())
        ),
        "the record and the task-history row name one span"
    );

    // A graceful shutdown flushes everything, `Goodbye!` included.
    let status = terminate(&mut child);
    assert!(
        status.success(),
        "spiced exits cleanly: {status}\n{}",
        child.report()
    );
    inbox.wait_for_spiced(
        Some(&mut child),
        "saying goodbye",
        Duration::from_secs(5),
        |r| r.body == "Goodbye!",
    );

    let exported = inbox.all();
    assert!(
        exported.iter().all(|r| r.scope != "task_history"),
        "task-history bookkeeping is not exported"
    );
    // The run outlasted many 1s metric intervals.
    assert_eq!(
        metrics_exports.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "`signals: [logs]` sends no metrics"
    );
}

/// Serves a one-row JSON array at every path on a loopback port, as the origin
/// of an `http` dataset.
fn serve_items(rt: &tokio::runtime::Runtime) -> SocketAddr {
    let app = axum::Router::new().fallback(|| async {
        (
            [(axum::http::header::CONTENT_TYPE, "application/json")],
            r#"[{"id":2,"name":"widget"}]"#,
        )
    });
    let listener = rt
        .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
        .expect("bind the dataset origin");
    let address = listener.local_addr().expect("the origin has an address");
    rt.spawn(async move { axum::serve(listener, app).await });
    address
}

/// Every record a query logs carries the trace id its response returns and its
/// `runtime.task_history` row stores, whether the client pinned that id with
/// `spice-trace-id`, pinned it with `traceparent`, or pinned nothing — the
/// records the HTTP connector logs on the query runtime while the plan executes
/// included. Regression test for #15357.
#[cfg(unix)]
#[test]
fn every_record_of_a_query_carries_the_trace_id_it_returns() {
    const PAGE_LOGGED: &str = "Pagination page 0: 1 rows fetched, total so far: 1";
    const TRACEPARENT_TRACE: &str = "0af7651916cd43dd8448eb211c80319c";

    let rt = tokio::runtime::Runtime::new().expect("receiver runtime");
    let inbox = Inbox::default();
    let (receiver, _) = serve_grpc(&rt, inbox.clone());
    let origin = serve_items(&rt);

    let dir = tempfile::tempdir().expect("create the spicepod directory");
    std::fs::write(
        dir.path().join("spicepod.yaml"),
        format!(
            "version: v1
kind: Spicepod
name: otel-logs-trace-ids
runtime:
  telemetry:
    otel_exporter:
      endpoint: {receiver}
      signals: [logs]
datasets:
  - from: http://{origin}
    name: items
    check_availability: disabled
    params:
      file_format: json
      request_query_filters: enabled
      allowed_request_paths: /**/**
"
        ),
    )
    .expect("write the spicepod");

    let http = unused_local_addr();
    // The connector logs the pages it reads at `debug`.
    let mut child =
        spawn(spiced_in(dir.path(), http).env("SPICED_LOG", "data_components=debug,info"));
    inbox.wait_for_spiced(Some(&mut child), "registering the dataset", STARTUP, |r| {
        r.body.starts_with("Dataset items registered")
    });

    let client = reqwest::Client::new();
    let queries = [
        (
            "spice-trace-id",
            Some(("spice-trace-id", PINNED.to_string())),
        ),
        (
            "traceparent",
            Some((
                "traceparent",
                format!("00-{TRACEPARENT_TRACE}-00f067aa0ba902b7-01"),
            )),
        ),
        ("no pinned id", None),
    ];
    // Each query's case, the trace id it returned, and its task's span id.
    let mut ran = Vec::new();
    for (n, (case, pin)) in queries.into_iter().enumerate() {
        // A distinct query string per case, so each reaches the origin.
        let mut request = client
            .post(format!("http://{http}/v1/sql"))
            .header("Content-Type", "text/plain")
            .body(format!(
                "SELECT * FROM items WHERE request_path = '/items' AND request_query = 'id={n}'"
            ));
        if let Some((header, value)) = &pin {
            request = request.header(*header, value);
        }
        let (status, returned, body) = rt
            .block_on(async {
                let response = request.send().await?;
                let returned = response
                    .headers()
                    .get("spice-trace-id")
                    .and_then(|value| value.to_str().ok())
                    .map(str::to_string);
                Ok::<_, reqwest::Error>((response.status(), returned, response.text().await?))
            })
            .expect("send the query");
        assert!(status.is_success(), "{case}: the query succeeds: {body}");
        let returned = returned.unwrap_or_else(|| panic!("{case}: the response names its trace"));
        match case {
            "spice-trace-id" => assert_eq!(returned, PINNED, "{case}"),
            "traceparent" => assert_eq!(returned, TRACEPARENT_TRACE, "{case}"),
            _ => {}
        }

        let deadline = Instant::now() + Duration::from_secs(60);
        let span_id = loop {
            let rows: serde_json::Value = rt
                .block_on(async {
                    client
                        .post(format!("http://{http}/v1/sql"))
                        .header("Content-Type", "text/plain")
                        .body(format!(
                            "SELECT span_id FROM runtime.task_history WHERE trace_id = '{returned}' AND task = 'sql_query'"
                        ))
                        .send()
                        .await?
                        .json()
                        .await
                })
                .expect("query runtime.task_history");
            if let Some(span_id) = rows
                .as_array()
                .and_then(|rows| rows.first())
                .and_then(|row| row["span_id"].as_str())
            {
                break span_id.to_string();
            }
            assert!(
                Instant::now() < deadline,
                "{case}: the query never reached runtime.task_history under {returned}"
            );
            std::thread::sleep(Duration::from_millis(250));
        };

        // Matched by span id alone: the trace id is what is under test.
        inbox.wait_for_spiced(
            Some(&mut child),
            &format!("{case}: from the query's scan"),
            Duration::from_secs(30),
            |r| r.span_id == span_id && r.body == PAGE_LOGGED,
        );
        ran.push((case, returned, span_id));
    }

    // A graceful shutdown exports every record still queued, so none of the
    // queries' records is left unchecked.
    let status = terminate(&mut child);
    assert!(
        status.success(),
        "spiced exits cleanly: {status}\n{}",
        child.report()
    );
    let exported = inbox.all();
    for (case, returned, span_id) in ran {
        let logged: Vec<(&str, &str)> = exported
            .iter()
            .filter(|r| r.span_id == span_id)
            .map(|r| (r.trace_id.as_str(), r.body.as_str()))
            .collect();
        let bodies: Vec<&str> = logged.iter().map(|(_, body)| *body).collect();
        for logged_during_execution in [
            "Detected file format from Content-Type header: json",
            PAGE_LOGGED,
        ] {
            assert!(
                bodies.contains(&logged_during_execution),
                "{case}: the scan logged {logged_during_execution:?}: {bodies:#?}"
            );
        }
        assert!(
            logged.iter().all(|(trace_id, _)| *trace_id == returned),
            "{case}: every record of the query carries {returned}: {logged:#?}"
        );
    }
}

/// HTTP end to end, on the documented `/v1/metrics` endpoint: a runtime that
/// cannot start still exports the error that ends it, and `Goodbye!`, because
/// log export outlives the query runtime.
#[test]
fn spiced_exports_the_fatal_error_that_ends_it_over_http() {
    let rt = tokio::runtime::Runtime::new().expect("receiver runtime");
    let inbox = Inbox::default();
    let receiver = serve_http(&rt, inbox.clone());

    let dir = tempfile::tempdir().expect("create the spicepod directory");
    std::fs::write(
        dir.path().join("spicepod.yaml"),
        format!(
            "version: v1
kind: Spicepod
name: otel-logs-http
runtime:
  telemetry:
    otel_exporter:
      endpoint: http://{receiver}/v1/metrics
      signals: [metrics, logs]
"
        ),
    )
    .expect("write the spicepod");

    // Hold the HTTP port, so the runtime fails to start its servers.
    let taken = TcpListener::bind("127.0.0.1:0").expect("hold a port");
    let mut child = start_spiced(
        dir.path(),
        taken.local_addr().expect("the held port's address"),
    );
    let status = exit_status(&mut child, STARTUP);
    assert_eq!(status.code(), Some(1), "{}", child.report());

    let fatal = inbox.wait_for_spiced(
        Some(&mut child),
        "for the error that ended spiced",
        Duration::from_secs(5),
        |r| r.severity_text == "ERROR",
    );
    assert_eq!(fatal.scope, "spiced");
    assert!(
        fatal
            .body
            .starts_with("Unable to start Spice Runtime servers: ")
            && fatal.body.contains("Address already in use"),
        "{fatal:?}"
    );
    inbox.wait_for_spiced(
        Some(&mut child),
        "saying goodbye",
        Duration::from_secs(5),
        |r| r.body == "Goodbye!",
    );
}

/// Cluster end to end, over mTLS: an executor has no exporter settings of its
/// own. It exports its log with the scheduler's once it joins, and resolves
/// their header template through the scheduler, the only node that holds the
/// secret. It does so before loading the app's components, so a catalog that
/// cannot be reached, which it retries for as long as it runs, does not hold
/// its log back.
#[cfg(unix)]
#[test]
fn a_cluster_executor_exports_its_log_with_the_schedulers_settings() {
    const TOKEN: &str = "known-only-to-the-scheduler";

    let rt = tokio::runtime::Runtime::new().expect("receiver runtime");
    let inbox = Inbox::default();
    let (receiver, _) = serve_grpc(&rt, inbox.clone());

    let root = tempfile::tempdir().expect("create the cluster directory");
    let pki = test_framework::pki::init_pki(&root.path().join("pki"))
        .expect("create the cluster's certificate authority");
    let state = root.path().join("state");
    let scheduler_dir = root.path().join("scheduler");
    let executor_dir = root.path().join("executor");
    for dir in [&state, &scheduler_dir, &executor_dir] {
        std::fs::create_dir(dir).expect("create a cluster directory");
    }
    std::fs::write(
        scheduler_dir.join("spicepod.yaml"),
        format!(
            "version: v1
kind: Spicepod
name: otel-logs-cluster
runtime:
  state:
    location: file://{state}
  telemetry:
    otel_exporter:
      endpoint: {receiver}
      signals: [logs]
      headers:
        {TOKEN_HEADER}: ${{ secrets:otel_logs_test_token }}
catalogs:
  - from: iceberg:http://{unreachable}/v1/namespaces
    name: unreachable
",
            state = state.display(),
            unreachable = unused_local_addr()
        ),
    )
    .expect("write the scheduler's spicepod");

    // A node is named by its role, as a cluster member and as the
    // `service.name` its records are sent under.
    let node = |role: &str, dir: &Path, cluster: SocketAddr| {
        let certificate = pki
            .create_client_cert(role)
            .expect("issue the node's certificate");
        let mut command = spiced_in(dir, unused_local_addr());
        command
            .args(["--role", role])
            .args(["--node-bind-address", &cluster.to_string()])
            .args(["--node-advertise-address", "localhost"])
            .arg("--node-mtls-ca-certificate-file")
            .arg(&pki.ca_cert_path)
            .arg("--node-mtls-certificate-file")
            .arg(&certificate.cert_path)
            .arg("--node-mtls-key-file")
            .arg(&certificate.key_path)
            .env("SPICE_INSTANCE_ID", role)
            .env("OTEL_SERVICE_NAME", role);
        command
    };
    let exporting = |service: &'static str| {
        move |r: &Received| r.service == service && r.body == "OTEL logs exporter enabled"
    };

    let scheduler_cluster = unused_local_addr();
    let mut scheduler_process = spawn(
        node("scheduler", &scheduler_dir, scheduler_cluster).env("OTEL_LOGS_TEST_TOKEN", TOKEN),
    );
    let scheduler = inbox.wait_for_spiced(
        Some(&mut scheduler_process),
        "from the scheduler starting export",
        STARTUP,
        exporting("scheduler"),
    );
    assert_eq!(scheduler.token.as_deref(), Some(TOKEN));

    // The executor's Spicepod directory is empty, and the secret is not in its
    // environment.
    let mut executor = spawn(
        node("executor", &executor_dir, unused_local_addr())
            .args([
                "--scheduler-address",
                &format!("https://localhost:{}", scheduler_cluster.port()),
            ])
            .env_remove("OTEL_LOGS_TEST_TOKEN"),
    );
    let joined = inbox.wait_for_spiced(
        Some(&mut executor),
        "from the executor starting export",
        STARTUP,
        exporting("executor"),
    );
    assert_eq!(
        joined.token.as_deref(),
        Some(TOKEN),
        "the executor resolves the header through the scheduler: {joined:?}"
    );

    // An executor flushes its export when it stops, as any runtime does. This
    // one is still retrying the catalog, so it stops only once the runtime's
    // 30 s wait for connections to drain has run out.
    let stopped = terminate(&mut executor);
    inbox.wait_for_spiced(
        Some(&mut executor),
        &format!("from the executor saying goodbye (it exited {stopped})"),
        Duration::from_secs(5),
        |r| r.service == "executor" && r.body == "Goodbye!",
    );
}
