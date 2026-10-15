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
//! receiver this test serves — gRPC for one run, HTTP for the other — the way it
//! would send to an OpenTelemetry Collector.

use std::collections::BTreeMap;
use std::net::{SocketAddr, TcpListener};
use std::path::Path;
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
    fn push(&self, request: ExportLogsServiceRequest) {
        let mut received = self.0.lock().expect("inbox");
        for resource in request.resource_logs {
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

    /// [`Self::wait_for`] for records a `spiced` in `dir` sends: a timeout
    /// also shows what that `spiced` printed, which says why nothing came.
    fn wait_for_spiced(
        &self,
        dir: Option<&Path>,
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
                "no record {what} within {within:?}; received: {:#?}\nspiced printed:\n{}",
                self.all().iter().map(|r| &r.body).collect::<Vec<_>>(),
                dir.map(stdout_of).unwrap_or_default()
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}

struct GrpcReceiver(Inbox);

#[tonic::async_trait]
impl LogsService for GrpcReceiver {
    async fn export(
        &self,
        request: tonic::Request<ExportLogsServiceRequest>,
    ) -> Result<tonic::Response<ExportLogsServiceResponse>, tonic::Status> {
        self.0.push(request.into_inner());
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
    let listener = rt
        .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
        .expect("bind the gRPC receiver");
    let address = listener.local_addr().expect("the receiver has an address");
    let metrics_exports = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    rt.spawn(
        tonic::transport::Server::builder()
            .add_service(LogsServiceServer::new(GrpcReceiver(inbox)))
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
        axum::routing::post(move |body: axum::body::Bytes| {
            let inbox = inbox.clone();
            async move {
                match ExportLogsServiceRequest::decode(body) {
                    Ok(request) => {
                        inbox.push(request);
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
        }),
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
struct Spiced(Child);

impl Drop for Spiced {
    fn drop(&mut self) {
        if matches!(self.0.try_wait(), Ok(None)) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}

/// Starts `spiced` on the Spicepod in `dir`, with its stdout kept in
/// `dir/spiced.log` for the failure messages below.
///
/// It runs in `dir`, which is also its instance directory: two runtimes in one
/// directory refuse to start, and tests run side by side.
fn start_spiced(dir: &Path, http: SocketAddr) -> Spiced {
    let log = std::fs::File::create(dir.join("spiced.log")).expect("create the log file");
    Spiced(
        Command::new(env!("CARGO_BIN_EXE_spiced"))
            .arg(dir)
            .args(["--http", &http.to_string()])
            .args(["--flight", &unused_local_addr().to_string()])
            .current_dir(dir)
            .stdout(Stdio::from(log))
            .stderr(Stdio::null())
            .spawn()
            .expect("start spiced"),
    )
}

fn stdout_of(dir: &Path) -> String {
    std::fs::read_to_string(dir.join("spiced.log")).unwrap_or_default()
}

/// Waits for `spiced` to exit; the guard kills it if it has not after `within`.
fn exit_status(spiced: &mut Spiced, within: Duration) -> std::process::ExitStatus {
    let deadline = Instant::now() + within;
    loop {
        if let Some(status) = spiced.0.try_wait().expect("poll spiced") {
            return status;
        }
        assert!(
            Instant::now() < deadline,
            "spiced did not exit within {within:?}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
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
        Some(dir.path()),
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
        inbox.wait_for_spiced(Some(dir.path()), "registering the dataset", STARTUP, |r| {
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
        Some(dir.path()),
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
    let pid = libc::pid_t::try_from(child.0.id()).expect("a pid fits pid_t");
    // SAFETY: `kill` only sends a signal to the child this test started.
    assert_eq!(
        unsafe { libc::kill(pid, libc::SIGTERM) },
        0,
        "signal spiced"
    );
    let status = exit_status(&mut child, Duration::from_mins(1));
    assert!(
        status.success(),
        "spiced exits cleanly: {status}\n{}",
        stdout_of(dir.path())
    );
    inbox.wait_for_spiced(
        Some(dir.path()),
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
    assert_eq!(status.code(), Some(1), "{}", stdout_of(dir.path()));

    let fatal = inbox.wait_for_spiced(
        Some(dir.path()),
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
        Some(dir.path()),
        "saying goodbye",
        Duration::from_secs(5),
        |r| r.body == "Goodbye!",
    );
}
