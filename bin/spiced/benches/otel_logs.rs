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

//! What exporting `spiced`'s log over OTLP costs each event.
//!
//! Every event goes through a subscriber assembled the way `init_tracing`
//! assembles it — the `info` filter and the console layer, writing to a sink —
//! in five arms:
//!
//! - `off`: no log export, the default.
//! - `unarmed`: the export layer installed but not armed, as on a cluster
//!   executor before its settings arrive.
//! - `armed`: export armed, batching on its own runtime to an exporter that
//!   discards, so the cost measured is the event path, not the network.
//! - `off, in query` and `armed, in query`: the same, with the event logged
//!   inside a query's trace span and task-history span, where the record also
//!   looks up the query's trace id.
//!
//! The arms are interleaved round by round, so drift on the machine lands on all
//! of them. Each round logs a burst and then pauses, which lets the batch
//! processor drain the way it does between bursts in a running runtime, so a
//! full queue does not stand in for the cost of logging. Each arm reports P50,
//! P99, and P99.9 of the per-event latency.
//!
//! ```text
//! cargo bench -p spiced --no-default-features --bench otel_logs
//! ```

use std::sync::Arc;
use std::time::{Duration, Instant};

use opentelemetry::logs::LoggerProvider as _;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::logs::log_processor_with_async_runtime::BatchLogProcessor;
use opentelemetry_sdk::logs::{LogBatch, LogExporter, SdkLoggerProvider};
use runtime::task_history::correlation;
use runtime_request_context::{Protocol, RequestContext};
use spiced::otel_logs::{self, LogsSink};
use tracing::Dispatch;
use tracing_subscriber::{EnvFilter, Layer as _, filter, layer::SubscriberExt as _};

const ROUNDS: usize = 200;
const EVENTS_PER_ROUND: u64 = 1_000;
const PAUSE: Duration = Duration::from_millis(5);

#[derive(Debug)]
struct Discard;

impl LogExporter for Discard {
    fn export(&self, _batch: LogBatch<'_>) -> impl Future<Output = OTelSdkResult> + Send {
        std::future::ready(Ok(()))
    }
}

/// The subscriber `init_tracing` installs, with the log layer exporting through
/// `otel` and, when `task_history`, the layer that numbers task-history spans.
fn subscriber(otel: Option<LogsSink>, task_history: bool) -> Dispatch {
    let tracer_provider = opentelemetry_sdk::trace::SdkTracerProvider::builder().build();
    let task_history_layer = task_history.then(|| {
        tracing_opentelemetry::layer()
            .with_tracer(tracer_provider.tracer("task_history"))
            .with_filter(filter::filter_fn(|metadata| {
                metadata.target() == "task_history"
            }))
    });
    let console = tracing_subscriber::fmt::layer()
        .with_ansi(false)
        .with_writer(std::io::sink)
        .with_filter(filter::filter_fn(|metadata| {
            metadata.target() != "task_history"
        }));
    Dispatch::new(
        tracing_subscriber::registry()
            .with(EnvFilter::new("info"))
            .with(task_history_layer)
            .with(console)
            .with(otel.map(otel_logs::layer)),
    )
}

/// A sink armed the way `otel_logs::start` arms one, exporting to [`Discard`].
fn armed(export_runtime: &tokio::runtime::Runtime) -> (LogsSink, SdkLoggerProvider) {
    let _export_context = export_runtime.enter();
    let provider = SdkLoggerProvider::builder()
        .with_log_processor(
            BatchLogProcessor::builder(Discard, opentelemetry_sdk::runtime::Tokio).build(),
        )
        .build();
    let sink = LogsSink::default();
    assert!(sink.arm(provider.logger("spiced")));
    (sink, provider)
}

struct Arm {
    name: &'static str,
    dispatch: Dispatch,
    in_query: bool,
    samples: Vec<u64>,
}

fn burst(arm: &mut Arm) {
    tracing::dispatcher::with_default(&arm.dispatch, || {
        let request = RequestContext::builder(Protocol::Http)
            .with_client_trace_id(Some(Arc::from("4bf92f3577b34da6a3ce929d0e0e4736")))
            .build();
        let task = arm
            .in_query
            .then(|| tracing::info_span!(target: "task_history", "sql_query"));
        let trace = task
            .as_ref()
            .map(|task| correlation::begin_task_trace(task, &request));
        // Entered the way a query's future is instrumented: the trace span
        // outside, the task span inside.
        let _trace = trace.as_ref().map(tracing::Span::enter);
        let _task = task.as_ref().map(tracing::Span::enter);
        for i in 0..EVENTS_PER_ROUND {
            let start = Instant::now();
            tracing::info!(
                target: "runtime::init::dataset",
                dataset = "trips",
                rows = i,
                "Loaded rows for dataset trips"
            );
            arm.samples
                .push(u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX));
        }
    });
}

/// The `per_mille`th per-mille of `sorted`: 500 is the median, 990 is P99.
fn percentile(sorted: &[u64], per_mille: usize) -> u64 {
    sorted[(sorted.len() - 1) * per_mille / 1000]
}

fn main() {
    let export_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .thread_name("otel-logs-export")
        .enable_all()
        .build()
        .expect("export runtime");
    let (armed_sink, armed_provider) = armed(&export_runtime);
    let (armed_query_sink, armed_query_provider) = armed(&export_runtime);

    let mut arms = vec![
        Arm {
            name: "off",
            dispatch: subscriber(None, false),
            in_query: false,
            samples: Vec::new(),
        },
        Arm {
            name: "unarmed",
            dispatch: subscriber(Some(LogsSink::default()), false),
            in_query: false,
            samples: Vec::new(),
        },
        Arm {
            name: "armed",
            dispatch: subscriber(Some(armed_sink), false),
            in_query: false,
            samples: Vec::new(),
        },
        Arm {
            name: "off, in query",
            dispatch: subscriber(None, true),
            in_query: true,
            samples: Vec::new(),
        },
        Arm {
            name: "armed, in query",
            dispatch: subscriber(Some(armed_query_sink), true),
            in_query: true,
            samples: Vec::new(),
        },
    ];

    for round in 0..ROUNDS {
        // Rotate which arm goes first, so none always runs right after a pause.
        let first = round % arms.len();
        for offset in 0..arms.len() {
            let index = (first + offset) % arms.len();
            burst(&mut arms[index]);
            std::thread::sleep(PAUSE);
        }
    }

    println!(
        "{:<18} {:>9} {:>9} {:>9} {:>9}",
        "arm (ns/event)", "p50", "p99", "p99.9", "samples"
    );
    for arm in &mut arms {
        arm.samples.sort_unstable();
        println!(
            "{:<18} {:>9} {:>9} {:>9} {:>9}",
            arm.name,
            percentile(&arm.samples, 500),
            percentile(&arm.samples, 990),
            percentile(&arm.samples, 999),
            arm.samples.len()
        );
    }

    let _ = armed_provider.shutdown();
    let _ = armed_query_provider.shutdown();
    export_runtime.shutdown_timeout(Duration::from_secs(1));
}
