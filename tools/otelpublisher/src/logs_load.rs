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

//! OTLP logs load generator: concurrent exporters sending log exports as fast as the
//! receiver acknowledges them, reporting acknowledged records per second and the export
//! latency distribution.
//!
//! Each worker owns one connection and sends one export at a time, the way an OpenTelemetry
//! Collector exporter queue consumer does, so `--concurrency` is the number of exports in
//! flight. Every record is unique (its time and a sequence number in its body), so a store
//! that deduplicated or dropped records would show fewer rows than the report counts.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use clap::{Parser, ValueEnum};
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::logs::v1::logs_service_client::LogsServiceClient;
use opentelemetry_proto::tonic::common::v1::{
    AnyValue, InstrumentationScope, KeyValue, any_value::Value,
};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message;
use tonic::IntoRequest;
use tonic::codec::CompressionEncoding;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum Transport {
    /// OTLP/gRPC `LogsService/Export`.
    Grpc,
    /// OTLP/HTTP `POST /v1/logs` with a protobuf body.
    Http,
}

#[derive(Parser, Debug)]
pub struct LogsLoadArgs {
    /// OTLP/gRPC endpoint, or the OTLP/HTTP base URL with `--transport http`.
    #[arg(long, default_value = "http://localhost:50051")]
    pub endpoint: String,

    #[arg(long, value_enum, default_value = "grpc")]
    pub transport: Transport,

    /// Exports in flight at once (one per worker).
    #[arg(long, default_value_t = 8)]
    pub concurrency: usize,

    /// Log records per export.
    #[arg(long, default_value_t = 1024)]
    pub batch: usize,

    /// How long to send, in seconds.
    #[arg(long, default_value_t = 30)]
    pub duration_secs: u64,

    /// Approximate body length of each record, in bytes.
    #[arg(long, default_value_t = 200)]
    pub body_bytes: usize,

    /// Attributes per record.
    #[arg(long, default_value_t = 5)]
    pub attributes: usize,

    /// Distinct `service.name` resources the records are spread over.
    #[arg(long, default_value_t = 4)]
    pub services: usize,

    /// Compress exports with gzip.
    #[arg(long)]
    pub gzip: bool,

    /// API key, sent as `x-api-key`.
    #[arg(long)]
    pub api_key: Option<String>,

    /// Write the report as JSON to this path.
    #[arg(long)]
    pub json_out: Option<String>,
}

const LEVELS: [(&str, i32); 4] = [("INFO", 9), ("WARN", 13), ("ERROR", 17), ("DEBUG", 5)];

fn string(value: impl Into<String>) -> AnyValue {
    AnyValue {
        value: Some(Value::StringValue(value.into())),
    }
}

fn attribute(key: &str, value: AnyValue) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(value),
        ..Default::default()
    }
}

fn now_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX))
}

/// One export of `args.batch` records from `worker`, its `sequence`-th, spread over
/// `args.services` resources.
fn build_export(args: &LogsLoadArgs, worker: usize, sequence: u64) -> ExportLogsServiceRequest {
    let services = args.services.max(1);
    let now = now_nanos();
    let filler = "x".repeat(args.body_bytes.saturating_sub(64));
    let mut resource_logs: Vec<ResourceLogs> = (0..services)
        .map(|s| ResourceLogs {
            resource: Some(Resource {
                attributes: vec![
                    attribute("service.name", string(format!("service-{s}"))),
                    attribute("service.version", string("1.4.2")),
                    attribute("host.name", string(format!("host-{worker}"))),
                    attribute("deployment.environment.name", string("production")),
                ],
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                scope: Some(InstrumentationScope {
                    name: "loadgen".to_string(),
                    version: "1.0".to_string(),
                    ..Default::default()
                }),
                log_records: Vec::with_capacity(args.batch / services + 1),
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        })
        .collect();
    for i in 0..args.batch {
        let (severity_text, severity_number) = LEVELS[i % LEVELS.len()];
        let index = u64::try_from(i).unwrap_or(u64::MAX);
        let mut trace_id = [0_u8; 16];
        trace_id[..8].copy_from_slice(&sequence.to_le_bytes());
        trace_id[8..].copy_from_slice(&index.to_le_bytes());
        trace_id[0] |= 1;
        let record = LogRecord {
            time_unix_nano: now + index,
            observed_time_unix_nano: now + index,
            severity_number,
            severity_text: severity_text.to_string(),
            body: Some(string(format!(
                "worker={worker} export={sequence} record={i} request completed {filler}"
            ))),
            attributes: (0..args.attributes)
                .map(|a| attribute(&format!("attr.{a}"), string(format!("value-{}", i % 97))))
                .collect(),
            flags: 1,
            trace_id: trace_id.to_vec(),
            span_id: trace_id[..8].to_vec(),
            ..Default::default()
        };
        if let Some(scope) = resource_logs[i % services].scope_logs.first_mut() {
            scope.log_records.push(record);
        }
    }
    resource_logs.retain(|r| r.scope_logs.iter().any(|s| !s.log_records.is_empty()));
    ExportLogsServiceRequest { resource_logs }
}

#[derive(Default)]
struct WorkerReport {
    latencies_us: Vec<u64>,
    records: u64,
    errors: BTreeMap<String, u64>,
}

async fn grpc_worker(
    args: Arc<LogsLoadArgs>,
    worker: usize,
    stop: Arc<AtomicBool>,
) -> Result<WorkerReport, Box<dyn std::error::Error + Send + Sync>> {
    let channel = Channel::from_shared(args.endpoint.clone())?
        .connect()
        .await?;
    let mut client = LogsServiceClient::new(channel)
        .max_encoding_message_size(usize::MAX)
        .max_decoding_message_size(usize::MAX);
    if args.gzip {
        client = client.send_compressed(CompressionEncoding::Gzip);
    }
    let api_key = args
        .api_key
        .as_deref()
        .map(MetadataValue::try_from)
        .transpose()?;
    let mut report = WorkerReport::default();
    let mut sequence = 0_u64;
    while !stop.load(Ordering::Relaxed) {
        sequence += 1;
        let mut request = build_export(&args, worker, sequence).into_request();
        if let Some(key) = &api_key {
            request.metadata_mut().insert("x-api-key", key.clone());
        }
        let start = Instant::now();
        let result = client.export(request).await;
        let elapsed = u64::try_from(start.elapsed().as_micros()).unwrap_or(u64::MAX);
        match result {
            Ok(_) => {
                report.latencies_us.push(elapsed);
                report.records += args.batch as u64;
            }
            Err(status) => {
                *report
                    .errors
                    .entry(format!("{:?}: {}", status.code(), status.message()))
                    .or_default() += 1;
            }
        }
    }
    Ok(report)
}

async fn http_worker(
    args: Arc<LogsLoadArgs>,
    worker: usize,
    stop: Arc<AtomicBool>,
) -> Result<WorkerReport, Box<dyn std::error::Error + Send + Sync>> {
    let client = reqwest::Client::new();
    let url = format!("{}/v1/logs", args.endpoint.trim_end_matches('/'));
    let mut report = WorkerReport::default();
    let mut sequence = 0_u64;
    while !stop.load(Ordering::Relaxed) {
        sequence += 1;
        let mut body = build_export(&args, worker, sequence).encode_to_vec();
        let mut request = client
            .post(&url)
            .header("content-type", "application/x-protobuf");
        if args.gzip {
            use std::io::Write as _;
            let mut encoder =
                flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
            encoder.write_all(&body)?;
            body = encoder.finish()?;
            request = request.header("content-encoding", "gzip");
        }
        if let Some(key) = &args.api_key {
            request = request.header("x-api-key", key);
        }
        let start = Instant::now();
        let response = request.body(body).send().await;
        let result = match response {
            Ok(response) => {
                let status = response.status();
                let _ = response.bytes().await;
                if status.is_success() {
                    Ok(())
                } else {
                    Err(format!("HTTP {status}"))
                }
            }
            Err(e) => Err(e.to_string()),
        };
        let elapsed = u64::try_from(start.elapsed().as_micros()).unwrap_or(u64::MAX);
        match result {
            Ok(()) => {
                report.latencies_us.push(elapsed);
                report.records += args.batch as u64;
            }
            Err(error) => *report.errors.entry(error).or_default() += 1,
        }
    }
    Ok(report)
}

fn percentile(sorted: &[u64], p: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    // Nearest rank.
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        clippy::cast_precision_loss,
        reason = "a rank within a latency sample of at most a few million entries"
    )]
    let rank = ((p / 100.0) * sorted.len() as f64).ceil() as usize;
    #[expect(
        clippy::cast_precision_loss,
        reason = "microsecond latencies are far below 2^52"
    )]
    let micros = sorted[rank.clamp(1, sorted.len()) - 1] as f64;
    micros / 1000.0
}

/// Runs the load and prints the report.
///
/// # Errors
///
/// Returns an error if a worker cannot connect or the report cannot be written.
pub async fn run(args: LogsLoadArgs) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Arc::new(args);
    let stop = Arc::new(AtomicBool::new(false));
    let start = Instant::now();
    let workers: Vec<_> = (0..args.concurrency.max(1))
        .map(|worker| {
            let args = Arc::clone(&args);
            let stop = Arc::clone(&stop);
            tokio::spawn(async move {
                match args.transport {
                    Transport::Grpc => grpc_worker(args, worker, stop).await,
                    Transport::Http => http_worker(args, worker, stop).await,
                }
            })
        })
        .collect();
    tokio::time::sleep(Duration::from_secs(args.duration_secs)).await;
    stop.store(true, Ordering::Relaxed);
    let mut latencies = Vec::new();
    let mut records = 0_u64;
    let mut errors: BTreeMap<String, u64> = BTreeMap::new();
    for worker in workers {
        let report = worker.await??;
        latencies.extend(report.latencies_us);
        records += report.records;
        for (error, count) in report.errors {
            *errors.entry(error).or_default() += count;
        }
    }
    let elapsed = start.elapsed().as_secs_f64();
    latencies.sort_unstable();
    #[expect(
        clippy::cast_precision_loss,
        reason = "record counts are far below 2^52"
    )]
    let records_per_sec = records as f64 / elapsed;
    let report = serde_json::json!({
        "transport": format!("{:?}", args.transport),
        "concurrency": args.concurrency,
        "batch": args.batch,
        "body_bytes": args.body_bytes,
        "gzip": args.gzip,
        "elapsed_secs": elapsed,
        "exports": latencies.len(),
        "records": records,
        "records_per_sec": records_per_sec.round(),
        "export_latency_ms": {
            "p50": percentile(&latencies, 50.0),
            "p90": percentile(&latencies, 90.0),
            "p99": percentile(&latencies, 99.0),
            "p99_9": percentile(&latencies, 99.9),
            "max": percentile(&latencies, 100.0),
        },
        "errors": errors,
    });
    println!("{}", serde_json::to_string_pretty(&report)?);
    if let Some(path) = &args.json_out {
        tokio::fs::write(path, serde_json::to_string_pretty(&report)?).await?;
    }
    Ok(())
}
