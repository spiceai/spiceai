/*
Copyright 2026 The Spice.ai OSS Authors

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

//! Streaming ingestion benchmark runner.
//!
//! Assumes the stream was prepared with `prepare-ingestion-stream`.
//!
//! ## Flow
//! 1. Derive `source_from` and `kafka_bootstrap_servers` from `--topic` / `--stream-mode`
//! 2. Load and transform the spicepod (inject source, unique consumer group, file paths)
//! 3. Start Spice with metrics enabled
//! 4. Poll until the permanent `benchmark-ready` marker appears in the accelerated table
//! 5. Record `ingestion_duration_ms` and `records_per_second`, emit telemetry
//! 6. Stop Spice

use std::time::{Duration, Instant};

use arrow::array::RecordBatch;
use futures::TryStreamExt;
use spicepod::acceleration::Mode;
use spicepod::component::ComponentOrReference;
use spicepod::metric::{Metric, Metrics};
use spicepod::param::{ParamValue, Params};
use test_framework::anyhow::{self, Context, Result};
use test_framework::git;
use test_framework::opentelemetry::KeyValue;
use test_framework::opentelemetry_sdk::Resource;
use test_framework::spiced::{SpicedInstance, StartRequest};

use super::sources::debezium::MARKER_EVENT_TYPE;
use super::utils::{generate_run_id, get_count_from_batch, load_spicepod_definition, write_temp_spicepod};
use crate::args::StreamingIngestionArgs;
use crate::args::streaming::StreamMode;
use crate::commands::create_telemetry_with_resource;
use crate::health::HealthMonitor;

/// Derive the spicepod `from` field and Kafka bootstrap servers from CLI args.
fn resolve_source(args: &StreamingIngestionArgs) -> (String, String) {
    let bootstrap = std::env::var("KAFKA_BOOTSTRAP_SERVERS")
        .unwrap_or_else(|_| "localhost:9092".to_string());

    let source_from = match args.stream_mode {
        StreamMode::Kafka => format!("kafka:{}", args.topic),
        StreamMode::Debezium => {
            format!("debezium:{}.public.events_bench", args.topic)
        }
    };

    (source_from, bootstrap)
}

/// Run the streaming ingestion benchmark against a pre-prepared stream.
#[expect(clippy::too_many_lines)]
pub async fn run_ingestion(args: &StreamingIngestionArgs) -> Result<()> {
    let run_id = generate_run_id();

    let config_name = args
        .common
        .spicepod_path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("unknown")
        .to_string();

    println!("=== Streaming Ingestion Benchmark ===");
    println!("Run ID:      {run_id}");
    println!("Config:      {config_name}");
    println!("Stream mode: {}", args.stream_mode);
    println!("Topic:       {}", args.topic);
    println!("Rows:        {}", args.rows);

    let (source_from, kafka_bootstrap_servers) = resolve_source(args);
    println!("Source:      {source_from}");
    println!("Bootstrap:   {kafka_bootstrap_servers}");

    // Phase 1: Load and transform spicepod
    println!("\n[1/5] Loading and transforming spicepod");
    let spicepod_def = load_spicepod_definition(&args.common.spicepod_path)?;
    let transformed = transform_ingestion_spicepod(
        spicepod_def,
        &run_id,
        &source_from,
        &kafka_bootstrap_servers,
    );
    let temp_path = write_temp_spicepod(&transformed, &run_id, &config_name, "ingestion")?;

    // Phase 2: Start Spice
    println!("\n[2/5] Starting Spice");
    let mut start_request = StartRequest::new(args.common.spiced_path_buf(), transformed)?
        .with_additional_args(vec!["--metrics".to_string(), "0.0.0.0:9090".to_string()]);

    if let Some(ref data_dir) = args.common.data_dir {
        start_request = start_request.with_data_dir(data_dir.clone());
    }

    let mut spiced_instance = SpicedInstance::start(start_request).await?;
    spiced_instance
        .wait_for_ready(Duration::from_secs(args.common.ready_wait))
        .await?;
    let spice_ready_time = Instant::now();

    let spiced_version = spiced_instance.version().to_string();

    // Phase 3: Build telemetry resource
    println!("\n[3/5] Building telemetry resource");
    let testoperator_commit_sha = git::get_commit_sha();
    let spiced_commit_sha =
        std::env::var("SPICED_COMMIT").unwrap_or_else(|_| "unknown".to_string());
    let branch_name = git::get_branch_name();

    let benchmark_resource = Resource::builder_empty()
        .with_attributes(vec![
            KeyValue::new("service.name", "testoperator"),
            KeyValue::new("type", "streaming_ingestion_benchmark"),
            KeyValue::new("config_name", config_name.clone()),
            KeyValue::new("run_id", run_id.clone()),
            KeyValue::new("stream_mode", args.stream_mode.to_string()),
            KeyValue::new("topic", args.topic.clone()),
            KeyValue::new("rows", args.rows.to_string()),
            KeyValue::new("testoperator_commit_sha", testoperator_commit_sha),
            KeyValue::new("spiced_commit_sha", spiced_commit_sha),
            KeyValue::new("spiced_version", spiced_version),
            KeyValue::new("branch_name", branch_name),
        ])
        .build();

    let telemetry = create_telemetry_with_resource(&args.common, benchmark_resource);

    // Phase 4: Start health monitor (optional)
    let health_monitor = if args.enable_liveness {
        println!("Starting health liveness monitor");
        Some(HealthMonitor::spawn()?)
    } else {
        None
    };

    // Phase 5: Poll for the permanent benchmark-ready marker
    println!("\n[4/5] Polling for '{MARKER_EVENT_TYPE}' marker...");
    let marker_query = format!(
        "SELECT COUNT(*) FROM events WHERE event_type = '{MARKER_EVENT_TYPE}'"
    );
    let timeout = Duration::from_secs(args.common.ready_wait);
    let detected =
        poll_for_marker(&spiced_instance, &marker_query, 1, timeout).await?;

    let ingestion_duration = spice_ready_time.elapsed();

    if !detected {
        spiced_instance.stop()?;
        let _ = std::fs::remove_file(&temp_path);
        return Err(anyhow::anyhow!(
            "Benchmark {config_name}: marker '{MARKER_EVENT_TYPE}' not detected within timeout. \
             Ensure the stream was prepared with `prepare-ingestion-stream --topic {}`.",
            args.topic
        ));
    }

    println!("Marker detected — ingestion complete.");
    println!("  Duration: {:.2}s", ingestion_duration.as_secs_f64());

    #[expect(clippy::cast_precision_loss)]
    let throughput = if ingestion_duration.as_secs_f64() > 0.0 {
        args.rows as f64 / ingestion_duration.as_secs_f64()
    } else {
        0.0
    };
    println!("  Throughput: {throughput:.1} records/s");

    // Collect health monitor results
    if let Some(monitor) = health_monitor {
        let report = monitor.stop().await?;
        let mut total_failures: u64 = 0;
        let mut max_latency_ms: f64 = 0.0;
        for stats in report.endpoints.values() {
            total_failures += stats.failure_count;
            let latency_ms = stats.max_latency.as_secs_f64() * 1000.0;
            if latency_ms > max_latency_ms {
                max_latency_ms = latency_ms;
            }
        }
        crate::metrics::LIVENESS_FAILURES.record(total_failures, &[]);
        crate::metrics::LIVENESS_MAX_LATENCY.record(max_latency_ms, &[]);
        if let Some(msg) = report.failure_message() {
            println!("Health liveness issues: {msg}");
        } else {
            println!("Health liveness: OK (max latency: {max_latency_ms:.1}ms)");
        }
    }

    // Phase 6: Record and emit telemetry
    println!("\n[5/5] Emitting telemetry");
    crate::metrics::INGESTION_DURATION.record(
        ingestion_duration.as_millis().try_into().unwrap_or(u64::MAX),
        &[],
    );
    crate::metrics::RECORDS_PER_SECOND.record(throughput, &[]);
    crate::metrics::RECORD_COUNT.record(args.rows, &[]);

    telemetry.emit().await?;

    spiced_instance.stop()?;
    let _ = std::fs::remove_file(&temp_path);

    println!("\n{}\nBenchmark Result\n{}", "=".repeat(60), "=".repeat(60));
    println!("  Config:    {config_name}");
    println!("  Run ID:    {run_id}");
    println!("  Mode:      {}", args.stream_mode);
    println!("  Topic:     {}", args.topic);
    println!("  Duration:  {:.2}s", ingestion_duration.as_secs_f64());
    println!("  Records:   {}", args.rows);
    println!("  Throughput: {throughput:.1} records/s");

    Ok(())
}

/// Transform a spicepod for a streaming ingestion benchmark run.
///
/// - Replaces the `from` field with the run-specific source identifier
/// - Sets `kafka_bootstrap_servers` and a unique `kafka_consumer_group_id`
///   so Spice always replays from offset 0
/// - Adds engine-specific file paths for file-mode accelerators
/// - Adds `records_consumed_total` metric
fn transform_ingestion_spicepod(
    mut spicepod: spicepod::spec::SpicepodDefinition,
    run_id: &str,
    source_from: &str,
    kafka_bootstrap_servers: &str,
) -> spicepod::spec::SpicepodDefinition {
    #[expect(clippy::expect_used)]
    std::fs::create_dir_all(format!("/tmp/benchmarks/{run_id}"))
        .expect("Failed to create benchmark directory");

    for dataset in &mut spicepod.datasets {
        if let ComponentOrReference::Component(d) = dataset {
            if d.from.starts_with("debezium:") || d.from.starts_with("kafka:") {
                d.from = source_from.to_string();
            }

            let params = d.params.get_or_insert_with(Params::default);
            params.data.insert(
                "kafka_bootstrap_servers".to_string(),
                ParamValue::String(kafka_bootstrap_servers.to_string()),
            );
            // Unique consumer group per run → Spice always starts from offset 0.
            params.data.insert(
                "kafka_consumer_group_id".to_string(),
                ParamValue::String(format!("spice-ingestion-{run_id}")),
            );

            if let Some(acc) = &mut d.acceleration {
                if !matches!(acc.mode, Mode::Memory) {
                    let acc_params = acc.params.get_or_insert_with(Params::default);
                    match acc.engine.as_deref() {
                        Some("duckdb") => {
                            acc_params.data.insert(
                                "duckdb_file".to_string(),
                                ParamValue::String(format!(
                                    "/tmp/benchmarks/{run_id}/{}.db",
                                    d.name
                                )),
                            );
                        }
                        Some("sqlite") => {
                            acc_params.data.insert(
                                "sqlite_file".to_string(),
                                ParamValue::String(format!(
                                    "/tmp/benchmarks/{run_id}/{}.sqlite",
                                    d.name
                                )),
                            );
                        }
                        Some("cayenne") => {
                            acc_params.data.insert(
                                "cayenne_file_path".to_string(),
                                ParamValue::String(format!(
                                    "/tmp/benchmarks/{run_id}/{}",
                                    d.name
                                )),
                            );
                        }
                        _ => {}
                    }
                }
            }

            d.metrics = Some(Metrics {
                metrics: vec![Metric {
                    name: "records_consumed_total".to_string(),
                    enabled: true,
                }],
            });
        }
    }

    spicepod
}

/// Poll Spice until the marker row count reaches `expected_count`.
async fn poll_for_marker(
    spiced: &SpicedInstance,
    query: &str,
    expected_count: i64,
    timeout: Duration,
) -> Result<bool> {
    let start = Instant::now();
    let poll_interval = Duration::from_millis(500);
    let spice_client = spiced.spice_client(None, false).await?;

    loop {
        if start.elapsed() > timeout {
            println!("Timeout waiting for marker (query: {query})");
            return Ok(false);
        }

        if let Ok(stream) = spice_client.sql(query).await
            && let Ok(batches) = stream.try_collect::<Vec<RecordBatch>>().await
        {
            for batch in &batches {
                if batch.num_rows() > 0
                    && let Some(count) = get_count_from_batch(batch)
                    && count >= expected_count
                {
                    return Ok(true);
                }
            }
        }

        tokio::time::sleep(poll_interval).await;
    }
}
