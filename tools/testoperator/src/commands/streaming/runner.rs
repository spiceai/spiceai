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

//! Unified `DynamoDB` Streams performance benchmark.
//!
//! Self-contained benchmark that creates tables, inserts TPC-H data, starts Spice,
//! measures ingestion throughput and stream lag, and optionally verifies data
//! correctness with TPC-H queries.
//!
//! ## Flow
//! 1. Create `DynamoDB` tables with unique prefix
//! 2. Generate TPC-H data and insert initial records (for schema inference)
//! 3. Transform and write spicepod, start Spice with metrics
//! 4. Insert remaining data and marker records
//! 5. Poll for markers to measure stream lag and ingestion duration
//! 6. Delete markers and confirm deletions
//! 7. Fetch `DynamoDB` metrics from Prometheus
//! 8. Optionally restart Spice and run TPC-H verification
//! 9. Emit telemetry and report results

use std::collections::HashMap;
use std::time::{Duration, Instant};

use arrow::array::RecordBatch;
use futures::future::try_join_all;
use test_framework::anyhow::{self, Result};
use test_framework::git;
use test_framework::opentelemetry::KeyValue;
use test_framework::opentelemetry_sdk::Resource;
use test_framework::spiced::{SpicedInstance, StartRequest};

use super::datasets::DatasetType;
use super::query_liveness::QueryLivenessMonitor;
use super::sources::{DynamoDbConfig, DynamoDbStreamsSource, transform_spicepod};
use super::traits::StreamingSource;
use super::utils;
use super::utils::{
    DatasetInfo, generate_run_id, load_spicepod_definition, poll_for_all_markers, skip_rows,
    wait_for_all_marker_deletions, write_temp_spicepod,
};
use crate::args::StreamingDynamodbArgs;
use crate::commands::create_telemetry_with_resource;
use crate::health::HealthMonitor;

/// Run the unified `DynamoDB` streaming performance benchmark.
///
/// This is a self-contained benchmark that handles the full lifecycle:
/// table creation, data insertion, Spice startup, ingestion measurement,
/// optional verification, and telemetry emission.
#[expect(clippy::too_many_lines)]
pub async fn run_benchmark(args: &StreamingDynamodbArgs) -> Result<()> {
    let run_id = generate_run_id();
    let datasets = args.queryset.get_datasets();

    let config_name = args
        .common
        .spicepod_path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("unknown")
        .to_string();

    println!("Starting DynamoDB streaming performance benchmark");
    println!("Run ID: {run_id}");
    println!("Config: {config_name}");
    println!("Query set: {}", args.queryset);
    println!("Scale factor: {}", args.scale_factor);
    println!("Initial records: {}", args.initial_records);
    println!(
        "Datasets: {}",
        datasets
            .iter()
            .map(|d| d.dataset_type().to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );

    // Phase 1: Create DynamoDB source and prepare
    println!("\nPhase 1: Preparing DynamoDB source");
    let config = DynamoDbConfig::from_env()?;
    let mut source = DynamoDbStreamsSource::new(config);
    source.set_table_prefix(run_id.clone());
    source.set_scale_factor(args.scale_factor);
    source.prepare().await?;

    // Phase 2: Create tables in parallel
    println!("\nPhase 2: Creating tables for all datasets (parallel)");
    let table_creation_futures: Vec<_> = datasets
        .iter()
        .map(|dataset| {
            let source = &source;
            let dataset_type = dataset.dataset_type();
            async move { source.create_table(dataset_type).await }
        })
        .collect();

    try_join_all(table_creation_futures).await?;
    println!("All tables created");

    // Brief sleep for table propagation
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Phase 3: Generate TPC-H data for all datasets
    println!("\nPhase 3: Generating data for all datasets");
    let mut dataset_infos = Vec::new();

    for dataset in datasets {
        let dataset_type = dataset.dataset_type();
        println!("  Generating data for {dataset_type}");
        let records = dataset.generate(args.scale_factor)?;
        let record_count: usize = records.iter().map(RecordBatch::num_rows).sum();
        println!("  Generated {record_count} records for {dataset_type}");

        let marker = dataset.marker_record()?;
        dataset_infos.push(DatasetInfo {
            dataset,
            marker,
            record_count,
            generated_data: records,
        });
    }

    // Phase 4: Insert initial records per dataset (for schema inference)
    println!(
        "\nPhase 4: Inserting {} initial records per dataset (for schema inference)",
        args.initial_records
    );
    for info in &dataset_infos {
        let table_name = source.get_table_name(info.dataset.table_name());
        let mut rows_inserted = 0;
        let rows_to_insert = args.initial_records;

        for batch in &info.generated_data {
            if rows_inserted >= rows_to_insert {
                break;
            }
            let remaining = rows_to_insert - rows_inserted;
            let take = remaining.min(batch.num_rows());
            if take > 0 {
                let slice = batch.slice(0, take);
                source.insert(&table_name, &[slice]).await?;
                rows_inserted += take;
            }
        }
        println!(
            "  Inserted {rows_inserted} initial records for {}",
            info.dataset.dataset_type()
        );
    }

    // Phase 5: Load and transform spicepod
    println!("\nPhase 5: Loading and transforming spicepod");
    let spicepod_def = load_spicepod_definition(&args.common.spicepod_path)?;
    let transformed = transform_spicepod(spicepod_def, &run_id, &config_name, true);

    let temp_path = write_temp_spicepod(&transformed, &run_id, &config_name, "benchmark")?;

    // Phase 6: Start Spice with metrics enabled
    println!("\nPhase 6: Starting Spice");
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

    // Get spiced version for telemetry
    let spiced_version = spiced_instance.version().to_string();

    // Phase 7: Build telemetry resource
    println!("\nPhase 7: Building telemetry resource");
    let testoperator_commit_sha = git::get_commit_sha();
    let spiced_commit_sha =
        std::env::var("SPICED_COMMIT").unwrap_or_else(|_| "unknown".to_string());
    let branch_name = git::get_branch_name();

    let benchmark_resource = Resource::builder_empty()
        .with_attributes(vec![
            KeyValue::new("service.name", "testoperator"),
            KeyValue::new("type", "streaming_benchmark"),
            KeyValue::new("config_name", config_name.clone()),
            KeyValue::new("run_id", run_id.clone()),
            KeyValue::new("queryset", args.queryset.to_string()),
            KeyValue::new("scale_factor", args.scale_factor.to_string()),
            KeyValue::new("testoperator_commit_sha", testoperator_commit_sha),
            KeyValue::new("spiced_commit_sha", spiced_commit_sha),
            KeyValue::new("spiced_version", spiced_version),
            KeyValue::new("branch_name", branch_name),
        ])
        .build();

    let telemetry = create_telemetry_with_resource(&args.common, benchmark_resource);

    // Phase 8: Start liveness monitors if enabled
    let health_monitor = if args.enable_liveness {
        println!("Starting health liveness monitor");
        Some(HealthMonitor::spawn()?)
    } else {
        None
    };

    let query_liveness_monitor = if args.enable_query_liveness {
        let datasets = args.queryset.get_datasets();
        let poll_interval = Duration::from_millis(args.query_liveness_interval_ms);
        println!(
            "Starting query liveness monitor (interval: {}ms)",
            args.query_liveness_interval_ms
        );
        Some(QueryLivenessMonitor::spawn(&spiced_instance, &datasets, poll_interval).await?)
    } else {
        None
    };

    // Phase 9: Insert remaining data
    println!("\nPhase 9: Inserting remaining data");
    for info in &dataset_infos {
        let dataset_type = info.dataset.dataset_type();
        let table_name = source.get_table_name(info.dataset.table_name());

        // Skip the initial_records already inserted
        let remaining_data = skip_rows(&info.generated_data, args.initial_records);
        let remaining_count: usize = remaining_data.iter().map(RecordBatch::num_rows).sum();

        if remaining_count > 0 {
            println!("  Inserting {remaining_count} remaining records for {dataset_type}");
            source.insert(&table_name, &remaining_data).await?;
        } else {
            println!("  No remaining records to insert for {dataset_type}");
        }
    }

    // Phase 10: Insert markers for each dataset
    println!("\nPhase 10: Inserting marker records");
    for info in &dataset_infos {
        let table_name = source.get_table_name(info.dataset.table_name());
        source
            .insert(&table_name, std::slice::from_ref(&info.marker))
            .await?;
    }

    let marker_insertion_time = Instant::now();

    // Phase 11: Poll for markers
    println!("\nPhase 11: Polling for marker detection");
    let marker_queries: HashMap<DatasetType, String> = dataset_infos
        .iter()
        .map(|info| {
            (
                info.dataset.dataset_type(),
                info.dataset.marker_detection_query(),
            )
        })
        .collect();

    let marker_counts: HashMap<DatasetType, usize> = dataset_infos
        .iter()
        .map(|info| (info.dataset.dataset_type(), info.dataset.marker_count()))
        .collect();

    let timeout = Duration::from_secs(args.common.ready_wait);
    let all_markers_detected =
        poll_for_all_markers(&spiced_instance, &marker_queries, &marker_counts, timeout).await?;

    let stream_lag = marker_insertion_time.elapsed();
    let ingestion_duration = spice_ready_time.elapsed();

    if !all_markers_detected {
        spiced_instance.stop()?;
        let _ = std::fs::remove_file(&temp_path);
        return Err(anyhow::anyhow!(
            "Benchmark {config_name}: markers not detected within timeout"
        ));
    }

    println!("All markers detected");
    println!("  Stream lag: {:.2}s", stream_lag.as_secs_f64());
    println!(
        "  Ingestion duration: {:.2}s",
        ingestion_duration.as_secs_f64()
    );

    // Phase 12: Delete markers and wait for deletions
    println!("\nPhase 12: Deleting markers");
    for info in &dataset_infos {
        source.delete_marker(info.dataset.dataset_type()).await?;
    }

    wait_for_all_marker_deletions(&spiced_instance, &marker_queries, Duration::from_secs(30))
        .await?;

    // Phase 13: Stop liveness monitors and record metrics
    println!("\nPhase 13: Collecting monitor metrics");
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

    if let Some(monitor) = query_liveness_monitor {
        let report = monitor.stop().await?;
        report.print_summary();

        let aggregate = report.aggregate_stats();
        crate::metrics::QUERY_LIVENESS_TOTAL.record(aggregate.total_queries, &[]);
        crate::metrics::QUERY_LIVENESS_FAILURES.record(aggregate.failed_queries, &[]);
        crate::metrics::QUERY_LIVENESS_SUCCESS_RATE.record(aggregate.success_rate(), &[]);
        crate::metrics::QUERY_LIVENESS_AVG_LATENCY
            .record(aggregate.avg_latency().as_secs_f64() * 1000.0, &[]);
        crate::metrics::QUERY_LIVENESS_MAX_LATENCY
            .record(aggregate.max_latency.as_secs_f64() * 1000.0, &[]);
        crate::metrics::QUERY_LIVENESS_P90_LATENCY
            .record(aggregate.p90().as_secs_f64() * 1000.0, &[]);
        crate::metrics::QUERY_LIVENESS_P95_LATENCY
            .record(aggregate.p95().as_secs_f64() * 1000.0, &[]);
        crate::metrics::QUERY_LIVENESS_P99_LATENCY
            .record(aggregate.p99().as_secs_f64() * 1000.0, &[]);
    }

    // Phase 14: Fetch DynamoDB metrics from Prometheus
    println!("\nPhase 14: Fetching DynamoDB metrics");
    let dynamodb_metrics = match utils::get_dynamodb_metrics().await {
        Ok(metrics) => {
            println!(
                "DynamoDB records consumed: {}",
                metrics.records_consumed_total
            );
            if metrics.errors_transient_total > 0 {
                println!(
                    "DynamoDB transient errors: {}",
                    metrics.errors_transient_total
                );
            }
            metrics
        }
        Err(e) => {
            println!("Warning: Failed to fetch DynamoDB metrics: {e}");
            utils::DynamoDbMetrics::default()
        }
    };

    // Phase 15: Record streaming metrics
    let record_count = dynamodb_metrics.records_consumed_total;

    #[expect(clippy::cast_precision_loss)]
    let throughput = if ingestion_duration.as_secs_f64() > 0.0 && record_count > 0 {
        record_count as f64 / ingestion_duration.as_secs_f64()
    } else {
        0.0
    };

    crate::metrics::STREAM_LAG.record(stream_lag.as_millis().try_into().unwrap_or(u64::MAX), &[]);
    crate::metrics::INGESTION_DURATION.record(
        ingestion_duration
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX),
        &[],
    );
    crate::metrics::RECORDS_PER_SECOND.record(throughput, &[]);
    crate::metrics::RECORD_COUNT.record(record_count, &[]);
    crate::metrics::DYNAMODB_TRANSIENT_ERRORS.record(dynamodb_metrics.errors_transient_total, &[]);

    // Phase 16: Emit telemetry
    telemetry.emit().await?;

    // Phase 17: Stop Spice and cleanup
    spiced_instance.stop()?;
    let _ = std::fs::remove_file(&temp_path);

    // Report results
    println!("\n{}\nBenchmark Result\n{}", "=".repeat(60), "=".repeat(60));
    println!("  Config: {config_name}");
    println!("  Run ID: {run_id}");
    println!(
        "  Ingestion Duration: {:.2}s",
        ingestion_duration.as_secs_f64()
    );
    println!("  Stream Lag: {:.2}s", stream_lag.as_secs_f64());
    println!("  Records Consumed: {record_count}");
    println!("  Throughput: {throughput:.1} records/s");
    if dynamodb_metrics.errors_transient_total > 0 {
        println!(
            "  Transient Errors: {}",
            dynamodb_metrics.errors_transient_total
        );
    }

    Ok(())
}
