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

//! `DynamoDB` Streams ingestion benchmark runner.
//!
//! This module contains the DynamoDB-specific benchmark orchestration logic.
//! It uses snapshot-based checkpoint capture to ensure fair benchmarking.
//!
//! ## Workflow
//!
//! The streaming benchmark uses a two-phase approach:
//!
//! 1. **Preparation phase** (`streaming-dynamodb-dispatch` command):
//!    - Creates `DynamoDB` tables
//!    - Inserts data and captures snapshots for each config
//!    - Tables and snapshots identified by a shared `run_id`
//!
//! 2. **Benchmark phase** (`streaming-dynamodb` command, this module):
//!    - Starts Spice from snapshot (using same `run_id`)
//!    - Inserts markers, waits for ingestion
//!    - Reports results with telemetry

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use test_framework::anyhow::{self, Result};
use test_framework::git;
use test_framework::metrics::QueryStatus;
use test_framework::opentelemetry::KeyValue;
use test_framework::opentelemetry_sdk::Resource;
use test_framework::spiced::{SpicedInstance, StartRequest};

use super::datasets::DatasetType;
use super::query_liveness::QueryLivenessMonitor;
use super::sources::{DynamoDbConfig, DynamoDbStreamsSource};
use super::traits::{DynamoDBStreamingSource, SnapshotConfig, StreamingSource};
use super::utils::{
    get_dynamodb_metrics, load_spicepod_definition, poll_for_all_markers,
    wait_for_all_marker_deletions, write_temp_spicepod,
};
use super::verification;
use crate::args::StreamingDynamodbTestArgs;
use crate::commands::create_telemetry_with_resource;
use crate::health::HealthMonitor;

/// Run the `DynamoDB` streaming ingestion benchmark from a snapshot.
///
/// This requires that `dispatch-dynamodb` has already:
/// 1. Created tables and inserted data
/// 2. Captured checkpoint snapshots for all configs
///
/// The benchmark:
/// 1. Starts Spice from the snapshot
/// 2. Inserts marker records
/// 3. Waits for markers to be ingested
/// 4. Reports results with telemetry
pub async fn run_dynamodb(args: &StreamingDynamodbTestArgs) -> Result<()> {
    let run_id = &args.run_id;
    let spicepod_path = &args.common.spicepod_path;
    let datasets = args.queryset.get_datasets();

    let config_name = spicepod_path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("unknown")
        .to_string();

    println!("Starting DynamoDB streaming ingestion benchmark");
    println!("Query set: {}", args.queryset);
    println!("Config: {config_name}");
    println!("Run ID: {run_id}");
    println!(
        "Datasets: {}",
        datasets
            .iter()
            .map(|d| d.dataset_type().to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );

    // Create source and set table prefix (for marker operations)
    let config = DynamoDbConfig::from_env()?;
    let mut source = DynamoDbStreamsSource::new(config);
    source.set_table_prefix(run_id.clone());
    source.set_scale_factor(args.scale_factor);

    let snapshot_config = build_snapshot_config().ok_or_else(|| {
        anyhow::anyhow!("DynamoDB benchmarks require SNAPSHOT_S3_LOCATION environment variable")
    })?;

    // Prepare source (connects to DynamoDB)
    source.prepare().await?;

    let source: Arc<dyn DynamoDBStreamingSource> = Arc::from(source);

    // Generate markers for each dataset
    let mut dataset_markers = Vec::new();
    for dataset in datasets {
        let marker = dataset.marker_record()?;
        dataset_markers.push((dataset.dataset_type(), marker));
    }

    // Load and transform spicepod
    let spicepod_def = load_spicepod_definition(spicepod_path)?;
    let transformed =
        source.prepare_benchmark_spicepod(spicepod_def, run_id, &config_name, &snapshot_config);

    // Write transformed spicepod to temp file
    let temp_path = write_temp_spicepod(&transformed, run_id, &config_name, "benchmark")?;
    println!("Spicepod written to temp file: {}", temp_path.display());

    // Start Spice with metrics enabled for record count tracking
    let mut start_request = StartRequest::new(args.common.spiced_path_buf(), transformed)?
        .with_additional_args(vec!["--metrics".to_string(), "0.0.0.0:9090".to_string()]);

    if let Some(ref data_dir) = args.common.data_dir {
        start_request = start_request.with_data_dir(data_dir.clone());
    }

    let mut spiced_instance = SpicedInstance::start(start_request).await?;

    spiced_instance
        .wait_for_ready(Duration::from_secs(args.common.ready_wait))
        .await?;

    // Track when Spice became ready (for CDC catchup timing)
    let spice_ready_time = Instant::now();

    // Get spiced version for telemetry (now that instance is started)
    let spiced_version = spiced_instance.version().to_string();

    // Build telemetry resource with benchmark attributes
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

    // Start liveness monitors if enabled
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

    // Insert markers
    for (dataset_type, marker) in &dataset_markers {
        let table_name = source.get_table_name(dataset_type.table_name());
        source
            .insert(&table_name, std::slice::from_ref(marker))
            .await?;
    }

    let marker_insertion_time = Instant::now();

    // Wait for markers
    let marker_queries: HashMap<DatasetType, String> = dataset_markers
        .iter()
        .map(|(dt, _)| (*dt, dt.create_dataset().marker_detection_query()))
        .collect();

    let marker_counts: HashMap<DatasetType, usize> = dataset_markers
        .iter()
        .map(|(dt, _)| (*dt, dt.create_dataset().marker_count()))
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

    // Delete markers
    for (dataset_type, _) in &dataset_markers {
        source.delete_marker(*dataset_type).await?;
    }

    // Wait for deletions
    wait_for_all_marker_deletions(&spiced_instance, &marker_queries, Duration::from_secs(30))
        .await?;

    // Stop liveness monitors and record metrics
    if let Some(monitor) = health_monitor {
        let report = monitor.stop().await?;

        // Record health liveness metrics
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

        // Record query liveness metrics
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

    // Fetch DynamoDB metrics from Spice's Prometheus endpoint before restart
    let dynamodb_metrics = match get_dynamodb_metrics().await {
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
            super::utils::DynamoDbMetrics::default()
        }
    };

    // Restart Spice before verification to ensure clean state with all data loaded
    let spiced_instance = if args.verify {
        println!("Restarting Spice before verification...");
        spiced_instance.stop()?;

        // Reload spicepod from temp file
        let spicepod_for_restart = load_spicepod_definition(&temp_path)?;
        let mut start_request =
            StartRequest::new(args.common.spiced_path_buf(), spicepod_for_restart)?;
        if let Some(ref data_dir) = args.common.data_dir {
            start_request = start_request.with_data_dir(data_dir.clone());
        }

        let mut new_instance = SpicedInstance::start(start_request).await?;
        new_instance
            .wait_for_ready(Duration::from_secs(args.common.ready_wait))
            .await?;

        println!("Spice restarted successfully");
        new_instance
    } else {
        spiced_instance
    };

    // Run verification if requested
    let (mut spiced_instance, verification_passed) = if args.verify {
        let verification_result =
            verification::run_verification(spiced_instance, 1, args.scale_factor).await?;

        // Emit per-query metrics
        for query in &verification_result.metrics.metrics {
            let query_name = &query.query_name;
            let row_count = verification_result.row_counts.get(query_name).unwrap_or(&0);
            let attributes = vec![KeyValue::new("query_name", query_name.to_string())];

            let status: u64 = u64::from(matches!(&query.query_status, QueryStatus::Passed));

            crate::metrics::QUERY_STATUS.record(status, &attributes);
            crate::metrics::MEDIAN_DURATION.record(query.median_duration_ms, &attributes);
            crate::metrics::MIN_DURATION.record(query.min_duration_ms, &attributes);
            crate::metrics::MAX_DURATION.record(query.max_duration_ms, &attributes);
            crate::metrics::ITERATIONS
                .record(query.iterations.try_into().unwrap_or(u64::MAX), &attributes);
            crate::metrics::P90_DURATION.record(query.percentile_90_duration_ms, &attributes);
            crate::metrics::P95_DURATION.record(query.percentile_95_duration_ms, &attributes);
            crate::metrics::P99_DURATION.record(query.percentile_99_duration_ms, &attributes);
            crate::metrics::ROW_COUNT
                .record((*row_count).try_into().unwrap_or(u64::MAX), &attributes);
        }

        (
            verification_result.spiced_instance,
            verification_result.all_passed,
        )
    } else {
        (spiced_instance, true)
    };

    let record_count = dynamodb_metrics.records_consumed_total;

    // Calculate throughput
    #[expect(clippy::cast_precision_loss)]
    let throughput = if ingestion_duration.as_secs_f64() > 0.0 && record_count > 0 {
        record_count as f64 / ingestion_duration.as_secs_f64()
    } else {
        0.0
    };

    // Stop Spice
    spiced_instance.stop()?;

    // Cleanup temp file
    let _ = std::fs::remove_file(&temp_path);

    // Record streaming metrics
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

    // Emit telemetry
    telemetry.emit().await?;

    // Report result
    println!("\nBenchmark Result:");
    println!("  Config: {config_name}");
    println!(
        "  Ingestion Duration: {:.2}s",
        ingestion_duration.as_secs_f64()
    );
    println!("  Stream Lag: {:.2}s", stream_lag.as_secs_f64());
    println!(
        "  Verification: {}",
        if verification_passed { "PASS" } else { "FAIL" }
    );

    Ok(())
}

/// Build snapshot configuration from environment variables.
///
/// Environment variables:
/// - `SNAPSHOT_S3_LOCATION`: S3 location for snapshots (e.g., `s3://bucket/snapshots/`)
/// - `SNAPSHOT_S3_ACCESS_KEY_ID`: S3 access key ID (optional)
/// - `SNAPSHOT_S3_SECRET_ACCESS_KEY`: S3 secret access key (optional)
/// - `SNAPSHOT_S3_REGION`: S3 region (optional)
pub fn build_snapshot_config() -> Option<SnapshotConfig> {
    let location = std::env::var("SNAPSHOT_S3_LOCATION").ok()?;

    Some(SnapshotConfig {
        location,
        access_key_id: std::env::var("SNAPSHOT_S3_ACCESS_KEY_ID").ok(),
        secret_access_key: std::env::var("SNAPSHOT_S3_SECRET_ACCESS_KEY").ok(),
        region: std::env::var("SNAPSHOT_S3_REGION").ok(),
    })
}
