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

//! Streaming ingestion benchmark command.
//!
//! This benchmark measures the time it takes for Spice to ingest data from
//! streaming sources like `DynamoDB` Streams, Kafka, etc.
//!
//! The benchmark is built around two abstractions:
//! - [`datasets::DatasetType`]: What table to generate (e.g., TPCH lineitem)
//! - [`sources::SourceType`]: Where data is sent (e.g., `DynamoDB` Streams)
//!
//! Multiple datasets can be specified for multi-table benchmarks.
//!
//! ## Workflow
//!
//! 1. Prepare streaming source (start containers, etc.)
//! 2. For each dataset: create table, generate data, insert data, insert marker
//! 3. Start Spiced with streaming connector
//! 4. Poll until ALL markers are detected in accelerated tables
//! 5. Delete ALL marker records (for clean validation data)
//! 6. Report ingestion duration

pub mod datasets;
pub mod mutations;
pub mod query_liveness;
pub mod querysets;
pub mod run_config;
pub mod sources;
mod traits;
pub mod verification;

use std::collections::HashMap;
use std::time::{Duration, Instant};

use arrow::array::{Int64Array, RecordBatch, UInt64Array};
use futures::TryStreamExt;
use test_framework::anyhow::{self, Result};
use test_framework::git;
use test_framework::opentelemetry::KeyValue;
use test_framework::opentelemetry_sdk::Resource;
use test_framework::spiced::{SpicedInstance, StartRequest};
use test_framework::spicepod_utils::load_spicepod;

pub use datasets::DatasetType;
pub use sources::SourceType;
pub use traits::{StreamingDataset, StreamingSource};

use crate::args::StreamingTestArgs;
use crate::health::{HealthCheckReport, HealthMonitor};

/// Information about a dataset being benchmarked.
struct DatasetInfo {
    dataset: Box<dyn StreamingDataset>,
    marker: RecordBatch,
    record_count: usize,
    /// Original generated data (for mutation testing).
    generated_data: Vec<RecordBatch>,
}

/// Run the streaming ingestion benchmark.
pub async fn run(args: &StreamingTestArgs) -> Result<()> {
    let datasets = args.queryset.get_datasets();

    println!("Starting streaming ingestion benchmark");
    println!("Source: {}", args.source);
    println!("Query set: {}", args.queryset);
    println!(
        "Datasets: {}",
        datasets
            .iter()
            .map(|d| d.dataset_type().to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );
    println!("Scale factor: {}", args.scale_factor);

    // Create run config for isolated test runs
    let mut run_config = run_config::RunConfig::new();

    // Collect table names for spicepod rewriting
    let table_names: Vec<&str> = datasets.iter().map(|d| d.table_name()).collect();

    // Prepare the spicepod with rewritten table names
    let spicepod_path = run_config.prepare_spicepod(&args.common.spicepod_path, &table_names)?;

    // Create source and set table prefix
    let mut source = create_source(args)?;
    source.set_table_prefix(run_config.run_id().to_string());

    // Phase 1: Prepare source
    println!("Phase 1: Preparing streaming source");
    source.prepare().await?;

    // Phase 2: Create tables for all datasets
    println!("Phase 2: Creating tables for all datasets");
    for dataset in &datasets {
        source.create_table(dataset.dataset_type()).await?;
    }

    // Small delay to ensure tables are ready
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Phase 3: Generate data for all datasets
    println!("Phase 3: Generating data for all datasets");
    let mut dataset_infos = Vec::new();

    for dataset in datasets {
        let dataset_type = dataset.dataset_type();

        println!("Generating data for {dataset_type}");
        let records = dataset.generate(args.scale_factor)?;
        let record_count: usize = records.iter().map(RecordBatch::num_rows).sum();
        println!("Generated {record_count} records for {dataset_type}");

        let marker = dataset.marker_record()?;
        dataset_infos.push(DatasetInfo {
            dataset,
            marker,
            record_count,
            generated_data: records,
        });
    }

    // Phase 4: Insert data (with or without mutations)
    let mut total_insert_duration = Duration::ZERO;
    let mutation_summary = if args.enable_mutations {
        // Phase 4a: Execute mutation sequences (INSERT mutated → UPDATEs → final UPDATE with TPC-H)
        println!("Phase 4: Executing mutation sequences for CDC testing");
        println!(
            "  Seed: {}, Mutations per row: {}, Max rows per dataset: {}",
            args.mutation_seed, args.mutations_per_row, args.max_mutation_rows
        );

        let config = mutations::MutationConfig {
            seed: args.mutation_seed,
            mutations_per_row: args.mutations_per_row,
            max_rows_per_dataset: if args.max_mutation_rows == 0 {
                None
            } else {
                Some(args.max_mutation_rows)
            },
        };

        // Collect datasets and original data for mutation execution
        let datasets_for_mutation: Vec<Box<dyn StreamingDataset>> = dataset_infos
            .iter()
            .map(|info| info.dataset.dataset_type().create_dataset())
            .collect();
        let original_data: Vec<(DatasetType, Vec<RecordBatch>)> = dataset_infos
            .iter()
            .map(|info| (info.dataset.dataset_type(), info.generated_data.clone()))
            .collect();

        let insert_start = Instant::now();
        let summary = mutations::execute_mutation_sequences(
            source.as_ref(),
            &datasets_for_mutation,
            &original_data,
            config,
        )
        .await?;
        total_insert_duration = insert_start.elapsed();
        summary.print();
        Some(summary)
    } else {
        // Phase 4b: Direct data insertion (no mutations)
        println!("Phase 4: Inserting data for all datasets");
        for info in &dataset_infos {
            let dataset_type = info.dataset.dataset_type();
            let table_name = source.get_table_name(info.dataset.table_name());
            println!("Inserting data for {dataset_type} into {table_name}");
            let insert_start = Instant::now();
            source.insert(&table_name, &info.generated_data).await?;
            total_insert_duration += insert_start.elapsed();
        }
        None
    };

    println!("Data insertion completed in {total_insert_duration:?}");

    // Phase 5: Insert markers for all datasets
    println!("Phase 5: Inserting marker records for all datasets");
    for info in &dataset_infos {
        let table_name = source.get_table_name(info.dataset.table_name());
        source
            .insert(&table_name, std::slice::from_ref(&info.marker))
            .await?;
    }

    // Small delay to ensure all writes are committed to the stream
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Phase 6: Start Spiced
    println!("Phase 6: Starting Spiced with streaming connector");
    println!("Using spicepod: {}", spicepod_path.display());

    let spicepod_def = load_spicepod(spicepod_path.clone())?;
    let mut start_request = StartRequest::new(args.common.spiced_path_buf(), spicepod_def)?;

    if let Some(ref data_dir) = args.common.data_dir {
        start_request = start_request.with_data_dir(data_dir.clone());
    }

    let mut spiced_instance = SpicedInstance::start(start_request).await?;

    // Wait for Spiced to be ready
    spiced_instance
        .wait_for_ready(Duration::from_secs(args.common.ready_wait))
        .await?;

    let spiced_version = spiced_instance.version().to_string();

    // Phase 6a: Start health monitoring (optional)
    let health_monitor = if args.enable_liveness {
        println!("Starting health monitor for liveness checks");
        Some(HealthMonitor::spawn()?)
    } else {
        None
    };

    // Phase 6b: Start query liveness monitoring (optional)
    let datasets_for_liveness: Vec<Box<dyn StreamingDataset>> = dataset_infos
        .iter()
        .map(|info| info.dataset.dataset_type().create_dataset())
        .collect();
    let query_liveness_monitor = if args.enable_query_liveness {
        println!("Starting query liveness monitor");
        let poll_interval = Duration::from_millis(args.query_liveness_interval_ms);
        Some(
            query_liveness::QueryLivenessMonitor::spawn(
                &spiced_instance,
                &datasets_for_liveness,
                poll_interval,
            )
            .await?,
        )
    } else {
        None
    };

    // Phase 7: Poll for ALL markers
    println!("Phase 7: Polling for all marker records");
    let ingestion_start = Instant::now();
    let timeout = Duration::from_secs(args.ingestion_timeout);

    let marker_queries: HashMap<DatasetType, String> = dataset_infos
        .iter()
        .map(|info| {
            (
                info.dataset.dataset_type(),
                info.dataset.marker_detection_query(),
            )
        })
        .collect();

    let all_markers_detected =
        poll_for_all_markers(&spiced_instance, &marker_queries, timeout).await?;
    let ingestion_duration = ingestion_start.elapsed();

    if !all_markers_detected {
        spiced_instance.stop()?;
        source.cleanup().await?;
        return Err(anyhow::anyhow!(
            "Ingestion timeout: not all markers detected within {timeout:?}"
        ));
    }

    println!("All markers detected! Ingestion completed in {ingestion_duration:?}");

    // Phase 8: Delete all markers
    println!("Phase 8: Deleting all marker records");
    for info in &dataset_infos {
        source.delete_marker(info.dataset.dataset_type()).await?;
    }

    // Wait for deletes to propagate
    println!("Waiting for marker deletions to propagate...");
    wait_for_all_marker_deletions(&spiced_instance, &marker_queries, Duration::from_secs(30))
        .await?;

    // Phase 8a: Stop health monitoring and collect report
    let health_report = if let Some(monitor) = health_monitor {
        println!("Stopping health monitor and collecting report");
        Some(monitor.stop().await?)
    } else {
        None
    };

    // Phase 8a2: Stop query liveness monitoring and collect report
    let query_liveness_report = if let Some(monitor) = query_liveness_monitor {
        println!("Stopping query liveness monitor and collecting report");
        let report = monitor.stop().await?;
        report.print_summary();
        Some(report)
    } else {
        None
    };

    // Phase 8b: Verify TPCH queries (optional)
    let verification_passed = if args.verify {
        println!("Phase 8b: Verifying TPCH queries");
        let report = verification::verify_tpch_queries(&spiced_instance).await?;
        report.print_summary();
        report.all_passed()
    } else {
        true
    };

    // Phase 9: Report metrics
    println!("Phase 9: Reporting metrics");

    let total_record_count: usize = dataset_infos.iter().map(|info| info.record_count).sum();

    let spiced_commit_sha =
        std::env::var("SPICED_COMMIT").unwrap_or_else(|_| "unknown".to_string());
    let testoperator_commit_sha = git::get_commit_sha();
    let branch_name = git::get_branch_name();

    let datasets_str = dataset_infos
        .iter()
        .map(|info| info.dataset.dataset_type().to_string())
        .collect::<Vec<_>>()
        .join(",");

    let resource = Resource::builder_empty()
        .with_attributes(vec![
            KeyValue::new("service.name", "testoperator"),
            KeyValue::new("type", "streaming_ingestion"),
            KeyValue::new("source", args.source.to_string()),
            KeyValue::new("queryset", args.queryset.to_string()),
            KeyValue::new("datasets", datasets_str),
            KeyValue::new("spiced_version", spiced_version),
            KeyValue::new("testoperator_commit_sha", testoperator_commit_sha),
            KeyValue::new("spiced_commit_sha", spiced_commit_sha),
            KeyValue::new("branch_name", branch_name),
            KeyValue::new("scale_factor", args.scale_factor.to_string()),
        ])
        .build();

    let telemetry = super::create_telemetry_with_resource(&args.common, resource);

    // Record metrics
    let ingestion_ms: u64 = ingestion_duration
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX);
    let insert_ms: u64 = total_insert_duration
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX);
    let records_per_sec = if ingestion_duration.as_secs_f64() > 0.0 {
        total_record_count as f64 / ingestion_duration.as_secs_f64()
    } else {
        0.0
    };

    crate::metrics::INGESTION_DURATION.record(ingestion_ms, &[]);
    crate::metrics::DATA_INSERTION_DURATION.record(insert_ms, &[]);
    crate::metrics::RECORD_COUNT.record(total_record_count.try_into().unwrap_or(u64::MAX), &[]);
    crate::metrics::RECORDS_PER_SECOND.record(records_per_sec, &[]);

    // Record liveness metrics if enabled
    let (liveness_failures, liveness_max_latency_ms) =
        calculate_liveness_stats(health_report.as_ref());
    if args.enable_liveness {
        crate::metrics::LIVENESS_FAILURES.record(liveness_failures, &[]);
        crate::metrics::LIVENESS_MAX_LATENCY.record(liveness_max_latency_ms, &[]);
    }

    // Print summary
    println!("\n{}", "=".repeat(60));
    println!("Streaming Ingestion Benchmark Results");
    println!("{}", "=".repeat(60));
    println!("Source:                 {}", args.source);
    println!("Query Set:              {}", args.queryset);
    println!(
        "Datasets:               {}",
        dataset_infos
            .iter()
            .map(|info| info.dataset.dataset_type().to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );
    println!("Scale Factor:           {}", args.scale_factor);
    println!("Total Records:          {total_record_count}");
    println!("Data Insertion Time:    {total_insert_duration:?}");
    println!("Ingestion Duration:     {ingestion_duration:?}");
    println!("Throughput:             {records_per_sec:.2} records/sec");
    if args.enable_liveness {
        println!("Liveness Failures:      {liveness_failures}");
        println!("Liveness Max Latency:   {liveness_max_latency_ms:.2} ms");
        if let Some(ref report) = health_report {
            if let Some(msg) = report.failure_message() {
                println!("Liveness Issues:        {msg}");
            } else {
                println!("Liveness Status:        All checks passed");
            }
        }
    }
    if args.verify {
        if verification_passed {
            println!("Verification:           PASSED");
        } else {
            println!("Verification:           FAILED");
        }
    }
    if let Some(ref summary) = mutation_summary {
        println!(
            "Mutations:              {} rows, {} mutations ({} successful, {} failed)",
            summary.total_rows,
            summary.total_mutations,
            summary.successful_mutations,
            summary.failed_mutations
        );
    }
    if args.enable_query_liveness
        && let Some(ref report) = query_liveness_report
    {
        let aggregate = report.aggregate_stats();
        println!(
            "Query Liveness:         {} queries ({:.1}% success, avg {:.1}ms, max {:.1}ms)",
            aggregate.total_queries,
            aggregate.success_rate(),
            aggregate.avg_latency().as_secs_f64() * 1000.0,
            aggregate.max_latency.as_secs_f64() * 1000.0
        );
    }
    println!("{}", "=".repeat(60));

    telemetry.emit().await?;

    // Cleanup
    spiced_instance.stop()?;
    source.cleanup().await?;

    Ok(())
}

/// Poll until ALL markers are detected in their respective accelerated tables.
async fn poll_for_all_markers(
    spiced: &SpicedInstance,
    marker_queries: &HashMap<DatasetType, String>,
    timeout: Duration,
) -> Result<bool> {
    let start = Instant::now();
    let poll_interval = Duration::from_millis(500);

    let spice_client = spiced.spice_client(None, false).await?;

    let mut detected: HashMap<DatasetType, bool> =
        marker_queries.keys().map(|dt| (*dt, false)).collect();

    loop {
        if start.elapsed() > timeout {
            let missing: Vec<_> = detected
                .iter()
                .filter(|&(_, v)| !v)
                .map(|(k, _)| k.to_string())
                .collect();
            println!("Timeout waiting for markers: {missing:?}");
            return Ok(false);
        }

        // Check each marker that hasn't been detected yet
        for (dataset_type, query) in marker_queries {
            if detected[dataset_type] {
                continue;
            }

            if let Ok(stream) = spice_client.query(query).await
                && let Ok(batches) = stream.try_collect::<Vec<RecordBatch>>().await
            {
                for batch in &batches {
                    if batch.num_rows() > 0
                        && let Some(count) = get_count_from_batch(batch)
                        && count > 0
                    {
                        println!("Marker detected for {dataset_type}");
                        detected.insert(*dataset_type, true);
                        break;
                    }
                }
            }
        }

        // Check if all markers are detected
        if detected.values().all(|&v| v) {
            return Ok(true);
        }

        tokio::time::sleep(poll_interval).await;
    }
}

/// Wait for ALL marker records to be deleted from their accelerated tables.
async fn wait_for_all_marker_deletions(
    spiced: &SpicedInstance,
    marker_queries: &HashMap<DatasetType, String>,
    timeout: Duration,
) -> Result<()> {
    let start = Instant::now();
    let poll_interval = Duration::from_millis(500);

    let spice_client = spiced.spice_client(None, false).await?;

    let mut deleted: HashMap<DatasetType, bool> =
        marker_queries.keys().map(|dt| (*dt, false)).collect();

    loop {
        if start.elapsed() > timeout {
            let missing: Vec<_> = deleted
                .iter()
                .filter(|&(_, v)| !v)
                .map(|(k, _)| k.to_string())
                .collect();
            println!(
                "Marker deletion did not propagate for all datasets within timeout: {missing:?}"
            );
            return Ok(());
        }

        for (dataset_type, query) in marker_queries {
            if deleted[dataset_type] {
                continue;
            }

            if let Ok(stream) = spice_client.query(query).await
                && let Ok(batches) = stream.try_collect::<Vec<RecordBatch>>().await
            {
                for batch in &batches {
                    if batch.num_rows() > 0
                        && let Some(count) = get_count_from_batch(batch)
                        && count == 0
                    {
                        println!("Marker deletion confirmed for {dataset_type}");
                        deleted.insert(*dataset_type, true);
                        break;
                    }
                }
            }
        }

        if deleted.values().all(|&v| v) {
            println!("All marker deletions confirmed");
            return Ok(());
        }

        tokio::time::sleep(poll_interval).await;
    }
}

/// Extract count value from a query result batch.
fn get_count_from_batch(batch: &RecordBatch) -> Option<i64> {
    if let Some(array) = batch.column(0).as_any().downcast_ref::<Int64Array>() {
        return Some(array.value(0));
    }
    if let Some(array) = batch.column(0).as_any().downcast_ref::<UInt64Array>() {
        return Some(array.value(0) as i64);
    }
    None
}

/// Calculate aggregate liveness stats from a health check report.
fn calculate_liveness_stats(report: Option<&HealthCheckReport>) -> (u64, f64) {
    let Some(report) = report else {
        return (0, 0.0);
    };

    let (total_failures, max_latency) = report.aggregate_stats();
    let max_latency_ms = max_latency.as_secs_f64() * 1000.0;

    (total_failures, max_latency_ms)
}

/// Create a streaming source based on the arguments.
///
/// Configuration is read from environment variables.
fn create_source(args: &StreamingTestArgs) -> Result<Box<dyn StreamingSource>> {
    args.source.create()
}
