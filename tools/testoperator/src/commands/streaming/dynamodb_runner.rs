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

//! DynamoDB Streams ingestion benchmark runner.
//!
//! This module contains the DynamoDB-specific benchmark orchestration logic.
//! It uses snapshot-based checkpoint capture to ensure fair benchmarking
//! when comparing multiple acceleration configurations.
//!
//! ## Flow
//! 1. Create tables, insert first record (for schema inference)
//! 2. For each config: start temp Spice, capture checkpoint snapshot
//! 3. Insert remaining data
//! 4. For each config: start benchmark Spice from snapshot, insert marker, wait for marker
//! 5. Report comparison results

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::RecordBatch;
use futures::future::try_join_all;
use test_framework::anyhow::{self, Result};
use test_framework::spiced::{SpicedInstance, StartRequest};

use super::datasets::DatasetType;
use super::mutations;
use super::traits::{DynamoDBStreamingSource, SnapshotConfig, StreamingDataset};
use super::utils::{
    generate_run_id, load_spicepod_definition, poll_for_all_markers, poll_for_snapshots,
    print_benchmark_summary, skip_first_row, wait_for_all_marker_deletions, write_temp_spicepod,
    BenchmarkResult, DatasetInfo,
};
use super::verification;
use crate::args::StreamingDynamodbTestArgs;

/// Run the DynamoDB streaming ingestion benchmark.
pub async fn run_dynamodb(args: &StreamingDynamodbTestArgs) -> Result<()> {
    let spicepod_paths = args.all_spicepod_paths();
    let datasets = args.queryset.get_datasets();

    println!("Starting DynamoDB streaming ingestion benchmark");
    println!("Source: {}", args.source);
    println!("Query set: {}", args.queryset);
    println!("Configs: {}", spicepod_paths.len());
    println!(
        "Datasets: {}",
        datasets
            .iter()
            .map(|d| d.dataset_type().to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );
    println!("Scale factor: {}", args.scale_factor);

    // Generate unique run ID for table isolation
    let run_id = generate_run_id();
    println!("Generated run ID: {run_id}");

    // Create source and set table prefix
    let mut source = create_dynamodb_source(args)?;
    source.set_table_prefix(run_id.clone());

    // Check if snapshots are configured (required for DynamoDB)
    let snapshot_config = build_snapshot_config().ok_or_else(|| {
        anyhow::anyhow!("DynamoDB benchmarks require SNAPSHOT_S3_LOCATION environment variable")
    })?;

    // Phase 1: Prepare source and create tables
    println!("Phase 1: Preparing streaming source");
    source.prepare().await?;

    let source: Arc<dyn DynamoDBStreamingSource> = Arc::from(source);

    println!("Phase 2: Creating tables for all datasets (parallel)");
    let table_creation_futures: Vec<_> = datasets
        .iter()
        .map(|dataset| {
            let source = Arc::clone(&source);
            let dataset_type = dataset.dataset_type();
            async move { source.create_table(dataset_type).await }
        })
        .collect();

    try_join_all(table_creation_futures).await?;
    println!("All tables created");

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

    // Phase 4: Insert first record per dataset (for schema inference)
    println!("Phase 4: Inserting first record per dataset (for schema)");
    for info in &dataset_infos {
        let table_name = source.get_table_name(info.dataset.table_name());
        if let Some(first_batch) = info.generated_data.first() {
            if first_batch.num_rows() > 0 {
                // Create a batch with just the first row
                let first_row = first_batch.slice(0, 1);
                source.insert(&table_name, &[first_row]).await?;
            }
        }
    }

    // Phase 5: Checkpoint capture (sequential due to fixed port limitation)
    let config_names: Vec<String> = spicepod_paths
        .iter()
        .map(|p| {
            p.file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("unknown")
                .to_string()
        })
        .collect();

    println!("Phase 5: Capturing checkpoints for all configs (sequential)");

    for (path, config_name) in spicepod_paths.iter().zip(config_names.iter()) {
        capture_checkpoint_snapshot(&source, path, &run_id, config_name, &snapshot_config, args)
            .await?;
    }

    println!("All checkpoint snapshots captured");

    // Phase 6: Insert remaining data
    println!("Phase 6: Inserting remaining data");
    let data_insertion_start = Instant::now();
    let mut total_insert_duration = Duration::ZERO;

    let mutation_summary = if args.mutation_ratio > 0.0 {
        println!("  Executing mutation sequences for CDC testing");
        println!(
            "  Seed: {}, Mutation ratio: {:.1}%",
            args.mutation_seed,
            args.mutation_ratio * 100.0
        );

        let config = mutations::MutationConfig {
            seed: args.mutation_seed,
            mutation_ratio: args.mutation_ratio,
        };

        let datasets_for_mutation: Vec<Box<dyn StreamingDataset>> = dataset_infos
            .iter()
            .map(|info| info.dataset.dataset_type().create_dataset())
            .collect();

        // Skip first row (already inserted)
        let original_data: Vec<(DatasetType, Vec<RecordBatch>)> = dataset_infos
            .iter()
            .map(|info| {
                let batches = skip_first_row(&info.generated_data);
                (info.dataset.dataset_type(), batches)
            })
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
        for info in &dataset_infos {
            let dataset_type = info.dataset.dataset_type();
            let table_name = source.get_table_name(info.dataset.table_name());
            println!("  Inserting data for {dataset_type}");

            // Skip first row (already inserted)
            let remaining_data = skip_first_row(&info.generated_data);

            let insert_start = Instant::now();
            source.insert(&table_name, &remaining_data).await?;
            total_insert_duration += insert_start.elapsed();
        }
        None
    };

    println!("Data insertion completed in {total_insert_duration:?}");

    // Phase 7: Run benchmarks (sequential due to fixed port limitation)
    println!("Phase 7: Running benchmarks for all configs (sequential)");

    let mut results = Vec::new();
    for (path, config_name) in spicepod_paths.iter().zip(config_names.iter()) {
        let dataset_markers: Vec<_> = dataset_infos
            .iter()
            .map(|info| (info.dataset.dataset_type(), info.marker.clone()))
            .collect();

        let result = benchmark_spice_dynamodb(
            &source,
            path,
            &run_id,
            config_name,
            &snapshot_config,
            args,
            &dataset_markers,
            data_insertion_start,
        )
        .await?;
        results.push(result);
    }

    // Phase 8: Report results
    println!("\nPhase 8: Reporting results");

    let total_record_count: usize = dataset_infos.iter().map(|info| info.record_count).sum();
    print_benchmark_summary(
        "DynamoDB Streaming Ingestion Benchmark Results",
        &args.source.to_string(),
        &args.queryset.to_string(),
        &dataset_infos,
        args.scale_factor,
        total_record_count,
        total_insert_duration,
        &results,
        mutation_summary.as_ref(),
    );

    // Cleanup
    source.cleanup().await?;

    Ok(())
}

/// Capture a checkpoint snapshot for a single configuration.
async fn capture_checkpoint_snapshot(
    source: &Arc<dyn DynamoDBStreamingSource>,
    spicepod_path: &Path,
    run_id: &str,
    config_name: &str,
    snapshot_config: &SnapshotConfig,
    args: &StreamingDynamodbTestArgs,
) -> Result<()> {
    println!("  Capturing checkpoint for {config_name}");

    // Load and transform spicepod
    let spicepod_def = load_spicepod_definition(spicepod_path)?;
    let transformed =
        source.prepare_checkpoint_spicepod(spicepod_def, run_id, config_name, snapshot_config);

    // Write transformed spicepod to temp file
    let temp_path = write_temp_spicepod(&transformed, run_id, config_name, "checkpoint")?;

    // Start temp Spice
    let mut start_request = StartRequest::new(args.common.spiced_path_buf(), transformed)?;

    if let Some(ref data_dir) = args.common.data_dir {
        start_request = start_request.with_data_dir(data_dir.clone());
    }

    let mut spiced_instance = SpicedInstance::start(start_request).await?;

    spiced_instance
        .wait_for_ready(Duration::from_secs(args.common.ready_wait))
        .await?;

    // Poll snapshots API until snapshot exists
    poll_for_snapshots(Duration::from_secs(60)).await?;

    // Stop temp Spice
    spiced_instance.stop()?;

    // Cleanup temp file
    let _ = std::fs::remove_file(&temp_path);

    println!("  Checkpoint captured for {config_name}");
    Ok(())
}

/// Run a single DynamoDB benchmark for one configuration.
#[expect(clippy::too_many_arguments)]
async fn benchmark_spice_dynamodb(
    source: &Arc<dyn DynamoDBStreamingSource>,
    spicepod_path: &Path,
    run_id: &str,
    config_name: &str,
    snapshot_config: &SnapshotConfig,
    args: &StreamingDynamodbTestArgs,
    dataset_markers: &[(DatasetType, RecordBatch)],
    data_insertion_start: Instant,
) -> Result<BenchmarkResult> {
    println!("  Starting DynamoDB benchmark for {config_name}");

    // Load and transform spicepod
    let spicepod_def = load_spicepod_definition(spicepod_path)?;
    let transformed =
        source.prepare_benchmark_spicepod(spicepod_def, run_id, config_name, snapshot_config);

    // Write transformed spicepod to temp file
    let temp_path = write_temp_spicepod(&transformed, run_id, config_name, "benchmark")?;

    // Start Spice
    let mut start_request = StartRequest::new(args.common.spiced_path_buf(), transformed)?;

    if let Some(ref data_dir) = args.common.data_dir {
        start_request = start_request.with_data_dir(data_dir.clone());
    }

    let mut spiced_instance = SpicedInstance::start(start_request).await?;

    spiced_instance
        .wait_for_ready(Duration::from_secs(args.common.ready_wait))
        .await?;

    // Insert markers
    for (dataset_type, marker) in dataset_markers {
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

    let timeout = Duration::from_secs(args.ingestion_timeout);
    let all_markers_detected =
        poll_for_all_markers(&spiced_instance, &marker_queries, timeout).await?;

    let end_to_end_duration = data_insertion_start.elapsed();
    let stream_lag = marker_insertion_time.elapsed();

    if !all_markers_detected {
        spiced_instance.stop()?;
        let _ = std::fs::remove_file(&temp_path);
        return Err(anyhow::anyhow!(
            "Benchmark {config_name}: markers not detected within timeout"
        ));
    }

    // Delete markers
    for (dataset_type, _) in dataset_markers {
        source.delete_marker(*dataset_type).await?;
    }

    // Wait for deletions
    wait_for_all_marker_deletions(&spiced_instance, &marker_queries, Duration::from_secs(30))
        .await?;

    // Run verification if requested
    let verification_passed = if args.verify {
        let report = verification::verify_tpch_queries(&spiced_instance).await?;
        report.all_passed()
    } else {
        true
    };

    // Calculate throughput
    let record_count: usize = dataset_markers.len(); // This should be total records, fix later
    let throughput = if end_to_end_duration.as_secs_f64() > 0.0 {
        record_count as f64 / end_to_end_duration.as_secs_f64()
    } else {
        0.0
    };

    // Stop Spice
    spiced_instance.stop()?;

    // Cleanup temp file
    let _ = std::fs::remove_file(&temp_path);

    println!("  Benchmark complete for {config_name}");

    Ok(BenchmarkResult {
        config_name: config_name.to_string(),
        end_to_end_duration,
        stream_lag,
        throughput,
        record_count,
        verification_passed,
    })
}

/// Create a DynamoDB streaming source based on the arguments.
fn create_dynamodb_source(
    args: &StreamingDynamodbTestArgs,
) -> Result<Box<dyn DynamoDBStreamingSource>> {
    args.source.create_dynamodb()
}

/// Build snapshot configuration from environment variables.
///
/// Environment variables:
/// - `SNAPSHOT_S3_LOCATION`: S3 location for snapshots (e.g., `s3://bucket/snapshots/`)
/// - `SNAPSHOT_S3_ACCESS_KEY_ID`: S3 access key ID (optional)
/// - `SNAPSHOT_S3_SECRET_ACCESS_KEY`: S3 secret access key (optional)
/// - `SNAPSHOT_S3_REGION`: S3 region (optional)
fn build_snapshot_config() -> Option<SnapshotConfig> {
    let location = std::env::var("SNAPSHOT_S3_LOCATION").ok()?;

    Some(SnapshotConfig {
        location,
        access_key_id: std::env::var("SNAPSHOT_S3_ACCESS_KEY_ID").ok(),
        secret_access_key: std::env::var("SNAPSHOT_S3_SECRET_ACCESS_KEY").ok(),
        region: std::env::var("SNAPSHOT_S3_REGION").ok(),
    })
}

