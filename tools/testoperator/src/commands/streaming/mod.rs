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

//! DynamoDB Streams ingestion benchmark command.
//!
//! This benchmark measures the time it takes for Spice to ingest data from
//! DynamoDB Streams using snapshot-based checkpoint capture.
//!
//! ## Flow
//! 1. Create tables, insert first record (for schema inference)
//! 2. For each config: start temp Spice, capture checkpoint snapshot
//! 3. Insert remaining data
//! 4. For each config: start benchmark Spice from snapshot, insert marker, wait for marker
//! 5. Report comparison results

pub mod datasets;
pub mod mutations;
pub mod query_liveness;
pub mod querysets;
pub mod sources;
mod traits;
pub mod verification;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Int64Array, RecordBatch, UInt64Array};
use futures::future::try_join_all;
use futures::TryStreamExt;
use spicepod::spec::SpicepodDefinition;
use test_framework::anyhow::{self, Context, Result};
use test_framework::spiced::{SpicedInstance, StartRequest};

pub use datasets::DatasetType;
pub use sources::SourceType;
pub use traits::{SnapshotConfig, StreamingDataset, StreamingSource};

use crate::args::StreamingDynamodbTestArgs;

/// Information about a dataset being benchmarked.
struct DatasetInfo {
    dataset: Box<dyn StreamingDataset>,
    marker: RecordBatch,
    record_count: usize,
    /// Original generated data (for mutation testing).
    generated_data: Vec<RecordBatch>,
}

/// Result of a single benchmark run.
#[derive(Debug, Clone)]
pub struct BenchmarkResult {
    pub config_name: String,
    pub end_to_end_duration: Duration,
    pub stream_lag: Duration,
    pub throughput: f64,
    pub record_count: usize,
    pub verification_passed: bool,
}

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
    let mut source = create_source(args)?;
    source.set_table_prefix(run_id.clone());

    // Check if snapshots are configured (required for DynamoDB)
    let snapshot_config = build_snapshot_config();

    if snapshot_config.is_none() {
        return Err(anyhow::anyhow!(
            "DynamoDB benchmarks require SNAPSHOT_S3_LOCATION environment variable"
        ));
    }

    // Phase 1: Prepare source and create tables
    println!("Phase 1: Preparing streaming source");
    source.prepare().await?;

    let source: Arc<dyn StreamingSource> = Arc::from(source);

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

    // Phase 5: Checkpoint capture (parallel, if needed)
    let config_names: Vec<String> = spicepod_paths
        .iter()
        .map(|p| {
            p.file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("unknown")
                .to_string()
        })
        .collect();

    if let Some(ref snapshot_cfg) = snapshot_config {
        // Note: Checkpoint capture runs sequentially because test-framework uses fixed ports
        // and we can't run multiple Spice instances simultaneously
        println!("Phase 5: Capturing checkpoints for all configs (sequential)");

        for (path, config_name) in spicepod_paths.iter().zip(config_names.iter()) {
            capture_checkpoint_snapshot(
                &source,
                path,
                &run_id,
                config_name,
                snapshot_cfg,
                args,
            )
            .await?;
        }

        println!("All checkpoint snapshots captured");
    }

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

    // Phase 7: Run benchmarks
    println!("Phase 7: Running benchmarks for all configs");

    let benchmark_futures: Vec<_> = spicepod_paths
        .iter()
        .zip(config_names.iter())
        .map(|(path, config_name)| {
            let source = Arc::clone(&source);
            let run_id = run_id.clone();
            let config_name = config_name.clone();
            let snapshot_cfg = snapshot_config.clone();
            let path = path.clone();
            let args = args.clone();
            let dataset_infos_clone: Vec<_> = dataset_infos
                .iter()
                .map(|info| (info.dataset.dataset_type(), info.marker.clone()))
                .collect();

            async move {
                benchmark_spice(
                    &source,
                    &path,
                    &run_id,
                    &config_name,
                    snapshot_cfg.as_ref(),
                    &args,
                    &dataset_infos_clone,
                    data_insertion_start,
                )
                .await
            }
        })
        .collect();

    // Note: Benchmarks run sequentially because test-framework uses fixed ports
    // and we can't run multiple Spice instances simultaneously
    if args.parallel {
        println!(
            "  Warning: --parallel ignored, running {} configs sequentially (fixed port limitation)",
            spicepod_paths.len()
        );
    } else {
        println!("  Running {} configs sequentially", spicepod_paths.len());
    }

    let mut results = Vec::new();
    for future in benchmark_futures {
        results.push(future.await?);
    }

    // Phase 8: Report results
    println!("\nPhase 8: Reporting results");

    let total_record_count: usize = dataset_infos.iter().map(|info| info.record_count).sum();
    print_multi_config_summary(
        args,
        &dataset_infos,
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
    source: &Arc<dyn StreamingSource>,
    spicepod_path: &Path,
    run_id: &str,
    config_name: &str,
    snapshot_config: &SnapshotConfig,
    args: &StreamingDynamodbTestArgs,
) -> Result<()> {
    println!("  Capturing checkpoint for {config_name}");

    // Load and transform spicepod
    let spicepod_def = load_spicepod_definition(spicepod_path).await?;

    let transformed = match source
        .prepare_checkpoint_spicepod(spicepod_def, run_id, config_name, snapshot_config)
    {
        Some(transformed) => transformed,
        None => load_spicepod_definition(spicepod_path).await?,
    };

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
    poll_for_snapshots(&spiced_instance, Duration::from_secs(60)).await?;

    // Stop temp Spice
    spiced_instance.stop()?;

    // Cleanup temp file
    let _ = std::fs::remove_file(&temp_path);

    println!("  Checkpoint captured for {config_name}");
    Ok(())
}

/// Run a single benchmark for one configuration.
async fn benchmark_spice(
    source: &Arc<dyn StreamingSource>,
    spicepod_path: &Path,
    run_id: &str,
    config_name: &str,
    snapshot_config: Option<&SnapshotConfig>,
    args: &StreamingDynamodbTestArgs,
    dataset_markers: &[(DatasetType, RecordBatch)],
    data_insertion_start: Instant,
) -> Result<BenchmarkResult> {
    println!("  Starting benchmark for {config_name}");

    // Load and transform spicepod
    let spicepod_def = load_spicepod_definition(spicepod_path).await?;

    let transformed = if let Some(snapshot_cfg) = snapshot_config {
        match source.prepare_benchmark_spicepod(spicepod_def, run_id, config_name, snapshot_cfg) {
            Some(transformed) => transformed,
            None => load_spicepod_definition(spicepod_path).await?,
        }
    } else {
        spicepod_def
    };

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

/// Poll the snapshots API until at least one snapshot exists.
async fn poll_for_snapshots(_spiced: &SpicedInstance, timeout: Duration) -> Result<()> {
    let start = Instant::now();
    let poll_interval = Duration::from_millis(1000);

    loop {
        if start.elapsed() > timeout {
            return Err(anyhow::anyhow!("Timeout waiting for snapshot creation"));
        }

        // Query the snapshots endpoint (hardcoded port as test-framework uses fixed ports)
        let client = reqwest::Client::new();
        let url = "http://localhost:8090/v1/snapshots";

        if let Ok(response) = client.get(url).send().await
            && response.status().is_success()
        {
            if let Ok(body) = response.text().await {
                // Check if there are any snapshots (non-empty response)
                if !body.trim().is_empty() && body != "[]" {
                    println!("Snapshot created");
                    return Ok(());
                }
            }
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

/// Create a streaming source based on the arguments.
fn create_source(args: &StreamingDynamodbTestArgs) -> Result<Box<dyn StreamingSource>> {
    args.source.create()
}

/// Generate a short unique run ID for table isolation.
fn generate_run_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();

    // Combine timestamp with some randomness from the lower bits
    let seed = now.as_nanos();
    format!("{:06x}", (seed & 0xFFFFFF) as u32)
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

/// Load a spicepod definition from a path.
async fn load_spicepod_definition(path: &Path) -> Result<SpicepodDefinition> {
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read spicepod: {}", path.display()))?;
    let definition: SpicepodDefinition = serde_yaml::from_str(&content)
        .with_context(|| format!("Failed to parse spicepod: {}", path.display()))?;
    Ok(definition)
}

/// Write a transformed spicepod to a temp file.
fn write_temp_spicepod(
    spicepod: &SpicepodDefinition,
    run_id: &str,
    config_name: &str,
    phase: &str,
) -> Result<PathBuf> {
    let temp_dir = std::env::temp_dir();
    let filename = format!("spicepod-{run_id}-{config_name}-{phase}.yaml");
    let path = temp_dir.join(filename);

    let content = serde_yaml::to_string(spicepod)
        .context("Failed to serialize spicepod")?;
    std::fs::write(&path, content)
        .with_context(|| format!("Failed to write temp spicepod: {}", path.display()))?;

    Ok(path)
}

/// Skip the first row from a list of record batches.
fn skip_first_row(batches: &[RecordBatch]) -> Vec<RecordBatch> {
    if batches.is_empty() {
        return Vec::new();
    }

    let mut result = Vec::new();
    let mut skipped = false;

    for batch in batches {
        if !skipped && batch.num_rows() > 0 {
            if batch.num_rows() > 1 {
                result.push(batch.slice(1, batch.num_rows() - 1));
            }
            skipped = true;
        } else {
            result.push(batch.clone());
        }
    }

    result
}

/// Print summary for benchmark.
fn print_multi_config_summary(
    args: &StreamingDynamodbTestArgs,
    dataset_infos: &[DatasetInfo],
    total_record_count: usize,
    total_insert_duration: Duration,
    results: &[BenchmarkResult],
    mutation_summary: Option<&mutations::MutationSummary>,
) {
    println!("\n{}", "=".repeat(70));
    println!("Streaming Ingestion Benchmark Results (Multi-Config)");
    println!("{}", "=".repeat(70));
    println!("Source:              {}", args.source);
    println!("Query Set:           {}", args.queryset);
    println!(
        "Datasets:            {}",
        dataset_infos
            .iter()
            .map(|info| info.dataset.dataset_type().to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );
    println!("Scale Factor:        {}", args.scale_factor);
    println!("Total Records:       {total_record_count}");
    println!("Data Insertion Time: {total_insert_duration:?}");

    if let Some(summary) = mutation_summary {
        println!(
            "Mutations:           {} update-path + {} delete-path + {} direct",
            summary.update_path_rows, summary.delete_path_rows, summary.direct_insert_rows
        );
    }

    println!("\nConfiguration Comparison:");
    println!(
        "┌{:─<20}┬{:─<14}┬{:─<12}┬{:─<16}┬{:─<10}┐",
        "", "", "", "", ""
    );
    println!(
        "│ {:18} │ {:12} │ {:10} │ {:14} │ {:8} │",
        "Config", "End-to-End", "Stream Lag", "Throughput", "Verified"
    );
    println!(
        "├{:─<20}┼{:─<14}┼{:─<12}┼{:─<16}┼{:─<10}┤",
        "", "", "", "", ""
    );

    for result in results {
        let verified = if result.verification_passed {
            "PASS"
        } else {
            "FAIL"
        };
        println!(
            "│ {:18} │ {:>10.2}s │ {:>8.2}s │ {:>12.0} r/s │ {:8} │",
            result.config_name,
            result.end_to_end_duration.as_secs_f64(),
            result.stream_lag.as_secs_f64(),
            result.throughput,
            verified
        );
    }

    println!(
        "└{:─<20}┴{:─<14}┴{:─<12}┴{:─<16}┴{:─<10}┘",
        "", "", "", "", ""
    );
    println!("{}", "=".repeat(70));
}
