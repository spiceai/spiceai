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

//! Generic utilities for streaming ingestion benchmarks.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use arrow::array::{Int64Array, RecordBatch, UInt64Array};
use futures::TryStreamExt;
use spicepod::spec::SpicepodDefinition;
use test_framework::anyhow::{self, Context, Result};
use test_framework::spiced::SpicedInstance;

use super::datasets::DatasetType;
use super::mutations::MutationSummary;
use super::traits::StreamingDataset;

/// Information about a dataset being benchmarked.
pub struct DatasetInfo {
    pub dataset: Box<dyn StreamingDataset>,
    pub marker: RecordBatch,
    pub record_count: usize,
    /// Original generated data (for mutation testing).
    pub generated_data: Vec<RecordBatch>,
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

/// Generate a short unique run ID for table isolation.
pub fn generate_run_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();

    // Combine timestamp with some randomness from the lower bits
    let seed = now.as_nanos();
    format!("{:06x}", (seed & 0xFFFFFF) as u32)
}

/// Load a spicepod definition from a path.
pub fn load_spicepod_definition(path: &Path) -> Result<SpicepodDefinition> {
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read spicepod: {}", path.display()))?;
    let definition: SpicepodDefinition = serde_yaml::from_str(&content)
        .with_context(|| format!("Failed to parse spicepod: {}", path.display()))?;
    Ok(definition)
}

/// Write a transformed spicepod to a temp file.
pub fn write_temp_spicepod(
    spicepod: &SpicepodDefinition,
    run_id: &str,
    config_name: &str,
    phase: &str,
) -> Result<PathBuf> {
    let temp_dir = std::env::temp_dir();
    let filename = format!("spicepod-{run_id}-{config_name}-{phase}.yaml");
    let path = temp_dir.join(filename);

    let content = serde_yaml::to_string(spicepod).context("Failed to serialize spicepod")?;
    std::fs::write(&path, content)
        .with_context(|| format!("Failed to write temp spicepod: {}", path.display()))?;

    Ok(path)
}

/// Skip the first row from a list of record batches.
pub fn skip_first_row(batches: &[RecordBatch]) -> Vec<RecordBatch> {
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

/// Extract count value from a query result batch.
pub fn get_count_from_batch(batch: &RecordBatch) -> Option<i64> {
    if let Some(array) = batch.column(0).as_any().downcast_ref::<Int64Array>() {
        return Some(array.value(0));
    }
    if let Some(array) = batch.column(0).as_any().downcast_ref::<UInt64Array>() {
        return Some(array.value(0) as i64);
    }
    None
}

/// Poll until ALL markers are detected in their respective accelerated tables.
pub async fn poll_for_all_markers(
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
pub async fn wait_for_all_marker_deletions(
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

/// Poll the snapshots API until at least one snapshot exists for ALL given datasets.
pub async fn poll_for_all_snapshots(dataset_names: &[&str], timeout: Duration) -> Result<()> {
    let start = Instant::now();
    let poll_interval = Duration::from_millis(1000);
    let client = reqwest::Client::new();

    let mut pending: std::collections::HashSet<&str> = dataset_names.iter().copied().collect();

    loop {
        if start.elapsed() > timeout {
            let missing: Vec<_> = pending.iter().copied().collect();
            return Err(anyhow::anyhow!(
                "Timeout waiting for snapshot creation. Missing: {missing:?}"
            ));
        }

        // Check each pending dataset
        let mut newly_completed = Vec::new();
        for dataset_name in &pending {
            let url = format!(
                "http://localhost:8090/v1/datasets/{dataset_name}/acceleration/snapshots"
            );

            if let Ok(response) = client.get(&url).send().await
                && response.status().is_success()
            {
                if let Ok(body) = response.text().await {
                    // Parse JSON response to check if snapshots array is non-empty
                    // Response format: {"dataset_name":"...","snapshots":[...],...}
                    if let Ok(json) = serde_json::from_str::<serde_json::Value>(&body) {
                        if let Some(snapshots) = json.get("snapshots").and_then(|s| s.as_array()) {
                            if !snapshots.is_empty() {
                                println!("Snapshot created for {dataset_name}");
                                newly_completed.push(*dataset_name);
                            }
                        }
                    }
                }
            }
        }

        for name in newly_completed {
            pending.remove(name);
        }

        if pending.is_empty() {
            println!("All snapshots created");
            return Ok(());
        }

        tokio::time::sleep(poll_interval).await;
    }
}

/// Print summary for benchmark results.
pub fn print_benchmark_summary(
    title: &str,
    source_name: &str,
    queryset_name: &str,
    dataset_infos: &[DatasetInfo],
    scale_factor: f64,
    total_record_count: usize,
    total_insert_duration: Duration,
    results: &[BenchmarkResult],
) {
    println!("\n{}", "=".repeat(70));
    println!("{title}");
    println!("{}", "=".repeat(70));
    println!("Source:              {source_name}");
    println!("Query Set:           {queryset_name}");
    println!(
        "Datasets:            {}",
        dataset_infos
            .iter()
            .map(|info| info.dataset.dataset_type().to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );
    println!("Scale Factor:        {scale_factor}");
    println!("Total Records:       {total_record_count}");
    println!("Data Insertion Time: {total_insert_duration:?}");

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
