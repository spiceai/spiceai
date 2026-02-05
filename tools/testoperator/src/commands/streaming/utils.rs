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
use test_framework::constants::METRICS_URL;
use test_framework::spiced::SpicedInstance;

use super::datasets::DatasetType;
use super::traits::StreamingDataset;

/// Information about a dataset being benchmarked.
#[expect(dead_code)]
pub struct DatasetInfo {
    pub dataset: Box<dyn StreamingDataset>,
    pub marker: RecordBatch,
    pub record_count: usize,
    /// Original generated data (for mutation testing).
    pub generated_data: Vec<RecordBatch>,
}

/// Generate a short unique run ID for table isolation.
pub fn generate_run_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();

    // Combine timestamp with some randomness from the lower bits
    let seed = now.as_nanos();
    format!("{:06x}", (seed & 0x00FF_FFFF) as u32)
}

/// Load a spicepod definition from a path.
pub fn load_spicepod_definition(path: &Path) -> Result<SpicepodDefinition> {
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read spicepod: {}", path.display()))?;
    let definition: SpicepodDefinition = yaml::from_str(&content)
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

    let content = yaml::to_string(spicepod).context("Failed to serialize spicepod")?;
    std::fs::write(&path, content)
        .with_context(|| format!("Failed to write temp spicepod: {}", path.display()))?;

    Ok(path)
}

/// Skip the first N rows from a list of record batches.
pub fn skip_rows(batches: &[RecordBatch], rows_to_skip: usize) -> Vec<RecordBatch> {
    if batches.is_empty() || rows_to_skip == 0 {
        return batches.to_vec();
    }

    let mut result = Vec::new();
    let mut remaining_to_skip = rows_to_skip;

    for batch in batches {
        if remaining_to_skip >= batch.num_rows() {
            remaining_to_skip -= batch.num_rows();
            continue;
        }

        if remaining_to_skip > 0 {
            let keep = batch.num_rows() - remaining_to_skip;
            result.push(batch.slice(remaining_to_skip, keep));
            remaining_to_skip = 0;
        } else {
            result.push(batch.clone());
        }
    }

    result
}

/// Extract count value from a query result batch.
#[expect(clippy::cast_possible_wrap)]
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
/// The `marker_counts` map specifies how many markers to expect for each dataset.
#[expect(clippy::cast_possible_wrap)]
pub async fn poll_for_all_markers(
    spiced: &SpicedInstance,
    marker_queries: &HashMap<DatasetType, String>,
    marker_counts: &HashMap<DatasetType, usize>,
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

            let expected_count = marker_counts.get(dataset_type).copied().unwrap_or(1) as i64;

            if let Ok(stream) = spice_client.sql(query).await
                && let Ok(batches) = stream.try_collect::<Vec<RecordBatch>>().await
            {
                for batch in &batches {
                    if batch.num_rows() > 0
                        && let Some(count) = get_count_from_batch(batch)
                        && count >= expected_count
                    {
                        println!("Marker detected for {dataset_type} ({count}/{expected_count})");
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

            if let Ok(stream) = spice_client.sql(query).await
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
            let url =
                format!("http://localhost:8090/v1/datasets/{dataset_name}/acceleration/snapshots");

            if let Ok(response) = client.get(&url).send().await
                && response.status().is_success()
                && let Ok(body) = response.text().await
            {
                // Parse JSON response to check if snapshots array is non-empty
                // Response format: {"dataset_name":"...","snapshots":[...],...}
                if let Ok(json) = serde_json::from_str::<serde_json::Value>(&body)
                    && let Some(snapshots) = json.get("snapshots").and_then(|s| s.as_array())
                    && !snapshots.is_empty()
                {
                    println!("Snapshot created for {dataset_name}");
                    newly_completed.push(*dataset_name);
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

/// `DynamoDB` metrics fetched from Spice's Prometheus endpoint.
#[derive(Debug, Default)]
pub struct DynamoDbMetrics {
    /// Total records consumed across all datasets.
    pub records_consumed_total: u64,
    /// Total transient errors across all datasets.
    pub errors_transient_total: u64,
}

/// Fetch `DynamoDB` metrics from Spice's Prometheus metrics endpoint.
///
/// This sums metrics across all datasets:
/// - `dataset_dynamodb_records_consumed_total`
/// - `dataset_dynamodb_errors_transient_total`
///
/// Requires spiced to be started with `--metrics` flag.
#[expect(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
pub async fn get_dynamodb_metrics() -> Result<DynamoDbMetrics> {
    let client = reqwest::Client::new();
    let response = client
        .get(METRICS_URL)
        .send()
        .await
        .context("Failed to fetch metrics from Spice")?
        .text()
        .await
        .context("Failed to read metrics response")?;

    let mut metrics = DynamoDbMetrics::default();

    for line in response.lines() {
        // Skip comments and empty lines
        if line.starts_with('#') || line.is_empty() {
            continue;
        }

        // Parse value from the last whitespace-separated token
        let parse_value = |line: &str| -> u64 {
            line.split_whitespace()
                .last()
                .and_then(|v| v.parse::<f64>().ok())
                .map_or(0, |v| v as u64)
        };

        // Match lines like: dataset_dynamodb_records_consumed_total{dataset="lineitem",...} 12345
        if line.starts_with("dataset_dynamodb_records_consumed_total") {
            metrics.records_consumed_total += parse_value(line);
        } else if line.starts_with("dataset_dynamodb_errors_transient_total") {
            metrics.errors_transient_total += parse_value(line);
        }
    }

    Ok(metrics)
}
