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

//! Staleness gap measurement for HTAP benchmarks.
//!
//! Probes TPC-C tables every 5s by comparing `MAX(_bench_ts)` between the
//! source and the Spice accelerated copy. The gap is the replication
//! staleness — how far behind Spice is from the source at any given moment.
//!
//! Which tables are probed is determined by the driver's `probe_tables()` method.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, TimestampMicrosecondArray};
use arrow::datatypes::DataType;
use chbench_driver::ChBenchDriver;
use futures::TryStreamExt;
use test_framework::anyhow;
use test_framework::opentelemetry::KeyValue;
use tokio_util::sync::CancellationToken;

/// Per-table staleness statistics.
#[derive(Debug, Clone)]
pub struct StalenessStats {
    pub p50: Duration,
    pub p99: Duration,
    pub max: Duration,
    pub samples: u64,
}

/// Data freshness report across all probed tables.
#[derive(Debug)]
pub struct StalenessReport {
    /// Per-table staleness statistics.
    pub tables: HashMap<String, StalenessStats>,
    /// Ordered list of probed table names (for consistent output).
    pub probe_tables: Vec<String>,
    /// Worst-case P99 across all tables.
    pub worst_p99: Duration,
}

impl StalenessReport {
    /// Print a human-readable data freshness summary and record OTEL metrics.
    pub fn emit(&self) {
        println!("\nData Freshness");
        println!(
            "  {:<14} {:>10} {:>10} {:>10} {:>10}",
            "dataset", "p50_ms", "p99_ms", "max_ms", "samples"
        );
        for table in &self.probe_tables {
            if let Some(stats) = self.tables.get(table.as_str()) {
                println!(
                    "  {:<14} {:>10} {:>10} {:>10} {:>10}",
                    table,
                    stats.p50.as_millis(),
                    stats.p99.as_millis(),
                    stats.max.as_millis(),
                    stats.samples,
                );
                #[expect(clippy::cast_precision_loss)]
                let p99_ms = stats.p99.as_millis() as f64;
                crate::metrics::DATA_FRESHNESS_P99
                    .record(p99_ms, &[KeyValue::new("dataset", table.clone())]);
            }
        }
        println!("  worst P99:     {}ms", self.worst_p99.as_millis());
        #[expect(clippy::cast_precision_loss)]
        let worst_ms = self.worst_p99.as_millis() as f64;
        crate::metrics::DATA_FRESHNESS_P99.record(worst_ms, &[]);
    }
}

/// Spawn a background task that probes staleness until cancelled.
///
/// Returns a `JoinHandle` that resolves to a `StalenessReport`.
pub fn spawn_staleness_probe(
    driver: Arc<dyn ChBenchDriver>,
    spice_client: spiceai::Client,
    cancel: CancellationToken,
) -> tokio::task::JoinHandle<anyhow::Result<StalenessReport>> {
    tokio::spawn(async move { run_staleness_probe(driver, spice_client, cancel).await })
}

/// Core probe loop. Runs until cancelled, collecting gap samples for each table.
async fn run_staleness_probe(
    driver: Arc<dyn ChBenchDriver>,
    spice_client: spiceai::Client,
    cancel: CancellationToken,
) -> anyhow::Result<StalenessReport> {
    let poll_interval = Duration::from_secs(5);
    let probe_tables = driver.probe_tables();

    // Per-table gap samples (microseconds).
    let mut samples: HashMap<String, Vec<i64>> = probe_tables
        .iter()
        .map(|t| ((*t).to_string(), Vec::new()))
        .collect();

    // Ordered list of table names for consistent report output.
    let probe_table_names: Vec<String> = probe_tables.iter().map(|t| (*t).to_string()).collect();

    // Wait briefly for initial data to be loaded and replicated before probing.
    tokio::time::sleep(Duration::from_secs(2)).await;

    loop {
        if cancel.is_cancelled() {
            break;
        }

        for table in probe_tables {
            // Query both endpoints concurrently.
            let (source_result, spice_result) = tokio::join!(
                driver.max_bench_ts(table),
                query_max_bench_ts_spice(&spice_client, table),
            );

            // Re-check cancellation after queries to suppress benign
            // "connection closed" errors during shutdown.
            if cancel.is_cancelled() {
                break;
            }

            match (source_result, spice_result) {
                (Ok(Some(source_ts)), Ok(Some(spice_ts))) => {
                    let gap_us = (source_ts - spice_ts).max(0);
                    if let Some(table_samples) = samples.get_mut(*table) {
                        table_samples.push(gap_us);
                    }
                }
                (source, spice) => {
                    eprintln!("Staleness probe: {table} source={source:?} spice={spice:?}");
                }
            }
        }

        tokio::select! {
            () = tokio::time::sleep(poll_interval) => {}
            () = cancel.cancelled() => { break; }
        }
    }

    Ok(build_report(samples, probe_table_names))
}

/// Query `MAX(_bench_ts)` from Spice via Flight SQL, returning microseconds since epoch.
pub(super) async fn query_max_bench_ts_spice(
    client: &spiceai::Client,
    table: &str,
) -> anyhow::Result<Option<i64>> {
    let query = format!("SELECT MAX(_bench_ts) AS max_ts FROM {table}");
    let mut stream = client.sql(&query).await?;

    while let Some(batch) = stream.try_next().await? {
        if batch.num_rows() == 0 {
            continue;
        }
        let col = batch.column(0);

        // Handle both Timestamp(Microsecond, _) and Timestamp(Nanosecond, _).
        match col.data_type() {
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, _) => {
                let arr = col
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .ok_or_else(|| anyhow::anyhow!("unexpected array type for _bench_ts"))?;
                if !arr.is_null(0) {
                    return Ok(Some(arr.value(0)));
                }
            }
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, _) => {
                let arr = col
                    .as_any()
                    .downcast_ref::<arrow::array::TimestampNanosecondArray>()
                    .ok_or_else(|| anyhow::anyhow!("unexpected array type for _bench_ts"))?;
                if !arr.is_null(0) {
                    // Convert nanoseconds to microseconds.
                    return Ok(Some(arr.value(0) / 1_000));
                }
            }
            other => {
                return Err(anyhow::anyhow!(
                    "unexpected data type for MAX(_bench_ts): {other:?}"
                ));
            }
        }
    }

    Ok(None)
}

/// Build the final report from raw gap samples (in microseconds).
fn build_report(samples: HashMap<String, Vec<i64>>, probe_tables: Vec<String>) -> StalenessReport {
    let mut tables = HashMap::new();
    let mut worst_p99 = Duration::ZERO;

    for (table, mut gaps) in samples {
        if gaps.is_empty() {
            tables.insert(
                table,
                StalenessStats {
                    p50: Duration::ZERO,
                    p99: Duration::ZERO,
                    max: Duration::ZERO,
                    samples: 0,
                },
            );
            continue;
        }

        gaps.sort_unstable();
        let n = gaps.len();

        // Use the same nearest-rank percentile formula as `QueryLiveness::percentile`.
        let pct = |p: f64| -> usize {
            #[expect(
                clippy::cast_precision_loss,
                clippy::cast_possible_truncation,
                clippy::cast_sign_loss
            )]
            let idx = (p * (n - 1) as f64).round() as usize;
            idx.min(n - 1)
        };

        let p50 = Duration::from_micros(gaps[pct(0.50)].cast_unsigned());
        let p99 = Duration::from_micros(gaps[pct(0.99)].cast_unsigned());
        let max = Duration::from_micros(gaps[n - 1].cast_unsigned());

        if p99 > worst_p99 {
            worst_p99 = p99;
        }

        tables.insert(
            table,
            StalenessStats {
                p50,
                p99,
                max,
                samples: n as u64,
            },
        );
    }

    StalenessReport {
        tables,
        probe_tables,
        worst_p99,
    }
}
