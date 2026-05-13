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
//! Probes 4 TPC-C tables every ~100ms by comparing `MAX(_bench_ts)` between the
//! Postgres source and the Spice accelerated copy. The gap is the replication
//! staleness — how far behind Spice is from the source at any given moment.
//!
//! Probe tables (selected for diversity of CDC code paths):
//! - `district`: small (10 rows), pipeline baseline
//! - `order_line`: high write volume (300K+ rows), 4-column PK
//! - `new_order`: exercises DELETE path (Delivery removes oldest orders)
//! - `history`: append-only, no primary key

use std::collections::HashMap;
use std::time::Duration;

use arrow::array::{Array, TimestampMicrosecondArray};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use chbench_driver::schema::STALENESS_PROBE_TABLES;
use futures::TryStreamExt;
use test_framework::anyhow;
use tokio_util::sync::CancellationToken;

/// Per-table staleness statistics.
#[derive(Debug, Clone)]
pub struct StalenessStats {
    pub p50: Duration,
    pub p99: Duration,
    pub max: Duration,
    pub samples: u64,
}

/// Staleness report across all probed tables.
#[derive(Debug)]
pub struct StalenessReport {
    /// Per-table staleness statistics.
    pub tables: HashMap<String, StalenessStats>,
    /// Headline metric: worst-case P99 across all tables.
    pub headline_p99: Duration,
}

impl StalenessReport {
    /// Print a human-readable staleness summary.
    pub fn print_summary(&self) {
        println!("\n--- Staleness Gap ---");
        for table in STALENESS_PROBE_TABLES {
            if let Some(stats) = self.tables.get(*table) {
                println!(
                    "  {:<14} P50={:>5}ms  P99={:>5}ms  max={:>5}ms  ({} samples)",
                    format!("{table}:"),
                    stats.p50.as_millis(),
                    stats.p99.as_millis(),
                    stats.max.as_millis(),
                    stats.samples,
                );
            }
        }
        println!("  ─────────────────");
        println!(
            "  headline P99:  {}ms (worst-case)",
            self.headline_p99.as_millis()
        );
    }
}

/// Spawn a background task that probes staleness until cancelled.
///
/// Returns a `JoinHandle` that resolves to a `StalenessReport`.
pub fn spawn_staleness_probe(
    pg_connection_string: String,
    spice_client: spiceai::Client,
    cancel: CancellationToken,
) -> tokio::task::JoinHandle<anyhow::Result<StalenessReport>> {
    tokio::spawn(async move {
        run_staleness_probe(&pg_connection_string, spice_client, cancel).await
    })
}

/// Core probe loop. Runs until cancelled, collecting gap samples for each table.
async fn run_staleness_probe(
    pg_conn_str: &str,
    spice_client: spiceai::Client,
    cancel: CancellationToken,
) -> anyhow::Result<StalenessReport> {
    let poll_interval = Duration::from_millis(100);

    // Connect to Postgres source.
    let (pg_client, pg_connection) =
        tokio_postgres::connect(pg_conn_str, tokio_postgres::NoTls).await?;
    let pg_cancel = cancel.clone();
    tokio::spawn(async move {
        tokio::select! {
            result = pg_connection => {
                if let Err(e) = result {
                    eprintln!("Staleness probe Postgres connection error: {e}");
                }
            }
            () = pg_cancel.cancelled() => {}
        }
    });

    // Per-table gap samples (microseconds).
    let mut samples: HashMap<String, Vec<i64>> = STALENESS_PROBE_TABLES
        .iter()
        .map(|t| ((*t).to_string(), Vec::new()))
        .collect();

    // Wait briefly for initial data to be loaded and replicated before probing.
    tokio::time::sleep(Duration::from_secs(2)).await;

    loop {
        if cancel.is_cancelled() {
            break;
        }

        for table in STALENESS_PROBE_TABLES {
            // Query both endpoints concurrently.
            let (pg_result, spice_result) = tokio::join!(
                query_max_bench_ts_pg(&pg_client, table),
                query_max_bench_ts_spice(&spice_client, table),
            );

            // After awaiting queries, re-check cancellation to avoid
            // logging benign "connection closed" errors during shutdown.
            if cancel.is_cancelled() {
                break;
            }

            match (pg_result, spice_result) {
                (Ok(Some(pg_ts)), Ok(Some(spice_ts))) => {
                    let gap_us = (pg_ts - spice_ts).max(0);
                    if let Some(table_samples) = samples.get_mut(*table) {
                        table_samples.push(gap_us);
                    }
                }
                (pg, spice) => {
                    eprintln!("Staleness probe: {table} pg={pg:?} spice={spice:?}");
                }
            }
        }

        tokio::select! {
            () = tokio::time::sleep(poll_interval) => {}
            () = cancel.cancelled() => { break; }
        }
    }

    Ok(build_report(samples))
}

/// Query `MAX(_bench_ts)` from Postgres, returning microseconds since epoch.
///
/// Retrieves the `TIMESTAMPTZ` value directly (via `tokio-postgres`'s `with-chrono-0_4`
/// feature) and converts to microseconds.
async fn query_max_bench_ts_pg(
    client: &tokio_postgres::Client,
    table: &str,
) -> anyhow::Result<Option<i64>> {
    let query = format!("SELECT MAX(_bench_ts) FROM {table}");
    let rows = client.query(&query, &[]).await?;
    if rows.is_empty() {
        return Ok(None);
    }
    let ts: Option<chrono::DateTime<chrono::Utc>> = rows[0].get(0);
    Ok(ts.map(|t| t.timestamp_micros()))
}

/// Query `MAX(_bench_ts)` from Spice via Flight SQL, returning microseconds since epoch.
async fn query_max_bench_ts_spice(
    client: &spiceai::Client,
    table: &str,
) -> anyhow::Result<Option<i64>> {
    let query = format!("SELECT MAX(_bench_ts) AS max_ts FROM {table}");
    let stream = client.sql(&query).await?;
    let batches = stream.try_collect::<Vec<RecordBatch>>().await?;

    for batch in &batches {
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
fn build_report(samples: HashMap<String, Vec<i64>>) -> StalenessReport {
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
        let p50 = Duration::from_micros(gaps[n / 2] as u64);
        let p99_idx = (n as f64 * 0.99).ceil() as usize;
        let p99 = Duration::from_micros(gaps[p99_idx.min(n - 1)] as u64);
        let max = Duration::from_micros(gaps[n - 1] as u64);

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
        headline_p99: worst_p99,
    }
}
