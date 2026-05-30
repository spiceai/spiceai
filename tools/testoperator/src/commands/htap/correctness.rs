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

//! Data-correctness for HTAP benchmarks.
//!
//! Waits for replication to fully drain (every probed table caught up to the source),
//! then asserts that source (Postgres) and Spice row counts match for each table.

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Array, AsArray};
use arrow::datatypes::Int64Type;
use chbench_driver::ChBenchDriver;
use futures::TryStreamExt;
use test_framework::anyhow;
use test_framework::opentelemetry::KeyValue;
use tokio::time::sleep;

use super::staleness::query_max_bench_ts_spice;

/// Per-table source-vs-Spice row-count comparison.
#[derive(Debug, Clone)]
pub struct TableCorrectness {
    pub table: String,
    pub source_count: i64,
    pub spice_count: i64,
}

impl TableCorrectness {
    fn matched(&self) -> bool {
        self.source_count == self.spice_count
    }
}

/// Final data-correctness report produced after replication drains.
#[derive(Debug)]
pub struct CorrectnessReport {
    /// Per-table final row-count comparison.
    pub tables: Vec<TableCorrectness>,
    /// Time at which replication fully converged, or `None` if it did not
    /// converge within the wait bound.
    pub converged_at: Option<Duration>,
    /// How long the drain wait took.
    pub wait_duration: Duration,
}

impl CorrectnessReport {
    /// Print a human-readable correctness summary and record OTEL metrics.
    pub fn emit(&self) {
        println!("\nData Correctness");
        if let Some(converged_at) = self.converged_at {
            println!("  replication converged in {}ms", converged_at.as_millis());
        } else {
            println!(
                "  replication DID NOT converge within {}ms",
                self.wait_duration.as_millis()
            );
        }
        println!(
            "  {:<14} {:>14} {:>14} {:>8}",
            "dataset", "source_count", "spice_count", "match"
        );

        let mut mismatches: u64 = 0;
        for t in &self.tables {
            let matched = t.matched();
            if !matched {
                mismatches += 1;
            }
            println!(
                "  {:<14} {:>14} {:>14} {:>8}",
                t.table,
                t.source_count,
                t.spice_count,
                if matched { "ok" } else { "MISMATCH" },
            );
            crate::metrics::ROW_COUNT.record(
                t.spice_count.max(0).cast_unsigned(),
                &[KeyValue::new("dataset", t.table.clone())],
            );
        }

        let failed = mismatches + u64::from(self.converged_at.is_none());
        crate::metrics::CORRECTNESS_ROUNDS_TOTAL.record(1, &[]);
        crate::metrics::CORRECTNESS_ROUNDS_PASSED.record(u64::from(failed == 0), &[]);
        crate::metrics::CORRECTNESS_ROUNDS_FAILED.record(u64::from(failed != 0), &[]);

        if failed == 0 {
            println!("  verdict: PASSED — all {} tables match", self.tables.len());
        } else {
            println!(
                "  verdict: FAILED — {mismatches} table(s) mismatched{}",
                if self.converged_at.is_some() {
                    String::new()
                } else {
                    ", replication did not converge".to_string()
                }
            );
        }
    }

    /// Returns a failure message if replication did not converge or any table's
    /// row counts disagree. `None` means the gate passed.
    pub fn failure_message(&self) -> Option<String> {
        let mut problems = Vec::new();
        if self.converged_at.is_none() {
            problems.push(format!(
                "replication did not converge within {}ms",
                self.wait_duration.as_millis()
            ));
        }
        for t in &self.tables {
            if !t.matched() {
                problems.push(format!(
                    "{}: source={} spice={} (diff {})",
                    t.table,
                    t.source_count,
                    t.spice_count,
                    t.source_count - t.spice_count,
                ));
            }
        }
        if problems.is_empty() {
            None
        } else {
            Some(format!(
                "HTAP data-correctness gate failed:\n  {}",
                problems.join("\n  ")
            ))
        }
    }
}

/// Wait for replication to fully drain, then snapshot final source/Spice counts.
///
/// Polls each table until both `MAX(_bench_ts)` and `COUNT(*)` agree between the
/// source and Spice, bounded by `max_wait`. After the wait (converged or timed
/// out) a final count comparison is taken for the report.
pub async fn verify_after_drain(
    driver: Arc<dyn ChBenchDriver>,
    spice_client: &spiceai::Client,
    tables: &[String],
    max_wait: Duration,
) -> anyhow::Result<CorrectnessReport> {
    let poll = Duration::from_secs(1);
    let start = Instant::now();
    let deadline = start + max_wait;

    println!(
        "\nWaiting up to {}s for replication to drain...",
        max_wait.as_secs()
    );

    let converged_at = loop {
        let mut all_caught_up = true;
        for table in tables {
            let (src_ts, spice_ts, src_n, spice_n) = tokio::join!(
                driver.max_bench_ts(table),
                query_max_bench_ts_spice(spice_client, table),
                driver.row_count(table),
                query_count_spice(spice_client, table),
            );

            // A transient error (e.g. a momentary connection blip while replication is still draining) should not fail
            // the whole gate — treat it as "not caught up" and keep polling until max_wait.
            match (src_ts, spice_ts, src_n, spice_n) {
                (Ok(src_ts), Ok(spice_ts), Ok(src_n), Ok(spice_n)) => {
                    if !(src_ts == spice_ts && src_n == spice_n) {
                        all_caught_up = false;
                    }
                }
                (src_ts, spice_ts, src_n, spice_n) => {
                    all_caught_up = false;
                    eprintln!(
                        "Data correctness probe: {table} src_ts={src_ts:?} spice_ts={spice_ts:?} src_count={src_n:?} spice_count={spice_n:?}"
                    );
                }
            }
        }

        if all_caught_up {
            break Some(start.elapsed());
        }
        if Instant::now() >= deadline {
            break None;
        }
        sleep(poll).await;
    };

    // Final count snapshot for the report.
    let mut table_results = Vec::with_capacity(tables.len());
    for table in tables {
        let (source_count, spice_count) = tokio::join!(
            driver.row_count(table),
            query_count_spice(spice_client, table)
        );
        table_results.push(TableCorrectness {
            table: table.clone(),
            source_count: source_count?,
            spice_count: spice_count?,
        });
    }

    Ok(CorrectnessReport {
        tables: table_results,
        converged_at,
        wait_duration: start.elapsed(),
    })
}

/// Query `COUNT(*)` from Spice via Flight SQL.
async fn query_count_spice(client: &spiceai::Client, table: &str) -> anyhow::Result<i64> {
    let query = format!("SELECT COUNT(*) FROM {table}");
    let mut stream = client.sql(&query).await?;

    while let Some(batch) = stream.try_next().await? {
        if batch.num_rows() == 0 {
            continue;
        }
        let col = batch
            .column(0)
            .as_primitive_opt::<Int64Type>()
            .ok_or_else(|| anyhow::anyhow!("unexpected array type for COUNT(*) on {table}"))?;
        if !col.is_null(0) {
            return Ok(col.value(0));
        }
    }

    Ok(0)
}
