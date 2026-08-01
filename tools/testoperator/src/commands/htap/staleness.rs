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
    pub p99: Duration,
    pub max: Duration,
    pub samples: u64,
    /// Samples discarded as absurd (> the reasonable-gap cap). Surfaced next to the
    /// percentiles so a legitimately pathological run isn't silently trimmed — a
    /// high discard count means the cap may be hiding real staleness, not just
    /// bootstrap/catch-up noise.
    pub discarded: u64,
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
    /// Worst-case max across all tables.
    pub worst_max: Duration,
}

impl StalenessReport {
    /// Print a human-readable data freshness summary and record OTEL metrics.
    pub fn emit(&self) {
        println!("\nData Freshness");
        println!(
            "  {:<14} {:>10} {:>10} {:>10}",
            "dataset", "p99_ms", "max_ms", "samples"
        );
        for table in &self.probe_tables {
            if let Some(stats) = self.tables.get(table.as_str()) {
                let discarded = if stats.samples == 0 && stats.discarded > 0 {
                    format!(
                        "  (all {} samples > cap; p99/max floored at the cap)",
                        stats.discarded
                    )
                } else if stats.discarded > 0 {
                    format!("  ({} discarded > cap)", stats.discarded)
                } else {
                    String::new()
                };
                println!(
                    "  {:<14} {:>10} {:>10} {:>10}{discarded}",
                    table,
                    stats.p99.as_millis(),
                    stats.max.as_millis(),
                    stats.samples,
                );
                let attributes = [KeyValue::new("dataset", table.clone())];
                #[expect(clippy::cast_precision_loss)]
                let p99_ms = stats.p99.as_millis() as f64;
                #[expect(clippy::cast_precision_loss)]
                let max_ms = stats.max.as_millis() as f64;
                crate::metrics::DATA_FRESHNESS_P99.record(p99_ms, &attributes);
                crate::metrics::DATA_FRESHNESS_MAX.record(max_ms, &attributes);
            }
        }
        println!("  worst P99: {}ms", self.worst_p99.as_millis());
        println!("  worst max: {}ms", self.worst_max.as_millis());
        #[expect(clippy::cast_precision_loss)]
        let worst_p99_ms = self.worst_p99.as_millis() as f64;
        #[expect(clippy::cast_precision_loss)]
        let worst_max_ms = self.worst_max.as_millis() as f64;
        crate::metrics::DATA_FRESHNESS_P99.record(worst_p99_ms, &[]);
        crate::metrics::DATA_FRESHNESS_MAX.record(worst_max_ms, &[]);
    }
}

/// Spawn a background task that probes staleness until cancelled.
///
/// Returns a `JoinHandle` that resolves to a `StalenessReport`.
pub fn spawn_staleness_probe(
    driver: Arc<dyn ChBenchDriver>,
    spice_client: spiceai::Client,
    cancel: CancellationToken,
    max_reasonable_gap: Duration,
) -> tokio::task::JoinHandle<anyhow::Result<StalenessReport>> {
    tokio::spawn(async move {
        run_staleness_probe(driver, spice_client, cancel, max_reasonable_gap).await
    })
}

/// Core probe loop. Runs until cancelled, collecting gap samples for each table.
async fn run_staleness_probe(
    driver: Arc<dyn ChBenchDriver>,
    spice_client: spiceai::Client,
    cancel: CancellationToken,
    max_reasonable_gap: Duration,
) -> anyhow::Result<StalenessReport> {
    let poll_interval = Duration::from_secs(5);
    let probe_tables = driver.probe_tables();

    // Per-table gap samples (microseconds).
    let mut samples: HashMap<String, Vec<i64>> = probe_tables
        .iter()
        .map(|t| ((*t).to_string(), Vec::new()))
        .collect();
    // Per-table count of samples discarded as absurd (surfaced next to percentiles).
    let mut discarded: HashMap<String, u64> = HashMap::new();

    // Ordered list of table names for consistent report output.
    let probe_table_names: Vec<String> = probe_tables.iter().map(|t| (*t).to_string()).collect();

    // Let CDC apply the first post-snapshot batches before sampling: the
    // earliest gaps otherwise measure bootstrap catch-up rather than
    // steady-state replication lag and land in the discard cap anyway.
    tokio::time::sleep(Duration::from_secs(30)).await;

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
                    // Discard absurd samples (e.g. a bootstrap-era catch-up reading)
                    // so a single outlier can't dominate p99/max on a short window.
                    if u128::try_from(gap_us).unwrap_or(0) > max_reasonable_gap.as_micros() {
                        *discarded.entry((*table).to_string()).or_insert(0) += 1;
                        eprintln!(
                            "Staleness probe: dropping absurd {table} freshness sample \
                             {}ms (> {}ms cap; likely bootstrap/catch-up)",
                            gap_us / 1_000,
                            max_reasonable_gap.as_millis(),
                        );
                    } else if let Some(table_samples) = samples.get_mut(*table) {
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

    Ok(build_report(
        samples,
        &discarded,
        probe_table_names,
        max_reasonable_gap,
    ))
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
///
/// `discard_cap` is the absurd-gap threshold the probe filtered with. A table
/// whose every sample exceeded it must not read as fresh: reporting it as
/// zero-with-zero-samples excludes it from the worst-P99 fold, so the aggregate
/// IMPROVES as the table falls further behind. Such a table's p99/max are
/// floored at the cap - the tightest bound the surviving evidence supports -
/// and folded into the worst figures. A table with no samples and no discards
/// (never seeded, no traffic) stays at zero: nothing was observed, and
/// inventing a floor there would be as dishonest as the zero was here.
fn build_report(
    samples: HashMap<String, Vec<i64>>,
    discarded: &HashMap<String, u64>,
    probe_tables: Vec<String>,
    discard_cap: Duration,
) -> StalenessReport {
    let mut tables = HashMap::new();
    let mut worst_p99 = Duration::ZERO;
    let mut worst_max = Duration::ZERO;

    for (table, mut gaps) in samples {
        let n_discarded = discarded.get(&table).copied().unwrap_or(0);
        if gaps.is_empty() {
            let floor = if n_discarded > 0 {
                discard_cap
            } else {
                Duration::ZERO
            };
            if floor > worst_p99 {
                worst_p99 = floor;
            }
            if floor > worst_max {
                worst_max = floor;
            }
            tables.insert(
                table,
                StalenessStats {
                    p99: floor,
                    max: floor,
                    samples: 0,
                    discarded: n_discarded,
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

        let p99 = Duration::from_micros(gaps[pct(0.99)].cast_unsigned());
        let max = Duration::from_micros(gaps[n - 1].cast_unsigned());

        if p99 > worst_p99 {
            worst_p99 = p99;
        }
        if max > worst_max {
            worst_max = max;
        }

        tables.insert(
            table,
            StalenessStats {
                p99,
                max,
                samples: n as u64,
                discarded: n_discarded,
            },
        );
    }

    StalenessReport {
        tables,
        probe_tables,
        worst_p99,
        worst_max,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn report(
        samples: &[(&str, &[i64])],
        discarded: &[(&str, u64)],
        cap: Duration,
    ) -> StalenessReport {
        let samples: HashMap<String, Vec<i64>> = samples
            .iter()
            .map(|(t, g)| ((*t).to_string(), g.to_vec()))
            .collect();
        let discarded: HashMap<String, u64> = discarded
            .iter()
            .map(|(t, n)| ((*t).to_string(), *n))
            .collect();
        let tables = samples.keys().cloned().collect();
        build_report(samples, &discarded, tables, cap)
    }

    #[test]
    fn a_fully_censored_table_floors_worst_p99_at_the_cap() {
        // Regression: a run whose order_line had 51 of 51 samples discarded
        // reported worst P99 over the OTHER six tables - the aggregate improved
        // as the slowest table got worse.
        let cap = Duration::from_mins(30);
        let r = report(
            &[("customer", &[10_000_000]), ("order_line", &[])],
            &[("order_line", 51)],
            cap,
        );
        assert_eq!(r.worst_p99, cap, "the censored table must set the floor");
        assert_eq!(r.worst_max, cap);
        let ol = &r.tables["order_line"];
        assert_eq!(ol.p99, cap);
        assert_eq!(ol.samples, 0, "flooring must not invent samples");
        assert_eq!(ol.discarded, 51);
    }

    #[test]
    fn a_table_with_no_samples_and_no_discards_stays_at_zero() {
        // No traffic observed is an unknown, not a lag of cap: inventing a
        // floor there would be as wrong as the zero was for censored tables.
        let r = report(
            &[("customer", &[5_000_000]), ("idle_table", &[])],
            &[],
            Duration::from_mins(30),
        );
        assert_eq!(r.tables["idle_table"].p99, Duration::ZERO);
        assert_eq!(r.worst_p99, Duration::from_secs(5));
    }

    #[test]
    fn a_partially_censored_table_keeps_its_measured_percentiles() {
        // Kept samples still carry the p99; the discard count is reported
        // alongside rather than replacing the measurement.
        let cap = Duration::from_mins(30);
        let r = report(&[("stock", &[1_000_000, 2_000_000])], &[("stock", 3)], cap);
        let stock = &r.tables["stock"];
        assert_eq!(stock.samples, 2);
        assert_eq!(stock.discarded, 3);
        assert!(
            stock.p99 < cap,
            "measured p99 must not be replaced by the cap"
        );
    }
}
