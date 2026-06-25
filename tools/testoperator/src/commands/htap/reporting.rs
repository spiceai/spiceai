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

//! Reporting helpers for the HTAP command: parse the metrics scraped from
//! spiced's `/metrics` endpoint and emit per-run summaries (printed tables +
//! OpenTelemetry gauges). Replication metrics are accelerator-agnostic; the
//! Cayenne sections are emitted only when the corresponding `cayenne_*` series
//! are present, so a non-Cayenne accelerator (e.g. `DuckDB`) cleanly skips them.

use test_framework::opentelemetry::KeyValue;

/// Emits replication metrics scraped from spiced's `/metrics` endpoint.
///
/// `phase` labels the scrape context (e.g. "under load", "post-drain re-scrape").
/// `record_telemetry` controls whether the values are recorded to OpenTelemetry —
/// only the primary under-load scrape should be recorded so diagnostic re-scrapes
/// don't overwrite the headline lag metric.
pub(super) fn emit_replication_metrics(
    metrics: &crate::spiced_metrics::SpicedMetrics,
    phase: &str,
    record_telemetry: bool,
) {
    use std::collections::{BTreeMap, BTreeSet};

    // Collect replication metrics per dataset from scraped samples.
    // Gauges (lag_ms, lag_bytes): use the last observed value — represents the
    // pipeline state when the scraper stopped (while OLTP was still active).
    // Counters (inserts, updates, deletes): use the last value (monotonic total).
    let mut lag_ms: BTreeMap<String, f64> = BTreeMap::new();
    let mut lag_bytes: BTreeMap<String, f64> = BTreeMap::new();
    let mut inserts: BTreeMap<String, f64> = BTreeMap::new();
    let mut updates: BTreeMap<String, f64> = BTreeMap::new();
    let mut deletes: BTreeMap<String, f64> = BTreeMap::new();
    let mut recv_errors: BTreeMap<String, f64> = BTreeMap::new();
    let mut reconnects: BTreeMap<String, f64> = BTreeMap::new();

    let gauge_metrics = [
        (
            "dataset_postgres_replication_lag_ms",
            &mut lag_ms as &mut BTreeMap<String, f64>,
        ),
        ("dataset_postgres_replication_lag_bytes", &mut lag_bytes),
    ];
    let counter_metrics = [
        (
            "dataset_postgres_replication_inserts_total",
            &mut inserts as &mut BTreeMap<String, f64>,
        ),
        ("dataset_postgres_replication_updates_total", &mut updates),
        ("dataset_postgres_replication_deletes_total", &mut deletes),
        (
            "dataset_postgres_replication_recv_errors_total",
            &mut recv_errors,
        ),
        (
            "dataset_postgres_replication_reconnects_total",
            &mut reconnects,
        ),
    ];

    for (metric_name, map) in gauge_metrics {
        if let Some(samples) = metrics.samples.get(metric_name) {
            for sample in samples {
                let dataset = sample
                    .labels
                    .get("name")
                    .cloned()
                    .unwrap_or_else(|| "unknown".to_string());
                // Gauge: last value wins (overwrites previous).
                map.insert(dataset, sample.value);
            }
        }
    }

    for (metric_name, map) in counter_metrics {
        if let Some(samples) = metrics.samples.get(metric_name) {
            for sample in samples {
                let dataset = sample
                    .labels
                    .get("name")
                    .cloned()
                    .unwrap_or_else(|| "unknown".to_string());
                // Counter: last observed value is the total.
                map.insert(dataset, sample.value);
            }
        }
    }

    if lag_ms.is_empty()
        && lag_bytes.is_empty()
        && inserts.is_empty()
        && updates.is_empty()
        && deletes.is_empty()
        && recv_errors.is_empty()
        && reconnects.is_empty()
    {
        return;
    }

    println!("\nReplication Metrics ({phase})");
    // Header
    println!(
        "  {:<14} {:>10} {:>12} {:>10} {:>10} {:>10} {:>10} {:>10}",
        "dataset",
        "lag_ms",
        "lag_bytes",
        "inserts",
        "updates",
        "deletes",
        "recv_errs",
        "reconnects"
    );

    let all_datasets: BTreeSet<&String> = lag_ms
        .keys()
        .chain(lag_bytes.keys())
        .chain(inserts.keys())
        .chain(updates.keys())
        .chain(deletes.keys())
        .chain(recv_errors.keys())
        .chain(reconnects.keys())
        .collect();

    let mut worst_lag_ms: f64 = 0.0;
    for dataset in &all_datasets {
        let l_ms = lag_ms.get(*dataset).copied().unwrap_or(0.0);
        let l_bytes = lag_bytes.get(*dataset).copied().unwrap_or(0.0);
        let ins = inserts.get(*dataset).copied().unwrap_or(0.0);
        let upd = updates.get(*dataset).copied().unwrap_or(0.0);
        let del = deletes.get(*dataset).copied().unwrap_or(0.0);
        let recv = recv_errors.get(*dataset).copied().unwrap_or(0.0);
        let reconn = reconnects.get(*dataset).copied().unwrap_or(0.0);
        println!(
            "  {dataset:<14} {l_ms:>10.0} {l_bytes:>12.0} {ins:>10.0} {upd:>10.0} {del:>10.0} {recv:>10.0} {reconn:>10.0}",
        );

        if record_telemetry {
            crate::metrics::REPLICATION_LAG_MS
                .record(l_ms, &[KeyValue::new("dataset", (*dataset).clone())]);
        }
        if l_ms > worst_lag_ms {
            worst_lag_ms = l_ms;
        }
    }
    println!();

    // Headline: worst replication lag across all datasets.
    if record_telemetry {
        crate::metrics::REPLICATION_LAG_MS.record(worst_lag_ms, &[]);
    }
}

/// Emits the p90 and p99 of Cayenne read amplification (`cayenne_ingest_read_amp`)
/// per table, computed from the gauge time series the background scraper collected
/// while the OLTP load was running.
pub(super) fn emit_cayenne_read_amp_percentiles(metrics: &crate::spiced_metrics::SpicedMetrics) {
    use std::collections::BTreeMap;

    let Some(samples) = metrics.samples.get("cayenne_ingest_read_amp") else {
        return;
    };

    let mut per_table: BTreeMap<String, Vec<f64>> = BTreeMap::new();
    for sample in samples {
        if sample.value.is_nan() {
            continue;
        }
        let table = sample
            .labels
            .get("table")
            .or_else(|| sample.labels.get("name"))
            .cloned()
            .unwrap_or_else(|| "unknown".to_string());
        per_table.entry(table).or_default().push(sample.value);
    }

    if per_table.is_empty() {
        return;
    }

    println!("\nCayenne Read Amplification (under load)");
    println!("  {:<20} {:>8} {:>8} {:>8}", "table", "p90", "p99", "max");
    for (table, mut values) in per_table {
        values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let p90 = percentile(&values, 0.90);
        let p99 = percentile(&values, 0.99);
        let max = *values.last().unwrap_or(&0.0);
        println!("  {table:<20} {p90:>8.0} {p99:>8.0} {max:>8.0}");

        let attributes = [KeyValue::new("table", table)];
        crate::metrics::CAYENNE_INGEST_READ_AMP_P90.record(p90, &attributes);
        crate::metrics::CAYENNE_INGEST_READ_AMP_P99.record(p99, &attributes);
    }
    println!();
}

/// Emits Cayenne compaction metrics scraped from spiced's `/metrics` endpoint,
/// reported per `table` and compaction `kind`
pub(super) fn emit_cayenne_compaction_metrics(metrics: &crate::spiced_metrics::SpicedMetrics) {
    use std::collections::{BTreeMap, BTreeSet};

    // Counter / histogram count+sum series are cumulative, so the latest value of
    // each distinct label-set is its max. Collapse to (table, kind), summing the
    // `result` dimension (e.g. completed + failed passes) — the native granularity
    // cayenne exposes, with no cross-table aggregation.
    let latest_per_table_kind = |name: &str| -> BTreeMap<(String, String), f64> {
        // Full label set identifies a series; track its max (= latest cumulative).
        let mut series_max: BTreeMap<String, (String, String, f64)> = BTreeMap::new();
        if let Some(samples) = metrics.samples.get(name) {
            for sample in samples {
                let table = sample
                    .labels
                    .get("table")
                    .cloned()
                    .unwrap_or_else(|| "unknown".to_string());
                let kind = sample
                    .labels
                    .get("kind")
                    .cloned()
                    .unwrap_or_else(|| "unknown".to_string());
                let mut fingerprint: Vec<String> = sample
                    .labels
                    .iter()
                    .map(|(k, v)| format!("{k}={v}"))
                    .collect();
                fingerprint.sort();
                let entry =
                    series_max
                        .entry(fingerprint.join(","))
                        .or_insert((table, kind, f64::MIN));
                if sample.value > entry.2 {
                    entry.2 = sample.value;
                }
            }
        }
        let mut out: BTreeMap<(String, String), f64> = BTreeMap::new();
        for (_series, (table, kind, value)) in series_max {
            *out.entry((table, kind)).or_insert(0.0) += value;
        }
        out
    };

    let passes = latest_per_table_kind("cayenne_compaction_duration_ms_count");
    let merged_bytes = latest_per_table_kind("cayenne_compaction_merged_bytes_sum");

    // p90/p99 pass duration per (table, kind), via Prometheus-style
    // histogram_quantile over the cumulative `_bucket` series (summing the
    // `result` dimension). A single end scrape suffices — the histogram is
    // cumulative for the run.
    let duration_pcts = compaction_duration_percentiles_per_table_kind(metrics);

    if passes.is_empty() && merged_bytes.is_empty() {
        return;
    }

    let all_keys: BTreeSet<&(String, String)> = passes.keys().chain(merged_bytes.keys()).collect();

    println!("\nCayenne Compaction Metrics");
    println!(
        "  {:<20} {:<8} {:>8} {:>12} {:>12} {:>16}",
        "table", "kind", "passes", "dur_p90_ms", "dur_p99_ms", "merged_bytes"
    );
    for key in &all_keys {
        let (table, kind) = (&key.0, &key.1);
        let p = passes.get(*key).copied().unwrap_or(0.0);
        let b = merged_bytes.get(*key).copied().unwrap_or(0.0);
        let pcts = duration_pcts.get(*key).copied();
        let (p90_display, p99_display) = pcts.unwrap_or((0.0, 0.0));
        println!(
            "  {table:<20} {kind:<8} {p:>8.0} {p90_display:>12.1} {p99_display:>12.1} {b:>16.0}"
        );

        let attributes = [
            KeyValue::new("table", table.clone()),
            KeyValue::new("kind", kind.clone()),
        ];
        crate::metrics::CAYENNE_COMPACTION_PASSES.record(to_u64(p), &attributes);
        if let Some((p90, p99)) = pcts {
            crate::metrics::CAYENNE_COMPACTION_DURATION_P90_MS.record(p90, &attributes);
            crate::metrics::CAYENNE_COMPACTION_DURATION_P99_MS.record(p99, &attributes);
        }
        // merged_bytes is only populated for kinds that emit the merged-bytes
        // histogram (currently `subset`); skip recording a misleading zero for
        // series that never report it.
        if merged_bytes.contains_key(*key) {
            crate::metrics::CAYENNE_COMPACTION_MERGED_BYTES.record(to_u64(b), &attributes);
        }
    }
    println!();
}

/// Computes the p90 and p99 Cayenne compaction-pass duration (ms) per
/// `(table, kind)` from the cumulative `cayenne_compaction_duration_ms_bucket`
fn compaction_duration_percentiles_per_table_kind(
    metrics: &crate::spiced_metrics::SpicedMetrics,
) -> std::collections::BTreeMap<(String, String), (f64, f64)> {
    use std::collections::BTreeMap;

    // (table, kind) -> (le -> cumulative count), summed across `result`.
    let mut buckets: BTreeMap<(String, String), BTreeMap<u64, f64>> = BTreeMap::new();
    if let Some(samples) = metrics.samples.get("cayenne_compaction_duration_ms_bucket") {
        for sample in samples {
            let Some(le_raw) = sample.labels.get("le") else {
                continue;
            };
            let le = if le_raw.eq_ignore_ascii_case("+inf") || le_raw.eq_ignore_ascii_case("inf") {
                f64::INFINITY
            } else {
                match le_raw.parse::<f64>() {
                    Ok(v) => v,
                    Err(_) => continue,
                }
            };
            let table = sample
                .labels
                .get("table")
                .cloned()
                .unwrap_or_else(|| "unknown".to_string());
            let kind = sample
                .labels
                .get("kind")
                .cloned()
                .unwrap_or_else(|| "unknown".to_string());
            // Bucket boundaries are stable integers in this histogram; key on the
            // bit pattern so +Inf sorts last and identical boundaries coalesce.
            let le_key = le.to_bits();
            *buckets
                .entry((table, kind))
                .or_default()
                .entry(le_key)
                .or_insert(0.0) += sample.value;
        }
    }

    let mut out: BTreeMap<(String, String), (f64, f64)> = BTreeMap::new();
    for (key, le_counts) in buckets {
        // Sort boundaries ascending (f64::INFINITY bits sort above finite values).
        let mut bounds: Vec<(f64, f64)> = le_counts
            .into_iter()
            .map(|(bits, count)| (f64::from_bits(bits), count))
            .collect();
        bounds.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));

        if bounds.last().is_none_or(|&(_, total)| total <= 0.0) {
            continue;
        }
        out.insert(
            key,
            (
                histogram_quantile(&bounds, 0.90),
                histogram_quantile(&bounds, 0.99),
            ),
        );
    }
    out
}

/// Nearest-rank percentile `q` (0.0–1.0) of a sorted, non-empty slice.
#[expect(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
fn percentile(sorted: &[f64], q: f64) -> f64 {
    let idx = (((sorted.len() as f64) * q).ceil() as usize)
        .saturating_sub(1)
        .min(sorted.len() - 1);
    sorted[idx]
}

/// Clamps a non-negative float metric value to `u64` for gauge recording.
#[expect(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn to_u64(value: f64) -> u64 {
    if value <= 0.0 { 0 } else { value as u64 }
}

/// Standard Prometheus `histogram_quantile` over ascending `(le, cumulative)`
/// bounds; `bounds.last()` is the +Inf bucket whose count is the total.
fn histogram_quantile(bounds: &[(f64, f64)], q: f64) -> f64 {
    let total = bounds.last().map_or(0.0, |&(_, c)| c);
    if total <= 0.0 {
        return 0.0;
    }
    let rank = q * total;
    let mut lower_le = 0.0_f64;
    let mut lower_cum = 0.0_f64;
    for &(le, cum) in bounds {
        if cum >= rank {
            if le.is_infinite() {
                // The quantile sits in the open-ended top bucket — report the
                // highest finite boundary (Prometheus convention).
                return lower_le;
            } else if cum > lower_cum {
                return lower_le + (le - lower_le) * (rank - lower_cum) / (cum - lower_cum);
            }
            return le;
        }
        lower_le = le;
        lower_cum = cum;
    }
    bounds.last().map_or(0.0, |&(le, _)| le)
}
