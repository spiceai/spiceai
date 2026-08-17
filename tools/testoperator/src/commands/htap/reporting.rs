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

use test_framework::anyhow::{self, Context};
use test_framework::opentelemetry::KeyValue;

use crate::stats::percentile;

/// Slack (bytes) below which authoritative per-slot retained WAL counts as "drained"
/// — ~1 WAL segment of padding, since `confirmed_flush_lsn` trails the WAL head by up
/// to a segment even when fully caught up.
const CAUGHT_UP_WAL_EPSILON: f64 = 16.0 * 1024.0 * 1024.0;

/// Headline replication-lag figures across all datasets — the worst (max over
/// datasets) of the last-observed lag, its P99, and its max over the under-load
/// window. Returned by [`emit_replication_metrics`] so the run summary can surface
/// the single worst-lag number without re-parsing the scraped series.
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct ReplicationLagSummary {
    /// Worst (across datasets) last-observed lag, in milliseconds.
    pub last: f64,
    /// Worst (across datasets) P99 lag over the under-load window, in milliseconds.
    pub p99: f64,
    /// Worst (across datasets) max lag over the under-load window, in milliseconds.
    pub max: f64,
}

/// Emits replication metrics scraped from spiced's `/metrics` endpoint.
///
/// `phase` labels the scrape context (e.g. "under load", "post-drain re-scrape").
/// `record_telemetry` controls whether the values are recorded to OpenTelemetry —
/// only the primary under-load scrape should be recorded so diagnostic re-scrapes
/// don't overwrite the headline lag metric.
///
/// Returns the headline worst-lag figures across all datasets, or `None` when the
/// `*_replication_lag_ms` series was not scraped for any dataset — either because no
/// replication metrics were scraped at all (non-Cayenne / no CDC) or because other
/// replication counters were present but the lag gauge was absent. The rest of the
/// replication table still prints in the latter case; only the lag summary is `None`.
pub(super) fn emit_replication_metrics(
    metrics: &crate::spiced_metrics::SpicedMetrics,
    engine: &str,
    pg_stats: &[crate::pg_stats::PgStatSample],
    phase: &str,
    record_telemetry: bool,
) -> Option<ReplicationLagSummary> {
    use std::collections::{BTreeMap, BTreeSet};

    // spiced names replication metrics per source connector, e.g.
    // `dataset_postgres_replication_*` or `dataset_mysql_replication_*`.
    let lag_ms_metric = format!("dataset_{engine}_replication_lag_ms");
    let lag_bytes_metric = format!("dataset_{engine}_replication_lag_bytes");
    let inserts_metric = format!("dataset_{engine}_replication_inserts_total");
    let updates_metric = format!("dataset_{engine}_replication_updates_total");
    let deletes_metric = format!("dataset_{engine}_replication_deletes_total");
    let recv_errors_metric = format!("dataset_{engine}_replication_recv_errors_total");
    let reconnects_metric = format!("dataset_{engine}_replication_reconnects_total");

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
            lag_ms_metric.as_str(),
            &mut lag_ms as &mut BTreeMap<String, f64>,
        ),
        (lag_bytes_metric.as_str(), &mut lag_bytes),
    ];
    let counter_metrics = [
        (
            inserts_metric.as_str(),
            &mut inserts as &mut BTreeMap<String, f64>,
        ),
        (updates_metric.as_str(), &mut updates),
        (deletes_metric.as_str(), &mut deletes),
        (recv_errors_metric.as_str(), &mut recv_errors),
        (reconnects_metric.as_str(), &mut reconnects),
    ];

    for (metric_name, map) in gauge_metrics {
        if let Some(samples) = metrics.samples.get(metric_name) {
            for sample in samples {
                // Skip NaN samples so the map holds the last observed *real* value
                if sample.value.is_nan() {
                    continue;
                }
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

    // Full lag_ms time series per dataset (every scraped sample), used to compute
    // P99 and max over the under-load window. The last-value gauge above only
    // captures the pipeline state when the scraper stopped, which understates the
    // worst lag seen during the run.
    let mut lag_ms_series: BTreeMap<String, Vec<f64>> = BTreeMap::new();
    if let Some(samples) = metrics.samples.get(lag_ms_metric.as_str()) {
        for sample in samples {
            if sample.value.is_nan() {
                continue;
            }
            let dataset = sample
                .labels
                .get("name")
                .cloned()
                .unwrap_or_else(|| "unknown".to_string());
            lag_ms_series.entry(dataset).or_default().push(sample.value);
        }
    }
    // (p99, max) of the lag_ms series for a dataset, or (0, 0) if no samples.
    let lag_p99_max = |dataset: &String| -> (f64, f64) {
        match lag_ms_series.get(dataset) {
            Some(values) if !values.is_empty() => {
                let mut sorted = values.clone();
                sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                (percentile(&sorted, 0.99), *sorted.last().unwrap_or(&0.0))
            }
            _ => (0.0, 0.0),
        }
    };

    if lag_ms.is_empty()
        && lag_bytes.is_empty()
        && inserts.is_empty()
        && updates.is_empty()
        && deletes.is_empty()
        && recv_errors.is_empty()
        && reconnects.is_empty()
    {
        return None;
    }

    println!("\nReplication Metrics ({phase})");
    // Header
    println!(
        "  {:<14} {:>10} {:>10} {:>10} {:>12} {:>10} {:>10} {:>10} {:>10} {:>10}",
        "dataset",
        "lag_ms",
        "lag_p99_ms",
        "lag_max_ms",
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
    let mut worst_lag_p99: f64 = 0.0;
    let mut worst_lag_max: f64 = 0.0;
    for dataset in &all_datasets {
        let l_ms = lag_ms.get(*dataset).copied().unwrap_or(0.0);
        let (l_p99, l_max) = lag_p99_max(dataset);
        let l_bytes = lag_bytes.get(*dataset).copied().unwrap_or(0.0);
        let ins = inserts.get(*dataset).copied().unwrap_or(0.0);
        let upd = updates.get(*dataset).copied().unwrap_or(0.0);
        let del = deletes.get(*dataset).copied().unwrap_or(0.0);
        let recv = recv_errors.get(*dataset).copied().unwrap_or(0.0);
        let reconn = reconnects.get(*dataset).copied().unwrap_or(0.0);
        println!(
            "  {dataset:<14} {l_ms:>10.0} {l_p99:>10.0} {l_max:>10.0} {l_bytes:>12.0} {ins:>10.0} {upd:>10.0} {del:>10.0} {recv:>10.0} {reconn:>10.0}",
        );

        if record_telemetry {
            let dataset_attr = [KeyValue::new("dataset", (*dataset).clone())];
            crate::metrics::REPLICATION_LAG_MS.record(l_ms, &dataset_attr);
            crate::metrics::REPLICATION_LAG_P99_MS.record(l_p99, &dataset_attr);
            crate::metrics::REPLICATION_LAG_MAX_MS.record(l_max, &dataset_attr);
            // Persist the WAL backlog in bytes — the source-side backpressure
            // signal (previously printed only).
            crate::metrics::REPLICATION_LAG_BYTES.record(l_bytes, &dataset_attr);
        }
        if l_ms > worst_lag_ms {
            worst_lag_ms = l_ms;
        }
        if l_p99 > worst_lag_p99 {
            worst_lag_p99 = l_p99;
        }
        if l_max > worst_lag_max {
            worst_lag_max = l_max;
        }
    }

    // Caught-up interpretation: `lag_ms` is wall-clock now minus the last applied commit
    // watermark, so on an idle/drained stream it grows unbounded even though no WAL is
    // outstanding. Client-view `lag_bytes == 0` (server_wal_end == confirmed_flush) is
    // NOT sufficient — and NOT the safe direction: a fully write-blocked walsender reads
    // client-view 0 (we acked everything we've SEEN) while the source still retains GiB
    // it hasn't shipped. So a dataset is "caught up" only when client `lag_bytes == 0`
    // AND the AUTHORITATIVE per-slot retained WAL (pg_replication_slots, source view) is
    // ~0. The inverse (client 0 but authoritative large) is the write-blocked failure
    // mode — surfaced as a WARNING, not silently called caught-up.
    // Authoritative retained per slot at (nearest to) the scrape: take the LAST
    // sample's value, not the window max. `pg_stats` is time-ordered, so the final
    // sample is closest to when the spiced `/metrics` values were read; a max over
    // the whole window would let a transient early spike falsely read as
    // "write-blocked" / not-caught-up at a moment the slot is actually drained.
    let mut slot_retained: BTreeMap<String, i64> = BTreeMap::new();
    for s in pg_stats {
        for (slot, b) in &s.slot_retained_bytes {
            slot_retained.insert(slot.clone(), *b);
        }
    }
    // dataset -> slot from the scraped member_attached gauge labels (shared-slot join).
    let mut ds_slot: BTreeMap<String, String> = BTreeMap::new();
    if let Some(samples) = metrics
        .samples
        .get("dataset_postgres_replication_member_attached")
    {
        for sample in samples {
            if let (Some(name), Some(slot)) = (sample.labels.get("name"), sample.labels.get("slot"))
            {
                ds_slot.insert(name.clone(), slot.clone());
            }
        }
    }
    // Authoritative retained for a dataset's slot; `None` when we lack an
    // authoritative view — either we can't join (older binary / non-shared: no
    // slot label) OR the pg_stats scraper didn't capture that slot (unavailable /
    // failed). We must NOT fabricate a zero here (`unwrap_or(0)`): a missing slot
    // sample would then read as "authoritative retained ~0" and falsely confirm
    // caught-up, when the truth is simply unknown.
    let auth_retained = |d: &str| -> Option<f64> {
        ds_slot
            .get(d)
            .and_then(|slot| slot_retained.get(slot).copied())
            .map(|r| {
                #[expect(
                    clippy::cast_precision_loss,
                    reason = "WAL byte counts compared against a ~16 MiB epsilon; f64 mantissa is ample"
                )]
                let bytes = r as f64;
                bytes
            })
    };
    // Require the lag_bytes series to be PRESENT and 0 — a MISSING series (not
    // scraped/parsed for this dataset) must NOT read as caught-up via unwrap_or(0.0).
    let client_zero = |d: &str| lag_bytes.get(d).is_some_and(|v| *v == 0.0);

    // Client-view says drained but a nonzero (idle) lag_ms stale-watermark remains.
    let client_drained: Vec<&String> = all_datasets
        .iter()
        .copied()
        .filter(|d| client_zero(d) && lag_ms.get(*d).copied().unwrap_or(0.0) > 0.0)
        .collect();
    // Confirmed caught-up: authoritative WAL agrees the slot is drained (~0).
    let caught_up_confirmed: Vec<&str> = client_drained
        .iter()
        .copied()
        .filter(|d| auth_retained(d).is_some_and(|r| r <= CAUGHT_UP_WAL_EPSILON))
        .map(std::string::String::as_str)
        .collect();
    // Client-view only: no authoritative view to confirm (older binary / non-shared
    // slot, or pg_stats unavailable). Reported separately so it is NOT mistaken for
    // authoritatively confirmed — a write-blocked walsender is undetectable here.
    let caught_up_clientonly: Vec<&str> = client_drained
        .iter()
        .copied()
        .filter(|d| auth_retained(d).is_none())
        .map(std::string::String::as_str)
        .collect();
    if !caught_up_confirmed.is_empty() {
        println!(
            "  caught-up (client lag_bytes=0 AND authoritative slot retained ~0 ⇒ all WAL \
             consumed; the nonzero lag_ms is an idle stale-watermark, not real staleness): {}",
            caught_up_confirmed.join(", ")
        );
    }
    if !caught_up_clientonly.is_empty() {
        println!(
            "  likely caught-up, CLIENT-VIEW ONLY (client lag_bytes=0 with nonzero idle lag_ms, \
             but no authoritative slot-retained view to confirm — a write-blocked walsender \
             would be undetectable): {}",
            caught_up_clientonly.join(", ")
        );
    }
    // Dangerous inverse: client-view says drained, but the source still retains WAL far
    // above the padding epsilon — the walsender is write-blocked; do NOT call it caught up.
    let write_blocked: Vec<(&String, f64)> = all_datasets
        .iter()
        .copied()
        .filter_map(|d| auth_retained(d).map(|r| (d, r)))
        .filter(|(d, r)| client_zero(d) && *r > CAUGHT_UP_WAL_EPSILON)
        .collect();
    for (d, r) in &write_blocked {
        println!(
            "  WARNING: {d} reads client lag_bytes=0 but authoritative slot retains {:.1} GiB \
             — walsender write-blocked, NOT caught up (client-view zero is unsafe here).",
            r / (1024.0 * 1024.0 * 1024.0)
        );
    }
    println!();

    // The lag figures are meaningful only if the `*_replication_lag_ms` series was
    // actually scraped. When it is absent (e.g. counters present but no lag gauge),
    // `worst_lag_*` stay at 0 — reporting that as "0.00 s lag" would falsely read as
    // perfect replication, so record no headline lag telemetry and return no summary.
    if lag_ms.is_empty() && lag_ms_series.is_empty() {
        return None;
    }

    // Headline: worst replication lag across all datasets (last value, P99, and max).
    if record_telemetry {
        crate::metrics::REPLICATION_LAG_MS.record(worst_lag_ms, &[]);
        crate::metrics::REPLICATION_LAG_P99_MS.record(worst_lag_p99, &[]);
        crate::metrics::REPLICATION_LAG_MAX_MS.record(worst_lag_max, &[]);
    }

    Some(ReplicationLagSummary {
        last: worst_lag_ms,
        p99: worst_lag_p99,
        max: worst_lag_max,
    })
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

/// Which path an operation runs on. Reported alongside the count, because a
/// count only means something once you know whether it scales with the workload:
/// `Write` and `Read` counts rise with the changes applied and the queries
/// served, while `Background` counts are the accelerator's own housekeeping.
/// Summing them into one "maintenance" figure would report a run that merely
/// applied more changes as having done more housekeeping.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum OperationPath {
    Background,
    Write,
    Read,
}

impl OperationPath {
    fn label(self) -> &'static str {
        match self {
            OperationPath::Background => "background",
            OperationPath::Write => "write",
            OperationPath::Read => "read",
        }
    }
}

/// Instrumented Cayenne operations, as `(reported name, scraped series, path)`.
/// Each series is cumulative for the run, so its end value is how many times that
/// operation ran.
///
/// The path of each was read off its emit site, not inferred from its name:
/// `cayenne_write_phase_duration_ms` comes from `record_cayenne_write_phase` on
/// the write path, the inline-cache populates come from `read_inlined_batches` /
/// `cached_inlined_view` on the scan path, and the inline tombstone writes come
/// from the on-conflict apply.
///
/// Compaction passes are deliberately absent: they carry `table`/`kind` labels
/// worth keeping apart, so [`emit_cayenne_compaction_metrics`] reports them at
/// that granularity instead of collapsing them to one number here.
///
/// A histogram contributes its `_count` series; a counter is already a count.
/// Everything here is an operation *count*, so a quantity in other units (e.g.
/// `cayenne_metastore_incremental_vacuum_pages_total`, which counts pages rather
/// than vacuums) does not belong.
const CAYENNE_OPERATION_SERIES: &[(&str, &str, OperationPath)] = &[
    (
        "mem_tier_checkpoint_ticks",
        "cayenne_mem_tier_checkpoint_tick_total",
        OperationPath::Background,
    ),
    (
        "metastore_checkpoints",
        "cayenne_metastore_checkpoint_ms_count",
        OperationPath::Background,
    ),
    (
        "metastore_vacuums",
        "cayenne_metastore_incremental_vacuum_ms_count",
        OperationPath::Background,
    ),
    (
        "autotune_adjustments",
        "cayenne_autotune_adjustments_total",
        OperationPath::Background,
    ),
    (
        "compaction_memory_exhausted",
        "cayenne_compaction_memory_exhausted_total",
        OperationPath::Background,
    ),
    (
        "write_phases",
        "cayenne_write_phase_duration_ms_count",
        OperationPath::Write,
    ),
    (
        "inline_tombstone_writes",
        "cayenne_inline_tombstone_writes_total",
        OperationPath::Write,
    ),
    (
        "mem_tier_reserve_refused",
        "cayenne_mem_tier_reserve_refused_total",
        OperationPath::Write,
    ),
    (
        "inline_cache_full_rebuilds",
        "cayenne_inline_cache_full_rebuilds_total",
        OperationPath::Read,
    ),
    (
        "inline_cache_delta_populates",
        "cayenne_inline_cache_delta_populates_total",
        OperationPath::Read,
    ),
];

/// Sums a cumulative series to its end-of-run total.
///
/// Every sample of a cumulative series is a running total, so the run's value for
/// one label-set is that series' maximum, and the total is the sum across
/// label-sets. Taking the max rather than the last sample keeps this correct
/// whichever order the scrapes are stored in, and a counter reset (a restart)
/// would under- rather than over-count.
///
/// Returns `None` when the series was never scraped, which distinguishes "this
/// build never ran the operation" from "this run had no Cayenne at all" — a
/// `DuckDB` baseline emits none of these.
fn cumulative_series_total(
    metrics: &crate::spiced_metrics::SpicedMetrics,
    name: &str,
) -> Option<f64> {
    use std::collections::BTreeMap;

    let samples = metrics.samples.get(name)?;
    let mut series_max: BTreeMap<String, f64> = BTreeMap::new();
    for sample in samples {
        let mut fingerprint: Vec<String> = sample
            .labels
            .iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect();
        fingerprint.sort();
        let entry = series_max.entry(fingerprint.join(",")).or_insert(f64::MIN);
        if sample.value > *entry {
            *entry = sample.value;
        }
    }
    if series_max.is_empty() {
        return None;
    }
    Some(series_max.values().sum())
}

/// Emits how many times each instrumented Cayenne operation ran during the run,
/// grouped by the path it runs on.
///
/// Compaction passes answer the same question per table and kind
/// ([`emit_cayenne_compaction_metrics`]); this covers the rest — the accelerator's
/// own housekeeping, plus the write- and read-path work that housekeeping competes
/// with — so a throughput or freshness delta between two runs can be attributed to
/// a change in that work rather than inferred.
///
/// The path matters to reading the number and is reported with it: only the
/// `background` counts are housekeeping, while `write` and `read` counts scale
/// with the changes applied and the queries served. The refusal/exhaustion
/// counters are included because they should normally be zero — a non-zero value
/// is back pressure that would otherwise only show up as an unexplained slowdown.
///
/// Silent when nothing was scraped, so a `DuckDB` baseline prints no empty section.
pub(super) fn emit_cayenne_operation_metrics(metrics: &crate::spiced_metrics::SpicedMetrics) {
    let totals: Vec<(&str, OperationPath, f64)> = CAYENNE_OPERATION_SERIES
        .iter()
        .filter_map(|(label, series, path)| {
            cumulative_series_total(metrics, series).map(|total| (*label, *path, total))
        })
        .collect();

    if totals.is_empty() {
        return;
    }

    println!("\nCayenne Operation Counts");
    println!("  {:<30} {:<12} {:>12}", "operation", "path", "count");
    for (label, path, total) in &totals {
        let path_label = path.label();
        println!("  {label:<30} {path_label:<12} {total:>12.0}");
        crate::metrics::CAYENNE_OPERATION_COUNTS.record(
            to_u64(*total),
            &[
                KeyValue::new("operation", *label),
                KeyValue::new("path", path_label),
            ],
        );
    }
    println!();
}

/// Emits Cayenne compaction metrics scraped from spiced's `/metrics` endpoint,
/// reported per `table` and compaction `kind`
///
/// `kind` is whatever Cayenne labels its passes with, so a newly-labelled pass
/// type (e.g. the seq-prefix `bake`) appears here without a change to this code.
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

/// Clamps a non-negative float metric value to `u64` for gauge recording.
#[expect(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn to_u64(value: f64) -> u64 {
    if value <= 0.0 { 0 } else { value as u64 }
}

/// Prints a compact CDC-backpressure summary localizing *where* the apply path
/// stalls, from the series the scraper collected under load. Human-facing (the
/// full time-series is persisted by [`write_metrics_dump`] for the waterfall
/// analysis): per-dataset prefetch-channel occupancy, plus the process-global
/// encode-budget headroom / acquire-wait and compaction acquire-wait, and the
/// in-memory CDC tier occupancy.
/// Per-table apply-phase coverage: `Σ(cayenne_write_phase _sum over phases) ÷
/// cdc_apply_burst_duration _sum`, from the cumulative histogram sums. A LOW ratio
/// means a CDC apply bottleneck sits in un-instrumented code (a blind spot — e.g. a
/// prior run showed one table at 0.8% coverage hiding a real stall). A HIGH ratio
/// (>100%) is normal and not a problem: write phases are non-additive (parallel
/// shards, commit/finalize overlapping the next burst). Returns (table, coverage).
pub(super) fn phase_coverage(metrics: &crate::spiced_metrics::SpicedMetrics) -> Vec<(String, f64)> {
    use std::collections::BTreeMap;
    // Sum the latest cumulative value of each distinct series into a `group`-label bucket.
    let sum_latest_by = |name: &str, group: &str| -> BTreeMap<String, f64> {
        let mut series_latest: BTreeMap<String, (String, i64, f64)> = BTreeMap::new();
        if let Some(samples) = metrics.samples.get(name) {
            for s in samples {
                if s.value.is_nan() {
                    continue;
                }
                let g = s.labels.get(group).cloned().unwrap_or_default();
                let mut fp: Vec<String> =
                    s.labels.iter().map(|(k, v)| format!("{k}={v}")).collect();
                fp.sort();
                let ts = s.ts_ms;
                let e = series_latest
                    .entry(fp.join(","))
                    .or_insert((g.clone(), ts, s.value));
                // Prefer the most recent scrape timestamp; fall back to max when timestamps are absent.
                if (ts != 0 && ts >= e.1) || (ts == 0 && s.value > e.2) {
                    e.0 = g;
                    e.1 = ts;
                    e.2 = s.value;
                }
            }
        }
        let mut out: BTreeMap<String, f64> = BTreeMap::new();
        for (_, (g, _ts, v)) in series_latest {
            if v.is_finite() {
                *out.entry(g).or_insert(0.0) += v;
            }
        }
        out
    };
    let phase_sum = sum_latest_by("cayenne_write_phase_duration_ms_sum", "table");
    let burst_sum = sum_latest_by(
        "dataset_acceleration_cdc_apply_burst_duration_ms_sum",
        "dataset",
    );
    let mut out: Vec<(String, f64)> = burst_sum
        .into_iter()
        .filter(|(_, bsum)| *bsum > 0.0) // skip static/full-refresh tables (no CDC bursts)
        .map(|(table, bsum)| {
            let psum = phase_sum.get(&table).copied().unwrap_or(0.0);
            (table, psum / bsum)
        })
        .collect();
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

/// Print per-table phase coverage and, when `min_coverage > 0`, return the tables
/// under that threshold (the caller decides warn vs. fail). Prints nothing if there
/// are no CDC apply bursts (non-Cayenne / non-changes run).
pub(super) fn emit_phase_coverage(
    metrics: &crate::spiced_metrics::SpicedMetrics,
    min_coverage: f64,
) -> Vec<(String, f64)> {
    let coverage = phase_coverage(metrics);
    if coverage.is_empty() {
        return Vec::new();
    }
    println!(
        "\nApply-phase coverage (Σ write-phase ÷ apply-burst; <85% ⇒ instrumentation blind spot)"
    );
    let mut violations = Vec::new();
    for (table, cov) in &coverage {
        let flag = if *cov < 0.85 { "  <<< BLIND SPOT" } else { "" };
        println!("  {table:<20} {:>6.1}%{flag}", cov * 100.0);
        if min_coverage > 0.0 && *cov < min_coverage {
            violations.push((table.clone(), *cov));
        }
    }
    violations
}

pub(super) fn emit_backpressure_summary(metrics: &crate::spiced_metrics::SpicedMetrics) {
    use std::collections::BTreeMap;

    // Gauge series -> sorted values per label value (keyed by `label`).
    let gauge_series_by_label = |name: &str, label: &str| -> BTreeMap<String, Vec<f64>> {
        let mut per: BTreeMap<String, Vec<f64>> = BTreeMap::new();
        if let Some(samples) = metrics.samples.get(name) {
            for sample in samples {
                if sample.value.is_nan() {
                    continue;
                }
                let key = sample
                    .labels
                    .get(label)
                    .cloned()
                    .unwrap_or_else(|| "-".to_string());
                per.entry(key).or_default().push(sample.value);
            }
        }
        for values in per.values_mut() {
            values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        }
        per
    };

    // p90/p99 of a histogram, aggregating its `_bucket` series across all label
    // sets (process-global valves have low cardinality — class/table — and we want
    // the aggregate stall distribution). Prometheus histogram buckets are cumulative
    // since process start, so we take each (label-set, le) series' LATEST value (its
    // max over scrapes) and only THEN sum across label sets — summing across scrapes
    // would multiply/time-weight earlier snapshots and skew the quantiles.
    let hist_pcts = |name: &str| -> Option<(f64, f64)> {
        let samples = metrics.samples.get(name)?;
        // latest[label-set fingerprint][le] = max cumulative bucket value seen.
        let mut latest: BTreeMap<String, BTreeMap<u64, f64>> = BTreeMap::new();
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
            let mut fp: Vec<String> = sample
                .labels
                .iter()
                .filter(|(k, _)| k.as_str() != "le")
                .map(|(k, v)| format!("{k}={v}"))
                .collect();
            fp.sort();
            let per_le = latest.entry(fp.join(",")).or_default();
            let v = per_le.entry(le.to_bits()).or_insert(0.0);
            *v = v.max(sample.value); // cumulative ⇒ latest == max
        }
        // Sum the latest per-label-set buckets into the aggregate distribution.
        let mut le_counts: BTreeMap<u64, f64> = BTreeMap::new();
        for per_le in latest.into_values() {
            for (le_bits, count) in per_le {
                *le_counts.entry(le_bits).or_insert(0.0) += count;
            }
        }
        let mut bounds: Vec<(f64, f64)> = le_counts
            .into_iter()
            .map(|(bits, count)| (f64::from_bits(bits), count))
            .collect();
        bounds.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        if bounds.last().is_none_or(|&(_, total)| total <= 0.0) {
            return None;
        }
        Some((
            histogram_quantile(&bounds, 0.90),
            histogram_quantile(&bounds, 0.99),
        ))
    };

    let prefetch = gauge_series_by_label(
        "dataset_acceleration_cdc_prefetch_buffer_occupancy",
        "dataset",
    );
    let recv_wait_present = metrics
        .samples
        .contains_key("dataset_acceleration_cdc_source_recv_wait_ms_bucket");
    if prefetch.is_empty()
        && !recv_wait_present
        && !metrics
            .samples
            .contains_key("cayenne_encode_permits_available")
    {
        return; // No CDC/Cayenne backpressure series scraped (non-Cayenne / no CDC).
    }

    println!("\nCDC Backpressure Summary (under load)");

    // Stage 2: prefetch channel occupancy per dataset (near capacity => apply-bound).
    if !prefetch.is_empty() {
        let capacity = gauge_series_by_label(
            "dataset_acceleration_cdc_prefetch_buffer_capacity",
            "dataset",
        );
        println!("  Prefetch channel (occupancy of capacity; high => apply-bound)");
        println!(
            "    {:<16} {:>8} {:>8} {:>8} {:>10}",
            "dataset", "p50", "p99", "max", "capacity"
        );
        for (dataset, values) in &prefetch {
            let p50 = percentile(values, 0.50);
            let p99 = percentile(values, 0.99);
            let max = *values.last().unwrap_or(&0.0);
            let cap = capacity
                .get(dataset)
                .and_then(|v| v.last().copied())
                .unwrap_or(0.0);
            println!("    {dataset:<16} {p50:>8.0} {p99:>8.0} {max:>8.0} {cap:>10.0}");
        }
    }

    // Stage 4a: process-global encode budget headroom + acquire-wait.
    let encode_avail = gauge_series_by_label("cayenne_encode_permits_available", "");
    if let Some(values) = encode_avail.get("-") {
        let min = values.first().copied().unwrap_or(0.0);
        let p50 = percentile(values, 0.50);
        let total = gauge_series_by_label("cayenne_encode_permits_total", "")
            .get("-")
            .and_then(|v| v.last().copied())
            .unwrap_or(0.0);
        println!(
            "  Encode budget permits available: min={min:.0} p50={p50:.0} of total={total:.0} (0 => encode-semaphore stall)"
        );
    }
    if let Some((p90, p99)) = hist_pcts("cayenne_encode_acquire_wait_ms_bucket") {
        println!("  Encode acquire wait: p90={p90:.1}ms p99={p99:.1}ms");
    }

    // Stage 6: compaction semaphore acquire-wait + headroom.
    if let Some((p90, p99)) = hist_pcts("cayenne_compaction_acquire_wait_ms_bucket") {
        println!("  Compaction acquire wait: p90={p90:.1}ms p99={p99:.1}ms");
    }
    if let Some(values) = gauge_series_by_label("cayenne_compaction_permits_available", "").get("-")
    {
        let min = values.first().copied().unwrap_or(0.0);
        let total = gauge_series_by_label("cayenne_compaction_permits_total", "")
            .get("-")
            .and_then(|v| v.last().copied())
            .unwrap_or(0.0);
        println!("  Compaction permits available: min={min:.0} of total={total:.0}");
    }

    // Stage 4c: in-memory CDC tier byte-budget occupancy (memory durability mode).
    if let Some(values) = gauge_series_by_label("cayenne_mem_tier_budget_used_bytes", "").get("-") {
        let max = values.last().copied().unwrap_or(0.0);
        let total = gauge_series_by_label("cayenne_mem_tier_budget_total_bytes", "")
            .get("-")
            .and_then(|v| v.last().copied())
            .unwrap_or(0.0);
        println!("  Mem-tier bytes used: max={max:.0} of total={total:.0}");
    }
    println!();
}

/// A single analytical query's headline figures for the run summary.
pub(super) struct QuerySummaryRow {
    pub query_name: String,
    pub passed: bool,
    pub iterations: usize,
    pub median_ms: u64,
    pub p90_ms: u64,
    pub p99_ms: u64,
}

/// OLTP figures for the run summary (absent when the OLTP task errored, so the
/// summary can still report the analytical + replication results it does have).
pub(super) struct OltpSummary {
    pub tpmc: f64,
    pub total_committed: u64,
    pub total_aborted: u64,
    pub abort_rate: f64,
}

/// Headline results for one HTAP run, assembled by the caller from the reporting
/// sections it already computes and rendered to Markdown for the CI job summary.
pub(super) struct RunSummary {
    pub qph: f64,
    pub completed_queries: usize,
    pub elapsed_secs: f64,
    pub oltp: Option<OltpSummary>,
    pub lag: Option<ReplicationLagSummary>,
    pub queries: Vec<QuerySummaryRow>,
}

impl RunSummary {
    /// Render the headline results as GitHub-flavored Markdown. Writes go to an
    /// in-memory `String`, which never fails, so the `fmt::Result`s are discarded.
    fn to_markdown(&self) -> String {
        use std::fmt::Write as _;

        // Lag is scraped in ms; report seconds to match the `cayenne_goal_*` SLOs.
        let lag_s = |ms: f64| format!("{:.2} s", ms / 1000.0);

        let mut out = String::new();
        let _ = writeln!(out, "## Results\n");
        let _ = writeln!(out, "| Metric | Value |\n| --- | --- |");

        if let Some(oltp) = &self.oltp {
            let _ = writeln!(out, "| tpmC (NewOrder/min) | {:.1} |", oltp.tpmc);
            let _ = writeln!(
                out,
                "| OLTP transactions | {} committed, {} aborted ({:.2}% abort) |",
                oltp.total_committed,
                oltp.total_aborted,
                oltp.abort_rate * 100.0,
            );
        } else {
            let _ = writeln!(out, "| tpmC (NewOrder/min) | _OLTP results unavailable_ |");
        }

        let _ = writeln!(out, "| QPH (analytical queries/hour) | {:.1} |", self.qph);
        let _ = writeln!(
            out,
            "| Analytical queries completed | {} in {:.1}s |",
            self.completed_queries, self.elapsed_secs,
        );

        if let Some(lag) = &self.lag {
            let _ = writeln!(
                out,
                "| Worst replication lag (last) | {} |",
                lag_s(lag.last)
            );
            let _ = writeln!(out, "| Worst replication lag (p99) | {} |", lag_s(lag.p99));
            let _ = writeln!(out, "| Worst replication lag (max) | {} |", lag_s(lag.max));
        } else {
            let _ = writeln!(
                out,
                "| Worst replication lag | _no replication lag metrics_ |"
            );
        }

        if !self.queries.is_empty() {
            let _ = writeln!(out, "\n### Per-query latency\n");
            let _ = writeln!(
                out,
                "| Query | Status | Iterations | Median (ms) | P90 (ms) | P99 (ms) |\n| --- | --- | --- | --- | --- | --- |",
            );
            for q in &self.queries {
                let status = if q.passed { "✅" } else { "❌" };
                let _ = writeln!(
                    out,
                    "| {} | {} | {} | {} | {} | {} |",
                    q.query_name, status, q.iterations, q.median_ms, q.p90_ms, q.p99_ms,
                );
            }
        }

        out
    }
}

/// Write the headline run summary as Markdown to `path`. CI appends this to the
/// GitHub Actions job summary so tpmC / QPH / lag / per-query latencies are visible
/// without opening the run log. The write is moved to a blocking thread so it can't
/// stall the async runtime (repo guidance forbids blocking in async paths).
pub(super) async fn write_run_summary(
    path: &std::path::Path,
    summary: &RunSummary,
) -> anyhow::Result<()> {
    let markdown = summary.to_markdown();
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating run-summary directory {}", parent.display()))?;
        }
        std::fs::write(&path, markdown)
            .with_context(|| format!("writing run summary to {}", path.display()))?;
        Ok(())
    })
    .await
    .context("run-summary write task failed to join (panicked or was cancelled)")?
}

/// Serializes the full scraped metrics time-series plus run metadata to `path` as
/// JSON — the durable, machine-readable artifact `scripts/chbench-waterfall.py`
/// consumes and CI uploads.
pub(super) async fn write_metrics_dump(
    path: &std::path::Path,
    run: &serde_json::Value,
    metrics: Option<&crate::spiced_metrics::SpicedMetrics>,
    pg_stats: &[crate::pg_stats::PgStatSample],
) -> anyhow::Result<()> {
    // Reduce the raw per-second series so the artifact stays small enough to
    // upload + analyze at SF-1000 (the full dump is dominated by cumulative
    // histogram `_bucket` series and reaches ~1 GB). GAUGES keep their full time
    // series (occupancy/permits/lag percentiles need it); cumulative series
    // (counter/histogram/summary) keep the FIRST and LAST sample per label-set so the
    // waterfall can compute *windowed* deltas (Δ over the run, excluding bootstrap) —
    // see `reduce_samples_for_dump`. Building the owned dump value borrows `metrics`,
    // so it happens here; the heavy JSON encode + filesystem write are moved to a
    // blocking thread so the ~34 MB serialize/write can't stall the async runtime
    // (diagnostics/shutdown tasks) — repo guidance forbids blocking in async paths.
    let reduced = metrics
        .map(|m| reduce_samples_for_dump(&m.samples))
        .unwrap_or_default();
    let dump = serde_json::json!({
        "run": run,
        "samples": reduced,
        "pg_stats": pg_stats,
    });
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating metrics-dump directory {}", parent.display()))?;
        }
        let bytes = serde_json::to_vec(&dump).context("serializing metrics dump")?;
        std::fs::write(&path, bytes)
            .with_context(|| format!("writing metrics dump to {}", path.display()))?;
        Ok(())
    })
    .await
    .context("metrics-dump write task panicked")?
}

/// Reduce the raw per-second series for the dump — see [`write_metrics_dump`].
/// Gauges keep their full time series (percentiles/min/max/windowing). Cumulative
/// (counter/histogram/summary) series keep the FIRST and LAST sample per distinct
/// label-set, so the analysis can compute *windowed* deltas (`Δ_sum`/`Δ_count`,
/// `Δ_bucket`, `Δ_count`/`Δt` across `ts_ms`) rather than lifetime totals — a stall is
/// diluted by lifetime aggregation, and the first snapshot lets the whole-run
/// window exclude the bootstrap/initial-snapshot writes baked into it.
fn reduce_samples_for_dump(
    samples: &std::collections::HashMap<String, Vec<crate::spiced_metrics::MetricSample>>,
) -> std::collections::BTreeMap<&str, Vec<&crate::spiced_metrics::MetricSample>> {
    use crate::spiced_metrics::MetricType;
    use std::collections::BTreeMap;

    let mut out: BTreeMap<&str, Vec<&crate::spiced_metrics::MetricSample>> = BTreeMap::new();
    for (name, series) in samples {
        let is_gauge = series
            .first()
            .is_some_and(|s| s.metric_type == MetricType::Gauge);
        if is_gauge {
            out.insert(name.as_str(), series.iter().collect());
        } else {
            // First + last cumulative sample per label-set (preserving scrape order).
            let mut first_idx: BTreeMap<Vec<(&str, &str)>, usize> = BTreeMap::new();
            let mut last_idx: BTreeMap<Vec<(&str, &str)>, usize> = BTreeMap::new();
            for (i, s) in series.iter().enumerate() {
                let mut key: Vec<(&str, &str)> = s
                    .labels
                    .iter()
                    .map(|(k, v)| (k.as_str(), v.as_str()))
                    .collect();
                key.sort_unstable();
                first_idx.entry(key.clone()).or_insert(i);
                last_idx.insert(key, i);
            }
            let mut idxs: Vec<usize> = first_idx
                .into_values()
                .chain(last_idx.into_values())
                .collect();
            idxs.sort_unstable();
            idxs.dedup();
            out.insert(
                name.as_str(),
                idxs.into_iter().map(|i| &series[i]).collect(),
            );
        }
    }
    out
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spiced_metrics::{MetricSample, MetricType, SpicedMetrics};
    use std::collections::HashMap;

    fn sample(name: &str, labels: &[(&str, &str)], value: f64, ts_ms: i64) -> MetricSample {
        MetricSample {
            name: name.to_string(),
            labels: labels
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect(),
            value,
            metric_type: MetricType::Counter,
            ts_ms,
        }
    }

    fn metrics(samples: Vec<MetricSample>) -> SpicedMetrics {
        let mut by_name: HashMap<String, Vec<MetricSample>> = HashMap::new();
        for sample in samples {
            by_name.entry(sample.name.clone()).or_default().push(sample);
        }
        SpicedMetrics { samples: by_name }
    }

    /// A cumulative series is a running total, so the run's value is its maximum —
    /// summing the samples instead would multiply the count by the scrape count.
    #[test]
    fn cumulative_total_takes_the_series_maximum_not_the_sample_sum() {
        let m = metrics(vec![
            sample("op_total", &[("table", "orders")], 3.0, 1),
            sample("op_total", &[("table", "orders")], 7.0, 2),
            sample("op_total", &[("table", "orders")], 11.0, 3),
        ]);
        assert_eq!(cumulative_series_total(&m, "op_total"), Some(11.0));
    }

    /// Distinct label-sets are distinct series, so their end values add.
    #[test]
    fn cumulative_total_sums_across_label_sets() {
        let m = metrics(vec![
            sample("op_total", &[("table", "orders")], 10.0, 2),
            sample("op_total", &[("table", "orders")], 4.0, 1),
            sample("op_total", &[("table", "stock")], 5.0, 1),
        ]);
        assert_eq!(cumulative_series_total(&m, "op_total"), Some(15.0));
    }

    /// Order must not matter: the scrape store is a map, so a later sample is not
    /// guaranteed to sit last.
    #[test]
    fn cumulative_total_is_independent_of_sample_order() {
        let ascending = metrics(vec![
            sample("op_total", &[], 1.0, 1),
            sample("op_total", &[], 9.0, 2),
        ]);
        let descending = metrics(vec![
            sample("op_total", &[], 9.0, 2),
            sample("op_total", &[], 1.0, 1),
        ]);
        assert_eq!(
            cumulative_series_total(&ascending, "op_total"),
            cumulative_series_total(&descending, "op_total")
        );
        assert_eq!(cumulative_series_total(&ascending, "op_total"), Some(9.0));
    }

    /// An unscraped series is absent rather than zero, so a run with no Cayenne at
    /// all (a `DuckDB` baseline) reports nothing instead of a row of zeros.
    #[test]
    fn cumulative_total_is_absent_for_an_unscraped_series() {
        assert_eq!(cumulative_series_total(&metrics(vec![]), "op_total"), None);
    }

    /// Zero is a real observation — the operation is instrumented and did not run —
    /// and must not be conflated with the series being missing.
    #[test]
    fn cumulative_total_reports_an_observed_zero() {
        let m = metrics(vec![sample("op_total", &[("kind", "bake")], 0.0, 1)]);
        assert_eq!(cumulative_series_total(&m, "op_total"), Some(0.0));
    }

    /// Every reported operation must name a distinct series, or one would overwrite
    /// another's gauge point.
    #[test]
    fn operation_series_are_uniquely_named() {
        let mut labels: Vec<&str> = CAYENNE_OPERATION_SERIES.iter().map(|(l, ..)| *l).collect();
        let mut series: Vec<&str> = CAYENNE_OPERATION_SERIES
            .iter()
            .map(|(_, s, _)| *s)
            .collect();
        let (labels_len, series_len) = (labels.len(), series.len());
        labels.sort_unstable();
        labels.dedup();
        series.sort_unstable();
        series.dedup();
        assert_eq!(labels.len(), labels_len, "duplicate operation label");
        assert_eq!(series.len(), series_len, "duplicate scraped series");
    }

    /// The write- and read-path counts scale with the workload, so classifying one
    /// as `background` would report a run that merely applied more changes or
    /// served more queries as having done more housekeeping. These three were read
    /// off their emit sites; pin them so a later edit cannot quietly relabel them.
    #[test]
    fn workload_scaling_operations_are_not_classified_as_background() {
        for (label, expected) in [
            ("write_phases", OperationPath::Write),
            ("inline_tombstone_writes", OperationPath::Write),
            ("mem_tier_reserve_refused", OperationPath::Write),
            ("inline_cache_full_rebuilds", OperationPath::Read),
            ("inline_cache_delta_populates", OperationPath::Read),
        ] {
            let found = CAYENNE_OPERATION_SERIES
                .iter()
                .find(|(name, ..)| *name == label)
                .map(|(_, _, path)| *path);
            assert_eq!(found, Some(expected), "{label} is on the wrong path");
        }
    }

    /// Housekeeping is what the section exists to surface, so at least the
    /// background group must be populated — an all-workload list would make the
    /// report unable to answer the question it was added for.
    #[test]
    fn the_background_group_is_populated() {
        let background = CAYENNE_OPERATION_SERIES
            .iter()
            .filter(|(_, _, path)| *path == OperationPath::Background)
            .count();
        assert!(background >= 5, "got {background} background operations");
    }
}
