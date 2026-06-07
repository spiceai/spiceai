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

//! Closed-loop dynamic auto-tuning for the CDC ingest path.
//!
//! The static tier (hardware profile + inferred schema, in the accelerator
//! crate) picks a *starting* config. This module measures how a table is
//! actually behaving and nudges the safe, per-operation knobs toward the
//! objective, correcting static-model error and workload drift (bursty CDC,
//! ramp-up, a host slower than its spec).
//!
//! It is a **feedback** controller, not feed-forward: it watches not just the
//! ingest rate (the input) but how the runtime is *responding* to it — whether
//! apply latency is keeping up with the offered load, read amplification, where
//! the per-batch wall time goes (encode vs metastore publish), and memory
//! headroom — and acts on the gap to the objective.
//!
//! ## Objective hierarchy (highest priority first)
//! 1. Stay within the memory budget — *hard* (enforced by the actuator bounds).
//! 2. Keep apply latency under the offered-load interval (don't fall behind).
//! 3. Keep read amplification bounded (query-side health).
//! 4. Efficiency — back off work when healthy (only when 1–3 hold).
//!
//! ## Safety
//! Every actuator is clamped to the static `[floor, ceiling]` derived by the
//! static tier, so a dynamic decision can only ever pick a value the static
//! tier could also have picked — it just picks a *better* one from observed
//! behavior. This makes the controller safe to leave on by construction: the
//! worst case is the worst *static* config, never worse. The decider makes at
//! most **one bounded move per tick**, gated by hysteresis thresholds and a
//! minimum dwell time, so actuators can't oscillate or fight each other.
//!
//! The accounting ([`IngestStats`]) and the decision ([`decide`]) are pure and
//! exhaustively unit-tested; the wiring (recording on the write path, reading
//! [`LiveKnobs`] at the use sites, running [`decide`] on the per-table
//! background task) lives in the provider.

use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use parking_lot::Mutex;

/// EWMA smoothing factor for the rolling rate/latency estimates. ~0.3 is
/// responsive enough to follow a workload shift within a handful of batches
/// while filtering single-batch noise.
const EWMA_ALPHA: f64 = 0.3;

/// Number of recorded batches before the controller will act, so a cold table
/// (or a bootstrap burst) doesn't trigger a spurious early adjustment.
const WARMUP_BATCHES: u64 = 16;

/// "Falling behind" hysteresis: act only once apply latency exceeds the offered-
/// load interval by this factor (not merely equals it), so we don't chase noise.
const BEHIND_RATIO: f64 = 1.2;

/// "Comfortably keeping up" hysteresis: only relax/back off work when apply
/// latency is well under the offered-load interval.
const HEALTHY_RATIO: f64 = 0.5;

/// Read-amplification (small-file count) above which compaction is judged to be
/// behind and should run more aggressively.
const READ_AMP_HIGH: usize = 8;

/// Read-amplification below which the table is judged compaction-healthy.
const READ_AMP_LOW: usize = 3;

/// Minimum wall time between two applied adjustments (dwell), so each change is
/// given time to take effect before the next is considered. Matched to a few
/// background ticks.
pub(crate) const MIN_DWELL: Duration = Duration::from_secs(30);

/// Memory-usage fraction (of the cgroup-aware budget) above which the controller
/// frees live memory (shrinks the memtable) before doing anything else.
const MEM_PRESSURE_HIGH: f64 = 0.85;

/// Memory-usage fraction below which it is safe to *grow* a live allocation
/// (e.g. enlarge the memtable to reduce small-file churn). Hysteresis gap to
/// [`MEM_PRESSURE_HIGH`] avoids grow/shrink flapping near the limit.
const MEM_PRESSURE_OK: f64 = 0.75;

// ---------------------------------------------------------------------------
// Environment detection: process memory budget + cgroup-aware usage
// ---------------------------------------------------------------------------

/// Process memory budget in bytes (the cgroup-aware limit), injected once at
/// startup by the runtime via [`set_global_memory_budget`]. `0` = unset, in
/// which case memory pressure is reported as unknown and the controller runs
/// without the memory rule. Process-wide because RAM is shared across tables.
static GLOBAL_MEMORY_BUDGET: AtomicU64 = AtomicU64::new(0);

/// Install the process memory budget (cgroup-aware total) the dynamic tuner uses
/// to compute memory pressure. Called once at startup by the runtime, mirroring
/// the encode-concurrency budget.
pub fn set_global_memory_budget(bytes: u64) {
    GLOBAL_MEMORY_BUDGET.store(bytes, Ordering::Relaxed);
}

fn global_memory_budget() -> Option<u64> {
    match GLOBAL_MEMORY_BUDGET.load(Ordering::Relaxed) {
        0 => None,
        b => Some(b),
    }
}

/// Current process/cgroup memory usage in bytes — cgroup v2 (`memory.current`)
/// then v1 (`memory.usage_in_bytes`); `None` when unavailable. This is the
/// "detect the environment and adjust" read that closes the loop on memory.
#[cfg(target_os = "linux")]
fn current_memory_bytes() -> Option<u64> {
    use std::fs;
    for path in [
        "/sys/fs/cgroup/memory.current",
        "/sys/fs/cgroup/memory/memory.usage_in_bytes",
    ] {
        if let Ok(s) = fs::read_to_string(path)
            && let Ok(v) = s.trim().parse::<u64>()
        {
            return Some(v);
        }
    }
    None
}

#[cfg(not(target_os = "linux"))]
fn current_memory_bytes() -> Option<u64> {
    None
}

/// Sample current memory pressure (cgroup-aware `used / budget`) into `stats`,
/// when a budget is installed and usage is readable. Called on the background
/// tick so the controller can close the loop on memory.
pub(crate) fn sample_mem_pressure(stats: &IngestStats) {
    if let (Some(budget), Some(used)) = (global_memory_budget(), current_memory_bytes())
        && budget > 0
    {
        #[allow(clippy::cast_precision_loss)]
        stats.set_mem_pressure(used as f64 / budget as f64);
    }
}

// ---------------------------------------------------------------------------
// Accounting: input + response signals
// ---------------------------------------------------------------------------

/// One CDC write's measurements, recorded into [`IngestStats`]. Durations are
/// measured by the caller (the write path already times these), keeping
/// [`IngestStats`] pure and deterministically testable.
#[derive(Debug, Clone, Copy)]
pub(crate) struct WriteSample {
    /// Rows applied in this batch.
    pub rows: u64,
    /// Serialized bytes applied in this batch.
    pub bytes: u64,
    /// Total apply wall time for the batch (the runtime's response: how long
    /// *we* took to absorb it).
    pub apply: Duration,
    /// Metastore publish/commit wall time within `apply`.
    pub publish: Duration,
    /// Encode wall time within `apply`.
    pub encode: Duration,
    /// Wall time since the previous batch (the offered-load interval). `None`
    /// for the first batch.
    pub arrival_gap: Option<Duration>,
}

#[derive(Debug, Default, Clone, Copy)]
struct EwmaInner {
    rows_per_sec: f64,
    bytes_per_sec: f64,
    apply_ms: f64,
    publish_ms: f64,
    encode_ms: f64,
    arrival_gap_ms: f64,
    samples: u64,
}

/// Per-table rolling accounting of both the **input** (ingest rate) and the
/// **response** (apply latency, where time goes, read amplification). Cheap:
/// monotonic counters are atomics; the EWMA floats are behind a short-held
/// mutex updated once per *batch* (not per row).
#[derive(Debug)]
pub(crate) struct IngestStats {
    inner: Mutex<EwmaInner>,
    total_rows: AtomicU64,
    total_bytes: AtomicU64,
    total_batches: AtomicU64,
    /// Current small-file count in the snapshot (read amplification), set by the
    /// compaction/scan path. Defaults to 0 (healthy) until first observed.
    read_amp: AtomicUsize,
    /// Memory usage as a fraction of the cgroup-aware budget, stored ×1000.
    /// `u64::MAX` is the sentinel for "unknown" (no budget/sample yet).
    mem_pressure_milli: AtomicU64,
}

impl Default for IngestStats {
    fn default() -> Self {
        Self::new()
    }
}

impl IngestStats {
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(EwmaInner::default()),
            total_rows: AtomicU64::new(0),
            total_bytes: AtomicU64::new(0),
            total_batches: AtomicU64::new(0),
            read_amp: AtomicUsize::new(0),
            mem_pressure_milli: AtomicU64::new(u64::MAX),
        }
    }

    /// Record one CDC write. Folds the batch's instantaneous rate/latency into
    /// the EWMAs and bumps the monotonic totals.
    pub fn record_write(&self, s: WriteSample) {
        self.total_rows.fetch_add(s.rows, Ordering::Relaxed);
        self.total_bytes.fetch_add(s.bytes, Ordering::Relaxed);
        self.total_batches.fetch_add(1, Ordering::Relaxed);

        let apply_ms = duration_ms(s.apply);
        let publish_ms = duration_ms(s.publish);
        let encode_ms = duration_ms(s.encode);
        // Offered rate = rows / inter-batch interval. Fall back to the apply
        // window for the first batch (no gap yet) so a single batch still yields
        // a finite rate.
        let window_ms = s
            .arrival_gap
            .map_or(apply_ms, duration_ms)
            .max(f64::from(1_u32) / 1000.0);
        let inst_rows_per_sec = (s.rows as f64) * 1000.0 / window_ms;
        let inst_bytes_per_sec = (s.bytes as f64) * 1000.0 / window_ms;
        let arrival_gap_ms = s.arrival_gap.map_or(apply_ms, duration_ms);

        let mut inner = self.inner.lock();
        let prior = inner.samples;
        ewma(&mut inner.rows_per_sec, inst_rows_per_sec, prior);
        ewma(&mut inner.bytes_per_sec, inst_bytes_per_sec, prior);
        ewma(&mut inner.apply_ms, apply_ms, prior);
        ewma(&mut inner.publish_ms, publish_ms, prior);
        ewma(&mut inner.encode_ms, encode_ms, prior);
        ewma(&mut inner.arrival_gap_ms, arrival_gap_ms, prior);
        inner.samples = prior.saturating_add(1);
    }

    /// Update the observed read amplification (small-file count). Called by the
    /// compaction/scan path.
    pub fn set_read_amp(&self, small_files: usize) {
        self.read_amp.store(small_files, Ordering::Relaxed);
    }

    /// Update the current memory usage as a fraction of the cgroup-aware budget
    /// (`used / budget`). Sampled on the background tick so the controller can
    /// close the loop on memory (shrink live allocations under pressure). Values
    /// ≥ 1.0 (over budget) are preserved as pressure > 1.
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    pub fn set_mem_pressure(&self, fraction: f64) {
        if !fraction.is_finite() || fraction < 0.0 {
            return;
        }
        let milli = (fraction * 1000.0).round().min((u64::MAX - 1) as f64) as u64;
        self.mem_pressure_milli.store(milli, Ordering::Relaxed);
    }

    /// Take a consistent snapshot of the derived signals for the controller.
    #[must_use]
    pub fn snapshot(&self) -> IngestSnapshot {
        let inner = *self.inner.lock();
        // Key response signal: apply latency relative to the offered-load
        // interval. > 1 means we absorb a batch slower than batches arrive — the
        // table is falling behind regardless of the absolute rate.
        let apply_vs_arrival = if inner.arrival_gap_ms > 0.0 {
            inner.apply_ms / inner.arrival_gap_ms
        } else {
            0.0
        };
        #[allow(clippy::cast_precision_loss)]
        let mem_pressure = match self.mem_pressure_milli.load(Ordering::Relaxed) {
            u64::MAX => None,
            milli => Some(milli as f64 / 1000.0),
        };
        IngestSnapshot {
            rows_per_sec: inner.rows_per_sec,
            bytes_per_sec: inner.bytes_per_sec,
            apply_ms: inner.apply_ms,
            publish_ms: inner.publish_ms,
            encode_ms: inner.encode_ms,
            arrival_gap_ms: inner.arrival_gap_ms,
            apply_vs_arrival,
            read_amp: self.read_amp.load(Ordering::Relaxed),
            mem_pressure,
            samples: inner.samples,
        }
    }
}

/// Derived, point-in-time signals the controller reasons over.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct IngestSnapshot {
    pub rows_per_sec: f64,
    pub bytes_per_sec: f64,
    pub apply_ms: f64,
    pub publish_ms: f64,
    pub encode_ms: f64,
    pub arrival_gap_ms: f64,
    /// `apply_ms / arrival_gap_ms`: > 1 ⇒ falling behind the offered load.
    pub apply_vs_arrival: f64,
    /// Small-file count (read amplification): the ingest→query coupling signal —
    /// high means ingest is producing files that slow scans.
    pub read_amp: usize,
    /// Memory usage as a fraction of the cgroup-aware budget (`used / budget`);
    /// `None` when no budget/sample is available. `> 1.0` means over budget.
    pub mem_pressure: Option<f64>,
    pub samples: u64,
}

// ---------------------------------------------------------------------------
// Actuators: live, atomically-updatable knobs
// ---------------------------------------------------------------------------

/// The subset of Vortex knobs that are safe to adjust at runtime because they
/// are read fresh per operation (no allocate-once state). Initialized from the
/// static config; the controller updates them, and the write/compaction paths
/// read them in place of the frozen config values.
#[derive(Debug)]
pub(crate) struct LiveKnobs {
    inline_flush_max_bytes: AtomicI64,
    inline_flush_max_rows: AtomicI64,
    inline_flush_max_segments: AtomicI64,
    compaction_background_interval_ms: AtomicU64,
    compaction_trigger_files: AtomicUsize,
    /// 0 means "unset" (use the session/default write concurrency).
    write_concurrency: AtomicUsize,
}

impl LiveKnobs {
    #[must_use]
    pub fn new(init: KnobValues) -> Self {
        Self {
            inline_flush_max_bytes: AtomicI64::new(init.inline_flush_max_bytes),
            inline_flush_max_rows: AtomicI64::new(init.inline_flush_max_rows),
            inline_flush_max_segments: AtomicI64::new(init.inline_flush_max_segments),
            compaction_background_interval_ms: AtomicU64::new(
                init.compaction_background_interval_ms,
            ),
            compaction_trigger_files: AtomicUsize::new(init.compaction_trigger_files),
            write_concurrency: AtomicUsize::new(init.write_concurrency),
        }
    }

    #[must_use]
    pub fn values(&self) -> KnobValues {
        KnobValues {
            inline_flush_max_bytes: self.inline_flush_max_bytes.load(Ordering::Relaxed),
            inline_flush_max_rows: self.inline_flush_max_rows.load(Ordering::Relaxed),
            inline_flush_max_segments: self.inline_flush_max_segments.load(Ordering::Relaxed),
            compaction_background_interval_ms: self
                .compaction_background_interval_ms
                .load(Ordering::Relaxed),
            compaction_trigger_files: self.compaction_trigger_files.load(Ordering::Relaxed),
            write_concurrency: self.write_concurrency.load(Ordering::Relaxed),
        }
    }

    pub fn inline_flush_max_bytes(&self) -> i64 {
        self.inline_flush_max_bytes.load(Ordering::Relaxed)
    }
    pub fn inline_flush_max_rows(&self) -> i64 {
        self.inline_flush_max_rows.load(Ordering::Relaxed)
    }
    pub fn inline_flush_max_segments(&self) -> i64 {
        self.inline_flush_max_segments.load(Ordering::Relaxed)
    }
    pub fn compaction_background_interval_ms(&self) -> u64 {
        self.compaction_background_interval_ms.load(Ordering::Relaxed)
    }
    pub fn compaction_trigger_files(&self) -> usize {
        self.compaction_trigger_files.load(Ordering::Relaxed)
    }
    pub fn write_concurrency(&self) -> usize {
        self.write_concurrency.load(Ordering::Relaxed)
    }

    /// Apply a controller decision. The new value is already clamped to bounds
    /// by [`decide`]; this just stores it.
    pub fn apply(&self, adj: &Adjustment) {
        match adj.knob {
            Knob::InlineFlushBytes => {
                self.inline_flush_max_bytes
                    .store(adj.new_value as i64, Ordering::Relaxed);
                // Keep rows/segments coherent with the byte budget (same ratios
                // the static derivation uses: ~1 KiB/row, ~128 KiB/segment).
                let rows = (adj.new_value / 1024).max(64) as i64;
                let segs = (adj.new_value / (128 * 1024)).clamp(16, 256) as i64;
                self.inline_flush_max_rows.store(rows, Ordering::Relaxed);
                self.inline_flush_max_segments.store(segs, Ordering::Relaxed);
            }
            Knob::CompactionIntervalMs => {
                self.compaction_background_interval_ms
                    .store(adj.new_value, Ordering::Relaxed);
            }
            Knob::CompactionTriggerFiles => {
                self.compaction_trigger_files
                    .store(adj.new_value as usize, Ordering::Relaxed);
            }
            Knob::WriteConcurrency => {
                self.write_concurrency
                    .store(adj.new_value as usize, Ordering::Relaxed);
            }
        }
    }
}

/// A plain snapshot of the live knob values (for the decider + observability).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct KnobValues {
    pub inline_flush_max_bytes: i64,
    pub inline_flush_max_rows: i64,
    pub inline_flush_max_segments: i64,
    pub compaction_background_interval_ms: u64,
    pub compaction_trigger_files: usize,
    pub write_concurrency: usize,
}

/// Static `[floor, ceiling]` per dynamically-tuned knob, derived by the static
/// tier. The controller can never move a knob outside these, so dynamic tuning
/// is bounded by — and can only improve on — the static config.
#[derive(Debug, Clone, Copy)]
pub(crate) struct TuningBounds {
    pub inline_flush_max_bytes: (i64, i64),
    pub compaction_background_interval_ms: (u64, u64),
    pub compaction_trigger_files: (usize, usize),
    pub write_concurrency: (usize, usize),
}

// ---------------------------------------------------------------------------
// Controller: pure decision
// ---------------------------------------------------------------------------

/// Which knob an [`Adjustment`] targets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Knob {
    InlineFlushBytes,
    CompactionIntervalMs,
    CompactionTriggerFiles,
    WriteConcurrency,
}

impl Knob {
    /// Stable label for metrics/logs.
    #[must_use]
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::InlineFlushBytes => "inline_flush_bytes",
            Self::CompactionIntervalMs => "compaction_interval_ms",
            Self::CompactionTriggerFiles => "compaction_trigger_files",
            Self::WriteConcurrency => "write_concurrency",
        }
    }
}

/// A single bounded actuator move with the reason it was made (logged for
/// trust/observability).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Adjustment {
    pub knob: Knob,
    /// New value (units depend on `knob`; already clamped to bounds).
    pub new_value: u64,
    pub reason: &'static str,
}

/// Multiplicative step for one adjustment (±50%). Large enough to converge in a
/// few ticks, small enough that one move can't overshoot the whole range.
const STEP: f64 = 1.5;

/// Decide the single best bounded actuator move for the current behavior, or
/// `None` to hold.
///
/// Pure: no I/O, no clock — `since_last` (time since the previous applied move)
/// is passed in so the dwell-time hysteresis is testable. Rules are evaluated in
/// objective-priority order and the first applicable one wins (one move per
/// tick).
#[must_use]
pub(crate) fn decide(
    s: &IngestSnapshot,
    cur: &KnobValues,
    b: &TuningBounds,
    since_last: Duration,
    min_dwell: Duration,
) -> Option<Adjustment> {
    // Don't act on a cold table, and respect the dwell time so moves don't
    // stack faster than their effect can be observed.
    if s.samples < WARMUP_BATCHES || since_last < min_dwell {
        return None;
    }

    let mem_high = s.mem_pressure.is_some_and(|p| p > MEM_PRESSURE_HIGH);
    let mem_ok = s.mem_pressure.is_none_or(|p| p < MEM_PRESSURE_OK);
    let behind = s.apply_vs_arrival > BEHIND_RATIO;
    let read_amp_high = s.read_amp > READ_AMP_HIGH;

    // (1) Memory pressure [hard, highest priority]: the cgroup-aware budget is
    // nearly exhausted. The only *live* memory lever is the inline memtable, so
    // shrink it toward the floor. Running first means no growth rule below can
    // enlarge memory on an already-tight box; query read-amp is instead relieved
    // by compaction (which costs CPU, not memory).
    if mem_high {
        if let Some(v) = clamp_move_i64(
            cur.inline_flush_max_bytes,
            shrink_i64(cur.inline_flush_max_bytes),
            b.inline_flush_max_bytes,
        ) {
            return Some(Adjustment {
                knob: Knob::InlineFlushBytes,
                new_value: u64::try_from(v).unwrap_or(0),
                reason: "memory pressure: shrink memtable to stay within the cgroup budget",
            });
        }
    }

    // The system is unhealthy if ingest is falling behind OR queries are being
    // hurt by ingest's small-file output. Both are handled here; ingest-speed
    // levers that would worsen query health (more write shards = more files) are
    // held back while read-amp is high.
    if behind || read_amp_high {
        // (3) Query health (ingest↔query coupling): too many small files slow
        // scans. Fix at the SOURCE first — a larger memtable checkpoints fewer,
        // larger Vortex files — but only if memory allows; otherwise drain the
        // backlog with compaction (CPU, not memory).
        if read_amp_high {
            if mem_ok {
                if let Some(v) = clamp_move_i64(
                    cur.inline_flush_max_bytes,
                    grow_i64(cur.inline_flush_max_bytes),
                    b.inline_flush_max_bytes,
                ) {
                    return Some(Adjustment {
                        knob: Knob::InlineFlushBytes,
                        new_value: u64::try_from(v).unwrap_or(0),
                        reason: "high read-amp: larger memtable → fewer small files for queries",
                    });
                }
            }
            if let Some(v) = clamp_move_u64(
                cur.compaction_background_interval_ms,
                scale_u64(cur.compaction_background_interval_ms, 1.0 / STEP),
                b.compaction_background_interval_ms,
            ) {
                return Some(Adjustment {
                    knob: Knob::CompactionIntervalMs,
                    new_value: v,
                    reason: "high read-amp: compact more often to drain small files for queries",
                });
            }
            if let Some(v) = clamp_move_usize(
                cur.compaction_trigger_files,
                cur.compaction_trigger_files.saturating_sub(1),
                b.compaction_trigger_files,
            ) {
                return Some(Adjustment {
                    knob: Knob::CompactionTriggerFiles,
                    new_value: u64::try_from(v).unwrap_or(0),
                    reason: "high read-amp: lower compaction trigger",
                });
            }
        }

        // (2) Ingest throughput, only when ingest is actually behind. Diagnose
        // from where the per-batch wall time goes.
        if behind {
            if s.publish_ms >= s.encode_ms {
                // Metastore-publish-bound → batch more per flush to amortize the
                // per-commit cost (also yields fewer, larger files: a query win).
                if mem_ok
                    && let Some(v) = clamp_move_i64(
                        cur.inline_flush_max_bytes,
                        grow_i64(cur.inline_flush_max_bytes),
                        b.inline_flush_max_bytes,
                    )
                {
                    return Some(Adjustment {
                        knob: Knob::InlineFlushBytes,
                        new_value: u64::try_from(v).unwrap_or(0),
                        reason: "falling behind, publish-bound: enlarge memtable to amortize commits",
                    });
                }
            } else if s.read_amp <= READ_AMP_LOW {
                // Encode-bound → more encode shards, but ONLY when files aren't
                // already a query problem (more shards mean more files): never
                // speed ingest at the cost of query latency.
                if let Some(v) = clamp_move_usize(
                    cur.write_concurrency.max(1),
                    scale_usize(cur.write_concurrency.max(1), STEP),
                    b.write_concurrency,
                ) {
                    return Some(Adjustment {
                        knob: Knob::WriteConcurrency,
                        new_value: u64::try_from(v).unwrap_or(0),
                        reason: "falling behind, encode-bound: raise write concurrency",
                    });
                }
            }
            // Last resort while behind: compact more to keep the snapshot lean.
            if let Some(v) = clamp_move_u64(
                cur.compaction_background_interval_ms,
                scale_u64(cur.compaction_background_interval_ms, 1.0 / STEP),
                b.compaction_background_interval_ms,
            ) {
                return Some(Adjustment {
                    knob: Knob::CompactionIntervalMs,
                    new_value: v,
                    reason: "falling behind: compact more to keep the snapshot lean",
                });
            }
        }
        return None;
    }

    // (4) Healthy on every axis (ingest caught up, queries not read-amp-bound,
    // memory comfortable) → give CPU back to queries by backing off background
    // compaction. Only when clearly idle, so we don't undo a recent speed-up.
    if s.apply_vs_arrival < HEALTHY_RATIO && s.read_amp <= READ_AMP_LOW && mem_ok {
        if let Some(v) = clamp_move_u64(
            cur.compaction_background_interval_ms,
            scale_u64(cur.compaction_background_interval_ms, STEP),
            b.compaction_background_interval_ms,
        ) {
            return Some(Adjustment {
                knob: Knob::CompactionIntervalMs,
                new_value: v,
                reason: "healthy: back off compaction to free CPU for queries",
            });
        }
    }

    None
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

fn duration_ms(d: Duration) -> f64 {
    // millis as f64; sub-ms resolution preserved via as_secs_f64.
    d.as_secs_f64() * 1000.0
}

fn ewma(slot: &mut f64, sample: f64, prior_samples: u64) {
    if prior_samples == 0 {
        *slot = sample;
    } else {
        *slot = EWMA_ALPHA * sample + (1.0 - EWMA_ALPHA) * *slot;
    }
}

#[allow(clippy::cast_precision_loss, clippy::cast_sign_loss, clippy::cast_possible_truncation)]
fn scale_u64(v: u64, factor: f64) -> u64 {
    ((v as f64) * factor).round() as u64
}

#[allow(clippy::cast_precision_loss, clippy::cast_sign_loss, clippy::cast_possible_truncation)]
fn scale_usize(v: usize, factor: f64) -> usize {
    ((v as f64) * factor).round() as usize
}

/// Grow an `i64` knob by one [`STEP`] (non-negative inputs only; clamped at use).
fn grow_i64(v: i64) -> i64 {
    i64::try_from(scale_u64(u64::try_from(v.max(0)).unwrap_or(0), STEP)).unwrap_or(i64::MAX)
}

/// Shrink an `i64` knob by one [`STEP`].
fn shrink_i64(v: i64) -> i64 {
    i64::try_from(scale_u64(u64::try_from(v.max(0)).unwrap_or(0), 1.0 / STEP)).unwrap_or(0)
}

/// Clamp `target` to `[lo, hi]` and return it only if it differs from `cur`
/// (otherwise the knob is already at its useful extreme — no-op, hold).
fn clamp_move_u64(cur: u64, target: u64, (lo, hi): (u64, u64)) -> Option<u64> {
    let v = target.clamp(lo, hi);
    (v != cur).then_some(v)
}

fn clamp_move_usize(cur: usize, target: usize, (lo, hi): (usize, usize)) -> Option<usize> {
    let v = target.clamp(lo, hi);
    (v != cur).then_some(v)
}

fn clamp_move_i64(cur: i64, target: i64, (lo, hi): (i64, i64)) -> Option<i64> {
    let v = target.clamp(lo, hi);
    (v != cur).then_some(v)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    fn sample(rows: u64, apply_ms: u64, publish_ms: u64, encode_ms: u64, gap_ms: u64) -> WriteSample {
        WriteSample {
            rows,
            bytes: rows * 256,
            apply: ms(apply_ms),
            publish: ms(publish_ms),
            encode: ms(encode_ms),
            arrival_gap: Some(ms(gap_ms)),
        }
    }

    fn bounds() -> TuningBounds {
        TuningBounds {
            inline_flush_max_bytes: (2 * 1024 * 1024, 128 * 1024 * 1024),
            compaction_background_interval_ms: (2_000, 60_000),
            compaction_trigger_files: (2, 32),
            write_concurrency: (1, 16),
        }
    }

    fn knobs() -> KnobValues {
        KnobValues {
            inline_flush_max_bytes: 8 * 1024 * 1024,
            inline_flush_max_rows: 8192,
            inline_flush_max_segments: 64,
            compaction_background_interval_ms: 10_000,
            compaction_trigger_files: 8,
            write_concurrency: 4,
        }
    }

    /// Feed `n` identical samples to warm the EWMA past the warmup gate.
    fn warm(stats: &IngestStats, s: WriteSample, n: u64) {
        for _ in 0..n {
            stats.record_write(s);
        }
    }

    // ---- accounting -------------------------------------------------------

    #[test]
    fn record_and_snapshot_basic_rates() {
        let stats = IngestStats::new();
        // 1000 rows every 100 ms ⇒ ~10k rows/s; apply 20 ms ≪ 100 ms gap.
        warm(&stats, sample(1000, 20, 5, 10, 100), 20);
        let s = stats.snapshot();
        assert_eq!(s.samples, 20);
        assert!(
            (s.rows_per_sec - 10_000.0).abs() < 1.0,
            "rows/s ~10000, got {}",
            s.rows_per_sec
        );
        assert!(s.apply_vs_arrival < 0.5, "20ms apply vs 100ms gap → keeping up");
        assert_eq!(stats.total_rows.load(Ordering::Relaxed), 20_000);
        assert_eq!(stats.total_batches.load(Ordering::Relaxed), 20);
    }

    #[test]
    fn apply_vs_arrival_detects_falling_behind() {
        let stats = IngestStats::new();
        // apply 150 ms but batches arrive every 100 ms → ratio 1.5 > 1.
        warm(&stats, sample(1000, 150, 100, 20, 100), 20);
        let s = stats.snapshot();
        assert!(
            s.apply_vs_arrival > BEHIND_RATIO,
            "ratio {} should exceed BEHIND_RATIO",
            s.apply_vs_arrival
        );
    }

    #[test]
    fn first_batch_without_gap_yields_finite_rate() {
        let stats = IngestStats::new();
        stats.record_write(WriteSample {
            rows: 500,
            bytes: 500 * 256,
            apply: ms(50),
            publish: ms(10),
            encode: ms(20),
            arrival_gap: None,
        });
        let s = stats.snapshot();
        assert!(s.rows_per_sec.is_finite() && s.rows_per_sec > 0.0);
    }

    // ---- controller: warmup + dwell --------------------------------------

    #[test]
    fn no_action_before_warmup() {
        let stats = IngestStats::new();
        warm(&stats, sample(1000, 150, 100, 20, 100), WARMUP_BATCHES - 1);
        let s = stats.snapshot();
        assert!(decide(&s, &knobs(), &bounds(), ms(60_000), ms(30_000)).is_none());
    }

    #[test]
    fn no_action_within_dwell() {
        let stats = IngestStats::new();
        warm(&stats, sample(1000, 150, 100, 20, 100), 20);
        let s = stats.snapshot();
        // since_last < min_dwell ⇒ hold even though falling behind.
        assert!(decide(&s, &knobs(), &bounds(), ms(5_000), ms(30_000)).is_none());
    }

    // ---- controller: diagnosis branches ----------------------------------

    #[test]
    fn high_read_amp_enlarges_memtable_then_compacts() {
        let stats = IngestStats::new();
        warm(&stats, sample(1000, 150, 100, 20, 100), 20);
        stats.set_read_amp(READ_AMP_HIGH + 5);
        let s = stats.snapshot();
        // Ingest↔query coupling: fix at the source first — a bigger memtable
        // checkpoints fewer, larger files, so ingest stops slowing scans.
        let adj = decide(&s, &knobs(), &bounds(), ms(60_000), ms(30_000)).expect("acts");
        assert_eq!(adj.knob, Knob::InlineFlushBytes);
        assert!(adj.new_value > u64::try_from(knobs().inline_flush_max_bytes).unwrap());
        // With the memtable already at its ceiling, drain via compaction instead.
        let at_ceiling = KnobValues {
            inline_flush_max_bytes: bounds().inline_flush_max_bytes.1,
            ..knobs()
        };
        let adj2 = decide(&s, &at_ceiling, &bounds(), ms(60_000), ms(30_000)).expect("acts");
        assert_eq!(adj2.knob, Knob::CompactionIntervalMs);
        assert!(adj2.new_value < knobs().compaction_background_interval_ms);
    }

    #[test]
    fn high_read_amp_acts_even_when_ingest_caught_up() {
        // The user's scenario: ingest keeps up (apply ≪ gap) but is creating too
        // many small files, so QUERIES are slow. The controller must still act.
        let stats = IngestStats::new();
        warm(&stats, sample(1000, 20, 5, 10, 100), 20); // apply 20 ≪ 100 gap
        stats.set_read_amp(READ_AMP_HIGH + 10);
        let s = stats.snapshot();
        assert!(s.apply_vs_arrival < HEALTHY_RATIO, "ingest is caught up");
        let adj = decide(&s, &knobs(), &bounds(), ms(60_000), ms(30_000))
            .expect("must act for query health even though ingest is fine");
        assert_eq!(adj.knob, Knob::InlineFlushBytes);
    }

    #[test]
    fn encode_bound_with_high_read_amp_does_not_add_shards() {
        // Ecosystem balance: never raise write concurrency (more files) while
        // queries are already read-amp-bound. The read-amp rule wins.
        let stats = IngestStats::new();
        warm(&stats, sample(1000, 150, 10, 120, 100), 20); // encode-bound
        stats.set_read_amp(READ_AMP_HIGH + 5);
        let s = stats.snapshot();
        let adj = decide(&s, &knobs(), &bounds(), ms(60_000), ms(30_000)).expect("acts");
        assert_ne!(
            adj.knob,
            Knob::WriteConcurrency,
            "must not add shards (more files) while read-amp is high"
        );
    }

    #[test]
    fn memory_pressure_shrinks_memtable_and_overrides_growth() {
        // Even falling behind + publish-bound (which would normally GROW the
        // memtable), high memory pressure forces a SHRINK — memory is the hard
        // constraint and runs first.
        let stats = IngestStats::new();
        warm(&stats, sample(1000, 150, 100, 20, 100), 20); // behind, publish-bound
        stats.set_mem_pressure(0.95); // over MEM_PRESSURE_HIGH
        let s = stats.snapshot();
        let bigger = KnobValues {
            inline_flush_max_bytes: 32 * 1024 * 1024,
            ..knobs()
        };
        let adj = decide(&s, &bigger, &bounds(), ms(60_000), ms(30_000)).expect("acts");
        assert_eq!(adj.knob, Knob::InlineFlushBytes);
        assert!(
            adj.new_value < u64::try_from(bigger.inline_flush_max_bytes).unwrap(),
            "memtable must shrink under memory pressure"
        );
    }

    #[test]
    fn memory_pressure_blocks_growth_but_allows_compaction_for_read_amp() {
        // Memory is between OK and HIGH (can't grow, not yet shrinking) and
        // read-amp is high → relieve queries via compaction (CPU, not memory),
        // not by enlarging the memtable.
        let stats = IngestStats::new();
        warm(&stats, sample(1000, 20, 5, 10, 100), 20);
        stats.set_read_amp(READ_AMP_HIGH + 5);
        stats.set_mem_pressure(0.80); // between OK (0.75) and HIGH (0.85)
        let s = stats.snapshot();
        let adj = decide(&s, &knobs(), &bounds(), ms(60_000), ms(30_000)).expect("acts");
        assert_eq!(
            adj.knob,
            Knob::CompactionIntervalMs,
            "no memory to grow the memtable → drain small files via compaction"
        );
    }

    #[test]
    fn pinned_knob_via_collapsed_bounds_is_skipped() {
        // An operator-pinned knob (in `adaptive` mode) has its bounds collapsed to
        // a single point. A publish-bound "falling behind" signal that would
        // normally GROW the memtable must instead fall through to another lever —
        // the explicit override is respected, never overwritten by the loop.
        let stats = IngestStats::new();
        warm(&stats, sample(1000, 150, 100, 20, 100), 20); // behind, publish-bound
        stats.set_read_amp(1);
        let s = stats.snapshot();
        let mut b = bounds();
        b.inline_flush_max_bytes = (
            knobs().inline_flush_max_bytes,
            knobs().inline_flush_max_bytes,
        );
        let adj = decide(&s, &knobs(), &b, ms(60_000), ms(30_000)).expect("acts via another lever");
        assert_ne!(
            adj.knob,
            Knob::InlineFlushBytes,
            "a pinned memtable must never be moved by the controller"
        );
        assert_eq!(adj.knob, Knob::CompactionIntervalMs);
    }

    #[test]
    fn behind_and_publish_bound_enlarges_memtable() {
        let stats = IngestStats::new();
        // publish (100) >> encode (20); read-amp healthy ⇒ publish-bound path.
        warm(&stats, sample(1000, 150, 100, 20, 100), 20);
        stats.set_read_amp(1);
        let s = stats.snapshot();
        let adj = decide(&s, &knobs(), &bounds(), ms(60_000), ms(30_000)).expect("acts");
        assert_eq!(adj.knob, Knob::InlineFlushBytes);
        assert!(adj.new_value > knobs().inline_flush_max_bytes as u64);
        assert!(adj.new_value <= bounds().inline_flush_max_bytes.1 as u64);
    }

    #[test]
    fn behind_and_encode_bound_raises_write_concurrency() {
        let stats = IngestStats::new();
        // encode (120) > publish (10); read-amp healthy ⇒ encode-bound path.
        warm(&stats, sample(1000, 150, 10, 120, 100), 20);
        stats.set_read_amp(1);
        let s = stats.snapshot();
        let adj = decide(&s, &knobs(), &bounds(), ms(60_000), ms(30_000)).expect("acts");
        assert_eq!(adj.knob, Knob::WriteConcurrency);
        assert!(adj.new_value > knobs().write_concurrency as u64);
        assert!(adj.new_value <= bounds().write_concurrency.1 as u64);
    }

    #[test]
    fn healthy_and_low_read_amp_backs_off_compaction() {
        let stats = IngestStats::new();
        // apply 20 ms ≪ 100 ms gap ⇒ healthy; low read-amp.
        warm(&stats, sample(1000, 20, 5, 10, 100), 20);
        stats.set_read_amp(1);
        let s = stats.snapshot();
        let adj = decide(&s, &knobs(), &bounds(), ms(60_000), ms(30_000)).expect("acts");
        assert_eq!(adj.knob, Knob::CompactionIntervalMs);
        assert!(
            adj.new_value > knobs().compaction_background_interval_ms,
            "interval should lengthen when idle"
        );
    }

    #[test]
    fn steady_state_holds() {
        let stats = IngestStats::new();
        // Keeping up but not idle (ratio ~0.8, between HEALTHY and BEHIND), mid
        // read-amp ⇒ no rule fires.
        warm(&stats, sample(1000, 80, 20, 30, 100), 20);
        stats.set_read_amp(5);
        let s = stats.snapshot();
        assert!(decide(&s, &knobs(), &bounds(), ms(60_000), ms(30_000)).is_none());
    }

    // ---- safety: bounds are never exceeded; no-op at extremes -------------

    #[test]
    #[allow(clippy::cast_sign_loss)]
    fn adjustments_never_exceed_bounds() {
        let stats = IngestStats::new();
        warm(&stats, sample(1000, 150, 100, 20, 100), 20); // behind
        stats.set_read_amp(READ_AMP_HIGH + 5); // and read-amp high
        let s = stats.snapshot();
        let b = bounds();
        // From a range of starting positions, every returned move stays within
        // the targeted knob's `[floor, ceiling]` (the clamp is by construction).
        for mult in [1_i64, 2, 4, 8, 16, 64] {
            let k = KnobValues {
                inline_flush_max_bytes: (2 * 1024 * 1024 * mult).min(b.inline_flush_max_bytes.1),
                compaction_background_interval_ms: (3_000 * mult as u64)
                    .min(b.compaction_background_interval_ms.1),
                compaction_trigger_files: knobs().compaction_trigger_files,
                write_concurrency: knobs().write_concurrency,
                ..knobs()
            };
            let Some(adj) = decide(&s, &k, &b, ms(60_000), ms(30_000)) else {
                continue;
            };
            let (lo, hi) = match adj.knob {
                Knob::InlineFlushBytes => (
                    b.inline_flush_max_bytes.0 as u64,
                    b.inline_flush_max_bytes.1 as u64,
                ),
                Knob::CompactionIntervalMs => b.compaction_background_interval_ms,
                Knob::CompactionTriggerFiles => (
                    b.compaction_trigger_files.0 as u64,
                    b.compaction_trigger_files.1 as u64,
                ),
                Knob::WriteConcurrency => {
                    (b.write_concurrency.0 as u64, b.write_concurrency.1 as u64)
                }
            };
            assert!(
                (lo..=hi).contains(&adj.new_value),
                "{:?} value {} out of bounds [{lo}, {hi}]",
                adj.knob,
                adj.new_value
            );
        }
    }

    #[test]
    fn convergence_does_not_oscillate() {
        // Repeatedly applying decisions for a fixed "falling behind, publish-
        // bound" signal must monotonically increase the memtable toward the
        // ceiling and then STOP (no flip-flop).
        let live = LiveKnobs::new(knobs());
        let stats = IngestStats::new();
        warm(&stats, sample(1000, 150, 100, 20, 100), 20);
        stats.set_read_amp(1);
        let s = stats.snapshot();
        let mut last = live.values().inline_flush_max_bytes;
        let mut moves = 0;
        for _ in 0..50 {
            match decide(&s, &live.values(), &bounds(), ms(60_000), ms(30_000)) {
                Some(adj) if adj.knob == Knob::InlineFlushBytes => {
                    live.apply(&adj);
                    let now = live.values().inline_flush_max_bytes;
                    assert!(now >= last, "memtable must not shrink under this signal");
                    last = now;
                    moves += 1;
                }
                // Memtable converged; the controller moved on to another lever.
                _ => break,
            }
        }
        assert!(moves > 0, "should make progress");
        assert_eq!(
            live.values().inline_flush_max_bytes,
            bounds().inline_flush_max_bytes.1,
            "memtable converges to the ceiling, then the controller stops growing it"
        );
    }

    // ---- LiveKnobs apply coherence ---------------------------------------

    #[test]
    fn apply_inline_bytes_keeps_rows_and_segments_coherent() {
        let live = LiveKnobs::new(knobs());
        live.apply(&Adjustment {
            knob: Knob::InlineFlushBytes,
            new_value: 64 * 1024 * 1024,
            reason: "t",
        });
        let v = live.values();
        assert_eq!(v.inline_flush_max_bytes, 64 * 1024 * 1024);
        assert_eq!(v.inline_flush_max_rows, (64 * 1024 * 1024 / 1024).max(64));
        assert_eq!(
            v.inline_flush_max_segments,
            (64 * 1024 * 1024 / (128 * 1024)).clamp(16, 256)
        );
    }
}
