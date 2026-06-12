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
//! actually behaving and nudges the safe, per-operation actuators toward the
//! objective, correcting static-model error and workload drift (bursty CDC,
//! ramp-up, a host slower than its spec).
//!
//! It is a **feedback** controller, not feed-forward: it watches not just the
//! ingest rate (the input) but how the runtime is *responding* to it — whether
//! apply latency is keeping up with the offered load, read amplification, and
//! memory headroom — and acts on the gap to the objective.
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
//! [`LiveActuators`] at the use sites, running [`decide`] on the per-table
//! background task) lives in the provider.

#[cfg(target_os = "linux")]
use std::sync::OnceLock;
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

/// Fraction of ingested rows that are deletes above which the table is judged
/// "delete-heavy". Adding write shards (encode parallelism) to a delete-heavy
/// stream multiplies the per-burst small-file fan-out and worsens delete routing
/// off the in-memory tier, so the controller withholds the write-concurrency
/// lever here and leans on buffer/compaction levers instead.
const DELETE_HEAVY_FRACTION: f64 = 0.2;

/// Arrival-interval coefficient of variation (σ/μ) above which the offered load
/// is judged "bursty". A CV > 1 means the inter-batch gap's standard deviation
/// exceeds its mean — spiky, not steady. Bursty tables pre-grow the durability
/// buffer so a spike is absorbed in RAM instead of forcing a synchronous spill
/// (an apply stall) when it lands.
const BURSTY_ARRIVAL_CV: f64 = 1.0;

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

const MIB: i64 = 1024 * 1024;

/// Minimum live inline-memtable budget the adaptive loop may target.
const INLINE_FLUSH_MIN_BYTES: i64 = 2 * MIB;

/// Historical fallback ceiling when the runtime has not installed a memory budget.
const INLINE_FLUSH_FALLBACK_MAX_BYTES: i64 = 256 * MIB;

/// Absolute per-table ceiling when memory is known. This keeps adaptive from
/// turning a memory-rich host into one giant inlined snapshot, while still giving
/// it more room than the static warm start on SF-scale ingest.
const INLINE_FLUSH_ADAPTIVE_MAX_BYTES: i64 = 1024 * MIB;

const INLINE_FLUSH_BUDGET_FRACTION: u64 = 32;

/// Minimum per-table mem-tier byte cap the adaptive loop may target. Below this
/// the synchronous spill fires too often (each spill is a writer-blocking
/// checkpoint), so it is the floor even under memory pressure.
const MEM_TIER_MIN_BYTES: i64 = 64 * MIB;

/// Absolute per-table mem-tier ceiling when memory is known. The durability tier
/// is allowed to grow larger than the inline memtable because a bigger tier means
/// fewer writer-blocking spills (the freshness lever) and the process-global
/// mem-tier budget still bounds the aggregate resident RAM.
const MEM_TIER_ADAPTIVE_MAX_BYTES: i64 = 2 * 1024 * MIB;

/// Historical fallback mem-tier ceiling when no memory budget is installed.
const MEM_TIER_FALLBACK_MAX_BYTES: i64 = 1024 * MIB;

const MEM_TIER_BUDGET_FRACTION: u64 = 16;

// ---------------------------------------------------------------------------
// Environment detection: process memory budget + cgroup-aware usage
// ---------------------------------------------------------------------------

/// Process memory budget in bytes (the cgroup-aware limit), injected once at
/// startup by the runtime via [`set_global_memory_budget`]. `0` = unset, in
/// which case memory pressure is reported as unknown and the controller runs
/// without the memory rule. Process-wide because RAM is shared across tables.
static GLOBAL_MEMORY_BUDGET: AtomicU64 = AtomicU64::new(0);

#[cfg(target_os = "linux")]
static CGROUP_V2_MEMORY_CURRENT_PATH: OnceLock<Option<String>> = OnceLock::new();

#[cfg(target_os = "linux")]
static CGROUP_V1_MEMORY_USAGE_PATH: OnceLock<Option<String>> = OnceLock::new();

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

/// Compute the dynamic inline-memtable movement bounds for the table's static
/// warm-start value. The adaptive ceiling is intentionally memory-budgeted rather
/// than fixed: on memory-rich hosts the controller can grow far enough to reduce
/// small-file churn; under pressure, the memory rule still shrinks first.
#[must_use]
pub(crate) fn adaptive_inline_flush_bounds(initial_bytes: i64) -> (i64, i64) {
    adaptive_inline_flush_bounds_for_budget(initial_bytes, global_memory_budget())
}

fn adaptive_inline_flush_bounds_for_budget(
    initial_bytes: i64,
    memory_budget: Option<u64>,
) -> (i64, i64) {
    let initial = initial_bytes.max(INLINE_FLUSH_MIN_BYTES);
    let fallback_ceiling = initial
        .saturating_mul(4)
        .clamp(INLINE_FLUSH_MIN_BYTES, INLINE_FLUSH_FALLBACK_MAX_BYTES);

    let ceiling = memory_budget.map_or(fallback_ceiling, |budget| {
        let budget_ceiling = budget
            .checked_div(INLINE_FLUSH_BUDGET_FRACTION)
            .and_then(|bytes| i64::try_from(bytes).ok())
            .unwrap_or(i64::MAX)
            .clamp(INLINE_FLUSH_MIN_BYTES, INLINE_FLUSH_ADAPTIVE_MAX_BYTES);
        initial
            .saturating_mul(8)
            .min(budget_ceiling)
            .max(fallback_ceiling.min(budget_ceiling))
    });

    // Keep the upper bound at least at the warm start. If the process is already
    // over budget, the memory-pressure branch will make an explicit shrink move;
    // a behind/read-amp growth rule must never accidentally shrink because the
    // ceiling was below the current value.
    (INLINE_FLUSH_MIN_BYTES, ceiling.max(initial))
}

/// Compute the dynamic movement bounds for the in-memory CDC durability tier byte
/// cap from the table's static warm-start value. Like the inline-memtable bounds,
/// the ceiling is memory-budgeted (a larger tier on a memory-rich host means
/// fewer writer-blocking spills); the floor is [`MEM_TIER_MIN_BYTES`] so even
/// under pressure the synchronous spill never fires pathologically often.
///
/// A non-positive `initial` means the operator (or the static tier) chose "no
/// per-table cap" — return a collapsed `(0, 0)` range so the controller leaves it
/// alone and the read accessor keeps treating it as "no cap" (the global mem-tier
/// budget still bounds aggregate RAM).
#[must_use]
pub(crate) fn adaptive_mem_tier_bounds(initial_bytes: i64) -> (i64, i64) {
    adaptive_mem_tier_bounds_for_budget(initial_bytes, global_memory_budget())
}

fn adaptive_mem_tier_bounds_for_budget(
    initial_bytes: i64,
    memory_budget: Option<u64>,
) -> (i64, i64) {
    if initial_bytes <= 0 {
        return (0, 0);
    }
    let initial = initial_bytes.max(MEM_TIER_MIN_BYTES);
    let fallback_ceiling = initial
        .saturating_mul(4)
        .clamp(MEM_TIER_MIN_BYTES, MEM_TIER_FALLBACK_MAX_BYTES);

    let ceiling = memory_budget.map_or(fallback_ceiling, |budget| {
        let budget_ceiling = budget
            .checked_div(MEM_TIER_BUDGET_FRACTION)
            .and_then(|bytes| i64::try_from(bytes).ok())
            .unwrap_or(i64::MAX)
            .clamp(MEM_TIER_MIN_BYTES, MEM_TIER_ADAPTIVE_MAX_BYTES);
        initial
            .saturating_mul(8)
            .min(budget_ceiling)
            .max(fallback_ceiling.min(budget_ceiling))
    });

    (MEM_TIER_MIN_BYTES, ceiling.max(initial))
}

/// Current process/cgroup memory usage in bytes — cgroup v2 (`memory.current`)
/// then v1 (`memory.usage_in_bytes`); `None` when unavailable. This is the
/// "detect the environment and adjust" read that closes the loop on memory.
#[cfg(target_os = "linux")]
fn current_memory_bytes() -> Option<u64> {
    cgroup_v2_memory_current()
        .or_else(cgroup_v1_memory_current)
        .or_else(proc_self_rss_bytes)
}

#[cfg(target_os = "linux")]
fn cgroup_v2_memory_current() -> Option<u64> {
    read_u64_file(
        CGROUP_V2_MEMORY_CURRENT_PATH
            .get_or_init(resolve_cgroup_v2_memory_current_path)
            .as_deref()?,
    )
}

#[cfg(target_os = "linux")]
fn cgroup_v1_memory_current() -> Option<u64> {
    read_u64_file(
        CGROUP_V1_MEMORY_USAGE_PATH
            .get_or_init(resolve_cgroup_v1_memory_usage_path)
            .as_deref()?,
    )
}

#[cfg(target_os = "linux")]
fn resolve_cgroup_v2_memory_current_path() -> Option<String> {
    let cgroup_path = process_cgroup_v2_path()?;
    let mountpoint = cgroup2_mountpoint().unwrap_or_else(|| "/sys/fs/cgroup".to_string());
    Some(cgroup_file_path(
        &mountpoint,
        &cgroup_path,
        "memory.current",
    ))
}

#[cfg(target_os = "linux")]
fn resolve_cgroup_v1_memory_usage_path() -> Option<String> {
    let cgroup_path = process_cgroup_v1_path("memory")?;
    let mountpoint =
        cgroup_v1_mountpoint("memory").unwrap_or_else(|| "/sys/fs/cgroup/memory".to_string());
    Some(cgroup_file_path(
        &mountpoint,
        &cgroup_path,
        "memory.usage_in_bytes",
    ))
}

#[cfg(target_os = "linux")]
fn read_u64_file(path: &str) -> Option<u64> {
    std::fs::read_to_string(path).ok()?.trim().parse().ok()
}

#[cfg(target_os = "linux")]
fn cgroup_file_path(mountpoint: &str, cgroup_path: &str, filename: &str) -> String {
    if cgroup_path == "/" || cgroup_path.is_empty() {
        format!("{mountpoint}/{filename}")
    } else {
        format!("{mountpoint}{cgroup_path}/{filename}")
    }
}

#[cfg(target_os = "linux")]
fn process_cgroup_v2_path() -> Option<String> {
    parse_proc_cgroup_v2_path(&std::fs::read_to_string("/proc/self/cgroup").ok()?)
}

#[cfg(target_os = "linux")]
fn process_cgroup_v1_path(controller: &str) -> Option<String> {
    parse_proc_cgroup_v1_path(
        &std::fs::read_to_string("/proc/self/cgroup").ok()?,
        controller,
    )
}

#[cfg(target_os = "linux")]
fn parse_proc_cgroup_v2_path(contents: &str) -> Option<String> {
    contents.lines().find_map(|line| {
        line.strip_prefix("0::").map(|path| {
            let trimmed = path.trim();
            if trimmed.is_empty() {
                "/".to_string()
            } else {
                trimmed.to_string()
            }
        })
    })
}

#[cfg(target_os = "linux")]
fn parse_proc_cgroup_v1_path(contents: &str, controller: &str) -> Option<String> {
    contents.lines().find_map(|line| {
        let mut parts = line.splitn(3, ':');
        let _hierarchy = parts.next()?;
        let controllers = parts.next()?;
        let path = parts.next()?.trim();
        controllers.split(',').any(|c| c == controller).then(|| {
            if path.is_empty() {
                "/".to_string()
            } else {
                path.to_string()
            }
        })
    })
}

#[cfg(target_os = "linux")]
fn cgroup2_mountpoint() -> Option<String> {
    parse_mountinfo_cgroup2(&std::fs::read_to_string("/proc/self/mountinfo").ok()?)
}

#[cfg(target_os = "linux")]
fn cgroup_v1_mountpoint(controller: &str) -> Option<String> {
    parse_mountinfo_cgroup_v1(
        &std::fs::read_to_string("/proc/self/mountinfo").ok()?,
        controller,
    )
}

#[cfg(target_os = "linux")]
fn parse_mountinfo_cgroup2(contents: &str) -> Option<String> {
    contents.lines().find_map(|line| {
        let (mount, fs) = line.split_once(" - ")?;
        (fs.split_whitespace().next()? == "cgroup2")
            .then(|| mount.split_whitespace().nth(4).map(ToString::to_string))?
    })
}

#[cfg(target_os = "linux")]
fn parse_mountinfo_cgroup_v1(contents: &str, controller: &str) -> Option<String> {
    contents.lines().find_map(|line| {
        let (mount, fs) = line.split_once(" - ")?;
        let mut fs_parts = fs.split_whitespace();
        if fs_parts.next()? != "cgroup" {
            return None;
        }
        let _source = fs_parts.next()?;
        let super_options = fs_parts.next()?;
        super_options
            .split(',')
            .any(|opt| opt == controller)
            .then(|| mount.split_whitespace().nth(4).map(ToString::to_string))?
    })
}

#[cfg(target_os = "linux")]
fn proc_self_rss_bytes() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    status.lines().find_map(|line| {
        let rest = line.strip_prefix("VmRSS:")?.trim();
        let kb = rest.split_whitespace().next()?.parse::<u64>().ok()?;
        kb.checked_mul(1024)
    })
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
        stats.set_mem_pressure(u64_to_f64(used) / u64_to_f64(budget));
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
    /// In-memory Arrow bytes applied in this batch.
    pub bytes: u64,
    /// Total apply wall time for the batch (the runtime's response: how long
    /// *we* took to absorb it).
    ///
    /// Under `cdc_durability: memory` this includes any synchronous mem-tier
    /// admission stall — the per-table BYTE-cap spill and the global-budget
    /// wait — which are the only writer-blocking mem-tier events: the tier's
    /// AGE cap is enforced by the non-blocking background checkpoint tick and
    /// never appears here. The `apply_vs_arrival` "behind" signal therefore
    /// reflects genuine backpressure, not scheduled age flushes.
    pub apply: Duration,
    /// Wall time since the previous batch (the offered-load interval). `None`
    /// for the first batch.
    pub arrival_gap: Option<Duration>,
    /// Rows in this batch that were deletes (tombstones / on-conflict deletions).
    /// Feeds the delete-fraction signal: a delete-heavy stream must NOT be sped
    /// up by adding write shards (more shards multiply the per-burst small-file
    /// fan-out and worsen delete routing off the in-memory tier).
    pub delete_rows: u64,
}

#[derive(Debug, Default, Clone, Copy)]
struct EwmaInner {
    rows_per_sec: f64,
    bytes_per_sec: f64,
    apply_ms: f64,
    arrival_gap_ms: f64,
    /// EWMA of `arrival_gap_ms²`, paired with `arrival_gap_ms` to derive the
    /// arrival-interval variance (and thus the burstiness CV) without storing a
    /// history: `Var = E[x²] − E[x]²`.
    arrival_gap_ms_sq: f64,
    /// EWMA of the per-batch delete fraction (`delete_rows / rows`), in `[0, 1]`.
    delete_fraction: f64,
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
        // Offered rate = rows / inter-batch interval. Fall back to the apply
        // window for the first batch (no gap yet) so a single batch still yields
        // a finite rate.
        let window_ms = s
            .arrival_gap
            .map_or(apply_ms, duration_ms)
            .max(f64::from(1_u32) / 1000.0);
        let inst_rows_per_sec = u64_to_f64(s.rows) * 1000.0 / window_ms;
        let inst_bytes_per_sec = u64_to_f64(s.bytes) * 1000.0 / window_ms;
        let arrival_gap_ms = s.arrival_gap.map_or(apply_ms, duration_ms);
        // Per-batch delete fraction, clamped to [0, 1] (deletes can't exceed the
        // batch's rows; guard the rows==0 corner so an empty batch contributes 0).
        let inst_delete_fraction = if s.rows > 0 {
            (u64_to_f64(s.delete_rows) / u64_to_f64(s.rows)).clamp(0.0, 1.0)
        } else {
            0.0
        };

        let mut inner = self.inner.lock();
        let prior = inner.samples;
        ewma(&mut inner.rows_per_sec, inst_rows_per_sec, prior);
        ewma(&mut inner.bytes_per_sec, inst_bytes_per_sec, prior);
        ewma(&mut inner.apply_ms, apply_ms, prior);
        ewma(&mut inner.arrival_gap_ms, arrival_gap_ms, prior);
        ewma(
            &mut inner.arrival_gap_ms_sq,
            arrival_gap_ms * arrival_gap_ms,
            prior,
        );
        ewma(&mut inner.delete_fraction, inst_delete_fraction, prior);
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
    #[expect(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    pub fn set_mem_pressure(&self, fraction: f64) {
        if !fraction.is_finite() || fraction < 0.0 {
            return;
        }
        // Cap at a sane 1000× (pressure is ~0..2 in practice) so the f64→u64
        // cast can't overflow; ×1000 keeps three decimals of resolution.
        let milli = (fraction.min(1000.0) * 1000.0).round() as u64;
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
        let mem_pressure = match self.mem_pressure_milli.load(Ordering::Relaxed) {
            u64::MAX => None,
            milli => Some(u64_to_f64(milli) / 1000.0),
        };
        // Burstiness as the coefficient of variation of the inter-batch interval:
        // σ/μ = sqrt(E[x²] − E[x]²) / E[x]. CV ≈ 0 is a metronome-steady stream;
        // CV > 1 means the gap's spread exceeds its mean (spiky). `0` until the
        // mean is positive (cold start) so it can't fire a spurious "bursty".
        let arrival_cv = if inner.arrival_gap_ms > 0.0 {
            let variance =
                (inner.arrival_gap_ms_sq - inner.arrival_gap_ms * inner.arrival_gap_ms).max(0.0);
            variance.sqrt() / inner.arrival_gap_ms
        } else {
            0.0
        };
        IngestSnapshot {
            rows_per_sec: inner.rows_per_sec,
            // Real bytes/sec once the first write has been recorded; -1.0 before
            // then (cold start) so the gauge is suppressed rather than emitting 0.
            bytes_per_sec: if self.total_bytes.load(Ordering::Relaxed) > 0 {
                inner.bytes_per_sec
            } else {
                -1.0
            },
            apply_ms: inner.apply_ms,
            arrival_gap_ms: inner.arrival_gap_ms,
            apply_vs_arrival,
            read_amp: self.read_amp.load(Ordering::Relaxed),
            mem_pressure,
            delete_fraction: inner.delete_fraction,
            arrival_cv,
            samples: inner.samples,
        }
    }
}

/// Derived, point-in-time signals the controller reasons over.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct IngestSnapshot {
    pub rows_per_sec: f64,
    /// Bytes/sec (EWMA). `-1.0` before the first write (cold start), when the
    /// gauge is suppressed rather than reporting a 0.
    pub bytes_per_sec: f64,
    pub apply_ms: f64,
    pub arrival_gap_ms: f64,
    /// `apply_ms / arrival_gap_ms`: > 1 ⇒ falling behind the offered load.
    pub apply_vs_arrival: f64,
    /// Small-file count (read amplification): the ingest→query coupling signal —
    /// high means ingest is producing files that slow scans.
    pub read_amp: usize,
    /// Memory usage as a fraction of the cgroup-aware budget (`used / budget`);
    /// `None` when no budget/sample is available. `> 1.0` means over budget.
    pub mem_pressure: Option<f64>,
    /// EWMA fraction of ingested rows that are deletes, in `[0, 1]`. High means
    /// the table is delete-heavy: speeding ingest by adding write shards would
    /// multiply small-file fan-out and worsen delete routing, so that lever is
    /// withheld here.
    pub delete_fraction: f64,
    /// Coefficient of variation (σ/μ) of the inter-batch arrival interval. ~0 is
    /// a steady stream; `> 1` is bursty/spiky. Bursty tables pre-grow the
    /// durability buffer so a spike lands in RAM instead of forcing a
    /// writer-blocking spill.
    pub arrival_cv: f64,
    pub samples: u64,
}

// ---------------------------------------------------------------------------
// Actuators: live, atomically-updatable control parameters
// ---------------------------------------------------------------------------

/// The subset of Vortex parameters that are safe to adjust at runtime because
/// they are read fresh per operation (no allocate-once state). Initialized from
/// the static config; the controller updates them, and the write/compaction
/// paths read them in place of the frozen config values.
#[derive(Debug)]
pub(crate) struct LiveActuators {
    inline_flush_max_bytes: AtomicI64,
    inline_flush_max_rows: AtomicI64,
    inline_flush_max_segments: AtomicI64,
    compaction_background_interval_ms: AtomicU64,
    compaction_trigger_files: AtomicUsize,
    /// 0 means "unset" (use the session/default write concurrency).
    write_concurrency: AtomicUsize,
    /// Per-table in-memory CDC durability tier byte cap (`cdc_durability: memory`).
    /// `<= 0` means "no per-table cap" (the process-global mem-tier budget still
    /// bounds aggregate RAM). The synchronous-spill freshness lever: a larger cap
    /// means fewer writer-blocking spills, so the controller grows it under
    /// backpressure (when memory allows) and shrinks it under memory pressure.
    mem_tier_max_bytes: AtomicI64,
    /// Observed bytes-per-row, seeded from the initial (schema-aware) config and
    /// then *relearned* from live ingest (EWMA bytes ÷ rows) so the row cap a byte
    /// budget implies tracks the table's real row width — not a stale static
    /// estimate. Atomic because the background tick updates it. Used when `apply`
    /// recomputes the row cap from a new byte budget, so a narrow-row table does
    /// not snap to the ~1 KiB/row fallback and flush by rows too early.
    bytes_per_row: AtomicI64,
    /// Static bytes-per-segment ratio from the initial config. Unlike row width
    /// this is an IPC-segmentation artifact, not directly observable from ingest,
    /// so it stays fixed. Immutable, so a plain int.
    bytes_per_segment: i64,
}

impl LiveActuators {
    #[must_use]
    pub fn new(init: ActuatorValues) -> Self {
        Self {
            inline_flush_max_bytes: AtomicI64::new(init.inline_flush_max_bytes),
            inline_flush_max_rows: AtomicI64::new(init.inline_flush_max_rows),
            inline_flush_max_segments: AtomicI64::new(init.inline_flush_max_segments),
            compaction_background_interval_ms: AtomicU64::new(
                init.compaction_background_interval_ms,
            ),
            compaction_trigger_files: AtomicUsize::new(init.compaction_trigger_files),
            write_concurrency: AtomicUsize::new(init.write_concurrency),
            mem_tier_max_bytes: AtomicI64::new(init.mem_tier_max_bytes),
            bytes_per_row: AtomicI64::new(
                (init.inline_flush_max_bytes / init.inline_flush_max_rows.max(1)).max(1),
            ),
            bytes_per_segment: (init.inline_flush_max_bytes
                / init.inline_flush_max_segments.max(1))
            .max(1),
        }
    }

    #[must_use]
    pub fn values(&self) -> ActuatorValues {
        ActuatorValues {
            inline_flush_max_bytes: self.inline_flush_max_bytes.load(Ordering::Relaxed),
            inline_flush_max_rows: self.inline_flush_max_rows.load(Ordering::Relaxed),
            inline_flush_max_segments: self.inline_flush_max_segments.load(Ordering::Relaxed),
            compaction_background_interval_ms: self
                .compaction_background_interval_ms
                .load(Ordering::Relaxed),
            compaction_trigger_files: self.compaction_trigger_files.load(Ordering::Relaxed),
            write_concurrency: self.write_concurrency.load(Ordering::Relaxed),
            mem_tier_max_bytes: self.mem_tier_max_bytes.load(Ordering::Relaxed),
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
        self.compaction_background_interval_ms
            .load(Ordering::Relaxed)
    }
    pub fn compaction_trigger_files(&self) -> usize {
        self.compaction_trigger_files.load(Ordering::Relaxed)
    }
    pub fn write_concurrency(&self) -> usize {
        self.write_concurrency.load(Ordering::Relaxed)
    }
    pub fn mem_tier_max_bytes(&self) -> i64 {
        self.mem_tier_max_bytes.load(Ordering::Relaxed)
    }

    /// Relearn the observed bytes-per-row from live ingest so a later byte-budget
    /// change derives a row cap matching the table's real row width. Clamped to a
    /// sane `[1 B, 16 MiB]` per row; callers pass the EWMA `bytes_per_sec /
    /// rows_per_sec`. Cheap (one atomic store), called on the background tick.
    pub fn observe_mean_row_bytes(&self, bytes_per_row: i64) {
        let clamped = bytes_per_row.clamp(1, 16 * MIB);
        self.bytes_per_row.store(clamped, Ordering::Relaxed);
    }

    /// Apply a controller decision. The new value is already clamped to bounds
    /// by [`decide`]; this just stores it.
    pub fn apply(&self, adj: &Adjustment) {
        match adj.actuator {
            Actuator::InlineFlushBytes => {
                let bytes = i64::try_from(adj.new_value).unwrap_or(i64::MAX);
                self.inline_flush_max_bytes.store(bytes, Ordering::Relaxed);
                // Recompute rows/segments preserving the bytes-per-row (relearned
                // from live ingest) and the static bytes-per-segment ratio, so
                // growing the byte budget doesn't reset the row cap to the ~1 KiB/
                // row fallback and prematurely flush a narrow-row table by rows.
                let bytes_per_row = self.bytes_per_row.load(Ordering::Relaxed).max(1);
                let rows = (bytes / bytes_per_row).max(64);
                let segs = (bytes / self.bytes_per_segment).clamp(16, 256);
                self.inline_flush_max_rows.store(rows, Ordering::Relaxed);
                self.inline_flush_max_segments
                    .store(segs, Ordering::Relaxed);
            }
            Actuator::MemTierMaxBytes => {
                let bytes = i64::try_from(adj.new_value).unwrap_or(i64::MAX);
                self.mem_tier_max_bytes.store(bytes, Ordering::Relaxed);
            }
            Actuator::CompactionIntervalMs => {
                self.compaction_background_interval_ms
                    .store(adj.new_value, Ordering::Relaxed);
            }
            Actuator::CompactionTriggerFiles => {
                self.compaction_trigger_files.store(
                    usize::try_from(adj.new_value).unwrap_or(usize::MAX),
                    Ordering::Relaxed,
                );
            }
            Actuator::WriteConcurrency => {
                self.write_concurrency.store(
                    usize::try_from(adj.new_value).unwrap_or(usize::MAX),
                    Ordering::Relaxed,
                );
            }
        }
    }
}

/// A plain snapshot of the live actuator values (for the decider + observability).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ActuatorValues {
    pub inline_flush_max_bytes: i64,
    pub inline_flush_max_rows: i64,
    pub inline_flush_max_segments: i64,
    pub compaction_background_interval_ms: u64,
    pub compaction_trigger_files: usize,
    pub write_concurrency: usize,
    pub mem_tier_max_bytes: i64,
}

/// Static `[floor, ceiling]` per dynamically-tuned actuator, derived by the static
/// tier. The controller can never move an actuator outside these, so dynamic
/// tuning is bounded by — and can only improve on — the static config.
#[derive(Debug, Clone, Copy)]
pub(crate) struct TuningBounds {
    pub inline_flush_max_bytes: (i64, i64),
    pub compaction_background_interval_ms: (u64, u64),
    pub compaction_trigger_files: (usize, usize),
    pub write_concurrency: (usize, usize),
    pub mem_tier_max_bytes: (i64, i64),
}

// ---------------------------------------------------------------------------
// Controller: pure decision
// ---------------------------------------------------------------------------

/// Which actuator an [`Adjustment`] targets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Actuator {
    InlineFlushBytes,
    MemTierMaxBytes,
    CompactionIntervalMs,
    CompactionTriggerFiles,
    WriteConcurrency,
}

impl Actuator {
    /// Stable label for metrics/logs.
    #[must_use]
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::InlineFlushBytes => "inline_flush_bytes",
            Self::MemTierMaxBytes => "mem_tier_max_bytes",
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
    pub actuator: Actuator,
    /// New value (units depend on `actuator`; already clamped to bounds).
    pub new_value: u64,
    pub reason: &'static str,
}

/// Decide the single best bounded actuator move for the current behavior, or
/// `None` to hold.
///
/// Pure: no I/O, no clock — `since_last` (time since the previous applied move)
/// is passed in so the dwell-time hysteresis is testable, and
/// `samples_at_last_move` (the batch count when the last move was applied) so the
/// fresh-sample gate is testable. Rules are evaluated in objective-priority order
/// and the first applicable one wins (one move per tick).
#[must_use]
pub(crate) fn decide(
    s: &IngestSnapshot,
    cur: &ActuatorValues,
    b: &TuningBounds,
    since_last: Duration,
    min_dwell: Duration,
    samples_at_last_move: u64,
) -> Option<Adjustment> {
    // Don't act on a cold table, and respect the dwell time so moves don't
    // stack faster than their effect can be observed.
    if s.samples < WARMUP_BATCHES || since_last < min_dwell {
        return None;
    }

    let mem_high = s.mem_pressure.is_some_and(|p| p > MEM_PRESSURE_HIGH);
    let mem_ok = s.mem_pressure.is_none_or(|p| p < MEM_PRESSURE_OK);
    // Fresh-sample gate: the rate/latency EWMAs only advance on a CDC write, so a
    // table that fell behind and then went idle keeps a stale `apply_vs_arrival`
    // above 1 indefinitely. Without this gate the controller would re-fire the
    // "behind"/burst rules every dwell window and ratchet every actuator to its
    // aggressive extreme on a table doing no work. Require genuinely new ingest
    // since the last applied move before acting on the *write-derived* signals.
    // The *environment* signals (memory pressure, read-amp) are sampled fresh on
    // the tick and are deliberately NOT gated by this.
    let ingest_fresh = s.samples > samples_at_last_move;
    let behind = ingest_fresh && s.apply_vs_arrival > BEHIND_RATIO;
    let read_amp_high = s.read_amp > READ_AMP_HIGH;
    let delete_heavy = s.delete_fraction > DELETE_HEAVY_FRACTION;
    let bursty = ingest_fresh && s.arrival_cv > BURSTY_ARRIVAL_CV;

    // (1) Memory pressure [hard, highest priority]: the cgroup-aware budget is
    // nearly exhausted. Shrink the two live memory buffers — the inline memtable
    // first, then the in-memory CDC durability tier — toward their floors, one per
    // tick. Running first means no growth rule below can enlarge memory on an
    // already-tight box; query read-amp is instead relieved by compaction (which
    // costs CPU, not memory).
    if mem_high {
        if let Some(v) = clamp_move_i64(
            cur.inline_flush_max_bytes,
            shrink_i64(cur.inline_flush_max_bytes),
            b.inline_flush_max_bytes,
        ) {
            return Some(Adjustment {
                actuator: Actuator::InlineFlushBytes,
                new_value: u64::try_from(v).unwrap_or(0),
                reason: "memory pressure: shrink memtable to stay within the cgroup budget",
            });
        }
        if let Some(v) = clamp_move_i64(
            cur.mem_tier_max_bytes,
            shrink_i64(cur.mem_tier_max_bytes),
            b.mem_tier_max_bytes,
        ) {
            return Some(Adjustment {
                actuator: Actuator::MemTierMaxBytes,
                new_value: u64::try_from(v).unwrap_or(0),
                reason: "memory pressure: shrink the in-memory CDC tier cap to free RAM",
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
            if mem_ok
                && let Some(v) = clamp_move_i64(
                    cur.inline_flush_max_bytes,
                    grow_i64(cur.inline_flush_max_bytes),
                    b.inline_flush_max_bytes,
                )
            {
                return Some(Adjustment {
                    actuator: Actuator::InlineFlushBytes,
                    new_value: u64::try_from(v).unwrap_or(0),
                    reason: "high read-amp: larger memtable → fewer small files for queries",
                });
            }
            if let Some(v) = clamp_move_u64(
                cur.compaction_background_interval_ms,
                shrink_u64(cur.compaction_background_interval_ms),
                b.compaction_background_interval_ms,
            ) {
                return Some(Adjustment {
                    actuator: Actuator::CompactionIntervalMs,
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
                    actuator: Actuator::CompactionTriggerFiles,
                    new_value: u64::try_from(v).unwrap_or(0),
                    reason: "high read-amp: lower compaction trigger",
                });
            }
        }

        // (2) Ingest throughput, only when ingest is actually behind. The levers
        // are tried in a robust, observable order rather than from per-write phase
        // timings (which aren't reliable on the staged CDC path, where publish is
        // backgrounded):
        if behind {
            // 1. Enlarge the memtable first — the safest, most generally-helpful
            //    lever for a behind table: fewer, larger files AND amortized
            //    metastore commits (the common bottleneck). Only while memory ok.
            if mem_ok
                && let Some(v) = clamp_move_i64(
                    cur.inline_flush_max_bytes,
                    grow_i64(cur.inline_flush_max_bytes),
                    b.inline_flush_max_bytes,
                )
            {
                return Some(Adjustment {
                    actuator: Actuator::InlineFlushBytes,
                    new_value: u64::try_from(v).unwrap_or(0),
                    reason: "falling behind: enlarge memtable (fewer files + amortized commits)",
                });
            }
            // 2. Enlarge the in-memory CDC durability tier next: a larger cap means
            //    the synchronous, writer-blocking spill fires less often, directly
            //    cutting apply-stall on the `cdc_durability: memory` path. Only
            //    while memory ok; the global mem-tier budget still bounds aggregate.
            if mem_ok
                && let Some(v) = clamp_move_i64(
                    cur.mem_tier_max_bytes,
                    grow_i64(cur.mem_tier_max_bytes),
                    b.mem_tier_max_bytes,
                )
            {
                return Some(Adjustment {
                    actuator: Actuator::MemTierMaxBytes,
                    new_value: u64::try_from(v).unwrap_or(0),
                    reason: "falling behind: enlarge the in-memory CDC tier (fewer writer-blocking spills)",
                });
            }
            // 3. Buffers maxed (or memory tight) and queries are NOT read-amp-bound
            //    → add encode parallelism. Gated on low read-amp because more shards
            //    mean more files; ALSO withheld when the stream is delete-heavy,
            //    where extra shards multiply the per-burst small-file fan-out and
            //    worsen delete routing off the in-memory tier.
            if s.read_amp <= READ_AMP_LOW
                && !delete_heavy
                && let Some(v) = clamp_move_usize(
                    cur.write_concurrency.max(1),
                    grow_usize(cur.write_concurrency.max(1)),
                    b.write_concurrency,
                )
            {
                return Some(Adjustment {
                    actuator: Actuator::WriteConcurrency,
                    new_value: u64::try_from(v).unwrap_or(0),
                    reason: "falling behind, buffers maxed: raise write concurrency",
                });
            }
            // 4. Last resort: compact more to keep the snapshot lean.
            if let Some(v) = clamp_move_u64(
                cur.compaction_background_interval_ms,
                shrink_u64(cur.compaction_background_interval_ms),
                b.compaction_background_interval_ms,
            ) {
                return Some(Adjustment {
                    actuator: Actuator::CompactionIntervalMs,
                    new_value: v,
                    reason: "falling behind: compact more to keep the snapshot lean",
                });
            }
        }
        return None;
    }

    // (3b) Bursty pre-sizing: not behind right now, but the arrival pattern is
    // spiky (high inter-batch CV). Grow the durability tier proactively so the
    // next spike lands in RAM instead of forcing a synchronous (writer-blocking)
    // spill. Only when memory allows and read-amp is healthy — a bigger tier is
    // free of query cost there. This is the controller's response to *spikiness*,
    // distinct from the mean-rate "behind" response above.
    if bursty
        && mem_ok
        && s.read_amp <= READ_AMP_LOW
        && let Some(v) = clamp_move_i64(
            cur.mem_tier_max_bytes,
            grow_i64(cur.mem_tier_max_bytes),
            b.mem_tier_max_bytes,
        )
    {
        return Some(Adjustment {
            actuator: Actuator::MemTierMaxBytes,
            new_value: u64::try_from(v).unwrap_or(0),
            reason: "bursty arrivals: pre-grow the in-memory CDC tier to absorb spikes without a spill stall",
        });
    }

    // (4) Healthy on every axis (ingest caught up, queries not read-amp-bound,
    // memory comfortable) → relax the actuators that cost queries/CPU back toward
    // their efficient defaults, one per tick in priority order: shed a write shard
    // (each adds files), then relax the compaction trigger up (less eager), then
    // lengthen the background compaction interval. The hysteresis gap
    // (HEALTHY_RATIO 0.5 vs BEHIND_RATIO 1.2) plus the dwell time keep these from
    // fighting the "behind" rules. The memory buffers (memtable, mem-tier) are
    // deliberately NOT shrunk here — there is no memory pressure, and keeping them
    // sized leaves the table ready for the next burst at no query/CPU cost.
    if s.apply_vs_arrival < HEALTHY_RATIO && s.read_amp <= READ_AMP_LOW && mem_ok {
        if let Some(v) = clamp_move_usize(
            cur.write_concurrency.max(1),
            shrink_usize(cur.write_concurrency.max(1)),
            b.write_concurrency,
        ) {
            return Some(Adjustment {
                actuator: Actuator::WriteConcurrency,
                new_value: u64::try_from(v).unwrap_or(0),
                reason: "healthy: shed a write shard to reduce small-file fan-out",
            });
        }
        if let Some(v) = clamp_move_usize(
            cur.compaction_trigger_files,
            grow_usize(cur.compaction_trigger_files),
            b.compaction_trigger_files,
        ) {
            return Some(Adjustment {
                actuator: Actuator::CompactionTriggerFiles,
                new_value: u64::try_from(v).unwrap_or(0),
                reason: "healthy: relax the compaction trigger to reduce background churn",
            });
        }
        if let Some(v) = clamp_move_u64(
            cur.compaction_background_interval_ms,
            grow_u64(cur.compaction_background_interval_ms),
            b.compaction_background_interval_ms,
        ) {
            return Some(Adjustment {
                actuator: Actuator::CompactionIntervalMs,
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

// One adjustment step is ×3/2 (grow) or ×2/3 (shrink) — i.e. ±50% — done in
// integer math to avoid lossy float casts. `grow` guarantees at least +1 so a
// small value still makes progress; `saturating_mul` guards the (practically
// impossible) overflow near the ceiling, where the clamp holds it anyway.
fn grow_u64(v: u64) -> u64 {
    (v.saturating_mul(3) / 2).max(v.saturating_add(1))
}

fn shrink_u64(v: u64) -> u64 {
    v * 2 / 3
}

fn grow_usize(v: usize) -> usize {
    (v.saturating_mul(3) / 2).max(v.saturating_add(1))
}

fn shrink_usize(v: usize) -> usize {
    v * 2 / 3
}

/// Grow an `i64` actuator by one step (non-negative inputs only; clamped at use).
fn grow_i64(v: i64) -> i64 {
    i64::try_from(grow_u64(u64::try_from(v.max(0)).unwrap_or(0))).unwrap_or(i64::MAX)
}

/// Shrink an `i64` actuator by one step.
fn shrink_i64(v: i64) -> i64 {
    i64::try_from(shrink_u64(u64::try_from(v.max(0)).unwrap_or(0))).unwrap_or(0)
}

/// `u64` → `f64` for rate/ratio/pressure math. The precision loss is acceptable:
/// these are EWMA estimates and metrics whose magnitudes never approach 2^52.
#[expect(clippy::cast_precision_loss)]
fn u64_to_f64(v: u64) -> f64 {
    v as f64
}

/// Clamp `target` to `[lo, hi]` and return it only if it differs from `cur`
/// (otherwise the actuator is already at its useful extreme — no-op, hold).
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
    #![allow(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        clippy::cast_precision_loss
    )]
    use super::*;

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    fn sample(rows: u64, apply_ms: u64, gap_ms: u64) -> WriteSample {
        sample_del(rows, 0, apply_ms, gap_ms)
    }

    fn sample_del(rows: u64, delete_rows: u64, apply_ms: u64, gap_ms: u64) -> WriteSample {
        WriteSample {
            rows,
            bytes: rows * 256,
            apply: ms(apply_ms),
            arrival_gap: Some(ms(gap_ms)),
            delete_rows,
        }
    }

    fn bounds() -> TuningBounds {
        TuningBounds {
            inline_flush_max_bytes: (2 * 1024 * 1024, 128 * 1024 * 1024),
            compaction_background_interval_ms: (2_000, 60_000),
            compaction_trigger_files: (2, 32),
            write_concurrency: (1, 16),
            mem_tier_max_bytes: (64 * 1024 * 1024, 2048 * 1024 * 1024),
        }
    }

    fn actuators() -> ActuatorValues {
        ActuatorValues {
            inline_flush_max_bytes: 8 * 1024 * 1024,
            inline_flush_max_rows: 8192,
            inline_flush_max_segments: 64,
            compaction_background_interval_ms: 10_000,
            compaction_trigger_files: 8,
            write_concurrency: 4,
            mem_tier_max_bytes: 256 * 1024 * 1024,
        }
    }

    /// A baseline, *healthy* snapshot past the warmup gate. Controller tests
    /// override just the fields they exercise — deterministic, no EWMA shaping.
    fn snap() -> IngestSnapshot {
        IngestSnapshot {
            rows_per_sec: 10_000.0,
            bytes_per_sec: 2_560_000.0,
            apply_ms: 20.0,
            arrival_gap_ms: 100.0,
            apply_vs_arrival: 0.2,
            read_amp: 1,
            mem_pressure: None,
            delete_fraction: 0.0,
            arrival_cv: 0.0,
            samples: WARMUP_BATCHES + 4,
        }
    }

    /// `decide` with the dwell satisfied and the fresh-sample gate open (the last
    /// move was at sample 0, so any warmed snapshot counts as fresh) — the common
    /// case for single-move tests.
    fn decide_fresh(
        s: &IngestSnapshot,
        cur: &ActuatorValues,
        b: &TuningBounds,
    ) -> Option<Adjustment> {
        decide(s, cur, b, ms(60_000), ms(30_000), 0)
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
        warm(&stats, sample(1000, 20, 100), 20);
        let s = stats.snapshot();
        assert_eq!(s.samples, 20);
        assert!(
            (s.rows_per_sec - 10_000.0).abs() < 1.0,
            "rows/s ~10000, got {}",
            s.rows_per_sec
        );
        assert!(
            s.apply_vs_arrival < 0.5,
            "20ms apply vs 100ms gap → keeping up"
        );
        assert_eq!(stats.total_rows.load(Ordering::Relaxed), 20_000);
        assert_eq!(stats.total_batches.load(Ordering::Relaxed), 20);
    }

    #[test]
    fn apply_vs_arrival_detects_falling_behind() {
        let stats = IngestStats::new();
        // apply 150 ms but batches arrive every 100 ms → ratio 1.5 > 1.
        warm(&stats, sample(1000, 150, 100), 20);
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
            arrival_gap: None,
            delete_rows: 0,
        });
        let s = stats.snapshot();
        assert!(s.rows_per_sec.is_finite() && s.rows_per_sec > 0.0);
    }

    #[test]
    fn delete_fraction_is_tracked() {
        let stats = IngestStats::new();
        // Half of every batch's rows are deletes ⇒ EWMA delete fraction → 0.5.
        warm(&stats, sample_del(1000, 500, 20, 100), 20);
        let s = stats.snapshot();
        assert!(
            (s.delete_fraction - 0.5).abs() < 0.01,
            "delete fraction ~0.5, got {}",
            s.delete_fraction
        );
    }

    #[test]
    fn arrival_cv_is_low_for_steady_and_high_for_bursty() {
        // Steady metronome stream ⇒ CV ≈ 0.
        let steady = IngestStats::new();
        warm(&steady, sample(1000, 20, 100), 30);
        assert!(
            steady.snapshot().arrival_cv < 0.1,
            "steady stream CV ≈ 0, got {}",
            steady.snapshot().arrival_cv
        );
        // Strongly bimodal arrivals (alternating 800 ms / 10 ms gaps, ending on a
        // short gap) ⇒ CV > 1: the spread of the interval exceeds its mean.
        let bursty = IngestStats::new();
        for i in 0..40 {
            let gap = if i % 2 == 0 { 800 } else { 10 };
            bursty.record_write(sample(1000, 5, gap));
        }
        assert!(
            bursty.snapshot().arrival_cv > BURSTY_ARRIVAL_CV,
            "bimodal stream is bursty, got CV {}",
            bursty.snapshot().arrival_cv
        );
    }

    // ---- controller: warmup + dwell --------------------------------------

    #[test]
    fn no_action_before_warmup() {
        let s = IngestSnapshot {
            apply_vs_arrival: 1.5,
            samples: WARMUP_BATCHES - 1,
            ..snap()
        };
        assert!(decide_fresh(&s, &actuators(), &bounds()).is_none());
    }

    #[test]
    fn no_action_within_dwell() {
        let s = IngestSnapshot {
            apply_vs_arrival: 1.5,
            ..snap()
        };
        // since_last < min_dwell ⇒ hold even though falling behind.
        assert!(decide(&s, &actuators(), &bounds(), ms(5_000), ms(30_000), 0).is_none());
    }

    // ---- P0: fresh-sample gate (no ratchet on an idle table) -------------

    #[test]
    fn behind_on_stale_samples_holds() {
        // Fell behind, then ingest went idle: the EWMA still reads behind, but no
        // new samples have arrived since the last move. The controller MUST hold
        // rather than ratchet every actuator to its extreme on a table doing no
        // work. (read-amp low, not bursty ⇒ nothing else fires either.)
        let s = IngestSnapshot {
            apply_vs_arrival: 1.5,
            samples: 40,
            ..snap()
        };
        assert!(
            decide(&s, &actuators(), &bounds(), ms(60_000), ms(30_000), 40).is_none(),
            "stale 'behind' must not trigger a move"
        );
    }

    #[test]
    fn behind_with_fresh_samples_acts() {
        // Same behind signal, but new batches HAVE arrived since the last move
        // (samples advanced past the recorded mark) ⇒ act.
        let s = IngestSnapshot {
            apply_vs_arrival: 1.5,
            samples: 45,
            ..snap()
        };
        let adj = decide(&s, &actuators(), &bounds(), ms(60_000), ms(30_000), 40).expect("acts");
        assert_eq!(adj.actuator, Actuator::InlineFlushBytes);
    }

    // ---- controller: diagnosis branches ----------------------------------

    #[test]
    fn high_read_amp_enlarges_memtable_then_compacts() {
        // Ingest↔query coupling: fix at the source first — a bigger memtable
        // checkpoints fewer, larger files, so ingest stops slowing scans.
        let s = IngestSnapshot {
            apply_vs_arrival: 1.5,
            read_amp: READ_AMP_HIGH + 5,
            ..snap()
        };
        let adj = decide_fresh(&s, &actuators(), &bounds()).expect("acts");
        assert_eq!(adj.actuator, Actuator::InlineFlushBytes);
        assert!(adj.new_value > u64::try_from(actuators().inline_flush_max_bytes).expect("fits"));
        // With the memtable already at its ceiling, drain via compaction instead.
        let at_ceiling = ActuatorValues {
            inline_flush_max_bytes: bounds().inline_flush_max_bytes.1,
            ..actuators()
        };
        let adj2 = decide_fresh(&s, &at_ceiling, &bounds()).expect("acts");
        assert_eq!(adj2.actuator, Actuator::CompactionIntervalMs);
        assert!(adj2.new_value < actuators().compaction_background_interval_ms);
    }

    #[test]
    fn high_read_amp_acts_even_when_ingest_caught_up() {
        // Ingest keeps up (apply ≪ gap) but is creating too many small files, so
        // QUERIES are slow. The controller must still act.
        let s = IngestSnapshot {
            apply_vs_arrival: 0.2,
            read_amp: READ_AMP_HIGH + 10,
            ..snap()
        };
        let adj = decide_fresh(&s, &actuators(), &bounds())
            .expect("must act for query health even though ingest is fine");
        assert_eq!(adj.actuator, Actuator::InlineFlushBytes);
    }

    #[test]
    fn behind_with_high_read_amp_does_not_add_shards() {
        // Ecosystem balance: never raise write concurrency (more files) while
        // queries are already read-amp-bound. The read-amp rule wins.
        let s = IngestSnapshot {
            apply_vs_arrival: 1.5,
            read_amp: READ_AMP_HIGH + 5,
            ..snap()
        };
        let adj = decide_fresh(&s, &actuators(), &bounds()).expect("acts");
        assert_ne!(
            adj.actuator,
            Actuator::WriteConcurrency,
            "must not add shards (more files) while read-amp is high"
        );
    }

    #[test]
    fn memory_pressure_shrinks_memtable_and_overrides_growth() {
        // Even falling behind (which would normally GROW the memtable), high
        // memory pressure forces a SHRINK — memory is the hard constraint.
        let s = IngestSnapshot {
            apply_vs_arrival: 1.5,
            mem_pressure: Some(0.95),
            ..snap()
        };
        let bigger = ActuatorValues {
            inline_flush_max_bytes: 32 * 1024 * 1024,
            ..actuators()
        };
        let adj = decide_fresh(&s, &bigger, &bounds()).expect("acts");
        assert_eq!(adj.actuator, Actuator::InlineFlushBytes);
        assert!(
            adj.new_value < u64::try_from(bigger.inline_flush_max_bytes).expect("fits"),
            "memtable must shrink under memory pressure"
        );
    }

    #[test]
    fn memory_pressure_shrinks_mem_tier_after_memtable() {
        // Memtable already at its floor (can't shrink further) ⇒ the next memory
        // lever is the in-memory CDC tier cap.
        let s = IngestSnapshot {
            apply_vs_arrival: 1.5,
            mem_pressure: Some(0.95),
            ..snap()
        };
        let at_floor = ActuatorValues {
            inline_flush_max_bytes: bounds().inline_flush_max_bytes.0,
            mem_tier_max_bytes: 512 * 1024 * 1024,
            ..actuators()
        };
        let adj = decide_fresh(&s, &at_floor, &bounds()).expect("acts");
        assert_eq!(adj.actuator, Actuator::MemTierMaxBytes);
        assert!(
            adj.new_value < u64::try_from(at_floor.mem_tier_max_bytes).expect("fits"),
            "mem-tier cap must shrink under memory pressure"
        );
    }

    #[test]
    fn memory_pressure_blocks_growth_but_allows_compaction_for_read_amp() {
        // Memory is between OK and HIGH (can't grow, not yet shrinking) and
        // read-amp is high → relieve queries via compaction (CPU, not memory).
        let s = IngestSnapshot {
            apply_vs_arrival: 0.2,
            read_amp: READ_AMP_HIGH + 5,
            mem_pressure: Some(0.80), // between OK (0.75) and HIGH (0.85)
            ..snap()
        };
        let adj = decide_fresh(&s, &actuators(), &bounds()).expect("acts");
        assert_eq!(
            adj.actuator,
            Actuator::CompactionIntervalMs,
            "no memory to grow the memtable → drain small files via compaction"
        );
    }

    #[test]
    fn pinned_actuator_via_collapsed_bounds_is_skipped() {
        // An operator-pinned actuator (in `adaptive` mode) has its bounds collapsed
        // to a single point. A "falling behind" signal that would normally GROW the
        // memtable must fall through to another lever — the override is respected.
        let s = IngestSnapshot {
            apply_vs_arrival: 1.5,
            ..snap()
        };
        let mut b = bounds();
        b.inline_flush_max_bytes = (
            actuators().inline_flush_max_bytes,
            actuators().inline_flush_max_bytes,
        );
        let adj = decide_fresh(&s, &actuators(), &b).expect("acts via another lever");
        assert_ne!(
            adj.actuator,
            Actuator::InlineFlushBytes,
            "a pinned memtable must never be moved by the controller"
        );
        // Pinned memtable can't grow → next behind lever is the mem-tier cap.
        assert_eq!(adj.actuator, Actuator::MemTierMaxBytes);
    }

    #[test]
    fn behind_enlarges_memtable_first() {
        // Falling behind, read-amp healthy ⇒ grow the memtable first: the safest,
        // most generally-helpful lever (fewer, larger files + amortized commits).
        let s = IngestSnapshot {
            apply_vs_arrival: 1.5,
            ..snap()
        };
        let adj = decide_fresh(&s, &actuators(), &bounds()).expect("acts");
        assert_eq!(adj.actuator, Actuator::InlineFlushBytes);
        assert!(adj.new_value > actuators().inline_flush_max_bytes as u64);
        assert!(adj.new_value <= bounds().inline_flush_max_bytes.1 as u64);
    }

    // ---- P1: in-memory CDC tier cap actuator -----------------------------

    #[test]
    fn behind_grows_mem_tier_after_memtable_maxed() {
        // Memtable at ceiling but the mem-tier cap has room ⇒ enlarge the tier (a
        // larger cap means fewer writer-blocking spills) BEFORE adding write shards.
        let s = IngestSnapshot {
            apply_vs_arrival: 1.5,
            ..snap()
        };
        let memtable_maxed = ActuatorValues {
            inline_flush_max_bytes: bounds().inline_flush_max_bytes.1,
            ..actuators()
        };
        let adj = decide_fresh(&s, &memtable_maxed, &bounds()).expect("acts");
        assert_eq!(adj.actuator, Actuator::MemTierMaxBytes);
        assert!(adj.new_value > actuators().mem_tier_max_bytes as u64);
        assert!(adj.new_value <= bounds().mem_tier_max_bytes.1 as u64);
    }

    #[test]
    fn behind_with_maxed_buffers_raises_write_concurrency() {
        // Both memory buffers at their ceilings and read-amp healthy ⇒ the next
        // lever is encode parallelism (more write shards).
        let s = IngestSnapshot {
            apply_vs_arrival: 1.5,
            ..snap()
        };
        let maxed = ActuatorValues {
            inline_flush_max_bytes: bounds().inline_flush_max_bytes.1,
            mem_tier_max_bytes: bounds().mem_tier_max_bytes.1,
            ..actuators()
        };
        let adj = decide_fresh(&s, &maxed, &bounds()).expect("acts");
        assert_eq!(adj.actuator, Actuator::WriteConcurrency);
        assert!(adj.new_value > actuators().write_concurrency as u64);
        assert!(adj.new_value <= bounds().write_concurrency.1 as u64);
    }

    // ---- P2: delete-heavy + bursty signals -------------------------------

    #[test]
    fn delete_heavy_stream_withholds_write_concurrency() {
        // Falling behind, buffers maxed, read-amp healthy — but the stream is
        // delete-heavy, so adding shards (more files, worse delete routing) is
        // withheld; the controller falls through to compaction instead.
        let s = IngestSnapshot {
            apply_vs_arrival: 1.5,
            delete_fraction: 0.5,
            ..snap()
        };
        let maxed = ActuatorValues {
            inline_flush_max_bytes: bounds().inline_flush_max_bytes.1,
            mem_tier_max_bytes: bounds().mem_tier_max_bytes.1,
            ..actuators()
        };
        let adj = decide_fresh(&s, &maxed, &bounds()).expect("acts");
        assert_ne!(
            adj.actuator,
            Actuator::WriteConcurrency,
            "delete-heavy stream must not add write shards"
        );
        assert_eq!(adj.actuator, Actuator::CompactionIntervalMs);
    }

    #[test]
    fn delete_light_stream_allows_write_concurrency() {
        // Contrast with the delete-heavy case: the same maxed-buffer behind state
        // on an insert-mostly stream DOES raise write concurrency.
        let s = IngestSnapshot {
            apply_vs_arrival: 1.5,
            delete_fraction: 0.0,
            ..snap()
        };
        let maxed = ActuatorValues {
            inline_flush_max_bytes: bounds().inline_flush_max_bytes.1,
            mem_tier_max_bytes: bounds().mem_tier_max_bytes.1,
            ..actuators()
        };
        let adj = decide_fresh(&s, &maxed, &bounds()).expect("acts");
        assert_eq!(adj.actuator, Actuator::WriteConcurrency);
    }

    #[test]
    fn bursty_arrivals_pre_grow_mem_tier() {
        // Not behind right now, but arrivals are spiky ⇒ proactively grow the
        // durability tier so the next spike lands in RAM, not a spill stall.
        let s = IngestSnapshot {
            apply_vs_arrival: 0.2,
            arrival_cv: 1.5,
            ..snap()
        };
        let adj = decide_fresh(&s, &actuators(), &bounds()).expect("acts");
        assert_eq!(adj.actuator, Actuator::MemTierMaxBytes);
        assert!(adj.new_value > actuators().mem_tier_max_bytes as u64);
    }

    #[test]
    fn bursty_on_stale_samples_does_not_pre_grow() {
        // Spiky history but no new ingest since the last move ⇒ the burst signal
        // is gated; the controller doesn't keep pre-growing an idle table.
        let s = IngestSnapshot {
            apply_vs_arrival: 0.2,
            arrival_cv: 1.5,
            samples: 40,
            ..snap()
        };
        let adj = decide(&s, &actuators(), &bounds(), ms(60_000), ms(30_000), 40);
        assert!(
            adj.is_none_or(|a| a.actuator != Actuator::MemTierMaxBytes),
            "stale burst signal must not pre-grow the mem-tier"
        );
    }

    // ---- P0: relax (back off) when healthy -------------------------------

    #[test]
    fn healthy_sheds_write_concurrency_first() {
        // Idle on every axis with shards still raised ⇒ shed a shard first
        // (reclaim small-file fan-out) before relaxing compaction.
        let s = snap(); // apply_vs_arrival 0.2, read_amp 1, mem ok
        let adj = decide_fresh(&s, &actuators(), &bounds()).expect("acts");
        assert_eq!(adj.actuator, Actuator::WriteConcurrency);
        assert!(
            adj.new_value < actuators().write_concurrency as u64,
            "healthy → shed a write shard"
        );
    }

    #[test]
    fn healthy_relaxes_compaction_trigger_after_shards_shed() {
        // Shards already at the floor ⇒ next relax the compaction trigger UP
        // (compact less eagerly).
        let s = snap();
        let shed = ActuatorValues {
            write_concurrency: 1,
            ..actuators()
        };
        let adj = decide_fresh(&s, &shed, &bounds()).expect("acts");
        assert_eq!(adj.actuator, Actuator::CompactionTriggerFiles);
        assert!(adj.new_value > actuators().compaction_trigger_files as u64);
    }

    #[test]
    fn healthy_backs_off_compaction_interval_last() {
        // Shards at floor and trigger at ceiling ⇒ finally lengthen the background
        // compaction interval to free CPU for queries.
        let s = snap();
        let relaxed = ActuatorValues {
            write_concurrency: 1,
            compaction_trigger_files: bounds().compaction_trigger_files.1,
            ..actuators()
        };
        let adj = decide_fresh(&s, &relaxed, &bounds()).expect("acts");
        assert_eq!(adj.actuator, Actuator::CompactionIntervalMs);
        assert!(
            adj.new_value > actuators().compaction_background_interval_ms,
            "interval should lengthen when idle"
        );
    }

    #[test]
    fn steady_state_holds() {
        // Keeping up but not idle (ratio ~0.8, between HEALTHY and BEHIND), mid
        // read-amp ⇒ no rule fires.
        let s = IngestSnapshot {
            apply_vs_arrival: 0.8,
            read_amp: 5,
            ..snap()
        };
        assert!(decide_fresh(&s, &actuators(), &bounds()).is_none());
    }

    // ---- safety: bounds are never exceeded; no-op at extremes -------------

    #[test]
    fn adjustments_never_exceed_bounds() {
        let s = IngestSnapshot {
            apply_vs_arrival: 1.5,       // behind
            read_amp: READ_AMP_HIGH + 5, // and read-amp high
            ..snap()
        };
        let b = bounds();
        // From a range of starting positions, every returned move stays within
        // the targeted actuator's `[floor, ceiling]` (the clamp is by construction).
        for mult in [1_i64, 2, 4, 8, 16, 64] {
            let k = ActuatorValues {
                inline_flush_max_bytes: (2 * 1024 * 1024 * mult).min(b.inline_flush_max_bytes.1),
                compaction_background_interval_ms: (3_000 * mult as u64)
                    .min(b.compaction_background_interval_ms.1),
                mem_tier_max_bytes: (64 * 1024 * 1024 * mult).min(b.mem_tier_max_bytes.1),
                ..actuators()
            };
            let Some(adj) = decide_fresh(&s, &k, &b) else {
                continue;
            };
            let (lo, hi) = match adj.actuator {
                Actuator::InlineFlushBytes => (
                    b.inline_flush_max_bytes.0 as u64,
                    b.inline_flush_max_bytes.1 as u64,
                ),
                Actuator::MemTierMaxBytes => {
                    (b.mem_tier_max_bytes.0 as u64, b.mem_tier_max_bytes.1 as u64)
                }
                Actuator::CompactionIntervalMs => b.compaction_background_interval_ms,
                Actuator::CompactionTriggerFiles => (
                    b.compaction_trigger_files.0 as u64,
                    b.compaction_trigger_files.1 as u64,
                ),
                Actuator::WriteConcurrency => {
                    (b.write_concurrency.0 as u64, b.write_concurrency.1 as u64)
                }
            };
            assert!(
                (lo..=hi).contains(&adj.new_value),
                "{:?} value {} out of bounds [{lo}, {hi}]",
                adj.actuator,
                adj.new_value
            );
        }
    }

    #[test]
    fn convergence_does_not_oscillate() {
        // Repeatedly applying decisions for a fixed "falling behind" signal must
        // monotonically increase the memtable toward the ceiling and then STOP
        // growing it (the controller moves on to another lever — no flip-flop).
        let live = LiveActuators::new(actuators());
        let s = IngestSnapshot {
            apply_vs_arrival: 1.5,
            samples: 1000, // always fresh relative to the last-move mark below
            ..snap()
        };
        let mut last = live.values().inline_flush_max_bytes;
        let mut moves = 0;
        for _ in 0..50 {
            match decide(&s, &live.values(), &bounds(), ms(60_000), ms(30_000), 0) {
                Some(adj) if adj.actuator == Actuator::InlineFlushBytes => {
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

    // ---- LiveActuators apply coherence -----------------------------------

    #[test]
    fn apply_inline_bytes_keeps_rows_and_segments_coherent() {
        let live = LiveActuators::new(actuators());
        live.apply(&Adjustment {
            actuator: Actuator::InlineFlushBytes,
            new_value: 64 * 1024 * 1024,
            reason: "t",
        });
        let v = live.values();
        assert_eq!(v.inline_flush_max_bytes, 64 * 1024 * 1024);
        assert_eq!(v.inline_flush_max_rows, 64 * 1024 * 1024 / 1024);
        assert_eq!(
            v.inline_flush_max_segments,
            (64 * 1024 * 1024 / (128 * 1024)).clamp(16, 256)
        );
    }

    #[test]
    fn apply_mem_tier_bytes_stores_value() {
        let live = LiveActuators::new(actuators());
        live.apply(&Adjustment {
            actuator: Actuator::MemTierMaxBytes,
            new_value: 512 * 1024 * 1024,
            reason: "t",
        });
        assert_eq!(live.values().mem_tier_max_bytes, 512 * 1024 * 1024);
        assert_eq!(live.mem_tier_max_bytes(), 512 * 1024 * 1024);
    }

    #[test]
    fn apply_preserves_schema_aware_row_width_ratio() {
        // A narrow-row table (static tier inferred ~256 B/row): growing the byte
        // budget must keep flushing by that width, not snap the row cap back to
        // the ~1 KiB/row fallback (which would flush ~4× too early → small files).
        let narrow = ActuatorValues {
            inline_flush_max_bytes: 8 * 1024 * 1024,
            inline_flush_max_rows: 8 * 1024 * 1024 / 256, // 256 B/row
            inline_flush_max_segments: 64,
            ..actuators()
        };
        let live = LiveActuators::new(narrow);
        live.apply(&Adjustment {
            actuator: Actuator::InlineFlushBytes,
            new_value: 16 * 1024 * 1024,
            reason: "t",
        });
        let v = live.values();
        assert_eq!(v.inline_flush_max_bytes, 16 * 1024 * 1024);
        // Rows track the inferred 256 B/row ratio (65536), NOT the 1 KiB fallback.
        assert_eq!(v.inline_flush_max_rows, 16 * 1024 * 1024 / 256);
        assert_ne!(v.inline_flush_max_rows, 16 * 1024 * 1024 / 1024);
    }

    #[test]
    fn observe_mean_row_bytes_relearns_width() {
        // Static seed is ~1 KiB/row (8 MiB / 8192); live ingest reveals 256 B/row.
        // After relearning, a byte-budget grow derives the row cap from the OBSERVED
        // width (16 MiB / 256), not the stale static ratio (16 MiB / 1024).
        let live = LiveActuators::new(actuators());
        live.observe_mean_row_bytes(256);
        live.apply(&Adjustment {
            actuator: Actuator::InlineFlushBytes,
            new_value: 16 * 1024 * 1024,
            reason: "t",
        });
        let v = live.values();
        assert_eq!(v.inline_flush_max_rows, 16 * 1024 * 1024 / 256);
        assert_ne!(v.inline_flush_max_rows, 16 * 1024 * 1024 / 1024);
    }

    #[test]
    fn adaptive_inline_flush_ceiling_uses_memory_budget() {
        let gib = 1024_u64 * 1024 * 1024;
        let initial = 128 * MIB;

        assert_eq!(
            adaptive_inline_flush_bounds_for_budget(initial, None),
            (INLINE_FLUSH_MIN_BYTES, INLINE_FLUSH_FALLBACK_MAX_BYTES)
        );
        assert_eq!(
            adaptive_inline_flush_bounds_for_budget(initial, Some(64 * gib)),
            (INLINE_FLUSH_MIN_BYTES, INLINE_FLUSH_ADAPTIVE_MAX_BYTES)
        );
        assert_eq!(
            adaptive_inline_flush_bounds_for_budget(8 * MIB, Some(64 * gib)),
            (INLINE_FLUSH_MIN_BYTES, 64 * MIB)
        );
    }

    #[test]
    fn adaptive_mem_tier_bounds_uses_memory_budget() {
        let gib = 1024_u64 * 1024 * 1024;
        // No budget: ceiling is the 4× fallback, clamped to the fallback max.
        assert_eq!(
            adaptive_mem_tier_bounds_for_budget(256 * MIB, None),
            (MEM_TIER_MIN_BYTES, MEM_TIER_FALLBACK_MAX_BYTES)
        );
        // With a 64 GiB budget the ceiling reaches the absolute adaptive max.
        assert_eq!(
            adaptive_mem_tier_bounds_for_budget(256 * MIB, Some(64 * gib)),
            (MEM_TIER_MIN_BYTES, MEM_TIER_ADAPTIVE_MAX_BYTES)
        );
        // A non-positive initial means "no per-table cap" ⇒ collapsed, untouched.
        assert_eq!(
            adaptive_mem_tier_bounds_for_budget(0, Some(64 * gib)),
            (0, 0)
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn parses_cgroup_paths_and_mounts() {
        assert_eq!(
            parse_proc_cgroup_v2_path("0::/kubepods.slice/pod123\n"),
            Some("/kubepods.slice/pod123".to_string())
        );
        assert_eq!(
            parse_proc_cgroup_v1_path("7:cpu,cpuacct:/x\n8:memory:/mem\n", "memory"),
            Some("/mem".to_string())
        );
        assert_eq!(
            parse_mountinfo_cgroup2("29 28 0:25 / /sys/fs/cgroup rw - cgroup2 cgroup2 rw\n"),
            Some("/sys/fs/cgroup".to_string())
        );
        assert_eq!(
            parse_mountinfo_cgroup_v1(
                "32 29 0:28 / /sys/fs/cgroup/memory rw - cgroup cgroup rw,memory\n",
                "memory"
            ),
            Some("/sys/fs/cgroup/memory".to_string())
        );
        assert_eq!(
            cgroup_file_path("/sys/fs/cgroup", "/", "memory.current"),
            "/sys/fs/cgroup/memory.current"
        );
    }
}
