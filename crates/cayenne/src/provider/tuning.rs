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
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

use parking_lot::{Mutex, RwLock};

use crate::metadata::StorageClass;

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

/// Fraction of ingested rows that MUTATE an existing key (deletes + upsert
/// updates) above which the table is judged "mutation-heavy". Adding write shards
/// (encode parallelism) to a mutation-heavy stream multiplies the per-burst
/// small-file fan-out and worsens key routing off the in-memory tier, so the
/// controller withholds the write-concurrency lever here and leans on
/// buffer/compaction levers instead. A delete-only stream has zero update
/// fraction, so this is the legacy delete-heavy gate exactly.
const MUTATION_HEAVY_FRACTION: f64 = 0.2;

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

/// CPU busy-fraction (of available cores, cgroup-aware) above which the controller
/// withholds CPU-stealing moves — adding write-encode shards and shrinking the
/// compaction interval both compete with query threads. Mirrors [`MEM_PRESSURE_HIGH`].
const CPU_PRESSURE_HIGH: f64 = 0.85;

/// CPU busy-fraction below which those CPU-stealing moves re-enable (hysteresis gap
/// to [`CPU_PRESSURE_HIGH`], like the memory pair).
const CPU_PRESSURE_OK: f64 = 0.75;

/// Fraction of the offered-load interval (`arrival_gap_ms`) that per-batch
/// object-store/disk write latency must exceed for the table to count as
/// "I/O-bound". Relative (not a fixed-ms cliff) so it scales with the table's rate.
const IO_BOUND_FRACTION: f64 = 0.5;

/// Same, for per-batch metastore publish latency → "publish-bound" (the
/// single-writer commit is eating the offered-load window).
const PUBLISH_BOUND_FRACTION: f64 = 0.5;

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
// Goal-driven tuning
// ---------------------------------------------------------------------------

/// Default convergence window: the time budget the goal-seeking controller aims
/// to reach its targets within. Operators override it per dataset
/// (`cayenne_goal_convergence_window`).
pub(crate) const DEFAULT_GOAL_CONVERGENCE_WINDOW: Duration = Duration::from_mins(1);

/// Number of correction steps the goal controller plans across the convergence
/// window. Sets BOTH the per-tick step cap (`range / N` — "no big jumps") AND the
/// goal-mode dwell (`window / N` — "converge within the window"): with the default
/// 60s window this is ~8 small steps at ~7.5s spacing, enough to span an
/// actuator's full range yet never jump far in one tick.
const STEPS_PER_WINDOW: u32 = 8;

/// Floor on the goal-mode dwell. lag/freshness/p99 respond over flush + publish
/// cycles (and, for queries, a window of executions); stepping faster than the
/// metric responds causes integral windup / overshoot. Floors `window / N` so a
/// tiny window can't out-run the metric. The legacy (non-goal) path keeps the
/// slower [`MIN_DWELL`].
const GOAL_DWELL_FLOOR: Duration = Duration::from_secs(5);

/// A goal is *violated* once the measured value is this fraction past its target
/// (a deadband, so the controller doesn't chase noise at the setpoint). The
/// goal-mode analogue of [`BEHIND_RATIO`].
const GOAL_VIOLATE_FRACTION: f64 = 0.20;

/// A goal is *comfortably met* once the measured value is this fraction better
/// than target — the gate for relaxing resources a goal no longer needs. The
/// goal-mode analogue of [`HEALTHY_RATIO`]; the gap to [`GOAL_VIOLATE_FRACTION`]
/// is the hysteresis band that stops relax/tighten flapping.
const GOAL_RELAX_FRACTION: f64 = 0.50;

/// Even a barely-violating goal takes at least this fraction of the per-tick step
/// cap, so the controller keeps crawling toward target instead of stalling just
/// outside the deadband.
const GOAL_MIN_STEP_FRACTION: f64 = 0.25;

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

/// Process-wide CPU busy-fraction of available cores (cgroup-aware), stored ×1000.
/// `u64::MAX` = unknown (non-Linux, unreadable, or the first/too-close sample with
/// no usable delta yet). Process-global: every per-table loop reads this one value,
/// because CPU is shared across tables (like the memory budget).
static CPU_PRESSURE_MILLI: AtomicU64 = AtomicU64::new(u64::MAX);

#[cfg(target_os = "linux")]
static CGROUP_V2_CPU_STAT_PATH: OnceLock<Option<String>> = OnceLock::new();

#[cfg(target_os = "linux")]
static CGROUP_V1_CPUACCT_USAGE_PATH: OnceLock<Option<String>> = OnceLock::new();

/// Previous CPU sample `(cumulative usage µs, wall instant)`. The per-table
/// background ticks all race to sample one process-global source; the short
/// critical section computes the busy-fraction delta and swaps in the new sample.
#[cfg(target_os = "linux")]
static CPU_PREV_SAMPLE: LazyLock<Mutex<Option<(u64, Instant)>>> =
    LazyLock::new(|| Mutex::new(None));

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
// CPU saturation sampler (process-global, cgroup-aware)
// ---------------------------------------------------------------------------

/// Sample process-wide CPU busy-fraction into the global [`CPU_PRESSURE_MILLI`]:
/// cgroup v2 `cpu.stat` `usage_usec` (v1 `cpuacct.usage` fallback), as a delta
/// busy-fraction `Δusage / (Δwall × cores)`. Needs the previous sample; a too-close
/// interval (< 0.5 s) is skipped so a double-call can't divide by ~0. Called on the
/// background tick. Linux-only; elsewhere a no-op (pressure stays unknown → the CPU
/// rule is inert). Process-global because CPU is shared across all per-table loops.
#[cfg(target_os = "linux")]
pub(crate) fn sample_cpu_pressure() {
    let Some(now_usage) = cgroup_cpu_usage_usec() else {
        return;
    };
    let now = Instant::now();
    let cores = cpu_cores_f64();
    let mut prev = CPU_PREV_SAMPLE.lock();
    match *prev {
        Some((prev_usage, prev_at)) => {
            let wall_secs = now.saturating_duration_since(prev_at).as_secs_f64();
            // Skip samples < 0.5 s apart: a tiny denominator makes the ratio noisy
            // (two ticks can fire close together on a multi-table host). Keep `prev`
            // and wait for a wider window.
            if wall_secs >= 0.5 && cores > 0.0 {
                let busy_secs = u64_to_f64(now_usage.saturating_sub(prev_usage)) / 1_000_000.0;
                store_cpu_pressure(busy_secs / (wall_secs * cores));
                *prev = Some((now_usage, now));
            }
        }
        None => *prev = Some((now_usage, now)), // prime; report nothing yet
    }
}

#[cfg(not(target_os = "linux"))]
pub(crate) fn sample_cpu_pressure() {}

/// Current process CPU busy-fraction (of available cores), or `None` when
/// unavailable. Read on the background tick into the snapshot.
#[must_use]
pub(crate) fn cpu_pressure() -> Option<f64> {
    milli_to_pressure(CPU_PRESSURE_MILLI.load(Ordering::Relaxed))
}

#[cfg(target_os = "linux")]
fn store_cpu_pressure(frac: f64) {
    if let Some(milli) = pressure_to_milli(frac) {
        CPU_PRESSURE_MILLI.store(milli, Ordering::Relaxed);
    }
}

#[cfg(target_os = "linux")]
fn cpu_cores_f64() -> f64 {
    let cores = std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get);
    u64_to_f64(u64::try_from(cores).unwrap_or(1))
}

/// Cumulative CPU usage in microseconds — cgroup v2 `cpu.stat` `usage_usec`, then
/// v1 `cpuacct.usage` (nanoseconds → µs). `None` when unreadable.
#[cfg(target_os = "linux")]
fn cgroup_cpu_usage_usec() -> Option<u64> {
    if let Some(path) = CGROUP_V2_CPU_STAT_PATH
        .get_or_init(resolve_cgroup_v2_cpu_stat_path)
        .as_deref()
        && let Some(usec) = read_cpu_stat_usage_usec(path)
    {
        return Some(usec);
    }
    CGROUP_V1_CPUACCT_USAGE_PATH
        .get_or_init(resolve_cgroup_v1_cpuacct_usage_path)
        .as_deref()
        .and_then(read_u64_file)
        .map(|nanos| nanos / 1_000)
}

#[cfg(target_os = "linux")]
fn read_cpu_stat_usage_usec(path: &str) -> Option<u64> {
    let contents = std::fs::read_to_string(path).ok()?;
    contents.lines().find_map(|line| {
        line.strip_prefix("usage_usec ")
            .and_then(|v| v.trim().parse::<u64>().ok())
    })
}

#[cfg(target_os = "linux")]
fn resolve_cgroup_v2_cpu_stat_path() -> Option<String> {
    let cgroup_path = process_cgroup_v2_path()?;
    let mountpoint = cgroup2_mountpoint().unwrap_or_else(|| "/sys/fs/cgroup".to_string());
    Some(cgroup_file_path(&mountpoint, &cgroup_path, "cpu.stat"))
}

#[cfg(target_os = "linux")]
fn resolve_cgroup_v1_cpuacct_usage_path() -> Option<String> {
    let cgroup_path = process_cgroup_v1_path("cpuacct")?;
    let mountpoint =
        cgroup_v1_mountpoint("cpuacct").unwrap_or_else(|| "/sys/fs/cgroup/cpuacct".to_string());
    Some(cgroup_file_path(&mountpoint, &cgroup_path, "cpuacct.usage"))
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
    /// Rows in this batch that were true tombstone deletes (the CDC `delete`
    /// op-group). Feeds the delete fraction of the mutation-heavy signal.
    pub delete_rows: u64,
    /// Rows in this batch that REPLACED an existing live PK (upsert updates — the
    /// `superseded` count). Heavy updates churn the keyset and fan out files like
    /// deletes, so they join `delete_rows` in the mutation-heavy gate. Inserts are
    /// the remainder (`rows − delete_rows − update_rows`), not stored.
    pub update_rows: u64,
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
    /// EWMA of the per-batch update fraction (`update_rows / rows`), in `[0, 1]`.
    /// Joins `delete_fraction` in the mutation-heavy gate.
    update_fraction: f64,
    samples: u64,
    /// EWMA per-batch object-store/disk write latency (the `vortex_write` phase),
    /// ms; paired with `io_samples` for cold-start priming. `0` / no samples ⇒
    /// the table has not spilled to Vortex (pure-inline) → I/O signal unavailable.
    io_latency_ms: f64,
    io_samples: u64,
    /// EWMA per-batch metastore publish latency (the `publish` phase — the
    /// single-writer commit), ms; paired with `publish_samples`.
    publish_latency_ms: f64,
    publish_samples: u64,
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
    /// Newest applied upstream commit timestamp (ms since the Unix epoch), folded
    /// in via `fetch_max` so it tracks the freshest committed row; `i64::MIN` until
    /// a source provides one. Feeds the replication-lag goal (`now − this`). A wall
    /// clock, NOT a monotonic `Instant`.
    newest_source_commit_ts_ms: AtomicI64,
    /// Wall-clock time (ms since the Unix epoch) the newest data was last applied
    /// (≈ became queryable); `i64::MIN` until the first apply. Feeds the freshness
    /// goal (`now − this`).
    last_visible_ts_ms: AtomicI64,
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
            newest_source_commit_ts_ms: AtomicI64::new(i64::MIN),
            last_visible_ts_ms: AtomicI64::new(i64::MIN),
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
        let inst_update_fraction = if s.rows > 0 {
            (u64_to_f64(s.update_rows) / u64_to_f64(s.rows)).clamp(0.0, 1.0)
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
        ewma(&mut inner.update_fraction, inst_update_fraction, prior);
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
    pub fn set_mem_pressure(&self, fraction: f64) {
        if let Some(milli) = pressure_to_milli(fraction) {
            self.mem_pressure_milli.store(milli, Ordering::Relaxed);
        }
    }

    /// Fold in an upstream commit timestamp (ms since the Unix epoch) for a freshly
    /// applied batch. `fetch_max`: the newest wins, an older replay never regresses
    /// the watermark. Called on the CDC write path when the source provides one.
    pub fn observe_source_commit_ts_ms(&self, ts_ms: i64) {
        self.newest_source_commit_ts_ms
            .fetch_max(ts_ms, Ordering::Relaxed);
    }

    /// Record that data was just applied (≈ made queryable) at wall-clock `now_ms`
    /// (ms since the Unix epoch). Feeds the freshness goal.
    pub fn set_last_visible_ts_ms(&self, now_ms: i64) {
        self.last_visible_ts_ms.store(now_ms, Ordering::Relaxed);
    }

    /// Fold one CDC batch's object-store/disk write latency (the `vortex_write`
    /// phase) into the rolling EWMA. Recorded only on batches that spill to Vortex,
    /// so a pure-inline table leaves `io_latency_ms` unavailable.
    pub fn record_io_latency(&self, d: Duration) {
        let mut inner = self.inner.lock();
        let prior = inner.io_samples;
        ewma(&mut inner.io_latency_ms, duration_ms(d), prior);
        inner.io_samples = prior.saturating_add(1);
    }

    /// Fold one CDC batch's metastore publish latency (the `publish` phase — the
    /// single-writer commit) into the rolling EWMA.
    pub fn record_publish_latency(&self, d: Duration) {
        let mut inner = self.inner.lock();
        let prior = inner.publish_samples;
        ewma(&mut inner.publish_latency_ms, duration_ms(d), prior);
        inner.publish_samples = prior.saturating_add(1);
    }

    /// Replication lag in seconds relative to `now_ms` (age of the newest applied
    /// upstream commit), or `None` if no source timestamp has been observed.
    /// Negative values (source clock ahead of local) clamp to 0; the absolute value
    /// is only as good as the source↔host clock sync, so the controller keys off
    /// the threshold/trend, not the sub-second value. Pure in `now_ms` (testable).
    #[must_use]
    pub fn replication_lag_secs(&self, now_ms: i64) -> Option<f64> {
        match self.newest_source_commit_ts_ms.load(Ordering::Relaxed) {
            i64::MIN => None,
            ts => Some(u64_to_f64(now_ms.saturating_sub(ts).max(0).unsigned_abs()) / 1000.0),
        }
    }

    /// Freshness in seconds relative to `now_ms` (age of the newest applied data),
    /// or `None` before the first apply. Stamped at apply time; on the backgrounded
    /// staged-CDC publish, true visibility trails apply by the finalize latency, so
    /// this is a lower bound on staleness.
    #[must_use]
    pub fn freshness_secs(&self, now_ms: i64) -> Option<f64> {
        match self.last_visible_ts_ms.load(Ordering::Relaxed) {
            i64::MIN => None,
            ts => Some(u64_to_f64(now_ms.saturating_sub(ts).max(0).unsigned_abs()) / 1000.0),
        }
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
        let mem_pressure = milli_to_pressure(self.mem_pressure_milli.load(Ordering::Relaxed));
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
            // The now-relative CDC signals, the query-side metrics, and the
            // per-table storage classes are filled in by
            // `CayenneContext::ingest_snapshot` (which holds the wall clock, the
            // query-observations handle, and the env profile); `snapshot` stays
            // clock-free. The CPU/op-mix/latency signals below ARE available here
            // (a process-global atomic and per-table EWMAs).
            replication_lag_secs: None,
            freshness_secs: None,
            query_latency_p99_ms: None,
            qph: None,
            cpu_pressure: cpu_pressure(),
            io_latency_ms: (inner.io_samples > 0).then_some(inner.io_latency_ms),
            publish_latency_ms: (inner.publish_samples > 0).then_some(inner.publish_latency_ms),
            update_fraction: inner.update_fraction,
            data_storage: StorageClass::default(),
            metastore_storage: StorageClass::default(),
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
    /// True end-to-end replication lag in seconds (`now − newest applied upstream
    /// commit ts`), or `None` when no source timestamp is available. Lower is
    /// better; drives the replication-lag goal.
    pub replication_lag_secs: Option<f64>,
    /// Freshness in seconds (`now − newest applied data wall-clock`), or `None`
    /// before the first apply. Lower is better; drives the freshness goal.
    pub freshness_secs: Option<f64>,
    /// p99 query latency in ms observed on this table (pushed down from the
    /// runtime), or `None` when no queries have run. Lower is better; drives the
    /// query-latency goal.
    pub query_latency_p99_ms: Option<f64>,
    /// Query throughput in queries/hour observed on this table, or `None` before a
    /// measurable interval. Higher is better; drives the QPH goal.
    pub qph: Option<f64>,
    /// Process-wide CPU busy-fraction of available cores (cgroup-aware), or `None`
    /// when unavailable (non-Linux / unreadable). High ⇒ withhold CPU-stealing
    /// moves (write-concurrency growth, compaction-interval shrink). Process-global.
    pub cpu_pressure: Option<f64>,
    /// EWMA per-batch object-store/disk write latency in ms, or `None` until the
    /// table spills to Vortex. High vs the arrival interval ⇒ bias to fewer/larger files.
    pub io_latency_ms: Option<f64>,
    /// EWMA per-batch metastore publish latency in ms, or `None` before the first
    /// publish. High vs the arrival interval ⇒ bias to larger inline-flush (amortize commits).
    pub publish_latency_ms: Option<f64>,
    /// EWMA fraction of ingested rows that REPLACE an existing key (upsert updates),
    /// in `[0, 1]`. Joins `delete_fraction` in the mutation-heavy gate.
    pub update_fraction: f64,
    /// Storage medium of the table's data files, surfaced for observability/telemetry.
    /// The slow-tier write bias is realized in the static warm-start (the accelerator
    /// sizes the initial inline-flush by storage class) and dynamically via the
    /// measured `io_latency_ms` signal — `decide` does not read this field directly.
    pub data_storage: StorageClass,
    /// Storage medium of the metastore, surfaced for observability/telemetry. The
    /// slow-tier commit-amortization bias is realized via the static warm-start and
    /// the measured `publish_latency_ms` signal — not read by `decide` directly.
    pub metastore_storage: StorageClass,
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

// ---------------------------------------------------------------------------
// Query-side observations (pushed down from the runtime)
// ---------------------------------------------------------------------------

/// Latency histogram bucket upper-bounds in ms (log-spaced). p99 is read as the
/// bucket whose running cumulative count first crosses 99%; an implicit
/// `(60s, +inf)` overflow bucket sits beyond the last bound.
const LAT_BUCKET_BOUNDS_MS: [f64; 15] = [
    1.0, 2.0, 5.0, 10.0, 20.0, 50.0, 100.0, 200.0, 500.0, 1_000.0, 2_000.0, 5_000.0, 10_000.0,
    20_000.0, 60_000.0,
];

/// Per-table query-side observations, fed by the runtime on query completion and
/// read by the per-table tuner on its background tick. Lock-free: a fixed bucket
/// histogram of `AtomicU64` for the p99 estimate (the right structure for a tail
/// quantile — an EWMA tracks the mean, not the tail — and cheaper/simpler than a
/// lock-guarded digest for a coarse, background-sampled consumer) plus a monotonic
/// query counter for QPH.
#[derive(Debug)]
pub struct QueryObservations {
    /// One slot beyond the bounds is the implicit `(60s, +inf)` overflow bucket.
    lat_buckets: [AtomicU64; LAT_BUCKET_BOUNDS_MS.len() + 1],
    total_queries: AtomicU64,
    anchor: Instant,
}

impl Default for QueryObservations {
    fn default() -> Self {
        Self::new()
    }
}

impl QueryObservations {
    /// Empty observations anchored at the current instant (the QPH baseline).
    #[must_use]
    pub fn new() -> Self {
        Self {
            lat_buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            total_queries: AtomicU64::new(0),
            anchor: Instant::now(),
        }
    }

    /// Record one finished query's wall latency. Lock-free (two `fetch_add`s);
    /// called by the runtime for every query that touched this table.
    pub fn record_query(&self, latency_ms: f64) {
        let idx = LAT_BUCKET_BOUNDS_MS
            .iter()
            .position(|&hi| latency_ms <= hi)
            .unwrap_or(LAT_BUCKET_BOUNDS_MS.len());
        self.lat_buckets[idx].fetch_add(1, Ordering::Relaxed);
        self.total_queries.fetch_add(1, Ordering::Relaxed);
    }

    /// p99 latency estimate (upper bound of the bucket where the running cumulative
    /// count crosses 99%), or `None` if no queries have been recorded.
    #[must_use]
    pub fn p99_latency_ms(&self) -> Option<f64> {
        // Two passes directly over the 16 atomics (no heap alloc): total, then the
        // bucket where the running count crosses 99%.
        let total: u64 = self
            .lat_buckets
            .iter()
            .map(|a| a.load(Ordering::Relaxed))
            .sum();
        if total == 0 {
            return None;
        }
        // ceil(total * 0.99), in integer math, ≥ 1.
        let threshold = total.saturating_mul(99).div_ceil(100).max(1);
        let mut cum = 0u64;
        for (i, bucket) in self.lat_buckets.iter().enumerate() {
            cum = cum.saturating_add(bucket.load(Ordering::Relaxed));
            if cum >= threshold {
                // The overflow bucket (i == bounds.len()) reports the top finite bound.
                let idx = i.min(LAT_BUCKET_BOUNDS_MS.len() - 1);
                return Some(LAT_BUCKET_BOUNDS_MS[idx]);
            }
        }
        None
    }

    /// Total queries recorded over the lifetime of these observations.
    #[must_use]
    pub fn total_queries(&self) -> u64 {
        self.total_queries.load(Ordering::Relaxed)
    }

    /// Queries-per-hour over the lifetime of these observations, or `None` before a
    /// measurable interval. Lifetime QPH is the simplest correct cut; it
    /// under-reacts to recent spikes (a windowed rate is a future refinement).
    #[must_use]
    pub fn qph(&self) -> Option<f64> {
        let hours = self.anchor.elapsed().as_secs_f64() / 3600.0;
        if hours <= f64::EPSILON {
            return None;
        }
        Some(u64_to_f64(self.total_queries.load(Ordering::Relaxed)) / hours)
    }
}

/// Process-global registry mapping a table key to its query observations, so the
/// runtime (which owns query execution and cannot be imported by this crate) can
/// push per-table latency/QPH *down* into the per-table tuner. Keyed by the bare
/// table name (see [`table_registry_key`]). A `RwLock<HashMap>` is ample:
/// registration/de-registration are rare (table create/drop); the per-query push
/// takes only a short read lock.
static QUERY_OBSERVATIONS: LazyLock<RwLock<HashMap<String, Arc<QueryObservations>>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

/// Normalize a dataset/table name to the bare table-name key used by the query
/// registry. Both the register side (the Cayenne context) and the push side (the
/// runtime, via `TableReference::table()`) must agree on this; using the bare name
/// relies on globally-unique accelerated dataset names (true in Spice).
#[must_use]
pub(crate) fn table_registry_key(name: &str) -> String {
    datafusion_common::TableReference::from(name)
        .table()
        .to_string()
}

/// Register (or fetch the existing) query-observations handle for a table.
/// Idempotent: a second call for the same key returns the same `Arc`, so a
/// recreated table reuses its handle.
#[must_use]
pub fn register_query_observations(name: &str) -> Arc<QueryObservations> {
    let key = table_registry_key(name);
    if let Some(existing) = QUERY_OBSERVATIONS.read().get(&key) {
        return Arc::clone(existing);
    }
    Arc::clone(
        QUERY_OBSERVATIONS
            .write()
            .entry(key)
            .or_insert_with(|| Arc::new(QueryObservations::new())),
    )
}

/// Push one finished query's wall latency to a table's observations, if it is a
/// registered (Cayenne-accelerated) table. Called by the runtime's query tracker
/// for every dataset a query touched; a no-op for unregistered tables.
pub fn record_query_latency(name: &str, latency_ms: f64) {
    let map = QUERY_OBSERVATIONS.read();
    // Fast path: the runtime pushes the already-bare table name
    // (`TableReference::table()`), so a borrowed lookup hits with no allocation or
    // parse — this runs per dataset per query.
    if let Some(obs) = map.get(name) {
        obs.record_query(latency_ms);
    } else if let Some(obs) = map.get(table_registry_key(name).as_str()) {
        // Fallback for a schema-qualified name (rare; never on the runtime hot
        // path) — normalize to the bare key registration used.
        obs.record_query(latency_ms);
    }
}

/// Drop a table's query observations (on table teardown) so the registry does not
/// leak handles or hand a recreated table a stale histogram.
pub fn deregister_query_observations(name: &str) {
    QUERY_OBSERVATIONS.write().remove(&table_registry_key(name));
}

// ---------------------------------------------------------------------------
// Goals: operator-facing SLOs the controller drives toward
// ---------------------------------------------------------------------------

/// Whether a goal metric is better lower (lag, freshness, latency) or higher (QPH).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GoalDir {
    LowerBetter,
    HigherBetter,
}

/// One operator-configured SLO: a target value plus which direction is "better".
/// Pure value type so [`decide`] stays testable.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct Goal {
    pub dir: GoalDir,
    pub target: f64,
}

impl Goal {
    fn lower(target: f64) -> Self {
        Self {
            dir: GoalDir::LowerBetter,
            target,
        }
    }
    fn higher(target: f64) -> Self {
        Self {
            dir: GoalDir::HigherBetter,
            target,
        }
    }

    /// Signed error in units of "fraction of target": `> 0` worse than target
    /// (violating), `<= 0` at/better. `None` when the metric is unavailable or the
    /// target is non-positive.
    fn normalized_error(self, measured: Option<f64>) -> Option<f64> {
        let m = measured?;
        if !self.target.is_finite() || self.target <= 0.0 || !m.is_finite() {
            return None;
        }
        Some(match self.dir {
            GoalDir::LowerBetter => (m - self.target) / self.target,
            GoalDir::HigherBetter => (self.target - m) / self.target,
        })
    }

    /// Violation magnitude past the deadband, clamped to `[0, 1]` — the
    /// proportional term that scales the step size. The clamp is error-level
    /// anti-windup (10× past target is not a 10× step). `0.0` = not violated.
    fn violation(self, measured: Option<f64>) -> f64 {
        match self.normalized_error(measured) {
            Some(e) if e > GOAL_VIOLATE_FRACTION => (e - GOAL_VIOLATE_FRACTION).clamp(0.0, 1.0),
            _ => 0.0,
        }
    }

    /// True when the metric has at least [`GOAL_RELAX_FRACTION`] headroom on the
    /// good side — safe to relax resources this goal would otherwise need.
    fn comfortably_met(self, measured: Option<f64>) -> bool {
        matches!(self.normalized_error(measured), Some(e) if e <= -GOAL_RELAX_FRACTION)
    }
}

/// Operator-configured tuning goals (each `None` when unset) plus the convergence
/// window. When every target is `None`, [`decide`] runs the legacy signal-driven
/// path unchanged.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct Goals {
    pub replication_lag: Option<Goal>,
    pub freshness: Option<Goal>,
    pub query_latency_p99: Option<Goal>,
    pub qph: Option<Goal>,
    pub convergence_window: Duration,
}

impl Default for Goals {
    fn default() -> Self {
        Self::none()
    }
}

impl Goals {
    /// No goals configured — [`decide`] behaves exactly as the legacy controller.
    #[must_use]
    pub(crate) fn none() -> Self {
        Self {
            replication_lag: None,
            freshness: None,
            query_latency_p99: None,
            qph: None,
            convergence_window: DEFAULT_GOAL_CONVERGENCE_WINDOW,
        }
    }

    /// Build from the per-dataset config knobs. Any positive target switches the
    /// controller into goal-seeking mode for that metric; non-positive/`None`
    /// leaves it on the legacy signal.
    #[must_use]
    pub(crate) fn from_targets(
        replication_lag_secs: Option<f64>,
        freshness_secs: Option<f64>,
        query_latency_p99_ms: Option<f64>,
        qph: Option<f64>,
        convergence_window: Duration,
    ) -> Self {
        Self {
            replication_lag: replication_lag_secs.filter(|v| *v > 0.0).map(Goal::lower),
            freshness: freshness_secs.filter(|v| *v > 0.0).map(Goal::lower),
            query_latency_p99: query_latency_p99_ms.filter(|v| *v > 0.0).map(Goal::lower),
            qph: qph.filter(|v| *v > 0.0).map(Goal::higher),
            convergence_window,
        }
    }

    #[must_use]
    pub(crate) fn any_active(self) -> bool {
        self.replication_lag.is_some()
            || self.freshness.is_some()
            || self.query_latency_p99.is_some()
            || self.qph.is_some()
    }

    /// The goal-mode dwell derived from the convergence window: `window / N`,
    /// floored so we never step faster than the metrics respond.
    fn dwell(self) -> Duration {
        (self.convergence_window / STEPS_PER_WINDOW).max(GOAL_DWELL_FLOOR)
    }

    /// Max violation among the lag/freshness goals — drives the ingest tier's step
    /// size; identifies the driving goal for attribution.
    fn ingest_violation(self, s: &IngestSnapshot) -> f64 {
        let lag = self
            .replication_lag
            .map_or(0.0, |g| g.violation(s.replication_lag_secs));
        let fresh = self.freshness.map_or(0.0, |g| g.violation(s.freshness_secs));
        lag.max(fresh)
    }

    /// Max violation among the query-side goals — drives the query tier's step size.
    fn query_violation(self, s: &IngestSnapshot) -> f64 {
        let lat = self
            .query_latency_p99
            .map_or(0.0, |g| g.violation(s.query_latency_p99_ms));
        let qph = self.qph.map_or(0.0, |g| g.violation(s.qph));
        lat.max(qph)
    }

    /// Are all *active* goals comfortably met? Gate for the healthy-relax tier.
    fn all_comfortably_met(self, s: &IngestSnapshot) -> bool {
        self.replication_lag
            .is_none_or(|g| g.comfortably_met(s.replication_lag_secs))
            && self
                .freshness
                .is_none_or(|g| g.comfortably_met(s.freshness_secs))
            && self
                .query_latency_p99
                .is_none_or(|g| g.comfortably_met(s.query_latency_p99_ms))
            && self.qph.is_none_or(|g| g.comfortably_met(s.qph))
    }
}

/// Decide the single best bounded actuator move for the current behavior, or
/// `None` to hold, given operator-configured [`Goals`]. When no goal is set this is
/// byte-for-byte the legacy signal-driven controller; when a goal is set, the
/// matching tier is triggered by the goal (not only the fixed thresholds) and uses
/// proportional, range-bounded steps on a window-derived dwell so it converges
/// without large jumps. Memory pressure stays the hard top priority either way, and
/// every move is still clamped to the static `TuningBounds` — the "never worse than
/// the static config" invariant is preserved.
///
/// Pure: no I/O, no clock — `since_last` (time since the previous applied move) is
/// passed in so the dwell-time hysteresis is testable, and `samples_at_last_move`
/// (the batch count when the last move was applied) so the fresh-sample gate is
/// testable. Rules are evaluated in objective-priority order; the first applicable
/// one wins (one move per tick).
#[must_use]
pub(crate) fn decide_with_goals(
    s: &IngestSnapshot,
    cur: &ActuatorValues,
    b: &TuningBounds,
    since_last: Duration,
    min_dwell: Duration,
    samples_at_last_move: u64,
    goals: &Goals,
) -> Option<Adjustment> {
    // Don't act on a cold table, and respect the dwell so moves don't stack
    // faster than their effect can be observed. In goal-seeking mode the dwell is
    // derived from the convergence window (shorter, so the controller can take its
    // ~N small steps within the window); otherwise it is the legacy `min_dwell`.
    let dwell = if goals.any_active() {
        goals.dwell()
    } else {
        min_dwell
    };
    if s.samples < WARMUP_BATCHES || since_last < dwell {
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
    let mutation_heavy = (s.delete_fraction + s.update_fraction) > MUTATION_HEAVY_FRACTION;
    let bursty = ingest_fresh && s.arrival_cv > BURSTY_ARRIVAL_CV;
    // Environment gates. CPU-bound withholds CPU-stealing moves (add write shards,
    // compact more); I/O-/publish-bound (per-batch latency eating the offered-load
    // window, gated on fresh ingest) biases toward fewer/larger files + amortized
    // commits. Each collapses to legacy behavior when its signal is unavailable
    // (`cpu_ok` true, `io_bound`/`publish_bound` false).
    let cpu_ok = s.cpu_pressure.is_none_or(|p| p < CPU_PRESSURE_OK);
    let io_bound = ingest_fresh && latency_bound(s.io_latency_ms, s.arrival_gap_ms, IO_BOUND_FRACTION);
    let publish_bound =
        ingest_fresh && latency_bound(s.publish_latency_ms, s.arrival_gap_ms, PUBLISH_BOUND_FRACTION);

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

    // Goal-seeking mode: with memory safe, drive the configured SLOs (replication
    // lag, freshness, query latency, QPH) toward target with proportional,
    // range-bounded steps. Bypasses the legacy signal rules below; memory pressure
    // (above) still wins. `ingest_fresh` gates the write-derived lag/freshness
    // goals so an idle table can't ratchet on a wall-clock-growing lag reading.
    if goals.any_active() {
        return decide_goal(s, cur, b, goals, mem_ok, ingest_fresh);
    }

    // The system is unhealthy if ingest is falling behind OR queries are being
    // hurt by ingest's small-file output. Both are handled here; ingest-speed
    // levers that would worsen query health (more write shards = more files) are
    // held back while read-amp is high.
    if behind || read_amp_high || io_bound || publish_bound {
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
            if cpu_ok
                && let Some(v) = clamp_move_u64(
                    cur.compaction_background_interval_ms,
                    shrink_u64(cur.compaction_background_interval_ms),
                    b.compaction_background_interval_ms,
                )
            {
                return Some(Adjustment {
                    actuator: Actuator::CompactionIntervalMs,
                    new_value: v,
                    reason: "high read-amp: compact more often to drain small files for queries",
                });
            }
            if cpu_ok
                && let Some(v) = clamp_move_usize(
                    cur.compaction_trigger_files,
                    cur.compaction_trigger_files.saturating_sub(1),
                    b.compaction_trigger_files,
                )
            {
                return Some(Adjustment {
                    actuator: Actuator::CompactionTriggerFiles,
                    new_value: u64::try_from(v).unwrap_or(0),
                    reason: "high read-amp: lower compaction trigger",
                });
            }
        }

        // An I/O- or publish-bound table that is NOT behind grows the memtable to
        // write fewer, larger files and amortize the per-commit cost — the same
        // lever the "behind" tier uses first, triggered here by latency. Memory-gated.
        if (io_bound || publish_bound)
            && !behind
            && mem_ok
            && let Some(v) = clamp_move_i64(
                cur.inline_flush_max_bytes,
                grow_i64(cur.inline_flush_max_bytes),
                b.inline_flush_max_bytes,
            )
        {
            return Some(Adjustment {
                actuator: Actuator::InlineFlushBytes,
                new_value: u64::try_from(v).unwrap_or(0),
                reason: "io/publish-bound: enlarge memtable to amortize commits and write fewer, larger files",
            });
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
            // 3. ... Also withheld when CPU-bound (more shards steal query
            //    threads) or I/O-bound (more shards = more files = more uploads).
            if s.read_amp <= READ_AMP_LOW
                && !mutation_heavy
                && cpu_ok
                && !io_bound
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
            // 4. Last resort: compact more to keep the snapshot lean — unless
            //    CPU-bound, where more compaction would steal query threads.
            if cpu_ok
                && let Some(v) = clamp_move_u64(
                    cur.compaction_background_interval_ms,
                    shrink_u64(cur.compaction_background_interval_ms),
                    b.compaction_background_interval_ms,
                )
            {
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
        return relax_step(cur, b);
    }

    None
}

/// Goal-seeking decision (called by [`decide_with_goals`] once memory is safe and
/// at least one goal is active). Evolves the legacy ladder: the query-health tier
/// fires on a violated latency/QPH goal, the ingest/lag tier on a violated
/// lag/freshness goal (gated on fresh ingest), the healthy-relax tier when every
/// goal is comfortably met. Steps are proportional to the violation and bounded to
/// `range / STEPS_PER_WINDOW` per tick. One move per tick; every move clamped to
/// `TuningBounds`. Query tier runs before the lag tier — never trade query health
/// for ingest speed — and the contested `write_concurrency` lever is gated so the
/// two never fight (queries shed shards only when not also behind on ingest; lag
/// adds shards only when no query goal is violated and read-amp/deletes are low).
#[expect(
    clippy::too_many_lines,
    reason = "one bounded move per tier; splitting would obscure the priority ladder"
)]
fn decide_goal(
    s: &IngestSnapshot,
    cur: &ActuatorValues,
    b: &TuningBounds,
    goals: &Goals,
    mem_ok: bool,
    ingest_fresh: bool,
) -> Option<Adjustment> {
    let query_v = goals.query_violation(s);
    let query_violated = query_v > 0.0;
    // Lag/freshness grow with the wall clock on an idle table, so only act on them
    // when genuinely new ingest has arrived since the last move.
    let ingest_v = goals.ingest_violation(s);
    let ingest_violated = ingest_fresh && ingest_v > 0.0;
    // Environment/data gates (same semantics as the legacy ladder): CPU-bound
    // withholds CPU-stealing moves, I/O-bound and mutation-heavy withhold the
    // write-concurrency lever (more shards = more files / key churn).
    let cpu_ok = s.cpu_pressure.is_none_or(|p| p < CPU_PRESSURE_OK);
    let io_bound = ingest_fresh && latency_bound(s.io_latency_ms, s.arrival_gap_ms, IO_BOUND_FRACTION);
    let mutation_heavy = (s.delete_fraction + s.update_fraction) > MUTATION_HEAVY_FRACTION;

    // (2) Query-health tier: a violated latency/QPH goal. Larger/fewer files and
    // more compaction help queries; shedding write shards cuts file fan-out.
    if query_violated {
        if mem_ok
            && let Some(v) = clamp_move_i64(
                cur.inline_flush_max_bytes,
                goal_grow_i64(cur.inline_flush_max_bytes, b.inline_flush_max_bytes, query_v),
                b.inline_flush_max_bytes,
            )
        {
            return Some(Adjustment {
                actuator: Actuator::InlineFlushBytes,
                new_value: u64::try_from(v).unwrap_or(0),
                reason: "query-latency goal: enlarge memtable → fewer small files for queries",
            });
        }
        if cpu_ok
            && let Some(v) = clamp_move_u64(
                cur.compaction_background_interval_ms,
                goal_shrink_u64(
                    cur.compaction_background_interval_ms,
                    b.compaction_background_interval_ms,
                    query_v,
                ),
                b.compaction_background_interval_ms,
            )
        {
            return Some(Adjustment {
                actuator: Actuator::CompactionIntervalMs,
                new_value: v,
                reason: "query-latency goal: compact more often to drain small files",
            });
        }
        if let Some(v) = clamp_move_usize(
            cur.compaction_trigger_files,
            goal_shrink_usize(cur.compaction_trigger_files, b.compaction_trigger_files, query_v),
            b.compaction_trigger_files,
        ) {
            return Some(Adjustment {
                actuator: Actuator::CompactionTriggerFiles,
                new_value: u64::try_from(v).unwrap_or(0),
                reason: "query-latency goal: lower compaction trigger",
            });
        }
        // Shed a write shard (fewer files) — but not while ingest is also behind,
        // so we don't slow the lag goal to help the query goal.
        if !ingest_violated
            && let Some(v) = clamp_move_usize(
                cur.write_concurrency.max(1),
                goal_shrink_usize(cur.write_concurrency.max(1), b.write_concurrency, query_v),
                b.write_concurrency,
            )
        {
            return Some(Adjustment {
                actuator: Actuator::WriteConcurrency,
                new_value: u64::try_from(v).unwrap_or(0),
                reason: "query-latency goal: shed a write shard to reduce small-file fan-out",
            });
        }
    }

    // (3) Ingest/lag tier: a violated replication-lag/freshness goal. Grow buffers
    // first (help lag AND queries), then the mem-tier, then add write shards —
    // gated so extra shards (= more files) never fire while a query goal is
    // violated, read-amp is high, or the stream is delete-heavy.
    if ingest_violated {
        if mem_ok
            && let Some(v) = clamp_move_i64(
                cur.inline_flush_max_bytes,
                goal_grow_i64(cur.inline_flush_max_bytes, b.inline_flush_max_bytes, ingest_v),
                b.inline_flush_max_bytes,
            )
        {
            return Some(Adjustment {
                actuator: Actuator::InlineFlushBytes,
                new_value: u64::try_from(v).unwrap_or(0),
                reason: "replication-lag goal: enlarge memtable (fewer files + amortized commits)",
            });
        }
        if mem_ok
            && let Some(v) = clamp_move_i64(
                cur.mem_tier_max_bytes,
                goal_grow_i64(cur.mem_tier_max_bytes, b.mem_tier_max_bytes, ingest_v),
                b.mem_tier_max_bytes,
            )
        {
            return Some(Adjustment {
                actuator: Actuator::MemTierMaxBytes,
                new_value: u64::try_from(v).unwrap_or(0),
                reason: "replication-lag goal: enlarge the in-memory CDC tier (fewer writer-blocking spills)",
            });
        }
        if !query_violated
            && s.read_amp <= READ_AMP_LOW
            && !mutation_heavy
            && cpu_ok
            && !io_bound
            && let Some(v) = clamp_move_usize(
                cur.write_concurrency.max(1),
                goal_grow_usize(cur.write_concurrency.max(1), b.write_concurrency, ingest_v),
                b.write_concurrency,
            )
        {
            return Some(Adjustment {
                actuator: Actuator::WriteConcurrency,
                new_value: u64::try_from(v).unwrap_or(0),
                reason: "replication-lag goal, buffers maxed: raise write concurrency",
            });
        }
        if cpu_ok
            && let Some(v) = clamp_move_u64(
                cur.compaction_background_interval_ms,
                goal_shrink_u64(
                    cur.compaction_background_interval_ms,
                    b.compaction_background_interval_ms,
                    ingest_v,
                ),
                b.compaction_background_interval_ms,
            )
        {
            return Some(Adjustment {
                actuator: Actuator::CompactionIntervalMs,
                new_value: v,
                reason: "replication-lag goal: compact more to keep the snapshot lean",
            });
        }
    }

    // (4) Healthy-relax: every active goal comfortably met and memory ok → hand
    // resources back, one per tick, smallest-goal-impact first. Relaxing need not
    // be incremental, so it reuses the legacy ±50% steps. The memory buffers are
    // deliberately NOT shrunk here (no memory pressure; keep them sized for the
    // next burst).
    if goals.all_comfortably_met(s) && mem_ok {
        return relax_step(cur, b);
    }

    None
}

/// The healthy/relax tier shared by both ladders: hand resources back one per tick
/// — shed a write shard, relax the compaction trigger, then lengthen the
/// background interval — using the legacy ±50% steps (relaxing need not be
/// incremental). The memory buffers (memtable, mem-tier) are deliberately NOT
/// shrunk here (no memory pressure; keep them sized for the next burst). Returns
/// `None` when every lever is already at its efficient extreme. The caller gates
/// on the appropriate healthy / all-goals-met + memory-ok condition.
fn relax_step(cur: &ActuatorValues, b: &TuningBounds) -> Option<Adjustment> {
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

// --- Goal-mode steps: additive, a bounded fraction of the actuator range ---
//
// `range / STEPS_PER_WINDOW` is the per-tick cap (so N steps span the range —
// "converge in the window") scaled by the violation (so a small violation takes a
// small step) with a crawl floor (so a barely-violating goal still progresses).
// Additive (not multiplicative) so the convergence guarantee holds against the
// linear range. The result is clamped to `[floor, ceiling]` by `clamp_move_*` at
// the call site, exactly like the legacy steps.

/// Per-tick goal-mode step magnitude for an actuator with the given `range`,
/// scaled by `violation` in `[0, 1]`. At least 1 so a tiny range still moves.
#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "step is a small non-negative value bounded by range/N; clamped at the call site"
)]
fn goal_step_magnitude_u64(range: u64, violation: f64) -> u64 {
    let max_step = (range / u64::from(STEPS_PER_WINDOW)).max(1);
    let frac = violation.clamp(GOAL_MIN_STEP_FRACTION, 1.0);
    ((u64_to_f64(max_step) * frac).round() as u64).max(1)
}

fn goal_grow_i64(v: i64, (lo, hi): (i64, i64), violation: f64) -> i64 {
    let range = u64::try_from(hi.saturating_sub(lo)).unwrap_or(0);
    let step = i64::try_from(goal_step_magnitude_u64(range, violation)).unwrap_or(i64::MAX);
    v.saturating_add(step)
}

fn goal_grow_usize(v: usize, (lo, hi): (usize, usize), violation: f64) -> usize {
    let range = u64::try_from(hi.saturating_sub(lo)).unwrap_or(u64::MAX);
    let step = usize::try_from(goal_step_magnitude_u64(range, violation)).unwrap_or(usize::MAX);
    v.saturating_add(step)
}

fn goal_shrink_u64(v: u64, (lo, hi): (u64, u64), violation: f64) -> u64 {
    v.saturating_sub(goal_step_magnitude_u64(hi.saturating_sub(lo), violation))
}

fn goal_shrink_usize(v: usize, (lo, hi): (usize, usize), violation: f64) -> usize {
    let range = u64::try_from(hi.saturating_sub(lo)).unwrap_or(u64::MAX);
    let step = usize::try_from(goal_step_magnitude_u64(range, violation)).unwrap_or(usize::MAX);
    v.saturating_sub(step)
}

/// `u64` → `f64` for rate/ratio/pressure math. The precision loss is acceptable:
/// these are EWMA estimates and metrics whose magnitudes never approach 2^52.
#[expect(clippy::cast_precision_loss)]
fn u64_to_f64(v: u64) -> f64 {
    v as f64
}

/// Encode a pressure fraction (memory or CPU `used/budget`) as the ×1000
/// `AtomicU64` wire value shared by the memory and CPU samplers, or `None` to skip
/// a non-finite/negative sample. Capped at 1000× so the f64→u64 cast can't
/// overflow; ×1000 keeps three decimals of resolution.
#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "capped at 1000× before the cast; the value is a small non-negative fraction"
)]
fn pressure_to_milli(fraction: f64) -> Option<u64> {
    (fraction.is_finite() && fraction >= 0.0).then(|| (fraction.min(1000.0) * 1000.0).round() as u64)
}

/// Decode the ×1000 pressure wire value back to a fraction; `u64::MAX` is the
/// "unknown" sentinel (no budget/sample yet).
fn milli_to_pressure(milli: u64) -> Option<f64> {
    (milli != u64::MAX).then(|| u64_to_f64(milli) / 1000.0)
}

/// True when a per-batch latency EWMA exceeds `frac` of the offered-load interval
/// (`arrival_gap_ms`). Relative gate: unavailable latency or a zero gap ⇒ false
/// (collapses to legacy behavior). Shared by the I/O- and publish-bound flags.
fn latency_bound(latency_ms: Option<f64>, arrival_gap_ms: f64, frac: f64) -> bool {
    matches!(latency_ms, Some(l) if arrival_gap_ms > 0.0 && l > frac * arrival_gap_ms)
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
        clippy::cast_precision_loss,
        clippy::cast_possible_wrap
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
            replication_lag_secs: None,
            freshness_secs: None,
            query_latency_p99_ms: None,
            qph: None,
            cpu_pressure: None,
            io_latency_ms: None,
            publish_latency_ms: None,
            update_fraction: 0.0,
            data_storage: StorageClass::Unknown,
            metastore_storage: StorageClass::Unknown,
        }
    }

    /// Legacy (no-goals) `decide`, exercising the signal-driven controller for the
    /// existing test suite (the goal layer is opt-in).
    fn decide(
        s: &IngestSnapshot,
        cur: &ActuatorValues,
        b: &TuningBounds,
        since_last: Duration,
        min_dwell: Duration,
        samples_at_last_move: u64,
    ) -> Option<Adjustment> {
        decide_with_goals(
            s,
            cur,
            b,
            since_last,
            min_dwell,
            samples_at_last_move,
            &Goals::none(),
        )
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

    // ---- goal-driven controller ------------------------------------------

    /// `decide_with_goals` past the (goal) dwell and warmup, fresh-sample gate open.
    fn goal_decide(
        s: &IngestSnapshot,
        cur: &ActuatorValues,
        b: &TuningBounds,
        goals: &Goals,
    ) -> Option<Adjustment> {
        decide_with_goals(s, cur, b, ms(60_000), ms(30_000), 0, goals)
    }

    fn lag_goal(target_secs: f64) -> Goals {
        Goals::from_targets(Some(target_secs), None, None, None, Duration::from_mins(1))
    }

    #[test]
    fn no_goals_matches_legacy_decide() {
        // With no goals, `decide_with_goals` must equal the legacy `decide` for the
        // same inputs across representative snapshots — the backward-compat guard.
        let cases = [
            IngestSnapshot {
                apply_vs_arrival: 2.0,
                samples: 40,
                ..snap()
            },
            IngestSnapshot {
                read_amp: 20,
                ..snap()
            },
            IngestSnapshot {
                apply_vs_arrival: 0.2,
                read_amp: 1,
                ..snap()
            },
            IngestSnapshot {
                mem_pressure: Some(0.95),
                apply_vs_arrival: 2.0,
                samples: 40,
                ..snap()
            },
        ];
        for s in cases {
            let legacy = decide(&s, &actuators(), &bounds(), ms(60_000), ms(30_000), 0);
            let with_none = decide_with_goals(
                &s,
                &actuators(),
                &bounds(),
                ms(60_000),
                ms(30_000),
                0,
                &Goals::none(),
            );
            assert_eq!(legacy, with_none, "no-goals must match legacy for {s:?}");
        }
    }

    #[test]
    fn lag_goal_violated_grows_memtable_first() {
        // 20s lag vs a 5s goal (violated), ingest fresh, memory ok: the first lever
        // is the memtable (helps lag AND queries).
        let s = IngestSnapshot {
            replication_lag_secs: Some(20.0),
            ..snap()
        };
        let adj = goal_decide(&s, &actuators(), &bounds(), &lag_goal(5.0)).expect("a move");
        assert_eq!(adj.actuator, Actuator::InlineFlushBytes);
        assert!(adj.new_value > actuators().inline_flush_max_bytes as u64);
    }

    #[test]
    fn freshness_goal_violated_grows_memtable() {
        let s = IngestSnapshot {
            freshness_secs: Some(30.0),
            ..snap()
        };
        let goals = Goals::from_targets(None, Some(5.0), None, None, Duration::from_mins(1));
        let adj = goal_decide(&s, &actuators(), &bounds(), &goals).expect("a move");
        assert_eq!(adj.actuator, Actuator::InlineFlushBytes);
    }

    #[test]
    fn query_latency_goal_violated_acts_on_query_levers() {
        // p99 500ms vs 100ms goal: first lever is the memtable grow (fewer files).
        let s = IngestSnapshot {
            query_latency_p99_ms: Some(500.0),
            read_amp: 2,
            ..snap()
        };
        let goals = Goals::from_targets(None, None, Some(100.0), None, Duration::from_mins(1));
        let adj = goal_decide(&s, &actuators(), &bounds(), &goals).expect("a move");
        assert_eq!(adj.actuator, Actuator::InlineFlushBytes);
    }

    #[test]
    fn query_latency_goal_sheds_write_concurrency_when_other_levers_maxed() {
        let s = IngestSnapshot {
            query_latency_p99_ms: Some(500.0),
            read_amp: 2,
            ..snap()
        };
        let goals = Goals::from_targets(None, None, Some(100.0), None, Duration::from_mins(1));
        // Memtable at ceiling, compaction at its floors → only the shard-shed remains.
        let cur = ActuatorValues {
            inline_flush_max_bytes: 128 * 1024 * 1024,
            compaction_background_interval_ms: 2_000,
            compaction_trigger_files: 2,
            write_concurrency: 8,
            ..actuators()
        };
        let adj = goal_decide(&s, &cur, &bounds(), &goals).expect("a move");
        assert_eq!(adj.actuator, Actuator::WriteConcurrency);
        assert!(adj.new_value < 8, "latency goal sheds a shard, never grows one");
    }

    #[test]
    fn qph_goal_below_target_acts_like_latency() {
        // QPH 100 vs a 5000 goal (violated, higher-is-better): query-health levers.
        let s = IngestSnapshot {
            qph: Some(100.0),
            read_amp: 2,
            ..snap()
        };
        let goals = Goals::from_targets(None, None, None, Some(5_000.0), Duration::from_mins(1));
        let adj = goal_decide(&s, &actuators(), &bounds(), &goals).expect("a move");
        assert_eq!(adj.actuator, Actuator::InlineFlushBytes);
    }

    #[test]
    fn lag_and_query_violated_serves_via_buffers_not_shards() {
        // Both lag and latency violated: buffers (help both) move first; the
        // contested write-concurrency lever is NOT grown while a query goal hurts.
        let s = IngestSnapshot {
            replication_lag_secs: Some(20.0),
            query_latency_p99_ms: Some(500.0),
            read_amp: 2,
            ..snap()
        };
        let goals =
            Goals::from_targets(Some(5.0), None, Some(100.0), None, Duration::from_mins(1));
        let adj = goal_decide(&s, &actuators(), &bounds(), &goals).expect("a move");
        assert_eq!(adj.actuator, Actuator::InlineFlushBytes);
    }

    #[test]
    fn lag_violated_no_query_goal_raises_concurrency_when_buffers_maxed() {
        let s = IngestSnapshot {
            replication_lag_secs: Some(20.0),
            read_amp: 1,
            delete_fraction: 0.0,
            ..snap()
        };
        let goals = lag_goal(5.0);
        let cur = ActuatorValues {
            inline_flush_max_bytes: 128 * 1024 * 1024,
            mem_tier_max_bytes: 2048 * 1024 * 1024,
            write_concurrency: 4,
            ..actuators()
        };
        let adj = goal_decide(&s, &cur, &bounds(), &goals).expect("a move");
        assert_eq!(adj.actuator, Actuator::WriteConcurrency);
        assert!(adj.new_value > 4, "lag goal raises concurrency when buffers maxed");
    }

    #[test]
    fn lag_violated_delete_heavy_withholds_concurrency() {
        // Delete-heavy stream: even with buffers maxed and read-amp low, the lag
        // goal must NOT add write shards (multiplies per-burst small-file fan-out).
        let s = IngestSnapshot {
            replication_lag_secs: Some(20.0),
            read_amp: 1,
            delete_fraction: 0.5,
            ..snap()
        };
        let cur = ActuatorValues {
            inline_flush_max_bytes: 128 * 1024 * 1024,
            mem_tier_max_bytes: 2048 * 1024 * 1024,
            compaction_background_interval_ms: 2_000,
            write_concurrency: 4,
            ..actuators()
        };
        let adj = goal_decide(&s, &cur, &bounds(), &lag_goal(5.0));
        if let Some(a) = adj {
            assert_ne!(a.actuator, Actuator::WriteConcurrency);
        }
    }

    #[test]
    fn goal_idle_table_with_stale_lag_holds() {
        // Lag goal violated, but no new ingest since the last move: the
        // fresh-sample gate must suppress the lag tier (no ratchet on an idle table).
        let s = IngestSnapshot {
            replication_lag_secs: Some(20.0),
            samples: 40,
            ..snap()
        };
        let adj =
            decide_with_goals(&s, &actuators(), &bounds(), ms(60_000), ms(30_000), 40, &lag_goal(5.0));
        assert!(adj.is_none(), "stale lag must not trigger a goal move");
    }

    #[test]
    fn goal_within_deadband_holds_then_moves() {
        // 16% over target is inside the 20% deadband → hold.
        let s = IngestSnapshot {
            replication_lag_secs: Some(5.8),
            ..snap()
        };
        assert!(goal_decide(&s, &actuators(), &bounds(), &lag_goal(5.0)).is_none());
        // 30% over → past the deadband → a move fires.
        let s2 = IngestSnapshot {
            replication_lag_secs: Some(6.5),
            ..snap()
        };
        assert!(goal_decide(&s2, &actuators(), &bounds(), &lag_goal(5.0)).is_some());
    }

    #[test]
    fn goal_step_never_exceeds_range_over_n() {
        // A massively-violated goal still moves the memtable by at most range/N.
        let s = IngestSnapshot {
            replication_lag_secs: Some(10_000.0),
            ..snap()
        };
        let b = bounds();
        let cur = actuators();
        let adj = goal_decide(&s, &cur, &b, &lag_goal(1.0)).expect("a move");
        assert_eq!(adj.actuator, Actuator::InlineFlushBytes);
        let (lo, hi) = b.inline_flush_max_bytes;
        let max_step = (hi - lo) / i64::from(STEPS_PER_WINDOW);
        let delta = adj.new_value as i64 - cur.inline_flush_max_bytes;
        assert!(delta > 0 && delta <= max_step, "delta {delta} not in (0, {max_step}]");
    }

    #[test]
    fn memory_pressure_overrides_goal_growth() {
        // Lag goal badly violated, but memory pressure high → the shared memory
        // block shrinks the memtable instead of growing it.
        let s = IngestSnapshot {
            replication_lag_secs: Some(50.0),
            mem_pressure: Some(0.95),
            ..snap()
        };
        let adj = goal_decide(&s, &actuators(), &bounds(), &lag_goal(5.0)).expect("a move");
        assert_eq!(adj.actuator, Actuator::InlineFlushBytes);
        assert!(
            adj.new_value < actuators().inline_flush_max_bytes as u64,
            "memory pressure must SHRINK the memtable, overriding the lag goal"
        );
    }

    #[test]
    fn all_goals_met_relaxes_resources() {
        // Lag well under target (comfortably met) + healthy → relax sheds a shard.
        let s = IngestSnapshot {
            replication_lag_secs: Some(1.0),
            read_amp: 1,
            apply_vs_arrival: 0.2,
            ..snap()
        };
        let adj = goal_decide(&s, &actuators(), &bounds(), &lag_goal(5.0)).expect("a move");
        assert_eq!(adj.actuator, Actuator::WriteConcurrency);
        assert!(adj.new_value < actuators().write_concurrency as u64);
    }

    #[test]
    fn lag_goal_converges_to_ceiling_within_window_steps() {
        // Hold a fixed large lag violation, applying each move: the memtable reaches
        // its ceiling within STEPS_PER_WINDOW(+slack) steps, with no step over the cap.
        let b = bounds();
        let live = LiveActuators::new(actuators());
        let (lo, hi) = b.inline_flush_max_bytes;
        let max_step = (hi - lo) / i64::from(STEPS_PER_WINDOW);
        let goals = lag_goal(1.0);
        let mut steps = 0u32;
        loop {
            let cur = live.values();
            let s = IngestSnapshot {
                replication_lag_secs: Some(100.0),
                ..snap()
            };
            let Some(adj) = goal_decide(&s, &cur, &b, &goals) else {
                break;
            };
            if adj.actuator != Actuator::InlineFlushBytes {
                break; // memtable maxed; later levers (mem-tier, etc.) take over.
            }
            let delta = adj.new_value as i64 - cur.inline_flush_max_bytes;
            assert!(delta > 0 && delta <= max_step, "step {delta} exceeds cap {max_step}");
            live.apply(&adj);
            steps += 1;
            assert!(steps <= STEPS_PER_WINDOW + 1, "did not converge within N steps");
        }
        assert_eq!(
            live.inline_flush_max_bytes(),
            hi,
            "memtable converged to its ceiling"
        );
    }

    // ---- query observations + registry -----------------------------------

    #[test]
    fn query_observations_p99_and_count() {
        let obs = QueryObservations::new();
        assert!(obs.p99_latency_ms().is_none());
        assert_eq!(obs.total_queries(), 0);
        for _ in 0..99 {
            obs.record_query(5.0);
        }
        obs.record_query(5_000.0); // the top-1% outlier
        assert_eq!(obs.total_queries(), 100);
        let p99 = obs.p99_latency_ms().expect("p99");
        // 99% threshold is reached within the 5ms bucket; the lone slow query is
        // the 100th (top 1%), so p99 reports the 5ms bucket bound.
        assert!((p99 - 5.0).abs() < f64::EPSILON, "p99 ~5ms, got {p99}");
    }

    #[test]
    fn query_observations_qph_positive_after_queries() {
        let obs = QueryObservations::new();
        obs.record_query(10.0);
        if let Some(qph) = obs.qph() {
            assert!(qph > 0.0);
        }
    }

    #[test]
    fn query_registry_push_matches_registration() {
        // Bare registration must be reachable by a schema-qualified push (both
        // normalize to the bare table name) — the key-normalization guard.
        let obs = register_query_observations("regtest_unique_tbl");
        assert_eq!(obs.total_queries(), 0);
        record_query_latency("public.regtest_unique_tbl", 42.0);
        assert_eq!(
            obs.total_queries(),
            1,
            "qualified push must reach the bare-registered handle"
        );
        record_query_latency("regtest_unique_tbl", 7.0);
        assert_eq!(obs.total_queries(), 2);
        // Unregistered table is a no-op (must not panic).
        record_query_latency("never_registered_zzz", 1.0);
        deregister_query_observations("regtest_unique_tbl");
        record_query_latency("regtest_unique_tbl", 9.0); // post-deregister: no-op
    }
}
