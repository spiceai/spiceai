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

use std::collections::HashMap;
#[cfg(target_os = "linux")]
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering};
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
pub(crate) const WARMUP_BATCHES: u64 = 16;

/// "Falling behind" hysteresis: act only once apply latency exceeds the offered-
/// load interval by this factor (not merely equals it), so we don't chase noise.
const BEHIND_RATIO: f64 = 1.2;

/// `apply_vs_arrival` at or above which the seq-prefix bake DEFERS
/// (back-pressure). `1.0` is break-even (per-batch apply latency == the
/// inter-batch arrival interval); above it the apply has no headroom, so a
/// background bake competing for the write path would push replication lag up.
/// The bake yields to the apply here. Tunable: lower to protect lag harder
/// (bake only with more slack), raise to recover bake/QPH at the cost of lag
/// headroom.
pub(crate) const BAKE_BACKPRESSURE_RATIO: f64 = 1.0;

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

/// Memory-usage fraction at which pressure is CRITICAL: close enough to the host
/// ceiling that one ×2/3 shrink step per dwell is too slow. At or above this the
/// controller (a) collapses the live memory caps — inline memtable then mem-tier —
/// straight to their floors in a single move, and (b) the impure mem-tier
/// checkpoint tick bypasses its churn gate to force an immediate drain, actually
/// releasing resident RAM (a cap shrink alone does not evict already-resident
/// bytes). A last-resort safety valve ABOVE [`MEM_PRESSURE_HIGH`], atop the
/// structural query-pool/tier partition that already prevents overcommit.
pub(crate) const MEM_PRESSURE_CRITICAL: f64 = 0.90;

/// CPU busy-fraction (of available cores, cgroup-aware) below which the controller
/// re-enables CPU-stealing moves — adding write-encode shards and shrinking the
/// compaction interval both compete with query threads, so they are withheld until
/// CPU is comfortably free. Unlike memory (a hard ceiling, which also needs a
/// high-water *shrink* trigger, [`MEM_PRESSURE_HIGH`]), CPU saturation needs no
/// active-shrink threshold: the relax tier already sheds load, so a single growth
/// gate suffices.
const CPU_PRESSURE_OK: f64 = 0.75;

/// Fraction of the offered-load interval (`arrival_gap_ms`) that per-batch
/// object-store/disk write latency must exceed for the table to count as
/// "I/O-bound". Relative (not a fixed-ms cliff) so it scales with the table's rate.
const IO_BOUND_FRACTION: f64 = 0.5;

/// Same, for per-batch metastore publish latency → "publish-bound" (the
/// single-writer commit is eating the offered-load window).
const PUBLISH_BOUND_FRACTION: f64 = 0.5;

/// Multiplier applied to [`IO_BOUND_FRACTION`] / [`PUBLISH_BOUND_FRACTION`] on a slow
/// storage tier (EBS, object store). Halving the bar makes the controller treat a
/// table as I/O-/publish-bound at a lower per-batch latency, so on slow/networked
/// media it amortizes commits (grows the memtable) and withholds write shards
/// *sooner* — the closed-loop counterpart to the warm-start's per-tier file-size
/// pre-sizing. Fast media (local SSD, tmpfs) use the base fraction unchanged.
const SLOW_TIER_BOUND_SCALE: f64 = 0.5;

/// EWMA smoothing for the FAST per-batch I/O/publish latency estimate that powers
/// the cliff detector. Higher than [`EWMA_ALPHA`] so it tracks a sudden latency
/// step (an EBS burst-credit cliff, the instance EBS pipe saturating) within a
/// batch or two — where the slow EWMA lags and the additive controller would crawl
/// down over many dwells while replication lag balloons.
const EWMA_ALPHA_FAST: f64 = 0.6;

/// I/O cliff trigger: the fast latency EWMA exceeding the slow EWMA by this factor
/// is a step-change in write/publish latency, not noise. The controller answers
/// with one decisive, multiplicative backoff (shed write shards, then amortize via
/// the memtable) instead of an additive crawl — the I/O analogue of
/// [`MEM_PRESSURE_CRITICAL`]'s fast path. ~3× separates a genuine cliff from EWMA
/// jitter at [`EWMA_ALPHA_FAST`].
const IO_CLIFF_RATIO: f64 = 3.0;

/// A cliff additionally requires the fast EWMA to clear this absolute floor (ms),
/// so a multiplicative jump within sub-millisecond noise (0.1 → 0.5 ms) cannot
/// trip it — only latency that actually threatens the offered-load window.
const IO_CLIFF_FLOOR_MS: f64 = 10.0;

/// Continuous slow-tier bias bounds (see [`tier_scale`]). Measured write
/// throughput at/below [`TIER_SCALE_SLOW_MBPS`] gets the full slow-tier scale
/// ([`SLOW_TIER_BOUND_SCALE`]); at/above [`TIER_SCALE_FAST_MBPS`] gets none (1.0);
/// linearly interpolated between. ~125 MiB/s ≈ a gp3 baseline volume; ~1 GiB/s ≈
/// fast local `NVMe` or a high-provisioned io2 — which should not get the same
/// commit-amortization pressure as slow gp3 even though both classify as `Ebs`.
const TIER_SCALE_SLOW_MBPS: f64 = 125.0;
const TIER_SCALE_FAST_MBPS: f64 = 1024.0;

/// Slow-tier drain offset: on EBS/object-store media the controller starts
/// shrinking the live memory buffers at a memory pressure this much BELOW
/// [`MEM_PRESSURE_HIGH`] / [`MEM_PRESSURE_OK`], so it drains earlier in smaller
/// increments rather than hitting the big synchronous critical drain — whose large
/// write burst would compete with ingest for the same slow pipe and worsen
/// backpressure exactly when the table is already struggling.
const SLOW_TIER_MEM_DRAIN_OFFSET: f64 = 0.07;

/// CPU growth gate on a T-family burstable instance: lower than [`CPU_PRESSURE_OK`]
/// so the controller stops adding CPU-stealing work (write shards, more compaction)
/// well before CPU credits deplete and the vCPUs throttle to a low baseline — a
/// cliff the busy-fraction sampler cannot see coming.
const CPU_PRESSURE_OK_BURSTABLE: f64 = 0.50;

/// Idle horizon for [`QueryObservations::qph`]: with no query observed within this
/// window the table is treated as having no QPH signal (the goal is skipped) rather
/// than reporting a lifetime rate that decays toward 0 while parked. ~5 minutes —
/// long enough to span normal gaps between analytical queries, short enough that a
/// parked table stops driving QPH tuning promptly.
const QPH_IDLE_MS: u64 = 300_000;

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

/// Consecutive eligible control ticks (past warmup + dwell) on which a goal stays
/// violated while the controller has NO move left to make — every relevant lever
/// already clamped at its bound — before the SLO is declared infeasible on the
/// current hardware. At [`STEPS_PER_WINDOW`] ticks per convergence window this is
/// ~2 windows of being maxed out, long enough to rule out transient convergence.
/// Surfaced as a telemetry gauge + one operator warning per infeasible episode,
/// naming the binding constraint (e.g. EBS-bandwidth-bound) so silent
/// underperformance becomes actionable (provision more IOPS/throughput, or relax
/// the SLO). The signal self-clears if the controller resumes progress.
pub(crate) const GOAL_INFEASIBLE_STUCK_TICKS: u64 = 2 * STEPS_PER_WINDOW as u64;

// ---------------------------------------------------------------------------
// Environment detection: process memory budget + cgroup-aware usage
// ---------------------------------------------------------------------------

/// Process memory budget in bytes (the cgroup-aware limit), installed at startup
/// and refreshed after an app reload observes a changed limit. `0` = unset, in which
/// case memory pressure is reported as unknown and the controller runs without the
/// memory rule. Process-wide because RAM is shared across tables.
static GLOBAL_MEMORY_BUDGET: AtomicU64 = AtomicU64::new(0);

#[cfg(target_os = "linux")]
static CGROUP_V2_MEMORY_CURRENT_PATH: OnceLock<Option<String>> = OnceLock::new();

#[cfg(target_os = "linux")]
static CGROUP_V2_MEMORY_STAT_PATH: OnceLock<Option<String>> = OnceLock::new();

#[cfg(target_os = "linux")]
static CGROUP_V1_MEMORY_USAGE_PATH: OnceLock<Option<String>> = OnceLock::new();

#[cfg(target_os = "linux")]
static CGROUP_V1_MEMORY_STAT_PATH: OnceLock<Option<String>> = OnceLock::new();

/// Process-wide CPU busy-fraction of available cores (cgroup-aware), stored ×1000.
/// `u64::MAX` = unknown (non-Linux, unreadable, or the first/too-close sample with
/// no usable delta yet). Process-global: every per-table loop reads this one value,
/// because CPU is shared across tables (like the memory budget).
static CPU_PRESSURE_MILLI: AtomicU64 = AtomicU64::new(u64::MAX);

/// Whether the host is a T-family burstable EC2 instance (detected via IMDS,
/// installed by the runtime). On a burstable instance CPU credits deplete under
/// sustained load and the vCPUs throttle to a low baseline — a cliff the
/// busy-fraction sampler alone cannot see — so the controller withholds
/// CPU-stealing moves at a lower busy-fraction ([`CPU_PRESSURE_OK_BURSTABLE`]).
/// Process-global: the instance type is a host fact, not a per-table one.
static CPU_BURSTABLE: AtomicBool = AtomicBool::new(false);

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

/// Install or refresh the cgroup-aware process memory budget the dynamic tuner uses
/// to compute memory pressure.
pub fn set_global_memory_budget(bytes: u64) {
    GLOBAL_MEMORY_BUDGET.store(bytes, Ordering::Release);
}

/// Record whether the host is a T-family burstable EC2 instance (from IMDS).
/// Idempotent; called by the runtime at table registration.
pub fn set_cpu_burstable(burstable: bool) {
    CPU_BURSTABLE.store(burstable, Ordering::Relaxed);
}

fn cpu_burstable() -> bool {
    CPU_BURSTABLE.load(Ordering::Relaxed)
}

fn global_memory_budget() -> Option<u64> {
    match GLOBAL_MEMORY_BUDGET.load(Ordering::Acquire) {
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
/// `[floor, ceiling]` for the adaptively-tuned target Vortex file size, derived
/// from the configured (storage-tier-aware) size: the controller may grow files
/// up to 4× the configured size for scan-heavy query goals and shrink to ½ — never
/// below a 64 MiB floor, never above 2 GiB — always bounded by the static config.
/// A configured size of `0` (size-rolling disabled) is left for the caller to pin.
pub(crate) fn adaptive_target_file_size_bounds(initial_bytes: i64) -> (i64, i64) {
    const FLOOR: i64 = 64 * MIB;
    const CEIL: i64 = 2048 * MIB;
    let lo = (initial_bytes / 2).max(FLOOR);
    let hi = initial_bytes.saturating_mul(4).clamp(lo, CEIL);
    (lo, hi)
}

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

/// Current process/cgroup memory *demand* in bytes — the unreclaimable working
/// set, from cgroup v2 then v1, falling back to process RSS; `None` when
/// unavailable. This is the "detect the environment and adjust" read that closes
/// the loop on memory.
///
/// Demand is the cgroup charge (`memory.current` / `memory.usage_in_bytes`) MINUS
/// the page cache the kernel can drop on demand. The total charge counts the
/// file-backed cache left behind by the table's own Vortex writes, which is
/// reclaimed — not OOM-killed — when the limit is approached: charging it as
/// demand makes a write-heavy CDC table read as critically short of memory while
/// its unreclaimable footprint sits far below the budget, and the controller
/// answers by collapsing the live buffers to their floors, which spills
/// continuously. `working_set_excludes_reclaimable_page_cache` carries the
/// measured cgroup accounting from the run that exposed this (issue #12531).
///
/// The numerator is read from THIS cgroup, while the budget it is divided against
/// ([`global_memory_budget`]) is the tightest `memory.max` along the whole cgroup
/// path (`telemetry::hardware::cgroup_memory_limit`). Where the binding limit sits
/// on an ancestor shared with other processes, the ratio reads low.
#[cfg(target_os = "linux")]
fn current_memory_bytes() -> Option<u64> {
    cgroup_v2_working_set()
        .or_else(cgroup_v1_working_set)
        .or_else(proc_self_rss_bytes)
}

/// cgroup v2 charge less its freely-reclaimable page cache. An unreadable or
/// unparseable `memory.stat` subtracts nothing, so the estimate degrades to the
/// raw charge rather than to an unknown signal.
#[cfg(target_os = "linux")]
fn cgroup_v2_working_set() -> Option<u64> {
    let current = cgroup_v2_memory_current()?;
    let reclaimable = read_cgroup_file(&CGROUP_V2_MEMORY_STAT_PATH, "memory.stat", true)
        .as_deref()
        // All file-backed cache, less the parts the kernel cannot drop without
        // first doing work: `shmem` (tmpfs pages need swap) and `file_dirty` /
        // `file_writeback` (writeback must complete first).
        .and_then(|stat| reclaimable_page_cache(stat, "file", RECLAIM_EXCLUDED_V2))
        .unwrap_or(0);
    Some(current.saturating_sub(reclaimable))
}

#[cfg(target_os = "linux")]
fn cgroup_v1_working_set() -> Option<u64> {
    let current = cgroup_v1_memory_current()?;
    let reclaimable = read_cgroup_file(&CGROUP_V1_MEMORY_STAT_PATH, "memory.stat", false)
        .as_deref()
        .and_then(v1_reclaimable_page_cache)
        .unwrap_or(0);
    Some(current.saturating_sub(reclaimable))
}

/// [`reclaimable_page_cache`] for a cgroup v1 `memory.stat`: the `total_*` keys are
/// the hierarchical tallies matching `memory.usage_in_bytes`, with the unprefixed
/// keys as the this-cgroup-only fallback for a kernel that omits them.
#[cfg(target_os = "linux")]
fn v1_reclaimable_page_cache(contents: &str) -> Option<u64> {
    reclaimable_page_cache(contents, "total_cache", RECLAIM_EXCLUDED_V1_TOTAL)
        .or_else(|| reclaimable_page_cache(contents, "cache", RECLAIM_EXCLUDED_V1))
}

#[cfg(target_os = "linux")]
const RECLAIM_EXCLUDED_V2: &[&str] = &["shmem", "file_dirty", "file_writeback"];
#[cfg(target_os = "linux")]
const RECLAIM_EXCLUDED_V1_TOTAL: &[&str] = &["total_shmem", "total_dirty", "total_writeback"];
#[cfg(target_os = "linux")]
const RECLAIM_EXCLUDED_V1: &[&str] = &["shmem", "dirty", "writeback"];

/// Freely-reclaimable page cache from a `memory.stat` body: the `cache_key` total
/// less the `excluded` keys the kernel cannot drop on demand. Everything the
/// charge holds beyond this — `anon`, kernel, socket — stays counted as demand.
///
/// Built from the totals, never the `*_file` LRU counters: `inactive_file` has
/// been observed exceeding the `file` total that contains it (issue #12531), so
/// the kubelet-style `current - inactive_file` working set is not trustworthy
/// here.
#[cfg(target_os = "linux")]
fn reclaimable_page_cache(contents: &str, cache_key: &str, excluded: &[&str]) -> Option<u64> {
    let cache = parse_cgroup_stat_key(contents, cache_key)?;
    Some(
        cache.saturating_sub(
            excluded
                .iter()
                .filter_map(|key| parse_cgroup_stat_key(contents, key))
                .fold(0, u64::saturating_add),
        ),
    )
}

/// Value for `key` in a cgroup stat body (`"<key> <value>"` per line).
///
/// Splits on arbitrary whitespace rather than a single space. The kernel emits one
/// space today, but a missed key is indistinguishable from an absent one here: the
/// caller reads `None` as "no reclaimable cache" and falls back to the raw charge —
/// silently restoring the over-counting this whole path exists to avoid. Being
/// lenient costs nothing and removes that failure mode.
#[cfg(target_os = "linux")]
fn parse_cgroup_stat_key(contents: &str, key: &str) -> Option<u64> {
    contents.lines().find_map(|line| {
        let mut fields = line.split_whitespace();
        (fields.next()? == key).then(|| fields.next()?.parse().ok())?
    })
}

/// Read a per-cgroup file, resolving and caching its path on first use. `v2`
/// selects the unified hierarchy; otherwise the v1 `memory` controller.
#[cfg(target_os = "linux")]
fn read_cgroup_file(
    cached_path: &OnceLock<Option<String>>,
    filename: &'static str,
    v2: bool,
) -> Option<String> {
    let path = cached_path.get_or_init(|| {
        let (mountpoint, cgroup_path) = if v2 {
            (
                cgroup2_mountpoint().unwrap_or_else(|| "/sys/fs/cgroup".to_string()),
                process_cgroup_v2_path()?,
            )
        } else {
            (
                cgroup_v1_mountpoint("memory")
                    .unwrap_or_else(|| "/sys/fs/cgroup/memory".to_string()),
                process_cgroup_v1_path("memory")?,
            )
        };
        Some(cgroup_file_path(&mountpoint, &cgroup_path, filename))
    });
    std::fs::read_to_string(path.as_deref()?).ok()
}

#[cfg(target_os = "linux")]
fn cgroup_v2_memory_current() -> Option<u64> {
    read_cgroup_u64(&CGROUP_V2_MEMORY_CURRENT_PATH, "memory.current", true)
}

#[cfg(target_os = "linux")]
fn cgroup_v1_memory_current() -> Option<u64> {
    read_cgroup_u64(&CGROUP_V1_MEMORY_USAGE_PATH, "memory.usage_in_bytes", false)
}

#[cfg(target_os = "linux")]
fn read_cgroup_u64(
    cached_path: &OnceLock<Option<String>>,
    filename: &'static str,
    v2: bool,
) -> Option<u64> {
    read_cgroup_file(cached_path, filename, v2)?
        .trim()
        .parse()
        .ok()
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

/// Sample current memory pressure (cgroup-aware `used / budget`, where `used` is
/// the unreclaimable working set — see [`current_memory_bytes`]) into `stats`,
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
/// cgroup v2 `cpu.stat` `usage_usec` (v1 `cpuacct.usage` fallback), divided by the
/// runtime's CPU *entitlement* rather than the host's core count — on a 4-core
/// entitlement misread as 18 cores a fully saturated process reports ~0.22, and the
/// controller then makes CPU-stealing moves believing CPU is idle. Needs the previous
/// sample; a too-close interval (< 0.5 s) is skipped so a double-call can't divide by
/// ~0. Called on the background tick. Linux-only; elsewhere a no-op (pressure stays
/// unknown → the CPU rule is inert). Process-global because CPU is shared across all
/// per-table loops.
#[cfg(target_os = "linux")]
pub(crate) fn sample_cpu_pressure() {
    let Some(now_usage) = cgroup_cpu_usage_usec() else {
        return;
    };
    let now = Instant::now();
    let mut prev = CPU_PREV_SAMPLE.lock();
    match *prev {
        Some((prev_usage, prev_at)) => {
            let wall_secs = now.saturating_duration_since(prev_at).as_secs_f64();
            // Skip samples < 0.5 s apart: a tiny denominator makes the ratio noisy
            // (two ticks can fire close together on a multi-table host). Keep `prev`
            // and wait for a wider window.
            if wall_secs >= 0.5 {
                let busy_secs = u64_to_f64(now_usage.saturating_sub(prev_usage)) / 1_000_000.0;
                store_cpu_pressure(
                    cpu_budget::cpu_budget().cpu_busy_fraction(busy_secs, wall_secs),
                );
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
    parse_cgroup_stat_key(&std::fs::read_to_string(path).ok()?, "usage_usec")
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
    /// Rows in this batch that mutated an existing key — the `superseded` count
    /// (rows removed/replaced by an upsert or a delete). Updates and deletes both
    /// supersede a live row and churn the keyset / fan out files the same way, so
    /// this is the "mutation" signal for the mutation-heavy gate; inserts (which
    /// supersede nothing) are the remainder. Named `delete_rows` for history.
    pub delete_rows: u64,
}

/// A tumbling-window maximum of a signal. The instantaneous `now − ts` freshness/
/// lag gauges are sampled at a random phase and ramp without bound while a table
/// is idle, so they both miss transient stalls AND mislead post-load — useless as
/// an SLO control signal. This holds the PEAK folded into the in-progress window
/// plus the last completed window's peak (so a reader always has a full-window
/// value), tumbling every [`Self::WINDOW_MS`]. Copy/cheap: three scalars, no
/// allocation; lives inside the mutex-guarded [`EwmaInner`].
#[derive(Debug, Clone, Copy, PartialEq)]
struct WindowMax {
    /// Start (epoch ms) of the in-progress window; `i64::MIN` before the first fold.
    cur_start_ms: i64,
    /// Peak folded into the in-progress window.
    cur_max: f64,
    /// Peak of the last completed window.
    prev_max: f64,
}

impl WindowMax {
    /// Peak window width, DERIVED from [`DEFAULT_GOAL_CONVERGENCE_WINDOW`] (60s) so
    /// the value and its doc can never drift if the default changes: the peak spans
    /// a full goal-convergence window — the horizon the SLO is stated over. FIXED to
    /// the DEFAULT: it does NOT track a per-dataset `cayenne_goal_convergence_window`
    /// override (that override retunes the controller's step dwell, not this
    /// observability window), so a table with a non-default convergence window still
    /// reports its freshness peak over this ~60s horizon. Making it track the
    /// configured window would mean threading a live `window_ms` through
    /// `IngestStats`; deliberately deferred as out of scope for the signal.
    #[expect(
        clippy::cast_possible_truncation,
        reason = "a minutes-scale convergence window's millis fit i64 with vast headroom"
    )]
    const WINDOW_MS: i64 = DEFAULT_GOAL_CONVERGENCE_WINDOW.as_millis() as i64;

    const fn new() -> Self {
        Self {
            cur_start_ms: i64::MIN,
            cur_max: 0.0,
            prev_max: 0.0,
        }
    }

    /// Tumble the window if [`Self::WINDOW_MS`] elapsed since it opened. Idempotent
    /// within a window, so it is safe to call from both `fold` (writer) and `peak`
    /// (reader) — whichever crosses the boundary first advances it. A gap spanning
    /// MORE than one window means the intervening windows saw no folds (idle), so
    /// only a single-window gap carries a `prev`; a longer gap resets to 0 — the
    /// idle-immunity that keeps a parked table from latching a stale peak.
    fn roll(&mut self, now_ms: i64) {
        if self.cur_start_ms == i64::MIN {
            self.cur_start_ms = now_ms;
        } else if now_ms.saturating_sub(self.cur_start_ms) >= Self::WINDOW_MS {
            let elapsed_windows = now_ms.saturating_sub(self.cur_start_ms) / Self::WINDOW_MS;
            self.prev_max = if elapsed_windows == 1 {
                self.cur_max
            } else {
                0.0
            };
            self.cur_max = 0.0;
            self.cur_start_ms = now_ms;
        }
    }

    /// Fold a non-negative `value` observed at `now_ms` into the current window.
    fn fold(&mut self, now_ms: i64, value: f64) {
        self.roll(now_ms);
        if value > self.cur_max {
            self.cur_max = value;
        }
    }

    /// The windowed peak at `now_ms` = max(in-progress, last-completed), after
    /// rolling so an idle table's peak decays instead of latching forever.
    fn peak(&mut self, now_ms: i64) -> f64 {
        self.roll(now_ms);
        self.cur_max.max(self.prev_max)
    }
}

#[derive(Debug, Clone, Copy)]
struct EwmaInner {
    rows_per_sec: Ewma,
    bytes_per_sec: Ewma,
    apply_ms: Ewma,
    arrival_gap_ms: Ewma,
    /// EWMA of `arrival_gap_ms²`, paired with `arrival_gap_ms` to derive the
    /// arrival-interval variance (and thus the burstiness CV) without storing a
    /// history: `Var = E[x²] − E[x]²`.
    arrival_gap_ms_sq: Ewma,
    /// EWMA of the per-batch mutation fraction (`superseded delete_rows / rows`),
    /// in `[0, 1]` — the mutation-heavy gate signal (updates + deletes).
    delete_fraction: Ewma,
    /// Count of recorded batches, for the controller's warmup/fresh-sample gates
    /// (NOT the EWMA seeding — each [`Ewma`] self-seeds on its first sample).
    samples: u64,
    /// EWMA per-batch object-store/disk write latency (the `vortex_write` phase),
    /// ms. Unseeded (`value() == None`) ⇒ the table has not spilled to Vortex
    /// (pure-inline) → I/O signal unavailable.
    io_latency_ms: Ewma,
    /// FAST EWMA (alpha [`EWMA_ALPHA_FAST`]) of the same `vortex_write` latency,
    /// for the cliff detector: a sudden step (burst-credit depletion) shows up
    /// here several dwells before the slow `io_latency_ms` catches up.
    io_latency_fast_ms: Ewma,
    /// EWMA per-batch metastore publish latency (the `publish` phase — the
    /// single-writer commit), ms.
    publish_latency_ms: Ewma,
    /// FAST EWMA of the `publish` latency, paired with `publish_latency_ms` for the
    /// publish-side cliff detector.
    publish_latency_fast_ms: Ewma,
    /// Tumbling-window PEAK of the per-apply end-to-end row freshness (`apply
    /// wall-clock − the applied batch's source-commit ts`) — the true worst-case
    /// PG-commit→queryable lag the freshness SLO is stated against, and the signal
    /// the freshness-goal shrink lever reads. Idle-immune (only folded on an apply
    /// that carried a source-commit ts, so a post-idle batch reports its own small
    /// lag, never the idle duration), unlike the unbounded instantaneous gauges.
    row_freshness_peak: WindowMax,
}

impl Default for EwmaInner {
    fn default() -> Self {
        Self {
            rows_per_sec: Ewma::new(),
            bytes_per_sec: Ewma::new(),
            apply_ms: Ewma::new(),
            arrival_gap_ms: Ewma::new(),
            arrival_gap_ms_sq: Ewma::new(),
            delete_fraction: Ewma::new(),
            samples: 0,
            io_latency_ms: Ewma::new(),
            io_latency_fast_ms: Ewma::with_alpha(EWMA_ALPHA_FAST),
            publish_latency_ms: Ewma::new(),
            publish_latency_fast_ms: Ewma::with_alpha(EWMA_ALPHA_FAST),
            row_freshness_peak: WindowMax::new(),
        }
    }
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

        let mut inner = self.inner.lock();
        inner.rows_per_sec.update(inst_rows_per_sec);
        inner.bytes_per_sec.update(inst_bytes_per_sec);
        inner.apply_ms.update(apply_ms);
        inner.arrival_gap_ms.update(arrival_gap_ms);
        inner
            .arrival_gap_ms_sq
            .update(arrival_gap_ms * arrival_gap_ms);
        inner.delete_fraction.update(inst_delete_fraction);
        inner.samples = inner.samples.saturating_add(1);
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

    /// Current memory pressure (`used / budget`), or `None` when unsampled
    /// (non-Linux, no budget installed). A single relaxed atomic load — the cheap
    /// path for hot loops that need only this one signal, not a full
    /// [`IngestSnapshot`].
    #[must_use]
    pub fn mem_pressure(&self) -> Option<f64> {
        milli_to_pressure(self.mem_pressure_milli.load(Ordering::Relaxed))
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
        let ms = duration_ms(d);
        inner.io_latency_ms.update(ms);
        inner.io_latency_fast_ms.update(ms);
    }

    /// Fold one CDC batch's metastore publish latency (the `publish` phase — the
    /// single-writer commit) into the rolling EWMA.
    pub fn record_publish_latency(&self, d: Duration) {
        let mut inner = self.inner.lock();
        let ms = duration_ms(d);
        inner.publish_latency_ms.update(ms);
        inner.publish_latency_fast_ms.update(ms);
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

    /// Fold this apply's end-to-end row freshness — `now_ms − source_commit_ts_ms`,
    /// the age at apply time of the batch we just made queryable — into the rolling
    /// windowed peak. This is the true PG-commit→queryable lag per batch; its peak
    /// over the window is the worst-case freshness the SLO is stated against, and
    /// what the freshness-goal shrink lever controls on. A no-op when the source
    /// carries no commit ts (nothing to measure). Idle-immune BY CONSTRUCTION: a
    /// batch that arrives after an idle gap carries a RECENT `source_commit_ts_ms`,
    /// so its measured lag is small — the wall-clock idle never enters the signal
    /// (unlike `freshness_secs`/`replication_lag_secs`, which both ramp on idle).
    /// Negative (source clock ahead of host) clamps to 0; the absolute value is only
    /// as good as source↔host clock sync, so the controller keys off the threshold.
    pub fn fold_row_freshness(&self, now_ms: i64, source_commit_ts_ms: Option<i64>) {
        let Some(ts) = source_commit_ts_ms else {
            return;
        };
        let lag_secs = u64_to_f64(now_ms.saturating_sub(ts).max(0).unsigned_abs()) / 1000.0;
        self.inner.lock().row_freshness_peak.fold(now_ms, lag_secs);
    }

    /// The windowed-peak per-apply row freshness in seconds (worst PG-commit→
    /// queryable lag over the rolling window), or `None` before the first apply that
    /// carried a source-commit ts. The freshness-goal control/SLO signal — robust to
    /// the instantaneous gauge's sampling-phase blindness and idle ramp.
    #[must_use]
    pub fn peak_row_freshness_secs(&self, now_ms: i64) -> Option<f64> {
        let mut inner = self.inner.lock();
        // `cur_start_ms == i64::MIN` ⇒ never folded (no apply yet with a commit ts).
        if inner.row_freshness_peak.cur_start_ms == i64::MIN {
            return None;
        }
        Some(inner.row_freshness_peak.peak(now_ms))
    }

    /// Take a consistent snapshot of the derived signals for the controller.
    #[must_use]
    pub fn snapshot(&self) -> IngestSnapshot {
        let inner = *self.inner.lock();
        // Resolve each EWMA to its current value; an unseeded average reads as the
        // cold-start `0.0` the prior bare-f64 slots defaulted to.
        let rows_per_sec = inner.rows_per_sec.value().unwrap_or(0.0);
        let bytes_per_sec = inner.bytes_per_sec.value().unwrap_or(0.0);
        let apply_ms = inner.apply_ms.value().unwrap_or(0.0);
        let arrival_gap_ms = inner.arrival_gap_ms.value().unwrap_or(0.0);
        let delete_fraction = inner.delete_fraction.value().unwrap_or(0.0);
        // Key response signal: apply latency relative to the offered-load
        // interval. > 1 means we absorb a batch slower than batches arrive — the
        // table is falling behind regardless of the absolute rate.
        let apply_vs_arrival = if arrival_gap_ms > 0.0 {
            apply_ms / arrival_gap_ms
        } else {
            0.0
        };
        let mem_pressure = milli_to_pressure(self.mem_pressure_milli.load(Ordering::Relaxed));
        // Burstiness as the coefficient of variation of the inter-batch interval:
        // σ/μ = sqrt(E[x²] − E[x]²) / E[x]. CV ≈ 0 is a metronome-steady stream;
        // CV > 1 means the gap's spread exceeds its mean (spiky). `0` until the
        // mean is positive (cold start) so it can't fire a spurious "bursty".
        let arrival_cv = if arrival_gap_ms > 0.0 {
            let arrival_gap_ms_sq = inner.arrival_gap_ms_sq.value().unwrap_or(0.0);
            let variance = (arrival_gap_ms_sq - arrival_gap_ms * arrival_gap_ms).max(0.0);
            variance.sqrt() / arrival_gap_ms
        } else {
            0.0
        };
        IngestSnapshot {
            rows_per_sec,
            // Real bytes/sec once the first write has been recorded; -1.0 before
            // then (cold start) so the gauge is suppressed rather than emitting 0.
            bytes_per_sec: if self.total_bytes.load(Ordering::Relaxed) > 0 {
                bytes_per_sec
            } else {
                -1.0
            },
            apply_ms,
            arrival_gap_ms,
            apply_vs_arrival,
            read_amp: self.read_amp.load(Ordering::Relaxed),
            mem_pressure,
            delete_fraction,
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
            cpu_burstable: cpu_burstable(),
            io_latency_ms: inner.io_latency_ms.value(),
            publish_latency_ms: inner.publish_latency_ms.value(),
            io_latency_fast_ms: inner.io_latency_fast_ms.value(),
            publish_latency_fast_ms: inner.publish_latency_fast_ms.value(),
            data_storage: StorageClass::default(),
            metastore_storage: StorageClass::default(),
            // Filled by `CayenneContext::ingest_snapshot` from the per-table config
            // (the measured calibration-probe throughput), like the storage classes.
            data_write_mbps: None,
            metastore_write_mbps: None,
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
    /// Windowed-PEAK per-apply row freshness in seconds — the worst-case
    /// PG-commit→queryable lag (`apply_wall_clock − batch_source_commit_ts`) over the
    /// rolling goal-convergence window ([`WindowMax::WINDOW_MS`], derived from
    /// [`DEFAULT_GOAL_CONVERGENCE_WINDOW`]), populated from
    /// [`IngestStats::peak_row_freshness_secs`]. NOT the instantaneous
    /// `now − last_visible` age: the peak captures transient stalls and is idle-immune,
    /// so it is the freshness-goal control/SLO signal. Falls back to that instantaneous
    /// age on sources without a commit timestamp (or before the first timestamped
    /// apply); `None` only before the first apply of any kind. Lower is better.
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
    /// Host is a T-family burstable EC2 instance (CPU credits deplete under
    /// sustained load). The CPU growth gate tightens to [`CPU_PRESSURE_OK_BURSTABLE`].
    /// Process-global (a host fact).
    pub cpu_burstable: bool,
    /// EWMA per-batch object-store/disk write latency in ms, or `None` until the
    /// table spills to Vortex. High vs the arrival interval ⇒ bias to fewer/larger files.
    pub io_latency_ms: Option<f64>,
    /// EWMA per-batch metastore publish latency in ms, or `None` before the first
    /// publish. High vs the arrival interval ⇒ bias to larger inline-flush (amortize commits).
    pub publish_latency_ms: Option<f64>,
    /// FAST EWMA of the per-batch write latency ([`EWMA_ALPHA_FAST`]), or `None`
    /// until the table spills. Paired with [`Self::io_latency_ms`] for the cliff
    /// detector: `fast ≫ slow` is a burst-credit/pipe-saturation step change.
    pub io_latency_fast_ms: Option<f64>,
    /// FAST EWMA of the per-batch publish latency, for the publish-side cliff.
    pub publish_latency_fast_ms: Option<f64>,
    /// Storage medium of the table's data files. Drives the *continuous* slow-tier
    /// I/O-bound bias ([`tier_scale`]) together with [`Self::data_write_mbps`], the
    /// `cliff` fast path, and the slow-tier earlier-drain gate; also surfaced for
    /// telemetry.
    pub data_storage: StorageClass,
    /// Storage medium of the metastore. Drives the continuous publish-bound bias
    /// with [`Self::metastore_write_mbps`]; surfaced for telemetry.
    pub metastore_storage: StorageClass,
    /// Measured data-volume write throughput (MiB/s) from the calibration probe, or
    /// `None` when unprobed. Refines [`Self::data_storage`] into a continuous bias —
    /// the only storage signal for a memory-tier table that never spills (and so
    /// produces no `io_latency_ms`).
    pub data_write_mbps: Option<f64>,
    /// Measured metastore-volume write throughput (MiB/s), refining the publish bias.
    pub metastore_write_mbps: Option<f64>,
}

impl IngestSnapshot {
    /// CPU has headroom to add CPU-stealing work (write shards, more compaction).
    /// The gate tightens to [`CPU_PRESSURE_OK_BURSTABLE`] on a T-family burstable
    /// instance, where credits deplete under sustained load. Unknown pressure ⇒
    /// `true` (the CPU rule is inert). Single source of truth for the gate, shared
    /// by both decide ladders and [`binding_constraint`].
    fn cpu_ok(&self) -> bool {
        let gate = if self.cpu_burstable {
            CPU_PRESSURE_OK_BURSTABLE
        } else {
            CPU_PRESSURE_OK
        };
        self.cpu_pressure.is_none_or(|p| p < gate)
    }

    /// The data volume is on a slow/networked tier — the continuous,
    /// measurement-aware predicate ([`tier_scale`] `< 1.0`), so a fast-measured io2
    /// volume is correctly treated as fast even though its class is `Ebs`.
    fn data_tier_is_slow(&self) -> bool {
        tier_scale(self.data_storage, self.data_write_mbps) < 1.0
    }

    /// The metastore volume is on a slow/networked tier (see [`Self::data_tier_is_slow`]).
    fn metastore_tier_is_slow(&self) -> bool {
        tier_scale(self.metastore_storage, self.metastore_write_mbps) < 1.0
    }

    /// A *confirmed* slow tier, for the earlier-drain decision: a measured-slow
    /// volume, or a known networked class (`Ebs`). The `Unknown` default (no
    /// detection, no measurement) is deliberately excluded so undetected storage
    /// keeps the standard memory thresholds — earlier draining only kicks in with
    /// positive evidence the write path is slow, never as a blanket default change.
    fn confirmed_slow_tier(&self) -> bool {
        let confirmed = |class: StorageClass, mbps: Option<f64>| {
            if mbps.is_some() {
                tier_scale(class, mbps) < 1.0
            } else {
                matches!(class, StorageClass::Ebs)
            }
        };
        confirmed(self.data_storage, self.data_write_mbps)
            || confirmed(self.metastore_storage, self.metastore_write_mbps)
    }
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
    /// Deletion-index size (live PK tombstone count) at or above which the
    /// seq-prefix bake fires. The merge-on-read read-amp lever: LOWERED when a
    /// query goal is unmet and the index is large (bake more often → smaller
    /// index → cheaper probe), RAISED under write pressure (bake less often →
    /// bound the bake's write amplification).
    bake_deletion_index_trigger: AtomicUsize,
    /// 0 means "unset" (use the session/default write concurrency).
    write_concurrency: AtomicUsize,
    /// Per-table in-memory CDC durability tier byte cap (`cdc_durability: memory`).
    /// `<= 0` means "no per-table cap" (the process-global mem-tier budget still
    /// bounds aggregate RAM). The synchronous-spill freshness lever: a larger cap
    /// means fewer writer-blocking spills, so the controller grows it under
    /// backpressure (when memory allows) and shrinks it under memory pressure.
    mem_tier_max_bytes: AtomicI64,
    /// Adaptive target Vortex file size (bytes). The query/scan read-amp lever:
    /// GROWN when a query goal is unmet (bigger files ⇒ fewer files + better
    /// per-file stats and compression for scans, less fan-out to probe), bounded
    /// by the static config (storage-tier-aware). `<= 0` keeps size-rolling off.
    target_vortex_file_size_bytes: AtomicI64,
    /// Query-admission permits to reserve for CDC apply (the CPU-fairness lever).
    /// Reported each background tick to the process-global query-admission
    /// governor, which holds that many permits on the shared admission semaphore so
    /// that many fewer analytical queries run concurrently — handing CPU back to
    /// the apply when it is behind under contention. `0` (the default) reserves
    /// nothing, so the lever is inert unless a lag/freshness goal drives it up.
    query_admission_reserve: AtomicUsize,
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
            bake_deletion_index_trigger: AtomicUsize::new(init.bake_deletion_index_trigger),
            write_concurrency: AtomicUsize::new(init.write_concurrency),
            mem_tier_max_bytes: AtomicI64::new(init.mem_tier_max_bytes),
            target_vortex_file_size_bytes: AtomicI64::new(init.target_vortex_file_size_bytes),
            query_admission_reserve: AtomicUsize::new(init.query_admission_reserve),
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
            bake_deletion_index_trigger: self.bake_deletion_index_trigger.load(Ordering::Relaxed),
            write_concurrency: self.write_concurrency.load(Ordering::Relaxed),
            mem_tier_max_bytes: self.mem_tier_max_bytes.load(Ordering::Relaxed),
            target_vortex_file_size_bytes: self
                .target_vortex_file_size_bytes
                .load(Ordering::Relaxed),
            query_admission_reserve: self.query_admission_reserve.load(Ordering::Relaxed),
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
    pub fn bake_deletion_index_trigger(&self) -> usize {
        self.bake_deletion_index_trigger.load(Ordering::Relaxed)
    }
    pub fn write_concurrency(&self) -> usize {
        self.write_concurrency.load(Ordering::Relaxed)
    }
    pub fn mem_tier_max_bytes(&self) -> i64 {
        self.mem_tier_max_bytes.load(Ordering::Relaxed)
    }
    pub fn target_vortex_file_size_bytes(&self) -> i64 {
        self.target_vortex_file_size_bytes.load(Ordering::Relaxed)
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
            Actuator::TargetVortexFileSize => {
                let bytes = i64::try_from(adj.new_value).unwrap_or(i64::MAX);
                self.target_vortex_file_size_bytes
                    .store(bytes, Ordering::Relaxed);
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
            Actuator::BakeDeletionIndexTrigger => {
                self.bake_deletion_index_trigger.store(
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
            Actuator::QueryAdmissionReserve => {
                self.query_admission_reserve.store(
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
    pub bake_deletion_index_trigger: usize,
    pub write_concurrency: usize,
    pub mem_tier_max_bytes: i64,
    pub target_vortex_file_size_bytes: i64,
    pub query_admission_reserve: usize,
}

/// Static `[floor, ceiling]` per dynamically-tuned actuator, derived by the static
/// tier. The controller can never move an actuator outside these, so dynamic
/// tuning is bounded by — and can only improve on — the static config.
#[derive(Debug, Clone, Copy)]
pub(crate) struct TuningBounds {
    pub inline_flush_max_bytes: (i64, i64),
    pub compaction_background_interval_ms: (u64, u64),
    pub compaction_trigger_files: (usize, usize),
    pub bake_deletion_index_trigger: (usize, usize),
    pub write_concurrency: (usize, usize),
    pub mem_tier_max_bytes: (i64, i64),
    pub target_vortex_file_size_bytes: (i64, i64),
    pub query_admission_reserve: (usize, usize),
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
    BakeDeletionIndexTrigger,
    WriteConcurrency,
    TargetVortexFileSize,
    /// Number of query-admission permits to RESERVE for CDC apply (shed that many
    /// concurrent analytical queries). The CPU-fairness lever: GROWN when a
    /// lag/freshness goal is unmet AND CPU is the contended resource (queries are
    /// starving the apply); RELEASED as soon as CPU frees or the lag goal is met.
    /// Reported to the process-global [`super::query_admission`] governor, which
    /// holds that many permits on the shared admission semaphore.
    QueryAdmissionReserve,
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
            Self::BakeDeletionIndexTrigger => "bake_deletion_index_trigger",
            Self::WriteConcurrency => "write_concurrency",
            Self::TargetVortexFileSize => "target_vortex_file_size_bytes",
            Self::QueryAdmissionReserve => "query_admission_reserve",
        }
    }

    /// Whether RAISING this actuator increases resident bytes. Every such raise
    /// must be gated on `mem_ok` — growing memory while the memory rule is busy
    /// shrinking it puts two rules in opposition, and the memory rule is the one
    /// holding the hard objective.
    ///
    /// An exhaustive match rather than a per-rule convention, so a new actuator
    /// cannot be added without answering the question, and
    /// `memory_consuming_actuators_are_never_raised_under_pressure` sweeps the
    /// decider against it. Test-only: it classifies the actuator set for that
    /// sweep, and the decider's own gating lives in the rules themselves.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn consumes_memory(self) -> bool {
        match self {
            // Live buffers, in-flight encode shards, and the compaction output
            // buffer: each raise is more resident bytes.
            Self::InlineFlushBytes
            | Self::MemTierMaxBytes
            | Self::WriteConcurrency
            | Self::TargetVortexFileSize => true,
            // These spend CPU or I/O, not bytes; they carry `cpu_ok` where the
            // resource they contend for warrants it.
            Self::CompactionIntervalMs
            | Self::CompactionTriggerFiles
            | Self::BakeDeletionIndexTrigger
            | Self::QueryAdmissionReserve => false,
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
    /// Millis since `anchor` of the most recent recorded query — gates [`Self::qph`]
    /// so an idle table reports "no QPH signal" instead of a decaying lifetime rate.
    last_query_millis: AtomicU64,
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
            last_query_millis: AtomicU64::new(0),
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
        // Stamp arrival (millis since anchor) so `qph` can distinguish a
        // currently-loaded table from one that has gone idle.
        self.last_query_millis.store(
            u64::try_from(self.anchor.elapsed().as_millis()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
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

    /// Queries-per-hour, or `None` when there is no current query signal: no queries
    /// yet, or the table has gone idle (no query within [`QPH_IDLE_MS`]). Without the
    /// idle gate a lifetime average decays toward 0 while parked, which would keep a
    /// QPH goal perpetually "violated" and drive tuning that cannot improve QPH
    /// without new queries. While active it is the lifetime rate — the simplest
    /// correct cut; it under-reacts to recent spikes (a true windowed rate is a
    /// future refinement).
    #[must_use]
    pub fn qph(&self) -> Option<f64> {
        let total = self.total_queries.load(Ordering::Relaxed);
        if total == 0 {
            return None;
        }
        let now_millis = u64::try_from(self.anchor.elapsed().as_millis()).unwrap_or(u64::MAX);
        let idle_millis = now_millis.saturating_sub(self.last_query_millis.load(Ordering::Relaxed));
        if idle_millis > QPH_IDLE_MS {
            return None;
        }
        let hours = self.anchor.elapsed().as_secs_f64() / 3600.0;
        if hours <= f64::EPSILON {
            return None;
        }
        Some(u64_to_f64(total) / hours)
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
/// for every dataset a query touched; a no-op for unregistered tables. Returns
/// `true` iff the table was Cayenne-registered, so the caller can decide whether
/// the query touched Cayenne at all (and thus counts toward global QPH).
pub fn record_query_latency(name: &str, latency_ms: f64) -> bool {
    let map = QUERY_OBSERVATIONS.read();
    // Fast path: the runtime pushes the already-bare table name
    // (`TableReference::table()`), so a borrowed lookup hits with no allocation or
    // parse — this runs per dataset per query.
    if let Some(obs) = map.get(name) {
        obs.record_query(latency_ms);
        true
    } else if let Some(obs) = map.get(table_registry_key(name).as_str()) {
        // Fallback for a schema-qualified name (rare; never on the runtime hot
        // path) — normalize to the bare key registration used.
        obs.record_query(latency_ms);
        true
    } else {
        false
    }
}

/// Drop a table's query observations on teardown. [`register_query_observations`] is
/// idempotent and reuses any existing handle, so observations live for the process
/// lifetime unless a caller explicitly deregisters here — a table that is dropped and
/// recreated WITHOUT an intervening deregister intentionally inherits its prior
/// histogram/QPH baseline. Call this only on genuine teardown, to reset that baseline
/// and avoid leaking handles.
pub fn deregister_query_observations(name: &str) {
    QUERY_OBSERVATIONS.write().remove(&table_registry_key(name));
}

/// Process-global query observations aggregating EVERY Cayenne-touching query
/// exactly once (regardless of how many datasets it touched), so QPH — a
/// SYSTEM-WIDE metric — is measured globally rather than summed across per-table
/// handles (which would multiply-count a join across its participants). The
/// per-table handles in [`QUERY_OBSERVATIONS`] still serve per-dataset p99 latency.
static GLOBAL_QUERY_OBSERVATIONS: LazyLock<QueryObservations> =
    LazyLock::new(QueryObservations::new);

/// Record one finished query against the global QPH aggregate. The runtime calls
/// this ONCE per query (not once per touched dataset), only when the query touched
/// at least one Cayenne table. Latency is folded into the global histogram too,
/// but only the global QPH is consumed today.
pub fn record_global_query(latency_ms: f64) {
    GLOBAL_QUERY_OBSERVATIONS.record_query(latency_ms);
}

/// System-wide queries-per-hour, or `None` when there is no current query signal
/// (no queries yet, or the system has gone idle). Every per-dataset controller's
/// QPH goal reads THIS — never a per-table rate — because a query spanning N
/// datasets is one unit of system throughput, not N.
#[must_use]
pub fn global_qph() -> Option<f64> {
    GLOBAL_QUERY_OBSERVATIONS.qph()
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

    /// The control dwell to enforce given the legacy `min_dwell`: the
    /// window-derived goal dwell when any goal is active, else `min_dwell`. Lets the
    /// caller compute "is this control tick eligible to move?" the same way
    /// [`decide_with_goals`] does, for the infeasibility tracker.
    pub(crate) fn control_dwell(self, min_dwell: Duration) -> Duration {
        if self.any_active() {
            self.dwell()
        } else {
            min_dwell
        }
    }

    /// True when an active goal's measured value is past target. Lag/freshness count
    /// only when `ingest_fresh` (they grow with the wall clock on an idle table, so
    /// a parked table isn't "failing" its SLO). Drives the infeasible-SLO tracker.
    pub(crate) fn any_actionable_violation(self, s: &IngestSnapshot, ingest_fresh: bool) -> bool {
        self.query_violation(s) > 0.0 || (ingest_fresh && self.ingest_violation(s) > 0.0)
    }

    /// Max violation among the lag/freshness goals — drives the ingest tier's step
    /// size; identifies the driving goal for attribution.
    fn ingest_violation(self, s: &IngestSnapshot) -> f64 {
        let lag = self
            .replication_lag
            .map_or(0.0, |g| g.violation(s.replication_lag_secs));
        let fresh = self
            .freshness
            .map_or(0.0, |g| g.violation(s.freshness_secs));
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

    /// Are the ingest-side goals (replication-lag + freshness) comfortably met?
    /// One of the RELEASE triggers for the query-admission reserve — the reserve's
    /// justification (CDC behind) is gone, so hand the query slots back.
    ///
    /// The reserve's release must NEVER be gated on a query goal being MET: the
    /// reserve suppresses queries, so a query goal (QPH/latency) could stay below
    /// "met" *because of the throttle itself*, and the reserve would never release
    /// (self-perpetuation). Releasing on a query goal being VIOLATED is the OPPOSITE
    /// direction and is safe — see [`Self::query_comfortably_met`] and the release
    /// tier — because throttling moves a query goal TOWARD violation, so
    /// "release on violated" is a stable negative-feedback brake, not a latch.
    fn ingest_comfortably_met(self, s: &IngestSnapshot) -> bool {
        self.replication_lag
            .is_none_or(|g| g.comfortably_met(s.replication_lag_secs))
            && self
                .freshness
                .is_none_or(|g| g.comfortably_met(s.freshness_secs))
    }

    /// Are the query-side goals (query-latency-p99 + QPH) comfortably met? The
    /// HEADROOM gate for GROWING the query-admission reserve. That lever sheds
    /// analytical queries to hand cores to the CDC apply, trading query throughput
    /// for ingest freshness — so it should only spend query capacity it demonstrably
    /// HAS: grow the reserve while the query SLOs sit comfortably above target, and
    /// stop once throttling has consumed that headroom. This makes the query SLOs
    /// the reserve's BUDGET — the apply may borrow query cores down to (but not
    /// through) the QPH/latency targets. `None` (unset) query goals read as
    /// comfortably met, so with no query SLO configured the reserve grows purely on
    /// the ingest+CPU signals (the prior behavior). Safe as a GROW gate — it only
    /// makes throttling LESS aggressive, never a latch: the self-perpetuation trap
    /// on [`Self::ingest_comfortably_met`] is specific to gating RELEASE on a query
    /// goal being met; this gates GROW, the opposite move.
    fn query_comfortably_met(self, s: &IngestSnapshot) -> bool {
        self.query_latency_p99
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

    // Slow-tier earlier drain: on EBS/object-store media (any tier the continuous
    // bias does not rate as fast) the controller starts shrinking the live memory
    // buffers EARLIER — at thresholds [`SLOW_TIER_MEM_DRAIN_OFFSET`] below the
    // defaults — so it drains in small increments instead of hitting the big
    // synchronous critical drain, whose large write burst would compete with ingest
    // for the same slow pipe. A fast-measured volume (scale 1.0) keeps the defaults.
    // Continuous slow-tier scales, computed once and reused for the I/O/publish
    // bound bars below (a fast-measured volume scales to 1.0 — no bias).
    let data_scale = tier_scale(s.data_storage, s.data_write_mbps);
    let meta_scale = tier_scale(s.metastore_storage, s.metastore_write_mbps);
    // Earlier-drain applies only to a CONFIRMED slow tier (known EBS or a
    // measured-slow volume), NOT the `Unknown` default — undetected storage keeps
    // the standard memory thresholds, so this never changes behavior blanket.
    let drain_offset = if s.confirmed_slow_tier() {
        SLOW_TIER_MEM_DRAIN_OFFSET
    } else {
        0.0
    };
    let mem_high = s
        .mem_pressure
        .is_some_and(|p| p > MEM_PRESSURE_HIGH - drain_offset);
    let mem_ok = s
        .mem_pressure
        .is_none_or(|p| p < MEM_PRESSURE_OK - drain_offset);
    // CRITICAL pressure: approaching the host ceiling. The shrink below collapses
    // the live caps straight to their floors (not one ×2/3 step) so they stop
    // admitting growth at once; the impure checkpoint tick pairs this with a
    // forced mem-tier drain to release resident RAM. A host-ceiling backstop, so it
    // is NOT shifted by the slow-tier offset (the earlier `mem_high` drain is what
    // keeps a slow tier from reaching it).
    let mem_critical = s.mem_pressure.is_some_and(|p| p >= MEM_PRESSURE_CRITICAL);
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
    let mutation_heavy = s.delete_fraction > MUTATION_HEAVY_FRACTION;
    let bursty = ingest_fresh && s.arrival_cv > BURSTY_ARRIVAL_CV;
    // Environment gates. CPU-bound withholds CPU-stealing moves (add write shards,
    // compact more); I/O-/publish-bound (per-batch latency eating the offered-load
    // window, gated on fresh ingest) biases toward fewer/larger files + amortized
    // commits. Each collapses to legacy behavior when its signal is unavailable
    // (`cpu_ok` true, `io_bound`/`publish_bound` false).
    let cpu_ok = s.cpu_ok();
    let io_bound = ingest_fresh
        && latency_bound(
            s.io_latency_ms,
            s.arrival_gap_ms,
            IO_BOUND_FRACTION * data_scale,
        );
    let publish_bound = ingest_fresh
        && latency_bound(
            s.publish_latency_ms,
            s.arrival_gap_ms,
            PUBLISH_BOUND_FRACTION * meta_scale,
        );
    // I/O cliff: the FAST latency EWMA has jumped well above the slow one — a
    // step-change (EBS burst-credit depletion, the instance EBS pipe saturating),
    // not noise. Gated on fresh ingest like the other write-derived signals.
    let io_cliff = ingest_fresh && is_cliff(s.io_latency_fast_ms, s.io_latency_ms);
    let publish_cliff = ingest_fresh && is_cliff(s.publish_latency_fast_ms, s.publish_latency_ms);

    // (1) Memory pressure [hard, highest priority]: the cgroup-aware budget is
    // nearly exhausted. Shrink the two live memory buffers — the inline memtable
    // first, then the in-memory CDC durability tier — toward their floors, one per
    // tick. Running first means no growth rule below can enlarge memory on an
    // already-tight box; query read-amp is instead relieved by compaction (which
    // costs CPU, not memory).
    if mem_high {
        // Under CRITICAL pressure jump straight to the floor in one move; otherwise
        // take a single ×2/3 step and re-evaluate next dwell. Same shape for the
        // inline memtable and the mem-tier cap (`reasons` = (critical, normal)).
        let shrink = |cur_v: i64,
                      bounds: (i64, i64),
                      actuator: Actuator,
                      reasons: (&'static str, &'static str)| {
            let target = if mem_critical {
                bounds.0
            } else {
                shrink_i64(cur_v)
            };
            clamp_move_i64(cur_v, target, bounds).map(|v| Adjustment {
                actuator,
                new_value: u64::try_from(v).unwrap_or(0),
                reason: if mem_critical { reasons.0 } else { reasons.1 },
            })
        };
        if let Some(adj) = shrink(
            cur.inline_flush_max_bytes,
            b.inline_flush_max_bytes,
            Actuator::InlineFlushBytes,
            (
                "critical memory pressure: collapse memtable to floor",
                "memory pressure: shrink memtable to stay within the cgroup budget",
            ),
        ) {
            return Some(adj);
        }
        if let Some(adj) = shrink(
            cur.mem_tier_max_bytes,
            b.mem_tier_max_bytes,
            Actuator::MemTierMaxBytes,
            (
                "critical memory pressure: collapse the in-memory CDC tier cap to floor",
                "memory pressure: shrink the in-memory CDC tier cap to free RAM",
            ),
        ) {
            return Some(adj);
        }
    }

    // (1b) I/O cliff fast path [below memory, above both ladders]: a sudden
    // multiplicative jump in per-batch write/publish latency vs its slow EWMA — an
    // EBS burst-credit cliff, or the shared instance EBS pipe saturating. The
    // additive/±50% ladders would crawl the write levers down over many dwells
    // while replication lag balloons; instead make ONE decisive move, the I/O
    // analogue of the critical-memory fast path. First shed write shards
    // (concurrent uploads only fragment a bandwidth-saturated pipe into more small
    // files — no throughput gained), then, once shards bottom out, amortize via a
    // larger memtable (fewer, larger writes/commits). Applies in BOTH legacy and
    // goal modes so an SLO-driven table can't keep adding shards into a cliff;
    // memory pressure (above) still wins. As the slow EWMA catches up the ratio
    // falls back under the trigger and the cliff clears, so this self-limits.
    if io_cliff || publish_cliff {
        let write_concurrency = cur.write_concurrency.max(1);
        if let Some(v) = clamp_move_usize(
            write_concurrency,
            shrink_usize(write_concurrency),
            b.write_concurrency,
        ) {
            return Some(Adjustment {
                actuator: Actuator::WriteConcurrency,
                new_value: u64::try_from(v).unwrap_or(0),
                reason: "io/publish cliff: shed write shards (parallel uploads only fragment a saturated pipe)",
            });
        }
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
                reason: "io/publish cliff: enlarge memtable to amortize writes onto the saturated pipe",
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
            // Lower the seq-prefix bake trigger so the bake fires sooner: a
            // smaller live deletion index means a cheaper merge-on-read probe —
            // the read-amp lever for the per-query tombstone scan. Multiplicative
            // step (the trigger's range spans 1e3..5e6), CPU-gated like the
            // compaction levers above (the bake runs on the compaction task).
            if cpu_ok
                && let Some(v) = clamp_move_usize(
                    cur.bake_deletion_index_trigger,
                    shrink_usize(cur.bake_deletion_index_trigger),
                    b.bake_deletion_index_trigger,
                )
            {
                return Some(Adjustment {
                    actuator: Actuator::BakeDeletionIndexTrigger,
                    new_value: u64::try_from(v).unwrap_or(0),
                    reason: "high read-amp: lower the bake trigger → smaller deletion index → cheaper merge-on-read probe",
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

        // I/O-/publish-bound but read-amp is NOT high (no query payoff from baking
        // sooner): RAISE the bake trigger so the bake's write-amplification stops
        // competing for the saturated write path. Withheld when read-amp is high
        // (the read-amp arm above already lowered it for query health — never fight
        // that). Multiplicative step over the wide 1e3..5e6 range.
        if (io_bound || publish_bound)
            && !read_amp_high
            && let Some(v) = clamp_move_usize(
                cur.bake_deletion_index_trigger,
                grow_usize(cur.bake_deletion_index_trigger),
                b.bake_deletion_index_trigger,
            )
        {
            return Some(Adjustment {
                actuator: Actuator::BakeDeletionIndexTrigger,
                new_value: u64::try_from(v).unwrap_or(0),
                reason: "io/publish-bound: raise the bake trigger → bake less often → bound bake write-amp",
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
            // 3. Buffers maxed and queries are NOT read-amp-bound → add encode
            //    parallelism. Gated on low read-amp because more shards mean more
            //    files; ALSO withheld when the stream is delete-heavy, where extra
            //    shards multiply the per-burst small-file fan-out and worsen delete
            //    routing off the in-memory tier.
            //    Also withheld when CPU-bound (more shards steal query threads),
            //    I/O-/publish-bound (more shards = more files, uploads, and
            //    metastore commits — the slow-storage/EBS bias), and under memory
            //    pressure: every extra shard is another in-flight encode buffer and
            //    inline memtable, so this lever buys lag relief with resident bytes
            //    — near the budget that trade is an OOM kill, not a recovery
            //    (measured at SF-1000 under a 96 GiB cgroup cap: the tuner raised
            //    shards at 0.98 pressure and the kernel ended the process).
            if s.read_amp <= READ_AMP_LOW
                && mem_ok
                && !mutation_heavy
                && cpu_ok
                && !io_bound
                && !publish_bound
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
    // Freshness split out from the combined ingest violation: freshness owns the
    // mem-tier SHRINK lever (a violated freshness SLO ⇒ apply-visibility lag ⇒
    // checkpoint smaller epochs sooner), while replication-lag keeps the buffer
    // GROW levers. Kept mutually exclusive on the mem-tier actuator (the shrink
    // tier fires first and the buffer-grow branches are gated `!freshness_violated`)
    // so the two never target it in opposite directions on one tick — no limit
    // cycle. Gated on `ingest_fresh` like every ingest signal: `s.freshness_secs`
    // carries the windowed-PEAK per-apply row freshness (idle-immune by
    // construction — see `IngestStats::fold_row_freshness`), but the fresh-sample
    // gate is still required so a parked table with a decaying peak never ratchets
    // the tier down.
    let fresh_v = goals
        .freshness
        .map_or(0.0, |g| g.violation(s.freshness_secs));
    let freshness_violated = ingest_fresh && fresh_v > 0.0;
    // Environment/data gates (same semantics as the legacy ladder): CPU-bound
    // withholds CPU-stealing moves; I/O-/publish-bound and mutation-heavy withhold the
    // write-concurrency lever (more shards = more files / uploads / key churn). On a
    // slow storage tier the I/O-/publish-bound bar is halved (see `tier_bound_fraction`),
    // so EBS/object-store tables stop adding shards and lean on buffer growth +
    // compaction sooner — the closed-loop half of the storage-tier awareness.
    let cpu_ok = s.cpu_ok();
    let io_bound = ingest_fresh
        && latency_bound(
            s.io_latency_ms,
            s.arrival_gap_ms,
            tier_bound_fraction(IO_BOUND_FRACTION, s.data_storage, s.data_write_mbps),
        );
    let publish_bound = ingest_fresh
        && latency_bound(
            s.publish_latency_ms,
            s.arrival_gap_ms,
            tier_bound_fraction(
                PUBLISH_BOUND_FRACTION,
                s.metastore_storage,
                s.metastore_write_mbps,
            ),
        );
    let mutation_heavy = s.delete_fraction > MUTATION_HEAVY_FRACTION;

    // (1b) Release the query-admission reserve as soon as its justification is gone
    // OR it has overshot a query SLO. Three triggers, all safe/stable:
    //   - CPU no longer contended (`cpu_ok`) — shedding queries can't help the apply
    //     if CPU isn't the bottleneck, so nothing to relieve;
    //   - the ingest goal is comfortably met (`ingest_comfortably_met`) — the apply
    //     caught up, the reserve's whole reason is gone;
    //   - a query SLO (QPH or query-latency) is now VIOLATED (`query_violated`) — the
    //     throttle has borrowed too much query capacity and pushed a query goal past
    //     target, so back off. This is the QUERY-SLO BRAKE: throttling moves QPH/
    //     latency toward violation, so releasing ON violation is stable negative
    //     feedback (never a latch). It is NOT the self-perpetuation trap documented
    //     on `ingest_comfortably_met` — that prohibits releasing on a query goal
    //     being MET; braking on VIOLATED is the opposite, safe direction.
    // Checked BEFORE the query and ingest tiers — handing query slots back (and
    // honoring the query SLOs) is high priority. Fast handback (legacy ±⅓ step);
    // the bound floor is 0.
    if cur.query_admission_reserve > 0
        && (cpu_ok || goals.ingest_comfortably_met(s) || query_violated)
        && let Some(v) = clamp_move_usize(
            cur.query_admission_reserve,
            shrink_usize(cur.query_admission_reserve),
            b.query_admission_reserve,
        )
    {
        // Attribute the release: a query-SLO overshoot is the interesting case (the
        // throttle traded away too much), distinct from the reserve simply no longer
        // being needed.
        let reason = if query_violated {
            "query SLO (QPH/latency) at target — stop borrowing query capacity: release a reserved query-admission slot"
        } else {
            "CPU uncontended or ingest goal met: release a reserved query-admission slot"
        };
        return Some(Adjustment {
            actuator: Actuator::QueryAdmissionReserve,
            new_value: u64::try_from(v).unwrap_or(0),
            reason,
        });
    }

    // (2) Query-health tier: a violated latency/QPH goal. Larger/fewer files and
    // more compaction help queries; shedding write shards cuts file fan-out.
    if query_violated {
        if mem_ok
            && let Some(v) = clamp_move_i64(
                cur.inline_flush_max_bytes,
                goal_grow_i64(
                    cur.inline_flush_max_bytes,
                    b.inline_flush_max_bytes,
                    query_v,
                ),
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
            goal_shrink_usize(
                cur.compaction_trigger_files,
                b.compaction_trigger_files,
                query_v,
            ),
            b.compaction_trigger_files,
        ) {
            return Some(Adjustment {
                actuator: Actuator::CompactionTriggerFiles,
                new_value: u64::try_from(v).unwrap_or(0),
                reason: "query-latency goal: lower compaction trigger",
            });
        }
        // Grow the target Vortex file size so compaction emits fewer, larger files
        // — better scan throughput and per-file stats, and less file fan-out to
        // probe per query. No CPU gate: it changes the size of the files the
        // background compactor already writes, not the write rate. It does carry
        // the same `mem_ok` gate as every other grow move, because a larger target
        // buffers more encoded bytes per output file — and because the memory rule
        // above drives the mem-tier cap to its floor, so an ungated raise here
        // leaves a floor-sized tier feeding a ceiling-sized file target, which
        // spills on nearly every apply.
        if mem_ok
            && let Some(v) = clamp_move_i64(
                cur.target_vortex_file_size_bytes,
                goal_grow_i64(
                    cur.target_vortex_file_size_bytes,
                    b.target_vortex_file_size_bytes,
                    query_v,
                ),
                b.target_vortex_file_size_bytes,
            )
        {
            return Some(Adjustment {
                actuator: Actuator::TargetVortexFileSize,
                new_value: u64::try_from(v).unwrap_or(0),
                reason: "query-latency goal: grow target file size → fewer, larger files for scans",
            });
        }
        // Lower the seq-prefix bake trigger so the bake fires sooner — a smaller
        // live deletion index means a cheaper merge-on-read probe per query.
        // Gated on read-amp being elevated (the read-side proxy for "the deletion
        // index is large enough to be hurting probes"): when scans are already
        // cheap (read-amp low), baking sooner would only add write-amp without a
        // query payoff, so leave the trigger where it is. No CPU/memory gate is
        // needed — lowering the trigger spends a future compaction CPU slice the
        // background compactor already schedules, not a new resource.
        if s.read_amp > READ_AMP_LOW
            && let Some(v) = clamp_move_usize(
                cur.bake_deletion_index_trigger,
                goal_shrink_usize(
                    cur.bake_deletion_index_trigger,
                    b.bake_deletion_index_trigger,
                    query_v,
                ),
                b.bake_deletion_index_trigger,
            )
        {
            return Some(Adjustment {
                actuator: Actuator::BakeDeletionIndexTrigger,
                new_value: u64::try_from(v).unwrap_or(0),
                reason: "query-latency goal: lower the bake trigger → smaller deletion index → cheaper merge-on-read probe",
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

    // (2.9) Freshness-shrink tier: a violated FRESHNESS SLO *and* an apply that is
    // actually behind offered load. SHRINK the mem-tier so checkpoints fire on
    // smaller epochs: earlier backpressure keeps a *deep apply backlog* shallow.
    //
    // **Gates (ladder A/B, 2026-07-15 local SF10 RATE-capped HTAP):** when
    // freshness is high but `apply_vs_arrival` is healthy (apply finishes well under
    // the inter-batch gap), the lag is source-side / multi-table / coalesce — not a
    // deep tier. Shrinking then collapses the absorb buffer (measured 1 GiB→67 MiB)
    // and *raises* order_line p99 freshness (base 5–9s → 11.6s; +shards 41s). Also
    // withhold shrink on mutation-heavy streams (`delete_fraction` above
    // `MUTATION_HEAVY_FRACTION`): each spill/checkpoint multiplies key-churn
    // cost the same way write-concurrency is withheld.
    //
    // When the gates block shrink, fall through to the ingest tier (write
    // concurrency / compaction) which can still act on `ingest_violated`. Ordered
    // BEFORE the ingest grow tier; buffer-grow branches stay gated
    // `!freshness_violated` so freshness still owns the *direction* on the tier
    // when shrink is eligible. `clamp_move_i64(…, b.mem_tier_max_bytes)` supplies
    // the floor + pin-respect. No `mem_ok` gate — shrink never needs headroom.
    // Match the rest of the controller: "behind" is strict `>` (see `behind` at
    // the adaptive ingest gate), not `>=`, so the equality boundary is one
    // definition across levers.
    let apply_backlogged = s.apply_vs_arrival > BEHIND_RATIO;
    if freshness_violated
        && apply_backlogged
        && !mutation_heavy
        && let Some(v) = clamp_move_i64(
            cur.mem_tier_max_bytes,
            goal_shrink_i64(cur.mem_tier_max_bytes, b.mem_tier_max_bytes, fresh_v),
            b.mem_tier_max_bytes,
        )
    {
        return Some(Adjustment {
            actuator: Actuator::MemTierMaxBytes,
            new_value: u64::try_from(v).unwrap_or(0),
            reason: "freshness goal + apply behind: shrink the in-memory CDC tier → checkpoint smaller epochs sooner (shallower apply backlog, lower visibility lag)",
        });
    }

    // (3) Ingest/lag tier: a violated replication-lag/freshness goal. Grow buffers
    // first (help lag AND queries), then the mem-tier, then add write shards —
    // gated so extra shards (= more files) never fire while a query goal is
    // violated, read-amp is high, or the stream is delete-heavy.
    if ingest_violated {
        // Buffer growth is withheld under a freshness violation (see the mem-tier
        // grow gate below): when data is too slow to become queryable, growing
        // buffers is the wrong direction. Under a pure LAG violation it fires as
        // before.
        if !freshness_violated
            && mem_ok
            && let Some(v) = clamp_move_i64(
                cur.inline_flush_max_bytes,
                goal_grow_i64(
                    cur.inline_flush_max_bytes,
                    b.inline_flush_max_bytes,
                    ingest_v,
                ),
                b.inline_flush_max_bytes,
            )
        {
            return Some(Adjustment {
                actuator: Actuator::InlineFlushBytes,
                new_value: u64::try_from(v).unwrap_or(0),
                reason: "replication-lag goal: enlarge memtable (fewer files + amortized commits)",
            });
        }
        // Gated `!freshness_violated`: growing the tier is the LAG lever (fewer
        // writer-blocking spills); it is the opposite of the freshness-shrink lever,
        // so it must not fire when freshness is the violation being served (else the
        // two limit-cycle the tier up/down). Freshness owns the tier; lag falls back
        // to the throughput levers below when freshness is also violated.
        if !freshness_violated
            && mem_ok
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
        // Withheld under memory pressure for the same reason as every other
        // grow move: each extra shard is another in-flight encode buffer and
        // inline memtable, so near the budget this lever converts a lag
        // violation into an OOM kill.
        if !query_violated
            && s.read_amp <= READ_AMP_LOW
            && mem_ok
            && !mutation_heavy
            && cpu_ok
            && !io_bound
            && !publish_bound
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
        // CPU is the contended resource, ingest is behind, AND the query SLOs have
        // headroom to spend: shed concurrent analytical queries to hand cores back to
        // the CDC apply. Three conjuncts:
        //   - `!cpu_ok` — shedding queries only helps when CPU is the contention (the
        //     complement of the CPU-gated levers above: raising write shards and
        //     compacting more are WITHHELD under contention, so admitting fewer
        //     queries is the only lever left);
        //   - `ingest_violated` (this tier's guard) — there is a lag/freshness goal to
        //     serve;
        //   - `query_comfortably_met` — the query SLOs (QPH + query-latency) sit
        //     comfortably above target, so we can borrow query capacity WITHOUT
        //     breaching them. This makes the query SLOs the reserve's BUDGET: the apply
        //     borrows query cores down to (not through) the QPH/latency targets, and
        //     tier (1b) brakes the moment throttling pushes a query goal to violation.
        //     Exactly the "QPH target met + lag target missed ⇒ redirect resources to
        //     ingest" trade — and, since unset query goals read as met, a strict no-op
        //     versus the prior ingest+CPU-only behavior when no query SLO is configured.
        // Bounded step like every other goal move; the governor re-clamps the reported
        // demand to the real admission pool's `max - 1`.
        if !cpu_ok
            && goals.query_comfortably_met(s)
            && let Some(v) = clamp_move_usize(
                cur.query_admission_reserve,
                goal_grow_usize(
                    cur.query_admission_reserve,
                    b.query_admission_reserve,
                    ingest_v,
                ),
                b.query_admission_reserve,
            )
        {
            return Some(Adjustment {
                actuator: Actuator::QueryAdmissionReserve,
                new_value: u64::try_from(v).unwrap_or(0),
                reason: "ingest goal behind under CPU contention with query-SLO headroom: reserve query-admission slots for CDC apply (shed concurrent analytical queries)",
            });
        }
    }

    // (3b) Write-pressure backoff for the bake trigger: when the write path is
    // I/O- or publish-bound (the same `vortex_write` / metastore-publish latency
    // signals the storage-tier logic reads — the bake's write-amplification adds
    // directly to that path), RAISE the bake trigger so the bake fires less often
    // and stops competing for write throughput. Scaled by whichever ingest/lag
    // violation is active (`ingest_v`), with the goal-mode crawl floor so it still
    // moves under a bare write-pressure signal. Withheld while a query goal is
    // violated so it never fights the query-tier LOWER move above (queries win).
    if (io_bound || publish_bound)
        && !query_violated
        && let Some(v) = clamp_move_usize(
            cur.bake_deletion_index_trigger,
            goal_grow_usize(
                cur.bake_deletion_index_trigger,
                b.bake_deletion_index_trigger,
                ingest_v,
            ),
            b.bake_deletion_index_trigger,
        )
    {
        return Some(Adjustment {
            actuator: Actuator::BakeDeletionIndexTrigger,
            new_value: u64::try_from(v).unwrap_or(0),
            reason: "write pressure (io/publish-bound): raise the bake trigger → bake less often → bound bake write-amp",
        });
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

/// An exponentially-weighted moving average with first-sample seeding.
///
/// The first [`update`](Self::update) seeds the average to the sample exactly (no
/// bias toward an initial zero); each later sample blends in at `alpha`
/// (`next = alpha·sample + (1−alpha)·prev`). Encapsulating the smoothing — rather
/// than scattering `alpha · x + (1−alpha) · slot` across the accounting struct —
/// keeps the seeding/blending logic in one unit-tested place and lets each signal
/// carry its own `alpha` (e.g. [`EWMA_ALPHA_FAST`] for the cliff detector).
#[derive(Debug, Clone, Copy)]
struct Ewma {
    alpha: f64,
    /// `None` until the first sample seeds it.
    value: Option<f64>,
}

impl Ewma {
    /// An EWMA with the default smoothing ([`EWMA_ALPHA`]).
    const fn new() -> Self {
        Self::with_alpha(EWMA_ALPHA)
    }

    /// An EWMA with an explicit smoothing factor (e.g. [`EWMA_ALPHA_FAST`] for the
    /// FAST I/O/publish latency estimate that powers the cliff detector).
    const fn with_alpha(alpha: f64) -> Self {
        Self { alpha, value: None }
    }

    /// Fold in a sample: the first seeds the average exactly, later samples blend
    /// at `alpha`.
    fn update(&mut self, sample: f64) {
        self.value = Some(match self.value {
            None => sample,
            Some(prev) => self.alpha * sample + (1.0 - self.alpha) * prev,
        });
    }

    /// The current average, or `None` before the first sample.
    #[must_use]
    fn value(&self) -> Option<f64> {
        self.value
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

/// Shrink an `i64` actuator by one goal-mode step (the `saturating_sub` twin of
/// [`goal_grow_i64`]). The result is clamped to `[floor, ceiling]` by
/// `clamp_move_i64` at the call site — the floor (e.g. [`MEM_TIER_MIN_BYTES`])
/// bounds how far the freshness lever can shrink the mem-tier.
fn goal_shrink_i64(v: i64, (lo, hi): (i64, i64), violation: f64) -> i64 {
    let range = u64::try_from(hi.saturating_sub(lo)).unwrap_or(0);
    let step = i64::try_from(goal_step_magnitude_u64(range, violation)).unwrap_or(i64::MAX);
    v.saturating_sub(step)
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
    (fraction.is_finite() && fraction >= 0.0)
        .then(|| (fraction.min(1000.0) * 1000.0).round() as u64)
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

/// True when the FAST latency EWMA has stepped well above the slow one — the I/O
/// cliff signal. Requires both a multiplicative jump ([`IO_CLIFF_RATIO`]) and an
/// absolute floor ([`IO_CLIFF_FLOOR_MS`]) so a 3× jump within sub-millisecond noise
/// can't trip it. `None` on either EWMA (the table hasn't spilled) ⇒ false.
fn is_cliff(fast_ms: Option<f64>, slow_ms: Option<f64>) -> bool {
    matches!(
        (fast_ms, slow_ms),
        (Some(fast), Some(slow))
            if slow > 0.0 && fast > IO_CLIFF_FLOOR_MS && fast > IO_CLIFF_RATIO * slow
    )
}

/// Continuous slow-tier bias factor in `[SLOW_TIER_BOUND_SCALE, 1.0]`. When the
/// calibration probe measured the volume's write throughput, the factor scales
/// linearly with it — `≤ TIER_SCALE_SLOW_MBPS` (a gp3-baseline volume) gets the
/// full slow-tier bias, `≥ TIER_SCALE_FAST_MBPS` (fast `NVMe` / high io2) gets none,
/// interpolated between — so a fast io2 volume is no longer penalized like slow
/// gp3 just because both classify as `Ebs`. Without a measurement (remote path,
/// probe failure, or a never-spilling memory-tier table), it falls back to the
/// binary class (`Unknown` treated slow — the safe default).
fn tier_scale(storage: StorageClass, measured_mbps: Option<f64>) -> f64 {
    if let Some(mbps) = measured_mbps.filter(|m| m.is_finite() && *m > 0.0) {
        let span = TIER_SCALE_FAST_MBPS - TIER_SCALE_SLOW_MBPS;
        let t = ((mbps - TIER_SCALE_SLOW_MBPS) / span).clamp(0.0, 1.0);
        SLOW_TIER_BOUND_SCALE + t * (1.0 - SLOW_TIER_BOUND_SCALE)
    } else if storage.is_slow_tier() {
        SLOW_TIER_BOUND_SCALE
    } else {
        1.0
    }
}

/// The effective latency-bound fraction for a storage tier: slow/networked media
/// trip the I/O-/publish-bound gate at a lower fraction so the closed loop
/// amortizes commits and withholds shards sooner. Continuous in the measured
/// throughput (see [`tier_scale`]); reads the detected tier + probe result so the
/// loop is storage-aware without a dedicated decide branch.
fn tier_bound_fraction(base: f64, storage: StorageClass, measured_mbps: Option<f64>) -> f64 {
    base * tier_scale(storage, measured_mbps)
}

/// Classify the resource most likely binding when a goal stays violated with no
/// actuator move left — the body of the infeasible-SLO operator warning. Pure;
/// reasons over the same signals the controller gates on, in priority order:
/// memory (the hard #1), then CPU, then the write path / storage, else the static
/// actuator bounds themselves.
pub(crate) fn binding_constraint(s: &IngestSnapshot) -> &'static str {
    // Mirror the controller's EFFECTIVE memory gate: a confirmed slow tier blocks
    // buffer growth `SLOW_TIER_MEM_DRAIN_OFFSET` earlier (the earlier-drain gate in
    // `decide`), so memory is the binding constraint at that same shifted threshold —
    // otherwise a slow-tier goal stuck behind the shifted gate is misclassified as
    // non-memory-bound.
    let mem_gate = if s.confirmed_slow_tier() {
        MEM_PRESSURE_OK - SLOW_TIER_MEM_DRAIN_OFFSET
    } else {
        MEM_PRESSURE_OK
    };
    // `>=`, not `>`: the controller's `mem_ok` is `p < mem_gate`, so it already
    // blocks growth at exactly `p == mem_gate` — bind on memory at the same point.
    if s.mem_pressure.is_some_and(|p| p >= mem_gate) {
        "memory-bound (at/over the RAM budget — the controller can't grow buffers to meet the SLO; add memory or lower runtime.query.memory_limit)"
    } else if !s.cpu_ok() {
        if s.cpu_burstable {
            "CPU-bound (burstable instance — CPU credits likely depleted; use a non-burstable instance)"
        } else {
            "CPU-bound (encode/compaction is saturating cores; scale up CPU)"
        }
    } else if is_cliff(s.io_latency_fast_ms, s.io_latency_ms)
        || is_cliff(s.publish_latency_fast_ms, s.publish_latency_ms)
        || latency_bound(
            s.io_latency_ms,
            s.arrival_gap_ms,
            tier_bound_fraction(IO_BOUND_FRACTION, s.data_storage, s.data_write_mbps),
        )
        || latency_bound(
            s.publish_latency_ms,
            s.arrival_gap_ms,
            tier_bound_fraction(
                PUBLISH_BOUND_FRACTION,
                s.metastore_storage,
                s.metastore_write_mbps,
            ),
        )
    {
        if matches!(s.data_storage, StorageClass::Ebs)
            || matches!(s.metastore_storage, StorageClass::Ebs)
        {
            "storage-bound: EBS write bandwidth (provision more IOPS/throughput, move the metastore to faster storage, or relax the SLO)"
        } else if s.data_tier_is_slow() || s.metastore_tier_is_slow() {
            "storage-bound: slow or undetected tier (use faster/local storage, or relax the SLO)"
        } else {
            "write-path I/O-bound (the storage write path can't keep up)"
        }
    } else {
        "actuator limits reached (the configured tuning bounds can't meet the SLO on this hardware — relax the SLO or scale up)"
    }
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
        clippy::cast_possible_wrap,
        // Controller/WindowMax tests assert on exact, small, representable f64 values
        // (folded literals + clean n/1000 divisions) where `==` is exact and correct.
        clippy::float_cmp
    )]
    use super::*;

    #[test]
    fn ewma_seeds_first_sample_then_blends() {
        let mut e = Ewma::with_alpha(0.5);
        assert!(e.value().is_none(), "an unseeded average reads as None");
        e.update(10.0);
        assert!(
            (e.value().unwrap_or(f64::NAN) - 10.0).abs() < 1e-12,
            "the first sample seeds the average exactly (no bias toward 0)"
        );
        e.update(20.0); // 0.5*20 + 0.5*10
        assert!((e.value().unwrap_or(f64::NAN) - 15.0).abs() < 1e-12);
        e.update(20.0); // 0.5*20 + 0.5*15
        assert!((e.value().unwrap_or(f64::NAN) - 17.5).abs() < 1e-12);
    }

    #[test]
    fn ewma_alpha_one_has_no_memory() {
        let mut e = Ewma::with_alpha(1.0);
        e.update(5.0);
        e.update(99.0);
        assert!(
            (e.value().unwrap_or(f64::NAN) - 99.0).abs() < 1e-12,
            "alpha=1 tracks the latest sample"
        );
    }

    #[test]
    fn ewma_default_alpha_blends_with_history() {
        let mut e = Ewma::new();
        e.update(100.0);
        e.update(0.0);
        // alpha*0 + (1-alpha)*100, derived from the constant so the test tracks it.
        let expected = (1.0 - EWMA_ALPHA) * 100.0;
        assert!((e.value().unwrap_or(f64::NAN) - expected).abs() < 1e-12);
    }

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
            bake_deletion_index_trigger: (1_000, 5_000_000),
            write_concurrency: (1, 16),
            mem_tier_max_bytes: (64 * 1024 * 1024, 2048 * 1024 * 1024),
            target_vortex_file_size_bytes: (64 * 1024 * 1024, 1024 * 1024 * 1024),
            query_admission_reserve: (0, 16),
        }
    }

    fn actuators() -> ActuatorValues {
        ActuatorValues {
            inline_flush_max_bytes: 8 * 1024 * 1024,
            inline_flush_max_rows: 8192,
            inline_flush_max_segments: 64,
            compaction_background_interval_ms: 10_000,
            compaction_trigger_files: 8,
            bake_deletion_index_trigger: 50_000,
            write_concurrency: 4,
            mem_tier_max_bytes: 256 * 1024 * 1024,
            target_vortex_file_size_bytes: 256 * 1024 * 1024,
            query_admission_reserve: 0,
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
            cpu_burstable: false,
            io_latency_ms: None,
            publish_latency_ms: None,
            io_latency_fast_ms: None,
            publish_latency_fast_ms: None,
            data_storage: StorageClass::Unknown,
            metastore_storage: StorageClass::Unknown,
            data_write_mbps: None,
            metastore_write_mbps: None,
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
    fn memory_pressure_withholds_the_write_concurrency_raise() {
        // Falling behind with buffers already maxed would normally add encode
        // parallelism — but every extra shard is more resident bytes, so under
        // memory pressure the raise must be withheld. Pressure between OK and
        // HIGH: no shrink tier fires, and the raise must not either. Regression
        // test for the SF-1000/96G OOMs where shards were raised at 0.98.
        let s = IngestSnapshot {
            apply_vs_arrival: 1.5,
            mem_pressure: Some(f64::midpoint(MEM_PRESSURE_OK, MEM_PRESSURE_HIGH)),
            ..snap()
        };
        let buffers_maxed = ActuatorValues {
            inline_flush_max_bytes: bounds().inline_flush_max_bytes.1,
            mem_tier_max_bytes: bounds().mem_tier_max_bytes.1,
            ..actuators()
        };
        if let Some(adj) = decide_fresh(&s, &buffers_maxed, &bounds()) {
            assert_ne!(
                adj.actuator,
                Actuator::WriteConcurrency,
                "must not add shards (resident bytes) under memory pressure"
            );
        }
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
                Actuator::BakeDeletionIndexTrigger => (
                    b.bake_deletion_index_trigger.0 as u64,
                    b.bake_deletion_index_trigger.1 as u64,
                ),
                Actuator::WriteConcurrency => {
                    (b.write_concurrency.0 as u64, b.write_concurrency.1 as u64)
                }
                Actuator::TargetVortexFileSize => (
                    b.target_vortex_file_size_bytes.0 as u64,
                    b.target_vortex_file_size_bytes.1 as u64,
                ),
                Actuator::QueryAdmissionReserve => (
                    b.query_admission_reserve.0 as u64,
                    b.query_admission_reserve.1 as u64,
                ),
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

    /// Current value of `actuator`, as the same `u64` an [`Adjustment`] carries,
    /// so a move can be classified as a raise or a shrink.
    fn current_value(cur: &ActuatorValues, actuator: Actuator) -> u64 {
        match actuator {
            Actuator::InlineFlushBytes => u64::try_from(cur.inline_flush_max_bytes).unwrap_or(0),
            Actuator::MemTierMaxBytes => u64::try_from(cur.mem_tier_max_bytes).unwrap_or(0),
            Actuator::TargetVortexFileSize => {
                u64::try_from(cur.target_vortex_file_size_bytes).unwrap_or(0)
            }
            Actuator::CompactionIntervalMs => cur.compaction_background_interval_ms,
            Actuator::CompactionTriggerFiles => {
                u64::try_from(cur.compaction_trigger_files).unwrap_or(0)
            }
            Actuator::BakeDeletionIndexTrigger => {
                u64::try_from(cur.bake_deletion_index_trigger).unwrap_or(0)
            }
            Actuator::WriteConcurrency => u64::try_from(cur.write_concurrency).unwrap_or(0),
            Actuator::QueryAdmissionReserve => {
                u64::try_from(cur.query_admission_reserve).unwrap_or(0)
            }
        }
    }

    /// The invariant behind [`Actuator::consumes_memory`]: no rule, in either
    /// ladder, may RAISE a memory-consuming actuator while memory is tight. The
    /// memory rule holds the hard objective (stay within the budget), so a growth
    /// rule that ignores it puts two rules in opposition — and the growth rule
    /// wins whenever it is reached first.
    ///
    /// A sweep rather than a per-rule test because both known violations were
    /// single rules missed in a hand audit: the write-concurrency raise and the
    /// target-file-size raise, each three lines from a gated sibling. Shrinks are
    /// expected and allowed — only raises are the violation.
    ///
    /// The decider returns at most ONE move per call and the memory rule runs
    /// first, so the buffers it shrinks are pinned at their floors here: with no
    /// shrink left to make, the tick falls through to the growth rules that are
    /// the actual subject. Anything left mid-range would mask them behind a
    /// shrink. `later_levers_exhausted` walks further still, retiring the
    /// non-memory levers each ladder reaches before its file-size / shard raises.
    #[test]
    fn memory_consuming_actuators_are_never_raised_under_pressure() {
        let b = bounds();
        // Memory is the one condition held constant: critically tight.
        let pressured = |s: IngestSnapshot| IngestSnapshot {
            mem_pressure: Some(0.95),
            ..s
        };
        let snapshots = [
            pressured(snap()),
            // Falling behind: the ingest-speed ladder.
            pressured(IngestSnapshot {
                apply_vs_arrival: 3.0,
                ..snap()
            }),
            // Read-amp high: the query-health ladder.
            pressured(IngestSnapshot {
                read_amp: 40,
                ..snap()
            }),
            // Bursty arrivals: the durability-buffer pre-grow.
            pressured(IngestSnapshot {
                arrival_cv: 2.0,
                ..snap()
            }),
            // I/O and publish cliffs: the decisive-backoff fast path.
            pressured(IngestSnapshot {
                io_latency_ms: Some(20.0),
                io_latency_fast_ms: Some(200.0),
                publish_latency_ms: Some(20.0),
                publish_latency_fast_ms: Some(200.0),
                ..snap()
            }),
            // Everything at once, CPU free so no CPU gate masks an ungated raise.
            pressured(IngestSnapshot {
                replication_lag_secs: Some(120.0),
                freshness_secs: Some(120.0),
                query_latency_p99_ms: Some(500.0),
                qph: Some(1.0),
                cpu_pressure: Some(0.1),
                read_amp: 40,
                apply_vs_arrival: 3.0,
                arrival_cv: 2.0,
                ..snap()
            }),
            // Lag violated with every *other* condition permissive — low read-amp,
            // CPU free, no I/O/publish bound, no query goal competing for the move.
            // The shard-raise branches sit behind exactly that combination, so
            // without it the sweep never reaches them.
            pressured(IngestSnapshot {
                replication_lag_secs: Some(120.0),
                apply_vs_arrival: 3.0,
                cpu_pressure: Some(0.1),
                read_amp: 1,
                ..snap()
            }),
        ];
        // The memory buffers sit at their floors so the memory rule has no shrink
        // to make and the tick reaches the growth rules.
        let buffers_at_floor = ActuatorValues {
            inline_flush_max_bytes: b.inline_flush_max_bytes.0,
            mem_tier_max_bytes: b.mem_tier_max_bytes.0,
            ..actuators()
        };
        let positions = [
            buffers_at_floor,
            // ...and with the CPU/IO levers each ladder tries first also retired,
            // so the file-size and shard raises beyond them are reachable.
            ActuatorValues {
                compaction_background_interval_ms: b.compaction_background_interval_ms.0,
                compaction_trigger_files: b.compaction_trigger_files.0,
                bake_deletion_index_trigger: b.bake_deletion_index_trigger.0,
                // Zero, not the ceiling: a non-zero reserve makes the handback rule
                // — which runs ahead of the query tier — consume the tick's one move.
                query_admission_reserve: b.query_admission_reserve.0,
                write_concurrency: b.write_concurrency.0,
                ..buffers_at_floor
            },
        ];
        // Each goal alone, then all together: a single-goal tick reaches rules that
        // an earlier-priority goal would otherwise consume the move for.
        let window = Duration::from_mins(1);
        let goal_sets = [
            Goals::from_targets(Some(5.0), None, None, None, window),
            Goals::from_targets(None, Some(5.0), None, None, window),
            Goals::from_targets(None, None, Some(100.0), None, window),
            Goals::from_targets(None, None, None, Some(10_000.0), window),
            Goals::from_targets(Some(5.0), Some(5.0), Some(100.0), Some(10_000.0), window),
        ];

        let mut reached = 0_usize;
        for s in &snapshots {
            for cur in &positions {
                let moves = std::iter::once(("legacy", decide_fresh(s, cur, &b))).chain(
                    goal_sets
                        .iter()
                        .map(|g| ("goal", goal_decide(s, cur, &b, g))),
                );
                for (mode, adj) in moves {
                    let Some(adj) = adj else { continue };
                    if !adj.actuator.consumes_memory() {
                        continue;
                    }
                    reached += 1;
                    assert!(
                        adj.new_value <= current_value(cur, adj.actuator),
                        "{mode} ladder raised {} from {} to {} at mem_pressure 0.95 (reason: {}) \
                         — every memory-consuming raise must be gated on `mem_ok`",
                        adj.actuator.as_str(),
                        current_value(cur, adj.actuator),
                        adj.new_value,
                        adj.reason,
                    );
                }
            }
        }
        // Guard against the sweep silently going vacuous: if no combination ever
        // returns a memory-consuming move, the assertion above proves nothing.
        assert!(
            reached > 0,
            "the sweep never reached a memory-consuming actuator — it is no longer testing anything"
        );
    }

    /// Regression test for #12531: the memory signal must not count the page
    /// cache the table's own Vortex writes leave behind. Numbers are the live
    /// cgroup accounting captured from a CH-benCHmark SF-1000 runner mid-run —
    /// 215.6 GiB charged against a 256 GiB limit (ratio 0.842, already past
    /// `MEM_PRESSURE_OK` and reaching CRITICAL under load — 19% of the run's
    /// samples, median 0.951) while unreclaimable demand was 152.5 GiB (0.596)
    /// and the kernel reported 50 µs of reclaim stall over the whole run.
    #[cfg(target_os = "linux")]
    #[test]
    fn working_set_excludes_reclaimable_page_cache() {
        const GIB: u64 = 1024 * 1024 * 1024;
        let stat = "anon 163775418368\n\
                    file 65558777856\n\
                    kernel 2179072000\n\
                    shmem 0\n\
                    file_dirty 9480785920\n\
                    file_writeback 0\n\
                    inactive_file 208657100800\n\
                    active_anon 20272680960\n";
        let current = 231_513_268_224_u64; // 215.6 GiB
        let limit = 256 * GIB;

        let reclaimable = reclaimable_page_cache(stat, "file", RECLAIM_EXCLUDED_V2)
            .expect("memory.stat carries `file`");
        // Only the clean, non-tmpfs page cache: `file` less dirty/writeback/shmem.
        assert_eq!(reclaimable, 65_558_777_856 - 9_480_785_920);

        let pressure = u64_to_f64(current.saturating_sub(reclaimable)) / u64_to_f64(limit);
        assert!(
            pressure < MEM_PRESSURE_OK,
            "unreclaimable demand must read as headroom, got {pressure}"
        );
        // The raw charge is what used to drive the collapse.
        assert!(u64_to_f64(current) / u64_to_f64(limit) > MEM_PRESSURE_OK);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn reclaimable_page_cache_parsing_edge_cases() {
        // v2: `shmem` (needs swap) and in-flight writeback are demand, not cache.
        assert_eq!(
            reclaimable_page_cache(
                "file 1000\nshmem 200\nfile_writeback 300\n",
                "file",
                RECLAIM_EXCLUDED_V2
            ),
            Some(500)
        );
        // Counters that do not reconcile can never inflate the subtraction.
        assert_eq!(
            reclaimable_page_cache("file 100\nshmem 900\n", "file", RECLAIM_EXCLUDED_V2),
            Some(0)
        );
        // No `file` key at all: unknown, so the caller keeps the raw charge.
        assert_eq!(
            reclaimable_page_cache("anon 100\n", "file", RECLAIM_EXCLUDED_V2),
            None
        );
        // v1 prefers the hierarchical `total_*` tallies...
        assert_eq!(
            v1_reclaimable_page_cache(
                "cache 10\ntotal_cache 1000\ntotal_shmem 100\ntotal_dirty 50\n"
            ),
            Some(850)
        );
        // ...and falls back to the this-cgroup-only keys when they are absent.
        assert_eq!(
            v1_reclaimable_page_cache("cache 1000\nshmem 100\nwriteback 50\n"),
            Some(850)
        );
    }

    /// Whitespace tolerance in [`parse_cgroup_stat_key`]. The kernel emits a single
    /// space, but a key missed on a formatting difference reads as "no reclaimable
    /// cache", which silently restores the raw-charge over-counting — so parse
    /// leniently rather than relying on the emitted format.
    #[cfg(target_os = "linux")]
    #[test]
    fn stat_key_parsing_tolerates_whitespace() {
        for body in [
            "file 1000\n",
            "  file   1000  \n",
            "file\t1000\n",
            "anon 5\nfile 1000\nshmem 0\n",
        ] {
            assert_eq!(
                parse_cgroup_stat_key(body, "file"),
                Some(1000),
                "failed to parse {body:?}"
            );
        }
        // A key that only appears as a *prefix* of another must not match.
        assert_eq!(parse_cgroup_stat_key("file_dirty 7\n", "file"), None);
        // Absent, empty, and value-less lines are all "unknown", never 0.
        assert_eq!(parse_cgroup_stat_key("anon 1\n", "file"), None);
        assert_eq!(parse_cgroup_stat_key("\n\n", "file"), None);
        assert_eq!(parse_cgroup_stat_key("file\n", "file"), None);
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
    fn goal_path_withholds_write_concurrency_raise_under_memory_pressure() {
        // Replication-lag goal violated, buffers maxed, CPU/IO/read-amp all
        // permissive — the one blocking condition is memory pressure between
        // OK and HIGH. The goal path must withhold the shard raise exactly
        // like the legacy ladder does. Regression test for the SF-1000/96G
        // OOMs where the lag goal raised write concurrency at 0.98 pressure.
        let s = IngestSnapshot {
            replication_lag_secs: Some(120.0),
            apply_vs_arrival: 1.5,
            mem_pressure: Some(f64::midpoint(MEM_PRESSURE_OK, MEM_PRESSURE_HIGH)),
            ..snap()
        };
        let buffers_maxed = ActuatorValues {
            inline_flush_max_bytes: bounds().inline_flush_max_bytes.1,
            mem_tier_max_bytes: bounds().mem_tier_max_bytes.1,
            ..actuators()
        };
        if let Some(adj) = goal_decide(&s, &buffers_maxed, &bounds(), &lag_goal(5.0)) {
            assert_ne!(
                adj.actuator,
                Actuator::WriteConcurrency,
                "the lag goal must not add shards (resident bytes) under memory pressure"
            );
        }
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
    fn freshness_goal_violated_behind_apply_shrinks_mem_tier_not_memtable() {
        // A violated freshness SLO *with apply behind* SHRINKS the in-memory CDC
        // tier and WITHHOLDS the buffer-grow levers. Healthy-apply freshness no
        // longer shrinks (ladder A scar) — see
        // `freshness_violation_healthy_apply_does_not_shrink`.
        let s = IngestSnapshot {
            freshness_secs: Some(30.0),
            apply_ms: 150.0,
            arrival_gap_ms: 100.0,
            apply_vs_arrival: 1.5,
            ..snap()
        };
        let goals = Goals::from_targets(None, Some(5.0), None, None, Duration::from_mins(1));
        let adj = goal_decide(&s, &actuators(), &bounds(), &goals).expect("a move");
        assert_eq!(adj.actuator, Actuator::MemTierMaxBytes);
        assert!(
            adj.new_value < actuators().mem_tier_max_bytes as u64,
            "freshness+behind shrinks the tier, never grows the memtable",
        );
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
        // Memtable at ceiling, compaction at its floors, target file size at its
        // ceiling → only the shard-shed remains.
        let cur = ActuatorValues {
            inline_flush_max_bytes: 128 * 1024 * 1024,
            compaction_background_interval_ms: 2_000,
            compaction_trigger_files: 2,
            target_vortex_file_size_bytes: 1024 * 1024 * 1024,
            write_concurrency: 8,
            ..actuators()
        };
        let adj = goal_decide(&s, &cur, &bounds(), &goals).expect("a move");
        assert_eq!(adj.actuator, Actuator::WriteConcurrency);
        assert!(
            adj.new_value < 8,
            "latency goal sheds a shard, never grows one"
        );
    }

    #[test]
    fn query_latency_goal_withholds_target_file_size_growth_under_memory_pressure() {
        // Same setup as the growth test below, but memory is tight. The memory
        // rule drives the mem-tier cap to its floor, so growing the file target
        // here would leave a floor-sized tier feeding a ceiling-sized target and
        // spill on nearly every apply (issue #12531: 24 spills, 465 s of a 908 s
        // window on `order_line`).
        let s = IngestSnapshot {
            query_latency_p99_ms: Some(500.0),
            read_amp: 2,
            mem_pressure: Some(0.95),
            ..snap()
        };
        let goals = Goals::from_targets(None, None, Some(100.0), None, Duration::from_mins(1));
        let cur = ActuatorValues {
            inline_flush_max_bytes: 128 * 1024 * 1024,
            compaction_background_interval_ms: 2_000,
            compaction_trigger_files: 2,
            target_vortex_file_size_bytes: 256 * 1024 * 1024,
            ..actuators()
        };
        if let Some(adj) = goal_decide(&s, &cur, &bounds(), &goals) {
            assert_ne!(
                adj.actuator,
                Actuator::TargetVortexFileSize,
                "the query goal must not grow the file target while memory is tight"
            );
        }
    }

    #[test]
    fn query_latency_goal_grows_target_file_size_when_compaction_maxed() {
        // p99 over goal; memtable at ceiling and the compaction levers at their
        // floors, so the decider falls through to growing the target Vortex file
        // size — fewer, larger files for scans — before it sheds a write shard.
        let s = IngestSnapshot {
            query_latency_p99_ms: Some(500.0),
            read_amp: 2,
            ..snap()
        };
        let goals = Goals::from_targets(None, None, Some(100.0), None, Duration::from_mins(1));
        let cur = ActuatorValues {
            inline_flush_max_bytes: 128 * 1024 * 1024,
            compaction_background_interval_ms: 2_000,
            compaction_trigger_files: 2,
            // 256 MiB, below the test bounds ceiling (1 GiB) → room to grow.
            target_vortex_file_size_bytes: 256 * 1024 * 1024,
            ..actuators()
        };
        let adj = goal_decide(&s, &cur, &bounds(), &goals).expect("a move");
        assert_eq!(adj.actuator, Actuator::TargetVortexFileSize);
        assert!(
            adj.new_value > 256 * 1024 * 1024,
            "the query goal grows the target file size, got {}",
            adj.new_value
        );
        assert!(
            adj.new_value <= bounds().target_vortex_file_size_bytes.1 as u64,
            "stays within the ceiling"
        );
    }

    #[test]
    fn adaptive_target_file_size_bounds_scale_with_config() {
        // Default 256 MiB → [½×, 4×].
        assert_eq!(
            adaptive_target_file_size_bounds(256 * 1024 * 1024),
            (128 * 1024 * 1024, 1024 * 1024 * 1024)
        );
        // S3-class 512 MiB default → up to the 2 GiB ceiling.
        assert_eq!(
            adaptive_target_file_size_bounds(512 * 1024 * 1024),
            (256 * 1024 * 1024, 2048 * 1024 * 1024)
        );
        // Small EBS-class 64 MiB → clamped to the 64 MiB floor.
        assert_eq!(
            adaptive_target_file_size_bounds(64 * 1024 * 1024),
            (64 * 1024 * 1024, 256 * 1024 * 1024)
        );
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
        let goals = Goals::from_targets(Some(5.0), None, Some(100.0), None, Duration::from_mins(1));
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
        assert!(
            adj.new_value > 4,
            "lag goal raises concurrency when buffers maxed"
        );
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
        let adj = decide_with_goals(
            &s,
            &actuators(),
            &bounds(),
            ms(60_000),
            ms(30_000),
            40,
            &lag_goal(5.0),
        );
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
        assert!(
            delta > 0 && delta <= max_step,
            "delta {delta} not in (0, {max_step}]"
        );
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
            assert!(
                delta > 0 && delta <= max_step,
                "step {delta} exceeds cap {max_step}"
            );
            live.apply(&adj);
            steps += 1;
            assert!(
                steps <= STEPS_PER_WINDOW + 1,
                "did not converge within N steps"
            );
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
    fn qph_is_none_without_queries() {
        // A table that has served no queries has no QPH signal. Reporting a
        // lifetime average here (→ 0 as the anchor ages) would falsely violate a
        // QPH goal forever and drive tuning that no tuning can satisfy.
        assert!(QueryObservations::new().qph().is_none());
    }

    #[test]
    fn global_qph_positive_after_record() {
        // QPH is measured against the process-global aggregate (a query spanning
        // datasets counts once). It is a shared singleton across tests, so once any
        // query is recorded it reports a positive rate (the idle horizon is minutes,
        // far beyond a test) — assert the post-record signal, not a None precondition.
        record_global_query(12.0);
        let qph = global_qph().expect("global qph after a recorded query");
        assert!(qph > 0.0, "expected positive global qph, got {qph}");
    }

    // ---- environment gates (Part A) ---------------------------------------

    #[test]
    fn cpu_bound_withholds_cpu_stealing_moves() {
        let b = bounds();
        // Buffers already maxed, so a behind table falls through to the
        // write-concurrency lever — the first CPU-stealing move.
        let cur = ActuatorValues {
            inline_flush_max_bytes: b.inline_flush_max_bytes.1,
            mem_tier_max_bytes: b.mem_tier_max_bytes.1,
            write_concurrency: 4,
            ..actuators()
        };
        let mut s = snap();
        s.apply_vs_arrival = 1.5; // falling behind

        // Low CPU: the controller is free to raise write concurrency.
        s.cpu_pressure = Some(0.50);
        assert!(
            matches!(
                decide_fresh(&s, &cur, &b),
                Some(adj) if adj.actuator == Actuator::WriteConcurrency
            ),
            "low CPU + behind + buffers maxed → raise write concurrency"
        );

        // High CPU: the same snapshot withholds every CPU-stealing move (more
        // write shards, more compaction), leaving nothing to do.
        s.cpu_pressure = Some(0.95);
        assert!(
            decide_fresh(&s, &cur, &b).is_none(),
            "CPU-bound: no write-concurrency growth or compaction shrink"
        );
    }

    #[test]
    fn io_bound_not_behind_grows_memtable() {
        let b = bounds();
        let cur = actuators();
        let mut s = snap(); // healthy / not behind (apply_vs_arrival 0.2)
        // Per-batch write latency eats > IO_BOUND_FRACTION of the 100 ms window.
        s.io_latency_ms = Some(80.0);
        let adj = decide_fresh(&s, &cur, &b).expect("io-bound table tunes");
        assert_eq!(
            adj.actuator,
            Actuator::InlineFlushBytes,
            "io-bound, not behind → enlarge memtable to amortize commits"
        );
    }

    #[test]
    fn publish_bound_not_behind_grows_memtable() {
        let b = bounds();
        let cur = actuators();
        let mut s = snap();
        // Metastore publish-wall eats > PUBLISH_BOUND_FRACTION of the window.
        s.publish_latency_ms = Some(80.0);
        let adj = decide_fresh(&s, &cur, &b).expect("publish-bound table tunes");
        assert_eq!(adj.actuator, Actuator::InlineFlushBytes);
    }

    #[test]
    fn env_signals_absent_preserves_legacy_behavior() {
        // Regression guard: with every environment signal unavailable (the snap()
        // default), a behind table with maxed buffers raises write concurrency
        // exactly as before the env/data work — no new gate fires when its signal
        // is absent.
        let b = bounds();
        let cur = ActuatorValues {
            inline_flush_max_bytes: b.inline_flush_max_bytes.1,
            mem_tier_max_bytes: b.mem_tier_max_bytes.1,
            ..actuators()
        };
        let mut s = snap();
        s.apply_vs_arrival = 1.5;
        assert!(
            s.cpu_pressure.is_none() && s.io_latency_ms.is_none() && s.publish_latency_ms.is_none()
        );
        let adj = decide_fresh(&s, &cur, &b).expect("legacy behind path still acts");
        assert_eq!(adj.actuator, Actuator::WriteConcurrency);
    }

    // ---- storage-tier awareness (the EBS bias) ----------------------------

    #[test]
    fn tier_bound_fraction_lowers_bar_for_slow_storage() {
        // With no measurement, the binary class fallback: slow/networked tiers
        // (EBS, object store / unknown) halve the latency bar; fast tiers keep it.
        let scaled = IO_BOUND_FRACTION * SLOW_TIER_BOUND_SCALE;
        let close = |a: f64, b: f64| (a - b).abs() < f64::EPSILON;
        assert!(close(
            tier_bound_fraction(IO_BOUND_FRACTION, StorageClass::Ebs, None),
            scaled
        ));
        assert!(close(
            tier_bound_fraction(IO_BOUND_FRACTION, StorageClass::Unknown, None),
            scaled
        ));
        assert!(close(
            tier_bound_fraction(IO_BOUND_FRACTION, StorageClass::LocalSsd, None),
            IO_BOUND_FRACTION
        ));
        assert!(close(
            tier_bound_fraction(IO_BOUND_FRACTION, StorageClass::Tmpfs, None),
            IO_BOUND_FRACTION
        ));
    }

    #[test]
    fn tier_scale_is_continuous_in_measured_throughput() {
        // A measured-slow EBS volume (gp3 baseline) gets the full slow-tier scale;
        // a measured-fast EBS volume (io2) gets none — even though both are `Ebs`.
        let close = |a: f64, b: f64| (a - b).abs() < 1e-9;
        assert!(close(
            tier_scale(StorageClass::Ebs, Some(TIER_SCALE_SLOW_MBPS)),
            SLOW_TIER_BOUND_SCALE
        ));
        assert!(close(
            tier_scale(StorageClass::Ebs, Some(50.0)),
            SLOW_TIER_BOUND_SCALE
        )); // below the slow floor clamps to the full bias
        assert!(close(
            tier_scale(StorageClass::Ebs, Some(TIER_SCALE_FAST_MBPS)),
            1.0
        ));
        assert!(close(tier_scale(StorageClass::Ebs, Some(4096.0)), 1.0)); // above fast clamps to none
        // Monotonic and strictly between at a mid throughput.
        let mid = tier_scale(StorageClass::Ebs, Some(512.0));
        assert!(mid > SLOW_TIER_BOUND_SCALE && mid < 1.0);
        // No measurement → class fallback (Unknown is slow, LocalSsd is fast).
        assert!(close(
            tier_scale(StorageClass::Unknown, None),
            SLOW_TIER_BOUND_SCALE
        ));
        assert!(close(tier_scale(StorageClass::LocalSsd, None), 1.0));
        // A non-finite/zero measurement is ignored (falls back to class).
        assert!(close(tier_scale(StorageClass::LocalSsd, Some(0.0)), 1.0));
        assert!(close(
            tier_scale(StorageClass::Ebs, Some(f64::NAN)),
            SLOW_TIER_BOUND_SCALE
        ));
    }

    #[test]
    fn is_cliff_requires_ratio_and_floor() {
        // A clear step (fast ≫ slow, above the floor) is a cliff.
        assert!(is_cliff(Some(100.0), Some(20.0)));
        // Ratio met but below the absolute floor → not a cliff (sub-ms noise).
        assert!(!is_cliff(Some(0.6), Some(0.1)));
        // Above the floor but ratio not met → not a cliff.
        assert!(!is_cliff(Some(15.0), Some(12.0)));
        // Missing either EWMA (never spilled) → not a cliff.
        assert!(!is_cliff(None, Some(20.0)));
        assert!(!is_cliff(Some(100.0), None));
        assert!(!is_cliff(Some(100.0), Some(0.0)));
    }

    #[test]
    fn io_cliff_sheds_write_shards_first() {
        // Fast write EWMA ≫ slow → shed a write shard in one move.
        let s = IngestSnapshot {
            io_latency_ms: Some(20.0),
            io_latency_fast_ms: Some(200.0),
            arrival_gap_ms: 100.0,
            ..snap()
        };
        let mut a = actuators();
        a.write_concurrency = 8;
        let adj = decide_fresh(&s, &a, &bounds()).expect("cliff should act");
        assert_eq!(adj.actuator, Actuator::WriteConcurrency);
        assert!(adj.new_value < 8);
    }

    #[test]
    fn io_cliff_amortizes_via_memtable_when_shards_floored() {
        // Publish-side cliff with shards already at the floor → grow the memtable.
        let s = IngestSnapshot {
            publish_latency_ms: Some(20.0),
            publish_latency_fast_ms: Some(200.0),
            arrival_gap_ms: 100.0,
            ..snap()
        };
        let mut a = actuators();
        a.write_concurrency = 1;
        let adj = decide_fresh(&s, &a, &bounds()).expect("cliff should amortize");
        assert_eq!(adj.actuator, Actuator::InlineFlushBytes);
    }

    #[test]
    fn io_cliff_fires_in_goal_mode_before_adding_shards() {
        // A lag-violated goal would add shards; a concurrent cliff sheds instead.
        let s = IngestSnapshot {
            apply_vs_arrival: 2.0,
            replication_lag_secs: Some(60.0),
            io_latency_ms: Some(20.0),
            io_latency_fast_ms: Some(200.0),
            arrival_gap_ms: 100.0,
            ..snap()
        };
        let mut a = actuators();
        a.write_concurrency = 8;
        let goals = Goals::from_targets(Some(5.0), None, None, None, Duration::from_mins(1));
        let adj = decide_with_goals(&s, &a, &bounds(), ms(60_000), ms(30_000), 0, &goals)
            .expect("cliff should act in goal mode");
        assert_eq!(adj.actuator, Actuator::WriteConcurrency);
        assert!(adj.new_value < 8);
    }

    #[test]
    fn slow_tier_drains_memtable_earlier_than_fast_tier() {
        // Pressure between the slow and default HIGH thresholds: the slow tier
        // shrinks the memtable; the fast tier neither grows nor shrinks from memory.
        let pressure = MEM_PRESSURE_HIGH - SLOW_TIER_MEM_DRAIN_OFFSET / 2.0;
        let cur_inline =
            u64::try_from(actuators().inline_flush_max_bytes).expect("non-negative inline cap");

        let slow = IngestSnapshot {
            mem_pressure: Some(pressure),
            data_storage: StorageClass::Ebs,
            ..snap()
        };
        let adj = decide_fresh(&slow, &actuators(), &bounds()).expect("slow tier should drain");
        assert_eq!(adj.actuator, Actuator::InlineFlushBytes);
        assert!(adj.new_value < cur_inline);

        let fast = IngestSnapshot {
            mem_pressure: Some(pressure),
            data_storage: StorageClass::LocalSsd,
            metastore_storage: StorageClass::LocalSsd,
            ..snap()
        };
        // Above the fast OK band (0.75) but below its HIGH (0.85): no memory move.
        assert!(decide_fresh(&fast, &actuators(), &bounds()).is_none());
    }

    // ---- lever 3: freshness-goal mem-tier shrink ---------------------------

    /// Base actuators with the mem-tier at 1 GiB — the SF-100 adaptive value the
    /// A/B started from, with headroom above the 64 MiB floor to shrink into.
    fn actuators_1gib() -> ActuatorValues {
        let mut a = actuators();
        a.mem_tier_max_bytes = 1024 * 1024 * 1024;
        a
    }

    #[test]
    fn freshness_violation_behind_apply_shrinks_mem_tier() {
        // A violated freshness SLO *with apply behind offered load* shrinks the
        // in-memory CDC tier (deep apply backlog → smaller epochs sooner).
        // `apply_vs_arrival > BEHIND_RATIO` is required — see
        // `freshness_violation_healthy_apply_does_not_shrink`.
        let s = IngestSnapshot {
            freshness_secs: Some(5.0),
            apply_ms: 150.0,
            arrival_gap_ms: 100.0,
            apply_vs_arrival: 1.5, // behind
            ..snap()
        };
        let goals = Goals::from_targets(None, Some(3.0), None, None, Duration::from_mins(1));
        let a = actuators_1gib();
        let adj = goal_decide(&s, &a, &bounds(), &goals).expect("freshness+behind must act");
        assert_eq!(adj.actuator, Actuator::MemTierMaxBytes);
        assert!(
            adj.new_value < a.mem_tier_max_bytes as u64,
            "freshness+behind must SHRINK the tier: got {} vs cur {}",
            adj.new_value,
            a.mem_tier_max_bytes,
        );
    }

    #[test]
    fn freshness_violation_healthy_apply_does_not_shrink() {
        // Ladder A scar: freshness high but apply healthy (apply_vs_arrival ≪ 1)
        // means source/coalesce lag, not a deep tier. Shrinking starves absorb
        // (1 GiB→67 MiB) and raised order_line p99. Must not shrink.
        let s = IngestSnapshot {
            freshness_secs: Some(5.0),
            apply_ms: 3.0,
            arrival_gap_ms: 250.0,
            apply_vs_arrival: 0.012, // healthy — matches cert soak EWMA
            delete_fraction: 0.0,
            ..snap()
        };
        let goals = Goals::from_targets(None, Some(3.0), None, None, Duration::from_mins(1));
        let a = actuators_1gib();
        let adj = goal_decide(&s, &a, &bounds(), &goals);
        assert!(
            adj.is_none_or(|a| a.actuator != Actuator::MemTierMaxBytes),
            "healthy apply must not shrink the tier on freshness alone: got {:?}",
            adj.map(|a| a.actuator),
        );
    }

    #[test]
    fn freshness_violation_mutation_heavy_does_not_shrink() {
        // Even with apply behind, mutation-heavy streams pay more per spill/
        // checkpoint on key churn — withhold shrink (same gate as write shards).
        let s = IngestSnapshot {
            freshness_secs: Some(5.0),
            apply_ms: 150.0,
            arrival_gap_ms: 100.0,
            apply_vs_arrival: 1.5,
            delete_fraction: 0.45, // delete-/mutation-heavy order_line shape
            ..snap()
        };
        let goals = Goals::from_targets(None, Some(3.0), None, None, Duration::from_mins(1));
        let a = actuators_1gib();
        let adj = goal_decide(&s, &a, &bounds(), &goals);
        assert!(
            adj.is_none_or(|a| a.actuator != Actuator::MemTierMaxBytes),
            "mutation-heavy must not shrink the tier: got {:?}",
            adj.map(|a| a.actuator),
        );
    }

    #[test]
    fn lag_only_violation_still_grows_mem_tier() {
        // A pure replication-lag violation (freshness met) keeps the existing GROW
        // behavior — the shrink lever must not regress it. Memtable maxed so the
        // mem-tier grow is the surfaced move.
        let s = IngestSnapshot {
            replication_lag_secs: Some(30.0),
            freshness_secs: Some(0.0),
            ..snap()
        };
        let goals = Goals::from_targets(Some(5.0), Some(3.0), None, None, Duration::from_mins(1));
        let mut a = actuators();
        a.inline_flush_max_bytes = bounds().inline_flush_max_bytes.1;
        let adj = goal_decide(&s, &a, &bounds(), &goals).expect("lag violation must act");
        assert_eq!(adj.actuator, Actuator::MemTierMaxBytes);
        assert!(
            adj.new_value > a.mem_tier_max_bytes as u64,
            "lag-only violation must GROW the tier",
        );
    }

    #[test]
    fn freshness_and_lag_both_violated_behind_apply_shrinks_not_grows() {
        // Both violated *and apply behind*: freshness OWNS the tier — it shrinks,
        // and the lag-grow is suppressed on the same tick (no-limit-cycle).
        let s = IngestSnapshot {
            replication_lag_secs: Some(30.0),
            freshness_secs: Some(5.0),
            apply_ms: 150.0,
            arrival_gap_ms: 100.0,
            apply_vs_arrival: 1.5,
            ..snap()
        };
        let goals = Goals::from_targets(Some(5.0), Some(3.0), None, None, Duration::from_mins(1));
        let a = actuators_1gib();
        let adj = goal_decide(&s, &a, &bounds(), &goals).expect("must act");
        assert_eq!(adj.actuator, Actuator::MemTierMaxBytes);
        assert!(
            adj.new_value < a.mem_tier_max_bytes as u64,
            "with both violated and apply behind, the freshness shrink must win over the lag grow",
        );
    }

    #[test]
    fn freshness_shrink_respects_operator_pin() {
        // An operator hard-pin collapses the mem-tier bounds to a point; the shrink
        // must no-op rather than fight the pin. Snapshot is apply-behind so the
        // freshness-shrink tier is *eligible* — otherwise a healthy
        // `apply_vs_arrival` from `snap()` would vacuous-pass this test.
        let s = IngestSnapshot {
            freshness_secs: Some(5.0),
            apply_ms: 150.0,
            arrival_gap_ms: 100.0,
            apply_vs_arrival: 1.5,
            ..snap()
        };
        let goals = Goals::from_targets(None, Some(3.0), None, None, Duration::from_mins(1));
        let a = actuators_1gib();
        let pinned = TuningBounds {
            mem_tier_max_bytes: (a.mem_tier_max_bytes, a.mem_tier_max_bytes),
            ..bounds()
        };
        // Unpinned control: same snapshot must still shrink.
        let unpinned = goal_decide(&s, &a, &bounds(), &goals).expect("eligible shrink");
        assert_eq!(unpinned.actuator, Actuator::MemTierMaxBytes);
        let adj = decide_with_goals(&s, &a, &pinned, ms(60_000), ms(30_000), 0, &goals);
        assert!(
            adj.is_none_or(|adj| adj.actuator != Actuator::MemTierMaxBytes),
            "a pinned tier must never be moved by the freshness lever",
        );
    }

    #[test]
    fn freshness_shrink_gated_on_fresh_samples() {
        // Idle table (samples unchanged since the last move): freshness is not
        // actionable (it climbs on the wall clock with no new data) — no shrink.
        let s = IngestSnapshot {
            freshness_secs: Some(5.0),
            ..snap()
        };
        let goals = Goals::from_targets(None, Some(3.0), None, None, Duration::from_mins(1));
        let a = actuators_1gib();
        let adj = decide_with_goals(&s, &a, &bounds(), ms(60_000), ms(30_000), s.samples, &goals);
        assert!(
            adj.is_none_or(|adj| adj.actuator != Actuator::MemTierMaxBytes),
            "an idle table must not ratchet the tier down on wall-clock freshness",
        );
    }

    #[test]
    fn freshness_shrink_stops_at_floor() {
        // Already at the floor: the shrink clamps to a no-op and yields the lever to
        // the throughput moves rather than returning a spurious same-value move.
        let s = IngestSnapshot {
            freshness_secs: Some(5.0),
            ..snap()
        };
        let goals = Goals::from_targets(None, Some(3.0), None, None, Duration::from_mins(1));
        let mut a = actuators();
        a.mem_tier_max_bytes = bounds().mem_tier_max_bytes.0;
        let adj = goal_decide(&s, &a, &bounds(), &goals);
        assert!(
            adj.is_none_or(|adj| adj.actuator != Actuator::MemTierMaxBytes),
            "at the floor the tier cannot shrink further",
        );
    }

    // ---- metric #3: windowed-peak row freshness ----------------------------

    #[test]
    fn window_max_folds_peak_and_tumbles() {
        let mut w = WindowMax::new();
        w.fold(0, 1.0);
        w.fold(1_000, 4.0);
        w.fold(2_000, 2.0);
        assert_eq!(w.peak(3_000), 4.0, "peak is the max within the window");
        // Advance one full window: the completed window's peak (4.0) carries as prev.
        w.fold(WindowMax::WINDOW_MS + 500, 1.5);
        assert_eq!(
            w.peak(WindowMax::WINDOW_MS + 600),
            4.0,
            "the just-completed window's peak counts for one more window",
        );
        // One more window (relative to the 1.5 fold): window-0's 4.0 has aged out of
        // the 2-window memory; window-1's 1.5 becomes prev, the new 0.5 is current.
        let t = 2 * WindowMax::WINDOW_MS + 600;
        w.fold(t, 0.5);
        assert_eq!(
            w.peak(t + 10),
            1.5,
            "the 2-window-old peak (4.0) decayed; window-1's 1.5 remains as prev",
        );
        // A further window with only a small value: 1.5 decays too, leaving 0.5.
        let t2 = 3 * WindowMax::WINDOW_MS + 700;
        w.fold(t2, 0.3);
        assert_eq!(
            w.peak(t2 + 10),
            0.5,
            "after another window the 1.5 decays; only 0.5 (prev) + 0.3 (cur) remain",
        );
    }

    #[test]
    fn window_max_multi_window_idle_gap_drops_stale_peak() {
        let mut w = WindowMax::new();
        w.fold(0, 9.0);
        // Jump 3 windows ahead (idle) with a fresh small sample.
        let t = 3 * WindowMax::WINDOW_MS;
        w.fold(t, 0.25);
        assert_eq!(
            w.peak(t + 10),
            0.25,
            "a multi-window idle gap drops the old peak, leaving only the new value",
        );
    }

    #[test]
    fn fold_row_freshness_is_idle_immune() {
        let stats = IngestStats::new();
        // No source ts folded yet ⇒ no peak signal.
        assert_eq!(stats.peak_row_freshness_secs(10_000), None);
        // A batch committed 4s before it applied ⇒ 4s row freshness.
        stats.fold_row_freshness(10_000, Some(6_000));
        assert_eq!(stats.peak_row_freshness_secs(10_500), Some(4.0));
        // After a long idle, a batch that just committed applies ~fresh: the peak
        // reflects the SMALL new lag (0.5s), never the multi-window idle duration.
        let later = 10_000 + 5 * WindowMax::WINDOW_MS;
        stats.fold_row_freshness(later, Some(later - 500));
        assert_eq!(
            stats.peak_row_freshness_secs(later + 10),
            Some(0.5),
            "post-idle freshness is the batch's own small lag, not the idle gap",
        );
    }

    #[test]
    fn fold_row_freshness_skips_without_source_ts() {
        let stats = IngestStats::new();
        stats.fold_row_freshness(10_000, None);
        assert_eq!(
            stats.peak_row_freshness_secs(10_100),
            None,
            "no source commit ts ⇒ nothing folded, no signal",
        );
    }

    #[test]
    fn fold_row_freshness_clock_skew_clamps_to_zero() {
        // Source commit ts AHEAD of the host apply clock (NTP skew between the PG box
        // and the host) ⇒ a negative raw lag, which MUST clamp to 0 — never underflow
        // the unsigned subtraction into a huge spurious "freshness" that would trip a
        // false shrink. Guards the `saturating_sub(...).max(0)` in `fold_row_freshness`.
        let stats = IngestStats::new();
        stats.fold_row_freshness(10_000, Some(12_000)); // "committed" 2s after it applied
        assert_eq!(
            stats.peak_row_freshness_secs(10_100),
            Some(0.0),
            "source clock ahead of host ⇒ lag clamps to 0, no unsigned underflow",
        );
    }

    #[test]
    fn burstable_cpu_withholds_shards_at_lower_pressure() {
        // CPU busy-fraction between the burstable gate (0.50) and the default
        // (0.75), buffers maxed so write concurrency is the remaining behind-lever.
        let base = IngestSnapshot {
            apply_vs_arrival: 2.0,
            cpu_pressure: Some(0.6),
            read_amp: 1,
            ..snap()
        };
        let mut a = actuators();
        a.inline_flush_max_bytes = bounds().inline_flush_max_bytes.1;
        a.mem_tier_max_bytes = bounds().mem_tier_max_bytes.1;
        a.write_concurrency = 2;

        let normal = IngestSnapshot {
            cpu_burstable: false,
            ..base
        };
        let adj = decide_fresh(&normal, &a, &bounds()).expect("normal instance raises shards");
        assert_eq!(adj.actuator, Actuator::WriteConcurrency);

        let bursty = IngestSnapshot {
            cpu_burstable: true,
            ..base
        };
        // Burstable: cpu not ok (0.6 > 0.50) → the shard lever (and CPU-gated
        // compaction) are withheld; nothing left to do.
        assert!(decide_fresh(&bursty, &a, &bounds()).is_none());
    }

    #[test]
    fn any_actionable_violation_gates_lag_on_fresh_ingest() {
        let goals = Goals::from_targets(Some(5.0), None, None, None, Duration::from_mins(1));
        let violated = IngestSnapshot {
            replication_lag_secs: Some(60.0),
            ..snap()
        };
        // Fresh ingest: 60s lag vs a 5s goal is an actionable violation.
        assert!(goals.any_actionable_violation(&violated, true));
        // Stale/idle: lag grows with the wall clock, so it is NOT actionable.
        assert!(!goals.any_actionable_violation(&violated, false));
        // Comfortably met: not violated.
        let met = IngestSnapshot {
            replication_lag_secs: Some(1.0),
            ..snap()
        };
        assert!(!goals.any_actionable_violation(&met, true));
    }

    #[test]
    fn query_admission_reserve_grows_under_lag_with_cpu_contention_and_releases_when_clear() {
        let goals = Goals::from_targets(None, Some(5.0), None, None, Duration::from_mins(1));
        let b = bounds();

        // GROW: freshness behind (60s vs 5s) AND CPU contended (0.95), with the
        // freshness SHRINK lever already exhausted (mem-tier at its floor) and the
        // write-concurrency / compaction levers withheld under CPU contention — so
        // shedding queries is the last lever left that serves the freshness goal.
        let buffers_maxed_reserve_zero = ActuatorValues {
            inline_flush_max_bytes: b.inline_flush_max_bytes.1,
            mem_tier_max_bytes: b.mem_tier_max_bytes.0,
            ..actuators()
        };
        let behind_contended = IngestSnapshot {
            freshness_secs: Some(60.0),
            cpu_pressure: Some(0.95),
            ..snap()
        };
        let adj = decide_with_goals(
            &behind_contended,
            &buffers_maxed_reserve_zero,
            &b,
            ms(60_000),
            ms(30_000),
            0,
            &goals,
        )
        .expect("lag behind + CPU contended + buffers maxed ⇒ reserve query-admission slots");
        assert_eq!(adj.actuator, Actuator::QueryAdmissionReserve);
        assert!(
            adj.new_value > 0,
            "the reserve grows from 0 to shed concurrent queries (got {})",
            adj.new_value
        );

        // RELEASE (CPU uncontended): a reserve is held but CPU is no longer the
        // contended resource — nothing to relieve, so hand query slots back even
        // though the lag goal is still violated. Tier (1b) runs before the ingest
        // grow, so the release wins.
        let reserve_held = ActuatorValues {
            query_admission_reserve: 3,
            ..actuators()
        };
        let behind_uncontended = IngestSnapshot {
            freshness_secs: Some(60.0),
            cpu_pressure: Some(0.10),
            ..snap()
        };
        let adj = decide_with_goals(
            &behind_uncontended,
            &reserve_held,
            &b,
            ms(60_000),
            ms(30_000),
            0,
            &goals,
        )
        .expect("CPU uncontended ⇒ release a reserved query-admission slot");
        assert_eq!(adj.actuator, Actuator::QueryAdmissionReserve);
        assert!(
            (adj.new_value as usize) < 3,
            "the reserve is released toward 0 (got {})",
            adj.new_value
        );

        // RELEASE (lag goal met): CPU is still contended, but the apply has caught
        // up — the reserve's justification is gone, so release it. Keyed on the
        // INGEST goal, never the (here unset) query goal, so it can't self-perpetuate.
        let met_contended = IngestSnapshot {
            freshness_secs: Some(1.0),
            cpu_pressure: Some(0.95),
            ..snap()
        };
        let adj = decide_with_goals(
            &met_contended,
            &reserve_held,
            &b,
            ms(60_000),
            ms(30_000),
            0,
            &goals,
        )
        .expect("lag goal met ⇒ release a reserved query-admission slot");
        assert_eq!(adj.actuator, Actuator::QueryAdmissionReserve);
        assert!(
            (adj.new_value as usize) < 3,
            "released once the lag goal is met"
        );
    }

    /// The query-admission reserve is BUDGETED by the query SLOs: the CDC apply may
    /// borrow query cores while QPH (and query-latency) have headroom, but not
    /// through their targets — exactly "QPH target met + lag missed ⇒ redirect
    /// resources to ingest, but only down to the QPH floor." QPH is `HigherBetter`, so
    /// against a 1000 target: comfortably met at ≥1500 (headroom), violated at <800.
    #[test]
    fn query_admission_reserve_is_budgeted_by_the_query_slos() {
        // freshness goal 5s + QPH goal 1000.
        let goals =
            Goals::from_targets(None, Some(5.0), None, Some(1000.0), Duration::from_mins(1));
        let b = bounds();
        // Freshness shrink lever exhausted (mem-tier at floor), so the reserve is the
        // lever in play (mirrors the sibling test's setup).
        let buffers_maxed = ActuatorValues {
            inline_flush_max_bytes: b.inline_flush_max_bytes.1,
            mem_tier_max_bytes: b.mem_tier_max_bytes.0,
            ..actuators()
        };

        // GROW: freshness behind (60s) + CPU contended (0.95) + QPH comfortably met
        // (1600 ≥ 1.5×1000 ⇒ headroom) ⇒ shed queries for the apply. The exact
        // "hitting QPH, missing lag ⇒ redirect to ingest" case.
        let behind_qph_headroom = IngestSnapshot {
            freshness_secs: Some(60.0),
            cpu_pressure: Some(0.95),
            qph: Some(1600.0),
            ..snap()
        };
        let adj = decide_with_goals(
            &behind_qph_headroom,
            &buffers_maxed,
            &b,
            ms(60_000),
            ms(30_000),
            0,
            &goals,
        )
        .expect("lag behind + CPU contended + QPH headroom ⇒ reserve query slots");
        assert_eq!(adj.actuator, Actuator::QueryAdmissionReserve);
        assert!(
            adj.new_value > 0,
            "grows while QPH has headroom (got {})",
            adj.new_value
        );

        // HOLD (headroom spent): same lag + CPU, but QPH now in the deadband
        // (1100: 0.8×1000=800 ≤ 1100 < 1.5×1000=1500 ⇒ neither met nor violated).
        // The reserve must NOT keep growing — that would eat into the QPH SLO.
        let behind_qph_deadband = IngestSnapshot {
            freshness_secs: Some(60.0),
            cpu_pressure: Some(0.95),
            qph: Some(1100.0),
            ..snap()
        };
        let adj = decide_with_goals(
            &behind_qph_deadband,
            &buffers_maxed,
            &b,
            ms(60_000),
            ms(30_000),
            0,
            &goals,
        );
        assert!(
            !matches!(adj, Some(a) if a.actuator == Actuator::QueryAdmissionReserve && a.new_value > 0),
            "must NOT grow the reserve once QPH headroom is spent (got {adj:?})"
        );

        // BRAKE (QPH violated): a reserve is held, lag still behind, CPU still
        // contended — but throttling has pushed QPH below target (700 < 0.8×1000=800
        // ⇒ violated). Tier (1b) releases to stop borrowing past the QPH SLO, and
        // wins over the query-health tier. This is the safe direction (release ON
        // violated), NOT the self-perpetuation trap (release on met).
        let reserve_held = ActuatorValues {
            query_admission_reserve: 3,
            ..actuators()
        };
        let behind_qph_violated = IngestSnapshot {
            freshness_secs: Some(60.0),
            cpu_pressure: Some(0.95),
            qph: Some(700.0),
            ..snap()
        };
        let adj = decide_with_goals(
            &behind_qph_violated,
            &reserve_held,
            &b,
            ms(60_000),
            ms(30_000),
            0,
            &goals,
        )
        .expect("QPH violated ⇒ release to honor the query SLO");
        assert_eq!(adj.actuator, Actuator::QueryAdmissionReserve);
        assert!(
            (adj.new_value as usize) < 3,
            "released to stop breaching the QPH SLO (got {})",
            adj.new_value
        );
    }

    #[test]
    fn binding_constraint_classifies_the_bottleneck() {
        // Memory dominates (the hard #1).
        let s = IngestSnapshot {
            mem_pressure: Some(0.95),
            ..snap()
        };
        assert!(binding_constraint(&s).contains("memory"));
        // Slow-tier earlier-drain gate: a confirmed slow tier (EBS) is memory-bound
        // at `MEM_PRESSURE_OK - SLOW_TIER_MEM_DRAIN_OFFSET` (0.68) — the same gate the
        // controller blocks buffer growth at — whereas the Unknown default keeps the
        // standard 0.75 gate and shrugs the identical pressure off.
        let slow = IngestSnapshot {
            mem_pressure: Some(0.70),
            data_storage: StorageClass::Ebs,
            ..snap()
        };
        assert!(
            binding_constraint(&slow).contains("memory"),
            "confirmed slow tier binds on memory at the shifted gate"
        );
        let unknown = IngestSnapshot {
            mem_pressure: Some(0.70),
            ..snap()
        };
        assert!(
            !binding_constraint(&unknown).contains("memory"),
            "the Unknown default keeps the standard 0.75 memory gate"
        );
        // Boundary: exactly at the shifted gate still binds on memory (`>=`,
        // matching the controller's `mem_ok = p < gate`), not the tier below.
        let at_gate = IngestSnapshot {
            mem_pressure: Some(MEM_PRESSURE_OK - SLOW_TIER_MEM_DRAIN_OFFSET),
            data_storage: StorageClass::Ebs,
            ..snap()
        };
        assert!(
            binding_constraint(&at_gate).contains("memory"),
            "memory binds at exactly the shifted gate"
        );
        // CPU next.
        let s = IngestSnapshot {
            cpu_pressure: Some(0.9),
            ..snap()
        };
        assert!(binding_constraint(&s).contains("CPU"));
        // Slow-tier write path → storage/EBS.
        let s = IngestSnapshot {
            data_storage: StorageClass::Ebs,
            io_latency_ms: Some(80.0),
            arrival_gap_ms: 100.0,
            ..snap()
        };
        let c = binding_constraint(&s);
        assert!(c.contains("EBS") || c.contains("storage"));
        // Nothing saturated → the actuator bounds themselves are the limit.
        assert!(binding_constraint(&snap()).contains("actuator"));
    }

    #[test]
    fn slow_storage_amortizes_where_fast_storage_tolerates() {
        let b = bounds();
        let cur = actuators();
        let mut s = snap();
        s.io_latency_ms = Some(30.0); // 30% of the 100 ms offered-load window

        // EBS: the bar is halved (25%) → io-bound → amortize via a larger memtable.
        s.data_storage = StorageClass::Ebs;
        assert!(
            matches!(
                decide_fresh(&s, &cur, &b),
                Some(adj) if adj.actuator == Actuator::InlineFlushBytes
            ),
            "EBS tolerates less per-batch write latency → grow the memtable"
        );

        // Local SSD: 30% is under the 50% bar → not io-bound, no io-driven growth.
        s.data_storage = StorageClass::LocalSsd;
        assert!(
            !matches!(
                decide_fresh(&s, &cur, &b),
                Some(adj) if adj.actuator == Actuator::InlineFlushBytes
            ),
            "local SSD tolerates 30% write latency → no I/O-bound amortization"
        );
    }

    #[test]
    fn lag_goal_on_slow_storage_prefers_buffers_over_shards() {
        // The production path (goals active). With buffers maxed, a violated lag
        // goal reaches the write-shard lever — but on a slow tier more shards mean
        // more parallel slow uploads + more small files. Tier is the only variable.
        let b = bounds();
        let cur = ActuatorValues {
            inline_flush_max_bytes: b.inline_flush_max_bytes.1,
            mem_tier_max_bytes: b.mem_tier_max_bytes.1,
            write_concurrency: 4,
            ..actuators()
        };
        let mut s = snap();
        s.replication_lag_secs = Some(60.0); // 12× over the 5 s goal
        s.io_latency_ms = Some(30.0); // io-bound only once the bar is halved

        // Local SSD: 30% is under the bar → the lag tier raises write concurrency.
        s.data_storage = StorageClass::LocalSsd;
        assert!(
            matches!(
                goal_decide(&s, &cur, &b, &lag_goal(5.0)),
                Some(adj) if adj.actuator == Actuator::WriteConcurrency
            ),
            "fast tier: lag-violated + buffers maxed → add a shard"
        );

        // EBS: the halved bar trips io-bound → withhold the shard, compact instead.
        s.data_storage = StorageClass::Ebs;
        assert!(
            matches!(
                goal_decide(&s, &cur, &b, &lag_goal(5.0)),
                Some(adj) if adj.actuator != Actuator::WriteConcurrency
            ),
            "slow tier: no shard growth — lean on buffers/compaction"
        );
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
