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

//! Cross-runtime stall diagnostics for long-held Cayenne operations.
//!
//! Motivation: cold-tier promotion holds the per-table `write_lock` across the
//! whole graduation (drain → checkpoint → scan → Z-order sort → encode → upload
//! → commit). When any of those stages wedges, the table's own CDC ingest
//! blocks behind the held lock and the runtime reports "not ready" forever with
//! **no further log output** — the failure we observed on the SF1000 cold
//! chbench run (`order_line` initial-snapshot bootstrap never completed; ~38
//! minutes of total silence before the ready-wait timeout).
//!
//! A tokio-task watchdog cannot diagnose that: the same runtime starvation /
//! lock contention that causes the wedge can also prevent the watchdog task from
//! ever being polled. So this watchdog runs on a **dedicated OS thread** that is
//! never scheduled by either tokio runtime, and reads a process-global registry
//! of in-flight operations.
//!
//! Each long operation opens a [`StallOp`] (an RAII handle) and advances its
//! [`StallOp::phase`] as it progresses. The watchdog thread wakes every
//! `CAYENNE_STALL_WATCHDOG_SECS` and emits one `WARN` per operation whose
//! current phase has not advanced within `CAYENNE_STALL_WATCHDOG_WARN_SECS`,
//! alongside the global mem-tier and encode-budget occupancy so a
//! budget-exhaustion deadlock is visible at a glance. Dropping the handle
//! (including on any `?`/early return) removes the entry, so a healthy op that
//! finishes before the threshold never warns.
//!
//! This is diagnostics only: it never changes locking or control flow. Enabled
//! by default; set `CAYENNE_STALL_WATCHDOG_SECS=0` to disable.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{LazyLock, OnceLock};
use std::time::{Duration, Instant};

use datafusion_execution::memory_pool::MemoryLimit;
use parking_lot::Mutex;

use super::mem_tier_budget::{global_mem_tier_total, global_mem_tier_used};
use super::write_budget::encode_budget_snapshot;

/// Reserved / limit bytes of the dedicated compaction memory pool (the pool the
/// cold-promotion Z-order `SortExec` draws from), or `(None, None)` when no
/// compaction runtime is installed. Reserved approaching the limit is the direct
/// signal that promotion sorts are spilling under memory pressure.
fn compaction_pool_usage() -> (Option<u64>, Option<u64>) {
    let Some(env) = super::compaction::compaction_runtime_env() else {
        return (None, None);
    };
    let used = u64::try_from(env.memory_pool.reserved()).ok();
    let total = match env.memory_pool.memory_limit() {
        MemoryLimit::Finite(limit) => u64::try_from(limit).ok(),
        MemoryLimit::Infinite | MemoryLimit::Unknown => None,
    };
    (used, total)
}

/// Monotonic id source for registry entries (avoids `Instant`/random keys).
static NEXT_ID: AtomicU64 = AtomicU64::new(1);

/// In-flight operations keyed by id. Guarded by a `parking_lot::Mutex`; the
/// critical sections are a single map insert/update/remove, so contention with
/// the ingest hot path is negligible (one op per write, not per row).
static REGISTRY: LazyLock<Mutex<HashMap<u64, OpEntry>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Set once when the watchdog thread has been started (or deliberately skipped
/// when disabled), so `ensure_watchdog` spawns at most one thread.
static WATCHDOG: OnceLock<()> = OnceLock::new();

const WATCHDOG_INTERVAL_ENV: &str = "CAYENNE_STALL_WATCHDOG_SECS";
const WATCHDOG_WARN_ENV: &str = "CAYENNE_STALL_WATCHDOG_WARN_SECS";
const DEFAULT_INTERVAL_SECS: u64 = 30;
const DEFAULT_WARN_SECS: u64 = 90;

struct OpEntry {
    table: String,
    kind: &'static str,
    phase: &'static str,
    started: Instant,
    /// When the CURRENT phase began — resets on every [`StallOp::phase`] so a
    /// long op that keeps making progress (advancing phases) never warns.
    phase_since: Instant,
    /// Optional monotonic progress counter (e.g. rows delivered to the cold
    /// sink). Lets the watchdog distinguish a truly-parked op from one that is
    /// slow-but-advancing even while its phase label is unchanged.
    progress: Arc<AtomicU64>,
    /// Progress value observed on the previous watchdog tick, for delta logging.
    last_progress: u64,
    /// Optional monotonic INPUT-side counter (e.g. rows the scan has fed into the
    /// promotion's bounded sort). Compared against `progress` (sink side) to
    /// localize a stall: `input` frozen ⇒ scan/upstream parked; `input`
    /// advancing while `progress` frozen ⇒ the sort/sink is parked downstream.
    input_progress: Arc<AtomicU64>,
    last_input_progress: u64,
    /// Tokio runtime the op was registered on (captured at `begin`), so the
    /// watchdog can request an async task dump of the exact runtime the parked
    /// scan runs on. `None` if `begin` ran outside a runtime.
    runtime: Option<tokio::runtime::Handle>,
    /// Set once a task dump has been emitted for this op, so a sustained stall
    /// dumps only once (dumps re-poll every task and are expensive).
    dumped: bool,
}

/// A stuck operation snapshot, collected under the registry lock and logged
/// outside it.
struct StuckOp {
    table: String,
    kind: &'static str,
    phase: &'static str,
    in_phase_s: u64,
    total_s: u64,
    progress: u64,
    progress_delta: u64,
    input_progress: u64,
    input_delta: u64,
    /// When set, the watchdog emits a one-time tokio task dump for this op after
    /// logging the stall WARN (only when genuinely parked: no input/output
    /// progress this tick).
    runtime: Option<tokio::runtime::Handle>,
    should_dump: bool,
}

/// RAII handle for a long-running Cayenne operation tracked by the stall
/// watchdog. Advance [`StallOp::phase`] at each stage boundary; drop removes the
/// entry (covering `?`, early return, and panic unwinding).
pub(crate) struct StallOp {
    id: u64,
    progress: Arc<AtomicU64>,
    input_progress: Arc<AtomicU64>,
}

impl StallOp {
    /// Register a new in-flight operation. `kind` is a stable `&'static str`
    /// classifier (e.g. `"cold-promotion"`, `"ingest-memory-write"`).
    pub(crate) fn begin(table: &str, kind: &'static str) -> Self {
        ensure_watchdog();
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        let now = Instant::now();
        let progress = Arc::new(AtomicU64::new(0));
        let input_progress = Arc::new(AtomicU64::new(0));
        REGISTRY.lock().insert(
            id,
            OpEntry {
                table: table.to_string(),
                kind,
                phase: "start",
                started: now,
                phase_since: now,
                progress: Arc::clone(&progress),
                last_progress: 0,
                input_progress: Arc::clone(&input_progress),
                last_input_progress: 0,
                runtime: tokio::runtime::Handle::try_current().ok(),
                dumped: false,
            },
        );
        Self {
            id,
            progress,
            input_progress,
        }
    }

    /// Advance to a new phase, resetting the stall timer for this operation.
    pub(crate) fn phase(&self, phase: &'static str) {
        if let Some(entry) = REGISTRY.lock().get_mut(&self.id) {
            entry.phase = phase;
            entry.phase_since = Instant::now();
        }
    }

    /// Shared monotonic progress counter for this op. Bump it (e.g. by rows
    /// delivered) so the watchdog can report throughput and tell a parked op
    /// apart from a slow-but-advancing one. Cheap `Relaxed` atomic adds.
    pub(crate) fn progress_counter(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.progress)
    }

    /// Shared INPUT-side counter (rows fed into the op's sort). Compared against
    /// [`Self::progress_counter`] by the watchdog to localize a stall as
    /// upstream (scan) vs downstream (sort/sink).
    pub(crate) fn input_progress_counter(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.input_progress)
    }
}

impl Drop for StallOp {
    fn drop(&mut self) {
        REGISTRY.lock().remove(&self.id);
    }
}

fn env_secs(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(default)
}

/// Start the watchdog OS thread exactly once. Cheap no-op on every call after
/// the first (a single `OnceLock` check).
fn ensure_watchdog() {
    if WATCHDOG.get().is_some() {
        return;
    }
    // Whichever caller wins the race spawns the thread; losers return.
    if WATCHDOG.set(()).is_err() {
        return;
    }

    let interval = env_secs(WATCHDOG_INTERVAL_ENV, DEFAULT_INTERVAL_SECS);
    if interval == 0 {
        tracing::info!(
            target: "cayenne::stall",
            "Cayenne stall watchdog disabled (CAYENNE_STALL_WATCHDOG_SECS=0)"
        );
        return;
    }
    let warn_after = env_secs(WATCHDOG_WARN_ENV, DEFAULT_WARN_SECS);
    let interval = Duration::from_secs(interval);
    let warn_after = Duration::from_secs(warn_after);

    let spawned = std::thread::Builder::new()
        .name("cayenne-stall-watchdog".to_string())
        .spawn(move || watchdog_loop(interval, warn_after));

    match spawned {
        Ok(_) => tracing::info!(
            target: "cayenne::stall",
            interval_s = interval.as_secs(),
            warn_after_s = warn_after.as_secs(),
            "Cayenne stall watchdog started"
        ),
        Err(error) => tracing::warn!(
            target: "cayenne::stall",
            %error,
            "Failed to start Cayenne stall watchdog thread; stall diagnostics unavailable"
        ),
    }
}

fn watchdog_loop(interval: Duration, warn_after: Duration) {
    loop {
        std::thread::sleep(interval);
        let now = Instant::now();

        // Snapshot the stuck entries under the lock, then log outside it so a
        // slow subscriber never extends the hot-path critical section. Updates
        // `last_progress` in place, so this needs a mutable borrow.
        let stuck: Vec<StuckOp> = {
            let mut registry = REGISTRY.lock();
            registry
                .values_mut()
                .filter_map(|entry| {
                    let in_phase = now.saturating_duration_since(entry.phase_since);
                    if in_phase < warn_after {
                        return None;
                    }
                    let progress = entry.progress.load(Ordering::Relaxed);
                    let delta = progress.saturating_sub(entry.last_progress);
                    entry.last_progress = progress;
                    let input_progress = entry.input_progress.load(Ordering::Relaxed);
                    let input_delta = input_progress.saturating_sub(entry.last_input_progress);
                    entry.last_input_progress = input_progress;
                    // Dump once, only when genuinely parked (no forward progress
                    // on either side this tick) — that's the deadlock signature and
                    // the case whose await backtraces we need.
                    let parked = delta == 0 && input_delta == 0;
                    let should_dump = parked && !entry.dumped;
                    if should_dump {
                        entry.dumped = true;
                    }
                    Some(StuckOp {
                        table: entry.table.clone(),
                        kind: entry.kind,
                        phase: entry.phase,
                        in_phase_s: in_phase.as_secs(),
                        total_s: now.saturating_duration_since(entry.started).as_secs(),
                        progress,
                        progress_delta: delta,
                        input_progress,
                        input_delta,
                        runtime: entry.runtime.clone(),
                        should_dump,
                    })
                })
                .collect()
        };

        if stuck.is_empty() {
            continue;
        }

        // Global budgets are the prime suspects for a promotion/ingest deadlock;
        // capture them once per tick for the whole stuck set.
        let mem_used = global_mem_tier_used();
        let mem_total = global_mem_tier_total();
        let encode = encode_budget_snapshot();
        let (encode_avail, encode_total) = encode
            .as_ref()
            .map_or((None, None), |s| (Some(s.available), Some(s.total)));
        let (compaction_pool_used, compaction_pool_total) = compaction_pool_usage();

        for op in stuck {
            tracing::warn!(
                target: "cayenne::stall",
                table = %op.table,
                kind = op.kind,
                phase = op.phase,
                in_phase_s = op.in_phase_s,
                total_s = op.total_s,
                progress = op.progress,
                progress_delta_tick = op.progress_delta,
                input_rows = op.input_progress,
                input_delta_tick = op.input_delta,
                mem_tier_used = ?mem_used,
                mem_tier_total = ?mem_total,
                encode_permits_available = ?encode_avail,
                encode_permits_total = ?encode_total,
                compaction_pool_used = ?compaction_pool_used,
                compaction_pool_total = ?compaction_pool_total,
                "Cayenne operation has not advanced its phase — possible stall/deadlock (write_lock likely held; ingest for this table is blocked behind it). input_delta_tick=0 ⇒ scan/upstream parked (no rows into the sort); input advancing while progress_delta_tick=0 ⇒ sort/sink parked downstream; compaction_pool_used near total ⇒ spilling sort"
            );
            if op.should_dump {
                if let Some(handle) = op.runtime.as_ref() {
                    dump_runtime_tasks(handle, &op.table, op.phase);
                }
            }
        }
    }
}

/// Emit a one-time async task dump of `handle`'s runtime when an op is parked,
/// so the parked scan task's exact await backtrace is captured in-process
/// (ptrace is blocked on CI runners). Requires the build to enable
/// `--cfg tokio_unstable`; otherwise this is a no-op that says so. Runs on the
/// watchdog OS thread (never a runtime worker), so `block_on` is safe; the dump
/// is bounded by a timeout because it re-polls every task and could otherwise
/// hang behind a truly wedged worker.
fn dump_runtime_tasks(handle: &tokio::runtime::Handle, table: &str, phase: &'static str) {
    // Tokio task dumps exist only under `--cfg tokio_unstable` + the `taskdump`
    // feature on Linux x86_64/aarch64 (tokio's `cfg_taskdump!`). Match those
    // conditions exactly so every other target compiles the no-op branch.
    #[cfg(all(
        tokio_unstable,
        target_os = "linux",
        any(target_arch = "x86_64", target_arch = "aarch64")
    ))]
    {
        tracing::warn!(
            target: "cayenne::stall",
            %table, phase,
            "Cayenne stall: capturing tokio task dump (await backtraces of all tasks on this runtime)"
        );
        let dump = handle.block_on(async {
            tokio::time::timeout(Duration::from_secs(25), handle.dump()).await
        });
        match dump {
            Ok(dump) => {
                for (idx, task) in dump.tasks().iter().enumerate() {
                    // Flatten the multi-line async backtrace to one log line.
                    let trace = format!("{}", task.trace()).replace('\n', " ⏎ ");
                    tracing::warn!(
                        target: "cayenne::stall",
                        %table, phase, task_idx = idx, task_id = ?task.id(),
                        "STALL TASK DUMP: {trace}"
                    );
                }
            }
            Err(_elapsed) => tracing::warn!(
                target: "cayenne::stall",
                %table, phase,
                "Cayenne stall: tokio task dump timed out after 25s (a worker may be blocked outside an await point)"
            ),
        }
    }
    #[cfg(not(all(
        tokio_unstable,
        target_os = "linux",
        any(target_arch = "x86_64", target_arch = "aarch64")
    )))]
    {
        let _ = (handle, phase);
        tracing::warn!(
            target: "cayenne::stall",
            %table,
            "Cayenne stall: task dump unavailable — rebuild spiced (Linux x86_64/aarch64) with the tokio `taskdump` feature and RUSTFLAGS=\"--cfg tokio_unstable\" to capture await backtraces"
        );
    }
}
