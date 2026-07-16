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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{LazyLock, OnceLock};
use std::time::{Duration, Instant};

use parking_lot::Mutex;

use super::mem_tier_budget::{global_mem_tier_total, global_mem_tier_used};
use super::write_budget::encode_budget_snapshot;

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
}

/// RAII handle for a long-running Cayenne operation tracked by the stall
/// watchdog. Advance [`StallOp::phase`] at each stage boundary; drop removes the
/// entry (covering `?`, early return, and panic unwinding).
pub(crate) struct StallOp {
    id: u64,
}

impl StallOp {
    /// Register a new in-flight operation. `kind` is a stable `&'static str`
    /// classifier (e.g. `"cold-promotion"`, `"ingest-memory-write"`).
    pub(crate) fn begin(table: &str, kind: &'static str) -> Self {
        ensure_watchdog();
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        let now = Instant::now();
        REGISTRY.lock().insert(
            id,
            OpEntry {
                table: table.to_string(),
                kind,
                phase: "start",
                started: now,
                phase_since: now,
            },
        );
        Self { id }
    }

    /// Advance to a new phase, resetting the stall timer for this operation.
    pub(crate) fn phase(&self, phase: &'static str) {
        if let Some(entry) = REGISTRY.lock().get_mut(&self.id) {
            entry.phase = phase;
            entry.phase_since = Instant::now();
        }
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
        // slow subscriber never extends the hot-path critical section.
        let stuck: Vec<(String, &'static str, &'static str, u64, u64)> = {
            let registry = REGISTRY.lock();
            registry
                .values()
                .filter_map(|entry| {
                    let in_phase = now.saturating_duration_since(entry.phase_since);
                    if in_phase >= warn_after {
                        Some((
                            entry.table.clone(),
                            entry.kind,
                            entry.phase,
                            in_phase.as_secs(),
                            now.saturating_duration_since(entry.started).as_secs(),
                        ))
                    } else {
                        None
                    }
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

        for (table, kind, phase, in_phase_s, total_s) in stuck {
            tracing::warn!(
                target: "cayenne::stall",
                table = %table,
                kind,
                phase,
                in_phase_s,
                total_s,
                mem_tier_used = ?mem_used,
                mem_tier_total = ?mem_total,
                encode_permits_available = ?encode_avail,
                encode_permits_total = ?encode_total,
                "Cayenne operation has not advanced its phase — possible stall/deadlock (write_lock likely held; ingest for this table is blocked behind it)"
            );
        }
    }
}
