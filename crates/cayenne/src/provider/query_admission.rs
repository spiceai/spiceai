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

//! Process-global query-admission throttle, driven by the per-table adaptive
//! CDC controller.
//!
//! The runtime already bounds concurrently-executing analytical queries with a
//! single count-based admission `Semaphore`
//! (`runtime.query.max_concurrent_queries`). This governor lets the closed-loop
//! CDC tuner SHED some of that concurrency when a `cdc_durability: memory` table
//! is behind its freshness / replication-lag SLO **and CPU is the contended
//! resource** — handing cores back to the CDC apply so it can keep up — then
//! restore it when the table catches up. Count-based admission (whole queries,
//! never partitions) is deadlock-safe: a query either runs with all its
//! parallelism or waits, so throttling can never wedge a partially-admitted plan.
//!
//! ## Mechanism: held permits (reversible), NOT `forget_permits`
//!
//! The throttle holds live `OwnedSemaphorePermit`s on the SAME `Arc<Semaphore>`
//! the runtime acquires from: while the governor holds `R` permits, at most
//! `max - R` queries run concurrently. The query path (`query.rs`) is unchanged —
//! it just finds fewer permits. Releasing is a `Vec::pop` (drop), which returns
//! the permit instantly. Growing uses NON-BLOCKING `try_acquire_owned`, grabbing
//! only what is free right now and converging over later ticks as running queries
//! finish — it never waits, so a tick can't stall behind a long query.
//!
//! This deliberately differs from [`super::write_budget`], which is static
//! precisely because dynamic resizing there was unsafe: (1) `forget_permits` can
//! only remove *available* permits and cannot be un-forgotten, and a pending
//! `acquire_many` clamped to the old cap could stall forever; (2) uncoordinated
//! per-table controllers mutating one global semaphore would break the
//! one-bounded-move-per-tick safety design. Held permits sidestep BOTH: they are
//! fully reversible (drop restores capacity) and never block; and the governor is
//! the SINGLE mutator — per-table controllers only REPORT their reserve demand,
//! and the governor reconciles to the MAX across tables (the most-behind table
//! wins). Each per-table move is one bounded step per tick, so the governor only
//! ever adjusts its held set by that bounded delta.
//!
//! When unset (unit tests, embedders that don't wire it up) every entry point is
//! a no-op, preserving the prior unbounded behavior.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use parking_lot::Mutex;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

struct QueryAdmissionGovernor {
    /// The SAME admission semaphore the runtime query path acquires from. Holding
    /// permits on it reduces what queries can acquire (an `Arc` clone of the
    /// runtime's handle, so both observe the same available count).
    semaphore: Arc<Semaphore>,
    /// The total admission permit count at install time (no queries running yet,
    /// so it equals the semaphore's capacity). Used only for the `max - 1` clamp
    /// below, never to resize the semaphore.
    max: usize,
    /// Permits currently held to throttle queries. `held.len()` is the live
    /// reserve; dropping a permit (pop) instantly returns it to the query pool.
    held: Vec<OwnedSemaphorePermit>,
    /// Each memory-mode table's current reserve demand (its
    /// `query_admission_reserve` actuator). The effective reserve is the MAX over
    /// tables — the most-behind table's demand wins; a caught-up table reporting 0
    /// never lowers a lagging table's throttle.
    per_table_demand: HashMap<Arc<str>, usize>,
}

impl QueryAdmissionGovernor {
    /// Reconcile `held` to the current demand. Grows with non-blocking
    /// `try_acquire_owned` (takes only free permits now; converges as queries
    /// finish) and shrinks by dropping held permits (instant release). Always
    /// leaves at least one query slot (`max - 1`) so queries are never fully
    /// starved by the throttle.
    fn reconcile_held(&mut self) {
        let demand = self.per_table_demand.values().copied().max().unwrap_or(0);
        let target = demand.min(self.max.saturating_sub(1));
        let before = self.held.len();
        while self.held.len() < target {
            match Arc::clone(&self.semaphore).try_acquire_owned() {
                Ok(permit) => self.held.push(permit),
                // No permit free right now (queries hold them) — stop. A later tick
                // re-runs this and grabs more as queries release. Never blocks.
                Err(_) => break,
            }
        }
        while self.held.len() > target {
            // Drop one held permit → instantly available to queries again.
            self.held.pop();
        }
        if self.held.len() != before {
            tracing::debug!(
                target: "cayenne::query_admission",
                reserved = self.held.len(),
                target_reserve = target,
                max = self.max,
                "Adjusted reserved query-admission permits (CDC-apply throttle)"
            );
        }
    }
}

/// Process-wide query-admission governor, injected once at startup by the runtime
/// with the admission semaphore + its permit count. Replaceable so a test binary
/// that builds and drops multiple runtimes does not retain a stale handle
/// (replacing drops the old governor, releasing any permits it held).
static GLOBAL_QUERY_ADMISSION: LazyLock<Mutex<Option<QueryAdmissionGovernor>>> =
    LazyLock::new(|| Mutex::new(None));

/// Install (or replace) the process-global query-admission governor. Called once
/// at startup with the runtime's admission `Semaphore` and its permit count
/// (`max`). At startup no queries are running, so the caller passes
/// `semaphore.available_permits()` as `max`. A later call replaces the previous
/// governor, dropping it (and releasing every permit it held).
pub fn set_query_admission_governor(semaphore: Arc<Semaphore>, max: usize) {
    let governor = QueryAdmissionGovernor {
        semaphore,
        max,
        held: Vec::new(),
        per_table_demand: HashMap::new(),
    };
    *GLOBAL_QUERY_ADMISSION.lock() = Some(governor);
}

/// Report `table`'s current query-admission reserve demand (its
/// `query_admission_reserve` actuator value) to the governor and reconcile the
/// held permits to the new MAX across all tables. Idempotent and cheap; safe to
/// call every background tick (the reconcile re-tries any permits that were busy
/// last time). A no-op when no governor is installed.
pub(crate) fn set_table_admission_reserve(table: &str, demand: usize) {
    let mut guard = GLOBAL_QUERY_ADMISSION.lock();
    let Some(governor) = guard.as_mut() else {
        return;
    };
    // Update in place when the table is already tracked (the steady-state case —
    // avoids re-allocating the `Arc<str>` key every tick).
    if let Some(slot) = governor.per_table_demand.get_mut(table) {
        *slot = demand;
    } else {
        governor.per_table_demand.insert(Arc::from(table), demand);
    }
    governor.reconcile_held();
}

/// The number of query-admission permits currently reserved for CDC apply (held
/// by the governor). `0` when no governor is installed. Test-only accessor; the
/// live held-count is also emitted by `reconcile_held`'s debug log.
#[cfg(test)]
#[must_use]
pub(crate) fn query_admission_reserved() -> usize {
    GLOBAL_QUERY_ADMISSION
        .lock()
        .as_ref()
        .map_or(0, |g| g.held.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    // One test fn (not several) because the governor is process-global: separate
    // `#[test]`s run in parallel and would race the single `GLOBAL_QUERY_ADMISSION`.
    #[test]
    fn governor_reserves_releases_and_clamps() {
        let sem = Arc::new(Semaphore::new(4));
        set_query_admission_governor(Arc::clone(&sem), 4);

        // No demand yet → nothing reserved, all permits available to queries.
        assert_eq!(query_admission_reserved(), 0);
        assert_eq!(sem.available_permits(), 4);

        // A behind table demands 2 → 2 held, 2 left for queries.
        set_table_admission_reserve("order_line", 2);
        assert_eq!(query_admission_reserved(), 2);
        assert_eq!(sem.available_permits(), 2);

        // A second, less-behind table: the MAX wins (most-behind table), not the sum.
        set_table_admission_reserve("stock", 1);
        assert_eq!(query_admission_reserved(), 2, "max(2,1) = 2, not the sum");
        assert_eq!(sem.available_permits(), 2);

        // The lagging table catches up (demand 0); the other still wants 1.
        set_table_admission_reserve("order_line", 0);
        assert_eq!(query_admission_reserved(), 1, "now max(0,1) = 1");
        assert_eq!(sem.available_permits(), 3);

        // A huge demand is clamped to max-1 so queries are never fully starved.
        set_table_admission_reserve("order_line", 100);
        assert_eq!(query_admission_reserved(), 3, "clamped to max-1 = 3");
        assert_eq!(sem.available_permits(), 1, "always >= 1 query slot");

        // Everyone caught up → all permits released back to queries.
        set_table_admission_reserve("order_line", 0);
        set_table_admission_reserve("stock", 0);
        assert_eq!(query_admission_reserved(), 0);
        assert_eq!(sem.available_permits(), 4);

        // Re-install (test-binary semantics): old governor dropped, state cleared.
        let sem2 = Arc::new(Semaphore::new(2));
        set_query_admission_governor(Arc::clone(&sem2), 2);
        assert_eq!(query_admission_reserved(), 0);
        assert_eq!(
            sem.available_permits(),
            4,
            "old semaphore's held permits freed"
        );
    }
}
