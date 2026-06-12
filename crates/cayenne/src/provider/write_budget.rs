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

//! Process-global encode-concurrency budget shared across all Cayenne tables.
//!
//! Per-table `cayenne_write_concurrency` sizes each table's intra-write encode
//! sharding *in isolation* — the unset default is conservative, but it can be
//! raised per table. Under a fleet of tables receiving CDC simultaneously the
//! per-table values simply SUM, with nothing coordinating them: e.g.
//! `order_line` 48, `stock` 32,
//! `customer` 32, … easily exceeds the core count, so independent datasets
//! oversubscribe the machine — contending for cores, encode-buffer memory, and
//! the query threads. Compaction already guards against exactly this with a
//! shared semaphore (see [`super::compaction::BackgroundCompactor`], "so a fleet
//! of tables can't overwhelm the writer pool"); this is the equivalent global
//! cap for the steady-state write/encode path, which previously had none.
//!
//! A write acquires `min(shards, total)` permits **atomically** before encoding
//! and holds them for the write's duration, so the aggregate number of
//! concurrent encode shards across every table is bounded by `total`. Acquiring
//! all of a write's permits in one call (never one-at-a-time) is what keeps the
//! cap deadlock-free: a write can never hold some permits while waiting for the
//! rest, so independent writes can't form a hold-and-wait cycle.
//!
//! When unset (unit tests, embedders that don't wire it up) acquisition is a
//! no-op and writes proceed ungated, preserving the prior per-table behavior.

use std::sync::{Arc, LazyLock};

use parking_lot::RwLock;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use super::delta_encoding::WriteClass;

/// Floor of the maintenance permit reserve: even the smallest budget keeps a
/// little guaranteed `Delta` headroom (see [`maintenance_permit_reserve`]).
const MAINTENANCE_PERMIT_RESERVE_MIN: usize = 2;

/// Ceiling of the maintenance permit reserve. A mem-tier checkpoint or staged
/// CDC append encodes with a handful of shards, so guaranteed headroom past
/// ~6 permits buys no additional apply-path latency — it would only idle
/// permits that compaction could use to drain read-amp.
const MAINTENANCE_PERMIT_RESERVE_MAX: usize = 6;

/// Encode permits the `Maintenance` class (compaction outputs, sorted
/// rewrites, overwrites) may never hold in aggregate, reserved so `Delta`
/// writes — CDC staged appends and mem-tier checkpoints, whose latency bounds
/// the apply path and the deferred slot ack — always find headroom instead of
/// queueing a whole-tier flush behind a multi-shard compaction encode (the
/// measured 14-41s checkpoint outages that stalled the CDC appliers).
/// Maintenance is throughput-oriented: under contention it merely proceeds
/// narrower.
///
/// Sized with the total budget (~25%, clamped to
/// `[MAINTENANCE_PERMIT_RESERVE_MIN, MAINTENANCE_PERMIT_RESERVE_MAX]`) at
/// install time: a flat reserve was proportionally huge on a 4-core container
/// (starving maintenance) and negligible on a 96-permit host (one fleet-wide
/// checkpoint wave could still queue). The reserve is deliberately **static
/// per install, not adaptively resized**: (1) the budget is process-global
/// while the closed-loop tuner runs one controller *per table* — uncoordinated
/// per-table controllers mutating one global semaphore would break the
/// one-bounded-move-per-tick safety design; (2) shrinking a tokio `Semaphore`
/// (`forget_permits`) only takes effect as held permits return, and a pending
/// `acquire_many` clamped against the *old* gate capacity could exceed the new
/// capacity forever (a permanent stall), so correct dynamic resizing needs
/// resize/clamp coordination that isn't justified by the bounded benefit;
/// (3) the failure mode this guards (a checkpoint queued behind compaction) is
/// already observable per-write via the `inmemory_spill`/write-phase metrics
/// if the sizing ever needs revisiting.
fn maintenance_permit_reserve(total: usize) -> usize {
    (total / 4).clamp(
        MAINTENANCE_PERMIT_RESERVE_MIN,
        MAINTENANCE_PERMIT_RESERVE_MAX,
    )
}

/// Aggregate permits the `Maintenance` class may hold for a budget of `total`:
/// `total - reserve`, floored at 1 so maintenance always makes progress. The
/// single source of truth for the gate size — used both when the gate
/// semaphore is created and when a request is clamped against it, so the two
/// can never disagree.
fn maintenance_gate_cap(total: usize) -> usize {
    total
        .saturating_sub(maintenance_permit_reserve(total))
        .max(1)
}

#[derive(Clone)]
struct EncodeBudget {
    semaphore: Arc<Semaphore>,
    /// Class gate sized [`maintenance_gate_cap`] (`total - reserve`, min 1). A
    /// `Maintenance` write holds gate permits 1:1 alongside its main permits,
    /// so maintenance writes COLLECTIVELY can never occupy the reserved
    /// slice — a per-request clamp alone would still let two maintenance
    /// writes sum to the full budget. Acquisition order is uniform (gate,
    /// then main) and `Delta` never touches the gate, so no hold-and-wait
    /// cycle exists across classes.
    maintenance_gate: Arc<Semaphore>,
    total: usize,
}

/// Permits held for one write's encode, released together on drop.
pub(crate) struct EncodePermits {
    _main: OwnedSemaphorePermit,
    _gate: Option<OwnedSemaphorePermit>,
}

/// Process-wide encode budget, injected once at startup by the binary (sized to
/// the host core budget). Replaceable so a test binary that builds and drops
/// multiple runtimes does not retain a stale semaphore.
static GLOBAL_ENCODE_BUDGET: LazyLock<RwLock<Option<EncodeBudget>>> =
    LazyLock::new(|| RwLock::new(None));

/// Install the process-global encode-concurrency budget. Called once at startup
/// with the host core budget (optionally minus a reserve for query threads).
/// `permits` is clamped to at least 1. Later calls replace the previous budget.
pub fn set_global_encode_concurrency(permits: usize) {
    let permits = permits.max(1);
    let budget = EncodeBudget {
        semaphore: Arc::new(Semaphore::new(permits)),
        maintenance_gate: Arc::new(Semaphore::new(maintenance_gate_cap(permits))),
        total: permits,
    };
    let mut guard = GLOBAL_ENCODE_BUDGET.write();
    if guard.is_some() {
        tracing::debug!(
            target: "cayenne::write_budget",
            permits,
            "Replacing global encode-concurrency budget"
        );
    }
    *guard = Some(budget);
}

/// Acquire up to `shards` encode permits from the global budget, atomically.
///
/// Returns the held permits (which release on drop, so callers scope them to
/// the write) or `None` when no budget is installed (proceed ungated). `shards`
/// is clamped to `[1, class cap]` so the request is always satisfiable and can
/// never block forever waiting for more permits than the budget can ever hold.
/// `Delta` writes may use the whole budget; `Maintenance` writes are capped to
/// [`maintenance_gate_cap`] in aggregate (see `maintenance_gate`).
pub(crate) async fn acquire_encode_permits(
    shards: usize,
    class: WriteClass,
) -> Option<EncodePermits> {
    let budget = GLOBAL_ENCODE_BUDGET.read().clone()?;
    acquire_from(&budget, shards, class).await
}

/// Acquire `min(shards, class cap)` permits from a specific budget. Extracted
/// from [`acquire_encode_permits`] so the clamp/cap behavior is unit-testable
/// against a local budget, without mutating the process-global state (which would
/// race sibling tests in the same binary).
///
/// Yields `None` only if a semaphore has been closed (teardown); a live
/// budget's semaphores are never closed, so in practice this always returns
/// permits. Returning `None` rather than panicking keeps a shutdown race
/// ungated instead of failing the write.
async fn acquire_from(
    budget: &EncodeBudget,
    shards: usize,
    class: WriteClass,
) -> Option<EncodePermits> {
    let gate = match class {
        WriteClass::Delta => None,
        WriteClass::Maintenance => {
            // Gate first (uniform order; see `maintenance_gate` docs). The gate
            // is sized below `total`, so the subsequent main acquisition of the
            // same count can always be satisfied once delta holders release.
            let gate_cap = maintenance_gate_cap(budget.total);
            let permits = u32::try_from(shards.clamp(1, gate_cap)).unwrap_or(u32::MAX);
            Some(
                Arc::clone(&budget.maintenance_gate)
                    .acquire_many_owned(permits)
                    .await
                    .ok()?,
            )
        }
    };
    let main_count = gate.as_ref().map_or_else(
        || shards.clamp(1, budget.total),
        |g| -> usize { g.num_permits().max(1) },
    );
    let main_count = u32::try_from(main_count).unwrap_or(u32::MAX);
    let main = Arc::clone(&budget.semaphore)
        .acquire_many_owned(main_count)
        .await
        .ok()?;
    Some(EncodePermits {
        _main: main,
        _gate: gate,
    })
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn budget(total: usize) -> EncodeBudget {
        EncodeBudget {
            semaphore: Arc::new(Semaphore::new(total)),
            maintenance_gate: Arc::new(Semaphore::new(maintenance_gate_cap(total))),
            total,
        }
    }

    /// The reserve scales with the total (~25%) within `[2, 6]`, and the gate
    /// never collapses to zero — pinning the whole derivation curve so the
    /// install-time sizing and the per-acquire clamp stay coherent on every
    /// budget size.
    #[test]
    fn maintenance_reserve_scales_with_total() {
        for (total, expected_reserve) in [
            (1, 2), // floor dominates tiny budgets...
            (2, 2), // ...where the gate floor of 1 takes over below
            (4, 2),
            (8, 2),
            (12, 3),
            (16, 4),
            (24, 6),
            (48, 6), // ceiling: guaranteed delta headroom needn't scale forever
            (96, 6),
        ] {
            assert_eq!(
                maintenance_permit_reserve(total),
                expected_reserve,
                "reserve for total={total}"
            );
            let gate = maintenance_gate_cap(total);
            assert!(gate >= 1, "gate floor for total={total}");
            assert!(
                gate <= total.max(1),
                "gate never exceeds the budget (total={total})"
            );
            if total > expected_reserve {
                assert_eq!(gate, total - expected_reserve, "gate for total={total}");
            }
        }
    }

    /// A request for more shards than the budget holds is clamped to the total,
    /// so it is always satisfiable rather than blocking forever.
    #[tokio::test]
    async fn acquire_clamps_to_total() {
        let b = budget(4);
        let _permit = acquire_from(&b, 16, WriteClass::Delta).await; // 16 clamped to 4
        assert_eq!(
            b.semaphore.available_permits(),
            0,
            "an over-large request takes all permits (clamped), never more"
        );
    }

    /// A request below the total takes exactly that many permits.
    #[tokio::test]
    async fn acquire_takes_requested_when_under_total() {
        let b = budget(8);
        let _permit = acquire_from(&b, 3, WriteClass::Delta).await;
        assert_eq!(b.semaphore.available_permits(), 5, "took exactly 3 of 8");
    }

    /// The budget caps aggregate concurrency: with `total` permits held, a
    /// further acquire blocks until one is released.
    #[tokio::test]
    async fn budget_caps_aggregate_concurrency() {
        let b = budget(2);
        let held = acquire_from(&b, 2, WriteClass::Delta).await; // holds all 2
        let pending = acquire_from(&b, 1, WriteClass::Delta);
        tokio::pin!(pending);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut pending)
                .await
                .is_err(),
            "acquire must block while the budget is fully held"
        );
        drop(held); // release — the waiter can now proceed.
        tokio::time::timeout(Duration::from_millis(500), &mut pending)
            .await
            .expect("waiter proceeds after a permit is released");
    }

    /// Maintenance writes can never exhaust the budget: even an over-large
    /// maintenance request leaves the derived reserve of main permits for
    /// delta writes, which then acquire without blocking.
    #[tokio::test]
    async fn maintenance_leaves_delta_reserve() {
        let total = 12;
        let reserve = maintenance_permit_reserve(total);
        let b = budget(total);
        let _maint = acquire_from(&b, 16, WriteClass::Maintenance).await; // clamped to the gate
        assert_eq!(
            b.semaphore.available_permits(),
            reserve,
            "maintenance is capped to total - reserve"
        );
        let delta = acquire_from(&b, reserve, WriteClass::Delta);
        tokio::pin!(delta);
        tokio::time::timeout(Duration::from_millis(500), &mut delta)
            .await
            .expect("a delta write acquires the reserved permits without waiting");
    }

    /// Two concurrent maintenance writes cannot SUM past the maintenance cap:
    /// the second queues on the gate while the first holds it, so the reserve
    /// stays available to delta writes throughout.
    #[tokio::test]
    async fn maintenance_aggregate_capped_by_gate() {
        let total = 12;
        let gate = maintenance_gate_cap(total);
        let b = budget(total);
        let held = acquire_from(&b, gate, WriteClass::Maintenance).await; // gate exhausted
        let second = acquire_from(&b, 1, WriteClass::Maintenance);
        tokio::pin!(second);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut second)
                .await
                .is_err(),
            "a second maintenance write must queue on the gate, not take the reserve"
        );
        assert_eq!(
            b.semaphore.available_permits(),
            maintenance_permit_reserve(total),
            "the reserve is untouched while maintenance queues"
        );
        drop(held);
        tokio::time::timeout(Duration::from_millis(500), &mut second)
            .await
            .expect("the queued maintenance write proceeds once the gate frees");
    }

    /// A tiny budget still admits maintenance (gate floor of 1) — the reserve
    /// never starves maintenance entirely.
    #[tokio::test]
    async fn tiny_budget_still_admits_maintenance() {
        let b = budget(2); // reserve = 2 → gate = max(2 - 2, 1) = 1
        let permits = acquire_from(&b, 4, WriteClass::Maintenance).await;
        assert!(
            permits.is_some(),
            "maintenance proceeds (narrower) on tiny budgets"
        );
        assert_eq!(
            b.semaphore.available_permits(),
            1,
            "maintenance took the single gated permit"
        );
    }

    /// With no global budget installed, acquisition is a no-op (`None`) and
    /// writes proceed ungated. No test mutates the global, so it is always unset
    /// here — keeping this free of cross-test interference.
    #[tokio::test]
    async fn acquire_encode_permits_is_noop_when_unset() {
        assert!(acquire_encode_permits(8, WriteClass::Delta).await.is_none());
    }
}
