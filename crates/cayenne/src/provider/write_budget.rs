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
//! That argument only holds for writes that are **independent**. Writes into a
//! partitioned table are not: the partition insert path demuxes one input
//! stream into per-partition writes over bounded channels
//! (`runtime_table_partition::insert`), so a child write parked on this budget
//! stalls the demux, which starves the permit-holding sibling writes of input —
//! a hold-and-wait cycle *through the channels* that left partitioned tables
//! permanently unready (spiceai/spiceai#11818). To keep such coupled writers
//! live regardless of partition count, **single-shard writes are exempt from
//! the budget** ([`acquire_for_write`]): a serial write is the pre-budget
//! baseline the budget was never needed for (it exists to cap encode *fan-out*,
//! and aggregate compaction concurrency is separately bounded by the
//! compactor's own semaphore), and an exempt serial write can always drain its
//! input, so the demux always makes progress.
//!
//! When unset (unit tests, embedders that don't wire it up) acquisition is a
//! no-op and writes proceed ungated, preserving the prior per-table behavior.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::Instant;

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
    /// The current budget ceiling, shared (an `Arc<AtomicUsize>` rather than a
    /// plain `usize`) so [`cap_global_encode_concurrency`] can lower it IN
    /// PLACE on the live `semaphore`/`maintenance_gate` — every clone an
    /// in-flight acquirer holds observes the new ceiling. Per-acquire clamps
    /// (`acquire_from`) read this, not a stale snapshot, so a request can never
    /// ask for more permits than the (possibly just-shrunk) budget can ever
    /// hold — closing the "pending `acquire_many` clamped to the old cap stalls
    /// forever" hazard.
    total: Arc<AtomicUsize>,
    /// Permits a cap-shrink still owes on `semaphore`: tokio's `forget_permits`
    /// can only remove *available* permits, so when held permits exceed the new
    /// cap the residual is recorded here and forgotten as those permits return
    /// (drained best-effort in `acquire_from`). This converges the live budget
    /// down to the cap WITHOUT ever re-adding capacity — the prior swap-in of a
    /// fresh full-size semaphore left old held permits running alongside a new
    /// full budget, transiently over-subscribing the shared EBS pipe by
    /// `old_held + new_cap`.
    pending_forget: Arc<AtomicUsize>,
    /// As [`Self::pending_forget`], for the `maintenance_gate` semaphore.
    pending_gate_forget: Arc<AtomicUsize>,
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
        total: Arc::new(AtomicUsize::new(permits)),
        pending_forget: Arc::new(AtomicUsize::new(0)),
        pending_gate_forget: Arc::new(AtomicUsize::new(0)),
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

/// Forget up to `delta` permits on `sem` now, recording any that could not be
/// forgotten (because they are currently held) as owed in `owed` — to be
/// forgotten as those permits return (see [`drain_owed`]). Never adds capacity.
fn forget_or_owe(sem: &Semaphore, owed: &AtomicUsize, delta: usize) {
    if delta == 0 {
        return;
    }
    let forgot = sem.forget_permits(delta);
    if forgot < delta {
        owed.fetch_add(delta - forgot, Ordering::AcqRel);
    }
}

/// Forget any still-owed permits that have become available since the last
/// cap-shrink (a held permit released back to `sem`). Best-effort and cheap: a
/// single `forget_permits` of whatever is owed, capped by what is available
/// now; the remainder drains on a later call. Called on the acquire path so the
/// live budget converges to its cap as in-flight encodes finish, without ever
/// having re-added capacity.
fn drain_owed(sem: &Semaphore, owed: &AtomicUsize) {
    let pending = owed.load(Ordering::Acquire);
    if pending == 0 {
        return;
    }
    let forgot = sem.forget_permits(pending);
    if forgot > 0 {
        owed.fetch_sub(forgot, Ordering::AcqRel);
    }
}

impl EncodeBudget {
    /// Lower this budget's ceiling to `max_permits` IN PLACE (never raises).
    /// Returns the previous total when the cap bound (shrank), or `None` when it
    /// was already at or below `max_permits`. The caller serializes (the global
    /// write lock); `max_permits` is clamped to ≥ 1.
    ///
    /// This is the SINGLE source of the cap-shrink arithmetic —
    /// [`cap_global_encode_concurrency`] and the unit test both drive it, so the
    /// test can never validate a drifted copy of the shrink logic.
    ///
    /// Shrinks via `forget_permits` on the live semaphores rather than swapping
    /// in fresh ones. A swap left every permit already held against the old
    /// semaphore running ALONGSIDE a brand-new full-size budget, so aggregate
    /// concurrency could transiently reach `old_held + max_permits` —
    /// over-subscribing the very EBS pipe the cap protects. Forgetting in place
    /// never adds capacity: it removes available permits immediately and records
    /// the residual still held as owed (`pending_forget`/`pending_gate_forget`),
    /// forgotten as those permits return ([`drain_owed`] on the acquire path), so
    /// the budget converges DOWN to the cap and never temporarily up.
    fn shrink_to(&self, max_permits: usize) -> Option<usize> {
        let max_permits = max_permits.max(1);
        let current = self.total.load(Ordering::Acquire);
        if current <= max_permits {
            return None; // Already at or below the cap — never raises.
        }
        forget_or_owe(&self.semaphore, &self.pending_forget, current - max_permits);
        let gate_delta =
            maintenance_gate_cap(current).saturating_sub(maintenance_gate_cap(max_permits));
        forget_or_owe(
            &self.maintenance_gate,
            &self.pending_gate_forget,
            gate_delta,
        );
        self.total.store(max_permits, Ordering::Release);
        Some(current)
    }
}

/// Lower the process-global encode budget to at most `max_permits`, never raising
/// it. The integration point for the instance EBS-bandwidth cap: a single EBS
/// volume is a shared, bandwidth-bounded pipe, so the aggregate parallel
/// encode/upload streams must be bounded below the core-derived budget or they
/// just fan out small files without adding throughput. Idempotent and safe across
/// multiple table registrations — each EBS-class table proposes its
/// bandwidth-derived cap and the tightest wins; a local-SSD table proposes
/// nothing. A no-op when no budget is installed yet or the cap does not bind.
pub fn cap_global_encode_concurrency(max_permits: usize) {
    // The write lock serializes concurrent registrations proposing different
    // caps: `shrink_to` re-reads the current total and shrinks only while it still
    // exceeds `max_permits`, so a looser cap landing last cannot raise the budget
    // back up ("tightest wins, never raises"). The semaphores are mutated in place
    // (the shared `Arc`s every in-flight acquirer holds), so the lock guards the
    // total/forget bookkeeping, not an `Option` swap.
    let guard = GLOBAL_ENCODE_BUDGET.write();
    let Some(budget) = guard.as_ref() else {
        return; // No budget installed yet.
    };
    if let Some(previous) = budget.shrink_to(max_permits) {
        tracing::info!(
            target: "cayenne::write_budget",
            max_permits,
            previous,
            "Capping global encode-concurrency budget to the instance EBS write-bandwidth ceiling"
        );
    }
}

/// Point-in-time occupancy of the process-global encode budget, for operator
/// metrics. `available`/`total` are the main-semaphore permits (aggregate encode
/// concurrency headroom / ceiling); `maintenance_gate_available` is the headroom
/// left in the reserved maintenance slice. `available == 0` under a growing WAL
/// backlog is the direct signature of the encode-semaphore stall that this
/// budget exists to bound.
#[derive(Debug, Clone, Copy)]
pub struct EncodeBudgetSnapshot {
    /// Available (unheld) main-semaphore permits — aggregate encode-concurrency
    /// headroom right now. `0` means every permit is held (writes queue).
    pub available: u64,
    /// The current budget ceiling (total main permits).
    pub total: u64,
    /// Available permits in the reserved maintenance slice (compaction/rewrite
    /// outputs); `Delta` writes never consume these.
    pub maintenance_gate_available: u64,
}

/// Snapshot the process-global encode budget's occupancy, or `None` when no
/// budget is installed (unit tests / embedders that don't wire it up). Cheap and
/// lock-light: a read-guard plus three atomic loads — safe to call from a metrics
/// scrape callback.
#[must_use]
pub fn encode_budget_snapshot() -> Option<EncodeBudgetSnapshot> {
    let guard = GLOBAL_ENCODE_BUDGET.read();
    let budget = guard.as_ref()?;
    Some(EncodeBudgetSnapshot {
        available: budget.semaphore.available_permits() as u64,
        total: budget.total.load(Ordering::Acquire) as u64,
        maintenance_gate_available: budget.maintenance_gate.available_permits() as u64,
    })
}

/// Acquire up to `shards` encode permits from the global budget, atomically.
///
/// Returns the held permits (which release on drop, so callers scope them to
/// the write) or `None` when the write proceeds ungated: no budget installed,
/// or a single-shard write (see [`acquire_for_write`]). For gated writes,
/// `shards` is clamped to `[2, class cap]` so the request is always satisfiable
/// and can never block forever waiting for more permits than the budget can
/// ever hold. `Delta` writes may use the whole budget; `Maintenance` writes are
/// capped to [`maintenance_gate_cap`] in aggregate (see `maintenance_gate`).
pub(crate) async fn acquire_encode_permits(
    shards: usize,
    class: WriteClass,
) -> Option<EncodePermits> {
    let budget = GLOBAL_ENCODE_BUDGET.read().clone()?;
    acquire_for_write(&budget, shards, class).await
}

/// Budget policy for a write with the given shard count: multi-shard writes
/// acquire permits ([`acquire_from`]); **single-shard writes are exempt** and
/// proceed ungated (`None`).
///
/// The exemption is load-bearing, not an optimization. Writes coupled through
/// a shared input demux — partitioned-table inserts route one stream into
/// per-partition writes over bounded channels — deadlock if a child write can
/// park on this budget: the parked write stalls the demux, starving the
/// permit-holding siblings of input, and no permit is ever released
/// (spiceai/spiceai#11818). A single-shard write is guaranteed to make
/// progress without ever parking here, which keeps any demux it is coupled to
/// draining. The budget's purpose — capping aggregate encode *fan-out* — is
/// unaffected: a serial write contributes exactly the one encode stream per
/// writer that existed before intra-write sharding (and before this budget),
/// and compaction's aggregate concurrency stays bounded by the
/// `BackgroundCompactor` semaphore independently of this gate.
async fn acquire_for_write(
    budget: &EncodeBudget,
    shards: usize,
    class: WriteClass,
) -> Option<EncodePermits> {
    if shards <= 1 {
        return None;
    }
    acquire_from(budget, shards, class).await
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
    // Forget any permits a prior cap-shrink still owes that have since returned,
    // converging the live budget down to its cap before we read it. Cheap no-op
    // in the common case (nothing owed).
    drain_owed(&budget.semaphore, &budget.pending_forget);
    drain_owed(&budget.maintenance_gate, &budget.pending_gate_forget);
    // Read the LIVE ceiling (not a stale snapshot) so a request is always
    // satisfiable under the current — possibly just-shrunk — budget and can
    // never block forever waiting for more permits than it can ever hold.
    let total = budget.total.load(Ordering::Acquire);
    // Time the blocking acquire so the wait attributable to encode-budget
    // contention is observable (`cayenne_encode_acquire_wait_ms{class}`). This is
    // the CDC apply-path backpressure signal: near-zero under headroom, seconds
    // when a fleet of tables saturates the shared budget.
    let wait_start = Instant::now();
    let gate = match class {
        WriteClass::Delta => None,
        WriteClass::Maintenance => {
            // Gate first (uniform order; see `maintenance_gate` docs). The gate
            // is sized below `total`, so the subsequent main acquisition of the
            // same count can always be satisfied once delta holders release.
            let gate_cap = maintenance_gate_cap(total);
            let permits = u32::try_from(shards.clamp(1, gate_cap)).unwrap_or(u32::MAX);
            Some(
                Arc::clone(&budget.maintenance_gate)
                    .acquire_many_owned(permits)
                    .await
                    .ok()?,
            )
        }
    };
    let main_count = gate
        .as_ref()
        .map_or_else(|| shards.clamp(1, total), |g| g.num_permits().max(1));
    let main_count = u32::try_from(main_count).unwrap_or(u32::MAX);
    let main = Arc::clone(&budget.semaphore)
        .acquire_many_owned(main_count)
        .await
        .ok()?;
    let class_label = match class {
        WriteClass::Delta => "delta",
        WriteClass::Maintenance => "maintenance",
    };
    telemetry::cayenne::track_encode_acquire_wait(
        wait_start.elapsed(),
        &[telemetry::KeyValue::new("class", class_label)],
    );
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
            total: Arc::new(AtomicUsize::new(total)),
            pending_forget: Arc::new(AtomicUsize::new(0)),
            pending_gate_forget: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// REGRESSION (the resize-without-coordination bug): capping the budget while
    /// permits are held must NOT add capacity. The prior implementation swapped in
    /// a fresh `Semaphore::new(max_permits)`, so the already-held permits kept
    /// running alongside a brand-new full budget — a further acquire succeeded
    /// immediately, pushing aggregate concurrency to `old_held + new_cap` and
    /// over-subscribing the shared EBS pipe the cap exists to protect.
    ///
    /// With the in-place forget, capping below the held count leaves zero
    /// available, so a further acquire BLOCKS (no new capacity) — and once the
    /// over-cap held permits drain, the budget converges to the new cap rather
    /// than reverting to the old total.
    #[tokio::test]
    async fn cap_under_held_permits_never_oversubscribes() {
        let b = budget(4);
        // Hold the entire budget (4 permits), modelling 4 in-flight encodes.
        let held = acquire_from(&b, 4, WriteClass::Delta)
            .await
            .expect("initial acquire of the full budget succeeds");
        assert_eq!(b.semaphore.available_permits(), 0, "all 4 permits held");

        // A registration tightens the EBS cap to 2 while those 4 are in flight.
        b.shrink_to(2);

        // BUG would allow this: a fresh 2-permit semaphore grants immediately,
        // so 4 (old) + up-to-2 (new) = up to 6 concurrent encodes. FIX: the live
        // semaphore has 0 available, so the new acquire must block.
        let pending = acquire_from(&b, 1, WriteClass::Delta);
        tokio::pin!(pending);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut pending)
                .await
                .is_err(),
            "capping below the held count must NOT grant new permits \
             (the swap re-added capacity, over-subscribing to old_held + new_cap)"
        );

        // Release the 4 over-cap in-flight permits; the budget must converge DOWN
        // to the new cap of 2, not revert to the old total of 4.
        drop(held);
        // The blocked acquire now proceeds (a returned permit satisfies it).
        let resumed = tokio::time::timeout(Duration::from_millis(500), &mut pending)
            .await
            .expect("the waiter proceeds once an in-flight encode releases")
            .expect("acquire yields permits after release");
        // One more acquire drives the owed-forget drain to completion, then with
        // 1 of the 2 capped permits held by `resumed`, exactly 1 remains.
        let _second = acquire_from(&b, 1, WriteClass::Delta)
            .await
            .expect("a second acquire fits within the capped budget of 2");
        assert_eq!(
            b.semaphore.available_permits(),
            0,
            "budget converged to the cap of 2 (both held), not the old total of 4"
        );
        // And a THIRD acquire must block: the cap of 2 is now fully held, proving
        // the old total of 4 was not silently restored when the held permits returned.
        let third = acquire_from(&b, 1, WriteClass::Delta);
        tokio::pin!(third);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut third)
                .await
                .is_err(),
            "the capped budget of 2 must stay enforced after the held permits drained"
        );
        drop(resumed);
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

    /// REGRESSION (spiceai/spiceai#11818): a single-shard write must proceed
    /// ungated even when the budget is fully held. Partitioned-table inserts
    /// couple their per-partition writes through one bounded-channel demux, so
    /// a serial child write that parks on the budget stalls the demux, starves
    /// the permit-holding siblings of input, and deadlocks the whole insert —
    /// datasets loaded but the table never ready.
    #[tokio::test]
    async fn single_shard_write_is_exempt_even_at_zero_headroom() {
        let b = budget(2);
        let held = acquire_from(&b, 2, WriteClass::Delta).await;
        assert!(held.is_some());
        assert_eq!(b.semaphore.available_permits(), 0, "budget fully held");

        let exempt = tokio::time::timeout(
            Duration::from_millis(500),
            acquire_for_write(&b, 1, WriteClass::Delta),
        )
        .await
        .expect("a single-shard write must never block on the budget");
        assert!(exempt.is_none(), "exempt writes hold no permits");
        assert_eq!(
            b.semaphore.available_permits(),
            0,
            "the exemption consumes no budget"
        );
    }

    /// The exemption applies to `Maintenance` too: a single-shard compaction
    /// output (compaction pins its output to one shard) must not park on the
    /// gate — its aggregate concurrency is bounded by the compactor's own
    /// semaphore, not this budget.
    #[tokio::test]
    async fn single_shard_maintenance_is_exempt() {
        let total = 16;
        let b = budget(total);
        let gate = maintenance_gate_cap(total);
        let _held = acquire_from(&b, gate, WriteClass::Maintenance).await;
        assert_eq!(b.maintenance_gate.available_permits(), 0, "gate exhausted");

        let exempt = tokio::time::timeout(
            Duration::from_millis(500),
            acquire_for_write(&b, 1, WriteClass::Maintenance),
        )
        .await
        .expect("a single-shard maintenance write must never block on the gate");
        assert!(exempt.is_none());
    }

    /// Multi-shard writes stay gated: the exemption is strictly `shards <= 1`.
    #[tokio::test]
    async fn multi_shard_write_is_still_gated() {
        let b = budget(2);
        let held = acquire_for_write(&b, 2, WriteClass::Delta).await;
        assert!(held.is_some(), "a gated acquire succeeds under headroom");
        assert_eq!(b.semaphore.available_permits(), 0);

        let mut pending = std::pin::pin!(acquire_for_write(&b, 2, WriteClass::Delta));
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut pending)
                .await
                .is_err(),
            "a second multi-shard write queues while the budget is held"
        );
        drop(held);
        tokio::time::timeout(Duration::from_millis(500), &mut pending)
            .await
            .expect("the queued write proceeds once permits release");
    }
}
