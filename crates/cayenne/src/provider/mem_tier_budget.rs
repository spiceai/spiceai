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

//! Process-global byte budget for the in-memory CDC durability tier, shared
//! across all Cayenne tables running in `cdc_durability: memory` mode.
//!
//! In memory mode the CDC write path appends each batch to an in-RAM tier
//! instead of persisting a durable metastore BLOB per batch, deferring the
//! source slot ack to a periodic/cap-triggered checkpoint. Each table has its
//! own per-table cap (`cayenne_cdc_mem_tier_max_bytes`), but per-table caps
//! sized in isolation simply SUM across a fleet of tables receiving CDC at once
//! — exactly the oversubscription failure mode the global encode budget guards
//! against (see [`super::write_budget`]). A per-table-only cap on
//! `stock`+`order_line`+`customer`+… would blow the box's `memory_limit`; this
//! global byte budget is the non-optional aggregate guard that makes memory
//! mode OOM-safe.
//!
//! A RAM append [`try_reserve_bytes`]`(incoming_bytes)` BEFORE growing the tier.
//! The reserved bytes are tracked in a single global atomic and are released via
//! [`release_bytes`] when the covering epoch is checkpointed (spilled to durable
//! Vortex). On failure (the aggregate would exceed the budget) the caller MUST
//! NOT grow the tier — it spills the current epoch and/or falls back to the
//! durable path. The budget therefore bounds the total resident RAM across every
//! memory-mode table to `total` bytes; there is no unbounded-growth path.
//!
//! Accounting is keyed on the tier's own byte total (the same dimension the
//! per-table cap uses): an append reserves `incoming_bytes`, and the checkpoint
//! that flushes the tier releases the flushed tier's `bytes`. A spill-then-retry
//! that frees this table's bytes is exactly a `release` followed by a fresh
//! `try_reserve`, so the global `used` always tracks the live aggregate RAM.
//!
//! Because the budget is shared, the table whose reservation is refused is
//! often NOT the table holding the bytes. [`reserve_bytes_or_wait`] lets such a
//! writer wait (bounded) for ANOTHER table's checkpoint to [`release_bytes`]
//! before resorting to spilling its own tier; every release signals the
//! process-global [`BUDGET_RELEASED`] notifier to wake those waiters.
//!
//! When unset (the default — file mode, unit tests, embedders that don't wire it
//! up) [`try_reserve_bytes`] always succeeds, so memory mode (if explicitly
//! opted into without the budget installed) is gated only by the per-table cap.
//! The runtime binary installs the budget once at startup, sized from total
//! system/container memory (`resource_monitor::get_total_memory() / 4`). This
//! budget is independent of `DataFusion`'s query memory pool.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use parking_lot::RwLock;
use tokio::sync::Notify;

/// How long a writer whose reservation was refused waits in
/// [`reserve_bytes_or_wait`] for ANOTHER table's checkpoint to release budget
/// before falling back to spilling its own tier: one background-checkpointer
/// tick (1 s — `cdc_mem_tier_checkpoint_interval_ms`) plus encode headroom, so
/// the table actually holding the budget usually flushes (and releases) within
/// the wait. Deliberately NOT a user parameter. Bounded so a budget held by
/// idle/cold tiers — whose churn-gated ticks may never flush — degrades to the
/// caller's self-spill backstop instead of an unbounded stall.
pub(crate) const BUDGET_WAIT: Duration = Duration::from_millis(1500);

/// A counting byte-budget. Reservations are tracked with a single atomic so the
/// reserve path is lock-free; the budget value itself is read once on install.
#[derive(Clone)]
struct MemTierBudget {
    /// Total bytes the in-memory CDC tier may hold across ALL tables.
    total: u64,
    /// Currently-reserved bytes (sum of live reservations across tables).
    used: Arc<AtomicU64>,
}

impl MemTierBudget {
    /// Attempt to reserve `bytes`. Returns `true` and records the reservation on
    /// success; returns `false` and reserves nothing when the reservation would
    /// push the aggregate over `total`.
    ///
    /// A request larger than the entire budget can never fit, so it always
    /// fails (the caller spills/falls back rather than blocking forever).
    fn try_reserve(&self, bytes: u64) -> bool {
        if bytes > self.total {
            return false;
        }
        // CAS loop: only commit the reservation if it still fits against the
        // latest `used`. This keeps the aggregate hard-capped under concurrent
        // appends from independent tables without a lock.
        let mut current = self.used.load(Ordering::Acquire);
        loop {
            let Some(next) = current.checked_add(bytes) else {
                return false;
            };
            if next > self.total {
                return false;
            }
            match self.used.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(observed) => current = observed,
            }
        }
    }

    /// Release a previously-reserved `bytes`. Saturating so a double-release or
    /// an over-release (defensive) can never wrap the counter below zero.
    fn release(&self, bytes: u64) {
        // `fetch_update` with a saturating subtraction keeps `used` monotone and
        // never underflows even if the same reservation is released twice.
        let _ = self
            .used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                Some(current.saturating_sub(bytes))
            });
    }

    /// [`Self::try_reserve`], but on refusal wait up to `max_wait` for a
    /// concurrent [`Self::release`] (signaled via `released` — the process-global
    /// [`BUDGET_RELEASED`] in production; tests pass their own notifier) to make
    /// room, retrying after each release. Returns `true` with the reservation
    /// RECORDED, or `false` with nothing reserved once the deadline passes.
    ///
    /// LOST-WAKEUP SAFETY: each iteration creates (and pins) the `notified()`
    /// future BEFORE re-trying the reservation. A `Notified` future is
    /// guaranteed to receive `notify_waiters()` calls made after its creation,
    /// even if it has not been polled yet — so a release that lands between the
    /// failed `try_reserve` and the `select!` await below still wakes us, and a
    /// release that lands before the creation is observed by that `try_reserve`
    /// itself (its CAS reads the latest `used`). No release can fall into a gap.
    async fn reserve_or_wait(&self, released: &Notify, bytes: u64, max_wait: Duration) -> bool {
        // A request larger than the entire budget can never fit no matter how
        // many releases land; fail fast to the caller's spill/fallback instead
        // of parking for the full bound (mirrors `try_reserve`).
        if bytes > self.total {
            return false;
        }
        // ONE absolute deadline across all retries — not a per-iteration
        // timeout — so frequent small releases that never make enough room
        // cannot extend the wait beyond `max_wait`.
        let deadline = tokio::time::sleep_until(tokio::time::Instant::now() + max_wait);
        tokio::pin!(deadline);
        loop {
            let notified = released.notified();
            tokio::pin!(notified);
            if self.try_reserve(bytes) {
                return true;
            }
            tokio::select! {
                () = &mut notified => {} // budget released — retry the reserve
                () = &mut deadline => return false, // bound hit — caller spills
            }
        }
    }
}

/// Process-wide in-memory CDC tier budget, injected once at startup by the
/// binary (sized to a fraction of TOTAL system/container memory —
/// `resource_monitor::get_total_memory() / 8` — and deliberately INDEPENDENT of
/// the `DataFusion` query memory pool, since the RAM tier lives off-pool).
/// Replaceable so a test binary that builds and drops multiple runtimes does not
/// retain a stale budget (mirrors [`super::write_budget`]).
static GLOBAL_MEM_TIER_BUDGET: LazyLock<RwLock<Option<MemTierBudget>>> =
    LazyLock::new(|| RwLock::new(None));

/// Process-global notifier signaled by [`release_bytes`] after every decrement,
/// waking writers parked in [`reserve_bytes_or_wait`]. Kept OUTSIDE
/// [`MemTierBudget`] (one per process, like the budget registry itself) so a
/// test binary replacing the budget via [`set_global_mem_tier_bytes`] cannot
/// strand a waiter on a dropped notifier.
static BUDGET_RELEASED: LazyLock<Notify> = LazyLock::new(Notify::new);

/// Install the process-global in-memory CDC tier byte budget. Called once at
/// startup with the byte ceiling for the aggregate RAM tier across all tables.
/// `bytes` of 0 leaves the budget UNSET (memory mode then relies on per-table
/// caps only); later calls replace the previous budget.
pub fn set_global_mem_tier_bytes(bytes: u64) {
    let mut guard = GLOBAL_MEM_TIER_BUDGET.write();
    if bytes == 0 {
        *guard = None;
        return;
    }
    if guard.is_some() {
        tracing::debug!(
            target: "cayenne::mem_tier_budget",
            bytes,
            "Replacing global in-memory CDC tier budget"
        );
    }
    *guard = Some(MemTierBudget {
        total: bytes,
        used: Arc::new(AtomicU64::new(0)),
    });
}

/// Attempt to reserve `bytes` from the global in-memory tier budget.
///
/// Returns `true` when the bytes fit (or when no budget is installed — an
/// ungated success) and records the reservation; returns `false` (recording
/// nothing) when the aggregate would exceed the budget. A `false` result means
/// the caller MUST NOT grow the RAM tier: spill the current epoch and/or fall
/// back to the durable path. The caller releases via [`release_bytes`] when the
/// covering epoch is checkpointed.
pub(crate) fn try_reserve_bytes(bytes: u64) -> bool {
    match GLOBAL_MEM_TIER_BUDGET.read().as_ref() {
        None => true,
        Some(budget) => budget.try_reserve(bytes),
    }
}

/// Release `bytes` previously reserved via [`try_reserve_bytes`] (or a
/// successful [`reserve_bytes_or_wait`]). Saturating and a no-op when no budget
/// is installed.
pub(crate) fn release_bytes(bytes: u64) {
    let guard = GLOBAL_MEM_TIER_BUDGET.read();
    let Some(budget) = guard.as_ref() else {
        return;
    };
    budget.release(bytes);
    drop(guard);
    // AFTER the decrement: wake every writer parked in `reserve_bytes_or_wait`
    // so it re-tries against the freed bytes. (Notifying before decrementing
    // could wake a waiter into a still-full budget with no second wake coming.)
    BUDGET_RELEASED.notify_waiters();
}

/// [`try_reserve_bytes`], but on refusal wait up to `max_wait` for another
/// table's checkpoint to [`release_bytes`] before giving up. Returns `true`
/// with the reservation RECORDED (the caller MUST NOT reserve again — it now
/// owns exactly one reservation, released like any other when its epoch is
/// checkpointed) or `true` immediately when no budget is installed (ungated,
/// same as [`try_reserve_bytes`]); returns `false` with nothing reserved once
/// the deadline passes — the caller then spills/falls back exactly as it would
/// after a plain refused reserve.
///
/// This exists for the eviction-victim inversion: when the global budget is
/// full, the requesting table is usually NOT the one holding it, and spilling
/// the requester evicts a small table on the hog's behalf. Waiting one
/// checkpoint tick lets the hog's own background flush pay instead.
pub(crate) async fn reserve_bytes_or_wait(bytes: u64, max_wait: Duration) -> bool {
    // Clone the budget handle out of the registry lock — a parking_lot guard
    // must not be held across an await. The clone shares `used` (an `Arc`), so
    // a reservation made through it is the same global accounting.
    let budget = GLOBAL_MEM_TIER_BUDGET.read().clone();
    match budget {
        None => true,
        Some(budget) => {
            budget
                .reserve_or_wait(&BUDGET_RELEASED, bytes, max_wait)
                .await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn budget(total: u64) -> MemTierBudget {
        MemTierBudget {
            total,
            used: Arc::new(AtomicU64::new(0)),
        }
    }

    /// A reservation that fits is granted and consumes exactly its bytes.
    #[test]
    fn reserve_takes_requested_when_under_total() {
        let b = budget(1000);
        assert!(b.try_reserve(300));
        assert_eq!(b.used.load(Ordering::Acquire), 300);
        assert!(b.try_reserve(700));
        assert_eq!(b.used.load(Ordering::Acquire), 1000);
    }

    /// The budget caps the aggregate: once full, a further reserve is refused
    /// without consuming anything (the caller spills/falls back). This is the
    /// no-global-cap regression guard applied to memory.
    #[test]
    fn budget_caps_aggregate() {
        let b = budget(1000);
        assert!(b.try_reserve(800));
        // 800 + 300 = 1100 > 1000 → refused, nothing taken.
        assert!(!b.try_reserve(300));
        assert_eq!(b.used.load(Ordering::Acquire), 800);
        // A smaller request that still fits succeeds.
        assert!(b.try_reserve(200));
        assert_eq!(b.used.load(Ordering::Acquire), 1000);
    }

    /// A single request larger than the whole budget can never fit, so it fails
    /// rather than blocking forever (parity with the encode-budget clamp).
    #[test]
    fn over_large_request_is_refused() {
        let b = budget(1000);
        assert!(!b.try_reserve(2000));
        assert_eq!(b.used.load(Ordering::Acquire), 0);
    }

    /// Releasing returns the bytes so the aggregate can be reused; release is
    /// saturating and never underflows.
    #[test]
    fn release_returns_bytes_and_saturates() {
        let b = budget(1000);
        assert!(b.try_reserve(600));
        b.release(600);
        assert_eq!(b.used.load(Ordering::Acquire), 0);
        // Over-release saturates at 0 rather than wrapping.
        b.release(600);
        assert_eq!(b.used.load(Ordering::Acquire), 0);
    }

    /// With no global budget installed, `try_reserve_bytes` is an ungated
    /// success (memory mode then relies on per-table caps only) and
    /// `release_bytes` is a harmless no-op. No test mutates the global, so it is
    /// always unset here.
    #[test]
    fn try_reserve_bytes_is_noop_when_unset() {
        assert!(try_reserve_bytes(1_000_000));
        release_bytes(1_000_000);
    }

    /// With no global budget installed, the bounded wait is the same ungated
    /// success as `try_reserve_bytes` — it must return immediately, not park.
    #[tokio::test(start_paused = true)]
    async fn reserve_bytes_or_wait_is_noop_when_unset() {
        assert!(reserve_bytes_or_wait(1_000_000, Duration::from_millis(1500)).await);
    }

    /// Budget-release wait (the eviction-victim inversion fix): a "big table"
    /// hogs the budget, a small table's writer parks in `reserve_or_wait`, and
    /// the big table's release (decrement-then-notify, exactly what
    /// `release_bytes` does) wakes the writer into a successful reservation —
    /// promptly, well before the bound, and WITHOUT the waiting table spilling
    /// (no checkpoint is involved anywhere in this wake path; the writer
    /// proceeds on the wait's success alone). Uses a local budget + notifier,
    /// mirroring this module's rule that no test mutates the process global.
    #[tokio::test(start_paused = true)]
    async fn reserve_or_wait_wakes_on_release_without_spill() {
        let b = budget(1000);
        let released = Arc::new(Notify::new());
        assert!(b.try_reserve(900), "the fake big table hogs the budget");

        let waiter_budget = b.clone();
        let waiter_notify = Arc::clone(&released);
        let waiter = tokio::spawn(async move {
            waiter_budget
                .reserve_or_wait(&waiter_notify, 300, Duration::from_millis(1500))
                .await
        });
        // Give the waiter a moment to park (900 + 300 > 1000, so it cannot
        // fit); it must still be blocked — i.e. it waits rather than failing
        // straight to the self-spill path.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !waiter.is_finished(),
            "the writer must park while the budget is held by another table"
        );

        // The big table's checkpoint frees its bytes: decrement, THEN notify —
        // the exact `release_bytes` sequence.
        b.release(900);
        released.notify_waiters();

        assert!(
            waiter.await.expect("join waiter"),
            "a release must wake the waiter into a successful reserve"
        );
        assert_eq!(
            b.used.load(Ordering::Acquire),
            300,
            "exactly the waiter's bytes are reserved — reserved once on wake, never double-reserved"
        );
    }

    /// Timeout fallback: with the budget exhausted and never released, the
    /// bounded wait returns `false` at ~the bound (single absolute deadline:
    /// at least the bound, well under 2x) having reserved NOTHING — after
    /// which the writer self-spills; its own checkpoint's release then lets
    /// the retry reserve succeed (modeled here as release + `try_reserve`,
    /// the exact post-spill retry the write path performs), keeping the
    /// durable fallback the last resort exactly as before.
    #[tokio::test(start_paused = true)]
    async fn reserve_or_wait_times_out_at_bound_then_self_spill_retry_succeeds() {
        let b = budget(1000);
        let released = Arc::new(Notify::new());
        assert!(b.try_reserve(900), "budget exhausted and never released");

        let bound = Duration::from_millis(1500);
        let start = tokio::time::Instant::now();
        let admitted = b.reserve_or_wait(&released, 300, bound).await;
        let elapsed = start.elapsed();
        assert!(
            !admitted,
            "no release ever lands, so the wait must time out"
        );
        assert!(
            elapsed >= bound,
            "the wait must hold for the full bound (elapsed {elapsed:?} < bound {bound:?})"
        );
        assert!(
            elapsed < bound * 2,
            "one absolute deadline — wakeups must not extend the wait (elapsed {elapsed:?})"
        );
        assert_eq!(
            b.used.load(Ordering::Acquire),
            900,
            "a timed-out wait reserves nothing"
        );

        // The writer path then self-spills: its checkpoint releases this
        // table's flushed bytes and the post-spill retry succeeds.
        b.release(900);
        released.notify_waiters();
        assert!(
            b.try_reserve(300),
            "after the self-spill's release the retry reserve succeeds"
        );
    }

    /// A request larger than the whole budget can never be satisfied by any
    /// number of releases, so the bounded wait fails fast instead of burning
    /// the full bound (parity with `try_reserve`'s over-large clamp).
    #[tokio::test(start_paused = true)]
    async fn reserve_or_wait_fails_fast_on_over_large_request() {
        let b = budget(1000);
        let released = Notify::new();
        let start = tokio::time::Instant::now();
        assert!(
            !b.reserve_or_wait(&released, 2000, Duration::from_millis(1500))
                .await
        );
        assert!(
            start.elapsed() < Duration::from_millis(1500),
            "an unsatisfiable request must not park for the bound"
        );
        assert_eq!(b.used.load(Ordering::Acquire), 0);
    }
}
