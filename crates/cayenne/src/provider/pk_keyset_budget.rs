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

//! Process-global byte ceiling for the PK keyset caches, shared across all
//! Cayenne tables. See [`GLOBAL_PK_KEYSET_BUDGET`] for why per-table budgets
//! sized in isolation are not enough.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};

use parking_lot::RwLock;

/// Process-global ceiling on the aggregate PK keyset cache across every Cayenne
/// table.
///
/// Each table derives its own keyset budget from `pk_keyset_cache_mb` — roughly
/// **1/32 of memory, clamped to [256 MiB, 8 GiB]** — and nothing bounded their
/// SUM. Seven CDC tables on a 96 GiB host is therefore 21 GiB of keyset budget
/// that no single table can ever exceed, so no table degrades and the aggregate
/// grows to whatever the fleet happens to need. A SF-1000 heap profile measured
/// ~14.5 GiB resident in keysets with **zero** over-budget events: every table
/// was correctly inside its own limit while the process ran to its cgroup cap.
///
/// This is the same failure the in-memory CDC tier already solves next door
/// (`mem_tier_budget`): per-table caps sized in isolation simply add up, and the
/// aggregate is what the kernel kills on. The two budgets are deliberately
/// separate — a table can be memory-mode without a PK, and vice versa — but they
/// share the shape.
///
/// A keyset checks [`try_reserve_keyset_bytes`] before growing and releases via
/// [`release_keyset_bytes`] when it shrinks, is degraded to a bloom, or is
/// dropped. Refusal is not an error: the caller already knows how to fall back
/// (degrade to a bloom under upsert, drop and lazily rebuild under `DoNothing`),
/// which is the same path an over-budget single table takes today.
///
/// Unset — the default for embedders and tests that never install it — makes
/// every reservation succeed, so behaviour is unchanged unless the runtime opts
/// in.
static GLOBAL_PK_KEYSET_BUDGET: LazyLock<RwLock<Option<PkKeysetBudget>>> =
    LazyLock::new(|| RwLock::new(None));

/// The ceiling and its outstanding reservations.
///
/// A plain value rather than a set of free functions over the global so the
/// arithmetic can be exercised on an instance: every reserve/release rule below
/// is a property of this type, and testing it through the process-global static
/// would make each assertion depend on whichever other test happens to be
/// running beside it.
#[derive(Debug)]
struct PkKeysetBudget {
    total: Arc<AtomicU64>,
    used: Arc<AtomicU64>,
}

impl PkKeysetBudget {
    fn new(total: u64) -> Self {
        Self {
            total: Arc::new(AtomicU64::new(total)),
            used: Arc::new(AtomicU64::new(0)),
        }
    }

    fn total(&self) -> u64 {
        self.total.load(Ordering::Relaxed)
    }

    fn used(&self) -> u64 {
        self.used.load(Ordering::Relaxed)
    }

    /// See [`try_reserve_keyset_bytes`] for what this does and does not
    /// guarantee.
    fn try_reserve(&self, bytes: u64) -> bool {
        let total = self.total.load(Ordering::Relaxed);
        let mut used = self.used.load(Ordering::Relaxed);
        loop {
            let next = used.saturating_add(bytes);
            if next > total {
                return false;
            }
            match self
                .used
                .compare_exchange_weak(used, next, Ordering::AcqRel, Ordering::Relaxed)
            {
                Ok(_) => return true,
                Err(observed) => used = observed,
            }
        }
    }

    /// Saturating: see [`release_keyset_bytes`].
    fn release(&self, bytes: u64) {
        let mut used = self.used.load(Ordering::Relaxed);
        loop {
            let next = used.saturating_sub(bytes);
            match self
                .used
                .compare_exchange_weak(used, next, Ordering::AcqRel, Ordering::Relaxed)
            {
                Ok(_) => return,
                Err(observed) => used = observed,
            }
        }
    }

    /// Unconditional: see [`force_reserve_keyset_bytes`].
    fn force_reserve(&self, bytes: u64) {
        self.used.fetch_add(bytes, Ordering::AcqRel);
    }
}

/// Install the process-global keyset ceiling, or change an installed one. `0`
/// removes it.
///
/// Changing an installed ceiling keeps `used`, because the live
/// `CayenneMemoryAccount`s hold reservations against it and settle them
/// individually — each restates its share as a delta and returns it on `Drop`.
/// Reseeding `used` to zero would strand those bytes: a table holding 600 that
/// is dropped after the change would release 600 it never reserved against the
/// new counter, taking a *sibling's* reservation to zero with it.
pub fn set_global_pk_keyset_bytes(bytes: u64) {
    let mut guard = GLOBAL_PK_KEYSET_BUDGET.write();
    if bytes == 0 {
        *guard = None;
        return;
    }
    match guard.as_ref() {
        Some(existing) => existing.total.store(bytes, Ordering::Relaxed),
        None => *guard = Some(PkKeysetBudget::new(bytes)),
    }
}

/// The installed ceiling, or `None` when unset.
#[must_use]
pub fn global_pk_keyset_total() -> Option<u64> {
    GLOBAL_PK_KEYSET_BUDGET.read().as_ref().map(PkKeysetBudget::total)
}

/// Aggregate keyset bytes currently reserved across every table.
#[must_use]
pub fn global_pk_keyset_used() -> Option<u64> {
    GLOBAL_PK_KEYSET_BUDGET.read().as_ref().map(PkKeysetBudget::used)
}

/// Reserve `bytes` against the aggregate ceiling, returning whether it fit.
/// Always succeeds when no budget is installed.
///
/// # What the compare-exchange does and does not buy
///
/// The loop makes the *counter* exact: concurrent callers never lose an update,
/// and two callers can never both be told a reservation fit when only one of
/// them had room for it.
///
/// It is **not** admission control for the caches themselves, because a table
/// does not reserve before it grows. It reads its ceiling
/// (`effective_pk_keyset_budget`), inserts up to it, and publishes the resulting
/// residency afterwards — so two tables can read the same headroom, both insert
/// into it, and the second publication then finds no room and records its bytes
/// through [`force_reserve_keyset_bytes`] anyway, leaving `used > total`.
///
/// That is deliberate, and it is why the overshoot is recorded rather than
/// hidden: once the bytes exist, the honest aggregate is what stops the *next*
/// grower, and a ceiling that under-reports would let siblings over-commit
/// against headroom that is not there. The overshoot is bounded by how far a
/// grower can get between reading its ceiling and publishing — one chunk, since
/// `ShardedPkIndex::record_keys_bounded` re-reads the tally every 512 keys —
/// not by a whole batch. Pre-claiming instead would mean reserving bytes a
/// caller cannot size in advance (it inserts key by key) and refunding the
/// remainder, which buys a tighter bound on a quantity that is already an
/// estimate.
pub fn try_reserve_keyset_bytes(bytes: u64) -> bool {
    GLOBAL_PK_KEYSET_BUDGET
        .read()
        .as_ref()
        .is_none_or(|budget| budget.try_reserve(bytes))
}

/// Return `bytes` to the aggregate ceiling. Saturating: a release larger than
/// the outstanding total clamps to zero rather than wrapping, so an accounting
/// slip degrades to "budget looks emptier" instead of "budget looks impossibly
/// full", which would wedge every table into permanent bloom fallback.
pub fn release_keyset_bytes(bytes: u64) {
    if let Some(budget) = GLOBAL_PK_KEYSET_BUDGET.read().as_ref() {
        budget.release(bytes);
    }
}

/// Clamp one cache's configured ceiling to what the fleet has left, or return
/// it untouched when no budget is installed.
///
/// `own` is the residency of the cache being clamped. See
/// [`clamp_to_fleet_headroom`] for the arithmetic and why it is a sum.
#[must_use]
pub(crate) fn clamp_pk_keyset_budget(per_cache: usize, own: usize) -> usize {
    let guard = GLOBAL_PK_KEYSET_BUDGET.read();
    let Some(budget) = guard.as_ref() else {
        return per_cache;
    };
    clamp_to_fleet_headroom(per_cache, own, budget.total(), budget.used())
}

/// `used` already includes `own`, so `total - used` is the headroom BESIDE this
/// cache. The ceiling is therefore what the cache already holds PLUS that
/// headroom — not the larger of the two, which would freeze a grown cache at its
/// current size while the fleet still had room (own 2 GiB + free 1 GiB reads as
/// a 2 GiB ceiling, admitting nothing).
///
/// Adding rather than replacing also keeps the ceiling from dropping below
/// current residency, so a cache at its limit is never told to shrink by
/// degrading: the fleet bound governs GROWTH, it does not evict.
fn clamp_to_fleet_headroom(per_cache: usize, own: usize, total: u64, used: u64) -> usize {
    let remaining = usize::try_from(total.saturating_sub(used)).unwrap_or(usize::MAX);
    per_cache.min(own.saturating_add(remaining))
}

/// Add `bytes` to the aggregate unconditionally.
///
/// For the accounting path, which reports residency that already exists: the
/// clamp in `effective_pk_keyset_budget` is what prevents growth, and once a
/// table HAS the bytes, hiding them from the aggregate would let every sibling
/// over-commit against headroom that is not there. Records the truth even when
/// the truth is over the ceiling.
pub fn force_reserve_keyset_bytes(bytes: u64) {
    if let Some(budget) = GLOBAL_PK_KEYSET_BUDGET.read().as_ref() {
        budget.force_reserve(bytes);
    }
}

#[cfg(test)]
mod tests {
    use super::{
        PkKeysetBudget, clamp_to_fleet_headroom, global_pk_keyset_total,
        set_global_pk_keyset_bytes, try_reserve_keyset_bytes,
    };
    use std::sync::{LazyLock, Mutex};

    /// Only the tests that touch the process-global static take this. The
    /// reservation rules themselves are exercised on a local [`PkKeysetBudget`],
    /// so they cannot be perturbed by a test elsewhere in the crate that grows a
    /// keyset (`CayenneMemoryAccount::set_keyset_bytes` publishes into the same
    /// global counter) — under a threaded `cargo test` those run beside these.
    static BUDGET_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

    /// The whole point: per-table budgets sized in isolation add up, and the
    /// aggregate is what the kernel kills on. Two tables that each fit
    /// individually must not both fit together.
    #[test]
    fn two_tables_that_each_fit_alone_do_not_both_fit_together() {
        let budget = PkKeysetBudget::new(1000);

        assert!(budget.try_reserve(600), "the first table fits");
        assert!(
            !budget.try_reserve(600),
            "the second must be refused: 600 fits alone but 1200 exceeds the fleet ceiling"
        );
        assert_eq!(budget.used(), 600, "a refusal reserves nothing");

        budget.release(600);
        assert_eq!(budget.used(), 0);
        assert!(
            budget.try_reserve(600),
            "the freed budget is reusable by the sibling"
        );
    }

    /// Changing an installed ceiling must not reseed `used`: the live memory
    /// accounts hold reservations against it and settle them individually, so a
    /// zeroed counter lets one table's drop release a sibling's bytes.
    #[test]
    fn changing_the_ceiling_keeps_outstanding_reservations() {
        let budget = PkKeysetBudget::new(1000);
        assert!(budget.try_reserve(600), "table A reserves its share");

        // What `set_global_pk_keyset_bytes` does to an already-installed budget.
        budget.total.store(2000, super::Ordering::Relaxed);

        assert_eq!(
            budget.used(),
            600,
            "table A's reservation survives the ceiling change"
        );
        assert!(budget.try_reserve(600), "table B reserves against the rest");
        budget.release(600); // table A is dropped
        assert_eq!(
            budget.used(),
            600,
            "dropping A leaves B's reservation standing, not a zeroed counter"
        );
    }

    /// The clamp must let a grown cache use free fleet headroom.
    ///
    /// `used` already includes the cache's own share, so `remaining` is the
    /// headroom BESIDE it. The ceiling is therefore `own + remaining`. Taking
    /// the larger of the two instead freezes a grown cache at its current size
    /// while the fleet still has room — own 2 GiB with 1 GiB free reads as a
    /// 2 GiB ceiling, admitting nothing.
    #[test]
    fn a_grown_cache_may_still_use_free_fleet_headroom() {
        let per_cache: usize = 4000;
        let own: usize = 2000;
        // Two other tables hold 1500 between them, so `used` is own + 1500.
        let ceiling = clamp_to_fleet_headroom(per_cache, own, 5000, own as u64 + 1500);

        assert_eq!(
            ceiling, 3500,
            "the cache may grow into the fleet's free headroom on top of what it holds"
        );
        assert!(
            ceiling > own,
            "a ceiling at or below current residency admits nothing and freezes the cache"
        );

        // The rejected formula, kept as the contrast it exists to prevent.
        let remaining = 5000 - (own + 1500);
        let frozen = per_cache.min(remaining.max(own));
        assert_eq!(frozen, own, "max() collapses to own and freezes growth");
    }

    /// A fleet already at its ceiling must not push a cache below what it holds:
    /// the bound governs growth, it does not evict.
    #[test]
    fn a_full_fleet_holds_a_cache_at_its_current_size_but_no_lower() {
        let own: usize = 2000;
        let ceiling = clamp_to_fleet_headroom(4000, own, 5000, 5000);
        assert_eq!(
            ceiling, own,
            "a full fleet pins the cache at its residency rather than shrinking it"
        );
    }

    /// A sharded table runs two live caches, and each is clamped against ITS OWN
    /// residency. Passing the table-wide sum instead hands each cache the
    /// other's bytes as extra allowance, so on a full fleet the pair grows to
    /// twice what the fleet had left — the bound defeated by the table it was
    /// bounding.
    #[test]
    fn each_cache_is_clamped_against_its_own_residency_not_the_table_sum() {
        let per_cache: usize = 4000;
        let (single, sharded) = (1000usize, 0usize);
        // The fleet is exactly full: everything `used` is this table's pair.
        let (total, used) = (1000u64, 1000u64);

        assert_eq!(
            clamp_to_fleet_headroom(per_cache, sharded, total, used),
            0,
            "the empty per-shard index gets no allowance from a full fleet"
        );
        assert_eq!(
            clamp_to_fleet_headroom(per_cache, single, total, used),
            single,
            "the cache that holds the bytes is pinned at them, not shrunk"
        );

        let table_sum = single + sharded;
        assert_eq!(
            clamp_to_fleet_headroom(per_cache, table_sum, total, used),
            single,
            "the rejected form: the sum would let the empty cache grow to 1000 as well, \
             doubling the pair against a fleet with nothing left"
        );
    }

    /// An over-release must clamp at zero. Wrapping would leave `used` near
    /// `u64::MAX`, which reads as a permanently full budget and would wedge
    /// every table into bloom fallback forever.
    #[test]
    fn an_over_release_clamps_instead_of_wrapping() {
        let budget = PkKeysetBudget::new(1000);
        assert!(budget.try_reserve(100));
        budget.release(9_999);
        assert_eq!(
            budget.used(),
            0,
            "an over-release must floor at zero, never wrap to a full budget"
        );
        assert!(budget.try_reserve(1000), "the budget is still usable");
    }

    /// Residency that already exists must be recorded even when it exceeds the
    /// ceiling: hiding it would let siblings over-commit against headroom that
    /// is not there.
    #[test]
    fn force_reserve_records_overshoot_so_siblings_see_no_headroom() {
        let budget = PkKeysetBudget::new(1000);
        budget.force_reserve(1500);
        assert_eq!(budget.used(), 1500, "the overshoot is recorded");
        assert!(
            !budget.try_reserve(1),
            "a sibling must see no headroom while the fleet is over its ceiling"
        );
    }

    /// The global wiring: install, change, uninstall. Asserts only `total`,
    /// which nothing but `set_global_pk_keyset_bytes` writes — `used` belongs to
    /// whichever tables exist in the process and is asserted on an instance
    /// above.
    #[test]
    fn installing_and_removing_the_global_ceiling() {
        let _guard = BUDGET_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let restore = global_pk_keyset_total();

        set_global_pk_keyset_bytes(4096);
        assert_eq!(global_pk_keyset_total(), Some(4096));
        set_global_pk_keyset_bytes(8192);
        assert_eq!(
            global_pk_keyset_total(),
            Some(8192),
            "an installed ceiling is changed in place"
        );

        set_global_pk_keyset_bytes(0);
        assert_eq!(global_pk_keyset_total(), None, "0 removes the ceiling");
        assert!(
            try_reserve_keyset_bytes(u64::MAX),
            "unset must be transparent, so embedders and tests behave as before"
        );

        // Leave the process as it was found, in case a budget was installed.
        if let Some(bytes) = restore {
            set_global_pk_keyset_bytes(bytes);
        }
    }
}
