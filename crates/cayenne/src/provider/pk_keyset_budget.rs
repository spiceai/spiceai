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

#[derive(Debug)]
struct PkKeysetBudget {
    total: Arc<AtomicU64>,
    used: Arc<AtomicU64>,
}

/// Install (or replace) the process-global keyset ceiling. `0` removes it.
pub fn set_global_pk_keyset_bytes(bytes: u64) {
    let mut guard = GLOBAL_PK_KEYSET_BUDGET.write();
    if bytes == 0 {
        *guard = None;
        return;
    }
    *guard = Some(PkKeysetBudget {
        total: Arc::new(AtomicU64::new(bytes)),
        used: Arc::new(AtomicU64::new(0)),
    });
}

/// The installed ceiling, or `None` when unset.
#[must_use]
pub fn global_pk_keyset_total() -> Option<u64> {
    GLOBAL_PK_KEYSET_BUDGET
        .read()
        .as_ref()
        .map(|b| b.total.load(Ordering::Relaxed))
}

/// Aggregate keyset bytes currently reserved across every table.
#[must_use]
pub fn global_pk_keyset_used() -> Option<u64> {
    GLOBAL_PK_KEYSET_BUDGET
        .read()
        .as_ref()
        .map(|b| b.used.load(Ordering::Relaxed))
}

/// Reserve `bytes` against the aggregate ceiling, returning whether it fit.
///
/// Always succeeds when no budget is installed. A compare-exchange loop rather
/// than fetch_add-then-check, so two tables growing concurrently can never both
/// observe room that only one of them has.
pub fn try_reserve_keyset_bytes(bytes: u64) -> bool {
    let guard = GLOBAL_PK_KEYSET_BUDGET.read();
    let Some(budget) = guard.as_ref() else {
        return true;
    };
    let total = budget.total.load(Ordering::Relaxed);
    let mut used = budget.used.load(Ordering::Relaxed);
    loop {
        let next = used.saturating_add(bytes);
        if next > total {
            return false;
        }
        match budget
            .used
            .compare_exchange_weak(used, next, Ordering::AcqRel, Ordering::Relaxed)
        {
            Ok(_) => return true,
            Err(observed) => used = observed,
        }
    }
}

/// Return `bytes` to the aggregate ceiling. Saturating: a release larger than
/// the outstanding total clamps to zero rather than wrapping, so an accounting
/// slip degrades to "budget looks emptier" instead of "budget looks impossibly
/// full", which would wedge every table into permanent bloom fallback.
pub fn release_keyset_bytes(bytes: u64) {
    let guard = GLOBAL_PK_KEYSET_BUDGET.read();
    let Some(budget) = guard.as_ref() else {
        return;
    };
    let mut used = budget.used.load(Ordering::Relaxed);
    loop {
        let next = used.saturating_sub(bytes);
        match budget
            .used
            .compare_exchange_weak(used, next, Ordering::AcqRel, Ordering::Relaxed)
        {
            Ok(_) => return,
            Err(observed) => used = observed,
        }
    }
}

/// Add `bytes` to the aggregate unconditionally.
///
/// For the accounting path, which reports residency that already exists: the
/// clamp in `effective_pk_keyset_budget` is what prevents growth, and once a
/// table HAS the bytes, hiding them from the aggregate would let every sibling
/// over-commit against headroom that is not there. Records the truth even when
/// the truth is over the ceiling.
pub fn force_reserve_keyset_bytes(bytes: u64) {
    let guard = GLOBAL_PK_KEYSET_BUDGET.read();
    let Some(budget) = guard.as_ref() else {
        return;
    };
    budget.used.fetch_add(bytes, Ordering::AcqRel);
}

#[cfg(test)]
mod tests {
    use super::{
        force_reserve_keyset_bytes, global_pk_keyset_total, global_pk_keyset_used,
        release_keyset_bytes, set_global_pk_keyset_bytes, try_reserve_keyset_bytes,
    };
    use std::sync::{LazyLock, Mutex};

    /// The budget is process-global, so the tests that install one must not
    /// interleave.
    static BUDGET_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

    /// The whole point: per-table budgets sized in isolation add up, and the
    /// aggregate is what the kernel kills on. Two tables that each fit
    /// individually must not both fit together.
    #[test]
    fn two_tables_that_each_fit_alone_do_not_both_fit_together() {
        let _guard = BUDGET_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        set_global_pk_keyset_bytes(1000);

        assert!(try_reserve_keyset_bytes(600), "the first table fits");
        assert!(
            !try_reserve_keyset_bytes(600),
            "the second must be refused: 600 fits alone but 1200 exceeds the fleet ceiling"
        );
        assert_eq!(
            global_pk_keyset_used(),
            Some(600),
            "a refusal reserves nothing"
        );

        release_keyset_bytes(600);
        assert_eq!(global_pk_keyset_used(), Some(0));
        assert!(
            try_reserve_keyset_bytes(600),
            "the freed budget is reusable by the sibling"
        );
        release_keyset_bytes(600);
        set_global_pk_keyset_bytes(0);
    }

    /// The clamp must let a grown table use free fleet headroom.
    ///
    /// `used` already includes the table's own share, so `remaining` is the
    /// headroom BESIDE it. The ceiling is therefore `own + remaining`. Taking
    /// the larger of the two instead freezes a grown table at its current size
    /// while the fleet still has room — own 2 GiB with 1 GiB free reads as a
    /// 2 GiB ceiling, admitting nothing. This models that arithmetic directly.
    #[test]
    fn a_grown_table_may_still_use_free_fleet_headroom() {
        let per_table: usize = 4000;
        let total: usize = 5000;
        let own: usize = 2000;
        // Two other tables hold 1500 between them, so `used` is own + 1500.
        let used: usize = own + 1500;
        let remaining = total - used;

        let ceiling = per_table.min(own.saturating_add(remaining));
        assert_eq!(
            ceiling, 3500,
            "the table may grow into the fleet's free headroom on top of what it holds"
        );
        assert!(
            ceiling > own,
            "a ceiling at or below current residency admits nothing and freezes the table"
        );

        // The rejected formula, kept as the contrast it exists to prevent.
        let frozen = per_table.min(remaining.max(own));
        assert_eq!(frozen, own, "max() collapses to own and freezes growth");
    }

    /// A fleet already at its ceiling must not push a table below what it holds:
    /// the bound governs growth, it does not evict.
    #[test]
    fn a_full_fleet_holds_a_table_at_its_current_size_but_no_lower() {
        let per_table: usize = 4000;
        let total: usize = 5000;
        let own: usize = 2000;
        let used: usize = total; // fleet exactly full
        let remaining = total.saturating_sub(used);

        let ceiling = per_table.min(own.saturating_add(remaining));
        assert_eq!(
            ceiling, own,
            "a full fleet pins the table at its residency rather than shrinking it"
        );
    }

    /// Unset must be transparent, so embedders and tests that never install a
    /// budget behave exactly as before.
    #[test]
    fn an_uninstalled_budget_admits_everything() {
        let _guard = BUDGET_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        set_global_pk_keyset_bytes(0);
        assert_eq!(global_pk_keyset_total(), None);
        assert!(try_reserve_keyset_bytes(u64::MAX));
        assert_eq!(global_pk_keyset_used(), None);
    }

    /// An over-release must clamp at zero. Wrapping would leave `used` near
    /// `u64::MAX`, which reads as a permanently full budget and would wedge
    /// every table into bloom fallback forever.
    #[test]
    fn an_over_release_clamps_instead_of_wrapping() {
        let _guard = BUDGET_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        set_global_pk_keyset_bytes(1000);
        assert!(try_reserve_keyset_bytes(100));
        release_keyset_bytes(9_999);
        assert_eq!(
            global_pk_keyset_used(),
            Some(0),
            "an over-release must floor at zero, never wrap to a full budget"
        );
        assert!(try_reserve_keyset_bytes(1000), "the budget is still usable");
        release_keyset_bytes(1000);
        set_global_pk_keyset_bytes(0);
    }

    /// Residency that already exists must be recorded even when it exceeds the
    /// ceiling: hiding it would let siblings over-commit against headroom that
    /// is not there.
    #[test]
    fn force_reserve_records_overshoot_so_siblings_see_no_headroom() {
        let _guard = BUDGET_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        set_global_pk_keyset_bytes(1000);
        force_reserve_keyset_bytes(1500);
        assert_eq!(
            global_pk_keyset_used(),
            Some(1500),
            "the overshoot is recorded"
        );
        assert!(
            !try_reserve_keyset_bytes(1),
            "a sibling must see no headroom while the fleet is over its ceiling"
        );
        release_keyset_bytes(1500);
        set_global_pk_keyset_bytes(0);
    }
}
