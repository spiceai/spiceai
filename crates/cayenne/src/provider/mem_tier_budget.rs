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
//! When unset (the default — file mode, unit tests, embedders that don't wire it
//! up) [`try_reserve_bytes`] always succeeds, so memory mode (if explicitly
//! opted into without the budget installed) is gated only by the per-table cap.
//! The runtime binary installs the budget once at startup, sized from total
//! system/container memory (`resource_monitor::get_total_memory() / 8`). This
//! budget is independent of DataFusion's query memory pool.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};

use parking_lot::RwLock;

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
}

/// Process-wide in-memory CDC tier budget, injected once at startup by the
/// binary (sized to a fraction of TOTAL system/container memory —
/// `resource_monitor::get_total_memory() / 8` — and deliberately INDEPENDENT of
/// the DataFusion query memory pool, since the RAM tier lives off-pool).
/// Replaceable so a test binary that builds and drops multiple runtimes does not
/// retain a stale budget (mirrors [`super::write_budget`]).
static GLOBAL_MEM_TIER_BUDGET: LazyLock<RwLock<Option<MemTierBudget>>> =
    LazyLock::new(|| RwLock::new(None));

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

/// Release `bytes` previously reserved via [`try_reserve_bytes`]. Saturating and
/// a no-op when no budget is installed.
pub(crate) fn release_bytes(bytes: u64) {
    if let Some(budget) = GLOBAL_MEM_TIER_BUDGET.read().as_ref() {
        budget.release(bytes);
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
}
