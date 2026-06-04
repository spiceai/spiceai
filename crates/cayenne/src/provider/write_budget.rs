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

#[derive(Clone)]
struct EncodeBudget {
    semaphore: Arc<Semaphore>,
    total: usize,
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
/// Returns the held permit (which releases on drop, so callers scope it to the
/// write) or `None` when no budget is installed (proceed ungated). `shards` is
/// clamped to `[1, total]` so the request is always satisfiable and can never
/// block forever waiting for more permits than the budget can ever hold.
pub(crate) async fn acquire_encode_permits(shards: usize) -> Option<OwnedSemaphorePermit> {
    let budget = GLOBAL_ENCODE_BUDGET.read().clone()?;
    acquire_from(&budget, shards).await
}

/// Acquire `min(shards, budget.total)` permits from a specific budget. Extracted
/// from [`acquire_encode_permits`] so the clamp/cap behavior is unit-testable
/// against a local budget, without mutating the process-global state (which would
/// race sibling tests in the same binary).
///
/// Yields `None` only if the semaphore has been closed (teardown); a live
/// budget's semaphore is never closed, so in practice this always returns a
/// permit. Returning `None` rather than panicking keeps a shutdown race ungated
/// instead of failing the write.
async fn acquire_from(budget: &EncodeBudget, shards: usize) -> Option<OwnedSemaphorePermit> {
    let permits = shards.clamp(1, budget.total);
    let permits = u32::try_from(permits).unwrap_or(u32::MAX);
    Arc::clone(&budget.semaphore)
        .acquire_many_owned(permits)
        .await
        .ok()
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn budget(total: usize) -> EncodeBudget {
        EncodeBudget {
            semaphore: Arc::new(Semaphore::new(total)),
            total,
        }
    }

    /// A request for more shards than the budget holds is clamped to the total,
    /// so it is always satisfiable rather than blocking forever.
    #[tokio::test]
    async fn acquire_clamps_to_total() {
        let b = budget(4);
        let _permit = acquire_from(&b, 16).await; // 16 clamped to 4
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
        let _permit = acquire_from(&b, 3).await;
        assert_eq!(b.semaphore.available_permits(), 5, "took exactly 3 of 8");
    }

    /// The budget caps aggregate concurrency: with `total` permits held, a
    /// further acquire blocks until one is released.
    #[tokio::test]
    async fn budget_caps_aggregate_concurrency() {
        let b = budget(2);
        let held = acquire_from(&b, 2).await; // holds all 2
        let pending = acquire_from(&b, 1);
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

    /// With no global budget installed, acquisition is a no-op (`None`) and
    /// writes proceed ungated. No test mutates the global, so it is always unset
    /// here — keeping this free of cross-test interference.
    #[tokio::test]
    async fn acquire_encode_permits_is_noop_when_unset() {
        assert!(acquire_encode_permits(8).await.is_none());
    }
}
