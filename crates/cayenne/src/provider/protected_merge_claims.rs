/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! Claims that keep concurrent protected-snapshot subset merges disjoint.
//!
//! Key-delete subset merges share the table's compaction lock, so claims keep
//! them apart: a merge may claim only unclaimed, still-published runs, at a tier
//! strictly below every running merge, and within the pass budget left by the
//! running merges. The catalog CAS remains the correctness backstop.

use std::collections::HashSet;
use std::sync::Arc;

use parking_lot::Mutex;

/// Protected-snapshot runs that in-flight subset merges are rewriting.
#[derive(Debug, Default)]
pub(crate) struct ProtectedMergeClaims {
    next_claim_id: u64,
    claims: Vec<ProtectedMergeClaim>,
}

#[derive(Debug)]
struct ProtectedMergeClaim {
    claim_id: u64,
    snapshot_ids: Vec<String>,
    tier: u32,
    bytes: u64,
}

impl ProtectedMergeClaims {
    /// Copy of the claimed state, so a pass can plan without holding the mutex.
    pub(crate) fn view(&self) -> ProtectedMergeClaimsView {
        ProtectedMergeClaimsView {
            snapshot_ids: self
                .claims
                .iter()
                .flat_map(|claim| claim.snapshot_ids.iter().cloned())
                .collect(),
            bytes: self.claimed_bytes(),
            min_tier: self.claims.iter().map(|claim| claim.tier).min(),
            merges: self.claims.len(),
        }
    }

    fn claimed_bytes(&self) -> u64 {
        self.claims
            .iter()
            .fold(0u64, |total, claim| total.saturating_add(claim.bytes))
    }

    /// Claims `snapshot_ids` for a merge at `tier` reading `bytes`. Returns the
    /// claim id, or `None` if a run is claimed or no longer live, `tier` is not
    /// below every running merge, or the claim would exceed `max_pass_bytes`
    /// together with the running merges.
    fn try_insert(
        &mut self,
        snapshot_ids: Vec<String>,
        tier: u32,
        bytes: u64,
        max_pass_bytes: Option<u64>,
        is_live: impl Fn(&str) -> bool,
    ) -> Option<u64> {
        if self.claims.iter().any(|claim| claim.tier <= tier) {
            return None;
        }
        if max_pass_bytes.is_some_and(|budget| self.claimed_bytes().saturating_add(bytes) > budget)
        {
            return None;
        }
        let overlaps = snapshot_ids.iter().any(|id| {
            !is_live(id)
                || self
                    .claims
                    .iter()
                    .any(|claim| claim.snapshot_ids.contains(id))
        });
        if overlaps {
            return None;
        }
        let claim_id = self.next_claim_id;
        self.next_claim_id = self.next_claim_id.wrapping_add(1);
        self.claims.push(ProtectedMergeClaim {
            claim_id,
            snapshot_ids,
            tier,
            bytes,
        });
        Some(claim_id)
    }

    fn remove(&mut self, claim_id: u64) {
        self.claims.retain(|claim| claim.claim_id != claim_id);
    }

    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.claims.is_empty()
    }
}

/// [`ProtectedMergeClaims`] as seen at Phase 1.
#[derive(Debug)]
pub(crate) struct ProtectedMergeClaimsView {
    snapshot_ids: HashSet<String>,
    bytes: u64,
    min_tier: Option<u32>,
    merges: usize,
}

impl ProtectedMergeClaimsView {
    pub(crate) fn len(&self) -> usize {
        self.merges
    }

    pub(crate) fn is_claimed(&self, snapshot_id: &str) -> bool {
        self.snapshot_ids.contains(snapshot_id)
    }

    /// Lowest tier a running merge holds. Only tiers below it may merge.
    pub(crate) fn min_tier(&self) -> Option<u32> {
        self.min_tier
    }

    /// Pass budget left after the running merges. `None` stays unbounded.
    pub(crate) fn remaining_budget(&self, max_pass_bytes: Option<u64>) -> Option<u64> {
        max_pass_bytes.map(|budget| budget.saturating_sub(self.bytes))
    }
}

/// Releases a [`ProtectedMergeClaims`] entry on every exit of the merge.
pub(crate) struct ProtectedMergeClaimGuard {
    claims: Arc<Mutex<ProtectedMergeClaims>>,
    claim_id: u64,
}

impl ProtectedMergeClaimGuard {
    pub(crate) fn try_claim(
        claims: &Arc<Mutex<ProtectedMergeClaims>>,
        snapshot_ids: Vec<String>,
        tier: u32,
        bytes: u64,
        max_pass_bytes: Option<u64>,
        is_live: impl Fn(&str) -> bool,
    ) -> Option<Self> {
        let claim_id =
            claims
                .lock()
                .try_insert(snapshot_ids, tier, bytes, max_pass_bytes, is_live)?;
        Some(Self {
            claims: Arc::clone(claims),
            claim_id,
        })
    }
}

impl Drop for ProtectedMergeClaimGuard {
    fn drop(&mut self) {
        self.claims.lock().remove(self.claim_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ids(names: &[&str]) -> Vec<String> {
        names.iter().map(|name| (*name).to_string()).collect()
    }

    fn live(_: &str) -> bool {
        true
    }

    #[test]
    fn claims_keep_concurrent_merges_disjoint() {
        let mut claims = ProtectedMergeClaims::default();

        let large = claims
            .try_insert(ids(&["a", "b"]), 2, 400, None, live)
            .expect("first merge claims its runs");
        assert!(
            claims
                .try_insert(ids(&["b", "c"]), 0, 1, None, live)
                .is_none(),
            "a run another merge holds cannot be claimed"
        );
        assert!(
            claims
                .try_insert(ids(&["c", "d"]), 2, 1, None, live)
                .is_none(),
            "a second merge at the running merge's tier is refused"
        );
        assert!(
            claims
                .try_insert(ids(&["c", "d"]), 3, 1, None, live)
                .is_none(),
            "a larger-tier merge is refused while a merge runs"
        );
        assert!(
            claims
                .try_insert(ids(&["c", "d"]), 0, 1, None, |id| id != "d")
                .is_none(),
            "a run already merged away (no longer live) cannot be claimed"
        );
        let small = claims
            .try_insert(ids(&["c", "d"]), 0, 8, None, live)
            .expect("disjoint lower-tier merge is admitted");

        let view = claims.view();
        assert_eq!(view.len(), 2);
        assert!(view.is_claimed("a") && view.is_claimed("d") && !view.is_claimed("e"));
        assert_eq!(view.min_tier(), Some(0));
        assert_eq!(view.remaining_budget(Some(1_000)), Some(592));
        assert_eq!(view.remaining_budget(Some(100)), Some(0));
        assert_eq!(view.remaining_budget(None), None);

        claims.remove(large);
        claims.remove(small);
        assert!(claims.is_empty());
        assert_eq!(claims.view().min_tier(), None);
    }

    /// A merge planned against a stale view must not push the running merges
    /// past the pass budget: admission re-checks the budget atomically.
    #[test]
    fn claims_enforce_the_budget_at_admission() {
        let budget = Some(1_000);
        let mut claims = ProtectedMergeClaims::default();

        // The small merge planned with the whole budget; the large one then
        // claimed first.
        let stale_view = claims.view();
        assert_eq!(stale_view.remaining_budget(budget), Some(1_000));
        claims
            .try_insert(ids(&["a", "b"]), 2, 900, budget, live)
            .expect("large merge fits the budget");

        assert!(
            claims
                .try_insert(ids(&["c", "d"]), 0, 400, budget, live)
                .is_none(),
            "900 + 400 exceeds the 1,000-byte budget"
        );
        assert!(
            claims
                .try_insert(ids(&["c", "d"]), 0, 100, budget, live)
                .is_some(),
            "900 + 100 fits exactly"
        );
    }

    /// A merge publishes its replacement before dropping its claim, so liveness
    /// must be read while the claims mutex is held: a snapshot loaded earlier can
    /// still list inputs that a merge finishing in between has already removed.
    #[test]
    fn liveness_is_read_under_the_claims_mutex() {
        use std::collections::HashMap;

        let published = arc_swap::ArcSwap::from_pointee(HashMap::from([
            ("a".to_string(), 0_i64),
            ("b".to_string(), 0_i64),
        ]));
        let claims = Arc::new(Mutex::new(ProtectedMergeClaims::default()));

        let stale = published.load_full();
        let merge =
            ProtectedMergeClaimGuard::try_claim(&claims, ids(&["a", "b"]), 1, 10, None, live)
                .expect("the finishing merge holds the runs");
        // The finishing merge publishes its replacement, then drops its claim.
        published.store(Arc::new(HashMap::from([("merged".to_string(), 0_i64)])));
        drop(merge);

        assert!(
            ProtectedMergeClaimGuard::try_claim(&claims, ids(&["a", "b"]), 0, 10, None, |id| {
                stale.contains_key(id)
            })
            .is_some(),
            "a snapshot loaded before the lock admits inputs that are already gone"
        );
        claims.lock().claims.clear();
        assert!(
            ProtectedMergeClaimGuard::try_claim(&claims, ids(&["a", "b"]), 0, 10, None, |id| {
                published.load().contains_key(id)
            })
            .is_none(),
            "reading liveness inside the callback rejects them"
        );
    }

    #[test]
    fn claim_guard_releases_on_drop() {
        let claims = Arc::new(Mutex::new(ProtectedMergeClaims::default()));
        let guard =
            ProtectedMergeClaimGuard::try_claim(&claims, ids(&["a", "b"]), 1, 10, None, live)
                .expect("claim");
        drop(guard);
        assert!(claims.lock().is_empty(), "drop releases the claim");
        assert!(
            ProtectedMergeClaimGuard::try_claim(&claims, ids(&["a", "b"]), 1, 10, None, live)
                .is_some(),
            "released runs can be claimed by a later merge"
        );
    }
}
