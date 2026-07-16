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

//! The frozen-tier drain ledger (proposal §4.5 — Stage 2a, Model B).
//!
//! A **drain generation** is a logical snapshot of the front `N_G` segments of each
//! mem-tier shard, captured at *freeze* and drained as a whole unit — either the
//! lightweight inline **seal** ([`crate::provider::CayenneTableProvider::seal_mem_tier_durable`]
//! — durable + slot ack, no Vortex) or the heavy **spill → publish**
//! ([`crate::provider::CayenneTableProvider::checkpoint_mem_tier`] — Vortex bake +
//! whole-unit atomic representation swap).
//!
//! ## Model B — a logical drain-ledger (the locked design)
//!
//! Freeze does NOT extract segments from the live [`MemTier`] that scans read. It
//! records the captured `Arc<MemTier>` snapshots plus the **relative** front
//! segment-count `N_G` per shard. Scans keep reading the live tier byte-for-byte
//! unchanged; publish removes exactly the front `N_G` segments via
//! [`MemTier::retain_after`](crate::provider::mem_tier::MemTier::retain_after), strictly
//! oldest-first. Relative counts compose to depth `D > 1`: generation `G` owns the
//! front `N_G`; publishing `G` drops `N_G` from the front, so `G+1` becomes the new
//! front with its own `N_{G+1}`.
//!
//! ## Depth at Stage 2b (`D = 1`)
//!
//! At `D = 1` the ledger holds at most one in-flight generation, so both entry
//! points stay byte-identical to the pre-2a code: a generation is always drained
//! (published or sealed) and reclaimed before the next freeze within one
//! `mem_checkpoint_lock`-held operation.
//!
//! Stage 2b Step 2 promotes the ledger from a per-operation local to a **shared
//! provider field** (`Arc<ParkingMutex<FrozenDrainLedger>>` on
//! [`crate::provider::CayenneTableProvider`]) that persists across calls. This is
//! now race-free because Step 1b serializes EVERY freeze/clear path (checkpoint,
//! seal, spill, schema-evolution flush, cold-tier promotion) on
//! `mem_checkpoint_lock` — a second concurrent `freeze()` can no longer exist, so
//! the shared `D = 1` ledger observes exactly one generation at a time. The
//! [`ParkingMutex`](parking_lot::Mutex) that guards it is therefore uncontended
//! today; it exists so an operation can drive the front generation's lifecycle
//! through brief, await-free critical sections (never holding the guard across an
//! `.await`), and so Stage 2b Step 3 can hand `D > 1` spills to the pinned ingest
//! pool where a worker drains a generation concurrently with the next freeze.
//! `max_depth` stays `1` until that step raises it.

use std::collections::VecDeque;
use std::sync::Arc;

use crate::provider::mem_tier::MemTier;

/// The lifecycle state of one drain generation (proposal §4.5 `stateDiagram-v2`).
///
/// `Active` in the diagram is the live tier, not a generation — a generation begins
/// at [`GenState::Frozen`]. The two entry points walk different sub-paths of the
/// same machine at `D = 1`:
///
/// - **seal** — `Frozen → Sealed`. The captured prefix is made crash-recoverable via
///   the inline shadow and the slot is acked; the segments STAY in the live tier
///   (the boundary is marked, nothing is removed), so the generation is dropped from
///   the ledger after `Sealed` while its rows remain readable.
/// - **checkpoint / bake** — `Frozen → Spilling → Published → Reclaimed` (the
///   un-decoupled path today: it bakes an un-sealed frozen tier directly and acks at
///   publish). `Published` removes the front `N_G` segments under the listing fence;
///   `Reclaimed` drops the captured Arcs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GenState {
    /// Captured off the active tier; still in the read union; slot NOT advanced past
    /// its epoch.
    Frozen,
    /// Inline seal shadow + tombstones durable, slot acked (the freshness cadence).
    /// No Vortex file yet; the rows are still in the read union.
    Sealed,
    /// A worker has claimed the generation and is baking it to a Vortex snapshot.
    Spilling,
    /// Whole-unit atomic publish done: `{data file + DV in, front prefix out}`, under
    /// the listing fence.
    Published,
    /// Budget released, captured Arcs dropped — the generation has left the ledger.
    Reclaimed,
}

impl GenState {
    /// Whether `self → next` is a legal edge of the §4.5 state machine. Enforced by
    /// [`FrozenGeneration::transition`] under `debug_assert!` so an illegal drive is
    /// caught in tests without adding a release-path branch.
    #[must_use]
    pub(crate) fn can_transition_to(self, next: GenState) -> bool {
        matches!(
            (self, next),
            // → Sealed: the seal cadence (Frozen), or a failed spill releasing its
            //   claim to retry (Spilling).
            (GenState::Frozen | GenState::Spilling, GenState::Sealed)
                // → Spilling: the un-decoupled bake (Frozen, D=1 today) or the
                //   decoupled bake of an already-sealed generation (Sealed; the seal
                //   already fired the ack) — the latter available for D>1.
                | (GenState::Frozen | GenState::Sealed, GenState::Spilling)
                // → Published: the whole-unit atomic publish.
                | (GenState::Spilling, GenState::Published)
                // → Reclaimed: after publish, or straight from a seal whose rows a
                //   later bake never revisits (the seal path drops it from the ledger).
                | (GenState::Published | GenState::Sealed, GenState::Reclaimed)
        )
    }
}

/// One drain generation: the captured, immutable freeze-time state of the front
/// `N_G` segments of every mem-tier shard, plus its lifecycle [`GenState`].
///
/// The per-generation state-pinning contract (proposal §4.5): the dominant RAM is
/// the captured shard snapshots (`Σ_shards tier.bytes`), pinned via the `Arc`s until
/// the generation is reclaimed. Everything else here is `O(shards)` or `O(1)`.
pub(crate) struct FrozenGeneration {
    /// The captured shard snapshots — the dominant pinned RAM. Model B reads these
    /// (never the live tier) for the off-lock encode/commit, so a post-freeze append
    /// to the live tier cannot tear this generation.
    pub(crate) shard_snapshots: Vec<Arc<MemTier>>,
    /// The **relative** front segment-count `N_G` per shard (the checkpoint's
    /// `flushed_counts` / the seal's `sealed_through`). Publish removes exactly this
    /// many front segments per shard; seal marks the boundary at it.
    pub(crate) relative_counts: Vec<usize>,
    /// The reserved snapshot/seal sequence. Known at freeze for the checkpoint
    /// (reserved under the capture locks); stamped after freeze for the seal
    /// (reserved off-lock). `None` for the empty / position-based checkpoint capture.
    pub(crate) reserved_seq: Option<i64>,
    /// The durable epoch this generation acks once it reaches its ack point
    /// (`Published` for the bake, `Sealed` for the seal). The slot ack is
    /// min-across-in-flight, never `fetch_max`; at `D = 1` this is the sole in-flight
    /// generation so it is trivially the min.
    pub(crate) epoch: u64,
    /// The §4.5 lifecycle state. Starts at [`GenState::Frozen`].
    state: GenState,
}

impl FrozenGeneration {
    /// Record a freshly frozen generation (state [`GenState::Frozen`]).
    #[must_use]
    pub(crate) fn freeze(
        shard_snapshots: Vec<Arc<MemTier>>,
        relative_counts: Vec<usize>,
        reserved_seq: Option<i64>,
        epoch: u64,
    ) -> Self {
        debug_assert_eq!(
            shard_snapshots.len(),
            relative_counts.len(),
            "a frozen generation captures one relative front-count per shard snapshot"
        );
        Self {
            shard_snapshots,
            relative_counts,
            reserved_seq,
            epoch,
            state: GenState::Frozen,
        }
    }

    /// The current lifecycle state. Exercised by the unit tests; production drives
    /// state via [`FrozenGeneration::transition`] and reads the private field
    /// directly within this module.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn state(&self) -> GenState {
        self.state
    }

    /// Drive the generation to `next`, asserting the edge is legal (§4.5). The
    /// transition itself carries no side effects — the caller performs the durable
    /// work (encode, publish, clear) and records the resulting state here so the
    /// ledger reflects the real lifecycle.
    pub(crate) fn transition(&mut self, next: GenState) {
        debug_assert!(
            self.state.can_transition_to(next),
            "illegal drain-generation transition {:?} -> {next:?}",
            self.state
        );
        self.state = next;
    }

    /// Stamp the reserved sequence after an off-lock reservation (the seal path,
    /// which reserves its `seal_sequence` outside the capture locks). Idempotent
    /// against re-stamping the same value.
    pub(crate) fn set_reserved_seq(&mut self, seq: i64) {
        self.reserved_seq = Some(seq);
    }
}

/// A bounded, ordered ledger of in-flight drain generations (proposal §4.5). Freeze
/// pushes at the back; drain/reclaim pops from the front (strictly oldest-first, so
/// [`MemTier::retain_after`](crate::provider::mem_tier::MemTier::retain_after) always
/// removes the front prefix). At Stage 2a `max_depth == 1`, so this degenerates to a
/// single-slot ledger; the abstraction is what Stage 2b promotes to a shared field.
pub(crate) struct FrozenDrainLedger {
    generations: VecDeque<FrozenGeneration>,
    max_depth: usize,
}

impl FrozenDrainLedger {
    /// A ledger bounded to `max_depth` resident generations. `max_depth` must be at
    /// least 1.
    #[must_use]
    pub(crate) fn new(max_depth: usize) -> Self {
        debug_assert!(max_depth >= 1, "a drain ledger admits at least one generation");
        Self {
            generations: VecDeque::with_capacity(max_depth),
            max_depth,
        }
    }

    /// Admit a frozen generation at the back and return a mutable handle to it (the
    /// new back, which at `D = 1` is also the front). Returns the generation back as
    /// `Err` when the ledger is already at `max_depth` (the back-pressure signal that
    /// stalls a freeze until a front generation drains). At `D = 1` this never
    /// triggers: a generation is always reclaimed before the next freeze within one
    /// `mem_checkpoint_lock`-held operation.
    pub(crate) fn freeze(
        &mut self,
        generation: FrozenGeneration,
    ) -> Result<&mut FrozenGeneration, FrozenGeneration> {
        if self.generations.len() >= self.max_depth {
            return Err(generation);
        }
        self.generations.push_back(generation);
        match self.generations.back_mut() {
            Some(generation) => Ok(generation),
            // The `push_back` above always leaves a back element.
            None => unreachable!("freeze pushed a generation but the deque is empty"),
        }
    }

    /// Reclaim (drop) the front generation, releasing its captured Arcs. The caller
    /// must have driven it to a terminal state ([`GenState::Published`] for a bake or
    /// [`GenState::Sealed`] for a seal) first; asserted under `debug_assert!`. Drives
    /// the reclaimed generation to [`GenState::Reclaimed`] before it leaves the
    /// ledger, completing the §4.5 lifecycle.
    pub(crate) fn reclaim_front(&mut self) -> Option<FrozenGeneration> {
        if let Some(front) = self.generations.front_mut() {
            debug_assert!(
                matches!(front.state, GenState::Published | GenState::Sealed),
                "reclaiming a drain generation in non-terminal state {:?}",
                front.state
            );
            front.transition(GenState::Reclaimed);
        }
        self.generations.pop_front()
    }

    /// Remove the front generation regardless of its lifecycle state, dropping its
    /// captured Arcs, WITHOUT the terminal-state assertion [`Self::reclaim_front`]
    /// makes. This is the **abort** path: a drain that fails on an early `?` return
    /// or panics mid-flight — before it published or sealed — must not strand its
    /// frozen generation in the shared ledger, or the next freeze would hit the
    /// depth bound and error the table permanently. Discarding a not-yet-published
    /// generation loses nothing: the mem-tier segments are removed only at publish
    /// (`retain_after`), so the rows are still live in the tier and the next capture
    /// re-freezes them. Returns the discarded generation, or `None` if the ledger is
    /// already empty (the happy path, where the drain reclaimed it explicitly).
    pub(crate) fn discard_front(&mut self) -> Option<FrozenGeneration> {
        self.generations.pop_front()
    }

    /// The front generation (oldest in-flight). Exercised by the ledger unit tests;
    /// the shared-field drain reads the captured fields it needs into operation
    /// locals at freeze time rather than through this borrow (the guard cannot be
    /// held across the off-lock encode).
    #[cfg(test)]
    #[must_use]
    pub(crate) fn front(&self) -> Option<&FrozenGeneration> {
        self.generations.front()
    }

    /// Mutable handle to the front generation. The shared-field drain (Stage 2b)
    /// drives the front generation's lifecycle transitions through this under a
    /// brief, await-free `drain_ledger` lock.
    #[must_use]
    pub(crate) fn front_mut(&mut self) -> Option<&mut FrozenGeneration> {
        self.generations.front_mut()
    }

    /// The number of resident (in-flight) generations. At `D = 1` this is `0` before
    /// a freeze and `1` after, asserted by the entry points to guard the invariant.
    #[must_use]
    pub(crate) fn depth(&self) -> usize {
        self.generations.len()
    }

    /// Whether the ledger holds no in-flight generation. The entry points assert
    /// this holds before they freeze (at `D = 1` every prior drain reclaimed or the
    /// cleanup guard discarded its generation before releasing `mem_checkpoint_lock`).
    #[must_use]
    pub(crate) fn is_empty(&self) -> bool {
        self.generations.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn one_shard_gen(epoch: u64) -> FrozenGeneration {
        FrozenGeneration::freeze(vec![Arc::new(MemTier::empty())], vec![0], Some(7), epoch)
    }

    #[test]
    fn freeze_starts_frozen() {
        let g = one_shard_gen(3);
        assert_eq!(g.state(), GenState::Frozen);
        assert_eq!(g.epoch, 3);
        assert_eq!(g.reserved_seq, Some(7));
        assert_eq!(g.relative_counts, vec![0]);
        assert_eq!(g.shard_snapshots.len(), 1);
    }

    #[test]
    fn bake_path_transitions_are_legal() {
        assert!(GenState::Frozen.can_transition_to(GenState::Spilling));
        assert!(GenState::Spilling.can_transition_to(GenState::Published));
        assert!(GenState::Published.can_transition_to(GenState::Reclaimed));
        // spill failure releases the claim and retries
        assert!(GenState::Spilling.can_transition_to(GenState::Sealed));
    }

    #[test]
    fn seal_path_transitions_are_legal() {
        assert!(GenState::Frozen.can_transition_to(GenState::Sealed));
        assert!(GenState::Sealed.can_transition_to(GenState::Reclaimed));
        // decoupled: a sealed generation is later baked (available for D>1)
        assert!(GenState::Sealed.can_transition_to(GenState::Spilling));
    }

    #[test]
    fn illegal_transitions_are_rejected() {
        assert!(!GenState::Frozen.can_transition_to(GenState::Published));
        assert!(!GenState::Frozen.can_transition_to(GenState::Reclaimed));
        assert!(!GenState::Published.can_transition_to(GenState::Spilling));
        assert!(!GenState::Reclaimed.can_transition_to(GenState::Frozen));
        assert!(!GenState::Sealed.can_transition_to(GenState::Published));
    }

    #[test]
    fn set_reserved_seq_stamps_after_freeze() {
        let mut g =
            FrozenGeneration::freeze(vec![Arc::new(MemTier::empty())], vec![0], None, 1);
        assert_eq!(g.reserved_seq, None);
        g.set_reserved_seq(42);
        assert_eq!(g.reserved_seq, Some(42));
    }

    #[test]
    fn ledger_admits_up_to_max_depth() {
        let mut ledger = FrozenDrainLedger::new(1);
        assert!(ledger.is_empty());
        assert!(ledger.freeze(one_shard_gen(1)).is_ok());
        assert_eq!(ledger.depth(), 1);
        // At D=1 a second freeze before reclaim is refused (back-pressure).
        let rejected = ledger.freeze(one_shard_gen(2));
        assert!(rejected.is_err());
        assert_eq!(rejected.err().map(|g| g.epoch), Some(2));
        assert_eq!(ledger.depth(), 1);
    }

    #[test]
    fn ledger_reclaims_front_oldest_first() {
        let mut ledger = FrozenDrainLedger::new(2);
        assert!(ledger.freeze(one_shard_gen(10)).is_ok());
        assert!(ledger.freeze(one_shard_gen(20)).is_ok());
        // Drive the front to a terminal state before reclaim.
        ledger.front_mut().expect("front").transition(GenState::Sealed);
        let reclaimed = ledger.reclaim_front().expect("reclaim front");
        assert_eq!(reclaimed.epoch, 10);
        assert_eq!(ledger.depth(), 1);
        assert_eq!(ledger.front().expect("remaining front").epoch, 20);
    }

    #[test]
    fn discard_front_removes_a_non_terminal_generation() {
        // The abort path: a generation frozen but not driven to a terminal state
        // (an error mid-drain) must still be removable, WITHOUT the terminal-state
        // assertion `reclaim_front` makes, so a failed drain never strands it.
        let mut ledger = FrozenDrainLedger::new(1);
        assert!(ledger.freeze(one_shard_gen(9)).is_ok());
        // Still Frozen — reclaim_front would assert; discard_front must not.
        assert_eq!(ledger.front().expect("front").state(), GenState::Frozen);
        let discarded = ledger.discard_front().expect("discard front");
        assert_eq!(discarded.epoch, 9);
        assert!(ledger.is_empty());
        // Discarding an empty ledger is a no-op (the happy path already reclaimed).
        assert!(ledger.discard_front().is_none());
    }

    #[test]
    fn front_generation_walks_the_bake_path() {
        let mut ledger = FrozenDrainLedger::new(1);
        let Ok(g) = ledger.freeze(one_shard_gen(5)) else {
            panic!("fresh D=1 ledger admits the first generation");
        };
        g.transition(GenState::Spilling);
        g.transition(GenState::Published);
        assert_eq!(ledger.front().expect("front").state(), GenState::Published);
        let reclaimed = ledger.reclaim_front().expect("reclaim");
        assert_eq!(reclaimed.epoch, 5);
        assert!(ledger.is_empty());
    }
}
