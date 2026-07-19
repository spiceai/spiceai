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
    /// The ledger-assigned identity of this generation, monotone in freeze order
    /// (oldest = lowest). Assigned by [`FrozenDrainLedger::freeze`], `0` until then.
    /// A drain drives / discards ITS OWN generation by this id rather than by ledger
    /// position, because at depth `D > 1` a drain's generation is NOT necessarily the
    /// front — the front may belong to an older still-in-flight drain. At `D = 1` the
    /// sole resident generation always carries the current id, so id-addressing is
    /// byte-identical to front-addressing.
    pub(crate) id: u64,
    /// The captured shard snapshots — the dominant pinned RAM. Model B reads these
    /// (never the live tier) for the off-lock encode/commit, so a post-freeze append
    /// to the live tier cannot tear this generation.
    pub(crate) shard_snapshots: Vec<Arc<MemTier>>,
    /// The per-shard **window base**: the freeze-time resident-prefix offset into
    /// the captured snapshot (Σ of the older still-resident generations' relative
    /// counts, from [`FrozenDrainLedger::resident_prefix_counts`]). A checkpoint
    /// encodes ONLY `segments[base..base + N_G)` of its captured snapshot, so a
    /// newer generation never re-bakes an older still-resident generation's front
    /// prefix (the D>1 double-count fix). `[0; n]` at `D = 1` (the ledger is empty at
    /// freeze), so the window is the whole snapshot and the encode is byte-identical.
    /// The seal path leaves this at the freeze-time base but does not window (it
    /// shadows the active piece via [`MemTier::unsealed_view`]).
    pub(crate) window_base: Vec<usize>,
    /// The **relative** front segment-count `N_G` per shard (the checkpoint's window
    /// length = absolute segment count − [`Self::window_base`]; the seal's
    /// `sealed_through` boundary). Publish removes exactly this many front segments
    /// per shard via `retain_after` (ordered oldest-first, so at publish the live
    /// tier's front IS this generation's window); seal marks the boundary at it.
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
        window_base: Vec<usize>,
        relative_counts: Vec<usize>,
        reserved_seq: Option<i64>,
        epoch: u64,
    ) -> Self {
        debug_assert_eq!(
            shard_snapshots.len(),
            relative_counts.len(),
            "a frozen generation captures one relative front-count per shard snapshot"
        );
        debug_assert_eq!(
            shard_snapshots.len(),
            window_base.len(),
            "a frozen generation captures one window base per shard snapshot"
        );
        Self {
            // Assigned by `FrozenDrainLedger::freeze` on admission; a placeholder here.
            id: 0,
            shard_snapshots,
            window_base,
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
    /// The next generation id to assign, monotone in freeze order. Never reset, so an
    /// id is unique for the life of the ledger — a reclaimed/discarded generation's id
    /// is never reused, so a stale id addresses nothing (returns `None`) rather than a
    /// later generation.
    next_id: u64,
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
            next_id: 0,
        }
    }

    /// The per-shard sum of the relative front-counts of every currently-resident
    /// generation — the **window base** a newly frozen generation starts at. Freeze
    /// captures the whole live `segments`, so the front `resident_prefix_counts`
    /// segments already belong to older still-resident generations; the new freeze
    /// owns `segments[base..]`, i.e. its own relative count is
    /// `segments.len() − base[shard]`. An EMPTY ledger yields an all-zero base ⇒ the
    /// freeze's relative counts equal its absolute segment counts (byte-identical at
    /// `D = 1`, where every prior drain reclaimed before the next freeze). Returns a
    /// `Vec` of length `n` (the shard count) because an empty ledger carries no
    /// generation to infer the fan-out from.
    #[must_use]
    pub(crate) fn resident_prefix_counts(&self, n: usize) -> Vec<usize> {
        let mut base = vec![0usize; n];
        for generation in &self.generations {
            for (b, c) in base.iter_mut().zip(generation.relative_counts.iter()) {
                *b = b.saturating_add(*c);
            }
        }
        base
    }

    /// Admit a frozen generation at the back and return a mutable handle to it (the
    /// new back, which at `D = 1` is also the front). Returns the generation back as
    /// `Err` when the ledger is already at `max_depth` (the back-pressure signal that
    /// stalls a freeze until a front generation drains). At `D = 1` this never
    /// triggers: a generation is always reclaimed before the next freeze within one
    /// `mem_checkpoint_lock`-held operation.
    pub(crate) fn freeze(
        &mut self,
        mut generation: FrozenGeneration,
    ) -> Result<&mut FrozenGeneration, FrozenGeneration> {
        if self.generations.len() >= self.max_depth {
            return Err(generation);
        }
        // Assign the identity on admission (monotone in freeze order). The caller reads
        // it off the returned handle and addresses this generation by id thereafter, so
        // a drain drives ITS OWN generation even when a newer freeze has since made it a
        // non-front element (depth `D > 1`).
        generation.id = self.next_id;
        self.next_id = self.next_id.wrapping_add(1);
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

    /// Reclaim the maximal contiguous prefix of TERMINAL (`Published` / `Sealed`)
    /// generations from the front (strictly oldest-first) and return the max ack epoch
    /// over that prefix — the slot watermark to fire — or `None` when the front is not
    /// yet terminal (an older generation is still in flight).
    ///
    /// This is the **ordered-publish + min-across-in-flight ack** contract. Encode/PUT
    /// fan out unordered across the `D` in-flight generations, but the durable
    /// watermark may only advance over a CONTIGUOUS terminal prefix from the oldest: a
    /// still-encoding or failed older generation `G` pins the ack at `G − 1`. So this
    /// walks the front, reclaiming each generation while it is terminal and stops at
    /// the first non-terminal one, returning the MAX epoch of the reclaimed run. It is
    /// NEVER a `fetch_max` over an arbitrary generation's epoch: a later generation
    /// that publishes first does NOT advance the watermark past an unfinished earlier
    /// one — its epoch is only acked once every older generation has published and been
    /// reclaimed ahead of it. Because segment (and therefore source-position) order is
    /// monotone with freeze order, the epochs of a contiguous prefix are
    /// non-decreasing, so the returned max is the newest reclaimed generation's epoch
    /// and successive calls yield a monotone watermark.
    ///
    /// At `D = 1` the sole resident generation is the front, so publishing it makes the
    /// front terminal and this reclaims exactly it and returns its epoch —
    /// byte-identical to the pre-3b `fire(epoch); reclaim_front()` pair.
    pub(crate) fn reclaim_terminal_prefix_ack(&mut self) -> Option<u64> {
        let mut watermark: Option<u64> = None;
        while let Some(front) = self.generations.front() {
            if !matches!(front.state, GenState::Published | GenState::Sealed) {
                break;
            }
            let epoch = front.epoch;
            // Front is terminal, so `reclaim_front`'s terminal-state assertion holds.
            self.reclaim_front();
            watermark = Some(watermark.map_or(epoch, |w: u64| w.max(epoch)));
        }
        watermark
    }

    /// CASCADE-discard: remove the generation with `id` AND every generation frozen
    /// AFTER it (younger, positioned behind it), dropping all their captured Arcs,
    /// WITHOUT the terminal-state assertion [`Self::reclaim_front`] makes. Returns the
    /// discarded run oldest-first (the generation with `id` first), or an empty `Vec`
    /// if no resident generation carries `id` (the happy path, where the drain already
    /// reclaimed it).
    ///
    /// This is the **abort / D>1 failure** contract (Stage 2b Steps 2 + 3b-b-iii). A
    /// drain that fails on an early `?` return or panics mid-flight — before it
    /// published or sealed — must discard its OWN frozen generation, or the next freeze
    /// eventually hits the depth bound and errors the table permanently. But a failure
    /// invalidates not just its own generation: it also breaks the
    /// [`window_base`](FrozenGeneration::window_base) of every YOUNGER resident
    /// generation, because each younger freeze summed this generation's
    /// `relative_counts` into its own base ([`Self::resident_prefix_counts`]) on the
    /// promise that this generation's front prefix would be cleared (`retain_after`)
    /// ahead of it. A failing generation never clears, so a younger generation that then
    /// published would `retain_after` the WRONG front prefix (this generation's
    /// un-cleared segments) — corrupting the tier. Discarding the whole younger run
    /// collapses the pipeline back to a safe point: nothing was cleared (publish clears
    /// the front only oldest-first, and none of these ever became the front), so every
    /// discarded generation's rows are still live in the tier, and the source replays
    /// their un-acked epochs PK-idempotently. The younger drains' own tasks abort at the
    /// ordered-publish gate (they observe `!is_resident`) rather than publishing a stale
    /// window. Addressing by id (not by front) is what makes this safe at depth `D > 1`,
    /// where the failing drain's generation may not be the front. Generations OLDER than
    /// `id` (ahead of it) are untouched — they froze before it, so their bases never
    /// counted it, and they publish/clear their own fronts correctly.
    ///
    /// At `D = 1` the sole resident generation is the only one at or behind `id`, so
    /// this reclaims exactly it (or nothing, if already reclaimed) — byte-identical to
    /// the pre-3b-b-iii single-generation discard.
    pub(crate) fn discard_from_id(&mut self, id: u64) -> Vec<FrozenGeneration> {
        let Some(pos) = self.generations.iter().position(|g| g.id == id) else {
            return Vec::new();
        };
        self.generations.drain(pos..).collect()
    }

    /// Mutable handle to the resident generation with `id`, or `None` if none carries
    /// it. A drain drives its own generation's lifecycle transitions
    /// (`Spilling` / `Published` / `Sealed`) and stamps its reserved seal sequence
    /// through this under a brief, await-free `drain_ledger` lock. At `D = 1` this is
    /// the front; at `D > 1` it may be a non-front element.
    #[must_use]
    pub(crate) fn generation_mut_by_id(&mut self, id: u64) -> Option<&mut FrozenGeneration> {
        self.generations.iter_mut().find(|g| g.id == id)
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

    /// Mutable handle to the front generation. Exercised by the ledger unit tests;
    /// the shared-field drain drives its OWN generation by id
    /// ([`Self::generation_mut_by_id`]), not by front, so it is correct at `D > 1`.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn front_mut(&mut self) -> Option<&mut FrozenGeneration> {
        self.generations.front_mut()
    }

    /// The number of resident (in-flight) generations. Exercised by the ledger unit
    /// tests; Stage 2b Step 3b's depth-`D` admission / back-pressure will read this in
    /// production (freeze stalls at `max_depth`), at which point it is un-`cfg`'d.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn depth(&self) -> usize {
        self.generations.len()
    }

    /// Whether the ledger holds no in-flight generation. The entry points assert
    /// this holds before they freeze at `D = 1` (every prior drain reclaimed or the
    /// cleanup guard discarded its generation before releasing `mem_checkpoint_lock`).
    #[must_use]
    pub(crate) fn is_empty(&self) -> bool {
        self.generations.is_empty()
    }

    /// The configured maximum number of resident generations (`D`). `1` at the config
    /// default; a `> 1` value (Stage 2b Step 3b-b) enables the detached pipelined
    /// drain. Read by the entry points to decide inline-vs-detached and by the
    /// admission back-pressure.
    #[must_use]
    pub(crate) fn max_depth(&self) -> usize {
        self.max_depth
    }

    /// The id of the FRONT (oldest in-flight) generation, or `None` when empty. The
    /// ordered-publish gate reads this: a drain may publish only once its generation is
    /// the front (every older generation has published and been reclaimed ahead of it).
    #[must_use]
    pub(crate) fn front_id(&self) -> Option<u64> {
        self.generations.front().map(|g| g.id)
    }

    /// Whether a generation with `id` is still resident. The ordered-publish gate uses
    /// this to stop waiting if its own generation was discarded (aborted) while other
    /// generations remain — it would otherwise never become the front.
    #[must_use]
    pub(crate) fn is_resident(&self, id: u64) -> bool {
        self.generations.iter().any(|g| g.id == id)
    }

    /// Test-only: raise (or reset) the resident-generation bound `D` so the D>1
    /// detached-drain path becomes reachable. The provider pairs this with a matching
    /// admission-semaphore capacity. Must be called before any freeze (an empty ledger).
    #[cfg(test)]
    pub(crate) fn set_max_depth_for_test(&mut self, max_depth: usize) {
        debug_assert!(max_depth >= 1, "a drain ledger admits at least one generation");
        debug_assert!(
            self.generations.is_empty(),
            "set_max_depth_for_test must run before any freeze"
        );
        self.max_depth = max_depth;
        self.generations.reserve(max_depth);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn one_shard_gen(epoch: u64) -> FrozenGeneration {
        FrozenGeneration::freeze(
            vec![Arc::new(MemTier::empty())],
            vec![0],
            vec![0],
            Some(7),
            epoch,
        )
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
            FrozenGeneration::freeze(vec![Arc::new(MemTier::empty())], vec![0], vec![0], None, 1);
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
    fn freeze_assigns_monotone_ids() {
        let mut ledger = FrozenDrainLedger::new(4);
        let id_a = ledger.freeze(one_shard_gen(1)).map(|g| g.id).ok();
        let id_b = ledger.freeze(one_shard_gen(2)).map(|g| g.id).ok();
        assert_eq!(id_a, Some(0));
        assert_eq!(id_b, Some(1));
        // Ids are never reused: reclaim the front, freeze again, the new id keeps
        // climbing (a stale id addresses nothing rather than a later generation).
        ledger.front_mut().expect("front").transition(GenState::Sealed);
        ledger.reclaim_front();
        let id_c = ledger.freeze(one_shard_gen(3)).map(|g| g.id).ok();
        assert_eq!(id_c, Some(2));
    }

    #[test]
    fn discard_from_id_removes_a_non_terminal_generation() {
        // The abort path: a generation frozen but not driven to a terminal state
        // (an error mid-drain) must still be removable, WITHOUT the terminal-state
        // assertion `reclaim_front` makes, so a failed drain never strands it.
        let mut ledger = FrozenDrainLedger::new(1);
        let id = ledger.freeze(one_shard_gen(9)).map(|g| g.id).ok().expect("admitted");
        // Still Frozen — reclaim_front would assert; discard_from_id must not.
        assert_eq!(ledger.front().expect("front").state(), GenState::Frozen);
        let discarded = ledger.discard_from_id(id);
        assert_eq!(discarded.iter().map(|g| g.epoch).collect::<Vec<_>>(), vec![9]);
        assert!(ledger.is_empty());
        // Discarding an unknown id is a no-op (the happy path already reclaimed it).
        assert!(ledger.discard_from_id(id).is_empty());
    }

    #[test]
    fn discard_from_id_cascades_the_failing_generation_and_all_younger() {
        // The D>1 FAILURE contract: discarding an OLDER generation invalidates every
        // YOUNGER generation's window base, so the cascade removes the failing one AND
        // all behind it, leaving the older ones intact. Freeze A,B,C,D; fail B → the
        // cascade discards [B,C,D] and leaves [A].
        let mut ledger = FrozenDrainLedger::new(4);
        let id_a = ledger.freeze(one_shard_gen(10)).map(|g| g.id).ok().expect("A");
        let id_b = ledger.freeze(one_shard_gen(20)).map(|g| g.id).ok().expect("B");
        let _id_c = ledger.freeze(one_shard_gen(30)).map(|g| g.id).ok().expect("C");
        let _id_d = ledger.freeze(one_shard_gen(40)).map(|g| g.id).ok().expect("D");
        assert_eq!(ledger.depth(), 4);

        let discarded = ledger.discard_from_id(id_b);
        // Oldest-first, the failing generation first.
        assert_eq!(
            discarded.iter().map(|g| g.epoch).collect::<Vec<_>>(),
            vec![20, 30, 40],
            "cascade returns the failing generation and all younger, oldest-first"
        );
        assert_eq!(ledger.depth(), 1, "only the older survivor remains");
        assert_eq!(ledger.front().expect("front").id, id_a);
        // The survivor still publishes normally.
        ledger.front_mut().expect("front").transition(GenState::Spilling);
        ledger.front_mut().expect("front").transition(GenState::Published);
        assert_eq!(ledger.reclaim_terminal_prefix_ack(), Some(10));
        assert!(ledger.is_empty());
    }

    #[test]
    fn discard_from_id_of_the_front_drains_the_whole_ledger() {
        // Failing the FRONT (oldest) generation cascades the entire resident run —
        // every younger generation counted the front in its base.
        let mut ledger = FrozenDrainLedger::new(3);
        let id_a = ledger.freeze(one_shard_gen(10)).map(|g| g.id).ok().expect("A");
        ledger.freeze(one_shard_gen(20)).ok().expect("B");
        ledger.freeze(one_shard_gen(30)).ok().expect("C");
        let discarded = ledger.discard_from_id(id_a);
        assert_eq!(discarded.len(), 3, "the whole ledger cascades");
        assert!(ledger.is_empty());
    }

    #[test]
    fn reclaim_terminal_prefix_ack_single_generation_returns_its_epoch() {
        // D=1: publishing the sole generation makes the front terminal, so the sweep
        // reclaims exactly it and returns its epoch (byte-identical to the pre-3b
        // `fire(epoch); reclaim_front()` pair).
        let mut ledger = FrozenDrainLedger::new(1);
        assert!(ledger.freeze(one_shard_gen(42)).is_ok());
        ledger.front_mut().expect("front").transition(GenState::Spilling);
        ledger.front_mut().expect("front").transition(GenState::Published);
        assert_eq!(ledger.reclaim_terminal_prefix_ack(), Some(42));
        assert!(ledger.is_empty());
    }

    #[test]
    fn reclaim_terminal_prefix_ack_pins_at_unfinished_older_generation() {
        // The min-across-in-flight rule: a LATER generation publishing FIRST must NOT
        // advance the watermark past an unfinished earlier one. Freeze A (front) then
        // B; publish B while A is still Frozen → the sweep returns None and reclaims
        // nothing (A pins the ack). Then publish A → the sweep reclaims A AND the
        // already-published B in one contiguous run, returning the max (B's epoch).
        let mut ledger = FrozenDrainLedger::new(2);
        let id_a = ledger.freeze(one_shard_gen(10)).map(|g| g.id).ok().expect("A");
        let id_b = ledger.freeze(one_shard_gen(20)).map(|g| g.id).ok().expect("B");

        // B finishes encoding first and publishes — but A (the front) is not terminal.
        ledger.generation_mut_by_id(id_b).expect("B").transition(GenState::Spilling);
        ledger.generation_mut_by_id(id_b).expect("B").transition(GenState::Published);
        assert_eq!(
            ledger.reclaim_terminal_prefix_ack(),
            None,
            "B publishing first must not ack past the unfinished A"
        );
        assert_eq!(ledger.depth(), 2, "nothing reclaimed while A is in flight");

        // A publishes — now the contiguous terminal prefix is [A, B].
        ledger.generation_mut_by_id(id_a).expect("A").transition(GenState::Spilling);
        ledger.generation_mut_by_id(id_a).expect("A").transition(GenState::Published);
        assert_eq!(
            ledger.reclaim_terminal_prefix_ack(),
            Some(20),
            "reclaiming [A,B] fires the monotone max epoch once"
        );
        assert!(ledger.is_empty());
    }

    #[test]
    fn reclaim_terminal_prefix_ack_stops_at_a_gap() {
        // A gap in the middle stops the sweep: front A published, B still in flight,
        // C published. The sweep reclaims only A (returns A's epoch); B and C stay.
        // When B publishes, the next sweep reclaims [B, C].
        let mut ledger = FrozenDrainLedger::new(3);
        let id_a = ledger.freeze(one_shard_gen(10)).map(|g| g.id).ok().expect("A");
        let id_b = ledger.freeze(one_shard_gen(20)).map(|g| g.id).ok().expect("B");
        let id_c = ledger.freeze(one_shard_gen(30)).map(|g| g.id).ok().expect("C");

        for id in [id_a, id_c] {
            ledger.generation_mut_by_id(id).expect("gen").transition(GenState::Spilling);
            ledger.generation_mut_by_id(id).expect("gen").transition(GenState::Published);
        }
        assert_eq!(
            ledger.reclaim_terminal_prefix_ack(),
            Some(10),
            "the sweep stops at the still-in-flight B"
        );
        assert_eq!(ledger.depth(), 2);

        ledger.generation_mut_by_id(id_b).expect("B").transition(GenState::Spilling);
        ledger.generation_mut_by_id(id_b).expect("B").transition(GenState::Published);
        assert_eq!(ledger.reclaim_terminal_prefix_ack(), Some(30));
        assert!(ledger.is_empty());
    }

    #[test]
    fn reclaim_terminal_prefix_ack_min_across_all_publish_orders() {
        // Fuzz the ordered-publish + min-ack contract over EVERY order in which K
        // generations can finish encoding (publish). Invariants for each permutation:
        //  * the sequence of fired watermarks is strictly monotone increasing;
        //  * a watermark W is only ever fired once every generation with epoch <= W has
        //    been published (never acks past an unfinished older generation);
        //  * after all K publish, every generation is reclaimed and the final coverage
        //    is exactly the newest generation's epoch.
        // Epoch == freeze index so "epoch <= W" is "index <= W" (monotone by freeze).
        const K: usize = 5;

        // Heap's algorithm would do; a simple factorial-indexed permutation is enough.
        fn permutation(mut rank: usize, n: usize) -> Vec<usize> {
            let mut items: Vec<usize> = (0..n).collect();
            let mut out = Vec::with_capacity(n);
            let mut divisor = 1usize;
            for k in 2..=n {
                divisor *= k;
            }
            for k in (1..=n).rev() {
                divisor /= k;
                let idx = rank / divisor;
                rank %= divisor;
                out.push(items.remove(idx));
            }
            out
        }

        let mut factorial = 1usize;
        for k in 2..=K {
            factorial *= k;
        }

        for rank in 0..factorial {
            let publish_order = permutation(rank, K);
            let mut ledger = FrozenDrainLedger::new(K);
            for i in 0..K {
                // epoch = freeze index; ids are 0..K in freeze order.
                assert!(
                    ledger
                        .freeze(one_shard_gen(u64::try_from(i).expect("small")))
                        .is_ok()
                );
            }

            let mut published = [false; K];
            let mut last_fired: Option<u64> = None;
            for &gen_id in &publish_order {
                let id = u64::try_from(gen_id).expect("small");
                ledger.generation_mut_by_id(id).expect("gen").transition(GenState::Spilling);
                ledger.generation_mut_by_id(id).expect("gen").transition(GenState::Published);
                published[gen_id] = true;
                if let Some(w) = ledger.reclaim_terminal_prefix_ack() {
                    // Monotone strictly increasing across the whole schedule.
                    assert!(
                        last_fired.is_none_or(|prev| w > prev),
                        "rank#{rank} order {publish_order:?}: watermark {w} not > previous {last_fired:?}"
                    );
                    last_fired = Some(w);
                    // Safety: every generation with epoch <= W is durable (published).
                    for (idx, &done) in published.iter().enumerate() {
                        if u64::try_from(idx).expect("small") <= w {
                            assert!(
                                done,
                                "rank#{rank} order {publish_order:?}: acked {w} while gen {idx} unfinished"
                            );
                        }
                    }
                }
            }
            assert!(ledger.is_empty(), "rank#{rank}: not fully reclaimed");
            assert_eq!(
                last_fired,
                Some(u64::try_from(K - 1).expect("small")),
                "rank#{rank}: final watermark != newest generation epoch"
            );
        }
    }

    #[test]
    fn resident_prefix_counts_sum_over_resident_generations() {
        // Two-shard generations; base composes as the element-wise sum of the
        // resident generations' relative counts. This is the window-base offset the
        // NEXT freeze starts at, so its window never re-encodes an older resident
        // generation's front prefix (the D>1 double-count fix, pure-logic form).
        let mut ledger = FrozenDrainLedger::new(3);
        // Empty ledger ⇒ all-zero base (byte-identical at D=1).
        assert_eq!(ledger.resident_prefix_counts(2), vec![0, 0]);

        let gen_a = FrozenGeneration::freeze(
            vec![Arc::new(MemTier::empty()), Arc::new(MemTier::empty())],
            vec![0, 0],
            vec![2, 1],
            Some(1),
            10,
        );
        assert!(ledger.freeze(gen_a).is_ok());
        // Next freeze's base = gen A's relative counts.
        assert_eq!(ledger.resident_prefix_counts(2), vec![2, 1]);

        let gen_b = FrozenGeneration::freeze(
            vec![Arc::new(MemTier::empty()), Arc::new(MemTier::empty())],
            vec![2, 1],
            vec![3, 4],
            Some(2),
            20,
        );
        assert!(ledger.freeze(gen_b).is_ok());
        // Now base = A + B, element-wise.
        assert_eq!(ledger.resident_prefix_counts(2), vec![5, 5]);
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

// ---------------------------------------------------------------------------
// loom model-check of the pipelined-drain concurrency invariants (Stage 2b
// Step 3d). Compiled ONLY under `RUSTFLAGS="--cfg cayenne_loom"`; run with:
//   RUSTFLAGS="--cfg cayenne_loom" cargo test -p cayenne --lib \
//     provider::drain::loom_model --release
//
// WHY A REDUCED MODEL (not the production code compiled under loom): the
// production drain orchestration (`table.rs`) is async and built on
// `tokio::sync::{Semaphore, Notify}` + `tokio::spawn`, none of which loom
// shims — loom instruments only `std`/`loom` sync primitives + `loom::thread`.
// So this models the SAME happens-before structure with loom-shimmable
// analogs, while driving the REAL `FrozenDrainLedger` logic
// (`freeze` / `reclaim_terminal_prefix_ack` / `discard_from_id` / `front_id` /
// `is_resident`) so the ordered-publish + min-across-in-flight ack + cascade
// contracts under test carry NO transcription drift from production:
//
//   * `parking_lot::Mutex<FrozenDrainLedger>` (`drain_ledger`)
//        -> `loom::sync::Mutex<FrozenDrainLedger>`
//   * `tokio::sync::Notify` (`drain_publish_notify`, the arm-before-check
//     ordered-publish gate + reclaim/discard `notify_waiters()`)
//        -> `loom::sync::Condvar` on the ledger mutex — the textbook
//           lost-wakeup-free "re-check the predicate under the lock in a loop,
//           notify_all while holding the lock" pattern. This is a faithful and
//           strictly MORE-conservative analog of the async arm-before-check
//           idiom: the condvar wait atomically releases the lock and parks, and
//           the notifier mutates the ledger + signals under that same lock, so
//           no wakeup can be lost between a waiter's predicate check and its
//           park (exactly the property `Notified::enable()`-before-check buys
//           the async gate).
//   * `tokio::spawn` detached drain            -> `loom::thread::spawn`
//   * off-lock encode/PUT completing unordered -> a `loom::thread::yield_now()`
//     scheduling point between freeze and the publish gate
//
// Generations carry EMPTY shard vectors: the ledger's ack / cascade / front
// logic never reads `shard_snapshots` (only `id` / `epoch` / `state`), so a
// zero-shard generation drives every interleaving under test without
// constructing a (loom-hostile, Arrow-heavy, atomic-laden) `MemTier`. State
// spaces are kept SMALL (2-3 generations, one yield point per drain) because
// loom is exponential in the number of shared-memory operations.
//
// NOTE ON miri: the drain path contains NO `unsafe`, raw pointers, `UnsafeCell`,
// or hand-rolled atomics (unlike the `task_queue` mailbox, whose `AtomicPtr` +
// `NonNull` baton transit is why that module carries a miri target) — it is all
// safe Rust over `VecDeque` + a mutex + tokio's `Notify`/`Semaphore`. There is
// therefore no UB surface for miri to check here; loom fully covers the
// happens-before invariants and miri is not warranted for Step 3d.
#[cfg(all(test, cayenne_loom))]
mod loom_model {
    use super::{FrozenDrainLedger, FrozenGeneration, GenState};
    use loom::sync::atomic::{AtomicU64, Ordering};
    use loom::sync::{Arc, Condvar, Mutex};

    /// A zero-shard frozen generation carrying only the `epoch` (and a dummy
    /// reserved sequence) — enough to drive the ledger's ack / cascade / front
    /// logic, which never touches `shard_snapshots`. `id` is assigned by
    /// `FrozenDrainLedger::freeze` on admission.
    fn model_gen(epoch: u64) -> FrozenGeneration {
        FrozenGeneration::freeze(Vec::new(), Vec::new(), Vec::new(), Some(1), epoch)
    }

    /// The outcome of the modeled ordered-publish gate — the loom analog of the
    /// production `PublishTurn` enum (which lives in `table.rs` and cannot be
    /// imported here).
    #[derive(PartialEq, Eq, Debug)]
    enum Turn {
        Ours,
        Discarded,
    }

    /// The shared drain state a set of racing drains operate over: the REAL
    /// ledger under a loom mutex, the condvar that models `drain_publish_notify`,
    /// and the observation logs the assertions read.
    struct DrainModel {
        ledger: Mutex<FrozenDrainLedger>,
        /// Faithful analog of `drain_publish_notify`: parks a drain until its
        /// generation is the ledger front (its ordered-publish turn) or it has
        /// been cascade-discarded.
        gate: Condvar,
        /// Generation ids in the order they WON their publish turn — asserted to
        /// be strictly oldest-first (the ordered-publish invariant).
        publish_order: Mutex<Vec<u64>>,
        /// Every watermark returned by `reclaim_terminal_prefix_ack`, in the
        /// order it was PRODUCED under the ledger lock — asserted monotone
        /// non-decreasing (the min-across-in-flight ack invariant). This is the
        /// sequence the ordered-publish contract actually guarantees; the
        /// downstream FIRE order can differ (see [`Self::fire`]).
        produced: Mutex<Vec<u64>>,
        /// The effective durable watermark the SOURCE SLOT sees — the loom analog
        /// of `last_fired_durable_epoch` (`fire_slot_advancer` advances it with a
        /// `fetch_max` off the ledger lock). Modeled explicitly so loom explores
        /// the fire reorder and checks it is monotone under every schedule.
        effective_wm: AtomicU64,
    }

    impl DrainModel {
        fn new(ledger: FrozenDrainLedger) -> Arc<Self> {
            Arc::new(Self {
                ledger: Mutex::new(ledger),
                gate: Condvar::new(),
                publish_order: Mutex::new(Vec::new()),
                produced: Mutex::new(Vec::new()),
                effective_wm: AtomicU64::new(0),
            })
        }

        /// The ordered-publish gate (`await_checkpoint_publish_turn` analog):
        /// park on the condvar until this generation is the front (its turn) or
        /// no longer resident (cascade-discarded by an older failure). Re-checks
        /// the predicate under the held ledger lock after every wakeup.
        fn await_publish_turn(&self, id: u64) -> Turn {
            let mut ledger = self.ledger.lock().expect("ledger lock");
            loop {
                if ledger.front_id() == Some(id) {
                    return Turn::Ours;
                }
                if !ledger.is_resident(id) {
                    return Turn::Discarded;
                }
                ledger = self.gate.wait(ledger).expect("condvar wait");
            }
        }

        /// The publish + ordered ack (`publish_generation_and_ack` analog): under
        /// ONE ledger lock, mark this generation terminal, reclaim the maximal
        /// contiguous terminal prefix, RECORD the returned watermark, and wake
        /// parked drains. Notifying under the lock is the lost-wakeup-free condvar
        /// discipline.
        ///
        /// The watermark is recorded INSIDE the critical section, at the reclaim
        /// point, because that is where the ordered-publish contract's ordering is
        /// determined — the reclaim-returned sequence under the lock is what must
        /// be monotone. Production fires the watermark (`fire_slot_advancer`)
        /// AFTER releasing the ledger lock, so two concurrent drains' fire calls
        /// can legitimately REORDER (loom surfaced exactly this — an initial model
        /// that recorded the fire order saw `[20, 10]`). That reorder is benign in
        /// production and NOT a bug: `fire_slot_advancer` advances
        /// `last_fired_durable_epoch` with a `fetch_max` (monotone) and then calls
        /// `SlotAdvancer::on_checkpoint_durable(w)`, which drains the deferred
        /// committer queue by an ABSOLUTE threshold (`epoch <= w`, FIFO) — so a
        /// higher watermark fired ahead of a lower one SUBSUMES it and the lower
        /// fire becomes a no-op. The downstream fire's reorder-safety is modeled
        /// separately (3d-ii); here we check the reclaim-order contract the ledger
        /// lock actually guarantees.
        fn publish_and_ack(&self, id: u64) {
            let ack = {
                let mut ledger = self.ledger.lock().expect("ledger lock");
                match ledger.generation_mut_by_id(id) {
                    Some(g) => g.transition(GenState::Published),
                    None => panic!("published generation {id} was no longer resident"),
                }
                let ack = ledger.reclaim_terminal_prefix_ack();
                if let Some(epoch) = ack {
                    self.produced.lock().expect("produced lock").push(epoch);
                }
                self.gate.notify_all();
                ack
            };
            // Fire OUTSIDE the ledger lock, exactly as `publish_generation_and_ack`
            // calls `fire_slot_advancer` after releasing it — so concurrent drains'
            // fires can reorder, and loom explores that.
            if let Some(epoch) = ack {
                self.fire(epoch);
            }
        }

        /// The downstream slot ack (`fire_slot_advancer` analog), run OFF the
        /// ledger lock. Advances the effective watermark with a `fetch_max`
        /// (`last_fired_durable_epoch`), so it is monotone regardless of the order
        /// two concurrent drains reach this. The safety of draining committers by
        /// the absolute threshold `epoch <= w` rests on: when a watermark `w` is
        /// fired, every epoch `<= w` was already PRODUCED by an earlier reclaim
        /// (min-across-in-flight guarantees `w` is only produced once its whole
        /// contiguous prefix is durable), which this asserts against `produced`.
        fn fire(&self, w: u64) {
            assert!(
                self.produced.lock().expect("produced lock").contains(&w),
                "fired watermark {w} must have been produced under the ledger lock first"
            );
            self.effective_wm.fetch_max(w, Ordering::SeqCst);
        }

        /// One detached CHECKPOINT drain: encode off-lock (a yield point, so loom
        /// explores every completion order), claim the bake (`-> Spilling`), wait
        /// for the ordered-publish turn, then publish + ack. On `Discarded` it
        /// aborts without publishing (its rows stay live for replay).
        fn drain_checkpoint(&self, id: u64) {
            // Encode/PUT overlaps across drains and completes in an arbitrary
            // order — the whole point of the pipeline. Model that as a scheduling
            // point before the gate.
            loom::thread::yield_now();
            self.transition_to_spilling(id);
            match self.await_publish_turn(id) {
                Turn::Ours => {
                    self.publish_order.lock().expect("publish_order lock").push(id);
                    self.publish_and_ack(id);
                }
                Turn::Discarded => {
                    // Abort: no metastore commit, no fence swap, no `retain_after`
                    // clear — the generation's rows stay live in the tier.
                }
            }
        }

        /// One detached CHECKPOINT drain that FAILS mid-flight — the loom analog of
        /// `arm_checkpoint_spill_fault` (which aborts in `Spilling`, after this
        /// drain has claimed its own bake). On the `?`-error return, production's
        /// `DrainCleanup::drop` runs the CASCADE: discard this generation AND every
        /// younger one still resident behind it, then wake the gate so a
        /// cascade-discarded younger drain re-checks and aborts. Models exactly
        /// that: `discard_from_id(id)` + `notify_all`, under the ledger lock.
        fn drain_checkpoint_failing(&self, id: u64) {
            loom::thread::yield_now();
            self.transition_to_spilling(id);
            // FAULT at the Spilling boundary → DrainCleanup cascade.
            let mut ledger = self.ledger.lock().expect("ledger lock");
            ledger.discard_from_id(id);
            self.gate.notify_all();
        }

        /// Claim this drain's own bake by driving its generation `Frozen ->
        /// Spilling`, mirroring the production `with_generation_by_id(id, |g|
        /// g.transition(Spilling))` at `table.rs:22955`. Production `debug_assert`s
        /// the generation is still resident here; this MODELS RELEASE SEMANTICS —
        /// a no-op when the generation was already cascade-discarded by an older
        /// failure (see the D>1 finding recorded on `cascade_from_front_*` below).
        fn transition_to_spilling(&self, id: u64) {
            let mut ledger = self.ledger.lock().expect("ledger lock");
            if let Some(g) = ledger.generation_mut_by_id(id) {
                g.transition(GenState::Spilling);
            }
            // No-op when the generation was already cascade-discarded by an older
            // failure — the release-correct behavior. A probe that instead mirrored
            // production's `debug_assert!(result.is_some())` here FAILED under loom
            // (a younger drain reached this after gen 0's cascade removed it), which
            // is the D>1 finding recorded on the cascade test below.
        }

        /// The seal drain (`drain_seal_generation` analog): mark the sealed
        /// generation terminal and reclaim it. The seal is single-ordered and runs
        /// under the quiesce barrier (see [`seal_quiesces_over_an_empty_ledger`]),
        /// so its generation is the only resident one — it is NOT windowed and gets
        /// no ordered-publish gate.
        fn seal_generation(&self, id: u64) {
            let mut ledger = self.ledger.lock().expect("ledger lock");
            match ledger.generation_mut_by_id(id) {
                Some(g) => g.transition(GenState::Sealed),
                None => panic!("sealing generation {id} was no longer resident"),
            }
            ledger.reclaim_terminal_prefix_ack();
            self.gate.notify_all();
        }
    }

    /// A counting semaphore modeled on loom primitives — the analog of the
    /// `drain_admission` `tokio::sync::Semaphore`, which loom does not shim.
    /// `acquire(n)` waits until `n` permits are available and then takes all `n`
    /// ATOMICALLY under the lock, so a waiter NEVER holds partial permits while
    /// blocked (the property tokio's `acquire_many` guarantees and the seal
    /// quiesce barrier's deadlock-freedom argument relies on). `acquire(1)` is the
    /// per-drain admission; `acquire(max_depth)` is the seal barrier.
    struct ModelSemaphore {
        permits: Mutex<usize>,
        available: Condvar,
    }

    impl ModelSemaphore {
        fn new(permits: usize) -> Arc<Self> {
            Arc::new(Self {
                permits: Mutex::new(permits),
                available: Condvar::new(),
            })
        }

        fn acquire(&self, n: usize) {
            let mut permits = self.permits.lock().expect("permits lock");
            while *permits < n {
                permits = self.available.wait(permits).expect("permits wait");
            }
            *permits -= n;
        }

        fn release(&self, n: usize) {
            let mut permits = self.permits.lock().expect("permits lock");
            *permits += n;
            self.available.notify_all();
        }
    }

    /// ORDERED-PUBLISH happens-before (property 1), two generations. Two drains
    /// race their encode + publish over a shared ledger holding gen 0 (epoch 10)
    /// ahead of gen 1 (epoch 20). loom explores every interleaving of the two
    /// drains' encode-completion order against the gate / publish / reclaim /
    /// notify operations, and checks that NO interleaving lets gen 1 publish
    /// before gen 0: the publish section (metastore commit → fence swap →
    /// `retain_after` front-clear → slot ack) runs strictly oldest-first, so the
    /// win order is [0, 1] and the ack watermark is monotone (10 then 20) in
    /// EVERY schedule. Gen 1's `retain_after` front-clear is only ever safe once
    /// it is the front — i.e. once gen 0's front prefix has already been cleared —
    /// which is exactly what winning-Ours-only-when-front enforces here.
    #[test]
    fn ordered_publish_two_generations() {
        loom::model(|| {
            let mut ledger = FrozenDrainLedger::new(2);
            // Freezes are serialized under `mem_checkpoint_lock` in production, so
            // model them up-front + ordered (ids 0, 1); only the drains race.
            ledger.freeze(model_gen(10)).ok().expect("freeze gen0");
            ledger.freeze(model_gen(20)).ok().expect("freeze gen1");
            let model = DrainModel::new(ledger);

            let m0 = Arc::clone(&model);
            let d0 = loom::thread::spawn(move || m0.drain_checkpoint(0));
            let m1 = Arc::clone(&model);
            let d1 = loom::thread::spawn(move || m1.drain_checkpoint(1));
            d0.join().expect("drain 0 joins");
            d1.join().expect("drain 1 joins");

            assert_eq!(
                *model.publish_order.lock().expect("publish_order"),
                vec![0, 1],
                "publish turns must be strictly oldest-first"
            );
            assert_eq!(
                *model.produced.lock().expect("produced"),
                vec![10, 20],
                "ack watermark must advance monotonically, one generation at a time"
            );
            assert_eq!(
                model.effective_wm.load(Ordering::SeqCst),
                20,
                "the source slot ends at the newest durable epoch"
            );
            assert!(
                model.ledger.lock().expect("ledger").is_empty(),
                "both generations must be reclaimed"
            );
        });
    }

    /// CASCADE-DISCARD on failure (property 3) + min-ack never advances past a
    /// FAILED older generation, two generations. Gen 0 (epoch 10) FAILS at its
    /// Spilling boundary; gen 1 (epoch 20) is younger. loom explores every
    /// interleaving and checks:
    ///   * gen 0's failure CASCADE-discards gen 0 AND gen 1 (`discard_from_id`);
    ///   * gen 1 (younger) ABORTS at the ordered-publish gate — it observes
    ///     `!is_resident` (`Turn::Discarded`) and never publishes, so no metastore
    ///     commit / fence swap / `retain_after` clear runs and its rows stay live
    ///     for the source's PK-idempotent replay;
    ///   * NOTHING is acked — the effective slot watermark never advances past the
    ///     failure (stays 0), so a crash replays from the last durable epoch;
    ///   * the ledger ends empty (no stranded generation blocks the next freeze).
    ///
    /// D>1 FINDING (loom-surfaced, recorded here). The production
    /// `with_generation_by_id(id, |g| g.transition(Spilling))` at `table.rs:22955`
    /// `debug_assert!`s the generation is still resident — but this model shows a
    /// younger drain can reach that transition AFTER an older failure already
    /// cascade-discarded it (gen 0 fails and discards gen 0+1 before gen 1's
    /// detached task is polled to its own Spilling transition — the spawn-to-poll
    /// gap). RELEASE builds handle it correctly (the transition is a no-op and the
    /// drain then aborts at the gate), but DEBUG/test builds would trip that
    /// `debug_assert`. It is unreachable at the config default (`D = 1`, no older
    /// generation to fail-cascade) and the existing D>1 tests avoid the schedule
    /// via the deterministic drain gate (which pauses AFTER the Spilling
    /// transition). This model deliberately uses release semantics
    /// ([`DrainModel::transition_to_spilling`] no-ops when the generation is gone)
    /// so it verifies the SAFETY invariants; the too-strong `debug_assert` is
    /// flagged for a separate (D>1-only, byte-identical at D=1) fix.
    #[test]
    fn cascade_from_front_failure_aborts_younger_and_pins_slot() {
        loom::model(|| {
            let mut ledger = FrozenDrainLedger::new(2);
            ledger.freeze(model_gen(10)).ok().expect("freeze gen0");
            ledger.freeze(model_gen(20)).ok().expect("freeze gen1");
            let model = DrainModel::new(ledger);

            let m0 = Arc::clone(&model);
            let d0 = loom::thread::spawn(move || m0.drain_checkpoint_failing(0));
            let m1 = Arc::clone(&model);
            let d1 = loom::thread::spawn(move || m1.drain_checkpoint(1));
            d0.join().expect("failing drain joins");
            d1.join().expect("younger drain joins");

            assert!(
                model.publish_order.lock().expect("publish_order").is_empty(),
                "no generation may publish: gen0 failed, gen1 was cascade-discarded"
            );
            assert!(
                model.produced.lock().expect("produced").is_empty(),
                "no watermark may be produced past the failure"
            );
            assert_eq!(
                model.effective_wm.load(Ordering::SeqCst),
                0,
                "the slot never advances past a failed older generation"
            );
            assert!(
                model.ledger.lock().expect("ledger").is_empty(),
                "the cascade leaves no stranded generation"
            );
        });
    }

    /// SEAL QUIESCE barrier deadlock-freedom (property 4), `max_depth = 2`. A
    /// detached checkpoint drain (admission permit 1) races a seal. The seal's
    /// `acquire_seal_quiesce_barrier` analog acquires ALL `max_depth` permits
    /// before it captures, so it captures over an EMPTY ledger (no resident
    /// checkpoint window for its un-windowed `unsealed_view` shadow to
    /// doubly-persist). loom explores every interleaving and checks:
    ///   * DEADLOCK-FREEDOM — both threads always terminate (loom flags any
    ///     schedule that leaves all threads blocked). The checkpoint's drain
    ///     (publish + reclaim) needs neither a permit nor anything the seal holds,
    ///     so a checkpoint holding its admission permit always completes and
    ///     releases it, letting the seal's `acquire(2)` make progress; the
    ///     semaphore takes all permits atomically, so the seal never holds a
    ///     partial set while waiting;
    ///   * QUIESCE — the seal only ever captures over an empty ledger (asserted at
    ///     its freeze), i.e. every resident checkpoint has published + reclaimed +
    ///     released its permit before the barrier is satisfied, so no checkpoint
    ///     window is doubly-persisted by the seal's whole-active shadow.
    ///
    /// The checkpoint drain provably takes no capture lock while off-lock (it
    /// released `mem_checkpoint_lock` after its freeze), so the barrier's
    /// `write`/`mem_checkpoint_lock` acquisition after it wins the permits cannot
    /// deadlock against an in-flight checkpoint — that lock non-dependency is
    /// structural in the production code and so is not re-modeled here; this model
    /// isolates the semaphore barrier that the deadlock-freedom argument turns on.
    #[test]
    fn seal_quiesces_over_an_empty_ledger() {
        loom::model(|| {
            let model = DrainModel::new(FrozenDrainLedger::new(2));
            let admission = ModelSemaphore::new(2);

            // Detached checkpoint: admission permit -> freeze -> drain -> reclaim ->
            // release (the permit rides the whole task, released after reclaim).
            let m_ckpt = Arc::clone(&model);
            let a_ckpt = Arc::clone(&admission);
            let checkpoint = loom::thread::spawn(move || {
                a_ckpt.acquire(1);
                let id = {
                    let mut ledger = m_ckpt.ledger.lock().expect("ledger lock");
                    ledger.freeze(model_gen(10)).ok().expect("checkpoint freeze").id
                };
                m_ckpt.drain_checkpoint(id);
                a_ckpt.release(1);
            });

            // Seal: quiesce barrier (ALL permits) -> capture over the empty ledger
            // -> seal -> release.
            let m_seal = Arc::clone(&model);
            let a_seal = Arc::clone(&admission);
            let seal = loom::thread::spawn(move || {
                a_seal.acquire(2);
                let id = {
                    let mut ledger = m_seal.ledger.lock().expect("ledger lock");
                    assert!(
                        ledger.is_empty(),
                        "seal must capture over an empty (quiesced) ledger — no resident checkpoint window"
                    );
                    ledger.freeze(model_gen(20)).ok().expect("seal freeze").id
                };
                m_seal.seal_generation(id);
                a_seal.release(2);
            });

            checkpoint.join().expect("checkpoint drain joins (no deadlock)");
            seal.join().expect("seal joins (no deadlock)");
            assert!(
                model.ledger.lock().expect("ledger").is_empty(),
                "both the checkpoint and the seal reclaim their generations"
            );
        });
    }
}
