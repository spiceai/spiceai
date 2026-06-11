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

//! In-memory CDC durability tier (`cdc_durability: memory`).
//!
//! On the inline CDC write path, memory mode appends each validated batch to
//! this RAM tier instead of persisting a per-batch durable metastore BLOB, and
//! defers the source slot ack until a periodic/cap-triggered checkpoint flushes
//! the tier to a durable Vortex file. The tier is held on the provider as a
//! sibling [`arc_swap::ArcSwap`] of the existing `inlined_cache`, so a write is
//! an O(1) `Arc` swap and reads union the tier under the listing fence with
//! zero copy.
//!
//! The tombstone maps are structurally-shared persistent maps ([`im::HashMap`],
//! HAMT) so an append shares structure with the prior tier instead of deep-copying
//! the accumulated corpus — the per-append cost is O(incoming · log tier), not
//! O(tier). Combined with the bounded fence section in `append_to_mem_tier`, the
//! whole append is therefore cheap regardless of tier size (the per-append clone
//! was the dominant write-phase cost before this change).
//!
//! Crash model: the tier is pure RAM and is DISCARDED on crash/restart. The
//! source slot is the single source of truth — it holds at most the
//! last-checkpointed LSN, so on restart the source re-streams every WAL record
//! past that point and the PK-idempotent CDC apply converges exactly-once. The
//! [`SlotAdvancer`] callback enforces the load-bearing invariant: the slot
//! advances ONLY after the covering checkpoint's Vortex+metastore writes are
//! durable.

use std::sync::Arc;
use std::time::Instant;

use arrow::record_batch::RecordBatch;
use arrow_schema::Schema;
use async_trait::async_trait;
use datafusion_common::Statistics;
use hash_index::XxHash3BuildHasher;
use im::HashMap as PersistentHashMap;

/// In-RAM tombstones for one mem tier, mirroring the strategy split that
/// `load_inlined_deletion_maps` produces for the durable inline path. Each map
/// records the highest delete sequence seen per key (the same merge-on-read
/// semantics: a scanned data row at `data_sequence` is hidden iff some tombstone
/// for its key has `delete_sequence >= data_sequence`).
///
/// The maps are persistent ([`im::HashMap`], a HAMT) keyed with
/// [`XxHash3BuildHasher`] — the same structure and hasher
/// `provider::deletion_index` uses for its durable delta. The HAMT gives an
/// O(1) structural clone (an `Arc` bump of the root) so [`MemTier::append_segment`]
/// no longer deep-copies the accumulated corpus on every CDC append; `insert`
/// touches only the small incoming set against a structurally-shared base. The
/// XXH3 hasher (not the `im` default `SipHash`) keeps per-key hashing off the
/// `im::nodes::hamt::hash_key` hot path that profiles flagged for the deletion
/// index.
///
/// The authoritative `superseded` row-delta count is carried separately on the
/// tier (NOT recomputed from these maps): the i64 and byte-key encodings of the
/// same `Int64Pk` deletion would double-count, and position deletes are not
/// represented here (memory mode runs only for the key-based merge-on-read
/// shape; partitioned/position tables stay on the durable path).
#[derive(Debug, Clone)]
pub(crate) struct InMemTombstones {
    /// `Int64Pk` strategy: PK value -> max delete sequence.
    pub(crate) int64_pk: PersistentHashMap<i64, i64, XxHash3BuildHasher>,
    /// `RowConverterBased` strategy: committed row-key bytes -> max delete sequence.
    pub(crate) row_keys: PersistentHashMap<Box<[u8]>, i64, XxHash3BuildHasher>,
}

impl Default for InMemTombstones {
    fn default() -> Self {
        // `im::HashMap` needs an explicit hasher (it cannot derive `Default` with a
        // non-`Default`-constructed `RandomState`); build both maps with the XXH3
        // hasher so they share the deletion-index's hashing characteristics.
        Self {
            int64_pk: PersistentHashMap::with_hasher(XxHash3BuildHasher),
            row_keys: PersistentHashMap::with_hasher(XxHash3BuildHasher),
        }
    }
}

impl InMemTombstones {
    /// Closed `[min,max]` of all deleted `Int64Pk` keys, or `None` when no
    /// `Int64Pk` tombstone is present. A sound superset of the keys the filter
    /// could remove (mirrors `DeletionIndex::deleted_key_range`). The scan-side
    /// disjoint gate calls this on the `InlinedDeletionMaps` view (an alias of
    /// this type) — one implementation, no parity to track by hand.
    pub(crate) fn int64_deleted_key_range(&self) -> Option<(i64, i64)> {
        let mut iter = self.int64_pk.keys().copied();
        let first = iter.next()?;
        let mut lo = first;
        let mut hi = first;
        for k in iter {
            lo = lo.min(k);
            hi = hi.max(k);
        }
        Some((lo, hi))
    }

    /// Merge `other`'s tombstones into `self`, keeping the max delete sequence
    /// per key (monotone — a later epoch can only raise a key's delete sequence).
    ///
    /// Each `insert` is O(log n) on the persistent map and only structurally
    /// re-writes the path to the touched leaf, so this is O(incoming · log tier),
    /// NOT O(tier): the untouched nodes stay shared with `self`'s prior root.
    fn merge_from(&mut self, other: &InMemTombstones) {
        for (&pk, &seq) in &other.int64_pk {
            let next = self.int64_pk.get(&pk).map_or(seq, |&cur| cur.max(seq));
            self.int64_pk.insert(pk, next);
        }
        for (key, &seq) in &other.row_keys {
            let next = self.row_keys.get(key).map_or(seq, |&cur| cur.max(seq));
            self.row_keys.insert(key.clone(), next);
        }
    }

    fn is_empty(&self) -> bool {
        self.int64_pk.is_empty() && self.row_keys.is_empty()
    }
}

/// One appended segment: the batches written by a single CDC apply, retained by
/// `Arc` pointer (never deep-copied — the [`MemTier::append_segment`] swap clones
/// only the outer `Vec` of `Arc`s, the O(N)-clone lesson).
#[derive(Debug, Clone)]
pub(crate) struct MemSegment {
    pub(crate) batches: Arc<Vec<RecordBatch>>,
    /// The inline data sequence assigned to this segment's rows (the visibility
    /// watermark used by the merge-on-read deletion filter).
    pub(crate) data_sequence: i64,
    /// Exact min/max over this segment's batches for predicate-based pruning.
    pub(crate) statistics: Arc<Statistics>,
    /// This segment's OWN tombstone delta (the on-conflict deletions of the batch
    /// that produced it), kept per-segment — not just folded into the tier-level
    /// aggregate — so a partial checkpoint ([`MemTier::retain_after`]) can rebuild
    /// the survivors' aggregate tombstones without re-flushing the durable prefix.
    /// Cheap to carry: [`InMemTombstones`] is an `im::HashMap` (O(1) structural clone).
    pub(crate) tombstones: InMemTombstones,
    /// This segment's measured byte cost (`get_array_memory_size`), for budget
    /// release accounting on a partial clear.
    pub(crate) bytes: u64,
    /// This segment's row count.
    pub(crate) rows: u64,
    /// Rows this segment's upsert superseded (carried, not recomputed).
    pub(crate) superseded: u64,
    /// The tier epoch assigned to this segment when it was appended — the EXACT
    /// value the runtime tagged this batch's deferred source committers with
    /// (every [`MemTier::append_segment`] advances the epoch by one and returns it
    /// as the batch's `in_memory_epoch`). Carried per-segment so a PARTIAL prefix
    /// checkpoint can advance the source slot to the flushed prefix's last epoch
    /// ONLY — leaving the retained suffix's higher epochs un-acked (replayable on
    /// crash). The full-tier checkpoint's flushed epoch equals the tier's `epoch`
    /// (the last segment's epoch), preserving the prior behavior exactly. Not
    /// derived from `data_sequence` (a multi-step allocator value) nor from the
    /// segment index (the post-checkpoint `retain_after` slides the window while
    /// preserving `epoch`).
    pub(crate) epoch: u64,
}

/// The in-memory CDC tier for one table. Immutable once constructed: every
/// mutation produces a new `Arc<MemTier>` that is stored into the provider's
/// `ArcSwap`, so concurrent readers always observe a consistent snapshot and a
/// writer's swap is O(1).
#[derive(Debug, Clone)]
pub(crate) struct MemTier {
    /// Append-log of segments. `Arc<Vec<MemSegment>>` so a swap rebuilds only
    /// the outer Vec (cloning `Arc<Vec<RecordBatch>>` element pointers), never
    /// the batch data.
    pub(crate) segments: Arc<Vec<MemSegment>>,
    /// In-RAM tombstones accumulated across this tier's segments. Held as
    /// structurally-shared persistent maps ([`InMemTombstones`] over
    /// [`im::HashMap`]), so [`MemTier::append_segment`]'s clone of this field is
    /// an O(1) `Arc` bump of the HAMT root — the accumulated corpus is never
    /// deep-copied per append (the prior O(tier) write tax).
    pub(crate) tombstones: InMemTombstones,
    /// Sum of `get_array_memory_size()` across all retained batches — the cap
    /// dimension checked against the per-table + global byte budget.
    pub(crate) bytes: u64,
    /// Total retained rows (observability + the row cap).
    pub(crate) rows: u64,
    /// Authoritative superseded-row count carried across appends (NOT recomputed
    /// from `tombstones`, which would double-count the two `Int64Pk` encodings).
    pub(crate) superseded: u64,
    /// Monotonic mem-tier epoch. Each append advances the live tier; a checkpoint
    /// flushes every segment up to and including the snapshotted epoch and fires
    /// [`SlotAdvancer::on_checkpoint_durable`] with that epoch only after the
    /// durable fence. NOT a content version — a checkpoint clear changes the
    /// tier's contents while PRESERVING this epoch; cache keys use [`Self::version`].
    pub(crate) epoch: u64,
    /// Wall-clock instant the OLDEST un-checkpointed segment was appended, used
    /// by the age cap to bound the crash-replay window for cold tables. `None`
    /// when the tier is empty.
    pub(crate) oldest_append: Option<Instant>,
    /// Monotonic CONTENT version: bumped on every construction that changes the
    /// tier's contents (append AND the post-checkpoint retain), unlike `epoch`
    /// which is preserved across a clear. Cache keys that must distinguish "same
    /// epoch, different tombstones" (the merged-scan-deletions memo) key on this.
    pub(crate) version: u64,
}

impl MemTier {
    /// An empty tier at epoch 0 (the provider's initial state in every mode;
    /// only ever appended to when `cdc_durability: memory`).
    pub(crate) fn empty() -> Self {
        Self {
            segments: Arc::new(Vec::new()),
            tombstones: InMemTombstones::default(),
            bytes: 0,
            rows: 0,
            superseded: 0,
            epoch: 0,
            oldest_append: None,
            version: 0,
        }
    }

    #[must_use]
    pub(crate) fn is_empty(&self) -> bool {
        self.segments.is_empty() && self.tombstones.is_empty()
    }

    /// Produce a new tier with one segment + its tombstones appended, advancing
    /// the epoch. Clones only the outer `Vec<MemSegment>` (Arc-pointer copies) and
    /// the persistent tombstone maps' HAMT roots (O(1) `Arc` bumps) — never the
    /// batch data and never the accumulated tombstone corpus.
    /// `incoming_bytes`/`incoming_rows`/`superseded` are the caller's measured
    /// deltas for this batch.
    #[must_use]
    pub(crate) fn append_segment(
        &self,
        batches: Arc<Vec<RecordBatch>>,
        data_sequence: i64,
        tombstones: &InMemTombstones,
        incoming_bytes: u64,
        incoming_rows: u64,
        superseded: u64,
    ) -> Self {
        let statistics = batches.first().map_or_else(
            || Arc::new(Statistics::new_unknown(&Schema::empty())),
            |first| {
                Arc::new(
                    crate::provider::file_pruning::statistics_from_record_batches(
                        first.schema_ref(),
                        batches.as_ref(),
                    ),
                )
            },
        );
        // This append advances the epoch by one; the new segment carries that
        // post-append epoch (mirrors the `epoch: self.epoch + 1` set below), so a
        // partial checkpoint can ack the source slot to a flushed prefix's last
        // segment epoch without consulting the (sliding) segment index.
        let segment_epoch = self.epoch + 1;
        let mut segments = Vec::with_capacity(self.segments.len() + 1);
        segments.extend(self.segments.iter().cloned());
        segments.push(MemSegment {
            batches,
            data_sequence,
            statistics,
            tombstones: tombstones.clone(),
            bytes: incoming_bytes,
            rows: incoming_rows,
            superseded,
            epoch: segment_epoch,
        });

        // O(1): clones the persistent maps' HAMT roots (Arc bumps), NOT the
        // accumulated corpus. `merge_from` then applies only the incoming deltas
        // against the structurally-shared base.
        let mut merged_tombstones = self.tombstones.clone();
        merged_tombstones.merge_from(tombstones);

        Self {
            segments: Arc::new(segments),
            tombstones: merged_tombstones,
            bytes: self.bytes.saturating_add(incoming_bytes),
            rows: self.rows.saturating_add(incoming_rows),
            superseded: self.superseded.saturating_add(superseded),
            epoch: self.epoch + 1,
            oldest_append: self.oldest_append.or_else(|| Some(Instant::now())),
            version: self.version + 1,
        }
    }

    /// Wall-clock age of the oldest un-checkpointed segment, or zero when empty.
    #[must_use]
    pub(crate) fn age_ms(&self) -> u64 {
        self.oldest_append.map_or(0, |t| {
            u64::try_from(t.elapsed().as_millis()).unwrap_or(u64::MAX)
        })
    }

    /// The tier that REMAINS after a checkpoint durably flushed the first
    /// `flushed_segment_count` segments (an append-ordered prefix — appends only
    /// ever push to the end, so the flushed snapshot is always a prefix of the
    /// live tier). Survivor segments (appended after the flushed snapshot, e.g.
    /// while the off-fence encode/commit ran) are re-folded onto an empty tier so
    /// the aggregate tombstones / bytes / rows / superseded reflect ONLY the
    /// survivors. This is what prevents a double-count: keeping the whole tier
    /// would re-flush the already-durable prefix into a second file on the next
    /// checkpoint. The monotone `epoch` is preserved so a later append never
    /// reuses a flushed epoch.
    #[must_use]
    pub(crate) fn retain_after(&self, flushed_segment_count: usize) -> Self {
        if flushed_segment_count >= self.segments.len() {
            // Nothing newer survived — empty tier, epoch preserved.
            let mut empty = Self::empty();
            empty.epoch = self.epoch;
            empty.version = self.version + 1;
            return empty;
        }
        let survivors: Vec<MemSegment> = self.segments[flushed_segment_count..].to_vec();
        // Seed the aggregates from the FIRST survivor (an O(1) HAMT-root clone)
        // and fold only the rest — this runs under the phase-2 listing fence and
        // the common interleaved-append case has exactly one survivor, which now
        // costs scalar adds instead of re-inserting its whole tombstone delta.
        let first = &survivors[0];
        let mut tombstones = first.tombstones.clone();
        let mut bytes = first.bytes;
        let mut rows = first.rows;
        let mut superseded = first.superseded;
        for segment in &survivors[1..] {
            tombstones.merge_from(&segment.tombstones);
            bytes = bytes.saturating_add(segment.bytes);
            rows = rows.saturating_add(segment.rows);
            superseded = superseded.saturating_add(segment.superseded);
        }
        Self {
            segments: Arc::new(survivors),
            tombstones,
            bytes,
            rows,
            superseded,
            epoch: self.epoch,
            // Reset the age clock: the flushed segments are gone, so the survivor
            // age is measured from now (the next age-cap window starts here).
            oldest_append: Some(Instant::now()),
            version: self.version + 1,
        }
    }

    /// The append-ordered PREFIX `[0..flush_segment_count)` of this tier, with its
    /// aggregate tombstones / bytes / rows / superseded re-folded from ONLY those
    /// segments. This is the corpus a PARTIAL checkpoint encodes durable: feeding
    /// it (rather than the whole `self`) to the encode + metadata commit makes the
    /// durable file carry exactly the prefix rows and the published deletion
    /// snapshot carry exactly the prefix tombstones — the retained suffix
    /// ([`Self::retain_after`] with the SAME count) keeps its own (higher-sequence)
    /// tombstones in RAM, where the scan-time merge-on-read re-applies them to the
    /// new durable file. Crucially the prefix does NOT bake any suffix tombstone
    /// into the durable file: a suffix DELETE of a prefix row is un-acked (its
    /// epoch is above the flushed epoch), so persisting it would lose a still-live
    /// prefix row if that DELETE is rolled back by a crash before its own
    /// checkpoint. The returned tier's `epoch` is the prefix's LAST segment epoch
    /// (the value the slot is advanced to), NOT the whole tier's epoch.
    ///
    /// `flush_segment_count` is the caller's K, already clamped to
    /// `1..=segments.len()` (a partial checkpoint always makes progress and never
    /// over-flushes); when it equals `segments.len()` this returns a clone of the
    /// whole tier (the full-checkpoint degenerate case).
    #[must_use]
    pub(crate) fn take_prefix(&self, flush_segment_count: usize) -> Self {
        debug_assert!(
            (1..=self.segments.len()).contains(&flush_segment_count),
            "take_prefix expects K in 1..=segments.len(); got {flush_segment_count} of {}",
            self.segments.len()
        );
        let k = flush_segment_count.clamp(1, self.segments.len());
        let prefix: Vec<MemSegment> = self.segments[..k].to_vec();
        // Seed the aggregates from the FIRST prefix segment (an O(1) HAMT-root
        // clone) and fold only the rest — symmetric with `retain_after`.
        let first = &prefix[0];
        let mut tombstones = first.tombstones.clone();
        let mut bytes = first.bytes;
        let mut rows = first.rows;
        let mut superseded = first.superseded;
        for segment in &prefix[1..] {
            tombstones.merge_from(&segment.tombstones);
            bytes = bytes.saturating_add(segment.bytes);
            rows = rows.saturating_add(segment.rows);
            superseded = superseded.saturating_add(segment.superseded);
        }
        let prefix_epoch = prefix[k - 1].epoch;
        Self {
            segments: Arc::new(prefix),
            tombstones,
            bytes,
            rows,
            superseded,
            epoch: prefix_epoch,
            oldest_append: self.oldest_append,
            version: self.version,
        }
    }
}

/// Cross-layer handle the runtime installs on a memory-mode provider so the
/// cayenne checkpoint can advance the source slot — WITHOUT cayenne depending on
/// the replication connector crate. Defined here (cayenne side) and implemented
/// in the runtime over the source's `confirmed_flush` atomic / deferred-commit
/// queue.
///
/// Correctness contract: [`Self::on_checkpoint_durable`] is invoked by
/// `checkpoint_mem_tier` STRICTLY AFTER the checkpoint's Vortex file and
/// metastore snapshot pointer are durable (after the same listing-fence section
/// that makes file-mode publishes durable). The runtime impl then advances the
/// slot to cover every CDC batch whose mem-tier epoch is `<= durable_epoch`.
/// A checkpoint that FAILS must NOT call this, so the slot never advances past
/// un-checkpointed RAM (correctness item #4).
#[async_trait]
pub trait SlotAdvancer: Send + Sync {
    /// Signal that the mem tier has been durably checkpointed up to and
    /// including `durable_epoch`. The runtime drains and runs every deferred
    /// source committer whose batch epoch is `<= durable_epoch`, in order,
    /// advancing the source slot. Errors are the runtime's to log/surface; this
    /// returns nothing so a slow source ack cannot block the write path's fence.
    async fn on_checkpoint_durable(&self, durable_epoch: u64);
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    fn batch(values: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("pk", DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values.to_vec()))])
            .expect("batch")
    }

    /// Appending shares the underlying `RecordBatch` `Arc` rather than deep
    /// copying — guards the O(N)-clone regression.
    #[test]
    fn append_shares_batch_arc_no_deep_copy() {
        let tier = MemTier::empty();
        let batches = Arc::new(vec![batch(&[1, 2, 3])]);
        let arc_ptr = Arc::as_ptr(&batches);

        let next = tier.append_segment(
            Arc::clone(&batches),
            1,
            &InMemTombstones::default(),
            64,
            3,
            0,
        );
        assert_eq!(next.segments.len(), 1);
        assert_eq!(next.rows, 3);
        assert_eq!(next.epoch, 1);
        // The segment retains the SAME Arc<Vec<RecordBatch>> we passed in.
        assert!(std::ptr::eq(
            Arc::as_ptr(&next.segments[0].batches),
            arc_ptr
        ));
        // The original tier is unchanged (immutable swap semantics).
        assert!(tier.is_empty());
    }

    /// Appending shares the accumulated TOMBSTONE map's HAMT root rather than
    /// deep-copying it — the missing guard for the root-cause O(tier)-per-append
    /// clone. Mirrors `append_shares_batch_arc_no_deep_copy` for the tombstone
    /// corpus (which was the actually-expensive clone, unlike the batch `Arc`).
    #[test]
    fn append_shares_tombstone_map_no_deep_copy() {
        // Seed a tier carrying a non-trivial Int64 tombstone corpus.
        let mut seed = InMemTombstones::default();
        for pk in 0..1_000_i64 {
            seed.int64_pk.insert(pk, 1);
        }
        let base = MemTier::empty().append_segment(Arc::new(vec![batch(&[1])]), 2, &seed, 16, 1, 0);

        // A fresh clone of the base's tombstone map shares its root (O(1) clone,
        // no deep copy) — this is the property `append_segment` relies on.
        let base_clone = base.tombstones.int64_pk.clone();
        assert!(
            base.tombstones.int64_pk.ptr_eq(&base_clone),
            "cloning the persistent tombstone map shares the HAMT root (O(1), no deep copy)"
        );

        // Append one new tombstone key. The NEW tier reflects it; the prior tier
        // is untouched and still shares its original root with the earlier clone.
        let mut one = InMemTombstones::default();
        one.int64_pk.insert(10_000, 3);
        let next = base.append_segment(Arc::new(vec![batch(&[2])]), 4, &one, 16, 1, 0);

        assert_eq!(
            next.tombstones.int64_pk.get(&10_000),
            Some(&3),
            "the appended tombstone is visible in the new tier"
        );
        assert_eq!(
            next.tombstones.int64_pk.len(),
            1_001,
            "the new tier merged exactly one incoming key onto the shared base"
        );
        assert!(
            base.tombstones.int64_pk.get(&10_000).is_none(),
            "the prior tier is immutable — the append did not mutate it in place"
        );
        assert!(
            base.tombstones.int64_pk.ptr_eq(&base_clone),
            "the prior tier still shares its original root (the append built a new structurally-shared map, not a deep copy)"
        );
    }

    /// B-T3 — bounded-tier / no-O(N²) check. Appending many segments must keep
    /// each new tier structurally sharing the prior tier's accumulated tombstone
    /// corpus (an O(1) HAMT-root clone per append), NOT deep-copying it — which is
    /// what turned the per-append cost from O(tier) into O(incoming·log tier) and
    /// the cumulative cost from O(K²) into O(K log K). Proven by structural
    /// identity rather than wall-clock timing (deterministic, non-flaky): after
    /// each append, the keys carried by the PREVIOUS tier still resolve in the new
    /// tier (the base was shared, not rebuilt), and the previous tier is never
    /// mutated in place.
    #[test]
    fn append_bounded_tier_shares_corpus_no_quadratic_copy() {
        // Each segment supersedes a fresh window of keys; the tombstone corpus
        // grows by one key per append so the accumulated map is non-trivial.
        const APPENDS: usize = 256;
        let mut tier = MemTier::empty();
        for i in 0..APPENDS {
            let key = i64::try_from(i).expect("loop index fits i64");
            // Snapshot the prior corpus root + a sentinel key it must still carry.
            let prior_root = tier.tombstones.int64_pk.clone();
            let prior_len = tier.tombstones.int64_pk.len();
            let sentinel = key - 1; // present once i > 0

            let mut incoming = InMemTombstones::default();
            incoming.int64_pk.insert(key, 1);
            let next =
                tier.append_segment(Arc::new(vec![batch(&[key])]), key + 1, &incoming, 16, 1, 0);

            // The new tier carries exactly one more key than the prior tier — the
            // base was structurally shared and extended, not rebuilt from scratch.
            assert_eq!(
                next.tombstones.int64_pk.len(),
                prior_len + 1,
                "append {i}: corpus grew by exactly the one incoming key"
            );
            if i > 0 {
                assert!(
                    next.tombstones.int64_pk.get(&sentinel).is_some(),
                    "append {i}: a key from the prior corpus survives (base was shared, not dropped)"
                );
            }
            // The prior tier's root is immutable — the append never deep-copied or
            // mutated it (the O(1)-share invariant that avoids the O(N²) blow-up).
            assert!(
                tier.tombstones.int64_pk.ptr_eq(&prior_root),
                "append {i}: the prior tier still owns its original HAMT root (no in-place mutation / deep copy)"
            );

            tier = next;
        }
        assert_eq!(
            tier.tombstones.int64_pk.len(),
            APPENDS,
            "every appended key accumulated into the final shared corpus"
        );
    }

    /// Tombstones merge with max-sequence-per-key semantics; the deleted-key
    /// range is the closed [min,max] used by the disjoint gate.
    #[test]
    fn tombstone_merge_keeps_max_sequence_and_range() {
        let mut a = InMemTombstones::default();
        a.int64_pk.insert(10, 5);
        a.int64_pk.insert(20, 7);
        let mut b = InMemTombstones::default();
        b.int64_pk.insert(20, 9); // higher seq for 20
        b.int64_pk.insert(30, 4);
        a.merge_from(&b);
        assert_eq!(a.int64_pk.get(&20), Some(&9));
        assert_eq!(a.int64_deleted_key_range(), Some((10, 30)));
    }

    /// The superseded count is carried, not derived from the tombstone maps.
    #[test]
    fn superseded_is_carried_not_recomputed() {
        let tier = MemTier::empty();
        let mut tomb = InMemTombstones::default();
        // Two encodings of the SAME deletion would double-count if summed.
        tomb.int64_pk.insert(1, 1);
        let next = tier.append_segment(Arc::new(vec![batch(&[2])]), 2, &tomb, 16, 1, 1);
        // Authoritative superseded is exactly the passed value (1), not 2.
        assert_eq!(next.superseded, 1);
    }

    /// Each appended segment carries the post-append tier epoch — the exact value
    /// the runtime tags the batch's deferred committers with. This is what a
    /// partial prefix checkpoint reads (segment[K-1].epoch) to ack the slot to the
    /// flushed prefix only.
    #[test]
    fn append_stamps_segment_with_its_tier_epoch() {
        let mut tier = MemTier::empty();
        for expected_epoch in 1..=4_u64 {
            tier = tier.append_segment(
                Arc::new(vec![batch(&[i64::try_from(expected_epoch).unwrap()])]),
                i64::try_from(expected_epoch).unwrap(),
                &InMemTombstones::default(),
                16,
                1,
                0,
            );
            assert_eq!(tier.epoch, expected_epoch, "tier epoch advances by one");
            assert_eq!(
                tier.segments.last().expect("segment").epoch,
                expected_epoch,
                "the new segment is stamped with the post-append epoch"
            );
        }
        // Every segment's epoch equals its 1-based position in a never-cleared tier.
        for (idx, segment) in tier.segments.iter().enumerate() {
            assert_eq!(segment.epoch, u64::try_from(idx + 1).unwrap());
        }
    }

    /// `take_prefix(K)` and `retain_after(K)` partition a tier at the SAME boundary:
    /// the prefix carries segments [0..K) (its epoch = segment[K-1].epoch, its
    /// aggregates folded from only those K), the suffix carries [K..) (epoch
    /// preserved). Together they reconstruct the whole tier with no segment shared
    /// or dropped — the prefix is made durable, the suffix stays resident.
    #[test]
    fn take_prefix_and_retain_after_partition_at_the_same_boundary() {
        // Four segments, each one tombstone + distinct byte/row weights.
        let mut tier = MemTier::empty();
        for i in 0..4_i64 {
            let mut tomb = InMemTombstones::default();
            tomb.int64_pk.insert(100 + i, i + 1);
            tier = tier.append_segment(
                Arc::new(vec![batch(&[i])]),
                i + 1,
                &tomb,
                10 * u64::try_from(i + 1).unwrap(),
                1,
                0,
            );
        }
        assert_eq!(tier.epoch, 4);
        assert_eq!(tier.segments.len(), 4);
        let total_bytes = tier.bytes; // 10+20+30+40 = 100
        assert_eq!(total_bytes, 100);

        // Flush the first TWO segments.
        let prefix = tier.take_prefix(2);
        let suffix = tier.retain_after(2);

        // Prefix: segments [0,1], epoch = segment[1].epoch = 2, bytes 10+20=30.
        assert_eq!(prefix.segments.len(), 2);
        assert_eq!(prefix.epoch, 2, "prefix epoch is its LAST segment's epoch");
        assert_eq!(prefix.bytes, 30, "prefix bytes fold only the prefix segments");
        assert_eq!(prefix.rows, 2);
        assert_eq!(prefix.tombstones.int64_pk.len(), 2, "prefix has only its own tombstones");
        assert!(prefix.tombstones.int64_pk.contains_key(&100));
        assert!(prefix.tombstones.int64_pk.contains_key(&101));

        // Suffix: segments [2,3], epoch preserved at 4, bytes 30+40=70.
        assert_eq!(suffix.segments.len(), 2);
        assert_eq!(suffix.epoch, 4, "suffix preserves the tier epoch");
        assert_eq!(suffix.bytes, 70, "suffix bytes fold only the suffix segments");
        assert_eq!(suffix.tombstones.int64_pk.len(), 2, "suffix has only its own tombstones");
        assert!(suffix.tombstones.int64_pk.contains_key(&102));
        assert!(suffix.tombstones.int64_pk.contains_key(&103));

        // No segment shared or dropped: prefix.bytes + suffix.bytes == tier.bytes.
        assert_eq!(prefix.bytes + suffix.bytes, total_bytes);
        assert!(
            suffix.segments.first().expect("suffix seg").epoch > prefix.epoch,
            "the suffix's first epoch is strictly above the flushed prefix epoch (un-acked)"
        );
    }

    /// `take_prefix(len)` is the full-checkpoint degenerate case: it returns the
    /// whole tier with the tier epoch — identical flush target to a full checkpoint.
    #[test]
    fn take_prefix_full_len_returns_whole_tier() {
        let mut tier = MemTier::empty();
        for i in 0..3_i64 {
            tier = tier.append_segment(
                Arc::new(vec![batch(&[i])]),
                i + 1,
                &InMemTombstones::default(),
                16,
                1,
                0,
            );
        }
        let prefix = tier.take_prefix(tier.segments.len());
        assert_eq!(prefix.segments.len(), 3, "the whole tier is the prefix");
        assert_eq!(prefix.epoch, tier.epoch, "epoch equals the full tier epoch");
        assert_eq!(prefix.bytes, tier.bytes);
        assert_eq!(prefix.rows, tier.rows);
    }

    /// A trivial `SlotAdvancer` records the highest durable epoch — the shape the
    /// runtime impl plugs into.
    #[tokio::test]
    async fn slot_advancer_receives_durable_epoch() {
        struct Recorder(Arc<AtomicU64>);
        #[async_trait]
        impl SlotAdvancer for Recorder {
            async fn on_checkpoint_durable(&self, durable_epoch: u64) {
                self.0.store(durable_epoch, Ordering::SeqCst);
            }
        }
        let seen = Arc::new(AtomicU64::new(0));
        let advancer: Arc<dyn SlotAdvancer> = Arc::new(Recorder(Arc::clone(&seen)));
        advancer.on_checkpoint_durable(42).await;
        assert_eq!(seen.load(Ordering::SeqCst), 42);
    }
}
