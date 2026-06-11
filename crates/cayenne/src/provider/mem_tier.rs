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
    ///
    /// The live fold path uses [`Self::merge_segment`] (a segment's key set at one
    /// uniform sequence); this per-key-map variant is retained as the reference
    /// the equivalence test checks `merge_segment` against.
    #[cfg(test)]
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

    /// Fold one segment's tombstones into `self`, keeping the per-key max delete
    /// sequence. Every key in `seg` was deleted at the SAME `seg.delete_sequence`
    /// (a segment is one CDC apply, which reserved one delete sequence), so this
    /// applies that single scalar to all of `seg`'s keys instead of carrying a
    /// redundant per-key value map — the equivalence to a per-key map folded by
    /// [`Self::merge_from`] is exact. O(seg-keys · log self), structurally
    /// sharing `self`'s untouched nodes (the same persistent-map cost
    /// `merge_from` has).
    pub(crate) fn merge_segment(&mut self, seg: &SegmentTombstones) {
        let seq = seg.delete_sequence;
        for &pk in seg.int64_pk.keys() {
            let next = self.int64_pk.get(&pk).map_or(seq, |&cur| cur.max(seq));
            self.int64_pk.insert(pk, next);
        }
        for key in seg.row_keys.keys() {
            let next = self.row_keys.get(key).map_or(seq, |&cur| cur.max(seq));
            self.row_keys.insert(key.clone(), next);
        }
    }

    fn is_empty(&self) -> bool {
        self.int64_pk.is_empty() && self.row_keys.is_empty()
    }
}

/// ONE CDC apply's tombstones, split into the part that is derivable from the
/// incoming batch alone (the deduplicated deleted-key SETS) and the part that is
/// reserved under the publish lock (the single `delete_sequence`).
///
/// Every superseded key in one apply is hidden at the SAME reserved
/// `delete_sequence` (the apply reserves exactly one delete sequence, strictly
/// below its data sequence so the fresh rows survive their own tombstones), so a
/// segment's tombstones are a *uniform* `delete_sequence` over a key set — there
/// is no per-key sequence variation within a segment. Storing the key sets
/// separately from the scalar is what lets the expensive O(batch) HAMT BUILD of
/// the key sets run OFF the `mem_tier_publish_lock`
/// ([`crate::provider::CayenneTableProvider::prepare_segment_tombstones`]), with
/// only the cheap [`Self::stamp`] of the reserved sequence happening under the
/// lock. The keys carry `()` values (a set, not a map) so the placeholder/late
/// sequence can never be misread as a real delete sequence; the authoritative
/// sequence is always the explicit `delete_sequence` field.
///
/// The tier-level aggregate stays an [`InMemTombstones`] (a real per-key
/// max-sequence map, folded via [`InMemTombstones::merge_segment`]), so every
/// scan-side consumer is byte-for-byte unchanged — only the per-segment delta
/// representation differs.
#[derive(Debug, Clone)]
pub(crate) struct SegmentTombstones {
    /// `Int64Pk` strategy: the set of deleted PK values (built off-lock).
    int64_pk: PersistentHashMap<i64, (), XxHash3BuildHasher>,
    /// `RowConverterBased` strategy: the set of deleted row-key bytes (off-lock).
    row_keys: PersistentHashMap<Box<[u8]>, (), XxHash3BuildHasher>,
    /// The single reserved delete sequence applied to EVERY key above. Set to a
    /// placeholder by the off-lock builder and overwritten by [`Self::stamp`]
    /// under the publish lock once the real sequence is reserved.
    delete_sequence: i64,
}

impl Default for SegmentTombstones {
    fn default() -> Self {
        Self {
            int64_pk: PersistentHashMap::with_hasher(XxHash3BuildHasher),
            row_keys: PersistentHashMap::with_hasher(XxHash3BuildHasher),
            // No keys yet, so the sequence is irrelevant until `stamp`.
            delete_sequence: 0,
        }
    }
}

impl SegmentTombstones {
    /// Build the deleted-key SETS from one apply's `Int64Pk` keys (off-lock). The
    /// `delete_sequence` stays at the placeholder until [`Self::stamp`]; the set
    /// values are `()`, so the placeholder is structurally unobservable.
    pub(crate) fn from_int64_keys(keys: impl IntoIterator<Item = i64>) -> Self {
        let mut int64_pk = PersistentHashMap::with_hasher(XxHash3BuildHasher);
        for pk in keys {
            int64_pk.insert(pk, ());
        }
        Self {
            int64_pk,
            row_keys: PersistentHashMap::with_hasher(XxHash3BuildHasher),
            delete_sequence: 0,
        }
    }

    /// Build the deleted-key SETS from one apply's row keys (off-lock). Takes
    /// owned `Box<[u8]>` keys so the byte allocation happens off the publish lock.
    pub(crate) fn from_row_keys(keys: impl IntoIterator<Item = Box<[u8]>>) -> Self {
        let mut row_keys = PersistentHashMap::with_hasher(XxHash3BuildHasher);
        for key in keys {
            row_keys.insert(key, ());
        }
        Self {
            int64_pk: PersistentHashMap::with_hasher(XxHash3BuildHasher),
            row_keys,
            delete_sequence: 0,
        }
    }

    /// Late-bind the reserved `delete_sequence` under the publish lock. O(1): the
    /// key sets were already built off-lock; this only records the scalar that
    /// [`InMemTombstones::merge_segment`] applies to every key.
    pub(crate) fn stamp(&mut self, delete_sequence: i64) {
        self.delete_sequence = delete_sequence;
    }

    /// The reserved delete sequence stamped on this segment (the uniform sequence
    /// applied to every key).
    pub(crate) fn delete_sequence(&self) -> i64 {
        self.delete_sequence
    }

    /// The deleted `Int64Pk` keys (empty for the row-key strategy). Each yields
    /// once; the effective delete sequence is the uniform [`Self::delete_sequence`].
    pub(crate) fn int64_keys(&self) -> impl Iterator<Item = i64> + '_ {
        self.int64_pk.keys().copied()
    }

    /// The deleted row keys (empty for the `Int64Pk` strategy). Each yields once;
    /// the effective delete sequence is the uniform [`Self::delete_sequence`].
    pub(crate) fn row_keys(&self) -> impl Iterator<Item = &[u8]> + '_ {
        self.row_keys.keys().map(Box::as_ref)
    }

    /// Whether this segment carries no `Int64Pk` tombstone keys.
    pub(crate) fn is_int64_empty(&self) -> bool {
        self.int64_pk.is_empty()
    }

    /// Whether this segment carries no row-key tombstone keys.
    pub(crate) fn is_row_keys_empty(&self) -> bool {
        self.row_keys.is_empty()
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
    /// Carried as a [`SegmentTombstones`] (deleted-key SET + the apply's single
    /// reserved `delete_sequence`) rather than a per-key map: a segment is one
    /// CDC apply whose keys all share one delete sequence, and keeping the key
    /// set separate from the scalar lets the key SET be built off the publish
    /// lock. Cheap to carry: the sets are `im::HashMap` (O(1) structural clone).
    pub(crate) tombstones: SegmentTombstones,
    /// This segment's measured byte cost (`get_array_memory_size`), for budget
    /// release accounting on a partial clear.
    pub(crate) bytes: u64,
    /// This segment's row count.
    pub(crate) rows: u64,
    /// Rows this segment's upsert superseded (carried, not recomputed).
    pub(crate) superseded: u64,
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
    /// deltas for this batch. `tombstones` is this apply's [`SegmentTombstones`]
    /// (key set built off the publish lock, sequence already stamped); it is moved
    /// into the new segment after its keys are folded into the tier aggregate.
    #[must_use]
    pub(crate) fn append_segment(
        &self,
        batches: Arc<Vec<RecordBatch>>,
        data_sequence: i64,
        tombstones: SegmentTombstones,
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

        // O(1): clones the persistent maps' HAMT roots (Arc bumps), NOT the
        // accumulated corpus. `merge_segment` then applies only the incoming
        // segment's keys (at its single stamped sequence) against the
        // structurally-shared base. Fold BEFORE moving `tombstones` into the
        // segment below.
        let mut merged_tombstones = self.tombstones.clone();
        merged_tombstones.merge_segment(&tombstones);

        let mut segments = Vec::with_capacity(self.segments.len() + 1);
        segments.extend(self.segments.iter().cloned());
        segments.push(MemSegment {
            batches,
            data_sequence,
            statistics,
            tombstones,
            bytes: incoming_bytes,
            rows: incoming_rows,
            superseded,
        });

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
        // Rebuild the survivor aggregate by folding each survivor's per-segment
        // tombstones (key set + its own stamped delete sequence) onto an empty
        // aggregate map — this runs at checkpoint time under the phase-2 listing
        // fence (NOT per-append), and the common interleaved-append case has
        // exactly one survivor, so it is one `merge_segment` of a small key set.
        // Each survivor folds at its OWN `delete_sequence` (segments do not share
        // a sequence), preserving per-key max-sequence semantics exactly.
        let mut tombstones = InMemTombstones::default();
        let mut bytes = 0u64;
        let mut rows = 0u64;
        let mut superseded = 0u64;
        for segment in &survivors {
            tombstones.merge_segment(&segment.tombstones);
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
            SegmentTombstones::default(),
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
        // Seed a tier carrying a non-trivial Int64 tombstone corpus (one segment,
        // 1000 keys, all at the segment's single delete sequence 1).
        let mut seed = SegmentTombstones::from_int64_keys(0..1_000_i64);
        seed.stamp(1);
        let base = MemTier::empty().append_segment(Arc::new(vec![batch(&[1])]), 2, seed, 16, 1, 0);

        // A fresh clone of the base's tombstone map shares its root (O(1) clone,
        // no deep copy) — this is the property `append_segment` relies on.
        let base_clone = base.tombstones.int64_pk.clone();
        assert!(
            base.tombstones.int64_pk.ptr_eq(&base_clone),
            "cloning the persistent tombstone map shares the HAMT root (O(1), no deep copy)"
        );

        // Append one new tombstone key. The NEW tier reflects it; the prior tier
        // is untouched and still shares its original root with the earlier clone.
        let mut one = SegmentTombstones::from_int64_keys([10_000]);
        one.stamp(3);
        let next = base.append_segment(Arc::new(vec![batch(&[2])]), 4, one, 16, 1, 0);

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

            let mut incoming = SegmentTombstones::from_int64_keys([key]);
            incoming.stamp(1);
            let next =
                tier.append_segment(Arc::new(vec![batch(&[key])]), key + 1, incoming, 16, 1, 0);

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

    /// `merge_segment` (key SET folded at one uniform sequence — the off-lock
    /// representation) is byte-for-byte equivalent to folding the same keys as a
    /// per-key map via `merge_from`. This is the invariant the publish-lock split
    /// rests on: building the key set off-lock and stamping the reserved sequence
    /// must produce the SAME aggregate as the old `build_mem_tombstones` +
    /// `merge_from`. Folds two segments at DIFFERENT sequences and asserts the
    /// per-key max matches the equivalent per-key-map fold.
    #[test]
    fn merge_segment_equivalent_to_per_key_map_fold() {
        // Two CDC applies: seg1 deletes {1,2,3} at seq 5, seg2 deletes {3,4} at
        // seq 9 (key 3 re-deleted at a higher seq). Row-key side mirrors it.
        let mut seg1 = SegmentTombstones::from_int64_keys([1, 2, 3]);
        seg1.stamp(5);
        let mut seg2 = SegmentTombstones::from_int64_keys([3, 4]);
        seg2.stamp(9);
        let key = |b: u8| -> Box<[u8]> { Box::from([b]) };
        let mut seg1k = SegmentTombstones::from_row_keys([key(1), key(2), key(3)]);
        seg1k.stamp(5);
        let mut seg2k = SegmentTombstones::from_row_keys([key(3), key(4)]);
        seg2k.stamp(9);

        // Fold via the segment representation (the new path).
        let mut via_segment = InMemTombstones::default();
        via_segment.merge_segment(&seg1);
        via_segment.merge_segment(&seg2);
        via_segment.merge_segment(&seg1k);
        via_segment.merge_segment(&seg2k);

        // Fold the identical keys/sequences via the per-key map path (the old
        // build_mem_tombstones + merge_from equivalent).
        let mut via_map = InMemTombstones::default();
        let mut m1 = InMemTombstones::default();
        for pk in [1, 2, 3] {
            m1.int64_pk.insert(pk, 5);
        }
        for b in [1u8, 2, 3] {
            m1.row_keys.insert(key(b), 5);
        }
        let mut m2 = InMemTombstones::default();
        for pk in [3, 4] {
            m2.int64_pk.insert(pk, 9);
        }
        for b in [3u8, 4] {
            m2.row_keys.insert(key(b), 9);
        }
        via_map.merge_from(&m1);
        via_map.merge_from(&m2);

        // Identical aggregate: same keys, same per-key MAX sequence.
        assert_eq!(via_segment.int64_pk.get(&1), Some(&5));
        assert_eq!(via_segment.int64_pk.get(&3), Some(&9), "key 3 takes the max seq 9");
        assert_eq!(via_segment.int64_pk.get(&4), Some(&9));
        assert_eq!(via_segment.int64_pk.len(), via_map.int64_pk.len());
        assert_eq!(via_segment.row_keys.len(), via_map.row_keys.len());
        for (k, v) in &via_map.int64_pk {
            assert_eq!(via_segment.int64_pk.get(k), Some(v), "int64 key {k} matches map fold");
        }
        for (k, v) in &via_map.row_keys {
            assert_eq!(via_segment.row_keys.get(k), Some(v), "row key {k:?} matches map fold");
        }
    }

    /// The superseded count is carried, not derived from the tombstone maps.
    #[test]
    fn superseded_is_carried_not_recomputed() {
        let tier = MemTier::empty();
        // Two encodings of the SAME deletion would double-count if summed.
        let mut tomb = SegmentTombstones::from_int64_keys([1]);
        tomb.stamp(1);
        let next = tier.append_segment(Arc::new(vec![batch(&[2])]), 2, tomb, 16, 1, 1);
        // Authoritative superseded is exactly the passed value (1), not 2.
        assert_eq!(next.superseded, 1);
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
