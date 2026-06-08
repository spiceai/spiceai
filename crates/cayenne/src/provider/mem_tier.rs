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
use async_trait::async_trait;
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
/// XXH3 hasher (not the `im` default SipHash) keeps per-key hashing off the
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
    /// compose-trap disjoint gate computes the equivalent range over the
    /// `InlinedDeletionMaps` the tier tombstones are projected into
    /// (`CayenneTableProvider::int64_map_key_range`); this method exists for the
    /// tier-level unit tests and as the documented parity point.
    #[cfg(test)]
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
    /// durable fence.
    pub(crate) epoch: u64,
    /// Wall-clock instant the OLDEST un-checkpointed segment was appended, used
    /// by the age cap to bound the crash-replay window for cold tables. `None`
    /// when the tier is empty.
    pub(crate) oldest_append: Option<Instant>,
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
        let mut segments = Vec::with_capacity(self.segments.len() + 1);
        segments.extend(self.segments.iter().cloned());
        segments.push(MemSegment {
            batches,
            data_sequence,
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
        }
    }

    /// Wall-clock age of the oldest un-checkpointed segment, or zero when empty.
    #[must_use]
    pub(crate) fn age_ms(&self) -> u64 {
        self.oldest_append.map_or(0, |t| {
            u64::try_from(t.elapsed().as_millis()).unwrap_or(u64::MAX)
        })
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
        let base = MemTier::empty().append_segment(
            Arc::new(vec![batch(&[1])]),
            2,
            &seed,
            16,
            1,
            0,
        );

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
