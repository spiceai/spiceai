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

//! Immutable deletion index with bloom-filter prefilter.
//!
//! [`DeletionIndex`] (Int64 PK) and [`KeyDeletionIndex`] (composite-key PK) are the
//! frozen snapshots that scans probe at query time. Each entry fuses the two
//! sequence numbers visibility needs — the highest *delete* sequence and, for keys
//! re-inserted by an upsert, the highest *insert* sequence — into one
//! [`TombstoneEntry`], so the scan hot path answers "is this row deleted, and was
//! it re-inserted after that?" with a **single** probe.
//!
//! # Layered storage: frozen flat base + small delta
//!
//! Entries live in two tiers (the in-memory analogue of an LSM memtable over a
//! frozen run):
//!
//! - **base**: an `Arc<std::collections::HashMap>` (`SwissTable`). Frozen — never
//!   mutated after construction — so publishing a new index generation shares it
//!   with an `Arc` clone, and probing it costs 1–2 cache lines regardless of
//!   size. This tier holds the overwhelming majority of entries at scale.
//! - **delta**: a small persistent [`im::HashMap`] holding writes since the last
//!   merge. `O(1)` to snapshot per publish (structural sharing), and because the
//!   merge policy caps it at `max(`[`DELTA_MERGE_MIN`]`, base/4)` entries it
//!   stays a shallow, cache-resident HAMT — the pointer-chasing that made a
//!   *large* HAMT the dominant scan cost (per-row `get` walks of 5+ levels at
//!   millions of entries) never develops.
//!
//! A probe checks the bloom filter, then delta, then base — at most one small
//! in-cache lookup plus one flat-table lookup. A write goes to delta only; when
//! delta outgrows the threshold it is folded into a fresh base (`O(base+delta)`
//! copy). The `base/4` ladder keeps total copy work per key logarithmic in the
//! final index size, and old generations pinned by long scans share the previous
//! base `Arc`, so a merge costs one transient extra base — not one per
//! generation.
//!
//! The bloom filter is keyed on *deletion* membership (a single cache line per
//! probe). Entries holding only an insert record (which occur after compaction
//! purges delete files while insert records remain in the catalog) are not
//! represented in the bloom and are reported as absent by [`get`] — exactly the
//! visibility semantics of the original two-index design.
//!
//! [`get`]: DeletionIndex::get
//!
//! # Build then publish
//!
//! All map mutation happens before the index is wrapped in an `Arc`/`ArcSwap`.
//! Construct via [`DeletionIndex::from_maps`] / [`DeletionIndex::empty`] (and the
//! matching [`KeyDeletionIndex`] constructors), publish through `ArcSwap`, and treat
//! the published index as immutable. To apply a write, build a new index with
//! [`extend_max_deletes`](DeletionIndex::extend_max_deletes) (delete-only writes) or
//! [`extend_max_conflicts`](DeletionIndex::extend_max_conflicts) (upsert conflicts,
//! which record a delete and a re-insert in one pass) and store the
//! `Arc<DeletionIndex>` back into the swap cell. Readers always see a fully-built
//! snapshot and never block.
//!
//! The bloom filter is the one deliberate exception to "frozen": it is shared
//! across generations behind an `Arc` and extended in place with relaxed atomic
//! stores. Older pinned generations may observe bits for keys added after they
//! were published — a safe superset that can only add false positives (caught by
//! the map probe), never false negatives. See [`SplitBlockBloomFilter`] for the
//! memory-ordering contract and [`extend_max_deletes`](DeletionIndex::extend_max_deletes)
//! for the rebuild policy.

use hash_index::{
    PrehashedBuildHasher, SplitBlockBloomFilter, XxHash3BuildHasher, hash_key_128, hash_key_i64,
};
use im::HashMap as PersistentHashMap;
use std::collections::HashMap;
use std::hash::{BuildHasher, Hash};
use std::sync::{Arc, LazyLock};

/// Bloom filter capacity floor: keep some signal even for empty / tiny sets so that the
/// "probably-not-present" path stays useful when a fresh index is constructed.
const MIN_BLOOM_CAPACITY: usize = 64;

/// Delta-size floor below which a merge into the base is never triggered.
/// Small in tests so layering (merge, pinned-generation, and counter behavior
/// across merges) is exercised by ordinary unit tests without inserting tens of
/// thousands of keys.
#[cfg(not(test))]
const DELTA_MERGE_MIN: usize = 16_384;
#[cfg(test)]
const DELTA_MERGE_MIN: usize = 64;

/// Delta size at which the delta is folded into a fresh frozen base.
///
/// `max(DELTA_MERGE_MIN, base/4)`: the floor avoids re-copying tiny bases on
/// every publish, and the `base/4` ladder makes merge cadence geometric — each
/// key is copied `O(log N)` times over the index's lifetime — while capping the
/// delta at a quarter of the base so it stays a shallow, cache-resident HAMT.
fn delta_merge_threshold(base_len: usize) -> usize {
    (base_len / 4).max(DELTA_MERGE_MIN)
}

/// Hasher for the Int64 index maps. `im::HashMap`/`std::collections::HashMap`
/// default to `RandomState` (SipHash-1-3), which showed up as
/// `im::nodes::hamt::hash_key` in executor profiles of changes-mode ingest —
/// every probe was paying a `SipHash` walk on top of the seeded-XXH3 bloom hash.
/// [`XxHash3BuildHasher`] uses the same seeded XXH3-64 as [`hash_key_i64`], so
/// map and bloom hashing now agree at a fraction of `SipHash`'s per-key cost.
pub type DeletionIndexHasher = XxHash3BuildHasher;

/// Bloom capacity for a deletion set of `len` keys: sized with 2x headroom so the
/// filter stays at or below its design false-positive rate for the whole window
/// between rebuilds. The previous policy sized the filter exactly and rebuilt
/// only when the entry count crossed `2x` capacity, which let the effective
/// bits-per-key halve before each rebuild — at millions of deletions the filter
/// spent long windows saturated, sending most not-deleted probes through to the
/// hash map.
fn bloom_capacity_for(len: usize) -> usize {
    len.saturating_mul(2).max(MIN_BLOOM_CAPACITY)
}

/// Keys swept per chunk by the batched probes ([`DeletionIndex::get_batch`] /
/// [`KeyDeletionIndex::get_batch`]). Sized so one chunk's working set (key
/// slice / hash buffer plus the candidate-position buffer) stays L1/L2
/// resident: candidate positions produced by the bloom sweep are still in
/// cache when the tier walk consumes them, while the chunk is large enough
/// that each pass runs as a long branch-light loop the out-of-order window
/// can overlap loads across.
const BATCH_SWEEP_CHUNK: usize = 2048;

/// Sentinel for "no sequence recorded" inside [`TombstoneEntry`]. Catalog
/// sequence numbers are non-negative, so `i64::MIN` can never collide with a
/// real sequence; packing both fields as raw `i64` keeps the per-entry payload
/// at 16 bytes (an `Option<i64>` pair would double it — at tens of millions of
/// entries that is hundreds of MiB).
const SEQUENCE_ABSENT: i64 = i64::MIN;

/// Raw fused map value: the highest delete and insert sequence numbers recorded
/// for one primary key, with [`SEQUENCE_ABSENT`] marking an unset side. Exposed
/// (read-only) so callers iterating [`iter_entries`](DeletionIndex::iter_entries)
/// can project the side they need.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TombstoneEntry {
    delete_seq: i64,
    insert_seq: i64,
}

impl TombstoneEntry {
    const EMPTY: Self = Self {
        delete_seq: SEQUENCE_ABSENT,
        insert_seq: SEQUENCE_ABSENT,
    };

    /// Highest delete sequence recorded for this key, if any.
    #[inline]
    #[must_use]
    pub fn delete_sequence(&self) -> Option<i64> {
        (self.delete_seq != SEQUENCE_ABSENT).then_some(self.delete_seq)
    }

    /// Highest insert (upsert re-insertion) sequence recorded for this key, if any.
    #[inline]
    #[must_use]
    pub fn insert_sequence(&self) -> Option<i64> {
        (self.insert_seq != SEQUENCE_ABSENT).then_some(self.insert_seq)
    }
}

/// Deletion state returned by a successful probe: the key **has** a recorded
/// deletion, plus the re-insertion sequence if an upsert wrote the key back.
///
/// Visibility under upsert semantics: the row is visible iff
/// `insert_sequence > delete_sequence` (re-inserted after the delete).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Tombstone {
    /// Highest delete sequence recorded for the key.
    pub delete_sequence: i64,
    /// Highest insert (upsert re-insertion) sequence for the key, if any.
    pub insert_sequence: Option<i64>,
}

impl Tombstone {
    #[inline]
    fn from_entry(entry: &TombstoneEntry) -> Option<Self> {
        entry.delete_sequence().map(|delete_sequence| Self {
            delete_sequence,
            insert_sequence: entry.insert_sequence(),
        })
    }
}

/// Outcome of merging one addition against a key's current effective entry.
#[derive(Clone, Copy)]
struct MergeOutcome {
    /// The merged entry to store.
    entry: TombstoneEntry,
    /// The key had no entry in either tier before this addition.
    new_key: bool,
    /// The delete side transitioned from absent to present (drives the bloom
    /// update and `delete_len`).
    new_delete: bool,
    /// The insert side transitioned from absent to present (drives `insert_len`).
    new_insert: bool,
}

/// Merge `delete_seq`/`insert_seq` (either possibly [`SEQUENCE_ABSENT`]) into
/// `current`, taking the per-side max.
#[inline]
fn merged_entry(current: Option<TombstoneEntry>, delete_seq: i64, insert_seq: i64) -> MergeOutcome {
    let new_key = current.is_none();
    let mut entry = current.unwrap_or(TombstoneEntry::EMPTY);
    let mut new_delete = false;
    let mut new_insert = false;
    if delete_seq != SEQUENCE_ABSENT {
        if entry.delete_seq == SEQUENCE_ABSENT {
            new_delete = true;
            entry.delete_seq = delete_seq;
        } else if delete_seq > entry.delete_seq {
            entry.delete_seq = delete_seq;
        }
    }
    if insert_seq != SEQUENCE_ABSENT {
        if entry.insert_seq == SEQUENCE_ABSENT {
            new_insert = true;
            entry.insert_seq = insert_seq;
        } else if insert_seq > entry.insert_seq {
            entry.insert_seq = insert_seq;
        }
    }
    MergeOutcome {
        entry,
        new_key,
        new_delete,
        new_insert,
    }
}

// =============================================================================
// Layered core
// =============================================================================

/// Shared layered-storage core for [`DeletionIndex`] (`K = i64`) and
/// [`KeyDeletionIndex`] (`K = u128` hash identity). See the [module docs](self)
/// for the tiering design. Key-specific concerns (how a key maps to its bloom
/// hash) stay in the wrappers and are passed in as functions.
#[derive(Debug, Clone)]
struct LayeredTombstones<K, S>
where
    K: Copy + Eq + Hash,
    S: BuildHasher + Default + Clone,
{
    /// Frozen flat tier: never mutated after construction, shared across
    /// generations via `Arc`.
    base: Arc<HashMap<K, TombstoneEntry, S>>,
    /// Mutable tier holding writes since the last merge; `O(1)` to snapshot.
    /// Values here are pre-merged with any base entry for the same key, so a
    /// delta hit never needs a second base lookup.
    delta: PersistentHashMap<K, TombstoneEntry, S>,
    bloom: Arc<SplitBlockBloomFilter>,
    /// Monotonic upper bound over the **delete** sequences in the current
    /// immutable entries. Stays exact because indexes are build-once /
    /// extend-only; any future removal API must recompute it instead of
    /// carrying a stale high-water mark.
    /// `CayenneTableProvider::apply_partial_deletion_filter` relies on this
    /// exact value to decide whether a protected snapshot can skip deletion
    /// filtering without letting deleted rows through.
    max_sequence_number: Option<i64>,
    /// Smallest / largest **deleted** key in the current immutable entries, in
    /// the key's own ordering. `None` when no key has a recorded deletion.
    /// Maintained monotonically (build-once / extend-only) exactly like
    /// `max_sequence_number`; a future removal API must recompute these instead
    /// of carrying a stale bound. Only the *delete* side populates these (a key
    /// gaining its first deletion), mirroring the bloom population and
    /// `delete_count` — insert-only entries are never folded in.
    ///
    /// For the Int64 index (`K = i64`) this is a true PK value range and the
    /// query path (`branch_int64_pk_range` / per-batch gate) uses it to prove a
    /// scan window is disjoint from every deletion and skip the row probe. For
    /// the hash-keyed composite index (`K = u128`) the value is a min/max over
    /// XXH3-128 *hashes* and is meaningless for PK-range pruning, so
    /// `KeyDeletionIndex` deliberately exposes no `deleted_key_range()` accessor
    /// (see its impl block).
    min_deleted_key: Option<K>,
    max_deleted_key: Option<K>,
    /// Number of distinct keys across both tiers.
    entry_count: usize,
    /// Number of keys with a recorded deletion (= bloom population).
    delete_count: usize,
    /// Number of keys with a recorded insert (upsert re-insertion).
    insert_count: usize,
    /// Deletion-key count the current `bloom` was sized for
    /// ([`bloom_capacity_for`]: 2x the deletion count at build time). When
    /// `delete_count` exceeds it, the extend path rebuilds the bloom from
    /// scratch into a fresh `Arc`; otherwise it inserts newly-deleted keys
    /// into the shared filter in place.
    bloom_capacity: usize,
}

impl<K, S> LayeredTombstones<K, S>
where
    // `Ord` is required to maintain the `min_deleted_key`/`max_deleted_key`
    // range. Both concrete key types (`i64`, `u128`) satisfy it, so no caller
    // breaks; the range is only *exposed* for the ordered Int64 key.
    K: Copy + Eq + Hash + Ord,
    S: BuildHasher + Default + Clone,
{
    fn empty() -> Self {
        Self {
            base: Arc::new(HashMap::default()),
            delta: PersistentHashMap::default(),
            bloom: Arc::new(SplitBlockBloomFilter::new(MIN_BLOOM_CAPACITY)),
            max_sequence_number: None,
            min_deleted_key: None,
            max_deleted_key: None,
            entry_count: 0,
            delete_count: 0,
            insert_count: 0,
            bloom_capacity: MIN_BLOOM_CAPACITY,
        }
    }

    /// Bulk-build: deletions land directly in the frozen base (no delta, no
    /// merges); insert records fold into the same entries.
    fn from_iters(
        deleted: impl ExactSizeIterator<Item = (K, i64)>,
        insert_records: impl Iterator<Item = (K, i64)>,
        bloom_hash_of: impl Fn(&K) -> u64,
    ) -> Self {
        let capacity = bloom_capacity_for(deleted.len());
        let bloom = SplitBlockBloomFilter::new(capacity);
        let delete_count = deleted.len();
        let mut max_sequence_number = None;
        let mut min_deleted_key: Option<K> = None;
        let mut max_deleted_key: Option<K> = None;
        let mut base: HashMap<K, TombstoneEntry, S> =
            HashMap::with_capacity_and_hasher(delete_count, S::default());
        for (key, delete_seq) in deleted {
            bloom.insert(bloom_hash_of(&key));
            if max_sequence_number.is_none_or(|max| delete_seq > max) {
                max_sequence_number = Some(delete_seq);
            }
            // Fold the deleted key into the running [min,max]. Delete side only,
            // matching the bloom population and `delete_count`.
            if min_deleted_key.is_none_or(|min| key < min) {
                min_deleted_key = Some(key);
            }
            if max_deleted_key.is_none_or(|max| key > max) {
                max_deleted_key = Some(key);
            }
            base.insert(
                key,
                TombstoneEntry {
                    delete_seq,
                    insert_seq: SEQUENCE_ABSENT,
                },
            );
        }
        let mut insert_count = 0;
        for (key, insert_seq) in insert_records {
            insert_count += 1;
            let outcome = merged_entry(base.get(&key).copied(), SEQUENCE_ABSENT, insert_seq);
            base.insert(key, outcome.entry);
        }

        Self {
            entry_count: base.len(),
            base: Arc::new(base),
            delta: PersistentHashMap::default(),
            bloom: Arc::new(bloom),
            max_sequence_number,
            min_deleted_key,
            max_deleted_key,
            delete_count,
            insert_count,
            bloom_capacity: capacity,
        }
    }

    /// Effective entry for `key`: delta overrides base (delta values are
    /// pre-merged, so the first hit is authoritative).
    #[inline]
    fn entry(&self, key: &K) -> Option<&TombstoneEntry> {
        self.delta.get(key).or_else(|| self.base.get(key))
    }

    /// Tombstone for `key`, bypassing the bloom filter. Callers must perform
    /// the bloom check first (the wrappers keep it in their thin `get` bodies
    /// so the bloom-reject fast path — the overwhelmingly common outcome on
    /// append-mostly scans — inlines into the per-row probe loop without
    /// dragging the tier-walk code with it).
    #[inline]
    fn tombstone_of(&self, key: &K) -> Option<Tombstone> {
        self.entry(key).and_then(Tombstone::from_entry)
    }

    /// Iterate all effective entries (delta first, then base entries the delta
    /// does not override). Each key yields exactly once with its newest value.
    fn iter_entries(&self) -> impl Iterator<Item = (K, TombstoneEntry)> + '_ {
        self.delta.iter().map(|(key, entry)| (*key, *entry)).chain(
            self.base
                .iter()
                .filter(|(key, _)| !self.delta.contains_key(key))
                .map(|(key, entry)| (*key, *entry)),
        )
    }

    /// Core of the extend methods: apply `additions` to a delta snapshot, run
    /// the bloom policy, then the merge policy. `insert_seq` uses
    /// [`SEQUENCE_ABSENT`] for "leave this side unchanged".
    fn extend(
        &self,
        additions: impl Iterator<Item = (K, i64, i64)>,
        bloom_hash_of: impl Fn(&K) -> u64,
    ) -> Self {
        let mut delta = self.delta.clone();
        let mut max_sequence_number = self.max_sequence_number;
        let mut min_deleted_key = self.min_deleted_key;
        let mut max_deleted_key = self.max_deleted_key;
        let mut entry_count = self.entry_count;
        let mut delete_count = self.delete_count;
        let mut insert_count = self.insert_count;
        // Track keys gaining their first deletion so the bloom can be updated
        // incrementally without re-iterating the entire entry set. Pre-size
        // from the iterator's hint to skip Vec growth reallocations.
        let mut new_delete_hashes: Vec<u64> = Vec::with_capacity(additions.size_hint().0);
        for (key, delete_seq, insert_seq) in additions {
            debug_assert_ne!(
                delete_seq, SEQUENCE_ABSENT,
                "real sequences are non-negative"
            );
            let current = self.entry_in(&delta, &key);
            let outcome = merged_entry(current, delete_seq, insert_seq);
            if outcome.new_key {
                entry_count += 1;
            }
            if outcome.new_delete {
                delete_count += 1;
                new_delete_hashes.push(bloom_hash_of(&key));
                // Fold the newly-deleted key into the running [min,max]. Mirrors
                // the bloom population; only keys gaining their first deletion
                // enter the range, never insert-only keys.
                if min_deleted_key.is_none_or(|min| key < min) {
                    min_deleted_key = Some(key);
                }
                if max_deleted_key.is_none_or(|max| key > max) {
                    max_deleted_key = Some(key);
                }
            }
            if outcome.new_insert {
                insert_count += 1;
            }
            if max_sequence_number.is_none_or(|max| outcome.entry.delete_seq > max) {
                max_sequence_number = Some(outcome.entry.delete_seq);
            }
            // Skip the delta write when the addition was a no-op (stale
            // sequences for an existing key): keeps the delta minimal and the
            // merge cadence honest.
            if current != Some(outcome.entry) {
                delta.insert(key, outcome.entry);
            }
        }

        // `max_sequence_number` is maintained incrementally above; we do not
        // re-scan entries here (a full scan would make extends O(N) in debug
        // builds and noticeably slow the test suite as the index grows).
        // `from_iters` is the single bulk-build path and recomputes the exact
        // max from scratch.
        //
        // Bloom policy: rebuild from scratch when deletion growth has outpaced
        // the sized capacity; the new filter takes 2x headroom so the rebuild
        // cadence is geometric. Otherwise insert only the newly-deleted keys
        // into the shared filter in place (O(K) relaxed atomic stores; older
        // pinned generations observe a safe superset — see the module docs).
        let (bloom, bloom_capacity) = if delete_count > self.bloom_capacity {
            let new_capacity = bloom_capacity_for(delete_count);
            let fresh = SplitBlockBloomFilter::new(new_capacity);
            // A key present in both tiers is inserted twice; bloom inserts are
            // idempotent, so that is harmless.
            for (key, entry) in self.base.iter().chain(delta.iter()) {
                if entry.delete_seq != SEQUENCE_ABSENT {
                    fresh.insert(bloom_hash_of(key));
                }
            }
            (Arc::new(fresh), new_capacity)
        } else {
            for hash in new_delete_hashes {
                self.bloom.insert(hash);
            }
            (Arc::clone(&self.bloom), self.bloom_capacity)
        };

        // Merge policy: fold the delta into a fresh frozen base once it
        // crosses the threshold. Old generations keep the previous base Arc,
        // so the transient cost is one extra base — not one per generation.
        let (base, delta) = if delta.len() >= delta_merge_threshold(self.base.len()) {
            let mut merged = HashMap::clone(&self.base);
            merged.reserve(delta.len());
            for (key, entry) in &delta {
                merged.insert(*key, *entry);
            }
            (Arc::new(merged), PersistentHashMap::default())
        } else {
            (Arc::clone(&self.base), delta)
        };

        Self {
            base,
            delta,
            bloom,
            max_sequence_number,
            min_deleted_key,
            max_deleted_key,
            entry_count,
            delete_count,
            insert_count,
            bloom_capacity,
        }
    }

    /// Effective-entry lookup against an in-progress delta (plus the frozen
    /// base), used while applying an additions batch.
    #[inline]
    fn entry_in(
        &self,
        delta: &PersistentHashMap<K, TombstoneEntry, S>,
        key: &K,
    ) -> Option<TombstoneEntry> {
        delta.get(key).or_else(|| self.base.get(key)).copied()
    }

    fn approx_bytes(&self, base_entry_bytes: usize, delta_entry_bytes: usize) -> usize {
        self.base
            .len()
            .saturating_mul(base_entry_bytes)
            .saturating_add(self.delta.len().saturating_mul(delta_entry_bytes))
    }
}

// =============================================================================
// Int64 primary keys
// =============================================================================

/// Frozen deletion index for tables with a single-column Int64 primary key.
///
/// Entries are layered across a frozen flat base and a small delta (see the
/// [module docs](self)), fronted by a bloom filter keyed on deletion
/// membership. The bloom filter's bit array is sized for `bloom_capacity`
/// deletion keys with 2x headroom; in-capacity extends update the shared
/// filter in place and a full O(N) rebuild only happens when the deletion
/// count outgrows the sized capacity, keeping amortized writer cost at O(K)
/// per call (K = number of additions) — see
/// [`extend_max_deletes`](Self::extend_max_deletes) for the full argument.
#[derive(Debug, Clone)]
pub struct DeletionIndex {
    core: LayeredTombstones<i64, DeletionIndexHasher>,
}

impl Default for DeletionIndex {
    fn default() -> Self {
        Self::empty()
    }
}

impl DeletionIndex {
    /// An empty deletion index. Probes always miss; bloom filter still allocated at the
    /// minimum capacity so size-0 indexes don't degrade once the index is extended.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            core: LayeredTombstones::empty(),
        }
    }

    /// Process-wide shared empty index. Use this instead of
    /// `Arc::new(Self::empty())` from per-scan hot paths — the bloom
    /// allocation is amortized across every caller. See
    /// `apply_partial_filter_empty_alloc` bench.
    #[must_use]
    pub fn shared_empty() -> Arc<Self> {
        static EMPTY: LazyLock<Arc<DeletionIndex>> =
            LazyLock::new(|| Arc::new(DeletionIndex::empty()));
        Arc::clone(&EMPTY)
    }

    /// Build a frozen index from an owned `HashMap` of `pk -> delete_sequence`
    /// with no insert records.
    #[must_use]
    pub fn from_map(deleted: HashMap<i64, i64>) -> Self {
        Self::from_maps(deleted, HashMap::new())
    }

    /// Build a frozen index from `pk -> delete_sequence` plus
    /// `pk -> insert_sequence` maps (the catalog load path persists the two
    /// sides separately).
    #[must_use]
    pub fn from_maps(deleted: HashMap<i64, i64>, insert_records: HashMap<i64, i64>) -> Self {
        Self {
            core: LayeredTombstones::from_iters(
                deleted.into_iter(),
                insert_records.into_iter(),
                |pk| hash_key_i64(*pk),
            ),
        }
    }

    /// Build a frozen index from an `Arc<HashMap>` of deletions (clones the map).
    #[must_use]
    pub fn from_arc_map(map: &Arc<HashMap<i64, i64>>) -> Self {
        Self::from_map((**map).clone())
    }

    /// Total number of fused entries (keys with a deletion, an insert record,
    /// or both).
    #[must_use]
    pub fn len(&self) -> usize {
        self.core.entry_count
    }

    /// Whether the index has no entries at all.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.core.entry_count == 0
    }

    /// Number of keys with a recorded deletion.
    #[must_use]
    pub fn delete_len(&self) -> usize {
        self.core.delete_count
    }

    /// Number of keys with a recorded insert (upsert re-insertion).
    #[must_use]
    pub fn insert_len(&self) -> usize {
        self.core.insert_count
    }

    /// Whether any key has a recorded deletion. Scan fast paths skip the
    /// filter entirely when this is `false` (insert-only entries never affect
    /// visibility).
    #[must_use]
    pub fn has_deletions(&self) -> bool {
        self.core.delete_count > 0
    }

    /// Approximate resident bytes for memory accounting. Base entries are
    /// flat-table slots (`i64 -> (i64, i64)` payload plus `SwissTable` load
    /// factor and control bytes); delta entries carry HAMT node/bitmap/Arc
    /// overhead. Shared structure retained by older reader-pinned generations
    /// is intentionally not charged to the latest snapshot again.
    #[must_use]
    pub fn approx_bytes(&self) -> usize {
        const APPROX_I64_BASE_ENTRY_BYTES: usize = 32;
        const APPROX_I64_DELTA_ENTRY_BYTES: usize = 80;
        self.core
            .approx_bytes(APPROX_I64_BASE_ENTRY_BYTES, APPROX_I64_DELTA_ENTRY_BYTES)
    }

    /// Highest **delete** sequence number in this index, if any.
    #[must_use]
    pub fn max_sequence_number(&self) -> Option<i64> {
        self.core.max_sequence_number
    }

    /// Closed `[min,max]` of all **deleted** PK values in this index, or `None`
    /// when no key has a recorded deletion (`delete_len() == 0`).
    ///
    /// This is a sound *superset* of the PKs the filter could remove: every PK
    /// with a tombstone lies within it (including deletions below a protected
    /// snapshot's `min_delete_seq_to_apply` cutoff that the visibility check
    /// would skip — the range is intentionally NOT narrowed by sequence, which
    /// keeps it a conservative superset of the applicable-by-sequence subset).
    /// A scan window proven disjoint from this range therefore contains no
    /// deletable PK, so the deletion filter can be skipped for that branch /
    /// batch without dropping a live row or keeping a deleted one. See
    /// `CayenneTableProvider::branch_int64_pk_range` and the
    /// `Int64PkDeletionFilterStream` per-batch fast path.
    #[must_use]
    pub fn deleted_key_range(&self) -> Option<(i64, i64)> {
        Some((self.core.min_deleted_key?, self.core.max_deleted_key?))
    }

    /// Bloom-filter check against deletion membership. Returns `false` if the key
    /// definitely has no recorded deletion; `true` if it might (and a
    /// [`get`](Self::get) is required to confirm).
    #[inline]
    #[must_use]
    pub fn might_contain(&self, pk: i64) -> bool {
        self.core.bloom.might_contain(hash_key_i64(pk))
    }

    /// Bloom-prefiltered lookup. Returns the key's [`Tombstone`] if `pk` has a
    /// recorded deletion, `None` otherwise.
    ///
    /// Keys holding only an insert record are reported as `None`: the bloom is
    /// keyed on deletion membership, and visibility treats "no deletion" and
    /// "insert-only" identically. One probe answers both the deletion and the
    /// re-insertion question.
    ///
    /// # Inlining trade-off (measured)
    ///
    /// Keeping the tier walk fully inlined here is a deliberate choice that
    /// favors present-key probes over bloom rejects. With the walk inlined,
    /// out-of-cache present-key probes overlap their base-map misses across
    /// loop iterations (memory-level parallelism): 92µs vs 143µs per 8192
    /// probes at 1M entries when the walk is forced out of line. The cost is
    /// the inverse effect on the bloom-reject path, whose tight probe loop no
    /// longer unrolls as densely (~3.7ns → ~6ns per rejected probe; composite
    /// keys are unaffected). Changes-mode scans — the workload this index is
    /// hot in — are present-key heavy, so the walk stays inlined. Callers
    /// probing a whole batch should prefer [`get_batch`](Self::get_batch),
    /// which sweeps the bloom across the batch first and walks only the
    /// survivors — winning on both paths.
    #[inline]
    #[must_use]
    pub fn get(&self, pk: i64) -> Option<Tombstone> {
        if !self.core.bloom.might_contain(hash_key_i64(pk)) {
            return None;
        }
        self.core.tombstone_of(&pk)
    }

    /// Batched bloom-prefiltered lookup: invokes `on_hit(index, tombstone)`
    /// for every `pks[index]` that has a recorded deletion, in ascending
    /// `index` order. Exactly equivalent to calling [`get`](Self::get) on each
    /// element and reporting the `Some` results — keys holding only an insert
    /// record are reported absent, per the same contract.
    ///
    /// # Why a batch entry point
    ///
    /// [`get`](Self::get) fuses the bloom probe with the tier walk per key, so
    /// on a bloom hit the delta/base map walk (an out-of-cache load at scale)
    /// serializes against the *next* key's bloom probe. This method
    /// restructures the loop into the column-sweep form the read-side
    /// `KeyBasedDeletionFilterExec` already uses (b3 sub-lever 2):
    ///
    /// 1. a tight bloom sweep over a chunk of keys — branch-light, one
    ///    independent 32-byte block load per key that the out-of-order window
    ///    overlaps across iterations — collecting candidate positions;
    /// 2. the delta→base tier walk only for the surviving candidates (bloom
    ///    false positives resolve to "absent" here, preserving per-key `get`
    ///    results).
    ///
    /// Keys are processed in [`BATCH_SWEEP_CHUNK`]-sized chunks so the
    /// candidate buffer is still cache-hot when pass 2 consumes it.
    pub fn get_batch(&self, pks: &[i64], mut on_hit: impl FnMut(usize, Tombstone)) {
        // The bloom is keyed on deletion membership only: with no recorded
        // deletions every probe would miss (insert-only entries probe as
        // absent), so skip the sweep entirely.
        if self.core.delete_count == 0 {
            return;
        }
        let mut candidates: Vec<u32> = Vec::with_capacity(BATCH_SWEEP_CHUNK.min(pks.len()));
        for (chunk_index, chunk) in pks.chunks(BATCH_SWEEP_CHUNK).enumerate() {
            let chunk_base = chunk_index * BATCH_SWEEP_CHUNK;
            candidates.clear();
            // Pass 1: bloom sweep — no tier walk in the loop body.
            for (i, &pk) in (0_u32..).zip(chunk.iter()) {
                if self.core.bloom.might_contain(hash_key_i64(pk)) {
                    candidates.push(i);
                }
            }
            // Pass 2: tier walk for the bloom survivors only.
            for &i in &candidates {
                if let Some(tombstone) = self.core.tombstone_of(&chunk[i as usize]) {
                    on_hit(chunk_base + i as usize, tombstone);
                }
            }
        }
    }

    /// Iterate all effective entries (for callers that need a full walk, e.g.
    /// benches; project sides via [`TombstoneEntry::delete_sequence`] /
    /// [`TombstoneEntry::insert_sequence`]). Each key yields exactly once with
    /// its newest value.
    pub fn iter_entries(&self) -> impl Iterator<Item = (i64, TombstoneEntry)> + '_ {
        self.core.iter_entries()
    }

    /// Build a new index from `self`'s entries plus delete-only `additions`
    /// (`pk -> delete_sequence`), taking the per-key max sequence on conflict.
    /// Used by writers to publish a new snapshot via `ArcSwap::store`.
    ///
    /// # Performance
    ///
    /// Additions land in the small delta tier (`O(log delta)` persistent-map
    /// inserts that share structure with reader-pinned generations); the
    /// frozen base is shared by `Arc` and only re-copied when the delta
    /// crosses [`delta_merge_threshold`] — a geometric cadence that amortizes
    /// to `O(log N)` total copy work per key. The bloom filter is *shared*
    /// with the parent index behind an `Arc`: keys gaining their first
    /// deletion are inserted into it in place (relaxed atomic stores, O(K), no
    /// copy of the bit array — at millions of entries a per-call copy would be
    /// a multi-megabyte memcpy on every CDC batch). A full O(N) bloom rebuild
    /// only happens when the deletion count outgrows the capacity the filter
    /// was sized for — 2x the deletion count at build time
    /// ([`bloom_capacity_for`]) — so the rebuild cadence is geometric and the
    /// amortized bloom cost stays O(K) per call.
    ///
    /// Sharing the filter means older pinned generations observe bits for keys
    /// added after they were published. That is a safe superset: extra bits can
    /// only add false positives (caught by the map probe), never false
    /// negatives. Writers are serialized by the per-table write lock, which
    /// keeps the `bloom_capacity` bookkeeping consistent across generations.
    ///
    /// **Why this matters**: a previous revision rebuilt the bloom from scratch on
    /// every extend call, which is the dominant cost (10K entries ≈ 10K hash
    /// ops ≈ ~1 ms per call before any map update work is counted).
    /// On high-rate upsert/delete workloads (each producing a small `additions`
    /// batch but operating on a deletion cache that grows over time), the wasted
    /// bloom rebuild work compounds — and is the root cause of the ingestion
    /// regression that prompted this fix.
    #[must_use]
    pub fn extend_max_deletes(&self, additions: impl IntoIterator<Item = (i64, i64)>) -> Self {
        Self {
            core: self.core.extend(
                additions
                    .into_iter()
                    .map(|(pk, delete_seq)| (pk, delete_seq, SEQUENCE_ABSENT)),
                |pk| hash_key_i64(*pk),
            ),
        }
    }

    /// Build a new index recording an upsert-conflict batch: every key in
    /// `keys` gains a deletion at `delete_sequence` **and** a re-insertion at
    /// `insert_sequence`, each merged with per-key max. One pass over the map
    /// replaces the previous two-index double extend.
    #[must_use]
    pub fn extend_max_conflicts(
        &self,
        keys: impl IntoIterator<Item = i64>,
        delete_sequence: i64,
        insert_sequence: i64,
    ) -> Self {
        Self {
            core: self.core.extend(
                keys.into_iter()
                    .map(|pk| (pk, delete_sequence, insert_sequence)),
                |pk| hash_key_i64(*pk),
            ),
        }
    }
}

// =============================================================================
// Composite / non-integer primary keys
// =============================================================================

/// Splits a 128-bit key hash into its map identity and the (independent)
/// 64 bits fed to the bloom filter. Using disjoint halves keeps the bloom's
/// block/bit selection uncorrelated with the map's bucket selection (the maps
/// consume the low half via [`PrehashedBuildHasher`]).
#[inline]
fn bloom_half(hash: u128) -> u64 {
    (hash >> 64) as u64
}

/// Frozen deletion index for tables with a composite or non-integer primary key.
/// Probe keys are the byte-encoded form produced by `arrow_row::RowConverter`.
///
/// Entries are keyed by the **seeded XXH3-128 hash of the key bytes**
/// ([`hash_key_128`]) rather than the bytes themselves: the hash is the key's
/// identity and the bytes are not retained. This fixes the per-entry footprint
/// at 32 bytes regardless of composite-key width, removes byte comparisons
/// from probes, and (via [`PrehashedBuildHasher`]) lets the maps reuse the
/// hash's own entropy instead of re-hashing per probe — one hash computation
/// serves the bloom filter and the maps together.
///
/// Two distinct keys colliding under XXH3-128 would share a tombstone; at one
/// billion keys the birthday bound puts that below ~1e-20 — orders of
/// magnitude under hardware error rates. (A 64-bit hash would NOT be safe as
/// identity at these cardinalities: ~0.3% collision odds at the same scale.)
///
/// See [`DeletionIndex`] for the fused-entry, layered-storage, bloom-capacity,
/// and shared-filter contracts.
#[derive(Debug, Clone)]
pub struct KeyDeletionIndex {
    core: LayeredTombstones<u128, PrehashedBuildHasher>,
}

impl Default for KeyDeletionIndex {
    fn default() -> Self {
        Self::empty()
    }
}

impl KeyDeletionIndex {
    /// An empty index.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            core: LayeredTombstones::empty(),
        }
    }

    /// Process-wide shared empty index. Use this instead of
    /// `Arc::new(Self::empty())` from per-scan hot paths.
    #[must_use]
    pub fn shared_empty() -> Arc<Self> {
        static EMPTY: LazyLock<Arc<KeyDeletionIndex>> =
            LazyLock::new(|| Arc::new(KeyDeletionIndex::empty()));
        Arc::clone(&EMPTY)
    }

    /// Build a frozen index from an owned `HashMap` of `pk_bytes ->
    /// delete_sequence` with no insert records.
    #[must_use]
    pub fn from_map(deleted: HashMap<Box<[u8]>, i64>) -> Self {
        Self::from_maps(deleted, HashMap::new())
    }

    /// Build a frozen index from `pk_bytes -> delete_sequence` plus
    /// `pk_bytes -> insert_sequence` maps (the catalog load path persists the
    /// two sides separately).
    #[must_use]
    pub fn from_maps(
        deleted: HashMap<Box<[u8]>, i64>,
        insert_records: HashMap<Box<[u8]>, i64>,
    ) -> Self {
        Self {
            core: LayeredTombstones::from_iters(
                deleted
                    .into_iter()
                    .map(|(key, seq)| (hash_key_128(&key), seq)),
                insert_records
                    .into_iter()
                    .map(|(key, seq)| (hash_key_128(&key), seq)),
                |key_hash| bloom_half(*key_hash),
            ),
        }
    }

    /// Build a frozen index from an `Arc<HashMap>` of deletions (clones the map).
    #[must_use]
    pub fn from_arc_map(map: &Arc<HashMap<Box<[u8]>, i64>>) -> Self {
        Self::from_map((**map).clone())
    }

    /// Total number of fused entries (keys with a deletion, an insert record,
    /// or both).
    #[must_use]
    pub fn len(&self) -> usize {
        self.core.entry_count
    }

    /// Number of keys with a recorded deletion.
    #[must_use]
    pub fn delete_len(&self) -> usize {
        self.core.delete_count
    }

    /// Number of keys with a recorded insert (upsert re-insertion).
    #[must_use]
    pub fn insert_len(&self) -> usize {
        self.core.insert_count
    }

    /// Whether any key has a recorded deletion. See
    /// [`DeletionIndex::has_deletions`].
    #[must_use]
    pub fn has_deletions(&self) -> bool {
        self.core.delete_count > 0
    }

    /// Approximate resident bytes for memory accounting. Base entries are
    /// flat-table slots (`u128 -> (i64, i64)` payload plus `SwissTable` load
    /// factor and control bytes); delta entries carry HAMT node/bitmap/Arc
    /// overhead. Key bytes are not retained (hash-keyed identity), so the
    /// estimate no longer depends on composite-key width. Shared structure
    /// retained by older reader-pinned generations is intentionally not
    /// charged to the latest snapshot again.
    #[must_use]
    pub fn approx_bytes(&self) -> usize {
        const APPROX_KEY_BASE_ENTRY_BYTES: usize = 40;
        const APPROX_KEY_DELTA_ENTRY_BYTES: usize = 88;
        self.core
            .approx_bytes(APPROX_KEY_BASE_ENTRY_BYTES, APPROX_KEY_DELTA_ENTRY_BYTES)
    }

    /// Whether the index has no entries at all.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.core.entry_count == 0
    }

    /// Highest **delete** sequence number in this index, if any.
    #[must_use]
    pub fn max_sequence_number(&self) -> Option<i64> {
        self.core.max_sequence_number
    }

    // NOTE: there is deliberately NO `deleted_key_range()` here. The core's
    // `min_deleted_key`/`max_deleted_key` are over the XXH3-128 *hash* of the
    // PK bytes (the original bytes are not retained — see the module docs), so a
    // min/max over them is meaningless for PK value-range pruning and could
    // wrongly report a hash-window as disjoint from a scan's value window. The
    // composite-PK read-tax is instead attacked by the batch-level bloom sweep
    // in `KeyBasedDeletionFilterStream` (b3 sub-lever 2), which is sound because
    // `might_contain` has no false negatives.

    /// Bloom-filter check against deletion membership; see
    /// [`DeletionIndex::might_contain`].
    #[inline]
    #[must_use]
    pub fn might_contain(&self, key: &[u8]) -> bool {
        self.core.bloom.might_contain(bloom_half(hash_key_128(key)))
    }

    /// Bloom-prefiltered lookup. Returns the key's [`Tombstone`] if it has a
    /// recorded deletion, `None` otherwise. See [`DeletionIndex::get`] for the
    /// insert-only-entry contract.
    ///
    /// One XXH3-128 computation serves both the bloom check (high half) and
    /// the map probes (full hash as identity, low half as bucket entropy via
    /// [`PrehashedBuildHasher`]) — the key bytes are never re-hashed or
    /// compared.
    #[inline]
    #[must_use]
    pub fn get(&self, key: &[u8]) -> Option<Tombstone> {
        let key_hash = hash_key_128(key);
        if !self.core.bloom.might_contain(bloom_half(key_hash)) {
            return None;
        }
        self.core.tombstone_of(&key_hash)
    }

    /// Batched bloom-prefiltered lookup over row-encoded keys: invokes
    /// `on_hit(index, tombstone)` for every key (0-based position in `keys`)
    /// that has a recorded deletion, in ascending `index` order. Exactly
    /// equivalent to calling [`get`](Self::get) on each key and reporting the
    /// `Some` results — insert-only entries are reported absent, per the same
    /// contract.
    ///
    /// Mirrors [`DeletionIndex::get_batch`]: a chunked XXH3-128 hash pass
    /// feeds a tight bloom sweep (high hash half), and the delta→base tier
    /// walk runs only for the surviving candidates. The full 128-bit hash is
    /// retained per chunk, so survivors probe the maps by hash identity
    /// without re-hashing the key bytes — one hash computation per key, same
    /// as [`get`](Self::get).
    pub fn get_batch<'k>(
        &self,
        keys: impl IntoIterator<Item = &'k [u8]>,
        mut on_hit: impl FnMut(usize, Tombstone),
    ) {
        // Deletion-keyed bloom: with no recorded deletions every probe would
        // miss (insert-only entries probe as absent), so skip the sweep.
        if self.core.delete_count == 0 {
            return;
        }
        let mut keys = keys.into_iter();
        let mut hashes: Vec<u128> = Vec::with_capacity(BATCH_SWEEP_CHUNK);
        let mut candidates: Vec<u32> = Vec::with_capacity(BATCH_SWEEP_CHUNK);
        let mut chunk_base = 0_usize;
        loop {
            hashes.clear();
            hashes.extend(keys.by_ref().take(BATCH_SWEEP_CHUNK).map(hash_key_128));
            if hashes.is_empty() {
                break;
            }
            candidates.clear();
            // Pass 1: bloom sweep over the chunk's hashes — no tier walk in
            // the loop body.
            for (i, &key_hash) in (0_u32..).zip(hashes.iter()) {
                if self.core.bloom.might_contain(bloom_half(key_hash)) {
                    candidates.push(i);
                }
            }
            // Pass 2: tier walk for the bloom survivors only (probed by the
            // retained hash identity; false positives resolve to "absent").
            for &i in &candidates {
                if let Some(tombstone) = self.core.tombstone_of(&hashes[i as usize]) {
                    on_hit(chunk_base + i as usize, tombstone);
                }
            }
            chunk_base += hashes.len();
        }
    }

    /// Iterate all effective entries, keyed by the XXH3-128 hash of the
    /// original key bytes. Each key yields exactly once with its newest value.
    pub fn iter_entries(&self) -> impl Iterator<Item = (u128, TombstoneEntry)> + '_ {
        self.core.iter_entries()
    }

    /// Build a new index from `self`'s entries plus delete-only `additions`,
    /// taking the per-key max sequence on conflict. Keys are borrowed — the
    /// index stores their XXH3-128 hash, never the bytes.
    ///
    /// See [`DeletionIndex::extend_max_deletes`] for the layering,
    /// amortization, and shared-filter safety argument.
    #[must_use]
    pub fn extend_max_deletes<K: AsRef<[u8]>>(
        &self,
        additions: impl IntoIterator<Item = (K, i64)>,
    ) -> Self {
        Self {
            core: self.core.extend(
                additions.into_iter().map(|(key, delete_seq)| {
                    (hash_key_128(key.as_ref()), delete_seq, SEQUENCE_ABSENT)
                }),
                |key_hash| bloom_half(*key_hash),
            ),
        }
    }

    /// Build a new index recording an upsert-conflict batch; see
    /// [`DeletionIndex::extend_max_conflicts`].
    #[must_use]
    pub fn extend_max_conflicts<K: AsRef<[u8]>>(
        &self,
        keys: impl IntoIterator<Item = K>,
        delete_sequence: i64,
        insert_sequence: i64,
    ) -> Self {
        Self {
            core: self.core.extend(
                keys.into_iter()
                    .map(|key| (hash_key_128(key.as_ref()), delete_sequence, insert_sequence)),
                |key_hash| bloom_half(*key_hash),
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn byte_key(value: u64) -> Box<[u8]> {
        value.to_be_bytes().to_vec().into_boxed_slice()
    }

    fn delete_seq_of(idx: &DeletionIndex, pk: i64) -> Option<i64> {
        idx.get(pk).map(|t| t.delete_sequence)
    }

    fn key_delete_seq_of(idx: &KeyDeletionIndex, key: &[u8]) -> Option<i64> {
        idx.get(key).map(|t| t.delete_sequence)
    }

    #[test]
    fn from_map_then_get() {
        let mut map = HashMap::new();
        map.insert(100, 1);
        map.insert(200, 2);
        map.insert(300, 3);
        let idx = DeletionIndex::from_map(map);

        assert_eq!(idx.len(), 3);
        assert_eq!(idx.delete_len(), 3);
        assert_eq!(idx.insert_len(), 0);
        assert_eq!(idx.max_sequence_number(), Some(3));
        assert_eq!(delete_seq_of(&idx, 100), Some(1));
        assert_eq!(delete_seq_of(&idx, 200), Some(2));
        assert_eq!(delete_seq_of(&idx, 300), Some(3));
        assert_eq!(idx.get(100).and_then(|t| t.insert_sequence), None);
        assert_eq!(idx.get(400), None);
    }

    // [b3 sub-lever 1] The deleted-key min/max range is the plan-time gate that
    // lets a PK-disjoint branch skip the deletion filter. It must (a) track the
    // true min/max over the delete side, (b) never include insert-only keys,
    // and (c) be `None` when there are no deletions.
    #[test]
    fn deleted_key_range_tracks_min_max_int64() {
        let idx = DeletionIndex::from_map(HashMap::from([(100, 1), (300, 3), (50, 2)]));
        assert_eq!(idx.deleted_key_range(), Some((50, 300)));

        // Extending with deletes outside the range widens it monotonically.
        let grown = idx.extend_max_deletes([(10, 4), (400, 5)]);
        assert_eq!(grown.deleted_key_range(), Some((10, 400)));

        // Empty index has no range.
        assert_eq!(DeletionIndex::empty().deleted_key_range(), None);
    }

    #[test]
    fn deleted_key_range_ignores_insert_only() {
        // Insert-only key (no deletion) must NOT enter the deleted-key range,
        // mirroring `delete_len() == 0`.
        let idx = DeletionIndex::from_maps(HashMap::new(), HashMap::from([(7_i64, 70_i64)]));
        assert_eq!(idx.delete_len(), 0);
        assert_eq!(idx.deleted_key_range(), None);
    }

    #[test]
    fn deleted_key_range_after_delete_of_insert_only() {
        // An insert-only key gains a range entry only once it is actually
        // deleted.
        let idx = DeletionIndex::from_maps(HashMap::new(), HashMap::from([(7_i64, 70_i64)]));
        assert_eq!(idx.deleted_key_range(), None);
        let after = idx.extend_max_deletes([(7_i64, 80_i64)]);
        assert_eq!(after.deleted_key_range(), Some((7, 7)));
    }

    #[test]
    fn empty_index_probes_to_none() {
        let idx = DeletionIndex::empty();
        assert!(idx.is_empty());
        assert!(!idx.has_deletions());
        assert_eq!(idx.max_sequence_number(), None);
        assert_eq!(idx.get(42), None);
    }

    #[test]
    fn extend_max_deletes_takes_higher_sequence() {
        let mut map = HashMap::new();
        map.insert(100, 5);
        let idx = DeletionIndex::from_map(map);

        let next = idx.extend_max_deletes([(100, 3), (200, 7)]);
        assert_eq!(next.max_sequence_number(), Some(7));
        assert_eq!(delete_seq_of(&next, 100), Some(5));
        assert_eq!(delete_seq_of(&next, 200), Some(7));

        let after = next.extend_max_deletes([(100, 10)]);
        assert_eq!(after.max_sequence_number(), Some(10));
        assert_eq!(delete_seq_of(&after, 100), Some(10));
    }

    #[test]
    fn conflicts_record_delete_and_insert() {
        let idx = DeletionIndex::empty();
        let next = idx.extend_max_conflicts([1, 2, 3], 10, 11);

        assert_eq!(next.len(), 3);
        assert_eq!(next.delete_len(), 3);
        assert_eq!(next.insert_len(), 3);
        assert_eq!(
            next.max_sequence_number(),
            Some(10),
            "insert seqs must not raise the delete max"
        );
        for pk in [1, 2, 3] {
            let tombstone = next.get(pk).expect("conflict key must have a tombstone");
            assert_eq!(tombstone.delete_sequence, 10);
            assert_eq!(tombstone.insert_sequence, Some(11));
        }
    }

    #[test]
    fn conflicts_merge_max_per_side() {
        let idx = DeletionIndex::empty();
        let first = idx.extend_max_conflicts([42], 10, 11);
        // An older conflict must not lower either side.
        let second = first.extend_max_conflicts([42], 5, 6);
        let tombstone = second.get(42).expect("tombstone");
        assert_eq!(tombstone.delete_sequence, 10);
        assert_eq!(tombstone.insert_sequence, Some(11));
        assert_eq!(second.len(), 1);
        assert_eq!(second.delete_len(), 1);
        assert_eq!(second.insert_len(), 1);

        // A newer conflict raises both sides.
        let third = second.extend_max_conflicts([42], 20, 21);
        let tombstone = third.get(42).expect("tombstone");
        assert_eq!(tombstone.delete_sequence, 20);
        assert_eq!(tombstone.insert_sequence, Some(21));
    }

    #[test]
    fn insert_only_entries_are_invisible_to_get() {
        // Post-compaction state: delete files purged, insert records remain.
        let inserts: HashMap<i64, i64> = (0..10).map(|pk| (pk, pk + 100)).collect();
        let idx = DeletionIndex::from_maps(HashMap::new(), inserts);

        assert_eq!(idx.len(), 10);
        assert_eq!(idx.delete_len(), 0);
        assert_eq!(idx.insert_len(), 10);
        assert!(!idx.has_deletions());
        assert_eq!(idx.max_sequence_number(), None);
        for pk in 0..10 {
            assert_eq!(
                idx.get(pk),
                None,
                "insert-only pk={pk} must probe as absent"
            );
        }
    }

    #[test]
    fn from_maps_fuses_overlapping_keys() {
        let deleted: HashMap<i64, i64> = HashMap::from([(1, 10), (2, 20)]);
        let inserts: HashMap<i64, i64> = HashMap::from([(2, 21), (3, 30)]);
        let idx = DeletionIndex::from_maps(deleted, inserts);

        assert_eq!(idx.len(), 3);
        assert_eq!(idx.delete_len(), 2);
        assert_eq!(idx.insert_len(), 2);
        assert_eq!(idx.max_sequence_number(), Some(20));

        let lone_delete = idx.get(1).expect("tombstone");
        assert_eq!(lone_delete.delete_sequence, 10);
        assert_eq!(lone_delete.insert_sequence, None);

        let fused = idx.get(2).expect("tombstone");
        assert_eq!(fused.delete_sequence, 20);
        assert_eq!(fused.insert_sequence, Some(21));

        assert_eq!(idx.get(3), None, "insert-only key must probe as absent");
    }

    #[test]
    fn delete_after_insert_only_enters_bloom() {
        // An existing insert-only entry gaining its first deletion must become
        // probeable (exercises the bloom insert on the occupied-entry path).
        let inserts: HashMap<i64, i64> = HashMap::from([(7, 70)]);
        let idx = DeletionIndex::from_maps(HashMap::new(), inserts);
        assert_eq!(idx.get(7), None);

        let next = idx.extend_max_deletes([(7, 80)]);
        let tombstone = next.get(7).expect("tombstone after delete");
        assert_eq!(tombstone.delete_sequence, 80);
        assert_eq!(tombstone.insert_sequence, Some(70));
        assert_eq!(next.delete_len(), 1);
        assert_eq!(
            next.len(),
            1,
            "delete of an insert-only key must not add an entry"
        );
    }

    #[test]
    fn bloom_rejects_most_misses() {
        let mut map = HashMap::new();
        for i in 0..100_i64 {
            map.insert(i * 2, i);
        }
        let idx = DeletionIndex::from_map(map);

        let mut rejects = 0;
        for i in 0..100 {
            let odd = i * 2 + 1;
            if !idx.might_contain(odd) {
                rejects += 1;
            }
        }
        assert!(
            rejects > 80,
            "expected bloom to reject most non-deleted keys, got {rejects}"
        );
    }

    /// Reference implementation for the `get_batch` equivalence tests: the
    /// scalar per-row probe the batch sweep replaces.
    fn per_row_gets_int64(idx: &DeletionIndex, pks: &[i64]) -> Vec<Option<Tombstone>> {
        pks.iter().map(|&pk| idx.get(pk)).collect()
    }

    /// Collect `get_batch` results into per-row form, asserting the callback
    /// contract along the way (strictly ascending indices, each at most once,
    /// all in bounds).
    fn batch_gets_int64(idx: &DeletionIndex, pks: &[i64]) -> Vec<Option<Tombstone>> {
        let mut out: Vec<Option<Tombstone>> = vec![None; pks.len()];
        let mut last: Option<usize> = None;
        idx.get_batch(pks, |i, tombstone| {
            assert!(i < pks.len(), "callback index out of bounds: {i}");
            assert!(
                last.is_none_or(|prev| prev < i),
                "callback indices must be strictly ascending: {last:?} then {i}"
            );
            last = Some(i);
            out[i] = Some(tombstone);
        });
        out
    }

    fn per_row_gets_key(idx: &KeyDeletionIndex, keys: &[Box<[u8]>]) -> Vec<Option<Tombstone>> {
        keys.iter().map(|key| idx.get(key)).collect()
    }

    fn batch_gets_key(idx: &KeyDeletionIndex, keys: &[Box<[u8]>]) -> Vec<Option<Tombstone>> {
        let mut out: Vec<Option<Tombstone>> = vec![None; keys.len()];
        let mut last: Option<usize> = None;
        idx.get_batch(keys.iter().map(AsRef::as_ref), |i, tombstone| {
            assert!(i < keys.len(), "callback index out of bounds: {i}");
            assert!(
                last.is_none_or(|prev| prev < i),
                "callback indices must be strictly ascending: {last:?} then {i}"
            );
            last = Some(i);
            out[i] = Some(tombstone);
        });
        out
    }

    /// `get_batch` must report exactly what per-row [`DeletionIndex::get`]
    /// reports, across a layered index (frozen base + delta + insert-only +
    /// fused conflict entries) and every batch shape the apply path produces.
    #[test]
    fn get_batch_matches_per_row_get_int64() {
        // Base tier: even keys 0..200 deleted at seq 1; key 4 also carries an
        // insert record (re-inserted at seq 50); key 1000 is insert-only.
        let deleted: HashMap<i64, i64> = (0..100).map(|i| (i * 2, 1)).collect();
        let inserts: HashMap<i64, i64> = HashMap::from([(4, 50), (1000, 70)]);
        let base = DeletionIndex::from_maps(deleted, inserts);
        // Delta tier: a small extend (below the test merge floor) so probes
        // exercise the delta→base walk, plus fused conflicts and extreme keys.
        let idx = base
            .extend_max_deletes([(3, 7), (i64::MIN, 9), (i64::MAX, 11)])
            .extend_max_conflicts([5_i64, 4], 20, 21);

        let shapes: &[&[i64]] = &[
            &[],                             // empty batch
            &[2],                            // single row, hit
            &[9999],                         // single row, miss
            &[0, 2, 4, 6, 8],                // all-hit
            &[1, 7, 9, 8887, 9999],          // all-miss (odd / out of range)
            &[0, 1, 2, 7, 4, 9, 3, 5],       // alternating hit/miss + delta hits
            &[2, 2, 9999, 2],                // duplicate keys in one batch
            &[i64::MIN, -1, 0, 1, i64::MAX], // extreme key values
            &[1000, 4, 5],                   // insert-only (absent) vs fused entries
        ];
        for pks in shapes {
            assert_eq!(
                batch_gets_int64(&idx, pks),
                per_row_gets_int64(&idx, pks),
                "get_batch diverged from per-row get for {pks:?}"
            );
        }

        // Spot-check the fused-entry payloads survive the batch path intact.
        let fused = batch_gets_int64(&idx, &[5, 4, 1000]);
        assert_eq!(
            fused[0],
            Some(Tombstone {
                delete_sequence: 20,
                insert_sequence: Some(21)
            })
        );
        // Key 4: insert record at 50 (base) fused with conflict insert at 21
        // (delta) — per-side max keeps 50; conflict delete at 20 wins over 1.
        assert_eq!(
            fused[1],
            Some(Tombstone {
                delete_sequence: 20,
                insert_sequence: Some(50)
            })
        );
        assert_eq!(fused[2], None, "insert-only key must probe as absent");
    }

    /// Batches longer than one sweep chunk must keep global (not chunk-local)
    /// indices and stay equivalent to per-row probes across the boundary.
    #[test]
    fn get_batch_spans_chunk_boundaries_int64() {
        let deleted: HashMap<i64, i64> = (0..3000).map(|i| (i * 3, i + 1)).collect();
        let idx = DeletionIndex::from_map(deleted);

        // 5000 keys = 2 full chunks + a partial tail at BATCH_SWEEP_CHUNK=2048.
        let pks: Vec<i64> = (0..5000).collect();
        assert!(pks.len() > 2 * BATCH_SWEEP_CHUNK);
        assert_eq!(batch_gets_int64(&idx, &pks), per_row_gets_int64(&idx, &pks));
    }

    /// Empty and insert-only indexes take the `delete_count == 0` early-out;
    /// it must agree with per-row probes (all absent).
    #[test]
    fn get_batch_empty_and_insert_only_indexes() {
        let pks: Vec<i64> = (0..100).collect();

        let empty = DeletionIndex::empty();
        assert_eq!(batch_gets_int64(&empty, &pks), vec![None; pks.len()]);

        let insert_only =
            DeletionIndex::from_maps(HashMap::new(), (0..50).map(|i| (i, i + 1)).collect());
        assert_eq!(batch_gets_int64(&insert_only, &pks), vec![None; pks.len()]);

        let empty_key_idx = KeyDeletionIndex::empty();
        let keys: Vec<Box<[u8]>> = (0..100_u64).map(byte_key).collect();
        assert_eq!(
            batch_gets_key(&empty_key_idx, &keys),
            vec![None; keys.len()]
        );
    }

    /// Composite-key equivalence: layered index, varying key widths (including
    /// the empty key), duplicates, and a chunk-spanning batch.
    #[test]
    fn get_batch_matches_per_row_get_composite() {
        let deleted: HashMap<Box<[u8]>, i64> = (0..100_u64).map(|i| (byte_key(i * 2), 1)).collect();
        let inserts: HashMap<Box<[u8]>, i64> = HashMap::from([(byte_key(4), 50)]);
        let base = KeyDeletionIndex::from_maps(deleted, inserts);
        let wide_key: Box<[u8]> = vec![7_u8; 24].into_boxed_slice(); // composite-width key
        let empty_key: Box<[u8]> = Vec::new().into_boxed_slice();
        let idx = base
            .extend_max_deletes([(wide_key.clone(), 7), (empty_key.clone(), 9)])
            .extend_max_conflicts([byte_key(6)], 20, 21);

        let shapes: Vec<Vec<Box<[u8]>>> = vec![
            vec![],                                             // empty batch
            vec![byte_key(2)],                                  // single hit
            vec![byte_key(3)],                                  // single miss
            (0..10_u64).map(|i| byte_key(i * 2)).collect(),     // all-hit
            (0..10_u64).map(|i| byte_key(i * 2 + 1)).collect(), // all-miss
            vec![
                byte_key(0),
                byte_key(1),
                wide_key.clone(),
                byte_key(3),
                empty_key.clone(),
                byte_key(6),
            ], // alternating + width mix + fused entry
            vec![byte_key(2), byte_key(2), byte_key(3), byte_key(2)], // duplicates
        ];
        for keys in &shapes {
            assert_eq!(
                batch_gets_key(&idx, keys),
                per_row_gets_key(&idx, keys),
                "composite get_batch diverged from per-row get"
            );
        }

        // Chunk-spanning batch (2 full chunks + tail).
        let many: Vec<Box<[u8]>> = (0..5000_u64).map(byte_key).collect();
        assert!(many.len() > 2 * BATCH_SWEEP_CHUNK);
        assert_eq!(batch_gets_key(&idx, &many), per_row_gets_key(&idx, &many));
    }

    #[test]
    fn key_index_basic() {
        let mut map: HashMap<Box<[u8]>, i64> = HashMap::new();
        let key1: Box<[u8]> = vec![1, 2, 3].into_boxed_slice();
        let key2: Box<[u8]> = vec![4, 5, 6].into_boxed_slice();
        map.insert(key1.clone(), 1);
        map.insert(key2.clone(), 2);

        let idx = KeyDeletionIndex::from_map(map);
        assert_eq!(idx.max_sequence_number(), Some(2));
        assert_eq!(key_delete_seq_of(&idx, &key1), Some(1));
        assert_eq!(key_delete_seq_of(&idx, &key2), Some(2));
        assert_eq!(idx.get(&[7, 8, 9]), None);
    }

    #[test]
    fn key_index_extend_max_deletes() {
        let key1: Box<[u8]> = vec![1, 2].into_boxed_slice();
        let mut map: HashMap<Box<[u8]>, i64> = HashMap::new();
        map.insert(key1.clone(), 5);
        let idx = KeyDeletionIndex::from_map(map);

        let next = idx.extend_max_deletes([(key1.clone(), 3)]);
        assert_eq!(next.max_sequence_number(), Some(5));
        assert_eq!(key_delete_seq_of(&next, &key1), Some(5));

        let after = next.extend_max_deletes([(key1.clone(), 10)]);
        assert_eq!(after.max_sequence_number(), Some(10));
        assert_eq!(key_delete_seq_of(&after, &key1), Some(10));
    }

    #[test]
    fn key_index_conflicts_and_insert_only_fusion() {
        let idx = KeyDeletionIndex::empty();
        let next = idx.extend_max_conflicts([byte_key(1), byte_key(2)], 10, 11);
        assert_eq!(next.delete_len(), 2);
        assert_eq!(next.insert_len(), 2);
        let tombstone = next.get(&byte_key(1)).expect("tombstone");
        assert_eq!(tombstone.delete_sequence, 10);
        assert_eq!(tombstone.insert_sequence, Some(11));

        // Insert-only entries from the catalog load path probe as absent until deleted.
        let loaded =
            KeyDeletionIndex::from_maps(HashMap::new(), HashMap::from([(byte_key(9), 90_i64)]));
        assert_eq!(loaded.get(&byte_key(9)), None);
        assert!(!loaded.has_deletions());
        let deleted = loaded.extend_max_deletes([(byte_key(9), 95)]);
        let tombstone = deleted.get(&byte_key(9)).expect("tombstone");
        assert_eq!(tombstone.delete_sequence, 95);
        assert_eq!(tombstone.insert_sequence, Some(90));
    }

    #[test]
    fn key_index_pinned_generation_is_unaffected_by_extends() {
        let mut map: HashMap<Box<[u8]>, i64> = HashMap::new();
        for value in 0_u16..512 {
            map.insert(
                value.to_be_bytes().to_vec().into_boxed_slice(),
                i64::from(value),
            );
        }
        let idx = KeyDeletionIndex::from_map(map);

        let pinned_reader_generation = idx.clone();
        let next =
            idx.extend_max_deletes([(999_u16.to_be_bytes().to_vec().into_boxed_slice(), 999)]);

        // The pinned generation never sees the new key's entry; the new
        // generation sees both old and new entries.
        assert_eq!(
            pinned_reader_generation.get(&999_u16.to_be_bytes()),
            None,
            "pinned generation must not observe later extends"
        );
        assert_eq!(
            key_delete_seq_of(&pinned_reader_generation, &0_u16.to_be_bytes()),
            Some(0)
        );
        assert_eq!(key_delete_seq_of(&next, &0_u16.to_be_bytes()), Some(0));
        assert_eq!(key_delete_seq_of(&next, &999_u16.to_be_bytes()), Some(999));
    }

    #[test]
    fn key_index_identity_is_content_based_not_allocation_based() {
        // Hash-keyed identity: equal key bytes from different allocations
        // resolve to the same entry.
        let key: Box<[u8]> = vec![9, 9].into_boxed_slice();
        let mut map: HashMap<Box<[u8]>, i64> = HashMap::new();
        map.insert(key, 1);
        let idx = KeyDeletionIndex::from_map(map);

        let replacement_key: Box<[u8]> = vec![9, 9].into_boxed_slice();
        let next = idx.extend_max_deletes([(replacement_key, 5)]);

        assert_eq!(next.len(), 1, "equal bytes must merge into one entry");
        assert_eq!(key_delete_seq_of(&idx, &[9, 9]), Some(1));
        assert_eq!(key_delete_seq_of(&next, &[9, 9]), Some(5));
    }

    // -------------------------------------------------------------------------
    // Regression tests for the incremental bloom-update path.
    //
    // A previous revision rebuilt the bloom filter from scratch on every
    // extend call (iterating ALL entries and re-hashing them). On high-rate
    // upsert/delete workloads this turned every per-row cache update into
    // O(N) work, where N is the cumulative deletion-cache size. The
    // cumulative effect across M writes is O(M*N), which is the root cause
    // of the ingestion regression the user reported (~200% on upsert-heavy
    // workloads with growing deletion sets).
    //
    // The fix rebuilds the bloom only when the deletion count outgrows the
    // sized capacity (which carries 2x headroom, so the cadence is geometric
    // / amortized O(K)) and inserts into the shared filter in place in
    // between. These tests exercise both code paths and verify correctness
    // across many extend cycles.
    // -------------------------------------------------------------------------

    #[test]
    fn extend_many_small_batches_preserves_all_entries() {
        // Simulates many small upserts each adding a single new PK to the
        // cache — the exact pattern that exposed the O(N²) regression. With
        // the test-sized DELTA_MERGE_MIN this also crosses several base
        // merges, covering the layered path end to end.
        let mut idx = DeletionIndex::empty();
        let n = 1024;
        for pk in 0_i64..n {
            idx = idx.extend_max_deletes([(pk, pk + 1)]);
        }
        assert_eq!(i64::try_from(idx.len()).expect("len fits in i64"), n);
        for pk in 0_i64..n {
            assert_eq!(
                delete_seq_of(&idx, pk),
                Some(pk + 1),
                "missing entry for pk={pk} after {n} incremental extends",
            );
        }
        // A key never inserted must not be reported as present.
        assert_eq!(idx.get(n + 100), None);
    }

    #[test]
    fn extend_rebuilds_bloom_with_headroom() {
        // Verify the bloom is rebuilt when the deletion count crosses the
        // sized capacity, and that the new filter takes 2x headroom so it
        // never runs past its design FPR between rebuilds (geometric
        // amortization).
        let mut idx = DeletionIndex::empty();
        assert_eq!(idx.core.bloom_capacity, MIN_BLOOM_CAPACITY);

        // 64 deletions fit the minimum capacity exactly: no rebuild.
        for pk in 0..64 {
            idx = idx.extend_max_deletes([(pk, 1)]);
        }
        assert_eq!(idx.len(), 64);
        assert_eq!(
            idx.core.bloom_capacity, MIN_BLOOM_CAPACITY,
            "no rebuild expected before exceeding the sized capacity"
        );

        // The 65th deletion crosses the sized capacity: rebuild with headroom.
        idx = idx.extend_max_deletes([(64, 1)]);
        assert_eq!(idx.len(), 65);
        assert_eq!(
            idx.core.bloom_capacity, 130,
            "rebuild must size the new bloom for 2x the deletion count"
        );

        // Keep growing to force another rebuild cycle at the next boundary.
        for pk in 65..200 {
            idx = idx.extend_max_deletes([(pk, 1)]);
        }
        assert!(
            idx.core.bloom_capacity >= 200,
            "bloom_capacity must stay ahead of {} deletions, got {}",
            idx.delete_len(),
            idx.core.bloom_capacity,
        );

        // Every inserted key probes positive.
        for pk in 0..200 {
            assert_eq!(
                delete_seq_of(&idx, pk),
                Some(1),
                "missing pk={pk} after rebuilds"
            );
        }
    }

    #[test]
    fn bloom_rebuild_sizes_by_deletion_count_not_total_entries() {
        // Insert-only entries must not count against the bloom capacity: the
        // filter is keyed on deletion membership.
        let inserts: HashMap<i64, i64> = (0..1000).map(|pk| (pk, 1)).collect();
        let idx = DeletionIndex::from_maps(HashMap::new(), inserts);
        assert_eq!(idx.core.bloom_capacity, MIN_BLOOM_CAPACITY);

        // 64 deletions still fit the minimum capacity despite 1000 entries.
        let mut grown = idx;
        for pk in 0..64 {
            grown = grown.extend_max_deletes([(pk, 2)]);
        }
        assert_eq!(grown.core.bloom_capacity, MIN_BLOOM_CAPACITY);
        assert_eq!(grown.delete_len(), 64);
        assert_eq!(grown.len(), 1000);
    }

    #[test]
    fn extend_preserves_max_sequence_under_repeated_updates() {
        // Same PK updated many times — every extend should preserve the max
        // sequence seen so far. Tests the occupied-entry path.
        let mut idx = DeletionIndex::empty();
        idx = idx.extend_max_deletes([(42, 100)]);
        idx = idx.extend_max_deletes([(42, 50)]); // older write, should not override
        idx = idx.extend_max_deletes([(42, 200)]); // newer write, takes max
        idx = idx.extend_max_deletes([(42, 150)]); // older write, should not override
        assert_eq!(delete_seq_of(&idx, 42), Some(200));
        assert_eq!(idx.len(), 1, "no new entry should have been added");
    }

    #[test]
    fn key_index_extend_many_small_batches_preserves_all_entries() {
        // Same regression case for byte-keyed (composite-PK) tables.
        let mut idx = KeyDeletionIndex::empty();
        let n = 256_usize;
        for i in 0..n {
            let key = byte_key(i as u64);
            idx = idx.extend_max_deletes([(key, i64::try_from(i).expect("i fits in i64") + 1)]);
        }
        assert_eq!(idx.len(), n);
        for i in 0..n {
            let key = byte_key(i as u64);
            assert_eq!(
                key_delete_seq_of(&idx, &key),
                Some(i64::try_from(i).expect("i fits in i64") + 1),
                "missing entry for key i={i} after {n} incremental extends",
            );
        }
    }

    #[test]
    fn extend_batch_only_pays_for_new_keys() {
        // When all additions are duplicates (already present), no new bloom
        // inserts should happen — verified indirectly by checking the
        // bloom_capacity is unchanged and queries still work.
        let mut map = HashMap::new();
        for pk in 0..32 {
            map.insert(pk, 1_i64);
        }
        let idx = DeletionIndex::from_map(map);
        let initial_cap = idx.core.bloom_capacity;

        // Extend with all-duplicate keys (different seq, but occupied path).
        let next = idx.extend_max_deletes((0..32).map(|pk| (pk, 2_i64)));
        assert_eq!(next.core.bloom_capacity, initial_cap);
        assert_eq!(next.len(), 32);
        for pk in 0..32 {
            assert_eq!(
                delete_seq_of(&next, pk),
                Some(2),
                "max-sequence update lost for pk={pk}"
            );
        }
    }

    #[test]
    fn extend_shares_bloom_until_rebuild() {
        // The common path must share the parent's filter (no per-call copy of
        // the bit array); a rebuild must allocate a fresh one.
        let idx = DeletionIndex::from_map((0..32).map(|pk| (pk, 1_i64)).collect());

        let extended = idx.extend_max_deletes([(100, 2)]);
        assert!(
            Arc::ptr_eq(&idx.core.bloom, &extended.core.bloom),
            "in-capacity extend must share the parent's bloom filter"
        );

        // Old generation sees a superset: the new key's bits are visible, but
        // a map probe correctly reports it absent from the old generation.
        assert!(idx.might_contain(100));
        assert_eq!(idx.get(100), None);
        assert_eq!(delete_seq_of(&extended, 100), Some(2));

        // Grow past the sized capacity (64): the rebuild must not alias the
        // shared filter.
        let mut grown = extended;
        for pk in 200..280 {
            grown = grown.extend_max_deletes([(pk, 3)]);
        }
        assert!(
            !Arc::ptr_eq(&idx.core.bloom, &grown.core.bloom),
            "rebuild must allocate a fresh filter"
        );
        assert_eq!(delete_seq_of(&grown, 100), Some(2));
        assert_eq!(delete_seq_of(&grown, 250), Some(3));
    }

    // -------------------------------------------------------------------------
    // Layered-storage tests (frozen base + delta). DELTA_MERGE_MIN is 64 under
    // cfg(test), so ordinary insert counts cross merge boundaries.
    // -------------------------------------------------------------------------

    #[test]
    fn delta_merges_into_base_at_threshold() {
        // Single-key extends accumulate in the delta until the threshold, then
        // fold into the frozen base.
        let mut idx = DeletionIndex::empty();
        for pk in 0..63_i64 {
            idx = idx.extend_max_deletes([(pk, 1)]);
        }
        assert_eq!(idx.core.delta.len(), 63, "delta below threshold");
        assert_eq!(idx.core.base.len(), 0);

        idx = idx.extend_max_deletes([(63, 1)]);
        assert_eq!(idx.core.delta.len(), 0, "delta folded into base");
        assert_eq!(idx.core.base.len(), 64);

        // Everything probeable through both tiers' lifetimes.
        for pk in 0..64 {
            assert_eq!(delete_seq_of(&idx, pk), Some(1), "missing pk={pk}");
        }
        assert_eq!(idx.len(), 64);
    }

    #[test]
    fn base_key_update_lands_in_delta_and_wins() {
        // Force keys into the frozen base, then update one: the merged entry
        // lives in the delta and overrides the base on probes, without
        // changing entry counts.
        let idx = DeletionIndex::from_map((0..100).map(|pk| (pk, 10_i64)).collect());
        assert_eq!(idx.core.base.len(), 100);
        assert_eq!(idx.core.delta.len(), 0);

        let updated = idx.extend_max_conflicts([5], 20, 21);
        assert_eq!(updated.core.base.len(), 100, "base stays frozen");
        assert_eq!(updated.core.delta.len(), 1, "update lands in delta");
        assert_eq!(updated.len(), 100, "no new entry for an existing key");
        assert_eq!(updated.delete_len(), 100);
        assert_eq!(updated.insert_len(), 1);

        let tombstone = updated.get(5).expect("tombstone");
        assert_eq!(tombstone.delete_sequence, 20, "delta overrides base");
        assert_eq!(tombstone.insert_sequence, Some(21));

        // A stale update is a no-op and must not grow the delta.
        let stale = updated.extend_max_deletes([(5, 15)]);
        assert_eq!(stale.core.delta.len(), 1, "no-op update must not re-insert");
        assert_eq!(stale.get(5).expect("tombstone").delete_sequence, 20);
    }

    #[test]
    fn pinned_generation_survives_base_merge() {
        // A reader pinning a pre-merge generation must keep its exact view
        // while the writer merges the delta into a new base.
        let mut idx = DeletionIndex::empty();
        for pk in 0..50_i64 {
            idx = idx.extend_max_deletes([(pk, 1)]);
        }
        let pinned = idx.clone();
        let pinned_base = Arc::clone(&pinned.core.base);

        // Cross the merge threshold.
        for pk in 50..130_i64 {
            idx = idx.extend_max_deletes([(pk, 2)]);
        }
        assert!(
            !Arc::ptr_eq(&pinned_base, &idx.core.base),
            "merge must produce a fresh base"
        );

        // Pinned view: first 50 keys only.
        for pk in 0..50 {
            assert_eq!(delete_seq_of(&pinned, pk), Some(1));
        }
        assert_eq!(pinned.get(75), None, "pinned view must not see later keys");
        assert_eq!(pinned.len(), 50);

        // New view: everything.
        for pk in 0..50 {
            assert_eq!(delete_seq_of(&idx, pk), Some(1));
        }
        for pk in 50..130 {
            assert_eq!(delete_seq_of(&idx, pk), Some(2));
        }
        assert_eq!(idx.len(), 130);
    }

    #[test]
    fn counters_consistent_across_merges() {
        // Interleave fresh keys, repeat updates, and conflicts across several
        // merge boundaries; counters must match a straightforward model.
        let mut idx = DeletionIndex::empty();
        let mut model: HashMap<i64, (i64, Option<i64>)> = HashMap::new();

        for round in 0..6_i64 {
            for slot in 0..40_i64 {
                let pk = round * 30 + slot; // overlapping ranges → repeat keys
                let delete_seq = round + 1;
                let insert_seq = round + 2;
                idx = idx.extend_max_conflicts([pk], delete_seq, insert_seq);
                let entry = model.entry(pk).or_insert((SEQUENCE_ABSENT, None));
                entry.0 = entry.0.max(delete_seq);
                entry.1 = Some(
                    entry
                        .1
                        .map_or(insert_seq, |existing: i64| existing.max(insert_seq)),
                );
            }
        }

        assert_eq!(idx.len(), model.len());
        assert_eq!(idx.delete_len(), model.len());
        assert_eq!(idx.insert_len(), model.len());
        for (pk, (delete_seq, insert_seq)) in &model {
            let tombstone = idx.get(*pk).expect("tombstone");
            assert_eq!(tombstone.delete_sequence, *delete_seq, "pk={pk}");
            assert_eq!(tombstone.insert_sequence, *insert_seq, "pk={pk}");
        }
    }

    #[test]
    fn iter_entries_yields_latest_values_without_duplicates() {
        // Keys split across base (merged) and delta (recent update of a base
        // key + a brand-new key): iteration must yield each key once with its
        // newest value.
        let idx = DeletionIndex::from_map((0..100).map(|pk| (pk, 1_i64)).collect());
        let idx = idx.extend_max_deletes([(5, 50), (200, 60)]);
        assert!(idx.core.delta.len() >= 2, "updates must be delta-resident");

        let collected: HashMap<i64, TombstoneEntry> = idx.iter_entries().collect();
        assert_eq!(collected.len(), 101);
        assert_eq!(
            idx.iter_entries().count(),
            101,
            "no duplicate keys in iteration"
        );
        assert_eq!(collected[&5].delete_sequence(), Some(50), "delta wins");
        assert_eq!(collected[&200].delete_sequence(), Some(60));
        assert_eq!(collected[&6].delete_sequence(), Some(1), "base preserved");
    }
}
