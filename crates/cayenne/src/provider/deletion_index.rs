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
//! it re-inserted after that?" with a **single** map probe. (These were previously
//! two separate indexes, costing two bloom checks and two HAMT walks per deleted
//! row — the dominant per-row cost in changes-mode profiles.)
//!
//! The index holds a persistent [`im::HashMap`] plus a [`SplitBlockBloomFilter`]
//! keyed on *deletion* membership (a single cache line per probe): a probe goes
//! through the bloom filter first, and falls through to the hash map only on a
//! possible hit. Entries holding only an insert record (which occur after
//! compaction purges delete files while insert records remain in the catalog) are
//! not represented in the bloom and are reported as absent by [`get`] — exactly
//! the visibility semantics of the previous two-index design.
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
    PrehashedBuildHasher, SplitBlockBloomFilter, XxHash3BuildHasher, hash_key, hash_key_128,
};
use im::HashMap as PersistentHashMap;
use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

/// Bloom filter capacity floor: keep some signal even for empty / tiny sets so that the
/// "probably-not-present" path stays useful when a fresh index is constructed.
const MIN_BLOOM_CAPACITY: usize = 64;

/// Hasher for the persistent HAMT. `im::HashMap` defaults to `RandomState`
/// (SipHash-1-3), which showed up as `im::nodes::hamt::hash_key` in executor
/// profiles of changes-mode ingest — every probe was paying a SipHash walk on
/// top of the seeded-XXH3 bloom hash. [`XxHash3BuildHasher`] uses the same
/// seeded XXH3-64 as [`hash_key`], so map and bloom hashing now agree at a
/// fraction of SipHash's per-key cost.
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

/// Sentinel for "no sequence recorded" inside [`TombstoneEntry`]. Catalog
/// sequence numbers are non-negative, so `i64::MIN` can never collide with a
/// real sequence; packing both fields as raw `i64` keeps the per-entry payload
/// at 16 bytes (an `Option<i64>` pair would double it — at tens of millions of
/// entries that is hundreds of MiB).
const SEQUENCE_ABSENT: i64 = i64::MIN;

/// Raw fused map value: the highest delete and insert sequence numbers recorded
/// for one primary key, with [`SEQUENCE_ABSENT`] marking an unset side. Exposed
/// (read-only) so callers iterating [`entries`](DeletionIndex::entries) can
/// project the side they need.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TombstoneEntry {
    delete_seq: i64,
    insert_seq: i64,
}

impl TombstoneEntry {
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

/// Outcome of merging one addition into the fused map: which sides newly
/// transitioned from absent to present. Drives the bloom update (delete side
/// only) and the `delete_len`/`insert_len` counters.
#[derive(Clone, Copy, Default)]
struct MergeTransitions {
    new_delete: bool,
    new_insert: bool,
}

/// Merge `delete_seq`/`insert_seq` (either possibly [`SEQUENCE_ABSENT`]) into
/// `entry`, taking the per-side max, and report absent→present transitions.
#[inline]
fn merge_max(entry: &mut TombstoneEntry, delete_seq: i64, insert_seq: i64) -> MergeTransitions {
    let mut transitions = MergeTransitions::default();
    if delete_seq != SEQUENCE_ABSENT {
        if entry.delete_seq == SEQUENCE_ABSENT {
            transitions.new_delete = true;
            entry.delete_seq = delete_seq;
        } else if delete_seq > entry.delete_seq {
            entry.delete_seq = delete_seq;
        }
    }
    if insert_seq != SEQUENCE_ABSENT {
        if entry.insert_seq == SEQUENCE_ABSENT {
            transitions.new_insert = true;
            entry.insert_seq = insert_seq;
        } else if insert_seq > entry.insert_seq {
            entry.insert_seq = insert_seq;
        }
    }
    transitions
}

/// Frozen deletion index for tables with a single-column Int64 primary key.
///
/// Holds the fused (pk → [`TombstoneEntry`]) map and an accompanying bloom filter
/// keyed on deletion membership. The bloom filter's bit array is sized for
/// `bloom_capacity` deletion keys; the writer tracks that capacity so the extend
/// methods can update the shared bloom in place for the common case where the
/// index grows slowly, only paying a full O(N) rebuild when the deletion count
/// outgrows the sized capacity. This keeps amortized writer cost at O(K) per
/// call (K = number of additions) instead of the O(N) it would otherwise be — see
/// [`extend_max_deletes`](Self::extend_max_deletes) for the full argument.
#[derive(Debug, Clone)]
pub struct DeletionIndex {
    entries: PersistentHashMap<i64, TombstoneEntry, DeletionIndexHasher>,
    bloom: Arc<SplitBlockBloomFilter>,
    /// Monotonic upper bound over the **delete** sequences in the current
    /// immutable entries. This stays exact because indexes are build-once /
    /// extend-only; any future removal API must recompute it instead of
    /// carrying a stale high-water mark.
    /// `CayenneTableProvider::apply_partial_deletion_filter` relies on this
    /// exact value to decide whether a protected snapshot can skip deletion
    /// filtering without letting deleted rows through.
    max_sequence_number: Option<i64>,
    /// Number of entries with a recorded deletion (= bloom population).
    delete_count: usize,
    /// Number of entries with a recorded insert (upsert re-insertion).
    insert_count: usize,
    /// Deletion-key count the current `bloom` was sized for
    /// ([`bloom_capacity_for`]: 2x the deletion count at build time). When
    /// `delete_count` exceeds it, the extend methods rebuild the bloom from
    /// scratch into a fresh `Arc`; otherwise they insert newly-deleted keys
    /// into the shared filter in place.
    bloom_capacity: usize,
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
            entries: PersistentHashMap::default(),
            bloom: Arc::new(SplitBlockBloomFilter::new(MIN_BLOOM_CAPACITY)),
            max_sequence_number: None,
            delete_count: 0,
            insert_count: 0,
            bloom_capacity: MIN_BLOOM_CAPACITY,
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
        let capacity = bloom_capacity_for(deleted.len());
        let bloom = SplitBlockBloomFilter::new(capacity);
        for &pk in deleted.keys() {
            bloom.insert(hash_key(&pk));
        }
        let max_sequence_number = deleted.values().copied().max();
        let delete_count = deleted.len();
        let insert_count = insert_records.len();

        let mut entries: PersistentHashMap<i64, TombstoneEntry, DeletionIndexHasher> =
            deleted
                .into_iter()
                .map(|(pk, delete_seq)| {
                    (
                        pk,
                        TombstoneEntry {
                            delete_seq,
                            insert_seq: SEQUENCE_ABSENT,
                        },
                    )
                })
                .collect();
        for (pk, insert_seq) in insert_records {
            merge_max(
                entries.entry(pk).or_insert(TombstoneEntry {
                    delete_seq: SEQUENCE_ABSENT,
                    insert_seq: SEQUENCE_ABSENT,
                }),
                SEQUENCE_ABSENT,
                insert_seq,
            );
        }

        Self {
            entries,
            bloom: Arc::new(bloom),
            max_sequence_number,
            delete_count,
            insert_count,
            bloom_capacity: capacity,
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
        self.entries.len()
    }

    /// Whether the index has no entries at all.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Number of keys with a recorded deletion.
    #[must_use]
    pub fn delete_len(&self) -> usize {
        self.delete_count
    }

    /// Number of keys with a recorded insert (upsert re-insertion).
    #[must_use]
    pub fn insert_len(&self) -> usize {
        self.insert_count
    }

    /// Whether any key has a recorded deletion. Scan fast paths skip the
    /// filter entirely when this is `false` (insert-only entries never affect
    /// visibility).
    #[must_use]
    pub fn has_deletions(&self) -> bool {
        self.delete_count > 0
    }

    /// Approximate resident bytes for memory accounting: each `i64 ->
    /// (i64, i64)` entry includes the key/value payload plus HAMT
    /// node/bitmap/Arc overhead. Shared nodes retained by older reader-pinned
    /// generations are intentionally not charged to the latest snapshot again.
    #[must_use]
    pub fn approx_bytes(&self) -> usize {
        const APPROX_I64_ENTRY_BYTES: usize = 80;
        self.entries.len().saturating_mul(APPROX_I64_ENTRY_BYTES)
    }

    /// Highest **delete** sequence number in this index, if any.
    #[must_use]
    pub fn max_sequence_number(&self) -> Option<i64> {
        self.max_sequence_number
    }

    /// Bloom-filter check against deletion membership. Returns `false` if the key
    /// definitely has no recorded deletion; `true` if it might (and a
    /// [`get`](Self::get) is required to confirm).
    #[inline]
    #[must_use]
    pub fn might_contain(&self, pk: i64) -> bool {
        self.bloom.might_contain(hash_key(&pk))
    }

    /// Bloom-prefiltered lookup. Returns the key's [`Tombstone`] if `pk` has a
    /// recorded deletion, `None` otherwise.
    ///
    /// Keys holding only an insert record are reported as `None`: the bloom is
    /// keyed on deletion membership, and visibility treats "no deletion" and
    /// "insert-only" identically. One probe answers both the deletion and the
    /// re-insertion question — previously two separate index walks.
    #[inline]
    #[must_use]
    pub fn get(&self, pk: i64) -> Option<Tombstone> {
        if !self.bloom.might_contain(hash_key(&pk)) {
            return None;
        }
        self.entries.get(&pk).and_then(Tombstone::from_entry)
    }

    /// Direct read-only access to the underlying fused entries (for callers
    /// that need to iterate, e.g. benches; project sides via
    /// [`TombstoneEntry::delete_sequence`] / [`TombstoneEntry::insert_sequence`]).
    #[must_use]
    pub fn entries(&self) -> &PersistentHashMap<i64, TombstoneEntry, DeletionIndexHasher> {
        &self.entries
    }

    /// Build a new index from `self`'s entries plus delete-only `additions`
    /// (`pk -> delete_sequence`), taking the per-key max sequence on conflict.
    /// Used by writers to publish a new snapshot via `ArcSwap::store`.
    ///
    /// # Performance
    ///
    /// `entries` is a persistent HAMT, so cloning the current map for a write
    /// shares unchanged nodes with any reader-pinned generation. Inserts update
    /// only the path to the touched key (O(log N)) instead of cloning every
    /// entry. The bloom filter is *shared* with the parent index behind an
    /// `Arc`: keys gaining their first deletion are inserted into it in place
    /// (relaxed atomic stores, O(K), no copy of the bit array — at millions of
    /// entries a per-call copy would be a multi-megabyte memcpy on every CDC
    /// batch). A full O(N) rebuild into a fresh filter only happens when the
    /// deletion count outgrows the capacity the filter was sized for — 2x the
    /// deletion count at build time ([`bloom_capacity_for`]) — so the rebuild
    /// cadence is geometric and the amortized bloom cost stays O(K) per call.
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
        self.extend_with(additions.into_iter().map(|(pk, delete_seq)| {
            (pk, delete_seq, SEQUENCE_ABSENT)
        }))
    }

    /// Build a new index recording an upsert-conflict batch: every key in
    /// `keys` gains a deletion at `delete_sequence` **and** a re-insertion at
    /// `insert_sequence`, each merged with per-key max. One pass over the map
    /// replaces the previous two-index double `extend_max`.
    #[must_use]
    pub fn extend_max_conflicts(
        &self,
        keys: impl IntoIterator<Item = i64>,
        delete_sequence: i64,
        insert_sequence: i64,
    ) -> Self {
        self.extend_with(
            keys.into_iter()
                .map(|pk| (pk, delete_sequence, insert_sequence)),
        )
    }

    /// Shared merge core for the extend methods. `delete_seq`/`insert_seq` use
    /// [`SEQUENCE_ABSENT`] for "leave this side unchanged".
    fn extend_with(&self, additions: impl Iterator<Item = (i64, i64, i64)>) -> Self {
        let mut entries = self.entries.clone();
        let mut max_sequence_number = self.max_sequence_number;
        let mut delete_count = self.delete_count;
        let mut insert_count = self.insert_count;
        // Track keys gaining their first deletion so the bloom can be updated
        // incrementally without re-iterating the entire entry set. Pre-size
        // from the iterator's hint to skip Vec growth reallocations.
        let mut new_delete_keys: Vec<i64> = Vec::with_capacity(additions.size_hint().0);
        for (pk, delete_seq, insert_seq) in additions {
            debug_assert_ne!(delete_seq, SEQUENCE_ABSENT, "real sequences are non-negative");
            let entry = entries.entry(pk).or_insert(TombstoneEntry {
                delete_seq: SEQUENCE_ABSENT,
                insert_seq: SEQUENCE_ABSENT,
            });
            let transitions = merge_max(entry, delete_seq, insert_seq);
            let stored_delete = entry.delete_seq;
            if transitions.new_delete {
                delete_count += 1;
                new_delete_keys.push(pk);
            }
            if transitions.new_insert {
                insert_count += 1;
            }
            if stored_delete != SEQUENCE_ABSENT
                && max_sequence_number.is_none_or(|max| stored_delete > max)
            {
                max_sequence_number = Some(stored_delete);
            }
        }

        // `max_sequence_number` is maintained incrementally above; we do not
        // re-scan `entries` here (a full scan would make extends O(N) in debug
        // builds and noticeably slow the test suite as the index grows).
        // `from_maps` is the single rebuild path and recomputes the exact max
        // from scratch.
        // Rebuild from scratch when deletion growth has outpaced the sized
        // capacity. The new filter takes 2x headroom, so the rebuild cadence
        // is geometric: between rebuilds we pay O(K) for in-place inserts; on
        // a rebuild we pay O(N), but the next rebuild is another doubling
        // away, so the total work across one doubling cycle amortizes to O(N).
        if delete_count > self.bloom_capacity {
            let new_capacity = bloom_capacity_for(delete_count);
            let bloom = SplitBlockBloomFilter::new(new_capacity);
            for (pk, entry) in &entries {
                if entry.delete_seq != SEQUENCE_ABSENT {
                    bloom.insert(hash_key(pk));
                }
            }
            return Self {
                entries,
                bloom: Arc::new(bloom),
                max_sequence_number,
                delete_count,
                insert_count,
                bloom_capacity: new_capacity,
            };
        }

        // Common path: insert only the newly-deleted keys into the shared
        // filter (O(K) relaxed atomic stores; see the safety argument above).
        for pk in &new_delete_keys {
            self.bloom.insert(hash_key(pk));
        }
        Self {
            entries,
            bloom: Arc::clone(&self.bloom),
            max_sequence_number,
            delete_count,
            insert_count,
            bloom_capacity: self.bloom_capacity,
        }
    }
}

/// Splits a 128-bit key hash into its map identity and the (independent)
/// 64 bits fed to the bloom filter. Using disjoint halves keeps the bloom's
/// block/bit selection uncorrelated with the map's bucket selection (the map
/// consumes the low half via [`PrehashedBuildHasher`]).
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
/// from probes, and (via [`PrehashedBuildHasher`]) lets the map reuse the
/// hash's own entropy instead of re-hashing per probe — one hash computation
/// serves the bloom filter and the map together.
///
/// Two distinct keys colliding under XXH3-128 would share a tombstone; at one
/// billion keys the birthday bound puts that below ~1e-20 — orders of
/// magnitude under hardware error rates. (A 64-bit hash would NOT be safe as
/// identity at these cardinalities: ~0.3% collision odds at the same scale.)
///
/// See [`DeletionIndex`] for the fused-entry, bloom-capacity, and shared-filter
/// contracts.
#[derive(Debug, Clone)]
pub struct KeyDeletionIndex {
    entries: PersistentHashMap<u128, TombstoneEntry, PrehashedBuildHasher>,
    bloom: Arc<SplitBlockBloomFilter>,
    /// Monotonic upper bound over the **delete** sequences in the current
    /// immutable entries. See [`DeletionIndex::max_sequence_number`].
    max_sequence_number: Option<i64>,
    /// Number of entries with a recorded deletion (= bloom population).
    delete_count: usize,
    /// Number of entries with a recorded insert (upsert re-insertion).
    insert_count: usize,
    /// Deletion-key count the current `bloom` was sized for. Mirrors
    /// [`DeletionIndex::bloom_capacity`] to amortize bloom rebuilds.
    bloom_capacity: usize,
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
            entries: PersistentHashMap::default(),
            bloom: Arc::new(SplitBlockBloomFilter::new(MIN_BLOOM_CAPACITY)),
            max_sequence_number: None,
            delete_count: 0,
            insert_count: 0,
            bloom_capacity: MIN_BLOOM_CAPACITY,
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
        let capacity = bloom_capacity_for(deleted.len());
        let bloom = SplitBlockBloomFilter::new(capacity);
        let max_sequence_number = deleted.values().copied().max();
        let delete_count = deleted.len();
        let insert_count = insert_records.len();

        let mut entries: PersistentHashMap<u128, TombstoneEntry, PrehashedBuildHasher> =
            deleted
                .into_iter()
                .map(|(key, delete_seq)| {
                    let key_hash = hash_key_128(&key);
                    bloom.insert(bloom_half(key_hash));
                    (
                        key_hash,
                        TombstoneEntry {
                            delete_seq,
                            insert_seq: SEQUENCE_ABSENT,
                        },
                    )
                })
                .collect();
        for (key, insert_seq) in insert_records {
            let key_hash = hash_key_128(&key);
            merge_max(
                entries.entry(key_hash).or_insert(TombstoneEntry {
                    delete_seq: SEQUENCE_ABSENT,
                    insert_seq: SEQUENCE_ABSENT,
                }),
                SEQUENCE_ABSENT,
                insert_seq,
            );
        }

        Self {
            entries,
            bloom: Arc::new(bloom),
            max_sequence_number,
            delete_count,
            insert_count,
            bloom_capacity: capacity,
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
        self.entries.len()
    }

    /// Number of keys with a recorded deletion.
    #[must_use]
    pub fn delete_len(&self) -> usize {
        self.delete_count
    }

    /// Number of keys with a recorded insert (upsert re-insertion).
    #[must_use]
    pub fn insert_len(&self) -> usize {
        self.insert_count
    }

    /// Whether any key has a recorded deletion. See
    /// [`DeletionIndex::has_deletions`].
    #[must_use]
    pub fn has_deletions(&self) -> bool {
        self.delete_count > 0
    }

    /// Approximate resident bytes for memory accounting: each `u128 ->
    /// (i64, i64)` entry is a fixed 32-byte payload plus HAMT node/bitmap/Arc
    /// overhead — key bytes are not retained (hash-keyed identity), so the
    /// estimate no longer depends on composite-key width. Shared nodes
    /// retained by older reader-pinned generations are intentionally not
    /// charged to the latest snapshot again.
    #[must_use]
    pub fn approx_bytes(&self) -> usize {
        const APPROX_KEY_ENTRY_BYTES: usize = 88;
        self.entries.len().saturating_mul(APPROX_KEY_ENTRY_BYTES)
    }

    /// Whether the index has no entries at all.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Highest **delete** sequence number in this index, if any.
    #[must_use]
    pub fn max_sequence_number(&self) -> Option<i64> {
        self.max_sequence_number
    }

    /// Bloom-filter check against deletion membership; see
    /// [`DeletionIndex::might_contain`].
    #[inline]
    #[must_use]
    pub fn might_contain(&self, key: &[u8]) -> bool {
        self.bloom.might_contain(bloom_half(hash_key_128(key)))
    }

    /// Bloom-prefiltered lookup. Returns the key's [`Tombstone`] if it has a
    /// recorded deletion, `None` otherwise. See [`DeletionIndex::get`] for the
    /// insert-only-entry contract.
    ///
    /// One XXH3-128 computation serves both the bloom check (high half) and
    /// the map probe (full hash as identity, low half as bucket entropy via
    /// [`PrehashedBuildHasher`]) — the key bytes are never re-hashed or
    /// compared.
    #[inline]
    #[must_use]
    pub fn get(&self, key: &[u8]) -> Option<Tombstone> {
        let key_hash = hash_key_128(key);
        if !self.bloom.might_contain(bloom_half(key_hash)) {
            return None;
        }
        self.entries.get(&key_hash).and_then(Tombstone::from_entry)
    }

    /// Direct read-only access to the underlying fused entries, keyed by the
    /// XXH3-128 hash of the original key bytes.
    #[must_use]
    pub fn entries(&self) -> &PersistentHashMap<u128, TombstoneEntry, PrehashedBuildHasher> {
        &self.entries
    }

    /// Build a new index from `self`'s entries plus delete-only `additions`,
    /// taking the per-key max sequence on conflict. Keys are borrowed — the
    /// index stores their XXH3-128 hash, never the bytes.
    ///
    /// See [`DeletionIndex::extend_max_deletes`] for the amortization and
    /// shared-filter safety argument. Bloom rebuilds only happen when the
    /// deletion count outgrows the sized capacity; otherwise only the
    /// newly-deleted keys are inserted into the shared filter in place.
    #[must_use]
    pub fn extend_max_deletes<K: AsRef<[u8]>>(
        &self,
        additions: impl IntoIterator<Item = (K, i64)>,
    ) -> Self {
        self.extend_with(additions.into_iter().map(|(key, delete_seq)| {
            (hash_key_128(key.as_ref()), delete_seq, SEQUENCE_ABSENT)
        }))
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
        self.extend_with(keys.into_iter().map(|key| {
            (
                hash_key_128(key.as_ref()),
                delete_sequence,
                insert_sequence,
            )
        }))
    }

    /// Shared merge core for the extend methods, operating on pre-hashed key
    /// identities. `insert_seq` uses [`SEQUENCE_ABSENT`] for "leave this side
    /// unchanged".
    fn extend_with(&self, additions: impl Iterator<Item = (u128, i64, i64)>) -> Self {
        let mut entries = self.entries.clone();
        let mut max_sequence_number = self.max_sequence_number;
        let mut delete_count = self.delete_count;
        let mut insert_count = self.insert_count;
        // Track keys gaining their first deletion so the bloom can be updated
        // incrementally without re-iterating the entire entry set. Pre-size
        // from the iterator's hint to skip Vec growth reallocations.
        let mut new_delete_hashes: Vec<u64> = Vec::with_capacity(additions.size_hint().0);
        for (key_hash, delete_seq, insert_seq) in additions {
            debug_assert_ne!(delete_seq, SEQUENCE_ABSENT, "real sequences are non-negative");
            let entry = entries.entry(key_hash).or_insert(TombstoneEntry {
                delete_seq: SEQUENCE_ABSENT,
                insert_seq: SEQUENCE_ABSENT,
            });
            let transitions = merge_max(entry, delete_seq, insert_seq);
            let stored_delete = entry.delete_seq;
            if transitions.new_delete {
                delete_count += 1;
                new_delete_hashes.push(bloom_half(key_hash));
            }
            if transitions.new_insert {
                insert_count += 1;
            }
            if max_sequence_number.is_none_or(|max| stored_delete > max) {
                max_sequence_number = Some(stored_delete);
            }
        }

        // See `DeletionIndex::extend_with` for the rationale behind not
        // re-scanning `entries` to validate `max_sequence_number` here.
        if delete_count > self.bloom_capacity {
            let new_capacity = bloom_capacity_for(delete_count);
            let bloom = SplitBlockBloomFilter::new(new_capacity);
            for (key_hash, entry) in &entries {
                if entry.delete_seq != SEQUENCE_ABSENT {
                    bloom.insert(bloom_half(*key_hash));
                }
            }
            return Self {
                entries,
                bloom: Arc::new(bloom),
                max_sequence_number,
                delete_count,
                insert_count,
                bloom_capacity: new_capacity,
            };
        }

        // Common path: insert only the newly-deleted keys into the shared
        // filter (O(K) relaxed atomic stores; see the safety argument in
        // `DeletionIndex::extend_max_deletes`).
        for h in new_delete_hashes {
            self.bloom.insert(h);
        }
        Self {
            entries,
            bloom: Arc::clone(&self.bloom),
            max_sequence_number,
            delete_count,
            insert_count,
            bloom_capacity: self.bloom_capacity,
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
        assert_eq!(next.max_sequence_number(), Some(10), "insert seqs must not raise the delete max");
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
            assert_eq!(idx.get(pk), None, "insert-only pk={pk} must probe as absent");
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
        assert_eq!(next.len(), 1, "delete of an insert-only key must not add an entry");
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
        let loaded = KeyDeletionIndex::from_maps(
            HashMap::new(),
            HashMap::from([(byte_key(9), 90_i64)]),
        );
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
        assert_eq!(
            key_delete_seq_of(&next, &999_u16.to_be_bytes()),
            Some(999)
        );
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
        // cache — the exact pattern that exposed the O(N²) regression.
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
        assert_eq!(idx.bloom_capacity, MIN_BLOOM_CAPACITY);

        // 64 deletions fit the minimum capacity exactly: no rebuild.
        for pk in 0..64 {
            idx = idx.extend_max_deletes([(pk, 1)]);
        }
        assert_eq!(idx.len(), 64);
        assert_eq!(
            idx.bloom_capacity, MIN_BLOOM_CAPACITY,
            "no rebuild expected before exceeding the sized capacity"
        );

        // The 65th deletion crosses the sized capacity: rebuild with headroom.
        idx = idx.extend_max_deletes([(64, 1)]);
        assert_eq!(idx.len(), 65);
        assert_eq!(
            idx.bloom_capacity, 130,
            "rebuild must size the new bloom for 2x the deletion count"
        );

        // Keep growing to force another rebuild cycle at the next boundary.
        for pk in 65..200 {
            idx = idx.extend_max_deletes([(pk, 1)]);
        }
        assert!(
            idx.bloom_capacity >= 200,
            "bloom_capacity must stay ahead of {} deletions, got {}",
            idx.delete_len(),
            idx.bloom_capacity,
        );

        // Every inserted key probes positive.
        for pk in 0..200 {
            assert_eq!(delete_seq_of(&idx, pk), Some(1), "missing pk={pk} after rebuilds");
        }
    }

    #[test]
    fn bloom_rebuild_sizes_by_deletion_count_not_total_entries() {
        // Insert-only entries must not count against the bloom capacity: the
        // filter is keyed on deletion membership.
        let inserts: HashMap<i64, i64> = (0..1000).map(|pk| (pk, 1)).collect();
        let idx = DeletionIndex::from_maps(HashMap::new(), inserts);
        assert_eq!(idx.bloom_capacity, MIN_BLOOM_CAPACITY);

        // 64 deletions still fit the minimum capacity despite 1000 entries.
        let mut grown = idx;
        for pk in 0..64 {
            grown = grown.extend_max_deletes([(pk, 2)]);
        }
        assert_eq!(grown.bloom_capacity, MIN_BLOOM_CAPACITY);
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
        let initial_cap = idx.bloom_capacity;

        // Extend with all-duplicate keys (different seq, but occupied path).
        let next = idx.extend_max_deletes((0..32).map(|pk| (pk, 2_i64)));
        assert_eq!(next.bloom_capacity, initial_cap);
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
            Arc::ptr_eq(&idx.bloom, &extended.bloom),
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
            !Arc::ptr_eq(&idx.bloom, &grown.bloom),
            "rebuild must allocate a fresh filter"
        );
        assert_eq!(delete_seq_of(&grown, 100), Some(2));
        assert_eq!(delete_seq_of(&grown, 250), Some(3));
    }
}
