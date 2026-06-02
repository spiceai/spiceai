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
//! frozen, share-by-`Arc` snapshots that scans probe at query time. They hold a plain
//! [`HashMap`] plus a [`BloomFilter`] sized for the deletion set, and expose only
//! read-only methods: a probe goes through the bloom filter first, and falls through to
//! the hash map only on a possible hit.
//!
//! # Build then publish
//!
//! All mutation happens before the index is wrapped in an `Arc`/`ArcSwap`. Construct
//! via [`DeletionIndex::from_map`] / [`DeletionIndex::empty`] (and the matching
//! [`KeyDeletionIndex`] constructors), publish through `ArcSwap`, and treat the
//! published index as immutable. To apply a write, build a new index with
//! [`DeletionIndex::extend_max`] (or [`KeyDeletionIndex::extend_max`]) and store the
//! `Arc<DeletionIndex>` back into the swap cell. Readers always see a fully-built
//! snapshot and never block.

use hash_index::{BloomFilter, hash_key};
use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

/// Bloom filter capacity floor: keep some signal even for empty / tiny sets so that the
/// "probably-not-present" path stays useful when a fresh index is constructed.
const MIN_BLOOM_CAPACITY: usize = 64;

/// Frozen deletion index for tables with a single-column Int64 primary key.
///
/// Holds the (pk → `delete_sequence`) map and an accompanying bloom filter. The bloom
/// filter's bit array is sized for `bloom_capacity` items; the writer tracks that
/// capacity so `extend_max` can update the bloom incrementally for the common case
/// where the index grows slowly, only paying a full O(N) rebuild when the entry count
/// crosses the next doubling boundary. This keeps amortized writer cost at O(K) per
/// call (K = number of additions) instead of the O(N) it would otherwise be — see
/// [`extend_max`](Self::extend_max) for the full argument.
#[derive(Debug, Clone)]
pub struct DeletionIndex {
    entries: Arc<HashMap<i64, i64>>,
    bloom: BloomFilter,
    /// Monotonic upper bound for the current immutable entries. This stays
    /// exact because indexes are build-once / extend-only; any future removal
    /// API must recompute it instead of carrying a stale high-water mark.
    /// `CayenneTableProvider::apply_partial_deletion_filter` relies on this
    /// exact value to decide whether a protected snapshot can skip deletion
    /// filtering without letting deleted rows through.
    max_sequence_number: Option<i64>,
    /// Item count the current `bloom` was sized for. When `entries.len()` exceeds
    /// `2 * bloom_capacity`, `extend_max` rebuilds the bloom from scratch to keep the
    /// false-positive rate bounded; otherwise it inserts incrementally.
    bloom_capacity: usize,
}

impl Default for DeletionIndex {
    fn default() -> Self {
        Self::empty()
    }
}

impl DeletionIndex {
    /// An empty deletion index. Probes always miss; bloom filter still allocated at the
    /// minimum capacity so size-0 indexes don't degrade once `extend_max` is called.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            entries: Arc::new(HashMap::new()),
            bloom: BloomFilter::new(MIN_BLOOM_CAPACITY),
            max_sequence_number: None,
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

    /// Build a frozen index from an owned `HashMap` of `pk -> delete_sequence`.
    #[must_use]
    pub fn from_map(entries: HashMap<i64, i64>) -> Self {
        let capacity = entries.len().max(MIN_BLOOM_CAPACITY);
        let mut bloom = BloomFilter::new(capacity);
        for &pk in entries.keys() {
            bloom.insert(hash_key(&pk));
        }
        let max_sequence_number = entries.values().copied().max();
        Self {
            entries: Arc::new(entries),
            bloom,
            max_sequence_number,
            bloom_capacity: capacity,
        }
    }

    /// Build a frozen index from an `Arc<HashMap>` (clones the map).
    #[must_use]
    pub fn from_arc_map(map: &Arc<HashMap<i64, i64>>) -> Self {
        Self::from_map((**map).clone())
    }

    /// Number of deletion entries in the index.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the index has any deletions.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Approximate resident bytes for memory accounting: each `i64 -> i64` entry
    /// is 16 bytes of key+value plus hash-table control/load-factor overhead.
    #[must_use]
    pub fn approx_bytes(&self) -> usize {
        const APPROX_I64_ENTRY_BYTES: usize = 48;
        self.entries.len().saturating_mul(APPROX_I64_ENTRY_BYTES)
    }

    /// Highest delete sequence number in this index, if any.
    #[must_use]
    pub fn max_sequence_number(&self) -> Option<i64> {
        self.max_sequence_number
    }

    /// Bloom-filter check. Returns `false` if the key is definitely not in the index;
    /// `true` if it might be (and a [`get`](Self::get) is required to confirm).
    #[inline]
    #[must_use]
    pub fn might_contain(&self, pk: i64) -> bool {
        self.bloom.might_contain(hash_key(&pk))
    }

    /// Bloom-prefiltered lookup. Returns the delete sequence number if `pk` is in the
    /// index, `None` otherwise.
    #[inline]
    #[must_use]
    pub fn get(&self, pk: i64) -> Option<i64> {
        if !self.bloom.might_contain(hash_key(&pk)) {
            return None;
        }
        self.entries.get(&pk).copied()
    }

    /// Direct read-only access to the underlying entries (for callers that need to
    /// rebuild a filtered index, e.g. partial-deletion filters).
    #[must_use]
    pub fn entries(&self) -> &HashMap<i64, i64> {
        &self.entries
    }

    /// Build a new index from `self`'s entries plus `additions`, taking the max sequence
    /// number on conflict. Used by writers to publish a new snapshot via `ArcSwap::store`.
    ///
    /// # Performance
    ///
    /// With `Arc<HashMap>` + `Arc::make_mut`, the map is mutated in place on the
    /// common single-writer path where no reader pins the latest generation; when
    /// readers do pin it, `Arc::make_mut` performs an O(N) clone. The bloom filter
    /// is updated incrementally (O(K) inserts
    /// for K new keys) instead of being rebuilt from scratch every call. A full
    /// O(N) rebuild only happens when the entry count crosses `2 * bloom_capacity`,
    /// giving amortized O(K) bloom cost per call.
    ///
    /// **Why this matters**: a previous revision rebuilt the bloom from scratch on
    /// every `extend_max` call, which is the dominant cost (10K entries ≈ 10K hash
    /// ops ≈ ~1 ms per call versus ~2 µs for the `HashMap` clone of the same size).
    /// On high-rate upsert/delete workloads (each producing a small `additions`
    /// batch but operating on a deletion cache that grows over time), the wasted
    /// bloom rebuild work compounds — and is the root cause of the ingestion
    /// regression that prompted this fix.
    #[must_use]
    pub fn extend_max(&self, additions: impl IntoIterator<Item = (i64, i64)>) -> Self {
        // Arc::make_mut mutates in place on the common single-writer path where
        // the latest DeletionIndex Arc is not held by any concurrent reader. Only
        // when readers pin the current generation do we pay the O(N) map clone.
        let mut entries_arc = Arc::clone(&self.entries);
        let entries = Arc::make_mut(&mut entries_arc);
        let mut max_sequence_number = self.max_sequence_number;
        let additions = additions.into_iter();
        // Track newly-inserted keys so the bloom can be updated incrementally
        // without re-iterating the entire entry set. Pre-size from the
        // iterator's hint to skip Vec growth reallocations.
        let mut new_keys: Vec<i64> = Vec::with_capacity(additions.size_hint().0);
        for (pk, seq) in additions {
            let stored_sequence = match entries.entry(pk) {
                std::collections::hash_map::Entry::Occupied(mut e) => {
                    let existing = *e.get();
                    if seq > existing {
                        *e.get_mut() = seq;
                        seq
                    } else {
                        existing
                    }
                }
                std::collections::hash_map::Entry::Vacant(e) => {
                    e.insert(seq);
                    new_keys.push(pk);
                    seq
                }
            };
            if max_sequence_number.is_none_or(|max| stored_sequence > max) {
                max_sequence_number = Some(stored_sequence);
            }
        }

        let new_len = entries.len();
        // `max_sequence_number` is maintained incrementally above; the inline
        // `is_none_or` check covers every mutation site, so we do not
        // re-scan `entries` here (a full scan would make `extend_max` O(N)
        // in debug builds and noticeably slow the test suite as the index
        // grows). `from_map` is the single rebuild path and recomputes the
        // exact max from scratch.
        // Rebuild from scratch when growth has outpaced bloom capacity by 2×.
        // The doubling threshold keeps amortized cost at O(K) per call:
        // between rebuilds we pay O(K) for incremental inserts; on a rebuild
        // we pay O(N), but at the next rebuild N has doubled again, so the
        // total work across one doubling cycle is geometric and amortizes
        // to O(N).
        if new_len > self.bloom_capacity.saturating_mul(2) {
            let new_capacity = new_len.max(MIN_BLOOM_CAPACITY);
            let mut bloom = BloomFilter::new(new_capacity);
            for &pk in entries.keys() {
                bloom.insert(hash_key(&pk));
            }
            return Self {
                entries: entries_arc,
                bloom,
                max_sequence_number,
                bloom_capacity: new_capacity,
            };
        }

        // Common path: clone the existing bloom (cheap — Vec<u64> memcpy of a
        // few KB) and insert only the new keys. O(K) work for K new keys.
        let mut bloom = self.bloom.clone();
        for pk in &new_keys {
            bloom.insert(hash_key(pk));
        }
        Self {
            entries: entries_arc,
            bloom,
            max_sequence_number,
            bloom_capacity: self.bloom_capacity,
        }
    }
}

/// Frozen deletion index for tables with a composite or non-integer primary key. Keys
/// are the byte-encoded form produced by `arrow_row::RowConverter`.
///
/// See [`DeletionIndex`] for the bloom-capacity / incremental-rebuild contract;
/// `KeyDeletionIndex` applies the same strategy to byte-keyed entries.
#[derive(Debug, Clone)]
pub struct KeyDeletionIndex {
    entries: Arc<HashMap<Box<[u8]>, i64>>,
    bloom: BloomFilter,
    /// Monotonic upper bound for the current immutable entries. This stays
    /// exact because indexes are build-once / extend-only; any future removal
    /// API must recompute it instead of carrying a stale high-water mark.
    /// `CayenneTableProvider::apply_partial_deletion_filter` relies on this
    /// exact value to decide whether a protected snapshot can skip deletion
    /// filtering without letting deleted rows through.
    max_sequence_number: Option<i64>,
    /// Item count the current `bloom` was sized for. Mirrors
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
            entries: Arc::new(HashMap::new()),
            bloom: BloomFilter::new(MIN_BLOOM_CAPACITY),
            max_sequence_number: None,
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

    /// Build a frozen index from an owned `HashMap` of `pk_bytes -> delete_sequence`.
    #[must_use]
    pub fn from_map(entries: HashMap<Box<[u8]>, i64>) -> Self {
        let capacity = entries.len().max(MIN_BLOOM_CAPACITY);
        let mut bloom = BloomFilter::new(capacity);
        for key in entries.keys() {
            bloom.insert(hash_key(&key.as_ref()));
        }
        let max_sequence_number = entries.values().copied().max();
        Self {
            entries: Arc::new(entries),
            bloom,
            max_sequence_number,
            bloom_capacity: capacity,
        }
    }

    /// Build a frozen index from an `Arc<HashMap>` (clones the map).
    #[must_use]
    pub fn from_arc_map(map: &Arc<HashMap<Box<[u8]>, i64>>) -> Self {
        Self::from_map((**map).clone())
    }

    /// Number of deletion entries.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Approximate resident bytes for memory accounting. Uses a per-entry
    /// estimate (a typical row-encoded composite key plus the `Box`, value, and
    /// hash-table control overhead) rather than summing every key length, so the
    /// call stays O(1) on the hot CDC path instead of O(total deletions).
    #[must_use]
    pub fn approx_bytes(&self) -> usize {
        const APPROX_KEY_ENTRY_BYTES: usize = 80;
        self.entries.len().saturating_mul(APPROX_KEY_ENTRY_BYTES)
    }

    /// Whether the index has any deletions.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Highest delete sequence number in this index, if any.
    #[must_use]
    pub fn max_sequence_number(&self) -> Option<i64> {
        self.max_sequence_number
    }

    /// Bloom-filter check; see [`DeletionIndex::might_contain`].
    #[inline]
    #[must_use]
    pub fn might_contain(&self, key: &[u8]) -> bool {
        self.bloom.might_contain(hash_key(&key))
    }

    /// Bloom-prefiltered lookup. Returns the delete sequence number if `key` is in the
    /// index, `None` otherwise.
    #[inline]
    #[must_use]
    pub fn get(&self, key: &[u8]) -> Option<i64> {
        if !self.bloom.might_contain(hash_key(&key)) {
            return None;
        }
        self.entries.get(key).copied()
    }

    /// Direct read-only access to the underlying entries.
    #[must_use]
    pub fn entries(&self) -> &HashMap<Box<[u8]>, i64> {
        &self.entries
    }

    /// Build a new index from `self`'s entries plus `additions`, taking the max sequence
    /// number on conflict.
    ///
    /// See [`DeletionIndex::extend_max`] for the amortization argument. Bloom rebuilds
    /// only happen when the entry count crosses `2 * bloom_capacity`; otherwise only
    /// the new keys are inserted into a clone of the existing bloom.
    #[must_use]
    pub fn extend_max(&self, additions: impl IntoIterator<Item = (Box<[u8]>, i64)>) -> Self {
        // Arc::make_mut mutates in place on the common single-writer path where
        // the latest KeyDeletionIndex Arc is not held by any concurrent reader.
        // Only when readers pin the current generation (or for composite PKs with
        // heavier Box<[u8]> keys) do we pay the O(N) map + key clone.
        let mut entries_arc = Arc::clone(&self.entries);
        let entries = Arc::make_mut(&mut entries_arc);
        let mut max_sequence_number = self.max_sequence_number;
        let additions = additions.into_iter();
        // Hash newly-inserted keys inline so the bloom can be updated
        // incrementally without paying for a `Box<[u8]>` clone per key (the
        // bloom only needs the hash, not the byte slice). Pre-size from the
        // iterator's hint to skip Vec growth reallocations.
        let mut new_hashes: Vec<u64> = Vec::with_capacity(additions.size_hint().0);
        for (key, seq) in additions {
            let stored_sequence = match entries.entry(key) {
                std::collections::hash_map::Entry::Occupied(mut e) => {
                    let existing = *e.get();
                    if seq > existing {
                        *e.get_mut() = seq;
                        seq
                    } else {
                        existing
                    }
                }
                std::collections::hash_map::Entry::Vacant(e) => {
                    new_hashes.push(hash_key(&e.key().as_ref()));
                    e.insert(seq);
                    seq
                }
            };
            if max_sequence_number.is_none_or(|max| stored_sequence > max) {
                max_sequence_number = Some(stored_sequence);
            }
        }

        let new_len = entries.len();
        // See `DeletionIndex::extend_max` for the rationale behind not
        // re-scanning `entries` to validate `max_sequence_number` here.
        if new_len > self.bloom_capacity.saturating_mul(2) {
            let new_capacity = new_len.max(MIN_BLOOM_CAPACITY);
            let mut bloom = BloomFilter::new(new_capacity);
            for key in entries.keys() {
                bloom.insert(hash_key(&key.as_ref()));
            }
            return Self {
                entries: entries_arc,
                bloom,
                max_sequence_number,
                bloom_capacity: new_capacity,
            };
        }

        let mut bloom = self.bloom.clone();
        for h in new_hashes {
            bloom.insert(h);
        }
        Self {
            entries: entries_arc,
            bloom,
            max_sequence_number,
            bloom_capacity: self.bloom_capacity,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_map_then_get() {
        let mut map = HashMap::new();
        map.insert(100, 1);
        map.insert(200, 2);
        map.insert(300, 3);
        let idx = DeletionIndex::from_map(map);

        assert_eq!(idx.len(), 3);
        assert_eq!(idx.max_sequence_number(), Some(3));
        assert_eq!(idx.get(100), Some(1));
        assert_eq!(idx.get(200), Some(2));
        assert_eq!(idx.get(300), Some(3));
        assert_eq!(idx.get(400), None);
    }

    #[test]
    fn empty_index_probes_to_none() {
        let idx = DeletionIndex::empty();
        assert!(idx.is_empty());
        assert_eq!(idx.max_sequence_number(), None);
        assert_eq!(idx.get(42), None);
    }

    #[test]
    fn extend_max_takes_higher_sequence() {
        let mut map = HashMap::new();
        map.insert(100, 5);
        let idx = DeletionIndex::from_map(map);

        let next = idx.extend_max([(100, 3), (200, 7)]);
        assert_eq!(next.max_sequence_number(), Some(7));
        assert_eq!(next.get(100), Some(5));
        assert_eq!(next.get(200), Some(7));

        let after = next.extend_max([(100, 10)]);
        assert_eq!(after.max_sequence_number(), Some(10));
        assert_eq!(after.get(100), Some(10));
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
        assert_eq!(idx.get(&key1), Some(1));
        assert_eq!(idx.get(&key2), Some(2));
        assert_eq!(idx.get(&[7, 8, 9]), None);
    }

    #[test]
    fn key_index_extend_max() {
        let key1: Box<[u8]> = vec![1, 2].into_boxed_slice();
        let mut map: HashMap<Box<[u8]>, i64> = HashMap::new();
        map.insert(key1.clone(), 5);
        let idx = KeyDeletionIndex::from_map(map);

        let next = idx.extend_max([(key1.clone(), 3)]);
        assert_eq!(next.max_sequence_number(), Some(5));
        assert_eq!(next.get(&key1), Some(5));

        let after = next.extend_max([(key1.clone(), 10)]);
        assert_eq!(after.max_sequence_number(), Some(10));
        assert_eq!(after.get(&key1), Some(10));
    }

    // -------------------------------------------------------------------------
    // Regression tests for the incremental bloom-update path.
    //
    // A previous revision rebuilt the bloom filter from scratch on every
    // `extend_max` call (iterating ALL entries and re-hashing them). On
    // high-rate upsert/delete workloads this turned every per-row cache
    // update into O(N) work, where N is the cumulative deletion-cache size.
    // The cumulative effect across M writes is O(M*N), which is the root
    // cause of the ingestion regression the user reported (~200% on
    // upsert-heavy workloads with growing deletion sets).
    //
    // The fix rebuilds the bloom only when entries cross `2 * bloom_capacity`
    // (amortized O(K)) and inserts incrementally in between. These tests
    // exercise both code paths and verify correctness across many extend
    // cycles.
    // -------------------------------------------------------------------------

    #[test]
    fn extend_max_many_small_batches_preserves_all_entries() {
        // Simulates many small upserts each adding a single new PK to the
        // cache — the exact pattern that exposed the O(N²) regression.
        let mut idx = DeletionIndex::empty();
        let n = 1024;
        for pk in 0_i64..n {
            idx = idx.extend_max([(pk, pk + 1)]);
        }
        assert_eq!(i64::try_from(idx.len()).expect("len fits in i64"), n);
        for pk in 0_i64..n {
            assert_eq!(
                idx.get(pk),
                Some(pk + 1),
                "missing entry for pk={pk} after {n} incremental extends",
            );
        }
        // A key never inserted must not be reported as present.
        assert_eq!(idx.get(n + 100), None);
    }

    #[test]
    fn extend_max_rebuilds_bloom_at_doubling_boundaries() {
        // Verify the bloom_capacity grows in doublings (geometric amortization).
        // The first `from_map`/`empty` builds at MIN_BLOOM_CAPACITY=64;
        // crossing 128 triggers a rebuild to ≥128; crossing 256 to ≥256; etc.
        let mut idx = DeletionIndex::empty();
        assert_eq!(idx.bloom_capacity, MIN_BLOOM_CAPACITY);

        // Add 64 items — still within original capacity (64 ≤ 128 = 2*64).
        for pk in 0..64 {
            idx = idx.extend_max([(pk, 1)]);
        }
        assert_eq!(idx.len(), 64);
        assert_eq!(
            idx.bloom_capacity, MIN_BLOOM_CAPACITY,
            "no rebuild expected before crossing 2x capacity"
        );

        // Add 65 more — cross 2*64=128. Rebuild expected.
        for pk in 64..129 {
            idx = idx.extend_max([(pk, 1)]);
        }
        assert_eq!(idx.len(), 129);
        assert!(
            idx.bloom_capacity >= 129,
            "bloom_capacity must grow to fit {} entries after rebuild, got {}",
            idx.len(),
            idx.bloom_capacity,
        );

        // Every inserted key probes positive.
        for pk in 0..129 {
            assert_eq!(idx.get(pk), Some(1), "missing pk={pk} after rebuild");
        }
    }

    #[test]
    fn extend_max_preserves_max_sequence_under_repeated_updates() {
        // Same PK updated many times — every extend should preserve the max
        // sequence seen so far. Tests the Occupied entry path.
        let mut idx = DeletionIndex::empty();
        idx = idx.extend_max([(42, 100)]);
        idx = idx.extend_max([(42, 50)]); // older write, should not override
        idx = idx.extend_max([(42, 200)]); // newer write, takes max
        idx = idx.extend_max([(42, 150)]); // older write, should not override
        assert_eq!(idx.get(42), Some(200));
        assert_eq!(idx.len(), 1, "no new entry should have been added");
    }

    #[test]
    fn key_index_extend_max_many_small_batches_preserves_all_entries() {
        // Same regression case for byte-keyed (composite-PK) tables.
        let mut idx = KeyDeletionIndex::empty();
        let n = 256_usize;
        for i in 0..n {
            let key: Box<[u8]> = (i as u64).to_le_bytes().to_vec().into_boxed_slice();
            idx = idx.extend_max([(key, i64::try_from(i).expect("i fits in i64") + 1)]);
        }
        assert_eq!(idx.len(), n);
        for i in 0..n {
            let key: Box<[u8]> = (i as u64).to_le_bytes().to_vec().into_boxed_slice();
            assert_eq!(
                idx.get(&key),
                Some(i64::try_from(i).expect("i fits in i64") + 1),
                "missing entry for key i={i} after {n} incremental extends",
            );
        }
    }

    #[test]
    fn extend_max_batch_only_pays_for_new_keys() {
        // When all additions are duplicates (already present), no new bloom
        // inserts should happen — verified indirectly by checking the
        // bloom_capacity is unchanged and queries still work.
        let mut map = HashMap::new();
        for pk in 0..32 {
            map.insert(pk, 1_i64);
        }
        let idx = DeletionIndex::from_map(map);
        let initial_cap = idx.bloom_capacity;

        // Extend with all-duplicate keys (different seq, but Occupied path).
        let next = idx.extend_max((0..32).map(|pk| (pk, 2_i64)));
        assert_eq!(next.bloom_capacity, initial_cap);
        assert_eq!(next.len(), 32);
        for pk in 0..32 {
            assert_eq!(
                next.get(pk),
                Some(2),
                "max-sequence update lost for pk={pk}"
            );
        }
    }
}
