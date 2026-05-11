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
use std::sync::Arc;

/// Bloom filter capacity floor: keep some signal even for empty / tiny sets so that the
/// "probably-not-present" path stays useful when a fresh index is constructed.
const MIN_BLOOM_CAPACITY: usize = 64;

/// Frozen deletion index for tables with a single-column Int64 primary key.
#[derive(Debug, Clone)]
pub struct DeletionIndex {
    entries: HashMap<i64, i64>,
    bloom: BloomFilter,
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
            entries: HashMap::new(),
            bloom: BloomFilter::new(MIN_BLOOM_CAPACITY),
        }
    }

    /// Build a frozen index from an owned `HashMap` of `pk -> delete_sequence`.
    #[must_use]
    pub fn from_map(entries: HashMap<i64, i64>) -> Self {
        let capacity = entries.len().max(MIN_BLOOM_CAPACITY);
        let mut bloom = BloomFilter::new(capacity);
        for &pk in entries.keys() {
            bloom.insert(hash_key(&pk));
        }
        Self { entries, bloom }
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
    #[must_use]
    pub fn extend_max(&self, additions: impl IntoIterator<Item = (i64, i64)>) -> Self {
        let mut entries = self.entries.clone();
        for (pk, seq) in additions {
            entries
                .entry(pk)
                .and_modify(|existing| *existing = (*existing).max(seq))
                .or_insert(seq);
        }
        Self::from_map(entries)
    }
}

/// Frozen deletion index for tables with a composite or non-integer primary key. Keys
/// are the byte-encoded form produced by `arrow_row::RowConverter`.
#[derive(Debug, Clone)]
pub struct KeyDeletionIndex {
    entries: HashMap<Box<[u8]>, i64>,
    bloom: BloomFilter,
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
            entries: HashMap::new(),
            bloom: BloomFilter::new(MIN_BLOOM_CAPACITY),
        }
    }

    /// Build a frozen index from an owned `HashMap` of `pk_bytes -> delete_sequence`.
    #[must_use]
    pub fn from_map(entries: HashMap<Box<[u8]>, i64>) -> Self {
        let capacity = entries.len().max(MIN_BLOOM_CAPACITY);
        let mut bloom = BloomFilter::new(capacity);
        for key in entries.keys() {
            bloom.insert(hash_key(&key.as_ref()));
        }
        Self { entries, bloom }
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

    /// Whether the index has any deletions.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
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
    #[must_use]
    pub fn extend_max(&self, additions: impl IntoIterator<Item = (Box<[u8]>, i64)>) -> Self {
        let mut entries = self.entries.clone();
        for (key, seq) in additions {
            entries
                .entry(key)
                .and_modify(|existing| *existing = (*existing).max(seq))
                .or_insert(seq);
        }
        Self::from_map(entries)
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
        assert_eq!(idx.get(100), Some(1));
        assert_eq!(idx.get(200), Some(2));
        assert_eq!(idx.get(300), Some(3));
        assert_eq!(idx.get(400), None);
    }

    #[test]
    fn empty_index_probes_to_none() {
        let idx = DeletionIndex::empty();
        assert!(idx.is_empty());
        assert_eq!(idx.get(42), None);
    }

    #[test]
    fn extend_max_takes_higher_sequence() {
        let mut map = HashMap::new();
        map.insert(100, 5);
        let idx = DeletionIndex::from_map(map);

        let next = idx.extend_max([(100, 3), (200, 7)]);
        assert_eq!(next.get(100), Some(5));
        assert_eq!(next.get(200), Some(7));

        let after = next.extend_max([(100, 10)]);
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
        assert_eq!(next.get(&key1), Some(5));

        let after = next.extend_max([(key1.clone(), 10)]);
        assert_eq!(after.get(&key1), Some(10));
    }
}
