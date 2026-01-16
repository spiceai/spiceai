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

//! High-performance deletion tracking using SIMD-optimized hash index.
//!
//! This module provides `DeletionIndex`, a drop-in replacement for `HashMap<i64, i64>`
//! that's optimized for the common case where most rows are NOT deleted.
//!
//! # Performance Benefits
//!
//! 1. **Bloom Filter**: O(1) rejection of definitely-not-deleted keys before probing
//! 2. **SIMD Probing**: Swiss table-style parallel slot checking (16 at a time on x86)
//! 3. **Cache-Friendly**: Control bytes separate from data for better prefetching
//!
//! # Typical Use Case
//!
//! In a table with 1 million rows and 1000 deletions:
//! - Without bloom filter: 1M hash table probes
//! - With bloom filter: ~1000 hash table probes + 1M bit tests (much faster)

use hash_index::{hash_key, BloomFilter};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

/// A high-performance deletion index using SIMD hash index with bloom filter.
///
/// This is optimized for the common case where most rows are NOT deleted.
/// The bloom filter provides fast O(1) rejection before probing the hash table.
#[derive(Debug)]
pub struct DeletionIndex {
    /// Map of deleted PK -> delete sequence number.
    /// Using `HashMap` internally but with bloom filter for fast negative lookups.
    entries: RwLock<HashMap<i64, i64>>,
    /// Bloom filter for fast negative lookups.
    bloom: RwLock<BloomFilter>,
    /// Whether bloom filter is enabled.
    use_bloom: bool,
}

impl Default for DeletionIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl DeletionIndex {
    /// Creates a new empty deletion index with bloom filter enabled.
    #[must_use]
    pub fn new() -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
            bloom: RwLock::new(BloomFilter::new(1024)), // Start with reasonable capacity
            use_bloom: true,
        }
    }

    /// Creates a new deletion index with the expected number of deletions.
    #[must_use]
    pub fn with_capacity(expected_deletions: usize) -> Self {
        let capacity = expected_deletions.max(64);
        Self {
            entries: RwLock::new(HashMap::with_capacity(capacity)),
            bloom: RwLock::new(BloomFilter::new(capacity)),
            use_bloom: true,
        }
    }

    /// Creates a deletion index from an existing `HashMap`.
    #[must_use]
    pub fn from_map(map: HashMap<i64, i64>) -> Self {
        let capacity = map.len().max(64);
        let mut bloom = BloomFilter::new(capacity);

        // Build bloom filter from existing entries
        for &pk in map.keys() {
            bloom.insert(hash_key(&pk));
        }

        Self {
            entries: RwLock::new(map),
            bloom: RwLock::new(bloom),
            use_bloom: true,
        }
    }

    /// Creates a deletion index from an Arc<HashMap>.
    ///
    /// This clones the `HashMap` to enable mutable operations.
    #[must_use]
    pub fn from_arc_map(map: &Arc<HashMap<i64, i64>>) -> Self {
        Self::from_map((**map).clone())
    }

    /// Returns the number of entries in the index.
    ///
    /// # Panics
    ///
    /// Panics if the lock is poisoned.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.read().len()
    }

    /// Returns true if the index is empty.
    ///
    /// # Panics
    ///
    /// Panics if the lock is poisoned.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.read().is_empty()
    }

    /// Checks if a key might be deleted (bloom filter check only).
    ///
    /// Returns `false` if the key is definitely NOT deleted.
    /// Returns `true` if the key might be deleted (requires full lookup).
    ///
    /// # Panics
    ///
    /// Panics if the lock is poisoned.
    #[inline]
    pub fn might_contain(&self, pk: i64) -> bool {
        if !self.use_bloom {
            return true;
        }
        self.bloom.read().might_contain(hash_key(&pk))
    }

    /// Gets the delete sequence for a key, if deleted.
    ///
    /// Uses bloom filter for fast negative lookups.
    ///
    /// # Panics
    ///
    /// Panics if the lock is poisoned.
    #[inline]
    pub fn get(&self, pk: i64) -> Option<i64> {
        // Fast path: bloom filter check
        if self.use_bloom && !self.bloom.read().might_contain(hash_key(&pk)) {
            return None;
        }
        // Slow path: hash table lookup
        self.entries.read().get(&pk).copied()
    }

    /// Checks if a key is deleted.
    ///
    /// Uses bloom filter for fast negative lookups.
    #[inline]
    pub fn contains(&self, pk: i64) -> bool {
        self.get(pk).is_some()
    }

    /// Inserts a deletion entry.
    ///
    /// Returns the previous delete sequence if the key was already deleted.
    ///
    /// # Panics
    ///
    /// Panics if the lock is poisoned.
    pub fn insert(&self, pk: i64, delete_sequence: i64) -> Option<i64> {
        if self.use_bloom {
            self.bloom.write().insert(hash_key(&pk));
        }
        self.entries.write().insert(pk, delete_sequence)
    }

    /// Inserts or updates a deletion entry with the maximum sequence number.
    ///
    /// If the key already exists, updates to the max of existing and new sequence.
    ///
    /// # Panics
    ///
    /// Panics if the lock is poisoned.
    pub fn insert_max(&self, pk: i64, delete_sequence: i64) {
        if self.use_bloom {
            self.bloom.write().insert(hash_key(&pk));
        }
        self.entries
            .write()
            .entry(pk)
            .and_modify(|seq| *seq = (*seq).max(delete_sequence))
            .or_insert(delete_sequence);
    }

    /// Inserts multiple deletion entries.
    ///
    /// # Panics
    ///
    /// Panics if the lock is poisoned.
    pub fn insert_batch(&self, entries: impl IntoIterator<Item = (i64, i64)>) {
        let entries_vec: Vec<_> = entries.into_iter().collect();
        if entries_vec.is_empty() {
            return;
        }

        if self.use_bloom {
            let mut bloom = self.bloom.write();
            for (pk, _) in &entries_vec {
                bloom.insert(hash_key(pk));
            }
        }

        let mut map = self.entries.write();
        for (pk, seq) in entries_vec {
            map.insert(pk, seq);
        }
    }

    /// Clears all entries.
    ///
    /// # Panics
    ///
    /// Panics if the lock is poisoned.
    pub fn clear(&self) {
        self.entries.write().clear();
        self.bloom.write().clear();
    }

    /// Returns a snapshot of the entries as an Arc<HashMap>.
    ///
    /// This is useful for passing to filter executors that expect Arc<HashMap>.
    ///
    /// # Panics
    ///
    /// Panics if the lock is poisoned.
    #[must_use]
    pub fn snapshot(&self) -> Arc<HashMap<i64, i64>> {
        Arc::new(self.entries.read().clone())
    }

    /// Provides iteration over keys (for compatibility).
    ///
    /// # Panics
    ///
    /// Panics if the lock is poisoned.
    pub fn keys(&self) -> Vec<i64> {
        self.entries.read().keys().copied().collect()
    }

    /// Returns all entries as a vector of (key, value) pairs.
    pub fn to_vec(&self) -> Vec<(i64, i64)> {
        self.entries.read().iter().map(|(&k, &v)| (k, v)).collect()
    }
}

/// A byte-key based deletion index for composite/non-integer primary keys.
///
/// Similar to `DeletionIndex` but for `Box<[u8]>` keys created by `RowConverter`.
#[derive(Debug)]
pub struct KeyDeletionIndex {
    /// Map of deleted PK bytes -> delete sequence number.
    entries: RwLock<HashMap<Box<[u8]>, i64>>,
    /// Bloom filter for fast negative lookups.
    bloom: RwLock<BloomFilter>,
    /// Whether bloom filter is enabled.
    use_bloom: bool,
}

impl Default for KeyDeletionIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl KeyDeletionIndex {
    /// Creates a new empty deletion index.
    #[must_use]
    pub fn new() -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
            bloom: RwLock::new(BloomFilter::new(1024)),
            use_bloom: true,
        }
    }

    /// Creates a deletion index from an existing `HashMap`.
    #[must_use]
    pub fn from_map(map: HashMap<Box<[u8]>, i64>) -> Self {
        let capacity = map.len().max(64);
        let mut bloom = BloomFilter::new(capacity);

        for key in map.keys() {
            bloom.insert(hash_key(&key.as_ref()));
        }

        Self {
            entries: RwLock::new(map),
            bloom: RwLock::new(bloom),
            use_bloom: true,
        }
    }

    /// Creates a deletion index from an Arc<HashMap>.
    #[must_use]
    pub fn from_arc_map(map: &Arc<HashMap<Box<[u8]>, i64>>) -> Self {
        Self::from_map((**map).clone())
    }

    /// Returns the number of entries.
    ///
    /// # Panics
    ///
    /// Panics if the lock is poisoned.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.read().len()
    }

    /// Returns true if empty.
    ///
    /// # Panics
    ///
    /// Panics if the lock is poisoned.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.read().is_empty()
    }

    /// Checks if a key might be deleted (bloom filter check only).
    ///
    /// # Panics
    ///
    /// Panics if the lock is poisoned.
    #[inline]
    pub fn might_contain(&self, key: &[u8]) -> bool {
        if !self.use_bloom {
            return true;
        }
        self.bloom.read().might_contain(hash_key(&key))
    }

    /// Gets the delete sequence for a key.
    ///
    /// # Panics
    ///
    /// Panics if the lock is poisoned.
    #[inline]
    pub fn get(&self, key: &[u8]) -> Option<i64> {
        if self.use_bloom && !self.bloom.read().might_contain(hash_key(&key)) {
            return None;
        }
        self.entries.read().get(key).copied()
    }

    /// Inserts a deletion entry.
    ///
    /// # Panics
    ///
    /// Panics if the lock is poisoned.
    pub fn insert(&self, key: Box<[u8]>, delete_sequence: i64) -> Option<i64> {
        if self.use_bloom {
            self.bloom.write().insert(hash_key(&key.as_ref()));
        }
        self.entries.write().insert(key, delete_sequence)
    }

    /// Inserts or updates with max sequence.
    ///
    /// # Panics
    ///
    /// Panics if the lock is poisoned.
    pub fn insert_max(&self, key: Box<[u8]>, delete_sequence: i64) {
        if self.use_bloom {
            self.bloom.write().insert(hash_key(&key.as_ref()));
        }
        self.entries
            .write()
            .entry(key)
            .and_modify(|seq| *seq = (*seq).max(delete_sequence))
            .or_insert(delete_sequence);
    }

    /// Returns a snapshot of the entries.
    ///
    /// # Panics
    ///
    /// Panics if the lock is poisoned.
    #[must_use]
    pub fn snapshot(&self) -> Arc<HashMap<Box<[u8]>, i64>> {
        Arc::new(self.entries.read().clone())
    }

    /// Clears all entries.
    ///
    /// # Panics
    ///
    /// Panics if the lock is poisoned.
    pub fn clear(&self) {
        self.entries.write().clear();
        self.bloom.write().clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deletion_index_basic() {
        let index = DeletionIndex::new();

        // Insert some deletions
        index.insert(100, 1);
        index.insert(200, 2);
        index.insert(300, 3);

        // Check lookups
        assert_eq!(index.get(100), Some(1));
        assert_eq!(index.get(200), Some(2));
        assert_eq!(index.get(300), Some(3));
        assert_eq!(index.get(400), None); // Not deleted

        // Bloom filter should reject definitely-not-deleted
        assert!(index.might_contain(100));
        // Note: 400 might still pass bloom filter (false positive possible)
    }

    #[test]
    fn test_deletion_index_insert_max() {
        let index = DeletionIndex::new();

        index.insert_max(100, 1);
        assert_eq!(index.get(100), Some(1));

        // Insert with higher sequence
        index.insert_max(100, 5);
        assert_eq!(index.get(100), Some(5));

        // Insert with lower sequence - should keep higher
        index.insert_max(100, 3);
        assert_eq!(index.get(100), Some(5));
    }

    #[test]
    fn test_deletion_index_batch() {
        let index = DeletionIndex::new();

        let entries = vec![(1, 10), (2, 20), (3, 30)];
        index.insert_batch(entries);

        assert_eq!(index.len(), 3);
        assert_eq!(index.get(1), Some(10));
        assert_eq!(index.get(2), Some(20));
        assert_eq!(index.get(3), Some(30));
    }

    #[test]
    fn test_deletion_index_from_map() {
        let mut map = HashMap::new();
        map.insert(1, 100);
        map.insert(2, 200);

        let index = DeletionIndex::from_map(map);

        assert_eq!(index.len(), 2);
        assert_eq!(index.get(1), Some(100));
        assert_eq!(index.get(2), Some(200));
    }

    #[test]
    fn test_key_deletion_index() {
        let index = KeyDeletionIndex::new();

        let key1: Box<[u8]> = vec![1, 2, 3].into_boxed_slice();
        let key2: Box<[u8]> = vec![4, 5, 6].into_boxed_slice();

        index.insert(key1.clone(), 1);
        index.insert(key2.clone(), 2);

        assert_eq!(index.get(&key1), Some(1));
        assert_eq!(index.get(&key2), Some(2));
        assert_eq!(index.get(&[7, 8, 9]), None);
    }

    #[test]
    fn test_bloom_filter_effectiveness() {
        let index = DeletionIndex::with_capacity(100);

        // Insert 100 deletions
        for i in 0..100 {
            index.insert(i * 2, i); // Even numbers only
        }

        // Check that odd numbers are quickly rejected
        // (bloom filter should catch most of them)
        let mut bloom_rejects = 0;
        for i in 0..100 {
            let odd = i * 2 + 1;
            if !index.might_contain(odd) {
                bloom_rejects += 1;
            }
        }

        // With good bloom filter, most odd numbers should be rejected
        // Allow for some false positives
        assert!(
            bloom_rejects > 80,
            "Bloom filter should reject most non-deleted keys, got {bloom_rejects}"
        );
    }
}
