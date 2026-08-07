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

//! Pingora-LRU based cache backend implementation.

use super::{CacheBackend, CacheBackendBuilder};
use crate::Sizeable;
use async_trait::async_trait;
use parking_lot::RwLock;
use pingora_lru::Lru;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

// 16 shards to match pingora-lru's internal sharding for optimal cache line alignment
// This sharding strategy provides:
// 1. Reduced lock contention (16x reduction vs single lock)
// 2. Better cache line alignment with pingora-lru's internal data structures
// 3. Improved throughput for concurrent operations (2-3x faster than single-threaded caches)
const NUM_KEY_SHARDS: usize = 16;

/// Entries to reserve per shard when the cache is created.
///
/// pingora-lru takes this as a predicted item count and reserves it eagerly, so it cannot
/// be derived from `max_size`, which is a byte budget: a 128 MiB cache would ask each of
/// the 16 shards to reserve 8.4 million entries before a single value is cached. The
/// shards grow on demand, so a small reservation only costs a few reallocations while the
/// cache is still cold.
const SHARD_RESERVED_ENTRIES: usize = 16;

/// Metadata for a cached entry, stored separately from the value.
///
/// This allows TTL checks without touching the pingora-lru structure,
/// which only supports destructive reads (remove + re-admit).
#[derive(Clone, Copy)]
struct KeyMetadata {
    /// When this entry expires
    expires_at: Instant,
}

/// A cached value together with the key it was admitted under.
///
/// pingora-lru reports only values and weights when it evicts, so the key travels with
/// the value. Without it an eviction could not name the metadata entry it has to drop,
/// and `len()`/`iter_keys()` would keep reporting keys whose values are gone.
struct KeyedValue<V> {
    key: u64,
    value: V,
}

/// Pingora-LRU based cache backend implementation
///
/// Provides:
/// - 2-3x higher throughput than Moka under concurrent load
/// - 16-shard architecture for reduced lock contention
/// - Separate metadata tracking for TTL and size (avoids race conditions on expiry checks)
///
/// Architecture:
/// - Values are stored in pingora-lru, paired with their key so evictions can be traced
///   back to the metadata they invalidate
/// - Eviction is driven by this backend: pingora-lru only enforces the weight limit when
///   asked to, so `insert` and `run_pending_tasks` evict down to it
/// - Metadata (TTL expiry, weight) is stored separately in sharded `HashMaps`
/// - TTL checks use metadata first, avoiding unnecessary cache removals
/// - `weighted_size()` uses pingora-lru's native `weight()` method for accuracy
///
/// Trade-offs:
/// - pingora-lru requires remove + re-admit to read values (no `peek_value` API)
/// - Brief race window during value retrieval under heavy concurrent load
/// - More complex implementation than Moka
pub struct PingoraBackend<V>
where
    V: Clone + Send + Sync + 'static,
{
    cache: Arc<Lru<KeyedValue<V>, 16>>,
    // 16-shard metadata tracking for TTL checks and key iteration
    // Each shard covers 1/16th of the key space (key % 16)
    // Stores expiry time and weight for each key
    metadata_shards: Arc<[RwLock<HashMap<u64, KeyMetadata>>; NUM_KEY_SHARDS]>,
    ttl: Duration,
}

impl<V> PingoraBackend<V>
where
    V: Sizeable + Clone + Send + Sync + 'static,
{
    /// Creates a new Pingora backend with the given configuration.
    #[must_use]
    pub fn new(builder: &CacheBackendBuilder) -> Self {
        Self::with_params(builder.max_capacity(), builder.ttl())
    }

    /// Creates a new Pingora backend with explicit capacity and TTL.
    #[must_use]
    pub fn with_params(max_capacity: u64, ttl: std::time::Duration) -> Self {
        let weight_limit = usize::try_from(max_capacity).unwrap_or(usize::MAX);
        let cache = Arc::new(Lru::with_capacity(weight_limit, SHARD_RESERVED_ENTRIES));

        // Initialize 16 shards for metadata tracking
        let metadata_shards: Arc<[RwLock<HashMap<u64, KeyMetadata>>; NUM_KEY_SHARDS]> =
            Arc::new(std::array::from_fn(|_| RwLock::new(HashMap::new())));

        Self {
            cache,
            metadata_shards,
            ttl,
        }
    }

    #[inline]
    #[expect(
        clippy::cast_possible_truncation,
        reason = "Shard index only needs low bits of u64 key"
    )]
    fn get_shard_index(key: u64) -> usize {
        (key as usize) % NUM_KEY_SHARDS
    }

    /// Check if an entry is expired based on its metadata.
    /// Returns None if the key doesn't exist in metadata.
    fn is_expired_by_metadata(&self, key: u64) -> Option<bool> {
        let shard_idx = Self::get_shard_index(key);
        let shard = self.metadata_shards[shard_idx].read();
        shard
            .get(&key)
            .map(|meta| Instant::now() >= meta.expires_at)
    }

    /// Remove a key from metadata tracking and update total weight.
    fn remove_metadata(&self, key: u64) -> Option<KeyMetadata> {
        let shard_idx = Self::get_shard_index(key);
        let mut shard = self.metadata_shards[shard_idx].write();
        let meta = shard.remove(&key)?;
        Some(meta)
    }

    /// Evict least-recently-used entries until the cache is back within its weight limit.
    ///
    /// pingora-lru never consults the limit on its own — `admit` only adds weight — so
    /// nothing bounds the cache unless this is called. Eviction is per shard and picks the
    /// coldest entry of each shard it visits, so the entries dropped approximate the
    /// least-recently-used set rather than ordering it globally.
    fn evict_to_weight_limit(&self) {
        for (entry, _) in self.cache.evict_to_limit() {
            // A concurrent `insert` can re-admit the key between its eviction above and
            // the metadata drop. Holding the shard across both the drop and the re-admit
            // check serialises this against `insert`, which publishes the value and its
            // metadata under the same lock: either the insert has already published and
            // both are dropped here, or it has not started and will publish both after.
            // Without the lock the insert could slip its metadata write in after this
            // branch and leave metadata naming a value that was just removed — a key
            // `len()`/`iter_keys()` still report but `get()` can never serve.
            let shard_idx = Self::get_shard_index(entry.key);
            let mut shard = self.metadata_shards[shard_idx].write();
            shard.remove(&entry.key);

            if self.cache.peek(entry.key) {
                self.cache.remove(entry.key);
            }
        }
    }
}

#[async_trait]
impl<V> CacheBackend<V> for PingoraBackend<V>
where
    V: Sizeable + Clone + Send + Sync + 'static,
{
    async fn insert(&self, key: u64, value: V) {
        // Calculate weight for the value
        let weight = value.get_memory_size();
        let expires_at = Instant::now() + self.ttl;

        let shard_idx = Self::get_shard_index(key);
        {
            // Publish the value and its metadata under one hold of the shard so a
            // concurrent eviction of this key cannot land between them (see
            // `evict_to_weight_limit`). `remove_metadata` is not reused here because the
            // lock is not reentrant.
            let mut shard = self.metadata_shards[shard_idx].write();

            // If key already exists, remove old metadata first to update weight correctly
            if shard.remove(&key).is_some() {
                // Remove from pingora-lru as well (admit will re-add)
                let _ = self.cache.remove(key);
            }

            // Store the value in pingora-lru, keyed so an eviction can find its metadata
            self.cache.admit(key, KeyedValue { key, value }, weight);

            shard.insert(key, KeyMetadata { expires_at });
        }

        // Admitting is what pushes the cache over its limit, so bring it back under.
        // A value heavier than the whole limit is evicted again here, as it is on Moka.
        self.evict_to_weight_limit();
    }

    async fn get(&self, key: &u64) -> Option<V> {
        // First, check metadata for expiration without touching pingora-lru
        // This avoids the race condition for expired items
        match self.is_expired_by_metadata(*key) {
            None => {
                // Key doesn't exist in metadata, so it's not in the cache
                return None;
            }
            Some(true) => {
                // Key is expired - remove from both metadata and pingora-lru
                self.remove_metadata(*key);
                self.cache.remove(*key);
                return None;
            }
            Some(false) => {
                // Key exists and is not expired - proceed to get value
            }
        }

        // NOTE: pingora-lru doesn't have a peek_value() API, only peek() which returns bool.
        // We must use remove() to get the value, then re-admit it to maintain LRU ordering.
        // There's a brief race window here where concurrent requests may see a cache miss.
        // This is acceptable because:
        // 1. The window is extremely small (single-digit microseconds)
        // 2. We already verified the item isn't expired (no unnecessary re-admission)
        // 3. Overall system throughput is 2-3x higher than alternatives
        // 4. Cache misses are handled gracefully by upstream code
        let (entry, weight) = self.cache.remove(*key)?;

        // Re-admit to maintain the value in cache (promotes to head of LRU).
        // The weight is unchanged, so this cannot push the cache over its limit.
        let cloned_value = entry.value.clone();
        self.cache.admit(*key, entry, weight);

        Some(cloned_value)
    }

    async fn remove(&self, key: &u64) -> Option<V> {
        let shard_idx = Self::get_shard_index(*key);

        // Drop the metadata and the value under one hold of the shard, for the same reason
        // `insert` publishes them under one hold: a concurrent `insert` of this key that
        // landed between the two would leave its metadata behind while this call removed
        // the value it names — a key `len()`/`iter_keys()` still report but `get()` can
        // never serve. `remove_metadata` is not reused here because the lock is not
        // reentrant.
        let mut shard = self.metadata_shards[shard_idx].write();
        shard.remove(key);

        self.cache.remove(*key).map(|(entry, _)| entry.value)
    }

    async fn clear(&self) {
        // Collect all keys from metadata shards
        // We must lock all shards for writing to ensure they clear without a new insert racing before the clear
        let shard_locks = self
            .metadata_shards
            .as_ref()
            .iter()
            .map(|shard| shard.write())
            .collect::<Vec<_>>();
        let keys: Vec<u64> = {
            let mut all_keys = Vec::new();
            for shard in &shard_locks {
                all_keys.extend(shard.keys().copied());
            }
            all_keys
        };

        // Remove each key from pingora-lru
        for key in keys {
            self.cache.remove(key);
        }

        // Clear all metadata shards
        for mut shard in shard_locks {
            shard.clear();
        }
    }

    async fn iter_keys(&self) -> Vec<u64> {
        let mut all_keys = Vec::new();
        for shard in self.metadata_shards.as_ref() {
            all_keys.extend(shard.read().keys().copied());
        }
        all_keys
    }

    async fn len(&self) -> usize {
        self.metadata_shards
            .iter()
            .map(|shard| shard.read().len())
            .sum()
    }

    async fn weighted_size(&self) -> u64 {
        // Use pingora-lru's native weight tracking for accuracy
        // This reflects the actual weight tracked by pingora-lru
        self.cache.weight() as u64
    }

    async fn run_pending_tasks(&self) {
        // `insert` already evicts, so this is normally a no-op. It still runs the sweep
        // because the metrics path and `LruCache::checkpoint` call it to read a settled
        // size, and because a cache that stopped being written to should not hold weight
        // it has been asked to give up.
        self.evict_to_weight_limit();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Simple test value that implements Sizeable
    #[derive(Clone, Debug, PartialEq)]
    struct TestValue {
        data: String,
        size: usize,
    }

    impl TestValue {
        fn new(data: &str) -> Self {
            let size = data.len();
            Self {
                data: data.to_string(),
                size,
            }
        }

        fn with_size(data: &str, size: usize) -> Self {
            Self {
                data: data.to_string(),
                size,
            }
        }
    }

    impl Sizeable for TestValue {
        fn get_memory_size(&self) -> usize {
            self.size
        }
    }

    fn create_backend(capacity: u64, ttl_secs: u64) -> PingoraBackend<TestValue> {
        PingoraBackend::with_params(capacity, Duration::from_secs(ttl_secs))
    }

    fn create_backend_millis(capacity: u64, ttl_millis: u64) -> PingoraBackend<TestValue> {
        PingoraBackend::with_params(capacity, Duration::from_millis(ttl_millis))
    }

    // ===================
    // insert() tests
    // ===================

    #[tokio::test]
    async fn test_insert_single_value() {
        let backend = create_backend(1024, 60);
        let key = 1u64;
        let value = TestValue::new("test_data");

        backend.insert(key, value.clone()).await;

        let retrieved = backend.get(&key).await;
        assert_eq!(retrieved, Some(value));
    }

    #[tokio::test]
    async fn test_insert_multiple_values() {
        let backend = create_backend(1024, 60);

        for i in 0..10 {
            backend
                .insert(i, TestValue::new(&format!("value_{i}")))
                .await;
        }

        assert_eq!(backend.len().await, 10);

        for i in 0..10 {
            let retrieved = backend.get(&i).await;
            assert_eq!(retrieved, Some(TestValue::new(&format!("value_{i}"))));
        }
    }

    #[tokio::test]
    async fn test_insert_overwrites_existing_key() {
        let backend = create_backend(1024, 60);
        let key = 42u64;

        backend.insert(key, TestValue::new("original")).await;
        assert_eq!(backend.get(&key).await, Some(TestValue::new("original")));

        backend.insert(key, TestValue::new("updated")).await;
        assert_eq!(backend.get(&key).await, Some(TestValue::new("updated")));

        // Should still be only one entry
        assert_eq!(backend.len().await, 1);
    }

    #[tokio::test]
    async fn test_insert_updates_weight_on_overwrite() {
        let backend = create_backend(1024, 60);
        let key = 1u64;

        // Insert with size 100
        backend
            .insert(key, TestValue::with_size("small", 100))
            .await;
        let weight_after_first = backend.weighted_size().await;

        // Overwrite with size 500
        backend
            .insert(key, TestValue::with_size("large", 500))
            .await;
        let weight_after_second = backend.weighted_size().await;

        // Weight should reflect only the new value, not accumulated
        assert_eq!(weight_after_first, 100);
        assert_eq!(weight_after_second, 500);
    }

    #[tokio::test]
    async fn test_insert_keys_across_multiple_shards() {
        let backend = create_backend(1024, 60);

        // Insert keys that will be distributed across different shards
        // Keys 0-15 will each go to a different shard (key % 16)
        for i in 0..16 {
            backend
                .insert(i, TestValue::new(&format!("shard_{i}")))
                .await;
        }

        assert_eq!(backend.len().await, 16);

        // Verify all values are retrievable
        for i in 0..16 {
            let retrieved = backend.get(&i).await;
            assert_eq!(retrieved, Some(TestValue::new(&format!("shard_{i}"))));
        }
    }

    // ===================
    // get() tests
    // ===================

    #[tokio::test]
    async fn test_get_existing_key() {
        let backend = create_backend(1024, 60);
        let key = 1u64;
        let value = TestValue::new("test_value");

        backend.insert(key, value.clone()).await;

        let retrieved = backend.get(&key).await;
        assert_eq!(retrieved, Some(value));
    }

    #[tokio::test]
    async fn test_get_nonexistent_key() {
        let backend = create_backend(1024, 60);

        let retrieved = backend.get(&999u64).await;
        assert_eq!(retrieved, None);
    }

    #[tokio::test]
    async fn test_get_expired_key_returns_none() {
        let backend = create_backend_millis(1024, 50); // 50ms TTL
        let key = 1u64;

        backend.insert(key, TestValue::new("expires_soon")).await;

        // Value should exist immediately
        assert!(backend.get(&key).await.is_some());

        // Wait for TTL to expire
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Value should be expired now
        assert_eq!(backend.get(&key).await, None);
    }

    #[tokio::test]
    async fn test_get_expired_key_removes_from_metadata() {
        let backend = create_backend_millis(1024, 50); // 50ms TTL
        let key = 1u64;

        backend.insert(key, TestValue::new("expires_soon")).await;
        assert_eq!(backend.len().await, 1);

        // Wait for TTL to expire
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Access the expired key (should trigger cleanup)
        let _ = backend.get(&key).await;

        // Metadata should be cleaned up
        assert_eq!(backend.len().await, 0);
    }

    #[tokio::test]
    async fn test_get_multiple_times_returns_same_value() {
        let backend = create_backend(1024, 60);
        let key = 1u64;
        let value = TestValue::new("consistent_value");

        backend.insert(key, value.clone()).await;

        // Get multiple times
        for _ in 0..5 {
            let retrieved = backend.get(&key).await;
            assert_eq!(retrieved, Some(value.clone()));
        }
    }

    // ===================
    // remove() tests
    // ===================

    #[tokio::test]
    async fn test_remove_existing_key() {
        let backend = create_backend(1024, 60);
        let key = 1u64;
        let value = TestValue::new("to_remove");

        backend.insert(key, value.clone()).await;

        let removed = backend.remove(&key).await;
        assert_eq!(removed, Some(value));

        // Key should no longer exist
        assert_eq!(backend.get(&key).await, None);
    }

    #[tokio::test]
    async fn test_remove_nonexistent_key() {
        let backend = create_backend(1024, 60);

        let removed = backend.remove(&999u64).await;
        assert_eq!(removed, None);
    }

    #[tokio::test]
    async fn test_remove_updates_len() {
        let backend = create_backend(1024, 60);

        backend.insert(1, TestValue::new("one")).await;
        backend.insert(2, TestValue::new("two")).await;
        backend.insert(3, TestValue::new("three")).await;

        assert_eq!(backend.len().await, 3);

        backend.remove(&2).await;
        assert_eq!(backend.len().await, 2);

        backend.remove(&1).await;
        assert_eq!(backend.len().await, 1);

        backend.remove(&3).await;
        assert_eq!(backend.len().await, 0);
    }

    #[tokio::test]
    async fn test_remove_updates_weighted_size() {
        let backend = create_backend(1024, 60);

        backend.insert(1, TestValue::with_size("a", 100)).await;
        backend.insert(2, TestValue::with_size("b", 200)).await;

        assert_eq!(backend.weighted_size().await, 300);

        backend.remove(&1).await;
        assert_eq!(backend.weighted_size().await, 200);

        backend.remove(&2).await;
        assert_eq!(backend.weighted_size().await, 0);
    }

    #[tokio::test]
    async fn test_remove_double_remove_returns_none() {
        let backend = create_backend(1024, 60);
        let key = 1u64;

        backend.insert(key, TestValue::new("value")).await;

        let first_remove = backend.remove(&key).await;
        assert!(first_remove.is_some());

        let second_remove = backend.remove(&key).await;
        assert_eq!(second_remove, None);
    }

    // ===================
    // clear() tests
    // ===================

    #[tokio::test]
    async fn test_clear_empty_cache() {
        let backend = create_backend(1024, 60);

        backend.clear().await;

        assert_eq!(backend.len().await, 0);
        assert_eq!(backend.weighted_size().await, 0);
    }

    #[tokio::test]
    async fn test_clear_removes_all_entries() {
        let backend = create_backend(1024, 60);

        for i in 0..10 {
            backend
                .insert(i, TestValue::new(&format!("value_{i}")))
                .await;
        }

        assert_eq!(backend.len().await, 10);

        backend.clear().await;

        assert_eq!(backend.len().await, 0);

        // Verify no keys are accessible
        for i in 0..10 {
            assert_eq!(backend.get(&i).await, None);
        }
    }

    #[tokio::test]
    async fn test_clear_resets_weighted_size() {
        let backend = create_backend(1024, 60);

        backend.insert(1, TestValue::with_size("a", 100)).await;
        backend.insert(2, TestValue::with_size("b", 200)).await;

        assert!(backend.weighted_size().await > 0);

        backend.clear().await;

        assert_eq!(backend.weighted_size().await, 0);
    }

    #[tokio::test]
    async fn test_clear_allows_reinsertion() {
        let backend = create_backend(1024, 60);
        let key = 1u64;

        backend.insert(key, TestValue::new("original")).await;
        backend.clear().await;

        backend.insert(key, TestValue::new("new_value")).await;

        assert_eq!(backend.get(&key).await, Some(TestValue::new("new_value")));
        assert_eq!(backend.len().await, 1);
    }

    // ===================
    // weighted_size() tests
    // ===================

    #[tokio::test]
    async fn test_weighted_size_empty_cache() {
        let backend = create_backend(1024, 60);

        assert_eq!(backend.weighted_size().await, 0);
    }

    #[tokio::test]
    async fn test_weighted_size_single_entry() {
        let backend = create_backend(1024, 60);

        backend.insert(1, TestValue::with_size("data", 256)).await;

        assert_eq!(backend.weighted_size().await, 256);
    }

    #[tokio::test]
    async fn test_weighted_size_multiple_entries() {
        let backend = create_backend(1024, 60);

        backend.insert(1, TestValue::with_size("a", 100)).await;
        backend.insert(2, TestValue::with_size("b", 200)).await;
        backend.insert(3, TestValue::with_size("c", 300)).await;

        assert_eq!(backend.weighted_size().await, 600);
    }

    #[tokio::test]
    async fn test_weighted_size_after_remove() {
        let backend = create_backend(1024, 60);

        backend.insert(1, TestValue::with_size("a", 100)).await;
        backend.insert(2, TestValue::with_size("b", 200)).await;

        backend.remove(&1).await;

        assert_eq!(backend.weighted_size().await, 200);
    }

    #[tokio::test]
    async fn test_weighted_size_after_overwrite() {
        let backend = create_backend(1024, 60);
        let key = 1u64;

        backend
            .insert(key, TestValue::with_size("small", 100))
            .await;
        assert_eq!(backend.weighted_size().await, 100);

        backend
            .insert(key, TestValue::with_size("large", 500))
            .await;
        assert_eq!(backend.weighted_size().await, 500);
    }

    // ===================
    // iter_keys() tests
    // ===================

    #[tokio::test]
    async fn test_iter_keys_empty_cache() {
        let backend = create_backend(1024, 60);

        let keys = backend.iter_keys().await;
        assert!(keys.is_empty());
    }

    #[tokio::test]
    async fn test_iter_keys_returns_all_keys() {
        let backend = create_backend(1024, 60);

        let inserted_keys: Vec<u64> = vec![1, 5, 10, 100, 1000];
        for &key in &inserted_keys {
            backend.insert(key, TestValue::new("value")).await;
        }

        let mut retrieved_keys = backend.iter_keys().await;
        retrieved_keys.sort_unstable();

        assert_eq!(retrieved_keys, inserted_keys);
    }

    #[tokio::test]
    async fn test_iter_keys_after_remove() {
        let backend = create_backend(1024, 60);

        backend.insert(1, TestValue::new("one")).await;
        backend.insert(2, TestValue::new("two")).await;
        backend.insert(3, TestValue::new("three")).await;

        backend.remove(&2).await;

        let mut keys = backend.iter_keys().await;
        keys.sort_unstable();

        assert_eq!(keys, vec![1, 3]);
    }

    #[tokio::test]
    async fn test_iter_keys_after_clear() {
        let backend = create_backend(1024, 60);

        for i in 0..5 {
            backend.insert(i, TestValue::new("value")).await;
        }

        backend.clear().await;

        let keys = backend.iter_keys().await;
        assert!(keys.is_empty());
    }

    #[tokio::test]
    async fn test_iter_keys_across_shards() {
        let backend = create_backend(1024, 60);

        // Insert keys across all 16 shards
        for i in 0..32 {
            backend
                .insert(i, TestValue::new(&format!("value_{i}")))
                .await;
        }

        let mut keys = backend.iter_keys().await;
        keys.sort_unstable();

        let expected: Vec<u64> = (0..32).collect();
        assert_eq!(keys, expected);
    }

    // ===================
    // len() tests
    // ===================

    #[tokio::test]
    async fn test_len_empty_cache() {
        let backend = create_backend(1024, 60);

        assert_eq!(backend.len().await, 0);
    }

    #[tokio::test]
    async fn test_len_after_inserts() {
        let backend = create_backend(1024, 60);

        for i in 0..5 {
            backend.insert(i, TestValue::new("value")).await;
            assert_eq!(
                backend.len().await,
                usize::try_from(i + 1).expect("Should be usize")
            );
        }
    }

    #[tokio::test]
    async fn test_len_after_removes() {
        let backend = create_backend(1024, 60);

        for i in 0..5 {
            backend.insert(i, TestValue::new("value")).await;
        }

        for i in 0..5 {
            backend.remove(&i).await;
            assert_eq!(
                backend.len().await,
                usize::try_from(4 - i).expect("Should be usize")
            );
        }
    }

    #[tokio::test]
    async fn test_len_overwrite_does_not_increase() {
        let backend = create_backend(1024, 60);
        let key = 1u64;

        backend.insert(key, TestValue::new("first")).await;
        assert_eq!(backend.len().await, 1);

        backend.insert(key, TestValue::new("second")).await;
        assert_eq!(backend.len().await, 1);

        backend.insert(key, TestValue::new("third")).await;
        assert_eq!(backend.len().await, 1);
    }

    #[tokio::test]
    async fn test_len_after_clear() {
        let backend = create_backend(1024, 60);

        for i in 0..10 {
            backend.insert(i, TestValue::new("value")).await;
        }

        backend.clear().await;

        assert_eq!(backend.len().await, 0);
    }

    // ===================
    // TTL expiration tests
    // ===================

    #[tokio::test]
    async fn test_ttl_value_accessible_before_expiry() {
        let backend = create_backend_millis(1024, 200); // 200ms TTL
        let key = 1u64;

        backend.insert(key, TestValue::new("value")).await;

        // Should be accessible immediately
        assert!(backend.get(&key).await.is_some());

        // Should still be accessible after short delay
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(backend.get(&key).await.is_some());
    }

    #[tokio::test]
    async fn test_ttl_multiple_keys_expire_independently() {
        let backend = create_backend_millis(1024, 100); // 100ms TTL

        backend.insert(1, TestValue::new("first")).await;

        tokio::time::sleep(Duration::from_millis(60)).await;

        backend.insert(2, TestValue::new("second")).await;

        tokio::time::sleep(Duration::from_millis(60)).await;

        // Key 1 should be expired (inserted 120ms ago)
        assert!(backend.get(&1).await.is_none());

        // Key 2 should still be valid (inserted 60ms ago)
        assert!(backend.get(&2).await.is_some());
    }

    // ===================
    // Edge case tests
    // ===================

    #[tokio::test]
    async fn test_large_key_values() {
        let backend = create_backend(1024 * 1024, 60); // 1MB capacity

        let large_key = u64::MAX;
        backend.insert(large_key, TestValue::new("max_key")).await;

        assert_eq!(
            backend.get(&large_key).await,
            Some(TestValue::new("max_key"))
        );
    }

    #[tokio::test]
    async fn test_zero_key() {
        let backend = create_backend(1024, 60);

        backend.insert(0, TestValue::new("zero_key")).await;

        assert_eq!(backend.get(&0).await, Some(TestValue::new("zero_key")));
    }

    #[tokio::test]
    async fn test_is_empty() {
        let backend = create_backend(1024, 60);

        assert!(backend.is_empty().await);

        backend.insert(1, TestValue::new("value")).await;
        assert!(!backend.is_empty().await);

        backend.remove(&1).await;
        assert!(backend.is_empty().await);
    }

    // ===================
    // max_size / eviction tests
    // ===================

    /// Collect the keys of `range` that the cache still serves.
    async fn present_keys(
        backend: &PingoraBackend<TestValue>,
        range: std::ops::Range<u64>,
    ) -> Vec<u64> {
        let mut present = Vec::new();
        for key in range {
            if backend.get(&key).await.is_some() {
                present.push(key);
            }
        }
        present
    }

    #[tokio::test]
    async fn insert_evicts_until_the_cache_fits_its_max_size() {
        // 100 weight units of room, fed 50 entries that each fill it exactly.
        let backend = create_backend(100, 60);

        for i in 0..50u64 {
            backend.insert(i, TestValue::with_size("x", 100)).await;
        }

        assert_eq!(
            backend.weighted_size().await,
            100,
            "the cache should hold its configured weight, not a multiple of it"
        );
        assert_eq!(
            backend.len().await,
            1,
            "only the entries that fit in max_size should be resident"
        );
        assert_eq!(
            present_keys(&backend, 0..50).await.len(),
            1,
            "exactly one of the 50 inserted entries should still be served"
        );
    }

    #[tokio::test]
    async fn eviction_drops_the_metadata_of_evicted_keys() {
        // Room for three entries of weight 100.
        let backend = create_backend(300, 60);

        for i in 0..10u64 {
            backend.insert(i, TestValue::with_size("x", 100)).await;
        }

        let mut reported = backend.iter_keys().await;
        reported.sort_unstable();
        let mut served = present_keys(&backend, 0..10).await;
        served.sort_unstable();

        assert_eq!(
            reported, served,
            "iter_keys should name the entries the cache can serve, not ones it evicted"
        );
        assert_eq!(backend.len().await, 3);
        assert_eq!(backend.weighted_size().await, 300);
    }

    #[tokio::test]
    async fn a_value_heavier_than_max_size_is_not_retained() {
        let backend = create_backend(100, 60);

        backend
            .insert(1, TestValue::with_size("oversized", 500))
            .await;

        assert_eq!(backend.get(&1).await, None);
        assert_eq!(
            backend.weighted_size().await,
            0,
            "a value that cannot fit the budget should not be left occupying it"
        );
        assert_eq!(backend.len().await, 0);
    }

    #[tokio::test]
    async fn entries_within_max_size_are_not_evicted() {
        let backend = create_backend(1000, 60);

        for i in 0..5u64 {
            backend.insert(i, TestValue::with_size("x", 100)).await;
        }

        assert_eq!(backend.len().await, 5);
        assert_eq!(backend.weighted_size().await, 500);
        assert_eq!(present_keys(&backend, 0..5).await, vec![0, 1, 2, 3, 4]);
    }

    #[tokio::test]
    async fn run_pending_tasks_leaves_a_cache_that_already_fits_alone() {
        let backend = create_backend(1000, 60);

        for i in 0..5u64 {
            backend.insert(i, TestValue::with_size("x", 100)).await;
        }

        backend.run_pending_tasks().await;

        assert_eq!(backend.len().await, 5);
        assert_eq!(backend.weighted_size().await, 500);
    }

    #[tokio::test]
    async fn a_large_max_size_does_not_reserve_its_entries_up_front() {
        // The reservation is an item count. Deriving it from this byte budget would ask
        // each of the 16 shards to reserve 67 million entries before anything is cached.
        let backend = create_backend(1024 * 1024 * 1024, 60);

        backend.insert(1, TestValue::new("value")).await;

        assert_eq!(backend.get(&1).await, Some(TestValue::new("value")));
    }
}
