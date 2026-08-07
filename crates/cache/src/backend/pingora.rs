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
use crate::metrics::{CacheMetrics, EvictionReason};
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
/// pingora-lru reports only values and weights, both when it evicts and when it hands
/// entries to an in-place scan ([`Lru::iter_for_each`]), so the key travels with the
/// value. Without it an eviction could not name the metadata entry it has to drop,
/// `len()`/`iter_keys()` would keep reporting keys whose values are gone, and
/// [`PingoraBackend::keys_matching`] could not name what it matched.
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
    V: Sizeable + CacheMetrics + Clone + Send + Sync + 'static,
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

    /// Drops `key`'s metadata and its value under the caller's hold of `key`'s shard,
    /// returning the value if one came out.
    ///
    /// Both drops belong to one hold: a concurrent `insert` of this key that landed
    /// between them would have its fresh metadata left behind while this call removed the
    /// value it names — a key `len()`/`iter_keys()` still report but `get()` can never
    /// serve — and its value would be discarded even though the write reported success.
    /// Taking the guard as an argument is what keeps that pairing checkable: every caller
    /// holds the shard across both, and each keeps its own acquisition, which
    /// [`Self::remove_if_expired`] needs because its expiry re-check has to be under the
    /// same non-reentrant hold.
    ///
    /// The value is handed back rather than dropped here so its destructor runs after the
    /// caller releases the shard. Dropping a large value inside the hold would block every
    /// reader and writer mapped to that shard, and a destructor that re-entered the cache
    /// would deadlock on the non-reentrant lock.
    fn drop_entry_locked(
        cache: &Lru<KeyedValue<V>, NUM_KEY_SHARDS>,
        shard: &mut HashMap<u64, KeyMetadata>,
        key: u64,
    ) -> Option<KeyedValue<V>> {
        shard.remove(&key);
        cache.remove(key).map(|(entry, _)| entry)
    }

    /// Drop an entry that was observed expired, provided it is still expired once the
    /// shard is held. Returns whether the entry was removed.
    ///
    /// The expiry is re-checked here because the caller observed it under a read lock it
    /// has since released. An `insert` in that gap has already published a live entry, so
    /// the observation is stale and the entry is left alone.
    fn remove_if_expired(&self, key: u64) -> bool {
        let shard_idx = Self::get_shard_index(key);
        let mut shard = self.metadata_shards[shard_idx].write();

        // Already gone, or made live again by an insert since the observation.
        if shard
            .get(&key)
            .is_none_or(|meta| Instant::now() < meta.expires_at)
        {
            return false;
        }

        let removed = Self::drop_entry_locked(&self.cache, &mut shard, key);
        drop(shard);

        // As in `evict_to_weight_limit`, no listener reports this engine's own
        // removals, so an expiry only reaches the counter if it is recorded here.
        // Counted only when the value actually came out of the cache: metadata can
        // briefly outlive the value while a size eviction (which has already counted
        // that removal) is still on its way to dropping the metadata.
        if removed.is_some() {
            V::record_eviction(EvictionReason::Expired);
        }
        drop(removed);
        true
    }

    /// Evict least-recently-used entries until the cache is back within its weight limit.
    ///
    /// pingora-lru never consults the limit on its own — `admit` only adds weight — so
    /// nothing bounds the cache unless this is called. Eviction is per shard and picks the
    /// coldest entry of each shard it visits, so the entries dropped approximate the
    /// least-recently-used set rather than ordering it globally.
    fn evict_to_weight_limit(&self) {
        for (entry, _) in self.cache.evict_to_limit() {
            // `evict_to_limit` has already taken this entry out under the weight limit,
            // so the removal is a fact by the time it is yielded here. Nothing else
            // counts it: the eviction listener that reports a moka removal belongs to
            // the moka cache, which this engine does not have, so a removal this
            // backend performs is only observable if it is recorded at its own call
            // site. Counted once per yielded entry — the `remove` below only drops a
            // key a concurrent `insert` re-admitted, which is a second removal of a
            // different admission rather than a second eviction of this one.
            V::record_eviction(EvictionReason::Size);

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

    /// The keys whose values satisfy `predicate`, most-recently-used first within each
    /// shard.
    ///
    /// Each shard is read in place, under that shard's read lock: the values are inspected
    /// where they sit, so the walk leaves both LRU recency and every entry's visibility to
    /// a concurrent reader untouched. pingora-lru's only by-key read is destructive
    /// (`remove` + re-`admit`), which is why matching goes through the in-place scan and
    /// [`KeyedValue`] carries the key the scan needs to name what it matched.
    ///
    /// What it costs instead is one contiguous hold of the shard being walked, so a hit or
    /// an insert mapping to that shard waits for the rest of its walk: `predicate` runs
    /// once per entry, and one shard holds about a sixteenth of the cache. That is why
    /// callers run this off the runtime worker, and why `predicate` should stay cheap —
    /// the two value types that reach it, `CachedQueryResult` and `CachedSearchResult`,
    /// answer `AsTableRefs` with an `Arc` clone.
    ///
    /// An entry present when its shard is reached is always seen; one admitted into a
    /// shard after that shard has been walked is not.
    fn keys_matching<F>(&self, predicate: F) -> Vec<u64>
    where
        F: Fn(&V) -> bool,
    {
        let mut matched: Vec<u64> = Vec::new();
        for shard in 0..self.cache.shards() {
            self.cache.iter_for_each(shard, |(entry, _weight)| {
                if predicate(&entry.value) {
                    matched.push(entry.key);
                }
            });
        }
        matched
    }

    /// Removes every entry whose value satisfies `predicate`, returning how many entries
    /// were removed.
    ///
    /// The matching entries are found by [`Self::keys_matching`], which documents what the
    /// in-place scan does and does not see. Removal runs after the scan rather than inside
    /// it, because `iter_for_each` holds the shard being walked and removing from it would
    /// deadlock.
    ///
    /// The window `keys_matching` leaves — an entry admitted after its shard was walked —
    /// is the same one moka's predicate-based invalidation leaves, since a moka predicate
    /// only matches entries last modified before it was registered. For served results it
    /// is closed on the write side by `TableInvalidationClock`, which refuses a cache write
    /// whose read began before the invalidation. In the other direction, a key matched by
    /// the scan and then rewritten by a concurrent `insert` is removed on the strength of
    /// the value the scan saw, so a fresh entry can be dropped — a cache miss, never a
    /// stale hit.
    pub(crate) fn invalidate_matching<F>(&self, predicate: F) -> usize
    where
        F: Fn(&V) -> bool,
    {
        let mut removed = 0;
        for key in self.keys_matching(predicate) {
            let shard_idx = Self::get_shard_index(key);
            let mut shard = self.metadata_shards[shard_idx].write();
            let dropped = Self::drop_entry_locked(&self.cache, &mut shard, key);
            drop(shard);

            // As with expiry and size eviction, no listener reports this engine's own
            // removals, so an invalidation only reaches the counter if it is recorded
            // here. Counted once per value that actually came out of the cache: a key
            // whose value a concurrent size eviction already took (and counted) leaves
            // nothing to count here.
            if dropped.is_some() {
                removed += 1;
                V::record_eviction(EvictionReason::Invalidated);
            }
            drop(dropped);
        }
        removed
    }

    /// The keys held in the pingora-lru shards, most-recently-used first within
    /// each shard.
    ///
    /// Only the shards themselves record recency, so this is the only way a test can assert
    /// that an operation left LRU ordering alone.
    ///
    /// It walks the shards itself rather than calling [`Self::keys_matching`] with a
    /// match-everything predicate, even though that is the same loop. Sharing the walk would
    /// make the observation run through the code under test: a `keys_matching` that promoted
    /// the entries it visited would promote them for this helper too, and the test asserting
    /// that recency survived an invalidation would pass on a scan that destroyed it.
    #[cfg(test)]
    fn keys_in_lru_order(&self) -> Vec<u64> {
        let mut keys = Vec::new();
        for shard in 0..self.cache.shards() {
            self.cache
                .iter_for_each(shard, |(entry, _weight)| keys.push(entry.key));
        }
        keys
    }
}

#[async_trait]
impl<V> CacheBackend<V> for PingoraBackend<V>
where
    V: Sizeable + CacheMetrics + Clone + Send + Sync + 'static,
{
    async fn insert(&self, key: u64, value: V) {
        // Calculate weight for the value
        let weight = value.get_memory_size();
        let expires_at = Instant::now() + self.ttl;

        let shard_idx = Self::get_shard_index(key);
        {
            // Publish the value and its metadata under one hold of the shard so a
            // concurrent eviction of this key cannot land between them (see
            // `evict_to_weight_limit`). The shard is taken here rather than through a
            // helper because the metadata drop and the publish share one hold, and the
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
                // Key is expired - remove from both metadata and pingora-lru, under one
                // hold of the shard so a concurrent `insert` cannot land between them.
                // If that insert got there first the entry is live again and is left
                // alone; this `get` still reports the miss it was about to report.
                self.remove_if_expired(*key);
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
        // 3. Cache misses are handled gracefully by upstream code
        //
        // What is *not* acceptable is another request writing this key inside that window.
        // The pair below is a read expressed as a mutation, so an `insert` that completes
        // between the remove and the re-admit is undone by the re-admit: the old value goes
        // back over the new one, while the insert's metadata — and so the new entry's
        // expiry — stays. The cache then serves a value the writer replaced, for the TTL of
        // the replacement (#12838).
        //
        // So the pair goes under one hold of the key's shard, the lock every writer of an
        // entry — `insert`, `remove`, `remove_if_expired`, `evict_to_weight_limit`, `clear` —
        // takes for writing. An `insert` that lands before the hold is what `remove` returns
        // and what the re-admit puts back; one that arrives during it waits and then wins
        // outright.
        //
        // A *read* hold is what that needs, and all it needs: this path reads the shard's
        // metadata and never changes it, so excluding the writers is the whole requirement,
        // and holding it shared leaves concurrent hits — and `len()`/`iter_keys()` — running
        // in parallel as they did before. Two hits on one key can still each `remove` and
        // have one of them come back empty; that is the transient miss noted above, which
        // costs a re-fetch and cannot lose a write.
        //
        // The lock is taken in the order `insert` already takes it — shard, then the
        // pingora-lru shard underneath `remove`/`admit` — so it adds no new ordering against
        // eviction, which materialises its victims (`evict_to_limit` returns an owned `Vec`)
        // before it takes any shard.
        let shard_idx = Self::get_shard_index(*key);
        let _shard = self.metadata_shards[shard_idx].read();

        let (entry, weight) = self.cache.remove(*key)?;

        // Re-admit to maintain the value in cache (promotes to head of LRU).
        // The weight is unchanged, so this cannot push the cache over its limit.
        let cloned_value = entry.value.clone();
        self.cache.admit(*key, entry, weight);

        Some(cloned_value)
    }

    async fn remove(&self, key: &u64) -> Option<V> {
        let shard_idx = Self::get_shard_index(*key);
        let mut shard = self.metadata_shards[shard_idx].write();
        let removed = Self::drop_entry_locked(&self.cache, &mut shard, *key);
        drop(shard);
        removed.map(|entry| entry.value)
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
    use std::sync::atomic::{AtomicU64, Ordering};

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

    impl CacheMetrics for TestValue {
        fn record_hit() {}
        fn record_miss() {}
        fn record_request() {}
        fn record_item_count(_count: u64) {}
        fn record_size(_size: u64) {}
        fn record_max_size(_size: u64) {}
        fn record_eviction(_reason: EvictionReason) {}
        fn record_stale_rejection() {}
        fn update_hit_ratio(_hits: u64, _total: u64) {}
        fn publish_counters_at_zero() {}
    }

    /// A test value whose evictions are counted in-process, so a test can assert
    /// what the backend actually reported without standing up an `OpenTelemetry`
    /// pipeline. [`CacheMetrics`] is implemented on the type rather than on an
    /// instance, so each test needs its own type to keep a count only it can move.
    macro_rules! counting_value {
        ($name:ident, $counter:ident) => {
            static $counter: AtomicU64 = AtomicU64::new(0);

            #[derive(Clone, Debug, PartialEq)]
            struct $name(TestValue);

            impl Sizeable for $name {
                fn get_memory_size(&self) -> usize {
                    self.0.get_memory_size()
                }
            }

            impl CacheMetrics for $name {
                fn record_hit() {}
                fn record_miss() {}
                fn record_request() {}
                fn record_item_count(_count: u64) {}
                fn record_size(_size: u64) {}
                fn record_max_size(_size: u64) {}
                fn record_stale_rejection() {}
                fn update_hit_ratio(_hits: u64, _total: u64) {}
                fn publish_counters_at_zero() {}

                fn record_eviction(_reason: EvictionReason) {
                    $counter.fetch_add(1, Ordering::Relaxed);
                }
            }
        };
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

    /// Backdate a key's expiry so the next `get` takes the expiry arm without the
    /// test having to wait out a TTL.
    fn expire_now(backend: &PingoraBackend<TestValue>, key: u64) {
        let shard_idx = PingoraBackend::<TestValue>::get_shard_index(key);
        let mut shard = backend.metadata_shards[shard_idx].write();
        let meta = shard.get_mut(&key).expect("key has metadata to backdate");
        meta.expires_at = Instant::now()
            .checked_sub(Duration::from_millis(1))
            .expect("instant is within range");
    }

    #[tokio::test]
    async fn test_expiry_spares_an_entry_an_insert_made_live() {
        // The interleaving this guards: a `get` observes the key expired under the read
        // lock, releases it, and a concurrent `insert` republishes the key before the
        // removal runs. `remove_if_expired` stands in for that removal with the insert
        // already applied — the state the removal actually finds.
        //
        // Removing unconditionally here drops the metadata and the value the insert just
        // published, so a write that reported success is discarded and the key is absent
        // from the cache entirely. Both sides go under one hold of the shard, so no
        // observer sees them out of step — losing the write is the whole of the damage,
        // and it is what this test pins.
        let backend = create_backend(4096, 60);
        let key = 11u64;

        backend.insert(key, TestValue::new("stale")).await;
        expire_now(&backend, key);

        // The racing insert: it republishes the key with a live expiry.
        backend.insert(key, TestValue::new("fresh")).await;

        assert!(
            !backend.remove_if_expired(key),
            "a live entry must not be removed on a stale expiry observation"
        );

        assert_eq!(backend.len().await, 1);
        assert_eq!(backend.get(&key).await, Some(TestValue::new("fresh")));
    }

    #[tokio::test]
    async fn test_remove_if_expired_reports_what_it_did() {
        let backend = create_backend(4096, 60);

        // Absent key — nothing to remove.
        assert!(!backend.remove_if_expired(1));

        // Live key — left in place.
        backend.insert(2, TestValue::new("live")).await;
        assert!(!backend.remove_if_expired(2));
        assert_eq!(backend.get(&2).await, Some(TestValue::new("live")));

        // Lapsed key — removed.
        backend.insert(3, TestValue::new("stale")).await;
        expire_now(&backend, 3);
        assert!(backend.remove_if_expired(3));
        assert!(backend.get(&3).await.is_none());
    }

    #[tokio::test]
    async fn test_expired_get_removes_metadata_and_value_together() {
        let backend = create_backend(4096, 60);
        let key = 7u64;

        backend.insert(key, TestValue::new("stale")).await;
        expire_now(&backend, key);

        assert!(backend.get(&key).await.is_none());

        // Both sides of the entry are gone, so the key is absent from every view of the
        // cache rather than lingering in the one `len()`/`iter_keys()` read.
        assert_eq!(backend.len().await, 0);
        assert!(backend.iter_keys().await.is_empty());
        assert_eq!(backend.weighted_size().await, 0);
    }

    #[tokio::test]
    async fn test_expired_get_is_idempotent_when_the_value_is_already_gone() {
        let backend = create_backend(4096, 60);
        let key = 9u64;

        backend.insert(key, TestValue::new("stale")).await;
        expire_now(&backend, key);

        // Take the value out from under the expiry, leaving metadata that names nothing.
        backend.cache.remove(key);

        assert!(backend.get(&key).await.is_none());
        assert_eq!(backend.len().await, 0);
        assert!(backend.iter_keys().await.is_empty());
    }

    /// Parks a reader inside `get`'s remove → re-admit window, so a writer can be aimed at
    /// exactly the gap the bug lives in. `get` clones the value between the two calls, which
    /// is the only point in the window this backend hands control to the value type.
    #[derive(Debug)]
    struct GatedValue {
        data: String,
        /// Set only on the copy the cache holds, so the reader's own clone-of-a-clone and
        /// the writer's fresh value pass straight through.
        gate: Option<Arc<Gate>>,
    }

    #[derive(Debug)]
    struct Gate {
        entered: std::sync::mpsc::SyncSender<()>,
        release: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
    }

    impl Clone for GatedValue {
        fn clone(&self) -> Self {
            if let Some(gate) = &self.gate {
                gate.entered.send(()).expect("the test awaits this");
                gate.release
                    .lock()
                    .expect("gate mutex is not poisoned")
                    .recv()
                    .expect("the test releases this");
            }
            Self {
                data: self.data.clone(),
                gate: None,
            }
        }
    }

    impl Sizeable for GatedValue {
        fn get_memory_size(&self) -> usize {
            self.data.len()
        }
    }

    impl CacheMetrics for GatedValue {
        fn record_hit() {}
        fn record_miss() {}
        fn record_request() {}
        fn record_item_count(_count: u64) {}
        fn record_size(_size: u64) {}
        fn record_max_size(_size: u64) {}
        fn record_eviction(_reason: EvictionReason) {}
        fn record_stale_rejection() {}
        fn update_hit_ratio(_hits: u64, _total: u64) {}
        fn publish_counters_at_zero() {}
    }

    /// The hit path reads by removing the value and re-admitting it. An `insert` that
    /// completes between the two is then undone by the re-admit: the old value goes back
    /// over the new one, and the new one's metadata stays, so the cache serves the replaced
    /// value for the replacement's TTL (#12838).
    ///
    /// Driven through the gate above rather than by racing two tasks and hoping: the window
    /// is a few microseconds wide, so a stress test reports the bug only occasionally and
    /// passes on the broken code most runs.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_hit_cannot_re_admit_over_a_concurrent_insert() {
        let backend = Arc::new(PingoraBackend::<GatedValue>::with_params(
            4096,
            Duration::from_mins(1),
        ));
        let key = 11u64;

        let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel::<()>(1);
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        backend
            .insert(
                key,
                GatedValue {
                    data: "old".to_string(),
                    gate: Some(Arc::new(Gate {
                        entered: entered_tx,
                        release: std::sync::Mutex::new(release_rx),
                    })),
                },
            )
            .await;

        let reader = Arc::clone(&backend);
        let hit = tokio::spawn(async move { reader.get(&key).await });
        entered_rx
            .recv()
            .expect("the reader reaches the re-admit window");

        // The reader is now holding the value out of the cache, mid-read.
        let writer = Arc::clone(&backend);
        let mut wrote = tokio::spawn(async move {
            writer
                .insert(
                    key,
                    GatedValue {
                        data: "new".to_string(),
                        gate: None,
                    },
                )
                .await;
        });

        // The insert must not be able to land while the read is mid-flight; if it does, the
        // re-admit below puts "old" back on top of it.
        assert!(
            tokio::time::timeout(Duration::from_millis(250), &mut wrote)
                .await
                .is_err(),
            "an insert completed inside the hit path's remove/re-admit window"
        );

        release_tx.send(()).expect("the reader is waiting on this");
        hit.await.expect("the read task does not panic");
        wrote.await.expect("the write task does not panic");

        let served = backend
            .get(&key)
            .await
            .expect("the key is cached after both operations");
        assert_eq!(
            served.data, "new",
            "the reader's re-admit undid the concurrent insert"
        );
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

    // ==========================
    // eviction reporting (#12792)
    // ==========================

    counting_value!(ExpiryCountedValue, EXPIRY_EVICTIONS);

    /// Regression test for #12792.
    ///
    /// `get` removes a lapsed entry inline. Eviction counting on the Moka engine
    /// rides on moka's `eviction_listener`; this engine has none, so a removal it
    /// performs itself is only counted if it is recorded at the call site. Before
    /// the fix the expiry series stayed at zero however many entries lapsed.
    #[tokio::test]
    async fn an_expiry_this_engine_removes_is_counted() {
        let backend: PingoraBackend<ExpiryCountedValue> =
            PingoraBackend::with_params(1024, Duration::from_millis(10));

        backend
            .insert(1, ExpiryCountedValue(TestValue::new("value")))
            .await;
        assert_eq!(
            EXPIRY_EVICTIONS.load(Ordering::Relaxed),
            0,
            "an entry that is still live must not be counted as evicted"
        );

        tokio::time::sleep(Duration::from_millis(30)).await;

        assert_eq!(
            backend.get(&1).await,
            None,
            "the entry has outlived its TTL, so it must not be served"
        );
        assert_eq!(
            EXPIRY_EVICTIONS.load(Ordering::Relaxed),
            1,
            "the expiry `get` removed must reach the eviction counter"
        );
    }

    counting_value!(MissCountedValue, MISS_EVICTIONS);

    /// A `get` for a key the cache never held removes nothing, so it must not be
    /// counted. Without this the counter would report evictions for plain misses,
    /// which is the same false reading as the zero it replaces.
    #[tokio::test]
    async fn a_miss_on_an_absent_key_is_not_an_eviction() {
        let backend: PingoraBackend<MissCountedValue> =
            PingoraBackend::with_params(1024, Duration::from_mins(1));

        assert_eq!(backend.get(&404).await, None);

        assert_eq!(
            MISS_EVICTIONS.load(Ordering::Relaxed),
            0,
            "a key the cache never held was not evicted"
        );
    }

    counting_value!(SizeCountedValue, SIZE_EVICTIONS);

    /// Regression test for #12792.
    ///
    /// The size eviction introduced with #12694 brings the cache back under its
    /// weight limit, and had the same shape as the expiry above: it removed
    /// entries and recorded nothing, so `reason="size"` stayed a false zero on
    /// this engine.
    #[tokio::test]
    async fn an_eviction_down_to_max_size_is_counted() {
        // Three 100-byte values against a 250-byte budget: the third admission
        // pushes the cache over, so at least one entry has to go.
        let backend: PingoraBackend<SizeCountedValue> =
            PingoraBackend::with_params(250, Duration::from_mins(1));

        for i in 0..3u64 {
            backend
                .insert(i, SizeCountedValue(TestValue::with_size("x", 100)))
                .await;
        }

        assert!(
            backend.weighted_size().await <= 250,
            "the cache must be back within its weight limit"
        );

        let evicted = SIZE_EVICTIONS.load(Ordering::Relaxed);
        assert!(
            evicted > 0,
            "an eviction down to max_size must reach the counter, got {evicted}"
        );
        assert_eq!(
            u64::try_from(3 - backend.len().await).unwrap_or(u64::MAX),
            evicted,
            "every entry the cache no longer holds must be accounted for exactly once"
        );
    }

    // ===================
    // invalidate_matching() tests
    // ===================

    /// Keys that all land in shard 0 (`key % 16`), so their relative LRU order
    /// is observable through `keys_in_lru_order`.
    const SHARD_ZERO_KEYS: [u64; 5] = [16, 32, 48, 64, 80];

    async fn backend_with_shard_zero_keys() -> PingoraBackend<TestValue> {
        let backend = create_backend(1024, 60);
        for key in SHARD_ZERO_KEYS {
            backend
                .insert(key, TestValue::new(&format!("v{key}")))
                .await;
        }
        backend
    }

    #[tokio::test]
    async fn test_invalidate_matching_removes_only_matching_entries() {
        let backend = backend_with_shard_zero_keys().await;

        let removed = backend.invalidate_matching(|value| value.data == "v48");

        assert_eq!(removed, 1);
        assert_eq!(backend.get(&48).await, None);
        for key in SHARD_ZERO_KEYS.into_iter().filter(|key| *key != 48) {
            assert_eq!(
                backend.get(&key).await,
                Some(TestValue::new(&format!("v{key}"))),
                "key {key} should have survived an invalidation that did not match it"
            );
        }
    }

    /// A predicate matching nothing must leave the cache exactly as it found it.
    #[tokio::test]
    async fn test_invalidate_matching_with_no_matches_removes_nothing() {
        let backend = backend_with_shard_zero_keys().await;
        let before = backend.keys_in_lru_order();

        let removed = backend.invalidate_matching(|value| value.data == "absent");

        assert_eq!(removed, 0);
        assert_eq!(backend.len().await, SHARD_ZERO_KEYS.len());
        assert_eq!(backend.keys_in_lru_order(), before);
    }

    /// Regression test for #12674: invalidation must not perturb LRU recency.
    ///
    /// Reading a value out of pingora-lru removes and re-admits it, so an
    /// invalidation that inspects entries with `get` promotes every key it
    /// visits and replaces recency with scan order across the whole cache.
    #[tokio::test]
    async fn test_invalidate_matching_preserves_lru_order() {
        let backend = backend_with_shard_zero_keys().await;

        // Read the oldest key so recency no longer matches insertion order —
        // otherwise a scan that rebuilt the order could coincidentally match.
        assert_eq!(backend.get(&16).await, Some(TestValue::new("v16")));
        assert_eq!(
            backend.keys_in_lru_order(),
            vec![16, 80, 64, 48, 32],
            "most-recently-used first"
        );

        let removed = backend.invalidate_matching(|value| value.data == "v48");
        assert_eq!(removed, 1);

        assert_eq!(
            backend.keys_in_lru_order(),
            vec![16, 80, 64, 32],
            "the surviving entries must keep the recency they had before the invalidation"
        );
    }

    /// Every removal must clear the metadata shard too, or the key keeps being
    /// reported by `len`/`iter_keys` after it is gone from the LRU.
    #[tokio::test]
    async fn test_invalidate_matching_clears_metadata() {
        let backend = backend_with_shard_zero_keys().await;

        let removed = backend.invalidate_matching(|value| value.data != "v16");

        assert_eq!(removed, 4);
        assert_eq!(backend.len().await, 1);
        assert_eq!(backend.iter_keys().await, vec![16]);
    }

    /// An entry past its TTL is still holding memory, so a matching invalidation
    /// must drop it rather than leave it for a read that may never come.
    #[tokio::test]
    async fn test_invalidate_matching_removes_expired_entries() {
        let backend = create_backend(1024, 60);
        backend.insert(1, TestValue::new("expiring")).await;
        expire_now(&backend, 1);

        let removed = backend.invalidate_matching(|value| value.data == "expiring");

        assert_eq!(removed, 1);
        assert_eq!(backend.len().await, 0);
    }

    /// The scan has to reach every shard, not just the one a small fixture
    /// happens to populate.
    #[tokio::test]
    async fn test_invalidate_matching_spans_all_shards() {
        let backend = create_backend(4096, 60);
        for key in 0..64u64 {
            backend
                .insert(
                    key,
                    TestValue::new(if key % 2 == 0 { "even" } else { "odd" }),
                )
                .await;
        }

        let removed = backend.invalidate_matching(|value| value.data == "even");

        assert_eq!(removed, 32);
        assert_eq!(backend.len().await, 32);
        for key in 0..64u64 {
            let expected = (key % 2 == 1).then(|| TestValue::new("odd"));
            assert_eq!(backend.get(&key).await, expected, "key {key}");
        }
    }
}
