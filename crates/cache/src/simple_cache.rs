/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use crate::{AsTableRefs, CacheProvider, HashProvider, Result, TabledCacheProvider};
use async_trait::async_trait;
use byte_unit::Byte;
use datafusion::sql::TableReference;
use parking_lot::RwLock;
use pingora_lru::Lru;
use std::collections::HashSet;
use std::fmt::Display;
use std::hash::{BuildHasher, Hasher};
use std::sync::Arc;
use std::time::{Duration, Instant};

const NUM_SHARDS: usize = 16;

#[inline]
#[allow(clippy::cast_possible_truncation)] // Modulo ensures result < 16
fn get_shard(key: u64) -> usize {
    (key as usize) % NUM_SHARDS
}

struct CachedValue<V> {
    value: V,
    inserted_at: Instant,
}

pub struct SimpleCache<
    V: Clone + Send + Sync + 'static,
    T: BuildHasher + Clone + Send + Sync + 'static,
> {
    cache: Lru<CachedValue<V>, NUM_SHARDS>,
    key_shards: [RwLock<HashSet<u64>>; NUM_SHARDS],
    hasher: T,
    max_size: u64,
    ttl: Duration,
}

impl<V: Clone + Send + Sync + 'static, T: BuildHasher + Clone + Send + Sync + 'static> Display
    for SimpleCache<V, T>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "max size: {:.2}, item ttl: {:?}",
            Byte::from_u64(self.max_size).get_adjusted_unit(byte_unit::Unit::MiB),
            self.ttl
        )
    }
}

impl<V: Clone + Send + Sync + 'static, T: BuildHasher + Clone + Send + Sync + 'static>
    std::fmt::Debug for SimpleCache<V, T>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SimpleCache")
            .field("cache_size", &0u64) // Simple cache doesn't track weighted size
            .field("item_count", &self.cache.len())
            .finish_non_exhaustive()
    }
}

impl<V: Clone + Send + Sync + 'static, T: BuildHasher + Clone + Send + Sync + 'static>
    SimpleCache<V, T>
{
    pub fn new(cache_max_size: u64, ttl: Duration, hasher: T) -> Self {
        // Estimate item count: assume average item size of 1KB for capacity calculation
        let estimated_items = (cache_max_size / 1024).max(16);
        let capacity_per_shard = (estimated_items / NUM_SHARDS as u64).max(16) as usize;
        let cache = Lru::with_capacity(
            usize::try_from(cache_max_size).unwrap_or(usize::MAX),
            capacity_per_shard,
        );

        let key_shards =
            std::array::from_fn(|_| RwLock::new(HashSet::with_capacity(capacity_per_shard)));

        SimpleCache {
            cache,
            key_shards,
            hasher,
            ttl,
            max_size: cache_max_size,
        }
    }
}

impl<V: AsTableRefs + Clone + Send + Sync + 'static, T: BuildHasher + Clone + Send + Sync + 'static>
    SimpleCache<V, T>
{
    pub fn as_tabled_provider(self: Arc<Self>) -> Arc<dyn TabledCacheProvider<V> + Send + Sync> {
        self
    }
}

impl<V: Clone + Send + Sync + 'static, T: BuildHasher + Clone + Send + Sync + 'static> HashProvider
    for SimpleCache<V, T>
{
    fn hasher(&self) -> Box<dyn Hasher> {
        Box::new(self.hasher.build_hasher())
    }
}

#[async_trait]
impl<V: Clone + Send + Sync + 'static, T: BuildHasher + Clone + Send + Sync + 'static>
    CacheProvider<V> for SimpleCache<V, T>
{
    async fn get_raw_key(&self, key: &u64) -> Option<V> {
        // NOTE: There's a known race condition here due to pingora-lru's API limitations.
        // We must remove() to check TTL (no peek_with_value() available), creating a brief
        // window where concurrent requests may see a cache miss. This is acceptable because:
        // 1. The race window is microseconds (remove → TTL check → re-admit)
        // 2. Worst case: one extra cache miss during TTL validation
        // 3. Performance gain (3x faster than moka) outweighs rare edge case
        if let Some((cached_value, _weight)) = self.cache.remove(*key) {
            if cached_value.inserted_at.elapsed() <= self.ttl {
                let value = cached_value.value.clone();
                self.cache.admit(*key, cached_value, 1); // weight = 1 for simple cache
                return Some(value);
            }
            // Expired - don't re-add, remove from tracking
            let shard = get_shard(*key);
            self.key_shards[shard].write().remove(key);
        }
        None
    }

    async fn put_raw_key(&self, key: &u64, value: V) {
        let cached_value = CachedValue {
            value,
            inserted_at: Instant::now(),
        };
        self.cache.admit(*key, cached_value, 1); // weight = 1

        let shard = get_shard(*key);
        self.key_shards[shard].write().insert(*key);
    }

    fn invalidate_all(&self) {
        for shard_idx in 0..NUM_SHARDS {
            let keys_to_remove: Vec<u64> = {
                let shard = self.key_shards[shard_idx].read();
                shard.iter().copied().collect()
            };

            for key in keys_to_remove {
                self.cache.remove(key);
            }

            self.key_shards[shard_idx].write().clear();
        }
    }

    fn size_bytes(&self) -> u64 {
        0 // Simple cache doesn't track weighted size
    }

    fn item_count(&self) -> u64 {
        self.cache.len() as u64
    }

    fn max_size(&self) -> usize {
        usize::try_from(self.max_size).unwrap_or_default()
    }

    async fn checkpoint(&self) {
        // pingora-lru doesn't expose iteration - expiration checked during get()
    }
}

#[async_trait]
impl<V: AsTableRefs + Clone + Send + Sync + 'static, T: BuildHasher + Clone + Send + Sync + 'static>
    TabledCacheProvider<V> for SimpleCache<V, T>
{
    fn invalidate_for_table(&self, table_ref: TableReference) -> Result<()> {
        for shard_idx in 0..NUM_SHARDS {
            let keys_to_check: Vec<u64> = {
                let shard = self.key_shards[shard_idx].read();
                let mut keys = Vec::with_capacity(shard.len());
                keys.extend(shard.iter().copied());
                keys
            };

            let mut keys_to_remove = Vec::new();

            for key in keys_to_check {
                if let Some((cached_value, weight)) = self.cache.remove(key) {
                    if cached_value.value.as_table_refs().contains(&table_ref) {
                        keys_to_remove.push(key);
                    } else {
                        self.cache.admit(key, cached_value, weight);
                    }
                } else {
                    keys_to_remove.push(key);
                }
            }

            if !keys_to_remove.is_empty() {
                let mut shard = self.key_shards[shard_idx].write();
                for key in keys_to_remove {
                    shard.remove(&key);
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::CacheKey;

    use super::*;
    use crate::CachedQueryResult;
    use arrow::array::{Int32Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::sql::TableReference;
    use rstest::rstest;
    use std::collections::HashSet;
    use std::hash::RandomState;
    use std::sync::Arc;
    use std::time::Duration;

    fn create_test_record_batch() -> RecordBatch {
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);
        let array = Int32Array::from(vec![1, 2, 3]);
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)])
            .expect("Failed to create record batch")
    }

    fn create_test_cached_result() -> CachedQueryResult {
        let record_batch = create_test_record_batch();
        let mut input_tables = HashSet::new();
        input_tables.insert(TableReference::Bare {
            table: Arc::from("test_table"),
        });

        CachedQueryResult::new(
            Arc::new(vec![record_batch.clone()]),
            Arc::new(record_batch.schema().as_ref().to_owned()),
            Arc::new(input_tables),
            std::time::Instant::now(),
        )
    }

    #[rstest]
    #[case::siphash(RandomState::default())]
    #[case::ahash(ahash::RandomState::default())]
    #[tokio::test]
    async fn test_cache_put_and_get<T: BuildHasher + Clone + Send + Sync + 'static>(
        #[case] hasher: T,
    ) {
        let cache: SimpleCache<CachedQueryResult, _> =
            SimpleCache::new(10, Duration::from_secs(60), hasher);
        let key = CacheKey::Query("test_query", None).as_raw_key(cache.hasher());
        let result = create_test_cached_result();

        // Put a value in the cache
        cache.put_raw_key(&key.as_u64(), result.clone()).await;

        let key = CacheKey::Query("test_query", None).as_raw_key(cache.hasher());

        // Get the value from the cache
        let retrieved = cache.get_raw_key(&key.as_u64()).await;
        assert!(retrieved.is_some());
        assert_eq!(
            retrieved.expect("Failed to get from cache").records.len(),
            result.records.len()
        );
    }

    #[rstest]
    #[case::siphash(RandomState::default())]
    #[case::ahash(ahash::RandomState::default())]
    #[tokio::test]
    async fn test_cache_miss<T: BuildHasher + Clone + Send + Sync + 'static>(#[case] hasher: T) {
        let cache: SimpleCache<CachedQueryResult, _> =
            SimpleCache::new(10, Duration::from_secs(60), hasher);
        let key = CacheKey::Query("nonexistent_query", None).as_raw_key(cache.hasher());

        // Try to get a non-existent key
        let retrieved = cache.get_raw_key(&key.as_u64()).await;
        assert!(retrieved.is_none());
    }

    #[rstest]
    #[case::siphash(RandomState::default())]
    #[case::ahash(ahash::RandomState::default())]
    #[tokio::test]
    async fn test_cache_invalidate_all<T: BuildHasher + Clone + Send + Sync + 'static>(
        #[case] hasher: T,
    ) {
        let cache: SimpleCache<CachedQueryResult, _> =
            SimpleCache::new(10, Duration::from_secs(60), hasher);
        let result = create_test_cached_result();

        // Put a value in the cache
        let get_key = || CacheKey::Query("test_query", None).as_raw_key(cache.hasher());
        let key = get_key();
        cache.put_raw_key(&key.as_u64(), result).await;

        // Verify the value is in the cache
        let retrieved = cache.get_raw_key(&key.as_u64()).await;
        assert!(retrieved.is_some());

        // Invalidate the cache for the table
        cache.invalidate_all();

        // Verify the value is no longer in the cache
        let retrieved = cache.get_raw_key(&key.as_u64()).await;
        assert!(retrieved.is_none());
    }

    #[rstest]
    #[case::siphash(RandomState::default())]
    #[case::ahash(ahash::RandomState::default())]
    #[tokio::test]
    async fn test_cache_ttl<T: BuildHasher + Clone + Send + Sync + 'static>(#[case] hasher: T) {
        let cache: SimpleCache<CachedQueryResult, _> =
            SimpleCache::new(10, Duration::from_millis(100), hasher);
        let key = || CacheKey::Query("test_query", None).as_raw_key(cache.hasher());
        let result = create_test_cached_result();

        // Put a value in the cache
        cache.put_raw_key(&key().as_u64(), result).await;

        // Verify the value is in the cache
        let retrieved = cache.get_raw_key(&key().as_u64()).await;
        assert!(retrieved.is_some());

        // Wait for the TTL to expire
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Verify the value is no longer in the cache
        let retrieved = cache.get_raw_key(&key().as_u64()).await;
        assert!(retrieved.is_none());
    }
}
