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

use crate::AsTableRefs;
use crate::FailedToInvalidateCacheSnafu;
use crate::HashBuilder;
use crate::HashProvider;
use crate::Result;
use crate::Sizeable;
use crate::TabledCacheProvider;
use crate::metrics::CacheMetrics;
use crate::{CacheProvider, get_hash_builder};
use async_trait::async_trait;
use byte_unit::Byte;
use datafusion::sql::TableReference;
use moka::future::Cache;
use snafu::ResultExt;
use spicepod::component::caching::CacheConfig;
use std::fmt::Display;
use std::hash::BuildHasher;
use std::hash::Hasher;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

// 'static is required by a bound from moka::Cache
pub struct LruCache<
    V: Sizeable + CacheMetrics + Clone + Send + Sync + 'static,
    T: BuildHasher + Clone + Send + Sync + 'static,
> {
    cache: Cache<u64, V, T>,
    hasher: T,
    max_size: u64,
    metrics_last_reported_time: AtomicU64,
    ttl: Duration,
    initial_instant: Instant,
    hits: AtomicU64,
    total_requests: AtomicU64,
}

impl<
    V: Sizeable + CacheMetrics + Clone + Send + Sync + 'static,
    T: BuildHasher + Clone + Send + Sync + 'static,
> Display for LruCache<V, T>
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

impl<
    V: Sizeable + CacheMetrics + Clone + Send + Sync + 'static,
    T: BuildHasher + Clone + Send + Sync + 'static,
> std::fmt::Debug for LruCache<V, T>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LruCache")
            .field("cache_size", &self.cache.weighted_size())
            .field("item_count", &self.cache.entry_count())
            .field(
                "metrics_reported_last_time",
                &self.metrics_last_reported_time,
            )
            .finish_non_exhaustive()
    }
}

/// Builds an LRU cache provider from the given configuration.
///
/// # Errors
///
/// - If the specified `max_size` cannot be parsed as a valid byte size.
/// - If the specified `item_ttl` cannot be parsed as a valid duration.
pub fn build_from_config<V: Sizeable + CacheMetrics + Clone + Send + Sync + 'static>(
    cache_config: &CacheConfig,
) -> Result<Arc<LruCache<V, HashBuilder>>> {
    let cache_max_size: u64 = match &cache_config.max_size {
        Some(cache_max_size) => Byte::parse_str(cache_max_size, true)
            .context(super::FailedToParseCacheMaxSizeSnafu)?
            .as_u64(),
        None => 128 * 1024 * 1024, // 128 MiB
    };

    let ttl = match &cache_config.item_ttl {
        Some(item_ttl) => {
            fundu::parse_duration(item_ttl).context(super::FailedToParseDurationSnafu {
                field: "item_ttl".to_string(),
            })?
        }
        None => std::time::Duration::from_secs(1),
    };

    let hash_builder = get_hash_builder(cache_config.hashing_algorithm)?;
    Ok(Arc::new(LruCache::new(cache_max_size, ttl, hash_builder)))
}

impl<
    V: Sizeable + CacheMetrics + Clone + Send + Sync + 'static,
    T: BuildHasher + Clone + Send + Sync + 'static,
> LruCache<V, T>
{
    pub fn new(cache_max_size: u64, ttl: Duration, hasher: T) -> Self {
        let cache: Cache<u64, V, T> = Cache::builder()
            .time_to_live(ttl)
            .weigher(|_key, value: &V| -> u32 {
                let val: usize = value.get_memory_size();

                match val.try_into() {
                    Ok(val) => val,
                    Err(e) => {
                        // This should never happen, as the size of record batches should be less than u32::MAX
                        tracing::warn!(
                            "Lru cache: Failed to convert query result size to u32: {}",
                            e
                        );
                        // Return the maximum value if we can't convert, so that we don't cache this record.
                        u32::MAX
                    }
                }
            })
            .max_capacity(cache_max_size)
            .eviction_policy(moka::policy::EvictionPolicy::lru())
            .support_invalidation_closures()
            .eviction_listener(|_key, _value, cause| {
                if cause.was_evicted() {
                    V::record_eviction();
                }
            })
            .build_with_hasher(hasher.clone());

        LruCache {
            cache,
            hasher,
            max_size: cache_max_size,
            metrics_last_reported_time: AtomicU64::new(0),
            ttl,
            initial_instant: Instant::now(),
            hits: AtomicU64::new(0),
            total_requests: AtomicU64::new(0),
        }
    }

    pub fn as_provider(self: Arc<Self>) -> Arc<dyn CacheProvider<V> + Send + Sync> {
        self
    }
}

impl<
    V: Sizeable + AsTableRefs + CacheMetrics + Clone + Send + Sync + 'static,
    T: BuildHasher + Clone + Send + Sync + 'static,
> LruCache<V, T>
{
    pub fn as_tabled_provider(self: Arc<Self>) -> Arc<dyn TabledCacheProvider<V> + Send + Sync> {
        self
    }
}

impl<
    V: Sizeable + CacheMetrics + Clone + Send + Sync + 'static,
    T: BuildHasher + Clone + Send + Sync + 'static,
> HashProvider for LruCache<V, T>
{
    fn hasher(&self) -> Box<dyn Hasher> {
        Box::new(self.hasher.build_hasher())
    }
}

#[async_trait]
impl<
    V: Sizeable + CacheMetrics + Clone + Send + Sync + 'static,
    T: BuildHasher + Clone + Send + Sync + 'static,
> CacheProvider<V> for LruCache<V, T>
{
    async fn get_raw_key(&self, key: &u64) -> Option<V> {
        V::record_request();
        self.total_requests.fetch_add(1, Ordering::Relaxed);

        if let Some(v) = self.cache.get(key).await {
            V::record_hit();
            self.hits.fetch_add(1, Ordering::Relaxed);
            Some(v)
        } else {
            V::record_miss();
            None
        }
    }

    async fn put_raw_key(&self, key: &u64, value: V) {
        self.cache.insert(*key, value).await;

        let now_seconds = self.initial_instant.elapsed().as_secs();
        let last_emitted = self.metrics_last_reported_time.load(Ordering::Relaxed);

        // compare_exchange ensures only 1 active thread emits metric updates every 5 seconds
        // performance is comparable with relaxed load/store
        if now_seconds.saturating_sub(last_emitted) >= 5
            && self
                .metrics_last_reported_time
                .compare_exchange(
                    last_emitted,
                    now_seconds,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                )
                .is_ok()
        {
            V::record_item_count(self.item_count().await);
            V::record_size(self.size_bytes().await);
            V::record_max_size(self.max_size() as u64);

            let hits = self.hits.load(Ordering::Relaxed);
            let total = self.total_requests.load(Ordering::Relaxed);
            V::update_hit_ratio(hits, total);
        }
    }

    async fn invalidate_all(&self) {
        self.cache.invalidate_all();

        let now_seconds = self.initial_instant.elapsed().as_secs();
        let last_emitted = self.metrics_last_reported_time.load(Ordering::Relaxed);

        // compare_exchange ensures only 1 active thread emits metric updates every 5 seconds
        // performance is comparable with relaxed load/store
        if now_seconds.saturating_sub(last_emitted) >= 5
            && self
                .metrics_last_reported_time
                .compare_exchange(
                    last_emitted,
                    now_seconds,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                )
                .is_ok()
        {
            V::record_item_count(self.item_count().await);
            V::record_size(self.size_bytes().await);
        }
    }

    async fn size_bytes(&self) -> u64 {
        self.cache.run_pending_tasks().await;
        self.cache.weighted_size()
    }

    async fn item_count(&self) -> u64 {
        self.cache.run_pending_tasks().await;
        self.cache.entry_count()
    }

    fn max_size(&self) -> usize {
        usize::try_from(self.max_size).unwrap_or_default()
    }

    async fn checkpoint(&self) {
        self.cache.run_pending_tasks().await;
    }
}

#[async_trait]
impl<
    V: Sizeable + AsTableRefs + CacheMetrics + Clone + Send + Sync + 'static,
    T: BuildHasher + Clone + Send + Sync + 'static,
> TabledCacheProvider<V> for LruCache<V, T>
{
    fn invalidate_for_table(&self, table_ref: TableReference) -> Result<()> {
        let table_name = match &table_ref {
            TableReference::Bare { table }
            | TableReference::Partial { table, .. }
            | TableReference::Full { table, .. } => table,
        };
        let table_name = Arc::clone(table_name);
        self.cache
            .invalidate_entries_if(move |_key, value| value.as_table_refs().contains(&table_ref))
            .context(FailedToInvalidateCacheSnafu { table_name })?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::CacheKey;
    use crate::result::query::CachedQueryResult;
    use crate::result::search::{CachedAggregationResult, CachedSearchResult};

    use super::*;
    use arrow::array::{Int32Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use rstest::rstest;
    #[cfg(feature = "xxhash")]
    use spicepod::component::caching::HashingAlgorithm;
    use std::collections::{HashMap, HashSet};
    use std::hash::RandomState;
    use std::time::Duration;

    fn create_test_record_batch() -> RecordBatch {
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);
        let array = Int32Array::from(vec![1, 2, 3]);
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)])
            .expect("Failed to create record batch")
    }

    async fn create_test_cached_result() -> CachedQueryResult {
        let record_batch = create_test_record_batch();
        let mut input_tables = HashSet::new();
        input_tables.insert(TableReference::Bare {
            table: Arc::from("test_table"),
        });

        let encoder = crate::encoding::get_encoder(spicepod::component::caching::Encoding::None);

        CachedQueryResult::from_batches(
            &[record_batch],
            Arc::new(input_tables),
            std::time::Instant::now(),
            encoder,
        )
        .await
        .expect("Failed to create cached result")
    }

    fn create_test_cached_search_result() -> CachedSearchResult {
        let mut results = HashMap::new();
        let record_batch = create_test_record_batch();
        let schema = record_batch.schema();
        let cached_aggregation_result = CachedAggregationResult {
            records: Arc::new(vec![record_batch]),
            primary_keys: Vec::new(),
            data_columns: Vec::new(),
            matches: HashMap::new(),
            schema,
        };

        results.insert(
            TableReference::Bare {
                table: Arc::from("test_table"),
            },
            cached_aggregation_result,
        );

        CachedSearchResult {
            results: Arc::new(results),
            input_tables: Arc::new(HashSet::from([TableReference::Bare {
                table: Arc::from("test_table"),
            }])),
        }
    }

    #[rstest]
    #[case::siphash(RandomState::default())]
    #[case::ahash(ahash::RandomState::default())]
    #[tokio::test]
    async fn test_cache_put_and_get<T: BuildHasher + Clone + Send + Sync + 'static>(
        #[case] hasher: T,
    ) {
        let cache: LruCache<CachedQueryResult, _> =
            LruCache::new(10, Duration::from_secs(60), hasher);
        let key = CacheKey::Query("test_query", None).as_raw_key(cache.hasher());
        let result = create_test_cached_result().await;

        // Put a value in the cache
        cache.put_raw_key(&key.as_u64(), result.clone()).await;

        let key = CacheKey::Query("test_query", None).as_raw_key(cache.hasher());

        // Get the value from the cache
        let retrieved = cache.get_raw_key(&key.as_u64()).await;
        assert!(retrieved.is_some());
        let retrieved = retrieved.expect("Failed to get from cache");
        assert_eq!(
            retrieved.records().await.expect("Failed to decode").len(),
            result.records().await.expect("Failed to decode").len()
        );
    }

    #[rstest]
    #[case::siphash(RandomState::default())]
    #[case::ahash(ahash::RandomState::default())]
    #[tokio::test]
    async fn test_cache_miss<T: BuildHasher + Clone + Send + Sync + 'static>(#[case] hasher: T) {
        let cache: LruCache<CachedQueryResult, _> =
            LruCache::new(10, Duration::from_secs(60), hasher);
        let key = CacheKey::Query("nonexistent_query", None).as_raw_key(cache.hasher());

        // Try to get a non-existent key
        let retrieved = cache.get_raw_key(&key.as_u64()).await;
        assert!(retrieved.is_none());
    }

    #[rstest]
    #[case::siphash(RandomState::default())]
    #[case::ahash(ahash::RandomState::default())]
    #[tokio::test]
    async fn test_cache_invalidate_for_table<T: BuildHasher + Clone + Send + Sync + 'static>(
        #[case] hasher: T,
    ) {
        let cache: LruCache<CachedQueryResult, _> =
            LruCache::new(10, Duration::from_secs(60), hasher);
        let table_ref = TableReference::Bare {
            table: Arc::from("test_table"),
        };
        let result = create_test_cached_result().await;

        // Put a value in the cache
        let get_key = || CacheKey::Query("test_query", None).as_raw_key(cache.hasher());
        let key = get_key();
        cache.put_raw_key(&key.as_u64(), result).await;

        // Verify the value is in the cache
        let retrieved = cache.get_raw_key(&key.as_u64()).await;
        assert!(retrieved.is_some());

        // Invalidate the cache for the table
        cache
            .invalidate_for_table(table_ref)
            .expect("should invalidate cache");

        // Verify the value is no longer in the cache
        let retrieved = cache.get_raw_key(&key.as_u64()).await;
        assert!(retrieved.is_none());
    }

    #[rstest]
    #[case::siphash(RandomState::default())]
    #[case::ahash(ahash::RandomState::default())]
    #[tokio::test]
    async fn test_search_cache_invalidate_for_table<
        T: BuildHasher + Clone + Send + Sync + 'static,
    >(
        #[case] hasher: T,
    ) {
        let cache: LruCache<CachedSearchResult, _> =
            LruCache::new(10, Duration::from_secs(60), hasher);
        let table_ref = TableReference::Bare {
            table: Arc::from("test_table"),
        };
        let result = create_test_cached_search_result();

        let raw_cache_key = 123_456;

        // Put a value in the cache
        cache.put_raw_key(&raw_cache_key, result).await;

        // Verify the value is in the cache
        let retrieved = cache.get_raw_key(&raw_cache_key).await;
        assert!(retrieved.is_some());

        // Invalidate the cache for the table
        cache
            .invalidate_for_table(table_ref)
            .expect("should invalidate cache");

        // Verify the value is no longer in the cache
        let retrieved = cache.get_raw_key(&raw_cache_key).await;
        assert!(retrieved.is_none());
    }

    #[rstest]
    #[case::siphash(RandomState::default())]
    #[case::ahash(ahash::RandomState::default())]
    #[tokio::test]
    async fn test_cache_ttl<T: BuildHasher + Clone + Send + Sync + 'static>(#[case] hasher: T) {
        let cache: LruCache<CachedQueryResult, _> =
            LruCache::new(10, Duration::from_millis(100), hasher);
        let key = || CacheKey::Query("test_query", None).as_raw_key(cache.hasher());
        let result = create_test_cached_result().await;

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

    #[cfg(feature = "xxhash")]
    #[rstest]
    #[case::xxh3(HashingAlgorithm::XXH3)]
    #[case::xxh32(HashingAlgorithm::XXH32)]
    #[case::xxh64(HashingAlgorithm::XXH64)]
    #[case::xxh128(HashingAlgorithm::XXH128)]
    #[tokio::test]
    async fn test_cache_ttl_xhash(#[case] hashing_algo: HashingAlgorithm) {
        let hasher = get_hash_builder(hashing_algo).expect("Failed to get hash builder");

        let cache: LruCache<CachedQueryResult, _> =
            LruCache::new(10, Duration::from_millis(100), hasher);
        let key = || CacheKey::Query("test_query", None).as_raw_key(cache.hasher());
        let result = create_test_cached_result().await;

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
