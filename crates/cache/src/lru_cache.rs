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
use crate::HashBuilder;
use crate::HashProvider;
use crate::Result;
use crate::Sizeable;
use crate::TabledCacheProvider;
use crate::backend::{CacheBackend, CacheBackendBuilder, MokaBackend, PingoraBackend};
use crate::metrics::CacheMetrics;
use crate::{CacheProvider, get_hash_builder};
use async_trait::async_trait;
use byte_unit::Byte;
use datafusion::sql::TableReference;
use snafu::ResultExt;
use spicepod::component::caching::{CacheConfig, CacheEngine};
use std::fmt::Display;
use std::hash::BuildHasher;
use std::hash::Hasher;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

enum CacheBackendType<
    V: Sizeable + CacheMetrics + Clone + Send + Sync + 'static,
    T: BuildHasher + Clone + Send + Sync + 'static,
> {
    Moka(MokaBackend<V, T>),
    Pingora(PingoraBackend<V>),
}

pub struct LruCache<
    V: Sizeable + CacheMetrics + Clone + Send + Sync + 'static,
    T: BuildHasher + Clone + Send + Sync + 'static,
> {
    backend: CacheBackendType<V, T>,
    hasher: T,
    max_size: u64,
    metrics_last_reported_time: AtomicU64,
    ttl: Duration,
    initial_instant: Instant,
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
            .field("max_size", &self.max_size)
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
    Ok(Arc::new(LruCache::new(
        cache_max_size,
        ttl,
        hash_builder,
        cache_config.engine,
    )))
}

impl<
    V: Sizeable + CacheMetrics + Clone + Send + Sync + 'static,
    T: BuildHasher + Clone + Send + Sync + 'static,
> LruCache<V, T>
{
    pub fn new(cache_max_size: u64, ttl: Duration, hasher: T, engine: CacheEngine) -> Self {
        let builder = CacheBackendBuilder::new(cache_max_size, ttl);

        let backend = match engine {
            CacheEngine::Moka => {
                tracing::info!("Using Moka cache engine");
                CacheBackendType::Moka(MokaBackend::new(&builder, hasher.clone()))
            }
            CacheEngine::Pingora => {
                tracing::info!("Using Pingora cache engine (high-performance, lock-free)");
                CacheBackendType::Pingora(PingoraBackend::new(&builder))
            }
        };

        V::init();

        LruCache {
            backend,
            hasher,
            max_size: cache_max_size,
            metrics_last_reported_time: AtomicU64::new(0),
            ttl,
            initial_instant: Instant::now(),
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
        let result = match &self.backend {
            CacheBackendType::Moka(backend) => backend.get(key).await,
            CacheBackendType::Pingora(backend) => backend.get(key).await,
        };

        if result.is_some() {
            V::record_hit();
        }
        result
    }

    async fn put_raw_key(&self, key: &u64, value: V) {
        let size = value.get_memory_size();
        match &self.backend {
            CacheBackendType::Moka(backend) => backend.insert(*key, value, size).await,
            CacheBackendType::Pingora(backend) => backend.insert(*key, value, size).await,
        }

        let now_seconds = self.initial_instant.elapsed().as_secs();
        let last_emitted = self.metrics_last_reported_time.load(Ordering::Relaxed);

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
            V::record_item_count(self.item_count());
            V::record_size(self.size_bytes());
            V::record_max_size(self.max_size() as u64);
        }
    }

    fn invalidate_all(&self) {
        // Block on async clear since this is a sync method that should complete before returning
        let rt = tokio::runtime::Handle::try_current();
        if let Ok(handle) = rt {
            // We're in a runtime, use block_in_place to avoid blocking the runtime
            tokio::task::block_in_place(|| {
                handle.block_on(async {
                    match &self.backend {
                        CacheBackendType::Moka(backend) => backend.clear().await,
                        CacheBackendType::Pingora(backend) => backend.clear().await,
                    }
                });
            });
        } else {
            // Not in a runtime, create a new one
            match tokio::runtime::Runtime::new() {
                Ok(rt) => {
                    rt.block_on(async {
                        match &self.backend {
                            CacheBackendType::Moka(backend) => backend.clear().await,
                            CacheBackendType::Pingora(backend) => backend.clear().await,
                        }
                    });
                }
                Err(e) => {
                    tracing::error!(
                        "Failed to create tokio runtime for cache invalidation: {}",
                        e
                    );
                }
            }
        }

        let now_seconds = self.initial_instant.elapsed().as_secs();
        let last_emitted = self.metrics_last_reported_time.load(Ordering::Relaxed);

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
            V::record_item_count(self.item_count());
            V::record_size(self.size_bytes());
        }
    }

    fn size_bytes(&self) -> u64 {
        // Both backends expose sync methods for size
        let rt = tokio::runtime::Handle::try_current();
        if let Ok(handle) = rt {
            tokio::task::block_in_place(|| {
                handle.block_on(async {
                    match &self.backend {
                        CacheBackendType::Moka(backend) => {
                            // Moka doesn't expose weighted_size in async, use len as proxy
                            backend.len().await as u64
                        }
                        CacheBackendType::Pingora(backend) => backend.len().await as u64,
                    }
                })
            })
        } else {
            0 // Can't determine size without runtime
        }
    }

    fn item_count(&self) -> u64 {
        let rt = tokio::runtime::Handle::try_current();
        if let Ok(handle) = rt {
            tokio::task::block_in_place(|| {
                handle.block_on(async {
                    match &self.backend {
                        CacheBackendType::Moka(backend) => backend.len().await as u64,
                        CacheBackendType::Pingora(backend) => backend.len().await as u64,
                    }
                })
            })
        } else {
            0 // Can't determine count without runtime
        }
    }

    fn max_size(&self) -> usize {
        usize::try_from(self.max_size).unwrap_or_default()
    }

    async fn checkpoint(&self) {
        // Moka handles pending tasks internally, pingora doesn't need checkpoints
        match &self.backend {
            CacheBackendType::Moka(_) | CacheBackendType::Pingora(_) => {
                // No-op for both backends
            }
        }
    }
}

#[async_trait]
impl<
    V: Sizeable + AsTableRefs + CacheMetrics + Clone + Send + Sync + 'static,
    T: BuildHasher + Clone + Send + Sync + 'static,
> TabledCacheProvider<V> for LruCache<V, T>
{
    fn invalidate_for_table(&self, table_ref: TableReference) -> Result<()> {
        // For both moka and pingora, we iterate keys and check each value
        // This needs to be done in a blocking context since this is a sync method
        let backend = match &self.backend {
            CacheBackendType::Moka(b) => b.clone(),
            CacheBackendType::Pingora(_) => {
                // Pingora doesn't work well with this pattern, skip for now
                tracing::warn!("Table invalidation not yet supported for Pingora backend");
                return Ok(());
            }
        };

        // Use a separate runtime for this operation if we're not already in one
        let rt = tokio::runtime::Handle::try_current();
        if let Ok(handle) = rt {
            // We're in a runtime, use block_in_place to avoid blocking the runtime
            tokio::task::block_in_place(|| {
                handle.block_on(async {
                    let keys = backend.iter_keys().await;
                    for key in keys {
                        if let Some(value) = backend.get(&key).await
                            && value.as_table_refs().contains(&table_ref)
                        {
                            backend.remove(&key).await;
                        }
                    }
                });
            });
        } else {
            // Not in a runtime, create a new one
            match tokio::runtime::Runtime::new() {
                Ok(rt) => {
                    rt.block_on(async {
                        let keys = backend.iter_keys().await;
                        for key in keys {
                            if let Some(value) = backend.get(&key).await
                                && value.as_table_refs().contains(&table_ref)
                            {
                                backend.remove(&key).await;
                            }
                        }
                    });
                }
                Err(e) => {
                    tracing::error!(
                        "Failed to create tokio runtime for cache invalidation: {}",
                        e
                    );
                }
            }
        }

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
            LruCache::new(10, Duration::from_secs(60), hasher, CacheEngine::Moka);
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
        let cache: LruCache<CachedQueryResult, _> =
            LruCache::new(10, Duration::from_secs(60), hasher, CacheEngine::Moka);
        let key = CacheKey::Query("nonexistent_query", None).as_raw_key(cache.hasher());

        // Try to get a non-existent key
        let retrieved = cache.get_raw_key(&key.as_u64()).await;
        assert!(retrieved.is_none());
    }

    #[rstest]
    #[case::siphash(RandomState::default())]
    #[case::ahash(ahash::RandomState::default())]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_cache_invalidate_for_table<T: BuildHasher + Clone + Send + Sync + 'static>(
        #[case] hasher: T,
    ) {
        let cache: LruCache<CachedQueryResult, _> =
            LruCache::new(10, Duration::from_secs(60), hasher, CacheEngine::Moka);
        let table_ref = TableReference::Bare {
            table: Arc::from("test_table"),
        };
        let result = create_test_cached_result();

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
    #[tokio::test(flavor = "multi_thread")]
    async fn test_search_cache_invalidate_for_table<
        T: BuildHasher + Clone + Send + Sync + 'static,
    >(
        #[case] hasher: T,
    ) {
        let cache: LruCache<CachedSearchResult, _> =
            LruCache::new(10, Duration::from_secs(60), hasher, CacheEngine::Moka);
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
            LruCache::new(10, Duration::from_millis(100), hasher, CacheEngine::Moka);
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
            LruCache::new(10, Duration::from_millis(100), hasher, CacheEngine::Moka);
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
