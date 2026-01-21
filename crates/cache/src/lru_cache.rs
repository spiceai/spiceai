/*
Copyright 2024-2026 The Spice.ai OSS Authors

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
use crate::backend::{CacheBackend, MokaBackend};
use crate::key::PassthroughHashBuilder;
use crate::metrics::CacheMetrics;
use crate::{CacheProvider, get_hash_builder};
use async_trait::async_trait;
use byte_unit::Byte;
use datafusion::sql::TableReference;
use moka::future::Cache;
use snafu::ResultExt;
use spicepod::component::caching::{CacheConfig, CacheEngine, CachingPolicy};
use std::fmt::Display;
use std::hash::BuildHasher;
use std::hash::Hasher;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

#[cfg(feature = "pingora")]
use crate::backend::PingoraBackend;

/// Internal enum to hold either backend type, enabling runtime backend selection.
enum CacheBackendEnum<V, T>
where
    V: Sizeable + CacheMetrics + Clone + Send + Sync + 'static,
    T: BuildHasher + Clone + Send + Sync + 'static,
    <T as BuildHasher>::Hasher: Send + Sync + 'static,
{
    Moka(MokaBackend<V, T>),
    #[cfg(feature = "pingora")]
    Pingora(PingoraBackend<V>),
    /// Fallback to Moka when Pingora is requested but feature not enabled
    #[cfg(not(feature = "pingora"))]
    MokaFallback(MokaBackend<V, T>),
}

#[async_trait]
impl<V, T> CacheBackend<V> for CacheBackendEnum<V, T>
where
    V: Sizeable + CacheMetrics + Clone + Send + Sync + 'static,
    T: BuildHasher + Clone + Send + Sync + 'static,
    <T as BuildHasher>::Hasher: Send + Sync + 'static,
{
    async fn insert(&self, key: u64, value: V) {
        match self {
            Self::Moka(backend) => backend.insert(key, value).await,
            #[cfg(feature = "pingora")]
            Self::Pingora(backend) => backend.insert(key, value).await,
            #[cfg(not(feature = "pingora"))]
            Self::MokaFallback(backend) => backend.insert(key, value).await,
        }
    }

    async fn get(&self, key: &u64) -> Option<V> {
        match self {
            Self::Moka(backend) => backend.get(key).await,
            #[cfg(feature = "pingora")]
            Self::Pingora(backend) => backend.get(key).await,
            #[cfg(not(feature = "pingora"))]
            Self::MokaFallback(backend) => backend.get(key).await,
        }
    }

    async fn remove(&self, key: &u64) -> Option<V> {
        match self {
            Self::Moka(backend) => backend.remove(key).await,
            #[cfg(feature = "pingora")]
            Self::Pingora(backend) => backend.remove(key).await,
            #[cfg(not(feature = "pingora"))]
            Self::MokaFallback(backend) => backend.remove(key).await,
        }
    }

    async fn clear(&self) {
        match self {
            Self::Moka(backend) => backend.clear().await,
            #[cfg(feature = "pingora")]
            Self::Pingora(backend) => backend.clear().await,
            #[cfg(not(feature = "pingora"))]
            Self::MokaFallback(backend) => backend.clear().await,
        }
    }

    async fn iter_keys(&self) -> Vec<u64> {
        match self {
            Self::Moka(backend) => backend.iter_keys().await,
            #[cfg(feature = "pingora")]
            Self::Pingora(backend) => backend.iter_keys().await,
            #[cfg(not(feature = "pingora"))]
            Self::MokaFallback(backend) => backend.iter_keys().await,
        }
    }

    async fn len(&self) -> usize {
        match self {
            Self::Moka(backend) => backend.len().await,
            #[cfg(feature = "pingora")]
            Self::Pingora(backend) => backend.len().await,
            #[cfg(not(feature = "pingora"))]
            Self::MokaFallback(backend) => backend.len().await,
        }
    }

    async fn weighted_size(&self) -> u64 {
        match self {
            Self::Moka(backend) => backend.weighted_size().await,
            #[cfg(feature = "pingora")]
            Self::Pingora(backend) => backend.weighted_size().await,
            #[cfg(not(feature = "pingora"))]
            Self::MokaFallback(backend) => backend.weighted_size().await,
        }
    }

    async fn run_pending_tasks(&self) {
        match self {
            Self::Moka(backend) => backend.run_pending_tasks().await,
            #[cfg(feature = "pingora")]
            Self::Pingora(backend) => backend.run_pending_tasks().await,
            #[cfg(not(feature = "pingora"))]
            Self::MokaFallback(backend) => backend.run_pending_tasks().await,
        }
    }
}

// 'static is required by a bound from moka::Cache
pub struct LruCache<
    V: Sizeable + CacheMetrics + Clone + Send + Sync + 'static,
    T: BuildHasher<Hasher = H> + Clone + Send + Sync + 'static,
    H: Hasher + Send + Sync + 'static,
> {
    /// The underlying cache backend (Moka or Pingora)
    backend: CacheBackendEnum<V, T>,
    /// Moka cache for table invalidation (only used when Moka engine or for `invalidate_entries_if`)
    moka_cache: Option<Cache<u64, V, PassthroughHashBuilder<T>>>,
    /// The selected cache engine
    engine: CacheEngine,
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
    T: BuildHasher<Hasher = H> + Clone + Send + Sync + 'static,
    H: Hasher + Send + Sync + 'static,
> Display for LruCache<V, T, H>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "max size: {:.2}, item ttl: {:?}, engine: {}",
            Byte::from_u64(self.max_size).get_adjusted_unit(byte_unit::Unit::MiB),
            self.ttl,
            self.engine
        )
    }
}

impl<
    V: Sizeable + CacheMetrics + Clone + Send + Sync + 'static,
    T: BuildHasher<Hasher = H> + Clone + Send + Sync + 'static,
    H: Hasher + Send + Sync + 'static,
> std::fmt::Debug for LruCache<V, T, H>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LruCache")
            .field("engine", &self.engine)
            .field("max_size", &self.max_size)
            .field(
                "metrics_reported_last_time",
                &self.metrics_last_reported_time,
            )
            .finish_non_exhaustive()
    }
}

type BuiltLruCache<V> = LruCache<V, HashBuilder, Box<dyn Hasher + Send + Sync + 'static>>;

/// Builds an LRU cache provider from the given configuration.
///
/// # Errors
///
/// - If the specified `max_size` cannot be parsed as a valid byte size.
/// - If the specified `item_ttl` cannot be parsed as a valid duration.
pub fn build_from_config<V: Sizeable + CacheMetrics + Clone + Send + Sync + 'static>(
    cache_config: &CacheConfig,
) -> Result<Arc<BuiltLruCache<V>>> {
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
        cache_config.caching_policy,
        cache_config.engine,
    )))
}

// Build the Moka cache (used for Moka backend or for table invalidation support)
fn build_moka_cache<
    V: Sizeable + CacheMetrics + Clone + Send + Sync + 'static,
    T: BuildHasher<Hasher = H> + Clone + Send + Sync + 'static,
    H: Hasher + Send + Sync + 'static,
>(
    cache_max_size: u64,
    ttl: Duration,
    hasher: T,
    caching_policy: CachingPolicy,
) -> Cache<u64, V, PassthroughHashBuilder<T>> {
    let moka_eviction_policy = match caching_policy {
        CachingPolicy::Lru => moka::policy::EvictionPolicy::lru(),
        CachingPolicy::TinyLfu => moka::policy::EvictionPolicy::tiny_lfu(),
    };

    Cache::builder()
        .time_to_live(ttl)
        .weigher(|_key, value: &V| -> u32 {
            let val: usize = value.get_memory_size();
            match val.try_into() {
                Ok(val) => val,
                Err(e) => {
                    tracing::warn!(
                        "Lru cache: Failed to convert query result size to u32: {}",
                        e
                    );
                    u32::MAX
                }
            }
        })
        .max_capacity(cache_max_size)
        .eviction_policy(moka_eviction_policy)
        .support_invalidation_closures()
        .eviction_listener(|_key, _value, cause| {
            if cause.was_evicted() {
                V::record_eviction();
            }
        })
        .build_with_hasher(PassthroughHashBuilder::new(hasher))
}

impl<
    V: Sizeable + CacheMetrics + Clone + Send + Sync + 'static,
    T: BuildHasher<Hasher = H> + Clone + Send + Sync + 'static,
    H: Hasher + Send + Sync + 'static,
> LruCache<V, T, H>
{
    #[must_use]
    pub fn new(
        cache_max_size: u64,
        ttl: Duration,
        hasher: T,
        caching_policy: CachingPolicy,
        engine: CacheEngine,
    ) -> Self
    where
        <T as BuildHasher>::Hasher: Send + Sync + 'static,
    {
        // Create the appropriate backend and moka_cache based on engine selection
        #[expect(
            clippy::type_complexity,
            reason = "Tuple is used locally for destructuring"
        )]
        let (backend, moka_cache, effective_engine): (
            CacheBackendEnum<V, T>,
            Option<Cache<u64, V, PassthroughHashBuilder<T>>>,
            CacheEngine,
        ) = match engine {
            CacheEngine::Moka => {
                tracing::debug!("Using Moka cache engine");
                let cache = build_moka_cache(cache_max_size, ttl, hasher.clone(), caching_policy);
                let backend = CacheBackendEnum::Moka(MokaBackend::from_cache(cache.clone()));
                (backend, Some(cache), CacheEngine::Moka)
            }
            CacheEngine::Pingora => {
                #[cfg(feature = "pingora")]
                {
                    tracing::debug!("Using Pingora cache engine.");
                    if matches!(caching_policy, CachingPolicy::TinyLfu) {
                        tracing::warn!(
                            "Pingora cache engine does not support TinyLFU caching policy. Falling back to LRU."
                        );
                    }

                    let backend =
                        CacheBackendEnum::Pingora(PingoraBackend::with_params(cache_max_size, ttl));
                    (backend, None, CacheEngine::Pingora)
                }
                #[cfg(not(feature = "pingora"))]
                {
                    tracing::warn!(
                        "Pingora cache engine requested but 'pingora' feature is not enabled. Falling back to Moka."
                    );
                    let cache =
                        build_moka_cache(cache_max_size, ttl, hasher.clone(), caching_policy);
                    let backend =
                        CacheBackendEnum::MokaFallback(MokaBackend::from_cache(cache.clone()));
                    (backend, Some(cache), CacheEngine::Moka)
                }
            }
        };

        LruCache {
            backend,
            moka_cache,
            engine: effective_engine,
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
    T: BuildHasher<Hasher = H> + Clone + Send + Sync + 'static,
    H: Hasher + Send + Sync + 'static,
> LruCache<V, T, H>
{
    pub fn as_tabled_provider(self: Arc<Self>) -> Arc<dyn TabledCacheProvider<V> + Send + Sync> {
        self
    }
}

impl<
    V: Sizeable + CacheMetrics + Clone + Send + Sync + 'static,
    T: BuildHasher<Hasher = H> + Clone + Send + Sync + 'static,
    H: Hasher + Send + Sync + 'static,
> HashProvider for LruCache<V, T, H>
{
    fn hasher(&self) -> Box<dyn Hasher> {
        Box::new(self.hasher.build_hasher())
    }
}

#[async_trait]
impl<
    V: Sizeable + CacheMetrics + Clone + Send + Sync + 'static,
    T: BuildHasher<Hasher = H> + Clone + Send + Sync + 'static,
    H: Hasher + Send + Sync + 'static,
> CacheProvider<V> for LruCache<V, T, H>
{
    async fn get_raw_key(&self, key: &u64) -> Option<V> {
        V::record_request();
        self.total_requests.fetch_add(1, Ordering::Relaxed);

        if let Some(v) = self.backend.get(key).await {
            V::record_hit();
            self.hits.fetch_add(1, Ordering::Relaxed);
            Some(v)
        } else {
            V::record_miss();
            None
        }
    }

    async fn put_raw_key(&self, key: &u64, value: V) {
        self.backend.insert(*key, value).await;

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
        self.backend.clear().await;

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
        self.backend.run_pending_tasks().await;
        self.backend.weighted_size().await
    }

    async fn item_count(&self) -> u64 {
        self.backend.run_pending_tasks().await;
        self.backend.len().await as u64
    }

    fn max_size(&self) -> usize {
        usize::try_from(self.max_size).unwrap_or_default()
    }

    async fn checkpoint(&self) {
        self.backend.run_pending_tasks().await;
    }
}

#[async_trait]
impl<
    V: Sizeable + AsTableRefs + CacheMetrics + Clone + Send + Sync + 'static,
    T: BuildHasher<Hasher = H> + Clone + Send + Sync + 'static,
    H: Hasher + Send + Sync + 'static,
> TabledCacheProvider<V> for LruCache<V, T, H>
{
    fn invalidate_for_table(&self, table_ref: TableReference) -> Result<()> {
        let table_name = match &table_ref {
            TableReference::Bare { table }
            | TableReference::Partial { table, .. }
            | TableReference::Full { table, .. } => table,
        };
        let table_name_arc = Arc::clone(table_name);

        // For Moka backend, use efficient closure-based invalidation
        // For Pingora (when moka_cache is None), we need to fall back to manual iteration
        if let Some(ref moka_cache) = self.moka_cache {
            moka_cache
                .invalidate_entries_if(move |_key, value| {
                    value.as_table_refs().contains(&table_ref)
                })
                .context(FailedToInvalidateCacheSnafu {
                    table_name: table_name_arc,
                })?;
        } else {
            // Pingora backend: iterate keys and remove matching entries
            // This is O(n) but Pingora doesn't support closure-based invalidation
            tracing::debug!(
                "Invalidating cache entries for table {} using key iteration (Pingora backend)",
                table_name
            );

            // Spawn a blocking task to handle the synchronous iteration
            // Note: This is suboptimal but necessary for Pingora's API
            let backend = &self.backend;
            let keys_to_remove: Vec<u64> = futures::executor::block_on(async {
                let mut keys_to_remove = Vec::new();
                for key in backend.iter_keys().await {
                    if let Some(value) = backend.get(&key).await
                        && value.as_table_refs().contains(&table_ref)
                    {
                        keys_to_remove.push(key);
                    }
                }
                keys_to_remove
            });

            for key in keys_to_remove {
                futures::executor::block_on(backend.remove(&key));
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
    use spicepod::component::caching::{CachingPolicy, HashingAlgorithm};
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
    #[case::xxhash32(twox_hash::xxhash32::RandomState::default())]
    #[tokio::test]
    async fn test_cache_put_and_get<
        H: Hasher + Send + Sync + 'static,
        T: BuildHasher<Hasher = H> + Clone + Send + Sync + 'static,
    >(
        #[case] hasher: T,
    ) {
        let cache: LruCache<CachedQueryResult, _, _> = LruCache::new(
            10,
            Duration::from_secs(60),
            hasher,
            CachingPolicy::Lru,
            CacheEngine::Moka,
        );
        let key = CacheKey::Query("test_query", None).as_raw_key(cache.hasher());
        let result = create_test_cached_result().await;

        // Put a value in the cache
        cache.put_raw_key(&key.as_u64(), result.clone()).await;

        let key = CacheKey::Query("test_query", None).as_raw_key(cache.hasher());

        // Get the value from the cache
        let retrieved = cache.get_raw_key(&key.as_u64()).await;
        let retrieved = retrieved.expect("cache should contain the key");
        let retrieved_len = retrieved.records().await.expect("Failed to decode").len();
        let result_len = result.records().await.expect("Failed to decode").len();
        (retrieved_len == result_len)
            .then_some(())
            .expect("retrieved and result should have same length");
    }

    #[rstest]
    #[case::siphash(RandomState::default())]
    #[case::ahash(ahash::RandomState::default())]
    #[case::xxhash32(twox_hash::xxhash32::RandomState::default())]
    #[tokio::test]
    async fn test_cache_miss<
        H: Hasher + Send + Sync + 'static,
        T: BuildHasher<Hasher = H> + Clone + Send + Sync + 'static,
    >(
        #[case] hasher: T,
    ) {
        let cache: LruCache<CachedQueryResult, _, _> = LruCache::new(
            10,
            Duration::from_secs(60),
            hasher,
            CachingPolicy::Lru,
            CacheEngine::Moka,
        );
        let key = CacheKey::Query("nonexistent_query", None).as_raw_key(cache.hasher());

        // Try to get a non-existent key
        let retrieved = cache.get_raw_key(&key.as_u64()).await;
        retrieved
            .is_none()
            .then_some(())
            .expect("cache should not contain nonexistent key");
    }

    #[rstest]
    #[case::siphash(RandomState::default())]
    #[case::ahash(ahash::RandomState::default())]
    #[case::xxhash32(twox_hash::xxhash32::RandomState::default())]
    #[tokio::test]
    async fn test_cache_invalidate_for_table<
        H: Hasher + Send + Sync + 'static,
        T: BuildHasher<Hasher = H> + Clone + Send + Sync + 'static,
    >(
        #[case] hasher: T,
    ) {
        let cache: LruCache<CachedQueryResult, _, _> = LruCache::new(
            10,
            Duration::from_secs(60),
            hasher,
            CachingPolicy::Lru,
            CacheEngine::Moka,
        );
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
        retrieved
            .is_some()
            .then_some(())
            .expect("cache should contain the key before invalidation");

        // Invalidate the cache for the table
        cache
            .invalidate_for_table(table_ref)
            .expect("should invalidate cache");

        // Verify the value is no longer in the cache
        let retrieved = cache.get_raw_key(&key.as_u64()).await;
        retrieved
            .is_none()
            .then_some(())
            .expect("cache should not contain key after invalidation");
    }

    #[rstest]
    #[case::siphash(RandomState::default())]
    #[case::ahash(ahash::RandomState::default())]
    #[case::xxhash32(twox_hash::xxhash32::RandomState::default())]
    #[tokio::test]
    async fn test_search_cache_invalidate_for_table<
        H: Hasher + Send + Sync + 'static,
        T: BuildHasher<Hasher = H> + Clone + Send + Sync + 'static,
    >(
        #[case] hasher: T,
    ) {
        let cache: LruCache<CachedSearchResult, _, _> = LruCache::new(
            10,
            Duration::from_secs(60),
            hasher,
            CachingPolicy::Lru,
            CacheEngine::Moka,
        );
        let table_ref = TableReference::Bare {
            table: Arc::from("test_table"),
        };
        let result = create_test_cached_search_result();

        let raw_cache_key = 123_456;

        // Put a value in the cache
        cache.put_raw_key(&raw_cache_key, result).await;

        // Verify the value is in the cache
        let retrieved = cache.get_raw_key(&raw_cache_key).await;
        retrieved
            .is_some()
            .then_some(())
            .expect("cache should contain the key before invalidation");

        // Invalidate the cache for the table
        cache
            .invalidate_for_table(table_ref)
            .expect("should invalidate cache");

        // Verify the value is no longer in the cache
        let retrieved = cache.get_raw_key(&raw_cache_key).await;
        retrieved
            .is_none()
            .then_some(())
            .expect("cache should not contain key after invalidation");
    }

    #[rstest]
    #[case::siphash(HashingAlgorithm::Siphash)]
    #[case::ahash(HashingAlgorithm::Ahash)]
    #[case::blake3(HashingAlgorithm::Blake3)]
    #[tokio::test]
    async fn test_cache_ttl(#[case] hashing_algo: HashingAlgorithm) {
        let hasher = get_hash_builder(hashing_algo).expect("Failed to get hash builder");

        let cache: LruCache<CachedQueryResult, _, _> = LruCache::new(
            10,
            Duration::from_millis(100),
            hasher,
            CachingPolicy::Lru,
            CacheEngine::Moka,
        );
        let key = || CacheKey::Query("test_query", None).as_raw_key(cache.hasher());
        let result = create_test_cached_result().await;

        // Put a value in the cache
        cache.put_raw_key(&key().as_u64(), result).await;

        // Verify the value is in the cache
        let retrieved = cache.get_raw_key(&key().as_u64()).await;
        retrieved
            .is_some()
            .then_some(())
            .expect("cache should contain the key before TTL expiry");

        // Wait for the TTL to expire
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Verify the value is no longer in the cache
        let retrieved = cache.get_raw_key(&key().as_u64()).await;
        retrieved
            .is_none()
            .then_some(())
            .expect("cache should not contain key after TTL expiry");
    }

    #[rstest]
    #[case::xxh3(HashingAlgorithm::XXH3)]
    #[case::xxh32(HashingAlgorithm::XXH32)]
    #[case::xxh64(HashingAlgorithm::XXH64)]
    #[case::xxh128(HashingAlgorithm::XXH128)]
    #[tokio::test]
    async fn test_cache_ttl_xhash(#[case] hashing_algo: HashingAlgorithm) {
        let hasher = get_hash_builder(hashing_algo).expect("Failed to get hash builder");

        let cache: LruCache<CachedQueryResult, _, _> = LruCache::new(
            10,
            Duration::from_millis(100),
            hasher,
            CachingPolicy::Lru,
            CacheEngine::Moka,
        );
        let key = || CacheKey::Query("test_query", None).as_raw_key(cache.hasher());
        let result = create_test_cached_result().await;

        // Put a value in the cache
        cache.put_raw_key(&key().as_u64(), result).await;

        // Verify the value is in the cache
        let retrieved = cache.get_raw_key(&key().as_u64()).await;
        retrieved
            .is_some()
            .then_some(())
            .expect("cache should contain the key before TTL expiry");

        // Wait for the TTL to expire
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Verify the value is no longer in the cache
        let retrieved = cache.get_raw_key(&key().as_u64()).await;
        retrieved
            .is_none()
            .then_some(())
            .expect("cache should not contain key after TTL expiry");
    }

    #[rstest]
    #[case::lru(CachingPolicy::Lru)]
    #[case::tiny_lfu(CachingPolicy::TinyLfu)]
    #[tokio::test]
    async fn test_cache_with_caching_policy(#[case] caching_policy: CachingPolicy) {
        let hasher = RandomState::default();
        let cache: LruCache<CachedQueryResult, _, _> = LruCache::new(
            10,
            Duration::from_secs(60),
            hasher,
            caching_policy,
            CacheEngine::Moka,
        );

        let key = CacheKey::Query("test_query", None).as_raw_key(cache.hasher());
        let result = create_test_cached_result().await;

        // Put a value in the cache
        cache.put_raw_key(&key.as_u64(), result.clone()).await;

        // Get the value from the cache
        let retrieved = cache.get_raw_key(&key.as_u64()).await;
        let retrieved = retrieved.expect("cache should contain the key");
        let retrieved_len = retrieved.records().await.expect("Failed to decode").len();
        let result_len = result.records().await.expect("Failed to decode").len();
        (retrieved_len == result_len)
            .then_some(())
            .expect("retrieved and result should have same length");
    }

    /// Test that Pingora backend works correctly when the feature is enabled.
    #[cfg(feature = "pingora")]
    #[tokio::test]
    async fn test_pingora_backend_put_and_get() {
        let hasher = RandomState::default();
        let cache: LruCache<CachedQueryResult, _, _> = LruCache::new(
            1024 * 1024, // 1 MB
            Duration::from_secs(60),
            hasher,
            CachingPolicy::Lru,
            CacheEngine::Pingora,
        );

        let key = CacheKey::Query("pingora_test_query", None).as_raw_key(cache.hasher());
        let result = create_test_cached_result().await;

        // Put a value in the cache
        cache.put_raw_key(&key.as_u64(), result.clone()).await;

        // Force pending tasks to complete
        cache.checkpoint().await;

        // Get the value from the cache
        let retrieved = cache.get_raw_key(&key.as_u64()).await;
        let retrieved = retrieved.expect("Pingora cache should contain the key");
        let retrieved_len = retrieved.records().await.expect("Failed to decode").len();
        let result_len = result.records().await.expect("Failed to decode").len();
        (retrieved_len == result_len)
            .then_some(())
            .expect("retrieved and result should have same length");
    }

    /// Test that Pingora backend cache miss works correctly.
    #[cfg(feature = "pingora")]
    #[tokio::test]
    async fn test_pingora_backend_cache_miss() {
        let hasher = RandomState::default();
        let cache: LruCache<CachedQueryResult, _, _> = LruCache::new(
            1024 * 1024, // 1 MB
            Duration::from_secs(60),
            hasher,
            CachingPolicy::Lru,
            CacheEngine::Pingora,
        );

        let key = CacheKey::Query("nonexistent_key", None).as_raw_key(cache.hasher());

        // Try to get a value that doesn't exist
        let retrieved = cache.get_raw_key(&key.as_u64()).await;
        retrieved
            .is_none()
            .then_some(())
            .expect("cache should not contain nonexistent key");
    }

    /// Test that Pingora backend `invalidate_all` works correctly.
    #[cfg(feature = "pingora")]
    #[tokio::test]
    async fn test_pingora_backend_invalidate_all() {
        let hasher = RandomState::default();
        let cache: LruCache<CachedQueryResult, _, _> = LruCache::new(
            1024 * 1024, // 1 MB
            Duration::from_secs(60),
            hasher,
            CachingPolicy::Lru,
            CacheEngine::Pingora,
        );

        let key = CacheKey::Query("pingora_invalidate_test", None).as_raw_key(cache.hasher());
        let result = create_test_cached_result().await;

        // Put a value in the cache
        cache.put_raw_key(&key.as_u64(), result).await;
        cache.checkpoint().await;

        // Verify it's in the cache
        let retrieved = cache.get_raw_key(&key.as_u64()).await;
        retrieved
            .is_some()
            .then_some(())
            .expect("cache should contain the key before invalidation");

        // Invalidate all entries
        cache.invalidate_all().await;

        // Verify the cache is empty
        let retrieved = cache.get_raw_key(&key.as_u64()).await;
        retrieved
            .is_none()
            .then_some(())
            .expect("cache should be empty after invalidate_all");
    }

    /// Test that Pingora backend table invalidation works correctly.
    #[cfg(feature = "pingora")]
    #[tokio::test]
    async fn test_pingora_invalidate_for_table() {
        let hasher = RandomState::default();
        let cache: LruCache<CachedQueryResult, _, _> = LruCache::new(
            1024 * 1024, // 1 MB
            Duration::from_secs(60),
            hasher,
            CachingPolicy::Lru,
            CacheEngine::Pingora,
        );

        let table_ref = TableReference::Bare {
            table: Arc::from("test_table"),
        };
        let result = create_test_cached_result().await;

        // Put a value in the cache
        let key = CacheKey::Query("pingora_table_test", None).as_raw_key(cache.hasher());
        cache.put_raw_key(&key.as_u64(), result).await;
        cache.checkpoint().await;

        // Verify the value is in the cache
        let retrieved = cache.get_raw_key(&key.as_u64()).await;
        retrieved
            .is_some()
            .then_some(())
            .expect("cache should contain the key before invalidation");

        // Invalidate the cache for the table
        cache
            .invalidate_for_table(table_ref)
            .expect("should invalidate cache for pingora");

        // Force pending tasks
        cache.checkpoint().await;

        // Verify the value is no longer in the cache
        let retrieved = cache.get_raw_key(&key.as_u64()).await;
        retrieved
            .is_none()
            .then_some(())
            .expect("cache should not contain key after table invalidation");
    }

    /// Test Pingora backend table invalidation with multiple entries - only matching tables removed.
    #[cfg(feature = "pingora")]
    #[tokio::test]
    async fn test_pingora_invalidate_for_table_selective() {
        let hasher = RandomState::default();
        let cache: LruCache<CachedQueryResult, _, _> = LruCache::new(
            1024 * 1024, // 1 MB
            Duration::from_secs(60),
            hasher,
            CachingPolicy::Lru,
            CacheEngine::Pingora,
        );

        // Create results for different tables
        let result_test_table = create_test_cached_result().await; // references "test_table"

        // Create a result that references a different table
        let different_table_batch = create_test_record_batch();
        let mut different_input_tables = HashSet::new();
        different_input_tables.insert(TableReference::Bare {
            table: Arc::from("other_table"),
        });
        let encoder = crate::encoding::get_encoder(spicepod::component::caching::Encoding::None);
        let result_other_table = CachedQueryResult::from_batches(
            &[different_table_batch],
            Arc::new(different_input_tables),
            std::time::Instant::now(),
            encoder,
        )
        .await
        .expect("Failed to create cached result");

        // Insert both into cache
        let key1 = CacheKey::Query("query_test_table", None).as_raw_key(cache.hasher());
        let key2 = CacheKey::Query("query_other_table", None).as_raw_key(cache.hasher());

        cache.put_raw_key(&key1.as_u64(), result_test_table).await;
        cache.put_raw_key(&key2.as_u64(), result_other_table).await;
        cache.checkpoint().await;

        // Both should be in cache
        assert!(
            cache.get_raw_key(&key1.as_u64()).await.is_some(),
            "key1 should be in cache"
        );
        assert!(
            cache.get_raw_key(&key2.as_u64()).await.is_some(),
            "key2 should be in cache"
        );

        // Invalidate only "test_table"
        let table_ref = TableReference::Bare {
            table: Arc::from("test_table"),
        };
        cache
            .invalidate_for_table(table_ref)
            .expect("should invalidate cache");
        cache.checkpoint().await;

        // key1 (test_table) should be removed
        assert!(
            cache.get_raw_key(&key1.as_u64()).await.is_none(),
            "key1 should be removed after invalidation"
        );

        // key2 (other_table) should still be present
        assert!(
            cache.get_raw_key(&key2.as_u64()).await.is_some(),
            "key2 should still be in cache"
        );
    }

    /// Test Pingora backend TTL expiration works correctly.
    #[cfg(feature = "pingora")]
    #[tokio::test]
    async fn test_pingora_ttl_expiration() {
        let hasher = RandomState::default();
        let cache: LruCache<CachedQueryResult, _, _> = LruCache::new(
            1024 * 1024,                // 1 MB
            Duration::from_millis(100), // Short TTL for testing
            hasher,
            CachingPolicy::Lru,
            CacheEngine::Pingora,
        );

        let key = CacheKey::Query("pingora_ttl_test", None).as_raw_key(cache.hasher());
        let result = create_test_cached_result().await;

        // Put a value in the cache
        cache.put_raw_key(&key.as_u64(), result).await;
        cache.checkpoint().await;

        // Value should exist immediately
        assert!(
            cache.get_raw_key(&key.as_u64()).await.is_some(),
            "value should exist before TTL"
        );

        // Wait for TTL to expire
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Value should be expired
        assert!(
            cache.get_raw_key(&key.as_u64()).await.is_none(),
            "value should be expired after TTL"
        );
    }

    /// Test Pingora backend size tracking works correctly.
    #[cfg(feature = "pingora")]
    #[tokio::test]
    async fn test_pingora_size_tracking() {
        let hasher = RandomState::default();
        let cache: LruCache<CachedQueryResult, _, _> = LruCache::new(
            1024 * 1024, // 1 MB
            Duration::from_secs(60),
            hasher,
            CachingPolicy::Lru,
            CacheEngine::Pingora,
        );

        // Cache should start empty
        assert_eq!(cache.item_count().await, 0);

        let key1 = CacheKey::Query("pingora_size_test_1", None).as_raw_key(cache.hasher());
        let key2 = CacheKey::Query("pingora_size_test_2", None).as_raw_key(cache.hasher());
        let result1 = create_test_cached_result().await;
        let result2 = create_test_cached_result().await;

        // Insert first entry
        cache.put_raw_key(&key1.as_u64(), result1).await;
        cache.checkpoint().await;
        assert_eq!(cache.item_count().await, 1);
        let size_after_first = cache.size_bytes().await;
        assert!(size_after_first > 0, "size should be positive after insert");

        // Insert second entry
        cache.put_raw_key(&key2.as_u64(), result2).await;
        cache.checkpoint().await;
        assert_eq!(cache.item_count().await, 2);
        let size_after_second = cache.size_bytes().await;
        assert!(
            size_after_second > size_after_first,
            "size should increase after second insert"
        );

        // Remove first entry
        cache.invalidate_all().await;
        cache.checkpoint().await;
        assert_eq!(cache.item_count().await, 0);
        assert_eq!(cache.size_bytes().await, 0);
    }

    /// Test Pingora backend with search results table invalidation.
    #[cfg(feature = "pingora")]
    #[tokio::test]
    async fn test_pingora_search_cache_invalidate_for_table() {
        let hasher = RandomState::default();
        let cache: LruCache<CachedSearchResult, _, _> = LruCache::new(
            1024 * 1024, // 1 MB
            Duration::from_secs(60),
            hasher,
            CachingPolicy::Lru,
            CacheEngine::Pingora,
        );

        let table_ref = TableReference::Bare {
            table: Arc::from("test_table"),
        };
        let result = create_test_cached_search_result();

        let raw_cache_key = 789_012u64;

        // Put a value in the cache
        cache.put_raw_key(&raw_cache_key, result).await;
        cache.checkpoint().await;

        // Verify the value is in the cache
        assert!(
            cache.get_raw_key(&raw_cache_key).await.is_some(),
            "search result should be in cache"
        );

        // Invalidate the cache for the table
        cache
            .invalidate_for_table(table_ref)
            .expect("should invalidate search cache for pingora");
        cache.checkpoint().await;

        // Verify the value is no longer in the cache
        assert!(
            cache.get_raw_key(&raw_cache_key).await.is_none(),
            "search result should be removed after table invalidation"
        );
    }
}
