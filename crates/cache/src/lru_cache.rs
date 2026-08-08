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
use crate::metrics::{CacheMetrics, EvictionReason};
use crate::{CacheProvider, get_hash_builder};
use async_trait::async_trait;
use byte_unit::Byte;
use datafusion::sql::TableReference;
use moka::future::Cache;
use moka::notification::RemovalCause;
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

/// Maps moka's removal cause onto the reason reported on `*_cache_evictions`,
/// or `None` for a removal that did not take an entry out of the cache.
///
/// `moka`'s own `RemovalCause::was_evicted` covers only `Expired` and `Size`,
/// which leaves out the cause that dominates an accelerated deployment:
/// `invalidate_entries_if` — how a refresh or a DML write drops the entries
/// referencing a table — delivers `Explicit`. An entry removed that way is just
/// as gone as one reclaimed under size pressure, and a query that would have
/// been served from it now misses, so it belongs on the counter.
///
/// `Replaced` is the one cause that is not an eviction: the key stays cached and
/// only its value was rewritten, so nothing was lost.
fn eviction_reason(cause: RemovalCause) -> Option<EvictionReason> {
    match cause {
        RemovalCause::Size => Some(EvictionReason::Size),
        RemovalCause::Expired => Some(EvictionReason::Expired),
        RemovalCause::Explicit => Some(EvictionReason::Invalidated),
        RemovalCause::Replaced => None,
    }
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
            if let Some(reason) = eviction_reason(cause) {
                V::record_eviction(reason);
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
                    crate::resolved_table_match(value.as_table_refs().as_ref(), &table_ref)
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
                        && crate::resolved_table_match(value.as_table_refs().as_ref(), &table_ref)
                    {
                        keys_to_remove.push(key);
                    }
                }
                keys_to_remove
            });

            // Nothing counts these for us: the eviction listener belongs to the
            // moka cache, which this engine does not have, so a removal here is
            // only observable if it is recorded at the call site.
            for key in keys_to_remove {
                if futures::executor::block_on(backend.remove(&key)).is_some() {
                    V::record_eviction(EvictionReason::Invalidated);
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
        create_test_cached_result_with_table(TableReference::bare("test_table")).await
    }

    async fn create_test_cached_result_with_table(table: TableReference) -> CachedQueryResult {
        let record_batch = create_test_record_batch();
        let input_tables = HashSet::from([table]);

        let encoder = crate::encoding::get_encoder(spicepod::component::caching::Encoding::None);

        CachedQueryResult::from_batches(
            vec![record_batch],
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
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
            Duration::from_mins(1),
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
            Duration::from_mins(1),
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
            Duration::from_mins(1),
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

    /// Regression test for #11266: cache invalidation must resolve both the
    /// stored and the invalidating table reference to fully-qualified form, so a
    /// differently-qualified entry (e.g. `spice.public.foo`) is still
    /// invalidated by a bare/partial reference (e.g. `foo`) for the same table,
    /// and vice versa. Exact `TableReference` equality misses these, leaving
    /// stale rows served as fresh cache hits until TTL.
    #[rstest]
    // Stored fully-qualified, invalidated bare.
    #[case::full_invalidated_by_bare(
        TableReference::full("spice", "public", "foo"),
        TableReference::bare("foo"),
        true
    )]
    // Stored bare, invalidated fully-qualified.
    #[case::bare_invalidated_by_full(
        TableReference::bare("foo"),
        TableReference::full("spice", "public", "foo"),
        true
    )]
    // Stored partial, invalidated bare (same default catalog).
    #[case::partial_invalidated_by_bare(
        TableReference::partial("public", "foo"),
        TableReference::bare("foo"),
        true
    )]
    // Different physical table — must NOT be invalidated.
    #[case::different_table_preserved(
        TableReference::full("spice", "public", "foo"),
        TableReference::bare("bar"),
        false
    )]
    // Different (non-default) schema — must NOT be invalidated.
    #[case::different_schema_preserved(
        TableReference::full("spice", "other", "foo"),
        TableReference::bare("foo"),
        false
    )]
    #[tokio::test]
    async fn test_cache_invalidate_resolves_qualification(
        #[case] stored: TableReference,
        #[case] invalidate_with: TableReference,
        #[case] expect_invalidated: bool,
    ) {
        let cache: LruCache<CachedQueryResult, _, _> = LruCache::new(
            10,
            Duration::from_mins(1),
            RandomState::default(),
            CachingPolicy::Lru,
            CacheEngine::Moka,
        );
        let result = create_test_cached_result_with_table(stored).await;

        let key = CacheKey::Query("test_query", None).as_raw_key(cache.hasher());
        cache.put_raw_key(&key.as_u64(), result).await;

        assert!(
            cache.get_raw_key(&key.as_u64()).await.is_some(),
            "cache should contain the key before invalidation"
        );

        cache
            .invalidate_for_table(invalidate_with)
            .expect("should invalidate cache");

        assert_eq!(
            cache.get_raw_key(&key.as_u64()).await.is_none(),
            expect_invalidated,
            "invalidation outcome mismatch"
        );
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
            Duration::from_mins(1),
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
            Duration::from_mins(1),
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
            Duration::from_mins(1),
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
            Duration::from_mins(1),
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
            Duration::from_mins(1),
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
            Duration::from_mins(1),
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
            Duration::from_mins(1),
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
            vec![different_table_batch],
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
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
            Duration::from_mins(1),
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
            Duration::from_mins(1),
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

    /// Regression test for #12687.
    ///
    /// A refresh drops its table's entries through `invalidate_entries_if`, which
    /// moka reports as `Explicit` — the dominant removal cause on an accelerated
    /// dataset. `RemovalCause::was_evicted` covers only `Expired` and `Size`, so
    /// the mapping has to name `Explicit` itself for that removal to be counted.
    #[test]
    fn invalidation_is_an_eviction_but_a_replaced_value_is_not() {
        assert_eq!(
            eviction_reason(RemovalCause::Explicit),
            Some(EvictionReason::Invalidated),
            "a refresh or DML invalidation removes the entry, so it must be counted"
        );
        assert_eq!(
            eviction_reason(RemovalCause::Size),
            Some(EvictionReason::Size),
            "size pressure must stay separable from invalidation"
        );
        assert_eq!(
            eviction_reason(RemovalCause::Expired),
            Some(EvictionReason::Expired)
        );
        assert_eq!(
            eviction_reason(RemovalCause::Replaced),
            None,
            "a replaced value leaves the key cached, so nothing was evicted"
        );
    }

    /// A cached value whose eviction reports are counted in-process, so a test
    /// can assert what the cache actually reported without standing up an
    /// `OpenTelemetry` pipeline. [`CacheMetrics`] is implemented on the type
    /// rather than on an instance, so each test needs its own type to keep a
    /// count only it can move.
    macro_rules! counting_value {
        ($name:ident, $counter:ident) => {
            static $counter: AtomicU64 = AtomicU64::new(0);

            #[derive(Clone)]
            struct $name(CachedQueryResult);

            impl Sizeable for $name {
                fn get_memory_size(&self) -> usize {
                    self.0.get_memory_size()
                }
            }

            impl AsTableRefs for $name {
                fn as_table_refs(&self) -> Arc<HashSet<TableReference>> {
                    self.0.as_table_refs()
                }
            }

            impl CacheMetrics for $name {
                fn record_hit() {}
                fn record_miss() {}
                fn record_request() {}
                fn record_item_count(_count: u64) {}
                fn record_size(_size: u64) {}
                fn record_max_size(_size: u64) {}
                fn update_hit_ratio(_hits: u64, _total: u64) {}
                fn publish_counters_at_zero() {}

                fn record_eviction(reason: EvictionReason) {
                    if reason == EvictionReason::Invalidated {
                        $counter.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        };
    }

    counting_value!(MokaCountedValue, MOKA_INVALIDATIONS);

    /// Regression test for #12687: a refresh drops its table's entries through
    /// `invalidate_entries_if`, which moka reports as `Explicit`. The removal that
    /// dominates an accelerated dataset has to reach the eviction counter.
    #[tokio::test]
    async fn moka_invalidation_is_reported_as_an_eviction() {
        let cache: LruCache<MokaCountedValue, _, _> = LruCache::new(
            1024 * 1024,
            Duration::from_mins(1),
            RandomState::default(),
            CachingPolicy::Lru,
            CacheEngine::Moka,
        );

        let table_ref = TableReference::bare("counted_table");
        let key = CacheKey::Query("counted_query", None).as_raw_key(cache.hasher());
        let value = MokaCountedValue(create_test_cached_result_with_table(table_ref.clone()).await);
        cache.put_raw_key(&key.as_u64(), value).await;

        cache
            .invalidate_for_table(table_ref)
            .expect("should invalidate cache");
        cache.checkpoint().await;

        assert!(
            cache.get_raw_key(&key.as_u64()).await.is_none(),
            "the entry must actually be gone, or the count below proves nothing"
        );
        assert_eq!(
            MOKA_INVALIDATIONS.load(Ordering::Relaxed),
            1,
            "invalidating the entry's table must report one eviction"
        );
    }

    #[cfg(feature = "pingora")]
    counting_value!(PingoraCountedValue, PINGORA_INVALIDATIONS);

    /// The Pingora engine has no moka cache, so its invalidation removes each key
    /// directly and never reaches an eviction listener. Without an explicit
    /// record at that call site the removal is invisible on every engine build.
    #[cfg(feature = "pingora")]
    #[tokio::test]
    async fn pingora_invalidation_is_reported_as_an_eviction() {
        let cache: LruCache<PingoraCountedValue, _, _> = LruCache::new(
            1024 * 1024,
            Duration::from_mins(1),
            RandomState::default(),
            CachingPolicy::Lru,
            CacheEngine::Pingora,
        );

        let table_ref = TableReference::bare("counted_table");
        let key = CacheKey::Query("counted_query", None).as_raw_key(cache.hasher());
        let value =
            PingoraCountedValue(create_test_cached_result_with_table(table_ref.clone()).await);
        cache.put_raw_key(&key.as_u64(), value).await;

        cache
            .invalidate_for_table(table_ref)
            .expect("should invalidate cache");
        cache.checkpoint().await;

        assert!(
            cache.get_raw_key(&key.as_u64()).await.is_none(),
            "the entry must actually be gone, or the count below proves nothing"
        );
        assert_eq!(
            PINGORA_INVALIDATIONS.load(Ordering::Relaxed),
            1,
            "the Pingora removal path must report the eviction itself"
        );
    }
}
