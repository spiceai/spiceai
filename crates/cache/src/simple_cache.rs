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

use crate::{
    AsTableRefs, CacheProvider, FailedToInvalidateCacheSnafu, HashProvider, Result,
    TabledCacheProvider,
};
use async_trait::async_trait;
use byte_unit::Byte;
use datafusion::sql::TableReference;
use moka::future::Cache;
use snafu::ResultExt;
use std::fmt::Display;
use std::hash::{BuildHasher, Hasher};
use std::sync::Arc;
use std::time::Duration;

// 'static is required by a bound from moka::Cache
pub struct SimpleCache<
    V: Clone + Send + Sync + 'static,
    T: BuildHasher + Clone + Send + Sync + 'static,
> {
    cache: Cache<u64, V, T>,
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
            .field("cache_size", &self.cache.weighted_size())
            .field("item_count", &self.cache.entry_count())
            .finish_non_exhaustive()
    }
}

impl<V: Clone + Send + Sync + 'static, T: BuildHasher + Clone + Send + Sync + 'static>
    SimpleCache<V, T>
{
    pub fn new(cache_max_size: u64, ttl: Duration, hasher: T) -> Self {
        let cache: Cache<u64, V, T> = Cache::builder()
            .time_to_live(ttl)
            .max_capacity(cache_max_size)
            .support_invalidation_closures()
            .build_with_hasher(hasher.clone());

        SimpleCache {
            cache,
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
        self.cache.get(key).await
    }

    async fn put_raw_key(&self, key: &u64, value: V) {
        self.cache.insert(*key, value).await;
    }

    fn invalidate_all(&self) {
        self.cache.invalidate_all();
    }

    fn size_bytes(&self) -> u64 {
        self.cache.weighted_size()
    }

    fn item_count(&self) -> u64 {
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
impl<V: AsTableRefs + Clone + Send + Sync + 'static, T: BuildHasher + Clone + Send + Sync + 'static>
    TabledCacheProvider<V> for SimpleCache<V, T>
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

        CachedQueryResult {
            records: Arc::new(vec![record_batch.clone()]),
            schema: Arc::new(record_batch.schema().as_ref().to_owned()),
            input_tables: Arc::new(input_tables),
        }
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
