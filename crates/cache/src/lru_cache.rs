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

use crate::CacheKey;
use crate::CachedQueryResult;
use crate::FailedToInvalidateCacheSnafu;
use crate::QueryResultCache;
use crate::RawCacheKey;
use crate::Result;
use async_trait::async_trait;
use datafusion::sql::TableReference;
use moka::future::Cache;
use snafu::ResultExt;
use std::sync::Arc;
use std::time::Duration;

pub struct LruCache {
    cache: Cache<u64, CachedQueryResult>,
}

impl LruCache {
    pub fn new(cache_max_size: u64, ttl: Duration) -> Self {
        let cache: Cache<u64, CachedQueryResult> = Cache::builder()
            .time_to_live(ttl)
            .weigher(|_key, value: &CachedQueryResult| -> u32 {
                let val: usize = value
                    .records
                    .iter()
                    .map(arrow::array::RecordBatch::get_array_memory_size)
                    .sum();

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
            .build();

        LruCache { cache }
    }
}

#[async_trait]
impl QueryResultCache for LruCache {
    async fn get<'a>(&self, key: CacheKey<'a>) -> Result<Option<CachedQueryResult>> {
        let raw_key = key.as_raw_key();
        self.get_raw_key(&raw_key).await
    }

    async fn get_raw_key(&self, raw_key: &RawCacheKey) -> Result<Option<CachedQueryResult>> {
        match self.cache.get(&raw_key.0).await {
            Some(value) => Ok(Some(value)),
            None => Ok(None),
        }
    }

    async fn put<'a>(&self, key: CacheKey<'a>, result: CachedQueryResult) -> Result<()> {
        self.cache.insert(key.as_raw_key().0, result).await;
        Ok(())
    }

    async fn put_raw_key(&self, raw_key: &RawCacheKey, result: CachedQueryResult) -> Result<()> {
        self.cache.insert(raw_key.0, result).await;
        Ok(())
    }

    async fn invalidate_for_table(&self, table_ref: TableReference) -> Result<()> {
        let table_name = match &table_ref {
            TableReference::Bare { table }
            | TableReference::Partial { table, .. }
            | TableReference::Full { table, .. } => table,
        };
        let table_name = Arc::clone(table_name);
        self.cache
            .invalidate_entries_if(move |_key, value| value.input_tables.contains(&table_ref))
            .context(FailedToInvalidateCacheSnafu { table_name })?;

        Ok(())
    }

    fn size_bytes(&self) -> u64 {
        self.cache.weighted_size()
    }

    fn item_count(&self) -> u64 {
        self.cache.entry_count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::collections::HashSet;
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

    #[tokio::test]
    async fn test_cache_put_and_get() {
        let cache = LruCache::new(10, Duration::from_secs(60));
        let key = CacheKey::Query("test_query", None);
        let result = create_test_cached_result();

        // Put a value in the cache
        cache
            .put(key, result.clone())
            .await
            .expect("Failed to put in cache");

        let key = CacheKey::Query("test_query", None);

        // Get the value from the cache
        let retrieved = cache.get(key).await.expect("Failed to get from cache");
        assert!(retrieved.is_some());
        assert_eq!(
            retrieved.expect("Failed to get from cache").records.len(),
            result.records.len()
        );
    }

    #[tokio::test]
    async fn test_cache_miss() {
        let cache = LruCache::new(10, Duration::from_secs(60));
        let key = CacheKey::Query("nonexistent_query", None);

        // Try to get a non-existent key
        let retrieved = cache.get(key).await.expect("Failed to get from cache");
        assert!(retrieved.is_none());
    }

    #[tokio::test]
    async fn test_cache_put_raw_key() {
        let cache = LruCache::new(10, Duration::from_secs(60));
        let raw_key = CacheKey::Query("test_query", None).as_raw_key();
        let result = create_test_cached_result();

        // Put a value with a raw key
        cache
            .put_raw_key(&raw_key, result.clone())
            .await
            .expect("Failed to put with raw key");

        let retrieved = cache
            .get(CacheKey::Query("test_query", None))
            .await
            .expect("Failed to get from cache");
        assert!(retrieved.is_some());
        assert_eq!(
            retrieved.expect("Failed to get from cache").records.len(),
            result.records.len()
        );
    }

    #[tokio::test]
    async fn test_cache_invalidate_for_table() {
        let cache = LruCache::new(10, Duration::from_secs(60));
        let table_ref = TableReference::Bare {
            table: Arc::from("test_table"),
        };
        let result = create_test_cached_result();

        // Put a value in the cache
        let get_key = || CacheKey::Query("test_query", None);
        let key = get_key();
        cache
            .put(key, result)
            .await
            .expect("Failed to put in cache");

        // Verify the value is in the cache
        let retrieved = cache
            .get(get_key())
            .await
            .expect("Failed to get from cache");
        assert!(retrieved.is_some());

        // Invalidate the cache for the table
        cache
            .invalidate_for_table(table_ref)
            .await
            .expect("Failed to invalidate cache");

        // Verify the value is no longer in the cache
        let retrieved = cache
            .get(get_key())
            .await
            .expect("Failed to get from cache");
        assert!(retrieved.is_none());
    }

    #[tokio::test]
    async fn test_cache_ttl() {
        let cache = LruCache::new(10, Duration::from_millis(100));
        let key = || CacheKey::Query("test_query", None);
        let result = create_test_cached_result();

        // Put a value in the cache
        cache
            .put(key(), result)
            .await
            .expect("Failed to put in cache");

        // Verify the value is in the cache
        let retrieved = cache.get(key()).await.expect("Failed to get from cache");
        assert!(retrieved.is_some());

        // Wait for the TTL to expire
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Verify the value is no longer in the cache
        let retrieved = cache.get(key()).await.expect("Failed to get from cache");
        assert!(retrieved.is_none());
    }
}
