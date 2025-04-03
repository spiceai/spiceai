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
        match self.cache.get(&key.as_raw_key().0).await {
            Some(value) => Ok(Some(value)),
            None => Ok(None),
        }
    }

    async fn put<'a>(&self, key: CacheKey<'a>, result: CachedQueryResult) -> Result<()> {
        self.cache.insert(key.as_raw_key().0, result).await;
        Ok(())
    }

    async fn put_raw_key(&self, raw_key: RawCacheKey, result: CachedQueryResult) -> Result<()> {
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
