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
use crate::CachedQueryResult;
use crate::Result;
use moka::future::Cache;
use std::hash::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;
use std::time::Duration;

pub struct RawLruCache {
    cache: Cache<u64, CachedQueryResult>,
}

impl RawLruCache {
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

        RawLruCache { cache }
    }

    pub async fn get(&self, sql: &str) -> Result<Option<CachedQueryResult>> {
        let key = key_for_sql(sql);
        match self.cache.get(&key).await {
            Some(value) => Ok(Some(value)),
            None => Ok(None),
        }
    }

    pub async fn put(&self, sql: &str, result: CachedQueryResult) -> Result<()> {
        let key = key_for_sql(sql);
        self.cache.insert(key, result).await;
        Ok(())
    }

    pub async fn put_key(&self, plan_key: u64, result: CachedQueryResult) -> Result<()> {
        self.cache.insert(plan_key, result).await;
        Ok(())
    }

    pub fn size_bytes(&self) -> u64 {
        self.cache.weighted_size()
    }

    pub fn item_count(&self) -> u64 {
        self.cache.entry_count()
    }
}

#[must_use]
pub fn key_for_sql(sql: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    sql.hash(&mut hasher);
    hasher.finish()
}
