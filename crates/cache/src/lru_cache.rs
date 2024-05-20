/*
Copyright 2024 The Spice.ai OSS Authors

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
use crate::QueryResultCache;
use crate::Result;
use arrow::array::RecordBatch;
use async_trait::async_trait;
use datafusion::logical_expr::LogicalPlan;
use moka::future::Cache;
use std::hash::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::Arc;
use std::time::Duration;

pub struct LruCache {
    cache: Cache<u64, Arc<Vec<RecordBatch>>>,
}

impl LruCache {
    pub fn new(cache_max_size: u64, ttl: Duration) -> Self {
        let cache: Cache<u64, Arc<Vec<RecordBatch>>> = Cache::builder()
            .time_to_live(ttl)
            .weigher(|_key, value: &Arc<Vec<RecordBatch>>| -> u32 {
                let val: usize = value
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
            .build();

        LruCache { cache }
    }
}

#[async_trait]
impl QueryResultCache for LruCache {
    async fn get(&self, plan: &LogicalPlan) -> Result<Option<Arc<Vec<RecordBatch>>>> {
        let key = key_for_logical_plan(plan);
        match self.cache.get(&key).await {
            Some(value) => Ok(Some(value)),
            None => Ok(None),
        }
    }

    async fn put(&self, plan: &LogicalPlan, result: Arc<Vec<RecordBatch>>) -> Result<()> {
        let key = key_for_logical_plan(plan);
        self.cache.insert(key, result).await;
        Ok(())
    }
}

pub fn key_for_logical_plan(plan: &LogicalPlan) -> u64 {
    let mut hasher = DefaultHasher::new();
    plan.hash(&mut hasher);
    hasher.finish()
}
