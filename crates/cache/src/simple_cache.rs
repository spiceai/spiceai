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

use crate::Result;
use async_trait::async_trait;
use datafusion::sql::TableReference;
use moka::future::Cache;
use spicepod::component::runtime::HashingAlgorithm;
use std::hash::{BuildHasher, Hasher};
use std::time::Duration;

// 'static is required by a bound from moka::Cache
pub struct SimpleCache<
    V: Clone + Send + Sync + 'static,
    T: BuildHasher + Clone + Send + Sync + 'static,
> {
    cache: Cache<u64, V, T>,
    hashing_algorithm: HashingAlgorithm,
}

impl<V: Clone + Send + Sync + 'static, T: BuildHasher + Clone + Send + Sync + 'static>
    SimpleCache<V, T>
{
    pub fn new(
        cache_max_size: u64,
        ttl: Duration,
        hasher: T,
        hashing_algorithm: HashingAlgorithm,
    ) -> Self {
        let cache: Cache<u64, V, T> = Cache::builder()
            .time_to_live(ttl)
            .max_capacity(cache_max_size)
            .support_invalidation_closures()
            .build_with_hasher(hasher);

        SimpleCache {
            cache,
            hashing_algorithm,
        }
    }
}

pub trait HashProvider {
    fn hasher(&self) -> Box<dyn Hasher + Send + Sync>;
}

#[async_trait]
pub trait CacheProvider<V: Clone + Send + Sync + 'static>: HashProvider {
    async fn get_raw_key(&self, key: &u64) -> Option<V>;
    async fn put_raw_key(&self, key: &u64, value: V);
    fn invalidate_all(&self);
    fn size_bytes(&self) -> u64;
    fn item_count(&self) -> u64;
    async fn checkpoint(&self);
}

pub trait TableInvalidator {
    fn invalidate_for_table(&self, table_ref: TableReference) -> Result<()>;
}

impl<V: Clone + Send + Sync + 'static, T: BuildHasher + Clone + Send + Sync + 'static> HashProvider
    for SimpleCache<V, T>
{
    fn hasher(&self) -> Box<dyn Hasher + Send + Sync> {
        match self.hashing_algorithm {
            HashingAlgorithm::Siphash => Box::new(std::hash::DefaultHasher::new()),
            HashingAlgorithm::Ahash => Box::new(ahash::AHasher::default()),
        }
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

    async fn checkpoint(&self) {
        self.cache.run_pending_tasks().await;
    }
}
