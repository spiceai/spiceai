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

use crate::Sizeable;
use crate::backend::{CacheBackend, CacheBackendBuilder};
use async_trait::async_trait;
use moka::future::Cache;
use moka::policy::EvictionPolicy;
use std::hash::BuildHasher;

/// Moka-based cache backend implementation
///
/// Provides:
/// - Built-in TTL support (no manual tracking needed)
/// - Atomic operations (no race conditions)
/// - Stable and well-tested
#[derive(Clone)]
pub struct MokaBackend<V, T>
where
    V: Sizeable + Clone + Send + Sync + 'static,
    T: BuildHasher + Clone + Send + Sync + 'static,
{
    cache: Cache<u64, V, T>,
}

impl<V, T> MokaBackend<V, T>
where
    V: Sizeable + Clone + Send + Sync + 'static,
    T: BuildHasher + Clone + Send + Sync + 'static,
{
    #[must_use]
    pub fn new(builder: &CacheBackendBuilder, hasher: T) -> Self {
        let cache = Cache::builder()
            .max_capacity(builder.max_size())
            .time_to_live(builder.ttl())
            .eviction_policy(EvictionPolicy::lru())
            .weigher(|_key, value: &V| value.get_memory_size().try_into().unwrap_or(u32::MAX))
            .build_with_hasher(hasher);

        Self { cache }
    }
}

#[async_trait]
impl<V, T> CacheBackend<V> for MokaBackend<V, T>
where
    V: Sizeable + Clone + Send + Sync + 'static,
    T: BuildHasher + Clone + Send + Sync + 'static,
{
    async fn insert(&self, key: u64, value: V, _size: usize) {
        // Moka handles size automatically via weigher
        self.cache.insert(key, value).await;
    }

    async fn get(&self, key: &u64) -> Option<V> {
        self.cache.get(key).await
    }

    async fn remove(&self, key: &u64) {
        self.cache.invalidate(key).await;
    }

    async fn clear(&self) {
        self.cache.invalidate_all();
        self.cache.run_pending_tasks().await;
    }

    async fn iter_keys(&self) -> Vec<u64> {
        // Moka doesn't provide a keys iterator, so we need to collect them
        // This is not ideal but matches the trait requirement
        self.cache.iter().map(|(k, _)| *k).collect()
    }

    async fn len(&self) -> usize {
        self.cache.entry_count().try_into().unwrap_or(usize::MAX)
    }
}
