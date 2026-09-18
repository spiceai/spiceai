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

//! Spice sharded-cache backend.
//!
//! This is the sole `LruCache` engine for SQL, search, and embeddings results.
//! Get is non-destructive (spiceai/spiceai#12985). Keys map to shard `key % 16`.

use super::{CacheBackend, CacheBackendBuilder};
use crate::Sizeable;
use crate::metrics::{CacheMetrics, EvictionReason};
use async_trait::async_trait;
use sharded_cache::{EvictionListener, EvictionPolicy, ShardedCache};
use std::marker::PhantomData;
use std::time::Duration;

/// Maps the crate-local eviction reason onto the metric label.
fn map_reason(reason: sharded_cache::EvictionReason) -> EvictionReason {
    match reason {
        sharded_cache::EvictionReason::Size => EvictionReason::Size,
        sharded_cache::EvictionReason::Expired => EvictionReason::Expired,
        sharded_cache::EvictionReason::Invalidated => EvictionReason::Invalidated,
    }
}

/// Forwards evictions to [`CacheMetrics`] on `V`.
struct MetricsListener<V>(PhantomData<fn() -> V>);

impl<V: CacheMetrics> EvictionListener for MetricsListener<V> {
    fn on_evict(reason: sharded_cache::EvictionReason) {
        V::record_eviction(map_reason(reason));
    }
}

/// Spice sharded cache implementing [`CacheBackend`].
pub struct SpiceBackend<V: Clone + Send + 'static> {
    cache: ShardedCache<V, MetricsListener<V>>,
}

impl<V> SpiceBackend<V>
where
    V: Sizeable + CacheMetrics + Clone + Send + Sync + 'static,
{
    /// Create a backend with an explicit byte budget, TTL, and eviction policy.
    #[must_use]
    pub fn new(max_capacity: u64, ttl: Duration, policy: EvictionPolicy) -> Self {
        Self {
            cache: ShardedCache::new(max_capacity, ttl, policy),
        }
    }

    /// Create a backend from the shared builder plus an eviction policy.
    #[must_use]
    pub fn from_builder(builder: &CacheBackendBuilder, policy: EvictionPolicy) -> Self {
        Self::new(builder.max_capacity(), builder.ttl(), policy)
    }

    /// Drop every entry whose value satisfies `predicate` without promoting survivors.
    pub fn invalidate_matching<F>(&self, predicate: F) -> usize
    where
        F: Fn(&V) -> bool,
    {
        self.cache.invalidate_matching(predicate)
    }

    /// Keys most-recently-used first within each shard. Test helper for recency.
    #[cfg(test)]
    pub(crate) fn keys_in_lru_order(&self) -> Vec<u64> {
        self.cache.keys_in_lru_order()
    }
}

#[async_trait]
impl<V> CacheBackend<V> for SpiceBackend<V>
where
    V: Sizeable + CacheMetrics + Clone + Send + Sync + 'static,
{
    async fn insert(&self, key: u64, value: V) {
        let weight = value.get_memory_size();
        self.cache.insert(key, value, weight);
    }

    async fn get(&self, key: &u64) -> Option<V> {
        self.cache.get(key)
    }

    async fn remove(&self, key: &u64) -> Option<V> {
        self.cache.remove(key)
    }

    async fn clear(&self) {
        self.cache.clear();
    }

    async fn iter_keys(&self) -> Vec<u64> {
        self.cache.iter_keys()
    }

    async fn len(&self) -> usize {
        self.cache.len()
    }

    async fn weighted_size(&self) -> u64 {
        self.cache.weighted_size()
    }

    async fn run_pending_tasks(&self) {
        self.cache.run_pending_tasks();
    }

    async fn invalidate_matching(&self, predicate: &(dyn Fn(&V) -> bool + Send + Sync)) -> usize {
        self.cache.invalidate_matching(predicate)
    }
}

#[cfg(test)]
mod tests {
    use super::map_reason;
    use crate::metrics::EvictionReason;

    #[test]
    fn spice_reasons_map_onto_metric_labels() {
        assert_eq!(
            map_reason(sharded_cache::EvictionReason::Size),
            EvictionReason::Size
        );
        assert_eq!(
            map_reason(sharded_cache::EvictionReason::Expired),
            EvictionReason::Expired
        );
        assert_eq!(
            map_reason(sharded_cache::EvictionReason::Invalidated),
            EvictionReason::Invalidated
        );
    }
}
