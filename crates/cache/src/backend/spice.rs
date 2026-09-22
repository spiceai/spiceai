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
use std::sync::Arc;
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

impl<V: CacheMetrics + Send + Sync + 'static> EvictionListener for MetricsListener<V> {
    fn on_evict(reason: sharded_cache::EvictionReason) {
        V::record_eviction(map_reason(reason));
    }
}

/// Spice sharded cache implementing [`CacheBackend`].
pub struct SpiceBackend<V: CacheMetrics + Clone + Send + Sync + 'static> {
    cache: Arc<ShardedCache<V, MetricsListener<V>>>,
}

impl<V> SpiceBackend<V>
where
    V: Sizeable + CacheMetrics + Clone + Send + Sync + 'static,
{
    /// Create a backend with an explicit byte budget, TTL, and eviction policy.
    #[must_use]
    pub fn new(max_capacity: u64, ttl: Duration, policy: EvictionPolicy) -> Self {
        Self {
            cache: Arc::new(ShardedCache::new(max_capacity, ttl, policy)),
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
}

#[async_trait]
impl<V> CacheBackend<V> for SpiceBackend<V>
where
    V: Sizeable + CacheMetrics + Clone + Send + Sync + 'static,
{
    async fn insert(&self, key: u64, value: V) {
        // Admission may expire a shard and walk LFU victims; keep that (and
        // O(result-size) `get_memory_size` for CachedQueryResult) off the Tokio
        // worker the way `clear` / `run_pending_tasks` already do.
        let cache = Arc::clone(&self.cache);
        if let Err(err) = tokio::task::spawn_blocking(move || {
            let weight = value.get_memory_size();
            cache.insert(key, value, weight);
        })
        .await
        {
            tracing::debug!("Spice cache insert task did not finish: {err}");
        }
    }

    async fn replace_if(
        &self,
        key: u64,
        value: V,
        should_replace: &(dyn for<'v> Fn(&'v V) -> bool + Send + Sync),
    ) -> bool {
        // Predicate is borrowed for the call, so we cannot `spawn_blocking`
        // without owning it. On a multi-thread runtime, `block_in_place` keeps
        // size + replace/evict off the cooperative scheduler the same way as
        // `insert`. On Tokio's current-thread runtime (e.g. plain
        // `#[tokio::test]`), `block_in_place` panics — run the sync path
        // inline instead.
        let cache = Arc::clone(&self.cache);
        let run = || {
            let weight = value.get_memory_size();
            let keep_ttl = value.keep_remaining_ttl();
            cache.replace_if(key, value, weight, keep_ttl, should_replace)
        };
        match tokio::runtime::Handle::try_current() {
            Ok(handle)
                if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::CurrentThread =>
            {
                run()
            }
            _ => tokio::task::block_in_place(run),
        }
    }

    async fn get(&self, key: &u64) -> Option<Arc<V>> {
        self.cache.get(key)
    }

    async fn remove(&self, key: &u64) -> Option<V> {
        self.cache.remove(key)
    }

    async fn clear(&self) {
        // `clear` walks every shard. Do it off the Tokio worker so a large
        // cache cannot stall `/health` the way `run_pending_tasks` would.
        let cache = Arc::clone(&self.cache);
        if let Err(err) = tokio::task::spawn_blocking(move || cache.clear()).await {
            tracing::debug!("Spice cache clear task did not finish: {err}");
        }
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
        let cache = Arc::clone(&self.cache);
        if let Err(err) = tokio::task::spawn_blocking(move || cache.run_pending_tasks()).await {
            tracing::debug!("Spice cache maintenance task did not finish: {err}");
        }
    }

    async fn invalidate_matching(
        &self,
        predicate: &(dyn for<'v> Fn(&'v V) -> bool + Send + Sync),
    ) -> usize {
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
