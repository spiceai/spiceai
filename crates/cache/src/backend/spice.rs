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

/// Largest value, in bytes, that `SpiceBackend::insert` tries to admit on the
/// calling task before it uses the blocking pool. A point lookup's result is a
/// few KiB.
const INLINE_ADMISSION_MAX_BYTES: usize = 64 * 1024;

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
        // Admission may expire a shard, walk eviction victims and drop them, and
        // what that costs depends on the cache, not on the value. That work runs
        // on the blocking pool, off the Tokio worker, the way `clear` and
        // `run_pending_tasks` already run. A small value that fits without it — a
        // point lookup's result, say — is admitted here by `try_insert`, because
        // the round trip through the pool costs more than that admission: a
        // wake-up on a pool thread and another back here, about 20µs added to
        // every uncached query, and at high query rates contention on the pool's
        // queue. Sizing walks a value's batches and columns, never its rows, so it
        // is done here to choose.
        let weight = value.get_memory_size();
        let value = if weight <= INLINE_ADMISSION_MAX_BYTES {
            match self.cache.try_insert(key, value, weight) {
                Ok(()) => return,
                Err(value) => value,
            }
        } else {
            value
        };
        let cache = Arc::clone(&self.cache);
        if let Err(err) =
            tokio::task::spawn_blocking(move || cache.insert(key, value, weight)).await
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
    use super::{SpiceBackend, map_reason};
    use crate::Sizeable;
    use crate::backend::CacheBackend;
    use crate::metrics::{CacheMetrics, EvictionReason, InvalidationMode, StaleRejectionReason};
    use futures::FutureExt;
    use rstest::rstest;
    use sharded_cache::EvictionPolicy;
    use std::sync::mpsc;
    use std::time::Duration;

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

    const ENTRY_BYTES: usize = 1024;

    /// A value that weighs [`ENTRY_BYTES`].
    #[derive(Clone)]
    struct Entry;

    impl Sizeable for Entry {
        fn get_memory_size(&self) -> usize {
            ENTRY_BYTES
        }
    }

    impl CacheMetrics for Entry {
        fn record_hit() {}
        fn record_miss() {}
        fn record_request() {}
        fn record_item_count(_count: u64) {}
        fn record_size(_size: u64) {}
        fn record_max_size(_size: u64) {}
        fn record_eviction(_reason: EvictionReason) {}
        fn record_stale_rejection(_reason: StaleRejectionReason) {}
        fn record_table_invalidation(_mode: InvalidationMode) {}
        fn update_hit_ratio(_hits: u64, _total: u64) {}
        fn publish_counters_at_zero() {}
    }

    /// A runtime whose blocking pool has one thread, kept busy until `release`
    /// is sent. An insert that needs the pool cannot finish on its first poll,
    /// so polling once tells an admission made on the calling task from one
    /// handed to the pool.
    struct HeldPool {
        // Declared first so it is dropped first: a failed assertion unwinds
        // before `release` is sent, and dropping the runtime waits for the
        // held thread, which only a sent or dropped `release` lets go.
        release: mpsc::Sender<()>,
        runtime: tokio::runtime::Runtime,
    }

    impl HeldPool {
        fn new() -> Self {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .max_blocking_threads(1)
                .build()
                .expect("current-thread runtime");
            let (release, released) = mpsc::channel::<()>();
            let (holding, held) = mpsc::channel::<()>();
            runtime.spawn_blocking(move || {
                holding.send(()).expect("report the pool thread as held");
                // A send or a dropped sender both let the thread go.
                released.recv().ok();
            });
            held.recv().expect("the pool's only thread is held");
            Self { release, runtime }
        }
    }

    /// Regression test for the review of spiceai/spiceai#14433: an admission
    /// that has to make room waits for the blocking pool, so eviction never runs
    /// on the Tokio worker, while one that fits is still made on the calling task.
    #[rstest]
    #[case::lru(EvictionPolicy::Lru)]
    #[case::lfu(EvictionPolicy::Lfu)]
    fn only_an_admission_that_fits_runs_on_the_calling_task(#[case] policy: EvictionPolicy) {
        let pool = HeldPool::new();
        pool.runtime.block_on(async {
            let backend =
                SpiceBackend::<Entry>::new(4 * ENTRY_BYTES as u64, Duration::from_mins(1), policy);
            for key in 0..4 {
                assert!(
                    backend.insert(key, Entry).now_or_never().is_some(),
                    "an insert that fits must be admitted on the calling task"
                );
            }
            let mut over_budget = backend.insert(4, Entry);
            assert!(
                (&mut over_budget).now_or_never().is_none(),
                "an insert that has to evict must wait for the blocking pool"
            );
            pool.release.send(()).expect("release the pool thread");
            over_budget.await;
            assert!(
                backend.get(&4).await.is_some(),
                "the pool admits the insert once it runs"
            );
            assert_eq!(
                backend.len().await,
                4,
                "one resident was evicted to make room"
            );
        });
    }

    /// W-TinyLFU admission expires the key's whole shard, so even an insert
    /// that fits waits for the blocking pool.
    #[test]
    fn a_tinylfu_admission_runs_on_the_blocking_pool() {
        let pool = HeldPool::new();
        pool.runtime.block_on(async {
            let backend = SpiceBackend::<Entry>::new(
                1024 * ENTRY_BYTES as u64,
                Duration::from_mins(1),
                EvictionPolicy::TinyLfu,
            );
            let mut first = backend.insert(0, Entry);
            assert!(
                (&mut first).now_or_never().is_none(),
                "a W-TinyLFU admission must wait for the blocking pool"
            );
            pool.release.send(()).expect("release the pool thread");
            first.await;
            assert!(
                backend.get(&0).await.is_some(),
                "the pool admits the insert once it runs"
            );
        });
    }
}
