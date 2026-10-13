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

/// Largest value, in bytes, that `SpiceBackend::insert_with_weight` tries to
/// admit on the calling task before it uses the blocking pool. A point lookup's
/// result is a few KiB.
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

    /// Admit `value`, which the caller has already sized to `weight`: what
    /// [`Sizeable::get_memory_size`] returns for it.
    ///
    /// A value of at most `INLINE_ADMISSION_MAX_BYTES` that fits without
    /// eviction or expiry — a point lookup's result, say — is admitted here by
    /// [`ShardedCache::try_insert`], because the round trip through the blocking
    /// pool costs more than that admission: a wake-up on a pool thread and
    /// another back here, about 20µs added to every uncached query, and at high
    /// query rates contention on the pool's queue. Anything else is admitted on
    /// the pool, as [`CacheBackend::insert`] admits every value.
    pub async fn insert_with_weight(&self, key: u64, value: V, weight: usize) {
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

    /// Drop every entry whose value satisfies `predicate` without promoting survivors.
    pub fn invalidate_matching<F>(&self, predicate: F) -> usize
    where
        F: Fn(&V) -> bool,
    {
        self.cache.invalidate_matching(predicate)
    }

    /// Insert `value` or replace the resident only when `admit` accepts what is
    /// stored now (`None` when the key is empty). `weight` must be
    /// [`Sizeable::get_memory_size`] of `value`.
    ///
    /// The predicate is borrowed, so this cannot move onto the blocking pool.
    /// It follows [`CacheBackend::replace_if`]: `block_in_place` on a
    /// multi-thread runtime, inline on a current-thread runtime.
    pub fn insert_if(
        &self,
        key: u64,
        value: V,
        weight: usize,
        admit: &(dyn for<'v> Fn(Option<&'v V>) -> bool + Send + Sync),
    ) -> bool {
        let run = || self.cache.insert_if(key, value, weight, admit);
        match tokio::runtime::Handle::try_current() {
            Ok(handle)
                if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::CurrentThread =>
            {
                run()
            }
            _ => tokio::task::block_in_place(run),
        }
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
        // worker the way `clear` / `run_pending_tasks` already do. A caller that
        // has already sized the value uses `insert_with_weight` instead.
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
    use super::{SpiceBackend, map_reason};
    use crate::Sizeable;
    use crate::backend::CacheBackend;
    use crate::metrics::{CacheMetrics, EvictionReason, InvalidationMode, StaleRejectionReason};
    use futures::FutureExt;
    use rstest::rstest;
    use sharded_cache::EvictionPolicy;
    use std::sync::{Arc, mpsc};
    use std::thread::ThreadId;
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
                    backend
                        .insert_with_weight(key, Entry, ENTRY_BYTES)
                        .now_or_never()
                        .is_some(),
                    "an insert that fits must be admitted on the calling task"
                );
            }
            let mut over_budget = Box::pin(backend.insert_with_weight(4, Entry, ENTRY_BYTES));
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
            let mut first = Box::pin(backend.insert_with_weight(0, Entry, ENTRY_BYTES));
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

    /// A value that records the thread each `get_memory_size` call runs on.
    #[derive(Clone)]
    struct SizedOn(Arc<parking_lot::Mutex<Vec<ThreadId>>>);

    impl Sizeable for SizedOn {
        fn get_memory_size(&self) -> usize {
            self.0.lock().push(std::thread::current().id());
            ENTRY_BYTES
        }
    }

    impl CacheMetrics for SizedOn {
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

    /// Sizing walks the whole value, so a value the caller has not sized is
    /// sized on the blocking pool, and one it has sized is not sized again.
    #[tokio::test(flavor = "current_thread")]
    async fn only_the_blocking_pool_sizes_a_value() {
        let sized_on = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let backend = SpiceBackend::<SizedOn>::new(
            1024 * ENTRY_BYTES as u64,
            Duration::from_mins(1),
            EvictionPolicy::Lru,
        );
        backend.insert(0, SizedOn(Arc::clone(&sized_on))).await;
        backend
            .insert_with_weight(1, SizedOn(Arc::clone(&sized_on)), ENTRY_BYTES)
            .await;
        let sized_on = sized_on.lock().clone();
        assert_eq!(
            sized_on.len(),
            1,
            "only the value without a known weight is sized"
        );
        assert_ne!(
            sized_on[0],
            std::thread::current().id(),
            "a value without a known weight must be sized on the blocking pool"
        );
        assert_eq!(backend.len().await, 2);
    }
}
