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

use crate::key::PassthroughHashBuilder;
use crate::{
    AsTableRefs, CacheProvider, FailedToInvalidateCacheSnafu, HashProvider, Result,
    TabledCacheProvider,
};
use async_trait::async_trait;
use byte_unit::Byte;
use datafusion::common::TableReference;
use moka::future::Cache;
use parking_lot::RwLock;
use snafu::ResultExt;
use std::fmt::Display;
use std::hash::{BuildHasher, Hasher};
use std::sync::Arc;
use std::time::Duration;

/// A cached value, behind a lock the eviction listener takes it out of.
///
/// moka removes an entry from its map during maintenance, but destroys it later, through
/// epoch-based reclamation, whenever some later cache operation happens to collect it. A
/// cached logical plan holds the providers it was planned against, so until then a
/// discarded plan keeps a removed dataset's provider, and the query memory that provider
/// has reserved, alive (#14251). moka calls the eviction listener as it removes the entry,
/// for every cause, and emptying the slot there drops the value at once; what moka
/// destroys later is an empty slot.
type Slot<V> = Arc<RwLock<Option<V>>>;

fn slot<V>(value: V) -> Slot<V> {
    Arc::new(RwLock::new(Some(value)))
}

// 'static is required by a bound from moka::Cache
pub struct SimpleCache<
    V: Clone + Send + Sync + 'static,
    T: BuildHasher<Hasher = H> + Clone + Send + Sync + 'static,
    H: Hasher + Send + Sync + 'static,
> {
    cache: Cache<u64, Slot<V>, PassthroughHashBuilder<T>>,
    hasher: T,
    max_size: u64,
    ttl: Duration,
}

impl<
    V: Clone + Send + Sync + 'static,
    T: BuildHasher<Hasher = H> + Clone + Send + Sync + 'static,
    H: Hasher + Send + Sync + 'static,
> Display for SimpleCache<V, T, H>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "max size: {:.2}, item ttl: {:?}",
            Byte::from_u64(self.max_size).get_adjusted_unit(byte_unit::Unit::MiB),
            self.ttl
        )
    }
}

impl<
    V: Clone + Send + Sync + 'static,
    T: BuildHasher<Hasher = H> + Clone + Send + Sync + 'static,
    H: Hasher + Send + Sync + 'static,
> std::fmt::Debug for SimpleCache<V, T, H>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SimpleCache")
            .field("cache_size", &self.cache.weighted_size())
            .field("item_count", &self.cache.entry_count())
            .finish_non_exhaustive()
    }
}

impl<
    V: Clone + Send + Sync + 'static,
    T: BuildHasher<Hasher = H> + Clone + Send + Sync + 'static,
    H: Hasher + Send + Sync + 'static,
> SimpleCache<V, T, H>
{
    pub fn new(cache_max_size: u64, ttl: Duration, hasher: T) -> Self {
        let cache: Cache<u64, Slot<V>, PassthroughHashBuilder<T>> = Cache::builder()
            .time_to_live(ttl)
            .max_capacity(cache_max_size)
            .support_invalidation_closures()
            .eviction_listener(|_key, slot: Slot<V>, _cause| {
                // Taken under the lock, dropped after it: dropping a plan can release the
                // providers it holds, and that should not block a reader of this key.
                let taken = slot.write().take();
                drop(taken);
            })
            .build_with_hasher(PassthroughHashBuilder::new(hasher.clone()));

        SimpleCache {
            cache,
            hasher,
            ttl,
            max_size: cache_max_size,
        }
    }

    async fn get_value(&self, key: &u64) -> Option<V> {
        self.cache.get(key).await?.read().clone()
    }
}

impl<
    V: AsTableRefs + Clone + Send + Sync + 'static,
    T: BuildHasher<Hasher = H> + Clone + Send + Sync + 'static,
    H: Hasher + Send + Sync + 'static,
> SimpleCache<V, T, H>
{
    pub fn as_tabled_provider(self: Arc<Self>) -> Arc<dyn TabledCacheProvider<V> + Send + Sync> {
        self
    }
}

impl<
    V: Clone + Send + Sync + 'static,
    T: BuildHasher<Hasher = H> + Clone + Send + Sync + 'static,
    H: Hasher + Send + Sync + 'static,
> HashProvider for SimpleCache<V, T, H>
{
    fn hasher(&self) -> Box<dyn Hasher> {
        Box::new(self.hasher.build_hasher())
    }
}

#[async_trait]
impl<
    V: Clone + Send + Sync + 'static,
    T: BuildHasher<Hasher = H> + Clone + Send + Sync + 'static,
    H: Hasher + Send + Sync + 'static,
> CacheProvider<V> for SimpleCache<V, T, H>
{
    async fn get_raw_key(&self, key: &u64) -> Option<std::sync::Arc<V>> {
        self.get_value(key).await.map(std::sync::Arc::new)
    }

    async fn get_raw_key_validated(
        &self,
        key: &u64,
        is_valid: &(dyn for<'v> Fn(&'v V) -> bool + Send + Sync),
    ) -> Option<std::sync::Arc<V>> {
        // This cache records no hit/miss metrics, so there is nothing to
        // misattribute; filtering the value is all that is needed.
        let value = self.get_value(key).await.map(std::sync::Arc::new)?;
        if is_valid(value.as_ref()) {
            Some(value)
        } else {
            None
        }
    }

    async fn put_raw_key(&self, key: &u64, value: V) {
        self.cache.insert(*key, slot(value)).await;
    }

    async fn put_raw_key_with_weight(&self, key: &u64, value: V, _weight: usize) {
        // Entries are bounded by count, so there is no weight to bill.
        self.put_raw_key(key, value).await;
    }

    async fn replace_if(
        &self,
        key: &u64,
        value: V,
        should_replace: &(dyn for<'v> Fn(&'v V) -> bool + Send + Sync),
    ) -> bool {
        let outcome = self
            .cache
            .entry(*key)
            .and_compute_with(|current| {
                let replace = current
                    .as_ref()
                    .is_some_and(|entry| entry.value().read().as_ref().is_some_and(should_replace));
                std::future::ready(if replace {
                    moka::ops::compute::Op::Put(slot(value))
                } else {
                    moka::ops::compute::Op::Nop
                })
            })
            .await;
        matches!(outcome, moka::ops::compute::CompResult::ReplacedWith(_))
    }

    async fn put_if(
        &self,
        key: &u64,
        value: V,
        _weight: usize,
        admit: &(dyn for<'v> Fn(Option<&'v V>) -> bool + Send + Sync),
    ) -> bool {
        // Entries are bounded by count, so `weight` is unused. `Op::Put` inserts
        // when the key is empty and replaces when it is occupied. The predicate
        // sees the inner value, not the eviction-listener slot.
        let outcome = self
            .cache
            .entry(*key)
            .and_compute_with(|current| {
                let accept = match current.as_ref() {
                    None => admit(None),
                    Some(entry) => admit(entry.value().read().as_ref()),
                };
                std::future::ready(if accept {
                    moka::ops::compute::Op::Put(slot(value))
                } else {
                    moka::ops::compute::Op::Nop
                })
            })
            .await;
        matches!(
            outcome,
            moka::ops::compute::CompResult::Inserted(_)
                | moka::ops::compute::CompResult::ReplacedWith(_)
        )
    }

    async fn invalidate_all(&self) {
        self.cache.invalidate_all();
        // With an eviction listener installed, moka ends a maintenance pass after 100 ms and
        // leaves the rest for the next one, so one pass may not reach every entry when values
        // are slow to drop. Repeat while passes still remove entries; what a pass cannot
        // remove was inserted after the invalidation and is valid.
        let mut remaining = u64::MAX;
        loop {
            self.cache.run_pending_tasks().await;
            let now = self.cache.entry_count();
            if now == 0 || now >= remaining {
                break;
            }
            remaining = now;
        }
    }

    async fn size_bytes(&self) -> u64 {
        self.cache.run_pending_tasks().await;
        self.cache.weighted_size()
    }

    async fn item_count(&self) -> u64 {
        self.cache.run_pending_tasks().await;
        self.cache.entry_count()
    }

    fn max_size(&self) -> usize {
        usize::try_from(self.max_size).unwrap_or_default()
    }

    async fn checkpoint(&self) {
        self.cache.run_pending_tasks().await;
    }
}

#[async_trait]
impl<
    V: AsTableRefs + Clone + Send + Sync + 'static,
    T: BuildHasher<Hasher = H> + Clone + Send + Sync + 'static,
    H: Hasher + Send + Sync + 'static,
> TabledCacheProvider<V> for SimpleCache<V, T, H>
{
    async fn invalidate_for_table(&self, table_ref: TableReference) -> Result<()> {
        let table_name = crate::invalidated_table_name(&table_ref);
        // The matching entries are removed, and their values dropped by the eviction listener,
        // at moka's next maintenance rather than here: matching walks every cached plan, and
        // this runs on every refresh, every applied CDC batch, and every DML statement, where
        // maintenance folds the predicates registered since its last pass into one walk.
        self.cache
            .invalidate_entries_if(move |_key, slot| {
                slot.read().as_ref().is_some_and(|value| {
                    crate::resolved_table_match(value.as_table_refs().as_ref(), &table_ref)
                })
            })
            .context(FailedToInvalidateCacheSnafu { table_name })?;

        Ok(())
    }

    /// `SimpleCache` keeps no table-change clock, so it cannot answer this and
    /// says so in the only direction that is safe to be wrong in.
    ///
    /// It backs the logical-plan cache, which is invalidated by discarding the
    /// affected plans outright when a hot reload replaces a function or a
    /// catalog — not by comparing a read instant against a per-table mark, so
    /// nothing on that path asks. A caller that did ask would be one this
    /// provider cannot serve correctly, and `false` is the answer that lets it
    /// serve: it would return a result whose tables had moved on. `true`
    /// forfeits the hit instead, which costs a lookup rather than correctness.
    fn tables_changed_since(
        &self,
        _tables: &std::collections::HashSet<TableReference>,
        _since: std::time::Instant,
    ) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use crate::CacheKey;

    use super::*;
    use crate::CachedQueryResult;
    use arrow::array::{Int32Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::TableReference;
    use datafusion::datasource::{MemTable, provider_as_source};
    use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder};
    use rstest::rstest;
    use std::collections::HashSet;
    use std::hash::RandomState;
    use std::sync::{Arc, Weak};
    use std::time::Duration;

    fn create_test_record_batch() -> RecordBatch {
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);
        let array = Int32Array::from(vec![1, 2, 3]);
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)])
            .expect("Failed to create record batch")
    }

    async fn create_test_cached_result() -> CachedQueryResult {
        let record_batch = create_test_record_batch();
        let mut input_tables = HashSet::new();
        input_tables.insert(TableReference::Bare {
            table: Arc::from("test_table"),
        });

        let encoder = crate::encoding::get_encoder(spicepod::component::caching::Encoding::None);

        CachedQueryResult::from_batches(
            vec![record_batch],
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
            Arc::new(input_tables),
            std::time::Instant::now(),
            std::time::Instant::now(),
            encoder,
        )
        .await
        .expect("Failed to create cached result")
    }

    /// The conservative answer is the whole reason this method has no default
    /// on the trait: a provider that cannot track table changes must not be
    /// the one that decides a cached result is still fresh. Nothing calls this
    /// today — the plan cache never asks, and the search and SQL-results paths
    /// are wired to providers that keep a real clock — so this pins the value
    /// against a future caller rather than a current one.
    #[test]
    fn a_cache_without_a_table_clock_reports_every_table_as_changed() {
        let cache: SimpleCache<CachedQueryResult, _, _> =
            SimpleCache::new(10, Duration::from_mins(1), RandomState::default());
        let mut tables = HashSet::new();
        tables.insert(TableReference::bare("orders"));

        assert!(
            TabledCacheProvider::tables_changed_since(&cache, &tables, std::time::Instant::now()),
            "a cache with no table-change clock must forfeit the hit, not serve it"
        );
        assert!(
            TabledCacheProvider::tables_changed_since(
                &cache,
                &HashSet::new(),
                std::time::Instant::now()
            ),
            "the answer must not depend on the table set it was asked about"
        );
    }

    #[rstest]
    #[case::siphash(RandomState::default())]
    #[case::ahash(ahash::RandomState::default())]
    #[tokio::test]
    async fn test_cache_put_and_get<
        H: Hasher + Send + Sync + 'static,
        T: BuildHasher<Hasher = H> + Clone + Send + Sync + 'static,
    >(
        #[case] hasher: T,
    ) {
        let cache: SimpleCache<CachedQueryResult, _, _> =
            SimpleCache::new(10, Duration::from_mins(1), hasher);
        let key = CacheKey::Query("test_query", None).as_raw_key(cache.hasher());
        let result = create_test_cached_result().await;

        // Put a value in the cache
        cache.put_raw_key(&key.as_u64(), result).await;

        let key = CacheKey::Query("test_query", None).as_raw_key(cache.hasher());

        // Get the value from the cache
        let retrieved = cache.get_raw_key(&key.as_u64()).await;
        let retrieved = retrieved.expect("cache should contain the key");
        let retrieved_batches = retrieved.records().await.expect("Failed to decode");
        assert_eq!(
            *retrieved_batches,
            [Arc::new(create_test_record_batch())],
            "the cache must serve exactly the batch that was stored (`id` = [1, 2, 3])"
        );
    }

    #[rstest]
    #[case::siphash(RandomState::default())]
    #[case::ahash(ahash::RandomState::default())]
    #[tokio::test]
    async fn test_cache_invalidate_all<
        H: Hasher + Send + Sync + 'static,
        T: BuildHasher<Hasher = H> + Clone + Send + Sync + 'static,
    >(
        #[case] hasher: T,
    ) {
        let cache: SimpleCache<CachedQueryResult, _, _> =
            SimpleCache::new(10, Duration::from_mins(1), hasher);
        let result = create_test_cached_result().await;

        // Put a value in the cache
        let get_key = || CacheKey::Query("test_query", None).as_raw_key(cache.hasher());
        let key = get_key();
        cache.put_raw_key(&key.as_u64(), result).await;

        // Verify the value is in the cache
        let retrieved = cache.get_raw_key(&key.as_u64()).await;
        retrieved
            .is_some()
            .then_some(())
            .expect("cache should contain the key before invalidation");

        // Invalidate the cache for the table
        cache.invalidate_all().await;

        // Verify the value is no longer in the cache
        let retrieved = cache.get_raw_key(&key.as_u64()).await;
        retrieved
            .is_none()
            .then_some(())
            .expect("cache should not contain key after invalidation");
    }

    #[rstest]
    #[case::siphash(RandomState::default())]
    #[case::ahash(ahash::RandomState::default())]
    #[tokio::test]
    async fn test_cache_ttl<
        H: Hasher + Send + Sync + 'static,
        T: BuildHasher<Hasher = H> + Clone + Send + Sync + 'static,
    >(
        #[case] hasher: T,
    ) {
        let cache: SimpleCache<CachedQueryResult, _, _> =
            SimpleCache::new(10, Duration::from_millis(100), hasher);
        let key = || CacheKey::Query("test_query", None).as_raw_key(cache.hasher());
        let result = create_test_cached_result().await;

        // Put a value in the cache
        cache.put_raw_key(&key().as_u64(), result).await;

        // Verify the value is in the cache
        let retrieved = cache.get_raw_key(&key().as_u64()).await;
        retrieved
            .is_some()
            .then_some(())
            .expect("cache should contain the key before TTL expiry");

        // Wait for the TTL to expire
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Verify the value is no longer in the cache
        let retrieved = cache.get_raw_key(&key().as_u64()).await;
        retrieved
            .is_none()
            .then_some(())
            .expect("cache should not contain key after TTL expiry");
    }

    /// A logical plan scanning `table`, and a handle that reports whether the provider
    /// it was planned against is still alive.
    fn plan_over(table: &str) -> (LogicalPlan, Weak<MemTable>) {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let provider =
            Arc::new(MemTable::try_new(Arc::clone(&schema), vec![vec![]]).expect("empty MemTable"));
        let alive = Arc::downgrade(&provider);
        let plan = LogicalPlanBuilder::scan(table, provider_as_source(provider), None)
            .expect("scan builds")
            .build()
            .expect("plan builds");
        (plan, alive)
    }

    // Regression test for #14251. A cached plan holds the providers it was planned
    // against, so a discard that leaves the plan alive keeps a removed dataset's
    // provider, and the memory it has reserved, alive with it.
    #[tokio::test]
    async fn invalidate_all_releases_the_providers_of_discarded_plans() {
        let cache = SimpleCache::new(10, Duration::from_hours(1), RandomState::default());
        let (plan, alive) = plan_over("removed");
        cache.put_raw_key(&1, plan).await;
        cache.checkpoint().await;
        assert!(
            alive.upgrade().is_some(),
            "the cached plan holds its provider"
        );

        cache.invalidate_all().await;

        assert!(
            alive.upgrade().is_none(),
            "a discarded plan must not keep its provider alive once invalidate_all returns"
        );
        assert!(cache.get_raw_key(&1).await.is_none());
    }

    #[tokio::test]
    async fn invalidate_for_table_discards_only_the_plans_over_that_table() {
        let cache = SimpleCache::new(10, Duration::from_hours(1), RandomState::default());
        let (removed_plan, _) = plan_over("removed");
        let (kept_plan, kept_alive) = plan_over("kept");
        cache.put_raw_key(&1, removed_plan).await;
        cache.put_raw_key(&2, kept_plan).await;
        cache.checkpoint().await;

        cache
            .invalidate_for_table(TableReference::bare("removed"))
            .await
            .expect("invalidation succeeds");

        assert!(cache.get_raw_key(&1).await.is_none());
        assert!(kept_alive.upgrade().is_some());
        let kept = cache
            .get_raw_key(&2)
            .await
            .expect("the other plan stays cached");
        assert_eq!(
            kept.as_table_refs().as_ref(),
            &HashSet::from([TableReference::bare("kept")])
        );
    }

    // A refresh invalidates the refreshed table's plans. The provider they hold must go once
    // moka removes them, or a later removal of the dataset cannot release it (#14251).
    #[tokio::test]
    async fn a_plan_invalidated_for_its_table_releases_its_provider_at_the_next_maintenance() {
        let cache = SimpleCache::new(10, Duration::from_hours(1), RandomState::default());
        let (plan, alive) = plan_over("refreshed");
        cache.put_raw_key(&1, plan).await;
        cache.checkpoint().await;

        cache
            .invalidate_for_table(TableReference::bare("refreshed"))
            .await
            .expect("invalidation succeeds");
        cache.checkpoint().await;

        assert!(
            alive.upgrade().is_none(),
            "a plan moka has removed must not keep its provider alive"
        );
    }

    /// A value whose last drop takes a while, as dropping a provider that owns real state can.
    struct SlowDrop;
    impl Drop for SlowDrop {
        fn drop(&mut self) {
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    // moka bounds each maintenance pass to 100 ms once an eviction listener is installed, so
    // a cache whose values are slow to drop is not emptied by a single pass.
    #[tokio::test]
    async fn invalidate_all_releases_every_value_even_when_dropping_them_is_slow() {
        let cache: SimpleCache<Arc<SlowDrop>, _, _> =
            SimpleCache::new(2_000, Duration::from_hours(1), RandomState::default());
        let mut alive = Vec::new();
        for key in 0..1_000u64 {
            let value = Arc::new(SlowDrop);
            alive.push(Arc::downgrade(&value));
            cache.put_raw_key(&key, value).await;
        }
        cache.checkpoint().await;

        cache.invalidate_all().await;

        let still_alive = alive.iter().filter(|w| w.upgrade().is_some()).count();
        assert_eq!(
            still_alive, 0,
            "values still alive after invalidate_all returned"
        );
    }
}
