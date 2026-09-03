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

#![expect(
    clippy::expect_used,
    reason = "the value/cache constructors below are shared helpers outside `#[test]` functions, where clippy's `allow-expect-in-tests` cannot see them"
)]

//! Both-orderings invalidation tests for every table-scoped cache.
//!
//! One invariant, stated once: **a cache entry built from a read of table `T`
//! must not be servable once `T` has been invalidated.** Whether the store
//! landed before or after the invalidation is an interleaving, not a licence.
//!
//! Two orderings can produce such an entry, and only one of them is covered by
//! `moka`'s predicate invalidation:
//!
//! | Ordering | Sequence | Covered by `invalidate_entries_if`? |
//! |----------|----------|-------------------------------------|
//! | A — store, then invalidate | read `T` → store → invalidate `T` | yes — the entry exists when the predicate registers |
//! | B — invalidate, then store of a pre-read | read `T` → invalidate `T` → store | **no** — the predicate only matches entries last modified at or before it registered |
//!
//! Ordering B is not exotic: it is what happens whenever a read outlives a
//! concurrent refresh or DML, which is every read longer than the gap between
//! two invalidations of the same table.
//!
//! Every cache closes ordering B the same way: the entry records when its read
//! began, and the serving lookup re-checks that against the per-table
//! invalidation stamp — `QueryResultsCacheProvider` in its own `get_raw_key`,
//! the plan and search caches through
//! [`TabledCacheProvider::get_raw_key_if_fresh`]. These tests hold all three
//! to the invariant through the lookup each one actually serves from, plus the
//! full-clear (`invalidate_all`) straddle the plan cache hits on schema
//! changes, and the counterparts proving none of it over-rejects.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Int32Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use cache::key::RawCacheKey;
use cache::result::query::CachedQueryResult;
use cache::result::search::{CachedAggregationResult, CachedSearchResult};
use cache::{
    CachedLogicalPlan, QueryResultsCacheProvider, SimpleCache, TabledCacheProvider,
    get_hash_builder, lru_cache,
};
use datafusion::logical_expr::LogicalPlan;
use datafusion::logical_expr::builder::{LogicalPlanBuilder, LogicalTableSource};
use datafusion::sql::TableReference;
use spicepod::component::caching::{CacheConfig, SQLResultsCacheConfig};

/// The table every entry in these tests reads, and the one that gets
/// invalidated.
const READ_TABLE: &str = "customer";

/// A table nothing under test reads. Invalidating it must leave every entry
/// alone — without this, a cache that dropped *everything* on any invalidation
/// would pass the ordering tests for the wrong reason.
const UNRELATED_TABLE: &str = "supplier";

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]))
}

fn record_batch() -> RecordBatch {
    RecordBatch::try_new(schema(), vec![Arc::new(Int32Array::from(vec![1, 2, 3]))])
        .expect("should build record batch")
}

// ---------------------------------------------------------------------------
// Cache constructors — each mirrors how `Runtime::init_caching` builds it, so
// these exercise the shipped configuration rather than a convenient one.
// `crates/runtime/src/init/caching.rs`.
// ---------------------------------------------------------------------------

fn results_cache() -> QueryResultsCacheProvider {
    QueryResultsCacheProvider::try_new(&SQLResultsCacheConfig::default(), Box::new([]))
        .expect("should build results cache")
}

fn search_cache() -> Arc<dyn TabledCacheProvider<CachedSearchResult> + Send + Sync> {
    lru_cache::build_from_config::<CachedSearchResult>(&CacheConfig {
        // The shipped default is one second, which would let ordering B pass by
        // expiry rather than by invalidation. A long TTL keeps the test about
        // the invalidation path; the default TTL is what bounds the *impact*,
        // not what decides the correctness.
        item_ttl: Some("1h".to_string()),
        ..CacheConfig::default()
    })
    .expect("should build search cache")
    .as_tabled_provider()
}

/// The plan cache as shipped: 512 entries, one-hour TTL, no `enabled` gate.
fn plan_cache() -> Arc<dyn TabledCacheProvider<CachedLogicalPlan> + Send + Sync> {
    Arc::new(SimpleCache::new(
        512,
        Duration::from_hours(1),
        get_hash_builder(SQLResultsCacheConfig::default().hashing_algorithm)
            .expect("should build hasher"),
    ))
    .as_tabled_provider()
}

// ---------------------------------------------------------------------------
// Value constructors
// ---------------------------------------------------------------------------

fn cached_query_result(read_started_at: Instant) -> CachedQueryResult {
    CachedQueryResult::new_raw(
        vec![record_batch()],
        schema(),
        Arc::new(HashSet::from([TableReference::bare(READ_TABLE)])),
        Instant::now(),
        read_started_at,
    )
}

fn cached_search_result(read_started_at: Instant) -> CachedSearchResult {
    let batch = record_batch();
    let aggregation = CachedAggregationResult::new(
        vec![batch.clone()],
        vec!["id".to_string()],
        Vec::new(),
        HashMap::new(),
        batch.schema(),
    );

    CachedSearchResult {
        results: Arc::new(HashMap::from([(
            TableReference::bare(READ_TABLE),
            aggregation,
        )])),
        input_tables: Arc::new(HashSet::from([TableReference::bare(READ_TABLE)])),
        read_started_at,
    }
}

/// A `LogicalPlan` whose only `TableScan` reads [`READ_TABLE`], so
/// `AsTableRefs` resolves to exactly that table.
fn logical_plan() -> LogicalPlan {
    LogicalPlanBuilder::scan(
        TableReference::bare(READ_TABLE),
        Arc::new(LogicalTableSource::new(schema())),
        None,
    )
    .expect("should build scan")
    .build()
    .expect("should build plan")
}

fn cached_plan(planned_at: Instant) -> CachedLogicalPlan {
    CachedLogicalPlan::new(logical_plan(), planned_at)
}

// ---------------------------------------------------------------------------
// Preconditions — a passing ordering test means nothing if the entry was never
// there, or if invalidation is simply dropping the whole cache.
// ---------------------------------------------------------------------------

/// Every value type must actually name [`READ_TABLE`] as an input, otherwise
/// the invalidation predicate has nothing to match and ordering A would "pass"
/// vacuously.
#[test]
fn every_value_under_test_names_the_table_it_read() {
    use cache::AsTableRefs;

    let expected = HashSet::from([TableReference::bare(READ_TABLE)]);

    assert_eq!(
        cached_query_result(Instant::now())
            .as_table_refs()
            .as_ref()
            .clone(),
        expected,
        "the results-cache value must name the table it read"
    );
    assert_eq!(
        cached_search_result(Instant::now())
            .as_table_refs()
            .as_ref()
            .clone(),
        expected,
        "the search-cache value must name the table it read"
    );
    assert_eq!(
        cached_plan(Instant::now()).as_table_refs().as_ref().clone(),
        expected,
        "the plan-cache value must name the table it scanned"
    );
}

// ---------------------------------------------------------------------------
// Ordering A — store, then invalidate.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn results_cache_ordering_a_store_then_invalidate() {
    let cache = results_cache();
    let key = RawCacheKey::new(1);

    cache
        .put_raw_key(&key, cached_query_result(Instant::now()))
        .await
        .expect("should store");
    assert!(
        cache
            .get_raw_key(&key)
            .await
            .expect("get should succeed")
            .is_some(),
        "hazard not reached: the entry must be servable before the invalidation"
    );

    cache
        .invalidate_for_table(TableReference::bare(READ_TABLE))
        .await
        .expect("should invalidate");

    assert!(
        cache
            .get_raw_key(&key)
            .await
            .expect("get should succeed")
            .is_none(),
        "results cache, ordering A: an entry stored before the invalidation must not be served after it"
    );
}

#[tokio::test]
async fn search_cache_ordering_a_store_then_invalidate() {
    let cache = search_cache();
    let key = 1u64;

    cache
        .put_raw_key(&key, cached_search_result(Instant::now()))
        .await;
    assert!(
        cache.get_raw_key_if_fresh(&key).await.is_some(),
        "hazard not reached: the entry must be servable before the invalidation"
    );

    cache
        .invalidate_for_table(TableReference::bare(READ_TABLE))
        .await
        .expect("should invalidate");
    cache.checkpoint().await;

    assert!(
        cache.get_raw_key_if_fresh(&key).await.is_none(),
        "search cache, ordering A: an entry stored before the invalidation must not be served after it"
    );
}

#[tokio::test]
async fn plan_cache_ordering_a_store_then_invalidate() {
    let cache = plan_cache();
    let key = 1u64;

    cache.put_raw_key(&key, cached_plan(Instant::now())).await;
    assert!(
        cache.get_raw_key_if_fresh(&key).await.is_some(),
        "hazard not reached: the entry must be servable before the invalidation"
    );

    cache
        .invalidate_for_table(TableReference::bare(READ_TABLE))
        .await
        .expect("should invalidate");
    cache.checkpoint().await;

    assert!(
        cache.get_raw_key_if_fresh(&key).await.is_none(),
        "plan cache, ordering A: an entry stored before the invalidation must not be served after it"
    );
}

// ---------------------------------------------------------------------------
// Ordering B — invalidate, then store a value whose read began earlier.
//
// The read start is captured *before* the invalidation in every case, so the
// stored value provably describes pre-invalidation state. The predicate
// registered by the invalidation cannot see the later store; only the
// read-time freshness check can reject it.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn results_cache_ordering_b_invalidate_then_store_of_pre_read() {
    let cache = results_cache();
    let key = RawCacheKey::new(1);

    let read_started_at = Instant::now();
    cache
        .invalidate_for_table(TableReference::bare(READ_TABLE))
        .await
        .expect("should invalidate");
    cache
        .put_raw_key(&key, cached_query_result(read_started_at))
        .await
        .expect("should store");

    assert!(
        cache
            .get_raw_key(&key)
            .await
            .expect("get should succeed")
            .is_none(),
        "results cache, ordering B: a result read before the invalidation must not be served after it"
    );
}

#[tokio::test]
async fn search_cache_ordering_b_invalidate_then_store_of_pre_read() {
    let cache = search_cache();
    let key = 1u64;

    let read_started_at = Instant::now();
    cache
        .invalidate_for_table(TableReference::bare(READ_TABLE))
        .await
        .expect("should invalidate");
    cache.checkpoint().await;
    cache
        .put_raw_key(&key, cached_search_result(read_started_at))
        .await;

    assert!(
        cache.get_raw_key_if_fresh(&key).await.is_none(),
        "search cache, ordering B: a search result read before the invalidation must not be served after it"
    );
}

#[tokio::test]
async fn plan_cache_ordering_b_invalidate_then_store_of_pre_read() {
    let cache = plan_cache();
    let key = 1u64;

    let planned_at = Instant::now();
    cache
        .invalidate_for_table(TableReference::bare(READ_TABLE))
        .await
        .expect("should invalidate");
    cache.checkpoint().await;
    cache.put_raw_key(&key, cached_plan(planned_at)).await;

    assert!(
        cache.get_raw_key_if_fresh(&key).await.is_none(),
        "plan cache, ordering B: a plan built before the invalidation must not be served after it"
    );
}

// ---------------------------------------------------------------------------
// Full-clear straddle — `invalidate_all`, then store a value whose read began
// earlier. This is the plan cache's schema-change path (`clear_cached_plans`):
// the clear names no table, so only the clock's global floor can reject the
// racing store.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn plan_cache_full_clear_rejects_store_of_pre_read() {
    let cache = plan_cache();
    let key = 1u64;

    let planned_at = Instant::now();
    cache.invalidate_all().await;
    cache.put_raw_key(&key, cached_plan(planned_at)).await;

    assert!(
        cache.get_raw_key_if_fresh(&key).await.is_none(),
        "plan cache: a plan built before a full clear must not be served after it"
    );
}

#[tokio::test]
async fn search_cache_full_clear_rejects_store_of_pre_read() {
    let cache = search_cache();
    let key = 1u64;

    let read_started_at = Instant::now();
    cache.invalidate_all().await;
    cache
        .put_raw_key(&key, cached_search_result(read_started_at))
        .await;

    assert!(
        cache.get_raw_key_if_fresh(&key).await.is_none(),
        "search cache: a result read before a full clear must not be served after it"
    );
}

// ---------------------------------------------------------------------------
// Self-healing — a value whose read began *after* the invalidation is fresh
// and must be served. Without these, rejecting every store would satisfy the
// ordering tests while permanently disabling the cache for any table that was
// ever invalidated.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn plan_cache_serves_a_plan_built_after_the_invalidation() {
    let cache = plan_cache();
    let key = 1u64;

    cache
        .invalidate_for_table(TableReference::bare(READ_TABLE))
        .await
        .expect("should invalidate");
    cache.invalidate_all().await;

    // Planning begins after both invalidations, so the plan reflects
    // post-invalidation state.
    let planned_at = Instant::now();
    cache.put_raw_key(&key, cached_plan(planned_at)).await;

    assert!(
        cache.get_raw_key_if_fresh(&key).await.is_some(),
        "plan cache: a plan built after the invalidation is fresh and must be served"
    );
}

#[tokio::test]
async fn search_cache_serves_a_result_read_after_the_invalidation() {
    let cache = search_cache();
    let key = 1u64;

    cache
        .invalidate_for_table(TableReference::bare(READ_TABLE))
        .await
        .expect("should invalidate");
    cache.invalidate_all().await;

    let read_started_at = Instant::now();
    cache
        .put_raw_key(&key, cached_search_result(read_started_at))
        .await;

    assert!(
        cache.get_raw_key_if_fresh(&key).await.is_some(),
        "search cache: a result read after the invalidation is fresh and must be served"
    );
}

// ---------------------------------------------------------------------------
// Counterpart tests — the caches must still cache.
//
// Without these, deleting every entry on every invalidation would satisfy the
// ordering tests above while destroying the feature.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn results_cache_keeps_entries_an_unrelated_invalidation_does_not_touch() {
    let cache = results_cache();
    let key = RawCacheKey::new(1);

    cache
        .put_raw_key(&key, cached_query_result(Instant::now()))
        .await
        .expect("should store");
    cache
        .invalidate_for_table(TableReference::bare(UNRELATED_TABLE))
        .await
        .expect("should invalidate");

    assert!(
        cache
            .get_raw_key(&key)
            .await
            .expect("get should succeed")
            .is_some(),
        "results cache: invalidating an unread table must not drop the entry"
    );
}

#[tokio::test]
async fn search_cache_keeps_entries_an_unrelated_invalidation_does_not_touch() {
    let cache = search_cache();
    let key = 1u64;

    cache
        .put_raw_key(&key, cached_search_result(Instant::now()))
        .await;
    cache
        .invalidate_for_table(TableReference::bare(UNRELATED_TABLE))
        .await
        .expect("should invalidate");
    cache.checkpoint().await;

    assert!(
        cache.get_raw_key_if_fresh(&key).await.is_some(),
        "search cache: invalidating an unread table must not drop the entry"
    );
}

#[tokio::test]
async fn plan_cache_keeps_entries_an_unrelated_invalidation_does_not_touch() {
    let cache = plan_cache();
    let key = 1u64;

    cache.put_raw_key(&key, cached_plan(Instant::now())).await;
    cache
        .invalidate_for_table(TableReference::bare(UNRELATED_TABLE))
        .await
        .expect("should invalidate");
    cache.checkpoint().await;

    assert!(
        cache.get_raw_key_if_fresh(&key).await.is_some(),
        "plan cache: invalidating an unread table must not drop the entry"
    );
}
