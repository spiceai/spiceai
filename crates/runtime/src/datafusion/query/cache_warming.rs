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

//! SQL results-cache warming after the first full/append refresh.
//!
//! Queries that populated the results cache before a dataset's first
//! full or append refresh (for example under `ready_state: on_registration`)
//! are recorded here. When that refresh completes, the accelerator is
//! rewritten and the results cache is invalidated; this module replays
//! the recorded queries on the refresh runtime until the cache is full,
//! then stops. Subsequent refreshes do not re-warm.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use cache::key::RawCacheKey;
use cache::resolved_table_match;
use datafusion::common::ParamValues;
use datafusion::sql::TableReference;
use runtime_request_context::CacheNamespace;

use crate::datafusion::{DataFusion, SPICE_RUNTIME_SCHEMA};

/// Upper bound on recorded warming queries. Each entry holds the SQL text
/// and bound parameters of a cacheable query, so the catalog cannot grow
/// with all-time query history.
const MAX_WARMING_QUERIES: usize = 8192;

/// A cacheable query that can be replayed to re-fill the SQL results cache.
#[derive(Clone)]
pub(crate) struct WarmingQuery {
    pub(super) raw_key: RawCacheKey,
    pub(super) sql: Arc<str>,
    pub(super) parameters: Option<ParamValues>,
    pub(super) namespace: CacheNamespace,
    pub(super) input_tables: Arc<HashSet<TableReference>>,
}

struct WarmingCatalog {
    by_key: HashMap<u64, WarmingQuery>,
    /// Least-recent first. [`Self::record`] moves a key to the back on every
    /// insert so a refresh warms the queries that ran most recently.
    lru: VecDeque<u64>,
}

impl WarmingCatalog {
    fn new() -> Self {
        Self {
            by_key: HashMap::new(),
            lru: VecDeque::new(),
        }
    }

    fn record(&mut self, query: WarmingQuery) {
        let key = query.raw_key.as_u64();
        if self.by_key.insert(key, query).is_some() {
            if let Some(pos) = self.lru.iter().position(|k| *k == key) {
                self.lru.remove(pos);
            }
        }
        self.lru.push_back(key);

        while self.lru.len() > MAX_WARMING_QUERIES {
            if let Some(evicted) = self.lru.pop_front() {
                self.by_key.remove(&evicted);
            }
        }
    }

    /// Matching queries, most recently recorded first.
    fn for_tables(&self, tables: &[TableReference]) -> Vec<WarmingQuery> {
        self.lru
            .iter()
            .rev()
            .filter_map(|key| {
                let query = self.by_key.get(key)?;
                tables
                    .iter()
                    .any(|table| resolved_table_match(query.input_tables.as_ref(), table))
                    .then(|| query.clone())
            })
            .collect()
    }
}

/// Records cacheable queries and serializes warming runs so two datasets
/// finishing their first refresh cannot overshoot `max_size` by filling
/// in parallel.
pub(crate) struct ResultsCacheWarmer {
    catalog: parking_lot::Mutex<WarmingCatalog>,
    run: tokio::sync::Mutex<()>,
}

impl ResultsCacheWarmer {
    pub(crate) fn new() -> Self {
        Self {
            catalog: parking_lot::Mutex::new(WarmingCatalog::new()),
            run: tokio::sync::Mutex::new(()),
        }
    }

    fn record(&self, query: WarmingQuery) {
        self.catalog.lock().record(query);
    }

    fn queries_for_tables(&self, tables: &[TableReference]) -> Vec<WarmingQuery> {
        self.catalog.lock().for_tables(tables)
    }
}

impl DataFusion {
    /// Remember a query that was stored in the SQL results cache, so the first
    /// full/append refresh of a table it reads can replay it.
    pub(crate) fn record_results_cache_warming_query(
        &self,
        raw_key: RawCacheKey,
        sql: Arc<str>,
        parameters: Option<ParamValues>,
        namespace: CacheNamespace,
        input_tables: Arc<HashSet<TableReference>>,
    ) {
        if sql.as_ref() == "<logical plan>" || sql.is_empty() {
            return;
        }
        if input_tables.is_empty() {
            return;
        }
        if input_tables
            .iter()
            .any(|table| matches!(table.schema(), Some(SPICE_RUNTIME_SCHEMA)))
        {
            return;
        }

        self.results_cache_warmer.record(WarmingQuery {
            raw_key,
            sql,
            parameters,
            namespace,
            input_tables,
        });
    }

    /// Callback the refresher invokes after the first successful full/append
    /// refresh. Returns `None` when this `DataFusion` has not been wrapped in
    /// an `Arc` yet (`set_self_ref` has not run), so tests that never start
    /// the refresher are unaffected.
    pub(crate) fn results_cache_warm_callback(
        &self,
    ) -> Option<crate::accelerated::refresh::ResultsCacheWarmCallback> {
        let weak = self.datafusion_ref().get()?.clone();
        Some(Arc::new(move |tables: Vec<TableReference>| {
            let Some(df) = weak.upgrade() else {
                return;
            };
            df.spawn_results_cache_warming(tables);
        }))
    }

    /// Spawn warming on the dedicated refresh runtime so it cannot take query
    /// runtime workers or admission permits. Falls back to the current runtime
    /// when no refresh runtime is configured (tests, single-runtime processes).
    fn spawn_results_cache_warming(self: &Arc<Self>, tables: Vec<TableReference>) {
        let df = Arc::clone(self);
        let task = async move {
            df.warm_results_cache(&tables).await;
        };
        if let Some(runtime) = self.refresh_runtime() {
            runtime.spawn(task);
        } else {
            tokio::spawn(task);
        }
    }

    /// Replay recorded queries that read `tables` until the results cache is
    /// full, then stop. Safe to call more than once: a second call finds the
    /// cache already full (or the same queries already resident) and exits.
    /// The refresher is what guarantees "first refresh only".
    pub(crate) async fn warm_results_cache(self: &Arc<Self>, tables: &[TableReference]) {
        let Some(cache_provider) = self.results_cache_provider() else {
            return;
        };

        let queries = self.results_cache_warmer.queries_for_tables(tables);
        if queries.is_empty() {
            tracing::debug!(
                "SQL results cache warming skipped: no recorded queries read the refreshed dataset(s)"
            );
            return;
        }

        let _run = self.results_cache_warmer.run.lock().await;

        cache_provider.run_pending_tasks().await;
        let size = cache_provider.size().await;
        let max_size = cache_provider.max_size();
        if size >= max_size {
            tracing::debug!(
                "SQL results cache warming skipped: the cache is already full ({:.2} of {:.2})",
                byte_unit::Byte::from_u64(size).get_adjusted_unit(byte_unit::Unit::MiB),
                byte_unit::Byte::from_u64(max_size).get_adjusted_unit(byte_unit::Unit::MiB),
            );
            return;
        }

        let names = tables
            .iter()
            .map(|table| format!("'{table}'"))
            .collect::<Vec<_>>()
            .join(", ");
        tracing::info!(
            "Warming SQL results cache for dataset {names} after its first refresh; this runs on the refresh runtime and stops once the cache is full"
        );

        let mut stored = 0_u64;
        let mut attempted = 0_u64;
        for query in queries {
            cache_provider.run_pending_tasks().await;
            if cache_provider.size().await >= cache_provider.max_size() {
                tracing::info!(
                    "Finished warming SQL results cache for dataset {names}: the cache is full after {stored} queries ({:.2}). Further refreshes will not re-warm",
                    byte_unit::Byte::from_u64(cache_provider.size().await)
                        .get_adjusted_unit(byte_unit::Unit::MiB),
                );
                return;
            }

            attempted += 1;
            if super::Query::warm_one_cached_query(self, &query).await {
                stored += 1;
            }
        }

        cache_provider.run_pending_tasks().await;
        tracing::info!(
            "Finished warming SQL results cache for dataset {names}: stored {stored} of {attempted} queries ({:.2}). Further refreshes will not re-warm",
            byte_unit::Byte::from_u64(cache_provider.size().await)
                .get_adjusted_unit(byte_unit::Unit::MiB),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cache::key::CacheKey;
    use std::hash::Hasher;

    fn key(sql: &str) -> RawCacheKey {
        CacheKey::Query(sql, None).as_raw_key(Box::new(std::hash::DefaultHasher::new()))
    }

    fn query(sql: &str, tables: &[&str]) -> WarmingQuery {
        WarmingQuery {
            raw_key: key(sql),
            sql: Arc::from(sql),
            parameters: None,
            namespace: CacheNamespace::Public,
            input_tables: Arc::new(tables.iter().map(|t| TableReference::bare(*t)).collect()),
        }
    }

    #[test]
    fn catalog_returns_matching_queries_most_recent_first() {
        let mut catalog = WarmingCatalog::new();
        catalog.record(query("SELECT 1 FROM a", &["a"]));
        catalog.record(query("SELECT 2 FROM a", &["a"]));
        catalog.record(query("SELECT 3 FROM b", &["b"]));

        let warmed = catalog.for_tables(&[TableReference::bare("a")]);
        assert_eq!(warmed.len(), 2);
        assert_eq!(warmed[0].sql.as_ref(), "SELECT 2 FROM a");
        assert_eq!(warmed[1].sql.as_ref(), "SELECT 1 FROM a");
    }

    #[test]
    fn catalog_matches_qualified_and_bare_table_names() {
        let mut catalog = WarmingCatalog::new();
        catalog.record(query("SELECT 1 FROM customer", &["customer"]));

        let warmed = catalog.for_tables(&[TableReference::full("spice", "public", "customer")]);
        assert_eq!(warmed.len(), 1);
        assert_eq!(warmed[0].sql.as_ref(), "SELECT 1 FROM customer");
    }

    #[test]
    fn catalog_re_recording_moves_a_query_to_most_recent() {
        let mut catalog = WarmingCatalog::new();
        catalog.record(query("SELECT 1 FROM a", &["a"]));
        catalog.record(query("SELECT 2 FROM a", &["a"]));
        catalog.record(query("SELECT 1 FROM a", &["a"]));

        let warmed = catalog.for_tables(&[TableReference::bare("a")]);
        assert_eq!(warmed.len(), 2, "re-recording must not duplicate the key");
        assert_eq!(warmed[0].sql.as_ref(), "SELECT 1 FROM a");
        assert_eq!(warmed[1].sql.as_ref(), "SELECT 2 FROM a");
    }

    #[test]
    fn catalog_evicts_the_least_recent_query_past_the_bound() {
        let mut catalog = WarmingCatalog::new();
        for i in 0..=MAX_WARMING_QUERIES {
            catalog.record(query(&format!("SELECT {i} FROM t"), &["t"]));
        }

        assert_eq!(catalog.by_key.len(), MAX_WARMING_QUERIES);
        assert_eq!(catalog.lru.len(), MAX_WARMING_QUERIES);

        let warmed = catalog.for_tables(&[TableReference::bare("t")]);
        assert_eq!(warmed.len(), MAX_WARMING_QUERIES);
        assert_eq!(
            warmed[0].sql.as_ref(),
            format!("SELECT {MAX_WARMING_QUERIES} FROM t")
        );
        assert!(
            warmed.iter().all(|q| q.sql.as_ref() != "SELECT 0 FROM t"),
            "the least-recent query must have been evicted"
        );
    }
}

#[cfg(test)]
mod warming_runtime_tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use cache::result::CacheStatus;
    use cache::{Caching, QueryResultsCacheProvider, SimpleCache};
    use datafusion::datasource::{MemTable, TableProvider};
    use futures::TryStreamExt;
    use runtime_request_context::{CacheControl, CacheKeyType, Protocol, RequestContext};
    use spicepod::component::caching::SQLResultsCacheConfig;
    use tokio::runtime::Handle;

    use crate::{
        builder::RuntimeBuilder,
        datafusion::{
            DataFusion,
            query::{QueryBuilder, ResultsCacheMode},
        },
        status,
    };

    fn request_context() -> Arc<RequestContext> {
        Arc::new(
            RequestContext::builder(Protocol::Internal)
                .with_cache_control(CacheControl::Cache(CacheKeyType::Raw))
                .build(),
        )
    }

    async fn prepare_runtime(max_size: Option<&str>) -> Arc<DataFusion> {
        let plans_cache = Arc::new(SimpleCache::new(
            512,
            Duration::from_hours(1),
            std::hash::BuildHasherDefault::<twox_hash::XxHash3_64>::default(),
        ));
        let results_cache_config = SQLResultsCacheConfig {
            item_ttl: Some("10m".to_string()),
            cache_key_type: spicepod::component::caching::CacheKeyType::Sql,
            max_size: max_size.map(ToString::to_string),
            ..Default::default()
        };
        let cache_provider =
            QueryResultsCacheProvider::try_new(&results_cache_config, Box::new([]))
                .expect("valid cache provider");
        let runtime = RuntimeBuilder::new().build().await;
        Arc::new(
            DataFusion::builder(
                status::RuntimeStatus::new(),
                runtime.accelerator_engine_registry(),
                Handle::current(),
            )
            .with_caching(Arc::new(
                Caching::new()
                    .with_results_cache(Arc::new(cache_provider))
                    .with_plans_cache(plans_cache),
            ))
            .build(),
        )
    }

    async fn register_table(df: &Arc<DataFusion>, name: &str, value: i64) {
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![value]))],
        )
        .expect("batch");
        let table = Arc::new(
            MemTable::try_new(schema, vec![vec![batch]]).expect("mem table"),
        );
        df.ctx
            .register_table(
                TableReference::bare(name),
                table as Arc<dyn TableProvider>,
            )
            .expect("register table");
    }

    async fn run_sql(df: &Arc<DataFusion>, sql: &str) -> CacheStatus {
        let result = QueryBuilder::new(sql, Arc::clone(df))
            .results_cache_mode(ResultsCacheMode::Default)
            .build()
            .run()
            .await
            .expect("query should succeed");
        let status = result.cache_status;
        result
            .data
            .try_collect::<Vec<_>>()
            .await
            .expect("collect");
        status
    }

    #[tokio::test]
    async fn warming_replays_recorded_queries_after_invalidation() {
        let df = prepare_runtime(None).await;
        register_table(&df, "orders", 1).await;
        let ctx = request_context();

        let miss = ctx.scope(run_sql(&df, "SELECT n FROM orders")).await;
        assert_eq!(miss, CacheStatus::CacheMiss);

        let hit = ctx.scope(run_sql(&df, "SELECT n FROM orders")).await;
        assert_eq!(hit, CacheStatus::CacheHit);

        df.caching()
            .invalidate_for_table(TableReference::bare("orders"))
            .await
            .expect("invalidate");
        let provider = df.results_cache_provider().expect("results cache");
        provider.run_pending_tasks().await;

        df.warm_results_cache(&[TableReference::bare("orders")])
            .await;

        let hit_after_warm = ctx.scope(run_sql(&df, "SELECT n FROM orders")).await;
        assert_eq!(
            hit_after_warm,
            CacheStatus::CacheHit,
            "warming must put the recorded query back in the results cache"
        );
    }

    #[tokio::test]
    async fn warming_stops_once_the_cache_is_full() {
        // Small enough that a handful of distinct results fill it; large
        // enough that a single tiny SELECT still fits.
        let df = prepare_runtime(Some("2KiB")).await;
        register_table(&df, "orders", 1).await;
        let ctx = request_context();

        for i in 0..32 {
            let sql = format!("SELECT n, {i} FROM orders");
            let _ = ctx.scope(run_sql(&df, &sql)).await;
        }

        let provider = df.results_cache_provider().expect("results cache");
        provider.run_pending_tasks().await;
        assert!(
            provider.item_count().await > 1,
            "the test must have cached more than one query"
        );

        df.caching()
            .invalidate_for_table(TableReference::bare("orders"))
            .await
            .expect("invalidate");
        provider.run_pending_tasks().await;

        df.warm_results_cache(&[TableReference::bare("orders")])
            .await;
        provider.run_pending_tasks().await;

        let after = provider.item_count().await;
        assert!(after > 0, "warming must store at least one query");
        assert!(
            after < 32,
            "warming must stop once the cache is full rather than replaying all 32 recorded queries, stored {after}"
        );
    }
}
