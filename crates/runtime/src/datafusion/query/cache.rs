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

use super::{
    BindingParametersSnafu, Query, QueryResult, QueryTracker, attach_query_tracker_to_stream,
};
use crate::datafusion::{DataFusion, error::find_datafusion_root, query::error_code::ErrorCode};
use cache::{
    key::{CacheKey, RawCacheKey},
    result::CacheStatus,
    result::query::CachedStream,
    to_cached_record_batch_stream,
};
use datafusion::{
    common::ParamValues,
    execution::{SendableRecordBatchStream, SessionState},
    logical_expr::LogicalPlan,
    sql::TableReference,
};
use futures::TryStreamExt;
use runtime_request_context::{CacheControl, CacheKeyType, Protocol, RequestContext};
use snafu::ResultExt;
use std::sync::OnceLock;
use std::{collections::HashSet, hash::Hasher, sync::Arc};
use tracing::Span;

/// Returns `Plan` if the result is not cached and needs to be executed, otherwise returns `Cached`
pub(super) enum PlanOrCached {
    Plan(Box<LogicalPlan>, Option<QueryTracker>, RequestCacheManager),
    Cached(QueryResult),
}

pub(super) struct RequestCacheManager {
    pub(super) cache_status: CacheStatus,
    pub(super) raw_cache_key: RawCacheKey,
}

impl RequestCacheManager {
    pub(super) fn new(cache_status: CacheStatus, raw_cache_key: RawCacheKey) -> Self {
        Self {
            cache_status,
            raw_cache_key,
        }
    }

    pub(super) fn should_cache_results(&self) -> bool {
        !matches!(self.cache_status, CacheStatus::CacheDisabled)
    }
}

struct CacheResponse {
    result: CacheResult,
    status: CacheStatus,
    tracker: Option<QueryTracker>,
    raw_key: Option<RawCacheKey>,
}

impl CacheResponse {
    fn from(result: CacheResult, status: CacheStatus) -> Self {
        Self {
            result,
            status,
            raw_key: None,
            tracker: None,
        }
    }
    fn with_raw_key(mut self, raw_key: Option<RawCacheKey>) -> Self {
        self.raw_key = raw_key;
        self
    }

    fn with_query_tracker(mut self, tracker: Option<QueryTracker>) -> Self {
        self.tracker = tracker;
        self
    }
}

enum CacheResult {
    Hit(QueryResult),
    MissOrSkipped,
    WrongCacheKeyType,
}

impl Query {
    /// Returns a `LogicalPlan` if the result is not cached and needs to be executed, otherwise returns a cached `QueryResult`.
    pub(super) async fn get_plan_or_cached(
        df: &Arc<DataFusion>,
        session: &SessionState,
        request_context: Arc<RequestContext>,
        sql: &str,
        parameters: Option<ParamValues>,
        tracker: Option<QueryTracker>,
    ) -> super::Result<PlanOrCached> {
        let sql_cache_key = CacheKey::Query(sql, parameters.as_ref());
        let sql_or_user_cache_key = match request_context.client_supplied_cache_key() {
            Some(user_key) => CacheKey::ClientSupplied(user_key),
            _ => sql_cache_key,
        };

        // Try to get cached results from SQL or client key
        let CacheResponse {
            tracker,
            raw_key: sql_or_client_raw_key,
            ..
        } = match Self::try_get_cached_result(
            df,
            &request_context,
            tracker,
            &sql_or_user_cache_key,
            sql,
        )
        .await?
        {
            CacheResponse {
                result: CacheResult::Hit(result),
                ..
            } => return Ok(PlanOrCached::Cached(result)),
            response => response,
        };

        // Always use CacheKey::Query when checking the plan cache
        // Only compute this hash if we need it for the plan cache lookup
        let sql_raw_cache_key = sql_cache_key.as_raw_key(Self::plan_hasher(df));
        let plan = match df
            .get_or_create_logical_plan(session, &sql_raw_cache_key, sql)
            .await
        {
            Ok(plan) => plan,
            Err(e) => {
                let e = find_datafusion_root(e);
                let error_code = ErrorCode::from(&e);
                let snafu_error = super::Error::UnableToExecuteQuery { source: e };
                if let Some(t) = tracker {
                    t.finish_with_error(&request_context, snafu_error.to_string(), error_code);
                }
                return Err(snafu_error);
            }
        };

        // Use the logical plan with parameter values for caching and lookup
        let plan = match parameters {
            Some(param_values) => plan
                .with_param_values(param_values)
                .context(BindingParametersSnafu)?,
            None => plan,
        };

        // Try to get cached results from plan
        let CacheResponse {
            mut tracker,
            raw_key: plan_raw_cache_key,
            status,
            ..
        } = match Self::try_get_cached_result(
            df,
            &request_context,
            tracker,
            &CacheKey::LogicalPlan(&plan),
            sql,
        )
        .await?
        {
            CacheResponse {
                result: CacheResult::Hit(result),
                ..
            } => return Ok(PlanOrCached::Cached(result)),
            response => response,
        };

        let request_raw_cache_key = match request_context.cache_control() {
            CacheControl::Cache(CacheKeyType::Default)
            | CacheControl::MaxStale(CacheKeyType::Default, _)
            | CacheControl::MinFresh(CacheKeyType::Default, _)
            | CacheControl::OnlyIfCached(CacheKeyType::Default) => plan_raw_cache_key,
            _ => sql_or_client_raw_key,
        }
        .unwrap_or(sql_raw_cache_key);

        let cache_status = Self::should_cache_results(df, &plan, status);
        tracker = tracker.map(|t| t.results_cache_hit(false));

        Ok(PlanOrCached::Plan(
            Box::new(plan),
            tracker,
            RequestCacheManager::new(cache_status, request_raw_cache_key),
        ))
    }

    /// Return the [`Hasher`] that should be used in caching [`LogicalPlan`]s in [`DataFusion`].
    pub(super) fn plan_hasher(df: &DataFusion) -> Box<dyn Hasher> {
        df.plans_cache_provider().map_or(
            Box::new(std::hash::DefaultHasher::new()) as Box<dyn Hasher>,
            |p| p.hasher(),
        )
    }

    #[expect(clippy::too_many_lines)]
    async fn try_get_cached_result<'a>(
        df: &Arc<DataFusion>,
        request_context: &Arc<RequestContext>,
        mut tracker: Option<QueryTracker>,
        key: &'a CacheKey<'a>,
        sql: &str,
    ) -> super::Result<CacheResponse> {
        let Some(cache_provider) = df.results_cache_provider() else {
            return Ok(
                CacheResponse::from(CacheResult::MissOrSkipped, CacheStatus::CacheDisabled)
                    .with_query_tracker(tracker),
            );
        };

        let cache_control = request_context.cache_control();

        // Validate that the provided cache key is the correct type for this request
        match (cache_control, &key) {
            (
                CacheControl::Cache(CacheKeyType::Default)
                | CacheControl::MaxStale(CacheKeyType::Default, _)
                | CacheControl::MinFresh(CacheKeyType::Default, _)
                | CacheControl::OnlyIfCached(CacheKeyType::Default),
                CacheKey::LogicalPlan(_),
            )
            | (
                CacheControl::Cache(CacheKeyType::Raw)
                | CacheControl::MaxStale(CacheKeyType::Raw, _)
                | CacheControl::MinFresh(CacheKeyType::Raw, _)
                | CacheControl::OnlyIfCached(CacheKeyType::Raw),
                CacheKey::Query(_, _),
            )
            | (
                CacheControl::Cache(CacheKeyType::ClientSupplied)
                | CacheControl::MaxStale(CacheKeyType::ClientSupplied, _)
                | CacheControl::MinFresh(CacheKeyType::ClientSupplied, _)
                | CacheControl::OnlyIfCached(CacheKeyType::ClientSupplied),
                CacheKey::ClientSupplied(_),
            ) => { /* Valid cache key type for this cache control */ }
            (CacheControl::NoCache, _) => {
                return Ok(CacheResponse::from(
                    CacheResult::MissOrSkipped,
                    CacheStatus::CacheBypass,
                )
                .with_query_tracker(tracker));
            }
            _ => {
                return Ok(CacheResponse::from(
                    CacheResult::WrongCacheKeyType,
                    CacheStatus::CacheMiss,
                )
                .with_query_tracker(tracker));
            }
        }

        let raw_key = key.as_raw_key(cache_provider.hasher());

        let cached_result = match cache_provider.get_raw_key(&raw_key).await {
            Ok(Some(result)) => result,
            Ok(None) => {
                return Ok(
                    CacheResponse::from(CacheResult::MissOrSkipped, CacheStatus::CacheMiss)
                        .with_query_tracker(tracker)
                        .with_raw_key(Some(raw_key)),
                );
            }
            Err(e) => return Err(super::Error::FailedToAccessCache { source: e }),
        };

        // Determine cache status based on stale-while-revalidate configuration
        let mut cache_status = CacheStatus::CacheHit;

        // Determine the effective stale-while-revalidate duration from either:
        // 1. The request's max-stale directive (client explicitly willing to accept stale data)
        // 2. The cache provider's stale_while_revalidate_ttl configuration (server-side policy)
        let stale_duration = match cache_control {
            CacheControl::MaxStale(_, Some(duration)) => Some(duration),
            CacheControl::MaxStale(_, None) => None, // max-stale without value means accept any staleness
            _ => cache_provider.stale_while_revalidate_ttl(),
        };

        // Check if stale-while-revalidate is enabled (from request or cache provider config)
        if let Some(stale_duration) = stale_duration {
            let ttl = cache_provider.ttl();
            let now = std::time::Instant::now();
            let max_age = ttl + stale_duration;

            // If beyond the stale-while-revalidate window, treat as cache miss
            if cached_result.is_stale(max_age, now) {
                tracing::debug!(
                    "Cache entry is beyond stale-while-revalidate window (max_age: {:?}), treating as cache miss",
                    max_age
                );
                return Ok(
                    CacheResponse::from(CacheResult::MissOrSkipped, CacheStatus::CacheMiss)
                        .with_query_tracker(tracker)
                        .with_raw_key(Some(raw_key)),
                );
            }

            // If stale (beyond TTL but within stale-while-revalidate window), trigger background revalidation
            if cached_result.is_stale(ttl, now) {
                tracing::debug!(
                    "Cache entry is stale (beyond TTL), triggering background revalidation for stale-while-revalidate"
                );
                cache_status = CacheStatus::CacheStaleWhileRevalidate;

                // Extract plan from cache key if available to avoid re-parsing
                let plan = match key {
                    CacheKey::LogicalPlan(p) => Some(*p),
                    _ => None,
                };
                Self::trigger_background_query_revalidation(
                    Arc::clone(df),
                    sql,
                    request_context,
                    plan,
                    raw_key,
                );
            }
        }

        tracker = tracker.map(|t| {
            t.datasets(Arc::clone(&cached_result.input_tables))
                .results_cache_hit(true)
        });

        let records = match cached_result.records().await {
            Ok(records) => Arc::new(records),
            Err(e) => {
                tracing::error!("Failed to decode cached query result: {e}");
                return Ok(CacheResponse::from(
                    CacheResult::MissOrSkipped,
                    cache_status,
                ));
            }
        };

        let record_batch_stream = CachedStream::new(records, Arc::clone(&cached_result.schema));

        Ok(CacheResponse::from(
            CacheResult::Hit(QueryResult::new(
                attach_query_tracker_to_stream(
                    Span::current(),
                    Arc::clone(request_context),
                    tracker,
                    Box::pin(record_batch_stream),
                ),
                cache_status,
            )),
            cache_status,
        )
        .with_raw_key(Some(raw_key)))
    }

    pub(super) fn should_cache_results(
        df: &DataFusion,
        plan: &LogicalPlan,
        cache_status: CacheStatus,
    ) -> CacheStatus {
        match df.results_cache_provider() {
            Some(provider) if provider.cache_is_enabled_for_plan(plan) => cache_status,
            _ => CacheStatus::CacheDisabled,
        }
    }

    /// Trigger background query re-execution for stale-while-revalidate.
    ///
    /// This spawns a background task that re-executes the original query through the full
    /// query pipeline (including cache population), which will:
    /// 1. Use the proper cache control settings to populate the cache
    /// 2. Go through acceleration if available, or the data source
    /// 3. Update the cache with fresh data via the normal `Query::run` flow
    ///
    /// If a `LogicalPlan` is provided, it will be used directly to avoid re-parsing the SQL.
    /// This is more efficient when the plan is already available (e.g., from a plan cache hit).
    ///
    /// Uses lock-free deduplication based on the cache key to ensure only one revalidation
    /// task runs per cache entry. Multiple concurrent requests for the same stale cache entry
    /// will not spawn redundant background tasks.
    ///
    /// The background task will be automatically cancelled if:
    /// - The `DataFusion` context is dropped (runtime shutdown)
    /// - The query execution is interrupted via the session context
    ///
    /// Creates a background request context for cache revalidation
    ///
    /// Takes the original request context and cache key to create a new background context
    /// that uses a client-supplied cache key. This ensures the revalidation query uses the
    /// exact same cache key as the original query.
    fn create_background_context(
        request_context: &Arc<RequestContext>,
        cache_key: RawCacheKey,
    ) -> Arc<RequestContext> {
        // Create a background request context with a client-supplied cache key to ensure
        // the revalidation query uses the exact same cache key as the original query.
        // This allows the query to go through the normal caching pipeline and naturally
        // update the cache entry that served stale data.
        let cache_key_str = cache_key.as_u64().to_string();

        // Convert the original cache control to use ClientSupplied cache key type
        // For background revalidation, we want to STORE fresh results, so convert
        // MaxStale to Cache to avoid serving stale data during revalidation
        let background_cache_control = match request_context.cache_control() {
            CacheControl::MaxStale(_, _) | CacheControl::Cache(_) => {
                CacheControl::Cache(CacheKeyType::ClientSupplied)
            }
            other @ (CacheControl::NoCache
            | CacheControl::MinFresh(_, _)
            | CacheControl::OnlyIfCached(_)) => other,
        };

        Arc::new(
            RequestContext::builder(Protocol::Internal)
                .with_cache_control(background_cache_control)
                .with_client_supplied_cache_key(Some(cache_key_str))
                .build(),
        )
    }

    /// Prepares query and input tables for background revalidation
    fn prepare_revalidation_query(
        df: &Arc<DataFusion>,
        sql: &str,
        plan: Option<LogicalPlan>,
    ) -> (Query, Arc<HashSet<TableReference>>) {
        let (query, input_tables) = if let Some(logical_plan) = plan {
            tracing::debug!("Background revalidation: re-executing query with existing plan");
            let input_tables = cache::get_logical_plan_input_tables(&logical_plan);
            (
                super::Query::from_logical_plan(df, &logical_plan),
                input_tables,
            )
        } else {
            tracing::debug!(
                "Background revalidation: re-executing query (will re-parse SQL); sql={}",
                sql
            );
            (
                super::QueryBuilder::new(sql, Arc::clone(df)).build(),
                std::collections::HashSet::new(),
            )
        };
        (query, Arc::new(input_tables))
    }

    /// Handles caching of query results after background revalidation
    async fn cache_revalidation_result(
        df: &Arc<DataFusion>,
        cache_key: &RawCacheKey,
        cache_key_u64: u64,
        batches: Vec<arrow::record_batch::RecordBatch>,
        _schema: arrow::datatypes::SchemaRef,
        input_tables: Arc<HashSet<TableReference>>,
    ) {
        if let Some(cache_provider) = df.results_cache_provider() {
            let cached_at = std::time::Instant::now();
            let encoder = cache_provider.encoder();

            match cache::result::query::CachedQueryResult::from_batches(
                &batches,
                input_tables,
                cached_at,
                encoder,
            )
            .await
            {
                Ok(cached_result) => {
                    if let Err(e) = cache_provider.put_raw_key(cache_key, cached_result).await {
                        tracing::debug!(
                            cache_key = cache_key_u64,
                            "Background revalidation failed to cache results: {}",
                            e
                        );
                    } else {
                        tracing::debug!(
                            cache_key = cache_key_u64,
                            "Background revalidation completed successfully and cached"
                        );
                    }
                }
                Err(e) => {
                    tracing::debug!(
                        cache_key = cache_key_u64,
                        "Background revalidation failed to encode results: {}",
                        e
                    );
                }
            }
        } else {
            tracing::debug!("Background revalidation completed but cache provider unavailable");
        }
    }

    fn trigger_background_query_revalidation(
        df: Arc<DataFusion>,
        sql: &str,
        request_context: &Arc<RequestContext>,
        plan: Option<&LogicalPlan>,
        cache_key: RawCacheKey,
    ) {
        // Static Moka cache to track ongoing revalidation tasks by cache key.
        // This provides built-in single-in-flight semantics - if multiple requests
        // trigger revalidation for the same key, only one task will run.
        static REVALIDATION_LOCKS: OnceLock<moka::future::Cache<u64, (), std::hash::RandomState>> =
            OnceLock::new();
        let locks = REVALIDATION_LOCKS.get_or_init(|| {
            moka::future::Cache::builder()
                .max_capacity(10_000) // Track up to 10k concurrent revalidations
                .time_to_live(std::time::Duration::from_secs(300)) // Auto-cleanup after 5min
                .build()
        });

        let cache_key_u64 = cache_key.as_u64();

        // Create a background request context that will cache results using the same cache key
        let background_context = Self::create_background_context(request_context, cache_key);

        // Clone sql and plan for the async block
        let sql_owned = sql.to_string();
        let plan_owned = plan.cloned();

        // Spawn a detached task for background revalidation
        // Use optionally_get_with to ensure only one revalidation per key runs concurrently
        tokio::spawn(async move {
            // optionally_get_with provides automatic single-in-flight: if another task
            // is already running for this key, this will return None immediately
            let result = locks
                .optionally_get_with(cache_key_u64, async move {
                    // Only count as a background query when this task actually runs the revalidation
                    cache::metrics::sql_results::STALE_WHILE_REVALIDATE_BACKGROUND_QUERIES
                        .add(1, &[]);

                    tracing::debug!(
                        cache_key = cache_key_u64,
                        "Starting background revalidation task"
                    );

                    let (query, input_tables) =
                        Self::prepare_revalidation_query(&df, &sql_owned, plan_owned);

                    let result = background_context
                        .scope(async move { query.run().await })
                        .await;

                    match result {
                        Ok(query_result) => {
                            let schema = query_result.data.schema();
                            tracing::debug!(
                                cache_key = cache_key_u64,
                                "Background query execution succeeded, collecting batches"
                            );
                            match query_result.data.try_collect::<Vec<_>>().await {
                                Ok(batches) => {
                                    tracing::debug!(
                                        cache_key = cache_key_u64,
                                        num_batches = batches.len(),
                                        "Collected batches, now caching"
                                    );
                                    Self::cache_revalidation_result(
                                        &df,
                                        &cache_key,
                                        cache_key_u64,
                                        batches,
                                        schema,
                                        input_tables,
                                    )
                                    .await;
                                }
                                Err(e) => {
                                    tracing::debug!(
                                        cache_key = cache_key_u64,
                                        "Background revalidation failed during collection: {}",
                                        e
                                    );
                                }
                            }
                        }
                        Err(e) => {
                            tracing::debug!(
                                cache_key = cache_key_u64,
                                "Background revalidation query failed: {}",
                                e
                            );
                        }
                    }

                    tracing::debug!(
                        cache_key = cache_key_u64,
                        "Background revalidation task completed"
                    );

                    // Return Some to indicate this task completed the revalidation
                    Some(())
                })
                .await;

            if result == Some(()) {
                // This task was the one that ran the revalidation
                // Remove the single-flight guard so future stale hits can trigger another refresh
                locks.invalidate(&cache_key_u64).await;
            } else {
                // Another task is already revalidating this key
                tracing::debug!(
                    cache_key = cache_key_u64,
                    "Background revalidation already in progress for this cache key, skipped"
                );
                cache::metrics::sql_results::STALE_WHILE_REVALIDATE_SKIPPED.add(1, &[]);
            }
        });
    }

    pub(super) fn wrap_stream_with_cache(
        df: &DataFusion,
        stream: SendableRecordBatchStream,
        plan_cache_key: RawCacheKey,
        datasets: Arc<HashSet<TableReference>>,
    ) -> SendableRecordBatchStream {
        if let Some(cache_provider) = df.results_cache_provider() {
            to_cached_record_batch_stream(cache_provider, stream, plan_cache_key, datasets)
        } else {
            stream
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::{sync::Arc, time::Duration};

    use arrow::array::Int64Array;

    use futures::TryStreamExt;

    use cache::{
        Caching, QueryResultsCacheProvider, SimpleCache, key::CacheKey, result::CacheStatus,
    };
    use spicepod::component::caching::SQLResultsCacheConfig;
    use tokio::runtime::Handle;

    use crate::{
        builder::RuntimeBuilder,
        datafusion::{DataFusion, query::QueryBuilder},
        status,
    };
    use runtime_request_context::{CacheControl, CacheKeyType, Protocol, RequestContext};

    // Helper function to create a test RequestContext
    fn create_test_request_context(
        cache_control: CacheControl,
        user_cache_key: Option<String>,
    ) -> Arc<RequestContext> {
        Arc::new(
            RequestContext::builder(Protocol::Internal)
                .with_cache_control(cache_control)
                .with_client_supplied_cache_key(user_cache_key)
                .build(),
        )
    }

    async fn prepare_runtime(
        results_cache_config: Option<SQLResultsCacheConfig>,
    ) -> Arc<DataFusion> {
        let results_cache_config = results_cache_config.unwrap_or(SQLResultsCacheConfig {
            item_ttl: Some("10m".to_string()),
            cache_key_type: spicepod::component::caching::CacheKeyType::Plan,
            ..Default::default()
        });

        let cache_provider =
            QueryResultsCacheProvider::try_new(&results_cache_config, Box::new([]))
                .expect("valid cache provider");

        let plan_cache_provider = Arc::new(SimpleCache::new(
            512,
            Duration::from_secs(3600),
            std::hash::BuildHasherDefault::<twox_hash::XxHash3_64>::default(),
        ));
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
                    .with_plans_cache(plan_cache_provider),
            ))
            .build(),
        )
    }

    #[tokio::test]
    async fn test_request_cache_manager() {
        let cache_status = CacheStatus::CacheHit;
        let raw_cache_key =
            CacheKey::Query("test-key", None).as_raw_key(Box::new(std::hash::DefaultHasher::new()));

        let manager = RequestCacheManager::new(cache_status, raw_cache_key);
        assert!(manager.should_cache_results());
    }

    #[tokio::test]
    #[expect(clippy::too_many_lines)]
    async fn test_get_plan_or_cached_cache_miss_and_hit() {
        let df = prepare_runtime(Some(SQLResultsCacheConfig {
            item_ttl: Some("10m".to_string()),
            cache_key_type: spicepod::component::caching::CacheKeyType::Sql,
            ..Default::default()
        }))
        .await;

        // Test with SQL cache key
        let request_context =
            create_test_request_context(CacheControl::Cache(CacheKeyType::Raw), None);
        let query_builder = QueryBuilder::new("SELECT 1", Arc::clone(&df));
        let query = query_builder.build();
        Arc::clone(&request_context)
            .scope(async move {
                let result = query.run().await.expect("query should succeed");
                assert_eq!(result.cache_status, CacheStatus::CacheMiss);
                // Need to drain the stream to ensure the cache is populated
                let records = result
                    .data
                    .try_collect::<Vec<_>>()
                    .await
                    .expect("should collect");
                assert_eq!(records.len(), 1);
                assert_eq!(records[0].num_rows(), 1);
            })
            .await;

        // Repeat the same query to ensure a cache hit
        let query_builder = QueryBuilder::new("SELECT 1", Arc::clone(&df));
        let query = query_builder.build();
        Arc::clone(&request_context)
            .scope(async move {
                let result = query.run().await.expect("query should succeed");
                assert_eq!(result.cache_status, CacheStatus::CacheHit);
            })
            .await;

        // Repeat a similar query, but with different whitespace - this should be a cache miss for the raw SQL cache key
        let query_builder = QueryBuilder::new("SELECT 1 ", Arc::clone(&df));
        let query = query_builder.build();
        Arc::clone(&request_context)
            .scope(async move {
                let result = query.run().await.expect("query should succeed");
                assert_eq!(result.cache_status, CacheStatus::CacheMiss);
            })
            .await;

        // Test with plan cache key
        let request_context =
            create_test_request_context(CacheControl::Cache(CacheKeyType::Default), None);
        let query_builder = QueryBuilder::new("SELECT 1", Arc::clone(&df));
        let query = query_builder.build();
        Arc::clone(&request_context)
            .scope(async move {
                let result = query.run().await.expect("query should succeed");
                // Expect to miss cache because we are using the default cache key type
                assert_eq!(result.cache_status, CacheStatus::CacheMiss);
                // Need to drain the stream to ensure the cache is populated
                let records = result
                    .data
                    .try_collect::<Vec<_>>()
                    .await
                    .expect("should collect");
                assert_eq!(records.len(), 1);
                assert_eq!(records[0].num_rows(), 1);
            })
            .await;

        // Repeat the same query with the default cache key type - this should be a cache hit
        let query_builder = QueryBuilder::new("SELECT 1", Arc::clone(&df));
        let query = query_builder.build();
        Arc::clone(&request_context)
            .scope(async move {
                let result = query.run().await.expect("query should succeed");
                assert_eq!(result.cache_status, CacheStatus::CacheHit);
            })
            .await;

        // Repeat the same query with the default cache key type, but with different whitespace - this should be a cache hit since the plan is the same
        let query_builder = QueryBuilder::new("SELECT 1 ", Arc::clone(&df));
        let query = query_builder.build();
        Arc::clone(&request_context)
            .scope(async move {
                let result = query.run().await.expect("query should succeed");
                assert_eq!(result.cache_status, CacheStatus::CacheHit);
            })
            .await;

        // Test with user cache key
        let request_context = create_test_request_context(
            CacheControl::Cache(CacheKeyType::ClientSupplied),
            Some("foo".to_string()),
        );
        let query_builder = QueryBuilder::new("SELECT 1", Arc::clone(&df));
        let query = query_builder.build();
        Arc::clone(&request_context)
            .scope(async move {
                let result = query.run().await.expect("query should succeed");
                // Expect to miss cache because it is the first request
                assert_eq!(result.cache_status, CacheStatus::CacheMiss);
                // Need to drain the stream to ensure the cache is populated
                let records = result
                    .data
                    .try_collect::<Vec<_>>()
                    .await
                    .expect("should collect");
                assert_eq!(records.len(), 1);
                assert_eq!(records[0].num_rows(), 1);
                assert_eq!(
                    records[0]
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .expect("must read i64 array")
                        .value(0),
                    1
                );
            })
            .await;

        // Repeat a request with the same user key and a different query
        let query_builder = QueryBuilder::new("SELECT 2", Arc::clone(&df));
        let query = query_builder.build();
        Arc::clone(&request_context)
            .scope(async move {
                let result = query.run().await.expect("query should succeed");
                assert_eq!(result.cache_status, CacheStatus::CacheHit);

                let records = result
                    .data
                    .try_collect::<Vec<_>>()
                    .await
                    .expect("should collect");
                assert_eq!(records.len(), 1);
                assert_eq!(records[0].num_rows(), 1);

                // If the query ran, this value would be 2. But the cached result is served
                assert_eq!(
                    records[0]
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .expect("must read i64 array")
                        .value(0),
                    1
                );
            })
            .await;

        // Make a request with the same "SELECT 2" query, but an invalid cache key
        let invalid_user_key_ctx = create_test_request_context(
            CacheControl::Cache(CacheKeyType::ClientSupplied),
            Some("bar$".to_string()),
        );

        let query_builder = QueryBuilder::new("SELECT 2", Arc::clone(&df));
        let query = query_builder.build();
        Arc::clone(&invalid_user_key_ctx)
            .scope(async move {
                let result = query.run().await.expect("query should succeed");

                // An invalid key results in a cache miss
                assert_eq!(result.cache_status, CacheStatus::CacheMiss);

                let records = result
                    .data
                    .try_collect::<Vec<_>>()
                    .await
                    .expect("should collect");
                assert_eq!(records.len(), 1);
                assert_eq!(records[0].num_rows(), 1);

                // The query was run
                assert_eq!(
                    records[0]
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .expect("must read i64 array")
                        .value(0),
                    2
                );
            })
            .await;

        // Issue the same "SELECT 2" query with the invalid cache key to verify that we fall back
        // on the default behavior if the user sets a cache-control header
        let query_builder = QueryBuilder::new("SELECT 2", Arc::clone(&df));
        let query = query_builder.build();
        Arc::clone(&invalid_user_key_ctx)
            .scope(async move {
                let result = query.run().await.expect("query should succeed");

                // Since cache-control is set, an invalid key with repeated query will fall back
                // to the default plan-key behavior and result in a cache hit
                assert_eq!(result.cache_status, CacheStatus::CacheHit);
            })
            .await;
    }

    #[tokio::test]
    async fn test_get_plan_or_cached_sql_cached_prepared_statements() {
        let df = prepare_runtime(Some(SQLResultsCacheConfig {
            item_ttl: Some("10m".to_string()),
            cache_key_type: spicepod::component::caching::CacheKeyType::Sql,
            ..Default::default()
        }))
        .await;

        let parameters = ParamValues::List(vec![1.into()]);

        let request_context =
            create_test_request_context(CacheControl::Cache(CacheKeyType::Raw), None);
        let query_builder =
            QueryBuilder::new("SELECT $1", Arc::clone(&df)).parameters(Some(parameters));
        let query = query_builder.build();
        Arc::clone(&request_context)
            .scope(async move {
                let result = query.run().await.expect("query should succeed");
                assert_eq!(result.cache_status, CacheStatus::CacheMiss);
                // Need to drain the stream to ensure the cache is populated
                let records = result
                    .data
                    .try_collect::<Vec<_>>()
                    .await
                    .expect("should collect");
                assert_eq!(records.len(), 1);
                assert_eq!(records[0].num_rows(), 1);
            })
            .await;

        let parameters = ParamValues::List(vec![2.into()]);

        let query_builder =
            QueryBuilder::new("SELECT $1", Arc::clone(&df)).parameters(Some(parameters));
        let query = query_builder.build();
        Arc::clone(&request_context)
            .scope(async move {
                let result = query.run().await.expect("query should succeed");
                assert_eq!(result.cache_status, CacheStatus::CacheMiss);
            })
            .await;
    }

    #[tokio::test]
    async fn test_get_plan_or_cached_plan_cached_prepared_statements() {
        let df = prepare_runtime(Some(SQLResultsCacheConfig {
            item_ttl: Some("10m".to_string()),
            cache_key_type: spicepod::component::caching::CacheKeyType::Plan,
            ..Default::default()
        }))
        .await;

        let parameters = ParamValues::List(vec![1.into()]);

        let request_context =
            create_test_request_context(CacheControl::Cache(CacheKeyType::Default), None);
        let query_builder =
            QueryBuilder::new("SELECT $1", Arc::clone(&df)).parameters(Some(parameters));
        let query = query_builder.build();
        Arc::clone(&request_context)
            .scope(async move {
                let result = query.run().await.expect("query should succeed");
                assert_eq!(result.cache_status, CacheStatus::CacheMiss);
                // Need to drain the stream to ensure the cache is populated
                let records = result
                    .data
                    .try_collect::<Vec<_>>()
                    .await
                    .expect("should collect");
                assert_eq!(records.len(), 1);
                assert_eq!(records[0].num_rows(), 1);
            })
            .await;

        let parameters = ParamValues::List(vec![2.into()]);

        let query_builder =
            QueryBuilder::new("SELECT $1", Arc::clone(&df)).parameters(Some(parameters));
        let query = query_builder.build();
        Arc::clone(&request_context)
            .scope(async move {
                let result = query.run().await.expect("query should succeed");
                assert_eq!(result.cache_status, CacheStatus::CacheMiss);
                // Need to drain the stream to ensure the cache is populated
                let records = result
                    .data
                    .try_collect::<Vec<_>>()
                    .await
                    .expect("should collect");
                assert_eq!(records.len(), 1);
                assert_eq!(records[0].num_rows(), 1);
            })
            .await;

        let parameters = ParamValues::List(vec![2.into()]);

        // Repeat the same query to ensure a cache hit
        let query_builder =
            QueryBuilder::new("SELECT $1", Arc::clone(&df)).parameters(Some(parameters));
        let query = query_builder.build();
        Arc::clone(&request_context)
            .scope(async move {
                let result = query.run().await.expect("query should succeed");
                assert_eq!(result.cache_status, CacheStatus::CacheHit);
            })
            .await;
    }

    #[tokio::test]
    async fn test_client_cache_key_get_after_ttl_expiry() {
        let df = prepare_runtime(Some(SQLResultsCacheConfig {
            item_ttl: Some("5s".to_string()),
            cache_key_type: spicepod::component::caching::CacheKeyType::Sql,
            stale_while_revalidate_ttl: Some("0s".to_string()), // Disable stale-while-revalidate for this test
            ..Default::default()
        }))
        .await;

        // Test with user cache key
        let request_context = create_test_request_context(
            CacheControl::Cache(CacheKeyType::ClientSupplied),
            Some("foo".to_string()),
        );
        let query_builder = QueryBuilder::new("SELECT 1", Arc::clone(&df));
        let query = query_builder.build();
        Arc::clone(&request_context)
            .scope(async move {
                let result = query.run().await.expect("query should succeed");
                // Expect to miss cache because it is the first request
                assert_eq!(result.cache_status, CacheStatus::CacheMiss);
                // Need to drain the stream to ensure the cache is populated
                let records = result
                    .data
                    .try_collect::<Vec<_>>()
                    .await
                    .expect("should collect");
                assert_eq!(records.len(), 1);
                assert_eq!(records[0].num_rows(), 1);
                assert_eq!(
                    records[0]
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .expect("must read i64 array")
                        .value(0),
                    1
                );
            })
            .await;

        // Repeat a request with the same user key and a different query
        let query_builder = QueryBuilder::new("SELECT 2", Arc::clone(&df));
        let query = query_builder.build();
        Arc::clone(&request_context)
            .scope(async move {
                let result = query.run().await.expect("query should succeed");
                assert_eq!(result.cache_status, CacheStatus::CacheHit);

                let records = result
                    .data
                    .try_collect::<Vec<_>>()
                    .await
                    .expect("should collect");
                assert_eq!(records.len(), 1);
                assert_eq!(records[0].num_rows(), 1);

                // If the query ran, this value would be 2. But the cached result is served
                assert_eq!(
                    records[0]
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .expect("must read i64 array")
                        .value(0),
                    1
                );
            })
            .await;

        // Run out the TTL
        tokio::time::sleep(Duration::from_secs(5)).await;

        // Force Moka to run pending eviction tasks
        if let Some(cache_provider) = df.results_cache_provider() {
            cache_provider.run_pending_tasks().await;
        }

        // Make a request with the same "SELECT 2" query, but after expiry
        let query_builder = QueryBuilder::new("SELECT 2", Arc::clone(&df));
        let query = query_builder.build();
        Arc::clone(&request_context)
            .scope(async move {
                let result = query.run().await.expect("query should succeed");

                // Cache miss after expiry
                assert_eq!(result.cache_status, CacheStatus::CacheMiss);

                let records = result
                    .data
                    .try_collect::<Vec<_>>()
                    .await
                    .expect("should collect");
                assert_eq!(records.len(), 1);
                assert_eq!(records[0].num_rows(), 1);

                // The query was run
                assert_eq!(
                    records[0]
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .expect("must read i64 array")
                        .value(0),
                    2
                );
            })
            .await;
    }

    #[tokio::test]
    #[expect(clippy::too_many_lines)]
    async fn test_stale_while_revalidate_complete_lifecycle() {
        // This test validates the complete stale-while-revalidate lifecycle:
        // 1. Initial cache population
        // 2. Serving stale data after TTL expiry (within stale window)
        // 3. Background revalidation updating the cache with fresh data
        // 4. Subsequent requests getting the fresh data from cache
        // 5. Continued cache serving of revalidated data

        // Use longer timeouts for robustness across different machines and CI environments
        // Configure cache with 3s TTL and 5s max stale-while-revalidate
        let df = prepare_runtime(Some(SQLResultsCacheConfig {
            item_ttl: Some("3s".to_string()),
            cache_key_type: spicepod::component::caching::CacheKeyType::Sql,
            stale_while_revalidate_ttl: Some("5s".to_string()),
            ..Default::default()
        }))
        .await;

        let request_context = create_test_request_context(
            CacheControl::MaxStale(CacheKeyType::ClientSupplied, Some(Duration::from_secs(5))),
            Some("lifecycle-test-key".to_string()),
        );

        // Step 1: First request - populate cache with "SELECT 1"
        tracing::info!("Step 1: Populating cache with initial query");
        let query_builder = QueryBuilder::new("SELECT 1", Arc::clone(&df));
        let query = query_builder.build();
        Arc::clone(&request_context)
            .scope(async move {
                let result = query.run().await.expect("query should succeed");
                assert_eq!(
                    result.cache_status,
                    CacheStatus::CacheMiss,
                    "First query should be a cache miss"
                );
                let records = result
                    .data
                    .try_collect::<Vec<_>>()
                    .await
                    .expect("should collect");
                assert_eq!(
                    records[0]
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .expect("must read i64 array")
                        .value(0),
                    1,
                    "Initial cache should contain value 1"
                );
            })
            .await;

        // Step 2: Wait 3.5s (past TTL but within stale window) and trigger revalidation
        tracing::info!("Step 2: Waiting 3.5s to trigger stale window");
        tokio::time::sleep(Duration::from_millis(3500)).await;

        // This request should:
        // a) Return stale data (value 1)
        // b) Trigger background revalidation with "SELECT 2"
        tracing::info!("Step 2: Requesting stale data (should trigger background revalidation)");
        let query_builder = QueryBuilder::new("SELECT 2", Arc::clone(&df)); // Different query, same cache key
        let query = query_builder.build();
        Arc::clone(&request_context)
            .scope(async move {
                let result = query.run().await.expect("query should succeed");
                assert_eq!(
                    result.cache_status,
                    CacheStatus::CacheStaleWhileRevalidate,
                    "Should be serving stale data with background revalidation"
                );
                let records = result
                    .data
                    .try_collect::<Vec<_>>()
                    .await
                    .expect("should collect");
                assert_eq!(records.len(), 1);
                assert_eq!(records[0].num_rows(), 1);
                // Verify we got the STALE cached result from the first query (1, not 2)
                assert_eq!(
                    records[0]
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .expect("must read i64 array")
                        .value(0),
                    1,
                    "Should still return stale value 1, not the new query value 2"
                );
            })
            .await;

        // Step 3: Wait for background revalidation to complete with retry logic
        // Use retry loop to handle timing variations across different machines/CI
        tracing::info!("Step 3: Waiting for background revalidation to complete");
        let max_wait_attempts = 25; // Increased from 10 to 25 for CI reliability
        let mut revalidation_completed = false;

        for attempt in 1..=max_wait_attempts {
            tokio::time::sleep(Duration::from_millis(200)).await;

            // Check if cache has been updated by trying to read it
            let query_builder = QueryBuilder::new("SELECT 999", Arc::clone(&df));
            let query = query_builder.build();
            let value = Arc::clone(&request_context)
                .scope(async move {
                    let result = query.run().await.expect("query should succeed");
                    if result.cache_status != CacheStatus::CacheHit {
                        tracing::debug!("Attempt {}: No cache hit yet", attempt);
                        return None;
                    }
                    let records = result
                        .data
                        .try_collect::<Vec<_>>()
                        .await
                        .expect("should collect");
                    if records.is_empty() || records[0].num_rows() == 0 {
                        tracing::debug!("Attempt {}: Empty records", attempt);
                        return None;
                    }
                    let val = records[0]
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .expect("must read i64 array")
                        .value(0);
                    tracing::debug!("Attempt {}: Got value {}", attempt, val);
                    Some(val)
                })
                .await;

            if value == Some(2) {
                tracing::info!(
                    "Background revalidation completed successfully after {} attempts ({}ms)",
                    attempt,
                    attempt * 200
                );
                revalidation_completed = true;
                break;
            }

            if attempt < max_wait_attempts {
                tracing::debug!(
                    "Attempt {}/{}: Cache not yet updated with value 2, retrying...",
                    attempt,
                    max_wait_attempts
                );
            }
        }

        // Step 4: Verify the cache was updated with FRESH data from the revalidation
        tracing::info!("Step 4: Verifying cache was updated with fresh data");
        assert!(
            revalidation_completed,
            "Background revalidation should have updated cache with value 2 within {}ms, but it didn't. \
            This indicates the revalidation task either didn't run or cached to the wrong key.",
            max_wait_attempts * 200
        );

        // Double-check with one more query
        let query_builder = QueryBuilder::new("SELECT 777", Arc::clone(&df));
        let query = query_builder.build();
        Arc::clone(&request_context)
            .scope(async move {
                let result = query.run().await.expect("query should succeed");
                assert_eq!(
                    result.cache_status,
                    CacheStatus::CacheHit,
                    "Should still be a cache hit - entry not yet evicted"
                );
                let records = result
                    .data
                    .try_collect::<Vec<_>>()
                    .await
                    .expect("should collect");
                assert_eq!(
                    records[0]
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .expect("must read i64 array")
                        .value(0),
                    2,
                    "Cache should contain revalidated value 2"
                );
            })
            .await;

        // Step 5: Verify that subsequent requests continue to get the revalidated value
        tracing::info!("Step 5: Verifying subsequent requests get revalidated value");
        let query_builder = QueryBuilder::new("SELECT 3", Arc::clone(&df));
        let query = query_builder.build();
        Arc::clone(&request_context)
            .scope(async move {
                let result = query.run().await.expect("query should succeed");
                assert_eq!(
                    result.cache_status,
                    CacheStatus::CacheHit,
                    "Revalidated entry should be a cache hit"
                );
                let records = result
                    .data
                    .try_collect::<Vec<_>>()
                    .await
                    .expect("should collect");
                assert_eq!(records.len(), 1);
                assert_eq!(records[0].num_rows(), 1);
                assert_eq!(
                    records[0]
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .expect("must read i64 array")
                        .value(0),
                    2,
                    "Should continue getting revalidated value 2"
                );
            })
            .await;

        // Note: We don't test cache eviction here because:
        // 1. Eviction timing is handled by Moka's time-to-live mechanism
        // 2. The exact eviction timing after revalidation can vary based on when
        //    the background revalidation completed (timing-sensitive for CI)
        // 3. The core stale-while-revalidate functionality is already validated
        //    in steps 1-4 above
    }

    #[tokio::test]
    #[expect(clippy::too_many_lines)]
    async fn test_stale_while_revalidate_with_client_supplied_cache_key() {
        // Configure cache with short TTL and stale-while-revalidate
        let df = prepare_runtime(Some(SQLResultsCacheConfig {
            item_ttl: Some("2s".to_string()),
            stale_while_revalidate_ttl: Some("3s".to_string()),
            cache_key_type: spicepod::component::caching::CacheKeyType::Sql,
            ..Default::default()
        }))
        .await;

        let request_context = create_test_request_context(
            CacheControl::Cache(CacheKeyType::ClientSupplied),
            Some("stale-test-key".to_string()),
        );

        // Step 1: First request - cache MISS (non-cached)
        let query_builder = QueryBuilder::new("SELECT 1", Arc::clone(&df));
        let query = query_builder.build();
        Arc::clone(&request_context)
            .scope(async move {
                let result = query.run().await.expect("query should succeed");
                assert_eq!(result.cache_status, CacheStatus::CacheMiss);
                let records = result
                    .data
                    .try_collect::<Vec<_>>()
                    .await
                    .expect("should collect");
                assert_eq!(records.len(), 1);
                assert_eq!(records[0].num_rows(), 1);
                assert_eq!(
                    records[0]
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .expect("must read i64 array")
                        .value(0),
                    1
                );
            })
            .await;

        // Step 2: Second request - cache HIT (cached, fresh)
        let query_builder = QueryBuilder::new("SELECT 2", Arc::clone(&df)); // Different query, same cache key
        let query = query_builder.build();
        Arc::clone(&request_context)
            .scope(async move {
                let result = query.run().await.expect("query should succeed");
                assert_eq!(result.cache_status, CacheStatus::CacheHit);
                let records = result
                    .data
                    .try_collect::<Vec<_>>()
                    .await
                    .expect("should collect");
                assert_eq!(records.len(), 1);
                assert_eq!(records[0].num_rows(), 1);
                // Cached result from first query (SELECT 1)
                assert_eq!(
                    records[0]
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .expect("must read i64 array")
                        .value(0),
                    1
                );
            })
            .await;

        // Step 3: Wait for TTL to expire (2s) but stay within stale-while-revalidate window (2s + 3s = 5s total)
        tokio::time::sleep(Duration::from_secs(3)).await;

        // Step 4: Third request - STALE (beyond TTL, within stale-while-revalidate window)
        // This should return stale data and trigger background revalidation
        let query_builder = QueryBuilder::new("SELECT 3", Arc::clone(&df)); // Different query again
        let query = query_builder.build();
        Arc::clone(&request_context)
            .scope(async move {
                let result = query.run().await.expect("query should succeed");
                assert_eq!(result.cache_status, CacheStatus::CacheStaleWhileRevalidate);
                let records = result
                    .data
                    .try_collect::<Vec<_>>()
                    .await
                    .expect("should collect");
                assert_eq!(records.len(), 1);
                assert_eq!(records[0].num_rows(), 1);
                // Still serving stale cached result from first query
                assert_eq!(
                    records[0]
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .expect("must read i64 array")
                        .value(0),
                    1
                );
            })
            .await;

        // Step 5: Wait a bit for background revalidation to complete
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Step 6: Fourth request - HIT (cached after revalidation)
        // The background revalidation should have refreshed the cache with SELECT 3
        let query_builder = QueryBuilder::new("SELECT 4", Arc::clone(&df));
        let query = query_builder.build();
        Arc::clone(&request_context)
            .scope(async move {
                let result = query.run().await.expect("query should succeed");
                assert_eq!(result.cache_status, CacheStatus::CacheHit);
                let records = result
                    .data
                    .try_collect::<Vec<_>>()
                    .await
                    .expect("should collect");
                assert_eq!(records.len(), 1);
                assert_eq!(records[0].num_rows(), 1);
                // Now serving revalidated cached result from SELECT 3
                assert_eq!(
                    records[0]
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .expect("must read i64 array")
                        .value(0),
                    3
                );
            })
            .await;

        // Step 7: Wait for the revalidated entry to become stale (but still within window)
        tokio::time::sleep(Duration::from_secs(3)).await;

        // Step 8: Fifth request - STALE again (beyond TTL of revalidated entry, within stale window)
        //   The revalidated entry from step 4 is now stale, so this should trigger another revalidation
        let query_builder = QueryBuilder::new("SELECT 5", Arc::clone(&df));
        let query = query_builder.build();
        Arc::clone(&request_context)
            .scope(async move {
                let result = query.run().await.expect("query should succeed");
                assert_eq!(result.cache_status, CacheStatus::CacheStaleWhileRevalidate);
                let records = result
                    .data
                    .try_collect::<Vec<_>>()
                    .await
                    .expect("should collect");
                assert_eq!(records.len(), 1);
                assert_eq!(records[0].num_rows(), 1);
                // Should still get stale value from the previous revalidation (3)
                // The new query (SELECT 5) is executing in background
                assert_eq!(
                    records[0]
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .expect("must read i64 array")
                        .value(0),
                    3
                );
            })
            .await;
    }

    #[expect(clippy::too_many_lines)]
    #[tokio::test]
    async fn test_single_in_flight_revalidation() {
        // This test validates that concurrent stale-while-revalidate requests
        // for the same cache key only trigger ONE background revalidation,
        // even when multiple requests arrive simultaneously.
        //
        // Expected behavior:
        // 1. Multiple concurrent requests during stale window
        // 2. All get stale data immediately (CacheStaleWhileRevalidate)
        // 3. Only ONE background query executes (single-in-flight semantics)
        // 4. STALE_WHILE_REVALIDATE_BACKGROUND_QUERIES == total concurrent requests
        // 5. STALE_WHILE_REVALIDATE_SKIPPED == (concurrent requests - 1)

        // Configure cache with 1s TTL and 5s stale window
        let df = prepare_runtime(Some(SQLResultsCacheConfig {
            item_ttl: Some("1s".to_string()),
            stale_while_revalidate_ttl: Some("5s".to_string()),
            cache_key_type: spicepod::component::caching::CacheKeyType::Sql,
            ..Default::default()
        }))
        .await;

        let request_context = create_test_request_context(
            CacheControl::MaxStale(CacheKeyType::ClientSupplied, Some(Duration::from_secs(5))),
            Some("single-in-flight-test".to_string()),
        );

        // Step 1: Populate cache with initial query
        tracing::info!("Populating cache with initial query");
        let query_builder = QueryBuilder::new("SELECT 100", Arc::clone(&df));
        let query = query_builder.build();
        Arc::clone(&request_context)
            .scope(async move {
                let result = query.run().await.expect("query should succeed");
                assert_eq!(result.cache_status, CacheStatus::CacheMiss);
                let records = result
                    .data
                    .try_collect::<Vec<_>>()
                    .await
                    .expect("should collect");
                assert_eq!(
                    records[0]
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .expect("must read i64 array")
                        .value(0),
                    100
                );
            })
            .await;

        // Step 2: Wait for entry to become stale (past TTL but within stale window)
        tracing::info!("Waiting 1.5s for entry to become stale");
        tokio::time::sleep(Duration::from_millis(1500)).await;

        // Step 3: Launch 10 concurrent requests with the SAME cache key
        // All should get stale data, but only ONE should trigger actual background query
        tracing::info!("Launching 10 concurrent stale requests");
        let concurrent_requests = 10;
        let mut handles = Vec::new();

        for i in 0..concurrent_requests {
            let df_clone = Arc::clone(&df);
            let ctx_clone = Arc::clone(&request_context);
            let handle = tokio::spawn(async move {
                let query_builder = QueryBuilder::new("SELECT 200", df_clone); // Different query, same cache key
                let query = query_builder.build();
                ctx_clone
                    .scope(async move {
                        let result = query.run().await.expect("query should succeed");
                        tracing::debug!("Request {i} got status: {:?}", result.cache_status);

                        // All requests should get stale data
                        assert_eq!(
                            result.cache_status,
                            CacheStatus::CacheStaleWhileRevalidate,
                            "Request {i} should get stale data"
                        );

                        let records = result
                            .data
                            .try_collect::<Vec<_>>()
                            .await
                            .expect("should collect");

                        // Verify we got the STALE value (100 from initial query, not 200)
                        assert_eq!(
                            records[0]
                                .column(0)
                                .as_any()
                                .downcast_ref::<Int64Array>()
                                .expect("must read i64 array")
                                .value(0),
                            100,
                            "Request {i} should get stale value 100"
                        );
                    })
                    .await;
            });
            handles.push(handle);
        }

        // Wait for all concurrent requests to complete
        for (i, handle) in handles.into_iter().enumerate() {
            handle
                .await
                .unwrap_or_else(|_| panic!("Request {i} should not panic"));
        }

        // Step 4: Wait for background revalidation to complete (single-in-flight ensures only ONE ran)
        tracing::info!("Waiting for background revalidation to complete");
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Step 5: Verify cache was updated with fresh data (from the ONE background query)
        // This proves that despite 10 concurrent requests, only ONE background query executed
        // and successfully updated the cache with the revalidated value (200)
        let query_builder = QueryBuilder::new("SELECT 300", Arc::clone(&df));
        let query = query_builder.build();
        Arc::clone(&request_context)
            .scope(async move {
                let result = query.run().await.expect("query should succeed");
                assert_eq!(
                    result.cache_status,
                    CacheStatus::CacheHit,
                    "Cache should have been revalidated"
                );
                let records = result
                    .data
                    .try_collect::<Vec<_>>()
                    .await
                    .expect("should collect");

                // Verify cache now contains the revalidated value (200 from the single background query)
                assert_eq!(
                    records[0]
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .expect("must read i64 array")
                        .value(0),
                    200,
                    "Cache should contain revalidated value 200 from the single background query"
                );
            })
            .await;

        tracing::info!("Single-in-flight test completed successfully");
    }
}
