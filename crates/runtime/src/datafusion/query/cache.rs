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
use dashmap::DashMap;
use datafusion::{
    common::ParamValues,
    execution::{SendableRecordBatchStream, SessionState},
    logical_expr::LogicalPlan,
    sql::TableReference,
};
use futures::TryStreamExt;
use runtime_request_context::{CacheControl, CacheKeyType, Protocol, RequestContext};
use scopeguard;
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
            Arc::clone(&request_context),
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
            Arc::clone(&request_context),
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
            | CacheControl::CacheWithStaleWhileRevalidate(CacheKeyType::Default, _) => {
                plan_raw_cache_key
            }
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

    #[allow(clippy::too_many_lines)]
    async fn try_get_cached_result<'a>(
        df: &Arc<DataFusion>,
        request_context: Arc<RequestContext>,
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
                | CacheControl::CacheWithStaleWhileRevalidate(CacheKeyType::Default, _),
                CacheKey::LogicalPlan(_),
            )
            | (
                CacheControl::Cache(CacheKeyType::Raw)
                | CacheControl::CacheWithStaleWhileRevalidate(CacheKeyType::Raw, _),
                CacheKey::Query(_, _),
            )
            | (
                CacheControl::Cache(CacheKeyType::ClientSupplied)
                | CacheControl::CacheWithStaleWhileRevalidate(CacheKeyType::ClientSupplied, _),
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

        // For stale-while-revalidate, check if the entry is beyond the allowed staleness window
        if let CacheControl::CacheWithStaleWhileRevalidate(_, stale_while_revalidate_duration) =
            cache_control
        {
            let ttl = cache_provider.ttl();
            let now = std::time::Instant::now();
            let max_age = ttl + stale_while_revalidate_duration;

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
        }

        tracker = tracker.map(|t| {
            t.datasets(Arc::clone(&cached_result.input_tables))
                .results_cache_hit(true)
        });

        // If stale-while-revalidate is enabled, check if data is stale (beyond TTL) and trigger background revalidation
        if let CacheControl::CacheWithStaleWhileRevalidate(_, _) = cache_control {
            let ttl = cache_provider.ttl();
            let now = std::time::Instant::now();
            if cached_result.is_stale(ttl, now) {
                tracing::debug!(
                    "Cache entry is stale (beyond TTL), triggering background revalidation for stale-while-revalidate"
                );
                // Extract plan from cache key if available to avoid re-parsing
                let plan = match key {
                    CacheKey::LogicalPlan(p) => Some(*p),
                    _ => None,
                };
                Self::trigger_background_query_revalidation(
                    Arc::clone(df),
                    sql,
                    &request_context,
                    plan,
                );
            }
        }

        let record_batch_stream = CachedStream::new(cached_result.records, cached_result.schema);

        Ok(CacheResponse::from(
            CacheResult::Hit(QueryResult::new(
                attach_query_tracker_to_stream(
                    Span::current(),
                    request_context,
                    tracker,
                    Box::pin(record_batch_stream),
                ),
                CacheStatus::CacheHit,
            )),
            CacheStatus::CacheHit,
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
    /// Uses lock-free deduplication to ensure only one revalidation task runs per query.
    /// Multiple concurrent requests for the same stale cache entry will not spawn redundant
    /// background tasks.
    ///
    /// The background task will be automatically cancelled if:
    /// - The `DataFusion` context is dropped (runtime shutdown)
    /// - The query execution is interrupted via the session context
    fn trigger_background_query_revalidation(
        df: Arc<DataFusion>,
        sql: &str,
        request_context: &Arc<RequestContext>,
        plan: Option<&LogicalPlan>,
    ) {
        // Static map to track ongoing revalidation tasks
        static REVALIDATION_TASKS: OnceLock<DashMap<String, ()>> = OnceLock::new();
        let tasks = REVALIDATION_TASKS.get_or_init(DashMap::new);

        let sql = sql.to_string();
        let plan = plan.cloned();

        // Try to insert a marker for this query - if it already exists, another task is running
        if tasks.insert(sql.clone(), ()).is_some() {
            tracing::debug!(
                "Background revalidation already in progress for this query, skipping duplicate"
            );
            return;
        }

        // Create a background request context that will cache results
        // Use the same cache key type as the original request, but remove stale-while-revalidate
        // to ensure the query executes normally and populates the cache
        let cache_control = match request_context.cache_control() {
            CacheControl::CacheWithStaleWhileRevalidate(key_type, _) => {
                CacheControl::Cache(key_type)
            }
            other => other,
        };
        let client_supplied_key = request_context.client_supplied_cache_key().clone();
        let background_context = Arc::new(
            RequestContext::builder(Protocol::Internal)
                .with_cache_control(cache_control)
                .with_client_supplied_cache_key(client_supplied_key)
                .build(),
        );

        // Spawn a detached task for background revalidation
        // The task will be automatically cancelled if the DataFusion context is dropped
        tokio::spawn(async move {
            // Ensure we remove the task marker when done, even if the task panics
            let _guard = scopeguard::guard((), |()| {
                tasks.remove(&sql);
            });

            // Execute the query through the full Query pipeline to ensure cache population
            let query = if let Some(logical_plan) = plan {
                tracing::debug!("Background revalidation: re-executing query with existing plan");
                super::Query::from_logical_plan(&df, &logical_plan)
            } else {
                tracing::debug!("Background revalidation: re-executing query (will re-parse SQL)");
                super::QueryBuilder::new(&sql, df).build()
            };
            let result = background_context
                .scope(async move { query.run().await })
                .await;

            match result {
                Ok(query_result) => {
                    // Drain the stream to ensure the query executes and cache is populated
                    match query_result.data.try_collect::<Vec<_>>().await {
                        Ok(batches) => {
                            tracing::debug!(
                                "Background revalidation completed successfully, {} batches cached",
                                batches.len()
                            );
                        }
                        Err(e) => {
                            tracing::debug!(
                                "Background revalidation failed during collection: {}",
                                e
                            );
                        }
                    }
                }
                Err(e) => {
                    tracing::debug!("Background revalidation query failed: {}", e);
                }
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
            std::hash::RandomState::default(),
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
    #[allow(clippy::too_many_lines)]
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
    async fn test_stale_while_revalidate_with_client_supplied_cache_key() {
        let df = prepare_runtime(Some(SQLResultsCacheConfig {
            item_ttl: Some("5s".to_string()),
            cache_key_type: spicepod::component::caching::CacheKeyType::Sql,
            ..Default::default()
        }))
        .await;

        // Test that CacheWithStaleWhileRevalidate validation accepts ClientSupplied cache keys
        // This verifies the validation logic works correctly  (testing actual timing behavior
        // is complex with moka's async eviction and is prone to flakiness)
        let request_context = create_test_request_context(
            CacheControl::CacheWithStaleWhileRevalidate(
                CacheKeyType::ClientSupplied,
                Duration::from_secs(5),
            ),
            Some("stale-test-key".to_string()),
        );

        // First request - cache miss, populates cache
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
            })
            .await;

        // Second request - cache hit (fresh data)
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
                // Cached result from first query
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
    }
}
