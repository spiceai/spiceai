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

use std::{collections::HashSet, hash::Hasher, sync::Arc};

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
use snafu::ResultExt;
use tracing::Span;

use crate::{
    datafusion::{DataFusion, error::find_datafusion_root, query::error_code::ErrorCode},
    request::{CacheControl, CacheKeyType, RequestContext},
};

use super::{
    BindingParametersSnafu, Query, QueryResult, QueryTracker, attach_query_tracker_to_stream,
};

/// Returns `Plan` if the result is not cached and needs to be executed, otherwise returns `Cached`
pub(super) enum PlanOrCached {
    Plan(LogicalPlan, Option<QueryTracker>, RequestCacheManager),
    Cached(QueryResult),
}

pub(super) struct RequestCacheManager {
    pub(super) cache_status: CacheStatus,
    pub(super) raw_cache_key: RawCacheKey,
}

impl RequestCacheManager {
    fn new(cache_status: CacheStatus, raw_cache_key: RawCacheKey) -> Self {
        Self {
            cache_status,
            raw_cache_key,
        }
    }

    pub(super) fn should_cache_results(&self) -> bool {
        !matches!(self.cache_status, CacheStatus::CacheDisabled)
    }
}

enum CacheResult {
    Hit(QueryResult),
    MissOrSkipped(Option<QueryTracker>, CacheStatus),
    WrongCacheKeyType(Option<QueryTracker>),
}

impl Query {
    /// Returns a `LogicalPlan` if the result is not cached and needs to be executed, otherwise returns a cached `QueryResult`.
    pub(super) async fn get_plan_or_cached(
        df: &DataFusion,
        session: &SessionState,
        request_context: Arc<RequestContext>,
        sql: &str,
        parameters: Option<ParamValues>,
        tracker: Option<QueryTracker>,
    ) -> super::Result<PlanOrCached> {
        // Try to get cached results first from sql
        let sql_cache_key = CacheKey::Query(sql, parameters.as_ref());
        let (tracker, cache_status, sql_raw_cache_key) = match Self::try_get_cached_result(
            df,
            Arc::clone(&request_context),
            tracker,
            &sql_cache_key,
        )
        .await?
        {
            (CacheResult::Hit(result), _) => return Ok(PlanOrCached::Cached(result)),
            (CacheResult::MissOrSkipped(tracker, status), sql_raw_cache_key) => {
                (tracker, Some(status), sql_raw_cache_key)
            }
            (CacheResult::WrongCacheKeyType(tracker), sql_raw_cache_key) => {
                (tracker, None, sql_raw_cache_key)
            }
        };

        let plan_hasher = df.plans_cache_provider().map_or(
            Box::new(std::hash::DefaultHasher::new()) as Box<dyn Hasher>,
            |p| p.hasher(),
        );

        let sql_raw_cache_key =
            sql_raw_cache_key.unwrap_or_else(|| sql_cache_key.as_raw_key(plan_hasher));

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
        let (mut tracker, cache_status, plan_cache_key) = match Self::try_get_cached_result(
            df,
            Arc::clone(&request_context),
            tracker,
            &CacheKey::LogicalPlan(&plan),
        )
        .await?
        {
            (CacheResult::Hit(result), _) => return Ok(PlanOrCached::Cached(result)),
            (CacheResult::MissOrSkipped(tracker, status), plan_cache_key) => {
                (tracker, status, plan_cache_key)
            }
            (CacheResult::WrongCacheKeyType(tracker), plan_cache_key) => (
                tracker,
                cache_status.unwrap_or(CacheStatus::CacheMiss),
                plan_cache_key,
            ),
        };

        let raw_cache_key = plan_cache_key.unwrap_or(sql_raw_cache_key);

        let cache_status = Self::should_cache_results(df, &plan, cache_status);
        tracker = tracker.map(|t| t.results_cache_hit(false));

        Ok(PlanOrCached::Plan(
            plan,
            tracker,
            RequestCacheManager::new(cache_status, raw_cache_key),
        ))
    }

    async fn try_get_cached_result(
        df: &DataFusion,
        request_context: Arc<RequestContext>,
        mut tracker: Option<QueryTracker>,
        key: &CacheKey<'_>,
    ) -> super::Result<(CacheResult, Option<RawCacheKey>)> {
        let Some(cache_provider) = df.results_cache_provider() else {
            return Ok((
                CacheResult::MissOrSkipped(tracker, CacheStatus::CacheDisabled),
                None,
            ));
        };

        let cache_control = request_context.cache_control();

        // Validate that the provided cache key is the correct type for this request
        match (cache_control, &key) {
            (CacheControl::Cache(CacheKeyType::Default), CacheKey::LogicalPlan(_))
            | (CacheControl::Cache(CacheKeyType::Raw), CacheKey::Query(_, _)) => {}
            (CacheControl::NoCache, _) => {
                return Ok((
                    CacheResult::MissOrSkipped(tracker, CacheStatus::CacheBypass),
                    Some(key.as_raw_key(cache_provider.hasher())),
                ));
            }
            _ => {
                return Ok((CacheResult::WrongCacheKeyType(tracker), None));
            }
        }

        let raw_key = key.as_raw_key(cache_provider.hasher());

        let cached_result = match cache_provider.get_raw_key(&raw_key).await {
            Ok(Some(result)) => result,
            Ok(None) => {
                return Ok((
                    CacheResult::MissOrSkipped(tracker, CacheStatus::CacheMiss),
                    Some(raw_key),
                ));
            }
            Err(e) => return Err(super::Error::FailedToAccessCache { source: e }),
        };

        tracker = tracker.map(|t| {
            t.datasets(cached_result.input_tables)
                .results_cache_hit(true)
        });

        let record_batch_stream = CachedStream::new(cached_result.records, cached_result.schema);

        Ok((
            CacheResult::Hit(QueryResult::new(
                attach_query_tracker_to_stream(
                    Span::current(),
                    request_context,
                    tracker,
                    Box::pin(record_batch_stream),
                ),
                CacheStatus::CacheHit,
            )),
            Some(raw_key),
        ))
    }

    fn should_cache_results(
        df: &DataFusion,
        plan: &LogicalPlan,
        cache_status: CacheStatus,
    ) -> CacheStatus {
        match df.results_cache_provider() {
            Some(provider) if provider.cache_is_enabled_for_plan(plan) => cache_status,
            _ => CacheStatus::CacheDisabled,
        }
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

    use futures::TryStreamExt;

    use cache::{
        Caching, QueryResultsCacheProvider, SimpleCache, key::CacheKey, result::CacheStatus,
    };
    use spicepod::component::caching::{CacheConfig, SQLResultsCacheConfig};

    use crate::{
        builder::RuntimeBuilder,
        datafusion::{DataFusion, query::QueryBuilder},
        request::{CacheControl, CacheKeyType, Protocol, RequestContext},
        status,
    };

    // Helper function to create a test RequestContext
    fn create_test_request_context(cache_control: CacheControl) -> Arc<RequestContext> {
        Arc::new(
            RequestContext::builder(Protocol::Internal)
                .with_cache_control(cache_control)
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
        let results_cache_config = SQLResultsCacheConfig {
            inner: CacheConfig {
                item_ttl: Some("10m".to_string()),
                ..Default::default()
            },
            cache_key_type: spicepod::component::caching::CacheKeyType::Sql,
        };
        let cache_provider =
            QueryResultsCacheProvider::try_new(&results_cache_config, Box::new([]))
                .expect("valid cache provider");

        let plan_cache_provider = Arc::new(SimpleCache::new(
            512,
            Duration::from_secs(3600),
            std::hash::RandomState::default(),
        ));
        let runtime = RuntimeBuilder::new().build().await;
        let df = Arc::new(
            DataFusion::builder(
                status::RuntimeStatus::new(),
                runtime.accelerator_engine_registry(),
            )
            .with_caching(Arc::new(
                Caching::new()
                    .with_results_cache(Arc::new(cache_provider))
                    .with_plans_cache(plan_cache_provider),
            ))
            .build(),
        );

        // Test with SQL cache key
        let request_context = create_test_request_context(CacheControl::Cache(CacheKeyType::Raw));
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
            create_test_request_context(CacheControl::Cache(CacheKeyType::Default));
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
    }

    #[tokio::test]
    async fn test_get_plan_or_cached_sql_cached_prepared_statements() {
        let results_cache_config = SQLResultsCacheConfig {
            inner: CacheConfig {
                item_ttl: Some("10m".to_string()),
                ..Default::default()
            },
            cache_key_type: spicepod::component::caching::CacheKeyType::Sql,
        };
        let cache_provider =
            QueryResultsCacheProvider::try_new(&results_cache_config, Box::new([]))
                .expect("valid cache provider");

        let plan_cache_provider = Arc::new(SimpleCache::new(
            512,
            Duration::from_secs(3600),
            std::hash::RandomState::default(),
        ));
        let runtime = RuntimeBuilder::new().build().await;
        let df = Arc::new(
            DataFusion::builder(
                status::RuntimeStatus::new(),
                runtime.accelerator_engine_registry(),
            )
            .with_caching(Arc::new(
                Caching::new()
                    .with_results_cache(Arc::new(cache_provider))
                    .with_plans_cache(plan_cache_provider),
            ))
            .build(),
        );

        let parameters = ParamValues::List(vec![1.into()]);

        let request_context = create_test_request_context(CacheControl::Cache(CacheKeyType::Raw));
        let query_builder = QueryBuilder::new("SELECT $1", Arc::clone(&df)).parameters(parameters);
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

        let query_builder = QueryBuilder::new("SELECT $1", Arc::clone(&df)).parameters(parameters);
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
        let results_cache_config = SQLResultsCacheConfig {
            inner: CacheConfig {
                item_ttl: Some("10m".to_string()),
                ..Default::default()
            },
            cache_key_type: spicepod::component::caching::CacheKeyType::Plan,
        };
        let cache_provider =
            QueryResultsCacheProvider::try_new(&results_cache_config, Box::new([]))
                .expect("valid cache provider");

        let plan_cache_provider = Arc::new(SimpleCache::new(
            512,
            Duration::from_secs(3600),
            std::hash::RandomState::default(),
        ));
        let runtime = RuntimeBuilder::new().build().await;
        let df = Arc::new(
            DataFusion::builder(
                status::RuntimeStatus::new(),
                runtime.accelerator_engine_registry(),
            )
            .with_caching(Arc::new(
                Caching::new()
                    .with_results_cache(Arc::new(cache_provider))
                    .with_plans_cache(plan_cache_provider),
            ))
            .build(),
        );

        let parameters = ParamValues::List(vec![1.into()]);

        let request_context =
            create_test_request_context(CacheControl::Cache(CacheKeyType::Default));
        let query_builder = QueryBuilder::new("SELECT $1", Arc::clone(&df)).parameters(parameters);
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

        let query_builder = QueryBuilder::new("SELECT $1", Arc::clone(&df)).parameters(parameters);
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
        let query_builder = QueryBuilder::new("SELECT $1", Arc::clone(&df)).parameters(parameters);
        let query = query_builder.build();
        Arc::clone(&request_context)
            .scope(async move {
                let result = query.run().await.expect("query should succeed");
                assert_eq!(result.cache_status, CacheStatus::CacheHit);
            })
            .await;
    }
}
