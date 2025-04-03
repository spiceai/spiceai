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

use std::{collections::HashSet, sync::Arc};

use cache::{to_cached_record_batch_stream, CacheKey, QueryResultsCacheStatus, RawCacheKey};
use datafusion::{
    execution::{SendableRecordBatchStream, SessionState},
    logical_expr::LogicalPlan,
    physical_plan::memory::MemoryStream,
    sql::TableReference,
};
use tracing::Span;

use crate::{
    datafusion::{error::find_datafusion_root, query::error_code::ErrorCode, DataFusion},
    request::{CacheControl, CacheKeyType, RequestContext},
};

use super::{attach_query_tracker_to_stream, Query, QueryResult, QueryTracker};

/// Returns `Plan` if the result is not cached and needs to be executed, otherwise returns `Cached`
pub(super) enum PlanOrCached {
    Plan(LogicalPlan, QueryTracker, RequestCacheManager),
    Cached(QueryResult),
}

pub(super) struct RequestCacheManager {
    pub(super) cache_status: QueryResultsCacheStatus,
    pub(super) raw_cache_key: RawCacheKey,
}

impl RequestCacheManager {
    fn new(cache_status: QueryResultsCacheStatus, raw_cache_key: RawCacheKey) -> Self {
        Self {
            cache_status,
            raw_cache_key,
        }
    }

    pub(super) fn should_cache_results(&self) -> bool {
        !matches!(self.cache_status, QueryResultsCacheStatus::CacheDisabled)
    }
}

enum CacheResult {
    Hit(QueryResult),
    MissOrSkipped(QueryTracker, QueryResultsCacheStatus),
    WrongCacheKeyType(QueryTracker),
}

impl Query {
    /// Returns a `LogicalPlan` if the result is not cached and needs to be executed, otherwise returns a cached `QueryResult`.
    pub(super) async fn get_plan_or_cached(
        df: &DataFusion,
        session: &SessionState,
        request_context: Arc<RequestContext>,
        sql: &str,
        tracker: QueryTracker,
    ) -> super::Result<PlanOrCached> {
        // Try to get cached results first from sql
        let (tracker, cache_status) = match Self::try_get_cached_result(
            df,
            Arc::clone(&request_context),
            tracker,
            CacheKey::String(sql),
        )
        .await?
        {
            CacheResult::Hit(result) => return Ok(PlanOrCached::Cached(result)),
            CacheResult::MissOrSkipped(tracker, status) => (tracker, Some(status)),
            CacheResult::WrongCacheKeyType(tracker) => (tracker, None),
        };

        let plan = match session.create_logical_plan(sql).await {
            Ok(plan) => plan,
            Err(e) => {
                let e = find_datafusion_root(e);
                let error_code = ErrorCode::from(&e);
                let snafu_error = super::Error::UnableToExecuteQuery { source: e };
                tracker.finish_with_error(&request_context, snafu_error.to_string(), error_code);
                return Err(snafu_error);
            }
        };

        // Try to get cached results from plan
        let (mut tracker, cache_status) = match Self::try_get_cached_result(
            df,
            Arc::clone(&request_context),
            tracker,
            CacheKey::LogicalPlan(&plan),
        )
        .await?
        {
            CacheResult::Hit(result) => return Ok(PlanOrCached::Cached(result)),
            CacheResult::MissOrSkipped(tracker, status) => (tracker, status),
            CacheResult::WrongCacheKeyType(tracker) => (
                tracker,
                cache_status.unwrap_or(QueryResultsCacheStatus::CacheMiss),
            ),
        };

        let cache_status = Self::should_cache_results(df, &plan, cache_status);
        let plan_cache_key = CacheKey::LogicalPlan(&plan).as_raw_key();
        tracker = tracker.results_cache_hit(false);

        Ok(PlanOrCached::Plan(
            plan,
            tracker,
            RequestCacheManager::new(cache_status, plan_cache_key),
        ))
    }

    async fn try_get_cached_result(
        df: &DataFusion,
        request_context: Arc<RequestContext>,
        mut tracker: QueryTracker,
        key: CacheKey<'_>,
    ) -> super::Result<CacheResult> {
        let Some(cache_provider) = df.cache_provider() else {
            return Ok(CacheResult::MissOrSkipped(
                tracker,
                QueryResultsCacheStatus::CacheDisabled,
            ));
        };

        let cache_control = request_context.cache_control();

        // If the user requested no caching, skip the cache lookup
        let CacheControl::Cache(cache_key) = cache_control else {
            return Ok(CacheResult::MissOrSkipped(
                tracker,
                QueryResultsCacheStatus::CacheBypass,
            ));
        };

        // Validate that the provided cache key is the correct type for this request
        match (cache_key, &key) {
            (CacheKeyType::Default, CacheKey::LogicalPlan(_))
            | (CacheKeyType::Raw, CacheKey::String(_)) => {}
            _ => {
                return Ok(CacheResult::WrongCacheKeyType(tracker));
            }
        }

        let cached_result = match cache_provider.get(key).await {
            Ok(Some(result)) => result,
            Ok(None) => {
                return Ok(CacheResult::MissOrSkipped(
                    tracker,
                    QueryResultsCacheStatus::CacheMiss,
                ))
            }
            Err(e) => return Err(super::Error::FailedToAccessCache { source: e }),
        };

        tracker = tracker
            .datasets(cached_result.input_tables)
            .results_cache_hit(true);

        let record_batch_stream =
            match MemoryStream::try_new(cached_result.records.to_vec(), cached_result.schema, None)
            {
                Ok(stream) => stream,
                Err(e) => return Err(super::Error::UnableToCreateMemoryStream { source: e }),
            };

        Ok(CacheResult::Hit(QueryResult::new(
            attach_query_tracker_to_stream(
                Span::current(),
                request_context,
                tracker,
                Box::pin(record_batch_stream),
            ),
            QueryResultsCacheStatus::CacheHit,
        )))
    }

    fn should_cache_results(
        df: &DataFusion,
        plan: &LogicalPlan,
        cache_status: QueryResultsCacheStatus,
    ) -> QueryResultsCacheStatus {
        match df.cache_provider() {
            Some(provider) if provider.cache_is_enabled_for_plan(plan) => cache_status,
            _ => QueryResultsCacheStatus::CacheDisabled,
        }
    }

    pub(super) fn wrap_stream_with_cache(
        df: &DataFusion,
        stream: SendableRecordBatchStream,
        plan_cache_key: RawCacheKey,
        datasets: Arc<HashSet<TableReference>>,
    ) -> SendableRecordBatchStream {
        if let Some(cache_provider) = df.cache_provider() {
            to_cached_record_batch_stream(cache_provider, stream, plan_cache_key, datasets)
        } else {
            stream
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::{collections::HashSet, sync::Arc};

    use tokio::time::Instant;

    use cache::{CacheKey, QueryResultsCacheProvider, QueryResultsCacheStatus};
    use spicepod::component::runtime::ResultsCache;

    use crate::{
        datafusion::DataFusion,
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

    fn create_query_tracker() -> QueryTracker {
        QueryTracker {
            schema: None,
            query_duration_secs: None,
            query_execution_duration_secs: None,
            rows_produced: 0,
            results_cache_hit: None,
            is_accelerated: None,
            error_message: None,
            error_code: None,
            query_duration_timer: Instant::now(),
            query_execution_duration_timer: Instant::now(),
            datasets: Arc::new(HashSet::default()),
        }
    }

    #[tokio::test]
    async fn test_request_cache_manager() {
        let cache_status = QueryResultsCacheStatus::CacheHit;
        let raw_cache_key = CacheKey::String("test-key").as_raw_key();

        let manager = RequestCacheManager::new(cache_status, raw_cache_key);
        assert!(manager.should_cache_results());

        let raw_cache_key = CacheKey::String("test-key").as_raw_key();

        let disabled_manager =
            RequestCacheManager::new(QueryResultsCacheStatus::CacheDisabled, raw_cache_key);
        assert!(!disabled_manager.should_cache_results());
    }

    #[tokio::test]
    async fn test_get_plan_or_cached_cache_miss_and_hit() {
        let results_cache_config = ResultsCache {
            enabled: true,
            cache_max_size: None,
            item_ttl: Some("10m".to_string()),
            eviction_policy: None,
            cache_key_type: spicepod::component::runtime::CacheKeyType::Sql,
        };
        let cache_provider =
            QueryResultsCacheProvider::try_new(&results_cache_config, Box::new([]))
                .expect("valid cache provider");
        let df = DataFusion::builder(status::RuntimeStatus::new())
            .with_cache_provider(Arc::new(cache_provider))
            .build();

        // Test with SQL cache key
        let request_context = create_test_request_context(CacheControl::Cache(CacheKeyType::Raw));
        let tracker = create_query_tracker();

        let session = df.ctx.state();

        let result = Query::get_plan_or_cached(
            &df,
            &session,
            Arc::clone(&request_context),
            "SELECT 1",
            tracker,
        )
        .await;

        match result {
            Ok(PlanOrCached::Plan(_, _, cache_manager)) => {
                assert_eq!(
                    cache_manager.cache_status,
                    QueryResultsCacheStatus::CacheMiss
                );
            }
            Err(e) => panic!("Expected PlanOrCached::Plan, got {e:?}"),
            Ok(PlanOrCached::Cached(_)) => panic!("Expected PlanOrCached::Plan, got Cached"),
        }

        let tracker = create_query_tracker();
        let result =
            Query::get_plan_or_cached(&df, &session, request_context, "SELECT 1", tracker).await;

        match result {
            Ok(PlanOrCached::Cached(result)) => {
                assert_eq!(
                    result.results_cache_status,
                    QueryResultsCacheStatus::CacheHit
                );
            }
            Err(e) => panic!("Expected PlanOrCached::Cached, got {e:?}"),
            Ok(PlanOrCached::Plan(_, _, _)) => panic!("Expected PlanOrCached::Cached, got Plan"),
        }
    }

    // #[tokio::test]
    // async fn test_get_plan_or_cached_cache_bypass() {
    //     let mut mock_df = MockDataFusion::new();
    //     let mut mock_session = MockSessionState::new();

    //     // Setup expectations
    //     mock_session
    //         .expect_create_logical_plan()
    //         .returning(|_| Box::pin(async { Ok(create_test_logical_plan()) }));

    //     // Test with cache bypass
    //     let request_context = create_test_request_context(CacheControl::NoCache);
    //     let tracker = QueryTracker::new();

    //     let result = Query::get_plan_or_cached(
    //         &mock_df,
    //         &mock_session,
    //         request_context,
    //         "SELECT * FROM test",
    //         tracker,
    //     )
    //     .await;

    //     assert!(matches!(result, Ok(PlanOrCached::Plan(_, _, _))));
    // }

    // #[test]
    // fn test_should_cache_results() {
    //     let mut mock_df = MockDataFusion::new();
    //     let mut mock_cache_provider = MockCacheProvider::new();
    //     let plan = create_test_logical_plan();

    //     // Test with cache enabled
    //     mock_df
    //         .expect_cache_provider()
    //         .returning(move || Some(Arc::new(mock_cache_provider.clone())));

    //     mock_cache_provider
    //         .expect_cache_is_enabled_for_plan()
    //         .returning(|_| true);

    //     let result =
    //         Query::should_cache_results(&mock_df, &plan, QueryResultsCacheStatus::CacheHit);

    //     assert_eq!(result, QueryResultsCacheStatus::CacheHit);

    //     // Test with cache disabled
    //     mock_df.expect_cache_provider().returning(|| None);

    //     let result =
    //         Query::should_cache_results(&mock_df, &plan, QueryResultsCacheStatus::CacheHit);

    //     assert_eq!(result, QueryResultsCacheStatus::CacheDisabled);
    // }
}
