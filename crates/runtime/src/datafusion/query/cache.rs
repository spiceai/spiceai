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
