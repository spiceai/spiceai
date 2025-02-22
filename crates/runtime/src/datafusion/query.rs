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

use std::{cell::LazyCell, collections::HashSet, sync::Arc};

use arrow::{
    array::RecordBatch,
    datatypes::{Schema, SchemaRef},
};
use arrow_tools::schema::verify_schema;
use cache::{
    get_logical_plan_input_tables, to_cached_record_batch_stream, QueryResult,
    QueryResultsCacheStatus,
};
use datafusion::{
    error::DataFusionError,
    execution::{context::SQLOptions, SendableRecordBatchStream},
    logical_expr::LogicalPlan,
    physical_plan::{memory::MemoryStream, stream::RecordBatchStreamAdapter},
    prelude::DataFrame,
    sql::TableReference,
};
use error_code::ErrorCode;
use snafu::{ResultExt, Snafu};
use tokio::time::Instant;
use tracing::Span;
use tracing_futures::Instrument;
pub(crate) use tracker::QueryTracker;

pub mod builder;
pub use builder::QueryBuilder;
pub mod error_code;
mod metrics;
mod tracker;

use async_stream::stream;
use futures::StreamExt;

use crate::request::{AsyncMarker, CacheControl, RequestContext};

use super::{error::find_datafusion_root, DataFusion, SPICE_RUNTIME_SCHEMA};

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to execute query: {source}"))]
    UnableToExecuteQuery { source: DataFusionError },

    #[snafu(display("Failed to access query results cache: {source}"))]
    FailedToAccessCache { source: cache::Error },

    #[snafu(display("Unable to convert cached result to a record batch stream: {source}"))]
    UnableToCreateMemoryStream { source: DataFusionError },

    #[snafu(display("Unable to collect results after query execution: {source}"))]
    UnableToCollectResults { source: DataFusionError },

    #[snafu(display("Schema mismatch: {source}"))]
    SchemaMismatch { source: arrow_tools::schema::Error },
}

// There is no need to have a synchronized SQLOptions across all threads, each thread can have its own instance.
thread_local! {
    static RESTRICTED_SQL_OPTIONS: LazyCell<SQLOptions> = LazyCell::new(|| {
        SQLOptions::new()
            .with_allow_ddl(false)
            .with_allow_dml(false)
            .with_allow_statements(false)
    });
}

pub struct Query {
    df: Arc<crate::datafusion::DataFusion>,
    sql: Arc<str>,
    tracker: QueryTracker,
}

macro_rules! handle_error {
    ($self:expr, $request_context:expr, $error_code:expr, $error:expr, $target_error:ident) => {{
        let snafu_error = Error::$target_error { source: $error };
        $self.finish_with_error($request_context, snafu_error.to_string(), $error_code);
        return Err(snafu_error);
    }};
}

enum CacheResult {
    Hit(QueryResult),
    MissOrSkipped(QueryTracker, QueryResultsCacheStatus),
    Error(Error),
}

impl Query {
    async fn try_get_cached_result(
        df: &DataFusion,
        ctx: Arc<RequestContext>,
        mut tracker: QueryTracker,
        plan: &LogicalPlan,
    ) -> CacheResult {
        let Some(cache_provider) = df.cache_provider() else {
            return CacheResult::MissOrSkipped(tracker, QueryResultsCacheStatus::CacheDisabled);
        };

        // If the user requested no caching, skip the cache lookup
        if matches!(ctx.cache_control(), CacheControl::NoCache) {
            return CacheResult::MissOrSkipped(tracker, QueryResultsCacheStatus::CacheBypass);
        }

        let cached_result = match cache_provider.get(plan).await {
            Ok(Some(result)) => result,
            Ok(None) => {
                return CacheResult::MissOrSkipped(tracker, QueryResultsCacheStatus::CacheMiss)
            }
            Err(e) => return CacheResult::Error(Error::FailedToAccessCache { source: e }),
        };

        tracker = tracker
            .datasets(cached_result.input_tables)
            .results_cache_hit(true);

        let record_batch_stream =
            match MemoryStream::try_new(cached_result.records.to_vec(), cached_result.schema, None)
            {
                Ok(stream) => stream,
                Err(e) => {
                    return CacheResult::Error(Error::UnableToCreateMemoryStream { source: e })
                }
            };

        CacheResult::Hit(QueryResult::new(
            attach_query_tracker_to_stream(
                Span::current(),
                ctx,
                tracker,
                Box::pin(record_batch_stream),
            ),
            QueryResultsCacheStatus::CacheHit,
        ))
    }

    fn should_cache_results(
        df: &DataFusion,
        plan: &LogicalPlan,
        cache_status: QueryResultsCacheStatus,
    ) -> (bool, QueryResultsCacheStatus) {
        match df.cache_provider() {
            Some(provider) if provider.cache_is_enabled_for_plan(plan) => (true, cache_status),
            _ => (false, QueryResultsCacheStatus::CacheDisabled),
        }
    }

    fn wrap_stream_with_cache(
        df: &DataFusion,
        stream: SendableRecordBatchStream,
        plan_cache_key: u64,
        datasets: Arc<HashSet<TableReference>>,
    ) -> SendableRecordBatchStream {
        if let Some(cache_provider) = df.cache_provider() {
            to_cached_record_batch_stream(cache_provider, stream, plan_cache_key, datasets)
        } else {
            stream
        }
    }

    #[allow(clippy::too_many_lines)]
    pub async fn run(self) -> Result<QueryResult> {
        let request_context = RequestContext::current(AsyncMarker::new().await);
        crate::metrics::telemetry::track_query_count(&request_context.to_dimensions());

        let span = tracing::span!(target: "task_history", tracing::Level::INFO, "sql_query", input = %self.sql, runtime_query = false);
        let inner_span = span.clone();

        let query_result = async {
            let mut session = self.df.ctx.state();

            let ctx = self;
            let tracker = ctx.tracker;

            // Sets the request context as an extension on DataFusion, to allow recovering it to track telemetry
            session
                .config_mut()
                .set_extension(Arc::clone(&request_context));

            let plan = match session.create_logical_plan(&ctx.sql).await {
                Ok(plan) => plan,
                Err(e) => {
                    let e = find_datafusion_root(e);
                    let error_code = ErrorCode::from(&e);
                    handle_error!(
                        tracker,
                        &request_context,
                        error_code,
                        e,
                        UnableToExecuteQuery
                    )
                }
            };

            // Try to get cached results first
            let (mut tracker, cache_status) = match Self::try_get_cached_result(
                &ctx.df,
                Arc::clone(&request_context),
                tracker,
                &plan,
            )
            .await
            {
                CacheResult::Hit(result) => return Ok(result),
                CacheResult::MissOrSkipped(tracker, status) => (tracker, status),
                CacheResult::Error(e) => return Err(e),
            };

            let (plan_is_cache_enabled, cache_status) =
                Self::should_cache_results(&ctx.df, &plan, cache_status);
            let plan_cache_key = cache::key_for_logical_plan(&plan);
            tracker = tracker.results_cache_hit(false);

            if let Err(e) =
                RESTRICTED_SQL_OPTIONS.with(|sql_options| sql_options.verify_plan(&plan))
            {
                let e = find_datafusion_root(e);
                handle_error!(
                    tracker,
                    &request_context,
                    ErrorCode::QueryPlanningError,
                    e,
                    UnableToExecuteQuery
                )
            }

            let input_tables = get_logical_plan_input_tables(&plan);
            if input_tables
                .iter()
                .any(|tr| matches!(tr.schema(), Some(SPICE_RUNTIME_SCHEMA)))
            {
                inner_span.record("runtime_query", true);
            }

            // If any of the input tables are accelerated, mark the query as accelerated
            let mut is_accelerated = false;
            for tr in &input_tables {
                if ctx.df.is_accelerated(tr).await {
                    is_accelerated = true;
                    break;
                }
            }
            if is_accelerated {
                tracker.is_accelerated = Some(true);
            }

            tracker = tracker.datasets(Arc::new(input_tables));

            // Start the timer for the query execution
            tracker.query_execution_duration_timer = Instant::now();

            let df = DataFrame::new(session, plan);

            let df_schema: SchemaRef = Arc::clone(df.schema().inner());

            let res_stream: SendableRecordBatchStream = match df.execute_stream().await {
                Ok(stream) => stream,
                Err(e) => {
                    let e = find_datafusion_root(e);
                    let error_code = ErrorCode::from(&e);
                    handle_error!(
                        tracker,
                        &request_context,
                        error_code,
                        e,
                        UnableToExecuteQuery
                    )
                }
            };

            let res_schema = res_stream.schema();

            if let Err(e) = verify_schema(df_schema.fields(), res_schema.fields()) {
                handle_error!(
                    tracker,
                    &request_context,
                    ErrorCode::InternalError,
                    e,
                    SchemaMismatch
                )
            };

            let final_stream = if plan_is_cache_enabled {
                Self::wrap_stream_with_cache(
                    &ctx.df,
                    res_stream,
                    plan_cache_key,
                    Arc::clone(&tracker.datasets),
                )
            } else {
                res_stream
            };

            Ok(QueryResult::new(
                attach_query_tracker_to_stream(
                    inner_span,
                    Arc::clone(&request_context),
                    tracker,
                    final_stream,
                ),
                cache_status,
            ))
        }
        .instrument(span.clone())
        .await;

        match query_result {
            Ok(result) => Ok(result),
            Err(e) => {
                tracing::error!(target: "task_history", parent: &span, "{e}");
                Err(e)
            }
        }
    }

    pub fn finish_with_error(
        self,
        request_context: &RequestContext,
        error_message: String,
        error_code: ErrorCode,
    ) {
        self.tracker
            .finish_with_error(request_context, error_message, error_code);
    }

    pub async fn get_schema(self) -> Result<Schema, DataFusionError> {
        let session = self.df.ctx.state();
        let request_context = RequestContext::current(AsyncMarker::new().await);
        let plan = match session.create_logical_plan(&self.sql).await {
            Ok(plan) => plan,
            Err(e) => {
                let e = find_datafusion_root(e);
                self.handle_schema_error(&request_context, &e);
                return Err(e);
            }
        };

        // Verify the plan against the restricted options
        if let Err(e) = RESTRICTED_SQL_OPTIONS.with(|sql_options| sql_options.verify_plan(&plan)) {
            let e = find_datafusion_root(e);
            self.handle_schema_error(&request_context, &e);
            return Err(e);
        }
        Ok(plan.schema().as_arrow().clone())
    }

    fn handle_schema_error(self, request_context: &RequestContext, e: &DataFusionError) {
        // If there is an error getting the schema, we still want to track it in task history
        let span = tracing::span!(target: "task_history", tracing::Level::INFO, "sql_query", input = %self.sql, runtime_query = false);
        let error_code = ErrorCode::from(e);
        span.in_scope(|| {
            self.finish_with_error(request_context, e.to_string(), error_code);
        });
    }
}

#[must_use]
/// Attaches a query tracker to a stream of record batches.
///
/// Processes a stream of record batches, updating the query tracker
/// with the number of records returned and saving query details at the end.
///
/// Note: If an error occurs during stream processing, the query tracker
/// is finalized with error details, and further streaming is terminated.
fn attach_query_tracker_to_stream(
    span: Span,
    request_context: Arc<RequestContext>,
    tracker: QueryTracker,
    mut stream: SendableRecordBatchStream,
) -> SendableRecordBatchStream {
    let schema = stream.schema();
    let schema_copy = Arc::clone(&schema);

    let mut num_records = 0u64;
    let mut num_output_bytes = 0u64;

    let mut captured_output = "[]".to_string(); // default to empty preview

    let inner_span = span.clone();
    let updated_stream = stream! {
        while let Some(batch_result) = stream.next().await {
            let batch_result = batch_result.map_err(find_datafusion_root);
            match &batch_result {
                Ok(batch) => {
                    // Create a truncated output for the query history table on first batch.
                    if num_records == 0 {
                        captured_output = write_to_json_string(&[batch.slice(0, batch.num_rows().min(3))]).unwrap_or_default();
                    }

                    num_output_bytes += batch.get_array_memory_size() as u64;

                    num_records += batch.num_rows() as u64;
                    yield batch_result
                }
                Err(e) => {
                    tracker
                        .schema(schema_copy)
                        .rows_produced(num_records)
                        .finish_with_error(
                            &request_context,
                            e.to_string(),
                            ErrorCode::QueryExecutionError,
                        );
                    tracing::error!(target: "task_history", parent: &inner_span, "{e}");
                    yield batch_result;
                    return;
                }
            }
        }

        crate::metrics::telemetry::track_bytes_returned(num_output_bytes, &request_context.to_dimensions());

        tracker
            .schema(schema_copy)
            .rows_produced(num_records)
            .finish(&request_context, &Arc::from(captured_output));
    };

    Box::pin(RecordBatchStreamAdapter::new(
        schema,
        Box::pin(updated_stream.instrument(span)),
    ))
}

pub fn write_to_json_string(
    data: &[RecordBatch],
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let buf = Vec::new();
    let mut writer = arrow_json::ArrayWriter::new(buf);

    writer.write_batches(data.iter().collect::<Vec<&RecordBatch>>().as_slice())?;
    writer.finish()?;

    String::from_utf8(writer.into_inner()).boxed()
}
