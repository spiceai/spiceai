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

use std::{fmt::Display, fmt::Write as _, sync::Arc};

use ::cache::{
    AsTableRefs, get_logical_plan_input_tables,
    key::CacheKey,
    result::{CacheStatus, query::QueryResult},
};
use arrow::{
    array::{
        Array, FixedSizeListArray, LargeListArray, MapArray, RecordBatch, StructArray, UnionArray,
    },
    datatypes::Schema,
};
use arrow_json::writer::JsonArray;
use arrow_schema::{Field, SchemaBuilder};
use arrow_tools::schema::verify_schema;
use cache::PlanOrCached;
use datafusion::{
    common::ParamValues,
    error::{DataFusionError, Result as DataFusionResult},
    execution::{SendableRecordBatchStream, TaskContext},
    logical_expr::LogicalPlan,
    physical_plan::{
        ExecutionPlan, ExecutionPlanProperties, execute_stream, repartition::RepartitionExec,
        sorts::sort_preserving_merge::SortPreservingMergeExec, stream::RecordBatchStreamAdapter,
    },
    scalar::ScalarValue,
    sql::TableReference,
};
use datafusion_functions_json::{JsonUnionEncoder, JsonUnionValue};
use error_code::ErrorCode;
use serde_json::{Map, Number, Value};
use snafu::{ResultExt, Snafu};
use tokio::time::Instant;
use tracing::Span;
use tracing_futures::Instrument;
pub(crate) use tracker::QueryTracker;

pub mod builder;
pub use builder::QueryBuilder;
mod cache;
pub mod error_code;
mod handle;
mod metrics;
pub mod registry;
pub mod stage_history;
mod tracker;

pub use handle::{DistributedJobStatus, QueryHandle, QueryHandleError};

use {
    ballista_core::extension::SessionConfigExt,
    ballista_scheduler::scheduler_server::SchedulerServer,
    datafusion_proto::protobuf::{LogicalPlanNode, PhysicalPlanNode},
};

use datafusion::execution::SessionState;
use datafusion::prelude::SessionContext;

use async_stream::stream;
use futures::StreamExt;

use super::{
    SPICE_RUNTIME_SCHEMA,
    error::{find_datafusion_root, format_datafusion_error},
};

use super::managed_runtime;
use crate::datafusion::{
    DataFusion,
    query::cache::RequestCacheManager,
    sql_validator::{validate_sql_query_operations, validate_sql_query_read_only},
};
use managed_runtime::ManagedRuntimeError;
use opentelemetry::KeyValue;
use runtime_datafusion::allowlist::ResolvedTableAwareAllowlist;
use runtime_datafusion::config::request_context_config::SpiceRequestContextConfig;
use runtime_request_context::{AsyncMarker, RequestContext};
use tokio::runtime::Handle;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to execute query: {}", format_datafusion_error(source)))]
    UnableToExecuteQuery { source: DataFusionError },

    #[snafu(display("Failed to access query results cache: {source}"))]
    FailedToAccessCache { source: ::cache::Error },

    #[snafu(display(
        "Unable to convert cached result to a record batch stream: {}",
        format_datafusion_error(source)
    ))]
    UnableToCreateMemoryStream { source: DataFusionError },

    #[snafu(display(
        "Unable to collect results after query execution: {}",
        format_datafusion_error(source)
    ))]
    UnableToCollectResults { source: DataFusionError },

    #[snafu(display("Schema mismatch: {source}"))]
    SchemaMismatch { source: arrow_tools::schema::Error },

    #[snafu(display(
        "Failed to set parameters in logical plan: {}",
        format_datafusion_error(source)
    ))]
    BindingParameters { source: DataFusionError },

    // Error message matches DataFusion's own error for table not found (not exposing existance of un-authorized table to unauthorized user).
    #[snafu(display("Failed to execute query: Error during planning: table {table} not found"))]
    TableAccessDisallowed { table: String },

    #[snafu(display(
        "Cache-Control header specifies 'stale-while-revalidate' which is only supported with cache_key_type: sql (raw). \
        The current configuration uses cache_key_type: {cache_key_type}. \
        Either remove 'stale-while-revalidate' from the Cache-Control header or change cache_key_type to 'sql'."
    ))]
    UnsupportedStaleWhileRevalidate { cache_key_type: String },

    #[snafu(display("Distributed query scheduler is not available"))]
    SchedulerUnavailable,

    #[snafu(display("Failed to create session for distributed query: {message}"))]
    SessionCreationFailed { message: String },

    #[snafu(display("Failed to submit job to distributed scheduler: {message}"))]
    JobSubmissionFailed { message: String },

    #[snafu(display(
        "Querying locally accelerated dataset '{table}' via async queries API is not currently supported. \
        Use the synchronous query API (/v1/sql or Flight SQL) instead."
    ))]
    AcceleratedTableNotSupportedInDistributedQuery { table: String },

    #[snafu(display(
        "Querying Cayenne catalog table '{table}' via async queries API is not currently supported. \
        Use the synchronous query API (/v1/sql or Flight SQL) instead."
    ))]
    CayenneCatalogTableNotSupportedInDistributedQuery { table: String },

    #[snafu(display("Query {query_id} was cancelled"))]
    QueryCancelled { query_id: String },
}

impl Error {
    // Attempts to return the internal [`DataFusionError`] if present. On error, returns the original error.
    pub fn attempt_internal_datafusion_err(self) -> Result<DataFusionError, Self> {
        match self {
            Self::UnableToExecuteQuery { source }
            | Self::UnableToCreateMemoryStream { source }
            | Self::UnableToCollectResults { source }
            | Self::BindingParameters { source } => Ok(source),
            e => Err(e),
        }
    }
}

pub enum QueryMethod {
    /// A pre-parsed logical plan with no associated SQL. The cache key is
    /// derived from the plan hash. Used by [`Query::from_logical_plan`].
    Plan(Box<LogicalPlan>),
    Text {
        sql: Arc<str>,
        parameters: Option<ParamValues>,

        /// An optional allowlist of tables that can be accessed by this query. When [`Option::is_some`], no SQL results caching is performed. [`LogicalPlan`] caching can still occur (since allowlisting is done post-plan).
        table_allowlist: Option<ResolvedTableAwareAllowlist>,

        /// A pre-parsed logical plan to use instead of re-parsing `sql`.
        /// The SQL string is still used for results-cache key computation so
        /// cached entries are shared with equivalent plain `Text` executions.
        pre_parsed_plan: Option<Box<LogicalPlan>>,
    },
}

impl Display for QueryMethod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Text { sql, .. } => write!(f, "{sql}"),
            Self::Plan(plan) => write!(f, "{}", plan.display_indent()),
        }
    }
}

pub struct Query {
    df: Arc<crate::datafusion::DataFusion>,
    sql: QueryMethod,
    tracker: Option<QueryTracker>,
    #[expect(
        clippy::struct_field_names,
        reason = "query_id matches the conventional naming for the query identifier"
    )]
    query_id: uuid::Uuid,
    /// Cancellation token for cooperative cancellation. If unset, query
    /// execution inherits the request-context cancellation token. Set this to
    /// override the request-context token for this query.
    cancellation_token: Option<tokio_util::sync::CancellationToken>,
    /// When true, the validator additionally rejects DDL, DML, COPY, or any
    /// `LogicalPlan::Statement` node (including PREPARE/EXECUTE/DEALLOCATE),
    /// regardless of per-catalog writability. Set via [`QueryBuilder::read_only`];
    /// used by `/v1/tools/sql` and `/v1/nsql` to contain LLM-generated SQL.
    read_only: bool,
}

macro_rules! handle_error {
    ($self:expr, $request_context:expr, $error_code:expr, $error:expr, $target_error:ident) => {{
        let snafu_error = Error::$target_error { source: $error };
        $self.map(|t| t.finish_with_error($request_context, snafu_error.to_string(), $error_code));
        return Err(snafu_error);
    }};
}

impl Query {
    fn ensure_not_cancelled(
        token: &tokio_util::sync::CancellationToken,
        query_id: &str,
    ) -> Result<()> {
        if token.is_cancelled() {
            return Err(Error::QueryCancelled {
                query_id: query_id.to_string(),
            });
        }
        Ok(())
    }

    /// Returns the session state for local query execution.
    ///
    /// For Flight SQL sessions, returns the session-specific context to preserve
    /// prepared statements. Otherwise, returns the default local context.
    fn get_session_state(&self, request_context: &Arc<RequestContext>) -> SessionState {
        // Check if there's a Flight SQL session-specific context
        if let Some(flight_session) =
            request_context.extension::<super::flight_session_extension::FlightSessionExtension>()
        {
            // Use session-specific context to preserve prepared statements
            return flight_session.session_context().state();
        }

        // Always use local execution for synchronous APIs (/v1/sql, FlightSQL)
        self.df.ctx.state()
    }

    /// Run a query and return the result.
    ///
    /// # Panics
    ///
    /// Panics when running under test if no cache key is computed for the query.
    pub async fn run(self) -> Result<QueryResult> {
        let request_context = RequestContext::current(AsyncMarker::new().await);
        if let Some(runtime_handle) = self.df.cpu_runtime().cloned() {
            return self
                .run_with_managed_runtime(request_context, runtime_handle)
                .await;
        }

        self.run_internal(request_context).await
    }

    /// Submit a query for distributed execution via Ballista and return a handle.
    ///
    /// This method submits a job to the Ballista scheduler and returns a `QueryHandle`
    /// that can be used to poll for status and retrieve results.
    /// This method returns immediately after job submission without waiting for completion.
    ///
    /// The returned `QueryHandle` provides methods for:
    /// - Polling job status (`poll_status`)
    /// - Cancelling the job (`cancel`)
    /// - Waiting for completion and retrieving results (`into_stream`)
    ///
    /// Results are cached based on the input cache key when retrieved.
    ///
    /// # Arguments
    ///
    /// * `job_id` - A unique identifier for this job, used as the Ballista session/job ID.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The scheduler server is not available
    /// - Session creation fails
    /// - The query plan cannot be created
    /// - Job submission fails
    pub async fn submit_distributed(self, job_id: &str) -> Result<QueryHandle> {
        let request_context = RequestContext::current(AsyncMarker::new().await);

        // Create the `task_history` span here so it survives the early
        // error paths inside `submit_distributed_internal`. On success the
        // span moves into `QueryHandle` and closes when the job
        // terminates; on failure (planning, validation, submission) we
        // emit an error event on it here so the row's `error_message` is
        // populated. Mirrors the sync `run_internal` shape.
        let span = tracing::span!(
            target: "task_history",
            tracing::Level::INFO,
            "sql_query",
            input = %self.sql,
            runtime_query = false,
            distributed = true,
            job_id = %job_id,
            // Distributed-job summary labels. Ballista 53 no longer exposes the
            // scheduler execution graph needed to populate these fields.
            ballista_job_id = tracing::field::Empty,
            stage_count = tracing::field::Empty,
            executor_count = tracing::field::Empty,
            total_tasks = tracing::field::Empty,
            total_executor_ms = tracing::field::Empty,
        );

        if let Some(traceparent) = request_context.trace_parent() {
            crate::http::traceparent::override_task_history_with_trace_parent(&span, traceparent);
        }

        let result = self
            .submit_distributed_internal(job_id, request_context, span.clone())
            .await;
        if let Err(e) = &result {
            tracing::error!(target: "task_history", parent: &span, "{e}");
        }
        result
    }

    /// Internal implementation for submitting a distributed query.
    async fn submit_distributed_internal(
        self,
        job_id: &str,
        request_context: Arc<RequestContext>,
        span: Span,
    ) -> Result<QueryHandle> {
        crate::metrics::telemetry::track_query_count(&request_context.to_dimensions());

        // Get the scheduler server
        let scheduler = Self::get_scheduler_server(&self.df)?;
        let tracker = self.tracker;

        // Create session for this job. The
        // `SpiceRequestContextConfig` extension propagates the originating
        // request's trace ids to executors through Ballista's
        // `TaskDefinition` props; the scheduler-side session builder reads
        // it back out and re-injects it on the built session config.
        let session_config = datafusion::prelude::SessionConfig::new_with_ballista()
            .with_option_extension(SpiceRequestContextConfig::from_request_context(
                &request_context,
            ));
        let session_ctx = scheduler
            .state
            .session_manager
            .create_or_update_session(job_id, &session_config)
            .await
            .map_err(|e| Error::SessionCreationFailed {
                message: e.to_string(),
            })?;

        // Get the session state for planning
        let session = session_ctx.state();

        // Get logical plan and cache key, reusing existing cache infrastructure
        let (plan, mut tracker, cache_key) = match &self.sql {
            QueryMethod::Text {
                sql,
                parameters,
                pre_parsed_plan,
                ..
            } => {
                // Use the existing get_plan_or_cached which handles all cache
                // control, stale-while-revalidate, and query tracking. The
                // cache itself is namespaced per principal and refuses to
                // store write-capable plans, so a read-only caller cannot
                // observe a cached entry produced by a write-capable plan.
                match Query::get_plan_or_cached(
                    &self.df,
                    &session,
                    Arc::clone(&request_context),
                    sql,
                    parameters.clone(),
                    tracker,
                    pre_parsed_plan.clone(),
                )
                .await?
                {
                    cache::PlanOrCached::Cached(cached_result) => {
                        tracing::debug!(job_id, "Returning cached result for distributed query");
                        // Return a QueryHandle with cached results
                        let schema = cached_result.data.schema();
                        return Ok(QueryHandle::new_with_cached_result(
                            job_id.to_string(),
                            schema,
                            Arc::clone(&self.df),
                            None, // Cache key already used for lookup
                            cached_result.data,
                            Arc::clone(&request_context),
                        ));
                    }
                    cache::PlanOrCached::Plan(plan, tracker, cache_manager) => {
                        // Plan needs execution - cache_manager contains the raw cache key for storing results
                        let cache_key = if cache_manager.should_cache_results() {
                            Some(cache_manager.raw_cache_key)
                        } else {
                            None
                        };
                        (*plan, tracker, cache_key)
                    }
                }
            }
            QueryMethod::Plan(logical_plan) => {
                // For direct plan submission, compute cache key and check cache
                let plan_cache_key =
                    CacheKey::LogicalPlan(logical_plan).as_raw_key(Self::plan_hasher(&self.df));

                // Check for cached results using the standard cache lookup
                if let Some(cache_provider) = self.df.results_cache_provider()
                    && let Ok(Some(cached_result)) =
                        cache_provider.get_raw_key(&plan_cache_key).await
                {
                    let ttl = cache_provider.ttl();
                    let now = std::time::Instant::now();
                    if !cached_result.is_stale(ttl, now)
                        && let Ok(records) = cached_result.records().await
                    {
                        tracing::debug!(
                            job_id,
                            cache_key = plan_cache_key.as_u64(),
                            "Returning cached result for distributed query (plan)"
                        );
                        let stream = ::cache::result::query::CachedStream::new(
                            Arc::new(records),
                            cached_result.schema,
                        );
                        return Ok(QueryHandle::new_with_cached_result(
                            job_id.to_string(),
                            Arc::clone(logical_plan.schema().inner()),
                            Arc::clone(&self.df),
                            None,
                            Box::pin(stream),
                            Arc::clone(&request_context),
                        ));
                    }
                }

                (logical_plan.as_ref().clone(), tracker, Some(plan_cache_key))
            }
        };

        // Validate query operations
        if let Err(e) = validate_sql_query_operations(&plan, &self.df) {
            let e = find_datafusion_root(e);
            return Err(Error::UnableToExecuteQuery { source: e });
        }
        if self.read_only
            && let Err(e) = validate_sql_query_read_only(&plan)
        {
            let e = find_datafusion_root(e);
            return Err(Error::UnableToExecuteQuery { source: e });
        }

        // Get the schema from the logical plan
        let schema = Arc::new(plan.schema().as_arrow().clone());

        let input_tables = get_logical_plan_input_tables(&plan);
        if input_tables
            .iter()
            .any(|tr| matches!(tr.schema(), Some(SPICE_RUNTIME_SCHEMA)))
        {
            span.record("runtime_query", true);
        }

        // Distributed execution doesn't currently support querying accelerated datasets
        // or Cayenne catalog tables
        for tr in &input_tables {
            if self.df.is_accelerated(tr).await {
                return Err(Error::AcceleratedTableNotSupportedInDistributedQuery {
                    table: tr.to_string(),
                });
            }
            if self.df.is_cayenne_catalog(tr) {
                return Err(Error::CayenneCatalogTableNotSupportedInDistributedQuery {
                    table: tr.to_string(),
                });
            }
        }

        // All tables verified non-accelerated above
        tracker = tracker.map(|mut t| {
            t.is_accelerated = Some(false);
            t
        });

        let datasets = Arc::new(input_tables);
        let tracker = tracker.map(|t| t.datasets(Arc::clone(&datasets)));

        // Start the timer for the query execution
        let tracker = tracker.map(|mut t| {
            t.query_execution_duration_timer = Instant::now();
            t
        });

        // Submit the job to the Ballista scheduler
        let ballista_job_id = scheduler
            .submit_job(job_id, session_ctx, &plan, None)
            .await
            .map_err(|e| Error::JobSubmissionFailed {
                message: e.to_string(),
            })?;

        tracing::debug!(
            job_id,
            ballista_job_id = %ballista_job_id,
            "Job submitted to Ballista scheduler"
        );

        Ok(QueryHandle::new(
            ballista_job_id,
            scheduler,
            schema,
            datasets,
            Arc::clone(&self.df),
            cache_key,
            tracker,
            request_context,
            span,
        ))
    }

    /// Returns the scheduler server if available.
    fn get_scheduler_server(
        df: &DataFusion,
    ) -> Result<Arc<SchedulerServer<LogicalPlanNode, PhysicalPlanNode>>> {
        df.scheduler_server
            .try_read()
            .ok()
            .and_then(|guard| guard.clone())
            .ok_or(Error::SchedulerUnavailable)
    }

    async fn run_with_managed_runtime(
        self,
        request_context: Arc<RequestContext>,
        runtime_handle: Handle,
    ) -> Result<QueryResult> {
        let span = Span::current();

        let runtime_request_context = Arc::clone(&request_context);
        let future_request_context = request_context;

        let managed_stream = managed_runtime::run_record_batch_stream_on_runtime(
            runtime_handle,
            runtime_request_context,
            span,
            async move {
                self.run_internal(future_request_context)
                    .await
                    .map(|query_result| (query_result.cache_status, query_result.data))
            },
        )
        .await
        .map_err(|err| match err {
            ManagedRuntimeError::Future(err) => err,
            ManagedRuntimeError::DriverTaskEnded => Error::UnableToExecuteQuery {
                source: DataFusionError::Execution(
                    "Query driver task ended unexpectedly".to_string(),
                ),
            },
        })?;

        let (cache_status, stream) = managed_stream.into_parts();

        Ok(QueryResult::new(stream, cache_status))
    }

    async fn run_internal(self, request_context: Arc<RequestContext>) -> Result<QueryResult> {
        crate::metrics::telemetry::track_query_count(&request_context.to_dimensions());

        let span = tracing::span!(target: "task_history", tracing::Level::INFO, "sql_query", input = %self.sql, runtime_query = false);

        if let Some(traceparent) = request_context.trace_parent() {
            crate::http::traceparent::override_task_history_with_trace_parent(&span, traceparent);
        }

        // Resolve the cancellation token for this query. An explicit token on
        // the `Query` takes precedence; otherwise the query inherits the
        // request-scoped token so that cancelling the originating request
        // cancels this query too. A child token is used so that cancelling the
        // query (via admin cancel endpoint) does not propagate upwards to the
        // request and abort other in-progress work.
        let query_cancel_token = match &self.cancellation_token {
            Some(t) => t.clone(),
            None => request_context.child_cancellation_token(),
        };

        // Register in the DataFusion-owned active-query registry so administrative
        // cancel endpoints can locate this query by id. The guard is captured
        // by the returned stream so the registration is removed on completion,
        // drop, or cancellation.
        let sql_preview = match &self.sql {
            QueryMethod::Text { sql, .. } => sql.as_ref(),
            QueryMethod::Plan(_) => "<logical plan>",
        };
        let active_query_guard = self.df.query_cancel_registry().register(
            self.query_id,
            sql_preview.as_ref(),
            request_context.protocol(),
            query_cancel_token.clone(),
        );
        let query_id_str = self.query_id.to_string();

        let inner_span = span.clone();

        let query_result =
            async {
                Self::ensure_not_cancelled(&query_cancel_token, &query_id_str)?;

                let mut session = self.get_session_state(&request_context);

                let ctx = self;
                let tracker = ctx.tracker;

                // Sets the request context as an extension on DataFusion, to allow recovering it to track telemetry
                session
                    .config_mut()
                    .set_extension(Arc::clone(&request_context));

                // Get the `LogicalPlan` or cached results
                let (plan, mut tracker, cache_manager) = match &ctx.sql {
                    QueryMethod::Text {
                        sql,
                        parameters,
                        table_allowlist: Some(allowlist),
                        pre_parsed_plan,
                    } => {
                        let raw_cache_key = CacheKey::Query(sql, parameters.as_ref())
                            .as_raw_key(Query::plan_hasher(&ctx.df));
                        let plan = if let Some(plan) = pre_parsed_plan {
                            plan.clone()
                        } else {
                            Self::ensure_not_cancelled(&query_cancel_token, &query_id_str)?;
                            match Self::get_plan(
                                &ctx.df,
                                &session,
                                sql,
                                &raw_cache_key,
                                parameters.clone(),
                            )
                            .await
                            {
                                Ok(plan) => Box::new(plan),
                                Err(e) => match e {
                                    Error::UnableToExecuteQuery { source } => {
                                        let code = ErrorCode::from(&source);
                                        let snafu_err = Error::UnableToExecuteQuery { source };
                                        if let Some(t) = tracker {
                                            t.finish_with_error(
                                                &request_context,
                                                snafu_err.to_string(),
                                                code,
                                            );
                                        }
                                        return Err(snafu_err);
                                    }
                                    _ => return Err(e),
                                },
                            }
                        };
                        Self::ensure_not_cancelled(&query_cancel_token, &query_id_str)?;
                        let tables_referenced = plan.as_table_refs();
                        if let Some(disallowed_table) = tables_referenced
                            .iter()
                            .find(|&t| !allowlist.table_is_allowed(t))
                        {
                            return Err(Error::TableAccessDisallowed {
                                table: disallowed_table.to_string(),
                            });
                        }

                        (
                            plan,
                            tracker,
                            RequestCacheManager::new(CacheStatus::CacheDisabled, raw_cache_key),
                        )
                    }
                    QueryMethod::Text {
                        sql,
                        parameters,
                        table_allowlist: None,
                        pre_parsed_plan,
                    } => {
                        match Self::get_plan_or_cached(
                            &ctx.df,
                            &session,
                            Arc::clone(&request_context),
                            sql,
                            parameters.clone(),
                            tracker,
                            pre_parsed_plan.clone(),
                        )
                        .await?
                        {
                            PlanOrCached::Plan(plan, tracker, cache_manager) => {
                                Self::ensure_not_cancelled(&query_cancel_token, &query_id_str)?;
                                (plan, tracker, cache_manager)
                            }
                            PlanOrCached::Cached(query_result) => {
                                Self::ensure_not_cancelled(&query_cancel_token, &query_id_str)?;
                                return Ok(attach_cancellation_to_query_result(
                                    query_result,
                                    query_cancel_token.clone(),
                                    query_id_str.clone(),
                                    active_query_guard,
                                ));
                            }
                        }
                    }
                    QueryMethod::Plan(logical_plan) => {
                        let cache_manager = RequestCacheManager::new(
                            CacheStatus::CacheMiss,
                            CacheKey::LogicalPlan(logical_plan)
                                .as_raw_key(Query::plan_hasher(&ctx.df)),
                        );
                        (logical_plan.clone(), None, cache_manager)
                    }
                };

                Self::ensure_not_cancelled(&query_cancel_token, &query_id_str)?;

                if let Err(e) = validate_sql_query_operations(&plan, &ctx.df) {
                    let e = find_datafusion_root(e);
                    handle_error!(
                        tracker,
                        &request_context,
                        ErrorCode::QueryPlanningError,
                        e,
                        UnableToExecuteQuery
                    )
                }

                if ctx.read_only
                    && let Err(e) = validate_sql_query_read_only(&plan)
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

                // Proactively invalidate cached query state for tables affected by
                // DML mutations (INSERT, DELETE, UPDATE).
                // - results cache must be cleared so repeated SQL does not replay
                //   pre-mutation answers
                // - plans cache must be cleared so future queries re-resolve table
                //   providers with up-to-date in-memory state.
                if let Some(dml_table) = extract_dml_target_table(&plan)
                    && let Err(e) = ctx.df.caching().invalidate_for_table(dml_table.clone())
                {
                    tracing::warn!(
                        "Failed to invalidate caches for table {dml_table} before DML: {e}",
                    );
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
                    tracker = tracker.map(|mut t| {
                        t.is_accelerated = Some(true);
                        t
                    });
                }

                let datasets = Arc::new(input_tables);
                tracker = tracker.map(|t| t.datasets(Arc::clone(&datasets)));

                // Start the timer for the query execution
                tracker = tracker.map(|mut t| {
                    t.query_execution_duration_timer = Instant::now();
                    t
                });

                // Statement plans (PREPARE, EXECUTE, DEALLOCATE) need special handling
                // They modify session state rather than producing query results, so must be
                // executed through SessionContext::execute_logical_plan() instead of create_physical_plan()
                let (res_stream, physical_plan): (
                    SendableRecordBatchStream,
                    Arc<dyn ExecutionPlan>,
                ) = if matches!(&*plan, LogicalPlan::Statement(_)) {
                    // For Statement plans, use SessionContext::execute_logical_plan()
                    // which handles PREPARE/EXECUTE/DEALLOCATE by modifying session state.
                    // Use the session-specific context if available to ensure prepared statements
                    // are scoped to individual sessions.
                    let session_ctx = if let Some(flight_session) =
                        request_context
                            .extension::<super::flight_session_extension::FlightSessionExtension>()
                    {
                        tracing::debug!(
                            "Statement plan using Flight session: {}",
                            flight_session.session_context().session_id()
                        );
                        Arc::clone(flight_session.session_context())
                    } else {
                        tracing::debug!(
                            "Statement plan using ad-hoc session (no FlightSessionExtension)"
                        );
                        Arc::new(SessionContext::new_with_state(ctx.df.ctx.state()))
                    };

                    Self::ensure_not_cancelled(&query_cancel_token, &query_id_str)?;
                    let dataframe = match session_ctx
                        .execute_logical_plan(plan.as_ref().clone())
                        .await
                    {
                        Ok(df) => df,
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

                    Self::ensure_not_cancelled(&query_cancel_token, &query_id_str)?;
                    // Create a physical plan from the dataframe and execute it with our own TaskContext
                    // that includes the request context. This ensures BytesProcessedExec has access to it.
                    let df_plan = match dataframe.create_physical_plan().await {
                        Ok(p) => p,
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

                    Self::ensure_not_cancelled(&query_cancel_token, &query_id_str)?;
                    let task_ctx = Arc::new(TaskContext::from(&session));
                    let stream = match execute_stream_preserving_output_order(
                        Arc::clone(&df_plan),
                        task_ctx,
                    ) {
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
                    (stream, df_plan)
                } else {
                    // For regular plans, use the standard physical plan execution
                    Self::ensure_not_cancelled(&query_cancel_token, &query_id_str)?;
                    let physical_plan = match session.create_physical_plan(&plan).await {
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

                    Self::ensure_not_cancelled(&query_cancel_token, &query_id_str)?;
                    let task_ctx = Arc::new(TaskContext::from(&session));

                    let stream = match execute_stream_preserving_output_order(
                        Arc::clone(&physical_plan),
                        task_ctx,
                    ) {
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
                    Self::ensure_not_cancelled(&query_cancel_token, &query_id_str)?;
                    (stream, physical_plan)
                };

                Self::ensure_not_cancelled(&query_cancel_token, &query_id_str)?;

                // Skip schema verification for Statement plans (PREPARE/EXECUTE/DEALLOCATE),
                // DDL plans (CREATE TABLE/DROP TABLE), DML Delete/Update plans, and Spice
                // DML extension nodes, as their logical plan schema may differ from the
                // actual execution result (DDL/DML plans may be rewritten by analyzer rules
                // into extension nodes with different output schemas).
                let res_stream =
                    if !matches!(&*plan, LogicalPlan::Statement(_) | LogicalPlan::Ddl(_))
                        && !matches!(
                            &*plan,
                            LogicalPlan::Dml(dml)
                                if matches!(
                                    &dml.op,
                                    datafusion::logical_expr::WriteOp::Delete
                                        | datafusion::logical_expr::WriteOp::Update
                                )
                        )
                        && !is_dml_extension(&plan)
                    {
                        let plan_schema = Arc::clone(plan.schema().inner());
                        let res_schema = res_stream.schema();

                        if let Err(e) = verify_schema(plan_schema.fields(), res_schema.fields()) {
                            handle_error!(
                                tracker,
                                &request_context,
                                ErrorCode::InternalError,
                                e,
                                SchemaMismatch
                            )
                        }
                        // The AggregateStatistics physical optimizer may replace an
                        // AggregateExec with a ProjectionExec containing a literal
                        // value, which changes the output nullability (literals report
                        // nullable = value.is_null()).  Reconcile the execution result
                        // schema with the logical plan schema so downstream consumers
                        // (e.g. FlightSQL GetFlightInfo vs DoGet) see consistent
                        // nullability.
                        reconcile_stream_nullability(res_stream, &plan_schema)
                    } else {
                        res_stream
                    };

                let final_stream = if cache_manager.should_cache_results() {
                    Self::wrap_stream_with_cache(
                        &ctx.df,
                        res_stream,
                        cache_manager.raw_cache_key,
                        datasets,
                    )
                } else {
                    res_stream
                };

                let final_stream = attach_physical_plan_metrics_to_stream(
                    final_stream,
                    physical_plan,
                    Arc::clone(&request_context),
                    inner_span.clone(),
                );

                let final_stream = attach_query_active_guard_to_stream(
                    final_stream,
                    &request_context,
                    inner_span.clone(),
                );

                // Wrap with cancellation observation so that cancelling the
                // query (via HTTP `/v1/sql/{id}/cancel`, the custom Flight
                // `CancelQuery` action, or client disconnect) terminates the
                // stream with a clear error. The active-query registry guard is held by the
                // wrapped stream so deregistration occurs on drop.
                let final_stream = attach_cancellation_to_stream(
                    final_stream,
                    query_cancel_token.clone(),
                    query_id_str.clone(),
                    active_query_guard,
                );

                Ok(QueryResult::new(
                    attach_query_tracker_to_stream(
                        inner_span,
                        Arc::clone(&request_context),
                        tracker,
                        final_stream,
                    ),
                    cache_manager.cache_status,
                ))
            }
            .instrument(span.clone());

        // Keep this large async block out of callers' state machines. This
        // preserves the concrete future type, avoiding dynamic dispatch while
        // still moving the state machine itself to the heap.
        let query_result = Box::pin(query_result).await;

        match query_result {
            Ok(result) => Ok(result),
            Err(e) => {
                tracing::error!(target: "task_history", parent: &span, "{e}");
                Err(e)
            }
        }
    }

    pub fn from_logical_plan(df: &Arc<DataFusion>, plan: &LogicalPlan) -> Self {
        Self {
            df: Arc::clone(df),
            sql: QueryMethod::Plan(Box::new(plan.clone())),
            tracker: None,
            query_id: uuid::Uuid::new_v4(),
            cancellation_token: None,
            read_only: false,
        }
    }

    #[must_use]
    pub fn display_sql(&self) -> String {
        format!("{}", self.sql)
    }

    pub fn finish_with_error(
        self,
        request_context: &RequestContext,
        error_message: String,
        error_code: ErrorCode,
    ) {
        if let Some(t) = self.tracker {
            t.finish_with_error(request_context, error_message, error_code);
        }
    }

    /// Return the schema for the data and (possibly) the parameters of a [`Query`].
    pub async fn get_schema(self) -> Result<(Schema, Option<Schema>), DataFusionError> {
        let request_context = RequestContext::current(AsyncMarker::new().await);

        // Check if there's a Flight SQL session-specific context for session isolation
        let session = if let Some(flight_session) =
            request_context.extension::<super::flight_session_extension::FlightSessionExtension>()
        {
            flight_session.session_context().state()
        } else {
            self.df.ctx.state()
        };

        let plan = match self.sql {
            QueryMethod::Plan(ref plan)
            | QueryMethod::Text {
                pre_parsed_plan: Some(ref plan),
                ..
            } => plan.clone(),
            QueryMethod::Text { ref sql, .. } => {
                match self.df.create_logical_plan(&session, sql).await {
                    Ok(plan) => Box::new(plan),
                    Err(e) => {
                        let e = find_datafusion_root(e);
                        self.handle_schema_error(&request_context, &e);
                        return Err(e);
                    }
                }
            }
        };

        // Verify the plan against the restricted options
        if let Err(e) = validate_sql_query_operations(&plan, &self.df) {
            let e = find_datafusion_root(e);
            self.handle_schema_error(&request_context, &e);
            return Err(e);
        }
        if self.read_only
            && let Err(e) = validate_sql_query_read_only(&plan)
        {
            let e = find_datafusion_root(e);
            self.handle_schema_error(&request_context, &e);
            return Err(e);
        }
        let dataset_schema = plan.schema().as_arrow().clone();
        let parameter_schema = parameter_schema_for_plan(&plan)?;

        Ok((dataset_schema, parameter_schema))
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

fn parameter_schema_for_plan(plan: &LogicalPlan) -> Result<Option<Schema>, DataFusionError> {
    let mut parameters: Vec<(String, arrow_schema::DataType)> = plan
        .get_parameter_types()?
        .into_iter()
        .map(|(name, dt)| {
            // If cannot determine datatype, we are assuming UInt64.
            // This appears to occur for LIMIT parameters such as for:
            // ```sql
            // SELECT * FROM table LIMIT $1
            // ```
            // Other cases are not known
            (name, dt.unwrap_or(arrow_schema::DataType::UInt64))
        })
        .collect();

    // Sort parameters by their numeric value to ensure correct ordering
    // For example, $1, $2, ..., $9, $10, $11 instead of $1, $10, $11, $2, ...
    parameters.sort_by(|a, b| {
        let parse_param_num =
            |param_name: &str| -> Option<u32> { param_name.strip_prefix('$')?.parse().ok() };

        let a_num = parse_param_num(&a.0);
        let b_num = parse_param_num(&b.0);

        match (a_num, b_num) {
            (Some(a), Some(b)) => a.cmp(&b),
            (Some(_), None) => std::cmp::Ordering::Less, // numeric params come before non-numeric
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => a.0.cmp(&b.0), // fallback to lexicographic for non-numeric params
        }
    });

    let maybe_schema = if parameters.is_empty() {
        None
    } else {
        let mut builder = SchemaBuilder::new();
        for (name, typ) in parameters {
            builder.push(Field::new(name, typ, false));
        }
        Some(builder.finish())
    };

    Ok(maybe_schema)
}

#[must_use]
/// Attaches a query tracker to a stream of record batches.
///
/// Processes a stream of record batches, updating the query tracker
/// with the number of records/bytes returned and saving query details at the end.
///
/// Note: If an error occurs during stream processing, the query tracker
/// is finalized with error details, and further streaming is terminated.
fn attach_query_tracker_to_stream(
    span: Span,
    request_context: Arc<RequestContext>,
    tracker: Option<QueryTracker>,
    mut stream: SendableRecordBatchStream,
) -> SendableRecordBatchStream {
    let Some(tracker) = tracker else {
        return stream;
    };

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
        crate::metrics::telemetry::track_rows_returned(num_records, &request_context.to_dimensions());

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

/// This guard guarantees:
///  * If we incremented nested query count, we will decrement. And vice versa.
///  * If we incremented active query count, we will decrement. And vice versa.
///  * Active query count decrement will be called with the same dimensions as increment.
pub struct QueryActiveGuard {
    request_context: Arc<RequestContext>,
    dimensions: &'static [KeyValue],
    active: bool,
}

impl QueryActiveGuard {
    pub fn new(request_context: Arc<RequestContext>) -> Self {
        let dimensions = request_context.to_protocol_dimensions();

        let active = request_context.entered_top_level_query();
        if active {
            crate::metrics::telemetry::inc_query_active_count(dimensions);
        }

        Self {
            request_context,
            dimensions,
            active,
        }
    }
}

impl Drop for QueryActiveGuard {
    fn drop(&mut self) {
        let exited = self.request_context.exited_top_level_query();
        if self.active && exited {
            crate::metrics::telemetry::dec_query_active_count(self.dimensions);
        }
    }
}

fn attach_query_active_guard_to_stream(
    stream: SendableRecordBatchStream,
    request_context: &Arc<RequestContext>,
    span: Span,
) -> SendableRecordBatchStream {
    let schema = stream.schema();

    let guard = QueryActiveGuard::new(Arc::clone(request_context));

    let updated_stream =
        futures::stream::unfold((stream, guard), |(mut stream, guard)| async move {
            stream
                .next()
                .await
                .map(|batch_result| (batch_result, (stream, guard)))
        });

    Box::pin(RecordBatchStreamAdapter::new(
        schema,
        Box::pin(updated_stream.instrument(span)),
    ))
}

fn attach_cancellation_to_query_result<G>(
    query_result: QueryResult,
    cancellation_token: tokio_util::sync::CancellationToken,
    query_id: String,
    guard: G,
) -> QueryResult
where
    G: Send + 'static,
{
    let QueryResult { data, cache_status } = query_result;
    QueryResult::new(
        attach_cancellation_to_stream(data, cancellation_token, query_id, guard),
        cache_status,
    )
}

/// Wraps a record batch stream so that cancellation via the supplied
/// [`CancellationToken`] yields a single [`DataFusionError::External`] wrapping
/// [`Error::QueryCancelled`] and terminates the stream.
///
/// Using [`Error::QueryCancelled`] lets downstream callers (HTTP status
/// mapping, Flight status mapping, metrics) distinguish cancellation from
/// other query failures via [`is_cancellation_error`] or an
/// [`std::error::Error::downcast_ref`] on the external error source.
///
/// The wrapper also keeps ownership of any `guard` (typically an
/// [`ActiveQueryGuard`]) so that the query's registry entry is removed when the
/// stream is dropped, whether it completes, errors, or is cancelled.
fn attach_cancellation_to_stream<G>(
    stream: SendableRecordBatchStream,
    cancellation_token: tokio_util::sync::CancellationToken,
    query_id: String,
    guard: G,
) -> SendableRecordBatchStream
where
    G: Send + 'static,
{
    struct State<G> {
        stream: Option<SendableRecordBatchStream>,
        token: tokio_util::sync::CancellationToken,
        query_id: String,
        guard: Option<G>,
        emitted_cancel: bool,
    }

    impl<G> State<G> {
        fn release_query_resources(&mut self) {
            self.stream.take();
            self.guard.take();
        }
    }

    fn cancellation_error(query_id: &str) -> DataFusionError {
        DataFusionError::External(Box::new(Error::QueryCancelled {
            query_id: query_id.to_string(),
        }))
    }

    let schema = stream.schema();

    let state = State {
        stream: Some(stream),
        token: cancellation_token,
        query_id,
        guard: Some(guard),
        emitted_cancel: false,
    };

    let wrapped = futures::stream::unfold(state, |mut state| async move {
        if state.emitted_cancel {
            return None;
        }
        if state.token.is_cancelled() {
            state.emitted_cancel = true;
            state.release_query_resources();
            return Some((Err(cancellation_error(&state.query_id)), state));
        }
        let token = state.token.clone();
        let mut stream = state.stream.take()?;
        tokio::select! {
            biased;
            () = token.cancelled() => {
                state.emitted_cancel = true;
                state.release_query_resources();
                Some((Err(cancellation_error(&state.query_id)), state))
            }
            next = stream.next() => {
                state.stream = Some(stream);
                next.map(|item| (item, state))
            }
        }
    });

    Box::pin(RecordBatchStreamAdapter::new(schema, Box::pin(wrapped)))
}

/// Returns true if `err` represents a query cancellation produced by
/// [`attach_cancellation_to_stream`].
#[must_use]
pub fn is_cancellation_error(err: &DataFusionError) -> bool {
    let DataFusionError::External(source) = err else {
        return false;
    };
    source
        .downcast_ref::<Error>()
        .is_some_and(|e| matches!(e, Error::QueryCancelled { .. }))
}

#[must_use]
/// Attaches logic to a stream which emits metrics from a physical plan.
fn attach_physical_plan_metrics_to_stream(
    mut stream: SendableRecordBatchStream,
    physical_plan: Arc<dyn ExecutionPlan>,
    request_context: Arc<RequestContext>,
    span: Span,
) -> SendableRecordBatchStream {
    let schema = stream.schema();

    let updated_stream = stream! {
        while let Some(batch_result) = stream.next().await {
            yield batch_result;
        }

        let mut totals = PhysicalPlanMetricsTotals::default();
        collect_physical_plan_metrics(physical_plan.as_ref(), &mut totals);

        crate::metrics::telemetry::track_produced_spills(totals.produced_spills, &request_context.to_dimensions());
        crate::metrics::telemetry::track_spilled_bytes(totals.spilled_bytes, &request_context.to_dimensions());
        crate::metrics::telemetry::track_spilled_rows(totals.spilled_rows, &request_context.to_dimensions());
    };

    Box::pin(RecordBatchStreamAdapter::new(
        schema,
        Box::pin(updated_stream.instrument(span)),
    ))
}

#[derive(Default, Debug)]
/// Used to collect aggregated metrics from a physical plan.
struct PhysicalPlanMetricsTotals {
    pub produced_spills: u64,
    pub spilled_bytes: u64,
    pub spilled_rows: u64,
}

fn collect_physical_plan_metrics(plan: &dyn ExecutionPlan, totals: &mut PhysicalPlanMetricsTotals) {
    if let Some(metrics) = plan.metrics() {
        totals.produced_spills += metrics.spill_count().unwrap_or_default() as u64;
        totals.spilled_bytes += metrics.spilled_bytes().unwrap_or_default() as u64;
        totals.spilled_rows += metrics.spilled_rows().unwrap_or_default() as u64;
    }

    for child in plan.children() {
        collect_physical_plan_metrics(child.as_ref(), totals);
    }
}

fn execute_stream_preserving_output_order(
    plan: Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
) -> DataFusionResult<SendableRecordBatchStream> {
    let plan = prepare_physical_plan_for_sync_results(plan)?;
    execute_stream(plan, context)
}

fn prepare_physical_plan_for_sync_results(
    plan: Arc<dyn ExecutionPlan>,
) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
    let plan = strip_root_order_preserving_repartition(plan)?;

    if plan.output_partitioning().partition_count() > 1
        && let Some(ordering) = plan.output_ordering().cloned()
    {
        // `execute_stream()` coalesces multi-partition output with
        // `CoalescePartitionsExec`, which does not preserve global ordering.
        // For synchronous APIs (/v1/sql, FlightSQL), preserve SQL ORDER BY
        // semantics by collapsing ordered multi-partition output with an
        // explicit sort-preserving merge first.
        return Ok(Arc::new(
            SortPreservingMergeExec::new(ordering, plan).with_round_robin_repartition(false),
        ));
    }

    Ok(plan)
}

fn strip_root_order_preserving_repartition(
    plan: Arc<dyn ExecutionPlan>,
) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
    let children = plan.children();
    if children.len() != 1 {
        return Ok(plan);
    }

    let child = Arc::clone(children[0]);
    let rewritten_child = strip_root_order_preserving_repartition(child)?;
    let plan = if Arc::ptr_eq(children[0], &rewritten_child) {
        plan
    } else {
        plan.with_new_children(vec![rewritten_child])?
    };

    if let Some(spm) = plan.as_any().downcast_ref::<SortPreservingMergeExec>() {
        return Ok(Arc::new(
            SortPreservingMergeExec::new(spm.expr().clone(), Arc::clone(spm.input()))
                .with_fetch(spm.fetch())
                .with_round_robin_repartition(false),
        ));
    }

    if let Some(repartition) = plan.as_any().downcast_ref::<RepartitionExec>()
        && repartition.input().output_partitioning().partition_count() == 1
        && repartition.input().output_ordering().is_some()
        && repartition.partitioning().partition_count() > 1
    {
        // The synchronous query APIs consume a single stream. Repartitioning a
        // single already-sorted stream back out to multiple output partitions at
        // the root only makes `execute_stream()` coalesce it again later,
        // destroying row order for ORDER BY queries.
        return Ok(Arc::clone(repartition.input()));
    }

    Ok(plan)
}

pub fn write_to_json_string(
    data: &[RecordBatch],
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    if data.iter().any(record_batch_has_union_columns) {
        serde_json::to_string(&write_union_batches_to_json_value(data)?).boxed()
    } else {
        String::from_utf8(write_to_json_bytes_with_arrow(data)?).boxed()
    }
}

pub fn write_to_json_value(
    data: &[RecordBatch],
) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
    if data.iter().any(record_batch_has_union_columns) {
        write_union_batches_to_json_value(data)
    } else {
        write_to_json_value_with_arrow(data)
    }
}

fn write_union_batches_to_json_value(
    data: &[RecordBatch],
) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
    let rows = data
        .iter()
        .map(record_batch_to_json_rows)
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .flatten()
        .map(Value::Object)
        .collect();
    Ok(Value::Array(rows))
}

fn write_to_json_value_with_arrow(
    data: &[RecordBatch],
) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
    serde_json::from_slice(write_to_json_bytes_with_arrow(data)?.as_slice()).boxed()
}

fn write_to_json_bytes_with_arrow(
    data: &[RecordBatch],
) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    let buf = Vec::new();
    let mut writer = arrow_json::WriterBuilder::new()
        .with_explicit_nulls(true)
        .build::<_, JsonArray>(buf);

    writer.write_batches(data.iter().collect::<Vec<&RecordBatch>>().as_slice())?;
    writer.finish()?;

    Ok(writer.into_inner())
}

fn record_batch_has_union_columns(batch: &RecordBatch) -> bool {
    batch
        .schema()
        .fields()
        .iter()
        .any(|field| data_type_contains_union(field.data_type()))
}

fn data_type_contains_union(data_type: &arrow::datatypes::DataType) -> bool {
    match data_type {
        arrow::datatypes::DataType::Union(_, _) => true,
        arrow::datatypes::DataType::List(field)
        | arrow::datatypes::DataType::LargeList(field)
        | arrow::datatypes::DataType::FixedSizeList(field, _)
        | arrow::datatypes::DataType::ListView(field)
        | arrow::datatypes::DataType::LargeListView(field)
        | arrow::datatypes::DataType::Map(field, _) => data_type_contains_union(field.data_type()),
        arrow::datatypes::DataType::Struct(fields) => fields
            .iter()
            .any(|field| data_type_contains_union(field.data_type())),
        arrow::datatypes::DataType::Dictionary(_, value_type) => {
            data_type_contains_union(value_type)
        }
        arrow::datatypes::DataType::RunEndEncoded(_, value_field) => {
            data_type_contains_union(value_field.data_type())
        }
        _ => false,
    }
}

fn record_batch_to_json_rows(
    batch: &RecordBatch,
) -> Result<Vec<Map<String, Value>>, Box<dyn std::error::Error + Send + Sync>> {
    let custom_indices: Vec<usize> = batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(index, field)| data_type_contains_union(field.data_type()).then_some(index))
        .collect();

    if custom_indices.is_empty() {
        return json_value_to_rows(write_to_json_value_with_arrow(std::slice::from_ref(batch))?);
    }

    let mut is_custom_index = vec![false; batch.num_columns()];
    for &custom_index in &custom_indices {
        is_custom_index[custom_index] = true;
    }

    let non_union_indices: Vec<usize> = (0..batch.num_columns())
        .filter(|index| !is_custom_index[*index])
        .collect();

    let mut rows = if non_union_indices.is_empty() {
        vec![Map::new(); batch.num_rows()]
    } else {
        let projected_batch = batch.project(&non_union_indices)?;
        json_value_to_rows(write_to_json_value_with_arrow(std::slice::from_ref(
            &projected_batch,
        ))?)?
    };

    for custom_index in custom_indices {
        let column_values = column_to_json_values(batch.column(custom_index).as_ref())?;
        let field_name = batch.schema().field(custom_index).name().clone();

        for (row, value) in rows.iter_mut().zip(column_values) {
            row.insert(field_name.clone(), value);
        }
    }

    Ok(rows)
}

fn column_to_json_values(
    array: &dyn Array,
) -> Result<Vec<Value>, Box<dyn std::error::Error + Send + Sync>> {
    if matches!(array.data_type(), arrow::datatypes::DataType::Union(_, _)) {
        return union_array_to_json_values(array);
    }

    (0..array.len())
        .map(|index| array_value_to_json(array, index))
        .collect()
}

fn json_value_to_rows(
    value: Value,
) -> Result<Vec<Map<String, Value>>, Box<dyn std::error::Error + Send + Sync>> {
    let Value::Array(rows) = value else {
        return Err("Expected JSON array of rows".into());
    };

    rows.into_iter()
        .map(|row| match row {
            Value::Object(map) => Ok(map),
            _ => Err("Expected each JSON row to be an object".into()),
        })
        .collect()
}

fn union_array_to_json_values(
    array: &dyn Array,
) -> Result<Vec<Value>, Box<dyn std::error::Error + Send + Sync>> {
    let union_array = array
        .as_any()
        .downcast_ref::<UnionArray>()
        .ok_or_else(|| "Expected UnionArray for Union-typed column".to_string())?;

    if let Some(encoder) = JsonUnionEncoder::from_union(union_array.clone()) {
        return (0..encoder.len())
            .map(|index| {
                let value = encoder.get_value(index);
                json_union_value_to_json(&value)
            })
            .collect();
    }

    (0..union_array.len())
        .map(|index| scalar_to_json_value(&ScalarValue::try_from_array(union_array, index)?))
        .collect()
}

fn json_union_value_to_json(
    value: &JsonUnionValue<'_>,
) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
    match value {
        JsonUnionValue::JsonNull => Ok(Value::Null),
        JsonUnionValue::Bool(value) => Ok(Value::Bool(*value)),
        JsonUnionValue::Int(value) => Ok(Value::Number(Number::from(*value))),
        JsonUnionValue::Float(value) => number_to_json(*value),
        JsonUnionValue::Str(value) => Ok(Value::String((*value).to_owned())),
        JsonUnionValue::Array(value) | JsonUnionValue::Object(value) => {
            serde_json::from_str(value).boxed()
        }
    }
}

fn array_value_to_json(
    array: &dyn Array,
    index: usize,
) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
    if let Some(union_array) = array.as_any().downcast_ref::<UnionArray>()
        && let Some(encoder) = JsonUnionEncoder::from_union(union_array.clone())
    {
        let value = encoder.get_value(index);
        return json_union_value_to_json(&value);
    }

    scalar_to_json_value(&ScalarValue::try_from_array(array, index)?)
}

fn scalar_to_json_value(
    value: &ScalarValue,
) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
    match value {
        ScalarValue::Boolean(value) => Ok(value.map(Value::Bool).unwrap_or(Value::Null)),
        ScalarValue::Float16(Some(value)) => number_to_json(f64::from(f32::from(*value))),
        ScalarValue::Float32(Some(value)) => number_to_json(f64::from(*value)),
        ScalarValue::Float64(Some(value)) => number_to_json(*value),
        ScalarValue::Int8(Some(value)) => Ok(Value::Number(Number::from(*value))),
        ScalarValue::Int16(Some(value)) => Ok(Value::Number(Number::from(*value))),
        ScalarValue::Int32(Some(value)) => Ok(Value::Number(Number::from(*value))),
        ScalarValue::Int64(Some(value)) => Ok(Value::Number(Number::from(*value))),
        ScalarValue::UInt8(Some(value)) => Ok(Value::Number(Number::from(*value))),
        ScalarValue::UInt16(Some(value)) => Ok(Value::Number(Number::from(*value))),
        ScalarValue::UInt32(Some(value)) => Ok(Value::Number(Number::from(*value))),
        ScalarValue::UInt64(Some(value)) => Ok(Value::Number(Number::from(*value))),
        ScalarValue::Utf8(Some(value))
        | ScalarValue::Utf8View(Some(value))
        | ScalarValue::LargeUtf8(Some(value)) => Ok(Value::String(value.clone())),
        ScalarValue::Binary(Some(value))
        | ScalarValue::BinaryView(Some(value))
        | ScalarValue::LargeBinary(Some(value))
        | ScalarValue::FixedSizeBinary(_, Some(value)) => {
            Ok(Value::String(bytes_to_hex(value.as_slice())))
        }
        ScalarValue::FixedSizeList(array) => single_row_fixed_size_list_to_json(array),
        ScalarValue::List(array) => single_row_list_to_json(array),
        ScalarValue::LargeList(array) => single_row_large_list_to_json(array),
        ScalarValue::Struct(array) => single_row_struct_to_json(array),
        ScalarValue::Map(array) => single_row_map_to_json(array),
        ScalarValue::Union(Some((_type_id, value)), _, _) => scalar_to_json_value(value),
        ScalarValue::Dictionary(_, value) => scalar_to_json_value(value),
        ScalarValue::RunEndEncoded(_, _, value) => scalar_to_json_value(value),
        ScalarValue::Null
        | ScalarValue::Float16(None)
        | ScalarValue::Float32(None)
        | ScalarValue::Float64(None)
        | ScalarValue::Int8(None)
        | ScalarValue::Int16(None)
        | ScalarValue::Int32(None)
        | ScalarValue::Int64(None)
        | ScalarValue::UInt8(None)
        | ScalarValue::UInt16(None)
        | ScalarValue::UInt32(None)
        | ScalarValue::UInt64(None)
        | ScalarValue::Utf8(None)
        | ScalarValue::Utf8View(None)
        | ScalarValue::LargeUtf8(None)
        | ScalarValue::Binary(None)
        | ScalarValue::BinaryView(None)
        | ScalarValue::LargeBinary(None)
        | ScalarValue::FixedSizeBinary(_, None)
        | ScalarValue::Union(None, _, _) => Ok(Value::Null),
        ScalarValue::Decimal32(..)
        | ScalarValue::Decimal64(..)
        | ScalarValue::Decimal128(..)
        | ScalarValue::Decimal256(..)
        | ScalarValue::Date32(..)
        | ScalarValue::Date64(..)
        | ScalarValue::Time32Second(..)
        | ScalarValue::Time32Millisecond(..)
        | ScalarValue::Time64Microsecond(..)
        | ScalarValue::Time64Nanosecond(..)
        | ScalarValue::TimestampSecond(..)
        | ScalarValue::TimestampMillisecond(..)
        | ScalarValue::TimestampMicrosecond(..)
        | ScalarValue::TimestampNanosecond(..)
        | ScalarValue::IntervalYearMonth(..)
        | ScalarValue::IntervalDayTime(..)
        | ScalarValue::IntervalMonthDayNano(..)
        | ScalarValue::DurationSecond(..)
        | ScalarValue::DurationMillisecond(..)
        | ScalarValue::DurationMicrosecond(..)
        | ScalarValue::DurationNanosecond(..) => Ok(Value::String(value.to_string())),
    }
}

fn single_row_list_to_json(
    array: &arrow::array::ListArray,
) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
    single_row_nested_array_to_json(array, 0)
}

fn single_row_large_list_to_json(
    array: &LargeListArray,
) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
    single_row_nested_array_to_json(array, 0)
}

fn single_row_fixed_size_list_to_json(
    array: &FixedSizeListArray,
) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
    single_row_nested_array_to_json(array, 0)
}

fn single_row_nested_array_to_json(
    array: &dyn Array,
    index: usize,
) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
    if !array.is_valid(index) {
        return Ok(Value::Null);
    }

    let values = if let Some(list_array) = array.as_any().downcast_ref::<arrow::array::ListArray>()
    {
        list_array.value(index)
    } else if let Some(list_array) = array.as_any().downcast_ref::<LargeListArray>() {
        list_array.value(index)
    } else if let Some(list_array) = array.as_any().downcast_ref::<FixedSizeListArray>() {
        list_array.value(index)
    } else {
        return Err("Expected a list-like Arrow array".into());
    };

    let items = (0..values.len())
        .map(|value_index| array_value_to_json(values.as_ref(), value_index))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Value::Array(items))
}

fn single_row_struct_to_json(
    array: &StructArray,
) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
    if !array.is_valid(0) {
        return Ok(Value::Null);
    }

    let mut object = Map::with_capacity(array.num_columns());
    for (field, column) in array.fields().iter().zip(array.columns()) {
        object.insert(
            field.name().clone(),
            array_value_to_json(column.as_ref(), 0)?,
        );
    }

    Ok(Value::Object(object))
}

fn single_row_map_to_json(
    array: &MapArray,
) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
    if !array.is_valid(0) {
        return Ok(Value::Null);
    }

    let entries = array.value(0);
    let keys = entries.column(0);
    let values = entries.column(1);

    let mut object = Map::with_capacity(entries.len());
    for index in 0..entries.len() {
        let key = scalar_to_json_key(&ScalarValue::try_from_array(keys.as_ref(), index)?)?;
        object.insert(key, array_value_to_json(values.as_ref(), index)?);
    }

    Ok(Value::Object(object))
}

fn scalar_to_json_key(
    value: &ScalarValue,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    match scalar_to_json_value(value)? {
        Value::String(value) => Ok(value),
        Value::Null => Ok("null".to_string()),
        Value::Bool(value) => Ok(value.to_string()),
        Value::Number(value) => Ok(value.to_string()),
        other => Ok(other.to_string()),
    }
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(&mut output, "{byte:02x}");
    }
    output
}

fn number_to_json(value: f64) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
    Number::from_f64(value)
        .map(Value::Number)
        .ok_or_else(|| format!("Unable to represent floating point value {value} as JSON").into())
}

/// Reconciles the nullability of a result stream with the logical plan schema.
///
/// Physical optimizer rules (e.g. `AggregateStatistics`) may replace aggregate
/// execution plans with literal projections whose nullability differs from the
/// logical plan.  For example, `MAX(int64)` is logically nullable (empty input
/// → NULL) but the optimized literal `ScalarValue::Int64(Some(v))` is
/// non-nullable.
///
/// This function widens non-nullable fields to nullable when the logical plan
/// schema says they should be nullable, and wraps the stream so every batch
/// conforms.  This uses the same `try_cast_to` mechanism as `SchemaCastScanExec`.
fn reconcile_stream_nullability(
    stream: SendableRecordBatchStream,
    plan_schema: &Arc<Schema>,
) -> SendableRecordBatchStream {
    let exec_schema = stream.schema();

    if exec_schema.fields().len() != plan_schema.fields().len() {
        tracing::warn!(
            "Schema field count mismatch during nullability reconciliation: \
             execution schema has {} fields, logical plan has {}",
            exec_schema.fields().len(),
            plan_schema.fields().len(),
        );
        return stream;
    }

    let mut needs_reconciliation = false;
    for (exec_field, plan_field) in exec_schema.fields().iter().zip(plan_schema.fields()) {
        if plan_field.is_nullable() && !exec_field.is_nullable() {
            needs_reconciliation = true;
            break;
        }
    }

    if !needs_reconciliation {
        return stream;
    }

    // Build a reconciled schema: widen to nullable where the logical plan says
    // so, but keep data types and metadata from the execution schema.
    let reconciled_fields: Vec<Field> = exec_schema
        .fields()
        .iter()
        .zip(plan_schema.fields())
        .map(|(exec_field, plan_field)| {
            if plan_field.is_nullable() && !exec_field.is_nullable() {
                exec_field.as_ref().clone().with_nullable(true)
            } else {
                exec_field.as_ref().clone()
            }
        })
        .collect();

    let reconciled =
        Arc::new(Schema::new(reconciled_fields).with_metadata(exec_schema.metadata().clone()));
    let target = Arc::clone(&reconciled);

    Box::pin(RecordBatchStreamAdapter::new(
        reconciled,
        stream.map(move |batch| {
            batch.and_then(|b| {
                arrow_tools::record_batch::try_cast_to(b, Arc::clone(&target)).map_err(Into::into)
            })
        }),
    ))
}

/// Extract the target table reference from a DML logical plan.
///
/// Handles both standard `DataFusion` `LogicalPlan::Dml` nodes and generic
/// `datafusion_dml::DmlExtensionNode` values.
fn extract_dml_target_table(plan: &LogicalPlan) -> Option<TableReference> {
    match plan {
        LogicalPlan::Dml(dml) => Some(dml.table_name.clone()),
        LogicalPlan::Extension(ext) => {
            let dml = ext
                .node
                .as_any()
                .downcast_ref::<datafusion_dml::DmlExtensionNode>()?;

            Some(match &dml.op {
                datafusion_dml::DmlNodeOp::Delete(params) => params.table_name.clone(),
                datafusion_dml::DmlNodeOp::Update(params) => params.table_name.clone(),
                datafusion_dml::DmlNodeOp::Insert(params) => params.table_name.clone(),
                datafusion_dml::DmlNodeOp::Merge(params) => params.target_table.clone(),
            })
        }
        _ => None,
    }
}

/// Returns `true` if the plan is a DML extension node.
///
/// Used to skip schema verification for DML extension nodes, whose output
/// schema may differ from the logical plan's schema.
fn is_dml_extension(plan: &LogicalPlan) -> bool {
    matches!(
        plan,
        LogicalPlan::Extension(ext)
            if ext
                .node
                .as_any()
                .downcast_ref::<datafusion_dml::DmlExtensionNode>()
                .is_some()
    )
}

#[cfg(test)]
mod tests {
    use ::cache::{Caching, QueryResultsCacheProvider, result::CacheStatus};
    use arrow::{
        array::{
            ArrayRef, BooleanArray, Int64Array, NullArray, RecordBatch, StringArray, StructArray,
            UnionArray,
        },
        buffer::Buffer,
        datatypes::{DataType, Field, Schema, UnionMode},
    };
    use datafusion::logical_expr::Extension;
    use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
    use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
    use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder, MetricsSet};
    use datafusion::physical_plan::{DisplayAs, DisplayFormatType, PlanProperties};
    use datafusion_functions_json::JSON_UNION_DATA_TYPE;
    use serde_json::json;
    use spicepod::component::caching::SQLResultsCacheConfig;
    use std::any::Any;
    use std::fmt::{Debug, Formatter};
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio_util::sync::CancellationToken;

    use crate::{
        dataaccelerator::AcceleratorEngineRegistry,
        datafusion::{builder::DataFusionBuilder, param_utils::convert_json_to_param_values},
        status::RuntimeStatus,
    };

    use super::*;

    #[derive(Debug)]
    struct NoopDmlHandler;

    #[async_trait::async_trait]
    impl datafusion_dml::CatalogDmlHandler for NoopDmlHandler {
        fn name(&self) -> &'static str {
            "test"
        }
    }

    #[test]
    fn test_extract_dml_target_table_from_generic_delete_extension() {
        let plan = LogicalPlan::Extension(Extension {
            node: Arc::new(datafusion_dml::DmlExtensionNode::new_with_count_output(
                datafusion_dml::DmlNodeOp::Delete(datafusion_dml::DeleteParams {
                    table_name: TableReference::parse_str("catalog.schema.target"),
                    filters: vec![],
                }),
                Arc::new(NoopDmlHandler),
                vec![],
            )),
        });

        let target = extract_dml_target_table(&plan).expect("should find DML target");
        assert_eq!(target.to_string(), "catalog.schema.target");
        assert!(is_dml_extension(&plan));
    }

    #[test]
    fn test_extract_dml_target_table_from_generic_merge_extension() {
        let plan = LogicalPlan::Extension(Extension {
            node: Arc::new(datafusion_dml::DmlExtensionNode::new_with_count_output(
                datafusion_dml::DmlNodeOp::Merge(Box::new(datafusion_dml::MergeParams {
                    target_table: TableReference::parse_str("catalog.schema.target"),
                    source_table: TableReference::parse_str("catalog.schema.source"),
                    target_qualifier: "t".to_string(),
                    source_qualifier: "s".to_string(),
                    on_keys: vec![("id".to_string(), "id".to_string())],
                    assignments: vec![],
                    original_sql: None,
                })),
                Arc::new(NoopDmlHandler),
                vec![],
            )),
        });

        let target = extract_dml_target_table(&plan).expect("should find MERGE target");
        assert_eq!(target.to_string(), "catalog.schema.target");
        assert!(is_dml_extension(&plan));
    }

    #[tokio::test]
    async fn parameterized_query() {
        let parameters = convert_json_to_param_values(json!([41])).expect("json to paramvalues");
        let config = SQLResultsCacheConfig::default();
        let cache_provider = Arc::new(
            QueryResultsCacheProvider::try_new(&config, Box::new([])).expect("cache provider new"),
        );
        let df = Arc::new(
            DataFusionBuilder::new(
                RuntimeStatus::new(),
                Arc::new(AcceleratorEngineRegistry::new()),
                Handle::current(),
            )
            .with_caching(Arc::new(Caching::new().with_results_cache(cache_provider)))
            .build(),
        );

        let mut query = QueryBuilder::new("SELECT $1 + 1 AS the_answer", Arc::clone(&df))
            .parameters(Some(parameters.clone()))
            .build()
            .run()
            .await
            .expect("Query::run");

        // Need to consume the stream to cache the result
        while let Some(Ok(batch)) = query.data.next().await {
            let column = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("value");
            let id_value = column.value(0);
            assert_eq!(id_value, 42);
        }

        assert_eq!(query.cache_status, CacheStatus::CacheMiss);

        let mut query = QueryBuilder::new("SELECT $1 + 1 AS the_answer", Arc::clone(&df))
            .parameters(Some(parameters))
            .build()
            .run()
            .await
            .expect("Query::run");
        assert_eq!(query.cache_status, CacheStatus::CacheHit);

        // Need to consume the stream to cache the result
        while let Some(Ok(batch)) = query.data.next().await {
            let column = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("value");
            let id_value = column.value(0);
            assert_eq!(id_value, 42);
        }

        // New parameters should not be cached
        let parameters = convert_json_to_param_values(json!([1])).expect("json to paramvalues");
        let mut query = QueryBuilder::new("SELECT $1 + 1 AS the_answer", df)
            .parameters(Some(parameters))
            .build()
            .run()
            .await
            .expect("Query::run");

        // Need to consume the stream to cache the result
        while let Some(Ok(batch)) = query.data.next().await {
            let column = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("value");
            let id_value = column.value(0);
            assert_eq!(id_value, 2);
        }

        assert_eq!(query.cache_status, CacheStatus::CacheMiss);
    }

    #[tokio::test]
    async fn cached_query_result_keeps_registry_entry_until_stream_drop_and_observes_cancel() {
        let config = SQLResultsCacheConfig::default();
        let cache_provider = Arc::new(
            QueryResultsCacheProvider::try_new(&config, Box::new([])).expect("cache provider new"),
        );
        let df = Arc::new(
            DataFusionBuilder::new(
                RuntimeStatus::new(),
                Arc::new(AcceleratorEngineRegistry::new()),
                Handle::current(),
            )
            .with_caching(Arc::new(Caching::new().with_results_cache(cache_provider)))
            .build(),
        );

        {
            let mut query = QueryBuilder::new("SELECT 42 AS value", Arc::clone(&df))
                .build()
                .run()
                .await
                .expect("initial query should run");
            assert_eq!(query.cache_status, CacheStatus::CacheMiss);
            while let Some(batch_result) = query.data.next().await {
                batch_result.expect("initial query stream should succeed");
            }
        }

        let query_id = uuid::Uuid::new_v4();
        let cancel_token = CancellationToken::new();
        let mut cached_query = QueryBuilder::new("SELECT 42 AS value", Arc::clone(&df))
            .query_id(query_id)
            .cancellation_token(cancel_token.clone())
            .build()
            .run()
            .await
            .expect("cached query should run");

        assert_eq!(cached_query.cache_status, CacheStatus::CacheHit);
        let registry = df.query_cancel_registry();
        assert!(
            registry.list().iter().any(|info| info.query_id == query_id),
            "cached query should remain registered while its stream is alive"
        );

        assert!(registry.cancel(query_id));
        assert!(cancel_token.is_cancelled());
        let cancellation = cached_query
            .data
            .next()
            .await
            .expect("cached stream should emit cancellation");
        assert_query_cancelled(
            cancellation.expect_err("cached stream item should be a cancellation error"),
            &query_id.to_string(),
        );
        assert!(cached_query.data.next().await.is_none());
        assert!(
            registry.list().iter().all(|info| info.query_id != query_id),
            "cached query should be deregistered after the stream terminates"
        );
    }

    #[tokio::test]
    async fn test_parameter_schema_ordering_basic() {
        use datafusion::execution::context::SessionContext;

        let ctx = SessionContext::new();

        // Test basic parameter ordering with small numbers
        let sql = "SELECT $1, $2, $3";
        let plan = ctx
            .state()
            .create_logical_plan(sql)
            .await
            .expect("create plan");
        let schema = parameter_schema_for_plan(&plan).expect("parameter schema");

        let schema = schema.expect("should have parameters");
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();

        assert_eq!(field_names, vec!["$1", "$2", "$3"]);
    }

    #[tokio::test]
    async fn test_parameter_schema_ordering_with_double_digits() {
        use datafusion::execution::context::SessionContext;

        let ctx = SessionContext::new();

        // Test parameter ordering with more than 10 parameters
        let sql = "SELECT $1, $10, $11, $12, $2, $3, $4, $5, $6, $7, $8, $9";
        let plan = ctx
            .state()
            .create_logical_plan(sql)
            .await
            .expect("create plan");
        let schema = parameter_schema_for_plan(&plan).expect("parameter schema");

        let schema = schema.expect("should have parameters");
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();

        // Should be sorted numerically, not lexicographically
        assert_eq!(
            field_names,
            vec![
                "$1", "$2", "$3", "$4", "$5", "$6", "$7", "$8", "$9", "$10", "$11", "$12"
            ]
        );
    }

    #[tokio::test]
    async fn test_parameter_schema_ordering_large_numbers() {
        use datafusion::execution::context::SessionContext;

        let ctx = SessionContext::new();

        // Test with larger parameter numbers to ensure numeric sorting works correctly
        let sql = "SELECT $1, $100, $11, $2, $20, $21, $3";
        let plan = ctx
            .state()
            .create_logical_plan(sql)
            .await
            .expect("create plan");
        let schema = parameter_schema_for_plan(&plan).expect("parameter schema");

        let schema = schema.expect("should have parameters");
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();

        assert_eq!(
            field_names,
            vec!["$1", "$2", "$3", "$11", "$20", "$21", "$100"]
        );
    }

    #[tokio::test]
    async fn test_parameter_schema_ordering_mixed_types() {
        use datafusion::execution::context::SessionContext;

        let ctx = SessionContext::new();

        // Test with different parameter types in different positions
        let sql = "SELECT $1::text, $10::int, $2::float";
        let plan = ctx
            .state()
            .create_logical_plan(sql)
            .await
            .expect("create plan");
        let schema = parameter_schema_for_plan(&plan).expect("parameter schema");

        let schema = schema.expect("should have parameters");
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();

        // Should still be ordered numerically regardless of types
        assert_eq!(field_names, vec!["$1", "$2", "$10"]);
    }

    #[tokio::test]
    async fn test_parameter_schema_empty() {
        use datafusion::execution::context::SessionContext;

        let ctx = SessionContext::new();

        // Test with no parameters
        let sql = "SELECT 1, 2, 3";
        let plan = ctx
            .state()
            .create_logical_plan(sql)
            .await
            .expect("create plan");
        let schema = parameter_schema_for_plan(&plan).expect("parameter schema");

        assert!(schema.is_none(), "should have no parameter schema");
    }

    #[tokio::test]
    async fn test_parameter_schema_ordering_with_limit() {
        use datafusion::execution::context::SessionContext;

        let ctx = SessionContext::new();

        // Test parameter ordering when parameters are used in LIMIT clause
        let sql = "SELECT $1, $2 LIMIT $3";
        let plan = ctx
            .state()
            .create_logical_plan(sql)
            .await
            .expect("create plan");
        let schema = parameter_schema_for_plan(&plan).expect("parameter schema");

        let schema = schema.expect("should have parameters");
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();

        assert_eq!(field_names, vec!["$1", "$2", "$3"]);

        // Check that $3 exists (the type may vary based on DataFusion's inference)
        let limit_field = schema.field_with_name("$3").expect("$3 field should exist");
        // The actual type may be Int64 or UInt64 depending on DataFusion's type inference
        assert!(
            limit_field.data_type() == &arrow_schema::DataType::UInt64
                || limit_field.data_type() == &arrow_schema::DataType::Int64,
            "Expected UInt64 or Int64, got {:?}",
            limit_field.data_type()
        );
    }

    #[tokio::test]
    async fn test_parameter_schema_ordering_non_standard_names() {
        use std::collections::HashMap;

        // Test edge case with non-standard parameter names

        let mut param_types = HashMap::new();
        param_types.insert("$1".to_string(), Some(arrow_schema::DataType::Int64));
        param_types.insert("$10".to_string(), Some(arrow_schema::DataType::Utf8));
        param_types.insert(
            "non_numeric_param".to_string(),
            Some(arrow_schema::DataType::Boolean),
        );
        param_types.insert("$2".to_string(), Some(arrow_schema::DataType::Float64));
        param_types.insert(
            "another_param".to_string(),
            Some(arrow_schema::DataType::Int32),
        );

        // Manually set parameter types for testing - we need to create a plan that would have these parameters
        // For testing purposes, we'll just test the sorting logic directly
        let mut parameters: Vec<(String, arrow_schema::DataType)> = param_types
            .into_iter()
            .map(|(name, dt)| (name, dt.unwrap_or(arrow_schema::DataType::UInt64)))
            .collect();

        // Apply the same sorting logic as in parameter_schema_for_plan
        parameters.sort_by(|a, b| {
            let parse_param_num =
                |param_name: &str| -> Option<u32> { param_name.strip_prefix('$')?.parse().ok() };

            let a_num = parse_param_num(&a.0);
            let b_num = parse_param_num(&b.0);

            match (a_num, b_num) {
                (Some(a), Some(b)) => a.cmp(&b),
                (Some(_), None) => std::cmp::Ordering::Less, // numeric params come before non-numeric
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (None, None) => a.0.cmp(&b.0), // fallback to lexicographic for non-numeric params
            }
        });

        let param_names: Vec<&str> = parameters.iter().map(|(name, _)| name.as_str()).collect();

        // Numeric parameters should come first, sorted numerically
        // Then non-numeric parameters sorted lexicographically
        assert_eq!(
            param_names,
            vec!["$1", "$2", "$10", "another_param", "non_numeric_param"]
        );
    }

    #[test]
    fn test_write_to_json_value_keeps_null_fields() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("last_modified_by", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1_i64])) as ArrayRef,
                Arc::new(StringArray::from(vec![Option::<&str>::None])) as ArrayRef,
            ],
        )
        .expect("to create record batch");

        let value = write_to_json_value(&[batch]).expect("to serialize JSON");

        assert_eq!(
            value,
            json!([
                {
                    "id": 1,
                    "last_modified_by": null
                }
            ])
        );
    }

    #[test]
    fn test_write_to_json_value_handles_json_union_fields() {
        let DataType::Union(fields, UnionMode::Sparse) = &*JSON_UNION_DATA_TYPE else {
            panic!("JSON union data type should be a sparse union");
        };

        let union_array = UnionArray::try_new(
            fields.clone(),
            Buffer::from_vec(vec![6_i8, 4_i8, 0_i8]).into(),
            None,
            vec![
                Arc::new(NullArray::new(3)) as ArrayRef,
                Arc::new(BooleanArray::from(vec![None, None, None])) as ArrayRef,
                Arc::new(Int64Array::from(vec![None, None, None])) as ArrayRef,
                Arc::new(arrow::array::Float64Array::from(vec![None, None, None])) as ArrayRef,
                Arc::new(StringArray::from(vec![
                    Option::<&str>::None,
                    Some("draft"),
                    Option::<&str>::None,
                ])) as ArrayRef,
                Arc::new(StringArray::from(vec![
                    Option::<&str>::None,
                    Option::<&str>::None,
                    Option::<&str>::None,
                ])) as ArrayRef,
                Arc::new(StringArray::from(vec![
                    Some(r#"{"enabled":true}"#),
                    Option::<&str>::None,
                    Option::<&str>::None,
                ])) as ArrayRef,
            ],
        )
        .expect("to create JSON union array");

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("sandbox", JSON_UNION_DATA_TYPE.clone(), true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1_i64, 2_i64, 3_i64])) as ArrayRef,
                Arc::new(union_array) as ArrayRef,
            ],
        )
        .expect("to create record batch");

        let value = write_to_json_value(&[batch]).expect("to serialize JSON");

        assert_eq!(
            value,
            json!([
                {
                    "id": 1,
                    "sandbox": {
                        "enabled": true
                    }
                },
                {
                    "id": 2,
                    "sandbox": "draft"
                },
                {
                    "id": 3,
                    "sandbox": null
                }
            ])
        );
    }

    #[test]
    fn test_write_to_json_value_handles_nested_json_union_fields() {
        let DataType::Union(fields, UnionMode::Sparse) = &*JSON_UNION_DATA_TYPE else {
            panic!("JSON union data type should be a sparse union");
        };

        let union_array = UnionArray::try_new(
            fields.clone(),
            Buffer::from_vec(vec![6_i8]).into(),
            None,
            vec![
                Arc::new(NullArray::new(1)) as ArrayRef,
                Arc::new(BooleanArray::from(vec![None])) as ArrayRef,
                Arc::new(Int64Array::from(vec![None])) as ArrayRef,
                Arc::new(arrow::array::Float64Array::from(vec![None])) as ArrayRef,
                Arc::new(StringArray::from(vec![Option::<&str>::None])) as ArrayRef,
                Arc::new(StringArray::from(vec![Option::<&str>::None])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some(r#"{"enabled":true}"#)])) as ArrayRef,
            ],
        )
        .expect("to create JSON union array");

        let payload_fields = vec![Arc::new(Field::new(
            "sandbox",
            JSON_UNION_DATA_TYPE.clone(),
            true,
        ))];
        let payload_array = StructArray::new(
            payload_fields.clone().into(),
            vec![Arc::new(union_array) as ArrayRef],
            None,
        );

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("payload", DataType::Struct(payload_fields.into()), true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1_i64])) as ArrayRef,
                Arc::new(payload_array) as ArrayRef,
            ],
        )
        .expect("to create record batch");

        let value = write_to_json_value(&[batch]).expect("to serialize JSON");

        assert_eq!(
            value,
            json!([
                {
                    "id": 1,
                    "payload": {
                        "sandbox": {
                            "enabled": true
                        }
                    }
                }
            ])
        );
    }

    struct TestExecutionPlan {
        metrics: Option<MetricsSet>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        properties: Arc<PlanProperties>,
    }

    impl TestExecutionPlan {
        fn new(metrics: Option<MetricsSet>, children: Vec<Arc<dyn ExecutionPlan>>) -> Self {
            Self {
                metrics,
                children,
                properties: Arc::new(PlanProperties::new(
                    EquivalenceProperties::new(Arc::new(Schema::empty())),
                    Partitioning::UnknownPartitioning(1),
                    EmissionType::Final,
                    Boundedness::Bounded,
                )),
            }
        }
    }

    impl Debug for TestExecutionPlan {
        fn fmt(&self, _f: &mut Formatter<'_>) -> std::fmt::Result {
            unimplemented!("Not used in tests")
        }
    }

    impl DisplayAs for TestExecutionPlan {
        fn fmt_as(&self, _t: DisplayFormatType, _f: &mut Formatter) -> std::fmt::Result {
            unimplemented!("Not used in tests")
        }
    }

    impl ExecutionPlan for TestExecutionPlan {
        fn name(&self) -> &'static str {
            "TestExecutionPlan"
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn properties(&self) -> &Arc<PlanProperties> {
            &self.properties
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            self.children.iter().collect()
        }

        fn with_new_children(
            self: Arc<Self>,
            _children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
            Ok(self)
        }

        fn metrics(&self) -> Option<MetricsSet> {
            self.metrics.clone()
        }

        fn execute(
            &self,
            _partition: usize,
            _context: Arc<TaskContext>,
        ) -> datafusion::common::Result<SendableRecordBatchStream> {
            unimplemented!("Not used in tests")
        }
    }

    #[tokio::test]
    async fn test_collect_physical_plan_metrics_no_children() {
        let metrics_set = ExecutionPlanMetricsSet::new();
        MetricBuilder::new(&metrics_set).spill_count(1).add(13);
        MetricBuilder::new(&metrics_set).spill_count(2).add(7);
        MetricBuilder::new(&metrics_set).spilled_rows(2).add(100);

        let plan = Arc::new(TestExecutionPlan::new(
            Some(metrics_set.clone_inner()),
            vec![],
        )) as Arc<dyn ExecutionPlan>;

        let mut totals = PhysicalPlanMetricsTotals::default();
        collect_physical_plan_metrics(plan.as_ref(), &mut totals);

        assert_eq!(totals.produced_spills, 20);
        assert_eq!(totals.spilled_bytes, 0);
        assert_eq!(totals.spilled_rows, 100);
    }

    #[tokio::test]
    async fn test_collect_physical_plan_metrics_with_children() {
        let metrics_set = ExecutionPlanMetricsSet::new();
        MetricBuilder::new(&metrics_set).spill_count(1).add(13);
        MetricBuilder::new(&metrics_set).spill_count(2).add(7);
        MetricBuilder::new(&metrics_set).spilled_rows(2).add(100);

        let child1 = Arc::new(TestExecutionPlan::new(
            Some(metrics_set.clone_inner()),
            vec![],
        )) as Arc<dyn ExecutionPlan>;

        let child2 = Arc::new(TestExecutionPlan::new(None, vec![])) as Arc<dyn ExecutionPlan>;

        let metrics_set = ExecutionPlanMetricsSet::new();
        MetricBuilder::new(&metrics_set).spill_count(1).add(13);
        MetricBuilder::new(&metrics_set).spill_count(2).add(7);
        MetricBuilder::new(&metrics_set).spilled_rows(2).add(100);

        let plan = Arc::new(TestExecutionPlan::new(
            Some(metrics_set.clone_inner()),
            vec![child1, child2],
        )) as Arc<dyn ExecutionPlan>;

        let mut totals = PhysicalPlanMetricsTotals::default();
        collect_physical_plan_metrics(plan.as_ref(), &mut totals);

        assert_eq!(totals.produced_spills, 40);
        assert_eq!(totals.spilled_bytes, 0);
        assert_eq!(totals.spilled_rows, 200);
    }

    /// Helper: build a `SendableRecordBatchStream` from a schema and batches.
    fn stream_from_batches(
        schema: &Arc<Schema>,
        batches: Vec<RecordBatch>,
    ) -> SendableRecordBatchStream {
        Box::pin(RecordBatchStreamAdapter::new(Arc::clone(schema), {
            futures::stream::iter(batches.into_iter().map(Ok))
        }))
    }

    fn pending_stream(schema: &Arc<Schema>) -> SendableRecordBatchStream {
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(schema),
            futures::stream::pending::<DataFusionResult<RecordBatch>>(),
        ))
    }

    struct DropFlag {
        dropped: Arc<AtomicBool>,
    }

    impl DropFlag {
        fn new(dropped: Arc<AtomicBool>) -> Self {
            Self { dropped }
        }
    }

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
        }
    }

    fn assert_query_cancelled(error: DataFusionError, expected_query_id: &str) {
        assert!(is_cancellation_error(&error));
        let DataFusionError::External(source) = error else {
            panic!("expected external cancellation error");
        };
        let cancellation = source
            .downcast_ref::<Error>()
            .expect("external error should be query cancellation");
        let Error::QueryCancelled { query_id } = cancellation else {
            panic!("expected query cancellation error");
        };
        assert_eq!(query_id, expected_query_id);
    }

    #[tokio::test]
    async fn attach_cancellation_to_stream_emits_single_cancel_then_terminates() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            arrow::datatypes::DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![42])) as ArrayRef],
        )
        .expect("batch");
        let cancel_token = CancellationToken::new();
        let guard_dropped = Arc::new(AtomicBool::new(false));
        let query_id = uuid::Uuid::new_v4().to_string();
        let mut stream = attach_cancellation_to_stream(
            stream_from_batches(&schema, vec![batch]),
            cancel_token.clone(),
            query_id.clone(),
            DropFlag::new(Arc::clone(&guard_dropped)),
        );

        let first = stream
            .next()
            .await
            .expect("stream should emit the first batch")
            .expect("first batch should succeed");
        assert_eq!(first.num_rows(), 1);
        assert!(!guard_dropped.load(Ordering::SeqCst));

        cancel_token.cancel();
        let cancellation = stream
            .next()
            .await
            .expect("stream should emit one cancellation error");
        assert_query_cancelled(
            cancellation.expect_err("second item should be cancellation"),
            &query_id,
        );
        assert!(guard_dropped.load(Ordering::SeqCst));
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn attach_cancellation_to_stream_wakes_pending_next_on_cancel() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            arrow::datatypes::DataType::Int64,
            false,
        )]));
        let cancel_token = CancellationToken::new();
        let guard_dropped = Arc::new(AtomicBool::new(false));
        let query_id = uuid::Uuid::new_v4().to_string();
        let mut stream = attach_cancellation_to_stream(
            pending_stream(&schema),
            cancel_token.clone(),
            query_id.clone(),
            DropFlag::new(Arc::clone(&guard_dropped)),
        );

        let pending_next = tokio::spawn(async move { stream.next().await });
        tokio::task::yield_now().await;
        cancel_token.cancel();

        let cancellation = pending_next
            .await
            .expect("pending stream task should complete")
            .expect("pending stream should emit cancellation");
        assert_query_cancelled(
            cancellation.expect_err("pending item should be cancellation"),
            &query_id,
        );
        assert!(guard_dropped.load(Ordering::SeqCst));
    }

    /// Collect all batches from a `SendableRecordBatchStream`.
    async fn collect_stream(mut stream: SendableRecordBatchStream) -> Vec<RecordBatch> {
        let mut batches = Vec::new();
        while let Some(result) = stream.next().await {
            batches.push(result.expect("unexpected error in stream"));
        }
        batches
    }

    #[tokio::test]
    async fn test_reconcile_stream_nullability_widens_non_nullable() {
        // Simulates AggregateStatistics replacing MAX(id) with a literal:
        // execution schema has non-nullable field, plan schema has nullable.
        let exec_schema = Arc::new(Schema::new(vec![Field::new(
            "max(id)",
            arrow::datatypes::DataType::Int64,
            false,
        )]));
        let plan_schema = Arc::new(Schema::new(vec![Field::new(
            "max(id)",
            arrow::datatypes::DataType::Int64,
            true,
        )]));

        let batch = RecordBatch::try_new(
            Arc::clone(&exec_schema),
            vec![Arc::new(Int64Array::from(vec![42]))],
        )
        .expect("batch");

        let stream = stream_from_batches(&exec_schema, vec![batch]);
        let reconciled = reconcile_stream_nullability(stream, &plan_schema);

        // Schema should now be nullable
        assert!(
            reconciled.schema().field(0).is_nullable(),
            "field should be nullable after reconciliation"
        );

        // Batch data should be preserved
        let batches = collect_stream(reconciled).await;
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        assert!(
            batches[0].schema().field(0).is_nullable(),
            "batch schema should also be nullable"
        );
        let col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 array");
        assert_eq!(col.value(0), 42);
    }

    #[tokio::test]
    async fn test_reconcile_stream_nullability_no_op_when_already_matching() {
        // Both schemas agree: nullable. No wrapping needed.
        let schema = Arc::new(Schema::new(vec![Field::new(
            "max(id)",
            arrow::datatypes::DataType::Int64,
            true,
        )]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![42]))],
        )
        .expect("batch");

        let stream = stream_from_batches(&schema, vec![batch]);
        let reconciled = reconcile_stream_nullability(stream, &schema);

        // Schema unchanged
        assert_eq!(reconciled.schema(), schema);

        let batches = collect_stream(reconciled).await;
        assert_eq!(batches.len(), 1);
    }

    #[tokio::test]
    async fn test_reconcile_stream_nullability_no_op_when_non_nullable_in_both() {
        // Both schemas agree: non-nullable. No wrapping needed.
        let schema = Arc::new(Schema::new(vec![Field::new(
            "count",
            arrow::datatypes::DataType::Int64,
            false,
        )]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![10]))],
        )
        .expect("batch");

        let stream = stream_from_batches(&schema, vec![batch]);
        let reconciled = reconcile_stream_nullability(stream, &schema);

        assert!(!reconciled.schema().field(0).is_nullable());

        let batches = collect_stream(reconciled).await;
        assert_eq!(batches.len(), 1);
    }

    #[tokio::test]
    async fn test_reconcile_stream_nullability_mixed_fields() {
        // Multiple fields: only some need reconciliation.
        let exec_schema = Arc::new(Schema::new(vec![
            Field::new("name", arrow::datatypes::DataType::Utf8, true), // already nullable
            Field::new("max(id)", arrow::datatypes::DataType::Int64, false), // needs widening
            Field::new("count", arrow::datatypes::DataType::Int64, false), // stays non-nullable
        ]));
        let plan_schema = Arc::new(Schema::new(vec![
            Field::new("name", arrow::datatypes::DataType::Utf8, true),
            Field::new("max(id)", arrow::datatypes::DataType::Int64, true), // nullable in plan
            Field::new("count", arrow::datatypes::DataType::Int64, false),  // non-nullable in plan
        ]));

        let batch = RecordBatch::try_new(
            Arc::clone(&exec_schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["a"])),
                Arc::new(Int64Array::from(vec![42])),
                Arc::new(Int64Array::from(vec![10])),
            ],
        )
        .expect("batch");

        let stream = stream_from_batches(&exec_schema, vec![batch]);
        let reconciled = reconcile_stream_nullability(stream, &plan_schema);

        let schema = reconciled.schema();
        assert!(schema.field(0).is_nullable(), "name stays nullable");
        assert!(schema.field(1).is_nullable(), "max(id) widened to nullable");
        assert!(!schema.field(2).is_nullable(), "count stays non-nullable");

        let batches = collect_stream(reconciled).await;
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(batches[0].schema(), schema);
    }

    #[tokio::test]
    async fn test_reconcile_stream_nullability_field_count_mismatch() {
        // Different field counts: return stream unchanged.
        let exec_schema = Arc::new(Schema::new(vec![Field::new(
            "a",
            arrow::datatypes::DataType::Int64,
            false,
        )]));
        let plan_schema = Arc::new(Schema::new(vec![
            Field::new("a", arrow::datatypes::DataType::Int64, true),
            Field::new("b", arrow::datatypes::DataType::Int64, true),
        ]));

        let batch = RecordBatch::try_new(
            Arc::clone(&exec_schema),
            vec![Arc::new(Int64Array::from(vec![1]))],
        )
        .expect("batch");

        let stream = stream_from_batches(&exec_schema, vec![batch]);
        let reconciled = reconcile_stream_nullability(stream, &plan_schema);

        // Should be unchanged — no widening when field counts differ
        assert!(!reconciled.schema().field(0).is_nullable());
    }

    #[tokio::test]
    async fn test_reconcile_stream_nullability_empty_stream() {
        let exec_schema = Arc::new(Schema::new(vec![Field::new(
            "max(id)",
            arrow::datatypes::DataType::Int64,
            false,
        )]));
        let plan_schema = Arc::new(Schema::new(vec![Field::new(
            "max(id)",
            arrow::datatypes::DataType::Int64,
            true,
        )]));

        let stream = stream_from_batches(&exec_schema, vec![]);
        let reconciled = reconcile_stream_nullability(stream, &plan_schema);

        assert!(reconciled.schema().field(0).is_nullable());

        let batches = collect_stream(reconciled).await;
        assert!(batches.is_empty());
    }

    #[tokio::test]
    async fn test_prepare_physical_plan_for_sync_results_preserves_ordered_rows() {
        use arrow::array::Int32Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use datafusion::{
            datasource::MemTable, execution::context::SessionContext, prelude::SessionConfig,
        };

        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(4));

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from_iter_values(0..32))],
        )
        .expect("batch");
        let table = MemTable::try_new(Arc::clone(&schema), vec![vec![batch]]).expect("table");
        ctx.register_table("t", Arc::new(table))
            .expect("register table");

        let dataframe = ctx
            .sql("SELECT id FROM t ORDER BY id DESC")
            .await
            .expect("sql");
        let plan = dataframe
            .create_physical_plan()
            .await
            .expect("physical plan");
        assert!(
            plan.output_ordering().is_some(),
            "expected ordered output plan"
        );

        let wrapped = Arc::new(
            RepartitionExec::try_new(plan, Partitioning::RoundRobinBatch(4)).expect("repartition"),
        ) as Arc<dyn ExecutionPlan>;
        assert!(
            wrapped.output_ordering().is_some(),
            "expected ordered output after repartition"
        );
        assert!(
            wrapped.output_partitioning().partition_count() > 1,
            "expected multi-partition output before sync rewrite"
        );

        let prepared = prepare_physical_plan_for_sync_results(wrapped).expect("prepare plan");
        assert_eq!(prepared.output_partitioning().partition_count(), 1);

        let batches = collect_stream(
            execute_stream(prepared, Arc::new(TaskContext::from(&ctx.state())))
                .expect("execute prepared plan"),
        )
        .await;

        let ids: Vec<i32> = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .expect("int32 array")
                    .iter()
                    .map(|v| v.expect("non-null id"))
                    .collect::<Vec<_>>()
            })
            .collect();

        assert_eq!(ids, (0..32).rev().collect::<Vec<_>>());
    }
}
