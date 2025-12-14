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

#[cfg(feature = "cluster")]
use datafusion::execution::SessionStateBuilder;
use std::{fmt::Display, sync::Arc};

use ::cache::{
    get_logical_plan_input_tables,
    key::CacheKey,
    result::{CacheStatus, query::QueryResult},
};
use arrow::{array::RecordBatch, datatypes::Schema};
use arrow_schema::{Field, SchemaBuilder};
use arrow_tools::schema::verify_schema;
use cache::PlanOrCached;
use datafusion::{
    common::ParamValues,
    error::DataFusionError,
    execution::SendableRecordBatchStream,
    execution::TaskContext,
    logical_expr::LogicalPlan,
    physical_plan::{ExecutionPlan, execute_stream, stream::RecordBatchStreamAdapter},
};
use error_code::ErrorCode;
use snafu::{ResultExt, Snafu};
use tokio::time::Instant;
use tracing::Span;
use tracing_futures::Instrument;
pub(crate) use tracker::QueryTracker;

pub mod builder;
pub use builder::QueryBuilder;
mod cache;
pub mod error_code;
mod metrics;
mod tracker;

#[cfg(feature = "cluster")]
use {
    crate::config::ClusterMode,
    crate::datafusion::builder::default_extension_planners,
    ballista_core::extension::{SessionConfigExt, SessionStateExt},
    ballista_core::planner::BallistaQueryPlanner,
    datafusion::physical_planner::DefaultPhysicalPlanner,
    datafusion_proto::protobuf::LogicalPlanNode,
};

use datafusion::execution::SessionState;

use async_stream::stream;
#[cfg(feature = "cluster")]
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use futures::StreamExt;

use super::{SPICE_RUNTIME_SCHEMA, error::find_datafusion_root};

use super::managed_runtime;
#[cfg(feature = "cluster")]
use crate::cluster::datafusion::codec::spice_logical_codec::SpiceLogicalCodec;
use crate::datafusion::{
    DataFusion, query::cache::RequestCacheManager, sql_validator::validate_sql_query_operations,
};
use managed_runtime::ManagedRuntimeError;
use opentelemetry::KeyValue;
#[cfg(feature = "cluster")]
use runtime_datafusion::config::cluster_config::SpiceClusterConfig;
use runtime_request_context::{AsyncMarker, RequestContext};
use tokio::runtime::Handle;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to execute query: {source}"))]
    UnableToExecuteQuery { source: DataFusionError },

    #[snafu(display("Failed to access query results cache: {source}"))]
    FailedToAccessCache { source: ::cache::Error },

    #[snafu(display("Unable to convert cached result to a record batch stream: {source}"))]
    UnableToCreateMemoryStream { source: DataFusionError },

    #[snafu(display("Unable to collect results after query execution: {source}"))]
    UnableToCollectResults { source: DataFusionError },

    #[snafu(display("Schema mismatch: {source}"))]
    SchemaMismatch { source: arrow_tools::schema::Error },

    #[snafu(display("Failed to set parameters in logical plan: {source}"))]
    BindingParameters { source: DataFusionError },

    #[snafu(display(
        "Cache-Control header specifies 'stale-while-revalidate' which is only supported with cache_key_type: sql (raw). \
        The current configuration uses cache_key_type: {cache_key_type}. \
        Either remove 'stale-while-revalidate' from the Cache-Control header or change cache_key_type to 'sql'."
    ))]
    UnsupportedStaleWhileRevalidate { cache_key_type: String },
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
    Plan(Box<LogicalPlan>),
    Text {
        sql: Arc<str>,
        parameters: Option<ParamValues>,
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
}

macro_rules! handle_error {
    ($self:expr, $request_context:expr, $error_code:expr, $error:expr, $target_error:ident) => {{
        let snafu_error = Error::$target_error { source: $error };
        $self.map(|t| t.finish_with_error($request_context, snafu_error.to_string(), $error_code));
        return Err(snafu_error);
    }};
}

impl Query {
    #[cfg(not(feature = "cluster"))]
    #[expect(clippy::unnecessary_wraps)]
    fn get_session_state(&self) -> Result<SessionState> {
        Ok(self.df.ctx.state())
    }

    #[cfg(feature = "cluster")]
    fn get_session_state(&self) -> Result<SessionState> {
        if !matches!(self.df.cluster_config.mode, Some(ClusterMode::Scheduler)) {
            return Ok(self.df.ctx.state());
        }

        let cfg = self
            .df
            .ctx
            .copied_config()
            .with_ballista_logical_extension_codec(SpiceLogicalCodec::new_codec());

        let query_planner: BallistaQueryPlanner<LogicalPlanNode> =
            BallistaQueryPlanner::with_local_planner(
                self.df.cluster_config.scheduler_url.to_string(),
                cfg.ballista_config(),
                SpiceLogicalCodec::new_codec(),
                DefaultPhysicalPlanner::with_extension_planners(default_extension_planners()),
            );

        SessionStateBuilder::new_from_existing(self.df.ctx.state())
            .with_config(
                cfg.with_ballista_query_planner(Arc::new(query_planner))
                    .with_option_extension(SpiceClusterConfig::default()),
            )
            .build()
            .upgrade_for_ballista(self.df.cluster_config.scheduler_url.to_string())
            .map_err(|e| Error::UnableToExecuteQuery { source: e })
    }

    #[cfg(feature = "cluster")]
    fn should_distribute_plan(plan: &LogicalPlan) -> datafusion::common::Result<bool> {
        let mut should_distribute = true;

        let _ = plan.apply(|p| {
            if let LogicalPlan::DescribeTable(_) = p {
                should_distribute = false;
            } else if let LogicalPlan::TableScan(scan) = p
                && matches!(scan.table_name.schema(), Some(SPICE_RUNTIME_SCHEMA))
            {
                should_distribute = false;
            }

            if should_distribute {
                Ok(TreeNodeRecursion::Continue)
            } else {
                Ok(TreeNodeRecursion::Stop)
            }
        })?;

        Ok(should_distribute)
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

        let inner_span = span.clone();

        let query_result = async {
            let mut session = self.get_session_state()?;

            let ctx = self;
            let tracker = ctx.tracker;

            // Sets the request context as an extension on DataFusion, to allow recovering it to track telemetry
            session
                .config_mut()
                .set_extension(Arc::clone(&request_context));

            // Get the `LogicalPlan` or cached results
            let (plan, mut tracker, cache_manager) = match &ctx.sql {
                QueryMethod::Text { sql, parameters } => {
                    match Self::get_plan_or_cached(
                        &ctx.df,
                        &session,
                        Arc::clone(&request_context),
                        sql,
                        parameters.clone(),
                        tracker,
                    )
                    .await?
                    {
                        PlanOrCached::Plan(plan, tracker, cache_manager) => {
                            (plan, tracker, cache_manager)
                        }
                        PlanOrCached::Cached(query_result) => return Ok(query_result),
                    }
                }
                QueryMethod::Plan(logical_plan) => {
                    let cache_manager = RequestCacheManager::new(
                        CacheStatus::CacheMiss,
                        CacheKey::LogicalPlan(logical_plan).as_raw_key(Query::plan_hasher(&ctx.df)),
                    );
                    (logical_plan.clone(), None, cache_manager)
                }
            };

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

            // Special handling in cluster mode - execute DescribeTable and runtime.* queries locally
            #[cfg(feature = "cluster")]
            let should_distribute =
                Self::should_distribute_plan(&plan).context(UnableToExecuteQuerySnafu)?;

            #[cfg(not(feature = "cluster"))]
            let should_distribute = false;

            let session_for_execution = if should_distribute {
                session
            } else {
                ctx.df.ctx.state()
            };

            let physical_plan = match session_for_execution.create_physical_plan(&plan).await {
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

            let task_ctx = Arc::new(TaskContext::from(&session_for_execution));

            let res_stream = match execute_stream(Arc::clone(&physical_plan), task_ctx) {
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

    pub fn from_logical_plan(df: &Arc<DataFusion>, plan: &LogicalPlan) -> Self {
        Self {
            df: Arc::clone(df),
            sql: QueryMethod::Plan(Box::new(plan.clone())),
            tracker: None,
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
        let session = self.df.ctx.state();
        let request_context = RequestContext::current(AsyncMarker::new().await);
        let plan = match self.sql {
            QueryMethod::Plan(ref plan) => plan.clone(),
            QueryMethod::Text { ref sql, .. } => match session.create_logical_plan(sql).await {
                Ok(plan) => Box::new(plan),
                Err(e) => {
                    let e = find_datafusion_root(e);
                    self.handle_schema_error(&request_context, &e);
                    return Err(e);
                }
            },
        };

        // Verify the plan against the restricted options
        if let Err(e) = validate_sql_query_operations(&plan, &self.df) {
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

pub fn write_to_json_string(
    data: &[RecordBatch],
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let buf = Vec::new();
    let mut writer = arrow_json::ArrayWriter::new(buf);

    writer.write_batches(data.iter().collect::<Vec<&RecordBatch>>().as_slice())?;
    writer.finish()?;

    String::from_utf8(writer.into_inner()).boxed()
}

#[cfg(test)]
mod tests {
    use ::cache::{Caching, QueryResultsCacheProvider, result::CacheStatus};
    use arrow::array::Int64Array;
    use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
    use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
    use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder, MetricsSet};
    use datafusion::physical_plan::{DisplayAs, DisplayFormatType, PlanProperties};
    use serde_json::json;
    use spicepod::component::caching::SQLResultsCacheConfig;
    use std::any::Any;
    use std::fmt::{Debug, Formatter};

    use crate::{
        dataaccelerator::AcceleratorEngineRegistry,
        datafusion::{builder::DataFusionBuilder, param_utils::convert_json_to_param_values},
        status::RuntimeStatus,
    };

    use super::*;

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

    struct TestExecutionPlan {
        metrics: Option<MetricsSet>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        properties: PlanProperties,
    }

    impl TestExecutionPlan {
        fn new(metrics: Option<MetricsSet>, children: Vec<Arc<dyn ExecutionPlan>>) -> Self {
            Self {
                metrics,
                children,
                properties: PlanProperties::new(
                    EquivalenceProperties::new(Arc::new(Schema::empty())),
                    Partitioning::UnknownPartitioning(1),
                    EmissionType::Final,
                    Boundedness::Bounded,
                ),
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

        fn properties(&self) -> &PlanProperties {
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
}
