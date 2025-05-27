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

use std::{cell::LazyCell, sync::Arc};

use ::cache::{QueryResult, get_logical_plan_input_tables};
use arrow::{
    array::RecordBatch,
    datatypes::{Schema, SchemaRef},
};
use arrow_schema::{Field, SchemaBuilder};
use arrow_tools::schema::verify_schema;
use cache::PlanOrCached;
use datafusion::{
    common::ParamValues,
    error::DataFusionError,
    execution::{SendableRecordBatchStream, context::SQLOptions},
    logical_expr::LogicalPlan,
    physical_plan::stream::RecordBatchStreamAdapter,
    prelude::DataFrame,
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

use async_stream::stream;
use futures::StreamExt;

use crate::request::{AsyncMarker, RequestContext};

use super::{SPICE_RUNTIME_SCHEMA, error::find_datafusion_root};

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
    parameters: Option<ParamValues>,
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
    /// Run a query and return the result.
    ///
    /// # Panics
    ///
    /// Panics when running under test if no cache key is computed for the query.
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

            // Get the `LogicalPlan` or cached results
            let (plan, mut tracker, cache_manager) = match Self::get_plan_or_cached(
                &ctx.df,
                &session,
                Arc::clone(&request_context),
                &ctx.sql,
                ctx.parameters.clone(),
                tracker,
            )
            .await?
            {
                PlanOrCached::Plan(plan, tracker, cache_manager) => (plan, tracker, cache_manager),
                PlanOrCached::Cached(query_result) => return Ok(query_result),
            };

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
        parameters
            .into_iter()
            .for_each(|(name, typ)| builder.push(Field::new(name, typ, false)));
        Some(builder.finish())
    };

    Ok(maybe_schema)
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

#[cfg(test)]
mod tests {
    use ::cache::{Caching, QueryResultsCacheProvider, QueryResultsCacheStatus};
    use arrow::array::Int64Array;
    use serde_json::json;
    use spicepod::component::runtime::ResultsCache;

    use crate::{
        dataaccelerator::AcceleratorEngineRegistry,
        datafusion::{builder::DataFusionBuilder, param_utils::convert_json_to_param_values},
        status::RuntimeStatus,
    };

    use super::*;

    #[tokio::test]
    async fn parameterized_query() {
        let parameters = convert_json_to_param_values(json!([41])).expect("json to paramvalues");
        let config = ResultsCache::default();
        let cache_provider = Arc::new(
            QueryResultsCacheProvider::try_new(&config, Box::new([])).expect("cache provider new"),
        );
        let df = Arc::new(
            DataFusionBuilder::new(
                RuntimeStatus::new(),
                Arc::new(AcceleratorEngineRegistry::new()),
            )
            .with_caching(Arc::new(Caching::new().with_results_cache(cache_provider)))
            .build(),
        );

        let mut query = QueryBuilder::new("SELECT $1 + 1 AS the_answer", Arc::clone(&df))
            .parameters(parameters.clone())
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

        assert_eq!(
            query.results_cache_status,
            QueryResultsCacheStatus::CacheMiss
        );

        let mut query = QueryBuilder::new("SELECT $1 + 1 AS the_answer", Arc::clone(&df))
            .parameters(parameters)
            .build()
            .run()
            .await
            .expect("Query::run");
        assert_eq!(
            query.results_cache_status,
            QueryResultsCacheStatus::CacheHit
        );

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
            .parameters(parameters)
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

        assert_eq!(
            query.results_cache_status,
            QueryResultsCacheStatus::CacheMiss
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
}
