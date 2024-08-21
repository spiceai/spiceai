/*
Copyright 2024 The Spice.ai OSS Authors

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

use std::{
    cell::LazyCell,
    sync::{Arc, LazyLock},
};

use arrow::{
    array::RecordBatch,
    datatypes::{Schema, SchemaRef},
};
use arrow_tools::schema::verify_schema;
use cache::{get_logical_plan_input_tables, to_cached_record_batch_stream, QueryResult};
use datafusion::{
    error::DataFusionError,
    execution::{context::SQLOptions, SendableRecordBatchStream},
    physical_plan::{memory::MemoryStream, stream::RecordBatchStreamAdapter},
};
use error_code::ErrorCode;
use snafu::{ResultExt, Snafu};
use tokio::time::Instant;
use tracing::Span;
use tracing_futures::Instrument;
pub(crate) use tracker::QueryTracker;

pub mod builder;
pub mod query_history;
pub use builder::QueryBuilder;
pub mod error_code;
mod metrics;
mod tracker;

use async_stream::stream;
use futures::StreamExt;

use super::SPICE_RUNTIME_SCHEMA;

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

#[derive(Debug, Copy, Clone)]
pub enum Protocol {
    Http,
    Flight,
    FlightSQL,
    Internal,
}

static HTTP: LazyLock<Arc<str>> = LazyLock::new(|| "http".into());
static FLIGHT: LazyLock<Arc<str>> = LazyLock::new(|| "flight".into());
static FLIGHTSQL: LazyLock<Arc<str>> = LazyLock::new(|| "flightsql".into());
static INTERNAL: LazyLock<Arc<str>> = LazyLock::new(|| "internal".into());

impl Protocol {
    #[must_use]
    pub fn as_arc_str(&self) -> Arc<str> {
        match self {
            Protocol::Http => Arc::clone(&HTTP),
            Protocol::Flight => Arc::clone(&FLIGHT),
            Protocol::FlightSQL => Arc::clone(&FLIGHTSQL),
            Protocol::Internal => Arc::clone(&INTERNAL),
        }
    }
}

impl std::fmt::Display for Protocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Protocol::Http => write!(f, "http"),
            Protocol::Flight => write!(f, "flight"),
            Protocol::FlightSQL => write!(f, "flightsql"),
            Protocol::Internal => write!(f, "internal"),
        }
    }
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
    restricted_sql_options: bool,
    tracker: QueryTracker,
}

macro_rules! handle_error {
    ($span:expr, $self:expr, $error_code:expr, $error:expr, $target_error:ident) => {{
        let snafu_error = Error::$target_error { source: $error };
        $self
            .finish_with_error(snafu_error.to_string(), $error_code)
            .instrument($span)
            .await;
        return Err(snafu_error);
    }};
}

impl Query {
    #[allow(clippy::too_many_lines)]
    pub async fn run(self) -> Result<QueryResult> {
        telemetry::track_query_count();
        let span = match &self.tracker.nsql {
            Some(nsql) => {
                tracing::span!(target: "task_history", tracing::Level::INFO, "nsql_query", input = %nsql, runtime_query = false)
            }
            None => {
                tracing::span!(target: "task_history", tracing::Level::INFO, "sql_query", input = %self.sql, runtime_query = false)
            }
        };
        let session = self.df.ctx.state();

        let ctx = self;
        let mut tracker = ctx.tracker;

        let plan = match session
            .create_logical_plan(&ctx.sql)
            .instrument(span.clone())
            .await
        {
            Ok(plan) => plan,
            Err(e) => {
                let error_code = ErrorCode::from(&e);
                handle_error!(span.clone(), tracker, error_code, e, UnableToExecuteQuery)
            }
        };

        let mut plan_is_cache_enabled = false;
        let plan_cache_key = cache::key_for_logical_plan(&plan);

        if let Some(cache_provider) = &ctx.df.cache_provider() {
            if let Some(cached_result) =
                match cache_provider.get(&plan).instrument(span.clone()).await {
                    Ok(Some(v)) => Some(v),
                    Ok(None) => None,
                    Err(e) => handle_error!(
                        span.clone(),
                        tracker,
                        ErrorCode::InternalError,
                        e,
                        FailedToAccessCache
                    ),
                }
            {
                tracker = tracker
                    .datasets(cached_result.input_tables)
                    .results_cache_hit(true);

                let record_batch_stream = match MemoryStream::try_new(
                    cached_result.records.to_vec(),
                    cached_result.schema,
                    None,
                ) {
                    Ok(stream) => stream,
                    Err(e) => {
                        handle_error!(
                            span.clone(),
                            tracker,
                            ErrorCode::InternalError,
                            e,
                            UnableToCreateMemoryStream
                        )
                    }
                };

                return Ok(QueryResult::new(
                    attach_query_tracker_to_stream(span, tracker, Box::pin(record_batch_stream)),
                    Some(true),
                ));
            }

            plan_is_cache_enabled = cache_provider.cache_is_enabled_for_plan(&plan);
            tracker = tracker.results_cache_hit(false);
        }

        if ctx.restricted_sql_options {
            if let Err(e) =
                RESTRICTED_SQL_OPTIONS.with(|sql_options| sql_options.verify_plan(&plan))
            {
                handle_error!(
                    span.clone(),
                    tracker,
                    ErrorCode::QueryPlanningError,
                    e,
                    UnableToExecuteQuery
                )
            }
        }

        let input_tables = get_logical_plan_input_tables(&plan);
        if input_tables
            .iter()
            .any(|tr| matches!(tr.schema(), Some(SPICE_RUNTIME_SCHEMA)))
        {
            span.record("runtime_query", true);
        }

        tracker = tracker.datasets(Arc::new(input_tables));

        // Start the timer for the query execution
        tracker.execution_timer = Instant::now();

        let df = match ctx
            .df
            .ctx
            .execute_logical_plan(plan)
            .instrument(span.clone())
            .await
        {
            Ok(df) => df,
            Err(e) => {
                let error_code = ErrorCode::from(&e);
                handle_error!(span.clone(), tracker, error_code, e, UnableToExecuteQuery)
            }
        };

        let df_schema: SchemaRef = Arc::clone(df.schema().inner());

        let res_stream: SendableRecordBatchStream =
            match df.execute_stream().instrument(span.clone()).await {
                Ok(stream) => stream,
                Err(e) => {
                    let error_code = ErrorCode::from(&e);
                    handle_error!(span.clone(), tracker, error_code, e, UnableToExecuteQuery)
                }
            };

        let res_schema = res_stream.schema();

        if let Err(e) = verify_schema(df_schema.fields(), res_schema.fields()) {
            handle_error!(
                span.clone(),
                tracker,
                ErrorCode::InternalError,
                e,
                SchemaMismatch
            )
        };

        if plan_is_cache_enabled {
            if let Some(cache_provider) = &ctx.df.cache_provider() {
                let record_batch_stream = to_cached_record_batch_stream(
                    Arc::clone(cache_provider),
                    res_stream,
                    plan_cache_key,
                    Arc::clone(&tracker.datasets),
                );

                return Ok(QueryResult::new(
                    attach_query_tracker_to_stream(span, tracker, record_batch_stream),
                    Some(false),
                ));
            }
        }

        Ok(QueryResult::new(
            attach_query_tracker_to_stream(span, tracker, res_stream),
            None,
        ))
    }

    pub async fn finish_with_error(self, error_message: String, error_code: ErrorCode) {
        self.tracker
            .finish_with_error(error_message, error_code)
            .await;
    }

    pub async fn get_schema(&self) -> Result<Schema, DataFusionError> {
        let df = self.df.ctx.sql(&self.sql).await?;
        Ok(df.schema().into())
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
    ctx: QueryTracker,
    mut stream: SendableRecordBatchStream,
) -> SendableRecordBatchStream {
    let schema = stream.schema();
    let schema_copy = Arc::clone(&schema);

    let mut num_records = 0u64;
    let mut num_output_bytes = 0u64;

    let mut truncated_output = "[]".to_string(); // default to empty preview

    let updated_stream = stream! {
        while let Some(batch_result) = stream.next().await {

            match &batch_result {
                Ok(batch) => {
                    // Create a truncated output for the query history table on first batch.
                    if num_records == 0 {
                        truncated_output = write_to_json_string(&[batch.slice(0, batch.num_rows().min(3))]).unwrap_or_default();
                    }

                    num_output_bytes += batch.get_array_memory_size() as u64;

                    num_records += batch.num_rows() as u64;
                    yield batch_result
                }
                Err(e) => {
                    ctx
                    .schema(schema_copy)
                    .rows_produced(num_records)
                    .finish_with_error(e.to_string(), ErrorCode::QueryExecutionError).await;
                    yield batch_result;
                    return;
                }
            }
        }

        telemetry::track_bytes_returned(num_output_bytes, ctx.protocol.as_arc_str());

        ctx
            .schema(schema_copy)
            .rows_produced(num_records)
            .finish(Arc::from(truncated_output))
            .await;
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
