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

use std::{cell::LazyCell, sync::Arc};

use arrow::datatypes::{Schema, SchemaRef};
use arrow_tools::schema::verify_schema;
use cache::{get_logical_plan_input_tables, to_cached_record_batch_stream, QueryResult};
use datafusion::{
    error::DataFusionError,
    execution::{context::SQLOptions, SendableRecordBatchStream},
    physical_plan::{memory::MemoryStream, stream::RecordBatchStreamAdapter},
};
use error_code::ErrorCode;
use snafu::Snafu;
pub(crate) use tracker::QueryTracker;

pub mod builder;
pub mod query_history;
pub use builder::QueryBuilder;
pub mod error_code;
mod tracker;

use async_stream::stream;
use futures::StreamExt;

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
}

impl std::fmt::Display for Protocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Protocol::Http => write!(f, "http"),
            Protocol::Flight => write!(f, "flight"),
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
    ($self:expr, $error_code:expr, $error:expr, $target_error:ident) => {{
        let snafu_error = Error::$target_error { source: $error };
        $self
            .finish_with_error(snafu_error.to_string(), $error_code)
            .await;
        return Err(snafu_error);
    }};
}

impl Query {
    pub async fn run(self) -> Result<QueryResult> {
        let session = self.df.ctx.state();

        let ctx = self;
        let mut tracker = ctx.tracker;

        let plan = match session.create_logical_plan(&ctx.sql).await {
            Ok(plan) => plan,
            Err(e) => {
                let error_code = ErrorCode::from(&e);
                handle_error!(tracker, error_code, e, UnableToExecuteQuery)
            }
        };

        let mut plan_is_cache_enabled = false;
        let plan_cache_key = cache::key_for_logical_plan(&plan);

        if let Some(cache_provider) = &ctx.df.cache_provider() {
            if let Some(cached_result) = match cache_provider.get(&plan).await {
                Ok(Some(v)) => Some(v),
                Ok(None) => None,
                Err(e) => handle_error!(tracker, ErrorCode::InternalError, e, FailedToAccessCache),
            } {
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
                            tracker,
                            ErrorCode::InternalError,
                            e,
                            UnableToCreateMemoryStream
                        )
                    }
                };

                return Ok(QueryResult::new(
                    attach_query_tracker_to_stream(tracker, Box::pin(record_batch_stream)),
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
                    tracker,
                    ErrorCode::QueryPlanningError,
                    e,
                    UnableToExecuteQuery
                )
            }
        }

        tracker = tracker.datasets(Arc::new(get_logical_plan_input_tables(&plan)));

        let df = match ctx.df.ctx.execute_logical_plan(plan).await {
            Ok(df) => df,
            Err(e) => {
                let error_code = ErrorCode::from(&e);
                handle_error!(tracker, error_code, e, UnableToExecuteQuery)
            }
        };

        let df_schema: SchemaRef = Arc::clone(df.schema().inner());

        let res_stream: SendableRecordBatchStream = match df.execute_stream().await {
            Ok(stream) => stream,
            Err(e) => {
                let error_code = ErrorCode::from(&e);
                handle_error!(tracker, error_code, e, UnableToExecuteQuery)
            }
        };

        let res_schema = res_stream.schema();

        if let Err(e) = verify_schema(df_schema.fields(), res_schema.fields()) {
            handle_error!(tracker, ErrorCode::InternalError, e, SchemaMismatch)
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
                    attach_query_tracker_to_stream(tracker, record_batch_stream),
                    Some(false),
                ));
            }
        }

        Ok(QueryResult::new(
            attach_query_tracker_to_stream(tracker, res_stream),
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
    ctx: QueryTracker,
    mut stream: SendableRecordBatchStream,
) -> SendableRecordBatchStream {
    let schema = stream.schema();
    let schema_copy = Arc::clone(&schema);

    let mut num_records = 0u64;

    let updated_stream = stream! {
        while let Some(batch_result) = stream.next().await {

            match &batch_result {
                Ok(batch) => {
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

        ctx
            .schema(schema_copy)
            .rows_produced(num_records)
            .finish()
            .await;
    };

    Box::pin(RecordBatchStreamAdapter::new(
        schema,
        Box::pin(updated_stream),
    ))
}
