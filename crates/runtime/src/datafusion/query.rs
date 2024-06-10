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

use std::{collections::HashSet, string, sync::Arc, time::SystemTime};

use arrow::datatypes::Schema;
use arrow_tools::schema::verify_schema;
use cache::{
    cache_is_enabled_for_plan, get_logical_plan_input_tables, to_cached_record_batch_stream,
    QueryResult,
};
use datafusion::{
    error::DataFusionError,
    execution::{context::SQLOptions, SendableRecordBatchStream},
    physical_plan::{memory::MemoryStream, stream::RecordBatchStreamAdapter},
};
use error_code::ErrorCode;
use snafu::Snafu;
use tokio::time::Instant;
use uuid::Uuid;

pub mod builder;
#[allow(clippy::module_name_repetitions)]
pub mod query_history;
#[allow(clippy::module_name_repetitions)]
pub use builder::QueryBuilder;

pub mod error_code;

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

#[derive(Debug, Clone)]
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

pub struct Query {
    df: Arc<crate::datafusion::DataFusion>,
    sql: String,
    query_id: Uuid,
    schema: Option<Arc<Schema>>,
    nsql: Option<String>,
    start_time: SystemTime,
    end_time: Option<SystemTime>,
    execution_time: Option<f32>,
    rows_produced: u64,
    results_cache_hit: Option<bool>,
    restricted_sql_options: Option<SQLOptions>,
    error_message: Option<String>,
    error_code: Option<ErrorCode>,
    timer: Instant,
    datasets: Arc<HashSet<String>>,
    protocol: Protocol,
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

        let mut ctx = self;

        let plan = match session.create_logical_plan(&ctx.sql).await {
            Ok(plan) => plan,
            Err(e) => {
                let error_code = ErrorCode::from(&e);
                handle_error!(ctx, error_code, e, UnableToExecuteQuery)
            }
        };

        if let Some(cache_provider) = &ctx.df.cache_provider() {
            if let Some(cached_result) = match cache_provider.get(&plan).await {
                Ok(Some(v)) => Some(v),
                Ok(None) => None,
                Err(e) => handle_error!(ctx, ErrorCode::InternalError, e, FailedToAccessCache),
            } {
                ctx = ctx
                    .datasets(cached_result.input_tables)
                    .results_cache_hit(true);

                let record_batch_stream = match MemoryStream::try_new(
                    cached_result.records.to_vec(),
                    cached_result.schema,
                    None,
                ) {
                    Ok(stream) => stream,
                    Err(e) => {
                        handle_error!(ctx, ErrorCode::InternalError, e, UnableToCreateMemoryStream)
                    }
                };

                return Ok(QueryResult::new(
                    attach_query_context_to_stream(ctx, Box::pin(record_batch_stream)),
                    Some(true),
                ));
            }

            ctx = ctx.results_cache_hit(false);
        }

        if let Some(restricted_sql_options) = ctx.restricted_sql_options {
            if let Err(e) = restricted_sql_options.verify_plan(&plan) {
                handle_error!(ctx, ErrorCode::QueryPlanningError, e, UnableToExecuteQuery)
            }
        }

        ctx = ctx.datasets(Arc::new(get_logical_plan_input_tables(&plan)));

        let plan_copy = plan.clone();

        let df = match ctx.df.ctx.execute_logical_plan(plan).await {
            Ok(df) => df,
            Err(e) => {
                let error_code = ErrorCode::from(&e);
                handle_error!(ctx, error_code, e, UnableToExecuteQuery)
            }
        };

        let df_schema: Arc<Schema> = df.schema().clone().into();

        let res_stream: SendableRecordBatchStream = match df.execute_stream().await {
            Ok(stream) => stream,
            Err(e) => {
                let error_code = ErrorCode::from(&e);
                handle_error!(ctx, error_code, e, UnableToExecuteQuery)
            }
        };

        let res_schema = res_stream.schema();

        if let Err(e) = verify_schema(df_schema.fields(), res_schema.fields()) {
            handle_error!(ctx, ErrorCode::InternalError, e, SchemaMismatch)
        };

        if cache_is_enabled_for_plan(&plan_copy) {
            if let Some(cache_provider) = &ctx.df.cache_provider() {
                let record_batch_stream = to_cached_record_batch_stream(
                    Arc::clone(cache_provider),
                    res_stream,
                    plan_copy,
                    Arc::clone(&ctx.datasets),
                );

                return Ok(QueryResult::new(
                    attach_query_context_to_stream(ctx, record_batch_stream),
                    Some(false),
                ));
            }
        }

        Ok(QueryResult::new(
            attach_query_context_to_stream(ctx, res_stream),
            None,
        ))
    }

    pub async fn get_schema(&self) -> Result<Schema, DataFusionError> {
        let df = self.df.ctx.sql(&self.sql).await?;
        Ok(df.schema().into())
    }

    pub async fn finish_with_error(mut self, error_message: String, error_code: ErrorCode) {
        tracing::debug!(
            "Query '{}' finished with error: {error_message}; code: {error_code}",
            self.sql
        );
        self.error_message = Some(error_message);
        self.error_code = Some(error_code);
        self.finish().await;
    }

    pub async fn finish(mut self) {
        if self.end_time.is_none() {
            self.end_time = Some(SystemTime::now());
        }

        let duration = self.timer.elapsed();

        if self.execution_time.is_none() {
            self.execution_time = Some(duration.as_secs_f32());
        }

        let mut tags = vec![];
        match self.results_cache_hit {
            Some(true) => {
                tags.push("cache-hit");
            }
            Some(false) => {
                tags.push("cache-miss");
            }
            None => {}
        }

        if self.error_message.is_some() {
            tags.push("error");
        }

        let mut labels = vec![
            ("tags", tags.join(",")),
            (
                "datasets",
                self.datasets
                    .iter()
                    .map(string::ToString::to_string)
                    .collect::<Vec<String>>()
                    .join(","),
            ),
            ("protocol", self.protocol.to_string()),
        ];

        metrics::histogram!("query_duration_seconds", &labels).record(duration.as_secs_f32());

        if let Some(err) = &self.error_code {
            labels.push(("err_code", err.to_string()));
            metrics::counter!("query_failures", &labels).increment(1);
        }

        if let Err(err) = self.write_query_history().await {
            tracing::error!("Error writing query history: {err}");
        };
    }

    #[must_use]
    fn schema(mut self, schema: Arc<Schema>) -> Self {
        self.schema = Some(schema);
        self
    }

    #[must_use]
    fn rows_produced(mut self, rows_produced: u64) -> Self {
        self.rows_produced = rows_produced;
        self
    }

    #[must_use]
    fn results_cache_hit(mut self, cache_hit: bool) -> Self {
        self.results_cache_hit = Some(cache_hit);
        self
    }

    #[must_use]
    fn datasets(mut self, datasets: Arc<HashSet<String>>) -> Self {
        self.datasets = datasets;
        self
    }
}

#[must_use]
/// Attaches a query context to a stream of record batches.
///
/// Processes a stream of record batches, updating the query context
/// with the number of records returned and saving query details at the end.
///
/// Note: If an error occurs during stream processing, the query context
/// is finalized with error details, and further streaming is terminated.
fn attach_query_context_to_stream(
    ctx: Query,
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
