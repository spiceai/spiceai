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

use std::{sync::Arc, time::SystemTime};

use arrow::datatypes::Schema;
use arrow_tools::schema::verify_schema;
use cache::{cache_is_enabled_for_plan, to_cached_record_batch_stream, QueryResult};
use datafusion::{
    error::DataFusionError,
    execution::{context::SQLOptions, SendableRecordBatchStream},
    physical_plan::{memory::MemoryStream, stream::RecordBatchStreamAdapter},
};
use snafu::{ResultExt, Snafu};
use uuid::Uuid;

pub mod builder;
#[allow(clippy::module_name_repetitions)]
pub mod query_history;
#[allow(clippy::module_name_repetitions)]
pub use builder::QueryBuilder;

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

pub struct Query {
    df: Arc<crate::datafusion::DataFusion>,
    sql: String,
    query_id: Uuid,
    schema: Option<Arc<Schema>>,
    nsql: Option<String>,
    start_time: SystemTime,
    end_time: Option<SystemTime>,
    execution_time: Option<u64>,
    rows_produced: Option<u64>,
    results_cache_hit: Option<bool>,
    restricted_sql_options: Option<SQLOptions>,
}

impl Query {
    pub async fn run(self) -> Result<QueryResult> {
        let session = self.df.ctx.state();
        let plan = session
            .create_logical_plan(&self.sql)
            .await
            .context(UnableToExecuteQuerySnafu)?;

        if let Some(cache_provider) = &self.df.cache_provider {
            if let Some(cached_result) = cache_provider
                .get(&plan)
                .await
                .context(FailedToAccessCacheSnafu)?
            {
                let record_batch_stream = Box::pin(
                    MemoryStream::try_new(
                        cached_result.records.to_vec(),
                        cached_result.schema,
                        None,
                    )
                    .context(UnableToCreateMemoryStreamSnafu)?,
                );

                return Ok(QueryResult::new(
                    attach_query_context_to_stream(
                        self, // self.results_cache_hit(true),
                        record_batch_stream,
                    ),
                    Some(true),
                ));
            }
        }

        if let Some(restricted_sql_options) = self.restricted_sql_options {
            restricted_sql_options
                .verify_plan(&plan)
                .context(UnableToExecuteQuerySnafu)?;
        }

        let plan_copy = plan.clone();

        let df = self
            .df
            .ctx
            .execute_logical_plan(plan)
            .await
            .context(UnableToExecuteQuerySnafu)?;

        let df_schema: Arc<Schema> = df.schema().clone().into();

        let res_stream: SendableRecordBatchStream = df
            .execute_stream()
            .await
            .context(UnableToCollectResultsSnafu)?;

        let res_schema = res_stream.schema();

        verify_schema(df_schema.fields(), res_schema.fields()).context(SchemaMismatchSnafu)?;

        if is_cache_allowed_for_query(&plan_copy) {
            if let Some(cache_provider) = &self.df.cache_provider {
                let record_batch_stream = to_cached_record_batch_stream(
                    Arc::clone(cache_provider),
                    res_stream,
                    plan_copy,
                );

                return Ok(QueryResult::new(
                    attach_query_context_to_stream(
                        self, //self.results_cache_hit(false),
                        record_batch_stream,
                    ),
                    Some(false),
                ));
            }
        }

        Ok(QueryResult::new(
            attach_query_context_to_stream(self, res_stream),
            None,
        ))
    }

    #[must_use]
    fn finish(mut self) -> Self {
        if self.end_time.is_none() {
            self.end_time = Some(SystemTime::now());
        }

        if self.execution_time.is_none() {
            if let Some(execution_time) = self
                .end_time
                .and_then(|end_time| end_time.duration_since(self.start_time).ok())
                .and_then(|duration| u64::try_from(duration.as_millis()).ok())
            {
                self.execution_time = Some(execution_time);
            }
        }
        if self.results_cache_hit.is_none() {
            self.results_cache_hit = Some(false);
        }

        self
    }

    #[must_use]
    fn schema(mut self, schema: Arc<Schema>) -> Self {
        self.schema = Some(schema);
        self
    }

    #[must_use]
    fn rows_produced(mut self, rows_produced: u64) -> Self {
        self.rows_produced = Some(rows_produced);
        self
    }
}

#[must_use]
fn attach_query_context_to_stream(
    ctx: Query,
    mut stream: SendableRecordBatchStream,
) -> SendableRecordBatchStream {
    let schema = stream.schema();
    let schema_copy = Arc::clone(&schema);

    let mut num_records = 0u64;

    let updated_stream = stream! {
        while let Some(batch_result) = stream.next().await {
            if let Ok(batch) = &batch_result {
                num_records += batch.num_rows() as u64;
            }
            yield batch_result;
        }

        if let Err(e) = ctx
            .schema(schema_copy)
            .rows_produced(num_records)
            .finish()
            .write_query_history().await {
                tracing::error!("Error writing query history: {e}");
            }
    };

    Box::pin(RecordBatchStreamAdapter::new(
        schema,
        Box::pin(updated_stream),
    ))
}
