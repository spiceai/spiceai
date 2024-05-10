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

use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::{Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::sql::TableReference;
use futures::stream;
use futures::StreamExt;
use futures::TryStreamExt;
use snafu::prelude::*;
use snowflake_api::SnowflakeApi;

use super::AsyncDbConnection;
use super::DbConnection;
use super::Result;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Not implemented"))]
    NotImplemented,

    #[snafu(display("Unable to retrieve schema: {reason}"))]
    UnableToRetrieveSchema { reason: String },

    #[snafu(display("Unexpected query response, expected Arrow, got JSON: {json}"))]
    UnexpectedResponse { json: String },

    #[snafu(display("Error executing query: {source}"))]
    SnowflakeQueryError {
        source: snowflake_api::SnowflakeApiError,
    },

    #[snafu(display("Error executing query: {source}"))]
    SnowflakeArrowError { source: arrow::error::ArrowError },
}

pub struct SnowflakeConnection {
    pub api: Arc<SnowflakeApi>,
}

impl<'a> DbConnection<Arc<SnowflakeApi>, &'a (dyn Sync)> for SnowflakeConnection {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn as_async(&self) -> Option<&dyn super::AsyncDbConnection<Arc<SnowflakeApi>, &'a (dyn Sync)>> {
        Some(self)
    }
}

#[async_trait]
impl<'a> AsyncDbConnection<Arc<SnowflakeApi>, &'a (dyn Sync)> for SnowflakeConnection {
    fn new(api: Arc<SnowflakeApi>) -> Self {
        SnowflakeConnection { api }
    }

    async fn get_schema(
        &self,
        table_reference: &TableReference,
    ) -> Result<SchemaRef, super::Error> {
        let table = table_reference.to_quoted_string();

        let res = self
            .api
            .exec(format!("SELECT * FROM {table} limit 1").as_str())
            .await
            .boxed()
            .context(super::UnableToGetSchemaSnafu)?;

        match res {
            snowflake_api::QueryResult::Arrow(record_batches) => {
                let schema = record_batches[0].schema();
                return Ok(Arc::clone(&schema));
            }
            snowflake_api::QueryResult::Empty => Err(super::Error::UnableToGetSchema {
                source: "Empty response".to_string().into(),
            }),
            snowflake_api::QueryResult::Json(_json) => Err(super::Error::UnableToGetSchema {
                source: "Unexpected response".to_string().into(),
            }),
        }
    }

    async fn query_arrow(
        &self,
        sql: &str,
        _: &[&'a (dyn Sync)],
    ) -> Result<SendableRecordBatchStream> {
        let sql = sql.to_string();

        let mut stream = self
            .api
            .exec_streamed(&sql)
            .await
            .context(SnowflakeQuerySnafu)?;

        let Some(first_batch) = stream.next().await else {
            return Ok(Box::pin(RecordBatchStreamAdapter::new(
                Arc::new(Schema::empty()),
                stream::empty(),
            )));
        };

        let batch = first_batch.context(SnowflakeArrowSnafu)?;
        let schema = batch.schema();

        // add first batch back to stream
        let run_once = stream::once(async move { Ok(batch) });
        let stream_adapter = RecordBatchStreamAdapter::new(
            schema,
            Box::pin(run_once.chain(stream).map_err(to_execution_error)),
        );

        return Ok(Box::pin(stream_adapter));
    }

    async fn execute(&self, _query: &str, _: &[&'a (dyn Sync)]) -> Result<u64> {
        return NotImplementedSnafu.fail()?;
    }
}

fn to_execution_error(e: impl Into<Box<dyn std::error::Error>>) -> DataFusionError {
    DataFusionError::Execution(format!("{}", e.into()).to_string())
}
