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
use std::pin::Pin;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::{Schema, SchemaRef};
use arrow_sql_gen::clickhouse::block_to_arrow;
use async_stream::stream;
use clickhouse_rs::{Block, ClientHandle, Pool};
use datafusion::common::TableReference;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_table_providers::sql::db_connection_pool::dbconnection::{
    self, AsyncDbConnection, DbConnection,
};
use futures::lock::Mutex;
use futures::{stream, Stream, StreamExt};
use snafu::prelude::*;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("ConnectionPoolError: {source}"))]
    ConnectionPoolError {
        source: clickhouse_rs::errors::Error,
    },
    #[snafu(display("{source}"))]
    QueryError {
        source: clickhouse_rs::errors::Error,
    },
    #[snafu(display("Failed to convert query result to Arrow: {source}"))]
    ConversionError {
        source: arrow_sql_gen::clickhouse::Error,
    },
}

pub struct ClickhouseConnection {
    pub conn: Arc<Mutex<ClientHandle>>,
    pool: Arc<Pool>,
}

impl ClickhouseConnection {
    // We need to pass the pool to the connection so that we can get a new connection handle
    // This needs to be done because the query_owned consumes the connection handle
    pub fn new(conn: ClientHandle, pool: Arc<Pool>) -> Self {
        Self {
            conn: Arc::new(Mutex::new(conn)),
            pool,
        }
    }
}

impl<'a> DbConnection<ClientHandle, &'a (dyn Sync)> for ClickhouseConnection {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn as_async(&self) -> Option<&dyn AsyncDbConnection<ClientHandle, &'a (dyn Sync)>> {
        Some(self)
    }
}

// Clickhouse doesn't have a params in signature. So just setting to `dyn Sync`.
// Looks like we don't actually pass any params to query_arrow.
// But keep it in mind.
#[async_trait::async_trait]
impl<'a> AsyncDbConnection<ClientHandle, &'a (dyn Sync)> for ClickhouseConnection {
    // Required by trait, but not used.
    fn new(_: ClientHandle) -> Self {
        unreachable!()
    }

    async fn get_schema(
        &self,
        table_reference: &TableReference,
    ) -> Result<SchemaRef, dbconnection::Error> {
        let mut conn = self.conn.lock().await;
        let conn = &mut *conn;
        let block = conn
            .query(&format!(
                "SELECT * FROM {} LIMIT 1",
                table_reference.to_quoted_string()
            ))
            .fetch_all()
            .await
            .boxed()
            .map_err(|e| dbconnection::Error::UnableToGetSchema { source: e })?;

        let rec = block_to_arrow(&block)
            .boxed()
            .map_err(|e| dbconnection::Error::UnableToGetSchema { source: e })?;

        Ok(rec.schema())
    }

    async fn query_arrow(
        &self,
        sql: &str,
        _: &[&'a (dyn Sync)],
        _projected_schema: Option<SchemaRef>,
    ) -> Result<SendableRecordBatchStream, Box<dyn std::error::Error + Send + Sync>> {
        let conn = self.pool.get_handle().await.context(ConnectionPoolSnafu)?;
        let mut block_stream = conn.query_owned(sql).stream_blocks();
        let first_block = block_stream.next().await;
        if first_block.is_none() {
            return Ok(Box::pin(RecordBatchStreamAdapter::new(
                Arc::new(Schema::empty()),
                stream::empty(),
            )));
        }
        let first_block = first_block
            .unwrap_or(Ok(Block::new()))
            .context(QuerySnafu)?;
        let rec = block_to_arrow(&first_block).context(ConversionSnafu)?;
        let schema = rec.schema();

        let stream_adapter =
            RecordBatchStreamAdapter::new(schema, query_to_stream(rec, block_stream));

        Ok(Box::pin(stream_adapter))
    }

    async fn execute(
        &self,
        query: &str,
        _: &[&'a (dyn Sync)],
    ) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        let mut conn = self.conn.lock().await;
        let conn = &mut *conn;
        conn.execute(query).await.context(QuerySnafu)?;
        // Clickhouse driver doesn't return number of rows affected.
        // Shouldn't be an issue for now since we don't have a data accelerator for now.
        Ok(0)
    }
}

fn query_to_stream(
    first_batch: RecordBatch,
    mut block_stream: Pin<
        Box<dyn Stream<Item = Result<Block, clickhouse_rs::errors::Error>> + Send>,
    >,
) -> impl Stream<Item = datafusion::common::Result<RecordBatch>> {
    stream! {
       yield Ok(first_batch);
       while let Some(block) = block_stream.next().await {
            match block {
                Ok(block) => {
                    let rec = block_to_arrow(&block);
                    match rec {
                        Ok(rec) => {
                            yield Ok(rec);
                        }
                        Err(e) => {
                            yield Err(DataFusionError::Execution(format!("Failed to convert query result to Arrow: {e}")));
                        }
                    }
                }
                Err(e) => {
                    yield Err(DataFusionError::Execution(format!("Failed to fetch block: {e}")));
                }
            }
       }
    }
}
