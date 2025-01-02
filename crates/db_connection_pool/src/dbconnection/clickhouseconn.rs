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

use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
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
    db: Arc<str>,
}

impl ClickhouseConnection {
    // We need to pass the pool to the connection so that we can get a new connection handle
    // This needs to be done because the query_owned consumes the connection handle
    pub fn new(conn: ClientHandle, pool: Arc<Pool>, db: Arc<str>) -> Self {
        Self {
            conn: Arc::new(Mutex::new(conn)),
            pool,
            db,
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

        let (database, table) = match table_reference {
            TableReference::Full { schema, table, .. }
            | TableReference::Partial { schema, table } => (schema, table),
            TableReference::Bare { table } => (&self.db, table),
        };

        let query = format!(
            "SELECT name, type FROM system.columns WHERE database = '{database}' AND table = '{table}'",
        );

        let block = conn
            .query(&query)
            .fetch_all()
            .await
            .boxed()
            .map_err(|e| dbconnection::Error::UnableToGetSchema { source: e })?;

        let fields = block
            .rows()
            .map(|row| {
                let name: String = row.get("name")?;
                let type_str: String = row.get("type")?;
                let data_type = map_clickhouse_type_to_arrow(&type_str)?;
                Ok(Field::new(name, data_type, true))
            })
            .collect::<Result<Vec<Field>, clickhouse_rs::errors::Error>>()
            .boxed()
            .map_err(|e| dbconnection::Error::UnableToGetSchema { source: e })?;

        Ok(Arc::new(Schema::new(fields)))
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

fn map_clickhouse_type_to_arrow(type_str: &str) -> Result<DataType, clickhouse_rs::errors::Error> {
    match type_str {
        "UUID" | "String" => Ok(DataType::Utf8),
        "Bool" => Ok(DataType::Boolean),
        "Int8" => Ok(DataType::Int8),
        "Int16" => Ok(DataType::Int16),
        "Int32" => Ok(DataType::Int32),
        "Int64" => Ok(DataType::Int64),
        "UInt8" => Ok(DataType::UInt8),
        "UInt16" => Ok(DataType::UInt16),
        "UInt32" => Ok(DataType::UInt32),
        "UInt64" => Ok(DataType::UInt64),
        "Float32" => Ok(DataType::Float32),
        "Float64" => Ok(DataType::Float64),
        s if s.starts_with("FixedString") => Ok(DataType::Utf8),
        "Date" => Ok(DataType::Date32),
        "DateTime" => Ok(DataType::Timestamp(TimeUnit::Second, None)),
        s if s.starts_with("Decimal") => {
            let parts: Vec<&str> = s
                .trim_start_matches("Decimal(")
                .trim_end_matches(')')
                .split(',')
                .collect();
            let (precision, scale) = match parts.len() {
                1 => (parts[0].trim().parse().unwrap_or(10), 0),
                2 => (
                    parts[0].trim().parse().unwrap_or(38),
                    parts[1].trim().parse().unwrap_or(0),
                ),
                _ => {
                    return Err(clickhouse_rs::errors::Error::Other(
                        format!("Invalid Decimal type: {type_str}").into(),
                    ))
                }
            };
            if precision <= 38 {
                Ok(DataType::Decimal128(precision, scale))
            } else if precision <= 76 {
                Ok(DataType::Decimal256(precision, scale))
            } else {
                Err(clickhouse_rs::errors::Error::Other(
                    format!("Unsupported Decimal precision: {precision}").into(),
                ))
            }
        }
        s if s.starts_with("Nullable") => {
            let inner_type = s.trim_start_matches("Nullable(").trim_end_matches(')');
            map_clickhouse_type_to_arrow(inner_type)
        }
        _ => Err(clickhouse_rs::errors::Error::Other(
            format!("Unsupported Clickhouse type: {type_str}").into(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;

    #[test]
    fn test_map_clickhouse_type_to_arrow() {
        let cases = vec![
            ("UUID", DataType::Utf8),
            ("Bool", DataType::Boolean),
            ("Int8", DataType::Int8),
            ("Int16", DataType::Int16),
            ("Int32", DataType::Int32),
            ("Int64", DataType::Int64),
            ("UInt8", DataType::UInt8),
            ("UInt16", DataType::UInt16),
            ("UInt32", DataType::UInt32),
            ("UInt64", DataType::UInt64),
            ("Float32", DataType::Float32),
            ("Float64", DataType::Float64),
            ("String", DataType::Utf8),
            ("FixedString(10)", DataType::Utf8),
            ("Date", DataType::Date32),
            ("DateTime", DataType::Timestamp(TimeUnit::Second, None)),
            ("Decimal(18, 4)", DataType::Decimal128(18, 4)),
            ("Decimal(18)", DataType::Decimal128(18, 0)),
            ("Decimal", DataType::Decimal128(10, 0)),
            ("Decimal(40, 10)", DataType::Decimal256(40, 10)),
            ("Nullable(Int32)", DataType::Int32),
        ];

        for (input, expected) in cases {
            let result = map_clickhouse_type_to_arrow(input).expect("valid for input");
            assert_eq!(result, expected, "Failed for input: {input}");
        }
    }

    #[test]
    fn test_map_clickhouse_type_to_arrow_invalid() {
        let invalid_cases = vec![
            "UnknownType",
            "Decimal(18, 4, 2)",
            "Nullable(UnknownType)",
            "Decimal(80)",
        ];

        for input in invalid_cases {
            let result = map_clickhouse_type_to_arrow(input);
            assert!(result.is_err(), "Expected error for input: {input}");
        }
    }
}
