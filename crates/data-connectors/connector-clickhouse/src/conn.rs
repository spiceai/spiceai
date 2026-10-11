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

use crate::block_to_arrow::{
    INT128_DATA_TYPE, block_to_arrow, datetime64_unit, list_data_type, map_data_type, tuple_field,
};
use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use async_stream::stream;
use clickhouse_rs::{Block, ClientHandle, Pool};
use datafusion::common::TableReference;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::EmptyRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_table_providers::sql::db_connection_pool::dbconnection::{
    self, AsyncDbConnection, DbConnection,
};
use futures::lock::Mutex;
use futures::{Stream, StreamExt};
use snafu::prelude::*;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to connect to ClickHouse: {source}"))]
    ConnectionPool {
        source: clickhouse_rs::errors::Error,
    },
    #[snafu(display("Failed to execute ClickHouse query: {source}"))]
    Query {
        source: clickhouse_rs::errors::Error,
    },
    #[snafu(display("Failed to convert query result to Arrow: {source}"))]
    Conversion {
        source: crate::block_to_arrow::Error,
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

impl<'a> DbConnection<ClientHandle, &'a dyn Sync> for ClickhouseConnection {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn as_async(&self) -> Option<&dyn AsyncDbConnection<ClientHandle, &'a dyn Sync>> {
        Some(self)
    }
}

// Clickhouse doesn't have a params in signature. So just setting to `dyn Sync`.
// Looks like we don't actually pass any params to query_arrow.
// But keep it in mind.
#[async_trait::async_trait]
impl<'a> AsyncDbConnection<ClientHandle, &'a dyn Sync> for ClickhouseConnection {
    // Required by trait, but not used.
    fn new(_: ClientHandle) -> Self {
        unreachable!()
    }

    async fn tables(&self, schema: &str) -> Result<Vec<String>, dbconnection::Error> {
        let mut conn = self.conn.lock().await;
        let conn = &mut *conn;

        // Escape single quotes by doubling them to prevent SQL injection
        let escaped_schema = schema.replace('\'', "''");
        let query = format!("SELECT name FROM system.tables WHERE database = '{escaped_schema}'");
        let block = conn
            .query(&query)
            .fetch_all()
            .await
            .boxed()
            .map_err(|e| dbconnection::Error::UnableToGetTables { source: e })?;

        let tables = block
            .rows()
            .map(|row| row.get::<String, _>("name"))
            .collect::<Result<Vec<String>, clickhouse_rs::errors::Error>>()
            .boxed()
            .map_err(|e| dbconnection::Error::UnableToGetTables { source: e })?;

        Ok(tables)
    }

    async fn schemas(&self) -> Result<Vec<String>, dbconnection::Error> {
        let mut conn = self.conn.lock().await;
        let conn = &mut *conn;

        let query = "SELECT name FROM system.databases WHERE name NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')";
        let block = conn
            .query(query)
            .fetch_all()
            .await
            .boxed()
            .map_err(|e| dbconnection::Error::UnableToGetSchemas { source: e })?;

        let schemas = block
            .rows()
            .map(|row| row.get::<String, _>("name"))
            .collect::<Result<Vec<String>, clickhouse_rs::errors::Error>>()
            .boxed()
            .map_err(|e| dbconnection::Error::UnableToGetSchemas { source: e })?;

        Ok(schemas)
    }

    async fn get_schema(
        &self,
        table_reference: &TableReference,
    ) -> Result<SchemaRef, dbconnection::Error> {
        let mut conn = self.conn.lock().await;
        let conn = &mut *conn;

        let (database, table) = match table_reference {
            TableReference::Full { schema, table, .. }
            | TableReference::Partial { schema, table } => (schema.as_ref(), table.as_ref()),
            TableReference::Bare { table } => (self.db.as_ref(), table.as_ref()),
        };

        // Escape single quotes by doubling them to prevent SQL injection
        let escaped_database = database.replace('\'', "''");
        let escaped_table = table.replace('\'', "''");
        let query = format!(
            "SELECT name, type FROM system.columns WHERE database = '{escaped_database}' AND table = '{escaped_table}'",
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
        _: &[&'a dyn Sync],
        projected_schema: Option<SchemaRef>,
    ) -> Result<SendableRecordBatchStream, Box<dyn std::error::Error + Send + Sync>> {
        let conn = self.pool.get_handle().await.context(ConnectionPoolSnafu)?;
        let mut block_stream = conn.query_owned(sql).stream_blocks();
        let first_block = block_stream.next().await;
        if first_block.is_none() {
            return Ok(empty_result_stream(projected_schema));
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
        _: &[&'a dyn Sync],
    ) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        let mut conn = self.conn.lock().await;
        let conn = &mut *conn;
        conn.execute(query).await.context(QuerySnafu)?;
        // Clickhouse driver doesn't return number of rows affected.
        // Shouldn't be an issue for now since we don't have a data accelerator for now.
        Ok(0)
    }
}

/// The stream for a result set the server answered with no blocks.
///
/// Every other schema on this path is read off the first block, so when there is
/// no block the schema has to come from the projected schema the plan was built
/// from. Falling back to [`Schema::empty`] would hand back a stream whose schema
/// contradicts the columns the query selected.
fn empty_result_stream(projected_schema: Option<SchemaRef>) -> SendableRecordBatchStream {
    let schema = projected_schema.unwrap_or_else(|| Arc::new(Schema::empty()));
    Box::pin(EmptyRecordBatchStream::new(schema))
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

/// Maps a column type, as `system.columns` names it, to the Arrow type `block_to_arrow`
/// reads it as.
fn map_clickhouse_type_to_arrow(type_str: &str) -> Result<DataType, clickhouse_rs::errors::Error> {
    map_type(type_str, false)
}

/// `nested` is true inside an `Array`, `Map` or `Tuple`, where the driver cannot decode a
/// `LowCardinality` column.
fn map_type(type_str: &str, nested: bool) -> Result<DataType, clickhouse_rs::errors::Error> {
    let type_str = type_str.trim();
    let (name, args) = match type_str.split_once('(') {
        Some((name, rest)) => (name, rest.strip_suffix(')')),
        None => (type_str, None),
    };
    match (name, args) {
        ("UUID" | "String" | "IPv4" | "IPv6", None)
        | ("FixedString" | "Enum8" | "Enum16", Some(_)) => Ok(DataType::Utf8),
        ("Bool", None) => Ok(DataType::Boolean),
        ("Int8", None) => Ok(DataType::Int8),
        ("Int16", None) => Ok(DataType::Int16),
        ("Int32", None) => Ok(DataType::Int32),
        ("Int64", None) => Ok(DataType::Int64),
        ("UInt8", None) => Ok(DataType::UInt8),
        ("UInt16", None) => Ok(DataType::UInt16),
        ("UInt32", None) => Ok(DataType::UInt32),
        ("UInt64", None) => Ok(DataType::UInt64),
        ("Int128" | "UInt128", None) => Ok(INT128_DATA_TYPE),
        ("Float32", None) => Ok(DataType::Float32),
        ("Float64", None) => Ok(DataType::Float64),
        ("Date" | "Date32", None) => Ok(DataType::Date32),
        ("DateTime", _) => Ok(DataType::Timestamp(TimeUnit::Second, None)),
        ("DateTime64", Some(args)) => {
            let precision = top_level_split(args, ',')
                .first()
                .and_then(|precision| precision.trim().parse().ok())
                .ok_or_else(|| unsupported(type_str))?;
            Ok(DataType::Timestamp(datetime64_unit(precision), None))
        }
        ("LowCardinality", Some(_)) if nested => Err(unsupported(&format!(
            "{type_str} inside an Array, Map or Tuple"
        ))),
        ("Nullable" | "LowCardinality", Some(inner)) => map_type(inner, nested),
        ("Array", Some(inner)) => Ok(list_data_type(map_type(inner, true)?)),
        ("Map", Some(args)) => match top_level_split(args, ',')[..] {
            [key, value] => Ok(map_data_type(map_type(key, true)?, map_type(value, true)?)),
            _ => Err(unsupported(type_str)),
        },
        ("Tuple", Some(args)) => {
            let fields = top_level_split(args, ',')
                .into_iter()
                .enumerate()
                .map(|(index, element)| {
                    let element = element.trim();
                    // A type has no whitespace outside its brackets, so a space there ends
                    // the element's name.
                    let (name, element_type) = match top_level_split(element, ' ')[..] {
                        [name, ref element_type @ ..] if !element_type.is_empty() => {
                            (Some(name.trim_matches('`')), &element[name.len()..])
                        }
                        _ => (None, element),
                    };
                    Ok(tuple_field(index, name, map_type(element_type, true)?))
                })
                .collect::<Result<Vec<Field>, clickhouse_rs::errors::Error>>()?;
            Ok(DataType::Struct(fields.into()))
        }
        _ if type_str.starts_with("Decimal") => map_decimal(type_str),
        _ => Err(unsupported(type_str)),
    }
}

fn unsupported(type_str: &str) -> clickhouse_rs::errors::Error {
    clickhouse_rs::errors::Error::Other(format!("Unsupported Clickhouse type: {type_str}").into())
}

/// Splits `source` on `separator` where it is outside brackets, quoted strings and quoted names.
fn top_level_split(source: &str, separator: char) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut start = 0;
    let mut depth = 0_usize;
    let mut quote = None;
    let mut escaped = false;
    for (offset, c) in source.char_indices() {
        if let Some(open) = quote {
            if escaped {
                escaped = false;
            } else if c == '\\' {
                escaped = true;
            } else if c == open {
                quote = None;
            }
            continue;
        }
        match c {
            '\'' | '`' => quote = Some(c),
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            c if c == separator && depth == 0 => {
                parts.push(&source[start..offset]);
                start = offset + c.len_utf8();
            }
            _ => {}
        }
    }
    parts.push(&source[start..]);
    parts
}

fn map_decimal(type_str: &str) -> Result<DataType, clickhouse_rs::errors::Error> {
    let parts: Vec<&str> = type_str
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
            ));
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;

    /// Regression test for #13015: a query the server answers with no blocks
    /// must still carry the projected schema, so an empty result is an empty
    /// table with the columns the query selected rather than no columns at all.
    #[test]
    fn empty_result_stream_keeps_the_projected_schema() {
        let projected: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]));

        let mut stream = empty_result_stream(Some(Arc::clone(&projected)));

        assert_eq!(
            stream.schema().as_ref(),
            projected.as_ref(),
            "an empty result must carry the projected schema"
        );
        assert!(
            futures::executor::block_on(stream.next()).is_none(),
            "an empty result must not yield a batch"
        );
    }

    #[test]
    fn empty_result_stream_without_a_projection_has_no_columns() {
        let stream = empty_result_stream(None);

        assert_eq!(
            stream.schema().fields().len(),
            0,
            "with no projected schema there is nothing to preserve"
        );
    }

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
            ("Date32", DataType::Date32),
            ("Nullable(Date32)", DataType::Date32),
            ("DateTime", DataType::Timestamp(TimeUnit::Second, None)),
            ("Decimal(18, 4)", DataType::Decimal128(18, 4)),
            ("Decimal(18)", DataType::Decimal128(18, 0)),
            ("Decimal", DataType::Decimal128(10, 0)),
            ("Decimal(40, 10)", DataType::Decimal256(40, 10)),
            ("Nullable(Int32)", DataType::Int32),
            ("LowCardinality(String)", DataType::Utf8),
            ("LowCardinality(Nullable(String))", DataType::Utf8),
            (
                "DateTime64(3)",
                DataType::Timestamp(TimeUnit::Millisecond, None),
            ),
            (
                "DateTime64(6, 'Asia/Tokyo')",
                DataType::Timestamp(TimeUnit::Microsecond, None),
            ),
            (
                "Nullable(DateTime64(9))",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
            ),
            ("DateTime64(0)", DataType::Timestamp(TimeUnit::Second, None)),
            (
                "DateTime('UTC')",
                DataType::Timestamp(TimeUnit::Second, None),
            ),
            (
                "Array(Int32)",
                DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true))),
            ),
            (
                "Array(Array(Nullable(String)))",
                DataType::List(Arc::new(Field::new_list_field(
                    DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true))),
                    true,
                ))),
            ),
            (
                "Map(String, Int32)",
                DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(
                            vec![
                                Field::new("keys", DataType::Utf8, false),
                                Field::new("values", DataType::Int32, true),
                            ]
                            .into(),
                        ),
                        false,
                    )),
                    false,
                ),
            ),
            (
                "Tuple(Int32, String)",
                DataType::Struct(
                    vec![
                        Field::new("1", DataType::Int32, true),
                        Field::new("2", DataType::Utf8, true),
                    ]
                    .into(),
                ),
            ),
            (
                "Tuple(id Int32, `a label` Nullable(String), at DateTime64(3, 'UTC'))",
                DataType::Struct(
                    vec![
                        Field::new("id", DataType::Int32, true),
                        Field::new("a label", DataType::Utf8, true),
                        Field::new("at", DataType::Timestamp(TimeUnit::Millisecond, None), true),
                    ]
                    .into(),
                ),
            ),
            ("Enum8('a' = 1, 'b' = 2)", DataType::Utf8),
            ("Enum16('a, (b)' = 1000)", DataType::Utf8),
            ("UInt128", DataType::Decimal256(39, 0)),
            ("Int128", DataType::Decimal256(39, 0)),
            ("IPv4", DataType::Utf8),
            ("Nullable(IPv6)", DataType::Utf8),
        ];

        for (input, expected) in cases {
            let result = map_clickhouse_type_to_arrow(input).expect("valid for input");
            assert_eq!(result, expected, "Failed for input: {input}");
        }
    }

    #[test]
    fn test_map_clickhouse_type_to_arrow_invalid() {
        // Each input reaches its own refusal, and the message names what was refused; a
        // `Nullable` wrapper is unwrapped first, so its inner type is the one named.
        for (input, refusal) in [
            ("UnknownType", "Unsupported Clickhouse type: UnknownType"),
            (
                "Decimal(18, 4, 2)",
                "Invalid Decimal type: Decimal(18, 4, 2)",
            ),
            (
                "Nullable(UnknownType)",
                "Unsupported Clickhouse type: UnknownType",
            ),
            ("Decimal(80)", "Unsupported Decimal precision: 80"),
            (
                "Array(LowCardinality(String))",
                "Unsupported Clickhouse type: LowCardinality(String) inside an Array, Map or Tuple",
            ),
            (
                "Map(LowCardinality(String), Int32)",
                "Unsupported Clickhouse type: LowCardinality(String) inside an Array, Map or Tuple",
            ),
            ("Map(String)", "Unsupported Clickhouse type: Map(String)"),
            ("DateTime64", "Unsupported Clickhouse type: DateTime64"),
        ] {
            let err = map_clickhouse_type_to_arrow(input)
                .expect_err("an invalid ClickHouse type must be refused");
            assert!(
                matches!(&err, clickhouse_rs::errors::Error::Other(message) if message == refusal),
                "{input}: {err:?}"
            );
        }
    }

    /// A dataset's schema is mapped from the type names `system.columns` reports, and its
    /// query results from the types the driver decodes; the two must agree, or every
    /// batch contradicts the schema it was planned with.
    #[test]
    fn a_type_name_maps_to_the_type_its_decoded_column_converts_to() {
        use clickhouse_rs::{row, types::Value};
        use std::collections::HashMap;

        let mut block = clickhouse_rs::Block::new();
        block
            .push(row! {
                datetime64: Value::DateTime64(0, (3, chrono_tz::Tz::UTC)),
                array: Value::from(vec![1_i32]),
                map: Value::from(HashMap::from([("k".to_string(), 1_i32)])),
                tuple: Value::Tuple(Arc::new(vec![Value::Int32(1), Value::from("x")])),
                uint128: Value::UInt128(1),
                ipv4: Value::Ipv4([1, 2, 3, 4]),
                enum8: Value::Enum8(vec![("a".to_string(), 1)], clickhouse_rs::types::Enum8::of(1)),
            })
            .expect("the row matches the block");
        let batch = block_to_arrow(&block).expect("the block converts to Arrow");

        for (field, type_name) in batch.schema().fields().iter().zip([
            "DateTime64(3, 'UTC')",
            "Array(Int32)",
            "Map(String, Int32)",
            "Tuple(Int32, String)",
            "UInt128",
            "IPv4",
            "Enum8('a' = 1)",
        ]) {
            assert_eq!(
                &map_clickhouse_type_to_arrow(type_name).expect("a supported type"),
                field.data_type(),
                "{type_name}"
            );
        }
    }
}
