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
use std::collections::HashMap;
use std::io::Cursor;
use std::sync::{Arc, LazyLock};
use std::time::Instant;

use arrow::array::{
    Array, ArrayRef, AsArray, Decimal128Array, Int32Array, Int64Array, PrimitiveArray, RecordBatch,
    StringArray, StructArray, TimestampNanosecondArray,
};
use arrow::datatypes::{
    ArrowPrimitiveType, DataType, Field, Int8Type, Int16Type, Int32Type, Int64Type, Schema,
    SchemaRef, TimeUnit,
};
use arrow::error::ArrowError;
use arrow::temporal_conversions::NANOSECONDS;
use async_trait::async_trait;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::sql::TableReference;
use datafusion_table_providers::sql::db_connection_pool::dbconnection::{
    self, AsyncDbConnection, DbConnection,
};
use futures::StreamExt;
use futures::TryStreamExt;
use futures::stream;
use snafu::prelude::*;
use snowflake_api::SnowflakeApi;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("This Snowflake operation is not implemented"))]
    NotImplemented,

    #[snafu(display("Unable to retrieve schema: {reason}"))]
    UnableToRetrieveSchema { reason: String },

    #[snafu(display("Unexpected query response, expected Arrow, got JSON: {json}"))]
    UnexpectedResponse { json: String },

    #[snafu(display("Error executing query: {source}"))]
    SnowflakeQueryError {
        source: snowflake_api::SnowflakeApiError,
    },

    #[snafu(display("Error reading Snowflake Arrow response: {source}"))]
    SnowflakeSourceArrowError { source: snowflake_api::ArrowError },

    #[snafu(display("Failed to serialize Snowflake Arrow response: {source}"))]
    SnowflakeArrowSerializeError { source: snowflake_api::ArrowError },

    #[snafu(display("Failed to deserialize Snowflake Arrow response: {source}"))]
    SnowflakeArrowDeserializeError { source: arrow::error::ArrowError },

    #[snafu(display("Failed to convert Snowflake timestamp value: {reason}"))]
    UnableToCastSnowflakeTimestamp { reason: String },

    #[snafu(display("Failed to convert Snowflake numeric value to decimal: {source}"))]
    UnableToCastSnowflakeNumericToDecimal { source: arrow::error::ArrowError },

    #[snafu(display("Failed to process Snowflake query result: {source}"))]
    FailedToCreateRecordBatch { source: arrow::error::ArrowError },
}

static UTC_TIMEZONE: LazyLock<Arc<str>> = LazyLock::new(|| Arc::from("UTC"));
const DESCRIPTION_METADATA_KEY: &str = "description";
const SOURCE_TYPE_METADATA_KEY: &str = "source_type";

pub struct SnowflakeConnection {
    pub api: Arc<SnowflakeApi>,
}

impl<'a> DbConnection<Arc<SnowflakeApi>, &'a dyn Sync> for SnowflakeConnection {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn as_async(&self) -> Option<&dyn AsyncDbConnection<Arc<SnowflakeApi>, &'a dyn Sync>> {
        Some(self)
    }
}

#[async_trait]
impl<'a> AsyncDbConnection<Arc<SnowflakeApi>, &'a dyn Sync> for SnowflakeConnection {
    fn new(api: Arc<SnowflakeApi>) -> Self {
        SnowflakeConnection { api }
    }

    async fn tables(&self, schema: &str) -> Result<Vec<String>, dbconnection::Error> {
        let start = Instant::now();
        // Quote the identifier to prevent SQL injection and handle special characters
        let escaped_schema = schema.replace('"', "\"\"");
        let query = format!("SHOW TABLES IN SCHEMA \"{escaped_schema}\"");
        tracing::debug!(query = %query, "Snowflake: listing tables");
        let res =
            self.api
                .exec(&query)
                .await
                .map_err(|e| dbconnection::Error::UnableToGetTables {
                    source: e.to_string().into(),
                })?;

        let result = match res {
            snowflake_api::QueryResult::Arrow(batches) => {
                names_from_snowflake_arrow_batches(batches, tables_error)
            }
            snowflake_api::QueryResult::Json(resp) => {
                names_from_json_rows(&resp.value, tables_error)
            }
            snowflake_api::QueryResult::Empty => Ok(Vec::new()),
        };
        tracing::debug!(duration_ms = start.elapsed().as_millis(), schema = %schema, count = result.as_ref().map_or(0, Vec::len), "Snowflake: listed tables");
        result
    }

    async fn schemas(&self) -> Result<Vec<String>, dbconnection::Error> {
        let start = Instant::now();
        let query = "SHOW SCHEMAS";
        tracing::debug!(query = %query, "Snowflake: listing schemas");
        let res =
            self.api
                .exec(query)
                .await
                .map_err(|e| dbconnection::Error::UnableToGetSchemas {
                    source: e.to_string().into(),
                })?;

        let result = match res {
            snowflake_api::QueryResult::Arrow(batches) => {
                names_from_snowflake_arrow_batches(batches, schemas_error)
            }
            snowflake_api::QueryResult::Json(resp) => {
                names_from_json_rows(&resp.value, schemas_error)
            }
            snowflake_api::QueryResult::Empty => Ok(Vec::new()),
        };
        tracing::debug!(
            duration_ms = start.elapsed().as_millis(),
            count = result.as_ref().map_or(0, Vec::len),
            "Snowflake: listed schemas"
        );
        result
    }

    async fn get_schema(
        &self,
        table_reference: &TableReference,
    ) -> Result<SchemaRef, dbconnection::Error> {
        let start = Instant::now();
        let table = table_reference.to_quoted_string();
        let query = format!("SHOW COLUMNS IN {table}");
        tracing::debug!(query = %query, "Snowflake: fetching schema");

        let res =
            self.api
                .exec(&query)
                .await
                .map_err(|e| dbconnection::Error::UnableToGetSchema {
                    source: e.to_string().into(),
                })?;

        let result = match res {
            snowflake_api::QueryResult::Json(resp) => {
                parse_schema_from_json(&resp.value).map_err(|e| {
                    dbconnection::Error::UnableToGetSchema {
                        source: e.to_string().into(),
                    }
                })
            }
            snowflake_api::QueryResult::Arrow(_) => Err(dbconnection::Error::UnableToGetSchema {
                source: "Unexpected Arrow response".to_string().into(),
            }),
            snowflake_api::QueryResult::Empty => Err(dbconnection::Error::UnableToGetSchema {
                source: "Empty response".to_string().into(),
            }),
        };
        tracing::debug!(duration_ms = start.elapsed().as_millis(), table = %table, "Snowflake: fetched schema");
        result
    }

    async fn query_arrow(
        &self,
        sql: &str,
        _: &[&'a dyn Sync],
        _projected_schema: Option<SchemaRef>,
    ) -> Result<SendableRecordBatchStream, Box<dyn std::error::Error + Send + Sync>> {
        let start = Instant::now();
        tracing::debug!("Snowflake: executing query");

        let stream = self
            .api
            .exec_streamed(sql)
            .await
            .context(SnowflakeQuerySnafu)?;

        tracing::debug!(
            duration_ms = start.elapsed().as_millis(),
            "Snowflake: query stream initiated"
        );

        let mut transformed_stream = stream.map(|batch| {
            let batch = batch.context(SnowflakeSourceArrowSnafu)?;
            let batch = snowflake_record_batch_to_arrow(&batch)?;
            snowflake_schema_cast(&batch)
        });

        let Some(first_batch) = transformed_stream.next().await else {
            return Ok(Box::pin(RecordBatchStreamAdapter::new(
                Arc::new(Schema::empty()),
                stream::empty(),
            )));
        };

        let batch = first_batch?;

        let schema = batch.schema();

        // add first batch back to stream
        let run_once = stream::once(async move { Ok(batch) });
        let stream_adapter = RecordBatchStreamAdapter::new(
            schema,
            Box::pin(
                run_once
                    .chain(transformed_stream)
                    .map_err(to_execution_error),
            ),
        );

        return Ok(Box::pin(stream_adapter));
    }

    async fn execute(
        &self,
        _query: &str,
        _: &[&'a dyn Sync],
    ) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        return NotImplementedSnafu.fail()?;
    }
}

fn to_execution_error(e: impl Into<Box<dyn std::error::Error>>) -> DataFusionError {
    DataFusionError::Execution(format!("{}", e.into()))
}

fn names_from_arrow_batches(
    batches: Vec<RecordBatch>,
    make_error: fn(String) -> dbconnection::Error,
) -> Result<Vec<String>, dbconnection::Error> {
    let mut names = Vec::new();

    for batch in batches {
        let name_column = batch
            .column_by_name("name")
            .ok_or_else(|| make_error("Arrow response missing 'name' column".to_string()))?;
        let array = name_column
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| make_error("'name' column is not a StringArray".to_string()))?;
        names.extend(array.iter().flatten().map(ToString::to_string));
    }

    Ok(names)
}

fn names_from_snowflake_arrow_batches(
    batches: Vec<snowflake_api::RecordBatch>,
    make_error: fn(String) -> dbconnection::Error,
) -> Result<Vec<String>, dbconnection::Error> {
    let batches = snowflake_batches_to_arrow(batches).map_err(|e| make_error(e.to_string()))?;
    names_from_arrow_batches(batches, make_error)
}

fn snowflake_batches_to_arrow(
    batches: Vec<snowflake_api::RecordBatch>,
) -> Result<Vec<RecordBatch>, Error> {
    batches
        .iter()
        .map(snowflake_record_batch_to_arrow)
        .collect()
}

fn snowflake_record_batch_to_arrow(
    record_batch: &snowflake_api::RecordBatch,
) -> Result<RecordBatch, Error> {
    let mut buffer = Vec::new();
    let schema = record_batch.schema();
    {
        let mut writer =
            source_arrow_ipc::writer::StreamWriter::try_new(&mut buffer, schema.as_ref())
                .context(SnowflakeArrowSerializeSnafu)?;
        writer
            .write(record_batch)
            .context(SnowflakeArrowSerializeSnafu)?;
        writer.finish().context(SnowflakeArrowSerializeSnafu)?;
    }

    let mut reader = arrow::ipc::reader::StreamReader::try_new(Cursor::new(buffer), None)
        .context(SnowflakeArrowDeserializeSnafu)?;
    let Some(batch) = reader.next() else {
        return Err(Error::FailedToCreateRecordBatch {
            source: arrow::error::ArrowError::ParseError(
                "Snowflake Arrow IPC stream did not contain a record batch".to_string(),
            ),
        });
    };

    batch.context(SnowflakeArrowDeserializeSnafu)
}

fn tables_error(msg: String) -> dbconnection::Error {
    dbconnection::Error::UnableToGetTables { source: msg.into() }
}

fn schemas_error(msg: String) -> dbconnection::Error {
    dbconnection::Error::UnableToGetSchemas { source: msg.into() }
}

/// Extracts names from a Snowflake JSON response.
///
/// Supports two formats:
/// - Array of objects with a `"name"` field (e.g., `[{"name": "foo"}, ...]`)
/// - Array of arrays where the second element (index 1) is the name
///   (matching the positional format used by `SHOW COLUMNS`)
fn names_from_json_rows(
    value: &serde_json::Value,
    make_error: fn(String) -> dbconnection::Error,
) -> Result<Vec<String>, dbconnection::Error> {
    let rows = value
        .as_array()
        .ok_or_else(|| make_error("Expected array response".to_string()))?;
    let mut names = Vec::with_capacity(rows.len());
    for row in rows {
        let name = if let Some(obj) = row.as_object() {
            // Object format: {"name": "table_name", ...}
            obj.get("name")
                .and_then(|v| v.as_str())
                .map(ToString::to_string)
        } else if let Some(arr) = row.as_array() {
            // Array format: [_, "table_name", ...] (name at index 1)
            arr.get(1).and_then(|v| v.as_str()).map(ToString::to_string)
        } else {
            None
        };
        names.push(name.ok_or_else(|| make_error("Row missing valid 'name' field".to_string()))?);
    }
    Ok(names)
}

/// Converts `Snowflake` specific types to standard Arrow types.
///
/// # Errors
///
/// Returns an error if there is a failure in converting Snowflake to Arrow types.
pub fn snowflake_schema_cast(record_batch: &RecordBatch) -> Result<RecordBatch, Error> {
    let mut fields = Vec::new();
    let mut columns = Vec::new();

    for (idx, field) in record_batch.schema().fields().iter().enumerate() {
        let column = record_batch.column(idx);
        let field_metadata = field.metadata();
        if let Some(sf_logical_type) = field_metadata.get("logicalType") {
            match sf_logical_type.to_lowercase().as_str() {
                "timestamp_ntz" | "timestamp_ltz" => {
                    fields.push(Arc::new(Field::new(
                        field.name(),
                        DataType::Timestamp(TimeUnit::Nanosecond, None),
                        field.is_nullable(),
                    )));
                    columns.push(cast_sf_timestamp_to_arrow_timestamp(column, false)?);
                    continue;
                }
                "timestamp_tz" => {
                    fields.push(Arc::new(Field::new(
                        field.name(),
                        DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                        field.is_nullable(),
                    )));
                    columns.push(cast_sf_timestamp_to_arrow_timestamp(column, true)?);
                    continue;
                }
                "fixed"
                    if !matches!(
                        field.data_type(),
                        DataType::Decimal128(_, _) | DataType::Decimal256(_, _)
                    ) =>
                {
                    if let (Some(precision_str), Some(scale_str)) =
                        (field_metadata.get("precision"), field_metadata.get("scale"))
                        && let (Ok(precision), Ok(scale)) =
                            (precision_str.parse::<u8>(), scale_str.parse::<i8>())
                    {
                        fields.push(Arc::new(Field::new(
                            field.name(),
                            DataType::Decimal128(precision, scale),
                            field.is_nullable(),
                        )));

                        columns.push(cast_sf_fixed_point_number_to_decimal(
                            column, precision, scale,
                        )?);
                        continue;
                    }
                }
                _ => {}
            }
        }
        fields.push(Arc::clone(field));
        columns.push(Arc::clone(column));
    }

    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, columns).context(FailedToCreateRecordBatchSnafu)
}

fn cast_sf_timestamp_to_arrow_timestamp(column: &ArrayRef, is_tz: bool) -> Result<ArrayRef, Error> {
    // Try to downcast to StructArray first
    if let Some(struct_array) = column.as_any().downcast_ref::<StructArray>() {
        let expected_fields = if is_tz { 3 } else { 2 };
        if struct_array.columns().len() < expected_fields {
            return UnableToCastSnowflakeTimestampSnafu {
                reason: format!("struct has fewer than {expected_fields} columns"),
            }
            .fail();
        }

        let epoch_array = struct_array
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .context(UnableToCastSnowflakeTimestampSnafu {
                reason: "epoch is missing or not an Int64Array",
            })?;
        let fraction_array = struct_array
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .context(UnableToCastSnowflakeTimestampSnafu {
                reason: "fraction is missing or not an Int32Array",
            })?;

        let mut builder = TimestampNanosecondArray::builder(struct_array.len());
        if is_tz {
            builder = builder.with_timezone(Arc::clone(&UTC_TIMEZONE));
        }

        for idx in 0..struct_array.len() {
            if struct_array.is_null(idx) {
                builder.append_null();
            } else {
                let epoch = epoch_array.value(idx);
                let fraction = i64::from(fraction_array.value(idx));
                let nanos = epoch
                    .checked_mul(NANOSECONDS)
                    .and_then(|n| n.checked_add(fraction));
                match nanos {
                    Some(ts) => builder.append_value(ts),
                    None => builder.append_null(),
                }
            }
        }

        Ok(Arc::new(builder.finish()) as ArrayRef)
    } else if let Some(epoch_array) = column.as_any().downcast_ref::<Int64Array>() {
        // Handle case where Snowflake returns a primitive Int64Array (seconds precision)
        let mut builder = TimestampNanosecondArray::builder(epoch_array.len());
        if is_tz {
            builder = builder.with_timezone(Arc::clone(&UTC_TIMEZONE));
        }

        for idx in 0..epoch_array.len() {
            if epoch_array.is_null(idx) {
                builder.append_null();
            } else {
                match epoch_array.value(idx).checked_mul(NANOSECONDS) {
                    Some(ts) => builder.append_value(ts),
                    None => builder.append_null(),
                }
            }
        }

        Ok(Arc::new(builder.finish()) as ArrayRef)
    } else {
        UnableToCastSnowflakeTimestampSnafu {
            reason: "input is neither a StructArray nor an Int64Array",
        }
        .fail()
    }
}

fn cast_sf_fixed_point_number_to_decimal(
    array: &ArrayRef,
    precision: u8,
    scale: i8,
) -> Result<ArrayRef, Error> {
    let data_type = array.data_type();
    let decimal_array = match array.data_type() {
        DataType::Int8 => {
            cast_integer_to_decimal(array.as_primitive::<Int8Type>(), precision, scale)
        }
        DataType::Int16 => {
            cast_integer_to_decimal(array.as_primitive::<Int16Type>(), precision, scale)
        }
        DataType::Int32 => {
            cast_integer_to_decimal(array.as_primitive::<Int32Type>(), precision, scale)
        }
        DataType::Int64 => {
            cast_integer_to_decimal(array.as_primitive::<Int64Type>(), precision, scale)
        }
        _ => Err(ArrowError::CastError(format!(
            "Casting from {data_type:?} is not supported"
        ))),
    }
    .context(UnableToCastSnowflakeNumericToDecimalSnafu)?;

    Ok(decimal_array)
}

fn cast_integer_to_decimal<T: ArrowPrimitiveType>(
    array: &PrimitiveArray<T>,
    precision: u8,
    scale: i8,
) -> Result<ArrayRef, ArrowError>
where
    T::Native: Into<i128>,
{
    let mut decimal_builder = Decimal128Array::builder(array.len());
    for value in array {
        match value {
            Some(value) => {
                decimal_builder.append_value(value.into());
            }
            None => decimal_builder.append_null(),
        }
    }

    let decimal_array = decimal_builder.finish();
    Ok(Arc::new(
        decimal_array.with_precision_and_scale(precision, scale)?,
    ))
}

/// Parses a Snowflake JSON type descriptor (e.g. `{"type":"FIXED","precision":38,...}`)
/// into an Arrow [`DataType`].
///
/// # Errors
///
/// Returns an error if the JSON is malformed or contains an unsupported type.
pub fn parse_snowflake_data_type(data_type_str: &str) -> Result<DataType, Error> {
    let data_type: serde_json::Value =
        serde_json::from_str(data_type_str).map_err(|e| Error::UnableToRetrieveSchema {
            reason: e.to_string(),
        })?;

    match data_type["type"].as_str() {
        Some("FIXED") => {
            // Snowflake's FIXED precision must fit in Arrow `Decimal128`
            // (max 38). Snowflake itself also caps precision at 38, so any
            // larger value is malformed/unexpected. Scale is bounded by the
            // same range. Use checked conversions with clear error messages
            // rather than `as` casts, which would silently truncate and
            // yield wrong schemas — a data-correctness violation.
            const MAX_DECIMAL128_PRECISION: u64 = 38;
            let precision_raw = data_type["precision"].as_u64().unwrap_or(38);
            if precision_raw == 0 || precision_raw > MAX_DECIMAL128_PRECISION {
                return Err(Error::UnableToRetrieveSchema {
                    reason: format!(
                        "FIXED precision {precision_raw} is out of range (expected 1..={MAX_DECIMAL128_PRECISION})",
                    ),
                });
            }
            // Safe: bounded above by 38.
            let precision =
                u8::try_from(precision_raw).map_err(|_| Error::UnableToRetrieveSchema {
                    reason: format!("FIXED precision {precision_raw} does not fit in u8"),
                })?;
            let scale_raw = data_type["scale"].as_i64().unwrap_or(0);
            let scale = i8::try_from(scale_raw).map_err(|_| Error::UnableToRetrieveSchema {
                reason: format!(
                    "FIXED scale {scale_raw} is out of range (expected {}..={})",
                    i8::MIN,
                    i8::MAX,
                ),
            })?;
            Ok(DataType::Decimal128(precision, scale))
        }
        // Semi-structured types (dynamic schema per row) and structured MAP
        // arrive as JSON-serialized strings. Geospatial types are serialized as
        // WKT/GeoJSON/WKB text. Utf8 is the correct lossless Arrow mapping.
        Some("TEXT" | "VARIANT" | "ARRAY" | "OBJECT" | "MAP" | "GEOGRAPHY" | "GEOMETRY") => {
            Ok(DataType::Utf8)
        }
        Some("REAL") => Ok(DataType::Float64),
        Some("BINARY") => Ok(DataType::Binary),
        Some("BOOLEAN") => Ok(DataType::Boolean),
        Some("DATE") => Ok(DataType::Date32),
        Some("TIMESTAMP_NTZ" | "TIMESTAMP_LTZ") => {
            // TIMESTAMP_NTZ has no time zone. TIMESTAMP_LTZ stores an
            // absolute instant (UTC-backed) that is rendered in the session
            // time zone. Mirror the `information_schema` path, which maps
            // both to a timezone-less Arrow Timestamp so schema discovery is
            // consistent across paths. TIMESTAMP_TZ (below) stores an
            // explicit offset per value and is represented with a UTC
            // timezone.
            Ok(DataType::Timestamp(TimeUnit::Nanosecond, None))
        }
        Some("TIME") => Ok(DataType::Time64(TimeUnit::Nanosecond)),
        Some("TIMESTAMP_TZ") => Ok(DataType::Timestamp(
            TimeUnit::Nanosecond,
            Some("UTC".into()),
        )),
        // VECTOR is a fixed-length numeric array. Snowflake's type descriptor
        // embeds `dimension` and `elementType` (FLOAT or INT); map to the
        // corresponding Arrow FixedSizeList for lossless representation.
        Some("VECTOR") => {
            let dimension = data_type["dimension"]
                .as_u64()
                .and_then(|n| i32::try_from(n).ok())
                .ok_or_else(|| Error::UnableToRetrieveSchema {
                    reason: "VECTOR type missing or invalid 'dimension'".to_string(),
                })?;
            // Snowflake VECTOR values are dense numeric arrays; a zero or
            // negative dimension is not a valid Snowflake type and would
            // produce a meaningless FixedSizeList. Reject it with a clear
            // error so schema discovery fails loudly instead of silently
            // yielding a wrong type.
            if dimension <= 0 {
                return Err(Error::UnableToRetrieveSchema {
                    reason: format!("VECTOR type has non-positive 'dimension': {dimension}"),
                });
            }
            let element_type = match data_type["elementType"].as_str() {
                Some("FLOAT" | "FLOAT32") => DataType::Float32,
                Some("INT" | "INT32") => DataType::Int32,
                Some(other) => {
                    return Err(Error::UnableToRetrieveSchema {
                        reason: format!("Unsupported VECTOR element type: {other}"),
                    });
                }
                None => {
                    return Err(Error::UnableToRetrieveSchema {
                        reason: "VECTOR type missing 'elementType'".to_string(),
                    });
                }
            };
            // Individual elements of a Snowflake VECTOR are never null — the
            // whole vector value is either present or SQL NULL. Mark the
            // inner `item` field non-nullable to model this accurately.
            Ok(DataType::FixedSizeList(
                Arc::new(Field::new("item", element_type, false)),
                dimension,
            ))
        }
        Some(t) => Err(Error::UnableToRetrieveSchema {
            reason: format!("Unsupported Snowflake data type: {t}"),
        }),
        None => Err(Error::UnableToRetrieveSchema {
            reason: "Missing data type".to_string(),
        }),
    }
}

/// Parses a `SHOW COLUMNS IN <table>` JSON response into an Arrow [`SchemaRef`].
///
/// # Errors
///
/// Returns an error if the response format is unexpected or contains unsupported types.
pub fn parse_schema_from_json(resp: &serde_json::Value) -> Result<SchemaRef, Error> {
    let columns: Vec<Vec<serde_json::Value>> = resp
        .as_array()
        .ok_or_else(|| Error::UnableToRetrieveSchema {
            reason: "Response is not an array".to_string(),
        })?
        .iter()
        .map(|column| {
            column
                .as_array()
                .ok_or_else(|| Error::UnableToRetrieveSchema {
                    reason: "Column data is not an array".to_string(),
                })
                .cloned()
        })
        .collect::<Result<Vec<_>, _>>()?;

    let mut fields = Vec::new();

    for column in columns {
        if column.len() < 5 {
            return Err(Error::UnableToRetrieveSchema {
                reason: "Invalid column data format".to_string(),
            });
        }

        let column_name = column[2]
            .as_str()
            .ok_or_else(|| Error::UnableToRetrieveSchema {
                reason: "Invalid column name".to_string(),
            })?;

        let data_type_str = column[3]
            .as_str()
            .ok_or_else(|| Error::UnableToRetrieveSchema {
                reason: "Invalid data type".to_string(),
            })?;

        let data_type: DataType = parse_snowflake_data_type(data_type_str)?;

        let is_nullable = column[4]
            .as_str()
            .is_none_or(|s| s.to_uppercase() == "TRUE");

        let mut metadata = HashMap::from([(
            SOURCE_TYPE_METADATA_KEY.to_string(),
            data_type_str.to_string(),
        )]);
        if let Some(comment) = column
            .get(5)
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|comment| !comment.is_empty())
        {
            metadata.insert(DESCRIPTION_METADATA_KEY.to_string(), comment.to_string());
        }

        let field = Field::new(column_name, data_type, is_nullable).with_metadata(metadata);

        fields.push(field);
    }

    Ok(Arc::new(Schema::new(fields)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        ArrayBuilder, ArrayRef, Int32Builder, Int64Builder, StructBuilder, TimestampNanosecondArray,
    };
    use arrow::datatypes::{DataType, Field};
    use arrow::util::display;
    use std::sync::Arc;

    #[test]
    fn test_cast_sf_timestamp_ntz_seconds_precision_to_arrow_timestamp() {
        let mut builder = Int64Builder::new();
        builder.append_values(&[1_696_164_330, 1_714_647_301], &[true, true]);
        let timestamp_ntz_array = Arc::new(builder.finish()) as Arc<dyn Array>;

        let result = cast_sf_timestamp_to_arrow_timestamp(&timestamp_ntz_array, false)
            .expect("Should cast Snowflake timestamp to Arrow timestamp");
        let result = result
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .expect("Should downcast to TimestampNanosecondArray");

        let expected_timestamps = [
            Some(1_696_164_330_000_000_000),
            Some(1_714_647_301_000_000_000),
        ];

        assert_eq!(result.value(0), expected_timestamps[0].unwrap_or_default());
        assert_eq!(result.value(1), expected_timestamps[1].unwrap_or_default());
    }

    #[test]
    fn test_cast_sf_timestamp_tz_seconds_precision_to_arrow_timestamp() {
        let mut builder = Int64Builder::new();
        builder.append_values(&[1_696_164_330, 1_714_647_301], &[true, true]);
        let timestamp_tz_array = Arc::new(builder.finish()) as Arc<dyn Array>;

        let result = cast_sf_timestamp_to_arrow_timestamp(&timestamp_tz_array, true)
            .expect("Should cast Snowflake timestamp to Arrow timestamp");
        let result = result
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .expect("Should downcast to TimestampNanosecondArray");

        let expected_timestamps = [
            Some(1_696_164_330_000_000_000),
            Some(1_714_647_301_000_000_000),
        ];

        assert_eq!(result.value(0), expected_timestamps[0].unwrap_or_default());
        assert_eq!(result.value(1), expected_timestamps[1].unwrap_or_default());
    }

    #[test]
    fn test_cast_sf_timestamp_ntz_to_arrow_timestamp() {
        let timestamp_ntz_array = create_timestamp_ntz_array(
            vec![Some(1_696_164_330), None, Some(1_714_647_301)],
            vec![Some(0), None, Some(739_000_000)],
        );
        let result = cast_sf_timestamp_to_arrow_timestamp(&timestamp_ntz_array, false)
            .expect("Should cast Snowflake timestamp to Arrow timestamp");
        let result = result
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .expect("Should downcast to TimestampNanosecondArray");

        let expected_timestamps = [
            Some(1_696_164_330_000_000_000),
            None,
            Some(1_714_647_301_739_000_000),
        ];

        assert_eq!(result.value(0), expected_timestamps[0].unwrap_or_default());
        assert!(result.is_null(1));
        assert_eq!(result.value(2), expected_timestamps[2].unwrap_or_default());
    }

    #[test]
    fn test_cast_sf_timestamp_tz_to_arrow_timestamp() {
        let timestamp_tz_array = create_timestamp_tz_array(
            vec![Some(1_696_164_330), None, Some(1_714_647_301)],
            vec![Some(0), None, Some(739_000_000)],
            vec![Some(1440), None, Some(1500)],
        );
        let result = cast_sf_timestamp_to_arrow_timestamp(&timestamp_tz_array, true)
            .expect("Should cast Snowflake timestamp to Arrow timestamp");
        let result = result
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .expect("Should downcast to TimestampNanosecondArray");

        let expected_timestamps = [
            Some(1_696_164_330_000_000_000),
            None,
            Some(1_714_647_301_739_000_000),
        ];

        assert_eq!(result.value(0), expected_timestamps[0].unwrap_or_default());
        assert!(result.is_null(1));
        assert_eq!(result.value(2), expected_timestamps[2].unwrap_or_default());
    }

    #[test]
    fn test_cast_sf_timestamp_ntz_to_arrow_timestamp_invalid_input() {
        let epoch_array = Arc::new(Int64Array::from(vec![
            Some(1_696_164_330),
            None,
            Some(1_714_647_301),
        ])) as ArrayRef;

        let timestamp_ntz_no_fraction = StructArray::from(vec![(
            Arc::new(Field::new("epoch", DataType::Int64, true)),
            epoch_array,
        )]);

        let result = cast_sf_timestamp_to_arrow_timestamp(
            &(Arc::new(timestamp_ntz_no_fraction) as ArrayRef),
            false,
        );

        result.expect_err("Should fail for missing fraction field");
    }

    #[test]
    #[expect(clippy::cast_possible_truncation)]
    fn test_cast_sf_fixed_point_number_to_decimal_i32() {
        let scale = 4i8;
        let data = vec![
            Some((0.123 * 10f64.powi(scale.into())) as i32),
            Some((-345.1234 * 10f64.powi(scale.into())) as i32),
            None,
        ];
        let int32_array = Int32Array::from(data);
        let decimal_array =
            cast_integer_to_decimal(&int32_array, 10, scale).expect("Should cast to decimal");
        let decimal_array = decimal_array
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .expect("Should downcast to Decimal128Array");

        assert_eq!(decimal_array.value(0), 1_230_i128);
        assert_eq!(decimal_array.value(1), -3_451_234_i128);

        assert_eq!(
            "0.1230",
            display::array_value_to_string(&decimal_array, 0).expect("Should format decimal")
        );
        assert_eq!(
            "-345.1234",
            display::array_value_to_string(&decimal_array, 1).expect("Should format decimal")
        );
        assert!(decimal_array.is_null(2), "The third entry should be null.");
    }

    #[test]
    #[expect(clippy::cast_possible_truncation)]
    fn test_cast_sf_fixed_point_number_to_decimal_i64() {
        let scale = 9i8;
        let data = vec![
            (0.000_000_001 * 10f64.powi(scale.into())) as i64,
            (999_999.999_999_999 * 10f64.powi(scale.into())) as i64,
        ];

        let int_array = Int64Array::from(data);
        let decimal_array =
            cast_integer_to_decimal(&int_array, 34, scale).expect("Should cast to decimal");
        let decimal_array = decimal_array
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .expect("Should downcast to Decimal128Array");

        assert_eq!(decimal_array.value(0), 1i128); // 0.000000001 scaled by 10^9
        assert_eq!(decimal_array.value(1), 999_999_999_999_999_i128);

        assert_eq!(
            "0.000000001",
            display::array_value_to_string(&decimal_array, 0).expect("Should format decimal")
        );
        assert_eq!(
            "999999.999999999",
            display::array_value_to_string(&decimal_array, 1).expect("Should format decimal")
        );
    }

    #[test]
    fn test_parse_snowflake_data_type() {
        let test_cases = vec![
            (
                r#"{"type":"FIXED","precision":38,"scale":0,"nullable":true}"#,
                DataType::Decimal128(38, 0),
            ),
            (
                r#"{"type":"FIXED","precision":10,"scale":2,"nullable":true}"#,
                DataType::Decimal128(10, 2),
            ),
            (
                r#"{"type":"TEXT","length":16777216,"byteLength":16777216,"nullable":true,"fixed":false}"#,
                DataType::Utf8,
            ),
            (r#"{"type":"REAL","nullable":true}"#, DataType::Float64),
            (
                r#"{"type":"BINARY","length":8388608,"byteLength":8388608,"nullable":true,"fixed":true}"#,
                DataType::Binary,
            ),
            (r#"{"type":"BOOLEAN","nullable":true}"#, DataType::Boolean),
            (r#"{"type":"DATE","nullable":true}"#, DataType::Date32),
            (
                r#"{"type":"TIMESTAMP_NTZ","precision":0,"scale":9,"nullable":true}"#,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
            ),
            (
                r#"{"type":"TIME","precision":0,"scale":9,"nullable":true}"#,
                DataType::Time64(TimeUnit::Nanosecond),
            ),
            (
                r#"{"type":"TIMESTAMP_TZ","precision":0,"scale":9,"nullable":true}"#,
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            ),
            (r#"{"type":"VARIANT","nullable":true}"#, DataType::Utf8),
            (r#"{"type":"ARRAY","nullable":true}"#, DataType::Utf8),
            (r#"{"type":"OBJECT","nullable":true}"#, DataType::Utf8),
        ];

        for (input, expected) in test_cases {
            let result = parse_snowflake_data_type(input);
            assert!(result.is_ok(), "Failed to parse: {input}");
            assert_eq!(
                result.expect("Failed to parse: {input}"),
                expected,
                "Mismatch for input: {input}"
            );
        }
    }

    #[test]
    fn test_parse_snowflake_data_type_semi_structured() {
        // Semi-structured Snowflake types (OBJECT, VARIANT, ARRAY) and
        // structured MAP are returned as JSON-serialized strings in
        // Snowflake's Arrow wire format and have dynamic, per-row shapes.
        // Utf8 is the correct lossless Arrow mapping.
        for input in [
            r#"{"type":"OBJECT","nullable":true}"#,
            r#"{"type":"OBJECT","nullable":false}"#,
            r#"{"type":"VARIANT","nullable":true}"#,
            r#"{"type":"ARRAY","nullable":true}"#,
            r#"{"type":"MAP","nullable":true}"#,
        ] {
            let got = parse_snowflake_data_type(input)
                .unwrap_or_else(|e| panic!("Failed to parse '{input}': {e:?}"));
            assert_eq!(got, DataType::Utf8, "Expected Utf8 for '{input}'");
        }
    }

    #[test]
    fn test_parse_snowflake_data_type_geospatial() {
        // GEOGRAPHY/GEOMETRY arrive as WKT/GeoJSON/WKB text strings.
        for input in [
            r#"{"type":"GEOGRAPHY","nullable":true}"#,
            r#"{"type":"GEOMETRY","nullable":false}"#,
        ] {
            let got = parse_snowflake_data_type(input)
                .unwrap_or_else(|e| panic!("Failed to parse '{input}': {e:?}"));
            assert_eq!(got, DataType::Utf8, "Expected Utf8 for '{input}'");
        }
    }

    #[test]
    fn test_parse_snowflake_data_type_timestamp_ltz() {
        // TIMESTAMP_LTZ must map consistently with the information_schema
        // path (Timestamp(ns, None)) so schema discovery agrees across paths.
        let got = parse_snowflake_data_type(
            r#"{"type":"TIMESTAMP_LTZ","precision":0,"scale":9,"nullable":true}"#,
        )
        .expect("Should parse TIMESTAMP_LTZ");
        assert_eq!(got, DataType::Timestamp(TimeUnit::Nanosecond, None));
    }

    #[test]
    fn test_parse_snowflake_data_type_vector() {
        // VECTOR<FLOAT, N> → FixedSizeList<Float32, N>. The inner `item`
        // field is non-nullable because Snowflake VECTOR elements are
        // always present (only the entire vector value can be SQL NULL).
        let got = parse_snowflake_data_type(
            r#"{"type":"VECTOR","dimension":128,"elementType":"FLOAT","nullable":true}"#,
        )
        .expect("Should parse VECTOR<FLOAT, 128>");
        assert_eq!(
            got,
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, false)), 128)
        );

        // VECTOR<INT, N> → FixedSizeList<Int32, N>
        let got = parse_snowflake_data_type(
            r#"{"type":"VECTOR","dimension":4,"elementType":"INT","nullable":true}"#,
        )
        .expect("Should parse VECTOR<INT, 4>");
        assert_eq!(
            got,
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Int32, false)), 4)
        );

        // Missing dimension must fail clearly rather than produce a wrong type.
        let err =
            parse_snowflake_data_type(r#"{"type":"VECTOR","elementType":"FLOAT","nullable":true}"#)
                .expect_err("Should error when dimension is missing");
        assert!(
            matches!(err, Error::UnableToRetrieveSchema { ref reason } if reason.contains("dimension")),
            "Expected missing dimension error, got: {err:?}"
        );

        // Zero or negative dimensions are not valid Snowflake VECTOR types;
        // reject them so schema discovery fails loudly.
        let err = parse_snowflake_data_type(
            r#"{"type":"VECTOR","dimension":0,"elementType":"FLOAT","nullable":true}"#,
        )
        .expect_err("Should error when dimension is zero");
        assert!(
            matches!(err, Error::UnableToRetrieveSchema { ref reason } if reason.contains("dimension")),
            "Expected non-positive dimension error, got: {err:?}"
        );

        // Missing elementType must fail clearly.
        let err = parse_snowflake_data_type(r#"{"type":"VECTOR","dimension":4,"nullable":true}"#)
            .expect_err("Should error when elementType is missing");
        assert!(
            matches!(err, Error::UnableToRetrieveSchema { ref reason } if reason.contains("elementType")),
            "Expected missing elementType error, got: {err:?}"
        );

        // Unknown elementType must fail clearly.
        let err = parse_snowflake_data_type(
            r#"{"type":"VECTOR","dimension":4,"elementType":"DOUBLE","nullable":true}"#,
        )
        .expect_err("Should error for unsupported elementType");
        assert!(
            matches!(err, Error::UnableToRetrieveSchema { ref reason } if reason.contains("DOUBLE")),
            "Expected unsupported elementType error, got: {err:?}"
        );
    }

    #[test]
    fn test_parse_snowflake_data_type_fixed_variants() {
        // FIXED honors precision/scale and falls back to (38, 0) when absent.
        let cases = [
            (
                r#"{"type":"FIXED","precision":38,"scale":0,"nullable":true}"#,
                DataType::Decimal128(38, 0),
            ),
            (
                r#"{"type":"FIXED","precision":18,"scale":9,"nullable":false}"#,
                DataType::Decimal128(18, 9),
            ),
            (
                r#"{"type":"FIXED","precision":5,"scale":-2,"nullable":true}"#,
                DataType::Decimal128(5, -2),
            ),
            (
                // Missing precision/scale: defaults to (38, 0).
                r#"{"type":"FIXED","nullable":true}"#,
                DataType::Decimal128(38, 0),
            ),
        ];
        for (input, expected) in cases {
            let got = parse_snowflake_data_type(input)
                .unwrap_or_else(|e| panic!("Failed to parse '{input}': {e:?}"));
            assert_eq!(got, expected, "Mismatch for '{input}'");
        }
    }

    #[test]
    fn test_parse_snowflake_data_type_fixed_out_of_range() {
        // Using `as` casts on precision/scale would silently truncate
        // out-of-range values and produce a subtly wrong Arrow schema, which
        // is a data-correctness violation. Confirm that each malformed or
        // out-of-range descriptor returns a structured schema error instead.
        let cases: &[(&str, &str)] = &[
            // precision > Arrow Decimal128 max (38)
            (
                r#"{"type":"FIXED","precision":39,"scale":0,"nullable":true}"#,
                "precision",
            ),
            // precision = 0 is nonsensical for Decimal128
            (
                r#"{"type":"FIXED","precision":0,"scale":0,"nullable":true}"#,
                "precision",
            ),
            // precision overflows u8 dramatically
            (
                r#"{"type":"FIXED","precision":300,"scale":0,"nullable":true}"#,
                "precision",
            ),
            // scale overflows i8
            (
                r#"{"type":"FIXED","precision":10,"scale":200,"nullable":true}"#,
                "scale",
            ),
            // scale underflows i8
            (
                r#"{"type":"FIXED","precision":10,"scale":-200,"nullable":true}"#,
                "scale",
            ),
        ];
        for (input, expected_field) in cases {
            let err = parse_snowflake_data_type(input)
                .expect_err(&format!("Should error for malformed input '{input}'"));
            let Error::UnableToRetrieveSchema { reason } = err else {
                panic!("Unexpected error type for '{input}': {err:?}");
            };
            assert!(
                reason.contains(expected_field),
                "Error '{reason}' should mention '{expected_field}' for input '{input}'"
            );
        }
    }

    #[test]
    fn test_parse_schema_from_json_covers_all_types() {
        // Simulates a `SHOW COLUMNS` JSON response containing every supported
        // Snowflake type, verifying the end-to-end schema discovery round-trip:
        // SHOW COLUMNS row -> JSON type descriptor -> Arrow Field.
        //
        // SHOW COLUMNS rows are positional arrays where index 2 is the column
        // name, index 3 is the type descriptor JSON, and index 4 is nullability.
        let rows = serde_json::json!([
            [
                "db",
                "schema",
                "c_fixed",
                r#"{"type":"FIXED","precision":38,"scale":0,"nullable":true}"#,
                "TRUE",
                ""
            ],
            [
                "db",
                "schema",
                "c_decimal",
                r#"{"type":"FIXED","precision":12,"scale":4,"nullable":false}"#,
                "FALSE",
                ""
            ],
            [
                "db",
                "schema",
                "c_text",
                r#"{"type":"TEXT","length":16777216,"nullable":true,"fixed":false}"#,
                "TRUE",
                ""
            ],
            [
                "db",
                "schema",
                "c_real",
                r#"{"type":"REAL","nullable":true}"#,
                "TRUE",
                ""
            ],
            [
                "db",
                "schema",
                "c_binary",
                r#"{"type":"BINARY","length":8388608,"nullable":true,"fixed":true}"#,
                "TRUE",
                ""
            ],
            [
                "db",
                "schema",
                "c_bool",
                r#"{"type":"BOOLEAN","nullable":false}"#,
                "FALSE",
                ""
            ],
            [
                "db",
                "schema",
                "c_date",
                r#"{"type":"DATE","nullable":true}"#,
                "TRUE",
                ""
            ],
            [
                "db",
                "schema",
                "c_ts_ntz",
                r#"{"type":"TIMESTAMP_NTZ","precision":0,"scale":9,"nullable":true}"#,
                "TRUE",
                ""
            ],
            [
                "db",
                "schema",
                "c_ts_tz",
                r#"{"type":"TIMESTAMP_TZ","precision":0,"scale":9,"nullable":true}"#,
                "TRUE",
                ""
            ],
            [
                "db",
                "schema",
                "c_time",
                r#"{"type":"TIME","precision":0,"scale":9,"nullable":true}"#,
                "TRUE",
                ""
            ],
            [
                "db",
                "schema",
                "c_variant",
                r#"{"type":"VARIANT","nullable":true}"#,
                "TRUE",
                ""
            ],
            [
                "db",
                "schema",
                "c_array",
                r#"{"type":"ARRAY","nullable":true}"#,
                "TRUE",
                ""
            ],
            [
                "db",
                "schema",
                "c_object",
                r#"{"type":"OBJECT","nullable":true}"#,
                "TRUE",
                ""
            ],
            [
                "db",
                "schema",
                "c_map",
                r#"{"type":"MAP","nullable":true}"#,
                "TRUE",
                ""
            ],
            [
                "db",
                "schema",
                "c_geography",
                r#"{"type":"GEOGRAPHY","nullable":true}"#,
                "TRUE",
                ""
            ],
            [
                "db",
                "schema",
                "c_geometry",
                r#"{"type":"GEOMETRY","nullable":true}"#,
                "TRUE",
                ""
            ],
            [
                "db",
                "schema",
                "c_ts_ltz",
                r#"{"type":"TIMESTAMP_LTZ","precision":0,"scale":9,"nullable":true}"#,
                "TRUE",
                ""
            ],
            [
                "db",
                "schema",
                "c_vector",
                r#"{"type":"VECTOR","dimension":4,"elementType":"FLOAT","nullable":true}"#,
                "TRUE",
                ""
            ],
        ]);

        let schema =
            parse_schema_from_json(&rows).expect("Should parse SHOW COLUMNS JSON into a Schema");

        let expected: Vec<(&str, DataType, bool)> = vec![
            ("c_fixed", DataType::Decimal128(38, 0), true),
            ("c_decimal", DataType::Decimal128(12, 4), false),
            ("c_text", DataType::Utf8, true),
            ("c_real", DataType::Float64, true),
            ("c_binary", DataType::Binary, true),
            ("c_bool", DataType::Boolean, false),
            ("c_date", DataType::Date32, true),
            (
                "c_ts_ntz",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
            (
                "c_ts_tz",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                true,
            ),
            ("c_time", DataType::Time64(TimeUnit::Nanosecond), true),
            ("c_variant", DataType::Utf8, true),
            ("c_array", DataType::Utf8, true),
            ("c_object", DataType::Utf8, true),
            ("c_map", DataType::Utf8, true),
            ("c_geography", DataType::Utf8, true),
            ("c_geometry", DataType::Utf8, true),
            (
                "c_ts_ltz",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
            (
                "c_vector",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, false)), 4),
                true,
            ),
        ];

        assert_eq!(
            schema.fields().len(),
            expected.len(),
            "Field count mismatch"
        );
        for (field, (name, dtype, nullable)) in schema.fields().iter().zip(expected.iter()) {
            assert_eq!(field.name(), name, "Name mismatch");
            assert_eq!(field.data_type(), dtype, "Type mismatch for {name}");
            assert_eq!(
                field.is_nullable(),
                *nullable,
                "Nullability mismatch for {name}"
            );
        }
    }

    #[test]
    fn test_parse_schema_from_json_preserves_column_comment() {
        let rows = serde_json::json!([[
            "db",
            "schema",
            "customer_id",
            r#"{"type":"FIXED","precision":38,"scale":0,"nullable":true}"#,
            "TRUE",
            "customer dimension key"
        ]]);

        let schema =
            parse_schema_from_json(&rows).expect("Should parse SHOW COLUMNS JSON into a Schema");

        assert_eq!(
            schema
                .field(0)
                .metadata()
                .get("description")
                .map(String::as_str),
            Some("customer dimension key")
        );
        assert_eq!(
            schema
                .field(0)
                .metadata()
                .get("source_type")
                .map(String::as_str),
            Some(r#"{"type":"FIXED","precision":38,"scale":0,"nullable":true}"#)
        );
    }

    #[test]
    fn test_snowflake_schema_cast_passes_through_semi_structured() {
        // Semi-structured columns (OBJECT/VARIANT/ARRAY) arrive from Snowflake
        // as Utf8 JSON strings and must pass through schema_cast unchanged,
        // preserving both type and values (data correctness).
        let json_object = r#"{"k":"v","n":42}"#;
        let json_array = r"[1,2,3]";
        let json_variant = r#""hello""#;

        let object_array = Arc::new(StringArray::from(vec![Some(json_object), None])) as ArrayRef;
        let array_array =
            Arc::new(StringArray::from(vec![Some(json_array), Some("[]")])) as ArrayRef;
        let variant_array =
            Arc::new(StringArray::from(vec![Some(json_variant), Some("null")])) as ArrayRef;

        let schema = Arc::new(Schema::new(vec![
            Field::new("obj", DataType::Utf8, true),
            Field::new("arr", DataType::Utf8, false),
            Field::new("var", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::clone(&object_array),
                Arc::clone(&array_array),
                Arc::clone(&variant_array),
            ],
        )
        .expect("Should build record batch");

        let out = snowflake_schema_cast(&batch).expect("schema_cast should succeed");

        assert_eq!(out.num_columns(), 3);
        assert_eq!(out.num_rows(), 2);
        for (i, name) in ["obj", "arr", "var"].iter().enumerate() {
            assert_eq!(out.schema().field(i).name(), name);
            assert_eq!(out.schema().field(i).data_type(), &DataType::Utf8);
        }

        let obj_out = out
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Should be StringArray");
        assert_eq!(obj_out.value(0), json_object);
        assert!(obj_out.is_null(1));

        let arr_out = out
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Should be StringArray");
        assert_eq!(arr_out.value(0), json_array);
        assert_eq!(arr_out.value(1), "[]");

        let var_out = out
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Should be StringArray");
        assert_eq!(var_out.value(0), json_variant);
        assert_eq!(var_out.value(1), "null");
    }

    #[test]
    fn test_parse_snowflake_data_type_errors() {
        let error_cases = vec![
            (
                r#"{"type":"UNKNOWN","nullable":true}"#,
                "Unsupported Snowflake data type: UNKNOWN",
            ),
            (r#"{"nullable":true}"#, "Missing data type"),
            ("invalid json", "expected value at line 1 column 1"),
        ];

        for (input, expected_error) in error_cases {
            let result = parse_snowflake_data_type(input);
            assert!(result.is_err(), "Expected error for input: {input}");
            let error = result.expect_err("Expected error for input: {input}");
            match error {
                Error::UnableToRetrieveSchema { reason } => {
                    assert!(
                        reason.contains(expected_error),
                        "Error '{reason}' does not contain expected message '{expected_error}' for input: '{input}'",
                    );
                }
                _ => panic!("Unexpected error type: {error:?}"),
            }
        }
    }

    fn create_timestamp_ntz_array(
        epochs: Vec<Option<i64>>,
        fractions: Vec<Option<i32>>,
    ) -> ArrayRef {
        let fields = vec![
            Field::new("epoch", DataType::Int64, true),
            Field::new("fraction", DataType::Int32, true),
        ];

        let mut builder = StructBuilder::new(
            fields,
            vec![
                Box::new(Int64Builder::new()) as Box<dyn ArrayBuilder>,
                Box::new(Int32Builder::new()) as Box<dyn ArrayBuilder>,
            ],
        );

        for (epoch, fraction) in epochs.into_iter().zip(fractions.into_iter()) {
            if let (Some(epoch_val), Some(fraction_val)) = (epoch, fraction) {
                builder
                    .field_builder::<Int64Builder>(0)
                    .expect("Should return a field builder")
                    .append_value(epoch_val);
                builder
                    .field_builder::<Int32Builder>(1)
                    .expect("Should return a field builder")
                    .append_value(fraction_val);
                builder.append(true);
            } else {
                builder.append(false);
                builder
                    .field_builder::<Int64Builder>(0)
                    .expect("Should return a field builder")
                    .append_null();
                builder
                    .field_builder::<Int32Builder>(1)
                    .expect("Should return a field builder")
                    .append_null();
            }
        }

        Arc::new(builder.finish()) as ArrayRef
    }

    fn create_timestamp_tz_array(
        epochs: Vec<Option<i64>>,
        fractions: Vec<Option<i32>>,
        timezones: Vec<Option<i32>>,
    ) -> ArrayRef {
        let fields = vec![
            Field::new("epoch", DataType::Int64, true),
            Field::new("fraction", DataType::Int32, true),
            Field::new("timezone", DataType::Int32, true),
        ];

        let mut builder = StructBuilder::new(
            fields,
            vec![
                Box::new(Int64Builder::new()) as Box<dyn ArrayBuilder>,
                Box::new(Int32Builder::new()) as Box<dyn ArrayBuilder>,
                Box::new(Int32Builder::new()) as Box<dyn ArrayBuilder>,
            ],
        );

        for (epoch, fraction, timezone) in epochs
            .into_iter()
            .zip(fractions)
            .zip(timezones)
            .map(|((a, b), c)| (a, b, c))
        {
            if let (Some(epoch_val), Some(fraction_val), Some(timezone_val)) =
                (epoch, fraction, timezone)
            {
                builder
                    .field_builder::<Int64Builder>(0)
                    .expect("Should return a field builder")
                    .append_value(epoch_val);
                builder
                    .field_builder::<Int32Builder>(1)
                    .expect("Should return a field builder")
                    .append_value(fraction_val);
                builder
                    .field_builder::<Int32Builder>(2)
                    .expect("Should return a field builder")
                    .append_value(timezone_val);
                builder.append(true);
            } else {
                builder.append(false);
                builder
                    .field_builder::<Int64Builder>(0)
                    .expect("Should return a field builder")
                    .append_null();
                builder
                    .field_builder::<Int32Builder>(1)
                    .expect("Should return a field builder")
                    .append_null();
                builder
                    .field_builder::<Int32Builder>(2)
                    .expect("Should return a field builder")
                    .append_null();
            }
        }

        Arc::new(builder.finish()) as ArrayRef
    }

    #[test]
    fn test_snowflake_schema_cast_timestamp_ltz() {
        let timestamp_ltz_array = create_timestamp_ntz_array(
            vec![Some(1_696_164_330), None, Some(1_714_647_301)],
            vec![Some(0), None, Some(500_000_000)],
        );

        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "created_at",
                DataType::Struct(
                    vec![
                        Field::new("epoch", DataType::Int64, true),
                        Field::new("fraction", DataType::Int32, true),
                    ]
                    .into(),
                ),
                true,
            )
            .with_metadata(
                [("logicalType".to_string(), "TIMESTAMP_LTZ".to_string())]
                    .into_iter()
                    .collect(),
            ),
        ]));

        let batch = RecordBatch::try_new(schema, vec![timestamp_ltz_array])
            .expect("Should create record batch");

        let result = snowflake_schema_cast(&batch).expect("Should cast TIMESTAMP_LTZ");
        assert_eq!(
            *result.schema().field(0).data_type(),
            DataType::Timestamp(TimeUnit::Nanosecond, None),
        );

        let ts_array = result
            .column(0)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .expect("Should downcast to TimestampNanosecondArray");

        assert_eq!(ts_array.value(0), 1_696_164_330_000_000_000);
        assert!(ts_array.is_null(1));
        assert_eq!(ts_array.value(2), 1_714_647_301_500_000_000);
    }

    #[test]
    fn test_cast_sf_timestamp_overflow_produces_null() {
        let overflow_epoch = i64::MAX / NANOSECONDS + 1;
        let timestamp_array = create_timestamp_ntz_array(
            vec![Some(overflow_epoch), Some(1_696_164_330)],
            vec![Some(0), Some(0)],
        );

        let result = cast_sf_timestamp_to_arrow_timestamp(&timestamp_array, false)
            .expect("Should not error on overflow");
        let result = result
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .expect("Should downcast to TimestampNanosecondArray");

        assert!(result.is_null(0), "Overflowing epoch should produce null");
        assert_eq!(result.value(1), 1_696_164_330_000_000_000);
    }

    #[test]
    fn test_cast_sf_timestamp_seconds_overflow_produces_null() {
        let mut builder = Int64Builder::new();
        builder.append_values(&[i64::MAX / NANOSECONDS + 1, 1_696_164_330], &[true, true]);
        let epoch_array = Arc::new(builder.finish()) as Arc<dyn Array>;

        let result = cast_sf_timestamp_to_arrow_timestamp(&epoch_array, false)
            .expect("Should not error on overflow");
        let result = result
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .expect("Should downcast to TimestampNanosecondArray");

        assert!(result.is_null(0), "Overflowing epoch should produce null");
        assert_eq!(result.value(1), 1_696_164_330_000_000_000);
    }
}
