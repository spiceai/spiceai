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
use std::sync::{Arc, LazyLock};

use arrow::array::{
    Array, ArrayRef, AsArray, Decimal128Array, Int32Array, Int64Array, PrimitiveArray, RecordBatch,
    StructArray, TimestampNanosecondBuilder,
};
use arrow::datatypes::{
    ArrowPrimitiveType, DataType, Field, Int16Type, Int32Type, Int64Type, Int8Type, Schema,
    SchemaRef, TimeUnit,
};
use arrow::error::ArrowError;
use async_trait::async_trait;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::sql::TableReference;
use datafusion_table_providers::sql::db_connection_pool::dbconnection::{
    self, AsyncDbConnection, DbConnection,
};
use futures::stream;
use futures::StreamExt;
use futures::TryStreamExt;
use snafu::prelude::*;
use snowflake_api::SnowflakeApi;

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

    #[snafu(display("Failed to cast snowflake timestamp to arrow timestamp: {reason}"))]
    UnableToCastSnowflakeTimestamp { reason: String },

    #[snafu(display("Failed to cast snowflake fixed-point number to decimal: {source}"))]
    UnableToCastSnowflakeNumericToDecimal { source: arrow::error::ArrowError },

    #[snafu(display("Failed to create record batch: {source}"))]
    FailedToCreateRecordBatch { source: arrow::error::ArrowError },
}

static UTC_TIMEZONE: LazyLock<Arc<str>> = LazyLock::new(|| Arc::from("UTC"));

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

    fn as_async(&self) -> Option<&dyn AsyncDbConnection<Arc<SnowflakeApi>, &'a (dyn Sync)>> {
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
    ) -> Result<SchemaRef, dbconnection::Error> {
        let table = table_reference.to_quoted_string();
        let query = format!("SHOW COLUMNS IN {table}");

        let res =
            self.api
                .exec(&query)
                .await
                .map_err(|e| dbconnection::Error::UnableToGetSchema {
                    source: e.to_string().into(),
                })?;

        match res {
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
        }
    }

    async fn query_arrow(
        &self,
        sql: &str,
        _: &[&'a (dyn Sync)],
        _projected_schema: Option<SchemaRef>,
    ) -> Result<SendableRecordBatchStream, Box<dyn std::error::Error + Send + Sync>> {
        let sql = sql.to_string();

        let stream = self
            .api
            .exec_streamed(&sql)
            .await
            .context(SnowflakeQuerySnafu)?;

        let mut transformed_stream = stream.map(|batch| {
            batch.and_then(|batch| {
                snowflake_schema_cast(&batch)
                    .map_err(|e| arrow::error::ArrowError::ExternalError(Box::new(e)))
            })
        });

        let Some(first_batch) = transformed_stream.next().await else {
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
        _: &[&'a (dyn Sync)],
    ) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        return NotImplementedSnafu.fail()?;
    }
}

fn to_execution_error(e: impl Into<Box<dyn std::error::Error>>) -> DataFusionError {
    DataFusionError::Execution(format!("{}", e.into()).to_string())
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
                "timestamp_ntz" => {
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
                    {
                        if let (Ok(precision), Ok(scale)) =
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
    let struct_array = column.as_any().downcast_ref::<StructArray>().context(
        UnableToCastSnowflakeTimestampSnafu {
            reason: "value is not a struct",
        },
    )?;

    let expected_fields = if is_tz { 3 } else { 2 };
    if struct_array.columns().len() < expected_fields {
        return UnableToCastSnowflakeTimestampSnafu {
            reason: format!("value is not a struct with {expected_fields} columns"),
        }
        .fail();
    }

    let epoch_array = struct_array
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .context(UnableToCastSnowflakeTimestampSnafu {
            reason: "epoch is missing",
        })?;
    let fraction_array = struct_array
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .context(UnableToCastSnowflakeTimestampSnafu {
            reason: "fraction is missing",
        })?;

    let builder = TimestampNanosecondBuilder::with_capacity(struct_array.len());

    // Snowflake already stores timestamps in UTC, so we don't need to adjust for timezone
    let mut builder = if is_tz {
        builder.with_timezone(Arc::clone(&UTC_TIMEZONE))
    } else {
        builder
    };

    for idx in 0..struct_array.len() {
        if struct_array.is_null(idx) {
            builder.append_null();
        } else {
            let epoch = epoch_array.value(idx);
            let fraction = i64::from(fraction_array.value(idx));
            let timestamp = epoch * 1_000_000_000 + fraction;

            builder.append_value(timestamp);
        }
    }

    let timestamp_array = builder.finish();
    Ok(Arc::new(timestamp_array) as ArrayRef)
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

#[allow(clippy::cast_possible_truncation)]
fn parse_snowflake_data_type(data_type_str: &str) -> Result<DataType, Error> {
    let data_type: serde_json::Value =
        serde_json::from_str(data_type_str).map_err(|e| Error::UnableToRetrieveSchema {
            reason: e.to_string(),
        })?;

    match data_type["type"].as_str() {
        Some("FIXED") => {
            let precision = data_type["precision"].as_u64().unwrap_or(38) as u8;
            let scale = data_type["scale"].as_i64().unwrap_or(0) as i8;
            Ok(DataType::Decimal128(precision, scale))
        }
        Some("TEXT" | "VARIANT" | "ARRAY") => Ok(DataType::Utf8),
        Some("REAL") => Ok(DataType::Float64),
        Some("BINARY") => Ok(DataType::Binary),
        Some("BOOLEAN") => Ok(DataType::Boolean),
        Some("DATE") => Ok(DataType::Date32),
        Some("TIMESTAMP_NTZ") => Ok(DataType::Timestamp(TimeUnit::Nanosecond, None)),
        Some("TIME") => Ok(DataType::Time64(TimeUnit::Nanosecond)),
        Some("TIMESTAMP_TZ") => Ok(DataType::Timestamp(
            TimeUnit::Nanosecond,
            Some("UTC".into()),
        )),
        Some(t) => Err(Error::UnableToRetrieveSchema {
            reason: format!("Unsupported Snowflake data type: {t}"),
        }),
        None => Err(Error::UnableToRetrieveSchema {
            reason: "Missing data type".to_string(),
        }),
    }
}

fn parse_schema_from_json(resp: &serde_json::Value) -> Result<SchemaRef, Error> {
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
            .map_or(true, |s| s.to_uppercase() == "TRUE");

        fields.push(Field::new(column_name, data_type, is_nullable));
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

        assert!(result.is_err());
    }

    #[test]
    #[allow(clippy::cast_possible_truncation)]
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
    #[allow(clippy::cast_possible_truncation)]
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
            fields.clone(),
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
            fields.clone(),
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
}
