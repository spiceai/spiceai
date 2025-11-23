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
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, Decimal128Array, Int32Array, Int64Array,
    PrimitiveArray, RecordBatch, StructArray, TimestampNanosecondArray,
};
use arrow::datatypes::{ArrowPrimitiveType, DataType, Field, Schema, SchemaRef, TimeUnit, Int8Type, Int16Type, Int32Type, Int64Type};
use arrow::error::ArrowError;
use async_trait::async_trait;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::sql::TableReference;
use datafusion_table_providers::sql::db_connection_pool::dbconnection::{
    self, AsyncDbConnection, DbConnection,
};
use once_cell::sync::Lazy;
use snafu::prelude::*;
use snowflake_api::SnowflakeApi;

const NANOSECONDS: i64 = 1_000_000_000;

static UTC_TIMEZONE: Lazy<Arc<str>> = Lazy::new(|| Arc::from("UTC"));

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnowflakeProtocol {
    Adbc,
    Http,
}

impl Default for SnowflakeProtocol {
    fn default() -> Self {
        Self::Adbc
    }
}

impl std::str::FromStr for SnowflakeProtocol {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "adbc" => Ok(Self::Adbc),
            "http" => Ok(Self::Http),
            _ => Err(format!("Invalid snowflake protocol: {s}. Expected 'adbc' or 'http'")),
        }
    }
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Not implemented"))]
    NotImplemented,

    #[snafu(display("Unable to retrieve schema: {reason}"))]
    UnableToRetrieveSchema { reason: String },

    #[snafu(display("Error executing query: {source}"))]
    SnowflakeQueryError { source: snowflake_api::SnowflakeApiError },

    #[snafu(display("Failed to create record batch: {source}"))]
    FailedToCreateRecordBatch { source: ArrowError },

    #[snafu(display("Unable to cast Snowflake timestamp: {reason}"))]
    UnableToCastSnowflakeTimestamp { reason: String },

    #[snafu(display("Unable to cast Snowflake numeric to decimal: {source}"))]
    UnableToCastSnowflakeNumericToDecimal { source: ArrowError },

    #[snafu(display("ADBC error: {source}"))]
    AdbcError { source: snowflake_adbc::Error },
}

pub enum SnowflakeConnectionInner {
    Http(Arc<SnowflakeApi>),
    Adbc(snowflake_adbc::Connection),
}

pub struct SnowflakeConnection {
    pub inner: SnowflakeConnectionInner,
}

impl<'a> DbConnection<Arc<snowflake_api::SnowflakeApi>, &'a dyn Sync> for SnowflakeConnection {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn as_async(&self) -> Option<&dyn AsyncDbConnection<Arc<snowflake_api::SnowflakeApi>, &'a dyn Sync>> {
        Some(self)
    }
}

#[async_trait]
impl<'a> AsyncDbConnection<Arc<snowflake_api::SnowflakeApi>, &'a dyn Sync> for SnowflakeConnection {
    fn new(api: Arc<snowflake_api::SnowflakeApi>) -> Self {
        SnowflakeConnection {
            inner: SnowflakeConnectionInner::Http(api),
        }
    }

    async fn get_schema(
        &self,
        table_reference: &TableReference,
    ) -> Result<SchemaRef, dbconnection::Error> {
        match &self.inner {
            SnowflakeConnectionInner::Http(api) => {
                let table = table_reference.to_quoted_string();
                let query = format!("SHOW COLUMNS IN {table}");

                // Execute query to get schema
                let raw_result = api
                    .exec_raw(&query)
                    .await
                    .map_err(|e| dbconnection::Error::UnableToGetSchema {
                        source: e.to_string().into(),
                    })?;

                // Deserialize to get schema from Arrow batches (async operation)
                let query_result = snowflake_api::RawQueryResult::deserialize_arrow(raw_result)
                    .await
                    .map_err(|e| dbconnection::Error::UnableToGetSchema {
                        source: e.to_string().into(),
                    })?;

                // Extract schema from the result
                match query_result {
                    snowflake_api::QueryResult::Arrow(batches) => {
                        // Get schema from first batch if available, otherwise create empty schema
                        let schema = batches
                            .first()
                            .map(|batch| batch.schema())
                            .unwrap_or_else(|| Arc::new(Schema::empty()));
                        Ok(schema)
                    }
                    snowflake_api::QueryResult::Json(_) | snowflake_api::QueryResult::Empty => {
                        // Return empty schema for non-Arrow results
                        Ok(Arc::new(Schema::empty()))
                    }
                }
            }
            SnowflakeConnectionInner::Adbc(conn) => {
                // Use ADBC to get schema
                let table = table_reference.to_quoted_string();
                let query = format!("SELECT * FROM {table} LIMIT 0");
                
                let mut statement = conn.create_statement();
                statement.set_sql_query(&query);
                statement.execute().await
                    .map_err(|e| dbconnection::Error::UnableToGetSchema {
                        source: e.to_string().into(),
                    })?;

                statement.schema()
                    .ok_or_else(|| dbconnection::Error::UnableToGetSchema {
                        source: "No schema available from ADBC".into(),
                    })
            }
        }
    }

    async fn query_arrow(
        &self,
        sql: &str,
        _: &[&'a dyn Sync],
        _projected_schema: Option<SchemaRef>,
    ) -> Result<SendableRecordBatchStream, Box<dyn std::error::Error + Send + Sync>> {
        match &self.inner {
            SnowflakeConnectionInner::Http(api) => {
                // Execute query and get raw response
                let raw_result = api.exec_raw(sql).await.context(SnowflakeQuerySnafu)?;

                // Deserialize Arrow result (async operation)
                let query_result = snowflake_api::RawQueryResult::deserialize_arrow(raw_result)
                    .await
                    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

                // Extract Arrow batches from the result
                let record_batches = match query_result {
                    snowflake_api::QueryResult::Arrow(batches) => batches,
                    snowflake_api::QueryResult::Json(_) | snowflake_api::QueryResult::Empty => {
                        // Return empty stream if not Arrow result
                        Vec::new()
                    }
                };

                // Cast Snowflake-specific types to standard Arrow types
                let schema = record_batches.first().map(|batch| batch.schema()).ok_or_else(|| {
                    Error::UnableToRetrieveSchema {
                        reason: "No batches returned".to_string(),
                    }
                })?;

                let casted_batches: Vec<Result<RecordBatch, Error>> = record_batches
                    .into_iter()
                    .map(|batch| snowflake_schema_cast(&batch))
                    .collect();

                let stream = futures::stream::iter(
                    casted_batches
                        .into_iter()
                        .map(|r| r.map_err(to_execution_error)),
                );

                let stream_adapter = RecordBatchStreamAdapter::new(schema, stream);

                Ok(Box::pin(stream_adapter))
            }
            SnowflakeConnectionInner::Adbc(conn) => {
                // Use ADBC to execute query
                let mut statement = conn.create_statement();
                statement.set_sql_query(sql);
                statement.execute().await.map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

                let schema = statement.schema().ok_or_else(|| {
                    Box::new(Error::UnableToRetrieveSchema {
                        reason: "No schema available from ADBC statement".to_string(),
                    }) as Box<dyn std::error::Error + Send + Sync>
                })?;

                let batch_stream = statement.into_record_batch_stream().await
                    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

                // Convert our stream to DataFusion's SendableRecordBatchStream
                let stream = futures::stream::unfold(
                    Box::pin(batch_stream),
                    |mut stream| async move {
                        use futures::StreamExt;
                        match stream.next().await {
                            Some(Ok(batch)) => Some((Ok(batch), stream)),
                            Some(Err(e)) => Some((Err(to_execution_error(e)), stream)),
                            None => None,
                        }
                    },
                );

                let stream_adapter = RecordBatchStreamAdapter::new(schema, stream);
                Ok(Box::pin(stream_adapter))
            }
        }
    }

    async fn execute(
        &self,
        query: &str,
        _: &[&'a dyn Sync],
    ) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        match &self.inner {
            SnowflakeConnectionInner::Http(api) => {
                let _ = api.exec(query).await.context(SnowflakeQuerySnafu)?;

                // Snowflake doesn't reliably return affected rows, return 0
                Ok(0)
            }
            SnowflakeConnectionInner::Adbc(conn) => {
                let mut statement = conn.create_statement();
                statement.set_sql_query(query);
                statement.execute().await.map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

                // Snowflake doesn't reliably return affected rows via ADBC either
                Ok(0)
            }
        }
    }
}

fn to_execution_error(e: impl Into<Box<dyn std::error::Error>>) -> DataFusionError {
    DataFusionError::Execution(format!("{}", e.into()))
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
                let timestamp = epoch * NANOSECONDS + fraction;
                builder.append_value(timestamp);
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
                // Convert epoch seconds to nanoseconds
                let timestamp = epoch_array.value(idx) * NANOSECONDS;
                builder.append_value(timestamp);
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

#[cfg(test)]
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
