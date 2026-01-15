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

use std::sync::Arc;

use crate::oracle::FailedToConvertBigDecimalToI128Snafu;
use crate::oracle::FailedToConvertNaiveDateTimeToNanosSnafu;
use crate::oracle::FailedToParseBigDecimalSnafu;
use arrow::array::BinaryBuilder;
use arrow::array::Date32Builder;
use arrow::array::Int64Builder;
use arrow::array::LargeBinaryBuilder;
use arrow::array::TimestampNanosecondBuilder;
use arrow::array::TimestampSecondBuilder;
use arrow::datatypes::Date32Type;
use arrow::datatypes::TimeUnit;
use arrow::{
    array::{
        ArrayBuilder, ArrayRef, BooleanBuilder, Decimal128Builder, Float32Builder, Float64Builder,
        LargeStringBuilder, RecordBatch, RecordBatchOptions, StringBuilder, make_builder,
    },
    datatypes::{DataType, SchemaRef},
};
use bigdecimal::BigDecimal;
use chrono::FixedOffset;
use oracle::Row;
use oracle::sql_type::OracleType;
use snafu::OptionExt;
use snafu::ResultExt;

/// Oracle Built-in Data Types
/// `<https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Data-Types.html#GUID-7B72E154-677A-4342-A1EA-C74C1EA928E6>`
pub(crate) fn map_oracle_type_to_arrow_type(
    data_type: &str,
    precision: Option<u8>,
    scale: Option<i8>,
) -> Option<arrow::datatypes::DataType> {
    let data_type = data_type.trim().to_uppercase();

    // Example: TIMESTAMP(6) WITH TIME ZONE
    if data_type.starts_with("TIMESTAMP") {
        let precision = scale.unwrap_or(6);
        let time_unit = match precision {
            0 => TimeUnit::Second,
            _ => TimeUnit::Nanosecond,
        };
        let tz =
            if data_type.contains("WITH TIME ZONE") || data_type.contains("WITH LOCAL TIME ZONE") {
                Some(Arc::<str>::from("UTC"))
            } else {
                None
            };
        return Some(DataType::Timestamp(time_unit, tz));
    }

    match data_type.as_str() {
        // Oracle types below max size is 32767 bytes
        "ROWID" | "CHAR" | "NCHAR" | "VARCHAR2" | "NVARCHAR2" | "LONG" => Some(DataType::Utf8),
        "CLOB" | "NCLOB" => Some(DataType::LargeUtf8),
        "NUMBER" => {
            // "The absence of precision and scale designators specifies the maximum range and precision for an Oracle number"
            let p = precision.unwrap_or(38); // Oracle-defined max precision
            let s = scale.unwrap_or(20); // Spice-default scale when not specified

            // Integer types in Oracle are represented as NUMBER with 0 scale.
            // Prefer Int64 over Decimal128 for integer types as it is much more efficient (including accelerators).
            if s == 0 && p <= 18 {
                return Some(DataType::Int64);
            }

            Some(DataType::Decimal128(p, s))
        }
        "DATE" => Some(DataType::Date32),
        "BINARY_FLOAT" => Some(DataType::Float32),
        // A subtype of the NUMBER data type having precision p. A FLOAT value is represented internally as NUMBER.
        // The precision p can range from 1 to 126 binary digits.
        "FLOAT" => {
            // If <=24: Float32, >24: Float64
            match precision {
                Some(p) if p <= 24 => Some(DataType::Float32),
                _ => Some(DataType::Float64),
            }
        }
        "BINARY_DOUBLE" => Some(DataType::Float64),
        "BOOLEAN" => Some(DataType::Boolean),
        // Up to 2 GB
        "RAW" | "LONG RAW" => Some(DataType::Binary),

        // Up to 4 GB
        "BLOB" => Some(DataType::LargeBinary),

        // INTERVAL YEAR and INTERVAL DAY are not currently supported
        _ => None,
    }
}

macro_rules! handle_primitive_type {
    ($builder:expr, $col:expr, $type:expr, $builder_ty:ty, $value_ty:ty, $row:expr, $index:expr, $convert:expr) => {
        let Some(builder) = $builder.as_any_mut().downcast_mut::<$builder_ty>() else {
            return super::FailedToDowncastBuilderSnafu {
                native_type: format!("{:?}", $type),
                column: $col.to_string(),
            }
            .fail();
        };

        let v = match $row.get::<usize, Option<$value_ty>>($index) {
            Ok(val) => val,
            Err(e) => {
                return Err(super::Error::FailedToRetrieveValue {
                    native_type: format!("{:?}", $type),
                    column: $col.to_string(),
                    source: e.into(),
                });
            }
        };

        match v {
            Some(v) => builder.append_value($convert(v)?),
            None => builder.append_null(),
        }
    };
}

pub(crate) fn rows_to_arrow(rows: &[Row], schema: &SchemaRef) -> super::Result<RecordBatch> {
    let mut arrow_columns_builders = vec![];
    for field in schema.fields() {
        let builder = make_builder(field.data_type(), rows.len());
        arrow_columns_builders.push(builder);
    }

    for row in rows {
        for (idx, field) in schema.fields.iter().enumerate() {
            let builder = &mut arrow_columns_builders[idx];

            let Some(col) = row.column_info().get(idx) else {
                return Err(super::Error::NoColumnForIndex { index: idx });
            };
            let native_type = col.oracle_type();

            match (field.data_type(), col.oracle_type()) {
                (DataType::Utf8, _) => {
                    handle_primitive_type!(
                        builder,
                        col,
                        native_type,
                        StringBuilder,
                        String,
                        row,
                        idx,
                        Result::Ok
                    );
                }
                (DataType::LargeUtf8, _) => {
                    handle_primitive_type!(
                        builder,
                        col,
                        native_type,
                        LargeStringBuilder,
                        String,
                        row,
                        idx,
                        Result::Ok
                    );
                }
                (DataType::Decimal128(_precision, scale), _) => {
                    handle_primitive_type!(
                        builder,
                        col,
                        native_type,
                        Decimal128Builder,
                        String,
                        row,
                        idx,
                        |v: String| {
                            let decimal = v
                                .parse::<BigDecimal>()
                                .context(FailedToParseBigDecimalSnafu { value: v.clone() })?;

                            big_decimal_to_i128(&decimal, *scale).context(
                                FailedToConvertBigDecimalToI128Snafu {
                                    big_decimal: decimal.clone(),
                                },
                            )
                        }
                    );
                }
                (DataType::Float32, _) => {
                    handle_primitive_type!(
                        builder,
                        native_type,
                        col,
                        Float32Builder,
                        f32,
                        row,
                        idx,
                        Result::Ok
                    );
                }
                (DataType::Float64, _) => {
                    handle_primitive_type!(
                        builder,
                        native_type,
                        col,
                        Float64Builder,
                        f64,
                        row,
                        idx,
                        Result::Ok
                    );
                }
                (DataType::Int64, _) => {
                    handle_primitive_type!(
                        builder,
                        native_type,
                        col,
                        Int64Builder,
                        i64,
                        row,
                        idx,
                        Result::Ok
                    );
                }
                (DataType::Boolean, _) => {
                    handle_primitive_type!(
                        builder,
                        native_type,
                        col,
                        BooleanBuilder,
                        bool,
                        row,
                        idx,
                        Result::Ok
                    );
                }
                (DataType::Date32, _) => {
                    handle_primitive_type!(
                        builder,
                        native_type,
                        col,
                        Date32Builder,
                        chrono::NaiveDate,
                        row,
                        idx,
                        |v: chrono::NaiveDate| {
                            Ok::<_, super::Error>(Date32Type::from_naive_date(v))
                        }
                    );
                }
                // If TIMESTAMP WITH LOCAL TIME ZONE or TIMESTAMP WITHOUT TIME ZONE (seconds precision)
                (DataType::Timestamp(TimeUnit::Second, _), OracleType::TimestampLTZ(_))
                | (DataType::Timestamp(TimeUnit::Second, None), _) => {
                    handle_primitive_type!(
                        builder,
                        field,
                        col,
                        TimestampSecondBuilder,
                        chrono::NaiveDateTime,
                        row,
                        idx,
                        |v: chrono::NaiveDateTime| {
                            // Assumes input is in session/local time and should be interpreted as UTC
                            let t = v.and_utc().timestamp();
                            Ok::<_, super::Error>(t)
                        }
                    );
                }
                // TIMESTAMP WITH TIME ZONE (seconds precision)
                (DataType::Timestamp(TimeUnit::Second, Some(_)), _) => {
                    handle_primitive_type!(
                        builder,
                        field,
                        col,
                        TimestampSecondBuilder,
                        chrono::DateTime<FixedOffset>,
                        row,
                        idx,
                        |v: chrono::DateTime<FixedOffset>| {
                            // Normalize to UTC before converting to epoch seconds
                            let utc_value = v.with_timezone(&chrono::Utc);
                            Ok::<_, super::Error>(utc_value.timestamp())
                        }
                    );
                }
                // If TIMESTAMP WITH LOCAL TIME ZONE or TIMESTAMP WITHOUT TIME ZONE (nanoseconds precision)
                (DataType::Timestamp(TimeUnit::Nanosecond, _), OracleType::TimestampLTZ(_))
                | (DataType::Timestamp(TimeUnit::Nanosecond, None), _) => {
                    handle_primitive_type!(
                        builder,
                        field,
                        col,
                        TimestampNanosecondBuilder,
                        chrono::NaiveDateTime,
                        row,
                        idx,
                        |v: chrono::NaiveDateTime| {
                            // Assumes input is in session/local time and should be interpreted as UTC
                            v.and_utc()
                                .timestamp_nanos_opt()
                                .context(FailedToConvertNaiveDateTimeToNanosSnafu { v })
                        }
                    );
                }
                // TIMESTAMP WITH TIME ZONE (nanoseconds precision)
                (DataType::Timestamp(TimeUnit::Nanosecond, Some(_)), _) => {
                    handle_primitive_type!(
                        builder,
                        field,
                        col,
                        TimestampNanosecondBuilder,
                        chrono::DateTime<FixedOffset>,
                        row,
                        idx,
                        |v: chrono::DateTime<FixedOffset>| {
                            // Normalize to UTC before converting to nanos timestamp
                            let utc_value = v.with_timezone(&chrono::Utc);
                            Ok::<_, super::Error>(
                                utc_value.timestamp_nanos_opt().unwrap_or_default(),
                            )
                        }
                    );
                }
                (DataType::Binary, _) => {
                    handle_primitive_type!(
                        builder,
                        native_type,
                        col,
                        BinaryBuilder,
                        Vec<u8>,
                        row,
                        idx,
                        Result::Ok
                    );
                }
                (DataType::LargeBinary, _) => {
                    handle_primitive_type!(
                        builder,
                        native_type,
                        col,
                        LargeBinaryBuilder,
                        Vec<u8>,
                        row,
                        idx,
                        Result::Ok
                    );
                }
                _ => {
                    return super::UnsupportedTypeSnafu {
                        data_type: format!("{native_type:?}"),
                        column: col.to_string(),
                    }
                    .fail();
                }
            }
        }
    }

    let columns = arrow_columns_builders
        .iter_mut()
        .map(arrow::array::ArrayBuilder::finish)
        .collect::<Vec<ArrayRef>>();

    let options = &RecordBatchOptions::new().with_row_count(Some(rows.len()));
    RecordBatch::try_new_with_options(Arc::clone(schema), columns, options)
        .map_err(|err| super::Error::FailedToBuildRecordBatch { source: err })
}

fn big_decimal_to_i128(decimal: &bigdecimal::BigDecimal, scale: i8) -> Option<i128> {
    use bigdecimal::{FromPrimitive, ToPrimitive};

    bigdecimal::BigDecimal::from_f32(10f32.powi(i32::from(scale)))
        .and_then(|scale| (decimal * scale).to_i128())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, TimeUnit};

    #[test]
    fn test_common_oracle_types_mappings() {
        // Test a typical Oracle table schema
        let columns_and_expected = vec![
            (("ID", "NUMBER", Some(10), Some(0)), DataType::Int64),
            (("NAME", "VARCHAR2", None, None), DataType::Utf8),
            (
                ("SALARY", "NUMBER", Some(10), Some(2)),
                DataType::Decimal128(10, 2),
            ),
            (("HIRE_DATE", "DATE", None, None), DataType::Date32),
            (
                ("CREATED_AT", "TIMESTAMP", None, Some(6)),
                DataType::Timestamp(TimeUnit::Nanosecond, None),
            ),
            (
                ("PROFILE_PICTURE", "BLOB", None, None),
                DataType::LargeBinary,
            ),
            // Decimal edge cases
            (
                ("BIG_DECIMAL", "NUMBER", Some(38), Some(10)),
                DataType::Decimal128(38, 10),
            ),
            (
                ("DEFAULT_DECIMAL", "NUMBER", None, None),
                DataType::Decimal128(38, 20),
            ),
            // Float
            (("FLOAT32", "FLOAT", Some(10), None), DataType::Float32),
            (("FLOAT64", "FLOAT", Some(30), None), DataType::Float64),
            (
                ("BINARY_FLOAT", "BINARY_FLOAT", None, None),
                DataType::Float32,
            ),
            (
                ("BINARY_DOUBLE", "BINARY_DOUBLE", None, None),
                DataType::Float64,
            ),
            // Timestamp with and without time zone
            (
                ("TS_NANO", "TIMESTAMP(9)", None, Some(9)),
                DataType::Timestamp(TimeUnit::Nanosecond, None),
            ),
            (
                ("TS_SEC", "TIMESTAMP(0)", None, Some(0)),
                DataType::Timestamp(TimeUnit::Second, None),
            ),
            (
                ("TS_TZ", "TIMESTAMP(6) WITH TIME ZONE", None, Some(6)),
                DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::<str>::from("UTC"))),
            ),
            (
                (
                    "TS_LOCAL_TZ",
                    "TIMESTAMP(3) WITH LOCAL TIME ZONE",
                    None,
                    Some(3),
                ),
                DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::<str>::from("UTC"))),
            ),
        ];

        for ((name, oracle_type, precision, scale), expected) in columns_and_expected {
            let result = map_oracle_type_to_arrow_type(oracle_type, precision, scale);
            assert_eq!(
                result,
                Some(expected.clone()),
                "Failed mapping for column {name}: {oracle_type} -> {expected:?}",
            );
        }
    }
}
