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
use crate::oracle::FailedToParseBigDecimalSnafu;
use arrow::{
    array::{
        ArrayBuilder, ArrayRef, BooleanBuilder, Decimal128Builder, Float32Builder, Float64Builder,
        LargeStringBuilder, RecordBatch, RecordBatchOptions, StringBuilder, make_builder,
    },
    datatypes::{DataType, SchemaRef},
};
use bigdecimal::BigDecimal;
use oracle::Row;
use snafu::OptionExt;
use snafu::ResultExt;

/// Oracle Built-in Data Types
/// `<https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Data-Types.html#GUID-7B72E154-677A-4342-A1EA-C74C1EA928E6>`
pub(crate) fn map_oracle_type_to_arrow_type(
    data_type: &str,
    precision: Option<u8>,
    scale: Option<i8>,
) -> Option<arrow::datatypes::DataType> {
    match data_type.to_uppercase().as_str() {
        "CHAR" | "NCHAR" | "VARCHAR2" | "NVARCHAR2" => Some(DataType::Utf8),
        "CLOB" | "NCLOB" => Some(DataType::LargeUtf8),
        "NUMBER" | "DECIMAL" => {
            // In Oracle, default precision and scale are (38, 0).
            let precision = precision.unwrap_or(38);
            let scale = scale.unwrap_or(0);
            Some(DataType::Decimal128(precision, scale))
        }
        "BINARY_FLOAT" => Some(DataType::Float32),
        "FLOAT" | "BINARY_DOUBLE" => Some(DataType::Float64),
        "BOOLEAN" => Some(DataType::Boolean),

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

#[allow(clippy::too_many_lines)]
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

            match field.data_type() {
                DataType::Utf8 => {
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
                DataType::LargeUtf8 => {
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
                DataType::Decimal128(_precision, scale) => {
                    handle_primitive_type!(
                        builder,
                        col,
                        native_type,
                        Decimal128Builder,
                        String,
                        row,
                        idx,
                        |v: String| {
                            let decimal =
                                v.parse::<BigDecimal>()
                                    .context(FailedToParseBigDecimalSnafu {
                                        value: v.to_string(),
                                    })?;

                            big_decimal_to_i128(&decimal, *scale).context(
                                FailedToConvertBigDecimalToI128Snafu {
                                    big_decimal: decimal.clone(),
                                },
                            )
                        }
                    );
                }
                DataType::Float32 => {
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
                DataType::Float64 => {
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
                DataType::Boolean => {
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
                _ => {
                    return super::UnsupportedTypeSnafu {
                        data_type: format!("{native_type:?}"),
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
