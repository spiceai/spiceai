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

use std::{convert, sync::Arc};

use arrow::{
    array::{
        ArrayBuilder, ArrayRef, BinaryBuilder, Float32Builder, Float64Builder, Int16Builder,
        Int32Builder, Int64Builder, Int8Builder, NullBuilder, RecordBatch, RecordBatchOptions,
        StringBuilder, UInt64Builder,
    },
    datatypes::{DataType, Field, Schema, TimeUnit},
};
use bigdecimal::BigDecimal;
use chrono::Timelike;
use mysql_async::{consts::ColumnType, Row, Value};
use snafu::{ResultExt, Snafu};

use crate::arrow::map_data_type_to_array_builder_optional;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to build record batch: {source}"))]
    FailedToBuildRecordBatch { source: arrow::error::ArrowError },

    #[snafu(display("No builder found for index {index}"))]
    NoBuilderForIndex { index: usize },

    #[snafu(display("Failed to downcast builder for {:?}", mysql_type))]
    FailedToDowncastBuilder { mysql_type: String },

    #[snafu(display("Integer overflow when converting u64 to i64: {source}"))]
    FailedToConvertU64toI64 {
        source: <u64 as convert::TryInto<i64>>::Error,
    },

    #[snafu(display("Integer overflow when converting u128 to i64: {source}"))]
    FailedToConvertU128toI64 {
        source: <u128 as convert::TryInto<i64>>::Error,
    },

    #[snafu(display("Failed to get a row value for {:?}: {}", mysql_type, source))]
    FailedToGetRowValue {
        mysql_type: ColumnType,
        source: mysql_async::FromValueError,
    },

    #[snafu(display("Failed to parse raw Postgres Bytes as BigDecimal: {:?}", bytes))]
    FailedToParseBigDecmialFromPostgres { bytes: Vec<u8> },

    #[snafu(display("Cannot represent BigDecimal as i128: {big_decimal}"))]
    FailedToConvertBigDecmialToI128 { big_decimal: BigDecimal },

    #[snafu(display("Failed to find field {column_name} in schema"))]
    FailedToFindFieldInSchema { column_name: String },

    #[snafu(display("No Arrow field found for index {index}"))]
    NoArrowFieldForIndex { index: usize },

    #[snafu(display("No column name for index: {index}"))]
    NoColumnNameForIndex { index: usize },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

macro_rules! handle_primitive_type {
    ($builder:expr, $type:expr, $builder_ty:ty, $value_ty:ty, $row:expr, $index:expr) => {{
        let Some(builder) = $builder else {
            return NoBuilderForIndexSnafu { index: $index }.fail();
        };
        let Some(builder) = builder.as_any_mut().downcast_mut::<$builder_ty>() else {
            return FailedToDowncastBuilderSnafu {
                mysql_type: format!("{:?}", $type),
            }
            .fail();
        };
        let v = $row
            .get_opt::<$value_ty, usize>($index)
            .transpose()
            .context(FailedToGetRowValueSnafu { mysql_type: $type })?;

        match v {
            Some(v) => builder.append_value(v),
            None => builder.append_null(),
        }
    }};
}

/// Converts `MySQL` `Row`s to an Arrow `RecordBatch`. Assumes that all rows have the same schema and
/// sets the schema based on the first row.
///
/// # Errors
///
/// Returns an error if there is a failure in converting the rows to a `RecordBatch`.
#[allow(clippy::too_many_lines)]
pub fn rows_to_arrow(rows: &[Row]) -> Result<RecordBatch> {
    let mut arrow_fields: Vec<Option<Field>> = Vec::new();
    let mut arrow_columns_builders: Vec<Option<Box<dyn ArrayBuilder>>> = Vec::new();
    let mut mysql_types: Vec<ColumnType> = Vec::new();
    let mut column_names: Vec<String> = Vec::new();

    if !rows.is_empty() {
        let row = &rows[0];
        for column in row.columns().iter() {
            let column_name = column.name_str();
            let column_type = column.column_type();
            let data_type = map_column_to_data_type(column_type);
            arrow_fields.push(
                data_type
                    .clone()
                    .map(|data_type| Field::new(column_name.clone(), data_type.clone(), true)),
            );
            arrow_columns_builders
                .push(map_data_type_to_array_builder_optional(data_type.as_ref()));
            mysql_types.push(column_type);
            column_names.push(column_name.to_string());
        }
    }

    for row in rows {
        for (i, mysql_type) in mysql_types.iter().enumerate() {
            let Some(builder) = arrow_columns_builders.get_mut(i) else {
                return NoBuilderForIndexSnafu { index: i }.fail();
            };
            match *mysql_type {
                ColumnType::MYSQL_TYPE_NULL => {
                    let Some(builder) = builder else {
                        return NoBuilderForIndexSnafu { index: i }.fail();
                    };
                    let Some(builder) = builder.as_any_mut().downcast_mut::<NullBuilder>() else {
                        return FailedToDowncastBuilderSnafu {
                            mysql_type: format!("{mysql_type:?}"),
                        }
                        .fail();
                    };
                    builder.append_null();
                }
                ColumnType::MYSQL_TYPE_BIT => {
                    let Some(builder) = builder else {
                        return NoBuilderForIndexSnafu { index: i }.fail();
                    };
                    let Some(builder) = builder.as_any_mut().downcast_mut::<UInt64Builder>() else {
                        return FailedToDowncastBuilderSnafu {
                            mysql_type: format!("{mysql_type:?}"),
                        }
                        .fail();
                    };
                    let value = row.get_opt::<Value, usize>(i).transpose().context(
                        FailedToGetRowValueSnafu {
                            mysql_type: ColumnType::MYSQL_TYPE_BIT,
                        },
                    )?;
                    match value {
                        Some(Value::Bytes(mut bytes)) => {
                            while bytes.len() < 8 {
                                bytes.insert(0, 0);
                            }
                            let mut array = [0u8; 8];
                            array.copy_from_slice(&bytes);
                            builder.append_value(u64::from_be_bytes(array));
                        }
                        _ => builder.append_null(),
                    }
                }
                ColumnType::MYSQL_TYPE_TINY => {
                    handle_primitive_type!(
                        builder,
                        ColumnType::MYSQL_TYPE_TINY,
                        Int8Builder,
                        i8,
                        row,
                        i
                    );
                }
                ColumnType::MYSQL_TYPE_SHORT => {
                    handle_primitive_type!(
                        builder,
                        ColumnType::MYSQL_TYPE_SHORT,
                        Int16Builder,
                        i16,
                        row,
                        i
                    );
                }
                ColumnType::MYSQL_TYPE_LONG => {
                    handle_primitive_type!(
                        builder,
                        ColumnType::MYSQL_TYPE_LONG,
                        Int32Builder,
                        i32,
                        row,
                        i
                    );
                }
                ColumnType::MYSQL_TYPE_LONGLONG => {
                    handle_primitive_type!(
                        builder,
                        ColumnType::MYSQL_TYPE_LONGLONG,
                        Int64Builder,
                        i64,
                        row,
                        i
                    );
                }
                ColumnType::MYSQL_TYPE_FLOAT => {
                    handle_primitive_type!(
                        builder,
                        ColumnType::MYSQL_TYPE_FLOAT,
                        Float32Builder,
                        f32,
                        row,
                        i
                    );
                }
                ColumnType::MYSQL_TYPE_DOUBLE => {
                    handle_primitive_type!(
                        builder,
                        ColumnType::MYSQL_TYPE_DOUBLE,
                        Float64Builder,
                        f64,
                        row,
                        i
                    );
                }
                ColumnType::MYSQL_TYPE_VARCHAR => {
                    handle_primitive_type!(
                        builder,
                        ColumnType::MYSQL_TYPE_VARCHAR,
                        StringBuilder,
                        String,
                        row,
                        i
                    );
                }
                ColumnType::MYSQL_TYPE_VAR_STRING => {
                    handle_primitive_type!(
                        builder,
                        ColumnType::MYSQL_TYPE_VAR_STRING,
                        StringBuilder,
                        String,
                        row,
                        i
                    );
                }
                ColumnType::MYSQL_TYPE_BLOB => {
                    handle_primitive_type!(
                        builder,
                        ColumnType::MYSQL_TYPE_BLOB,
                        BinaryBuilder,
                        Vec<u8>,
                        row,
                        i
                    );
                }
                ColumnType::MYSQL_TYPE_TIMESTAMP => {
                    let Some(builder) = builder else {
                        return NoBuilderForIndexSnafu { index: i }.fail();
                    };
                    let Some(builder) = builder.as_any_mut().downcast_mut::<Int64Builder>() else {
                        return FailedToDowncastBuilderSnafu {
                            mysql_type: format!("{mysql_type:?}"),
                        }
                        .fail();
                    };
                    let v = row.get_opt::<Value, usize>(i).transpose().context(
                        FailedToGetRowValueSnafu {
                            mysql_type: ColumnType::MYSQL_TYPE_TIMESTAMP,
                        },
                    )?;
                    match v {
                        Some(v) => {
                            let timestamp = match v {
                                Value::Date(year, month, day, hour, minute, second, micros) => {
                                    let timestamp = chrono::NaiveDate::from_ymd_opt(
                                        year as i32,
                                        month as u32,
                                        day as u32,
                                    )
                                    .unwrap_or_default()
                                    .and_hms_micro_opt(
                                        hour as u32,
                                        minute as u32,
                                        second as u32,
                                        micros as u32,
                                    )
                                    .unwrap_or_default()
                                    .and_utc();
                                    timestamp.timestamp()
                                }
                                Value::Time(is_neg, days, hours, minutes, seconds, micros) => {
                                    let naivetime = chrono::NaiveTime::from_hms_micro_opt(
                                        hours as u32,
                                        minutes as u32,
                                        seconds as u32,
                                        micros as u32,
                                    )
                                    .unwrap_or_default();

                                    let time: i64 = naivetime.num_seconds_from_midnight().into();

                                    let timestamp = days as i64 * 24 * 60 * 60 + time;

                                    if is_neg {
                                        -timestamp
                                    } else {
                                        timestamp
                                    }
                                }
                                _ => 0,
                            };
                            builder.append_value(timestamp);
                        }
                        None => builder.append_null(),
                    }
                }
                _ => unimplemented!("Unsupported column type {:?}", mysql_type),
            }
        }
    }

    let columns = arrow_columns_builders
        .into_iter()
        .filter_map(|builder| builder.map(|mut b| b.finish()))
        .collect::<Vec<ArrayRef>>();
    let arrow_fields = arrow_fields.into_iter().flatten().collect::<Vec<Field>>();
    let options = &RecordBatchOptions::new().with_row_count(Some(rows.len()));
    RecordBatch::try_new_with_options(Arc::new(Schema::new(arrow_fields)), columns, options)
        .map_err(|err| Error::FailedToBuildRecordBatch { source: err })
}

#[allow(clippy::unnecessary_wraps)]
fn map_column_to_data_type(column_type: ColumnType) -> Option<DataType> {
    match column_type {
        ColumnType::MYSQL_TYPE_NULL => Some(DataType::Null),
        ColumnType::MYSQL_TYPE_BIT => Some(DataType::UInt64),
        ColumnType::MYSQL_TYPE_TINY => Some(DataType::Int8),
        ColumnType::MYSQL_TYPE_SHORT => Some(DataType::Int16),
        ColumnType::MYSQL_TYPE_LONG => Some(DataType::Int32),
        ColumnType::MYSQL_TYPE_LONGLONG => Some(DataType::Int64),
        ColumnType::MYSQL_TYPE_FLOAT => Some(DataType::Float32),
        ColumnType::MYSQL_TYPE_DOUBLE => Some(DataType::Float64),
        ColumnType::MYSQL_TYPE_TIMESTAMP => Some(DataType::Timestamp(TimeUnit::Second, None)),
        ColumnType::MYSQL_TYPE_DATE => Some(DataType::Date32),
        ColumnType::MYSQL_TYPE_VARCHAR
        | ColumnType::MYSQL_TYPE_STRING
        | ColumnType::MYSQL_TYPE_VAR_STRING => Some(DataType::Utf8),
        ColumnType::MYSQL_TYPE_BLOB => Some(DataType::Binary),
        _ => unimplemented!("Unsupported column type {:?}", column_type),
    }
}
