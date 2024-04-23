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

use std::sync::Arc;

use arrow::{
    array::{
    ArrayBuilder, ArrayRef, BooleanBuilder, Date32Builder, FixedSizeBinaryBuilder, Float32Builder, Float64Builder, Int16Builder, Int32Builder, Int64Builder, Int8Builder, RecordBatch, RecordBatchOptions, StringBuilder, TimestampSecondBuilder, UInt16Builder, UInt32Builder, UInt64Builder, UInt8Builder
    },
    datatypes::{DataType, Date32Type, Field, Schema, TimeUnit},
};
use chrono::NaiveDate;
use chrono_tz::Tz;
use clickhouse_rs::{
    types::{Complex, SqlType},
    Block,
};
use snafu::{ResultExt, Snafu};

use crate::arrow::map_data_type_to_array_builder_optional;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to build record batch: {source}"))]
    FailedToBuildRecordBatch { source: arrow::error::ArrowError },

    #[snafu(display("No builder found for index {index}"))]
    NoBuilderForIndex { index: usize },

    #[snafu(display("Failed to downcast builder for {:?}", clickhouse_type))]
    FailedToDowncastBuilder { clickhouse_type: String },

    #[snafu(display("Failed to get a row value for {}: {}", clickhouse_type, source))]
    FailedToGetRowValue {
        clickhouse_type: SqlType,
        source: clickhouse_rs::errors::Error,
    },

    #[snafu(display("Failed to append a row value for {}: {}", clickhouse_type, source))]
    FailedToAppendRowValue { clickhouse_type: SqlType, source: arrow::error::ArrowError },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

macro_rules! handle_primitive_type {
    ($builder:expr, $type:expr, $builder_ty:ty, $value_ty:ty, $row:expr, $index:expr) => {{
        let Some(builder) = $builder else {
            return NoBuilderForIndexSnafu { index: $index }.fail();
        };
        let Some(builder) = builder.as_any_mut().downcast_mut::<$builder_ty>() else {
            return FailedToDowncastBuilderSnafu {
                clickhouse_type: format!("{:?}", $type),
            }
            .fail();
        };
        let v = $row
            .get::<$value_ty, usize>($index)
            .context(FailedToGetRowValueSnafu {
                clickhouse_type: $type,
            })?;

        builder.append_value(v)
    }};
}

/// Converts `Clickhouse` `Block` to an Arrow `RecordBatch`. Assumes that all rows have the same schema and
/// sets the schema based on the `sql_type` returned for column.
///
/// # Errors
///
/// Returns an error if there is a failure in converting the rows to a `RecordBatch`.
#[allow(clippy::too_many_lines)]
pub fn block_to_arrow(block: &Block<Complex>) -> Result<RecordBatch> {
    let mut arrow_fields: Vec<Option<Field>> = Vec::new();
    let mut arrow_columns_builders: Vec<Option<Box<dyn ArrayBuilder>>> = Vec::new();
    let mut clickhouse_types: Vec<SqlType> = Vec::new();
    let mut column_names: Vec<String> = Vec::new();

    if !block.is_empty() {
        let columns = block.columns();
        for column in columns {
            let column_name = column.name();
            let column_type = column.sql_type();
            let data_type = map_column_to_data_type(&column_type);
            arrow_fields.push(
                data_type
                    .clone()
                    .map(|data_type| Field::new(column_name, data_type.clone(), true)),
            );
            arrow_columns_builders
                .push(map_data_type_to_array_builder_optional(data_type.as_ref()));
            clickhouse_types.push(column_type);
            column_names.push(column_name.to_string());
        }
    }

    for row in block.rows() {
        for (i, clickhouse_type) in clickhouse_types.iter().enumerate() {
            let Some(builder) = arrow_columns_builders.get_mut(i) else {
                return NoBuilderForIndexSnafu { index: i }.fail();
            };

            match *clickhouse_type {
                SqlType::Uuid => {
                    let Some(builder) = builder else {
                        return NoBuilderForIndexSnafu { index: i }.fail();
                    };
                    let Some(builder) = builder.as_any_mut().downcast_mut::<FixedSizeBinaryBuilder>() else {
                        return FailedToDowncastBuilderSnafu {
                            clickhouse_type: format!("{:?}", SqlType::Uuid),
                        }
                        .fail();
                    };
                    let v = row
                        .get::<uuid::Uuid, usize>(i)
                        .context(FailedToGetRowValueSnafu {
                            clickhouse_type: SqlType::Uuid,
                        })?;
                    let _ = builder.append_value(v.as_bytes()).context(FailedToAppendRowValueSnafu {
                        clickhouse_type: SqlType::Uuid,
                    });
                }
                SqlType::Bool => {
                    handle_primitive_type!(builder, SqlType::Bool, BooleanBuilder, bool, row, i);
                }
                SqlType::Int8 => {
                    handle_primitive_type!(builder, SqlType::Int8, Int8Builder, i8, row, i);
                }
                SqlType::Int16 => {
                    handle_primitive_type!(builder, SqlType::Int16, Int16Builder, i16, row, i);
                }
                SqlType::Int32 => {
                    handle_primitive_type!(builder, SqlType::Int32, Int32Builder, i32, row, i);
                }
                SqlType::Int64 => {
                    handle_primitive_type!(builder, SqlType::Int64, Int64Builder, i64, row, i);
                }
                SqlType::UInt8 => {
                    handle_primitive_type!(builder, SqlType::UInt8, UInt8Builder, u8, row, i);
                }
                SqlType::UInt16 => {
                    handle_primitive_type!(builder, SqlType::UInt16, UInt16Builder, u16, row, i);
                }
                SqlType::UInt32 => {
                    handle_primitive_type!(builder, SqlType::UInt32, UInt32Builder, u32, row, i);
                }
                SqlType::UInt64 => {
                    handle_primitive_type!(builder, SqlType::UInt64, UInt64Builder, u64, row, i);
                }
                SqlType::Float32 => {
                    handle_primitive_type!(builder, SqlType::Float32, Float32Builder, f32, row, i);
                }
                SqlType::Float64 => {
                    handle_primitive_type!(builder, SqlType::Float64, Float64Builder, f64, row, i);
                }
                SqlType::String => {
                    handle_primitive_type!(builder, SqlType::String, StringBuilder, String, row, i);
                }
                SqlType::FixedString(size) => {
                    handle_primitive_type!(builder, SqlType::FixedString(size), StringBuilder, String, row, i);
                }
                SqlType::Date => {
                    let Some(builder) = builder else {
                        return NoBuilderForIndexSnafu { index: i }.fail();
                    };
                    let Some(builder) = builder.as_any_mut().downcast_mut::<Date32Builder>() else {
                        return FailedToDowncastBuilderSnafu {
                            clickhouse_type: format!("{:?}", SqlType::Date),
                        }
                        .fail();
                    };
                    let v = row
                        .get::<NaiveDate, usize>(i)
                        .context(FailedToGetRowValueSnafu {
                            clickhouse_type: SqlType::Date,
                        })?;
                    builder.append_value(Date32Type::from_naive_date(v));
                }
                SqlType::DateTime(date_type) => {
                    let Some(builder) = builder else {
                        return NoBuilderForIndexSnafu { index: i }.fail();
                    };
                    let Some(builder) = builder.as_any_mut().downcast_mut::<TimestampSecondBuilder>() else {
                        return FailedToDowncastBuilderSnafu {
                            clickhouse_type: format!("{:?}", SqlType::DateTime(date_type)),
                        }
                        .fail();
                    };
                    let v = row
                                .get::<chrono::DateTime<Tz>, usize>(i)
                                .context(FailedToGetRowValueSnafu {
                                    clickhouse_type: SqlType::DateTime(date_type),
                                })?;
                    builder.append_value(v.timestamp());
                }
                _ => unimplemented!(),
            }
        }
    }

    let columns = arrow_columns_builders
        .into_iter()
        .filter_map(|builder| builder.map(|mut b| b.finish()))
        .collect::<Vec<ArrayRef>>();
    let arrow_fields = arrow_fields.into_iter().flatten().collect::<Vec<Field>>();
    let options = &RecordBatchOptions::new().with_row_count(Some(block.row_count()));
    RecordBatch::try_new_with_options(Arc::new(Schema::new(arrow_fields)), columns, options)
        .map_err(|err| Error::FailedToBuildRecordBatch { source: err })
}

#[allow(clippy::unnecessary_wraps)]
fn map_column_to_data_type(column_type: &SqlType) -> Option<DataType> {
    match column_type {
        SqlType::Uuid => Some(DataType::FixedSizeBinary(16)),
        SqlType::Bool => Some(DataType::Boolean),
        SqlType::Int8 => Some(DataType::Int8),
        SqlType::Int16 => Some(DataType::Int16),
        SqlType::Int32 => Some(DataType::Int32),
        SqlType::Int64 => Some(DataType::Int64),
        SqlType::UInt8 => Some(DataType::UInt8),
        SqlType::UInt16 => Some(DataType::UInt16),
        SqlType::UInt32 => Some(DataType::UInt32),
        SqlType::UInt64 => Some(DataType::UInt64),
        SqlType::Float32 => Some(DataType::Float32),
        SqlType::Float64 => Some(DataType::Float64),
        SqlType::String | SqlType::FixedString(_) => Some(DataType::Utf8),
        SqlType::Date => Some(DataType::Date32),
        SqlType::DateTime(_) => Some(DataType::Timestamp(TimeUnit::Second, None)),
        _ => unimplemented!("Unsupported column type {:?}", column_type),
    }
}
