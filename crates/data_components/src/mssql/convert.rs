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

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime};
use datafusion_table_providers::sql::arrow_sql_gen::arrow::map_data_type_to_array_builder;
use tiberius::{numeric::Numeric, xml::XmlData, ColumnType, Row};
use uuid::Uuid;

use arrow::{
    array::{
        ArrayBuilder, ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder,
        Float32Builder, Float64Builder, Int16Builder, Int32Builder, Int64Builder, NullBuilder,
        RecordBatch, RecordBatchOptions, StringBuilder, Time64NanosecondBuilder,
        TimestampMillisecondBuilder, TimestampNanosecondBuilder, UInt8Builder,
    },
    datatypes::{DataType, Date32Type, SchemaRef, TimeUnit},
};

use chrono::Timelike;

macro_rules! handle_primitive_type {
    ($builder:expr, $type:expr, $builder_ty:ty, $value_ty:ty, $row:expr, $index:expr) => {{
        let Some(builder) = $builder.as_any_mut().downcast_mut::<$builder_ty>() else {
            return super::FailedToDowncastBuilderSnafu {
                mssql_type: format!("{:?}", $type),
            }
            .fail();
        };
        let v = $row.get::<$value_ty, usize>($index);
        match v {
            Some(v) => builder.append_value(v),
            None => builder.append_null(),
        }
    }};
}

#[allow(clippy::too_many_lines)]
pub(crate) fn rows_to_arrow(rows: &[Row], schema: &SchemaRef) -> super::Result<RecordBatch> {
    let mut arrow_columns_builders: Vec<Box<dyn ArrayBuilder>> = Vec::new();
    let mut mssql_types: Vec<ColumnType> = Vec::new();
    let mut column_names: Vec<String> = Vec::new();

    if !rows.is_empty() {
        let row = &rows[0];
        for column in row.columns() {
            let column_name = column.name();
            let column_type = column.column_type();
            let (decimal_precision, decimal_scale) = match column_type {
                ColumnType::Decimaln | ColumnType::Numericn => {
                    // use 38, 10 as default precision and scale for decimal types
                    let (precision, scale) =
                        get_column_precision_and_scale(column_name, schema).unwrap_or((38, 10));
                    (Some(precision), Some(scale))
                }
                _ => (None, None),
            };
            let data_type =
                map_column_type_to_arrow_type(column_type, decimal_precision, decimal_scale);

            arrow_columns_builders.push(map_data_type_to_array_builder(&data_type));
            mssql_types.push(column_type);
            column_names.push(column_name.to_string());
        }
    }

    for row in rows {
        for (i, mssql_type) in mssql_types.iter().enumerate() {
            let Some(builder) = arrow_columns_builders.get_mut(i) else {
                return super::NoBuilderForIndexSnafu { index: i }.fail();
            };
            match *mssql_type {
                ColumnType::Null => {
                    let Some(builder) = builder.as_any_mut().downcast_mut::<NullBuilder>() else {
                        return super::FailedToDowncastBuilderSnafu {
                            mssql_type: format!("{mssql_type:?}"),
                        }
                        .fail();
                    };
                    builder.append_null();
                }
                ColumnType::Int4 => {
                    handle_primitive_type!(builder, ColumnType::Int4, Int32Builder, i32, row, i);
                }
                ColumnType::Int1 => {
                    handle_primitive_type!(builder, ColumnType::Int1, UInt8Builder, u8, row, i);
                }
                ColumnType::Int2 => {
                    handle_primitive_type!(builder, ColumnType::Int2, Int16Builder, i16, row, i);
                }
                ColumnType::Int8 => {
                    handle_primitive_type!(builder, ColumnType::Int8, Int64Builder, i64, row, i);
                }
                ColumnType::Float4 => {
                    handle_primitive_type!(
                        builder,
                        ColumnType::Float4,
                        Float32Builder,
                        f32,
                        row,
                        i
                    );
                }
                ColumnType::Float8 | ColumnType::Money | ColumnType::Money4 => {
                    handle_primitive_type!(builder, *mssql_type, Float64Builder, f64, row, i);
                }
                ColumnType::Bit | ColumnType::Bitn => {
                    handle_primitive_type!(builder, *mssql_type, BooleanBuilder, bool, row, i);
                }
                ColumnType::Datetime | ColumnType::Datetimen | ColumnType::Datetime4 => {
                    let Some(builder) = builder
                        .as_any_mut()
                        .downcast_mut::<TimestampMillisecondBuilder>()
                    else {
                        return super::FailedToDowncastBuilderSnafu {
                            mssql_type: format!("{mssql_type:?}"),
                        }
                        .fail();
                    };
                    let v = row.get::<NaiveDateTime, usize>(i);
                    match v {
                        Some(v) => builder.append_value(v.and_utc().timestamp_millis()),
                        None => builder.append_null(),
                    }
                }
                ColumnType::Datetime2 => {
                    let Some(builder) = builder
                        .as_any_mut()
                        .downcast_mut::<TimestampNanosecondBuilder>()
                    else {
                        return super::FailedToDowncastBuilderSnafu {
                            mssql_type: format!("{mssql_type:?}"),
                        }
                        .fail();
                    };
                    let v = row.get::<NaiveDateTime, usize>(i);
                    match v {
                        Some(v) => builder
                            .append_value(v.and_utc().timestamp_nanos_opt().unwrap_or_default()),
                        None => builder.append_null(),
                    }
                }
                ColumnType::DatetimeOffsetn => {
                    let Some(builder) = builder
                        .as_any_mut()
                        .downcast_mut::<TimestampNanosecondBuilder>()
                    else {
                        return super::FailedToDowncastBuilderSnafu {
                            mssql_type: format!("{mssql_type:?}"),
                        }
                        .fail();
                    };

                    let v = row.get::<DateTime<chrono::FixedOffset>, usize>(i);
                    match v {
                        Some(v) => {
                            let utc_value = v.with_timezone(&chrono::Utc);
                            builder
                                .append_value(utc_value.timestamp_nanos_opt().unwrap_or_default());
                        }
                        None => builder.append_null(),
                    }
                }
                ColumnType::Daten => {
                    let Some(builder) = builder.as_any_mut().downcast_mut::<Date32Builder>() else {
                        return super::FailedToDowncastBuilderSnafu {
                            mssql_type: format!("{mssql_type:?}"),
                        }
                        .fail();
                    };
                    let v = row.get::<NaiveDate, usize>(i);
                    match v {
                        Some(v) => builder.append_value(Date32Type::from_naive_date(v)),
                        None => builder.append_null(),
                    }
                }
                ColumnType::Timen => {
                    let Some(builder) = builder
                        .as_any_mut()
                        .downcast_mut::<Time64NanosecondBuilder>()
                    else {
                        return super::FailedToDowncastBuilderSnafu {
                            mssql_type: format!("{mssql_type:?}"),
                        }
                        .fail();
                    };
                    let v = row.get::<NaiveTime, usize>(i);
                    match v {
                        Some(v) => {
                            let timestamp: i64 = i64::from(v.num_seconds_from_midnight())
                                * 1_000_000_000
                                + i64::from(v.nanosecond());
                            builder.append_value(timestamp);
                        }
                        None => builder.append_null(),
                    }
                }
                ColumnType::Decimaln | ColumnType::Numericn => {
                    let Some(builder) = builder.as_any_mut().downcast_mut::<Decimal128Builder>()
                    else {
                        return super::FailedToDowncastBuilderSnafu {
                            mssql_type: format!("{mssql_type:?}"),
                        }
                        .fail();
                    };
                    let v = row.get::<Numeric, usize>(i);
                    match v {
                        Some(v) => builder.append_value(v.value()),
                        None => builder.append_null(),
                    }
                }
                ColumnType::Guid => {
                    let Some(builder) = builder.as_any_mut().downcast_mut::<StringBuilder>() else {
                        return super::FailedToDowncastBuilderSnafu {
                            mssql_type: format!("{mssql_type:?}"),
                        }
                        .fail();
                    };
                    let v = row.get::<Uuid, usize>(i);
                    match v {
                        Some(v) => builder.append_value(v.to_string()),
                        None => builder.append_null(),
                    }
                }
                ColumnType::Xml => {
                    let Some(builder) = builder.as_any_mut().downcast_mut::<StringBuilder>() else {
                        return super::FailedToDowncastBuilderSnafu {
                            mssql_type: format!("{mssql_type:?}"),
                        }
                        .fail();
                    };
                    let v = row.get::<&XmlData, usize>(i);
                    match v {
                        Some(v) => builder.append_value(v),
                        None => builder.append_null(),
                    }
                }
                ColumnType::Image | ColumnType::BigVarBin | ColumnType::BigBinary => {
                    let Some(builder) = builder.as_any_mut().downcast_mut::<BinaryBuilder>() else {
                        return super::FailedToDowncastBuilderSnafu {
                            mssql_type: format!("{mssql_type:?}"),
                        }
                        .fail();
                    };
                    let v = row.get::<&[u8], usize>(i);
                    match v {
                        Some(v) => builder.append_value(v),
                        None => builder.append_null(),
                    }
                }
                ColumnType::BigVarChar
                | ColumnType::BigChar
                | ColumnType::NVarchar
                | ColumnType::NChar
                | ColumnType::Udt
                | ColumnType::Text
                | ColumnType::NText
                | ColumnType::SSVariant => {
                    let Some(builder) = builder.as_any_mut().downcast_mut::<StringBuilder>() else {
                        return super::FailedToDowncastBuilderSnafu {
                            mssql_type: format!("{mssql_type:?}"),
                        }
                        .fail();
                    };
                    let v = row.get::<&str, usize>(i);
                    match v {
                        Some(v) => {
                            if matches!(mssql_type, ColumnType::BigChar | ColumnType::NChar) {
                                builder.append_value(v.trim_end()); // MS SQL returns space-padded chars to the length of the char
                                                                    // memory execution does not trim space-padded chars in WHERE clauses, but MS SQL does - trim incoming CHARs to replicate this behavior
                            } else {
                                builder.append_value(v);
                            }
                        }
                        None => builder.append_null(),
                    }
                }
                _ => {
                    return super::UnsupportedTypeSnafu {
                        data_type: format!("{mssql_type:?}"),
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

pub(crate) fn map_column_type_to_arrow_type(
    column_type: ColumnType,
    decimal_precision: Option<u8>,
    decimal_scale: Option<i8>,
) -> DataType {
    match column_type {
        ColumnType::Null => DataType::Null,
        // https://learn.microsoft.com/en-us/sql/t-sql/data-types/int-bigint-smallint-and-tinyint-transact-sql
        ColumnType::Int1 => DataType::UInt8,
        ColumnType::Int2 => DataType::Int16,
        ColumnType::Int4 => DataType::Int32,
        ColumnType::Int8 | ColumnType::Intn => DataType::Int64,
        ColumnType::Float4 => DataType::Float32,
        ColumnType::Float8 | ColumnType::Floatn | ColumnType::Money | ColumnType::Money4 => {
            DataType::Float64
        }
        ColumnType::Datetime4 | ColumnType::Datetime | ColumnType::Datetimen => {
            DataType::Timestamp(TimeUnit::Millisecond, None)
        }
        ColumnType::Datetime2 => DataType::Timestamp(TimeUnit::Nanosecond, None),
        ColumnType::DatetimeOffsetn => {
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
        }
        ColumnType::Decimaln | ColumnType::Numericn => {
            let precision = decimal_precision.unwrap_or(38);
            let scale = decimal_scale.unwrap_or(10);
            DataType::Decimal128(precision, scale)
        }
        ColumnType::Daten => DataType::Date32,
        ColumnType::Timen => DataType::Time64(TimeUnit::Nanosecond),
        ColumnType::Image | ColumnType::BigVarBin | ColumnType::BigBinary => DataType::Binary,
        ColumnType::Guid
        | ColumnType::BigVarChar
        | ColumnType::BigChar
        | ColumnType::NVarchar
        | ColumnType::NChar
        | ColumnType::Xml
        | ColumnType::Udt
        | ColumnType::Text
        | ColumnType::NText
        | ColumnType::SSVariant => DataType::Utf8,
        ColumnType::Bit | ColumnType::Bitn => DataType::Boolean,
    }
}

pub(crate) fn map_type_name_to_column_type(data_type: &str) -> Option<ColumnType> {
    // https://github.com/prisma/tiberius/blob/51f0cbb3e430db74ba0ea4830b236e89f1b1e03f/src/tds/codec/token/token_col_metadata.rs#L26
    // Data types (Transact-SQL): https://learn.microsoft.com/en-us/sql/t-sql/data-types/data-types-transact-sql
    let column_type = match data_type.to_lowercase().as_str() {
        "int" => ColumnType::Int4,
        "bigint" => ColumnType::Int8,
        "smallint" => ColumnType::Int2,
        "tinyint" => ColumnType::Int1,
        "float" => ColumnType::Float8,
        "real" => ColumnType::Float4,
        "decimal" | "numeric" => ColumnType::Decimaln,
        "char" | "varchar" | "text" => ColumnType::BigVarChar,
        "nchar" | "nvarchar" | "ntext" => ColumnType::NVarchar,
        "uniqueidentifier" => ColumnType::Guid,
        "binary" | "timestamp" | "rowversion" => ColumnType::BigBinary, // Timestamp is not a typical ISO timestamp in MS SQL - it's a binary synonym for rowversion
        "varbinary" | "image" => ColumnType::BigVarBin,
        "xml" => ColumnType::Xml,
        "money" => ColumnType::Money,
        "smallmoney" => ColumnType::Money4,
        "date" => ColumnType::Daten,
        "time" => ColumnType::Timen,
        "datetime" => ColumnType::Datetime,
        "smalldatetime" => ColumnType::Datetime4,
        "datetime2" => ColumnType::Datetime2,
        "datetimeoffset" => ColumnType::DatetimeOffsetn,
        "bit" => ColumnType::Bit,
        // ColumnType::Udt types such as Spatial/geography are not supported by Tiberius
        _ => {
            return None;
        }
    };

    Some(column_type)
}

fn get_column_precision_and_scale(
    column_name: &str,
    projected_schema: &SchemaRef,
) -> Option<(u8, i8)> {
    let field = projected_schema.field_with_name(column_name).ok()?;
    match field.data_type() {
        DataType::Decimal128(precision, scale) => Some((*precision, *scale)),
        _ => None,
    }
}
