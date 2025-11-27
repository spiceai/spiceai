use crate::sql::arrow_sql_gen::arrow::map_data_type_to_array_builder_optional;
use arrow::{
    array::{
        ArrayBuilder, ArrayRef, BinaryBuilder, Date32Builder, Decimal128Builder, Decimal256Builder,
        Float32Builder, Float64Builder, Int16Builder, Int32Builder, Int64Builder, Int8Builder,
        LargeBinaryBuilder, LargeStringBuilder, NullBuilder, RecordBatch, RecordBatchOptions,
        StringBuilder, StringDictionaryBuilder, Time64NanosecondBuilder,
        TimestampMicrosecondBuilder, UInt64Builder,
    },
    datatypes::{i256, DataType, Date32Type, Field, Schema, SchemaRef, TimeUnit, UInt16Type},
};
use bigdecimal::BigDecimal;
use bigdecimal::ToPrimitive;
use chrono::{NaiveDate, NaiveTime, Timelike};
use mysql_async::{consts::ColumnFlags, consts::ColumnType, FromValueError, Row, Value};
use snafu::{ResultExt, Snafu};
use std::{convert, sync::Arc};
use time::PrimitiveDateTime;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to build record batch: {source}"))]
    FailedToBuildRecordBatch {
        source: datafusion::arrow::error::ArrowError,
    },

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

    #[snafu(display("Failed to get a row value for column {column}({mysql_type:?}): {source}"))]
    FailedToGetRowValue {
        column: String,
        mysql_type: ColumnType,
        source: mysql_async::FromValueError,
    },

    #[snafu(display("Cannot represent BigDecimal as i128: {big_decimal}"))]
    FailedToConvertBigDecimalToI128 { big_decimal: BigDecimal },

    #[snafu(display("Failed to find field {column_name} in schema"))]
    FailedToFindFieldInSchema { column_name: String },

    #[snafu(display("No Arrow field found for index {index}"))]
    NoArrowFieldForIndex { index: usize },

    #[snafu(display("No column name for index: {index}"))]
    NoColumnNameForIndex { index: usize },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

macro_rules! handle_primitive_type {
    ($builder:expr, $type:expr, $builder_ty:ty, $value_ty:ty, $row:expr, $index:expr, $column_name:expr) => {{
        let Some(builder) = $builder else {
            return NoBuilderForIndexSnafu { index: $index }.fail();
        };
        let Some(builder) = builder.as_any_mut().downcast_mut::<$builder_ty>() else {
            return FailedToDowncastBuilderSnafu {
                mysql_type: format!("{:?}", $type),
            }
            .fail();
        };
        let v = handle_null_error($row.get_opt::<$value_ty, usize>($index).transpose()).context(
            FailedToGetRowValueSnafu {
                column: $column_name,
                mysql_type: $type,
            },
        )?;

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
pub fn rows_to_arrow(rows: &[Row], projected_schema: &Option<SchemaRef>) -> Result<RecordBatch> {
    let mut arrow_fields: Vec<Option<Field>> = Vec::new();
    let mut arrow_columns_builders: Vec<Option<Box<dyn ArrayBuilder>>> = Vec::new();
    let mut mysql_types: Vec<ColumnType> = Vec::new();
    let mut column_names: Vec<String> = Vec::new();
    let mut column_is_binary_stats: Vec<bool> = Vec::new();
    let mut column_is_enum_stats: Vec<bool> = Vec::new();
    let mut column_use_large_str_or_blob_stats: Vec<bool> = Vec::new();

    if !rows.is_empty() {
        let row = &rows[0];
        for column in row.columns().iter() {
            let column_name = column.name_str();
            let column_type = column.column_type();
            let column_is_binary = column.flags().contains(ColumnFlags::BINARY_FLAG);
            let column_is_enum = column.flags().contains(ColumnFlags::ENUM_FLAG);
            let column_use_large_str_or_blob = column.column_length() > 2_u32.pow(31) - 1;

            let (decimal_precision, decimal_scale) = match column_type {
                ColumnType::MYSQL_TYPE_DECIMAL | ColumnType::MYSQL_TYPE_NEWDECIMAL => {
                    // use 76 as default precision for decimal types if there is no way to get the precision from the column
                    match projected_schema {
                        Some(schema) => {
                            let precision =
                                get_decimal_column_precision(&column_name, schema).unwrap_or(76);
                            (Some(precision), Some(column.decimals() as i8))
                        }
                        None => (Some(76), Some(column.decimals() as i8)),
                    }
                }
                _ => (None, None),
            };

            let data_type = map_column_to_data_type(
                column_type,
                column_is_binary,
                column_is_enum,
                column_use_large_str_or_blob,
                decimal_precision,
                decimal_scale,
            );

            // Determine nullability from projected_schema if available, otherwise default to true
            let nullable = projected_schema
                .as_ref()
                .and_then(|schema| schema.field_with_name(&column_name).ok())
                .map(|field| field.is_nullable())
                .unwrap_or(true);

            arrow_fields.push(
                data_type
                    .clone()
                    .map(|data_type| Field::new(column_name.clone(), data_type.clone(), nullable)),
            );
            arrow_columns_builders
                .push(map_data_type_to_array_builder_optional(data_type.as_ref()));
            mysql_types.push(column_type);
            column_names.push(column_name.to_string());
            column_is_binary_stats.push(column_is_binary);
            column_is_enum_stats.push(column_is_enum);
            column_use_large_str_or_blob_stats.push(column_use_large_str_or_blob);
        }
    }

    for row in rows {
        for (i, mysql_type) in mysql_types.iter().enumerate() {
            let Some(builder) = arrow_columns_builders.get_mut(i) else {
                return NoBuilderForIndexSnafu { index: i }.fail();
            };

            let column_name = column_names.get(i).cloned().unwrap_or_default();

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
                            column: column_name,
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
                        i,
                        column_name
                    );
                }
                column_type @ (ColumnType::MYSQL_TYPE_SHORT | ColumnType::MYSQL_TYPE_YEAR) => {
                    handle_primitive_type!(
                        builder,
                        column_type,
                        Int16Builder,
                        i16,
                        row,
                        i,
                        column_name
                    );
                }
                column_type @ (ColumnType::MYSQL_TYPE_INT24 | ColumnType::MYSQL_TYPE_LONG) => {
                    handle_primitive_type!(
                        builder,
                        column_type,
                        Int32Builder,
                        i32,
                        row,
                        i,
                        column_name
                    );
                }
                ColumnType::MYSQL_TYPE_LONGLONG => {
                    handle_primitive_type!(
                        builder,
                        ColumnType::MYSQL_TYPE_LONGLONG,
                        Int64Builder,
                        i64,
                        row,
                        i,
                        column_name
                    );
                }
                ColumnType::MYSQL_TYPE_FLOAT => {
                    handle_primitive_type!(
                        builder,
                        ColumnType::MYSQL_TYPE_FLOAT,
                        Float32Builder,
                        f32,
                        row,
                        i,
                        column_name
                    );
                }
                ColumnType::MYSQL_TYPE_DOUBLE => {
                    handle_primitive_type!(
                        builder,
                        ColumnType::MYSQL_TYPE_DOUBLE,
                        Float64Builder,
                        f64,
                        row,
                        i,
                        column_name
                    );
                }
                ColumnType::MYSQL_TYPE_DECIMAL | ColumnType::MYSQL_TYPE_NEWDECIMAL => {
                    let Some(builder) = builder else {
                        return NoBuilderForIndexSnafu { index: i }.fail();
                    };

                    let arrow_field = match arrow_fields.get(i) {
                        Some(Some(field)) => field,
                        _ => return NoArrowFieldForIndexSnafu { index: i }.fail(),
                    };

                    match arrow_field.data_type() {
                        DataType::Decimal128(_, _) => {
                            let Some(builder) =
                                builder.as_any_mut().downcast_mut::<Decimal128Builder>()
                            else {
                                return FailedToDowncastBuilderSnafu {
                                    mysql_type: format!("{mysql_type:?}"),
                                }
                                .fail();
                            };
                            let val =
                                handle_null_error(row.get_opt::<BigDecimal, usize>(i).transpose())
                                    .context(FailedToGetRowValueSnafu {
                                        column: column_name,
                                        mysql_type: ColumnType::MYSQL_TYPE_DECIMAL,
                                    })?;

                            let scale = match &val {
                                Some(val) => val.fractional_digit_count(),
                                None => 0,
                            };

                            let Some(val) = val else {
                                builder.append_null();
                                continue;
                            };

                            let Some(val) = to_decimal_128(&val, scale) else {
                                return FailedToConvertBigDecimalToI128Snafu { big_decimal: val }
                                    .fail();
                            };

                            builder.append_value(val);
                        }
                        DataType::Decimal256(_, _) => {
                            let Some(builder) =
                                builder.as_any_mut().downcast_mut::<Decimal256Builder>()
                            else {
                                return FailedToDowncastBuilderSnafu {
                                    mysql_type: format!("{mysql_type:?}"),
                                }
                                .fail();
                            };

                            let val =
                                handle_null_error(row.get_opt::<BigDecimal, usize>(i).transpose())
                                    .context(FailedToGetRowValueSnafu {
                                        column: column_name,
                                        mysql_type: ColumnType::MYSQL_TYPE_DECIMAL,
                                    })?;

                            let Some(val) = val else {
                                builder.append_null();
                                continue;
                            };

                            let val = to_decimal_256(&val);

                            builder.append_value(val);
                        }
                        // ColumnType::MYSQL_TYPE_DECIMAL & ColumnType::MYSQL_TYPE_NEWDECIMAL are only mapped to Decimal128/Decimal256 in `map_column_to_data_type` function
                        _ => unreachable!(),
                    }
                }
                column_type @ (ColumnType::MYSQL_TYPE_VARCHAR | ColumnType::MYSQL_TYPE_JSON) => {
                    handle_primitive_type!(
                        builder,
                        column_type,
                        LargeStringBuilder,
                        String,
                        row,
                        i,
                        column_name
                    );
                }
                ColumnType::MYSQL_TYPE_BLOB => {
                    match (
                        column_use_large_str_or_blob_stats[i],
                        column_is_binary_stats[i],
                    ) {
                        (true, true) => handle_primitive_type!(
                            builder,
                            ColumnType::MYSQL_TYPE_BLOB,
                            LargeBinaryBuilder,
                            Vec<u8>,
                            row,
                            i,
                            column_name
                        ),
                        (true, false) => handle_primitive_type!(
                            builder,
                            ColumnType::MYSQL_TYPE_BLOB,
                            LargeStringBuilder,
                            String,
                            row,
                            i,
                            column_name
                        ),
                        (false, true) => handle_primitive_type!(
                            builder,
                            ColumnType::MYSQL_TYPE_BLOB,
                            BinaryBuilder,
                            Vec<u8>,
                            row,
                            i,
                            column_name
                        ),
                        (false, false) => handle_primitive_type!(
                            builder,
                            ColumnType::MYSQL_TYPE_BLOB,
                            StringBuilder,
                            String,
                            row,
                            i,
                            column_name
                        ),
                    }
                }
                ColumnType::MYSQL_TYPE_ENUM => {
                    // ENUM and SET values are returned as strings. For these, check that the type value is MYSQL_TYPE_STRING and that the ENUM_FLAG or SET_FLAG flag is set in the flags value.
                    // https://dev.mysql.com/doc/c-api/9.0/en/c-api-data-structures.html
                    unreachable!()
                }
                column_type @ (ColumnType::MYSQL_TYPE_STRING
                | ColumnType::MYSQL_TYPE_VAR_STRING) => {
                    // Handle MYSQL_TYPE_ENUM value
                    if column_is_enum_stats[i] {
                        let Some(builder) = builder else {
                            return NoBuilderForIndexSnafu { index: i }.fail();
                        };
                        let Some(builder) = builder
                            .as_any_mut()
                            .downcast_mut::<StringDictionaryBuilder<UInt16Type>>()
                        else {
                            return FailedToDowncastBuilderSnafu {
                                mysql_type: format!("{mysql_type:?}"),
                            }
                            .fail();
                        };

                        let v = handle_null_error(row.get_opt::<String, usize>(i).transpose())
                            .context(FailedToGetRowValueSnafu {
                                column: column_name,
                                mysql_type: ColumnType::MYSQL_TYPE_ENUM,
                            })?;

                        match v {
                            Some(v) => {
                                builder.append_value(v);
                            }
                            None => builder.append_null(),
                        }
                    } else if column_is_binary_stats[i] {
                        handle_primitive_type!(
                            builder,
                            column_type,
                            BinaryBuilder,
                            Vec<u8>,
                            row,
                            i,
                            column_name
                        );
                    } else {
                        handle_primitive_type!(
                            builder,
                            column_type,
                            StringBuilder,
                            String,
                            row,
                            i,
                            column_name
                        );
                    }
                }
                ColumnType::MYSQL_TYPE_DATE => {
                    let Some(builder) = builder else {
                        return NoBuilderForIndexSnafu { index: i }.fail();
                    };
                    let Some(builder) = builder.as_any_mut().downcast_mut::<Date32Builder>() else {
                        return FailedToDowncastBuilderSnafu {
                            mysql_type: format!("{mysql_type:?}"),
                        }
                        .fail();
                    };

                    let v = match handle_null_error(row.get_opt::<NaiveDate, usize>(i).transpose())
                    {
                        Ok(v) => v,
                        Err(err) => {
                            // Handle '0000-00-00', that can't be parsed automatically. For more details: https://dev.mysql.com/doc/refman/8.4/en/using-date.html
                            if matches!(err, FromValueError(Value::Date(0, 0, 0, 0, 0, 0, 0))) {
                                None
                            } else {
                                return Err(Error::FailedToGetRowValue {
                                    column: column_name,
                                    mysql_type: ColumnType::MYSQL_TYPE_DATE,
                                    source: err,
                                });
                            }
                        }
                    };

                    match v {
                        Some(v) => {
                            builder.append_value(Date32Type::from_naive_date(v));
                        }
                        None => builder.append_null(),
                    }
                }
                ColumnType::MYSQL_TYPE_TIME => {
                    let Some(builder) = builder else {
                        return NoBuilderForIndexSnafu { index: i }.fail();
                    };
                    let Some(builder) = builder
                        .as_any_mut()
                        .downcast_mut::<Time64NanosecondBuilder>()
                    else {
                        return FailedToDowncastBuilderSnafu {
                            mysql_type: format!("{mysql_type:?}"),
                        }
                        .fail();
                    };
                    let v = handle_null_error(row.get_opt::<NaiveTime, usize>(i).transpose())
                        .context(FailedToGetRowValueSnafu {
                            column: column_name,
                            mysql_type: ColumnType::MYSQL_TYPE_TIME,
                        })?;

                    match v {
                        Some(value) => {
                            builder.append_value(
                                i64::from(value.num_seconds_from_midnight()) * 1_000_000_000
                                    + i64::from(value.nanosecond()),
                            );
                        }
                        None => builder.append_null(),
                    }
                }
                column_type @ (ColumnType::MYSQL_TYPE_TIMESTAMP
                | ColumnType::MYSQL_TYPE_DATETIME) => {
                    let Some(builder) = builder else {
                        return NoBuilderForIndexSnafu { index: i }.fail();
                    };
                    let Some(builder) = builder
                        .as_any_mut()
                        .downcast_mut::<TimestampMicrosecondBuilder>()
                    else {
                        return FailedToDowncastBuilderSnafu {
                            mysql_type: format!("{mysql_type:?}"),
                        }
                        .fail();
                    };
                    let v = match handle_null_error(
                        row.get_opt::<PrimitiveDateTime, usize>(i).transpose(),
                    ) {
                        Ok(v) => v,
                        Err(err) => {
                            // Handle '0000-00-00', that can't be parsed automatically. For more details: https://dev.mysql.com/doc/refman/8.4/en/using-date.html
                            if matches!(err, FromValueError(Value::Date(0, 0, 0, 0, 0, 0, 0))) {
                                None
                            } else {
                                return Err(Error::FailedToGetRowValue {
                                    column: column_name,
                                    mysql_type: column_type,
                                    source: err,
                                });
                            }
                        }
                    };

                    match v {
                        Some(v) => {
                            let micros: i64 = (v.assume_utc().unix_timestamp_nanos() / 1000)
                                .try_into()
                                .unwrap();
                            builder.append_value(micros);
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
pub fn map_column_to_data_type(
    column_type: ColumnType,
    column_is_binary: bool,
    column_is_enum: bool,
    column_use_large_str_or_blob: bool,
    column_decimal_precision: Option<u8>,
    column_decimal_scale: Option<i8>,
) -> Option<DataType> {
    match column_type {
        ColumnType::MYSQL_TYPE_NULL => Some(DataType::Null),
        ColumnType::MYSQL_TYPE_BIT => Some(DataType::UInt64),
        ColumnType::MYSQL_TYPE_TINY => Some(DataType::Int8),
        ColumnType::MYSQL_TYPE_YEAR | ColumnType::MYSQL_TYPE_SHORT => Some(DataType::Int16),
        ColumnType::MYSQL_TYPE_INT24 | ColumnType::MYSQL_TYPE_LONG => Some(DataType::Int32),
        ColumnType::MYSQL_TYPE_LONGLONG => Some(DataType::Int64),
        ColumnType::MYSQL_TYPE_FLOAT => Some(DataType::Float32),
        ColumnType::MYSQL_TYPE_DOUBLE => Some(DataType::Float64),
        // Decimal precision must be a value between 0x00 - 0x51, so it's safe to unwrap_or_default here
        ColumnType::MYSQL_TYPE_DECIMAL | ColumnType::MYSQL_TYPE_NEWDECIMAL => {
            if column_decimal_precision.unwrap_or_default() > 38 {
                return Some(DataType::Decimal256(column_decimal_precision.unwrap_or_default(), column_decimal_scale.unwrap_or_default()));
            }
            Some(DataType::Decimal128(column_decimal_precision.unwrap_or_default(), column_decimal_scale.unwrap_or_default()))
        },
        ColumnType::MYSQL_TYPE_TIMESTAMP | ColumnType::MYSQL_TYPE_DATETIME => {
            Some(DataType::Timestamp(TimeUnit::Microsecond, None))
        },
        ColumnType::MYSQL_TYPE_DATE => Some(DataType::Date32),
        ColumnType::MYSQL_TYPE_TIME => {
            Some(DataType::Time64(TimeUnit::Nanosecond))
        }
        ColumnType::MYSQL_TYPE_VARCHAR
        | ColumnType::MYSQL_TYPE_JSON => Some(DataType::LargeUtf8),
        // MYSQL_TYPE_BLOB includes TINYBLOB, BLOB, MEDIUMBLOB, LONGBLOB, TINYTEXT, TEXT, MEDIUMTEXT, LONGTEXT https://dev.mysql.com/doc/c-api/8.0/en/c-api-data-structures.html
        // MySQL String Type Storage requirement: https://dev.mysql.com/doc/refman/8.4/en/storage-requirements.html
        // Binary / Utf8 stores up to 2^31 - 1 length binary / non-binary string
        ColumnType::MYSQL_TYPE_BLOB => {
            match (column_use_large_str_or_blob, column_is_binary) {
                (true, true) => Some(DataType::LargeBinary),
                (true, false) => Some(DataType::LargeUtf8),
                (false, true) => Some(DataType::Binary),
                (false, false) => Some(DataType::Utf8),
            }
        }
        ColumnType::MYSQL_TYPE_ENUM | ColumnType::MYSQL_TYPE_SET => unreachable!(),
        ColumnType::MYSQL_TYPE_STRING
        | ColumnType::MYSQL_TYPE_VAR_STRING => {
            if column_is_enum {
                Some(DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8)))
            } else if column_is_binary {
                Some(DataType::Binary)
            } else {
                Some(DataType::Utf8)
            }
        },
        // replication only
        ColumnType::MYSQL_TYPE_TYPED_ARRAY
        // internal
        | ColumnType::MYSQL_TYPE_NEWDATE
        // Unsupported yet
        | ColumnType::MYSQL_TYPE_UNKNOWN
        | ColumnType::MYSQL_TYPE_TIMESTAMP2
        | ColumnType::MYSQL_TYPE_DATETIME2
        | ColumnType::MYSQL_TYPE_TIME2
        | ColumnType::MYSQL_TYPE_LONG_BLOB
        | ColumnType::MYSQL_TYPE_TINY_BLOB
        | ColumnType::MYSQL_TYPE_MEDIUM_BLOB
        | ColumnType::MYSQL_TYPE_GEOMETRY
        | ColumnType::MYSQL_TYPE_VECTOR => {
            unimplemented!("Unsupported column type {:?}", column_type)
        }
    }
}

fn to_decimal_128(decimal: &BigDecimal, scale: i64) -> Option<i128> {
    (decimal * 10i128.pow(scale.try_into().unwrap_or_default())).to_i128()
}

fn to_decimal_256(decimal: &BigDecimal) -> i256 {
    let (bigint_value, _) = decimal.as_bigint_and_exponent();
    let mut bigint_bytes = bigint_value.to_signed_bytes_le();

    let is_negative = bigint_value.sign() == num_bigint::Sign::Minus;
    let fill_byte = if is_negative { 0xFF } else { 0x00 };

    if bigint_bytes.len() > 32 {
        bigint_bytes.truncate(32);
    } else {
        bigint_bytes.resize(32, fill_byte);
    };

    let mut array = [0u8; 32];
    array.copy_from_slice(&bigint_bytes);

    i256::from_le_bytes(array)
}

fn get_decimal_column_precision(column_name: &str, projected_schema: &SchemaRef) -> Option<u8> {
    let field = projected_schema.field_with_name(column_name).ok()?;
    match field.data_type() {
        DataType::Decimal256(precision, _) | DataType::Decimal128(precision, _) => Some(*precision),
        _ => None,
    }
}
fn handle_null_error<T>(
    result: Result<Option<T>, FromValueError>,
) -> Result<Option<T>, FromValueError> {
    match result {
        Ok(val) => Ok(val),
        Err(FromValueError(Value::NULL)) => Ok(None),
        err => err,
    }
}
