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

use std::{str::FromStr, sync::Arc};

use arrow::{
    array::{
        ArrayBuilder, ArrayRef, BooleanBuilder, Date32Builder, Decimal128Builder, Float32Builder,
        Float64Builder, Int16Builder, Int32Builder, Int64Builder, Int8Builder, RecordBatch,
        RecordBatchOptions, StringBuilder, TimestampSecondBuilder, UInt16Builder, UInt32Builder,
        UInt64Builder, UInt8Builder,
    },
    datatypes::{DataType, Date32Type, Field, Schema, TimeUnit},
};
use bigdecimal::{BigDecimal, ToPrimitive};
use chrono::NaiveDate;
use chrono_tz::Tz;
use clickhouse_rs::{
    types::{ColumnType, Decimal, SqlType},
    Block,
};
use snafu::{ResultExt, Snafu};

use datafusion_table_providers::sql::arrow_sql_gen::arrow::map_data_type_to_array_builder;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to build record batch: {source}"))]
    FailedToBuildRecordBatch { source: arrow::error::ArrowError },

    #[snafu(display("No builder found for index {index}"))]
    NoBuilderForIndex { index: usize },

    #[snafu(display("Failed to downcast builder for {clickhouse_type}"))]
    FailedToDowncastBuilder { clickhouse_type: SqlType },

    #[snafu(display("Failed to get a row value for {clickhouse_type}: {source}"))]
    FailedToGetRowValue {
        clickhouse_type: SqlType,
        source: clickhouse_rs::errors::Error,
    },

    #[snafu(display("Failed to append a row value for {}: {}", clickhouse_type, source))]
    FailedToAppendRowValue {
        clickhouse_type: SqlType,
        source: arrow::error::ArrowError,
    },

    #[snafu(display("No Arrow field found for index {index}"))]
    NoArrowFieldForIndex { index: usize },

    #[snafu(display("No column name for index: {index}"))]
    NoColumnNameForIndex { index: usize },

    #[snafu(display("Cannot represent BigDecimal as i128: {big_decimal}"))]
    FailedToConvertBigDecimalToI128 { big_decimal: BigDecimal },

    #[snafu(display("Failed to parse decimal string as BigInterger {value}: {source}"))]
    FailedToParseBigDecimalFromClickhouse {
        value: String,
        source: bigdecimal::ParseBigDecimalError,
    },

    #[snafu(display("Unsupported column type: {:?}", column_type))]
    UnsupportedColumnType { column_type: SqlType },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

macro_rules! handle_primitive_type {
    ($builder:expr, $type:expr, $builder_ty:ty, $value_ty:ty, $row:expr, $index:expr) => {{
        let Some(builder) = $builder else {
            return NoBuilderForIndexSnafu { index: $index }.fail();
        };
        let Some(builder) = builder.as_any_mut().downcast_mut::<$builder_ty>() else {
            return FailedToDowncastBuilderSnafu {
                clickhouse_type: $type,
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

macro_rules! handle_primitive_nullable_type {
    ($builder:expr, $type:expr, $builder_ty:ty, $value_ty:ty, $row:expr, $index:expr) => {{
        let Some(builder) = $builder else {
            return NoBuilderForIndexSnafu { index: $index }.fail();
        };
        let Some(builder) = builder.as_any_mut().downcast_mut::<$builder_ty>() else {
            return FailedToDowncastBuilderSnafu {
                clickhouse_type: $type,
            }
            .fail();
        };
        let v = $row
            .get::<Option<$value_ty>, usize>($index)
            .context(FailedToGetRowValueSnafu {
                clickhouse_type: $type,
            })?;

        match v {
            Some(v) => builder.append_value(v),
            None => builder.append_null(),
        }
    }};
}

/// Converts `Clickhouse` `Block` to an Arrow `RecordBatch`. Assumes that all rows have the same schema and
/// sets the schema based on the `sql_type` returned for column.
///
/// # Errors
///
/// Returns an error if there is a failure in converting the rows to a `RecordBatch`.
#[allow(clippy::too_many_lines)]
pub fn block_to_arrow<T: ColumnType>(block: &Block<T>) -> Result<RecordBatch> {
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
            arrow_fields.push(Some(Field::new(column_name, data_type.clone(), true)));
            arrow_columns_builders.push(Some(map_data_type_to_array_builder(&data_type)));
            clickhouse_types.push(column_type);
            column_names.push(column_name.to_string());
        }
    }

    for row in block.rows() {
        for (i, clickhouse_type) in clickhouse_types.iter().enumerate() {
            let Some(builder) = arrow_columns_builders.get_mut(i) else {
                return NoBuilderForIndexSnafu { index: i }.fail();
            };

            let Some(arrow_field) = arrow_fields.get_mut(i) else {
                return NoArrowFieldForIndexSnafu { index: i }.fail();
            };

            match *clickhouse_type {
                SqlType::Uuid | SqlType::Nullable(SqlType::Uuid) => {
                    let Some(builder) = builder else {
                        return NoBuilderForIndexSnafu { index: i }.fail();
                    };
                    let Some(builder) = builder.as_any_mut().downcast_mut::<StringBuilder>() else {
                        return FailedToDowncastBuilderSnafu {
                            clickhouse_type: SqlType::Uuid,
                        }
                        .fail();
                    };
                    let v = match *clickhouse_type {
                        SqlType::Uuid => Some(row.get::<uuid::Uuid, usize>(i).context(
                            FailedToGetRowValueSnafu {
                                clickhouse_type: SqlType::Uuid,
                            },
                        )?),
                        SqlType::Nullable(SqlType::Uuid) => row
                            .get::<Option<uuid::Uuid>, usize>(i)
                            .context(FailedToGetRowValueSnafu {
                                clickhouse_type: SqlType::Uuid,
                            })?,
                        _ => unreachable!(),
                    };

                    match v {
                        Some(v) => builder.append_value(v.to_string()),
                        None => builder.append_null(),
                    }
                }
                SqlType::Bool => {
                    handle_primitive_type!(builder, SqlType::Bool, BooleanBuilder, bool, row, i);
                }
                SqlType::Nullable(SqlType::Bool) => {
                    handle_primitive_nullable_type!(
                        builder,
                        SqlType::Bool,
                        BooleanBuilder,
                        bool,
                        row,
                        i
                    );
                }
                SqlType::Int8 => {
                    handle_primitive_type!(builder, SqlType::Int8, Int8Builder, i8, row, i);
                }
                SqlType::Nullable(SqlType::Int8) => {
                    handle_primitive_nullable_type!(
                        builder,
                        SqlType::Int8,
                        Int8Builder,
                        i8,
                        row,
                        i
                    );
                }
                SqlType::Int16 => {
                    handle_primitive_type!(builder, SqlType::Int16, Int16Builder, i16, row, i);
                }
                SqlType::Nullable(SqlType::Int16) => {
                    handle_primitive_nullable_type!(
                        builder,
                        SqlType::Int16,
                        Int16Builder,
                        i16,
                        row,
                        i
                    );
                }
                SqlType::Int32 => {
                    handle_primitive_type!(builder, SqlType::Int32, Int32Builder, i32, row, i);
                }
                SqlType::Nullable(SqlType::Int32) => {
                    handle_primitive_nullable_type!(
                        builder,
                        SqlType::Int32,
                        Int32Builder,
                        i32,
                        row,
                        i
                    );
                }
                SqlType::Int64 => {
                    handle_primitive_type!(builder, SqlType::Int64, Int64Builder, i64, row, i);
                }
                SqlType::Nullable(SqlType::Int64) => {
                    handle_primitive_nullable_type!(
                        builder,
                        SqlType::Int64,
                        Int64Builder,
                        i64,
                        row,
                        i
                    );
                }
                SqlType::UInt8 => {
                    handle_primitive_type!(builder, SqlType::UInt8, UInt8Builder, u8, row, i);
                }
                SqlType::Nullable(SqlType::UInt8) => {
                    handle_primitive_nullable_type!(
                        builder,
                        SqlType::UInt8,
                        UInt8Builder,
                        u8,
                        row,
                        i
                    );
                }
                SqlType::UInt16 => {
                    handle_primitive_type!(builder, SqlType::UInt16, UInt16Builder, u16, row, i);
                }
                SqlType::Nullable(SqlType::UInt16) => {
                    handle_primitive_nullable_type!(
                        builder,
                        SqlType::UInt16,
                        UInt16Builder,
                        u16,
                        row,
                        i
                    );
                }
                SqlType::UInt32 => {
                    handle_primitive_type!(builder, SqlType::UInt32, UInt32Builder, u32, row, i);
                }
                SqlType::Nullable(SqlType::UInt32) => {
                    handle_primitive_nullable_type!(
                        builder,
                        SqlType::UInt32,
                        UInt32Builder,
                        u32,
                        row,
                        i
                    );
                }
                SqlType::UInt64 => {
                    handle_primitive_type!(builder, SqlType::UInt64, UInt64Builder, u64, row, i);
                }
                SqlType::Nullable(SqlType::UInt64) => {
                    handle_primitive_nullable_type!(
                        builder,
                        SqlType::UInt64,
                        UInt64Builder,
                        u64,
                        row,
                        i
                    );
                }
                SqlType::Float32 => {
                    handle_primitive_type!(builder, SqlType::Float32, Float32Builder, f32, row, i);
                }
                SqlType::Nullable(SqlType::Float32) => {
                    handle_primitive_nullable_type!(
                        builder,
                        SqlType::Float32,
                        Float32Builder,
                        f32,
                        row,
                        i
                    );
                }
                SqlType::Float64 => {
                    handle_primitive_type!(builder, SqlType::Float64, Float64Builder, f64, row, i);
                }
                SqlType::Nullable(SqlType::Float64) => {
                    handle_primitive_nullable_type!(
                        builder,
                        SqlType::Float64,
                        Float64Builder,
                        f64,
                        row,
                        i
                    );
                }
                SqlType::String => {
                    handle_primitive_type!(builder, SqlType::String, StringBuilder, String, row, i);
                }
                SqlType::Nullable(SqlType::String) => {
                    handle_primitive_nullable_type!(
                        builder,
                        SqlType::String,
                        StringBuilder,
                        String,
                        row,
                        i
                    );
                }
                SqlType::FixedString(size) => {
                    handle_primitive_type!(
                        builder,
                        SqlType::FixedString(size),
                        StringBuilder,
                        String,
                        row,
                        i
                    );
                }
                SqlType::Nullable(SqlType::FixedString(size)) => {
                    handle_primitive_nullable_type!(
                        builder,
                        SqlType::FixedString(*size),
                        StringBuilder,
                        String,
                        row,
                        i
                    );
                }
                SqlType::Date | SqlType::Nullable(SqlType::Date) => {
                    let Some(builder) = builder else {
                        return NoBuilderForIndexSnafu { index: i }.fail();
                    };
                    let Some(builder) = builder.as_any_mut().downcast_mut::<Date32Builder>() else {
                        return FailedToDowncastBuilderSnafu {
                            clickhouse_type: SqlType::Date,
                        }
                        .fail();
                    };
                    let v = match *clickhouse_type {
                        SqlType::Date => Some(row.get::<NaiveDate, usize>(i).context(
                            FailedToGetRowValueSnafu {
                                clickhouse_type: SqlType::Date,
                            },
                        )?),
                        SqlType::Nullable(SqlType::Date) => row
                            .get::<Option<NaiveDate>, usize>(i)
                            .context(FailedToGetRowValueSnafu {
                                clickhouse_type: SqlType::Date,
                            })?,
                        _ => unreachable!(),
                    };
                    match v {
                        Some(v) => builder.append_value(Date32Type::from_naive_date(v)),
                        None => builder.append_null(),
                    }
                }
                SqlType::DateTime(ref date_type)
                | SqlType::Nullable(SqlType::DateTime(ref date_type)) => {
                    let Some(builder) = builder else {
                        return NoBuilderForIndexSnafu { index: i }.fail();
                    };
                    let Some(builder) = builder
                        .as_any_mut()
                        .downcast_mut::<TimestampSecondBuilder>()
                    else {
                        return FailedToDowncastBuilderSnafu {
                            clickhouse_type: SqlType::DateTime(*date_type),
                        }
                        .fail();
                    };
                    let v = match *clickhouse_type {
                        SqlType::DateTime(_) => {
                            Some(row.get::<chrono::DateTime<Tz>, usize>(i).context(
                                FailedToGetRowValueSnafu {
                                    clickhouse_type: SqlType::DateTime(*date_type),
                                },
                            )?)
                        }
                        SqlType::Nullable(SqlType::DateTime(_)) => row
                            .get::<Option<chrono::DateTime<Tz>>, usize>(i)
                            .context(FailedToGetRowValueSnafu {
                                clickhouse_type: SqlType::DateTime(*date_type),
                            })?,
                        _ => unreachable!(),
                    };
                    match v {
                        Some(v) => builder.append_value(v.timestamp()),
                        None => builder.append_null(),
                    }
                }

                SqlType::Decimal(ref size, ref align)
                | SqlType::Nullable(SqlType::Decimal(ref size, ref align)) => {
                    let size = *size;
                    let align = *align;
                    let scale = align.try_into().unwrap_or_default();
                    let dec_builder = builder.get_or_insert_with(|| {
                        Box::new(
                            Decimal128Builder::new()
                                .with_precision_and_scale(size, scale)
                                .unwrap_or_default(),
                        )
                    });
                    let Some(dec_builder) =
                        dec_builder.as_any_mut().downcast_mut::<Decimal128Builder>()
                    else {
                        return FailedToDowncastBuilderSnafu {
                            clickhouse_type: SqlType::Decimal(size, align),
                        }
                        .fail();
                    };

                    if arrow_field.is_none() {
                        let Some(field_name) = column_names.get(i) else {
                            return NoColumnNameForIndexSnafu { index: i }.fail();
                        };
                        let new_arrow_field =
                            Field::new(field_name, DataType::Decimal128(size, scale), true);

                        *arrow_field = Some(new_arrow_field);
                    }

                    let v = match *clickhouse_type {
                        SqlType::Decimal(_, _) => Some(row.get::<Decimal, usize>(i).context(
                            FailedToGetRowValueSnafu {
                                clickhouse_type: SqlType::Decimal(size, align),
                            },
                        )?),
                        SqlType::Nullable(SqlType::Decimal(_, _)) => row
                            .get::<Option<Decimal>, usize>(i)
                            .context(FailedToGetRowValueSnafu {
                                clickhouse_type: SqlType::Decimal(size, align),
                            })?,
                        _ => unreachable!(),
                    };
                    match v {
                        Some(v) => {
                            let v = BigDecimal::from_str(v.to_string().as_str()).context(
                                FailedToParseBigDecimalFromClickhouseSnafu {
                                    value: v.to_string(),
                                },
                            )?;
                            let Some(v) = to_decimal_128(&v, scale) else {
                                return FailedToConvertBigDecimalToI128Snafu { big_decimal: v }
                                    .fail();
                            };
                            dec_builder.append_value(v);
                        }
                        None => dec_builder.append_null(),
                    }
                }
                _ => UnsupportedColumnTypeSnafu {
                    column_type: clickhouse_type.clone(),
                }
                .fail()?,
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

fn map_column_to_data_type(column_type: &SqlType) -> DataType {
    match column_type {
        SqlType::Bool => DataType::Boolean,
        SqlType::Int8 => DataType::Int8,
        SqlType::Int16 => DataType::Int16,
        SqlType::Int32 => DataType::Int32,
        SqlType::Int64 => DataType::Int64,
        SqlType::UInt8 => DataType::UInt8,
        SqlType::UInt16 => DataType::UInt16,
        SqlType::UInt32 => DataType::UInt32,
        SqlType::UInt64 => DataType::UInt64,
        SqlType::Float32 => DataType::Float32,
        SqlType::Float64 => DataType::Float64,
        SqlType::String | SqlType::FixedString(_) | SqlType::Uuid => DataType::Utf8,
        SqlType::Date => DataType::Date32,
        SqlType::DateTime(_) => DataType::Timestamp(TimeUnit::Second, None),
        SqlType::Decimal(size, align) => {
            DataType::Decimal128(*size, (*align).try_into().unwrap_or_default())
        }
        SqlType::Nullable(inner) => map_column_to_data_type(inner),
        _ => unimplemented!("Unsupported column type {:?}", column_type),
    }
}

fn to_decimal_128(decimal: &BigDecimal, scale: i8) -> Option<i128> {
    (decimal * 10i128.pow(scale.try_into().unwrap_or_default())).to_i128()
}

mod tests {
    #[test]
    fn test_block_to_arrow() {
        use super::block_to_arrow;
        use clickhouse_rs::{types::Decimal, Block};

        let block = Block::new()
            .add_column("int_8", vec![1_i8, 2, 4])
            .add_column("int_16", vec![1_i16, 2, 4])
            .add_column("int_32", vec![1_i32, 2, 4])
            .add_column("int_64", vec![1_i64, 2, 4])
            .add_column("uint_8", vec![1_u8, 2, 4])
            .add_column("uint_16", vec![1_u16, 2, 4])
            .add_column("uint_32", vec![1_u32, 2, 4])
            .add_column("uint_64", vec![1_u64, 2, 4])
            .add_column("float_32", vec![1.0_f32, 2.0, 4.0])
            .add_column("float_64", vec![1.0_f64, 2.0, 4.0])
            .add_column("string", vec!["a", "b", "c"])
            .add_column(
                "uuid",
                vec![
                    uuid::Uuid::default(),
                    uuid::Uuid::default(),
                    uuid::Uuid::default(),
                ],
            )
            .add_column("nullable_int32", vec![Some(1_i32), None, Some(3)])
            .add_column(
                "date",
                vec![
                    chrono::NaiveDate::from_ymd_opt(2021, 1, 1),
                    chrono::NaiveDate::from_ymd_opt(2021, 1, 2),
                    chrono::NaiveDate::from_ymd_opt(2021, 1, 3),
                ],
            )
            .add_column("bool", vec![true, false, true])
            .add_column(
                "decimal",
                vec![
                    Decimal::new(123, 2),
                    Decimal::new(543, 2),
                    Decimal::new(451, 2),
                ],
            );

        let rec = block_to_arrow(&block).expect("Failed to convert block to arrow");

        assert_eq!(rec.num_rows(), 3, "Number of rows mismatch");
        assert_eq!(rec.num_columns(), 16, "Number of columns mismatch");

        let expected_datatypes = vec![
            arrow::datatypes::DataType::Int8,
            arrow::datatypes::DataType::Int16,
            arrow::datatypes::DataType::Int32,
            arrow::datatypes::DataType::Int64,
            arrow::datatypes::DataType::UInt8,
            arrow::datatypes::DataType::UInt16,
            arrow::datatypes::DataType::UInt32,
            arrow::datatypes::DataType::UInt64,
            arrow::datatypes::DataType::Float32,
            arrow::datatypes::DataType::Float64,
            arrow::datatypes::DataType::Utf8,
            arrow::datatypes::DataType::Utf8,
            arrow::datatypes::DataType::Int32,
            arrow::datatypes::DataType::Date32,
            arrow::datatypes::DataType::Boolean,
            arrow::datatypes::DataType::Decimal128(18, 2),
        ];
        for (index, field) in rec.schema().fields.iter().enumerate() {
            assert_eq!(
                *field.data_type(),
                expected_datatypes[index],
                "Data type mismatch"
            );
        }
    }
}
