use std::convert;
use std::io::Read;
use std::sync::Arc;

use crate::sql::arrow_sql_gen::arrow::map_data_type_to_array_builder_optional;
use crate::sql::arrow_sql_gen::statement::map_data_type_to_column_type;
use arrow::array::{
    ArrayBuilder, ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder,
    FixedSizeListBuilder, Float32Builder, Float64Builder, Int16Builder, Int32Builder, Int64Builder,
    Int8Builder, IntervalMonthDayNanoBuilder, LargeBinaryBuilder, LargeStringBuilder, ListBuilder,
    RecordBatch, RecordBatchOptions, StringBuilder, StringDictionaryBuilder, StructBuilder,
    Time64NanosecondBuilder, TimestampNanosecondBuilder, UInt32Builder,
};
use arrow::datatypes::{
    DataType, Date32Type, Field, Int8Type, IntervalMonthDayNanoType, IntervalUnit, Schema,
    SchemaRef, TimeUnit,
};
use bigdecimal::BigDecimal;
use byteorder::{BigEndian, ReadBytesExt};
use chrono::{DateTime, Timelike, Utc};
use composite::CompositeType;
use geo_types::geometry::Point;
use rust_decimal::Decimal;
use sea_query::{Alias, ColumnType, SeaRc};
use serde_json::Value;
use snafu::prelude::*;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio_postgres::types::FromSql;
use tokio_postgres::types::Kind;
use tokio_postgres::{types::Type, Row};

pub mod builder;
pub mod composite;
pub mod schema;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to build record batch: {source}"))]
    FailedToBuildRecordBatch {
        source: datafusion::arrow::error::ArrowError,
    },

    #[snafu(display("No builder found for index {index}"))]
    NoBuilderForIndex { index: usize },

    #[snafu(display("Failed to downcast builder for {postgres_type}"))]
    FailedToDowncastBuilder { postgres_type: String },

    #[snafu(display("Integer overflow when converting u64 to i64: {source}"))]
    FailedToConvertU64toI64 {
        source: <u64 as convert::TryInto<i64>>::Error,
    },

    #[snafu(display("Integer overflow when converting u128 to i64: {source}"))]
    FailedToConvertU128toI64 {
        source: <u128 as convert::TryInto<i64>>::Error,
    },

    #[snafu(display("Failed to get a row value for {pg_type}: {source}"))]
    FailedToGetRowValue {
        pg_type: Type,
        source: tokio_postgres::Error,
    },

    #[snafu(display("Failed to get a composite row value for {pg_type}: {source}"))]
    FailedToGetCompositeRowValue {
        pg_type: Type,
        source: composite::Error,
    },

    #[snafu(display("Failed to parse raw Postgres Bytes as BigDecimal: {:?}", bytes))]
    FailedToParseBigDecimalFromPostgres { bytes: Vec<u8> },

    #[snafu(display("Cannot represent BigDecimal as i128: {big_decimal}"))]
    FailedToConvertBigDecimalToI128 { big_decimal: BigDecimal },

    #[snafu(display("Failed to find field {column_name} in schema"))]
    FailedToFindFieldInSchema { column_name: String },

    #[snafu(display("No Arrow field found for index {index}"))]
    NoArrowFieldForIndex { index: usize },

    #[snafu(display("No PostgreSQL scale found for index {index}"))]
    NoPostgresScaleForIndex { index: usize },

    #[snafu(display("No column name for index: {index}"))]
    NoColumnNameForIndex { index: usize },

    #[snafu(display("The field '{field_name}' has an unsupported data type: {data_type}."))]
    UnsupportedDataType {
        data_type: String,
        field_name: String,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

macro_rules! handle_primitive_type {
    ($builder:expr, $type:expr, $builder_ty:ty, $value_ty:ty, $row:expr, $index:expr) => {{
        let Some(builder) = $builder else {
            return NoBuilderForIndexSnafu { index: $index }.fail();
        };
        let Some(builder) = builder.as_any_mut().downcast_mut::<$builder_ty>() else {
            return FailedToDowncastBuilderSnafu {
                postgres_type: format!("{:?}", $type),
            }
            .fail();
        };
        let v: Option<$value_ty> = $row
            .try_get($index)
            .context(FailedToGetRowValueSnafu { pg_type: $type })?;

        match v {
            Some(v) => builder.append_value(v),
            None => builder.append_null(),
        }
    }};
}

macro_rules! handle_primitive_array_type {
    ($type:expr, $builder:expr, $row:expr, $i:expr, $list_builder:ty, $value_type:ty) => {{
        let Some(builder) = $builder else {
            return NoBuilderForIndexSnafu { index: $i }.fail();
        };
        let Some(builder) = builder.as_any_mut().downcast_mut::<$list_builder>() else {
            return FailedToDowncastBuilderSnafu {
                postgres_type: format!("{:?}", $type),
            }
            .fail();
        };
        let v: Option<Vec<$value_type>> = $row
            .try_get($i)
            .context(FailedToGetRowValueSnafu { pg_type: $type })?;
        match v {
            Some(v) => {
                let v = v.into_iter().map(Some);
                builder.append_value(v);
            }
            None => builder.append_null(),
        }
    }};
}

macro_rules! handle_composite_type {
    ($BuilderType:ty, $ValueType:ty, $pg_type:expr, $composite_type:expr, $builder:expr, $idx:expr, $field_name:expr) => {{
        let Some(field_builder) = $builder.field_builder::<$BuilderType>($idx) else {
            return FailedToDowncastBuilderSnafu {
                postgres_type: format!("{}", $pg_type),
            }
            .fail();
        };
        let v: Option<$ValueType> =
            $composite_type
                .try_get($field_name)
                .context(FailedToGetCompositeRowValueSnafu {
                    pg_type: $pg_type.clone(),
                })?;
        match v {
            Some(v) => field_builder.append_value(v),
            None => field_builder.append_null(),
        }
    }};
}

macro_rules! handle_composite_types {
    ($field_type:expr, $pg_type:expr, $composite_type:expr, $builder:expr, $idx:expr, $field_name:expr, $($DataType:ident => ($BuilderType:ty, $ValueType:ty)),*) => {
        match $field_type {
            $(
                DataType::$DataType => {
                    handle_composite_type!(
                        $BuilderType,
                        $ValueType,
                        $pg_type,
                        $composite_type,
                        $builder,
                        $idx,
                        $field_name
                    );
                }
            )*
            _ => unimplemented!("Unsupported field type {:?}", $field_type),
        }
    }
}

/// Converts Postgres `Row`s to an Arrow `RecordBatch`. Assumes that all rows have the same schema and
/// sets the schema based on the first row.
///
/// # Errors
///
/// Returns an error if there is a failure in converting the rows to a `RecordBatch`.
#[allow(clippy::too_many_lines)]
pub fn rows_to_arrow(rows: &[Row], projected_schema: &Option<SchemaRef>) -> Result<RecordBatch> {
    let mut arrow_fields: Vec<Option<Field>> = Vec::new();
    let mut arrow_columns_builders: Vec<Option<Box<dyn ArrayBuilder>>> = Vec::new();
    let mut postgres_types: Vec<Type> = Vec::new();
    let mut postgres_numeric_scales: Vec<Option<u32>> = Vec::new();
    let mut column_names: Vec<String> = Vec::new();

    if !rows.is_empty() {
        let row = &rows[0];
        for column in row.columns() {
            let column_name = column.name();
            let column_type = column.type_();

            let projected_field = projected_schema
                .as_ref()
                .and_then(|schema| schema.field_with_name(column_name).ok());

            let mut numeric_scale: Option<u32> = None;

            let data_type = if *column_type == Type::NUMERIC {
                if let Some(field) = projected_field.as_ref() {
                    if let DataType::Decimal128(precision, scale) = field.data_type() {
                        numeric_scale = Some(u32::try_from(*scale).unwrap_or_default());
                        Some(DataType::Decimal128(*precision, *scale))
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                map_column_type_to_data_type(column_type, column_name)?
            };

            let nullable = projected_field
                .map(|field| field.is_nullable())
                .unwrap_or(true);

            match &data_type {
                Some(data_type) => {
                    arrow_fields.push(Some(Field::new(column_name, data_type.clone(), nullable)));
                }
                None => arrow_fields.push(None),
            }
            postgres_numeric_scales.push(numeric_scale);
            arrow_columns_builders
                .push(map_data_type_to_array_builder_optional(data_type.as_ref()));
            postgres_types.push(column_type.clone());
            column_names.push(column_name.to_string());
        }
    }

    for row in rows {
        for (i, postgres_type) in postgres_types.iter().enumerate() {
            let Some(builder) = arrow_columns_builders.get_mut(i) else {
                return NoBuilderForIndexSnafu { index: i }.fail();
            };

            let Some(arrow_field) = arrow_fields.get_mut(i) else {
                return NoArrowFieldForIndexSnafu { index: i }.fail();
            };

            let Some(postgres_numeric_scale) = postgres_numeric_scales.get_mut(i) else {
                return NoPostgresScaleForIndexSnafu { index: i }.fail();
            };

            match *postgres_type {
                Type::INT2 => {
                    handle_primitive_type!(builder, Type::INT2, Int16Builder, i16, row, i);
                }
                Type::INT4 => {
                    handle_primitive_type!(builder, Type::INT4, Int32Builder, i32, row, i);
                }
                Type::INT8 => {
                    handle_primitive_type!(builder, Type::INT8, Int64Builder, i64, row, i);
                }
                Type::OID => {
                    handle_primitive_type!(builder, Type::OID, UInt32Builder, u32, row, i);
                }
                Type::XID => {
                    handle_primitive_type!(builder, Type::XID, UInt32Builder, u32, row, i);
                }
                Type::FLOAT4 => {
                    handle_primitive_type!(builder, Type::FLOAT4, Float32Builder, f32, row, i);
                }
                Type::FLOAT8 => {
                    handle_primitive_type!(builder, Type::FLOAT8, Float64Builder, f64, row, i);
                }
                Type::CHAR => {
                    handle_primitive_type!(builder, Type::CHAR, Int8Builder, i8, row, i);
                }
                Type::TEXT => {
                    handle_primitive_type!(builder, Type::TEXT, StringBuilder, &str, row, i);
                }
                Type::VARCHAR => {
                    handle_primitive_type!(builder, Type::VARCHAR, StringBuilder, &str, row, i);
                }
                Type::NAME => {
                    handle_primitive_type!(builder, Type::NAME, StringBuilder, &str, row, i);
                }
                Type::BYTEA => {
                    handle_primitive_type!(builder, Type::BYTEA, BinaryBuilder, Vec<u8>, row, i);
                }
                Type::BPCHAR => {
                    let Some(builder) = builder else {
                        return NoBuilderForIndexSnafu { index: i }.fail();
                    };
                    let Some(builder) = builder.as_any_mut().downcast_mut::<StringBuilder>() else {
                        return FailedToDowncastBuilderSnafu {
                            postgres_type: format!("{postgres_type}"),
                        }
                        .fail();
                    };
                    let v: Option<&str> = row.try_get(i).context(FailedToGetRowValueSnafu {
                        pg_type: Type::BPCHAR,
                    })?;

                    match v {
                        Some(v) => builder.append_value(v.trim_end()),
                        None => builder.append_null(),
                    }
                }
                Type::BOOL => {
                    handle_primitive_type!(builder, Type::BOOL, BooleanBuilder, bool, row, i);
                }
                Type::MONEY => {
                    let Some(builder) = builder else {
                        return NoBuilderForIndexSnafu { index: i }.fail();
                    };
                    let Some(builder) = builder.as_any_mut().downcast_mut::<Int64Builder>() else {
                        return FailedToDowncastBuilderSnafu {
                            postgres_type: format!("{postgres_type}"),
                        }
                        .fail();
                    };
                    let v = row
                        .try_get::<usize, Option<MoneyFromSql>>(i)
                        .with_context(|_| FailedToGetRowValueSnafu {
                            pg_type: Type::MONEY,
                        })?;

                    match v {
                        Some(v) => {
                            builder.append_value(v.cash_value);
                        }
                        None => builder.append_null(),
                    }
                }
                // Schema validation will only allow JSONB columns when `UnsupportedTypeAction` is set to `String`, so it is safe to handle JSONB here as strings.
                Type::JSON | Type::JSONB => {
                    let Some(builder) = builder else {
                        return NoBuilderForIndexSnafu { index: i }.fail();
                    };
                    let Some(builder) = builder.as_any_mut().downcast_mut::<StringBuilder>() else {
                        return FailedToDowncastBuilderSnafu {
                            postgres_type: format!("{postgres_type}"),
                        }
                        .fail();
                    };
                    let v = row.try_get::<usize, Option<Value>>(i).with_context(|_| {
                        FailedToGetRowValueSnafu {
                            pg_type: postgres_type.clone(),
                        }
                    })?;

                    match v {
                        Some(v) => {
                            builder.append_value(v.to_string());
                        }
                        None => builder.append_null(),
                    }
                }
                Type::TIME => {
                    let Some(builder) = builder else {
                        return NoBuilderForIndexSnafu { index: i }.fail();
                    };
                    let Some(builder) = builder
                        .as_any_mut()
                        .downcast_mut::<Time64NanosecondBuilder>()
                    else {
                        return FailedToDowncastBuilderSnafu {
                            postgres_type: format!("{postgres_type}"),
                        }
                        .fail();
                    };
                    let v = row
                        .try_get::<usize, Option<chrono::NaiveTime>>(i)
                        .with_context(|_| FailedToGetRowValueSnafu {
                            pg_type: Type::TIME,
                        })?;

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
                Type::POINT => {
                    let Some(builder) = builder else {
                        return NoBuilderForIndexSnafu { index: i }.fail();
                    };
                    let Some(builder) = builder
                        .as_any_mut()
                        .downcast_mut::<FixedSizeListBuilder<Float64Builder>>()
                    else {
                        return FailedToDowncastBuilderSnafu {
                            postgres_type: format!("{postgres_type}"),
                        }
                        .fail();
                    };

                    let v = row.try_get::<usize, Option<Point>>(i).with_context(|_| {
                        FailedToGetRowValueSnafu {
                            pg_type: Type::POINT,
                        }
                    })?;

                    if let Some(v) = v {
                        builder.values().append_value(v.x());
                        builder.values().append_value(v.y());
                        builder.append(true);
                    } else {
                        builder.values().append_null();
                        builder.values().append_null();
                        builder.append(false);
                    }
                }
                Type::INTERVAL => {
                    let Some(builder) = builder else {
                        return NoBuilderForIndexSnafu { index: i }.fail();
                    };
                    let Some(builder) = builder
                        .as_any_mut()
                        .downcast_mut::<IntervalMonthDayNanoBuilder>()
                    else {
                        return FailedToDowncastBuilderSnafu {
                            postgres_type: format!("{postgres_type}"),
                        }
                        .fail();
                    };

                    let v: Option<IntervalFromSql> =
                        row.try_get(i).context(FailedToGetRowValueSnafu {
                            pg_type: Type::INTERVAL,
                        })?;
                    match v {
                        Some(v) => {
                            let interval_month_day_nano = IntervalMonthDayNanoType::make_value(
                                v.month,
                                v.day,
                                v.time * 1_000,
                            );
                            builder.append_value(interval_month_day_nano);
                        }
                        None => builder.append_null(),
                    }
                }
                Type::NUMERIC => {
                    let v: Option<Decimal> = row.try_get(i).context(FailedToGetRowValueSnafu {
                        pg_type: Type::NUMERIC,
                    })?;
                    let scale = {
                        if let Some(v) = &v {
                            v.scale()
                        } else {
                            0
                        }
                    };

                    let dec_builder = builder.get_or_insert_with(|| {
                        Box::new(
                            Decimal128Builder::new()
                                .with_precision_and_scale(38, scale.try_into().unwrap_or_default())
                                .unwrap_or_default(),
                        )
                    });

                    let Some(dec_builder) =
                        dec_builder.as_any_mut().downcast_mut::<Decimal128Builder>()
                    else {
                        return FailedToDowncastBuilderSnafu {
                            postgres_type: format!("{postgres_type}"),
                        }
                        .fail();
                    };

                    if arrow_field.is_none() {
                        let Some(field_name) = column_names.get(i) else {
                            return NoColumnNameForIndexSnafu { index: i }.fail();
                        };
                        let new_arrow_field = Field::new(
                            field_name,
                            DataType::Decimal128(38, scale.try_into().unwrap_or_default()),
                            true,
                        );

                        *arrow_field = Some(new_arrow_field);
                    }

                    if postgres_numeric_scale.is_none() {
                        *postgres_numeric_scale = Some(scale);
                    };

                    let Some(mut v) = v else {
                        dec_builder.append_null();
                        continue;
                    };

                    // Record Batch Scale is determined by first row, while Postgres Numeric Type doesn't have fixed scale
                    // Resolve scale difference for incoming records
                    let dest_scale = postgres_numeric_scale.unwrap_or_default();
                    v.rescale(dest_scale);
                    dec_builder.append_value(v.mantissa());
                }
                Type::TIMESTAMP => {
                    let Some(builder) = builder else {
                        return NoBuilderForIndexSnafu { index: i }.fail();
                    };
                    let Some(builder) = builder
                        .as_any_mut()
                        .downcast_mut::<TimestampNanosecondBuilder>()
                    else {
                        return FailedToDowncastBuilderSnafu {
                            postgres_type: format!("{postgres_type}"),
                        }
                        .fail();
                    };
                    let v = row
                        .try_get::<usize, Option<SystemTime>>(i)
                        .with_context(|_| FailedToGetRowValueSnafu {
                            pg_type: Type::TIMESTAMP,
                        })?;

                    match v {
                        Some(v) => {
                            if let Ok(v) = v.duration_since(UNIX_EPOCH) {
                                let timestamp: i64 = v
                                    .as_nanos()
                                    .try_into()
                                    .context(FailedToConvertU128toI64Snafu)?;
                                builder.append_value(timestamp);
                            }
                        }
                        None => builder.append_null(),
                    }
                }
                Type::TIMESTAMPTZ => {
                    let v = row
                        .try_get::<usize, Option<DateTime<Utc>>>(i)
                        .with_context(|_| FailedToGetRowValueSnafu {
                            pg_type: Type::TIMESTAMPTZ,
                        })?;

                    let timestamptz_builder = builder.get_or_insert_with(|| {
                        Box::new(TimestampNanosecondBuilder::new().with_timezone("UTC"))
                    });

                    let Some(timestamptz_builder) = timestamptz_builder
                        .as_any_mut()
                        .downcast_mut::<TimestampNanosecondBuilder>()
                    else {
                        return FailedToDowncastBuilderSnafu {
                            postgres_type: format!("{postgres_type}"),
                        }
                        .fail();
                    };

                    if arrow_field.is_none() {
                        let Some(field_name) = column_names.get(i) else {
                            return NoColumnNameForIndexSnafu { index: i }.fail();
                        };
                        let new_arrow_field = Field::new(
                            field_name,
                            DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("UTC"))),
                            true,
                        );

                        *arrow_field = Some(new_arrow_field);
                    }

                    match v {
                        Some(v) => {
                            let utc_timestamp =
                                v.to_utc().timestamp_nanos_opt().unwrap_or_default();
                            timestamptz_builder.append_value(utc_timestamp);
                        }
                        None => timestamptz_builder.append_null(),
                    }
                }

                Type::DATE => {
                    let Some(builder) = builder else {
                        return NoBuilderForIndexSnafu { index: i }.fail();
                    };
                    let Some(builder) = builder.as_any_mut().downcast_mut::<Date32Builder>() else {
                        return FailedToDowncastBuilderSnafu {
                            postgres_type: format!("{postgres_type}"),
                        }
                        .fail();
                    };
                    let v = row.try_get::<usize, Option<chrono::NaiveDate>>(i).context(
                        FailedToGetRowValueSnafu {
                            pg_type: Type::DATE,
                        },
                    )?;

                    match v {
                        Some(v) => builder.append_value(Date32Type::from_naive_date(v)),
                        None => builder.append_null(),
                    }
                }
                Type::UUID => {
                    let Some(builder) = builder else {
                        return NoBuilderForIndexSnafu { index: i }.fail();
                    };
                    let Some(builder) = builder.as_any_mut().downcast_mut::<StringBuilder>() else {
                        return FailedToDowncastBuilderSnafu {
                            postgres_type: format!("{postgres_type}"),
                        }
                        .fail();
                    };
                    let v = row.try_get::<usize, Option<uuid::Uuid>>(i).context(
                        FailedToGetRowValueSnafu {
                            pg_type: Type::UUID,
                        },
                    )?;

                    match v {
                        Some(v) => builder.append_value(v.to_string()),
                        None => builder.append_null(),
                    }
                }
                Type::INT2_ARRAY => handle_primitive_array_type!(
                    Type::INT2_ARRAY,
                    builder,
                    row,
                    i,
                    ListBuilder<Int16Builder>,
                    i16
                ),
                Type::INT4_ARRAY => handle_primitive_array_type!(
                    Type::INT4_ARRAY,
                    builder,
                    row,
                    i,
                    ListBuilder<Int32Builder>,
                    i32
                ),
                Type::INT8_ARRAY => handle_primitive_array_type!(
                    Type::INT8_ARRAY,
                    builder,
                    row,
                    i,
                    ListBuilder<Int64Builder>,
                    i64
                ),
                Type::OID_ARRAY => handle_primitive_array_type!(
                    Type::OID_ARRAY,
                    builder,
                    row,
                    i,
                    ListBuilder<UInt32Builder>,
                    u32
                ),
                Type::FLOAT4_ARRAY => handle_primitive_array_type!(
                    Type::FLOAT4_ARRAY,
                    builder,
                    row,
                    i,
                    ListBuilder<Float32Builder>,
                    f32
                ),
                Type::FLOAT8_ARRAY => handle_primitive_array_type!(
                    Type::FLOAT8_ARRAY,
                    builder,
                    row,
                    i,
                    ListBuilder<Float64Builder>,
                    f64
                ),
                Type::TEXT_ARRAY => handle_primitive_array_type!(
                    Type::TEXT_ARRAY,
                    builder,
                    row,
                    i,
                    ListBuilder<StringBuilder>,
                    String
                ),
                Type::BOOL_ARRAY => handle_primitive_array_type!(
                    Type::BOOL_ARRAY,
                    builder,
                    row,
                    i,
                    ListBuilder<BooleanBuilder>,
                    bool
                ),
                Type::BYTEA_ARRAY => handle_primitive_array_type!(
                    Type::BYTEA_ARRAY,
                    builder,
                    row,
                    i,
                    ListBuilder<BinaryBuilder>,
                    Vec<u8>
                ),
                _ if matches!(postgres_type.name(), "geometry" | "geography") => {
                    let Some(builder) = builder else {
                        return NoBuilderForIndexSnafu { index: i }.fail();
                    };
                    let Some(builder) = builder.as_any_mut().downcast_mut::<BinaryBuilder>() else {
                        return FailedToDowncastBuilderSnafu {
                            postgres_type: format!("{postgres_type}"),
                        }
                        .fail();
                    };
                    let v = row.try_get::<usize, Option<GeometryFromSql>>(i).context(
                        FailedToGetRowValueSnafu {
                            pg_type: postgres_type.clone(),
                        },
                    )?;

                    match v {
                        Some(v) => builder.append_value(v.wkb),
                        None => builder.append_null(),
                    }
                }
                _ if matches!(postgres_type.name(), "_geometry" | "_geography") => {
                    let Some(builder) = builder else {
                        return NoBuilderForIndexSnafu { index: i }.fail();
                    };
                    let Some(builder) = builder
                        .as_any_mut()
                        .downcast_mut::<ListBuilder<BinaryBuilder>>()
                    else {
                        return FailedToDowncastBuilderSnafu {
                            postgres_type: format!("{postgres_type}"),
                        }
                        .fail();
                    };
                    let v: Option<Vec<GeometryFromSql>> =
                        row.try_get(i).context(FailedToGetRowValueSnafu {
                            pg_type: postgres_type.clone(),
                        })?;
                    match v {
                        Some(v) => {
                            let v = v.into_iter().map(|item| Some(item.wkb));
                            builder.append_value(v);
                        }
                        None => builder.append_null(),
                    }
                }
                _ => match *postgres_type.kind() {
                    Kind::Composite(_) => {
                        let Some(builder) = builder else {
                            return NoBuilderForIndexSnafu { index: i }.fail();
                        };
                        let Some(builder) = builder.as_any_mut().downcast_mut::<StructBuilder>()
                        else {
                            return FailedToDowncastBuilderSnafu {
                                postgres_type: format!("{postgres_type}"),
                            }
                            .fail();
                        };

                        let v = row.try_get::<usize, Option<CompositeType>>(i).context(
                            FailedToGetRowValueSnafu {
                                pg_type: postgres_type.clone(),
                            },
                        )?;

                        let Some(composite_type) = v else {
                            builder.append_null();
                            continue;
                        };

                        builder.append(true);

                        let fields = composite_type.fields();
                        for (idx, field) in fields.iter().enumerate() {
                            let field_name = field.name();
                            let Some(field_type) =
                                map_column_type_to_data_type(field.type_(), field_name)?
                            else {
                                return FailedToDowncastBuilderSnafu {
                                    postgres_type: format!("{}", field.type_()),
                                }
                                .fail();
                            };

                            handle_composite_types!(
                                field_type,
                                field.type_(),
                                composite_type,
                                builder,
                                idx,
                                field_name,
                                Boolean => (BooleanBuilder, bool),
                                Int8 => (Int8Builder, i8),
                                Int16 => (Int16Builder, i16),
                                Int32 => (Int32Builder, i32),
                                Int64 => (Int64Builder, i64),
                                UInt32 => (UInt32Builder, u32),
                                Float32 => (Float32Builder, f32),
                                Float64 => (Float64Builder, f64),
                                Binary => (BinaryBuilder, Vec<u8>),
                                LargeBinary => (LargeBinaryBuilder, Vec<u8>),
                                Utf8 => (StringBuilder, String),
                                LargeUtf8 => (LargeStringBuilder, String)
                            );
                        }
                    }
                    Kind::Enum(_) => {
                        let Some(builder) = builder else {
                            return NoBuilderForIndexSnafu { index: i }.fail();
                        };
                        let Some(builder) = builder
                            .as_any_mut()
                            .downcast_mut::<StringDictionaryBuilder<Int8Type>>()
                        else {
                            return FailedToDowncastBuilderSnafu {
                                postgres_type: format!("{postgres_type}"),
                            }
                            .fail();
                        };

                        let v = row.try_get::<usize, Option<EnumValueFromSql>>(i).context(
                            FailedToGetRowValueSnafu {
                                pg_type: postgres_type.clone(),
                            },
                        )?;

                        match v {
                            Some(v) => builder.append_value(v.enum_value),
                            None => builder.append_null(),
                        }
                    }
                    _ => {
                        return UnsupportedDataTypeSnafu {
                            data_type: postgres_type.to_string(),
                            field_name: column_names[i].clone(),
                        }
                        .fail();
                    }
                },
            }
        }
    }

    let columns = arrow_columns_builders
        .into_iter()
        .filter_map(|builder| builder.map(|mut b| b.finish()))
        .collect::<Vec<ArrayRef>>();
    let arrow_fields = arrow_fields.into_iter().flatten().collect::<Vec<Field>>();

    let options = &RecordBatchOptions::new().with_row_count(Some(rows.len()));
    match RecordBatch::try_new_with_options(Arc::new(Schema::new(arrow_fields)), columns, options) {
        Ok(record_batch) => Ok(record_batch),
        Err(e) => Err(e).context(FailedToBuildRecordBatchSnafu),
    }
}

fn map_column_type_to_data_type(column_type: &Type, field_name: &str) -> Result<Option<DataType>> {
    match *column_type {
        Type::INT2 => Ok(Some(DataType::Int16)),
        Type::INT4 => Ok(Some(DataType::Int32)),
        Type::INT8 | Type::MONEY => Ok(Some(DataType::Int64)),
        Type::OID | Type::XID => Ok(Some(DataType::UInt32)),
        Type::FLOAT4 => Ok(Some(DataType::Float32)),
        Type::FLOAT8 => Ok(Some(DataType::Float64)),
        Type::CHAR => Ok(Some(DataType::Int8)),
        Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::UUID | Type::NAME => {
            Ok(Some(DataType::Utf8))
        }
        Type::BYTEA => Ok(Some(DataType::Binary)),
        Type::BOOL => Ok(Some(DataType::Boolean)),
        // Schema validation will only allow JSONB columns when `UnsupportedTypeAction` is set to `String`, so it is safe to handle JSONB here as strings.
        Type::JSON | Type::JSONB => Ok(Some(DataType::Utf8)),
        // Inspect the scale from the first row. Precision will always be 38 for Decimal128.
        Type::NUMERIC => Ok(None),
        Type::TIMESTAMPTZ => Ok(Some(DataType::Timestamp(
            TimeUnit::Nanosecond,
            Some(Arc::from("UTC")),
        ))),
        // We get a SystemTime that we can always convert into milliseconds
        Type::TIMESTAMP => Ok(Some(DataType::Timestamp(TimeUnit::Nanosecond, None))),
        Type::DATE => Ok(Some(DataType::Date32)),
        Type::TIME => Ok(Some(DataType::Time64(TimeUnit::Nanosecond))),
        Type::INTERVAL => Ok(Some(DataType::Interval(IntervalUnit::MonthDayNano))),
        Type::POINT => Ok(Some(DataType::FixedSizeList(
            Arc::new(Field::new("item", DataType::Float64, true)),
            2,
        ))),
        Type::PG_NODE_TREE => Ok(Some(DataType::Utf8)),
        Type::INT2_ARRAY => Ok(Some(DataType::List(Arc::new(Field::new(
            "item",
            DataType::Int16,
            true,
        ))))),
        Type::INT4_ARRAY => Ok(Some(DataType::List(Arc::new(Field::new(
            "item",
            DataType::Int32,
            true,
        ))))),
        Type::INT8_ARRAY => Ok(Some(DataType::List(Arc::new(Field::new(
            "item",
            DataType::Int64,
            true,
        ))))),
        Type::OID_ARRAY => Ok(Some(DataType::List(Arc::new(Field::new(
            "item",
            DataType::UInt32,
            true,
        ))))),
        Type::FLOAT4_ARRAY => Ok(Some(DataType::List(Arc::new(Field::new(
            "item",
            DataType::Float32,
            true,
        ))))),
        Type::FLOAT8_ARRAY => Ok(Some(DataType::List(Arc::new(Field::new(
            "item",
            DataType::Float64,
            true,
        ))))),
        Type::TEXT_ARRAY => Ok(Some(DataType::List(Arc::new(Field::new(
            "item",
            DataType::Utf8,
            true,
        ))))),
        Type::BOOL_ARRAY => Ok(Some(DataType::List(Arc::new(Field::new(
            "item",
            DataType::Boolean,
            true,
        ))))),
        Type::BYTEA_ARRAY => Ok(Some(DataType::List(Arc::new(Field::new(
            "item",
            DataType::Binary,
            true,
        ))))),
        _ if matches!(column_type.name(), "geometry" | "geography") => Ok(Some(DataType::Binary)),
        _ if matches!(column_type.name(), "_geometry" | "_geography") => Ok(Some(DataType::List(
            Arc::new(Field::new("item", DataType::Binary, true)),
        ))),
        _ => match *column_type.kind() {
            Kind::Composite(ref fields) => {
                let mut arrow_fields = Vec::new();
                for field in fields {
                    let field_name = field.name();
                    let field_type = map_column_type_to_data_type(field.type_(), field_name)?;
                    match field_type {
                        Some(field_type) => {
                            arrow_fields.push(Field::new(field_name, field_type, true));
                        }
                        None => {
                            return UnsupportedDataTypeSnafu {
                                data_type: field.type_().to_string(),
                                field_name: field_name.to_string(),
                            }
                            .fail();
                        }
                    }
                }
                Ok(Some(DataType::Struct(arrow_fields.into())))
            }
            Kind::Enum(_) => Ok(Some(DataType::Dictionary(
                Box::new(DataType::Int8),
                Box::new(DataType::Utf8),
            ))),
            _ => UnsupportedDataTypeSnafu {
                data_type: column_type.to_string(),
                field_name: field_name.to_string(),
            }
            .fail(),
        },
    }
}

pub(crate) fn map_data_type_to_column_type_postgres(
    data_type: &DataType,
    table_name: &str,
    field_name: &str,
) -> ColumnType {
    match data_type {
        DataType::Struct(_) => ColumnType::Custom(SeaRc::new(Alias::new(
            get_postgres_composite_type_name(table_name, field_name),
        ))),
        _ => map_data_type_to_column_type(data_type),
    }
}

#[must_use]
pub(crate) fn get_postgres_composite_type_name(table_name: &str, field_name: &str) -> String {
    format!("struct_{table_name}_{field_name}")
}
// interval_send - Postgres C (https://github.com/postgres/postgres/blob/master/src/backend/utils/adt/timestamp.c#L1032)
// interval values are internally stored as three integral fields: months, days, and microseconds
struct IntervalFromSql {
    time: i64,
    day: i32,
    month: i32,
}

impl<'a> FromSql<'a> for IntervalFromSql {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> std::prelude::v1::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        let mut cursor = std::io::Cursor::new(raw);

        let time = cursor.read_i64::<BigEndian>()?;
        let day = cursor.read_i32::<BigEndian>()?;
        let month = cursor.read_i32::<BigEndian>()?;

        Ok(IntervalFromSql { time, day, month })
    }

    fn accepts(ty: &Type) -> bool {
        matches!(*ty, Type::INTERVAL)
    }
}

// cash_send - Postgres C (https://github.com/postgres/postgres/blob/bd8fe12ef3f727ed3658daf9b26beaf2b891e9bc/src/backend/utils/adt/cash.c#L603)
struct MoneyFromSql {
    cash_value: i64,
}

impl<'a> FromSql<'a> for MoneyFromSql {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> std::prelude::v1::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        let mut cursor = std::io::Cursor::new(raw);
        let cash_value = cursor.read_i64::<BigEndian>()?;
        Ok(MoneyFromSql { cash_value })
    }

    fn accepts(ty: &Type) -> bool {
        matches!(*ty, Type::MONEY)
    }
}

struct EnumValueFromSql {
    enum_value: String,
}

impl<'a> FromSql<'a> for EnumValueFromSql {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        let mut cursor = std::io::Cursor::new(raw);
        let mut enum_value = String::new();
        cursor.read_to_string(&mut enum_value)?;
        Ok(EnumValueFromSql { enum_value })
    }

    fn accepts(ty: &Type) -> bool {
        matches!(*ty.kind(), Kind::Enum(_))
    }
}

pub struct GeometryFromSql<'a> {
    wkb: &'a [u8],
}

impl<'a> FromSql<'a> for GeometryFromSql<'a> {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(GeometryFromSql { wkb: raw })
    }

    fn accepts(ty: &Type) -> bool {
        matches!(ty.name(), "geometry" | "geography")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveTime;
    use datafusion::arrow::array::{Time64NanosecondArray, Time64NanosecondBuilder};
    use geo_types::{point, polygon, Geometry};
    use geozero::{CoordDimensions, ToWkb};
    use std::str::FromStr;

    #[allow(clippy::cast_possible_truncation)]
    #[tokio::test]
    async fn test_decimal_from_sql() {
        let positive_u16: Vec<u16> = vec![5, 3, 0, 5, 9345, 1293, 2903, 1293, 932];
        let positive_raw: Vec<u8> = positive_u16
            .iter()
            .flat_map(|&x| vec![(x >> 8) as u8, x as u8])
            .collect();
        let positive = Decimal::from_str("9345129329031293.0932").expect("Failed to parse decimal");
        let positive_result = Decimal::from_sql(&Type::NUMERIC, positive_raw.as_slice())
            .expect("Failed to run FromSql");
        assert_eq!(positive_result, positive);

        let negative_u16: Vec<u16> = vec![5, 3, 0x4000, 5, 9345, 1293, 2903, 1293, 932];
        let negative_raw: Vec<u8> = negative_u16
            .iter()
            .flat_map(|&x| vec![(x >> 8) as u8, x as u8])
            .collect();

        let negative =
            Decimal::from_str("-9345129329031293.0932").expect("Failed to parse decimal");
        let negative_result = Decimal::from_sql(&Type::NUMERIC, negative_raw.as_slice())
            .expect("Failed to run FromSql");
        assert_eq!(negative_result, negative);
    }

    #[test]
    fn test_interval_from_sql() {
        let positive_time: i64 = 123_123;
        let positive_day: i32 = 10;
        let positive_month: i32 = 2;

        let mut positive_raw: Vec<u8> = Vec::new();
        positive_raw.extend_from_slice(&positive_time.to_be_bytes());
        positive_raw.extend_from_slice(&positive_day.to_be_bytes());
        positive_raw.extend_from_slice(&positive_month.to_be_bytes());

        let positive_result = IntervalFromSql::from_sql(&Type::INTERVAL, positive_raw.as_slice())
            .expect("Failed to run FromSql");
        assert_eq!(positive_result.day, positive_day);
        assert_eq!(positive_result.time, positive_time);
        assert_eq!(positive_result.month, positive_month);

        let negative_time: i64 = -123_123;
        let negative_day: i32 = -10;
        let negative_month: i32 = -2;

        let mut negative_raw: Vec<u8> = Vec::new();
        negative_raw.extend_from_slice(&negative_time.to_be_bytes());
        negative_raw.extend_from_slice(&negative_day.to_be_bytes());
        negative_raw.extend_from_slice(&negative_month.to_be_bytes());

        let negative_result = IntervalFromSql::from_sql(&Type::INTERVAL, negative_raw.as_slice())
            .expect("Failed to run FromSql");
        assert_eq!(negative_result.day, negative_day);
        assert_eq!(negative_result.time, negative_time);
        assert_eq!(negative_result.month, negative_month);
    }

    #[test]
    fn test_money_from_sql() {
        let positive_cash_value: i64 = 123;
        let mut positive_raw: Vec<u8> = Vec::new();
        positive_raw.extend_from_slice(&positive_cash_value.to_be_bytes());

        let positive_result = MoneyFromSql::from_sql(&Type::MONEY, positive_raw.as_slice())
            .expect("Failed to run FromSql");
        assert_eq!(positive_result.cash_value, positive_cash_value);

        let negative_cash_value: i64 = -123;
        let mut negative_raw: Vec<u8> = Vec::new();
        negative_raw.extend_from_slice(&negative_cash_value.to_be_bytes());

        let negative_result = MoneyFromSql::from_sql(&Type::MONEY, negative_raw.as_slice())
            .expect("Failed to run FromSql");
        assert_eq!(negative_result.cash_value, negative_cash_value);
    }

    #[test]
    fn test_chrono_naive_time_to_time64nanosecond() {
        let chrono_naive_vec = vec![
            NaiveTime::from_hms_opt(10, 30, 00).unwrap_or_default(),
            NaiveTime::from_hms_opt(10, 45, 15).unwrap_or_default(),
        ];

        let time_array: Time64NanosecondArray = vec![
            (10 * 3600 + 30 * 60) * 1_000_000_000,
            (10 * 3600 + 45 * 60 + 15) * 1_000_000_000,
        ]
        .into();

        let mut builder = Time64NanosecondBuilder::new();
        for time in chrono_naive_vec {
            let timestamp: i64 = i64::from(time.num_seconds_from_midnight()) * 1_000_000_000
                + i64::from(time.nanosecond());
            builder.append_value(timestamp);
        }
        let converted_result = builder.finish();
        assert_eq!(converted_result, time_array);
    }

    #[test]
    fn test_geometry_from_sql() {
        let positive_geometry = Geometry::from(point! { x: 181.2, y: 51.79 })
            .to_wkb(CoordDimensions::xy())
            .unwrap();
        let mut positive_raw: Vec<u8> = Vec::new();
        positive_raw.extend_from_slice(&positive_geometry);

        let positive_result = GeometryFromSql::from_sql(
            &Type::new(
                "geometry".to_owned(),
                16462,
                Kind::Simple,
                "public".to_owned(),
            ),
            positive_raw.as_slice(),
        )
        .expect("Failed to run FromSql");
        assert_eq!(positive_result.wkb, positive_geometry);

        let positive_geometry = Geometry::from(polygon![
            (x: -111., y: 45.),
            (x: -111., y: 41.),
            (x: -104., y: 41.),
            (x: -104., y: 45.),
        ])
        .to_wkb(CoordDimensions::xy())
        .unwrap();
        let mut positive_raw: Vec<u8> = Vec::new();
        positive_raw.extend_from_slice(&positive_geometry);

        let positive_result = GeometryFromSql::from_sql(
            &Type::new(
                "geometry".to_owned(),
                16462,
                Kind::Simple,
                "public".to_owned(),
            ),
            positive_raw.as_slice(),
        )
        .expect("Failed to run FromSql");
        assert_eq!(positive_result.wkb, positive_geometry);
    }
}
