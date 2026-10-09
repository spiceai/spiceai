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

use std::{
    net::{Ipv4Addr, Ipv6Addr},
    str::FromStr,
    sync::Arc,
};

use arrow::{
    array::{
        ArrayBuilder, ArrayRef, BooleanBuilder, Date32Builder, Decimal128Builder,
        Decimal256Builder, Float32Builder, Float64Builder, Int8Builder, Int16Builder, Int32Builder,
        Int64Builder, ListBuilder, MapBuilder, PrimitiveBuilder, RecordBatch, RecordBatchOptions,
        StringBuilder, StructBuilder, TimestampSecondBuilder, UInt8Builder, UInt16Builder,
        UInt32Builder, UInt64Builder, make_builder,
    },
    datatypes::{
        ArrowTimestampType, DataType, Date32Type, Field, Fields, Schema, TimeUnit,
        TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
        TimestampSecondType, i256,
    },
};
use bigdecimal::{BigDecimal, ToPrimitive};
use chrono::NaiveDate;
use chrono_tz::Tz;
use clickhouse_rs::{
    Block,
    types::{
        ColumnType, DateTimeType, Decimal, Enum8, Enum16, FromSql, FromSqlResult, SqlType, ValueRef,
    },
};
use snafu::{ResultExt, Snafu};

#[derive(Debug, Snafu)]
pub(crate) enum Error {
    #[snafu(display("Failed to process ClickHouse query result: {source}"))]
    FailedToBuildRecordBatch { source: arrow::error::ArrowError },

    #[snafu(display(
        "Failed to process ClickHouse query result: unsupported type '{clickhouse_type}'"
    ))]
    FailedToDowncastBuilder { clickhouse_type: SqlType },

    #[snafu(display("Failed to get a row value for {clickhouse_type}: {source}"))]
    FailedToGetRowValue {
        clickhouse_type: SqlType,
        source: clickhouse_rs::errors::Error,
    },

    #[snafu(display("A {clickhouse_type} column returned a value of another type: {value}"))]
    UnexpectedValue {
        clickhouse_type: SqlType,
        value: String,
    },

    #[snafu(display("The {clickhouse_type} value {value} is out of the range Arrow can hold"))]
    ValueOutOfRange {
        clickhouse_type: SqlType,
        value: String,
    },

    #[snafu(display("Cannot represent BigDecimal as i128: {big_decimal}"))]
    FailedToConvertBigDecimalToI128 { big_decimal: BigDecimal },

    #[snafu(display("Failed to parse decimal string as BigInteger '{value}': {source}"))]
    FailedToParseBigDecimalFromClickhouse {
        value: String,
        source: bigdecimal::ParseBigDecimalError,
    },

    #[snafu(display("Unsupported column type: {:?}", column_type))]
    UnsupportedColumnType { column_type: SqlType },
}

pub(crate) type Result<T, E = Error> = std::result::Result<T, E>;

/// A cell as the driver decoded it, before it is converted to an Arrow value.
struct Cell<'a>(ValueRef<'a>);

impl<'a> FromSql<'a> for Cell<'a> {
    fn from_sql(value: ValueRef<'a>) -> FromSqlResult<Self> {
        Ok(Cell(value))
    }
}

/// Converts `Clickhouse` `Block` to an Arrow `RecordBatch`. Assumes that all rows have the same schema and
/// sets the schema based on the `sql_type` returned for column.
///
/// # Errors
///
/// Returns an error if there is a failure in converting the rows to a `RecordBatch`.
pub(crate) fn block_to_arrow<T: ColumnType>(block: &Block<T>) -> Result<RecordBatch> {
    let mut arrow_fields = Vec::new();
    let mut builders = Vec::new();
    let mut clickhouse_types = Vec::new();

    if !block.is_empty() {
        for column in block.columns() {
            let column_type = column.sql_type();
            let data_type = map_column_to_data_type(&column_type)?;
            builders.push(make_builder(&data_type, block.row_count()));
            arrow_fields.push(Field::new(column.name(), data_type, true));
            clickhouse_types.push(column_type);
        }
    }

    for row in block.rows() {
        for (i, (clickhouse_type, builder)) in
            clickhouse_types.iter().zip(builders.iter_mut()).enumerate()
        {
            let cell = row
                .get::<Cell, usize>(i)
                .context(FailedToGetRowValueSnafu {
                    clickhouse_type: clickhouse_type.clone(),
                })?;
            append_value(builder.as_mut(), clickhouse_type, Some(cell.0))?;
        }
    }

    let columns = builders
        .iter_mut()
        .map(ArrayBuilder::finish)
        .collect::<Vec<ArrayRef>>();
    let options = &RecordBatchOptions::new().with_row_count(Some(block.row_count()));
    RecordBatch::try_new_with_options(Arc::new(Schema::new(arrow_fields)), columns, options)
        .context(FailedToBuildRecordBatchSnafu)
}

/// The Arrow type a `ClickHouse` column is read as. `conn::map_clickhouse_type_to_arrow`
/// maps the same types from their names and must agree with this.
fn map_column_to_data_type(column_type: &SqlType) -> Result<DataType> {
    Ok(match column_type {
        SqlType::Bool => DataType::Boolean,
        SqlType::Int8 => DataType::Int8,
        SqlType::Int16 => DataType::Int16,
        SqlType::Int32 => DataType::Int32,
        SqlType::Int64 => DataType::Int64,
        SqlType::UInt8 => DataType::UInt8,
        SqlType::UInt16 => DataType::UInt16,
        SqlType::UInt32 => DataType::UInt32,
        SqlType::UInt64 => DataType::UInt64,
        SqlType::Int128 | SqlType::UInt128 => INT128_DATA_TYPE,
        SqlType::Float32 => DataType::Float32,
        SqlType::Float64 => DataType::Float64,
        SqlType::String
        | SqlType::FixedString(_)
        | SqlType::Uuid
        | SqlType::Ipv4
        | SqlType::Ipv6
        | SqlType::Enum8(_)
        | SqlType::Enum16(_) => DataType::Utf8,
        SqlType::Date => DataType::Date32,
        SqlType::DateTime(DateTimeType::DateTime64(precision, _)) => {
            DataType::Timestamp(datetime64_unit(*precision), None)
        }
        SqlType::DateTime(_) => DataType::Timestamp(TimeUnit::Second, None),
        SqlType::Decimal(size, align) => {
            DataType::Decimal128(*size, (*align).try_into().unwrap_or_default())
        }
        SqlType::Nullable(inner) | SqlType::LowCardinality(inner) => {
            map_column_to_data_type(inner)?
        }
        SqlType::Array(inner) => list_data_type(map_column_to_data_type(inner)?),
        SqlType::Map(key, value) => map_data_type(
            map_column_to_data_type(key)?,
            map_column_to_data_type(value)?,
        ),
        SqlType::Tuple(elements) => DataType::Struct(
            elements
                .iter()
                .enumerate()
                .map(|(index, (name, element))| {
                    Ok(tuple_field(
                        index,
                        name.as_deref(),
                        map_column_to_data_type(element)?,
                    ))
                })
                .collect::<Result<Fields>>()?,
        ),
        SqlType::SimpleAggregateFunction(..) => {
            return UnsupportedColumnTypeSnafu {
                column_type: column_type.clone(),
            }
            .fail();
        }
    })
}

/// `Int128` and `UInt128` both need 39 decimal digits.
pub(crate) const INT128_DATA_TYPE: DataType = DataType::Decimal256(39, 0);

/// The coarsest Arrow unit that holds a `DateTime64(precision)` value exactly.
pub(crate) fn datetime64_unit(precision: u32) -> TimeUnit {
    match precision {
        0 => TimeUnit::Second,
        1..=3 => TimeUnit::Millisecond,
        4..=6 => TimeUnit::Microsecond,
        _ => TimeUnit::Nanosecond,
    }
}

pub(crate) fn list_data_type(item: DataType) -> DataType {
    DataType::List(Arc::new(Field::new_list_field(item, true)))
}

/// The field names are the ones [`MapBuilder`] writes.
pub(crate) fn map_data_type(key: DataType, value: DataType) -> DataType {
    let entries = Fields::from(vec![
        Field::new("keys", key, false),
        Field::new("values", value, true),
    ]);
    DataType::Map(
        Arc::new(Field::new("entries", DataType::Struct(entries), false)),
        false,
    )
}

/// An unnamed tuple element is named by its 1-based position, as `ClickHouse` addresses it.
pub(crate) fn tuple_field(index: usize, name: Option<&str>, data_type: DataType) -> Field {
    let name = name.map_or_else(|| (index + 1).to_string(), str::to_string);
    Field::new(name, data_type, true)
}

fn downcast<'b, B: ArrayBuilder>(
    builder: &'b mut dyn ArrayBuilder,
    clickhouse_type: &SqlType,
) -> Result<&'b mut B> {
    builder
        .as_any_mut()
        .downcast_mut::<B>()
        .ok_or_else(|| Error::FailedToDowncastBuilder {
            clickhouse_type: clickhouse_type.clone(),
        })
}

fn from_sql<'a, V: FromSql<'a>>(value: ValueRef<'a>, clickhouse_type: &SqlType) -> Result<V> {
    V::from_sql(value).context(FailedToGetRowValueSnafu {
        clickhouse_type: clickhouse_type.clone(),
    })
}

fn unexpected_value<V>(value: &ValueRef<'_>, clickhouse_type: &SqlType) -> Result<V> {
    UnexpectedValueSnafu {
        clickhouse_type: clickhouse_type.clone(),
        value: value.to_string(),
    }
    .fail()
}

/// Appends `value`, or a null when it is `None`, to a builder of type `$builder_ty`.
macro_rules! append {
    ($builder:expr, $builder_ty:ty, $clickhouse_type:expr, $value:expr, |$v:ident| $convert:expr) => {{
        let builder = downcast::<$builder_ty>($builder, $clickhouse_type)?;
        match $value {
            Some($v) => builder.append_value($convert),
            None => builder.append_null(),
        }
    }};
}

fn append_value(
    builder: &mut dyn ArrayBuilder,
    clickhouse_type: &SqlType,
    value: Option<ValueRef<'_>>,
) -> Result<()> {
    let t = clickhouse_type;
    match t {
        SqlType::Nullable(inner) => {
            let value = match value {
                Some(value) => from_sql::<Option<Cell>>(value, t)?.map(|cell| cell.0),
                None => None,
            };
            return append_value(builder, inner, value);
        }
        // The driver resolves a `LowCardinality` cell to its dictionary value.
        SqlType::LowCardinality(inner) => return append_value(builder, inner, value),
        SqlType::Bool => append!(builder, BooleanBuilder, t, value, |v| from_sql(v, t)?),
        SqlType::Int8 => append!(builder, Int8Builder, t, value, |v| from_sql(v, t)?),
        SqlType::Int16 => append!(builder, Int16Builder, t, value, |v| from_sql(v, t)?),
        SqlType::Int32 => append!(builder, Int32Builder, t, value, |v| from_sql(v, t)?),
        SqlType::Int64 => append!(builder, Int64Builder, t, value, |v| from_sql(v, t)?),
        SqlType::UInt8 => append!(builder, UInt8Builder, t, value, |v| from_sql(v, t)?),
        SqlType::UInt16 => append!(builder, UInt16Builder, t, value, |v| from_sql(v, t)?),
        SqlType::UInt32 => append!(builder, UInt32Builder, t, value, |v| from_sql(v, t)?),
        SqlType::UInt64 => append!(builder, UInt64Builder, t, value, |v| from_sql(v, t)?),
        SqlType::Int128 => append!(builder, Decimal256Builder, t, value, |v| {
            i256::from_i128(from_sql(v, t)?)
        }),
        SqlType::UInt128 => append!(builder, Decimal256Builder, t, value, |v| {
            i256::from_parts(from_sql(v, t)?, 0)
        }),
        SqlType::Float32 => append!(builder, Float32Builder, t, value, |v| from_sql(v, t)?),
        SqlType::Float64 => append!(builder, Float64Builder, t, value, |v| from_sql(v, t)?),
        SqlType::String | SqlType::FixedString(_) => {
            append!(builder, StringBuilder, t, value, |v| from_sql::<&str>(
                v, t
            )?);
        }
        SqlType::Uuid => append!(builder, StringBuilder, t, value, |v| {
            from_sql::<uuid::Uuid>(v, t)?.to_string()
        }),
        SqlType::Ipv4 => append!(builder, StringBuilder, t, value, |v| {
            from_sql::<Ipv4Addr>(v, t)?.to_string()
        }),
        SqlType::Ipv6 => append!(builder, StringBuilder, t, value, |v| {
            from_sql::<Ipv6Addr>(v, t)?.to_string()
        }),
        SqlType::Enum8(names) => append!(builder, StringBuilder, t, value, |v| {
            enum_name(names, from_sql::<Enum8>(v, t)?.internal(), t)?
        }),
        SqlType::Enum16(names) => append!(builder, StringBuilder, t, value, |v| {
            enum_name(names, from_sql::<Enum16>(v, t)?.internal(), t)?
        }),
        SqlType::Date => append!(builder, Date32Builder, t, value, |v| {
            Date32Type::from_naive_date(from_sql::<NaiveDate>(v, t)?)
        }),
        SqlType::DateTime(DateTimeType::DateTime64(precision, _)) => {
            let ticks = value.map(|v| match v {
                ValueRef::DateTime64(ticks, _) => Ok(ticks),
                other => unexpected_value(&other, t),
            });
            let ticks = ticks.transpose()?;
            match datetime64_unit(*precision) {
                TimeUnit::Second => {
                    append_timestamp::<TimestampSecondType>(builder, t, *precision, ticks)?;
                }
                TimeUnit::Millisecond => {
                    append_timestamp::<TimestampMillisecondType>(builder, t, *precision, ticks)?;
                }
                TimeUnit::Microsecond => {
                    append_timestamp::<TimestampMicrosecondType>(builder, t, *precision, ticks)?;
                }
                TimeUnit::Nanosecond => {
                    append_timestamp::<TimestampNanosecondType>(builder, t, *precision, ticks)?;
                }
            }
        }
        SqlType::DateTime(_) => append!(builder, TimestampSecondBuilder, t, value, |v| {
            from_sql::<chrono::DateTime<Tz>>(v, t)?.timestamp()
        }),
        SqlType::Decimal(_, align) => {
            let scale = (*align).try_into().unwrap_or_default();
            append!(builder, Decimal128Builder, t, value, |v| {
                let v = from_sql::<Decimal>(v, t)?;
                let v = BigDecimal::from_str(v.to_string().as_str()).context(
                    FailedToParseBigDecimalFromClickhouseSnafu {
                        value: v.to_string(),
                    },
                )?;
                let Some(v) = to_decimal_128(&v, scale) else {
                    return FailedToConvertBigDecimalToI128Snafu { big_decimal: v }.fail();
                };
                v
            });
        }
        SqlType::Array(inner) => {
            let builder = downcast::<ListBuilder<Box<dyn ArrayBuilder>>>(builder, t)?;
            match value {
                Some(ValueRef::Array(_, items)) => {
                    for item in items.iter() {
                        append_value(builder.values().as_mut(), inner, Some(item.clone()))?;
                    }
                    builder.append(true);
                }
                Some(other) => return unexpected_value(&other, t),
                None => builder.append(false),
            }
        }
        SqlType::Map(key_type, value_type) => {
            let builder =
                downcast::<MapBuilder<Box<dyn ArrayBuilder>, Box<dyn ArrayBuilder>>>(builder, t)?;
            let is_valid = match value {
                Some(ValueRef::Map(_, _, entries)) => {
                    for (key, entry_value) in entries.iter() {
                        append_value(builder.keys().as_mut(), key_type, Some(key.clone()))?;
                        append_value(
                            builder.values().as_mut(),
                            value_type,
                            Some(entry_value.clone()),
                        )?;
                    }
                    true
                }
                Some(other) => return unexpected_value(&other, t),
                None => false,
            };
            builder
                .append(is_valid)
                .context(FailedToBuildRecordBatchSnafu)?;
        }
        SqlType::Tuple(elements) => {
            let builder = downcast::<StructBuilder>(builder, t)?;
            let items = match value {
                Some(ValueRef::Tuple(items)) if items.len() == elements.len() => {
                    Some(items.iter().cloned().map(Some).collect::<Vec<_>>())
                }
                Some(other) => return unexpected_value(&other, t),
                None => None,
            };
            let is_valid = items.is_some();
            let items = items.unwrap_or_else(|| vec![None; elements.len()]);
            for ((field_builder, (_, element_type)), item) in builder
                .field_builders_mut()
                .iter_mut()
                .zip(elements)
                .zip(items)
            {
                append_value(field_builder.as_mut(), element_type, item)?;
            }
            builder.append(is_valid);
        }
        SqlType::SimpleAggregateFunction(..) => {
            return UnsupportedColumnTypeSnafu {
                column_type: t.clone(),
            }
            .fail();
        }
    }
    Ok(())
}

/// Appends a `DateTime64(precision)` value, counted in `10^-precision` seconds, in the unit of `T`.
fn append_timestamp<T: ArrowTimestampType>(
    builder: &mut dyn ArrayBuilder,
    clickhouse_type: &SqlType,
    precision: u32,
    ticks: Option<i64>,
) -> Result<()> {
    let builder = downcast::<PrimitiveBuilder<T>>(builder, clickhouse_type)?;
    let Some(ticks) = ticks else {
        builder.append_null();
        return Ok(());
    };
    let unit_digits: u32 = match T::UNIT {
        TimeUnit::Second => 0,
        TimeUnit::Millisecond => 3,
        TimeUnit::Microsecond => 6,
        TimeUnit::Nanosecond => 9,
    };
    let scaled = unit_digits
        .checked_sub(precision)
        .and_then(|digits| 10_i64.checked_pow(digits))
        .and_then(|factor| ticks.checked_mul(factor))
        .ok_or_else(|| Error::ValueOutOfRange {
            clickhouse_type: clickhouse_type.clone(),
            value: ticks.to_string(),
        })?;
    builder.append_value(scaled);
    Ok(())
}

fn enum_name<V: Copy + PartialEq + std::fmt::Display>(
    names: &[(String, V)],
    value: V,
    clickhouse_type: &SqlType,
) -> Result<String> {
    names
        .iter()
        .find(|(_, code)| *code == value)
        .map(|(name, _)| name.clone())
        .ok_or_else(|| Error::UnexpectedValue {
            clickhouse_type: clickhouse_type.clone(),
            value: value.to_string(),
        })
}

fn to_decimal_128(decimal: &BigDecimal, scale: i8) -> Option<i128> {
    (decimal * 10i128.pow(scale.try_into().unwrap_or_default())).to_i128()
}

#[cfg(test)]
mod tests {
    /// The decode `block_to_arrow` performs for a `ClickHouse` `Date32` column, over
    /// the range that makes `Date32` a distinct type: `ClickHouse` `Date` is 16
    /// unsigned bits of days from the epoch, so it stops at 1970-01-01 on one side
    /// and 2149-06-06 on the other, and `Date32` is the only way to carry a date
    /// outside that.
    ///
    /// `Date32` support is a Spice patch to the `spiceai/clickhouse-rs` fork, in
    /// three parts: `DateConverter for i32`, the `Value`/`ValueRef::Date32`
    /// variants, and the `FromSql for NaiveDate` arm asserted here — which is the
    /// one `block_to_arrow` calls, through `row.get::<NaiveDate, _>()` on the
    /// `SqlType::Date` arm.
    ///
    /// Fed a `ValueRef` directly rather than a decoded column because a `Date32`
    /// column cannot be built client-side: `Block::add_column` over `NaiveDate`
    /// resolves through `SqlType::from(Value::Date32) == SqlType::Date` to a 16-bit
    /// `DateColumnData<u16>`, and the only thing that produces the 32-bit one is
    /// `column::factory`'s `"Date32"` wire-type arm, reached from `Block::load`,
    /// which is `pub(crate)`. That half of the patch is guarded end-to-end instead,
    /// by the `Date32` column in `test/scripts/setup-data-clickhouse.sql` — losing
    /// it leaves the wire type unrecognised and fails the whole `SELECT`.
    #[test]
    fn a_date32_value_decodes_the_dates_a_date_column_cannot_hold() {
        use chrono::NaiveDate;
        use clickhouse_rs::types::{FromSql, SqlType, Value, ValueRef};

        // Day counts from the Unix epoch. -25_567 is 1900-01-01, before the epoch
        // `Date` counts from at all; 84_006 is 2200-01-01, past the 2149-06-06 that
        // is `Date`'s last representable day (65_535).
        for (days, expected) in [
            (-25_567_i32, (1900, 1, 1)),
            (84_006_i32, (2200, 1, 1)),
            (0_i32, (1970, 1, 1)),
        ] {
            let expected = NaiveDate::from_ymd_opt(expected.0, expected.1, expected.2)
                .expect("the guard's expected date is a real date");

            let decoded =
                <NaiveDate as FromSql>::from_sql(ValueRef::Date32(days)).unwrap_or_else(|e| {
                    panic!(
                        "a ClickHouse Date32 column holding {expected} failed to decode, so every \
                         query against a dataset with a Date32 column fails: {e}"
                    )
                });
            assert_eq!(
                decoded, expected,
                "a ClickHouse Date32 of {days} days decoded as {decoded}, not {expected}: the \
                 dataset reports a date that is not the one stored"
            );

            // The same conversion reached through `Value`, which is what a column
            // built from these values pushes through.
            assert_eq!(
                NaiveDate::from(Value::Date32(days)),
                expected,
                "Value::Date32 of {days} days converted to the wrong date"
            );
        }

        // `block_to_arrow` selects the `NaiveDate` decode above by matching
        // `SqlType::Date`, and a `Date32` column reports exactly that. A re-cut
        // that gave `Date32` a `SqlType` of its own would leave the column
        // matching no arm at all — an unsupported-type error on a column that
        // decodes fine today — so the mapping is part of what has to hold.
        assert_eq!(
            SqlType::from(Value::Date32(0)),
            SqlType::Date,
            "a Date32 column no longer reports SqlType::Date, so block_to_arrow's Date arm does \
             not claim it and the column is rejected as an unsupported type"
        );
    }

    #[test]
    fn test_block_to_arrow() {
        use super::block_to_arrow;
        use clickhouse_rs::{Block, types::Decimal};

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

    fn converted(block: &clickhouse_rs::Block) -> String {
        let rec = super::block_to_arrow(block).expect("the block converts to Arrow");
        arrow::util::pretty::pretty_format_batches(&[rec])
            .expect("the batch formats")
            .to_string()
    }

    #[test]
    fn scalar_types_convert_to_their_arrow_values() {
        use std::net::{Ipv4Addr, Ipv6Addr};

        let block = clickhouse_rs::Block::new()
            .add_column("ipv4", vec![Ipv4Addr::LOCALHOST, Ipv4Addr::BROADCAST])
            .add_column(
                "nullable_ipv4",
                vec![None, Some(Ipv4Addr::new(192, 168, 1, 20))],
            )
            .add_column(
                "ipv6",
                vec![
                    "2001:db8::1".parse::<Ipv6Addr>().expect("valid IPv6"),
                    Ipv4Addr::new(1, 2, 3, 4).to_ipv6_mapped(),
                ],
            )
            .add_column("uint128", vec![0_u128, u128::MAX])
            .add_column("int128", vec![i128::MIN, -1_i128]);

        let rec = super::block_to_arrow(&block).expect("the block converts to Arrow");
        assert_eq!(
            rec.schema().field(3).data_type(),
            &arrow::datatypes::DataType::Decimal256(39, 0)
        );
        insta::assert_snapshot!(converted(&block), @r"
        +-----------------+---------------+----------------+-----------------------------------------+------------------------------------------+
        | ipv4            | nullable_ipv4 | ipv6           | uint128                                 | int128                                   |
        +-----------------+---------------+----------------+-----------------------------------------+------------------------------------------+
        | 127.0.0.1       |               | 2001:db8::1    | 0                                       | -170141183460469231731687303715884105728 |
        | 255.255.255.255 | 192.168.1.20  | ::ffff:1.2.3.4 | 340282366920938463463374607431768211455 | -1                                       |
        +-----------------+---------------+----------------+-----------------------------------------+------------------------------------------+
        ");
    }

    #[test]
    fn an_enum_converts_to_its_names() {
        use clickhouse_rs::{row, types::Enum8, types::Value};

        let names = vec![("a".to_string(), 1_i8), ("b".to_string(), 2_i8)];
        let mut block = clickhouse_rs::Block::new();
        for code in [1, 2, 1] {
            block
                .push(row! { e: Value::Enum8(names.clone(), Enum8::of(code)) })
                .expect("the row matches the block");
        }

        insta::assert_snapshot!(converted(&block), @r"
        +---+
        | e |
        +---+
        | a |
        | b |
        | a |
        +---+
        ");
    }

    #[test]
    fn a_datetime64_converts_in_the_unit_its_precision_needs() {
        use arrow::datatypes::{DataType, TimeUnit};
        use chrono_tz::Tz;
        use clickhouse_rs::{row, types::Value};

        let mut block = clickhouse_rs::Block::new();
        // Ticks of 10^-precision seconds since the epoch.
        for (millis, micros, nanos) in [
            (1_713_962_096_789_i64, 1_713_962_096_789_012_i64, 1_i64),
            (
                4_102_444_800_001,
                4_102_444_800_000_001,
                4_102_444_800_000_000_001,
            ),
        ] {
            block
                .push(row! {
                    ms: Value::DateTime64(millis, (3, Tz::UTC)),
                    us: Value::DateTime64(micros, (6, Tz::UTC)),
                    ns: Value::DateTime64(nanos, (9, Tz::UTC)),
                    s: Value::DateTime64(millis / 1000, (0, Tz::UTC)),
                })
                .expect("the row matches the block");
        }

        let rec = super::block_to_arrow(&block).expect("the block converts to Arrow");
        let units: Vec<_> = rec
            .schema()
            .fields()
            .iter()
            .map(|field| field.data_type().clone())
            .collect();
        assert_eq!(
            units,
            [
                TimeUnit::Millisecond,
                TimeUnit::Microsecond,
                TimeUnit::Nanosecond,
                TimeUnit::Second
            ]
            .map(|unit| DataType::Timestamp(unit, None))
        );
        insta::assert_snapshot!(converted(&block), @r"
        +-------------------------+----------------------------+-------------------------------+---------------------+
        | ms                      | us                         | ns                            | s                   |
        +-------------------------+----------------------------+-------------------------------+---------------------+
        | 2024-04-24T12:34:56.789 | 2024-04-24T12:34:56.789012 | 1970-01-01T00:00:00.000000001 | 2024-04-24T12:34:56 |
        | 2100-01-01T00:00:00.001 | 2100-01-01T00:00:00.000001 | 2100-01-01T00:00:00.000000001 | 2100-01-01T00:00:00 |
        +-------------------------+----------------------------+-------------------------------+---------------------+
        ");
    }

    #[test]
    fn arrays_maps_and_tuples_convert_to_nested_arrow_values() {
        use std::{collections::HashMap, sync::Arc};

        use clickhouse_rs::{row, types::Value};

        let mut block = clickhouse_rs::Block::new();
        for (items, key, tuple) in [
            (vec![1_i32, 2, 3], "a", (1_i32, "one")),
            (vec![], "k", (-2, "")),
        ] {
            block
                .push(row! {
                    array: Value::from(items),
                    map: Value::from(HashMap::from([(key.to_string(), 7_i32)])),
                    tuple: Value::Tuple(Arc::new(vec![
                        Value::Int32(tuple.0),
                        Value::from(tuple.1),
                    ])),
                })
                .expect("the row matches the block");
        }

        insta::assert_snapshot!(converted(&block), @r"
        +-----------+--------+----------------+
        | array     | map    | tuple          |
        +-----------+--------+----------------+
        | [1, 2, 3] | {a: 7} | {1: 1, 2: one} |
        | []        | {k: 7} | {1: -2, 2: }   |
        +-----------+--------+----------------+
        ");
    }
}
