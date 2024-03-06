use std::convert;
use std::sync::Arc;

use arrow::array::ArrayBuilder;
use arrow::array::ArrayRef;
use arrow::array::RecordBatch;
use arrow::array::RecordBatchOptions;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;
use bigdecimal::num_bigint::BigInt;
use bigdecimal::num_bigint::Sign;
use bigdecimal::BigDecimal;
use bigdecimal::ToPrimitive;
use postgres::types::FromSql;
use postgres::{types::Type, Row};
use snafu::prelude::*;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to build record batch: {source}"))]
    FailedToBuildRecordBatch { source: arrow::error::ArrowError },

    #[snafu(display("No builder found for index {index}"))]
    NoBuilderForIndex { index: usize },

    #[snafu(display("Failed to downcast builder for {postgres_type}"))]
    FailedToDowncastBuilder { postgres_type: String },

    #[snafu(display("Integer overflow when converting u64 to i64: {source}"))]
    FailedToConvertU64toI64 {
        source: <u64 as convert::TryInto<i64>>::Error,
    },

    #[snafu(display("Failed to parse Bytes as BigDecimal: {:?}", bytes))]
    FailedToParseBigDecmial { bytes: Vec<u8> },

    #[snafu(display("Cannot represent BigDecimal as i128: {big_decimal}"))]
    FailedToConvertBigDecmialToI128 { big_decimal: BigDecimal },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Converts Postgres `Row`s to an Arrow `RecordBatch`.
///
/// # Errors
///
/// Returns an error if there is a failure in converting the rows to a `RecordBatch`.
#[allow(clippy::too_many_lines)]
pub fn rows_to_arrow(rows: &[Row]) -> Result<RecordBatch> {
    let mut arrow_fields: Vec<Field> = Vec::new();
    let mut arrow_columns_builders: Vec<Box<dyn ArrayBuilder>> = Vec::new();
    let mut postgres_types: Vec<Type> = Vec::new();

    if !rows.is_empty() {
        let row = &rows[0];
        for column in row.columns() {
            let column_name = column.name();
            let column_type = column.type_();

            arrow_fields.push(Field::new(
                column_name,
                map_column_type_to_data_type(column_type),
                true, // TODO: Set nullable properly based on postgres schema
            ));
            arrow_columns_builders.push(map_column_type_to_array_builder(column_type));
            postgres_types.push(column_type.clone());
        }
    }

    for row in rows {
        for (i, postgres_type) in postgres_types.iter().enumerate() {
            let Some(builder) = arrow_columns_builders.get_mut(i) else {
                return NoBuilderForIndexSnafu { index: i }.fail();
            };

            match *postgres_type {
                Type::INT2 => {
                    let Some(builder) = builder
                        .as_any_mut()
                        .downcast_mut::<arrow::array::Int16Builder>()
                    else {
                        return FailedToDowncastBuilderSnafu {
                            postgres_type: format!("{postgres_type}"),
                        }
                        .fail();
                    };
                    let v: i16 = row.get(i);
                    builder.append_value(v);
                }
                Type::INT4 => {
                    let Some(builder) = builder
                        .as_any_mut()
                        .downcast_mut::<arrow::array::Int32Builder>()
                    else {
                        return FailedToDowncastBuilderSnafu {
                            postgres_type: format!("{postgres_type}"),
                        }
                        .fail();
                    };
                    let v: i32 = row.get(i);
                    builder.append_value(v);
                }
                Type::INT8 => {
                    let Some(builder) = builder
                        .as_any_mut()
                        .downcast_mut::<arrow::array::Int64Builder>()
                    else {
                        return FailedToDowncastBuilderSnafu {
                            postgres_type: format!("{postgres_type}"),
                        }
                        .fail();
                    };
                    let v: i64 = row.get(i);
                    builder.append_value(v);
                }
                Type::FLOAT4 => {
                    let Some(builder) = builder
                        .as_any_mut()
                        .downcast_mut::<arrow::array::Float32Builder>()
                    else {
                        return FailedToDowncastBuilderSnafu {
                            postgres_type: format!("{postgres_type}"),
                        }
                        .fail();
                    };
                    let v: f32 = row.get(i);
                    builder.append_value(v);
                }
                Type::FLOAT8 => {
                    let Some(builder) = builder
                        .as_any_mut()
                        .downcast_mut::<arrow::array::Float64Builder>()
                    else {
                        return FailedToDowncastBuilderSnafu {
                            postgres_type: format!("{postgres_type}"),
                        }
                        .fail();
                    };
                    let v: f64 = row.get(i);
                    builder.append_value(v);
                }
                Type::TEXT => {
                    let Some(builder) = builder
                        .as_any_mut()
                        .downcast_mut::<arrow::array::StringBuilder>()
                    else {
                        return FailedToDowncastBuilderSnafu {
                            postgres_type: format!("{postgres_type}"),
                        }
                        .fail();
                    };
                    let v: &str = row.get(i);
                    builder.append_value(v);
                }
                Type::BOOL => {
                    let Some(builder) = builder
                        .as_any_mut()
                        .downcast_mut::<arrow::array::BooleanBuilder>()
                    else {
                        return FailedToDowncastBuilderSnafu {
                            postgres_type: format!("{postgres_type}"),
                        }
                        .fail();
                    };
                    let v: bool = row.get(i);
                    builder.append_value(v);
                }
                Type::NUMERIC => {
                    let Some(builder) = builder
                        .as_any_mut()
                        .downcast_mut::<arrow::array::Decimal128Builder>()
                    else {
                        return FailedToDowncastBuilderSnafu {
                            postgres_type: format!("{postgres_type}"),
                        }
                        .fail();
                    };

                    let v: BigDecimalFromSql = row.get(i);
                    tracing::error!("v: {}", v.0);
                    let Some(v_i128) = v.0.to_i128() else {
                        return FailedToConvertBigDecmialToI128Snafu { big_decimal: v.0 }.fail();
                    };
                    builder.append_value(v_i128);
                }
                Type::TIMESTAMP => {
                    // TODO: Figure out how to properly extract Timestamp TimeUnit from postgres, now we assume it's in microseconds
                    let Some(builder) = builder
                        .as_any_mut()
                        .downcast_mut::<arrow::array::TimestampMicrosecondBuilder>(
                    ) else {
                        return FailedToDowncastBuilderSnafu {
                            postgres_type: format!("{postgres_type}"),
                        }
                        .fail();
                    };
                    let v = row.get::<usize, SystemTime>(i);

                    if let Ok(v) = v.duration_since(UNIX_EPOCH) {
                        let timestamp: i64 = v
                            .as_secs()
                            .try_into()
                            .context(FailedToConvertU64toI64Snafu)?;
                        builder.append_value(timestamp);
                    }
                }
                _ => unimplemented!("Unsupported type {:?} for column index {i}", postgres_type,),
            }
        }
    }

    let columns = arrow_columns_builders
        .into_iter()
        .map(|mut builder| builder.finish())
        .collect::<Vec<ArrayRef>>();

    let options = &RecordBatchOptions::new().with_row_count(Some(rows.len()));
    match RecordBatch::try_new_with_options(Arc::new(Schema::new(arrow_fields)), columns, options) {
        Ok(record_batch) => Ok(record_batch),
        Err(e) => Err(e).context(FailedToBuildRecordBatchSnafu),
    }
}

struct BigDecimalFromSql(BigDecimal);

impl<'a> FromSql<'a> for BigDecimalFromSql {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> std::prelude::v1::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        let raw_u16: Vec<u16> = raw
            .chunks(2)
            .map(|chunk| {
                if chunk.len() == 2 {
                    ((u16::from(chunk[0])) << 8) | (u16::from(chunk[1]))
                } else {
                    (u16::from(chunk[0])) << 8
                }
            })
            .collect();

        let digit_count = raw_u16[0];
        let weight = raw_u16[1];
        let sign = raw_u16[2];
        let scale = raw_u16[3];

        tracing::error!("raw: {:?}", raw);
        tracing::error!("raw_u16: {:?}", raw_u16);
        tracing::error!("digit_count: {}", digit_count);
        tracing::error!("weight: {}", weight);
        tracing::error!("sign: {}", sign);
        tracing::error!("scale: {}", scale);

        let mut digits = Vec::with_capacity(digit_count as usize);
        for i in 4..4 + digit_count {
            digits.push(raw_u16[i as usize]);
        }

        tracing::error!("digits: {:?}", digits);

        let mut u8_digits = Vec::new();
        for digit in digits {
            let high_byte = (digit >> 8) as u8;
            let low_byte = (digit & 0xFF) as u8;
            u8_digits.push(high_byte);
            u8_digits.push(low_byte);
        }

        let sign = match sign {
            0x4000 => Sign::Minus,
            0x0000 => Sign::Plus,
            _ => {
                return Err(Box::new(Error::FailedToParseBigDecmial {
                    bytes: raw.to_vec(),
                }))
            }
        };

        let digits = BigInt::from_bytes_be(sign, u8_digits.as_slice());
        Ok(BigDecimalFromSql(BigDecimal::new(digits, i64::from(scale))))
    }

    fn accepts(ty: &Type) -> bool {
        matches!(*ty, Type::NUMERIC)
    }
}

fn map_column_type_to_data_type(column_type: &Type) -> DataType {
    match *column_type {
        Type::INT2 => DataType::Int16,
        Type::INT4 => DataType::Int32,
        Type::INT8 => DataType::Int64,
        Type::FLOAT4 => DataType::Float32,
        Type::FLOAT8 => DataType::Float64,
        Type::TEXT => DataType::Utf8,
        Type::BOOL => DataType::Boolean,
        // TODO: Figure out how to handle decimal scale and precision, it isn't specified as a type in postgres types
        Type::NUMERIC => DataType::Decimal128(38, 9),
        // TODO: Figure out how to properly extract Timestamp TimeUnit from postgres, now we assume it's in microseconds
        Type::TIMESTAMP => DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
        _ => unimplemented!("Unsupported column type {:?}", column_type),
    }
}

fn map_column_type_to_array_builder(column_type: &Type) -> Box<dyn ArrayBuilder> {
    match *column_type {
        Type::INT2 => Box::new(arrow::array::Int16Builder::new()),
        Type::INT4 => Box::new(arrow::array::Int32Builder::new()),
        Type::INT8 => Box::new(arrow::array::Int64Builder::new()),
        Type::FLOAT4 => Box::new(arrow::array::Float32Builder::new()),
        Type::FLOAT8 => Box::new(arrow::array::Float64Builder::new()),
        Type::TEXT => Box::new(arrow::array::StringBuilder::new()),
        Type::BOOL => Box::new(arrow::array::BooleanBuilder::new()),
        // TODO: Figure out how to handle decimal scale and precision, it isn't specified as a type in postgres types
        Type::NUMERIC => Box::new(
            arrow::array::Decimal128Builder::new()
                .with_precision_and_scale(38, 9)
                .unwrap_or_default(),
        ),
        Type::TIMESTAMP => Box::new(arrow::array::TimestampMicrosecondBuilder::new()),
        _ => unimplemented!("Unsupported column type {:?}", column_type),
    }
}
