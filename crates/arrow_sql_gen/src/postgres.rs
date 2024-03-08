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
use snafu::prelude::*;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio_postgres::types::FromSql;
use tokio_postgres::{types::Type, Row};

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

    #[snafu(display("Failed to parse raw Postgres Bytes as BigDecimal: {:?}", bytes))]
    FailedToParseBigDecmialFromPostgres { bytes: Vec<u8> },

    #[snafu(display("Cannot represent BigDecimal as i128: {big_decimal}"))]
    FailedToConvertBigDecmialToI128 { big_decimal: BigDecimal },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Converts Postgres `Row`s to an Arrow `RecordBatch`. Assumes that all rows have the same schema and
/// sets the schema based on the first row.
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

            let data_type = map_column_type_to_data_type(column_type);
            arrow_fields.push(Field::new(
                column_name,
                data_type.clone(),
                true, // TODO: Set nullable properly based on postgres schema
            ));
            arrow_columns_builders.push(map_data_type_to_array_builder(&data_type));
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
                    let Some(v_i128) = v.to_decimal_128() else {
                        return FailedToConvertBigDecmialToI128Snafu {
                            big_decimal: v.inner,
                        }
                        .fail();
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

fn map_data_type_to_array_builder(data_type: &DataType) -> Box<dyn ArrayBuilder> {
    match data_type {
        DataType::Int16 => Box::new(arrow::array::Int16Builder::new()),
        DataType::Int32 => Box::new(arrow::array::Int32Builder::new()),
        DataType::Int64 => Box::new(arrow::array::Int64Builder::new()),
        DataType::Float32 => Box::new(arrow::array::Float32Builder::new()),
        DataType::Float64 => Box::new(arrow::array::Float64Builder::new()),
        DataType::Utf8 => Box::new(arrow::array::StringBuilder::new()),
        DataType::Boolean => Box::new(arrow::array::BooleanBuilder::new()),
        DataType::Decimal128(precision, scale) => Box::new(
            arrow::array::Decimal128Builder::new()
                .with_precision_and_scale(*precision, *scale)
                .unwrap_or_default(),
        ),
        DataType::Timestamp(time_unit, time_zone) => match time_unit {
            arrow::datatypes::TimeUnit::Microsecond => Box::new(
                arrow::array::TimestampMicrosecondBuilder::new()
                    .with_timezone_opt(time_zone.clone()),
            ),
            arrow::datatypes::TimeUnit::Second => Box::new(
                arrow::array::TimestampSecondBuilder::new().with_timezone_opt(time_zone.clone()),
            ),
            arrow::datatypes::TimeUnit::Millisecond => Box::new(
                arrow::array::TimestampMillisecondBuilder::new()
                    .with_timezone_opt(time_zone.clone()),
            ),
            arrow::datatypes::TimeUnit::Nanosecond => Box::new(
                arrow::array::TimestampNanosecondBuilder::new()
                    .with_timezone_opt(time_zone.clone()),
            ),
        },
        _ => unimplemented!("Unsupported data type {:?}", data_type),
    }
}

struct BigDecimalFromSql {
    inner: BigDecimal,
    scale: u16,
}

impl BigDecimalFromSql {
    fn to_decimal_128(&self) -> Option<i128> {
        (&self.inner * 10i128.pow(u32::from(self.scale))).to_i128()
    }
}

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

        let base_10_000_digit_count = raw_u16[0];
        let weight = raw_u16[1];
        let sign = raw_u16[2];
        let scale = raw_u16[3];

        let mut base_10_000_digits = Vec::new();
        for i in 4..4 + base_10_000_digit_count {
            base_10_000_digits.push(raw_u16[i as usize]);
        }

        let mut u8_digits = Vec::new();
        for &base_10_000_digit in base_10_000_digits.iter().rev() {
            let mut base_10_000_digit = base_10_000_digit;
            let mut temp_result = Vec::new();
            while base_10_000_digit > 0 {
                temp_result.push((base_10_000_digit % 10) as u8);
                base_10_000_digit /= 10;
            }
            while temp_result.len() < 4 {
                temp_result.push(0);
            }
            u8_digits.extend(temp_result);
        }
        u8_digits.reverse();

        let base_10_000_digits_right_of_decimal = base_10_000_digit_count - weight - 1;
        let implied_base_10_zeros = scale - (base_10_000_digits_right_of_decimal * 4);
        u8_digits.resize(u8_digits.len() + implied_base_10_zeros as usize, 0);

        let sign = match sign {
            0x4000 => Sign::Minus,
            0x0000 => Sign::Plus,
            _ => {
                return Err(Box::new(Error::FailedToParseBigDecmialFromPostgres {
                    bytes: raw.to_vec(),
                }))
            }
        };

        let Some(digits) = BigInt::from_radix_be(sign, u8_digits.as_slice(), 10) else {
            return Err(Box::new(Error::FailedToParseBigDecmialFromPostgres {
                bytes: raw.to_vec(),
            }));
        };
        Ok(BigDecimalFromSql {
            inner: BigDecimal::new(digits, i64::from(scale)),
            scale,
        })
    }

    fn accepts(ty: &Type) -> bool {
        matches!(*ty, Type::NUMERIC)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[allow(clippy::cast_possible_truncation)]
    #[tokio::test]
    async fn test_big_decimal_from_sql() {
        let positive_u16: Vec<u16> = vec![5, 3, 0, 5, 9345, 1293, 2903, 1293, 932];
        let positive_raw: Vec<u8> = positive_u16
            .iter()
            .flat_map(|&x| vec![(x >> 8) as u8, x as u8])
            .collect();
        let positive =
            BigDecimal::from_str("9345129329031293.0932").expect("Failed to parse big decimal");
        let positive_result = BigDecimalFromSql::from_sql(&Type::NUMERIC, positive_raw.as_slice())
            .expect("Failed to run FromSql");
        assert_eq!(positive_result.inner, positive);

        let negative_u16: Vec<u16> = vec![5, 3, 0x4000, 5, 9345, 1293, 2903, 1293, 932];
        let negative_raw: Vec<u8> = negative_u16
            .iter()
            .flat_map(|&x| vec![(x >> 8) as u8, x as u8])
            .collect();
        let negative =
            BigDecimal::from_str("-9345129329031293.0932").expect("Failed to parse big decimal");
        let negative_result = BigDecimalFromSql::from_sql(&Type::NUMERIC, negative_raw.as_slice())
            .expect("Failed to run FromSql");
        assert_eq!(negative_result.inner, negative);
    }
}
