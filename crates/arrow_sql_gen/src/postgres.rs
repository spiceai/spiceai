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

use std::convert;
use std::sync::Arc;

use crate::arrow::map_data_type_to_array_builder_optional;
use arrow::array::ArrayBuilder;
use arrow::array::ArrayRef;
use arrow::array::BooleanBuilder;
use arrow::array::Date32Builder;
use arrow::array::Decimal128Builder;
use arrow::array::FixedSizeBinaryBuilder;
use arrow::array::Float32Builder;
use arrow::array::Float64Builder;
use arrow::array::Int16Builder;
use arrow::array::Int32Builder;
use arrow::array::Int64Builder;
use arrow::array::ListBuilder;
use arrow::array::RecordBatch;
use arrow::array::RecordBatchOptions;
use arrow::array::StringBuilder;
use arrow::array::TimestampMillisecondBuilder;
use arrow::datatypes::DataType;
use arrow::datatypes::Date32Type;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;
use arrow::datatypes::TimeUnit;
use bigdecimal::num_bigint::BigInt;
use bigdecimal::num_bigint::Sign;
use bigdecimal::BigDecimal;
use bigdecimal::ToPrimitive;
use snafu::prelude::*;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio_postgres::types::FromSql;
use tokio_postgres::{types::Type, Column, Row};

pub mod builder;

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

    #[snafu(display("Integer overflow when converting u128 to i64: {source}"))]
    FailedToConvertU128toI64 {
        source: <u128 as convert::TryInto<i64>>::Error,
    },

    #[snafu(display("Error building fixed size binary field: {source}"))]
    FailedToBuildFixedSizeBinary { source: arrow::error::ArrowError },

    #[snafu(display("Failed to get a row value for {pg_type}: {source}"))]
    FailedToGetRowValue {
        pg_type: Type,
        source: tokio_postgres::Error,
    },

    #[snafu(display("Failed to parse raw Postgres Bytes as BigDecimal: {:?}", bytes))]
    FailedToParseBigDecimalFromPostgres { bytes: Vec<u8> },

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

/// Converts Postgres Columns to Arrow Data Types
///
/// # Errors
///
/// Returns an error if the Postgres column type is not supported
pub fn columns_to_schema(cols: &[Column]) -> Result<Arc<Schema>> {
    let mut arrow_fields: Vec<Option<Field>> = Vec::new();

    for column in cols {
        let column_name = column.name();
        let column_type = column.type_();
        let data_type = map_column_type_to_data_type(column_type);
        match &data_type {
            Some(data_type) => {
                arrow_fields.push(Some(Field::new(column_name, data_type.clone(), true)));
            }
            None => arrow_fields.push(None),
        }
    }

    let arrow_fields = arrow_fields.into_iter().flatten().collect::<Vec<Field>>();

    Ok(Arc::new(Schema::new(arrow_fields)))
}

/// Converts Postgres `Row`s to an Arrow `RecordBatch`. Assumes that all rows have the same schema and
/// sets the schema based on the first row.
///
/// # Errors
///
/// Returns an error if there is a failure in converting the rows to a `RecordBatch`.
#[allow(clippy::too_many_lines)]
pub fn rows_to_arrow(rows: &[Row]) -> Result<RecordBatch> {
    let mut arrow_fields: Vec<Option<Field>> = Vec::new();
    let mut arrow_columns_builders: Vec<Option<Box<dyn ArrayBuilder>>> = Vec::new();
    let mut postgres_types: Vec<Type> = Vec::new();
    let mut column_names: Vec<String> = Vec::new();

    if !rows.is_empty() {
        let row = &rows[0];
        for column in row.columns() {
            let column_name = column.name();
            let column_type = column.type_();
            let data_type = map_column_type_to_data_type(column_type);
            match &data_type {
                Some(data_type) => {
                    arrow_fields.push(Some(Field::new(column_name, data_type.clone(), true)));
                }
                None => arrow_fields.push(None),
            }
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
                Type::FLOAT4 => {
                    handle_primitive_type!(builder, Type::FLOAT4, Float32Builder, f32, row, i);
                }
                Type::FLOAT8 => {
                    handle_primitive_type!(builder, Type::FLOAT8, Float64Builder, f64, row, i);
                }
                Type::TEXT => {
                    handle_primitive_type!(builder, Type::TEXT, StringBuilder, &str, row, i);
                }
                Type::VARCHAR => {
                    handle_primitive_type!(builder, Type::VARCHAR, StringBuilder, &str, row, i);
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
                Type::NUMERIC => {
                    let v: Option<BigDecimalFromSql> =
                        row.try_get(i).context(FailedToGetRowValueSnafu {
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

                    let Some(v) = v else {
                        dec_builder.append_null();
                        continue;
                    };

                    let Some(v_i128) = v.to_decimal_128() else {
                        return FailedToConvertBigDecimalToI128Snafu {
                            big_decimal: v.inner,
                        }
                        .fail();
                    };
                    dec_builder.append_value(v_i128);
                }
                ref pg_type @ (Type::TIMESTAMP | Type::TIMESTAMPTZ) => {
                    let Some(builder) = builder else {
                        return NoBuilderForIndexSnafu { index: i }.fail();
                    };
                    let Some(builder) = builder
                        .as_any_mut()
                        .downcast_mut::<TimestampMillisecondBuilder>()
                    else {
                        return FailedToDowncastBuilderSnafu {
                            postgres_type: format!("{postgres_type}"),
                        }
                        .fail();
                    };
                    let v = row
                        .try_get::<usize, Option<SystemTime>>(i)
                        .with_context(|_| FailedToGetRowValueSnafu {
                            pg_type: pg_type.clone(),
                        })?;

                    match v {
                        Some(v) => {
                            if let Ok(v) = v.duration_since(UNIX_EPOCH) {
                                let timestamp: i64 = v
                                    .as_millis()
                                    .try_into()
                                    .context(FailedToConvertU128toI64Snafu)?;
                                builder.append_value(timestamp);
                            }
                        }
                        None => builder.append_null(),
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
                    let Some(builder) = builder
                        .as_any_mut()
                        .downcast_mut::<FixedSizeBinaryBuilder>()
                    else {
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
                        Some(v) => builder
                            .append_value(v)
                            .context(FailedToBuildFixedSizeBinarySnafu)?,
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
                _ => unimplemented!("Unsupported type {:?} for column index {i}", postgres_type,),
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

fn map_column_type_to_data_type(column_type: &Type) -> Option<DataType> {
    match *column_type {
        Type::INT2 => Some(DataType::Int16),
        Type::INT4 => Some(DataType::Int32),
        Type::INT8 => Some(DataType::Int64),
        Type::FLOAT4 => Some(DataType::Float32),
        Type::FLOAT8 => Some(DataType::Float64),
        Type::TEXT | Type::VARCHAR | Type::BPCHAR => Some(DataType::Utf8),
        Type::BOOL => Some(DataType::Boolean),
        // Inspect the scale from the first row. Precision will always be 38 for Decimal128.
        Type::NUMERIC => None,
        // We get a SystemTime that we can always convert into milliseconds
        Type::TIMESTAMP | Type::TIMESTAMPTZ => {
            Some(DataType::Timestamp(TimeUnit::Millisecond, None))
        }
        Type::DATE => Some(DataType::Date32),
        Type::UUID => Some(DataType::FixedSizeBinary(16)),
        Type::INT2_ARRAY => Some(DataType::List(Arc::new(Field::new(
            "item",
            DataType::Int16,
            true,
        )))),
        Type::INT4_ARRAY => Some(DataType::List(Arc::new(Field::new(
            "item",
            DataType::Int32,
            true,
        )))),
        Type::INT8_ARRAY => Some(DataType::List(Arc::new(Field::new(
            "item",
            DataType::Int64,
            true,
        )))),
        Type::FLOAT4_ARRAY => Some(DataType::List(Arc::new(Field::new(
            "item",
            DataType::Float32,
            true,
        )))),
        Type::FLOAT8_ARRAY => Some(DataType::List(Arc::new(Field::new(
            "item",
            DataType::Float64,
            true,
        )))),
        Type::TEXT_ARRAY => Some(DataType::List(Arc::new(Field::new(
            "item",
            DataType::Utf8,
            true,
        )))),
        Type::BOOL_ARRAY => Some(DataType::List(Arc::new(Field::new(
            "item",
            DataType::Boolean,
            true,
        )))),
        _ => unimplemented!("Unsupported column type {:?}", column_type),
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

    fn scale(&self) -> u16 {
        self.scale
    }
}

#[allow(clippy::cast_sign_loss)]
#[allow(clippy::cast_possible_wrap)]
#[allow(clippy::cast_possible_truncation)]
impl<'a> FromSql<'a> for BigDecimalFromSql {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> std::prelude::v1::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        let raw_u16: Vec<u16> = raw
            .chunks(2)
            .map(|chunk| {
                if chunk.len() == 2 {
                    u16::from_be_bytes([chunk[0], chunk[1]])
                } else {
                    u16::from_be_bytes([chunk[0], 0])
                }
            })
            .collect();

        let base_10_000_digit_count = raw_u16[0];
        let weight = raw_u16[1] as i16;
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

        let value_scale = 4 * (i64::from(base_10_000_digit_count) - i64::from(weight) - 1);
        let size = i64::try_from(u8_digits.len())? + i64::from(scale) - value_scale;
        u8_digits.resize(size as usize, 0);

        let sign = match sign {
            0x4000 => Sign::Minus,
            0x0000 => Sign::Plus,
            _ => {
                return Err(Box::new(Error::FailedToParseBigDecimalFromPostgres {
                    bytes: raw.to_vec(),
                }))
            }
        };

        let Some(digits) = BigInt::from_radix_be(sign, u8_digits.as_slice(), 10) else {
            return Err(Box::new(Error::FailedToParseBigDecimalFromPostgres {
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
