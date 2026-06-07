/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! Postgres `Row` → Arrow `RecordBatch` conversion for the analytical
//! correctness gate. Maps the subset of PG types produced by CH-benCH queries
//! into target Arrow types compatible with the validation comparator
//! (see `test_framework::queries::validation::datatype_equivalent`).

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanBuilder, Date32Builder, Float32Builder, Float64Builder, Int32Builder,
    Int64Builder, RecordBatch, StringBuilder, TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use tokio_postgres::Row;
use tokio_postgres::types::Type;

fn epoch_date() -> NaiveDate {
    NaiveDate::from_ymd_opt(1970, 1, 1).expect("valid epoch date")
}

/// Convert a vector of Postgres rows into a single Arrow `RecordBatch`.
///
/// Returns `Err` with a descriptive message if a column has an unsupported
/// Postgres type or if a value fails to decode.
pub fn rows_to_record_batch(rows: &[Row]) -> Result<RecordBatch, String> {
    let columns = if rows.is_empty() {
        Vec::new()
    } else {
        rows[0].columns().to_vec()
    };

    let fields: Vec<Field> = columns
        .iter()
        .map(|col| {
            let arrow_ty = pg_type_to_arrow(col.type_())
                .ok_or_else(|| format!("unsupported PG type for column `{}`: {}", col.name(), col.type_()))?;
            Ok::<_, String>(Field::new(col.name(), arrow_ty, true))
        })
        .collect::<Result<_, _>>()?;

    let schema = Arc::new(Schema::new(fields));

    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(columns.len());
    for (idx, col) in columns.iter().enumerate() {
        let array = build_column(rows, idx, col.type_(), col.name())?;
        arrays.push(array);
    }

    if arrays.is_empty() {
        // No columns: return an empty batch with row count from `rows`.
        return RecordBatch::try_new_with_options(
            schema,
            arrays,
            &arrow::array::RecordBatchOptions::new().with_row_count(Some(rows.len())),
        )
        .map_err(|e| format!("failed to build empty RecordBatch: {e}"));
    }

    RecordBatch::try_new(schema, arrays).map_err(|e| format!("failed to build RecordBatch: {e}"))
}

fn pg_type_to_arrow(ty: &Type) -> Option<DataType> {
    Some(match *ty {
        Type::INT2 | Type::INT4 => DataType::Int32,
        Type::INT8 => DataType::Int64,
        Type::FLOAT4 => DataType::Float32,
        Type::FLOAT8 | Type::NUMERIC => DataType::Float64,
        Type::BOOL => DataType::Boolean,
        Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME | Type::CHAR => DataType::Utf8,
        Type::DATE => DataType::Date32,
        Type::TIMESTAMP | Type::TIMESTAMPTZ => DataType::Timestamp(TimeUnit::Microsecond, None),
        _ => return None,
    })
}

#[allow(clippy::too_many_lines)]
fn build_column(rows: &[Row], idx: usize, ty: &Type, name: &str) -> Result<ArrayRef, String> {
    let convert_err = |e: tokio_postgres::Error| {
        format!("decode column `{name}` (type {ty}, row index {idx}): {e}")
    };

    Ok(match *ty {
        Type::INT2 => {
            let mut b = Int32Builder::with_capacity(rows.len());
            for row in rows {
                let v: Option<i16> = row.try_get(idx).map_err(convert_err)?;
                b.append_option(v.map(i32::from));
            }
            Arc::new(b.finish())
        }
        Type::INT4 => {
            let mut b = Int32Builder::with_capacity(rows.len());
            for row in rows {
                let v: Option<i32> = row.try_get(idx).map_err(convert_err)?;
                b.append_option(v);
            }
            Arc::new(b.finish())
        }
        Type::INT8 => {
            let mut b = Int64Builder::with_capacity(rows.len());
            for row in rows {
                let v: Option<i64> = row.try_get(idx).map_err(convert_err)?;
                b.append_option(v);
            }
            Arc::new(b.finish())
        }
        Type::FLOAT4 => {
            let mut b = Float32Builder::with_capacity(rows.len());
            for row in rows {
                let v: Option<f32> = row.try_get(idx).map_err(convert_err)?;
                b.append_option(v);
            }
            Arc::new(b.finish())
        }
        Type::FLOAT8 => {
            let mut b = Float64Builder::with_capacity(rows.len());
            for row in rows {
                let v: Option<f64> = row.try_get(idx).map_err(convert_err)?;
                b.append_option(v);
            }
            Arc::new(b.finish())
        }
        Type::NUMERIC => {
            let mut b = Float64Builder::with_capacity(rows.len());
            for row in rows {
                let v: Option<Decimal> = row.try_get(idx).map_err(convert_err)?;
                b.append_option(v.and_then(|d| d.to_f64()));
            }
            Arc::new(b.finish())
        }
        Type::BOOL => {
            let mut b = BooleanBuilder::with_capacity(rows.len());
            for row in rows {
                let v: Option<bool> = row.try_get(idx).map_err(convert_err)?;
                b.append_option(v);
            }
            Arc::new(b.finish())
        }
        Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME => {
            let mut b = StringBuilder::with_capacity(rows.len(), rows.len() * 16);
            for row in rows {
                let v: Option<&str> = row.try_get(idx).map_err(convert_err)?;
                b.append_option(v);
            }
            Arc::new(b.finish())
        }
        Type::CHAR => {
            // PG `"char"` (single byte) — read as i8 and stringify.
            let mut b = StringBuilder::with_capacity(rows.len(), rows.len());
            for row in rows {
                let v: Option<i8> = row.try_get(idx).map_err(convert_err)?;
                b.append_option(v.map(|c| (c as u8 as char).to_string()));
            }
            Arc::new(b.finish())
        }
        Type::DATE => {
            let epoch = epoch_date();
            let mut b = Date32Builder::with_capacity(rows.len());
            for row in rows {
                let v: Option<NaiveDate> = row.try_get(idx).map_err(convert_err)?;
                b.append_option(v.map(|d| {
                    i32::try_from(d.signed_duration_since(epoch).num_days()).unwrap_or(i32::MAX)
                }));
            }
            Arc::new(b.finish())
        }
        Type::TIMESTAMP => {
            let mut b = TimestampMicrosecondBuilder::with_capacity(rows.len());
            for row in rows {
                let v: Option<NaiveDateTime> = row.try_get(idx).map_err(convert_err)?;
                b.append_option(v.map(|ts| ts.and_utc().timestamp_micros()));
            }
            Arc::new(b.finish())
        }
        Type::TIMESTAMPTZ => {
            let mut b = TimestampMicrosecondBuilder::with_capacity(rows.len());
            for row in rows {
                let v: Option<DateTime<Utc>> = row.try_get(idx).map_err(convert_err)?;
                b.append_option(v.map(|ts| ts.timestamp_micros()));
            }
            Arc::new(b.finish())
        }
        _ => return Err(format!("unsupported PG type for column `{name}`: {ty}")),
    })
}
