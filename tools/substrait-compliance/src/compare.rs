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

//! Result comparison based on IBM/substrait-compliance Rust SDK semantics
//! (row/column counts, normalised types, epsilon on numerics), with three
//! widenings needed for a DataFusion 54 result: `Integer`/`Bigint` are the
//! same family (`COUNT(*)` is Int64), strings trim CHAR padding, and numeric
//! compare uses relative as well as absolute 1e-9 epsilon.

use chrono::NaiveDate;
use datafusion::arrow::array::{
    Array, BooleanArray, Date32Array, Decimal128Array, Float64Array, Int32Array, Int64Array,
    StringArray, StringViewArray,
};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;

const NUMERIC_EPSILON: f64 = 1e-9;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CanonicalType {
    Integer,
    Bigint,
    Double,
    Boolean,
    String,
}

#[derive(Debug, Clone)]
pub struct TableData {
    pub columns: Vec<(String, CanonicalType)>,
    pub rows: Vec<Vec<String>>,
}

impl TableData {
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    pub fn column_count(&self) -> usize {
        self.columns.len()
    }
}

pub fn compare_results(actual: &TableData, expected: &TableData) -> Result<(), String> {
    if actual.row_count() != expected.row_count() {
        return Err(format!(
            "row count mismatch: actual {} vs expected {}",
            actual.row_count(),
            expected.row_count()
        ));
    }
    if actual.column_count() != expected.column_count() {
        return Err(format!(
            "column count mismatch: actual {} vs expected {}",
            actual.column_count(),
            expected.column_count()
        ));
    }
    for (idx, ((_, actual_ty), (_, expected_ty))) in actual
        .columns
        .iter()
        .zip(expected.columns.iter())
        .enumerate()
    {
        if !types_compatible(*actual_ty, *expected_ty) {
            return Err(format!(
                "column {idx} type mismatch: actual {actual_ty:?} vs expected {expected_ty:?}"
            ));
        }
    }
    for (row_idx, (actual_row, expected_row)) in
        actual.rows.iter().zip(expected.rows.iter()).enumerate()
    {
        for (col_idx, (actual_val, expected_val)) in
            actual_row.iter().zip(expected_row.iter()).enumerate()
        {
            if !values_match(actual_val, expected_val) {
                return Err(format!(
                    "cell [{row_idx},{col_idx}] mismatch: actual {actual_val:?} vs expected {expected_val:?}"
                ));
            }
        }
    }
    Ok(())
}

/// `Integer` and `Bigint` compare as the same family: `COUNT(*)` is Int64 in
/// DataFusion and `integer` in the IBM expected CSVs. Values are still
/// checked cell-by-cell.
fn types_compatible(actual: CanonicalType, expected: CanonicalType) -> bool {
    actual == expected
        || matches!(
            (actual, expected),
            (CanonicalType::Integer, CanonicalType::Bigint)
                | (CanonicalType::Bigint, CanonicalType::Integer)
        )
}

pub fn values_match(actual: &str, expected: &str) -> bool {
    if actual == expected {
        return true;
    }
    if let (Ok(a), Ok(e)) = (actual.parse::<f64>(), expected.parse::<f64>()) {
        if a.is_nan() && e.is_nan() {
            return true;
        }
        let abs_diff = (a - e).abs();
        if abs_diff <= NUMERIC_EPSILON {
            return true;
        }
        let scale = a.abs().max(e.abs()).max(1.0);
        return abs_diff / scale <= NUMERIC_EPSILON;
    }
    let actual_trim = actual.trim();
    let expected_trim = expected.trim();
    if actual_trim == expected_trim {
        return true;
    }
    actual_trim.eq_ignore_ascii_case(expected_trim)
        && matches!(actual_trim.to_ascii_lowercase().as_str(), "true" | "false")
}

pub fn parse_data_type(s: &str) -> CanonicalType {
    match s.to_ascii_lowercase().as_str() {
        "integer" | "int" | "int32" | "i32" | "smallint" | "int4" | "tinyint" | "i8" | "i16" => {
            CanonicalType::Integer
        }
        "bigint" | "int64" | "i64" | "long" | "int8" => CanonicalType::Bigint,
        "double" | "fp64" | "float8" | "numeric" | "decimal" | "float" | "fp32" | "real"
        | "float4" | "number" => CanonicalType::Double,
        "boolean" | "bool" => CanonicalType::Boolean,
        _ => CanonicalType::String,
    }
}

pub fn arrow_type_to_canonical(dt: &DataType) -> CanonicalType {
    match dt {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::UInt8 | DataType::UInt16 => {
            CanonicalType::Integer
        }
        DataType::Int64 | DataType::UInt32 | DataType::UInt64 => CanonicalType::Bigint,
        DataType::Float16
        | DataType::Float32
        | DataType::Float64
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _) => CanonicalType::Double,
        DataType::Boolean => CanonicalType::Boolean,
        _ => CanonicalType::String,
    }
}

pub fn batches_to_table_data(batches: &[RecordBatch]) -> Result<TableData, String> {
    if batches.is_empty() {
        return Ok(TableData {
            columns: Vec::new(),
            rows: Vec::new(),
        });
    }
    let schema = batches[0].schema();
    let columns = schema
        .fields()
        .iter()
        .map(|f| (f.name().clone(), arrow_type_to_canonical(f.data_type())))
        .collect();

    let mut rows = Vec::new();
    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            let mut row = Vec::with_capacity(batch.num_columns());
            for col_idx in 0..batch.num_columns() {
                row.push(format_cell(batch.column(col_idx).as_ref(), row_idx)?);
            }
            rows.push(row);
        }
    }
    Ok(TableData { columns, rows })
}

fn format_cell(array: &dyn Array, idx: usize) -> Result<String, String> {
    if array.is_null(idx) {
        return Ok(String::new());
    }
    match array.data_type() {
        DataType::Boolean => {
            let values = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| "boolean downcast failed".to_string())?;
            Ok(values.value(idx).to_string())
        }
        DataType::Int32 => {
            let values = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| "int32 downcast failed".to_string())?;
            Ok(values.value(idx).to_string())
        }
        DataType::Int64 => {
            let values = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "int64 downcast failed".to_string())?;
            Ok(values.value(idx).to_string())
        }
        DataType::Float64 => {
            let values = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| "float64 downcast failed".to_string())?;
            Ok(values.value(idx).to_string())
        }
        DataType::Decimal128(_, scale) => {
            let values = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| "decimal128 downcast failed".to_string())?;
            let raw = values.value(idx);
            let scale = i32::from(*scale);
            let value = raw as f64 / 10_f64.powi(scale);
            Ok(value.to_string())
        }
        DataType::Date32 => {
            let values = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| "date32 downcast failed".to_string())?;
            let days = i64::from(values.value(idx));
            let date = NaiveDate::from_ymd_opt(1970, 1, 1)
                .and_then(|epoch| epoch.checked_add_signed(chrono::Duration::days(days)))
                .ok_or_else(|| format!("date32 out of range: {days}"))?;
            Ok(date.format("%Y-%m-%d").to_string())
        }
        DataType::Timestamp(TimeUnit::Microsecond, _)
        | DataType::Timestamp(TimeUnit::Millisecond, _)
        | DataType::Timestamp(TimeUnit::Second, _)
        | DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            Ok(format!("<{dt:?}>", dt = array.data_type()))
        }
        DataType::Utf8 => {
            let values = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "utf8 downcast failed".to_string())?;
            Ok(values.value(idx).to_string())
        }
        DataType::Utf8View => {
            let values = array
                .as_any()
                .downcast_ref::<StringViewArray>()
                .ok_or_else(|| "utf8view downcast failed".to_string())?;
            Ok(values.value(idx).to_string())
        }
        other => Ok(format!("<{other:?}>")),
    }
}

/// Parse a pipe-delimited expected CSV with a typed header (`name:type|…`).
pub fn load_expected_csv(text: &str) -> Result<TableData, String> {
    let mut lines = text.lines().filter(|l| !l.trim().is_empty());
    let header = lines
        .next()
        .ok_or_else(|| "expected CSV is empty".to_string())?;
    let columns = header
        .split('|')
        .map(|field| {
            let (name, ty) = field.split_once(':').unwrap_or((field, "varchar"));
            (name.trim().to_string(), parse_data_type(ty.trim()))
        })
        .collect::<Vec<_>>();

    let mut rows = Vec::new();
    for line in lines {
        let row: Vec<String> = line.split('|').map(|v| v.trim().to_string()).collect();
        rows.push(row);
    }
    Ok(TableData { columns, rows })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn numeric_epsilon_matches() {
        assert!(values_match("380456.0", "380456"));
        assert!(values_match("1.0000000001", "1.0"));
        assert!(values_match("1193053.2253", "1193053.225299999"));
        assert!(values_match(
            " are carefully. slyly ",
            "are carefully. slyly"
        ));
        assert!(!values_match("1.1", "1.0"));
    }

    #[test]
    fn integer_and_bigint_are_compatible() {
        assert!(types_compatible(
            CanonicalType::Integer,
            CanonicalType::Bigint
        ));
        assert!(!types_compatible(
            CanonicalType::Integer,
            CanonicalType::Double
        ));
    }

    #[test]
    fn typed_header_parses() {
        let table =
            load_expected_csv("l_returnflag:string|count_order:integer\nA|14876\n").expect("parse");
        assert_eq!(table.column_count(), 2);
        assert_eq!(table.row_count(), 1);
        assert_eq!(table.columns[1].1, CanonicalType::Integer);
    }

    #[test]
    fn compare_detects_mismatch() {
        let expected = load_expected_csv("x:integer\n1\n").expect("expected");
        let actual = load_expected_csv("x:integer\n2\n").expect("actual");
        let err = compare_results(&actual, &expected).expect_err("mismatch");
        assert!(err.contains("mismatch"), "{err}");
    }
}
