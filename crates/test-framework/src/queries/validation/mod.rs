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
    collections::{BTreeMap, HashMap, HashSet},
    io::Seek,
    sync::{Arc, LazyLock},
};

use anyhow::{Result, anyhow};

use arrow::{
    array::{
        Array, BinaryArray, BinaryViewArray, BooleanArray, Date32Array, Date64Array,
        Decimal128Array, Decimal256Array, Float32Array, Float64Array, Int8Array, Int16Array,
        Int32Array, Int64Array, LargeBinaryArray, LargeStringArray, RecordBatch, StringArray,
        StringViewArray, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray, TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array,
        UInt64Array,
    },
    csv::reader::Format,
    datatypes::TimeUnit,
};
use arrow::{
    csv::ReaderBuilder,
    datatypes::{DataType, SchemaRef},
};
use chrono::{DateTime, NaiveDate};

use arrow_tools::schema::schema_difference;

use super::Query;

pub mod sort_order;

pub use sort_order::{
    KeyedSortLimit, SortKeyCells, SortKeyColumn, SortKeyResolution, SortOrderViolation,
    UnorderedLimit, has_top_level_limit, has_top_level_order_by, projected_sort_limit,
    resolve_sort_key, unordered_limit, unprojected_sort_limit,
};

// Not re-exported: the outcome type is plumbing between this module and
// `sort_order`. Callers consume the reasons via `SortCheckedComparison`.
use sort_order::SortCheck;

/// A content comparison plus what the row-order check could and could not cover.
///
/// `unchecked` exists so a coverage hole cannot masquerade as a pass. A skipped
/// or partial sort check leaves `result` at whatever the content comparison said
/// — the rows really were compared — while naming the part of the `ORDER BY`
/// nobody verified, for the caller to count and report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortCheckedComparison {
    pub result: QueryValidationResult,
    /// One entry per side whose `ORDER BY` was not fully verified.
    pub unchecked: Vec<String>,
}

impl SortCheckedComparison {
    /// True when the content matched *and* the whole `ORDER BY` was verified.
    #[must_use]
    pub fn is_fully_verified_pass(&self) -> bool {
        self.result == QueryValidationResult::Pass && self.unchecked.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryValidationFailReason {
    NoExpectedAnswer,
    /// A static TPCH answer exists for the query, but only at scale factor 1.0.
    /// Validating at any other scale factor requires a configured reference
    /// schema, so this is reported distinctly from [`Self::NoExpectedAnswer`]
    /// (which means no expected answer exists for the query at all).
    NoExpectedAnswerAtScaleFactor,
    NoAnswer,
    SchemaMismatch,
    RowCountMismatch {
        expected: usize,
        actual: usize,
    },
    DataMismatch {
        column: String,
        row_number: usize,
        expected: String,
        actual: String,
    },
    ColumnLengthMismatch {
        column_name: String,
        left_len: usize,
        right_len: usize,
    },
    /// One engine returned rows that do not honor the query's own top-level
    /// `ORDER BY`. Reported per side, because it is a property of that engine's
    /// output rather than of the two results' relationship.
    SortOrderViolation {
        side: String,
        violation: SortOrderViolation,
    },
    /// A query with a top-level `LIMIT` returned a row its full result does not
    /// allow at that `LIMIT`: one the full result does not have, or one past the
    /// rows its `ORDER BY` keeps.
    RowNotAllowedByLimit {
        row_number: usize,
        row: String,
    },
    /// A query that sorts on a column its result does not return put a row where
    /// its `ORDER BY` does not allow it: the row belongs to the answer, but the
    /// reference's sort keys place it in a different tie group.
    RowOutOfSortOrder {
        row_number: usize,
        row: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryValidationResult {
    Pass,
    Fail(QueryValidationFailReason),
}

macro_rules! generate_tpch_answers {
    ( $( $i:tt ),* ) => {
        vec![
            $(
                (
                    concat!("tpch_q", stringify!($i)),
                    include_str!(concat!("./tpch/q", stringify!($i), ".csv"))
                )
            ),*
        ]
    }
}

static TPCH_ANSWERS: LazyLock<BTreeMap<Arc<str>, Vec<RecordBatch>>> = LazyLock::new(|| {
    #[expect(clippy::expect_used)]
    {
        let mut map = BTreeMap::new();
        // Load TPCH answers from CSV files, into RecordBatches
        // and store them in the map with the query name as the key
        let answers = generate_tpch_answers!(
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22
        );

        for (query_name, csv_contents) in answers {
            let mut string_reader = std::io::Cursor::new(csv_contents);
            let format = Format::default().with_delimiter(b'|').with_header(true);
            let (schema, _) = format
                .infer_schema(&mut string_reader, None)
                .expect("Should infer schema");
            string_reader.rewind().expect("Should rewind file");

            // create a builder
            let reader = ReaderBuilder::new(Arc::new(schema))
                .with_format(format.clone())
                .build(string_reader)
                .expect("Should build reader");

            // read the batches
            let mut batches = Vec::new();
            for batch in reader {
                let batch = batch.expect("Should read batch");
                batches.push(batch);
            }

            // Store the batches in the map
            map.insert(query_name.into(), batches.clone());
            map.insert(
                query_name.replace("tpch_", "tpch[parameterized]_").into(),
                batches,
            );
        }

        map
    }
});

#[must_use]
pub(crate) fn has_static_tpch_answer(query: &Query) -> bool {
    TPCH_ANSWERS.contains_key(&query.name)
}

#[must_use]
pub fn should_validate_with_static_tpch_answer(query: &Query, scale_factor: f64) -> bool {
    (scale_factor - 1.0).abs() < f64::EPSILON && has_static_tpch_answer(query)
}

/// True for a `Date32`/`Date64` against a timezone-free `Timestamp`, in either
/// order. Oracle's `DATE` is a datetime, so a date column arrives as a
/// `Timestamp`.
fn is_date_and_timestamp_pair(left: &DataType, right: &DataType) -> bool {
    matches!(
        (left, right),
        (
            DataType::Date32 | DataType::Date64,
            DataType::Timestamp(_, None)
        ) | (
            DataType::Timestamp(_, None),
            DataType::Date32 | DataType::Date64
        )
    )
}

fn datatype_equivalent(expected_type: &DataType, actual_type: &DataType) -> bool {
    if expected_type == actual_type || is_date_and_timestamp_pair(expected_type, actual_type) {
        return true;
    }

    // Check for logical equivalence, with a lenient set of rules
    // E.g. a number could be returned as a string, number, or float.
    match (expected_type, actual_type) {
        // Handle timestamp timezone differences
        (DataType::Timestamp(unit1, tz1), DataType::Timestamp(unit2, tz2)) => {
            // Same time unit is required
            if unit1 != unit2 {
                return false;
            }
            // Allow timezone differences between None and Some("UTC")
            matches!(
                (tz1.as_deref(), tz2.as_deref()),
                (None, Some("UTC" | "+00:00")) | (Some("UTC" | "+00:00"), None)
            )
        }
        // Existing numeric and string type equivalences
        _ => matches!(
            (expected_type, actual_type),
            (DataType::Float32, DataType::Float64)
                | (DataType::Float64 | DataType::Int32, DataType::Int64)
                | (
                    DataType::Float64 | DataType::Int64,
                    DataType::Decimal128(_, _)
                )
                | (
                    DataType::Decimal128(_, _),
                    DataType::Float64 | DataType::Int64
                )
                | (
                    DataType::Int64,
                    DataType::Int32
                        | DataType::Int8
                        | DataType::Float64
                        | DataType::Utf8
                        | DataType::LargeUtf8
                        | DataType::Utf8View
                )
                | (DataType::Utf8, DataType::LargeUtf8 | DataType::Utf8View)
                | (DataType::Utf8View, DataType::Utf8 | DataType::LargeUtf8)
                | (DataType::LargeUtf8, DataType::Utf8)
                | (
                    DataType::Date32,
                    DataType::Date64 | DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
                )
                | (DataType::Date64, DataType::Date32)
        ),
    }
}

fn equivalent_schemas(expected_schema: &SchemaRef, actual_schema: &SchemaRef) -> bool {
    if expected_schema.fields().len() != actual_schema.fields().len() {
        return false;
    }

    expected_schema
        .fields()
        .iter()
        .zip(actual_schema.fields().iter())
        .all(|(f1, f2)| datatype_equivalent(f1.data_type(), f2.data_type()))
}

macro_rules! downcast_and_stringify {
    ($array:expr, $index:expr, $t:ty) => {{
        Ok(Some(
            $array
                .as_any()
                .downcast_ref::<$t>()
                .ok_or_else(|| anyhow!("Failed to downcast array"))?
                .value($index)
                .to_string(),
        ))
    }};
}

macro_rules! downcast_and_stringify_ts {
    ($array:expr, $index:expr, $t:ty, $scale:expr, $format:expr) => {{
        let ts = $array
            .as_any()
            .downcast_ref::<$t>()
            .ok_or_else(|| anyhow!("Failed to downcast timestamp array"))?
            .value($index);
        let secs = ts / $scale;
        let sub = ts.rem_euclid($scale);
        let sub_u32 = u32::try_from(sub)
            .map_err(|_| anyhow!("Subsecond value out of range for u32: {}", sub))?;
        let nanos = sub_u32 * (1_000_000_000u32 / $scale as u32);
        let dt = DateTime::from_timestamp(secs, nanos)
            .ok_or_else(|| anyhow!("Invalid timestamp from seconds={} nanos={}", secs, nanos))?;
        Ok(Some(dt.format($format).to_string()))
    }};
}

/// Converts a value from an Arrow `Array` at a specific index into its string representation.
///
/// Designed not to be used for production stringification, but rather for producing consistent values for validation results.
/// Using input `RecordBatch` values, it attempts to remove any system differences from underlying sources (e.g. timestamp formats, etc).
///
/// # Parameters:
/// - `array`: A reference to a dynamically typed Arrow `Array`. This is the array that holds the data.
/// - `index`: The index of the value to convert to a string.
///
/// # Returns:
/// - `Ok(Some(String))`: A string representation of the value at the specified index.
/// - `Ok(None)`: If the value is `null` at the given index, or the type is not implemented for conversion.
/// - `Err(anyhow::Error)`: If there is an error (e.g., invalid index, failed downcast).
///
/// # Example:
/// ```rust,ignore
/// use arrow::array::Int64Array;
/// let array = Int64Array::from(vec![12345]);
/// let result = array_value_to_string(&array, 0);
/// assert_eq!(result.unwrap(), Some("12345".to_string()));
/// ```
///
/// # Error Handling:
/// - If the `index` is out of bounds, the function returns an error indicating the invalid index.
/// - If the value at the index is `null`, `None` is returned.
/// - If the function fails to downcast the array to the expected type (e.g., if the array's type is
///   mismatched), it will return an error.
/// - If the array's data type is not supported for conversion, `None` is returned.
pub fn array_value_to_string(array: &dyn Array, index: usize) -> Result<Option<String>> {
    if array.len() <= index {
        return Err(anyhow!("Index out of bounds: {index} >= {}", array.len()));
    }

    if array.is_null(index) {
        return Ok(None);
    }

    match array.data_type() {
        // Entirely-null columns (e.g. CSV import of all-`\\N` field) collapse to
        // Arrow `Null` — every cell is null.
        DataType::Null => Ok(None),
        DataType::Int64 => downcast_and_stringify!(array, index, Int64Array),
        DataType::Int32 => downcast_and_stringify!(array, index, Int32Array),
        DataType::Int16 => downcast_and_stringify!(array, index, Int16Array),
        DataType::Int8 => downcast_and_stringify!(array, index, Int8Array),
        DataType::UInt64 => downcast_and_stringify!(array, index, UInt64Array),
        DataType::UInt32 => downcast_and_stringify!(array, index, UInt32Array),
        DataType::UInt16 => downcast_and_stringify!(array, index, UInt16Array),
        DataType::UInt8 => downcast_and_stringify!(array, index, UInt8Array),
        DataType::Float32 => downcast_and_stringify!(array, index, Float32Array),
        DataType::Float64 => downcast_and_stringify!(array, index, Float64Array),
        DataType::Utf8 => Ok(Some(text_to_string(
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow!("Failed to downcast Utf8 array"))?
                .value(index),
        ))),
        DataType::LargeUtf8 => Ok(Some(text_to_string(
            array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| anyhow!("Failed to downcast LargeUtf8 array"))?
                .value(index),
        ))),
        DataType::Utf8View => Ok(Some(text_to_string(
            array
                .as_any()
                .downcast_ref::<StringViewArray>()
                .ok_or_else(|| anyhow!("Failed to downcast Utf8View array"))?
                .value(index),
        ))),
        DataType::Binary => Ok(Some(bytes_to_string(
            array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| anyhow!("Failed to downcast Binary array"))?
                .value(index),
        ))),
        DataType::LargeBinary => Ok(Some(bytes_to_string(
            array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| anyhow!("Failed to downcast LargeBinary array"))?
                .value(index),
        ))),
        DataType::BinaryView => Ok(Some(bytes_to_string(
            array
                .as_any()
                .downcast_ref::<BinaryViewArray>()
                .ok_or_else(|| anyhow!("Failed to downcast BinaryView array"))?
                .value(index),
        ))),
        DataType::Boolean => downcast_and_stringify!(array, index, BooleanArray),

        DataType::Date32 => {
            let days = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| anyhow!("Failed to downcast Date32 array"))?
                .value(index);
            let date = NaiveDate::from_ymd_opt(1970, 1, 1)
                .ok_or_else(|| anyhow!("Invalid base date"))?
                .checked_add_signed(chrono::Duration::days(i64::from(days)))
                .ok_or_else(|| anyhow!("Date out of range"))?;
            Ok(Some(date.format("%Y-%m-%d").to_string()))
        }

        DataType::Date64 => {
            let millis = array
                .as_any()
                .downcast_ref::<Date64Array>()
                .ok_or_else(|| anyhow!("Failed to downcast Date64 array"))?
                .value(index);
            let days = millis / 86_400_000; // Convert milliseconds to days
            let date = NaiveDate::from_ymd_opt(1970, 1, 1)
                .ok_or_else(|| anyhow!("Invalid base date"))?
                .checked_add_signed(chrono::Duration::days(days))
                .ok_or_else(|| anyhow!("Date out of range"))?;
            Ok(Some(date.format("%Y-%m-%d").to_string()))
        }

        DataType::Decimal128(_, scale) => {
            let val = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| anyhow!("Failed to downcast Decimal128 array"))?
                .value(index);

            let sign = if val < 0 { "-" } else { "" };
            let abs_val = val.abs();
            let scale = usize::try_from(*scale)?; // Convert scale to usize

            let str_val = abs_val.to_string(); // Convert the absolute value to a string

            // Split the string into integer and fractional parts
            let len = str_val.len();
            let (int_part, frac_part) = if len > scale {
                let (a, b) = str_val.split_at(len - scale);
                (a.to_string(), b.to_string())
            } else {
                ("0".to_string(), format!("{str_val:0>scale$}"))
            };

            if frac_part.is_empty() {
                Ok(Some(format!("{sign}{int_part}")))
            } else {
                Ok(Some(format!("{sign}{int_part}.{frac_part}")))
            }
        }

        DataType::Decimal256(_, scale) => {
            let val = array
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .ok_or_else(|| anyhow!("Failed to downcast Decimal256 array"))?
                .value(index);

            // `i256::to_string()` renders the full signed integer; split it into
            // integer/fractional parts by the declared scale, mirroring the
            // Decimal128 arm. Working from the string sidesteps i256 abs/compare
            // APIs and preserves the exact digits. A `MySQL` `SUM(..)` widens to
            // DECIMAL(65, s) -> Arrow Decimal256(76, s); this renders the same
            // string the Int64/Decimal128 side produces, so the values compare
            // equal (scale 0 -> integer string; scale s -> s fractional digits).
            let str_signed = val.to_string();
            let sign = if str_signed.starts_with('-') { "-" } else { "" };
            let abs_str = str_signed.strip_prefix('-').unwrap_or(&str_signed);
            let scale = usize::try_from(*scale)?;

            let len = abs_str.len();
            let (int_part, frac_part) = if len > scale {
                let (a, b) = abs_str.split_at(len - scale);
                (a.to_string(), b.to_string())
            } else {
                ("0".to_string(), format!("{abs_str:0>scale$}"))
            };

            if frac_part.is_empty() {
                Ok(Some(format!("{sign}{int_part}")))
            } else {
                Ok(Some(format!("{sign}{int_part}.{frac_part}")))
            }
        }

        DataType::Timestamp(unit, _) => match unit {
            TimeUnit::Second => {
                let ts = array
                    .as_any()
                    .downcast_ref::<TimestampSecondArray>()
                    .ok_or_else(|| anyhow!("Failed to downcast TimestampSecondArray"))?
                    .value(index);
                let dt = DateTime::from_timestamp(ts, 0)
                    .ok_or_else(|| anyhow!("Invalid timestamp for seconds={ts}"))?;
                Ok(Some(dt.format("%Y-%m-%d %H:%M:%S").to_string()))
            }
            TimeUnit::Millisecond => {
                let ts = array
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .ok_or_else(|| anyhow!("Failed to downcast TimestampMillisecondArray"))?
                    .value(index);
                let secs = ts / 1000;
                let sub_ms = ts.rem_euclid(1000);
                let sub_u32 = u32::try_from(sub_ms)?;
                let nanos = sub_u32 * 1_000_000;
                let dt = DateTime::from_timestamp(secs, nanos)
                    .ok_or_else(|| anyhow!("Invalid timestamp"))?;
                Ok(Some(dt.format("%Y-%m-%d %H:%M:%S%.3f").to_string()))
            }
            TimeUnit::Microsecond => {
                downcast_and_stringify_ts!(
                    array,
                    index,
                    TimestampMicrosecondArray,
                    1_000_000,
                    "%Y-%m-%d %H:%M:%S%.6f"
                )
            }
            TimeUnit::Nanosecond => {
                downcast_and_stringify_ts!(
                    array,
                    index,
                    TimestampNanosecondArray,
                    1_000_000_000,
                    "%Y-%m-%d %H:%M:%S%.9f"
                )
            }
        },

        dt => Err(anyhow::anyhow!(
            "Unsupported data type for validation: {dt:?}",
        )),
    }
}

/// Renders a text cell with every backslash doubled.
///
/// The doubling keeps text and bytes apart: a binary cell that is not valid UTF-8
/// renders as `\x` and hex digits, a form no rendered text can take once each of
/// its backslashes is doubled, so the text `\xff` never equals the byte `0xff`.
fn text_to_string(text: &str) -> String {
    text.replace('\\', "\\\\")
}

/// Renders a binary cell as the text its bytes spell, rendered by [`text_to_string`],
/// so a column read as bytes compares equal to the same values read as a string:
/// `ClickBench` stores its text columns as parquet `BINARY`. Bytes that are not valid
/// UTF-8 render as `\x` followed by their hex digits instead.
fn bytes_to_string(bytes: &[u8]) -> String {
    const HEX_DIGITS: &[u8; 16] = b"0123456789abcdef";
    if let Ok(text) = std::str::from_utf8(bytes) {
        return text_to_string(text);
    }
    let mut hex = String::with_capacity(2 + 2 * bytes.len());
    hex.push_str("\\x");
    for byte in bytes {
        hex.push(char::from(HEX_DIGITS[usize::from(byte >> 4)]));
        hex.push(char::from(HEX_DIGITS[usize::from(byte & 0x0f)]));
    }
    hex
}

/// The largest relative difference at which two numeric cells still compare equal.
///
/// Engines legitimately disagree in the last digits of the same answer — a float
/// `avg()` summed in a different partition order — so the comparison cannot be
/// exact. 0.1% absorbs that while failing an answer that is wrong by more. A
/// decimal rounded by one engine and truncated by another also gets one unit of
/// slack in a fine last place; see `numeric_strings_match`.
pub const NUMERIC_RELATIVE_TOLERANCE: f64 = 0.001;

/// The fewest decimal places at which one unit in the last place can be rounding.
/// A decimal average or ratio carries several places; an amount written to two is
/// a stored value, and one unit there is a different value.
const ROUNDED_PLACES_MIN: usize = 4;

/// The largest relative difference that one unit of last-place rounding may make.
const ROUNDED_RELATIVE_TOLERANCE: f64 = 0.01;

/// Whether two rendered numeric cells hold the same answer.
///
/// They do when they differ by at most [`NUMERIC_RELATIVE_TOLERANCE`] of the first,
/// or when they differ only in how their last decimal place was rounded: both are
/// written to the same number of places, at least [`ROUNDED_PLACES_MIN`], one unit
/// apart in the last, and within [`ROUNDED_RELATIVE_TOLERANCE`] of each other. That
/// is where an engine that rounds a decimal result and one that truncates it part
/// ways — TPC-DS Q53's `224.796667` against `224.796666` — and for a value as small
/// as Q98's `revenueratio` of `0.000812`, one unit is more than 0.1%. A coarser last
/// place is a different value (`0.1` against `0.0`), and so is a unit that is most
/// of a tiny value. An infinity or NaN matches only the same infinity or NaN.
fn numeric_strings_match(expected: &str, actual: &str) -> bool {
    let (Ok(expected_num), Ok(actual_num)) = (expected.parse::<f64>(), actual.parse::<f64>())
    else {
        return false;
    };
    if !expected_num.is_finite() || !actual_num.is_finite() {
        return (expected_num.is_nan() && actual_num.is_nan())
            || (expected_num.is_infinite()
                && actual_num.is_infinite()
                && expected_num.is_sign_positive() == actual_num.is_sign_positive());
    }
    let diff = (expected_num - actual_num).abs();
    if diff <= (expected_num.abs() * NUMERIC_RELATIVE_TOLERANCE).max(1e-12) {
        return true;
    }
    match (decimal_places(expected), decimal_places(actual)) {
        (Some(places), Some(actual_places))
            if places == actual_places && places >= ROUNDED_PLACES_MIN =>
        {
            let Ok(exponent) = i32::try_from(places) else {
                return false;
            };
            // `f64` leaves `0.000813 - 0.000812` a hair over one unit.
            diff <= 10_f64.powi(-exponent) * (1.0 + 1e-9)
                && diff <= expected_num.abs().max(actual_num.abs()) * ROUNDED_RELATIVE_TOLERANCE
        }
        _ => false,
    }
}

/// The digits after the decimal point of a plainly written number, or `None` for
/// one in exponent notation, whose last written digit marks no decimal place.
fn decimal_places(value: &str) -> Option<usize> {
    if value.contains(['e', 'E']) {
        return None;
    }
    Some(
        value
            .split_once('.')
            .map_or(0, |(_, fraction)| fraction.len()),
    )
}

pub fn validate_batches_as_strings(
    expected: &RecordBatch,
    actual: &RecordBatch,
) -> Result<QueryValidationResult> {
    let schema = expected.schema();

    for (i, field) in schema.fields().iter().enumerate() {
        let column_name = field.name().clone();
        let data_type = field.data_type();
        let expected_array = expected.column(i).as_ref();
        let actual_array = actual.column(i).as_ref();

        if expected_array.len() != actual_array.len() {
            return Ok(QueryValidationResult::Fail(
                QueryValidationFailReason::ColumnLengthMismatch {
                    column_name,
                    left_len: expected_array.len(),
                    right_len: actual_array.len(),
                },
            ));
        }

        for row in 0..expected_array.len() {
            let expected_val = array_value_to_string(expected_array, row)?;
            let actual_val = array_value_to_string(actual_array, row)?;
            if cells_match(
                expected_val.as_deref(),
                actual_val.as_deref(),
                data_type,
                actual_array.data_type(),
            ) {
                continue;
            }
            return Ok(QueryValidationResult::Fail(
                QueryValidationFailReason::DataMismatch {
                    column: column_name,
                    row_number: row + 1, // indexes are 0-based, counts are 1-based
                    expected: expected_val
                        .map_or_else(|| "None".to_string(), |val| format!("{val:?}")),
                    actual: actual_val.map_or_else(|| "None".to_string(), |val| format!("{val:?}")),
                },
            ));
        }
    }

    Ok(QueryValidationResult::Pass)
}

/// Whether two rendered cells hold the same value, by the rule every comparison
/// here uses: the same text, numbers that [`numeric_strings_match`], timestamps
/// that differ only in trailing fractional-second zeros (nanosecond and
/// microsecond engines pad differently), or a date and a timestamp at its
/// midnight. Rendered cells no longer carry their type, so the numeric rule
/// applies only to a numeric expected column, and the midnight rule only when one
/// column is a date and the other a timestamp.
fn cells_match(
    expected: Option<&str>,
    actual: Option<&str>,
    expected_type: &DataType,
    actual_type: &DataType,
) -> bool {
    match (expected, actual) {
        (None, None) => true,
        (Some(expected), Some(actual)) => {
            expected == actual
                || (expected_type.is_numeric() && numeric_strings_match(expected, actual))
                || timestamp_strings_equivalent(expected, actual)
                || (is_date_and_timestamp_pair(expected_type, actual_type)
                    && date_and_midnight_timestamp_equivalent(expected, actual))
        }
        (Some(_), None) | (None, Some(_)) => false,
    }
}

pub fn validate_tpch_query(
    query: &Query,
    batches: &[RecordBatch],
) -> Result<QueryValidationResult> {
    let Some(expected_batches) = TPCH_ANSWERS.get(&query.name) else {
        return Ok(QueryValidationResult::Fail(
            QueryValidationFailReason::NoExpectedAnswer,
        ));
    };

    match (expected_batches.is_empty(), batches.is_empty()) {
        (true, true) | (false, false) => {}
        (true, false) => {
            return Ok(QueryValidationResult::Fail(
                QueryValidationFailReason::NoExpectedAnswer,
            ));
        }
        _ => {
            return Ok(QueryValidationResult::Fail(
                QueryValidationFailReason::NoAnswer,
            ));
        }
    }

    let Some(expected_schema) = expected_batches
        .first()
        .map(arrow::array::RecordBatch::schema)
    else {
        return Ok(QueryValidationResult::Fail(
            QueryValidationFailReason::NoAnswer,
        ));
    };
    let Some(actual_schema) = batches.first().map(arrow::array::RecordBatch::schema) else {
        return Ok(QueryValidationResult::Fail(
            QueryValidationFailReason::NoAnswer,
        ));
    };

    if !equivalent_schemas(&expected_schema, &actual_schema) {
        if let Some(diff) = schema_difference(&expected_schema, &actual_schema) {
            println!("Schema mismatch:\n{diff}");
        } else {
            println!("expected_schema: {expected_schema:?}");
            println!("actual_schema: {actual_schema:?}");
        }

        return Ok(QueryValidationResult::Fail(
            QueryValidationFailReason::SchemaMismatch,
        ));
    }

    // combine all expected batches and all actual batches into a single RecordBatch
    let expected_batches = arrow::compute::concat_batches(&expected_schema, expected_batches)?;
    let actual_batches = arrow::compute::concat_batches(&actual_schema, batches)?;

    // check the row counts are equal
    if expected_batches.num_rows() != actual_batches.num_rows() {
        return Ok(QueryValidationResult::Fail(
            QueryValidationFailReason::RowCountMismatch {
                expected: expected_batches.num_rows(),
                actual: actual_batches.num_rows(),
            },
        ));
    }

    // check the actual data batches are equal
    validate_batches_as_strings(&expected_batches, &actual_batches)
}

pub fn validate_tpch_query_at_scale(
    query: &Query,
    batches: &[RecordBatch],
    scale_factor: f64,
) -> Result<QueryValidationResult> {
    if has_static_tpch_answer(query)
        && !should_validate_with_static_tpch_answer(query, scale_factor)
    {
        // A static answer exists, but only at scale factor 1.0. Report this
        // distinctly from `NoExpectedAnswer` so callers can tell the query has a
        // known SF=1 answer and that validating at this scale factor needs a
        // reference schema instead.
        return Ok(QueryValidationResult::Fail(
            QueryValidationFailReason::NoExpectedAnswerAtScaleFactor,
        ));
    }

    validate_tpch_query(query, batches)
}

/// Validate a query against expected results from a custom query set
/// This is a generic validation function that can be used for custom queries
pub fn validate_with_expected_batches(
    query_name: &str,
    actual_batches: &[RecordBatch],
    expected_batches: &[RecordBatch],
) -> Result<QueryValidationResult> {
    if expected_batches.is_empty() && actual_batches.is_empty() {
        return Ok(QueryValidationResult::Pass);
    }

    if expected_batches.is_empty() {
        return Ok(QueryValidationResult::Fail(
            QueryValidationFailReason::NoExpectedAnswer,
        ));
    }

    if actual_batches.is_empty() {
        return Ok(QueryValidationResult::Fail(
            QueryValidationFailReason::NoAnswer,
        ));
    }

    let Some(expected_schema) = expected_batches
        .first()
        .map(arrow::array::RecordBatch::schema)
    else {
        return Ok(QueryValidationResult::Fail(
            QueryValidationFailReason::NoAnswer,
        ));
    };

    let Some(actual_schema) = actual_batches
        .first()
        .map(arrow::array::RecordBatch::schema)
    else {
        return Ok(QueryValidationResult::Fail(
            QueryValidationFailReason::NoAnswer,
        ));
    };

    if !equivalent_schemas(&expected_schema, &actual_schema) {
        println!("Query '{query_name}' schema mismatch:");
        if let Some(diff) = schema_difference(&expected_schema, &actual_schema) {
            println!("{diff}");
        } else {
            println!("  expected_schema: {expected_schema:?}");
            println!("  actual_schema: {actual_schema:?}");
        }

        return Ok(QueryValidationResult::Fail(
            QueryValidationFailReason::SchemaMismatch,
        ));
    }

    // combine all expected batches and all actual batches into a single RecordBatch
    let expected_batches = arrow::compute::concat_batches(&expected_schema, expected_batches)?;
    let actual_batches = arrow::compute::concat_batches(&actual_schema, actual_batches)?;

    // check the row counts are equal
    if expected_batches.num_rows() != actual_batches.num_rows() {
        println!("Query '{query_name}' row count mismatch:");
        println!("  expected: {}", expected_batches.num_rows());
        println!("  actual: {}", actual_batches.num_rows());

        return Ok(QueryValidationResult::Fail(
            QueryValidationFailReason::RowCountMismatch {
                expected: expected_batches.num_rows(),
                actual: actual_batches.num_rows(),
            },
        ));
    }

    validate_batches_as_strings(&expected_batches, &actual_batches)
}

/// Validate that actual batches have the expected row count
pub fn validate_row_count(
    query_name: &str,
    actual_batches: &[RecordBatch],
    expected_row_count: usize,
) -> Result<QueryValidationResult> {
    let actual_row_count: usize = actual_batches.iter().map(RecordBatch::num_rows).sum();

    if actual_row_count == expected_row_count {
        Ok(QueryValidationResult::Pass)
    } else {
        println!("Query '{query_name}' row count mismatch:");
        println!("  expected: {expected_row_count}");
        println!("  actual: {actual_row_count}");

        Ok(QueryValidationResult::Fail(
            QueryValidationFailReason::RowCountMismatch {
                expected: expected_row_count,
                actual: actual_row_count,
            },
        ))
    }
}

/// True when `s` matches `shape`, where `#` marks a digit and every other byte
/// is a literal at that offset.
fn matches_shape(s: &str, shape: &[u8]) -> bool {
    s.len() == shape.len()
        && s.bytes().zip(shape).all(|(c, &want)| {
            if want == b'#' {
                c.is_ascii_digit()
            } else {
                c == want
            }
        })
}

/// True when one string is a plain date and the other is that date at midnight:
/// the answer set's `1995-03-05` against the `1995-03-05 00:00:00` an engine
/// whose `DATE` carries a time component (Oracle) returns.
///
/// Only midnight matches. A non-midnight time is a real difference in the data.
fn date_and_midnight_timestamp_equivalent(a: &str, b: &str) -> bool {
    /// Exactly `YYYY-MM-DD`, the form [`array_value_to_string`] emits for a date.
    fn is_date(s: &str) -> bool {
        matches_shape(s, b"####-##-##")
    }

    /// The date part of a `YYYY-MM-DD 00:00:00` timestamp, fractional second
    /// permitted only if zero. `None` for anything else.
    fn midnight_date(s: &str) -> Option<&str> {
        let (date, time) = s.split_once(' ')?;
        if !is_date(date) {
            return None;
        }
        let fraction = time.strip_prefix("00:00:00")?;
        let fraction_is_zero = match fraction.strip_prefix('.') {
            Some(digits) => !digits.is_empty() && digits.bytes().all(|c| c == b'0'),
            None => fraction.is_empty(),
        };
        fraction_is_zero.then_some(date)
    }

    match (is_date(a), is_date(b)) {
        (true, false) => midnight_date(b) == Some(a),
        (false, true) => midnight_date(a) == Some(b),
        _ => false,
    }
}

/// True when both strings are timestamps in the format [`array_value_to_string`]
/// emits and differ only by fractional-second zero padding — a nanosecond engine's
/// `2024-01-01 00:00:00.000000000` against a microsecond engine's
/// `2024-01-01 00:00:00.000000`.
///
/// The shape test is deliberately exact rather than a "contains `.` and `:`"
/// heuristic: a loose guard also matches values such as `http://host/a.100`, where
/// trimming trailing zeros would silently mask a real mismatch. Anything that is
/// not the emitted timestamp format returns `false`, so the caller falls through
/// to reporting the mismatch.
fn timestamp_strings_equivalent(a: &str, b: &str) -> bool {
    /// Exactly `YYYY-MM-DD HH:MM:SS`, the prefix [`array_value_to_string`] emits
    /// for every `Timestamp` unit.
    fn is_timestamp_prefix(s: &str) -> bool {
        matches_shape(s, b"####-##-## ##:##:##")
    }

    /// Splits into the `YYYY-MM-DD HH:MM:SS` prefix and its fractional digits with
    /// trailing zeros trimmed. `None` when `s` is not the emitted format.
    fn split_timestamp(s: &str) -> Option<(&str, &str)> {
        match s.split_once('.') {
            Some((prefix, frac)) => {
                let frac_is_digits = !frac.is_empty() && frac.bytes().all(|c| c.is_ascii_digit());
                (is_timestamp_prefix(prefix) && frac_is_digits)
                    .then(|| (prefix, frac.trim_end_matches('0')))
            }
            None => is_timestamp_prefix(s).then_some((s, "")),
        }
    }

    match (split_timestamp(a), split_timestamp(b)) {
        (Some(left), Some(right)) => left == right,
        _ => false,
    }
}

/// How rows should be compared when validating two independent query results.
///
/// SQL without `ORDER BY` does not define row order, so engines may return the
/// same multiset of rows in different orders. [`RowOrder::Multiset`] sorts both
/// sides into a canonical order before cell-by-cell comparison.
/// [`RowOrder::Preserved`] requires identical row order (use when the SQL has
/// an explicit `ORDER BY` that both engines honor).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RowOrder {
    /// Sort both results lexicographically by all columns, then compare.
    #[default]
    Multiset,
    /// Compare rows in the order returned (for `ORDER BY` queries).
    Preserved,
}

/// Infer [`RowOrder`] from SQL text: presence of `ORDER BY` (case-insensitive)
/// means preserved order; otherwise multiset. Comments are not stripped — the
/// inventory queries do not put `ORDER BY` only inside comments.
#[must_use]
pub fn row_order_from_sql(sql: &str) -> RowOrder {
    let upper = sql.to_ascii_uppercase();
    if upper.contains("ORDER BY") {
        RowOrder::Preserved
    } else {
        RowOrder::Multiset
    }
}

/// Full-content equality of two independent result sets (schema + cell values).
///
/// This is the engine-vs-engine parity path: both sides are treated as "actual"
/// answers for the same SQL on the same data. Numeric comparison reuses the
/// relative tolerance, [`NUMERIC_RELATIVE_TOLERANCE`], in [`validate_batches_as_strings`].
///
/// When `row_order` is [`RowOrder::Multiset`], both sides are concatenated and
/// sorted into a canonical order so differing physical scan orders do not
/// produce false mismatches — which means this function says **nothing about the
/// order an engine returned rows in**. Engine-parity callers want
/// [`compare_query_result_batches_with_sort_check`], which adds that check; this
/// one is the content half, kept separate so its own behavior stays testable.
pub fn compare_query_result_batches(
    query_name: &str,
    left_batches: &[RecordBatch],
    right_batches: &[RecordBatch],
    row_order: RowOrder,
) -> Result<QueryValidationResult> {
    if left_batches.is_empty() && right_batches.is_empty() {
        return Ok(QueryValidationResult::Pass);
    }

    if left_batches.is_empty() {
        return Ok(QueryValidationResult::Fail(
            QueryValidationFailReason::NoAnswer,
        ));
    }

    if right_batches.is_empty() {
        return Ok(QueryValidationResult::Fail(
            QueryValidationFailReason::NoExpectedAnswer,
        ));
    }

    let Some(left_schema) = left_batches.first().map(RecordBatch::schema) else {
        return Ok(QueryValidationResult::Fail(
            QueryValidationFailReason::NoAnswer,
        ));
    };
    let Some(right_schema) = right_batches.first().map(RecordBatch::schema) else {
        return Ok(QueryValidationResult::Fail(
            QueryValidationFailReason::NoExpectedAnswer,
        ));
    };

    // Engine-vs-engine parity cares about cell values, not Arrow physical types
    // (Utf8View vs Utf8, Decimal128 vs Float64, qualified vs bare aggregate
    // names). Require the same arity; stringified comparison below absorbs type
    // representation differences the way `validate_batches_as_strings` already
    // does for TPCH CSV answers.
    if left_schema.fields().len() != right_schema.fields().len() {
        println!("Query '{query_name}' schema arity mismatch (left vs right):");
        if let Some(diff) = schema_difference(&left_schema, &right_schema) {
            println!("{diff}");
        } else {
            println!("  left_schema: {left_schema:?}");
            println!("  right_schema: {right_schema:?}");
        }
        return Ok(QueryValidationResult::Fail(
            QueryValidationFailReason::SchemaMismatch,
        ));
    }
    if !equivalent_schemas(&left_schema, &right_schema) {
        // Log but continue — positional string compare still validates content.
        if let Some(diff) = schema_difference(&left_schema, &right_schema) {
            println!(
                "Query '{query_name}' logical schema differs (continuing value compare):\n{diff}"
            );
        }
    }

    let mut left = arrow::compute::concat_batches(&left_schema, left_batches)?;
    let mut right = arrow::compute::concat_batches(&right_schema, right_batches)?;

    if left.num_rows() != right.num_rows() {
        println!("Query '{query_name}' row count mismatch:");
        println!("  left: {}", left.num_rows());
        println!("  right: {}", right.num_rows());
        return Ok(QueryValidationResult::Fail(
            QueryValidationFailReason::RowCountMismatch {
                expected: left.num_rows(),
                actual: right.num_rows(),
            },
        ));
    }

    if row_order == RowOrder::Multiset {
        left = sort_batch_lexicographic_as_strings(&left)?;
        right = sort_batch_lexicographic_as_strings(&right)?;
    }

    // `validate_batches_as_strings` compares expected (first arg) to actual
    // (second). For engine-vs-engine parity the labels are arbitrary; left is
    // treated as the reference side in mismatch messages.
    let result = validate_batches_as_strings(&left, &right)?;
    if let QueryValidationResult::Fail(ref reason) = result {
        println!("Query '{query_name}' content mismatch: {reason:?}");
    }
    Ok(result)
}

/// Content equality **plus** a per-side check that each engine honored the
/// query's own top-level `ORDER BY`.
///
/// [`compare_query_result_batches`] under [`RowOrder::Multiset`] canonically
/// sorts both sides before comparing, so it establishes that the two engines
/// returned the same rows and nothing about the order they returned them in.
/// Most of the suite corpus sorts without a `LIMIT` and is compared that way, so
/// without this check an engine whose sort is wrong compares equal.
///
/// A side that breaks its own `ORDER BY` is reported as that, in preference to
/// the content difference it also causes. Under [`RowOrder::Preserved`] —
/// `ORDER BY … LIMIT`, where the set of rows depends on the order — the same
/// rows in the wrong order fail as a content mismatch, which says the two
/// results differ without naming the side that is wrong on its own terms. A
/// caller adjudicating a disagreement between engines needs that distinction:
/// a content difference may come from dialect or arithmetic, a sort violation
/// cannot. When no side violates, the content result stands.
///
/// A tie under the sort key is never a violation, so the check adds no
/// sensitivity to the engine-dependent ordering of equal rows that
/// [`RowOrder::Multiset`] exists to absorb.
///
/// **A sort check that could not run is returned, not swallowed.** Whatever the
/// check could not cover lands in [`SortCheckedComparison::unchecked`], because
/// a hole that reads as a pass is the failure this check exists to remove. A
/// caller that ignores that field is back to reporting unverified order as
/// verified. The field is populated even when the content comparison failed, for
/// the caller that recovers from that failure and would otherwise pass an order
/// nothing verified.
///
/// `sql` must be the statement that produced *both* result sets. Where the two
/// engines run textually different SQL, pass the form whose projection matches
/// the compared results.
///
/// # Errors
/// Returns an error if the batches cannot be concatenated or compared.
pub fn compare_query_result_batches_with_sort_check(
    query_name: &str,
    sql: &str,
    left_batches: &[RecordBatch],
    right_batches: &[RecordBatch],
    row_order: RowOrder,
) -> Result<SortCheckedComparison> {
    let content = if row_order == RowOrder::Preserved {
        compare_limit_results_allowing_cutoff_ties(query_name, sql, left_batches, right_batches)?
    } else {
        compare_query_result_batches(query_name, left_batches, right_batches, row_order)?
    };
    let content_passed = content == QueryValidationResult::Pass;

    // Parsed once for both sides: the AST does not depend on which engine's rows
    // are being checked, and the corpus carries multi-kilobyte statements.
    let statement = sort_order::parse_one_statement(sql);
    let mut unchecked = Vec::new();

    for (side, batches) in [("left", left_batches), ("right", right_batches)] {
        let Some(schema) = batches.first().map(RecordBatch::schema) else {
            continue;
        };
        let batch = arrow::compute::concat_batches(&schema, batches)?;
        match sort_order::check_sort_order_parsed(statement.as_ref(), &batch)? {
            SortCheck::Ordered => {}
            SortCheck::PartiallyOrdered { unchecked: reason } | SortCheck::Skipped { reason } => {
                println!("Query '{query_name}' ({side}) sort order unchecked: {reason}");
                unchecked.push(format!("{side}: {reason}"));
            }
            SortCheck::Violation(v) => {
                println!(
                    "Query '{query_name}' ({side}) violates its own ORDER BY on column '{}' at row {}: {} then {}",
                    v.column, v.row_number, v.previous, v.current
                );
                return Ok(SortCheckedComparison {
                    result: QueryValidationResult::Fail(
                        QueryValidationFailReason::SortOrderViolation {
                            side: side.to_string(),
                            violation: v,
                        },
                    ),
                    unchecked,
                });
            }
        }
    }

    if !content_passed {
        // The hole is carried even though the comparison failed, because not
        // every caller treats that failure as final: one that recovers from it
        // — the chDB lane retries a schema mismatch as string rows — would
        // otherwise turn an order that was never verified into a clean pass.
        return Ok(SortCheckedComparison {
            result: content,
            unchecked,
        });
    }

    Ok(SortCheckedComparison {
        result: QueryValidationResult::Pass,
        unchecked,
    })
}

/// Compare an under-test result to a live reference-schema result.
///
/// This is the `--validate` path for query sets that have no static answer
/// files (TPC-DS, and TPC-H at scale factors other than 1): both sides are
/// treated as engine answers for the same SQL on the same data. Row order
/// follows the Cayenne correctness suite: positional equality only when the
/// row set itself depends on order (top-level `ORDER BY` + `LIMIT`); otherwise
/// a multiset compare so scan order cannot produce a false mismatch. A side
/// that violates its own `ORDER BY` still fails.
///
/// An `ORDER BY` the sort check could not fully verify is not a failure here —
/// the rows were still compared — except an `ORDER BY … LIMIT` on something the
/// result does not return: that answer must match the reference row by row, and a
/// mismatch is left for [`validate_against_keyed_reference`], which has the sort
/// keys. A mismatch in any other `ORDER BY … LIMIT` answer can be left for it too,
/// because the row at a `LIMIT` or `OFFSET` cutoff may be one of several that tie
/// there. Callers that need to count that hole should use
/// [`compare_query_result_batches_with_sort_check`] directly.
///
/// # Errors
/// Returns an error if the batches cannot be concatenated or compared.
pub fn validate_against_reference_batches(
    query: &Query,
    actual: &[RecordBatch],
    reference: &[RecordBatch],
) -> Result<QueryValidationResult> {
    let order = if has_top_level_order_by(&query.sql) && has_top_level_limit(&query.sql) {
        RowOrder::Preserved
    } else {
        RowOrder::Multiset
    };
    let comparison = compare_query_result_batches_with_sort_check(
        &query.name,
        &query.sql,
        actual,
        reference,
        order,
    )?;
    // Rows cannot show an order decided by a sort key they do not return, so a
    // match that ignored order proves nothing about it.
    if comparison.result == QueryValidationResult::Pass
        && let Some(schema) = actual.first().map(RecordBatch::schema)
        && unprojected_sort_limit(&query.sql, &schema).is_some()
    {
        return compare_query_result_batches(&query.name, actual, reference, RowOrder::Preserved);
    }
    Ok(comparison.result)
}

/// Checks an answer to a query with an [`UnorderedLimit`] against the full result
/// its `LIMIT` was taken from.
///
/// Two engines' answers to such a query cannot be compared directly, because SQL
/// lets each keep different rows: `ClickBench` Q18 (`GROUP BY … LIMIT 10` with no
/// `ORDER BY`) returns different groups from `DuckDB` at 1, 2, 8 and 16 threads
/// over the same file. [`unordered_limit`] only accepts a grouped query that returns
/// its group keys, so every row names its group, and an answer is correct when it
/// has as many rows as the full result leaves after the `OFFSET`, up to the `LIMIT`,
/// and every row it returned is a row of the full result, counted as a multiset.
/// `OFFSET` is applied to each engine's own unspecified order, so a row the
/// reference stream would skip is still eligible.
///
/// Cells render through [`array_value_to_string`] like every comparison here but
/// must match exactly, with no numeric tolerance, so a returned row passes only if
/// the full result holds that exact row. The full result arrives one batch at a
/// time through [`Self::observe`] and only the returned rows are kept, so memory
/// stays bounded by the `LIMIT` however large the full result is.
pub struct UnorderedLimitSubsetCheck {
    limit: usize,
    offset: usize,
    /// The returned rows, in the order they were returned.
    returned: Vec<Vec<Option<String>>>,
    /// Per distinct returned row: how many times it was returned, and how many
    /// copies of it the full result has shown so far, capped at the former.
    copies: HashMap<Vec<Option<String>>, (usize, usize)>,
    /// First-column values of the returned rows, so a full-result row that cannot
    /// match is skipped without rendering the rest of it.
    first_column_values: HashSet<Option<String>>,
    full_result_rows: usize,
    schema_mismatch: bool,
}

impl UnorderedLimitSubsetCheck {
    /// # Errors
    /// Returns an error if a cell of a returned row cannot be rendered.
    pub fn new(unordered_limit: &UnorderedLimit, returned: &[RecordBatch]) -> Result<Self> {
        let mut rows = Vec::new();
        for batch in returned {
            for row in 0..batch.num_rows() {
                rows.push(row_as_strings(batch, row)?);
            }
        }
        let mut copies = HashMap::new();
        for row in &rows {
            copies.entry(row.clone()).or_insert((0, 0)).0 += 1;
        }
        let first_column_values = rows.iter().filter_map(|row| row.first().cloned()).collect();
        Ok(Self {
            limit: unordered_limit.limit,
            offset: unordered_limit.offset,
            returned: rows,
            copies,
            first_column_values,
            full_result_rows: 0,
            schema_mismatch: false,
        })
    }

    /// Matches one batch of the full result against the returned rows.
    ///
    /// Every full-result row may update `copies`. `OFFSET` is applied to each
    /// engine's own unspecified stream, so a row this reference stream would skip
    /// can still be returned (for example `LIMIT 1 OFFSET 1` over `[a, b, c]`
    /// may return `a` from stream `[c, a, b]`). [`Self::finish`] still sizes the
    /// answer from `offset` and `limit`.
    ///
    /// # Errors
    /// Returns an error if a cell of `batch` cannot be rendered.
    pub fn observe(&mut self, batch: &RecordBatch) -> Result<()> {
        self.full_result_rows += batch.num_rows();
        let Some(width) = self.returned.first().map(Vec::len) else {
            return Ok(());
        };
        if batch.num_columns() != width {
            self.schema_mismatch = true;
            return Ok(());
        }
        let first_column = batch.column(0).as_ref();
        for row in 0..batch.num_rows() {
            if !self
                .first_column_values
                .contains(&array_value_to_string(first_column, row)?)
            {
                continue;
            }
            if let Some((times_returned, times_seen)) =
                self.copies.get_mut(&row_as_strings(batch, row)?)
                && *times_seen < *times_returned
            {
                *times_seen += 1;
            }
        }
        Ok(())
    }

    /// The verdict, once every batch of the full result has been observed.
    #[must_use]
    pub fn finish(mut self) -> QueryValidationResult {
        if self.schema_mismatch {
            return QueryValidationResult::Fail(QueryValidationFailReason::SchemaMismatch);
        }
        let expected_rows = self
            .full_result_rows
            .saturating_sub(self.offset)
            .min(self.limit);
        if self.returned.len() != expected_rows {
            return QueryValidationResult::Fail(QueryValidationFailReason::RowCountMismatch {
                expected: expected_rows,
                actual: self.returned.len(),
            });
        }
        for (index, row) in self.returned.iter().enumerate() {
            match self.copies.get_mut(row) {
                Some((_, times_seen)) if *times_seen > 0 => *times_seen -= 1,
                _ => {
                    return QueryValidationResult::Fail(
                        QueryValidationFailReason::RowNotAllowedByLimit {
                            row_number: index + 1,
                            row: format!("{row:?}"),
                        },
                    );
                }
            }
        }
        QueryValidationResult::Pass
    }
}

fn row_as_strings(batch: &RecordBatch, row: usize) -> Result<Vec<Option<String>>> {
    batch
        .columns()
        .iter()
        .map(|column| array_value_to_string(column.as_ref(), row))
        .collect()
}

/// The sort key of row `row_index` of `keyed_reference`, counted across its
/// batches, or `None` when there is no such row or it has no cells where `key`
/// says.
fn keyed_row_key(
    keyed_reference: &[RecordBatch],
    row_index: usize,
    key: &SortKeyCells,
) -> Result<Option<Vec<Option<String>>>> {
    let mut remaining = row_index;
    for batch in keyed_reference {
        if remaining < batch.num_rows() {
            let cells = row_as_strings(batch, remaining)?;
            return Ok(match key {
                SortKeyCells::Appended(key_columns) => cells
                    .len()
                    .checked_sub(*key_columns)
                    .map(|width| cells[width..].to_vec()),
                SortKeyCells::Returned(key_indexes) => key_indexes
                    .iter()
                    .map(|index| cells.get(*index).cloned())
                    .collect(),
            });
        }
        remaining -= batch.num_rows();
    }
    Ok(None)
}

/// True when `keyed_reference` already holds a row past the page, which ends
/// `offset + limit` rows down, whose sort key differs from the key at the cut:
/// the tie group the `LIMIT` cuts has ended, and later batches cannot change the
/// verdict.
///
/// # Errors
/// Returns an error if a cell cannot be rendered.
pub fn keyed_reference_cutoff_closed(
    keyed_reference: &[RecordBatch],
    limit: usize,
    offset: usize,
    key: &SortKeyCells,
) -> Result<bool> {
    let fetched: usize = keyed_reference.iter().map(RecordBatch::num_rows).sum();
    let Some(last_row) = limit.checked_sub(1) else {
        return Ok(true);
    };
    let cut = offset.saturating_add(last_row);
    if fetched <= cut {
        return Ok(false);
    }
    let Some(cut_key) = keyed_row_key(keyed_reference, cut, key)? else {
        return Ok(false);
    };
    let Some(last_key) = keyed_row_key(keyed_reference, fetched - 1, key)? else {
        return Ok(false);
    };
    Ok(cut_key != last_key)
}

/// Feeds keyed-reference batches to [`validate_against_keyed_reference`] until
/// the tie group the `LIMIT` cuts has closed or the batches end, so the rows past
/// that group never have to be held in memory. `requested_rows` is the `LIMIT`
/// the keyed query asked for; fewer rows than that means the result ended.
///
/// Returns the same `Option` as [`validate_against_keyed_reference`], plus how
/// many reference rows were consumed.
///
/// # Errors
/// Returns an error if a cell cannot be rendered.
pub fn decide_from_keyed_reference_batches(
    actual: &[RecordBatch],
    batches: impl IntoIterator<Item = RecordBatch>,
    limit: usize,
    offset: usize,
    key: &SortKeyCells,
    requested_rows: usize,
) -> Result<(Option<QueryValidationResult>, usize)> {
    let mut keyed_reference = Vec::new();
    let mut fetched_rows: usize = 0;
    for batch in batches {
        fetched_rows = fetched_rows.saturating_add(batch.num_rows());
        keyed_reference.push(batch);
        if keyed_reference_cutoff_closed(&keyed_reference, limit, offset, key)? {
            return Ok((
                validate_against_keyed_reference(
                    actual,
                    &keyed_reference,
                    limit,
                    offset,
                    key,
                    false,
                )?,
                fetched_rows,
            ));
        }
    }
    Ok((
        validate_against_keyed_reference(
            actual,
            &keyed_reference,
            limit,
            offset,
            key,
            fetched_rows < requested_rows,
        )?,
        fetched_rows,
    ))
}

/// Checks an answer to a top-level `ORDER BY … LIMIT` against the reference
/// query's leading rows read back with their sort keys, the rows
/// [`KeyedSortLimit::keyed_sql`] returns.
///
/// `keyed_reference` holds the reference rows in `ORDER BY` order from the top of
/// its result, each carrying its sort key where `key` says, and the keys split them
/// into tie groups. `actual` is the page that starts `offset` rows down. Every tie
/// group inside the page fills exactly its own positions of `actual`, in any order
/// within the group, while the positions of a tie group that the `OFFSET` or the
/// `LIMIT` cuts through may hold any of that group's rows: those are the only rows
/// SQL leaves an engine free to choose among. Cells match by the rule of every
/// comparison here, so numbers match within [`NUMERIC_RELATIVE_TOLERANCE`]. A row
/// the answer holds but in another group's positions fails as
/// [`QueryValidationFailReason::RowOutOfSortOrder`]; a row the answer cannot hold
/// fails as [`QueryValidationFailReason::RowNotAllowedByLimit`].
///
/// Returns `None` while `keyed_reference` ends inside the tie group the `LIMIT`
/// cuts through and `reached_end` is `false`: a verdict needs the whole group, so
/// the caller reads more reference rows and asks again.
///
/// # Errors
/// Returns an error if a cell cannot be rendered.
pub fn validate_against_keyed_reference(
    actual: &[RecordBatch],
    keyed_reference: &[RecordBatch],
    limit: usize,
    offset: usize,
    key: &SortKeyCells,
    reached_end: bool,
) -> Result<Option<QueryValidationResult>> {
    let mut reference = Vec::new();
    let mut reference_types = Vec::new();
    for batch in keyed_reference {
        let width = match key {
            SortKeyCells::Appended(key_columns) => batch.num_columns().checked_sub(*key_columns),
            SortKeyCells::Returned(key_indexes) => Some(batch.num_columns())
                .filter(|width| key_indexes.iter().all(|index| index < width)),
        };
        let Some(width) = width else {
            return Ok(Some(QueryValidationResult::Fail(
                QueryValidationFailReason::SchemaMismatch,
            )));
        };
        if actual
            .iter()
            .any(|actual_batch| actual_batch.num_columns() != width)
        {
            return Ok(Some(QueryValidationResult::Fail(
                QueryValidationFailReason::SchemaMismatch,
            )));
        }
        reference_types = batch
            .schema()
            .fields()
            .iter()
            .take(width)
            .map(|field| field.data_type().clone())
            .collect();
        for row in 0..batch.num_rows() {
            let mut cells = row_as_strings(batch, row)?;
            let sort_key = match key {
                SortKeyCells::Appended(_) => cells.split_off(width),
                SortKeyCells::Returned(key_indexes) => key_indexes
                    .iter()
                    .map(|index| cells[*index].clone())
                    .collect(),
            };
            reference.push((cells, sort_key));
        }
    }
    let actual_types: Vec<DataType> = actual.first().map_or_else(Vec::new, |batch| {
        batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.data_type().clone())
            .collect()
    });

    let expected_rows = if reached_end {
        reference.len().saturating_sub(offset).min(limit)
    } else {
        limit
    };
    let actual_rows: usize = actual.iter().map(RecordBatch::num_rows).sum();
    let Some(last_row) = expected_rows.checked_sub(1) else {
        return Ok(Some(if actual_rows == 0 {
            QueryValidationResult::Pass
        } else {
            QueryValidationResult::Fail(QueryValidationFailReason::RowCountMismatch {
                expected: 0,
                actual: actual_rows,
            })
        }));
    };
    // Reference rows count from the top of the result, so the page is rows
    // `offset..page_end` and `cut` is its last row.
    let cut = offset.saturating_add(last_row);
    let page_end = cut.saturating_add(1);
    let Some((_, cut_key)) = reference.get(cut) else {
        return Ok(None);
    };
    let group_start = reference[..cut]
        .iter()
        .rposition(|(_, row_key)| row_key != cut_key)
        .map_or(0, |index| index + 1);
    let group_end = reference[cut..]
        .iter()
        .position(|(_, row_key)| row_key != cut_key)
        .map_or(reference.len(), |rows| cut + rows);
    if group_end == reference.len() && !reached_end {
        return Ok(None);
    }
    if actual_rows != expected_rows {
        return Ok(Some(QueryValidationResult::Fail(
            QueryValidationFailReason::RowCountMismatch {
                expected: expected_rows,
                actual: actual_rows,
            },
        )));
    }

    let mut answer = Vec::with_capacity(actual_rows);
    for batch in actual {
        for row in 0..batch.num_rows() {
            answer.push(row_as_strings(batch, row)?);
        }
    }

    // The tie group of the page's first row starts above the page when the `OFFSET`
    // cuts through it, and the rows it skipped were the engine's to choose too.
    let first_key = &reference[offset].1;
    let first_group_start = reference[..offset]
        .iter()
        .rposition(|(_, row_key)| row_key != first_key)
        .map_or(0, |index| index + 1);

    // A correct answer lists each tie group in exactly that group's positions, so
    // walk the page one tie group at a time. The positions from `group_start` on
    // belong to the tie group the `LIMIT` cuts through.
    let mut block_start = offset;
    while block_start < page_end {
        let block_key = &reference[block_start].1;
        let rows_start = if block_start == offset {
            first_group_start
        } else {
            block_start
        };
        let block_end = if block_start >= group_start {
            group_end
        } else {
            reference[block_start..group_start]
                .iter()
                .position(|(_, row_key)| row_key != block_key)
                .map_or(group_start, |rows| block_start + rows)
        };
        let mut unmatched: HashMap<&[Option<String>], usize> = HashMap::new();
        for (cells, _) in &reference[rows_start..block_end] {
            *unmatched.entry(cells.as_slice()).or_insert(0) += 1;
        }
        // Exact matches go first, so a row that matches only within the numeric
        // tolerance never takes a reference row another answer row equals.
        let mut inexact = Vec::new();
        for position in block_start..block_end.min(page_end) {
            match unmatched.get_mut(answer[position - offset].as_slice()) {
                Some(count) if *count > 0 => *count -= 1,
                _ => inexact.push(position),
            }
        }
        for position in inexact {
            let cells = answer[position - offset].as_slice();
            if let Some(count) = unmatched.iter_mut().find_map(|(reference_cells, count)| {
                (*count > 0
                    && rendered_rows_match(reference_cells, cells, &reference_types, &actual_types))
                .then_some(count)
            }) {
                *count -= 1;
                continue;
            }
            let row_index = position - offset;
            let row_number = row_index + 1; // indexes are 0-based, counts are 1-based
            let row = format!("{cells:?}");
            // A row the page may hold, not yet over its count, is in the wrong tie
            // group; any other row is one the LIMIT and OFFSET do not keep.
            let copies_allowed = reference[first_group_start..group_end]
                .iter()
                .filter(|(reference_cells, _)| {
                    rendered_rows_match(reference_cells, cells, &reference_types, &actual_types)
                })
                .count();
            let copies_returned = answer[..=row_index]
                .iter()
                .filter(|answer_cells| {
                    rendered_rows_match(answer_cells, cells, &actual_types, &actual_types)
                })
                .count();
            return Ok(Some(QueryValidationResult::Fail(
                if copies_returned <= copies_allowed {
                    QueryValidationFailReason::RowOutOfSortOrder { row_number, row }
                } else {
                    QueryValidationFailReason::RowNotAllowedByLimit { row_number, row }
                },
            )));
        }
        block_start = block_end;
    }
    Ok(Some(QueryValidationResult::Pass))
}

/// Whether two rendered rows match cell by cell, by [`cells_match`].
fn rendered_rows_match(
    expected: &[Option<String>],
    actual: &[Option<String>],
    expected_types: &[DataType],
    actual_types: &[DataType],
) -> bool {
    expected.len() == actual.len()
        && expected
            .iter()
            .zip(actual)
            .enumerate()
            .all(|(column, (expected, actual))| {
                match (expected_types.get(column), actual_types.get(column)) {
                    (Some(expected_type), Some(actual_type)) => cells_match(
                        expected.as_deref(),
                        actual.as_deref(),
                        expected_type,
                        actual_type,
                    ),
                    _ => expected == actual,
                }
            })
}

/// Compare `ORDER BY … LIMIT` results when the sort key is not unique.
///
/// SQL does not define which tied rows a `LIMIT` keeps. TPC-DS Q65 orders by
/// `s_store_name, i_item_desc`; many items share description `"A"`, so two
/// correct engines may return different 100-row subsets. Requiring positional
/// cell equality then fails even though both honor the `ORDER BY`.
///
/// Complete tie-groups (a run of equal sort keys followed by a greater key)
/// must still match as a multiset. The last run is a cutoff when the result
/// filled the `LIMIT` and that run has more than one row, so only the sort keys
/// are required to match there: tied rows past the boundary may have taken the
/// places of the ones returned. A last run of one row is compared in full,
/// although rows past the `LIMIT` may tie with it too (two of `ClickBench` Q31's
/// groups share its tenth count), because its other cells are still worth
/// checking; a caller that can read the reference's rows past the `LIMIT` judges
/// a mismatch there with [`validate_against_keyed_reference`]. A result shorter
/// than `LIMIT` was not truncated at all. The first run is a cutoff the same way
/// for a query whose `OFFSET` skipped rows, since the skipped rows may tie with
/// it. A numeric sort key
/// compares the way a numeric cell does, so a key two engines round differently
/// still puts a row in the same tie group.
fn compare_limit_results_allowing_cutoff_ties(
    query_name: &str,
    sql: &str,
    left_batches: &[RecordBatch],
    right_batches: &[RecordBatch],
) -> Result<QueryValidationResult> {
    if left_batches.is_empty() && right_batches.is_empty() {
        return Ok(QueryValidationResult::Pass);
    }
    if left_batches.is_empty() {
        return Ok(QueryValidationResult::Fail(
            QueryValidationFailReason::NoAnswer,
        ));
    }
    if right_batches.is_empty() {
        return Ok(QueryValidationResult::Fail(
            QueryValidationFailReason::NoExpectedAnswer,
        ));
    }

    let left_schema = left_batches[0].schema();
    let right_schema = right_batches[0].schema();
    if left_schema.fields().len() != right_schema.fields().len() {
        return Ok(QueryValidationResult::Fail(
            QueryValidationFailReason::SchemaMismatch,
        ));
    }

    let left = arrow::compute::concat_batches(&left_schema, left_batches)?;
    let right = arrow::compute::concat_batches(&right_schema, right_batches)?;
    if left.num_rows() != right.num_rows() {
        return Ok(QueryValidationResult::Fail(
            QueryValidationFailReason::RowCountMismatch {
                expected: left.num_rows(),
                actual: right.num_rows(),
            },
        ));
    }

    let statement = sort_order::parse_one_statement(sql);
    let resolution = statement.as_ref().map_or_else(
        || SortKeyResolution::Unresolved {
            reason: "SQL did not parse as a single statement".to_string(),
        },
        |parsed| sort_order::resolve_statement_sort_key(parsed, &left.schema()),
    );
    let SortKeyResolution::Resolved {
        key,
        unresolved_suffix,
    } = resolution
    else {
        return compare_query_result_batches(
            query_name,
            left_batches,
            right_batches,
            RowOrder::Preserved,
        );
    };
    if unresolved_suffix.is_some() {
        // A hidden sort term means ties on the visible prefix are not free.
        // Compare the full rows so a payload mismatch reaches the keyed-reference
        // fallback, which has every `ORDER BY` term.
        return compare_query_result_batches(
            query_name,
            left_batches,
            right_batches,
            RowOrder::Multiset,
        );
    }

    let n = left.num_rows();
    let limit_filled = sort_order::top_level_limit_count(sql).is_some_and(|limit| n == limit);
    let offset_skips_rows =
        sort_order::top_level_offset_count(sql).is_some_and(|offset| offset > 0);
    let mut run_start = 0_usize;
    while run_start < n {
        let run_key = row_sort_key(&left, run_start, &key)?;
        if let Some(mismatch) = sort_key_mismatch(&left, &right, run_start, &key, &run_key)? {
            println!(
                "Query '{query_name}' ORDER BY key mismatch at row {} (left vs right cutoff)",
                run_start + 1
            );
            return Ok(QueryValidationResult::Fail(mismatch));
        }
        let mut run_end = run_start + 1;
        while run_end < n && row_sort_key(&left, run_end, &key)? == run_key {
            if let Some(mismatch) = sort_key_mismatch(&left, &right, run_end, &key, &run_key)? {
                return Ok(QueryValidationResult::Fail(mismatch));
            }
            run_end += 1;
        }
        let is_last_run = run_end == n;
        // A multi-row last group is not itself proof of LIMIT truncation:
        // `LIMIT 100` of two tied rows still has room for both, and skipping
        // the non-key compare would let a wrong `revenue` pass. Only a result
        // that filled the literal `LIMIT` can have cut a tie (TPC-DS Q65). The
        // first group is the other end of the page: an `OFFSET` may have cut
        // through it (ClickBench Q41 skips 100 rows).
        let truncated_cutoff = (run_end - run_start) > 1
            && ((is_last_run && limit_filled) || (run_start == 0 && offset_skips_rows));
        if !truncated_cutoff {
            let left_run = left.slice(run_start, run_end - run_start);
            let right_run = right.slice(run_start, run_end - run_start);
            let run_result = compare_query_result_batches(
                query_name,
                &[left_run],
                &[right_run],
                RowOrder::Multiset,
            )?;
            if let QueryValidationResult::Fail(reason) = run_result {
                return Ok(QueryValidationResult::Fail(reason));
            }
        }
        run_start = run_end;
    }
    Ok(QueryValidationResult::Pass)
}

/// Compares `right`'s sort key at `row` with `run_key`, the key of the tie group
/// `left` has there, and names the first key column that differs. A numeric key
/// column compares with [`numeric_strings_match`].
fn sort_key_mismatch(
    left: &RecordBatch,
    right: &RecordBatch,
    row: usize,
    key: &[SortKeyColumn],
    run_key: &[Option<String>],
) -> Result<Option<QueryValidationFailReason>> {
    let right_key = row_sort_key(right, row, key)?;
    let position = key.iter().zip(run_key.iter().zip(&right_key)).position(
        |(column, (left_value, right_value))| match (left_value, right_value) {
            (Some(left_value), Some(right_value)) => {
                left_value != right_value
                    && !(left
                        .schema_ref()
                        .field(column.index)
                        .data_type()
                        .is_numeric()
                        && numeric_strings_match(left_value, right_value))
            }
            (left_value, right_value) => left_value != right_value,
        },
    );
    Ok(
        position.map(|position| QueryValidationFailReason::DataMismatch {
            column: key[position].name.clone(),
            row_number: row + 1, // indexes are 0-based, counts are 1-based
            expected: run_key[position].as_deref().unwrap_or("").to_string(),
            actual: right_key[position].as_deref().unwrap_or("").to_string(),
        }),
    )
}

fn row_sort_key(
    batch: &RecordBatch,
    row: usize,
    key: &[SortKeyColumn],
) -> Result<Vec<Option<String>>> {
    let mut out = Vec::with_capacity(key.len());
    for column in key {
        out.push(array_value_to_string(
            batch.column(column.index).as_ref(),
            row,
        )?);
    }
    Ok(out)
}

/// Canonical row order for multiset equality: sort by stringified cell values
/// across all columns (same string forms used by [`validate_batches_as_strings`]).
fn sort_batch_lexicographic_as_strings(batch: &RecordBatch) -> Result<RecordBatch> {
    let n = batch.num_rows();
    if n <= 1 {
        return Ok(batch.clone());
    }

    let mut keys: Vec<(Vec<Option<String>>, usize)> = Vec::with_capacity(n);
    for row in 0..n {
        let mut key = Vec::with_capacity(batch.num_columns());
        for col in 0..batch.num_columns() {
            key.push(array_value_to_string(batch.column(col).as_ref(), row)?);
        }
        keys.push((key, row));
    }
    keys.sort_by(|a, b| a.0.cmp(&b.0));

    let indices: Vec<u32> = keys
        .into_iter()
        .map(|(_, i)| u32::try_from(i).map_err(|_| anyhow!("row index does not fit in u32")))
        .collect::<Result<Vec<_>>>()?;
    let index_array = arrow::array::UInt32Array::from(indices);

    let columns: Result<Vec<_>> = batch
        .columns()
        .iter()
        .map(|col| {
            arrow::compute::take(col.as_ref(), &index_array, None)
                .map_err(|e| anyhow!("take failed during multiset sort: {e}"))
        })
        .collect();
    Ok(RecordBatch::try_new(batch.schema(), columns?)?)
}

#[cfg(test)]
mod test {
    use crate::queries::QuerySet;

    use super::*;
    use arrow::{
        array::{
            ArrayRef, Decimal128Builder, Decimal256Builder, Float32Array, Int8Array, Int16Array,
            UInt8Array, UInt16Array, UInt32Array, UInt64Array,
        },
        datatypes::{Field, Schema, SchemaRef, i256},
    };
    use rstest::rstest;
    use std::sync::Arc;

    #[test]
    fn test_tpch_answers() {
        // Check that the TPCH answers are loaded correctly
        assert_eq!(TPCH_ANSWERS.len(), 44);
        assert_eq!(
            TPCH_ANSWERS
                .get("tpch_q1")
                .expect("should have q1 answer")
                .len(),
            1
        );

        let batches = TPCH_ANSWERS
            .get("tpch_q1")
            .expect("should have q1 answer")
            .clone();
        let schema = batches[0].schema();
        assert_eq!(schema.fields().len(), 10);
    }

    #[test]
    fn test_static_tpch_answers_are_sf1_only() {
        let query = Query::new("tpch_q22".into(), "SELECT 1".into(), false);

        assert!(has_static_tpch_answer(&query));
        assert!(should_validate_with_static_tpch_answer(&query, 1.0));
        assert!(!should_validate_with_static_tpch_answer(&query, 10.0));
        assert!(!should_validate_with_static_tpch_answer(&query, 100.0));

        let batches = TPCH_ANSWERS
            .get("tpch_q22")
            .expect("should have q22 answer")
            .clone();

        assert_eq!(
            validate_tpch_query_at_scale(&query, &batches, 100.0).expect("should validate"),
            QueryValidationResult::Fail(QueryValidationFailReason::NoExpectedAnswerAtScaleFactor)
        );
    }

    #[test]
    fn test_validate_tpch_query() {
        // Create a dummy query
        let query = Query::new("tpch_q1".into(), "SELECT * FROM lineitem".into(), false);

        // Create a batch of results using the real answer columns
        // l_returnflag|l_linestatus|sum_qty|sum_base_price|sum_disc_price|sum_charge|avg_qty|avg_price|avg_disc|count_order
        let schema = Schema::new(vec![
            Field::new("l_returnflag", arrow::datatypes::DataType::Utf8, false),
            Field::new("l_linestatus", arrow::datatypes::DataType::Utf8, false),
            Field::new("sum_qty", arrow::datatypes::DataType::Float64, false),
            Field::new("sum_base_price", arrow::datatypes::DataType::Float64, false),
            Field::new("sum_disc_price", arrow::datatypes::DataType::Float64, false),
            Field::new("sum_charge", arrow::datatypes::DataType::Float64, false),
            Field::new("avg_qty", arrow::datatypes::DataType::Float64, false),
            Field::new("avg_price", arrow::datatypes::DataType::Float64, false),
            Field::new("avg_disc", arrow::datatypes::DataType::Float64, false),
            Field::new("count_order", arrow::datatypes::DataType::Int32, false),
        ]);

        let schema_ref: SchemaRef = Arc::new(schema);
        let batch = RecordBatch::try_new(
            Arc::clone(&schema_ref),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["A", "B"])),
                Arc::new(arrow::array::StringArray::from(vec!["C", "D"])),
                Arc::new(arrow::array::Float64Array::from(vec![1.0, 2.0])),
                Arc::new(arrow::array::Float64Array::from(vec![3.0, 4.0])),
                Arc::new(arrow::array::Float64Array::from(vec![5.0, 6.0])),
                Arc::new(arrow::array::Float64Array::from(vec![7.0, 8.0])),
                Arc::new(arrow::array::Float64Array::from(vec![9.0, 10.0])),
                Arc::new(arrow::array::Float64Array::from(vec![11.0, 12.0])),
                Arc::new(arrow::array::Float64Array::from(vec![13.0, 14.0])),
                Arc::new(arrow::array::Int32Array::from(vec![15, 16])),
            ],
        )
        .expect("Should create batch");
        let batches = vec![batch];

        // Validate the query
        let result = validate_tpch_query(&query, &batches);
        assert!(result.is_ok());
        assert_eq!(
            result.expect("Should validate"),
            QueryValidationResult::Fail(QueryValidationFailReason::RowCountMismatch {
                expected: 4,
                actual: 2
            })
        );

        // Use the correct answer
        let correct_batches = TPCH_ANSWERS
            .get("tpch_q1")
            .expect("should have q1 answer")
            .clone();
        let result = validate_tpch_query(&query, &correct_batches);
        assert!(result.is_ok());
        assert_eq!(
            result.expect("Should validate"),
            QueryValidationResult::Pass
        );
    }

    #[tokio::test]
    async fn test_correct_answer_wrong_type() {
        // Use the correct answer, but a different datatype
        // Q22 from CSV, cntrycode is Utf8. Query returns it as Int64
        let query = QuerySet::Tpch
            .get_queries(None, None, None, None)
            .await
            .expect("to get queries")
            .get(20)
            .expect("Should have q22")
            .clone();
        assert_eq!(query.name, "tpch_q22".into());
        let schema = Schema::new(vec![
            Field::new("cntrycode", arrow::datatypes::DataType::Int64, false),
            Field::new("numcust", arrow::datatypes::DataType::Int64, false),
            Field::new("totacctbal", arrow::datatypes::DataType::Float64, false),
        ]);

        let schema_ref: SchemaRef = Arc::new(schema);
        let batch = RecordBatch::try_new(
            Arc::clone(&schema_ref),
            vec![
                Arc::new(arrow::array::Int64Array::from(vec![
                    13, 17, 18, 23, 29, 30, 31,
                ])),
                Arc::new(arrow::array::Int64Array::from(vec![
                    888, 861, 964, 892, 948, 909, 922,
                ])),
                Arc::new(arrow::array::Float64Array::from(vec![
                    6_737_713.99,
                    6_460_573.72,
                    7_236_687.40,
                    6_701_457.95,
                    7_158_866.63,
                    6_808_436.13,
                    6_806_670.18,
                ])),
            ],
        )
        .expect("Should create batch");

        let batches = vec![batch];
        let result = validate_tpch_query(&query, &batches);
        assert!(result.is_ok());
        assert_eq!(
            result.expect("Should validate"),
            QueryValidationResult::Pass
        );

        let schema = Schema::new(vec![
            Field::new("cntrycode", arrow::datatypes::DataType::Utf8, false),
            Field::new("numcust", arrow::datatypes::DataType::Int64, false),
            Field::new("totacctbal", arrow::datatypes::DataType::Float64, false),
        ]);

        let schema_ref: SchemaRef = Arc::new(schema);
        let batch = RecordBatch::try_new(
            Arc::clone(&schema_ref),
            vec![
                Arc::new(arrow::array::StringArray::from(vec![
                    "13", "17", "18", "23", "29", "30", "31",
                ])),
                Arc::new(arrow::array::Int64Array::from(vec![
                    888, 861, 964, 892, 948, 909, 922,
                ])),
                Arc::new(arrow::array::Float64Array::from(vec![
                    6_737_713.99,
                    6_460_573.72,
                    7_236_687.40,
                    6_701_457.95,
                    7_158_866.63,
                    6_808_436.13,
                    6_806_670.18,
                ])),
            ],
        )
        .expect("Should create batch");

        let batches = vec![batch];
        let result = validate_tpch_query(&query, &batches);
        assert!(result.is_ok());
        assert_eq!(
            result.expect("Should validate"),
            QueryValidationResult::Pass
        );
    }

    #[tokio::test]
    async fn test_wrong_answers() {
        // Use the wrong answer and validate it fails
        let query = QuerySet::Tpch
            .get_queries(None, None, None, None)
            .await
            .expect("to get queries")
            .get(20)
            .expect("Should have q22")
            .clone();
        assert_eq!(query.name, "tpch_q22".into());
        let schema = Schema::new(vec![
            Field::new("cntrycode", arrow::datatypes::DataType::Int64, false),
            Field::new("numcust", arrow::datatypes::DataType::Int64, false),
            Field::new("totacctbal", arrow::datatypes::DataType::Float64, false),
        ]);

        let schema_ref: SchemaRef = Arc::new(schema);
        let batch = RecordBatch::try_new(
            Arc::clone(&schema_ref),
            vec![
                Arc::new(arrow::array::Int64Array::from(vec![
                    13, 17, 18, 23, 29, 30, 39,
                ])),
                Arc::new(arrow::array::Int64Array::from(vec![
                    888, 861, 964, 892, 948, 909, 922,
                ])),
                Arc::new(arrow::array::Float64Array::from(vec![
                    6_737_713.99,
                    6_460_573.72,
                    7_236_687.40,
                    6_701_457.95,
                    7_158_866.63,
                    6_808_436.13,
                    6_806_670.18,
                ])),
            ],
        )
        .expect("Should create batch");

        let batches = vec![batch];
        let result = validate_tpch_query(&query, &batches);
        assert!(result.is_ok());
        assert_eq!(
            result.expect("Should validate"),
            QueryValidationResult::Fail(QueryValidationFailReason::DataMismatch {
                column: "cntrycode".to_string(),
                row_number: 7,
                expected: format!("{:?}", "31"),
                actual: format!("{:?}", "39"),
            })
        );

        let schema = Schema::new(vec![
            Field::new("cntrycode", arrow::datatypes::DataType::Utf8, false),
            Field::new("numcust", arrow::datatypes::DataType::Int64, false),
            Field::new("totacctbal", arrow::datatypes::DataType::Float64, false),
        ]);

        let schema_ref: SchemaRef = Arc::new(schema);
        let batch = RecordBatch::try_new(
            Arc::clone(&schema_ref),
            vec![
                Arc::new(arrow::array::StringArray::from(vec![
                    "13", "17", "18", "23", "29", "14", "31",
                ])),
                Arc::new(arrow::array::Int64Array::from(vec![
                    888, 861, 964, 892, 948, 909, 922,
                ])),
                Arc::new(arrow::array::Float64Array::from(vec![
                    6_737_713.99,
                    6_460_573.72,
                    7_236_687.40,
                    6_701_457.95,
                    7_158_866.63,
                    6_808_436.13,
                    6_806_670.18,
                ])),
            ],
        )
        .expect("Should create batch");

        let batches = vec![batch];
        let result = validate_tpch_query(&query, &batches);
        assert!(result.is_ok());
        assert_eq!(
            result.expect("Should validate"),
            QueryValidationResult::Fail(QueryValidationFailReason::DataMismatch {
                column: "cntrycode".to_string(),
                row_number: 6,
                expected: format!("{:?}", "30"),
                actual: format!("{:?}", "14"),
            })
        );
    }

    #[rstest]
    #[case(2, 1_234_567_890_123_456_789_i128, "12345678901234567.89")]
    #[case(3, 1_234_567_890_123_456_789_i128, "1234567890123456.789")]
    #[case(10, 1_234_567_890_123_456_789_i128, "123456789.0123456789")]
    #[case(0, 1_234_567_890_123_456_789_i128, "1234567890123456789")]
    #[case(2, -1_234_567_890_123_456_789_i128, "-12345678901234567.89")]
    #[case(3, -1_234_567_890_123_456_789_i128, "-1234567890123456.789")]
    #[case(10, -1_234_567_890_123_456_789_i128, "-123456789.0123456789")]
    #[case(0, -1_234_567_890_123_456_789_i128, "-1234567890123456789")]
    fn test_decimal_values(#[case] scale: i8, #[case] value: i128, #[case] expected: &str) {
        // Test a positive value with scale = 2
        let mut builder = Decimal128Builder::new()
            .with_precision_and_scale(38, scale)
            .expect("Should create builder");
        builder.append_value(value);
        let array = builder.finish();

        let result = array_value_to_string(&array, 0).expect("Should convert value to string");
        assert_eq!(result, Some(expected.to_string()));
    }

    // A `MySQL` `SUM(DECIMAL)` widens to Decimal256(76, scale); ensure the i256
    // arm renders identically to the Decimal128 arm across sign, scale 0/>0, and
    // the scale-exceeds-digits case (fractional zero-padding).
    #[rstest]
    #[case(0, 12_345_i128, "12345")]
    #[case(2, 12_345_i128, "123.45")]
    #[case(3, 12_345_i128, "12.345")]
    #[case(0, -12_345_i128, "-12345")]
    #[case(2, -12_345_i128, "-123.45")]
    #[case(6, 1_i128, "0.000001")]
    #[case(4, -7_i128, "-0.0007")]
    fn test_decimal256_values(#[case] scale: i8, #[case] value: i128, #[case] expected: &str) {
        let mut builder = Decimal256Builder::new()
            .with_precision_and_scale(76, scale)
            .expect("Should create Decimal256 builder");
        builder.append_value(i256::from_i128(value));
        let array = builder.finish();

        let result = array_value_to_string(&array, 0).expect("Should convert value to string");
        assert_eq!(result, Some(expected.to_string()));
    }

    #[rstest]
    #[case(123_456_789_i64, "123456789")]
    #[case(987_654_321_i64, "987654321")]
    #[case(-123_456_789_i64, "-123456789")]
    #[case(-987_654_321_i64, "-987654321")]
    fn test_int64(#[case] value: i64, #[case] expected: &str) {
        // Test an Int64 array
        let int_values = vec![value];
        let array = Int64Array::from(int_values);

        let result = array_value_to_string(&array, 0).expect("Failed to convert value to string");
        assert_eq!(result, Some(expected.to_string()));
    }

    #[rstest]
    #[case(123_456_789_i32, "123456789")]
    #[case(987_654_321_i32, "987654321")]
    #[case(-123_456_789_i32, "-123456789")]
    #[case(-987_654_321_i32, "-987654321")]
    fn test_int32_64(#[case] value: i32, #[case] expected: &str) {
        // Test an Int32 array
        let int_values = vec![value];
        let array = Int32Array::from(int_values);

        let result = array_value_to_string(&array, 0).expect("Failed to convert value to string");
        assert_eq!(result, Some(expected.to_string()));

        // Test an Int64 array
        let int_values = vec![i64::from(value)];
        let array = Int64Array::from(int_values);
        let result = array_value_to_string(&array, 0).expect("Failed to convert value to string");
        assert_eq!(result, Some(expected.to_string()));
    }

    #[rstest]
    #[case(2_i8, "2")]
    #[case(3_i8, "3")]
    #[case(-2_i8, "-2")]
    #[case(-3_i8, "-3")]
    fn test_int8_16(#[case] value: i8, #[case] expected: &str) {
        // Test an Int8 array
        let int_values = vec![value];
        let array = Int8Array::from(int_values);

        let result = array_value_to_string(&array, 0).expect("Failed to convert value to string");
        assert_eq!(result, Some(expected.to_string()));

        // Test an Int16 array
        let int_values = vec![i16::from(value)];
        let array = Int16Array::from(int_values);
        let result = array_value_to_string(&array, 0).expect("Failed to convert value to string");
        assert_eq!(result, Some(expected.to_string()));
    }

    #[rstest]
    #[case(123_456_789_u32, "123456789")]
    #[case(987_654_321_u32, "987654321")]
    fn test_uint32_64(#[case] value: u32, #[case] expected: &str) {
        // Test an Int32 array
        let int_values = vec![value];
        let array = UInt32Array::from(int_values);

        let result = array_value_to_string(&array, 0).expect("Failed to convert value to string");
        assert_eq!(result, Some(expected.to_string()));

        // Test an Int64 array
        let int_values = vec![u64::from(value)];
        let array = UInt64Array::from(int_values);
        let result = array_value_to_string(&array, 0).expect("Failed to convert value to string");
        assert_eq!(result, Some(expected.to_string()));
    }

    #[rstest]
    #[case(2_u8, "2")]
    #[case(3_u8, "3")]
    fn test_uint8_16(#[case] value: u8, #[case] expected: &str) {
        // Test an Int8 array
        let int_values = vec![value];
        let array = UInt8Array::from(int_values);

        let result = array_value_to_string(&array, 0).expect("Failed to convert value to string");
        assert_eq!(result, Some(expected.to_string()));

        // Test an Int16 array
        let int_values = vec![u16::from(value)];
        let array = UInt16Array::from(int_values);
        let result = array_value_to_string(&array, 0).expect("Failed to convert value to string");
        assert_eq!(result, Some(expected.to_string()));
    }

    #[rstest]
    #[case(123_456_789_f64, "123456789")]
    #[case(987_654_321_f64, "987654321")]
    #[case(-123_456_789_f64, "-123456789")]
    #[case(-987_654_321_f64, "-987654321")]
    #[case(123_456_789.123_456_79_f64, "123456789.12345679")]
    #[case(987_654_321.987_654_3_f64, "987654321.9876543")]
    #[case(-123_456_789.123_456_79_f64, "-123456789.12345679")]
    #[case(-987_654_321.987_654_3_f64, "-987654321.9876543")]
    fn test_float64(#[case] value: f64, #[case] expected: &str) {
        // Test a Float64 array
        let float_values = vec![value];
        let array = Float64Array::from(float_values);

        let result = array_value_to_string(&array, 0).expect("Failed to convert value to string");
        assert_eq!(result, Some(expected.to_string()));
    }

    #[rstest]
    #[case(123_456_f32, "123456")]
    #[case(987_654_f32, "987654")]
    #[case(-123_456_f32, "-123456")]
    #[case(-987_654_f32, "-987654")]
    #[case(123_456.12_f32, "123456.12")]
    #[case(-123_456.12_f32, "-123456.12")]
    fn test_float32(#[case] value: f32, #[case] expected: &str) {
        // Test a Float32 array
        let float_values = vec![value];
        let array = Float32Array::from(float_values);

        let result = array_value_to_string(&array, 0).expect("Failed to convert value to string");
        assert_eq!(result, Some(expected.to_string()));
    }

    #[test]
    fn test_dates_and_timestamps() {
        // Test a Date32 array
        let date_values = vec![14_600_i32];
        let array = Date32Array::from(date_values);

        let result = array_value_to_string(&array, 0).expect("Failed to convert value to string");
        assert_eq!(result, Some("2009-12-22".to_string()));

        // Test a TimestampSecond array
        let timestamp_values = vec![123_456_789_i64];
        let array = TimestampSecondArray::from(timestamp_values);

        let result = array_value_to_string(&array, 0).expect("Failed to convert value to string");
        assert_eq!(result, Some("1973-11-29 21:33:09".to_string()));

        // Test a TimestampMillisecond array
        let timestamp_values = vec![123_456_789_123_i64];
        let array = TimestampMillisecondArray::from(timestamp_values);
        let result = array_value_to_string(&array, 0).expect("Failed to convert value to string");

        assert_eq!(result, Some("1973-11-29 21:33:09.123".to_string()));

        // Test a TimestampMicrosecond array
        let timestamp_values = vec![123_456_789_123_456_i64];
        let array = TimestampMicrosecondArray::from(timestamp_values);
        let result = array_value_to_string(&array, 0).expect("Failed to convert value to string");

        assert_eq!(result, Some("1973-11-29 21:33:09.123456".to_string()));

        // Test a TimestampNanosecond array
        let timestamp_values = vec![123_456_789_123_456_789_i64];
        let array = TimestampNanosecondArray::from(timestamp_values);
        let result = array_value_to_string(&array, 0).expect("Failed to convert value to string");

        assert_eq!(result, Some("1973-11-29 21:33:09.123456789".to_string()));
    }

    #[test]
    fn test_invalid_index() {
        // Test index out of bounds
        let decimal_values = vec![1_234_567_890_123_456_789_i128];
        let array = Decimal128Array::from(decimal_values);

        // Index 1 doesn't exist in a 1-element array
        let result = array_value_to_string(&array, 1);
        assert_eq!(
            result.expect_err("Should return an error").to_string(),
            "Index out of bounds: 1 >= 1"
        );
    }

    #[test]
    fn test_compare_query_result_batches_multiset_reorders() {
        // Same multiset, different physical order — Multiset must pass; Preserved must fail.
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", arrow::datatypes::DataType::Utf8, false),
            Field::new("v", arrow::datatypes::DataType::Int64, false),
        ]));
        let left = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["a", "b"])),
                Arc::new(arrow::array::Int64Array::from(vec![1, 2])),
            ],
        )
        .expect("left batch");
        let right = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["b", "a"])),
                Arc::new(arrow::array::Int64Array::from(vec![2, 1])),
            ],
        )
        .expect("right batch");

        let multiset = compare_query_result_batches(
            "reorder",
            std::slice::from_ref(&left),
            std::slice::from_ref(&right),
            RowOrder::Multiset,
        )
        .expect("compare multiset");
        assert_eq!(multiset, QueryValidationResult::Pass);

        let preserved =
            compare_query_result_batches("reorder", &[left], &[right], RowOrder::Preserved)
                .expect("compare preserved");
        assert!(
            matches!(
                preserved,
                QueryValidationResult::Fail(QueryValidationFailReason::DataMismatch { .. })
            ),
            "preserved order must detect swapped rows: {preserved:?}"
        );
    }

    #[test]
    fn test_compare_query_result_batches_detects_value_mismatch() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "v",
            arrow::datatypes::DataType::Int64,
            false,
        )]));
        let left = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(arrow::array::Int64Array::from(vec![1, 2, 3]))],
        )
        .expect("left");
        let right = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(arrow::array::Int64Array::from(vec![1, 2, 99]))],
        )
        .expect("right");

        let result = compare_query_result_batches("values", &[left], &[right], RowOrder::Multiset)
            .expect("compare");
        assert!(
            matches!(
                result,
                QueryValidationResult::Fail(QueryValidationFailReason::DataMismatch { .. })
            ),
            "value mismatch must fail: {result:?}"
        );
    }

    #[test]
    fn test_validate_against_reference_accepts_reordered_rows() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", arrow::datatypes::DataType::Utf8, false),
            Field::new("v", arrow::datatypes::DataType::Int64, false),
        ]));
        let actual = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["a", "b"])),
                Arc::new(arrow::array::Int64Array::from(vec![1, 2])),
            ],
        )
        .expect("actual batch");
        let reference = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["b", "a"])),
                Arc::new(arrow::array::Int64Array::from(vec![2, 1])),
            ],
        )
        .expect("reference batch");

        let query = Query::new("tpcds_q1".into(), "SELECT k, v FROM t".into(), false);
        let result = validate_against_reference_batches(
            &query,
            std::slice::from_ref(&actual),
            std::slice::from_ref(&reference),
        )
        .expect("compare");
        assert_eq!(
            result,
            QueryValidationResult::Pass,
            "TPC-DS queries without ORDER BY + LIMIT must compare as a multiset: {result:?}"
        );
    }

    fn q65_tied_batches(
        left_revenue: [&str; 2],
        right_revenue: [&str; 2],
    ) -> (RecordBatch, RecordBatch) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("s_store_name", arrow::datatypes::DataType::Utf8, false),
            Field::new("i_item_desc", arrow::datatypes::DataType::Utf8, false),
            Field::new("revenue", arrow::datatypes::DataType::Utf8, false),
        ]));
        let actual = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["able", "able"])),
                Arc::new(arrow::array::StringArray::from(vec!["A", "A"])),
                Arc::new(arrow::array::StringArray::from(vec![
                    left_revenue[0],
                    left_revenue[1],
                ])),
            ],
        )
        .expect("actual");
        let reference = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["able", "able"])),
                Arc::new(arrow::array::StringArray::from(vec!["A", "A"])),
                Arc::new(arrow::array::StringArray::from(vec![
                    right_revenue[0],
                    right_revenue[1],
                ])),
            ],
        )
        .expect("reference");
        (actual, reference)
    }

    #[test]
    fn test_offset_cutoff_ties_at_both_ends_of_the_page_are_free() {
        // ClickBench Q41 pages with `ORDER BY PageViews DESC LIMIT 10 OFFSET 100`: the
        // tie group the OFFSET cuts through starts the page and the one the LIMIT cuts
        // ends it, so two correct engines put different rows at both ends.
        let schema = Arc::new(Schema::new(vec![
            Field::new("URLHash", DataType::Int64, false),
            Field::new("PageViews", DataType::Int64, false),
        ]));
        let page = |hashes: [i64; 6]| {
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(hashes.to_vec())),
                    Arc::new(Int64Array::from(vec![27, 27, 26, 26, 24, 24])),
                ],
            )
            .expect("page batch")
        };
        let reference = page([1, 2, 3, 4, 5, 6]);
        let served = page([7, 8, 4, 3, 6, 9]);
        let offset_query = Query::new(
            "offset_page".into(),
            r#"SELECT "URLHash", "PageViews" FROM hits ORDER BY "PageViews" DESC LIMIT 6 OFFSET 100"#
                .into(),
            false,
        );
        assert_eq!(
            validate_against_reference_batches(
                &offset_query,
                std::slice::from_ref(&served),
                std::slice::from_ref(&reference)
            )
            .expect("compare offset page"),
            QueryValidationResult::Pass
        );
        // The tie groups between the two ends are whole, so their rows must match.
        assert!(matches!(
            validate_against_reference_batches(
                &offset_query,
                &[page([7, 8, 3, 10, 6, 9])],
                std::slice::from_ref(&reference)
            )
            .expect("compare wrong middle"),
            QueryValidationResult::Fail(_)
        ));
        // Without an OFFSET the page starts at the top of the result, so its first tie
        // group is whole too.
        let top_query = Query::new(
            "top_page".into(),
            r#"SELECT "URLHash", "PageViews" FROM hits ORDER BY "PageViews" DESC LIMIT 6"#.into(),
            false,
        );
        assert!(matches!(
            validate_against_reference_batches(&top_query, &[served], &[reference])
                .expect("compare top page"),
            QueryValidationResult::Fail(_)
        ));
    }

    #[test]
    fn test_validate_against_reference_rejects_tied_rows_when_limit_is_not_filled() {
        // LIMIT 100 of two rows cannot have truncated a tie group, so a
        // different `revenue` is a real mismatch — not a legal LIMIT subset.
        let (actual, reference) = q65_tied_batches(["4.63", "8.64"], ["4.40", "1.74"]);
        let query = Query::new(
            "tpcds_q65".into(),
            "SELECT s_store_name, i_item_desc, revenue FROM t \
             ORDER BY s_store_name, i_item_desc LIMIT 100"
                .into(),
            false,
        );
        let result =
            validate_against_reference_batches(&query, &[actual], &[reference]).expect("compare");
        assert!(
            matches!(
                result,
                QueryValidationResult::Fail(QueryValidationFailReason::DataMismatch { .. })
            ),
            "a short result under LIMIT 100 must still compare non-key cells: {result:?}"
        );
    }

    #[test]
    fn test_validate_against_reference_accepts_tied_limit_subsets() {
        // TPC-DS Q65: ORDER BY store name + item desc is not unique — many
        // items share description "A". When the result fills LIMIT, the last
        // tie group may be truncated and two engines may keep different
        // members; both answers are SQL-correct.
        let (actual, reference) = q65_tied_batches(["4.63", "8.64"], ["4.40", "1.74"]);
        let query = Query::new(
            "tpcds_q65".into(),
            "SELECT s_store_name, i_item_desc, revenue FROM t \
             ORDER BY s_store_name, i_item_desc LIMIT 2"
                .into(),
            false,
        );
        let result =
            validate_against_reference_batches(&query, &[actual], &[reference]).expect("compare");
        assert_eq!(
            result,
            QueryValidationResult::Pass,
            "tied ORDER BY + filled LIMIT subsets must pass: {result:?}"
        );
    }

    #[test]
    fn test_validate_against_reference_rejects_unique_limit_mismatch() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", arrow::datatypes::DataType::Utf8, false),
            Field::new("v", arrow::datatypes::DataType::Int64, false),
        ]));
        let actual = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["a", "b"])),
                Arc::new(arrow::array::Int64Array::from(vec![1, 2])),
            ],
        )
        .expect("actual");
        let reference = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["a", "b"])),
                Arc::new(arrow::array::Int64Array::from(vec![1, 99])),
            ],
        )
        .expect("reference");
        let query = Query::new(
            "tpcds_q1".into(),
            "SELECT k, v FROM t ORDER BY k LIMIT 2".into(),
            false,
        );
        let result =
            validate_against_reference_batches(&query, &[actual], &[reference]).expect("compare");
        assert!(
            matches!(
                result,
                QueryValidationResult::Fail(QueryValidationFailReason::DataMismatch { .. })
            ),
            "a unique ORDER BY key must still fail on a wrong cell: {result:?}"
        );
    }

    const UNORDERED_GROUP_LIMIT: &str = r#"SELECT "UserID", "SearchPhrase", COUNT(*) FROM hits GROUP BY "UserID", "SearchPhrase" LIMIT 2"#;

    fn user_phrase_counts(rows: &[(i64, Option<&str>, i64)]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("UserID", DataType::Int64, false),
            Field::new("SearchPhrase", DataType::Utf8, true),
            Field::new("count(*)", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
                Arc::new(StringArray::from(
                    rows.iter().map(|row| row.1).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.2))),
            ],
        )
        .expect("user phrase count batch")
    }

    fn check_unordered_limit_subset(
        sql: &str,
        returned: &RecordBatch,
        full_result: &[RecordBatch],
    ) -> QueryValidationResult {
        let unordered_limit = unordered_limit(sql).expect("query should have an unordered LIMIT");
        let mut check =
            UnorderedLimitSubsetCheck::new(&unordered_limit, std::slice::from_ref(returned))
                .expect("subset check should build");
        for batch in full_result {
            check
                .observe(batch)
                .expect("full result batch should be observed");
        }
        check.finish()
    }

    #[test]
    fn test_unordered_limit_is_only_a_grouped_limit_that_returns_its_group_keys() {
        let limit = unordered_limit(UNORDERED_GROUP_LIMIT).expect("LIMIT without ORDER BY");
        assert_eq!((limit.limit, limit.offset), (2, 0));
        assert_eq!(
            limit.unlimited_sql,
            r#"SELECT "UserID", "SearchPhrase", COUNT(*) FROM hits GROUP BY "UserID", "SearchPhrase""#
        );
        for (sql, limit_and_offset) in [
            (
                "SELECT a, count(*) FROM t GROUP BY a LIMIT 10 OFFSET 5",
                (10, 5),
            ),
            (
                "SELECT a AS k, count(*) FROM t GROUP BY k LIMIT 10",
                (10, 0),
            ),
            ("SELECT a, count(*) FROM t GROUP BY 1 LIMIT 10", (10, 0)),
        ] {
            assert_eq!(
                unordered_limit(sql).map(|limit| (limit.limit, limit.offset)),
                Some(limit_and_offset),
                "{sql}"
            );
        }
        for sql in [
            "SELECT a FROM t ORDER BY a LIMIT 10",
            "SELECT a FROM (SELECT a FROM t LIMIT 10) AS s",
            "SELECT a FROM t LIMIT $1",
            "SELECT a FROM t FETCH FIRST 10 ROWS ONLY",
            "SELECT a FROM t",
            // A plain projection: rows computed wrongly can still be rows of the
            // full result (TPC-H simple_q6).
            "SELECT * FROM (SELECT o_orderkey + 1 FROM orders) AS c(key) LIMIT 10",
            "SELECT DISTINCT a FROM t LIMIT 10",
            // The groups are not returned, so a wrong count can match another group.
            "SELECT count(*) FROM t GROUP BY a LIMIT 10",
            // A nested LIMIT leaves the full result itself unspecified (TPC-H simple_q7).
            "SELECT * FROM (SELECT o_orderkey FROM orders LIMIT 10) AS c(key) LIMIT 10",
            "SELECT a, count(*) FROM (SELECT a FROM t LIMIT 5) AS s GROUP BY a LIMIT 10",
            // The LIMIT applies to a parenthesized query that orders its own rows.
            "(SELECT a FROM t ORDER BY a) LIMIT 10",
            "(SELECT a, count(*) FROM t GROUP BY a ORDER BY a) LIMIT 10",
        ] {
            assert_eq!(unordered_limit(sql), None, "{sql}");
        }
    }

    #[test]
    fn test_unordered_limit_may_keep_different_rows_only_when_the_limit_can_cut() {
        let limit = unordered_limit(UNORDERED_GROUP_LIMIT).expect("LIMIT without ORDER BY");
        assert!(limit.may_keep_different_rows(2, 2));
        assert!(
            !limit.may_keep_different_rows(1, 1),
            "a result shorter than the LIMIT is the whole full result"
        );
        assert!(
            !limit.may_keep_different_rows(2, 1),
            "the full result fixes how many rows are kept"
        );
        let with_offset = unordered_limit("SELECT a, count(*) FROM t GROUP BY a LIMIT 2 OFFSET 1")
            .expect("LIMIT without ORDER BY");
        assert!(
            with_offset.may_keep_different_rows(1, 1),
            "an OFFSET skips rows the query does not specify"
        );
    }

    #[test]
    fn test_unordered_limit_subset_accepts_different_rows_of_the_full_result() {
        let full_result = user_phrase_counts(&[(1, Some("a"), 3), (2, None, 5), (3, Some("c"), 7)]);
        let returned = user_phrase_counts(&[(3, Some("c"), 7), (2, None, 5)]);
        let reference = user_phrase_counts(&[(1, Some("a"), 3), (2, None, 5)]);
        let query = Query::new("clickbench_q18".into(), UNORDERED_GROUP_LIMIT.into(), false);
        assert!(
            matches!(
                validate_against_reference_batches(
                    &query,
                    std::slice::from_ref(&returned),
                    &[reference]
                )
                .expect("compare"),
                QueryValidationResult::Fail(_)
            ),
            "two correct answers that kept different groups do not compare equal directly"
        );
        assert_eq!(
            check_unordered_limit_subset(UNORDERED_GROUP_LIMIT, &returned, &[full_result]),
            QueryValidationResult::Pass
        );
    }

    #[test]
    fn test_unordered_limit_subset_rejects_a_row_the_full_result_does_not_have() {
        let full_result = user_phrase_counts(&[(1, Some("a"), 3), (2, None, 5), (3, Some("c"), 7)]);
        let returned = user_phrase_counts(&[(3, Some("c"), 6), (2, None, 5)]);
        assert_eq!(
            check_unordered_limit_subset(UNORDERED_GROUP_LIMIT, &returned, &[full_result]),
            QueryValidationResult::Fail(QueryValidationFailReason::RowNotAllowedByLimit {
                row_number: 1,
                row: r#"[Some("3"), Some("c"), Some("6")]"#.to_string(),
            })
        );
    }

    #[test]
    fn test_unordered_limit_subset_rejects_a_short_result() {
        let full_result = user_phrase_counts(&[(1, Some("a"), 3), (2, None, 5), (3, Some("c"), 7)]);
        let returned = user_phrase_counts(&[(2, None, 5)]);
        assert_eq!(
            check_unordered_limit_subset(UNORDERED_GROUP_LIMIT, &returned, &[full_result]),
            QueryValidationResult::Fail(QueryValidationFailReason::RowCountMismatch {
                expected: 2,
                actual: 1
            })
        );
    }

    #[test]
    fn test_unordered_limit_subset_counts_repeated_rows() {
        let full_result = user_phrase_counts(&[(1, Some("a"), 3), (2, None, 5)]);
        let returned = user_phrase_counts(&[(1, Some("a"), 3), (1, Some("a"), 3)]);
        assert_eq!(
            check_unordered_limit_subset(UNORDERED_GROUP_LIMIT, &returned, &[full_result]),
            QueryValidationResult::Fail(QueryValidationFailReason::RowNotAllowedByLimit {
                row_number: 2,
                row: r#"[Some("1"), Some("a"), Some("3")]"#.to_string(),
            })
        );
    }

    #[test]
    fn test_unordered_limit_subset_offset_changes_how_many_rows_not_which() {
        // OFFSET is applied to each engine's own unspecified order. LIMIT 1
        // OFFSET 1 over [a, b, c] may return a (stream [c, a, b]), b, or c.
        let sql =
            r#"SELECT "UserID", "SearchPhrase", COUNT(*) FROM hits GROUP BY 1, 2 LIMIT 1 OFFSET 1"#;
        let full_result = [
            user_phrase_counts(&[(1, Some("a"), 3), (2, None, 5)]),
            user_phrase_counts(&[(3, Some("c"), 7)]),
        ];
        for row in [(1, Some("a"), 3), (2, None, 5), (3, Some("c"), 7)] {
            assert_eq!(
                check_unordered_limit_subset(sql, &user_phrase_counts(&[row]), &full_result),
                QueryValidationResult::Pass,
                "{row:?}"
            );
        }
        assert_eq!(
            check_unordered_limit_subset(
                sql,
                &user_phrase_counts(&[(2, None, 5), (3, Some("c"), 7)]),
                &full_result
            ),
            QueryValidationResult::Fail(QueryValidationFailReason::RowCountMismatch {
                expected: 1,
                actual: 2
            })
        );
    }

    #[test]
    fn test_numeric_cells_match_within_a_tenth_of_a_percent() {
        let revenue = |value: f64| {
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "revenue",
                    DataType::Float64,
                    false,
                )])),
                vec![Arc::new(Float64Array::from(vec![value]))],
            )
            .expect("revenue batch")
        };
        // TPC-H SF1 Q6's answer.
        let expected = revenue(123_141_078.228_3);
        assert_eq!(
            validate_batches_as_strings(&expected, &revenue(123_141_078.228_3 * 1.000_9))
                .expect("compare"),
            QueryValidationResult::Pass,
            "0.09% apart"
        );
        assert!(
            matches!(
                validate_batches_as_strings(&expected, &revenue(123_141_078.228_3 * 1.001_1))
                    .expect("compare"),
                QueryValidationResult::Fail(QueryValidationFailReason::DataMismatch { .. })
            ),
            "0.11% apart"
        );
        // What Q6 returns over a `lineitem` with 2% of its rows dropped.
        assert!(matches!(
            validate_batches_as_strings(&expected, &revenue(120_774_800.171_3)).expect("compare"),
            QueryValidationResult::Fail(QueryValidationFailReason::DataMismatch { .. })
        ));

        let count_order = |value: i64| {
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "count_order",
                    DataType::Int64,
                    false,
                )])),
                vec![Arc::new(Int64Array::from(vec![value]))],
            )
            .expect("count batch")
        };
        // TPC-H SF1 Q1's A|F `count_order`, and the count over 2% fewer `lineitem` rows.
        assert!(matches!(
            validate_batches_as_strings(&count_order(1_478_493), &count_order(1_449_049))
                .expect("compare"),
            QueryValidationResult::Fail(QueryValidationFailReason::DataMismatch { .. })
        ));
    }

    #[test]
    fn test_binary_cells_render_as_the_text_they_hold() {
        let bytes: &[u8] = b"google";
        for array in [
            Arc::new(BinaryArray::from_iter_values([bytes])) as ArrayRef,
            Arc::new(LargeBinaryArray::from_iter_values([bytes])),
            Arc::new(BinaryViewArray::from_iter_values([bytes])),
        ] {
            assert_eq!(
                array_value_to_string(array.as_ref(), 0).expect("binary cell should render"),
                Some("google".to_string()),
                "{:?}",
                array.data_type()
            );
        }
        let not_utf8 = LargeBinaryArray::from_iter_values([[0xff_u8, 0x00].as_slice()]);
        assert_eq!(
            array_value_to_string(&not_utf8, 0).expect("binary cell should render"),
            Some("\\xff00".to_string())
        );

        // A column read as bytes on one side and as text on the other compares equal.
        let schema = |data_type| {
            Arc::new(Schema::new(vec![Field::new(
                "SearchPhrase",
                data_type,
                false,
            )]))
        };
        let text = RecordBatch::try_new(
            schema(DataType::Utf8),
            vec![Arc::new(StringArray::from(vec!["google"]))],
        )
        .expect("text batch");
        let binary = RecordBatch::try_new(
            schema(DataType::LargeBinary),
            vec![Arc::new(LargeBinaryArray::from_iter_values([bytes]))],
        )
        .expect("binary batch");
        assert_eq!(
            compare_query_result_batches("clickbench_q13", &[text], &[binary], RowOrder::Multiset)
                .expect("compare"),
            QueryValidationResult::Pass
        );
    }

    const Q25: &str = r#"SELECT "SearchPhrase" FROM hits WHERE "SearchPhrase" <> '' ORDER BY to_timestamp("EventTime") LIMIT 10"#;

    fn search_phrases(values: &[&str]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "SearchPhrase",
                DataType::Utf8,
                false,
            )])),
            vec![Arc::new(StringArray::from(values.to_vec()))],
        )
        .expect("search phrase batch")
    }

    /// `ClickBench` Q25's first thirteen reference rows over `hits_0.parquet`, each
    /// with its sort key: rows 6-8 share one `EventTime`, and `LIMIT 10` cuts
    /// through the four rows that share the next.
    fn q25_keyed_reference() -> RecordBatch {
        let rows = [
            ("a", 3),
            ("a", 3),
            ("b", 5),
            ("b", 5),
            ("c", 6),
            ("d", 7),
            ("d", 7),
            ("c", 7),
            ("e", 9),
            ("e", 9),
            ("f", 9),
            ("f", 9),
            ("g", 10),
        ];
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("SearchPhrase", DataType::Utf8, false),
                Field::new("__validation_sort_key_0", DataType::Int64, false),
            ])),
            vec![
                Arc::new(StringArray::from(
                    rows.iter().map(|row| row.0).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.1))),
            ],
        )
        .expect("keyed reference batch")
    }

    #[test]
    fn test_unprojected_sort_limit_reads_the_sort_keys_back() {
        let schema = search_phrases(&[]).schema();
        let sort_limit =
            unprojected_sort_limit(Q25, &schema).expect("the sort key is not a result column");
        assert_eq!(
            (sort_limit.limit, sort_limit.offset, &sort_limit.key),
            (10, 0, &SortKeyCells::Appended(1))
        );
        assert_eq!(
            sort_limit.keyed_sql(20),
            r#"SELECT "SearchPhrase", to_timestamp("EventTime") AS __validation_sort_key_0 FROM hits WHERE "SearchPhrase" <> '' ORDER BY to_timestamp("EventTime") LIMIT 20"#
        );
        for sql in [
            r#"SELECT "SearchPhrase" FROM hits ORDER BY "SearchPhrase" LIMIT 10"#,
            r#"SELECT DISTINCT "SearchPhrase" FROM hits ORDER BY to_timestamp("EventTime") LIMIT 10"#,
            r#"SELECT "SearchPhrase" FROM hits ORDER BY to_timestamp("EventTime") LIMIT 10 OFFSET 5"#,
            r#"SELECT "SearchPhrase" FROM hits ORDER BY to_timestamp("EventTime")"#,
            r#"SELECT "SearchPhrase" FROM hits LIMIT 10"#,
        ] {
            assert_eq!(unprojected_sort_limit(sql, &schema), None, "{sql}");
        }
    }

    #[test]
    fn test_unprojected_sort_limit_includes_a_hidden_suffix() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("payload", DataType::Utf8, false),
        ]));
        let sql = "SELECT id, payload FROM t ORDER BY id, hidden LIMIT 2";
        let sort_limit = unprojected_sort_limit(sql, &schema)
            .expect("a hidden ORDER BY suffix must be read back with the result");
        assert_eq!(
            (sort_limit.limit, sort_limit.offset, &sort_limit.key),
            (2, 0, &SortKeyCells::Appended(2))
        );
        let keyed = sort_limit.keyed_sql(4);
        assert!(
            keyed.contains("__validation_sort_key_0") && keyed.contains("__validation_sort_key_1"),
            "{keyed}"
        );
        assert!(keyed.contains("hidden"), "{keyed}");
    }

    #[test]
    fn test_visible_prefix_cutoff_does_not_accept_rows_a_hidden_key_excludes() {
        // ORDER BY id, hidden LIMIT 2: id=1 ties are not free when hidden
        // distinguishes them. Reference kept (p1, p2) at hidden='a'; candidate
        // kept (p3, p4) at hidden='b'.
        let sql = "SELECT id, payload FROM t ORDER BY id, hidden LIMIT 2";
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("payload", DataType::Utf8, false),
        ]));
        let id_payload = |payloads: &[&str]| {
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(vec![1, 1])),
                    Arc::new(StringArray::from(payloads.to_vec())),
                ],
            )
            .expect("id/payload batch")
        };
        let reference = id_payload(&["p1", "p2"]);
        let candidate = id_payload(&["p3", "p4"]);
        let query = Query::new("mixed_sort".into(), sql.into(), false);
        assert!(
            matches!(
                validate_against_reference_batches(
                    &query,
                    std::slice::from_ref(&candidate),
                    std::slice::from_ref(&reference)
                )
                .expect("compare"),
                QueryValidationResult::Fail(_)
            ),
            "visible-key ties must not accept a different payload pair"
        );

        let keyed_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("payload", DataType::Utf8, false),
            Field::new("__validation_sort_key_0", DataType::Int64, false),
            Field::new("__validation_sort_key_1", DataType::Utf8, false),
        ]));
        let keyed = RecordBatch::try_new(
            keyed_schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 1, 1, 1])),
                Arc::new(StringArray::from(vec!["p1", "p2", "p3", "p4"])),
                Arc::new(Int64Array::from(vec![1, 1, 1, 1])),
                Arc::new(StringArray::from(vec!["a", "a", "b", "b"])),
            ],
        )
        .expect("keyed mixed-sort reference");
        assert_eq!(
            unprojected_sort_limit(sql, &schema).map(|s| (s.limit, s.offset, s.key)),
            Some((2, 0, SortKeyCells::Appended(2)))
        );
        assert_eq!(
            validate_against_keyed_reference(
                std::slice::from_ref(&candidate),
                std::slice::from_ref(&keyed),
                2,
                0,
                &SortKeyCells::Appended(2),
                true
            )
            .expect("keyed candidate"),
            Some(QueryValidationResult::Fail(
                QueryValidationFailReason::RowNotAllowedByLimit {
                    row_number: 1,
                    row: r#"[Some("1"), Some("p3")]"#.to_string(),
                }
            ))
        );
        assert_eq!(
            validate_against_keyed_reference(
                &[reference],
                &[keyed],
                2,
                0,
                &SortKeyCells::Appended(2),
                true
            )
            .expect("keyed reference"),
            Some(QueryValidationResult::Pass)
        );
    }

    #[test]
    fn test_visible_prefix_inversion_is_a_sort_order_violation() {
        let sql = "SELECT id, payload FROM t ORDER BY id, hidden LIMIT 2";
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("payload", DataType::Utf8, false),
        ]));
        let rows = |ids: &[i64], payloads: &[&str]| {
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(ids.to_vec())),
                    Arc::new(StringArray::from(payloads.to_vec())),
                ],
            )
            .expect("id/payload batch")
        };
        let reference = rows(&[1, 2], &["a", "b"]);
        let inverted = rows(&[2, 1], &["b", "a"]);
        let query = Query::new("mixed_sort_order".into(), sql.into(), false);
        assert!(
            matches!(
                validate_against_reference_batches(
                    &query,
                    std::slice::from_ref(&inverted),
                    std::slice::from_ref(&reference)
                )
                .expect("compare"),
                QueryValidationResult::Fail(QueryValidationFailReason::SortOrderViolation { .. })
            ),
            "inverting a visible ORDER BY prefix must fail as SortOrderViolation"
        );

        let keyed = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("payload", DataType::Utf8, false),
                Field::new("__validation_sort_key_0", DataType::Int64, false),
                Field::new("__validation_sort_key_1", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["a", "b"])),
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["x", "y"])),
            ],
        )
        .expect("keyed inversion reference");
        assert_eq!(
            validate_against_keyed_reference(
                &[inverted],
                &[keyed],
                2,
                0,
                &SortKeyCells::Appended(2),
                true
            )
            .expect("keyed check"),
            Some(QueryValidationResult::Fail(
                QueryValidationFailReason::RowOutOfSortOrder {
                    row_number: 1,
                    row: r#"[Some("2"), Some("b")]"#.to_string(),
                }
            ))
        );
    }

    #[test]
    fn test_hidden_sort_suffix_requires_the_reference_row_order() {
        // ORDER BY id, hidden LIMIT 2: both rows tie on `id`, and `hidden` puts p1
        // first. The same two rows in the other order are not the answer.
        let sql = "SELECT id, payload FROM t ORDER BY id, hidden LIMIT 2";
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("payload", DataType::Utf8, false),
        ]));
        let rows = |payloads: &[&str]| {
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(vec![1, 1])),
                    Arc::new(StringArray::from(payloads.to_vec())),
                ],
            )
            .expect("id/payload batch")
        };
        let reference = rows(&["p1", "p2"]);
        let swapped = rows(&["p2", "p1"]);
        let query = Query::new("hidden_suffix_order".into(), sql.into(), false);
        assert!(
            matches!(
                validate_against_reference_batches(
                    &query,
                    std::slice::from_ref(&swapped),
                    std::slice::from_ref(&reference)
                )
                .expect("compare"),
                QueryValidationResult::Fail(QueryValidationFailReason::DataMismatch { .. })
            ),
            "without the hidden sort keys, the rows must match the reference row by row"
        );
        assert_eq!(
            validate_against_reference_batches(
                &query,
                std::slice::from_ref(&reference),
                std::slice::from_ref(&reference)
            )
            .expect("compare"),
            QueryValidationResult::Pass
        );

        let keyed = |hidden: [&str; 2]| {
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Int64, false),
                    Field::new("payload", DataType::Utf8, false),
                    Field::new("__validation_sort_key_0", DataType::Int64, false),
                    Field::new("__validation_sort_key_1", DataType::Utf8, false),
                ])),
                vec![
                    Arc::new(Int64Array::from(vec![1, 1])),
                    Arc::new(StringArray::from(vec!["p1", "p2"])),
                    Arc::new(Int64Array::from(vec![1, 1])),
                    Arc::new(StringArray::from(hidden.to_vec())),
                ],
            )
            .expect("keyed hidden-suffix reference")
        };
        assert_eq!(
            validate_against_keyed_reference(
                std::slice::from_ref(&swapped),
                &[keyed(["a", "b"])],
                2,
                0,
                &SortKeyCells::Appended(2),
                true
            )
            .expect("check distinct hidden keys"),
            Some(QueryValidationResult::Fail(
                QueryValidationFailReason::RowOutOfSortOrder {
                    row_number: 1,
                    row: r#"[Some("1"), Some("p2")]"#.to_string(),
                }
            )),
            "`hidden` orders p1 before p2"
        );
        assert_eq!(
            validate_against_keyed_reference(
                &[swapped],
                &[keyed(["a", "a"])],
                2,
                0,
                &SortKeyCells::Appended(2),
                true
            )
            .expect("check tied hidden keys"),
            Some(QueryValidationResult::Pass),
            "rows that tie on every sort key may come in either order"
        );
    }

    #[test]
    fn test_numeric_cells_rounded_one_unit_apart_in_the_last_place_match() {
        // TPC-DS Q98's `revenueratio` for one item: the reference returned 0.000812
        // and DuckDB 0.000813, the same ratio written to six places and rounded two
        // ways. One unit in the sixth place is 0.12% of so small a value.
        let decimals = |scale: i8, values: &[i128]| {
            let mut builder = Decimal128Builder::new()
                .with_precision_and_scale(20, scale)
                .expect("decimal type");
            for value in values {
                builder.append_value(*value);
            }
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "revenueratio",
                    DataType::Decimal128(20, scale),
                    false,
                )])),
                vec![Arc::new(builder.finish())],
            )
            .expect("decimal batch")
        };
        let integers = |values: &[i64]| {
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "count",
                    DataType::Int64,
                    false,
                )])),
                vec![Arc::new(Int64Array::from(values.to_vec()))],
            )
            .expect("integer batch")
        };
        let matches = |expected: &RecordBatch, actual: &RecordBatch| {
            validate_batches_as_strings(expected, actual).expect("compare")
                == QueryValidationResult::Pass
        };
        assert!(
            matches(&decimals(6, &[812]), &decimals(6, &[813])),
            "one unit apart in the sixth place"
        );
        assert!(
            !matches(&decimals(6, &[812]), &decimals(6, &[814])),
            "two units apart is a different value"
        );
        assert!(
            !matches(&decimals(6, &[812]), &decimals(7, &[8135])),
            "a value written to more places does not widen the coarser one"
        );
        assert!(
            !matches(&integers(&[5]), &integers(&[6])),
            "an integer has no rounded place"
        );
    }

    #[test]
    fn test_numeric_strings_match_only_rounding_of_the_same_value() {
        // Rounding one way or the other moves only the last of several decimal places.
        assert!(numeric_strings_match("0.000812", "0.000813"));
        assert!(numeric_strings_match("224.796666", "224.796667"));
        // A coarse last place is not rounding noise: 0.1 against 0.0 is a different
        // answer, and so is a unit that is most of the value.
        for (expected, actual) in [
            ("0.0", "0.1"),
            ("1.0", "1.1"),
            ("0.05", "0.06"),
            ("0.0000", "0.0001"),
        ] {
            assert!(
                !numeric_strings_match(expected, actual),
                "{expected} against {actual}"
            );
        }
        // Infinities and NaN match only themselves, however they are written.
        assert!(numeric_strings_match("inf", "Infinity"));
        assert!(numeric_strings_match("NaN", "nan"));
        for (expected, actual) in [("inf", "1"), ("inf", "-inf"), ("NaN", "1"), ("1", "inf")] {
            assert!(
                !numeric_strings_match(expected, actual),
                "{expected} against {actual}"
            );
        }
    }

    #[test]
    fn test_sort_keys_rounded_one_unit_apart_share_a_position() {
        // TPC-DS Q63 sorts on `avg_monthly_sales`, which the reference returned as
        // 1677.624166 and DuckDB as 1677.624167.
        let sql = "SELECT i_manager_id, sum_sales, avg_monthly_sales FROM t \
                   ORDER BY i_manager_id, avg_monthly_sales, sum_sales LIMIT 2";
        let rows = |averages: [i128; 2]| {
            let mut sums = Decimal128Builder::new()
                .with_precision_and_scale(15, 2)
                .expect("sum type");
            sums.append_value(55_653);
            sums.append_value(72_200);
            let mut monthly = Decimal128Builder::new()
                .with_precision_and_scale(15, 6)
                .expect("average type");
            for average in averages {
                monthly.append_value(average);
            }
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("i_manager_id", DataType::Int64, false),
                    Field::new("sum_sales", DataType::Decimal128(15, 2), false),
                    Field::new("avg_monthly_sales", DataType::Decimal128(15, 6), false),
                ])),
                vec![
                    Arc::new(Int64Array::from(vec![1, 1])),
                    Arc::new(sums.finish()),
                    Arc::new(monthly.finish()),
                ],
            )
            .expect("q63 batch")
        };
        let query = Query::new("tpcds_q63".into(), sql.into(), false);
        let reference = rows([1_677_624_166, 1_677_624_166]);
        assert_eq!(
            validate_against_reference_batches(
                &query,
                &[rows([1_677_624_167, 1_677_624_167])],
                std::slice::from_ref(&reference)
            )
            .expect("compare rounded keys"),
            QueryValidationResult::Pass
        );
        assert_eq!(
            validate_against_reference_batches(
                &query,
                &[rows([1_680_000_000, 1_680_000_000])],
                &[reference]
            )
            .expect("compare different keys"),
            QueryValidationResult::Fail(QueryValidationFailReason::DataMismatch {
                column: "avg_monthly_sales".to_string(),
                row_number: 1,
                expected: "1680.000000".to_string(),
                actual: "1677.624166".to_string(),
            })
        );
    }

    #[test]
    fn test_keyed_reference_accepts_any_order_of_tied_rows_and_any_pick_at_the_cutoff() {
        // The answers the reference query and the Arrow accelerator returned for Q25.
        let reference = search_phrases(&["a", "a", "b", "b", "c", "c", "d", "d", "e", "f"]);
        let accelerated = search_phrases(&["a", "a", "b", "b", "c", "d", "d", "c", "e", "e"]);
        let query = Query::new("clickbench_q25".into(), Q25.into(), false);
        assert!(
            matches!(
                validate_against_reference_batches(
                    &query,
                    std::slice::from_ref(&accelerated),
                    std::slice::from_ref(&reference)
                )
                .expect("compare"),
                QueryValidationResult::Fail(_)
            ),
            "compared row by row, the two correct answers differ"
        );
        for answer in [reference, accelerated] {
            assert_eq!(
                validate_against_keyed_reference(
                    &[answer],
                    &[q25_keyed_reference()],
                    10,
                    0,
                    &SortKeyCells::Appended(1),
                    false
                )
                .expect("check"),
                Some(QueryValidationResult::Pass)
            );
        }
    }

    #[test]
    fn test_keyed_reference_rejects_a_row_past_the_cut_tie_group() {
        let answer = search_phrases(&["a", "a", "b", "b", "c", "d", "d", "c", "e", "g"]);
        assert_eq!(
            validate_against_keyed_reference(
                &[answer],
                &[q25_keyed_reference()],
                10,
                0,
                &SortKeyCells::Appended(1),
                false
            )
            .expect("check"),
            Some(QueryValidationResult::Fail(
                QueryValidationFailReason::RowNotAllowedByLimit {
                    row_number: 10,
                    row: r#"[Some("g")]"#.to_string(),
                }
            ))
        );
    }

    #[test]
    fn test_keyed_reference_rejects_an_answer_missing_a_row_before_the_cut() {
        // Both `d` rows sort before the cut, so row 8 cannot already be an `e` from
        // the tie group the LIMIT cuts through.
        let answer = search_phrases(&["a", "a", "b", "b", "c", "d", "c", "e", "e", "f"]);
        assert_eq!(
            validate_against_keyed_reference(
                &[answer],
                &[q25_keyed_reference()],
                10,
                0,
                &SortKeyCells::Appended(1),
                false
            )
            .expect("check"),
            Some(QueryValidationResult::Fail(
                QueryValidationFailReason::RowOutOfSortOrder {
                    row_number: 8,
                    row: r#"[Some("e")]"#.to_string(),
                }
            ))
        );
    }

    #[test]
    fn test_keyed_reference_rejects_a_pre_cutoff_row_replaced_by_a_cutoff_duplicate() {
        // ORDER BY key: (X,1), (Y,2), (X,3). LIMIT 3 must keep X and Y and one X
        // from the key-3 group. [X, X] is one short; [X, X, X] has no Y even
        // though X also appears in the cutoff group.
        let keyed = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("SearchPhrase", DataType::Utf8, false),
                Field::new("__validation_sort_key_0", DataType::Int64, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["X", "Y", "X"])),
                Arc::new(Int64Array::from(vec![1, 2, 3])),
            ],
        )
        .expect("keyed overlap reference");

        assert_eq!(
            validate_against_keyed_reference(
                &[search_phrases(&["X", "X"])],
                std::slice::from_ref(&keyed),
                3,
                0,
                &SortKeyCells::Appended(1),
                true
            )
            .expect("check short"),
            Some(QueryValidationResult::Fail(
                QueryValidationFailReason::RowCountMismatch {
                    expected: 3,
                    actual: 2
                }
            ))
        );
        assert_eq!(
            validate_against_keyed_reference(
                &[search_phrases(&["X", "X", "X"])],
                std::slice::from_ref(&keyed),
                3,
                0,
                &SortKeyCells::Appended(1),
                true
            )
            .expect("check missing Y"),
            Some(QueryValidationResult::Fail(
                QueryValidationFailReason::RowOutOfSortOrder {
                    row_number: 2,
                    row: r#"[Some("X")]"#.to_string(),
                }
            ))
        );
        assert_eq!(
            validate_against_keyed_reference(
                &[search_phrases(&["X", "Y", "X"])],
                &[keyed],
                3,
                0,
                &SortKeyCells::Appended(1),
                true
            )
            .expect("check complete"),
            Some(QueryValidationResult::Pass)
        );
    }

    #[test]
    fn test_live_oracle_schema_mismatch_is_arity_not_physical_type() {
        let boolean = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Boolean,
                false,
            )])),
            vec![Arc::new(BooleanArray::from(vec![true]))],
        )
        .expect("boolean batch");
        let utf8 = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "other",
                DataType::Utf8,
                false,
            )])),
            vec![Arc::new(StringArray::from(vec!["true"]))],
        )
        .expect("utf8 batch");
        let query = Query::new("schema_q".into(), "SELECT true".into(), false);
        assert_eq!(
            validate_against_reference_batches(
                &query,
                std::slice::from_ref(&utf8),
                std::slice::from_ref(&boolean)
            )
            .expect("direct compare"),
            QueryValidationResult::Pass,
            "engine-vs-engine compare stringifies cells; Boolean true and Utf8 true are equal"
        );

        let two_col = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("a", DataType::Utf8, false),
                Field::new("b", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["true"])),
                Arc::new(StringArray::from(vec!["x"])),
            ],
        )
        .expect("two-column batch");
        assert_eq!(
            validate_against_reference_batches(&query, &[utf8], &[two_col]).expect("arity compare"),
            QueryValidationResult::Fail(QueryValidationFailReason::SchemaMismatch)
        );
    }

    #[test]
    fn test_keyed_reference_rejects_a_short_answer() {
        let answer = search_phrases(&["a", "a", "b", "b", "c", "d", "d", "c", "e"]);
        assert_eq!(
            validate_against_keyed_reference(
                &[answer],
                &[q25_keyed_reference()],
                10,
                0,
                &SortKeyCells::Appended(1),
                false
            )
            .expect("check"),
            Some(QueryValidationResult::Fail(
                QueryValidationFailReason::RowCountMismatch {
                    expected: 10,
                    actual: 9
                }
            ))
        );
    }

    #[test]
    fn test_keyed_reference_needs_the_whole_cut_tie_group() {
        let keyed = q25_keyed_reference().slice(0, 10);
        let answer = search_phrases(&["a", "a", "b", "b", "c", "d", "d", "c", "e", "e"]);
        // Ten rows end inside the tie group the LIMIT cuts, so more must be read...
        assert_eq!(
            validate_against_keyed_reference(
                std::slice::from_ref(&answer),
                std::slice::from_ref(&keyed),
                10,
                0,
                &SortKeyCells::Appended(1),
                false
            )
            .expect("check"),
            None
        );
        // ...unless they are the whole result.
        assert_eq!(
            validate_against_keyed_reference(
                &[answer],
                &[keyed],
                10,
                0,
                &SortKeyCells::Appended(1),
                true
            )
            .expect("check"),
            Some(QueryValidationResult::Pass)
        );
    }

    const Q31: &str = r#"SELECT "SearchEngineID", "ClientIP", COUNT(*) AS c, SUM("IsRefresh"), AVG("ResolutionWidth") FROM hits WHERE "SearchPhrase" <> '' GROUP BY "SearchEngineID", "ClientIP" ORDER BY c DESC LIMIT 10;"#;

    /// `ClickBench` Q31's first thirteen groups over the full `hits` dataset, in
    /// `ORDER BY c DESC` order, each average as the engines rendered it. The tenth
    /// and eleventh groups share the count 1058.
    const Q31_LEADING_ROWS: [(i16, i32, i64, i64, &str); 13] = [
        (2, 1_138_507_705, 1633, 35, "1408.0122473974282"),
        (2, 1_740_861_572, 1331, 28, "1577.945905334335"),
        (2, -807_147_100, 1144, 35, "1553.1984265734266"),
        (2, -497_906_719, 1140, 36, "1543.4140350877192"),
        (2, -1_945_757_555, 1105, 30, "1557.387330316742"),
        (2, -1_870_623_097, 1102, 31, "1555.6588021778584"),
        (2, -631_062_503, 1083, 31, "1581.8171745152354"),
        (2, -465_813_166, 1082, 30, "1541.253234750462"),
        (2, -1_743_596_151, 1080, 24, "1559.8092592592593"),
        (2, -1_125_673_878, 1058, 22, "1587.0"),
        (2, -265_917_476, 1058, 32, "1556.2003780718337"),
        (2, -947_382_330, 1055, 23, "1569.5260663507108"),
        (2, -748_701_126, 1043, 15, "1565.1418983700862"),
    ];

    fn q31_rows(rows: &[(i16, i32, i64, i64, &str)]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("SearchEngineID", DataType::Int16, false),
                Field::new("ClientIP", DataType::Int32, false),
                Field::new("c", DataType::Int64, false),
                Field::new("sum(hits.IsRefresh)", DataType::Int64, true),
                Field::new("avg(hits.ResolutionWidth)", DataType::Float64, true),
            ])),
            vec![
                Arc::new(Int16Array::from_iter_values(rows.iter().map(|row| row.0))),
                Arc::new(arrow::array::Int32Array::from_iter_values(
                    rows.iter().map(|row| row.1),
                )),
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.2))),
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.3))),
                Arc::new(arrow::array::Float64Array::from_iter_values(
                    rows.iter()
                        .map(|row| row.4.parse::<f64>().expect("rendered average")),
                )),
            ],
        )
        .expect("q31 batch")
    }

    /// Q31's answer with `tenth` as its tenth row.
    fn q31_answer(tenth: (i16, i32, i64, i64, &'static str)) -> RecordBatch {
        let mut rows = Q31_LEADING_ROWS[..9].to_vec();
        rows.push(tenth);
        q31_rows(&rows)
    }

    #[test]
    fn test_projected_sort_limit_raises_the_limit_and_drops_the_offset() {
        let sort_limit = projected_sort_limit(Q31, &q31_rows(&[]).schema())
            .expect("every sort term is a result column");
        assert_eq!(
            (sort_limit.limit, sort_limit.offset, &sort_limit.key),
            (10, 0, &SortKeyCells::Returned(vec![2]))
        );
        assert_eq!(
            sort_limit.keyed_sql(20),
            r#"SELECT "SearchEngineID", "ClientIP", COUNT(*) AS c, SUM("IsRefresh"), AVG("ResolutionWidth") FROM hits WHERE "SearchPhrase" <> '' GROUP BY "SearchEngineID", "ClientIP" ORDER BY c DESC LIMIT 20"#
        );

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        for sql in [
            // A sort term the result does not return is `unprojected_sort_limit`'s.
            "SELECT id, v FROM t ORDER BY hidden LIMIT 10",
            // A collation decides which rows tie, and rendered keys cannot show it.
            "SELECT id, v FROM t ORDER BY v COLLATE NOCASE LIMIT 10",
            // A nested row limit leaves the rows past the page unspecified.
            "SELECT id, v FROM (SELECT id, v FROM t LIMIT 5) AS s ORDER BY v LIMIT 2",
            "SELECT id, v FROM t ORDER BY v",
            "SELECT id, v FROM t LIMIT 10",
            "SELECT id, v FROM t ORDER BY v FETCH FIRST 2 ROWS ONLY",
        ] {
            assert_eq!(projected_sort_limit(sql, &schema), None, "{sql}");
        }

        // `ORDER BY 1` is a result column, so the keyed read uses it in place
        // rather than appending a hidden key.
        let positional = projected_sort_limit("SELECT id, v FROM t ORDER BY 1 LIMIT 2", &schema)
            .expect("ordinal 1 is the first result column");
        assert_eq!(
            (positional.limit, positional.offset, &positional.key),
            (2, 0, &SortKeyCells::Returned(vec![0]))
        );
        assert_eq!(
            unprojected_sort_limit("SELECT id, v FROM t ORDER BY 1 LIMIT 2", &schema),
            None,
            "a positional term is not a hidden sort key"
        );
    }

    #[test]
    fn test_keyed_reference_accepts_either_group_tied_at_a_lone_cutoff_row() {
        // ClickBench Q31 over the full `hits` dataset: the reference query kept the
        // tenth group with ClientIP -1125673878 and federated Spice Cloud the one with
        // -265917476. Both count 1058, so both answers are correct, though compared
        // row by row they differ.
        let reference_answer = q31_answer(Q31_LEADING_ROWS[9]);
        let other_answer = q31_answer(Q31_LEADING_ROWS[10]);
        let query = Query::new("clickbench_q31".into(), Q31.into(), false);
        assert!(
            matches!(
                validate_against_reference_batches(
                    &query,
                    std::slice::from_ref(&other_answer),
                    std::slice::from_ref(&reference_answer)
                )
                .expect("compare"),
                QueryValidationResult::Fail(QueryValidationFailReason::DataMismatch { .. })
            ),
            "compared row by row, the two correct answers differ in their tenth row"
        );

        let sort_limit = projected_sort_limit(Q31, &other_answer.schema())
            .expect("every sort term is a result column");
        let keyed_reference = q31_rows(&Q31_LEADING_ROWS);
        let check = |answer: RecordBatch| {
            validate_against_keyed_reference(
                &[answer],
                std::slice::from_ref(&keyed_reference),
                sort_limit.limit,
                sort_limit.offset,
                &sort_limit.key,
                false,
            )
            .expect("check")
        };
        assert_eq!(check(reference_answer), Some(QueryValidationResult::Pass));
        assert_eq!(check(other_answer), Some(QueryValidationResult::Pass));

        // A tenth row with the tied count but a sum neither tied group has is wrong,
        // and so is the group with the next count down.
        let (engine, client_ip, count, _, average) = Q31_LEADING_ROWS[10];
        for tenth in [
            (engine, client_ip, count, 33, average),
            Q31_LEADING_ROWS[11],
        ] {
            assert!(
                matches!(
                    check(q31_answer(tenth)),
                    Some(QueryValidationResult::Fail(
                        QueryValidationFailReason::RowNotAllowedByLimit { row_number: 10, .. }
                    ))
                ),
                "{tenth:?}"
            );
        }
    }

    #[test]
    fn test_keyed_reference_matches_cells_by_the_row_comparison_rule() {
        // An engine that renders the tied row's average one digit shorter returned
        // that row; an average 0.2% away is a different row.
        let keyed_reference = q31_rows(&Q31_LEADING_ROWS);
        let (engine, client_ip, count, sum, _) = Q31_LEADING_ROWS[10];
        let check = |average: &'static str| {
            validate_against_keyed_reference(
                &[q31_answer((engine, client_ip, count, sum, average))],
                std::slice::from_ref(&keyed_reference),
                10,
                0,
                &SortKeyCells::Returned(vec![2]),
                false,
            )
            .expect("check")
        };
        assert_eq!(
            check("1556.200378071834"),
            Some(QueryValidationResult::Pass)
        );
        assert!(matches!(
            check("1559.4"),
            Some(QueryValidationResult::Fail(
                QueryValidationFailReason::RowNotAllowedByLimit { row_number: 10, .. }
            ))
        ));
    }

    #[test]
    fn test_keyed_reference_lets_an_offset_page_start_with_a_row_tied_with_skipped_ones() {
        // ORDER BY v DESC over (1, 10), (2, 5), (3, 5), (4, 1): the `v = 5` group holds
        // positions two and three, so a page that starts or ends inside it may hold
        // either of its rows.
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let rows = |pairs: &[(i64, i64)]| {
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from_iter_values(
                        pairs.iter().map(|pair| pair.0),
                    )),
                    Arc::new(Int64Array::from_iter_values(
                        pairs.iter().map(|pair| pair.1),
                    )),
                ],
            )
            .expect("id/v batch")
        };
        let sort_limit = projected_sort_limit(
            "SELECT id, v FROM t ORDER BY v DESC LIMIT 1 OFFSET 1",
            &schema,
        )
        .expect("v is a result column");
        assert_eq!(
            (sort_limit.limit, sort_limit.offset, &sort_limit.key),
            (1, 1, &SortKeyCells::Returned(vec![1]))
        );
        assert_eq!(
            sort_limit.keyed_sql(3),
            "SELECT id, v FROM t ORDER BY v DESC LIMIT 3"
        );

        let reference = rows(&[(1, 10), (2, 5), (3, 5), (4, 1)]);
        let check = |answer: &[(i64, i64)], limit: usize, offset: usize| {
            validate_against_keyed_reference(
                &[rows(answer)],
                std::slice::from_ref(&reference),
                limit,
                offset,
                &sort_limit.key,
                true,
            )
            .expect("check")
        };
        let pass = Some(QueryValidationResult::Pass);
        // LIMIT 1 OFFSET 1: either `v = 5` row, and nothing else.
        assert_eq!(check(&[(3, 5)], 1, 1), pass);
        assert_eq!(check(&[(2, 5)], 1, 1), pass);
        for wrong in [(1, 10), (4, 1)] {
            assert!(
                matches!(
                    check(&[wrong], 1, 1),
                    Some(QueryValidationResult::Fail(
                        QueryValidationFailReason::RowNotAllowedByLimit { row_number: 1, .. }
                    ))
                ),
                "{wrong:?}"
            );
        }
        // LIMIT 2 OFFSET 1: the whole group, each row once.
        assert_eq!(check(&[(3, 5), (2, 5)], 2, 1), pass);
        assert!(matches!(
            check(&[(3, 5), (3, 5)], 2, 1),
            Some(QueryValidationResult::Fail(
                QueryValidationFailReason::RowNotAllowedByLimit { row_number: 2, .. }
            ))
        ));
        // LIMIT 2 OFFSET 2: one `v = 5` row, then (4, 1) in its own position.
        assert_eq!(check(&[(2, 5), (4, 1)], 2, 2), pass);
        assert!(matches!(
            check(&[(4, 1), (2, 5)], 2, 2),
            Some(QueryValidationResult::Fail(
                QueryValidationFailReason::RowOutOfSortOrder { row_number: 1, .. }
            ))
        ));
        // A fetch may stop at the first row past the group the LIMIT cuts.
        assert!(
            !keyed_reference_cutoff_closed(
                &[rows(&[(1, 10), (2, 5), (3, 5)])],
                1,
                1,
                &sort_limit.key
            )
            .expect("open group")
        );
        assert!(
            keyed_reference_cutoff_closed(
                &[rows(&[(1, 10), (2, 5), (3, 5), (4, 1)])],
                1,
                1,
                &sort_limit.key
            )
            .expect("closed group")
        );
        // Reference rows that end inside the group the LIMIT cuts cannot settle it.
        assert_eq!(
            validate_against_keyed_reference(
                &[rows(&[(3, 5)])],
                &[rows(&[(1, 10), (2, 5), (3, 5)])],
                1,
                1,
                &sort_limit.key,
                false,
            )
            .expect("check partial"),
            None
        );
    }

    #[test]
    fn test_keyed_reference_stops_collecting_once_the_cutoff_group_closes() {
        let full = q25_keyed_reference();
        let needed = full.num_rows();
        let extra = full.slice(needed - 1, 1);
        let extra_count = 8;
        let mut pulls = 0;
        let batches = (0..needed)
            .map(|row| full.slice(row, 1))
            .chain(std::iter::repeat_with(|| extra.slice(0, extra.num_rows())).take(extra_count))
            .inspect(|_| pulls += 1);
        let answer = search_phrases(&["a", "a", "b", "b", "c", "d", "d", "c", "e", "e"]);
        let (result, rows) = decide_from_keyed_reference_batches(
            std::slice::from_ref(&answer),
            batches,
            10,
            0,
            &SortKeyCells::Appended(1),
            1_048_576,
        )
        .expect("decide after the cutoff group closes");
        assert_eq!(result, Some(QueryValidationResult::Pass));
        assert_eq!(
            pulls, needed,
            "the {extra_count} batches after the closing key must not be pulled"
        );
        assert_eq!(rows, needed);
        assert!(
            keyed_reference_cutoff_closed(
                &[full.slice(0, needed)],
                10,
                0,
                &SortKeyCells::Appended(1)
            )
            .expect("closed after the first later key"),
            "row 13's key 10 ends the key-9 group that `LIMIT` 10 cuts"
        );
        assert!(
            !keyed_reference_cutoff_closed(
                &[full.slice(0, needed - 1)],
                10,
                0,
                &SortKeyCells::Appended(1)
            )
            .expect("still open inside the cut group"),
            "twelve rows still end inside the key-9 group"
        );
    }

    #[test]
    fn test_unprojected_sort_limit_refuses_a_collated_sort_key() {
        // A collation decides which rows tie, and rendered keys cannot show it: under
        // `COLLATE NOCASE`, 'a' and 'A' are one tie group but two different strings.
        let schema = Arc::new(Schema::new(vec![Field::new(
            "payload",
            DataType::Utf8,
            false,
        )]));
        for sql in [
            "SELECT payload FROM t ORDER BY hidden COLLATE NOCASE LIMIT 1",
            "SELECT payload FROM t ORDER BY id, lower(hidden COLLATE \"C\") LIMIT 2",
        ] {
            assert_eq!(unprojected_sort_limit(sql, &schema), None, "{sql}");
        }
        assert!(
            unprojected_sort_limit("SELECT payload FROM t ORDER BY hidden LIMIT 1", &schema)
                .is_some(),
            "without the collation the sort key is read back"
        );
    }

    #[test]
    fn test_unprojected_sort_limit_refuses_a_select_alias_sort_key() {
        // A select-list alias is not in scope inside the select list that defines it,
        // so a sort key naming one cannot be read back beside the result. TPC-DS Q36
        // orders by `lochierarchy`, an alias, and by a `CASE` over it that the result
        // does not return.
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        for sql in [
            "SELECT a AS x FROM t ORDER BY x, hidden LIMIT 1",
            "SELECT a AS x FROM t ORDER BY CASE WHEN x = 0 THEN b END LIMIT 1",
        ] {
            assert_eq!(unprojected_sort_limit(sql, &schema), None, "{sql}");
        }
        assert!(
            unprojected_sort_limit("SELECT a AS x FROM t ORDER BY hidden LIMIT 1", &schema)
                .is_some(),
            "a sort key that names no alias is still read back"
        );
    }

    #[test]
    fn test_unprojected_sort_limit_refuses_a_nested_limit() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        // The inner LIMIT keeps unspecified rows, so no reference run fixes which
        // rows sort first.
        assert_eq!(
            unprojected_sort_limit(
                "SELECT x FROM (SELECT x, t FROM s LIMIT 5) AS d ORDER BY t LIMIT 2",
                &schema
            ),
            None
        );
    }

    #[test]
    fn test_sqlite_tpcds_queries_divide_float_casts() {
        // SQLite keeps `CAST(7 AS DECIMAL(15,4))` an integer, so a ratio of two such
        // casts divides as integers and returns 0 (#3238). The SQLite TPC-DS set runs
        // its FLOAT-cast Q49, Q75 and Q90 in their place.
        let queries = crate::queries::get_tpcds_test_queries(
            Some(crate::queries::QueryOverrides::SQLite),
            None,
        );
        for (name, float_cast) in [
            (
                "tpcds_q49",
                "cast(sum(coalesce(wr.wr_return_quantity,0)) as float)",
            ),
            (
                "tpcds_q75",
                "cast(curr_yr.sales_cnt as float)/cast(prev_yr.sales_cnt as float)",
            ),
            ("tpcds_q90", "cast(amc as float)/cast(pmc as float)"),
        ] {
            let matching: Vec<&Query> = queries
                .iter()
                .filter(|query| &*query.name == name)
                .collect();
            assert_eq!(matching.len(), 1, "{name} runs exactly once");
            assert!(
                matching[0].sql.to_lowercase().contains(float_cast),
                "{name} divides FLOAT casts: {}",
                matching[0].sql
            );
        }
    }

    #[test]
    fn test_only_clickbench_q18_takes_the_unordered_limit_subset_check() {
        use crate::queries::{
            QueryOverrides, get_clickbench_test_queries, get_tpcds_test_queries,
            get_tpch_test_queries,
        };
        use std::collections::BTreeSet;

        // The subset check only proves a row a query returns when the row carries
        // its group key. A query that newly qualifies has to be added here on
        // purpose; every other query keeps the direct comparison.
        let overrides = [
            None,
            Some(QueryOverrides::SQLite),
            Some(QueryOverrides::PostgreSQL),
            Some(QueryOverrides::MySQL),
            Some(QueryOverrides::Dremio),
            Some(QueryOverrides::Spark),
            Some(QueryOverrides::ODBCAthena),
            Some(QueryOverrides::ODBCDatabricks),
            Some(QueryOverrides::DuckDB),
            Some(QueryOverrides::DuckDBOnZeroResults),
            Some(QueryOverrides::Snowflake),
            Some(QueryOverrides::Oracle),
            Some(QueryOverrides::IcebergSF1),
            Some(QueryOverrides::IcebergHadoop),
            Some(QueryOverrides::SpicecloudCatalog),
            Some(QueryOverrides::GlueCatalog),
            Some(QueryOverrides::PostgresCatalog),
            Some(QueryOverrides::MysqlCatalog),
            Some(QueryOverrides::MSSqlCatalog),
            Some(QueryOverrides::OracleCatalog),
            Some(QueryOverrides::DucklakeCatalog),
            Some(QueryOverrides::SnowflakeCatalog),
            Some(QueryOverrides::Spicecloud),
            Some(QueryOverrides::DynamoDB),
            Some(QueryOverrides::Arrow),
            Some(QueryOverrides::Cayenne),
            Some(QueryOverrides::Turso),
            Some(QueryOverrides::BigQuery),
            Some(QueryOverrides::ScyllaDB),
            Some(QueryOverrides::ChbenchSkipSlow),
        ];
        let mut subset_checked = BTreeSet::new();
        for query_overrides in overrides {
            for query in get_tpch_test_queries(query_overrides)
                .into_iter()
                .chain(get_tpcds_test_queries(query_overrides, None))
                .chain(get_clickbench_test_queries(query_overrides))
            {
                if unordered_limit(&query.sql).is_some() {
                    subset_checked.insert(query.name.to_string());
                }
            }
        }
        assert_eq!(
            subset_checked,
            BTreeSet::from(["clickbench_q18".to_string()])
        );
    }

    #[test]
    fn test_text_and_binary_cells_compare_equal_only_when_they_hold_the_same_text() {
        let column = |data_type: DataType, array: ArrayRef| {
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("c", data_type, false)])),
                vec![array],
            )
            .expect("single column batch")
        };
        let compare = |left: &RecordBatch, right: &RecordBatch| {
            compare_query_result_batches(
                "q",
                std::slice::from_ref(left),
                std::slice::from_ref(right),
                RowOrder::Multiset,
            )
            .expect("compare")
        };

        let escape_spelled_as_text =
            column(DataType::Utf8, Arc::new(StringArray::from(vec![r"\xff"])));
        let invalid_utf8 = column(
            DataType::LargeBinary,
            Arc::new(LargeBinaryArray::from_iter_values([[0xff_u8].as_slice()])),
        );
        assert!(
            matches!(
                compare(&escape_spelled_as_text, &invalid_utf8),
                QueryValidationResult::Fail(_)
            ),
            r"the four characters `\xff` are not the byte 0xff"
        );

        let text_with_backslash = column(DataType::Utf8, Arc::new(StringArray::from(vec![r"a\b"])));
        let bytes_with_backslash = column(
            DataType::LargeBinary,
            Arc::new(LargeBinaryArray::from_iter_values([br"a\b".as_slice()])),
        );
        assert_eq!(
            compare(&text_with_backslash, &bytes_with_backslash),
            QueryValidationResult::Pass
        );
    }

    #[test]
    fn test_validate_against_reference_detects_value_mismatch() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "v",
            arrow::datatypes::DataType::Int64,
            false,
        )]));
        let actual = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(arrow::array::Int64Array::from(vec![1, 2, 3]))],
        )
        .expect("actual");
        let reference = RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow::array::Int64Array::from(vec![1, 2, 99]))],
        )
        .expect("reference");

        let query = Query::new("tpcds_q64".into(), "SELECT v FROM t".into(), false);
        let result =
            validate_against_reference_batches(&query, &[actual], &[reference]).expect("compare");
        assert!(
            matches!(
                result,
                QueryValidationResult::Fail(QueryValidationFailReason::DataMismatch { .. })
            ),
            "a wrong cell must fail TPC-DS reference validation: {result:?}"
        );
    }

    #[test]
    fn test_row_order_from_sql() {
        assert_eq!(
            row_order_from_sql("SELECT a FROM t ORDER BY a"),
            RowOrder::Preserved
        );
        assert_eq!(
            row_order_from_sql("select a from t order by a desc"),
            RowOrder::Preserved
        );
        assert_eq!(
            row_order_from_sql("SELECT a FROM t GROUP BY a"),
            RowOrder::Multiset
        );
    }

    #[test]
    fn test_timestamp_strings_equivalent() {
        // Same instant, different fractional-second width across engines.
        assert!(timestamp_strings_equivalent(
            "2024-01-01 00:00:00.000000000",
            "2024-01-01 00:00:00.000000"
        ));
        assert!(timestamp_strings_equivalent(
            "2024-01-01 00:00:00.123000000",
            "2024-01-01 00:00:00.123"
        ));
        // A bare second-precision timestamp equals an all-zero fraction.
        assert!(timestamp_strings_equivalent(
            "2024-01-01 00:00:00",
            "2024-01-01 00:00:00.000"
        ));
        assert!(timestamp_strings_equivalent(
            "2024-01-01 00:00:00",
            "2024-01-01 00:00:00"
        ));

        // Genuinely different instants must never be equivalent.
        assert!(!timestamp_strings_equivalent(
            "2024-01-01 00:00:00.100000000",
            "2024-01-01 00:00:00.000000"
        ));
        assert!(!timestamp_strings_equivalent(
            "2024-01-01 00:00:01",
            "2024-01-01 00:00:00"
        ));
        assert!(!timestamp_strings_equivalent(
            "2024-01-02 00:00:00",
            "2024-01-01 00:00:00"
        ));

        // Non-timestamps must not be normalized: a loose "contains `.` and `:`"
        // guard would collapse these trailing zeros and mask a real mismatch.
        assert!(!timestamp_strings_equivalent(
            "http://host/a.100",
            "http://host/a.1"
        ));
        assert!(!timestamp_strings_equivalent("1.100", "1.1"));
        assert!(!timestamp_strings_equivalent("12:30.100", "12:30.1"));
        // Decimals must fall through to the numeric comparison path.
        assert!(!timestamp_strings_equivalent("1.10", "1.1"));
    }

    #[test]
    fn test_date_and_midnight_timestamp_equivalent() {
        // Either argument may be the date.
        assert!(date_and_midnight_timestamp_equivalent(
            "1995-03-05",
            "1995-03-05 00:00:00"
        ));
        assert!(date_and_midnight_timestamp_equivalent(
            "1995-03-05 00:00:00",
            "1995-03-05"
        ));
        // A zero fraction is still midnight.
        assert!(date_and_midnight_timestamp_equivalent(
            "1995-03-05",
            "1995-03-05 00:00:00.000"
        ));

        // Non-midnight times are real differences.
        assert!(!date_and_midnight_timestamp_equivalent(
            "1995-03-05",
            "1995-03-05 06:00:00"
        ));
        assert!(!date_and_midnight_timestamp_equivalent(
            "1995-03-05",
            "1995-03-05 00:00:01"
        ));
        assert!(!date_and_midnight_timestamp_equivalent(
            "1995-03-05",
            "1995-03-05 00:00:00.001"
        ));
        // A different day differs even at midnight.
        assert!(!date_and_midnight_timestamp_equivalent(
            "1995-03-05",
            "1995-03-06 00:00:00"
        ));
        // Two dates, or two timestamps, are left to the caller's other rules.
        assert!(!date_and_midnight_timestamp_equivalent(
            "1995-03-05",
            "1995-03-05"
        ));
        assert!(!date_and_midnight_timestamp_equivalent(
            "1995-03-05 00:00:00",
            "1995-03-05 00:00:00"
        ));
        // Anything outside the emitted format falls through.
        assert!(!date_and_midnight_timestamp_equivalent(
            "1995-3-5",
            "1995-03-05 00:00:00"
        ));
        assert!(!date_and_midnight_timestamp_equivalent(
            "1995-03-05",
            "1995-03-05T00:00:00"
        ));
    }

    #[test]
    fn test_midnight_normalization_is_limited_to_date_columns() {
        fn compare(data_type: DataType, expected: ArrayRef, actual: ArrayRef) -> String {
            let schema: SchemaRef =
                Arc::new(Schema::new(vec![Field::new("value", data_type, false)]));
            let expected = RecordBatch::try_new(Arc::clone(&schema), vec![expected])
                .expect("expected batch should build");
            // The actual batch carries the engine's own type for the column.
            let actual_schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
                "value",
                actual.data_type().clone(),
                false,
            )]));
            let actual = RecordBatch::try_new(actual_schema, vec![actual])
                .expect("actual batch should build");
            format!(
                "{:?}",
                validate_batches_as_strings(&expected, &actual).expect("comparison should run")
            )
        }

        // A date against the same date at midnight passes.
        let result = compare(
            DataType::Date32,
            Arc::new(Date32Array::from(vec![9194])),
            Arc::new(TimestampSecondArray::from(vec![794_361_600])),
        );
        assert!(
            result.contains("Pass"),
            "date vs midnight timestamp: {result}"
        );

        // The same two strings in a text column are still a mismatch.
        let result = compare(
            DataType::Utf8,
            Arc::new(StringArray::from(vec!["1995-03-05"])),
            Arc::new(StringArray::from(vec!["1995-03-05 00:00:00"])),
        );
        assert!(result.contains("DataMismatch"), "text column: {result}");
    }

    #[test]
    fn test_datatype_equivalent_date_and_timestamp() {
        // The answer set infers `Date32`; an Oracle `DATE` arrives as a
        // second-resolution `Timestamp`.
        assert!(datatype_equivalent(
            &DataType::Date32,
            &DataType::Timestamp(TimeUnit::Second, None)
        ));
        assert!(datatype_equivalent(
            &DataType::Timestamp(TimeUnit::Second, None),
            &DataType::Date32
        ));
        // A zoned timestamp against a date stays a mismatch.
        assert!(!datatype_equivalent(
            &DataType::Date32,
            &DataType::Timestamp(TimeUnit::Second, Some("UTC".into()))
        ));
    }
}
