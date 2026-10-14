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

use std::collections::{HashMap, HashSet};

use anyhow::{Result, anyhow};

use arrow::{
    array::{
        Array, AsArray, BinaryArray, BinaryViewArray, BooleanArray, Date32Array, Date64Array,
        Decimal128Array, Decimal256Array, Float32Array, Float64Array, Int8Array, Int16Array,
        Int32Array, Int64Array, LargeBinaryArray, LargeStringArray, RecordBatch, StringArray,
        StringViewArray, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray, TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array,
        UInt64Array,
    },
    datatypes::{DataType, SchemaRef, TimeUnit},
};
use chrono::{DateTime, NaiveDate};

use arrow_tools::schema::schema_difference;

pub mod sort_order;

/// Whether mismatch diagnostics are printed to stdout (the default). A
/// harness that retries comparisons until they converge turns this off and
/// reports the final mismatch itself.
static PRINT_DIAGNOSTICS: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(true);

/// Turns the printing of mismatch diagnostics on or off, for the whole process.
pub fn set_print_diagnostics(enabled: bool) {
    PRINT_DIAGNOSTICS.store(enabled, std::sync::atomic::Ordering::Relaxed);
}

macro_rules! diag {
    ($($arg:tt)*) => {
        if PRINT_DIAGNOSTICS.load(std::sync::atomic::Ordering::Relaxed) {
            println!($($arg)*);
        }
    };
}

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

/// Whether two schemas have the same arity and pairwise equivalent column types,
/// treating types that render to the same values (such as string encodings) as equal.
#[must_use]
pub fn equivalent_schemas(expected_schema: &SchemaRef, actual_schema: &SchemaRef) -> bool {
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
///
/// # Errors
/// Returns an error if the array cannot be downcast to the type its data type names.
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

        // An enum or other dictionary-encoded column compares by its values.
        DataType::Dictionary(_, value_type) => {
            let values = arrow::compute::cast(&array.slice(index, 1), value_type)?;
            array_value_to_string(values.as_ref(), 0)
        }

        // Both sides are typed alike by the time they are compared, so Arrow's
        // own rendering of these is the same for equal values.
        DataType::Time32(_) | DataType::Time64(_) | DataType::Duration(_) | DataType::Interval(_) => {
            Ok(Some(arrow::util::display::array_value_to_string(array, index)?))
        }

        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => {
            let values = match array.data_type() {
                DataType::List(_) => array.as_list::<i32>().value(index),
                DataType::LargeList(_) => array.as_list::<i64>().value(index),
                _ => array.as_fixed_size_list().value(index),
            };
            let elements = (0..values.len())
                .map(|i| {
                    Ok(array_value_to_string(values.as_ref(), i)?
                        .map_or_else(|| "NULL".to_string(), |v| format!("{v:?}")))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(Some(format!("[{}]", elements.join(", "))))
        }

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
    let within_rounding_share =
        diff <= expected_num.abs().max(actual_num.abs()) * ROUNDED_RELATIVE_TOLERANCE;
    match (decimal_places(expected), decimal_places(actual)) {
        (Some(places), Some(actual_places))
            if places == actual_places && places >= ROUNDED_PLACES_MIN =>
        {
            let Ok(exponent) = i32::try_from(places) else {
                return false;
            };
            // `f64` leaves `0.000813 - 0.000812` a hair over one unit.
            diff <= 10_f64.powi(-exponent) * (1.0 + 1e-9) && within_rounding_share
        }
        // One engine carried fewer places than the other — DataFusion keeps six for
        // a decimal quotient where DuckDB returns a full double, as in TPC-DS Q20's
        // `0.000200` against `0.00020044651747561313`. The coarser value is the same
        // answer only if it is the finer one cut to its places, by truncation or by
        // rounding; anything else is a different value at the coarser precision.
        (Some(places), Some(actual_places)) if places != actual_places => {
            let (coarse, coarse_places, fine) = if places < actual_places {
                (expected_num, places, actual_num)
            } else {
                (actual_num, actual_places, expected_num)
            };
            coarse_places >= ROUNDED_PLACES_MIN
                && within_rounding_share
                && is_cut_to_places(coarse, coarse_places, fine)
        }
        _ => false,
    }
}

/// Whether `coarse`, written to `places` decimal places, is `fine` truncated or
/// rounded to that many places.
fn is_cut_to_places(coarse: f64, places: usize, fine: f64) -> bool {
    let Ok(exponent) = i32::try_from(places) else {
        return false;
    };
    let scale = 10_f64.powi(exponent);
    let coarse_units = (coarse * scale).round();
    let fine_units = fine * scale;
    // Both sides are whole numbers of units, so they are equal exactly when
    // they differ by less than half of one.
    let same_units = |units: f64| (coarse_units - units).abs() < 0.5;
    // Truncation and rounding both count: an exact boundary value that `f64`
    // stores a hair low truncates one unit short but still rounds to it.
    same_units(fine_units.trunc()) || same_units(fine_units.round())
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

/// Compares two single-batch answers cell by cell, as rendered strings, in row order.
///
/// # Errors
/// Returns an error if a cell cannot be rendered.
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

/// Whether a column holds exact whole numbers: an integer, or a decimal with no
/// fractional digits — the type `DuckDB` returns for a `SUM` over `BIGINT`.
fn is_integral(data_type: &DataType) -> bool {
    data_type.is_integer()
        || matches!(
            data_type,
            DataType::Decimal32(_, 0)
                | DataType::Decimal64(_, 0)
                | DataType::Decimal128(_, 0)
                | DataType::Decimal256(_, 0)
        )
}

/// Whether two numeric cells may be matched within a tolerance rather than
/// exactly. Two whole-number columns never may: a count or a key has no rounding
/// to absorb, so `1478493` against `1478494` is a wrong answer, not arithmetic
/// noise, however large the value.
fn numeric_tolerance_applies(expected_type: &DataType, actual_type: &DataType) -> bool {
    expected_type.is_numeric() && !(is_integral(expected_type) && is_integral(actual_type))
}

/// Whether two rendered cells hold the same value, by the rule every comparison
/// here uses: the same text, numbers that [`numeric_strings_match`], timestamps
/// that differ only in trailing fractional-second zeros (nanosecond and
/// microsecond engines pad differently), or a date and a timestamp at its
/// midnight. Rendered cells no longer carry their type, so the numeric rule
/// applies only to a numeric expected column that is not a whole number on both
/// sides, and the midnight rule only when one column is a date and the other a
/// timestamp.
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
                || (numeric_tolerance_applies(expected_type, actual_type)
                    && numeric_strings_match(expected, actual))
                || timestamp_strings_equivalent(expected, actual)
                || (is_date_and_timestamp_pair(expected_type, actual_type)
                    && date_and_midnight_timestamp_equivalent(expected, actual))
        }
        (Some(_), None) | (None, Some(_)) => false,
    }
}

/// Validate a query against expected results from a custom query set
/// This is a generic validation function that can be used for custom queries
///
/// # Errors
/// Returns an error if the batches cannot be concatenated or compared.
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
        diag!("Query '{query_name}' schema mismatch:");
        if let Some(diff) = schema_difference(&expected_schema, &actual_schema) {
            diag!("{diff}");
        } else {
            diag!("  expected_schema: {expected_schema:?}");
            diag!("  actual_schema: {actual_schema:?}");
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
        diag!("Query '{query_name}' row count mismatch:");
        diag!("  expected: {}", expected_batches.num_rows());
        diag!("  actual: {}", actual_batches.num_rows());

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
///
/// # Errors
/// Never returns an error today; the `Result` keeps the signature uniform with the other validators.
pub fn validate_row_count(
    query_name: &str,
    actual_batches: &[RecordBatch],
    expected_row_count: usize,
) -> Result<QueryValidationResult> {
    let actual_row_count: usize = actual_batches.iter().map(RecordBatch::num_rows).sum();

    if actual_row_count == expected_row_count {
        Ok(QueryValidationResult::Pass)
    } else {
        diag!("Query '{query_name}' row count mismatch:");
        diag!("  expected: {expected_row_count}");
        diag!("  actual: {actual_row_count}");

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
///
/// # Errors
/// Returns an error if the batches cannot be concatenated, sorted or rendered.
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
        diag!("Query '{query_name}' schema arity mismatch (left vs right):");
        if let Some(diff) = schema_difference(&left_schema, &right_schema) {
            diag!("{diff}");
        } else {
            diag!("  left_schema: {left_schema:?}");
            diag!("  right_schema: {right_schema:?}");
        }
        return Ok(QueryValidationResult::Fail(
            QueryValidationFailReason::SchemaMismatch,
        ));
    }
    if !equivalent_schemas(&left_schema, &right_schema) {
        // Log but continue — positional string compare still validates content.
        if let Some(diff) = schema_difference(&left_schema, &right_schema) {
            diag!(
                "Query '{query_name}' logical schema differs (continuing value compare):\n{diff}"
            );
        }
    }

    let mut left = arrow::compute::concat_batches(&left_schema, left_batches)?;
    let mut right = arrow::compute::concat_batches(&right_schema, right_batches)?;

    if left.num_rows() != right.num_rows() {
        diag!("Query '{query_name}' row count mismatch:");
        diag!("  left: {}", left.num_rows());
        diag!("  right: {}", right.num_rows());
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
        diag!("Query '{query_name}' content mismatch: {reason:?}");
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
                diag!("Query '{query_name}' ({side}) sort order unchecked: {reason}");
                unchecked.push(format!("{side}: {reason}"));
            }
            SortCheck::Violation(v) => {
                diag!(
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

/// Compares `actual` against `reference`, the answer an oracle gave to the same
/// `sql`, choosing the row order the query fixes.
///
/// The rows are compared as a multiset unless the query has a top-level
/// `ORDER BY … LIMIT`; an `ORDER BY` alone is checked against each side's own
/// rows by the sort check, so NULL placement is judged only where the query
/// states it. When the rows match but the query orders by a key it does not
/// return, the answer is re-compared row by row, since the rows alone cannot show
/// that order.
///
/// # Errors
/// Returns an error if the batches cannot be concatenated or compared.
pub fn validate_against_reference(
    query_name: &str,
    sql: &str,
    actual: &[RecordBatch],
    reference: &[RecordBatch],
) -> Result<QueryValidationResult> {
    let order = if has_top_level_order_by(sql) && has_top_level_limit(sql) {
        RowOrder::Preserved
    } else {
        RowOrder::Multiset
    };
    let comparison =
        compare_query_result_batches_with_sort_check(query_name, sql, actual, reference, order)?;
    if comparison.result == QueryValidationResult::Pass
        && let Some(schema) = actual.first().map(RecordBatch::schema)
        && unprojected_sort_limit(sql, &schema).is_some()
    {
        return compare_query_result_batches(query_name, actual, reference, RowOrder::Preserved);
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

    /// Whether later full-result batches cannot change [`Self::finish`]'s verdict
    /// or the row count it reports.
    ///
    /// A schema mismatch is final at once. Otherwise the answer must hold as many
    /// rows as the full result has past the `OFFSET`, up to the `LIMIT`, and that
    /// count is settled only once the full result reaches the `LIMIT` past the
    /// `OFFSET`: from then an answer of any other size fails against it, and an
    /// answer of that size passes as soon as every returned row has been seen.
    /// Until then the stream keeps going, so a failure names the count the full
    /// result requires rather than the rows read so far.
    #[must_use]
    pub fn observation_complete(&self) -> bool {
        if self.schema_mismatch {
            return true;
        }
        if self.full_result_rows.saturating_sub(self.offset) < self.limit {
            return false;
        }
        self.returned.len() != self.limit
            || self
                .copies
                .values()
                .all(|(times_returned, times_seen)| times_seen >= times_returned)
    }

    /// The verdict, once the full-result stream has ended or
    /// [`Self::observation_complete`] is true.
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
            diag!(
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
/// column compares with [`numeric_strings_match`] unless it is a whole number on
/// both sides, where only equal values tie.
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
                    && !(numeric_tolerance_applies(
                        left.schema_ref().field(column.index).data_type(),
                        right.schema_ref().field(column.index).data_type(),
                    ) && numeric_strings_match(left_value, right_value))
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
    use super::*;
    use arrow::datatypes::TimeUnit;


    #[test]
    fn test_numeric_strings_match_only_rounding_of_the_same_value() {
        // Rounding one way or the other moves only the last of several decimal places.
        assert!(numeric_strings_match("0.000812", "0.000813"));
        assert!(numeric_strings_match("224.796666", "224.796667"));
        // A decimal quotient kept to six places against the same ratio as a double
        // (TPC-DS Q20 and Q98, DataFusion against DuckDB), in either order.
        assert!(numeric_strings_match("0.000200", "0.00020044651747561313"));
        assert!(numeric_strings_match("0.0008128277417277396", "0.000812"));
        assert!(numeric_strings_match("0.000813", "0.0008128277417277396"));
        // The coarser value must be the finer one cut to its places — one unit off
        // either cut is a different value, and so is a cut kept to too few places.
        for (expected, actual) in [
            ("0.000201", "0.00020044651747561313"),
            ("0.000199", "0.00020044651747561313"),
            ("0.000814", "0.0008128277417277396"),
            ("0.01", "0.0149"),
            ("0.0001", "0.000199"),
        ] {
            assert!(
                !numeric_strings_match(expected, actual),
                "{expected} against {actual}"
            );
        }
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
