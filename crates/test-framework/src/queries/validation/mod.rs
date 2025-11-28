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
    collections::BTreeMap,
    io::Seek,
    sync::{Arc, LazyLock},
};

use anyhow::{Result, anyhow};

use arrow::{
    array::{
        Array, BooleanArray, Date32Array, Date64Array, Decimal128Array, Float32Array, Float64Array,
        Int8Array, Int16Array, Int32Array, Int64Array, LargeStringArray, RecordBatch, StringArray,
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

use super::Query;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryValidationFailReason {
    NoExpectedAnswer,
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

fn datatype_equivalent(expected_type: &DataType, actual_type: &DataType) -> bool {
    if expected_type == actual_type {
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
                | (
                    DataType::Float64 | DataType::Int64,
                    DataType::Decimal128(_, _)
                )
                | (DataType::Int32, DataType::Int64)
                | (
                    DataType::Int64,
                    DataType::Int32
                        | DataType::Int8
                        | DataType::Float64
                        | DataType::Utf8
                        | DataType::LargeUtf8
                        | DataType::Utf8View
                )
                | (DataType::Utf8, DataType::LargeUtf8)
                | (DataType::LargeUtf8, DataType::Utf8)
                | (DataType::Date32, DataType::Date64)
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
/// ```
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
#[expect(clippy::too_many_lines)]
pub fn array_value_to_string(array: &dyn Array, index: usize) -> Result<Option<String>> {
    if array.len() <= index {
        return Err(anyhow!("Index out of bounds: {index} >= {}", array.len()));
    }

    if array.is_null(index) {
        return Ok(None);
    }

    match array.data_type() {
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
        DataType::Utf8 => downcast_and_stringify!(array, index, StringArray),
        DataType::LargeUtf8 => downcast_and_stringify!(array, index, LargeStringArray),
        DataType::Utf8View => downcast_and_stringify!(array, index, StringViewArray),
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

            match (expected_val, actual_val) {
                (None, None) => {}
                (Some(val), None) => {
                    return Ok(QueryValidationResult::Fail(
                        QueryValidationFailReason::DataMismatch {
                            column: column_name,
                            row_number: row + 1, // indexes are 0-based, counts are 1-based
                            expected: format!("{val:?}"),
                            actual: "None".to_string(),
                        },
                    ));
                }
                (None, Some(val)) => {
                    return Ok(QueryValidationResult::Fail(
                        QueryValidationFailReason::DataMismatch {
                            column: column_name,
                            row_number: row + 1, // indexes are 0-based, counts are 1-based
                            expected: "None".to_string(),
                            actual: format!("{val:?}"),
                        },
                    ));
                }
                (Some(expected_val), Some(actual_val)) => {
                    if expected_val != actual_val {
                        if data_type.is_numeric() {
                            let delta = 0.05;

                            if let (Ok(expected_num), Ok(actual_num)) =
                                (expected_val.parse::<f64>(), actual_val.parse::<f64>())
                            {
                                let diff = (expected_num - actual_num).abs();
                                let tolerance = (expected_num.abs() * delta).max(1e-12); // avoid zero-multiplied tolerance
                                if diff <= tolerance {
                                    continue; // numeric match within tolerance
                                }
                            }
                        }

                        return Ok(QueryValidationResult::Fail(
                            QueryValidationFailReason::DataMismatch {
                                column: column_name,
                                row_number: row + 1, // indexes are 0-based, counts are 1-based
                                expected: format!("{expected_val:?}"),
                                actual: format!("{actual_val:?}"),
                            },
                        ));
                    }
                }
            }
        }
    }

    Ok(QueryValidationResult::Pass)
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
        println!("expected_schema: {expected_schema:?}");
        println!("actual_schema: {actual_schema:?}");

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
        println!("  expected_schema: {expected_schema:?}");
        println!("  actual_schema: {actual_schema:?}");

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

#[cfg(test)]
mod test {
    use crate::queries::QuerySet;

    use super::*;
    use arrow::{
        array::{
            Decimal128Builder, Float32Array, Int8Array, Int16Array, UInt8Array, UInt16Array,
            UInt32Array, UInt64Array,
        },
        datatypes::{Field, Schema, SchemaRef},
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

    #[test]
    fn test_correct_answer_wrong_type() {
        // Use the correct answer, but a different datatype
        // Q22 from CSV, cntrycode is Utf8. Query returns it as Int64
        let query = QuerySet::Tpch
            .get_queries(None)
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

    #[test]
    fn test_wrong_answers() {
        // Use the wrong answer and validate it fails
        let query = QuerySet::Tpch
            .get_queries(None)
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
}
