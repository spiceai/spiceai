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

//! Validation of benchmark answers. The comparator itself lives in the
//! `result-compare` crate, re-exported here; this module adds the static TPC-H
//! answers and the [`Query`]-based entry points.

use std::{
    collections::BTreeMap,
    io::Seek,
    sync::{Arc, LazyLock},
};

use anyhow::Result;
use arrow::{
    array::RecordBatch,
    csv::{ReaderBuilder, reader::Format},
};
use arrow_tools::schema::schema_difference;

pub use result_compare::*;

use super::Query;

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
    validate_against_reference(&query.name, &query.sql, actual, reference)
}

#[cfg(test)]
mod test {
    use crate::queries::QuerySet;

    use super::*;
    use arrow::{
        array::{
            Array, ArrayRef, BinaryArray, BinaryViewArray, BooleanArray, Date32Array,
            Decimal128Array, Decimal128Builder, Decimal256Builder, Float32Array, Float64Array,
            Int8Array, Int16Array, Int32Array, Int64Array, LargeBinaryArray, StringArray,
            TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
            TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
        },
        datatypes::{DataType, Field, Schema, SchemaRef, i256},
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
            if check.observation_complete() {
                break;
            }
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
            "SELECT TOP 10 a, count(*) FROM t GROUP BY a",
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
    fn test_unordered_limit_subset_stops_once_membership_and_count_are_proven() {
        let sql = UNORDERED_GROUP_LIMIT;
        let returned = user_phrase_counts(&[(1, Some("a"), 3), (2, None, 5)]);
        let first = user_phrase_counts(&[(1, Some("a"), 3), (2, None, 5)]);
        let unordered = unordered_limit(sql).expect("query should have an unordered LIMIT");
        let mut check = UnorderedLimitSubsetCheck::new(&unordered, std::slice::from_ref(&returned))
            .expect("subset check should build");
        assert!(
            !check.observation_complete(),
            "no full-result row has been seen"
        );
        check
            .observe(&first)
            .expect("first full-result batch should be observed");
        assert!(
            check.observation_complete(),
            "both returned rows fill the LIMIT and are in the first batch"
        );
        assert_eq!(check.finish(), QueryValidationResult::Pass);
    }


    #[test]
    fn test_unordered_limit_subset_keeps_reading_when_offset_leaves_the_count_open() {
        let sql =
            r#"SELECT "UserID", "SearchPhrase", COUNT(*) FROM hits GROUP BY 1, 2 LIMIT 1 OFFSET 1"#;
        let returned = user_phrase_counts(&[(1, Some("a"), 3)]);
        let first = user_phrase_counts(&[(1, Some("a"), 3)]);
        let second = user_phrase_counts(&[(2, None, 5)]);
        let unordered = unordered_limit(sql).expect("query should have an unordered LIMIT");
        let mut check = UnorderedLimitSubsetCheck::new(&unordered, std::slice::from_ref(&returned))
            .expect("subset check should build");
        check
            .observe(&first)
            .expect("first full-result batch should be observed");
        assert!(
            !check.observation_complete(),
            "OFFSET 1 plus LIMIT 1 still needs a second full-result row"
        );
        check
            .observe(&second)
            .expect("second full-result batch should be observed");
        assert!(check.observation_complete());
        assert_eq!(check.finish(), QueryValidationResult::Pass);
    }


    #[test]
    fn test_unordered_limit_subset_stops_a_short_result_once_the_full_result_is_too_long() {
        let returned = user_phrase_counts(&[(2, None, 5)]);
        let first = user_phrase_counts(&[(2, None, 5)]);
        let extra = user_phrase_counts(&[(1, Some("a"), 3)]);
        let unordered =
            unordered_limit(UNORDERED_GROUP_LIMIT).expect("query should have an unordered LIMIT");
        let mut check = UnorderedLimitSubsetCheck::new(&unordered, std::slice::from_ref(&returned))
            .expect("subset check should build");
        check
            .observe(&first)
            .expect("first full-result batch should be observed");
        assert!(
            !check.observation_complete(),
            "a one-row answer to LIMIT 2 is only a count failure after a second full-result row"
        );
        check
            .observe(&extra)
            .expect("extra full-result batch should be observed");
        assert!(
            check.observation_complete(),
            "two full-result rows already exceed a one-row LIMIT 2 answer"
        );
        assert_eq!(
            check.finish(),
            QueryValidationResult::Fail(QueryValidationFailReason::RowCountMismatch {
                expected: 2,
                actual: 1
            })
        );
    }


    #[test]
    fn test_unordered_limit_subset_reports_the_count_the_full_result_requires() {
        // LIMIT 3 over five groups that arrive one row per batch. A one-row answer is
        // short from the second full-result row on, but the count it fails against is
        // three, which only the third row settles.
        let sql = r#"SELECT "UserID", "SearchPhrase", COUNT(*) FROM hits GROUP BY 1, 2 LIMIT 3"#;
        let rows = [
            (1, Some("a"), 3),
            (2, None, 5),
            (3, Some("c"), 7),
            (4, Some("d"), 9),
            (5, Some("e"), 11),
        ];
        let full_result: Vec<RecordBatch> =
            rows.iter().map(|row| user_phrase_counts(&[*row])).collect();
        assert_eq!(
            check_unordered_limit_subset(sql, &user_phrase_counts(&[(2, None, 5)]), &full_result),
            QueryValidationResult::Fail(QueryValidationFailReason::RowCountMismatch {
                expected: 3,
                actual: 1
            })
        );

        // An answer longer than its LIMIT fails whatever the full result holds, but
        // the count it should have had still needs the full result.
        let unordered = unordered_limit(sql).expect("query should have an unordered LIMIT");
        let long_answer = user_phrase_counts(&rows[..4]);
        let check = UnorderedLimitSubsetCheck::new(&unordered, std::slice::from_ref(&long_answer))
            .expect("subset check should build");
        assert!(
            !check.observation_complete(),
            "before any full-result row, the expected count is unknown"
        );
        assert_eq!(
            check_unordered_limit_subset(sql, &long_answer, &full_result),
            QueryValidationResult::Fail(QueryValidationFailReason::RowCountMismatch {
                expected: 3,
                actual: 4
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
    fn test_whole_number_cells_match_only_exactly() {
        fn one_cell(data_type: DataType, column: ArrayRef) -> RecordBatch {
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("c", data_type, false)])),
                vec![column],
            )
            .expect("one-cell batch")
        }
        let int64 = |value: i64| one_cell(DataType::Int64, Arc::new(Int64Array::from(vec![value])));
        // DuckDB returns `SUM` over `BIGINT` as a `HUGEINT`, which arrives as a
        // scale-0 decimal.
        let hugeint = |value: i128| {
            one_cell(
                DataType::Decimal128(38, 0),
                Arc::new(
                    Decimal128Array::from(vec![value])
                        .with_precision_and_scale(38, 0)
                        .expect("decimal(38, 0)"),
                ),
            )
        };
        let float64 =
            |value: f64| one_cell(DataType::Float64, Arc::new(Float64Array::from(vec![value])));
        let mismatch = |expected: &RecordBatch, actual: &RecordBatch| {
            matches!(
                validate_batches_as_strings(expected, actual).expect("compare"),
                QueryValidationResult::Fail(QueryValidationFailReason::DataMismatch { .. })
            )
        };

        // TPC-H SF1 Q1's A|F `count_order` against the same count one row off —
        // 0.00007% apart, far inside the float tolerance, and still a wrong answer.
        assert!(mismatch(&int64(1_478_493), &int64(1_478_494)));
        assert!(mismatch(&int64(1_478_493), &hugeint(1_478_494)));
        assert!(mismatch(&hugeint(1_478_494), &int64(1_478_493)));
        assert_eq!(
            validate_batches_as_strings(&int64(1_478_493), &hugeint(1_478_493)).expect("compare"),
            QueryValidationResult::Pass
        );

        // A whole number against a float is a representation the float side may
        // have rounded — SQLite averages and sums in `REAL` — so the tolerance
        // still applies there.
        assert_eq!(
            validate_batches_as_strings(&int64(1_478_493), &float64(1_478_493.000_1))
                .expect("compare"),
            QueryValidationResult::Pass
        );
        assert!(mismatch(&int64(1_478_493), &float64(1_449_049.0)));
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


    /// `LIMIT 0` keeps no rows. The matcher must reject a row even when that
    /// row is in the keyed reference: a raised-LIMIT read-back is not a cutoff
    /// the engine is free to pick from.
    #[test]
    fn test_keyed_reference_limit_zero_rejects_any_row() {
        let keyed = q25_keyed_reference();
        let answer = search_phrases(&["a"]);
        assert_eq!(
            validate_against_keyed_reference(
                &[answer],
                std::slice::from_ref(&keyed),
                0,
                0,
                &SortKeyCells::Appended(1),
                false
            )
            .expect("check"),
            Some(QueryValidationResult::Fail(
                QueryValidationFailReason::RowCountMismatch {
                    expected: 0,
                    actual: 1
                }
            ))
        );
        assert_eq!(
            validate_against_keyed_reference(
                &[],
                &[keyed],
                0,
                0,
                &SortKeyCells::Appended(1),
                false
            )
            .expect("check"),
            Some(QueryValidationResult::Pass)
        );
        assert!(
            keyed_reference_cutoff_closed(&[], 0, 0, &SortKeyCells::Appended(1))
                .expect("empty LIMIT 0 cutoff"),
            "LIMIT 0 has no cutoff group to finish"
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
    fn test_projected_sort_limit_keeps_positional_distinct_and_set_operation_queries() {
        // Raising the LIMIT and dropping the OFFSET leaves the query's own rows, so a
        // positional sort term, DISTINCT and a set operation all keep the read-back,
        // unlike `unprojected_sort_limit`, which appends the sort terms to a SELECT.
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        for (sql, offset, keyed_sql) in [
            (
                "SELECT id, v FROM t ORDER BY 2 DESC LIMIT 5",
                0,
                "SELECT id, v FROM t ORDER BY 2 DESC LIMIT 10",
            ),
            (
                "SELECT DISTINCT id, v FROM t ORDER BY v LIMIT 5",
                0,
                "SELECT DISTINCT id, v FROM t ORDER BY v LIMIT 10",
            ),
            (
                "SELECT id, v FROM t UNION ALL SELECT id, v FROM u ORDER BY v LIMIT 5 OFFSET 3",
                3,
                "SELECT id, v FROM t UNION ALL SELECT id, v FROM u ORDER BY v LIMIT 10",
            ),
        ] {
            let sort_limit =
                projected_sort_limit(sql, &schema).expect("sort term is a result column");
            assert_eq!(
                (sort_limit.limit, sort_limit.offset, &sort_limit.key),
                (5, offset, &SortKeyCells::Returned(vec![1])),
                "{sql}"
            );
            assert_eq!(sort_limit.keyed_sql(10), keyed_sql, "{sql}");
            assert_eq!(unprojected_sort_limit(sql, &schema), None, "{sql}");
        }
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
}
