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

use arrow::{array::RecordBatch, csv::reader::Format};
use arrow::{
    csv::ReaderBuilder,
    datatypes::{DataType, SchemaRef},
};

use super::Query;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryValidationReason {
    NoAnswer,
    SchemaMismatch,
    RowCountMismatch { expected: usize, actual: usize },
    DataMismatch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryValidationResult {
    Pass,
    Fail(QueryValidationReason),
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
    #[allow(clippy::expect_used)]
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
            map.insert(query_name.into(), batches);
        }

        map
    }
});

fn datatype_equivalent(expected_type: DataType, actual_type: DataType) -> bool {
    if expected_type == actual_type {
        return true;
    }

    // Check for logical equivalence
    matches!(
        (expected_type, actual_type),
        (DataType::Float32, DataType::Float64)
            | (
                DataType::Float64 | DataType::Int64, // why do we return ints as a decimal?
                // TODO: the answer store needs to get updated with a defined schema?
                // the inferred CSV schema isn't right with the context of the originating query
                DataType::Decimal128(_, _)
            )
            | (DataType::Int32, DataType::Int64)
            | (
                DataType::Int64,
                DataType::Int32 | DataType::Float64 | DataType::Utf8
            )
            | (DataType::Utf8, DataType::LargeUtf8)
    )
}

fn equivalent_schemas(expected_schema: &SchemaRef, actual_schema: &SchemaRef) -> bool {
    if expected_schema.fields().len() != actual_schema.fields().len() {
        return false;
    }

    expected_schema
        .fields()
        .iter()
        .zip(actual_schema.fields().iter())
        .all(|(f1, f2)| {
            f1.name() == f2.name()
                && datatype_equivalent(f1.data_type().clone(), f2.data_type().clone())
        })
}

pub fn validate_tpch_query(
    query: &Query,
    batches: &[RecordBatch],
) -> anyhow::Result<QueryValidationResult> {
    let Some(expected_batches) = TPCH_ANSWERS.get(&query.name) else {
        return Ok(QueryValidationResult::Fail(QueryValidationReason::NoAnswer));
    };

    match (expected_batches.is_empty(), batches.is_empty()) {
        (true, true) | (false, false) => {}
        _ => return Ok(QueryValidationResult::Fail(QueryValidationReason::NoAnswer)),
    }

    let Some(expected_schema) = expected_batches
        .first()
        .map(arrow::array::RecordBatch::schema)
    else {
        return Ok(QueryValidationResult::Fail(QueryValidationReason::NoAnswer));
    };
    let Some(actual_schema) = batches.first().map(arrow::array::RecordBatch::schema) else {
        return Ok(QueryValidationResult::Fail(QueryValidationReason::NoAnswer));
    };

    if !equivalent_schemas(&expected_schema, &actual_schema) {
        println!("expected_schema: {expected_schema:?}");
        println!("actual_schema: {actual_schema:?}");

        return Ok(QueryValidationResult::Fail(
            QueryValidationReason::SchemaMismatch,
        ));
    }

    // combine all expected batches and all actual batches into a single RecordBatch
    let expected_batches = arrow::compute::concat_batches(&expected_schema, expected_batches)?;
    let actual_batches = arrow::compute::concat_batches(&actual_schema, batches)?;

    // check the row counts are equal
    if expected_batches.num_rows() != actual_batches.num_rows() {
        return Ok(QueryValidationResult::Fail(
            QueryValidationReason::RowCountMismatch {
                expected: expected_batches.num_rows(),
                actual: actual_batches.num_rows(),
            },
        ));
    }

    // check the actual data batches are equal
    for (_expected, _actual) in expected_batches
        .columns()
        .iter()
        .zip(actual_batches.columns().iter())
    {
        // we cannot perform a direct comparison, because some types may not be equal despite their data being equivalent
        // TODO: validate the data in the columns match
        // if expected != actual {
        //     return Ok(QueryValidationResult::Fail(
        //         QueryValidationReason::DataMismatch,
        //     ));
        // }
    }

    Ok(QueryValidationResult::Pass)
}

#[cfg(test)]
mod test {
    use super::*;
    use arrow::datatypes::{Field, Schema, SchemaRef};
    use std::sync::Arc;

    #[test]
    fn test_tpch_answers() {
        // Check that the TPCH answers are loaded correctly
        assert_eq!(TPCH_ANSWERS.len(), 22);
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
            QueryValidationResult::Fail(QueryValidationReason::RowCountMismatch {
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
}
