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

use std::sync::Arc;

use crate::debezium::{arrow::convert_fields_to_arrow_schema, change_event::ChangeEvent};

use super::arrow::to_record_batch;

#[test]
#[allow(clippy::too_many_lines)]
fn parse_arrow_schema() {
    let change_event_json = include_str!("./all_types.json");

    let change_event: ChangeEvent =
        serde_json::from_str(change_event_json).expect("to deserialize change event");

    let after_schema = change_event
        .schema
        .fields
        .into_iter()
        .find(|field| field.field.as_ref().is_some_and(|field| field == "after"))
        .expect("to find after field");

    let fields = after_schema.fields.expect("fields");
    let arrow_schema = convert_fields_to_arrow_schema(&fields).expect("to convert to arrow schema");

    assert_eq!(arrow_schema.fields.len(), 24);
    assert_eq!(arrow_schema.field(0).name(), "id");
    assert_eq!(
        arrow_schema.field(0).data_type(),
        &arrow::datatypes::DataType::Int32
    );
    assert_eq!(arrow_schema.field(1).name(), "int2_column");
    assert_eq!(
        arrow_schema.field(1).data_type(),
        &arrow::datatypes::DataType::Int16
    );
    assert_eq!(arrow_schema.field(2).name(), "int4_column");
    assert_eq!(
        arrow_schema.field(2).data_type(),
        &arrow::datatypes::DataType::Int32
    );
    assert_eq!(arrow_schema.field(3).name(), "int8_column");
    assert_eq!(
        arrow_schema.field(3).data_type(),
        &arrow::datatypes::DataType::Int64
    );
    assert_eq!(arrow_schema.field(4).name(), "float4_column");
    assert_eq!(
        arrow_schema.field(4).data_type(),
        &arrow::datatypes::DataType::Float32
    );
    assert_eq!(arrow_schema.field(5).name(), "float8_column");
    assert_eq!(
        arrow_schema.field(5).data_type(),
        &arrow::datatypes::DataType::Float64
    );
    assert_eq!(arrow_schema.field(6).name(), "text_column");
    assert_eq!(
        arrow_schema.field(6).data_type(),
        &arrow::datatypes::DataType::Utf8
    );
    assert_eq!(arrow_schema.field(7).name(), "varchar_column");
    assert_eq!(
        arrow_schema.field(7).data_type(),
        &arrow::datatypes::DataType::Utf8
    );
    assert_eq!(arrow_schema.field(8).name(), "bpchar_column");
    assert_eq!(
        arrow_schema.field(8).data_type(),
        &arrow::datatypes::DataType::Utf8
    );
    assert_eq!(arrow_schema.field(9).name(), "bool_column");
    assert_eq!(
        arrow_schema.field(9).data_type(),
        &arrow::datatypes::DataType::Boolean
    );
    assert_eq!(arrow_schema.field(10).name(), "numeric_column");
    assert_eq!(
        arrow_schema.field(10).data_type(),
        &arrow::datatypes::DataType::Decimal128(38, 9)
    );
    assert_eq!(arrow_schema.field(11).name(), "timestamp_column");
    assert_eq!(
        arrow_schema.field(11).data_type(),
        &arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
    );
    assert_eq!(arrow_schema.field(12).name(), "timestamptz_column");
    assert_eq!(
        arrow_schema.field(12).data_type(),
        &arrow::datatypes::DataType::Timestamp(
            arrow::datatypes::TimeUnit::Microsecond,
            Some("UTC".into())
        )
    );
    assert_eq!(arrow_schema.field(13).name(), "time_column");
    assert_eq!(
        arrow_schema.field(13).data_type(),
        &arrow::datatypes::DataType::Time64(arrow::datatypes::TimeUnit::Microsecond)
    );
    assert_eq!(arrow_schema.field(14).name(), "timetz_column");
    assert_eq!(
        arrow_schema.field(14).data_type(),
        &arrow::datatypes::DataType::Time64(arrow::datatypes::TimeUnit::Microsecond)
    );
    assert_eq!(arrow_schema.field(15).name(), "date_column");
    assert_eq!(
        arrow_schema.field(15).data_type(),
        &arrow::datatypes::DataType::Date32
    );
    assert_eq!(arrow_schema.field(16).name(), "uuid_column");
    assert_eq!(
        arrow_schema.field(16).data_type(),
        &arrow::datatypes::DataType::Utf8
    );
    assert_eq!(arrow_schema.field(17).name(), "int2_array_column");
    assert_eq!(
        arrow_schema.field(17).data_type(),
        &arrow::datatypes::DataType::List(Arc::new(arrow::datatypes::Field::new(
            "item",
            arrow::datatypes::DataType::Int16,
            true
        )))
    );
    assert_eq!(arrow_schema.field(18).name(), "int4_array_column");
    assert_eq!(
        arrow_schema.field(18).data_type(),
        &arrow::datatypes::DataType::List(Arc::new(arrow::datatypes::Field::new(
            "item",
            arrow::datatypes::DataType::Int32,
            true
        )))
    );
    assert_eq!(arrow_schema.field(19).name(), "int8_array_column");
    assert_eq!(
        arrow_schema.field(19).data_type(),
        &arrow::datatypes::DataType::List(Arc::new(arrow::datatypes::Field::new(
            "item",
            arrow::datatypes::DataType::Int64,
            true
        )))
    );
    assert_eq!(arrow_schema.field(20).name(), "float4_array_column");
    assert_eq!(
        arrow_schema.field(20).data_type(),
        &arrow::datatypes::DataType::List(Arc::new(arrow::datatypes::Field::new(
            "item",
            arrow::datatypes::DataType::Float32,
            true
        )))
    );
    assert_eq!(arrow_schema.field(21).name(), "float8_array_column");
    assert_eq!(
        arrow_schema.field(21).data_type(),
        &arrow::datatypes::DataType::List(Arc::new(arrow::datatypes::Field::new(
            "item",
            arrow::datatypes::DataType::Float64,
            true
        )))
    );
    assert_eq!(arrow_schema.field(22).name(), "text_array_column");
    assert_eq!(
        arrow_schema.field(22).data_type(),
        &arrow::datatypes::DataType::List(Arc::new(arrow::datatypes::Field::new(
            "item",
            arrow::datatypes::DataType::Utf8,
            true
        )))
    );
    assert_eq!(arrow_schema.field(23).name(), "bool_array_column");
    assert_eq!(
        arrow_schema.field(23).data_type(),
        &arrow::datatypes::DataType::List(Arc::new(arrow::datatypes::Field::new(
            "item",
            arrow::datatypes::DataType::Boolean,
            true
        )))
    );
}

#[test]
fn parse_values() {
    let change_event_json = include_str!("./all_types.json");

    let change_event: ChangeEvent =
        serde_json::from_str(change_event_json).expect("to deserialize change event");

    let after_schema = change_event
        .schema
        .fields
        .into_iter()
        .find(|field| field.field.as_ref().is_some_and(|field| field == "after"))
        .expect("to find after field");

    let fields = after_schema.fields.expect("fields");
    let arrow_schema = convert_fields_to_arrow_schema(&fields).expect("to convert to arrow schema");

    let record_batch = to_record_batch(vec![change_event.payload.after], &arrow_schema)
        .expect("to convert to record batch");

    assert_eq!(record_batch.num_columns(), 24);
    assert_eq!(record_batch.num_rows(), 1);
}
