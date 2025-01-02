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

use std::sync::Arc;

use arrow::array::{
    Array, ArrayAccessor, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, ListArray, RecordBatch, StringArray,
    Time64MicrosecondArray, TimestampMicrosecondArray,
};

use crate::debezium::{arrow::convert_fields_to_arrow_schema, change_event::ChangeEvent};

use super::arrow::to_record_batch;

#[test]
#[allow(clippy::too_many_lines)]
fn parse_arrow_schema() {
    let change_event_json = include_str!("./all_types.json");

    let change_event: ChangeEvent =
        serde_json::from_str(change_event_json).expect("to deserialize change event");

    let fields = change_event
        .get_schema_fields()
        .expect("to get schema fields");
    let arrow_schema = convert_fields_to_arrow_schema(fields).expect("to convert to arrow schema");

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
#[allow(clippy::float_cmp)]
fn parse_values() {
    let change_event_json = include_str!("./all_types.json");

    let change_event: ChangeEvent =
        serde_json::from_str(change_event_json).expect("to deserialize change event");

    let fields = change_event
        .get_schema_fields()
        .expect("to get schema fields");
    let arrow_schema = convert_fields_to_arrow_schema(fields).expect("to convert to arrow schema");

    let record_batch = to_record_batch(vec![change_event.payload.after], &arrow_schema)
        .expect("to convert to record batch");

    assert_eq!(record_batch.num_columns(), 24);
    assert_eq!(record_batch.num_rows(), 1);

    let mut i: usize = 0;
    assert_eq!(val::<i32, Int32Array>(&record_batch, &mut i), 1);
    assert_eq!(val::<i16, Int16Array>(&record_batch, &mut i), 1);
    assert_eq!(val::<i32, Int32Array>(&record_batch, &mut i), 2);
    assert_eq!(val::<i64, Int64Array>(&record_batch, &mut i), 3);
    assert_eq!(val::<f32, Float32Array>(&record_batch, &mut i), 4f32);
    assert_eq!(val::<f64, Float64Array>(&record_batch, &mut i), 5f64);
    assert_eq!(val::<&str, StringArray>(&record_batch, &mut i), "test");
    assert_eq!(val::<&str, StringArray>(&record_batch, &mut i), "test");
    assert_eq!(val::<&str, StringArray>(&record_batch, &mut i), "test");
    assert!(val::<bool, BooleanArray>(&record_batch, &mut i));
    assert_eq!(
        val::<i128, Decimal128Array>(&record_batch, &mut i),
        6_000_000_000_i128
    );
    assert_eq!(
        val::<i64, TimestampMicrosecondArray>(&record_batch, &mut i),
        1_719_400_371_219_026
    );
    assert_eq!(
        val::<i64, TimestampMicrosecondArray>(&record_batch, &mut i),
        1_719_367_971_219_026
    );
    assert_eq!(
        val::<i64, Time64MicrosecondArray>(&record_batch, &mut i),
        40_371_219_026
    );
    assert_eq!(
        val::<i64, Time64MicrosecondArray>(&record_batch, &mut i),
        7_971_219_026
    );
    assert_eq!(val::<i32, Date32Array>(&record_batch, &mut i), 19900);
    assert_eq!(
        val::<&str, StringArray>(&record_batch, &mut i),
        "86f701e6-f8f8-4dc4-8cbd-ed84280b5b84"
    );
    assert_eq!(val_list::<i16, Int16Array>(&record_batch, i, 0), 1);
    assert_eq!(val_list::<i16, Int16Array>(&record_batch, i, 1), 2);
    i += 1;
    assert_eq!(val_list::<i32, Int32Array>(&record_batch, i, 0), 3);
    assert_eq!(val_list::<i32, Int32Array>(&record_batch, i, 1), 4);
    i += 1;
    assert_eq!(val_list::<i64, Int64Array>(&record_batch, i, 0), 5);
    assert_eq!(val_list::<i64, Int64Array>(&record_batch, i, 1), 6);
    i += 1;
    assert_eq!(val_list::<f32, Float32Array>(&record_batch, i, 0), 7f32);
    assert_eq!(val_list::<f32, Float32Array>(&record_batch, i, 1), 8f32);
    i += 1;
    assert_eq!(val_list::<f64, Float64Array>(&record_batch, i, 0), 9f64);
    assert_eq!(val_list::<f64, Float64Array>(&record_batch, i, 1), 10f64);
    i += 1;
    assert_eq!(val_list_str(&record_batch, i, 0), "test1");
    assert_eq!(val_list_str(&record_batch, i, 1), "test2");
    i += 1;
    assert!(val_list::<bool, BooleanArray>(&record_batch, i, 0));
    assert!(!val_list::<bool, BooleanArray>(&record_batch, i, 1));
}

fn val<'a, 'b, T, A: 'static>(rb: &'a RecordBatch, col: &mut usize) -> T
where
    &'b A: ArrayAccessor<Item = T> + Array,
    'a: 'b,
{
    let array_ref = rb
        .column(*col)
        .as_any()
        .downcast_ref::<A>()
        .expect("to downcast");

    *col += 1;

    array_ref.value(0)
}

fn val_list<'a, 'b, T, A: 'static>(rb: &'a RecordBatch, col: usize, list_row: usize) -> T
where
    for<'c> &'c A: ArrayAccessor<Item = T> + Array,
    T: 'a,
{
    let list_array = rb
        .column(col)
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("to downcast");

    let list_values = list_array.value(0);
    let list = list_values
        .as_any()
        .downcast_ref::<A>()
        .expect("to downcast");

    assert!(!list.is_empty());
    list.value(list_row)
}

fn val_list_str<'a>(rb: &'a RecordBatch, col: usize, list_row: usize) -> &'a str {
    let list_array = rb
        .column(col)
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("to downcast");

    let list_values = list_array.value(0);
    let list = list_values
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("to downcast");

    assert!(!list.is_empty());
    let value = list.value(list_row);
    // Safety: We know the value is valid for the lifetime of the record batch
    unsafe { std::mem::transmute::<&str, &'a str>(value) }
}
