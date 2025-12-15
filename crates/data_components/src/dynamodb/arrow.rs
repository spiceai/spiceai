/*
Copyright 2025 The Spice.ai OSS Authors

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

use super::{Error, Result};
use crate::arrow::struct_builder::StructBuilder;
use arrow::array::{
    BinaryBuilder, BooleanBuilder, Date32Builder, Float64Builder, Int64Builder, ListBuilder,
    NullBuilder, RecordBatch, StringBuilder, TimestampMillisecondBuilder,
};
use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use arrow_array::builder::ArrayBuilder;
use aws_sdk_dynamodb::types::AttributeValue;
use chrono::NaiveDate;
use serde_json::Value;
use std::collections::HashMap;
use util::time_format::{ParsedDateTime, parse_datetime};

/// Maximum recursion depth for nested `DynamoDB` structures during JSON conversion.
/// This limit prevents stack overflow from maliciously crafted deeply nested data.
const MAX_RECURSION_DEPTH: usize = 100;

pub fn dynamodb_items_to_arrow(
    items: &[HashMap<String, AttributeValue>],
    projected_schema: SchemaRef,
    time_format: &str,
) -> Result<RecordBatch> {
    if items.is_empty() {
        return Ok(RecordBatch::new_empty(projected_schema));
    }

    // Create a single StructBuilder instead of HashMap of builders
    let mut struct_builder =
        StructBuilder::from_fields(projected_schema.fields().clone(), items.len());

    for item in items {
        append_item_to_struct_builder(item, &mut struct_builder, time_format)?;
    }

    Ok(struct_builder.finish().into())
}

pub fn append_item_to_struct_builder(
    item: &HashMap<String, AttributeValue>,
    struct_builder: &mut StructBuilder,
    time_format: &str,
) -> Result<(), Error> {
    // Always append a valid struct row
    struct_builder.append(true);

    let fields = struct_builder.fields();

    for (idx, field) in fields.iter().enumerate() {
        let field_name = field.name();
        let value = item.get(field_name);
        let field_builder = struct_builder.field_builder_array(idx);

        append_value_to_builder(field_builder, value, field.data_type(), time_format)?;
    }

    Ok(())
}

#[expect(clippy::too_many_lines)]
fn append_value_to_builder(
    builder: &mut dyn ArrayBuilder,
    value: Option<&AttributeValue>,
    data_type: &DataType,
    time_format: &str,
) -> Result<(), Error> {
    match data_type {
        DataType::Boolean => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<BooleanBuilder>()
                .ok_or_else(|| Error::ConversionError {
                    source: Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Failed to downcast to BooleanBuilder",
                    )),
                })?;
            match value {
                Some(AttributeValue::Bool(v)) => b.append_value(*v),
                _ => b.append_null(),
            }
        }
        DataType::Int64 => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<Int64Builder>()
                .ok_or_else(|| Error::ConversionError {
                    source: Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Failed to downcast to Int64Builder",
                    )),
                })?;
            match value {
                Some(AttributeValue::N(n)) => {
                    if let Ok(i) = n.parse::<i64>() {
                        b.append_value(i);
                    } else {
                        b.append_null();
                    }
                }
                _ => b.append_null(),
            }
        }
        DataType::Float64 => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<Float64Builder>()
                .ok_or_else(|| Error::ConversionError {
                    source: Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Failed to downcast to Float64Builder",
                    )),
                })?;
            match value {
                Some(AttributeValue::N(n)) => {
                    if let Ok(f) = n.parse::<f64>() {
                        b.append_value(f);
                    } else {
                        b.append_null();
                    }
                }
                _ => b.append_null(),
            }
        }
        DataType::Utf8 => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<StringBuilder>()
                .ok_or_else(|| Error::ConversionError {
                    source: Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Failed to downcast to StringBuilder",
                    )),
                })?;
            match value {
                Some(AttributeValue::S(s)) => b.append_value(s),
                Some(AttributeValue::M(m)) => {
                    let json_str =
                        serde_json::to_string(&attribute_map_to_json(m)).map_err(|e| {
                            Error::ConversionError {
                                source: Box::new(e),
                            }
                        })?;
                    b.append_value(&json_str);
                }
                _ => b.append_null(),
            }
        }
        DataType::Binary => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<BinaryBuilder>()
                .ok_or_else(|| Error::ConversionError {
                    source: Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Failed to downcast to BinaryBuilder",
                    )),
                })?;
            match value {
                Some(AttributeValue::B(bytes)) => b.append_value(bytes.as_ref()),
                _ => b.append_null(),
            }
        }
        DataType::List(field) => match field.data_type() {
            DataType::Utf8 => {
                let b = builder
                    .as_any_mut()
                    .downcast_mut::<ListBuilder<Box<dyn ArrayBuilder>>>()
                    .ok_or_else(|| Error::ConversionError {
                        source: Box::new(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "Failed to downcast to ListBuilder",
                        )),
                    })?;

                let values_builder = b
                    .values()
                    .as_any_mut()
                    .downcast_mut::<StringBuilder>()
                    .ok_or_else(|| Error::ConversionError {
                        source: Box::new(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "Failed to downcast values builder to StringBuilder",
                        )),
                    })?;

                match value {
                    Some(AttributeValue::L(list)) => {
                        for item in list {
                            match item {
                                AttributeValue::S(s) | AttributeValue::N(s) => {
                                    values_builder.append_value(s);
                                }
                                AttributeValue::Bool(s) => {
                                    values_builder.append_value(s.to_string());
                                }
                                _ => values_builder.append_null(),
                            }
                        }
                        b.append(true);
                    }
                    Some(AttributeValue::Ss(list)) => {
                        for item in list {
                            values_builder.append_value(item);
                        }
                        b.append(true);
                    }
                    _ => b.append_null(),
                }
            }
            DataType::Int64 => {
                let b = builder
                    .as_any_mut()
                    .downcast_mut::<ListBuilder<Box<dyn ArrayBuilder>>>()
                    .ok_or_else(|| Error::ConversionError {
                        source: Box::new(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "Failed to downcast to ListBuilder",
                        )),
                    })?;

                let values_builder = b
                    .values()
                    .as_any_mut()
                    .downcast_mut::<Int64Builder>()
                    .ok_or_else(|| Error::ConversionError {
                        source: Box::new(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "Failed to downcast values builder to Int64Builder",
                        )),
                    })?;

                match value {
                    Some(AttributeValue::L(list)) => {
                        for item in list {
                            match item {
                                AttributeValue::N(n) => {
                                    if let Ok(i) = n.parse::<i64>() {
                                        values_builder.append_value(i);
                                    } else {
                                        values_builder.append_null();
                                    }
                                }
                                _ => values_builder.append_null(),
                            }
                        }
                        b.append(true);
                    }
                    Some(AttributeValue::Ns(list)) => {
                        for item in list {
                            if let Ok(i) = item.parse::<i64>() {
                                values_builder.append_value(i);
                            } else {
                                values_builder.append_null();
                            }
                        }
                        b.append(true);
                    }
                    _ => b.append_null(),
                }
            }
            DataType::Binary => {
                let b = builder
                    .as_any_mut()
                    .downcast_mut::<ListBuilder<Box<dyn ArrayBuilder>>>()
                    .ok_or_else(|| Error::ConversionError {
                        source: Box::new(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "Failed to downcast to ListBuilder",
                        )),
                    })?;

                let values_builder = b
                    .values()
                    .as_any_mut()
                    .downcast_mut::<BinaryBuilder>()
                    .ok_or_else(|| Error::ConversionError {
                        source: Box::new(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "Failed to downcast values builder to BinaryBuilder",
                        )),
                    })?;

                match value {
                    Some(AttributeValue::Bs(binary_set)) => {
                        for blob in binary_set {
                            values_builder.append_value(blob.as_ref());
                        }
                        b.append(true);
                    }
                    _ => b.append_null(),
                }
            }
            DataType::Float64 => {
                let b = builder
                    .as_any_mut()
                    .downcast_mut::<ListBuilder<Box<dyn ArrayBuilder>>>()
                    .ok_or_else(|| Error::ConversionError {
                        source: Box::new(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "Failed to downcast to ListBuilder",
                        )),
                    })?;

                let values_builder = b
                    .values()
                    .as_any_mut()
                    .downcast_mut::<Float64Builder>()
                    .ok_or_else(|| Error::ConversionError {
                        source: Box::new(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "Failed to downcast values builder to Float64Builder",
                        )),
                    })?;

                match value {
                    Some(AttributeValue::Ns(number_set)) => {
                        for n in number_set {
                            if let Ok(val) = n.parse::<f64>() {
                                values_builder.append_value(val);
                            } else {
                                values_builder.append_null();
                            }
                        }
                        b.append(true);
                    }
                    _ => b.append_null(),
                }
            }
            _ => {
                return Err(Error::UnsupportedType {
                    unsupported_type_name: data_type.to_string(),
                });
            }
        },
        DataType::Struct(fields) => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<StructBuilder>()
                .ok_or_else(|| Error::ConversionError {
                    source: Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Failed to downcast to StructBuilder",
                    )),
                })?;
            match value {
                Some(AttributeValue::M(map)) => {
                    b.append(true);
                    for (idx, field) in fields.iter().enumerate() {
                        let nested_value = map.get(field.name());
                        let nested_builder = b.field_builder_array(idx);
                        append_value_to_builder(
                            nested_builder,
                            nested_value,
                            field.data_type(),
                            time_format,
                        )?;
                    }
                }
                Some(AttributeValue::Null(_)) | None => {
                    b.append(false);

                    // Still need to append nulls to all child builders to keep arrays aligned
                    for idx in 0..fields.len() {
                        let nested_builder = b.field_builder_array(idx);
                        let nested_field = &fields[idx];
                        append_value_to_builder(
                            nested_builder,
                            None,
                            nested_field.data_type(),
                            time_format,
                        )?;
                    }
                }
                _ => {
                    return Err(Error::ConversionError {
                        source: Box::new(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("Expected Map or Null for Struct field, got: {value:?}"),
                        )),
                    });
                }
            }
        }
        DataType::Null => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<NullBuilder>()
                .ok_or_else(|| Error::ConversionError {
                    source: Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Failed to downcast to NullBuilder",
                    )),
                })?;
            b.append_null();
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<TimestampMillisecondBuilder>()
                .ok_or_else(|| Error::ConversionError {
                    source: Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Failed to downcast to TimestampMillisecondBuilder",
                    )),
                })?;
            match value {
                Some(AttributeValue::S(s)) => {
                    if let Some(ts) = parse_datetime(s, time_format) {
                        match ts {
                            ParsedDateTime::Naive(ts) => {
                                b.append_value(ts.and_utc().timestamp_millis());
                            }
                            ParsedDateTime::WithOffset(ts) => b.append_value(ts.timestamp_millis()),
                        }
                    } else {
                        b.append_null();
                    }
                }
                _ => b.append_null(),
            }
        }
        DataType::Date32 => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<Date32Builder>()
                .ok_or_else(|| Error::ConversionError {
                    source: Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Failed to downcast to Date32Builder",
                    )),
                })?;
            match value {
                Some(AttributeValue::S(s)) => {
                    // Parse YYYY-MM-DD string to Date32 (days since epoch)
                    match parse_date_yyyy_mm_dd(s) {
                        Some(days) => b.append_value(days),
                        None => b.append_null(),
                    }
                }
                _ => b.append_null(),
            }
        }
        _ => {
            return Err(Error::UnsupportedType {
                unsupported_type_name: data_type.to_string(),
            });
        }
    }
    Ok(())
}

fn parse_date_yyyy_mm_dd(s: &str) -> Option<i32> {
    // Parse YYYY-MM-DD format
    if s.len() == 10
        && s.chars().filter(|c| *c == '-').count() == 2
        && let Ok(date) = NaiveDate::parse_from_str(s, "%Y-%m-%d")
    {
        // Convert to days since Unix epoch (1970-01-01)
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)?;
        let duration = date.signed_duration_since(epoch);
        return i32::try_from(duration.num_days()).ok();
    }
    None
}

pub fn attribute_map_to_json(map: &HashMap<String, AttributeValue>) -> Value {
    attribute_map_to_json_with_depth(map, 0)
}

fn attribute_map_to_json_with_depth(map: &HashMap<String, AttributeValue>, depth: usize) -> Value {
    if depth > MAX_RECURSION_DEPTH {
        // Return null for excessively nested structures to prevent stack overflow
        return Value::Null;
    }
    Value::Object(
        map.iter()
            .map(|(k, v)| (k.clone(), attribute_value_to_json_with_depth(v, depth)))
            .collect(),
    )
}

fn attribute_value_to_json_with_depth(av: &AttributeValue, depth: usize) -> Value {
    if depth > MAX_RECURSION_DEPTH {
        // Return null for excessively nested structures to prevent stack overflow
        return Value::Null;
    }
    match av {
        AttributeValue::S(s) => Value::String(s.clone()),
        AttributeValue::N(n) => {
            // DynamoDB numbers are strings, so we need to parse them
            if let Ok(i) = n.parse::<i64>() {
                Value::Number(i.into())
            } else if let Ok(f) = n.parse::<f64>() {
                // Need to check if it's a valid JSON number
                serde_json::Number::from_f64(f)
                    .map(Value::Number)
                    .unwrap_or(Value::String(n.clone()))
            } else {
                Value::String(n.clone())
            }
        }
        AttributeValue::Bool(b) => Value::Bool(*b),
        AttributeValue::L(list) => Value::Array(
            list.iter()
                .map(|item| attribute_value_to_json_with_depth(item, depth + 1))
                .collect(),
        ),
        AttributeValue::M(map) => attribute_map_to_json_with_depth(map, depth + 1),
        AttributeValue::Null(_) | _ => Value::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::*;
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use aws_sdk_dynamodb::types::AttributeValue;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn create_test_schema(fields: Vec<Field>) -> SchemaRef {
        Arc::new(Schema::new(fields))
    }

    #[test]
    fn test_empty_items() {
        let schema = create_test_schema(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("count", DataType::Int64, true),
        ]);

        let items: Vec<HashMap<String, AttributeValue>> = vec![];
        let result = dynamodb_items_to_arrow(&items, schema, "2006-01-02T15:04:05.000Z07:00")
            .expect("record_batch");

        assert_eq!(result.num_rows(), 0);
        assert_eq!(result.num_columns(), 2);
    }

    #[test]
    fn test_single_item_all_types() {
        let schema = create_test_schema(vec![
            Field::new("bool_field", DataType::Boolean, true),
            Field::new("int_field", DataType::Int64, true),
            Field::new("float_field", DataType::Float64, true),
            Field::new("string_field", DataType::Utf8, true),
            Field::new("binary_field", DataType::Binary, true),
        ]);

        let mut item = HashMap::new();
        item.insert("bool_field".to_string(), AttributeValue::Bool(true));
        item.insert("int_field".to_string(), AttributeValue::N("42".to_string()));
        item.insert(
            "float_field".to_string(),
            AttributeValue::N("6.14".to_string()),
        );
        item.insert(
            "string_field".to_string(),
            AttributeValue::S("hello".to_string()),
        );
        item.insert(
            "binary_field".to_string(),
            AttributeValue::B(vec![1, 2, 3].into()),
        );

        let items = vec![item];
        let result = dynamodb_items_to_arrow(&items, schema, "2006-01-02T15:04:05.000Z07:00")
            .expect("record_batch");

        assert_eq!(result.num_rows(), 1);
        assert_eq!(result.num_columns(), 5);

        let bool_array = result
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("array");
        assert!(bool_array.value(0));

        let int_array = result
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("array");
        assert_eq!(int_array.value(0), 42);

        let float_array = result
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("array");
        // assert_eq!(float_array.value(0), 6.14);
        assert!((float_array.value(0) - 6.14).abs() < 1e-10);

        let string_array = result
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("array");
        assert_eq!(string_array.value(0), "hello");

        let binary_array = result
            .column(4)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("array");
        assert_eq!(binary_array.value(0), &[1, 2, 3]);
    }

    #[test]
    fn test_null_values() {
        let schema = create_test_schema(vec![
            Field::new("string_field", DataType::Utf8, true),
            Field::new("int_field", DataType::Int64, true),
        ]);

        let mut item = HashMap::new();
        item.insert("string_field".to_string(), AttributeValue::Null(true));
        // int_field is missing entirely

        let items = vec![item];
        let result = dynamodb_items_to_arrow(&items, schema, "2006-01-02T15:04:05.000Z07:00")
            .expect("record_batch");

        assert_eq!(result.num_rows(), 1);

        let string_array = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("array");
        assert!(string_array.is_null(0));

        let int_array = result
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("array");
        assert!(int_array.is_null(0));
    }

    #[test]
    fn test_multiple_items() {
        let schema = create_test_schema(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("value", DataType::Int64, true),
        ]);

        let mut item1 = HashMap::new();
        item1.insert("id".to_string(), AttributeValue::S("1".to_string()));
        item1.insert("value".to_string(), AttributeValue::N("100".to_string()));

        let mut item2 = HashMap::new();
        item2.insert("id".to_string(), AttributeValue::S("2".to_string()));
        item2.insert("value".to_string(), AttributeValue::N("200".to_string()));

        let mut item3 = HashMap::new();
        item3.insert("id".to_string(), AttributeValue::S("3".to_string()));
        item3.insert("value".to_string(), AttributeValue::Null(true));

        let items = vec![item1, item2, item3];
        let result = dynamodb_items_to_arrow(&items, schema, "2006-01-02T15:04:05.000Z07:00")
            .expect("record_batch");

        assert_eq!(result.num_rows(), 3);

        let string_array = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("array");
        assert_eq!(string_array.value(0), "1");
        assert_eq!(string_array.value(1), "2");
        assert_eq!(string_array.value(2), "3");

        let int_array = result
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("array");
        assert_eq!(int_array.value(0), 100);
        assert_eq!(int_array.value(1), 200);
        assert!(int_array.is_null(2));
    }

    #[test]
    fn test_list_of_strings() {
        let schema = create_test_schema(vec![Field::new(
            "tags",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        )]);

        let mut item = HashMap::new();
        item.insert(
            "tags".to_string(),
            AttributeValue::L(vec![
                AttributeValue::S("rust".to_string()),
                AttributeValue::S("arrow".to_string()),
                AttributeValue::S("dynamodb".to_string()),
            ]),
        );

        let items = vec![item];
        let result = dynamodb_items_to_arrow(&items, schema, "2006-01-02T15:04:05.000Z07:00")
            .expect("record_batch");

        assert_eq!(result.num_rows(), 1);

        let list_array = result
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("array");
        let values = list_array.value(0);
        let string_values = values
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("array");

        assert_eq!(string_values.len(), 3);
        assert_eq!(string_values.value(0), "rust");
        assert_eq!(string_values.value(1), "arrow");
        assert_eq!(string_values.value(2), "dynamodb");
    }

    #[test]
    fn test_list_of_integers() {
        let schema = create_test_schema(vec![Field::new(
            "numbers",
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
            true,
        )]);

        let mut item = HashMap::new();
        item.insert(
            "numbers".to_string(),
            AttributeValue::L(vec![
                AttributeValue::N("1".to_string()),
                AttributeValue::N("2".to_string()),
                AttributeValue::N("3".to_string()),
            ]),
        );

        let items = vec![item];
        let result = dynamodb_items_to_arrow(&items, schema, "2006-01-02T15:04:05.000Z07:00")
            .expect("record_batch");

        let list_array = result
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("array");
        let values = list_array.value(0);
        let int_values = values.as_any().downcast_ref::<Int64Array>().expect("array");

        assert_eq!(int_values.len(), 3);
        assert_eq!(int_values.value(0), 1);
        assert_eq!(int_values.value(1), 2);
        assert_eq!(int_values.value(2), 3);
    }

    #[test]
    fn test_list_of_floats() {
        let schema = create_test_schema(vec![Field::new(
            "prices",
            DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
            true,
        )]);

        let mut item = HashMap::new();
        item.insert(
            "prices".to_string(),
            AttributeValue::Ns(vec![
                "9.99".to_string(),
                "19.99".to_string(),
                "29.99".to_string(),
            ]),
        );

        let items = vec![item];
        let result = dynamodb_items_to_arrow(&items, schema, "2006-01-02T15:04:05.000Z07:00")
            .expect("record_batch");

        let list_array = result
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("array");
        let values = list_array.value(0);
        let float_values = values
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("array");

        assert_eq!(float_values.len(), 3);
        assert!((float_values.value(0) - 9.99).abs() < 1e-10);
        assert!((float_values.value(1) - 19.99).abs() < 1e-10);
        assert!((float_values.value(2) - 29.99).abs() < 1e-10);
    }

    #[test]
    fn test_list_of_binary() {
        let schema = create_test_schema(vec![Field::new(
            "blobs",
            DataType::List(Arc::new(Field::new("item", DataType::Binary, true))),
            true,
        )]);

        let mut item = HashMap::new();
        item.insert(
            "blobs".to_string(),
            AttributeValue::Bs(vec![vec![1, 2, 3].into(), vec![4, 5, 6].into()]),
        );

        let items = vec![item];
        let result = dynamodb_items_to_arrow(&items, schema, "2006-01-02T15:04:05.000Z07:00")
            .expect("record_batch");

        let list_array = result
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("array");
        let values = list_array.value(0);
        let binary_values = values
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("array");

        assert_eq!(binary_values.len(), 2);
        assert_eq!(binary_values.value(0), &[1, 2, 3]);
        assert_eq!(binary_values.value(1), &[4, 5, 6]);
    }

    #[test]
    fn test_nested_struct() {
        let schema = create_test_schema(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new(
                "address",
                DataType::Struct(
                    vec![
                        Field::new("city", DataType::Utf8, true),
                        Field::new("zip", DataType::Utf8, true),
                    ]
                    .into(),
                ),
                true,
            ),
        ]);

        let mut address_map = HashMap::new();
        address_map.insert("city".to_string(), AttributeValue::S("Seattle".to_string()));
        address_map.insert("zip".to_string(), AttributeValue::S("98101".to_string()));

        let mut item = HashMap::new();
        item.insert("name".to_string(), AttributeValue::S("John".to_string()));
        item.insert("address".to_string(), AttributeValue::M(address_map));

        let items = vec![item];
        let result = dynamodb_items_to_arrow(&items, schema, "2006-01-02T15:04:05.000Z07:00")
            .expect("Failed to create record batch");

        assert_eq!(result.num_rows(), 1);

        // Check name column
        let name_array = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Failed to downcast to StringArray");
        assert_eq!(name_array.value(0), "John");

        // Check address struct
        let struct_array = result
            .column(1)
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("Failed to downcast to StructArray");

        // Verify struct is not null
        assert!(!struct_array.is_null(0));

        let city_array = struct_array
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Failed to downcast city to StringArray");
        let zip_array = struct_array
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Failed to downcast zip to StringArray");

        assert_eq!(city_array.value(0), "Seattle");
        assert_eq!(zip_array.value(0), "98101");
    }

    #[test]
    fn test_nested_struct_with_null() {
        let schema = create_test_schema(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new(
                "address",
                DataType::Struct(
                    vec![
                        Field::new("city", DataType::Utf8, true),
                        Field::new("zip", DataType::Utf8, true),
                    ]
                    .into(),
                ),
                true,
            ),
        ]);

        let mut item = HashMap::new();
        item.insert("name".to_string(), AttributeValue::S("Jane".to_string()));
        // No address field - should be null

        let items = vec![item];
        let result = dynamodb_items_to_arrow(&items, schema, "2006-01-02T15:04:05.000Z07:00")
            .expect("Failed to create record batch");

        assert_eq!(result.num_rows(), 1);

        let name_array = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Failed to downcast to StringArray");
        assert_eq!(name_array.value(0), "Jane");

        let struct_array = result
            .column(1)
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("Failed to downcast to StructArray");

        // Verify struct is null
        assert!(struct_array.is_null(0));
    }

    #[test]
    fn test_nested_struct_with_partial_fields() {
        let schema = create_test_schema(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new(
                "address",
                DataType::Struct(
                    vec![
                        Field::new("city", DataType::Utf8, true),
                        Field::new("zip", DataType::Utf8, true),
                    ]
                    .into(),
                ),
                true,
            ),
        ]);

        let mut address_map = HashMap::new();
        address_map.insert(
            "city".to_string(),
            AttributeValue::S("Portland".to_string()),
        );
        // zip is missing

        let mut item = HashMap::new();
        item.insert("name".to_string(), AttributeValue::S("Bob".to_string()));
        item.insert("address".to_string(), AttributeValue::M(address_map));

        let items = vec![item];
        let result = dynamodb_items_to_arrow(&items, schema, "2006-01-02T15:04:05.000Z07:00")
            .expect("Failed to create record batch");

        assert_eq!(result.num_rows(), 1);

        let struct_array = result
            .column(1)
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("Failed to downcast to StructArray");

        let city_array = struct_array
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Failed to downcast city to StringArray");
        let zip_array = struct_array
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Failed to downcast zip to StringArray");

        assert_eq!(city_array.value(0), "Portland");
        assert!(zip_array.is_null(0)); // zip should be null
    }

    #[test]
    fn test_timestamp_millisecond() {
        let schema = create_test_schema(vec![Field::new(
            "created_at",
            DataType::Timestamp(TimeUnit::Millisecond, Some(Arc::from("UTC"))),
            true,
        )]);

        let mut item = HashMap::new();
        item.insert(
            "created_at".to_string(),
            AttributeValue::S("2024-01-15T10:30:00.123Z".to_string()),
        );

        let items = vec![item];
        let result = dynamodb_items_to_arrow(&items, schema, "2006-01-02T15:04:05.000Z07:00")
            .expect("record_batch");

        let ts_array = result
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .expect("array");
        assert!(!ts_array.is_null(0));
        assert_eq!(ts_array.value(0), 1_705_314_600_123);
    }

    #[test]
    fn test_naive_timestamp() {
        let schema = create_test_schema(vec![Field::new(
            "created_at",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        )]);

        let mut item = HashMap::new();
        item.insert(
            "created_at".to_string(),
            AttributeValue::S("2024-01-15T10:30:00".to_string()),
        );

        let items = vec![item];
        let result =
            dynamodb_items_to_arrow(&items, schema, "2006-01-02T15:04:05").expect("record_batch");

        let ts_array = result
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .expect("array");
        assert!(!ts_array.is_null(0));
        assert_eq!(ts_array.value(0), 1_705_314_600_000);
    }

    #[test]
    fn test_date32() {
        let schema = create_test_schema(vec![Field::new("birth_date", DataType::Date32, true)]);

        let mut item = HashMap::new();
        item.insert(
            "birth_date".to_string(),
            AttributeValue::S("2024-01-15".to_string()),
        );

        let items = vec![item];
        let result = dynamodb_items_to_arrow(&items, schema, "2006-01-02T15:04:05.000Z07:00")
            .expect("record_batch");

        let date_array = result
            .column(0)
            .as_any()
            .downcast_ref::<Date32Array>()
            .expect("array");
        assert!(!date_array.is_null(0));
        // Value would depend on parse_date_yyyy_mm_dd implementation
    }

    #[test]
    fn test_invalid_number_parsing() {
        let schema = create_test_schema(vec![
            Field::new("int_field", DataType::Int64, true),
            Field::new("float_field", DataType::Float64, true),
        ]);

        let mut item = HashMap::new();
        item.insert(
            "int_field".to_string(),
            AttributeValue::N("not_a_number".to_string()),
        );
        item.insert(
            "float_field".to_string(),
            AttributeValue::N("invalid".to_string()),
        );

        let items = vec![item];
        let result = dynamodb_items_to_arrow(&items, schema, "2006-01-02T15:04:05.000Z07:00")
            .expect("record_batch");

        let int_array = result
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("array");
        assert!(int_array.is_null(0));

        let float_array = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("array");
        assert!(float_array.is_null(0));
    }

    #[test]
    fn test_type_mismatch_becomes_null() {
        let schema = create_test_schema(vec![Field::new("int_field", DataType::Int64, true)]);

        let mut item = HashMap::new();
        // Providing a string when expecting a number
        item.insert(
            "int_field".to_string(),
            AttributeValue::S("hello".to_string()),
        );

        let items = vec![item];
        let result = dynamodb_items_to_arrow(&items, schema, "2006-01-02T15:04:05.000Z07:00")
            .expect("record_batch");

        let int_array = result
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("array");
        assert!(int_array.is_null(0));
    }

    #[test]
    fn test_null_type() {
        let schema = create_test_schema(vec![Field::new("null_field", DataType::Null, true)]);

        let mut item = HashMap::new();
        item.insert("null_field".to_string(), AttributeValue::Null(true));

        let items = vec![item];
        let result = dynamodb_items_to_arrow(&items, schema, "2006-01-02T15:04:05.000Z07:00")
            .expect("record_batch");

        assert_eq!(result.num_rows(), 1);
        let null_array = result
            .column(0)
            .as_any()
            .downcast_ref::<NullArray>()
            .expect("array");
        // NullArrays do not have a null buffer, and therefore always
        // return false for is_null.
        assert!(!null_array.is_null(0));
    }

    #[test]
    fn test_empty_list() {
        let schema = create_test_schema(vec![Field::new(
            "tags",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        )]);

        let mut item = HashMap::new();
        item.insert("tags".to_string(), AttributeValue::L(vec![]));

        let items = vec![item];
        let result = dynamodb_items_to_arrow(&items, schema, "2006-01-02T15:04:05.000Z07:00")
            .expect("record_batch");

        let list_array = result
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("array");
        let values = list_array.value(0);
        assert_eq!(values.len(), 0);
    }

    #[test]
    fn test_list_with_nulls() {
        let schema = create_test_schema(vec![Field::new(
            "values",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        )]);

        let mut item = HashMap::new();
        item.insert(
            "values".to_string(),
            AttributeValue::L(vec![
                AttributeValue::S("valid".to_string()),
                AttributeValue::Null(true),
                AttributeValue::S("also_valid".to_string()),
            ]),
        );

        let items = vec![item];
        let result = dynamodb_items_to_arrow(&items, schema, "2006-01-02T15:04:05.000Z07:00")
            .expect("record_batch");

        let list_array = result
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("array");
        let values = list_array.value(0);
        let string_values = values
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("array");

        assert_eq!(string_values.len(), 3);
        assert_eq!(string_values.value(0), "valid");
        assert!(string_values.is_null(1));
        assert_eq!(string_values.value(2), "also_valid");
    }

    #[test]
    fn test_attribute_map_to_json_max_depth() {
        // Build a deeply nested structure that exceeds MAX_RECURSION_DEPTH
        let depth = super::MAX_RECURSION_DEPTH + 10;

        // Start with the innermost map
        let mut current_map = HashMap::new();
        current_map.insert("leaf".to_string(), AttributeValue::S("value".to_string()));

        // Wrap it in nested maps
        for i in 0..depth {
            let mut outer_map = HashMap::new();
            outer_map.insert(format!("level_{i}"), AttributeValue::M(current_map));
            current_map = outer_map;
        }

        // Should not panic - returns null for too-deep structures
        let result = attribute_map_to_json(&current_map);

        // The result should be a valid JSON value (we truncate at depth limit)
        assert!(result.is_object());
    }

    #[test]
    fn test_attribute_map_to_json_within_limit() {
        // Build a nested structure within the limit
        let depth = 50; // Well within MAX_RECURSION_DEPTH of 100

        // Start with the innermost map
        let mut current_map = HashMap::new();
        current_map.insert("leaf".to_string(), AttributeValue::S("value".to_string()));

        // Wrap it in nested maps
        for i in 0..depth {
            let mut outer_map = HashMap::new();
            outer_map.insert(format!("level_{i}"), AttributeValue::M(current_map));
            current_map = outer_map;
        }

        let result = attribute_map_to_json(&current_map);

        // Should be valid object
        assert!(result.is_object());

        // Navigate to the leaf value
        let mut current = &result;
        for i in (0..depth).rev() {
            let key = format!("level_{i}");
            current = current.get(&key).expect("expected nested object");
        }
        assert_eq!(
            current.get("leaf"),
            Some(&Value::String("value".to_string()))
        );
    }
}
