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
use super::{Error, Result};
use arrow::array::{
    Array, ArrayRef, BinaryBuilder, BooleanBuilder, Float64Builder, Int64Builder, ListBuilder,
    NullBuilder, RecordBatch, StringBuilder,
};
use arrow::datatypes::{DataType, SchemaRef};
use aws_sdk_dynamodb::types::AttributeValue;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

pub fn dynamodb_items_to_arrow(
    items: &[HashMap<String, AttributeValue>],
    projected_schema: SchemaRef,
) -> Result<RecordBatch> {
    if items.is_empty() {
        // Return empty batch with correct schema
        let empty_arrays: Vec<ArrayRef> = projected_schema
            .fields()
            .iter()
            .map(|field| create_empty_array(field.data_type()))
            .collect();

        return RecordBatch::try_new(projected_schema, empty_arrays).map_err(|e| {
            Error::ConversionError {
                source: Box::new(e),
            }
        });
    }

    let mut builders = create_builders(&projected_schema, items.len())?;

    for item in items {
        append_item_to_builders(item, &projected_schema, &mut builders)?;
    }

    let arrays = finish_builders(builders, &projected_schema)?;

    RecordBatch::try_new(projected_schema, arrays).map_err(|e| Error::ConversionError {
        source: Box::new(e),
    })
}

fn create_empty_array(data_type: &DataType) -> ArrayRef {
    match data_type {
        DataType::Boolean => Arc::new(BooleanBuilder::new().finish()),
        DataType::Int64 => Arc::new(Int64Builder::new().finish()),
        DataType::Float64 => Arc::new(Float64Builder::new().finish()),
        DataType::Utf8 => Arc::new(StringBuilder::new().finish()),
        DataType::Binary => Arc::new(BinaryBuilder::new().finish()),
        DataType::List(field) => match field.data_type() {
            DataType::Utf8 => {
                let values_builder = StringBuilder::new();
                Arc::new(ListBuilder::new(values_builder).finish())
            }
            DataType::Int64 => {
                let values_builder = Int64Builder::new();
                Arc::new(ListBuilder::new(values_builder).finish())
            }
            DataType::Float64 => {
                let values_builder = Float64Builder::new();
                Arc::new(ListBuilder::new(values_builder).finish())
            }
            DataType::Binary => {
                let values_builder = BinaryBuilder::new();
                Arc::new(ListBuilder::new(values_builder).finish())
            }
            _ => {
                let values_builder = StringBuilder::new();
                Arc::new(ListBuilder::new(values_builder).finish())
            }
        },
        DataType::Null => Arc::new(NullBuilder::new().finish()),
        _ => {
            // Fallback to string for unsupported types
            Arc::new(StringBuilder::new().finish())
        }
    }
}

pub fn attribute_map_to_json(map: &HashMap<String, AttributeValue>) -> Value {
    Value::Object(
        map.iter()
            .map(|(k, v)| (k.clone(), attribute_value_to_json(v)))
            .collect(),
    )
}

fn attribute_value_to_json(av: &AttributeValue) -> Value {
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
        AttributeValue::L(list) => Value::Array(list.iter().map(attribute_value_to_json).collect()),
        AttributeValue::M(map) => attribute_map_to_json(map),
        AttributeValue::Null(_) | _ => Value::Null,
    }
}

type BuilderMap = HashMap<String, Box<dyn ArrayBuilderTrait>>;

trait ArrayBuilderTrait {
    fn append_attribute_value(&mut self, value: Option<&AttributeValue>) -> Result<(), Error>;
    fn finish_builder(self: Box<Self>) -> Result<ArrayRef, Error>;
}

fn create_builders(schema: &SchemaRef, capacity: usize) -> Result<BuilderMap, Error> {
    let mut builders: BuilderMap = HashMap::new();

    for field in schema.fields() {
        let builder: Box<dyn ArrayBuilderTrait> = match field.data_type() {
            DataType::Boolean => Box::new(BooleanArrayBuilder::new(capacity)),
            DataType::Int64 => Box::new(Int64ArrayBuilder::new(capacity)),
            DataType::Float64 => Box::new(Float64ArrayBuilder::new(capacity)),
            DataType::Utf8 => Box::new(StringArrayBuilder::new(capacity)),
            DataType::Binary => Box::new(BinaryArrayBuilder::new(capacity)),
            DataType::List(field) => match field.data_type() {
                DataType::Utf8 => Box::new(StringListArrayBuilder::new(capacity)),
                DataType::Int64 => Box::new(Int64ListArrayBuilder::new(capacity)),
                DataType::Float64 => Box::new(Float64ListArrayBuilder::new(capacity)),
                DataType::Binary => Box::new(BinaryListArrayBuilder::new(capacity)),
                _ => Box::new(StringListArrayBuilder::new(capacity)),
            },
            DataType::Null => Box::new(NullArrayBuilder::new()),
            _ => {
                // Fallback to string for unsupported types
                Box::new(StringArrayBuilder::new(capacity))
            }
        };

        builders.insert(field.name().clone(), builder);
    }

    Ok(builders)
}

fn append_item_to_builders(
    item: &HashMap<String, AttributeValue>,
    schema: &SchemaRef,
    builders: &mut BuilderMap,
) -> Result<(), Error> {
    for field in schema.fields() {
        let field_name = field.name();
        let value = item.get(field_name);

        if let Some(builder) = builders.get_mut(field_name) {
            builder.append_attribute_value(value)?;
        }
    }
    Ok(())
}

fn finish_builders(mut builders: BuilderMap, schema: &SchemaRef) -> Result<Vec<ArrayRef>, Error> {
    let mut arrays = Vec::new();

    for field in schema.fields() {
        let field_name = field.name();
        if let Some(builder) = builders.remove(field_name) {
            arrays.push(builder.finish_builder()?);
        } else {
            return Err(Error::ConversionError {
                source: Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Missing builder for field: {field_name}"),
                )),
            });
        }
    }

    Ok(arrays)
}

// Array Builders

struct BooleanArrayBuilder(BooleanBuilder);
struct Int64ArrayBuilder(Int64Builder);
struct Float64ArrayBuilder(Float64Builder);
struct StringArrayBuilder(StringBuilder);
struct BinaryArrayBuilder(BinaryBuilder);
struct StringListArrayBuilder(ListBuilder<StringBuilder>);
struct Int64ListArrayBuilder(ListBuilder<Int64Builder>);
struct Float64ListArrayBuilder(ListBuilder<Float64Builder>);
struct BinaryListArrayBuilder(ListBuilder<BinaryBuilder>);
struct NullArrayBuilder(NullBuilder);

impl BooleanArrayBuilder {
    fn new(capacity: usize) -> Self {
        Self(BooleanBuilder::with_capacity(capacity))
    }
}

impl ArrayBuilderTrait for BooleanArrayBuilder {
    fn append_attribute_value(&mut self, value: Option<&AttributeValue>) -> Result<(), Error> {
        match value {
            Some(AttributeValue::Bool(b)) => self.0.append_value(*b),
            Some(AttributeValue::Null(_)) | None => self.0.append_null(),
            Some(_) => self.0.append_null(),
        }
        Ok(())
    }

    fn finish_builder(mut self: Box<Self>) -> Result<ArrayRef, Error> {
        Ok(Arc::new(self.0.finish()))
    }
}

impl Int64ArrayBuilder {
    fn new(capacity: usize) -> Self {
        Self(Int64Builder::with_capacity(capacity))
    }
}

impl ArrayBuilderTrait for Int64ArrayBuilder {
    fn append_attribute_value(&mut self, value: Option<&AttributeValue>) -> Result<(), Error> {
        match value {
            Some(AttributeValue::N(n)) => {
                if let Ok(val) = n.parse::<i64>() {
                    self.0.append_value(val);
                } else {
                    self.0.append_null();
                }
            }
            Some(AttributeValue::Null(_)) | None => self.0.append_null(),
            Some(_) => self.0.append_null(),
        }
        Ok(())
    }

    fn finish_builder(mut self: Box<Self>) -> Result<ArrayRef, Error> {
        Ok(Arc::new(self.0.finish()))
    }
}

impl Float64ArrayBuilder {
    fn new(capacity: usize) -> Self {
        Self(Float64Builder::with_capacity(capacity))
    }
}

impl ArrayBuilderTrait for Float64ArrayBuilder {
    fn append_attribute_value(&mut self, value: Option<&AttributeValue>) -> Result<(), Error> {
        match value {
            Some(AttributeValue::N(n)) => {
                if let Ok(val) = n.parse::<f64>() {
                    self.0.append_value(val);
                } else {
                    self.0.append_null();
                }
            }
            Some(AttributeValue::Null(_)) | None => self.0.append_null(),
            Some(_) => self.0.append_null(),
        }
        Ok(())
    }

    fn finish_builder(mut self: Box<Self>) -> Result<ArrayRef, Error> {
        Ok(Arc::new(self.0.finish()))
    }
}

impl StringArrayBuilder {
    fn new(capacity: usize) -> Self {
        Self(StringBuilder::with_capacity(capacity, 256))
    }
}

impl ArrayBuilderTrait for StringArrayBuilder {
    fn append_attribute_value(&mut self, value: Option<&AttributeValue>) -> Result<(), Error> {
        match value {
            Some(AttributeValue::S(s)) => self.0.append_value(s),
            Some(AttributeValue::M(map)) => {
                // Convert map to JSON string
                let json_str = serde_json::to_string(&attribute_map_to_json(map)).map_err(|e| {
                    Error::ConversionError {
                        source: Box::new(e),
                    }
                })?;
                self.0.append_value(&json_str);
            }
            Some(AttributeValue::Null(_)) => self.0.append_null(),
            Some(other) => {
                // Convert other types to string representation
                self.0.append_value(format!("{:?}", other));
            }
            None => self.0.append_null(),
        }
        Ok(())
    }

    fn finish_builder(mut self: Box<Self>) -> Result<ArrayRef, Error> {
        Ok(Arc::new(self.0.finish()))
    }
}

impl BinaryArrayBuilder {
    fn new(capacity: usize) -> Self {
        Self(BinaryBuilder::with_capacity(capacity, 1024))
    }
}

impl ArrayBuilderTrait for BinaryArrayBuilder {
    fn append_attribute_value(&mut self, value: Option<&AttributeValue>) -> Result<(), Error> {
        match value {
            Some(AttributeValue::B(blob)) => self.0.append_value(blob.as_ref()),
            Some(AttributeValue::Null(_)) | None => self.0.append_null(),
            Some(_) => self.0.append_null(),
        }
        Ok(())
    }

    fn finish_builder(mut self: Box<Self>) -> Result<ArrayRef, Error> {
        Ok(Arc::new(self.0.finish()))
    }
}

impl StringListArrayBuilder {
    fn new(capacity: usize) -> Self {
        let values_builder = StringBuilder::with_capacity(capacity * 4, 256);
        Self(ListBuilder::new(values_builder))
    }
}

impl ArrayBuilderTrait for StringListArrayBuilder {
    fn append_attribute_value(&mut self, value: Option<&AttributeValue>) -> Result<(), Error> {
        match value {
            Some(AttributeValue::Ss(string_set)) => {
                for s in string_set {
                    self.0.values().append_value(s);
                }
                self.0.append(true);
            }
            Some(AttributeValue::L(list)) => {
                // DynamoDB lists are heterogeneous - convert all to strings
                for item in list {
                    match item {
                        AttributeValue::S(s) => self.0.values().append_value(s),
                        AttributeValue::N(n) => self.0.values().append_value(n),
                        AttributeValue::Bool(b) => self.0.values().append_value(b.to_string()),
                        AttributeValue::Null(_) => self.0.values().append_value("null"),
                        other => self.0.values().append_value(format!("{:?}", other)),
                    }
                }
                self.0.append(true);
            }
            Some(AttributeValue::Null(_)) | None => self.0.append_null(),
            Some(_) => self.0.append_null(),
        }
        Ok(())
    }

    fn finish_builder(mut self: Box<Self>) -> Result<ArrayRef, Error> {
        Ok(Arc::new(self.0.finish()))
    }
}

impl Int64ListArrayBuilder {
    fn new(capacity: usize) -> Self {
        let values_builder = Int64Builder::with_capacity(capacity * 4);
        Self(ListBuilder::new(values_builder))
    }
}

impl ArrayBuilderTrait for Int64ListArrayBuilder {
    fn append_attribute_value(&mut self, value: Option<&AttributeValue>) -> Result<(), Error> {
        match value {
            Some(AttributeValue::Ns(number_set)) => {
                for n in number_set {
                    if let Ok(val) = n.parse::<i64>() {
                        self.0.values().append_value(val);
                    } else {
                        self.0.values().append_null();
                    }
                }
                self.0.append(true);
            }
            Some(AttributeValue::Null(_)) | None => self.0.append_null(),
            Some(_) => self.0.append_null(),
        }
        Ok(())
    }

    fn finish_builder(mut self: Box<Self>) -> Result<ArrayRef, Error> {
        Ok(Arc::new(self.0.finish()))
    }
}

impl Float64ListArrayBuilder {
    fn new(capacity: usize) -> Self {
        let values_builder = Float64Builder::with_capacity(capacity * 4);
        Self(ListBuilder::new(values_builder))
    }
}

impl ArrayBuilderTrait for Float64ListArrayBuilder {
    fn append_attribute_value(&mut self, value: Option<&AttributeValue>) -> Result<(), Error> {
        match value {
            Some(AttributeValue::Ns(number_set)) => {
                for n in number_set {
                    if let Ok(val) = n.parse::<f64>() {
                        self.0.values().append_value(val);
                    } else {
                        self.0.values().append_null();
                    }
                }
                self.0.append(true);
            }
            Some(AttributeValue::Null(_)) | None => self.0.append_null(),
            Some(_) => self.0.append_null(),
        }
        Ok(())
    }

    fn finish_builder(mut self: Box<Self>) -> Result<ArrayRef, Error> {
        Ok(Arc::new(self.0.finish()))
    }
}

impl BinaryListArrayBuilder {
    fn new(capacity: usize) -> Self {
        let values_builder = BinaryBuilder::with_capacity(capacity * 4, 256);
        Self(ListBuilder::new(values_builder))
    }
}

impl ArrayBuilderTrait for BinaryListArrayBuilder {
    fn append_attribute_value(&mut self, value: Option<&AttributeValue>) -> Result<(), Error> {
        match value {
            Some(AttributeValue::Bs(binary_set)) => {
                for blob in binary_set {
                    self.0.values().append_value(blob.as_ref());
                }
                self.0.append(true);
            }
            Some(AttributeValue::Null(_)) | None => self.0.append_null(),
            Some(_) => self.0.append_null(),
        }
        Ok(())
    }

    fn finish_builder(mut self: Box<Self>) -> Result<ArrayRef, Error> {
        Ok(Arc::new(self.0.finish()))
    }
}

impl NullArrayBuilder {
    fn new() -> Self {
        Self(NullBuilder::new())
    }
}

impl ArrayBuilderTrait for NullArrayBuilder {
    fn append_attribute_value(&mut self, _value: Option<&AttributeValue>) -> Result<(), Error> {
        self.0.append_null();
        Ok(())
    }

    fn finish_builder(mut self: Box<Self>) -> Result<ArrayRef, Error> {
        Ok(Arc::new(self.0.finish()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use aws_sdk_dynamodb::primitives::Blob;
    use aws_sdk_dynamodb::types::AttributeValue;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn av_string(s: &str) -> AttributeValue {
        AttributeValue::S(s.to_string())
    }

    fn av_number(n: &str) -> AttributeValue {
        AttributeValue::N(n.to_string())
    }

    fn av_bool(b: bool) -> AttributeValue {
        AttributeValue::Bool(b)
    }

    fn av_null() -> AttributeValue {
        AttributeValue::Null(true)
    }

    fn av_binary(bytes: Vec<u8>) -> AttributeValue {
        AttributeValue::B(Blob::new(bytes))
    }

    #[test]
    fn test_empty_items() {
        let items: Vec<HashMap<String, AttributeValue>> = vec![];
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int64, true),
        ]));

        let result = dynamodb_items_to_arrow(&items, schema.clone()).unwrap();
        assert_eq!(result.num_rows(), 0);
        assert_eq!(result.num_columns(), 2);
    }

    #[test]
    fn test_simple_types() {
        let mut item = HashMap::new();
        item.insert("name".to_string(), av_string("Alice"));
        item.insert("age".to_string(), av_number("30"));
        item.insert("height".to_string(), av_number("5.6"));
        item.insert("is_active".to_string(), av_bool(true));

        let items = vec![item];

        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int64, true),
            Field::new("height", DataType::Float64, true),
            Field::new("is_active", DataType::Boolean, true),
        ]));

        let result = dynamodb_items_to_arrow(&items, schema).unwrap();
        assert_eq!(result.num_rows(), 1);
        assert_eq!(result.num_columns(), 4);

        // Verify data
        let name_array = result
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(name_array.value(0), "Alice");

        let age_array = result
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(age_array.value(0), 30);

        let height_array = result
            .column(2)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();
        assert_eq!(height_array.value(0), 5.6);

        let is_active_array = result
            .column(3)
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .unwrap();
        assert!(is_active_array.value(0));
    }

    #[test]
    fn test_string_set() {
        let mut item = HashMap::new();
        item.insert(
            "tags".to_string(),
            AttributeValue::Ss(vec![
                "tag1".to_string(),
                "tag2".to_string(),
                "tag3".to_string(),
            ]),
        );

        let items = vec![item];

        let schema = Arc::new(Schema::new(vec![Field::new(
            "tags",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        )]));

        let result = dynamodb_items_to_arrow(&items, schema).unwrap();
        assert_eq!(result.num_rows(), 1);

        let tags_array = result
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::ListArray>()
            .unwrap();

        let arc = tags_array.value(0);
        let values = arc
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();

        assert_eq!(values.len(), 3);
        assert_eq!(values.value(0), "tag1");
        assert_eq!(values.value(1), "tag2");
        assert_eq!(values.value(2), "tag3");
    }

    #[test]
    fn test_number_set_int() {
        let mut item = HashMap::new();
        item.insert(
            "scores".to_string(),
            AttributeValue::Ns(vec!["10".to_string(), "20".to_string(), "30".to_string()]),
        );

        let items = vec![item];

        let schema = Arc::new(Schema::new(vec![Field::new(
            "scores",
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
            true,
        )]));

        let result = dynamodb_items_to_arrow(&items, schema).unwrap();
        assert_eq!(result.num_rows(), 1);

        let scores_array = result
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::ListArray>()
            .unwrap();

        let arc = scores_array.value(0);
        let values = arc
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();

        assert_eq!(values.len(), 3);
        assert_eq!(values.value(0), 10);
        assert_eq!(values.value(1), 20);
        assert_eq!(values.value(2), 30);
    }

    #[test]
    fn test_number_set_float() {
        let mut item = HashMap::new();
        item.insert(
            "ratings".to_string(),
            AttributeValue::Ns(vec![
                "1.5".to_string(),
                "2.5".to_string(),
                "3.14".to_string(),
            ]),
        );

        let items = vec![item];

        let schema = Arc::new(Schema::new(vec![Field::new(
            "ratings",
            DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
            true,
        )]));

        let result = dynamodb_items_to_arrow(&items, schema).unwrap();
        assert_eq!(result.num_rows(), 1);

        let ratings_array = result
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::ListArray>()
            .unwrap();

        let arc = ratings_array.value(0);
        let values = arc
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();

        assert_eq!(values.len(), 3);
        assert_eq!(values.value(0), 1.5);
        assert_eq!(values.value(1), 2.5);
        assert_eq!(values.value(2), 3.14);
    }

    #[test]
    fn test_binary_set() {
        let mut item = HashMap::new();
        item.insert(
            "data".to_string(),
            AttributeValue::Bs(vec![Blob::new(vec![1, 2, 3]), Blob::new(vec![4, 5, 6])]),
        );

        let items = vec![item];

        let schema = Arc::new(Schema::new(vec![Field::new(
            "data",
            DataType::List(Arc::new(Field::new("item", DataType::Binary, true))),
            true,
        )]));

        let result = dynamodb_items_to_arrow(&items, schema).unwrap();
        assert_eq!(result.num_rows(), 1);

        let data_array = result
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::ListArray>()
            .unwrap();

        let arc = data_array.value(0);
        let values = arc
            .as_any()
            .downcast_ref::<arrow::array::BinaryArray>()
            .unwrap();

        assert_eq!(values.len(), 2);
        assert_eq!(values.value(0), &[1, 2, 3]);
        assert_eq!(values.value(1), &[4, 5, 6]);
    }

    #[test]
    fn test_binary_type() {
        let mut item = HashMap::new();
        item.insert("file_data".to_string(), av_binary(vec![1, 2, 3, 4, 5]));

        let items = vec![item];

        let schema = Arc::new(Schema::new(vec![Field::new(
            "file_data",
            DataType::Binary,
            true,
        )]));

        let result = dynamodb_items_to_arrow(&items, schema).unwrap();
        assert_eq!(result.num_rows(), 1);

        let data_array = result
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::BinaryArray>()
            .unwrap();

        assert_eq!(data_array.value(0), &[1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_heterogeneous_list() {
        let mut item = HashMap::new();
        item.insert(
            "mixed".to_string(),
            AttributeValue::L(vec![
                av_string("text"),
                av_number("42"),
                av_bool(true),
                av_null(),
            ]),
        );

        let items = vec![item];

        let schema = Arc::new(Schema::new(vec![Field::new(
            "mixed",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        )]));

        let result = dynamodb_items_to_arrow(&items, schema).unwrap();
        assert_eq!(result.num_rows(), 1);

        let mixed_array = result
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::ListArray>()
            .unwrap();

        let arc = mixed_array.value(0);
        let values = arc
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();

        assert_eq!(values.len(), 4);
        assert_eq!(values.value(0), "text");
        assert_eq!(values.value(1), "42");
        assert_eq!(values.value(2), "true");
        assert_eq!(values.value(3), "null");
    }

    #[test]
    fn test_map_to_json_string() {
        let mut inner_map = HashMap::new();
        inner_map.insert("name".to_string(), av_string("Alice"));
        inner_map.insert("age".to_string(), av_number("30"));

        let mut item = HashMap::new();
        item.insert("user".to_string(), AttributeValue::M(inner_map));

        let items = vec![item];

        let schema = Arc::new(Schema::new(vec![Field::new("user", DataType::Utf8, true)]));

        let result = dynamodb_items_to_arrow(&items, schema).unwrap();
        assert_eq!(result.num_rows(), 1);

        let user_array = result
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();

        let json_str = user_array.value(0);
        let parsed: serde_json::Value = serde_json::from_str(json_str).unwrap();

        assert_eq!(parsed["name"], "Alice");
        assert!(parsed["age"].is_number() || parsed["age"].is_string());
    }

    #[test]
    fn test_null_values() {
        let mut item1 = HashMap::new();
        item1.insert("name".to_string(), av_string("Alice"));
        item1.insert("age".to_string(), av_null());

        let mut item2 = HashMap::new();
        item2.insert("name".to_string(), av_null());
        item2.insert("age".to_string(), av_number("30"));

        let items = vec![item1, item2];

        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int64, true),
        ]));

        let result = dynamodb_items_to_arrow(&items, schema).unwrap();
        assert_eq!(result.num_rows(), 2);

        let name_array = result
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(name_array.value(0), "Alice");
        assert!(name_array.is_null(1));

        let age_array = result
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert!(age_array.is_null(0));
        assert_eq!(age_array.value(1), 30);
    }

    #[test]
    fn test_missing_fields() {
        let mut item1 = HashMap::new();
        item1.insert("name".to_string(), av_string("Alice"));
        item1.insert("age".to_string(), av_number("30"));

        let mut item2 = HashMap::new();
        item2.insert("name".to_string(), av_string("Bob"));
        // age is missing

        let items = vec![item1, item2];

        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int64, true),
        ]));

        let result = dynamodb_items_to_arrow(&items, schema).unwrap();
        assert_eq!(result.num_rows(), 2);

        let age_array = result
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(age_array.value(0), 30);
        assert!(age_array.is_null(1)); // Missing field treated as null
    }

    #[test]
    fn test_number_parsing_edge_cases() {
        let mut item = HashMap::new();
        item.insert("int_pos".to_string(), av_number("42"));
        item.insert("int_neg".to_string(), av_number("-42"));
        item.insert("float_pos".to_string(), av_number("3.14"));
        item.insert("float_neg".to_string(), av_number("-3.14"));
        item.insert("scientific".to_string(), av_number("1.5e10"));
        item.insert("zero".to_string(), av_number("0"));

        let items = vec![item];

        let schema = Arc::new(Schema::new(vec![
            Field::new("int_pos", DataType::Int64, true),
            Field::new("int_neg", DataType::Int64, true),
            Field::new("float_pos", DataType::Float64, true),
            Field::new("float_neg", DataType::Float64, true),
            Field::new("scientific", DataType::Float64, true),
            Field::new("zero", DataType::Int64, true),
        ]));

        let result = dynamodb_items_to_arrow(&items, schema).unwrap();
        assert_eq!(result.num_rows(), 1);

        let int_pos = result
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(int_pos.value(0), 42);

        let int_neg = result
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(int_neg.value(0), -42);

        let float_pos = result
            .column(2)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();
        assert_eq!(float_pos.value(0), 3.14);

        let float_neg = result
            .column(3)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();
        assert_eq!(float_neg.value(0), -3.14);

        let scientific = result
            .column(4)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();
        assert_eq!(scientific.value(0), 1.5e10);

        let zero = result
            .column(5)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(zero.value(0), 0);
    }

    #[test]
    fn test_multiple_items() {
        let mut item1 = HashMap::new();
        item1.insert("id".to_string(), av_string("1"));
        item1.insert("name".to_string(), av_string("Alice"));
        item1.insert("age".to_string(), av_number("30"));

        let mut item2 = HashMap::new();
        item2.insert("id".to_string(), av_string("2"));
        item2.insert("name".to_string(), av_string("Bob"));
        item2.insert("age".to_string(), av_number("25"));

        let mut item3 = HashMap::new();
        item3.insert("id".to_string(), av_string("3"));
        item3.insert("name".to_string(), av_string("Charlie"));
        item3.insert("age".to_string(), av_number("35"));

        let items = vec![item1, item2, item3];

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int64, true),
        ]));

        let result = dynamodb_items_to_arrow(&items, schema).unwrap();
        assert_eq!(result.num_rows(), 3);
        assert_eq!(result.num_columns(), 3);

        let name_array = result
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(name_array.value(0), "Alice");
        assert_eq!(name_array.value(1), "Bob");
        assert_eq!(name_array.value(2), "Charlie");
    }

    #[test]
    fn test_empty_list() {
        let mut item = HashMap::new();
        item.insert("empty_list".to_string(), AttributeValue::L(vec![]));

        let items = vec![item];

        let schema = Arc::new(Schema::new(vec![Field::new(
            "empty_list",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        )]));

        let result = dynamodb_items_to_arrow(&items, schema).unwrap();
        assert_eq!(result.num_rows(), 1);

        let list_array = result
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::ListArray>()
            .unwrap();

        let arc = list_array.value(0);
        let values = arc
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();

        assert_eq!(values.len(), 0);
    }

    #[test]
    fn test_nested_map_in_list() {
        let mut inner_map = HashMap::new();
        inner_map.insert("key".to_string(), av_string("value"));

        let mut item = HashMap::new();
        item.insert(
            "nested".to_string(),
            AttributeValue::L(vec![AttributeValue::M(inner_map)]),
        );

        let items = vec![item];

        let schema = Arc::new(Schema::new(vec![Field::new(
            "nested",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        )]));

        let result = dynamodb_items_to_arrow(&items, schema).unwrap();
        assert_eq!(result.num_rows(), 1);

        let list_array = result
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::ListArray>()
            .unwrap();

        let arc = list_array.value(0);
        let values = arc
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();

        assert_eq!(values.len(), 1);
        // The nested map should be converted to a string representation
        assert!(values.value(0).contains("key") || values.value(0).contains("Map"));
    }

    #[test]
    fn test_all_null_values() {
        let mut item = HashMap::new();
        item.insert("nullable_string".to_string(), av_null());
        item.insert("nullable_int".to_string(), av_null());
        item.insert("nullable_float".to_string(), av_null());
        item.insert("nullable_bool".to_string(), av_null());

        let items = vec![item];

        let schema = Arc::new(Schema::new(vec![
            Field::new("nullable_string", DataType::Utf8, true),
            Field::new("nullable_int", DataType::Int64, true),
            Field::new("nullable_float", DataType::Float64, true),
            Field::new("nullable_bool", DataType::Boolean, true),
        ]));

        let result = dynamodb_items_to_arrow(&items, schema).unwrap();

        let string_array = result
            .column_by_name("nullable_string")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(string_array.len(), 1);
        assert!(string_array.is_null(0));

        let int_array = result
            .column_by_name("nullable_int")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(int_array.len(), 1);
        assert!(int_array.is_null(0));

        let float_array = result
            .column_by_name("nullable_float")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();
        assert_eq!(float_array.len(), 1);
        assert!(float_array.is_null(0));

        let bool_array = result
            .column_by_name("nullable_bool")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .unwrap();
        assert_eq!(bool_array.len(), 1);
        assert!(bool_array.is_null(0));
    }
}
