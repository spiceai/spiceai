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
