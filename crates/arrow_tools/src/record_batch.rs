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

use arrow::{
    array::{new_null_array, Array, ArrayBuilder, ArrayRef, BooleanBuilder, Int32Builder, Int64Builder, RecordBatch, StringArray, StringBuilder, StructBuilder},
    compute::cast,
    datatypes::{DataType, Fields, SchemaRef}, error::ArrowError,
};
use serde_json::Value;
use snafu::prelude::*;
use std::sync::Arc;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error converting record batch: {source}",))]
    UnableToConvertRecordBatch { source: arrow::error::ArrowError },

    #[snafu(display("Field is not nullable: {field}"))]
    FieldNotNullable { field: String },
}

/// Cast a given record batch into a new record batch with the given schema.
///
/// # Errors
///
/// This function will return an error if the record batch cannot be cast.
#[allow(clippy::needless_pass_by_value)]
pub fn try_cast_to(record_batch: RecordBatch, schema: SchemaRef) -> Result<RecordBatch> {
    let existing_schema = record_batch.schema();

    // When schema is superset of the existing schema, including a new column, and nullable column,
    // return a new RecordBatch to reflect the change
    if schema.contains(&existing_schema) {
        return record_batch
            .with_schema(schema)
            .context(UnableToConvertRecordBatchSnafu);
    }

    let cols = schema
        .fields()
        .into_iter()
        .map(|field| {
            if let (Ok(existing_field), Some(column)) = (
                record_batch.schema().field_with_name(field.name()),
                record_batch.column_by_name(field.name()),
            ) {
                if field.contains(existing_field) {
                    Ok(Arc::clone(column))
                } else {
                    {                        
                        match (column.data_type(), field.data_type()) {
                            (DataType::Utf8, DataType::Struct(fields)) => {
                                return cast_string_to_struct(&*Arc::clone(column), fields)
                                    .context(UnableToConvertRecordBatchSnafu);

                            },
                            _ => {
                                return cast(&*Arc::clone(column), field.data_type())
                                .context(UnableToConvertRecordBatchSnafu);
                            }

                        }
                    }
                }
            } else if field.is_nullable() {
                Ok(new_null_array(field.data_type(), record_batch.num_rows()))
            } else {
                FieldNotNullableSnafu {
                    field: field.name(),
                }
                .fail()
            }
        })
        .collect::<Result<Vec<Arc<dyn Array>>>>()?;

    RecordBatch::try_new(schema, cols).context(UnableToConvertRecordBatchSnafu)
}

fn cast_string_to_struct(array: &dyn Array, struct_fields: &Fields) -> Result<ArrayRef, ArrowError> {

    let string_array = array.as_any().downcast_ref::<StringArray>()
        .ok_or_else(|| ArrowError::CastError("Failed to downcast to StringArray".to_string()))?;

    let field_builders: Vec<Box<dyn ArrayBuilder>> = struct_fields.iter().map(|field| {
        match field.data_type() {
            DataType::Utf8 => Box::new(StringBuilder::new()) as Box<dyn ArrayBuilder>,
            DataType::Boolean => Box::new(BooleanBuilder::new()) as Box<dyn ArrayBuilder>,
            DataType::Int32 => Box::new(Int32Builder::new()) as Box<dyn ArrayBuilder>,
            DataType::Int64 => Box::new(Int64Builder::new()) as Box<dyn ArrayBuilder>,
            // TODO add more types
            _ => unimplemented!(),
        }
    }).collect();

    let mut struct_builder = StructBuilder::new(struct_fields.to_vec(), field_builders);

    for i in 0..string_array.len() {

        // TODO: support for nulls

        let string_value = string_array.value(i);
        let json_value: Value = serde_json::from_str(&string_value).unwrap();

        struct_builder.append(true);

        for idx in 0..struct_fields.len() {
            let field_name = struct_fields[idx].name();
            match struct_fields[idx].data_type() {
                DataType::Utf8 => {
                    let value = json_value[field_name].as_str().unwrap();
                    struct_builder
                        .field_builder::<StringBuilder>(idx)
                        .expect("Should return a field builder")
                        .append_value(value);
                },
                DataType::Boolean => {
                    let value = json_value[field_name].as_bool().unwrap();

                    struct_builder
                        .field_builder::<BooleanBuilder>(idx)
                        .expect("Should return a field builder")
                        .append_value(value);
                },
                DataType::Int64 => {
                    let value = json_value[field_name].as_i64().unwrap();

                    struct_builder
                        .field_builder::<Int64Builder>(idx)
                        .expect("Should return a field builder")
                        .append_value(value);
                },
                DataType::Int32 => {
                    if let Some(value) = json_value[field_name].as_i64() { 
                        if let Ok(value) = i32::try_from(value) {
                            let builder = struct_builder
                                .field_builder::<Int32Builder>(idx)
                                .expect("Should return a field builder");
                
                            builder.append_value(value); // append the value to the builder
                        } 
                    }
                },
                // TODO add more types
                _ => unimplemented!(),
            }
        }
    }

    Ok(Arc::new(struct_builder.finish()))

}

#[cfg(test)]
mod test {

    use arrow::{
        array::{Int32Array, StringArray},
        datatypes::{DataType, Field, Schema, TimeUnit},
    };

    use super::*;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, false),
            Field::new("c", DataType::Utf8, false),
        ]))
    }

    fn to_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::LargeUtf8, false),
            Field::new("c", DataType::Timestamp(TimeUnit::Microsecond, None), false),
        ]))
    }

    fn batch_input() -> RecordBatch {
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["foo", "bar", "baz"])),
                Arc::new(StringArray::from(vec![
                    "2024-01-13 03:18:09.000000",
                    "2024-01-13 03:18:09",
                    "2024-01-13 03:18:09.000",
                ])),
            ],
        )
        .expect("record batch should not panic")
    }

    #[test]
    fn test_string_to_timestamp_conversion() {
        let result = try_cast_to(batch_input(), to_schema()).expect("converted");
        assert_eq!(3, result.num_rows());
    }
}
