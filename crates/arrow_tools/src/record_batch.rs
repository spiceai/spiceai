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
    array::{new_null_array, Array, ArrayRef, ListArray, RecordBatch, StructArray},
    buffer::{Buffer, OffsetBuffer},
    compute::cast,
    datatypes::{DataType, Field, SchemaRef},
    error::ArrowError,
};
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
                        return cast(&*Arc::clone(column), field.data_type())
                            .context(UnableToConvertRecordBatchSnafu);
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

/// Flattens a list of struct types with a single field into a list of primitive types.
/// The struct field must be a primitive type.
/// If the struct has multiple fields, all except the first field will be ignored.
///
/// # Errors
///
/// This function will return an error if the column cannot be cast to a list of struct types with a single field.
pub fn to_primitive_type_list(
    column: &ArrayRef,
    field: &Arc<Field>,
) -> Result<(ArrayRef, Arc<Field>), ArrowError> {
    if let DataType::List(inner_field) = field.data_type() {
        if let DataType::Struct(struct_fields) = inner_field.data_type() {
            if struct_fields.len() == 1 {
                let list_item_field = Arc::clone(&struct_fields[0]);

                let original_list_array =
                    column
                        .as_any()
                        .downcast_ref::<ListArray>()
                        .ok_or(ArrowError::CastError(
                            "Failed to downcast to ListArray".into(),
                        ))?;

                let struct_array = original_list_array
                    .values()
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .ok_or(ArrowError::CastError(
                        "Failed to downcast to StructArray".into(),
                    ))?;

                let struct_column_array = Arc::clone(struct_array.column(0));

                let new_list_field = Arc::new(Field::new(
                    field.name(),
                    DataType::List(Arc::clone(&list_item_field)),
                    field.is_nullable(),
                ));
                let new_list_array = ListArray::new(
                    list_item_field,
                    OffsetBuffer::new(
                        Buffer::from_slice_ref(original_list_array.value_offsets()).into(),
                    ),
                    struct_column_array,
                    original_list_array.logical_nulls(),
                );

                return Ok((Arc::new(new_list_array), new_list_field));
            }
        }
    }

    Err(ArrowError::CastError("Invalid column type".into()))
}

/// Recursively truncates the data in a record batch to the specified maximum number of characters.
/// The truncation is applied to `Utf8` and `Utf8View` data.
///
/// # Errors
///
/// This function will return an error if arrow conversion fails.
pub fn truncate_string_columns(
    record_batch: &RecordBatch,
    max_characters: usize,
) -> Result<RecordBatch, ArrowError> {
    let schema = record_batch.schema();
    let columns = record_batch
        .columns()
        .iter()
        .zip(schema.fields())
        .map(|(column, field)| truncate_column_data(Arc::clone(column), field, max_characters))
        .collect::<Result<Vec<_>, _>>()?;

    RecordBatch::try_new(schema, columns)
}

fn truncate_column_data(
    column: ArrayRef,
    field: &Arc<Field>,
    max_characters: usize,
) -> Result<ArrayRef, ArrowError> {
    match field.data_type() {
        DataType::Utf8View => {
            let string_array = column
                .as_any()
                .downcast_ref::<arrow::array::StringViewArray>()
                .ok_or(ArrowError::CastError(
                    "Failed to downcast to StringViewArray".into(),
                ))?;

            let truncated = string_array
                .iter()
                .map(|x| trancate_str(x, max_characters))
                .collect::<arrow::array::StringViewArray>();

            Ok(Arc::new(truncated) as ArrayRef)
        }
        DataType::Utf8 => {
            let string_array = column
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .ok_or(ArrowError::CastError(
                    "Failed to downcast to ListArray".into(),
                ))?;

            let truncated = string_array
                .iter()
                .map(|x| trancate_str(x, max_characters))
                .collect::<arrow::array::StringArray>();

            Ok(Arc::new(truncated) as ArrayRef)
        }
        DataType::List(field) => {
            let list_array = column
                .as_any()
                .downcast_ref::<arrow::array::ListArray>()
                .ok_or_else(|| ArrowError::CastError("Failed to downcast to ListArray".into()))?;

            let truncated_values =
                truncate_column_data(Arc::clone(list_array.values()), field, max_characters)?;

            let list = ListArray::new(
                Arc::clone(field),
                arrow::buffer::OffsetBuffer::new(
                    arrow::buffer::Buffer::from_slice_ref(list_array.value_offsets()).into(),
                ),
                truncated_values,
                list_array.logical_nulls(),
            );

            Ok(Arc::new(list) as ArrayRef)
        }
        DataType::Struct(fields) => {
            let struct_array = column
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| ArrowError::CastError("Failed to downcast to StructArray".into()))?;

            let columns = fields
                .iter()
                .enumerate()
                .map(|(i, field)| {
                    let field_data = struct_array.column(i);
                    truncate_column_data(Arc::clone(field_data), field, max_characters)
                })
                .collect::<Result<Vec<_>, _>>()?;

            let truncated_struct =
                StructArray::from(fields.iter().cloned().zip(columns).collect::<Vec<_>>());
            Ok(Arc::new(truncated_struct) as ArrayRef)
        }
        _ => Ok(column),
    }
}

fn trancate_str(str: Option<&str>, max_characters: usize) -> Option<&str> {
    match str {
        Some(value) => {
            if value.len() > max_characters {
                Some(&value[..max_characters])
            } else {
                Some(value)
            }
        }
        None => None,
    }
}

#[cfg(test)]
mod test {

    use arrow::{
        array::{Int32Array, StringArray},
        datatypes::{DataType, Field, Schema, TimeUnit},
        json::ReaderBuilder,
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

    fn parse_json_to_batch(json_data: &str, schema: SchemaRef) -> RecordBatch {
        let reader = ReaderBuilder::new(schema)
            .build(std::io::Cursor::new(json_data))
            .expect("Failed to create JSON reader");

        reader
            .into_iter()
            .next()
            .expect("Expected a record batch")
            .expect("Failed to read record batch")
    }

    #[test]
    fn test_to_primitive_type_list() {
        let input_batch_json_data = r#"
            {"labels": [{"id": 1}, {"id": 2}]}
            {"labels": null}
            {"labels": null}
            {"labels": null}
            {"labels": [{"id": 3}, {"id": null}]}
            {"labels": [{"id": 4,"name":"test"}, {"id": null,"name":null}]}
            {"labels": null}
            "#;

        let input_batch = parse_json_to_batch(
            input_batch_json_data,
            Arc::new(Schema::new(vec![Field::new(
                "labels",
                DataType::List(Arc::new(Field::new(
                    "struct",
                    DataType::Struct(vec![Field::new("id", DataType::Int32, true)].into()),
                    true,
                ))),
                true,
            )])),
        );

        let expected_list_json_data = r#"
            {"labels": [1, 2]}
            {"labels": null}
            {"labels": null}
            {"labels": null}
            {"labels": [3, null]}
            {"labels": [4, null]}
            {"labels": null}
            "#;

        let expected_list_batch = parse_json_to_batch(
            expected_list_json_data,
            Arc::new(Schema::new(vec![Field::new(
                "labels",
                DataType::List(Arc::new(Field::new("id", DataType::Int32, true))),
                true,
            )])),
        );

        let (processed_array, processed_field) = to_primitive_type_list(
            input_batch.column(0),
            &Arc::new(input_batch.schema().field(0).clone()),
        )
        .expect("to_primitive_type_list should succeed");

        let processed_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![processed_field])),
            vec![processed_array],
        )
        .expect("should create new record batch");

        assert_eq!(expected_list_batch, processed_batch);
    }

    #[test]
    fn test_truncate_record_batch_data_complex_data() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "labels",
            DataType::List(Arc::new(Field::new(
                "struct",
                DataType::Struct(
                    vec![
                        Field::new("id", DataType::Int32, true),
                        Field::new("name", DataType::Utf8, true),
                    ]
                    .into(),
                ),
                true,
            ))),
            true,
        )]));

        let input_batch_json_data = r#"
            {"labels": [{"id": 1, "name": "123"}, {"id": 2, "name": "12345"}, {"id": 1, "name": "123456789"}]}
            {"labels": null}
            {"labels": [{"id": 4,"name":"test12345"}, {"id": null,"name":null}]}
            "#;

        let input_batch = parse_json_to_batch(input_batch_json_data, Arc::clone(&schema));

        let processed_batch = truncate_string_columns(&input_batch, 5)
            .expect("truncate_record_batch_data should succeed");

        let expected_batch_json_data = r#"
            {"labels": [{"id": 1, "name": "123"}, {"id": 2, "name": "12345"}, {"id": 1, "name": "12345"}]}
            {"labels": null}
            {"labels": [{"id": 4,"name":"test1"}, {"id": null,"name":null}]}
            "#;

        let expected_batch = parse_json_to_batch(expected_batch_json_data, schema);

        assert_eq!(processed_batch, expected_batch);
    }
}
