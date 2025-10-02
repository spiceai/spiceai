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
use crate::schema::to_source_native_type_name;
use arrow::array::{Array, ArrayRef, FixedSizeListArray, ListArray, RecordBatch, StructArray};
use arrow::buffer::OffsetBuffer;
use arrow::compute::concat;
use arrow_cast::display::{ArrayFormatter, FormatOptions};
use arrow_schema::{ArrowError, DataType, Field, Schema};
use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;

static MARKDOWN_TABLE_SEPARATOR_ROW: [&str; 5] = ["---"; 5];

/// Operations to apply to [`ArrayRef`] or [`RecordBatch`] data so as to prepare it for display.
///
/// Note: Operations do not preserve all original data, and as such, should be used for human display purposes only.
pub enum FormatOperation {
    /// Truncate strings to be no larger than a given length. This includesnested strings (i.e.
    /// UTF8 elements within lists and structs).
    TruncateUtf8Length(usize),

    /// Truncate lists to contain no more than a given number of elements.
    TruncateListLength(usize),
}

#[allow(clippy::too_many_lines)]
pub(crate) fn format_column_data(
    column: ArrayRef,
    field: &Arc<Field>,
    operation: FormatOperation,
) -> Result<ArrayRef, ArrowError> {
    match (operation, get_possible_nested_list_datatype(field)) {
        (FormatOperation::TruncateUtf8Length(max_characters), (DataType::Utf8View, _)) => {
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
        (FormatOperation::TruncateUtf8Length(max_characters), (DataType::Utf8, _)) => {
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
        (
            FormatOperation::TruncateListLength(num_elements),
            (
                DataType::List(_)
                | DataType::LargeList(_)
                | DataType::FixedSizeList(_, _)
                | DataType::ListView(_),
                Some(_),
            ),
        ) => {
            let array_ref = if let DataType::FixedSizeList(_, _) = column.data_type() {
                let fixed_list_array = column
                    .as_any()
                    .downcast_ref::<arrow::array::FixedSizeListArray>()
                    .ok_or_else(|| {
                        ArrowError::CastError("Failed to downcast to FixedSizeListArray".into())
                    })?;
                Arc::new(truncate_fixed_size_list_array(
                    fixed_list_array,
                    num_elements,
                )?) as ArrayRef
            } else {
                let list_array = column
                    .as_any()
                    .downcast_ref::<arrow::array::ListArray>()
                    .ok_or_else(|| {
                        ArrowError::CastError("Failed to downcast to ListArray".into())
                    })?;
                Arc::new(truncate_list_array(list_array, num_elements)?) as ArrayRef
            };
            Ok(array_ref)
        }
        (FormatOperation::TruncateUtf8Length(max_characters), (DataType::List(field), _)) => {
            let list_array = column
                .as_any()
                .downcast_ref::<arrow::array::ListArray>()
                .ok_or_else(|| ArrowError::CastError("Failed to downcast to ListArray".into()))?;

            let truncated_values = format_column_data(
                Arc::clone(list_array.values()),
                &field,
                FormatOperation::TruncateUtf8Length(max_characters),
            )?;

            let list = ListArray::new(
                Arc::clone(&field),
                arrow::buffer::OffsetBuffer::new(
                    arrow::buffer::Buffer::from_slice_ref(list_array.value_offsets()).into(),
                ),
                truncated_values,
                list_array.logical_nulls(),
            );

            Ok(Arc::new(list) as ArrayRef)
        }
        (FormatOperation::TruncateUtf8Length(max_characters), (DataType::Struct(fields), _)) => {
            let struct_array = column
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| ArrowError::CastError("Failed to downcast to StructArray".into()))?;

            let columns = fields
                .iter()
                .enumerate()
                .map(|(i, field)| {
                    let field_data = struct_array.column(i);
                    format_column_data(
                        Arc::clone(field_data),
                        field,
                        FormatOperation::TruncateUtf8Length(max_characters),
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;

            let truncated_struct =
                StructArray::from(fields.iter().cloned().zip(columns).collect::<Vec<_>>());
            Ok(Arc::new(truncated_struct) as ArrayRef)
        }
        _ => Ok(column),
    }
}

/// Get both the [`DataType`] of the field, and if its a list-like type, the [`DataType`] of elements in the list.
fn get_possible_nested_list_datatype(f: &Arc<Field>) -> (DataType, Option<DataType>) {
    (
        f.data_type().clone(),
        match f.data_type() {
            DataType::List(f) | DataType::FixedSizeList(f, _) | DataType::LargeList(f) => {
                Some(f.data_type().clone())
            }
            _ => None,
        },
    )
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

#[allow(
    clippy::cast_sign_loss,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap
)]
fn truncate_fixed_size_list_array(
    list_array: &FixedSizeListArray,
    max_len: usize,
) -> Result<FixedSizeListArray, ArrowError> {
    let child_array = list_array.values();
    let original_size = list_array.value_length() as usize;
    let truncated_size = max_len.min(original_size);

    let sliced_arrays: Vec<Arc<dyn Array>> = (0..list_array.len())
        .map(|i| child_array.slice(i * original_size, truncated_size))
        .collect();

    let new_child_array = Arc::new(concat(
        &sliced_arrays.iter().map(AsRef::as_ref).collect::<Vec<_>>(),
    )?);
    let nulls = new_child_array.nulls().cloned();

    FixedSizeListArray::try_new(
        Arc::new(Field::new(
            "item",
            child_array.data_type().clone(),
            child_array.is_nullable(),
        )),
        truncated_size as i32,
        new_child_array,
        nulls,
    )
}

#[allow(
    clippy::cast_sign_loss,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap
)]
fn truncate_list_array(list_array: &ListArray, max_len: usize) -> Result<ListArray, ArrowError> {
    let child_array = list_array.values();
    let offsets = list_array.value_offsets();

    let new_lengths: Vec<usize> = (0..list_array.len())
        .map(|i| {
            let start = offsets[i] as usize;
            let end = offsets[i + 1] as usize;
            max_len.min(end - start)
        })
        .collect();

    let sliced_arrays: Vec<Arc<dyn Array>> = new_lengths
        .iter()
        .enumerate()
        .map(|(i, &len)| child_array.slice(offsets[i] as usize, len))
        .collect();

    let new_child_array = Arc::new(concat(
        &sliced_arrays.iter().map(AsRef::as_ref).collect::<Vec<_>>(),
    )?);

    let nulls = new_child_array.nulls().cloned();

    ListArray::try_new(
        Arc::new(Field::new(
            "item",
            child_array.data_type().clone(),
            child_array.is_nullable(),
        )),
        OffsetBuffer::from_lengths(new_lengths),
        new_child_array,
        nulls,
    )
}

/// Creates a visual representation of record batches using markdown document format with additional header fields.
///
/// # Errors
///
/// Returns an `ArrowError` if the record batch cannot be formatted or content column is not found.
pub fn to_markdown_documents(
    results: &[RecordBatch],
    content_column: &str,
    content_alias: Option<&str>,
    header_fields: &[String],
) -> Result<String, ArrowError> {
    let options: FormatOptions = FormatOptions::default();
    let mut buffer = Vec::new();

    for batch in results {
        let schema = batch.schema();
        let formatters = batch
            .columns()
            .iter()
            .map(|c| ArrayFormatter::try_new(c.as_ref(), &options))
            .collect::<Result<Vec<_>, ArrowError>>()?;

        let header_indices = column_indices(batch, header_fields)?;

        let Some(idx_content) = schema
            .fields()
            .iter()
            .position(|f| f.name() == content_column)
        else {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Column '{content_column}' not found in schema"
            )));
        };

        for row in 0..batch.num_rows() {
            // write document header / attributes
            writeln!(&mut buffer, "---")?;
            for i in header_indices.as_slice() {
                let field = schema.field(*i);
                if let Some(formatter) = formatters.get(*i) {
                    writeln!(buffer, "{}: {}", field.name(), formatter.value(row))?;
                }
            }

            if let Some(column_name) = content_alias {
                // Write content alias as column name in header
                writeln!(buffer, "column: {column_name}")?;
            }

            writeln!(&mut buffer, "---")?;

            // write main document content
            writeln!(buffer, "{}\n", formatters[idx_content].value(row))?;
        }
    }

    String::from_utf8(buffer).map_err(|e| {
        ArrowError::from_external_error(format!("Failed to convert byte array to utf8: {e}").into())
    })
}

fn column_indices(batch: &RecordBatch, column_names: &[String]) -> Result<Vec<usize>, ArrowError> {
    column_names
        .iter()
        .map(|name| {
            batch
                .schema()
                .index_of(name)
                .map_err(|_| ArrowError::InvalidArgumentError(format!("Column '{name}' not found")))
        })
        .collect()
}

fn hashmap_to_string(map: &HashMap<String, String>, separator: &str) -> String {
    let mut keys: Vec<_> = map.keys().collect();
    keys.sort(); // To make this function reproducible
    keys.iter()
        .map(|k| format!("* {}: {}", k, map[*k]))
        .collect::<Vec<_>>()
        .join(separator)
}

#[allow(clippy::doc_lazy_continuation)]
/// Creates a markdown representation of tables' schemas in the following format:
///
/// **Table: users**
/// Metadata:
/// * owner: admin
/// * description: All the users of the world
/// | Column | Sql Type | Arrow Type | Nullable | Metadata |
/// | --- | --- | --- | --- | --- |
/// | id | BIGINT | Int64 | false | * comment: autoincrement<br>* description: user id |
/// | name | VARCHAR | Utf8 | true |  |
#[must_use]
pub fn table_schemas_to_markdown_table(table_schemas: Vec<(String, Schema)>) -> String {
    let mut table_schemas_formatted = Vec::with_capacity(table_schemas.len());

    for (table_name, table_schema) in table_schemas {
        // Header row and separator for markdown
        let header = ["Column", "Sql Type", "Arrow Type", "Nullable", "Metadata"];

        let mut md_table = vec![
            format!("| {} |", header.join(" | ")),
            format!("| {} |", MARKDOWN_TABLE_SEPARATOR_ROW.join(" | ")),
        ];

        for field in table_schema.fields() {
            md_table.push(format!(
                "| {} | {} | {} | {} | {} |",
                field.name(),
                to_source_native_type_name(field.data_type()),
                field.data_type(),
                field.is_nullable(),
                hashmap_to_string(field.metadata(), "<br>"),
            ));
        }

        let mut sections = vec![format!("**Table: {table_name}**")];
        if !table_schema.metadata().is_empty() {
            sections.push(format!(
                "Metadata:\n{}",
                hashmap_to_string(table_schema.metadata(), "\n")
            ));
        }
        sections.push(md_table.join("\n"));
        table_schemas_formatted.push(sections.join("\n"));
    }

    table_schemas_formatted.join("\n\n")
}

#[cfg(test)]
mod tests {
    use arrow::{
        array::{ArrayRef, FixedSizeListArray, Float32Array, ListArray, RecordBatch, StringArray},
        datatypes::Int32Type,
    };
    use arrow_schema::{DataType, Field, Schema};
    use snafu::ResultExt;
    use std::collections::HashMap;
    use std::sync::Arc;

    use super::*;

    #[test]
    fn test_pretty_format_markdown() -> Result<(), Box<dyn std::error::Error>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("content_chunk", DataType::Utf8, true),
            Field::new("location", DataType::Utf8, true),
            Field::new("dist", DataType::Float32, true),
        ]));

        let content_chunk = StringArray::from(vec![
            Some(
                "Lorem ipsum dolor sit amet, consectetur adipiscing elit.

Sed do eiusmod tempor aliqua.

reprehenderit nulla pariatur.",
            ),
            Some(
                "Lorem ipsum dolor adipiscing elit.
Cras venenatis euismod malesuada.",
            ),
        ]);

        let location = StringArray::from(vec![
            Some("path/to/folder/file_12345.txt"),
            Some("path/to/folder/file_67890.txt"),
        ]);

        let dist = Float32Array::from(vec![Some(0.376_276), Some(0.123_456)]);

        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(content_chunk), Arc::new(location), Arc::new(dist)],
        )?;

        let formatted = to_markdown_documents(
            std::slice::from_ref(&batch),
            "content_chunk",
            None,
            &["location".to_string(), "dist".to_string()],
        )
        .expect("format record batch")
        .to_string();

        insta::assert_snapshot!(formatted);

        let formatted = to_markdown_documents(
            &[batch],
            "content_chunk",
            Some("content"),
            &["location".to_string(), "dist".to_string()],
        )
        .expect("format record batch")
        .to_string();

        insta::assert_snapshot!("with_alias", formatted);

        Ok(())
    }

    #[test]
    fn test_truncate_list_array() {
        let test_cases: Vec<(&str, usize, ListArray)> = vec![
            (
                "truncate_list_array_basic",
                2,
                ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
                    Some(vec![Some(0), Some(1), Some(2)]),
                    Some(vec![Some(3), Some(4), Some(5)]),
                    Some(vec![Some(6), Some(7)]),
                ]),
            ),
            (
                "truncate_list_array_unchanged",
                5,
                ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
                    Some(vec![Some(0), Some(1), Some(2)]),
                    Some(vec![Some(3), Some(4), Some(5)]),
                    Some(vec![Some(6), Some(7)]),
                ]),
            ),
            (
                "truncate_list_array_split",
                3,
                ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
                    Some(vec![Some(0), Some(1), Some(2), Some(3)]),
                    Some(vec![Some(3), Some(4), Some(5), Some(6)]),
                    Some(vec![Some(6), Some(7)]),
                ]),
            ),
        ];
        for (test_name, max_len, input) in test_cases {
            let output: ArrayRef =
                Arc::new(truncate_list_array(&input, max_len).expect("truncate_list_array failed"));
            insta::assert_json_snapshot!(
                test_name,
                write_to_json_value(output).expect("could not write ListArray to JSON")
            );
        }
    }

    #[test]
    fn test_truncate_fixed_size_list_array() {
        let test_cases: Vec<(&str, usize, FixedSizeListArray)> = vec![
            (
                "truncate_fixed_size_list_array_basic",
                2,
                FixedSizeListArray::from_iter_primitive::<Int32Type, _, _>(
                    vec![
                        Some(vec![Some(0), Some(1), Some(2)]),
                        Some(vec![Some(3), Some(4), Some(5)]),
                        Some(vec![Some(6), Some(7), Some(8)]),
                    ],
                    3,
                ),
            ),
            (
                "truncate_fixed_size_list_array_unchanged",
                5,
                FixedSizeListArray::from_iter_primitive::<Int32Type, _, _>(
                    vec![
                        Some(vec![Some(0), Some(1), Some(2)]),
                        Some(vec![Some(3), Some(4), Some(5)]),
                        Some(vec![Some(6), Some(7), Some(8)]),
                    ],
                    3,
                ),
            ),
        ];
        for (test_name, max_len, input) in test_cases {
            let output: ArrayRef = Arc::new(
                truncate_fixed_size_list_array(&input, max_len)
                    .expect("truncate_fixed_size_list_array failed"),
            );
            insta::assert_json_snapshot!(
                test_name,
                write_to_json_value(output).expect("could not write FixedSizeListArray to JSON")
            );
        }
    }

    pub fn write_to_json_value(
        data: ArrayRef,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        let rb = RecordBatch::try_new(
            Schema::new(vec![Field::new(
                "col",
                data.data_type().clone(),
                data.is_nullable(),
            )])
            .into(),
            vec![data],
        )
        .boxed()?;
        let buf = Vec::new();
        let mut writer = arrow_json::ArrayWriter::new(buf);

        writer.write_batches([rb].iter().collect::<Vec<&RecordBatch>>().as_slice())?;
        writer.finish()?;
        serde_json::from_reader::<_, serde_json::Value>(writer.into_inner().as_slice()).boxed()
    }

    #[test]
    fn test_table_schemas_to_markdown_table() {
        let mut field_metadata = HashMap::new();
        field_metadata.insert("comment".to_string(), "autoincrement".to_string());
        field_metadata.insert("description".to_string(), "user id".to_string());

        let mut schema_metadata = HashMap::new();
        schema_metadata.insert("owner".to_string(), "admin".to_string());
        schema_metadata.insert(
            "description".to_string(),
            "All the users of the world".to_string(),
        );

        let fields = vec![
            Field::new("id", DataType::Int64, false).with_metadata(field_metadata.clone()),
            Field::new("name", DataType::Utf8, true),
        ];

        let schema = Schema::new(fields).with_metadata(schema_metadata.clone());
        let output = table_schemas_to_markdown_table(vec![("users".to_string(), schema)]);

        insta::assert_snapshot!(output);
    }
}
