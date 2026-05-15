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
use arrow::array::{
    Array, ArrayRef, FixedSizeListArray, LargeListArray, LargeListViewArray, ListArray,
    ListViewArray, RecordBatch, StructArray,
};
use arrow::buffer::{OffsetBuffer, ScalarBuffer};
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
                .map(|x| truncate_str(x, max_characters))
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
                .map(|x| truncate_str(x, max_characters))
                .collect::<arrow::array::StringArray>();

            Ok(Arc::new(truncated) as ArrayRef)
        }
        (
            FormatOperation::TruncateListLength(num_elements),
            (
                DataType::List(_)
                | DataType::LargeList(_)
                | DataType::FixedSizeList(_, _)
                | DataType::ListView(_)
                | DataType::LargeListView(_),
                Some(_),
            ),
        ) => {
            let array_ref = match column.data_type() {
                DataType::FixedSizeList(_, _) => {
                    let fixed_list_array = column
                        .as_any()
                        .downcast_ref::<FixedSizeListArray>()
                        .ok_or_else(|| {
                            ArrowError::CastError("Failed to downcast to FixedSizeListArray".into())
                        })?;
                    Arc::new(truncate_fixed_size_list_array(
                        fixed_list_array,
                        num_elements,
                    )?) as ArrayRef
                }
                DataType::LargeList(_) => {
                    let list_array = column
                        .as_any()
                        .downcast_ref::<LargeListArray>()
                        .ok_or_else(|| {
                            ArrowError::CastError("Failed to downcast to LargeListArray".into())
                        })?;
                    Arc::new(truncate_large_list_array(list_array, num_elements)?) as ArrayRef
                }
                DataType::ListView(_) => {
                    let list_array =
                        column
                            .as_any()
                            .downcast_ref::<ListViewArray>()
                            .ok_or_else(|| {
                                ArrowError::CastError("Failed to downcast to ListViewArray".into())
                            })?;
                    Arc::new(truncate_list_view_array(list_array, num_elements)?) as ArrayRef
                }
                DataType::LargeListView(_) => {
                    let list_array = column
                        .as_any()
                        .downcast_ref::<LargeListViewArray>()
                        .ok_or_else(|| {
                            ArrowError::CastError("Failed to downcast to LargeListViewArray".into())
                        })?;
                    Arc::new(truncate_large_list_view_array(list_array, num_elements)?) as ArrayRef
                }
                _ => {
                    let list_array =
                        column.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
                            ArrowError::CastError("Failed to downcast to ListArray".into())
                        })?;
                    Arc::new(truncate_list_array(list_array, num_elements)?) as ArrayRef
                }
            };
            Ok(array_ref)
        }
        (FormatOperation::TruncateUtf8Length(max_characters), (DataType::List(field), _)) => {
            let list_array = column
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| ArrowError::CastError("Failed to downcast to ListArray".into()))?;

            let truncated_values = format_column_data(
                Arc::clone(list_array.values()),
                &field,
                FormatOperation::TruncateUtf8Length(max_characters),
            )?;

            let list = ListArray::new(
                Arc::clone(&field),
                list_array.offsets().clone(),
                truncated_values,
                list_array.nulls().cloned(),
            );

            Ok(Arc::new(list) as ArrayRef)
        }
        (FormatOperation::TruncateUtf8Length(max_characters), (DataType::LargeList(field), _)) => {
            let list_array = column
                .as_any()
                .downcast_ref::<LargeListArray>()
                .ok_or_else(|| {
                    ArrowError::CastError("Failed to downcast to LargeListArray".into())
                })?;

            let truncated_values = format_column_data(
                Arc::clone(list_array.values()),
                &field,
                FormatOperation::TruncateUtf8Length(max_characters),
            )?;

            let list = LargeListArray::new(
                Arc::clone(&field),
                list_array.offsets().clone(),
                truncated_values,
                list_array.nulls().cloned(),
            );

            Ok(Arc::new(list) as ArrayRef)
        }
        (
            FormatOperation::TruncateUtf8Length(max_characters),
            (DataType::FixedSizeList(field, size), _),
        ) => {
            let list_array = column
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .ok_or_else(|| {
                    ArrowError::CastError("Failed to downcast to FixedSizeListArray".into())
                })?;

            let truncated_values = format_column_data(
                Arc::clone(list_array.values()),
                &field,
                FormatOperation::TruncateUtf8Length(max_characters),
            )?;

            let list = FixedSizeListArray::new(
                Arc::clone(&field),
                size,
                truncated_values,
                list_array.nulls().cloned(),
            );

            Ok(Arc::new(list) as ArrayRef)
        }
        (FormatOperation::TruncateUtf8Length(max_characters), (DataType::ListView(field), _)) => {
            let list_array = column
                .as_any()
                .downcast_ref::<ListViewArray>()
                .ok_or_else(|| {
                    ArrowError::CastError("Failed to downcast to ListViewArray".into())
                })?;

            let truncated_values = format_column_data(
                Arc::clone(list_array.values()),
                &field,
                FormatOperation::TruncateUtf8Length(max_characters),
            )?;

            ListViewArray::try_new(
                Arc::clone(&field),
                list_array.offsets().clone(),
                list_array.sizes().clone(),
                truncated_values,
                list_array.nulls().cloned(),
            )
            .map(|list| Arc::new(list) as ArrayRef)
        }
        (
            FormatOperation::TruncateUtf8Length(max_characters),
            (DataType::LargeListView(field), _),
        ) => {
            let list_array = column
                .as_any()
                .downcast_ref::<LargeListViewArray>()
                .ok_or_else(|| {
                    ArrowError::CastError("Failed to downcast to LargeListViewArray".into())
                })?;

            let truncated_values = format_column_data(
                Arc::clone(list_array.values()),
                &field,
                FormatOperation::TruncateUtf8Length(max_characters),
            )?;

            LargeListViewArray::try_new(
                Arc::clone(&field),
                list_array.offsets().clone(),
                list_array.sizes().clone(),
                truncated_values,
                list_array.nulls().cloned(),
            )
            .map(|list| Arc::new(list) as ArrayRef)
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
            DataType::List(f)
            | DataType::FixedSizeList(f, _)
            | DataType::LargeList(f)
            | DataType::ListView(f)
            | DataType::LargeListView(f) => Some(f.data_type().clone()),
            _ => None,
        },
    )
}

/// Truncates a string to at most `max_characters` Unicode characters.
///
/// This function correctly handles multi-byte UTF-8 characters (e.g., emoji, CJK)
/// by truncating at character boundaries rather than byte boundaries.
fn truncate_str(str: Option<&str>, max_characters: usize) -> Option<&str> {
    str.map(|value| {
        // Find the byte index of the (max_characters)th character boundary
        // If the string has fewer characters, return the whole string
        value
            .char_indices()
            .nth(max_characters)
            .map_or(value, |(byte_idx, _)| &value[..byte_idx])
    })
}

fn value_to_usize<T>(value: T, value_name: &str) -> Result<usize, ArrowError>
where
    T: TryInto<usize> + std::fmt::Display + Copy,
{
    value.try_into().map_err(|_| {
        ArrowError::InvalidArgumentError(format!(
            "{value_name} {value} cannot be represented as usize"
        ))
    })
}

fn value_to_i32(value: usize, value_name: &str) -> Result<i32, ArrowError> {
    i32::try_from(value).map_err(|_| {
        ArrowError::InvalidArgumentError(format!(
            "{value_name} {value} cannot be represented as i32"
        ))
    })
}

fn list_slice_range<T>(
    start_offset: T,
    end_offset: T,
    array_name: &str,
) -> Result<(usize, usize), ArrowError>
where
    T: TryInto<usize> + std::fmt::Display + Copy,
{
    let start = value_to_usize(start_offset, array_name)?;
    let end = value_to_usize(end_offset, array_name)?;
    let len = end.checked_sub(start).ok_or_else(|| {
        ArrowError::InvalidArgumentError(format!(
            "{array_name} end offset {end} is before start offset {start}"
        ))
    })?;

    Ok((start, len))
}

fn list_element_field(data_type: &DataType, array_name: &str) -> Result<Arc<Field>, ArrowError> {
    match data_type {
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _) => Ok(Arc::clone(field)),
        _ => Err(ArrowError::InvalidArgumentError(format!(
            "{array_name} data type is not a list type: {data_type}"
        ))),
    }
}

fn truncate_fixed_size_list_array(
    list_array: &FixedSizeListArray,
    max_len: usize,
) -> Result<FixedSizeListArray, ArrowError> {
    if list_array.is_empty() {
        return Ok(list_array.clone());
    }
    let child_array = list_array.values();
    let original_size =
        value_to_usize(list_array.value_length(), "FixedSizeListArray value length")?;
    // Fast path: zero-copy clone when truncation would be a no-op. This is the
    // common case for display formatting (generous max_len, small actual lists)
    // and avoids per-row slice + concat allocation/copy entirely.
    if max_len >= original_size {
        return Ok(list_array.clone());
    }
    let truncated_size = max_len; // known to be < original_size
    let truncated_size_i32 = value_to_i32(truncated_size, "FixedSizeListArray truncated size")?;
    let element_field = list_element_field(list_array.data_type(), "FixedSizeListArray")?;

    let sliced_arrays: Vec<Arc<dyn Array>> = (0..list_array.len())
        .map(|i| child_array.slice(i * original_size, truncated_size))
        .collect();

    let new_child_array = Arc::new(concat(
        &sliced_arrays.iter().map(AsRef::as_ref).collect::<Vec<_>>(),
    )?);
    // Parent list-level nulls (which slots are NULL lists) are preserved as-is;
    // they live separately from the child array's element-level null bitmap.
    let nulls = list_array.nulls().cloned();

    FixedSizeListArray::try_new(element_field, truncated_size_i32, new_child_array, nulls)
}

fn truncate_list_array(list_array: &ListArray, max_len: usize) -> Result<ListArray, ArrowError> {
    if list_array.is_empty() {
        return Ok(list_array.clone());
    }
    let child_array = list_array.values();
    let offsets = list_array.value_offsets();
    // Fast path: zero-copy clone when no list element exceeds max_len.
    // Avoids building per-row slices + concat (major allocation + copy overhead
    // in the common display path where lists are short).
    let needs_trunc = (0..list_array.len()).try_fold(false, |needs_trunc, i| {
        if needs_trunc {
            return Ok::<_, ArrowError>(true);
        }

        list_slice_range(offsets[i], offsets[i + 1], "ListArray").map(|(_, len)| len > max_len)
    })?;
    if !needs_trunc {
        return Ok(list_array.clone());
    }
    let element_field = list_element_field(list_array.data_type(), "ListArray")?;

    let slice_ranges: Vec<(usize, usize)> = (0..list_array.len())
        .map(|i| {
            list_slice_range(offsets[i], offsets[i + 1], "ListArray")
                .map(|(start, len)| (start, max_len.min(len)))
        })
        .collect::<Result<_, ArrowError>>()?;
    let new_lengths: Vec<usize> = slice_ranges.iter().map(|&(_, len)| len).collect();

    let sliced_arrays: Vec<Arc<dyn Array>> = new_lengths
        .iter()
        .zip(slice_ranges.iter())
        .map(|(&len, &(start, _))| child_array.slice(start, len))
        .collect();

    let new_child_array = Arc::new(concat(
        &sliced_arrays.iter().map(AsRef::as_ref).collect::<Vec<_>>(),
    )?);

    let nulls = list_array.nulls().cloned();

    ListArray::try_new(
        element_field,
        OffsetBuffer::from_lengths(new_lengths),
        new_child_array,
        nulls,
    )
}

fn truncate_large_list_array(
    list_array: &LargeListArray,
    max_len: usize,
) -> Result<LargeListArray, ArrowError> {
    if list_array.is_empty() {
        return Ok(list_array.clone());
    }
    let child_array = list_array.values();
    let offsets = list_array.value_offsets();
    // Fast path: zero-copy clone when truncation is unnecessary (common for
    // result formatting). LargeList uses i64 offsets, so validate conversion
    // before deciding whether the slow path is needed.
    let needs_trunc = (0..list_array.len()).try_fold(false, |needs_trunc, i| {
        if needs_trunc {
            return Ok::<_, ArrowError>(true);
        }

        list_slice_range(offsets[i], offsets[i + 1], "LargeListArray").map(|(_, len)| len > max_len)
    })?;
    if !needs_trunc {
        return Ok(list_array.clone());
    }
    let element_field = list_element_field(list_array.data_type(), "LargeListArray")?;

    let slice_ranges: Vec<(usize, usize)> = (0..list_array.len())
        .map(|i| {
            list_slice_range(offsets[i], offsets[i + 1], "LargeListArray")
                .map(|(start, len)| (start, max_len.min(len)))
        })
        .collect::<Result<_, ArrowError>>()?;
    let new_lengths: Vec<usize> = slice_ranges.iter().map(|&(_, len)| len).collect();

    let sliced_arrays: Vec<Arc<dyn Array>> = new_lengths
        .iter()
        .zip(slice_ranges.iter())
        .map(|(&len, &(start, _))| child_array.slice(start, len))
        .collect();

    let new_child_array = Arc::new(concat(
        &sliced_arrays.iter().map(AsRef::as_ref).collect::<Vec<_>>(),
    )?);

    let nulls = list_array.nulls().cloned();

    LargeListArray::try_new(
        element_field,
        OffsetBuffer::from_lengths(new_lengths),
        new_child_array,
        nulls,
    )
}

fn truncate_list_view_array(
    list_array: &ListViewArray,
    max_len: usize,
) -> Result<ListViewArray, ArrowError> {
    if list_array.is_empty() {
        return Ok(list_array.clone());
    }
    let child_array = list_array.values();
    let sizes = list_array.value_sizes();
    // Fast path for ListView: sizes are stored explicitly, cheap any() scan.
    // When no element exceeds the limit we return the original (preserving
    // whatever non-contiguous view layout the caller had).
    let needs_trunc = sizes.iter().try_fold(false, |needs_trunc, &size| {
        if needs_trunc {
            return Ok::<_, ArrowError>(true);
        }

        value_to_usize(size, "ListViewArray size").map(|size| size > max_len)
    })?;
    if !needs_trunc {
        return Ok(list_array.clone());
    }
    let offsets = list_array.value_offsets();
    let element_field = list_element_field(list_array.data_type(), "ListViewArray")?;

    let slice_ranges: Vec<(usize, usize, i32)> = (0..list_array.len())
        .map(|i| {
            let start = value_to_usize(offsets[i], "ListViewArray offset")?;
            let original_size = value_to_usize(sizes[i], "ListViewArray size")?;
            let truncated_size = max_len.min(original_size);
            let truncated_size_i32 = value_to_i32(truncated_size, "ListViewArray truncated size")?;

            Ok((start, truncated_size, truncated_size_i32))
        })
        .collect::<Result<_, ArrowError>>()?;
    let new_sizes: Vec<i32> = slice_ranges
        .iter()
        .map(|&(_, _, truncated_size_i32)| truncated_size_i32)
        .collect();

    let sliced_arrays: Vec<Arc<dyn Array>> = (0..list_array.len())
        .map(|i| child_array.slice(slice_ranges[i].0, slice_ranges[i].1))
        .collect();

    let new_child_array = Arc::new(concat(
        &sliced_arrays.iter().map(AsRef::as_ref).collect::<Vec<_>>(),
    )?);

    let nulls = list_array.nulls().cloned();

    // After concat, the new values buffer is laid out contiguously, so we
    // re-derive offsets from the truncated sizes. Bail on overflow rather
    // than silently producing misaligned offsets.
    let mut new_offsets: Vec<i32> = Vec::with_capacity(new_sizes.len());
    let mut running: i32 = 0;
    for &s in &new_sizes {
        new_offsets.push(running);
        running = running.checked_add(s).ok_or_else(|| {
            ArrowError::InvalidArgumentError("ListViewArray cumulative size overflowed i32".into())
        })?;
    }

    ListViewArray::try_new(
        element_field,
        ScalarBuffer::from(new_offsets),
        ScalarBuffer::from(new_sizes),
        new_child_array,
        nulls,
    )
}

fn truncate_large_list_view_array(
    list_array: &LargeListViewArray,
    max_len: usize,
) -> Result<LargeListViewArray, ArrowError> {
    if list_array.is_empty() {
        return Ok(list_array.clone());
    }
    let child_array = list_array.values();
    let sizes = list_array.value_sizes();
    // Fast path for LargeListView (i64 sizes): cheap scan over the sizes buffer.
    // When nothing needs truncation we avoid the expensive concat + offset rebuild.
    let max_len_i64 = i64::try_from(max_len).map_err(|_| {
        ArrowError::InvalidArgumentError(format!("max_len {max_len} cannot be represented as i64"))
    })?;
    if !sizes.iter().any(|&s| s > max_len_i64) {
        return Ok(list_array.clone());
    }
    let offsets = list_array.value_offsets();
    let element_field = list_element_field(list_array.data_type(), "LargeListViewArray")?;

    let new_sizes: Vec<i64> = (0..list_array.len())
        .map(|i| sizes[i].min(max_len_i64))
        .collect();

    let slice_ranges: Vec<(usize, usize)> = (0..list_array.len())
        .map(|i| {
            let start = value_to_usize(offsets[i], "LargeListViewArray offset")?;
            let len = value_to_usize(new_sizes[i], "LargeListViewArray size")?;

            Ok((start, len))
        })
        .collect::<Result<_, ArrowError>>()?;

    let sliced_arrays: Vec<Arc<dyn Array>> = (0..list_array.len())
        .map(|i| child_array.slice(slice_ranges[i].0, slice_ranges[i].1))
        .collect();

    let new_child_array = Arc::new(concat(
        &sliced_arrays.iter().map(AsRef::as_ref).collect::<Vec<_>>(),
    )?);

    let nulls = list_array.nulls().cloned();

    let mut new_offsets: Vec<i64> = Vec::with_capacity(new_sizes.len());
    let mut running: i64 = 0;
    for &s in &new_sizes {
        new_offsets.push(running);
        running = running.checked_add(s).ok_or_else(|| {
            ArrowError::InvalidArgumentError(
                "LargeListViewArray cumulative size overflowed i64".into(),
            )
        })?;
    }

    LargeListViewArray::try_new(
        element_field,
        ScalarBuffer::from(new_offsets),
        ScalarBuffer::from(new_sizes),
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

#[expect(clippy::doc_lazy_continuation)]
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

/// Pretty prints an Arrow Schema in a format similar to Python's pyarrow output.
///
/// Example format:
/// col1: string
/// col2: int64
/// col3: list<item: float32>
///
/// # Errors
///
/// Returns a `std::fmt::Error` if writing to the output fails.
pub fn pretty_print_schema(
    schema: &Arc<Schema>,
    output: &mut impl std::fmt::Write,
) -> std::fmt::Result {
    // Helper function to recursively format complex types directly to a writer
    fn write_data_type(data_type: &DataType, w: &mut impl std::fmt::Write) -> std::fmt::Result {
        match data_type {
            DataType::List(field) => {
                w.write_str("list<item: ")?;
                write_data_type(field.data_type(), w)?;
                w.write_char('>')
            }
            DataType::LargeList(field) => {
                w.write_str("large_list<item: ")?;
                write_data_type(field.data_type(), w)?;
                w.write_char('>')
            }
            DataType::FixedSizeList(field, size) => {
                w.write_str("fixed_size_list<item: ")?;
                write_data_type(field.data_type(), w)?;
                write!(w, ">[{size}]")
            }
            DataType::ListView(field) => {
                w.write_str("list_view<item: ")?;
                write_data_type(field.data_type(), w)?;
                w.write_char('>')
            }
            DataType::LargeListView(field) => {
                w.write_str("large_list_view<item: ")?;
                write_data_type(field.data_type(), w)?;
                w.write_char('>')
            }
            DataType::Map(field, _) => {
                w.write_str("map<")?;
                write_data_type(field.data_type(), w)?;
                w.write_char('>')
            }
            DataType::Struct(fields) => {
                w.write_str("struct<")?;
                for (i, f) in fields.iter().enumerate() {
                    if i > 0 {
                        w.write_str(", ")?;
                    }
                    w.write_str(f.name())?;
                    w.write_char(' ')?;
                    write_data_type(f.data_type(), w)?;
                }
                w.write_char('>')
            }
            // For all other simple types (Int32, Utf8, Timestamp, etc.)
            _ => {
                // Write Debug format in lowercase without allocating
                write!(w, "{data_type:?}")?;
                Ok(())
            }
        }
    }

    // Write Debug format of DataType in lowercase
    fn write_data_type_lowercase(
        data_type: &DataType,
        w: &mut impl std::fmt::Write,
    ) -> std::fmt::Result {
        struct LowercaseWriter<'a, W: std::fmt::Write>(&'a mut W);

        impl<W: std::fmt::Write> std::fmt::Write for LowercaseWriter<'_, W> {
            fn write_str(&mut self, s: &str) -> std::fmt::Result {
                for c in s.chars() {
                    self.0.write_char(c.to_ascii_lowercase())?;
                }
                Ok(())
            }
        }

        write_data_type(data_type, &mut LowercaseWriter(w))
    }

    for field in schema.fields() {
        output.write_char(' ')?;
        output.write_str(field.name())?;
        output.write_str(": ")?;
        write_data_type_lowercase(field.data_type(), output)?;
        if field.is_nullable() {
            output.write_str(" (nullable)")?;
        }
        output.write_char('\n')?;
    }

    Ok(())
}

/// A wrapper that implements `Display` for pretty-printing an Arrow schema.
///
/// This allows zero-allocation schema formatting when used with `tracing` macros
/// or any other context that accepts `Display` types.
///
/// # Example
/// ```ignore
/// use arrow_tools::format::SchemaDisplay;
/// tracing::debug!("Schema: {}", SchemaDisplay(&schema));
/// ```
pub struct SchemaDisplay<'a>(pub &'a Arc<Schema>);

impl std::fmt::Display for SchemaDisplay<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        pretty_print_schema(self.0, f)
    }
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

    /// Test that `truncate_str` correctly handles multi-byte UTF-8 strings.
    /// After the fix, this should truncate by character count, not byte count.
    #[test]
    fn test_truncate_str_multibyte_utf8() {
        // "日本語" is 9 bytes (3 chars × 3 bytes each)
        // Truncating to 2 characters should give us "日本" (6 bytes)
        let input = Some("日本語");
        let result = truncate_str(input, 2);
        assert_eq!(result, Some("日本"));

        // Truncating to 5 characters on a 3-char string should return all of it
        let result = truncate_str(input, 5);
        assert_eq!(result, Some("日本語"));
    }

    /// Test that `truncate_str` correctly handles emoji (4-byte UTF-8 characters).
    #[test]
    fn test_truncate_str_emoji() {
        // 🎉 is 4 bytes, 🚀 is 4 bytes
        let input = Some("🎉🚀");
        // Truncating to 1 character should give us just the first emoji
        let result = truncate_str(input, 1);
        assert_eq!(result, Some("🎉"));

        // Truncating to 2 characters should give us both emojis
        let result = truncate_str(input, 2);
        assert_eq!(result, Some("🎉🚀"));
    }

    /// Test mixed ASCII and multi-byte characters
    #[test]
    fn test_truncate_str_mixed() {
        let input = Some("Hello世界!");
        // 8 characters: H, e, l, l, o, 世, 界, !
        let result = truncate_str(input, 6);
        assert_eq!(result, Some("Hello世"));
    }

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
        .expect("format record batch");

        insta::assert_snapshot!(formatted);

        let formatted = to_markdown_documents(
            &[batch],
            "content_chunk",
            Some("content"),
            &["location".to_string(), "dist".to_string()],
        )
        .expect("format record batch");

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
    fn test_truncate_large_list_array() {
        use arrow::array::{Int32Array, LargeListArray as LargeListArrayAlias};
        use arrow::buffer::OffsetBuffer;

        let values = Int32Array::from(vec![0, 1, 2, 3, 4, 5, 6, 7]);
        let offsets = OffsetBuffer::<i64>::new(vec![0_i64, 3, 6, 8].into());
        let input = LargeListArrayAlias::new(
            Arc::new(Field::new("item", DataType::Int32, true)),
            offsets,
            Arc::new(values),
            None,
        );

        let output =
            truncate_large_list_array(&input, 2).expect("truncate_large_list_array failed");

        assert_eq!(output.len(), 3);
        // Each sublist should now have at most 2 elements.
        let observed_lengths: Vec<usize> = (0..output.len())
            .map(|i| {
                let start = output.value_offsets()[i] as usize;
                let end = output.value_offsets()[i + 1] as usize;
                end - start
            })
            .collect();
        assert_eq!(observed_lengths, vec![2, 2, 2]);
    }

    /// Parent list-level nulls (which slots are NULL lists) must survive
    /// truncation. The bug we're guarding against: pulling the child array's
    /// null bitmap and stuffing it into the parent's `nulls` slot, which can
    /// flip valid slots to NULL or vice versa.
    #[test]
    fn test_truncate_list_array_preserves_parent_nulls() {
        let input = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(0), Some(1), Some(2), Some(3)]),
            None,
            Some(vec![Some(4), Some(5)]),
        ]);
        let parent_nulls_before: Vec<bool> = (0..input.len()).map(|i| input.is_null(i)).collect();

        let output = truncate_list_array(&input, 2).expect("truncate_list_array failed");
        let parent_nulls_after: Vec<bool> = (0..output.len()).map(|i| output.is_null(i)).collect();

        assert_eq!(parent_nulls_before, parent_nulls_after);
        assert_eq!(parent_nulls_after, vec![false, true, false]);
    }

    #[test]
    fn test_truncate_large_list_array_preserves_parent_nulls() {
        use arrow::array::{Int32Array, LargeListArray as LargeListArrayAlias};
        use arrow::buffer::{NullBuffer, OffsetBuffer};

        let values = Int32Array::from(vec![0, 1, 2, 3, 4, 5]);
        let offsets = OffsetBuffer::<i64>::new(vec![0_i64, 3, 3, 6].into());
        // Mark the middle slot as a NULL list.
        let nulls = NullBuffer::from(vec![true, false, true]);
        let input = LargeListArrayAlias::new(
            Arc::new(Field::new("item", DataType::Int32, true)),
            offsets,
            Arc::new(values),
            Some(nulls),
        );

        let output =
            truncate_large_list_array(&input, 2).expect("truncate_large_list_array failed");

        assert!(!output.is_null(0));
        assert!(output.is_null(1));
        assert!(!output.is_null(2));
    }

    /// Truncation must carry the original element `Field` through unchanged.
    /// Building a fresh field with a hardcoded name ("item") and
    /// `child_array.is_nullable()` (which reflects the *data*, not the
    /// declared schema) would silently rewrite the resulting `DataType` and
    /// drop any field-level metadata.
    #[test]
    fn test_truncate_helpers_preserve_element_field() {
        use arrow::array::{
            FixedSizeListArray as FixedSizeListArrayAlias, Int32Array,
            LargeListArray as LargeListArrayAlias, LargeListViewArray as LargeListViewArrayAlias,
            ListViewArray as ListViewArrayAlias,
        };
        use arrow::buffer::{OffsetBuffer, ScalarBuffer};

        let metadata: HashMap<String, String> = [("origin".to_string(), "audit_test".to_string())]
            .into_iter()
            .collect();
        // Original field has a custom name, declared-nullable=false, and metadata;
        // none of which the truncation helper should be free to discard.
        let element_field =
            Arc::new(Field::new("value", DataType::Int32, false).with_metadata(metadata.clone()));

        let values = || Int32Array::from(vec![0, 1, 2, 3, 4, 5, 6, 7]);

        let assert_field_preserved = |actual: &Arc<Field>| {
            assert_eq!(actual.name(), "value");
            assert!(!actual.is_nullable());
            assert_eq!(actual.metadata(), &metadata);
        };

        // List
        let list = ListArray::new(
            Arc::clone(&element_field),
            OffsetBuffer::<i32>::new(vec![0_i32, 3, 6, 8].into()),
            Arc::new(values()),
            None,
        );
        let output = truncate_list_array(&list, 2).expect("truncate List");
        match output.data_type() {
            DataType::List(field) => assert_field_preserved(field),
            other => panic!("unexpected {other:?}"),
        }

        // LargeList
        let large_list = LargeListArrayAlias::new(
            Arc::clone(&element_field),
            OffsetBuffer::<i64>::new(vec![0_i64, 3, 6, 8].into()),
            Arc::new(values()),
            None,
        );
        let output = truncate_large_list_array(&large_list, 2).expect("truncate LargeList");
        match output.data_type() {
            DataType::LargeList(field) => assert_field_preserved(field),
            other => panic!("unexpected {other:?}"),
        }

        // FixedSizeList
        let fixed_size_list = FixedSizeListArrayAlias::new(
            Arc::clone(&element_field),
            4,
            Arc::new(Int32Array::from(vec![0, 1, 2, 3, 4, 5, 6, 7])),
            None,
        );
        let output =
            truncate_fixed_size_list_array(&fixed_size_list, 2).expect("truncate FixedSizeList");
        match output.data_type() {
            DataType::FixedSizeList(field, _) => assert_field_preserved(field),
            other => panic!("unexpected {other:?}"),
        }

        // ListView
        let list_view = ListViewArrayAlias::try_new(
            Arc::clone(&element_field),
            ScalarBuffer::<i32>::from(vec![0_i32, 3, 6]),
            ScalarBuffer::<i32>::from(vec![3_i32, 3, 2]),
            Arc::new(values()),
            None,
        )
        .expect("ListViewArray construction");
        let output = truncate_list_view_array(&list_view, 2).expect("truncate ListView");
        match output.data_type() {
            DataType::ListView(field) => assert_field_preserved(field),
            other => panic!("unexpected {other:?}"),
        }

        // LargeListView
        let large_list_view = LargeListViewArrayAlias::try_new(
            Arc::clone(&element_field),
            ScalarBuffer::<i64>::from(vec![0_i64, 3, 6]),
            ScalarBuffer::<i64>::from(vec![3_i64, 3, 2]),
            Arc::new(values()),
            None,
        )
        .expect("LargeListViewArray construction");
        let output =
            truncate_large_list_view_array(&large_list_view, 2).expect("truncate LargeListView");
        match output.data_type() {
            DataType::LargeListView(field) => assert_field_preserved(field),
            other => panic!("unexpected {other:?}"),
        }
    }

    /// `TruncateUtf8Length` must recurse into every list-like variant, not
    /// just `List`. Otherwise strings nested under `LargeList` /
    /// `FixedSizeList` / `ListView` / `LargeListView` silently skip
    /// truncation when callers run `truncate_string_columns` over a record
    /// batch.
    #[test]
    fn test_truncate_utf8_recurses_into_all_list_variants() {
        use arrow::array::{
            FixedSizeListArray as FixedSizeListArrayAlias, LargeListArray as LargeListArrayAlias,
            LargeListViewArray as LargeListViewArrayAlias, ListViewArray as ListViewArrayAlias,
            StringArray,
        };
        use arrow::buffer::{OffsetBuffer, ScalarBuffer};

        fn assert_first_string_truncated(arr: &ArrayRef, expected_first_char: &str) {
            // Each variant stores its UTF8 elements contiguously in the child
            // array; inspecting element 0 is enough to prove truncation ran.
            let dt = arr.data_type().clone();
            let child: ArrayRef = match &dt {
                DataType::List(_) => arr
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .expect("ListArray")
                    .values()
                    .clone(),
                DataType::LargeList(_) => arr
                    .as_any()
                    .downcast_ref::<LargeListArrayAlias>()
                    .expect("LargeListArray")
                    .values()
                    .clone(),
                DataType::FixedSizeList(_, _) => arr
                    .as_any()
                    .downcast_ref::<FixedSizeListArrayAlias>()
                    .expect("FixedSizeListArray")
                    .values()
                    .clone(),
                DataType::ListView(_) => arr
                    .as_any()
                    .downcast_ref::<ListViewArrayAlias>()
                    .expect("ListViewArray")
                    .values()
                    .clone(),
                DataType::LargeListView(_) => arr
                    .as_any()
                    .downcast_ref::<LargeListViewArrayAlias>()
                    .expect("LargeListViewArray")
                    .values()
                    .clone(),
                other => panic!("unexpected outer type {other:?}"),
            };
            let strings = child
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("StringArray child");
            assert_eq!(strings.value(0), expected_first_char);
        }

        let inner_field = Arc::new(Field::new("item", DataType::Utf8, true));
        let strings = || StringArray::from(vec!["abcdef", "ghijkl"]);

        // List<Utf8>
        let list = ListArray::new(
            Arc::clone(&inner_field),
            OffsetBuffer::<i32>::new(vec![0_i32, 1, 2].into()),
            Arc::new(strings()),
            None,
        );
        let truncated = format_column_data(
            Arc::new(list) as ArrayRef,
            &Arc::new(Field::new(
                "col",
                DataType::List(Arc::clone(&inner_field)),
                true,
            )),
            FormatOperation::TruncateUtf8Length(1),
        )
        .expect("List Utf8 truncate");
        assert_first_string_truncated(&truncated, "a");

        // LargeList<Utf8>
        let large_list = LargeListArrayAlias::new(
            Arc::clone(&inner_field),
            OffsetBuffer::<i64>::new(vec![0_i64, 1, 2].into()),
            Arc::new(strings()),
            None,
        );
        let truncated = format_column_data(
            Arc::new(large_list) as ArrayRef,
            &Arc::new(Field::new(
                "col",
                DataType::LargeList(Arc::clone(&inner_field)),
                true,
            )),
            FormatOperation::TruncateUtf8Length(1),
        )
        .expect("LargeList Utf8 truncate");
        assert_first_string_truncated(&truncated, "a");

        // FixedSizeList<Utf8>[1]
        let fsl =
            FixedSizeListArrayAlias::new(Arc::clone(&inner_field), 1, Arc::new(strings()), None);
        let truncated = format_column_data(
            Arc::new(fsl) as ArrayRef,
            &Arc::new(Field::new(
                "col",
                DataType::FixedSizeList(Arc::clone(&inner_field), 1),
                true,
            )),
            FormatOperation::TruncateUtf8Length(1),
        )
        .expect("FixedSizeList Utf8 truncate");
        assert_first_string_truncated(&truncated, "a");

        // ListView<Utf8>
        let list_view = ListViewArrayAlias::try_new(
            Arc::clone(&inner_field),
            ScalarBuffer::<i32>::from(vec![0_i32, 1]),
            ScalarBuffer::<i32>::from(vec![1_i32, 1]),
            Arc::new(strings()),
            None,
        )
        .expect("ListViewArray construction");
        let truncated = format_column_data(
            Arc::new(list_view) as ArrayRef,
            &Arc::new(Field::new(
                "col",
                DataType::ListView(Arc::clone(&inner_field)),
                true,
            )),
            FormatOperation::TruncateUtf8Length(1),
        )
        .expect("ListView Utf8 truncate");
        assert_first_string_truncated(&truncated, "a");

        // LargeListView<Utf8>
        let large_list_view = LargeListViewArrayAlias::try_new(
            Arc::clone(&inner_field),
            ScalarBuffer::<i64>::from(vec![0_i64, 1]),
            ScalarBuffer::<i64>::from(vec![1_i64, 1]),
            Arc::new(strings()),
            None,
        )
        .expect("LargeListViewArray construction");
        let truncated = format_column_data(
            Arc::new(large_list_view) as ArrayRef,
            &Arc::new(Field::new(
                "col",
                DataType::LargeListView(Arc::clone(&inner_field)),
                true,
            )),
            FormatOperation::TruncateUtf8Length(1),
        )
        .expect("LargeListView Utf8 truncate");
        assert_first_string_truncated(&truncated, "a");
    }

    /// `arrow::compute::concat` errors when given an empty slice of arrays.
    /// 0-row record batches are a realistic input (e.g. a sample query that
    /// returns no rows), so each list truncation helper must short-circuit
    /// before reaching `concat`.
    #[test]
    fn test_truncate_helpers_handle_empty_arrays() {
        use arrow::array::{
            FixedSizeListArray as FixedSizeListArrayAlias, Int32Array,
            LargeListArray as LargeListArrayAlias, LargeListViewArray as LargeListViewArrayAlias,
            ListViewArray as ListViewArrayAlias,
        };
        use arrow::buffer::{OffsetBuffer, ScalarBuffer};

        // Empty ListArray
        let empty_list = ListArray::new(
            Arc::new(Field::new("item", DataType::Int32, true)),
            OffsetBuffer::<i32>::new(vec![0_i32].into()),
            Arc::new(Int32Array::from(Vec::<i32>::new())),
            None,
        );
        assert_eq!(
            truncate_list_array(&empty_list, 4)
                .expect("empty ListArray must round-trip")
                .len(),
            0
        );

        // Empty LargeListArray
        let empty_large_list = LargeListArrayAlias::new(
            Arc::new(Field::new("item", DataType::Int32, true)),
            OffsetBuffer::<i64>::new(vec![0_i64].into()),
            Arc::new(Int32Array::from(Vec::<i32>::new())),
            None,
        );
        assert_eq!(
            truncate_large_list_array(&empty_large_list, 4)
                .expect("empty LargeListArray must round-trip")
                .len(),
            0
        );

        // Empty FixedSizeListArray
        let empty_fsl = FixedSizeListArrayAlias::new(
            Arc::new(Field::new("item", DataType::Int32, true)),
            3,
            Arc::new(Int32Array::from(Vec::<i32>::new())),
            None,
        );
        assert_eq!(
            truncate_fixed_size_list_array(&empty_fsl, 2)
                .expect("empty FixedSizeListArray must round-trip")
                .len(),
            0
        );

        // Empty ListViewArray
        let empty_list_view = ListViewArrayAlias::try_new(
            Arc::new(Field::new("item", DataType::Int32, true)),
            ScalarBuffer::<i32>::from(Vec::<i32>::new()),
            ScalarBuffer::<i32>::from(Vec::<i32>::new()),
            Arc::new(Int32Array::from(Vec::<i32>::new())),
            None,
        )
        .expect("empty ListViewArray construction");
        assert_eq!(
            truncate_list_view_array(&empty_list_view, 4)
                .expect("empty ListViewArray must round-trip")
                .len(),
            0
        );

        // Empty LargeListViewArray
        let empty_large_list_view = LargeListViewArrayAlias::try_new(
            Arc::new(Field::new("item", DataType::Int32, true)),
            ScalarBuffer::<i64>::from(Vec::<i64>::new()),
            ScalarBuffer::<i64>::from(Vec::<i64>::new()),
            Arc::new(Int32Array::from(Vec::<i32>::new())),
            None,
        )
        .expect("empty LargeListViewArray construction");
        assert_eq!(
            truncate_large_list_view_array(&empty_large_list_view, 4)
                .expect("empty LargeListViewArray must round-trip")
                .len(),
            0
        );
    }

    #[test]
    fn test_truncate_fixed_size_list_array_preserves_parent_nulls() {
        use arrow::array::{FixedSizeListArray as FixedSizeListArrayAlias, Int32Array};
        use arrow::buffer::NullBuffer;

        let values = Int32Array::from(vec![0, 1, 2, 3, 4, 5, 6, 7, 8]);
        let nulls = NullBuffer::from(vec![true, false, true]);
        let input = FixedSizeListArrayAlias::new(
            Arc::new(Field::new("item", DataType::Int32, true)),
            3,
            Arc::new(values),
            Some(nulls),
        );

        let output = truncate_fixed_size_list_array(&input, 2)
            .expect("truncate_fixed_size_list_array failed");

        assert!(!output.is_null(0));
        assert!(output.is_null(1));
        assert!(!output.is_null(2));
    }

    #[test]
    fn test_truncate_list_view_array() {
        use arrow::array::{Int32Array, ListViewArray as ListViewArrayAlias};
        use arrow::buffer::ScalarBuffer;

        let values = Int32Array::from(vec![0, 1, 2, 3, 4, 5, 6, 7]);
        let offsets = ScalarBuffer::<i32>::from(vec![0_i32, 3, 6]);
        let sizes = ScalarBuffer::<i32>::from(vec![3_i32, 3, 2]);
        let input = ListViewArrayAlias::try_new(
            Arc::new(Field::new("item", DataType::Int32, true)),
            offsets,
            sizes,
            Arc::new(values),
            None,
        )
        .expect("ListViewArray::try_new");

        let output = truncate_list_view_array(&input, 2).expect("truncate_list_view_array failed");

        assert_eq!(output.len(), 3);
        // After truncation each entry has at most 2 elements.
        for size in output.value_sizes().iter() {
            assert!(*size <= 2, "size {size} exceeded max_len");
        }
    }

    #[test]
    fn test_truncate_large_list_view_array() {
        use arrow::array::{Int32Array, LargeListViewArray as LargeListViewArrayAlias};
        use arrow::buffer::ScalarBuffer;

        let values = Int32Array::from(vec![0, 1, 2, 3, 4, 5, 6, 7]);
        let offsets = ScalarBuffer::<i64>::from(vec![0_i64, 3, 6]);
        let sizes = ScalarBuffer::<i64>::from(vec![3_i64, 3, 2]);
        let input = LargeListViewArrayAlias::try_new(
            Arc::new(Field::new("item", DataType::Int32, true)),
            offsets,
            sizes,
            Arc::new(values),
            None,
        )
        .expect("LargeListViewArray::try_new");

        let output = truncate_large_list_view_array(&input, 2)
            .expect("truncate_large_list_view_array failed");

        assert_eq!(output.len(), 3);
        for size in output.value_sizes().iter() {
            assert!(*size <= 2, "size {size} exceeded max_len");
        }
    }

    #[test]
    fn test_get_possible_nested_list_datatype_view_variants() {
        let inner = DataType::Int32;
        let cases = vec![
            DataType::List(Arc::new(Field::new("item", inner.clone(), true))),
            DataType::LargeList(Arc::new(Field::new("item", inner.clone(), true))),
            DataType::FixedSizeList(Arc::new(Field::new("item", inner.clone(), true)), 4),
            DataType::ListView(Arc::new(Field::new("item", inner.clone(), true))),
            DataType::LargeListView(Arc::new(Field::new("item", inner.clone(), true))),
        ];
        for dt in cases {
            let f = Arc::new(Field::new("col", dt, true));
            let (_, inner_dt) = get_possible_nested_list_datatype(&f);
            assert_eq!(inner_dt.as_ref(), Some(&inner), "field {f:?}");
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

    #[test]
    fn test_pretty_print_schema() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new(
                "scores",
                DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
                true,
            ),
            Field::new(
                "metadata",
                DataType::Struct(
                    vec![
                        Field::new("key", DataType::Utf8, false),
                        Field::new("value", DataType::Utf8, true),
                    ]
                    .into(),
                ),
                true,
            ),
        ]));

        let mut pretty_output = String::new();
        pretty_print_schema(&schema, &mut pretty_output).expect("write schema");
        insta::assert_snapshot!(pretty_output, @r"
        id: int64
        name: utf8 (nullable)
        scores: list<item: float32> (nullable)
        metadata: struct<key utf8, value utf8> (nullable)
        ");
    }

    /// max_len = 0 is a valid (if unusual) truncation request: every list
    /// must become a 0-element list while parent nulls and the element Field
    /// (including metadata/nullable) are preserved exactly.
    #[test]
    fn test_truncate_list_to_zero_elements() {
        use arrow::array::{Int32Array, ListArray as ListArrayAlias};
        use arrow::buffer::{NullBuffer, OffsetBuffer};

        let element_field = Arc::new(
            Field::new("value", DataType::Int32, false)
                .with_metadata([("audit".to_string(), "zero_len".to_string())].into()),
        );

        // List<i32> with mixed lengths + one parent NULL
        // lists: [0,1,2], [], [3,4,5], [6]  (values 0..6)
        let list = ListArrayAlias::new(
            Arc::clone(&element_field),
            OffsetBuffer::<i32>::new(vec![0_i32, 3, 3, 6, 7].into()),
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7])),
            Some(NullBuffer::from(vec![true, false, true, true])),
        );
        let out = truncate_list_array(&list, 0).expect("truncate to 0");
        assert_eq!(out.len(), 4);
        assert!(out.is_null(1));
        assert!(!out.is_null(0));
        // All non-null lists must report length 0
        for i in [0, 2, 3] {
            assert!(!out.is_null(i));
            assert_eq!(out.value(i).len(), 0);
        }
        // Element field preserved
        if let DataType::List(f) = out.data_type() {
            assert_eq!(f.name(), "value");
            assert!(!f.is_nullable());
            assert_eq!(f.metadata().get("audit"), Some(&"zero_len".to_string()));
        } else {
            panic!("expected List");
        }
    }

    /// Fast-path + max_len=0 for ListView (non-contiguous layout).
    #[test]
    fn test_truncate_list_view_to_zero_and_fast_path() {
        use arrow::array::{Int32Array, ListViewArray as ListViewArrayAlias};
        use arrow::buffer::{NullBuffer, ScalarBuffer};

        let element_field = Arc::new(Field::new("item", DataType::Int32, true));
        let values = Int32Array::from(vec![10, 20, 30, 40]);
        // Two lists: [10,20,30] at offset 0 size 3, and a NULL list, and [40]
        let offsets = ScalarBuffer::<i32>::from(vec![0_i32, 0, 3]);
        let sizes = ScalarBuffer::<i32>::from(vec![3_i32, 0, 1]);
        let nulls = NullBuffer::from(vec![true, false, true]);
        let input = ListViewArrayAlias::try_new(
            Arc::clone(&element_field),
            offsets,
            sizes,
            Arc::new(values),
            Some(nulls),
        )
        .expect("ListView construction");

        // Fast path: max_len larger than any size (including the 0-size one)
        let out_fast = truncate_list_view_array(&input, 100).expect("fast path");
        assert_eq!(out_fast.len(), 3);
        assert!(out_fast.is_null(1));
        assert_eq!(out_fast.value_sizes().as_ref(), &[3, 0, 1]); // unchanged layout

        // max_len=0 path
        let out_zero = truncate_list_view_array(&input, 0).expect("zero len view");
        assert_eq!(out_zero.len(), 3);
        assert!(out_zero.is_null(1));
        for i in [0, 2] {
            assert!(!out_zero.is_null(i));
            assert_eq!(out_zero.value(i).len(), 0);
        }
    }

    /// Using the public truncate_numeric_column_length API with a RecordBatch
    /// that contains a mix of list columns that do and do not need truncation.
    /// When the fast path triggers for some columns, the original column Arc
    /// should be reused (identity) for those that didn't change.
    #[test]
    fn test_truncate_numeric_column_length_fast_path_and_mixed() {
        use arrow::array::{Int32Array, ListArray as ListArrayAlias};
        use arrow::buffer::OffsetBuffer;
        use arrow::datatypes::Field as ArrowField;

        use crate::record_batch::truncate_numeric_column_length;

        let schema = Arc::new(Schema::new(vec![
            ArrowField::new(
                "short_lists",
                DataType::List(Arc::new(Field::new("v", DataType::Int32, true))),
                true,
            ),
            ArrowField::new(
                "long_lists",
                DataType::List(Arc::new(Field::new("v", DataType::Int32, true))),
                true,
            ),
        ]));

        let short = ListArrayAlias::new(
            Arc::new(Field::new("v", DataType::Int32, true)),
            OffsetBuffer::<i32>::new(vec![0_i32, 1, 2].into()),
            Arc::new(Int32Array::from(vec![1, 2])),
            None,
        );
        let long = ListArrayAlias::new(
            Arc::new(Field::new("v", DataType::Int32, true)),
            OffsetBuffer::<i32>::new(vec![0_i32, 5, 10].into()),
            Arc::new(Int32Array::from((0..10).collect::<Vec<_>>())),
            None,
        );

        let batch =
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(short), Arc::new(long)])
                .expect("batch");

        // max_elements=3 : short_lists hits fast path (no change), long_lists truncated
        let processed =
            truncate_numeric_column_length(&batch, 3).expect("truncate_numeric_column_length");

        assert_eq!(processed.num_columns(), 2);
        // First column should be the exact same Arc (fast path clone in practice
        // returns the input, but RecordBatch construction may not dedup Arcs;
        // we at least verify logical identity of data).
        let short_in = batch.column(0);
        let short_out = processed.column(0);
        assert_eq!(short_in.data_type(), short_out.data_type());
        assert_eq!(short_in.len(), short_out.len());
        // Second column must have been truncated (lengths now <=3)
        let long_out = processed
            .column(1)
            .as_any()
            .downcast_ref::<ListArrayAlias>()
            .expect("ListArray");
        for i in 0..long_out.len() {
            assert!(long_out.value(i).len() <= 3);
        }
    }
}
