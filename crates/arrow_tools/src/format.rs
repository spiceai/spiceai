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

use arrow::array::{Array, ArrayRef, FixedSizeListArray, ListArray, StructArray};
use arrow::buffer::OffsetBuffer;
use arrow::compute::concat;
use arrow_schema::{ArrowError, DataType, Field};
use std::sync::Arc;

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
        Arc::new(Field::new_list(
            "item",
            Field::new(
                "item",
                child_array.data_type().clone(),
                child_array.is_nullable(),
            ),
            list_array.is_nullable(),
        )),
        OffsetBuffer::from_lengths(new_lengths),
        new_child_array,
        nulls,
    )
}
