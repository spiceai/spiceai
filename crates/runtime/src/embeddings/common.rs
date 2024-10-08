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
use arrow::datatypes::{DataType, Field, SchemaRef};

#[macro_export]
/// Generate the name of the embedding column for a given column
/// ```rust
/// let col = "temperature";
/// assert_eq!(
///    embedding_col!(col),
///    "temperature_embedding"
/// );
macro_rules! embedding_col {
    ($col:expr) => {
        format!("{}_embedding", $col)
    };
}

#[macro_export]
/// Generate the name of the embedding offset column for a given column
/// ```rust
/// let col = "temperature";
/// assert_eq!(
///     offset_col!(col),
///     "temperature_offset"
/// );
/// ```
macro_rules! offset_col {
    ($col:expr) => {
        format!("{}_offset", $col)
    };
}

pub(crate) fn is_float_type(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Float16 | DataType::Float32 | DataType::Float64
    )
}

pub(crate) fn is_fixed_size_list_of_floats(dt: &DataType) -> bool {
    matches!(dt, DataType::FixedSizeList(field, _) if is_float_type(field.data_type()))
}

pub(crate) fn is_valid_embedding_type(dt: &DataType) -> bool {
    match dt {
        DataType::List(inner) | DataType::LargeList(inner) | DataType::FixedSizeList(inner, _) => {
            match inner.data_type() {
                // Doubly nested list
                DataType::FixedSizeList(_, _) => is_fixed_size_list_of_floats(inner.data_type()),
                // Single nested list
                dt => is_float_type(dt),
            }
        }
        _ => false,
    }
}

pub(crate) fn is_valid_offset_type(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::List(inner) if matches!(
            inner.data_type(),
            DataType::FixedSizeList(offset_field, 2) if matches!(offset_field.data_type(), DataType::Int32)
        )
    )
}
