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

//! Numeric conversion utilities for Cayenne.
//!
//! This module provides type-safe conversion functions for numeric types,
//! with proper error handling and contextual error messages.

use std::convert::TryInto;

/// Generic conversion function that handles type conversion with proper error handling.
///
/// This is the core conversion utility that all type conversion functions delegate to.
/// It provides consistent error messages with context about what value failed to convert
/// and includes both the source and target types in the error message.
///
/// # Design Rationale
///
/// This function consolidates conversion logic to ensure consistent error handling across
/// all numeric conversions in Cayenne. Without it, we would have duplicate error handling
/// code for each conversion pair (usize→i64, u64→i64, etc.), making it harder to maintain
/// consistent error messages.
///
/// # Generic Parameters
///
/// * `T` - Source type (must implement `TryInto<U>`, `Copy`, and `Display`)
/// * `U` - Target type (must implement `Display`)
///
/// # Why `Copy` is Required
///
/// The `Copy` bound is required because:
/// 1. The value is used twice: once for the conversion attempt and once in the error message
/// 2. `TryInto::try_into` consumes `self`, so without `Copy` we would move the value
/// 3. All numeric types (`usize`, `u64`, `i64`, etc.) implement `Copy`, so this isn't restrictive
///
/// # When to Use
///
/// Use this function when you need a numeric conversion that returns
/// `datafusion_common::Result<T>`.
///
/// For boxed-error conversions to `u64`, prefer `convert_to_u64_box()`.
///
/// # Examples
///
/// ```ignore
/// let value = try_convert::<usize, i64>(batch.num_rows(), "batch size")?;
/// ```
pub fn try_convert<T, U>(value: T, context: &str) -> datafusion_common::Result<U>
where
    T: TryInto<U> + Copy + std::fmt::Display,
    T::Error: std::error::Error + Send + Sync + 'static,
    U: std::fmt::Display,
{
    value.try_into().map_err(|err| {
        datafusion_common::DataFusionError::Execution(format!(
            "Failed to convert {context} value {value} to {}: {err}",
            std::any::type_name::<U>()
        ))
    })
}

/// Convert a numeric value to `u64` with boxed error type.
///
/// Use this function when converting numeric values to `u64` in contexts that require
/// boxed errors. This is primarily used for return values that must be `u64`, such as
/// row counts returned from deletion operations.
///
/// # Arguments
///
/// * `value` - The numeric value to convert
/// * `context` - Description of what the value represents (e.g., "deleted row count")
///
/// # Examples
///
/// ```ignore
/// // Converting deletion count from usize to u64
/// let deleted_count = convert_to_u64_box(row_ids.len(), "deleted row count")?;
/// ```
pub fn convert_to_u64_box<T>(
    value: T,
    context: &str,
) -> Result<u64, Box<dyn std::error::Error + Send + Sync>>
where
    T: TryInto<u64> + Copy + std::fmt::Display,
    T::Error: std::error::Error + Send + Sync + 'static,
{
    try_convert::<T, u64>(value, context)
        .map_err(|err| Box::new(err) as Box<dyn std::error::Error + Send + Sync>)
}
