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

use arrow::{
    array::{
        ArrayRef, ListArray, ListBuilder, StringBuilder, UInt32Builder, UInt64Builder,
        new_empty_array,
    },
    compute::{TakeOptions, take},
};
use arrow_schema::ArrowError;

/// Converts string-like Arrow types into an iterator [`Option<Box<dyn Iterator<Item = Option<&str>>>>`]. If the downcast conversion
/// fails, returns `None`.
#[macro_export]
macro_rules! convert_string_arrow_to_iterator {
    ($data:expr) => {{
        None.or($data
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|arr| Box::new(arr.iter()) as Box<dyn Iterator<Item = Option<&str>> + Send>))
            .or($data
                .as_any()
                .downcast_ref::<StringViewArray>()
                .map(|arr| Box::new(arr.iter()) as Box<dyn Iterator<Item = Option<&str>> + Send>))
            .or($data
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .map(|arr| Box::new(arr.iter()) as Box<dyn Iterator<Item = Option<&str>> + Send>))
    }};
}

/// Repeat each element in `arr` according to `repeats`.
/// `repeats.len()` must equal `arr.len()`.
///
/// # Errors
///
/// This function will return an [`ArrowError`] if `arr` and `repeats` are unequal.
#[allow(clippy::checked_conversions, clippy::cast_possible_truncation)]
pub fn repeat(arr: &ArrayRef, repeats: &[usize]) -> Result<ArrayRef, ArrowError> {
    let len = arr.len();
    if repeats.len() != len {
        return Err(ArrowError::ComputeError(
            "repeats.len() must equal arr.len()".to_string(),
        ));
    }

    let total: usize = repeats.iter().sum();
    if total == 0 {
        // Return an empty array with the same data type
        return Ok(new_empty_array(arr.data_type()));
    }

    // Choose index width based on number of input rows
    if len <= u32::MAX as usize {
        let mut builder = UInt32Builder::with_capacity(total);
        for (i, &count) in repeats.iter().enumerate() {
            builder.append_value_n(i as u32, count);
        }
        let indices = builder.finish();
        let options = TakeOptions { check_bounds: true };
        take(arr.as_ref(), &indices, Some(options))
    } else {
        let mut builder = UInt64Builder::with_capacity(total);
        for (i, &count) in repeats.iter().enumerate() {
            builder.append_value_n(i as u64, count);
        }
        let indices = builder.finish();
        let options = TakeOptions { check_bounds: true };
        take(arr.as_ref(), &indices, Some(options))
    }
}

#[must_use]
pub fn to_list_array(chunks: &[Vec<&str>]) -> ListArray {
    let mut builder = ListBuilder::new(StringBuilder::new());

    for chunk in chunks {
        if chunk.is_empty() {
            builder.append_null();
        } else {
            for item in chunk {
                builder.values().append_value(item);
            }
            builder.append(true);
        }
    }

    builder.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int32Array, StringArray};
    use std::sync::Arc;

    #[test]
    fn test_repeat_basic() {
        let arr: ArrayRef = Arc::new(StringArray::from(vec!["foo", "bar", "baz"])) as ArrayRef;
        let repeats: Vec<usize> = vec![2, 3, 1];
        let result = repeat(&arr, &repeats).expect("failed to call 'repeat'");

        let string_result = result
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("failed to downcast result to 'StringArray'");
        let expected = ["foo", "foo", "bar", "bar", "bar", "baz"];

        assert_eq!(string_result.len(), 6);
        for (i, expected_val) in expected.iter().enumerate() {
            assert_eq!(string_result.value(i), *expected_val);
        }
    }

    #[test]
    fn test_repeat_with_zeros() {
        let arr: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4]));
        let repeats = vec![1, 0, 3, 2];
        let result = repeat(&arr, &repeats).expect("failed to call 'repeat'");

        let int_result = result
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("failed to downcast result to 'Int32Array'");
        let expected = [1, 3, 3, 3, 4, 4];

        assert_eq!(int_result.len(), 6);
        for (i, expected_val) in expected.iter().enumerate() {
            assert_eq!(int_result.value(i), *expected_val);
        }
    }

    #[test]
    fn test_repeat_empty() {
        let arr: ArrayRef = Arc::new(StringArray::from(vec![] as Vec<&str>));
        let repeats = vec![];
        let result = repeat(&arr, &repeats).expect("failed to call 'repeat'");
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_repeat_mismatched_lengths() {
        let arr: ArrayRef = Arc::new(StringArray::from(vec!["foo", "bar"]));
        let repeats = vec![1, 2, 3];
        let _ =
            repeat(&arr, &repeats).expect_err("should error if lengths of inputs are mismatched");
    }
}
