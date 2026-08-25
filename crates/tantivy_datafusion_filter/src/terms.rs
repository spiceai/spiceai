/*
Copyright 2024-2026 The Spice.ai OSS Authors
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

//! Generic Arrow-array → tantivy [`Term`] encoding, shared by index writes and by literal
//! encoding in [`crate::filter`] so both encode a value identically.

use arrow::{
    array::{
        Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Date64Array, Float16Array,
        Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
        LargeBinaryArray, LargeStringArray, StringArray, UInt8Array, UInt16Array, UInt32Array,
        UInt64Array,
    },
    datatypes::DataType,
    error::ArrowError,
};
use tantivy::{Term, schema::Field};

/// Macro to downcast an `ArrayRef` to concrete Arrow array type or return Err.
///
/// Users should check type-compatibility beforehand using [`ArrayRef::data_type`].
macro_rules! downcast_array {
    ($ARRAY:expr, $TY:ty) => {
        $ARRAY.as_any().downcast_ref::<$TY>().ok_or_else(|| {
            ArrowError::CastError(format!("Expected arrow array of type {}", stringify!($TY)))
        })?
    };
}

/// Encode every non-null value of an Arrow array into a tantivy [`Term`] for `field`, using the
/// same per-type encoding tantivy applies on index write so a literal matches an indexed value.
///
/// # Errors
///
/// Returns [`ArrowError::NotYetImplemented`] for an Arrow type that has no tantivy term encoding,
/// or [`ArrowError::CastError`] if `arr` does not downcast to the concrete array for its
/// [`ArrayRef::data_type`].
pub fn array_to_terms(field: Field, arr: &ArrayRef) -> Result<Vec<Term>, ArrowError> {
    let mut terms = Vec::with_capacity(arr.len());

    match arr.data_type() {
        // --- Floats → f64
        DataType::Float16 => {
            let a = downcast_array!(arr, Float16Array);
            for i in 0..a.len() {
                if a.is_valid(i) {
                    let v = f64::from(a.value(i).to_f32());
                    terms.push(Term::from_field_f64(field, v));
                }
            }
        }
        DataType::Float32 => {
            let a = downcast_array!(arr, Float32Array);
            for i in 0..a.len() {
                if a.is_valid(i) {
                    let v = f64::from(a.value(i));
                    terms.push(Term::from_field_f64(field, v));
                }
            }
        }
        DataType::Float64 => {
            let a = downcast_array!(arr, Float64Array);
            for i in 0..a.len() {
                if a.is_valid(i) {
                    terms.push(Term::from_field_f64(field, a.value(i)));
                }
            }
        }

        // --- Unsigned ints → u64
        DataType::UInt8 => {
            let a = downcast_array!(arr, UInt8Array);
            for i in 0..a.len() {
                if a.is_valid(i) {
                    terms.push(Term::from_field_u64(field, u64::from(a.value(i))));
                }
            }
        }
        DataType::UInt16 => {
            let a = downcast_array!(arr, UInt16Array);
            for i in 0..a.len() {
                if a.is_valid(i) {
                    terms.push(Term::from_field_u64(field, u64::from(a.value(i))));
                }
            }
        }
        DataType::UInt32 => {
            let a = downcast_array!(arr, UInt32Array);
            for i in 0..a.len() {
                if a.is_valid(i) {
                    terms.push(Term::from_field_u64(field, u64::from(a.value(i))));
                }
            }
        }
        DataType::UInt64 => {
            let a = downcast_array!(arr, UInt64Array);
            for i in 0..a.len() {
                if a.is_valid(i) {
                    terms.push(Term::from_field_u64(field, a.value(i)));
                }
            }
        }

        // --- Signed ints → i64
        DataType::Int8 => {
            let a = downcast_array!(arr, Int8Array);
            for i in 0..a.len() {
                if a.is_valid(i) {
                    terms.push(Term::from_field_i64(field, i64::from(a.value(i))));
                }
            }
        }
        DataType::Int16 => {
            let a = downcast_array!(arr, Int16Array);
            for i in 0..a.len() {
                if a.is_valid(i) {
                    terms.push(Term::from_field_i64(field, i64::from(a.value(i))));
                }
            }
        }
        DataType::Int32 => {
            let a = downcast_array!(arr, Int32Array);
            for i in 0..a.len() {
                if a.is_valid(i) {
                    terms.push(Term::from_field_i64(field, i64::from(a.value(i))));
                }
            }
        }
        DataType::Int64 => {
            let a = downcast_array!(arr, Int64Array);
            for i in 0..a.len() {
                if a.is_valid(i) {
                    terms.push(Term::from_field_i64(field, a.value(i)));
                }
            }
        }

        // --- Boolean
        DataType::Boolean => {
            let a = downcast_array!(arr, BooleanArray);
            for i in 0..a.len() {
                if a.is_valid(i) {
                    terms.push(Term::from_field_bool(field, a.value(i)));
                }
            }
        }

        // --- Dates
        DataType::Date32 => {
            let a = downcast_array!(arr, Date32Array);
            for i in 0..a.len() {
                if a.is_valid(i) {
                    terms.push(Term::from_field_date(
                        field,
                        tantivy::DateTime::from_timestamp_secs(i64::from(a.value(i)) * 86_400),
                    ));
                }
            }
        }
        DataType::Date64 => {
            let a = downcast_array!(arr, Date64Array);
            for i in 0..a.len() {
                if a.is_valid(i) {
                    terms.push(Term::from_field_date(
                        field,
                        tantivy::DateTime::from_timestamp_millis(a.value(i)),
                    ));
                }
            }
        }

        // --- UTF8 text
        DataType::Utf8 => {
            let a = downcast_array!(arr, StringArray);
            for i in 0..a.len() {
                if a.is_valid(i) {
                    terms.push(Term::from_field_text(field, a.value(i)));
                }
            }
        }
        DataType::LargeUtf8 => {
            let a = downcast_array!(arr, LargeStringArray);
            for i in 0..a.len() {
                if a.is_valid(i) {
                    terms.push(Term::from_field_text(field, a.value(i)));
                }
            }
        }

        // --- Binary blobs
        DataType::Binary => {
            let a = downcast_array!(arr, BinaryArray);
            for i in 0..a.len() {
                if a.is_valid(i) {
                    terms.push(Term::from_field_bytes(field, a.value(i)));
                }
            }
        }
        DataType::LargeBinary => {
            let a = downcast_array!(arr, LargeBinaryArray);
            for i in 0..a.len() {
                if a.is_valid(i) {
                    terms.push(Term::from_field_bytes(field, a.value(i)));
                }
            }
        }

        // --- Everything else is unsupported
        other => {
            return Err(ArrowError::NotYetImplemented(format!(
                "Cannot use primary key of arrow type {other:?} for full-text search"
            )));
        }
    }

    Ok(terms)
}
