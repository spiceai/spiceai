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

//! Generic Arrow-array → tantivy encoding, shared by index writes (both the delete [`Term`] and
//! the inserted [`TantivyDocument`] field value) and by literal encoding in [`crate::filter`], so
//! all three encode a value identically. A single encoding never disagreeing with itself is the
//! fix for #12235: previously the delete term was built from the raw Arrow value while the
//! inserted document went through an arrow-json → tantivy-JSON round trip, and the two diverged
//! for Float32/Float16 (different f64 bit pattern), Binary (hex vs. base64), and
//! Utf8View/BinaryView (unimplemented on one side only).

use arrow::{
    array::{
        Array, ArrayRef, BinaryArray, BinaryViewArray, BooleanArray, Date32Array, Date64Array,
        Float16Array, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
        LargeBinaryArray, LargeStringArray, StringArray, StringViewArray, UInt8Array, UInt16Array,
        UInt32Array, UInt64Array,
    },
    datatypes::DataType,
    error::ArrowError,
};
use tantivy::{TantivyDocument, Term, schema::Field};

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

/// A single row's value, decoded from an Arrow array into the one typed representation from
/// which both a delete [`Term`] and an inserted document's field value are derived. Keeping this
/// as one enum (rather than duplicating the match-per-Arrow-type logic in two functions) is what
/// guarantees the delete and insert paths can never independently drift again.
enum RowValue<'a> {
    F64(f64),
    U64(u64),
    I64(i64),
    Bool(bool),
    Date(tantivy::DateTime),
    Text(&'a str),
    Bytes(&'a [u8]),
}

impl RowValue<'_> {
    fn to_term(&self, field: Field) -> Term {
        match self {
            RowValue::F64(v) => Term::from_field_f64(field, *v),
            RowValue::U64(v) => Term::from_field_u64(field, *v),
            RowValue::I64(v) => Term::from_field_i64(field, *v),
            RowValue::Bool(v) => Term::from_field_bool(field, *v),
            RowValue::Date(v) => Term::from_field_date(field, *v),
            RowValue::Text(v) => Term::from_field_text(field, v),
            RowValue::Bytes(v) => Term::from_field_bytes(field, v),
        }
    }

    fn add_to_document(&self, field: Field, doc: &mut TantivyDocument) {
        match self {
            RowValue::F64(v) => doc.add_f64(field, *v),
            RowValue::U64(v) => doc.add_u64(field, *v),
            RowValue::I64(v) => doc.add_i64(field, *v),
            RowValue::Bool(v) => doc.add_bool(field, *v),
            RowValue::Date(v) => doc.add_date(field, *v),
            RowValue::Text(v) => doc.add_text(field, v),
            RowValue::Bytes(v) => doc.add_bytes(field, v),
        }
    }
}

/// Decode every row of `arr` into a [`RowValue`] (`None` for a null row), preserving `arr`'s row
/// order and length.
///
/// # Errors
///
/// Returns [`ArrowError::NotYetImplemented`] for an Arrow type with no tantivy encoding, or
/// [`ArrowError::CastError`] if `arr` does not downcast to the concrete array for its
/// [`ArrayRef::data_type`].
fn array_to_row_values(arr: &ArrayRef) -> Result<Vec<Option<RowValue<'_>>>, ArrowError> {
    let mut values: Vec<Option<RowValue>> = Vec::with_capacity(arr.len());

    macro_rules! push_each {
        ($ARR:expr, $MAP:expr) => {
            for i in 0..$ARR.len() {
                values.push(if $ARR.is_valid(i) {
                    Some($MAP($ARR, i))
                } else {
                    None
                });
            }
        };
    }

    match arr.data_type() {
        // --- Floats → f64
        DataType::Float16 => {
            let a = downcast_array!(arr, Float16Array);
            push_each!(a, |a: &Float16Array, i: usize| RowValue::F64(f64::from(
                a.value(i).to_f32()
            )));
        }
        DataType::Float32 => {
            let a = downcast_array!(arr, Float32Array);
            push_each!(a, |a: &Float32Array, i: usize| RowValue::F64(f64::from(
                a.value(i)
            )));
        }
        DataType::Float64 => {
            let a = downcast_array!(arr, Float64Array);
            push_each!(a, |a: &Float64Array, i: usize| RowValue::F64(a.value(i)));
        }

        // --- Unsigned ints → u64
        DataType::UInt8 => {
            let a = downcast_array!(arr, UInt8Array);
            push_each!(a, |a: &UInt8Array, i: usize| RowValue::U64(u64::from(
                a.value(i)
            )));
        }
        DataType::UInt16 => {
            let a = downcast_array!(arr, UInt16Array);
            push_each!(a, |a: &UInt16Array, i: usize| RowValue::U64(u64::from(
                a.value(i)
            )));
        }
        DataType::UInt32 => {
            let a = downcast_array!(arr, UInt32Array);
            push_each!(a, |a: &UInt32Array, i: usize| RowValue::U64(u64::from(
                a.value(i)
            )));
        }
        DataType::UInt64 => {
            let a = downcast_array!(arr, UInt64Array);
            push_each!(a, |a: &UInt64Array, i: usize| RowValue::U64(a.value(i)));
        }

        // --- Signed ints → i64
        DataType::Int8 => {
            let a = downcast_array!(arr, Int8Array);
            push_each!(a, |a: &Int8Array, i: usize| RowValue::I64(i64::from(
                a.value(i)
            )));
        }
        DataType::Int16 => {
            let a = downcast_array!(arr, Int16Array);
            push_each!(a, |a: &Int16Array, i: usize| RowValue::I64(i64::from(
                a.value(i)
            )));
        }
        DataType::Int32 => {
            let a = downcast_array!(arr, Int32Array);
            push_each!(a, |a: &Int32Array, i: usize| RowValue::I64(i64::from(
                a.value(i)
            )));
        }
        DataType::Int64 => {
            let a = downcast_array!(arr, Int64Array);
            push_each!(a, |a: &Int64Array, i: usize| RowValue::I64(a.value(i)));
        }

        // --- Boolean
        DataType::Boolean => {
            let a = downcast_array!(arr, BooleanArray);
            push_each!(a, |a: &BooleanArray, i: usize| RowValue::Bool(a.value(i)));
        }

        // --- Dates
        DataType::Date32 => {
            let a = downcast_array!(arr, Date32Array);
            push_each!(a, |a: &Date32Array, i: usize| RowValue::Date(
                tantivy::DateTime::from_timestamp_secs(i64::from(a.value(i)) * 86_400)
            ));
        }
        DataType::Date64 => {
            let a = downcast_array!(arr, Date64Array);
            push_each!(a, |a: &Date64Array, i: usize| RowValue::Date(
                tantivy::DateTime::from_timestamp_millis(a.value(i))
            ));
        }

        // --- UTF8 text
        //
        // Borrowed text/bytes variants tie `RowValue`'s lifetime to `arr`, which a closure's own
        // elided lifetime can't express (rustc rejects `push_each!` here) — write these arms out
        // directly instead.
        DataType::Utf8 => {
            let a = downcast_array!(arr, StringArray);
            for i in 0..a.len() {
                values.push(a.is_valid(i).then(|| RowValue::Text(a.value(i))));
            }
        }
        DataType::LargeUtf8 => {
            let a = downcast_array!(arr, LargeStringArray);
            for i in 0..a.len() {
                values.push(a.is_valid(i).then(|| RowValue::Text(a.value(i))));
            }
        }
        DataType::Utf8View => {
            let a = downcast_array!(arr, StringViewArray);
            for i in 0..a.len() {
                values.push(a.is_valid(i).then(|| RowValue::Text(a.value(i))));
            }
        }

        // --- Binary blobs
        DataType::Binary => {
            let a = downcast_array!(arr, BinaryArray);
            for i in 0..a.len() {
                values.push(a.is_valid(i).then(|| RowValue::Bytes(a.value(i))));
            }
        }
        DataType::LargeBinary => {
            let a = downcast_array!(arr, LargeBinaryArray);
            for i in 0..a.len() {
                values.push(a.is_valid(i).then(|| RowValue::Bytes(a.value(i))));
            }
        }
        DataType::BinaryView => {
            let a = downcast_array!(arr, BinaryViewArray);
            for i in 0..a.len() {
                values.push(a.is_valid(i).then(|| RowValue::Bytes(a.value(i))));
            }
        }

        // --- Everything else is unsupported
        other => {
            return Err(ArrowError::NotYetImplemented(format!(
                "Cannot use primary key of arrow type {other:?} for full-text search"
            )));
        }
    }

    Ok(values)
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
    Ok(array_to_row_values(arr)?
        .into_iter()
        .flatten()
        .map(|v| v.to_term(field))
        .collect())
}

/// Sets `field`'s value on each of `docs` directly from `arr`, row-for-row (`docs[i]` gets
/// `arr`'s row `i`; a null row is left unset). Uses the exact same per-type conversion as
/// [`array_to_terms`], so a document built this way is guaranteed to contain the value that
/// `array_to_terms` would build a matching delete [`Term`] for — see the module docs for why that
/// guarantee otherwise breaks (#12235).
///
/// # Errors
///
/// Returns [`ArrowError::NotYetImplemented`]/[`ArrowError::CastError`] as [`array_to_terms`], or
/// [`ArrowError::InvalidArgumentError`] if `arr.len() != docs.len()`.
pub fn set_document_values(
    field: Field,
    arr: &ArrayRef,
    docs: &mut [TantivyDocument],
) -> Result<(), ArrowError> {
    if arr.len() != docs.len() {
        return Err(ArrowError::InvalidArgumentError(format!(
            "Array of length {} does not match document count {}",
            arr.len(),
            docs.len()
        )));
    }

    for (doc, value) in docs.iter_mut().zip(array_to_row_values(arr)?) {
        if let Some(v) = value {
            v.add_to_document(field, doc);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{BinaryViewArray, Float32Array, StringViewArray};
    use std::sync::Arc;
    use tantivy::Index;
    use tantivy::schema::{INDEXED, STORED, STRING, Schema as TantivySchema};

    /// Indexes a single-row `arr` exactly as the search crate's `update_index` now does
    /// (`set_document_values` for the insert, `array_to_terms` for the delete), then reports how
    /// many documents remain. A value whose delete term doesn't match what was actually indexed
    /// leaves this at 1 instead of 0 — the exact failure mode of #12235.
    fn index_then_delete_count(schema: TantivySchema, field: Field, arr: &ArrayRef) -> u64 {
        let index = Index::create_in_ram(schema);
        let mut writer = index.writer(15_000_000).expect("index writer");

        let mut doc = TantivyDocument::default();
        set_document_values(field, arr, std::slice::from_mut(&mut doc)).expect("set doc value");
        writer.add_document(doc).expect("add document");
        writer.commit().expect("commit insert");

        for term in array_to_terms(field, arr).expect("delete terms") {
            writer.delete_term(term);
        }
        writer.commit().expect("commit delete");

        let reader = index.reader().expect("reader");
        reader.reload().expect("reload");
        reader.searcher().num_docs()
    }

    #[test]
    fn float32_delete_matches_the_indexed_value() {
        let mut builder = TantivySchema::builder();
        let field = builder.add_f64_field("pk", INDEXED | STORED);
        let arr: ArrayRef = Arc::new(Float32Array::from(vec![0.1_f32]));

        assert_eq!(index_then_delete_count(builder.build(), field, &arr), 0);
    }

    #[test]
    fn binary_delete_matches_the_indexed_value() {
        let mut builder = TantivySchema::builder();
        let field = builder.add_bytes_field("pk", INDEXED | STORED);
        // Odd-length bytes: the hex encoding this used to round-trip through is exactly the
        // shape that fails to decode as base64.
        let arr: ArrayRef = Arc::new(arrow::array::BinaryArray::from(vec![
            [0x00_u8, 0x01, 0x02].as_slice(),
        ]));

        assert_eq!(index_then_delete_count(builder.build(), field, &arr), 0);
    }

    #[test]
    fn utf8_view_is_supported_and_round_trips() {
        let mut builder = TantivySchema::builder();
        let field = builder.add_text_field("pk", STRING | STORED);
        let arr: ArrayRef = Arc::new(StringViewArray::from(vec!["hello view"]));

        assert_eq!(index_then_delete_count(builder.build(), field, &arr), 0);
    }

    #[test]
    fn binary_view_is_supported_and_round_trips() {
        let mut builder = TantivySchema::builder();
        let field = builder.add_bytes_field("pk", INDEXED | STORED);
        let arr: ArrayRef = Arc::new(BinaryViewArray::from(vec![[0xAA_u8, 0xBB].as_slice()]));

        assert_eq!(index_then_delete_count(builder.build(), field, &arr), 0);
    }
}
