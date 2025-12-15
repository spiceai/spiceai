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
use serde_json::{Map, Value};
use snafu::ResultExt;
use std::sync::Arc;

use arrow::{
    array::{
        Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Date64Array, Float16Array,
        Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
        LargeBinaryArray, LargeStringArray, RecordBatch, StringArray, UInt8Array, UInt16Array,
        UInt32Array, UInt64Array,
    },
    datatypes::DataType,
    error::ArrowError,
};
use arrow_schema::{Field as ArrowField, Schema, SchemaRef};
use tantivy::{Term, schema::Field};

use serde_json::to_string;

/// Adds an additional [`StringArray`] column to a [`RecordBatch`] as a JSON-string representation
/// from a subset of the columns present.
pub fn with_json_subset_column(
    batch: &RecordBatch,
    subset_columns: &[String],
    new_column_name: &str,
) -> Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>> {
    let mut subset_fields = Vec::with_capacity(subset_columns.len());
    let mut subset_arrays = Vec::with_capacity(subset_columns.len());
    for col_name in subset_columns {
        let idx = batch.schema().index_of(col_name.as_str()).boxed()?;
        subset_fields.push(batch.schema().field(idx).clone());
        subset_arrays.push(Arc::clone(batch.column(idx)));
    }

    let subset_schema: SchemaRef = Arc::new(Schema::new(subset_fields));
    let subset_batch = RecordBatch::try_new(Arc::clone(&subset_schema), subset_arrays).boxed()?;

    let buf = Vec::new();
    let mut writer = arrow_json::ArrayWriter::new(buf);
    writer.write_batches(&[&subset_batch]).boxed()?;
    writer.finish().boxed()?;
    let json_data = writer.into_inner();

    let json_strings: Vec<String> =
        serde_json::from_reader::<_, Vec<Map<String, Value>>>(json_data.as_slice())
            .boxed()?
            .into_iter()
            .map(|v| to_string(&v).boxed())
            .collect::<Result<Vec<String>, _>>()?;

    let json_array: ArrayRef = Arc::new(StringArray::from(json_strings));

    let mut new_fields: Vec<_> = batch.schema().fields().iter().cloned().collect();
    new_fields.push(Arc::new(ArrowField::new(
        new_column_name,
        DataType::Utf8,
        false,
    )));
    let new_schema: SchemaRef = Arc::new(Schema::new(new_fields));

    let mut new_columns: Vec<ArrayRef> = batch.columns().to_vec();
    new_columns.push(json_array);

    RecordBatch::try_new(new_schema, new_columns).boxed()
}

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
