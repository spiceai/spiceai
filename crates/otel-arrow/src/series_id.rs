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

//! Scalar identity of a metric data point's attribute set.
//!
//! `runtime.metrics` stores attributes as a list. Two series exported at the
//! same time under the same metric name differ only in that list, so a key of
//! `(time_unix_nano, name)` gives them one Drasi element id and the later
//! series overwrites the earlier one. [`series_ids`] hashes the list into one
//! `Utf8` value the forwarder can key. The hash is derived, so dropping the
//! column on the way back to the `OTel` protobuf loses nothing: the attributes
//! it was hashed from are still on the row.

use arrow::array::{
    Array, BinaryArray, BooleanArray, Float64Array, Int64Array, ListArray, StringArray,
    StringBuilder, StructArray, UInt8Array,
};
use arrow::datatypes::ArrowNativeType;
use arrow::error::ArrowError;

use crate::schema::AttributeValueType;

/// One `series_id` per row of a data-point `attributes` list.
///
/// A null or empty list hashes as the empty set, so two points with no
/// attributes share an identity. Attribute order does not change the result.
///
/// # Errors
///
/// Returns an error when `attributes` is not the metrics schema's list of
/// attribute structs.
pub fn series_ids(attributes: &ListArray) -> Result<StringArray, ArrowError> {
    let values = attributes
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| {
            ArrowError::InvalidArgumentError(format!(
                "metrics attributes values are {:?}, expected the attribute struct",
                attributes.values().data_type()
            ))
        })?;

    let keys = utf8_column(values, "key")?;
    let types = uint8_column(values, "type")?;
    let strings = utf8_column(values, "str")?;
    let ints = int64_column(values, "int")?;
    let doubles = float64_column(values, "double")?;
    let bools = bool_column(values, "bool")?;
    let bytes = binary_column(values, "bytes")?;
    let serialized = binary_column(values, "ser")?;

    let offsets = attributes.offsets();
    let mut builder = StringBuilder::with_capacity(attributes.len(), attributes.len() * 64);
    for row in 0..attributes.len() {
        if attributes.is_null(row) {
            builder.append_value(identity(&[]));
            continue;
        }

        let start = offsets[row].as_usize();
        let end = offsets[row + 1].as_usize();
        let mut parts = Vec::with_capacity(end.saturating_sub(start));
        for index in start..end {
            if values.is_null(index) || keys.is_null(index) {
                continue;
            }
            let kind = types.value(index);
            let payload = payload_of(
                kind, index, strings, ints, doubles, bools, bytes, serialized,
            );
            parts.push((keys.value(index).to_string(), kind, payload));
        }
        parts.sort_by(|left, right| {
            (&left.0, left.1, left.2.as_slice()).cmp(&(&right.0, right.1, right.2.as_slice()))
        });
        builder.append_value(identity(&parts));
    }

    Ok(builder.finish())
}

fn identity(parts: &[(String, u8, Vec<u8>)]) -> String {
    // Length-prefixed framing: delimiter bytes can appear inside binary/map
    // payloads, so a raw `0xff`/`0xfe` separator is not injective. Prefix each
    // field with its length (and the attribute count) so distinct attribute
    // sets cannot collide.
    let mut hasher = blake3::Hasher::new();
    hasher.update(&u64::try_from(parts.len()).unwrap_or(u64::MAX).to_le_bytes());
    for (key, kind, payload) in parts {
        let key_bytes = key.as_bytes();
        hasher.update(
            &u64::try_from(key_bytes.len())
                .unwrap_or(u64::MAX)
                .to_le_bytes(),
        );
        hasher.update(key_bytes);
        hasher.update(&[*kind]);
        hasher.update(
            &u64::try_from(payload.len())
                .unwrap_or(u64::MAX)
                .to_le_bytes(),
        );
        hasher.update(payload);
    }
    hasher.finalize().to_hex().to_string()
}

#[expect(clippy::too_many_arguments)]
fn payload_of(
    kind: u8,
    index: usize,
    strings: &StringArray,
    ints: &Int64Array,
    doubles: &Float64Array,
    bools: &BooleanArray,
    bytes: &BinaryArray,
    serialized: &BinaryArray,
) -> Vec<u8> {
    match AttributeValueType::from_u8(kind) {
        Some(AttributeValueType::Str) if !strings.is_null(index) => {
            strings.value(index).as_bytes().to_vec()
        }
        Some(AttributeValueType::Int) if !ints.is_null(index) => {
            ints.value(index).to_le_bytes().to_vec()
        }
        Some(AttributeValueType::Double) if !doubles.is_null(index) => {
            doubles.value(index).to_bits().to_le_bytes().to_vec()
        }
        Some(AttributeValueType::Bool) if !bools.is_null(index) => {
            vec![u8::from(bools.value(index))]
        }
        Some(AttributeValueType::Bytes | AttributeValueType::Slice) if !bytes.is_null(index) => {
            bytes.value(index).to_vec()
        }
        Some(AttributeValueType::Map) if !serialized.is_null(index) => {
            serialized.value(index).to_vec()
        }
        _ => Vec::new(),
    }
}

fn utf8_column<'a>(values: &'a StructArray, name: &str) -> Result<&'a StringArray, ArrowError> {
    downcast_column(values, name)
}

fn uint8_column<'a>(values: &'a StructArray, name: &str) -> Result<&'a UInt8Array, ArrowError> {
    downcast_column(values, name)
}

fn int64_column<'a>(values: &'a StructArray, name: &str) -> Result<&'a Int64Array, ArrowError> {
    downcast_column(values, name)
}

fn float64_column<'a>(values: &'a StructArray, name: &str) -> Result<&'a Float64Array, ArrowError> {
    downcast_column(values, name)
}

fn bool_column<'a>(values: &'a StructArray, name: &str) -> Result<&'a BooleanArray, ArrowError> {
    downcast_column(values, name)
}

fn binary_column<'a>(values: &'a StructArray, name: &str) -> Result<&'a BinaryArray, ArrowError> {
    downcast_column(values, name)
}

fn downcast_column<'a, T: Array + 'static>(
    values: &'a StructArray,
    name: &str,
) -> Result<&'a T, ArrowError> {
    let column = values.column_by_name(name).ok_or_else(|| {
        ArrowError::InvalidArgumentError(format!("metrics attribute struct has no '{name}' column"))
    })?;
    column.as_any().downcast_ref::<T>().ok_or_else(|| {
        ArrowError::InvalidArgumentError(format!(
            "metrics attribute column '{name}' is {:?}, expected {}",
            column.data_type(),
            std::any::type_name::<T>()
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        ArrayBuilder, ArrayRef, BinaryBuilder, BooleanBuilder, Float64Builder, Int64Builder,
        StringBuilder, UInt8Builder,
    };
    use arrow::buffer::{NullBuffer, OffsetBuffer};
    use arrow::datatypes::Field;
    use std::sync::Arc;

    use crate::schema::{attribute_list_field, attribute_struct_fields};

    fn attributes(rows: &[Option<Vec<(&str, &str)>>]) -> ListArray {
        let mut keys = StringBuilder::new();
        let mut types = UInt8Builder::new();
        let mut strings = StringBuilder::new();
        let mut ints = Int64Builder::new();
        let mut doubles = Float64Builder::new();
        let mut bools = BooleanBuilder::new();
        let mut bytes = BinaryBuilder::new();
        let mut serialized = BinaryBuilder::new();
        let mut offsets = Vec::with_capacity(rows.len() + 1);
        offsets.push(0_i32);
        let mut list_nulls = Vec::with_capacity(rows.len());

        for row in rows {
            match row {
                None => list_nulls.push(false),
                Some(pairs) => {
                    list_nulls.push(true);
                    for (key, value) in pairs {
                        keys.append_value(*key);
                        types.append_value(AttributeValueType::Str.to_u8());
                        strings.append_value(*value);
                        ints.append_null();
                        doubles.append_null();
                        bools.append_null();
                        bytes.append_null();
                        serialized.append_null();
                    }
                }
            }
            offsets.push(i32::try_from(keys.len()).expect("attribute count fits"));
        }

        let arrays: Vec<ArrayRef> = vec![
            Arc::new(keys.finish()),
            Arc::new(types.finish()),
            Arc::new(strings.finish()),
            Arc::new(ints.finish()),
            Arc::new(doubles.finish()),
            Arc::new(bools.finish()),
            Arc::new(bytes.finish()),
            Arc::new(serialized.finish()),
        ];
        let values = StructArray::try_new(attribute_struct_fields().into(), arrays, None)
            .expect("attribute struct");
        let field: Field = attribute_list_field();
        let nulls = NullBuffer::from(list_nulls);
        ListArray::try_new(
            Arc::new(field),
            OffsetBuffer::new(offsets.into()),
            Arc::new(values),
            Some(nulls),
        )
        .expect("attribute list")
    }

    fn ids(rows: &[Option<Vec<(&str, &str)>>]) -> Vec<String> {
        let list = attributes(rows);
        let ids = series_ids(&list).expect("derives ids");
        (0..ids.len())
            .map(|row| ids.value(row).to_string())
            .collect()
    }

    #[test]
    fn different_attributes_get_different_identities() {
        let ids = ids(&[Some(vec![("status", "200")]), Some(vec![("status", "500")])]);
        assert_ne!(ids[0], ids[1]);
    }

    #[test]
    fn attribute_order_does_not_change_the_identity() {
        let ids = ids(&[
            Some(vec![("region", "us"), ("status", "200")]),
            Some(vec![("status", "200"), ("region", "us")]),
        ]);
        assert_eq!(ids[0], ids[1]);
    }

    #[test]
    fn a_null_list_matches_an_empty_list() {
        let ids = ids(&[None, Some(vec![])]);
        assert_eq!(ids[0], ids[1]);
        assert!(!ids[0].is_empty());
    }

    /// Binary attribute lists whose payloads embed former delimiter bytes.
    fn binary_attributes(rows: &[Vec<(&str, &[u8])>]) -> ListArray {
        let mut keys = StringBuilder::new();
        let mut types = UInt8Builder::new();
        let mut strings = StringBuilder::new();
        let mut ints = Int64Builder::new();
        let mut doubles = Float64Builder::new();
        let mut bools = BooleanBuilder::new();
        let mut bytes = BinaryBuilder::new();
        let mut serialized = BinaryBuilder::new();
        let mut offsets = Vec::with_capacity(rows.len() + 1);
        offsets.push(0_i32);
        let mut list_nulls = Vec::with_capacity(rows.len());

        for row in rows {
            list_nulls.push(true);
            for (key, value) in row {
                keys.append_value(*key);
                types.append_value(AttributeValueType::Bytes.to_u8());
                strings.append_null();
                ints.append_null();
                doubles.append_null();
                bools.append_null();
                bytes.append_value(*value);
                serialized.append_null();
            }
            offsets.push(i32::try_from(keys.len()).expect("attribute count fits"));
        }

        let arrays: Vec<ArrayRef> = vec![
            Arc::new(keys.finish()),
            Arc::new(types.finish()),
            Arc::new(strings.finish()),
            Arc::new(ints.finish()),
            Arc::new(doubles.finish()),
            Arc::new(bools.finish()),
            Arc::new(bytes.finish()),
            Arc::new(serialized.finish()),
        ];
        let values = StructArray::try_new(attribute_struct_fields().into(), arrays, None)
            .expect("attribute struct");
        let field: Field = attribute_list_field();
        let nulls = NullBuffer::from(list_nulls);
        ListArray::try_new(
            Arc::new(field),
            OffsetBuffer::new(offsets.into()),
            Arc::new(values),
            Some(nulls),
        )
        .expect("attribute list")
    }

    #[test]
    fn binary_payloads_cannot_forge_attribute_boundaries() {
        // Old delimiter framing hashed these to the same BLAKE3 digest:
        // one attr `a = x\xfeb\xff\x07\xffy` vs two attrs `a = x`, `b = y`.
        let colliding_payload = b"x\xfeb\xff\x07\xffy";
        let list = binary_attributes(&[
            vec![("a", colliding_payload)],
            vec![("a", b"x".as_slice()), ("b", b"y".as_slice())],
        ]);
        let ids = series_ids(&list).expect("derives ids");
        assert_ne!(
            ids.value(0),
            ids.value(1),
            "length-prefixed framing must keep these attribute sets distinct"
        );
    }
}
