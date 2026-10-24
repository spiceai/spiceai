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

use std::collections::HashMap;
use std::sync::Arc;

use crate::error::{Result, UnsupportedPayloadTypeSnafu};
use arrow::array::{Array, ArrayRef};
use arrow_schema::{DataType, Field};
use qdrant_client::qdrant::value::Kind;
use qdrant_client::qdrant::{ListValue, PointId, PointStruct, Value};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct PointData {
    pub id: Option<PointId>,
    pub payload: HashMap<String, Value>,
    pub vector: Vec<f32>,
}

impl From<PointData> for PointStruct {
    fn from(point: PointData) -> Self {
        PointStruct {
            id: point.id,
            payload: point.payload,
            vectors: Some(point.vector.into()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SearchResult {
    pub id: PointId,
    pub score: f32,
    pub payload: HashMap<String, Value>,
    pub vector: Option<Vec<f32>>,
}

impl From<qdrant_client::qdrant::ScoredPoint> for SearchResult {
    fn from(point: qdrant_client::qdrant::ScoredPoint) -> Self {
        let vector = point
            .vectors
            .as_ref()
            .and_then(qdrant_client::qdrant::VectorsOutput::get_vector)
            .and_then(|v| match v {
                qdrant_client::qdrant::vector_output::Vector::Dense(dense) => Some(dense.data),
                _ => None,
            });
        Self {
            id: point.id.unwrap_or_default(),
            score: point.score,
            payload: point.payload,
            vector,
        }
    }
}

/// Payload key naming the dataset that wrote a point. Spice reads, cleans up, and deletes only
/// the points of its own dataset, so it never touches points another writer stored.
pub const DATASET_KEY: &str = "spice_dataset";

/// Payload key holding the write generation of a point. A full refresh starts a new generation
/// and then deletes the points of its dataset that carry an older one.
pub const GENERATION_KEY: &str = "spice_generation";

/// Payload key holding the exact vector of a point in a cosine collection, because Qdrant
/// normalizes cosine vectors on upload.
pub const VECTOR_KEY: &str = "spice_vector";

/// Payload keys Spice writes on every point, which a dataset column cannot use.
pub const RESERVED_KEYS: [&str; 3] = [DATASET_KEY, GENERATION_KEY, VECTOR_KEY];

/// Encodes `vector` as base64 of its little-endian `f32` bytes, which keeps every bit.
#[must_use]
pub fn encode_vector(vector: &[f32]) -> Value {
    use base64::Engine as _;
    let bytes: Vec<u8> = vector.iter().flat_map(|x| x.to_le_bytes()).collect();
    Value::from(base64::engine::general_purpose::STANDARD.encode(bytes))
}

/// Decodes a vector written by [`encode_vector`].
///
/// # Errors
///
/// Returns an error when `value` is not a base64 string of whole `f32` values.
pub fn decode_vector(value: &Value) -> Result<Vec<f32>> {
    use base64::Engine as _;
    let malformed = |detail: String| crate::error::Error::VectorDecode { detail };
    let Some(Kind::StringValue(text)) = &value.kind else {
        return Err(malformed(format!(
            "expected a string, found {}",
            kind_name(value)
        )));
    };
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(text)
        .map_err(|e| malformed(e.to_string()))?;
    let (floats, rest) = bytes.as_chunks::<4>();
    if !rest.is_empty() {
        return Err(malformed(format!(
            "{} bytes is not a whole number of 32-bit floats",
            bytes.len()
        )));
    }
    Ok(floats.iter().map(|b| f32::from_le_bytes(*b)).collect())
}

/// Types stored as their exact string form, since Qdrant has no native value for them.
#[must_use]
pub fn is_string_encoded(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Decimal32(..)
            | DataType::Decimal64(..)
            | DataType::Decimal128(..)
            | DataType::Decimal256(..)
            | DataType::Date32
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Timestamp(..)
    )
}

#[must_use]
pub fn is_storable(data_type: &DataType) -> bool {
    match data_type {
        DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float32
        | DataType::Float64
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View => true,
        DataType::FixedSizeList(field, _) => {
            !matches!(field.data_type(), DataType::FixedSizeList(..))
                && !is_string_encoded(field.data_type())
                && is_storable(field.data_type())
        }
        other => is_string_encoded(other),
    }
}

/// Casts a string-encoded column to `Utf8` for [`arrow_value_to_qdrant`].
///
/// # Errors
///
/// Returns an error when the cast fails.
pub fn payload_column(array: &ArrayRef) -> Result<ArrayRef> {
    if !is_string_encoded(array.data_type()) {
        return Ok(Arc::clone(array));
    }
    arrow::compute::cast(array, &DataType::Utf8).map_err(|_| unsupported(array.as_ref()))
}

/// Builds a column of `field`'s type from payload values. A NULL or missing value decodes as
/// NULL. Any other value must match the type exactly.
///
/// # Errors
///
/// Returns an error when a value has the wrong kind, does not fit the type, or does not parse.
pub fn payload_values_to_arrow(values: &[&Value], field: &Field) -> Result<ArrayRef> {
    decode(values, field.data_type()).map_err(|detail| crate::error::Error::PayloadDecode {
        column: field.name().clone(),
        arrow_type: field.data_type().to_string(),
        detail,
    })
}

fn present(value: &Value) -> Option<&Kind> {
    match &value.kind {
        None | Some(Kind::NullValue(_)) => None,
        Some(kind) => Some(kind),
    }
}

fn kind_name(value: &Value) -> &'static str {
    match &value.kind {
        None | Some(Kind::NullValue(_)) => "null",
        Some(Kind::BoolValue(_)) => "a boolean",
        Some(Kind::IntegerValue(_)) => "an integer",
        Some(Kind::DoubleValue(_)) => "a float",
        Some(Kind::StringValue(_)) => "a string",
        Some(Kind::ListValue(_)) => "a list",
        Some(Kind::StructValue(_)) => "an object",
    }
}

fn mismatch(value: &Value, data_type: &DataType) -> String {
    format!("found {} where {data_type} was expected", kind_name(value))
}

fn decode_integer<T: TryFrom<i64>>(
    value: &Value,
    data_type: &DataType,
) -> std::result::Result<Option<T>, String> {
    match present(value) {
        None => Ok(None),
        Some(Kind::IntegerValue(v)) => T::try_from(*v)
            .map(Some)
            .map_err(|_| format!("{v} does not fit in {data_type}")),
        Some(_) => Err(mismatch(value, data_type)),
    }
}

#[expect(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    reason = "each conversion is checked to round trip before it is accepted"
)]
fn decode_float(value: &Value, data_type: &DataType) -> std::result::Result<Option<f64>, String> {
    // 2^63 is the one `f64` an `i64` rounds to that saturates back to `i64::MAX`.
    const TWO_POW_63: f64 = 9_223_372_036_854_775_808.0;
    let exact_in_f32 = |f: f64| f.is_nan() || f64::from(f as f32).to_bits() == f.to_bits();
    let decoded = match present(value) {
        None => return Ok(None),
        Some(Kind::DoubleValue(f)) => *f,
        Some(Kind::IntegerValue(v)) if (*v as f64) < TWO_POW_63 && (*v as f64) as i64 == *v => {
            *v as f64
        }
        Some(Kind::IntegerValue(v)) => return Err(format!("{v} is not exact in {data_type}")),
        Some(_) => return Err(mismatch(value, data_type)),
    };
    if *data_type == DataType::Float32 && !exact_in_f32(decoded) {
        return Err(format!("{decoded} is not exact in {data_type}"));
    }
    Ok(Some(decoded))
}

fn decode_string<'a>(
    value: &'a Value,
    data_type: &DataType,
) -> std::result::Result<Option<&'a str>, String> {
    match present(value) {
        None => Ok(None),
        Some(Kind::StringValue(v)) => Ok(Some(v.as_str())),
        Some(_) => Err(mismatch(value, data_type)),
    }
}

#[expect(
    clippy::cast_possible_truncation,
    reason = "decode_float accepts a Float32 value only when it is exact in f32"
)]
fn decode(values: &[&Value], data_type: &DataType) -> std::result::Result<ArrayRef, String> {
    use arrow::array::{
        BooleanArray, FixedSizeListArray, Float32Array, Float64Array, Int8Array, Int16Array,
        Int32Array, Int64Array, LargeStringArray, StringArray, StringViewArray, UInt8Array,
        UInt16Array, UInt32Array, UInt64Array,
    };
    macro_rules! integers {
        ($array:ty) => {
            Arc::new(
                values
                    .iter()
                    .map(|v| decode_integer(v, data_type))
                    .collect::<std::result::Result<$array, String>>()?,
            ) as ArrayRef
        };
    }
    macro_rules! strings {
        ($array:ty) => {
            Arc::new(
                values
                    .iter()
                    .map(|v| decode_string(v, data_type))
                    .collect::<std::result::Result<$array, String>>()?,
            ) as ArrayRef
        };
    }

    if is_string_encoded(data_type) {
        let strings = values
            .iter()
            .map(|v| decode_string(v, data_type))
            .collect::<std::result::Result<StringArray, String>>()?;
        let options = arrow::compute::CastOptions {
            safe: false,
            ..Default::default()
        };
        return arrow::compute::cast_with_options(&strings, data_type, &options)
            .map_err(|e| e.to_string());
    }
    Ok(match data_type {
        DataType::Boolean => Arc::new(
            values
                .iter()
                .map(|v| match present(v) {
                    None => Ok(None),
                    Some(Kind::BoolValue(b)) => Ok(Some(*b)),
                    Some(_) => Err(mismatch(v, data_type)),
                })
                .collect::<std::result::Result<BooleanArray, String>>()?,
        ),
        DataType::Int8 => integers!(Int8Array),
        DataType::Int16 => integers!(Int16Array),
        DataType::Int32 => integers!(Int32Array),
        DataType::Int64 => integers!(Int64Array),
        DataType::UInt8 => integers!(UInt8Array),
        DataType::UInt16 => integers!(UInt16Array),
        DataType::UInt32 => integers!(UInt32Array),
        DataType::UInt64 => integers!(UInt64Array),
        DataType::Float32 => Arc::new(
            values
                .iter()
                .map(|v| decode_float(v, data_type).map(|f| f.map(|f| f as f32)))
                .collect::<std::result::Result<Float32Array, String>>()?,
        ),
        DataType::Float64 => Arc::new(
            values
                .iter()
                .map(|v| decode_float(v, data_type))
                .collect::<std::result::Result<Float64Array, String>>()?,
        ),
        DataType::Utf8 => strings!(StringArray),
        DataType::LargeUtf8 => strings!(LargeStringArray),
        DataType::Utf8View => strings!(StringViewArray),
        DataType::FixedSizeList(child, size) => {
            let len = usize::try_from(*size).map_err(|e| e.to_string())?;
            let null = null_value();
            let mut items: Vec<&Value> = Vec::with_capacity(values.len() * len);
            let mut valid: Vec<bool> = Vec::with_capacity(values.len());
            for value in values {
                match present(value) {
                    None => {
                        items.extend(std::iter::repeat_n(&null, len));
                        valid.push(false);
                    }
                    Some(Kind::ListValue(list)) if list.values.len() == len => {
                        items.extend(list.values.iter());
                        valid.push(true);
                    }
                    Some(Kind::ListValue(list)) => {
                        return Err(format!(
                            "found a list of {} values where {data_type} was expected",
                            list.values.len()
                        ));
                    }
                    Some(_) => return Err(mismatch(value, data_type)),
                }
            }
            let child_values = decode(&items, child.data_type())?;
            Arc::new(
                FixedSizeListArray::try_new(
                    Arc::clone(child),
                    *size,
                    child_values,
                    Some(arrow::buffer::NullBuffer::from(valid)),
                )
                .map_err(|e| e.to_string())?,
            )
        }
        other => return Err(format!("{other} cannot be read from a Qdrant payload")),
    })
}

fn unsupported(array: &dyn Array) -> crate::error::Error {
    UnsupportedPayloadTypeSnafu {
        arrow_type: format!("{:?}", array.data_type()),
    }
    .build()
}

/// Converts one Arrow cell into a Qdrant payload value.
///
/// # Errors
///
/// Returns an error for Arrow types Qdrant cannot store (and for unsigned
/// integers that exceed Qdrant's 64-bit integer range) rather than silently
/// storing NULL, which would corrupt the index.
pub fn arrow_value_to_qdrant(array: &dyn Array, row: usize) -> Result<Value> {
    use arrow::array::{
        BooleanArray, FixedSizeListArray, Float32Array, Float64Array, Int8Array, Int16Array,
        Int32Array, Int64Array, LargeStringArray, StringArray, StringViewArray, UInt8Array,
        UInt16Array, UInt32Array, UInt64Array,
    };
    macro_rules! payload {
        ($array:expr, $ty:ty, $row:expr, $conv:expr) => {
            $array
                .as_any()
                .downcast_ref::<$ty>()
                .map($conv)
                .ok_or_else(|| unsupported($array))
        };
    }
    if array.is_null(row) {
        return Ok(null_value());
    }
    match array.data_type() {
        DataType::Boolean => payload!(array, BooleanArray, row, |a| Value::from(a.value(row))),
        DataType::Int8 => payload!(array, Int8Array, row, |a| Value::from(i64::from(
            a.value(row)
        ))),
        DataType::Int16 => payload!(array, Int16Array, row, |a| Value::from(i64::from(
            a.value(row)
        ))),
        DataType::Int32 => payload!(array, Int32Array, row, |a| Value::from(i64::from(
            a.value(row)
        ))),
        DataType::Int64 => payload!(array, Int64Array, row, |a| Value::from(a.value(row))),
        DataType::UInt8 => payload!(array, UInt8Array, row, |a| Value::from(i64::from(
            a.value(row)
        ))),
        DataType::UInt16 => payload!(array, UInt16Array, row, |a| Value::from(i64::from(
            a.value(row)
        ))),
        DataType::UInt32 => payload!(array, UInt32Array, row, |a| Value::from(i64::from(
            a.value(row)
        ))),
        DataType::UInt64 => {
            let Some(uints) = array.as_any().downcast_ref::<UInt64Array>() else {
                return Err(unsupported(array));
            };
            i64::try_from(uints.value(row))
                .map(Value::from)
                .map_err(|_| unsupported(array))
        }
        DataType::FixedSizeList(_, size) => {
            let Some(list) = array.as_any().downcast_ref::<FixedSizeListArray>() else {
                return Err(unsupported(array));
            };
            let size = usize::try_from(*size).unwrap_or(0);
            let slot = usize::try_from(list.value_offset(row)).unwrap_or(0);
            let values = (0..size)
                .map(|i| arrow_value_to_qdrant(list.values().as_ref(), slot + i))
                .collect::<Result<Vec<_>>>()?;
            Ok(Value {
                kind: Some(Kind::ListValue(ListValue { values })),
            })
        }
        DataType::Float32 => payload!(array, Float32Array, row, |a| Value::from(a.value(row))),
        DataType::Float64 => payload!(array, Float64Array, row, |a| Value::from(a.value(row))),
        DataType::Utf8 => payload!(array, StringArray, row, |a| Value::from(
            a.value(row).to_string()
        )),
        DataType::LargeUtf8 => payload!(array, LargeStringArray, row, |a| Value::from(
            a.value(row).to_string()
        )),
        DataType::Utf8View => payload!(array, StringViewArray, row, |a| Value::from(
            a.value(row).to_string()
        )),
        _ => Err(unsupported(array)),
    }
}

fn null_value() -> Value {
    Value {
        kind: Some(Kind::NullValue(0)),
    }
}

#[must_use]
pub fn point_uuid(values: &[String]) -> String {
    let name = values.join("\u{1f}");
    Uuid::new_v5(&Uuid::NAMESPACE_URL, name.as_bytes()).to_string()
}

#[must_use]
pub fn point_id_from_values(values: &[String]) -> PointId {
    PointId::from(point_uuid(values))
}
#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use arrow::array::{
        FixedSizeListArray, Float32Array, Int32Array, Int64Array, RecordBatch, StringArray,
        UInt8Array, UInt64Array,
    };
    use arrow_schema::Field;

    use super::*;

    #[test]
    fn point_id_is_deterministic_uuid_v5() {
        let id1 = point_id_from_values(&["id-1".to_string()]);
        let id2 = point_id_from_values(&["id-1".to_string()]);
        let id3 = point_id_from_values(&["id-2".to_string()]);
        let composite = point_id_from_values(&["a".to_string(), "b".to_string()]);
        let composite_swapped = point_id_from_values(&["b".to_string(), "a".to_string()]);

        assert_eq!(id1, id2);
        assert_ne!(id1, id3);
        assert_ne!(composite, composite_swapped);

        let Some(qdrant_client::qdrant::point_id::PointIdOptions::Uuid(uuid)) =
            id1.point_id_options
        else {
            panic!("expected a UUID point id");
        };
        assert_eq!(uuid, "6210c986-d7df-51f8-8e73-b4fc8e99b456");
        assert_eq!(
            uuid.parse::<uuid::Uuid>().expect("parse").get_version_num(),
            5
        );
    }

    #[test]
    fn arrow_value_to_qdrant_rejects_unsupported_and_overflow() {
        let u64s = UInt64Array::from(vec![u64::MAX]);
        let err = arrow_value_to_qdrant(&u64s, 0).expect_err("out-of-range integer must error");
        assert!(err.to_string().contains("Unsupported Arrow type"));

        let dates = arrow::array::Date32Array::from(vec![0]);
        let err = arrow_value_to_qdrant(&dates, 0).expect_err("unsupported type must error");
        assert!(err.to_string().contains("Date32"));
    }

    #[test]
    fn arrow_value_to_qdrant_converts_values_nulls_and_slices() {
        let strings = StringArray::from(vec![Some("x"), None]);
        assert!(matches!(
            arrow_value_to_qdrant(&strings, 1).expect("null cell").kind,
            Some(Kind::NullValue(0))
        ));
        assert_eq!(
            arrow_value_to_qdrant(&strings, 0)
                .expect("string cell")
                .kind,
            Some(Kind::StringValue("x".to_string()))
        );

        let values = Int32Array::from(vec![1, 2, 3, 4]);
        let fsl = FixedSizeListArray::new(
            Arc::new(Field::new("item", arrow_schema::DataType::Int32, false)),
            2,
            Arc::new(values),
            None,
        );
        let sliced = fsl.slice(1, 1);
        let Some(Kind::ListValue(list)) = arrow_value_to_qdrant(&sliced, 0)
            .expect("list payload")
            .kind
        else {
            panic!("expected a list payload value");
        };
        let nums: Vec<i64> = list
            .values
            .iter()
            .map(|v| match &v.kind {
                Some(Kind::IntegerValue(n)) => *n,
                other => panic!("expected integer payload value, got {other:?}"),
            })
            .collect();
        assert_eq!(nums, vec![3, 4]);
    }

    #[test]
    fn string_encoded_types_round_trip_exactly() {
        use arrow::array::{
            Date32Array, Decimal128Array, Time64MicrosecondArray, TimestampMicrosecondArray,
        };
        let columns: Vec<(ArrayRef, &str)> = vec![
            (
                Arc::new(
                    Decimal128Array::from(vec![Some(1250), None])
                        .with_precision_and_scale(12, 2)
                        .expect("decimal"),
                ),
                "12.50",
            ),
            (
                Arc::new(Date32Array::from(vec![Some(19_726), None])),
                "2024-01-04",
            ),
            (
                Arc::new(TimestampMicrosecondArray::from(vec![
                    Some(1_704_112_215_123_456),
                    None,
                ])),
                "2024-01-01T12:30:15.123456",
            ),
            (
                Arc::new(
                    TimestampMicrosecondArray::from(vec![Some(1_704_112_215_123_456), None])
                        .with_timezone("+05:30"),
                ),
                "2024-01-01T18:00:15.123456+05:30",
            ),
            (
                Arc::new(Time64MicrosecondArray::from(vec![
                    Some(45_015_123_456),
                    None,
                ])),
                "12:30:15.123456",
            ),
        ];
        for (column, text) in columns {
            let encoded = payload_column(&column).expect("encode");
            let values: Vec<Value> = (0..2)
                .map(|row| arrow_value_to_qdrant(encoded.as_ref(), row).expect("value"))
                .collect();
            assert_eq!(values[0].kind, Some(Kind::StringValue(text.to_string())));
            assert_eq!(values[1].kind, Some(Kind::NullValue(0)));
            let refs: Vec<&Value> = values.iter().collect();
            let field = Field::new("c", column.data_type().clone(), true);
            let decoded = payload_values_to_arrow(&refs, &field).expect("decode");
            assert_eq!(decoded.as_ref(), column.as_ref());
        }
    }

    #[test]
    fn a_value_of_the_wrong_kind_is_an_error_never_null() {
        let field = |dt| Field::new("c", dt, true);
        let cases: Vec<(Value, DataType, &str)> = vec![
            (
                Value::from("soon"),
                DataType::Date32,
                "Failed to read column 'c' from Qdrant: a stored payload value does not match the column type Date32 (Cast error: Cannot cast string 'soon' to value of Date32 type). Recreate the Qdrant collection, or set a new `qdrant_collection`, so Spice writes every point with the dataset's current schema. See: https://spiceai.org/docs/components/vectors",
            ),
            (
                Value::from(19_726i64),
                DataType::Date32,
                "found an integer where Date32 was expected",
            ),
            (
                Value::from(i64::MAX),
                DataType::Int8,
                "9223372036854775807 does not fit in Int8",
            ),
            (
                Value::from(-1i64),
                DataType::UInt64,
                "-1 does not fit in UInt64",
            ),
            (
                Value::from("7"),
                DataType::Int64,
                "found a string where Int64 was expected",
            ),
            (
                Value::from(true),
                DataType::Utf8,
                "found a boolean where Utf8 was expected",
            ),
            (
                Value::from(0.1f64),
                DataType::Float32,
                "0.1 is not exact in Float32",
            ),
            (
                Value::from(9_007_199_254_740_993i64),
                DataType::Float64,
                "9007199254740993 is not exact in Float64",
            ),
            (
                Value::from(i64::MAX),
                DataType::Float64,
                "9223372036854775807 is not exact in Float64",
            ),
            (
                Value::from(vec![Value::from(1.0f64)]),
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 2),
                "found a list of 1 values where FixedSizeList(2 x Float32) was expected",
            ),
        ];
        for (value, data_type, expected) in cases {
            let err = payload_values_to_arrow(&[&value], &field(data_type.clone()))
                .expect_err("a mismatched value must not decode");
            assert!(err.to_string().contains(expected), "{data_type}: {err}");
        }
    }

    #[test]
    fn null_and_missing_values_decode_as_null() {
        let null = null_value();
        let missing = Value { kind: None };
        for data_type in [
            DataType::Int8,
            DataType::Utf8,
            DataType::Float32,
            DataType::Date32,
            DataType::Decimal128(12, 2),
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, false)), 2),
        ] {
            let decoded = payload_values_to_arrow(
                &[&null, &missing],
                &Field::new("c", data_type.clone(), true),
            )
            .expect("decode nulls");
            assert_eq!(decoded.data_type(), &data_type);
            assert_eq!(decoded.null_count(), 2, "{data_type}");
        }
    }

    #[test]
    fn vectors_round_trip_bit_for_bit() {
        let vector = vec![
            0.1f32,
            -0.0,
            f32::MIN_POSITIVE,
            1.0e-45,
            f32::MAX,
            0.333_333_34,
        ];
        let decoded = decode_vector(&encode_vector(&vector)).expect("decode");
        assert_eq!(
            decoded.iter().map(|x| x.to_bits()).collect::<Vec<_>>(),
            vector.iter().map(|x| x.to_bits()).collect::<Vec<_>>()
        );
        let err = decode_vector(&Value::from("AAA=")).expect_err("two bytes are not a float");
        assert!(
            err.to_string().contains("2 bytes is not a whole number"),
            "{err}"
        );
        let err = decode_vector(&Value::from(1i64)).expect_err("an integer is not a vector");
        assert!(
            err.to_string()
                .contains("expected a string, found an integer"),
            "{err}"
        );
    }

    #[test]
    fn storable_types() {
        let item = |dt| Arc::new(Field::new("item", dt, true));
        assert!(is_storable(&DataType::Decimal128(38, 10)));
        assert!(is_storable(&DataType::FixedSizeList(
            item(DataType::Float32),
            4
        )));
        assert!(!is_storable(&DataType::FixedSizeList(
            item(DataType::Date32),
            4
        )));
        assert!(!is_storable(&DataType::Binary));
        assert!(!is_storable(&DataType::List(item(DataType::Utf8))));
    }

    #[test]
    fn payload_round_trips_and_narrows_when_the_value_fits() {
        let ints = Int64Array::from(vec![Some(42i64), None]);
        let strings = StringArray::from(vec![Some("hello"), Some("world")]);
        let batch = RecordBatch::try_new(
            Arc::new(arrow_schema::Schema::new(vec![
                Field::new("id", arrow_schema::DataType::Int64, true),
                Field::new("name", arrow_schema::DataType::Utf8, true),
            ])),
            vec![Arc::new(ints), Arc::new(strings)],
        )
        .expect("build batch");

        let payload = HashMap::from([
            (
                "id".to_string(),
                arrow_value_to_qdrant(batch.column(0).as_ref(), 0).expect("int payload"),
            ),
            (
                "name".to_string(),
                arrow_value_to_qdrant(batch.column(1).as_ref(), 0).expect("string payload"),
            ),
        ]);

        let decode = |key: &str, data_type: DataType| {
            payload_values_to_arrow(
                &[payload.get(key).expect("payload key")],
                &Field::new(key, data_type, true),
            )
            .expect("decode")
        };
        assert_eq!(
            decode("id", DataType::Int64)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("int64 array")
                .value(0),
            42
        );
        assert_eq!(
            decode("name", DataType::Utf8)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("string array")
                .value(0),
            "hello"
        );
        assert_eq!(
            decode("id", DataType::UInt8)
                .as_any()
                .downcast_ref::<UInt8Array>()
                .expect("uint8 array")
                .value(0),
            42
        );
        let float = payload_values_to_arrow(
            &[&Value::from(2.5f64)],
            &Field::new("f", DataType::Float32, true),
        )
        .expect("decode float");
        assert_eq!(
            float
                .as_any()
                .downcast_ref::<Float32Array>()
                .expect("float32 array")
                .value(0)
                .to_bits(),
            2.5f32.to_bits()
        );
    }
}
