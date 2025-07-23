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

//! Create filenames from [`ScalarValue`]s and infer [`ScalarValue`]s from
//! filenames.
//!
//! `serde_qs` crate is used to serialize/deserialize [`ScalarValue`]s into
//! [`String`]s that are URL encoded. This is helpful because we get the
//! encoding and decoding functionality and special characters are escaped
//! making them filesystem compatible.
//!
//! A subset of the [`ScalarValue`] variants are copied into a new type
//! [`SupportedScalarValue`] so that we can derive `Serialize` and `Deserialize`
//! on variants that have types that can be serialized/deserialized.

use arrow_schema::DataType;
use datafusion::scalar::ScalarValue;
use serde::{Deserialize, Serialize};
use snafu::prelude::*;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to serialize: {source}"))]
    Serialize { source: serde_qs::Error },

    #[snafu(display("Failed to deserialize: {source}"))]
    Deserialize { source: serde_qs::Error },

    #[snafu(display("Unsupported scalar value type: {data_type}"))]
    UnsupportedType { data_type: DataType },
}

#[derive(Serialize, Deserialize)]
struct SerializablePair {
    scalar: SupportedScalarValue,
    exprs_hash: u64,
}

/// Encodes a [`ScalarValue`] and a hash of the `partition_by` expressions
///
/// # Errors
/// Returns an error if the [`ScalarValue`] is not supported or cannot be
/// serialized.
pub fn encode_pair(scalar: &ScalarValue, exprs_hash: u64) -> Result<String, Error> {
    let supported_scalar = SupportedScalarValue::try_from(scalar.clone())?;
    let pair = SerializablePair {
        scalar: supported_scalar,
        exprs_hash,
    };
    let encoded = serde_qs::to_string(&pair).context(SerializeSnafu)?;
    Ok(encoded)
}

/// Decodes a [`String`] back into a [`ScalarValue`] and the hash of the
/// `partition_by` expressions.
///
/// # Errors
/// Returns an error if the str cannot be deserialized or converted to a
/// [`ScalarValue`]
pub fn decode_pair(value: &str) -> Result<(ScalarValue, u64), Error> {
    let pair: SerializablePair = serde_qs::from_str(value).context(DeserializeSnafu)?;
    let scalar = ScalarValue::try_from(pair.scalar)?;
    Ok((scalar, pair.exprs_hash))
}

#[derive(Serialize, Deserialize)]
enum SupportedScalarValue {
    Boolean(Option<bool>),
    Int8(Option<i8>),
    Int16(Option<i16>),
    Int32(Option<i32>),
    Int64(Option<i64>),
    UInt8(Option<u8>),
    UInt16(Option<u16>),
    UInt32(Option<u32>),
    UInt64(Option<u64>),
    Utf8(Option<String>),
    Utf8View(Option<String>),
    LargeUtf8(Option<String>),
    Date32(Option<i32>),
    Date64(Option<i64>),
    Time32Second(Option<i32>),
    Time32Millisecond(Option<i32>),
    Time64Microsecond(Option<i64>),
    Time64Nanosecond(Option<i64>),
    TimestampSecond(Option<i64>, Option<String>),
    TimestampMillisecond(Option<i64>, Option<String>),
    TimestampMicrosecond(Option<i64>, Option<String>),
    TimestampNanosecond(Option<i64>, Option<String>),
    IntervalYearMonth(Option<i32>),
}

impl TryFrom<ScalarValue> for SupportedScalarValue {
    type Error = Error;

    fn try_from(value: ScalarValue) -> Result<Self, Self::Error> {
        Ok(match value {
            ScalarValue::Boolean(maybe_value) => Self::Boolean(maybe_value),
            ScalarValue::Int8(maybe_value) => Self::Int8(maybe_value),
            ScalarValue::Int16(maybe_value) => Self::Int16(maybe_value),
            ScalarValue::Int32(maybe_value) => Self::Int32(maybe_value),
            ScalarValue::Int64(maybe_value) => Self::Int64(maybe_value),
            ScalarValue::UInt8(maybe_value) => Self::UInt8(maybe_value),
            ScalarValue::UInt16(maybe_value) => Self::UInt16(maybe_value),
            ScalarValue::UInt32(maybe_value) => Self::UInt32(maybe_value),
            ScalarValue::UInt64(maybe_value) => Self::UInt64(maybe_value),
            ScalarValue::Utf8(maybe_value) => Self::Utf8(maybe_value),
            ScalarValue::Utf8View(maybe_value) => Self::Utf8View(maybe_value),
            ScalarValue::LargeUtf8(maybe_value) => Self::LargeUtf8(maybe_value),
            ScalarValue::Date32(maybe_value) => Self::Date32(maybe_value),
            ScalarValue::Date64(maybe_value) => Self::Date64(maybe_value),
            ScalarValue::Time32Second(maybe_value) => Self::Time32Second(maybe_value),
            ScalarValue::Time32Millisecond(maybe_value) => Self::Time32Millisecond(maybe_value),
            ScalarValue::Time64Microsecond(maybe_value) => Self::Time64Microsecond(maybe_value),
            ScalarValue::Time64Nanosecond(maybe_value) => Self::Time64Nanosecond(maybe_value),
            ScalarValue::TimestampSecond(maybe_value, maybe_str) => {
                Self::TimestampSecond(maybe_value, maybe_str.map(|s| s.to_string()))
            }
            ScalarValue::TimestampMillisecond(maybe_value, maybe_str) => {
                Self::TimestampMillisecond(maybe_value, maybe_str.map(|s| s.to_string()))
            }
            ScalarValue::TimestampMicrosecond(maybe_value, maybe_str) => {
                Self::TimestampMicrosecond(maybe_value, maybe_str.map(|s| s.to_string()))
            }
            ScalarValue::TimestampNanosecond(maybe_value, maybe_str) => {
                Self::TimestampNanosecond(maybe_value, maybe_str.map(|s| s.to_string()))
            }
            ScalarValue::IntervalYearMonth(maybe_value) => Self::IntervalYearMonth(maybe_value),
            _ => {
                return UnsupportedTypeSnafu {
                    data_type: value.data_type(),
                }
                .fail();
            }
        })
    }
}

impl TryFrom<SupportedScalarValue> for ScalarValue {
    type Error = Error;

    fn try_from(value: SupportedScalarValue) -> Result<Self, Self::Error> {
        Ok(match value {
            SupportedScalarValue::Boolean(maybe_value) => Self::Boolean(maybe_value),
            SupportedScalarValue::Int8(maybe_value) => Self::Int8(maybe_value),
            SupportedScalarValue::Int16(maybe_value) => Self::Int16(maybe_value),
            SupportedScalarValue::Int32(maybe_value) => Self::Int32(maybe_value),
            SupportedScalarValue::Int64(maybe_value) => Self::Int64(maybe_value),
            SupportedScalarValue::UInt8(maybe_value) => Self::UInt8(maybe_value),
            SupportedScalarValue::UInt16(maybe_value) => Self::UInt16(maybe_value),
            SupportedScalarValue::UInt32(maybe_value) => Self::UInt32(maybe_value),
            SupportedScalarValue::UInt64(maybe_value) => Self::UInt64(maybe_value),
            SupportedScalarValue::Utf8(maybe_value) => Self::Utf8(maybe_value),
            SupportedScalarValue::Utf8View(maybe_value) => Self::Utf8View(maybe_value),
            SupportedScalarValue::LargeUtf8(maybe_value) => Self::LargeUtf8(maybe_value),
            SupportedScalarValue::Date32(maybe_value) => Self::Date32(maybe_value),
            SupportedScalarValue::Date64(maybe_value) => Self::Date64(maybe_value),
            SupportedScalarValue::Time32Second(maybe_value) => Self::Time32Second(maybe_value),
            SupportedScalarValue::Time32Millisecond(maybe_value) => {
                Self::Time32Millisecond(maybe_value)
            }
            SupportedScalarValue::Time64Microsecond(maybe_value) => {
                Self::Time64Microsecond(maybe_value)
            }
            SupportedScalarValue::Time64Nanosecond(maybe_value) => {
                Self::Time64Nanosecond(maybe_value)
            }
            SupportedScalarValue::TimestampSecond(maybe_value, maybe_str) => {
                Self::TimestampSecond(maybe_value, maybe_str.map(Into::into))
            }
            SupportedScalarValue::TimestampMillisecond(maybe_value, maybe_str) => {
                Self::TimestampMillisecond(maybe_value, maybe_str.map(Into::into))
            }
            SupportedScalarValue::TimestampMicrosecond(maybe_value, maybe_str) => {
                Self::TimestampMicrosecond(maybe_value, maybe_str.map(Into::into))
            }
            SupportedScalarValue::TimestampNanosecond(maybe_value, maybe_str) => {
                Self::TimestampNanosecond(maybe_value, maybe_str.map(Into::into))
            }
            SupportedScalarValue::IntervalYearMonth(maybe_value) => {
                Self::IntervalYearMonth(maybe_value)
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const EXPRS_HASH: u64 = 7;

    #[test]
    fn test_encode_decode_boolean() {
        let values = vec![Some(true), Some(false), None];
        for value in values {
            let scalar = ScalarValue::Boolean(value);
            let encoded = encode_pair(&scalar, EXPRS_HASH).expect("Failed to encode boolean pair");
            let (decoded_scalar, decoded_exprs_hash) =
                decode_pair(&encoded).expect("Failed to decode boolean pair");
            assert_eq!(
                decoded_scalar, scalar,
                "Boolean value {value:?} failed to encode/decode correctly"
            );
            assert_eq!(
                decoded_exprs_hash, EXPRS_HASH,
                "Exprs hash for boolean {value:?} failed to encode/decode correctly"
            );
        }
    }

    #[test]
    fn test_encode_decode_int8() {
        let values = vec![Some(42_i8), Some(-42_i8), None];
        for value in values {
            let scalar = ScalarValue::Int8(value);
            let encoded = encode_pair(&scalar, EXPRS_HASH).expect("Failed to encode int8 pair");
            let (decoded_scalar, decoded_exprs_hash) =
                decode_pair(&encoded).expect("Failed to decode int8 pair");
            assert_eq!(
                decoded_scalar, scalar,
                "Int8 value {value:?} failed to encode/decode correctly"
            );
            assert_eq!(
                decoded_exprs_hash, EXPRS_HASH,
                "Exprs hash for int8 {value:?} failed to encode/decode correctly"
            );
        }
    }

    #[test]
    fn test_encode_decode_int16() {
        let values = vec![Some(1000_i16), Some(-1000_i16), None];
        for value in values {
            let scalar = ScalarValue::Int16(value);
            let encoded = encode_pair(&scalar, EXPRS_HASH).expect("Failed to encode int16 pair");
            let (decoded_scalar, decoded_exprs_hash) =
                decode_pair(&encoded).expect("Failed to decode int16 pair");
            assert_eq!(
                decoded_scalar, scalar,
                "Int16 value {value:?} failed to encode/decode correctly"
            );
            assert_eq!(
                decoded_exprs_hash, EXPRS_HASH,
                "Exprs hash for int16 {value:?} failed to encode/decode correctly"
            );
        }
    }

    #[test]
    fn test_encode_decode_int32() {
        let values = vec![Some(100_000_i32), Some(-100_000_i32), None];
        for value in values {
            let scalar = ScalarValue::Int32(value);
            let encoded = encode_pair(&scalar, EXPRS_HASH).expect("Failed to encode int32 pair");
            let (decoded_scalar, decoded_exprs_hash) =
                decode_pair(&encoded).expect("Failed to decode int32 pair");
            assert_eq!(
                decoded_scalar, scalar,
                "Int32 value {value:?} failed to encode/decode correctly"
            );
            assert_eq!(
                decoded_exprs_hash, EXPRS_HASH,
                "Exprs hash for int32 {value:?} failed to encode/decode correctly"
            );
        }
    }

    #[test]
    fn test_encode_decode_int64() {
        let values = vec![Some(1_000_000_000_i64), Some(-1_000_000_000_i64), None];
        for value in values {
            let scalar = ScalarValue::Int64(value);
            let encoded = encode_pair(&scalar, EXPRS_HASH).expect("Failed to encode int64 pair");
            let (decoded_scalar, decoded_exprs_hash) =
                decode_pair(&encoded).expect("Failed to decode int64 pair");
            assert_eq!(
                decoded_scalar, scalar,
                "Int64 value {value:?} failed to encode/decode correctly"
            );
            assert_eq!(
                decoded_exprs_hash, EXPRS_HASH,
                "Exprs hash for int64 {value:?} failed to encode/decode correctly"
            );
        }
    }

    #[test]
    fn test_encode_decode_uint8() {
        let values = vec![Some(255_u8), Some(0_u8), None];
        for value in values {
            let scalar = ScalarValue::UInt8(value);
            let encoded = encode_pair(&scalar, EXPRS_HASH).expect("Failed to encode uint8 pair");
            let (decoded_scalar, decoded_exprs_hash) =
                decode_pair(&encoded).expect("Failed to decode uint8 pair");
            assert_eq!(
                decoded_scalar, scalar,
                "UInt8 value {value:?} failed to encode/decode correctly"
            );
            assert_eq!(
                decoded_exprs_hash, EXPRS_HASH,
                "Exprs hash for uint8 {value:?} failed to encode/decode correctly"
            );
        }
    }

    #[test]
    fn test_encode_decode_uint16() {
        let values = vec![Some(65_535_u16), Some(0_u16), None];
        for value in values {
            let scalar = ScalarValue::UInt16(value);
            let encoded = encode_pair(&scalar, EXPRS_HASH).expect("Failed to encode uint16 pair");
            let (decoded_scalar, decoded_exprs_hash) =
                decode_pair(&encoded).expect("Failed to decode uint16 pair");
            assert_eq!(
                decoded_scalar, scalar,
                "UInt16 value {value:?} failed to encode/decode correctly"
            );
            assert_eq!(
                decoded_exprs_hash, EXPRS_HASH,
                "Exprs hash for uint16 {value:?} failed to encode/decode correctly"
            );
        }
    }

    #[test]
    fn test_encode_decode_uint32() {
        let values = vec![Some(4_294_967_295_u32), Some(0_u32), None];
        for value in values {
            let scalar = ScalarValue::UInt32(value);
            let encoded = encode_pair(&scalar, EXPRS_HASH).expect("Failed to encode uint32 pair");
            let (decoded_scalar, decoded_exprs_hash) =
                decode_pair(&encoded).expect("Failed to decode uint32 pair");
            assert_eq!(
                decoded_scalar, scalar,
                "UInt32 value {value:?} failed to encode/decode correctly"
            );
            assert_eq!(
                decoded_exprs_hash, EXPRS_HASH,
                "Exprs hash for uint32 {value:?} failed to encode/decode correctly"
            );
        }
    }

    #[test]
    fn test_encode_decode_uint64() {
        let values = vec![Some(18_446_744_073_709_551_615_u64), Some(0_u64), None];
        for value in values {
            let scalar = ScalarValue::UInt64(value);
            let encoded = encode_pair(&scalar, EXPRS_HASH).expect("Failed to encode uint64 pair");
            let (decoded_scalar, decoded_exprs_hash) =
                decode_pair(&encoded).expect("Failed to decode uint64 pair");
            assert_eq!(
                decoded_scalar, scalar,
                "UInt64 value {value:?} failed to encode/decode correctly"
            );
            assert_eq!(
                decoded_exprs_hash, EXPRS_HASH,
                "Exprs hash for uint64 {value:?} failed to encode/decode correctly"
            );
        }
    }

    #[test]
    fn test_encode_decode_utf8() {
        let values = vec![Some("hello".to_string()), Some(String::new()), None];
        for value in values {
            let scalar = ScalarValue::Utf8(value.clone());
            let encoded = encode_pair(&scalar, EXPRS_HASH).expect("Failed to encode utf8 pair");
            let (decoded_scalar, decoded_exprs_hash) =
                decode_pair(&encoded).expect("Failed to decode utf8 pair");
            assert_eq!(
                decoded_scalar, scalar,
                "Utf8 value {value:?} failed to encode/decode correctly"
            );
            assert_eq!(
                decoded_exprs_hash, EXPRS_HASH,
                "Exprs hash for utf8 {value:?} failed to encode/decode correctly"
            );
        }
    }

    #[test]
    fn test_encode_decode_utf8_view() {
        let values = vec![Some("world".to_string()), Some(String::new()), None];
        for value in values {
            let scalar = ScalarValue::Utf8View(value.clone());
            let encoded = encode_pair(&scalar, EXPRS_HASH).expect("Failed to encode utf8view pair");
            let (decoded_scalar, decoded_exprs_hash) =
                decode_pair(&encoded).expect("Failed to decode utf8view pair");
            assert_eq!(
                decoded_scalar, scalar,
                "Utf8View value {value:?} failed to encode/decode correctly"
            );
            assert_eq!(
                decoded_exprs_hash, EXPRS_HASH,
                "Exprs hash for utf8view {value:?} failed to encode/decode correctly"
            );
        }
    }

    #[test]
    fn test_encode_decode_large_utf8() {
        let values = vec![Some("large string".to_string()), Some(String::new()), None];
        for value in values {
            let scalar = ScalarValue::LargeUtf8(value.clone());
            let encoded =
                encode_pair(&scalar, EXPRS_HASH).expect("Failed to encode large utf8 pair");
            let (decoded_scalar, decoded_exprs_hash) =
                decode_pair(&encoded).expect("Failed to decode large utf8 pair");
            assert_eq!(
                decoded_scalar, scalar,
                "LargeUtf8 value {value:?} failed to encode/decode correctly"
            );
            assert_eq!(
                decoded_exprs_hash, EXPRS_HASH,
                "Exprs hash for large utf8 {value:?} failed to encode/decode correctly"
            );
        }
    }
}
