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
    #[snafu(display("Failed to serialize scalar value: {source}"))]
    Serialize { source: serde_qs::Error },

    #[snafu(display("Failed to deserialize scalar value: {source}"))]
    Deserialize { source: serde_qs::Error },

    #[snafu(display("Unsupported scalar value type: {data_type}"))]
    UnsupportedType { data_type: DataType },
}

/// Converts a [`ScalarValue`] to its [`String`] representation.
///
/// # Errors
/// Returns an error if the [`ScalarValue`] is not supported.
pub fn encode_scalar_value(value: &ScalarValue) -> Result<String, Error> {
    let supported_value = SupportedScalarValue::try_from(value.clone())?;
    let encoded = serde_qs::to_string(&supported_value).context(SerializeSnafu)?;
    Ok(encoded)
}

/// Converts a [`String`] back to a [`ScalarValue`].
///
/// # Errors
/// Returns an error if a [`ScalarValue`] cannot be created.
pub fn decode_scalar_value(value: &str) -> Result<ScalarValue, Error> {
    let decoded: SupportedScalarValue = serde_qs::from_str(value).context(DeserializeSnafu)?;
    let value = ScalarValue::try_from(decoded)?;
    Ok(value)
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
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_boolean() {
        let values = vec![Some(true), Some(false), None];
        for value in values {
            let scalar = ScalarValue::Boolean(value);
            let encoded =
                encode_scalar_value(&scalar).expect("Failed to encode boolean scalar value");
            let decoded =
                decode_scalar_value(&encoded).expect("Failed to decode boolean scalar value");
            assert_eq!(
                decoded, scalar,
                "Boolean value {value:?} failed to encode/decode correctly"
            );
        }
    }

    #[test]
    fn test_encode_decode_int8() {
        let values = vec![Some(42_i8), Some(-42_i8), None];
        for value in values {
            let scalar = ScalarValue::Int8(value);
            let encoded = encode_scalar_value(&scalar).expect("Failed to encode int8 scalar value");
            let decoded =
                decode_scalar_value(&encoded).expect("Failed to decode int8 scalar value");
            assert_eq!(
                decoded, scalar,
                "Int8 value {value:?} failed to encode/decode correctly"
            );
        }
    }

    #[test]
    fn test_encode_decode_int16() {
        let values = vec![Some(1000_i16), Some(-1000_i16), None];
        for value in values {
            let scalar = ScalarValue::Int16(value);
            let encoded =
                encode_scalar_value(&scalar).expect("Failed to encode int16 scalar value");
            let decoded =
                decode_scalar_value(&encoded).expect("Failed to decode int16 scalar value");
            assert_eq!(
                decoded, scalar,
                "Int16 value {value:?} failed to encode/decode correctly"
            );
        }
    }

    #[test]
    fn test_encode_decode_int32() {
        let values = vec![Some(100_000_i32), Some(-100_000_i32), None];
        for value in values {
            let scalar = ScalarValue::Int32(value);
            let encoded =
                encode_scalar_value(&scalar).expect("Failed to encode int32 scalar value");
            let decoded =
                decode_scalar_value(&encoded).expect("Failed to decode int32 scalar value");
            assert_eq!(
                decoded, scalar,
                "Int32 value {value:?} failed to encode/decode correctly"
            );
        }
    }

    #[test]
    fn test_encode_decode_int64() {
        let values = vec![Some(1_000_000_000_i64), Some(-1_000_000_000_i64), None];
        for value in values {
            let scalar = ScalarValue::Int64(value);
            let encoded =
                encode_scalar_value(&scalar).expect("Failed to encode int64 scalar value");
            let decoded =
                decode_scalar_value(&encoded).expect("Failed to decode int64 scalar value");
            assert_eq!(
                decoded, scalar,
                "Int64 value {value:?} failed to encode/decode correctly"
            );
        }
    }

    #[test]
    fn test_encode_decode_uint8() {
        let values = vec![Some(255_u8), Some(0_u8), None];
        for value in values {
            let scalar = ScalarValue::UInt8(value);
            let encoded =
                encode_scalar_value(&scalar).expect("Failed to encode uint8 scalar value");
            let decoded =
                decode_scalar_value(&encoded).expect("Failed to decode uint8 scalar value");
            assert_eq!(
                decoded, scalar,
                "UInt8 value {value:?} failed to encode/decode correctly"
            );
        }
    }

    #[test]
    fn test_encode_decode_uint16() {
        let values = vec![Some(65_535_u16), Some(0_u16), None];
        for value in values {
            let scalar = ScalarValue::UInt16(value);
            let encoded =
                encode_scalar_value(&scalar).expect("Failed to encode uint16 scalar value");
            let decoded =
                decode_scalar_value(&encoded).expect("Failed to decode uint16 scalar value");
            assert_eq!(
                decoded, scalar,
                "UInt16 value {value:?} failed to encode/decode correctly"
            );
        }
    }

    #[test]
    fn test_encode_decode_uint32() {
        let values = vec![Some(4_294_967_295_u32), Some(0_u32), None];
        for value in values {
            let scalar = ScalarValue::UInt32(value);
            let encoded =
                encode_scalar_value(&scalar).expect("Failed to encode uint32 scalar value");
            let decoded =
                decode_scalar_value(&encoded).expect("Failed to decode uint32 scalar value");
            assert_eq!(
                decoded, scalar,
                "UInt32 value {value:?} failed to encode/decode correctly"
            );
        }
    }

    #[test]
    fn test_encode_decode_uint64() {
        let values = vec![Some(18_446_744_073_709_551_615_u64), Some(0_u64), None];
        for value in values {
            let scalar = ScalarValue::UInt64(value);
            let encoded =
                encode_scalar_value(&scalar).expect("Failed to encode uint64 scalar value");
            let decoded =
                decode_scalar_value(&encoded).expect("Failed to decode uint64 scalar value");
            assert_eq!(
                decoded, scalar,
                "UInt64 value {value:?} failed to encode/decode correctly"
            );
        }
    }

    #[test]
    fn test_encode_decode_utf8() {
        let values = vec![Some("hello".to_string()), Some(String::new()), None];
        for value in values {
            let scalar = ScalarValue::Utf8(value.clone());
            let encoded = encode_scalar_value(&scalar).expect("Failed to encode utf8 scalar value");
            let decoded =
                decode_scalar_value(&encoded).expect("Failed to decode utf8 scalar value");
            assert_eq!(
                decoded, scalar,
                "Utf8 value {value:?} failed to encode/decode correctly"
            );
        }
    }

    #[test]
    fn test_encode_decode_utf8_view() {
        let values = vec![Some("world".to_string()), Some(String::new()), None];
        for value in values {
            let scalar = ScalarValue::Utf8View(value.clone());
            let encoded =
                encode_scalar_value(&scalar).expect("Failed to encode utf8view scalar value");
            let decoded =
                decode_scalar_value(&encoded).expect("Failed to decode utf8view scalar value");
            assert_eq!(
                decoded, scalar,
                "Utf8View value {value:?} failed to encode/decode correctly"
            );
        }
    }

    #[test]
    fn test_encode_decode_large_utf8() {
        let values = vec![Some("large string".to_string()), Some(String::new()), None];
        for value in values {
            let scalar = ScalarValue::LargeUtf8(value.clone());
            let encoded =
                encode_scalar_value(&scalar).expect("Failed to encode large utf8 scalar value");
            let decoded =
                decode_scalar_value(&encoded).expect("Failed to decode large utf8 scalar value");
            assert_eq!(
                decoded, scalar,
                "LargeUtf8 value {value:?} failed to encode/decode correctly"
            );
        }
    }
}
