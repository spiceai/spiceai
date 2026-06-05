/*
Copyright 2026 The Spice.ai OSS Authors

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

//! Utilities for converting JSON values to Arrow `Decimal128` (scaled `i128`).

use base64::prelude::*;
use serde_json::Value as Json;
use snafu::prelude::*;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Overflow converting to scaled decimal"))]
    Overflow,

    #[snafu(display("Invalid decimal value: {reason}"))]
    Invalid { reason: String },

    #[snafu(display("Failed to decode base64-encoded decimal: {source}"))]
    Base64Decode { source: base64::DecodeError },

    #[snafu(display("Decimal bytes must be exactly 16, got {}", value.len()))]
    BytesLength { value: Vec<u8> },

    #[snafu(display("Missing `scale` field in decimal object"))]
    MissingScale,

    #[snafu(display("`scale` field is not an integer"))]
    NonIntegerScale,

    #[snafu(display("Missing `value` field in decimal object"))]
    MissingValue,

    #[snafu(display("Unsupported JSON type for decimal: {actual_type}"))]
    UnsupportedType { actual_type: &'static str },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Rescales `unscaled` from `src_scale` decimal places to `dst_scale`.
///
/// # Errors
/// Returns [`Error::Overflow`] if the rescaling would overflow `i128`.
pub fn rescale_i128(unscaled: i128, src_scale: i8, dst_scale: i8) -> Result<i128> {
    use std::cmp::Ordering::{Equal, Greater, Less};
    match src_scale.cmp(&dst_scale) {
        Equal => Ok(unscaled),
        Less => {
            let diff = dst_scale - src_scale;
            let mul = 10_i128
                .checked_pow(u32::from(diff.cast_unsigned()))
                .context(OverflowSnafu)?;
            unscaled.checked_mul(mul).context(OverflowSnafu)
        }
        Greater => {
            let diff = src_scale - dst_scale;
            let div = 10_i128
                .checked_pow(u32::from(diff.cast_unsigned()))
                .context(OverflowSnafu)?;
            Ok(unscaled / div)
        }
    }
}

/// Decodes a base64-encoded big-endian signed integer into an `i128`.
fn decode_base64_decimal(s: &str) -> Result<i128> {
    let mut bytes = BASE64_STANDARD.decode(s).context(Base64DecodeSnafu)?;

    while bytes.len() < 16 {
        bytes.insert(0, 0);
    }

    let arr: [u8; 16] = bytes
        .try_into()
        .map_err(|v| Error::BytesLength { value: v })?;
    Ok(i128::from_be_bytes(arr))
}

/// Converts a plain JSON number to an `i128` scaled to `target_scale` decimal places,
/// suitable for storage in an Arrow `Decimal128` column.
///
/// Parses the number's string representation directly to avoid `f64` precision loss.
/// Scientific notation falls back to `f64` rounding.
///
/// # Errors
/// Returns [`Error::Invalid`] for unparseable input or [`Error::Overflow`] on scaling overflow.
pub fn parse_number_to_decimal(n: &serde_json::Number, target_scale: i8) -> Result<i128> {
    let s = n.to_string();

    if s.bytes().any(|b| b == b'e' || b == b'E') {
        let f: f64 = s.parse().map_err(|_| Error::Invalid {
            reason: format!("cannot parse '{s}' as decimal"),
        })?;
        let scale_factor = 10_i128
            .checked_pow(u32::from(target_scale.cast_unsigned()))
            .context(OverflowSnafu)?;
        #[expect(clippy::cast_possible_truncation, clippy::cast_precision_loss)]
        return Ok((f * scale_factor as f64).round() as i128);
    }

    let negative = s.starts_with('-');
    let digits = if negative { &s[1..] } else { &s };

    let (int_str, frac_str) = match digits.find('.') {
        Some(pos) => (&digits[..pos], &digits[pos + 1..]),
        None => (digits, ""),
    };

    let int_val: i128 = int_str.parse().map_err(|_| Error::Invalid {
        reason: format!("cannot parse integer part '{int_str}'"),
    })?;

    let frac_scale = i8::try_from(frac_str.len()).map_err(|_| Error::Invalid {
        reason: "fractional part too long".to_string(),
    })?;

    let frac_val: i128 = if frac_str.is_empty() {
        0
    } else {
        frac_str.parse().map_err(|_| Error::Invalid {
            reason: format!("cannot parse fractional part '{frac_str}'"),
        })?
    };

    let scaled_int = rescale_i128(int_val, 0, target_scale)?;
    let scaled_frac = rescale_i128(frac_val, frac_scale, target_scale)?;
    let result = scaled_int + scaled_frac;

    Ok(if negative { -result } else { result })
}

/// Parses a JSON value into a scaled `i128` for Arrow `Decimal128` storage.
///
/// Supported inputs:
/// - `null` → `None`
/// - JSON string: base64-encoded big-endian signed integer (Debezium bytes mode)
/// - JSON object `{"scale": <int>, "value": "<base64>"}` (Debezium precise mode)
/// - JSON number: plain decimal (Debezium `decimal.handling.mode=double`)
///
/// # Errors
/// Returns an [`Error`] variant for invalid input, base64 decode failures, scaling overflow,
/// missing object fields, or unsupported JSON types.
pub fn convert_json_to_decimal(v: &Json, target_scale: i8) -> Result<Option<i128>> {
    if !(0..=38).contains(&target_scale) {
        return InvalidSnafu {
            reason: "target_scale must be in 0..=38".to_string(),
        }
        .fail();
    }

    match v {
        Json::Null => Ok(None),
        Json::String(s) => Ok(Some(decode_base64_decimal(s)?)),
        Json::Object(m) => {
            #[expect(clippy::cast_possible_truncation)]
            let src_scale = m
                .get("scale")
                .context(MissingScaleSnafu)?
                .as_i64()
                .context(NonIntegerScaleSnafu)? as i8;

            let value = m
                .get("value")
                .and_then(|x| x.as_str())
                .context(MissingValueSnafu)?;

            let unscaled = decode_base64_decimal(value)?;
            Ok(Some(rescale_i128(unscaled, src_scale, target_scale)?))
        }
        Json::Number(n) => Ok(Some(parse_number_to_decimal(n, target_scale)?)),
        _ => {
            let actual_type = match v {
                Json::Bool(_) => "boolean",
                Json::Array(_) => "array",
                _ => "unknown",
            };
            UnsupportedTypeSnafu { actual_type }.fail()
        }
    }
}
