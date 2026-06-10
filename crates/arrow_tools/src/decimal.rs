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

    #[snafu(display("Decimal value exceeds the 16-byte `i128` range, got {} bytes", value.len()))]
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
            // Widen before subtracting so a hostile/truncated `src_scale`
            // (e.g. -128 from a malformed Debezium `scale`) cannot overflow i8
            // and panic in debug builds; the magnitude is then bounded by
            // `checked_pow` below.
            let diff = i16::from(dst_scale) - i16::from(src_scale);
            let exp = u32::try_from(diff).map_err(|_| OverflowSnafu.build())?;
            let mul = 10_i128.checked_pow(exp).context(OverflowSnafu)?;
            unscaled.checked_mul(mul).context(OverflowSnafu)
        }
        Greater => {
            let diff = i16::from(src_scale) - i16::from(dst_scale);
            let exp = u32::try_from(diff).map_err(|_| OverflowSnafu.build())?;
            let div = 10_i128.checked_pow(exp).context(OverflowSnafu)?;
            Ok(unscaled / div)
        }
    }
}

/// Decodes a base64-encoded big-endian signed integer into an `i128`.
///
/// Debezium encodes decimals as the minimal-width big-endian two's-complement
/// representation of a Java `BigInteger` (`BigInteger.toByteArray()`), so a
/// negative value arrives with fewer than 16 bytes and the sign bit set in its
/// most-significant byte (e.g. `-1` is the single byte `0xFF`). The value must
/// therefore be *sign-extended* to 16 bytes — left-padding with `0x00` would
/// turn `-1` into `255`. Positive values whose top bit is clear are zero-padded
/// as before.
fn decode_base64_decimal(s: &str) -> Result<i128> {
    let bytes = BASE64_STANDARD.decode(s).context(Base64DecodeSnafu)?;

    if bytes.len() > 16 {
        return BytesLengthSnafu { value: bytes }.fail();
    }

    // Sign-extend using the high bit of the most-significant (first) byte.
    let pad = if bytes.first().is_some_and(|b| b & 0x80 != 0) {
        0xFF
    } else {
        0x00
    };
    let mut arr = [pad; 16];
    let start = 16 - bytes.len();
    arr[start..].copy_from_slice(&bytes);
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Encodes `bytes` exactly as Debezium does — minimal-width big-endian
    /// two's-complement, base64 — without zero/sign padding to 16 bytes.
    fn b64(bytes: &[u8]) -> String {
        BASE64_STANDARD.encode(bytes)
    }

    #[test]
    fn rescale_does_not_panic_on_extreme_scales() {
        // Regression: a hostile/truncated scale (e.g. `i8::MIN` from a malformed
        // Debezium `scale`) must yield an Overflow error, never an i8-subtraction
        // panic (which would crash debug builds on attacker-controlled CDC input).
        for (src, dst) in [
            (i8::MIN, 20_i8),
            (20, i8::MIN),
            (i8::MIN, i8::MAX),
            (i8::MAX, i8::MIN),
        ] {
            let r = rescale_i128(123, src, dst);
            assert!(
                r.is_err(),
                "expected Overflow for scales ({src},{dst}), got {r:?}"
            );
        }
        // Normal rescales are unaffected.
        assert_eq!(rescale_i128(5, 0, 2).expect("ok"), 500);
        assert_eq!(rescale_i128(500, 2, 0).expect("ok"), 5);
    }

    #[test]
    fn negative_minimal_width_base64_is_sign_extended() {
        // Debezium encodes -1 as the single byte 0xFF (BigInteger.toByteArray()).
        // Zero-padding to 16 bytes would decode this as 255 — the value must be
        // sign-extended instead.
        let v =
            convert_json_to_decimal(&Json::String(b64(&[0xFF])), 0).expect("decode should succeed");
        assert_eq!(v, Some(-1));

        // -123 minimally encoded as the single byte 0x85.
        let v =
            convert_json_to_decimal(&Json::String(b64(&[0x85])), 0).expect("decode should succeed");
        assert_eq!(v, Some(-123));

        // A larger negative value, -12_345 == 0xCFC7 in two's complement.
        let v = convert_json_to_decimal(&Json::String(b64(&[0xCF, 0xC7])), 0)
            .expect("decode should succeed");
        assert_eq!(v, Some(-12_345));
    }

    #[test]
    fn positive_minimal_width_base64_is_zero_extended() {
        // 255 minimally encoded as 0x00 0xFF — the leading zero marks it positive
        // and must not be treated as sign-extension.
        let v = convert_json_to_decimal(&Json::String(b64(&[0x00, 0xFF])), 0)
            .expect("decode should succeed");
        assert_eq!(v, Some(255));

        // 127 == 0x7F (top bit clear), single byte.
        let v =
            convert_json_to_decimal(&Json::String(b64(&[0x7F])), 0).expect("decode should succeed");
        assert_eq!(v, Some(127));
    }

    #[test]
    fn negative_object_value_is_rescaled_correctly() {
        // VariableScaleDecimal precise mode: {"scale": 2, "value": <base64>}.
        // -12.34 has unscaled value -1234 == 0xFB2E in two's complement.
        let input = json!({ "scale": 2, "value": b64(&[0xFB, 0x2E]) });
        let v = convert_json_to_decimal(&input, 2).expect("decode should succeed");
        assert_eq!(v, Some(-1234));
    }

    #[test]
    fn full_width_16_byte_encoding_still_decodes() {
        // A full 16-byte encoding (e.g. produced by i128::to_be_bytes) must keep
        // decoding correctly for both signs.
        let v = convert_json_to_decimal(&Json::String(b64(&(-12_345_i128).to_be_bytes())), 0)
            .expect("decode should succeed");
        assert_eq!(v, Some(-12_345));

        let v = convert_json_to_decimal(&Json::String(b64(&(98_765_i128).to_be_bytes())), 0)
            .expect("decode should succeed");
        assert_eq!(v, Some(98_765));
    }

    #[test]
    fn over_16_bytes_is_rejected() {
        let result = convert_json_to_decimal(&Json::String(b64(&[0x01; 17])), 0);
        result.expect_err("a value wider than 16 bytes must be rejected, not truncated");
    }
}
