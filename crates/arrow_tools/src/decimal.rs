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

use arrow::datatypes::{DECIMAL128_MAX_PRECISION, Decimal128Type, DecimalType};
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

    #[snafu(display(
        "Decimal value {value} does not fit a Decimal128 of precision {precision} (max {} digits)",
        precision
    ))]
    PrecisionExceeded { value: i128, precision: u8 },
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
/// The result is bounded by the physical `i128` range only. It is **not** checked
/// against any column's declared precision — a scaled value can be representable
/// as an `i128` and still be too wide for the `Decimal128(p, s)` it is destined
/// for. [`convert_json_to_decimal`] applies that check; callers reaching for this
/// primitive directly must apply it themselves before building an Arrow array.
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
        #[expect(clippy::cast_precision_loss)]
        let scaled = (f * scale_factor as f64).round();
        // `as i128` *saturates* on an out-of-range float, so an unchecked cast
        // would silently store `i128::MAX` for a value the column cannot hold.
        // The representable range is `[-2^127, 2^127)`; both bounds are exact
        // in f64, so the comparison admits exactly the castable values.
        let limit = 2.0_f64.powi(127);
        ensure!(
            scaled.is_finite() && scaled >= -limit && scaled < limit,
            OverflowSnafu
        );
        #[expect(clippy::cast_possible_truncation)]
        return Ok(scaled as i128);
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
    // Each half fits on its own, but their sum need not: at `target_scale` 38,
    // `1.<38 nines>` scales to just under `2 * 10^38`, past `i128::MAX`. An
    // unchecked add would wrap in release builds and store a sign-flipped value.
    let result = scaled_int.checked_add(scaled_frac).context(OverflowSnafu)?;

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
/// `precision` is the destination column's declared precision. A value that does
/// not fit it is rejected rather than written: `Decimal128Builder` does not check
/// precision on append, so an unchecked value would produce an array whose
/// contents contradict its own schema.
///
/// # Errors
/// Returns an [`Error`] variant for invalid input, base64 decode failures, scaling overflow,
/// missing object fields, unsupported JSON types, or a value too wide for `precision`.
pub fn convert_json_to_decimal(v: &Json, precision: u8, target_scale: i8) -> Result<Option<i128>> {
    if !(0..=38).contains(&target_scale) {
        return InvalidSnafu {
            reason: "target_scale must be in 0..=38".to_string(),
        }
        .fail();
    }
    ensure!(
        (1..=DECIMAL128_MAX_PRECISION).contains(&precision),
        InvalidSnafu {
            reason: format!("precision must be in 1..={DECIMAL128_MAX_PRECISION}, got {precision}")
        }
    );

    let unscaled = match v {
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
    }?;

    // Every input form funnels through here, so one check covers base64 strings,
    // `{scale, value}` objects and plain JSON numbers alike.
    if let Some(value) = unscaled {
        ensure!(
            Decimal128Type::is_valid_decimal_precision(value, precision),
            PrecisionExceededSnafu { value, precision }
        );
    }
    Ok(unscaled)
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
        let v = convert_json_to_decimal(&Json::String(b64(&[0xFF])), 38, 0)
            .expect("decode should succeed");
        assert_eq!(v, Some(-1));

        // -123 minimally encoded as the single byte 0x85.
        let v = convert_json_to_decimal(&Json::String(b64(&[0x85])), 38, 0)
            .expect("decode should succeed");
        assert_eq!(v, Some(-123));

        // A larger negative value, -12_345 == 0xCFC7 in two's complement.
        let v = convert_json_to_decimal(&Json::String(b64(&[0xCF, 0xC7])), 38, 0)
            .expect("decode should succeed");
        assert_eq!(v, Some(-12_345));
    }

    #[test]
    fn positive_minimal_width_base64_is_zero_extended() {
        // 255 minimally encoded as 0x00 0xFF — the leading zero marks it positive
        // and must not be treated as sign-extension.
        let v = convert_json_to_decimal(&Json::String(b64(&[0x00, 0xFF])), 38, 0)
            .expect("decode should succeed");
        assert_eq!(v, Some(255));

        // 127 == 0x7F (top bit clear), single byte.
        let v = convert_json_to_decimal(&Json::String(b64(&[0x7F])), 38, 0)
            .expect("decode should succeed");
        assert_eq!(v, Some(127));
    }

    #[test]
    fn negative_object_value_is_rescaled_correctly() {
        // VariableScaleDecimal precise mode: {"scale": 2, "value": <base64>}.
        // -12.34 has unscaled value -1234 == 0xFB2E in two's complement.
        let input = json!({ "scale": 2, "value": b64(&[0xFB, 0x2E]) });
        let v = convert_json_to_decimal(&input, 38, 2).expect("decode should succeed");
        assert_eq!(v, Some(-1234));
    }

    #[test]
    fn full_width_16_byte_encoding_still_decodes() {
        // A full 16-byte encoding (e.g. produced by i128::to_be_bytes) must keep
        // decoding correctly for both signs.
        let v = convert_json_to_decimal(&Json::String(b64(&(-12_345_i128).to_be_bytes())), 38, 0)
            .expect("decode should succeed");
        assert_eq!(v, Some(-12_345));

        let v = convert_json_to_decimal(&Json::String(b64(&(98_765_i128).to_be_bytes())), 38, 0)
            .expect("decode should succeed");
        assert_eq!(v, Some(98_765));
    }

    #[test]
    fn over_16_bytes_is_rejected() {
        let result = convert_json_to_decimal(&Json::String(b64(&[0x01; 17])), 38, 0);
        result.expect_err("a value wider than 16 bytes must be rejected, not truncated");
    }

    fn number(literal: &str) -> serde_json::Number {
        serde_json::from_str::<serde_json::Number>(literal).expect("valid JSON number")
    }

    /// A float too large for the column must be an error, not `i128::MAX`.
    /// `as i128` saturates, so an unchecked cast would silently store a wrong
    /// number in a decimal column.
    #[test]
    fn an_out_of_range_scientific_number_overflows_instead_of_saturating() {
        for literal in ["1e300", "-1e300", "1e39"] {
            let result = parse_number_to_decimal(&number(literal), 2);
            assert!(
                matches!(result, Err(Error::Overflow)),
                "{literal} must overflow, got {result:?}"
            );
        }
    }

    #[test]
    fn a_scientific_number_within_range_still_converts() {
        assert_eq!(
            parse_number_to_decimal(&number("1.5e3"), 2).expect("in range"),
            150_000
        );
        assert_eq!(
            parse_number_to_decimal(&number("-1.5e3"), 2).expect("in range"),
            -150_000
        );
    }

    /// Regression test for #12747, case 1: the two halves are each in range but
    /// their sum is not. `1.8` at scale 38 is `1.0e38 + 0.8e38`, past
    /// `i128::MAX`. `overflow-checks` is off in release, so an unchecked add
    /// wrapped this to a large *negative* decimal and served it as fact.
    #[test]
    fn a_sum_of_two_in_range_halves_that_overflows_is_an_error() {
        assert!(
            matches!(
                parse_number_to_decimal(&number("1.8"), 38),
                Err(Error::Overflow)
            ),
            "1.8 at scale 38 must overflow rather than wrap negative"
        );
        assert!(matches!(
            parse_number_to_decimal(&number("-1.8"), 38),
            Err(Error::Overflow)
        ));
    }

    /// Regression test for #12747, case 2: a float-to-int `as` cast saturates
    /// rather than wrapping, so an out-of-range scientific-notation value became
    /// `i128::MAX` — a plausible-looking wrong number.
    #[test]
    fn a_scientific_value_past_the_i128_range_is_an_error_not_i128_max() {
        let result = parse_number_to_decimal(&number("1e39"), 0);
        assert!(
            matches!(result, Err(Error::Overflow)),
            "1e39 must overflow, got {result:?}"
        );
        assert!(matches!(
            parse_number_to_decimal(&number("-1e39"), 0),
            Err(Error::Overflow)
        ));
    }

    /// `parse_number_to_decimal` guards the physical `i128` range and nothing
    /// more: `1` at scale 38 scales to `10^38`, which is a 39-digit value that
    /// no `Decimal128` can hold (max precision is 38). The primitive returns it;
    /// rejecting it is `convert_json_to_decimal`'s job. Pinned so the split in
    /// responsibility stays deliberate rather than becoming a silent hole.
    #[test]
    fn parse_number_to_decimal_bounds_i128_only_not_decimal128_precision() {
        let too_wide = parse_number_to_decimal(&number("1"), 38).expect("fits i128");
        assert_eq!(
            too_wide,
            100_000_000_000_000_000_000_000_000_000_000_000_000_i128
        );
        assert_eq!(too_wide.to_string().len(), 39, "39 digits: past Decimal128");
        assert!(matches!(
            parse_number_to_decimal(&number("2"), 38),
            Err(Error::Overflow)
        ));
    }

    /// The value above must not reach an Arrow array. `Decimal128Builder` does
    /// not check precision on append, so an unchecked value would build an array
    /// whose contents contradict its declared schema.
    #[test]
    fn a_value_wider_than_the_declared_precision_is_rejected() {
        for precision in [1_u8, 10, 38] {
            assert!(
                matches!(
                    convert_json_to_decimal(&json!(1), precision, 38),
                    Err(Error::PrecisionExceeded { .. })
                ),
                "10^38 must not pass precision {precision}"
            );
        }
    }

    /// A narrow column is the common case: `Decimal128(3, 2)` holds up to
    /// `9.99`, so `10.00` has to be refused rather than silently stored.
    #[test]
    fn a_narrow_decimal_column_accepts_its_maximum_and_refuses_one_past_it() {
        assert_eq!(
            convert_json_to_decimal(&json!(9.99), 3, 2).expect("9.99 fits (3,2)"),
            Some(999)
        );
        assert!(matches!(
            convert_json_to_decimal(&json!(10.00), 3, 2),
            Err(Error::PrecisionExceeded {
                value: 1000,
                precision: 3
            })
        ));
        assert_eq!(
            convert_json_to_decimal(&json!(-9.99), 3, 2).expect("-9.99 fits (3,2)"),
            Some(-999)
        );
        assert!(matches!(
            convert_json_to_decimal(&json!(-10.00), 3, 2),
            Err(Error::PrecisionExceeded { .. })
        ));
    }

    /// The precision check covers every input form, not just plain numbers:
    /// base64 bytes and `{scale, value}` objects funnel through the same guard.
    #[test]
    fn the_precision_check_covers_base64_and_object_forms_too() {
        assert!(matches!(
            convert_json_to_decimal(&Json::String(b64(&1000_i128.to_be_bytes())), 3, 2),
            Err(Error::PrecisionExceeded { .. })
        ));
        assert!(matches!(
            convert_json_to_decimal(
                &json!({"scale": 2, "value": b64(&1000_i128.to_be_bytes())}),
                3,
                2
            ),
            Err(Error::PrecisionExceeded { .. })
        ));
    }

    /// A precision Arrow itself cannot express is a configuration error, not a
    /// value error.
    #[test]
    fn a_precision_outside_the_decimal128_range_is_rejected() {
        for precision in [0_u8, 39, u8::MAX] {
            assert!(matches!(
                convert_json_to_decimal(&json!(1), precision, 2),
                Err(Error::Invalid { .. })
            ));
        }
    }

    /// Narrowing the scale truncates toward zero — it does not round — and does
    /// so identically for both signs, so `-1.239` and `1.239` stay symmetric.
    #[test]
    fn narrowing_truncates_toward_zero_for_both_signs() {
        assert_eq!(
            parse_number_to_decimal(&number("1.239"), 2).expect("ok"),
            123
        );
        assert_eq!(
            parse_number_to_decimal(&number("-1.239"), 2).expect("ok"),
            -123
        );
        assert_eq!(rescale_i128(1_235, 3, 2).expect("ok"), 123);
        assert_eq!(rescale_i128(-1_235, 3, 2).expect("ok"), -123);
    }

    #[test]
    fn widening_the_scale_is_exact() {
        assert_eq!(rescale_i128(123, 2, 5).expect("ok"), 123_000);
        assert_eq!(rescale_i128(-123, 2, 5).expect("ok"), -123_000);
        assert_eq!(rescale_i128(123, 2, 2).expect("ok"), 123);
    }

    #[test]
    fn a_plain_number_keeps_every_declared_digit() {
        // The string path exists precisely so this does not go through f64.
        assert_eq!(
            parse_number_to_decimal(&number("123.456"), 3).expect("ok"),
            123_456
        );
        assert_eq!(parse_number_to_decimal(&number("0"), 4).expect("ok"), 0);
        assert_eq!(
            parse_number_to_decimal(&number("-0.5"), 2).expect("ok"),
            -50
        );
    }

    /// `Decimal128` supports scales `0..=38`; anything else is a configuration
    /// error and must be refused rather than producing a scaled value the
    /// column cannot store.
    #[test]
    fn a_target_scale_outside_the_decimal128_range_is_rejected() {
        for scale in [-1_i8, 39, i8::MAX, i8::MIN] {
            assert!(
                matches!(
                    convert_json_to_decimal(&json!("AQ=="), 38, scale),
                    Err(Error::Invalid { .. })
                ),
                "scale {scale} must be rejected"
            );
        }
    }

    #[test]
    fn a_json_null_yields_no_decimal_rather_than_zero() {
        assert_eq!(
            convert_json_to_decimal(&Json::Null, 38, 2).expect("null is accepted"),
            None
        );
    }

    #[test]
    fn a_non_decimal_json_type_is_rejected_by_name() {
        assert!(matches!(
            convert_json_to_decimal(&json!(true), 38, 2),
            Err(Error::UnsupportedType {
                actual_type: "boolean"
            })
        ));
        assert!(matches!(
            convert_json_to_decimal(&json!([1, 2]), 38, 2),
            Err(Error::UnsupportedType {
                actual_type: "array"
            })
        ));
    }

    #[test]
    fn a_decimal_object_missing_a_required_field_is_rejected() {
        assert!(matches!(
            convert_json_to_decimal(&json!({"value": "AQ=="}), 38, 2),
            Err(Error::MissingScale)
        ));
        assert!(matches!(
            convert_json_to_decimal(&json!({"scale": 2}), 38, 2),
            Err(Error::MissingValue)
        ));
        assert!(matches!(
            convert_json_to_decimal(&json!({"scale": "two", "value": "AQ=="}), 38, 2),
            Err(Error::NonIntegerScale)
        ));
    }

    #[test]
    fn a_zero_valued_decimal_object_round_trips() {
        assert_eq!(
            convert_json_to_decimal(&json!({"scale": 4, "value": b64(&[0])}), 38, 2).expect("ok"),
            Some(0)
        );
    }
}
