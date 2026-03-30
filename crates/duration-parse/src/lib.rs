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

//! Human-readable duration parsing and formatting.
//!
//! Parses strings like `"10s"`, `"5m"`, `"1h30m"`, `"2.5d"` into [`std::time::Duration`].
//! Formats [`std::time::Duration`] into human-readable strings like `"1h 30m 45s"`.
//!
//! # Supported Time Units
//!
//! | Unit | Suffix |
//! |------|--------|
//! | Nanosecond | `ns` |
//! | Microsecond | `us`, `Ms` |
//! | Millisecond | `ms` |
//! | Second | `s` (default) |
//! | Minute | `m` |
//! | Hour | `h` |
//! | Day | `d` |
//! | Week | `w` |
//!
//! # Examples
//!
//! ```
//! use std::time::Duration;
//! use duration_parse::{parse_duration, format_duration};
//!
//! let d = parse_duration("1h30m").unwrap();
//! assert_eq!(d, Duration::from_secs(5400));
//!
//! let s = format_duration(Duration::from_secs(3661));
//! assert_eq!(s, "1h 1m 1s");
//! ```

use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use snafu::Snafu;

/// Error type for duration parsing.
#[derive(Debug, Clone, PartialEq, Eq, Snafu)]
pub enum ParseError {
    /// The input string is empty or otherwise invalid.
    #[snafu(display("{message}"))]
    InvalidInput { message: String },

    /// A syntax error at a specific byte position.
    #[snafu(display("at position {position}: {message}"))]
    Syntax { position: usize, message: String },
}

impl ParseError {
    /// Creates an `InvalidInput` error with the given message.
    #[must_use]
    pub fn invalid_input(message: impl Into<String>) -> Self {
        Self::InvalidInput {
            message: message.into(),
        }
    }
}

/// Parse a human-readable duration string into a [`Duration`].
///
/// Accepts strings like `"10s"`, `"5m"`, `"1h30m"`, `"2.5d"`, `"500ms"`.
/// If no time unit is specified, seconds are assumed.
///
/// # Errors
///
/// Returns a [`ParseError`] if the input cannot be parsed as a valid duration.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use duration_parse::parse_duration;
///
/// assert_eq!(parse_duration("10s").unwrap(), Duration::from_secs(10));
/// assert_eq!(parse_duration("5m").unwrap(), Duration::from_secs(300));
/// assert_eq!(parse_duration("1.5h").unwrap(), Duration::from_secs(5400));
/// assert_eq!(parse_duration("500ms").unwrap(), Duration::from_millis(500));
/// assert_eq!(parse_duration("42").unwrap(), Duration::from_secs(42));
/// ```
pub fn parse_duration(input: &str) -> Result<Duration, ParseError> {
    let input = input.trim();
    if input.is_empty() {
        return Err(ParseError::invalid_input("Empty input"));
    }

    // Handle sign prefix
    let (input, _positive) = if let Some(rest) = input.strip_prefix('+') {
        (rest, true)
    } else if input.strip_prefix('-').is_some() {
        // Negative durations are rejected since std::time::Duration can't represent them.
        return Err(ParseError::invalid_input(
            "Negative durations are not supported",
        ));
    } else {
        (input, true)
    };

    if input.is_empty() {
        return Err(ParseError::invalid_input("Missing number after sign"));
    }

    // Handle infinity
    if input.eq_ignore_ascii_case("inf") || input.eq_ignore_ascii_case("infinity") {
        return Ok(Duration::MAX);
    }

    // Try to parse as a compound duration (e.g., "1h30m10s")
    parse_compound_duration(input)
}

/// Parse a compound duration like "1h30m10s" or a single segment like "10s".
fn parse_compound_duration(input: &str) -> Result<Duration, ParseError> {
    let mut total = Duration::ZERO;
    let mut remaining = input;
    let mut parsed_any = false;

    while !remaining.is_empty() {
        // Skip whitespace between segments (e.g. "1h 30m 10s")
        remaining = remaining.trim_start();
        if remaining.is_empty() {
            break;
        }

        // Parse numeric part
        let (number, rest) = parse_number(remaining)?;
        // Parse unit suffix
        let (unit_nanos, rest) = parse_time_unit(rest);

        let nanos = multiply_to_nanos(number, unit_nanos);
        total = total.saturating_add(nanos_to_duration(nanos));
        parsed_any = true;
        remaining = rest;
    }

    if !parsed_any {
        return Err(ParseError::invalid_input(format!(
            "Invalid duration: '{input}'"
        )));
    }

    Ok(total)
}

/// Represents a parsed number (integer + fractional parts) without floating point.
#[derive(Debug, Clone, Copy)]
struct ParsedNumber {
    /// The integer part of the number.
    integer: u64,
    /// The fractional part as a count of digits after the decimal point.
    frac_digits: u32,
    /// The fractional part numerator (e.g., for "1.5", `frac_value=5`, `frac_digits=1`).
    frac_value: u64,
}

/// Parse a number from the start of the string, returning (`parsed_number`, remaining).
fn parse_number(input: &str) -> Result<(ParsedNumber, &str), ParseError> {
    let bytes = input.as_bytes();
    if bytes.is_empty() {
        return Err(ParseError::invalid_input("Expected a number"));
    }

    let mut pos = 0;

    // Parse integer part
    let int_start = pos;
    while pos < bytes.len() && bytes[pos].is_ascii_digit() {
        pos += 1;
    }
    let int_end = pos;

    // Parse optional fractional part
    let mut frac_digits: u32 = 0;
    let mut frac_value: u64 = 0;
    if pos < bytes.len() && bytes[pos] == b'.' {
        pos += 1; // skip '.'
        let frac_start = pos;
        while pos < bytes.len() && bytes[pos].is_ascii_digit() {
            pos += 1;
        }
        frac_digits = u32::try_from(pos - frac_start).unwrap_or(u32::MAX);
        if frac_digits > 0 {
            frac_value = input[frac_start..pos]
                .parse::<u64>()
                .map_err(|_| ParseError::Syntax {
                    position: frac_start,
                    message: "Invalid fractional part".to_string(),
                })?;
        }
    }

    // Ensure we consumed at least some digits
    if int_end == int_start && frac_digits == 0 {
        return Err(ParseError::Syntax {
            position: 0,
            message: format!(
                "Expected a number, found '{}'",
                &input[..1.min(input.len())]
            ),
        });
    }

    let integer = if int_end > int_start {
        input[int_start..int_end]
            .parse::<u64>()
            .map_err(|_| ParseError::invalid_input("Number too large"))?
    } else {
        0
    };

    // Handle optional exponent
    let remaining = &input[pos..];
    let (integer, frac_value, frac_digits, remaining) =
        if remaining.starts_with('e') || remaining.starts_with('E') {
            let (adjusted_int, adjusted_frac_val, adjusted_frac_digits, rest) =
                apply_exponent(integer, frac_value, frac_digits, &remaining[1..])?;
            (adjusted_int, adjusted_frac_val, adjusted_frac_digits, rest)
        } else {
            (integer, frac_value, frac_digits, remaining)
        };

    Ok((
        ParsedNumber {
            integer,
            frac_digits,
            frac_value,
        },
        remaining,
    ))
}

/// Apply an exponent (e.g., `e2`, `e-3`, `e+1`) to the parsed number using integer arithmetic.
///
/// Shifts the decimal point by the exponent value without converting to floating point,
/// preserving exact precision for all representable values.
fn apply_exponent(
    integer: u64,
    frac_value: u64,
    frac_digits: u32,
    input: &str,
) -> Result<(u64, u64, u32, &str), ParseError> {
    let bytes = input.as_bytes();
    let mut pos = 0;

    // Parse optional sign
    let exp_negative = if pos < bytes.len() && bytes[pos] == b'-' {
        pos += 1;
        true
    } else if pos < bytes.len() && bytes[pos] == b'+' {
        pos += 1;
        false
    } else {
        false
    };

    // Parse exponent digits
    let exp_start = pos;
    while pos < bytes.len() && bytes[pos].is_ascii_digit() {
        pos += 1;
    }

    if pos == exp_start {
        return Err(ParseError::invalid_input(
            "Expected digits after exponent 'e'",
        ));
    }

    let exp_val: i32 = input[exp_start..pos]
        .parse::<i32>()
        .map_err(|_| ParseError::invalid_input("Exponent too large"))?;
    let exp = if exp_negative { -exp_val } else { exp_val };

    // Cap exponent magnitude to prevent excessive memory allocation from
    // building large intermediate strings (e.g., "1e-1000000000" would try
    // to allocate a multi-GB string of leading zeros).
    if exp.abs() > MAX_EXPONENT_MAGNITUDE {
        return Err(ParseError::invalid_input(format!(
            "Exponent magnitude {} exceeds maximum ({MAX_EXPONENT_MAGNITUDE})",
            exp.abs()
        )));
    }

    // Build the full digit string: integer digits + fractional digits
    // e.g., integer=1, frac_value=5, frac_digits=1 -> digits="15", decimal_pos=1
    // The decimal point is `frac_digits` positions from the right end of the digit string.
    let int_str = integer.to_string();
    let frac_str = if frac_digits > 0 {
        let raw = frac_value.to_string();
        let pad = frac_digits.saturating_sub(u32::try_from(raw.len()).unwrap_or(u32::MAX));
        "0".repeat(pad as usize) + &raw
    } else {
        String::new()
    };
    let all_digits = format!("{int_str}{frac_str}");

    // Current decimal position from the right = frac_digits
    // After applying exponent, new decimal position from the right = frac_digits - exp
    // (positive exp shifts decimal right, reducing frac digits)
    let new_frac_digits_i64 = i64::from(frac_digits) - i64::from(exp);

    if new_frac_digits_i64 <= 0 {
        // All digits become integer part, possibly with trailing zeros
        let trailing_zeros = new_frac_digits_i64.unsigned_abs();
        let new_integer = all_digits
            .parse::<u128>()
            .map_err(|_| ParseError::invalid_input("Number too large"))?;
        let multiplier = 10u128.checked_pow(u32::try_from(trailing_zeros).unwrap_or(u32::MAX));
        match multiplier {
            Some(m) => match new_integer.checked_mul(m) {
                Some(result) if result <= u128::from(u64::MAX) => {
                    #[expect(clippy::cast_possible_truncation)]
                    let result_u64 = result as u64;
                    Ok((result_u64, 0, 0, &input[pos..]))
                }
                _ => Ok((u64::MAX, 0, 0, &input[pos..])),
            },
            None => Ok((u64::MAX, 0, 0, &input[pos..])),
        }
    } else {
        // Split all_digits into integer part and fractional part
        #[expect(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let new_frac_count = new_frac_digits_i64 as usize;
        let total_len = all_digits.len();

        if new_frac_count >= total_len {
            // All digits are fractional (e.g., 1e-3 -> 0.001)
            let leading_zeros = new_frac_count - total_len;
            let padded_frac = "0".repeat(leading_zeros) + &all_digits;
            let new_frac_val = padded_frac
                .parse::<u64>()
                .map_err(|_| ParseError::invalid_input("Fractional part too large"))?;
            #[expect(clippy::cast_possible_truncation)]
            let new_frac_dig = padded_frac.len() as u32;
            Ok((0, new_frac_val, new_frac_dig, &input[pos..]))
        } else {
            let split_at = total_len - new_frac_count;
            let int_part = &all_digits[..split_at];
            let frac_part = &all_digits[split_at..];
            let new_integer = int_part
                .parse::<u64>()
                .map_err(|_| ParseError::invalid_input("Number too large"))?;
            let new_frac_val = if frac_part.is_empty() {
                0
            } else {
                frac_part
                    .parse::<u64>()
                    .map_err(|_| ParseError::invalid_input("Fractional part too large"))?
            };
            #[expect(clippy::cast_possible_truncation)]
            let new_frac_dig = frac_part.len() as u32;
            Ok((new_integer, new_frac_val, new_frac_dig, &input[pos..]))
        }
    }
}

/// Parse a time unit suffix, returning (`nanoseconds_per_unit`, `remaining_input`).
/// If no unit is found, defaults to seconds.
fn parse_time_unit(input: &str) -> (u64, &str) {
    // Order matters: try longest prefixes first to avoid ambiguity (e.g., "ms" before "m")
    let units: &[(&str, u64)] = &[
        ("ns", NANOS_PER_NANOSECOND),
        ("us", NANOS_PER_MICROSECOND),
        ("Ms", NANOS_PER_MICROSECOND), // fundu compatibility
        ("ms", NANOS_PER_MILLISECOND),
        ("s", NANOS_PER_SECOND),
        ("m", NANOS_PER_MINUTE),
        ("h", NANOS_PER_HOUR),
        ("d", NANOS_PER_DAY),
        ("w", NANOS_PER_WEEK),
    ];

    for &(suffix, nanos) in units {
        if let Some(rest) = input.strip_prefix(suffix) {
            return (nanos, rest);
        }
    }

    // Check for unrecognized unit
    if !input.is_empty() && input.as_bytes()[0].is_ascii_alphabetic() {
        // This will be caught as a parse error downstream, but for now
        // we can't return an error from this function. We default to seconds
        // which will leave the alphabetic chars and cause a subsequent parse error.
        // Actually, let's just default to seconds and let remaining chars cause error.
        return (NANOS_PER_SECOND, input);
    }

    // No unit specified, default to seconds
    (NANOS_PER_SECOND, input)
}

/// Multiply a parsed number by nanoseconds-per-unit without floating point loss.
fn multiply_to_nanos(number: ParsedNumber, unit_nanos: u64) -> u128 {
    let integer_nanos = u128::from(number.integer) * u128::from(unit_nanos);

    let frac_nanos = if number.frac_digits > 0 && number.frac_value > 0 {
        // frac_value / 10^frac_digits * unit_nanos
        // Use checked arithmetic to avoid overflow and division-by-zero.
        let denominator = match 10u128.checked_pow(number.frac_digits) {
            Some(d) if d != 0 => d,
            _ => return integer_nanos,
        };

        u128::from(number.frac_value)
            .checked_mul(u128::from(unit_nanos))
            .and_then(|numerator| numerator.checked_div(denominator))
            .unwrap_or_default()
    } else {
        0
    };

    integer_nanos + frac_nanos
}

/// Convert nanoseconds to Duration, saturating at `Duration::MAX`.
fn nanos_to_duration(nanos: u128) -> Duration {
    let secs = nanos / 1_000_000_000;
    let subsec_nanos = (nanos % 1_000_000_000) as u32;

    if let Ok(secs) = u64::try_from(secs) {
        Duration::new(secs, subsec_nanos)
    } else {
        Duration::MAX
    }
}

// Nanoseconds per time unit
const NANOS_PER_NANOSECOND: u64 = 1;
const NANOS_PER_MICROSECOND: u64 = 1_000;
const NANOS_PER_MILLISECOND: u64 = 1_000_000;
const NANOS_PER_SECOND: u64 = 1_000_000_000;
const NANOS_PER_MINUTE: u64 = 60 * NANOS_PER_SECOND;
const NANOS_PER_HOUR: u64 = 60 * NANOS_PER_MINUTE;
const NANOS_PER_DAY: u64 = 24 * NANOS_PER_HOUR;
const NANOS_PER_WEEK: u64 = 7 * NANOS_PER_DAY;

/// Maximum allowed exponent magnitude for scientific notation in duration strings.
/// This prevents excessive memory allocation from building large intermediate strings.
const MAX_EXPONENT_MAGNITUDE: i32 = 20;

/// Format a [`Duration`] into a human-readable string.
///
/// Produces output like `"1h 30m 45s"`, `"500ms"`, `"2d 3h"`.
/// Components with zero value are omitted. Sub-second precision is shown
/// down to nanoseconds when present.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use duration_parse::format_duration;
///
/// assert_eq!(format_duration(Duration::from_secs(3661)), "1h 1m 1s");
/// assert_eq!(format_duration(Duration::from_millis(500)), "500ms");
/// assert_eq!(format_duration(Duration::ZERO), "0s");
/// ```
#[must_use]
pub fn format_duration(duration: Duration) -> String {
    if duration.is_zero() {
        return "0s".to_string();
    }

    let mut secs = duration.as_secs();
    let nanos = duration.subsec_nanos();

    let mut parts = Vec::new();

    let weeks = secs / (7 * 24 * 3600);
    if weeks > 0 {
        parts.push(format!("{weeks}w"));
        secs %= 7 * 24 * 3600;
    }

    let days = secs / (24 * 3600);
    if days > 0 {
        parts.push(format!("{days}d"));
        secs %= 24 * 3600;
    }

    let hours = secs / 3600;
    if hours > 0 {
        parts.push(format!("{hours}h"));
        secs %= 3600;
    }

    let minutes = secs / 60;
    if minutes > 0 {
        parts.push(format!("{minutes}m"));
        secs %= 60;
    }

    if secs > 0 {
        parts.push(format!("{secs}s"));
    }

    // Sub-second components
    let mut remaining_nanos = nanos;

    let millis = remaining_nanos / 1_000_000;
    if millis > 0 {
        parts.push(format!("{millis}ms"));
        remaining_nanos %= 1_000_000;
    }

    let micros = remaining_nanos / 1_000;
    if micros > 0 {
        parts.push(format!("{micros}us"));
        remaining_nanos %= 1_000;
    }

    if remaining_nanos > 0 {
        parts.push(format!("{remaining_nanos}ns"));
    }

    parts.join(" ")
}

/// A newtype wrapper around [`Duration`] that implements [`FromStr`].
///
/// This is useful for command-line argument parsing with crates like `clap`.
///
/// # Examples
///
/// ```
/// use duration_parse::DurationArg;
///
/// let d: DurationArg = "30s".parse().unwrap();
/// let duration: std::time::Duration = d.into();
/// assert_eq!(duration, std::time::Duration::from_secs(30));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DurationArg(pub Duration);

impl FromStr for DurationArg {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        parse_duration(s).map(DurationArg)
    }
}

impl From<DurationArg> for Duration {
    fn from(d: DurationArg) -> Self {
        d.0
    }
}

impl fmt::Display for DurationArg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", format_duration(self.0))
    }
}

impl std::ops::Deref for DurationArg {
    type Target = Duration;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_basic_units() {
        assert_eq!(parse_duration("10s").expect("10s"), Duration::from_secs(10));
        assert_eq!(parse_duration("5m").expect("5m"), Duration::from_secs(300));
        assert_eq!(parse_duration("2h").expect("2h"), Duration::from_secs(7200));
        assert_eq!(
            parse_duration("1d").expect("1d"),
            Duration::from_secs(86400)
        );
        assert_eq!(
            parse_duration("1w").expect("1w"),
            Duration::from_secs(604_800)
        );
        assert_eq!(
            parse_duration("500ms").expect("500ms"),
            Duration::from_millis(500)
        );
        assert_eq!(
            parse_duration("100us").expect("100us"),
            Duration::from_micros(100)
        );
        assert_eq!(
            parse_duration("100Ms").expect("100Ms"),
            Duration::from_micros(100)
        );
        assert_eq!(
            parse_duration("50ns").expect("50ns"),
            Duration::from_nanos(50)
        );
    }

    #[test]
    fn test_parse_bare_number() {
        assert_eq!(parse_duration("42").expect("42"), Duration::from_secs(42));
        assert_eq!(parse_duration("0").expect("0"), Duration::ZERO);
    }

    #[test]
    fn test_parse_decimal() {
        assert_eq!(
            parse_duration("1.5s").expect("1.5s"),
            Duration::from_millis(1500)
        );
        assert_eq!(
            parse_duration("0.5m").expect("0.5m"),
            Duration::from_secs(30)
        );
        assert_eq!(
            parse_duration(".5s").expect(".5s"),
            Duration::from_millis(500)
        );
        assert_eq!(
            parse_duration("2.5h").expect("2.5h"),
            Duration::from_secs(9000)
        );
    }

    #[test]
    fn test_parse_compound() {
        assert_eq!(
            parse_duration("1h30m").expect("1h30m"),
            Duration::from_secs(5400)
        );
        assert_eq!(
            parse_duration("1m30s").expect("1m30s"),
            Duration::from_secs(90)
        );
        assert_eq!(
            parse_duration("1h2m3s").expect("1h2m3s"),
            Duration::from_secs(3723)
        );
    }

    #[test]
    fn test_parse_with_sign() {
        assert_eq!(
            parse_duration("+10s").expect("+10s"),
            Duration::from_secs(10)
        );
        parse_duration("-10s").expect_err("negative duration should fail");
    }

    #[test]
    fn test_parse_infinity() {
        assert_eq!(parse_duration("inf").expect("inf"), Duration::MAX);
        assert_eq!(parse_duration("infinity").expect("infinity"), Duration::MAX);
        assert_eq!(parse_duration("+inf").expect("+inf"), Duration::MAX);
    }

    #[test]
    fn test_parse_scientific_notation() {
        assert_eq!(
            parse_duration("1e2s").expect("1e2s"),
            Duration::from_secs(100)
        );
        assert_eq!(
            parse_duration("1.5e1s").expect("1.5e1s"),
            Duration::from_secs(15)
        );
    }

    #[test]
    fn test_parse_scientific_notation_extreme_exponent_rejected() {
        // Extremely large exponents should be rejected to prevent DoS via
        // excessive memory allocation in intermediate string construction.
        parse_duration("1e100s").expect_err("extreme positive exponent should fail");
        parse_duration("1e-100s").expect_err("extreme negative exponent should fail");
        parse_duration("1e1000000000s").expect_err("huge exponent should fail");
    }

    #[test]
    fn test_parse_whitespace_trimming() {
        assert_eq!(
            parse_duration("  10s  ").expect("trimmed"),
            Duration::from_secs(10)
        );
    }

    #[test]
    fn test_parse_errors() {
        parse_duration("").expect_err("empty string should fail");
        parse_duration("abc").expect_err("non-numeric input should fail");
        parse_duration("-5s").expect_err("negative duration should fail");
    }

    #[test]
    fn test_format_zero() {
        assert_eq!(format_duration(Duration::ZERO), "0s");
    }

    #[test]
    fn test_format_seconds() {
        assert_eq!(format_duration(Duration::from_secs(1)), "1s");
        assert_eq!(format_duration(Duration::from_secs(59)), "59s");
    }

    #[test]
    fn test_format_compound() {
        assert_eq!(format_duration(Duration::from_secs(3661)), "1h 1m 1s");
        assert_eq!(format_duration(Duration::from_secs(90)), "1m 30s");
        assert_eq!(
            format_duration(Duration::from_secs(86_400 + 3600 + 60 + 1)),
            "1d 1h 1m 1s"
        );
    }

    #[test]
    fn test_format_subsecond() {
        assert_eq!(format_duration(Duration::from_millis(500)), "500ms");
        assert_eq!(format_duration(Duration::from_micros(100)), "100us");
        assert_eq!(format_duration(Duration::from_nanos(50)), "50ns");
        assert_eq!(format_duration(Duration::new(1, 500_000_000)), "1s 500ms");
    }

    #[test]
    fn test_format_weeks() {
        assert_eq!(format_duration(Duration::from_secs(604_800)), "1w");
        assert_eq!(
            format_duration(Duration::from_secs(604_800 + 86_400)),
            "1w 1d"
        );
    }

    #[test]
    fn test_duration_arg_fromstr() {
        let d: DurationArg = "30s".parse().expect("30s");
        assert_eq!(Duration::from(d), Duration::from_secs(30));
    }

    #[test]
    fn test_roundtrip_common_values() {
        // Values commonly used in Spice configs
        for input in &["10s", "5m", "1h", "30s", "500ms", "2d", "1w"] {
            let parsed =
                parse_duration(input).unwrap_or_else(|e| panic!("Failed to parse '{input}': {e}"));
            assert!(parsed > Duration::ZERO, "'{input}' should be positive");
        }
    }

    #[test]
    fn test_parse_with_whitespace_between_segments() {
        // format_duration produces space-separated segments like "1h 1m 1s"
        assert_eq!(
            parse_duration("1h 30m").expect("1h 30m"),
            Duration::from_secs(5400),
        );
        assert_eq!(
            parse_duration("1h 1m 1s").expect("1h 1m 1s"),
            Duration::from_secs(3661),
        );
        assert_eq!(
            parse_duration("1d  2h   3m").expect("multi-space"),
            Duration::from_secs(86_400 + 7200 + 180),
        );
    }

    #[test]
    fn test_format_then_parse_roundtrip() {
        // Ensure format_duration output is parseable back to the same duration
        let durations = vec![
            Duration::from_secs(3661),
            Duration::from_secs(86_400 + 3600 + 60 + 1),
            Duration::from_millis(1500),
            Duration::from_secs(604_800 + 86_400),
        ];
        for d in durations {
            let formatted = format_duration(d);
            let reparsed = parse_duration(&formatted)
                .unwrap_or_else(|e| panic!("Failed to parse formatted '{formatted}': {e}"));
            assert_eq!(d, reparsed, "Roundtrip failed for '{formatted}'");
        }
    }

    #[test]
    fn test_extreme_fractional_precision_does_not_panic() {
        // A very large number of fractional digits should not panic due to
        // 10u128.pow() overflow. The parser gracefully falls back to the integer part.
        let huge_frac = format!("1.{}s", "0".repeat(200));
        let result = parse_duration(&huge_frac);
        // Should succeed (fractional contribution is effectively zero)
        assert_eq!(result.expect("extreme precision"), Duration::from_secs(1));
    }
}
