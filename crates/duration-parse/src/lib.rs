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
        // We parse negative but return zero-clamped since std::time::Duration can't be negative.
        // Match fundu behavior: negative durations without allow_negative return error.
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
        // Parse numeric part
        let (number, rest) = parse_number(remaining)?;
        // Parse unit suffix
        let (unit_nanos, rest) = parse_time_unit(rest);

        let nanos = multiply_to_nanos(number, unit_nanos)?;
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
    /// The fractional part numerator (e.g., for "1.5", frac_value=5, frac_digits=1).
    frac_value: u64,
}

/// Parse a number from the start of the string, returning (parsed_number, remaining).
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
        frac_digits = (pos - frac_start) as u32;
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

/// Apply an exponent (e.g., `e2`, `e-3`, `e+1`) to the parsed number.
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

    // Reconstruct the full number as a string, shift decimal point by exp
    // For simplicity, convert to f64 for exponent handling
    let mut full_str = integer.to_string();
    if frac_digits > 0 {
        full_str.push('.');
        // Pad with leading zeros if needed
        let frac_str = frac_value.to_string();
        for _ in 0..frac_digits.saturating_sub(frac_str.len() as u32) {
            full_str.push('0');
        }
        full_str.push_str(&frac_str);
    }

    let base: f64 = full_str
        .parse()
        .map_err(|_| ParseError::invalid_input("Invalid number"))?;
    let multiplied = base * 10f64.powi(exp);

    if multiplied < 0.0 {
        return Err(ParseError::invalid_input(
            "Negative durations are not supported",
        ));
    }
    if multiplied.is_infinite() || multiplied > u64::MAX as f64 {
        return Ok((u64::MAX, 0, 0, &input[pos..]));
    }

    let new_integer = multiplied.trunc() as u64;
    let frac_part = multiplied.fract();
    // Preserve up to 9 decimal digits (nanosecond precision)
    let new_frac_value = (frac_part * 1_000_000_000.0).round() as u64;
    let new_frac_digits = 9;

    Ok((new_integer, new_frac_value, new_frac_digits, &input[pos..]))
}

/// Parse a time unit suffix, returning (nanoseconds_per_unit, remaining_input).
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
fn multiply_to_nanos(number: ParsedNumber, unit_nanos: u64) -> Result<u128, ParseError> {
    let integer_nanos = u128::from(number.integer) * u128::from(unit_nanos);

    let frac_nanos = if number.frac_digits > 0 && number.frac_value > 0 {
        // frac_value / 10^frac_digits * unit_nanos
        let denominator = 10u128.pow(number.frac_digits);
        (u128::from(number.frac_value) * u128::from(unit_nanos)) / denominator
    } else {
        0
    };

    Ok(integer_nanos + frac_nanos)
}

/// Convert nanoseconds to Duration, saturating at Duration::MAX.
fn nanos_to_duration(nanos: u128) -> Duration {
    let secs = nanos / 1_000_000_000;
    let subsec_nanos = (nanos % 1_000_000_000) as u32;

    if secs > u64::MAX as u128 {
        Duration::MAX
    } else {
        Duration::new(secs as u64, subsec_nanos)
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
            Duration::from_secs(604800)
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
        assert!(parse_duration("-10s").is_err());
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
    fn test_parse_whitespace_trimming() {
        assert_eq!(
            parse_duration("  10s  ").expect("trimmed"),
            Duration::from_secs(10)
        );
    }

    #[test]
    fn test_parse_errors() {
        assert!(parse_duration("").is_err());
        assert!(parse_duration("abc").is_err());
        assert!(parse_duration("-5s").is_err());
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
            format_duration(Duration::from_secs(86400 + 3600 + 60 + 1)),
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
        assert_eq!(format_duration(Duration::from_secs(604800)), "1w");
        assert_eq!(
            format_duration(Duration::from_secs(604800 + 86400)),
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
}
