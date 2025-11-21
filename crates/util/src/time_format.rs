/*
Copyright 2025 The Spice.ai OSS Authors

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
use chrono::{DateTime, FixedOffset, NaiveDateTime};

#[derive(Debug)]
pub enum ParsedDateTime {
    Naive(NaiveDateTime),
    WithOffset(DateTime<FixedOffset>),
}

#[must_use]
pub fn is_valid_format(value: &str) -> bool {
    convert_go_format_to_rust(value).is_some()
}

/// Format a timestamp using go-style formatting.
#[must_use]
pub fn format_datetime(dt: DateTime<FixedOffset>, go_format: &str) -> Option<String> {
    let rust_format = convert_go_format_to_rust(go_format)?;

    let is_utc = dt.offset().local_minus_utc() == 0;
    let formatted = dt.format(&rust_format).to_string();

    // Post-process: Replace +00:00 or +0000 with Z if format uses Z07:00
    if is_utc && go_format.contains("Z07:00") {
        return Some(formatted.replace("+00:00", "Z"));
    }
    if is_utc && go_format.contains("Z0700") {
        return Some(formatted.replace("+0000", "Z"));
    }

    Some(formatted)
}

#[must_use]
pub fn parse_datetime(input: &str, go_format: &str) -> Option<ParsedDateTime> {
    let rust_format = convert_go_format_to_rust(go_format)?;

    // Check if format expects timezone info
    let has_tz =
        go_format.contains("Z07") || go_format.contains("-07") || go_format.contains("MST");

    if has_tz {
        let normalized_input = if go_format.contains("Z07:00") {
            input.replace('Z', "+00:00")
        } else if go_format.contains("Z0700") {
            input.replace('Z', "+0000")
        } else {
            input.to_string()
        };

        DateTime::parse_from_str(&normalized_input, &rust_format)
            .ok()
            .map(ParsedDateTime::WithOffset)
    } else {
        NaiveDateTime::parse_from_str(input, &rust_format)
            .ok()
            .map(ParsedDateTime::Naive)
    }
}

#[allow(clippy::too_many_lines)]
fn convert_go_format_to_rust(go_format: &str) -> Option<String> {
    if go_format.is_empty() {
        return None;
    }

    let mut result = String::new();
    let mut chars = go_format.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch.is_ascii_digit() {
            let mut num_str = ch.to_string();
            while let Some(&next_ch) = chars.peek() {
                if next_ch.is_ascii_digit() {
                    num_str.push(chars.next().unwrap_or_default());
                } else {
                    break;
                }
            }

            match num_str.as_str() {
                "2006" => result.push_str("%Y"),
                "06" => result.push_str("%y"),
                "01" => result.push_str("%m"),
                "1" => result.push_str("%-m"),
                "02" => result.push_str("%d"),
                "2" => result.push_str("%-d"),
                "15" => result.push_str("%H"),
                "3" => result.push_str("%-I"),
                "03" => result.push_str("%I"),
                "04" => result.push_str("%M"),
                "4" => result.push_str("%-M"),
                "05" => result.push_str("%S"),
                "5" => result.push_str("%-S"),
                "0700" => result.push_str("%z"),
                "07" => {
                    let next_chars: String = chars.clone().take(3).collect();
                    if next_chars == ":00" {
                        result.push_str("%:z");
                        for _ in 0..3 {
                            chars.next();
                        }
                    } else {
                        return None; // Unexpected: "07" without ":00"
                    }
                }
                _ => return None, // Unexpected numeric sequence
            }
        } else if ch.is_ascii_alphabetic() {
            // Special case: Z followed by timezone offset pattern
            if ch == 'Z' {
                let next_chars: String = chars.clone().take(5).collect();
                if next_chars == "07:00" {
                    result.push_str("%:z");
                    for _ in 0..5 {
                        chars.next();
                    }
                    continue;
                } else if next_chars.starts_with("0700") {
                    result.push_str("%z");
                    for _ in 0..4 {
                        chars.next();
                    }
                    continue;
                }
                // Otherwise, treat Z as a literal (UTC indicator in ISO-8601)
                result.push('Z');
                continue;
            }

            // Collect the full alphabetic sequence first
            let mut word = ch.to_string();
            while let Some(&next_ch) = chars.peek() {
                if next_ch.is_ascii_alphabetic() {
                    word.push(chars.next().unwrap_or_default());
                } else {
                    break;
                }
            }

            match word.as_str() {
                "January" => result.push_str("%B"),
                "Jan" => result.push_str("%b"),
                "Monday" => result.push_str("%A"),
                "Mon" => result.push_str("%a"),
                "MST" => result.push_str("%Z"),
                "PM" | "pm" => result.push_str("%p"),
                "AM" | "am" => result.push_str("%P"), // Note: check if this is correct for your use case
                "T" | "Z" => result.push_str(&word),  // Common ISO-8601 literals
                _ => return None,                     // Unexpected alphabetic sequence
            }
        } else {
            match ch {
                '.' => {
                    let next_chars: String = chars.clone().take(9).collect();

                    if next_chars.starts_with("000000000") || next_chars.starts_with("999999999") {
                        result.push_str(".%9f");
                        for _ in 0..9 {
                            chars.next();
                        }
                    } else if next_chars.starts_with("000000") || next_chars.starts_with("999999") {
                        result.push_str(".%6f");
                        for _ in 0..6 {
                            chars.next();
                        }
                    } else if next_chars.starts_with("000") || next_chars.starts_with("999") {
                        result.push_str(".%3f");
                        for _ in 0..3 {
                            chars.next();
                        }
                    } else {
                        result.push(ch); // Literal dot
                    }
                }
                'Z' => {
                    let next_chars: String = chars.clone().take(5).collect();
                    if next_chars == "07:00" {
                        result.push_str("%:z");
                        for _ in 0..5 {
                            chars.next();
                        }
                    } else if next_chars.starts_with("0700") {
                        result.push_str("%z");
                        for _ in 0..4 {
                            chars.next();
                        }
                    } else {
                        result.push(ch); // Literal 'Z' (e.g., UTC indicator)
                    }
                }
                '+' | '-' => {
                    let next_chars: String = chars.clone().take(5).collect();

                    if next_chars.len() >= 5 && next_chars.starts_with("07:00") {
                        result.push_str("%:z");
                        for _ in 0..5 {
                            chars.next();
                        }
                    } else if next_chars.starts_with("0700") {
                        result.push_str("%z");
                        for _ in 0..4 {
                            chars.next();
                        }
                    } else {
                        // Allow as literal separator (e.g., in date ranges)
                        result.push(ch);
                    }
                }
                // Allowed literal characters (separators, whitespace, punctuation)
                ' ' | ':' | '/' | ',' | '\'' | '"' | '(' | ')' | '[' | ']' | '\t' | '\n' => {
                    result.push(ch);
                }
                _ => return None, // Unexpected character
            }
        }
    }

    Some(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Datelike, FixedOffset, TimeZone, Timelike, Utc};

    #[allow(clippy::too_many_arguments)]
    fn make_utc(
        year: i32,
        month: u32,
        day: u32,
        hour: u32,
        min: u32,
        sec: u32,
        nano: u32,
    ) -> DateTime<FixedOffset> {
        Utc.with_ymd_and_hms(year, month, day, hour, min, sec)
            .unwrap()
            .with_nanosecond(nano)
            .expect("Invalid datetime")
            .fixed_offset()
    }

    /// Helper to convert a UTC datetime to a different timezone offset
    fn utc_to_offset(dt: DateTime<FixedOffset>, offset_secs: i32) -> DateTime<FixedOffset> {
        let offset = FixedOffset::east_opt(offset_secs).expect("FixedOffset");
        dt.with_timezone(&offset)
    }

    #[test]
    fn test_millisecond_precision() {
        let dt = make_utc(2024, 11, 19, 10, 30, 45, 123_000_000); // .123 seconds

        assert_eq!(
            format_datetime(dt, "2006-01-02T15:04:05.000"),
            Some("2024-11-19T10:30:45.123".to_string())
        );

        assert_eq!(
            format_datetime(dt, "15:04:05.000"),
            Some("10:30:45.123".to_string())
        );
    }

    #[test]
    fn test_microsecond_precision() {
        let dt = make_utc(2024, 11, 19, 10, 30, 45, 123_456_000); // .123456 seconds

        assert_eq!(
            format_datetime(dt, "2006-01-02T15:04:05.000000"),
            Some("2024-11-19T10:30:45.123456".to_string())
        );

        assert_eq!(
            format_datetime(dt, "15:04:05.000000"),
            Some("10:30:45.123456".to_string())
        );
    }

    #[test]
    fn test_nanosecond_precision() {
        let dt = make_utc(2024, 11, 19, 10, 30, 45, 123_456_789); // .123456789 seconds

        assert_eq!(
            format_datetime(dt, "2006-01-02T15:04:05.000000000"),
            Some("2024-11-19T10:30:45.123456789".to_string())
        );

        assert_eq!(
            format_datetime(dt, "15:04:05.000000000"),
            Some("10:30:45.123456789".to_string())
        );
    }

    #[test]
    fn test_subsecond_with_timezone() {
        // UTC time: 2024-11-19T10:30:45.123456789Z
        let utc_dt = make_utc(2024, 11, 19, 10, 30, 45, 123_456_789);

        // Convert to +05:30 (19800 seconds)
        let dt_plus_530 = utc_to_offset(utc_dt, 5 * 3600 + 30 * 60);
        assert_eq!(
            format_datetime(dt_plus_530, "2006-01-02T15:04:05.000Z07:00"),
            Some("2024-11-19T16:00:45.123+05:30".to_string())
        );

        // Convert to -08:00 (-28800 seconds)
        let dt_minus_8 = utc_to_offset(utc_dt, -8 * 3600);
        assert_eq!(
            format_datetime(dt_minus_8, "2006-01-02T15:04:05.000000-07:00"),
            Some("2024-11-19T02:30:45.123456-08:00".to_string())
        );

        // Keep as UTC (offset 0), should show Z
        assert_eq!(
            format_datetime(utc_dt, "2006-01-02T15:04:05.000000000Z07:00"),
            Some("2024-11-19T10:30:45.123456789Z".to_string())
        );
    }

    #[test]
    fn test_zero_subseconds() {
        let dt = make_utc(2024, 11, 19, 10, 30, 45, 0); // No fractional seconds

        assert_eq!(
            format_datetime(dt, "2006-01-02T15:04:05.000"),
            Some("2024-11-19T10:30:45.000".to_string())
        );

        assert_eq!(
            format_datetime(dt, "2006-01-02T15:04:05.000000"),
            Some("2024-11-19T10:30:45.000000".to_string())
        );

        assert_eq!(
            format_datetime(dt, "2006-01-02T15:04:05.000000000"),
            Some("2024-11-19T10:30:45.000000000".to_string())
        );
    }

    #[test]
    fn test_half_second_subseconds() {
        let dt = make_utc(2024, 11, 19, 10, 30, 45, 500_000_000); // .5 seconds

        assert_eq!(
            format_datetime(dt, "15:04:05.000"),
            Some("10:30:45.500".to_string())
        );

        assert_eq!(
            format_datetime(dt, "15:04:05.000000"),
            Some("10:30:45.500000".to_string())
        );

        assert_eq!(
            format_datetime(dt, "15:04:05.000000000"),
            Some("10:30:45.500000000".to_string())
        );
    }

    #[test]
    fn test_rfc3339_with_subseconds() {
        let utc_dt = make_utc(2024, 11, 19, 10, 30, 45, 123_456_000);

        // UTC should produce Z
        assert_eq!(
            format_datetime(utc_dt, "2006-01-02T15:04:05.000000Z07:00"),
            Some("2024-11-19T10:30:45.123456Z".to_string())
        );

        // Convert to +08:00
        let dt_plus_8 = utc_to_offset(utc_dt, 8 * 3600);
        assert_eq!(
            format_datetime(dt_plus_8, "2006-01-02T15:04:05.000000Z07:00"),
            Some("2024-11-19T18:30:45.123456+08:00".to_string())
        );
    }

    #[test]
    fn test_mixed_precision() {
        let dt = make_utc(2024, 11, 19, 10, 30, 45, 100_000_000); // .1 seconds

        assert_eq!(
            format_datetime(dt, "15:04:05.000"),
            Some("10:30:45.100".to_string())
        );

        assert_eq!(
            format_datetime(dt, "15:04:05.000000"),
            Some("10:30:45.100000".to_string())
        );

        assert_eq!(
            format_datetime(dt, "15:04:05.000000000"),
            Some("10:30:45.100000000".to_string())
        );
    }

    #[test]
    fn test_conversion_with_subseconds() {
        assert_eq!(convert_go_format_to_rust(".000"), Some(".%3f".to_string()));
        assert_eq!(
            convert_go_format_to_rust(".000000"),
            Some(".%6f".to_string())
        );
        assert_eq!(
            convert_go_format_to_rust(".000000000"),
            Some(".%9f".to_string())
        );

        assert_eq!(convert_go_format_to_rust(".999"), Some(".%3f".to_string()));
        assert_eq!(
            convert_go_format_to_rust(".999999"),
            Some(".%6f".to_string())
        );
        assert_eq!(
            convert_go_format_to_rust(".999999999"),
            Some(".%9f".to_string())
        );

        assert_eq!(
            convert_go_format_to_rust("2006-01-02T15:04:05.000000Z07:00"),
            Some("%Y-%m-%dT%H:%M:%S.%6f%:z".to_string())
        );
    }

    #[test]
    fn test_invalid_formats() {
        assert_eq!(convert_go_format_to_rust("foo"), None);
        assert_eq!(convert_go_format_to_rust("2024"), None);
        assert_eq!(convert_go_format_to_rust("2024-11"), None);
        assert_eq!(convert_go_format_to_rust("2024-11-09"), None);
        assert_eq!(convert_go_format_to_rust("2024-11-09"), None);
        assert_eq!(convert_go_format_to_rust("2024-15-09"), None);
        assert_eq!(convert_go_format_to_rust("2006-06-0"), None);
        assert_eq!(convert_go_format_to_rust(""), None);
    }

    #[test]
    fn test_subseconds_with_date_boundaries() {
        let utc_dt = make_utc(2024, 11, 19, 23, 59, 59, 999_999_999);

        assert_eq!(
            format_datetime(utc_dt, "2006-01-02 15:04:05.000000000"),
            Some("2024-11-19 23:59:59.999999999".to_string())
        );

        // Add 2 hours offset, should roll over to next day
        let dt_plus_2 = utc_to_offset(utc_dt, 2 * 3600);
        assert_eq!(
            format_datetime(dt_plus_2, "2006-01-02 15:04:05.000000000"),
            Some("2024-11-20 01:59:59.999999999".to_string()),
        );
    }

    #[test]
    fn test_very_small_nanoseconds() {
        let dt = make_utc(2024, 11, 19, 10, 30, 45, 1); // 1 nanosecond

        assert_eq!(
            format_datetime(dt, "15:04:05.000000000"),
            Some("10:30:45.000000001".to_string()),
        );
    }

    #[test]
    fn test_subseconds_all_formats_combined() {
        let utc_dt = make_utc(2024, 11, 19, 14, 30, 45, 987_654_321);

        // Test complete RFC3339 with nanoseconds (UTC produces Z)
        assert_eq!(
            format_datetime(utc_dt, "2006-01-02T15:04:05.000000000Z07:00"),
            Some("2024-11-19T14:30:45.987654321Z".to_string()),
        );

        // With positive offset (+09:00)
        let dt_plus_9 = utc_to_offset(utc_dt, 9 * 3600);
        assert_eq!(
            format_datetime(dt_plus_9, "2006-01-02T15:04:05.000000-07:00"),
            Some("2024-11-19T23:30:45.987654+09:00".to_string()),
        );
    }

    // Helper to create expected DateTime values
    fn utc_offset() -> FixedOffset {
        FixedOffset::east_opt(0).expect("Expected FixedOffset")
    }

    fn offset_hours(hours: i32) -> FixedOffset {
        FixedOffset::east_opt(hours * 3600).expect("Expected FixedOffset")
    }

    // Helper to unwrap as WithOffset variant
    #[allow(clippy::needless_pass_by_value)]
    fn expect_with_offset(result: Option<ParsedDateTime>) -> DateTime<FixedOffset> {
        match result {
            Some(ParsedDateTime::WithOffset(dt)) => dt,
            Some(ParsedDateTime::Naive(_)) => panic!("Expected WithOffset, got Naive"),
            None => panic!("Expected WithOffset, got None"),
        }
    }

    // Helper to unwrap as Naive variant
    #[allow(clippy::needless_pass_by_value)]
    fn expect_naive(result: Option<ParsedDateTime>) -> NaiveDateTime {
        match result {
            Some(ParsedDateTime::Naive(dt)) => dt,
            Some(ParsedDateTime::WithOffset(_)) => panic!("Expected Naive, got WithOffset"),
            None => panic!("Expected Naive, got None"),
        }
    }

    // ==================== WithOffset tests ====================

    #[test]
    fn test_parse_utc_with_z_colon_format() {
        let result = parse_datetime("2024-01-15T10:30:00Z", "2006-01-02T15:04:05Z07:00");
        let dt = expect_with_offset(result);

        assert_eq!(dt.offset(), &utc_offset());
        assert_eq!(dt.year(), 2024);
        assert_eq!(dt.month(), 1);
        assert_eq!(dt.day(), 15);
        assert_eq!(dt.hour(), 10);
        assert_eq!(dt.minute(), 30);
        assert_eq!(dt.second(), 0);
    }

    #[test]
    fn test_parse_utc() {
        let result = parse_datetime("2023-08-31T12:34:56Z", "2006-01-02T15:04:05Z07:00");
        let dt = expect_with_offset(result);

        assert_eq!(dt.offset(), &utc_offset());
        assert_eq!(dt.year(), 2023);
        assert_eq!(dt.month(), 8);
    }

    #[test]
    fn test_parse_utc_with_z_no_colon_format() {
        let result = parse_datetime("2024-01-15T10:30:00Z", "2006-01-02T15:04:05Z0700");
        let dt = expect_with_offset(result);
        assert_eq!(dt.offset(), &utc_offset());
    }

    #[test]
    fn test_parse_positive_offset_with_z_format() {
        let result = parse_datetime("2024-01-15T10:30:00+05:30", "2006-01-02T15:04:05Z07:00");
        let dt = expect_with_offset(result);
        let expected_offset =
            FixedOffset::east_opt(5 * 3600 + 30 * 60).expect("Expected FixedOffset");
        assert_eq!(dt.offset(), &expected_offset);
    }

    #[test]
    fn test_parse_negative_offset() {
        let result = parse_datetime("2024-01-15T10:30:00-08:00", "2006-01-02T15:04:05Z07:00");
        let dt = expect_with_offset(result);
        assert_eq!(dt.offset(), &offset_hours(-8));
    }

    #[test]
    fn test_parse_explicit_plus_zero_offset() {
        let result = parse_datetime("2024-01-15T10:30:00+00:00", "2006-01-02T15:04:05Z07:00");
        let dt = expect_with_offset(result);
        assert_eq!(dt.offset(), &utc_offset());
    }

    #[test]
    fn test_parse_non_z_format_with_offset() {
        let result = parse_datetime("2024-01-15T10:30:00-05:00", "2006-01-02T15:04:05-07:00");
        let dt = expect_with_offset(result);
        assert_eq!(dt.offset(), &offset_hours(-5));
    }

    // ==================== Naive tests ====================
    #[test]
    fn test_parse_datetime_without_timezone() {
        let result = parse_datetime("2024-01-15 10:30:00", "2006-01-02 15:04:05");
        let dt = expect_naive(result);
        assert_eq!(dt.year(), 2024);
        assert_eq!(dt.month(), 1);
        assert_eq!(dt.day(), 15);
        assert_eq!(dt.hour(), 10);
        assert_eq!(dt.minute(), 30);
        assert_eq!(dt.second(), 0);

        let result = parse_datetime("2024/01/15 10+30+00", "2006/01/02 15+04+05");
        let dt = expect_naive(result);
        assert_eq!(dt.year(), 2024);
        assert_eq!(dt.month(), 1);
        assert_eq!(dt.day(), 15);
        assert_eq!(dt.hour(), 10);
        assert_eq!(dt.minute(), 30);
        assert_eq!(dt.second(), 0);
    }

    // ==================== Error cases ====================

    #[test]
    fn test_parse_invalid_format_returns_none() {
        let result = parse_datetime("not-a-date", "2006-01-02T15:04:05Z07:00");
        assert!(result.is_none());
    }

    #[test]
    fn test_parse_mismatched_format_returns_none() {
        let result = parse_datetime("2024/01/15", "2006-01-02T15:04:05Z07:00");
        assert!(result.is_none());
    }

    #[test]
    fn test_parse_naive_with_invalid_input_returns_none() {
        let result = parse_datetime("not-a-date", "2006-01-02");
        assert!(result.is_none());
    }

    // ==================== Roundtrip tests ====================

    #[test]
    fn test_parse_roundtrip_with_z() {
        let input = "2024-06-20T14:30:45Z";
        let format = "2006-01-02T15:04:05Z07:00";

        let parsed = expect_with_offset(parse_datetime(input, format));
        let formatted = format_datetime(parsed, format).expect("datetime");

        assert_eq!(formatted, input);
    }

    #[test]
    fn test_parse_roundtrip_with_offset() {
        let input = "2024-06-20T14:30:45+05:30";
        let format = "2006-01-02T15:04:05Z07:00";

        let parsed = expect_with_offset(parse_datetime(input, format));
        let formatted = format_datetime(parsed, format).expect("datetime");

        assert_eq!(formatted, input);
    }

    #[test]
    fn test_parse_roundtrip_z0700() {
        let input = "2024-06-20T14:30:45Z";
        let format = "2006-01-02T15:04:05Z0700";

        let parsed = expect_with_offset(parse_datetime(input, format));
        let formatted = format_datetime(parsed, format).expect("datetime");

        assert_eq!(formatted, input);
    }
}
