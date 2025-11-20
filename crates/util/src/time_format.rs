use chrono::{FixedOffset, TimeZone, Utc};

/// Format a timestamp with optional timezone
///
/// # Arguments
/// * `nanos` - Nanoseconds since Unix epoch
/// * `go_format` - Go-style format string
/// * `timezone` - Optional timezone in format:
///   - `None` or `Some("Z")` or `Some("+00:00")` = UTC
///   - `Some("+07:00")` = UTC+7
///   - `Some("-05:30")` = UTC-5:30
///   - `Some("07:00")` = UTC+7 (assumes positive if no sign)
pub fn format_datetime(nanos: i64, go_format: &str, timezone: Option<&str>) -> String {
    let secs = nanos / 1_000_000_000;
    let nsecs = (nanos % 1_000_000_000) as u32;

    let rust_format = convert_go_format_to_rust(go_format);

    // Parse timezone string to offset in seconds
    let offset_secs = parse_timezone(timezone);
    let is_utc = offset_secs == 0;

    let formatted = if offset_secs == 0 {
        let dt = Utc.timestamp_opt(secs, nsecs).unwrap();
        dt.format(&rust_format).to_string()
    } else {
        let offset = FixedOffset::east_opt(offset_secs).unwrap();
        let dt = offset.timestamp_opt(secs, nsecs).unwrap();
        dt.format(&rust_format).to_string()
    };

    // Post-process: Replace +00:00 or +0000 with Z if format uses Z07:00
    if is_utc && go_format.contains("Z07:00") {
        return formatted.replace("+00:00", "Z");
    }
    if is_utc && go_format.contains("Z0700") {
        return formatted.replace("+0000", "Z");
    }

    formatted
}

/// Parse timezone string to offset in seconds
/// Supports formats: "+07:00", "-05:30", "07:00", "Z", "+0700", "-0530"
fn parse_timezone(timezone: Option<&str>) -> i32 {
    let tz = match timezone {
        None => return 0,
        Some(s) => s.trim(),
    };

    // Handle "Z" as UTC
    if tz == "Z" || tz == "z" {
        return 0;
    }

    // Parse sign
    let (sign, tz) = if tz.starts_with('+') {
        (1, &tz[1..])
    } else if tz.starts_with('-') {
        (-1, &tz[1..])
    } else {
        (1, tz) // Default to positive
    };

    // Parse hours and minutes
    let (hours, minutes) = if tz.contains(':') {
        // Format: "07:00" or "05:30"
        let parts: Vec<&str> = tz.split(':').collect();
        if parts.len() != 2 {
            return 0; // Invalid format, default to UTC
        }
        let hours = parts[0].parse::<i32>().unwrap_or(0);
        let minutes = parts[1].parse::<i32>().unwrap_or(0);
        (hours, minutes)
    } else if tz.len() == 4 {
        // Format: "0700" or "0530"
        let hours = tz[0..2].parse::<i32>().unwrap_or(0);
        let minutes = tz[2..4].parse::<i32>().unwrap_or(0);
        (hours, minutes)
    } else if tz.len() == 2 {
        // Format: "07"
        let hours = tz.parse::<i32>().unwrap_or(0);
        (hours, 0)
    } else {
        return 0; // Invalid format, default to UTC
    };

    sign * (hours * 3600 + minutes * 60)
}

fn convert_go_format_to_rust(go_format: &str) -> String {
    let mut result = String::new();
    let mut chars = go_format.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch.is_ascii_digit() {
            let mut num_str = ch.to_string();
            while let Some(&next_ch) = chars.peek() {
                if next_ch.is_ascii_digit() {
                    num_str.push(chars.next().unwrap());
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
                        result.push_str(&num_str);
                    }
                }
                _ => result.push_str(&num_str),
            }
        } else {
            match ch {
                '.' => {
                    // Check for subsecond precision
                    let next_chars: String = chars.clone().take(9).collect();

                    if next_chars.starts_with("000000000") || next_chars.starts_with("999999999") {
                        result.push_str(".%9f"); // nanoseconds
                        for _ in 0..9 {
                            chars.next();
                        }
                    } else if next_chars.starts_with("000000") || next_chars.starts_with("999999") {
                        result.push_str(".%6f"); // microseconds
                        for _ in 0..6 {
                            chars.next();
                        }
                    } else if next_chars.starts_with("000") || next_chars.starts_with("999") {
                        result.push_str(".%3f"); // milliseconds
                        for _ in 0..3 {
                            chars.next();
                        }
                    } else {
                        result.push(ch);
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
                        result.push(ch);
                    }
                }
                'J' => {
                    let next_chars: String = chars.clone().take(6).collect();
                    if next_chars.starts_with("anuary") {
                        result.push_str("%B");
                        for _ in 0..6 {
                            chars.next();
                        }
                    } else if next_chars.starts_with("an") {
                        result.push_str("%b");
                        for _ in 0..2 {
                            chars.next();
                        }
                    } else {
                        result.push(ch);
                    }
                }
                'M' => {
                    let next_chars: String = chars.clone().take(5).collect();
                    if next_chars.starts_with("onday") {
                        result.push_str("%A");
                        for _ in 0..5 {
                            chars.next();
                        }
                    } else if next_chars.starts_with("on") {
                        result.push_str("%a");
                        for _ in 0..2 {
                            chars.next();
                        }
                    } else if next_chars.starts_with("ST") {
                        result.push_str("%Z");
                        for _ in 0..2 {
                            chars.next();
                        }
                    } else {
                        result.push(ch);
                    }
                }
                'P' => {
                    if chars.peek() == Some(&'M') {
                        result.push_str("%p");
                        chars.next();
                    } else {
                        result.push(ch);
                    }
                }
                '+' | '-' => {
                    let next_chars: String = chars.clone().take(5).collect();

                    if next_chars.len() >= 5
                        && next_chars.chars().nth(0) == Some('0')
                        && next_chars.chars().nth(1) == Some('7')
                        && next_chars.chars().nth(2) == Some(':')
                        && next_chars.chars().nth(3) == Some('0')
                        && next_chars.chars().nth(4) == Some('0')
                    {
                        result.push_str("%:z");
                        for _ in 0..5 {
                            chars.next();
                        }
                    } else if next_chars.len() >= 4
                        && next_chars.chars().take(4).all(|c| c.is_ascii_digit())
                        && next_chars.starts_with("0700")
                    {
                        result.push_str("%z");
                        for _ in 0..4 {
                            chars.next();
                        }
                    } else if next_chars.len() >= 4
                        && next_chars.chars().take(4).all(|c| c.is_ascii_digit())
                        && !next_chars.starts_with("19")
                        && !next_chars.starts_with("20")
                    {
                        result.push_str("%z");
                        for _ in 0..4 {
                            chars.next();
                        }
                    } else {
                        result.push(ch);
                    }
                }
                _ => result.push(ch),
            }
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn make_nanos(
        year: i32,
        month: u32,
        day: u32,
        hour: u32,
        min: u32,
        sec: u32,
        nano: u32,
    ) -> i64 {
        let dt = Utc
            .with_ymd_and_hms(year, month, day, hour, min, sec)
            .unwrap();
        let timestamp = dt.timestamp();
        timestamp * 1_000_000_000 + nano as i64
    }

    // ... (keep all previous tests) ...

    #[test]
    fn test_millisecond_precision() {
        let nanos = make_nanos(2024, 11, 19, 10, 30, 45, 123000000); // .123 seconds

        assert_eq!(
            format_datetime(nanos, "2006-01-02T15:04:05.000", None),
            "2024-11-19T10:30:45.123"
        );

        assert_eq!(format_datetime(nanos, "15:04:05.000", None), "10:30:45.123");
    }

    #[test]
    fn test_microsecond_precision() {
        let nanos = make_nanos(2024, 11, 19, 10, 30, 45, 123456000); // .123456 seconds

        assert_eq!(
            format_datetime(nanos, "2006-01-02T15:04:05.000000", None),
            "2024-11-19T10:30:45.123456"
        );

        assert_eq!(
            format_datetime(nanos, "15:04:05.000000", None),
            "10:30:45.123456"
        );
    }

    #[test]
    fn test_nanosecond_precision() {
        let nanos = make_nanos(2024, 11, 19, 10, 30, 45, 123456789); // .123456789 seconds

        assert_eq!(
            format_datetime(nanos, "2006-01-02T15:04:05.000000000", None),
            "2024-11-19T10:30:45.123456789"
        );

        assert_eq!(
            format_datetime(nanos, "15:04:05.000000000", None),
            "10:30:45.123456789"
        );
    }

    #[test]
    fn test_subsecond_with_timezone() {
        let nanos = make_nanos(2024, 11, 19, 10, 30, 45, 123456789);

        assert_eq!(
            format_datetime(nanos, "2006-01-02T15:04:05.000Z07:00", Some("+05:30")),
            "2024-11-19T16:00:45.123+05:30"
        );

        assert_eq!(
            format_datetime(nanos, "2006-01-02T15:04:05.000000-07:00", Some("-08:00")),
            "2024-11-19T02:30:45.123456-08:00"
        );

        assert_eq!(
            format_datetime(nanos, "2006-01-02T15:04:05.000000000Z07:00", Some("Z")),
            "2024-11-19T10:30:45.123456789Z"
        );
    }

    #[test]
    fn test_zero_subseconds() {
        let nanos = make_nanos(2024, 11, 19, 10, 30, 45, 0); // No fractional seconds

        assert_eq!(
            format_datetime(nanos, "2006-01-02T15:04:05.000", None),
            "2024-11-19T10:30:45.000"
        );

        assert_eq!(
            format_datetime(nanos, "2006-01-02T15:04:05.000000", None),
            "2024-11-19T10:30:45.000000"
        );

        assert_eq!(
            format_datetime(nanos, "2006-01-02T15:04:05.000000000", None),
            "2024-11-19T10:30:45.000000000"
        );
    }

    #[test]
    fn test_half_second_subseconds() {
        let nanos = make_nanos(2024, 11, 19, 10, 30, 45, 500000000); // .5 seconds

        assert_eq!(format_datetime(nanos, "15:04:05.000", None), "10:30:45.500");

        assert_eq!(
            format_datetime(nanos, "15:04:05.000000", None),
            "10:30:45.500000"
        );

        assert_eq!(
            format_datetime(nanos, "15:04:05.000000000", None),
            "10:30:45.500000000"
        );
    }

    #[test]
    fn test_rfc3339_with_subseconds() {
        let nanos = make_nanos(2024, 11, 19, 10, 30, 45, 123456000);

        assert_eq!(
            format_datetime(nanos, "2006-01-02T15:04:05.000000Z07:00", None),
            "2024-11-19T10:30:45.123456Z"
        );

        assert_eq!(
            format_datetime(nanos, "2006-01-02T15:04:05.000000Z07:00", Some("+08:00")),
            "2024-11-19T18:30:45.123456+08:00"
        );
    }

    #[test]
    fn test_mixed_precision() {
        let nanos = make_nanos(2024, 11, 19, 10, 30, 45, 100000000); // .1 seconds

        assert_eq!(format_datetime(nanos, "15:04:05.000", None), "10:30:45.100");

        assert_eq!(
            format_datetime(nanos, "15:04:05.000000", None),
            "10:30:45.100000"
        );

        assert_eq!(
            format_datetime(nanos, "15:04:05.000000000", None),
            "10:30:45.100000000"
        );
    }

    #[test]
    fn test_conversion_with_subseconds() {
        assert_eq!(convert_go_format_to_rust(".000"), ".%3f");
        assert_eq!(convert_go_format_to_rust(".000000"), ".%6f");
        assert_eq!(convert_go_format_to_rust(".000000000"), ".%9f");

        assert_eq!(convert_go_format_to_rust(".999"), ".%3f");
        assert_eq!(convert_go_format_to_rust(".999999"), ".%6f");
        assert_eq!(convert_go_format_to_rust(".999999999"), ".%9f");

        assert_eq!(
            convert_go_format_to_rust("2006-01-02T15:04:05.000000Z07:00"),
            "%Y-%m-%dT%H:%M:%S.%6f%:z"
        );
    }

    #[test]
    fn test_subseconds_with_date_boundaries() {
        let nanos = make_nanos(2024, 11, 19, 23, 59, 59, 999999999);

        assert_eq!(
            format_datetime(nanos, "2006-01-02 15:04:05.000000000", None),
            "2024-11-19 23:59:59.999999999"
        );

        // Add 2 hours, should roll over to next day
        assert_eq!(
            format_datetime(nanos, "2006-01-02 15:04:05.000000000", Some("+02:00")),
            "2024-11-20 01:59:59.999999999"
        );
    }

    #[test]
    fn test_very_small_nanoseconds() {
        let nanos = make_nanos(2024, 11, 19, 10, 30, 45, 1); // 1 nanosecond

        assert_eq!(
            format_datetime(nanos, "15:04:05.000000000", None),
            "10:30:45.000000001"
        );
    }

    #[test]
    fn test_subseconds_all_formats_combined() {
        let nanos = make_nanos(2024, 11, 19, 14, 30, 45, 987654321);

        // Test complete RFC3339 with nanoseconds
        assert_eq!(
            format_datetime(nanos, "2006-01-02T15:04:05.000000000Z07:00", None),
            "2024-11-19T14:30:45.987654321Z"
        );

        // With positive offset
        assert_eq!(
            format_datetime(nanos, "2006-01-02T15:04:05.000000-07:00", Some("+09:00")),
            "2024-11-19T23:30:45.987654+09:00"
        );
    }
}
