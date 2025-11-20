use chrono::{DateTime, Utc, FixedOffset, TimeZone};

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
                        for _ in 0..3 { chars.next(); }
                    } else {
                        result.push_str(&num_str);
                    }
                },
                _ => result.push_str(&num_str),
            }
        } else {
            match ch {
                'Z' => {
                    let next_chars: String = chars.clone().take(5).collect();
                    if next_chars == "07:00" {
                        result.push_str("%:z");
                        for _ in 0..5 { chars.next(); }
                    } else if next_chars.starts_with("0700") {
                        result.push_str("%z");
                        for _ in 0..4 { chars.next(); }
                    } else {
                        result.push(ch);
                    }
                },
                'J' => {
                    let next_chars: String = chars.clone().take(6).collect();
                    if next_chars.starts_with("anuary") {
                        result.push_str("%B");
                        for _ in 0..6 { chars.next(); }
                    } else if next_chars.starts_with("an") {
                        result.push_str("%b");
                        for _ in 0..2 { chars.next(); }
                    } else {
                        result.push(ch);
                    }
                },
                'M' => {
                    let next_chars: String = chars.clone().take(5).collect();
                    if next_chars.starts_with("onday") {
                        result.push_str("%A");
                        for _ in 0..5 { chars.next(); }
                    } else if next_chars.starts_with("on") {
                        result.push_str("%a");
                        for _ in 0..2 { chars.next(); }
                    } else if next_chars.starts_with("ST") {
                        result.push_str("%Z");
                        for _ in 0..2 { chars.next(); }
                    } else {
                        result.push(ch);
                    }
                },
                'P' => {
                    if chars.peek() == Some(&'M') {
                        result.push_str("%p");
                        chars.next();
                    } else {
                        result.push(ch);
                    }
                },
                '+' | '-' => {
                    let next_chars: String = chars.clone().take(5).collect();

                    if next_chars.len() >= 5
                        && next_chars.chars().nth(0) == Some('0')
                        && next_chars.chars().nth(1) == Some('7')
                        && next_chars.chars().nth(2) == Some(':')
                        && next_chars.chars().nth(3) == Some('0')
                        && next_chars.chars().nth(4) == Some('0') {
                        result.push_str("%:z");
                        for _ in 0..5 { chars.next(); }
                    } else if next_chars.len() >= 4
                        && next_chars.chars().take(4).all(|c| c.is_ascii_digit())
                        && next_chars.starts_with("0700") {
                        result.push_str("%z");
                        for _ in 0..4 { chars.next(); }
                    } else if next_chars.len() >= 4
                        && next_chars.chars().take(4).all(|c| c.is_ascii_digit())
                        && !next_chars.starts_with("19")
                        && !next_chars.starts_with("20") {
                        result.push_str("%z");
                        for _ in 0..4 { chars.next(); }
                    } else {
                        result.push(ch);
                    }
                },
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

    fn make_nanos(year: i32, month: u32, day: u32, hour: u32, min: u32, sec: u32, nano: u32) -> i64 {
        let dt = Utc.with_ymd_and_hms(year, month, day, hour, min, sec).unwrap();
        let timestamp = dt.timestamp();
        timestamp * 1_000_000_000 + nano as i64
    }

    #[test]
    fn test_parse_timezone() {
        assert_eq!(parse_timezone(None), 0);
        assert_eq!(parse_timezone(Some("Z")), 0);
        assert_eq!(parse_timezone(Some("z")), 0);
        assert_eq!(parse_timezone(Some("+00:00")), 0);
        assert_eq!(parse_timezone(Some("-00:00")), 0);

        assert_eq!(parse_timezone(Some("+07:00")), 7 * 3600);
        assert_eq!(parse_timezone(Some("07:00")), 7 * 3600);
        assert_eq!(parse_timezone(Some("-05:00")), -5 * 3600);

        assert_eq!(parse_timezone(Some("+05:30")), 5 * 3600 + 30 * 60);
        assert_eq!(parse_timezone(Some("-03:30")), -(3 * 3600 + 30 * 60));

        assert_eq!(parse_timezone(Some("+0700")), 7 * 3600);
        assert_eq!(parse_timezone(Some("-0530")), -(5 * 3600 + 30 * 60));

        assert_eq!(parse_timezone(Some("07")), 7 * 3600);
        assert_eq!(parse_timezone(Some("+12")), 12 * 3600);
    }

    #[test]
    fn test_utc_none_with_z_format() {
        let nanos = make_nanos(2024, 11, 19, 10, 30, 0, 0);

        assert_eq!(
            format_datetime(nanos, "2006-01-02T15:04:05Z07:00", None),
            "2024-11-19T10:30:00Z"
        );
    }

    #[test]
    fn test_utc_z_string_with_z_format() {
        let nanos = make_nanos(2024, 11, 19, 10, 30, 0, 0);

        assert_eq!(
            format_datetime(nanos, "2006-01-02T15:04:05Z07:00", Some("Z")),
            "2024-11-19T10:30:00Z"
        );
    }

    #[test]
    fn test_utc_with_plus_minus_format() {
        let nanos = make_nanos(2024, 11, 19, 10, 30, 0, 0);

        assert_eq!(
            format_datetime(nanos, "2006-01-02T15:04:05-07:00", None),
            "2024-11-19T10:30:00+00:00"
        );

        assert_eq!(
            format_datetime(nanos, "2006-01-02T15:04:05-0700", Some("Z")),
            "2024-11-19T10:30:00+0000"
        );
    }

    #[test]
    fn test_positive_timezone_offset() {
        let nanos = make_nanos(2024, 11, 19, 10, 30, 0, 0);

        assert_eq!(
            format_datetime(nanos, "2006-01-02T15:04:05Z07:00", Some("+05:00")),
            "2024-11-19T15:30:00+05:00"
        );

        assert_eq!(
            format_datetime(nanos, "2006-01-02T15:04:05-07:00", Some("05:00")),
            "2024-11-19T15:30:00+05:00"
        );

        assert_eq!(
            format_datetime(nanos, "2006-01-02T15:04:05-0700", Some("+0500")),
            "2024-11-19T15:30:00+0500"
        );
    }

    #[test]
    fn test_negative_timezone_offset() {
        let nanos = make_nanos(2024, 11, 19, 10, 30, 0, 0);

        assert_eq!(
            format_datetime(nanos, "2006-01-02T15:04:05Z07:00", Some("-05:00")),
            "2024-11-19T05:30:00-05:00"
        );

        assert_eq!(
            format_datetime(nanos, "2006-01-02T15:04:05-07:00", Some("-05:00")),
            "2024-11-19T05:30:00-05:00"
        );

        assert_eq!(
            format_datetime(nanos, "2006-01-02T15:04:05-0700", Some("-0500")),
            "2024-11-19T05:30:00-0500"
        );
    }

    #[test]
    fn test_half_hour_timezone_offset() {
        let nanos = make_nanos(2024, 11, 19, 10, 0, 0, 0);

        assert_eq!(
            format_datetime(nanos, "2006-01-02T15:04:05Z07:00", Some("+05:30")),
            "2024-11-19T15:30:00+05:30"
        );

        assert_eq!(
            format_datetime(nanos, "2006-01-02T15:04:05Z07:00", Some("-03:30")),
            "2024-11-19T06:30:00-03:30"
        );

        assert_eq!(
            format_datetime(nanos, "2006-01-02T15:04:05-0700", Some("+0530")),
            "2024-11-19T15:30:00+0530"
        );
    }

    #[test]
    fn test_date_boundary_crossing() {
        let nanos = make_nanos(2024, 11, 19, 2, 0, 0, 0);

        assert_eq!(
            format_datetime(nanos, "2006-01-02 15:04:05", Some("-05:00")),
            "2024-11-18 21:00:00"
        );

        assert_eq!(
            format_datetime(nanos, "2006-01-02 15:04:05", Some("+10:00")),
            "2024-11-19 12:00:00"
        );
    }

    #[test]
    fn test_year_boundary_crossing() {
        let nanos = make_nanos(2024, 1, 1, 2, 0, 0, 0);

        assert_eq!(
            format_datetime(nanos, "2006-01-02 15:04:05", Some("-05:00")),
            "2023-12-31 21:00:00"
        );
    }

    #[test]
    fn test_basic_date_format_with_timezone() {
        let nanos = make_nanos(2024, 11, 19, 14, 30, 45, 0);

        assert_eq!(format_datetime(nanos, "2006-01-02", None), "2024-11-19");
        assert_eq!(format_datetime(nanos, "2006/01/02", None), "2024/11/19");
        assert_eq!(format_datetime(nanos, "02-01-2006", None), "19-11-2024");

        assert_eq!(format_datetime(nanos, "2006-01-02", Some("+05:00")), "2024-11-19");
    }

    #[test]
    fn test_basic_time_format_with_timezone() {
        let nanos = make_nanos(2024, 11, 19, 14, 30, 45, 0);

        assert_eq!(format_datetime(nanos, "15:04:05", None), "14:30:45");
        assert_eq!(format_datetime(nanos, "15:04:05", Some("+05:00")), "19:30:45");
    }

    #[test]
    fn test_month_names_with_timezone() {
        let nanos = make_nanos(2024, 1, 1, 2, 0, 0, 0);

        assert_eq!(format_datetime(nanos, "January 2, 2006", None), "January 1, 2024");
        assert_eq!(format_datetime(nanos, "January 2, 2006", Some("-05:00")), "December 31, 2023");
    }

    #[test]
    fn test_weekday_names_with_timezone() {
        let nanos = make_nanos(2024, 11, 19, 23, 0, 0, 0);

        assert_eq!(format_datetime(nanos, "Monday", None), "Tuesday");
        assert_eq!(format_datetime(nanos, "Monday", Some("+02:00")), "Wednesday");
    }

    #[test]
    fn test_rfc3339_format() {
        let nanos = make_nanos(2024, 11, 19, 10, 30, 0, 0);

        assert_eq!(
            format_datetime(nanos, "2006-01-02T15:04:05Z07:00", None),
            "2024-11-19T10:30:00Z"
        );

        assert_eq!(
            format_datetime(nanos, "2006-01-02T15:04:05Z07:00", Some("+08:00")),
            "2024-11-19T18:30:00+08:00"
        );
    }

    #[test]
    fn test_12_hour_format_with_timezone() {
        let nanos = make_nanos(2024, 11, 19, 14, 30, 0, 0);

        assert_eq!(format_datetime(nanos, "03:04 PM", None), "02:30 PM");
        assert_eq!(format_datetime(nanos, "03:04 PM", Some("-08:00")), "06:30 AM");
    }

    #[test]
    fn test_extreme_timezone_offsets() {
        let nanos = make_nanos(2024, 11, 19, 12, 0, 0, 0);

        assert_eq!(
            format_datetime(nanos, "2006-01-02T15:04:05-07:00", Some("+14:00")),
            "2024-11-20T02:00:00+14:00"
        );

        assert_eq!(
            format_datetime(nanos, "2006-01-02T15:04:05-07:00", Some("-12:00")),
            "2024-11-19T00:00:00-12:00"
        );
    }

    #[test]
    fn test_all_timezone_format_combinations() {
        let nanos = make_nanos(2024, 11, 19, 10, 30, 0, 0);

        assert_eq!(format_datetime(nanos, "Z07:00", None), "Z");
        assert_eq!(format_datetime(nanos, "Z0700", Some("Z")), "Z");
        assert_eq!(format_datetime(nanos, "-07:00", None), "+00:00");
        assert_eq!(format_datetime(nanos, "-0700", None), "+0000");

        assert_eq!(format_datetime(nanos, "Z07:00", Some("+05:45")), "+05:45");
        assert_eq!(format_datetime(nanos, "-07:00", Some("05:45")), "+05:45");
    }
}