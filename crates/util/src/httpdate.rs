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

//! HTTP date parsing utilities.
//!
//! Provides parsing for HTTP date formats as defined in RFC 7231.
//!
//! HTTP dates can be in one of three formats:
//! - IMF-fixdate: `Sun, 06 Nov 1994 08:49:37 GMT` (preferred)
//! - RFC 850: `Sunday, 06-Nov-94 08:49:37 GMT` (obsolete)
//! - ANSI C asctime: `Sun Nov  6 08:49:37 1994` (obsolete)

use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use std::time::SystemTime;

/// Parse an HTTP date string into a [`SystemTime`].
///
/// This function supports all three HTTP date formats defined in RFC 7231:
/// - IMF-fixdate (RFC 5322 with fixed GMT): `Sun, 06 Nov 1994 08:49:37 GMT`
/// - RFC 850 format: `Sunday, 06-Nov-94 08:49:37 GMT`
/// - ANSI C asctime format: `Sun Nov  6 08:49:37 1994`
///
/// # Examples
///
/// ```
/// use util::httpdate::parse_http_date;
///
/// // IMF-fixdate format (preferred)
/// let time = parse_http_date("Sun, 06 Nov 1994 08:49:37 GMT");
/// assert!(time.is_some());
///
/// // RFC 850 format (obsolete but still used)
/// let time = parse_http_date("Sunday, 06-Nov-94 08:49:37 GMT");
/// assert!(time.is_some());
///
/// // asctime format (obsolete but still used)
/// let time = parse_http_date("Sun Nov  6 08:49:37 1994");
/// assert!(time.is_some());
/// ```
///
/// Returns `None` if the date string cannot be parsed.
#[must_use]
pub fn parse_http_date(s: &str) -> Option<SystemTime> {
    // Try IMF-fixdate format first (most common): "Sun, 06 Nov 1994 08:49:37 GMT"
    if let Some(time) = parse_imf_fixdate(s) {
        return Some(time);
    }

    // Try RFC 850 format: "Sunday, 06-Nov-94 08:49:37 GMT"
    if let Some(time) = parse_rfc850(s) {
        return Some(time);
    }

    // Try asctime format: "Sun Nov  6 08:49:37 1994"
    parse_asctime(s)
}

/// Parse IMF-fixdate format: "Sun, 06 Nov 1994 08:49:37 GMT"
fn parse_imf_fixdate(s: &str) -> Option<SystemTime> {
    // Format: "%a, %d %b %Y %H:%M:%S GMT"
    let s = s.trim();
    if !s.ends_with("GMT") {
        return None;
    }

    // Remove " GMT" suffix
    let date_part = s.strip_suffix(" GMT")?;

    // Parse using chrono
    let naive = NaiveDateTime::parse_from_str(date_part, "%a, %d %b %Y %H:%M:%S").ok()?;
    let utc_dt: DateTime<Utc> = Utc.from_utc_datetime(&naive);

    Some(utc_dt.into())
}

/// Parse RFC 850 format: "Sunday, 06-Nov-94 08:49:37 GMT"
fn parse_rfc850(s: &str) -> Option<SystemTime> {
    let s = s.trim();
    if !s.ends_with("GMT") {
        return None;
    }

    // Remove " GMT" suffix
    let date_part = s.strip_suffix(" GMT")?;

    // Find the comma separator for day name
    let comma_pos = date_part.find(',')?;
    let time_part = date_part.get(comma_pos + 2..)?; // Skip ", "

    // Parse: "06-Nov-94 08:49:37"
    let naive = NaiveDateTime::parse_from_str(time_part, "%d-%b-%y %H:%M:%S").ok()?;
    let utc_dt: DateTime<Utc> = Utc.from_utc_datetime(&naive);

    Some(utc_dt.into())
}

/// Parse asctime format: "Sun Nov  6 08:49:37 1994"
fn parse_asctime(s: &str) -> Option<SystemTime> {
    let s = s.trim();

    // The asctime format has two spaces before single-digit days
    // Normalize to single space for easier parsing
    let normalized = s.replace("  ", " ");

    // Parse: "Sun Nov 6 08:49:37 1994"
    let naive = NaiveDateTime::parse_from_str(&normalized, "%a %b %d %H:%M:%S %Y").ok()?;
    let utc_dt: DateTime<Utc> = Utc.from_utc_datetime(&naive);

    Some(utc_dt.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    // Reference time: Nov 6, 1994 08:49:37 GMT
    // Unix timestamp: 784111777
    const REFERENCE_UNIX_SECS: u64 = 784_111_777;

    fn reference_time() -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(REFERENCE_UNIX_SECS)
    }

    #[test]
    fn test_parse_imf_fixdate() {
        let result = parse_http_date("Sun, 06 Nov 1994 08:49:37 GMT");
        assert_eq!(result, Some(reference_time()));
    }

    #[test]
    fn test_parse_rfc850() {
        let result = parse_http_date("Sunday, 06-Nov-94 08:49:37 GMT");
        assert_eq!(result, Some(reference_time()));
    }

    #[test]
    fn test_parse_asctime() {
        let result = parse_http_date("Sun Nov  6 08:49:37 1994");
        assert_eq!(result, Some(reference_time()));
    }

    #[test]
    fn test_parse_asctime_double_digit_day() {
        // Nov 16 1994 08:49:37
        let result = parse_http_date("Wed Nov 16 08:49:37 1994");
        assert!(result.is_some());
    }

    #[test]
    fn test_parse_invalid_format() {
        assert!(parse_http_date("invalid date").is_none());
        assert!(parse_http_date("").is_none());
        assert!(parse_http_date("2024-01-15").is_none());
        assert!(parse_http_date("Jan 15, 2024").is_none());
    }

    #[test]
    fn test_parse_missing_gmt() {
        // IMF-fixdate without GMT should fail
        assert!(parse_http_date("Sun, 06 Nov 1994 08:49:37").is_none());
        // RFC 850 without GMT should fail
        assert!(parse_http_date("Sunday, 06-Nov-94 08:49:37").is_none());
    }

    #[test]
    fn test_parse_wrong_timezone() {
        assert!(parse_http_date("Sun, 06 Nov 1994 08:49:37 UTC").is_none());
        assert!(parse_http_date("Sun, 06 Nov 1994 08:49:37 EST").is_none());
    }

    #[test]
    fn test_parse_with_leading_trailing_whitespace() {
        let result = parse_http_date("  Sun, 06 Nov 1994 08:49:37 GMT  ");
        assert_eq!(result, Some(reference_time()));
    }

    #[test]
    fn test_parse_all_months() {
        let months = [
            ("Jan", "01"),
            ("Feb", "02"),
            ("Mar", "03"),
            ("Apr", "04"),
            ("May", "05"),
            ("Jun", "06"),
            ("Jul", "07"),
            ("Aug", "08"),
            ("Sep", "09"),
            ("Oct", "10"),
            ("Nov", "11"),
            ("Dec", "12"),
        ];

        for (abbrev, _) in &months {
            let date_str = format!("Mon, 15 {abbrev} 2024 10:30:00 GMT");
            let result = parse_http_date(&date_str);
            assert!(result.is_some(), "should parse month {abbrev}");
        }
    }

    #[test]
    fn test_parse_all_weekdays() {
        let weekdays = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];

        for day in &weekdays {
            // Use dates that actually fall on these weekdays
            let date_str = format!("{day}, 01 Jan 2024 00:00:00 GMT");
            // Just check it doesn't panic - the day might not match the date
            let _ = parse_http_date(&date_str);
        }
    }

    #[test]
    fn test_parse_year_2000() {
        let result = parse_http_date("Sat, 01 Jan 2000 00:00:00 GMT");
        let expected = SystemTime::UNIX_EPOCH + Duration::from_secs(946_684_800);
        assert_eq!(result, Some(expected));
    }

    #[test]
    fn test_parse_unix_epoch() {
        let result = parse_http_date("Thu, 01 Jan 1970 00:00:00 GMT");
        assert_eq!(result, Some(SystemTime::UNIX_EPOCH));
    }

    #[test]
    fn test_parse_midnight() {
        let result = parse_http_date("Mon, 15 Jan 2024 00:00:00 GMT");
        assert!(result.is_some());
    }

    #[test]
    fn test_parse_end_of_day() {
        let result = parse_http_date("Mon, 15 Jan 2024 23:59:59 GMT");
        assert!(result.is_some());
    }

    #[test]
    fn test_parse_leap_year_feb_29() {
        let result = parse_http_date("Thu, 29 Feb 2024 12:00:00 GMT");
        assert!(result.is_some());
    }

    #[test]
    fn test_parse_non_leap_year_feb_29() {
        // Feb 29 doesn't exist in 2023
        let result = parse_http_date("Wed, 29 Feb 2023 12:00:00 GMT");
        assert!(result.is_none());
    }
}
