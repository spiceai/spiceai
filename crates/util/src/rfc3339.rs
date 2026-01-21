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

//! RFC 3339 timestamp formatting utilities.
//!
//! Provides formatting for [`std::time::SystemTime`] values in RFC 3339 format.

use chrono::{DateTime, SecondsFormat, Utc};
use std::fmt::{self, Display, Formatter};
use std::time::SystemTime;

/// A wrapper around [`SystemTime`] that implements [`Display`] for RFC 3339 output.
///
/// The format follows RFC 3339 with millisecond precision (e.g., `2024-01-15T10:30:00.123Z`).
///
/// # Examples
///
/// ```
/// use util::rfc3339::FormattedTimestamp;
/// use std::time::SystemTime;
///
/// let ts = FormattedTimestamp(SystemTime::now());
/// println!("{ts}");  // e.g., "2024-01-15T10:30:00.123Z"
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FormattedTimestamp(pub SystemTime);

impl Display for FormattedTimestamp {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let datetime: DateTime<Utc> = self.0.into();
        write!(
            f,
            "{}",
            datetime.to_rfc3339_opts(SecondsFormat::Millis, true)
        )
    }
}

/// Format a system time into an RFC 3339 string.
///
/// This is a convenience function that wraps [`FormattedTimestamp`].
///
/// # Examples
///
/// ```
/// use util::rfc3339::format_rfc3339;
/// use std::time::SystemTime;
///
/// let ts = format_rfc3339(SystemTime::now());
/// println!("{ts}");  // e.g., "2024-01-15T10:30:00.123Z"
/// ```
#[must_use]
pub fn format_rfc3339(time: SystemTime) -> FormattedTimestamp {
    FormattedTimestamp(time)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_format_unix_epoch() {
        let epoch = SystemTime::UNIX_EPOCH;
        let formatted = format!("{}", FormattedTimestamp(epoch));
        assert_eq!(formatted, "1970-01-01T00:00:00.000Z");
    }

    #[test]
    fn test_format_specific_time() {
        // 2024-01-15 10:30:00 UTC
        let time = SystemTime::UNIX_EPOCH + Duration::from_secs(1_705_315_800);
        let formatted = format!("{}", FormattedTimestamp(time));
        assert_eq!(formatted, "2024-01-15T10:30:00.000Z");
    }

    #[test]
    fn test_format_with_milliseconds() {
        // 2024-01-15 10:30:00.123 UTC
        let time = SystemTime::UNIX_EPOCH + Duration::from_millis(1_705_315_800_123);
        let formatted = format!("{}", FormattedTimestamp(time));
        assert_eq!(formatted, "2024-01-15T10:30:00.123Z");
    }

    #[test]
    fn test_format_with_subsecond_precision() {
        // Test that we truncate to milliseconds (not microseconds or nanoseconds)
        let time = SystemTime::UNIX_EPOCH + Duration::from_nanos(1_705_315_800_123_456_789);
        let formatted = format!("{}", FormattedTimestamp(time));
        // Should be truncated to .123 (milliseconds only)
        assert_eq!(formatted, "2024-01-15T10:30:00.123Z");
    }

    #[test]
    fn test_format_current_time_is_valid() {
        let now = SystemTime::now();
        let formatted = format!("{}", FormattedTimestamp(now));

        // Basic RFC 3339 format checks
        assert!(formatted.ends_with('Z'), "should end with Z for UTC");
        assert!(formatted.contains('T'), "should contain T separator");
        assert!(
            formatted.len() == 24,
            "should be 24 chars with milliseconds"
        );

        // Parse it back to verify it's valid
        let parsed = DateTime::parse_from_rfc3339(&formatted);
        assert!(parsed.is_ok(), "should be parseable as RFC 3339");
    }

    #[test]
    fn test_format_rfc3339_convenience() {
        let now = SystemTime::now();
        let formatted = format_rfc3339(now);
        let formatted_str = format!("{formatted}");
        assert!(formatted_str.ends_with('Z'));
    }

    #[test]
    fn test_clone_and_copy() {
        let ts1 = FormattedTimestamp(SystemTime::UNIX_EPOCH);
        let ts2 = ts1;
        let ts3 = ts1.clone();
        assert_eq!(ts1, ts2);
        assert_eq!(ts1, ts3);
    }

    #[test]
    fn test_debug_format() {
        let ts = FormattedTimestamp(SystemTime::UNIX_EPOCH);
        let debug_str = format!("{ts:?}");
        assert!(debug_str.contains("FormattedTimestamp"));
    }

    #[test]
    fn test_year_2000() {
        // Y2K: 2000-01-01 00:00:00 UTC
        let y2k = SystemTime::UNIX_EPOCH + Duration::from_secs(946_684_800);
        let formatted = format!("{}", FormattedTimestamp(y2k));
        assert_eq!(formatted, "2000-01-01T00:00:00.000Z");
    }

    #[test]
    fn test_far_future() {
        // 2100-01-01 00:00:00 UTC
        let future = SystemTime::UNIX_EPOCH + Duration::from_secs(4_102_444_800);
        let formatted = format!("{}", FormattedTimestamp(future));
        assert_eq!(formatted, "2100-01-01T00:00:00.000Z");
    }
}
