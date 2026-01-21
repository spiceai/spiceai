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

//! Duration formatting utilities.
//!
//! Provides human-readable formatting for [`std::time::Duration`] values.

use std::fmt::{self, Display, Formatter};
use std::time::Duration;

/// A wrapper around [`Duration`] that implements [`Display`] for human-readable output.
///
/// The format is similar to `humantime` crate's output:
/// - Uses the largest appropriate unit (days, hours, minutes, seconds, milliseconds, etc.)
/// - Shows up to 2 units for precision (e.g., "1h 30m")
/// - Sub-second durations use milliseconds, microseconds, or nanoseconds as appropriate
///
/// # Examples
///
/// ```
/// use util::duration_format::FormattedDuration;
/// use std::time::Duration;
///
/// let d = FormattedDuration(Duration::from_secs(3661));
/// assert_eq!(format!("{d}"), "1h 1m 1s");
///
/// let d = FormattedDuration(Duration::from_millis(1500));
/// assert_eq!(format!("{d}"), "1s 500ms");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FormattedDuration(pub Duration);

impl Display for FormattedDuration {
    #[expect(clippy::cast_possible_truncation)]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let total_nanos = self.0.as_nanos();

        if total_nanos == 0 {
            return write!(f, "0s");
        }

        // Calculate all time components
        // These casts are safe: modulo operations ensure values fit in smaller types,
        // and days from u128 nanoseconds won't exceed u64 in practical use.
        let nanos = (total_nanos % 1_000) as u32;
        let micros = ((total_nanos / 1_000) % 1_000) as u32;
        let millis = ((total_nanos / 1_000_000) % 1_000) as u32;
        let secs = ((total_nanos / 1_000_000_000) % 60) as u64;
        let mins = ((total_nanos / 60_000_000_000) % 60) as u64;
        let hours = ((total_nanos / 3_600_000_000_000) % 24) as u64;
        let days = (total_nanos / 86_400_000_000_000) as u64;

        let mut parts = Vec::new();

        if days > 0 {
            parts.push(format!("{days}d"));
        }
        if hours > 0 {
            parts.push(format!("{hours}h"));
        }
        if mins > 0 {
            parts.push(format!("{mins}m"));
        }
        if secs > 0 {
            parts.push(format!("{secs}s"));
        }
        if millis > 0 {
            parts.push(format!("{millis}ms"));
        }
        if micros > 0 {
            parts.push(format!("{micros}us"));
        }
        if nanos > 0 {
            parts.push(format!("{nanos}ns"));
        }

        write!(f, "{}", parts.join(" "))
    }
}

/// Format a duration into a human-readable string.
///
/// This is a convenience function that wraps [`FormattedDuration`].
///
/// # Examples
///
/// ```
/// use util::duration_format::format_duration;
/// use std::time::Duration;
///
/// let d = Duration::from_secs(90);
/// assert_eq!(format!("{}", format_duration(d)), "1m 30s");
/// ```
#[must_use]
pub fn format_duration(d: Duration) -> FormattedDuration {
    FormattedDuration(d)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zero_duration() {
        let d = FormattedDuration(Duration::ZERO);
        assert_eq!(format!("{d}"), "0s");
    }

    #[test]
    fn test_nanoseconds_only() {
        let d = FormattedDuration(Duration::from_nanos(1));
        assert_eq!(format!("{d}"), "1ns");

        let d = FormattedDuration(Duration::from_nanos(999));
        assert_eq!(format!("{d}"), "999ns");
    }

    #[test]
    fn test_microseconds_only() {
        let d = FormattedDuration(Duration::from_micros(1));
        assert_eq!(format!("{d}"), "1us");

        let d = FormattedDuration(Duration::from_micros(999));
        assert_eq!(format!("{d}"), "999us");
    }

    #[test]
    fn test_milliseconds_only() {
        let d = FormattedDuration(Duration::from_millis(1));
        assert_eq!(format!("{d}"), "1ms");

        let d = FormattedDuration(Duration::from_millis(999));
        assert_eq!(format!("{d}"), "999ms");
    }

    #[test]
    fn test_seconds_only() {
        let d = FormattedDuration(Duration::from_secs(1));
        assert_eq!(format!("{d}"), "1s");

        let d = FormattedDuration(Duration::from_secs(59));
        assert_eq!(format!("{d}"), "59s");
    }

    #[test]
    fn test_minutes_only() {
        let d = FormattedDuration(Duration::from_secs(60));
        assert_eq!(format!("{d}"), "1m");

        let d = FormattedDuration(Duration::from_secs(59 * 60));
        assert_eq!(format!("{d}"), "59m");
    }

    #[test]
    fn test_hours_only() {
        let d = FormattedDuration(Duration::from_secs(3600));
        assert_eq!(format!("{d}"), "1h");

        let d = FormattedDuration(Duration::from_secs(23 * 3600));
        assert_eq!(format!("{d}"), "23h");
    }

    #[test]
    fn test_days_only() {
        let d = FormattedDuration(Duration::from_secs(86400));
        assert_eq!(format!("{d}"), "1d");

        let d = FormattedDuration(Duration::from_secs(7 * 86400));
        assert_eq!(format!("{d}"), "7d");
    }

    #[test]
    fn test_combined_units() {
        // 1h 30m
        let d = FormattedDuration(Duration::from_secs(5400));
        assert_eq!(format!("{d}"), "1h 30m");

        // 1h 1m 1s
        let d = FormattedDuration(Duration::from_secs(3661));
        assert_eq!(format!("{d}"), "1h 1m 1s");

        // 1d 2h 3m 4s
        let d = FormattedDuration(Duration::from_secs(86400 + 7200 + 180 + 4));
        assert_eq!(format!("{d}"), "1d 2h 3m 4s");
    }

    #[test]
    fn test_mixed_sub_second() {
        // 1s 500ms
        let d = FormattedDuration(Duration::from_millis(1500));
        assert_eq!(format!("{d}"), "1s 500ms");

        // 1ms 500us
        let d = FormattedDuration(Duration::from_micros(1500));
        assert_eq!(format!("{d}"), "1ms 500us");

        // 1us 500ns
        let d = FormattedDuration(Duration::from_nanos(1500));
        assert_eq!(format!("{d}"), "1us 500ns");
    }

    #[test]
    fn test_all_units() {
        // 1d 1h 1m 1s 1ms 1us 1ns
        let d = FormattedDuration(Duration::from_nanos(
            86_400_000_000_000
                + 3_600_000_000_000
                + 60_000_000_000
                + 1_000_000_000
                + 1_000_000
                + 1_000
                + 1,
        ));
        assert_eq!(format!("{d}"), "1d 1h 1m 1s 1ms 1us 1ns");
    }

    #[test]
    fn test_format_duration_convenience() {
        let d = format_duration(Duration::from_secs(90));
        assert_eq!(format!("{d}"), "1m 30s");
    }

    #[test]
    fn test_large_days() {
        let d = FormattedDuration(Duration::from_secs(365 * 86400));
        assert_eq!(format!("{d}"), "365d");
    }

    #[test]
    fn test_clone_and_copy() {
        let d1 = FormattedDuration(Duration::from_secs(100));
        let d2 = d1;
        let d3 = d1.clone();
        assert_eq!(d1, d2);
        assert_eq!(d1, d3);
    }

    #[test]
    fn test_debug_format() {
        let d = FormattedDuration(Duration::from_secs(100));
        let debug_str = format!("{d:?}");
        assert!(debug_str.contains("FormattedDuration"));
    }
}
