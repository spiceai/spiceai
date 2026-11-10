/*
Copyright 2024-2026 The Spice.ai OSS Authors
Copyright (c) 2014-2026 Kang Seonghoon and contributors

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

//! Functionality for rounding or truncating a `DateTime` by a `TimeDelta`.

use crate::{DateTime, NaiveDateTime, TimeDelta, TimeZone, Timelike};
use core::cmp::Ordering;
use core::fmt;
use core::ops::{Add, Sub};

/// Extension trait for rounding or truncating a DateTime by a TimeDelta.
///
/// # Limitations
/// Both rounding and truncating are done via [`TimeDelta::num_nanoseconds`] and
/// [`DateTime::timestamp_nanos_opt`]. This means that they will fail if either the
/// `TimeDelta` or the `DateTime` are too big to represented as nanoseconds. They
/// will also fail if the `TimeDelta` is bigger than the timestamp, negative or zero.
pub trait DurationRound: Sized {
    /// Error that can occur in rounding or truncating
    type Err: std::error::Error;

    /// Return a copy rounded by TimeDelta.
    ///
    /// # Example
    /// ``` rust
    /// # use chrono::{DurationRound, TimeDelta, NaiveDate};
    /// let dt = NaiveDate::from_ymd_opt(2018, 1, 11)
    ///     .unwrap()
    ///     .and_hms_nano_opt(12, 0, 0, 154_000_000)
    ///     .unwrap()
    ///     .and_utc();
    /// assert_eq!(
    ///     dt.duration_round(TimeDelta::try_milliseconds(10).unwrap()).unwrap().to_string(),
    ///     "2018-01-11 12:00:00.150 UTC"
    /// );
    /// assert_eq!(
    ///     dt.duration_round(TimeDelta::try_days(1).unwrap()).unwrap().to_string(),
    ///     "2018-01-12 00:00:00 UTC"
    /// );
    /// ```
    fn duration_round(self, duration: TimeDelta) -> Result<Self, Self::Err>;

    /// Return a copy truncated by TimeDelta.
    ///
    /// # Example
    /// ``` rust
    /// # use chrono::{DurationRound, TimeDelta, NaiveDate};
    /// let dt = NaiveDate::from_ymd_opt(2018, 1, 11)
    ///     .unwrap()
    ///     .and_hms_nano_opt(12, 0, 0, 154_000_000)
    ///     .unwrap()
    ///     .and_utc();
    /// assert_eq!(
    ///     dt.duration_trunc(TimeDelta::try_milliseconds(10).unwrap()).unwrap().to_string(),
    ///     "2018-01-11 12:00:00.150 UTC"
    /// );
    /// assert_eq!(
    ///     dt.duration_trunc(TimeDelta::try_days(1).unwrap()).unwrap().to_string(),
    ///     "2018-01-11 00:00:00 UTC"
    /// );
    /// ```
    fn duration_trunc(self, duration: TimeDelta) -> Result<Self, Self::Err>;
}

impl<Tz: TimeZone> DurationRound for DateTime<Tz> {
    type Err = RoundingError;

    fn duration_round(self, duration: TimeDelta) -> Result<Self, Self::Err> {
        duration_round(self.naive_local(), self, duration)
    }

    fn duration_trunc(self, duration: TimeDelta) -> Result<Self, Self::Err> {
        duration_trunc(self.naive_local(), self, duration)
    }
}

impl DurationRound for NaiveDateTime {
    type Err = RoundingError;

    fn duration_round(self, duration: TimeDelta) -> Result<Self, Self::Err> {
        duration_round(self, self, duration)
    }

    fn duration_trunc(self, duration: TimeDelta) -> Result<Self, Self::Err> {
        duration_trunc(self, self, duration)
    }
}

fn duration_round<T>(
    naive: NaiveDateTime,
    original: T,
    duration: TimeDelta,
) -> Result<T, RoundingError>
where
    T: Timelike + Add<TimeDelta, Output = T> + Sub<TimeDelta, Output = T>,
{
    if let Some(span) = duration.num_nanoseconds() {
        if span <= 0 {
            return Err(RoundingError::DurationExceedsLimit);
        }
        let stamp = naive
            .and_utc()
            .timestamp_nanos_opt()
            .ok_or(RoundingError::TimestampExceedsLimit)?;
        let delta_down = stamp % span;
        if delta_down == 0 {
            Ok(original)
        } else {
            let (delta_up, delta_down) = if delta_down < 0 {
                (delta_down.abs(), span - delta_down.abs())
            } else {
                (span - delta_down, delta_down)
            };
            if delta_up <= delta_down {
                Ok(original + TimeDelta::nanoseconds(delta_up))
            } else {
                Ok(original - TimeDelta::nanoseconds(delta_down))
            }
        }
    } else {
        Err(RoundingError::DurationExceedsLimit)
    }
}

fn duration_trunc<T>(
    naive: NaiveDateTime,
    original: T,
    duration: TimeDelta,
) -> Result<T, RoundingError>
where
    T: Timelike + Add<TimeDelta, Output = T> + Sub<TimeDelta, Output = T>,
{
    if let Some(span) = duration.num_nanoseconds() {
        if span <= 0 {
            return Err(RoundingError::DurationExceedsLimit);
        }
        let stamp = naive
            .and_utc()
            .timestamp_nanos_opt()
            .ok_or(RoundingError::TimestampExceedsLimit)?;
        let delta_down = stamp % span;
        match delta_down.cmp(&0) {
            Ordering::Equal => Ok(original),
            Ordering::Greater => Ok(original - TimeDelta::nanoseconds(delta_down)),
            Ordering::Less => Ok(original - TimeDelta::nanoseconds(span - delta_down.abs())),
        }
    } else {
        Err(RoundingError::DurationExceedsLimit)
    }
}

/// An error from rounding by `TimeDelta`
///
/// See: [`DurationRound`]
#[derive(Debug, Clone, PartialEq, Eq, Copy)]
pub enum RoundingError {
    /// Error when the TimeDelta exceeds the TimeDelta from or until the Unix epoch.
    ///
    /// Note: this error is not produced anymore.
    DurationExceedsTimestamp,

    /// Error when `TimeDelta.num_nanoseconds` exceeds the limit.
    ///
    /// ``` rust
    /// # use chrono::{DurationRound, TimeDelta, RoundingError, NaiveDate};
    /// let dt = NaiveDate::from_ymd_opt(2260, 12, 31)
    ///     .unwrap()
    ///     .and_hms_nano_opt(23, 59, 59, 1_75_500_000)
    ///     .unwrap()
    ///     .and_utc();
    ///
    /// assert_eq!(
    ///     dt.duration_round(TimeDelta::try_days(300 * 365).unwrap()),
    ///     Err(RoundingError::DurationExceedsLimit)
    /// );
    /// ```
    DurationExceedsLimit,

    /// Error when `DateTime.timestamp_nanos` exceeds the limit.
    ///
    /// ``` rust
    /// # use chrono::{DurationRound, TimeDelta, RoundingError, TimeZone, Utc};
    /// let dt = Utc.with_ymd_and_hms(2300, 12, 12, 0, 0, 0).unwrap();
    ///
    /// assert_eq!(
    ///     dt.duration_round(TimeDelta::try_days(1).unwrap()),
    ///     Err(RoundingError::TimestampExceedsLimit)
    /// );
    /// ```
    TimestampExceedsLimit,
}

impl fmt::Display for RoundingError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            RoundingError::DurationExceedsTimestamp => {
                write!(f, "duration in nanoseconds exceeds timestamp")
            }
            RoundingError::DurationExceedsLimit => {
                write!(f, "duration exceeds num_nanoseconds limit")
            }
            RoundingError::TimestampExceedsLimit => {
                write!(f, "timestamp exceeds num_nanoseconds limit")
            }
        }
    }
}

impl std::error::Error for RoundingError {
    #[allow(deprecated)]
    fn description(&self) -> &str {
        "error from rounding or truncating with DurationRound"
    }
}

#[cfg(test)]
mod tests {
    use super::{DurationRound, RoundingError, TimeDelta};
    use crate::offset::{FixedOffset, TimeZone, Utc};
    use crate::{DateTime, NaiveDate};

    #[test]
    fn test_duration_round() {
        let dt = Utc
            .from_local_datetime(
                &NaiveDate::from_ymd_opt(2016, 12, 31)
                    .unwrap()
                    .and_hms_nano_opt(23, 59, 59, 175_500_000)
                    .unwrap(),
            )
            .unwrap();

        assert_eq!(
            dt.duration_round(TimeDelta::new(-1, 0).unwrap()),
            Err(RoundingError::DurationExceedsLimit)
        );
        assert_eq!(
            dt.duration_round(TimeDelta::zero()),
            Err(RoundingError::DurationExceedsLimit)
        );

        assert_eq!(
            dt.duration_round(TimeDelta::try_milliseconds(10).unwrap())
                .unwrap()
                .to_string(),
            "2016-12-31 23:59:59.180 UTC"
        );

        // round up
        let dt = Utc
            .from_local_datetime(
                &NaiveDate::from_ymd_opt(2012, 12, 12)
                    .unwrap()
                    .and_hms_nano_opt(18, 22, 30, 0)
                    .unwrap(),
            )
            .unwrap();
        assert_eq!(
            dt.duration_round(TimeDelta::try_minutes(5).unwrap())
                .unwrap()
                .to_string(),
            "2012-12-12 18:25:00 UTC"
        );
        // round down
        let dt = Utc
            .from_local_datetime(
                &NaiveDate::from_ymd_opt(2012, 12, 12)
                    .unwrap()
                    .and_hms_nano_opt(18, 22, 29, 999_000_000)
                    .unwrap(),
            )
            .unwrap();
        assert_eq!(
            dt.duration_round(TimeDelta::try_minutes(5).unwrap())
                .unwrap()
                .to_string(),
            "2012-12-12 18:20:00 UTC"
        );

        assert_eq!(
            dt.duration_round(TimeDelta::try_minutes(10).unwrap())
                .unwrap()
                .to_string(),
            "2012-12-12 18:20:00 UTC"
        );
        assert_eq!(
            dt.duration_round(TimeDelta::try_minutes(30).unwrap())
                .unwrap()
                .to_string(),
            "2012-12-12 18:30:00 UTC"
        );
        assert_eq!(
            dt.duration_round(TimeDelta::try_hours(1).unwrap())
                .unwrap()
                .to_string(),
            "2012-12-12 18:00:00 UTC"
        );
        assert_eq!(
            dt.duration_round(TimeDelta::try_days(1).unwrap())
                .unwrap()
                .to_string(),
            "2012-12-13 00:00:00 UTC"
        );

        // timezone east
        let dt = FixedOffset::east_opt(3600)
            .unwrap()
            .with_ymd_and_hms(2020, 10, 27, 15, 0, 0)
            .unwrap();
        assert_eq!(
            dt.duration_round(TimeDelta::try_days(1).unwrap())
                .unwrap()
                .to_string(),
            "2020-10-28 00:00:00 +01:00"
        );
        assert_eq!(
            dt.duration_round(TimeDelta::try_days(7).unwrap())
                .unwrap()
                .to_string(),
            "2020-10-29 00:00:00 +01:00"
        );

        // timezone west
        let dt = FixedOffset::west_opt(3600)
            .unwrap()
            .with_ymd_and_hms(2020, 10, 27, 15, 0, 0)
            .unwrap();
        assert_eq!(
            dt.duration_round(TimeDelta::try_days(1).unwrap())
                .unwrap()
                .to_string(),
            "2020-10-28 00:00:00 -01:00"
        );
        assert_eq!(
            dt.duration_round(TimeDelta::try_days(7).unwrap())
                .unwrap()
                .to_string(),
            "2020-10-29 00:00:00 -01:00"
        );
    }

    #[test]
    fn test_duration_round_naive() {
        let dt = Utc
            .from_local_datetime(
                &NaiveDate::from_ymd_opt(2016, 12, 31)
                    .unwrap()
                    .and_hms_nano_opt(23, 59, 59, 175_500_000)
                    .unwrap(),
            )
            .unwrap()
            .naive_utc();

        assert_eq!(
            dt.duration_round(TimeDelta::new(-1, 0).unwrap()),
            Err(RoundingError::DurationExceedsLimit)
        );
        assert_eq!(
            dt.duration_round(TimeDelta::zero()),
            Err(RoundingError::DurationExceedsLimit)
        );

        assert_eq!(
            dt.duration_round(TimeDelta::try_milliseconds(10).unwrap())
                .unwrap()
                .to_string(),
            "2016-12-31 23:59:59.180"
        );

        // round up
        let dt = Utc
            .from_local_datetime(
                &NaiveDate::from_ymd_opt(2012, 12, 12)
                    .unwrap()
                    .and_hms_nano_opt(18, 22, 30, 0)
                    .unwrap(),
            )
            .unwrap()
            .naive_utc();
        assert_eq!(
            dt.duration_round(TimeDelta::try_minutes(5).unwrap())
                .unwrap()
                .to_string(),
            "2012-12-12 18:25:00"
        );
        // round down
        let dt = Utc
            .from_local_datetime(
                &NaiveDate::from_ymd_opt(2012, 12, 12)
                    .unwrap()
                    .and_hms_nano_opt(18, 22, 29, 999_000_000)
                    .unwrap(),
            )
            .unwrap()
            .naive_utc();
        assert_eq!(
            dt.duration_round(TimeDelta::try_minutes(5).unwrap())
                .unwrap()
                .to_string(),
            "2012-12-12 18:20:00"
        );

        assert_eq!(
            dt.duration_round(TimeDelta::try_minutes(10).unwrap())
                .unwrap()
                .to_string(),
            "2012-12-12 18:20:00"
        );
        assert_eq!(
            dt.duration_round(TimeDelta::try_minutes(30).unwrap())
                .unwrap()
                .to_string(),
            "2012-12-12 18:30:00"
        );
        assert_eq!(
            dt.duration_round(TimeDelta::try_hours(1).unwrap())
                .unwrap()
                .to_string(),
            "2012-12-12 18:00:00"
        );
        assert_eq!(
            dt.duration_round(TimeDelta::try_days(1).unwrap())
                .unwrap()
                .to_string(),
            "2012-12-13 00:00:00"
        );
    }

    #[test]
    fn test_duration_round_pre_epoch() {
        let dt = Utc.with_ymd_and_hms(1969, 12, 12, 12, 12, 12).unwrap();
        assert_eq!(
            dt.duration_round(TimeDelta::try_minutes(10).unwrap())
                .unwrap()
                .to_string(),
            "1969-12-12 12:10:00 UTC"
        );
    }

    #[test]
    fn test_duration_trunc() {
        let dt = Utc
            .from_local_datetime(
                &NaiveDate::from_ymd_opt(2016, 12, 31)
                    .unwrap()
                    .and_hms_nano_opt(23, 59, 59, 175_500_000)
                    .unwrap(),
            )
            .unwrap();

        assert_eq!(
            dt.duration_trunc(TimeDelta::new(-1, 0).unwrap()),
            Err(RoundingError::DurationExceedsLimit)
        );
        assert_eq!(
            dt.duration_trunc(TimeDelta::zero()),
            Err(RoundingError::DurationExceedsLimit)
        );

        assert_eq!(
            dt.duration_trunc(TimeDelta::try_milliseconds(10).unwrap())
                .unwrap()
                .to_string(),
            "2016-12-31 23:59:59.170 UTC"
        );

        // would round up
        let dt = Utc
            .from_local_datetime(
                &NaiveDate::from_ymd_opt(2012, 12, 12)
                    .unwrap()
                    .and_hms_nano_opt(18, 22, 30, 0)
                    .unwrap(),
            )
            .unwrap();
        assert_eq!(
            dt.duration_trunc(TimeDelta::try_minutes(5).unwrap())
                .unwrap()
                .to_string(),
            "2012-12-12 18:20:00 UTC"
        );
        // would round down
        let dt = Utc
            .from_local_datetime(
                &NaiveDate::from_ymd_opt(2012, 12, 12)
                    .unwrap()
                    .and_hms_nano_opt(18, 22, 29, 999_000_000)
                    .unwrap(),
            )
            .unwrap();
        assert_eq!(
            dt.duration_trunc(TimeDelta::try_minutes(5).unwrap())
                .unwrap()
                .to_string(),
            "2012-12-12 18:20:00 UTC"
        );
        assert_eq!(
            dt.duration_trunc(TimeDelta::try_minutes(10).unwrap())
                .unwrap()
                .to_string(),
            "2012-12-12 18:20:00 UTC"
        );
        assert_eq!(
            dt.duration_trunc(TimeDelta::try_minutes(30).unwrap())
                .unwrap()
                .to_string(),
            "2012-12-12 18:00:00 UTC"
        );
        assert_eq!(
            dt.duration_trunc(TimeDelta::try_hours(1).unwrap())
                .unwrap()
                .to_string(),
            "2012-12-12 18:00:00 UTC"
        );
        assert_eq!(
            dt.duration_trunc(TimeDelta::try_days(1).unwrap())
                .unwrap()
                .to_string(),
            "2012-12-12 00:00:00 UTC"
        );

        // timezone east
        let dt = FixedOffset::east_opt(3600)
            .unwrap()
            .with_ymd_and_hms(2020, 10, 27, 15, 0, 0)
            .unwrap();
        assert_eq!(
            dt.duration_trunc(TimeDelta::try_days(1).unwrap())
                .unwrap()
                .to_string(),
            "2020-10-27 00:00:00 +01:00"
        );
        assert_eq!(
            dt.duration_trunc(TimeDelta::try_days(7).unwrap())
                .unwrap()
                .to_string(),
            "2020-10-22 00:00:00 +01:00"
        );

        // timezone west
        let dt = FixedOffset::west_opt(3600)
            .unwrap()
            .with_ymd_and_hms(2020, 10, 27, 15, 0, 0)
            .unwrap();
        assert_eq!(
            dt.duration_trunc(TimeDelta::try_days(1).unwrap())
                .unwrap()
                .to_string(),
            "2020-10-27 00:00:00 -01:00"
        );
        assert_eq!(
            dt.duration_trunc(TimeDelta::try_days(7).unwrap())
                .unwrap()
                .to_string(),
            "2020-10-22 00:00:00 -01:00"
        );
    }

    #[test]
    fn test_duration_trunc_naive() {
        let dt = Utc
            .from_local_datetime(
                &NaiveDate::from_ymd_opt(2016, 12, 31)
                    .unwrap()
                    .and_hms_nano_opt(23, 59, 59, 175_500_000)
                    .unwrap(),
            )
            .unwrap()
            .naive_utc();

        assert_eq!(
            dt.duration_trunc(TimeDelta::new(-1, 0).unwrap()),
            Err(RoundingError::DurationExceedsLimit)
        );
        assert_eq!(
            dt.duration_trunc(TimeDelta::zero()),
            Err(RoundingError::DurationExceedsLimit)
        );

        assert_eq!(
            dt.duration_trunc(TimeDelta::try_milliseconds(10).unwrap())
                .unwrap()
                .to_string(),
            "2016-12-31 23:59:59.170"
        );

        // would round up
        let dt = Utc
            .from_local_datetime(
                &NaiveDate::from_ymd_opt(2012, 12, 12)
                    .unwrap()
                    .and_hms_nano_opt(18, 22, 30, 0)
                    .unwrap(),
            )
            .unwrap()
            .naive_utc();
        assert_eq!(
            dt.duration_trunc(TimeDelta::try_minutes(5).unwrap())
                .unwrap()
                .to_string(),
            "2012-12-12 18:20:00"
        );
        // would round down
        let dt = Utc
            .from_local_datetime(
                &NaiveDate::from_ymd_opt(2012, 12, 12)
                    .unwrap()
                    .and_hms_nano_opt(18, 22, 29, 999_000_000)
                    .unwrap(),
            )
            .unwrap()
            .naive_utc();
        assert_eq!(
            dt.duration_trunc(TimeDelta::try_minutes(5).unwrap())
                .unwrap()
                .to_string(),
            "2012-12-12 18:20:00"
        );
        assert_eq!(
            dt.duration_trunc(TimeDelta::try_minutes(10).unwrap())
                .unwrap()
                .to_string(),
            "2012-12-12 18:20:00"
        );
        assert_eq!(
            dt.duration_trunc(TimeDelta::try_minutes(30).unwrap())
                .unwrap()
                .to_string(),
            "2012-12-12 18:00:00"
        );
        assert_eq!(
            dt.duration_trunc(TimeDelta::try_hours(1).unwrap())
                .unwrap()
                .to_string(),
            "2012-12-12 18:00:00"
        );
        assert_eq!(
            dt.duration_trunc(TimeDelta::try_days(1).unwrap())
                .unwrap()
                .to_string(),
            "2012-12-12 00:00:00"
        );
    }

    #[test]
    fn test_duration_trunc_pre_epoch() {
        let dt = Utc.with_ymd_and_hms(1969, 12, 12, 12, 12, 12).unwrap();
        assert_eq!(
            dt.duration_trunc(TimeDelta::try_minutes(10).unwrap())
                .unwrap()
                .to_string(),
            "1969-12-12 12:10:00 UTC"
        );
    }

    #[test]
    fn issue1010() {
        let dt = DateTime::from_timestamp(-4_227_854_320, 678_774_288).unwrap();
        let span = TimeDelta::microseconds(-7_019_067_213_869_040);
        assert_eq!(
            dt.duration_trunc(span),
            Err(RoundingError::DurationExceedsLimit)
        );

        let dt = DateTime::from_timestamp(320_041_586, 920_103_021).unwrap();
        let span = TimeDelta::nanoseconds(-8_923_838_508_697_114_584);
        assert_eq!(
            dt.duration_round(span),
            Err(RoundingError::DurationExceedsLimit)
        );

        let dt = DateTime::from_timestamp(-2_621_440, 0).unwrap();
        let span = TimeDelta::nanoseconds(-9_223_372_036_854_771_421);
        assert_eq!(
            dt.duration_round(span),
            Err(RoundingError::DurationExceedsLimit)
        );
    }

    #[test]
    fn test_duration_trunc_close_to_epoch() {
        let span = TimeDelta::try_minutes(15).unwrap();

        let dt = NaiveDate::from_ymd_opt(1970, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 15)
            .unwrap();
        assert_eq!(
            dt.duration_trunc(span).unwrap().to_string(),
            "1970-01-01 00:00:00"
        );

        let dt = NaiveDate::from_ymd_opt(1969, 12, 31)
            .unwrap()
            .and_hms_opt(23, 59, 45)
            .unwrap();
        assert_eq!(
            dt.duration_trunc(span).unwrap().to_string(),
            "1969-12-31 23:45:00"
        );
    }

    #[test]
    fn test_duration_round_close_to_epoch() {
        let span = TimeDelta::try_minutes(15).unwrap();

        let dt = NaiveDate::from_ymd_opt(1970, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 15)
            .unwrap();
        assert_eq!(
            dt.duration_round(span).unwrap().to_string(),
            "1970-01-01 00:00:00"
        );

        let dt = NaiveDate::from_ymd_opt(1969, 12, 31)
            .unwrap()
            .and_hms_opt(23, 59, 45)
            .unwrap();
        assert_eq!(
            dt.duration_round(span).unwrap().to_string(),
            "1970-01-01 00:00:00"
        );
    }

    #[test]
    fn test_duration_round_close_to_min_max() {
        let span = TimeDelta::nanoseconds(i64::MAX);

        let dt = DateTime::from_timestamp_nanos(i64::MIN / 2 - 1);
        assert_eq!(
            dt.duration_round(span).unwrap().to_string(),
            "1677-09-21 00:12:43.145224193 UTC"
        );

        let dt = DateTime::from_timestamp_nanos(i64::MIN / 2 + 1);
        assert_eq!(
            dt.duration_round(span).unwrap().to_string(),
            "1970-01-01 00:00:00 UTC"
        );

        let dt = DateTime::from_timestamp_nanos(i64::MAX / 2 + 1);
        assert_eq!(
            dt.duration_round(span).unwrap().to_string(),
            "2262-04-11 23:47:16.854775807 UTC"
        );

        let dt = DateTime::from_timestamp_nanos(i64::MAX / 2 - 1);
        assert_eq!(
            dt.duration_round(span).unwrap().to_string(),
            "1970-01-01 00:00:00 UTC"
        );
    }
}
