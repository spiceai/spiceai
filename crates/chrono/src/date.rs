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

//! ISO 8601 calendar date with time zone.
#![allow(deprecated)]

use core::cmp::Ordering;
use core::ops::{Add, AddAssign, Sub, SubAssign};
use core::{fmt, hash};

use crate::naive::{IsoWeek, NaiveDate, NaiveTime};
use crate::offset::TimeZone;
use crate::{DateTime, Datelike, TimeDelta, Weekday};

/// ISO 8601 calendar date with time zone.
///
/// You almost certainly want to be using a [`NaiveDate`] instead of this type.
///
/// This type primarily exists to aid in the construction of DateTimes that
/// have a timezone by way of the [`TimeZone`] datelike constructors (e.g.
/// [`TimeZone::ymd_opt`]).
///
/// This type should be considered ambiguous at best, due to the inherent lack
/// of precision required for the time zone resolution.
///
/// There are some guarantees on the usage of `Date<Tz>`:
///
/// - If properly constructed via [`TimeZone::ymd_opt`] and others without an error,
///   the corresponding local date should exist for at least a moment.
///   (It may still have a gap from the offset changes.)
///
/// - The `TimeZone` is free to assign *any* [`Offset`](crate::offset::Offset) to the
///   local date, as long as that offset did occur in given day.
///
///   For example, if `2015-03-08T01:59-08:00` is followed by `2015-03-08T03:00-07:00`,
///   it may produce either `2015-03-08-08:00` or `2015-03-08-07:00`
///   but *not* `2015-03-08+00:00` and others.
///
/// - Once constructed as a full `DateTime`, its date should be the original `Date`.
///
/// - The date is timezone-agnostic up to one day (i.e. practically always),
///   so the local date and UTC date should be equal for most cases
///   even though the raw calculation between `NaiveDate` and `TimeDelta` may not.
#[deprecated(since = "0.4.23", note = "Use `NaiveDate` or `DateTime<Tz>` instead")]
#[derive(Clone)]
pub struct Date<Tz: TimeZone> {
    date: NaiveDate,
    offset: Tz::Offset,
}

impl<Tz: TimeZone> Date<Tz> {
    /// Makes a new `Date` with given *UTC* date and offset.
    /// The local date should be constructed via the `TimeZone` trait.
    #[inline]
    #[must_use]
    pub fn from_utc(date: NaiveDate, offset: Tz::Offset) -> Date<Tz> {
        Date { date, offset }
    }

    /// Makes a new `DateTime` from the current date and given `NaiveTime`.
    /// The offset in the current date is preserved.
    ///
    /// Returns `None` on invalid datetime.
    #[inline]
    #[must_use]
    pub fn and_time(&self, time: NaiveTime) -> Option<DateTime<Tz>> {
        let localdt = self.naive_local().and_time(time);
        self.timezone().from_local_datetime(&localdt).single()
    }

    /// Makes a new `DateTime` from the current date, hour, minute, second and nanosecond.
    /// The nanosecond part can exceed 1,000,000,000 in order to represent the leap second.
    /// The offset in the current date is preserved.
    ///
    /// Returns `None` on invalid hour, minute, second and/or nanosecond.
    #[inline]
    #[must_use]
    pub fn and_hms_nano_opt(
        &self,
        hour: u32,
        min: u32,
        sec: u32,
        nano: u32,
    ) -> Option<DateTime<Tz>> {
        NaiveTime::from_hms_nano_opt(hour, min, sec, nano).and_then(|time| self.and_time(time))
    }

    /// Retrieves an associated offset from UTC.
    #[inline]
    #[must_use]
    pub fn offset(&self) -> &Tz::Offset {
        &self.offset
    }

    /// Retrieves an associated time zone.
    #[inline]
    #[must_use]
    pub fn timezone(&self) -> Tz {
        TimeZone::from_offset(&self.offset)
    }

    /// Adds given `TimeDelta` to the current date.
    ///
    /// Returns `None` when it will result in overflow.
    #[inline]
    #[must_use]
    pub fn checked_add_signed(self, rhs: TimeDelta) -> Option<Date<Tz>> {
        let date = self.date.checked_add_signed(rhs)?;
        Some(Date {
            date,
            offset: self.offset,
        })
    }

    /// Subtracts given `TimeDelta` from the current date.
    ///
    /// Returns `None` when it will result in overflow.
    #[inline]
    #[must_use]
    pub fn checked_sub_signed(self, rhs: TimeDelta) -> Option<Date<Tz>> {
        let date = self.date.checked_sub_signed(rhs)?;
        Some(Date {
            date,
            offset: self.offset,
        })
    }

    /// Subtracts another `Date` from the current date.
    /// Returns a `TimeDelta` of integral numbers.
    ///
    /// This does not overflow or underflow at all,
    /// as all possible output fits in the range of `TimeDelta`.
    #[inline]
    #[must_use]
    pub fn signed_duration_since<Tz2: TimeZone>(self, rhs: Date<Tz2>) -> TimeDelta {
        self.date.signed_duration_since(rhs.date)
    }

    /// Returns a view to the naive local date.
    ///
    /// This is technically the same as [`naive_utc`](#method.naive_utc)
    /// because the offset is restricted to never exceed one day,
    /// but provided for the consistency.
    #[inline]
    #[must_use]
    pub fn naive_local(&self) -> NaiveDate {
        self.date
    }
}

/// Maps the local date to other date with given conversion function.
fn map_local<Tz: TimeZone, F>(d: &Date<Tz>, mut f: F) -> Option<Date<Tz>>
where
    F: FnMut(NaiveDate) -> Option<NaiveDate>,
{
    f(d.naive_local()).and_then(|date| d.timezone().from_local_date(&date).single())
}

impl<Tz: TimeZone> Date<Tz> where Tz::Offset: fmt::Display {}

impl<Tz: TimeZone> Datelike for Date<Tz> {
    #[inline]
    fn year(&self) -> i32 {
        self.naive_local().year()
    }
    #[inline]
    fn month(&self) -> u32 {
        self.naive_local().month()
    }
    #[inline]
    fn month0(&self) -> u32 {
        self.naive_local().month0()
    }
    #[inline]
    fn day(&self) -> u32 {
        self.naive_local().day()
    }
    #[inline]
    fn ordinal(&self) -> u32 {
        self.naive_local().ordinal()
    }
    #[inline]
    fn weekday(&self) -> Weekday {
        self.naive_local().weekday()
    }
    #[inline]
    fn iso_week(&self) -> IsoWeek {
        self.naive_local().iso_week()
    }

    #[inline]
    fn with_year(&self, year: i32) -> Option<Date<Tz>> {
        map_local(self, |date| date.with_year(year))
    }

    #[inline]
    fn with_month(&self, month: u32) -> Option<Date<Tz>> {
        map_local(self, |date| date.with_month(month))
    }

    #[inline]
    fn with_month0(&self, month0: u32) -> Option<Date<Tz>> {
        map_local(self, |date| date.with_month0(month0))
    }

    #[inline]
    fn with_day(&self, day: u32) -> Option<Date<Tz>> {
        map_local(self, |date| date.with_day(day))
    }

    #[inline]
    fn with_day0(&self, day0: u32) -> Option<Date<Tz>> {
        map_local(self, |date| date.with_day0(day0))
    }

    #[inline]
    fn with_ordinal(&self, ordinal: u32) -> Option<Date<Tz>> {
        map_local(self, |date| date.with_ordinal(ordinal))
    }
}

// we need them as automatic impls cannot handle associated types
impl<Tz: TimeZone> Copy for Date<Tz> where <Tz as TimeZone>::Offset: Copy {}
unsafe impl<Tz: TimeZone> Send for Date<Tz> where <Tz as TimeZone>::Offset: Send {}

impl<Tz: TimeZone, Tz2: TimeZone> PartialEq<Date<Tz2>> for Date<Tz> {
    fn eq(&self, other: &Date<Tz2>) -> bool {
        self.date == other.date
    }
}

impl<Tz: TimeZone> Eq for Date<Tz> {}

impl<Tz: TimeZone> PartialOrd for Date<Tz> {
    fn partial_cmp(&self, other: &Date<Tz>) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<Tz: TimeZone> Ord for Date<Tz> {
    fn cmp(&self, other: &Date<Tz>) -> Ordering {
        self.date.cmp(&other.date)
    }
}

impl<Tz: TimeZone> hash::Hash for Date<Tz> {
    fn hash<H: hash::Hasher>(&self, state: &mut H) {
        self.date.hash(state)
    }
}

impl<Tz: TimeZone> Add<TimeDelta> for Date<Tz> {
    type Output = Date<Tz>;

    #[inline]
    #[track_caller]
    fn add(self, rhs: TimeDelta) -> Date<Tz> {
        self.checked_add_signed(rhs)
            .expect("`Date + TimeDelta` overflowed")
    }
}

impl<Tz: TimeZone> AddAssign<TimeDelta> for Date<Tz> {
    #[inline]
    #[track_caller]
    fn add_assign(&mut self, rhs: TimeDelta) {
        self.date = self
            .date
            .checked_add_signed(rhs)
            .expect("`Date + TimeDelta` overflowed");
    }
}

impl<Tz: TimeZone> Sub<TimeDelta> for Date<Tz> {
    type Output = Date<Tz>;

    #[inline]
    #[track_caller]
    fn sub(self, rhs: TimeDelta) -> Date<Tz> {
        self.checked_sub_signed(rhs)
            .expect("`Date - TimeDelta` overflowed")
    }
}

impl<Tz: TimeZone> SubAssign<TimeDelta> for Date<Tz> {
    #[inline]
    #[track_caller]
    fn sub_assign(&mut self, rhs: TimeDelta) {
        self.date = self
            .date
            .checked_sub_signed(rhs)
            .expect("`Date - TimeDelta` overflowed");
    }
}

impl<Tz: TimeZone> Sub<Date<Tz>> for Date<Tz> {
    type Output = TimeDelta;

    #[inline]
    fn sub(self, rhs: Date<Tz>) -> TimeDelta {
        self.signed_duration_since(rhs)
    }
}

impl<Tz: TimeZone> fmt::Debug for Date<Tz> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.naive_local().fmt(f)?;
        self.offset.fmt(f)
    }
}

impl<Tz: TimeZone> fmt::Display for Date<Tz>
where
    Tz::Offset: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.naive_local().fmt(f)?;
        self.offset.fmt(f)
    }
}

// Note that implementation of Arbitrary cannot be automatically derived for Date<Tz>, due to
// the nontrivial bound <Tz as TimeZone>::Offset: Arbitrary.

#[cfg(test)]
mod tests {
    use super::Date;

    use crate::{NaiveDate, TimeDelta, Utc};

    #[test]
    fn test_date_add_assign() {
        let naivedate = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let date = Date::<Utc>::from_utc(naivedate, Utc);
        let mut date_add = date;

        date_add += TimeDelta::days(5);
        assert_eq!(date_add, date + TimeDelta::days(5));
    }

    #[test]
    fn test_date_sub_assign() {
        let naivedate = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let date = Date::<Utc>::from_utc(naivedate, Utc);
        let mut date_sub = date;

        date_sub -= TimeDelta::days(5);
        assert_eq!(date_sub, date - TimeDelta::days(5));
    }
}
