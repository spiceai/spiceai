/*
Copyright 2024-2026 The Spice.ai OSS Authors
Copyright (c) 2016-2025 The chrono-tz contributors

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

//! IANA time zones for [`chrono`]: the part of chrono-tz 0.10.4 that Spice.ai and its
//! dependencies use, with tables `build.rs` generates from the tz database release in `tz/`.
//! `README.md` says why the crate exists and how to update the tz database.
//!
//! [`Tz`] implements [`chrono::TimeZone`], and parses from an IANA time zone name:
//!
//! ```
//! use chrono::{DateTime, Utc};
//! use chrono_tz::Tz;
//!
//! let london: Tz = "Europe/London".parse().expect("an IANA time zone name");
//! let utc: DateTime<Utc> = "2016-05-10T11:00:00Z".parse().expect("an RFC 3339 instant");
//! assert_eq!(utc.with_timezone(&london).to_string(), "2016-05-10 12:00:00 BST");
//! assert_eq!(utc.with_timezone(&london).to_rfc3339(), "2016-05-10T12:00:00+01:00");
//! ```

// The workspace lints apply to this crate. The expectations below name the ones the
// chrono-tz 0.10.4 code trips, so the code stays as upstream wrote and tested it and new code
// is still held to every other lint. An expectation that stops firing fails the build, so
// the list only ever shrinks.
#![expect(
    clippy::cast_lossless,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "offset arithmetic converts between integer widths inside the range of tz database offsets"
)]
#![expect(
    clippy::unwrap_used,
    reason = "every instant has an offset and every tz database offset is a valid `FixedOffset`, as upstream"
)]
#![expect(
    clippy::allow_attributes,
    clippy::doc_markdown,
    clippy::items_after_statements,
    clippy::match_same_arms,
    reason = "upstream chrono-tz 0.10.4 code, kept as written"
)]
#![cfg_attr(
    not(test),
    expect(
        clippy::enum_glob_use,
        reason = "upstream chrono-tz 0.10.4 code, kept as written; this fires only outside test builds"
    )
)]
#![cfg_attr(
    test,
    expect(
        clippy::ignore_without_reason,
        clippy::needless_collect,
        clippy::needless_pass_by_value,
        reason = "upstream chrono-tz 0.10.4 tests, kept as written"
    )
)]

mod binary_search;
mod timezone_impl;
mod timezones {
    #![expect(
        non_camel_case_types,
        clippy::cloned_instead_of_copied,
        clippy::doc_markdown,
        clippy::match_same_arms,
        clippy::must_use_candidate,
        clippy::unreadable_literal,
        reason = "the tables build.rs writes, in the form chrono-tz-build 0.5.0 writes them"
    )]
    include!(concat!(env!("OUT_DIR"), "/timezones.rs"));
}

pub use crate::timezone_impl::TzOffset;
pub use timezones::ParseError;
pub use timezones::Tz;

/// The `UTC` zone.
pub const UTC: Tz = Tz::UTC;

// The `Europe::London`-style constant for every zone, and `IANA_TZDB_VERSION`, which
// upstream's tests name zones and the release by.
#[cfg(test)]
mod directory {
    #![expect(
        dead_code,
        non_snake_case,
        non_upper_case_globals,
        reason = "a constant for every zone, named as the tz database names it"
    )]
    include!(concat!(env!("OUT_DIR"), "/directory.rs"));
}
#[cfg(test)]
use directory::*;

#[cfg(test)]
mod numberphile;
#[cfg(test)]
mod system_tzdb;

#[cfg(test)]
mod tests {
    use super::Africa::Addis_Ababa;
    use super::America::Danmarkshavn;
    use super::America::Scoresbysund;
    use super::Antarctica::Casey;
    use super::Asia::Dhaka;
    use super::Australia::Adelaide;
    use super::Europe::Berlin;
    use super::Europe::London;
    use super::Europe::Moscow;
    use super::Europe::Vilnius;
    use super::Europe::Warsaw;
    use super::IANA_TZDB_VERSION;
    use super::Pacific::Apia;
    use super::Pacific::Noumea;
    use super::Pacific::Tahiti;
    use super::Tz;
    use super::US::Eastern;
    use super::UTC;
    use chrono::{Duration, NaiveDate, TimeZone};

    #[test]
    fn london_to_berlin() {
        let dt = London.with_ymd_and_hms(2016, 10, 8, 17, 0, 0).unwrap();
        let converted = dt.with_timezone(&Berlin);
        let expected = Berlin.with_ymd_and_hms(2016, 10, 8, 18, 0, 0).unwrap();
        assert_eq!(converted, expected);
    }

    #[test]
    fn us_eastern_dst_commutativity() {
        let dt = UTC.with_ymd_and_hms(2002, 4, 7, 7, 0, 0).unwrap();
        for days in -420..720 {
            let dt1 = (dt + Duration::days(days)).with_timezone(&Eastern);
            let dt2 = dt.with_timezone(&Eastern) + Duration::days(days);
            assert_eq!(dt1, dt2);
        }
    }

    #[test]
    fn test_addition_across_dst_boundary() {
        use chrono::TimeZone;
        let two_hours = Duration::hours(2);
        let edt = Eastern.with_ymd_and_hms(2019, 11, 3, 0, 0, 0).unwrap();
        let est = edt + two_hours;

        assert_eq!(edt.to_string(), "2019-11-03 00:00:00 EDT".to_string());
        assert_eq!(est.to_string(), "2019-11-03 01:00:00 EST".to_string());
        assert_eq!(est.timestamp(), edt.timestamp() + two_hours.num_seconds());
    }

    #[test]
    fn warsaw_tz_name() {
        let dt = UTC.with_ymd_and_hms(1915, 8, 4, 22, 35, 59).unwrap();
        assert_eq!(dt.with_timezone(&Warsaw).format("%Z").to_string(), "WMT");
        let dt = dt + Duration::seconds(1);
        assert_eq!(dt.with_timezone(&Warsaw).format("%Z").to_string(), "CET");
    }

    #[test]
    fn vilnius_utc_offset() {
        let dt = UTC
            .with_ymd_and_hms(1916, 12, 31, 22, 35, 59)
            .unwrap()
            .with_timezone(&Vilnius);
        assert_eq!(
            dt,
            Vilnius.with_ymd_and_hms(1916, 12, 31, 23, 59, 59).unwrap()
        );
        let dt = dt + Duration::seconds(1);
        assert_eq!(dt, Vilnius.with_ymd_and_hms(1917, 1, 1, 0, 11, 36).unwrap());
    }

    #[test]
    fn victorian_times() {
        let dt = UTC
            .with_ymd_and_hms(1847, 12, 1, 0, 1, 14)
            .unwrap()
            .with_timezone(&London);
        assert_eq!(
            dt,
            London.with_ymd_and_hms(1847, 11, 30, 23, 59, 59).unwrap()
        );
        let dt = dt + Duration::seconds(1);
        assert_eq!(dt, London.with_ymd_and_hms(1847, 12, 1, 0, 1, 15).unwrap());
    }

    #[test]
    fn london_dst() {
        let dt = London.with_ymd_and_hms(2016, 3, 10, 5, 0, 0).unwrap();
        let later = dt + Duration::days(180);
        let expected = London.with_ymd_and_hms(2016, 9, 6, 6, 0, 0).unwrap();
        assert_eq!(later, expected);
    }

    #[test]
    fn international_date_line_change() {
        let dt = UTC
            .with_ymd_and_hms(2011, 12, 30, 9, 59, 59)
            .unwrap()
            .with_timezone(&Apia);
        assert_eq!(dt, Apia.with_ymd_and_hms(2011, 12, 29, 23, 59, 59).unwrap());
        let dt = dt + Duration::seconds(1);
        assert_eq!(dt, Apia.with_ymd_and_hms(2011, 12, 31, 0, 0, 0).unwrap());
    }

    #[test]
    fn negative_offset_with_minutes_and_seconds() {
        let dt = UTC
            .with_ymd_and_hms(1900, 1, 1, 12, 0, 0)
            .unwrap()
            .with_timezone(&Danmarkshavn);
        assert_eq!(
            dt,
            Danmarkshavn
                .with_ymd_and_hms(1900, 1, 1, 10, 45, 20)
                .unwrap()
        );
    }

    #[test]
    fn monotonicity() {
        let mut dt = Noumea.with_ymd_and_hms(1800, 1, 1, 12, 0, 0).unwrap();
        for _ in 0..24 * 356 * 400 {
            let new = dt + Duration::hours(1);
            assert!(new > dt);
            assert!(new.with_timezone(&UTC) > dt.with_timezone(&UTC));
            dt = new;
        }
    }

    fn test_inverse<T: TimeZone>(tz: T, begin: i32, end: i32) {
        for y in begin..end {
            for d in 1..366 {
                let date = NaiveDate::from_yo_opt(y, d).unwrap();
                for h in 0..24 {
                    for m in 0..60 {
                        let dt = date.and_hms_opt(h, m, 0).unwrap().and_utc();
                        let with_tz = dt.with_timezone(&tz);
                        let utc = with_tz.with_timezone(&UTC);
                        assert_eq!(dt, utc);
                    }
                }
            }
        }
    }

    #[test]
    fn inverse_london() {
        test_inverse(London, 1989, 1994);
    }

    #[test]
    fn inverse_dhaka() {
        test_inverse(Dhaka, 1995, 2000);
    }

    #[test]
    fn inverse_apia() {
        test_inverse(Apia, 2011, 2012);
    }

    #[test]
    fn inverse_tahiti() {
        test_inverse(Tahiti, 1911, 1914);
    }

    #[test]
    fn string_representation() {
        let dt = UTC
            .with_ymd_and_hms(2000, 9, 1, 12, 30, 15)
            .unwrap()
            .with_timezone(&Adelaide);
        assert_eq!(dt.to_string(), "2000-09-01 22:00:15 ACST");
        assert_eq!(format!("{dt:?}"), "2000-09-01T22:00:15ACST");
        assert_eq!(dt.to_rfc3339(), "2000-09-01T22:00:15+09:30");
        assert_eq!(format!("{dt}"), "2000-09-01 22:00:15 ACST");
    }

    #[test]
    fn tahiti() {
        let dt = UTC
            .with_ymd_and_hms(1912, 10, 1, 9, 58, 16)
            .unwrap()
            .with_timezone(&Tahiti);
        let before = dt - Duration::hours(1);
        assert_eq!(
            before,
            Tahiti.with_ymd_and_hms(1912, 9, 30, 23, 0, 0).unwrap()
        );
        let after = dt + Duration::hours(1);
        assert_eq!(
            after,
            Tahiti.with_ymd_and_hms(1912, 10, 1, 0, 58, 16).unwrap()
        );
    }

    #[test]
    fn nonexistent_time() {
        assert!(
            London
                .with_ymd_and_hms(2016, 3, 27, 1, 30, 0)
                .single()
                .is_none()
        );
    }

    #[test]
    fn nonexistent_time_2() {
        assert!(
            London
                .with_ymd_and_hms(2016, 3, 27, 1, 0, 0)
                .single()
                .is_none()
        );
    }

    #[test]
    fn time_exists() {
        assert!(
            London
                .with_ymd_and_hms(2016, 3, 27, 2, 0, 0)
                .single()
                .is_some()
        );
    }

    #[test]
    fn ambiguous_time() {
        let ambiguous = London.with_ymd_and_hms(2016, 10, 30, 1, 0, 0);
        let earliest_utc = NaiveDate::from_ymd_opt(2016, 10, 30)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        assert_eq!(
            ambiguous.earliest().unwrap(),
            London.from_utc_datetime(&earliest_utc)
        );
        let latest_utc = NaiveDate::from_ymd_opt(2016, 10, 30)
            .unwrap()
            .and_hms_opt(1, 0, 0)
            .unwrap();
        assert_eq!(
            ambiguous.latest().unwrap(),
            London.from_utc_datetime(&latest_utc)
        );
    }

    #[test]
    fn ambiguous_time_2() {
        let ambiguous = London.with_ymd_and_hms(2016, 10, 30, 1, 30, 0);
        let earliest_utc = NaiveDate::from_ymd_opt(2016, 10, 30)
            .unwrap()
            .and_hms_opt(0, 30, 0)
            .unwrap();
        assert_eq!(
            ambiguous.earliest().unwrap(),
            London.from_utc_datetime(&earliest_utc)
        );
        let latest_utc = NaiveDate::from_ymd_opt(2016, 10, 30)
            .unwrap()
            .and_hms_opt(1, 30, 0)
            .unwrap();
        assert_eq!(
            ambiguous.latest().unwrap(),
            London.from_utc_datetime(&latest_utc)
        );
    }

    #[test]
    fn ambiguous_time_3() {
        let ambiguous = Moscow.with_ymd_and_hms(2014, 10, 26, 1, 30, 0);
        let earliest_utc = NaiveDate::from_ymd_opt(2014, 10, 25)
            .unwrap()
            .and_hms_opt(21, 30, 0)
            .unwrap();
        assert_eq!(
            ambiguous.earliest().unwrap().fixed_offset(),
            Moscow.from_utc_datetime(&earliest_utc).fixed_offset()
        );
        let latest_utc = NaiveDate::from_ymd_opt(2014, 10, 25)
            .unwrap()
            .and_hms_opt(22, 30, 0)
            .unwrap();
        assert_eq!(
            ambiguous.latest().unwrap(),
            Moscow.from_utc_datetime(&latest_utc)
        );
    }

    #[test]
    fn ambiguous_time_4() {
        let ambiguous = Moscow.with_ymd_and_hms(2014, 10, 26, 1, 0, 0);
        let earliest_utc = NaiveDate::from_ymd_opt(2014, 10, 25)
            .unwrap()
            .and_hms_opt(21, 0, 0)
            .unwrap();
        assert_eq!(
            ambiguous.earliest().unwrap().fixed_offset(),
            Moscow.from_utc_datetime(&earliest_utc).fixed_offset()
        );
        let latest_utc = NaiveDate::from_ymd_opt(2014, 10, 25)
            .unwrap()
            .and_hms_opt(22, 0, 0)
            .unwrap();
        assert_eq!(
            ambiguous.latest().unwrap(),
            Moscow.from_utc_datetime(&latest_utc)
        );
    }

    #[test]
    fn unambiguous_time() {
        assert!(
            London
                .with_ymd_and_hms(2016, 10, 30, 2, 0, 0)
                .single()
                .is_some()
        );
    }

    #[test]
    fn unambiguous_time_2() {
        assert!(
            Moscow
                .with_ymd_and_hms(2014, 10, 26, 2, 0, 0)
                .single()
                .is_some()
        );
    }

    #[test]
    fn test_get_name() {
        assert_eq!(London.name(), "Europe/London");
        assert_eq!(Tz::Africa__Abidjan.name(), "Africa/Abidjan");
        assert_eq!(Tz::UTC.name(), "UTC");
        assert_eq!(Tz::Zulu.name(), "Zulu");
    }

    #[test]
    fn test_display() {
        assert_eq!(format!("{London}"), "Europe/London");
        assert_eq!(format!("{}", Tz::Africa__Abidjan), "Africa/Abidjan");
        assert_eq!(format!("{}", Tz::UTC), "UTC");
        assert_eq!(format!("{}", Tz::Zulu), "Zulu");
    }

    #[test]
    fn test_impl_hash() {
        #[allow(dead_code)]
        #[derive(Hash)]
        struct Foo(Tz);
    }

    #[test]
    fn test_iana_tzdb_version() {
        // Format should be something like 2023c.
        assert_eq!(5, IANA_TZDB_VERSION.len());
        let numbers: Vec<&str> = IANA_TZDB_VERSION.matches(char::is_numeric).collect();
        assert_eq!(4, numbers.len());
        assert!(IANA_TZDB_VERSION.ends_with(|c: char| c.is_ascii_lowercase()));
    }

    #[test]
    fn test_numeric_names() {
        let dt = Scoresbysund.with_ymd_and_hms(2024, 5, 1, 0, 0, 0).unwrap();
        assert_eq!(format!("{}", dt.offset()), "-01");
        assert_eq!(format!("{:?}", dt.offset()), "-01");
        let dt = Casey.with_ymd_and_hms(2022, 11, 1, 0, 0, 0).unwrap();
        assert_eq!(format!("{}", dt.offset()), "+11");
        assert_eq!(format!("{:?}", dt.offset()), "+11");
        let dt = Addis_Ababa.with_ymd_and_hms(1937, 2, 1, 0, 0, 0).unwrap();
        assert_eq!(format!("{}", dt.offset()), "+0245");
        assert_eq!(format!("{:?}", dt.offset()), "+0245");
    }

    #[test]
    fn casey_utc_change_time() {
        assert_eq!(
            NaiveDate::from_ymd_opt(2012, 2, 21)
                .unwrap()
                .and_hms_opt(16, 59, 59)
                .unwrap()
                .and_utc()
                .with_timezone(&Casey)
                .offset()
                .to_string(),
            "+11"
        );

        assert_eq!(
            NaiveDate::from_ymd_opt(2012, 2, 21)
                .unwrap()
                .and_hms_opt(17, 00, 00)
                .unwrap()
                .and_utc()
                .with_timezone(&Casey)
                .offset()
                .to_string(),
            "+08"
        );
    }
}
