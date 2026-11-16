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

use core::fmt;
use serde::{de, ser};

use super::DateTime;
use crate::format::{SecondsFormat, write_rfc3339};

use crate::offset::Local;
use crate::offset::{FixedOffset, Offset, TimeZone, Utc};

/// Serialize to an RFC 3339 formatted string
///
/// As an extension to RFC 3339 this can serialize `DateTime`s outside the range of 0-9999 years
/// using an ISO 8601 syntax (which prepends an `-` or `+`).
impl<Tz: TimeZone> ser::Serialize for DateTime<Tz> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        struct FormatIso8601<'a, Tz: TimeZone> {
            inner: &'a DateTime<Tz>,
        }

        impl<Tz: TimeZone> fmt::Display for FormatIso8601<'_, Tz> {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                let naive = self.inner.naive_local();
                let offset = self.inner.offset.fix();
                write_rfc3339(f, naive, offset, SecondsFormat::AutoSi, true)
            }
        }

        serializer.collect_str(&FormatIso8601 { inner: self })
    }
}

struct DateTimeVisitor;

impl de::Visitor<'_> for DateTimeVisitor {
    type Value = DateTime<FixedOffset>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("an RFC 3339 formatted date and time string")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        value.parse().map_err(E::custom)
    }
}

/// Deserialize an RFC 3339 formatted string into a `DateTime<FixedOffset>`
///
/// As an extension to RFC 3339 this can deserialize to `DateTime`s outside the range of 0-9999
/// years using an ISO 8601 syntax (which prepends an `-` or `+`).
impl<'de> de::Deserialize<'de> for DateTime<FixedOffset> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        deserializer.deserialize_str(DateTimeVisitor)
    }
}

/// Deserialize an RFC 3339 formatted string into a `DateTime<Utc>`
///
/// If the value contains an offset from UTC that is not zero, the value will be converted to UTC.
///
/// As an extension to RFC 3339 this can deserialize to `DateTime`s outside the range of 0-9999
/// years using an ISO 8601 syntax (which prepends an `-` or `+`).
impl<'de> de::Deserialize<'de> for DateTime<Utc> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        deserializer
            .deserialize_str(DateTimeVisitor)
            .map(|dt| dt.with_timezone(&Utc))
    }
}

/// Deserialize an RFC 3339 formatted string into a `DateTime<Local>`
///
/// The value will remain the same instant in UTC, but the offset will be recalculated to match
/// that of the `Local` platform time zone.
///
/// As an extension to RFC 3339 this can deserialize to `DateTime`s outside the range of 0-9999
/// years using an ISO 8601 syntax (which prepends an `-` or `+`).
impl<'de> de::Deserialize<'de> for DateTime<Local> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        deserializer
            .deserialize_str(DateTimeVisitor)
            .map(|dt| dt.with_timezone(&Local))
    }
}

#[cfg(test)]
mod tests {

    use crate::Local;
    use crate::{DateTime, FixedOffset, TimeZone, Utc};
    use core::fmt;

    #[test]
    fn test_serde_serialize() {
        assert_eq!(
            serde_json::to_string(&Utc.with_ymd_and_hms(2014, 7, 24, 12, 34, 6).unwrap()).ok(),
            Some(r#""2014-07-24T12:34:06Z""#.to_owned())
        );
        assert_eq!(
            serde_json::to_string(
                &FixedOffset::east_opt(3660)
                    .unwrap()
                    .with_ymd_and_hms(2014, 7, 24, 12, 34, 6)
                    .unwrap()
            )
            .ok(),
            Some(r#""2014-07-24T12:34:06+01:01""#.to_owned())
        );
        assert_eq!(
            serde_json::to_string(
                &FixedOffset::east_opt(3650)
                    .unwrap()
                    .with_ymd_and_hms(2014, 7, 24, 12, 34, 6)
                    .unwrap()
            )
            .ok(),
            // An offset with seconds is not allowed by RFC 3339, so we round it to the nearest minute.
            // In this case `+01:00:50` becomes `+01:01`
            Some(r#""2014-07-24T12:34:06+01:01""#.to_owned())
        );
    }

    #[test]
    fn test_serde_deserialize() {
        // should check against the offset as well (the normal DateTime comparison will ignore them)
        fn norm<Tz: TimeZone>(dt: &Option<DateTime<Tz>>) -> Option<(&DateTime<Tz>, &Tz::Offset)> {
            dt.as_ref().map(|dt| (dt, dt.offset()))
        }

        let dt: Option<DateTime<Utc>> = serde_json::from_str(r#""2014-07-24T12:34:06Z""#).ok();
        assert_eq!(
            norm(&dt),
            norm(&Some(Utc.with_ymd_and_hms(2014, 7, 24, 12, 34, 6).unwrap()))
        );
        let dt: Option<DateTime<Utc>> = serde_json::from_str(r#""2014-07-24T13:57:06+01:23""#).ok();
        assert_eq!(
            norm(&dt),
            norm(&Some(Utc.with_ymd_and_hms(2014, 7, 24, 12, 34, 6).unwrap()))
        );

        let dt: Option<DateTime<FixedOffset>> =
            serde_json::from_str(r#""2014-07-24T12:34:06Z""#).ok();
        assert_eq!(
            norm(&dt),
            norm(&Some(
                FixedOffset::east_opt(0)
                    .unwrap()
                    .with_ymd_and_hms(2014, 7, 24, 12, 34, 6)
                    .unwrap()
            ))
        );
        let dt: Option<DateTime<FixedOffset>> =
            serde_json::from_str(r#""2014-07-24T13:57:06+01:23""#).ok();
        assert_eq!(
            norm(&dt),
            norm(&Some(
                FixedOffset::east_opt(60 * 60 + 23 * 60)
                    .unwrap()
                    .with_ymd_and_hms(2014, 7, 24, 13, 57, 6)
                    .unwrap()
            ))
        );

        // we don't know the exact local offset but we can check that
        // the conversion didn't change the instant itself

        {
            let dt: DateTime<Local> =
                serde_json::from_str(r#""2014-07-24T12:34:06Z""#).expect("local should parse");
            assert_eq!(dt, Utc.with_ymd_and_hms(2014, 7, 24, 12, 34, 6).unwrap());

            let dt: DateTime<Local> = serde_json::from_str(r#""2014-07-24T13:57:06+01:23""#)
                .expect("local should parse with offset");
            assert_eq!(dt, Utc.with_ymd_and_hms(2014, 7, 24, 12, 34, 6).unwrap());
        }

        assert!(serde_json::from_str::<DateTime<Utc>>(r#""2014-07-32T12:34:06Z""#).is_err());
        assert!(
            serde_json::from_str::<DateTime<FixedOffset>>(r#""2014-07-32T12:34:06Z""#).is_err()
        );
    }

    #[test]
    fn test_serde_no_offset_debug() {
        use crate::{MappedLocalTime, NaiveDate, NaiveDateTime, Offset};
        use core::fmt::Debug;

        #[derive(Clone)]
        struct TestTimeZone;
        impl Debug for TestTimeZone {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "TEST")
            }
        }
        impl TimeZone for TestTimeZone {
            type Offset = TestTimeZone;
            fn from_offset(_state: &TestTimeZone) -> TestTimeZone {
                TestTimeZone
            }
            fn offset_from_local_date(&self, _local: &NaiveDate) -> MappedLocalTime<TestTimeZone> {
                MappedLocalTime::Single(TestTimeZone)
            }
            fn offset_from_local_datetime(
                &self,
                _local: &NaiveDateTime,
            ) -> MappedLocalTime<TestTimeZone> {
                MappedLocalTime::Single(TestTimeZone)
            }
            fn offset_from_utc_date(&self, _utc: &NaiveDate) -> TestTimeZone {
                TestTimeZone
            }
            fn offset_from_utc_datetime(&self, _utc: &NaiveDateTime) -> TestTimeZone {
                TestTimeZone
            }
        }
        impl Offset for TestTimeZone {
            fn fix(&self) -> FixedOffset {
                FixedOffset::east_opt(15 * 60 * 60).unwrap()
            }
        }

        let tz = TestTimeZone;
        assert_eq!(format!("{:?}", &tz), "TEST");

        let dt = tz.with_ymd_and_hms(2023, 4, 24, 21, 10, 33).unwrap();
        let encoded = serde_json::to_string(&dt).unwrap();
        dbg!(&encoded);
        let decoded: DateTime<FixedOffset> = serde_json::from_str(&encoded).unwrap();
        assert_eq!(dt, decoded);
        assert_eq!(dt.offset().fix(), *decoded.offset());
    }
}
