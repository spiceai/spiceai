// use chrono::{DateTime, FixedOffset, NaiveDateTime};
//
// pub fn parse_iso8601_timestamp(s: &str) -> Option<DateTime<FixedOffset>> {
//     s.parse::<DateTime<FixedOffset>>().ok()
// }
//
// pub fn parse_naive_timestamp(s: &str) -> Option<i64> {
//     if let Ok(naive) = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f") {
//         return Some(naive.and_utc().timestamp_millis());
//     }
//
//     if let Ok(naive) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f") {
//         return Some(naive.and_utc().timestamp_millis());
//     }
//
//     None
// }
