/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! Rule changes from the tz database releases after 2025b, the release crates.io
//! chrono-tz 0.10.4 carries. The expected local times are the ones `tz/NEWS` announces
//! for each release, at an instant before the change and one after it, so these fail on
//! tables generated from an older release.

use chrono::{DateTime, Utc};
use chrono_tz::Tz;

/// `utc` as local time in `zone`, with its offset and abbreviation.
fn local(zone: &str, utc: &str) -> String {
    let tz: Tz = zone.parse().expect("an IANA time zone name");
    let utc: DateTime<Utc> = utc.parse().expect("an RFC 3339 instant");
    utc.with_timezone(&tz)
        .format("%Y-%m-%dT%H:%M:%S%:z %Z")
        .to_string()
}

/// 2026b: British Columbia moved to permanent -07, abbreviated MST, modeled from 2026-11-01.
#[test]
fn british_columbia_keeps_mst_after_2026_11_01() {
    assert_eq!(
        local("America/Vancouver", "2026-01-15T12:00:00Z"),
        "2026-01-15T04:00:00-08:00 PST"
    );
    assert_eq!(
        local("America/Vancouver", "2026-12-01T12:00:00Z"),
        "2026-12-01T05:00:00-07:00 MST"
    );
}

/// 2026c: Alberta moved to permanent -06, abbreviated CST, modeled from 2026-11-01.
#[test]
fn alberta_keeps_cst_after_2026_11_01() {
    assert_eq!(
        local("America/Edmonton", "2026-01-15T12:00:00Z"),
        "2026-01-15T05:00:00-07:00 MST"
    );
    assert_eq!(
        local("America/Edmonton", "2026-12-01T12:00:00Z"),
        "2026-12-01T06:00:00-06:00 CST"
    );
}

/// 2026c: Morocco moved to permanent +00 on 2026-09-20.
#[test]
fn morocco_keeps_utc_after_2026_09_20() {
    assert_eq!(
        local("Africa/Casablanca", "2026-01-15T12:00:00Z"),
        "2026-01-15T13:00:00+01:00 +01"
    );
    assert_eq!(
        local("Africa/Casablanca", "2026-12-01T12:00:00Z"),
        "2026-12-01T12:00:00+00:00 +00"
    );
}

/// 2026d: the Northwest Territories (`America/Inuvik`) moved to permanent -06, abbreviated
/// CST, modeled from 2026-11-01.
#[test]
fn northwest_territories_keep_cst_after_2026_11_01() {
    assert_eq!(
        local("America/Inuvik", "2026-01-15T12:00:00Z"),
        "2026-01-15T05:00:00-07:00 MST"
    );
    assert_eq!(
        local("America/Inuvik", "2026-12-01T12:00:00Z"),
        "2026-12-01T06:00:00-06:00 CST"
    );
}

/// 2026e: Manitoba moved to permanent -05, abbreviated EST, modeled from 2026-11-01.
#[test]
fn manitoba_keeps_est_after_2026_11_01() {
    assert_eq!(
        local("America/Winnipeg", "2026-01-15T12:00:00Z"),
        "2026-01-15T06:00:00-06:00 CST"
    );
    assert_eq!(
        local("America/Winnipeg", "2026-12-01T12:00:00Z"),
        "2026-12-01T07:00:00-05:00 EST"
    );
}

/// 2026f: Cuba did not fall back on 2026-11-01 and stays on -04 through 2027-11-07.
#[test]
fn cuba_stays_on_daylight_time_after_2026_11_01() {
    assert_eq!(
        local("America/Havana", "2026-01-15T12:00:00Z"),
        "2026-01-15T07:00:00-05:00 CST"
    );
    assert_eq!(
        local("America/Havana", "2026-12-01T12:00:00Z"),
        "2026-12-01T08:00:00-04:00 CDT"
    );
}
