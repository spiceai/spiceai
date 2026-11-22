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

//! Every generated zone against the system tz database: the same IANA release compiled by
//! `zic` and read with jiff, which share no code with parse-zoneinfo or `build.rs`.
//!
//! An offset is a step function of time, so two zones that agree just before and at every
//! transition either side has agree everywhere between. Each zone is compared at those
//! instants from 1900 to 2099, the last year parse-zoneinfo generates transitions for.
//!
//! It needs a system tz database of the release in `tz/`, so it is ignored by default.
//! Run it after updating `tz/`, on a machine whose system database is that release:
//! `cargo test -p chrono-tz --lib -- --ignored system_tzdb`.

use std::collections::BTreeSet;
use std::fs;

use chrono::{DateTime, Offset, TimeZone};
use jiff::Timestamp;
use jiff::tz::TimeZoneDatabase;

use crate::IANA_TZDB_VERSION;
use crate::timezone_impl::TimeSpans;
use crate::timezones::TZ_VARIANTS;

const SYSTEM_TZDB: &str = "/usr/share/zoneinfo";
/// 1900-01-01T00:00:00Z.
const FROM: i64 = -2_208_988_800;
/// 2100-01-01T00:00:00Z.
const UNTIL: i64 = 4_102_444_800;

/// The release of the system tz database: macOS writes it to `+VERSION`, and the
/// `tzdata.zi` that Linux distributions install starts with `# version <release>`.
fn system_tzdb_release() -> Option<String> {
    if let Ok(version) = fs::read_to_string(format!("{SYSTEM_TZDB}/+VERSION")) {
        return Some(version.trim().to_string());
    }
    let zi = fs::read_to_string(format!("{SYSTEM_TZDB}/tzdata.zi")).ok()?;
    let first = zi.lines().next()?;
    Some(first.strip_prefix("# version ")?.trim().to_string())
}

#[test]
#[ignore = "needs a system tz database of the release in tz/; see the module docs"]
fn every_zone_matches_the_system_tz_database() {
    let release = system_tzdb_release().expect("the system tz database names its release");
    assert_eq!(
        release, IANA_TZDB_VERSION,
        "the system tz database at {SYSTEM_TZDB} is {release}, not the {IANA_TZDB_VERSION} in tz/"
    );
    let db = TimeZoneDatabase::from_dir(SYSTEM_TZDB).expect("the system tz database opens");

    let mut compared = 0_usize;
    let mut instants_compared = 0_usize;
    let mut absent = Vec::new();
    let mut mismatches = Vec::new();
    for tz in TZ_VARIANTS {
        let Ok(system) = db.get(tz.name()) else {
            absent.push(tz.name());
            continue;
        };

        let mut instants = BTreeSet::from([FROM]);
        let generated = tz.timespans().rest.iter().map(|(at, _)| *at);
        let compiled = system
            .following(Timestamp::from_second(FROM).expect("1900 is a valid timestamp"))
            .map(|transition| transition.timestamp().as_second())
            .take_while(|at| *at < UNTIL);
        for at in generated
            .chain(compiled)
            .filter(|at| (FROM..UNTIL).contains(at))
        {
            instants.insert(at - 1);
            instants.insert(at);
        }

        for at in instants {
            let utc = DateTime::from_timestamp(at, 0)
                .expect("in range")
                .naive_utc();
            let ours = tz.offset_from_utc_datetime(&utc);
            let theirs = system.to_offset_info(Timestamp::from_second(at).expect("in range"));
            let (ours_seconds, theirs_seconds) =
                (ours.fix().local_minus_utc(), theirs.offset().seconds());
            let (ours_abbreviation, theirs_abbreviation) =
                (ours.to_string(), theirs.abbreviation().to_string());
            if ours_seconds != theirs_seconds || ours_abbreviation != theirs_abbreviation {
                mismatches.push(format!(
                    "{} at {at}: generated {ours_seconds}s {ours_abbreviation}, system {theirs_seconds}s {theirs_abbreviation}",
                    tz.name()
                ));
            }
            instants_compared += 1;
        }
        compared += 1;
    }

    eprintln!(
        "compared {compared} zones at {instants_compared} instants; absent from {SYSTEM_TZDB}: {absent:?}"
    );
    assert!(
        mismatches.is_empty(),
        "{} mismatches, first 20: {:#?}",
        mismatches.len(),
        &mismatches[..mismatches.len().min(20)]
    );
    assert!(
        compared * 10 >= TZ_VARIANTS.len() * 9,
        "only {compared} of {} zones are in {SYSTEM_TZDB}; absent: {absent:?}",
        TZ_VARIANTS.len()
    );
}
