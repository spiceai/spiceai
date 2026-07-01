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

//! Small wall-clock time helpers shared across crates.

use std::time::{SystemTime, UNIX_EPOCH};

/// Milliseconds since the Unix epoch for a wall-clock [`SystemTime`].
///
/// Returns `None` for pre-epoch times or if the millisecond count overflows
/// `i64`. Used to derive `source_commit_ts_ms`-style timestamps for the CDC
/// replication-lag metric (`now_ms - source_commit_ts_ms`).
#[must_use]
pub fn system_time_to_unix_ms(t: SystemTime) -> Option<i64> {
    i64::try_from(t.duration_since(UNIX_EPOCH).ok()?.as_millis()).ok()
}

/// Current wall-clock time in milliseconds since the Unix epoch. Used to compute
/// CDC replication lag as `now_unix_ms() - source_commit_ts_ms`. Falls back to `0`
/// only in the impossible cases the clock is pre-epoch or the value overflows
/// `i64`.
#[must_use]
pub fn now_unix_ms() -> i64 {
    system_time_to_unix_ms(SystemTime::now()).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn epoch_is_zero() {
        assert_eq!(system_time_to_unix_ms(UNIX_EPOCH), Some(0));
    }

    #[test]
    fn known_offset_round_trips() {
        let t = UNIX_EPOCH + Duration::from_millis(1_700_000_000_123);
        assert_eq!(system_time_to_unix_ms(t), Some(1_700_000_000_123));
    }

    #[test]
    fn pre_epoch_is_none() {
        let t = UNIX_EPOCH - Duration::from_millis(1);
        assert_eq!(system_time_to_unix_ms(t), None);
    }
}
