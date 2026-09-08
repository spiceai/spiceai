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

use std::time::{SystemTime, UNIX_EPOCH};

/// Convert a [`SystemTime`] to Unix-epoch milliseconds as an `i64`, returning
/// `None` for pre-epoch instants or values that overflow `i64`. Used to derive
/// `source_commit_ts_ms`-style timestamps and to read the wall clock for the CDC
/// replication-lag metric (`now_ms - source_commit_ts_ms`).
#[must_use]
pub fn system_time_to_unix_ms(t: SystemTime) -> Option<i64> {
    i64::try_from(t.duration_since(UNIX_EPOCH).ok()?.as_millis()).ok()
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
