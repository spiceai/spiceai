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

//! Shared configuration vocabulary for CDC (`refresh_mode: changes`) connectors.
//!
//! These enums define the user-facing contract that every CDC source connector
//! (`MySQL` binlog, `MongoDB` change streams, `DynamoDB` streams, …) exposes
//! under its `{connector}_replication_*` parameters. Sharing the types keeps the
//! accepted values identical across connectors and gives the optimizer one place
//! to reason about the vocabulary. Each connector still owns its own parameter
//! plumbing — deprecated aliases, error type, and the behavior each value drives
//! against the source (a binlog reposition, an oplog token, a shard checkpoint).

use std::time::Duration;

/// When the initial snapshot of a source's existing rows runs for a
/// `refresh_mode: changes` dataset. User parameter:
/// `{connector}_replication_initial_snapshot` (`auto|always|disabled`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum InitialSnapshotMode {
    /// `auto` (default): snapshot when no resumable position/token/checkpoint
    /// exists; resume without a snapshot when one does.
    #[default]
    Auto,
    /// `always`: snapshot on every start, discarding any persisted position.
    Always,
    /// `disabled`: never snapshot; stream changes only.
    Disabled,
}

impl InitialSnapshotMode {
    /// The accepted user-facing values, in canonical order. Use for
    /// `ParameterSpec::one_of_ignore_ascii_case` and error messages.
    pub const VALUES: &'static [&'static str] = &["auto", "always", "disabled"];

    /// Parse a canonical value (case-insensitive, surrounding whitespace
    /// trimmed). Returns `None` for anything unrecognized.
    #[must_use]
    pub fn from_canonical(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "auto" => Some(Self::Auto),
            "always" => Some(Self::Always),
            "disabled" => Some(Self::Disabled),
            _ => None,
        }
    }
}

/// What to do when a persisted CDC checkpoint (binlog position, resume token, or
/// stream checkpoint) can no longer be honored by the source. User parameter:
/// `{connector}_replication_invalid_checkpoint_behavior` (`error|restart`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum InvalidCheckpointBehavior {
    /// `error` (default): surface a clear error so the operator can decide,
    /// because silently re-snapshotting a large source should be opt-in.
    #[default]
    Error,
    /// `restart`: drop the persisted checkpoint and re-snapshot the source.
    Restart,
}

impl InvalidCheckpointBehavior {
    /// The accepted user-facing values, in canonical order.
    pub const VALUES: &'static [&'static str] = &["error", "restart"];

    /// Parse a canonical value (case-insensitive, surrounding whitespace
    /// trimmed). Returns `None` for anything unrecognized.
    #[must_use]
    pub fn from_canonical(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "error" => Some(Self::Error),
            "restart" => Some(Self::Restart),
            _ => None,
        }
    }
}

/// Default readiness lag behind every connector's
/// `{connector}_replication_ready_lag`: a `refresh_mode: changes` dataset is
/// marked Ready once its replication lag (wall-clock now minus the freshest
/// applied source-commit timestamp) falls below this. `2s` tolerates ordinary
/// streaming jitter while still gating out a dataset that is snapshotting,
/// draining a backlog, or rebuilding after a restart. Mirrors `DynamoDB`'s
/// original `ready_lag` default.
pub const DEFAULT_READY_LAG: Duration = Duration::from_secs(2);

/// Cadence for the idle heartbeats that keep lag-based readiness live on a
/// quiet source: half the readiness lag (so an idle-but-caught-up source
/// reaches Ready within ~`ready_lag` of catching up), floored at 1s so a tiny
/// `ready_lag` cannot turn into a busy-poll of the source.
#[must_use]
pub fn heartbeat_interval(ready_lag: Duration) -> Duration {
    (ready_lag / 2).max(Duration::from_secs(1))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initial_snapshot_mode_from_canonical() {
        assert_eq!(
            InitialSnapshotMode::from_canonical(" AUTO "),
            Some(InitialSnapshotMode::Auto)
        );
        assert_eq!(
            InitialSnapshotMode::from_canonical("always"),
            Some(InitialSnapshotMode::Always)
        );
        assert_eq!(
            InitialSnapshotMode::from_canonical("disabled"),
            Some(InitialSnapshotMode::Disabled)
        );
        assert_eq!(InitialSnapshotMode::from_canonical("never"), None);
        assert_eq!(InitialSnapshotMode::default(), InitialSnapshotMode::Auto);
    }

    #[test]
    fn invalid_checkpoint_behavior_from_canonical() {
        assert_eq!(
            InvalidCheckpointBehavior::from_canonical("ERROR"),
            Some(InvalidCheckpointBehavior::Error)
        );
        assert_eq!(
            InvalidCheckpointBehavior::from_canonical("restart"),
            Some(InvalidCheckpointBehavior::Restart)
        );
        assert_eq!(
            InvalidCheckpointBehavior::from_canonical("rebootstrap"),
            None
        );
        assert_eq!(
            InvalidCheckpointBehavior::default(),
            InvalidCheckpointBehavior::Error
        );
    }

    #[test]
    fn heartbeat_interval_is_half_ready_lag_floored_at_1s() {
        assert_eq!(
            heartbeat_interval(Duration::from_secs(2)),
            Duration::from_secs(1)
        );
        assert_eq!(
            heartbeat_interval(Duration::from_secs(10)),
            Duration::from_secs(5)
        );
        // Floored at 1s so a tiny ready_lag never busy-polls the source.
        assert_eq!(
            heartbeat_interval(Duration::from_millis(200)),
            Duration::from_secs(1)
        );
    }
}
