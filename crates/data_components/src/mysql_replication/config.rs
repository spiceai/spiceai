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

//! Replication parameters and binlog-position primitives.

use std::cmp::Ordering;
use std::time::Duration;

use crate::cdc::{InitialSnapshotMode, InvalidCheckpointBehavior};

/// The kind of cursor a persisted checkpoint resumes from. Stored explicitly
/// with each checkpoint (never inferred from the presence of a GTID set): a
/// GTID checkpoint with an empty executed set (`gtid_mode = ON`, zero
/// transactions applied yet) must still reload as GTID, so an engine that maps
/// an empty string to `NULL` on the round-trip cannot silently reclassify it as
/// file — which would resume from a server-local offset unrelated to the GTID
/// set and open a silent gap on failover.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CursorType {
    /// File+offset positioning (`COM_BINLOG_DUMP`). Server-local; re-snapshots
    /// on failover.
    File,
    /// GTID auto-positioning (`COM_BINLOG_DUMP_GTID`). Failover-safe.
    Gtid,
}

impl CursorType {
    /// The value persisted in the sidecar `cursor_type` column.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::File => "file",
            Self::Gtid => "gtid",
        }
    }

    /// Parse a stored `cursor_type` value. Returns `None` for an absent/unknown
    /// value (a row that predates the column, or corrupt data); the sidecar
    /// loader resolves that (unreleased-feature-only) case by inferring the type
    /// from the persisted GTID set rather than propagating the `None`.
    #[must_use]
    pub fn from_stored(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "file" => Some(Self::File),
            "gtid" => Some(Self::Gtid),
            _ => None,
        }
    }
}

/// Parameters for a single dataset's binlog replication stream.
///
/// Built by the connector from spicepod params; see
/// `connector-mysql::replication::replication_params_from_connector_params`.
#[derive(Clone)]
pub struct ReplicationParams {
    /// Connection options for both the setup/snapshot connections and the
    /// binlog dump connection. Built by the connector from the same params
    /// that configure the federated read pool.
    pub opts: mysql_async::Opts,
    /// The `server_id` this replica registers on the source with
    /// (`COM_BINLOG_DUMP`). Must be unique among all replicas concurrently
    /// attached to the same source — `MySQL` disconnects the older of two
    /// connections sharing an id. Unlike a Postgres replication slot, no
    /// server-side state is keyed on it, so it may change across restarts.
    pub server_id: u32,
    /// Whether/when the initial table snapshot runs.
    pub snapshot_mode: InitialSnapshotMode,
    /// Rows per emitted snapshot batch during initial bootstrap.
    pub bootstrap_batch_size: usize,
    /// How often the stream persists its committed binlog position to the
    /// sidecar while idle or between commits, and the source heartbeat
    /// period. A crash loses at most this much ack progress; the overlap is
    /// re-streamed and applied idempotently via the PK upsert.
    pub checkpoint_interval: Duration,
    /// What to do when the persisted binlog position is no longer available
    /// on the source (binary logs purged past it).
    pub invalid_position_behavior: InvalidCheckpointBehavior,
    /// Lag-based readiness threshold: the dataset is marked Ready once its
    /// replication lag (now minus the newest applied commit's binlog-header
    /// timestamp) falls below this, so a snapshotting or backlog-draining
    /// dataset stays not-ready and never serves stale data. User param
    /// `mysql_replication_ready_lag` (default 2s).
    pub ready_lag: Duration,
}

impl std::fmt::Debug for ReplicationParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplicationParams")
            .field("host", &self.opts.ip_or_hostname())
            .field("port", &self.opts.tcp_port())
            .field("user", &self.opts.user())
            .field("database", &self.opts.db_name())
            .field("server_id", &self.server_id)
            .field("snapshot_mode", &self.snapshot_mode)
            .field("bootstrap_batch_size", &self.bootstrap_batch_size)
            .field("checkpoint_interval", &self.checkpoint_interval)
            .field("invalid_position_behavior", &self.invalid_position_behavior)
            .field("ready_lag", &self.ready_lag)
            .finish_non_exhaustive()
    }
}

/// A position in the source's binary log: file name plus byte offset.
///
/// This is the client-side analog of a Postgres replication slot's
/// `confirmed_flush_lsn` — `MySQL` keeps no per-replica cursor on the server,
/// so Spice persists this in the accelerator sidecar and passes it to
/// `COM_BINLOG_DUMP` on (re)start.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BinlogPosition {
    /// Binlog file name, e.g. `binlog.000042`.
    pub file: String,
    /// Byte offset of the next event to read within `file`.
    pub pos: u64,
}

impl BinlogPosition {
    #[must_use]
    pub fn new(file: impl Into<String>, pos: u64) -> Self {
        Self {
            file: file.into(),
            pos,
        }
    }

    /// The numeric suffix of the binlog file name (`binlog.000042` → 42).
    /// `None` when the name has no `.NNNNNN` suffix (never the case for real
    /// server-generated names, but kept total for robustness).
    pub(crate) fn file_ordinal(&self) -> Option<u64> {
        self.file.rsplit_once('.')?.1.parse().ok()
    }
}

impl std::fmt::Display for BinlogPosition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.file, self.pos)
    }
}

impl PartialOrd for BinlogPosition {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for BinlogPosition {
    /// Orders by binlog file, then offset. Files compare by their numeric
    /// suffix when both have one (`binlog.000009 < binlog.000010`, which
    /// plain string comparison also gets right for the server's zero-padded
    /// names — the numeric parse additionally survives a `RESET` changing
    /// padding width) and fall back to lexicographic comparison otherwise.
    fn cmp(&self, other: &Self) -> Ordering {
        if self.file == other.file {
            return self.pos.cmp(&other.pos);
        }
        match (self.file_ordinal(), other.file_ordinal()) {
            (Some(a), Some(b)) if a != b => a.cmp(&b),
            _ => self
                .file
                .cmp(&other.file)
                .then_with(|| self.pos.cmp(&other.pos)),
        }
    }
}

/// Derive a default `server_id` for a dataset's binlog connection.
///
/// Properties:
///   - Stable for a given dataset within a process (no self-collision when a
///     dataset restarts its stream).
///   - Distinct across processes with high probability (`process_nonce`
///     mixes in the pid + process start time), so two spiced replicas
///     streaming the same dataset name don't kick each other off the source.
///   - Clamped to `>= MIN_DERIVED_SERVER_ID` to stay clear of the small ids
///     operators typically hand-assign to real replicas.
///
/// `MySQL` keeps no server-side state keyed on the id, so a different value
/// after a process restart is harmless.
#[must_use]
pub fn derive_server_id(dataset_name: &str, process_nonce: u32) -> u32 {
    const MIN_DERIVED_SERVER_ID: u32 = 100_000;
    let hash = fnv1a_32(dataset_name.as_bytes()) ^ process_nonce.rotate_left(16);
    if hash < MIN_DERIVED_SERVER_ID {
        hash + MIN_DERIVED_SERVER_ID
    } else {
        hash
    }
}

/// A per-process random-ish nonce for [`derive_server_id`]: pid mixed with
/// process start time. Computed once and cached.
#[must_use]
pub fn process_nonce() -> u32 {
    use std::sync::OnceLock;
    static NONCE: OnceLock<u32> = OnceLock::new();
    *NONCE.get_or_init(|| {
        let pid = std::process::id();
        // Hash the full epoch-nanos timestamp, not just the sub-second part:
        // containerized deployments commonly share pid=1, so the timestamp
        // carries most of the cross-instance entropy.
        let now_nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0u128, |d| d.as_nanos());
        fnv1a_32(&pid.to_le_bytes()) ^ fnv1a_32(&now_nanos.to_le_bytes())
    })
}

/// 32-bit FNV-1a. Inlined (rather than a hashing dep) so the derivation is
/// stable across dependency upgrades.
fn fnv1a_32(bytes: &[u8]) -> u32 {
    let mut hash: u32 = 0x811c_9dc5;
    for b in bytes {
        hash ^= u32::from(*b);
        hash = hash.wrapping_mul(0x0100_0193);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn position_orders_within_a_file() {
        let a = BinlogPosition::new("binlog.000001", 100);
        let b = BinlogPosition::new("binlog.000001", 200);
        assert!(a < b);
        assert!(b > a);
        assert_eq!(a.cmp(&a.clone()), Ordering::Equal);
    }

    #[test]
    fn position_orders_across_file_rollover() {
        // Offset in the later file is smaller — the file must dominate.
        let old = BinlogPosition::new("binlog.000009", 900_000);
        let new = BinlogPosition::new("binlog.000010", 4);
        assert!(old < new);
    }

    #[test]
    fn position_orders_across_padding_width_change() {
        // Numeric suffix comparison survives differing zero-padding, where
        // lexicographic comparison would invert ("binlog.99" > "binlog.100").
        let old = BinlogPosition::new("binlog.99", 4);
        let new = BinlogPosition::new("binlog.100", 4);
        assert!(old < new);
    }

    #[test]
    fn position_falls_back_to_lexicographic_for_unparsable_names() {
        let a = BinlogPosition::new("alpha-bin", 4);
        let b = BinlogPosition::new("beta-bin", 4);
        assert!(a < b);
    }

    #[test]
    fn derived_server_id_is_stable_and_clamped() {
        let a = derive_server_id("orders", 12345);
        let b = derive_server_id("orders", 12345);
        assert_eq!(a, b, "same inputs must derive the same id");
        assert!(a >= 100_000, "derived id {a} must clear the reserved range");

        let c = derive_server_id("orders", 54321);
        assert_ne!(
            a, c,
            "different process nonces should produce different ids"
        );
    }

    #[test]
    fn derived_server_id_differs_per_dataset() {
        let nonce = 7;
        assert_ne!(
            derive_server_id("orders", nonce),
            derive_server_id("customers", nonce)
        );
    }
}
