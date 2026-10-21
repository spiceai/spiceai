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

//! The **`MySQL` binlog** checkpoint shape: the position in the source's binary log
//! that a dataset's acceleration is complete as of.

use std::time::SystemTime;

use crate::CheckpointError;

/// A dataset's binlog resume position.
///
/// `schema_json` is the schema in its durable JSON encoding rather than an Arrow
/// `SchemaRef`, which is what keeps this crate free of an Arrow dependency. Callers
/// convert with `arrow_tools::schema::{schema_to_json, schema_from_json}`.
#[derive(Clone, Debug, Default)]
pub struct MySqlBinlogCheckpoint {
    /// Binlog file name to resume from, e.g. `binlog.000042`.
    pub binlog_file: String,
    /// Byte offset of the next event to read within `binlog_file`.
    pub binlog_pos: u64,
    /// Serialized schema/layout snapshot, for detecting drift between runs. Opaque to
    /// the store, which only round-trips the string.
    pub schema_json: Option<String>,
    /// Executed GTID set (`uuid:range` text) for failover-safe resume via
    /// `COM_BINLOG_DUMP_GTID`. `None` for file+offset positioning; may be an empty
    /// string when `gtid_mode = ON` but no transactions have committed.
    pub gtid_executed: Option<String>,
    /// The checkpoint's positioning type (`file` | `gtid`), stored explicitly so resume
    /// does not have to infer it from `gtid_executed`. `Option` only because the column
    /// is nullable; a reader that finds `None` infers the type from `gtid_executed`.
    pub cursor_type: Option<String>,
    /// When the row was last written, as recorded by the store.
    pub updated_at: Option<SystemTime>,
}

/// Converts the replication layer's `u64` offset into the `BIGINT` a sidecar stores.
///
/// Positions beyond `i64::MAX` cannot occur (binlog files cap at 1 GiB), but clamp
/// defensively rather than wrap.
#[must_use]
pub fn position_to_i64(pos: u64) -> i64 {
    i64::try_from(pos).unwrap_or(i64::MAX)
}

/// Converts a stored `BIGINT` position back to the replication layer's `u64`.
///
/// A negative value cannot be a real position, so it reads as 0 — a re-bootstrap —
/// rather than wrapping to an enormous offset that would skip the whole binlog.
#[must_use]
pub fn position_from_i64(pos: i64) -> u64 {
    u64::try_from(pos).unwrap_or(0)
}

/// The `MySQL` binlog checkpoint store, satisfied by the accelerator and called by the
/// `MySQL` data connector. Object-safe, so it is used as `Arc<dyn MySqlBinlogStore>`.
#[async_trait::async_trait]
pub trait MySqlBinlogStore: Send + Sync {
    /// Load this dataset's resume position, or `None` when there is nothing to resume
    /// from.
    ///
    /// A failed read is reported as `None`, not as an error: the replication layer's
    /// only recovery from an unreadable position is the same as from an absent one — a
    /// re-bootstrap — so the two are deliberately not distinguished here.
    async fn get(&self) -> Option<MySqlBinlogCheckpoint>;

    /// Persist a resume position, overwriting any previous one.
    ///
    /// Implementations retry a transient accelerator write lock: the sidecar shares the
    /// accelerator's connection pool, so this contends with the accelerator's own
    /// CDC-apply transactions, and dropping a checkpoint interval widens the
    /// crash-replay window.
    async fn upsert(&self, checkpoint: &MySqlBinlogCheckpoint) -> Result<(), CheckpointError>;

    /// Discard this dataset's resume position, so the next run re-bootstraps.
    async fn delete(&self) -> Result<(), CheckpointError>;
}
