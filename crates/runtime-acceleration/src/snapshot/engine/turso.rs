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

//! Turso (libsql) snapshot engine implementation.
//!
//! **Status:** WAL flush is currently a no-op for Turso. The first attempt
//! reused [`super::sqlite::SqliteSnapshotEngine`] under the assumption that
//! libsql's on-disk format is byte-compatible with classic `SQLite` for
//! read-only opens via rusqlite. In practice the integration test surfaced
//! `"file is not a database"` from rusqlite when opening a libsql primary
//! file. Tracked in spiceai/spiceai#10657.
//!
//! Until that issue is fixed by routing the checkpoint pragma through a
//! turso/libsql-native connection, snapshot creation for Turso accelerators
//! falls back to the default behavior: `fs::copy` of the live file as-is,
//! with the same WAL-loss caveat that #10643 originally documented.
//! `refresh_mode: snapshot` against Turso is therefore disabled for now
//! (see `tests/snapshot_refresh/turso.rs`).

use async_trait::async_trait;
use std::path::{Path, PathBuf};

use super::SnapshotEngine;

/// Snapshot engine for Turso accelerators.
///
/// Currently a no-op (defers WAL flushing to a future libsql-native
/// implementation; see spiceai/spiceai#10657). The struct exists so that
/// `create_snapshot_engine` can return a stable, Turso-specific type and
/// the call sites stay symmetric with the other engine implementations.
pub struct TursoSnapshotEngine;

impl TursoSnapshotEngine {
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl Default for TursoSnapshotEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl SnapshotEngine for TursoSnapshotEngine {
    async fn checkpoint_live(
        &self,
        _live_path: &Path,
        _dataset_name: &str,
    ) -> Result<(), super::SnapshotEngineError> {
        // No-op: see spiceai/spiceai#10657. Returning Ok here matches
        // historical Turso behavior; once the issue is fixed this will route
        // through a libsql-native checkpoint.
        Ok(())
    }

    async fn prepare_for_upload(
        &self,
        source_path: &Path,
        _dataset_name: &str,
    ) -> Result<PathBuf, super::SnapshotEngineError> {
        // No-op: pass the copy through unchanged. See spiceai/spiceai#10657.
        Ok(source_path.to_path_buf())
    }

    fn supports_compaction(&self) -> bool {
        false
    }
}
