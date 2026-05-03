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
//! Turso uses libsql, which is on-disk-compatible with SQLite and runs in
//! WAL journal mode. The same WAL-flush problem documented in
//! `engine/sqlite.rs` applies. We reuse the SQLite checkpoint logic here
//! by opening the file with rusqlite (which is binary-compatible with
//! libsql's on-disk format).
//!
//! `runtime-acceleration`'s `turso` feature pulls in `rusqlite` directly, so
//! the WAL-flush logic is always available in any build that includes the
//! Turso snapshot engine — it does **not** depend on the `sqlite` feature
//! also being enabled.

use async_trait::async_trait;
use std::path::{Path, PathBuf};

use super::SnapshotEngine;
use super::sqlite::SqliteSnapshotEngine;

/// Snapshot engine for Turso accelerators. Delegates the WAL-flush + journal
/// normalization work to [`SqliteSnapshotEngine`] because libsql's on-disk
/// format is byte-compatible with SQLite.
pub struct TursoSnapshotEngine {
    inner: SqliteSnapshotEngine,
}

impl TursoSnapshotEngine {
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: SqliteSnapshotEngine::new(),
        }
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
        live_path: &Path,
        dataset_name: &str,
    ) -> Result<(), super::SnapshotEngineError> {
        self.inner.checkpoint_live(live_path, dataset_name).await
    }

    async fn prepare_for_upload(
        &self,
        source_path: &Path,
        dataset_name: &str,
    ) -> Result<PathBuf, super::SnapshotEngineError> {
        self.inner
            .prepare_for_upload(source_path, dataset_name)
            .await
    }

    fn supports_compaction(&self) -> bool {
        false
    }
}
