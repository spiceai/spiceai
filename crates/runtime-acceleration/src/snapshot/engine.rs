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

//! Snapshot engine trait and implementations for different acceleration engines.

use async_trait::async_trait;
use snafu::prelude::*;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::AccelerationEngine;

#[cfg(feature = "duckdb")]
mod duckdb;
#[cfg(feature = "duckdb")]
pub use duckdb::DuckDBSnapshotEngine;

#[cfg(feature = "sqlite")]
mod sqlite;
#[cfg(feature = "sqlite")]
pub use sqlite::SqliteSnapshotEngine;

#[cfg(feature = "turso")]
mod turso;
#[cfg(feature = "turso")]
pub use turso::TursoSnapshotEngine;

#[derive(Debug, Snafu)]
pub enum SnapshotEngineError {
    #[snafu(display("DuckDB snapshot error: {source}"))]
    #[cfg(feature = "duckdb")]
    DuckDB { source: duckdb::DuckDBSnapshotError },

    #[snafu(display("SQLite snapshot error: {source}"))]
    #[cfg(feature = "sqlite")]
    Sqlite { source: sqlite::SqliteSnapshotError },

    #[snafu(display("Turso snapshot error: {source}"))]
    #[cfg(feature = "turso")]
    Turso { source: turso::TursoSnapshotError },

    /// Placeholder variant for when no snapshot-capable feature is enabled.
    #[snafu(display(
        "No snapshot engine is available. Enable a snapshot engine feature \
         (e.g., 'duckdb', 'sqlite', or 'turso')."
    ))]
    #[cfg(not(any(feature = "duckdb", feature = "sqlite", feature = "turso")))]
    Generic,

    /// Open-ended variant used by engines that live outside `runtime-acceleration`
    /// (e.g. `CayenneSnapshotEngine` in the runtime crate). The owning crate
    /// formats its rich error to a string and wraps it here.
    #[snafu(display("{message}"))]
    Custom { message: String },
}

impl SnapshotEngineError {
    /// Construct a [`SnapshotEngineError::Custom`] from anything that renders
    /// to a string. Convenience for engines defined in downstream crates.
    pub fn from_display<D: std::fmt::Display>(message: D) -> Self {
        SnapshotEngineError::Custom {
            message: message.to_string(),
        }
    }
}

/// Trait defining engine-specific snapshot operations.
#[async_trait]
pub trait SnapshotEngine: Send + Sync {
    /// Hook invoked on the **live** accelerator file *before* it is copied to a
    /// temporary snapshot location. Engines that buffer writes outside the
    /// primary file (e.g. SQLite/Turso WAL) should checkpoint here so that the
    /// subsequent `fs::copy` produces a self-contained file.
    ///
    /// Default implementation is a no-op.
    ///
    /// The caller holds the accelerator's write lock for the duration of this
    /// call, so no concurrent writes are in flight.
    async fn checkpoint_live(
        &self,
        _live_path: &Path,
        _dataset_name: &str,
    ) -> Result<(), SnapshotEngineError> {
        Ok(())
    }

    /// Prepares a snapshot file for upload.
    /// For engines that support compaction (e.g., `DuckDB`), this may compact the file.
    /// For other engines, this returns the source path unchanged.
    ///
    /// # Arguments
    /// * `source_path` - Path to the original snapshot file
    /// * `dataset_name` - Name of the dataset for logging/error messages
    ///
    /// # Returns
    /// Path to the prepared file (may be a new compacted file or the original)
    async fn prepare_for_upload(
        &self,
        source_path: &Path,
        dataset_name: &str,
    ) -> Result<PathBuf, SnapshotEngineError>;

    /// Returns whether this engine supports compaction.
    fn supports_compaction(&self) -> bool;

    /// Hook invoked by `SnapshotManager` *before* archiving a directory-layout
    /// snapshot. Returns a [`DirectorySnapshotPlan`] that controls which files
    /// are skipped from the source directories and which extra in-memory
    /// entries are added to the archive.
    ///
    /// Default implementation includes everything, adds nothing.
    ///
    /// `dirs` is `(local_directory, archive_prefix)` pairs as passed to the
    /// archive layer. `dataset_name` is the name of the dataset whose snapshot
    /// is being created.
    async fn prepare_directory_snapshot(
        &self,
        dirs: &[(PathBuf, String)],
        dataset_name: &str,
    ) -> Result<DirectorySnapshotPlan, SnapshotEngineError> {
        let _ = (dirs, dataset_name);
        Ok(DirectorySnapshotPlan::default())
    }

    /// Hook invoked by `SnapshotManager` *after* extracting a directory-layout
    /// snapshot. Allows engines to perform engine-specific post-processing
    /// (e.g. import a metastore slice that was written into one of the
    /// extracted directories at upload time).
    ///
    /// `dirs` is the same `(local_directory, archive_prefix)` pairs supplied
    /// to the download path. The engine should locate any virtual entries it
    /// emitted from `prepare_directory_snapshot` by their well-known archive
    /// paths within `dirs` (the upload-time `extras` list cannot be passed
    /// across the upload → download boundary).
    ///
    /// Default implementation is a no-op.
    async fn finalize_directory_snapshot(
        &self,
        dirs: &[(PathBuf, String)],
        dataset_name: &str,
    ) -> Result<(), SnapshotEngineError> {
        let _ = (dirs, dataset_name);
        Ok(())
    }
}

/// A virtual entry to be added to a directory-snapshot tar archive that does
/// not come from the on-disk source directories.
#[derive(Debug, Clone)]
pub struct DirectoryArchiveExtra {
    /// Path within the tar archive (e.g. `"metastore/slice.json"`). Must not
    /// collide with a file produced by walking the source directories.
    pub archive_path: String,
    /// Raw bytes of the entry.
    pub bytes: Vec<u8>,
}

/// Engine-supplied plan that controls how a directory-layout snapshot is
/// archived (creation side) and what extras the corresponding extract-side
/// hook should expect to find.
#[derive(Debug, Clone, Default)]
pub struct DirectorySnapshotPlan {
    /// Filenames (relative to each `dirs[i].0`) that must be excluded from
    /// the archive. Engines use this to drop files they intend to replace
    /// (e.g. Cayenne drops `cayenne.db*` because the metastore is captured
    /// as a JSON slice instead).
    pub skip_relative_paths: HashSet<PathBuf>,
    /// Extra in-memory entries to add to the archive after the on-disk
    /// directory contents are written.
    pub extra_entries: Vec<DirectoryArchiveExtra>,
}

/// Default snapshot engine for engines that don't require special preparation.
pub struct DefaultSnapshotEngine;

#[async_trait]
impl SnapshotEngine for DefaultSnapshotEngine {
    async fn prepare_for_upload(
        &self,
        source_path: &Path,
        _dataset_name: &str,
    ) -> Result<PathBuf, SnapshotEngineError> {
        Ok(source_path.to_path_buf())
    }

    fn supports_compaction(&self) -> bool {
        false
    }
}

/// Creates a snapshot engine for the given acceleration engine.
#[must_use]
pub fn create_snapshot_engine(
    engine: &AccelerationEngine,
    #[cfg(feature = "duckdb")] compaction_enabled: bool,
    #[cfg(not(feature = "duckdb"))] _compaction_enabled: bool,
) -> Arc<dyn SnapshotEngine> {
    match engine {
        #[cfg(feature = "duckdb")]
        AccelerationEngine::DuckDB => {
            if compaction_enabled {
                tracing::debug!("Creating DuckDB snapshot engine with compaction enabled");
            }
            Arc::new(DuckDBSnapshotEngine::new(compaction_enabled))
        }
        #[cfg(feature = "sqlite")]
        AccelerationEngine::Sqlite => Arc::new(SqliteSnapshotEngine::new()),
        #[cfg(feature = "turso")]
        AccelerationEngine::Turso => Arc::new(TursoSnapshotEngine::new()),
        AccelerationEngine::Cayenne => Arc::new(DefaultSnapshotEngine),
    }
}
