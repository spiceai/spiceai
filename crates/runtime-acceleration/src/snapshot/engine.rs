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
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::AccelerationEngine;

#[cfg(feature = "duckdb")]
mod duckdb;
#[cfg(feature = "duckdb")]
pub use duckdb::DuckDBSnapshotEngine;

#[cfg(any(feature = "sqlite", feature = "turso"))]
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
    #[cfg(any(feature = "sqlite", feature = "turso"))]
    Sqlite { source: sqlite::SqliteSnapshotError },

    /// Placeholder variant for when no snapshot-capable feature is enabled.
    #[snafu(display(
        "No snapshot engine is available. Enable a snapshot engine feature \
         (e.g., 'duckdb', 'sqlite', or 'turso')."
    ))]
    #[cfg(not(any(feature = "duckdb", feature = "sqlite", feature = "turso")))]
    Generic,
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
