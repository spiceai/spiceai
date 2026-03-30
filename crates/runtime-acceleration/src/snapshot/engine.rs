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

#[derive(Debug, Snafu)]
pub enum SnapshotEngineError {
    #[snafu(display("DuckDB snapshot error: {source}"))]
    #[cfg(feature = "duckdb")]
    DuckDB { source: duckdb::DuckDBSnapshotError },

    /// Placeholder variant for when no features are enabled
    #[snafu(display(
        "No snapshot engine is available. Enable a snapshot engine feature (e.g., 'duckdb')."
    ))]
    #[cfg(not(feature = "duckdb"))]
    Generic,
}

/// Trait defining engine-specific snapshot operations.
#[async_trait]
pub trait SnapshotEngine: Send + Sync {
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
        AccelerationEngine::Sqlite => Arc::new(DefaultSnapshotEngine),
        #[cfg(feature = "turso")]
        AccelerationEngine::Turso => Arc::new(DefaultSnapshotEngine),
        AccelerationEngine::Cayenne => Arc::new(DefaultSnapshotEngine),
    }
}
