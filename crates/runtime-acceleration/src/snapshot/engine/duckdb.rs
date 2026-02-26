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

//! DuckDB-specific snapshot engine implementation.

use async_trait::async_trait;
use snafu::prelude::*;
use std::path::{Path, PathBuf};

use super::SnapshotEngine;

#[derive(Debug, Snafu)]
#[expect(clippy::enum_variant_names)]
pub enum DuckDBSnapshotError {
    #[snafu(display("Failed to open DuckDB for snapshot preparation: {path:?}"))]
    CompactionConnect {
        path: PathBuf,
        source: duckdb::Error,
    },
    #[snafu(display("Failed to attach DuckDB database for snapshot preparation: {path:?}"))]
    CompactionAttach {
        path: PathBuf,
        source: duckdb::Error,
    },
    #[snafu(display("Failed to copy data during snapshot preparation for dataset '{dataset}'"))]
    CompactionCopy {
        dataset: String,
        source: duckdb::Error,
    },
    #[snafu(display("Snapshot preparation task failed unexpectedly for dataset '{dataset}'"))]
    CompactionJoin {
        dataset: String,
        source: tokio::task::JoinError,
    },
}

/// `DuckDB` snapshot engine with optional compaction support.
pub struct DuckDBSnapshotEngine {
    compaction_enabled: bool,
}

impl DuckDBSnapshotEngine {
    pub fn new(compaction_enabled: bool) -> Self {
        Self { compaction_enabled }
    }

    /// Compacts a `DuckDB` database using COPY FROM DATABASE.
    async fn compact_duckdb(
        &self,
        source: &Path,
        dest: &Path,
        dataset_name: &str,
    ) -> Result<(), DuckDBSnapshotError> {
        let source = source.to_path_buf();
        let dest = dest.to_path_buf();
        let dataset_name_owned = dataset_name.to_string();
        let dataset_name_for_join = dataset_name.to_string();

        let result = tokio::task::spawn_blocking(move || {
            // Remove destination if it exists
            let _ = std::fs::remove_file(&dest);

            // Open DuckDB in-memory, attach source as read-only and dest for writing
            let conn = duckdb::Connection::open_in_memory().map_err(|e| {
                DuckDBSnapshotError::CompactionConnect {
                    path: source.clone(),
                    source: e,
                }
            })?;

            let source_escaped = escape_duckdb_string(&source.to_string_lossy());
            conn.execute(
                &format!("ATTACH '{source_escaped}' AS source (READ_ONLY)"),
                [],
            )
            .map_err(|e| DuckDBSnapshotError::CompactionAttach {
                path: source.clone(),
                source: e,
            })?;

            let dest_escaped = escape_duckdb_string(&dest.to_string_lossy());
            conn.execute(&format!("ATTACH '{dest_escaped}' AS dest"), [])
                .map_err(|e| DuckDBSnapshotError::CompactionAttach {
                    path: dest.clone(),
                    source: e,
                })?;

            conn.execute("COPY FROM DATABASE source TO dest", [])
                .map_err(|e| DuckDBSnapshotError::CompactionCopy {
                    dataset: dataset_name_owned.clone(),
                    source: e,
                })?;

            Ok::<_, DuckDBSnapshotError>(())
        })
        .await;

        result.map_err(|e| DuckDBSnapshotError::CompactionJoin {
            dataset: dataset_name_for_join,
            source: e,
        })??;

        Ok(())
    }
}

#[async_trait]
impl SnapshotEngine for DuckDBSnapshotEngine {
    async fn prepare_for_upload(
        &self,
        source_path: &Path,
        dataset_name: &str,
    ) -> Result<PathBuf, super::SnapshotEngineError> {
        if self.compaction_enabled {
            let compacted_path = source_path.with_extension("compacted");
            self.compact_duckdb(source_path, &compacted_path, dataset_name)
                .await
                .map_err(|e| super::SnapshotEngineError::DuckDB { source: e })?;
            Ok(compacted_path)
        } else {
            Ok(source_path.to_path_buf())
        }
    }

    fn supports_compaction(&self) -> bool {
        true
    }
}

fn escape_duckdb_string(s: &str) -> String {
    s.replace('\'', "''")
}
