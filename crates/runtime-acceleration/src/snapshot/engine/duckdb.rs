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

//! `DuckDB`-specific snapshot engine implementation.
//!
//! `DuckDB` buffers committed writes in a `<db>.wal` sidecar. A snapshot copies
//! the main file alone, so an unclean shutdown — or any path that reaches
//! `create_file_snapshot` without a prior `CHECKPOINT` — would publish a copy
//! that omits those rows. `checkpoint_live` folds the log into the main file
//! before that copy, which is how both `file_create` init and schema-recreate
//! (`snapshot_before_recreate`) pick the fold up: they share
//! `create_file_snapshot`, they do not each fold at the call site.

use async_trait::async_trait;
use snafu::prelude::*;
use std::path::{Path, PathBuf};

use super::SnapshotEngine;

#[derive(Debug, Snafu)]
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
    #[snafu(display("Failed to open DuckDB to fold the write-ahead log at {path:?}"))]
    CheckpointConnect {
        path: PathBuf,
        source: duckdb::Error,
    },
    #[snafu(display(
        "Failed to fold DuckDB's write-ahead log into the database file for dataset '{dataset}' at {path:?}: {source}"
    ))]
    Checkpoint {
        dataset: String,
        path: PathBuf,
        source: duckdb::Error,
    },
    #[snafu(display(
        "DuckDB write-ahead log fold task failed unexpectedly for dataset '{dataset}'"
    ))]
    CheckpointJoin {
        dataset: String,
        source: tokio::task::JoinError,
    },
}

/// `DuckDB` snapshot engine with optional compaction support.
pub struct DuckDBSnapshotEngine {
    compaction_enabled: bool,
}

impl DuckDBSnapshotEngine {
    #[must_use]
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
    async fn checkpoint_live(
        &self,
        live_path: &Path,
        dataset_name: &str,
    ) -> Result<(), super::SnapshotEngineError> {
        // An uncached connection: the shared pool is keyed by path, and
        // pre-recreation callers delete the live file immediately after this
        // copy. `CHECKPOINT` folds `<db>.wal` into the main file so
        // `fs::copy` cannot omit committed rows.
        let live_path = live_path.to_path_buf();
        let dataset = dataset_name.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = duckdb::Connection::open(&live_path).context(CheckpointConnectSnafu {
                path: live_path.clone(),
            })?;
            conn.execute("CHECKPOINT", []).context(CheckpointSnafu {
                dataset: dataset.clone(),
                path: live_path,
            })?;
            Ok::<(), DuckDBSnapshotError>(())
        })
        .await
        .context(CheckpointJoinSnafu {
            dataset: dataset_name.to_string(),
        })
        .map_err(|e| super::SnapshotEngineError::DuckDB { source: e })?
        .map_err(|e| super::SnapshotEngineError::DuckDB { source: e })
    }

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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn duckdb_wal_sidecar_path(database_path: &Path) -> PathBuf {
        let mut wal = database_path.as_os_str().to_os_string();
        wal.push(".wal");
        PathBuf::from(wal)
    }

    fn count_rows_in_duckdb_file(path: &Path) -> i64 {
        let connection = duckdb::Connection::open(path).expect("open DuckDB file");
        connection
            .query_row("SELECT COUNT(*) FROM t", [], |row| row.get(0))
            .expect("count rows")
    }

    /// Writes a table whose schema is in the main file and whose rows are left
    /// in the write-ahead log — the unclean-shutdown shape a pre-recreation
    /// snapshot would otherwise copy without folding.
    fn write_duckdb_with_wal_resident_rows(path: &Path, rows: i32) {
        let connection = duckdb::Connection::open(path).expect("open DuckDB file");
        connection
            .execute_batch(
                "PRAGMA disable_checkpoint_on_shutdown;
                 PRAGMA checkpoint_threshold='1TB';
                 CREATE TABLE t(id INTEGER);
                 CHECKPOINT;",
            )
            .expect("persist the empty table to the main file");
        connection
            .execute(
                &format!("INSERT INTO t SELECT i FROM generate_series(1, {rows})"),
                [],
            )
            .expect("insert rows that should remain in the write-ahead log");
    }

    /// Both `file_create` init and schema-recreate publish through
    /// `create_file_snapshot`, which calls `checkpoint_live` before copying.
    /// A main-file-only copy of an unclean-shutdown `DuckDB` would omit committed
    /// WAL rows; folding first makes that copy complete. Raised by Copilot on
    /// #13477.
    #[tokio::test]
    async fn checkpoint_live_then_copy_captures_all_rows() {
        let dir = TempDir::new().expect("temp dir");
        let live = dir.path().join("acceleration.db");
        let rows = 50;
        write_duckdb_with_wal_resident_rows(&live, rows);

        let wal = duckdb_wal_sidecar_path(&live);
        assert!(
            wal.exists(),
            "expected a write-ahead log so this test can show a main-file-only copy is incomplete"
        );

        let copy_without_fold = dir.path().join("copy_without_fold.db");
        std::fs::copy(&live, &copy_without_fold).expect("copy the main file before folding");
        assert_eq!(
            count_rows_in_duckdb_file(&copy_without_fold),
            0,
            "copying the main file without folding must omit WAL-resident rows"
        );

        let engine = DuckDBSnapshotEngine::new(false);
        engine
            .checkpoint_live(&live, "orders")
            .await
            .expect("fold the write-ahead log into the main file");

        let copy_after_fold = dir.path().join("copy_after_fold.db");
        std::fs::copy(&live, &copy_after_fold).expect("copy the main file after folding");
        assert_eq!(
            count_rows_in_duckdb_file(&copy_after_fold),
            i64::from(rows),
            "a main-file copy after checkpoint_live must include the WAL-resident rows"
        );
    }

    #[tokio::test]
    async fn checkpoint_live_refuses_a_file_that_is_not_a_duckdb_database() {
        let dir = TempDir::new().expect("temp dir");
        let path = dir.path().join("not-a-database.db");
        std::fs::write(&path, b"not a DuckDB database").expect("write an unverifiable file");

        let engine = DuckDBSnapshotEngine::new(false);
        let error = engine.checkpoint_live(&path, "orders").await.expect_err(
            "fold must refuse a file that is not a DuckDB database rather than publish it",
        );

        let message = error.to_string();
        assert!(
            message.contains("not-a-database.db"),
            "the error must name the file: {message}"
        );
    }

    #[tokio::test]
    async fn checkpoint_live_returns_ok_on_empty_database() {
        let dir = TempDir::new().expect("temp dir");
        let live = dir.path().join("empty.db");
        {
            drop(duckdb::Connection::open(&live).expect("create empty DuckDB file"));
        }

        let engine = DuckDBSnapshotEngine::new(false);
        engine
            .checkpoint_live(&live, "ds_empty")
            .await
            .expect("checkpoint live empty");
    }
}
