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
pub enum DuckDBSnapshotError {
    #[snafu(display(
        "Failed to snapshot dataset '{dataset}' (duckdb): its acceleration file at {path:?} could not be opened to flush pending writes. Check the file is readable and not in use by another process. See: https://spiceai.org/docs/components/data-accelerators/duckdb"
    ))]
    CheckpointConnect {
        dataset: String,
        path: PathBuf,
        source: duckdb::Error,
    },
    #[snafu(display(
        "Failed to snapshot dataset '{dataset}' (duckdb): pending writes in {path:?} could not be flushed, and snapshotting without them would omit the most recent rows. Retry the snapshot; if it keeps failing, check for another process writing the file. See: https://spiceai.org/docs/components/data-accelerators/duckdb"
    ))]
    Checkpoint {
        dataset: String,
        path: PathBuf,
        source: duckdb::Error,
    },
    #[snafu(display(
        "Failed to snapshot dataset '{dataset}' (duckdb): the task flushing pending writes ended unexpectedly"
    ))]
    CheckpointJoin {
        dataset: String,
        source: tokio::task::JoinError,
    },
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
    /// `DuckDB` holds committed writes in a `<db>.wal` sidecar until a checkpoint folds
    /// them into the database file, and the snapshot upload copies that file on its own —
    /// so a write still resident in the log is absent from the snapshot. `CHECKPOINT`
    /// against the live database drains it.
    ///
    /// The caller holds the accelerator write lock, so no write is in flight. A reader's
    /// open transaction does not block the checkpoint.
    async fn checkpoint_live(
        &self,
        live_path: &Path,
        dataset_name: &str,
    ) -> Result<(), super::SnapshotEngineError> {
        let live_path = live_path.to_path_buf();
        let dataset = dataset_name.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = duckdb::Connection::open(&live_path).context(CheckpointConnectSnafu {
                dataset: dataset.clone(),
                path: live_path.clone(),
            })?;
            conn.execute("CHECKPOINT", []).context(CheckpointSnafu {
                dataset,
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
    use super::{DuckDBSnapshotEngine, SnapshotEngine};
    use std::path::{Path, PathBuf};

    fn wal_path(live: &Path) -> PathBuf {
        PathBuf::from(format!("{}.wal", live.display()))
    }

    fn wal_bytes(live: &Path) -> u64 {
        std::fs::metadata(wal_path(live)).map_or(0, |m| m.len())
    }

    fn row_count(path: &Path) -> i64 {
        let conn = duckdb::Connection::open(path).expect("open for verification");
        conn.query_row("SELECT COUNT(*) FROM t", [], |row| row.get(0))
            .expect("count rows")
    }

    /// Regression guard for #13912. `DuckDB` keeps a committed write in its
    /// write-ahead log until a checkpoint folds it into the database file, and
    /// `create_file_snapshot` copies that file on its own — so without this hook the
    /// snapshot ships without the write. The connection stays open throughout, as the
    /// accelerator's pool holds it.
    #[tokio::test]
    async fn checkpoint_live_then_copy_captures_an_uncheckpointed_write() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let live = tmp.path().join("live.db");
        let conn = duckdb::Connection::open(&live).expect("open live database");
        conn.execute_batch("CREATE TABLE t(id INTEGER); INSERT INTO t VALUES (1); CHECKPOINT;")
            .expect("seed a checkpointed baseline");
        conn.execute_batch("INSERT INTO t VALUES (2);")
            .expect("write without checkpointing");

        // Assert the shape this test exists for, so it cannot pass vacuously if a
        // future DuckDB checkpoints eagerly and leaves nothing in the log.
        assert!(
            wal_bytes(&live) > 0,
            "the second write must still be in the write-ahead log for this test to mean anything"
        );

        DuckDBSnapshotEngine::new(false)
            .checkpoint_live(&live, "ds")
            .await
            .expect("checkpoint the live database");

        assert_eq!(wal_bytes(&live), 0, "the write-ahead log must be drained");

        // Exactly what the upload path does next: copy the database file alone.
        let copy = tmp.path().join("copy.db");
        std::fs::copy(&live, &copy).expect("copy the database file");
        assert_eq!(
            row_count(&copy),
            2,
            "the copy must carry the write that was still in the log"
        );
    }

    /// A reader's transaction may still be open: the caller's lock guards writes, not
    /// reads. The checkpoint must still drain the log rather than fail the snapshot.
    #[tokio::test]
    async fn checkpoint_live_drains_the_log_with_a_reader_transaction_open() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let live = tmp.path().join("live.db");
        let conn = duckdb::Connection::open(&live).expect("open live database");
        conn.execute_batch("CREATE TABLE t(id INTEGER); INSERT INTO t VALUES (1); CHECKPOINT;")
            .expect("seed a checkpointed baseline");
        conn.execute_batch("INSERT INTO t VALUES (2);")
            .expect("write without checkpointing");
        assert!(wal_bytes(&live) > 0, "the write must still be in the log");

        let reader = duckdb::Connection::open(&live).expect("open a reader");
        reader.execute_batch("BEGIN TRANSACTION").expect("begin");
        reader
            .query_row("SELECT COUNT(*) FROM t", [], |row| row.get::<_, i64>(0))
            .expect("read inside the transaction");

        DuckDBSnapshotEngine::new(false)
            .checkpoint_live(&live, "ds")
            .await
            .expect("checkpoint with a reader transaction open");

        assert_eq!(wal_bytes(&live), 0, "the write-ahead log must be drained");
        reader.execute_batch("COMMIT").expect("commit");
    }

    /// A dataset that has never been written has an empty database and no log.
    #[tokio::test]
    async fn checkpoint_live_is_ok_on_an_empty_database() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let live = tmp.path().join("empty.db");
        let conn = duckdb::Connection::open(&live).expect("create an empty database");
        drop(conn);

        DuckDBSnapshotEngine::new(false)
            .checkpoint_live(&live, "ds_empty")
            .await
            .expect("checkpointing an empty database must succeed");
    }

    /// The snapshot must fail rather than ship a file whose pending writes could not be
    /// flushed, and the failure must name the dataset and the file so an operator can act
    /// on it without reading the source.
    #[tokio::test]
    async fn a_file_that_is_not_a_database_fails_with_a_message_naming_the_dataset() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let live = tmp.path().join("not_a_database.db");
        std::fs::write(&live, b"this is not a DuckDB database").expect("write a decoy file");

        let err = DuckDBSnapshotEngine::new(false)
            .checkpoint_live(&live, "orders")
            .await
            .expect_err("a file DuckDB cannot open must fail the snapshot");

        let message = err.to_string();
        assert!(
            message.contains("'orders'"),
            "the message must name the dataset: {message}"
        );
        assert!(
            message.contains("not_a_database.db"),
            "the message must name the file: {message}"
        );
        assert!(
            message.contains("spiceai.org/docs"),
            "the message must point at the docs: {message}"
        );
    }
}
