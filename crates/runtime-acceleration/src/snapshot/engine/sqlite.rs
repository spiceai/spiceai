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

//! `SQLite`-specific snapshot engine implementation.
//!
//! `SQLite` accelerator databases run in WAL (write-ahead log) journal mode.
//! In WAL mode, writes are buffered into a `<db>-wal` sidecar file and only
//! periodically checkpointed back into the main `.sqlite` file. A naive
//! `fs::copy` of just the main file therefore captures only the durable
//! pages (often just the 4 KB header on a freshly-written DB) and loses
//! every uncheckpointed write.
//!
//! The `SqliteSnapshotEngine` addresses this by:
//!   1. **`checkpoint_live`** — under the accelerator write lock, opens a
//!      short-lived rusqlite connection to the live DB and runs a full
//!      `wal_checkpoint(TRUNCATE)`. This is safe under WAL mode (multiple
//!      connections are supported) and the existing accelerator write lock
//!      held by the caller guarantees no other writers race us.
//!   2. **`prepare_for_upload`** — defensively switches the *copied* file
//!      to `journal_mode=DELETE` so the uploaded snapshot has no `-wal`
//!      sidecar at all and is fully self-contained.

use async_trait::async_trait;
use snafu::prelude::*;
use std::path::{Path, PathBuf};

use super::SnapshotEngine;

#[derive(Debug, Snafu)]
pub enum SqliteSnapshotError {
    #[snafu(display("Failed to open SQLite for snapshot preparation: {path:?}"))]
    Connect {
        path: PathBuf,
        source: rusqlite::Error,
    },
    #[snafu(display(
        "Failed to checkpoint SQLite WAL for dataset '{dataset}' at {path:?}: {source}"
    ))]
    Checkpoint {
        dataset: String,
        path: PathBuf,
        source: rusqlite::Error,
    },
    #[snafu(display(
        "Incomplete WAL checkpoint for dataset '{dataset}' at {path:?}: \
         busy={busy}, log_frames={log_frames}, checkpointed_frames={checkpointed_frames}. \
         Another connection is holding the WAL or not all frames were flushed; \
         snapshotting now would lose data."
    ))]
    CheckpointIncomplete {
        dataset: String,
        path: PathBuf,
        busy: i64,
        log_frames: i64,
        checkpointed_frames: i64,
    },
    #[snafu(display(
        "Failed to switch SQLite copy to journal_mode=DELETE for dataset '{dataset}' at {path:?}: {source}"
    ))]
    JournalMode {
        dataset: String,
        path: PathBuf,
        source: rusqlite::Error,
    },
    #[snafu(display(
        "SQLite snapshot preparation task failed unexpectedly for dataset '{dataset}'"
    ))]
    JoinError {
        dataset: String,
        source: tokio::task::JoinError,
    },
}

pub struct SqliteSnapshotEngine;

impl SqliteSnapshotEngine {
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl Default for SqliteSnapshotEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl SnapshotEngine for SqliteSnapshotEngine {
    async fn checkpoint_live(
        &self,
        live_path: &Path,
        dataset_name: &str,
    ) -> Result<(), super::SnapshotEngineError> {
        let live_path = live_path.to_path_buf();
        let dataset = dataset_name.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = rusqlite::Connection::open(&live_path).context(ConnectSnafu {
                path: live_path.clone(),
            })?;
            // wal_checkpoint(TRUNCATE) forces all WAL frames into the main
            // database file and truncates the WAL to zero length. This is the
            // strongest available checkpoint short of switching journal mode.
            //
            // The pragma returns one row `(busy, log, checkpointed)`:
            //   * `busy != 0` means another connection (e.g. a stuck
            //     read-transaction) is holding the WAL and the truncation
            //     could not complete.
            //   * `checkpointed < log` means not every frame was flushed.
            //
            // Either case would let post-checkpoint writes leak past the copy
            // we're about to take, defeating the whole point of the hook. We
            // surface them as `Checkpoint` errors so the caller can either
            // retry or fall back rather than silently snapshot a corrupted
            // database.
            let (busy, log_frames, checkpointed_frames): (i64, i64, i64) = conn
                .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                })
                .context(CheckpointSnafu {
                    dataset: dataset.clone(),
                    path: live_path.clone(),
                })?;
            if busy != 0 || checkpointed_frames < log_frames {
                return Err(SqliteSnapshotError::CheckpointIncomplete {
                    dataset: dataset.clone(),
                    path: live_path,
                    busy,
                    log_frames,
                    checkpointed_frames,
                });
            }
            Ok::<(), SqliteSnapshotError>(())
        })
        .await
        .context(JoinSnafu {
            dataset: dataset_name.to_string(),
        })
        .map_err(|e| super::SnapshotEngineError::Sqlite { source: e })?
        .map_err(|e| super::SnapshotEngineError::Sqlite { source: e })
    }

    async fn prepare_for_upload(
        &self,
        source_path: &Path,
        dataset_name: &str,
    ) -> Result<PathBuf, super::SnapshotEngineError> {
        // The caller has already done `fs::copy(live_db, source_path)` after
        // `checkpoint_live` flushed the WAL. The copy should already be
        // self-contained, but as defense-in-depth we switch the copy to
        // `journal_mode=DELETE` to guarantee no `-wal`/`-shm` sidecars exist
        // adjacent to the file we're about to upload.
        let copy_path = source_path.to_path_buf();
        let dataset = dataset_name.to_string();
        let path = tokio::task::spawn_blocking(move || {
            let conn = rusqlite::Connection::open(&copy_path).context(ConnectSnafu {
                path: copy_path.clone(),
            })?;
            // Switching to DELETE mode forces a final checkpoint and removes
            // any -wal/-shm files. If the copy never had a WAL (because the
            // live checkpoint already truncated it), this is a no-op.
            conn.query_row("PRAGMA journal_mode=DELETE", [], |_row| Ok(()))
                .context(JournalModeSnafu {
                    dataset,
                    path: copy_path.clone(),
                })?;
            Ok::<PathBuf, SqliteSnapshotError>(copy_path)
        })
        .await
        .context(JoinSnafu {
            dataset: dataset_name.to_string(),
        })
        .map_err(|e| super::SnapshotEngineError::Sqlite { source: e })?
        .map_err(|e| super::SnapshotEngineError::Sqlite { source: e })?;
        Ok(path)
    }

    fn supports_compaction(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;
    use tempfile::TempDir;

    fn create_wal_mode_db_with_rows(tmp: &TempDir, rows: &[(i64, &str)]) -> PathBuf {
        let db_path = tmp.path().join("live.sqlite");
        let conn = Connection::open(&db_path).expect("open");
        // Force WAL mode and write rows; do NOT checkpoint, so the main
        // file remains nearly-empty and the data lives only in the WAL.
        conn.query_row("PRAGMA journal_mode=WAL", [], |_| Ok(()))
            .expect("wal");
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT)", [])
            .expect("create");
        for (id, name) in rows {
            conn.execute("INSERT INTO t(id, name) VALUES (?1, ?2)", (id, name))
                .expect("insert");
        }
        // Drop without explicit checkpoint -- keep WAL state intact.
        drop(conn);
        db_path
    }

    fn count_rows(path: &Path) -> i64 {
        let conn = Connection::open(path).expect("open verify");
        conn.query_row("SELECT COUNT(*) FROM t", [], |row| row.get(0))
            .expect("count")
    }

    #[tokio::test]
    async fn checkpoint_live_then_copy_captures_all_rows() {
        let tmp = TempDir::new().expect("tmp");
        let rows = vec![(1, "a"), (2, "b"), (3, "c")];
        let live = create_wal_mode_db_with_rows(&tmp, &rows);

        let engine = SqliteSnapshotEngine::new();
        engine
            .checkpoint_live(&live, "ds")
            .await
            .expect("checkpoint live");

        let copy = tmp.path().join("copy.sqlite");
        std::fs::copy(&live, &copy).expect("copy");

        let final_path = engine
            .prepare_for_upload(&copy, "ds")
            .await
            .expect("prepare");

        // No -wal or -shm should exist next to the prepared file.
        assert!(!final_path.with_extension("sqlite-wal").exists());
        assert!(!final_path.with_extension("sqlite-shm").exists());

        assert_eq!(
            count_rows(&final_path),
            i64::try_from(rows.len()).expect("row count fits in i64")
        );
    }
}
