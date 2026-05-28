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
//! Turso accelerator databases share `SQLite`'s on-disk page layout but store
//! pending writes in a libsql-specific WAL sidecar that `rusqlite` cannot
//! open ("file is not a database"). The previous implementation routed the
//! checkpoint pragma through rusqlite and therefore silently lost any
//! uncheckpointed WAL frames at snapshot time — see spiceai/spiceai#10657.
//!
//! This engine routes `PRAGMA wal_checkpoint(TRUNCATE)` through the
//! `turso` crate's native connection so the checkpoint actually runs
//! against the libsql file. The result-row contract (`busy`, `log`,
//! `checkpointed`) matches `SQLite`'s, so the same completeness check applies.

use async_trait::async_trait;
use snafu::prelude::*;
use std::path::{Path, PathBuf};

use super::SnapshotEngine;

#[derive(Debug, Snafu)]
pub enum TursoSnapshotError {
    #[snafu(display("Turso live-database path is not valid UTF-8 and cannot be opened: {path:?}"))]
    NonUtf8Path { path: PathBuf },
    #[snafu(display("Failed to open Turso database for snapshot preparation: {path:?}"))]
    Open { path: PathBuf, source: turso::Error },
    #[snafu(display("Failed to connect to Turso database for snapshot preparation: {path:?}"))]
    Connect { path: PathBuf, source: turso::Error },
    #[snafu(display(
        "Failed to checkpoint Turso WAL for dataset '{dataset}' at {path:?}: {source}"
    ))]
    Checkpoint {
        dataset: String,
        path: PathBuf,
        source: turso::Error,
    },
    #[snafu(display(
        "Turso wal_checkpoint did not return the expected (busy, log, checkpointed) row \
         for dataset '{dataset}' at {path:?}"
    ))]
    CheckpointNoRow { dataset: String, path: PathBuf },
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
}

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
        live_path: &Path,
        dataset_name: &str,
    ) -> Result<(), super::SnapshotEngineError> {
        let path = live_path.to_path_buf();
        let dataset = dataset_name.to_string();

        let result = checkpoint_via_turso(path, dataset).await;
        result.map_err(|e| super::SnapshotEngineError::Turso { source: e })
    }

    async fn prepare_for_upload(
        &self,
        source_path: &Path,
        _dataset_name: &str,
    ) -> Result<PathBuf, super::SnapshotEngineError> {
        // After `checkpoint_live` has flushed the WAL into the main file and
        // the caller has performed `fs::copy(live, source_path)`, the copy
        // contains all data; the WAL/SHM sidecars (if any remained) belong
        // to the *live* path and are not copied alongside the main file.
        // Nothing more to do here.
        Ok(source_path.to_path_buf())
    }

    fn supports_compaction(&self) -> bool {
        false
    }
}

async fn checkpoint_via_turso(
    live_path: PathBuf,
    dataset: String,
) -> Result<(), TursoSnapshotError> {
    let path_str = live_path
        .to_str()
        .ok_or_else(|| TursoSnapshotError::NonUtf8Path {
            path: live_path.clone(),
        })?;
    let database = turso::Builder::new_local(path_str)
        .build()
        .await
        .context(OpenSnafu {
            path: live_path.clone(),
        })?;

    let conn = database.connect().context(ConnectSnafu {
        path: live_path.clone(),
    })?;

    // PRAGMA wal_checkpoint(TRUNCATE) returns one row `(busy, log,
    // checkpointed)` matching SQLite's contract. busy != 0 means a
    // concurrent connection held the WAL; checkpointed < log means not
    // every frame was flushed. Either case would let post-checkpoint
    // writes leak past the copy we're about to take, so we surface the
    // partial-flush as an error.
    let mut rows = conn
        .query("PRAGMA wal_checkpoint(TRUNCATE)", ())
        .await
        .context(CheckpointSnafu {
            dataset: dataset.clone(),
            path: live_path.clone(),
        })?;

    let row = rows
        .next()
        .await
        .context(CheckpointSnafu {
            dataset: dataset.clone(),
            path: live_path.clone(),
        })?
        .ok_or_else(|| TursoSnapshotError::CheckpointNoRow {
            dataset: dataset.clone(),
            path: live_path.clone(),
        })?;

    let busy = row
        .get_value(0)
        .ok()
        .as_ref()
        .and_then(value_as_i64)
        .unwrap_or(0);
    let log_frames = row
        .get_value(1)
        .ok()
        .as_ref()
        .and_then(value_as_i64)
        .unwrap_or(0);
    let checkpointed_frames = row
        .get_value(2)
        .ok()
        .as_ref()
        .and_then(value_as_i64)
        .unwrap_or(0);

    if busy != 0 || checkpointed_frames < log_frames {
        return Err(TursoSnapshotError::CheckpointIncomplete {
            dataset,
            path: live_path,
            busy,
            log_frames,
            checkpointed_frames,
        });
    }
    Ok(())
}

fn value_as_i64(value: &turso::Value) -> Option<i64> {
    match value {
        turso::Value::Integer(n) => Some(*n),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    async fn write_rows(path: &Path, rows: &[(i64, &str)]) {
        let database = turso::Builder::new_local(path.to_str().expect("path"))
            .build()
            .await
            .expect("open turso");
        let conn = database.connect().expect("connect");
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT)", ())
            .await
            .expect("create");
        for (id, name) in rows {
            conn.execute(
                "INSERT INTO t(id, name) VALUES (?1, ?2)",
                (*id, (*name).to_string()),
            )
            .await
            .expect("insert");
        }
        // Drop the connection / database to release any in-process handles
        // before the engine reopens the file.
        drop(conn);
        drop(database);
    }

    async fn count_rows(path: &Path) -> i64 {
        let database = turso::Builder::new_local(path.to_str().expect("path"))
            .build()
            .await
            .expect("open verify");
        let conn = database.connect().expect("connect verify");
        let mut rows = conn
            .query("SELECT COUNT(*) FROM t", ())
            .await
            .expect("count query");
        let row = rows.next().await.expect("count step").expect("count row");
        match row.get_value(0).expect("count column") {
            turso::Value::Integer(n) => n,
            other => panic!("expected integer count, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn checkpoint_live_then_copy_captures_all_rows() {
        let tmp = TempDir::new().expect("tmp");
        let live = tmp.path().join("live.turso");
        let rows = vec![(1, "a"), (2, "b"), (3, "c")];
        write_rows(&live, &rows).await;

        let engine = TursoSnapshotEngine::new();
        engine
            .checkpoint_live(&live, "ds")
            .await
            .expect("checkpoint live");

        // Copy the main file only (no sidecars). If checkpoint truly
        // flushed the WAL, the copy must contain every row.
        let copy = tmp.path().join("copy.turso");
        std::fs::copy(&live, &copy).expect("copy");

        let final_path = engine
            .prepare_for_upload(&copy, "ds")
            .await
            .expect("prepare");

        assert_eq!(
            count_rows(&final_path).await,
            i64::try_from(rows.len()).expect("row count fits in i64")
        );
    }

    #[tokio::test]
    async fn checkpoint_live_returns_ok_on_empty_database() {
        let tmp = TempDir::new().expect("tmp");
        let live = tmp.path().join("empty.turso");
        // Create-and-close to materialize the file.
        {
            let database = turso::Builder::new_local(live.to_str().expect("path"))
                .build()
                .await
                .expect("open turso");
            let _ = database.connect().expect("connect");
        }

        let engine = TursoSnapshotEngine::new();
        engine
            .checkpoint_live(&live, "ds_empty")
            .await
            .expect("checkpoint live empty");
    }
}
