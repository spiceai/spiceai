/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! Changing a file-accelerated dataset and reloading the spicepod must leave that
//! dataset queryable.
//!
//! `mode: file_create` replaces the acceleration file during the reload, so an
//! engine whose cached pool keeps serving the replaced file reads its
//! `spice_sys` checkpoint from a file no longer at the configured path. Its two
//! checkpoint lookups then disagree: the refresh scheduler sees a current
//! checkpoint and schedules nothing, while readiness — decided from the path —
//! never arrives.
//!
//! The `DuckDB` `mode: file` and `SQLite` arms reach the reload with a usable
//! file either way, and are here to keep the assertions honest: an on-disk read
//! that cannot pass for an unaffected engine proves nothing about an affected
//! one.

use anyhow::anyhow;
use app::{App, AppBuilder};
use arrow::array::RecordBatch;
use runtime::Runtime;
use spicepod::acceleration::{Acceleration, Mode, RefreshMode};
use spicepod::component::dataset::Dataset;
use spicepod::param::Params;
use std::collections::HashMap;
use std::fmt::Write as _;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use crate::acceleration::load_runtime_datasets;
use crate::{
    configure_test_datafusion, init_tracing,
    utils::{register_test_connectors, run_query, test_request_context, wait_until_true},
};

const LOAD_TIMEOUT: Duration = Duration::from_mins(1);

/// Rows in the source before the reload, and after it. They differ so an
/// assertion on the post-reload count distinguishes "reloaded" from "still
/// serving what it had before".
const ROWS_BEFORE: u64 = 8;
const ROWS_AFTER: u64 = 16;

fn write_csv_source(path: &Path, rows: u64) -> Result<(), anyhow::Error> {
    let mut csv = String::from("id,payload\n");
    for id in 0..rows {
        writeln!(csv, "{id},payload_{id}")?;
    }
    std::fs::write(path, csv)?;
    Ok(())
}

fn file_accelerated_dataset(
    from: &str,
    name: &str,
    engine: &str,
    mode: &Mode,
    db_path: &str,
) -> Dataset {
    let mut dataset = Dataset::new(from, name);
    dataset.acceleration = Some(Acceleration {
        params: Some(Params::from_string_map(HashMap::from([(
            format!("{engine}_file"),
            db_path.to_string(),
        )]))),
        enabled: true,
        engine: Some(engine.to_string()),
        mode: mode.clone(),
        refresh_mode: Some(RefreshMode::Full),
        ..Acceleration::default()
    });
    dataset
}

fn row_count(batches: &[RecordBatch]) -> Result<i64, anyhow::Error> {
    batches
        .first()
        .filter(|b| b.num_rows() > 0)
        .ok_or_else(|| anyhow!("expected a count row, got {batches:?}"))?
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .map(|c| c.value(0))
        .ok_or_else(|| anyhow!("count column is not a BIGINT"))
}

async fn count_rows(rt: &Arc<Runtime>, dataset: &str) -> Result<i64, anyhow::Error> {
    row_count(&run_query(rt, &format!("select count(*) from {dataset}")).await?)
}

/// Loads a pod holding one file-accelerated dataset, grows its source, applies a
/// reload that changes that dataset, and requires it to serve `expected_after`
/// rows within [`LOAD_TIMEOUT`].
///
/// A wedged dataset fails every query with "Acceleration not ready", so this
/// fails rather than hangs, and reports the last count it saw.
///
/// Returns the still-live temporary directory alongside the acceleration file's
/// path, so a caller can inspect the file the shut-down runtime left behind.
async fn reload_changed_file_accelerated_dataset(
    pod_name: &str,
    engine: &str,
    mode: &Mode,
    expected_after: i64,
) -> Result<(tempfile::TempDir, std::path::PathBuf), anyhow::Error> {
    let dir = tempfile::tempdir()?;
    let source = dir.path().join("rows.csv");
    write_csv_source(&source, ROWS_BEFORE)?;
    let from = format!("file://{}", source.display());

    let db_file = dir.path().join(format!("{engine}_accelerated.db"));
    let db_param = db_file.to_string_lossy().to_string();

    let pod = |changed: bool| {
        let mut dataset = file_accelerated_dataset(&from, "reloaded", engine, mode, &db_param);
        if changed {
            dataset.params = Some(Params::from_string_map(HashMap::from([(
                "file_format".to_string(),
                "csv".to_string(),
            )])));
        }
        AppBuilder::new(pod_name).with_dataset(dataset).build()
    };

    configure_test_datafusion();
    let rt = Arc::new(Runtime::builder().with_app(pod(false)).build().await);
    load_runtime_datasets(&rt, LOAD_TIMEOUT).await?;

    let count = count_rows(&rt, "reloaded").await?;
    if count != i64::try_from(ROWS_BEFORE)? {
        return Err(anyhow!(
            "reloaded accelerated {count} rows, expected {ROWS_BEFORE}"
        ));
    }

    write_csv_source(&source, ROWS_AFTER)?;

    let reloaded: Arc<App> = Arc::new(pod(true));
    if !Arc::clone(&rt).apply_app(Arc::clone(&reloaded)).await {
        return Err(anyhow!("the runtime did not apply the reloaded spicepod"));
    }

    let served = wait_until_true(LOAD_TIMEOUT, || async {
        count_rows(&rt, "reloaded").await.ok() == Some(expected_after)
    })
    .await;
    if !served {
        let observed = count_rows(&rt, "reloaded")
            .await
            .map_or_else(|e| format!("{e}"), |count| format!("{count} rows"));
        return Err(anyhow!(
            "the changed {engine} {mode} dataset never served {expected_after} rows after the reload: {observed}"
        ));
    }

    // Dropping the runtime releases the accelerator's pool, which is what closes
    // the engine; a caller reading the acceleration file needs it closed, not
    // merely idle.
    rt.shutdown().await;
    drop(rt);

    if !db_file.exists() {
        return Err(anyhow!(
            "the reload left no acceleration file at {}",
            db_file.display()
        ));
    }

    Ok((dir, db_file))
}

/// Counts the rows a `DuckDB` database file holds, opening it directly rather
/// than through the runtime, so the assertion observes what is on disk at the
/// configured path — a table served out of a file that was replaced at that path
/// answers queries normally while the path holds something else entirely.
#[cfg(feature = "duckdb")]
async fn rows_on_disk(db_file: &Path, table: &str) -> Result<i64, anyhow::Error> {
    use datafusion_table_providers::sql::db_connection_pool::DbConnectionPool;
    use futures::TryStreamExt;

    // A retired `DuckDB` instance releases its file only once the last pooled
    // connection drains, and it is that close which checkpoints the write-ahead
    // log into the database file. Reading before then reads a half-written file.
    let wal = db_file.with_extension("db.wal");
    if !wait_until_true(LOAD_TIMEOUT, || async { !wal.exists() }).await {
        return Err(anyhow!(
            "{} still had a write-ahead log after the runtime was dropped",
            db_file.display()
        ));
    }

    let db_path = db_file.to_string_lossy().to_string();
    // `ReadWrite`, not `ReadOnly`: a database whose last writes are still in its
    // WAL only shows them once the WAL is replayed, which a read-only open
    // cannot do.
    let pool = datafusion_table_providers::sql::db_connection_pool::duckdbpool::DuckDbConnectionPool::new_file(
        &db_path,
        &duckdb::AccessMode::ReadWrite,
    )
    .map_err(|e| anyhow!("failed to open {db_path}: {e}"))?;
    let conn_dyn = pool
        .connect()
        .await
        .map_err(|e| anyhow!("failed to connect to {db_path}: {e}"))?;
    let conn = conn_dyn
        .as_sync()
        .ok_or_else(|| anyhow!("expected a sync DuckDB connection"))?;

    let batches: Vec<RecordBatch> = conn
        .query_arrow(&format!("select count(*) from \"{table}\""), &[], None)
        .map_err(|e| anyhow!("count query failed: {e}"))?
        .try_collect::<Vec<RecordBatch>>()
        .await
        .map_err(|e| anyhow!("collecting the count failed: {e}"))?;

    row_count(&batches)
}

/// `mode: file` keeps its acceleration file across the reload, so its checkpoint
/// survives and the dataset is ready immediately, serving the rows it already
/// holds until its next refresh. What it must not do is wedge.
#[cfg(feature = "duckdb")]
#[tokio::test]
async fn test_reload_changed_duckdb_file_dataset() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope_retry(3, || async {
            let expected = i64::try_from(ROWS_BEFORE)?;
            let (_dir, db_file) = reload_changed_file_accelerated_dataset(
                "test_reload_changed_duckdb_file",
                "duckdb",
                &Mode::File,
                expected,
            )
            .await?;

            let on_disk = rows_on_disk(&db_file, "reloaded").await?;
            if on_disk != expected {
                return Err(anyhow!(
                    "the acceleration file at {} holds {on_disk} rows, expected {expected}",
                    db_file.display()
                ));
            }
            Ok(())
        })
        .await
}

/// `SQLite` reopens its database file on every pool handout, so it recreates the
/// file `file_create` removed on its own. This pins that.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_reload_changed_sqlite_file_create_dataset() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope_retry(3, || async {
            reload_changed_file_accelerated_dataset(
                "test_reload_changed_sqlite_file_create",
                "sqlite",
                &Mode::FileCreate,
                i64::try_from(ROWS_AFTER)?,
            )
            .await
            .map(|_| ())
        })
        .await
}

/// The Turso counterpart of [`rows_on_disk`].
#[cfg(feature = "turso")]
async fn turso_rows_on_disk(db_file: &Path, table: &str) -> Result<i64, anyhow::Error> {
    let db_path = db_file.to_string_lossy().to_string();
    let pool = data_components::turso::TursoConnectionPool::new(&db_path)
        .await
        .map_err(|e| anyhow!("failed to open {db_path}: {e}"))?;
    let conn = pool
        .connect()
        .await
        .map_err(|e| anyhow!("failed to connect to {db_path}: {e}"))?;

    let mut rows = conn
        .query(&format!("select count(*) from \"{table}\""), ())
        .await
        .map_err(|e| anyhow!("count query failed: {e}"))?;
    let row = rows
        .next()
        .await
        .map_err(|e| anyhow!("reading the count failed: {e}"))?
        .ok_or_else(|| anyhow!("count query returned no rows"))?;

    match row
        .get_value(0)
        .map_err(|e| anyhow!("reading the count column failed: {e}"))?
    {
        turso::Value::Integer(count) => Ok(count),
        other => Err(anyhow!("count column is not an integer: {other:?}")),
    }
}

#[cfg(feature = "turso")]
#[tokio::test]
async fn test_reload_changed_turso_file_create_dataset() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope_retry(3, || async {
            let expected = i64::try_from(ROWS_AFTER)?;
            let (_dir, db_file) = reload_changed_file_accelerated_dataset(
                "test_reload_changed_turso_file_create",
                "turso",
                &Mode::FileCreate,
                expected,
            )
            .await?;

            let on_disk = turso_rows_on_disk(&db_file, "reloaded").await?;
            if on_disk != expected {
                return Err(anyhow!(
                    "the acceleration file at {} holds {on_disk} rows, expected {expected}: the reloaded data went somewhere else",
                    db_file.display()
                ));
            }
            Ok(())
        })
        .await
}
