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

//! `on_full_refresh: replace_file` — a full refresh streams into a fresh `DuckDB`
//! database file which is then atomically swapped over the configured path.
//!
//! The swap's correctness rests on two invariants that these tests exercise:
//!
//! * every object in the live file that does **not** belong to the refreshed
//!   dataset is carried forward into the new file — other datasets' tables,
//!   views and indexes, and the `spice_sys_*` metadata tables; and
//! * every writer to the shared file holds the pool's write gate, so no write
//!   can land in a file that is about to be retired.
//!
//! Readers are never blocked, so queries must keep succeeding — and keep
//! returning correct results — across any number of swaps.

use crate::acceleration::{load_runtime_datasets, wait_for_checkpoints};
use app::AppBuilder;
use arrow::array::RecordBatch;
use datafusion_table_providers::sql::db_connection_pool::DbConnectionPool;
use datafusion_table_providers::sql::db_connection_pool::duckdbpool::DuckDbConnectionPool;
use duckdb::AccessMode;
use futures::TryStreamExt;

use anyhow::anyhow;
use runtime::Runtime;
use runtime::dataaccelerator::spice_sys::{OpenOption, dataset_checkpoint::DatasetCheckpoint};
use spicepod::acceleration::{Acceleration, Mode, RefreshMode};
use spicepod::component::dataset::Dataset;
use spicepod::param::Params;
use std::fmt::Write as _;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use crate::{
    configure_test_datafusion, init_tracing,
    utils::{register_test_connectors, run_query, test_request_context, wait_until_true},
};

const LOAD_TIMEOUT: Duration = Duration::from_mins(1);

/// Staging and generation files (`{db}.refresh.*`) left beside `db_file`. These
/// belong to the replacement protocol and none may survive a completed
/// replacement.
///
/// A `{db}.wal` is deliberately *not* counted: it is ordinary `DuckDB` state from
/// whatever committed most recently, so a test that writes right up to shutdown
/// legitimately leaves one behind for the next open to replay. Only the tests
/// that end on a completed replacement with no writes after it assert on it, via
/// [`wal_beside`].
fn replacement_debris_beside(db_file: &Path) -> Result<Vec<String>, anyhow::Error> {
    let dir = db_file.parent().unwrap_or(Path::new("."));
    let Some(file_name) = db_file.file_name().and_then(|n| n.to_str()) else {
        return Ok(Vec::new());
    };
    let generation_prefix = format!("{file_name}.refresh.");

    Ok(std::fs::read_dir(dir)?
        .filter_map(Result::ok)
        .map(|e| e.file_name().to_string_lossy().to_string())
        .filter(|name| name.starts_with(&generation_prefix))
        .collect())
}

/// Whether a write-ahead log sits beside `db_file`.
fn wal_beside(db_file: &Path) -> bool {
    let mut wal = db_file.as_os_str().to_os_string();
    wal.push(".wal");
    Path::new(&wal).exists()
}

/// Polls until no staging/generation debris remains beside `db_file`. Retired
/// `DuckDB` instances only release their files once the last pooled connection
/// drains, so the check has to wait for the actual condition.
async fn wait_for_replacements_to_settle(db_file: &Path) -> Result<(), anyhow::Error> {
    let settled = wait_until_true(Duration::from_secs(30), || async {
        replacement_debris_beside(db_file).is_ok_and(|debris| debris.is_empty())
    })
    .await;

    if settled {
        Ok(())
    } else {
        Err(anyhow!(
            "file replacement left debris behind: {:?}",
            replacement_debris_beside(db_file)?
        ))
    }
}

/// Reads one row of `N` `BIGINT` columns from `batches`, erroring rather than
/// panicking on an unexpected shape.
fn bigint_row<const N: usize>(batches: &[RecordBatch]) -> Result<[i64; N], anyhow::Error> {
    let batch = batches
        .first()
        .filter(|b| b.num_rows() > 0 && b.num_columns() >= N)
        .ok_or_else(|| anyhow!("expected one row of {N} counts, got {batches:?}"))?;

    let mut counts = [0i64; N];
    for (i, count) in counts.iter_mut().enumerate() {
        *count = batch
            .column(i)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .map(|c| c.value(0))
            .ok_or_else(|| anyhow!("count column {i} is not a BIGINT"))?;
    }
    Ok(counts)
}

/// Opens the database file directly (outside the runtime's pool) and reads one
/// row of `N` `BIGINT` counts, so assertions observe what is actually on disk.
async fn counts_on_disk<const N: usize>(
    db_path: &str,
    sql: &str,
) -> Result<[i64; N], anyhow::Error> {
    let pool = DuckDbConnectionPool::new_file(db_path, &AccessMode::ReadWrite)
        .map_err(|e| anyhow!("failed to open {db_path}: {e}"))?;
    let conn_dyn = pool
        .connect()
        .await
        .map_err(|e| anyhow!("failed to connect to {db_path}: {e}"))?;
    let conn = conn_dyn
        .as_sync()
        .ok_or_else(|| anyhow!("expected a sync DuckDB connection"))?;

    let batches: Vec<RecordBatch> = conn
        .query_arrow(sql, &[], None)
        .map_err(|e| anyhow!("query '{sql}' failed: {e}"))?
        .try_collect::<Vec<RecordBatch>>()
        .await
        .map_err(|e| anyhow!("collecting '{sql}' failed: {e}"))?;

    bigint_row(&batches)
}

fn acceleration_params(path: &str, on_full_refresh: Option<&str>) -> Params {
    let mut params = vec![("duckdb_file".to_string(), path.to_string())];
    if let Some(mode) = on_full_refresh {
        params.push(("on_full_refresh".to_string(), mode.to_string()));
    }
    Params::from_string_map(params.into_iter().collect())
}

/// A dataset accelerated into the shared `path`, with `on_full_refresh` and the
/// refresh cadence under the caller's control.
fn replace_file_dataset(
    from: &str,
    name: &str,
    path: &str,
    refresh_mode: RefreshMode,
    on_full_refresh: Option<&str>,
    refresh_check_interval: Option<&str>,
) -> Dataset {
    let mut dataset = Dataset::new(from, name);
    dataset.acceleration = Some(Acceleration {
        params: Some(acceleration_params(path, on_full_refresh)),
        enabled: true,
        engine: Some("duckdb".to_string()),
        mode: Mode::File,
        refresh_mode: Some(refresh_mode),
        refresh_check_interval: refresh_check_interval.map(ToString::to_string),
        refresh_sql: None,
        ..Acceleration::default()
    });
    dataset
}

fn full_refresh_replace_file_dataset(from: &str, name: &str, path: &str) -> Dataset {
    replace_file_dataset(
        from,
        name,
        path,
        RefreshMode::Full,
        Some("replace_file"),
        None,
    )
}

/// Writes `rows` of incompressible data to a CSV `file://` source. `md5` output
/// defeats `DuckDB`'s compression, so a database file's size tracks the rows it
/// holds rather than block-count noise — which is what makes the growth
/// assertion in `..._bounded_growth_under_query_load` meaningful.
fn write_csv_source(path: &Path, rows: u64) -> Result<(), anyhow::Error> {
    let mut csv = String::from("id,payload\n");
    for id in 0..rows {
        // A 32-char pseudo-random payload derived from the id, generated here
        // rather than by DuckDB so the source is a plain file.
        let payload = format!("{:032x}", id.wrapping_mul(0x9E37_79B9_7F4A_7C15));
        writeln!(csv, "{id},{payload}")?;
    }
    std::fs::write(path, csv)?;
    Ok(())
}

/// Loads a runtime that is *expected* to reject a dataset, and returns that
/// dataset's error message. Unlike [`load_runtime_datasets`] this must not
/// assert the runtime becomes ready — the point is that a dataset does not load.
async fn dataset_error_message(rt: &Arc<Runtime>, dataset: &str) -> Result<String, anyhow::Error> {
    let loading = tokio::spawn(Arc::clone(rt).load_components());

    let table_ref = datafusion::sql::TableReference::bare(dataset.to_string());
    let status = rt.datafusion().runtime_status();

    let became_error = wait_until_true(LOAD_TIMEOUT, || {
        let status = Arc::clone(&status);
        let table_ref = table_ref.clone();
        async move {
            status
                .get_dataset_status(&table_ref)
                .is_some_and(|s| s.is_error())
        }
    })
    .await;

    let observed = status.get_dataset_status(&table_ref);
    loading.abort();

    if !became_error {
        return Err(anyhow!(
            "expected dataset '{dataset}' to be rejected, but its status is {observed:?}"
        ));
    }

    Ok(observed
        .and_then(|s| s.error_message().map(ToString::to_string))
        .unwrap_or_default())
}

/// A swap and a `refresh_mode: snapshot` reload are two independent mechanisms
/// that both replace the same database file out-of-band — a snapshot reload even
/// evicts the shared pool and builds a new one. Whichever runs second unlinks
/// the file the other just installed, leaving the pool and the configured path
/// pointing at different data (queries then disagree depending on when their
/// connection was checked out). The combination must be refused at
/// configuration time rather than raced at runtime.
#[tokio::test]
async fn test_duckdb_file_swap_rejects_snapshot_refresh_peer_on_same_file()
-> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope_retry(3, || async {
            let dir = tempfile::tempdir()?;
            let db_path = dir.path().join("snapshot_peer.db").to_string_lossy().to_string();

            let mut snapshot_peer = replace_file_dataset(
                "https://public-data.spiceai.org/eth.recent_logs.parquet",
                "snapshot_peer",
                &db_path,
                RefreshMode::Snapshot,
                None,
                None,
            );
            if let Some(acceleration) = snapshot_peer.acceleration.as_mut() {
                acceleration.snapshots = spicepod::acceleration::SnapshotBehavior::Enabled;
            }

            let app = AppBuilder::new("test_duckdb_file_swap_rejects_snapshot_peer")
                .with_dataset(full_refresh_replace_file_dataset(
                    "https://public-data.spiceai.org/decimal.parquet",
                    "swapper",
                    &db_path,
                ))
                .with_dataset(snapshot_peer)
                .build();

            configure_test_datafusion();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);

            let message = dataset_error_message(&rt, "swapper").await?;
            if !message.contains("refresh_mode: snapshot") {
                return Err(anyhow!(
                    "expected the rejection to name the conflicting snapshot dataset, got: {message}"
                ));
            }

            rt.shutdown().await;
            Ok(())
        })
        .await
}

/// A dataset accelerated into a *different* `DuckDB` file `ATTACH`es this
/// dataset's file, and `DuckDB` binds that attachment to a file once per instance.
/// A file replacement therefore leaves the peer instance holding the retired
/// file, which would serve pre-replacement data to cross-file federated queries
/// until the process restarted. Attachments re-resolve themselves when the file
/// underneath them changes, so both datasets must stay queryable and consistent
/// across repeated replacements — including through a join that forces the
/// cross-file attachment to be used.
#[tokio::test]
async fn test_duckdb_file_replace_refreshes_cross_file_attachment() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope_retry(3, || async {
            let dir = tempfile::tempdir()?;
            let replaced_db = dir
                .path()
                .join("replaced_side.db")
                .to_string_lossy()
                .to_string();
            let peer_db = dir.path().join("peer_side.db").to_string_lossy().to_string();

            let app = AppBuilder::new("test_duckdb_file_replace_cross_file_attachment")
                // Replaces its file repeatedly.
                .with_dataset(replace_file_dataset(
                    "https://public-data.spiceai.org/decimal.parquet",
                    "replaced",
                    &replaced_db,
                    RefreshMode::Full,
                    Some("replace_file"),
                    Some("1s"),
                ))
                // Its own file, so its instance ATTACHes `replaced_db`.
                .with_dataset(replace_file_dataset(
                    "https://public-data.spiceai.org/eth.recent_logs.parquet",
                    "other_file",
                    &peer_db,
                    RefreshMode::Full,
                    None,
                    None,
                ))
                .build();

            configure_test_datafusion();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);
            let runtime_datasets = load_runtime_datasets(&rt, LOAD_TIMEOUT).await?;
            wait_for_checkpoints(runtime_datasets, 120).await?;

            let batches = run_query(&rt, "SELECT COUNT(1)::BIGINT FROM replaced").await?;
            let [expected] = bigint_row::<1>(&batches)?;
            if expected == 0 {
                return Err(anyhow!("'replaced' loaded no rows"));
            }

            // Keep querying across many replacements. A stale attachment shows up
            // as a wrong count (the retired file's contents) or a hard failure
            // once the retired file is unlinked.
            let mut samples = 0;
            let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
            while tokio::time::Instant::now() < deadline {
                // Force the cross-file attachment to participate.
                let batches = run_query(
                    &rt,
                    "SELECT COUNT(1)::BIGINT FROM replaced
                     WHERE EXISTS (SELECT 1 FROM other_file)",
                )
                .await?;
                let [observed] = bigint_row::<1>(&batches)?;
                if observed != expected {
                    return Err(anyhow!(
                        "cross-file query read {observed} rows, expected {expected}, after {samples} samples — a replaced file's attachment was not re-resolved"
                    ));
                }
                samples += 1;
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            if samples < 10 {
                return Err(anyhow!(
                    "expected to sample across many replacements, only sampled {samples}"
                ));
            }

            rt.shutdown().await;
            drop(rt);
            wait_for_replacements_to_settle(Path::new(&replaced_db)).await?;

            Ok(())
        })
        .await
}

/// T1 — datasets on **different refresh schedules and `on_full_refresh` modes**
/// share one `DuckDB` file without conflicting or producing inconsistent results.
///
/// Two datasets full-refresh via `replace_file` on different intervals (so their
/// swaps overlap and must serialize on the per-file write gate), a third
/// full-refreshes **in place** on the same file, and a fourth is refreshed only
/// once at startup.
///
/// The sharp invariant is on that fourth dataset: it is never refreshed again,
/// so every one of its rows in the final file got there by being *carried
/// forward* through each swap. Its row count is sampled continuously while the
/// others swap — a swap that dropped or staled a sibling shows up immediately as
/// a changed count or a missing table, which a final-state assertion alone can
/// miss.
#[tokio::test]
async fn test_duckdb_file_swap_mixed_refresh_modes_share_one_file() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope_retry(3, || async {
            let dir = tempfile::tempdir()?;
            let db_file = dir.path().join("mixed_modes.db");
            let db_path = db_file.to_string_lossy().to_string();

            let app = AppBuilder::new("test_duckdb_file_swap_mixed_refresh_modes")
                // Swaps repeatedly underneath the others.
                .with_dataset(replace_file_dataset(
                    "https://public-data.spiceai.org/decimal.parquet",
                    "swapper",
                    &db_path,
                    RefreshMode::Full,
                    Some("replace_file"),
                    Some("2s"),
                ))
                // A second swapper on a different cadence: overlapping swaps
                // must serialize on the per-file write gate rather than racing.
                .with_dataset(replace_file_dataset(
                    "https://public-data.spiceai.org/eth.recent_logs.parquet",
                    "swapper_two",
                    &db_path,
                    RefreshMode::Full,
                    Some("replace_file"),
                    Some("3s"),
                ))
                // A reuse_file (default) full refresh on the same file: mixing the two
                // `on_full_refresh` modes must not lose either one's data.
                .with_dataset(replace_file_dataset(
                    "https://public-data.spiceai.org/decimal.parquet",
                    "reuse_file_ds",
                    &db_path,
                    RefreshMode::Full,
                    Some("reuse_file"),
                    Some("2s"),
                ))
                // Refreshed once at startup and never again: its rows only stay
                // present if every swap carries them forward.
                .with_dataset(replace_file_dataset(
                    "https://public-data.spiceai.org/eth.recent_logs.parquet",
                    "carried_forward",
                    &db_path,
                    RefreshMode::Full,
                    None,
                    None,
                ))
                .build();

            configure_test_datafusion();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);
            let runtime_datasets = load_runtime_datasets(&rt, LOAD_TIMEOUT).await?;

            wait_for_checkpoints(runtime_datasets, 120).await?;

            // Establish the never-refreshed dataset's row count, then hold it
            // to that exact count while the other three refresh underneath.
            let batches = run_query(&rt, "SELECT COUNT(1)::BIGINT FROM carried_forward").await?;
            let [expected_carried] = bigint_row::<1>(&batches)?;
            if expected_carried == 0 {
                return Err(anyhow!("'carried_forward' loaded no rows; nothing to carry"));
            }

            let mut samples = 0;
            let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
            while tokio::time::Instant::now() < deadline {
                let batches =
                    run_query(&rt, "SELECT COUNT(1)::BIGINT FROM carried_forward").await?;
                let [carried] = bigint_row::<1>(&batches)?;
                if carried != expected_carried {
                    return Err(anyhow!(
                        "a swap failed to carry 'carried_forward' forward intact: expected {expected_carried} rows, read {carried} after {samples} samples"
                    ));
                }
                samples += 1;
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            if samples < 10 {
                return Err(anyhow!(
                    "expected to sample across many swaps, only sampled {samples} times"
                ));
            }

            rt.shutdown().await;
            drop(rt);
            wait_for_replacements_to_settle(&db_file).await?;

            // All four datasets, and every dataset's checkpoint, live in the
            // final swapped-in file.
            let [swapper, swapper_two, reuse_file_ds, carried, checkpoints] = counts_on_disk::<5>(
                &db_path,
                "SELECT (SELECT COUNT(1) FROM swapper)::BIGINT,
                        (SELECT COUNT(1) FROM swapper_two)::BIGINT,
                        (SELECT COUNT(1) FROM reuse_file_ds)::BIGINT,
                        (SELECT COUNT(1) FROM carried_forward)::BIGINT,
                        (SELECT COUNT(1) FROM spice_sys_dataset_checkpoint)::BIGINT",
            )
            .await?;

            if swapper == 0 || swapper_two == 0 || reuse_file_ds == 0 {
                return Err(anyhow!(
                    "every refreshing dataset must survive the swaps (swapper={swapper}, swapper_two={swapper_two}, reuse_file_ds={reuse_file_ds})"
                ));
            }
            if carried != expected_carried {
                return Err(anyhow!(
                    "'carried_forward' must end with the rows it loaded once: expected {expected_carried}, found {carried}"
                ));
            }
            if checkpoints != 4 {
                return Err(anyhow!(
                    "expected all 4 dataset checkpoints in the swapped-in file, found {checkpoints}"
                ));
            }

            Ok(())
        })
        .await
}

/// T2 — the database file must not grow without bound across repeated
/// `replace_file` refreshes, **while queries are in flight**.
///
/// Three overlapping query tasks hold pooled connections for the whole run, so
/// each swap has to complete against a live, in-use instance. Every query must
/// succeed and return the same correct row count: a reader may never observe a
/// half-swapped file.
#[tokio::test]
async fn test_duckdb_file_swap_bounded_growth_under_query_load() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope_retry(3, || async {
            const SOURCE_ROWS: u64 = 20_000;

            let dir = tempfile::tempdir()?;
            let db_file = dir.path().join("bounded_growth.db");
            let db_path = db_file.to_string_lossy().to_string();

            // A fixed-size source: the accelerated table holds the same rows
            // after every refresh, so a growing file can only be unreclaimed
            // space from the replaced generations.
            let source = dir.path().join("growth_source.csv");
            write_csv_source(&source, SOURCE_ROWS)?;
            let from = format!("file://{}", source.display());

            let app = AppBuilder::new("test_duckdb_file_swap_bounded_growth")
                .with_dataset(replace_file_dataset(
                    &from,
                    "fixed",
                    &db_path,
                    RefreshMode::Full,
                    Some("replace_file"),
                    Some("1s"),
                ))
                .build();

            configure_test_datafusion();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);
            let runtime_datasets = load_runtime_datasets(&rt, LOAD_TIMEOUT).await?;
            wait_for_checkpoints(runtime_datasets, 120).await?;

            // Three overlapping query tasks for the duration of the refreshes.
            let stop = Arc::new(AtomicBool::new(false));
            let queries_run = Arc::new(AtomicU64::new(0));
            let query_tasks: Vec<_> = (0..3)
                .map(|task| {
                    let rt = Arc::clone(&rt);
                    let stop = Arc::clone(&stop);
                    let queries_run = Arc::clone(&queries_run);
                    tokio::spawn(async move {
                        while !stop.load(Ordering::Relaxed) {
                            let batches =
                                run_query(&rt, "SELECT COUNT(1)::BIGINT FROM fixed").await?;
                            let [rows] = bigint_row::<1>(&batches)?;
                            // A reader must never see a partially swapped file.
                            if rows != i64::try_from(SOURCE_ROWS)? {
                                return Err(anyhow!(
                                    "query task {task} read {rows} rows, expected {SOURCE_ROWS}"
                                ));
                            }
                            queries_run.fetch_add(1, Ordering::Relaxed);
                            tokio::time::sleep(Duration::from_millis(50)).await;
                        }
                        Ok::<(), anyhow::Error>(())
                    })
                })
                .collect();

            // Let the file reach steady state before taking the baseline: the
                // first swap still carries the pre-swap generation's cost.
            tokio::time::sleep(Duration::from_secs(5)).await;
            let baseline = std::fs::metadata(&db_file)?.len();

            // Keep refreshing well past the baseline. Unbounded growth (the bug
            // this feature fixes) compounds per refresh, so ~15s of 1s refreshes
            // is far more than enough to separate "flat" from "growing".
            tokio::time::sleep(Duration::from_secs(15)).await;
            let after = std::fs::metadata(&db_file)?.len();

            stop.store(true, Ordering::Relaxed);
            for task in query_tasks {
                task.await??;
            }

            let queries = queries_run.load(Ordering::Relaxed);
            if queries < 30 {
                return Err(anyhow!(
                    "expected sustained query load across the refreshes, only {queries} queries completed"
                ));
            }

            // Steady state: bounded by a generous factor so the assertion is
            // about "not growing per refresh", not about exact block counts.
            if after > baseline.saturating_mul(3) / 2 {
                return Err(anyhow!(
                    "database file grew across repeated swap refreshes: baseline={baseline} bytes, after={after} bytes ({queries} queries in flight)"
                ));
            }

            rt.shutdown().await;
            drop(rt);
            wait_for_replacements_to_settle(&db_file).await?;

            Ok(())
        })
        .await
}

/// T3 — an out-of-band writer must not lose writes to a concurrent swap.
///
/// Every writer to the shared file takes the pool's write gate before checking
/// out a connection; without it a write commits into the instance the swap is
/// retiring and is silently dropped from the new file. Here a dataset
/// checkpoint is re-persisted in a tight loop while another dataset swaps, and
/// the checkpoint must still be present afterwards.
#[tokio::test]
async fn test_duckdb_file_swap_preserves_concurrent_out_of_band_writes() -> Result<(), anyhow::Error>
{
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope_retry(3, || async {
            let dir = tempfile::tempdir()?;
            let db_file = dir.path().join("out_of_band.db");
            let db_path = db_file.to_string_lossy().to_string();

            let app = AppBuilder::new("test_duckdb_file_swap_out_of_band_writes")
                .with_dataset(replace_file_dataset(
                    "https://public-data.spiceai.org/decimal.parquet",
                    "swapper",
                    &db_path,
                    RefreshMode::Full,
                    Some("replace_file"),
                    Some("1s"),
                ))
                .with_dataset(replace_file_dataset(
                    "https://public-data.spiceai.org/eth.recent_logs.parquet",
                    "writer",
                    &db_path,
                    RefreshMode::Full,
                    None,
                    None,
                ))
                .build();

            configure_test_datafusion();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);
            let runtime_datasets = load_runtime_datasets(&rt, LOAD_TIMEOUT).await?;
            wait_for_checkpoints(runtime_datasets.clone(), 120).await?;

            let writer_dataset = runtime_datasets
                .iter()
                .find(|ds| ds.name.to_string() == "writer")
                .ok_or_else(|| anyhow!("'writer' dataset not found"))?
                .clone();
            let registry = writer_dataset.runtime.accelerator_engine_registry();
            let checkpoint =
                DatasetCheckpoint::try_new(&writer_dataset, registry, OpenOption::OpenExisting)
                    .await
                    .map_err(|e| anyhow!("failed to open the writer's checkpoint: {e}"))?;
            let schema = Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new("marker", arrow::datatypes::DataType::Int64, true),
            ]));

            // Re-persist the checkpoint repeatedly while `swapper` swaps the
            // file underneath. Each upsert is an out-of-band write to the
            // shared file and must land in whichever file survives.
            let mut upserts = 0;
            let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
            while tokio::time::Instant::now() < deadline {
                checkpoint
                    .checkpoint(&schema, None)
                    .await
                    .map_err(|e| anyhow!("checkpoint upsert {upserts} failed during a swap: {e}"))?;
                upserts += 1;
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            if upserts < 20 {
                return Err(anyhow!(
                    "expected sustained out-of-band writes across the swaps, got {upserts}"
                ));
            }

            rt.shutdown().await;
            drop(rt);
            wait_for_replacements_to_settle(&db_file).await?;

            // The acknowledged checkpoints must be readable from the file the
            // swaps left behind — and no foreign WAL may sit beside it (that is
            // asserted by `wait_for_replacements_to_settle` above).
            let [checkpoints] = counts_on_disk::<1>(
                &db_path,
                "SELECT COUNT(1)::BIGINT FROM spice_sys_dataset_checkpoint",
            )
            .await?;
            if checkpoints != 2 {
                return Err(anyhow!(
                    "expected both datasets' checkpoints to survive {upserts} concurrent upserts across the swaps, found {checkpoints}"
                ));
            }

            Ok(())
        })
        .await
}

/// T4 — an interrupted swap is recovered at the next startup.
///
/// A crash can leave a `.building` staging file (always discarded) or a
/// completed generation file whose rename over the configured path never
/// happened (adopted, because the old file was already unlinked). Boot recovery
/// must normalize both before any pool opens the file, and queries must then
/// serve the adopted data.
///
/// The second runtime is configured at a *different* path than the first, which
/// is what a real second boot looks like from recovery's point of view: recovery
/// runs at most once per path per process, so reusing the first path here would
/// simply skip it and assert nothing.
#[tokio::test]
async fn test_duckdb_file_swap_recovers_interrupted_swap_on_boot() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope_retry(3, || async {
            let dir = tempfile::tempdir()?;
            let db_file = dir.path().join("recovery.db");
            let db_path = db_file.to_string_lossy().to_string();

            // First lifecycle: produce a real, fully populated database file.
            let app = AppBuilder::new("test_duckdb_file_swap_recovery_seed")
                .with_dataset(full_refresh_replace_file_dataset(
                    "https://public-data.spiceai.org/decimal.parquet",
                    "decimal",
                    &db_path,
                ))
                .build();

            configure_test_datafusion();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);
            let runtime_datasets = load_runtime_datasets(&rt, LOAD_TIMEOUT).await?;
            wait_for_checkpoints(runtime_datasets, 120).await?;
            rt.shutdown().await;
            drop(rt);
            wait_for_replacements_to_settle(&db_file).await?;

            let [seeded_rows] =
                counts_on_disk::<1>(&db_path, "SELECT COUNT(1)::BIGINT FROM decimal").await?;

            // Simulate a crash after the generation was completed but before it
            // was renamed over the configured path: the configured file is gone
            // and only the generation survives, alongside `.building` debris
            // from a staging load that never finished.
            let recovered_file = dir.path().join("recovered.db");
            let recovered_path = recovered_file.to_string_lossy().to_string();
            let generation = dir.path().join("recovered.db.refresh.1700000000000-0");
            let building = dir
                .path()
                .join("recovered.db.refresh.1700000000001-1.building");
            std::fs::rename(&db_file, &generation)?;
            std::fs::write(&building, b"incomplete staging output")?;

            // Second lifecycle: boot recovery adopts the generation and
            // discards the staging leftover.
            let app = AppBuilder::new("test_duckdb_file_swap_recovery_adopt")
                .with_dataset(full_refresh_replace_file_dataset(
                    "https://public-data.spiceai.org/decimal.parquet",
                    "decimal",
                    &recovered_path,
                ))
                .build();

            let rt = Arc::new(Runtime::builder().with_app(app).build().await);
            let runtime_datasets = load_runtime_datasets(&rt, LOAD_TIMEOUT).await?;
            wait_for_checkpoints(runtime_datasets, 120).await?;

            if building.exists() {
                return Err(anyhow!(
                    "boot recovery must discard incomplete staging files, but {} remains",
                    building.display()
                ));
            }
            if generation.exists() {
                return Err(anyhow!(
                    "boot recovery must consume the adopted generation, but {} remains",
                    generation.display()
                ));
            }
            if !recovered_file.exists() {
                return Err(anyhow!(
                    "boot recovery must restore the configured path {}",
                    recovered_file.display()
                ));
            }

            // The adopted file serves queries.
            let batches = run_query(&rt, "SELECT COUNT(1)::BIGINT FROM decimal").await?;
            let [rows] = bigint_row::<1>(&batches)?;
            if rows != seeded_rows {
                return Err(anyhow!(
                    "expected the recovered file to serve {seeded_rows} rows, got {rows}"
                ));
            }

            rt.shutdown().await;
            drop(rt);
            wait_for_replacements_to_settle(&recovered_file).await?;

            Ok(())
        })
        .await
}

/// Two datasets share one `DuckDB` file with `on_full_refresh: replace_file`; both
/// full refreshes run concurrently at startup, so the swaps serialize on the
/// per-file write gate and each must carry the other's data forward. The
/// database file must end up as a fresh generation (new inode) at the
/// configured path, with both datasets' data and checkpoints and no swap
/// debris left behind.
#[tokio::test]
async fn test_acceleration_duckdb_full_refresh_file_swap() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope_retry(3, || async {
            // A per-test directory keeps the debris assertion below scoped to
            // this test's artifacts and leaves nothing in the checkout.
            let dir = tempfile::tempdir()?;
            let db_file = dir.path().join("file_swap_duckdb.db");
            let db_path = db_file.to_string_lossy().to_string();

            // Pre-create the database file so the refresh provably replaces it
            // (the file swap produces a new inode at the configured path).
            DuckDbConnectionPool::new_file(&db_path, &AccessMode::ReadWrite).expect("valid path");
            #[cfg(unix)]
            let initial_inode = {
                use std::os::unix::fs::MetadataExt;
                std::fs::metadata(&db_file)?.ino()
            };

            let app = AppBuilder::new("test_acceleration_duckdb_full_refresh_file_swap")
                .with_dataset(full_refresh_replace_file_dataset(
                    "https://public-data.spiceai.org/decimal.parquet",
                    "decimal",
                    &db_path,
                ))
                .with_dataset(full_refresh_replace_file_dataset(
                    "https://public-data.spiceai.org/eth.recent_logs.parquet",
                    "logs",
                    &db_path,
                ))
                .build();

            configure_test_datafusion();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);
            let runtime_datasets = load_runtime_datasets(&rt, LOAD_TIMEOUT).await?;

            // Verify checkpoints are created before shutting down runtime
            wait_for_checkpoints(runtime_datasets, 120).await?;

            rt.shutdown().await;
            drop(rt);
            wait_for_replacements_to_settle(&db_file).await?;

            // Nothing writes after the final replacement here, so the file it
            // left behind must be the checkpointed, WAL-free one it produced.
            if wal_beside(&db_file) {
                return Err(anyhow!(
                    "a completed replacement must leave a checkpointed, WAL-free file"
                ));
            }

            // The swap replaced the file at the configured path with a fresh
            // generation.
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt;
                let swapped_inode = std::fs::metadata(&db_file)?.ino();
                if swapped_inode == initial_inode {
                    return Err(anyhow!(
                        "expected the full refresh to swap in a new database file, but the inode is unchanged"
                    ));
                }
            }

            // Both datasets' data and checkpoints live in the swapped-in file.
            let [decimal_rows, logs_rows, checkpoint_rows] = counts_on_disk::<3>(
                &db_path,
                "SELECT (SELECT COUNT(1) FROM decimal)::BIGINT,
                        (SELECT COUNT(1) FROM logs)::BIGINT,
                        (SELECT COUNT(1) FROM spice_sys_dataset_checkpoint)::BIGINT",
            )
            .await?;

            if decimal_rows == 0 || logs_rows == 0 {
                return Err(anyhow!(
                    "expected both datasets to have rows after the swap (decimal={decimal_rows}, logs={logs_rows})"
                ));
            }
            if checkpoint_rows != 2 {
                return Err(anyhow!(
                    "expected both dataset checkpoints in the swapped-in file, found {checkpoint_rows}"
                ));
            }

            Ok(())
        })
        .await
}
