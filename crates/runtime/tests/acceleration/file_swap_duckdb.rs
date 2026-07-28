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

use crate::acceleration::wait_for_checkpoints;
use app::AppBuilder;
use arrow::array::RecordBatch;
use datafusion_table_providers::sql::db_connection_pool::DbConnectionPool;
use datafusion_table_providers::sql::db_connection_pool::duckdbpool::DuckDbConnectionPool;
use duckdb::AccessMode;
use futures::TryStreamExt;

use anyhow::anyhow;
use runtime::{Runtime, component::dataset::builder::DatasetBuilder};
use spicepod::acceleration::{Acceleration, Mode, RefreshMode};
use spicepod::component::dataset::Dataset;
use spicepod::param::Params;
use std::sync::Arc;

use crate::{
    configure_test_datafusion, init_tracing,
    utils::{register_test_connectors, runtime_ready_check, test_request_context},
};

fn get_swap_dataset(from: &str, name: &str, path: &str) -> Dataset {
    let mut dataset = Dataset::new(from, name);
    dataset.acceleration = Some(Acceleration {
        params: Some(Params::from_string_map(
            vec![
                ("duckdb_file".to_string(), path.to_string()),
                ("on_full_refresh".to_string(), "swap_file".to_string()),
            ]
            .into_iter()
            .collect(),
        )),
        enabled: true,
        engine: Some("duckdb".to_string()),
        mode: Mode::File,
        refresh_mode: Some(RefreshMode::Full),
        refresh_sql: None,
        ..Acceleration::default()
    });
    dataset
}

/// Two datasets share one DuckDB file with `on_full_refresh: swap_file`; both
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
            let expected_path = "./file_swap_duckdb.db";
            // Pre-create the database file so the refresh provably replaces it
            // (the file swap produces a new inode at the configured path).
            let _ = std::fs::remove_file(expected_path);
            {
                let pool = DuckDbConnectionPool::new_file(expected_path, &AccessMode::ReadWrite)
                    .expect("valid path");
                drop(pool);
            }
            #[cfg(unix)]
            let initial_inode = {
                use std::os::unix::fs::MetadataExt;
                std::fs::metadata(expected_path)?.ino()
            };

            let app = AppBuilder::new("test_acceleration_duckdb_full_refresh_file_swap")
                .with_dataset(get_swap_dataset(
                    "https://public-data.spiceai.org/decimal.parquet",
                    "decimal",
                    expected_path,
                ))
                .with_dataset(get_swap_dataset(
                    "https://public-data.spiceai.org/eth.recent_logs.parquet",
                    "logs",
                    expected_path,
                ))
                .build();

            configure_test_datafusion();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);

            let app_ref = rt.app();
            let app_lock = app_ref.read().await;
            let Some(app) = app_lock.as_ref() else {
                return Err(anyhow!("Failed to obtain app from runtime"));
            };

            let cloned_rt = Arc::clone(&rt);
            let runtime_datasets = app
                .datasets
                .clone()
                .into_iter()
                .map(DatasetBuilder::try_from)
                .map(move |ds_builder| {
                    ds_builder
                        .map_err(|e| anyhow!("Failed to create dataset builder: {e}"))
                        .and_then(|ds_builder| {
                            ds_builder
                                .with_app(Arc::clone(app))
                                .with_runtime(Arc::clone(&cloned_rt))
                                .build()
                                .map_err(|e| anyhow!("Failed to build dataset: {e}"))
                        })
                })
                .collect::<Result<Vec<_>, _>>()?;

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
                    return Err(anyhow::Error::msg("Timed out waiting for datasets to load"));
                }
                () = Arc::clone(&rt).load_components() => {}
            }

            runtime_ready_check(&rt).await;

            // Verify checkpoints are created before shutting down runtime
            wait_for_checkpoints(runtime_datasets, 120).await?;

            rt.shutdown().await;
            drop(rt);
            tokio::time::sleep(std::time::Duration::from_secs(15)).await;

            // The swap replaced the file at the configured path with a fresh
            // generation.
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt;
                let swapped_inode = std::fs::metadata(expected_path)?.ino();
                if swapped_inode == initial_inode {
                    return Err(anyhow!(
                        "expected the full refresh to swap in a new database file, but the inode is unchanged"
                    ));
                }
            }

            // No staging/generation/WAL debris may remain next to the file.
            let debris: Vec<String> = std::fs::read_dir(".")?
                .filter_map(Result::ok)
                .map(|e| e.file_name().to_string_lossy().to_string())
                .filter(|name| {
                    name.starts_with("file_swap_duckdb.db.refresh.")
                        || name == "file_swap_duckdb.db.wal"
                })
                .collect();
            if !debris.is_empty() {
                return Err(anyhow!("file swap left debris behind: {debris:?}"));
            }

            // Both datasets' data and checkpoints live in the swapped-in file.
            let pool = DuckDbConnectionPool::new_file(expected_path, &AccessMode::ReadWrite)
                .expect("valid path");
            let conn_dyn = pool.connect().await.expect("valid connection");
            let conn = conn_dyn.as_sync().expect("sync connection");

            let mut counts = Vec::new();
            for sql in [
                "SELECT COUNT(1)::BIGINT FROM decimal",
                "SELECT COUNT(1)::BIGINT FROM logs",
                "SELECT COUNT(1)::BIGINT FROM spice_sys_dataset_checkpoint",
            ] {
                let batches: Vec<RecordBatch> = conn
                    .query_arrow(sql, &[], None)
                    .expect("query executes")
                    .try_collect::<Vec<RecordBatch>>()
                    .await
                    .expect("collects results");
                let value = batches
                    .first()
                    .and_then(|b| {
                        b.column(0)
                            .as_any()
                            .downcast_ref::<arrow::array::Int64Array>()
                    })
                    .ok_or_else(|| anyhow!("count query '{sql}' returned no rows"))?
                    .value(0);
                counts.push(value);
            }
            let (decimal_rows, logs_rows, checkpoint_rows) = (counts[0], counts[1], counts[2]);

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

            // Remove the file
            std::fs::remove_file(expected_path).expect("remove file");

            Ok(())
        })
        .await
}
