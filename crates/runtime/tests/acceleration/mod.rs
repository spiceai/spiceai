/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use runtime::{component::dataset::Dataset, dataaccelerator::spice_sys::dataset_checkpointer};
use runtime_acceleration::sidecar::OpenOption;
use runtime_acceleration::snapshot::SnapshotBehavior;
use spicepod::{acceleration::Mode, param::Params};
use std::sync::Arc;

mod caching_mode;
#[cfg(feature = "duckdb")]
mod caching_mode_per_principal;
#[cfg(feature = "duckdb")]
mod caching_mode_post_filter;
#[cfg(not(target_os = "windows"))]
mod cayenne_append_overlap;
#[cfg(not(target_os = "windows"))]
mod cayenne_append_timestamptz;
#[cfg(not(target_os = "windows"))]
mod cayenne_maintained_aggregates;
#[cfg(not(target_os = "windows"))]
mod cayenne_memory;
#[cfg(feature = "duckdb")]
mod checkpoint_duckdb;
#[cfg(feature = "postgres-accel")]
mod checkpoint_postgres;
#[cfg(feature = "sqlite")]
mod checkpoint_sqlite;
#[cfg(feature = "turso")]
mod checkpoint_turso;
#[cfg(feature = "duckdb")]
mod cron;
#[cfg(feature = "duckdb")]
mod file_create_duckdb;
#[cfg(feature = "duckdb")]
mod file_swap_duckdb;
#[cfg(feature = "sqlite")]
mod file_watcher;
mod hash_index;
mod localpod_sync;
#[cfg(all(feature = "postgres-accel", feature = "duckdb", feature = "sqlite"))]
mod on_conflict;

#[cfg(not(target_os = "windows"))]
mod on_conflict_cayenne;
#[cfg(feature = "duckdb")]
mod on_conflict_options;
mod partition_by_arrow;
#[cfg(not(target_os = "windows"))]
mod partition_by_cayenne;
#[cfg(feature = "postgres-accel")]
mod query_push_down;
mod refresh;
#[cfg(any(feature = "duckdb", feature = "sqlite", feature = "turso"))]
mod reload_file_accelerated;
mod retention_arrow;
#[cfg(not(target_os = "windows"))]
mod retention_cayenne;
#[cfg(feature = "duckdb")]
mod single_instance_duckdb;
#[cfg(feature = "snapshots")]
mod snapshot_lock_contention;
#[cfg(feature = "snapshots")]
mod snapshot_mutex;

/// Queue a refresh of `table`. Callers poll for the result rather than waiting on the
/// returned notifier: completion signals with `notify_waiters`, which stores no permit,
/// so a refresh that finishes first would leave a later waiter hanging.
pub(crate) async fn trigger_refresh(
    rt: &Arc<runtime::Runtime>,
    table: &str,
) -> Result<(), anyhow::Error> {
    rt.datafusion()
        .refresh_table(&datafusion::sql::TableReference::from(table), None)
        .await
        .map_err(|e| anyhow::anyhow!("refresh_table failed for {table}: {e}"))?
        .ok_or_else(|| anyhow::anyhow!("no refresh notifier for {table}"))?;
    Ok(())
}

/// Run a `COUNT(*)` query and read back its single value.
pub(crate) async fn count(rt: &Arc<runtime::Runtime>, sql: &str) -> Result<i64, anyhow::Error> {
    let batches = crate::utils::run_query(rt, sql).await?;
    let batch = batches
        .iter()
        .find(|batch| batch.num_rows() > 0)
        .ok_or_else(|| anyhow::anyhow!("count query returned no rows"))?;
    Ok(batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .ok_or_else(|| anyhow::anyhow!("count column is not Int64"))?
        .value(0))
}

/// Rows currently in `table`.
pub(crate) async fn row_count(
    rt: &Arc<runtime::Runtime>,
    table: &str,
) -> Result<i64, anyhow::Error> {
    count(rt, &format!("SELECT COUNT(*) AS cnt FROM {table}")).await
}

/// Whether any `.vortex` file exists under `dir` — the gate a Cayenne test uses to
/// prove its rows reached a file rather than staying in the metastore's inline tier.
pub(crate) fn has_vortex_file(dir: &std::path::Path) -> bool {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return false;
    };
    entries.flatten().any(|entry| {
        let path = entry.path();
        if path.is_dir() {
            has_vortex_file(&path)
        } else {
            path.extension().is_some_and(|ext| ext == "vortex")
        }
    })
}

pub(crate) fn get_params(mode: &Mode, file: Option<String>, engine: &str) -> Option<Params> {
    let param_name = format!("{engine}_file");
    if mode == &Mode::File {
        return Some(Params::from_string_map(
            vec![(param_name, file.unwrap_or_default())]
                .into_iter()
                .collect(),
        ));
    }
    None
}

/// Materializes the runtime's configured datasets — the [`Dataset`] values that
/// [`wait_for_checkpoints`] needs, which the runtime does not expose directly —
/// then loads the runtime's components under `load_timeout` and waits for it to
/// report ready.
pub(crate) async fn load_runtime_datasets(
    rt: &Arc<runtime::Runtime>,
    load_timeout: std::time::Duration,
) -> Result<Vec<Dataset>, anyhow::Error> {
    let datasets = {
        let app_ref = rt.app();
        let app_lock = app_ref.read().await;
        let Some(app) = app_lock.as_ref() else {
            return Err(anyhow::anyhow!("Failed to obtain app from runtime"));
        };

        app.datasets
            .clone()
            .into_iter()
            .map(runtime::component::dataset::builder::DatasetBuilder::try_from)
            .map(|ds_builder| {
                ds_builder
                    .map_err(|e| anyhow::anyhow!("Failed to create dataset builder: {e}"))
                    .and_then(|ds_builder| {
                        ds_builder
                            .with_app(Arc::clone(app))
                            .with_runtime(Arc::clone(rt))
                            .build()
                            .map_err(|e| anyhow::anyhow!("Failed to build dataset: {e}"))
                    })
            })
            .collect::<Result<Vec<_>, _>>()?
    };

    tokio::select! {
        () = tokio::time::sleep(load_timeout) => {
            return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
        }
        () = Arc::clone(rt).load_components() => {}
    }

    crate::utils::runtime_ready_check(rt).await;

    Ok(datasets)
}

async fn wait_for_checkpoints(
    datasets: Vec<Dataset>,
    timeout_secs: u64,
) -> Result<(), anyhow::Error> {
    let mut checkpoint_futures = Vec::new();

    for dataset in datasets {
        let registry = dataset.runtime.accelerator_engine_registry();
        let check_future = async move {
            match dataset_checkpointer(
                &dataset,
                registry,
                OpenOption::OpenExisting,
                SnapshotBehavior::Disabled,
            )
            .await
            {
                Ok(checkpoint) => {
                    while !checkpoint.exists().await {
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    }
                    Ok(())
                }
                Err(e) => Err(anyhow::anyhow!("Failed to verify checkpoint: {e}")),
            }
        };
        checkpoint_futures.push(check_future);
    }

    tokio::select! {
        () = tokio::time::sleep(std::time::Duration::from_secs(timeout_secs)) => {
            Err(anyhow::anyhow!("Timed out waiting for dataset checkpoints"))
        },
        result = futures::future::try_join_all(checkpoint_futures) => {
            result.map(|_| ())
        }
    }
}
