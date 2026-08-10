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

use runtime::{
    component::dataset::Dataset,
    dataaccelerator::spice_sys::{OpenOption, dataset_checkpoint::DatasetCheckpoint},
};
use spicepod::{acceleration::Mode, param::Params};
use std::sync::Arc;

mod caching_mode;
#[cfg(feature = "duckdb")]
mod caching_mode_per_principal;
#[cfg(feature = "duckdb")]
mod caching_mode_post_filter;
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
#[cfg(feature = "duckdb")]
mod single_instance_duckdb;
#[cfg(feature = "snapshots")]
mod snapshot_lock_contention;
#[cfg(feature = "snapshots")]
mod snapshot_mutex;

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
            match DatasetCheckpoint::try_new(&dataset, registry, OpenOption::OpenExisting).await {
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
