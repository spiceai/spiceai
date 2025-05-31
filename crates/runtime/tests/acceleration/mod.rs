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
    component::dataset::Dataset, dataaccelerator::spice_sys::dataset_checkpoint::DatasetCheckpoint,
};
use spicepod::{acceleration::Mode, param::Params};

#[cfg(feature = "duckdb")]
mod checkpoint_duckdb;
#[cfg(feature = "postgres")]
mod checkpoint_postgres;
#[cfg(feature = "sqlite")]
mod checkpoint_sqlite;
#[cfg(feature = "duckdb")]
mod cron;
#[cfg(feature = "sqlite")]
mod file_watcher;
#[cfg(all(feature = "postgres", feature = "duckdb", feature = "sqlite"))]
mod on_conflict;
mod query_push_down;
#[cfg(feature = "duckdb")]
mod single_instance_duckdb;

pub(crate) fn get_params(mode: &Mode, file: Option<String>, engine: &str) -> Option<Params> {
    let param_name = format!("{engine}_file",);
    if mode == &Mode::File {
        return Some(Params::from_string_map(
            vec![(param_name, file.unwrap_or_default())]
                .into_iter()
                .collect(),
        ));
    }
    None
}

async fn wait_for_checkpoints(
    datasets: Vec<Dataset>,
    timeout_secs: u64,
) -> Result<(), anyhow::Error> {
    let mut checkpoint_futures = Vec::new();

    for dataset in datasets {
        let check_future = async move {
            match DatasetCheckpoint::try_new(&dataset).await {
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
