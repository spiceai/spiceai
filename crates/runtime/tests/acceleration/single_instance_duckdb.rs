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
use std::sync::Arc;

use crate::{
    configure_test_datafusion, init_tracing,
    utils::{runtime_ready_check, test_request_context},
};

use super::get_params;

fn get_dataset(from: &str, name: &str, path: &str) -> Dataset {
    let mut dataset = Dataset::new(from, name);
    dataset.acceleration = Some(Acceleration {
        params: get_params(&Mode::File, Some(path.to_string()), "duckdb"),
        enabled: true,
        engine: Some("duckdb".to_string()),
        mode: Mode::File,
        refresh_mode: Some(RefreshMode::Full),
        refresh_sql: None,
        ..Acceleration::default()
    });
    dataset
}

#[tokio::test]
#[expect(clippy::too_many_lines)]
async fn test_acceleration_duckdb_single_instance() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope_retry(3, || async {
            let expected_path = "./single_duckdb.db";
            let app = AppBuilder::new("test_acceleration_duckdb_single_instance")
                .with_dataset(get_dataset(
                    "https://public-data.spiceai.org/decimal.parquet",
                    "decimal",
                    expected_path,
                ))
                .with_dataset(get_dataset(
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
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
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

            let pool = DuckDbConnectionPool::new_file(expected_path, &AccessMode::ReadWrite)
                .expect("valid path");
            let conn_dyn = pool.connect().await.expect("valid connection");
            let conn = conn_dyn.as_sync().expect("sync connection");
            let result: Vec<RecordBatch> = conn
                .query_arrow(
                    "SELECT dataset_name FROM spice_sys_dataset_checkpoint ORDER BY dataset_name",
                    &[],
                    None,
                )
                .expect("query executes")
                .try_collect::<Vec<RecordBatch>>()
                .await
                .expect("collects results");

            let pretty = arrow::util::pretty::pretty_format_batches(&result)
                .map_err(|e| anyhow::Error::msg(e.to_string()))?;
            insta::assert_snapshot!(pretty);

            let persisted_records_decimal: Vec<RecordBatch> = conn
                .query_arrow("SELECT * FROM decimal ORDER BY id", &[], None)
                .expect("query executes")
                .try_collect::<Vec<RecordBatch>>()
                .await
                .expect("collects results");

            let persisted_records_decimal_pretty =
                arrow::util::pretty::pretty_format_batches(&persisted_records_decimal)
                    .map_err(|e| anyhow::Error::msg(e.to_string()))?;
            insta::assert_snapshot!(persisted_records_decimal_pretty);

            let persisted_records_logs: Vec<RecordBatch> = conn
                .query_arrow(
                    "SELECT * FROM logs ORDER BY transaction_hash, log_index LIMIT 10",
                    &[],
                    None,
                )
                .expect("query executes")
                .try_collect::<Vec<RecordBatch>>()
                .await
                .expect("collects results");

            let persisted_records_logs_pretty =
                arrow::util::pretty::pretty_format_batches(&persisted_records_logs)
                    .map_err(|e| anyhow::Error::msg(e.to_string()))?;
            insta::assert_snapshot!(persisted_records_logs_pretty);

            // Remove the file
            std::fs::remove_file(expected_path).expect("remove file");

            Ok(())
        })
        .await
}
