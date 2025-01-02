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

use app::AppBuilder;
use arrow::array::RecordBatch;
use datafusion_table_providers::sql::db_connection_pool::duckdbpool::DuckDbConnectionPool;
use datafusion_table_providers::sql::db_connection_pool::DbConnectionPool;
use duckdb::AccessMode;
use futures::TryStreamExt;
use runtime::{status, Runtime};
use spicepod::component::dataset::{
    acceleration::{Acceleration, Mode, RefreshMode},
    Dataset,
};
use std::sync::Arc;

use crate::{
    acceleration::get_params,
    get_test_datafusion, init_tracing,
    utils::{runtime_ready_check, test_request_context},
};

fn get_dataset() -> Dataset {
    Dataset::new("https://public-data.spiceai.org/decimal.parquet", "decimal")
}

#[tokio::test]
async fn test_acceleration_duckdb_checkpoint() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    let _guard = super::ACCELERATION_MUTEX.lock().await;

    test_request_context()
        .scope(async {
            let status = status::RuntimeStatus::new();
            let df = get_test_datafusion(Arc::clone(&status));

            let mut dataset = get_dataset();
            dataset.acceleration = Some(Acceleration {
                params: get_params(&Mode::File, Some("./decimal.db".to_string()), "duckdb"),
                enabled: true,
                engine: Some("duckdb".to_string()),
                mode: Mode::File,
                refresh_mode: Some(RefreshMode::Full),
                refresh_sql: None,
                ..Acceleration::default()
            });

            let app = AppBuilder::new("test_acceleration_duckdb_metadata")
                .with_dataset(dataset)
                .build();

            let rt = Arc::new(
                Runtime::builder()
                    .with_app(app)
                    .with_datafusion(df)
                    .with_runtime_status(status)
                    .build()
                    .await,
            );

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                    return Err(anyhow::Error::msg("Timed out waiting for datasets to load"));
                }
                () = rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            // Wait for the checkpoint to be created
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            drop(rt);
            runtime::dataaccelerator::clear_registry().await;
            runtime::dataaccelerator::register_all().await;
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;

            let pool = DuckDbConnectionPool::new_file("./decimal.db", &AccessMode::ReadWrite)
                .expect("valid path");
            let conn_dyn = pool.connect().await.expect("valid connection");
            let conn = conn_dyn.as_sync().expect("sync connection");
            let result: Vec<RecordBatch> = conn
                .query_arrow(
                    "SELECT dataset_name FROM spice_sys_dataset_checkpoint",
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

            let persisted_records: Vec<RecordBatch> = conn
                .query_arrow("SELECT * FROM decimal ORDER BY id", &[], None)
                .expect("query executes")
                .try_collect::<Vec<RecordBatch>>()
                .await
                .expect("collects results");

            let persisted_records_pretty =
                arrow::util::pretty::pretty_format_batches(&persisted_records)
                    .map_err(|e| anyhow::Error::msg(e.to_string()))?;
            insta::assert_snapshot!(persisted_records_pretty);

            // Remove the file
            std::fs::remove_file("./decimal.db").expect("remove file");

            Ok(())
        })
        .await
}
