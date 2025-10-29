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
use anyhow::anyhow;
use app::AppBuilder;
use arrow::array::RecordBatch;
use datafusion_table_providers::sql::db_connection_pool::DbConnectionPool;
use datafusion_table_providers::sql::db_connection_pool::JoinPushDown;
use datafusion_table_providers::sql::db_connection_pool::sqlitepool::SqliteConnectionPool;
use futures::TryStreamExt;
use runtime::{Runtime, component::dataset::builder::DatasetBuilder};
use spicepod::acceleration::Mode;
use spicepod::acceleration::{Acceleration, RefreshMode};
use spicepod::component::dataset::Dataset;
use std::sync::Arc;

use crate::acceleration::get_params;
use crate::utils::test_request_context;
use crate::{configure_test_datafusion, init_tracing, utils::runtime_ready_check};

#[tokio::test]
async fn test_acceleration_sqlite_checkpoint() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let mut dataset =
                Dataset::new("https://public-data.spiceai.org/decimal.parquet", "decimal");
            dataset.acceleration = Some(Acceleration {
                params: get_params(
                    &Mode::File,
                    Some("./decimal_sqlite.db".to_string()),
                    "sqlite",
                ),
                enabled: true,
                engine: Some("sqlite".to_string()),
                mode: Mode::File,
                refresh_mode: Some(RefreshMode::Full),
                refresh_sql: Some("SELECT * FROM decimal".to_string()),
                ..Acceleration::default()
            });

            let app = AppBuilder::new("test_acceleration_sqlite_checkpoint")
                .with_dataset(dataset)
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

            let conn_pool = SqliteConnectionPool::new(
                "./decimal_sqlite.db",
                datafusion_table_providers::sql::db_connection_pool::Mode::File,
                JoinPushDown::Disallow,
                vec![],
                std::time::Duration::from_millis(5000),
            )
            .await
            .expect("connection pool");

            let results = query(
                &conn_pool,
                "SELECT dataset_name FROM spice_sys_dataset_checkpoint",
            )
            .await;

            let pretty = arrow::util::pretty::pretty_format_batches(&results).expect("pretty");
            insta::assert_snapshot!(pretty);

            let persisted_records: Vec<RecordBatch> =
                query(&conn_pool, "SELECT * FROM decimal ORDER BY id").await;

            let pretty_decimal = arrow::util::pretty::pretty_format_batches(&persisted_records)
                .expect("pretty print");
            insta::assert_snapshot!(pretty_decimal);

            // Remove the file
            std::fs::remove_file("./decimal_sqlite.db").expect("remove file");

            Ok(())
        })
        .await
}

#[expect(clippy::expect_used)]
async fn query(conn_pool: &SqliteConnectionPool, query: &str) -> Vec<RecordBatch> {
    conn_pool
        .connect()
        .await
        .expect("connection")
        .as_async()
        .expect("async connection")
        .query_arrow(query, &[], None)
        .await
        .expect("query")
        .try_collect::<Vec<RecordBatch>>()
        .await
        .expect("valid results")
}
