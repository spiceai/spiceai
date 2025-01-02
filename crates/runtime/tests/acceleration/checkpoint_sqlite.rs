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
use datafusion_table_providers::sql::db_connection_pool::sqlitepool::SqliteConnectionPool;
use datafusion_table_providers::sql::db_connection_pool::DbConnectionPool;
use datafusion_table_providers::sql::db_connection_pool::JoinPushDown;
use futures::TryStreamExt;
use runtime::{status, Runtime};
use spicepod::component::dataset::acceleration::Mode;
use spicepod::component::dataset::acceleration::{Acceleration, RefreshMode};
use spicepod::component::dataset::Dataset;
use std::sync::Arc;

use crate::acceleration::get_params;
use crate::utils::test_request_context;
use crate::{get_test_datafusion, init_tracing, utils::runtime_ready_check};

#[tokio::test]
async fn test_acceleration_sqlite_checkpoint() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    let _guard = super::ACCELERATION_MUTEX.lock().await;

    test_request_context()
        .scope(async {
            let status = status::RuntimeStatus::new();
            let df = get_test_datafusion(Arc::clone(&status));

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
