/*
Copyright 2024 The Spice.ai OSS Authors

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
use spicepod::component::dataset::acceleration::{Acceleration, Mode, RefreshMode};
use std::sync::Arc;

use crate::{
    acceleration::get_params, get_test_datafusion, init_tracing, runtime_ready_check,
    s3::get_s3_dataset,
};

// Disabled until https://github.com/spiceai/spiceai/pull/2669 is merged
#[tokio::test]
#[ignore]
async fn test_acceleration_duckdb_checkpoint() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    let status = status::RuntimeStatus::new();
    let df = get_test_datafusion(Arc::clone(&status));

    let mut dataset = get_s3_dataset();
    dataset.acceleration = Some(Acceleration {
        params: get_params(&Mode::File, Some("./taxi_trips.db".to_string()), "duckdb"),
        enabled: true,
        engine: Some("duckdb".to_string()),
        mode: Mode::File,
        refresh_mode: Some(RefreshMode::Full),
        refresh_sql: Some("SELECT * FROM taxi_trips LIMIT 10".to_string()),
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

    drop(rt);
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let conn = DuckDbConnectionPool::new_file("./taxi_trips.db", &AccessMode::ReadWrite)
        .expect("valid path");
    let result: Vec<RecordBatch> = conn
        .connect()
        .await
        .expect("valid connection")
        .as_sync()
        .expect("sync connection")
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

    // Remove the file
    std::fs::remove_file("./taxi_trips.db").expect("remove file");

    Ok(())
}
