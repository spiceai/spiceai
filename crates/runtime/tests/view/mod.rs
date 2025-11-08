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

use arrow::array::RecordBatch;
use futures::TryStreamExt;
use runtime::Runtime;
use runtime::{
    component::view::ViewBuilder,
    dataaccelerator::spice_sys::{OpenOption, dataset_checkpoint::DatasetCheckpoint},
};
use spicepod::acceleration::{Acceleration, Mode, RefreshMode};
use spicepod::component::{dataset::Dataset, view::View};
use std::sync::Arc;

use crate::acceleration::get_params;
use crate::{
    configure_test_datafusion, init_tracing,
    utils::{runtime_ready_check, test_request_context},
};

#[cfg(feature = "duckdb")]
#[allow(clippy::too_many_lines)]
#[tokio::test]
async fn accelerated_view_duckdb() -> Result<(), anyhow::Error> {
    use datafusion_table_providers::sql::db_connection_pool::{
        DbConnectionPool, duckdbpool::DuckDbConnectionPool,
    };
    use duckdb::AccessMode;

    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            // Clean up any existing acceleration file
            if std::path::Path::new("./taxi_trips_vw.db").exists() {
                std::fs::remove_file("./taxi_trips_vw.db").expect("to remove file");
            }

            let dataset = Dataset::new("s3://spiceai-public-datasets/taxi_small_samples/taxi_sample.parquet", "taxi_trips");
            let mut view = View::new("taxi_trips_vw".to_string());
            view.sql = Some("SELECT VendorID, AVG(trip_distance) AS avg_trip_distance, AVG(fare_amount) AS avg_fare_amount FROM taxi_trips GROUP BY VendorID".to_string());
            view.acceleration = Some(Acceleration {
                params: get_params(&Mode::File, Some("./taxi_trips_vw.db".to_string()), "duckdb"),
                enabled: true,
                engine: Some("duckdb".to_string()),
                mode: Mode::File,
                refresh_mode: Some(RefreshMode::Full),
                ..Acceleration::default()
            });

            let view_copy = view.clone();

            let app = app::AppBuilder::new("test_view_acceleration_duckdb")
                .with_dataset(dataset)
                .with_view(view)
                .build();

            let app_copy = app.clone();

            configure_test_datafusion();
            let rt = Arc::new(
                Runtime::builder()
                    .with_app(app)
                    .build()
                    .await,
            );

            let cloned_rt = Arc::clone(&rt);

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
                }
                () = cloned_rt.load_components() => {}
            }
            runtime_ready_check(&rt).await;

            let view = ViewBuilder::try_from(view_copy).expect("to parse view")
                .build_with(Arc::clone(&rt), Arc::new(app_copy));

            // Ensure Checkpoint is created after initial view load
            let checkpoint = DatasetCheckpoint::try_new(&view, OpenOption::OpenExisting).await.expect("Failed to create view checkpoint");
            assert!(checkpoint.exists().await, "Checkpoint does not exist");
            let last_checkpoint_time = checkpoint
                .last_checkpoint_time()
                .await
                .expect("Failed to get last checkpoint time");
            assert!(last_checkpoint_time.is_some(), "Last checkpoint time is not set");

            // Test explain to ensure duckdb is used
            let query_result = rt
                .datafusion()
                .query_builder("EXPLAIN SELECT * FROM taxi_trips_vw ORDER BY avg_trip_distance;")
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!(e))?
                .data
                .try_collect::<Vec<RecordBatch>>()
                .await
                .expect("collects results");

            let pretty = arrow::util::pretty::pretty_format_batches(&query_result)
                .map_err(|e| anyhow::Error::msg(e.to_string()))?;
            insta::assert_snapshot!("duckdb_query_explain", pretty);

            // Test query output
            let query_result = rt
                .datafusion()
                .query_builder("SELECT * FROM taxi_trips_vw ORDER BY avg_trip_distance")
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!(e))?
                .data
                .try_collect::<Vec<RecordBatch>>()
                .await
                .expect("collects results");

            let pretty = arrow::util::pretty::pretty_format_batches(&query_result)
                .map_err(|e| anyhow::Error::msg(e.to_string()))?;
            insta::assert_snapshot!("duckdb_query_result", pretty);

            rt.shutdown().await;
            drop(rt);

            tokio::time::sleep(std::time::Duration::from_secs(1)).await;

            let pool = DuckDbConnectionPool::new_file("./taxi_trips_vw.db", &AccessMode::ReadWrite)
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
            insta::assert_snapshot!("duckdb_spice_sys_dataset_checkpoint", pretty);

            let persisted_records: Vec<RecordBatch> = conn
                .query_arrow("SELECT * FROM taxi_trips_vw ORDER BY avg_trip_distance", &[], None)
                .expect("query executes")
                .try_collect::<Vec<RecordBatch>>()
                .await
                .expect("collects results");

            let persisted_records_pretty =
                arrow::util::pretty::pretty_format_batches(&persisted_records)
                    .map_err(|e| anyhow::Error::msg(e.to_string()))?;
            insta::assert_snapshot!("duckdb_persisted_records", persisted_records_pretty);

            // Remove the file
            std::fs::remove_file("./taxi_trips_vw.db").expect("to remove file");

            Ok(())
        })
        .await
}
