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
use std::{collections::HashMap, path::Path, sync::Arc, time::Duration};

use app::AppBuilder;

use runtime::Runtime;
use spicepod::{acceleration::Acceleration, component::dataset::Dataset, param::Params};
use tempfile::TempDir;
use tokio::fs;

use crate::{
    configure_test_datafusion, init_tracing,
    utils::{runtime_ready_check, test_request_context},
};

fn make_spiceai_dataset(path: &str, name: &str, engine: &str, retention_sql: &str) -> Dataset {
    let mut ds = Dataset::new(format!("spice.ai/{path}"), name.to_string());
    ds.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some(engine.to_string()),
        retention_sql: Some(retention_sql.to_string()),
        retention_check_enabled: true,
        retention_check_interval: Some("200ms".to_string()),
        ..Default::default()
    });
    ds
}

fn make_s3_dataset(
    path: &str,
    name: &str,
    engine: &str,
    retention_sql: &str,
    time_column: Option<&str>,
    retention_period: Option<&str>,
) -> Dataset {
    let mut ds = Dataset::new(format!("s3://{path}"), name.to_string());
    ds.time_column = time_column.map(ToString::to_string);
    ds.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some(engine.to_string()),
        retention_sql: Some(retention_sql.to_string()),
        retention_check_enabled: true,
        retention_check_interval: Some("200ms".to_string()),
        retention_period: retention_period.map(ToString::to_string),
        ..Default::default()
    });
    ds
}

fn make_local_csv_dataset(path: &Path, name: &str, retention_sql: Option<&str>) -> Dataset {
    let mut dataset = Dataset::new(format!("file://{}", path.display()), name.to_string());

    let mut params = HashMap::new();
    params.insert("file_format".to_string(), "csv".to_string());
    params.insert("csv_has_header".to_string(), "true".to_string());
    dataset.params = Some(Params::from_string_map(params));

    let mut acceleration = Acceleration {
        engine: Some("arrow".to_string()),
        ..Default::default()
    };
    acceleration.retention_sql = retention_sql.map(std::string::ToString::to_string);
    dataset.acceleration = Some(acceleration);

    dataset
}

#[tokio::test]
async fn test_retention_sql() -> Result<(), anyhow::Error> {
    let _ = rustls::crypto::CryptoProvider::install_default(
        rustls::crypto::aws_lc_rs::default_provider(),
    );
    let _tracing = init_tracing(None);

    test_request_context()
        .scope(async {
            let app = AppBuilder::new("retention_sql")
                .with_dataset(make_spiceai_dataset(
                    "spiceai/tpch/datasets/tpch.nation",
                    "nation",
                    "arrow",
                    // keep only ALGERIA, ARGENTINA and CANADA
                    "DELETE FROM nation WHERE n_nationkey >= 5 OR n_name NOT LIKE '%A'",
                ))
                .with_dataset(make_s3_dataset(
                    "spiceai-public-datasets/taxi_small_samples/taxi_sample.parquet",
                    "taxi_trips",
                    "duckdb",
                    "DELETE FROM taxi_trips WHERE VendorID != 2 OR Airport_fee != 1.75",
                    Some("tpep_pickup_datetime"),
                    Some("1000000000w"), // Some large retention period to ensure data is not fitlered out by time
                ))
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder()
                .with_app(app)
                .build()
                .await;

            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(120)) => {
                    panic!("Timeout waiting for components to load");
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            tokio::time::sleep(Duration::from_secs(1)).await; // Allow retention to complete

            for (sql, snapshot_name) in [
                (
                    "SELECT n_nationkey, n_name, n_regionkey FROM nation",
                    "retention_sql",
                ),
                ("SELECT VendorID, Airport_fee, tpep_pickup_datetime, passenger_count, trip_distance FROM taxi_trips", "retention_sql_and_time_column"),
            ] {
                let query = rt.datafusion().query_builder(sql).build().run().await?;

                let results: Vec<RecordBatch> =
                    query.data.try_collect::<Vec<RecordBatch>>().await?;

                let results_str =
                    arrow::util::pretty::pretty_format_batches(&results).expect("pretty batches");
                insta::assert_snapshot!(snapshot_name, results_str);
            }

            Ok(())
        })
        .await
}

#[tokio::test]
async fn test_retention_sql_initial_refresh_filters_data() -> Result<(), anyhow::Error> {
    let _ = rustls::crypto::CryptoProvider::install_default(
        rustls::crypto::aws_lc_rs::default_provider(),
    );
    let _tracing = init_tracing(None);

    test_request_context()
        .scope(async {
            let temp_dir = TempDir::new()?;
            let csv_path = temp_dir.path().join("retention_sample.csv");
            fs::write(
                &csv_path,
                "id,status\n1,active\n2,expired\n3,active\n4,expired\n",
            )
            .await?;

            let retained_dataset_name = "retained_records";
            let retention_sql =
                format!("DELETE FROM {retained_dataset_name} WHERE status = 'expired'");

            let retained_dataset =
                make_local_csv_dataset(&csv_path, retained_dataset_name, Some(&retention_sql));
            let full_dataset = make_local_csv_dataset(&csv_path, "all_records", None);

            let app = AppBuilder::new("retention_sql_initial_refresh")
                .with_dataset(retained_dataset)
                .with_dataset(full_dataset)
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;

            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(120)) => {
                    panic!("Timeout waiting for components to load");
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            let query = rt
                .datafusion()
                .query_builder("SELECT id, status FROM retained_records ORDER BY id")
                .build()
                .run()
                .await?;

            let results: Vec<RecordBatch> = query.data.try_collect::<Vec<RecordBatch>>().await?;
            let results_str =
                arrow::util::pretty::pretty_format_batches(&results).expect("pretty batches");
            insta::assert_snapshot!("retention_sql_initial_refresh_filtered", results_str);

            let all_query = rt
                .datafusion()
                .query_builder("SELECT id, status FROM all_records ORDER BY id")
                .build()
                .run()
                .await?;

            let all_results: Vec<RecordBatch> =
                all_query.data.try_collect::<Vec<RecordBatch>>().await?;
            let all_results_str =
                arrow::util::pretty::pretty_format_batches(&all_results).expect("pretty batches");
            insta::assert_snapshot!("retention_sql_initial_refresh_all_rows", all_results_str);

            Ok(())
        })
        .await
}
