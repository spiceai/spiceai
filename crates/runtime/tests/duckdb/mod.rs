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

use std::sync::Arc;

use crate::configure_test_datafusion;
use crate::{
    RecordBatch, init_tracing,
    utils::{runtime_ready_check, test_request_context},
};
use app::AppBuilder;
use datafusion::assert_batches_eq;
use futures::TryStreamExt;
use runtime::Runtime;
use scopeguard::defer;
use spicepod::acceleration::{Acceleration, Mode, RefreshMode};
use spicepod::component::dataset::Dataset;
use tempfile::NamedTempFile;

fn make_duckdb_dataset(ds_name: &str, fn_name: &str, path_str: &str) -> Dataset {
    let mut dataset = Dataset::new(
        format!("duckdb:read_{fn_name}({path_str})"),
        fn_name.to_string(),
    );
    dataset.name = ds_name.to_string();
    dataset
}

fn make_duckdb_acceleration_dataset(ds_name: &str, fn_name: &str, path_str: &str) -> Dataset {
    let mut dataset = Dataset::new(
        format!("duckdb:read_{fn_name}({path_str})"),
        fn_name.to_string(),
    );
    dataset.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some("duckdb".to_string()),
        mode: Mode::Memory,
        refresh_mode: Some(RefreshMode::Full),
        refresh_sql: None,
        ..Acceleration::default()
    });
    dataset.name = ds_name.to_string();
    dataset
}

fn make_test_query(table_name: &str) -> String {
    format!("SELECT DISTINCT(\"VendorID\") FROM {table_name} ORDER BY \"VendorID\" DESC")
}

#[tokio::test]
async fn duckdb_from_functions() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let sample_csv_contents = include_str!("../test_data/taxi_sample.csv");
            let sample_json_contents = include_str!("../test_data/taxi_sample.json");
            // Write the sample file to a temporary directory
            let temp_dir = std::env::temp_dir().join("spiced_test_data");
            std::fs::create_dir_all(&temp_dir).expect("failed to create temp dir");
            let sample_csv_path = temp_dir.join("taxi_sample.csv");
            std::fs::write(&sample_csv_path, sample_csv_contents)
                .expect("failed to write sample file");
            let sample_json_path = temp_dir.join("taxi_sample.json");
            std::fs::write(&sample_json_path, sample_json_contents)
                .expect("failed to write sample file");
            defer! {
                std::fs::remove_dir_all(&temp_dir).expect("failed to remove temp dir");
            }
            let app = AppBuilder::new("duckdb_function_test")
        .with_dataset(make_duckdb_dataset(
            "csv_remote",
            "csv",
            "'s3://spiceai-public-datasets/taxi_small_samples/taxi_sample.csv', HEADER=true",
        ))
        .with_dataset(make_duckdb_dataset(
            "csv_local",
            "csv",
            &format!("'{}'", sample_csv_path.display()),
        ))
        .with_dataset(make_duckdb_dataset(
            "parquet_remote",
            "parquet",
            "'s3://spiceai-public-datasets/taxi_small_samples/taxi_sample.parquet'",
        ))
        .with_dataset(make_duckdb_dataset(
            "json_remote",
            "json",
            "'s3://spiceai-public-datasets/taxi_small_samples/taxi_sample.json'",
        ))
        .with_dataset(make_duckdb_dataset(
            "json_local",
            "json",
            &format!("'{}'", sample_json_path.display()),
        ))
        .build();

            let rt = Runtime::builder()
                .with_app(app)
                .with_datafusion_configuration_fn(configure_test_datafusion)
                .build()
                .await;
            let cloned_rt = Arc::new(rt.clone());

            // Set a timeout for the test
            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err("Timed out waiting for datasets to load".to_string());
                }
                () = cloned_rt.load_components() => {}
            }

            let queries = vec![
                ("csv_remote", make_test_query("csv_remote")),
                ("csv_local", make_test_query("csv_local")),
                ("parquet_remote", make_test_query("parquet_remote")),
                //("parquet_local", make_test_query("parquet_local")),
                ("json_remote", make_test_query("json_remote")),
                ("json_local", make_test_query("json_local")),
            ];

            let expected_results = [
                "+----------+",
                "| VendorID |",
                "+----------+",
                "| 2        |",
                "| 1        |",
                "+----------+",
            ];

            for (ds_name, query) in queries {
                let query_result = rt
                    .datafusion()
                    .query_builder(&query)
                    .build()
                    .run()
                    .await
                    .map_err(|e| format!("query `{query}` to plan: {e}"))?;
                let data = query_result
                    .data
                    .try_collect::<Vec<RecordBatch>>()
                    .await
                    .map_err(|e| format!("{ds_name}: query `{query}` to results: {e}"))?;

                assert_batches_eq!(expected_results, &data);
            }

            Ok(())
        })
        .await
}

#[tokio::test]
async fn duckdb_order_by_special_cases() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let sample_csv_contents = include_str!("../test_data/taxi_sample.csv");
            // Write the sample file to a temporary directory
            let temp_dir = std::env::temp_dir().join("spiced_test_data_order_by");
            std::fs::create_dir_all(&temp_dir).expect("failed to create temp dir");
            let sample_csv_path = temp_dir.join("taxi_sample.csv");
            std::fs::write(&sample_csv_path, sample_csv_contents)
                .expect("failed to write sample file");
            defer! {
                std::fs::remove_dir_all(&temp_dir).expect("failed to remove temp dir");
            }

            let app = AppBuilder::new("duckdb_order_by_test")
                .with_dataset(make_duckdb_acceleration_dataset(
                    "csv_test",
                    "csv",
                    &format!("'{}'", sample_csv_path.display()),
                ))
                .build();

            let rt = Runtime::builder()
                .with_app(app)
                .with_datafusion_configuration_fn(configure_test_datafusion)
                .build()
                .await;
            let cloned_rt = Arc::new(rt.clone());

            // Set a timeout for the test
            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err("Timed out waiting for datasets to load".to_string());
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            // Test ORDER BY NULL
            let order_by_null_query = "SELECT \"VendorID\" FROM csv_test ORDER BY NULL LIMIT 5";
            let query_result = rt
                .datafusion()
                .query_builder(order_by_null_query)
                .build()
                .run()
                .await
                .map_err(|e| format!("ORDER BY NULL query failed: {e}"))?;

            let _data = query_result
                .data
                .try_collect::<Vec<RecordBatch>>()
                .await
                .map_err(|e| format!("ORDER BY NULL query execution failed: {e}"))?;

            // Test ORDER BY rand()
            let order_by_rand_query = "SELECT \"VendorID\" FROM csv_test ORDER BY rand() LIMIT 5";
            let query_result = rt
                .datafusion()
                .query_builder(order_by_rand_query)
                .build()
                .run()
                .await
                .map_err(|e| format!("ORDER BY rand() query failed: {e}"))?;

            let _data = query_result
                .data
                .try_collect::<Vec<RecordBatch>>()
                .await
                .map_err(|e| format!("ORDER BY rand() query execution failed: {e}"))?;

            Ok(())
        })
        .await
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn duckdb_regexp() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let sample_csv_contents = include_str!("../test_data/regions.csv");
            let temp_file = NamedTempFile::new().expect("Should create temp file");
            std::fs::write(temp_file.path(), sample_csv_contents)
                .expect("failed to write sample file");

            let mut other_dataset = make_duckdb_acceleration_dataset(
                "csv_test_arrow",
                "csv",
                &format!("'{}'", temp_file.path().display()),
            );
            other_dataset.acceleration = Some(Acceleration {
                enabled: true,
                ..Default::default()
            });

            let app = AppBuilder::new("duckdb_regexp_test")
                .with_dataset(make_duckdb_acceleration_dataset(
                    "csv_test",
                    "csv",
                    &format!("'{}'", temp_file.path().display()),
                ))
                .with_dataset(other_dataset)
                .build();

            let rt = Runtime::builder()
                .with_app(app)
                .with_datafusion_configuration_fn(configure_test_datafusion)
                .build()
                .await;
            let cloned_rt = Arc::new(rt.clone());

            // Set a timeout for the test
            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err("Timed out waiting for datasets to load".to_string());
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            let cases = vec![
                (
                    "test_regexp_like_is_case_sensitive",
                    "SELECT * FROM csv_test WHERE regexp_like(region, 'america')",
                ),
                (
                    "test_regexp_like_with_case_insensitive_flag",
                    "SELECT * FROM csv_test WHERE regexp_like(region, 'america', 'i')",
                ),
                // disabled until github.com/spiceai/spiceai/issues/6964 is addressed
                // (
                //     "test_regexp_match",
                //     "SELECT regexp_match(region, 'AMERICA') FROM csv_test",
                // ),
                (
                    "test_regexp_count",
                    "SELECT regexp_count(region, 'AMERICA') FROM csv_test",
                ),
                (
                    "test_regexp_replace",
                    "SELECT regexp_replace(region, 'AMERICA', 'AUSTRALIA') FROM csv_test",
                ),
                (
                    "test_regexp_replace_case_insensitive",
                    "SELECT regexp_replace(region, 'america', 'australia', 'i') FROM csv_test",
                ),
                (
                    "test_regexp_results_match",
                    "WITH duckdb_regexp_like AS (
                        SELECT * FROM csv_test WHERE regexp_like(region, 'america', 'i')
                    ), arrow_regexp_like AS (
                        SELECT * FROM csv_test_arrow WHERE regexp_like(region, 'america', 'i')
                    )

                    SELECT * FROM duckdb_regexp_like d JOIN arrow_regexp_like a ON d.region = a.region",
                ),
            ];

            for (name, query) in cases {
                let result: Vec<RecordBatch> = rt
                    .datafusion()
                    .query_builder(query)
                    .build()
                    .run()
                    .await
                    .expect("query is successful")
                    .data
                    .try_collect()
                    .await
                    .expect("collects results");

                let pretty = arrow::util::pretty::pretty_format_batches(&result)
                    .map_err(|e| anyhow::Error::msg(e.to_string()))
                    .expect("Should format batches");
                insta::assert_snapshot!(format!("{name}_results"), pretty);

                let explain_plan = rt
                    .datafusion()
                    .query_builder(&format!("EXPLAIN {query}"))
                    .build()
                    .run()
                    .await
                    .map_err(|e| format!("explain plan for `{query}` failed: {e}"))?
                    .data
                    .try_collect::<Vec<RecordBatch>>()
                    .await
                    .map_err(|e| format!("explain plan for `{query}` execution failed: {e}"))?;
                let pretty = arrow::util::pretty::pretty_format_batches(&explain_plan)
                    .map_err(|e| anyhow::Error::msg(e.to_string()))
                    .expect("Should format batches");
                insta::assert_snapshot!(format!("{name}_explain"), pretty);
            }

            Ok(())
        })
        .await
}
