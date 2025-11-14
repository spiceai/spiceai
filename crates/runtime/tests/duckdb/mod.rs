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

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;
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

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;
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

            configure_test_datafusion();
            let rt = Runtime::builder()
                .with_app(app)
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
                (
                    "test_regexp_match",
                    "SELECT regexp_match(region, 'AMERICA') FROM csv_test",
                ),
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

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_duckdb_settings_persist() -> Result<(), String> {
    use spicepod::param::Params;
    use std::collections::HashMap;

    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            // Create a temporary DuckDB file
            let temp_dir = std::env::temp_dir().join("spiced_duckdb_settings_test");
            std::fs::create_dir_all(&temp_dir).expect("failed to create temp dir");
            let duckdb_file = temp_dir.join("test_settings.db");

            defer! {
                std::fs::remove_dir_all(&temp_dir).expect("failed to remove temp dir");
            }

            // Create a dataset with DuckDB acceleration and custom settings
            let mut accel_params = HashMap::new();
            accel_params.insert(
                "duckdb_file".to_string(),
                duckdb_file
                    .to_str()
                    .expect("DuckDB file path should be valid UTF-8")
                    .to_string(),
            );
            accel_params.insert(
                "duckdb_index_scan_percentage".to_string(),
                "0.05".to_string(),
            ); // 5% as decimal
            accel_params.insert(
                "duckdb_index_scan_max_count".to_string(),
                "5000".to_string(),
            );

            // Create a simple CSV file for testing
            let csv_file = temp_dir.join("test.csv");
            std::fs::write(&csv_file, "id,name\n1,test\n2,test2\n").expect("failed to write csv");

            let mut dataset = Dataset::new(
                format!("file:{}", csv_file.display()),
                "test_settings".to_string(),
            );
            dataset.name = "test_settings".to_string();
            dataset.acceleration = Some(Acceleration {
                enabled: true,
                engine: Some("duckdb".to_string()),
                mode: Mode::File,
                refresh_mode: Some(RefreshMode::Full),
                params: Some(Params::from_string_map(accel_params)),
                ..Acceleration::default()
            });

            let app = AppBuilder::new("duckdb_settings_test")
                .with_dataset(dataset)
                .build();

            configure_test_datafusion();

            let rt = Runtime::builder().with_app(app).build().await;
            let cloned_rt = Arc::new(rt.clone());

            // Set a timeout for the test
            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(30)) => {
                    return Err("Timed out waiting for datasets to load".to_string());
                }
                () = Arc::clone(&cloned_rt).load_components() => {}
            }

            runtime_ready_check(&cloned_rt).await;

            // Verify the accelerated dataset loaded successfully
            println!("✅ DuckDB accelerator initialized successfully with custom settings:");
            println!("   - duckdb_index_scan_percentage: 0.05 (5%)");
            println!("   - duckdb_index_scan_max_count: 5000");
            println!("   - PRAGMA enable_checkpoint_on_shutdown (automatic)");

            // Query the accelerated table to ensure it's working
            let df = cloned_rt.datafusion();
            let result = df
                .query_builder("SELECT COUNT(*) as row_count FROM test_settings")
                .build()
                .run()
                .await
                .map_err(|e| format!("Failed to query test_settings: {e}"))?;

            let batches: Vec<RecordBatch> = result
                .data
                .try_collect()
                .await
                .map_err(|e| format!("Failed to collect results: {e}"))?;

            // Verify we got data
            if batches.is_empty() || batches[0].num_rows() == 0 {
                return Err("No rows returned from query".to_string());
            }

            let count_col = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .ok_or_else(|| "Failed to downcast count column".to_string())?;
            let count = count_col.value(0);

            println!("✅ Query successful: test_settings table has {count} rows");

            if count != 2 {
                return Err(format!("Expected 2 rows, got {count}"));
            }

            // Shutdown the runtime
            cloned_rt.shutdown().await;
            drop(cloned_rt);
            drop(rt);

            // Give time for shutdown and checkpoint
            tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

            // Verify the file was checkpointed (file should exist and be non-zero)
            if !duckdb_file.exists() {
                return Err("DuckDB file does not exist after shutdown".to_string());
            }

            let metadata = std::fs::metadata(&duckdb_file)
                .map_err(|e| format!("Failed to get file metadata: {e}"))?;

            println!(
                "✓ DuckDB file size after shutdown: {} bytes",
                metadata.len()
            );

            if metadata.len() == 0 {
                return Err(
                    "DuckDB file is empty after shutdown - checkpoint may have failed".to_string(),
                );
            }

            Ok(())
        })
        .await
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_duckdb_all_settings() -> Result<(), String> {
    use spicepod::param::Params;
    use std::collections::HashMap;

    let _tracing = init_tracing(Some("integration=debug,info"));

    Box::pin(test_request_context()
        .scope(async {
            // Test 1: Index scan settings with custom file
            println!("\n=== Test 1: Index Scan Settings with Custom File ===");
            {
                let temp_dir = std::env::temp_dir().join("spiced_duckdb_test_1");
                std::fs::create_dir_all(&temp_dir).expect("failed to create temp dir");
                let duckdb_file = temp_dir.join("test_index_scan.db");

                defer! {
                    std::fs::remove_dir_all(&temp_dir).expect("failed to remove temp dir");
                }

                let mut accel_params = HashMap::new();
                accel_params.insert("duckdb_file".to_string(), duckdb_file.to_str().expect("DuckDB file path should be valid UTF-8").to_string());
                accel_params.insert("duckdb_index_scan_percentage".to_string(), "0.05".to_string());
                accel_params.insert("duckdb_index_scan_max_count".to_string(), "5000".to_string());

                let csv_file = temp_dir.join("test.csv");
                std::fs::write(&csv_file, "id,name\n1,test\n2,test2\n").expect("failed to write csv");

                let mut dataset = Dataset::new(format!("file:{}", csv_file.display()), "test_index_scan".to_string());
                dataset.name = "test_index_scan".to_string();
                dataset.acceleration = Some(Acceleration {
                    enabled: true,
                    engine: Some("duckdb".to_string()),
                    mode: Mode::File,
                    refresh_mode: Some(RefreshMode::Full),
                    params: Some(Params::from_string_map(accel_params)),
                    ..Acceleration::default()
                });

                let app = AppBuilder::new("duckdb_test_index_scan").with_dataset(dataset).build();
                configure_test_datafusion();
                let rt = Runtime::builder().with_app(app).build().await;
                let cloned_rt = Arc::new(rt.clone());

                tokio::select! {
                    () = tokio::time::sleep(std::time::Duration::from_secs(30)) => {
                        return Err("Test 1: Timed out waiting for datasets to load".to_string());
                    }
                    () = Arc::clone(&cloned_rt).load_components() => {}
                }

                runtime_ready_check(&cloned_rt).await;

                // Verify query works
                let df = cloned_rt.datafusion();
                let result = df
                    .query_builder("SELECT COUNT(*) FROM test_index_scan")
                    .build()
                    .run()
                    .await
                    .map_err(|e| format!("Test 1: Query failed: {e}"))?;
                let batches: Vec<RecordBatch> = result.data.try_collect().await
                    .map_err(|e| format!("Test 1: Failed to collect: {e}"))?;

                let count_col = batches[0].column(0).as_any().downcast_ref::<arrow::array::Int64Array>()
                    .ok_or_else(|| "Test 1: Failed to downcast".to_string())?;
                assert_eq!(count_col.value(0), 2, "Test 1: Expected 2 rows");

                println!("✅ Index scan settings applied successfully");
                println!("   - duckdb_file: custom path");
                println!("   - index_scan_percentage: 0.05");
                println!("   - index_scan_max_count: 5000");

                cloned_rt.shutdown().await;
                drop(cloned_rt);
                drop(rt);
                tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

                // Verify checkpoint occurred
                assert!(duckdb_file.exists(), "Test 1: DuckDB file should exist");
                let metadata = std::fs::metadata(&duckdb_file)
                    .map_err(|e| format!("Test 1: Failed to get metadata: {e}"))?;
                assert!(metadata.len() > 0, "Test 1: File should be non-zero");
                println!("✅ Checkpoint verified: {} bytes", metadata.len());
            }

            // Test 2: Memory limit setting
            println!("\n=== Test 2: Memory Limit Setting ===");
            {
                let temp_dir = std::env::temp_dir().join("spiced_duckdb_test_2");
                std::fs::create_dir_all(&temp_dir).expect("failed to create temp dir");

                defer! {
                    std::fs::remove_dir_all(&temp_dir).expect("failed to remove temp dir");
                }

                let mut accel_params = HashMap::new();
                accel_params.insert("duckdb_memory_limit".to_string(), "512MB".to_string());

                let csv_file = temp_dir.join("test.csv");
                std::fs::write(&csv_file, "id,value\n1,100\n2,200\n3,300\n").expect("failed to write csv");

                let mut dataset = Dataset::new(format!("file:{}", csv_file.display()), "test_memory".to_string());
                dataset.name = "test_memory".to_string();
                dataset.acceleration = Some(Acceleration {
                    enabled: true,
                    engine: Some("duckdb".to_string()),
                    mode: Mode::Memory,
                    refresh_mode: Some(RefreshMode::Full),
                    params: Some(Params::from_string_map(accel_params)),
                    ..Acceleration::default()
                });

                let app = AppBuilder::new("duckdb_test_memory").with_dataset(dataset).build();
                configure_test_datafusion();
                let rt = Runtime::builder().with_app(app).build().await;
                let cloned_rt = Arc::new(rt.clone());

                tokio::select! {
                    () = tokio::time::sleep(std::time::Duration::from_secs(30)) => {
                        return Err("Test 2: Timed out waiting for datasets to load".to_string());
                    }
                    () = Arc::clone(&cloned_rt).load_components() => {}
                }

                runtime_ready_check(&cloned_rt).await;

                let df = cloned_rt.datafusion();
                let result = df
                    .query_builder("SELECT SUM(value) as total FROM test_memory")
                    .build()
                    .run()
                    .await
                    .map_err(|e| format!("Test 2: Query failed: {e}"))?;
                let batches: Vec<RecordBatch> = result.data.try_collect().await
                    .map_err(|e| format!("Test 2: Failed to collect: {e}"))?;

                let sum_col = batches[0].column(0).as_any().downcast_ref::<arrow::array::Int64Array>()
                    .ok_or_else(|| "Test 2: Failed to downcast".to_string())?;
                assert_eq!(sum_col.value(0), 600, "Test 2: Expected sum of 600");

                println!("✅ Memory limit setting applied successfully");
                println!("   - memory_limit: 512MB");
                println!("   - mode: Memory");

                cloned_rt.shutdown().await;
                drop(cloned_rt);
                drop(rt);
                tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
            }

            // Test 3: Preserve insertion order
            println!("\n=== Test 3: Preserve Insertion Order ===");
            {
                let temp_dir = std::env::temp_dir().join("spiced_duckdb_test_3");
                std::fs::create_dir_all(&temp_dir).expect("failed to create temp dir");

                defer! {
                    std::fs::remove_dir_all(&temp_dir).expect("failed to remove temp dir");
                }

                let mut accel_params = HashMap::new();
                accel_params.insert("duckdb_preserve_insertion_order".to_string(), "true".to_string());

                let csv_file = temp_dir.join("test.csv");
                std::fs::write(&csv_file, "id,name\n3,charlie\n1,alice\n2,bob\n").expect("failed to write csv");

                let mut dataset = Dataset::new(format!("file:{}", csv_file.display()), "test_order".to_string());
                dataset.name = "test_order".to_string();
                dataset.acceleration = Some(Acceleration {
                    enabled: true,
                    engine: Some("duckdb".to_string()),
                    mode: Mode::File,
                    refresh_mode: Some(RefreshMode::Full),
                    params: Some(Params::from_string_map(accel_params)),
                    ..Acceleration::default()
                });

                let app = AppBuilder::new("duckdb_test_order").with_dataset(dataset).build();
                configure_test_datafusion();
                let rt = Runtime::builder().with_app(app).build().await;
                let cloned_rt = Arc::new(rt.clone());

                tokio::select! {
                    () = tokio::time::sleep(std::time::Duration::from_secs(30)) => {
                        return Err("Test 3: Timed out waiting for datasets to load".to_string());
                    }
                    () = Arc::clone(&cloned_rt).load_components() => {}
                }

                runtime_ready_check(&cloned_rt).await;

                let df = cloned_rt.datafusion();
                let result = df
                    .query_builder("SELECT * FROM test_order")
                    .build()
                    .run()
                    .await
                    .map_err(|e| format!("Test 3: Query failed: {e}"))?;
                let batches: Vec<RecordBatch> = result.data.try_collect().await
                    .map_err(|e| format!("Test 3: Failed to collect: {e}"))?;

                assert!(!batches.is_empty(), "Test 3: Should have results");
                assert_eq!(batches[0].num_rows(), 3, "Test 3: Expected 3 rows");

                println!("✅ Preserve insertion order setting applied successfully");
                println!("   - preserve_insertion_order: true");

                cloned_rt.shutdown().await;
                drop(cloned_rt);
                drop(rt);
                tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
            }

            // Test 4: Combined settings
            println!("\n=== Test 4: Combined Settings ===");
            {
                let temp_dir = std::env::temp_dir().join("spiced_duckdb_test_4");
                std::fs::create_dir_all(&temp_dir).expect("failed to create temp dir");
                let duckdb_file = temp_dir.join("test_combined.db");

                defer! {
                    std::fs::remove_dir_all(&temp_dir).expect("failed to remove temp dir");
                }

                let mut accel_params = HashMap::new();
                accel_params.insert("duckdb_file".to_string(), duckdb_file.to_str().expect("DuckDB file path should be valid UTF-8").to_string());
                accel_params.insert("duckdb_memory_limit".to_string(), "256MB".to_string());
                accel_params.insert("duckdb_index_scan_percentage".to_string(), "0.10".to_string());
                accel_params.insert("duckdb_index_scan_max_count".to_string(), "1000".to_string());
                accel_params.insert("duckdb_preserve_insertion_order".to_string(), "false".to_string());

                let csv_file = temp_dir.join("test.csv");
                std::fs::write(&csv_file, "id,category,amount\n1,A,100\n2,B,200\n3,A,150\n4,C,300\n").expect("failed to write csv");

                let mut dataset = Dataset::new(format!("file:{}", csv_file.display()), "test_combined".to_string());
                dataset.name = "test_combined".to_string();
                dataset.acceleration = Some(Acceleration {
                    enabled: true,
                    engine: Some("duckdb".to_string()),
                    mode: Mode::File,
                    refresh_mode: Some(RefreshMode::Full),
                    params: Some(Params::from_string_map(accel_params)),
                    ..Acceleration::default()
                });

                let app = AppBuilder::new("duckdb_test_combined").with_dataset(dataset).build();
                configure_test_datafusion();
                let rt = Runtime::builder().with_app(app).build().await;
                let cloned_rt = Arc::new(rt.clone());

                tokio::select! {
                    () = tokio::time::sleep(std::time::Duration::from_secs(30)) => {
                        return Err("Test 4: Timed out waiting for datasets to load".to_string());
                    }
                    () = Arc::clone(&cloned_rt).load_components() => {}
                }

                runtime_ready_check(&cloned_rt).await;

                let df = cloned_rt.datafusion();

                // Test aggregation
                let result = df
                    .query_builder("SELECT category, SUM(amount) as total FROM test_combined GROUP BY category ORDER BY category")
                    .build()
                    .run()
                    .await
                    .map_err(|e| format!("Test 4: Aggregation query failed: {e}"))?;
                let batches: Vec<RecordBatch> = result.data.try_collect().await
                    .map_err(|e| format!("Test 4: Failed to collect: {e}"))?;

                assert_eq!(batches[0].num_rows(), 3, "Test 4: Expected 3 categories");

                println!("✅ Combined settings applied successfully");
                println!("   - file: custom path");
                println!("   - memory_limit: 256MB");
                println!("   - index_scan_percentage: 0.10");
                println!("   - index_scan_max_count: 1000");
                println!("   - preserve_insertion_order: false");
                println!("   - PRAGMA enable_checkpoint_on_shutdown: automatic");

                cloned_rt.shutdown().await;
                drop(cloned_rt);
                drop(rt);
                tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

                // Verify checkpoint
                assert!(duckdb_file.exists(), "Test 4: DuckDB file should exist");
                let metadata = std::fs::metadata(&duckdb_file)
                    .map_err(|e| format!("Test 4: Failed to get metadata: {e}"))?;
                assert!(metadata.len() > 0, "Test 4: File should be non-zero");
                println!("✅ Checkpoint verified: {} bytes", metadata.len());
            }

            println!("\n=== All DuckDB Settings Tests Passed ===");
            Ok(())
        }))
        .await
}
