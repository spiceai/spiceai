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
use datafusion::sql::TableReference;
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
    utils::{register_test_connectors, runtime_ready_check, test_request_context},
};

#[cfg(feature = "duckdb")]
#[tokio::test]
async fn accelerated_view_duckdb() -> Result<(), anyhow::Error> {
    use datafusion_table_providers::sql::db_connection_pool::{
        DbConnectionPool, duckdbpool::DuckDbConnectionPool,
    };
    use duckdb::AccessMode;

    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

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

            // Ensure Checkpoint is created after initial view load (poll since checkpoint creation is async)
            let checkpoint = DatasetCheckpoint::try_new(&view, OpenOption::OpenExisting).await.expect("Failed to create view checkpoint");
            let checkpoint_timeout = std::time::Duration::from_secs(30);
            let checkpoint_start = std::time::Instant::now();
            while !checkpoint.exists().await {
                if checkpoint_start.elapsed() > checkpoint_timeout {
                    return Err(anyhow::anyhow!("Timed out waiting for checkpoint to exist"));
                }
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
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

#[tokio::test]
async fn test_view_dependency_ordering() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            // Create a test CSV file
            let test_csv = "value\n5\n15\n25\n50\n75\n150";
            std::fs::write("./test_view_deps.csv", test_csv).expect("write file");

            // Create a dataset
            let dataset = Dataset::new("file:./test_view_deps.csv", "base_data");

            // Create a view that depends on the dataset
            let mut view1 = View::new("view_level_1".to_string());
            view1.sql = Some("SELECT * FROM base_data WHERE value > 10".to_string());

            // Create a view that depends on view1
            let mut view2 = View::new("view_level_2".to_string());
            view2.sql = Some("SELECT * FROM view_level_1 WHERE value < 100".to_string());

            // Create a view that depends on view2
            let mut view3 = View::new("view_level_3".to_string());
            view3.sql = Some("SELECT COUNT(*) as count FROM view_level_2".to_string());

            // Add views in WRONG order (should be auto-sorted by dependency)
            let app = app::AppBuilder::new("test_view_dependency_ordering")
                .with_dataset(dataset)
                .with_view(view3.clone()) // Level 3 first
                .with_view(view1.clone()) // Level 1 second
                .with_view(view2.clone()) // Level 2 third
                .build();

            configure_test_datafusion();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);

            let cloned_rt = Arc::clone(&rt);

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for views to load"));
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            // Verify all views are registered and ready
            let status = rt.status();
            let view_statuses = status.get_view_statuses();

            let view1_ref = TableReference::bare("view_level_1");
            let view1_status = view_statuses.get(&view1_ref).expect("view_level_1 should exist");
            assert_eq!(
                *view1_status,
                runtime::status::ComponentStatus::Ready,
                "view_level_1 should be ready, got {view1_status:?}"
            );

            let view2_ref = TableReference::bare("view_level_2");
            let view2_status = view_statuses.get(&view2_ref).expect("view_level_2 should exist");
            assert_eq!(
                *view2_status,
                runtime::status::ComponentStatus::Ready,
                "view_level_2 should be ready, got {view2_status:?}"
            );

            let view3_ref = TableReference::bare("view_level_3");
            let view3_status = view_statuses.get(&view3_ref).expect("view_level_3 should exist");
            assert_eq!(
                *view3_status,
                runtime::status::ComponentStatus::Ready,
                "view_level_3 should be ready, got {view3_status:?}"
            );

            // Test that we can query the final view (which depends on the chain)
            let query_result = rt
                .datafusion()
                .query_builder("SELECT * FROM view_level_3")
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!(e))?
                .data
                .try_collect::<Vec<RecordBatch>>()
                .await;

            assert!(
                query_result.is_ok(),
                "Should be able to query view_level_3 which depends on view_level_2 which depends on view_level_1"
            );

            // Verify the count is correct (should be 3: values 15, 25, 50, 75 are > 10 and < 100)
            let batches = query_result.expect("query succeeded");
            let pretty = arrow::util::pretty::pretty_format_batches(&batches)
                .map_err(|e| anyhow::Error::msg(e.to_string()))?;

            // Should see count of 4 (15, 25, 50, 75)
            assert!(pretty.to_string().contains('4'), "Expected count of 4 in result: {pretty}");

            rt.shutdown().await;

            // Clean up test file
            std::fs::remove_file("./test_view_deps.csv").ok();

            Ok(())
        })
        .await
}

#[tokio::test]
async fn test_view_depending_on_dataset() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            // Create a test CSV file
            let test_csv = "id,name\n1,Alice\n2,Bob\n3,Charlie";
            std::fs::write("./test_view_dataset_dep.csv", test_csv).expect("write file");

            // Create a dataset
            let dataset = Dataset::new("file:./test_view_dataset_dep.csv", "users");

            // Create a view that depends on the dataset (not another view)
            let mut view1 = View::new("active_users".to_string());
            view1.sql = Some("SELECT * FROM users WHERE id > 0".to_string());

            // Create another view that depends on BOTH a dataset and a view
            let mut view2 = View::new("user_summary".to_string());
            view2.sql = Some("SELECT COUNT(*) as total FROM active_users".to_string());

            // Add in order that would fail without dataset dependency filtering
            let app = app::AppBuilder::new("test_view_dataset_dep")
                .with_dataset(dataset)
                .with_view(view1.clone())
                .with_view(view2.clone())
                .build();

            configure_test_datafusion();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);

            let cloned_rt = Arc::clone(&rt);

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for views to load"));
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            // Verify both views are ready (would fail with false cycle detection)
            let status = rt.status();
            let view_statuses = status.get_view_statuses();

            let view1_ref = TableReference::bare("active_users");
            let view1_status = view_statuses.get(&view1_ref).expect("active_users should exist");
            assert_eq!(
                *view1_status,
                runtime::status::ComponentStatus::Ready,
                "active_users should be ready (depends on dataset), got {view1_status:?}"
            );

            let view2_ref = TableReference::bare("user_summary");
            let view2_status = view_statuses.get(&view2_ref).expect("user_summary should exist");
            assert_eq!(
                *view2_status,
                runtime::status::ComponentStatus::Ready,
                "user_summary should be ready (depends on view and dataset), got {view2_status:?}"
            );

            // Test query to ensure data flows correctly
            let query_result = rt
                .datafusion()
                .query_builder("SELECT * FROM user_summary")
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!(e))?
                .data
                .try_collect::<Vec<RecordBatch>>()
                .await;

            assert!(
                query_result.is_ok(),
                "Should be able to query user_summary which depends on active_users which depends on dataset"
            );

            let batches = query_result.expect("query succeeded");
            let pretty = arrow::util::pretty::pretty_format_batches(&batches)
                .map_err(|e| anyhow::Error::msg(e.to_string()))?;

            // Should see count of 3 (Alice, Bob, Charlie)
            assert!(pretty.to_string().contains('3'), "Expected count of 3 in result: {pretty}");

            rt.shutdown().await;

            // Clean up test file
            std::fs::remove_file("./test_view_dataset_dep.csv").ok();

            Ok(())
        })
        .await
}

#[tokio::test]
async fn test_multiple_views_same_dataset() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            // Create a test CSV file
            let test_csv = "product,price\nApple,1.50\nBanana,0.75\nCherry,2.00\nDate,3.50";
            std::fs::write("./test_multi_view_dataset.csv", test_csv).expect("write file");

            // Create a dataset
            let dataset = Dataset::new("file:./test_multi_view_dataset.csv", "products");

            // Create multiple views that all depend on the same dataset
            let mut cheap_products = View::new("cheap_products".to_string());
            cheap_products.sql = Some("SELECT * FROM products WHERE price < 2.0".to_string());

            let mut expensive_products = View::new("expensive_products".to_string());
            expensive_products.sql = Some("SELECT * FROM products WHERE price >= 2.0".to_string());

            let mut product_count = View::new("product_count".to_string());
            product_count.sql = Some("SELECT COUNT(*) as count FROM products".to_string());

            // Add all views - none depend on each other, all depend on dataset
            let app = app::AppBuilder::new("test_multi_view_dataset")
                .with_dataset(dataset)
                .with_view(expensive_products.clone())
                .with_view(cheap_products.clone())
                .with_view(product_count.clone())
                .build();

            configure_test_datafusion();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);

            let cloned_rt = Arc::clone(&rt);

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for views to load"));
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            // Verify all views are ready (no false cycles even though multiple views depend on same dataset)
            let status = rt.status();
            let view_statuses = status.get_view_statuses();

            for view_name in ["cheap_products", "expensive_products", "product_count"] {
                let view_ref = TableReference::bare(view_name);
                let view_status = view_statuses
                    .get(&view_ref)
                    .unwrap_or_else(|| panic!("{view_name} should exist"));
                assert_eq!(
                    *view_status,
                    runtime::status::ComponentStatus::Ready,
                    "{view_name} should be ready, got {view_status:?}"
                );
            }

            // Verify queries work
            let cheap_result = rt
                .datafusion()
                .query_builder("SELECT COUNT(*) as cnt FROM cheap_products")
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!(e))?
                .data
                .try_collect::<Vec<RecordBatch>>()
                .await
                .expect("query succeeded");

            let pretty = arrow::util::pretty::pretty_format_batches(&cheap_result)
                .map_err(|e| anyhow::Error::msg(e.to_string()))?;
            assert!(
                pretty.to_string().contains('2'),
                "Expected 2 cheap products: {pretty}"
            );

            rt.shutdown().await;

            // Clean up test file
            std::fs::remove_file("./test_multi_view_dataset.csv").ok();

            Ok(())
        })
        .await
}

#[tokio::test]
async fn test_view_sql_validation() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            // Create test dataset
            let csv_data = "id,name,value\n1,test1,100\n2,test2,200\n3,test3,300";
            std::fs::write("./test_validation.csv", csv_data).expect("to write CSV file");

            let dataset = Dataset::new("file:./test_validation.csv", "validation_data");

            configure_test_datafusion();

            // Test 1: Valid SQL - should succeed
            {
                let mut view = View::new("valid_view".to_string());
                view.sql = Some("SELECT * FROM validation_data".to_string());

                let app = app::AppBuilder::new("test_valid_sql")
                    .with_dataset(dataset.clone())
                    .with_view(view)
                    .build();

                let rt = Arc::new(Runtime::builder().with_app(app).build().await);

                let cloned_rt = Arc::clone(&rt);
                tokio::select! {
                    () = tokio::time::sleep(std::time::Duration::from_secs(30)) => {
                        return Err(anyhow::anyhow!("Timed out waiting for components to load"));
                    }
                    () = cloned_rt.load_components() => {}
                }

                // View should be ready
                let view_statuses = rt.status().get_view_statuses();
                let view_status = view_statuses.get(&TableReference::bare("valid_view"));
                if let Some(status) = view_status {
                    if *status != runtime::status::ComponentStatus::Ready {
                        return Err(anyhow::anyhow!(
                            "Valid view should be ready, got {status:?}"
                        ));
                    }
                } else {
                    return Err(anyhow::anyhow!("Valid view status not found"));
                }

                rt.shutdown().await;
            }

            // Test 2: Invalid SQL syntax - should fail with error
            {
                let mut view = View::new("invalid_syntax_view".to_string());
                view.sql = Some("SELECT * FORM validation_data".to_string()); // FORM instead of FROM

                let app = app::AppBuilder::new("test_invalid_syntax")
                    .with_dataset(dataset.clone())
                    .with_view(view)
                    .build();

                let rt = Arc::new(Runtime::builder().with_app(app).build().await);

                let cloned_rt = Arc::clone(&rt);
                tokio::select! {
                    () = tokio::time::sleep(std::time::Duration::from_secs(30)) => {
                        return Err(anyhow::anyhow!("Timed out waiting for components to load"));
                    }
                    () = cloned_rt.load_components() => {}
                }

                // View should be in error state
                let view_statuses = rt.status().get_view_statuses();
                let view_status = view_statuses.get(&TableReference::bare("invalid_syntax_view"));
                if let Some(status) = view_status {
                    if *status != runtime::status::ComponentStatus::Error {
                        return Err(anyhow::anyhow!(
                            "Invalid SQL view should be in error state, got {status:?}"
                        ));
                    }
                } else {
                    return Err(anyhow::anyhow!("Invalid SQL view status not found"));
                }

                rt.shutdown().await;
            }

            // Test 3: Empty SQL - should fail
            {
                let mut view = View::new("empty_sql_view".to_string());
                view.sql = Some(String::new());

                let app = app::AppBuilder::new("test_empty_sql")
                    .with_dataset(dataset.clone())
                    .with_view(view)
                    .build();

                let rt = Arc::new(Runtime::builder().with_app(app).build().await);

                let cloned_rt = Arc::clone(&rt);
                tokio::select! {
                    () = tokio::time::sleep(std::time::Duration::from_secs(30)) => {
                        return Err(anyhow::anyhow!("Timed out waiting for components to load"));
                    }
                    () = cloned_rt.load_components() => {}
                }

                // View should be in error state
                let view_statuses = rt.status().get_view_statuses();
                let view_status = view_statuses.get(&TableReference::bare("empty_sql_view"));
                if let Some(status) = view_status {
                    if *status != runtime::status::ComponentStatus::Error {
                        return Err(anyhow::anyhow!(
                            "Empty SQL view should be in error state, got {status:?}"
                        ));
                    }
                } else {
                    return Err(anyhow::anyhow!("Empty SQL view status not found"));
                }

                rt.shutdown().await;
            }

            // Test 4: Multiple statements - should fail
            {
                let mut view = View::new("multi_statement_view".to_string());
                view.sql = Some("SELECT * FROM validation_data; SELECT COUNT(*) FROM validation_data".to_string());

                let app = app::AppBuilder::new("test_multi_statement")
                    .with_dataset(dataset.clone())
                    .with_view(view)
                    .build();

                let rt = Arc::new(Runtime::builder().with_app(app).build().await);

                let cloned_rt = Arc::clone(&rt);
                tokio::select! {
                    () = tokio::time::sleep(std::time::Duration::from_secs(30)) => {
                        return Err(anyhow::anyhow!("Timed out waiting for components to load"));
                    }
                    () = cloned_rt.load_components() => {}
                }

                // View should be in error state
                let view_statuses = rt.status().get_view_statuses();
                let view_status = view_statuses.get(&TableReference::bare("multi_statement_view"));
                if let Some(status) = view_status {
                    if *status != runtime::status::ComponentStatus::Error {
                        return Err(anyhow::anyhow!(
                            "Multi-statement view should be in error state, got {status:?}"
                        ));
                    }
                } else {
                    return Err(anyhow::anyhow!("Multi-statement view status not found"));
                }

                rt.shutdown().await;
            }

            // Test 5: Non-SELECT statement - should fail
            {
                let mut view = View::new("insert_view".to_string());
                view.sql = Some("INSERT INTO validation_data VALUES (4, 'test4', 400)".to_string());

                let app = app::AppBuilder::new("test_non_select")
                    .with_dataset(dataset.clone())
                    .with_view(view)
                    .build();

                let rt = Arc::new(Runtime::builder().with_app(app).build().await);

                let cloned_rt = Arc::clone(&rt);
                tokio::select! {
                    () = tokio::time::sleep(std::time::Duration::from_secs(30)) => {
                        return Err(anyhow::anyhow!("Timed out waiting for components to load"));
                    }
                    () = cloned_rt.load_components() => {}
                }

                // View should be in error state
                let view_statuses = rt.status().get_view_statuses();
                let view_status = view_statuses.get(&TableReference::bare("insert_view"));
                if let Some(status) = view_status {
                    if *status != runtime::status::ComponentStatus::Error {
                        return Err(anyhow::anyhow!(
                            "Non-SELECT view should be in error state, got {status:?}"
                        ));
                    }
                } else {
                    return Err(anyhow::anyhow!("Non-SELECT view status not found"));
                }

                rt.shutdown().await;
            }

            // Test 6: Complex valid SQL with joins and aggregations - should succeed
            {
                let mut view = View::new("complex_valid_view".to_string());
                view.sql = Some(
                    "SELECT name, SUM(value) as total_value FROM validation_data GROUP BY name HAVING SUM(value) > 50".to_string()
                );

                let app = app::AppBuilder::new("test_complex_valid")
                    .with_dataset(dataset.clone())
                    .with_view(view)
                    .build();

                let rt = Arc::new(Runtime::builder().with_app(app).build().await);

                let cloned_rt = Arc::clone(&rt);
                tokio::select! {
                    () = tokio::time::sleep(std::time::Duration::from_secs(30)) => {
                        return Err(anyhow::anyhow!("Timed out waiting for components to load"));
                    }
                    () = cloned_rt.load_components() => {}
                }

                // View should be ready
                let view_statuses = rt.status().get_view_statuses();
                let view_status = view_statuses.get(&TableReference::bare("complex_valid_view"));
                if let Some(status) = view_status {
                    if *status != runtime::status::ComponentStatus::Ready {
                        return Err(anyhow::anyhow!(
                            "Complex valid view should be ready, got {status:?}"
                        ));
                    }
                } else {
                    return Err(anyhow::anyhow!("Complex valid view status not found"));
                }

                // Verify the view can be queried
                let query_result = rt
                    .datafusion()
                    .query_builder("SELECT * FROM complex_valid_view")
                    .build()
                    .run()
                    .await
                    .map_err(|e| anyhow::anyhow!(e))?
                    .data
                    .try_collect::<Vec<RecordBatch>>()
                    .await
                    .expect("collects results");

                if query_result.is_empty() {
                    return Err(anyhow::anyhow!("Query should return results"));
                }

                rt.shutdown().await;
            }

            // Clean up test file
            std::fs::remove_file("./test_validation.csv").ok();

            Ok(())
        })
        .await
}
