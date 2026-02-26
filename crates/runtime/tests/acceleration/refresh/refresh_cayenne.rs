/*
Copyright 2025 The Spice.ai OSS Authors

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
use crate::acceleration::refresh::common::{
    execute_ps_sql, execute_rt_sql, get_acceleration_config_append, get_acceleration_config_full,
    get_dataset_no_time_column, initialize_postgres, initialize_postgres_with_value_column,
    refresh_table, start_test_runtime,
};
use crate::postgres::common;
use crate::postgres::common::get_random_port;
use crate::{
    configure_test_datafusion, configure_test_datafusion_request_context, init_tracing,
    utils::test_request_context,
};
use arrow::array::Array;
use spicepod::acceleration::Mode;
use spicepod::param::Params;
use std::collections::HashMap;
use std::sync::Arc;

#[tokio::test]
async fn test_acceleration_refresh_cayenne_append() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let port: usize = get_random_port()?;
            let running_container = common::start_postgres_docker_container(port).await?;

            let db_conn = initialize_postgres(port).await?;

            // Create unique temp directory for this test
            let temp_dir = tempfile::tempdir()?;
            let metadata_dir = temp_dir.path().join("cayenne_metadata");
            std::fs::create_dir_all(&metadata_dir)?;

            let mut params = HashMap::new();
            params.insert(
                "cayenne_metadata_dir".to_string(),
                metadata_dir.to_str().expect("valid UTF-8 path").to_string(),
            );

            let mut acceleration_config =
                get_acceleration_config_append("cayenne", Some(Params::from_string_map(params)));
            acceleration_config.mode = Mode::File;
            // Cayenne append mode requires either primary_key or time_column
            // Here we remove primary_key but keep time_column (via start_test_runtime)
            acceleration_config.primary_key = None;
            let rt = start_test_runtime(port, acceleration_config).await?;

            let results = execute_rt_sql(Arc::clone(&rt), "SELECT * from test_table").await?;
            assert_eq!(
                results
                    .iter()
                    .map(arrow::array::RecordBatch::num_rows)
                    .sum::<usize>(),
                1
            );

            execute_ps_sql(
                &db_conn,
                "INSERT INTO test_table (created_at) VALUES (date_trunc('milliseconds', now()));",
            )
            .await?;

            refresh_table(Arc::clone(&rt), "test_table").await?;

            let results = execute_rt_sql(Arc::clone(&rt), "SELECT * from test_table").await?;
            assert_eq!(
                results
                    .iter()
                    .map(arrow::array::RecordBatch::num_rows)
                    .sum::<usize>(),
                2
            );

            running_container.remove().await?;
            Ok(())
        })
        .await
}

#[tokio::test]
async fn test_acceleration_refresh_cayenne_full() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let port: usize = get_random_port()?;
            let running_container = common::start_postgres_docker_container(port).await?;

            let db_conn = initialize_postgres(port).await?;

            // Create unique temp directory for this test
            let temp_dir = tempfile::tempdir()?;
            let metadata_dir = temp_dir.path().join("cayenne_metadata");
            std::fs::create_dir_all(&metadata_dir)?;

            let mut params = HashMap::new();
            params.insert(
                "cayenne_metadata_dir".to_string(),
                metadata_dir.to_str().expect("valid UTF-8 path").to_string(),
            );

            let mut acceleration_config =
                get_acceleration_config_full("cayenne", Some(Params::from_string_map(params)));
            acceleration_config.mode = Mode::File;
            let rt = start_test_runtime(port, acceleration_config).await?;

            let results = execute_rt_sql(Arc::clone(&rt), "SELECT * from test_table").await?;
            assert_eq!(
                results
                    .iter()
                    .map(arrow::array::RecordBatch::num_rows)
                    .sum::<usize>(),
                1
            );

            execute_ps_sql(
                &db_conn,
                "INSERT INTO test_table (created_at) VALUES (date_trunc('milliseconds', now()));",
            )
            .await?;

            refresh_table(Arc::clone(&rt), "test_table").await?;

            let results = execute_rt_sql(Arc::clone(&rt), "SELECT * from test_table").await?;
            assert_eq!(
                results
                    .iter()
                    .map(arrow::array::RecordBatch::num_rows)
                    .sum::<usize>(),
                2
            );

            running_container.remove().await?;
            Ok(())
        })
        .await
}

/// Test that Cayenne append mode works with both `primary_key` and `time_column` specified.
/// This validates that:
/// 1. `time_column` is used for incremental queries (fetch only new data)
/// 2. `primary_key` is used for deduplication at insert time (upsert behavior)
#[tokio::test]
async fn test_cayenne_append_mode_with_pk_and_time_column() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let port: usize = get_random_port()?;
            let running_container = common::start_postgres_docker_container(port).await?;

            // Use table with value column for testing upserts
            let db_conn = initialize_postgres_with_value_column(port).await?;

            // Create unique temp directory for this test
            let temp_dir = tempfile::tempdir()?;
            let metadata_dir = temp_dir.path().join("cayenne_metadata");
            std::fs::create_dir_all(&metadata_dir)?;

            let mut params = HashMap::new();
            params.insert(
                "cayenne_metadata_dir".to_string(),
                metadata_dir.to_str().expect("valid UTF-8 path").to_string(),
            );

            // Configure with BOTH primary_key and time_column
            let mut acceleration_config =
                get_acceleration_config_append("cayenne", Some(Params::from_string_map(params)));
            acceleration_config.mode = Mode::File;
            // Keep both primary_key (from get_acceleration_config_append) and time_column (from get_dataset)

            let rt = start_test_runtime(port, acceleration_config).await?;

            // Verify initial data loaded with correct value
            let results =
                execute_rt_sql(Arc::clone(&rt), "SELECT id, value from test_table").await?;
            assert_eq!(
                results
                    .iter()
                    .map(arrow::array::RecordBatch::num_rows)
                    .sum::<usize>(),
                1,
                "Expected 1 row after initial load"
            );

            // Check initial value
            let initial_value = results
                .first()
                .and_then(|batch| {
                    batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<arrow::array::StringArray>()
                })
                .and_then(|arr| if arr.is_empty() { None } else { Some(arr.value(0).to_string()) })
                .expect("Should have initial value");
            assert_eq!(initial_value, "initial_value", "Initial value should be 'initial_value'");

            // Get the ID of the first row for later upsert test
            let first_id = results
                .first()
                .and_then(|batch| {
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<arrow::array::Int32Array>()
                })
                .and_then(|arr| if arr.is_empty() { None } else { Some(arr.value(0)) })
                .expect("Should have at least one row");

            // Insert a new row with a new timestamp and different value
            execute_ps_sql(
                &db_conn,
                "INSERT INTO test_table (created_at, value) VALUES (date_trunc('milliseconds', now()), 'second_value');",
            )
            .await?;

            refresh_table(Arc::clone(&rt), "test_table").await?;

            // Verify new row was appended
            let results = execute_rt_sql(Arc::clone(&rt), "SELECT * from test_table").await?;
            assert_eq!(
                results
                    .iter()
                    .map(arrow::array::RecordBatch::num_rows)
                    .sum::<usize>(),
                2,
                "Expected 2 rows after refresh with new data"
            );

            // Test upsert: update existing row (same PK, new timestamp, new value)
            execute_ps_sql(
                &db_conn,
                &format!(
                    "UPDATE test_table SET created_at = date_trunc('milliseconds', now() + interval '1 second'), value = 'updated_value' WHERE id = {first_id};"
                ),
            )
            .await?;

            refresh_table(Arc::clone(&rt), "test_table").await?;

            // Should still have 2 rows (upsert updated the existing row, not inserted a new one)
            let results = execute_rt_sql(
                Arc::clone(&rt),
                &format!("SELECT id, value from test_table WHERE id = {first_id}"),
            )
            .await?;
            assert_eq!(
                results
                    .iter()
                    .map(arrow::array::RecordBatch::num_rows)
                    .sum::<usize>(),
                1,
                "Expected exactly 1 row with the upserted id"
            );

            // Verify the value was actually updated
            let updated_value = results
                .first()
                .and_then(|batch| {
                    batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<arrow::array::StringArray>()
                })
                .and_then(|arr| if arr.is_empty() { None } else { Some(arr.value(0).to_string()) })
                .expect("Should have updated value");
            assert_eq!(
                updated_value, "updated_value",
                "Value should be updated to 'updated_value' after upsert"
            );

            // Verify total row count is still 2
            let results = execute_rt_sql(Arc::clone(&rt), "SELECT * from test_table").await?;
            assert_eq!(
                results
                    .iter()
                    .map(arrow::array::RecordBatch::num_rows)
                    .sum::<usize>(),
                2,
                "Expected 2 rows total after upsert (not 3) - PK deduplication should work"
            );

            tracing::info!("✓ Cayenne append mode with both primary_key and time_column works correctly");

            running_container.remove().await?;
            Ok(())
        })
        .await
}

#[tokio::test]
#[ignore = "https://github.com/spiceai/spiceai/issues/7860"] // https://github.com/spiceai/spiceai/issues/7860
async fn test_cayenne_append_mode_requires_constraint() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let port: usize = get_random_port()?;
            let running_container = common::start_postgres_docker_container(port).await?;

            let _db_conn = initialize_postgres(port).await?;

            // Create unique temp directory for this test
            let temp_dir = tempfile::tempdir()?;
            let metadata_dir = temp_dir.path().join("cayenne_metadata");
            std::fs::create_dir_all(&metadata_dir)?;

            let mut params = HashMap::new();
            params.insert(
                "cayenne_metadata_dir".to_string(),
                metadata_dir.to_str().expect("valid UTF-8 path").to_string(),
            );

            let mut acceleration_config =
                get_acceleration_config_append("cayenne", Some(Params::from_string_map(params)));
            acceleration_config.mode = Mode::File;

            // Remove both primary_key and time_column - this should cause dataset initialization to fail
            acceleration_config.primary_key = None;

            // Create the dataset with invalid configuration
            let mut dataset = get_dataset_no_time_column(port);
            dataset.acceleration = Some(acceleration_config);

            let app = app::AppBuilder::new("test_acceleration_refresh")
                .with_dataset(dataset)
                .build();

            configure_test_datafusion();
            configure_test_datafusion_request_context();

            let rt = Arc::new(runtime::Runtime::builder().with_app(app).build().await);

            // Spawn load_components in background (it will keep retrying)
            let rt_clone = Arc::clone(&rt);
            tokio::spawn(async move {
                rt_clone.load_components().await;
            });

            // Wait a bit for the first initialization attempt
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;

            // Verify that the dataset is not available (failed to initialize)
            let result = execute_rt_sql(Arc::clone(&rt), "SELECT * from test_table").await;
            assert!(
                result.is_err(),
                "Expected query to fail because dataset should not be initialized"
            );

            let err_msg = result.expect_err("Expected error").to_string();
            assert!(
                err_msg.contains("test_table")
                    || err_msg.contains("not found")
                    || err_msg.contains("'datafusion.catalog.spice.public.test_table' not found"),
                "Error message should indicate table not found, got: {err_msg}"
            );
            tracing::info!("✓ Validation correctly rejects append mode without constraints");

            running_container.remove().await?;
            Ok(())
        })
        .await
}
