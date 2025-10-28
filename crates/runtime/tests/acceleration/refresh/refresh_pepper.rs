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
    initialize_postgres, refresh_table, start_test_runtime, start_test_runtime_no_time_column,
};
use crate::postgres::common;
use crate::postgres::common::get_random_port;
use crate::{init_tracing, utils::test_request_context};
use spicepod::acceleration::Mode;
use spicepod::param::Params;
use std::collections::HashMap;
use std::sync::Arc;

#[tokio::test]
async fn test_acceleration_refresh_pepper_append() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let port: usize = get_random_port()?;
            let running_container = common::start_postgres_docker_container(port).await?;

            let db_conn = initialize_postgres(port).await?;

            // Create unique temp directory for this test
            let temp_dir = tempfile::tempdir()?;
            let metadata_dir = temp_dir.path().join("pepper_metadata");
            std::fs::create_dir_all(&metadata_dir)?;

            let mut params = HashMap::new();
            params.insert(
                "pepper_metadata_dir".to_string(),
                metadata_dir.to_str().expect("valid UTF-8 path").to_string(),
            );

            let mut acceleration_config =
                get_acceleration_config_append("pepper", Some(Params::from_string_map(params)));
            acceleration_config.mode = Mode::File;
            // Pepper append mode requires either primary_key or time_column
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
async fn test_acceleration_refresh_pepper_full() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let port: usize = get_random_port()?;
            let running_container = common::start_postgres_docker_container(port).await?;

            let db_conn = initialize_postgres(port).await?;

            // Create unique temp directory for this test
            let temp_dir = tempfile::tempdir()?;
            let metadata_dir = temp_dir.path().join("pepper_metadata");
            std::fs::create_dir_all(&metadata_dir)?;

            let mut params = HashMap::new();
            params.insert(
                "pepper_metadata_dir".to_string(),
                metadata_dir.to_str().expect("valid UTF-8 path").to_string(),
            );

            let mut acceleration_config =
                get_acceleration_config_full("pepper", Some(Params::from_string_map(params)));
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

#[tokio::test]
async fn test_pepper_append_mode_requires_constraint() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let port: usize = get_random_port()?;
            let running_container = common::start_postgres_docker_container(port).await?;

            let _db_conn = initialize_postgres(port).await?;

            // Create unique temp directory for this test
            let temp_dir = tempfile::tempdir()?;
            let metadata_dir = temp_dir.path().join("pepper_metadata");
            std::fs::create_dir_all(&metadata_dir)?;

            let mut params = HashMap::new();
            params.insert(
                "pepper_metadata_dir".to_string(),
                metadata_dir.to_str().expect("valid UTF-8 path").to_string(),
            );

            let mut acceleration_config =
                get_acceleration_config_append("pepper", Some(Params::from_string_map(params)));
            acceleration_config.mode = Mode::File;

            // Remove both primary_key and time_column - this should cause an error
            acceleration_config.primary_key = None;

            // Attempt to start runtime - should fail with validation error
            let result = start_test_runtime_no_time_column(port, acceleration_config).await;

            // Verify that the runtime fails to start with appropriate error
            assert!(
                result.is_err(),
                "Expected error when neither primary_key nor time_column is specified for append mode"
            );

            let err_msg = result.expect_err("Expected error").to_string();
            assert!(
                err_msg.contains("primary_key") || err_msg.contains("time_column"),
                "Error message should mention primary_key or time_column requirement, got: {err_msg}"
            );
            tracing::info!("✓ Validation correctly rejects append mode without constraints");

            running_container.remove().await?;
            Ok(())
        })
        .await
}
