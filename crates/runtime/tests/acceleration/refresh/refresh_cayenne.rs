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
    get_dataset_no_time_column, initialize_postgres, refresh_table, start_test_runtime,
};
use crate::postgres::common;
use crate::postgres::common::get_random_port;
use crate::{
    configure_test_datafusion, configure_test_datafusion_request_context, init_tracing,
    utils::test_request_context,
};
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
