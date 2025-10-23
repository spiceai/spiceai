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
    initialize_postgres, initialize_postgres_vortex_workaround, refresh_table, start_test_runtime,
    start_test_runtime_no_time_column,
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
            // Pepper append mode supports primary_key, time_column, or neither (for duplicate appends), but not both
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

/// Test Pepper append mode WITHOUT constraints (no `primary_key`, no `time_column`)
/// Workaround for Vortex v0.52.1 timestamp metadata bug - uses INT column instead of TIMESTAMP
/// This tests the new feature: append mode with no constraints appends all data including duplicates
///
/// IGNORED: Blocked by Vortex v0.52.1 bugs:
/// 1. Timestamp `ExtMetadata` encoding mismatch (workaround: use INT columns)
/// 2. Schema nullability mismatch - Arrow infers nullable but Vortex expects non-nullable
///    even for NOT NULL columns from `PostgreSQL`
#[tokio::test]
#[ignore = "Blocked by Vortex v0.52.1 schema nullability mismatch bug"]
async fn test_acceleration_refresh_pepper_append_no_constraints_vortex_workaround()
-> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let port: usize = get_random_port()?;
            let running_container = common::start_postgres_docker_container(port).await?;

            // Use INT column workaround for Vortex v0.52.1 timestamp metadata bug
            let db_conn = initialize_postgres_vortex_workaround(port).await?;

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
            // NO primary_key AND NO time_column - tests the new no-constraints feature
            acceleration_config.primary_key = None;
            let rt = start_test_runtime_no_time_column(port, acceleration_config).await?;

            let results = execute_rt_sql(Arc::clone(&rt), "SELECT * from test_table").await?;
            assert_eq!(
                results
                    .iter()
                    .map(arrow::array::RecordBatch::num_rows)
                    .sum::<usize>(),
                1
            );

            // Insert duplicate row (same data)
            execute_ps_sql(
                &db_conn,
                "INSERT INTO test_table (id, created_at) VALUES (2, 1);",
            )
            .await?;

            refresh_table(Arc::clone(&rt), "test_table").await?;

            // Should have 2 rows now (including duplicate)
            let results = execute_rt_sql(Arc::clone(&rt), "SELECT * from test_table").await?;
            assert_eq!(
                results
                    .iter()
                    .map(arrow::array::RecordBatch::num_rows)
                    .sum::<usize>(),
                2
            );

            // Refresh again without new data - should still have 2 rows
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
