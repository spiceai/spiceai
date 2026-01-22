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

//! Parameterized tests for mode: append and mode: full refresh across all supported accelerators.
//!
//! This test suite validates that all accelerator engines correctly handle both append and full
//! refresh modes using a helper function with variants for each engine.

use crate::acceleration::refresh::common::{
    execute_ps_sql, execute_rt_sql, get_acceleration_config_append, get_acceleration_config_full,
    initialize_postgres, refresh_table, start_test_runtime,
};
use crate::postgres::common;
use crate::postgres::common::get_random_port;
use crate::{init_tracing, utils::test_request_context};
use spicepod::param::Params;
use std::sync::Arc;

/// Get acceleration parameters for postgres engine
fn get_postgres_acceleration_params(port: usize) -> Params {
    let mut params = common::get_pg_params(port);
    // Override pg_db to use the acceleration database instead of the default
    params.insert(
        "pg_db".to_string(),
        secrecy::SecretString::from("acceleration".to_string()),
    );

    Params::from_string_map(
        params
            .into_iter()
            .map(|(k, v)| (k, secrecy::ExposeSecret::expose_secret(&v).to_string()))
            .collect(),
    )
}

/// Helper function to test append mode for a given engine
async fn test_refresh_append_for_engine(engine: &str) -> Result<(), anyhow::Error> {
    test_request_context()
        .scope(async {
            let port: usize = get_random_port()?;
            let running_container = common::start_postgres_docker_container(port).await?;

            let db_conn = initialize_postgres(port).await?;

            // Postgres acceleration requires connection parameters
            let acceleration_params = if engine == "postgres" {
                Some(get_postgres_acceleration_params(port))
            } else {
                None
            };

            let acceleration_config = get_acceleration_config_append(engine, acceleration_params);
            let rt = start_test_runtime(port, acceleration_config).await?;

            // Initial state: 1 row
            let results = execute_rt_sql(Arc::clone(&rt), "SELECT * FROM test_table").await?;
            let initial_count: usize = results
                .iter()
                .map(arrow::array::RecordBatch::num_rows)
                .sum();
            assert_eq!(
                initial_count, 1,
                "{engine} append mode: Expected 1 row initially"
            );

            // Insert new row in source
            execute_ps_sql(
                &db_conn,
                "INSERT INTO test_table (created_at) VALUES (now());",
            )
            .await?;

            // Refresh should append the new row
            refresh_table(Arc::clone(&rt), "test_table").await?;

            // After refresh: 2 rows (append mode keeps old + adds new)
            let results = execute_rt_sql(Arc::clone(&rt), "SELECT * FROM test_table").await?;
            let final_count: usize = results
                .iter()
                .map(arrow::array::RecordBatch::num_rows)
                .sum();
            assert_eq!(
                final_count, 2,
                "{engine} append mode: Expected 2 rows after append refresh"
            );

            running_container.remove().await?;
            Ok(())
        })
        .await
}

/// Helper function to test full mode for a given engine
async fn test_refresh_full_for_engine(engine: &str) -> Result<(), anyhow::Error> {
    test_request_context()
        .scope(async {
            let port: usize = get_random_port()?;
            let running_container = common::start_postgres_docker_container(port).await?;

            let db_conn = initialize_postgres(port).await?;

            // Postgres acceleration requires connection parameters
            let acceleration_params = if engine == "postgres" {
                Some(get_postgres_acceleration_params(port))
            } else {
                None
            };

            let acceleration_config = get_acceleration_config_full(engine, acceleration_params);
            let rt = start_test_runtime(port, acceleration_config).await?;

            // Initial state: 1 row
            let results = execute_rt_sql(Arc::clone(&rt), "SELECT * FROM test_table").await?;
            let initial_count: usize = results
                .iter()
                .map(arrow::array::RecordBatch::num_rows)
                .sum();
            assert_eq!(
                initial_count, 1,
                "{engine} full mode: Expected 1 row initially"
            );

            // Insert new row in source
            execute_ps_sql(
                &db_conn,
                "INSERT INTO test_table (created_at) VALUES (now());",
            )
            .await?;

            // Refresh should replace all data with current source state
            refresh_table(Arc::clone(&rt), "test_table").await?;

            // After refresh: 2 rows (full mode replaces with current source)
            let results = execute_rt_sql(Arc::clone(&rt), "SELECT * FROM test_table").await?;
            let final_count: usize = results
                .iter()
                .map(arrow::array::RecordBatch::num_rows)
                .sum();
            assert_eq!(
                final_count, 2,
                "{engine} full mode: Expected 2 rows after full refresh"
            );

            running_container.remove().await?;
            Ok(())
        })
        .await
}

// ============================================================================
// Test variants for each accelerator engine
// ============================================================================

// Arrow (always available)
#[tokio::test]
async fn test_acceleration_refresh_arrow_append_variant() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    test_refresh_append_for_engine("arrow").await
}

#[tokio::test]
async fn test_acceleration_refresh_arrow_full_variant() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    test_refresh_full_for_engine("arrow").await
}

// DuckDB (feature-gated)
#[cfg(feature = "duckdb")]
#[tokio::test]
async fn test_acceleration_refresh_duckdb_append_variant() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    test_refresh_append_for_engine("duckdb").await
}

#[cfg(feature = "duckdb")]
#[tokio::test]
async fn test_acceleration_refresh_duckdb_full_variant() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    test_refresh_full_for_engine("duckdb").await
}

// SQLite (feature-gated)
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_acceleration_refresh_sqlite_append_variant() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    test_refresh_append_for_engine("sqlite").await
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_acceleration_refresh_sqlite_full_variant() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    test_refresh_full_for_engine("sqlite").await
}

// Postgres (feature-gated)
#[cfg(feature = "postgres-accel")]
#[tokio::test]
async fn test_acceleration_refresh_postgres_append_variant() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    test_refresh_append_for_engine("postgres").await
}

#[cfg(feature = "postgres-accel")]
#[tokio::test]
async fn test_acceleration_refresh_postgres_full_variant() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    test_refresh_full_for_engine("postgres").await
}

// Cayenne (not available on Windows)
// Note: Cayenne requires mode: file in addition to refresh_mode: append/full.
// These tests need custom helpers that set both mode and refresh_mode.
#[cfg(not(windows))]
#[tokio::test]
#[ignore = "Cayenne requires mode: file which is not set by the generic test helpers"]
async fn test_acceleration_refresh_cayenne_append_variant() {
    let _tracing = init_tracing(Some("integration=debug,info"));
    test_refresh_append_for_engine("cayenne")
        .await
        .expect("Test should pass when mode: file is properly configured");
}

#[cfg(not(windows))]
#[tokio::test]
#[ignore = "Cayenne requires mode: file which is not set by the generic test helpers"]
async fn test_acceleration_refresh_cayenne_full_variant() {
    let _tracing = init_tracing(Some("integration=debug,info"));
    test_refresh_full_for_engine("cayenne")
        .await
        .expect("Test should pass when mode: file is properly configured");
}
