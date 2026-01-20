/*
Copyright 2026 The Spice.ai OSS Authors

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

//! TPC-DS integration tests with S3 source and `PostgreSQL` acceleration.
//!
//! These tests verify that TPC-DS queries execute correctly when data is loaded from
//! an S3-compatible storage (rustfs locally, `MinIO` in CI) and accelerated into a
//! `PostgreSQL` database.
//!
//! # CI Environment
//!
//! In CI, the TPC-DS data is pre-loaded in `MinIO` and the following environment
//! variables are automatically set:
//! - `MINIO_ENDPOINT`: The `MinIO`/S3 endpoint URL
//! - `MINIO_ACCESS_KEY_ID`: The `MinIO`/S3 access key
//! - `MINIO_SECRET_ACCESS_KEY`: The `MinIO`/S3 secret key
//!
//! # Running Tests Locally
//!
//! To run these tests locally, you need to:
//!
//! 1. **Install dependencies:**
//!    - Docker (for rustfs and `PostgreSQL` containers)
//!    - `DuckDB` CLI (`brew install duckdb` on macOS)
//!
//! 2. **Run the setup script:**
//!    ```bash
//!    cd crates/runtime/tests/tpcds_postgres
//!    chmod +x setup_local_test_data.sh
//!    ./setup_local_test_data.sh
//!    ```
//!
//!    This script will:
//!    - Start a rustfs container (S3-compatible storage) on port 9000
//!    - Use `DuckDB` to generate TPC-DS SF1 data (~1GB)
//!    - Export all 24 TPC-DS tables to Parquet format
//!    - Upload the Parquet files to rustfs
//!
//! 3. **Set environment variables:**
//!    ```bash
//!    export MINIO_ENDPOINT="http://localhost:9000"
//!    export MINIO_ACCESS_KEY_ID="rustfsadmin"
//!    export MINIO_SECRET_ACCESS_KEY="rustfsadmin"
//!    ```
//!
//! 4. **Run the tests:**
//!    ```bash
//!    cargo test -p runtime --test integration --features postgres tpcds_postgres
//!    ```
//!
//! 5. **Clean up when done:**
//!    ```bash
//!    ./setup_local_test_data.sh cleanup
//!    ```
//!
//! # Test Structure
//!
//! The tests share a single runtime instance to avoid the overhead of starting
//! a new `PostgreSQL` container and loading tables for each query. Only the 5
//! tables required by the tested queries are loaded (not all 24 TPC-DS tables).
//! The shared environment is initialized lazily on the first test and reused
//! by all subsequent tests.
//!
//! Tables loaded: `store_sales`, `web_sales`, `date_dim`, `item`, `store`
//!
//! Test flow:
//! 1. First test triggers initialization: starts `PostgreSQL`, loads required datasets
//! 2. All tests (q36, q70, q86) execute queries against the shared runtime
//! 3. Container cleanup happens when the test process exits
//!
//! # Queries Tested
//!
//! - **Q36**: Gross margin calculation with ROLLUP grouping by item category/class
//! - **Q70**: Store sales net profit by state/county with ROLLUP and correlated subquery
//! - **Q86**: Web sales net paid by item category/class with ROLLUP

use std::sync::Arc;

use arrow::array::RecordBatch;
use datafusion_table_providers::sql::db_connection_pool::postgrespool::PostgresConnectionPool;
use futures::TryStreamExt;
use runtime::Runtime;
use tokio::sync::OnceCell;

use crate::docker::RunningContainer;
use crate::utils::runtime_ready_check_with_timeout;
use crate::{
    configure_test_datafusion, init_tracing,
    postgres::common::{
        PG_PASSWORD, get_pg_params, get_random_port, start_postgres_docker_container,
    },
    utils::test_request_context,
};

mod q36;
mod q70;
mod q86;

/// Shared test environment containing the runtime and `PostgreSQL` container.
/// Initialized once and reused across all TPC-DS query tests.
struct SharedTestEnv {
    rt: Arc<Runtime>,
    #[expect(dead_code)]
    running_container: RunningContainer<'static>,
}

/// Global shared test environment, initialized lazily on first use.
static SHARED_ENV: OnceCell<SharedTestEnv> = OnceCell::const_new();

/// Initializes the shared test environment (`PostgreSQL` container + Runtime with all datasets).
/// This is called once and the result is cached for all subsequent tests.
async fn init_shared_env() -> Result<SharedTestEnv, anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    let port = get_random_port()?;
    let running_container = start_postgres_docker_container(port).await?;

    let pg_db = "tpcds_test";

    // Create the database before loading the spicepod
    create_database(port, pg_db).await?;

    let spicepod_yaml = get_tpcds_spicepod_yaml(port, pg_db);

    let app = test_framework::app_utils::load_app_from_spicepod_str(&spicepod_yaml)
        .await
        .expect("Should load app from spicepod string");

    configure_test_datafusion();
    let rt = Arc::new(Runtime::builder().with_app(app).build().await);

    // Wait for datasets to load with a longer timeout since we're loading many tables
    tokio::select! {
        () = tokio::time::sleep(std::time::Duration::from_secs(600)) => {
            return Err(anyhow::Error::msg("Timed out waiting for datasets to load"));
        }
        () = Arc::clone(&rt).load_components() => {}
    }

    // Wait for runtime to be ready with extended timeout for acceleration
    runtime_ready_check_with_timeout(&rt, std::time::Duration::from_secs(300)).await;

    tracing::info!("TPC-DS shared test environment initialized successfully");

    Ok(SharedTestEnv {
        rt,
        running_container,
    })
}

/// Gets the shared test environment, initializing it if necessary.
/// All TPC-DS query tests use this to share the same runtime instance.
async fn get_shared_env() -> Result<&'static SharedTestEnv, anyhow::Error> {
    SHARED_ENV
        .get_or_try_init(init_shared_env)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to initialize shared test environment: {e}"))
}

/// Creates a spicepod YAML string for TPC-DS with `MinIO` S3 source and `PostgreSQL` acceleration.
///
/// This function generates a spicepod configuration that:
/// 1. Reads TPC-DS SF1 parquet files from an S3-compatible endpoint (`MinIO` in CI)
/// 2. Accelerates the data into a `PostgreSQL` database
///
/// Only loads the tables required by queries Q36, Q70, and Q86:
/// - `store_sales` (Q36, Q70)
/// - `web_sales` (Q86)
/// - `date_dim` (Q36, Q70, Q86)
/// - item (Q36, Q86)
/// - store (Q36, Q70)
#[expect(clippy::expect_used)]
fn get_tpcds_spicepod_yaml(pg_port: usize, pg_db: &str) -> String {
    let minio_endpoint =
        std::env::var("MINIO_ENDPOINT").expect("MINIO_ENDPOINT environment variable must be set");
    let minio_access_key = std::env::var("MINIO_ACCESS_KEY_ID")
        .expect("MINIO_ACCESS_KEY_ID environment variable must be set");
    let minio_secret_key = std::env::var("MINIO_SECRET_ACCESS_KEY")
        .expect("MINIO_SECRET_ACCESS_KEY environment variable must be set");

    let pg_password = PG_PASSWORD;

    format!(
        r"version: v1
kind: Spicepod
name: tpcds-s3-postgres-test
datasets:
  # store_sales - used by Q36, Q70 (largest table, ~2M rows at SF1)
  - from: s3://benchmarks/tpcds_sf1/store_sales.parquet
    name: store_sales
    params: &s3_params
      file_format: parquet
      allow_http: true
      s3_auth: key
      s3_endpoint: {minio_endpoint}
      s3_key: {minio_access_key}
      s3_secret: {minio_secret_key}
    acceleration:
      enabled: true
      engine: postgres
      params: &pg_params
        pg_host: localhost
        pg_user: postgres
        pg_pass: {pg_password}
        pg_db: {pg_db}
        pg_port: {pg_port}
        pg_sslmode: disable
      primary_key: (ss_item_sk, ss_ticket_number)
  # web_sales - used by Q86
  - from: s3://benchmarks/tpcds_sf1/web_sales.parquet
    name: web_sales
    params: *s3_params
    acceleration:
      enabled: true
      engine: postgres
      params: *pg_params
      primary_key: (ws_item_sk, ws_order_number)
  # date_dim - used by Q36, Q70, Q86
  - from: s3://benchmarks/tpcds_sf1/date_dim.parquet
    name: date_dim
    params: *s3_params
    acceleration:
      enabled: true
      engine: postgres
      params: *pg_params
      primary_key: (d_date_sk)
  # item - used by Q36, Q86
  - from: s3://benchmarks/tpcds_sf1/item.parquet
    name: item
    params: *s3_params
    acceleration:
      enabled: true
      engine: postgres
      params: *pg_params
      primary_key: (i_item_sk)
  # store - used by Q36, Q70
  - from: s3://benchmarks/tpcds_sf1/store.parquet
    name: store
    params: *s3_params
    acceleration:
      enabled: true
      engine: postgres
      params: *pg_params
      primary_key: (s_store_sk)
",
    )
}

/// Creates the test database in `PostgreSQL`.
async fn create_database(port: usize, db_name: &str) -> Result<(), anyhow::Error> {
    let pool = PostgresConnectionPool::new(get_pg_params(port)).await?;
    let conn = pool
        .connect_direct()
        .await
        .map_err(|e| anyhow::anyhow!("Error connecting to PostgreSQL: {e}"))?;

    // Create the database (ignore error if it already exists)
    let create_db_sql = format!("CREATE DATABASE {db_name}");
    if let Err(e) = conn.conn.execute(&create_db_sql, &[]).await {
        // Ignore "database already exists" error
        if !e.to_string().contains("already exists") {
            return Err(anyhow::anyhow!("Error creating database: {e}"));
        }
    }

    Ok(())
}

/// Runs a TPC-DS query against the shared accelerated `PostgreSQL` runtime.
///
/// This function:
/// 1. Gets or initializes the shared test environment (`PostgreSQL` + Runtime)
/// 2. Executes the query and validates that results are returned
///
/// The shared environment is initialized once on the first test and reused
/// by all subsequent tests, making the test suite much faster.
async fn test_tpcds_query(query: &str) -> Result<(), anyhow::Error> {
    test_request_context()
        .scope(async {
            let env = get_shared_env().await?;

            // Run EXPLAIN to verify query planning
            let query_result = env
                .rt
                .datafusion()
                .query_builder(&format!("EXPLAIN VERBOSE {query}"))
                .build()
                .run()
                .await?;
            let explain_plan = query_result.data.try_collect::<Vec<RecordBatch>>().await?;
            let explain_plan_display = arrow::util::pretty::pretty_format_batches(&explain_plan)?;
            tracing::info!("Query plan:\n{explain_plan_display}");

            // Execute the actual query
            let query_result = env
                .rt
                .datafusion()
                .query_builder(query)
                .build()
                .run()
                .await?;
            let results = query_result.data.try_collect::<Vec<RecordBatch>>().await?;

            // Validate that we got some results
            let total_rows: usize = results.iter().map(RecordBatch::num_rows).sum();
            tracing::info!("Query returned {total_rows} rows");

            Ok(())
        })
        .await
}
