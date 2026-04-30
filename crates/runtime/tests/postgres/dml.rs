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

//! Integration tests for DELETE and UPDATE DML operations through the Spice runtime
//! with a Postgres source.
//!
//! Tests cover two access patterns:
//!
//! - **Write-through (no acceleration)**: The dataset is backed directly by Postgres
//!   with `access: ReadWrite`. DML goes straight to Postgres.
//!
//! - **Write-through with `DuckDB` acceleration**: The dataset has a `DuckDB` local
//!   accelerator and `write_mode: WriteThrough`. For non-Cayenne engines this falls
//!   through to `FederatedOnly` (DML to Postgres only); an explicit refresh re-syncs
//!   the `DuckDB` accelerator afterward.
//!
//! The key regression being tested: `DataFusion`'s `PushDownFilter` optimizer moves
//! `Filter` nodes into `TableScan.filters`. The Spice runtime's
//! `CacheInvalidationExtensionPlanner` must extract those filters correctly so that
//! DELETE/UPDATE WHERE clauses are not silently dropped.

use std::{collections::HashMap, sync::Arc, time::Duration};

use app::AppBuilder;
use datafusion::{assert_batches_eq, common::TableReference};
use datafusion_table_providers::sql::db_connection_pool::dbconnection::DbConnection;
use futures::TryStreamExt;
use secrecy::ExposeSecret;
use spicepod::{
    acceleration::{Acceleration, RefreshMode, WriteMode},
    component::{access::AccessMode, dataset::Dataset},
    param::Params,
};

use crate::{
    configure_test_datafusion, init_tracing,
    postgres::common::{self, get_pg_params},
    utils::{register_test_connectors, run_query, runtime_ready_check, test_request_context},
};

// ── helpers ──────────────────────────────────────────────────────────────────

/// Build a Postgres-backed dataset with `access: ReadWrite` (no acceleration).
fn write_through_dataset(port: usize, table: &str, name: &str) -> Dataset {
    let mut ds = Dataset::new(format!("postgres:{table}"), name.to_string());
    ds.access = AccessMode::ReadWrite;
    ds.params = Some(Params::from_string_map(
        get_pg_params(port)
            .into_iter()
            .map(|(k, v)| (k, v.expose_secret().to_string()))
            .collect::<HashMap<String, String>>(),
    ));
    ds
}

/// Build a Postgres-backed dataset with `DuckDB` acceleration, write-through mode,
/// and upsert on the `id` primary key.
#[cfg(feature = "duckdb")]
fn write_through_duckdb_dataset(port: usize, table: &str, name: &str) -> Dataset {
    let mut ds = write_through_dataset(port, table, name);
    ds.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some("duckdb".to_string()),
        refresh_mode: Some(RefreshMode::Full),
        write_mode: WriteMode::WriteThrough,
        ..Acceleration::default()
    });
    ds
}

/// Start the runtime with a single dataset and wait until it is ready.
async fn start_runtime(dataset: Dataset) -> Result<Arc<runtime::Runtime>, anyhow::Error> {
    register_test_connectors().await;
    let app = AppBuilder::new("postgres_dml_test")
        .with_dataset(dataset)
        .build();

    configure_test_datafusion();
    let rt = Arc::new(runtime::Runtime::builder().with_app(app).build().await);

    tokio::select! {
        () = tokio::time::sleep(Duration::from_secs(120)) => {
            return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
        }
        () = Arc::clone(&rt).load_components() => {}
    }

    runtime_ready_check(&rt).await;
    Ok(rt)
}

/// Seed three rows into the named Postgres table.
async fn seed_items(
    pool: &datafusion_table_providers::sql::db_connection_pool::postgrespool::PostgresConnectionPool,
    table: &str,
) -> Result<(), anyhow::Error> {
    let conn = pool
        .connect_direct()
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    conn.conn
        .execute(
            &format!(
                "CREATE TABLE {table} (id INT PRIMARY KEY, name TEXT NOT NULL, value INT NOT NULL)"
            ),
            &[],
        )
        .await?;
    conn.conn
        .execute(
            &format!(
                "INSERT INTO {table} VALUES (1, 'alpha', 10), (2, 'beta', 20), (3, 'gamma', 30)"
            ),
            &[],
        )
        .await?;
    Ok(())
}

/// Trigger a full refresh of the named dataset and wait for it to complete.
async fn refresh_dataset(rt: &runtime::Runtime, name: &str) -> Result<(), anyhow::Error> {
    let notifier = rt
        .datafusion()
        .refresh_table(&TableReference::from(name), None)
        .await?;
    notifier
        .ok_or_else(|| anyhow::anyhow!("no completion notifier for {name}"))?
        .notified()
        .await;
    Ok(())
}

/// Query rows directly from Postgres to verify the source state.
async fn pg_rows(
    pool: &datafusion_table_providers::sql::db_connection_pool::postgrespool::PostgresConnectionPool,
    table: &str,
) -> Result<Vec<arrow::array::RecordBatch>, anyhow::Error> {
    let conn = pool
        .connect_direct()
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    let async_conn = conn
        .as_async()
        .ok_or_else(|| anyhow::anyhow!("no async conn"))?;
    let batches = async_conn
        .query_arrow(
            &format!("SELECT id, name, value FROM {table} ORDER BY id"),
            &[],
            None,
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?
        .try_collect::<Vec<_>>()
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(batches)
}

// ── write-through (no acceleration) tests ────────────────────────────────────

/// DELETE with a WHERE clause must remove only the matching row, not all rows.
///
/// Regression: `DataFusion`'s `PushDownFilter` moved the WHERE predicate into
/// TableScan.filters. `extract_dml_filters` only checked Filter nodes, so the
/// WHERE clause was silently dropped and all rows were deleted.
#[tokio::test]
async fn test_postgres_write_through_delete_with_where() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let port = common::get_random_port()?;
            let _container = common::start_postgres_docker_container(port).await?;
            let pool = common::get_postgres_connection_pool(port, None).await?;
            seed_items(&pool, "items").await?;

            let rt = start_runtime(write_through_dataset(port, "items", "items")).await?;

            // Delete only id=2
            let result = run_query(&rt, "DELETE FROM items WHERE id = 2").await?;
            assert_batches_eq!(
                &[
                    "+-------+",
                    "| count |",
                    "+-------+",
                    "| 1     |",
                    "+-------+",
                ],
                &result
            );

            // Verify via Spice
            let rows = run_query(&rt, "SELECT id, name, value FROM items ORDER BY id").await?;
            assert_batches_eq!(
                &[
                    "+----+-------+-------+",
                    "| id | name  | value |",
                    "+----+-------+-------+",
                    "| 1  | alpha | 10    |",
                    "| 3  | gamma | 30    |",
                    "+----+-------+-------+",
                ],
                &rows
            );

            // Verify directly in Postgres
            let pg_result = pg_rows(&pool, "items").await?;
            assert_batches_eq!(
                &[
                    "+----+-------+-------+",
                    "| id | name  | value |",
                    "+----+-------+-------+",
                    "| 1  | alpha | 10    |",
                    "| 3  | gamma | 30    |",
                    "+----+-------+-------+",
                ],
                &pg_result
            );

            Ok(())
        })
        .await
}

/// UPDATE with a WHERE clause must modify only the matching row.
///
/// Same regression as DELETE: the WHERE predicate in TableScan.filters was
/// ignored, causing all rows to be updated.
#[tokio::test]
async fn test_postgres_write_through_update_with_where() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let port = common::get_random_port()?;
            let _container = common::start_postgres_docker_container(port).await?;
            let pool = common::get_postgres_connection_pool(port, None).await?;
            seed_items(&pool, "items").await?;

            let rt = start_runtime(write_through_dataset(port, "items", "items")).await?;

            // Update only id=1
            let result = run_query(
                &rt,
                "UPDATE items SET name = 'updated', value = 99 WHERE id = 1",
            )
            .await?;
            assert_batches_eq!(
                &[
                    "+-------+",
                    "| count |",
                    "+-------+",
                    "| 1     |",
                    "+-------+",
                ],
                &result
            );

            // Verify via Spice
            let rows = run_query(&rt, "SELECT id, name, value FROM items ORDER BY id").await?;
            assert_batches_eq!(
                &[
                    "+----+---------+-------+",
                    "| id | name    | value |",
                    "+----+---------+-------+",
                    "| 1  | updated | 99    |",
                    "| 2  | beta    | 20    |",
                    "| 3  | gamma   | 30    |",
                    "+----+---------+-------+",
                ],
                &rows
            );

            // Verify directly in Postgres
            let pg_result = pg_rows(&pool, "items").await?;
            assert_batches_eq!(
                &[
                    "+----+---------+-------+",
                    "| id | name    | value |",
                    "+----+---------+-------+",
                    "| 1  | updated | 99    |",
                    "| 2  | beta    | 20    |",
                    "| 3  | gamma   | 30    |",
                    "+----+---------+-------+",
                ],
                &pg_result
            );

            Ok(())
        })
        .await
}

/// DELETE without a WHERE clause must delete all rows.
#[tokio::test]
async fn test_postgres_write_through_delete_all() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let port = common::get_random_port()?;
            let _container = common::start_postgres_docker_container(port).await?;
            let pool = common::get_postgres_connection_pool(port, None).await?;
            seed_items(&pool, "items").await?;

            let rt = start_runtime(write_through_dataset(port, "items", "items")).await?;

            let result = run_query(&rt, "DELETE FROM items").await?;
            assert_batches_eq!(
                &[
                    "+-------+",
                    "| count |",
                    "+-------+",
                    "| 3     |",
                    "+-------+",
                ],
                &result
            );

            let rows = run_query(&rt, "SELECT id FROM items ORDER BY id").await?;
            assert_batches_eq!(&["++", "++"], &rows);

            Ok(())
        })
        .await
}

// ── DuckDB-accelerated write-through tests ────────────────────────────────────

/// DELETE with WHERE on a DuckDB-accelerated dataset with write-through mode.
///
/// For `DuckDB` + Full refresh, `WriteThrough` falls through to `FederatedOnly`:
/// the DELETE is routed to Postgres only. A manual refresh is triggered to
/// re-sync the `DuckDB` accelerator from the updated Postgres source, after
/// which queries through the runtime read the correct (post-delete) data.
#[cfg(feature = "duckdb")]
#[tokio::test]
async fn test_postgres_duckdb_accel_delete_with_where() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let port = common::get_random_port()?;
            let _container = common::start_postgres_docker_container(port).await?;
            let pool = common::get_postgres_connection_pool(port, None).await?;
            seed_items(&pool, "items").await?;

            let rt = start_runtime(write_through_duckdb_dataset(port, "items", "items")).await?;

            // Delete only id=3
            let result = run_query(&rt, "DELETE FROM items WHERE id = 3").await?;
            assert_batches_eq!(
                &[
                    "+-------+",
                    "| count |",
                    "+-------+",
                    "| 1     |",
                    "+-------+",
                ],
                &result
            );

            // Postgres source reflects the deletion
            let pg_result = pg_rows(&pool, "items").await?;
            assert_batches_eq!(
                &[
                    "+----+-------+-------+",
                    "| id | name  | value |",
                    "+----+-------+-------+",
                    "| 1  | alpha | 10    |",
                    "| 2  | beta  | 20    |",
                    "+----+-------+-------+",
                ],
                &pg_result
            );

            // Re-sync the DuckDB accelerator from the updated Postgres source
            refresh_dataset(&rt, "items").await?;

            // Accelerator reflects the deletion
            let rows = run_query(&rt, "SELECT id, name, value FROM items ORDER BY id").await?;
            assert_batches_eq!(
                &[
                    "+----+-------+-------+",
                    "| id | name  | value |",
                    "+----+-------+-------+",
                    "| 1  | alpha | 10    |",
                    "| 2  | beta  | 20    |",
                    "+----+-------+-------+",
                ],
                &rows
            );

            Ok(())
        })
        .await
}

/// UPDATE with WHERE on a DuckDB-accelerated dataset with write-through mode.
///
/// Same `FederatedOnly` routing as the delete test: UPDATE goes to Postgres, then
/// an explicit refresh re-syncs `DuckDB`.
#[cfg(feature = "duckdb")]
#[tokio::test]
async fn test_postgres_duckdb_accel_update_with_where() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let port = common::get_random_port()?;
            let _container = common::start_postgres_docker_container(port).await?;
            let pool = common::get_postgres_connection_pool(port, None).await?;
            seed_items(&pool, "items").await?;

            let rt = start_runtime(write_through_duckdb_dataset(port, "items", "items")).await?;

            // Update only id=2
            let result = run_query(
                &rt,
                "UPDATE items SET name = 'modified', value = 42 WHERE id = 2",
            )
            .await?;
            assert_batches_eq!(
                &[
                    "+-------+",
                    "| count |",
                    "+-------+",
                    "| 1     |",
                    "+-------+",
                ],
                &result
            );

            // Postgres source reflects the update
            let pg_result = pg_rows(&pool, "items").await?;
            assert_batches_eq!(
                &[
                    "+----+----------+-------+",
                    "| id | name     | value |",
                    "+----+----------+-------+",
                    "| 1  | alpha    | 10    |",
                    "| 2  | modified | 42    |",
                    "| 3  | gamma    | 30    |",
                    "+----+----------+-------+",
                ],
                &pg_result
            );

            // Re-sync the DuckDB accelerator from the updated Postgres source
            refresh_dataset(&rt, "items").await?;

            // Accelerator reflects the update
            let rows = run_query(&rt, "SELECT id, name, value FROM items ORDER BY id").await?;
            assert_batches_eq!(
                &[
                    "+----+----------+-------+",
                    "| id | name     | value |",
                    "+----+----------+-------+",
                    "| 1  | alpha    | 10    |",
                    "| 2  | modified | 42    |",
                    "| 3  | gamma    | 30    |",
                    "+----+----------+-------+",
                ],
                &rows
            );

            Ok(())
        })
        .await
}

/// UPDATE without a WHERE clause must update all rows.
///
/// Same `FederatedOnly` routing: UPDATE goes to Postgres, explicit refresh
/// re-syncs `DuckDB`.
#[cfg(feature = "duckdb")]
#[tokio::test]
async fn test_postgres_duckdb_accel_update_all() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let port = common::get_random_port()?;
            let _container = common::start_postgres_docker_container(port).await?;
            let pool = common::get_postgres_connection_pool(port, None).await?;
            seed_items(&pool, "items").await?;

            let rt = start_runtime(write_through_duckdb_dataset(port, "items", "items")).await?;

            let result = run_query(&rt, "UPDATE items SET value = 0").await?;
            assert_batches_eq!(
                &[
                    "+-------+",
                    "| count |",
                    "+-------+",
                    "| 3     |",
                    "+-------+",
                ],
                &result
            );

            let pg_result = pg_rows(&pool, "items").await?;
            assert_batches_eq!(
                &[
                    "+----+-------+-------+",
                    "| id | name  | value |",
                    "+----+-------+-------+",
                    "| 1  | alpha | 0     |",
                    "| 2  | beta  | 0     |",
                    "| 3  | gamma | 0     |",
                    "+----+-------+-------+",
                ],
                &pg_result
            );

            // Re-sync the DuckDB accelerator from the updated Postgres source
            refresh_dataset(&rt, "items").await?;

            // Accelerator reflects the bulk update
            let rows = run_query(&rt, "SELECT id, name, value FROM items ORDER BY id").await?;
            assert_batches_eq!(
                &[
                    "+----+-------+-------+",
                    "| id | name  | value |",
                    "+----+-------+-------+",
                    "| 1  | alpha | 0     |",
                    "| 2  | beta  | 0     |",
                    "| 3  | gamma | 0     |",
                    "+----+-------+-------+",
                ],
                &rows
            );

            Ok(())
        })
        .await
}
