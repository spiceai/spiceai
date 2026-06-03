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

//! Integration tests for Cayenne Catalog DDL operations.
//!
//! Validates that the Cayenne Catalog supports `DataFrame` DDL (CREATE TABLE via SQL),
//! and that INSERT, UPDATE (upsert), and DELETE operations produce correct results.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::{
    configure_test_datafusion, init_tracing,
    utils::{register_test_connectors, runtime_ready_check_with_timeout, test_request_context},
};
use app::AppBuilder;
use arrow::array::{Float64Array, Int64Array, RecordBatch};
use datafusion::assert_batches_eq;
use futures::TryStreamExt;
use runtime::Runtime;
use runtime::cluster::ResolvedClusterConfig;
use runtime::config::{ClusterConfig, ClusterRole, Config};
use spicepod::component::access::AccessMode;
use spicepod::component::catalog::Catalog;
use spicepod::param::Params;

/// Creates a [`ResolvedClusterConfig`] with Executor role for tests.
///
/// Cayenne catalogs require distributed mode. Using the Executor role enables
/// distributed-mode checks in the planner while avoiding scheduler-only
/// distributed DML rewrites in these tests; Cayenne DDL rewriting via
/// `CayenneDdlAnalyzerRule` is not gated on the cluster role.
fn test_cluster_config() -> ResolvedClusterConfig {
    ResolvedClusterConfig::try_new(ClusterConfig {
        role: Some(ClusterRole::Executor),
        allow_insecure_connections: true,
        node_advertise_address: Some("127.0.0.1".to_string()),
        ..Default::default()
    })
    .expect(
        "failed to build Cayenne catalog DDL test ResolvedClusterConfig; expected Executor role, \
         allow_insecure_connections = true, and node_advertise_address to be set",
    )
}

/// Helper to run a SQL query against the runtime and collect results.
async fn run_query(rt: &Runtime, sql: &str) -> Result<Vec<RecordBatch>, String> {
    let result = rt
        .datafusion()
        .query_builder(sql)
        .build()
        .run()
        .await
        .map_err(|e| format!("query '{sql}' failed: {e}"))?;

    result
        .data
        .try_collect::<Vec<RecordBatch>>()
        .await
        .map_err(|e| format!("collecting results for '{sql}' failed: {e}"))
}

/// Helper to run a SQL query and assert it succeeds (for DDL/DML where we don't check rows).
async fn exec(rt: &Runtime, sql: &str) -> Result<(), String> {
    run_query(rt, sql).await?;
    Ok(())
}

/// Helper to get a single i64 scalar from a `SELECT COUNT(*)` style query.
async fn query_scalar_i64(rt: &Runtime, sql: &str) -> Result<i64, String> {
    let batches = run_query(rt, sql).await?;
    let batch = batches
        .first()
        .ok_or_else(|| format!("no batches returned for '{sql}'"))?;
    let col = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| format!("expected Int64Array for '{sql}'"))?;
    Ok(col.value(0))
}

/// Creates a Cayenne `Catalog` component with `read_write_create` access, pointing
/// at the given temp directories.
fn make_cayenne_catalog(catalog_name: &str, data_dir: &str, metadata_dir: &str) -> Catalog {
    let mut catalog = Catalog::new("cayenne".to_string(), catalog_name.to_string())
        .with_access(AccessMode::ReadWriteCreate);
    catalog.params = Some(Params::from_string_map(
        vec![
            ("cayenne_data_dir".to_string(), data_dir.to_string()),
            ("cayenne_metadata_dir".to_string(), metadata_dir.to_string()),
        ]
        .into_iter()
        .collect::<HashMap<String, String>>(),
    ));
    catalog
}

// =============================================================================
// Test: Create table, insert, select, update (upsert), delete — full lifecycle
// =============================================================================

#[tokio::test]
#[cfg_attr(
    not(feature = "spicebench"),
    ignore = "requires the spicebench feature"
)]
async fn cayenne_catalog_ddl_create_insert_update_delete() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    let temp_dir = tempfile::tempdir().map_err(|e| e.to_string())?;
    let data_dir = temp_dir.path().join("data");
    let metadata_dir = temp_dir.path().join("metadata");

    test_request_context()
        .scope(async {
            let catalog = make_cayenne_catalog(
                "test_cat",
                &data_dir.to_string_lossy(),
                &metadata_dir.to_string_lossy(),
            );

            let app = AppBuilder::new("cayenne_ddl_lifecycle")
                .with_catalog(catalog)
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder()
                .with_app(app)
                .with_resolved_cluster_config(test_cluster_config())
                .with_runtime_config(Config::default().with_caching_disabled())
                .build()
                .await;
            let cloned_rt = Arc::new(rt.clone());

            // Load components (registers the Cayenne catalog with DDL support).
            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(30)) => {
                    return Err("Timeout waiting for components to load".to_string());
                }
                () = cloned_rt.load_components() => {}
            }
            runtime_ready_check_with_timeout(&rt, Duration::from_secs(30)).await;

            // -----------------------------------------------------------------
            // Step 1: CREATE SCHEMA + CREATE TABLE via SQL DDL
            // -----------------------------------------------------------------
            exec(&rt, "CREATE SCHEMA test_cat.myschema").await?;

            exec(
                &rt,
                "CREATE TABLE test_cat.myschema.users (
                    id BIGINT NOT NULL,
                    name VARCHAR NOT NULL,
                    email VARCHAR,
                    age BIGINT,
                    PRIMARY KEY (id)
                ) PARTITION BY id",
            )
            .await?;

            // Verify the table appears in information_schema.
            let batches = run_query(
                &rt,
                "SELECT table_catalog, table_schema, table_name
                 FROM information_schema.tables
                 WHERE table_catalog = 'test_cat' AND table_name = 'users'",
            )
            .await?;
            assert!(
                !batches.is_empty() && batches[0].num_rows() == 1,
                "Expected users table in information_schema, got {batches:?}"
            );

            // Verify table is empty.
            let count =
                query_scalar_i64(&rt, "SELECT COUNT(*) FROM test_cat.myschema.users").await?;
            assert_eq!(count, 0, "Table should be empty after creation");

            // -----------------------------------------------------------------
            // Step 2: INSERT rows
            // -----------------------------------------------------------------
            exec(
                &rt,
                "INSERT INTO test_cat.myschema.users VALUES
                    (1, 'Alice',   'alice@example.com',   30),
                    (2, 'Bob',     'bob@example.com',     25),
                    (3, 'Charlie', 'charlie@example.com', 35),
                    (4, 'Diana',   'diana@example.com',   28),
                    (5, 'Eve',     NULL,                  22)",
            )
            .await?;

            // Validate row count.
            let count =
                query_scalar_i64(&rt, "SELECT COUNT(*) FROM test_cat.myschema.users").await?;
            assert_eq!(count, 5, "Expected 5 rows after initial insert");

            // Validate specific rows.
            let batches = run_query(
                &rt,
                "SELECT id, name, email, age FROM test_cat.myschema.users ORDER BY id",
            )
            .await?;

            assert_batches_eq!(
                &[
                    "+----+---------+---------------------+-----+",
                    "| id | name    | email               | age |",
                    "+----+---------+---------------------+-----+",
                    "| 1  | Alice   | alice@example.com   | 30  |",
                    "| 2  | Bob     | bob@example.com     | 25  |",
                    "| 3  | Charlie | charlie@example.com | 35  |",
                    "| 4  | Diana   | diana@example.com   | 28  |",
                    "| 5  | Eve     |                     | 22  |",
                    "+----+---------+---------------------+-----+",
                ],
                &batches
            );

            // -----------------------------------------------------------------
            // Step 3: INSERT additional rows (second batch)
            // -----------------------------------------------------------------
            exec(
                &rt,
                "INSERT INTO test_cat.myschema.users VALUES
                    (6, 'Frank', 'frank@example.com', 40),
                    (7, 'Grace', 'grace@example.com', 33)",
            )
            .await?;

            let count =
                query_scalar_i64(&rt, "SELECT COUNT(*) FROM test_cat.myschema.users").await?;
            assert_eq!(count, 7, "Expected 7 rows after second insert");

            // -----------------------------------------------------------------
            // Step 4: DELETE specific rows
            // -----------------------------------------------------------------
            exec(&rt, "DELETE FROM test_cat.myschema.users WHERE id = 3").await?;

            let count =
                query_scalar_i64(&rt, "SELECT COUNT(*) FROM test_cat.myschema.users").await?;
            assert_eq!(count, 6, "Expected 6 rows after deleting id=3");

            // Verify the deleted row is gone.
            let batches =
                run_query(&rt, "SELECT id FROM test_cat.myschema.users WHERE id = 3").await?;
            let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
            assert_eq!(total_rows, 0, "id=3 should no longer exist");

            // -----------------------------------------------------------------
            // Step 5: DELETE multiple rows with a range filter
            // -----------------------------------------------------------------
            exec(&rt, "DELETE FROM test_cat.myschema.users WHERE age < 26").await?;

            // Bob (25) and Eve (22) should be deleted.
            let count =
                query_scalar_i64(&rt, "SELECT COUNT(*) FROM test_cat.myschema.users").await?;
            assert_eq!(count, 4, "Expected 4 rows after deleting age < 26");

            let batches = run_query(
                &rt,
                "SELECT id, name FROM test_cat.myschema.users ORDER BY id",
            )
            .await?;

            assert_batches_eq!(
                &[
                    "+----+-------+",
                    "| id | name  |",
                    "+----+-------+",
                    "| 1  | Alice |",
                    "| 4  | Diana |",
                    "| 6  | Frank |",
                    "| 7  | Grace |",
                    "+----+-------+",
                ],
                &batches
            );

            // -----------------------------------------------------------------
            // Step 6: INSERT after deletes — verify correctness
            // -----------------------------------------------------------------
            exec(
                &rt,
                "INSERT INTO test_cat.myschema.users VALUES
                    (8, 'Heidi', 'heidi@example.com', 29)",
            )
            .await?;

            let count =
                query_scalar_i64(&rt, "SELECT COUNT(*) FROM test_cat.myschema.users").await?;
            assert_eq!(count, 5, "Expected 5 rows after inserting Heidi");

            // -----------------------------------------------------------------
            // Step 7: Aggregation queries — validate computations
            // -----------------------------------------------------------------
            // Remaining: Alice(30), Diana(28), Frank(40), Grace(33), Heidi(29)
            let avg_age = {
                let batches = run_query(
                    &rt,
                    "SELECT AVG(age) as avg_age FROM test_cat.myschema.users",
                )
                .await?;
                let batch = &batches[0];
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .expect("avg_age column")
                    .value(0)
            };
            // Average = (30 + 28 + 40 + 33 + 29) / 5 = 160 / 5 = 32.0
            assert!(
                (avg_age - 32.0).abs() < f64::EPSILON,
                "Expected AVG(age) = 32.0, got {avg_age}"
            );

            let max_age =
                query_scalar_i64(&rt, "SELECT MAX(age) FROM test_cat.myschema.users").await?;
            assert_eq!(max_age, 40, "Expected MAX(age) = 40 (Frank)");

            let min_age =
                query_scalar_i64(&rt, "SELECT MIN(age) FROM test_cat.myschema.users").await?;
            assert_eq!(min_age, 28, "Expected MIN(age) = 28 (Diana)");

            let sum_age =
                query_scalar_i64(&rt, "SELECT SUM(age) FROM test_cat.myschema.users").await?;
            assert_eq!(sum_age, 160, "Expected SUM(age) = 160");

            // -----------------------------------------------------------------
            // Step 8: NULL handling validation
            // -----------------------------------------------------------------
            exec(
                &rt,
                "INSERT INTO test_cat.myschema.users VALUES (9, 'Ivan', NULL, NULL)",
            )
            .await?;

            // COUNT(*) counts all rows; COUNT(email) should exclude NULLs.
            let count_star =
                query_scalar_i64(&rt, "SELECT COUNT(*) FROM test_cat.myschema.users").await?;
            assert_eq!(count_star, 6, "COUNT(*) should be 6");

            let count_email =
                query_scalar_i64(&rt, "SELECT COUNT(email) FROM test_cat.myschema.users").await?;
            // Ivan has NULL email; all others have emails.
            assert_eq!(
                count_email, 5,
                "COUNT(email) should be 5 (only Ivan has NULL email)"
            );

            // Query rows where email IS NULL.
            let batches = run_query(
                &rt,
                "SELECT id, name FROM test_cat.myschema.users WHERE email IS NULL ORDER BY id",
            )
            .await?;
            assert_batches_eq!(
                &[
                    "+----+------+",
                    "| id | name |",
                    "+----+------+",
                    "| 9  | Ivan |",
                    "+----+------+",
                ],
                &batches
            );

            // -----------------------------------------------------------------
            // Step 9: DELETE all remaining rows
            // -----------------------------------------------------------------
            exec(&rt, "DELETE FROM test_cat.myschema.users WHERE true").await?;

            let count =
                query_scalar_i64(&rt, "SELECT COUNT(*) FROM test_cat.myschema.users").await?;
            assert_eq!(count, 0, "Table should be empty after DELETE WHERE true");

            // -----------------------------------------------------------------
            // Step 10: INSERT into empty table after full delete
            // -----------------------------------------------------------------
            exec(
                &rt,
                "INSERT INTO test_cat.myschema.users VALUES (100, 'Zara', 'zara@example.com', 45)",
            )
            .await?;

            let batches = run_query(
                &rt,
                "SELECT id, name, email, age FROM test_cat.myschema.users ORDER BY id",
            )
            .await?;
            assert_batches_eq!(
                &[
                    "+-----+------+------------------+-----+",
                    "| id  | name | email            | age |",
                    "+-----+------+------------------+-----+",
                    "| 100 | Zara | zara@example.com | 45  |",
                    "+-----+------+------------------+-----+",
                ],
                &batches
            );

            Ok(())
        })
        .await
}

// =============================================================================
// Test: CREATE TABLE IF NOT EXISTS idempotency
// =============================================================================

#[tokio::test]
#[cfg_attr(
    not(feature = "spicebench"),
    ignore = "requires the spicebench feature"
)]
async fn cayenne_catalog_ddl_create_if_not_exists() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    let temp_dir = tempfile::tempdir().map_err(|e| e.to_string())?;
    let data_dir = temp_dir.path().join("data");
    let metadata_dir = temp_dir.path().join("metadata");

    test_request_context()
        .scope(async {
            let catalog = make_cayenne_catalog(
                "cat_idempotent",
                &data_dir.to_string_lossy(),
                &metadata_dir.to_string_lossy(),
            );

            let app = AppBuilder::new("cayenne_ddl_idempotent")
                .with_catalog(catalog)
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder()
                .with_app(app)
                .with_resolved_cluster_config(test_cluster_config())
                .with_runtime_config(Config::default().with_caching_disabled())
                .build()
                .await;
            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(30)) => {
                    return Err("Timeout waiting for components to load".to_string());
                }
                () = cloned_rt.load_components() => {}
            }
            runtime_ready_check_with_timeout(&rt, Duration::from_secs(30)).await;

            exec(&rt, "CREATE SCHEMA cat_idempotent.s1").await?;
            exec(
                &rt,
                "CREATE TABLE cat_idempotent.s1.t1 (id BIGINT NOT NULL, val BIGINT) PARTITION BY id",
            )
            .await?;

            // Insert a row.
            exec(&rt, "INSERT INTO cat_idempotent.s1.t1 VALUES (1, 100)").await?;

            // CREATE TABLE IF NOT EXISTS should not fail or drop data.
            exec(
                &rt,
                "CREATE TABLE IF NOT EXISTS cat_idempotent.s1.t1 (id BIGINT NOT NULL, val BIGINT) PARTITION BY id",
            )
            .await?;

            // Data must still be present.
            let count = query_scalar_i64(&rt, "SELECT COUNT(*) FROM cat_idempotent.s1.t1").await?;
            assert_eq!(
                count, 1,
                "Data should be preserved after CREATE IF NOT EXISTS"
            );

            let batches =
                run_query(&rt, "SELECT id, val FROM cat_idempotent.s1.t1 ORDER BY id").await?;
            assert_batches_eq!(
                &[
                    "+----+-----+",
                    "| id | val |",
                    "+----+-----+",
                    "| 1  | 100 |",
                    "+----+-----+",
                ],
                &batches
            );

            Ok(())
        })
        .await
}

// =============================================================================
// Test: Multiple tables in the same schema
// =============================================================================

#[tokio::test]
#[cfg_attr(
    not(feature = "spicebench"),
    ignore = "requires the spicebench feature"
)]
async fn cayenne_catalog_ddl_multiple_tables() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    let temp_dir = tempfile::tempdir().map_err(|e| e.to_string())?;
    let data_dir = temp_dir.path().join("data");
    let metadata_dir = temp_dir.path().join("metadata");

    test_request_context()
        .scope(async {
            let catalog = make_cayenne_catalog(
                "cat_multi",
                &data_dir.to_string_lossy(),
                &metadata_dir.to_string_lossy(),
            );

            let app = AppBuilder::new("cayenne_ddl_multi_tables")
                .with_catalog(catalog)
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder()
                .with_app(app)
                .with_resolved_cluster_config(test_cluster_config())
                .with_runtime_config(Config::default().with_caching_disabled())
                .build()
                .await;
            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(30)) => {
                    return Err("Timeout waiting for components to load".to_string());
                }
                () = cloned_rt.load_components() => {}
            }
            runtime_ready_check_with_timeout(&rt, Duration::from_secs(30)).await;

            exec(&rt, "CREATE SCHEMA cat_multi.store").await?;

            // Create two related tables.
            exec(
                &rt,
                "CREATE TABLE cat_multi.store.products (
                    product_id BIGINT NOT NULL,
                    name VARCHAR NOT NULL,
                    price DOUBLE NOT NULL
                ) PARTITION BY product_id",
            )
            .await?;

            exec(
                &rt,
                "CREATE TABLE cat_multi.store.orders (
                    order_id BIGINT NOT NULL,
                    product_id BIGINT NOT NULL,
                    quantity BIGINT NOT NULL
                ) PARTITION BY order_id",
            )
            .await?;

            // Insert data into both tables.
            exec(
                &rt,
                "INSERT INTO cat_multi.store.products VALUES
                    (1, 'Widget',  9.99),
                    (2, 'Gadget', 19.99),
                    (3, 'Gizmo',  14.50)",
            )
            .await?;

            exec(
                &rt,
                "INSERT INTO cat_multi.store.orders VALUES
                    (100, 1, 5),
                    (101, 2, 2),
                    (102, 1, 3),
                    (103, 3, 1)",
            )
            .await?;

            // Validate each table independently.
            let product_count =
                query_scalar_i64(&rt, "SELECT COUNT(*) FROM cat_multi.store.products").await?;
            assert_eq!(product_count, 3);

            let order_count =
                query_scalar_i64(&rt, "SELECT COUNT(*) FROM cat_multi.store.orders").await?;
            assert_eq!(order_count, 4);

            // Cross-table JOIN query.
            let batches = run_query(
                &rt,
                "SELECT p.name, SUM(o.quantity) as total_qty
                 FROM cat_multi.store.orders o
                 JOIN cat_multi.store.products p ON o.product_id = p.product_id
                 GROUP BY p.name
                 ORDER BY p.name",
            )
            .await?;

            assert_batches_eq!(
                &[
                    "+--------+-----------+",
                    "| name   | total_qty |",
                    "+--------+-----------+",
                    "| Gadget | 2         |",
                    "| Gizmo  | 1         |",
                    "| Widget | 8         |",
                    "+--------+-----------+",
                ],
                &batches
            );

            // Delete from one table and validate join still correct.
            exec(
                &rt,
                "DELETE FROM cat_multi.store.orders WHERE order_id = 100",
            )
            .await?;

            let batches = run_query(
                &rt,
                "SELECT p.name, SUM(o.quantity) as total_qty
                 FROM cat_multi.store.orders o
                 JOIN cat_multi.store.products p ON o.product_id = p.product_id
                 GROUP BY p.name
                 ORDER BY p.name",
            )
            .await?;

            assert_batches_eq!(
                &[
                    "+--------+-----------+",
                    "| name   | total_qty |",
                    "+--------+-----------+",
                    "| Gadget | 2         |",
                    "| Gizmo  | 1         |",
                    "| Widget | 3         |",
                    "+--------+-----------+",
                ],
                &batches
            );

            Ok(())
        })
        .await
}

// =============================================================================
// Test: DROP TABLE
// =============================================================================

#[tokio::test]
#[cfg_attr(
    not(feature = "spicebench"),
    ignore = "requires the spicebench feature"
)]
async fn cayenne_catalog_ddl_drop_table() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    let temp_dir = tempfile::tempdir().map_err(|e| e.to_string())?;
    let data_dir = temp_dir.path().join("data");
    let metadata_dir = temp_dir.path().join("metadata");

    test_request_context()
        .scope(async {
            let catalog = make_cayenne_catalog(
                "cat_drop",
                &data_dir.to_string_lossy(),
                &metadata_dir.to_string_lossy(),
            );

            let app = AppBuilder::new("cayenne_ddl_drop")
                .with_catalog(catalog)
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder()
                .with_app(app)
                .with_resolved_cluster_config(test_cluster_config())
                .with_runtime_config(Config::default().with_caching_disabled())
                .build()
                .await;
            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(30)) => {
                    return Err("Timeout waiting for components to load".to_string());
                }
                () = cloned_rt.load_components() => {}
            }
            runtime_ready_check_with_timeout(&rt, Duration::from_secs(30)).await;

            exec(&rt, "CREATE SCHEMA cat_drop.ns").await?;
            exec(
                &rt,
                "CREATE TABLE cat_drop.ns.ephemeral (id BIGINT NOT NULL) PARTITION BY id",
            )
            .await?;

            exec(&rt, "INSERT INTO cat_drop.ns.ephemeral VALUES (1), (2)").await?;

            let count = query_scalar_i64(&rt, "SELECT COUNT(*) FROM cat_drop.ns.ephemeral").await?;
            assert_eq!(count, 2);

            // DROP the table.
            exec(&rt, "DROP TABLE cat_drop.ns.ephemeral").await?;

            // Querying the dropped table should fail.
            let result = run_query(&rt, "SELECT * FROM cat_drop.ns.ephemeral").await;
            assert!(
                result.is_err(),
                "Querying a dropped table should produce an error"
            );

            // DROP TABLE IF EXISTS on a non-existent table should succeed.
            exec(&rt, "DROP TABLE IF EXISTS cat_drop.ns.ephemeral").await?;

            // Re-create the table — should work fine.
            exec(
                &rt,
                "CREATE TABLE cat_drop.ns.ephemeral (id BIGINT NOT NULL, val VARCHAR) PARTITION BY id",
            )
            .await?;

            exec(&rt, "INSERT INTO cat_drop.ns.ephemeral VALUES (10, 'new')").await?;

            let batches =
                run_query(&rt, "SELECT id, val FROM cat_drop.ns.ephemeral ORDER BY id").await?;
            assert_batches_eq!(
                &[
                    "+----+-----+",
                    "| id | val |",
                    "+----+-----+",
                    "| 10 | new |",
                    "+----+-----+",
                ],
                &batches
            );

            Ok(())
        })
        .await
}

// =============================================================================
// Test: CREATE TABLE with PRIMARY KEY — upsert on conflict via INSERT
// =============================================================================

#[tokio::test]
#[cfg_attr(
    not(feature = "spicebench"),
    ignore = "requires the spicebench feature"
)]
async fn cayenne_catalog_ddl_primary_key_upsert() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    let temp_dir = tempfile::tempdir().map_err(|e| e.to_string())?;
    let data_dir = temp_dir.path().join("data");
    let metadata_dir = temp_dir.path().join("metadata");

    test_request_context()
        .scope(async {
            let catalog = make_cayenne_catalog(
                "cat_pk",
                &data_dir.to_string_lossy(),
                &metadata_dir.to_string_lossy(),
            );

            let app = AppBuilder::new("cayenne_ddl_pk_upsert")
                .with_catalog(catalog)
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder()
                .with_app(app)
                .with_resolved_cluster_config(test_cluster_config())
                .with_runtime_config(Config::default().with_caching_disabled())
                .build()
                .await;
            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(30)) => {
                    return Err("Timeout waiting for components to load".to_string());
                }
                () = cloned_rt.load_components() => {}
            }
            runtime_ready_check_with_timeout(&rt, Duration::from_secs(30)).await;

            // -----------------------------------------------------------------
            // Step 1: CREATE TABLE with PRIMARY KEY
            // -----------------------------------------------------------------
            exec(&rt, "CREATE SCHEMA cat_pk.myschema").await?;

            exec(
                &rt,
                "CREATE TABLE cat_pk.myschema.users (
                    id BIGINT NOT NULL,
                    name VARCHAR NOT NULL,
                    email VARCHAR,
                    PRIMARY KEY (id)
                ) PARTITION BY id",
            )
            .await?;

            // -----------------------------------------------------------------
            // Step 2: Initial INSERT
            // -----------------------------------------------------------------
            exec(
                &rt,
                "INSERT INTO cat_pk.myschema.users VALUES
                    (1, 'Alice', 'alice@example.com'),
                    (2, 'Bob',   'bob@example.com'),
                    (3, 'Charlie', 'charlie@example.com')",
            )
            .await?;

            let count = query_scalar_i64(&rt, "SELECT COUNT(*) FROM cat_pk.myschema.users").await?;
            assert_eq!(count, 3, "Expected 3 rows after initial insert");

            let batches = run_query(
                &rt,
                "SELECT id, name, email FROM cat_pk.myschema.users ORDER BY id",
            )
            .await?;
            assert_batches_eq!(
                &[
                    "+----+---------+---------------------+",
                    "| id | name    | email               |",
                    "+----+---------+---------------------+",
                    "| 1  | Alice   | alice@example.com   |",
                    "| 2  | Bob     | bob@example.com     |",
                    "| 3  | Charlie | charlie@example.com |",
                    "+----+---------+---------------------+",
                ],
                &batches
            );

            // -----------------------------------------------------------------
            // Step 3: INSERT with conflicting PKs — should upsert (replace)
            // -----------------------------------------------------------------
            exec(
                &rt,
                "INSERT INTO cat_pk.myschema.users VALUES
                    (2, 'Bob Updated', 'bob_new@example.com'),
                    (4, 'Diana', 'diana@example.com')",
            )
            .await?;

            // Should have 4 rows: Alice(1), Bob Updated(2), Charlie(3), Diana(4)
            // Bob's row should be replaced, not duplicated.
            let count = query_scalar_i64(&rt, "SELECT COUNT(*) FROM cat_pk.myschema.users").await?;
            assert_eq!(
                count, 4,
                "Expected 4 rows after upsert (Bob replaced, Diana added)"
            );

            let batches = run_query(
                &rt,
                "SELECT id, name, email FROM cat_pk.myschema.users ORDER BY id",
            )
            .await?;
            assert_batches_eq!(
                &[
                    "+----+-------------+---------------------+",
                    "| id | name        | email               |",
                    "+----+-------------+---------------------+",
                    "| 1  | Alice       | alice@example.com   |",
                    "| 2  | Bob Updated | bob_new@example.com |",
                    "| 3  | Charlie     | charlie@example.com |",
                    "| 4  | Diana       | diana@example.com   |",
                    "+----+-------------+---------------------+",
                ],
                &batches
            );

            // -----------------------------------------------------------------
            // Step 4: INSERT all conflicting PKs — pure upsert, no new rows
            // -----------------------------------------------------------------
            exec(
                &rt,
                "INSERT INTO cat_pk.myschema.users VALUES
                    (1, 'Alice V2', 'alice_v2@example.com'),
                    (3, 'Charlie V2', 'charlie_v2@example.com')",
            )
            .await?;

            let count = query_scalar_i64(&rt, "SELECT COUNT(*) FROM cat_pk.myschema.users").await?;
            assert_eq!(count, 4, "Row count should remain 4 after pure upsert");

            let batches = run_query(
                &rt,
                "SELECT id, name, email FROM cat_pk.myschema.users ORDER BY id",
            )
            .await?;
            assert_batches_eq!(
                &[
                    "+----+-------------+------------------------+",
                    "| id | name        | email                  |",
                    "+----+-------------+------------------------+",
                    "| 1  | Alice V2    | alice_v2@example.com   |",
                    "| 2  | Bob Updated | bob_new@example.com    |",
                    "| 3  | Charlie V2  | charlie_v2@example.com |",
                    "| 4  | Diana       | diana@example.com      |",
                    "+----+-------------+------------------------+",
                ],
                &batches
            );

            // -----------------------------------------------------------------
            // Step 5: Verify DELETE still works on PK table
            // -----------------------------------------------------------------
            exec(&rt, "DELETE FROM cat_pk.myschema.users WHERE id = 2").await?;

            let count = query_scalar_i64(&rt, "SELECT COUNT(*) FROM cat_pk.myschema.users").await?;
            assert_eq!(count, 3, "Expected 3 rows after delete");

            let batches = run_query(
                &rt,
                "SELECT id, name FROM cat_pk.myschema.users ORDER BY id",
            )
            .await?;
            assert_batches_eq!(
                &[
                    "+----+------------+",
                    "| id | name       |",
                    "+----+------------+",
                    "| 1  | Alice V2   |",
                    "| 3  | Charlie V2 |",
                    "| 4  | Diana      |",
                    "+----+------------+",
                ],
                &batches
            );

            Ok(())
        })
        .await
}

// =============================================================================
// Test: Multiple schemas in the same catalog
// =============================================================================

#[tokio::test]
#[cfg_attr(
    not(feature = "spicebench"),
    ignore = "requires the spicebench feature"
)]
async fn cayenne_catalog_ddl_multiple_schemas() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    let temp_dir = tempfile::tempdir().map_err(|e| e.to_string())?;
    let data_dir = temp_dir.path().join("data");
    let metadata_dir = temp_dir.path().join("metadata");

    test_request_context()
        .scope(async {
            let catalog = make_cayenne_catalog(
                "cat_schemas",
                &data_dir.to_string_lossy(),
                &metadata_dir.to_string_lossy(),
            );

            let app = AppBuilder::new("cayenne_ddl_multi_schemas")
                .with_catalog(catalog)
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder()
                .with_app(app)
                .with_resolved_cluster_config(test_cluster_config())
                .with_runtime_config(Config::default().with_caching_disabled())
                .build()
                .await;
            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(30)) => {
                    return Err("Timeout waiting for components to load".to_string());
                }
                () = cloned_rt.load_components() => {}
            }
            runtime_ready_check_with_timeout(&rt, Duration::from_secs(30)).await;

            // Create two separate schemas.
            exec(&rt, "CREATE SCHEMA cat_schemas.finance").await?;
            exec(&rt, "CREATE SCHEMA cat_schemas.hr").await?;

            // Create tables with the same name in different schemas.
            exec(
                &rt,
                "CREATE TABLE cat_schemas.finance.records (id BIGINT NOT NULL, amount DOUBLE) PARTITION BY id",
            )
            .await?;
            exec(
                &rt,
                "CREATE TABLE cat_schemas.hr.records (id BIGINT NOT NULL, employee VARCHAR) PARTITION BY id",
            )
            .await?;

            // Insert data into both.
            exec(
                &rt,
                "INSERT INTO cat_schemas.finance.records VALUES (1, 1000.50), (2, 2500.75)",
            )
            .await?;
            exec(
                &rt,
                "INSERT INTO cat_schemas.hr.records VALUES (1, 'Alice'), (2, 'Bob')",
            )
            .await?;

            // Validate isolation — each schema has its own data.
            let batches = run_query(
                &rt,
                "SELECT id, amount FROM cat_schemas.finance.records ORDER BY id",
            )
            .await?;
            assert_batches_eq!(
                &[
                    "+----+---------+",
                    "| id | amount  |",
                    "+----+---------+",
                    "| 1  | 1000.5  |",
                    "| 2  | 2500.75 |",
                    "+----+---------+",
                ],
                &batches
            );

            let batches = run_query(
                &rt,
                "SELECT id, employee FROM cat_schemas.hr.records ORDER BY id",
            )
            .await?;
            assert_batches_eq!(
                &[
                    "+----+----------+",
                    "| id | employee |",
                    "+----+----------+",
                    "| 1  | Alice    |",
                    "| 2  | Bob      |",
                    "+----+----------+",
                ],
                &batches
            );

            // Delete from one schema, verify the other is untouched.
            exec(&rt, "DELETE FROM cat_schemas.finance.records WHERE id = 1").await?;

            let finance_count =
                query_scalar_i64(&rt, "SELECT COUNT(*) FROM cat_schemas.finance.records").await?;
            assert_eq!(finance_count, 1, "finance.records should have 1 row");

            let hr_count =
                query_scalar_i64(&rt, "SELECT COUNT(*) FROM cat_schemas.hr.records").await?;
            assert_eq!(
                hr_count, 2,
                "hr.records should still have 2 rows (untouched)"
            );

            Ok(())
        })
        .await
}

// =============================================================================
// Test: MERGE INTO — matched update with aliases, expressions, no-match rows
// =============================================================================

#[tokio::test]
#[cfg_attr(
    not(feature = "spicebench"),
    ignore = "requires the spicebench feature"
)]
async fn cayenne_catalog_merge_into() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    let temp_dir = tempfile::tempdir().map_err(|e| e.to_string())?;
    let data_dir = temp_dir.path().join("data");
    let metadata_dir = temp_dir.path().join("metadata");

    test_request_context()
        .scope(async {
            let catalog = make_cayenne_catalog(
                "cat_merge",
                &data_dir.to_string_lossy(),
                &metadata_dir.to_string_lossy(),
            );

            let app = AppBuilder::new("cayenne_merge_into")
                .with_catalog(catalog)
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder()
                .with_app(app)
                .with_resolved_cluster_config(test_cluster_config())
                .with_runtime_config(Config::default().with_caching_disabled())
                .build()
                .await;
            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(30)) => {
                    return Err("Timeout waiting for components to load".to_string());
                }
                () = cloned_rt.load_components() => {}
            }
            runtime_ready_check_with_timeout(&rt, Duration::from_secs(30)).await;

            exec(&rt, "CREATE SCHEMA cat_merge.s").await?;

            exec(
                &rt,
                "CREATE TABLE cat_merge.s.inventory (
                    id BIGINT NOT NULL,
                    name VARCHAR NOT NULL,
                    qty BIGINT NOT NULL
                ) PARTITION BY id",
            )
            .await?;

            exec(
                &rt,
                "CREATE TABLE cat_merge.s.updates (
                    id BIGINT NOT NULL,
                    name VARCHAR NOT NULL,
                    qty BIGINT NOT NULL
                ) PARTITION BY id",
            )
            .await?;

            // -----------------------------------------------------------------
            // Seed data
            // -----------------------------------------------------------------
            exec(
                &rt,
                "INSERT INTO cat_merge.s.inventory VALUES
                    (1, 'apple',  10),
                    (2, 'banana', 20),
                    (3, 'cherry', 30)",
            )
            .await?;

            exec(
                &rt,
                "INSERT INTO cat_merge.s.updates VALUES
                    (1, 'apple',  50),
                    (3, 'cherry', 100)",
            )
            .await?;

            // -----------------------------------------------------------------
            // Step 1: Basic MERGE — update qty from source
            // -----------------------------------------------------------------
            exec(
                &rt,
                "MERGE INTO cat_merge.s.inventory AS t
                 USING cat_merge.s.updates AS s
                 ON t.id = s.id
                 WHEN MATCHED THEN UPDATE SET qty = s.qty",
            )
            .await?;

            let batches = run_query(
                &rt,
                "SELECT id, name, qty FROM cat_merge.s.inventory ORDER BY id",
            )
            .await?;

            assert_batches_eq!(
                &[
                    "+----+--------+-----+",
                    "| id | name   | qty |",
                    "+----+--------+-----+",
                    "| 1  | apple  | 50  |",
                    "| 2  | banana | 20  |",
                    "| 3  | cherry | 100 |",
                    "+----+--------+-----+",
                ],
                &batches
            );

            // -----------------------------------------------------------------
            // Step 2: MERGE with expression — qty = s.qty + t.qty
            // -----------------------------------------------------------------
            exec(
                &rt,
                "MERGE INTO cat_merge.s.inventory AS t
                 USING cat_merge.s.updates AS s
                 ON t.id = s.id
                 WHEN MATCHED THEN UPDATE SET qty = s.qty + t.qty",
            )
            .await?;

            let batches = run_query(
                &rt,
                "SELECT id, name, qty FROM cat_merge.s.inventory ORDER BY id",
            )
            .await?;

            // apple: 50 + 50 = 100, cherry: 100 + 100 = 200, banana unchanged
            assert_batches_eq!(
                &[
                    "+----+--------+-----+",
                    "| id | name   | qty |",
                    "+----+--------+-----+",
                    "| 1  | apple  | 100 |",
                    "| 2  | banana | 20  |",
                    "| 3  | cherry | 200 |",
                    "+----+--------+-----+",
                ],
                &batches
            );

            // -----------------------------------------------------------------
            // Step 3: MERGE updating multiple columns
            // -----------------------------------------------------------------
            exec(
                &rt,
                "MERGE INTO cat_merge.s.inventory AS t
                 USING cat_merge.s.updates AS s
                 ON t.id = s.id
                 WHEN MATCHED THEN UPDATE SET qty = s.qty, name = s.name",
            )
            .await?;

            let batches = run_query(
                &rt,
                "SELECT id, name, qty FROM cat_merge.s.inventory ORDER BY id",
            )
            .await?;

            assert_batches_eq!(
                &[
                    "+----+--------+-----+",
                    "| id | name   | qty |",
                    "+----+--------+-----+",
                    "| 1  | apple  | 50  |",
                    "| 2  | banana | 20  |",
                    "| 3  | cherry | 100 |",
                    "+----+--------+-----+",
                ],
                &batches
            );

            // -----------------------------------------------------------------
            // Step 4: MERGE without aliases (table-name qualifiers)
            // -----------------------------------------------------------------
            exec(
                &rt,
                "MERGE INTO cat_merge.s.inventory
                 USING cat_merge.s.updates
                 ON inventory.id = updates.id
                 WHEN MATCHED THEN UPDATE SET qty = updates.qty + 1",
            )
            .await?;

            let batches = run_query(
                &rt,
                "SELECT id, name, qty FROM cat_merge.s.inventory ORDER BY id",
            )
            .await?;

            // apple: 50 + 1 = 51, cherry: 100 + 1 = 101
            assert_batches_eq!(
                &[
                    "+----+--------+-----+",
                    "| id | name   | qty |",
                    "+----+--------+-----+",
                    "| 1  | apple  | 51  |",
                    "| 2  | banana | 20  |",
                    "| 3  | cherry | 101 |",
                    "+----+--------+-----+",
                ],
                &batches
            );

            // -----------------------------------------------------------------
            // Step 5: MERGE with zero matches — no rows should change
            // -----------------------------------------------------------------
            // Replace source data with a non-matching ID.
            exec(&rt, "DELETE FROM cat_merge.s.updates WHERE id IN (1, 3)").await?;
            exec(
                &rt,
                "INSERT INTO cat_merge.s.updates VALUES (99, 'ghost', 999)",
            )
            .await?;

            exec(
                &rt,
                "MERGE INTO cat_merge.s.inventory AS t
                 USING cat_merge.s.updates AS s
                 ON t.id = s.id
                 WHEN MATCHED THEN UPDATE SET qty = s.qty",
            )
            .await?;

            let batches = run_query(
                &rt,
                "SELECT id, name, qty FROM cat_merge.s.inventory ORDER BY id",
            )
            .await?;

            // Unchanged from step 4.
            assert_batches_eq!(
                &[
                    "+----+--------+-----+",
                    "| id | name   | qty |",
                    "+----+--------+-----+",
                    "| 1  | apple  | 51  |",
                    "| 2  | banana | 20  |",
                    "| 3  | cherry | 101 |",
                    "+----+--------+-----+",
                ],
                &batches
            );

            let count = query_scalar_i64(&rt, "SELECT COUNT(*) FROM cat_merge.s.inventory").await?;
            assert_eq!(count, 3, "Row count should remain 3 after zero-match MERGE");

            Ok(())
        })
        .await
}

// =============================================================================
// Test: MERGE INTO with partition key different from join key
// =============================================================================

#[tokio::test]
#[cfg_attr(
    not(feature = "spicebench"),
    ignore = "requires the spicebench feature"
)]
async fn cayenne_catalog_merge_partition_key_differs_from_join_key() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    let temp_dir = tempfile::tempdir().map_err(|e| e.to_string())?;
    let data_dir = temp_dir.path().join("data");
    let metadata_dir = temp_dir.path().join("metadata");

    test_request_context()
        .scope(async {
            let catalog = make_cayenne_catalog(
                "cat_mp",
                &data_dir.to_string_lossy(),
                &metadata_dir.to_string_lossy(),
            );

            let app = AppBuilder::new("cayenne_merge_partition")
                .with_catalog(catalog)
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder()
                .with_app(app)
                .with_resolved_cluster_config(test_cluster_config())
                .with_runtime_config(Config::default().with_caching_disabled())
                .build()
                .await;
            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(30)) => {
                    return Err("Timeout waiting for components to load".to_string());
                }
                () = cloned_rt.load_components() => {}
            }
            runtime_ready_check_with_timeout(&rt, Duration::from_secs(30)).await;

            exec(&rt, "CREATE SCHEMA cat_mp.s").await?;

            // Partitioned by `region` but MERGE joins on `sku`.
            exec(
                &rt,
                "CREATE TABLE cat_mp.s.stock (
                    sku VARCHAR NOT NULL,
                    region VARCHAR NOT NULL,
                    qty BIGINT NOT NULL
                ) PARTITION BY region",
            )
            .await?;

            exec(
                &rt,
                "CREATE TABLE cat_mp.s.inbound (
                    sku VARCHAR NOT NULL,
                    region VARCHAR NOT NULL,
                    qty BIGINT NOT NULL
                ) PARTITION BY region",
            )
            .await?;

            // Stock spans two partitions (US and EU).
            exec(
                &rt,
                "INSERT INTO cat_mp.s.stock VALUES
                    ('A', 'US', 10),
                    ('B', 'US', 20),
                    ('A', 'EU', 30),
                    ('C', 'EU', 40)",
            )
            .await?;

            // Inbound shipments only for SKU A across both regions.
            exec(
                &rt,
                "INSERT INTO cat_mp.s.inbound VALUES
                    ('A', 'US', 100),
                    ('A', 'EU', 200)",
            )
            .await?;

            // MERGE on sku + region (composite ON) — updates rows in both partitions.
            exec(
                &rt,
                "MERGE INTO cat_mp.s.stock AS t
                 USING cat_mp.s.inbound AS s
                 ON t.sku = s.sku AND t.region = s.region
                 WHEN MATCHED THEN UPDATE SET qty = s.qty",
            )
            .await?;

            let batches = run_query(
                &rt,
                "SELECT sku, region, qty FROM cat_mp.s.stock ORDER BY region, sku",
            )
            .await?;

            assert_batches_eq!(
                &[
                    "+-----+--------+-----+",
                    "| sku | region | qty |",
                    "+-----+--------+-----+",
                    "| A   | EU     | 200 |",
                    "| C   | EU     | 40  |",
                    "| A   | US     | 100 |",
                    "| B   | US     | 20  |",
                    "+-----+--------+-----+",
                ],
                &batches
            );

            // Verify row count unchanged.
            let count = query_scalar_i64(&rt, "SELECT COUNT(*) FROM cat_mp.s.stock").await?;
            assert_eq!(count, 4);

            Ok(())
        })
        .await
}

// =============================================================================
// Test: MERGE INTO across multiple partitions with 3-way composite ON key
// =============================================================================

#[tokio::test]
#[cfg_attr(
    not(feature = "spicebench"),
    ignore = "requires the spicebench feature"
)]
async fn cayenne_catalog_merge_composite_on_key() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    let temp_dir = tempfile::tempdir().map_err(|e| e.to_string())?;
    let data_dir = temp_dir.path().join("data");
    let metadata_dir = temp_dir.path().join("metadata");

    test_request_context()
        .scope(async {
            let catalog = make_cayenne_catalog(
                "cat_mcp",
                &data_dir.to_string_lossy(),
                &metadata_dir.to_string_lossy(),
            );

            let app = AppBuilder::new("cayenne_merge_composite")
                .with_catalog(catalog)
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder()
                .with_app(app)
                .with_resolved_cluster_config(test_cluster_config())
                .with_runtime_config(Config::default().with_caching_disabled())
                .build()
                .await;
            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(30)) => {
                    return Err("Timeout waiting for components to load".to_string());
                }
                () = cloned_rt.load_components() => {}
            }
            runtime_ready_check_with_timeout(&rt, Duration::from_secs(30)).await;

            exec(&rt, "CREATE SCHEMA cat_mcp.s").await?;

            // Partitioned by month, MERGE joins on sensor_id + year + month.
            exec(
                &rt,
                "CREATE TABLE cat_mcp.s.metrics (
                    sensor_id BIGINT NOT NULL,
                    year BIGINT NOT NULL,
                    month BIGINT NOT NULL,
                    reading DOUBLE NOT NULL
                ) PARTITION BY month",
            )
            .await?;

            exec(
                &rt,
                "CREATE TABLE cat_mcp.s.corrections (
                    sensor_id BIGINT NOT NULL,
                    year BIGINT NOT NULL,
                    month BIGINT NOT NULL,
                    reading DOUBLE NOT NULL
                ) PARTITION BY month",
            )
            .await?;

            // Data spanning two partitions (month=1 and month=2).
            exec(
                &rt,
                "INSERT INTO cat_mcp.s.metrics VALUES
                    (1, 2025, 1, 10.0),
                    (2, 2025, 1, 20.0),
                    (1, 2025, 2, 30.0),
                    (3, 2025, 2, 40.0)",
            )
            .await?;

            // Corrections for sensor 1 in both months, sensor 2 in Jan only.
            exec(
                &rt,
                "INSERT INTO cat_mcp.s.corrections VALUES
                    (1, 2025, 1, 11.5),
                    (2, 2025, 1, 22.5),
                    (1, 2025, 2, 33.5)",
            )
            .await?;

            // MERGE joining on sensor_id + year + month (3-way composite ON key).
            exec(
                &rt,
                "MERGE INTO cat_mcp.s.metrics AS t
                 USING cat_mcp.s.corrections AS s
                 ON t.sensor_id = s.sensor_id
                    AND t.year = s.year
                    AND t.month = s.month
                 WHEN MATCHED THEN UPDATE SET reading = s.reading",
            )
            .await?;

            let batches = run_query(
                &rt,
                "SELECT sensor_id, year, month, reading
                 FROM cat_mcp.s.metrics
                 ORDER BY year, month, sensor_id",
            )
            .await?;

            assert_batches_eq!(
                &[
                    "+-----------+------+-------+---------+",
                    "| sensor_id | year | month | reading |",
                    "+-----------+------+-------+---------+",
                    "| 1         | 2025 | 1     | 11.5    |",
                    "| 2         | 2025 | 1     | 22.5    |",
                    "| 1         | 2025 | 2     | 33.5    |",
                    "| 3         | 2025 | 2     | 40.0    |",
                    "+-----------+------+-------+---------+",
                ],
                &batches
            );

            // sensor 3 in Feb should remain untouched (no matching correction).
            let count = query_scalar_i64(&rt, "SELECT COUNT(*) FROM cat_mcp.s.metrics").await?;
            assert_eq!(count, 4);

            Ok(())
        })
        .await
}

/// Regression test: composite ON keys must use tuple-aware deletion predicates.
///
/// With two composite key columns (region, sku), if the matched rows are
/// (US, A) and (EU, B), the old independent IN-list approach would build:
///   `region IN ('US','EU') AND sku IN ('A','B')`
/// which also matches (US, B) and (EU, A) — corrupting unmatched rows.
///
/// This test has exactly that pattern: target has (US,A), (US,B), (EU,A), (EU,B)
/// but source only matches (US,A) and (EU,B). The unmatched rows (US,B) and (EU,A)
/// must be preserved unchanged.
#[tokio::test]
#[cfg_attr(
    not(feature = "spicebench"),
    ignore = "requires the spicebench feature"
)]
async fn cayenne_catalog_merge_composite_key_no_cross_product() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    let temp_dir = tempfile::tempdir().map_err(|e| e.to_string())?;
    let data_dir = temp_dir.path().join("data");
    let metadata_dir = temp_dir.path().join("metadata");

    test_request_context()
        .scope(async {
            let catalog = make_cayenne_catalog(
                "cat_xprod",
                &data_dir.to_string_lossy(),
                &metadata_dir.to_string_lossy(),
            );

            let app = AppBuilder::new("cayenne_merge_cross_product")
                .with_catalog(catalog)
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder()
                .with_app(app)
                .with_resolved_cluster_config(test_cluster_config())
                .with_runtime_config(Config::default().with_caching_disabled())
                .build()
                .await;
            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(30)) => {
                    return Err("Timeout waiting for components to load".to_string());
                }
                () = cloned_rt.load_components() => {}
            }
            runtime_ready_check_with_timeout(&rt, Duration::from_secs(30)).await;

            exec(&rt, "CREATE SCHEMA cat_xprod.s").await?;

            // Target table with composite key (region, sku). Partitioned by region.
            exec(
                &rt,
                "CREATE TABLE cat_xprod.s.inventory (
                    region VARCHAR NOT NULL,
                    sku VARCHAR NOT NULL,
                    qty BIGINT NOT NULL
                ) PARTITION BY region",
            )
            .await?;

            exec(
                &rt,
                "CREATE TABLE cat_xprod.s.updates (
                    region VARCHAR NOT NULL,
                    sku VARCHAR NOT NULL,
                    qty BIGINT NOT NULL
                ) PARTITION BY region",
            )
            .await?;

            // All 4 combinations of region x sku exist in target.
            exec(
                &rt,
                "INSERT INTO cat_xprod.s.inventory VALUES
                    ('US', 'A', 10),
                    ('US', 'B', 20),
                    ('EU', 'A', 30),
                    ('EU', 'B', 40)",
            )
            .await?;

            // Source only updates the diagonal: (US, A) and (EU, B).
            // A cross-product bug would also delete/corrupt (US, B) and (EU, A).
            exec(
                &rt,
                "INSERT INTO cat_xprod.s.updates VALUES
                    ('US', 'A', 99),
                    ('EU', 'B', 88)",
            )
            .await?;

            // MERGE with composite ON key.
            exec(
                &rt,
                "MERGE INTO cat_xprod.s.inventory AS t
                 USING cat_xprod.s.updates AS s
                 ON t.region = s.region AND t.sku = s.sku
                 WHEN MATCHED THEN UPDATE SET qty = s.qty",
            )
            .await?;

            let batches = run_query(
                &rt,
                "SELECT region, sku, qty
                 FROM cat_xprod.s.inventory
                 ORDER BY region, sku",
            )
            .await?;

            // Only (US,A) and (EU,B) should be updated.
            // (US,B) and (EU,A) must be UNCHANGED — not deleted, not modified.
            assert_batches_eq!(
                &[
                    "+--------+-----+-----+",
                    "| region | sku | qty |",
                    "+--------+-----+-----+",
                    "| EU     | A   | 30  |",
                    "| EU     | B   | 88  |",
                    "| US     | A   | 99  |",
                    "| US     | B   | 20  |",
                    "+--------+-----+-----+",
                ],
                &batches
            );

            // Row count must still be 4 — no rows lost.
            let count = query_scalar_i64(&rt, "SELECT COUNT(*) FROM cat_xprod.s.inventory").await?;
            assert_eq!(count, 4);

            Ok(())
        })
        .await
}

// =============================================================================
// Test: MERGE INTO — duplicate source keys must error without losing target rows
// =============================================================================

#[tokio::test]
#[cfg_attr(
    not(feature = "spicebench"),
    ignore = "requires the spicebench feature"
)]
async fn cayenne_catalog_merge_duplicate_source_keys_rejected() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    let temp_dir = tempfile::tempdir().map_err(|e| e.to_string())?;
    let data_dir = temp_dir.path().join("data");
    let metadata_dir = temp_dir.path().join("metadata");

    test_request_context()
        .scope(async {
            let catalog = make_cayenne_catalog(
                "cat_dupkey",
                &data_dir.to_string_lossy(),
                &metadata_dir.to_string_lossy(),
            );

            let app = AppBuilder::new("cayenne_merge_dup_key")
                .with_catalog(catalog)
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder()
                .with_app(app)
                .with_resolved_cluster_config(test_cluster_config())
                .with_runtime_config(Config::default().with_caching_disabled())
                .build()
                .await;
            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(30)) => {
                    return Err("Timeout waiting for components to load".to_string());
                }
                () = cloned_rt.load_components() => {}
            }
            runtime_ready_check_with_timeout(&rt, Duration::from_secs(30)).await;

            exec(&rt, "CREATE SCHEMA cat_dupkey.s").await?;

            exec(
                &rt,
                "CREATE TABLE cat_dupkey.s.target (
                    id BIGINT NOT NULL,
                    val BIGINT NOT NULL
                ) PARTITION BY id",
            )
            .await?;

            exec(
                &rt,
                "CREATE TABLE cat_dupkey.s.source (
                    id BIGINT NOT NULL,
                    val BIGINT NOT NULL
                ) PARTITION BY id",
            )
            .await?;

            // Insert one row into target.
            exec(&rt, "INSERT INTO cat_dupkey.s.target VALUES (1, 100)").await?;

            // Insert TWO rows with the same key into source — this is the
            // duplicate key scenario that must not cause data loss.
            exec(
                &rt,
                "INSERT INTO cat_dupkey.s.source VALUES (1, 200), (1, 300)",
            )
            .await?;

            // MERGE should error because source has duplicate keys for the
            // matched target row.
            let merge_result = run_query(
                &rt,
                "MERGE INTO cat_dupkey.s.target AS t
                 USING cat_dupkey.s.source AS s
                 ON t.id = s.id
                 WHEN MATCHED THEN UPDATE SET val = s.val",
            )
            .await;
            assert!(
                merge_result.is_err(),
                "MERGE with duplicate source keys should fail, got: {merge_result:?}"
            );
            let err_msg =
                merge_result.expect_err("MERGE with duplicate source keys should return error");
            assert!(
                err_msg.contains("duplicate"),
                "Error should mention duplicate keys, got: {err_msg}"
            );

            // Verify the target table is UNCHANGED — the row must not be lost.
            let count = query_scalar_i64(&rt, "SELECT COUNT(*) FROM cat_dupkey.s.target").await?;
            assert_eq!(count, 1, "Target must still have 1 row after failed MERGE");

            let batches = run_query(&rt, "SELECT id, val FROM cat_dupkey.s.target").await?;
            assert_batches_eq!(
                [
                    "+----+-----+",
                    "| id | val |",
                    "+----+-----+",
                    "| 1  | 100 |",
                    "+----+-----+",
                ],
                &batches
            );

            Ok(())
        })
        .await
}

// =============================================================================
// Test: UPDATE — single-row, expression, NULL, multi-column, zero-match
// =============================================================================
//
// Exercises the single-node `CayenneTableProvider::update` path. The cluster
// variant in tests/cluster/distributed_cayenne_catalog.rs covers the scheduler
// → executor forwarding path; this test covers the direct TableProvider::update
// call.
#[tokio::test]
#[cfg_attr(
    not(feature = "spicebench"),
    ignore = "requires the spicebench feature"
)]
async fn cayenne_catalog_ddl_update() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    let temp_dir = tempfile::tempdir().map_err(|e| e.to_string())?;
    let data_dir = temp_dir.path().join("data");
    let metadata_dir = temp_dir.path().join("metadata");

    test_request_context()
        .scope(async {
            let catalog = make_cayenne_catalog(
                "cat_upd",
                &data_dir.to_string_lossy(),
                &metadata_dir.to_string_lossy(),
            );

            let app = AppBuilder::new("cayenne_ddl_update")
                .with_catalog(catalog)
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder()
                .with_app(app)
                .with_resolved_cluster_config(test_cluster_config())
                .with_runtime_config(Config::default().with_caching_disabled())
                .build()
                .await;
            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(30)) => {
                    return Err("Timeout waiting for components to load".to_string());
                }
                () = cloned_rt.load_components() => {}
            }
            runtime_ready_check_with_timeout(&rt, Duration::from_secs(30)).await;

            exec(&rt, "CREATE SCHEMA cat_upd.s").await?;
            exec(
                &rt,
                "CREATE TABLE cat_upd.s.users (
                    id BIGINT NOT NULL,
                    name VARCHAR NOT NULL,
                    email VARCHAR,
                    age BIGINT,
                    PRIMARY KEY (id)
                ) PARTITION BY id",
            )
            .await?;

            exec(
                &rt,
                "INSERT INTO cat_upd.s.users VALUES
                    (1, 'Alice',   'alice@example.com',   30),
                    (2, 'Bob',     'bob@example.com',     25),
                    (3, 'Charlie', 'charlie@example.com', 35),
                    (4, 'Diana',   'diana@example.com',   28),
                    (5, 'Eve',     'eve@example.com',     22)",
            )
            .await?;

            // Single-row UPDATE.
            exec(&rt, "UPDATE cat_upd.s.users SET age = 31 WHERE id = 1").await?;
            let batches = run_query(&rt, "SELECT age FROM cat_upd.s.users WHERE id = 1").await?;
            assert_batches_eq!(
                &["+-----+", "| age |", "+-----+", "| 31  |", "+-----+",],
                &batches
            );

            // Expression UPDATE — bump ages > 30 by 10.
            exec(
                &rt,
                "UPDATE cat_upd.s.users SET age = age + 10 WHERE age > 30",
            )
            .await?;
            let batches = run_query(&rt, "SELECT id, age FROM cat_upd.s.users ORDER BY id").await?;
            // Alice(31→41), Charlie(35→45); Bob(25), Diana(28), Eve(22) unchanged.
            assert_batches_eq!(
                &[
                    "+----+-----+",
                    "| id | age |",
                    "+----+-----+",
                    "| 1  | 41  |",
                    "| 2  | 25  |",
                    "| 3  | 45  |",
                    "| 4  | 28  |",
                    "| 5  | 22  |",
                    "+----+-----+",
                ],
                &batches
            );

            // Set column to NULL.
            exec(&rt, "UPDATE cat_upd.s.users SET email = NULL WHERE id = 4").await?;
            let batches = run_query(&rt, "SELECT email FROM cat_upd.s.users WHERE id = 4").await?;
            assert_batches_eq!(
                &[
                    "+-------+",
                    "| email |",
                    "+-------+",
                    "|       |",
                    "+-------+",
                ],
                &batches
            );

            // Multi-column UPDATE.
            exec(
                &rt,
                "UPDATE cat_upd.s.users SET name = 'Bobby', age = 99 WHERE id = 2",
            )
            .await?;
            let batches = run_query(
                &rt,
                "SELECT id, name, age FROM cat_upd.s.users WHERE id = 2",
            )
            .await?;
            assert_batches_eq!(
                &[
                    "+----+-------+-----+",
                    "| id | name  | age |",
                    "+----+-------+-----+",
                    "| 2  | Bobby | 99  |",
                    "+----+-------+-----+",
                ],
                &batches
            );

            // Zero-match UPDATE — no-op.
            exec(&rt, "UPDATE cat_upd.s.users SET age = 0 WHERE id = 9999").await?;

            // Row count unchanged throughout.
            let count = query_scalar_i64(&rt, "SELECT COUNT(*) FROM cat_upd.s.users").await?;
            assert_eq!(count, 5, "UPDATE must not change row count");

            // Final state check — only touched rows changed.
            let batches = run_query(
                &rt,
                "SELECT id, name, email, age FROM cat_upd.s.users ORDER BY id",
            )
            .await?;
            assert_batches_eq!(
                &[
                    "+----+---------+---------------------+-----+",
                    "| id | name    | email               | age |",
                    "+----+---------+---------------------+-----+",
                    "| 1  | Alice   | alice@example.com   | 41  |",
                    "| 2  | Bobby   | bob@example.com     | 99  |",
                    "| 3  | Charlie | charlie@example.com | 45  |",
                    "| 4  | Diana   |                     | 28  |",
                    "| 5  | Eve     | eve@example.com     | 22  |",
                    "+----+---------+---------------------+-----+",
                ],
                &batches
            );

            Ok(())
        })
        .await
}

// =============================================================================
// Test: UPDATE/DELETE without a WHERE clause
// =============================================================================
//
// Distinct SQL parse path from `WHERE true`.
#[tokio::test]
#[cfg_attr(
    not(feature = "spicebench"),
    ignore = "requires the spicebench feature"
)]
async fn cayenne_catalog_ddl_dml_no_where() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    let temp_dir = tempfile::tempdir().map_err(|e| e.to_string())?;
    let data_dir = temp_dir.path().join("data");
    let metadata_dir = temp_dir.path().join("metadata");

    test_request_context()
        .scope(async {
            let catalog = make_cayenne_catalog(
                "cat_nw",
                &data_dir.to_string_lossy(),
                &metadata_dir.to_string_lossy(),
            );

            let app = AppBuilder::new("cayenne_ddl_dml_no_where")
                .with_catalog(catalog)
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder()
                .with_app(app)
                .with_resolved_cluster_config(test_cluster_config())
                .with_runtime_config(Config::default().with_caching_disabled())
                .build()
                .await;
            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(30)) => {
                    return Err("Timeout waiting for components to load".to_string());
                }
                () = cloned_rt.load_components() => {}
            }
            runtime_ready_check_with_timeout(&rt, Duration::from_secs(30)).await;

            exec(&rt, "CREATE SCHEMA cat_nw.s").await?;
            // PRIMARY KEY is required for delete-all / update-all on this branch:
            // the position-based deletion path does not yet support no-predicate
            // deletion without a PK (see CayenneTableProvider deletion sink).
            exec(
                &rt,
                "CREATE TABLE cat_nw.s.t (
                    id BIGINT NOT NULL,
                    v BIGINT NOT NULL,
                    PRIMARY KEY (id)
                ) PARTITION BY id",
            )
            .await?;
            exec(
                &rt,
                "INSERT INTO cat_nw.s.t VALUES (1, 10), (2, 20), (3, 30)",
            )
            .await?;

            // UPDATE with no WHERE — should touch every row.
            exec(&rt, "UPDATE cat_nw.s.t SET v = 99").await?;
            let batches = run_query(&rt, "SELECT id, v FROM cat_nw.s.t ORDER BY id").await?;
            assert_batches_eq!(
                &[
                    "+----+----+",
                    "| id | v  |",
                    "+----+----+",
                    "| 1  | 99 |",
                    "| 2  | 99 |",
                    "| 3  | 99 |",
                    "+----+----+",
                ],
                &batches
            );

            // DELETE with no WHERE — should empty the table.
            exec(&rt, "DELETE FROM cat_nw.s.t").await?;
            let count = query_scalar_i64(&rt, "SELECT COUNT(*) FROM cat_nw.s.t").await?;
            assert_eq!(count, 0, "DELETE FROM t (no WHERE) must empty the table");

            Ok(())
        })
        .await
}

// =============================================================================
// Test: DML filter references non-partition column (forces full-partition scan)
// =============================================================================
//
// All other DML tests filter on `id` where `PARTITION BY id` enables
// partition pruning. This test uses `PARTITION BY region` and filters on
// `sku` to exercise the no-pruning path.
#[tokio::test]
#[cfg_attr(
    not(feature = "spicebench"),
    ignore = "requires the spicebench feature"
)]
async fn cayenne_catalog_ddl_dml_non_partition_filter() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    let temp_dir = tempfile::tempdir().map_err(|e| e.to_string())?;
    let data_dir = temp_dir.path().join("data");
    let metadata_dir = temp_dir.path().join("metadata");

    test_request_context()
        .scope(async {
            let catalog = make_cayenne_catalog(
                "cat_npf",
                &data_dir.to_string_lossy(),
                &metadata_dir.to_string_lossy(),
            );

            let app = AppBuilder::new("cayenne_ddl_non_partition_filter")
                .with_catalog(catalog)
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder()
                .with_app(app)
                .with_resolved_cluster_config(test_cluster_config())
                .with_runtime_config(Config::default().with_caching_disabled())
                .build()
                .await;
            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(30)) => {
                    return Err("Timeout waiting for components to load".to_string());
                }
                () = cloned_rt.load_components() => {}
            }
            runtime_ready_check_with_timeout(&rt, Duration::from_secs(30)).await;

            exec(&rt, "CREATE SCHEMA cat_npf.s").await?;
            exec(
                &rt,
                "CREATE TABLE cat_npf.s.inv (
                    region VARCHAR NOT NULL,
                    sku VARCHAR NOT NULL,
                    qty BIGINT NOT NULL
                ) PARTITION BY region",
            )
            .await?;

            exec(
                &rt,
                "INSERT INTO cat_npf.s.inv VALUES
                    ('US', 'A', 10),
                    ('US', 'B', 20),
                    ('EU', 'A', 30),
                    ('EU', 'B', 40)",
            )
            .await?;

            // UPDATE filtering on non-partition column `sku`.
            exec(
                &rt,
                "UPDATE cat_npf.s.inv SET qty = qty + 1 WHERE sku = 'A'",
            )
            .await?;
            let batches = run_query(
                &rt,
                "SELECT region, sku, qty FROM cat_npf.s.inv ORDER BY region, sku",
            )
            .await?;
            // Only rows where sku='A' should change; both partitions updated.
            assert_batches_eq!(
                &[
                    "+--------+-----+-----+",
                    "| region | sku | qty |",
                    "+--------+-----+-----+",
                    "| EU     | A   | 31  |",
                    "| EU     | B   | 40  |",
                    "| US     | A   | 11  |",
                    "| US     | B   | 20  |",
                    "+--------+-----+-----+",
                ],
                &batches
            );

            // DELETE filtering on non-partition column `sku`.
            exec(&rt, "DELETE FROM cat_npf.s.inv WHERE sku = 'B'").await?;
            let batches = run_query(
                &rt,
                "SELECT region, sku, qty FROM cat_npf.s.inv ORDER BY region, sku",
            )
            .await?;
            assert_batches_eq!(
                &[
                    "+--------+-----+-----+",
                    "| region | sku | qty |",
                    "+--------+-----+-----+",
                    "| EU     | A   | 31  |",
                    "| US     | A   | 11  |",
                    "+--------+-----+-----+",
                ],
                &batches
            );

            Ok(())
        })
        .await
}

// =============================================================================
// Test: DML on a table partitioned by a VARCHAR column
// =============================================================================
//
// All other DML tests except the MERGE ones use `PARTITION BY id BIGINT`.
// This test exercises INSERT/UPDATE/DELETE on a string-partitioned table.
#[tokio::test]
#[cfg_attr(
    not(feature = "spicebench"),
    ignore = "requires the spicebench feature"
)]
async fn cayenne_catalog_ddl_string_partition_dml() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    let temp_dir = tempfile::tempdir().map_err(|e| e.to_string())?;
    let data_dir = temp_dir.path().join("data");
    let metadata_dir = temp_dir.path().join("metadata");

    test_request_context()
        .scope(async {
            let catalog = make_cayenne_catalog(
                "cat_strp",
                &data_dir.to_string_lossy(),
                &metadata_dir.to_string_lossy(),
            );

            let app = AppBuilder::new("cayenne_ddl_string_partition")
                .with_catalog(catalog)
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder()
                .with_app(app)
                .with_resolved_cluster_config(test_cluster_config())
                .with_runtime_config(Config::default().with_caching_disabled())
                .build()
                .await;
            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(30)) => {
                    return Err("Timeout waiting for components to load".to_string());
                }
                () = cloned_rt.load_components() => {}
            }
            runtime_ready_check_with_timeout(&rt, Duration::from_secs(30)).await;

            exec(&rt, "CREATE SCHEMA cat_strp.s").await?;
            exec(
                &rt,
                "CREATE TABLE cat_strp.s.events (
                    region VARCHAR NOT NULL,
                    id BIGINT NOT NULL,
                    payload VARCHAR
                ) PARTITION BY region",
            )
            .await?;

            exec(
                &rt,
                "INSERT INTO cat_strp.s.events VALUES
                    ('US', 1, 'a'),
                    ('US', 2, 'b'),
                    ('EU', 3, 'c'),
                    ('EU', 4, 'd'),
                    ('APAC', 5, 'e')",
            )
            .await?;

            // UPDATE using partition column in the filter.
            exec(
                &rt,
                "UPDATE cat_strp.s.events SET payload = 'X' WHERE region = 'US'",
            )
            .await?;
            let batches = run_query(
                &rt,
                "SELECT region, id, payload FROM cat_strp.s.events ORDER BY region, id",
            )
            .await?;
            assert_batches_eq!(
                &[
                    "+--------+----+---------+",
                    "| region | id | payload |",
                    "+--------+----+---------+",
                    "| APAC   | 5  | e       |",
                    "| EU     | 3  | c       |",
                    "| EU     | 4  | d       |",
                    "| US     | 1  | X       |",
                    "| US     | 2  | X       |",
                    "+--------+----+---------+",
                ],
                &batches
            );

            // DELETE an entire partition worth of rows.
            exec(&rt, "DELETE FROM cat_strp.s.events WHERE region = 'EU'").await?;
            let count = query_scalar_i64(&rt, "SELECT COUNT(*) FROM cat_strp.s.events").await?;
            assert_eq!(count, 3, "expected 3 rows after dropping EU partition");

            let batches = run_query(
                &rt,
                "SELECT region, id FROM cat_strp.s.events ORDER BY region, id",
            )
            .await?;
            assert_batches_eq!(
                &[
                    "+--------+----+",
                    "| region | id |",
                    "+--------+----+",
                    "| APAC   | 5  |",
                    "| US     | 1  |",
                    "| US     | 2  |",
                    "+--------+----+",
                ],
                &batches
            );

            Ok(())
        })
        .await
}

// =============================================================================
// Test: Cayenne catalog rejected in non-distributed (standalone) mode
// =============================================================================

#[tokio::test]
#[cfg_attr(
    not(feature = "spicebench"),
    ignore = "requires the spicebench feature"
)]
async fn cayenne_catalog_rejected_without_distributed_mode() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    let temp_dir = tempfile::tempdir().map_err(|e| e.to_string())?;
    let data_dir = temp_dir.path().join("data");
    let metadata_dir = temp_dir.path().join("metadata");

    test_request_context()
        .scope(async {
            let catalog = make_cayenne_catalog(
                "standalone_cat",
                &data_dir.to_string_lossy(),
                &metadata_dir.to_string_lossy(),
            );

            let app = AppBuilder::new("cayenne_standalone_reject")
                .with_catalog(catalog.clone())
                .build();

            configure_test_datafusion();
            // Build runtime WITHOUT cluster config (standalone / non-distributed mode).
            let rt = Runtime::builder()
                .with_app(app)
                .with_runtime_config(Config::default().with_caching_disabled())
                .build()
                .await;
            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(30)) => {
                    return Err("Timeout waiting for components to load".to_string());
                }
                () = cloned_rt.load_components() => {}
            }

            // The catalog should have failed to register with a configuration error.
            let statuses = rt.status().get_catalog_statuses();
            let status = statuses
                .get("standalone_cat")
                .ok_or("expected catalog 'standalone_cat' in status map")?;

            assert!(status.is_error(), "expected Error status, got: {status}");
            let err_msg = status
                .error_message()
                .ok_or("expected error message in catalog status")?;
            assert!(
                err_msg.contains("distributed"),
                "expected error about distributed mode, got: {err_msg}"
            );

            // The catalog should NOT be registered in DataFusion.
            assert!(
                rt.datafusion().ctx.catalog("standalone_cat").is_none(),
                "cayenne catalog should not be registered in standalone mode"
            );

            Ok(())
        })
        .await
}
