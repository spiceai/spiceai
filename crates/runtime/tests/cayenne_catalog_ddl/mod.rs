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
//! Validates that the Cayenne Catalog supports DataFrame DDL (CREATE TABLE via SQL),
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
use runtime::config::Config;
use spicepod::component::access::AccessMode;
use spicepod::component::catalog::Catalog;
use spicepod::param::Params;

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
                    age BIGINT
                )",
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
            let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
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
            // Step 6: UPDATE single row — modify a column value
            // -----------------------------------------------------------------
            exec(
                &rt,
                "UPDATE test_cat.myschema.users SET age = 31 WHERE id = 1",
            )
            .await?;

            // Alice's age should now be 31 (was 30).
            let batches = run_query(
                &rt,
                "SELECT id, name, age FROM test_cat.myschema.users WHERE id = 1",
            )
            .await?;
            assert_batches_eq!(
                &[
                    "+----+-------+-----+",
                    "| id | name  | age |",
                    "+----+-------+-----+",
                    "| 1  | Alice | 31  |",
                    "+----+-------+-----+",
                ],
                &batches
            );

            // Row count should remain the same after UPDATE.
            let count =
                query_scalar_i64(&rt, "SELECT COUNT(*) FROM test_cat.myschema.users").await?;
            assert_eq!(count, 4, "UPDATE should not change row count");

            // -----------------------------------------------------------------
            // Step 7: UPDATE multiple rows — bulk modification
            // -----------------------------------------------------------------
            exec(
                &rt,
                "UPDATE test_cat.myschema.users SET age = age + 10 WHERE age > 30",
            )
            .await?;

            // Alice(31→41), Diana(28 unchanged), Frank(40→50), Grace(33→43)
            let batches = run_query(
                &rt,
                "SELECT id, name, age FROM test_cat.myschema.users ORDER BY id",
            )
            .await?;
            assert_batches_eq!(
                &[
                    "+----+-------+-----+",
                    "| id | name  | age |",
                    "+----+-------+-----+",
                    "| 1  | Alice | 41  |",
                    "| 4  | Diana | 28  |",
                    "| 6  | Frank | 50  |",
                    "| 7  | Grace | 43  |",
                    "+----+-------+-----+",
                ],
                &batches
            );

            // -----------------------------------------------------------------
            // Step 8: UPDATE with NULL — set a column to NULL
            // -----------------------------------------------------------------
            exec(
                &rt,
                "UPDATE test_cat.myschema.users SET email = NULL WHERE id = 4",
            )
            .await?;

            let batches = run_query(
                &rt,
                "SELECT id, name, email FROM test_cat.myschema.users WHERE id = 4",
            )
            .await?;
            assert_batches_eq!(
                &[
                    "+----+-------+-------+",
                    "| id | name  | email |",
                    "+----+-------+-------+",
                    "| 4  | Diana |       |",
                    "+----+-------+-------+",
                ],
                &batches
            );

            // -----------------------------------------------------------------
            // Step 9: UPDATE multiple columns at once
            // -----------------------------------------------------------------
            exec(
                &rt,
                "UPDATE test_cat.myschema.users SET name = 'Grace Updated', age = 99 WHERE id = 7",
            )
            .await?;

            let batches = run_query(
                &rt,
                "SELECT id, name, age FROM test_cat.myschema.users WHERE id = 7",
            )
            .await?;
            assert_batches_eq!(
                &[
                    "+----+---------------+-----+",
                    "| id | name          | age |",
                    "+----+---------------+-----+",
                    "| 7  | Grace Updated | 99  |",
                    "+----+---------------+-----+",
                ],
                &batches
            );

            // Restore ages for subsequent steps: Alice=41, Diana=28, Frank=50, Grace=99
            // Overall state: 4 rows

            // -----------------------------------------------------------------
            // Step 10: INSERT after UPDATE — verify correctness
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
            // Step 11: Aggregation queries — validate computations
            // -----------------------------------------------------------------
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
            // Remaining: Alice(41), Diana(28), Frank(50), Grace Updated(99), Heidi(29)
            // Average = (41 + 28 + 50 + 99 + 29) / 5 = 247 / 5 = 49.4
            assert!(
                (avg_age - 49.4).abs() < f64::EPSILON,
                "Expected AVG(age) = 49.4, got {avg_age}"
            );

            let max_age =
                query_scalar_i64(&rt, "SELECT MAX(age) FROM test_cat.myschema.users").await?;
            assert_eq!(max_age, 99, "Expected MAX(age) = 99 (Grace Updated)");

            let min_age =
                query_scalar_i64(&rt, "SELECT MIN(age) FROM test_cat.myschema.users").await?;
            assert_eq!(min_age, 28, "Expected MIN(age) = 28 (Diana)");

            let sum_age =
                query_scalar_i64(&rt, "SELECT SUM(age) FROM test_cat.myschema.users").await?;
            assert_eq!(sum_age, 247, "Expected SUM(age) = 247");

            // -----------------------------------------------------------------
            // Step 12: NULL handling validation
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
            // Ivan has NULL email, Diana has NULL email (set in Step 8); all others have emails.
            assert_eq!(
                count_email, 4,
                "COUNT(email) should be 4 (Ivan and Diana have NULL email)"
            );

            // Query rows where email IS NULL.
            let batches = run_query(
                &rt,
                "SELECT id, name FROM test_cat.myschema.users WHERE email IS NULL ORDER BY id",
            )
            .await?;
            assert_batches_eq!(
                &[
                    "+----+-------+",
                    "| id | name  |",
                    "+----+-------+",
                    "| 4  | Diana |",
                    "| 9  | Ivan  |",
                    "+----+-------+",
                ],
                &batches
            );

            // -----------------------------------------------------------------
            // Step 13: DELETE all remaining rows
            // -----------------------------------------------------------------
            exec(&rt, "DELETE FROM test_cat.myschema.users WHERE true").await?;

            let count =
                query_scalar_i64(&rt, "SELECT COUNT(*) FROM test_cat.myschema.users").await?;
            assert_eq!(count, 0, "Table should be empty after DELETE WHERE true");

            // -----------------------------------------------------------------
            // Step 14: INSERT into empty table after full delete
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
                "CREATE TABLE cat_idempotent.s1.t1 (id BIGINT NOT NULL, val BIGINT)",
            )
            .await?;

            // Insert a row.
            exec(&rt, "INSERT INTO cat_idempotent.s1.t1 VALUES (1, 100)").await?;

            // CREATE TABLE IF NOT EXISTS should not fail or drop data.
            exec(
                &rt,
                "CREATE TABLE IF NOT EXISTS cat_idempotent.s1.t1 (id BIGINT NOT NULL, val BIGINT)",
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
                )",
            )
            .await?;

            exec(
                &rt,
                "CREATE TABLE cat_multi.store.orders (
                    order_id BIGINT NOT NULL,
                    product_id BIGINT NOT NULL,
                    quantity BIGINT NOT NULL
                )",
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
                "CREATE TABLE cat_drop.ns.ephemeral (id BIGINT NOT NULL)",
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
                "CREATE TABLE cat_drop.ns.ephemeral (id BIGINT NOT NULL, val VARCHAR)",
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
// Test: Multiple schemas in the same catalog
// =============================================================================

#[tokio::test]
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
                "CREATE TABLE cat_schemas.finance.records (id BIGINT NOT NULL, amount DOUBLE)",
            )
            .await?;
            exec(
                &rt,
                "CREATE TABLE cat_schemas.hr.records (id BIGINT NOT NULL, employee VARCHAR)",
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
