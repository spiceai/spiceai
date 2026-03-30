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

//! Integration tests for Cayenne Catalog DDL and DML operations in a distributed cluster.
//!
//! Validates that the Cayenne Catalog supports DDL (CREATE SCHEMA, CREATE TABLE, DROP TABLE),
//! DML (INSERT, UPDATE, DELETE), and query operations when running on a scheduler node
//! within a distributed Spice cluster (scheduler + executors).
//!
//! These tests verify that the cluster infrastructure (mTLS, executor management, scheduler
//! server) does not interfere with catalog operations, and that DDL/DML flows correctly
//! through the scheduler's DataFusion context in cluster mode.

use std::collections::HashMap;
use std::time::Duration;

use app::AppBuilder;
use arrow::array::{Int64Array, RecordBatch};
use datafusion::assert_batches_eq;
use spicepod::component::access::AccessMode;
use spicepod::component::catalog::Catalog;
use spicepod::param::Params;

use crate::{configure_test_datafusion, init_tracing, utils::test_request_context};

use super::harness::ClusterHarness;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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

/// Extract a single i64 scalar from the first column of the first batch.
fn scalar_i64(batches: &[RecordBatch]) -> Result<i64, anyhow::Error> {
    let batch = batches
        .first()
        .ok_or_else(|| anyhow::anyhow!("no batches returned"))?;
    let col = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| anyhow::anyhow!("expected Int64Array in first column"))?;
    Ok(col.value(0))
}

/// Total number of rows across all batches.
fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

// =============================================================================
// Test: Full DDL/DML lifecycle in cluster mode
// =============================================================================

/// Tests the complete lifecycle of Cayenne catalog DDL and DML operations when the
/// runtime is running as a scheduler in a distributed cluster with one executor.
///
/// Steps:
/// 1. CREATE SCHEMA and CREATE TABLE via SQL DDL
/// 2. INSERT rows and verify with SELECT
/// 3. UPDATE rows (single, bulk, NULL, multi-column) and verify
/// 4. DELETE rows (single, range filter, all) and verify
/// 5. INSERT after full delete to verify table is still usable
/// 6. DROP TABLE and verify it is inaccessible, then re-create
#[tokio::test(flavor = "multi_thread")]
#[cfg(not(target_os = "windows"))]
async fn test_distributed_cayenne_ddl_lifecycle() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    let temp_dir = tempfile::tempdir()?;
    let data_dir = temp_dir.path().join("data");
    let metadata_dir = temp_dir.path().join("metadata");

    test_request_context()
        .scope(async {
            configure_test_datafusion();

            let catalog = make_cayenne_catalog(
                "tcat",
                &data_dir.to_string_lossy(),
                &metadata_dir.to_string_lossy(),
            );

            let app = AppBuilder::new("distributed_cayenne_ddl_lifecycle")
                .with_catalog(catalog)
                .build();

            let harness = ClusterHarness::builder()
                .scheduler(app)
                .executors(1)
                .start()
                .await?;

            harness.wait_for_executors(Duration::from_secs(15)).await?;

            // -----------------------------------------------------------------
            // Step 1: CREATE SCHEMA + CREATE TABLE
            // -----------------------------------------------------------------
            harness.query("CREATE SCHEMA tcat.myschema").await?;

            harness
                .query(
                    "CREATE TABLE tcat.myschema.users (
                        id BIGINT NOT NULL,
                        name VARCHAR NOT NULL,
                        email VARCHAR,
                        age BIGINT
                    )",
                )
                .await?;

            // Verify the table appears in information_schema.
            let info_batches = harness
                .query(
                    "SELECT table_catalog, table_schema, table_name
                     FROM information_schema.tables
                     WHERE table_catalog = 'tcat' AND table_name = 'users'",
                )
                .await?;
            assert_eq!(
                total_rows(&info_batches),
                1,
                "users table should appear in information_schema"
            );

            // Verify table is empty.
            let count = scalar_i64(
                &harness
                    .query("SELECT COUNT(*) FROM tcat.myschema.users")
                    .await?,
            )?;
            assert_eq!(count, 0, "table should be empty after creation");

            // -----------------------------------------------------------------
            // Step 2: INSERT rows
            // -----------------------------------------------------------------
            harness
                .query(
                    "INSERT INTO tcat.myschema.users VALUES
                        (1, 'Alice',   'alice@example.com',   30),
                        (2, 'Bob',     'bob@example.com',     25),
                        (3, 'Charlie', 'charlie@example.com', 35),
                        (4, 'Diana',   'diana@example.com',   28),
                        (5, 'Eve',     NULL,                  22)",
                )
                .await?;

            let count = scalar_i64(
                &harness
                    .query("SELECT COUNT(*) FROM tcat.myschema.users")
                    .await?,
            )?;
            assert_eq!(count, 5, "expected 5 rows after insert");

            let batches = harness
                .query("SELECT id, name, email, age FROM tcat.myschema.users ORDER BY id")
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
            // Step 3: UPDATE — single row
            // -----------------------------------------------------------------
            harness
                .query("UPDATE tcat.myschema.users SET age = 31 WHERE id = 1")
                .await?;

            let batches = harness
                .query("SELECT id, name, age FROM tcat.myschema.users WHERE id = 1")
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

            // Row count unchanged after UPDATE.
            let count = scalar_i64(
                &harness
                    .query("SELECT COUNT(*) FROM tcat.myschema.users")
                    .await?,
            )?;
            assert_eq!(count, 5, "UPDATE should not change row count");

            // -----------------------------------------------------------------
            // Step 4: UPDATE — bulk modification
            // -----------------------------------------------------------------
            harness
                .query("UPDATE tcat.myschema.users SET age = age + 10 WHERE age > 30")
                .await?;

            // Alice(31→41), Charlie(35→45); Bob(25), Diana(28), Eve(22) unchanged.
            let batches = harness
                .query("SELECT id, name, age FROM tcat.myschema.users ORDER BY id")
                .await?;
            assert_batches_eq!(
                &[
                    "+----+---------+-----+",
                    "| id | name    | age |",
                    "+----+---------+-----+",
                    "| 1  | Alice   | 41  |",
                    "| 2  | Bob     | 25  |",
                    "| 3  | Charlie | 45  |",
                    "| 4  | Diana   | 28  |",
                    "| 5  | Eve     | 22  |",
                    "+----+---------+-----+",
                ],
                &batches
            );

            // -----------------------------------------------------------------
            // Step 5: UPDATE — set column to NULL
            // -----------------------------------------------------------------
            harness
                .query("UPDATE tcat.myschema.users SET email = NULL WHERE id = 4")
                .await?;

            let batches = harness
                .query("SELECT id, name, email FROM tcat.myschema.users WHERE id = 4")
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
            // Step 6: DELETE — single row
            // -----------------------------------------------------------------
            harness
                .query("DELETE FROM tcat.myschema.users WHERE id = 3")
                .await?;

            let count = scalar_i64(
                &harness
                    .query("SELECT COUNT(*) FROM tcat.myschema.users")
                    .await?,
            )?;
            assert_eq!(count, 4, "expected 4 rows after deleting id=3");

            // Verify deleted row is gone.
            let batches = harness
                .query("SELECT id FROM tcat.myschema.users WHERE id = 3")
                .await?;
            assert_eq!(total_rows(&batches), 0, "id=3 should no longer exist");

            // -----------------------------------------------------------------
            // Step 7: DELETE — range filter
            // -----------------------------------------------------------------
            harness
                .query("DELETE FROM tcat.myschema.users WHERE age < 26")
                .await?;

            // Bob(25) and Eve(22) should be deleted, leaving Alice(41) and Diana(28).
            let count = scalar_i64(
                &harness
                    .query("SELECT COUNT(*) FROM tcat.myschema.users")
                    .await?,
            )?;
            assert_eq!(count, 2, "expected 2 rows after deleting age < 26");

            let batches = harness
                .query("SELECT id, name FROM tcat.myschema.users ORDER BY id")
                .await?;
            assert_batches_eq!(
                &[
                    "+----+-------+",
                    "| id | name  |",
                    "+----+-------+",
                    "| 1  | Alice |",
                    "| 4  | Diana |",
                    "+----+-------+",
                ],
                &batches
            );

            // -----------------------------------------------------------------
            // Step 8: DELETE — all remaining rows
            // -----------------------------------------------------------------
            harness
                .query("DELETE FROM tcat.myschema.users WHERE true")
                .await?;

            let count = scalar_i64(
                &harness
                    .query("SELECT COUNT(*) FROM tcat.myschema.users")
                    .await?,
            )?;
            assert_eq!(count, 0, "table should be empty after DELETE WHERE true");

            // -----------------------------------------------------------------
            // Step 9: INSERT into empty table after full delete
            // -----------------------------------------------------------------
            harness
                .query(
                    "INSERT INTO tcat.myschema.users VALUES (100, 'Zara', 'zara@example.com', 45)",
                )
                .await?;

            let batches = harness
                .query("SELECT id, name, email, age FROM tcat.myschema.users ORDER BY id")
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

            // -----------------------------------------------------------------
            // Step 10: DROP TABLE and re-create
            // -----------------------------------------------------------------
            harness.query("DROP TABLE tcat.myschema.users").await?;

            // Querying the dropped table should fail.
            let result = harness.query("SELECT * FROM tcat.myschema.users").await;
            assert!(
                result.is_err(),
                "querying a dropped table should produce an error"
            );

            // DROP TABLE IF EXISTS on a non-existent table should succeed.
            harness
                .query("DROP TABLE IF EXISTS tcat.myschema.users")
                .await?;

            // Re-create with different schema and insert.
            harness
                .query("CREATE TABLE tcat.myschema.users (id BIGINT NOT NULL, val VARCHAR)")
                .await?;

            harness
                .query("INSERT INTO tcat.myschema.users VALUES (10, 'recreated')")
                .await?;

            let batches = harness
                .query("SELECT id, val FROM tcat.myschema.users ORDER BY id")
                .await?;
            assert_batches_eq!(
                &[
                    "+----+-----------+",
                    "| id | val       |",
                    "+----+-----------+",
                    "| 10 | recreated |",
                    "+----+-----------+",
                ],
                &batches
            );

            harness.shutdown().await;
            Ok(())
        })
        .await
}

// =============================================================================
// Test: Multi-table JOIN through a cayenne catalog in cluster mode
// =============================================================================

/// Tests that multiple tables in a Cayenne catalog can be created, populated, and
/// joined in a distributed cluster environment.
///
/// Verifies:
/// - Two tables in the same schema can coexist
/// - Cross-table JOINs produce correct results
/// - DELETE on one table does not affect the other
/// - Aggregations over JOINed data are correct
#[tokio::test(flavor = "multi_thread")]
#[cfg(not(target_os = "windows"))]
async fn test_distributed_cayenne_multi_table_join() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    let temp_dir = tempfile::tempdir()?;
    let data_dir = temp_dir.path().join("data");
    let metadata_dir = temp_dir.path().join("metadata");

    test_request_context()
        .scope(async {
            configure_test_datafusion();

            let catalog = make_cayenne_catalog(
                "jcat",
                &data_dir.to_string_lossy(),
                &metadata_dir.to_string_lossy(),
            );

            let app = AppBuilder::new("distributed_cayenne_join")
                .with_catalog(catalog)
                .build();

            let harness = ClusterHarness::builder()
                .scheduler(app)
                .executors(1)
                .start()
                .await?;

            harness.wait_for_executors(Duration::from_secs(15)).await?;

            // Create schema and two related tables.
            harness.query("CREATE SCHEMA jcat.store").await?;

            harness
                .query(
                    "CREATE TABLE jcat.store.products (
                        product_id BIGINT NOT NULL,
                        name VARCHAR NOT NULL,
                        price DOUBLE NOT NULL
                    )",
                )
                .await?;

            harness
                .query(
                    "CREATE TABLE jcat.store.orders (
                        order_id BIGINT NOT NULL,
                        product_id BIGINT NOT NULL,
                        quantity BIGINT NOT NULL
                    )",
                )
                .await?;

            // Populate both tables.
            harness
                .query(
                    "INSERT INTO jcat.store.products VALUES
                        (1, 'Widget',  9.99),
                        (2, 'Gadget', 19.99),
                        (3, 'Gizmo',  14.50)",
                )
                .await?;

            harness
                .query(
                    "INSERT INTO jcat.store.orders VALUES
                        (100, 1, 5),
                        (101, 2, 2),
                        (102, 1, 3),
                        (103, 3, 1)",
                )
                .await?;

            // Validate independent counts.
            let product_count = scalar_i64(
                &harness
                    .query("SELECT COUNT(*) FROM jcat.store.products")
                    .await?,
            )?;
            assert_eq!(product_count, 3);

            let order_count = scalar_i64(
                &harness
                    .query("SELECT COUNT(*) FROM jcat.store.orders")
                    .await?,
            )?;
            assert_eq!(order_count, 4);

            // Cross-table JOIN with aggregation.
            let batches = harness
                .query(
                    "SELECT p.name, SUM(o.quantity) as total_qty
                     FROM jcat.store.orders o
                     JOIN jcat.store.products p ON o.product_id = p.product_id
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

            // Delete from orders and verify JOIN still correct.
            harness
                .query("DELETE FROM jcat.store.orders WHERE order_id = 100")
                .await?;

            let batches = harness
                .query(
                    "SELECT p.name, SUM(o.quantity) as total_qty
                     FROM jcat.store.orders o
                     JOIN jcat.store.products p ON o.product_id = p.product_id
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

            // Products table should be unaffected by order deletion.
            let product_count = scalar_i64(
                &harness
                    .query("SELECT COUNT(*) FROM jcat.store.products")
                    .await?,
            )?;
            assert_eq!(
                product_count, 3,
                "products should be unaffected by order deletion"
            );

            harness.shutdown().await;
            Ok(())
        })
        .await
}

// =============================================================================
// Test: Schema isolation in a cayenne catalog in cluster mode
// =============================================================================

/// Tests that multiple schemas in a single Cayenne catalog are properly isolated
/// in a distributed cluster: operations on one schema do not affect another.
///
/// Also tests same-named tables in different schemas to verify namespace correctness.
#[tokio::test(flavor = "multi_thread")]
#[cfg(not(target_os = "windows"))]
async fn test_distributed_cayenne_schema_isolation() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    let temp_dir = tempfile::tempdir()?;
    let data_dir = temp_dir.path().join("data");
    let metadata_dir = temp_dir.path().join("metadata");

    test_request_context()
        .scope(async {
            configure_test_datafusion();

            let catalog = make_cayenne_catalog(
                "scat",
                &data_dir.to_string_lossy(),
                &metadata_dir.to_string_lossy(),
            );

            let app = AppBuilder::new("distributed_cayenne_schema_isolation")
                .with_catalog(catalog)
                .build();

            let harness = ClusterHarness::builder()
                .scheduler(app)
                .executors(1)
                .start()
                .await?;

            harness.wait_for_executors(Duration::from_secs(15)).await?;

            // Create two separate schemas.
            harness.query("CREATE SCHEMA scat.finance").await?;
            harness.query("CREATE SCHEMA scat.hr").await?;

            // Create tables with the same name in different schemas.
            harness
                .query("CREATE TABLE scat.finance.records (id BIGINT NOT NULL, amount DOUBLE)")
                .await?;
            harness
                .query("CREATE TABLE scat.hr.records (id BIGINT NOT NULL, employee VARCHAR)")
                .await?;

            // Insert data into both.
            harness
                .query("INSERT INTO scat.finance.records VALUES (1, 1000.50), (2, 2500.75)")
                .await?;
            harness
                .query("INSERT INTO scat.hr.records VALUES (1, 'Alice'), (2, 'Bob')")
                .await?;

            // Validate isolation — each schema has its own data and columns.
            let batches = harness
                .query("SELECT id, amount FROM scat.finance.records ORDER BY id")
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

            let batches = harness
                .query("SELECT id, employee FROM scat.hr.records ORDER BY id")
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
            harness
                .query("DELETE FROM scat.finance.records WHERE id = 1")
                .await?;

            let finance_count = scalar_i64(
                &harness
                    .query("SELECT COUNT(*) FROM scat.finance.records")
                    .await?,
            )?;
            assert_eq!(finance_count, 1, "finance.records should have 1 row");

            let hr_count = scalar_i64(
                &harness
                    .query("SELECT COUNT(*) FROM scat.hr.records")
                    .await?,
            )?;
            assert_eq!(hr_count, 2, "hr.records should still have 2 rows");

            // Drop one table, verify the other still works.
            harness.query("DROP TABLE scat.finance.records").await?;

            let hr_batches = harness
                .query("SELECT id, employee FROM scat.hr.records ORDER BY id")
                .await?;
            assert_eq!(
                total_rows(&hr_batches),
                2,
                "hr.records should still be queryable after dropping finance.records"
            );

            harness.shutdown().await;
            Ok(())
        })
        .await
}

// =============================================================================
// Test: Primary key upsert behavior in cluster mode
// =============================================================================

/// Tests that CREATE TABLE with PRIMARY KEY and upsert-on-conflict behavior
/// works correctly through the distributed cluster scheduler.
///
/// Verifies:
/// - INSERT with conflicting PKs replaces existing rows (upsert)
/// - Pure upsert (all conflicts) preserves row count
/// - DELETE on PK tables works correctly
#[tokio::test(flavor = "multi_thread")]
#[cfg(not(target_os = "windows"))]
async fn test_distributed_cayenne_primary_key_upsert() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    let temp_dir = tempfile::tempdir()?;
    let data_dir = temp_dir.path().join("data");
    let metadata_dir = temp_dir.path().join("metadata");

    test_request_context()
        .scope(async {
            configure_test_datafusion();

            let catalog = make_cayenne_catalog(
                "pkcat",
                &data_dir.to_string_lossy(),
                &metadata_dir.to_string_lossy(),
            );

            let app = AppBuilder::new("distributed_cayenne_pk_upsert")
                .with_catalog(catalog)
                .build();

            let harness = ClusterHarness::builder()
                .scheduler(app)
                .executors(1)
                .start()
                .await?;

            harness.wait_for_executors(Duration::from_secs(15)).await?;

            harness.query("CREATE SCHEMA pkcat.myschema").await?;

            harness
                .query(
                    "CREATE TABLE pkcat.myschema.users (
                        id BIGINT NOT NULL,
                        name VARCHAR NOT NULL,
                        email VARCHAR,
                        PRIMARY KEY (id)
                    )",
                )
                .await?;

            // Initial insert.
            harness
                .query(
                    "INSERT INTO pkcat.myschema.users VALUES
                        (1, 'Alice',   'alice@example.com'),
                        (2, 'Bob',     'bob@example.com'),
                        (3, 'Charlie', 'charlie@example.com')",
                )
                .await?;

            let count = scalar_i64(
                &harness
                    .query("SELECT COUNT(*) FROM pkcat.myschema.users")
                    .await?,
            )?;
            assert_eq!(count, 3, "expected 3 rows after initial insert");

            // Insert with conflicting PKs — should upsert.
            harness
                .query(
                    "INSERT INTO pkcat.myschema.users VALUES
                        (2, 'Bob Updated', 'bob_new@example.com'),
                        (4, 'Diana',       'diana@example.com')",
                )
                .await?;

            // Bob replaced, Diana added → 4 rows.
            let count = scalar_i64(
                &harness
                    .query("SELECT COUNT(*) FROM pkcat.myschema.users")
                    .await?,
            )?;
            assert_eq!(count, 4, "expected 4 rows after upsert");

            let batches = harness
                .query("SELECT id, name, email FROM pkcat.myschema.users ORDER BY id")
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

            // Pure upsert — all conflicting PKs, no new rows.
            harness
                .query(
                    "INSERT INTO pkcat.myschema.users VALUES
                        (1, 'Alice V2',   'alice_v2@example.com'),
                        (3, 'Charlie V2', 'charlie_v2@example.com')",
                )
                .await?;

            let count = scalar_i64(
                &harness
                    .query("SELECT COUNT(*) FROM pkcat.myschema.users")
                    .await?,
            )?;
            assert_eq!(count, 4, "row count should remain 4 after pure upsert");

            let batches = harness
                .query("SELECT id, name, email FROM pkcat.myschema.users ORDER BY id")
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

            // DELETE still works on PK table.
            harness
                .query("DELETE FROM pkcat.myschema.users WHERE id = 2")
                .await?;

            let count = scalar_i64(
                &harness
                    .query("SELECT COUNT(*) FROM pkcat.myschema.users")
                    .await?,
            )?;
            assert_eq!(count, 3, "expected 3 rows after delete");

            let batches = harness
                .query("SELECT id, name FROM pkcat.myschema.users ORDER BY id")
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

            harness.shutdown().await;
            Ok(())
        })
        .await
}

// =============================================================================
// Test: NULL handling and aggregations in cluster mode
// =============================================================================

/// Tests that NULL handling and aggregation queries produce correct results
/// for Cayenne catalog tables in a distributed cluster.
///
/// Verifies:
/// - COUNT(*) vs COUNT(column) with NULLs
/// - AVG, MIN, MAX, SUM with NULL values
/// - WHERE IS NULL / IS NOT NULL filtering
#[tokio::test(flavor = "multi_thread")]
#[cfg(not(target_os = "windows"))]
async fn test_distributed_cayenne_null_handling_and_aggregations() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    let temp_dir = tempfile::tempdir()?;
    let data_dir = temp_dir.path().join("data");
    let metadata_dir = temp_dir.path().join("metadata");

    test_request_context()
        .scope(async {
            configure_test_datafusion();

            let catalog = make_cayenne_catalog(
                "ncat",
                &data_dir.to_string_lossy(),
                &metadata_dir.to_string_lossy(),
            );

            let app = AppBuilder::new("distributed_cayenne_null_agg")
                .with_catalog(catalog)
                .build();

            let harness = ClusterHarness::builder()
                .scheduler(app)
                .executors(1)
                .start()
                .await?;

            harness.wait_for_executors(Duration::from_secs(15)).await?;

            harness.query("CREATE SCHEMA ncat.ns").await?;

            harness
                .query(
                    "CREATE TABLE ncat.ns.metrics (
                        id BIGINT NOT NULL,
                        label VARCHAR,
                        value BIGINT
                    )",
                )
                .await?;

            harness
                .query(
                    "INSERT INTO ncat.ns.metrics VALUES
                        (1, 'alpha',  10),
                        (2, 'beta',   20),
                        (3, NULL,     30),
                        (4, 'delta',  NULL),
                        (5, NULL,     NULL)",
                )
                .await?;

            // COUNT(*) counts all rows; COUNT(label) and COUNT(value) exclude NULLs.
            let count_star = scalar_i64(
                &harness
                    .query("SELECT COUNT(*) FROM ncat.ns.metrics")
                    .await?,
            )?;
            assert_eq!(count_star, 5, "COUNT(*) should be 5");

            let count_label = scalar_i64(
                &harness
                    .query("SELECT COUNT(label) FROM ncat.ns.metrics")
                    .await?,
            )?;
            assert_eq!(
                count_label, 3,
                "COUNT(label) should be 3 (ids 3 and 5 have NULL label)"
            );

            let count_value = scalar_i64(
                &harness
                    .query("SELECT COUNT(value) FROM ncat.ns.metrics")
                    .await?,
            )?;
            assert_eq!(
                count_value, 3,
                "COUNT(value) should be 3 (ids 4 and 5 have NULL value)"
            );

            // SUM, MIN, MAX should skip NULLs.
            let sum_value = scalar_i64(
                &harness
                    .query("SELECT SUM(value) FROM ncat.ns.metrics")
                    .await?,
            )?;
            assert_eq!(sum_value, 60, "SUM(value) should be 10+20+30 = 60");

            let min_value = scalar_i64(
                &harness
                    .query("SELECT MIN(value) FROM ncat.ns.metrics")
                    .await?,
            )?;
            assert_eq!(min_value, 10, "MIN(value) should be 10");

            let max_value = scalar_i64(
                &harness
                    .query("SELECT MAX(value) FROM ncat.ns.metrics")
                    .await?,
            )?;
            assert_eq!(max_value, 30, "MAX(value) should be 30");

            // WHERE IS NULL / IS NOT NULL.
            let batches = harness
                .query("SELECT id FROM ncat.ns.metrics WHERE label IS NULL ORDER BY id")
                .await?;
            assert_batches_eq!(
                &["+----+", "| id |", "+----+", "| 3  |", "| 5  |", "+----+",],
                &batches
            );

            let batches = harness
                .query("SELECT id FROM ncat.ns.metrics WHERE value IS NOT NULL ORDER BY id")
                .await?;
            assert_batches_eq!(
                &[
                    "+----+", "| id |", "+----+", "| 1  |", "| 2  |", "| 3  |", "+----+",
                ],
                &batches
            );

            harness.shutdown().await;
            Ok(())
        })
        .await
}
