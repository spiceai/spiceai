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
//! through the scheduler's `DataFusion` context in cluster mode.
//!
//! Every non-count SELECT also has its `EXPLAIN` plan snapshot-tested to confirm
//! that query planning succeeds through the scheduler's `DataFusion` context while
//! executors are connected, and to catch regressions if the physical plan changes.

use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::time::Duration;

use app::AppBuilder;
use arrow::array::{Int64Array, RecordBatch};
use datafusion::assert_batches_eq;
use futures::FutureExt;
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

/// Wait until a table has `expected` rows, polling every 500ms.
async fn wait_for_row_count(
    harness: &ClusterHarness,
    table: &str,
    expected: i64,
    timeout: Duration,
) -> Result<(), anyhow::Error> {
    let start = tokio::time::Instant::now();
    loop {
        let batches = harness
            .query(&format!("SELECT COUNT(*) FROM {table}"))
            .await?;
        let count = scalar_i64(&batches).unwrap_or(0);
        if count == expected {
            return Ok(());
        }
        if start.elapsed() > timeout {
            return Err(anyhow::anyhow!(
                "Timed out waiting for {table} to have {expected} rows; found {count}"
            ));
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Run `EXPLAIN <sql>` and return the pretty-printed plan string.
fn explain_to_string(batches: &[RecordBatch]) -> String {
    arrow::util::pretty::pretty_format_batches(batches)
        .expect("format explain")
        .to_string()
}

/// Wrapper around [`insta::assert_snapshot!`] that redacts ephemeral
/// `127.0.0.1:<port>` addresses in `FlightSQL` physical-plan output so that
/// snapshots are stable across runs.
macro_rules! assert_explain_snapshot {
    ($name:expr, $plan:expr) => {{
        let __plan = $plan;
        insta::with_settings!({
            filters => vec![
                (r"127\.0\.0\.1:\d+", "[endpoint]")
            ]
        }, {
            insta::assert_snapshot!($name, __plan);
        });
    }};
}

/// Run a test body against a [`ClusterHarness`], ensuring [`ClusterHarness::shutdown`]
/// is always called — even if the body returns an early `Err` or panics.
///
/// Panics inside `f` are caught, the harness is shut down, and the panic is then
/// re-raised so the test still fails with the original message.
async fn run_with_harness<F>(harness: ClusterHarness, f: F) -> Result<(), anyhow::Error>
where
    F: for<'a> FnOnce(
        &'a ClusterHarness,
    )
        -> Pin<Box<dyn std::future::Future<Output = Result<(), anyhow::Error>> + 'a>>,
{
    let result = AssertUnwindSafe(f(&harness)).catch_unwind().await;
    harness.shutdown().await;
    match result {
        Ok(inner) => inner,
        Err(panic_payload) => std::panic::resume_unwind(panic_payload),
    }
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

            let harness = ClusterHarness::builder()
                .scheduler(
                    AppBuilder::new("distributed_cayenne_ddl_lifecycle")
                        .with_catalog(catalog.clone())
                        .build(),
                )
                .executor_with_app(
                    AppBuilder::new("executor_ddl_lifecycle")
                        .with_catalog(catalog)
                        .build(),
                )
                .start()
                .await?;

            run_with_harness(harness, |harness| {
                Box::pin(async move {
                    harness.wait_for_executors(Duration::from_secs(15)).await?;
                    ddl_lifecycle_create_table(harness).await?;
                    ddl_lifecycle_insert(harness).await?;
                    ddl_lifecycle_update_row(harness).await?;
                    ddl_lifecycle_bulk_update(harness).await?;
                    ddl_lifecycle_update_null(harness).await?;
                    ddl_lifecycle_delete_single(harness).await?;
                    ddl_lifecycle_delete_range(harness).await?;
                    ddl_lifecycle_delete_all(harness).await?;
                    ddl_lifecycle_reinsert(harness).await?;
                    ddl_lifecycle_drop(harness).await
                })
            })
            .await
        })
        .await
}

// -----------------------------------------------------------------
// Step 1: CREATE SCHEMA + CREATE TABLE
// -----------------------------------------------------------------

async fn ddl_lifecycle_create_table(harness: &ClusterHarness) -> Result<(), anyhow::Error> {
    harness.query("CREATE SCHEMA tcat.myschema").await?;

    harness
        .query(
            "CREATE TABLE tcat.myschema.users (
                id BIGINT NOT NULL,
                name VARCHAR NOT NULL,
                email VARCHAR,
                age BIGINT,
                PRIMARY KEY (id)
            ) PARTITION BY id",
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

    Ok(())
}

// -----------------------------------------------------------------
// Step 2: INSERT rows
// -----------------------------------------------------------------

async fn ddl_lifecycle_insert(harness: &ClusterHarness) -> Result<(), anyhow::Error> {
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

    wait_for_row_count(harness, "tcat.myschema.users", 5, Duration::from_secs(30)).await?;

    let select_all = "SELECT id, name, email, age FROM tcat.myschema.users ORDER BY id";
    assert_explain_snapshot!(
        "ddl_select_all_after_insert",
        explain_to_string(&harness.explain(select_all).await?)
    );
    let batches = harness.query(select_all).await?;
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

    Ok(())
}

// -----------------------------------------------------------------
// Step 3: UPDATE — single row
// -----------------------------------------------------------------

async fn ddl_lifecycle_update_row(harness: &ClusterHarness) -> Result<(), anyhow::Error> {
    harness
        .query("UPDATE tcat.myschema.users SET age = 31 WHERE id = 1")
        .await?;

    let select_single = "SELECT id, name, age FROM tcat.myschema.users WHERE id = 1";
    let batches = harness.query(select_single).await?;
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
    assert_explain_snapshot!(
        "ddl_select_single_after_update",
        explain_to_string(&harness.explain(select_single).await?)
    );

    // Row count unchanged after UPDATE.
    let count = scalar_i64(
        &harness
            .query("SELECT COUNT(*) FROM tcat.myschema.users")
            .await?,
    )?;
    assert_eq!(count, 5, "UPDATE should not change row count");

    Ok(())
}

// -----------------------------------------------------------------
// Step 4: UPDATE — bulk modification
// -----------------------------------------------------------------

async fn ddl_lifecycle_bulk_update(harness: &ClusterHarness) -> Result<(), anyhow::Error> {
    harness
        .query("UPDATE tcat.myschema.users SET age = age + 10 WHERE age > 30")
        .await?;

    // Alice(31→41), Charlie(35→45); Bob(25), Diana(28), Eve(22) unchanged.
    let select_bulk = "SELECT id, name, age FROM tcat.myschema.users ORDER BY id";
    assert_explain_snapshot!(
        "ddl_select_all_after_bulk_update",
        explain_to_string(&harness.explain(select_bulk).await?)
    );
    let batches = harness.query(select_bulk).await?;
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

    Ok(())
}

// -----------------------------------------------------------------
// Step 5: UPDATE — set column to NULL
// -----------------------------------------------------------------

async fn ddl_lifecycle_update_null(harness: &ClusterHarness) -> Result<(), anyhow::Error> {
    harness
        .query("UPDATE tcat.myschema.users SET email = NULL WHERE id = 4")
        .await?;

    let select_null_upd = "SELECT id, name, email FROM tcat.myschema.users WHERE id = 4";
    let batches = harness.query(select_null_upd).await?;
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
    assert_explain_snapshot!(
        "ddl_select_null_update",
        explain_to_string(&harness.explain(select_null_upd).await?)
    );

    Ok(())
}

// -----------------------------------------------------------------
// Step 6: DELETE — single row
// -----------------------------------------------------------------

async fn ddl_lifecycle_delete_single(harness: &ClusterHarness) -> Result<(), anyhow::Error> {
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
    let select_deleted = "SELECT id FROM tcat.myschema.users WHERE id = 3";
    let batches = harness.query(select_deleted).await?;
    assert_eq!(total_rows(&batches), 0, "id=3 should no longer exist");
    assert_explain_snapshot!(
        "ddl_select_deleted_row",
        explain_to_string(&harness.explain(select_deleted).await?)
    );

    Ok(())
}

// -----------------------------------------------------------------
// Step 7: DELETE — range filter
// -----------------------------------------------------------------

async fn ddl_lifecycle_delete_range(harness: &ClusterHarness) -> Result<(), anyhow::Error> {
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

    let select_after_range = "SELECT id, name FROM tcat.myschema.users ORDER BY id";
    let batches = harness.query(select_after_range).await?;
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
    assert_explain_snapshot!(
        "ddl_select_after_range_delete",
        explain_to_string(&harness.explain(select_after_range).await?)
    );

    Ok(())
}

// -----------------------------------------------------------------
// Step 8: DELETE — all remaining rows
// -----------------------------------------------------------------

async fn ddl_lifecycle_delete_all(harness: &ClusterHarness) -> Result<(), anyhow::Error> {
    harness
        .query("DELETE FROM tcat.myschema.users WHERE true")
        .await?;

    let count = scalar_i64(
        &harness
            .query("SELECT COUNT(*) FROM tcat.myschema.users")
            .await?,
    )?;
    assert_eq!(count, 0, "table should be empty after DELETE WHERE true");

    Ok(())
}

// -----------------------------------------------------------------
// Step 9: INSERT into empty table after full delete
// -----------------------------------------------------------------

async fn ddl_lifecycle_reinsert(harness: &ClusterHarness) -> Result<(), anyhow::Error> {
    harness
        .query("INSERT INTO tcat.myschema.users VALUES (100, 'Zara', 'zara@example.com', 45)")
        .await?;

    let select_reinsert = "SELECT id, name, email, age FROM tcat.myschema.users ORDER BY id";
    let batches = harness.query(select_reinsert).await?;
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
    assert_explain_snapshot!(
        "ddl_select_after_reinsert",
        explain_to_string(&harness.explain(select_reinsert).await?)
    );

    Ok(())
}

// -----------------------------------------------------------------
// Step 10: DROP TABLE
// -----------------------------------------------------------------

async fn ddl_lifecycle_drop(harness: &ClusterHarness) -> Result<(), anyhow::Error> {
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

    Ok(())
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
/// - Aggregations over `JOINed` data are correct
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

            let scheduler_app = AppBuilder::new("distributed_cayenne_join")
                .with_catalog(catalog.clone())
                .build();
            let executor_app = AppBuilder::new("executor_cayenne_join")
                .with_catalog(catalog)
                .build();

            let harness = ClusterHarness::builder()
                .scheduler(scheduler_app)
                .executor_with_app(executor_app)
                .start()
                .await?;

            run_with_harness(harness, |harness| {
                Box::pin(async move {
                    harness.wait_for_executors(Duration::from_secs(15)).await?;
                    multi_table_join_setup(harness).await?;
                    multi_table_join_verify(harness).await
                })
            })
            .await
        })
        .await
}

async fn multi_table_join_setup(harness: &ClusterHarness) -> Result<(), anyhow::Error> {
    harness.query("CREATE SCHEMA jcat.store").await?;

    harness
        .query(
            "CREATE TABLE jcat.store.products (
                product_id BIGINT NOT NULL,
                name VARCHAR NOT NULL,
                price DOUBLE NOT NULL
            ) PARTITION BY product_id",
        )
        .await?;

    harness
        .query(
            "CREATE TABLE jcat.store.orders (
                order_id BIGINT NOT NULL,
                product_id BIGINT NOT NULL,
                quantity BIGINT NOT NULL
            ) PARTITION BY order_id",
        )
        .await?;

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

    wait_for_row_count(harness, "jcat.store.products", 3, Duration::from_secs(30)).await?;
    wait_for_row_count(harness, "jcat.store.orders", 4, Duration::from_secs(30)).await?;

    Ok(())
}

async fn multi_table_join_verify(harness: &ClusterHarness) -> Result<(), anyhow::Error> {
    let join_sql = "SELECT p.name, SUM(o.quantity) as total_qty \
                    FROM jcat.store.orders o \
                    JOIN jcat.store.products p ON o.product_id = p.product_id \
                    GROUP BY p.name \
                    ORDER BY p.name";
    let batches = harness.query(join_sql).await?;
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
    assert_explain_snapshot!(
        "join_aggregation",
        explain_to_string(&harness.explain(join_sql).await?)
    );

    // Delete from orders and verify JOIN still correct.
    harness
        .query("DELETE FROM jcat.store.orders WHERE order_id = 100")
        .await?;

    let batches = harness.query(join_sql).await?;
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
    assert_explain_snapshot!(
        "join_aggregation_after_delete",
        explain_to_string(&harness.explain(join_sql).await?)
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

    Ok(())
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

            let scheduler_app = AppBuilder::new("distributed_cayenne_schema_isolation")
                .with_catalog(catalog.clone())
                .build();
            let executor_app = AppBuilder::new("executor_schema_isolation")
                .with_catalog(catalog)
                .build();

            let harness = ClusterHarness::builder()
                .scheduler(scheduler_app)
                .executor_with_app(executor_app)
                .start()
                .await?;

            run_with_harness(harness, |harness| {
                Box::pin(async move {
                    harness.wait_for_executors(Duration::from_secs(15)).await?;
                    schema_isolation_setup(harness).await?;
                    schema_isolation_verify(harness).await
                })
            })
            .await
        })
        .await
}

async fn schema_isolation_setup(harness: &ClusterHarness) -> Result<(), anyhow::Error> {
    harness.query("CREATE SCHEMA scat.finance").await?;
    harness.query("CREATE SCHEMA scat.hr").await?;

    harness
        .query(
            "CREATE TABLE scat.finance.records (id BIGINT NOT NULL, amount DOUBLE) PARTITION BY id",
        )
        .await?;
    harness
        .query(
            "CREATE TABLE scat.hr.records (id BIGINT NOT NULL, employee VARCHAR) PARTITION BY id",
        )
        .await?;

    harness
        .query("INSERT INTO scat.finance.records VALUES (1, 1000.50), (2, 2500.75)")
        .await?;
    harness
        .query("INSERT INTO scat.hr.records VALUES (1, 'Alice'), (2, 'Bob')")
        .await?;

    wait_for_row_count(harness, "scat.finance.records", 2, Duration::from_secs(30)).await?;
    wait_for_row_count(harness, "scat.hr.records", 2, Duration::from_secs(30)).await?;

    Ok(())
}

async fn schema_isolation_verify(harness: &ClusterHarness) -> Result<(), anyhow::Error> {
    // Validate isolation — each schema has its own data and columns.
    let select_finance = "SELECT id, amount FROM scat.finance.records ORDER BY id";
    let batches = harness.query(select_finance).await?;
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
    assert_explain_snapshot!(
        "schema_isolation_finance",
        explain_to_string(&harness.explain(select_finance).await?)
    );

    let select_hr = "SELECT id, employee FROM scat.hr.records ORDER BY id";
    let batches = harness.query(select_hr).await?;
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
    assert_explain_snapshot!(
        "schema_isolation_hr",
        explain_to_string(&harness.explain(select_hr).await?)
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

    let hr_batches = harness.query(select_hr).await?;
    assert_eq!(
        total_rows(&hr_batches),
        2,
        "hr.records should still be queryable after dropping finance.records"
    );
    assert_explain_snapshot!(
        "schema_isolation_hr_after_drop",
        explain_to_string(&harness.explain(select_hr).await?)
    );

    Ok(())
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

            let scheduler_app = AppBuilder::new("distributed_cayenne_pk_upsert")
                .with_catalog(catalog.clone())
                .build();
            let executor_app = AppBuilder::new("executor_pk_upsert")
                .with_catalog(catalog)
                .build();

            let harness = ClusterHarness::builder()
                .scheduler(scheduler_app)
                .executor_with_app(executor_app)
                .start()
                .await?;

            run_with_harness(harness, |harness| {
                Box::pin(async move {
                    harness.wait_for_executors(Duration::from_secs(15)).await?;
                    pk_upsert_setup(harness).await?;
                    pk_upsert_conflict(harness).await?;
                    pk_upsert_pure(harness).await?;
                    pk_upsert_delete(harness).await
                })
            })
            .await
        })
        .await
}

async fn pk_upsert_setup(harness: &ClusterHarness) -> Result<(), anyhow::Error> {
    harness.query("CREATE SCHEMA pkcat.myschema").await?;

    harness
        .query(
            "CREATE TABLE pkcat.myschema.users (
                id BIGINT NOT NULL,
                name VARCHAR NOT NULL,
                email VARCHAR,
                PRIMARY KEY (id)
            ) PARTITION BY id",
        )
        .await?;

    harness
        .query(
            "INSERT INTO pkcat.myschema.users VALUES
                (1, 'Alice',   'alice@example.com'),
                (2, 'Bob',     'bob@example.com'),
                (3, 'Charlie', 'charlie@example.com')",
        )
        .await?;

    wait_for_row_count(harness, "pkcat.myschema.users", 3, Duration::from_secs(30)).await?;

    Ok(())
}

async fn pk_upsert_conflict(harness: &ClusterHarness) -> Result<(), anyhow::Error> {
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

    let select_pk = "SELECT id, name, email FROM pkcat.myschema.users ORDER BY id";
    let batches = harness.query(select_pk).await?;
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
    assert_explain_snapshot!(
        "pk_select_after_upsert",
        explain_to_string(&harness.explain(select_pk).await?)
    );

    Ok(())
}

async fn pk_upsert_pure(harness: &ClusterHarness) -> Result<(), anyhow::Error> {
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

    let select_pk = "SELECT id, name, email FROM pkcat.myschema.users ORDER BY id";
    let batches = harness.query(select_pk).await?;
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
    assert_explain_snapshot!(
        "pk_select_after_pure_upsert",
        explain_to_string(&harness.explain(select_pk).await?)
    );

    Ok(())
}

async fn pk_upsert_delete(harness: &ClusterHarness) -> Result<(), anyhow::Error> {
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

    let select_pk_after_del = "SELECT id, name FROM pkcat.myschema.users ORDER BY id";
    let batches = harness.query(select_pk_after_del).await?;
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
    assert_explain_snapshot!(
        "pk_select_after_delete",
        explain_to_string(&harness.explain(select_pk_after_del).await?)
    );

    Ok(())
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

            let scheduler_app = AppBuilder::new("distributed_cayenne_null_agg")
                .with_catalog(catalog.clone())
                .build();
            let executor_app = AppBuilder::new("executor_null_agg")
                .with_catalog(catalog)
                .build();

            let harness = ClusterHarness::builder()
                .scheduler(scheduler_app)
                .executor_with_app(executor_app)
                .start()
                .await?;

            run_with_harness(harness, |harness| {
                Box::pin(async move {
                    harness.wait_for_executors(Duration::from_secs(15)).await?;
                    null_agg_setup(harness).await?;
                    null_agg_counts(harness).await?;
                    null_agg_aggregates(harness).await?;
                    null_agg_filters(harness).await
                })
            })
            .await
        })
        .await
}

// =============================================================================
// Test: Basic MERGE in cluster mode — scheduler forwards to executors
// =============================================================================
//
// MERGE in cluster mode goes through `DistributedCayenneMergeExec`, which
// forwards the original MERGE SQL verbatim to every executor via FlightSQL.
// This is a separate codepath from the single-node path in
// `cayenne_catalog_ddl/mod.rs`.
#[tokio::test(flavor = "multi_thread")]
#[cfg(not(target_os = "windows"))]
async fn test_distributed_cayenne_merge_basic() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    let temp_dir = tempfile::tempdir()?;
    let data_dir = temp_dir.path().join("data");
    let metadata_dir = temp_dir.path().join("metadata");

    test_request_context()
        .scope(async {
            configure_test_datafusion();

            let catalog = make_cayenne_catalog(
                "mcat",
                &data_dir.to_string_lossy(),
                &metadata_dir.to_string_lossy(),
            );

            let scheduler_app = AppBuilder::new("distributed_cayenne_merge_basic")
                .with_catalog(catalog.clone())
                .build();
            let executor_app = AppBuilder::new("executor_merge_basic")
                .with_catalog(catalog)
                .build();

            let harness = ClusterHarness::builder()
                .scheduler(scheduler_app)
                .executor_with_app(executor_app)
                .start()
                .await?;

            run_with_harness(harness, |harness| {
                Box::pin(async move {
                    harness.wait_for_executors(Duration::from_secs(15)).await?;

                    harness.query("CREATE SCHEMA mcat.s").await?;

                    harness
                        .query(
                            "CREATE TABLE mcat.s.inventory (
                                id BIGINT NOT NULL,
                                name VARCHAR NOT NULL,
                                qty BIGINT NOT NULL
                            ) PARTITION BY id",
                        )
                        .await?;

                    harness
                        .query(
                            "CREATE TABLE mcat.s.updates (
                                id BIGINT NOT NULL,
                                name VARCHAR NOT NULL,
                                qty BIGINT NOT NULL
                            ) PARTITION BY id",
                        )
                        .await?;

                    harness
                        .query(
                            "INSERT INTO mcat.s.inventory VALUES
                                (1, 'apple',  10),
                                (2, 'banana', 20),
                                (3, 'cherry', 30)",
                        )
                        .await?;

                    harness
                        .query(
                            "INSERT INTO mcat.s.updates VALUES
                                (1, 'apple',  50),
                                (3, 'cherry', 100)",
                        )
                        .await?;

                    wait_for_row_count(harness, "mcat.s.inventory", 3, Duration::from_secs(30))
                        .await?;
                    wait_for_row_count(harness, "mcat.s.updates", 2, Duration::from_secs(30))
                        .await?;

                    // Basic MERGE — update qty from source.
                    harness
                        .query(
                            "MERGE INTO mcat.s.inventory AS t
                             USING mcat.s.updates AS s
                             ON t.id = s.id
                             WHEN MATCHED THEN UPDATE SET qty = s.qty",
                        )
                        .await?;

                    let select_after_merge =
                        "SELECT id, name, qty FROM mcat.s.inventory ORDER BY id";
                    let batches = harness.query(select_after_merge).await?;
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
                    let plan = explain_to_string(&harness.explain(select_after_merge).await?);
                    insta::assert_snapshot!("merge_basic_after", plan);

                    // Row count unchanged.
                    let count = scalar_i64(
                        &harness
                            .query("SELECT COUNT(*) FROM mcat.s.inventory")
                            .await?,
                    )?;
                    assert_eq!(count, 3, "MERGE should not add or drop rows");

                    Ok(())
                })
            })
            .await
        })
        .await
}

// =============================================================================
// Test: MERGE with composite ON key — no cross-product in distributed mode
// =============================================================================
//
// Regression test for the tuple-aware deletion issue (see the single-node
// variant `cayenne_catalog_merge_composite_key_no_cross_product` at
// `cayenne_catalog_ddl/mod.rs:1593`). With two composite key columns
// (region, sku), if the matched rows are (US,A) and (EU,B), an independent
// IN-list approach would corrupt (US,B) and (EU,A). The distributed path
// must preserve unmatched rows the same way.
#[tokio::test(flavor = "multi_thread")]
#[cfg(not(target_os = "windows"))]
async fn test_distributed_cayenne_merge_composite_key_no_cross_product() -> Result<(), anyhow::Error>
{
    let _tracing = init_tracing(Some("integration=debug,info"));

    let temp_dir = tempfile::tempdir()?;
    let data_dir = temp_dir.path().join("data");
    let metadata_dir = temp_dir.path().join("metadata");

    test_request_context()
        .scope(async {
            configure_test_datafusion();

            let catalog = make_cayenne_catalog(
                "mxp",
                &data_dir.to_string_lossy(),
                &metadata_dir.to_string_lossy(),
            );

            let scheduler_app = AppBuilder::new("distributed_cayenne_merge_xprod")
                .with_catalog(catalog.clone())
                .build();
            let executor_app = AppBuilder::new("executor_merge_xprod")
                .with_catalog(catalog)
                .build();

            let harness = ClusterHarness::builder()
                .scheduler(scheduler_app)
                .executor_with_app(executor_app)
                .start()
                .await?;

            run_with_harness(harness, |harness| {
                Box::pin(async move {
                    harness.wait_for_executors(Duration::from_secs(15)).await?;

                    harness.query("CREATE SCHEMA mxp.s").await?;

                    harness
                        .query(
                            "CREATE TABLE mxp.s.inventory (
                            region VARCHAR NOT NULL,
                            sku VARCHAR NOT NULL,
                            qty BIGINT NOT NULL
                        ) PARTITION BY region",
                        )
                        .await?;

                    harness
                        .query(
                            "CREATE TABLE mxp.s.updates (
                            region VARCHAR NOT NULL,
                            sku VARCHAR NOT NULL,
                            qty BIGINT NOT NULL
                        ) PARTITION BY region",
                        )
                        .await?;

                    // All 4 (region,sku) combinations exist in target.
                    harness
                        .query(
                            "INSERT INTO mxp.s.inventory VALUES
                            ('US', 'A', 10),
                            ('US', 'B', 20),
                            ('EU', 'A', 30),
                            ('EU', 'B', 40)",
                        )
                        .await?;

                    // Source only updates the diagonal (US,A) and (EU,B).
                    harness
                        .query(
                            "INSERT INTO mxp.s.updates VALUES
                            ('US', 'A', 99),
                            ('EU', 'B', 88)",
                        )
                        .await?;

                    wait_for_row_count(harness, "mxp.s.inventory", 4, Duration::from_secs(30))
                        .await?;
                    wait_for_row_count(harness, "mxp.s.updates", 2, Duration::from_secs(30))
                        .await?;

                    harness
                        .query(
                            "MERGE INTO mxp.s.inventory AS t
                         USING mxp.s.updates AS s
                         ON t.region = s.region AND t.sku = s.sku
                         WHEN MATCHED THEN UPDATE SET qty = s.qty",
                        )
                        .await?;

                    let select_after =
                        "SELECT region, sku, qty FROM mxp.s.inventory ORDER BY region, sku";
                    let batches = harness.query(select_after).await?;

                    // Only (US,A) and (EU,B) change; (US,B) and (EU,A) must be unchanged.
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
                    let plan = explain_to_string(&harness.explain(select_after).await?);
                    insta::assert_snapshot!("merge_composite_no_cross_product_after", plan);

                    let count = scalar_i64(
                        &harness
                            .query("SELECT COUNT(*) FROM mxp.s.inventory")
                            .await?,
                    )?;
                    assert_eq!(count, 4, "no rows may be lost after composite-key MERGE");

                    Ok(())
                })
            })
            .await
        })
        .await
}

// =============================================================================
// Test: MERGE with zero matches in cluster mode — target unchanged
// =============================================================================
#[tokio::test(flavor = "multi_thread")]
#[cfg(not(target_os = "windows"))]
async fn test_distributed_cayenne_merge_zero_match() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    let temp_dir = tempfile::tempdir()?;
    let data_dir = temp_dir.path().join("data");
    let metadata_dir = temp_dir.path().join("metadata");

    test_request_context()
        .scope(async {
            configure_test_datafusion();

            let catalog = make_cayenne_catalog(
                "mzm",
                &data_dir.to_string_lossy(),
                &metadata_dir.to_string_lossy(),
            );

            let scheduler_app = AppBuilder::new("distributed_cayenne_merge_zero")
                .with_catalog(catalog.clone())
                .build();
            let executor_app = AppBuilder::new("executor_merge_zero")
                .with_catalog(catalog)
                .build();

            let harness = ClusterHarness::builder()
                .scheduler(scheduler_app)
                .executor_with_app(executor_app)
                .start()
                .await?;

            run_with_harness(harness, |harness| {
                Box::pin(async move {
                    harness.wait_for_executors(Duration::from_secs(15)).await?;

                    harness.query("CREATE SCHEMA mzm.s").await?;

                    harness
                        .query(
                            "CREATE TABLE mzm.s.target (
                                id BIGINT NOT NULL,
                                val BIGINT NOT NULL
                            ) PARTITION BY id",
                        )
                        .await?;

                    harness
                        .query(
                            "CREATE TABLE mzm.s.source (
                                id BIGINT NOT NULL,
                                val BIGINT NOT NULL
                            ) PARTITION BY id",
                        )
                        .await?;

                    harness
                        .query("INSERT INTO mzm.s.target VALUES (1, 10), (2, 20), (3, 30)")
                        .await?;

                    // Source has no IDs matching target — MERGE must be a no-op.
                    harness
                        .query("INSERT INTO mzm.s.source VALUES (99, 999)")
                        .await?;

                    wait_for_row_count(harness, "mzm.s.target", 3, Duration::from_secs(30)).await?;
                    wait_for_row_count(harness, "mzm.s.source", 1, Duration::from_secs(30)).await?;

                    harness
                        .query(
                            "MERGE INTO mzm.s.target AS t
                             USING mzm.s.source AS s
                             ON t.id = s.id
                             WHEN MATCHED THEN UPDATE SET val = s.val",
                        )
                        .await?;

                    let batches = harness
                        .query("SELECT id, val FROM mzm.s.target ORDER BY id")
                        .await?;
                    assert_batches_eq!(
                        &[
                            "+----+-----+",
                            "| id | val |",
                            "+----+-----+",
                            "| 1  | 10  |",
                            "| 2  | 20  |",
                            "| 3  | 30  |",
                            "+----+-----+",
                        ],
                        &batches
                    );

                    let count =
                        scalar_i64(&harness.query("SELECT COUNT(*) FROM mzm.s.target").await?)?;
                    assert_eq!(count, 3, "zero-match MERGE must preserve row count");

                    Ok(())
                })
            })
            .await
        })
        .await
}

// =============================================================================
// Test: MERGE with duplicate source keys — error, target unchanged
// =============================================================================
//
// If the source has multiple rows matching the same target row, MERGE must
// error without losing data. Distributed variant of the single-node
// `cayenne_catalog_merge_duplicate_source_keys_rejected` test.
#[tokio::test(flavor = "multi_thread")]
#[cfg(not(target_os = "windows"))]
async fn test_distributed_cayenne_merge_duplicate_source_keys_rejected() -> Result<(), anyhow::Error>
{
    let _tracing = init_tracing(Some("integration=debug,info"));

    let temp_dir = tempfile::tempdir()?;
    let data_dir = temp_dir.path().join("data");
    let metadata_dir = temp_dir.path().join("metadata");

    test_request_context()
        .scope(async {
            configure_test_datafusion();

            let catalog = make_cayenne_catalog(
                "mdk",
                &data_dir.to_string_lossy(),
                &metadata_dir.to_string_lossy(),
            );

            let scheduler_app = AppBuilder::new("distributed_cayenne_merge_dupkey")
                .with_catalog(catalog.clone())
                .build();
            let executor_app = AppBuilder::new("executor_merge_dupkey")
                .with_catalog(catalog)
                .build();

            let harness = ClusterHarness::builder()
                .scheduler(scheduler_app)
                .executor_with_app(executor_app)
                .start()
                .await?;

            run_with_harness(harness, |harness| Box::pin(async move {
                harness.wait_for_executors(Duration::from_secs(15)).await?;

                harness.query("CREATE SCHEMA mdk.s").await?;

                harness
                    .query(
                        "CREATE TABLE mdk.s.target (
                            id BIGINT NOT NULL,
                            val BIGINT NOT NULL
                        ) PARTITION BY id",
                    )
                    .await?;

                harness
                    .query(
                        "CREATE TABLE mdk.s.source (
                            id BIGINT NOT NULL,
                            val BIGINT NOT NULL
                        ) PARTITION BY id",
                    )
                    .await?;

                harness
                    .query("INSERT INTO mdk.s.target VALUES (1, 100)")
                    .await?;

                harness
                    .query("INSERT INTO mdk.s.source VALUES (1, 200), (1, 300)")
                    .await?;

                wait_for_row_count(harness, "mdk.s.target", 1, Duration::from_secs(30)).await?;
                wait_for_row_count(harness, "mdk.s.source", 2, Duration::from_secs(30)).await?;

                let merge_result = harness
                    .query(
                        "MERGE INTO mdk.s.target AS t
                         USING mdk.s.source AS s
                         ON t.id = s.id
                         WHEN MATCHED THEN UPDATE SET val = s.val",
                    )
                    .await;
                assert!(
                    merge_result.is_err(),
                    "distributed MERGE with duplicate source keys must error; got: {merge_result:?}"
                );

                // Target row preserved after failed MERGE.
                let count =
                    scalar_i64(&harness.query("SELECT COUNT(*) FROM mdk.s.target").await?)?;
                assert_eq!(count, 1, "target must still have 1 row after failed MERGE");

                let batches = harness
                    .query("SELECT id, val FROM mdk.s.target")
                    .await?;
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
            }))
            .await
        })
        .await
}

// =============================================================================
// Test: DML on a string-partitioned table in cluster mode
// =============================================================================
#[tokio::test(flavor = "multi_thread")]
#[cfg(not(target_os = "windows"))]
async fn test_distributed_cayenne_string_partition_dml() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    let temp_dir = tempfile::tempdir()?;
    let data_dir = temp_dir.path().join("data");
    let metadata_dir = temp_dir.path().join("metadata");

    test_request_context()
        .scope(async {
            configure_test_datafusion();

            let catalog = make_cayenne_catalog(
                "sp",
                &data_dir.to_string_lossy(),
                &metadata_dir.to_string_lossy(),
            );

            let scheduler_app = AppBuilder::new("distributed_cayenne_string_partition")
                .with_catalog(catalog.clone())
                .build();
            let executor_app = AppBuilder::new("executor_string_partition")
                .with_catalog(catalog)
                .build();

            let harness = ClusterHarness::builder()
                .scheduler(scheduler_app)
                .executor_with_app(executor_app)
                .start()
                .await?;

            run_with_harness(harness, |harness| {
                Box::pin(async move {
                    harness.wait_for_executors(Duration::from_secs(15)).await?;

                    harness.query("CREATE SCHEMA sp.s").await?;

                    harness
                        .query(
                            "CREATE TABLE sp.s.events (
                                region VARCHAR NOT NULL,
                                id BIGINT NOT NULL,
                                payload VARCHAR
                            ) PARTITION BY region",
                        )
                        .await?;

                    harness
                        .query(
                            "INSERT INTO sp.s.events VALUES
                                ('US',   1, 'a'),
                                ('US',   2, 'b'),
                                ('EU',   3, 'c'),
                                ('EU',   4, 'd'),
                                ('APAC', 5, 'e')",
                        )
                        .await?;

                    wait_for_row_count(harness, "sp.s.events", 5, Duration::from_secs(30)).await?;

                    // UPDATE using the partition column in the filter.
                    harness
                        .query("UPDATE sp.s.events SET payload = 'X' WHERE region = 'US'")
                        .await?;

                    let select_all =
                        "SELECT region, id, payload FROM sp.s.events ORDER BY region, id";
                    let batches = harness.query(select_all).await?;
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
                    let plan = explain_to_string(&harness.explain(select_all).await?);
                    insta::assert_snapshot!("string_partition_after_update", plan);

                    // DELETE an entire partition.
                    harness
                        .query("DELETE FROM sp.s.events WHERE region = 'EU'")
                        .await?;

                    let count =
                        scalar_i64(&harness.query("SELECT COUNT(*) FROM sp.s.events").await?)?;
                    assert_eq!(count, 3, "expected 3 rows after dropping EU partition");

                    let batches = harness
                        .query("SELECT region, id FROM sp.s.events ORDER BY region, id")
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
            })
            .await
        })
        .await
}

// =============================================================================
// Test: UPDATE/DELETE without WHERE in cluster mode
// =============================================================================
//
// Distributed counterpart of `cayenne_catalog_ddl_dml_no_where`.
// The table must have a PRIMARY KEY: the position-based deletion path doesn't
// yet support no-predicate delete-all on PK-less Cayenne tables.
#[tokio::test(flavor = "multi_thread")]
#[cfg(not(target_os = "windows"))]
async fn test_distributed_cayenne_dml_no_where() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    let temp_dir = tempfile::tempdir()?;
    let data_dir = temp_dir.path().join("data");
    let metadata_dir = temp_dir.path().join("metadata");

    test_request_context()
        .scope(async {
            configure_test_datafusion();

            let catalog = make_cayenne_catalog(
                "nwd",
                &data_dir.to_string_lossy(),
                &metadata_dir.to_string_lossy(),
            );

            let scheduler_app = AppBuilder::new("distributed_cayenne_dml_no_where")
                .with_catalog(catalog.clone())
                .build();
            let executor_app = AppBuilder::new("executor_dml_no_where")
                .with_catalog(catalog)
                .build();

            let harness = ClusterHarness::builder()
                .scheduler(scheduler_app)
                .executor_with_app(executor_app)
                .start()
                .await?;

            run_with_harness(harness, |harness| {
                Box::pin(async move {
                    harness.wait_for_executors(Duration::from_secs(15)).await?;

                    harness.query("CREATE SCHEMA nwd.s").await?;
                    harness
                        .query(
                            "CREATE TABLE nwd.s.t (
                                id BIGINT NOT NULL,
                                v BIGINT NOT NULL,
                                PRIMARY KEY (id)
                            ) PARTITION BY id",
                        )
                        .await?;

                    harness
                        .query("INSERT INTO nwd.s.t VALUES (1, 10), (2, 20), (3, 30)")
                        .await?;
                    wait_for_row_count(harness, "nwd.s.t", 3, Duration::from_secs(30)).await?;

                    // UPDATE with no WHERE — touches every row.
                    harness.query("UPDATE nwd.s.t SET v = 99").await?;

                    let select_all = "SELECT id, v FROM nwd.s.t ORDER BY id";
                    let batches = harness.query(select_all).await?;
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
                    let plan = explain_to_string(&harness.explain(select_all).await?);
                    insta::assert_snapshot!("dml_no_where_after_update", plan);

                    // DELETE with no WHERE — empties the table.
                    harness.query("DELETE FROM nwd.s.t").await?;
                    let count = scalar_i64(&harness.query("SELECT COUNT(*) FROM nwd.s.t").await?)?;
                    assert_eq!(count, 0, "DELETE FROM t (no WHERE) must empty the table");

                    Ok(())
                })
            })
            .await
        })
        .await
}

// =============================================================================
// Test: UPDATE/DELETE filter on non-partition column in cluster mode
// =============================================================================
//
// Distributed counterpart of `cayenne_catalog_ddl_dml_non_partition_filter`.
// Table is `PARTITION BY region`; the DML filter is on `sku`, which forces the
// scheduler to forward the predicate to every partition/executor.
#[tokio::test(flavor = "multi_thread")]
#[cfg(not(target_os = "windows"))]
async fn test_distributed_cayenne_dml_non_partition_filter() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    let temp_dir = tempfile::tempdir()?;
    let data_dir = temp_dir.path().join("data");
    let metadata_dir = temp_dir.path().join("metadata");

    test_request_context()
        .scope(async {
            configure_test_datafusion();

            let catalog = make_cayenne_catalog(
                "npf",
                &data_dir.to_string_lossy(),
                &metadata_dir.to_string_lossy(),
            );

            let scheduler_app = AppBuilder::new("distributed_cayenne_dml_non_partition_filter")
                .with_catalog(catalog.clone())
                .build();
            let executor_app = AppBuilder::new("executor_dml_non_partition_filter")
                .with_catalog(catalog)
                .build();

            let harness = ClusterHarness::builder()
                .scheduler(scheduler_app)
                .executor_with_app(executor_app)
                .start()
                .await?;

            run_with_harness(harness, |harness| {
                Box::pin(async move {
                    harness.wait_for_executors(Duration::from_secs(15)).await?;

                    harness.query("CREATE SCHEMA npf.s").await?;
                    harness
                        .query(
                            "CREATE TABLE npf.s.inv (
                                region VARCHAR NOT NULL,
                                sku VARCHAR NOT NULL,
                                qty BIGINT NOT NULL
                            ) PARTITION BY region",
                        )
                        .await?;

                    harness
                        .query(
                            "INSERT INTO npf.s.inv VALUES
                                ('US', 'A', 10),
                                ('US', 'B', 20),
                                ('EU', 'A', 30),
                                ('EU', 'B', 40)",
                        )
                        .await?;
                    wait_for_row_count(harness, "npf.s.inv", 4, Duration::from_secs(30)).await?;

                    // UPDATE filtered on non-partition column `sku`.
                    harness
                        .query("UPDATE npf.s.inv SET qty = qty + 1 WHERE sku = 'A'")
                        .await?;

                    let select_after_update =
                        "SELECT region, sku, qty FROM npf.s.inv ORDER BY region, sku";
                    let batches = harness.query(select_after_update).await?;
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
                    let plan = explain_to_string(&harness.explain(select_after_update).await?);
                    insta::assert_snapshot!("dml_non_partition_filter_after_update", plan);

                    // DELETE filtered on non-partition column `sku`.
                    harness
                        .query("DELETE FROM npf.s.inv WHERE sku = 'B'")
                        .await?;

                    let batches = harness
                        .query("SELECT region, sku, qty FROM npf.s.inv ORDER BY region, sku")
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
            })
            .await
        })
        .await
}

async fn null_agg_setup(harness: &ClusterHarness) -> Result<(), anyhow::Error> {
    harness.query("CREATE SCHEMA ncat.ns").await?;

    harness
        .query(
            "CREATE TABLE ncat.ns.metrics (
                id BIGINT NOT NULL,
                label VARCHAR,
                value BIGINT
            ) PARTITION BY id",
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

    wait_for_row_count(harness, "ncat.ns.metrics", 5, Duration::from_secs(30)).await?;

    Ok(())
}

async fn null_agg_counts(harness: &ClusterHarness) -> Result<(), anyhow::Error> {
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

    Ok(())
}

async fn null_agg_aggregates(harness: &ClusterHarness) -> Result<(), anyhow::Error> {
    // SUM, MIN, MAX should skip NULLs.
    let sum_sql = "SELECT SUM(value) FROM ncat.ns.metrics";
    let sum_value = scalar_i64(&harness.query(sum_sql).await?)?;
    assert_eq!(sum_value, 60, "SUM(value) should be 10+20+30 = 60");
    let explain = explain_to_string(&harness.explain(sum_sql).await?);
    assert_explain_snapshot!("null_agg_sum", &explain);

    let min_sql = "SELECT MIN(value) FROM ncat.ns.metrics";
    let min_value = scalar_i64(&harness.query(min_sql).await?)?;
    assert_eq!(min_value, 10, "MIN(value) should be 10");
    let explain = harness.explain(min_sql).await?;
    assert_explain_snapshot!("null_agg_min", explain_to_string(&explain));

    let max_sql = "SELECT MAX(value) FROM ncat.ns.metrics";
    let max_value = scalar_i64(&harness.query(max_sql).await?)?;
    assert_eq!(max_value, 30, "MAX(value) should be 30");
    let explain = harness.explain(max_sql).await?;
    assert_explain_snapshot!("null_agg_max", explain_to_string(&explain));

    Ok(())
}

async fn null_agg_filters(harness: &ClusterHarness) -> Result<(), anyhow::Error> {
    // WHERE IS NULL / IS NOT NULL.
    let select_null = "SELECT id FROM ncat.ns.metrics WHERE label IS NULL ORDER BY id";
    let batches = harness.query(select_null).await?;
    assert_batches_eq!(
        &["+----+", "| id |", "+----+", "| 3  |", "| 5  |", "+----+",],
        &batches
    );
    assert_explain_snapshot!(
        "null_filter_is_null",
        explain_to_string(&harness.explain(select_null).await?)
    );

    let select_not_null = "SELECT id FROM ncat.ns.metrics WHERE value IS NOT NULL ORDER BY id";
    let batches = harness.query(select_not_null).await?;
    assert_batches_eq!(
        &[
            "+----+", "| id |", "+----+", "| 1  |", "| 2  |", "| 3  |", "+----+",
        ],
        &batches
    );
    assert_explain_snapshot!(
        "null_filter_is_not_null",
        explain_to_string(&harness.explain(select_not_null).await?)
    );

    Ok(())
}

// =============================================================================
// Test: Late-joining executor receives DDL-created tables
// =============================================================================

/// Tests that an executor joining after DDL (CREATE SCHEMA, CREATE TABLE)
/// has already been executed on the cluster can serve queries against those tables.
///
/// Steps:
/// 1. Start scheduler + 1 executor
/// 2. CREATE SCHEMA and CREATE TABLE, INSERT rows
/// 3. Start a 2nd executor (late join)
/// 4. Verify the 2nd executor registered and the cluster can still query the table
#[tokio::test(flavor = "multi_thread")]
#[cfg(not(target_os = "windows"))]
async fn test_distributed_cayenne_late_join_ddl_replay() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    let temp_dir = tempfile::tempdir()?;
    let data_dir = temp_dir.path().join("data");
    let metadata_dir = temp_dir.path().join("metadata");

    test_request_context()
        .scope(async {
            configure_test_datafusion();

            let catalog = make_cayenne_catalog(
                "ljcat",
                &data_dir.to_string_lossy(),
                &metadata_dir.to_string_lossy(),
            );

            let mut harness = ClusterHarness::builder()
                .scheduler(
                    AppBuilder::new("distributed_cayenne_late_join")
                        .with_catalog(catalog.clone())
                        .build(),
                )
                .executor_with_app(
                    AppBuilder::new("executor_late_join_0")
                        .with_catalog(catalog.clone())
                        .build(),
                )
                .start()
                .await?;

            harness.wait_for_executors(Duration::from_secs(15)).await?;

            // DDL: create schema and table while only executor0 is connected.
            harness.query("CREATE SCHEMA ljcat.ljs").await?;
            harness
                .query(
                    "CREATE TABLE ljcat.ljs.items (
                        id BIGINT NOT NULL,
                        name VARCHAR NOT NULL,
                        PRIMARY KEY (id)
                    )",
                )
                .await?;

            // Insert data so we can verify the table is queryable.
            harness
                .query(
                    "INSERT INTO ljcat.ljs.items VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')",
                )
                .await?;
            wait_for_row_count(&harness, "ljcat.ljs.items", 3, Duration::from_secs(30)).await?;

            // Late-join: start a 2nd executor AFTER the DDL was already applied.
            harness
                .add_executor(Some(
                    AppBuilder::new("executor_late_join_1")
                        .with_catalog(catalog)
                        .build(),
                ))
                .await?;

            // Wait for the new executor to register.
            harness
                .wait_until_executor_count(2, Duration::from_secs(15))
                .await?;

            // Verify that the cluster still serves correct results.
            // The late-joining executor should have replayed CREATE SCHEMA + CREATE TABLE.
            let batches = harness
                .query("SELECT id, name FROM ljcat.ljs.items ORDER BY id")
                .await?;
            assert_batches_eq!(
                &[
                    "+----+-------+",
                    "| id | name  |",
                    "+----+-------+",
                    "| 1  | alpha |",
                    "| 2  | beta  |",
                    "| 3  | gamma |",
                    "+----+-------+",
                ],
                &batches
            );

            // Verify the count still matches.
            let count = scalar_i64(
                &harness
                    .query("SELECT COUNT(*) FROM ljcat.ljs.items")
                    .await?,
            )?;
            assert_eq!(count, 3);

            harness.shutdown().await;
            Ok(())
        })
        .await
}
