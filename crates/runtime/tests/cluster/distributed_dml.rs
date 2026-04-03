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

//! Integration tests for distributed DML (INSERT, UPDATE, DELETE) operations
//! on Cayenne catalog tables in a scheduler + executor cluster.
//!
//! These tests exercise the full distributed path:
//! - Scheduler receives SQL via `query()`
//! - `CayenneDdlAnalyzerRule` rewrites DML into distributed extension nodes
//! - Physical planner creates `DistributedCayenne{Insert,Update,Delete}Exec`
//! - `forward_dml_to_executors` sends SQL to connected executors via FlightSQL
//! - Executors execute the DML locally on their Cayenne tables

use std::collections::HashMap;
use std::time::Duration;

use app::AppBuilder;
use arrow::array::{Int64Array, RecordBatch};
use datafusion::assert_batches_eq;
use spicepod::component::access::AccessMode;
use spicepod::component::catalog::Catalog;
use spicepod::component::runtime::{
    PartitionManagement, Runtime as SpicepodRuntime, Scheduler as SchedulerConfig,
};
use spicepod::param::Params;
use tracing_subscriber::EnvFilter;

use crate::{
    configure_test_datafusion,
    utils::{test_request_context, verify_env_secret_exists},
};

use super::harness::ClusterHarness;

/// Creates a Cayenne catalog with `read_write_create` access using the given temp dirs.
fn make_cayenne_catalog(name: &str, data_dir: &str, metadata_dir: &str) -> Catalog {
    let mut catalog =
        Catalog::new("cayenne".to_string(), name.to_string()).with_access(AccessMode::ReadWriteCreate);
    catalog.params = Some(Params::from_string_map(
        HashMap::from([
            ("cayenne_data_dir".to_string(), data_dir.to_string()),
            (
                "cayenne_metadata_dir".to_string(),
                metadata_dir.to_string(),
            ),
        ]),
    ));
    catalog
}

fn make_scheduler_config(test_name: &str) -> SchedulerConfig {
    let run_id = uuid::Uuid::new_v4();
    SchedulerConfig {
        state_location: format!(
            "s3://spiceai-integration-tests/cluster-state/{test_name}/{run_id}/"
        ),
        params: Some(Params::from_string_map(HashMap::from([
            ("s3_region".to_string(), "us-east-1".to_string()),
            (
                "s3_key".to_string(),
                "${env:AWS_S3_VECTORS_KEY}".to_string(),
            ),
            (
                "s3_secret".to_string(),
                "${env:AWS_S3_VECTORS_SECRET}".to_string(),
            ),
            ("s3_auth".to_string(), "key".to_string()),
        ]))),
        partition_management: Some(PartitionManagement {
            interval: "1s".to_string(),
            ..Default::default()
        }),
    }
}

/// Helper: run a query through the harness and return scalar i64 from first column/row.
async fn query_scalar_i64(harness: &ClusterHarness, sql: &str) -> Result<i64, anyhow::Error> {
    let batches = harness.query(sql).await?;
    let batch = batches
        .first()
        .ok_or_else(|| anyhow::anyhow!("no batches for '{sql}'"))?;
    let col = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| anyhow::anyhow!("expected Int64Array for '{sql}'"))?;
    Ok(col.value(0))
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
        let count = query_scalar_i64(harness, &format!("SELECT COUNT(*) FROM {table}")).await?;
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

/// Test distributed INSERT, UPDATE, and DELETE on a Cayenne catalog table
/// with a single-column primary key.
///
/// Verifies the full scheduler→executor DML forwarding path.
#[tokio::test(flavor = "multi_thread")]
#[cfg(not(target_os = "windows"))]
async fn test_distributed_cayenne_dml_single_key() -> Result<(), anyhow::Error> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new("runtime=debug,info"))
        .with_ansi(true)
        .try_init();

    for env_var in ["AWS_S3_VECTORS_KEY", "AWS_S3_VECTORS_SECRET"] {
        verify_env_secret_exists(env_var)
            .await
            .map_err(anyhow::Error::msg)?;
    }

    let cayenne_tempdir = tempfile::tempdir()?;
    let data_dir = cayenne_tempdir.path().join("data").to_string_lossy().to_string();
    let metadata_dir = cayenne_tempdir
        .path()
        .join("metadata")
        .to_string_lossy()
        .to_string();

    test_request_context()
        .scope(async {
            configure_test_datafusion();

            let catalog = make_cayenne_catalog("testcat", &data_dir, &metadata_dir);

            let scheduler_app = AppBuilder::new("distributed_dml_single_key")
                .with_catalog(catalog.clone())
                .with_runtime(SpicepodRuntime {
                    scheduler: Some(make_scheduler_config("distributed_dml_single_key")),
                    ..SpicepodRuntime::default()
                })
                .build();

            let executor_app = AppBuilder::new("executor_dml_single_key")
                .with_catalog(catalog)
                .build();

            let harness = ClusterHarness::builder()
                .scheduler(scheduler_app)
                .executor_with_app(executor_app)
                .start()
                .await?;

            harness.wait_for_executors(Duration::from_secs(15)).await?;

            // Create a partitioned table via the scheduler
            harness
                .query("CREATE SCHEMA testcat.bench")
                .await?;
            harness
                .query(
                    "CREATE TABLE testcat.bench.orders (
                        id BIGINT NOT NULL,
                        customer VARCHAR,
                        amount DOUBLE,
                        PRIMARY KEY (id)
                    ) PARTITION BY (bucket(3, id))",
                )
                .await?;

            // Allow time for DDL to propagate to executor
            tokio::time::sleep(Duration::from_secs(2)).await;

            // INSERT via scheduler — forwarded to executor
            harness
                .query(
                    "INSERT INTO testcat.bench.orders VALUES
                        (1, 'Alice', 100.0),
                        (2, 'Bob', 200.0),
                        (3, 'Charlie', 300.0),
                        (4, 'Diana', 400.0),
                        (5, 'Eve', 500.0)",
                )
                .await?;

            wait_for_row_count(&harness, "testcat.bench.orders", 5, Duration::from_secs(15))
                .await?;

            // UPDATE via scheduler — forwarded to executor
            harness
                .query("UPDATE testcat.bench.orders SET amount = 999.0 WHERE id = 3")
                .await?;

            let batches = harness
                .query("SELECT id, amount FROM testcat.bench.orders WHERE id = 3")
                .await?;
            assert_batches_eq!(
                &[
                    "+----+--------+",
                    "| id | amount |",
                    "+----+--------+",
                    "| 3  | 999.0  |",
                    "+----+--------+",
                ],
                &batches
            );

            // DELETE single row via scheduler — forwarded to executor
            harness
                .query("DELETE FROM testcat.bench.orders WHERE id = 2")
                .await?;

            let count =
                query_scalar_i64(&harness, "SELECT COUNT(*) FROM testcat.bench.orders").await?;
            assert_eq!(count, 4, "Expected 4 rows after deleting id=2");

            // DELETE with IN clause
            harness
                .query("DELETE FROM testcat.bench.orders WHERE id IN (1, 5)")
                .await?;

            let count =
                query_scalar_i64(&harness, "SELECT COUNT(*) FROM testcat.bench.orders").await?;
            assert_eq!(count, 2, "Expected 2 rows after IN delete");

            let batches = harness
                .query("SELECT id, customer FROM testcat.bench.orders ORDER BY id")
                .await?;
            assert_batches_eq!(
                &[
                    "+----+----------+",
                    "| id | customer |",
                    "+----+----------+",
                    "| 3  | Charlie  |",
                    "| 4  | Diana    |",
                    "+----+----------+",
                ],
                &batches
            );

            harness.shutdown().await;
            Ok(())
        })
        .await
}

/// Test distributed DELETE with composite primary key and tuple IN syntax.
///
/// This is the pattern spicebench uses for multi-column key deletes:
///   DELETE FROM t WHERE (col1, col2) IN ((v1, v2), (v3, v4))
///
/// Regression test: commit 983d4d8de introduced DF-native DML which converts
/// tuple IN into struct() comparisons that may fail in expr_to_sql conversion.
#[tokio::test(flavor = "multi_thread")]
#[cfg(not(target_os = "windows"))]
async fn test_distributed_cayenne_dml_composite_key() -> Result<(), anyhow::Error> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new("runtime=debug,info"))
        .with_ansi(true)
        .try_init();

    for env_var in ["AWS_S3_VECTORS_KEY", "AWS_S3_VECTORS_SECRET"] {
        verify_env_secret_exists(env_var)
            .await
            .map_err(anyhow::Error::msg)?;
    }

    let cayenne_tempdir = tempfile::tempdir()?;
    let data_dir = cayenne_tempdir.path().join("data").to_string_lossy().to_string();
    let metadata_dir = cayenne_tempdir
        .path()
        .join("metadata")
        .to_string_lossy()
        .to_string();

    test_request_context()
        .scope(async {
            configure_test_datafusion();

            let catalog = make_cayenne_catalog("testcat", &data_dir, &metadata_dir);

            let scheduler_app = AppBuilder::new("distributed_dml_composite_key")
                .with_catalog(catalog.clone())
                .with_runtime(SpicepodRuntime {
                    scheduler: Some(make_scheduler_config("distributed_dml_composite_key")),
                    ..SpicepodRuntime::default()
                })
                .build();

            let executor_app = AppBuilder::new("executor_dml_composite_key")
                .with_catalog(catalog)
                .build();

            let harness = ClusterHarness::builder()
                .scheduler(scheduler_app)
                .executor_with_app(executor_app)
                .start()
                .await?;

            harness.wait_for_executors(Duration::from_secs(15)).await?;

            // Create table with composite primary key (like TPC-H lineitem)
            harness
                .query("CREATE SCHEMA testcat.bench")
                .await?;
            harness
                .query(
                    "CREATE TABLE testcat.bench.lineitem (
                        l_orderkey BIGINT NOT NULL,
                        l_linenumber INT NOT NULL,
                        l_quantity DOUBLE,
                        l_comment VARCHAR,
                        PRIMARY KEY (l_orderkey, l_linenumber)
                    ) PARTITION BY (bucket(3, l_linenumber))",
                )
                .await?;

            tokio::time::sleep(Duration::from_secs(2)).await;

            // INSERT test data
            harness
                .query(
                    "INSERT INTO testcat.bench.lineitem VALUES
                        (1, 1, 10.0, 'a'),
                        (1, 2, 20.0, 'b'),
                        (2, 1, 30.0, 'c'),
                        (2, 2, 40.0, 'd'),
                        (3, 1, 50.0, 'e'),
                        (3, 2, 60.0, 'f'),
                        (3, 3, 70.0, 'g')",
                )
                .await?;

            wait_for_row_count(
                &harness,
                "testcat.bench.lineitem",
                7,
                Duration::from_secs(15),
            )
            .await?;

            // UPDATE with composite key filter
            harness
                .query(
                    "UPDATE testcat.bench.lineitem SET l_quantity = 999.0
                     WHERE l_orderkey = 1 AND l_linenumber = 2",
                )
                .await?;

            let batches = harness
                .query(
                    "SELECT l_orderkey, l_linenumber, l_quantity
                     FROM testcat.bench.lineitem
                     WHERE l_orderkey = 1 AND l_linenumber = 2",
                )
                .await?;
            assert_batches_eq!(
                &[
                    "+------------+--------------+------------+",
                    "| l_orderkey | l_linenumber | l_quantity |",
                    "+------------+--------------+------------+",
                    "| 1          | 2            | 999.0      |",
                    "+------------+--------------+------------+",
                ],
                &batches
            );

            // DELETE with AND-style composite key (always works)
            harness
                .query(
                    "DELETE FROM testcat.bench.lineitem
                     WHERE l_orderkey = 3 AND l_linenumber = 3",
                )
                .await?;

            let count = query_scalar_i64(
                &harness,
                "SELECT COUNT(*) FROM testcat.bench.lineitem",
            )
            .await?;
            assert_eq!(count, 6, "Expected 6 rows after AND-style delete");

            // DELETE with tuple IN syntax — this is what spicebench generates
            // for composite-key deletes. Tests the struct() IN conversion path.
            harness
                .query(
                    "DELETE FROM testcat.bench.lineitem
                     WHERE (l_orderkey, l_linenumber) IN ((1, 1), (2, 2))",
                )
                .await?;

            let count = query_scalar_i64(
                &harness,
                "SELECT COUNT(*) FROM testcat.bench.lineitem",
            )
            .await?;
            assert_eq!(count, 4, "Expected 4 rows after tuple IN delete");

            // Verify remaining rows
            let batches = harness
                .query(
                    "SELECT l_orderkey, l_linenumber, l_comment
                     FROM testcat.bench.lineitem ORDER BY l_orderkey, l_linenumber",
                )
                .await?;
            assert_batches_eq!(
                &[
                    "+------------+--------------+-----------+",
                    "| l_orderkey | l_linenumber | l_comment |",
                    "+------------+--------------+-----------+",
                    "| 1          | 2            | b         |",
                    "| 2          | 1            | c         |",
                    "| 3          | 1            | e         |",
                    "| 3          | 2            | f         |",
                    "+------------+--------------+-----------+",
                ],
                &batches
            );

            harness.shutdown().await;
            Ok(())
        })
        .await
}
