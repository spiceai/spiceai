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

//! Integration tests for WAL-backed write-back acceleration (DuckDB accelerator + Postgres source).
//!
//! Test scenarios:
//! - Happy path: INSERT/UPDATE/DELETE propagate to Postgres.
//! - Postgres down before write: commits to DuckDB immediately, delivers on reconnect.
//! - Postgres goes down mid-batch: surviving writes are queued and eventually delivered.
//! - Multiple operations queued: ordering and completeness preserved across outage.
//! - DELETE while Postgres is down: tracked in WAL and replayed on reconnect.
//! - UPDATE while Postgres is down: tracked in WAL and replayed on reconnect.

use crate::{
    configure_test_datafusion, init_tracing,
    postgres::common::{self, PG_PASSWORD, get_pg_params, get_random_port},
    utils::{register_test_connectors, run_query, runtime_ready_check, test_request_context},
};
use app::AppBuilder;
use futures::TryStreamExt;
use runtime::Runtime;
use spicepod::{
    acceleration::{Acceleration, Mode, RefreshMode, WriteMode},
    component::{
        access::AccessMode,
        dataset::{Dataset, replication::Replication},
    },
    param::Params,
};
use std::{sync::Arc, time::Duration};

use arrow::array::RecordBatch;
use datafusion_table_providers::sql::db_connection_pool::{
    DbConnectionPool, postgrespool::PostgresConnectionPool,
};

// ── Helpers ──────────────────────────────────────────────────────────────────

/// Create the source table and seed it with initial rows via a direct Postgres connection.
async fn setup_source_table(pool: &PostgresConnectionPool) -> Result<(), anyhow::Error> {
    let conn = pool
        .connect_direct()
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    conn.conn
        .execute(
            "CREATE TABLE IF NOT EXISTS wal_test (
                id      INTEGER PRIMARY KEY,
                name    TEXT    NOT NULL,
                value   INTEGER NOT NULL
            )",
            &[],
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    conn.conn
        .execute("TRUNCATE TABLE wal_test", &[])
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    conn.conn
        .execute(
            "INSERT INTO wal_test (id, name, value) VALUES
                (1, 'alice', 10),
                (2, 'bob',   20),
                (3, 'carol', 30)",
            &[],
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(())
}

/// Build a DuckDB-memory–accelerated dataset backed by a Postgres source in WAL write-back mode.
///
/// Memory mode avoids partition/view tables that DuckDB file mode creates after a full refresh
/// (which would reject DELETE/UPDATE with "Can only delete from base table!"). The WAL itself
/// lives inside the in-memory DuckDB, which is sufficient for testing network-outage scenarios.
fn make_wal_dataset(port: usize) -> Dataset {
    let mut dataset = Dataset::new("postgres:wal_test", "wal_test");
    dataset.params = Some(pg_params(port));
    dataset.replication = Some(Replication { enabled: true });
    dataset.access = AccessMode::ReadWrite;

    dataset.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some("duckdb".to_string()),
        mode: Mode::Memory,
        refresh_mode: Some(RefreshMode::Full),
        primary_key: Some("id".to_string()),
        write_mode: WriteMode::WriteBack,
        ..Acceleration::default()
    });
    dataset
}

fn pg_params(port: usize) -> Params {
    Params::from_string_map(
        vec![
            ("pg_host".to_string(), "localhost".to_string()),
            ("pg_port".to_string(), port.to_string()),
            ("pg_user".to_string(), "postgres".to_string()),
            ("pg_pass".to_string(), PG_PASSWORD.to_string()),
            ("pg_db".to_string(), "postgres".to_string()),
            ("pg_sslmode".to_string(), "disable".to_string()),
        ]
        .into_iter()
        .collect(),
    )
}

/// Count rows in `wal_test` via a direct Postgres connection.
async fn pg_row_count(pool: &PostgresConnectionPool) -> Result<i64, anyhow::Error> {
    let conn = pool.connect().await.map_err(|e| anyhow::anyhow!("{e}"))?;
    let batches: Vec<RecordBatch> = conn
        .as_async()
        .ok_or_else(|| anyhow::anyhow!("expected async connection"))?
        .query_arrow("SELECT COUNT(*) AS n FROM wal_test", &[], None)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?
        .try_collect()
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    let n = batches
        .first()
        .and_then(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .and_then(|a| if a.is_empty() { None } else { Some(a.value(0)) })
        })
        .unwrap_or(0);
    Ok(n)
}

/// Poll `pg_row_count` until it equals `expected` or the timeout expires.
async fn wait_for_pg_row_count(
    pool: &PostgresConnectionPool,
    expected: i64,
    timeout: Duration,
) -> Result<(), anyhow::Error> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        match pg_row_count(pool).await {
            Ok(n) if n == expected => return Ok(()),
            _ => {}
        }
        if tokio::time::Instant::now() >= deadline {
            let actual = pg_row_count(pool).await.unwrap_or(-1);
            return Err(anyhow::anyhow!(
                "Timed out waiting for {expected} rows in postgres; got {actual}"
            ));
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Wait until the DuckDB-side query returns a specific row count.
async fn wait_for_local_row_count(
    rt: &Arc<Runtime>,
    expected: usize,
    timeout: Duration,
) -> Result<(), anyhow::Error> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Ok(batches) = run_query(rt, "SELECT COUNT(*) FROM wal_test").await {
            let n: usize = batches
                .iter()
                .map(|b| {
                    b.column(0)
                        .as_any()
                        .downcast_ref::<arrow::array::Int64Array>()
                        .map_or(0, |a| if a.is_empty() { 0 } else { a.value(0) as usize })
                })
                .sum();
            if n == expected {
                return Ok(());
            }
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(anyhow::anyhow!(
                "Timed out waiting for {expected} rows in local DuckDB"
            ));
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

/// Happy path: INSERT/UPDATE/DELETE all propagate to Postgres within a few seconds.
#[cfg(all(feature = "duckdb", feature = "postgres-accel"))]
#[tokio::test]
async fn test_wal_write_back_happy_path() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let port = get_random_port()?;
            let container = common::start_postgres_docker_container(port).await?;
            let pool = common::get_postgres_connection_pool(port, None).await?;
            setup_source_table(&pool).await?;

            let dataset = make_wal_dataset(port);

            configure_test_datafusion();
            let rt = Arc::new(
                Runtime::builder()
                    .with_app(
                        AppBuilder::new("wal_happy_path")
                            .with_dataset(dataset)
                            .build(),
                    )
                    .build()
                    .await,
            );

            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(120)) => {
                    return Err(anyhow::anyhow!("Timed out loading components"));
                }
                () = Arc::clone(&rt).load_components() => {}
            }
            runtime_ready_check(&rt).await;

            // INSERT a new row
            run_query(
                &rt,
                "INSERT INTO wal_test (id, name, value) VALUES (4, 'dave', 40)",
            )
            .await?;
            // UPDATE an existing row
            run_query(&rt, "UPDATE wal_test SET value = 99 WHERE id = 1").await?;
            // DELETE a row
            run_query(&rt, "DELETE FROM wal_test WHERE id = 2").await?;

            // Expected: original 3 + 1 insert - 1 delete = 3, plus id=1 updated
            wait_for_pg_row_count(&pool, 3, Duration::from_secs(30)).await?;

            // Verify the updated value reached Postgres
            let conn = pool.connect().await.map_err(|e| anyhow::anyhow!("{e}"))?;
            let async_conn = conn
                .as_async()
                .ok_or_else(|| anyhow::anyhow!("async conn"))?;
            let batches: Vec<RecordBatch> = async_conn
                .query_arrow("SELECT value FROM wal_test WHERE id = 1", &[], None)
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?
                .try_collect()
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            let val = batches
                .first()
                .and_then(|b| {
                    b.column(0)
                        .as_any()
                        .downcast_ref::<arrow::array::Int32Array>()
                        .map(|a| a.value(0))
                })
                .unwrap_or(0);
            assert_eq!(val, 99, "UPDATE should propagate to Postgres");

            // dave (id=4) should be present
            let batches: Vec<RecordBatch> = async_conn
                .query_arrow("SELECT name FROM wal_test WHERE id = 4", &[], None)
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?
                .try_collect()
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            assert!(
                !batches.is_empty() && batches[0].num_rows() == 1,
                "INSERT should propagate"
            );

            rt.shutdown().await;
            container.remove().await?;
            Ok(())
        })
        .await
}

/// Postgres is completely down when writes arrive; they commit to DuckDB immediately
/// and are delivered once Postgres comes back up.
#[cfg(all(feature = "duckdb", feature = "postgres-accel"))]
#[tokio::test]
async fn test_wal_write_back_postgres_down_then_recover() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let port = get_random_port()?;
            let container = common::start_postgres_docker_container(port).await?;
            let pool = common::get_postgres_connection_pool(port, None).await?;
            setup_source_table(&pool).await?;

            let dataset = make_wal_dataset(port);

            configure_test_datafusion();
            let rt = Arc::new(
                Runtime::builder()
                    .with_app(AppBuilder::new("wal_pg_down").with_dataset(dataset).build())
                    .build()
                    .await,
            );

            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(120)) => {
                    return Err(anyhow::anyhow!("Timed out loading components"));
                }
                () = Arc::clone(&rt).load_components() => {}
            }
            runtime_ready_check(&rt).await;

            // Bring Postgres down BEFORE writing
            container.stop().await?;

            // These writes should succeed immediately (commit to local DuckDB)
            run_query(
                &rt,
                "INSERT INTO wal_test (id, name, value) VALUES (4, 'dave', 40)",
            )
            .await?;
            run_query(
                &rt,
                "INSERT INTO wal_test (id, name, value) VALUES (5, 'eve', 50)",
            )
            .await?;
            run_query(&rt, "DELETE FROM wal_test WHERE id = 3").await?;

            // Local accelerator must reflect the writes immediately
            wait_for_local_row_count(&rt, 4, Duration::from_secs(5)).await?; // 3 orig - 1 del + 2 ins

            // Restore Postgres
            container.start().await?;
            crate::docker::wait_for_tcp_port("127.0.0.1", port as u16, Duration::from_secs(60))
                .await?;

            // WAL worker should deliver: 3 original + 2 inserts - 1 delete = 4
            wait_for_pg_row_count(&pool, 4, Duration::from_secs(60)).await?;

            rt.shutdown().await;
            container.remove().await?;
            Ok(())
        })
        .await
}

/// Multiple heterogeneous operations (INSERTs, UPDATEs, DELETEs) are queued while
/// Postgres is down and all arrive in order once it recovers.
#[cfg(all(feature = "duckdb", feature = "postgres-accel"))]
#[tokio::test]
async fn test_wal_write_back_multiple_ops_queued() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let port = get_random_port()?;
            let container = common::start_postgres_docker_container(port).await?;
            let pool = common::get_postgres_connection_pool(port, None).await?;
            setup_source_table(&pool).await?;

            let dataset = make_wal_dataset(port);

            configure_test_datafusion();
            let rt = Arc::new(
                Runtime::builder()
                    .with_app(
                        AppBuilder::new("wal_multi_ops")
                            .with_dataset(dataset)
                            .build(),
                    )
                    .build()
                    .await,
            );

            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(120)) => {
                    return Err(anyhow::anyhow!("Timed out loading components"));
                }
                () = Arc::clone(&rt).load_components() => {}
            }
            runtime_ready_check(&rt).await;

            container.stop().await?;

            // Queue a mix of operations while Postgres is down
            run_query(
                &rt,
                "INSERT INTO wal_test (id, name, value) VALUES (4, 'dave', 40)",
            )
            .await?;
            run_query(
                &rt,
                "INSERT INTO wal_test (id, name, value) VALUES (5, 'eve', 50)",
            )
            .await?;
            run_query(
                &rt,
                "INSERT INTO wal_test (id, name, value) VALUES (6, 'frank', 60)",
            )
            .await?;
            run_query(&rt, "UPDATE wal_test SET value = 11 WHERE id = 1").await?;
            run_query(&rt, "UPDATE wal_test SET value = 22 WHERE id = 2").await?;
            run_query(&rt, "DELETE FROM wal_test WHERE id = 3").await?;

            // Local: 3 orig - 1 del + 3 ins = 5
            wait_for_local_row_count(&rt, 5, Duration::from_secs(5)).await?;

            container.start().await?;
            crate::docker::wait_for_tcp_port("127.0.0.1", port as u16, Duration::from_secs(60))
                .await?;

            // Wait until all WAL entries are delivered: 5 rows with the exact final values.
            // We cannot rely on wait_for_pg_row_count(5) alone because the count passes through 5
            // after only 2 of the 3 INSERTs are delivered (3→4→5), before UPDATEs and DELETE land.
            let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
            let batches = loop {
                let conn = pool.connect().await.map_err(|e| anyhow::anyhow!("{e}"))?;
                let async_conn = conn
                    .as_async()
                    .ok_or_else(|| anyhow::anyhow!("async conn"))?;
                let batches: Vec<RecordBatch> = async_conn
                    .query_arrow("SELECT id, value FROM wal_test ORDER BY id", &[], None)
                    .await
                    .map_err(|e| anyhow::anyhow!("{e}"))?
                    .try_collect()
                    .await
                    .map_err(|e| anyhow::anyhow!("{e}"))?;
                let n_matching: i64 = async_conn
                    .query_arrow(
                        "SELECT COUNT(*) AS n FROM wal_test \
                         WHERE (id = 1 AND value = 11) OR (id = 2 AND value = 22) \
                            OR id IN (4, 5, 6)",
                        &[],
                        None,
                    )
                    .await
                    .map_err(|e| anyhow::anyhow!("{e}"))?
                    .try_collect::<Vec<RecordBatch>>()
                    .await
                    .map_err(|e| anyhow::anyhow!("{e}"))?
                    .first()
                    .and_then(|b| {
                        b.column(0)
                            .as_any()
                            .downcast_ref::<arrow::array::Int64Array>()
                            .map(|a| a.value(0))
                    })
                    .unwrap_or(0);
                if n_matching == 5 {
                    break batches;
                }
                if tokio::time::Instant::now() >= deadline {
                    return Err(anyhow::anyhow!(
                        "Timed out waiting for all WAL entries to reach Postgres; \
                         only {n_matching}/5 rows have the expected final values"
                    ));
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            };
            let pretty = arrow::util::pretty::pretty_format_batches(&batches)?;
            insta::assert_snapshot!(pretty);

            rt.shutdown().await;
            container.remove().await?;
            Ok(())
        })
        .await
}

/// Only DELETEs are issued while Postgres is down; they must all reach Postgres on recovery.
#[cfg(all(feature = "duckdb", feature = "postgres-accel"))]
#[tokio::test]
async fn test_wal_write_back_deletes_while_down() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let port = get_random_port()?;
            let container = common::start_postgres_docker_container(port).await?;
            let pool = common::get_postgres_connection_pool(port, None).await?;
            setup_source_table(&pool).await?;

            let dataset = make_wal_dataset(port);

            configure_test_datafusion();
            let rt = Arc::new(
                Runtime::builder()
                    .with_app(
                        AppBuilder::new("wal_deletes_down")
                            .with_dataset(dataset)
                            .build(),
                    )
                    .build()
                    .await,
            );

            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(120)) => {
                    return Err(anyhow::anyhow!("Timed out loading components"));
                }
                () = Arc::clone(&rt).load_components() => {}
            }
            runtime_ready_check(&rt).await;

            container.stop().await?;

            run_query(&rt, "DELETE FROM wal_test WHERE id = 1").await?;
            run_query(&rt, "DELETE FROM wal_test WHERE id = 2").await?;
            run_query(&rt, "DELETE FROM wal_test WHERE id = 3").await?;

            wait_for_local_row_count(&rt, 0, Duration::from_secs(5)).await?;

            container.start().await?;
            crate::docker::wait_for_tcp_port("127.0.0.1", port as u16, Duration::from_secs(60))
                .await?;

            wait_for_pg_row_count(&pool, 0, Duration::from_secs(60)).await?;

            rt.shutdown().await;
            container.remove().await?;
            Ok(())
        })
        .await
}

/// Only UPDATEs are issued while Postgres is down; they must all reach Postgres on recovery.
#[cfg(all(feature = "duckdb", feature = "postgres-accel"))]
#[tokio::test]
async fn test_wal_write_back_updates_while_down() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let port = get_random_port()?;
            let container = common::start_postgres_docker_container(port).await?;
            let pool = common::get_postgres_connection_pool(port, None).await?;
            setup_source_table(&pool).await?;

            let dataset = make_wal_dataset(port);

            configure_test_datafusion();
            let rt = Arc::new(
                Runtime::builder()
                    .with_app(
                        AppBuilder::new("wal_updates_down")
                            .with_dataset(dataset)
                            .build(),
                    )
                    .build()
                    .await,
            );

            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(120)) => {
                    return Err(anyhow::anyhow!("Timed out loading components"));
                }
                () = Arc::clone(&rt).load_components() => {}
            }
            runtime_ready_check(&rt).await;

            container.stop().await?;

            run_query(&rt, "UPDATE wal_test SET value = 100 WHERE id = 1").await?;
            run_query(&rt, "UPDATE wal_test SET value = 200 WHERE id = 2").await?;
            run_query(&rt, "UPDATE wal_test SET value = 300 WHERE id = 3").await?;

            // Row count unchanged; just values updated
            wait_for_local_row_count(&rt, 3, Duration::from_secs(5)).await?;

            container.start().await?;
            crate::docker::wait_for_tcp_port("127.0.0.1", port as u16, Duration::from_secs(60))
                .await?;

            // Row count stays at 3 after UPDATEs, so poll the actual updated values instead.
            // All 3 UPDATE WAL entries are delivered in order; wait until all are present.
            let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
            loop {
                let conn = pool.connect().await.map_err(|e| anyhow::anyhow!("{e}"))?;
                let async_conn = conn
                    .as_async()
                    .ok_or_else(|| anyhow::anyhow!("async conn"))?;
                let batches: Vec<RecordBatch> = async_conn
                    .query_arrow(
                        "SELECT COUNT(*) AS n FROM wal_test WHERE value IN (100, 200, 300)",
                        &[],
                        None,
                    )
                    .await
                    .map_err(|e| anyhow::anyhow!("{e}"))?
                    .try_collect()
                    .await
                    .map_err(|e| anyhow::anyhow!("{e}"))?;
                let n = batches
                    .first()
                    .and_then(|b| {
                        b.column(0)
                            .as_any()
                            .downcast_ref::<arrow::array::Int64Array>()
                            .map(|a| a.value(0))
                    })
                    .unwrap_or(0);
                if n == 3 {
                    break;
                }
                if tokio::time::Instant::now() >= deadline {
                    return Err(anyhow::anyhow!(
                        "Timed out waiting for all WAL updates to reach Postgres; \
                         only {n}/3 rows have the expected values"
                    ));
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }

            let conn = pool.connect().await.map_err(|e| anyhow::anyhow!("{e}"))?;
            let batches: Vec<RecordBatch> = conn
                .as_async()
                .ok_or_else(|| anyhow::anyhow!("async conn"))?
                .query_arrow("SELECT id, value FROM wal_test ORDER BY id", &[], None)
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?
                .try_collect()
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            let pretty = arrow::util::pretty::pretty_format_batches(&batches)?;
            insta::assert_snapshot!(pretty);

            rt.shutdown().await;
            container.remove().await?;
            Ok(())
        })
        .await
}

/// Postgres goes down in the middle of a concurrent write batch.
///
/// This scenario is inherently racy: the goal is to verify that no writes are
/// silently lost — every write either committed to the local accelerator (and
/// thus appears in the WAL for eventual delivery) or returned an error to the
/// caller. After Postgres recovers the local and remote row counts must agree.
#[cfg(all(feature = "duckdb", feature = "postgres-accel"))]
#[tokio::test]
async fn test_wal_write_back_postgres_fails_mid_batch() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let port = get_random_port()?;
            let container = common::start_postgres_docker_container(port).await?;
            let pool = common::get_postgres_connection_pool(port, None).await?;
            setup_source_table(&pool).await?;

            let dataset = make_wal_dataset(port);

            configure_test_datafusion();
            let rt = Arc::new(
                Runtime::builder()
                    .with_app(
                        AppBuilder::new("wal_mid_batch")
                            .with_dataset(dataset)
                            .build(),
                    )
                    .build()
                    .await,
            );

            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(120)) => {
                    return Err(anyhow::anyhow!("Timed out loading components"));
                }
                () = Arc::clone(&rt).load_components() => {}
            }
            runtime_ready_check(&rt).await;

            // Fire off 20 concurrent inserts and simultaneously stop Postgres
            let rt2 = Arc::clone(&rt);
            let inserts = tokio::spawn(async move {
                let mut results = Vec::new();
                for i in 4..24_i32 {
                    let q = format!(
                        "INSERT INTO wal_test (id, name, value) VALUES ({i}, 'row{i}', {i})"
                    );
                    results.push(run_query(&rt2, &q).await);
                }
                results
            });

            // Give the first few inserts a head start, then yank Postgres
            tokio::time::sleep(Duration::from_millis(50)).await;
            container.stop().await?;

            let insert_results = inserts.await?;
            let succeeded = insert_results.iter().filter(|r| r.is_ok()).count();
            let failed = insert_results.len() - succeeded;
            tracing::info!("mid-batch inserts: {succeeded} succeeded, {failed} failed/errored");

            // There must be at least one successful insert
            assert!(succeeded > 0, "at least some inserts should have succeeded");

            // The local row count must equal seed rows + succeeded inserts
            let local_batches = run_query(&rt, "SELECT COUNT(*) FROM wal_test").await?;
            let local_count: i64 = local_batches
                .first()
                .and_then(|b| {
                    b.column(0)
                        .as_any()
                        .downcast_ref::<arrow::array::Int64Array>()
                        .map(|a| a.value(0))
                })
                .unwrap_or(0);
            assert_eq!(
                local_count as usize,
                3 + succeeded,
                "local count must equal seed + succeeded inserts"
            );

            // Restore Postgres and wait for WAL delivery
            container.start().await?;
            crate::docker::wait_for_tcp_port("127.0.0.1", port as u16, Duration::from_secs(60))
                .await?;

            wait_for_pg_row_count(&pool, local_count, Duration::from_secs(60)).await?;

            rt.shutdown().await;
            container.remove().await?;
            Ok(())
        })
        .await
}
