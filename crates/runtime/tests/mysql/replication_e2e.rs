/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! End-to-end integration test for the `MySQL` binlog-replication path, driven
//! through the full Spice Runtime (Spicepod datasets + `DuckDB` accelerator).
//!
//! What this test validates that the pure-library test at `replication.rs`
//! does not:
//!   - A real `AcceleratedTable` pipeline with `refresh_mode: changes`.
//!   - Multiple datasets in one Spice instance, each on its own binlog
//!     connection.
//!   - Actual SQL queries through `Runtime::datafusion()` (not raw
//!     `ChangeBatch` assertions) — so we prove end users can query the
//!     replicated data.
//!   - A range of `MySQL` data types (`INT`, `BIGINT`, `TEXT`, `DOUBLE`,
//!     `DECIMAL`, `DATE`, `DATETIME`) surviving INSERT/UPDATE/DELETE across
//!     the binlog path.
//!   - The stream surviving a compatible `ALTER TABLE` on the source
//!     (columns added on the source are not replicated, but replication
//!     continues).
//!
//! Mirrors `postgres/replication_tpch.rs` in shape.

#![allow(clippy::expect_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use app::AppBuilder;
use mysql_async::prelude::Queryable;
use runtime::Runtime;
use spicepod::acceleration::{Acceleration, OnConflictBehavior, RefreshMode};
use spicepod::component::dataset::Dataset;
use spicepod::param::Params;
use tokio::time::sleep;

use crate::mysql::common;
use crate::utils::{
    register_test_connectors, run_query, runtime_ready_check, test_request_context, wait_until_true,
};
use crate::{configure_test_datafusion, init_tracing};

const MYSQL_E2E_PORT: u16 = 13322;
#[cfg(not(target_os = "windows"))]
const MYSQL_E2E_CAYENNE_PORT: u16 = 13323;
#[cfg(not(target_os = "windows"))]
const MYSQL_E2E_RESTART_PORT: u16 = 13321;
#[cfg(not(target_os = "windows"))]
const MYSQL_E2E_GTID_PORT: u16 = 13330;
#[cfg(not(target_os = "windows"))]
const MYSQL_E2E_RECONNECT_PORT: u16 = 13331;
#[cfg(not(target_os = "windows"))]
const MYSQL_E2E_TYPES_PORT: u16 = 13332;

/// The accelerator engine a run of the e2e exercises.
struct EngineConfig {
    engine: &'static str,
    mode: spicepod::acceleration::Mode,
    /// Engine-specific acceleration params (e.g. cayenne data/metastore dirs).
    accel_params: HashMap<String, String>,
}

/// How long to wait for the replication stream to apply a change before
/// failing.
const CHANGE_PROPAGATION_TIMEOUT: Duration = Duration::from_secs(30);

const DDL_STATEMENTS: &[&str] = &[
    r"CREATE TABLE repl_products (
        p_id        INT PRIMARY KEY,
        p_name      TEXT NOT NULL,
        p_price     DECIMAL(10, 2) NOT NULL,
        p_weight    DOUBLE NOT NULL,
        p_added_on  DATE NOT NULL,
        p_restocked DATETIME
    )",
    r"CREATE TABLE repl_orders (
        o_id     BIGINT PRIMARY KEY,
        o_p_id   INT NOT NULL,
        o_qty    INT NOT NULL,
        o_status TEXT
    )",
];

const SEED_STATEMENTS: &[&str] = &[
    "INSERT INTO repl_products VALUES \
     (1, 'anvil',   19.99, 45.0,  '2024-01-05', '2024-06-01 08:30:00'),\
     (2, 'rocket',  99.50,  3.25, '2024-02-11', NULL),\
     (3, 'magnet',   5.75,  0.5,  '2024-03-20', '2024-05-15 17:45:10')",
    "INSERT INTO repl_orders VALUES \
     (100, 1, 2, 'shipped'),\
     (101, 2, 1, 'pending'),\
     (102, 3, 7, 'shipped'),\
     (103, 1, 1, NULL)",
];

struct ReplicatedDataset {
    dataset_name: &'static str,
    table: &'static str,
    primary_key: &'static str,
    expected_initial_count: u64,
}

const DATASETS: &[ReplicatedDataset] = &[
    ReplicatedDataset {
        dataset_name: "repl_products",
        table: "mysqldb.repl_products",
        primary_key: "p_id",
        expected_initial_count: 3,
    },
    ReplicatedDataset {
        dataset_name: "repl_orders",
        table: "mysqldb.repl_orders",
        primary_key: "o_id",
        expected_initial_count: 4,
    },
];

fn mysql_params(port: u16) -> HashMap<String, String> {
    HashMap::from([
        ("mysql_host".to_string(), "localhost".to_string()),
        ("mysql_tcp_port".to_string(), port.to_string()),
        ("mysql_user".to_string(), "root".to_string()),
        (
            "mysql_pass".to_string(),
            common::MYSQL_ROOT_PASSWORD.to_string(),
        ),
        ("mysql_db".to_string(), "mysqldb".to_string()),
        ("mysql_sslmode".to_string(), "disabled".to_string()),
        // Short interval so the test's change waits stay snappy.
        (
            "mysql_replication_checkpoint_interval".to_string(),
            "1s".to_string(),
        ),
    ])
}

fn make_dataset(
    ds: &ReplicatedDataset,
    params: &HashMap<String, String>,
    engine: &EngineConfig,
) -> Dataset {
    let mut dataset = Dataset::new(format!("mysql:{}", ds.table), ds.dataset_name.to_string());
    dataset.params = Some(Params::from_string_map(params.clone()));
    dataset.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some(engine.engine.to_string()),
        mode: engine.mode.clone(),
        params: (!engine.accel_params.is_empty())
            .then(|| Params::from_string_map(engine.accel_params.clone())),
        refresh_mode: Some(RefreshMode::Changes),
        primary_key: Some(ds.primary_key.to_string()),
        on_conflict: vec![(ds.primary_key.to_string(), OnConflictBehavior::Upsert)]
            .into_iter()
            .collect(),
        ..Acceleration::default()
    });
    dataset
}

async fn exec(pool: &mysql_async::Pool, sql: &str) -> Result<(), anyhow::Error> {
    let mut conn = pool.get_conn().await?;
    conn.query_drop(sql)
        .await
        .map_err(|e| anyhow!("mysql error running `{sql}`: {e}"))?;
    Ok(())
}

async fn scalar_i64(rt: &Arc<Runtime>, sql: &str) -> Result<i64, anyhow::Error> {
    let batches = run_query(rt, sql).await?;
    let batch = batches
        .first()
        .filter(|b| b.num_rows() > 0)
        .ok_or_else(|| anyhow!("no rows from `{sql}`"))?;
    let column = batch.column(0);
    // The result width depends on the query and engine: a `COUNT(*)` is
    // Int64 while a raw `INT` column scan stays Int32 — accept both and
    // compare as i64.
    if let Some(a) = column.as_any().downcast_ref::<arrow::array::Int64Array>() {
        return Ok(a.value(0));
    }
    if let Some(a) = column.as_any().downcast_ref::<arrow::array::Int32Array>() {
        return Ok(i64::from(a.value(0)));
    }
    Err(anyhow!(
        "non-integer result from `{sql}`: got {}",
        column.data_type()
    ))
}

/// Poll `sql` (which must return a single Int64 scalar) until it reports
/// `expected`, or time out. This is the back-pressure signal that the
/// replication stream has applied changes end-to-end.
async fn wait_for_scalar_i64(
    rt: &Arc<Runtime>,
    sql: &str,
    expected: i64,
) -> Result<(), anyhow::Error> {
    let deadline = std::time::Instant::now() + CHANGE_PROPAGATION_TIMEOUT;
    loop {
        let actual = scalar_i64(rt, sql).await?;
        if actual == expected {
            return Ok(());
        }
        if std::time::Instant::now() >= deadline {
            return Err(anyhow!(
                "timed out waiting for `{sql}` to reach {expected}; last saw {actual}"
            ));
        }
        sleep(Duration::from_millis(250)).await;
    }
}

async fn run_replication_e2e(port: u16, engine: EngineConfig) -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some(
        "integration=debug,runtime=debug,data_components::mysql_replication=debug,info",
    ));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let _container = common::start_mysql_docker_container(port)
                .await
                .map_err(|e| anyhow!("start container: {e}"))?;

            // ------------------------------------------------------------
            // 1. Create schema + seed on the source.
            // ------------------------------------------------------------
            let pool = common::get_mysql_conn(port)?;
            for ddl in DDL_STATEMENTS {
                exec(&pool, ddl).await?;
            }
            for seed in SEED_STATEMENTS {
                exec(&pool, seed).await?;
            }

            // ------------------------------------------------------------
            // 2. Build a Spice app with the replicated datasets.
            // ------------------------------------------------------------
            let params = mysql_params(port);
            let mut builder =
                AppBuilder::new(format!("mysql_replication_integration_{}", engine.engine));
            for ds in DATASETS {
                builder = builder.with_dataset(make_dataset(ds, &params, &engine));
            }
            let app = builder.build();

            configure_test_datafusion();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);

            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(90)) => {
                    return Err(anyhow!("timed out waiting for datasets to load"));
                }
                () = Arc::clone(&rt).load_components() => {}
            }
            runtime_ready_check(&rt).await;

            // ------------------------------------------------------------
            // 3. Bootstrap validation — snapshot counts via SQL.
            // ------------------------------------------------------------
            for ds in DATASETS {
                wait_for_scalar_i64(
                    &rt,
                    &format!("SELECT count(*) FROM {}", ds.dataset_name),
                    i64::try_from(ds.expected_initial_count)?,
                )
                .await?;
            }
            // Type spot-checks from the snapshot path.
            assert_eq!(
                scalar_i64(
                    &rt,
                    "SELECT CAST(p_price * 100 AS BIGINT) FROM repl_products WHERE p_id = 1"
                )
                .await?,
                1999,
                "DECIMAL survived the snapshot"
            );
            assert_eq!(
                scalar_i64(
                    &rt,
                    "SELECT count(*) FROM repl_products WHERE p_added_on = DATE '2024-02-11'"
                )
                .await?,
                1,
                "DATE survived the snapshot"
            );
            assert_eq!(
                scalar_i64(
                    &rt,
                    "SELECT count(*) FROM repl_products WHERE p_restocked IS NULL"
                )
                .await?,
                1,
                "NULL DATETIME survived the snapshot"
            );

            // ------------------------------------------------------------
            // 4. Live INSERT.
            // ------------------------------------------------------------
            exec(
                &pool,
                "INSERT INTO repl_products VALUES \
                 (4, 'gizmo', 42.00, 1.5, '2024-04-01', '2024-06-30 12:00:00')",
            )
            .await?;
            wait_for_scalar_i64(&rt, "SELECT count(*) FROM repl_products", 4).await?;

            // ------------------------------------------------------------
            // 5. Live UPDATE applies as an upsert (no duplicate rows).
            // ------------------------------------------------------------
            exec(
                &pool,
                "UPDATE repl_orders SET o_qty = 9, o_status = 'shipped' WHERE o_id = 101",
            )
            .await?;
            wait_for_scalar_i64(&rt, "SELECT o_qty FROM repl_orders WHERE o_id = 101", 9).await?;
            assert_eq!(
                scalar_i64(&rt, "SELECT count(*) FROM repl_orders").await?,
                4,
                "UPDATE must not duplicate rows"
            );

            // ------------------------------------------------------------
            // 6. Live DELETE.
            // ------------------------------------------------------------
            exec(&pool, "DELETE FROM repl_orders WHERE o_id = 103").await?;
            wait_for_scalar_i64(&rt, "SELECT count(*) FROM repl_orders", 3).await?;

            // ------------------------------------------------------------
            // 7. Compatible ALTER TABLE on the source: the stream adopts the
            //    new layout and keeps replicating (the new column is not
            //    replicated — it isn't in the dataset schema).
            // ------------------------------------------------------------
            exec(&pool, "ALTER TABLE repl_orders ADD COLUMN o_note TEXT").await?;
            exec(
                &pool,
                "INSERT INTO repl_orders (o_id, o_p_id, o_qty, o_status, o_note) \
                 VALUES (104, 2, 3, 'pending', 'post-DDL row')",
            )
            .await?;
            wait_for_scalar_i64(&rt, "SELECT count(*) FROM repl_orders", 4).await?;
            wait_for_scalar_i64(&rt, "SELECT o_qty FROM repl_orders WHERE o_id = 104", 3).await?;

            // ------------------------------------------------------------
            // 8. TRUNCATE propagates.
            // ------------------------------------------------------------
            exec(&pool, "TRUNCATE TABLE repl_orders").await?;
            wait_for_scalar_i64(&rt, "SELECT count(*) FROM repl_orders", 0).await?;
            // The other dataset is untouched.
            assert_eq!(
                scalar_i64(&rt, "SELECT count(*) FROM repl_products").await?,
                4,
                "TRUNCATE must only affect its own table"
            );

            pool.disconnect().await?;
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn mysql_binlog_replication_end_to_end() -> Result<(), anyhow::Error> {
    run_replication_e2e(
        MYSQL_E2E_PORT,
        EngineConfig {
            engine: "duckdb",
            mode: spicepod::acceleration::Mode::Memory,
            accel_params: HashMap::new(),
        },
    )
    .await
}

/// Same lifecycle against the Cayenne accelerator — the file-backed engine
/// additionally exercises the `spice_sys_mysql_binlog` position sidecar.
#[cfg(not(target_os = "windows"))]
#[tokio::test(flavor = "multi_thread")]
async fn mysql_binlog_replication_end_to_end_cayenne() -> Result<(), anyhow::Error> {
    let temp_dir = tempfile::tempdir()?;
    let data_dir = temp_dir.path().join("cayenne");
    std::fs::create_dir_all(&data_dir)?;
    let accel_params = HashMap::from([
        (
            "cayenne_file_path".to_string(),
            data_dir.display().to_string(),
        ),
        (
            "cayenne_metadata_dir".to_string(),
            temp_dir.path().join("metadata.db").display().to_string(),
        ),
    ]);
    run_replication_e2e(
        MYSQL_E2E_CAYENNE_PORT,
        EngineConfig {
            engine: "cayenne",
            mode: spicepod::acceleration::Mode::File,
            accel_params,
        },
    )
    .await
}

/// Restart / resume round-trip through the full runtime on the file-backed
/// Cayenne accelerator: a second runtime pointed at the same accelerator dirs
/// resumes from the persisted `spice_sys_mysql_binlog` position (no
/// re-snapshot), replays the changes made while it was down, and re-reaches
/// Ready via lag-based readiness — proving durable data + resumable position
/// survive a process restart.
#[cfg(not(target_os = "windows"))]
#[tokio::test(flavor = "multi_thread")]
async fn mysql_binlog_replication_restart_resume_cayenne() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some(
        "integration=debug,runtime=debug,data_components::mysql_replication=debug,info",
    ));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let port = MYSQL_E2E_RESTART_PORT;
            let _container = common::start_mysql_docker_container(port)
                .await
                .map_err(|e| anyhow!("start container: {e}"))?;

            // Seed just the orders table on the source.
            let pool = common::get_mysql_conn(port)?;
            exec(&pool, DDL_STATEMENTS[1]).await?; // repl_orders
            exec(&pool, SEED_STATEMENTS[1]).await?; // 4 rows

            // File-backed Cayenne dirs shared across both runtime instances, so
            // the second run finds the persisted rows + binlog position.
            let temp_dir = tempfile::tempdir()?;
            let data_dir = temp_dir.path().join("cayenne");
            std::fs::create_dir_all(&data_dir)?;
            let accel_params = HashMap::from([
                (
                    "cayenne_file_path".to_string(),
                    data_dir.display().to_string(),
                ),
                (
                    "cayenne_metadata_dir".to_string(),
                    temp_dir.path().join("metadata.db").display().to_string(),
                ),
            ]);
            // Distinct server_ids so the two runs never collide on the source
            // replica id within the same process (a real process restart reuses
            // the derived id, but sequential runs here overlap briefly).
            let make_rt = |server_id: &str| {
                let mut params = mysql_params(port);
                params.insert(
                    "mysql_replication_server_id".to_string(),
                    server_id.to_string(),
                );
                let engine = EngineConfig {
                    engine: "cayenne",
                    mode: spicepod::acceleration::Mode::File,
                    accel_params: accel_params.clone(),
                };
                let ds = make_dataset(&DATASETS[1], &params, &engine);
                AppBuilder::new("mysql_replication_restart").with_dataset(ds)
            };

            configure_test_datafusion();

            // ---- Run 1: cold bootstrap the snapshot, then shut down. ----
            {
                let rt = Arc::new(
                    Runtime::builder()
                        .with_app(make_rt("42001").build())
                        .build()
                        .await,
                );
                tokio::select! {
                    () = tokio::time::sleep(Duration::from_secs(90)) => {
                        return Err(anyhow!("run 1: timed out loading"));
                    }
                    () = Arc::clone(&rt).load_components() => {}
                }
                runtime_ready_check(&rt).await;
                wait_for_scalar_i64(&rt, "SELECT count(*) FROM repl_orders", 4).await?;
                // Dropping the runtime cancels the binlog stream and releases the
                // source replica connection.
                drop(rt);
            }

            // ---- Gap: a change made while no runtime is streaming. ----
            exec(
                &pool,
                "INSERT INTO repl_orders (o_id, o_p_id, o_qty, o_status) VALUES (999, 1, 5, 'gap')",
            )
            .await?;

            // ---- Run 2: resume from the persisted sidecar position. ----
            {
                let rt = Arc::new(
                    Runtime::builder()
                        .with_app(make_rt("42002").build())
                        .build()
                        .await,
                );
                tokio::select! {
                    () = tokio::time::sleep(Duration::from_secs(90)) => {
                        return Err(anyhow!("run 2: timed out loading"));
                    }
                    () = Arc::clone(&rt).load_components() => {}
                }
                runtime_ready_check(&rt).await;

                // The gap insert replays from the resumed position -> 5 rows,
                // and the originally-snapshotted rows are still present (durable
                // accelerator data, not a re-snapshot that could have raced).
                wait_for_scalar_i64(&rt, "SELECT count(*) FROM repl_orders", 5).await?;
                assert_eq!(
                    scalar_i64(&rt, "SELECT o_qty FROM repl_orders WHERE o_id = 999").await?,
                    5,
                    "the change made while down must replay after resume"
                );
                assert_eq!(
                    scalar_i64(&rt, "SELECT count(*) FROM repl_orders WHERE o_id = 100").await?,
                    1,
                    "originally-replicated rows must survive the restart"
                );
                drop(rt);
            }

            pool.disconnect().await?;
            Ok(())
        })
        .await
}

/// GTID auto-positioning resume: on a `gtid_mode = ON` source,
/// the connector bootstraps a GTID cursor and, after a process restart, resumes
/// via `COM_BINLOG_DUMP_GTID` from the persisted executed set with no
/// duplicates or gaps. If GTID resume were broken (e.g. the dump-request set or
/// the empty-set round-trip), run 2 would error or diverge, failing this test.
/// Cursor-type persistence itself is unit-tested per engine in
/// `runtime::dataaccelerator::spice_sys::mysql_binlog`.
#[cfg(not(target_os = "windows"))]
#[tokio::test(flavor = "multi_thread")]
async fn mysql_binlog_replication_gtid_resume_cayenne() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some(
        "integration=debug,runtime=debug,data_components::mysql_replication=debug,info",
    ));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let port = MYSQL_E2E_GTID_PORT;
            let _container = common::start_mysql_gtid_docker_container(port)
                .await
                .map_err(|e| anyhow!("start gtid container: {e}"))?;

            let pool = common::get_mysql_conn(port)?;
            exec(&pool, DDL_STATEMENTS[1]).await?; // repl_orders
            exec(&pool, SEED_STATEMENTS[1]).await?; // 4 rows

            // The source must actually be issuing GTIDs, else this test would
            // silently exercise the file+offset path instead.
            let gtid_mode = {
                let mut conn = pool.get_conn().await?;
                conn.query_first::<String, _>("SELECT @@GLOBAL.gtid_mode")
                    .await?
                    .ok_or_else(|| anyhow!("gtid_mode query returned no row"))?
            };
            assert_eq!(gtid_mode, "ON", "container must run with gtid_mode = ON");

            let temp_dir = tempfile::tempdir()?;
            let data_dir = temp_dir.path().join("cayenne");
            std::fs::create_dir_all(&data_dir)?;
            let accel_params = HashMap::from([
                (
                    "cayenne_file_path".to_string(),
                    data_dir.display().to_string(),
                ),
                (
                    "cayenne_metadata_dir".to_string(),
                    temp_dir.path().join("metadata.db").display().to_string(),
                ),
            ]);
            // Distinct server_ids so the two runs never collide on the source
            // replica id (a real process restart reuses the derived id, but
            // sequential runs here overlap briefly).
            let make_rt = |server_id: &str| {
                let mut params = mysql_params(port);
                params.insert(
                    "mysql_replication_server_id".to_string(),
                    server_id.to_string(),
                );
                let engine = EngineConfig {
                    engine: "cayenne",
                    mode: spicepod::acceleration::Mode::File,
                    accel_params: accel_params.clone(),
                };
                let ds = make_dataset(&DATASETS[1], &params, &engine);
                AppBuilder::new("mysql_replication_gtid").with_dataset(ds)
            };

            configure_test_datafusion();

            // ---- Run 1: cold bootstrap by GTID, then shut down. ----
            {
                let rt = Arc::new(
                    Runtime::builder()
                        .with_app(make_rt("42011").build())
                        .build()
                        .await,
                );
                tokio::select! {
                    () = tokio::time::sleep(Duration::from_secs(90)) => {
                        return Err(anyhow!("run 1: timed out loading"));
                    }
                    () = Arc::clone(&rt).load_components() => {}
                }
                runtime_ready_check(&rt).await;
                wait_for_scalar_i64(&rt, "SELECT count(*) FROM repl_orders", 4).await?;
                drop(rt);
            }

            // ---- Gap: a change made while no runtime is streaming. ----
            exec(
                &pool,
                "INSERT INTO repl_orders (o_id, o_p_id, o_qty, o_status) VALUES (999, 1, 5, 'gap')",
            )
            .await?;

            // ---- Run 2: resume from the persisted GTID set. ----
            {
                let rt = Arc::new(
                    Runtime::builder()
                        .with_app(make_rt("42012").build())
                        .build()
                        .await,
                );
                tokio::select! {
                    () = tokio::time::sleep(Duration::from_secs(90)) => {
                        return Err(anyhow!("run 2: timed out loading"));
                    }
                    () = Arc::clone(&rt).load_components() => {}
                }
                runtime_ready_check(&rt).await;

                // GTID resume replays the gap insert (no gap) and the original
                // snapshot rows survive (no re-snapshot, no duplicates).
                wait_for_scalar_i64(&rt, "SELECT count(*) FROM repl_orders", 5).await?;
                assert_eq!(
                    scalar_i64(&rt, "SELECT o_qty FROM repl_orders WHERE o_id = 999").await?,
                    5,
                    "the change made while down must replay after GTID resume"
                );
                assert_eq!(
                    scalar_i64(&rt, "SELECT count(*) FROM repl_orders WHERE o_id = 100").await?,
                    1,
                    "originally-replicated rows must survive the GTID resume"
                );
                drop(rt);
            }

            pool.disconnect().await?;
            Ok(())
        })
        .await
}

/// One `UPDATE`-only table whose `update_count` column is incremented by every
/// write, so a lost or stale row version shows up as a short `SUM` while the
/// row count stays correct.
const RECONNECT_DDL: &str = r"CREATE TABLE repl_counters (
        c_id          INT PRIMARY KEY,
        update_count  INT NOT NULL,
        payload       VARCHAR(64) NOT NULL
    )";

/// Rows repeatedly updated by the workload. Small enough that several versions
/// of the same primary key are in flight at once.
const RECONNECT_ROWS: i64 = 20;
/// Updates applied to every row, half before the dump connection is killed and
/// half after.
const RECONNECT_UPDATES_PER_ROW: i64 = 40;

/// The `Binlog Dump` thread's connection id, or `None` when the pump has not
/// (re)connected yet.
async fn binlog_dump_thread_id(pool: &mysql_async::Pool) -> Result<Option<u64>, anyhow::Error> {
    let mut conn = pool.get_conn().await?;
    let id: Option<u64> = conn
        .query_first(
            "SELECT id FROM information_schema.processlist \
             WHERE command LIKE 'Binlog Dump%' ORDER BY id DESC LIMIT 1",
        )
        .await?;
    Ok(id)
}

/// Wait for the pump's `Binlog Dump` thread to appear and return its id. It
/// registers a moment after the first rows land, so the test waits rather than
/// racing it.
async fn wait_for_binlog_dump_thread(pool: &mysql_async::Pool) -> Result<u64, anyhow::Error> {
    let up = wait_until_true(CHANGE_PROPAGATION_TIMEOUT, || async {
        matches!(binlog_dump_thread_id(pool).await, Ok(Some(_)))
    })
    .await;
    if !up {
        return Err(anyhow!("no `Binlog Dump` thread appeared"));
    }
    binlog_dump_thread_id(pool)
        .await?
        .ok_or_else(|| anyhow!("the `Binlog Dump` thread vanished between polls"))
}

/// The floor the pump raises the dump session's `net_write_timeout` to, mirroring
/// `data_components::mysql_replication::binlog::DUMP_NET_WRITE_TIMEOUT_SECS`
/// (private to that crate). The server default is 60s, so anything at or above
/// this proves the raise was accepted and applied.
const EXPECTED_DUMP_NET_WRITE_TIMEOUT_SECS: u64 = 180;

/// The `net_write_timeout` in force on another session, read back from the
/// server rather than inferred from the statement Spice sent.
///
/// `Ok(None)` when `performance_schema` cannot answer — the instrumentation is
/// on by default but can be built out or turned off, and a suite that fails for
/// that reason would be reporting on the image rather than on the runtime.
async fn session_net_write_timeout(
    pool: &mysql_async::Pool,
    processlist_id: u64,
) -> Result<Option<u64>, anyhow::Error> {
    let mut conn = pool.get_conn().await?;
    let sql = format!(
        "SELECT v.VARIABLE_VALUE FROM performance_schema.variables_by_thread v \
         JOIN performance_schema.threads t ON t.THREAD_ID = v.THREAD_ID \
         WHERE t.PROCESSLIST_ID = {processlist_id} \
           AND v.VARIABLE_NAME = 'net_write_timeout'"
    );
    let value: Option<String> = match conn.query_first(sql.as_str()).await {
        Ok(value) => value,
        Err(e) => {
            // Said out loud: a silent `None` here would turn the assertion below
            // into one that can never fail.
            eprintln!("performance_schema could not answer `{sql}`: {e}");
            return Ok(None);
        }
    };
    // No row is "cannot answer" — the dump session is not instrumented — and
    // skips the assertion. A row whose value is not a number is a different
    // thing entirely, and folding it into the same `None` would let this test
    // pass without ever reading the session it exists to read.
    let Some(value) = value else {
        return Ok(None);
    };
    let seconds = value.parse::<u64>().map_err(|e| {
        anyhow!(
            "performance_schema reported net_write_timeout = `{value}`, which is not a number: {e}"
        )
    })?;
    Ok(Some(seconds))
}

/// Wait for the dump thread id to change, i.e. the pump has reconnected.
async fn wait_for_dump_thread_change(
    pool: &mysql_async::Pool,
    previous: u64,
) -> Result<(), anyhow::Error> {
    let reconnected = wait_until_true(CHANGE_PROPAGATION_TIMEOUT, || async {
        matches!(binlog_dump_thread_id(pool).await, Ok(Some(id)) if id != previous)
    })
    .await;
    if reconnected {
        Ok(())
    } else {
        Err(anyhow!(
            "timed out waiting for a dump thread other than {previous}; \
             the pump either never reconnected or has no dump thread at all"
        ))
    }
}

async fn mysql_scalar_i64(pool: &mysql_async::Pool, sql: &str) -> Result<i64, anyhow::Error> {
    let mut conn = pool.get_conn().await?;
    conn.query_first(sql)
        .await?
        .ok_or_else(|| anyhow!("no rows from source query `{sql}`"))
}

/// Bump every `repl_counters` row `rounds` times, one statement per row so each
/// update is its own transaction. `MySQL` emits one envelope per transaction per
/// table, so single-row transactions give the pump many envelopes to coalesce; a
/// set-based `UPDATE` per round would produce a fraction of them and lose the
/// overlap this test depends on.
async fn bump_counter_rows(pool: &mysql_async::Pool, rounds: i64) -> Result<(), anyhow::Error> {
    for round in 0..rounds {
        for id in 1..=RECONNECT_ROWS {
            exec(
                pool,
                &format!(
                    "UPDATE repl_counters SET update_count = update_count + 1, \
                     payload = 'round-{round}' WHERE c_id = {id}"
                ),
            )
            .await?;
        }
    }
    Ok(())
}

/// Killing the dump connection mid-stream makes the pump resume from its ack
/// floor and re-send everything delivered but not yet durably applied. An update
/// past the floor is lost, and a re-sent older image landing over a newer one
/// leaves the row stale — both keep the row count correct, so only an aggregate
/// detects them. Every update is worth `+1`, making the final `SUM` exact, and
/// `MIN` catches one row left behind even if another offsets the sum.
///
/// Coalescing is pinned wide so the consumer is holding a batch of not-yet-durable
/// updates when the kill lands.
#[cfg(not(target_os = "windows"))]
#[tokio::test(flavor = "multi_thread")]
async fn mysql_binlog_replication_survives_a_dump_reconnect_cayenne() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some(
        "integration=debug,runtime=debug,data_components::mysql_replication=debug,info",
    ));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let _container = common::start_mysql_docker_container(MYSQL_E2E_RECONNECT_PORT)
                .await
                .map_err(|e| anyhow!("start container: {e}"))?;

            let pool = common::get_mysql_conn(MYSQL_E2E_RECONNECT_PORT)?;
            exec(&pool, RECONNECT_DDL).await?;
            for id in 1..=RECONNECT_ROWS {
                exec(
                    &pool,
                    &format!("INSERT INTO repl_counters VALUES ({id}, 0, 'seed')"),
                )
                .await?;
            }

            let temp_dir = tempfile::tempdir()?;
            let data_dir = temp_dir.path().join("cayenne");
            std::fs::create_dir_all(&data_dir)?;
            let accel_params = HashMap::from([
                (
                    "cayenne_file_path".to_string(),
                    data_dir.display().to_string(),
                ),
                (
                    "cayenne_metadata_dir".to_string(),
                    temp_dir.path().join("metadata.db").display().to_string(),
                ),
                // Widen coalescing so one write carries many envelopes.
                ("cdc_prefetch_buffer".to_string(), "16384".to_string()),
                (
                    "cdc_max_coalesced_envelopes".to_string(),
                    "16384".to_string(),
                ),
                ("cdc_max_coalesce_age_ms".to_string(), "2000".to_string()),
            ]);
            let dataset = ReplicatedDataset {
                dataset_name: "counters",
                table: "mysqldb.repl_counters",
                primary_key: "c_id",
                expected_initial_count: u64::try_from(RECONNECT_ROWS)?,
            };
            let engine = EngineConfig {
                engine: "cayenne",
                mode: spicepod::acceleration::Mode::File,
                accel_params,
            };
            let app = AppBuilder::new("mysql_replication_reconnect")
                .with_dataset(make_dataset(
                    &dataset,
                    &mysql_params(MYSQL_E2E_RECONNECT_PORT),
                    &engine,
                ))
                .build();

            configure_test_datafusion();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);
            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(90)) => {
                    return Err(anyhow!("timed out waiting for the dataset to load"));
                }
                () = Arc::clone(&rt).load_components() => {}
            }
            runtime_ready_check(&rt).await;
            wait_for_scalar_i64(
                &rt,
                "SELECT count(*) FROM counters",
                i64::try_from(dataset.expected_initial_count)?,
            )
            .await?;

            // The dump thread may register a moment after the first rows land,
            // so wait for it rather than racing it.
            let dump_thread = wait_for_binlog_dump_thread(&pool).await?;

            // Regression test for #13307, and the reason it is here rather than
            // in a unit test: the floor was expressed as an expression MySQL
            // refuses for a system variable, and every unit test asserted the
            // SQL Spice generated rather than what the server did with it, so
            // they passed while the session kept the 60s default. Read the value
            // back off the dump session itself.
            match session_net_write_timeout(&pool, dump_thread).await? {
                Some(seconds) => assert!(
                    seconds >= EXPECTED_DUMP_NET_WRITE_TIMEOUT_SECS,
                    "the dump session must carry the raised net_write_timeout, \
                     got {seconds}s (the server default is 60s, so this means the \
                     source rejected or ignored the statement that raises it)"
                ),
                None => eprintln!(
                    "skipped the net_write_timeout assertion: performance_schema \
                     did not report the dump session's value"
                ),
            }

            let half = RECONNECT_UPDATES_PER_ROW / 2;
            bump_counter_rows(&pool, half).await?;

            // Kill the dump connection mid-stream, then keep writing so the
            // replay window overlaps live traffic.
            exec(&pool, &format!("KILL {dump_thread}")).await?;
            bump_counter_rows(&pool, RECONNECT_UPDATES_PER_ROW - half).await?;
            wait_for_dump_thread_change(&pool, dump_thread).await?;

            // Every update is worth exactly +1, so the source total is the
            // yardstick: a stale image winning anywhere leaves Spice short.
            let expected_total = mysql_scalar_i64(
                &pool,
                "SELECT CAST(SUM(update_count) AS SIGNED) FROM repl_counters",
            )
            .await?;
            assert_eq!(
                expected_total,
                RECONNECT_ROWS * RECONNECT_UPDATES_PER_ROW,
                "the workload must have applied every update on the source"
            );
            wait_for_scalar_i64(
                &rt,
                "SELECT SUM(update_count) FROM counters",
                expected_total,
            )
            .await?;
            assert_eq!(
                scalar_i64(&rt, "SELECT min(update_count) FROM counters").await?,
                RECONNECT_UPDATES_PER_ROW,
                "every row must carry its final value, not an earlier one"
            );

            pool.disconnect().await?;
            Ok(())
        })
        .await
}

/// One column per decode branch the row-image decoder has to get right:
/// every integer width signed and unsigned, both decimal metadata shapes, the
/// temporal types at several fractional-second precisions, both `VARCHAR`
/// length-prefix widths, every blob length width, and the types whose value is
/// resolved from table-map metadata rather than the wire type (`BIT`, `ENUM`,
/// `SET`).
const TYPES_DDL: &str = r"CREATE TABLE repl_types (
        t_id      INT PRIMARY KEY,
        c_tiny    TINYINT,          c_tiny_u   TINYINT UNSIGNED,
        c_small   SMALLINT,         c_small_u  SMALLINT UNSIGNED,
        c_medium  MEDIUMINT,        c_medium_u MEDIUMINT UNSIGNED,
        c_int     INT,              c_int_u    INT UNSIGNED,
        c_big     BIGINT,           c_big_u    BIGINT UNSIGNED,
        c_float   FLOAT,            c_double   DOUBLE,
        c_dec     DECIMAL(6,2),     c_dec_wide DECIMAL(20,8),
        c_date    DATE,             c_time     TIME(3),
        c_dt      DATETIME(6),      c_ts       TIMESTAMP(3) NULL,
        c_year    YEAR,
        c_char    CHAR(10),         c_varchar  VARCHAR(64),
        c_varchar_long VARCHAR(300),
        c_utf     VARCHAR(10) CHARACTER SET utf8mb4,
        c_bin     BINARY(4),        c_varbin   VARBINARY(64),
        c_tinyblob TINYBLOB,        c_blob     BLOB,
        c_medblob MEDIUMBLOB,       c_longblob LONGBLOB,
        c_tinytext TINYTEXT,        c_text     TEXT,
        c_bit     BIT(9),
        c_enum    ENUM('alpha','beta','gamma'),
        c_set     SET('x','y','z'),
        c_json    JSON
    )";

/// Row 1 carries ordinary values, row 2 the width boundaries, row 3 is NULL in
/// every nullable column — which also exercises the null bitmap either side of
/// its first byte, since the table is far wider than eight columns.
const TYPES_SEED: &[&str] = &[
    r#"INSERT INTO repl_types VALUES (1,
        -12, 200, -1234, 60000, -8000000, 16000000, -70000, 4000000000,
        -5000000000, 9000000000, 1.5, 2.25, 1234.56, 12.34567890,
        '2026-07-30', '12:34:56.789', '2026-07-30 12:34:56.123456',
        '2026-07-30 12:34:56.123', 2026,
        'char', 'varchar', REPEAT('x', 300), 'ünïcødé',
        'abcd', 'varbinary', 'tiny', 'blob', 'medium', 'long',
        'tinytext', 'text', b'101010101', 'beta', 'x,z', '{"k": 7}')"#,
    // The signed minimum and unsigned maximum at each width. `c_big_u` stops at
    // i64::MAX and `c_time` at the end of the day; see the note on the test.
    r"INSERT INTO repl_types VALUES (2,
        -128, 255, -32768, 65535, -8388608, 16777215, -2147483648, 4294967295,
        -9223372036854775808, 9223372036854775807, 0, 0, -9999.99, -99999999999.99999999,
        '1000-01-01', '23:59:59.999', '1000-01-01 00:00:00.000000',
        '1970-01-02 00:00:01.000', 1901,
        '', '', '', '',
        '\0\0\0\0', '', '', '', '', '', '', '',
        b'0', 'alpha', '', '[]')",
    "INSERT INTO repl_types (t_id) VALUES (3)",
];

/// `(sql, expected)` checks run against the accelerator. Every check returns a
/// single integer so one helper covers the whole matrix: values are compared
/// directly, text and binary by length or equality, so nothing depends on how
/// either engine formats a value.
const TYPES_CHECKS: &[(&str, i64)] = &[
    ("SELECT count(*) FROM types", 3),
    // Signed columns keep their sign; unsigned columns above the signed range
    // are the case that silently corrupts if the table map's signedness block
    // is ever misread.
    (
        "SELECT CAST(c_tiny AS BIGINT) FROM types WHERE t_id = 1",
        -12,
    ),
    (
        "SELECT CAST(c_tiny_u AS BIGINT) FROM types WHERE t_id = 1",
        200,
    ),
    (
        "SELECT CAST(c_small AS BIGINT) FROM types WHERE t_id = 1",
        -1234,
    ),
    (
        "SELECT CAST(c_small_u AS BIGINT) FROM types WHERE t_id = 1",
        60000,
    ),
    (
        "SELECT CAST(c_medium AS BIGINT) FROM types WHERE t_id = 1",
        -8_000_000,
    ),
    (
        "SELECT CAST(c_medium_u AS BIGINT) FROM types WHERE t_id = 1",
        16_000_000,
    ),
    (
        "SELECT CAST(c_int AS BIGINT) FROM types WHERE t_id = 1",
        -70000,
    ),
    (
        "SELECT CAST(c_int_u AS BIGINT) FROM types WHERE t_id = 1",
        4_000_000_000,
    ),
    (
        "SELECT CAST(c_big AS BIGINT) FROM types WHERE t_id = 1",
        -5_000_000_000,
    ),
    // Floats scaled to an integer, which is exact for these values.
    (
        "SELECT CAST(c_float * 2 AS BIGINT) FROM types WHERE t_id = 1",
        3,
    ),
    (
        "SELECT CAST(c_double * 4 AS BIGINT) FROM types WHERE t_id = 1",
        9,
    ),
    (
        "SELECT CAST(c_tiny AS BIGINT) FROM types WHERE t_id = 2",
        -128,
    ),
    (
        "SELECT CAST(c_tiny_u AS BIGINT) FROM types WHERE t_id = 2",
        255,
    ),
    (
        "SELECT CAST(c_small AS BIGINT) FROM types WHERE t_id = 2",
        -32768,
    ),
    (
        "SELECT CAST(c_small_u AS BIGINT) FROM types WHERE t_id = 2",
        65535,
    ),
    (
        "SELECT CAST(c_medium AS BIGINT) FROM types WHERE t_id = 2",
        -8_388_608,
    ),
    (
        "SELECT CAST(c_medium_u AS BIGINT) FROM types WHERE t_id = 2",
        16_777_215,
    ),
    (
        "SELECT CAST(c_int AS BIGINT) FROM types WHERE t_id = 2",
        -2_147_483_648,
    ),
    (
        "SELECT CAST(c_int_u AS BIGINT) FROM types WHERE t_id = 2",
        4_294_967_295,
    ),
    (
        "SELECT CAST(c_big AS BIGINT) FROM types WHERE t_id = 2",
        i64::MIN,
    ),
    (
        "SELECT CAST(c_big_u AS BIGINT) FROM types WHERE t_id = 2",
        i64::MAX,
    ),
    // Decimal scale comes from table-map metadata, so a decoder that ignored it
    // would still produce a plausible number.
    (
        "SELECT CAST(c_dec * 100 AS BIGINT) FROM types WHERE t_id = 1",
        123_456,
    ),
    (
        "SELECT CAST(c_dec_wide * 100000000 AS BIGINT) FROM types WHERE t_id = 1",
        1_234_567_890,
    ),
    // Temporal: date, fractional seconds at 3 and 6 digits, and YEAR.
    (
        "SELECT count(*) FROM types WHERE c_date = DATE '2026-07-30'",
        1,
    ),
    (
        "SELECT count(*) FROM types \
         WHERE c_dt = TIMESTAMP '2026-07-30 12:34:56.123456'",
        1,
    ),
    (
        "SELECT count(*) FROM types WHERE c_ts = TIMESTAMP '2026-07-30 12:34:56.123'",
        1,
    ),
    (
        "SELECT CAST(c_year AS BIGINT) FROM types WHERE t_id = 1",
        2026,
    ),
    // `TIME` as nanoseconds since midnight, which is exact and independent of
    // how either engine formats the value: 12:34:56.789 and the last
    // millisecond of the day.
    (
        "SELECT CAST(c_time AS BIGINT) FROM types WHERE t_id = 1",
        45_296_789_000_000,
    ),
    (
        "SELECT CAST(c_time AS BIGINT) FROM types WHERE t_id = 2",
        86_399_999_000_000,
    ),
    // Strings: the 1-byte and 2-byte VARCHAR length prefixes, and a multi-byte
    // charset where character count and byte count differ.
    (
        "SELECT CAST(character_length(c_varchar_long) AS BIGINT) FROM types WHERE t_id = 1",
        300,
    ),
    (
        "SELECT CAST(character_length(c_utf) AS BIGINT) FROM types WHERE t_id = 1",
        7,
    ),
    ("SELECT count(*) FROM types WHERE c_varchar = 'varchar'", 1),
    ("SELECT count(*) FROM types WHERE c_char = 'char'", 1),
    // Every blob length width (1, 2, 3 and 4 byte prefixes), plus the binary
    // string types. A misread length prefix changes the bytes, so comparing the
    // value covers the prefix too. The cast normalizes `Binary`, `LargeBinary`,
    // `Utf8` and `LargeUtf8`, which is how the four widths and the binary flag
    // land in Arrow; every value here is ASCII, so the cast is exact.
    (
        "SELECT count(*) FROM types WHERE CAST(c_tinyblob AS VARCHAR) = 'tiny'",
        1,
    ),
    (
        "SELECT count(*) FROM types WHERE CAST(c_blob AS VARCHAR) = 'blob'",
        1,
    ),
    (
        "SELECT count(*) FROM types WHERE CAST(c_medblob AS VARCHAR) = 'medium'",
        1,
    ),
    (
        "SELECT count(*) FROM types WHERE CAST(c_longblob AS VARCHAR) = 'long'",
        1,
    ),
    (
        "SELECT count(*) FROM types WHERE CAST(c_bin AS VARCHAR) = 'abcd'",
        1,
    ),
    (
        "SELECT count(*) FROM types WHERE CAST(c_varbin AS VARCHAR) = 'varbinary'",
        1,
    ),
    (
        "SELECT count(*) FROM types WHERE CAST(c_tinytext AS VARCHAR) = 'tinytext'",
        1,
    ),
    ("SELECT count(*) FROM types WHERE c_text = 'text'", 1),
    // Values resolved from table-map metadata rather than the wire type: the
    // ENUM/SET variant lists and the BIT width all live there, so a decoder
    // that read the wire type alone could not produce these.
    ("SELECT count(*) FROM types WHERE c_enum = 'beta'", 1),
    ("SELECT count(*) FROM types WHERE c_enum = 'alpha'", 1),
    ("SELECT count(*) FROM types WHERE c_set = 'x,z'", 1),
    (
        "SELECT CAST(c_bit AS BIGINT) FROM types WHERE t_id = 1",
        341,
    ),
    // JSON arrives as `JSONB` and is decoded to text, so the content has to be
    // checked, not just its presence. The empty array is compared exactly; the
    // object is matched on key and value, since `MySQL` chooses the spacing.
    (r"SELECT count(*) FROM types WHERE c_json = '[]'", 1),
    (
        r#"SELECT count(*) FROM types WHERE c_json LIKE '{%"k"%7%}'"#,
        1,
    ),
    // The all-NULL row: every nullable column is NULL, and the primary key is
    // still readable.
    ("SELECT count(c_tiny) FROM types", 2),
    ("SELECT count(c_varchar_long) FROM types", 2),
    ("SELECT count(c_json) FROM types", 2),
    ("SELECT count(*) FROM types WHERE t_id = 3", 1),
];

/// Every `MySQL` column type through the binlog decode path, checked against a
/// real server. Each value is asserted from the snapshot and again after a
/// binlog INSERT, then an UPDATE covers the before/after row images.
///
/// Two columns stop short of `MySQL`'s range because the Arrow type cannot hold
/// it: `c_big_u` at `i64::MAX`, and `c_time` at the end of the day, since
/// `Time64` is a time-of-day while `TIME` spans ±838 hours. Negative `TIME` is
/// rejected with a structured error, covered by `negative_time_errors` in
/// `data_components::mysql_replication::rows`.
#[cfg(not(target_os = "windows"))]
#[tokio::test(flavor = "multi_thread")]
async fn mysql_binlog_replication_decodes_every_column_type_cayenne() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some(
        "integration=debug,runtime=debug,data_components::mysql_replication=debug,info",
    ));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let _container = common::start_mysql_docker_container(MYSQL_E2E_TYPES_PORT)
                .await
                .map_err(|e| anyhow!("start container: {e}"))?;

            let pool = common::get_mysql_conn(MYSQL_E2E_TYPES_PORT)?;
            exec(&pool, TYPES_DDL).await?;
            for seed in TYPES_SEED {
                exec(&pool, seed).await?;
            }

            let temp_dir = tempfile::tempdir()?;
            let data_dir = temp_dir.path().join("cayenne");
            std::fs::create_dir_all(&data_dir)?;
            let accel_params = HashMap::from([
                (
                    "cayenne_file_path".to_string(),
                    data_dir.display().to_string(),
                ),
                (
                    "cayenne_metadata_dir".to_string(),
                    temp_dir.path().join("metadata.db").display().to_string(),
                ),
            ]);
            let dataset = ReplicatedDataset {
                dataset_name: "types",
                table: "mysqldb.repl_types",
                primary_key: "t_id",
                expected_initial_count: 3,
            };
            let app = AppBuilder::new("mysql_replication_types")
                .with_dataset(make_dataset(
                    &dataset,
                    &mysql_params(MYSQL_E2E_TYPES_PORT),
                    &EngineConfig {
                        engine: "cayenne",
                        mode: spicepod::acceleration::Mode::File,
                        accel_params,
                    },
                ))
                .build();

            configure_test_datafusion();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);
            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(90)) => {
                    return Err(anyhow!("timed out waiting for the dataset to load"));
                }
                () = Arc::clone(&rt).load_components() => {}
            }
            runtime_ready_check(&rt).await;

            // The snapshot path first. TYPES_CHECKS opens with the row count,
            // so waiting on it here covers the whole list settling.
            wait_for_scalar_i64(&rt, TYPES_CHECKS[0].0, TYPES_CHECKS[0].1).await?;
            for (sql, expected) in TYPES_CHECKS {
                assert_eq!(scalar_i64(&rt, sql).await?, *expected, "snapshot: `{sql}`");
            }

            // Then the same values again through the binlog INSERT path.
            exec(&pool, "DELETE FROM repl_types").await?;
            wait_for_scalar_i64(&rt, "SELECT count(*) FROM types", 0).await?;
            for seed in TYPES_SEED {
                exec(&pool, seed).await?;
            }
            wait_for_scalar_i64(&rt, "SELECT count(*) FROM types", 3).await?;
            for (sql, expected) in TYPES_CHECKS {
                assert_eq!(
                    scalar_i64(&rt, sql).await?,
                    *expected,
                    "binlog insert: `{sql}`"
                );
            }

            // An UPDATE carries a full before and after image of the row, so
            // changing a few columns still decodes every column twice.
            exec(
                &pool,
                "UPDATE repl_types SET c_tiny = 7, c_big_u = 42, c_varchar = 'updated', \
                 c_varchar_long = REPEAT('y', 300), c_enum = 'gamma', c_dec = 1.00 \
                 WHERE t_id = 1",
            )
            .await?;
            wait_for_scalar_i64(
                &rt,
                "SELECT CAST(c_tiny AS BIGINT) FROM types WHERE t_id = 1",
                7,
            )
            .await?;
            assert_eq!(
                scalar_i64(&rt, "SELECT count(*) FROM types WHERE c_enum = 'gamma'").await?,
                1,
                "an updated ENUM must resolve to its new variant"
            );
            assert_eq!(
                scalar_i64(
                    &rt,
                    "SELECT CAST(length(c_varchar_long) AS BIGINT) FROM types WHERE t_id = 1"
                )
                .await?,
                300,
                "a two-byte-length VARCHAR must survive an update"
            );
            // The rest of the columns the UPDATE touched, so an update applied
            // partially or not at all cannot pass.
            assert_eq!(
                scalar_i64(
                    &rt,
                    "SELECT CAST(c_big_u AS BIGINT) FROM types WHERE t_id = 1"
                )
                .await?,
                42,
                "an updated BIGINT UNSIGNED must carry its new value"
            );
            assert_eq!(
                scalar_i64(
                    &rt,
                    "SELECT count(*) FROM types WHERE c_varchar = 'updated'"
                )
                .await?,
                1,
                "an updated VARCHAR must carry its new value"
            );
            assert_eq!(
                scalar_i64(
                    &rt,
                    "SELECT CAST(c_dec * 100 AS BIGINT) FROM types WHERE t_id = 1"
                )
                .await?,
                100,
                "an updated DECIMAL must keep its scale"
            );

            // And a DELETE, whose row image is the full before-image.
            exec(&pool, "DELETE FROM repl_types WHERE t_id = 2").await?;
            wait_for_scalar_i64(&rt, "SELECT count(*) FROM types", 2).await?;

            pool.disconnect().await?;
            Ok(())
        })
        .await
}
