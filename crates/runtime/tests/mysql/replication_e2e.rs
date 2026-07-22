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
    register_test_connectors, run_query, runtime_ready_check, test_request_context,
};
use crate::{configure_test_datafusion, init_tracing};

const MYSQL_E2E_PORT: u16 = 13322;
#[cfg(not(target_os = "windows"))]
const MYSQL_E2E_CAYENNE_PORT: u16 = 13323;
#[cfg(not(target_os = "windows"))]
const MYSQL_E2E_RESTART_PORT: u16 = 13321;

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
#[ignore = "flaky: the final TRUNCATE step races the Cayenne CDC mem-tier — delete-all (DELETE WHERE TRUE) clears the durable/inlined tiers but not un-checkpointed mem-tier rows, so count stalls at 2 instead of 0. Connector-agnostic Cayenne bug (affects Postgres/Debezium CDC too). Re-enable once Cayenne's delete_from(WHERE TRUE) also clears the mem-tier. Tracking: <ISSUE>"]
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
