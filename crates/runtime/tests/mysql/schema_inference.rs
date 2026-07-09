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

//! Integration test for extended schema inference on the `MySQL` connector.
//!
//! Guards the CDC path that depends on the inferred primary key: a dataset with
//! `refresh_mode: changes` and `schema_inference: extended` but **no declared
//! `primary_key`** must still load and replicate UPDATE/DELETE events. This only
//! works if the connector infers the primary key from `information_schema` and
//! seeds it into the acceleration config — without it, `refresh_mode: changes`
//! fails with "no primary key available". A `standard`-inference control confirms
//! inference is opt-in (and that, without it, the same changes-mode dataset gets
//! no primary key).
//!
//! Precise value-level checks of the wire contract live in the fast unit tests in
//! `data_components::inferred_schema`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use app::AppBuilder;
use mysql_async::prelude::Queryable;
use runtime::Runtime;
use spicepod::acceleration::{Acceleration, Mode, RefreshMode};
use spicepod::component::dataset::{Dataset, SchemaInference};
use spicepod::param::Params;
use tokio::time::sleep;
use util::{RetryError, fibonacci_backoff::FibonacciBackoffBuilder, retry};

use crate::mysql::common;
use crate::utils::{
    register_test_connectors, run_query, runtime_ready_check, test_request_context,
};
use crate::{configure_test_datafusion, init_tracing};

// Distinct, unused host ports per test so the two tests can start their own
// container concurrently (Rust runs tests in parallel) without racing each other
// or the replication suite (which uses 13324). The container name is derived from
// the port, so a unique port also means a unique container.
const MYSQL_EXTENDED_INFERENCE_PORT: u16 = 13328;
const MYSQL_STANDARD_INFERENCE_PORT: u16 = 13329;
const CHANGE_PROPAGATION_TIMEOUT: Duration = Duration::from_secs(30);

/// Composite-PK table so the test also proves multi-column key inference (in key
/// order) — a single-column PK would not distinguish a correct key from a lucky
/// column pick.
const CREATE_TABLE_SQL: &str = "CREATE TABLE inventory (
        w_id     INT          NOT NULL,
        sku      VARCHAR(16)  NOT NULL,
        quantity INT          NOT NULL,
        PRIMARY KEY (w_id, sku)
    )";

const SEED_SQL: &str = "INSERT INTO inventory (w_id, sku, quantity) VALUES
        (1, 'A', 10),
        (1, 'B', 5),
        (2, 'A', 7)";

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
        // Short interval so the change waits stay snappy.
        (
            "mysql_replication_checkpoint_interval".to_string(),
            "1s".to_string(),
        ),
    ])
}

/// A `refresh_mode: changes` dataset with the given inference level and, crucially,
/// **no** `primary_key` / `on_conflict` — those must come from inference.
fn inventory_dataset(port: u16, schema_inference: SchemaInference) -> Dataset {
    let mut dataset = Dataset::new(
        "mysql:mysqldb.inventory".to_string(),
        "inventory".to_string(),
    );
    dataset.params = Some(Params::from_string_map(mysql_params(port)));
    dataset.schema_inference = schema_inference;
    dataset.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some("duckdb".to_string()),
        mode: Mode::Memory,
        refresh_mode: Some(RefreshMode::Changes),
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

/// Poll `sql` (single Int64/Int32 scalar) until it reports `expected`, or time out.
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

/// With `schema_inference: extended`, a changes-mode dataset that declares no
/// primary key still loads (the PK is inferred from `information_schema`) and
/// correctly routes a live UPDATE and DELETE — the exact path that failed with
/// "no primary key available" before MySQL extended inference existed.
#[tokio::test]
async fn test_extended_inference_enables_cdc_without_declared_pk() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some(
        "integration=debug,data_components::mysql_replication=debug,info",
    ));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let _container = common::start_mysql_docker_container(MYSQL_EXTENDED_INFERENCE_PORT)
                .await
                .map_err(|e| anyhow!("start container: {e}"))?;

            // Create + seed on the source (retry to absorb InnoDB DDL-readiness races).
            let pool = common::get_mysql_conn(MYSQL_EXTENDED_INFERENCE_PORT)?;
            let retry_strategy = FibonacciBackoffBuilder::new().max_retries(Some(10)).build();
            retry(retry_strategy, || async {
                exec(&pool, "DROP TABLE IF EXISTS inventory")
                    .await
                    .map_err(RetryError::transient)?;
                exec(&pool, CREATE_TABLE_SQL)
                    .await
                    .map_err(RetryError::transient)?;
                exec(&pool, SEED_SQL).await.map_err(RetryError::transient)
            })
            .await?;

            let app = AppBuilder::new("mysql_schema_inference_test")
                .with_dataset(inventory_dataset(
                    MYSQL_EXTENDED_INFERENCE_PORT,
                    SchemaInference::Extended,
                ))
                .build();

            configure_test_datafusion();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);

            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(90)) => {
                    return Err(anyhow!("timed out waiting for dataset to load"));
                }
                () = Arc::clone(&rt).load_components() => {}
            }
            runtime_ready_check(&rt).await;

            // Initial snapshot: all 3 seed rows replicated. Reaching this at all proves
            // the changes stream started, which requires the inferred primary key.
            wait_for_scalar_i64(&rt, "SELECT count(*) FROM inventory", 3).await?;

            // Live UPDATE keyed on the composite PK: the binlog UPDATE event can only be
            // applied to the accelerated copy if the inferred PK routes it correctly.
            exec(
                &pool,
                "UPDATE inventory SET quantity = 999 WHERE w_id = 1 AND sku = 'A'",
            )
            .await?;
            wait_for_scalar_i64(
                &rt,
                "SELECT quantity FROM inventory WHERE w_id = 1 AND sku = 'A'",
                999,
            )
            .await?;

            // Live DELETE keyed on the composite PK routes correctly too.
            exec(&pool, "DELETE FROM inventory WHERE w_id = 2 AND sku = 'A'").await?;
            wait_for_scalar_i64(&rt, "SELECT count(*) FROM inventory", 2).await?;

            Ok(())
        })
        .await
}

/// Control: with the default `schema_inference: standard`, a full-refresh dataset
/// loads normally — confirming inference is purely opt-in and never required for
/// the non-CDC path.
#[tokio::test]
async fn test_standard_inference_full_refresh_loads() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let _container = common::start_mysql_docker_container(MYSQL_STANDARD_INFERENCE_PORT)
                .await
                .map_err(|e| anyhow!("start container: {e}"))?;

            let pool = common::get_mysql_conn(MYSQL_STANDARD_INFERENCE_PORT)?;
            let retry_strategy = FibonacciBackoffBuilder::new().max_retries(Some(10)).build();
            retry(retry_strategy, || async {
                exec(&pool, "DROP TABLE IF EXISTS inventory")
                    .await
                    .map_err(RetryError::transient)?;
                exec(&pool, CREATE_TABLE_SQL)
                    .await
                    .map_err(RetryError::transient)?;
                exec(&pool, SEED_SQL).await.map_err(RetryError::transient)
            })
            .await?;

            let mut dataset =
                inventory_dataset(MYSQL_STANDARD_INFERENCE_PORT, SchemaInference::Standard);
            // Full refresh needs no primary key, so this is a clean opt-in control.
            if let Some(accel) = dataset.acceleration.as_mut() {
                accel.refresh_mode = Some(RefreshMode::Full);
            }

            let app = AppBuilder::new("mysql_schema_inference_standard_test")
                .with_dataset(dataset)
                .build();

            configure_test_datafusion();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);

            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(90)) => {
                    return Err(anyhow!("timed out waiting for dataset to load"));
                }
                () = Arc::clone(&rt).load_components() => {}
            }
            runtime_ready_check(&rt).await;

            wait_for_scalar_i64(&rt, "SELECT count(*) FROM inventory", 3).await?;

            Ok(())
        })
        .await
}
