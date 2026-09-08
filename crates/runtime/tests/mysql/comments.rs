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

//! Integration tests verifying that `MySQL` table and column comments are
//! accessible via `obj_description` and `col_description` UDFs when the dataset
//! is loaded with `DuckDB` acceleration.

use std::{sync::Arc, time::Duration};

use app::AppBuilder;
use datafusion::assert_batches_eq;
use mysql_async::prelude::Queryable;
use spicepod::{
    acceleration::{Acceleration, RefreshMode},
    component::dataset::Dataset,
};

use util::{RetryError, fibonacci_backoff::FibonacciBackoffBuilder, retry};

use crate::{
    configure_test_datafusion, init_tracing,
    mysql::common::{get_mysql_conn, make_mysql_dataset, start_mysql_docker_container},
    utils::{register_test_connectors, run_query, runtime_ready_check, test_request_context},
};

const MYSQL_COMMENTS_PORT: u16 = 13320;

fn commented_dataset(port: u16) -> Dataset {
    let mut ds = make_mysql_dataset("orders", "orders", port, false);
    ds.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some("duckdb".to_string()),
        refresh_mode: Some(RefreshMode::Full),
        ..Acceleration::default()
    });
    ds
}

/// Waits until `MySQL`'s `InnoDB` is ready to execute DDL by probing with a trivial
/// CREATE/DROP. `mysqladmin ping` and even `SELECT 1` can succeed before the data
/// dictionary is fully initialized, causing the first real CREATE TABLE to block
/// indefinitely. Each probe attempt has a short timeout so abandoned connections
/// don't pile up; the loop retries until success or the overall deadline is hit.
async fn wait_for_ddl_ready(port: u16) -> Result<(), anyhow::Error> {
    tracing::debug!("wait_for_ddl_ready: probing InnoDB DDL readiness on port {port}");
    let deadline = tokio::time::Instant::now() + Duration::from_mins(3);
    let mut attempt = 0u32;
    loop {
        attempt += 1;
        if tokio::time::Instant::now() >= deadline {
            return Err(anyhow::anyhow!(
                "MySQL InnoDB not ready for DDL after 180s ({attempt} attempts)"
            ));
        }
        let pool = get_mysql_conn(port)?;
        let result = tokio::time::timeout(Duration::from_secs(10), async {
            let mut conn = pool
                .get_conn()
                .await
                .map_err(|e| anyhow::anyhow!("get_conn: {e}"))?;
            conn.query_drop("CREATE TABLE IF NOT EXISTS _ddl_probe (id INT PRIMARY KEY)")
                .await
                .map_err(|e| anyhow::anyhow!("probe CREATE: {e}"))?;
            conn.query_drop("DROP TABLE IF EXISTS _ddl_probe")
                .await
                .map_err(|e| anyhow::anyhow!("probe DROP: {e}"))
        })
        .await;
        // Ignore disconnect errors — the connection may already be gone after a timeout.
        let _ = pool.disconnect().await;
        match result {
            Ok(Ok(())) => {
                tracing::debug!("wait_for_ddl_ready: InnoDB ready (attempt {attempt})");
                return Ok(());
            }
            Ok(Err(e)) => {
                tracing::debug!("wait_for_ddl_ready: attempt {attempt} error: {e}");
            }
            Err(_elapsed) => {
                tracing::debug!(
                    "wait_for_ddl_ready: attempt {attempt} timed out, InnoDB not ready yet"
                );
            }
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

async fn seed_orders(port: u16) -> Result<(), anyhow::Error> {
    let pool = get_mysql_conn(port)?;
    {
        let mut conn = pool.get_conn().await?;
        conn.query_drop("DROP TABLE IF EXISTS orders").await?;
        conn.query_drop(
            "CREATE TABLE orders (
                id       INT AUTO_INCREMENT PRIMARY KEY COMMENT 'Unique order identifier',
                customer TEXT NOT NULL                  COMMENT 'Customer name or identifier',
                amount   DECIMAL(10,2) NOT NULL         COMMENT 'Order total in USD'
            ) COMMENT='Customer purchase orders'",
        )
        .await?;
        conn.query_drop(
            "INSERT INTO orders (customer, amount) VALUES ('Alice', 42.50), ('Bob', 18.00)",
        )
        .await?;
    } // conn returned to pool here
    pool.disconnect().await?;
    Ok(())
}

async fn start_runtime(dataset: Dataset) -> Result<Arc<runtime::Runtime>, anyhow::Error> {
    register_test_connectors().await;
    let app = AppBuilder::new("mysql_comments_test")
        .with_dataset(dataset)
        .build();

    configure_test_datafusion();
    let rt = Arc::new(runtime::Runtime::builder().with_app(app).build().await);

    tokio::select! {
        () = tokio::time::sleep(Duration::from_mins(2)) => {
            return Err(anyhow::anyhow!("Timed out waiting for dataset to load"));
        }
        () = Arc::clone(&rt).load_components() => {}
    }

    runtime_ready_check(&rt).await;
    Ok(rt)
}

/// `obj_description('orders')` returns the `MySQL` table comment and
/// `col_description('orders', N)` returns `MySQL` column comments by 1-based
/// position when the dataset is accelerated with `DuckDB`.
#[tokio::test]
async fn test_mysql_comments_with_duckdb_acceleration() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let _container = start_mysql_docker_container(MYSQL_COMMENTS_PORT).await?;

            wait_for_ddl_ready(MYSQL_COMMENTS_PORT).await?;

            let retry_strategy = FibonacciBackoffBuilder::new().max_retries(Some(3)).build();
            retry(retry_strategy, || async {
                seed_orders(MYSQL_COMMENTS_PORT)
                    .await
                    .map_err(RetryError::transient)
            })
            .await?;

            let rt = start_runtime(commented_dataset(MYSQL_COMMENTS_PORT)).await?;

            let obj_results =
                run_query(&rt, "SELECT obj_description('orders') AS table_comment").await?;
            assert_batches_eq!(
                &[
                    "+--------------------------+",
                    "| table_comment            |",
                    "+--------------------------+",
                    "| Customer purchase orders |",
                    "+--------------------------+",
                ],
                &obj_results
            );

            let col_results = run_query(
                &rt,
                "SELECT col_description('orders', 1) AS c1, \
                        col_description('orders', 2) AS c2, \
                        col_description('orders', 3) AS c3",
            )
            .await?;
            assert_batches_eq!(
                &[
                    "+-------------------------+-----------------------------+--------------------+",
                    "| c1                      | c2                          | c3                 |",
                    "+-------------------------+-----------------------------+--------------------+",
                    "| Unique order identifier | Customer name or identifier | Order total in USD |",
                    "+-------------------------+-----------------------------+--------------------+",
                ],
                &col_results
            );

            Ok(())
        })
        .await
}
