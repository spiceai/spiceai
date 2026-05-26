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

//! Integration tests verifying that PostgreSQL table and column comments are
//! accessible via `obj_description` and `col_description` UDFs when the dataset
//! is loaded with DuckDB acceleration.

use std::{collections::HashMap, sync::Arc, time::Duration};

use app::AppBuilder;
use datafusion::assert_batches_eq;
use secrecy::ExposeSecret;
use spicepod::{
    acceleration::{Acceleration, RefreshMode},
    component::dataset::Dataset,
    param::Params,
};

use crate::{
    configure_test_datafusion, init_tracing,
    postgres::common::{self, get_pg_params},
    utils::{register_test_connectors, run_query, runtime_ready_check, test_request_context},
};

fn commented_dataset(port: usize) -> Dataset {
    let mut ds = Dataset::new("postgres:orders".to_string(), "orders".to_string());
    ds.params = Some(Params::from_string_map(
        get_pg_params(port)
            .into_iter()
            .map(|(k, v)| (k, v.expose_secret().to_string()))
            .collect::<HashMap<String, String>>(),
    ));
    ds.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some("duckdb".to_string()),
        refresh_mode: Some(RefreshMode::Full),
        ..Acceleration::default()
    });
    ds
}

async fn seed_orders(port: usize) -> Result<(), anyhow::Error> {
    let pool = common::get_postgres_connection_pool(port, None).await?;
    let conn = pool
        .connect_direct()
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    conn.conn
        .execute(
            "CREATE TABLE orders (
                id        SERIAL PRIMARY KEY,
                customer  TEXT    NOT NULL,
                amount    NUMERIC NOT NULL
            )",
            &[],
        )
        .await?;

    conn.conn
        .execute(
            "COMMENT ON TABLE  orders          IS 'Customer purchase orders'",
            &[],
        )
        .await?;
    conn.conn
        .execute(
            "COMMENT ON COLUMN orders.id       IS 'Unique order identifier'",
            &[],
        )
        .await?;
    conn.conn
        .execute(
            "COMMENT ON COLUMN orders.customer IS 'Customer name or identifier'",
            &[],
        )
        .await?;
    conn.conn
        .execute(
            "COMMENT ON COLUMN orders.amount   IS 'Order total in USD'",
            &[],
        )
        .await?;

    conn.conn
        .execute(
            "INSERT INTO orders (customer, amount) VALUES ('Alice', 42.50), ('Bob', 18.00)",
            &[],
        )
        .await?;

    Ok(())
}

async fn start_runtime(dataset: Dataset) -> Result<Arc<runtime::Runtime>, anyhow::Error> {
    register_test_connectors().await;
    let app = AppBuilder::new("postgres_comments_test")
        .with_dataset(dataset)
        .build();

    configure_test_datafusion();
    let rt = Arc::new(runtime::Runtime::builder().with_app(app).build().await);

    tokio::select! {
        () = tokio::time::sleep(Duration::from_secs(120)) => {
            return Err(anyhow::anyhow!("Timed out waiting for dataset to load"));
        }
        () = Arc::clone(&rt).load_components() => {}
    }

    runtime_ready_check(&rt).await;
    Ok(rt)
}

/// `obj_description('orders')` returns the PostgreSQL table comment when the
/// dataset is accelerated with DuckDB.
#[tokio::test]
async fn test_postgres_obj_description_with_duckdb_acceleration() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let port = common::get_random_port()?;
            let _container = common::start_postgres_docker_container(port).await?;

            seed_orders(port).await?;

            let rt = start_runtime(commented_dataset(port)).await?;

            let results =
                run_query(&rt, "SELECT obj_description('orders') AS table_comment").await?;
            assert_batches_eq!(
                &[
                    "+--------------------------+",
                    "| table_comment            |",
                    "+--------------------------+",
                    "| Customer purchase orders |",
                    "+--------------------------+",
                ],
                &results
            );

            Ok(())
        })
        .await
}

/// `col_description('orders', N)` returns PostgreSQL column comments by
/// 1-based position when the dataset is accelerated with DuckDB.
#[tokio::test]
async fn test_postgres_col_description_with_duckdb_acceleration() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let port = common::get_random_port()?;
            let _container = common::start_postgres_docker_container(port).await?;

            seed_orders(port).await?;

            let rt = start_runtime(commented_dataset(port)).await?;

            let results = run_query(
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
                &results
            );

            Ok(())
        })
        .await
}
