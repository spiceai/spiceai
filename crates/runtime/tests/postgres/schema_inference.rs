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

//! Integration tests for extended schema inference on the `PostgreSQL` connector.
//!
//! A real `PostgreSQL` table is seeded with a composite primary key and a variety of
//! secondary indexes — unique, non-unique, partial, expression, and a clustered
//! (DESC) index. The dataset is then accelerated with `DuckDB` under
//! `schema_inference: extended`, exercising the full pipeline end-to-end: the
//! `pg_catalog` query (the riskiest new SQL) must run on the real server, and the
//! inferred primary key / indexes / sort order must be accepted by the accelerator
//! without error. A `standard` (default) control confirms inference is opt-in.
//!
//! Precise value-level checks of the inference mapping live in fast unit tests:
//! `data_components::inferred_schema` (the wire contract) and
//! `runtime::component::dataset::schema_inference` (the apply logic per engine).

use std::{collections::HashMap, sync::Arc, time::Duration};

use app::AppBuilder;
use datafusion::assert_batches_eq;
use secrecy::ExposeSecret;
use spicepod::{
    acceleration::{Acceleration, RefreshMode},
    component::dataset::{Dataset, SchemaInference},
    param::Params,
};

use crate::{
    configure_test_datafusion, init_tracing,
    postgres::common::{self, get_pg_params},
    utils::{register_test_connectors, run_query, runtime_ready_check, test_request_context},
};

/// Build an `inventory` dataset accelerated with `DuckDB` (full refresh) at the
/// given schema-inference level.
fn inventory_dataset(port: usize, schema_inference: SchemaInference) -> Dataset {
    let mut ds = Dataset::new("postgres:inventory".to_string(), "inventory".to_string());
    ds.params = Some(Params::from_string_map(
        get_pg_params(port)
            .into_iter()
            .map(|(k, v)| (k, v.expose_secret().to_string()))
            .collect::<HashMap<String, String>>(),
    ));
    ds.schema_inference = schema_inference;
    ds.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some("duckdb".to_string()),
        refresh_mode: Some(RefreshMode::Full),
        ..Acceleration::default()
    });
    ds
}

/// Seed an `inventory` table that exercises every branch of the inference query:
/// a composite primary key, a unique index, a plain index, a partial unique index
/// (must be ignored), an expression index (must be ignored), and a clustered DESC
/// index (drives sort order).
async fn seed_inventory(port: usize) -> Result<(), anyhow::Error> {
    let pool = common::get_postgres_connection_pool(port, None).await?;
    let conn = pool
        .connect_direct()
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    conn.conn
        .execute(
            "CREATE TABLE inventory (
                warehouse_id INT         NOT NULL,
                sku          TEXT        NOT NULL,
                name         TEXT        NOT NULL,
                barcode      TEXT        NOT NULL,
                quantity     INT         NOT NULL,
                active       BOOLEAN     NOT NULL,
                updated_at   TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (warehouse_id, sku)
            )",
            &[],
        )
        .await?;

    conn.conn
        .execute("CREATE UNIQUE INDEX uq_barcode ON inventory (barcode)", &[])
        .await?;
    conn.conn
        .execute("CREATE INDEX idx_quantity ON inventory (quantity)", &[])
        .await?;
    // Partial unique index — not a table-wide guarantee, must be excluded.
    conn.conn
        .execute(
            "CREATE UNIQUE INDEX uq_active_sku ON inventory (sku) WHERE active",
            &[],
        )
        .await?;
    // Expression index — has no plain column, must be excluded.
    conn.conn
        .execute(
            "CREATE INDEX idx_lower_name ON inventory (lower(name))",
            &[],
        )
        .await?;
    // Clustered DESC index — drives the inferred sort order.
    conn.conn
        .execute(
            "CREATE INDEX idx_updated ON inventory (updated_at DESC)",
            &[],
        )
        .await?;
    conn.conn
        .execute("CLUSTER inventory USING idx_updated", &[])
        .await?;

    conn.conn
        .execute(
            // `uq_active_sku` is UNIQUE on (sku) WHERE active, so at most one active
            // row may share a sku — keep sku 'A' across warehouses (exercises the
            // composite PK) but only the warehouse-1 row is active.
            "INSERT INTO inventory (warehouse_id, sku, name, barcode, quantity, active, updated_at) VALUES
                (1, 'A', 'Widget', 'BC1', 10, true,  now()),
                (1, 'B', 'Gadget', 'BC2', 5,  false, now()),
                (2, 'A', 'Widget', 'BC3', 7,  false, now())",
            &[],
        )
        .await?;

    Ok(())
}

async fn start_runtime(dataset: Dataset) -> Result<Arc<runtime::Runtime>, anyhow::Error> {
    register_test_connectors().await;
    let app = AppBuilder::new("postgres_schema_inference_test")
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

/// With `schema_inference: extended`, the rich-index table loads end-to-end and
/// returns correct data — proving the `pg_catalog` query runs on the real server
/// and the inferred primary key, indexes, and sort order are accepted by `DuckDB`.
#[tokio::test]
async fn test_extended_schema_inference_loads_and_queries() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let port = common::get_random_port()?;
            let _container = common::start_postgres_docker_container(port).await?;

            seed_inventory(port).await?;

            let rt = start_runtime(inventory_dataset(port, SchemaInference::Extended)).await?;

            // The whole extended pipeline must succeed end-to-end: the pg_catalog
            // inference query runs on the real server, and the inferred primary key,
            // indexes, and sort order are all accepted by the DuckDB accelerator. A
            // correct row count proves the dataset loaded without any of those steps
            // erroring. (Precise value-level mapping is covered by unit tests.)
            let results = run_query(&rt, "SELECT COUNT(*) AS n FROM inventory").await?;
            assert_batches_eq!(
                &[
                    "+---+", //
                    "| n |", //
                    "+---+", //
                    "| 3 |", //
                    "+---+", //
                ],
                &results
            );

            // A filtered query over the accelerated table also confirms the seed
            // loaded with the expected values (exactly one active row).
            let active = run_query(&rt, "SELECT COUNT(*) AS n FROM inventory WHERE active").await?;
            assert_batches_eq!(
                &[
                    "+---+", //
                    "| n |", //
                    "+---+", //
                    "| 1 |", //
                    "+---+", //
                ],
                &active
            );

            Ok(())
        })
        .await
}

/// With the default `schema_inference: standard`, the same table still loads and
/// queries correctly — inference is opt-in and never required.
#[tokio::test]
async fn test_standard_schema_inference_loads_and_queries() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let port = common::get_random_port()?;
            let _container = common::start_postgres_docker_container(port).await?;

            seed_inventory(port).await?;

            let rt = start_runtime(inventory_dataset(port, SchemaInference::Standard)).await?;

            let results = run_query(&rt, "SELECT COUNT(*) AS n FROM inventory").await?;
            assert_batches_eq!(
                &[
                    "+---+", //
                    "| n |", //
                    "+---+", //
                    "| 3 |", //
                    "+---+", //
                ],
                &results
            );

            Ok(())
        })
        .await
}
