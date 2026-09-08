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

//! Integration test for schema inference on the `PostgreSQL` connector.
//!
//! A real `PostgreSQL` table is seeded with a composite primary key and a variety of
//! secondary indexes — unique, non-unique, partial, expression, and a clustered
//! (DESC) index. The dataset is then accelerated with `DuckDB`, exercising the full
//! always-on pipeline end-to-end: the `pg_catalog` inference query (the riskiest
//! SQL) must run on the real server, and the inferred settings must be accepted by
//! the accelerator without error. For `DuckDB` + full refresh, inferred physical
//! constraints (primary key / indexes) are intentionally *not* applied (its
//! versioned internal-table rebuild rejects them on the second refresh); only the
//! inferred sort order flows through — so the test also triggers a second refresh,
//! which is exactly where an inferred constraint would blow up.
//!
//! Precise value-level checks of the inference mapping live in fast unit tests:
//! `data_components::inferred_schema` (the wire contract) and
//! `runtime::component::dataset::schema_inference` (the apply logic per engine +
//! refresh mode).

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

/// Build an `inventory` dataset accelerated with `DuckDB` (full refresh). Schema
/// inference is always on, so there is no level to configure.
fn inventory_dataset(port: usize) -> Dataset {
    let mut ds = Dataset::new("postgres:inventory".to_string(), "inventory".to_string());
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
        () = tokio::time::sleep(Duration::from_mins(2)) => {
            return Err(anyhow::anyhow!("Timed out waiting for dataset to load"));
        }
        () = Arc::clone(&rt).load_components() => {}
    }

    runtime_ready_check(&rt).await;
    Ok(rt)
}

/// Trigger a refresh and wait (bounded) for it to complete via the completion
/// notifier, so a refresh that never finishes fails fast with a clear message
/// instead of hanging until the CI job times out.
async fn refresh_dataset(rt: &runtime::Runtime, name: &str) -> Result<(), anyhow::Error> {
    let notifier = rt
        .datafusion()
        .refresh_table(&datafusion::common::TableReference::from(name), None)
        .await?;
    let notify = notifier.ok_or_else(|| anyhow::anyhow!("no completion notifier for {name}"))?;
    tokio::time::timeout(Duration::from_mins(1), notify.wait())
        .await
        .map_err(|_| {
            anyhow::anyhow!("timed out after 1 minute waiting for {name} refresh to complete")
        })?;
    Ok(())
}

/// The rich-index table loads end-to-end and returns correct data — proving the
/// always-on `pg_catalog` inference query runs on the real server and the inferred
/// settings that apply to `DuckDB` + full refresh (the sort order; physical
/// constraints are intentionally skipped for this engine/mode) are accepted.
///
/// A second, manually-triggered refresh is part of the test: `DuckDB` verifies
/// declared constraints against the internal table left by the previous load, so a
/// wrongly-applied inferred primary key or index only fails on the SECOND refresh —
/// the initial load alone cannot catch it (this is the merge-queue regression from
/// #11880).
#[tokio::test]
async fn test_schema_inference_loads_and_queries() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let port = common::get_random_port()?;
            let _container = common::start_postgres_docker_container(port).await?;

            seed_inventory(port).await?;

            let rt = start_runtime(inventory_dataset(port)).await?;

            // The whole inference pipeline must succeed end-to-end: the pg_catalog
            // inference query runs on the real server, and the inferred settings
            // applied for DuckDB + full refresh are accepted by the accelerator. A
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

            // Second refresh: DuckDB rebuilds its internal table and verifies the
            // declared constraints against the previous one — this is the step that
            // rejects wrongly-inferred physical constraints, so it must succeed and
            // still return correct data.
            refresh_dataset(&rt, "inventory").await?;

            let after_refresh = run_query(&rt, "SELECT COUNT(*) AS n FROM inventory").await?;
            assert_batches_eq!(
                &[
                    "+---+", //
                    "| n |", //
                    "+---+", //
                    "| 3 |", //
                    "+---+", //
                ],
                &after_refresh
            );

            Ok(())
        })
        .await
}
