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

//! Integration test for catalog-level CDC acceleration (#11850).
//!
//! A `PostgreSQL` catalog configured with `acceleration: { refresh_mode:
//! changes }` should, with zero per-table configuration, bootstrap and
//! CDC-accelerate every discovered table that has a primary key: the table
//! becomes queryable through the catalog's own namespace
//! (`{catalog}.public.<table>`), backed by a synthesized dataset driven
//! through the exact same lifecycle as any spicepod-declared dataset.
//!
//! Every included table must have a primary key -- catalog setup fails
//! naming the table if one is missing.
//!
//! This is the first genuinely workable, end-to-end slice: it proves
//! bootstrap through the catalog path works. It deliberately does not yet
//! assert CDC convergence after a source mutation or a shared replication
//! slot -- those land in follow-up commits.

use std::{collections::HashMap, sync::Arc, time::Duration};

use app::AppBuilder;
use datafusion::assert_batches_eq;
use runtime::Runtime;
use secrecy::ExposeSecret;
use spicepod::{
    component::catalog::{
        Catalog, CatalogAcceleration, CatalogAccelerationEngine, CatalogRefreshMode,
    },
    param::Params,
};

use crate::{
    init_tracing,
    postgres::common::{self, get_pg_params},
    utils::{register_test_connectors, run_query, runtime_ready_check, test_request_context, wait_until_true},
};

const CATALOG_NAME: &str = "pg_accel_e2e";

/// Seed a table with a primary key and a couple of rows.
async fn seed_orders_table(port: usize) -> Result<(), anyhow::Error> {
    let pool = common::get_postgres_connection_pool(port, None).await?;
    let conn = pool
        .connect_direct()
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    conn.conn
        .simple_query(
            "CREATE TABLE orders (id INT PRIMARY KEY, customer TEXT NOT NULL); \
             INSERT INTO orders (id, customer) VALUES (1, 'alice'), (2, 'bob');",
        )
        .await?;

    Ok(())
}

/// Build a `PostgreSQL` catalog with catalog-level CDC acceleration enabled.
fn accelerated_pg_catalog(port: usize) -> Catalog {
    let mut catalog = Catalog::new("pg:postgres".to_string(), CATALOG_NAME.to_string());
    catalog.params = Some(Params::from_string_map(
        get_pg_params(port)
            .into_iter()
            .map(|(k, v)| (k, v.expose_secret().to_string()))
            .collect::<HashMap<String, String>>(),
    ));
    catalog.acceleration = Some(CatalogAcceleration {
        engine: CatalogAccelerationEngine::Cayenne,
        refresh_mode: CatalogRefreshMode::Changes,
    });
    catalog
}

async fn start_runtime(catalog: Catalog) -> Result<Arc<Runtime>, anyhow::Error> {
    register_test_connectors().await;
    let app = AppBuilder::new("postgres_catalog_changes_test")
        .with_catalog(catalog)
        .build();

    let rt = Arc::new(Runtime::builder().with_app(app).build().await);

    tokio::select! {
        () = tokio::time::sleep(Duration::from_mins(2)) => {
            return Err(anyhow::anyhow!("Timed out waiting for catalog to load"));
        }
        () = Arc::clone(&rt).load_components() => {}
    }

    runtime_ready_check(&rt).await;
    Ok(rt)
}

/// A table with a primary key, discovered by a catalog with `acceleration:
/// { refresh_mode: changes }`, becomes queryable through the catalog's own
/// namespace once its synthesized dataset finishes bootstrapping -- with
/// zero per-table configuration.
#[tokio::test(flavor = "multi_thread")]
async fn test_catalog_acceleration_bootstraps_table_with_primary_key() -> Result<(), anyhow::Error>
{
    let _tracing = init_tracing(Some("integration=debug,info,runtime::catalogconnector=debug"));

    test_request_context()
        .scope(async {
            let port = common::get_random_port()?;
            let _container = common::start_postgres_docker_container_with_logical_wal(port).await?;

            seed_orders_table(port).await?;

            let rt = start_runtime(accelerated_pg_catalog(port)).await?;

            // The synthesized per-table dataset bootstraps in the background
            // (fire-and-forget, same as any spicepod-declared dataset) --
            // poll until it's ready rather than assuming it's ready the
            // instant catalog registration returns.
            let ready = wait_until_true(Duration::from_mins(2), || {
                let rt = Arc::clone(&rt);
                async move {
                    run_query(
                        &rt,
                        &format!("SELECT COUNT(*) AS n FROM {CATALOG_NAME}.public.orders"),
                    )
                    .await
                    .is_ok_and(|batches| {
                        batches
                            .first()
                            .is_some_and(|b| b.num_rows() > 0)
                    })
                }
            })
            .await;
            anyhow::ensure!(
                ready,
                "accelerated table {CATALOG_NAME}.public.orders never became queryable"
            );

            let count = run_query(
                &rt,
                &format!("SELECT COUNT(*) AS n FROM {CATALOG_NAME}.public.orders"),
            )
            .await?;
            assert_batches_eq!(
                &[
                    "+---+", //
                    "| n |", //
                    "+---+", //
                    "| 2 |", //
                    "+---+", //
                ],
                &count
            );

            Ok(())
        })
        .await
}
