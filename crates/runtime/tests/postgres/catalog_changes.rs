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
//! naming the table if one is missing. Every synthesized dataset shares one
//! replication slot, so a multi-table catalog opens exactly one replication
//! connection rather than one per table. A table matched by `exclude` is
//! never synthesized at all -- absent from the catalog's namespace, not
//! merely unaccelerated. `check_cdc_prerequisites` fails catalog setup fast,
//! naming the problem, before any table is touched, when the source can't
//! do CDC at all (e.g. `wal_level` isn't `logical`).
//!
//! After bootstrap, source inserts/updates/deletes propagate through the
//! shared replication slot and are reflected in the catalog's queryable
//! tables -- this isn't just a one-time snapshot.

use std::{collections::HashMap, sync::Arc, time::Duration};

use app::AppBuilder;
use data_components::postgres::provider::check_cdc_prerequisites;
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
    utils::{
        register_test_connectors, run_query, runtime_ready_check, test_request_context,
        wait_until_true,
    },
};

const CATALOG_NAME: &str = "pg_accel_e2e";

/// Seed two tables with primary keys and a couple of rows each, so the
/// shared-replication-slot behavior has more than one table to share across.
async fn seed_tables(port: usize) -> Result<(), anyhow::Error> {
    let pool = common::get_postgres_connection_pool(port, None).await?;
    let conn = pool
        .connect_direct()
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    conn.conn
        .simple_query(
            "CREATE TABLE orders (id INT PRIMARY KEY, customer TEXT NOT NULL); \
             INSERT INTO orders (id, customer) VALUES (1, 'alice'), (2, 'bob'); \
             CREATE TABLE items (id INT PRIMARY KEY, name TEXT NOT NULL); \
             INSERT INTO items (id, name) VALUES (1, 'widget'), (2, 'gadget'), (3, 'gizmo');",
        )
        .await?;

    Ok(())
}

/// Number of rows in `pg_replication_slots` on the source database --
/// asserts how many replication connections a multi-table catalog opened.
async fn replication_slot_count(port: usize) -> Result<i64, anyhow::Error> {
    let pool = common::get_postgres_connection_pool(port, None).await?;
    let conn = pool
        .connect_direct()
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    let count: i64 = conn
        .conn
        .query_one("SELECT COUNT(*) FROM pg_replication_slots", &[])
        .await?
        .get(0);
    Ok(count)
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

/// Same as [`accelerated_pg_catalog`], but excluding `items` -- validates
/// what the catalog's startup summary counts as "excluded by include/exclude
/// filters": the table is never synthesized into a dataset at all, so it's
/// simply absent from the catalog's namespace (not merely unaccelerated).
fn accelerated_pg_catalog_excluding_items(port: usize) -> Catalog {
    let mut catalog = accelerated_pg_catalog(port);
    catalog.exclude = vec!["public.items".to_string()];
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

/// Poll until `SELECT COUNT(*) FROM {CATALOG_NAME}.public.{table}` returns
/// at least one row -- the synthesized dataset bootstraps in the background
/// (fire-and-forget, same as any spicepod-declared dataset), so this can't
/// be assumed ready the instant catalog registration returns.
async fn wait_for_table_ready(rt: &Arc<Runtime>, table: &str) -> Result<(), anyhow::Error> {
    let ready = wait_until_true(Duration::from_mins(2), || {
        let rt = Arc::clone(rt);
        async move {
            run_query(
                &rt,
                &format!("SELECT COUNT(*) AS n FROM {CATALOG_NAME}.public.{table}"),
            )
            .await
            .is_ok_and(|batches| batches.first().is_some_and(|b| b.num_rows() > 0))
        }
    })
    .await;
    anyhow::ensure!(
        ready,
        "accelerated table {CATALOG_NAME}.public.{table} never became queryable"
    );
    Ok(())
}

/// Run `sql` (expected to select a single `BIGINT`/`COUNT(*)`-shaped column)
/// and return the scalar value, or `None` if the query itself failed.
async fn query_i64(rt: &Arc<Runtime>, sql: &str) -> Option<i64> {
    let batches = run_query(rt, sql).await.ok()?;
    let batch = batches.first()?;
    batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .map(|arr| arr.value(0))
}

/// Run `sql` (expected to select a single `TEXT`-shaped column) and return
/// the scalar value, or `None` if the query itself failed or returned no
/// rows.
async fn query_string(rt: &Arc<Runtime>, sql: &str) -> Option<String> {
    let batches = run_query(rt, sql).await.ok()?;
    let batch = batches.first()?;
    if batch.num_rows() == 0 {
        return None;
    }
    batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .map(|arr| arr.value(0).to_string())
}

/// Every table with a primary key, discovered by a catalog with
/// `acceleration: { refresh_mode: changes }`, becomes queryable through the
/// catalog's own namespace once its synthesized dataset finishes
/// bootstrapping -- with zero per-table configuration. Every synthesized
/// dataset shares one replication slot, so a two-table catalog opens
/// exactly one replication connection rather than two.
#[tokio::test(flavor = "multi_thread")]
async fn test_catalog_acceleration_bootstraps_tables_with_primary_key() -> Result<(), anyhow::Error>
{
    let _tracing = init_tracing(Some(
        "integration=debug,info,runtime::catalogconnector=debug",
    ));

    test_request_context()
        .scope(async {
            let port = common::get_random_port()?;
            let _container = common::start_postgres_docker_container_with_logical_wal(port).await?;

            seed_tables(port).await?;

            let rt = start_runtime(accelerated_pg_catalog(port)).await?;

            wait_for_table_ready(&rt, "orders").await?;
            wait_for_table_ready(&rt, "items").await?;

            let orders_count = run_query(
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
                &orders_count
            );

            let items_count = run_query(
                &rt,
                &format!("SELECT COUNT(*) AS n FROM {CATALOG_NAME}.public.items"),
            )
            .await?;
            assert_batches_eq!(
                &[
                    "+---+", //
                    "| n |", //
                    "+---+", //
                    "| 3 |", //
                    "+---+", //
                ],
                &items_count
            );

            let slot_count = replication_slot_count(port).await?;
            assert_eq!(
                slot_count, 1,
                "both tables should share one replication slot, found {slot_count}"
            );

            Ok(())
        })
        .await
}

/// A table excluded by the catalog's `exclude` patterns is never
/// synthesized into a dataset -- it's simply absent from the catalog's
/// namespace, not merely left unaccelerated. This is what the startup
/// summary's "excluded by include/exclude filters" count reflects.
#[tokio::test(flavor = "multi_thread")]
async fn test_catalog_acceleration_respects_exclude_filter() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some(
        "integration=debug,info,runtime::catalogconnector=debug",
    ));

    test_request_context()
        .scope(async {
            let port = common::get_random_port()?;
            let _container = common::start_postgres_docker_container_with_logical_wal(port).await?;

            seed_tables(port).await?;

            let rt = start_runtime(accelerated_pg_catalog_excluding_items(port)).await?;

            wait_for_table_ready(&rt, "orders").await?;

            let items_result = run_query(
                &rt,
                &format!("SELECT COUNT(*) AS n FROM {CATALOG_NAME}.public.items"),
            )
            .await;
            anyhow::ensure!(
                items_result.is_err(),
                "excluded table {CATALOG_NAME}.public.items should not be queryable, \
                but the query succeeded"
            );

            Ok(())
        })
        .await
}

/// `check_cdc_prerequisites` fails fast, naming the specific problem, when
/// `wal_level` isn't `logical` -- catalog acceleration should never get as
/// far as discovering tables against a source that can't do CDC at all.
#[tokio::test(flavor = "multi_thread")]
async fn test_check_cdc_prerequisites_rejects_non_logical_wal_level() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let port = common::get_random_port()?;
            // Deliberately the plain container (default wal_level, not
            // `logical`) rather than `..._with_logical_wal`.
            let _container = common::start_postgres_docker_container(port).await?;

            let pool = common::get_postgres_connection_pool(port, None).await?;

            let result = check_cdc_prerequisites(&pool).await;
            let err = result.expect_err("expected wal_level check to fail");
            let message = err.to_string();
            anyhow::ensure!(
                message.contains("wal_level"),
                "expected error naming wal_level, got: {message}"
            );

            Ok(())
        })
        .await
}

/// After bootstrap, an insert, an update, and a delete on the source all
/// propagate through the catalog's single shared replication slot and are
/// reflected in the catalog's queryable tables -- proving this is a live
/// CDC stream, not a one-time snapshot.
#[tokio::test(flavor = "multi_thread")]
async fn test_catalog_acceleration_converges_after_source_mutation() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some(
        "integration=debug,info,runtime::catalogconnector=debug",
    ));

    test_request_context()
        .scope(async {
            let port = common::get_random_port()?;
            let _container = common::start_postgres_docker_container_with_logical_wal(port).await?;

            seed_tables(port).await?;

            let rt = start_runtime(accelerated_pg_catalog(port)).await?;

            wait_for_table_ready(&rt, "orders").await?;
            wait_for_table_ready(&rt, "items").await?;

            // Insert a new order, update an existing order's customer, and
            // delete an item -- one of each change type CDC must apply.
            let pool = common::get_postgres_connection_pool(port, None).await?;
            let conn = pool
                .connect_direct()
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            conn.conn
                .simple_query(
                    "INSERT INTO orders (id, customer) VALUES (3, 'carol'); \
                     UPDATE orders SET customer = 'alice2' WHERE id = 1; \
                     DELETE FROM items WHERE id = 2;",
                )
                .await?;

            let orders_count_sql =
                format!("SELECT COUNT(*) AS n FROM {CATALOG_NAME}.public.orders");
            let updated_customer_sql =
                format!("SELECT customer FROM {CATALOG_NAME}.public.orders WHERE id = 1");
            let items_count_sql = format!("SELECT COUNT(*) AS n FROM {CATALOG_NAME}.public.items");

            let converged = wait_until_true(Duration::from_mins(2), || {
                let rt = Arc::clone(&rt);
                let orders_count_sql = orders_count_sql.clone();
                let updated_customer_sql = updated_customer_sql.clone();
                let items_count_sql = items_count_sql.clone();
                async move {
                    query_i64(&rt, &orders_count_sql).await == Some(3)
                        && query_string(&rt, &updated_customer_sql).await.as_deref()
                            == Some("alice2")
                        && query_i64(&rt, &items_count_sql).await == Some(2)
                }
            })
            .await;
            anyhow::ensure!(
                converged,
                "source insert/update/delete never converged through CDC: \
                orders_count={:?}, updated_customer={:?}, items_count={:?}",
                query_i64(&rt, &orders_count_sql).await,
                query_string(&rt, &updated_customer_sql).await,
                query_i64(&rt, &items_count_sql).await,
            );

            Ok(())
        })
        .await
}
