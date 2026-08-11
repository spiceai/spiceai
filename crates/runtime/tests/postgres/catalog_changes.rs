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
//! CDC-accelerate every discovered table that has a usable `REPLICA IDENTITY`:
//! the table becomes queryable through the catalog's own namespace
//! (`{catalog}.public.<table>`), backed by a synthesized dataset driven
//! through the exact same lifecycle as any spicepod-declared dataset.
//!
//! A table with no usable CDC key -- `REPLICA IDENTITY NOTHING`, keyless
//! `DEFAULT`, etc. -- is skipped with a warning and simply absent from the
//! catalog's namespace, rather than failing the whole catalog; the remaining
//! tables still replicate (`test_catalog_acceleration_replica_identity_matrix`).
//! `USING INDEX` (keyed by the nominated unique index, no formal primary key)
//! and `FULL` are both supported. Every synthesized dataset shares one
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
use data_components::postgres::provider::{
    check_cdc_prerequisites, ensure_replication_slot_capacity, replication_slot_status,
};
use data_components::postgres_replication::config::catalog_slot_name;
use datafusion::assert_batches_eq;
use datafusion_table_providers::sql::db_connection_pool::postgrespool::PostgresConnectionPool;
use runtime::Runtime;
use runtime::status::ComponentStatus;
use secrecy::{ExposeSecret, SecretString};
use spicepod::{
    acceleration::Mode as AccelerationMode,
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

/// Seed one CDC-eligible table (`orders`, primary key) alongside view-like
/// relations that cannot be CDC-accelerated: a regular view and a materialized
/// view over it. The catalog should accelerate `orders`, warn that the views
/// aren't replicated, and leave the views absent from its namespace -- without
/// failing (a view is not a REPLICA-IDENTITY error, #11911).
async fn seed_table_and_views(port: usize) -> Result<(), anyhow::Error> {
    let pool = common::get_postgres_connection_pool(port, None).await?;
    let conn = pool
        .connect_direct()
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    conn.conn
        .simple_query(
            "CREATE TABLE orders (id INT PRIMARY KEY, customer TEXT NOT NULL); \
             INSERT INTO orders (id, customer) VALUES (1, 'alice'), (2, 'bob'); \
             CREATE VIEW orders_view AS SELECT id, customer FROM orders; \
             CREATE MATERIALIZED VIEW orders_matview AS SELECT id, customer FROM orders;",
        )
        .await?;

    Ok(())
}

/// Seed one table per `REPLICA IDENTITY` mode so the catalog's per-table
/// eligibility can be observed end-to-end: the three keyed tables
/// (`ri_default`, `ri_using_index`, `ri_full`) must become queryable, while the
/// two keyless tables (`ri_nothing`, `ri_keyless`) must be skipped and absent.
/// Each table is seeded with two rows.
async fn seed_replica_identity_tables(port: usize) -> Result<(), anyhow::Error> {
    let pool = common::get_postgres_connection_pool(port, None).await?;
    let conn = pool
        .connect_direct()
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    // NOTE: this string is assembled with Rust line-continuations (`\`), which
    // strip the newlines -- so it must contain NO `--` SQL comments (a `--`
    // would comment out the entire remainder of the single joined line). Per
    // table: ri_default = DEFAULT + primary key (eligible); ri_using_index = no
    // primary key, keyed by a UNIQUE NOT NULL index via REPLICA IDENTITY USING
    // INDEX (eligible); ri_full = FULL + primary key (eligible, heavier);
    // ri_nothing = REPLICA IDENTITY NOTHING (skipped); ri_keyless = no primary
    // key, DEFAULT (skipped).
    conn.conn
        .simple_query(
            "CREATE TABLE ri_default (id INT PRIMARY KEY, name TEXT NOT NULL); \
             INSERT INTO ri_default VALUES (1, 'a'), (2, 'b'); \
             CREATE TABLE ri_using_index (uid INT NOT NULL, name TEXT NOT NULL); \
             CREATE UNIQUE INDEX ri_using_index_uid_key ON ri_using_index (uid); \
             ALTER TABLE ri_using_index REPLICA IDENTITY USING INDEX ri_using_index_uid_key; \
             INSERT INTO ri_using_index VALUES (10, 'x'), (20, 'y'); \
             CREATE TABLE ri_full (id INT PRIMARY KEY, name TEXT NOT NULL); \
             ALTER TABLE ri_full REPLICA IDENTITY FULL; \
             INSERT INTO ri_full VALUES (1, 'a'), (2, 'b'); \
             CREATE TABLE ri_nothing (id INT PRIMARY KEY, name TEXT NOT NULL); \
             ALTER TABLE ri_nothing REPLICA IDENTITY NOTHING; \
             INSERT INTO ri_nothing VALUES (1, 'a'), (2, 'b'); \
             CREATE TABLE ri_keyless (name TEXT NOT NULL); \
             INSERT INTO ri_keyless VALUES ('a'), ('b');",
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

/// All replication slot names on the source database, sorted -- used to assert a
/// restart REUSES the catalog's deterministic slot (the set stays `[expected]`)
/// rather than orphaning it and creating a second one.
async fn replication_slot_names(port: usize) -> Result<Vec<String>, anyhow::Error> {
    let pool = common::get_postgres_connection_pool(port, None).await?;
    let conn = pool
        .connect_direct()
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    let rows = conn
        .conn
        .query(
            "SELECT slot_name FROM pg_replication_slots ORDER BY slot_name",
            &[],
        )
        .await?;
    Ok(rows.iter().map(|row| row.get(0)).collect())
}

/// Whether the named replication slot exists and is currently `active` (held by
/// a live consumer). `None` when the slot does not exist.
async fn slot_active(port: usize, slot_name: &str) -> Result<Option<bool>, anyhow::Error> {
    let pool = common::get_postgres_connection_pool(port, None).await?;
    let status = replication_slot_status(&pool, slot_name)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(status.map(|s| s.active))
}

/// Lower the source's `wal_sender_timeout` so the catalog's bounded
/// slot-in-use wait (sized from it) stays short in the fail-loud test. Applied
/// via `ALTER SYSTEM` + reload, so it takes effect for walsenders started after.
///
/// `ALTER SYSTEM` cannot run inside a transaction block, and the simple-query
/// protocol wraps a multi-statement string in one implicit transaction -- so the
/// two statements are issued as separate queries rather than a single
/// `";"`-joined one.
async fn set_wal_sender_timeout(port: usize, value: &str) -> Result<(), anyhow::Error> {
    let pool = common::get_postgres_connection_pool(port, None).await?;
    let conn = pool
        .connect_direct()
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    conn.conn
        .simple_query(&format!("ALTER SYSTEM SET wal_sender_timeout = '{value}'"))
        .await?;
    conn.conn.simple_query("SELECT pg_reload_conf()").await?;
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
        mode: AccelerationMode::default(),
        params: None,
    });
    catalog
}

/// A catalog whose acceleration is durable (`mode: file`), so its replication
/// slot genuinely has resume value across a restart and is kept at shutdown.
/// The default `mode: memory` catalog re-snapshots on every start, so its slot
/// is released instead -- see
/// `test_catalog_acceleration_releases_the_slot_when_the_acceleration_is_not_durable`.
///
/// Restricted to one table: on a RESUMING shared slot, the member that joins
/// second can be registered above changes it has not consumed and skip them
/// (#12609). With a single member the resume position is the slot's own
/// `confirmed_flush_lsn`, so what this fixture backs is deterministic.
fn durable_accelerated_pg_catalog(port: usize, data_dir: &std::path::Path) -> Catalog {
    let mut catalog = accelerated_pg_catalog(port);
    catalog.include = vec!["public.orders".to_string()];
    catalog.acceleration = Some(CatalogAcceleration {
        engine: CatalogAccelerationEngine::Cayenne,
        refresh_mode: CatalogRefreshMode::Changes,
        mode: AccelerationMode::File,
        params: Some(Params::from_string_map(HashMap::from([(
            "cayenne_file_path".to_string(),
            data_dir.to_string_lossy().to_string(),
        )]))),
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

/// Same as [`accelerated_pg_catalog`], but including only `orders` -- validates
/// the `include` filter's positive form: a table that doesn't match `include`
/// is never synthesized (absent from the catalog's namespace), the mirror of
/// [`accelerated_pg_catalog_excluding_items`].
fn accelerated_pg_catalog_including_only_orders(port: usize) -> Catalog {
    let mut catalog = accelerated_pg_catalog(port);
    catalog.include = vec!["public.orders".to_string()];
    catalog
}

/// Seed only tables that CANNOT be CDC-accelerated: `REPLICA IDENTITY NOTHING`
/// and a keyless `DEFAULT` table. A catalog pointed here discovers tables but
/// finds none eligible -- the fail-loud path
/// (`test_catalog_acceleration_fails_loudly_when_no_tables_eligible`).
async fn seed_only_ineligible_tables(port: usize) -> Result<(), anyhow::Error> {
    let pool = common::get_postgres_connection_pool(port, None).await?;
    let conn = pool
        .connect_direct()
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    conn.conn
        .simple_query(
            "CREATE TABLE ri_nothing (id INT PRIMARY KEY, name TEXT NOT NULL); \
             ALTER TABLE ri_nothing REPLICA IDENTITY NOTHING; \
             INSERT INTO ri_nothing VALUES (1, 'a'), (2, 'b'); \
             CREATE TABLE ri_keyless (name TEXT NOT NULL); \
             INSERT INTO ri_keyless VALUES ('a'), ('b');",
        )
        .await?;

    Ok(())
}

/// Create a `LOGIN` role WITHOUT the `REPLICATION` privilege and return a
/// connection pool authenticating as it, for exercising the
/// replication-privilege prerequisite branch of `check_cdc_prerequisites`.
async fn pool_for_non_replication_role(
    port: usize,
) -> Result<PostgresConnectionPool, anyhow::Error> {
    let admin = common::get_postgres_connection_pool(port, None).await?;
    let conn = admin
        .connect_direct()
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    // New roles default to NOREPLICATION NOSUPERUSER; make that explicit.
    conn.conn
        .simple_query("CREATE ROLE norepl LOGIN PASSWORD 'norepl_pw' NOSUPERUSER NOREPLICATION;")
        .await?;

    let mut params: HashMap<String, SecretString> = get_pg_params(port);
    params.insert("pg_user".to_string(), SecretString::from("norepl"));
    params.insert("pg_pass".to_string(), SecretString::from("norepl_pw"));

    let pool = PostgresConnectionPool::new(params)
        .await
        .map_err(|e| anyhow::anyhow!("failed to build non-replication-role pool: {e}"))?;
    Ok(pool)
}

/// Build a runtime with `catalog` and run component loading to completion, but
/// WITHOUT asserting the runtime became ready. Used by the fail-loud test: a
/// catalog that correctly fails to load leaves the runtime not-ready by design,
/// so [`runtime_ready_check`] (which [`start_runtime`] runs) would panic before
/// the test could observe the catalog's Error status.
async fn build_and_load_runtime(catalog: Catalog) -> Result<Arc<Runtime>, anyhow::Error> {
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

    Ok(rt)
}

async fn start_runtime(catalog: Catalog) -> Result<Arc<Runtime>, anyhow::Error> {
    let rt = build_and_load_runtime(catalog).await?;
    runtime_ready_check(&rt).await;
    Ok(rt)
}

/// How long a synthesized dataset gets to bootstrap -- or, after a restart, to
/// come back -- before [`wait_for_table_ready`] gives up.
const TABLE_READY_TIMEOUT: Duration = Duration::from_mins(2);

/// Interval between readiness polls. Every poll is a full `COUNT(*)` through the
/// runtime and emits several `task_history` log lines, so a wait that runs to
/// its timeout writes thousands of lines that bury the failure they surround
/// (#12729). Half a second still resolves a two-minute budget finely, and is
/// well below the granularity any assertion here depends on.
const TABLE_READY_POLL_INTERVAL: Duration = Duration::from_millis(500);

/// Poll until `SELECT COUNT(*) FROM {CATALOG_NAME}.public.{table}` returns a
/// non-zero count -- the synthesized dataset bootstraps in the background
/// (fire-and-forget, same as any spicepod-declared dataset), so this can't
/// be assumed ready the instant catalog registration returns.
///
/// The readiness signal must be the count *value*, not the row count: a
/// `COUNT(*)` query returns exactly one row as soon as it succeeds -- which
/// happens the moment the table is registered, before the background refresh
/// has loaded any data -- so `num_rows() > 0` is always true and would let the
/// exact-count assertions downstream race the bootstrap. Every table this waits
/// on is seeded non-empty, so `n > 0` is the correct "data present" condition.
///
/// `phase` names the call site, because several tests wait on the same table
/// more than once -- `test_catalog_acceleration_reuses_slot_across_restart`
/// waits either side of a restart -- and one shared message does not say which
/// of them ran out of time.
///
/// The timeout also reports the last thing it observed. A poll whose query
/// *errored* (the table is not registered at all) and one that *returned zero*
/// (registered, but no rows arrived) have completely different causes, and
/// collapsing both into "never became queryable" leaves the reader unable to
/// tell them apart without the full job log.
async fn wait_for_table_ready(
    rt: &Arc<Runtime>,
    table: &str,
    phase: &str,
) -> Result<(), anyhow::Error> {
    let sql = format!("SELECT COUNT(*) AS n FROM {CATALOG_NAME}.public.{table}");
    let started = std::time::Instant::now();
    let mut last: Option<Result<i64, String>> = None;

    // Every wait inside the loop is bounded by what is left of the budget, so
    // the deadline holds even when a poll never returns. A bare
    // `query_count(..).await` would be unbounded: the loop condition is only
    // evaluated between polls, so one stalled query -- the failure this
    // diagnostic exists to describe -- would hang the test past its two minutes
    // and past the point of emitting anything at all.
    while let Some(remaining) = TABLE_READY_TIMEOUT.checked_sub(started.elapsed()) {
        match tokio::time::timeout(remaining, query_count(rt, &sql)).await {
            Ok(Ok(n)) if n > 0 => return Ok(()),
            Ok(outcome) => last = Some(outcome),
            // The budget is spent, so there is nothing left to poll with. Record
            // the stall as the observation -- it is a distinct cause from a poll
            // that ran and reported, and the one a bare await would have lost.
            Err(_) => {
                last = Some(Err(format!("poll did not return within {remaining:?}")));
                break;
            }
        }
        // Capped for the same reason: a sleep that outlives the budget would
        // push the report past the deadline it is reporting on.
        let left = TABLE_READY_TIMEOUT.saturating_sub(started.elapsed());
        if left.is_zero() {
            break;
        }
        tokio::time::sleep(TABLE_READY_POLL_INTERVAL.min(left)).await;
    }

    let observed = match last {
        Some(Ok(n)) => format!("last poll returned {n} rows"),
        Some(Err(error)) => format!("last poll could not run: {error}"),
        // Defensive: the loop records an outcome on every path out, including a
        // poll that timed out, so this stands only if the budget was already
        // spent before the first poll began.
        None => "no poll completed".to_string(),
    };
    anyhow::bail!(
        "accelerated table {CATALOG_NAME}.public.{table} never became queryable during \
         '{phase}' after {elapsed:?}: {observed}",
        elapsed = started.elapsed()
    )
}

/// Run `sql` (expected to select a single `BIGINT`/`COUNT(*)`-shaped column)
/// and return the scalar value, keeping the reason on failure so a caller that
/// polls can report *why* it never saw a value rather than only that it didn't.
async fn query_count(rt: &Arc<Runtime>, sql: &str) -> Result<i64, String> {
    let batches = run_query(rt, sql).await.map_err(|e| e.to_string())?;
    let batch = batches
        .first()
        .ok_or_else(|| "query returned no record batches".to_string())?;
    if batch.num_rows() == 0 {
        return Err("query returned an empty record batch".to_string());
    }
    let column = batch
        .columns()
        .first()
        .ok_or_else(|| "query returned no columns".to_string())?;
    column
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .map(|values| values.value(0))
        .ok_or_else(|| format!("count column is {:?}, expected Int64", column.data_type()))
}

/// Run `sql` (expected to select a single `BIGINT`/`COUNT(*)`-shaped column)
/// and return the scalar value, or `None` if the query itself failed.
async fn query_i64(rt: &Arc<Runtime>, sql: &str) -> Option<i64> {
    query_count(rt, sql).await.ok()
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

            wait_for_table_ready(&rt, "orders", "bootstrap").await?;
            wait_for_table_ready(&rt, "items", "bootstrap").await?;

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

            // The per-table datasets the catalog synthesizes are an internal
            // registration detail: a user reaches each table through the
            // catalog's namespace, so listing the synthesized names too would
            // show every accelerated table a second time under a name that
            // isn't part of the catalog's interface.
            let listed = run_query(
                &rt,
                "SELECT table_name FROM information_schema.tables \
                 WHERE table_catalog = 'spice' AND table_schema = 'data'",
            )
            .await?;
            let listed_names: Vec<String> = listed
                .iter()
                .flat_map(|batch| {
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<arrow::array::StringArray>()
                        .map_or_else(Vec::new, |arr| {
                            arr.iter().flatten().map(ToString::to_string).collect()
                        })
                })
                .collect();
            assert!(
                !listed_names
                    .iter()
                    .any(|name| name.starts_with("__catalog_accel_")),
                "synthesized catalog datasets must not be listed in spice.data, found: {listed_names:?}"
            );

            Ok(())
        })
        .await
}

/// A view-like relation (view / materialized view) is not CDC-accelerable, so
/// it must be handled gracefully -- the accelerated catalog emits a "not
/// replicated" warning (asserted at the unit level via `AccelerationSummary`;
/// not asserted here because the runtime test harness drops worker-thread
/// tracing) rather than a fatal REPLICA-IDENTITY error. This test validates the
/// observable outcome: the catalog still loads (its one eligible table
/// accelerates and becomes Ready) and the views are simply absent from the
/// catalog's namespace (#11911).
#[tokio::test(flavor = "multi_thread")]
async fn test_catalog_acceleration_excludes_views_and_still_loads() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some(
        "integration=debug,info,runtime::catalogconnector=debug",
    ));

    test_request_context()
        .scope(async {
            let port = common::get_random_port()?;
            let _container = common::start_postgres_docker_container_with_logical_wal(port).await?;

            seed_table_and_views(port).await?;

            // The catalog loads successfully despite the views -- a view is not a
            // fatal REPLICA-IDENTITY error. `start_runtime` asserts readiness.
            let rt = start_runtime(accelerated_pg_catalog(port)).await?;

            // The eligible base table accelerates and becomes queryable.
            wait_for_table_ready(&rt, "orders", "bootstrap").await?;

            // The view and materialized view are absent from the catalog's
            // namespace (not replicated), so querying them fails.
            let view_query = run_query(
                &rt,
                &format!("SELECT COUNT(*) FROM {CATALOG_NAME}.public.orders_view"),
            )
            .await;
            anyhow::ensure!(
                view_query.is_err(),
                "a view must not be queryable through the accelerated catalog"
            );

            let matview_query = run_query(
                &rt,
                &format!("SELECT COUNT(*) FROM {CATALOG_NAME}.public.orders_matview"),
            )
            .await;
            anyhow::ensure!(
                matview_query.is_err(),
                "a materialized view must not be queryable through the accelerated catalog"
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

            wait_for_table_ready(&rt, "orders", "bootstrap").await?;

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

/// The slot-capacity pre-flight passes on a server with room, then rejects one
/// whose `max_replication_slots` is exhausted with an actionable error -- so an
/// operator hits a clear message at startup instead of a cryptic slot-creation
/// failure later, deep in replication setup.
#[tokio::test(flavor = "multi_thread")]
async fn test_replication_slot_capacity_rejects_exhausted_server() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let port = common::get_random_port()?;
            let _container = common::start_postgres_docker_container_with_logical_wal(port).await?;

            let pool = common::get_postgres_connection_pool(port, None).await?;

            // A fresh server has capacity.
            ensure_replication_slot_capacity(&pool)
                .await
                .map_err(|e| anyhow::anyhow!("fresh server should have slot capacity: {e}"))?;

            // Fill every remaining replication slot.
            let conn = pool
                .connect_direct()
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            let max: i64 = conn
                .conn
                .query_one(
                    "SELECT current_setting('max_replication_slots')::bigint",
                    &[],
                )
                .await?
                .get(0);
            let used: i64 = conn
                .conn
                .query_one("SELECT count(*)::bigint FROM pg_replication_slots", &[])
                .await?
                .get(0);
            for i in used..max {
                conn.conn
                    .query(
                        "SELECT pg_create_logical_replication_slot($1, 'pgoutput')",
                        &[&format!("cap_test_{i}")],
                    )
                    .await?;
            }

            // Now exhausted -> the pre-flight must reject with an actionable error.
            let err = ensure_replication_slot_capacity(&pool)
                .await
                .expect_err("exhausted server should be rejected");
            let message = err.to_string();
            anyhow::ensure!(
                message.contains("max_replication_slots")
                    && message.contains("pg_drop_replication_slot"),
                "expected an actionable slot-capacity error, got: {message}"
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

            wait_for_table_ready(&rt, "orders", "bootstrap").await?;
            wait_for_table_ready(&rt, "items", "bootstrap").await?;

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

/// A catalog pointed at a database with a mix of `REPLICA IDENTITY` modes
/// replicates every table that has a usable CDC key and skips the rest --
/// without failing the whole catalog. `DEFAULT` + primary key, `USING INDEX`
/// (no formal primary key), and `FULL` + primary key all become queryable;
/// `NOTHING` and keyless `DEFAULT` are absent from the catalog namespace. The
/// three eligible tables share the catalog's single replication slot; the
/// skipped ones open none.
#[tokio::test(flavor = "multi_thread")]
async fn test_catalog_acceleration_replica_identity_matrix() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some(
        "integration=debug,info,runtime::catalogconnector=debug",
    ));

    test_request_context()
        .scope(async {
            let port = common::get_random_port()?;
            let _container = common::start_postgres_docker_container_with_logical_wal(port).await?;

            seed_replica_identity_tables(port).await?;

            let rt = start_runtime(accelerated_pg_catalog(port)).await?;

            // Eligible tables (each seeded with 2 rows) become queryable.
            for table in ["ri_default", "ri_using_index", "ri_full"] {
                wait_for_table_ready(&rt, table, "bootstrap").await?;
                let count = query_i64(
                    &rt,
                    &format!("SELECT COUNT(*) AS n FROM {CATALOG_NAME}.public.{table}"),
                )
                .await;
                anyhow::ensure!(
                    count == Some(2),
                    "eligible table {table} should have 2 rows, got {count:?}"
                );
            }

            // Skipped tables are absent from the catalog namespace entirely.
            for table in ["ri_nothing", "ri_keyless"] {
                let result = run_query(
                    &rt,
                    &format!("SELECT COUNT(*) AS n FROM {CATALOG_NAME}.public.{table}"),
                )
                .await;
                anyhow::ensure!(
                    result.is_err(),
                    "skipped table {CATALOG_NAME}.public.{table} should be absent, \
                    but the query succeeded"
                );
            }

            let slot_count = replication_slot_count(port).await?;
            anyhow::ensure!(
                slot_count == 1,
                "the eligible tables should share one replication slot, found {slot_count}"
            );

            Ok(())
        })
        .await
}

/// A `REPLICA IDENTITY USING INDEX` table (no formal primary key) applies live
/// insert/update/delete correctly, keyed by the nominated unique index -- the
/// load-bearing proof that the CDC apply path routes by the identity columns
/// the catalog declared, not by a formal primary key. The UPDATE must mutate
/// the existing row in place (not append a duplicate), so both the row count
/// and the updated value must converge.
#[tokio::test(flavor = "multi_thread")]
async fn test_catalog_acceleration_using_index_cdc_converges() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some(
        "integration=debug,info,runtime::catalogconnector=debug",
    ));

    test_request_context()
        .scope(async {
            let port = common::get_random_port()?;
            let _container = common::start_postgres_docker_container_with_logical_wal(port).await?;

            seed_replica_identity_tables(port).await?;

            let rt = start_runtime(accelerated_pg_catalog(port)).await?;

            wait_for_table_ready(&rt, "ri_using_index", "bootstrap").await?;

            // Mutate keyed by the identity column `uid`: insert uid 30, update
            // uid 10's non-key column, delete uid 20. Starting from 2 rows, the
            // net is still 2 (one added, one removed); the update must land in
            // place on uid 10, not append a second row.
            let pool = common::get_postgres_connection_pool(port, None).await?;
            let conn = pool
                .connect_direct()
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            conn.conn
                .simple_query(
                    "INSERT INTO ri_using_index VALUES (30, 'z'); \
                     UPDATE ri_using_index SET name = 'x2' WHERE uid = 10; \
                     DELETE FROM ri_using_index WHERE uid = 20;",
                )
                .await?;

            let count_sql =
                format!("SELECT COUNT(*) AS n FROM {CATALOG_NAME}.public.ri_using_index");
            let updated_sql =
                format!("SELECT name FROM {CATALOG_NAME}.public.ri_using_index WHERE uid = 10");

            let converged = wait_until_true(Duration::from_mins(2), || {
                let rt = Arc::clone(&rt);
                let count_sql = count_sql.clone();
                let updated_sql = updated_sql.clone();
                async move {
                    query_i64(&rt, &count_sql).await == Some(2)
                        && query_string(&rt, &updated_sql).await.as_deref() == Some("x2")
                }
            })
            .await;
            anyhow::ensure!(
                converged,
                "USING INDEX CDC never converged: count={:?} (expected 2), updated_name={:?} (expected \"x2\")",
                query_i64(&rt, &count_sql).await,
                query_string(&rt, &updated_sql).await,
            );

            Ok(())
        })
        .await
}

/// The `include` filter's positive form: a catalog that includes only
/// `public.orders` synthesizes that table and nothing else -- `items`, though
/// eligible, is never synthesized and is absent from the catalog namespace.
/// Mirror of `test_catalog_acceleration_respects_exclude_filter`.
#[tokio::test(flavor = "multi_thread")]
async fn test_catalog_acceleration_respects_include_filter() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some(
        "integration=debug,info,runtime::catalogconnector=debug",
    ));

    test_request_context()
        .scope(async {
            let port = common::get_random_port()?;
            let _container = common::start_postgres_docker_container_with_logical_wal(port).await?;

            seed_tables(port).await?;

            let rt = start_runtime(accelerated_pg_catalog_including_only_orders(port)).await?;

            wait_for_table_ready(&rt, "orders", "bootstrap").await?;

            let items_result = run_query(
                &rt,
                &format!("SELECT COUNT(*) AS n FROM {CATALOG_NAME}.public.items"),
            )
            .await;
            anyhow::ensure!(
                items_result.is_err(),
                "table {CATALOG_NAME}.public.items is not in the include set and should be \
                absent, but the query succeeded"
            );

            Ok(())
        })
        .await
}

/// A catalog whose every discovered table is ineligible (`REPLICA IDENTITY
/// NOTHING`, keyless `DEFAULT`) must fail loudly -- reaching an `Error` status
/// with an actionable message -- rather than registering an empty catalog.
/// Failing the initial refresh means the catalog never registers (and so never
/// gets a periodic refresh to reconsider), so zero eligible tables at load is a
/// permanent configuration error.
#[tokio::test(flavor = "multi_thread")]
async fn test_catalog_acceleration_fails_loudly_when_no_tables_eligible()
-> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some(
        "integration=debug,info,runtime::catalogconnector=debug",
    ));

    test_request_context()
        .scope(async {
            let port = common::get_random_port()?;
            let _container = common::start_postgres_docker_container_with_logical_wal(port).await?;

            seed_only_ineligible_tables(port).await?;

            // NOT `start_runtime`: the catalog is expected to fail to load, which
            // leaves the runtime not-ready by design, so a readiness assertion
            // would fire before we can observe the catalog's Error status.
            let rt = build_and_load_runtime(accelerated_pg_catalog(port)).await?;

            // The catalog must reach Error state -- fail loud, not register empty.
            let errored = wait_until_true(Duration::from_mins(1), || {
                let rt = Arc::clone(&rt);
                async move {
                    rt.status()
                        .get_catalog_statuses()
                        .get(CATALOG_NAME)
                        .is_some_and(|s| matches!(s, ComponentStatus::Error(_)))
                }
            })
            .await;
            anyhow::ensure!(
                errored,
                "catalog with no eligible tables should reach Error state, got {:?}",
                rt.status().get_catalog_statuses().get(CATALOG_NAME)
            );

            // And the error must carry an actionable message. An `Error(None)`
            // (message-less) or any non-`Error` status is a failure here, not a
            // case to skip -- the whole point is that the failure is loud AND
            // explains itself.
            match rt.status().get_catalog_statuses().get(CATALOG_NAME) {
                Some(ComponentStatus::Error(Some(message))) => {
                    // Actionable message: names the zero-eligible outcome and the
                    // REPLICA IDENTITY fix (the exact count is dynamic, so match on
                    // the stable phrasing rather than "0 of N").
                    anyhow::ensure!(
                        message.contains("discovered table(s) are eligible for CDC acceleration")
                            && message.contains("REPLICA IDENTITY"),
                        "error message should be actionable, got: {message}"
                    );
                }
                other => anyhow::bail!(
                    "catalog should be in an Error state with an actionable message, got {other:?}"
                ),
            }

            // The catalog's tables must not be queryable.
            let query_result = run_query(
                &rt,
                &format!("SELECT COUNT(*) AS n FROM {CATALOG_NAME}.public.ri_nothing"),
            )
            .await;
            anyhow::ensure!(
                query_result.is_err(),
                "an ineligible-only catalog should register no queryable tables"
            );

            Ok(())
        })
        .await
}

/// `check_cdc_prerequisites` fails, naming the replication-privilege problem,
/// when the connecting role lacks the `REPLICATION` attribute -- the second
/// prerequisite branch, alongside the `wal_level` check.
#[tokio::test(flavor = "multi_thread")]
async fn test_check_cdc_prerequisites_rejects_non_replication_role() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let port = common::get_random_port()?;
            // Needs wal_level=logical so the check gets PAST the wal_level gate
            // and reaches the replication-privilege check.
            let _container = common::start_postgres_docker_container_with_logical_wal(port).await?;

            let pool = pool_for_non_replication_role(port).await?;

            let result = check_cdc_prerequisites(&pool).await;
            let err = result.expect_err("expected replication-privilege check to fail");
            let message = err.to_string();
            anyhow::ensure!(
                message.contains("replication") && message.contains("norepl"),
                "expected error naming the replication privilege and role, got: {message}"
            );

            Ok(())
        })
        .await
}

/// The mirror of the durable restart case: a catalog left on the default
/// `mode: memory` re-runs its initial snapshot on every start, so its slot has
/// no resume value and is released at shutdown rather than left pinning WAL on
/// the source. Regression for the slot-lifecycle work -- before it, the slot
/// survived and retained WAL for the whole time Spice was down.
#[tokio::test(flavor = "multi_thread")]
async fn test_catalog_acceleration_releases_the_slot_when_the_acceleration_is_not_durable()
-> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some(
        "integration=debug,info,runtime::catalogconnector=debug",
    ));

    test_request_context()
        .scope(async {
            let port = common::get_random_port()?;
            let _container = common::start_postgres_docker_container_with_logical_wal(port).await?;
            seed_tables(port).await?;

            let expected_slot = catalog_slot_name(CATALOG_NAME);

            let rt = start_runtime(accelerated_pg_catalog(port)).await?;
            wait_for_table_ready(&rt, "orders", "bootstrap").await?;
            anyhow::ensure!(
                replication_slot_names(port).await? == vec![expected_slot.clone()],
                "the catalog should hold exactly one slot while running"
            );

            rt.shutdown().await;
            drop(rt);

            // Poll rather than assert once: the drop runs after the walsender
            // exits, and the server clears that asynchronously.
            let released = wait_until_true(Duration::from_secs(90), || {
                let slot = expected_slot.clone();
                async move {
                    replication_slot_names(port)
                        .await
                        .is_ok_and(|slots| !slots.contains(&slot))
                }
            })
            .await;
            anyhow::ensure!(
                released,
                "a non-durable catalog acceleration must release its slot at shutdown; still present: {:?}",
                replication_slot_names(port).await
            );

            Ok(())
        })
        .await
}

/// Restart/recovery for a DURABLE catalog acceleration (`mode: file`): the
/// slot's name is derived deterministically from the catalog and is INDEPENDENT
/// of the Spice instance, so a restart -- even one that would reschedule onto a
/// different host -- recomputes the same name and REUSES the existing slot
/// rather than orphaning it and creating a second. The slot persists across the
/// shutdown, so a change made to the source while Spice is down is reflected
/// after it comes back (#11850; feeds slot-lifecycle #12018).
///
/// Two constraints on how this test can be written, both about the replication
/// stream surviving to deliver that change:
///
/// * It needs process isolation, which `cargo nextest` (what CI runs) gives it.
///   `Runtime::shutdown` signals every CDC source in the *process*
///   (`data_components::cdc::begin_shutdown`), so under a shared-process runner
///   any other test shutting a runtime down stops this one's pump for good
///   (#12608).
/// * Its catalog covers a single table. A member joining a RESUMING shared slot
///   second can be registered above changes it has not consumed and skip them
///   (#12609) -- with two tables this assertion fails whenever the join order
///   puts `orders` second.
#[tokio::test(flavor = "multi_thread")]
async fn test_catalog_acceleration_reuses_slot_across_restart() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some(
        "integration=debug,info,runtime::catalogconnector=debug",
    ));

    test_request_context()
        .scope(async {
            let port = common::get_random_port()?;
            let _container = common::start_postgres_docker_container_with_logical_wal(port).await?;

            seed_tables(port).await?;

            let expected_slot = catalog_slot_name(CATALOG_NAME);
            // A durable acceleration: its slot carries real resume value, so it
            // is kept across the restart. A `mode: memory` catalog re-snapshots
            // on every start, so its slot is released at shutdown instead.
            let data_dir = tempfile::tempdir()?;

            // First run: bootstrap + stream.
            let rt = start_runtime(durable_accelerated_pg_catalog(port, data_dir.path())).await?;
            wait_for_table_ready(&rt, "orders", "bootstrap before shutdown").await?;

            // Exactly one slot, named deterministically from the catalog (with no
            // instance component).
            let slots = replication_slot_names(port).await?;
            anyhow::ensure!(
                slots == vec![expected_slot.clone()],
                "expected exactly one catalog slot '{expected_slot}', got {slots:?}"
            );

            // Shut the runtime down -- simulating a restart / reschedule.
            rt.shutdown().await;
            drop(rt);

            // The slot must PERSIST across shutdown (it is not a temporary slot)
            // and go inactive once the walsender exits -- that is what lets the
            // restart resume it instead of re-creating it. Allow up to the
            // server's wal_sender_timeout in case the connection isn't torn down
            // gracefully.
            let freed = wait_until_true(Duration::from_secs(90), || {
                let slot = expected_slot.clone();
                async move { matches!(slot_active(port, &slot).await, Ok(Some(false))) }
            })
            .await;
            anyhow::ensure!(
                freed,
                "slot '{expected_slot}' never became inactive after shutdown"
            );
            let during_downtime = replication_slot_names(port).await?;
            anyhow::ensure!(
                during_downtime == vec![expected_slot.clone()],
                "slot must persist across shutdown, got {during_downtime:?}"
            );

            // Mutate the source while Spice is down.
            let pool = common::get_postgres_connection_pool(port, None).await?;
            let conn = pool
                .connect_direct()
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            conn.conn
                .simple_query("INSERT INTO orders (id, customer) VALUES (3, 'carol-offline');")
                .await?;

            // Restart with the SAME catalog config -> same deterministic slot.
            let rt = start_runtime(durable_accelerated_pg_catalog(port, data_dir.path())).await?;
            wait_for_table_ready(&rt, "orders", "after restart").await?;

            // Still exactly one slot, same name: the restart REUSED it. An
            // instance-dependent name would have left the first orphaned and
            // created a second here.
            let slots_after = replication_slot_names(port).await?;
            anyhow::ensure!(
                slots_after == vec![expected_slot.clone()],
                "restart must reuse the single catalog slot, got {slots_after:?}"
            );

            // The change made while Spice was down (id = 3) is delivered after
            // restart -- this is the reused slot's guarantee: it resumes from its
            // retained `restart_lsn`, so WAL accumulated during the downtime is
            // replayed, nothing lost. A durable acceleration reaches this row
            // only through that replay: unlike `mode: memory`, it does not
            // re-snapshot the source on start.
            let offline_row_sql =
                format!("SELECT customer FROM {CATALOG_NAME}.public.orders WHERE id = 3");
            let delivered = wait_until_true(Duration::from_mins(2), || {
                let rt = Arc::clone(&rt);
                let sql = offline_row_sql.clone();
                async move { query_string(&rt, &sql).await.as_deref() == Some("carol-offline") }
            })
            .await;
            anyhow::ensure!(
                delivered,
                "change made during downtime (orders id=3) was not delivered after restart via the reused slot: got {:?}",
                query_string(&rt, &offline_row_sql).await
            );

            Ok(())
        })
        .await
}

/// Two Spice instances pointed at the same catalog resolve to the SAME
/// deterministic slot name. `PostgreSQL` permits only one consumer per slot, so
/// the second instance must FAIL LOUDLY rather than silently compete for the
/// first's stream. (A fast self-restart is distinguished from this by the
/// bounded wait -- exercised by `test_catalog_acceleration_reuses_slot_across_restart`.)
#[tokio::test(flavor = "multi_thread")]
async fn test_catalog_acceleration_fails_loud_when_slot_already_active() -> Result<(), anyhow::Error>
{
    let _tracing = init_tracing(Some(
        "integration=debug,info,runtime::catalogconnector=debug",
    ));

    test_request_context()
        .scope(async {
            let port = common::get_random_port()?;
            let _container = common::start_postgres_docker_container_with_logical_wal(port).await?;

            seed_tables(port).await?;

            // The second instance's slot-in-use wait is sized from
            // wal_sender_timeout; lower it so the test fails fast. 10s stays well
            // above the CDC keepalive round-trip, so instance A's own walsender is
            // not killed, while bounding instance B's wait to ~15s.
            set_wal_sender_timeout(port, "10s").await?;

            let expected_slot = catalog_slot_name(CATALOG_NAME);

            // Instance A acquires and actively holds the slot.
            let rt_a = start_runtime(accelerated_pg_catalog(port)).await?;
            wait_for_table_ready(&rt_a, "orders", "instance A bootstrap").await?;
            anyhow::ensure!(
                matches!(slot_active(port, &expected_slot).await?, Some(true)),
                "instance A should hold the slot active"
            );

            // Instance B, same catalog -> same slot -> must fail loudly. Use
            // build_and_load_runtime (no readiness assertion): B's catalog is
            // expected to error, leaving its runtime not-ready by design.
            let rt_b = build_and_load_runtime(accelerated_pg_catalog(port)).await?;
            let errored = wait_until_true(Duration::from_mins(1), || {
                let rt_b = Arc::clone(&rt_b);
                async move {
                    rt_b.status()
                        .get_catalog_statuses()
                        .get(CATALOG_NAME)
                        .is_some_and(|s| matches!(s, ComponentStatus::Error(_)))
                }
            })
            .await;
            anyhow::ensure!(
                errored,
                "second instance should fail loudly on the in-use slot, got {:?}",
                rt_b.status().get_catalog_statuses().get(CATALOG_NAME)
            );
            match rt_b.status().get_catalog_statuses().get(CATALOG_NAME) {
                Some(ComponentStatus::Error(Some(message))) => {
                    // Must be the in-use error AND name the specific slot, so the
                    // assertion can't pass on some unrelated "already in use" text.
                    anyhow::ensure!(
                        message.contains("already in use") && message.contains(&expected_slot),
                        "error should report slot '{expected_slot}' already in use, got: {message}"
                    );
                }
                other => anyhow::bail!(
                    "second instance should be in an Error state with an actionable message, got {other:?}"
                ),
            }

            // Instance A is unaffected -- still the single active consumer, still
            // serving its accelerated tables.
            anyhow::ensure!(
                replication_slot_names(port).await? == vec![expected_slot.clone()],
                "there must still be exactly one slot"
            );
            let count_sql = format!("SELECT COUNT(*) AS n FROM {CATALOG_NAME}.public.orders");
            anyhow::ensure!(
                query_i64(&rt_a, &count_sql).await == Some(2),
                "instance A must keep serving its accelerated tables"
            );

            Ok(())
        })
        .await
}
