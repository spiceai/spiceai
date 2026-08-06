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

//! Integration test for the `PostgreSQL` catalog connector's handling of
//! declaratively-partitioned tables (#11726).
//!
//! A partitioned parent with two leaf partitions (plus a plain table) is seeded,
//! then discovered through the catalog connector. The connector must
//! register the partitioned parent but *not* its leaf partitions: the parent is a
//! union over its children, so registering both would double-count aggregates and
//! clutter the catalog. Querying the parent still returns rows from every
//! partition. This mirrors how the CDC path attributes partitioned-table changes
//! to the parent (`publish_via_partition_root = true`), so the one-time import and
//! the streamed import agree on the parent as the unit of a partitioned table.

use std::{collections::HashMap, sync::Arc, time::Duration};

use app::AppBuilder;
use arrow::array::{Array, RecordBatch, StringArray};
use datafusion::assert_batches_eq;
use runtime::Runtime;
use secrecy::ExposeSecret;
use spicepod::{component::catalog::Catalog, param::Params};

use crate::{
    configure_test_datafusion, init_tracing,
    postgres::common::{self, get_pg_params},
    utils::{register_test_connectors, run_query, runtime_ready_check, test_request_context},
};

const CATALOG_NAME: &str = "pg_e2e";

/// Seed a range-partitioned `events` table with two leaf partitions, populate it
/// both by inserting directly into each child and by inserting into the parent
/// (which `PostgreSQL` auto-routes to the matching leaf), and add a plain table to
/// confirm non-partitioned tables still surface. Verifies at the source that the
/// parent-routed rows physically landed in the leaves.
async fn seed_partitioned_schema(port: usize) -> Result<(), anyhow::Error> {
    let pool = common::get_postgres_connection_pool(port, None).await?;
    let conn = pool
        .connect_direct()
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    // Range-partitioned parent (relkind 'p') with two leaf partitions (relkind
    // 'r'). The partition key must be part of the primary key for a partitioned
    // PK, matching real-world declarative partitioning.
    conn.conn
        .simple_query(
            "CREATE TABLE events ( \
                 id      INT  NOT NULL, \
                 payload TEXT NOT NULL, \
                 PRIMARY KEY (id) \
             ) PARTITION BY RANGE (id); \
             CREATE TABLE events_lo PARTITION OF events FOR VALUES FROM (0) TO (100); \
             CREATE TABLE events_hi PARTITION OF events FOR VALUES FROM (100) TO (1000);",
        )
        .await?;

    // Insert directly into each child (3 into `events_lo`, 2 into `events_hi`)...
    conn.conn
        .simple_query(
            "INSERT INTO events_lo (id, payload) VALUES (1, 'a'), (2, 'b'), (3, 'c'); \
             INSERT INTO events_hi (id, payload) VALUES (100, 'd'), (200, 'e');",
        )
        .await?;

    // ...and insert into the parent, letting PostgreSQL auto-route by the
    // partition key: id 50 -> `events_lo`, id 150 -> `events_hi`. Seven rows total.
    conn.conn
        .simple_query("INSERT INTO events (id, payload) VALUES (50, 'f'), (150, 'g');")
        .await?;

    // The parent stores no rows of its own; every row lives in a leaf. This
    // confirms the parent-routed inserts landed in the leaves (auto-partitioning).
    let only_parent: i64 = conn
        .conn
        .query_one("SELECT COUNT(*) FROM ONLY events", &[])
        .await?
        .get(0);
    anyhow::ensure!(
        only_parent == 0,
        "partitioned parent should store no rows itself, found {only_parent}"
    );
    let lo: i64 = conn
        .conn
        .query_one("SELECT COUNT(*) FROM events_lo", &[])
        .await?
        .get(0);
    let hi: i64 = conn
        .conn
        .query_one("SELECT COUNT(*) FROM events_hi", &[])
        .await?
        .get(0);
    anyhow::ensure!(
        lo == 4 && hi == 3,
        "expected 4 rows in events_lo and 3 in events_hi after auto-routing, found lo={lo} hi={hi}"
    );

    // A plain (non-partitioned) table must still be discovered.
    conn.conn
        .simple_query(
            "CREATE TABLE widgets (id INT PRIMARY KEY, name TEXT NOT NULL); \
             INSERT INTO widgets (id, name) VALUES (1, 'widget');",
        )
        .await?;

    Ok(())
}

/// Build a `PostgreSQL` catalog against the seeded database.
fn pg_catalog(port: usize) -> Catalog {
    let mut catalog = Catalog::new("pg:postgres".to_string(), CATALOG_NAME.to_string());
    catalog.params = Some(Params::from_string_map(
        get_pg_params(port)
            .into_iter()
            .map(|(k, v)| (k, v.expose_secret().to_string()))
            .collect::<HashMap<String, String>>(),
    ));
    catalog
}

/// Collect the values of a `Utf8` column across every batch, in row order.
fn string_column_values(batches: &[RecordBatch], column: &str) -> Vec<String> {
    let mut values = Vec::new();
    for batch in batches {
        let idx = batch
            .schema()
            .index_of(column)
            .unwrap_or_else(|e| panic!("expected column `{column}` in query result: {e}"));
        let array = batch
            .column(idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("column should be a Utf8 array");
        for i in 0..array.len() {
            values.push(array.value(i).to_string());
        }
    }
    values
}

/// Seed a plain table, a materialized view over it, and (via `postgres_fdw`)
/// foreign tables pointed at it, to confirm all three relation kinds are
/// discovered (#11725).
///
/// One foreign table wraps a populated remote table and one wraps an empty one,
/// so the schema a foreign table reports can be checked independently of whether
/// it has any rows (#12585).
async fn seed_matview_and_foreign_table(port: usize) -> Result<(), anyhow::Error> {
    let pool = common::get_postgres_connection_pool(port, None).await?;
    let conn = pool
        .connect_direct()
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    conn.conn
        .simple_query(
            "CREATE TABLE source_data (id INT PRIMARY KEY, val TEXT, amount NUMERIC(10,2)); \
             INSERT INTO source_data (id, val, amount) VALUES (1, 'a', 1.50), (2, 'b', 2.25); \
             CREATE MATERIALIZED VIEW mv_source_data AS SELECT * FROM source_data; \
             CREATE TABLE empty_source (id INT, note TEXT);",
        )
        .await?;

    conn.conn
        .simple_query(&format!(
            "CREATE EXTENSION IF NOT EXISTS postgres_fdw; \
             CREATE SERVER loopback FOREIGN DATA WRAPPER postgres_fdw \
                 OPTIONS (host 'localhost', port '5432', dbname 'postgres'); \
             CREATE USER MAPPING FOR postgres SERVER loopback \
                 OPTIONS (user 'postgres', password '{}'); \
             CREATE FOREIGN TABLE ft_source_data (id INT, val TEXT, amount NUMERIC(10,2)) \
                 SERVER loopback OPTIONS (table_name 'source_data'); \
             CREATE FOREIGN TABLE ft_empty (id INT, note TEXT) \
                 SERVER loopback OPTIONS (table_name 'empty_source');",
            common::PG_PASSWORD
        ))
        .await?;

    Ok(())
}

/// Seed a table with a `jsonb` column (the type the underlying connector
/// documents as convertible under `unsupported_type_action: string`, see
/// `pg_data_type_to_arrow_type` in `datafusion-table-providers`) plus real rows.
async fn seed_unsupported_type_table(port: usize) -> Result<(), anyhow::Error> {
    let pool = common::get_postgres_connection_pool(port, None).await?;
    let conn = pool
        .connect_direct()
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    conn.conn
        .simple_query(
            "CREATE TABLE widgets_jsonb ( \
                 id       INT  PRIMARY KEY, \
                 metadata JSONB NOT NULL \
             ); \
             INSERT INTO widgets_jsonb (id, metadata) VALUES \
                 (1, '{\"color\": \"red\"}'), (2, '{\"color\": \"blue\"}');",
        )
        .await?;

    Ok(())
}

async fn start_runtime(catalog: Catalog) -> Result<Arc<Runtime>, anyhow::Error> {
    register_test_connectors().await;
    let app = AppBuilder::new("postgres_catalog_partition_test")
        .with_catalog(catalog)
        .build();

    configure_test_datafusion();
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

/// The catalog connector registers the partitioned parent but omits its leaf
/// partitions, and the parent still exposes every partition's rows.
#[tokio::test]
async fn test_partitioned_table_registers_parent_only() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let port = common::get_random_port()?;
            let _container = common::start_postgres_docker_container(port).await?;

            seed_partitioned_schema(port).await?;

            let rt = start_runtime(pg_catalog(port)).await?;

            // Only the partitioned parent and the plain table are registered
            // under the catalog — the two leaf partitions (`events_lo`,
            // `events_hi`) must be absent, otherwise a "count across all tables"
            // would double-count the parent's rows.
            let tables = run_query(
                &rt,
                &format!(
                    "SELECT table_name FROM information_schema.tables \
                     WHERE table_catalog = '{CATALOG_NAME}' AND table_schema = 'public' \
                     ORDER BY table_name"
                ),
            )
            .await?;
            assert_batches_eq!(
                &[
                    "+------------+",
                    "| table_name |",
                    "+------------+",
                    "| events     |",
                    "| widgets    |",
                    "+------------+",
                ],
                &tables
            );

            // The parent exposes rows from both leaf partitions — the five
            // inserted directly into children plus the two auto-routed through
            // the parent (7 total) — proving parent-only registration loses no
            // data regardless of how rows were inserted.
            let count = run_query(
                &rt,
                &format!("SELECT COUNT(*) AS n FROM {CATALOG_NAME}.public.events"),
            )
            .await?;
            assert_batches_eq!(
                &[
                    "+---+", //
                    "| n |", //
                    "+---+", //
                    "| 7 |", //
                    "+---+", //
                ],
                &count
            );

            Ok(())
        })
        .await
}

/// The catalog connector discovers materialized views and foreign tables, not
/// just base tables and standard views (#11725).
#[tokio::test]
async fn test_materialized_view_and_foreign_table_discovered() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let port = common::get_random_port()?;
            let _container = common::start_postgres_docker_container(port).await?;

            seed_matview_and_foreign_table(port).await?;

            let rt = start_runtime(pg_catalog(port)).await?;

            let tables = run_query(
                &rt,
                &format!(
                    "SELECT table_name FROM information_schema.tables \
                     WHERE table_catalog = '{CATALOG_NAME}' AND table_schema = 'public' \
                     ORDER BY table_name"
                ),
            )
            .await?;
            assert_eq!(
                string_column_values(&tables, "table_name"),
                vec![
                    "empty_source".to_string(),
                    "ft_empty".to_string(),
                    "ft_source_data".to_string(),
                    "mv_source_data".to_string(),
                    "source_data".to_string(),
                ],
                "the base tables, materialized view, and foreign tables should all be registered"
            );

            let mv_count = run_query(
                &rt,
                &format!("SELECT COUNT(*) AS n FROM {CATALOG_NAME}.public.mv_source_data"),
            )
            .await?;
            assert_batches_eq!(&["+---+", "| n |", "+---+", "| 2 |", "+---+"], &mv_count);

            let ft_count = run_query(
                &rt,
                &format!("SELECT COUNT(*) AS n FROM {CATALOG_NAME}.public.ft_source_data"),
            )
            .await?;
            assert_batches_eq!(&["+---+", "| n |", "+---+", "| 2 |", "+---+"], &ft_count);

            // A foreign table's schema comes from its local `pg_attribute`
            // definition, not from sampling its rows, so it carries the declared
            // types rather than whatever a sample row happened to imply. An empty
            // foreign table used to register with no columns at all, and a
            // `NUMERIC(p,s)` column used to widen to the fallback precision.
            let ft_columns = run_query(
                &rt,
                &format!(
                    "SELECT column_name, data_type FROM information_schema.columns \
                     WHERE table_catalog = '{CATALOG_NAME}' AND table_schema = 'public' \
                     AND table_name = 'ft_empty' ORDER BY column_name"
                ),
            )
            .await?;
            assert_eq!(
                string_column_values(&ft_columns, "column_name"),
                vec!["id".to_string(), "note".to_string()],
                "an empty foreign table must still expose its declared columns"
            );

            let ft_types = run_query(
                &rt,
                &format!(
                    "SELECT data_type FROM information_schema.columns \
                     WHERE table_catalog = '{CATALOG_NAME}' AND table_schema = 'public' \
                     AND table_name = 'ft_source_data' AND column_name = 'amount'"
                ),
            )
            .await?;
            assert_eq!(
                string_column_values(&ft_types, "data_type"),
                vec!["Decimal128(10, 2)".to_string()],
                "a foreign table must report the declared precision of its remote column"
            );

            Ok(())
        })
        .await
}

/// By default (no `dataset_params` override), a table with an unsupported
/// column type (`jsonb`) is registered with that column converted to a
/// string — matching the direct `PostgreSQL` data connector's default
/// `unsupported_type_action: string` — rather than being dropped from the
/// catalog entirely (#11728).
#[tokio::test]
async fn test_unsupported_type_action_defaults_to_string() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let port = common::get_random_port()?;
            let _container = common::start_postgres_docker_container(port).await?;

            seed_unsupported_type_table(port).await?;

            let rt = start_runtime(pg_catalog(port)).await?;

            let tables = run_query(
                &rt,
                &format!(
                    "SELECT table_name FROM information_schema.tables \
                     WHERE table_catalog = '{CATALOG_NAME}' AND table_schema = 'public' \
                     AND table_name = 'widgets_jsonb'"
                ),
            )
            .await?;
            assert_eq!(
                string_column_values(&tables, "table_name"),
                vec!["widgets_jsonb".to_string()],
                "a table with an unsupported jsonb column should still be registered by default"
            );

            // The unsupported `metadata` (jsonb) column must still be present —
            // converted to a string, not silently dropped — alongside `id`.
            let columns = run_query(
                &rt,
                &format!(
                    "SELECT column_name FROM information_schema.columns \
                     WHERE table_catalog = '{CATALOG_NAME}' AND table_schema = 'public' \
                     AND table_name = 'widgets_jsonb' \
                     ORDER BY column_name"
                ),
            )
            .await?;
            assert_eq!(
                string_column_values(&columns, "column_name"),
                vec!["id".to_string(), "metadata".to_string()],
                "the unsupported jsonb column should be kept (converted to a string), not dropped"
            );

            let count = run_query(
                &rt,
                &format!("SELECT COUNT(*) AS n FROM {CATALOG_NAME}.public.widgets_jsonb"),
            )
            .await?;
            assert_batches_eq!(&["+---+", "| n |", "+---+", "| 2 |", "+---+"], &count);

            Ok(())
        })
        .await
}

/// Setting `dataset_params.unsupported_type_action: error` on the catalog
/// threads through to the underlying connection pool, restoring the stricter
/// whole-table-dropped behavior for callers who want it (#11728).
#[tokio::test]
async fn test_unsupported_type_action_override_drops_table() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let port = common::get_random_port()?;
            let _container = common::start_postgres_docker_container(port).await?;

            seed_unsupported_type_table(port).await?;

            let mut catalog = pg_catalog(port);
            catalog.dataset_params = Some(Params::from_string_map(HashMap::from([(
                "unsupported_type_action".to_string(),
                "error".to_string(),
            )])));

            let rt = start_runtime(catalog).await?;

            let tables = run_query(
                &rt,
                &format!(
                    "SELECT table_name FROM information_schema.tables \
                     WHERE table_catalog = '{CATALOG_NAME}' AND table_schema = 'public' \
                     AND table_name = 'widgets_jsonb'"
                ),
            )
            .await?;
            assert!(
                string_column_values(&tables, "table_name").is_empty(),
                "a table with an unsupported jsonb column should be dropped when unsupported_type_action=error"
            );

            Ok(())
        })
        .await
}
