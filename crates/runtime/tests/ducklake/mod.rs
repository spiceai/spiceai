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

use std::collections::HashMap;
use std::sync::Arc;

use crate::utils::{register_test_connectors, runtime_ready_check, test_request_context};
use crate::{configure_test_datafusion, init_tracing};
use app::AppBuilder;
use arrow::array::RecordBatch;
use datafusion::assert_batches_eq;
use futures::TryStreamExt;
use runtime::Runtime;
use spicepod::component::access::AccessMode;
use spicepod::component::catalog::Catalog;
use spicepod::component::dataset::Dataset;
use spicepod::param::Params;
use tempfile::TempDir;

/// Run `EXPLAIN` and return pretty-formatted output for snapshot comparison.
async fn explain_plan(rt: &Runtime, sql: &str) -> String {
    let result = rt
        .datafusion()
        .query_builder(&format!("EXPLAIN {sql}"))
        .build()
        .run()
        .await
        .expect("EXPLAIN should succeed");
    let batches: Vec<RecordBatch> = result.data.try_collect().await.expect("collect");
    arrow::util::pretty::pretty_format_batches(&batches)
        .expect("format")
        .to_string()
}

/// Opens an in-memory `DuckDB` connection with the `ducklake` extension loaded
/// and the lake at `metadata_path` attached under `alias`.
fn attach_ducklake(alias: &str, metadata_path: &str, data_path: &str) -> duckdb::Connection {
    let db = duckdb::Connection::open_in_memory().expect("open in-memory DuckDB");
    db.execute("INSTALL ducklake", [])
        .expect("install ducklake");
    db.execute("LOAD ducklake", []).expect("load ducklake");

    let escaped_metadata = metadata_path.replace('\'', "''");
    let escaped_data = data_path.replace('\'', "''");
    let attach_sql =
        format!("ATTACH 'ducklake:{escaped_metadata}' AS {alias} (DATA_PATH '{escaped_data}')");
    db.execute(&attach_sql, []).expect("attach ducklake");

    db
}

/// Bootstrap a `DuckLake` catalog at the given metadata path with test tables.
///
/// Creates tables: `orders`, `customers`, `products` under the `main` schema.
fn bootstrap_ducklake(metadata_path: &str, data_path: &str) {
    let db = attach_ducklake("test_lake", metadata_path, data_path);

    db.execute(
        "CREATE TABLE test_lake.main.orders (id INTEGER, customer_id INTEGER, total DOUBLE)",
        [],
    )
    .expect("create orders");
    db.execute(
        "INSERT INTO test_lake.main.orders VALUES (1, 10, 99.99), (2, 20, 149.50), (3, 10, 25.00)",
        [],
    )
    .expect("insert orders");

    db.execute(
        "CREATE TABLE test_lake.main.customers (id INTEGER, name VARCHAR)",
        [],
    )
    .expect("create customers");
    db.execute(
        "INSERT INTO test_lake.main.customers VALUES (10, 'Alice'), (20, 'Bob')",
        [],
    )
    .expect("insert customers");

    db.execute(
        "CREATE TABLE test_lake.main.products (id INTEGER, name VARCHAR, price DOUBLE)",
        [],
    )
    .expect("create products");
    db.execute(
        "INSERT INTO test_lake.main.products VALUES (1, 'Widget', 9.99), (2, 'Gadget', 19.99)",
        [],
    )
    .expect("insert products");
}

fn make_ducklake_catalog_params(metadata_path: &str) -> Params {
    Params::from_string_map(HashMap::from([(
        "ducklake_connection_string".to_string(),
        metadata_path.to_string(),
    )]))
}

/// Tests that the `include` filter on a `DuckLake` catalog correctly limits
/// which tables are registered. Without the fix, all tables would appear.
#[tokio::test]
async fn ducklake_catalog_include_filter() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("runtime=DEBUG,data_components=DEBUG"));
    register_test_connectors().await;

    let tmp_dir = TempDir::new()?;
    let metadata_path = tmp_dir.path().join("test.ducklake").display().to_string();
    let data_path = tmp_dir.path().join("data").display().to_string();
    std::fs::create_dir_all(&data_path)?;

    bootstrap_ducklake(&metadata_path, &data_path);

    test_request_context()
        .scope(async {
            // Register catalog with include filter: only main.orders and main.customers
            let mut catalog = Catalog::new("ducklake".to_string(), "test_lake".to_string());
            catalog.params = Some(make_ducklake_catalog_params(&metadata_path));
            catalog.include = vec!["main.orders".to_string(), "main.customers".to_string()];

            let app = AppBuilder::new("ducklake_include_test")
                .with_catalog(catalog)
                .build();

            configure_test_datafusion();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);
            let cloned_rt = Arc::clone(&rt);

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
                    panic!("Timeout waiting for components to load");
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            // Query information_schema to verify only included tables are registered
            let result = rt
                .datafusion()
                .query_builder(
                    "SELECT table_catalog, table_schema, table_name, table_type \
                     FROM information_schema.tables \
                     WHERE table_catalog = 'test_lake' \
                       AND table_schema != 'information_schema' \
                     ORDER BY table_name",
                )
                .build()
                .run()
                .await?;

            let results: Vec<RecordBatch> = result.data.try_collect().await?;

            // Only orders and customers should appear, NOT products
            assert_batches_eq!(
                &[
                    "+---------------+--------------+------------+------------+",
                    "| table_catalog | table_schema | table_name | table_type |",
                    "+---------------+--------------+------------+------------+",
                    "| test_lake     | main         | customers  | BASE TABLE |",
                    "| test_lake     | main         | orders     | BASE TABLE |",
                    "+---------------+--------------+------------+------------+",
                ],
                &results
            );

            // Verify included tables are queryable
            let result = rt
                .datafusion()
                .query_builder("SELECT id, name FROM test_lake.main.customers ORDER BY id")
                .build()
                .run()
                .await?;
            let results: Vec<RecordBatch> = result.data.try_collect().await?;
            assert_batches_eq!(
                &[
                    "+----+-------+",
                    "| id | name  |",
                    "+----+-------+",
                    "| 10 | Alice |",
                    "| 20 | Bob   |",
                    "+----+-------+",
                ],
                &results
            );

            Ok(())
        })
        .await
}

/// Tests that a `DuckLake` catalog without an `include` filter registers all tables.
#[tokio::test]
async fn ducklake_catalog_no_filter() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("runtime=DEBUG,data_components=DEBUG"));
    register_test_connectors().await;

    let tmp_dir = TempDir::new()?;
    let metadata_path = tmp_dir
        .path()
        .join("test_all.ducklake")
        .display()
        .to_string();
    let data_path = tmp_dir.path().join("data_all").display().to_string();
    std::fs::create_dir_all(&data_path)?;

    bootstrap_ducklake(&metadata_path, &data_path);

    test_request_context()
        .scope(async {
            // Register catalog WITHOUT include filter — all tables should appear
            let mut catalog = Catalog::new("ducklake".to_string(), "all_tables".to_string());
            catalog.params = Some(make_ducklake_catalog_params(&metadata_path));

            let app = AppBuilder::new("ducklake_all_test")
                .with_catalog(catalog)
                .build();

            configure_test_datafusion();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);
            let cloned_rt = Arc::clone(&rt);

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
                    panic!("Timeout waiting for components to load");
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            // Query information_schema to verify ALL tables are registered
            let result = rt
                .datafusion()
                .query_builder(
                    "SELECT table_catalog, table_schema, table_name, table_type \
                     FROM information_schema.tables \
                     WHERE table_catalog = 'all_tables' \
                       AND table_schema != 'information_schema' \
                     ORDER BY table_name",
                )
                .build()
                .run()
                .await?;

            let results: Vec<RecordBatch> = result.data.try_collect().await?;

            assert_batches_eq!(
                &[
                    "+---------------+--------------+------------+------------+",
                    "| table_catalog | table_schema | table_name | table_type |",
                    "+---------------+--------------+------------+------------+",
                    "| all_tables    | main         | customers  | BASE TABLE |",
                    "| all_tables    | main         | orders     | BASE TABLE |",
                    "| all_tables    | main         | products   | BASE TABLE |",
                    "+---------------+--------------+------------+------------+",
                ],
                &results
            );

            // Verify a catalog table is queryable
            let result = rt
                .datafusion()
                .query_builder("SELECT id, total FROM all_tables.main.orders ORDER BY id")
                .build()
                .run()
                .await?;
            let results: Vec<RecordBatch> = result.data.try_collect().await?;
            assert_batches_eq!(
                &[
                    "+----+-------+",
                    "| id | total |",
                    "+----+-------+",
                    "| 1  | 99.99 |",
                    "| 2  | 149.5 |",
                    "| 3  | 25.0  |",
                    "+----+-------+",
                ],
                &results
            );

            // Verify single-table query is federated
            let plan = explain_plan(&rt, "SELECT id, total FROM all_tables.main.orders WHERE id > 1").await;
            insta::assert_snapshot!("read_only_catalog_single_table", plan);

            // Verify cross-table JOIN is federated
            let plan = explain_plan(
                &rt,
                "SELECT o.id, c.name, o.total FROM all_tables.main.orders o JOIN all_tables.main.customers c ON o.customer_id = c.id",
            ).await;
            insta::assert_snapshot!("read_only_catalog_cross_table_join", plan);

            Ok(())
        })
        .await
}

/// Tests that the `include` filter works with glob patterns.
#[tokio::test]
async fn ducklake_catalog_include_glob_pattern() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("runtime=DEBUG,data_components=DEBUG"));
    register_test_connectors().await;

    let tmp_dir = TempDir::new()?;
    let metadata_path = tmp_dir
        .path()
        .join("test_glob.ducklake")
        .display()
        .to_string();
    let data_path = tmp_dir.path().join("data_glob").display().to_string();
    std::fs::create_dir_all(&data_path)?;

    bootstrap_ducklake(&metadata_path, &data_path);

    test_request_context()
        .scope(async {
            // Use glob pattern: main.o* should match only 'orders'
            let mut catalog = Catalog::new("ducklake".to_string(), "glob_lake".to_string());
            catalog.params = Some(make_ducklake_catalog_params(&metadata_path));
            catalog.include = vec!["main.o*".to_string()];

            let app = AppBuilder::new("ducklake_glob_test")
                .with_catalog(catalog)
                .build();

            configure_test_datafusion();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);
            let cloned_rt = Arc::clone(&rt);

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
                    panic!("Timeout waiting for components to load");
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            let result = rt
                .datafusion()
                .query_builder(
                    "SELECT table_catalog, table_schema, table_name, table_type \
                     FROM information_schema.tables \
                     WHERE table_catalog = 'glob_lake' \
                       AND table_schema != 'information_schema' \
                     ORDER BY table_name",
                )
                .build()
                .run()
                .await?;

            let results: Vec<RecordBatch> = result.data.try_collect().await?;

            assert_batches_eq!(
                &[
                    "+---------------+--------------+------------+------------+",
                    "| table_catalog | table_schema | table_name | table_type |",
                    "+---------------+--------------+------------+------------+",
                    "| glob_lake     | main         | orders     | BASE TABLE |",
                    "+---------------+--------------+------------+------------+",
                ],
                &results
            );

            Ok(())
        })
        .await
}

/// Tests that a standalone `DuckLake` dataset (registered individually, not via catalog)
/// is queryable.
#[tokio::test]
async fn ducklake_standalone_dataset() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("runtime=DEBUG,data_components=DEBUG"));
    register_test_connectors().await;

    let tmp_dir = TempDir::new()?;
    let metadata_path = tmp_dir
        .path()
        .join("test_standalone.ducklake")
        .display()
        .to_string();
    let data_path = tmp_dir.path().join("data_standalone").display().to_string();
    std::fs::create_dir_all(&data_path)?;

    bootstrap_ducklake(&metadata_path, &data_path);

    test_request_context()
        .scope(async {
            // Register a standalone dataset using the ducklake data connector
            let mut dataset = Dataset::new(
                "ducklake:main.products".to_string(),
                "my_products".to_string(),
            );
            dataset.params = Some(Params::from_string_map(HashMap::from([(
                "ducklake_connection_string".to_string(),
                metadata_path.clone(),
            )])));

            let app = AppBuilder::new("ducklake_standalone_test")
                .with_dataset(dataset)
                .build();

            configure_test_datafusion();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);
            let cloned_rt = Arc::clone(&rt);

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
                    panic!("Timeout waiting for components to load");
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            let result = rt
                .datafusion()
                .query_builder("SELECT id, name, price FROM my_products ORDER BY id")
                .build()
                .run()
                .await?;
            let results: Vec<RecordBatch> = result.data.try_collect().await?;
            assert_batches_eq!(
                &[
                    "+----+--------+-------+",
                    "| id | name   | price |",
                    "+----+--------+-------+",
                    "| 1  | Widget | 9.99  |",
                    "| 2  | Gadget | 19.99 |",
                    "+----+--------+-------+",
                ],
                &results
            );

            Ok(())
        })
        .await
}

/// Tests that INSERT works on a standalone `DuckLake` dataset when `access: read_write` is set.
#[tokio::test]
async fn ducklake_standalone_read_write_insert() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("runtime=DEBUG,data_components=DEBUG"));
    register_test_connectors().await;

    let tmp_dir = TempDir::new()?;
    let metadata_path = tmp_dir
        .path()
        .join("test_standalone_rw.ducklake")
        .display()
        .to_string();
    let data_path = tmp_dir
        .path()
        .join("data_standalone_rw")
        .display()
        .to_string();
    std::fs::create_dir_all(&data_path)?;

    bootstrap_ducklake(&metadata_path, &data_path);

    test_request_context()
        .scope(async {
            let mut dataset =
                Dataset::new("ducklake:main.orders".to_string(), "my_orders".to_string());
            dataset.params = Some(Params::from_string_map(HashMap::from([(
                "ducklake_connection_string".to_string(),
                metadata_path.clone(),
            )])));
            dataset.access = AccessMode::ReadWrite;

            let app = AppBuilder::new("ducklake_standalone_rw_test")
                .with_dataset(dataset)
                .build();

            configure_test_datafusion();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);
            let cloned_rt = Arc::clone(&rt);

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
                    panic!("Timeout waiting for components to load");
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            // Verify standalone dataset query is federated with read_write access
            let plan = explain_plan(&rt, "SELECT id, total FROM my_orders WHERE id > 1").await;
            insta::assert_snapshot!("read_write_standalone_single_table", plan);

            // Verify initial data
            let result = rt
                .datafusion()
                .query_builder("SELECT COUNT(*) AS cnt FROM my_orders")
                .build()
                .run()
                .await?;
            let results: Vec<RecordBatch> = result.data.try_collect().await?;
            assert_batches_eq!(
                &["+-----+", "| cnt |", "+-----+", "| 3   |", "+-----+",],
                &results
            );

            // INSERT a new row
            let insert_result = rt
                .datafusion()
                .query_builder(
                    "INSERT INTO my_orders (id, customer_id, total) VALUES (4, 30, 75.25)",
                )
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!("INSERT failed: {e}"))?;
            let _: Vec<RecordBatch> = insert_result.data.try_collect().await?;

            // Verify the row was inserted
            let result = rt
                .datafusion()
                .query_builder("SELECT id, customer_id, total FROM my_orders ORDER BY id")
                .build()
                .run()
                .await?;
            let results: Vec<RecordBatch> = result.data.try_collect().await?;
            assert_batches_eq!(
                &[
                    "+----+-------------+-------+",
                    "| id | customer_id | total |",
                    "+----+-------------+-------+",
                    "| 1  | 10          | 99.99 |",
                    "| 2  | 20          | 149.5 |",
                    "| 3  | 10          | 25.0  |",
                    "| 4  | 30          | 75.25 |",
                    "+----+-------------+-------+",
                ],
                &results
            );

            Ok(())
        })
        .await
}

/// Tests that INSERT works on a `DuckLake` catalog table when `access: read_write` is set.
#[tokio::test]
async fn ducklake_catalog_read_write_insert() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("runtime=DEBUG,data_components=DEBUG"));
    register_test_connectors().await;

    let tmp_dir = TempDir::new()?;
    let metadata_path = tmp_dir
        .path()
        .join("test_rw.ducklake")
        .display()
        .to_string();
    let data_path = tmp_dir.path().join("data_rw").display().to_string();
    std::fs::create_dir_all(&data_path)?;

    bootstrap_ducklake(&metadata_path, &data_path);

    test_request_context()
        .scope(async {
            let mut catalog = Catalog::new("ducklake".to_string(), "rw_lake".to_string())
                .with_access(AccessMode::ReadWrite);
            catalog.params = Some(make_ducklake_catalog_params(&metadata_path));

            let app = AppBuilder::new("ducklake_rw_test")
                .with_catalog(catalog)
                .build();

            configure_test_datafusion();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);
            let cloned_rt = Arc::clone(&rt);

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
                    panic!("Timeout waiting for components to load");
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            // Verify initial data
            let result = rt
                .datafusion()
                .query_builder("SELECT COUNT(*) AS cnt FROM rw_lake.main.orders")
                .build()
                .run()
                .await?;
            let results: Vec<RecordBatch> = result.data.try_collect().await?;
            assert_batches_eq!(
                &[
                    "+-----+",
                    "| cnt |",
                    "+-----+",
                    "| 3   |",
                    "+-----+",
                ],
                &results
            );

            // Verify single-table query is federated even with read_write access
            let plan = explain_plan(&rt, "SELECT id, total FROM rw_lake.main.orders WHERE id > 1").await;
            insta::assert_snapshot!("read_write_catalog_single_table", plan);

            // Verify cross-table JOIN is federated as a single pushed-down query
            let plan = explain_plan(
                &rt,
                "SELECT o.id, c.name, o.total FROM rw_lake.main.orders o JOIN rw_lake.main.customers c ON o.customer_id = c.id",
            ).await;
            insta::assert_snapshot!("read_write_catalog_cross_table_join", plan);

            // INSERT a new row (drain the stream to ensure the INSERT completes)
            let insert_result = rt
                .datafusion()
                .query_builder(
                    "INSERT INTO rw_lake.main.orders (id, customer_id, total) VALUES (4, 30, 75.25)",
                )
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!("INSERT failed: {e}"))?;
            let _: Vec<RecordBatch> = insert_result.data.try_collect().await?;

            // Verify the row was inserted
            let result = rt
                .datafusion()
                .query_builder(
                    "SELECT id, customer_id, total FROM rw_lake.main.orders ORDER BY id",
                )
                .build()
                .run()
                .await?;
            let results: Vec<RecordBatch> = result.data.try_collect().await?;
            assert_batches_eq!(
                &[
                    "+----+-------------+-------+",
                    "| id | customer_id | total |",
                    "+----+-------------+-------+",
                    "| 1  | 10          | 99.99 |",
                    "| 2  | 20          | 149.5 |",
                    "| 3  | 10          | 25.0  |",
                    "| 4  | 30          | 75.25 |",
                    "+----+-------------+-------+",
                ],
                &results
            );

            Ok(())
        })
        .await
}

/// A `DuckLake` catalog whose one table carries the strings the rewritten
/// built-ins are measured on: space-padded, character-padded, neither, and
/// NULL.
///
/// Deliberately a separate lake from [`bootstrap_ducklake`]: the tests above
/// assert the exact set of tables `information_schema` reports, so adding a
/// table to the shared fixture would change what they see.
fn bootstrap_ducklake_names(metadata_path: &str, data_path: &str) {
    let db = attach_ducklake("name_lake", metadata_path, data_path);

    db.execute(
        "CREATE TABLE name_lake.main.names (id INTEGER, name VARCHAR)",
        [],
    )
    .expect("create names");
    db.execute(
        "INSERT INTO name_lake.main.names VALUES \
         (1, '  padded  '), (2, 'xyhelloyx'), (3, 'Alpha'), (4, NULL)",
        [],
    )
    .expect("insert names");
}

/// The `DuckDB` unparser dialect has to be installed on the **catalog** route,
/// not only the dataset one.
///
/// `DuckLake` is `DuckDB`, so the SQL a federated scan sends it has to be
/// spelled the way `DuckDB` spells it. Two `DataFusion` built-ins federate to a
/// name `DuckDB` does not have: `trim` reaches the unparser as `btrim` (its own
/// name — `trim` is only an alias), and `regexp_like` has to become
/// `regexp_matches`. Only the dialect rewrites either, and nothing withholds a
/// built-in from pushdown, so a catalog built with the stock dialect sends
/// `DuckDB` a statement it rejects with
/// `Catalog Error: Scalar Function with name btrim does not exist!`.
///
/// The dataset route in the same app is the control: it registers the same
/// table through the connector, which has always attached the dialect, so the
/// two routes disagreeing is the asymmetry this pins (regression test for
/// #13825).
///
/// The `base_sql` assertions are what make this a guard rather than a smoke
/// test. They read the statement `DuckDB` is actually asked to run, so the test
/// fails if the call stops being pushed down at all — which is the other way an
/// answer-only assertion could go green with the dialect removed.
#[tokio::test]
async fn ducklake_catalog_route_installs_the_duckdb_dialect() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("runtime=DEBUG,data_components=DEBUG"));
    register_test_connectors().await;

    let tmp_dir = TempDir::new()?;
    let metadata_path = tmp_dir
        .path()
        .join("test_names.ducklake")
        .display()
        .to_string();
    let data_path = tmp_dir.path().join("data_names").display().to_string();
    std::fs::create_dir_all(&data_path)?;

    bootstrap_ducklake_names(&metadata_path, &data_path);

    test_request_context()
        .scope(async {
            let mut catalog = Catalog::new("ducklake".to_string(), "name_lake".to_string());
            catalog.params = Some(make_ducklake_catalog_params(&metadata_path));

            let mut dataset =
                Dataset::new("ducklake:main.names".to_string(), "names_ds".to_string());
            dataset.params = Some(Params::from_string_map(HashMap::from([(
                "ducklake_connection_string".to_string(),
                metadata_path.clone(),
            )])));

            let app = AppBuilder::new("ducklake_dialect_test")
                .with_catalog(catalog)
                .with_dataset(dataset)
                .build();

            configure_test_datafusion();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);
            let cloned_rt = Arc::clone(&rt);

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
                    panic!("Timeout waiting for components to load");
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            let query = "SELECT id, trim(name) AS trimmed, \
                         regexp_like(name, 'ell') AS matched \
                         FROM {table} ORDER BY id";
            let catalog_query = query.replace("{table}", "name_lake.main.names");

            // `base_sql` is the statement the federated scan sends DuckDB, and
            // the only part of the plan that says how the call is spelled: the
            // logical plan above it names the DataFusion function either way.
            let plan = explain_plan(&rt, &catalog_query).await;
            let remote_sql: String = plan
                .split("base_sql=")
                .skip(1)
                .map(|tail| tail.split('\n').next().unwrap_or_default().to_string())
                .collect::<Vec<_>>()
                .join("\n");
            assert!(
                remote_sql.contains("trim(") && !remote_sql.contains("btrim("),
                "DuckDB has no `btrim`; the catalog route must push down `trim`. Plan was:\n{plan}"
            );
            assert!(
                remote_sql.contains("regexp_matches(") && !remote_sql.contains("regexp_like("),
                "DuckDB has no `regexp_like`; the catalog route must push down \
                 `regexp_matches`. Plan was:\n{plan}"
            );

            let from_catalog: Vec<RecordBatch> = rt
                .datafusion()
                .query_builder(&catalog_query)
                .build()
                .run()
                .await?
                .data
                .try_collect()
                .await?;

            assert_batches_eq!(
                &[
                    "+----+-----------+---------+",
                    "| id | trimmed   | matched |",
                    "+----+-----------+---------+",
                    "| 1  | padded    | false   |",
                    "| 2  | xyhelloyx | true    |",
                    "| 3  | Alpha     | false   |",
                    "| 4  |           |         |",
                    "+----+-----------+---------+",
                ],
                &from_catalog
            );

            // The dataset route over the same table, which has always carried
            // the dialect. Answering the same thing is the point: a rewrite
            // that reached DuckDB but changed the answer would pass every
            // assertion above and fail here.
            let from_dataset: Vec<RecordBatch> = rt
                .datafusion()
                .query_builder(&query.replace("{table}", "names_ds"))
                .build()
                .run()
                .await?
                .data
                .try_collect()
                .await?;

            assert_eq!(
                arrow::util::pretty::pretty_format_batches(&from_catalog)?.to_string(),
                arrow::util::pretty::pretty_format_batches(&from_dataset)?.to_string(),
                "the catalog and dataset routes over one DuckLake table must agree"
            );

            Ok(())
        })
        .await
}
