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
use spicepod::component::catalog::Catalog;
use spicepod::component::dataset::Dataset;
use spicepod::param::Params;
use tempfile::TempDir;

/// Bootstrap a `DuckLake` catalog at the given metadata path with test tables.
///
/// Creates tables: `orders`, `customers`, `products` under the `main` schema.
fn bootstrap_ducklake(metadata_path: &str, data_path: &str) {
    let db = duckdb::Connection::open_in_memory().expect("open in-memory DuckDB");
    db.execute("INSTALL ducklake", [])
        .expect("install ducklake");
    db.execute("LOAD ducklake", []).expect("load ducklake");

    let escaped_metadata = metadata_path.replace('\'', "''");
    let escaped_data = data_path.replace('\'', "''");
    let attach_sql =
        format!("ATTACH 'ducklake:{escaped_metadata}' AS test_lake (DATA_PATH '{escaped_data}')");
    db.execute(&attach_sql, []).expect("attach ducklake");

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
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
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
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
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
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
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
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
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
    use spicepod::component::access::AccessMode;

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
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    panic!("Timeout waiting for components to load");
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

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
    use spicepod::component::access::AccessMode;

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
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
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
