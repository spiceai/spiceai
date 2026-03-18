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

use crate::{
    configure_test_datafusion, init_tracing,
    utils::{runtime_ready_check, test_request_context},
};
use app::AppBuilder;
use datafusion::assert_batches_eq;
use futures::TryStreamExt;
use runtime::Runtime;
use rusqlite::Connection;
use spicepod::component::dataset::Dataset;
use spicepod::param::Params;
use std::collections::HashMap;
use std::sync::Arc;

fn make_adbc_sqlite_dataset(ds_name: &str, table: &str, uri: &str) -> Dataset {
    let mut params = HashMap::new();
    params.insert("adbc_driver".to_string(), "sqlite".to_string());
    params.insert("adbc_uri".to_string(), uri.to_string());
    params.insert("connection_pool_size".to_string(), "1".to_string());

    let mut dataset = Dataset::new(format!("adbc:{table}"), ds_name.to_string());
    dataset.params = Some(Params::from_string_map(params));
    dataset
}

/// Returns a `(db_path, _guard)` pair. The `_guard` is a `TempDir` that
/// automatically removes the directory (and the database file inside it)
/// when it goes out of scope — even if the test panics.
fn temp_sqlite_db(name: &str) -> (String, tempfile::TempDir) {
    let dir = tempfile::Builder::new()
        .prefix(&format!("spice_adbc_test_{name}_"))
        .tempdir()
        .expect("Failed to create temp directory for ADBC test");
    let db_path = dir
        .path()
        .join("test.db")
        .to_str()
        .expect("Temp path is not valid UTF-8")
        .to_string();
    (db_path, dir)
}

/// Pre-create a table in the SQLite database so the ADBC connector can
/// discover its schema during `load_components()`.  Idempotent: drops
/// any previous version of the table first.
fn setup_sqlite_table(db_path: &str, table_name: &str, setup_sql: &str) {
    let conn = Connection::open(db_path).expect("Failed to open SQLite database");
    conn.execute_batch(&format!("DROP TABLE IF EXISTS {table_name};"))
        .expect("Failed to drop existing table");
    conn.execute_batch(setup_sql)
        .expect("Failed to execute setup SQL in SQLite");
}

#[tokio::test]
async fn test_adbc_sqlite_file_backed() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    let (db_path, _guard) = temp_sqlite_db("basic");

    setup_sqlite_table(
        &db_path,
        "test_table",
        "CREATE TABLE test_table (id INTEGER, name TEXT, value DOUBLE);
         INSERT INTO test_table VALUES (1, 'alice', 10.5), (2, 'bob', 20.3), (3, 'charlie', 15.7);",
    );

    test_request_context()
        .scope(async {
            let app = AppBuilder::new("adbc_sqlite_test")
                .with_dataset(make_adbc_sqlite_dataset(
                    "test_table",
                    "test_table",
                    &db_path,
                ))
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err("Timed out waiting for datasets to load".to_string());
                }
                () = Arc::new(rt.clone()).load_components() => {}
            }

            runtime_ready_check(&rt).await;

            // Query the data
            let result = rt
                .datafusion()
                .query_builder("SELECT * FROM test_table ORDER BY id")
                .build()
                .run()
                .await
                .map_err(|e| e.to_string())?
                .data
                .try_collect::<Vec<_>>()
                .await
                .map_err(|e| e.to_string())?;

            let expected = [
                "+----+---------+-------+",
                "| id | name    | value |",
                "+----+---------+-------+",
                "| 1  | alice   | 10.5  |",
                "| 2  | bob     | 20.3  |",
                "| 3  | charlie | 15.7  |",
                "+----+---------+-------+",
            ];
            assert_batches_eq!(expected, &result);

            // Test filter pushdown
            let filter_result = rt
                .datafusion()
                .query_builder("SELECT * FROM test_table WHERE id > 1 ORDER BY id")
                .build()
                .run()
                .await
                .map_err(|e| e.to_string())?
                .data
                .try_collect::<Vec<_>>()
                .await
                .map_err(|e| e.to_string())?;

            let expected_filtered = [
                "+----+---------+-------+",
                "| id | name    | value |",
                "+----+---------+-------+",
                "| 2  | bob     | 20.3  |",
                "| 3  | charlie | 15.7  |",
                "+----+---------+-------+",
            ];
            assert_batches_eq!(expected_filtered, &filter_result);

            // Test projection pushdown
            let projection_result = rt
                .datafusion()
                .query_builder("SELECT name FROM test_table ORDER BY id")
                .build()
                .run()
                .await
                .map_err(|e| e.to_string())?
                .data
                .try_collect::<Vec<_>>()
                .await
                .map_err(|e| e.to_string())?;

            let expected_projection = [
                "+---------+",
                "| name    |",
                "+---------+",
                "| alice   |",
                "| bob     |",
                "| charlie |",
                "+---------+",
            ];
            assert_batches_eq!(expected_projection, &projection_result);

            // Test limit pushdown
            let limit_result = rt
                .datafusion()
                .query_builder("SELECT * FROM test_table ORDER BY id LIMIT 2")
                .build()
                .run()
                .await
                .map_err(|e| e.to_string())?
                .data
                .try_collect::<Vec<_>>()
                .await
                .map_err(|e| e.to_string())?;

            let expected_limit = [
                "+----+-------+-------+",
                "| id | name  | value |",
                "+----+-------+-------+",
                "| 1  | alice | 10.5  |",
                "| 2  | bob   | 20.3  |",
                "+----+-------+-------+",
            ];
            assert_batches_eq!(expected_limit, &limit_result);

            Ok(())
        })
        .await
}

#[tokio::test]
#[ignore = "Requires ADBC DuckDB driver to be installed"]
async fn test_adbc_duckdb_file_backed() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    let (db_path, _guard) = temp_sqlite_db("duckdb");

    test_request_context()
        .scope(async {
            let mut params = HashMap::new();
            params.insert("adbc_driver".to_string(), "duckdb".to_string());
            params.insert("adbc_uri".to_string(), db_path);
            params.insert("connection_pool_size".to_string(), "1".to_string());

            let mut dataset = Dataset::new("adbc:test_table".to_string(), "test_table".to_string());
            dataset.params = Some(Params::from_string_map(params));

            let app = AppBuilder::new("adbc_duckdb_test")
                .with_dataset(dataset)
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err("Timed out waiting for datasets to load".to_string());
                }
                () = Arc::new(rt.clone()).load_components() => {}
            }

            runtime_ready_check(&rt).await;

            // Create test table
            rt.datafusion()
                .query_builder(
                    "CREATE TABLE test_table (id INTEGER, description VARCHAR, amount DECIMAL(10,2))",
                )
                .build()
                .run()
                .await
                .map_err(|e| format!("Failed to create table: {e}"))?;

            // Insert test data
            rt.datafusion()
                .query_builder(
                    "INSERT INTO test_table VALUES (1, 'first', 100.50), (2, 'second', 200.75), (3, 'third', 150.25)",
                )
                .build()
                .run()
                .await
                .map_err(|e| format!("Failed to insert data: {e}"))?;

            // Query with aggregation
            let agg_result = rt
                .datafusion()
                .query_builder("SELECT COUNT(*) as count, SUM(amount) as total FROM test_table")
                .build()
                .run()
                .await
                .map_err(|e| e.to_string())?
                .data
                .try_collect::<Vec<_>>()
                .await
                .map_err(|e| e.to_string())?;

            let expected_agg = [
                "+-------+--------+",
                "| count | total  |",
                "+-------+--------+",
                "| 3     | 451.50 |",
                "+-------+--------+",
            ];
            assert_batches_eq!(expected_agg, &agg_result);

            Ok(())
        })
        .await
}

#[tokio::test]
async fn test_adbc_sqlite_prepopulated_data() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    let (db_path, _guard) = temp_sqlite_db("rw");

    setup_sqlite_table(
        &db_path,
        "rw_table",
        "CREATE TABLE rw_table (key INTEGER PRIMARY KEY, value TEXT);
         INSERT INTO rw_table VALUES (1, 'one'), (2, 'two');",
    );

    test_request_context()
        .scope(async {
            let app = AppBuilder::new("adbc_rw_test")
                .with_dataset(make_adbc_sqlite_dataset("rw_table", "rw_table", &db_path))
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err("Timed out waiting for datasets to load".to_string());
                }
                () = Arc::new(rt.clone()).load_components() => {}
            }

            runtime_ready_check(&rt).await;

            // Read back pre-populated data
            let select_result = rt
                .datafusion()
                .query_builder("SELECT * FROM rw_table ORDER BY key")
                .build()
                .run()
                .await
                .map_err(|e| e.to_string())?
                .data
                .try_collect::<Vec<_>>()
                .await
                .map_err(|e| e.to_string())?;

            let expected_select = [
                "+-----+-------+",
                "| key | value |",
                "+-----+-------+",
                "| 1   | one   |",
                "| 2   | two   |",
                "+-----+-------+",
            ];
            assert_batches_eq!(expected_select, &select_result);

            // Test filter pushdown
            let filter_result = rt
                .datafusion()
                .query_builder("SELECT * FROM rw_table WHERE key = 1")
                .build()
                .run()
                .await
                .map_err(|e| e.to_string())?
                .data
                .try_collect::<Vec<_>>()
                .await
                .map_err(|e| e.to_string())?;

            let expected_filter = [
                "+-----+-------+",
                "| key | value |",
                "+-----+-------+",
                "| 1   | one   |",
                "+-----+-------+",
            ];
            assert_batches_eq!(expected_filter, &filter_result);

            Ok(())
        })
        .await
}

#[tokio::test]
async fn test_adbc_connection_options() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    let (db_path, _guard) = temp_sqlite_db("options");

    setup_sqlite_table(
        &db_path,
        "options_test",
        "CREATE TABLE options_test (id INTEGER);",
    );

    test_request_context()
        .scope(async {
            let mut params = HashMap::new();
            params.insert("adbc_driver".to_string(), "sqlite".to_string());
            params.insert("adbc_uri".to_string(), db_path.clone());
            params.insert("connection_pool_size".to_string(), "3".to_string());
            params.insert("connection_pool_min_idle".to_string(), "1".to_string());

            let mut dataset =
                Dataset::new("adbc:options_test".to_string(), "options_test".to_string());
            dataset.params = Some(Params::from_string_map(params));

            let app = AppBuilder::new("adbc_options_test")
                .with_dataset(dataset)
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err("Timed out waiting for datasets to load".to_string());
                }
                () = Arc::new(rt.clone()).load_components() => {}
            }

            runtime_ready_check(&rt).await;

            // Simple connectivity test
            let result = rt
                .datafusion()
                .query_builder("SELECT 1 as test")
                .build()
                .run()
                .await
                .map_err(|e| e.to_string())?
                .data
                .try_collect::<Vec<_>>()
                .await
                .map_err(|e| e.to_string())?;

            let expected = ["+------+", "| test |", "+------+", "| 1    |", "+------+"];
            assert_batches_eq!(expected, &result);

            Ok(())
        })
        .await
}
