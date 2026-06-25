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
use std::time::Duration;

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

/// Pre-create a table in the `SQLite` database so the ADBC connector can
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
                () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
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
                () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
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
                () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
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
                () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
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

#[tokio::test]
async fn test_adbc_sqlite_schema_inference() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    let (db_path, _guard) = temp_sqlite_db("schema_inference");

    setup_sqlite_table(
        &db_path,
        "diverse_types",
        "CREATE TABLE diverse_types (
            int_col INTEGER,
            text_col TEXT,
            real_col REAL,
            blob_col BLOB,
            numeric_col NUMERIC
        );
         INSERT INTO diverse_types VALUES (42, 'hello', 3.14, X'DEADBEEF', 99.9);",
    );

    test_request_context()
        .scope(async {
            let app = AppBuilder::new("adbc_schema_inference_test")
                .with_dataset(make_adbc_sqlite_dataset(
                    "diverse_types",
                    "diverse_types",
                    &db_path,
                ))
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;

            tokio::select! {
                () = tokio::time::sleep(Duration::from_mins(1)) => {
                    return Err("Timed out waiting for datasets to load".to_string());
                }
                () = Arc::new(rt.clone()).load_components() => {}
            }

            runtime_ready_check(&rt).await;

            let result = rt
                .datafusion()
                .query_builder(
                    "SELECT column_name, data_type FROM information_schema.columns \
                     WHERE table_name = 'diverse_types' ORDER BY ordinal_position",
                )
                .build()
                .run()
                .await
                .map_err(|e| e.to_string())?
                .data
                .try_collect::<Vec<_>>()
                .await
                .map_err(|e| e.to_string())?;

            let expected = [
                "+-------------+-----------+",
                "| column_name | data_type |",
                "+-------------+-----------+",
                "| int_col     | Int64     |",
                "| text_col    | Utf8      |",
                "| real_col    | Float64   |",
                "| blob_col    | Binary    |",
                "| numeric_col | Float64   |",
                "+-------------+-----------+",
            ];
            assert_batches_eq!(expected, &result);

            Ok(())
        })
        .await
}

#[tokio::test]
async fn test_adbc_sqlite_empty_table_schema() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    let (db_path, _guard) = temp_sqlite_db("empty_schema");

    setup_sqlite_table(
        &db_path,
        "empty_table",
        "CREATE TABLE empty_table (id INTEGER, name TEXT, value REAL);",
    );

    test_request_context()
        .scope(async {
            let app = AppBuilder::new("adbc_empty_schema_test")
                .with_dataset(make_adbc_sqlite_dataset(
                    "empty_table",
                    "empty_table",
                    &db_path,
                ))
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;

            tokio::select! {
                () = tokio::time::sleep(Duration::from_mins(1)) => {
                    return Err("Timed out waiting for datasets to load".to_string());
                }
                () = Arc::new(rt.clone()).load_components() => {}
            }

            runtime_ready_check(&rt).await;

            // Schema should be discovered even with no rows
            let schema_result = rt
                .datafusion()
                .query_builder(
                    "SELECT column_name, data_type FROM information_schema.columns \
                     WHERE table_name = 'empty_table' ORDER BY ordinal_position",
                )
                .build()
                .run()
                .await
                .map_err(|e| e.to_string())?
                .data
                .try_collect::<Vec<_>>()
                .await
                .map_err(|e| e.to_string())?;

            // SQLite uses dynamic typing; for empty tables the ADBC driver
            // cannot infer column types from data and falls back to Int64.
            let expected_schema = [
                "+-------------+-----------+",
                "| column_name | data_type |",
                "+-------------+-----------+",
                "| id          | Int64     |",
                "| name        | Int64     |",
                "| value       | Int64     |",
                "+-------------+-----------+",
            ];
            assert_batches_eq!(expected_schema, &schema_result);

            // Query should return 0 rows
            let data_result = rt
                .datafusion()
                .query_builder("SELECT * FROM empty_table")
                .build()
                .run()
                .await
                .map_err(|e| e.to_string())?
                .data
                .try_collect::<Vec<_>>()
                .await
                .map_err(|e| e.to_string())?;

            let total_rows: usize = data_result
                .iter()
                .map(datafusion::arrow::array::RecordBatch::num_rows)
                .sum();
            assert_eq!(total_rows, 0, "Expected 0 rows from empty table");
            if let Some(batch) = data_result.first() {
                assert_eq!(batch.num_columns(), 3, "Expected 3 columns");
            }

            Ok(())
        })
        .await
}

#[tokio::test]
async fn test_adbc_sqlite_dataset_registration_in_information_schema() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    let (db_path, _guard) = temp_sqlite_db("info_schema");

    setup_sqlite_table(
        &db_path,
        "table_alpha",
        "CREATE TABLE table_alpha (id INTEGER, label TEXT);",
    );
    setup_sqlite_table(
        &db_path,
        "table_beta",
        "CREATE TABLE table_beta (key INTEGER, value REAL);",
    );

    test_request_context()
        .scope(async {
            let app = AppBuilder::new("adbc_info_schema_test")
                .with_dataset(make_adbc_sqlite_dataset(
                    "table_alpha",
                    "table_alpha",
                    &db_path,
                ))
                .with_dataset(make_adbc_sqlite_dataset(
                    "table_beta",
                    "table_beta",
                    &db_path,
                ))
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;

            tokio::select! {
                () = tokio::time::sleep(Duration::from_mins(1)) => {
                    return Err("Timed out waiting for datasets to load".to_string());
                }
                () = Arc::new(rt.clone()).load_components() => {}
            }

            runtime_ready_check(&rt).await;

            let result = rt
                .datafusion()
                .query_builder(
                    "SELECT table_name FROM information_schema.tables \
                     WHERE table_name IN ('table_alpha', 'table_beta') \
                     ORDER BY table_name",
                )
                .build()
                .run()
                .await
                .map_err(|e| e.to_string())?
                .data
                .try_collect::<Vec<_>>()
                .await
                .map_err(|e| e.to_string())?;

            let expected = [
                "+-------------+",
                "| table_name  |",
                "+-------------+",
                "| table_alpha |",
                "| table_beta  |",
                "+-------------+",
            ];
            assert_batches_eq!(expected, &result);

            Ok(())
        })
        .await
}

#[tokio::test]
async fn test_adbc_sqlite_missing_table_error() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    let (db_path, _guard) = temp_sqlite_db("missing_table");

    // Create the database file but NOT the table the dataset references
    setup_sqlite_table(
        &db_path,
        "other_table",
        "CREATE TABLE other_table (id INTEGER);",
    );

    test_request_context()
        .scope(async {
            let app = AppBuilder::new("adbc_missing_table_test")
                .with_dataset(make_adbc_sqlite_dataset(
                    "nonexistent_table",
                    "nonexistent_table",
                    &db_path,
                ))
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;

            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(10)) => {}
                () = Arc::new(rt.clone()).load_components() => {}
            }

            // The dataset should not become ready because the table doesn't exist
            assert!(
                !rt.status().is_ready(),
                "Runtime should not be ready when a dataset references a nonexistent table"
            );

            Ok(())
        })
        .await
}

#[tokio::test]
async fn test_adbc_sqlite_in_memory_rejected() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let app = AppBuilder::new("adbc_in_memory_test")
                .with_dataset(make_adbc_sqlite_dataset(
                    "memory_table",
                    "memory_table",
                    ":memory:",
                ))
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;

            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(10)) => {}
                () = Arc::new(rt.clone()).load_components() => {}
            }

            // In-memory URIs should be rejected — the dataset should not be ready
            assert!(
                !rt.status().is_ready(),
                "Runtime should not be ready when using an in-memory URI"
            );

            Ok(())
        })
        .await
}

#[tokio::test]
async fn test_adbc_sqlite_missing_driver_param() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            // Create a dataset without the required adbc_driver parameter
            let mut params = HashMap::new();
            params.insert("adbc_uri".to_string(), "/tmp/test.db".to_string());

            let mut dataset = Dataset::new("adbc:some_table".to_string(), "some_table".to_string());
            dataset.params = Some(Params::from_string_map(params));

            let app = AppBuilder::new("adbc_missing_driver_test")
                .with_dataset(dataset)
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;

            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(10)) => {}
                () = Arc::new(rt.clone()).load_components() => {}
            }

            assert!(
                !rt.status().is_ready(),
                "Runtime should not be ready when adbc_driver param is missing"
            );

            Ok(())
        })
        .await
}

#[tokio::test]
async fn test_adbc_invalid_driver_name() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let mut params = HashMap::new();
            params.insert(
                "adbc_driver".to_string(),
                "nonexistent_driver_xyz".to_string(),
            );
            params.insert("adbc_uri".to_string(), "/tmp/test.db".to_string());

            let mut dataset = Dataset::new("adbc:some_table".to_string(), "some_table".to_string());
            dataset.params = Some(Params::from_string_map(params));

            let app = AppBuilder::new("adbc_invalid_driver_test")
                .with_dataset(dataset)
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;

            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(10)) => {}
                () = Arc::new(rt.clone()).load_components() => {}
            }

            assert!(
                !rt.status().is_ready(),
                "Runtime should not be ready when ADBC driver name is invalid"
            );

            Ok(())
        })
        .await
}

#[tokio::test]
async fn test_adbc_invalid_uri() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let app = AppBuilder::new("adbc_invalid_uri_test")
                .with_dataset(make_adbc_sqlite_dataset(
                    "some_table",
                    "some_table",
                    "/nonexistent/path/to/nowhere/db.sqlite",
                ))
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;

            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(10)) => {}
                () = Arc::new(rt.clone()).load_components() => {}
            }

            assert!(
                !rt.status().is_ready(),
                "Runtime should not be ready when ADBC URI points to a nonexistent path"
            );

            Ok(())
        })
        .await
}
