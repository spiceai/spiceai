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
use adbc_core::options::{AdbcVersion, OptionDatabase, OptionValue};
use adbc_core::{Connection as _, Database as _, Driver as _, LOAD_FLAG_DEFAULT, Statement as _};
use adbc_driver_manager::ManagedDriver;
use app::AppBuilder;
use datafusion::assert_batches_eq;
use futures::TryStreamExt;
use runtime::Runtime;
use spicepod::component::access::AccessMode;
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

fn temp_sqlite_uri(name: &str) -> String {
    let dir = std::env::temp_dir().join(format!("spice_adbc_test_{name}_{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("Failed to create temp directory for ADBC test");
    dir.join("test.db").to_string_lossy().to_string()
}

/// Pre-creates a SQLite database file and executes the given SQL statements via the ADBC
/// driver. Must be called before building the [`Runtime`] so that `load_components()` can
/// successfully introspect the table schema via `get_schema()`.
///
/// The root cause of the test failures is that `AdbcTableFactory::table_provider()` calls
/// `get_schema()` on the ADBC connection during `load_components()`. If the table does not
/// yet exist in the SQLite file, `get_schema()` returns an error and the dataset load retry
/// loop runs indefinitely until the 60-second test timeout fires.
async fn setup_sqlite_via_adbc(uri: &str, sql_statements: &[&str]) {
    let uri_owned = uri.to_string();
    let stmts: Vec<String> = sql_statements.iter().map(|s| (*s).to_string()).collect();

    tokio::task::spawn_blocking(move || {
        let mut driver = ManagedDriver::load_from_name(
            "sqlite",
            None,
            AdbcVersion::V110,
            LOAD_FLAG_DEFAULT,
            None,
        )
        .expect("Failed to load SQLite ADBC driver for test setup");

        let db = driver
            .new_database_with_opts([(OptionDatabase::Uri, OptionValue::String(uri_owned))])
            .expect("Failed to open SQLite ADBC database for test setup");

        let mut conn = db
            .new_connection()
            .expect("Failed to create ADBC connection for test setup");

        for sql in &stmts {
            let mut stmt = conn
                .new_statement()
                .expect("Failed to create ADBC statement for test setup");
            stmt.set_sql_query(sql.as_str())
                .expect("Failed to set SQL query for test setup");
            let _ = stmt
                .execute_update()
                .expect("Failed to execute SQL for test setup");
        }
    })
    .await
    .expect("ADBC test setup task panicked");
}

#[tokio::test]
async fn test_adbc_sqlite_file_backed() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    let db_path = temp_sqlite_uri("basic");

    // Pre-create and populate the table so that load_components() can read its schema.
    // Without this, get_schema() fails with "no such table" and the dataset load retry
    // loop runs indefinitely, causing the 60-second timeout.
    setup_sqlite_via_adbc(
        &db_path,
        &[
            "CREATE TABLE test_table (id INTEGER, name TEXT, value DOUBLE)",
            "INSERT INTO test_table VALUES (1, 'alice', 10.5), (2, 'bob', 20.3), (3, 'charlie', 15.7)",
        ],
    )
    .await;

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

            // Query the pre-populated data
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
    let db_path = temp_sqlite_uri("duckdb");

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
async fn test_adbc_read_write_operations() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    let db_path = temp_sqlite_uri("rw");

    // Pre-create the (empty) table so load_components() can introspect its schema.
    // The AccessMode::ReadWrite dataset registration calls read_write_provider() which
    // also calls get_schema(), so the table must exist before the runtime starts.
    setup_sqlite_via_adbc(
        &db_path,
        &["CREATE TABLE rw_table (key INTEGER PRIMARY KEY, value TEXT)"],
    )
    .await;

    test_request_context()
        .scope(async {
            // Use ReadWrite access mode so DataFusion INSERT statements are routed
            // through the ADBCTableWriter -> AdbcDataSink -> bulk_insert path.
            let mut dataset = make_adbc_sqlite_dataset("rw_table", "rw_table", &db_path);
            dataset.access = AccessMode::ReadWrite;

            let app = AppBuilder::new("adbc_rw_test")
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

            // Test INSERT
            rt.datafusion()
                .query_builder("INSERT INTO rw_table VALUES (1, 'one'), (2, 'two')")
                .build()
                .run()
                .await
                .map_err(|e| e.to_string())?;

            // Test SELECT after first INSERT
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

            // Test a second INSERT to verify append semantics
            rt.datafusion()
                .query_builder("INSERT INTO rw_table VALUES (3, 'three')")
                .build()
                .run()
                .await
                .map_err(|e| e.to_string())?;

            // Verify all three rows are present
            let appended_result = rt
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

            let expected_appended = [
                "+-----+-------+",
                "| key | value |",
                "+-----+-------+",
                "| 1   | one   |",
                "| 2   | two   |",
                "| 3   | three |",
                "+-----+-------+",
            ];
            assert_batches_eq!(expected_appended, &appended_result);

            Ok(())
        })
        .await
}

#[tokio::test]
async fn test_adbc_connection_options() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    let db_path = temp_sqlite_uri("options");

    // Pre-create the table so load_components() can introspect its schema.
    setup_sqlite_via_adbc(&db_path, &["CREATE TABLE options_test (id INTEGER)"]).await;

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

            // Simple connectivity test — verifies the runtime is responsive and the
            // connection pool with custom options was successfully created.
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
