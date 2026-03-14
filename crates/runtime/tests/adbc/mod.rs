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
    init_tracing,
    utils::{runtime_ready_check, test_request_context},
};
use app::AppBuilder;
use datafusion::assert_batches_eq;
use futures::TryStreamExt;
use spicepod::component::dataset::Dataset;
use std::collections::HashMap;

fn make_adbc_sqlite_dataset(ds_name: &str, table: &str, uri: &str) -> Dataset {
    let mut params = HashMap::new();
    params.insert("adbc_driver".to_string(), "sqlite".to_string());
    params.insert("adbc_uri".to_string(), uri.to_string());
    params.insert("connection_pool_size".to_string(), "1".to_string());

    let mut dataset = Dataset::new(format!("adbc:{table}"), ds_name.to_string());
    dataset.params = params;
    dataset
}

#[tokio::test]
async fn test_adbc_sqlite_in_memory() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let app = AppBuilder::new("adbc_sqlite_test")
                .with_dataset(make_adbc_sqlite_dataset(
                    "test_table",
                    "test_table",
                    ":memory:",
                ))
                .build();

            let status = runtime_ready_check(app).await;
            let rt = status.runtime;

            // Create test table
            rt.datafusion()
                .query_builder(
                    "CREATE TABLE test_table (id INTEGER, name TEXT, value DOUBLE)",
                )
                .build()
                .run()
                .await
                .map_err(|e| format!("Failed to create table: {e}"))?;

            // Insert test data
            rt.datafusion()
                .query_builder(
                    "INSERT INTO test_table VALUES (1, 'alice', 10.5), (2, 'bob', 20.3), (3, 'charlie', 15.7)",
                )
                .build()
                .run()
                .await
                .map_err(|e| format!("Failed to insert data: {e}"))?;

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
#[ignore] // Requires ADBC DuckDB driver to be installed
async fn test_adbc_duckdb_in_memory() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let mut params = HashMap::new();
            params.insert("adbc_driver".to_string(), "duckdb".to_string());
            params.insert("adbc_uri".to_string(), ":memory:".to_string());
            params.insert("connection_pool_size".to_string(), "1".to_string());

            let mut dataset = Dataset::new("adbc:test_table".to_string(), "test_table".to_string());
            dataset.params = params;

            let app = AppBuilder::new("adbc_duckdb_test")
                .with_dataset(dataset)
                .build();

            let status = runtime_ready_check(app).await;
            let rt = status.runtime;

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

    test_request_context()
        .scope(async {
            let app = AppBuilder::new("adbc_rw_test")
                .with_dataset(make_adbc_sqlite_dataset("rw_table", "rw_table", ":memory:"))
                .build();

            let status = runtime_ready_check(app).await;
            let rt = status.runtime;

            // Create table
            rt.datafusion()
                .query_builder("CREATE TABLE rw_table (key INTEGER PRIMARY KEY, value TEXT)")
                .build()
                .run()
                .await
                .map_err(|e| e.to_string())?;

            // Test INSERT
            rt.datafusion()
                .query_builder("INSERT INTO rw_table VALUES (1, 'one'), (2, 'two')")
                .build()
                .run()
                .await
                .map_err(|e| e.to_string())?;

            // Test SELECT
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

            // Test UPDATE
            rt.datafusion()
                .query_builder("UPDATE rw_table SET value = 'updated' WHERE key = 1")
                .build()
                .run()
                .await
                .map_err(|e| e.to_string())?;

            // Verify UPDATE
            let update_result = rt
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

            let expected_update = [
                "+-----+---------+",
                "| key | value   |",
                "+-----+---------+",
                "| 1   | updated |",
                "+-----+---------+",
            ];
            assert_batches_eq!(expected_update, &update_result);

            Ok(())
        })
        .await
}

#[tokio::test]
async fn test_adbc_connection_options() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            // Test with only declared/supported connection options
            let mut params = HashMap::new();
            params.insert("adbc_driver".to_string(), "sqlite".to_string());
            params.insert("adbc_uri".to_string(), ":memory:".to_string());
            params.insert("connection_pool_size".to_string(), "3".to_string());
            params.insert("connection_pool_min_idle".to_string(), "1".to_string());

            let mut dataset =
                Dataset::new("adbc:options_test".to_string(), "options_test".to_string());
            dataset.params = params;

            let app = AppBuilder::new("adbc_options_test")
                .with_dataset(dataset)
                .build();

            let status = runtime_ready_check(app).await;
            let rt = status.runtime;

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
