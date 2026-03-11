/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

//! Integration tests for hash index functionality with Arrow/MemTable accelerator.
//!
//! These tests verify that the hash index feature works correctly when enabled
//! on Arrow-accelerated datasets with primary keys.

use app::AppBuilder;
use arrow::array::RecordBatch;
use datafusion::assert_batches_eq;
use futures::TryStreamExt;
use runtime::Runtime;
use spicepod::{
    acceleration::{Acceleration, Mode, RefreshMode},
    component::dataset::Dataset,
    param::Params,
};
use std::collections::HashMap;
use std::sync::Arc;

use crate::{
    configure_test_datafusion, init_tracing,
    utils::{runtime_ready_check, test_request_context},
};

const TEST_CSV: &str = "id,name,age,city,score
1,John Doe,28,New York,85
2,Jane Smith,34,Los Angeles,92
3,Mike Johnson,45,Chicago,78
4,Emily Brown,31,Houston,89
5,David Lee,39,Phoenix,76
";

/// Test that verifies hash index works with Arrow accelerator and primary key.
///
/// This test:
/// 1. Creates a dataset with `hash_index: enabled` and a primary key
/// 2. Loads data from a CSV file
/// 3. Verifies all data is queryable
/// 4. Verifies point lookups by primary key work correctly
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_hash_index_arrow_accelerator() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            // Write the CSV to a file
            let csv_path = std::env::temp_dir().join("test_hash_index.csv");
            std::fs::write(&csv_path, TEST_CSV)?;

            // Create dataset with hash_index enabled and primary key
            let mut dataset =
                Dataset::new(format!("file://{}", csv_path.display()), "hash_index_test");

            let mut params = HashMap::new();
            params.insert("hash_index".to_string(), "enabled".to_string());

            dataset.acceleration = Some(Acceleration {
                params: Some(Params::from_string_map(params)),
                enabled: true,
                engine: None, // Uses Arrow/MemTable by default
                mode: Mode::Memory,
                refresh_mode: Some(RefreshMode::Full),
                primary_key: Some("id".to_string()),
                ..Acceleration::default()
            });

            let app = AppBuilder::new("test_hash_index")
                .with_dataset(dataset)
                .build();

            configure_test_datafusion();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(30)) => {
                    return Err(anyhow::Error::msg("Timed out waiting for datasets to load"));
                }
                () = Arc::clone(&rt).load_components() => {}
            }

            runtime_ready_check(&rt).await;

            // Query all data to verify it loaded correctly
            let result: Vec<RecordBatch> = rt
                .datafusion()
                .query_builder("SELECT * FROM hash_index_test ORDER BY id")
                .build()
                .run()
                .await?
                .data
                .try_collect()
                .await?;

            let expected = [
                "+----+--------------+-----+-------------+-------+",
                "| id | name         | age | city        | score |",
                "+----+--------------+-----+-------------+-------+",
                "| 1  | John Doe     | 28  | New York    | 85    |",
                "| 2  | Jane Smith   | 34  | Los Angeles | 92    |",
                "| 3  | Mike Johnson | 45  | Chicago     | 78    |",
                "| 4  | Emily Brown  | 31  | Houston     | 89    |",
                "| 5  | David Lee    | 39  | Phoenix     | 76    |",
                "+----+--------------+-----+-------------+-------+",
            ];
            assert_batches_eq!(&expected, &result);

            // Test point lookup by primary key (exercises hash index)
            let result: Vec<RecordBatch> = rt
                .datafusion()
                .query_builder("SELECT name, city FROM hash_index_test WHERE id = 3")
                .build()
                .run()
                .await?
                .data
                .try_collect()
                .await?;

            let expected = [
                "+--------------+---------+",
                "| name         | city    |",
                "+--------------+---------+",
                "| Mike Johnson | Chicago |",
                "+--------------+---------+",
            ];
            assert_batches_eq!(&expected, &result);

            // Test point lookup for non-existent key
            let result: Vec<RecordBatch> = rt
                .datafusion()
                .query_builder("SELECT * FROM hash_index_test WHERE id = 999")
                .build()
                .run()
                .await?
                .data
                .try_collect()
                .await?;

            assert!(result.is_empty() || result.iter().all(|b| b.num_rows() == 0));

            // Clean up
            drop(rt);
            std::fs::remove_file(&csv_path)?;

            Ok(())
        })
        .await
}

/// Test that hash index requires a primary key.
///
/// When `hash_index: enabled` is specified but no primary key is defined,
/// the dataset should fail to load with an appropriate error.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_hash_index_requires_primary_key() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            // Write the CSV to a file
            let csv_path = std::env::temp_dir().join("test_hash_index_no_pk.csv");
            std::fs::write(&csv_path, TEST_CSV)?;

            // Create dataset with hash_index enabled but NO primary key
            let mut dataset = Dataset::new(
                format!("file://{}", csv_path.display()),
                "hash_index_no_pk_test",
            );

            let mut params = HashMap::new();
            params.insert("hash_index".to_string(), "enabled".to_string());

            dataset.acceleration = Some(Acceleration {
                params: Some(Params::from_string_map(params)),
                enabled: true,
                engine: None,
                mode: Mode::Memory,
                refresh_mode: Some(RefreshMode::Full),
                primary_key: None, // No primary key - should cause error
                ..Acceleration::default()
            });

            let app = AppBuilder::new("test_hash_index_no_pk")
                .with_dataset(dataset)
                .build();

            configure_test_datafusion();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);

            // Give some time for component loading to process
            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {}
                () = Arc::clone(&rt).load_components() => {}
            }

            // The dataset should have failed to load - verify by trying to query it
            let result = rt
                .datafusion()
                .query_builder("SELECT * FROM hash_index_no_pk_test")
                .build()
                .run()
                .await;

            // The table should not exist or the query should fail
            assert!(
                result.is_err(),
                "Expected query to fail because hash_index requires primary_key"
            );

            // Clean up
            drop(rt);
            std::fs::remove_file(&csv_path)?;

            Ok(())
        })
        .await
}
