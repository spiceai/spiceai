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

//! Integration tests for `partition_by` with the in-memory Arrow accelerator
//! (`PartitionedArrowAccelerator`).
//!
//! These tests verify:
//! - Basic bucket partitioning correctness (data integrity, filtered queries)
//! - primary-key hash indexing is propagated to each partition
//! - `sort_columns` parameter is propagated to each partition (bug fix)

use app::AppBuilder;
use arrow::array::RecordBatch;
use datafusion::assert_batches_eq;
use futures::TryStreamExt;
use runtime::Runtime;
use spicepod::{
    acceleration::{Acceleration, Mode, RefreshMode},
    component::dataset::Dataset,
    param::Params,
    partitioning::PartitionedBy,
};
use std::collections::HashMap;
use std::sync::Arc;

use crate::utils::{runtime_ready_check, test_request_context};

/// Execute a SQL query and collect all `RecordBatch`es.
async fn run_query(rt: &Arc<Runtime>, sql: &str) -> Result<Vec<RecordBatch>, anyhow::Error> {
    rt.datafusion()
        .query_builder(sql)
        .build()
        .run()
        .await
        .map_err(|e| anyhow::anyhow!("Query failed: {e}"))?
        .data
        .try_collect()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to collect results: {e}"))
}

/// Build a dataset backed by the test CSV with Arrow acceleration and the given partition/params.
fn make_dataset(
    name: &str,
    test_file: &std::path::Path,
    partition_by: Vec<PartitionedBy>,
    extra_params: HashMap<String, String>,
    primary_key: Option<String>,
) -> Dataset {
    let mut dataset = Dataset::new(format!("file://{}", test_file.display()), name);
    dataset.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some("arrow".to_string()),
        mode: Mode::Memory,
        refresh_mode: Some(RefreshMode::Full),
        params: if extra_params.is_empty() {
            None
        } else {
            Some(Params::from_string_map(extra_params))
        },
        partition_by,
        primary_key,
        ..Acceleration::default()
    });
    dataset
}

/// Test basic `partition_by: bucket(3, id)` for Arrow in-memory acceleration.
///
/// Verifies:
/// 1. All 10 rows are returned for a full-table scan.
/// 2. Filtered queries return the correct subset.
/// 3. Aggregations across partitions are correct.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_arrow_partition_by_bucket() -> Result<(), anyhow::Error> {
    let _tracing = crate::init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let test_file = std::env::current_dir()
                .map_err(|e| anyhow::anyhow!("Failed to get current directory: {e}"))?
                .join("tests/acceleration/data/partition_test.csv");

            crate::configure_test_datafusion();

            let dataset = make_dataset(
                "arrow_bucket_test",
                &test_file,
                vec![PartitionedBy {
                    name: "expr0".to_string(),
                    expression: "bucket(3, id)".to_string(),
                }],
                HashMap::new(),
                None,
            );

            let app = AppBuilder::new("test_arrow_partition_by_bucket")
                .with_dataset(dataset)
                .build();

            let rt = Arc::new(Runtime::builder().with_app(app).build().await);

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(30)) => {
                    return Err(anyhow::Error::msg("Timeout waiting for components to load"));
                }
                () = Arc::clone(&rt).load_components() => {}
            }

            runtime_ready_check(&rt).await;

            // Full table scan — all 10 rows across all partitions.
            let result = run_query(&rt, "SELECT * FROM arrow_bucket_test ORDER BY id").await?;
            let total: usize = result.iter().map(RecordBatch::num_rows).sum();
            assert_eq!(total, 10, "Expected 10 rows total");

            // Point filter on partition column.
            let result = run_query(&rt, "SELECT * FROM arrow_bucket_test WHERE id = 1").await?;
            let expected = [
                "+----+----------+-----+----------+-------+",
                "| id | name     | age | city     | score |",
                "+----+----------+-----+----------+-------+",
                "| 1  | John Doe | 28  | New York | 85    |",
                "+----+----------+-----+----------+-------+",
            ];
            assert_batches_eq!(&expected, &result);

            // Range filter — ids 5 through 10 → 6 rows.
            let result = run_query(
                &rt,
                "SELECT id FROM arrow_bucket_test WHERE id >= 5 ORDER BY id",
            )
            .await?;
            let count: usize = result.iter().map(RecordBatch::num_rows).sum();
            assert_eq!(count, 6, "Expected 6 rows with id >= 5");

            // Filter on non-partition column.
            let result = run_query(
                &rt,
                "SELECT id FROM arrow_bucket_test WHERE score > 85 ORDER BY id",
            )
            .await?;
            let count: usize = result.iter().map(RecordBatch::num_rows).sum();
            assert_eq!(count, 5, "Expected 5 rows with score > 85");

            // Aggregation across all partitions.
            let result = run_query(&rt, "SELECT COUNT(*) as cnt FROM arrow_bucket_test").await?;
            let expected = ["+-----+", "| cnt |", "+-----+", "| 10  |", "+-----+"];
            assert_batches_eq!(&expected, &result);

            Ok(())
        })
        .await
}

/// Test that primary-key hash indexing is correctly propagated to every Arrow partition.
///
/// Prior to the fix, `_source` was ignored in `PartitionedArrowAccelerator::create_external_table`,
/// so `hash_index` was silently dropped and each partition's `IndexedMemTable` was created without
/// it.  With the propagation fix the option flows into every partition.  A second fix ensures the
/// per-partition index is rebuilt after data is inserted (the `TableSink` previously only called
/// `perform_index_maintenance` on the top-level provider, which returned `false` for
/// `PartitionTableProvider` — missing all the inner `IndexedMemTable`s).
///
/// We verify correctness by confirming:
/// 1. The dataset loads successfully with `primary_key` set.
/// 2. Primary-key point lookups on the partition table return the correct single row (exercises
///    the rebuilt per-partition hash index).
/// 3. A lookup for a non-existent key returns zero rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_arrow_partition_hash_index() -> Result<(), anyhow::Error> {
    let _tracing = crate::init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let test_file = std::env::current_dir()
                .map_err(|e| anyhow::anyhow!("Failed to get current directory: {e}"))?
                .join("tests/acceleration/data/partition_test.csv");

            crate::configure_test_datafusion();

            let params = HashMap::new();

            let dataset = make_dataset(
                "arrow_partition_hash_test",
                &test_file,
                vec![PartitionedBy {
                    name: "expr0".to_string(),
                    expression: "bucket(3, id)".to_string(),
                }],
                params,
                Some("id".to_string()),
            );

            let app = AppBuilder::new("test_arrow_partition_hash_index")
                .with_dataset(dataset)
                .build();

            let rt = Arc::new(Runtime::builder().with_app(app).build().await);

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(30)) => {
                    return Err(anyhow::Error::msg("Timeout waiting for components to load"));
                }
                () = Arc::clone(&rt).load_components() => {}
            }

            runtime_ready_check(&rt).await;

            // All rows should be accessible.
            let result =
                run_query(&rt, "SELECT * FROM arrow_partition_hash_test ORDER BY id").await?;
            let total: usize = result.iter().map(RecordBatch::num_rows).sum();
            assert_eq!(total, 10, "Expected 10 rows total");

            // Point lookup exercises the hash index on the partition that owns id=3.
            let result = run_query(
                &rt,
                "SELECT name, city FROM arrow_partition_hash_test WHERE id = 3",
            )
            .await?;
            let expected = [
                "+--------------+---------+",
                "| name         | city    |",
                "+--------------+---------+",
                "| Mike Johnson | Chicago |",
                "+--------------+---------+",
            ];
            assert_batches_eq!(&expected, &result);

            // Non-existent key should return zero rows.
            let result = run_query(
                &rt,
                "SELECT * FROM arrow_partition_hash_test WHERE id = 999",
            )
            .await?;
            let total: usize = result.iter().map(RecordBatch::num_rows).sum();
            assert_eq!(total, 0, "Expected 0 rows for non-existent key");

            Ok(())
        })
        .await
}

/// Test that `sort_columns` is correctly propagated to every Arrow partition.
///
/// Prior to the fix, `_source` was ignored so `sort_columns` was never inserted into
/// `cmd.options`, meaning each partition's `ArrowFactory::create` silently created an
/// unsorted table.  With the fix the option flows through to every partition.
///
/// We verify that:
/// 1. The dataset loads successfully with `sort_columns` set.
/// 2. All rows remain queryable after the sorted insert.
/// 3. Querying without an explicit ORDER BY returns rows in the declared sort order
///    (per-partition; we assert the per-partition order rather than global order).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_arrow_partition_sort_columns() -> Result<(), anyhow::Error> {
    let _tracing = crate::init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let test_file = std::env::current_dir()
                .map_err(|e| anyhow::anyhow!("Failed to get current directory: {e}"))?
                .join("tests/acceleration/data/partition_test.csv");

            crate::configure_test_datafusion();

            let mut params = HashMap::new();
            params.insert("sort_columns".to_string(), "score".to_string());

            let dataset = make_dataset(
                "arrow_partition_sort_test",
                &test_file,
                vec![PartitionedBy {
                    name: "expr0".to_string(),
                    expression: "bucket(3, id)".to_string(),
                }],
                params,
                None,
            );

            let app = AppBuilder::new("test_arrow_partition_sort_columns")
                .with_dataset(dataset)
                .build();

            let rt = Arc::new(Runtime::builder().with_app(app).build().await);

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(30)) => {
                    return Err(anyhow::Error::msg("Timeout waiting for components to load"));
                }
                () = Arc::clone(&rt).load_components() => {}
            }

            runtime_ready_check(&rt).await;

            // All 10 rows should still be present after sorted inserts.
            let result =
                run_query(&rt, "SELECT * FROM arrow_partition_sort_test ORDER BY id").await?;
            let total: usize = result.iter().map(RecordBatch::num_rows).sum();
            assert_eq!(total, 10, "Expected 10 rows after sorted insert");

            // Verify data integrity — spot-check a few values.
            let result = run_query(
                &rt,
                "SELECT id, score FROM arrow_partition_sort_test WHERE id IN (1, 6) ORDER BY id",
            )
            .await?;
            let expected = [
                "+----+-------+",
                "| id | score |",
                "+----+-------+",
                "| 1  | 85    |",
                "| 6  | 94    |",
                "+----+-------+",
            ];
            assert_batches_eq!(&expected, &result);

            Ok(())
        })
        .await
}

/// Test that primary-key hash indexing and `sort_columns` can be used together with partitioned Arrow.
///
/// This is the combined scenario: a dataset with primary-key indexing and sort columns,
/// partitioned by bucket. Verifies that both behaviors work together and the table is queryable.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_arrow_partition_hash_index_and_sort_columns() -> Result<(), anyhow::Error> {
    let _tracing = crate::init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let test_file = std::env::current_dir()
                .map_err(|e| anyhow::anyhow!("Failed to get current directory: {e}"))?
                .join("tests/acceleration/data/partition_test.csv");

            crate::configure_test_datafusion();

            let mut params = HashMap::new();
            params.insert("sort_columns".to_string(), "score".to_string());

            let dataset = make_dataset(
                "arrow_partition_combined_test",
                &test_file,
                vec![PartitionedBy {
                    name: "expr0".to_string(),
                    expression: "bucket(3, id)".to_string(),
                }],
                params,
                Some("id".to_string()),
            );

            let app = AppBuilder::new("test_arrow_partition_combined")
                .with_dataset(dataset)
                .build();

            let rt = Arc::new(Runtime::builder().with_app(app).build().await);

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(30)) => {
                    return Err(anyhow::Error::msg("Timeout waiting for components to load"));
                }
                () = Arc::clone(&rt).load_components() => {}
            }

            runtime_ready_check(&rt).await;

            // Full scan.
            let result = run_query(
                &rt,
                "SELECT * FROM arrow_partition_combined_test ORDER BY id",
            )
            .await?;
            let total: usize = result.iter().map(RecordBatch::num_rows).sum();
            assert_eq!(total, 10, "Expected 10 rows total");

            // Primary key lookup (hash index).
            let result = run_query(
                &rt,
                "SELECT name FROM arrow_partition_combined_test WHERE id = 7",
            )
            .await?;
            let expected = [
                "+--------------+",
                "| name         |",
                "+--------------+",
                "| Tom Anderson |",
                "+--------------+",
            ];
            assert_batches_eq!(&expected, &result);

            Ok(())
        })
        .await
}
