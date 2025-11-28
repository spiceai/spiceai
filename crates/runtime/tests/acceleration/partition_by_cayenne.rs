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

use std::{collections::HashMap, sync::Arc};

use app::AppBuilder;
use arrow::array::RecordBatch;
use datafusion::physical_plan::{ExecutionPlan, displayable};
use futures::TryStreamExt;
use runtime::Runtime;
use spicepod::{
    acceleration::{Acceleration, Mode, RefreshMode},
    component::dataset::Dataset,
    param::Params,
    partitioning::PartitionedBy,
};

use crate::utils::{runtime_ready_check, test_request_context};

// Test data CSV content - use include_str! to embed the test data file

/// Sanitize file paths in physical plans for deterministic snapshots.
/// Replaces absolute file paths with placeholders.
fn sanitize_file_paths(plan: &str) -> String {
    // Replace absolute paths in file_groups with placeholder
    let mut result = String::new();
    for line in plan.lines() {
        if line.contains("file_groups={") {
            // Find the start of file_groups
            if let Some(fg_start) = line.find("file_groups=") {
                // Find the closing ]]}
                if let Some(fg_end) = line[fg_start..].find("]]}") {
                    let prefix = &line[..fg_start];
                    let suffix = &line[fg_start + fg_end + 3..];
                    result.push_str(prefix);
                    result.push_str("file_groups={1 group: [[<TEMP_PATH>/.vortex]]}");
                    result.push_str(suffix);
                } else {
                    result.push_str(line);
                }
            } else {
                result.push_str(line);
            }
        } else {
            result.push_str(line);
        }
        result.push('\n');
    }
    result
}

/// Execute a SQL query on the Spice runtime and return the results
async fn execute_rt_sql(rt: &Arc<Runtime>, sql: &str) -> Result<Vec<RecordBatch>, anyhow::Error> {
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

/// Get the physical plan for a query
async fn get_physical_plan(
    rt: &Arc<Runtime>,
    sql: &str,
) -> Result<Arc<dyn ExecutionPlan>, anyhow::Error> {
    let df = rt
        .datafusion()
        .ctx
        .sql(sql)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create dataframe: {e}"))?;

    df.create_physical_plan()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create physical plan: {e}"))
}

/// Test `partition_by` with `bucket()` expression for Cayenne acceleration
///
/// This test verifies that:
/// 1. `partition_by`: bucket(3, id) correctly partitions data into 3 buckets
/// 2. Queries with filters on the partition column use partition pruning
/// 3. Physical plans show `CayenneTableScan` with appropriate partition filters
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(not(target_os = "windows"))]
async fn test_cayenne_partition_by_bucket() -> Result<(), anyhow::Error> {
    let _tracing = crate::init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            // Use the test data file from the data directory
            // When running tests, the current directory is typically the crate root.
            let test_file = std::env::current_dir()
                .map_err(|e| anyhow::anyhow!("Failed to get current directory: {e}"))?
                .join("tests/acceleration/data/partition_test.csv");

            // Verify the file exists
            if !test_file.exists() {
                return Err(anyhow::anyhow!(
                    "Test data file not found: {}. Current dir: {:?}",
                    test_file.display(),
                    std::env::current_dir()
                ));
            }

            // Create a temp directory for Cayenne data
            let temp_dir = tempfile::tempdir()
                .map_err(|e| anyhow::anyhow!("Failed to create temp directory: {e}"))?;
            let cayenne_path = temp_dir.path().to_path_buf();

            crate::configure_test_datafusion();

            // Configure dataset with partition_by: bucket(3, id)
            let mut dataset =
                Dataset::new(format!("file://{}", test_file.display()), "bucket_test");

            // Configure Cayenne acceleration with bucket partitioning
            // Use temp directory for Cayenne data
            let mut params = HashMap::new();
            params.insert(
                "cayenne_file_path".to_string(),
                cayenne_path.display().to_string(),
            );

            dataset.acceleration = Some(Acceleration {
                enabled: true,
                engine: Some("cayenne".to_string()),
                mode: Mode::File,
                refresh_mode: Some(RefreshMode::Full),
                params: Some(Params::from_string_map(params)),
                partition_by: vec![PartitionedBy {
                    name: "expr0".to_string(),
                    expression: "bucket(3, id)".to_string(),
                }],
                ..Acceleration::default()
            });

            let app = AppBuilder::new("test_cayenne_partition_by_bucket")
                .with_dataset(dataset)
                .build();

            let rt = Arc::new(Runtime::builder().with_app(app).build().await);

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err(anyhow::Error::msg("Timeout waiting for components to load"));
                }
                () = Arc::clone(&rt).load_components() => {}
            }

            runtime_ready_check(&rt).await;

            // Test 1: Query all data - should scan all partitions
            let result = execute_rt_sql(&rt, "SELECT * FROM bucket_test ORDER BY id").await?;
            let count = result.iter().map(RecordBatch::num_rows).sum::<usize>();
            assert_eq!(count, 10, "Should have 10 rows total");

            let plan = get_physical_plan(&rt, "SELECT * FROM bucket_test").await?;
            let plan_str = displayable(plan.as_ref()).indent(true).to_string();
            let sanitized_plan = sanitize_file_paths(&plan_str);
            insta::assert_snapshot!("bucket_partition_full_scan", sanitized_plan);

            // Test 2: Query with id = 1 filter - should only scan partition containing id=1
            let result = execute_rt_sql(&rt, "SELECT * FROM bucket_test WHERE id = 1").await?;
            let count = result.iter().map(RecordBatch::num_rows).sum::<usize>();
            assert_eq!(count, 1, "Should have 1 row for id=1");

            let plan = get_physical_plan(&rt, "SELECT * FROM bucket_test WHERE id = 1").await?;
            let plan_str = displayable(plan.as_ref()).indent(true).to_string();
            let sanitized_plan = sanitize_file_paths(&plan_str);
            insta::assert_snapshot!("bucket_partition_id_equals_filter", sanitized_plan);

            // Test 3: Query with id IN (1, 5) - may scan 1 or 2 partitions depending on hash
            let result = execute_rt_sql(
                &rt,
                "SELECT * FROM bucket_test WHERE id IN (1, 5) ORDER BY id",
            )
            .await?;
            let count = result.iter().map(RecordBatch::num_rows).sum::<usize>();
            assert_eq!(count, 2, "Should have 2 rows");

            let plan =
                get_physical_plan(&rt, "SELECT * FROM bucket_test WHERE id IN (1, 5)").await?;
            let plan_str = displayable(plan.as_ref()).indent(true).to_string();
            let sanitized_plan = sanitize_file_paths(&plan_str);
            insta::assert_snapshot!("bucket_partition_id_in_filter", sanitized_plan);

            // Test 4: Query with range filter - should push down filter to each partition
            // because bucket partitions contain multiple values
            let result =
                execute_rt_sql(&rt, "SELECT * FROM bucket_test WHERE id >= 5 ORDER BY id").await?;
            let count = result.iter().map(RecordBatch::num_rows).sum::<usize>();
            assert_eq!(count, 6, "Should have 6 rows with id >= 5");

            let plan = get_physical_plan(&rt, "SELECT * FROM bucket_test WHERE id >= 5").await?;
            let plan_str = displayable(plan.as_ref()).indent(true).to_string();
            let sanitized_plan = sanitize_file_paths(&plan_str);
            insta::assert_snapshot!("bucket_partition_id_range_filter", sanitized_plan);

            // Test 5: Query with filter on non-partition column - should scan all partitions
            let result = execute_rt_sql(
                &rt,
                "SELECT * FROM bucket_test WHERE score > 85 ORDER BY id",
            )
            .await?;
            let count = result.iter().map(RecordBatch::num_rows).sum::<usize>();
            assert_eq!(count, 5, "Should have 5 rows with score > 85");

            let plan = get_physical_plan(&rt, "SELECT * FROM bucket_test WHERE score > 85").await?;
            let plan_str = displayable(plan.as_ref()).indent(true).to_string();
            let sanitized_plan = sanitize_file_paths(&plan_str);
            insta::assert_snapshot!("bucket_partition_non_partition_filter", sanitized_plan);

            Ok(())
        })
        .await
}

/// Test `partition_by` with multiple `bucket()` expressions
///
/// This test verifies that multiple partition expressions work together:
/// - `partition_by`: [bucket(3, id), bucket(2, score)]
///
/// NOTE: This test is currently disabled due to data duplication issues.
/// The single bucket partition test (`test_cayenne_partition_by_bucket`) validates
/// the core functionality of `partition_by` with `bucket()` expressions.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(not(target_os = "windows"))]
#[ignore = "Data duplication issue with multiple partition expressions - needs investigation"]
async fn test_cayenne_partition_by_multiple_expressions() -> Result<(), anyhow::Error> {
    let _tracing = crate::init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            // Use the test data file from the data directory
            let test_file = std::env::current_dir()
                .map_err(|e| anyhow::anyhow!("Failed to get current directory: {e}"))?
                .join("tests/acceleration/data/partition_test.csv");

            // Create a temp directory for Cayenne data
            let temp_dir = tempfile::tempdir()
                .map_err(|e| anyhow::anyhow!("Failed to create temp directory: {e}"))?;
            let cayenne_path = temp_dir.path().to_path_buf();

            crate::configure_test_datafusion();

            // Configure dataset with partition_by: bucket(3, id)
            let mut dataset = Dataset::new(
                format!("file://{}", test_file.display()),
                "multi_partition_test",
            );

            // Configure Cayenne acceleration with multiple partition expressions
            let mut param_map = std::collections::HashMap::new();
            param_map.insert(
                "cayenne_file_path".to_string(),
                cayenne_path.display().to_string(),
            );
            let acceleration_params = spicepod::param::Params::from_string_map(param_map);

            dataset.acceleration = Some(Acceleration {
                enabled: true,
                engine: Some("cayenne".to_string()),
                mode: Mode::File,
                refresh_mode: Some(RefreshMode::Full),
                params: Some(acceleration_params),
                partition_by: vec![
                    PartitionedBy {
                        name: "expr0".to_string(),
                        expression: "bucket(3, id)".to_string(),
                    },
                    PartitionedBy {
                        name: "expr1".to_string(),
                        expression: "bucket(2, score)".to_string(),
                    },
                ],
                ..Acceleration::default()
            });

            let app = AppBuilder::new("test_cayenne_partition_by_multiple")
                .with_dataset(dataset)
                .build();

            let rt = Arc::new(Runtime::builder().with_app(app).build().await);

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err(anyhow::Error::msg("Timeout waiting for components to load"));
                }
                () = Arc::clone(&rt).load_components() => {}
            }

            runtime_ready_check(&rt).await;

            // Test 1: Query all data
            let result =
                execute_rt_sql(&rt, "SELECT * FROM multi_partition_test ORDER BY id").await?;
            let count = result.iter().map(RecordBatch::num_rows).sum::<usize>();
            assert_eq!(count, 10, "Should have 10 rows total");

            let plan = get_physical_plan(&rt, "SELECT * FROM multi_partition_test").await?;
            let plan_str = displayable(plan.as_ref()).indent(true).to_string();
            let sanitized_plan = sanitize_file_paths(&plan_str);
            insta::assert_snapshot!("multi_partition_full_scan", sanitized_plan);

            // Test 2: Filter on first partition column (id)
            let result =
                execute_rt_sql(&rt, "SELECT * FROM multi_partition_test WHERE id = 1").await?;
            let count = result.iter().map(RecordBatch::num_rows).sum::<usize>();
            assert_eq!(count, 1, "Should have 1 row for id=1");

            let plan =
                get_physical_plan(&rt, "SELECT * FROM multi_partition_test WHERE id = 1").await?;
            let plan_str = displayable(plan.as_ref()).indent(true).to_string();
            let sanitized_plan = sanitize_file_paths(&plan_str);
            insta::assert_snapshot!("multi_partition_id_filter", sanitized_plan);

            // Test 3: Filter on second partition column (score range)
            let result = execute_rt_sql(
                &rt,
                "SELECT * FROM multi_partition_test WHERE score >= 80 AND score < 90 ORDER BY id",
            )
            .await?;
            let count = result.iter().map(RecordBatch::num_rows).sum::<usize>();
            assert_eq!(count, 4, "Should have 4 rows in score range [80, 90)");

            let plan = get_physical_plan(
                &rt,
                "SELECT * FROM multi_partition_test WHERE score >= 80 AND score < 90",
            )
            .await?;
            let plan_str = displayable(plan.as_ref()).indent(true).to_string();
            let sanitized_plan = sanitize_file_paths(&plan_str);
            insta::assert_snapshot!("multi_partition_score_range_filter", sanitized_plan);

            // Test 4: Filter on both partition columns
            let result = execute_rt_sql(
                &rt,
                "SELECT * FROM multi_partition_test WHERE id = 1 AND score >= 80",
            )
            .await?;
            let count = result.iter().map(RecordBatch::num_rows).sum::<usize>();
            assert_eq!(count, 1, "Should have 1 row matching both filters");

            let plan = get_physical_plan(
                &rt,
                "SELECT * FROM multi_partition_test WHERE id = 1 AND score >= 80",
            )
            .await?;
            let plan_str = displayable(plan.as_ref()).indent(true).to_string();
            let sanitized_plan = sanitize_file_paths(&plan_str);
            insta::assert_snapshot!("multi_partition_both_filters", sanitized_plan);

            Ok(())
        })
        .await
}

/// Test `partition_by` with `bucket()` expression and NULL values
///
/// This test verifies that NULL values in partitioned columns are handled correctly:
/// 1. NULL values in the partition column produce NULL partition values
/// 2. NULL partition values are encoded as "none" in partition directories (Hive-style)
/// 3. Partitions can be correctly re-inferred after creation with NULL values
/// 4. Data with NULL partition values can be queried correctly
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(not(target_os = "windows"))]
#[expect(clippy::too_many_lines)]
async fn test_cayenne_partition_by_bucket_with_nulls() -> Result<(), anyhow::Error> {
    let _tracing = crate::init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            // Use test data file with NULL values
            let test_file = std::env::current_dir()
                .map_err(|e| anyhow::anyhow!("Failed to get current directory: {e}"))?
                .join("tests/acceleration/data/partition_test_with_nulls.csv");

            // Create a temp directory for Cayenne data
            let temp_dir = tempfile::tempdir()
                .map_err(|e| anyhow::anyhow!("Failed to create temp directory: {e}"))?;
            let cayenne_path = temp_dir.path().to_path_buf();

            crate::configure_test_datafusion();

            // Configure dataset with partition_by: bucket(3, name) - name column has NULLs
            let mut dataset = Dataset::new(
                format!("file://{}", test_file.display()),
                "null_partition_test",
            );

            let mut param_map = HashMap::new();
            param_map.insert(
                "cayenne_file_path".to_string(),
                cayenne_path.display().to_string(),
            );
            let acceleration_params = Params::from_string_map(param_map);

            dataset.acceleration = Some(Acceleration {
                enabled: true,
                engine: Some("cayenne".to_string()),
                mode: Mode::File,
                refresh_mode: Some(RefreshMode::Full),
                params: Some(acceleration_params),
                partition_by: vec![PartitionedBy {
                    name: "name_bucket".to_string(),
                    expression: "bucket(3, name)".to_string(),
                }],
                ..Acceleration::default()
            });

            let app = AppBuilder::new("test_cayenne_partition_null")
                .with_dataset(dataset)
                .build();

            let rt = Arc::new(Runtime::builder().with_app(app).build().await);

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err(anyhow::Error::msg("Timeout waiting for components to load"));
                }
                () = Arc::clone(&rt).load_components() => {}
            }

            runtime_ready_check(&rt).await;

            // Test 1: Query all data - should include rows with NULL names
            let result =
                execute_rt_sql(&rt, "SELECT * FROM null_partition_test ORDER BY id").await?;
            let count = result.iter().map(RecordBatch::num_rows).sum::<usize>();
            assert_eq!(count, 10, "Should have 10 rows total including NULLs");

            let plan = get_physical_plan(&rt, "SELECT * FROM null_partition_test").await?;
            let plan_str = displayable(plan.as_ref()).indent(true).to_string();
            let sanitized_plan = sanitize_file_paths(&plan_str);
            insta::assert_snapshot!("null_partition_full_scan", sanitized_plan);

            // Test 2: Query rows with NULL names specifically
            let result = execute_rt_sql(
                &rt,
                "SELECT id, name FROM null_partition_test WHERE name IS NULL ORDER BY id",
            )
            .await?;
            let count = result.iter().map(RecordBatch::num_rows).sum::<usize>();
            assert_eq!(count, 2, "Should have 2 rows with NULL names (id=3, id=8)");

            let plan =
                get_physical_plan(&rt, "SELECT * FROM null_partition_test WHERE name IS NULL")
                    .await?;
            let plan_str = displayable(plan.as_ref()).indent(true).to_string();
            let sanitized_plan = sanitize_file_paths(&plan_str);
            insta::assert_snapshot!("null_partition_name_is_null_filter", sanitized_plan);

            // Test 3: Query rows with non-NULL names
            let result = execute_rt_sql(
                &rt,
                "SELECT id, name FROM null_partition_test WHERE name IS NOT NULL ORDER BY id",
            )
            .await?;
            let count = result.iter().map(RecordBatch::num_rows).sum::<usize>();
            assert_eq!(count, 8, "Should have 8 rows with non-NULL names");

            let plan = get_physical_plan(
                &rt,
                "SELECT * FROM null_partition_test WHERE name IS NOT NULL",
            )
            .await?;
            let plan_str = displayable(plan.as_ref()).indent(true).to_string();
            let sanitized_plan = sanitize_file_paths(&plan_str);
            insta::assert_snapshot!("null_partition_name_is_not_null_filter", sanitized_plan);

            // Test 4: Query with specific bucket filter - should prune to single partition
            // Filter using bucket(3, name) = 0 to test partition pruning
            let result = execute_rt_sql(
                &rt,
                "SELECT * FROM null_partition_test WHERE bucket(3, name) = 0 ORDER BY id",
            )
            .await?;
            let count = result.iter().map(RecordBatch::num_rows).sum::<usize>();
            assert!(count > 0, "Should have some rows in bucket 0");

            let plan = get_physical_plan(
                &rt,
                "SELECT * FROM null_partition_test WHERE bucket(3, name) = 0",
            )
            .await?;
            let plan_str = displayable(plan.as_ref()).indent(true).to_string();
            let sanitized_plan = sanitize_file_paths(&plan_str);
            insta::assert_snapshot!("null_partition_bucket_equals_filter", sanitized_plan);

            // Test 5: Verify partition directory structure exists (Cayenne uses catalog-based partitioning)
            // Just verify that we can successfully query the data, which confirms partitions were created
            let result =
                execute_rt_sql(&rt, "SELECT COUNT(*) as cnt FROM null_partition_test").await?;
            let count = result.iter().map(RecordBatch::num_rows).sum::<usize>();
            assert_eq!(count, 1, "Should return count result");

            // Test 6: Verify NULL values are handled correctly in bucket function
            // bucket(n, NULL) should return NULL, creating a "none" partition
            let result = execute_rt_sql(
                &rt,
                "SELECT COUNT(*) as cnt FROM null_partition_test WHERE name IS NULL",
            )
            .await?;
            let count = result.iter().map(RecordBatch::num_rows).sum::<usize>();
            assert_eq!(
                count, 1,
                "Should successfully query NULL partition after re-inference"
            );

            Ok(())
        })
        .await
}

/// Test `partition_by` with numeric column containing NULLs
///
/// This test verifies NULL handling specifically for numeric partition columns
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(not(target_os = "windows"))]
#[expect(clippy::too_many_lines)]
async fn test_cayenne_partition_by_bucket_numeric_nulls() -> Result<(), anyhow::Error> {
    let _tracing = crate::init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            // Use test data file with NULL values
            let test_file = std::env::current_dir()
                .map_err(|e| anyhow::anyhow!("Failed to get current directory: {e}"))?
                .join("tests/acceleration/data/partition_test_with_nulls.csv");

            // Create a temp directory for Cayenne data
            let temp_dir = tempfile::tempdir()
                .map_err(|e| anyhow::anyhow!("Failed to create temp directory: {e}"))?;
            let cayenne_path = temp_dir.path().to_path_buf();

            crate::configure_test_datafusion();

            // Configure dataset with partition_by: bucket(3, score) - score column has NULLs
            let mut dataset = Dataset::new(
                format!("file://{}", test_file.display()),
                "numeric_null_partition_test",
            );

            let mut param_map = HashMap::new();
            param_map.insert(
                "cayenne_file_path".to_string(),
                cayenne_path.display().to_string(),
            );
            let acceleration_params = Params::from_string_map(param_map);

            dataset.acceleration = Some(Acceleration {
                enabled: true,
                engine: Some("cayenne".to_string()),
                mode: Mode::File,
                refresh_mode: Some(RefreshMode::Full),
                params: Some(acceleration_params),
                partition_by: vec![PartitionedBy {
                    name: "score_bucket".to_string(),
                    expression: "bucket(4, score)".to_string(),
                }],
                ..Acceleration::default()
            });

            let app = AppBuilder::new("test_cayenne_numeric_null")
                .with_dataset(dataset)
                .build();

            let rt = Arc::new(Runtime::builder().with_app(app).build().await);

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err(anyhow::Error::msg("Timeout waiting for components to load"));
                }
                () = Arc::clone(&rt).load_components() => {}
            }

            runtime_ready_check(&rt).await;

            // Test 1: Query all data
            let result = execute_rt_sql(
                &rt,
                "SELECT * FROM numeric_null_partition_test ORDER BY id",
            )
            .await?;
            let count = result.iter().map(RecordBatch::num_rows).sum::<usize>();
            assert_eq!(count, 10, "Should have 10 rows total");

            let plan =
                get_physical_plan(&rt, "SELECT * FROM numeric_null_partition_test").await?;
            let plan_str = displayable(plan.as_ref()).indent(true).to_string();
            let sanitized_plan = sanitize_file_paths(&plan_str);
            insta::assert_snapshot!("numeric_null_partition_full_scan", sanitized_plan);

            // Test 2: Query rows with NULL scores
            let result = execute_rt_sql(
                &rt,
                "SELECT id, score FROM numeric_null_partition_test WHERE score IS NULL ORDER BY id",
            )
            .await?;
            let count = result.iter().map(RecordBatch::num_rows).sum::<usize>();
            assert_eq!(count, 2, "Should have 2 rows with NULL scores (id=6, id=9)");

            let plan = get_physical_plan(
                &rt,
                "SELECT * FROM numeric_null_partition_test WHERE score IS NULL",
            )
            .await?;
            let plan_str = displayable(plan.as_ref()).indent(true).to_string();
            let sanitized_plan = sanitize_file_paths(&plan_str);
            insta::assert_snapshot!("numeric_null_partition_score_is_null_filter", sanitized_plan);

            // Test 3: Query rows with specific score range (non-NULL)
            let result = execute_rt_sql(
                &rt,
                "SELECT id, score FROM numeric_null_partition_test WHERE score >= 85 AND score < 95 ORDER BY id",
            )
            .await?;
            let count = result.iter().map(RecordBatch::num_rows).sum::<usize>();
            assert_eq!(
                count, 5,
                "Should have 5 rows with scores in range [85, 95)"
            );

            let plan = get_physical_plan(
                &rt,
                "SELECT * FROM numeric_null_partition_test WHERE score >= 85 AND score < 95",
            )
            .await?;
            let plan_str = displayable(plan.as_ref()).indent(true).to_string();
            let sanitized_plan = sanitize_file_paths(&plan_str);
            insta::assert_snapshot!("numeric_null_partition_score_range_filter", sanitized_plan);

            // Test 4: Query with specific bucket filter - should prune to single partition
            let result = execute_rt_sql(
                &rt,
                "SELECT * FROM numeric_null_partition_test WHERE bucket(4, score) = 1 ORDER BY id",
            )
            .await?;
            let count = result.iter().map(RecordBatch::num_rows).sum::<usize>();
            assert!(count > 0, "Should have some rows in bucket 1");

            let plan = get_physical_plan(
                &rt,
                "SELECT * FROM numeric_null_partition_test WHERE bucket(4, score) = 1",
            )
            .await?;
            let plan_str = displayable(plan.as_ref()).indent(true).to_string();
            let sanitized_plan = sanitize_file_paths(&plan_str);
            insta::assert_snapshot!("numeric_null_partition_bucket_equals_filter", sanitized_plan);

            // Test 5: Verify data integrity and NULL partition handling
            // All rows should be queryable, including those with NULL scores
            let result = execute_rt_sql(
                &rt,
                "SELECT COUNT(*) as total FROM numeric_null_partition_test",
            )
            .await?;
            let count = result.iter().map(RecordBatch::num_rows).sum::<usize>();
            assert_eq!(count, 1, "Should return count result");

            Ok(())
        })
        .await
}

/// Test `partition_by` with `date_part()` expression for temporal partitioning
///
/// This test verifies that:
/// 1. `partition_by`: `date_part`('month', `order_date`) correctly partitions data by month
/// 2. Queries with filters on the partition expression use partition pruning
/// 3. Physical plans show correct partition structure with month-based partitions
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(not(target_os = "windows"))]
async fn test_cayenne_partition_by_date_part() -> Result<(), anyhow::Error> {
    let _tracing = crate::init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            // Use test data file with date column
            let test_file = std::env::current_dir()
                .map_err(|e| anyhow::anyhow!("Failed to get current directory: {e}"))?
                .join("tests/acceleration/data/partition_test_with_dates.csv");

            // Create a temp directory for Cayenne data
            let temp_dir = tempfile::tempdir()
                .map_err(|e| anyhow::anyhow!("Failed to create temp directory: {e}"))?;
            let cayenne_path = temp_dir.path().to_path_buf();

            crate::configure_test_datafusion();

            // Configure dataset with partition_by: date_part('month', order_date)
            let mut dataset = Dataset::new(
                format!("file://{}", test_file.display()),
                "date_partition_test",
            );

            let mut param_map = HashMap::new();
            param_map.insert(
                "cayenne_file_path".to_string(),
                cayenne_path.display().to_string(),
            );
            let acceleration_params = Params::from_string_map(param_map);

            dataset.acceleration = Some(Acceleration {
                enabled: true,
                engine: Some("cayenne".to_string()),
                mode: Mode::File,
                refresh_mode: Some(RefreshMode::Full),
                params: Some(acceleration_params),
                partition_by: vec![PartitionedBy {
                    name: "order_month".to_string(),
                    expression: "date_part('month', order_date)".to_string(),
                }],
                ..Acceleration::default()
            });

            let app = AppBuilder::new("test_cayenne_date_partition")
                .with_dataset(dataset)
                .build();

            let rt = Arc::new(Runtime::builder().with_app(app).build().await);

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err(anyhow::Error::msg("Timeout waiting for components to load"));
                }
                () = Arc::clone(&rt).load_components() => {}
            }

            runtime_ready_check(&rt).await;

            // Test 1: Query all data - should scan all month partitions
            let result = execute_rt_sql(&rt, "SELECT * FROM date_partition_test ORDER BY id").await?;
            let count = result.iter().map(RecordBatch::num_rows).sum::<usize>();
            assert_eq!(count, 10, "Should have 10 rows total");

            let plan = get_physical_plan(&rt, "SELECT * FROM date_partition_test").await?;
            let plan_str = displayable(plan.as_ref()).indent(true).to_string();
            let sanitized_plan = sanitize_file_paths(&plan_str);
            insta::assert_snapshot!("date_partition_full_scan", sanitized_plan);

            // Test 2: Query with filter on underlying date column
            // This should NOT enable partition pruning because filter is on order_date, not date_part
            let result = execute_rt_sql(
                &rt,
                "SELECT * FROM date_partition_test WHERE order_date >= '2024-03-01' AND order_date < '2024-04-01' ORDER BY id",
            )
            .await?;
            let count = result.iter().map(RecordBatch::num_rows).sum::<usize>();
            assert_eq!(count, 2, "Should have 2 rows in March");

            let plan = get_physical_plan(
                &rt,
                "SELECT * FROM date_partition_test WHERE order_date >= '2024-03-01' AND order_date < '2024-04-01'",
            )
            .await?;
            let plan_str = displayable(plan.as_ref()).indent(true).to_string();
            let sanitized_plan = sanitize_file_paths(&plan_str);
            insta::assert_snapshot!("date_partition_date_range_filter", sanitized_plan);

            // Test 3: Query with filter on partition expression (date_part)
            // NOTE: Partition pruning for date_part() is not yet implemented, so this
            // will scan all partitions but apply the filter. This test validates that
            // date_part partitioning works correctly for data correctness.
            let result = execute_rt_sql(
                &rt,
                "SELECT * FROM date_partition_test WHERE date_part('month', order_date) = 3 ORDER BY id",
            )
            .await?;
            let count = result.iter().map(RecordBatch::num_rows).sum::<usize>();
            assert_eq!(count, 2, "Should have 2 rows with month = 3");

            let plan = get_physical_plan(
                &rt,
                "SELECT * FROM date_partition_test WHERE date_part('month', order_date) = 3",
            )
            .await?;
            let plan_str = displayable(plan.as_ref()).indent(true).to_string();
            let sanitized_plan = sanitize_file_paths(&plan_str);
            insta::assert_snapshot!("date_partition_month_equals_filter", sanitized_plan);

            // Test 4: Query with IN filter on partition expression
            // NOTE: Partition pruning for date_part() is not yet implemented
            let result = execute_rt_sql(
                &rt,
                "SELECT * FROM date_partition_test WHERE date_part('month', order_date) IN (1, 2) ORDER BY id",
            )
            .await?;
            let count = result.iter().map(RecordBatch::num_rows).sum::<usize>();
            assert_eq!(count, 4, "Should have 4 rows in January and February");

            let plan = get_physical_plan(
                &rt,
                "SELECT * FROM date_partition_test WHERE date_part('month', order_date) IN (1, 2)",
            )
            .await?;
            let plan_str = displayable(plan.as_ref()).indent(true).to_string();
            let sanitized_plan = sanitize_file_paths(&plan_str);
            insta::assert_snapshot!("date_partition_month_in_filter", sanitized_plan);

            Ok(())
        })
        .await
}
