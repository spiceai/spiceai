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

//! Integration test for distributed acceleration with `bucket()` partitioning.
//!
//! Verifies that in a cluster with scheduler + executors:
//! 1. Partition values are resolved statically for deterministic `bucket(N, col)` expressions
//! 2. Executors correctly load accelerated data for their assigned partitions
//! 3. Queries through the scheduler return correct results from accelerated executors
//! 4. The `bucket()` UDF is available in the refresh context for partition filtering

use app::AppBuilder;
use spicepod::component::dataset::Dataset;
use spicepod::component::runtime::{
    PartitionManagement, Runtime as SpicepodRuntime, Scheduler as SchedulerConfig,
};
use spicepod::{
    acceleration::{Acceleration, Mode, RefreshMode},
    partitioning::PartitionedBy,
};
use std::time::Duration;
use tracing_subscriber::EnvFilter;

use crate::{
    configure_test_datafusion,
    utils::{test_request_context, verify_env_secret_exists},
};

use super::harness::ClusterHarness;

/// CSV test data
const TEST_DATA_CSV: &str = r"id,name,age,city,score
1,John Doe,28,New York,85
2,Jane Smith,34,Los Angeles,92
3,Mike Johnson,45,Chicago,78
4,Emily Brown,31,Houston,89
5,David Lee,39,Phoenix,76
6,Sarah Wilson,26,Philadelphia,94
7,Tom Anderson,52,San Antonio,81
8,Lisa Taylor,29,San Diego,88
9,Chris Martin,37,Dallas,79
10,Anna Garcia,41,San Jose,90
";

/// Secondary CSV for join tests: id + category + rating, matching TEST_DATA_CSV ids 1–10.
const CATEGORIES_CSV: &str = r"id,category,rating
1,A,4.5
2,B,3.2
3,A,4.8
4,C,2.1
5,B,3.9
6,A,4.2
7,C,1.8
8,B,3.5
9,A,4.6
10,C,2.7
";

/// Test that distributed acceleration with `bucket()` partitioning works end to end
/// with an executor.
///
/// Sets up a cluster with 1 scheduler + 1 executor accelerating data
/// with `partition_by: bucket(3, id)` using the Cayenne engine. Verifies:
/// - `bucket()` UDF can be used in the dataset definition for partitioning
/// - Queries return correct, complete results across all executors
/// - EXPLAIN plans correctly reflect the distributed execution plan
///
/// The dataset configures a single Cayenne `base_dir` that is shared by all
/// in-process executors for this test, using an isolated temporary directory
/// per test run to avoid interference with other tests.
#[tokio::test(flavor = "multi_thread")]
#[cfg(not(target_os = "windows"))]
async fn test_distributed_acceleration_with_bucket_partitioning() -> Result<(), anyhow::Error> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new("runtime=debug,info"))
        .with_ansi(true)
        .try_init();

    for env_var in ["AWS_S3_VECTORS_KEY", "AWS_S3_VECTORS_SECRET"] {
        verify_env_secret_exists(env_var)
            .await
            .map_err(anyhow::Error::msg)?;
    }

    // Keep the tempdirs alive for the duration of the test.
    let csv_tempdir = tempfile::tempdir().expect("csv tempdir");
    let csv_path = csv_tempdir.path().join("test_data.csv");
    tokio::fs::write(&csv_path, TEST_DATA_CSV)
        .await
        .expect("write test data file");

    let cayenne_tempdir = tempfile::tempdir().expect("cayenne tempdir");

    test_request_context()
        .scope(async {
            configure_test_datafusion();
            let app = AppBuilder::new("test_distributed_accel")
                .with_dataset(make_accelerated_dataset(
                    format!("file:{}", csv_path.display()),
                    "test_data",
                    3,
                    "id",
                    cayenne_tempdir.path(),
                ))
                .with_runtime(SpicepodRuntime {
                    scheduler: Some(make_named_scheduler_config(
                        "test_distributed_acceleration_with_bucket_partitioning",
                    )),
                    ..SpicepodRuntime::default()
                })
                .build();
            let harness = ClusterHarness::builder()
                .scheduler(app)
                .executors(1)
                .start()
                .await?;

            harness.wait_for_executors(Duration::from_secs(15)).await?;

            // Wait for executors to load and accelerate their assigned partitions.
            wait_for_row_count(&harness, "test_data", 10, Duration::from_secs(60)).await?;

            // --- Test 1: SELECT all rows ---
            let select_all_sql = "SELECT id, name, age, city, score FROM test_data ORDER BY id";

            let plan = harness.explain(select_all_sql).await?;
            let plan_fmt = arrow::util::pretty::pretty_format_batches(&plan)
                .expect("format explain")
                .to_string();
            insta::assert_snapshot!(plan_fmt, @r#"
            +---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
            | plan_type     | plan                                                                                                                                                                                    |
            +---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
            | logical_plan  | Sort: test_data.id ASC NULLS LAST                                                                                                                                                       |
            |               |   TableScan: test_data projection=[id, name, age, city, score], full_filters=[bucket(Int64(3), id) = Utf8("0") OR bucket(Int64(3), id) = Utf8("1") OR bucket(Int64(3), id) = Utf8("2")] |
            | physical_plan | SortExec: expr=[id@0 ASC NULLS LAST], preserve_partitioning=[false]                                                                                                                     |
            |               |   CooperativeExec                                                                                                                                                                       |
            |               |     BytesProcessedExec                                                                                                                                                                  |
            |               |       FlightSqlExec sql=SELECT id, name, age, city, score FROM test_data WHERE bucket(3, "id") = '0' OR bucket(3, "id") = '1' OR bucket(3, "id") = '2'                                   |
            |               |                                                                                                                                                                                         |
            +---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
            "#);

            let rows = harness.query(select_all_sql).await?;
            let rows_fmt = arrow::util::pretty::pretty_format_batches(&rows).expect("format rows");
            insta::assert_snapshot!( rows_fmt, @r"
            +----+--------------+-----+--------------+-------+
            | id | name         | age | city         | score |
            +----+--------------+-----+--------------+-------+
            | 1  | John Doe     | 28  | New York     | 85    |
            | 2  | Jane Smith   | 34  | Los Angeles  | 92    |
            | 3  | Mike Johnson | 45  | Chicago      | 78    |
            | 4  | Emily Brown  | 31  | Houston      | 89    |
            | 5  | David Lee    | 39  | Phoenix      | 76    |
            | 6  | Sarah Wilson | 26  | Philadelphia | 94    |
            | 7  | Tom Anderson | 52  | San Antonio  | 81    |
            | 8  | Lisa Taylor  | 29  | San Diego    | 88    |
            | 9  | Chris Martin | 37  | Dallas       | 79    |
            | 10 | Anna Garcia  | 41  | San Jose     | 90    |
            +----+--------------+-----+--------------+-------+
            ");

            // --- Test 2: Aggregation ---
            let aggregation_sql = "SELECT COUNT(*) as total_rows, AVG(score) as avg_score, \
                                   MIN(age) as min_age, MAX(age) as max_age FROM test_data";

            let agg_plan = harness.explain(aggregation_sql).await?;
            let agg_plan_fmt = arrow::util::pretty::pretty_format_batches(&agg_plan)
                .expect("format explain agg")
                .to_string();
            insta::assert_snapshot!(agg_plan_fmt, @r#"
            +---------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
            | plan_type     | plan                                                                                                                                                                      |
            +---------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
            | logical_plan  | Projection: count(Int64(1)) AS total_rows, avg(test_data.score) AS avg_score, min(test_data.age) AS min_age, max(test_data.age) AS max_age                                |
            |               |   Aggregate: groupBy=[[]], aggr=[[count(Int64(1)), avg(CAST(test_data.score AS Float64)), min(test_data.age), max(test_data.age)]]                                        |
            |               |     TableScan: test_data projection=[age, score], full_filters=[bucket(Int64(3), id) = Utf8("0") OR bucket(Int64(3), id) = Utf8("1") OR bucket(Int64(3), id) = Utf8("2")] |
            | physical_plan | ProjectionExec: expr=[count(Int64(1))@0 as total_rows, avg(test_data.score)@1 as avg_score, min(test_data.age)@2 as min_age, max(test_data.age)@3 as max_age]             |
            |               |   AggregateExec: mode=Final, gby=[], aggr=[count(Int64(1)), avg(test_data.score), min(test_data.age), max(test_data.age)]                                                 |
            |               |     CoalescePartitionsExec                                                                                                                                                |
            |               |       AggregateExec: mode=Partial, gby=[], aggr=[count(Int64(1)), avg(test_data.score), min(test_data.age), max(test_data.age)]                                           |
            |               |         RepartitionExec: partitioning=RoundRobinBatch(3), input_partitions=1                                                                                              |
            |               |           CooperativeExec                                                                                                                                                 |
            |               |             BytesProcessedExec                                                                                                                                            |
            |               |               FlightSqlExec sql=SELECT age, score FROM test_data WHERE bucket(3, "id") = '0' OR bucket(3, "id") = '1' OR bucket(3, "id") = '2'                            |
            |               |                                                                                                                                                                           |
            +---------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
            "#);

            let agg = harness.query(aggregation_sql).await?;
            let agg_fmt = arrow::util::pretty::pretty_format_batches(&agg).expect("format agg");
            insta::assert_snapshot!(agg_fmt, @r"
            +------------+-----------+---------+---------+
            | total_rows | avg_score | min_age | max_age |
            +------------+-----------+---------+---------+
            | 10         | 85.2      | 26      | 52      |
            +------------+-----------+---------+---------+
            ");

            harness.shutdown().await;
            Ok(())
        })
        .await
}

/// Test that 2 executors with `bucket(4, id)` partitioning correctly split data and
/// that the scheduler generates a plan across both.
///
/// Verifies:
/// - 4 buckets are distributed across 2 executors (2 buckets each)
/// - Full SELECT returns all 10 rows without duplicates
/// - COUNT/AVG aggregations produce correct cross-executor results
#[tokio::test(flavor = "multi_thread")]
#[cfg(not(target_os = "windows"))]
async fn test_distributed_acceleration_multi_executor() -> Result<(), anyhow::Error> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new("runtime=debug,info"))
        .with_ansi(true)
        .try_init();

    for env_var in ["AWS_S3_VECTORS_KEY", "AWS_S3_VECTORS_SECRET"] {
        verify_env_secret_exists(env_var)
            .await
            .map_err(anyhow::Error::msg)?;
    }

    let csv_tempdir = tempfile::tempdir().expect("csv tempdir");
    let csv_path = csv_tempdir.path().join("test_data.csv");
    tokio::fs::write(&csv_path, TEST_DATA_CSV)
        .await
        .expect("write test data");

    test_request_context()
        .scope(async {
            configure_test_datafusion();
            let app = AppBuilder::new("test_multi_executor_accel")
                .with_dataset(make_memory_accelerated_dataset(
                    format!("file:{}", csv_path.display()),
                    "test_data",
                    4,
                    "id",
                ))
                .with_runtime(SpicepodRuntime {
                    scheduler: Some(make_named_scheduler_config(
                        "test_distributed_acceleration_multi_executor",
                    )),
                    ..SpicepodRuntime::default()
                })
                .build();

            let harness = ClusterHarness::builder()
                .scheduler(app)
                .executors(2)
                .start()
                .await?;

            harness.wait_for_executors(Duration::from_secs(15)).await?;
            wait_for_row_count(&harness, "test_data", 10, Duration::from_secs(60)).await?;

            // --- SELECT all rows ---
            let select_all_sql = "SELECT id, name, age, city, score FROM test_data ORDER BY id";

            let plan = harness.explain(select_all_sql).await?;
            let plan_fmt = arrow::util::pretty::pretty_format_batches(&plan)
                .expect("format explain")
                .to_string();
            insta::assert_snapshot!(plan_fmt, @r"
            +---------------+----------------------------------------------------------------------------+
            | plan_type     | plan                                                                       |
            +---------------+----------------------------------------------------------------------------+
            | logical_plan  | Sort: test_data.id ASC NULLS LAST                                          |
            |               |   TableScan: test_data projection=[id, name, age, city, score]             |
            | physical_plan | SortExec: expr=[id@0 ASC NULLS LAST], preserve_partitioning=[false]        |
            |               |   CooperativeExec                                                          |
            |               |     BytesProcessedExec                                                     |
            |               |       FlightSqlExec sql=SELECT id, name, age, city, score FROM test_data   |
            |               |                                                                            |
            +---------------+----------------------------------------------------------------------------+
            ");

            let rows = harness.query(select_all_sql).await?;
            let rows_fmt = arrow::util::pretty::pretty_format_batches(&rows).expect("format rows");
            insta::assert_snapshot!(rows_fmt, @r"
            +----+--------------+-----+--------------+-------+
            | id | name         | age | city         | score |
            +----+--------------+-----+--------------+-------+
            | 1  | John Doe     | 28  | New York     | 85    |
            | 2  | Jane Smith   | 34  | Los Angeles  | 92    |
            | 3  | Mike Johnson | 45  | Chicago      | 78    |
            | 4  | Emily Brown  | 31  | Houston      | 89    |
            | 5  | David Lee    | 39  | Phoenix      | 76    |
            | 6  | Sarah Wilson | 26  | Philadelphia | 94    |
            | 7  | Tom Anderson | 52  | San Antonio  | 81    |
            | 8  | Lisa Taylor  | 29  | San Diego    | 88    |
            | 9  | Chris Martin | 37  | Dallas       | 79    |
            | 10 | Anna Garcia  | 41  | San Jose     | 90    |
            +----+--------------+-----+--------------+-------+
            ");

            // --- Aggregation across both executors ---
            let agg_sql = "SELECT COUNT(*) as total_rows, AVG(score) as avg_score, \
                           MIN(age) as min_age, MAX(age) as max_age FROM test_data";

            let agg = harness.query(agg_sql).await?;
            let agg_fmt = arrow::util::pretty::pretty_format_batches(&agg).expect("format agg");
            insta::assert_snapshot!(agg_fmt, @r"
            +------------+-----------+---------+---------+
            | total_rows | avg_score | min_age | max_age |
            +------------+-----------+---------+---------+
            | 10         | 85.2      | 26      | 52      |
            +------------+-----------+---------+---------+
            ");

            harness.shutdown().await;
            Ok(())
        })
        .await
}

/// Test that user predicates are pushed down into the `FlightSqlExec` queries sent to
/// executors, rather than being applied as a post-fetch filter on the scheduler.
///
/// Verifies:
/// - `WHERE score > 85` appears in the `FlightSqlExec` sql string in the EXPLAIN plan
/// - Only rows matching the predicate are returned (5 rows: ids 2,4,6,8,10)
#[tokio::test(flavor = "multi_thread")]
#[cfg(not(target_os = "windows"))]
async fn test_distributed_acceleration_predicate_pushdown() -> Result<(), anyhow::Error> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new("runtime=debug,info"))
        .with_ansi(true)
        .try_init();

    for env_var in ["AWS_S3_VECTORS_KEY", "AWS_S3_VECTORS_SECRET"] {
        verify_env_secret_exists(env_var)
            .await
            .map_err(anyhow::Error::msg)?;
    }

    let csv_tempdir = tempfile::tempdir().expect("csv tempdir");
    let csv_path = csv_tempdir.path().join("test_data.csv");
    tokio::fs::write(&csv_path, TEST_DATA_CSV)
        .await
        .expect("write test data");

    test_request_context()
        .scope(async {
            configure_test_datafusion();
            let app = AppBuilder::new("test_predicate_pushdown_accel")
                .with_dataset(make_memory_accelerated_dataset(
                    format!("file:{}", csv_path.display()),
                    "test_data",
                    3,
                    "id",
                ))
                .with_runtime(SpicepodRuntime {
                    scheduler: Some(make_named_scheduler_config(
                        "test_distributed_acceleration_predicate_pushdown",
                    )),
                    ..SpicepodRuntime::default()
                })
                .build();

            let harness = ClusterHarness::builder()
                .scheduler(app)
                .executors(1)
                .start()
                .await?;

            harness.wait_for_executors(Duration::from_secs(15)).await?;
            wait_for_row_count(&harness, "test_data", 10, Duration::from_secs(60)).await?;

            // The user predicate `score > 85` must be visible inside the FlightSqlExec
            // sql string — confirming it was pushed to the executor, not applied above.
            let filtered_sql = "SELECT id, name, score FROM test_data WHERE score > 85 ORDER BY id";

            let plan = harness.explain(filtered_sql).await?;
            let plan_fmt = arrow::util::pretty::pretty_format_batches(&plan)
                .expect("format explain")
                .to_string();
            insta::assert_snapshot!(plan_fmt, @r#"
            +---------------+-------------------------------------------------------------------------------------------------+
            | plan_type     | plan                                                                                            |
            +---------------+-------------------------------------------------------------------------------------------------+
            | logical_plan  | Sort: test_data.id ASC NULLS LAST                                                               |
            |               |   TableScan: test_data projection=[id, name, score], full_filters=[test_data.score > Int64(85)] |
            | physical_plan | SortExec: expr=[id@0 ASC NULLS LAST], preserve_partitioning=[false]                             |
            |               |   CooperativeExec                                                                               |
            |               |     BytesProcessedExec                                                                          |
            |               |       FlightSqlExec sql=SELECT id, name, score FROM test_data WHERE "score" > 85                |
            |               |                                                                                                 |
            +---------------+-------------------------------------------------------------------------------------------------+
            "#);

            let rows = harness.query(filtered_sql).await?;
            let rows_fmt = arrow::util::pretty::pretty_format_batches(&rows).expect("format rows");
            insta::assert_snapshot!(rows_fmt, @r"
            +----+--------------+-------+
            | id | name         | score |
            +----+--------------+-------+
            | 2  | Jane Smith   | 92    |
            | 4  | Emily Brown  | 89    |
            | 6  | Sarah Wilson | 94    |
            | 8  | Lisa Taylor  | 88    |
            | 10 | Anna Garcia  | 90    |
            +----+--------------+-------+
            ");

            harness.shutdown().await;
            Ok(())
        })
        .await
}

/// Test that after an executor shuts down the scheduler rebalances its partitions to
/// the surviving executor, and queries continue to return correct results.
///
/// Flow:
/// 1. Start 1 scheduler + 2 executors with `bucket(4, id)`
/// 2. Wait for both executors to connect and load their partitions
/// 3. Shut down executor[0]
/// 4. Wait for the scheduler to detect the disconnect and reassign all 4 buckets to
///    executor[1]
/// 5. Verify queries still return all 10 rows
#[tokio::test(flavor = "multi_thread")]
#[cfg(not(target_os = "windows"))]
async fn test_distributed_acceleration_executor_shutdown_and_rebalance() -> Result<(), anyhow::Error>
{
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new("runtime=debug,info"))
        .with_ansi(true)
        .try_init();

    for env_var in ["AWS_S3_VECTORS_KEY", "AWS_S3_VECTORS_SECRET"] {
        verify_env_secret_exists(env_var)
            .await
            .map_err(anyhow::Error::msg)?;
    }

    let csv_tempdir = tempfile::tempdir().expect("csv tempdir");
    let csv_path = csv_tempdir.path().join("test_data.csv");
    tokio::fs::write(&csv_path, TEST_DATA_CSV)
        .await
        .expect("write test data");

    test_request_context()
        .scope(async {
            configure_test_datafusion();
            let app = AppBuilder::new("test_rebalance_accel")
                .with_dataset(make_memory_accelerated_dataset(
                    format!("file:{}", csv_path.display()),
                    "test_data",
                    4,
                    "id",
                ))
                .with_runtime(SpicepodRuntime {
                    scheduler: Some(make_named_scheduler_config(
                        "test_distributed_acceleration_executor_shutdown_and_rebalance",
                    )),
                    ..SpicepodRuntime::default()
                })
                .build();

            let harness = ClusterHarness::builder()
                .scheduler(app)
                .executors(2)
                .start()
                .await?;

            harness.wait_for_executors(Duration::from_secs(15)).await?;
            wait_for_row_count(&harness, "test_data", 10, Duration::from_secs(60)).await?;

            // Baseline: both executors up, all 10 rows visible.
            let select_all = "SELECT id FROM test_data ORDER BY id";
            let baseline = harness.query(select_all).await?;
            assert_eq!(
                baseline
                    .iter()
                    .map(arrow::array::RecordBatch::num_rows)
                    .sum::<usize>(),
                10,
                "baseline should return 10 rows with 2 executors"
            );

            // Shut down executor[0].  The scheduler's PartitionManagementTask (1s interval)
            // will detect the disconnect and reassign its buckets to executor[1].
            harness.executors[0].shutdown().await;
            harness
                .wait_until_executor_count(1, Duration::from_secs(30))
                .await?;

            // Wait for the partition manager to reassign and executor[1] to refresh.
            wait_for_row_count(&harness, "test_data", 10, Duration::from_secs(60)).await?;

            // After rebalance executor[1] should hold all 4 buckets and return all rows.
            let rows = harness.query(select_all).await?;
            let rows_fmt = arrow::util::pretty::pretty_format_batches(&rows).expect("format rows");
            insta::assert_snapshot!(rows_fmt, @r"
            +----+
            | id |
            +----+
            | 1  |
            | 2  |
            | 3  |
            | 4  |
            | 5  |
            | 6  |
            | 7  |
            | 8  |
            | 9  |
            | 10 |
            +----+
            ");

            // Explicit shutdown of the remaining executor before harness drop.
            harness.executors[1].shutdown().await;
            harness.scheduler.shutdown().await;
            Ok(())
        })
        .await
}

/// Test that the scheduler correctly plans and executes a JOIN across two independently
/// partitioned accelerated tables distributed over 2 executors.
///
/// Both `test_data` and `categories` use `bucket(4, id)` so rows with matching ids
/// may reside on different executors.  The scheduler must scatter both scans and
/// merge the join result correctly.
///
/// Verifies:
/// - JOIN returns the expected 10 rows with correct column values from both tables
/// - EXPLAIN plan reflects the distributed scatter-gather for both tables
#[tokio::test(flavor = "multi_thread")]
#[cfg(not(target_os = "windows"))]
async fn test_distributed_acceleration_join_two_partitioned_tables() -> Result<(), anyhow::Error> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new("runtime=debug,info"))
        .with_ansi(true)
        .try_init();

    for env_var in ["AWS_S3_VECTORS_KEY", "AWS_S3_VECTORS_SECRET"] {
        verify_env_secret_exists(env_var)
            .await
            .map_err(anyhow::Error::msg)?;
    }

    let csv_tempdir = tempfile::tempdir().expect("csv tempdir");
    let data_path = csv_tempdir.path().join("test_data.csv");
    let cat_path = csv_tempdir.path().join("categories.csv");
    tokio::fs::write(&data_path, TEST_DATA_CSV)
        .await
        .expect("write test_data");
    tokio::fs::write(&cat_path, CATEGORIES_CSV)
        .await
        .expect("write categories");

    test_request_context()
        .scope(async {
            configure_test_datafusion();
            let app = AppBuilder::new("test_join_partitioned")
                .with_dataset(make_memory_accelerated_dataset(
                    format!("file:{}", data_path.display()),
                    "test_data",
                    4,
                    "id",
                ))
                .with_dataset(make_memory_accelerated_dataset(
                    format!("file:{}", cat_path.display()),
                    "categories",
                    4,
                    "id",
                ))
                .with_runtime(SpicepodRuntime {
                    scheduler: Some(make_named_scheduler_config(
                        "test_distributed_acceleration_join_two_partitioned_tables",
                    )),
                    ..SpicepodRuntime::default()
                })
                .build();

            let harness = ClusterHarness::builder()
                .scheduler(app)
                .executors(2)
                .start()
                .await?;

            harness.wait_for_executors(Duration::from_secs(15)).await?;
            // Wait for both tables to be fully accelerated.
            wait_for_row_count(&harness, "test_data", 10, Duration::from_secs(60)).await?;
            wait_for_row_count(&harness, "categories", 10, Duration::from_secs(60)).await?;

            let join_sql = "SELECT t.id, t.name, c.category, c.rating \
                            FROM test_data t JOIN categories c ON t.id = c.id \
                            ORDER BY t.id";

            let plan = harness.explain(join_sql).await?;
            let plan_fmt = arrow::util::pretty::pretty_format_batches(&plan)
                .expect("format explain")
                .to_string();
            insta::assert_snapshot!(plan_fmt, @r"
            +---------------+------------------------------------------------------------------------------------------------------------------------------------------------------------+
            | plan_type     | plan                                                                                                                                                       |
            +---------------+------------------------------------------------------------------------------------------------------------------------------------------------------------+
            | logical_plan  | Sort: t.id ASC NULLS LAST                                                                                                                                  |
            |               |   Projection: t.id, t.name, c.category, c.rating                                                                                                           |
            |               |     Inner Join: t.id = c.id                                                                                                                                |
            |               |       SubqueryAlias: t                                                                                                                                     |
            |               |         TableScan: test_data projection=[id, name]                                                                                                         |
            |               |       SubqueryAlias: c                                                                                                                                     |
            |               |         TableScan: categories projection=[id, category, rating]                                                                                            |
            | physical_plan | SortPreservingMergeExec: [id@0 ASC NULLS LAST]                                                                                                             |
            |               |   SortExec: expr=[id@0 ASC NULLS LAST], preserve_partitioning=[true]                                                                                       |
            |               |     HashJoinExec: mode=CollectLeft, join_type=Inner, accumulator=MinMaxLeftAccumulator, on=[(id@0, id@0)], projection=[id@0, name@1, category@3, rating@4] |
            |               |       CooperativeExec                                                                                                                                      |
            |               |         BytesProcessedExec                                                                                                                                 |
            |               |           FlightSqlExec sql=SELECT id, name FROM test_data                                                                                                 |
            |               |       RepartitionExec: partitioning=RoundRobinBatch(3), input_partitions=1                                                                                 |
            |               |         CooperativeExec                                                                                                                                    |
            |               |           BytesProcessedExec                                                                                                                               |
            |               |             FlightSqlExec sql=SELECT id, category, rating FROM categories                                                                                  |
            |               |                                                                                                                                                            |
            +---------------+------------------------------------------------------------------------------------------------------------------------------------------------------------+
            ");

            let rows = harness.query(join_sql).await?;
            let rows_fmt = arrow::util::pretty::pretty_format_batches(&rows).expect("format rows");
            insta::assert_snapshot!( rows_fmt, @r"
            +----+--------------+----------+--------+
            | id | name         | category | rating |
            +----+--------------+----------+--------+
            | 1  | John Doe     | A        | 4.5    |
            | 2  | Jane Smith   | B        | 3.2    |
            | 3  | Mike Johnson | A        | 4.8    |
            | 4  | Emily Brown  | C        | 2.1    |
            | 5  | David Lee    | B        | 3.9    |
            | 6  | Sarah Wilson | A        | 4.2    |
            | 7  | Tom Anderson | C        | 1.8    |
            | 8  | Lisa Taylor  | B        | 3.5    |
            | 9  | Chris Martin | A        | 4.6    |
            | 10 | Anna Garcia  | C        | 2.7    |
            +----+--------------+----------+--------+
            ");

            harness.shutdown().await;
            Ok(())
        })
        .await
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Poll `SELECT COUNT(*) FROM {table}` until it returns `expected` rows, or time out.
async fn wait_for_row_count(
    harness: &ClusterHarness,
    table: &str,
    expected: usize,
    timeout: Duration,
) -> Result<(), anyhow::Error> {
    let start = std::time::Instant::now();
    loop {
        if let Ok(batches) = harness
            .query(&format!("SELECT COUNT(*) AS cnt FROM {table}"))
            .await
        {
            let total: usize = batches
                .iter()
                .map(arrow::array::RecordBatch::num_rows)
                .sum();
            if total > 0 {
                let arr = batches[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>();
                if let Some(arr) = arr {
                    #[allow(clippy::cast_sign_loss)]
                    let count = arr.value(0) as usize;
                    if count == expected {
                        return Ok(());
                    }
                }
            }
        }
        if start.elapsed() > timeout {
            return Err(anyhow::anyhow!(
                "Timed out waiting for {table} to have {expected} rows"
            ));
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Create a dataset configured with Cayenne file-mode acceleration and `bucket()` partitioning.
///
/// `base_dir` is the root under which Cayenne stores data (`{base_dir}/data/`) and
/// metadata (`{base_dir}/metadata/`). Partition data files are per-partition, so
/// multiple executors sharing the same base dir will not collide on data writes.
fn make_accelerated_dataset(
    source_path: impl Into<String>,
    name: &str,
    num_buckets: i64,
    partition_column: &str,
    base_dir: &std::path::Path,
) -> Dataset {
    let mut dataset = Dataset::new(source_path, name);

    dataset.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some("cayenne".to_string()),
        mode: Mode::File,
        refresh_mode: Some(RefreshMode::Full),
        partition_by: vec![PartitionedBy {
            name: "expr0".to_string(),
            expression: format!("bucket({num_buckets}, {partition_column})"),
        }],
        params: Some(spicepod::param::Params::from_string_map(
            std::collections::HashMap::from([
                (
                    "cayenne_file_path".to_string(),
                    base_dir.join("data").to_string_lossy().to_string(),
                ),
                (
                    "cayenne_metadata_dir".to_string(),
                    base_dir.join("metadata").to_string_lossy().to_string(),
                ),
            ]),
        )),
        ..Acceleration::default()
    });

    dataset
}

/// Create a dataset configured with in-memory Arrow acceleration and `bucket()` partitioning.
///
/// Unlike [`make_accelerated_dataset`], this uses `Mode::Memory` so no shared file path
/// is required between executors — each executor materialises its assigned partitions
/// independently in process memory.
fn make_memory_accelerated_dataset(
    source_path: impl Into<String>,
    name: &str,
    num_buckets: i64,
    partition_column: &str,
) -> Dataset {
    let mut dataset = Dataset::new(source_path, name);

    dataset.acceleration = Some(Acceleration {
        enabled: true,
        mode: Mode::Memory,
        refresh_mode: Some(RefreshMode::Full),
        partition_by: vec![PartitionedBy {
            name: "expr0".to_string(),
            expression: format!("bucket({num_buckets}, {partition_column})"),
        }],
        ..Acceleration::default()
    });

    dataset
}

/// Return a `SchedulerConfig` pointing at an S3 path scoped to `test_name`.
///
/// `PartitionManager` uses OCC (optimistic concurrency control) which needs
/// conditional-put support (`PutMode::Update`); the local filesystem `ObjectStore`
/// does not support this, so S3 is required.
///
/// A UUID suffix ensures each test run starts with clean state, avoiding stale
/// partition assignments from previous runs routing queries to dead executors.
fn make_named_scheduler_config(test_name: &str) -> SchedulerConfig {
    let run_id = uuid::Uuid::new_v4();
    SchedulerConfig {
        state_location: format!(
            "s3://spiceai-integration-tests/cluster-state/{test_name}/{run_id}/"
        ),
        params: Some(spicepod::param::Params::from_string_map(
            std::collections::HashMap::from([
                ("s3_region".to_string(), "us-east-1".to_string()),
                (
                    "s3_key".to_string(),
                    "${env:AWS_S3_VECTORS_KEY}".to_string(),
                ),
                (
                    "s3_secret".to_string(),
                    "${env:AWS_S3_VECTORS_SECRET}".to_string(),
                ),
                ("s3_auth".to_string(), "key".to_string()),
            ]),
        )),
        partition_management: Some(PartitionManagement {
            interval: "1s".to_string(),
            ..Default::default()
        }),
    }
}
