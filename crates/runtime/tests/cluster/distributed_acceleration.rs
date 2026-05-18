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
use spicepod::component::runtime::{Runtime as SpicepodRuntime, Scheduler as SchedulerConfig};
use spicepod::{
    acceleration::{Acceleration, Mode, RefreshMode},
    partitioning::PartitionedBy,
};
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::time::sleep;
use tracing_subscriber::EnvFilter;

use crate::{
    configure_test_datafusion,
    utils::{test_request_context, verify_env_secret_exists},
};

use super::harness::ClusterHarness;

/// Wrapper around [`insta::assert_snapshot!`] that redacts
/// `127.0.0.1:<port>` addresses in physical-plan output so that
/// snapshots are stable across runs.
macro_rules! assert_explain_snapshot {
    ($name:expr, $plan:expr) => {{
        let __plan = $plan;
        insta::with_settings!({
            filters => vec![
                (r"127\.0\.0\.1:\d+", "[endpoint]")
            ]
        }, {
            insta::assert_snapshot!($name, __plan);
        });
    }};
}

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

/// Secondary CSV for join tests: id + category + rating, matching `TEST_DATA_CSV` ids 1–10.
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

            tokio::time::sleep(Duration::from_secs(2)).await;
            harness.wait_for_executors(Duration::from_secs(15)).await?;

            // Wait for executors to load and accelerate their assigned partitions.
            wait_for_row_count(&harness, "test_data", 10, Duration::from_secs(60)).await?;

            // --- Test 1: SELECT all rows ---
            let select_all_sql = "SELECT id, name, age, city, score FROM test_data ORDER BY id";

            let plan = harness.explain(select_all_sql).await?;
            let plan_fmt = arrow::util::pretty::pretty_format_batches(&plan)
                .expect("format explain")
                .to_string();

            assert_explain_snapshot!("bucket_partitioning_plan", plan_fmt);

            let rows = harness.query(select_all_sql).await?;
            let rows_fmt = arrow::util::pretty::pretty_format_batches(&rows).expect("format rows");
            insta::assert_snapshot!("bucket_partitioning_rows", rows_fmt);

            // --- Test 2: Aggregation ---
            let aggregation_sql = "SELECT COUNT(*) as total_rows, AVG(score) as avg_score, \
                                   MIN(age) as min_age, MAX(age) as max_age FROM test_data";

            let agg_plan = harness.explain(aggregation_sql).await?;
            let agg_plan_fmt = arrow::util::pretty::pretty_format_batches(&agg_plan)
                .expect("format explain agg")
                .to_string();
            assert_explain_snapshot!("bucket_partitioning_agg_plan", agg_plan_fmt);

            let agg = harness.query(aggregation_sql).await?;
            let agg_fmt = arrow::util::pretty::pretty_format_batches(&agg).expect("format agg");
            insta::assert_snapshot!("bucket_partitioning_agg", agg_fmt);

            harness.shutdown().await;
            Ok(())
        })
        .await
}

/// Test that 2 executors with `bucket(4, id)` partitioning produce correct query results.
///
/// Verifies:
/// - Full SELECT returns all 10 rows without duplicates
/// - COUNT/AVG aggregations produce correct results
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
                    format!("file://{}", csv_path.display()),
                    "test_data",
                    4,
                    "id",
                ))
                .with_runtime(SpicepodRuntime {
                    scheduler: Some(
                        make_named_scheduler_config_with_max_partitions_per_executor(
                            "test_distributed_acceleration_multi_executor",
                            2,
                        ),
                    ),
                    ..SpicepodRuntime::default()
                })
                .build();

            let harness = ClusterHarness::builder()
                .scheduler(app)
                .executors(2)
                .start()
                .await?;

            sleep(Duration::from_secs(2)).await; // Ensure we get an initial partition assignment.
            harness.wait_for_executors(Duration::from_secs(15)).await?;
            wait_for_row_count(&harness, "test_data", 10, Duration::from_secs(60)).await?;

            // --- SELECT all rows ---
            let select_all_sql = "SELECT id, name, age, city, score FROM test_data ORDER BY id";

            let plan = harness.explain(select_all_sql).await?;
            let plan_fmt = arrow::util::pretty::pretty_format_batches(&plan)
                .expect("format explain")
                .to_string();
            // Plan structure is non-deterministic (partition-to-executor assignment varies),
            // so verify structural properties instead of an exact snapshot.
            assert!(
                plan_fmt.contains("Sort"),
                "plan should contain Sort operator"
            );
            assert!(
                plan_fmt.contains("FlightSqlExec"),
                "plan should use FlightSqlExec for distributed execution \n {plan_fmt}"
            );
            // Partition values are no longer injected as bucket filters because
            // executors only materialise data for their assigned partitions.
            // Verify that no redundant bucket filters appear in the plan.
            assert!(
                !plan_fmt.contains("bucket("),
                "plan should not contain bucket filters; executors already own only their assigned data"
            );

            let rows = harness.query(select_all_sql).await?;
            let rows_fmt = arrow::util::pretty::pretty_format_batches(&rows).expect("format rows");
            insta::assert_snapshot!("multi_executor_rows", rows_fmt);

            // --- Aggregation across both executors ---
            let agg_sql = "SELECT COUNT(*) as total_rows, AVG(score) as avg_score, \
                           MIN(age) as min_age, MAX(age) as max_age FROM test_data";

            let agg = harness.query(agg_sql).await?;
            let agg_fmt = arrow::util::pretty::pretty_format_batches(&agg).expect("format agg");
            insta::assert_snapshot!("multi_executor_agg", agg_fmt);

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
                    format!("file://{}", csv_path.display()),
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

            tokio::time::sleep(Duration::from_secs(2)).await;
            harness.wait_for_executors(Duration::from_secs(15)).await?;
            wait_for_row_count(&harness, "test_data", 10, Duration::from_secs(60)).await?;

            // The user predicate `score > 85` must be visible inside the FlightSqlExec
            // sql string — confirming it was pushed to the executor, not applied above.
            let filtered_sql = "SELECT id, name, score FROM test_data WHERE score > 85 ORDER BY id";

            let plan = harness.explain(filtered_sql).await?;
            let plan_fmt = arrow::util::pretty::pretty_format_batches(&plan)
                .expect("format explain")
                .to_string();
            assert_explain_snapshot!("predicate_pushdown_plan", plan_fmt);

            let rows = harness.query(filtered_sql).await?;
            let rows_fmt = arrow::util::pretty::pretty_format_batches(&rows).expect("format rows");
            insta::assert_snapshot!("predicate_pushdown_rows", rows_fmt);

            harness.shutdown().await;
            Ok(())
        })
        .await
}

/// Test that `ORDER BY col LIMIT N` is pushed down into each executor's `FlightSqlExec`
/// so each partition returns at most N rows (`TopK`)
#[tokio::test(flavor = "multi_thread")]
#[cfg(not(target_os = "windows"))]
async fn test_distributed_acceleration_order_by_limit_pushdown() -> Result<(), anyhow::Error> {
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
            let app = AppBuilder::new("test_order_limit_pushdown")
                .with_dataset(make_memory_accelerated_dataset(
                    format!("file://{}", csv_path.display()),
                    "test_data",
                    4,
                    "id",
                ))
                .with_runtime(SpicepodRuntime {
                    scheduler: Some(
                        make_named_scheduler_config_with_max_partitions_per_executor(
                            "test_distributed_acceleration_order_by_limit_pushdown",
                            2,
                        ),
                    ),
                    ..SpicepodRuntime::default()
                })
                .build();

            let harness = ClusterHarness::builder()
                .scheduler(app)
                .executors(2)
                .start()
                .await?;

            sleep(Duration::from_secs(2)).await;
            harness.wait_for_executors(Duration::from_secs(15)).await?;
            wait_for_row_count(&harness, "test_data", 10, Duration::from_secs(60)).await?;

            // --- ORDER BY score DESC LIMIT 3 ---
            // Expected top-3 scores from TEST_DATA_CSV: Sarah Wilson=94, Jane Smith=92, Anna Garcia=90
            let limit_sql = "SELECT id, name, score FROM test_data ORDER BY score DESC LIMIT 3";

            let plan = harness.explain(limit_sql).await?;
            let plan_fmt = arrow::util::pretty::pretty_format_batches(&plan)
                .expect("format explain")
                .to_string();

            assert_explain_snapshot!("order_by_limit_pushdown_plan", plan_fmt);

            let rows = harness.query(limit_sql).await?;
            let rows_fmt = arrow::util::pretty::pretty_format_batches(&rows).expect("format rows");
            insta::assert_snapshot!("order_by_limit_pushdown_rows", rows_fmt);

            // --- ORDER BY id ASC LIMIT 5 with predicate ---
            // Combines predicate pushdown with limit pushdown.
            // Rows with score > 80: ids 1(85), 2(92), 4(89), 6(94), 7(81), 8(88), 10(90) → 7 rows
            // LIMIT 5 → first 5 by id: 1, 2, 4, 6, 7
            let limit_pred_sql =
                "SELECT id, name, score FROM test_data WHERE score > 80 ORDER BY id ASC LIMIT 5";

            let pred_plan = harness.explain(limit_pred_sql).await?;
            let pred_plan_fmt = arrow::util::pretty::pretty_format_batches(&pred_plan)
                .expect("format explain")
                .to_string();

            assert_explain_snapshot!("order_by_limit_with_predicate_plan", pred_plan_fmt);

            let pred_rows = harness.query(limit_pred_sql).await?;
            let pred_rows_fmt =
                arrow::util::pretty::pretty_format_batches(&pred_rows).expect("format rows");
            insta::assert_snapshot!("order_by_limit_with_predicate_rows", pred_rows_fmt);

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
                    format!("file://{}", csv_path.display()),
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

            tokio::time::sleep(Duration::from_secs(2)).await;
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

            // Shut down executor[0].  The scheduler's PartitionAssignmentTask (1s interval)
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
            insta::assert_snapshot!("rebalance_rows", rows_fmt);

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
                    format!("file://{}", data_path.display()),
                    "test_data",
                    4,
                    "id",
                ))
                .with_dataset(make_memory_accelerated_dataset(
                    format!("file://{}", cat_path.display()),
                    "categories",
                    4,
                    "id",
                ))
                .with_runtime(SpicepodRuntime {
                    scheduler: Some({
                        let mut cfg = make_named_scheduler_config(
                            "test_distributed_acceleration_join_two_partitioned_tables",
                        );
                        // Limit each executor to 4 partitions (2 per table) so
                        // that partitions are forced to split across the 2 executors,
                        // producing a UnionExec in the query plan.
                        // Note: this is a global limit across all tables, so with
                        // 2 tables × 4 buckets we need at least 4 per executor.
                        cfg.partition_assignment_interval = "1s".to_string();
                        cfg.max_partitions_per_executor = 4;
                        // This is to avoid:
                        //  - 4 partitions of tableA -> executor1, then
                        //  - 4 partitions of tableB -> executor2
                        //
                        // We want executor1: 2 partitions of tableA, 2 partitions of tableB. (similar for executor2).
                        cfg.max_partition_assignments_per_interval = 2;
                        cfg
                    }),
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

            // Wait for partition metadata to be fully assigned across both
            // executors before querying. Without this, the scheduler may
            // route to a single executor producing a non-distributed plan.
            let partition_store = harness
                .scheduler
                .partition_store()
                .expect("scheduler should have partition store");

            for table_name in ["test_data", "categories"] {
                let table_ref = datafusion::sql::TableReference::parse_str(table_name);
                let assigned = crate::utils::wait_until_true(Duration::from_secs(30), || async {
                    partition_store.refresh().await.ok();
                    partition_store
                        .get_table_metadata(&table_ref)
                        .await
                        .ok()
                        .flatten()
                        .is_some_and(|m| {
                            m.partitions.len() == 4
                                && m.partitions
                                    .iter()
                                    .all(runtime::cluster::PartitionMetadata::is_assigned)
                        })
                })
                .await;
                assert!(
                    assigned,
                    "All 4 partitions for {table_name} should be assigned"
                );
            }

            let join_sql = "SELECT t.id, t.name, c.category, c.rating \
                            FROM test_data t JOIN categories c ON t.id = c.id \
                            ORDER BY t.id";

            let plan = harness.explain(join_sql).await?;
            let plan_fmt = arrow::util::pretty::pretty_format_batches(&plan)
                .expect("format explain")
                .to_string();

            assert_explain_snapshot!("join_plan", plan_fmt);

            let rows = harness.query(join_sql).await?;
            let rows_fmt = arrow::util::pretty::pretty_format_batches(&rows).expect("format rows");
            insta::assert_snapshot!("join_rows", rows_fmt);

            harness.shutdown().await;
            Ok(())
        })
        .await
}

/// Test that `refresh_table()` on the scheduler forwards the refresh command to executors
/// via the `RefreshDataset` control stream message, rather than failing with a
/// "channel closed" error.
///
/// Verifies:
/// - The scheduler detects it is in scheduler mode and forwards to executors
/// - Executors receive the `RefreshDataset` command and trigger a local refresh
/// - Data remains correct after the distributed refresh
#[tokio::test(flavor = "multi_thread")]
#[cfg(not(target_os = "windows"))]
async fn test_distributed_refresh_forwarding() -> Result<(), anyhow::Error> {
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
            let app = AppBuilder::new("test_refresh_forwarding")
                .with_dataset(make_memory_accelerated_dataset(
                    format!("file://{}", csv_path.display()),
                    "test_data",
                    3,
                    "id",
                ))
                .with_runtime(SpicepodRuntime {
                    scheduler: Some(make_named_scheduler_config(
                        "test_distributed_refresh_forwarding",
                    )),
                    ..SpicepodRuntime::default()
                })
                .build();

            let harness = ClusterHarness::builder()
                .scheduler(app)
                .executors(1)
                .start()
                .await?;

            tokio::time::sleep(Duration::from_secs(2)).await;

            harness.wait_for_executors(Duration::from_secs(15)).await?;
            wait_for_row_count(&harness, "test_data", 10, Duration::from_secs(60)).await?;

            // Trigger refresh from the scheduler. Previously this would fail with
            // "the refresh worker is no longer running. channel closed" because the
            // scheduler doesn't run local refresh workers. Now it forwards to executors.
            let table_ref = datafusion::sql::TableReference::parse_str("test_data");
            harness
                .scheduler
                .datafusion()
                .refresh_table(&table_ref, None)
                .await
                .expect("refresh_table on scheduler should forward to executors");

            // Allow time for the executor to process the refresh command.
            tokio::time::sleep(Duration::from_secs(2)).await;

            // Verify data is still correct after refresh.
            wait_for_row_count(&harness, "test_data", 10, Duration::from_secs(30)).await?;

            let rows = harness
                .query("SELECT COUNT(*) as cnt FROM test_data")
                .await?;
            let rows_fmt = arrow::util::pretty::pretty_format_batches(&rows).expect("format rows");
            insta::assert_snapshot!("refresh_forwarding_count", rows_fmt);

            harness.shutdown().await;
            Ok(())
        })
        .await
}

/// Test that on-demand refresh discovers genuinely new partition values from the
/// source, assigns them in the partition store, and makes the data queryable.
///
/// Uses `city` as a column-value partition (not `bucket()`), so each unique city
/// is its own partition value. The initial CSV has 10 cities. After the cluster
/// is running, we append a row with a new city ("Seattle") and trigger refresh.
///
/// Verifies:
/// 1. Before refresh: partition store has 10 partition values
/// 2. After refresh: partition store has 11 partition values (new city discovered + assigned)
/// 3. All 11 rows are queryable
#[tokio::test(flavor = "multi_thread")]
#[cfg(not(target_os = "windows"))]
async fn test_on_demand_refresh_discovers_new_partitions() -> Result<(), anyhow::Error> {
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

            // Partition by city (column value), not bucket — each unique city is a partition.
            let app = AppBuilder::new("test_refresh_discovers_partitions")
                .with_dataset(make_column_partitioned_dataset(
                    format!("file:{}", csv_path.display()),
                    "test_data",
                    "city",
                ))
                .with_runtime(SpicepodRuntime {
                    scheduler: Some(
                        make_named_scheduler_config_with_max_partitions_per_executor(
                            "test_on_demand_refresh_discovers_new_partitions",
                            20,
                        ),
                    ),
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

            // Wait for partition management cycle to discover and assign all partitions.
            // The cycle runs every 1s (configured in make_named_scheduler_config).
            let partition_store = harness
                .scheduler
                .partition_store()
                .expect("scheduler should have partition store");
            let table_ref = datafusion::sql::TableReference::parse_str("test_data");

            let partitions_assigned =
                crate::utils::wait_until_true(Duration::from_secs(30), || async {
                    partition_store.refresh().await.ok();
                    partition_store
                        .get_table_metadata(&table_ref)
                        .await
                        .ok()
                        .flatten()
                        .is_some_and(|m| {
                            m.partitions.len() == 10
                                && m.partitions
                                    .iter()
                                    .all(runtime::cluster::PartitionMetadata::is_assigned)
                        })
                })
                .await;
            assert!(
                partitions_assigned,
                "All 10 initial partitions should be discovered and assigned"
            );

            // Append a row with a NEW city that doesn't exist in the initial data.
            let new_row = "\n11,New Person,33,Seattle,87\n";
            tokio::fs::OpenOptions::new()
                .append(true)
                .open(&csv_path)
                .await
                .expect("open csv for append")
                .write_all(new_row.as_bytes())
                .await
                .expect("append new row");

            // Trigger on-demand refresh. PartitionService.discover_and_assign_for_table()
            // should discover "Seattle" as a new partition value, add it to the store,
            // assign it, and then forward the refresh to executors.
            harness
                .scheduler
                .datafusion()
                .refresh_table(&table_ref, None)
                .await
                .expect("refresh_table should succeed");

            // Verify partition store: should now have 11 partitions with Seattle present
            // and assigned. Use polling because S3 writes may not be immediately visible.
            let seattle_assigned =
                crate::utils::wait_until_true(Duration::from_secs(30), || async {
                    partition_store.refresh().await.ok();
                    partition_store
                        .get_table_metadata(&table_ref)
                        .await
                        .ok()
                        .flatten()
                        .is_some_and(|m| {
                            m.partitions.len() == 11
                                && m.partitions.iter().any(|p| {
                                    p.partition_value.values().any(|v| v.as_deref() == Some("Seattle"))
                                        && p.is_assigned()
                                })
                        })
                })
                .await;
            assert!(
                seattle_assigned,
                "Seattle partition should be discovered, added to store, and assigned"
            );

            // Wait for the executor to pick up the new partition and load the data.
            // The executor needs to receive the UpdatePartitions message, update its
            // partition filter, and then the next refresh will include Seattle.
            wait_for_row_count(&harness, "test_data", 11, Duration::from_secs(60)).await?;

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
    let mut last_count: Option<usize> = None;
    loop {
        if let Ok(batches) = harness
            .query(&format!("SELECT COUNT(*) AS cnt FROM {table}"))
            .await
        {
            for batch in &batches {
                if batch.num_rows() == 0 {
                    continue;
                }
                if let Some(arr) = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                {
                    #[expect(clippy::cast_sign_loss, reason = "COUNT(*) is always non-negative")]
                    #[expect(
                        clippy::cast_possible_truncation,
                        reason = "row count fits in usize on 64-bit"
                    )]
                    let count = arr.value(0) as usize;
                    last_count = Some(count);
                    if count == expected {
                        return Ok(());
                    }
                }
            }
        }
        if start.elapsed() > timeout {
            return Err(anyhow::anyhow!(
                "Timed out waiting for {table} to have {expected} rows (last count: {last_count:?})"
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

/// Create a dataset partitioned by a raw column value (not `bucket()`).
/// Each unique value of `partition_column` becomes its own partition.
fn make_column_partitioned_dataset(
    source_path: impl Into<String>,
    name: &str,
    partition_column: &str,
) -> Dataset {
    let mut dataset = Dataset::new(source_path, name);

    dataset.acceleration = Some(Acceleration {
        enabled: true,
        mode: Mode::Memory,
        refresh_mode: Some(RefreshMode::Full),
        partition_by: vec![PartitionedBy {
            name: partition_column.to_string(),
            expression: partition_column.to_string(),
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
    make_named_scheduler_config_with_max_partitions_per_executor(test_name, 10)
}

fn make_named_scheduler_config_with_max_partitions_per_executor(
    test_name: &str,
    max_partitions_per_executor: usize,
) -> SchedulerConfig {
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
        partition_assignment_interval: "1s".to_string(),
        max_partition_assignments_per_interval:
            spicepod::component::runtime::default_max_partition_assignments_per_interval(),
        max_partitions_per_executor,
        partition_discovery_timeout:
            spicepod::component::runtime::default_partition_discovery_timeout(),
    }
}
