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
            wait_for_row_count(&harness, "test_data", 10, Duration::from_mins(1)).await?;

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

/// Regression: same as `test_distributed_acceleration_with_bucket_partitioning`, but the
/// source is a FEDERATED connector (`DuckDB`, a `PolyTableProvider`) instead of a local CSV
/// file — the cloud shape (federated source + cayenne partitioned acceleration). The
/// federation wrapper must not hide the inner `AcceleratedTable` from `should_partition`;
/// the query must still distribute (`FlightSqlExec`).
#[tokio::test(flavor = "multi_thread")]
// Reads the source through the installed DuckDB library, which the `duckdb:`
// connector loads over ADBC.
#[cfg(feature = "adbc")]
async fn cluster_distributes_accelerated_table_with_federated_source() -> Result<(), anyhow::Error>
{
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new("runtime=info,warn"))
        .with_ansi(true)
        .try_init();

    // Build a local DuckDB database file to act as the federated source.
    let duck_tempdir = tempfile::tempdir().expect("duckdb tempdir");
    let db_path = duck_tempdir.path().join("source.duckdb");
    {
        let conn = duckdb_oracle::Connection::open(&db_path).expect("open duckdb");
        conn.execute_batch(
            "CREATE TABLE test_data (id BIGINT, name VARCHAR, age BIGINT, city VARCHAR, score BIGINT);
             INSERT INTO test_data VALUES
             (1,'John Doe',28,'New York',85),(2,'Jane Smith',34,'Los Angeles',92),
             (3,'Mike Johnson',45,'Chicago',78),(4,'Emily Brown',31,'Houston',89),
             (5,'David Lee',39,'Phoenix',76),(6,'Sarah Wilson',26,'Philadelphia',94),
             (7,'Tom Anderson',52,'San Antonio',81),(8,'Lisa Taylor',29,'San Diego',88),
             (9,'Chris Martin',37,'Dallas',79),(10,'Anna Garcia',41,'San Jose',90);",
        )
        .expect("populate duckdb");
    }

    let cayenne_tempdir = tempfile::tempdir().expect("cayenne tempdir");
    let state_tempdir = tempfile::tempdir().expect("state tempdir");

    test_request_context()
        .scope(async {
            configure_test_datafusion();
            crate::utils::register_test_connectors().await;

            // Cayenne file-mode acceleration with bucket(3, id), but federated DuckDB source.
            let mut dataset = make_accelerated_dataset(
                "duckdb:test_data",
                "test_data",
                3,
                "id",
                cayenne_tempdir.path(),
            );
            dataset.params = Some(spicepod::param::Params::from_string_map(
                std::collections::HashMap::from([(
                    "duckdb_open".to_string(),
                    db_path.display().to_string(),
                )]),
            ));

            // Local-filesystem cluster state (avoids the S3 partition store the other
            // cluster tests use, so this runs hermetically without AWS creds).
            let scheduler_cfg = SchedulerConfig {
                state_location: Some(format!("file://{}", state_tempdir.path().display())),
                params: None,
                partition_assignment_interval: "1s".to_string(),
                max_partition_assignments_per_interval:
                    spicepod::component::runtime::default_max_partition_assignments_per_interval(),
                max_partitions_per_executor: 10,
                partition_discovery_timeout:
                    spicepod::component::runtime::default_partition_discovery_timeout(),
            };

            let app = AppBuilder::new("repro_federated_source")
                .with_dataset(dataset)
                .with_runtime(SpicepodRuntime {
                    scheduler: Some(scheduler_cfg),
                    ..SpicepodRuntime::default()
                })
                .build();

            let harness = ClusterHarness::builder()
                .scheduler(app)
                .executors(1)
                .start()
                .await?;

            sleep(Duration::from_secs(2)).await;
            harness.wait_for_executors(Duration::from_secs(15)).await?;
            wait_for_row_count(&harness, "test_data", 10, Duration::from_mins(1)).await?;
            // Give the executor time to finish loading + acking its assigned partitions.
            sleep(Duration::from_secs(8)).await;

            let select_all_sql = "SELECT id, name, age, city, score FROM test_data ORDER BY id";
            let plan = harness.explain(select_all_sql).await?;
            let plan_fmt = arrow::util::pretty::pretty_format_batches(&plan)
                .expect("format explain")
                .to_string();

            let distributes = plan_fmt.contains("FlightSqlExec");
            harness.shutdown().await;

            assert!(
                distributes,
                "federated-source accelerated table must distribute to executors \
                 (expected FlightSqlExec), but the coordinator federated to the source:\n{plan_fmt}"
            );
            Ok(())
        })
        .await
}

/// Regression: a clustered cayenne-accelerated dataset that carries column metadata
/// (descriptions) — like the cloud `amazon_reviews_accelerated` — must still distribute to
/// executors. Column/table metadata makes `register_table` wrap the `AcceleratedTable` in a
/// `MetadataEnrichedTableProvider`; the coordinator's `should_partition` must see through that
/// wrapper (via `find_concrete_table_provider`), otherwise it silently federates the read to
/// the source instead of distributing. This is identical to `bucket_partitioning_plan` EXCEPT
/// the dataset has a column description.
#[tokio::test(flavor = "multi_thread")]
async fn cluster_distributes_accelerated_table_with_column_metadata() -> Result<(), anyhow::Error> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new("runtime=info,warn"))
        .with_ansi(true)
        .try_init();

    let csv_tempdir = tempfile::tempdir().expect("csv tempdir");
    let csv_path = csv_tempdir.path().join("test_data.csv");
    tokio::fs::write(&csv_path, TEST_DATA_CSV)
        .await
        .expect("write test data");
    let cayenne_tempdir = tempfile::tempdir().expect("cayenne tempdir");
    let state_tempdir = tempfile::tempdir().expect("state tempdir");

    test_request_context()
        .scope(async {
            configure_test_datafusion();
            crate::utils::register_test_connectors().await;

            let mut dataset = make_accelerated_dataset(
                format!("file://{}", csv_path.display()),
                "test_data",
                3,
                "id",
                cayenne_tempdir.path(),
            );
            // THE ONLY DIFFERENCE vs the passing bucket_partitioning test: column descriptions.
            let mut id_col = spicepod::semantic::Column::new("id");
            id_col.description = Some("the row identifier".to_string());
            dataset.columns = vec![id_col];

            let scheduler_cfg = SchedulerConfig {
                state_location: Some(format!("file://{}", state_tempdir.path().display())),
                params: None,
                partition_assignment_interval: "1s".to_string(),
                max_partition_assignments_per_interval:
                    spicepod::component::runtime::default_max_partition_assignments_per_interval(),
                max_partitions_per_executor: 10,
                partition_discovery_timeout:
                    spicepod::component::runtime::default_partition_discovery_timeout(),
            };

            let app = AppBuilder::new("repro_column_metadata")
                .with_dataset(dataset)
                .with_runtime(SpicepodRuntime {
                    scheduler: Some(scheduler_cfg),
                    ..SpicepodRuntime::default()
                })
                .build();

            let harness = ClusterHarness::builder()
                .scheduler(app)
                .executors(1)
                .start()
                .await?;

            sleep(Duration::from_secs(2)).await;
            harness.wait_for_executors(Duration::from_secs(15)).await?;
            wait_for_row_count(&harness, "test_data", 10, Duration::from_mins(1)).await?;
            sleep(Duration::from_secs(5)).await;

            let select_all_sql = "SELECT id, name, age, city, score FROM test_data ORDER BY id";
            let plan = harness.explain(select_all_sql).await?;
            let plan_fmt = arrow::util::pretty::pretty_format_batches(&plan)
                .expect("format explain")
                .to_string();
            let rows = harness.query(select_all_sql).await?;
            let row_count: usize = rows.iter().map(arrow::array::RecordBatch::num_rows).sum();

            let distributes = plan_fmt.contains("FlightSqlExec");
            harness.shutdown().await;

            assert_eq!(row_count, 10, "query should return all 10 rows");
            assert!(
                distributes,
                "accelerated table with column metadata must distribute to executors \
                 (expected FlightSqlExec), but the coordinator federated to the source:\n{plan_fmt}"
            );
            Ok(())
        })
        .await
}

/// The only executor may hold one partition, so the scheduler leaves the others unassigned.
/// A query on the table must fail: the scheduler holds none of the data, and an answer from
/// the assigned partition alone would be wrong.
#[tokio::test(flavor = "multi_thread")]
async fn query_fails_when_a_partition_has_no_ready_executor() -> Result<(), anyhow::Error> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new("runtime=info,warn"))
        .with_ansi(true)
        .try_init();

    let csv_tempdir = tempfile::tempdir().expect("csv tempdir");
    let cayenne_tempdir = tempfile::tempdir().expect("cayenne tempdir");
    let csv_path = csv_tempdir.path().join("test_data.csv");
    tokio::fs::write(&csv_path, TEST_DATA_CSV)
        .await
        .expect("write test data");
    let state_tempdir = tempfile::tempdir().expect("state tempdir");

    test_request_context()
        .scope(async {
            configure_test_datafusion();

            let app = AppBuilder::new("partition_without_executor")
                .with_dataset(make_accelerated_dataset(
                    format!("file://{}", csv_path.display()),
                    "test_data",
                    3,
                    "id",
                    cayenne_tempdir.path(),
                ))
                .with_runtime(SpicepodRuntime {
                    scheduler: Some(SchedulerConfig {
                        state_location: Some(format!(
                            "file://{}",
                            state_tempdir.path().display()
                        )),
                        params: None,
                        partition_assignment_interval: "1s".to_string(),
                        max_partition_assignments_per_interval:
                            spicepod::component::runtime::default_max_partition_assignments_per_interval(),
                        max_partitions_per_executor: 1,
                        partition_discovery_timeout:
                            spicepod::component::runtime::default_partition_discovery_timeout(),
                    }),
                    ..SpicepodRuntime::default()
                })
                .build();

            let harness = ClusterHarness::builder()
                .scheduler(app)
                .executors(1)
                .start()
                .await?;
            harness.wait_for_executors(Duration::from_secs(15)).await?;
            let start = std::time::Instant::now();
            while harness
                .partition_owner_counts("test_data")
                .values()
                .sum::<usize>()
                == 0
            {
                if start.elapsed() > Duration::from_mins(1) {
                    anyhow::bail!("the scheduler assigned no partition of test_data");
                }
                sleep(Duration::from_millis(200)).await;
            }

            let result = harness
                .query("SELECT COUNT(*) AS cnt FROM test_data")
                .await;
            harness.shutdown().await;

            match result {
                Ok(batches) => anyhow::bail!(
                    "the query should fail while partitions of test_data have no executor, but returned:\n{}",
                    arrow::util::pretty::pretty_format_batches(&batches)?
                ),
                Err(error) => assert!(
                    error.to_string().contains("no ready executor serves"),
                    "unexpected error: {error}"
                ),
            }
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
    let cayenne_tempdir = tempfile::tempdir().expect("cayenne tempdir");
    let csv_path = csv_tempdir.path().join("test_data.csv");
    tokio::fs::write(&csv_path, TEST_DATA_CSV)
        .await
        .expect("write test data");

    test_request_context()
        .scope(async {
            configure_test_datafusion();
            let app = AppBuilder::new("test_predicate_pushdown_accel")
                .with_dataset(make_accelerated_dataset(
                    format!("file://{}", csv_path.display()),
                    "test_data",
                    3,
                    "id",
                    cayenne_tempdir.path(),
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
            wait_for_row_count(&harness, "test_data", 10, Duration::from_mins(1)).await?;

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

/// Test that `refresh_table()` on the scheduler forwards the refresh command to executors
/// via the `RefreshDataset` control stream message, rather than failing with a
/// "channel closed" error.
///
/// Verifies:
/// - The scheduler detects it is in scheduler mode and forwards to executors
/// - Executors receive the `RefreshDataset` command and trigger a local refresh
/// - Data remains correct after the distributed refresh
#[tokio::test(flavor = "multi_thread")]
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
    let cayenne_tempdir = tempfile::tempdir().expect("cayenne tempdir");
    let csv_path = csv_tempdir.path().join("test_data.csv");
    tokio::fs::write(&csv_path, TEST_DATA_CSV)
        .await
        .expect("write test data");

    test_request_context()
        .scope(async {
            configure_test_datafusion();
            let app = AppBuilder::new("test_refresh_forwarding")
                .with_dataset(make_accelerated_dataset(
                    format!("file://{}", csv_path.display()),
                    "test_data",
                    3,
                    "id",
                    cayenne_tempdir.path(),
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
            wait_for_row_count(&harness, "test_data", 10, Duration::from_mins(1)).await?;

            // Trigger refresh from the scheduler. Previously this would fail with
            // "the refresh worker is no longer running. channel closed" because the
            // scheduler doesn't run local refresh workers. Now it forwards to executors.
            let table_ref = datafusion::common::TableReference::parse_str("test_data");
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
    let cayenne_tempdir = tempfile::tempdir().expect("cayenne tempdir");
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
                    cayenne_tempdir.path(),
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
            wait_for_row_count(&harness, "test_data", 10, Duration::from_mins(1)).await?;

            // Wait for partition management cycle to discover and assign all partitions.
            // The cycle runs every 1s (configured in make_named_scheduler_config).
            let partition_store = harness
                .scheduler
                .partition_store()
                .expect("scheduler should have partition store");
            let table_ref = datafusion::common::TableReference::parse_str("test_data");

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
                                    p.partition_value
                                        .values()
                                        .any(|v| v.as_deref() == Some("Seattle"))
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
            wait_for_row_count(&harness, "test_data", 11, Duration::from_mins(1)).await?;

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
    make_cayenne_partitioned_dataset(
        source_path,
        name,
        PartitionedBy {
            name: "expr0".to_string(),
            expression: format!("bucket({num_buckets}, {partition_column})"),
        },
        base_dir,
    )
}

/// A Cayenne file-mode acceleration under `base_dir`, partitioned by `partition_by`.
/// Cayenne refuses `partition_by` in memory mode, so every partitioned Cayenne test
/// here uses file mode.
fn make_cayenne_partitioned_dataset(
    source_path: impl Into<String>,
    name: &str,
    partition_by: PartitionedBy,
    base_dir: &std::path::Path,
) -> Dataset {
    let mut dataset = Dataset::new(source_path, name);

    dataset.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some("cayenne".to_string()),
        mode: Mode::File,
        refresh_mode: Some(RefreshMode::Full),
        partition_by: vec![partition_by],
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

/// Create a dataset with Cayenne file-mode acceleration, partitioned by a raw column value
/// (not `bucket()`): each unique value of `partition_column` becomes its own partition.
/// `base_dir` is used as in [`make_accelerated_dataset`].
fn make_column_partitioned_dataset(
    source_path: impl Into<String>,
    name: &str,
    partition_column: &str,
    base_dir: &std::path::Path,
) -> Dataset {
    make_cayenne_partitioned_dataset(
        source_path,
        name,
        PartitionedBy {
            name: partition_column.to_string(),
            expression: partition_column.to_string(),
        },
        base_dir,
    )
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
    let mut params = std::collections::HashMap::from([
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
    ]);
    params.extend(crate::utils::aws_session_token_param(
        "s3_session_token",
        "AWS_S3_VECTORS_SESSION_TOKEN",
    ));
    SchedulerConfig {
        state_location: Some(format!(
            "s3://spiceai-ci-integration-tests/cluster-state/{test_name}/{run_id}/"
        )),
        params: Some(spicepod::param::Params::from_string_map(params)),
        partition_assignment_interval: "1s".to_string(),
        max_partition_assignments_per_interval:
            spicepod::component::runtime::default_max_partition_assignments_per_interval(),
        max_partitions_per_executor,
        partition_discovery_timeout:
            spicepod::component::runtime::default_partition_discovery_timeout(),
    }
}
