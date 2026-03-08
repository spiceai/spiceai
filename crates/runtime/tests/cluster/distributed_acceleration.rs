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
use tokio::time::sleep;
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

/// Test that distributed acceleration with `bucket()` partitioning works end to end
/// with multiple executors.
///
/// Sets up a cluster with 1 scheduler + 1 executors accelerating data
/// with `partition_by: bucket(3, id)` using the Cayenne engine. Verifies:
/// - `bucket()` UDF can be used in the dataset definition for partitioning
/// - Queries return correct, complete results across all executors
/// - EXPLAIN plans correctly reflect the distributed execution plan
///
/// Each executor uses its own `cayenne_data_dir` and `cayenne_metadata_dir` to
/// avoid filesystem contention between in-process executors.
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
    std::fs::write(&csv_path, TEST_DATA_CSV).expect("write test data file");

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
                    scheduler: Some(make_scheduler_config()),
                    ..SpicepodRuntime::default()
                })
                .build();
            let harness = ClusterHarness::builder()
                .scheduler(app)
                .executors(1)
                .start()
                .await?;

            harness.wait_for_executors(Duration::from_secs(15)).await?;

            // Give executors time to load and accelerate their assigned partitions.
            sleep(Duration::from_secs(12)).await;

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
            |               |       FlightSqlExec sql=SELECT id, name, age, city, score FROM test_data WHERE bucket(3, "id") = '0' OR bucket(3, "id") = '1' OR bucket(3, "id") = '2'                                  |
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

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Return a `SchedulerConfig` pointing at an S3 bucket for partition state.
///
/// `PartitionManager` uses OCC (optimistic concurrency control) which needs
/// conditional-put support (`PutMode::Update`); the local filesystem `ObjectStore`
/// does not support this, so S3 is required.
///
/// A UUID suffix ensures each test run starts with clean state, avoiding stale
/// partition assignments from previous runs routing queries to dead executors.
fn make_scheduler_config() -> SchedulerConfig {
    let run_id = uuid::Uuid::new_v4();
    SchedulerConfig {
        state_location: format!(
            "s3://spiceai-integration-tests/cluster-state/test_distributed_acceleration_with_bucket_partitioning/{run_id}/"
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
