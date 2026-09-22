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

//! Guards the `datafusion-ballista` fork's per-partition file-scan restriction
//! (fork PR #57, porting `apache/datafusion-ballista#1907`):
//! `restrict_scan_to_partition` in `ballista/executor/src/execution_engine.rs`.
//!
//! Ballista runs one partition of a file-backed scan per task, each on its own
//! plan instance in its own process. Without the restriction, a task's lone
//! stream drains the scan's shared file work-queue by itself instead of only
//! the file group its `partition_id` was assigned, so it reads every file in
//! the table rather than just its own. Every task's shuffle output is then
//! summed downstream, so the query reports success with an aggregate inflated
//! by the number of tasks that read the leaf scan.
//!
//! This distributes a plain (non-accelerated) three-file dataset across two
//! executors, running `target_partitions = 3` (`configure_test_datafusion`)
//! so the scan is split into three file groups — more file groups than any
//! one executor can serve alone, which is what forces a real cross-process
//! task split rather than a single in-process scan that would read the same
//! files correctly either way. `COUNT(*)` and `SUM(id)` are exact sums over
//! disjoint files, so an over-read by any task shows up as a multiple of the
//! correct total rather than a rounding difference.

use app::AppBuilder;
use arrow::array::{AsArray, RecordBatch};
use arrow::datatypes::Int64Type;
use futures::TryStreamExt;
use runtime::datafusion::query::QueryBuilder;
use spicepod::component::dataset::Dataset;
use spicepod::component::runtime::{Runtime as SpicepodRuntime, Scheduler as SchedulerConfig};
use std::time::Duration;
use tokio::time::sleep;

use crate::{configure_test_datafusion, utils::test_request_context};

use super::harness::ClusterHarness;

/// Three files, disjoint id ranges, uneven row counts so an N-fold over-read
/// cannot coincide with a plausible correct answer.
const FILE_A: &str = "id,city\n1,Anchorage\n2,Boise\n3,Chicago\n4,Denver\n";
const FILE_B: &str = "id,city\n5,Erie\n6,Fargo\n7,Gary\n8,Helena\n9,Irving\n10,Juneau\n11,Kenai\n";
const FILE_C: &str = "id,city\n12,Laredo\n13,Miami\n14,Nashua\n15,Omaha\n16,Provo\n";

const TOTAL_ROWS: i64 = 16;
const TOTAL_ID_SUM: i64 = 16 * (16 + 1) / 2;

/// Local scheduler state directory, avoiding the S3 partition store — this
/// dataset carries no acceleration or partitioning of its own, so there is no
/// partition-assignment metadata to persist.
fn local_scheduler_config(state_dir: &std::path::Path) -> SchedulerConfig {
    SchedulerConfig {
        state_location: format!("file://{}", state_dir.display()),
        params: None,
        partition_assignment_interval: "1s".to_string(),
        max_partition_assignments_per_interval:
            spicepod::component::runtime::default_max_partition_assignments_per_interval(),
        max_partitions_per_executor: 10,
        partition_discovery_timeout:
            spicepod::component::runtime::default_partition_discovery_timeout(),
    }
}

fn single_i64(rows: &[RecordBatch]) -> i64 {
    assert_eq!(
        rows.len(),
        1,
        "expected one result batch, got {}",
        rows.len()
    );
    let batch = &rows[0];
    assert_eq!(
        batch.num_rows(),
        1,
        "expected one row, got {}",
        batch.num_rows()
    );
    batch.column(0).as_primitive::<Int64Type>().value(0)
}

/// Submit `sql` as a distributed Ballista job against the scheduler and
/// collect its results.
async fn run_distributed(
    harness: &ClusterHarness,
    sql: &str,
    job_name: &str,
) -> Result<Vec<RecordBatch>, anyhow::Error> {
    let handle = QueryBuilder::new(sql, harness.scheduler.datafusion())
        .build()
        .submit_distributed(job_name)
        .await
        .map_err(|e| anyhow::Error::msg(format!("submit_distributed failed: {e}")))?;
    handle
        .into_stream()
        .await
        .map_err(|e| anyhow::Error::msg(format!("into_stream failed: {e}")))?
        .try_collect::<Vec<RecordBatch>>()
        .await
        .map_err(|e| anyhow::Error::msg(format!("collect failed: {e}")))
}

/// Regression guard for the ballista fork's `restrict_scan_to_partition` /
/// `restrict_scans` (fork PR #57): a distributed aggregate over a
/// multi-file, non-accelerated dataset must return the exact row count and
/// id sum, not a multiple of them.
#[tokio::test(flavor = "multi_thread")]
#[cfg(not(target_os = "windows"))]
async fn distributed_scan_reads_each_task_its_own_file_group() -> Result<(), anyhow::Error> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new("runtime=info,warn"))
        .with_ansi(true)
        .try_init();

    let data_dir = tempfile::tempdir().expect("data tempdir");
    std::fs::write(data_dir.path().join("a.csv"), FILE_A).expect("write file a");
    std::fs::write(data_dir.path().join("b.csv"), FILE_B).expect("write file b");
    std::fs::write(data_dir.path().join("c.csv"), FILE_C).expect("write file c");

    let state_dir = tempfile::tempdir().expect("state tempdir");

    test_request_context()
        .scope(async {
            configure_test_datafusion();

            let mut dataset =
                Dataset::new(format!("file:{}/", data_dir.path().display()), "orders");
            dataset.params = Some(spicepod::param::Params::from_string_map(
                std::collections::HashMap::from([("file_format".to_string(), "csv".to_string())]),
            ));

            let app = AppBuilder::new("ballista_partition_scoped_scan")
                .with_dataset(dataset)
                .with_runtime(SpicepodRuntime {
                    scheduler: Some(local_scheduler_config(state_dir.path())),
                    ..SpicepodRuntime::default()
                })
                .build();

            let harness = ClusterHarness::builder()
                .scheduler(app)
                .executors(2)
                .start()
                .await?;

            harness.wait_for_executors(Duration::from_secs(15)).await?;
            // Give the scheduler a moment to observe both executors' task
            // slots before planning, so the leaf scan's tasks land on both
            // rather than racing capacity propagation onto only one.
            sleep(Duration::from_secs(2)).await;

            let plan_rows = harness
                .explain("SELECT COUNT(*) FROM orders")
                .await
                .map_err(|e| anyhow::Error::msg(format!("explain failed: {e}")))?;
            let plan_fmt = arrow::util::pretty::pretty_format_batches(&plan_rows)
                .expect("format explain")
                .to_string();
            assert!(
                !plan_fmt.contains("file_groups={1 group"),
                "expected the scan to plan with more than one file group, so the leaf \
                 stage has more than one task to distribute across executors, got:\n{plan_fmt}"
            );

            let count_rows = run_distributed(
                &harness,
                "SELECT COUNT(*) AS cnt FROM orders",
                "ballista_partition_scoped_scan_count",
            )
            .await?;
            let count = single_i64(&count_rows);
            assert_eq!(
                count, TOTAL_ROWS,
                "COUNT(*) over a 3-file, 2-executor distributed scan came back as \
                 {count}, not the exact {TOTAL_ROWS} rows across the three files — a task \
                 reading every file group instead of only its own inflates this by an \
                 integer multiple of the true count"
            );

            let sum_rows = run_distributed(
                &harness,
                "SELECT SUM(id) AS total FROM orders",
                "ballista_partition_scoped_scan_sum",
            )
            .await?;
            let sum = single_i64(&sum_rows);
            assert_eq!(
                sum, TOTAL_ID_SUM,
                "SUM(id) over a 3-file, 2-executor distributed scan came back as {sum}, \
                 not the exact {TOTAL_ID_SUM} — a task reading every file group instead of \
                 only its own inflates this by an integer multiple of the true sum"
            );

            harness.shutdown().await;
            Ok(())
        })
        .await
}
