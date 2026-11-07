/*
Copyright 2024-2026 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

    10|Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

//! Two-executor cluster regression for checked integer `SUM`.
//!
//! `FlightSQLPartialAggregatePushdown` matches the registry name `sum` and used
//! to send `SUM(col)` to each executor. Production `CheckedSum` then narrowed
//! to `Int64` before the scheduler merged, so a partition `[MAX, MAX]` errored
//! even when another executor held `[-MAX]` and the true total was `MAX`.
//!
//! The remote SQL is now `SUM(CAST(col AS DECIMAL(38,0)))`. This file drives
//! that path with two executors, real Flight SQL, response remapping, and the
//! production accumulator — not a stand-in UDAF or precomputed memory batches.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use app::AppBuilder;
use arrow::array::{AsArray, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Int64Type, Schema};
use datafusion::parquet::arrow::ArrowWriter;
use futures::TryStreamExt;
use runtime::Runtime;
use runtime::datafusion::query::QueryBuilder;
use spicepod::acceleration::{Acceleration, Mode, RefreshMode};
use spicepod::component::dataset::Dataset;
use spicepod::component::runtime::{Runtime as SpicepodRuntime, Scheduler as SchedulerConfig};
use spicepod::partitioning::PartitionedBy;

use crate::{
    configure_test_datafusion, init_tracing,
    utils::{test_request_context, wait_until_true},
};

use super::harness::ClusterHarness;

const TABLE: &str = "t";
const SUM_SQL: &str = "SELECT SUM(v) FROM t";
const WIDE_SUM_SQL: &str = r#"SUM(CAST("v" AS DECIMAL(38,0)))"#;

fn local_scheduler_config(state_dir: &std::path::Path) -> SchedulerConfig {
    SchedulerConfig {
        state_location: Some(format!("file://{}", state_dir.display())),
        params: None,
        partition_assignment_interval: "1s".to_string(),
        max_partition_assignments_per_interval:
            spicepod::component::runtime::default_max_partition_assignments_per_interval(),
        // One partition per executor so `[MAX, MAX]` and `[-MAX]` cannot land
        // on the same node (that would hide the inter-executor cancel).
        max_partitions_per_executor: 1,
        partition_discovery_timeout:
            spicepod::component::runtime::default_partition_discovery_timeout(),
    }
}

fn write_partitioned_parquet(path: &std::path::Path, parts: &[(i64, Vec<i64>)]) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("part", DataType::Int64, false),
        Field::new("v", DataType::Int64, true),
    ]));
    let mut part_col = Vec::new();
    let mut value_col = Vec::new();
    for (part, values) in parts {
        for value in values {
            part_col.push(*part);
            value_col.push(Some(*value));
        }
    }
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(part_col)),
            Arc::new(Int64Array::from(value_col)),
        ],
    )
    .expect("partitioned overflow batch");
    let file = std::fs::File::create(path).expect("create parquet");
    let mut writer = ArrowWriter::try_new(file, schema, None).expect("parquet writer");
    writer.write(&batch).expect("write parquet batch");
    writer.close().expect("close parquet writer");
}

fn partitioned_dataset(parquet_path: &std::path::Path) -> Dataset {
    let mut dataset = Dataset::new(format!("file:{}", parquet_path.display()), TABLE);
    dataset.params = Some(spicepod::param::Params::from_string_map(HashMap::from([(
        "file_format".to_string(),
        "parquet".to_string(),
    )])));
    dataset.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some("arrow".to_string()),
        mode: Mode::Memory,
        refresh_mode: Some(RefreshMode::Full),
        partition_by: vec![PartitionedBy {
            name: "part".to_string(),
            expression: "part".to_string(),
        }],
        ..Acceleration::default()
    });
    dataset
}

async fn query_runtime(rt: &Runtime, sql: &str) -> Result<Vec<RecordBatch>, anyhow::Error> {
    QueryBuilder::new(sql, rt.datafusion())
        .build()
        .run()
        .await
        .map_err(|e| anyhow::Error::msg(format!("query `{sql}` failed: {e}")))?
        .data
        .try_collect::<Vec<_>>()
        .await
        .map_err(|e| anyhow::Error::msg(format!("query `{sql}` stream failed: {e}")))
}

fn single_i64(rows: &[RecordBatch]) -> i64 {
    let batch = rows.first().expect("one result batch");
    assert_eq!(batch.num_rows(), 1, "expected one row, got {batch:?}");
    batch.column(0).as_primitive::<Int64Type>().value(0)
}

fn format_batches(rows: &[RecordBatch]) -> String {
    arrow::util::pretty::pretty_format_batches(rows)
        .expect("format batches")
        .to_string()
}

fn overflow_error(error: &anyhow::Error) -> bool {
    error.to_string().to_ascii_lowercase().contains("overflow")
}

/// Start a two-executor cluster whose arrow-accelerated table is partitioned
/// by `part`, with each executor owning exactly one partition.
async fn start_two_executor_cluster(
    parts: &[(i64, Vec<i64>)],
    expected_rows: usize,
) -> Result<(ClusterHarness, tempfile::TempDir, tempfile::TempDir), anyhow::Error> {
    let data_dir = tempfile::tempdir().expect("data tempdir");
    let state_dir = tempfile::tempdir().expect("state tempdir");
    let parquet_path = data_dir.path().join("t.parquet");
    write_partitioned_parquet(&parquet_path, parts);

    let mut app = AppBuilder::new("cluster_checked_integer_sum")
        .with_dataset(partitioned_dataset(&parquet_path))
        .with_runtime(SpicepodRuntime {
            scheduler: Some(local_scheduler_config(state_dir.path())),
            ..SpicepodRuntime::default()
        })
        .build();
    app.runtime
        .caching
        .sql_results
        .get_or_insert_default()
        .enabled = false;

    let harness = ClusterHarness::builder()
        .scheduler(app)
        .executors(2)
        .start()
        .await?;
    harness.wait_for_executors(Duration::from_secs(30)).await?;

    let assigned = wait_until_true(Duration::from_secs(60), || {
        let counts = harness.partition_owner_counts(TABLE);
        let total: usize = counts.values().sum();
        async move { counts.len() == 2 && total == 2 && counts.values().all(|&n| n == 1) }
    })
    .await;
    let owner_counts = harness.partition_owner_counts(TABLE);
    anyhow::ensure!(
        assigned,
        "expected each of 2 executors to own 1 partition of {TABLE}, got {owner_counts:?}"
    );

    wait_for_row_count(&harness, expected_rows, Duration::from_secs(60)).await?;
    Ok((harness, data_dir, state_dir))
}

async fn wait_for_row_count(
    harness: &ClusterHarness,
    expected: usize,
    timeout: Duration,
) -> Result<(), anyhow::Error> {
    let expected_i64 = i64::try_from(expected).expect("row count fits i64");
    let ready = wait_until_true(timeout, || async {
        harness
            .query(&format!("SELECT COUNT(*) FROM {TABLE}"))
            .await
            .is_ok_and(|rows| single_i64(&rows) == expected_i64)
    })
    .await;
    if ready {
        return Ok(());
    }
    let last = match harness
        .query(&format!("SELECT COUNT(*) FROM {TABLE}"))
        .await
    {
        Ok(rows) => Some(single_i64(&rows)),
        Err(error) => {
            anyhow::bail!(
                "timed out waiting for {TABLE} to have {expected} rows (last query error: {error})"
            )
        }
    };
    anyhow::bail!("timed out waiting for {TABLE} to have {expected} rows (last count: {last:?})");
}

async fn wait_for_wide_two_executor_plan(
    harness: &ClusterHarness,
) -> Result<String, anyhow::Error> {
    let ready = wait_until_true(Duration::from_secs(30), || async {
        harness
            .explain(SUM_SQL)
            .await
            .is_ok_and(|rows| plan_is_wide_two_executor(&format_batches(&rows)))
    })
    .await;
    let plan = format_batches(&harness.explain(SUM_SQL).await?);
    anyhow::ensure!(
        ready && plan_is_wide_two_executor(&plan),
        "expected two PartialAggregationFlightSqlExec nodes and {WIDE_SUM_SQL}, got:\n{plan}"
    );
    anyhow::ensure!(
        !plan.contains(r#"sql=SELECT SUM("v")"#),
        "plain SUM(\"v\") would narrow on each executor, got:\n{plan}"
    );
    Ok(plan)
}

fn plan_is_wide_two_executor(plan: &str) -> bool {
    plan.matches("PartialAggregationFlightSqlExec").count() == 2 && plan.contains(WIDE_SUM_SQL)
}

/// Each executor's local `SUM(v)` runs production `CheckedSum` on its own
/// partition (no remapping). Returns the successful totals; overflow errors
/// are counted separately.
async fn local_executor_sums(harness: &ClusterHarness) -> Result<(Vec<i64>, usize), anyhow::Error> {
    let mut totals = Vec::new();
    let mut overflows = 0;
    for (index, executor) in harness.executors.iter().enumerate() {
        match query_runtime(executor, SUM_SQL).await {
            Ok(rows) => totals.push(single_i64(&rows)),
            Err(error) if overflow_error(&error) => overflows += 1,
            Err(error) => {
                return Err(anyhow::Error::msg(format!(
                    "executor{index} SUM(v) failed without overflow: {error}"
                )));
            }
        }
    }
    Ok((totals, overflows))
}

/// Two executors, partitions `[MAX, MAX]` and `[-MAX]`: local `SUM` on the
/// first executor overflows, the scheduler-wide `SUM` is `MAX`.
#[tokio::test(flavor = "multi_thread")]
async fn cluster_checked_sum_cancels_across_two_executors() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=info,warn"));

    test_request_context()
        .scope(async {
            configure_test_datafusion();
            let (harness, _data_dir, _state_dir) = start_two_executor_cluster(
                &[(0, vec![i64::MAX, i64::MAX]), (1, vec![-i64::MAX])],
                3,
            )
            .await?;

            let result = async {
                let plan = wait_for_wide_two_executor_plan(&harness).await?;
                eprintln!("cancel plan:\n{plan}");

                let (local_totals, local_overflows) = local_executor_sums(&harness).await?;
                assert_eq!(
                    local_overflows, 1,
                    "the executor that owns [MAX, MAX] must overflow production CheckedSum, \
                     totals={local_totals:?} plan:\n{plan}"
                );
                assert_eq!(
                    local_totals,
                    vec![-i64::MAX],
                    "the executor that owns [-MAX] must return -MAX, plan:\n{plan}"
                );

                let rows = harness.query(SUM_SQL).await.map_err(|error| {
                    anyhow::Error::msg(format!(
                        "scheduler SUM(v) over [MAX, MAX] and [-MAX] must be MAX, got {error}; plan:\n{plan}"
                    ))
                })?;
                assert_eq!(
                    single_i64(&rows),
                    i64::MAX,
                    "two-executor cancel must return MAX; plan:\n{plan}"
                );
                Ok::<(), anyhow::Error>(())
            }
            .await;

            harness.shutdown().await;
            result
        })
        .await
}

/// Two executors, partitions `[MAX, MAX]` and `[MAX, MAX]`: both local sums
/// overflow and the scheduler-wide `SUM` overflows.
#[tokio::test(flavor = "multi_thread")]
async fn cluster_checked_sum_errors_when_true_total_overflows() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=info,warn"));

    test_request_context()
        .scope(async {
            configure_test_datafusion();
            let (harness, _data_dir, _state_dir) = start_two_executor_cluster(
                &[(0, vec![i64::MAX, i64::MAX]), (1, vec![i64::MAX, i64::MAX])],
                4,
            )
            .await?;

            let result = async {
                let plan = wait_for_wide_two_executor_plan(&harness).await?;
                eprintln!("overflow plan:\n{plan}");

                let (local_totals, local_overflows) = local_executor_sums(&harness).await?;
                assert_eq!(
                    local_overflows, 2,
                    "both executors own [MAX, MAX] and must overflow production CheckedSum, \
                     totals={local_totals:?} plan:\n{plan}"
                );

                let error = harness
                    .query(SUM_SQL)
                    .await
                    .expect_err("true total MAX+MAX+MAX+MAX must overflow at the scheduler");
                anyhow::ensure!(
                    overflow_error(&error),
                    "true overflow must mention overflow, got {error}; plan:\n{plan}"
                );
                Ok::<(), anyhow::Error>(())
            }
            .await;

            harness.shutdown().await;
            result
        })
        .await
}
