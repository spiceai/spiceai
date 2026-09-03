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

use app::AppBuilder;
use arrow::array::RecordBatch;
use datafusion::assert_batches_eq;
use datafusion::common::TableReference;
use futures::TryStreamExt;
use runtime::Runtime;
use spicepod::{
    acceleration::{Acceleration, Mode, RefreshMode},
    component::dataset::Dataset,
    partitioning::PartitionedBy,
};
use std::sync::Arc;

use crate::utils::{runtime_ready_check, test_request_context, wait_until_true};

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

async fn refresh_table(rt: &Arc<Runtime>, table_name: &str) -> Result<(), anyhow::Error> {
    let notifier = rt
        .datafusion()
        .refresh_table(&TableReference::from(table_name), None)
        .await?;
    notifier
        .ok_or_else(|| anyhow::anyhow!("Failed to refresh table"))?
        .wait()
        .await;
    Ok(())
}

fn make_dataset(name: &str, partition_by: Vec<PartitionedBy>) -> Result<Dataset, anyhow::Error> {
    let test_file = std::env::current_dir()
        .map_err(|e| anyhow::anyhow!("Failed to get current directory: {e}"))?
        .join("tests/acceleration/data/partition_test.csv");

    let mut dataset = Dataset::new(format!("file://{}", test_file.display()), name);
    dataset.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some("arrow".to_string()),
        mode: Mode::Memory,
        refresh_mode: Some(RefreshMode::Full),
        retention_sql: Some(format!("DELETE FROM {name} WHERE score < 90")),
        retention_check_enabled: false,
        retention_check_interval: None,
        partition_by,
        ..Acceleration::default()
    });
    Ok(dataset)
}

async fn assert_retention_sql_applies_on_refresh(
    dataset_name: &str,
    partition_by: Vec<PartitionedBy>,
) -> Result<(), anyhow::Error> {
    let _tracing = crate::init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            crate::configure_test_datafusion();

            let app = AppBuilder::new(dataset_name)
                .with_dataset(make_dataset(dataset_name, partition_by)?)
                .build();

            let rt = Arc::new(Runtime::builder().with_app(app).build().await);

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(30)) => {
                    return Err(anyhow::Error::msg("Timeout waiting for components to load"));
                }
                () = Arc::clone(&rt).load_components() => {}
            }

            runtime_ready_check(&rt).await;
            refresh_table(&rt, dataset_name).await?;

            let retained = run_query(
                &rt,
                &format!("SELECT id, score FROM {dataset_name} ORDER BY id"),
            )
            .await?;
            let expected = [
                "+----+-------+",
                "| id | score |",
                "+----+-------+",
                "| 2  | 92    |",
                "| 6  | 94    |",
                "| 10 | 90    |",
                "+----+-------+",
            ];
            assert_batches_eq!(&expected, &retained);

            let violating = run_query(
                &rt,
                &format!("SELECT id FROM {dataset_name} WHERE score < 90"),
            )
            .await?;
            let violating_count: usize = violating.iter().map(RecordBatch::num_rows).sum();
            assert_eq!(
                violating_count, 0,
                "retention_sql should be applied during the Arrow refresh write path"
            );

            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_arrow_retention_sql_applies_on_refresh_without_retention_interval()
-> Result<(), anyhow::Error> {
    assert_retention_sql_applies_on_refresh("arrow_retention_write_path_test", Vec::new()).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_partitioned_arrow_retention_sql_applies_on_refresh_without_retention_interval()
-> Result<(), anyhow::Error> {
    assert_retention_sql_applies_on_refresh(
        "partitioned_arrow_retention_write_path_test",
        vec![PartitionedBy {
            name: "expr0".to_string(),
            expression: "bucket(3, id)".to_string(),
        }],
    )
    .await
}

/// The retention period for the tests below. Rows older than this are what a retention
/// pass is supposed to delete.
const RETENTION_PERIOD: std::time::Duration = std::time::Duration::from_mins(60);

/// Rows this many minutes old, so they sit well outside [`RETENTION_PERIOD`] but well
/// inside the refresh window the dataset declares.
const STALE_AGE_MINUTES: i64 = 180;

/// How many of the source's rows are stale, and how many are fresh.
const STALE_ROWS: usize = 5;
const FRESH_ROWS: usize = 5;

/// Write a source whose `ts` column straddles [`RETENTION_PERIOD`]: [`STALE_ROWS`] rows
/// old enough for a retention pass to delete, and [`FRESH_ROWS`] it must keep.
///
/// The window is expressed against the process clock rather than fixed instants so the
/// same file means the same thing whenever the test runs.
fn write_straddling_source(path: &std::path::Path) -> Result<(), anyhow::Error> {
    use std::fmt::Write as _;

    let now = chrono::Utc::now();
    let mut csv = String::from("id,ts\n");
    for i in 0..STALE_ROWS {
        let ts = now - chrono::Duration::minutes(STALE_AGE_MINUTES + i as i64);
        writeln!(csv, "{},{}", i, ts.format("%Y-%m-%dT%H:%M:%S%.6fZ"))?;
    }
    for i in 0..FRESH_ROWS {
        let ts = now - chrono::Duration::seconds(i as i64);
        writeln!(
            csv,
            "{},{}",
            STALE_ROWS + i,
            ts.format("%Y-%m-%dT%H:%M:%S%.6fZ")
        )?;
    }
    std::fs::write(path, csv)?;
    Ok(())
}

/// A time-based retention policy, complete except for `retention_check_interval` when
/// `check_interval` is `None` — the one setting with no default, and the configuration
/// #13804 is about.
///
/// `refresh_data_window` is declared wider than `retention_period` on purpose: the
/// refresh window otherwise falls back to `retention_period`
/// (`DataFusion::create_accelerated_table`), which would filter the stale rows out at
/// load and leave nothing for a retention pass to be observed deleting. No
/// `refresh_check_interval` is set either, so the source loads once and a later refresh
/// cannot put back what retention removed.
fn make_time_retention_dataset(
    source: &std::path::Path,
    name: &str,
    check_interval: Option<&str>,
) -> Dataset {
    let mut dataset = Dataset::new(format!("file://{}", source.display()), name);
    dataset.time_column = Some("ts".to_string());
    dataset.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some("arrow".to_string()),
        mode: Mode::Memory,
        refresh_mode: Some(RefreshMode::Full),
        refresh_data_window: Some("24h".to_string()),
        retention_period: Some(format!("{}s", RETENTION_PERIOD.as_secs())),
        retention_check_enabled: true,
        retention_check_interval: check_interval.map(ToString::to_string),
        ..Acceleration::default()
    });
    dataset
}

/// Load `dataset` and return the runtime, with every row of the source in the
/// acceleration.
async fn load_with_all_rows(name: &str, dataset: Dataset) -> Result<Arc<Runtime>, anyhow::Error> {
    let app = AppBuilder::new(name).with_dataset(dataset).build();
    let rt = Arc::new(Runtime::builder().with_app(app).build().await);

    tokio::select! {
        () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
            return Err(anyhow::Error::msg("Timeout waiting for components to load"));
        }
        () = Arc::clone(&rt).load_components() => {}
    }
    runtime_ready_check(&rt).await;

    let total = i64::try_from(STALE_ROWS + FRESH_ROWS)?;
    let loaded = wait_until_true(std::time::Duration::from_secs(30), || async {
        crate::acceleration::row_count(&rt, name)
            .await
            .is_ok_and(|c| c == total)
    })
    .await;
    anyhow::ensure!(
        loaded,
        "the acceleration holds {} row(s) after load, expected all {total}: the refresh window did not admit the stale rows, so this test cannot observe retention",
        crate::acceleration::row_count(&rt, name).await?
    );
    Ok(rt)
}

/// A complete time-based retention policy runs, and deletes exactly the stale rows.
///
/// The companion test below is the same configuration with `retention_check_interval`
/// removed; together they are what asserts a policy actually starts for a given
/// configuration, which nothing covered when #13804 was filed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_arrow_time_retention_with_a_check_interval_evicts_the_stale_rows()
-> Result<(), anyhow::Error> {
    let _tracing = crate::init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            crate::configure_test_datafusion();

            let temp_dir = tempfile::tempdir()?;
            let source = temp_dir.path().join("events.csv");
            write_straddling_source(&source)?;

            let name = "arrow_time_retention_with_interval";
            let rt =
                load_with_all_rows(name, make_time_retention_dataset(&source, name, Some("1s")))
                    .await?;

            let fresh = i64::try_from(FRESH_ROWS)?;
            let evicted = wait_until_true(std::time::Duration::from_secs(60), || async {
                crate::acceleration::row_count(&rt, name)
                    .await
                    .is_ok_and(|c| c == fresh)
            })
            .await;
            assert!(
                evicted,
                "the acceleration holds {} row(s), expected the {fresh} inside the retention period: the retention pass did not run",
                crate::acceleration::row_count(&rt, name).await?
            );

            let survivors = run_query(&rt, &format!("SELECT id FROM {name} ORDER BY id")).await?;
            let expected = [
                "+----+", "| id |", "+----+", "| 5  |", "| 6  |", "| 7  |", "| 8  |", "| 9  |",
                "+----+",
            ];
            assert_batches_eq!(&expected, &survivors);

            Ok(())
        })
        .await
}

/// The same policy with no `retention_check_interval` never evicts anything.
///
/// This is the state #13804 reports: the setting has no default, so an operator who
/// configured everything else gets no retention. The assertion is that the rows stay,
/// which is the observable half — that the runtime now *says* so is asserted on the
/// refusal itself in `runtime-table`, where the message is a value rather than a log
/// line a spawned task writes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_arrow_time_retention_without_a_check_interval_evicts_nothing()
-> Result<(), anyhow::Error> {
    let _tracing = crate::init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            crate::configure_test_datafusion();

            let temp_dir = tempfile::tempdir()?;
            let source = temp_dir.path().join("events.csv");
            write_straddling_source(&source)?;

            let name = "arrow_time_retention_without_interval";
            let rt = load_with_all_rows(name, make_time_retention_dataset(&source, name, None))
                .await?;

            let total = i64::try_from(STALE_ROWS + FRESH_ROWS)?;
            let fresh = i64::try_from(FRESH_ROWS)?;
            // The companion test above converges in a second or two on the same rig, so
            // a window this long is generous rather than tight: what is under test is
            // that no pass is scheduled at all.
            let evicted = wait_until_true(std::time::Duration::from_secs(20), || async {
                crate::acceleration::row_count(&rt, name)
                    .await
                    .is_ok_and(|c| c < total)
            })
            .await;
            assert!(
                !evicted,
                "the acceleration dropped to {} row(s) with no `retention_check_interval` set, so retention ran after all and this test no longer covers #13804",
                crate::acceleration::row_count(&rt, name).await?
            );
            assert_eq!(
                crate::acceleration::row_count(&rt, name).await?,
                total,
                "every row must survive, including the {} outside the retention period",
                total - fresh
            );

            Ok(())
        })
        .await
}
