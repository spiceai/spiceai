/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! `retention_sql` on a file-mode Cayenne acceleration under `refresh_mode: full`.
//!
//! A full refresh reloads every source row, so the retention predicate has to run
//! again over the new snapshot on every refresh — the way the DuckDB accelerator
//! applies it before each refresh commit. Cayenne applies it from its post-write
//! maintenance loop, so the rows disappear shortly *after* the refresh commits rather
//! than inside the refresh write path; the polling below is what that difference costs
//! the test.
//!
//! The second refresh is the point of the test, not a repeat of the first: the rows the
//! source still carries come back with it, so a retention that ran only on the initial
//! load would leave them in the acceleration from then on.

use std::sync::Arc;

use app::AppBuilder;
use arrow::array::RecordBatch;
use datafusion::assert_batches_eq;
use datafusion::common::TableReference;
use futures::TryStreamExt;
use runtime::Runtime;
use spicepod::{
    acceleration::{Acceleration, Mode, RefreshMode},
    component::dataset::Dataset,
    param::Params,
};

use crate::utils::{runtime_ready_check, test_request_context, wait_until_true};

/// The accelerated table under test.
const TABLE: &str = "cayenne_retention_sql_it";

/// Rows scoring below this are deleted by the retention predicate; the shared CSV
/// leaves ids 2, 6 and 10 above it and 7 rows below.
const SCORE_FLOOR: i64 = 90;

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

async fn refresh_table(rt: &Arc<Runtime>) -> Result<(), anyhow::Error> {
    rt.datafusion()
        .refresh_table(&TableReference::from(TABLE), None)
        .await?
        .ok_or_else(|| anyhow::anyhow!("no refresh notifier for {TABLE}"))?
        .notified()
        .await;
    Ok(())
}

/// Rows still violating the retention predicate.
async fn violating_rows(rt: &Arc<Runtime>) -> Result<usize, anyhow::Error> {
    let batches = run_query(
        rt,
        &format!("SELECT id FROM {TABLE} WHERE score < {SCORE_FLOOR}"),
    )
    .await?;
    Ok(batches.iter().map(RecordBatch::num_rows).sum())
}

/// Wait for retention to clear the rows this refresh reloaded, then check that it
/// deleted those rows and only those.
async fn assert_retention_applied(rt: &Arc<Runtime>, round: &str) -> Result<(), anyhow::Error> {
    let applied = wait_until_true(std::time::Duration::from_secs(60), || async {
        violating_rows(rt).await.is_ok_and(|n| n == 0)
    })
    .await;
    if !applied {
        return Err(anyhow::anyhow!(
            "after the {round} refresh, retention_sql left {} row(s) below score {SCORE_FLOOR}: \
             the Cayenne accelerator did not apply its retention filters",
            violating_rows(rt).await?
        ));
    }

    let retained = run_query(rt, &format!("SELECT id, score FROM {TABLE} ORDER BY id")).await?;
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
    Ok(())
}

fn make_dataset(data_path: &std::path::Path) -> Result<Dataset, anyhow::Error> {
    let test_file = std::env::current_dir()
        .map_err(|e| anyhow::anyhow!("Failed to get current directory: {e}"))?
        .join("tests/acceleration/data/partition_test.csv");

    let mut dataset = Dataset::new(format!("file://{}", test_file.display()), TABLE);
    dataset.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some("cayenne".to_string()),
        mode: Mode::File,
        refresh_mode: Some(RefreshMode::Full),
        retention_sql: Some(format!("DELETE FROM {TABLE} WHERE score < {SCORE_FLOOR}")),
        // Cayenne arms retention from its own post-write maintenance, so the periodic
        // retention worker is not what is under test here.
        retention_check_enabled: false,
        retention_check_interval: None,
        // Keep the acceleration — data files and the metastore that indexes them —
        // inside this run's temp directory, away from the metastore shared by every
        // file-mode Cayenne dataset in the process and across runs.
        params: Some(Params::from_string_map(
            [(
                "cayenne_file_path".to_string(),
                data_path.to_string_lossy().to_string(),
            )]
            .into_iter()
            .collect(),
        )),
        ..Acceleration::default()
    });
    Ok(dataset)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[cfg(not(target_os = "windows"))]
async fn cayenne_full_refresh_applies_retention_sql_on_every_refresh() -> Result<(), anyhow::Error>
{
    let _tracing = crate::init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            crate::configure_test_datafusion();

            let temp_dir = tempfile::tempdir()?;
            let data_path = temp_dir.path().join("accelerator");

            let app = AppBuilder::new("test_cayenne_retention_sql")
                .with_dataset(make_dataset(&data_path)?)
                .build();

            let rt = Arc::new(Runtime::builder().with_app(app).build().await);

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err(anyhow::Error::msg("Timeout waiting for components to load"));
                }
                () = Arc::clone(&rt).load_components() => {}
            }
            runtime_ready_check(&rt).await;

            assert_retention_applied(&rt, "initial").await?;

            // The source is unchanged, so this refresh reloads the same 10 rows —
            // including the 7 retention deleted. Retention has to run again.
            refresh_table(&rt).await?;
            assert_retention_applied(&rt, "second").await?;

            Ok(())
        })
        .await
}
