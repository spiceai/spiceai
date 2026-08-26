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

//! `refresh_append_overlap` on a file-mode Cayenne acceleration.
//!
//! An append refresh resumes from `max(time_column)` read back out of the acceleration.
//! `refresh_append_overlap` moves that mark back by a fixed duration, so a row that
//! reaches the source late — carrying a timestamp below the mark — is still fetched.
//! Everything else inside that window is re-fetched on every refresh, and the copies
//! already stored are dropped by the exact-row comparison before the write.
//!
//! Both halves are load-bearing and this covers them together: the late row has to
//! arrive, and the re-fetched rows must not be appended a second time. A dedupe that
//! failed here would add the whole window to the table on every single refresh.

use std::sync::Arc;

use app::AppBuilder;
use arrow::array::{Int64Array, RecordBatch, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::prelude::SessionContext;
use datafusion::sql::TableReference;
use runtime::Runtime;
use spicepod::{
    acceleration::{Acceleration, Mode, RefreshMode},
    component::dataset::{Dataset, TimeFormat},
    param::Params,
};

use crate::utils::{run_query, runtime_ready_check, test_request_context, wait_until_true};

/// The accelerated table under test.
const TABLE: &str = "cayenne_append_overlap_it";

/// 2023-11-14T22:13:20Z, an arbitrary fixed instant.
const BASE_NANOS: i64 = 1_700_000_000_000_000_000;

/// One row per second, so the high-water mark is unambiguous and the overlap below
/// covers a known number of rows.
const STEP_NANOS: i64 = 1_000_000_000;

/// Rows the initial load carries. Their offsets are 1..=`INITIAL_ROWS` seconds.
const INITIAL_ROWS: i64 = 100;

/// How far back each refresh moves the mark, and the same value in rows: at one row per
/// second, a 30s overlap re-fetches the 30 rows below the mark every time.
const OVERLAP: &str = "30s";
const OVERLAP_ROWS: i64 = 30;

/// The row that reaches the source late, after the mark has already passed its
/// timestamp. `LATE_OFFSET` sits inside the overlap window but below the mark the
/// initial load left, so only the overlap can bring it in.
const LATE_ID: i64 = 1_000;
const LATE_OFFSET: i64 = INITIAL_ROWS - 15;

fn source_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
    ]))
}

/// One row per `(id, offset_seconds)` pair, at `BASE_NANOS + offset * STEP_NANOS`.
fn source_batch(rows: &[(i64, i64)]) -> Result<RecordBatch, anyhow::Error> {
    let ids: Vec<i64> = rows.iter().map(|(id, _)| *id).collect();
    let names: Vec<String> = ids.iter().map(|id| format!("row-{id}")).collect();
    let times: Vec<i64> = rows
        .iter()
        .map(|(_, offset)| BASE_NANOS + offset * STEP_NANOS)
        .collect();
    Ok(RecordBatch::try_new(
        source_schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
            Arc::new(TimestampNanosecondArray::from(times)),
        ],
    )?)
}

/// Write `rows` to `path` as a single Parquet file, replacing whatever is there.
async fn write_source(path: &std::path::Path, rows: &[(i64, i64)]) -> Result<(), anyhow::Error> {
    let _ = std::fs::remove_file(path);
    let ctx = SessionContext::new();
    ctx.read_batch(source_batch(rows)?)?
        .write_parquet(
            &path.to_string_lossy(),
            DataFrameWriteOptions::new().with_single_file_output(true),
            None,
        )
        .await?;
    Ok(())
}

/// Queue an append refresh. The caller polls for the result rather than waiting on the
/// returned notifier: completion signals with `notify_waiters`, which stores no permit,
/// so a refresh that finishes first would leave a later waiter hanging.
async fn trigger_refresh(rt: &Arc<Runtime>, table: &str) -> Result<(), anyhow::Error> {
    rt.datafusion()
        .refresh_table(&TableReference::from(table), None)
        .await
        .map_err(|e| anyhow::anyhow!("refresh_table failed for {table}: {e}"))?
        .ok_or_else(|| anyhow::anyhow!("no refresh notifier for {table}"))?;
    Ok(())
}

/// Run a `COUNT(*)` query and read back its single value.
async fn count(rt: &Arc<Runtime>, sql: &str) -> Result<i64, anyhow::Error> {
    let batches = run_query(rt, sql).await?;
    let batch = batches
        .iter()
        .find(|batch| batch.num_rows() > 0)
        .ok_or_else(|| anyhow::anyhow!("count query returned no rows"))?;
    Ok(batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| anyhow::anyhow!("count column is not Int64"))?
        .value(0))
}

/// Rows currently in the accelerated table.
async fn row_count(rt: &Arc<Runtime>) -> Result<i64, anyhow::Error> {
    count(rt, &format!("SELECT COUNT(*) AS cnt FROM {TABLE}")).await
}

/// How many ids appear more than once — the shape a leaked overlap takes.
async fn duplicate_id_count(rt: &Arc<Runtime>) -> Result<i64, anyhow::Error> {
    count(
        rt,
        &format!(
            "SELECT COUNT(*) AS cnt FROM \
             (SELECT id FROM {TABLE} GROUP BY id HAVING COUNT(*) > 1) AS dupes"
        ),
    )
    .await
}

/// Refresh, then wait for the table to reach `expected` rows. Fails with the last count
/// observed rather than a bare timeout, so an over-count is as readable as a stall.
async fn refresh_to(rt: &Arc<Runtime>, round: &str, expected: i64) -> Result<(), anyhow::Error> {
    trigger_refresh(rt, TABLE).await?;

    let reached = wait_until_true(std::time::Duration::from_mins(1), || async {
        row_count(rt).await.is_ok_and(|c| c >= expected)
    })
    .await;

    let observed = row_count(rt).await?;
    if !reached {
        return Err(anyhow::anyhow!(
            "append refresh ({round}) stalled at {observed} rows, expected {expected}"
        ));
    }
    // The wait admits an over-count so this can name it: every extra row here is a copy
    // of one already stored, re-fetched by the overlap and let through the dedupe.
    if observed != expected {
        return Err(anyhow::anyhow!(
            "append refresh ({round}) left {observed} rows, expected {expected}: \
             the overlap window was appended instead of de-duplicated"
        ));
    }
    Ok(())
}

/// Whether any `.vortex` file exists under `dir`.
fn has_vortex_file(dir: &std::path::Path) -> bool {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return false;
    };
    entries.flatten().any(|entry| {
        let path = entry.path();
        if path.is_dir() {
            has_vortex_file(&path)
        } else {
            path.extension().is_some_and(|ext| ext == "vortex")
        }
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[cfg(not(target_os = "windows"))]
async fn cayenne_append_refresh_honours_refresh_append_overlap() -> Result<(), anyhow::Error> {
    let _tracing = crate::init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let temp_dir = tempfile::tempdir()?;
            let source = temp_dir.path().join("events.parquet");
            let mut rows: Vec<(i64, i64)> = (1..=INITIAL_ROWS).map(|id| (id, id)).collect();
            write_source(&source, &rows).await?;
            crate::configure_test_datafusion();

            // Keep the acceleration — data files and the metastore that indexes them —
            // inside this run's temp directory. The default location is a metastore
            // shared by every file-mode Cayenne dataset in the process and across runs,
            // where a leftover table entry from an earlier run would decide what this
            // one opens.
            let data_path = temp_dir.path().join("accelerator");

            let mut dataset = Dataset::new(format!("file://{}", source.display()), TABLE);
            dataset.time_column = Some("event_time".to_string());
            dataset.time_format = Some(TimeFormat::Timestamp);
            dataset.acceleration = Some(Acceleration {
                enabled: true,
                engine: Some("cayenne".to_string()),
                mode: Mode::File,
                refresh_mode: Some(RefreshMode::Append),
                refresh_append_overlap: Some(OVERLAP.to_string()),
                params: Some(Params::from_string_map(
                    [
                        (
                            "cayenne_file_path".to_string(),
                            data_path.to_string_lossy().to_string(),
                        ),
                        // Send every write straight to a Vortex file, so the reads the
                        // overlap depends on — the high-water mark and the comparison
                        // set — go through the scan rather than the metastore's inline
                        // tier.
                        ("cayenne_inline_max_rows".to_string(), "0".to_string()),
                    ]
                    .into_iter()
                    .collect(),
                )),
                ..Acceleration::default()
            });

            let app = AppBuilder::new("test_cayenne_append_overlap")
                .with_dataset(dataset)
                .build();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
                    return Err(anyhow::Error::msg("Timeout waiting for components to load"));
                }
                () = Arc::clone(&rt).load_components() => {}
            }
            runtime_ready_check(&rt).await;

            assert_eq!(
                row_count(&rt).await?,
                INITIAL_ROWS,
                "row count after the initial load"
            );

            // Validity gate: the rows have to land in a Vortex file rather than the
            // metastore's inline tier, or the refreshes below never reach the scan.
            assert!(
                has_vortex_file(&data_path),
                "the initial load must have written a Vortex file under {data_path:?}"
            );

            // The mark now sits at INITIAL_ROWS seconds, and the overlap pulls it back
            // to INITIAL_ROWS - OVERLAP_ROWS. This round adds a row below the mark (only
            // the overlap can reach it) and one above it, and re-fetches the rest of the
            // window — which is already stored, so none of it may be appended again.
            rows.push((LATE_ID, LATE_OFFSET));
            rows.push((INITIAL_ROWS + 1, INITIAL_ROWS + 1));
            write_source(&source, &rows).await?;
            refresh_to(&rt, "late arrival", INITIAL_ROWS + 2).await?;

            assert_eq!(
                count(
                    &rt,
                    &format!("SELECT COUNT(*) AS cnt FROM {TABLE} WHERE id = {LATE_ID}"),
                )
                .await?,
                1,
                "the late row is below the high-water mark the initial load left, \
                 so refresh_append_overlap is the only thing that can fetch it — exactly once"
            );

            // Two more rounds, each adding a single new row. Every one of them re-fetches
            // the whole overlap window, so a dedupe that leaked would grow the table by
            // OVERLAP_ROWS a round instead of by one.
            for round in 2..=3_i64 {
                let id = INITIAL_ROWS + round;
                rows.push((id, id));
                write_source(&source, &rows).await?;
                refresh_to(&rt, &format!("round {round}"), INITIAL_ROWS + 1 + round).await?;
            }

            assert_eq!(
                duplicate_id_count(&rt).await?,
                0,
                "no id may appear twice: every refresh re-fetched the {OVERLAP_ROWS} rows \
                 inside the {OVERLAP} overlap window and none of them may be stored again"
            );

            Ok(())
        })
        .await
}
