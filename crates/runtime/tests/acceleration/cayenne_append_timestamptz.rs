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

//! Regression test for <https://github.com/spiceai/spiceai/issues/13468>: append
//! refresh of a file-mode Cayenne acceleration whose `time_column` is timezone-aware.
//!
//! Every append refresh reads `max(time_column)` back out of the acceleration to learn
//! where to resume. A cast on that read's sort key reaches the Vortex scan as a pruning
//! predicate it has no kernel for, so the high-water mark stops advancing. This covers
//! the path end to end: repeated refreshes over a `Timestamp(ns, "UTC")` column must
//! keep advancing the mark and appending only the new rows.

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

use crate::utils::{run_query, runtime_ready_check, test_request_context};

/// The timezone a `PostgreSQL` `timestamptz` arrives with.
const TZ: &str = "UTC";

/// 2023-11-14T22:13:20Z, an arbitrary fixed instant.
const BASE_NANOS: i64 = 1_700_000_000_000_000_000;

/// One row per second, so the high-water mark is unambiguous.
const STEP_NANOS: i64 = 1_000_000_000;

/// Rows the initial load carries, and rows each later append adds.
const ROWS_PER_ROUND: i64 = 500;

/// Append rounds after the initial load. Every round leaves another Vortex
/// file, and the pruning predicate this test is about only reaches Vortex once
/// a scan partition holds more than one of them.
const ROUNDS: i64 = 2;

fn source_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Nanosecond, Some(TZ.into())),
            false,
        ),
    ]))
}

/// Rows `ids`, each with an `event_time` of `BASE_NANOS + id * STEP_NANOS`.
fn source_batch(ids: &[i64]) -> Result<RecordBatch, anyhow::Error> {
    let names: Vec<String> = ids.iter().map(|id| format!("row-{id}")).collect();
    let times: Vec<i64> = ids.iter().map(|id| BASE_NANOS + id * STEP_NANOS).collect();
    Ok(RecordBatch::try_new(
        source_schema(),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(StringArray::from(names)),
            Arc::new(TimestampNanosecondArray::from(times).with_timezone(TZ)),
        ],
    )?)
}

/// Write `ids` to `path` as a single Parquet file, replacing whatever is there.
///
/// Parquet records the timezone-aware column as `isAdjustedToUTC`, so the file
/// connector reads it back as `Timestamp(ns, "UTC")` — the shape a `PostgreSQL`
/// `timestamptz` reaches the accelerator with.
async fn write_source(path: &std::path::Path, ids: &[i64]) -> Result<(), anyhow::Error> {
    let _ = std::fs::remove_file(path);
    let ctx = SessionContext::new();
    ctx.read_batch(source_batch(ids)?)?
        .write_parquet(
            &path.to_string_lossy(),
            DataFrameWriteOptions::new().with_single_file_output(true),
            None,
        )
        .await?;
    Ok(())
}

async fn refresh(rt: &Arc<Runtime>, table: &str) -> Result<(), anyhow::Error> {
    let notifier = rt
        .datafusion()
        .refresh_table(&TableReference::from(table), None)
        .await
        .map_err(|e| anyhow::anyhow!("refresh_table failed: {e}"))?;
    notifier
        .ok_or_else(|| anyhow::anyhow!("no refresh notifier for {table}"))?
        .notified()
        .await;
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
async fn cayenne_append_refresh_advances_a_timezone_aware_time_column()
-> Result<(), anyhow::Error> {
    let _tracing = crate::init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let temp_dir = tempfile::tempdir()?;
            let source = temp_dir.path().join("events.parquet");
            let initial: Vec<i64> = (1..=ROWS_PER_ROUND).collect();
            write_source(&source, &initial).await?;
            crate::configure_test_datafusion();

            // Keep the acceleration — data files and the metastore that indexes
            // them — inside this run's temp directory. The default location is a
            // metastore shared by every file-mode Cayenne dataset in the process
            // and across runs, where a leftover table entry from an earlier run
            // would decide what this one opens.
            let data_path = temp_dir.path().join("accelerator");

            let mut dataset = Dataset::new(
                format!("file://{}", source.display()),
                "cayenne_append_tz_it",
            );
            dataset.time_column = Some("event_time".to_string());
            dataset.time_format = Some(TimeFormat::Timestamptz);
            dataset.acceleration = Some(Acceleration {
                enabled: true,
                engine: Some("cayenne".to_string()),
                mode: Mode::File,
                refresh_mode: Some(RefreshMode::Append),
                params: Some(Params::from_string_map(
                    [
                        (
                            "cayenne_file_path".to_string(),
                            data_path.to_string_lossy().to_string(),
                        ),
                        // Send every write straight to a Vortex file. The
                        // high-water-mark read only reaches Vortex once the rows
                        // leave the metastore's inline tier, so without this the
                        // refresh below would be served from the inline rows and
                        // prove nothing.
                        ("cayenne_inline_max_rows".to_string(), "0".to_string()),
                    ]
                    .into_iter()
                    .collect(),
                )),
                ..Acceleration::default()
            });

            let app = AppBuilder::new("test_cayenne_append_timestamptz")
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
                count(&rt, "SELECT COUNT(*) AS cnt FROM cayenne_append_tz_it").await?,
                ROWS_PER_ROUND,
                "row count after the initial load"
            );

            // Validity gate: the rows have to land in a Vortex file rather than the
            // metastore's inline tier, or the refreshes below never reach the scan.
            assert!(
                has_vortex_file(&data_path),
                "the initial load must have written a Vortex file under {data_path:?}"
            );

            // Each refresh appends its rows as another Vortex file. Once a scan
            // partition holds more than one, the later files are opened with the
            // high-water-mark read's dynamic filter as their pruning predicate.
            for round in 1..=ROUNDS {
                let rows = ROWS_PER_ROUND * (round + 1);
                write_source(&source, &(1..=rows).collect::<Vec<i64>>()).await?;
                tokio::time::timeout(
                    std::time::Duration::from_mins(1),
                    refresh(&rt, "cayenne_append_tz_it"),
                )
                .await
                .map_err(|_| anyhow::anyhow!("append refresh round {round} did not settle"))??;

                assert_eq!(
                    count(&rt, "SELECT COUNT(*) AS cnt FROM cayenne_append_tz_it").await?,
                    rows,
                    "row count after append round {round}"
                );
            }

            // The counts above check how many rows landed; this checks which ones.
            // The mark is the last round's first instant, so exactly that round's
            // rows sit above it. Arrow holds a timezone-aware timestamp as
            // epoch-relative, so dropping the zone re-labels the instant without
            // moving it.
            let mark = chrono::DateTime::from_timestamp_nanos(
                BASE_NANOS + ROWS_PER_ROUND * ROUNDS * STEP_NANOS,
            )
            .naive_utc();
            assert_eq!(
                count(
                    &rt,
                    &format!(
                        "SELECT COUNT(*) AS cnt FROM cayenne_append_tz_it \
                         WHERE CAST(event_time AS TIMESTAMP) > TIMESTAMP '{mark}'"
                    ),
                )
                .await?,
                ROWS_PER_ROUND,
                "the last round's rows must be the ones above the mark"
            );

            Ok(())
        })
        .await
}
