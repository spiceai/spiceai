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

//! `acceleration.refresh_sql` with `SELECT DISTINCT ON (<primary key>) ... ORDER BY
//! <primary key>, <time column> DESC NULLS LAST`: keep the latest row per key.
//!
//! The source is a directory of Parquet files, so a key's versions arrive in different
//! files — and different record batches — of one refresh, which is the case the
//! existing `upsert_dedup*` modes cannot resolve (they deduplicate one batch at a time).
//! Each case runs two refreshes against both `DuckDB` and Cayenne, under `full` and
//! `append` (with `refresh_append_overlap`) and every `on_conflict` behavior, and checks
//! the exact rows the acceleration holds after each.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use app::AppBuilder;
use arrow::array::{AsArray, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Int64Type, Schema, SchemaRef, TimeUnit};
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::prelude::SessionContext;
use runtime::Runtime;
use spicepod::{
    acceleration::{Acceleration, Mode, OnConflictBehavior, RefreshMode},
    component::dataset::{Dataset, TimeFormat},
    param::Params,
};

use crate::acceleration::trigger_refresh;
use crate::utils::{
    run_query, runtime_ready_check, runtime_ready_check_with_timeout_err, test_request_context,
    wait_until_true,
};

const TABLE: &str = "events";

/// `DISTINCT ON` over every column, newest `occurred_at` first, NULL times last.
const LATEST_PER_KEY: &str =
    "SELECT DISTINCT ON (id) * FROM events ORDER BY id, occurred_at DESC NULLS LAST";

/// Microseconds for 2026-01-`day` 00:00:00 UTC.
fn day(day: i64) -> i64 {
    // 2026-01-01T00:00:00Z
    const JAN_1_2026_MICROS: i64 = 1_767_225_600_000_000;
    JAN_1_2026_MICROS + (day - 1) * 86_400_000_000
}

/// One source row: `(id, region, occurred_at day, seq, v)`. `None` is a NULL time.
type Row = (i64, &'static str, Option<i64>, i64, &'static str);

fn source_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, false),
        Field::new(
            "occurred_at",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new("seq", DataType::Int64, false),
        Field::new("v", DataType::Utf8, false),
    ]))
}

/// Write `rows` as one Parquet file at `path`.
async fn write_parquet(path: &Path, rows: &[Row]) -> Result<(), anyhow::Error> {
    let batch = RecordBatch::try_new(
        source_schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.1))),
            Arc::new(
                rows.iter()
                    .map(|r| r.2.map(day))
                    .collect::<TimestampMicrosecondArray>(),
            ),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.3))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.4))),
        ],
    )?;
    SessionContext::new()
        .read_batch(batch)?
        .write_parquet(
            &path.to_string_lossy(),
            DataFrameWriteOptions::new().with_single_file_output(true),
            None,
        )
        .await?;
    Ok(())
}

/// First refresh: two files, so `id = 1`'s three versions and `id = 2`'s two versions
/// arrive in different batches. `id = 2`'s NULL time must lose to its real one.
const FILE_A: &[Row] = &[
    (1, "us", Some(10), 1, "id1-us-jan10"),
    (2, "us", Some(5), 1, "id2-jan5"),
    (3, "us", Some(1), 1, "id3-jan1"),
    (5, "eu", Some(9), 1, "id5-eu-jan9"),
];
const FILE_B: &[Row] = &[
    (1, "us", Some(8), 1, "id1-us-jan8"),
    (2, "us", None, 1, "id2-null-time"),
    (1, "eu", Some(11), 1, "id1-eu-jan11"),
];
/// Second refresh: a late, older version of `id = 1` (inside the overlap window, so
/// append re-reads it) that must not replace the stored newer one, and a newer version
/// of `id = 3` (whose stored version is outside the window).
const FILE_C: &[Row] = &[
    (1, "us", Some(9), 1, "id1-us-late-jan9"),
    (3, "us", Some(12), 1, "id3-jan12"),
];

#[derive(Clone, Copy, Debug)]
enum Engine {
    DuckDb,
    Cayenne,
}

impl Engine {
    fn name(self) -> &'static str {
        match self {
            Engine::DuckDb => "duckdb",
            Engine::Cayenne => "cayenne",
        }
    }
}

struct Case<'a> {
    engine: Engine,
    refresh_mode: RefreshMode,
    on_conflict: OnConflictBehavior,
    refresh_sql: &'a str,
}

impl Case<'_> {
    fn label(&self) -> String {
        format!(
            "{} {:?} on_conflict={:?}",
            self.engine.name(),
            self.refresh_mode,
            self.on_conflict
        )
    }

    fn dataset(&self, source_dir: &Path, data_dir: &Path) -> Dataset {
        let mut dataset = Dataset::new(format!("file://{}/", source_dir.display()), TABLE);
        dataset.params = Some(Params::from_string_map(HashMap::from([(
            "file_format".to_string(),
            "parquet".to_string(),
        )])));
        dataset.time_column = Some("occurred_at".to_string());
        dataset.time_format = Some(TimeFormat::Timestamp);
        let params = match self.engine {
            // Keep a file-mode Cayenne table in this case's own directory, not the
            // metastore shared by every file-mode Cayenne dataset in the process.
            Engine::Cayenne => Some(Params::from_string_map(HashMap::from([(
                "cayenne_file_path".to_string(),
                data_dir.to_string_lossy().to_string(),
            )]))),
            Engine::DuckDb => None,
        };
        dataset.acceleration = Some(Acceleration {
            enabled: true,
            engine: Some(self.engine.name().to_string()),
            mode: match self.engine {
                Engine::Cayenne => Mode::File,
                Engine::DuckDb => Mode::Memory,
            },
            refresh_mode: Some(self.refresh_mode.clone()),
            refresh_sql: Some(self.refresh_sql.to_string()),
            refresh_append_overlap: matches!(self.refresh_mode, RefreshMode::Append)
                .then(|| "7d".to_string()),
            primary_key: Some("id".to_string()),
            on_conflict: HashMap::from([("id".to_string(), self.on_conflict)]),
            params,
            ..Acceleration::default()
        });
        dataset
    }
}

async fn start(dataset: Dataset, app_name: &str) -> Result<Arc<Runtime>, anyhow::Error> {
    crate::configure_test_datafusion();
    let app = AppBuilder::new(app_name).with_dataset(dataset).build();
    let rt = Arc::new(Runtime::builder().with_app(app).build().await);
    tokio::select! {
        () = tokio::time::sleep(Duration::from_mins(1)) => {
            return Err(anyhow::anyhow!("timed out loading components for {app_name}"));
        }
        () = Arc::clone(&rt).load_components() => {}
    }
    Ok(rt)
}

/// `(id, v)` for every stored row, ordered by id. The columns are cast to `Int64` and
/// `Utf8` because the engines return different physical string types.
async fn stored(rt: &Arc<Runtime>) -> Result<Vec<(i64, String)>, anyhow::Error> {
    let batches = run_query(rt, &format!("SELECT id, v FROM {TABLE} ORDER BY id")).await?;
    let mut rows = Vec::new();
    for batch in batches {
        let ids = arrow::compute::cast(batch.column(0), &DataType::Int64)?;
        let vs = arrow::compute::cast(batch.column(1), &DataType::Utf8)?;
        let ids = ids.as_primitive::<Int64Type>();
        let vs = vs.as_string::<i32>();
        for i in 0..batch.num_rows() {
            rows.push((ids.value(i), vs.value(i).to_string()));
        }
    }
    Ok(rows)
}

fn rows(expected: &[(i64, &str)]) -> Vec<(i64, String)> {
    expected
        .iter()
        .map(|(id, v)| (*id, (*v).to_string()))
        .collect()
}

/// Run both refreshes for one case and check the stored rows after each.
async fn run_case(case: &Case<'_>) -> Result<(), anyhow::Error> {
    let label = case.label();
    let temp = tempfile::tempdir()?;
    let source_dir = temp.path().join("source");
    std::fs::create_dir_all(&source_dir)?;
    write_parquet(&source_dir.join("a.parquet"), FILE_A).await?;
    write_parquet(&source_dir.join("b.parquet"), FILE_B).await?;

    let rt = start(
        case.dataset(&source_dir, &temp.path().join("accelerator")),
        "refresh_sql_distinct_on",
    )
    .await?;
    runtime_ready_check(&rt).await;

    let first = rows(&[
        (1, "id1-eu-jan11"),
        (2, "id2-jan5"),
        (3, "id3-jan1"),
        (5, "id5-eu-jan9"),
    ]);
    assert_eq!(
        stored(&rt).await?,
        first,
        "{label}: first refresh must keep exactly the latest row per id, with the NULL time losing"
    );

    write_parquet(&source_dir.join("c.parquet"), FILE_C).await?;
    trigger_refresh(&rt, TABLE).await?;

    // `id = 3` gets a newer version unless an append refresh drops conflicts: a full
    // refresh replaces the table whatever `on_conflict` says.
    let newer_id3_applies = matches!(case.refresh_mode, RefreshMode::Full)
        || !matches!(case.on_conflict, OnConflictBehavior::Drop);
    let second = rows(&[
        (1, "id1-eu-jan11"),
        (2, "id2-jan5"),
        (
            3,
            if newer_id3_applies {
                "id3-jan12"
            } else {
                "id3-jan1"
            },
        ),
        (5, "id5-eu-jan9"),
    ]);
    let reached = wait_until_true(Duration::from_secs(30), || async {
        stored(&rt).await.is_ok_and(|r| r == second)
    })
    .await;
    assert!(
        reached,
        "{label}: after the second refresh expected {second:?}, found {:?}. The late older \
         id=1 row must never replace the stored newer one",
        stored(&rt).await?
    );
    Ok(())
}

const ALL_ON_CONFLICT: [OnConflictBehavior; 4] = [
    OnConflictBehavior::Upsert,
    OnConflictBehavior::UpsertDedup,
    OnConflictBehavior::UpsertDedupByRowId,
    OnConflictBehavior::Drop,
];

async fn run_matrix(engine: Engine, refresh_mode: RefreshMode) -> Result<(), anyhow::Error> {
    let _tracing = crate::init_tracing(Some("integration=debug,info"));
    test_request_context()
        .scope(async {
            for on_conflict in ALL_ON_CONFLICT {
                run_case(&Case {
                    engine,
                    refresh_mode: refresh_mode.clone(),
                    on_conflict,
                    refresh_sql: LATEST_PER_KEY,
                })
                .await?;
            }
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn distinct_on_duckdb_full_refresh() -> Result<(), anyhow::Error> {
    run_matrix(Engine::DuckDb, RefreshMode::Full).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn distinct_on_duckdb_append_refresh() -> Result<(), anyhow::Error> {
    run_matrix(Engine::DuckDb, RefreshMode::Append).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn distinct_on_cayenne_full_refresh() -> Result<(), anyhow::Error> {
    run_matrix(Engine::Cayenne, RefreshMode::Full).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn distinct_on_cayenne_append_refresh() -> Result<(), anyhow::Error> {
    run_matrix(Engine::Cayenne, RefreshMode::Append).await
}

/// A `WHERE` filter and a column subset still apply, and the filter runs before
/// `DISTINCT ON`: `id = 1`'s newest row is in region `eu`, so with `region = 'us'` the
/// latest *us* row wins instead of the key disappearing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn distinct_on_with_filter_and_projection() -> Result<(), anyhow::Error> {
    let _tracing = crate::init_tracing(Some("integration=debug,info"));
    test_request_context()
        .scope(async {
            for engine in [Engine::DuckDb, Engine::Cayenne] {
                let case = Case {
                    engine,
                    refresh_mode: RefreshMode::Full,
                    on_conflict: OnConflictBehavior::Upsert,
                    refresh_sql: "SELECT DISTINCT ON (id) id, occurred_at, v FROM events \
                         WHERE region = 'us' ORDER BY id, occurred_at DESC NULLS LAST",
                };
                let temp = tempfile::tempdir()?;
                let source_dir = temp.path().join("source");
                std::fs::create_dir_all(&source_dir)?;
                write_parquet(&source_dir.join("a.parquet"), FILE_A).await?;
                write_parquet(&source_dir.join("b.parquet"), FILE_B).await?;
                let rt = start(
                    case.dataset(&source_dir, &temp.path().join("accelerator")),
                    "refresh_sql_distinct_on_filter",
                )
                .await?;
                runtime_ready_check(&rt).await;

                assert_eq!(
                    stored(&rt).await?,
                    rows(&[(1, "id1-us-jan10"), (2, "id2-jan5"), (3, "id3-jan1")]),
                    "{}: the region filter must apply before DISTINCT ON",
                    case.label()
                );
                let columns: Vec<String> =
                    run_query(&rt, &format!("SELECT * FROM {TABLE} LIMIT 1"))
                        .await?
                        .first()
                        .map(|b| {
                            b.schema()
                                .fields()
                                .iter()
                                .map(|f| f.name().clone())
                                .collect()
                        })
                        .unwrap_or_default();
                assert_eq!(
                    columns,
                    vec!["id", "occurred_at", "v"],
                    "{}: the acceleration holds only the selected columns",
                    case.label()
                );
            }
            Ok(())
        })
        .await
}

/// Equal times are broken by the next `ORDER BY` column, in either direction.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn distinct_on_breaks_ties_with_order_by() -> Result<(), anyhow::Error> {
    let _tracing = crate::init_tracing(Some("integration=debug,info"));
    test_request_context()
        .scope(async {
            for engine in [Engine::DuckDb, Engine::Cayenne] {
                for (direction, expected) in [("DESC", "tie-seq2"), ("ASC", "tie-seq1")] {
                    let refresh_sql = format!(
                        "SELECT DISTINCT ON (id) * FROM events \
                         ORDER BY id, occurred_at DESC NULLS LAST, seq {direction}"
                    );
                    let case = Case {
                        engine,
                        refresh_mode: RefreshMode::Full,
                        on_conflict: OnConflictBehavior::Upsert,
                        refresh_sql: &refresh_sql,
                    };
                    let temp = tempfile::tempdir()?;
                    let source_dir = temp.path().join("source");
                    std::fs::create_dir_all(&source_dir)?;
                    // The tied rows are in different files, so read order cannot decide.
                    write_parquet(
                        &source_dir.join("a.parquet"),
                        &[(7, "us", Some(6), 1, "tie-seq1")],
                    )
                    .await?;
                    write_parquet(
                        &source_dir.join("b.parquet"),
                        &[(7, "us", Some(6), 2, "tie-seq2")],
                    )
                    .await?;
                    let rt = start(
                        case.dataset(&source_dir, &temp.path().join("accelerator")),
                        "refresh_sql_distinct_on_ties",
                    )
                    .await?;
                    runtime_ready_check(&rt).await;
                    assert_eq!(
                        stored(&rt).await?,
                        rows(&[(7, expected)]),
                        "{}: seq {direction} must break the tie",
                        case.label()
                    );
                }
            }
            Ok(())
        })
        .await
}

/// A `DISTINCT ON` refresh SQL on a dataset with no primary key is refused: nothing
/// could match its rows to the ones already loaded.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn distinct_on_without_primary_key_is_refused() -> Result<(), anyhow::Error> {
    let _tracing = crate::init_tracing(Some("integration=debug,info"));
    test_request_context()
        .scope(async {
            let temp = tempfile::tempdir()?;
            let source_dir = temp.path().join("source");
            std::fs::create_dir_all(&source_dir)?;
            write_parquet(&source_dir.join("a.parquet"), FILE_A).await?;
            let mut dataset = Case {
                engine: Engine::DuckDb,
                refresh_mode: RefreshMode::Full,
                on_conflict: OnConflictBehavior::Upsert,
                refresh_sql: LATEST_PER_KEY,
            }
            .dataset(&source_dir, &temp.path().join("accelerator"));
            if let Some(acceleration) = dataset.acceleration.as_mut() {
                acceleration.primary_key = None;
                acceleration.on_conflict = HashMap::new();
            }
            crate::configure_test_datafusion();
            let app = AppBuilder::new("refresh_sql_distinct_on_no_pk")
                .with_dataset(dataset)
                .build();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);
            // The dataset is refused, so loading finishes with it in an error state
            // rather than retrying; a timeout here means it is being retried.
            tokio::time::timeout(Duration::from_mins(1), Arc::clone(&rt).load_components())
                .await
                .map_err(|_| {
                    anyhow::anyhow!(
                        "loading kept retrying the refused dataset instead of failing it"
                    )
                })?;
            assert!(
                runtime_ready_check_with_timeout_err(&rt, Duration::from_secs(10))
                    .await
                    .is_err(),
                "a DISTINCT ON refresh SQL without a primary key must not load"
            );
            assert!(
                run_query(&rt, &format!("SELECT id FROM {TABLE}"))
                    .await
                    .map_or(true, |batches| batches.iter().all(|b| b.num_rows() == 0)),
                "the refused dataset must hold no rows"
            );
            Ok(())
        })
        .await
}
