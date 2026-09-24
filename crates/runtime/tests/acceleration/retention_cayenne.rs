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
//! A full refresh reloads every source row, so the retention predicate has to run again
//! over the new snapshot on every refresh — the way the `DuckDB` accelerator applies it
//! before each refresh commit. Cayenne applies it from its post-write maintenance loop,
//! so the rows disappear shortly *after* the refresh commits rather than inside the
//! refresh write path; the polling below is what that difference costs the test.
//!
//! The second refresh is the point of the test, not a repeat of the first, and it adds a
//! row to the source so that it has an outcome the first refresh cannot produce. Waiting
//! on "no row violates the predicate" alone would be satisfied by the state the initial
//! load already left, so the test would pass without the second refresh ever running —
//! which is precisely the regression it exists to catch.

use std::sync::Arc;

use app::AppBuilder;
use arrow::array::RecordBatch;
use datafusion::assert_batches_eq;
use runtime::Runtime;
use spicepod::{
    acceleration::{Acceleration, Mode, RefreshMode},
    component::dataset::Dataset,
    param::Params,
};

use crate::acceleration::{row_count, trigger_refresh};
use crate::utils::{run_query, runtime_ready_check, test_request_context, wait_until_true};

/// The accelerated table under test.
const TABLE: &str = "cayenne_retention_sql_it";
/// The `mode: memory` twin of [`TABLE`]. Distinct so the two tests never share an
/// acceleration or a metastore slice.
const MEM_TABLE: &str = "cayenne_retention_sql_memory_it";
/// A second `mode: memory` table, over a fixture whose `score` has NULLs. Its own
/// acceleration so it never shares a tier with [`MEM_TABLE`].
const MEM_NULL_TABLE: &str = "cayenne_retention_sql_memory_null_it";
/// A third `mode: memory` table, whose `retention_sql` predicate matches every row.
const MEM_ALL_TABLE: &str = "cayenne_retention_sql_memory_all_it";

/// Rows scoring below this are deleted by the retention predicate.
const SCORE_FLOOR: i64 = 90;

/// The source's initial rows as `(id, score)`. Seven fall below the floor, so retention
/// has something to delete and the reload has something to bring back.
const INITIAL_ROWS: [(i64, i64); 10] = [
    (1, 85),
    (2, 92),
    (3, 78),
    (4, 89),
    (5, 76),
    (6, 94),
    (7, 81),
    (8, 88),
    (9, 79),
    (10, 90),
];

/// The row added before the second refresh. It survives the predicate, so it can only
/// appear once that refresh has reloaded the source and retention has run over it again.
const ADDED_ROW: (i64, i64) = (11, 95);

/// Write `rows` to `path` as CSV, replacing whatever is there.
fn write_source(path: &std::path::Path, rows: &[(i64, i64)]) -> Result<(), anyhow::Error> {
    let rows: Vec<(i64, Option<i64>)> = rows.iter().map(|&(id, score)| (id, Some(score))).collect();
    write_nullable_source(path, &rows)
}

/// Write `rows` to `path` as CSV, an absent `score` written as an empty field.
fn write_nullable_source(
    path: &std::path::Path,
    rows: &[(i64, Option<i64>)],
) -> Result<(), anyhow::Error> {
    use std::fmt::Write as _;

    let mut csv = String::from("id,score\n");
    for (id, score) in rows {
        match score {
            Some(score) => writeln!(csv, "{id},{score}")?,
            None => writeln!(csv, "{id},")?,
        }
    }
    std::fs::write(path, csv)?;
    Ok(())
}

/// The pretty-printed table `rows` should render as, for `assert_batches_eq!`.
fn expected_table(rows: &[(i64, i64)]) -> Vec<String> {
    let mut lines = vec![
        "+----+-------+".to_string(),
        "| id | score |".to_string(),
        "+----+-------+".to_string(),
    ];
    for (id, score) in rows {
        lines.push(format!("| {id: <2} | {score: <5} |"));
    }
    lines.push("+----+-------+".to_string());
    lines
}

async fn run_sql(rt: &Arc<Runtime>, sql: &str) -> Result<Vec<RecordBatch>, anyhow::Error> {
    run_query(rt, sql).await
}

/// Wait for the acceleration to hold exactly `expected` rows with none violating the
/// predicate, then check that those are the rows retention was supposed to leave.
///
/// Both halves matter: the count is what makes the state specific to this refresh, and
/// the predicate check is what proves retention ran rather than the reload simply
/// landing.
async fn assert_retention_left(
    rt: &Arc<Runtime>,
    round: &str,
    expected: &[(i64, i64)],
) -> Result<(), anyhow::Error> {
    let want = i64::try_from(expected.len())?;
    let settled = wait_until_true(std::time::Duration::from_mins(1), || async {
        row_count(rt, TABLE).await.is_ok_and(|c| c == want)
    })
    .await;
    if !settled {
        return Err(anyhow::anyhow!(
            "after the {round} refresh the acceleration holds {} row(s), expected {want}: \
             the reload or the retention that follows it did not complete",
            row_count(rt, TABLE).await?
        ));
    }

    let retained = run_sql(rt, &format!("SELECT id, score FROM {TABLE} ORDER BY id")).await?;
    let lines = expected_table(expected);
    let rendered: Vec<&str> = lines.iter().map(String::as_str).collect();
    assert_batches_eq!(&rendered, &retained);
    Ok(())
}

/// The `mode: memory` twin of [`make_dataset`]. A memory acceleration builds its
/// data and metastore in RAM, so it takes no `cayenne_file_path`.
fn make_memory_dataset(source: &std::path::Path, table: &str) -> Dataset {
    make_memory_dataset_with_predicate(source, table, &format!("score < {SCORE_FLOOR}"))
}

/// [`make_memory_dataset`] with the `retention_sql` WHERE clause spelled out, for the
/// predicate shapes the score floor cannot express.
fn make_memory_dataset_with_predicate(
    source: &std::path::Path,
    table: &str,
    predicate: &str,
) -> Dataset {
    let mut dataset = Dataset::new(format!("file://{}", source.display()), table);
    dataset.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some("cayenne".to_string()),
        mode: Mode::Memory,
        refresh_mode: Some(RefreshMode::Full),
        retention_sql: Some(format!("DELETE FROM {table} WHERE {predicate}")),
        retention_check_enabled: false,
        retention_check_interval: None,
        ..Acceleration::default()
    });
    dataset
}

fn make_dataset(source: &std::path::Path, data_path: &std::path::Path) -> Dataset {
    let mut dataset = Dataset::new(format!("file://{}", source.display()), TABLE);
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
    dataset
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
            let source = temp_dir.path().join("scores.csv");
            let data_path = temp_dir.path().join("accelerator");
            let mut rows = INITIAL_ROWS.to_vec();
            write_source(&source, &rows)?;

            let app = AppBuilder::new("test_cayenne_retention_sql")
                .with_dataset(make_dataset(&source, &data_path))
                .build();

            let rt = Arc::new(Runtime::builder().with_app(app).build().await);

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
                    return Err(anyhow::Error::msg("Timeout waiting for components to load"));
                }
                () = Arc::clone(&rt).load_components() => {}
            }
            runtime_ready_check(&rt).await;

            let survivors: Vec<(i64, i64)> = INITIAL_ROWS
                .iter()
                .copied()
                .filter(|(_, score)| *score >= SCORE_FLOOR)
                .collect();
            assert_retention_left(&rt, "initial", &survivors).await?;

            // The second refresh reloads the same 10 rows — including the 7 retention
            // deleted — plus one more that survives the predicate. Only that reload can
            // produce the row count below, so this cannot pass on the state above.
            rows.push(ADDED_ROW);
            write_source(&source, &rows)?;
            trigger_refresh(&rt, TABLE).await?;

            let mut expected = survivors;
            expected.push(ADDED_ROW);
            assert_retention_left(&rt, "second", &expected).await?;

            Ok(())
        })
        .await
}

/// `retention_sql` removes rows from a `mode: memory` acceleration (regression test
/// for #14045).
///
/// `apply_retention_filters` is the only `build_deletion_vector_sink` caller that
/// used to pass the table `write_lock` INTO the sink instead of holding it, and the
/// mem-tier arm both deletion sinks reach requires the caller to hold that
/// non-reentrant lock — so retention reached the durable tiers and nothing else.
/// Under `mode: memory` those tiers are empty and the RAM tier is the whole table,
/// so a matching row survived for the life of the process while a client `DELETE`
/// over the same predicate removed it.
///
/// The survivors are asserted BY VALUE, not only by count: a repair that deletes
/// the wrong rows, or too many, has to fail this as loudly as one that deletes none.
/// Its file-mode twin above is the control — it must keep passing unchanged, which
/// is what says the shared durable path was not disturbed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[cfg(not(target_os = "windows"))]
async fn cayenne_memory_mode_applies_retention_sql() -> Result<(), anyhow::Error> {
    let _tracing = crate::init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            crate::configure_test_datafusion();

            let temp_dir = tempfile::tempdir()?;
            let source = temp_dir.path().join("scores.csv");
            write_source(&source, INITIAL_ROWS.as_ref())?;

            let app = AppBuilder::new("test_cayenne_retention_sql_memory")
                .with_dataset(make_memory_dataset(&source, MEM_TABLE))
                .build();

            let rt = Arc::new(Runtime::builder().with_app(app).build().await);
            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
                    return Err(anyhow::Error::msg("Timeout waiting for components to load"));
                }
                () = Arc::clone(&rt).load_components() => {}
            }
            runtime_ready_check(&rt).await;

            // Cayenne arms retention from its own post-write maintenance, which is
            // debounced and asynchronous, so the initial load alone does not say when
            // it has run. Refreshing arms that maintenance again, and the poll below
            // is what waits for it rather than a fixed sleep.
            trigger_refresh(&rt, MEM_TABLE).await?;

            let expected: Vec<(i64, i64)> = INITIAL_ROWS
                .iter()
                .copied()
                .filter(|(_, score)| *score >= SCORE_FLOOR)
                .collect();
            let survivors = i64::try_from(expected.len())?;
            let matching = INITIAL_ROWS.len() - expected.len();
            assert!(
                matching > 0,
                "the fixture must contain rows the retention predicate matches, or this \
                 test asserts nothing"
            );

            let retention_applied = wait_until_true(std::time::Duration::from_secs(60), || {
                let rt = Arc::clone(&rt);
                async move {
                    row_count(&rt, MEM_TABLE)
                        .await
                        .is_ok_and(|n| n == survivors)
                }
            })
            .await;

            let rows = run_query(
                &rt,
                &format!("SELECT id, score FROM {MEM_TABLE} ORDER BY id"),
            )
            .await?;
            let below_floor = run_query(
                &rt,
                &format!("SELECT COUNT(*) AS n FROM {MEM_TABLE} WHERE score < {SCORE_FLOOR}"),
            )
            .await?;
            eprintln!(
                "[mode: memory] retention_sql `score < {SCORE_FLOOR}` after a refresh; rows \
                 served:\n{}\nof which below the floor:\n{}",
                arrow::util::pretty::pretty_format_batches(&rows)?,
                arrow::util::pretty::pretty_format_batches(&below_floor)?
            );

            assert!(
                retention_applied,
                "retention_sql must remove the {matching} matching row(s) from a \
                 `mode: memory` acceleration (#14045); the table still holds {} row(s)",
                row_count(&rt, MEM_TABLE).await?
            );

            // By value, not only by count: this is what fails a repair that deletes the
            // wrong rows as loudly as one that deletes none.
            let lines = expected_table(&expected);
            let rendered: Vec<&str> = lines.iter().map(String::as_str).collect();
            assert_batches_eq!(&rendered, &rows);

            Ok(())
        })
        .await
}

/// The NULL-bearing fixture as `(id, Option<score>)`. Two rows match the predicate, two
/// survive it on value, and two cannot be evaluated against it at all.
const NULL_ROWS: [(i64, Option<i64>); 6] = [
    (1, Some(85)),
    (2, Some(92)),
    (3, None),
    (4, Some(76)),
    (5, None),
    (6, Some(95)),
];

/// A row the predicate cannot evaluate is KEPT, and a row it evaluates FALSE is kept too.
///
/// `SQL` deletes only where the predicate is TRUE, so `score < 90` must leave a NULL
/// `score` alone — `NULL < 90` is NULL, not TRUE. This is the direction the ten-row
/// fixture above cannot test, because every one of its rows answers the predicate
/// definitively; it is also the direction that fails silently, since a retention pass
/// that deleted the NULLs would still leave a table with no row below the floor.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[cfg(not(target_os = "windows"))]
async fn cayenne_memory_mode_retention_sql_keeps_a_null_the_predicate_cannot_evaluate()
-> Result<(), anyhow::Error> {
    let _tracing = crate::init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            crate::configure_test_datafusion();

            let temp_dir = tempfile::tempdir()?;
            let source = temp_dir.path().join("nullable_scores.csv");
            write_nullable_source(&source, NULL_ROWS.as_ref())?;

            let app = AppBuilder::new("test_cayenne_retention_sql_memory_null")
                .with_dataset(make_memory_dataset(&source, MEM_NULL_TABLE))
                .build();

            let rt = Arc::new(Runtime::builder().with_app(app).build().await);
            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
                    return Err(anyhow::Error::msg("Timeout waiting for components to load"));
                }
                () = Arc::clone(&rt).load_components() => {}
            }
            runtime_ready_check(&rt).await;
            trigger_refresh(&rt, MEM_NULL_TABLE).await?;

            let survivors = i64::try_from(
                NULL_ROWS
                    .iter()
                    .filter(|(_, score)| score.is_none_or(|score| score >= SCORE_FLOOR))
                    .count(),
            )?;
            let settled = wait_until_true(std::time::Duration::from_secs(60), || {
                let rt = Arc::clone(&rt);
                async move {
                    row_count(&rt, MEM_NULL_TABLE)
                        .await
                        .is_ok_and(|n| n == survivors)
                }
            })
            .await;

            let rows = run_query(
                &rt,
                &format!("SELECT id, score FROM {MEM_NULL_TABLE} ORDER BY id"),
            )
            .await?;
            eprintln!(
                "[mode: memory, nullable] retention_sql `score < {SCORE_FLOOR}`; rows served:\n{}",
                arrow::util::pretty::pretty_format_batches(&rows)?
            );
            assert!(
                settled,
                "retention must leave the {survivors} row(s) the predicate does not answer \
                 TRUE for; the table holds {}",
                row_count(&rt, MEM_NULL_TABLE).await?
            );

            // The NULLs are named individually rather than counted: a pass that deleted
            // them and spared two others would satisfy the count above.
            assert_batches_eq!(
                &[
                    "+----+-------+",
                    "| id | score |",
                    "+----+-------+",
                    "| 2  | 92    |",
                    "| 3  |       |",
                    "| 5  |       |",
                    "| 6  | 95    |",
                    "+----+-------+",
                ],
                &rows
            );

            Ok(())
        })
        .await
}

/// An all-true `retention_sql` empties a `mode: memory` acceleration.
///
/// This shape is why retention calls `delete_mem_tier_rows_matching` directly instead of
/// the wrapper a client `DELETE` composes: that wrapper routes an all-true predicate to a
/// whole-tier purge with no memory-residency gate, which would reach the tier of a table
/// that is not memory-resident and return bytes to a budget a memory-mode write never
/// took from. The bypass is only correct if the filtered arm empties the tier on its own,
/// and that is a claim about `is_delete_all`-shaped input that no other test makes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[cfg(not(target_os = "windows"))]
async fn cayenne_memory_mode_retention_sql_matching_every_row_empties_the_tier()
-> Result<(), anyhow::Error> {
    let _tracing = crate::init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            crate::configure_test_datafusion();

            let temp_dir = tempfile::tempdir()?;
            let source = temp_dir.path().join("all_scores.csv");
            write_source(&source, INITIAL_ROWS.as_ref())?;

            let app = AppBuilder::new("test_cayenne_retention_sql_memory_all")
                .with_dataset(make_memory_dataset_with_predicate(
                    &source,
                    MEM_ALL_TABLE,
                    "true",
                ))
                .build();

            let rt = Arc::new(Runtime::builder().with_app(app).build().await);
            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
                    return Err(anyhow::Error::msg("Timeout waiting for components to load"));
                }
                () = Arc::clone(&rt).load_components() => {}
            }
            runtime_ready_check(&rt).await;
            trigger_refresh(&rt, MEM_ALL_TABLE).await?;

            let emptied = wait_until_true(std::time::Duration::from_secs(60), || {
                let rt = Arc::clone(&rt);
                async move { row_count(&rt, MEM_ALL_TABLE).await.is_ok_and(|n| n == 0) }
            })
            .await;

            let rows = run_query(
                &rt,
                &format!("SELECT id, score FROM {MEM_ALL_TABLE} ORDER BY id"),
            )
            .await?;
            eprintln!(
                "[mode: memory, all-true] retention_sql `true`; rows served:\n{}",
                arrow::util::pretty::pretty_format_batches(&rows)?
            );
            assert!(
                emptied,
                "an all-true retention_sql must empty a `mode: memory` acceleration; it \
                 still holds {} row(s)",
                row_count(&rt, MEM_ALL_TABLE).await?
            );

            Ok(())
        })
        .await
}
