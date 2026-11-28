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

//! `retention_sql` on a file-mode Cayenne acceleration under `refresh_mode: full`,
//! and the memory-mode, partitioned and `retention_period` cases beside it.
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
    component::{caching::SQLResultsCacheConfig, dataset::Dataset},
    param::Params,
    partitioning::PartitionedBy,
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

            // Refresh leaves cached SQL results in place; reads after one must skip the cache.
            let app = AppBuilder::new("test_cayenne_retention_sql")
                .with_dataset(make_dataset(&source, &data_path))
                .with_sql_cache(SQLResultsCacheConfig {
                    enabled: false,
                    ..Default::default()
                })
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
                .with_sql_cache(SQLResultsCacheConfig {
                    enabled: false,
                    ..Default::default()
                })
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

            let retention_applied = wait_until_true(std::time::Duration::from_mins(1), || {
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
                .with_sql_cache(SQLResultsCacheConfig {
                    enabled: false,
                    ..Default::default()
                })
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
            let settled = wait_until_true(std::time::Duration::from_mins(1), || {
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
                .with_sql_cache(SQLResultsCacheConfig {
                    enabled: false,
                    ..Default::default()
                })
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

            let emptied = wait_until_true(std::time::Duration::from_mins(1), || {
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

/// `retention_sql` applies to a partitioned file-mode acceleration, in every partition.
///
/// Cayenne refuses `partition_by` in memory mode, so partitioned retention is only
/// reachable in file mode. Retention runs from post-write maintenance after each
/// partition's write commits, so the poll is what waits for it. As in the file-mode
/// test above, the refresh adds a surviving row the initial load cannot produce, so
/// the assertions pass only once the refresh has reloaded every partition and
/// retention has run over it again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cayenne_partitioned_file_mode_applies_retention_sql_in_every_partition()
-> Result<(), anyhow::Error> {
    const PART_TABLE: &str = "cayenne_retention_sql_partitioned_it";

    let _tracing = crate::init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            crate::configure_test_datafusion();

            let temp_dir = tempfile::tempdir()?;
            let source = temp_dir.path().join("scores.csv");
            write_source(&source, INITIAL_ROWS.as_ref())?;

            let mut dataset = Dataset::new(format!("file://{}", source.display()), PART_TABLE);
            dataset.acceleration = Some(Acceleration {
                enabled: true,
                engine: Some("cayenne".to_string()),
                mode: Mode::File,
                refresh_mode: Some(RefreshMode::Full),
                retention_sql: Some(format!(
                    "DELETE FROM {PART_TABLE} WHERE score < {SCORE_FLOOR}"
                )),
                retention_check_enabled: false,
                retention_check_interval: None,
                partition_by: vec![PartitionedBy {
                    name: "expr0".to_string(),
                    expression: "bucket(3, id)".to_string(),
                }],
                params: Some(Params::from_string_map(
                    [(
                        "cayenne_file_path".to_string(),
                        temp_dir.path().join("accelerator").display().to_string(),
                    )]
                    .into_iter()
                    .collect(),
                )),
                ..Acceleration::default()
            });

            let app = AppBuilder::new("test_cayenne_retention_sql_partitioned")
                .with_dataset(dataset)
                .with_sql_cache(SQLResultsCacheConfig {
                    enabled: false,
                    ..Default::default()
                })
                .build();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);
            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
                    return Err(anyhow::Error::msg("Timeout waiting for components to load"));
                }
                () = Arc::clone(&rt).load_components() => {}
            }
            runtime_ready_check(&rt).await;

            // The scan must read the partitions, or this is the unpartitioned test again.
            let plan = run_query(&rt, &format!("EXPLAIN SELECT id FROM {PART_TABLE}")).await?;
            let plan = arrow::util::pretty::pretty_format_batches(&plan)?.to_string();
            assert!(
                plan.contains("PartitionedUnionExec"),
                "{PART_TABLE} sets `partition_by` but its scan reads no partitions:\n{plan}"
            );

            let mut rows = INITIAL_ROWS.to_vec();
            rows.push(ADDED_ROW);
            write_source(&source, &rows)?;
            trigger_refresh(&rt, PART_TABLE).await?;

            let expected: Vec<(i64, i64)> = rows
                .iter()
                .copied()
                .filter(|(_, score)| *score >= SCORE_FLOOR)
                .collect();
            let survivors = i64::try_from(expected.len())?;
            let retention_applied = wait_until_true(std::time::Duration::from_mins(1), || {
                let rt = Arc::clone(&rt);
                async move {
                    row_count(&rt, PART_TABLE)
                        .await
                        .is_ok_and(|n| n == survivors)
                }
            })
            .await;
            assert!(
                retention_applied,
                "retention_sql must remove every row below {SCORE_FLOOR} from a partitioned \
                 acceleration; the table still holds {} row(s)",
                row_count(&rt, PART_TABLE).await?
            );

            let served = run_query(
                &rt,
                &format!("SELECT id, score FROM {PART_TABLE} ORDER BY id"),
            )
            .await?;
            let lines = expected_table(&expected);
            let rendered: Vec<&str> = lines.iter().map(String::as_str).collect();
            assert_batches_eq!(&rendered, &served);

            Ok(())
        })
        .await
}

/// Rows this many minutes old sit outside the one-hour `retention_period` below and
/// inside its 24-hour `refresh_data_window`.
const STALE_AGE_MINUTES: i64 = 180;
/// The source's stale rows (ids `0..STALE_ROWS`) and fresh rows (the next
/// `FRESH_ROWS` ids).
const STALE_ROWS: i64 = 5;
const FRESH_ROWS: i64 = 5;

/// Write an `id,ts` source whose first [`STALE_ROWS`] rows are [`STALE_AGE_MINUTES`]
/// old and whose last [`FRESH_ROWS`] rows are seconds old, against the process clock.
fn write_straddling_source(path: &std::path::Path) -> Result<(), anyhow::Error> {
    use std::fmt::Write as _;

    let now = chrono::Utc::now();
    let mut csv = String::from("id,ts\n");
    for id in 0..STALE_ROWS + FRESH_ROWS {
        let ts = if id < STALE_ROWS {
            now - chrono::Duration::minutes(STALE_AGE_MINUTES + id)
        } else {
            now - chrono::Duration::seconds(id)
        };
        writeln!(csv, "{id},{}", ts.format("%Y-%m-%dT%H:%M:%S%.6fZ"))?;
    }
    std::fs::write(path, csv)?;
    Ok(())
}

/// A memory acceleration of `source` with a `time_column` and a 24-hour
/// `refresh_data_window`, so the stale rows load. `retention` is the
/// `(retention_period, retention_check_interval)` to set, if any.
fn make_time_dataset(
    source: &std::path::Path,
    name: &str,
    retention: Option<(&str, Option<&str>)>,
) -> Dataset {
    let mut dataset = Dataset::new(format!("file://{}", source.display()), name);
    dataset.time_column = Some("ts".to_string());
    dataset.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some("cayenne".to_string()),
        mode: Mode::Memory,
        refresh_mode: Some(RefreshMode::Full),
        refresh_data_window: Some("24h".to_string()),
        retention_period: retention.map(|(period, _)| period.to_string()),
        retention_check_enabled: retention.is_some(),
        retention_check_interval: retention
            .and_then(|(_, interval)| interval.map(ToString::to_string)),
        ..Acceleration::default()
    });
    dataset
}

/// `retention_period` hides expired rows from every read on Cayenne, whether or not
/// `retention_check_interval` is set.
///
/// `retention_check_interval` has no default, so a policy without it schedules no
/// retention pass (#13804); on Cayenne that leaves the storage unreclaimed, which the
/// accelerator warns about at load, but never serves an expired row, because Cayenne
/// applies `retention_period` as a filter on every scan. The third dataset is the same
/// source and refresh window with no `retention_period`: it must serve every row,
/// which is what shows the stale rows were loaded and the other two hide them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cayenne_time_retention_hides_expired_rows_with_or_without_a_check_interval()
-> Result<(), anyhow::Error> {
    const WITH_INTERVAL: &str = "cayenne_time_retention_with_interval";
    const WITHOUT_INTERVAL: &str = "cayenne_time_retention_without_interval";
    const NO_RETENTION: &str = "cayenne_time_retention_none";

    let _tracing = crate::init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            crate::configure_test_datafusion();

            let temp_dir = tempfile::tempdir()?;
            let source = temp_dir.path().join("events.csv");
            write_straddling_source(&source)?;

            let app = AppBuilder::new("test_cayenne_time_retention")
                .with_dataset(make_time_dataset(
                    &source,
                    WITH_INTERVAL,
                    Some(("3600s", Some("1s"))),
                ))
                .with_dataset(make_time_dataset(
                    &source,
                    WITHOUT_INTERVAL,
                    Some(("3600s", None)),
                ))
                .with_dataset(make_time_dataset(&source, NO_RETENTION, None))
                .with_sql_cache(SQLResultsCacheConfig {
                    enabled: false,
                    ..Default::default()
                })
                .build();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);
            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
                    return Err(anyhow::Error::msg("Timeout waiting for components to load"));
                }
                () = Arc::clone(&rt).load_components() => {}
            }
            runtime_ready_check(&rt).await;

            let all = STALE_ROWS + FRESH_ROWS;
            let loaded = wait_until_true(std::time::Duration::from_secs(30), || {
                let rt = Arc::clone(&rt);
                async move { row_count(&rt, NO_RETENTION).await.is_ok_and(|n| n == all) }
            })
            .await;
            assert!(
                loaded,
                "with no retention_period the acceleration must serve all {all} rows, but it \
                 holds {}: the refresh window did not admit the stale rows, so this test \
                 cannot observe retention",
                row_count(&rt, NO_RETENTION).await?
            );

            let fresh_ids = [
                "+----+", "| id |", "+----+", "| 5  |", "| 6  |", "| 7  |", "| 8  |", "| 9  |",
                "+----+",
            ];
            for table in [WITH_INTERVAL, WITHOUT_INTERVAL] {
                let rows = run_query(&rt, &format!("SELECT id FROM {table} ORDER BY id")).await?;
                assert_batches_eq!(&fresh_ids, &rows);
            }

            Ok(())
        })
        .await
}
