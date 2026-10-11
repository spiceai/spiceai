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

//! A Cayenne acceleration with a primary key and a `time_column` keeps, per key,
//! the row with the greatest `time_column`, whatever order the source returns a
//! key's versions in, and the later arrival of rows with the same time (#14576).
//!
//! Cayenne is run in memory and file mode:
//!
//! - a full refresh, and an append's first load, over two files, each holding the newest version of one key and an
//!   older version of the other, so whichever file is read first one key's older
//!   version arrives first; a key repeated within one batch; and a filler large enough
//!   that the files span several record batches;
//! - an append refresh, where a late row older than the stored version (inside
//!   `refresh_append_overlap`) must not replace it, and a newer row must;
//! - an append refresh with `retention_sql`, which removes a key whose newest version
//!   it matches, in the first load and in a later append;
//! - an append that Cayenne does not take with row versions, so the refresh resolves
//!   them itself: with `cayenne_pk_conflict_detection: none`, and after every row is
//!   deleted while the table's files remain;
//! - a NULL `time_column`, which is older than any time;
//! - a `localpod` child of a file-mode Cayenne parent, which must keep the same
//!   versions as its parent.
#![expect(clippy::expect_used)]

use std::collections::HashMap;
use std::fmt::Write as _;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use app::AppBuilder;
use arrow::array::AsArray;
use runtime::Runtime;
use spicepod::acceleration::{Acceleration, Mode, RefreshMode, WriteMode};
use spicepod::component::access::AccessMode;
use spicepod::component::dataset::Dataset;
use spicepod::param::Params;

use crate::acceleration::{row_count, trigger_refresh};
use crate::configure_test_datafusion;
use crate::utils::{
    run_query, runtime_ready_check_with_timeout_err, test_request_context, wait_until_true,
};

const TABLE: &str = "events";

/// Keys that only fill record batches, so a key's versions land in different batches,
/// and enough of them (well over Cayenne's 4 MiB write buffer) that a load streams.
const FILLER_KEYS: i64 = 300_000;

/// One row for each filler key.
fn filler() -> impl Iterator<Item = (i64, &'static str, &'static str)> {
    (0..FILLER_KEYS).map(|i| (1_000 + i, "2026-01-01T00:00:00", "filler"))
}

fn modes() -> [Mode; 2] {
    [Mode::Memory, Mode::File]
}

fn csv(rows: &[(i64, &str, &str)]) -> String {
    let mut csv = String::from("id,occurred_at,v\n");
    for (id, at, v) in rows {
        writeln!(csv, "{id},{at},{v}").expect("write to a String");
    }
    csv
}

fn write(dir: &Path, name: &str, contents: &str) {
    std::fs::write(dir.join(name), contents).expect("write source file");
}

/// Load a dataset over the CSV files in `source`, returning the runtime and whether it
/// became ready.
async fn load(
    source: &Path,
    accel_dir: &Path,
    mode: &Mode,
    refresh: RefreshMode,
    label: &str,
) -> (Arc<Runtime>, bool) {
    load_with(source, accel_dir, mode, refresh, |_| {}, label).await
}

/// [`load`], with `configure` applied to the dataset first.
async fn load_with(
    source: &Path,
    accel_dir: &Path,
    mode: &Mode,
    refresh: RefreshMode,
    configure: impl FnOnce(&mut Dataset),
    label: &str,
) -> (Arc<Runtime>, bool) {
    let mut params = HashMap::new();
    if *mode == Mode::File {
        params.insert(
            "cayenne_file_path".to_string(),
            accel_dir.join("data").display().to_string(),
        );
        params.insert(
            "cayenne_metadata_dir".to_string(),
            accel_dir.join("meta").display().to_string(),
        );
    }

    let mut dataset = Dataset::new(format!("file://{}/", source.display()), TABLE);
    dataset.time_column = Some("occurred_at".to_string());
    dataset.params = Some(Params::from_string_map(
        [("file_format".to_string(), "csv".to_string())].into(),
    ));
    dataset.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some("cayenne".to_string()),
        mode: mode.clone(),
        refresh_mode: Some(refresh),
        refresh_append_overlap: Some("7d".to_string()),
        params: (!params.is_empty()).then(|| Params::from_string_map(params)),
        primary_key: Some("id".to_string()),
        ..Acceleration::default()
    });
    configure(&mut dataset);

    configure_test_datafusion();
    let app = AppBuilder::new(format!("newest_by_time_{label}"))
        .with_dataset(dataset)
        .build();
    let rt = Arc::new(Runtime::builder().with_app(app).build().await);
    tokio::select! {
        () = tokio::time::sleep(Duration::from_mins(2)) => panic!("{label}: load timed out"),
        () = Arc::clone(&rt).load_components() => {}
    }
    let ready = runtime_ready_check_with_timeout_err(&rt, Duration::from_secs(15))
        .await
        .is_ok();
    (rt, ready)
}

/// The acceleration [`load_with`] gives every dataset.
fn acceleration(dataset: &mut Dataset) -> &mut Acceleration {
    dataset
        .acceleration
        .as_mut()
        .expect("the dataset is accelerated")
}

/// Every stored `v` for `id`, so a duplicate key shows up as two values.
async fn values_of(rt: &Arc<Runtime>, id: i64) -> Vec<String> {
    values_in(rt, TABLE, id).await
}

/// Every stored `v` for `id` in `table`.
async fn values_in(rt: &Arc<Runtime>, table: &str, id: i64) -> Vec<String> {
    run_query(rt, &format!("SELECT v FROM {table} WHERE id = {id}"))
        .await
        .expect("query a key")
        .iter()
        .flat_map(|batch| {
            let values = batch.column(0).as_string::<i32>();
            (0..batch.num_rows())
                .map(|row| values.value(row).to_string())
                .collect::<Vec<_>>()
        })
        .collect()
}

async fn rows(rt: &Arc<Runtime>) -> i64 {
    row_count(rt, TABLE).await.expect("count rows")
}

#[tokio::test]
async fn full_refresh_keeps_the_newest_version_of_each_key() {
    test_request_context()
        .scope(async {
            // An append's first load reads the whole source too, so it must resolve the
            // same way.
            for (mode, refresh) in modes().into_iter().flat_map(|mode| {
                [RefreshMode::Full, RefreshMode::Append].map(move |r| (mode.clone(), r))
            }) {
                let label = format!("first_load_{refresh:?}_{mode:?}");
                let source = tempfile::tempdir().expect("source dir");
                let accel = tempfile::tempdir().expect("acceleration dir");

                let mut a = vec![
                    (1, "2026-01-10T00:00:00", "id1-newest"),
                    (2, "2026-01-05T00:00:00", "id2-older"),
                    (3, "2026-01-01T00:00:00", "id3-only"),
                    (5, "2026-01-04T00:00:00", "id5-tie-a"),
                ];
                a.extend(filler());
                write(source.path(), "a.csv", &csv(&a));
                write(
                    source.path(),
                    "b.csv",
                    &csv(&[
                        (1, "2026-01-08T00:00:00", "id1-older"),
                        (2, "2026-01-07T00:00:00", "id2-newest"),
                        (4, "2026-01-02T00:00:00", "id4-older"),
                        (4, "2026-01-03T00:00:00", "id4-newest"),
                        (5, "2026-01-04T00:00:00", "id5-tie-b"),
                    ]),
                );

                let (rt, ready) = load(source.path(), accel.path(), &mode, refresh, &label).await;
                assert!(ready, "{label}: the dataset should load");

                assert_eq!(values_of(&rt, 1).await, ["id1-newest"], "{label}: key 1");
                assert_eq!(values_of(&rt, 2).await, ["id2-newest"], "{label}: key 2");
                assert_eq!(values_of(&rt, 3).await, ["id3-only"], "{label}: key 3");
                assert_eq!(values_of(&rt, 4).await, ["id4-newest"], "{label}: key 4");
                // Equal times keep the tied row that arrived last, which depends on
                // the order the files are read in; either way, one row.
                assert_eq!(values_of(&rt, 5).await.len(), 1, "{label}: key 5");
                assert_eq!(rows(&rt).await, 5 + FILLER_KEYS, "{label}: one row per key");
            }
        })
        .await;
}

#[tokio::test]
async fn append_refresh_keeps_a_stored_newer_version_and_takes_a_newer_one() {
    test_request_context()
        .scope(async {
            for mode in modes() {
                let label = format!("append_{mode:?}");
                let source = tempfile::tempdir().expect("source dir");
                let accel = tempfile::tempdir().expect("acceleration dir");
                write(
                    source.path(),
                    "a.csv",
                    &csv(&[
                        (1, "2026-01-10T00:00:00", "stored-jan10"),
                        (9, "2026-01-10T00:00:00", "other-key"),
                    ]),
                );

                let (rt, ready) = load(
                    source.path(),
                    accel.path(),
                    &mode,
                    RefreshMode::Append,
                    &label,
                )
                .await;
                assert!(ready, "{label}: the dataset should load");
                assert_eq!(rows(&rt).await, 2, "{label}: initial load");

                // A late row for key 1, older than the stored version but inside the
                // overlap, alongside a new key whose arrival shows the refresh ran.
                write(
                    source.path(),
                    "b.csv",
                    &csv(&[
                        (1, "2026-01-08T00:00:00", "late-jan8"),
                        (10, "2026-01-09T00:00:00", "new-key"),
                    ]),
                );
                trigger_refresh(&rt, TABLE).await.expect("refresh");
                assert!(
                    wait_until_true(Duration::from_mins(1), || async { rows(&rt).await == 3 })
                        .await,
                    "{label}: the late refresh never added key 10 ({} rows)",
                    rows(&rt).await
                );
                assert_eq!(
                    values_of(&rt, 1).await,
                    ["stored-jan10"],
                    "{label}: a late, older row must not replace the stored newer one"
                );

                write(
                    source.path(),
                    "c.csv",
                    &csv(&[(1, "2026-01-12T00:00:00", "newer-jan12")]),
                );
                trigger_refresh(&rt, TABLE).await.expect("refresh");
                assert!(
                    wait_until_true(Duration::from_mins(1), || async {
                        values_of(&rt, 1).await == ["newer-jan12"]
                    })
                    .await,
                    "{label}: the newer row never replaced the stored one: {:?}",
                    values_of(&rt, 1).await
                );
                assert_eq!(rows(&rt).await, 3, "{label}: one row per key");
            }
        })
        .await;
}

/// An append with `retention_sql` loads (regression test for #14876), and Cayenne
/// deletes the rows it matches after each key's newest version is chosen: a key whose
/// newest version matches is gone, and an older version of it does not take its place.
#[tokio::test]
async fn append_refresh_with_retention_sql_keeps_the_newest_version_of_each_key() {
    test_request_context()
        .scope(async {
            for mode in modes() {
                let label = format!("append_retention_{mode:?}");
                let source = tempfile::tempdir().expect("source dir");
                let accel = tempfile::tempdir().expect("acceleration dir");

                let mut a = vec![
                    (1, "2026-01-10T00:00:00", "id1-newest"),
                    (2, "2026-01-05T00:00:00", "id2-older"),
                    (3, "2026-01-01T00:00:00", "expired"),
                    (7, "2026-01-02T00:00:00", "id7-older"),
                ];
                a.extend(filler());
                write(source.path(), "a.csv", &csv(&a));
                write(
                    source.path(),
                    "b.csv",
                    &csv(&[
                        (1, "2026-01-08T00:00:00", "id1-older"),
                        (2, "2026-01-07T00:00:00", "id2-newest"),
                        (7, "2026-01-09T00:00:00", "expired"),
                    ]),
                );

                let (rt, ready) = load_with(
                    source.path(),
                    accel.path(),
                    &mode,
                    RefreshMode::Append,
                    |dataset| {
                        acceleration(dataset).retention_sql =
                            Some(format!("DELETE FROM {TABLE} WHERE v = 'expired'"));
                    },
                    &label,
                )
                .await;
                assert!(ready, "{label}: the dataset should load");
                assert_eq!(values_of(&rt, 1).await, ["id1-newest"], "{label}: key 1");
                assert_eq!(values_of(&rt, 2).await, ["id2-newest"], "{label}: key 2");
                assert!(
                    wait_until_true(Duration::from_mins(1), || async {
                        values_of(&rt, 3).await.is_empty() && values_of(&rt, 7).await.is_empty()
                    })
                    .await,
                    "{label}: retention never removed the expired keys: key 3 {:?}, key 7 {:?}",
                    values_of(&rt, 3).await,
                    values_of(&rt, 7).await
                );
                assert_eq!(rows(&rt).await, 2 + FILLER_KEYS, "{label}: initial load");

                write(
                    source.path(),
                    "c.csv",
                    &csv(&[
                        (2, "2026-01-12T00:00:00", "expired"),
                        (8, "2026-01-11T00:00:00", "new-key"),
                    ]),
                );
                trigger_refresh(&rt, TABLE).await.expect("refresh");
                assert!(
                    wait_until_true(Duration::from_mins(1), || async {
                        values_of(&rt, 8).await == ["new-key"] && values_of(&rt, 2).await.is_empty()
                    })
                    .await,
                    "{label}: after the append, key 8 is {:?} and key 2 (newest version expired) is {:?}",
                    values_of(&rt, 8).await,
                    values_of(&rt, 2).await
                );
                assert_eq!(rows(&rt).await, 2 + FILLER_KEYS, "{label}: after the append");
            }
        })
        .await;
}

/// With `cayenne_pk_conflict_detection: none` the table resolves no keys, so it does
/// not take an append's first load with row versions and the refresh resolves them
/// before writing: the dataset loads, and so does a later append (regression test for
/// #14883). The source's keys are unique, as that setting requires.
#[tokio::test]
async fn an_append_with_pk_conflict_detection_none_loads() {
    test_request_context()
        .scope(async {
            for mode in modes() {
                let label = format!("append_pk_conflict_detection_none_{mode:?}");
                let source = tempfile::tempdir().expect("source dir");
                let accel = tempfile::tempdir().expect("acceleration dir");
                write(
                    source.path(),
                    "a.csv",
                    &csv(&[
                        (1, "2026-01-10T00:00:00", "id1"),
                        (2, "2026-01-05T00:00:00", "id2"),
                    ]),
                );
                write(
                    source.path(),
                    "b.csv",
                    &csv(&[(3, "2026-01-08T00:00:00", "id3")]),
                );

                let (rt, ready) = load_with(
                    source.path(),
                    accel.path(),
                    &mode,
                    RefreshMode::Append,
                    |dataset| {
                        let acceleration = acceleration(dataset);
                        let mut params = acceleration
                            .params
                            .as_ref()
                            .map(Params::as_string_map)
                            .unwrap_or_default();
                        params.insert(
                            "cayenne_pk_conflict_detection".to_string(),
                            "none".to_string(),
                        );
                        acceleration.params = Some(Params::from_string_map(params));
                    },
                    &label,
                )
                .await;
                assert!(ready, "{label}: the dataset should load");
                assert_eq!(values_of(&rt, 1).await, ["id1"], "{label}: key 1");
                assert_eq!(values_of(&rt, 2).await, ["id2"], "{label}: key 2");
                assert_eq!(values_of(&rt, 3).await, ["id3"], "{label}: key 3");
                assert_eq!(rows(&rt).await, 3, "{label}: initial load");

                write(
                    source.path(),
                    "c.csv",
                    &csv(&[(4, "2026-01-11T00:00:00", "id4")]),
                );
                trigger_refresh(&rt, TABLE).await.expect("refresh");
                assert!(
                    wait_until_true(Duration::from_mins(1), || async {
                        values_of(&rt, 4).await == ["id4"]
                    })
                    .await,
                    "{label}: the append never loaded key 4: {:?}",
                    values_of(&rt, 4).await
                );
                assert_eq!(rows(&rt).await, 4, "{label}: after the append");
            }
        })
        .await;
}

/// Deleting every row leaves a file-mode table's files in place, so the table holds
/// rows although none is visible, and it does not take an append with row versions.
/// The next append, which has no high-water mark and reads the whole source, must be
/// resolved by the refresh, or every refresh fails.
#[tokio::test]
async fn an_append_after_every_row_is_deleted_reloads_the_source() {
    test_request_context()
        .scope(async {
            let label = "append_after_delete_File";
            let source = tempfile::tempdir().expect("source dir");
            let accel = tempfile::tempdir().expect("acceleration dir");
            // Enough rows that the load is written to data files, not inline rows a
            // delete would rewrite away.
            let mut a = vec![(1, "2026-01-10T00:00:00", "id1")];
            a.extend(filler());
            write(source.path(), "a.csv", &csv(&a));

            let (rt, ready) = load_with(
                source.path(),
                accel.path(),
                &Mode::File,
                RefreshMode::Append,
                |dataset| {
                    dataset.access = AccessMode::ReadWrite;
                    acceleration(dataset).write_mode = WriteMode::Acceleration;
                },
                label,
            )
            .await;
            assert!(ready, "{label}: the dataset should load");
            assert_eq!(rows(&rt).await, 1 + FILLER_KEYS, "{label}: initial load");

            // A predicate the primary-key fast path cannot turn into a key set, so the
            // delete marks rows in the files rather than replacing them.
            run_query(&rt, &format!("DELETE FROM {TABLE} WHERE id % 2 IN (0, 1)"))
                .await
                .expect("delete every row");
            assert_eq!(rows(&rt).await, 0, "{label}: every row deleted");

            write(
                source.path(),
                "b.csv",
                &csv(&[(2, "2026-01-12T00:00:00", "id2")]),
            );
            trigger_refresh(&rt, TABLE).await.expect("refresh");
            assert!(
                wait_until_true(Duration::from_mins(1), || async {
                    values_of(&rt, 2).await == ["id2"]
                })
                .await,
                "{label}: the append never loaded key 2: {:?}",
                values_of(&rt, 2).await
            );
            assert_eq!(values_of(&rt, 1).await, ["id1"], "{label}: key 1 reloaded");
            assert_eq!(
                rows(&rt).await,
                2 + FILLER_KEYS,
                "{label}: after the append"
            );
        })
        .await;
}

#[tokio::test]
async fn a_null_time_column_is_the_oldest_version() {
    test_request_context()
        .scope(async {
            for mode in modes() {
                let label = format!("null_{mode:?}");
                let source = tempfile::tempdir().expect("source dir");
                let accel = tempfile::tempdir().expect("acceleration dir");
                write(
                    source.path(),
                    "a.csv",
                    &csv(&[
                        (1, "", "only-null"),
                        (2, "2026-01-03T00:00:00", "timed"),
                        (2, "", "null"),
                        (3, "", "null"),
                        (3, "2026-01-01T00:00:00", "timed"),
                    ]),
                );

                let (rt, ready) = load(
                    source.path(),
                    accel.path(),
                    &mode,
                    RefreshMode::Full,
                    &label,
                )
                .await;
                assert!(ready, "{label}: a NULL time_column loads");
                assert_eq!(values_of(&rt, 1).await, ["only-null"], "{label}: key 1");
                assert_eq!(values_of(&rt, 2).await, ["timed"], "{label}: key 2");
                assert_eq!(values_of(&rt, 3).await, ["timed"], "{label}: key 3");
                assert_eq!(rows(&rt).await, 3, "{label}: one row per key");
            }
        })
        .await;
}

/// The same, in a write too large to buffer, so a file-mode refresh resolves its keys
/// through the streaming path, which carries each surviving row's time, a NULL one
/// included, in its own columns.
#[tokio::test]
async fn a_null_time_column_is_the_oldest_version_in_a_streamed_write() {
    test_request_context()
        .scope(async {
            for mode in modes() {
                let label = format!("null_streamed_{mode:?}");
                let source = tempfile::tempdir().expect("source dir");
                let accel = tempfile::tempdir().expect("acceleration dir");
                let mut source_rows = vec![
                    (1, "", "only-null"),
                    (2, "2026-01-03T00:00:00", "timed"),
                    (2, "", "null"),
                    (3, "", "null"),
                    (3, "2026-01-01T00:00:00", "timed"),
                ];
                source_rows.extend(filler());
                write(source.path(), "a.csv", &csv(&source_rows));

                let (rt, ready) = load(
                    source.path(),
                    accel.path(),
                    &mode,
                    RefreshMode::Full,
                    &label,
                )
                .await;
                assert!(ready, "{label}: a NULL time_column loads");
                assert_eq!(values_of(&rt, 1).await, ["only-null"], "{label}: key 1");
                assert_eq!(values_of(&rt, 2).await, ["timed"], "{label}: key 2");
                assert_eq!(values_of(&rt, 3).await, ["timed"], "{label}: key 3");
                assert_eq!(rows(&rt).await, 3 + FILLER_KEYS, "{label}: one row per key");
            }
        })
        .await;
}

/// Every stored `v` for `id` in `table`.
/// Refresh `table` and wait for the refresh to apply.
async fn refresh_and_wait(rt: &Arc<Runtime>, table: &str) {
    let waiter = rt
        .datafusion()
        .refresh_table(&datafusion::common::TableReference::from(table), None)
        .await
        .expect("trigger refresh")
        .expect("refresh notifier");
    let outcome = tokio::time::timeout(Duration::from_mins(1), waiter.wait())
        .await
        .expect("refresh finishes within a minute");
    assert!(outcome.is_answered(), "{table}: refresh finished");
}

/// A `localpod` child writes the rows its parent's refresh writes, so a parent that
/// resolves versions as it writes them (unpartitioned file-mode Cayenne) must resolve
/// them before the child sees them: the child keeps the newest version, and a NULL
/// `time_column` loses to a timed one in both.
#[tokio::test]
async fn a_synchronized_child_keeps_the_same_versions_as_its_parent() {
    test_request_context()
        .scope(async {
            let source = tempfile::tempdir().expect("source dir");
            let accel = tempfile::tempdir().expect("acceleration dir");
            write(
                source.path(),
                "a.csv",
                &csv(&[(1, "2026-01-02T00:00:00", "initial")]),
            );

            let mut parent = Dataset::new(format!("file://{}/", source.path().display()), TABLE);
            parent.time_column = Some("occurred_at".to_string());
            parent.params = Some(Params::from_string_map(
                [("file_format".to_string(), "csv".to_string())].into(),
            ));
            parent.acceleration = Some(Acceleration {
                enabled: true,
                engine: Some("cayenne".to_string()),
                mode: Mode::File,
                refresh_mode: Some(RefreshMode::Full),
                params: Some(Params::from_string_map(
                    [
                        (
                            "cayenne_file_path".to_string(),
                            accel.path().join("data").display().to_string(),
                        ),
                        (
                            "cayenne_metadata_dir".to_string(),
                            accel.path().join("meta").display().to_string(),
                        ),
                    ]
                    .into(),
                )),
                primary_key: Some("id".to_string()),
                ..Acceleration::default()
            });
            let mut child = Dataset::new(format!("localpod:{TABLE}"), "events_child");
            child.acceleration = Some(Acceleration {
                enabled: true,
                refresh_mode: Some(RefreshMode::Full),
                ..Acceleration::default()
            });

            configure_test_datafusion();
            let app = AppBuilder::new("newest_by_time_synchronized_child")
                .with_dataset(parent)
                .with_dataset(child)
                .build();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);
            tokio::select! {
                () = tokio::time::sleep(Duration::from_mins(2)) => panic!("load timed out"),
                () = Arc::clone(&rt).load_components() => {}
            }
            runtime_ready_check_with_timeout_err(&rt, Duration::from_secs(30))
                .await
                .expect("ready");

            // Whichever file is read first, key 1's newest version is in `a.csv`.
            write(
                source.path(),
                "a.csv",
                &csv(&[
                    (1, "2026-01-03T00:00:00", "newer"),
                    (2, "2026-01-01T00:00:00", "two"),
                ]),
            );
            write(
                source.path(),
                "b.csv",
                &csv(&[(1, "2026-01-02T12:00:00", "older")]),
            );
            refresh_and_wait(&rt, TABLE).await;
            for table in [TABLE, "events_child"] {
                assert_eq!(values_in(&rt, table, 1).await, ["newer"], "{table}: key 1");
                assert_eq!(values_in(&rt, table, 2).await, ["two"], "{table}: key 2");
            }

            write(
                source.path(),
                "a.csv",
                &csv(&[(1, "2026-01-01T00:00:00", "timed"), (1, "", "null-time")]),
            );
            std::fs::remove_file(source.path().join("b.csv")).expect("remove b.csv");
            refresh_and_wait(&rt, TABLE).await;
            for table in [TABLE, "events_child"] {
                assert_eq!(
                    values_in(&rt, table, 1).await,
                    ["timed"],
                    "{table}: a NULL time_column loses to a timed one"
                );
            }
        })
        .await;
}
