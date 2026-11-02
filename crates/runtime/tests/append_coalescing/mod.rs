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

//! Concurrent appends to one Cayenne-accelerated `sink` dataset, the shape the
//! OpenTelemetry ingest writes, are written together as one write when they
//! queue behind each other. Every append here goes through
//! `DataFusion::write_data`, so the dataset's accelerator write lock and the
//! accelerated table are in the loop.
//!
//! Each test takes the dataset's append turn, sends appends one at a time until
//! each is queued (so the queue order is known), releases the turn, and checks
//! what each caller was told, which rows the dataset holds, and that the appends
//! really shared writes.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use app::AppBuilder;
use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::TableReference;
use runtime::Runtime;
use runtime::dataupdate::{DataUpdate, UpdateType};
use spicepod::acceleration::{Acceleration, Mode, RefreshMode};
use spicepod::component::access::AccessMode;
use spicepod::component::dataset::Dataset;
use spicepod::param::Params;

use crate::{
    configure_test_datafusion, init_tracing,
    utils::{register_test_connectors, run_query, runtime_ready_check},
};

type Rows = Vec<(i64, i64)>;

/// A `sink` dataset accelerated by Cayenne in file mode, as OpenTelemetry metric
/// datasets are, keyed on `id` when `keyed`. `params` adds acceleration params.
fn make_dataset(name: &str, dir: &Path, keyed: bool, params: &[(&str, &str)]) -> Dataset {
    let mut acceleration_params = HashMap::from([
        (
            "cayenne_file_path".to_string(),
            dir.join(name).join("data").to_string_lossy().to_string(),
        ),
        (
            "cayenne_metadata_dir".to_string(),
            dir.join(name)
                .join("metadata")
                .to_string_lossy()
                .to_string(),
        ),
    ]);
    for (key, value) in params {
        acceleration_params.insert((*key).to_string(), (*value).to_string());
    }
    let mut dataset = Dataset::new(format!("sink:{name}"), name.to_string());
    dataset.access = AccessMode::ReadWrite;
    dataset.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some("cayenne".to_string()),
        mode: Mode::File,
        // An append refresh needs a key or a time column; a dataset without a key
        // keeps the default refresh mode, and takes appends all the same.
        refresh_mode: keyed.then_some(RefreshMode::Append),
        primary_key: keyed.then(|| "id".to_string()),
        params: Some(Params::from_string_map(acceleration_params)),
        ..Acceleration::default()
    });
    dataset
}

async fn start_runtime(datasets: Vec<Dataset>) -> Arc<Runtime> {
    let mut app = AppBuilder::new("append_coalescing");
    for dataset in datasets {
        app = app.with_dataset(dataset);
    }
    configure_test_datafusion();
    let rt = Arc::new(Runtime::builder().with_app(app.build()).build().await);
    let loading = Arc::clone(&rt);
    tokio::select! {
        () = tokio::time::sleep(Duration::from_mins(1)) => panic!("timed out loading components"),
        () = loading.load_components() => {}
    }
    runtime_ready_check(&rt).await;
    rt
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

fn batch(rows: &[(i64, i64)]) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.0).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            )),
        ],
    )
    .expect("batch")
}

fn rows_of(ids: std::ops::RangeInclusive<i64>, value: i64) -> Rows {
    ids.map(|id| (id, value)).collect()
}

async fn append(rt: &Runtime, table: &TableReference, rows: &[(i64, i64)]) -> anyhow::Result<()> {
    rt.datafusion()
        .write_data(
            table,
            DataUpdate {
                schema: schema(),
                data: vec![batch(rows)],
                update_type: UpdateType::Append,
            },
        )
        .await
        .map_err(|error| anyhow::anyhow!("{error}"))
}

fn spawn_append(
    rt: &Arc<Runtime>,
    table: &TableReference,
    rows: Rows,
) -> tokio::task::JoinHandle<anyhow::Result<()>> {
    let rt = Arc::clone(rt);
    let table = table.clone();
    tokio::spawn(async move { append(&rt, &table, &rows).await })
}

/// Poll until exactly `expected` appends to `table` wait to be written together.
async fn wait_for_queued(rt: &Runtime, table: &TableReference, expected: usize) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let (queued, _, _) = rt.datafusion().append_queue_counts(table);
        if queued == expected {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "expected {expected} queued appends to {table}, found {queued} after 30s"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// Take `table`'s append turn and queue `appends` behind it, in order.
async fn queue_appends(
    rt: &Arc<Runtime>,
    table: &TableReference,
    appends: Vec<Rows>,
) -> (
    tokio::sync::OwnedMutexGuard<()>,
    Vec<tokio::task::JoinHandle<anyhow::Result<()>>>,
) {
    let turn = rt
        .datafusion()
        .hold_append_turn(table)
        .await
        .expect("appends to a file-mode Cayenne sink dataset are written together");
    let mut handles = Vec::with_capacity(appends.len());
    for (queued, rows) in appends.into_iter().enumerate() {
        handles.push(spawn_append(rt, table, rows));
        wait_for_queued(rt, table, queued + 1).await;
    }
    (turn, handles)
}

/// Release the turn and return what each queued append's caller is told.
async fn release(
    turn: tokio::sync::OwnedMutexGuard<()>,
    handles: Vec<tokio::task::JoinHandle<anyhow::Result<()>>>,
) -> Vec<anyhow::Result<()>> {
    drop(turn);
    let mut results = Vec::with_capacity(handles.len());
    for handle in handles {
        results.push(
            tokio::time::timeout(Duration::from_mins(1), handle)
                .await
                .expect("the queued append finished")
                .expect("append task"),
        );
    }
    results
}

/// Every row the dataset holds, sorted.
async fn stored_rows(rt: &Arc<Runtime>, table: &str) -> Rows {
    let batches = run_query(rt, &format!("SELECT id, value FROM {table}"))
        .await
        .expect("query the dataset");
    let mut rows = Vec::new();
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id column");
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("value column");
        rows.extend(
            ids.values()
                .iter()
                .copied()
                .zip(values.values().iter().copied()),
        );
    }
    rows.sort_unstable();
    rows
}

fn last_copy_of_each_key<'a>(writes: impl IntoIterator<Item = &'a Rows>) -> Rows {
    let mut model = BTreeMap::new();
    for rows in writes {
        for &(id, value) in rows {
            model.insert(id, value);
        }
    }
    model.into_iter().collect()
}

/// Appends that share keys keep the last-queued copy of each key, exactly as the
/// same appends written one after another do, and an append that shares a key
/// with an earlier member starts a new group.
#[cfg(not(windows))]
#[tokio::test]
async fn later_queued_append_wins_each_key_it_shares_with_an_earlier_one() {
    let _tracing = init_tracing(Some("integration=debug,info"));
    let dir = tempfile::tempdir().expect("temp dir");
    register_test_connectors().await;
    let rt = start_runtime(vec![
        make_dataset("coalesced", dir.path(), true, &[]),
        make_dataset("serial", dir.path(), true, &[]),
    ])
    .await;
    let coalesced = TableReference::bare("coalesced");
    let serial = TableReference::bare("serial");

    // Key 5 is stored before the appends, which then replace it.
    let seed = [rows_of(1000..=1009, 0), vec![(5, 0)]].concat();
    // The second shares keys 200..=300 with the first, so the first is written
    // alone; the third and fourth share no key with the second or each other, so
    // they join it. The fourth repeats key 700 itself.
    let appends = vec![
        rows_of(1..=300, 1),
        rows_of(200..=500, 2),
        rows_of(600..=650, 3),
        [rows_of(1..=50, 4), vec![(700, 70), (700, 71)]].concat(),
    ];
    for table in [&coalesced, &serial] {
        append(&rt, table, &seed).await.expect("seed");
    }

    let (turn, handles) = queue_appends(&rt, &coalesced, appends.clone()).await;
    for result in release(turn, handles).await {
        result.expect("queued append");
    }
    let (queued, groups, members) = rt.datafusion().append_queue_counts(&coalesced);
    assert_eq!(
        (queued, groups, members),
        (0, 2, 4),
        "the first append was written alone and the other three together"
    );

    // The oracle: the same appends, each written alone, one after another.
    for rows in &appends {
        append(&rt, &serial, rows).await.expect("serial append");
    }
    let (_, groups, members) = rt.datafusion().append_queue_counts(&serial);
    assert_eq!(groups, members, "no oracle append shared a write");

    let expected = last_copy_of_each_key(std::iter::once(&seed).chain(&appends));
    assert_eq!(
        expected.len(),
        562,
        "1..=500, 600..=650, 700, and 10 seeded"
    );
    assert_eq!(expected.iter().find(|row| row.0 == 5), Some(&(5, 4)));
    assert_eq!(expected.iter().find(|row| row.0 == 250), Some(&(250, 2)));
    assert_eq!(expected.iter().find(|row| row.0 == 700), Some(&(700, 71)));
    assert_eq!(stored_rows(&rt, "serial").await, expected);
    assert_eq!(stored_rows(&rt, "coalesced").await, expected);

    rt.shutdown().await;
}

/// Appends to a dataset without a key store every row they send, duplicates
/// included, when written together.
#[cfg(not(windows))]
#[tokio::test]
async fn queued_appends_to_a_dataset_without_a_key_store_every_row() {
    let _tracing = init_tracing(Some("integration=debug,info"));
    let dir = tempfile::tempdir().expect("temp dir");
    register_test_connectors().await;
    let rt = start_runtime(vec![make_dataset("unkeyed", dir.path(), false, &[])]).await;
    let table = TableReference::bare("unkeyed");
    append(&rt, &table, &[(-2, -2)]).await.expect("seed");

    let appends: Vec<Rows> = (0..8)
        .map(|writer| {
            [
                rows_of(writer * 100..=writer * 100 + 99, writer),
                vec![(-1, -1), (-1, -1)],
            ]
            .concat()
        })
        .collect();
    let (turn, handles) = queue_appends(&rt, &table, appends.clone()).await;
    for result in release(turn, handles).await {
        result.expect("queued append");
    }
    let (_, groups, members) = rt.datafusion().append_queue_counts(&table);
    assert_eq!(
        (groups, members),
        (1, 8),
        "the eight appends were written as one"
    );

    let mut expected: Rows = std::iter::once(vec![(-2, -2)])
        .chain(appends)
        .flatten()
        .collect();
    expected.sort_unstable();
    assert_eq!(expected.len(), 817);
    assert_eq!(stored_rows(&rt, "unkeyed").await, expected);

    rt.shutdown().await;
}

/// A shared write that fails fails every append in it, and stores none of their
/// rows.
#[cfg(not(windows))]
#[tokio::test]
async fn a_failed_shared_write_fails_every_append_in_it_and_stores_none() {
    let _tracing = init_tracing(Some("integration=debug,info"));
    let dir = tempfile::tempdir().expect("temp dir");
    register_test_connectors().await;
    // Every write goes to a Vortex file, so it needs the data directory.
    let rt = start_runtime(vec![make_dataset(
        "failing",
        dir.path(),
        true,
        &[("cayenne_inline_max_rows", "0")],
    )])
    .await;
    let table = TableReference::bare("failing");
    let seed = rows_of(1..=10, 0);
    append(&rt, &table, &seed).await.expect("seed");
    let data_dir = dir.path().join("failing").join("data");
    assert!(
        data_dir.is_dir(),
        "{} holds the dataset",
        data_dir.display()
    );

    let (turn, handles) = queue_appends(
        &rt,
        &table,
        vec![
            rows_of(1..=100, 1),
            rows_of(200..=300, 2),
            rows_of(400..=410, 3),
        ],
    )
    .await;
    // A file where the data directory was: the shared write cannot create the
    // directory its new files go in, whoever runs the test.
    let aside = dir.path().join("failing").join("data_aside");
    std::fs::rename(&data_dir, &aside).expect("move the data directory aside");
    std::fs::write(&data_dir, b"not a directory").expect("block the data directory");
    let results = release(turn, handles).await;
    std::fs::remove_file(&data_dir).expect("unblock the data directory");
    std::fs::rename(&aside, &data_dir).expect("restore the data directory");

    let (_, groups, members) = rt.datafusion().append_queue_counts(&table);
    assert_eq!(
        (groups, members),
        (1, 3),
        "the three appends were written as one"
    );
    let errors: Vec<String> = results
        .into_iter()
        .map(|result| {
            result
                .expect_err("no append of a failed write is told it succeeded")
                .to_string()
        })
        .collect();
    assert!(
        errors.iter().all(|error| *error == errors[0]),
        "every append reports the shared write's error: {errors:?}"
    );
    assert!(
        !errors[0].contains("stopped before reporting"),
        "the shared write reported its own failure: {}",
        errors[0]
    );
    assert_eq!(
        stored_rows(&rt, "failing").await,
        seed,
        "the failed write stored none of its rows"
    );

    rt.shutdown().await;
}

/// An append too large to share a write still waits behind the appends queued
/// before it, so its copy of a key they share is the one kept, as when the two
/// are written one after another.
#[cfg(not(windows))]
#[tokio::test]
async fn an_append_too_large_to_share_a_write_keeps_its_place_in_the_queue() {
    let _tracing = init_tracing(Some("integration=debug,info"));
    let dir = tempfile::tempdir().expect("temp dir");
    register_test_connectors().await;
    let rt = start_runtime(vec![make_dataset("large", dir.path(), true, &[])]).await;
    let table = TableReference::bare("large");
    let seed = rows_of(100_000..=100_001, 0);
    append(&rt, &table, &seed).await.expect("seed");

    let earlier = rows_of(1..=10, 1);
    // More rows than one append may bring to a shared write, and key 5 again.
    let later = rows_of(1..=9_000, 2);
    let turn = rt
        .datafusion()
        .hold_append_turn(&table)
        .await
        .expect("appends to a file-mode Cayenne sink dataset are written together");
    let first = spawn_append(&rt, &table, earlier.clone());
    wait_for_queued(&rt, &table, 1).await;
    let second = spawn_append(&rt, &table, later.clone());
    // Queued behind the first rather than written ahead of it.
    wait_for_queued(&rt, &table, 2).await;
    assert!(
        !second.is_finished(),
        "the large append waits for the one queued before it"
    );
    drop(turn);

    first.await.expect("append task").expect("earlier append");
    second.await.expect("append task").expect("later append");
    let (_, groups, members) = rt.datafusion().append_queue_counts(&table);
    assert_eq!(
        (groups, members),
        (2, 2),
        "each written from the queue, in its own write"
    );
    let expected = last_copy_of_each_key([&seed, &earlier, &later]);
    assert_eq!(expected.iter().find(|row| row.0 == 5), Some(&(5, 2)));
    assert_eq!(stored_rows(&rt, "large").await, expected);

    rt.shutdown().await;
}

/// An append whose caller goes away after its rows were claimed into a shared
/// write still gets the work every committed write gets, its broadcast to
/// subscribers among it: the shared write commits its rows whether or not the
/// caller waits.
#[cfg(not(windows))]
#[tokio::test]
async fn an_append_cancelled_after_its_rows_were_claimed_is_still_broadcast() {
    let _tracing = init_tracing(Some("integration=debug,info"));
    let dir = tempfile::tempdir().expect("temp dir");
    register_test_connectors().await;
    let rt = start_runtime(vec![make_dataset("followup", dir.path(), true, &[])]).await;
    let table = TableReference::bare("followup");
    append(&rt, &table, &rows_of(1000..=1001, 0))
        .await
        .expect("seed");
    let mut updates = rt
        .datafusion()
        .data_update_broadcaster()
        .subscribe(&TableReference::full("spice", "public", "followup"))
        .await;

    let cancelled_rows = rows_of(1..=10, 1);
    let kept_rows = rows_of(11..=20, 2);
    let (turn, mut handles) =
        queue_appends(&rt, &table, vec![cancelled_rows.clone(), kept_rows.clone()]).await;
    // Hold the write back behind the dataset's write lock once it is claimed.
    let write_lock = rt
        .datafusion()
        .hold_accelerator_write_lock(&table)
        .await
        .expect("an accelerated dataset");
    drop(turn);
    wait_for_queued(&rt, &table, 0).await;
    let (_, groups, members) = rt.datafusion().append_queue_counts(&table);
    assert_eq!(
        (groups, members),
        (1, 2),
        "both appends were claimed together"
    );

    let kept = handles.pop().expect("kept append");
    let cancelled = handles.pop().expect("cancelled append");
    cancelled.abort();
    assert!(
        cancelled
            .await
            .expect_err("aborted while its write waits")
            .is_cancelled(),
        "the first append's caller went away"
    );
    drop(write_lock);
    kept.await.expect("append task").expect("kept append");

    // The shared write stored both appends' rows.
    let expected = last_copy_of_each_key([&rows_of(1000..=1001, 0), &cancelled_rows, &kept_rows]);
    assert_eq!(stored_rows(&rt, "followup").await, expected);

    // And both were broadcast, the cancelled one's included.
    let mut broadcast: Rows = Vec::new();
    while broadcast.len() < cancelled_rows.len() + kept_rows.len() {
        let update = tokio::time::timeout(Duration::from_secs(10), updates.recv())
            .await
            .unwrap_or_else(|_| {
                panic!("only these rows were broadcast after the shared write: {broadcast:?}")
            })
            .expect("broadcast channel open");
        for batch in &update.data {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("id column");
            let values = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("value column");
            broadcast.extend(
                ids.values()
                    .iter()
                    .copied()
                    .zip(values.values().iter().copied()),
            );
        }
    }
    broadcast.sort_unstable();
    let mut sent = [cancelled_rows, kept_rows].concat();
    sent.sort_unstable();
    assert_eq!(broadcast, sent);

    rt.shutdown().await;
}

/// An append whose caller goes away while it is queued is never written.
#[cfg(not(windows))]
#[tokio::test]
async fn an_append_dropped_while_queued_is_never_written() {
    let _tracing = init_tracing(Some("integration=debug,info"));
    let dir = tempfile::tempdir().expect("temp dir");
    register_test_connectors().await;
    let rt = start_runtime(vec![make_dataset("cancelled", dir.path(), true, &[])]).await;
    let table = TableReference::bare("cancelled");
    let seed = rows_of(1000..=1001, 0);
    append(&rt, &table, &seed).await.expect("seed");

    let turn = rt
        .datafusion()
        .hold_append_turn(&table)
        .await
        .expect("appends to a file-mode Cayenne sink dataset are written together");
    let kept = spawn_append(&rt, &table, rows_of(1..=10, 1));
    wait_for_queued(&rt, &table, 1).await;
    let dropped = spawn_append(&rt, &table, rows_of(11..=20, 2));
    wait_for_queued(&rt, &table, 2).await;
    dropped.abort();
    assert!(
        dropped
            .await
            .expect_err("aborted while queued")
            .is_cancelled(),
        "the second append was cancelled, not finished"
    );
    wait_for_queued(&rt, &table, 1).await;
    drop(turn);

    kept.await.expect("append task").expect("kept append");
    let (_, groups, members) = rt.datafusion().append_queue_counts(&table);
    assert_eq!(
        (groups, members),
        (1, 1),
        "the kept append was written from the queue, alone"
    );
    let mut expected = [seed, rows_of(1..=10, 1)].concat();
    expected.sort_unstable();
    assert_eq!(stored_rows(&rt, "cancelled").await, expected);

    rt.shutdown().await;
}
