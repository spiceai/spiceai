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

//! `refresh_mode: append` on an Iceberg table follows its snapshots (#15139).
//!
//! The acceleration is a file-mode Cayenne table keyed by `(sort_key, hash_key)` that
//! keeps the newest version of each key by `event_ingestion`. The oracle is the same
//! Iceberg table registered unaccelerated, reduced to its newest row per key. Every
//! scenario compares the two row for row after each refresh.
//!
//! Against a real REST catalog over object storage, in containers.

mod lab;

use std::sync::{Arc, LazyLock};
use std::time::Duration;

use app::AppBuilder;
use arrow::array::{Array as _, AsArray as _, RecordBatch};
use arrow::datatypes::Int64Type;
use data_components::iceberg::append::AppendCursor;
use datafusion::common::TableReference;
use futures::StreamExt as _;
use iceberg::spec::Operation;
use runtime::Runtime;
use runtime::accelerated::refresh::RefreshOverrides;
use runtime::component::dataset::acceleration::RefreshMode;
use runtime::datafusion::query::ResultsCacheMode;
use spicepod::acceleration::Mode;

use crate::acceleration::{load_runtime_datasets, refresh_and_wait};
use crate::utils::{PROMETHEUS, counter_with_label};
use lab::{Accel, Lab, Row, TimeKind, at_text, keys};

/// The accelerated dataset, and the unaccelerated oracle over the same table.
const ACCELERATED: &str = "events";
const SOURCE: &str = "events_source";

/// The value of counter `name` for the accelerated dataset, `0` before it is first
/// recorded.
fn counter(name: &str) -> f64 {
    counter_with_label(&PROMETHEUS, name, "dataset", ACCELERATED)
}

/// Asserts that a counter reading, or the difference of two, equals `expected`.
fn assert_count(recorded: f64, expected: u32, what: &str) {
    assert!(
        (recorded - f64::from(expected)).abs() < f64::EPSILON,
        "{what}: recorded {recorded}, expected {expected}"
    );
}

const ROWS_PROCESSED: &str = "dataset_acceleration_refresh_processed_rows";
const FETCHES_SKIPPED: &str = "dataset_acceleration_refresh_data_fetches_skipped";
const FALLBACKS: &str = "dataset_acceleration_refresh_fallbacks";

/// The runtime serving the accelerated dataset and the oracle, with the initial load
/// done, and the runtime's view of its datasets.
async fn start_spice(
    lab: &Lab,
    accel: &Accel,
) -> anyhow::Result<(Arc<Runtime>, Vec<runtime::component::dataset::Dataset>)> {
    crate::utils::require_process_per_test(
        "these scenarios install the process-global OpenTelemetry meter provider and read exact counts from it",
    );
    // Instruments bind to the provider installed when they are first used, so the
    // registry exists before the runtime does.
    LazyLock::force(&PROMETHEUS);
    crate::configure_test_datafusion();
    let app = AppBuilder::new("iceberg_append_snapshots")
        .with_dataset(lab.dataset(ACCELERATED, Some(accel)))
        .with_dataset(lab.dataset(SOURCE, None))
        .build();
    let rt = Arc::new(Runtime::builder().with_app(app).build().await);
    let datasets = load_runtime_datasets(&rt, Duration::from_mins(3)).await?;
    Ok((rt, datasets))
}

/// Runs one append refresh to completion.
async fn refresh(rt: &Arc<Runtime>) -> anyhow::Result<()> {
    refresh_and_wait(rt, ACCELERATED, Duration::from_mins(3)).await
}

/// Runs one full refresh to completion, as a user repairing the dataset does.
async fn full_refresh(rt: &Arc<Runtime>) -> anyhow::Result<()> {
    one_off_refresh(rt, RefreshMode::Full, None).await
}

/// Runs one refresh in `mode` to completion, reading `sql` in place of the dataset's
/// `refresh_sql` when given, as a user calling the refresh API does.
async fn one_off_refresh(
    rt: &Arc<Runtime>,
    mode: RefreshMode,
    sql: Option<String>,
) -> anyhow::Result<()> {
    let waiter = rt
        .datafusion()
        .refresh_table(
            &TableReference::from(ACCELERATED),
            Some(RefreshOverrides {
                sql,
                mode: Some(mode),
                max_jitter: None,
            }),
        )
        .await?
        .ok_or_else(|| anyhow::anyhow!("no refresh notifier for {ACCELERATED}"))?;
    let outcome = tokio::time::timeout(Duration::from_mins(3), waiter.wait()).await?;
    anyhow::ensure!(
        outcome.is_answered(),
        "the one-off refresh did not complete: {outcome:?}"
    );
    Ok(())
}

/// Runs `sql` against the current table state, past the results cache: the scenarios
/// repeat one query across refreshes, and a cached answer would hide a refresh.
async fn query(rt: &Arc<Runtime>, sql: &str) -> anyhow::Result<Vec<RecordBatch>> {
    let mut result = rt
        .datafusion()
        .query_builder(sql)
        .results_cache_mode(ResultsCacheMode::Bypass)
        .build()
        .run()
        .await?;
    let mut batches = Vec::new();
    while let Some(batch) = result.data.next().await {
        batches.push(batch?);
    }
    Ok(batches)
}

/// `COUNT(*)` of the accelerated dataset, past the results cache.
async fn accelerated_count(rt: &Arc<Runtime>) -> anyhow::Result<i64> {
    let batches = query(rt, &format!("SELECT COUNT(*) AS cnt FROM {ACCELERATED}")).await?;
    let batch = batches
        .iter()
        .find(|b| b.num_rows() > 0)
        .ok_or_else(|| anyhow::anyhow!("count query returned no rows"))?;
    Ok(batch.column(0).as_primitive::<Int64Type>().value(0))
}

/// `(hash_key, event_ingestion as text, val)` rows.
type Rows = Vec<(String, Option<String>, String)>;

fn rows_of(batches: &[RecordBatch]) -> Rows {
    let mut rows = Vec::new();
    for batch in batches {
        // The engines answer with `Utf8`, `LargeUtf8`, or `Utf8View`; read them all as one.
        let text = |i: usize| {
            arrow::compute::cast(batch.column(i), &arrow::datatypes::DataType::Utf8)
                .expect("a string column casts to Utf8")
        };
        let (keys, times, vals) = (text(0), text(1), text(2));
        let keys = keys.as_string::<i32>();
        let times = times.as_string::<i32>();
        let vals = vals.as_string::<i32>();
        for i in 0..batch.num_rows() {
            rows.push((
                keys.value(i).to_string(),
                times.is_valid(i).then(|| times.value(i).to_string()),
                vals.value(i).to_string(),
            ));
        }
    }
    rows
}

/// Every row the acceleration holds, by key.
async fn accelerated_rows(rt: &Arc<Runtime>) -> anyhow::Result<Rows> {
    let batches = query(
        rt,
        &format!(
            "SELECT hash_key, CAST(event_ingestion AS VARCHAR) AS ts, val FROM {ACCELERATED} ORDER BY hash_key, ts, val"
        ),
    )
    .await?;
    Ok(rows_of(&batches))
}

/// The newest row per key in the source table. A NULL time is older than any time.
async fn source_newest_rows(rt: &Arc<Runtime>, time_kind: TimeKind) -> anyhow::Result<Rows> {
    let instant = match time_kind {
        TimeKind::Timestamp => "event_ingestion",
        TimeKind::Text => "CAST(event_ingestion AS TIMESTAMP)",
    };
    let batches = query(
        rt,
        &format!(
            "WITH r AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY sort_key, hash_key ORDER BY {instant} DESC NULLS LAST) AS rn FROM {SOURCE}) \
             SELECT hash_key, CAST(event_ingestion AS VARCHAR) AS ts, val FROM r WHERE rn = 1 ORDER BY hash_key, ts, val"
        ),
    )
    .await?;
    Ok(rows_of(&batches))
}

/// The acceleration holds exactly the source's newest row per key.
async fn assert_matches_source(
    rt: &Arc<Runtime>,
    time_kind: TimeKind,
    step: &str,
) -> anyhow::Result<Rows> {
    let accelerated = accelerated_rows(rt).await?;
    let source = source_newest_rows(rt, time_kind).await?;
    let differing: Vec<String> = accelerated
        .iter()
        .zip(source.iter())
        .filter(|(a, s)| a != s)
        .map(|(a, s)| format!("accelerated={a:?} source={s:?}"))
        .chain(
            accelerated
                .iter()
                .skip(source.len())
                .map(|a| format!("accelerated={a:?} source=None")),
        )
        .chain(
            source
                .iter()
                .skip(accelerated.len())
                .map(|s| format!("accelerated=None source={s:?}")),
        )
        .collect();
    assert!(
        differing.is_empty(),
        "{step}: the acceleration ({} rows) must equal the source's newest row per key ({} rows):\n{}",
        accelerated.len(),
        source.len(),
        differing.join("\n")
    );
    Ok(accelerated)
}

fn row<'a>(rows: &'a Rows, key: &str) -> Option<&'a (String, Option<String>, String)> {
    rows.iter().find(|(k, _, _)| k == key)
}

/// The out-of-order appends of #15139 and the rig in FINDINGS: rows older than
/// `max(time_column) − refresh_append_overlap` are read, a late older version does not
/// overwrite a newer stored one, a late newer version replaces an older stored one, and
/// a repeated refresh adds nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn late_rows_outside_the_overlap_window_are_loaded() -> anyhow::Result<()> {
    let _tracing = crate::init_tracing(Some(
        "integration=debug,data_components=debug,runtime_table=debug,info",
    ));
    let lab = Lab::start(TimeKind::Timestamp).await?;
    // c1: k1..k100 at 10:00.
    lab.append(&keys("k", 1..=100, 0, "v1")).await?;
    let (rt, _) = start_spice(&lab, &Accel::default()).await?;
    assert_eq!(accelerated_count(&rt).await?, 100, "initial load");

    // c2: new keys at 10:30, so max − overlap is 10:10.
    lab.append(&keys("k", 101..=110, 1800, "v1")).await?;
    refresh(&rt).await?;
    assert_matches_source(&rt, TimeKind::Timestamp, "after c2").await?;

    // c3, inside the window: new keys, a newer k1, a stale k101, and a row exactly at
    // the window start. c4, outside it: new keys, a newer k2, one second outside.
    lab.append(
        &[
            keys("k", 201..=205, 900, "late_in"),
            vec![
                Row::at("k1", 720, "v2_in"),
                Row::at("k101", 1200, "stale_in"),
                Row::at("k401", 600, "boundary"),
            ],
        ]
        .concat(),
    )
    .await?;
    lab.append(
        &[
            keys("k", 301..=305, 300, "late_out"),
            vec![
                Row::at("k2", 480, "v2_out"),
                Row::at("k402", 599, "just_outside"),
            ],
        ]
        .concat(),
    )
    .await?;
    refresh(&rt).await?;
    let rows = assert_matches_source(&rt, TimeKind::Timestamp, "after c3 and c4").await?;
    assert_eq!(rows.len(), 122, "100 + 10 + 5 + 1 + 5 + 1 keys");
    assert_eq!(
        row(&rows, "k2"),
        Some(&("k2".to_string(), Some(at_text(480)), "v2_out".to_string())),
        "a late newer version below the window start replaces the stored one"
    );
    assert_eq!(
        row(&rows, "k101"),
        Some(&("k101".to_string(), Some(at_text(1800)), "v1".to_string())),
        "a late older version does not overwrite the newer stored one"
    );
    assert_eq!(
        row(&rows, "k1"),
        Some(&("k1".to_string(), Some(at_text(720)), "v2_in".to_string()))
    );
    for key in ["k301", "k401", "k402"] {
        assert!(
            row(&rows, key).is_some(),
            "{key} was appended late and must load"
        );
    }

    refresh(&rt).await?;
    let again = assert_matches_source(&rt, TimeKind::Timestamp, "after a repeated refresh").await?;
    assert_eq!(again.len(), 122, "a repeated refresh adds no duplicate");
    Ok(())
}

/// A late row with a NULL time is older than any timed version of its key.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_late_null_time_row_does_not_overwrite_a_timed_version() -> anyhow::Result<()> {
    let _tracing = crate::init_tracing(Some(
        "integration=debug,data_components=debug,runtime_table=debug,info",
    ));
    let lab = Lab::start(TimeKind::Timestamp).await?;
    lab.append(&keys("k", 1..=10, 0, "v1")).await?;
    let (rt, _) = start_spice(&lab, &Accel::default()).await?;

    lab.append(&[Row::null_time("k1", "null_v"), Row::null_time("k900", "n")])
        .await?;
    refresh(&rt).await?;
    let rows =
        assert_matches_source(&rt, TimeKind::Timestamp, "after the NULL-time append").await?;
    assert_eq!(
        row(&rows, "k1"),
        Some(&("k1".to_string(), Some(at_text(0)), "v1".to_string())),
        "the timed version is kept"
    );
    assert_eq!(
        row(&rows, "k900"),
        Some(&("k900".to_string(), None, "n".to_string())),
        "a new key loads with its NULL time"
    );
    Ok(())
}

/// A string `time_column` whose values carry different UTC offsets: its file bounds
/// are not instant bounds, so every stored row is compared, as instants.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_string_time_column_with_mixed_offsets_keeps_the_newest_instant() -> anyhow::Result<()> {
    let _tracing = crate::init_tracing(Some(
        "integration=debug,data_components=debug,runtime_table=debug,info",
    ));
    let lab = Lab::start(TimeKind::Text).await?;
    lab.append(&[
        Row::text("k1", "2026-10-14T10:30:00+00:00", "v1"),
        Row::text("k2", "2026-10-14T10:00:00+00:00", "v1"),
    ])
    .await?;
    let (rt, _) = start_spice(&lab, &Accel::default()).await?;

    // Both sort after the stored text, but only k2's is a later instant.
    lab.append(&[
        Row::text("k1", "2026-10-14T11:00:00+02:00", "older_instant"),
        Row::text("k2", "2026-10-14T13:00:00+02:00", "newer_instant"),
    ])
    .await?;
    refresh(&rt).await?;
    let rows = assert_matches_source(&rt, TimeKind::Text, "after the offset append").await?;
    assert_eq!(
        row(&rows, "k1"),
        Some(&(
            "k1".to_string(),
            Some("2026-10-14T10:30:00+00:00".to_string()),
            "v1".to_string()
        )),
        "11:00+02:00 is 09:00Z, older than the stored 10:30Z"
    );
    assert_eq!(
        row(&rows, "k2"),
        Some(&(
            "k2".to_string(),
            Some("2026-10-14T13:00:00+02:00".to_string()),
            "newer_instant".to_string()
        )),
        "13:00+02:00 is 11:00Z, newer than the stored 10:00Z"
    );
    Ok(())
}

/// A compaction of files older than the cursor is skipped: nothing is read, and the
/// rows it rewrote are not stored twice. A later append is still read on its own.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_compaction_of_loaded_files_reads_nothing_and_adds_no_duplicate() -> anyhow::Result<()> {
    let _tracing = crate::init_tracing(Some(
        "integration=debug,data_components=debug,runtime_table=debug,info",
    ));
    let lab = Lab::start(TimeKind::Timestamp).await?;
    let c1 = lab.append(&keys("k", 1..=100, 0, "v1")).await?;
    let c2 = lab.append(&keys("k", 101..=110, 1800, "v1")).await?;
    let (rt, _) = start_spice(&lab, &Accel::default()).await?;
    assert_eq!(accelerated_count(&rt).await?, 110, "initial load");

    let compacted = [lab.added_files(c1).await?, lab.added_files(c2).await?].concat();
    lab.rewrite(
        Operation::Replace,
        &compacted,
        &[
            keys("k", 1..=100, 0, "v1"),
            keys("k", 101..=110, 1800, "v1"),
        ]
        .concat(),
    )
    .await?;
    let processed_before = counter(ROWS_PROCESSED);
    let skipped_before = counter(FETCHES_SKIPPED);
    refresh(&rt).await?;
    assert_count(
        counter(ROWS_PROCESSED) - processed_before,
        0,
        "a replace snapshot changes no row, so none is read",
    );
    assert_count(
        counter(FETCHES_SKIPPED) - skipped_before,
        1,
        "the refresh found nothing to fetch",
    );
    let rows = assert_matches_source(&rt, TimeKind::Timestamp, "after the compaction").await?;
    assert_eq!(rows.len(), 110);

    lab.append(&keys("k", 111..=115, 1900, "v1")).await?;
    let processed_before = counter(ROWS_PROCESSED);
    refresh(&rt).await?;
    assert_count(
        counter(ROWS_PROCESSED) - processed_before,
        5,
        "only the appended file is read",
    );
    assert_matches_source(
        &rt,
        TimeKind::Timestamp,
        "after the append following the compaction",
    )
    .await?;
    Ok(())
}

/// A compaction between two refreshes rewrites the files the interval's appends
/// added. Those appends are still read from their own files, late rows included,
/// and the compacted file is not.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_compaction_inside_the_interval_keeps_the_late_rows() -> anyhow::Result<()> {
    let _tracing = crate::init_tracing(Some(
        "integration=debug,data_components=debug,runtime_table=debug,info",
    ));
    let lab = Lab::start(TimeKind::Timestamp).await?;
    let c1 = lab.append(&keys("k", 1..=100, 0, "v1")).await?;
    lab.append(&keys("k", 101..=110, 1800, "v1")).await?;
    let (rt, _) = start_spice(&lab, &Accel::default()).await?;

    let late = [
        keys("k", 301..=305, 300, "late_out"),
        vec![Row::at("k2", 480, "v2_out")],
    ]
    .concat();
    let newer = keys("k", 111..=112, 2000, "v1");
    let c3 = lab.append(&late).await?;
    let c4 = lab.append(&newer).await?;
    let compacted = [
        lab.added_files(c1).await?,
        lab.added_files(c3).await?,
        lab.added_files(c4).await?,
    ]
    .concat();
    lab.rewrite(
        Operation::Replace,
        &compacted,
        &[keys("k", 1..=100, 0, "v1"), late.clone(), newer.clone()].concat(),
    )
    .await?;

    let processed_before = counter(ROWS_PROCESSED);
    refresh(&rt).await?;
    assert_count(
        counter(ROWS_PROCESSED) - processed_before,
        u32::try_from(late.len() + newer.len())?,
        "the two appends are read from their own files; the compacted file is not",
    );
    let rows = assert_matches_source(&rt, TimeKind::Timestamp, "after the compaction").await?;
    assert_eq!(
        rows.len(),
        117,
        "110 keys, five late new keys, two newer keys; k2 is replaced"
    );
    assert_eq!(
        row(&rows, "k2"),
        Some(&("k2".to_string(), Some(at_text(480)), "v2_out".to_string()))
    );
    assert!(row(&rows, "k301").is_some());
    Ok(())
}

/// Expiring a snapshot inside the interval breaks the chain: that refresh reads by time
/// once, never reloads, and follows snapshots again from the current one. Expiring the
/// saved snapshot itself does not break it.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn an_expired_snapshot_reads_by_time_once() -> anyhow::Result<()> {
    let _tracing = crate::init_tracing(Some(
        "integration=debug,data_components=debug,runtime_table=debug,info",
    ));
    let lab = Lab::start(TimeKind::Timestamp).await?;
    lab.append(&keys("k", 1..=100, 0, "v1")).await?;
    lab.append(&keys("k", 101..=110, 1800, "v1")).await?;
    let (rt, _) = start_spice(&lab, &Accel::default()).await?;
    assert_eq!(accelerated_count(&rt).await?, 110, "initial load");

    // The interval holds a late row and newer rows; the late row's snapshot expires.
    let c3 = lab.append(&[Row::at("k301", 300, "late_out")]).await?;
    lab.append(&keys("k", 111..=115, 1900, "v1")).await?;
    lab.expire(&[c3]).await?;
    let processed_before = counter(ROWS_PROCESSED);
    refresh(&rt).await?;
    assert_count(
        counter(ROWS_PROCESSED) - processed_before,
        5,
        "the time window reads the newer rows and nothing is reloaded",
    );
    let rows = accelerated_rows(&rt).await?;
    assert_eq!(rows.len(), 115);
    assert!(
        row(&rows, "k301").is_none(),
        "the late row is outside the window the fallback reads"
    );

    let skipped_before = counter(FETCHES_SKIPPED);
    refresh(&rt).await?;
    assert_count(
        counter(FETCHES_SKIPPED) - skipped_before,
        1,
        "the fallback set the cursor",
    );

    // Snapshots are followed again: a late row loads.
    let c5 = lab.append(&[Row::at("k302", 310, "late_out")]).await?;
    refresh(&rt).await?;
    assert!(row(&accelerated_rows(&rt).await?, "k302").is_some());

    // The saved snapshot itself expires. Its child's parent pointer still names it,
    // so the chain is intact and following goes on: the late row loads.
    lab.append(&[Row::at("k303", 320, "late_out")]).await?;
    lab.expire(&[c5]).await?;
    let processed_before = counter(ROWS_PROCESSED);
    refresh(&rt).await?;
    assert_count(
        counter(ROWS_PROCESSED) - processed_before,
        1,
        "only the new file is read",
    );
    // Not compared with the source: k301 stays absent from the first fallback by design.
    let rows = accelerated_rows(&rt).await?;
    assert_eq!(
        rows.len(),
        117,
        "nothing reloaded, nothing removed, the new row added"
    );
    assert!(
        row(&rows, "k303").is_some(),
        "following went on past the expired saved snapshot"
    );

    lab.append(&[Row::at("k304", 330, "late_out")]).await?;
    refresh(&rt).await?;
    assert!(row(&accelerated_rows(&rt).await?, "k304").is_some());
    Ok(())
}

/// Rolling the table back makes the saved snapshot a descendant of the current one:
/// that refresh reads by time once; the rolled-back rows stay, since an append refresh
/// never removes a row; later appends are followed from the rolled-back snapshot.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_rollback_reads_by_time_once_and_removes_nothing() -> anyhow::Result<()> {
    let _tracing = crate::init_tracing(Some(
        "integration=debug,data_components=debug,runtime_table=debug,info",
    ));
    let lab = Lab::start(TimeKind::Timestamp).await?;
    let c1 = lab.append(&keys("k", 1..=100, 0, "v1")).await?;
    let (rt, _) = start_spice(&lab, &Accel::default()).await?;
    lab.append(&keys("k", 101..=110, 1800, "v1")).await?;
    refresh(&rt).await?;
    assert_eq!(accelerated_count(&rt).await?, 110);

    lab.set_main(c1).await?;
    refresh(&rt).await?;
    assert_eq!(
        accelerated_count(&rt).await?,
        110,
        "the rolled-back rows stay; nothing is reloaded"
    );

    lab.append(&[
        Row::at("k301", 300, "late_out"),
        Row::at("k111", 1900, "v1"),
    ])
    .await?;
    refresh(&rt).await?;
    let rows = accelerated_rows(&rt).await?;
    assert_eq!(
        rows.len(),
        112,
        "snapshots are followed again from the rolled-back one"
    );
    assert!(row(&rows, "k301").is_some());
    Ok(())
}

/// Each snapshot is decided on its own, so a delete or an overwrite does not hide the
/// late appends committed beside it. With a primary key an overwrite's files are read:
/// keep-newest drops its copies of stored rows and keeps its changed row. A snapshot
/// that only deletes (a retention job) is skipped, and its rows stay.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_delete_or_overwrite_does_not_hide_the_late_rows_beside_it() -> anyhow::Result<()> {
    let _tracing = crate::init_tracing(Some(
        "integration=debug,data_components=debug,runtime_table=debug,info",
    ));
    let lab = Lab::start(TimeKind::Timestamp).await?;
    let c1 = lab.append(&keys("k", 1..=100, 0, "v1")).await?;
    let c2 = lab.append(&keys("k", 101..=110, 1800, "v1")).await?;
    let (rt, _) = start_spice(&lab, &Accel::default()).await?;

    // A copy-on-write update of k5 rewrites c1's file: 99 copies and the new k5.
    let updated: Vec<Row> = keys("k", 1..=100, 0, "v1")
        .into_iter()
        .map(|r| {
            if r.key == "k5" {
                Row::at("k5", 60, "v2")
            } else {
                r
            }
        })
        .collect();
    lab.rewrite(Operation::Overwrite, &lab.added_files(c1).await?, &updated)
        .await?;
    lab.append(&[
        Row::at("k301", 300, "late_out"),
        Row::at("k501", 2400, "in_window"),
    ])
    .await?;
    refresh(&rt).await?;
    let rows = assert_matches_source(
        &rt,
        TimeKind::Timestamp,
        "after an overwrite and a late append",
    )
    .await?;
    assert_eq!(
        rows.len(),
        112,
        "110 keys plus k301 and k501, each stored once"
    );
    assert_eq!(
        row(&rows, "k5"),
        Some(&("k5".to_string(), Some(at_text(60)), "v2".to_string())),
        "the overwrite's changed row loads"
    );
    assert!(
        row(&rows, "k301").is_some(),
        "a late row beside an overwrite loads"
    );

    // A whole-file delete of c2, as a retention job commits, beside a late append.
    lab.rewrite(Operation::Delete, &lab.added_files(c2).await?, &[])
        .await?;
    lab.append(&[Row::at("k302", 310, "late_out")]).await?;
    refresh(&rt).await?;
    let rows = accelerated_rows(&rt).await?;
    assert!(
        row(&rows, "k302").is_some(),
        "a late row beside a delete loads"
    );
    assert!(
        row(&rows, "k101").is_some(),
        "append refresh does not remove deleted rows"
    );
    assert_eq!(rows.len(), 113);
    assert_count(counter(FALLBACKS), 0, "no refresh read by time");
    Ok(())
}

/// An overwrite that writes a row and deletes it in the same commit (a streaming
/// upsert that changed a key twice between commits) is read with its own position
/// deletes: the deleted versions never existed and do not load.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn an_overwrite_is_read_with_its_own_position_deletes() -> anyhow::Result<()> {
    let _tracing = crate::init_tracing(Some(
        "integration=debug,data_components=debug,runtime_table=debug,info",
    ));
    let lab = Lab::start(TimeKind::Timestamp).await?;
    lab.append(&keys("k", 1..=100, 0, "v1")).await?;
    let (rt, _) = start_spice(&lab, &Accel::default()).await?;

    // Positions 0 and 1 are deleted by the commit that writes them: k7 is gone again,
    // and k8's first version gives way to its second, older-stamped one.
    lab.append_with_own_deletes(
        &[
            Row::at("k7", 600, "deleted"),
            Row::at("k8", 700, "deleted"),
            Row::at("k8", 650, "v2"),
            Row::at("k601", 300, "late"),
        ],
        &[0, 1],
    )
    .await?;
    refresh(&rt).await?;
    let rows = assert_matches_source(
        &rt,
        TimeKind::Timestamp,
        "after an overwrite with its own deletes",
    )
    .await?;
    assert_eq!(rows.len(), 101);
    assert_eq!(
        row(&rows, "k8"),
        Some(&("k8".to_string(), Some(at_text(650)), "v2".to_string()))
    );
    assert_eq!(
        row(&rows, "k7"),
        Some(&("k7".to_string(), Some(at_text(0)), "v1".to_string()))
    );
    assert_count(counter(FALLBACKS), 0, "no refresh read by time");
    Ok(())
}

/// Without a primary key a snapshot that only deletes is skipped too, but an overwrite
/// makes that refresh read by time: nothing tells its copies of stored rows from new
/// rows. The refresh after it follows snapshots again.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn an_overwrite_without_a_primary_key_reads_by_time_once() -> anyhow::Result<()> {
    let _tracing = crate::init_tracing(Some(
        "integration=debug,data_components=debug,runtime_table=debug,info",
    ));
    let lab = Lab::start(TimeKind::Timestamp).await?;
    let c1 = lab.append(&keys("k", 1..=100, 0, "v1")).await?;
    let c2 = lab.append(&keys("k", 101..=110, 1800, "v1")).await?;
    let accel = Accel {
        primary_key: false,
        ..Accel::default()
    };
    let (rt, _) = start_spice(&lab, &accel).await?;

    lab.rewrite(Operation::Delete, &lab.added_files(c2).await?, &[])
        .await?;
    lab.append(&[Row::at("k301", 300, "late_out")]).await?;
    refresh(&rt).await?;
    let rows = accelerated_rows(&rt).await?;
    assert!(
        row(&rows, "k301").is_some(),
        "a late row beside a delete loads"
    );
    assert_eq!(
        rows.len(),
        111,
        "the deleted rows stay, nothing is stored twice"
    );
    assert_count(counter(FALLBACKS), 0, "no refresh read by time");

    // A copy-on-write delete of k5 rewrites c1's file without it.
    let without_k5: Vec<Row> = keys("k", 1..=100, 0, "v1")
        .into_iter()
        .filter(|r| r.key != "k5")
        .collect();
    lab.rewrite(
        Operation::Overwrite,
        &lab.added_files(c1).await?,
        &without_k5,
    )
    .await?;
    lab.append(&[
        Row::at("k302", 300, "late_out"),
        Row::at("k501", 2400, "in_window"),
    ])
    .await?;
    refresh(&rt).await?;
    let rows = accelerated_rows(&rt).await?;
    assert!(
        row(&rows, "k501").is_some(),
        "a row inside the window loads"
    );
    assert!(
        row(&rows, "k5").is_some(),
        "append refresh does not remove deleted rows"
    );
    assert!(
        row(&rows, "k302").is_none(),
        "the time-filtered read does not reach a late row outside the window"
    );
    assert_eq!(
        rows.len(),
        112,
        "the overwrite's copies are not stored twice"
    );
    assert_count(counter(FALLBACKS), 1, "the fallback is counted");

    lab.append(&[Row::at("k303", 310, "late_out")]).await?;
    refresh(&rt).await?;
    let rows = accelerated_rows(&rt).await?;
    assert!(
        row(&rows, "k303").is_some(),
        "the next interval is followed again"
    );
    assert_eq!(rows.len(), 113);
    assert_count(counter(FALLBACKS), 1, "the fallback is counted once");
    Ok(())
}

/// A full refresh reads the table pinned to its current snapshot and saves it, so the
/// next append refresh reads only the commits after it and stores nothing twice, with
/// and without a primary key.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_full_refresh_moves_the_cursor() -> anyhow::Result<()> {
    let _tracing = crate::init_tracing(Some(
        "integration=debug,data_components=debug,runtime_table=debug,info",
    ));
    for primary_key in [false, true] {
        let lab = Lab::start(TimeKind::Timestamp).await?;
        lab.append(&keys("k", 1..=100, 0, "v1")).await?;
        let accel = Accel {
            primary_key,
            ..Accel::default()
        };
        let (rt, _) = start_spice(&lab, &accel).await?;

        lab.append(&keys("k", 101..=110, 1800, "v1")).await?;
        full_refresh(&rt).await?;
        assert_eq!(
            accelerated_count(&rt).await?,
            110,
            "primary_key={primary_key}"
        );

        lab.append(&[Row::at("k301", 300, "late")]).await?;
        refresh(&rt).await?;
        let rows = assert_matches_source(
            &rt,
            TimeKind::Timestamp,
            &format!("after the append that follows a full refresh (primary_key={primary_key})"),
        )
        .await?;
        assert_eq!(
            rows.len(),
            111,
            "the fully refreshed rows are not read again"
        );
        rt.shutdown().await;
    }
    Ok(())
}

/// A one-off refresh with its own `refresh_sql` can write the rows of commits after the
/// cursor. The next append refresh does not store them again, which it would without a
/// primary key.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_one_off_refresh_sql_adds_no_duplicate() -> anyhow::Result<()> {
    let _tracing = crate::init_tracing(Some(
        "integration=debug,data_components=debug,runtime_table=debug,info",
    ));
    for mode in [RefreshMode::Full, RefreshMode::Append] {
        let lab = Lab::start(TimeKind::Timestamp).await?;
        lab.append(&keys("k", 1..=100, 0, "v1")).await?;
        let accel = Accel {
            primary_key: false,
            ..Accel::default()
        };
        let (rt, _) = start_spice(&lab, &accel).await?;

        lab.append(&keys("k", 101..=110, 1800, "v1")).await?;
        one_off_refresh(&rt, mode, Some(format!("SELECT * FROM {ACCELERATED}"))).await?;
        assert_eq!(
            accelerated_count(&rt).await?,
            110,
            "after the one-off {mode:?} refresh"
        );

        lab.append(&[Row::at("k301", 300, "late")]).await?;
        refresh(&rt).await?;
        let rows = assert_matches_source(
            &rt,
            TimeKind::Timestamp,
            &format!("after the append that follows a one-off {mode:?} refresh"),
        )
        .await?;
        assert_eq!(
            rows.len(),
            111,
            "the rows the one-off refresh wrote are not stored again"
        );
        rt.shutdown().await;
    }
    Ok(())
}

/// The rows are written before the cursor is saved. A cursor left behind by a crash
/// between the two is absorbed: the rows are read again and dropped as copies, with
/// and without a primary key.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_cursor_behind_the_written_rows_is_absorbed() -> anyhow::Result<()> {
    let _tracing = crate::init_tracing(Some(
        "integration=debug,data_components=debug,runtime_table=debug,info",
    ));
    for primary_key in [true, false] {
        let lab = Lab::start(TimeKind::Timestamp).await?;
        let c1 = lab.append(&keys("k", 1..=100, 0, "v1")).await?;
        let accel = Accel {
            primary_key,
            ..Accel::default()
        };
        let (rt, datasets) = start_spice(&lab, &accel).await?;
        let c2 = lab
            .append(
                &[
                    keys("k", 101..=110, 1800, "v1"),
                    vec![Row::at("k301", 300, "late")],
                ]
                .concat(),
            )
            .await?;
        let lab_uuid = lab.table_uuid().await?;
        refresh(&rt).await?;
        assert_eq!(
            accelerated_count(&rt).await?,
            111,
            "primary_key={primary_key}"
        );

        // The rows of the last refresh are stored, but its cursor save is lost: the
        // store still says the previous commit is written and this one is in flight.
        let dataset = datasets
            .iter()
            .find(|d| d.name.to_string() == ACCELERATED)
            .ok_or_else(|| anyhow::anyhow!("the accelerated dataset is registered"))?;
        let store = runtime::dataaccelerator::spice_sys::append_cursor_store(dataset).await;
        let cursor = |snapshot_id| {
            AppendCursor {
                table_uuid: lab_uuid.clone(),
                snapshot_id,
            }
            .to_json()
        };
        store.save_committed(&cursor(c1)).await?;
        store.save_pending(&cursor(c2)).await?;

        refresh(&rt).await?;
        let rows = assert_matches_source(
            &rt,
            TimeKind::Timestamp,
            &format!("after the replay (primary_key={primary_key})"),
        )
        .await?;
        assert_eq!(rows.len(), 111, "the replayed rows are dropped as copies");
        rt.shutdown().await;
    }
    Ok(())
}

/// Without a primary key, a row the source appends twice in two commits is stored
/// twice: the rows a commit added are new rows, so nothing is compared with them.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn an_unkeyed_dataset_keeps_a_legitimate_duplicate() -> anyhow::Result<()> {
    let _tracing = crate::init_tracing(Some(
        "integration=debug,data_components=debug,runtime_table=debug,info",
    ));
    let lab = Lab::start(TimeKind::Timestamp).await?;
    lab.append(&keys("k", 1..=10, 0, "v1")).await?;
    let accel = Accel {
        primary_key: false,
        ..Accel::default()
    };
    let (rt, _) = start_spice(&lab, &accel).await?;

    // The same row again, in a later commit.
    lab.append(&[Row::at("k5", 0, "v1")]).await?;
    refresh(&rt).await?;
    assert_eq!(accelerated_count(&rt).await?, 11);
    let copies = query(
        &rt,
        &format!("SELECT COUNT(*) AS cnt FROM {ACCELERATED} WHERE hash_key = 'k5'"),
    )
    .await?;
    assert_eq!(
        copies[0].column(0).as_primitive::<Int64Type>().value(0),
        2,
        "both copies of the legitimately duplicated row are stored"
    );

    refresh(&rt).await?;
    assert_eq!(
        accelerated_count(&rt).await?,
        11,
        "a repeated refresh adds nothing"
    );
    Ok(())
}

/// A refresh that finds the table at the snapshot it last loaded scans no data.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn an_unchanged_table_scans_no_data() -> anyhow::Result<()> {
    let _tracing = crate::init_tracing(Some(
        "integration=debug,data_components=debug,runtime_table=debug,info",
    ));
    let lab = Lab::start(TimeKind::Timestamp).await?;
    lab.append(&keys("k", 1..=100, 0, "v1")).await?;
    let (rt, _) = start_spice(&lab, &Accel::default()).await?;

    let processed_before = counter(ROWS_PROCESSED);
    let skipped_before = counter(FETCHES_SKIPPED);
    refresh(&rt).await?;
    refresh(&rt).await?;
    assert_count(
        counter(ROWS_PROCESSED) - processed_before,
        0,
        "no row was read",
    );
    assert_count(
        counter(FETCHES_SKIPPED) - skipped_before,
        2,
        "both refreshes skipped the fetch",
    );
    assert_eq!(accelerated_count(&rt).await?, 100);
    Ok(())
}

/// A table dropped and created again under the same name has a new UUID: that refresh
/// reads by time once (the old table's rows stay, newer versions win), then the new
/// table's snapshots are followed.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_recreated_table_is_read_by_time_once() -> anyhow::Result<()> {
    let _tracing = crate::init_tracing(Some(
        "integration=debug,data_components=debug,runtime_table=debug,info",
    ));
    let lab = Lab::start(TimeKind::Timestamp).await?;
    lab.append(&keys("k", 1..=100, 0, "v1")).await?;
    let (rt, _) = start_spice(&lab, &Accel::default()).await?;

    lab.recreate_with(&keys("k", 1..=50, 60, "fresh")).await?;
    refresh(&rt).await?;
    let rows = accelerated_rows(&rt).await?;
    assert_eq!(rows.len(), 100, "the old table's rows stay");
    assert_eq!(
        row(&rows, "k1"),
        Some(&("k1".to_string(), Some(at_text(60)), "fresh".to_string())),
        "the new table's newer version wins"
    );
    assert_eq!(
        row(&rows, "k51"),
        Some(&("k51".to_string(), Some(at_text(0)), "v1".to_string()))
    );

    lab.append(&[Row::at("k200", 5, "late_out")]).await?;
    refresh(&rt).await?;
    assert!(
        row(&accelerated_rows(&rt).await?, "k200").is_some(),
        "the new table's snapshots are followed"
    );
    Ok(())
}

/// A memory-mode acceleration keeps its cursor for the process: late rows load.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_memory_mode_acceleration_follows_snapshots_too() -> anyhow::Result<()> {
    let _tracing = crate::init_tracing(Some(
        "integration=debug,data_components=debug,runtime_table=debug,info",
    ));
    let lab = Lab::start(TimeKind::Timestamp).await?;
    lab.append(&keys("k", 1..=100, 0, "v1")).await?;
    lab.append(&keys("k", 101..=110, 1800, "v1")).await?;
    let accel = Accel {
        mode: Mode::Memory,
        ..Accel::default()
    };
    let (rt, _) = start_spice(&lab, &accel).await?;

    lab.append(&[
        Row::at("k301", 300, "late_out"),
        Row::at("k2", 480, "v2_out"),
    ])
    .await?;
    refresh(&rt).await?;
    let rows = assert_matches_source(&rt, TimeKind::Timestamp, "after the late append").await?;
    assert_eq!(rows.len(), 111);
    assert_eq!(
        row(&rows, "k2"),
        Some(&("k2".to_string(), Some(at_text(480)), "v2_out".to_string()))
    );
    Ok(())
}

/// `rows` without their times, which the engines render as text differently.
fn key_vals(rows: Rows) -> Vec<(String, String)> {
    rows.into_iter().map(|(key, _, val)| (key, val)).collect()
}

/// Regression test for #15273: `DuckDB` and `SQLite` with `on_conflict: upsert` read by time,
/// so a late older version does not overwrite the newer stored one. A late row below the
/// window start is not loaded, unlike on Cayenne.
#[cfg(any(feature = "duckdb", feature = "sqlite"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_last_arrival_engine_reads_by_time_and_keeps_the_newer_version() -> anyhow::Result<()> {
    let _tracing = crate::init_tracing(Some(
        "integration=debug,data_components=debug,runtime_table=debug,info",
    ));
    let engines = [
        #[cfg(feature = "duckdb")]
        "duckdb",
        #[cfg(feature = "sqlite")]
        "sqlite",
    ];
    for engine in engines {
        let lab = Lab::start(TimeKind::Timestamp).await?;
        // c1: k1..k100 at 10:00.
        lab.append(&keys("k", 1..=100, 0, "v1")).await?;
        let accel = Accel {
            engine,
            ..Accel::default()
        };
        let (rt, _) = start_spice(&lab, &accel).await?;
        assert_eq!(accelerated_count(&rt).await?, 100, "{engine}: initial load");

        // c2: k1..k10 updated at 10:30, so the window starts at 10:10.
        lab.append(&keys("k", 1..=10, 1800, "v2")).await?;
        refresh(&rt).await?;
        assert_eq!(
            key_vals(accelerated_rows(&rt).await?),
            key_vals(source_newest_rows(&rt, TimeKind::Timestamp).await?),
            "{engine}: after c2"
        );

        // c3: stale k1..k10 and new k201 at 10:05 (below the window), new k202 at 10:25.
        lab.append(
            &[
                keys("k", 1..=10, 300, "stale"),
                vec![
                    Row::at("k201", 300, "late_out"),
                    Row::at("k202", 1500, "late_in"),
                ],
            ]
            .concat(),
        )
        .await?;
        refresh(&rt).await?;
        let expected: Vec<(String, String)> =
            key_vals(source_newest_rows(&rt, TimeKind::Timestamp).await?)
                .into_iter()
                .filter(|(key, _)| key != "k201")
                .collect();
        assert_eq!(
            key_vals(accelerated_rows(&rt).await?),
            expected,
            "{engine}: after c3, k1..k10 keep v2, k202 loads, and k201 (below the window start) does not"
        );
        assert_count(
            counter(FALLBACKS),
            0,
            "a refresh that reads by time by design is no fallback",
        );
        rt.shutdown().await;
    }
    Ok(())
}
