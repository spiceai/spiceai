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

#![allow(clippy::expect_used)]
#![allow(clippy::clone_on_ref_ptr)]

//! Integration tests for tiered small-files compaction on a Cayenne table.
//!
//! Each test drives writes that bypass the inline memtable (rows >
//! `INLINE_MAX_ROWS`), so each insert lands as a distinct Vortex file in the
//! current snapshot dir. With a low `target_vortex_file_size_mb` and a low
//! `compaction_trigger_files`, even tiny tests can exercise the picker +
//! rewrite + snapshot-swap path end-to-end.

mod common;

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use cayenne::metadata::{CreateTableOptions, VortexConfig};
use cayenne::{CayenneTableProvider, CayenneTableProviderBuilder, MetadataCatalog};
use datafusion::datasource::TableProvider;
use datafusion::prelude::{col, lit};

use datafusion::prelude::SessionContext;
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};

/// Build a tiny test schema with an i64 PK column.
fn pk_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Utf8, false),
    ]))
}

/// `VortexConfig` tuned aggressively for tests: tiny target file size so a few
/// thousand rows immediately count as "small", and a low trigger so 4 small
/// files are enough to fire compaction.
fn aggressive_compaction_config() -> VortexConfig {
    VortexConfig {
        // 1 MiB target → small_max = 256 KiB → every test write (~12 KiB IPC for
        // ~1500 i64 rows) counts as small.
        target_vortex_file_size_mb: 1,
        compaction_trigger_files: 4,
        compaction_max_levels: 3,
        compaction_max_files_per_pick: 32,
        // Stops the interval scheduler, so tests drive compaction explicitly via
        // maybe_compact_small_files() on the inline path or from the test body.
        // NOTE: it does NOT stop compaction from running on its own. An append
        // still calls `schedule_post_write_compaction`, which spawns a pass
        // regardless of this interval, so a test that measures file counts around
        // its own writes must quiesce first — see
        // `wait_until_current_snapshot_compacts`.
        compaction_background_interval_ms: 0,
        ..VortexConfig::default()
    }
}

fn aggressive_sorted_compaction_config() -> VortexConfig {
    VortexConfig {
        sort_columns: vec!["id".to_string()],
        ..aggressive_compaction_config()
    }
}

/// Same as [`aggressive_compaction_config`] but forces key-based deletion.
///
/// Key deletion is *necessary* for the warm-subset rewrite but not sufficient:
/// `subset_rewrite_eligibility` also requires the picker's candidate to be a
/// proper subset of the current files. This config does not settle which
/// rewrite runs, and neither does its file count: `compaction_max_files_per_pick`
/// caps the files taken from the single tier bucket that fired, not the
/// snapshot, so a candidate is a proper subset — well under the cap of 32 —
/// whenever any current file is already settled or sits in the other tier.
///
/// So the tests here assert delete/row semantics, never a rewrite path. The
/// path is observable via `last_small_file_compact_path()`, and
/// `p1_subset_path_test.rs` is where each one is driven and asserted.
fn aggressive_key_deletion_compaction_config() -> VortexConfig {
    VortexConfig {
        deletion_mode: cayenne::metadata::DeletionMode::Key,
        ..aggressive_compaction_config()
    }
}

/// Build a batch of `n` rows whose ids start at `start` and whose values are
/// derived strings. n must be > `INLINE_MAX_ROWS` (1024) to bypass inlining.
fn make_batch(schema: &Arc<Schema>, start: i64, n: i64) -> RecordBatch {
    let ids: Vec<i64> = (start..start + n).collect();
    let values: Vec<String> = ids
        .iter()
        .map(|row_id| value_payload("v", *row_id))
        .collect();
    RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(values)),
        ],
    )
    .expect("test batch is valid")
}

fn make_batch_from_ids(schema: &Arc<Schema>, ids: Vec<i64>) -> RecordBatch {
    let values: Vec<String> = ids
        .iter()
        .map(|row_id| value_payload("v", *row_id))
        .collect();
    RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(values)),
        ],
    )
    .expect("test batch is valid")
}

fn value_payload(prefix: &str, row_id: i64) -> String {
    let row_id = u64::try_from(row_id).expect("test id should be non-negative");
    format!(
        "{prefix}_{row_id:020}_{:016x}_{:016x}_{:016x}",
        row_id.wrapping_mul(0x9E37_79B9_7F4A_7C15),
        row_id.wrapping_mul(0xC2B2_AE3D_27D4_EB4F),
        row_id.wrapping_mul(0x1656_67B1_9E37_79F9),
    )
}

/// Count `.vortex` files in `<data_path>/<table_id>/<current_snapshot_id>`.
///
/// A snapshot dir that does not exist yet holds no files, so that reads as 0. Any
/// other error is raised: treating it as 0 would read as "compaction consolidated
/// everything" and pass the very assertions that call this.
async fn count_vortex_files(data_path: &Path, table_id: &str, snapshot_id: &str) -> usize {
    let snapshot_dir = data_path.join(table_id).join(snapshot_id);
    let mut entries = match tokio::fs::read_dir(&snapshot_dir).await {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return 0,
        Err(e) => panic!("read_dir {} failed: {e}", snapshot_dir.display()),
    };
    let mut count = 0;
    while let Some(entry) = entries.next_entry().await.expect("read_dir") {
        let name = entry.file_name();
        let Some(name_str) = name.to_str() else {
            continue;
        };
        if name_str.ends_with(".vortex") && !name_str.starts_with('.') {
            count += 1;
        }
    }
    count
}

async fn count_protected_snapshots(fixture: &common::TestFixture, table_id: &str) -> usize {
    fixture
        .catalog
        .get_all_snapshot_sequences(table_id)
        .await
        .expect("snapshot sequences should load")
        .len()
}

/// Total row count via `SELECT COUNT(*)` for verification.
async fn count_rows(ctx: &SessionContext, table_name: &str) -> i64 {
    let df = ctx
        .sql(&format!("SELECT COUNT(*) FROM {table_name}"))
        .await
        .expect("count sql planned");
    let batches = df.collect().await.expect("count collected");
    let merged =
        arrow::compute::concat_batches(&batches[0].schema(), &batches).expect("concat batches");
    merged
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count column")
        .value(0)
}

async fn count_rows_matching(ctx: &SessionContext, table_name: &str, where_clause: &str) -> i64 {
    let df = ctx
        .sql(&format!(
            "SELECT COUNT(*) FROM {table_name} WHERE {where_clause}"
        ))
        .await
        .expect("count matching sql planned");
    let batches = df.collect().await.expect("count matching collected");
    let merged =
        arrow::compute::concat_batches(&batches[0].schema(), &batches).expect("concat batches");
    merged
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count column")
        .value(0)
}

/// Wait for `table`'s in-flight maintenance to drain, bounded.
///
/// `drain_in_flight_maintenance` has no timeout of its own, so a pass that never
/// finishes would hang until the much larger harness process timeout, reporting
/// nothing about where it stopped. `context` names what the caller was about to
/// do, so the panic identifies which wait wedged.
async fn drain_in_flight_maintenance_bounded(
    table: &Arc<CayenneTableProvider>,
    fixture: &common::TestFixture,
    table_name: &str,
    context: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    const DRAIN_TIMEOUT: Duration = Duration::from_mins(2);

    let Ok(drained) =
        tokio::time::timeout(DRAIN_TIMEOUT, table.drain_in_flight_maintenance()).await
    else {
        let table_meta = fixture.catalog.get_table(table_name).await?;
        let snapshot_id = table_meta.current_snapshot_id;
        let files =
            count_vortex_files(&fixture.data_path, &table_meta.table_id, &snapshot_id).await;
        panic!(
            "draining {table_name}'s in-flight maintenance did not finish within \
             {DRAIN_TIMEOUT:?} before {context} (snapshot {snapshot_id}, {files} files)"
        );
    };
    drained?;

    Ok(())
}

/// Report the current snapshot once its file count is below
/// `uncompacted_file_count`, i.e. once small-file compaction has consolidated
/// the seeded appends, or `None` if no compaction is reachable.
///
/// Pass the number of appends the test *seeded* whenever the caller asserts that
/// compaction fired. Appends drive `schedule_post_write_compaction`, which is NOT
/// disabled by `compaction_background_interval_ms: 0` — that only stops the
/// interval scheduler — so a pass gets spawned and can consolidate the seed while
/// the test is still writing it. (These tests install no dedicated compaction
/// runtime, so that pass lands on the ambient one and interleaves at the test's
/// await points.) A count listed from the store after the writes may therefore
/// already be the compacted count, making a further reduction unreachable and this
/// helper's answer depend on that race.
///
/// A caller for which both answers are correct may still pass a listed count —
/// `two_phase_compact`'s phase B does, and reads `None` as "post-write already
/// drained the backlog". What a listed count cannot support is asserting that
/// `Some` must come back.
///
/// Quiescing first is what makes the answer deterministic. A post-write pass and
/// this helper both call `compact_current_snapshot_small_files`, so they contend
/// for the same `compaction_lock` (`try_lock`, so the loser reports a no-op) and
/// for the same one-shot `new_files_since_last_compaction` credit, which the
/// winner resets on commit. Once that credit is spent the explicit trigger
/// declines *permanently*, so waiting longer cannot recover it — the wait has to
/// end with the background pass, not with a wall clock. After the drain no pass
/// is in flight or scheduled and only a new write could schedule one, so the
/// observation below is stable; the bounded loop is a backstop for a staged
/// append still finalizing, not the mechanism.
///
/// The drain is bounded (see [`drain_in_flight_maintenance_bounded`]).
async fn wait_until_current_snapshot_compacts(
    table: &Arc<CayenneTableProvider>,
    fixture: &common::TestFixture,
    table_name: &str,
    uncompacted_file_count: usize,
) -> Result<Option<(String, usize)>, Box<dyn std::error::Error>> {
    const TIMEOUT: Duration = Duration::from_secs(10);
    const POLL_INTERVAL: Duration = Duration::from_millis(50);

    drain_in_flight_maintenance_bounded(
        table,
        fixture,
        table_name,
        &format!("waiting for a fan-out below {uncompacted_file_count}"),
    )
    .await?;

    let started = Instant::now();
    loop {
        let table_meta = fixture.catalog.get_table(table_name).await?;
        let snapshot_id = table_meta.current_snapshot_id;
        let file_count =
            count_vortex_files(&fixture.data_path, &table_meta.table_id, &snapshot_id).await;
        if file_count < uncompacted_file_count {
            return Ok(Some((snapshot_id, file_count)));
        }

        if table.compact_current_snapshot_small_files().await? {
            let table_meta = fixture.catalog.get_table(table_name).await?;
            let snapshot_id = table_meta.current_snapshot_id;
            let file_count =
                count_vortex_files(&fixture.data_path, &table_meta.table_id, &snapshot_id).await;
            return Ok(Some((snapshot_id, file_count)));
        }

        if started.elapsed() >= TIMEOUT {
            return Ok(None);
        }

        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

async fn build_table(
    fixture: &common::TestFixture,
    name: &str,
    schema: Arc<Schema>,
    pk: Option<&str>,
    vortex_config: VortexConfig,
) -> (Arc<CayenneTableProvider>, SessionContext, String) {
    let on_conflict =
        pk.map(|pk_col| OnConflict::Upsert(ColumnReference::new(vec![pk_col.to_string()])));
    let primary_key = pk.map_or_else(Vec::new, |pk_col| vec![pk_col.to_string()]);

    let options = CreateTableOptions {
        table_name: name.to_string(),
        schema: Arc::clone(&schema),
        primary_key,
        on_conflict,
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config,
    };

    let catalog_arc: Arc<dyn MetadataCatalog> = fixture.catalog.clone();
    let ctx = SessionContext::new();
    let table = CayenneTableProvider::create_table(catalog_arc, options, ctx.runtime_env())
        .await
        .expect("create_table");
    let table = Arc::new(table);
    let table_id = fixture
        .catalog
        .get_table(name)
        .await
        .expect("get_table")
        .table_id;
    ctx.register_table(
        name,
        Arc::clone(&table) as Arc<dyn datafusion::datasource::TableProvider>,
    )
    .expect("register table");
    (table, ctx, table_id)
}

test_with_backends!(compaction_reduces_file_count_after_n_small_appends);
async fn compaction_reduces_file_count_after_n_small_appends(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = pk_schema();
    let (table, ctx, table_id) = build_table(
        &fixture,
        "compaction_files",
        Arc::clone(&schema),
        None,
        aggressive_compaction_config(),
    )
    .await;

    // 20 small batches above INLINE_MAX_ROWS (1024) so each lands as a Vortex
    // file. The production write path schedules compaction off the append hot
    // path; this test drives the trigger explicitly after each write so the
    // file-count assertion remains deterministic with the background scheduler
    // disabled.
    let batch_rows: i64 = 1500;
    for batch_idx in 0..20_i64 {
        let start = batch_idx * batch_rows;
        let batch = make_batch(&schema, start, batch_rows);
        common::insert_batch(&table, batch).await?;
        let _ = run_compaction(&table).await;
    }

    // Read the current snapshot id off the provider — compactions advance it.
    let snapshot_id = fixture
        .catalog
        .get_table("compaction_files")
        .await?
        .current_snapshot_id;
    let file_count = count_vortex_files(&fixture.data_path, &table_id, &snapshot_id).await;

    // The exact post-compaction count depends on the light delta-encoding's
    // file sizes (they set how many folded outputs graduate at the 1 MiB
    // target): the Sparse+Dict light set packed these appends into <= 6 files,
    // the Zstd-only light set into 7. Assert the behavior under test — 20
    // appends fold to a small handful — with headroom for encoding-size drift.
    assert!(
        file_count <= 8,
        "expected post-compaction file count <= 8, found {file_count} files in snapshot {snapshot_id}"
    );

    // Row count must be preserved end-to-end.
    let total = count_rows(&ctx, "compaction_files").await;
    assert_eq!(total, batch_rows * 20);

    Ok(())
}

test_with_backends!(compaction_runs_for_sort_column_tables);
async fn compaction_runs_for_sort_column_tables(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = pk_schema();
    let (table, _ctx, _table_id) = build_table(
        &fixture,
        "compaction_sorted",
        Arc::clone(&schema),
        None,
        aggressive_sorted_compaction_config(),
    )
    .await;

    let batch_rows = 1500_i64;
    let batch_count = 8_i64;
    for batch_idx in 0..batch_count {
        let start = batch_idx * batch_rows;
        let mut ids: Vec<i64> = (start..start + batch_rows).collect();
        ids.reverse();
        common::insert_batch(&table, make_batch_from_ids(&schema, ids)).await?;
    }

    let Some((_snapshot_id, file_count)) = wait_until_current_snapshot_compacts(
        &table,
        &fixture,
        "compaction_sorted",
        usize::try_from(batch_count).expect("batch count fits usize"),
    )
    .await?
    else {
        panic!("sort-column compaction should commit a rewrite");
    };
    assert!(
        file_count < usize::try_from(batch_count).expect("batch count fits usize"),
        "sort-column compaction should reduce file count below {batch_count}, found {file_count}"
    );
    let ctx = SessionContext::new();
    ctx.register_table(
        "compaction_sorted",
        Arc::clone(&table) as Arc<dyn datafusion::datasource::TableProvider>,
    )?;
    assert_eq!(
        count_rows(&ctx, "compaction_sorted").await,
        batch_rows * batch_count
    );

    Ok(())
}

test_with_backends!(compaction_preserves_pk_upsert_semantics);
async fn compaction_preserves_pk_upsert_semantics(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = pk_schema();
    let (table, ctx, _table_id) = build_table(
        &fixture,
        "compaction_upsert",
        Arc::clone(&schema),
        Some("id"),
        aggressive_compaction_config(),
    )
    .await;

    // Seed N rows in 4 batches that bypass inlining, then upsert each ID with
    // a "second" tagged value. After all writes + compactions, only the
    // second-version rows should remain visible.
    let batch_rows: i64 = 1500;
    for batch_idx in 0..4_i64 {
        let start = batch_idx * batch_rows;
        let ids: Vec<i64> = (start..start + batch_rows).collect();
        let values: Vec<String> = ids
            .iter()
            .map(|row_id| value_payload("first", *row_id))
            .collect();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(values)),
            ],
        )?;
        common::insert_batch(&table, batch).await?;
    }

    for batch_idx in 0..4_i64 {
        let start = batch_idx * batch_rows;
        let ids: Vec<i64> = (start..start + batch_rows).collect();
        let values: Vec<String> = ids
            .iter()
            .map(|row_id| value_payload("second", *row_id))
            .collect();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(values)),
            ],
        )?;
        common::insert_batch(&table, batch).await?;
    }

    let mut compacted = false;
    for _ in 0..3 {
        if !run_compaction(&table).await {
            break;
        }
        compacted = true;
    }
    assert!(
        compacted,
        "test setup should produce a compaction candidate"
    );

    // Total rows must equal the unique-PK count (4 * 1500 = 6000), not double.
    let total = count_rows(&ctx, "compaction_upsert").await;
    assert_eq!(total, batch_rows * 4);

    // Every visible row should now hold the "second_" value.
    let df = ctx
        .sql("SELECT COUNT(*) FROM compaction_upsert WHERE value NOT LIKE 'second_%'")
        .await?;
    let batches = df.collect().await?;
    let merged = arrow::compute::concat_batches(&batches[0].schema(), &batches)?;
    let stale_count = merged
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count column")
        .value(0);
    assert_eq!(stale_count, 0, "upsert + compaction must drop stale rows");

    Ok(())
}

test_with_backends!(compaction_collapses_tiny_protected_snapshots);
async fn compaction_collapses_tiny_protected_snapshots(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = pk_schema();
    let config = VortexConfig {
        target_vortex_file_size_mb: 128,
        compaction_trigger_files: 4,
        compaction_trigger_protected_snapshots: 4,
        compaction_background_interval_ms: 0,
        ..Default::default()
    };
    let (table, ctx, table_id) = build_table(
        &fixture,
        "compaction_protected_snapshots",
        Arc::clone(&schema),
        Some("id"),
        config,
    )
    .await;

    let batch_rows = 1500_i64;
    for batch_idx in 0..4_i64 {
        let start = batch_idx * batch_rows;
        common::insert_batch(&table, make_batch(&schema, start, batch_rows)).await?;
    }

    assert_eq!(
        count_protected_snapshots(&fixture, &table_id).await,
        4,
        "test setup should create protected snapshots before explicit compaction"
    );

    // The protected-snapshot count trigger (firing even below the byte
    // threshold) drives a maintenance compaction that collapses the tiny
    // protected snapshots. That compaction runs on a spawned maintenance task
    // sharing `compaction_lock`, so the explicit trigger can transiently lose the
    // try-lock while the background incremental subset merges the protected set
    // down to a single merged snapshot; re-trigger until it has collapsed
    // (<= 1 protected snapshot remaining).
    let mut collapsed = false;
    for _ in 0..200 {
        let _ = run_compaction(&table).await;
        if count_protected_snapshots(&fixture, &table_id).await <= 1 {
            collapsed = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(
        collapsed,
        "protected snapshot count trigger should collapse the tiny protected snapshots"
    );

    let total = count_rows(&ctx, "compaction_protected_snapshots").await;
    assert_eq!(total, batch_rows * 4);

    Ok(())
}

test_with_backends!(current_snapshot_publish_preserves_protected_snapshot_visibility);
async fn current_snapshot_publish_preserves_protected_snapshot_visibility(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = pk_schema();
    let config = VortexConfig {
        target_vortex_file_size_mb: 128,
        compaction_trigger_files: 100,
        compaction_background_interval_ms: 0,
        ..Default::default()
    };
    let (table, ctx, table_id) = build_table(
        &fixture,
        "scan_listing_cache",
        Arc::clone(&schema),
        Some("id"),
        config,
    )
    .await;

    let batch_rows = 1500_i64;
    for batch_idx in 0..4_i64 {
        let start = batch_idx * batch_rows;
        common::insert_batch(&table, make_batch(&schema, start, batch_rows)).await?;
    }

    let protected_snapshot_count = count_protected_snapshots(&fixture, &table_id).await;
    assert_eq!(protected_snapshot_count, 4);

    let total = count_rows(&ctx, "scan_listing_cache").await;
    assert_eq!(total, batch_rows * 4);

    table.publish_current_snapshot_files_changed().await;

    let total = count_rows(&ctx, "scan_listing_cache").await;
    assert_eq!(total, batch_rows * 4);

    Ok(())
}

test_with_backends!(compaction_idempotent_when_no_candidates);
async fn compaction_idempotent_when_no_candidates(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = pk_schema();
    // Disable inline triggers via a high trigger_files count, so the first few
    // writes don't auto-compact and we can call compact explicitly.
    let config = VortexConfig {
        target_vortex_file_size_mb: 1,
        compaction_trigger_files: 1000,
        compaction_background_interval_ms: 0,
        ..Default::default()
    };

    let (table, _ctx, table_id) = build_table(
        &fixture,
        "compaction_noop",
        Arc::clone(&schema),
        None,
        config,
    )
    .await;

    // One small write — single file.
    let batch = make_batch(&schema, 0, 1500);
    common::insert_batch(&table, batch).await?;

    let snapshot_before = fixture
        .catalog
        .get_table("compaction_noop")
        .await?
        .current_snapshot_id;

    // No candidate exists (only one file) — picker returns None, no rewrite.
    assert!(
        !run_compaction(&table).await,
        "compaction must be a no-op when there's nothing to do"
    );
    let snapshot_after_first = fixture
        .catalog
        .get_table("compaction_noop")
        .await?
        .current_snapshot_id;
    assert_eq!(
        snapshot_before, snapshot_after_first,
        "no compaction should leave snapshot id unchanged"
    );

    // Second call also a no-op.
    assert!(!run_compaction(&table).await);

    let file_count = count_vortex_files(&fixture.data_path, &table_id, &snapshot_after_first).await;
    assert_eq!(
        file_count, 1,
        "snapshot should still hold the original file"
    );

    Ok(())
}

test_with_backends!(compaction_disabled_when_trigger_unreachable);
async fn compaction_disabled_when_trigger_unreachable(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = pk_schema();
    // High trigger threshold + small writes → picker keeps returning None →
    // no compaction. Acts as a regression test that the inline trigger does
    // not aggressively rewrite without sufficient small-file pressure.
    let config = VortexConfig {
        target_vortex_file_size_mb: 1,
        compaction_trigger_files: 100,
        compaction_background_interval_ms: 0,
        ..Default::default()
    };

    let (table, ctx, table_id) = build_table(
        &fixture,
        "compaction_off",
        Arc::clone(&schema),
        None,
        config,
    )
    .await;

    let batch_rows: i64 = 1500;
    for batch_idx in 0..6_i64 {
        let start = batch_idx * batch_rows;
        let batch = make_batch(&schema, start, batch_rows);
        common::insert_batch(&table, batch).await?;
    }

    let snapshot_id = fixture
        .catalog
        .get_table("compaction_off")
        .await?
        .current_snapshot_id;
    let file_count = count_vortex_files(&fixture.data_path, &table_id, &snapshot_id).await;
    assert!(
        file_count >= 6,
        "expected at least 6 files when compaction trigger is unreachable, found {file_count}"
    );

    let total = count_rows(&ctx, "compaction_off").await;
    assert_eq!(total, batch_rows * 6);

    Ok(())
}

test_with_backends!(compaction_handles_concurrent_compaction_triggers);
async fn compaction_handles_concurrent_compaction_triggers(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = pk_schema();
    let (table, ctx, _table_id) = build_table(
        &fixture,
        "compaction_concurrent",
        Arc::clone(&schema),
        None,
        aggressive_compaction_config(),
    )
    .await;

    // Pre-load 8 small batches so the picker definitely has work.
    let batch_rows: i64 = 1500;
    for batch_idx in 0..8_i64 {
        let start = batch_idx * batch_rows;
        let batch = make_batch(&schema, start, batch_rows);
        common::insert_batch(&table, batch).await?;
    }

    // Fire 4 concurrent compaction triggers. The internal try_lock should
    // serialize: at most one rewrite proceeds at a time, the rest no-op.
    let triggers: Vec<_> = (0..4_usize)
        .map(|_| {
            let t = Arc::clone(&table);
            tokio::spawn(async move { run_compaction(&t).await })
        })
        .collect();

    for handle in triggers {
        let _ = handle.await.expect("compaction task did not panic");
    }

    // Data must be intact.
    let total = count_rows(&ctx, "compaction_concurrent").await;
    assert_eq!(total, batch_rows * 8);

    Ok(())
}

// --- Production current-snapshot small-file compaction (#11392) ----------------
//
// #11130 removed the general compactor from `run_compaction_trigger`, which
// early-outs when a table has no protected snapshots. Append-only tables never
// create protected snapshots, so they stopped being compacted. These tests
// drive the restored production path (`compact_current_snapshot_small_files`,
// the method `run_compaction_trigger` now calls regardless of the protected set)
// directly and assert it consolidates small files on a table with ZERO protected
// snapshots, and that a concurrent append can never lose rows.

test_with_backends!(production_trigger_compacts_append_only_table);
async fn production_trigger_compacts_append_only_table(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = pk_schema();
    // pk = None → append-only: every insert lands as a new Vortex file in the
    // current snapshot dir and NO protected snapshot is ever created.
    let (table, ctx, table_id) = build_table(
        &fixture,
        "prod_append_only",
        Arc::clone(&schema),
        None,
        aggressive_compaction_config(),
    )
    .await;

    let batch_rows: i64 = 1500;
    let batches = 12_i64;
    for batch_idx in 0..batches {
        let start = batch_idx * batch_rows;
        common::insert_batch(&table, make_batch(&schema, start, batch_rows)).await?;
    }

    // No protected snapshots exist — this is exactly the case #11130's
    // early-out starved.
    assert_eq!(
        count_protected_snapshots(&fixture, &table_id).await,
        0,
        "append-only table must have no protected snapshots"
    );

    let Some((_snapshot_id, file_count)) = wait_until_current_snapshot_compacts(
        &table,
        &fixture,
        "prod_append_only",
        usize::try_from(batches).expect("batch count fits usize"),
    )
    .await?
    else {
        panic!(
            "current-snapshot compaction must fire on an append-only table with no protected snapshots"
        );
    };

    // A fresh snapshot dir was minted and the file count is bounded.
    assert!(
        file_count < usize::try_from(batches).expect("batch count fits usize"),
        "expected the current snapshot to be consolidated below {batches} files, found {file_count} files"
    );

    // Rows preserved end-to-end.
    assert_eq!(
        count_rows(&ctx, "prod_append_only").await,
        batch_rows * batches
    );

    Ok(())
}

test_with_backends!(concurrent_append_during_compaction_loses_no_rows);
async fn concurrent_append_during_compaction_loses_no_rows(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = pk_schema();
    let (table, ctx, _table_id) = build_table(
        &fixture,
        "concurrent_append_compaction",
        Arc::clone(&schema),
        None,
        aggressive_compaction_config(),
    )
    .await;

    let batch_rows: i64 = 1500;
    // Seed enough small files to make the compaction picker fire.
    let seeded = 8_i64;
    for batch_idx in 0..seeded {
        let start = batch_idx * batch_rows;
        common::insert_batch(&table, make_batch(&schema, start, batch_rows)).await?;
    }

    // Race a compaction against a burst of appends. Whether the guard commits or
    // aborts (because an append landed mid-rewrite), the invariant is the same:
    // NO appended row is dropped — an abort leaves the old snapshot current and
    // intact, a commit carries every pre-scan file forward.
    let extra = 6_i64;
    let compaction_table = Arc::clone(&table);
    let compaction = tokio::spawn(async move {
        // A few passes so at least one overlaps the appends below.
        for _ in 0..extra {
            let _ = compaction_table
                .compact_current_snapshot_small_files()
                .await
                .expect("compaction pass should not error");
            tokio::task::yield_now().await;
        }
    });

    for batch_idx in seeded..(seeded + extra) {
        let start = batch_idx * batch_rows;
        common::insert_batch(&table, make_batch(&schema, start, batch_rows)).await?;
    }

    compaction.await.expect("compaction task did not panic");

    // Every appended row must still be visible.
    assert_eq!(
        count_rows(&ctx, "concurrent_append_compaction").await,
        batch_rows * (seeded + extra),
        "a concurrent append during compaction must never lose rows"
    );

    Ok(())
}

test_with_backends!(compaction_survives_provider_reopen);
async fn compaction_survives_provider_reopen(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = pk_schema();
    let (table, _ctx, table_id) = build_table(
        &fixture,
        "compaction_reopen",
        Arc::clone(&schema),
        None,
        aggressive_compaction_config(),
    )
    .await;

    let batch_rows: i64 = 1500;
    let batches = 10_i64;
    for batch_idx in 0..batches {
        let start = batch_idx * batch_rows;
        common::insert_batch(&table, make_batch(&schema, start, batch_rows)).await?;
    }

    let Some((committed_snapshot_id, file_count)) = wait_until_current_snapshot_compacts(
        &table,
        &fixture,
        "compaction_reopen",
        usize::try_from(batches).expect("batch count fits usize"),
    )
    .await?
    else {
        panic!("compaction must commit before the reopen");
    };

    assert!(
        file_count < usize::try_from(batches).expect("batch count fits usize"),
        "expected reopen test compaction to reduce file count below {batches}, found {file_count} files"
    );

    // Drop the live provider and reopen from the persisted catalog — a fresh
    // provider with empty in-memory state must read the committed consolidated
    // snapshot, with every row intact.
    drop(table);
    let reopen_ctx = SessionContext::new();
    let reopened = Arc::new(
        CayenneTableProviderBuilder::new(fixture.catalog.clone(), reopen_ctx.runtime_env())
            .open("compaction_reopen")
            .await?,
    );
    reopen_ctx.register_table(
        "compaction_reopen",
        Arc::clone(&reopened) as Arc<dyn datafusion::datasource::TableProvider>,
    )?;

    assert_eq!(
        fixture
            .catalog
            .get_table("compaction_reopen")
            .await?
            .current_snapshot_id,
        committed_snapshot_id,
        "the reopened table must point at the consolidated snapshot"
    );
    assert_eq!(
        count_rows(&reopen_ctx, "compaction_reopen").await,
        batch_rows * batches,
        "all rows must survive the compaction + reopen"
    );
    // Sanity: the committed snapshot dir physically exists and is consolidated.
    let file_count =
        count_vortex_files(&fixture.data_path, &table_id, &committed_snapshot_id).await;
    assert!(
        file_count < usize::try_from(batches).expect("batch count fits usize"),
        "consolidated snapshot should reduce file count below {batches}, found {file_count}"
    );

    Ok(())
}

/// Helper that calls into the `#[doc(hidden)] pub` `maybe_compact_small_files`
/// trigger directly. Returns true if a rewrite happened.
///
/// Tests don't go through the [`cayenne::provider::compaction::CompactionRunner`]
/// adapter the background scheduler uses, because that adapter `try_lock`s
/// `write_lock` to serialize with appends. Single-table integration tests have
/// no concurrent writers, so calling the trigger directly is correct.
async fn run_compaction(table: &Arc<CayenneTableProvider>) -> bool {
    table
        .maybe_compact_small_files()
        .await
        .expect("compaction must succeed in tests")
}

// --- Subset current-snapshot rewrite + link_or_copy (P1-1) --------------------
//
// Pure gate branches live in unit tests (`subset_rewrite_eligibility_*`).
// Here we cover the production compact API (append-only tables, which the
// small-file path was restored for) and the hardlink helper used by subset.

/// On Linux, return the inode of `path`, or `None` if the file is missing.
#[cfg(target_os = "linux")]
fn file_inode(path: &Path) -> Option<u64> {
    use std::os::unix::fs::MetadataExt;
    std::fs::metadata(path).ok().map(|m| m.ino())
}

test_with_backends!(compact_current_after_seed_then_more_preserves_all_rows);
async fn compact_current_after_seed_then_more_preserves_all_rows(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    // Two-phase compact: seed → compact → more small files → compact again.
    // Covers the production `compact_current_snapshot_small_files` entry
    // (including write_amp logging + subset/full dispatch) end-to-end.
    let schema = pk_schema();
    let (table, ctx, table_id) = build_table(
        &fixture,
        "two_phase_compact",
        Arc::clone(&schema),
        None,
        aggressive_compaction_config(),
    )
    .await;

    let small_rows: i64 = 1500;
    let seed_batches = 12_i64;
    for batch_idx in 0..seed_batches {
        common::insert_batch(
            &table,
            make_batch(&schema, batch_idx * small_rows, small_rows),
        )
        .await?;
    }
    let Some((_snap_a, files_a)) = wait_until_current_snapshot_compacts(
        &table,
        &fixture,
        "two_phase_compact",
        usize::try_from(seed_batches).expect("fits"),
    )
    .await?
    else {
        panic!("phase-A compact must fire");
    };
    assert!(files_a < usize::try_from(seed_batches).expect("fits"));

    let more = 10_i64;
    for batch_idx in 0..more {
        let start = seed_batches * small_rows + batch_idx * small_rows;
        common::insert_batch(&table, make_batch(&schema, start, small_rows)).await?;
    }
    let expected = small_rows * (seed_batches + more);
    assert_eq!(count_rows(&ctx, "two_phase_compact").await, expected);

    let snap_before = fixture
        .catalog
        .get_table("two_phase_compact")
        .await?
        .current_snapshot_id;
    let files_before = count_vortex_files(&fixture.data_path, &table_id, &snap_before).await;

    // Phase B: either an explicit compact reduces the file count, or post-write
    // maintenance already consolidated during the inserts (common when the
    // threshold is crossed mid-seed). Both are correct; the invariant is
    // row-count preservation + a bounded file count.
    if let Some((snap_after, files_after)) =
        wait_until_current_snapshot_compacts(&table, &fixture, "two_phase_compact", files_before)
            .await?
    {
        assert_ne!(snap_after, snap_before);
        assert!(
            files_after < files_before,
            "explicit phase-B compact must reduce file count ({files_before} → {files_after})"
        );
    } else {
        // Post-write already drained the backlog. The bound has to be what an
        // un-consolidated phase B would leave — the files phase A settled on
        // plus one per phase-B append, since each append clears
        // `INLINE_MAX_ROWS` and is too small to shard. Comparing against the
        // raw append total instead asserts nothing: phase A already proved
        // `files_a < seed_batches`, so `files_a + more < seed_batches + more`
        // holds by construction whether or not post-write consolidated.
        let unconsolidated = files_a + usize::try_from(more).expect("fits");
        assert!(
            files_before < unconsolidated,
            "without an explicit compact, post-write must have consolidated \
             below {unconsolidated} files (phase-A settled at {files_a}, \
             +{more} appends; found {files_before})"
        );
    }
    assert_eq!(count_rows(&ctx, "two_phase_compact").await, expected);
    Ok(())
}

test_with_backends!(link_or_copy_snapshot_files_hardlinks_locally);
async fn link_or_copy_snapshot_files_hardlinks_locally(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = pk_schema();
    // Append-only so each insert lands as a current-snapshot vortex file.
    let (table, _ctx, table_id) = build_table(
        &fixture,
        "link_copy",
        Arc::clone(&schema),
        None,
        aggressive_compaction_config(),
    )
    .await;

    common::insert_batch(&table, make_batch(&schema, 0, 1500)).await?;
    let snapshot_id = fixture
        .catalog
        .get_table("link_copy")
        .await?
        .current_snapshot_id;
    let src_dir = fixture.data_path.join(&table_id).join(&snapshot_id);
    let mut entries = tokio::fs::read_dir(&src_dir).await?;
    let mut basename = None;
    while let Some(entry) = entries.next_entry().await? {
        let name = entry.file_name();
        let Some(s) = name.to_str() else {
            continue;
        };
        if s.ends_with(".vortex") {
            basename = Some(s.to_string());
            break;
        }
    }
    let basename = basename.expect("insert must create a vortex file");

    // Empty basenames is a documented no-op.
    table
        .link_or_copy_snapshot_files(&snapshot_id, "empty-target", &[])
        .await?;

    let target_id = "link-target-snapshot";
    table
        .link_or_copy_snapshot_files(&snapshot_id, target_id, &[basename.as_str()])
        .await?;

    let target_path = fixture
        .data_path
        .join(&table_id)
        .join(target_id)
        .join(&basename);
    assert!(
        target_path.exists(),
        "linked/copied file must exist at {}",
        target_path.display()
    );

    #[cfg(target_os = "linux")]
    {
        let src_path = src_dir.join(&basename);
        let src_ino = file_inode(&src_path).expect("src inode");
        let dst_ino = file_inode(&target_path).expect("dst inode");
        assert_eq!(
            src_ino, dst_ino,
            "local link_or_copy must hard-link (same inode), not only copy"
        );
    }

    Ok(())
}

test_with_backends!(sorted_table_still_compacts_via_full_rewrite);
async fn sorted_table_still_compacts_via_full_rewrite(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    // Sort columns disable subset eligibility (pure gate); full rewrite must
    // still preserve rows.
    let schema = pk_schema();
    let (table, ctx, _table_id) = build_table(
        &fixture,
        "sorted_full_rewrite",
        Arc::clone(&schema),
        None,
        aggressive_sorted_compaction_config(),
    )
    .await;

    let batch_rows: i64 = 1500;
    let batches = 12_i64;
    for batch_idx in 0..batches {
        common::insert_batch(
            &table,
            make_batch(&schema, batch_idx * batch_rows, batch_rows),
        )
        .await?;
    }

    let Some((_snap, files)) = wait_until_current_snapshot_compacts(
        &table,
        &fixture,
        "sorted_full_rewrite",
        usize::try_from(batches).expect("fits"),
    )
    .await?
    else {
        panic!("sorted table must still compact via full rewrite");
    };
    assert!(files < usize::try_from(batches).expect("fits"));
    assert_eq!(
        count_rows(&ctx, "sorted_full_rewrite").await,
        batch_rows * batches
    );
    Ok(())
}

// --- Warm-subset + key-delete MoR (this PR) -----------------------------------
//
// Warm-subset rewrite only engages for key-delete tables with NO protected
// snapshots (upsert inserts publish protected snapshots and force the full
// rewrite / protected-leveler path). Build a PK table without `OnConflict` so
// appends accumulate many files in the *current* snapshot dir, then DELETE by
// key and re-compact — MoR tombstones must survive the subset rewrite.

/// Like [`build_table`] with a PK for key deletes, but no upsert so inserts
/// stay append-only (files pile into the current snapshot; subset rewrite is
/// eligible).
async fn build_append_only_key_delete_table(
    fixture: &common::TestFixture,
    name: &str,
    schema: Arc<Schema>,
) -> (Arc<CayenneTableProvider>, SessionContext, String) {
    let options = CreateTableOptions {
        table_name: name.to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["id".to_string()],
        on_conflict: None,
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: aggressive_key_deletion_compaction_config(),
    };

    let catalog_arc: Arc<dyn MetadataCatalog> = fixture.catalog.clone();
    let ctx = SessionContext::new();
    let table = CayenneTableProvider::create_table(catalog_arc, options, ctx.runtime_env())
        .await
        .expect("create_table");
    let table = Arc::new(table);
    let table_id = fixture
        .catalog
        .get_table(name)
        .await
        .expect("get_table")
        .table_id;
    ctx.register_table(
        name,
        Arc::clone(&table) as Arc<dyn datafusion::datasource::TableProvider>,
    )
    .expect("register table");
    (table, ctx, table_id)
}

test_with_backends!(key_delete_survives_compaction_and_reseed);
async fn key_delete_survives_compaction_and_reseed(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    // Append-only key-mode PK table: compact, DELETE one PK, re-seed, compact
    // again. MoR must keep the deleted key hidden and the exact row total must
    // match what we inserted after the delete.
    //
    // Which rewrite this drives — full or warm subset — is deliberately not
    // asserted: the candidate is a proper subset whenever a current file is
    // settled or in the other tier, and the sizes here do not pin that down
    // (see `aggressive_key_deletion_compaction_config`). What the PK buys is
    // the key-delete topology — a PK-less table resolves to position-based
    // deletion and a different MoR path.
    let schema = pk_schema();
    let (table, ctx, table_id) =
        build_append_only_key_delete_table(&fixture, "key_delete_survives", Arc::clone(&schema))
            .await;

    let batch_rows: i64 = 1500;
    let seed_batches = 8_i64;
    for batch_idx in 0..seed_batches {
        common::insert_batch(
            &table,
            make_batch(&schema, batch_idx * batch_rows, batch_rows),
        )
        .await?;
    }

    let Some((_snap_a, files_a)) = wait_until_current_snapshot_compacts(
        &table,
        &fixture,
        "key_delete_survives",
        usize::try_from(seed_batches).expect("fits"),
    )
    .await?
    else {
        panic!("key-mode append-only table should produce a small-file compaction candidate");
    };
    assert!(
        files_a < usize::try_from(seed_batches).expect("fits"),
        "phase-A compact must reduce file count"
    );

    let filter = col("id").eq(lit(10_i64));
    let plan = table
        .delete_from(&ctx.state(), vec![filter])
        .await
        .expect("plan delete");
    let _ = datafusion_physical_plan::collect(plan, ctx.task_ctx())
        .await
        .expect("run delete");

    assert_eq!(
        count_rows_matching(&ctx, "key_delete_survives", "id = 10").await,
        0,
        "delete must hide id=10"
    );
    let before = count_rows(&ctx, "key_delete_survives").await;
    let snap_after_delete = fixture
        .catalog
        .get_table("key_delete_survives")
        .await?
        .current_snapshot_id;

    // Seed more small files and drive the deterministic `maybe_compact` path
    // after each write (waits for the compaction lock — `try_lock` can miss
    // under concurrent suite load). At least one pass under the pending key
    // delete must rewrite so MoR survival is actually exercised.
    let more_batches = 10_i64;
    let mut phase_b_compacted = false;
    for batch_idx in 0..more_batches {
        common::insert_batch(
            &table,
            make_batch(&schema, 50_000 + batch_idx * batch_rows, batch_rows),
        )
        .await?;
        if run_compaction(&table).await {
            phase_b_compacted = true;
        }
    }
    // One final multi-pass attempt if post-write/threshold gating delayed the
    // rewrite until after the last insert.
    if !phase_b_compacted {
        for _ in 0..4 {
            if run_compaction(&table).await {
                phase_b_compacted = true;
                break;
            }
        }
    }

    let snap_after = fixture
        .catalog
        .get_table("key_delete_survives")
        .await?
        .current_snapshot_id;
    let files_after = count_vortex_files(&fixture.data_path, &table_id, &snap_after).await;
    assert!(
        phase_b_compacted || snap_after != snap_after_delete,
        "phase B must compact under the pending key delete \
         (compacted={phase_b_compacted}, snap {snap_after_delete}→{snap_after}, \
         files={files_after})"
    );

    // Exact expected total: rows before re-seed + more_batches * batch_rows.
    // (id=10 is already excluded from `before`.)
    let expected = before + batch_rows * more_batches;
    let after = count_rows(&ctx, "key_delete_survives").await;
    assert_eq!(
        after, expected,
        "exact row total after re-seed + compact (before={before}, more={more_batches}*{batch_rows})"
    );
    assert_eq!(
        count_rows_matching(&ctx, "key_delete_survives", "id = 10").await,
        0,
        "deleted key must remain hidden after compaction (MoR kept)"
    );

    Ok(())
}

// Regression test for #12602. Pins the hazard every fan-out assertion here rests
// on: a seed's own appends get consolidated by a post-write pass nobody asked
// for, so a fan-out listed after the writes is already the compacted one.
// Reaching that state deliberately — rather than hoping to race into it — is
// what makes this deterministic: from here a premise listed off the store can
// never be beaten, because the pass that produced it also spent the one-shot
// `new_files_since_last_compaction` credit the explicit trigger needs.
test_with_backends!(a_seed_is_consolidated_before_its_fanout_can_be_listed);
async fn a_seed_is_consolidated_before_its_fanout_can_be_listed(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = pk_schema();
    // A real PK (and no `OnConflict`), so the table is not silently resolved to
    // the position-based strategy the way a PK-less one is.
    let (table, ctx, table_id) =
        build_append_only_key_delete_table(&fixture, "consolidated_seed", Arc::clone(&schema))
            .await;

    let batch_rows: i64 = 1500;
    let batches = 12_i64;
    // One append of 1500 rows clears `INLINE_MAX_ROWS` and, at ~130 KiB against a
    // 1 MiB target, is not sharded, so the un-compacted seed is one file per
    // append. Counted in appends because that is the number the test controls.
    let seeded_appends = usize::try_from(batches).expect("batch count fits usize");
    for batch_idx in 0..batches {
        common::insert_batch(
            &table,
            make_batch(&schema, batch_idx * batch_rows, batch_rows),
        )
        .await?;
    }

    // Let the unasked-for pass finish instead of racing it.
    drain_in_flight_maintenance_bounded(
        &table,
        &fixture,
        "consolidated_seed",
        "listing the settled fan-out",
    )
    .await?;

    let settled_snapshot = fixture
        .catalog
        .get_table("consolidated_seed")
        .await?
        .current_snapshot_id;
    let settled_files = count_vortex_files(&fixture.data_path, &table_id, &settled_snapshot).await;
    assert!(
        settled_files < seeded_appends,
        "a post-write pass must consolidate the seed unprompted, which is what makes a \
         listed fan-out unusable as the premise (seeded={seeded_appends}, settled={settled_files})"
    );

    // Measured against the seeded appends the reduction is still visible from this
    // already-consolidated state; measured against `settled_files` it could not be.
    let Some((_post_snap, post_count)) =
        wait_until_current_snapshot_compacts(&table, &fixture, "consolidated_seed", seeded_appends)
            .await?
    else {
        panic!("an already-consolidated seed must still report a reduced fan-out");
    };
    assert!(
        post_count < seeded_appends,
        "fan-out must stay below the seeded appends (seeded={seeded_appends}, post={post_count})"
    );

    // The drain waits for the pass rather than cancelling it, so every row it
    // consolidated must still be readable.
    assert_eq!(
        count_rows(&ctx, "consolidated_seed").await,
        batch_rows * batches,
        "draining compaction must preserve every seeded row"
    );

    Ok(())
}

test_with_backends!(full_rewrite_reduces_small_file_fanout);
async fn full_rewrite_reduces_small_file_fanout(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = pk_schema();
    let (table, ctx, _table_id) = build_table(
        &fixture,
        "full_rewrite_fanout",
        Arc::clone(&schema),
        None,
        // No primary key, so `DeletionMode::Key` resolves to position-based
        // deletion and `subset_rewrite_eligibility` rejects the subset rewrite
        // outright — what this exercises is the full-rewrite small-file path.
        // The subset rewrite is covered by `p1_subset_path_test.rs`, which
        // asserts the recorded path rather than inferring it from the config.
        aggressive_key_deletion_compaction_config(),
    )
    .await;

    // Sample the pre-seed snapshot BEFORE writing: an append can drive a
    // post-write compaction that advances the snapshot mid-seed, so a pointer
    // read taken after the loop is not reliably the un-compacted one.
    let pre_snapshot = fixture
        .catalog
        .get_table("full_rewrite_fanout")
        .await?
        .current_snapshot_id;

    let batch_rows: i64 = 1500;
    let batches = 12_i64;
    for batch_idx in 0..batches {
        common::insert_batch(
            &table,
            make_batch(&schema, batch_idx * batch_rows, batch_rows),
        )
        .await?;
    }

    // Each append clears `INLINE_MAX_ROWS` and is too small to shard, so an
    // un-compacted seed is one vortex file per append. That seeded count — not a
    // count listed after the writes — is the fan-out compaction has to beat; see
    // `wait_until_current_snapshot_compacts` for why a listed count can already
    // be the compacted one.
    let seeded_appends = usize::try_from(batches).expect("batch count fits usize");
    let Some((post_snap, post_count)) = wait_until_current_snapshot_compacts(
        &table,
        &fixture,
        "full_rewrite_fanout",
        seeded_appends,
    )
    .await?
    else {
        panic!("small-file compaction should fire");
    };

    assert_ne!(post_snap, pre_snapshot, "compact must advance the snapshot");
    assert!(
        post_count < seeded_appends,
        "compaction must strictly reduce fan-out (seeded={seeded_appends}, post={post_count})"
    );
    assert_eq!(
        count_rows(&ctx, "full_rewrite_fanout").await,
        batch_rows * batches
    );
    Ok(())
}
