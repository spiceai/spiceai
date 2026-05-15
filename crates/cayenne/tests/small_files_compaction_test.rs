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

use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use cayenne::metadata::{CreateTableOptions, VortexConfig};
use cayenne::{CayenneTableProvider, MetadataCatalog};

use datafusion::prelude::SessionContext;
use datafusion_table_providers::util::{column_reference::ColumnReference, on_conflict::OnConflict};

/// Build a tiny test schema with an i64 PK column.
fn pk_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Utf8, false),
    ]))
}

/// VortexConfig tuned aggressively for tests: tiny target file size so a few
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
        // Disable the background scheduler so tests are deterministic — we
        // drive compaction explicitly via maybe_compact_small_files() on the
        // inline path or by triggering it from the test body.
        compaction_background_interval_ms: 0,
        ..VortexConfig::default()
    }
}

/// Build a batch of `n` rows whose ids start at `start` and whose values are
/// derived strings. n must be > INLINE_MAX_ROWS (1024) to bypass inlining.
fn make_batch(schema: &Arc<Schema>, start: i64, n: i64) -> RecordBatch {
    let ids: Vec<i64> = (start..start + n).collect();
    let values: Vec<String> = ids.iter().map(|i| format!("v_{i}")).collect();
    RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(values)),
        ],
    )
    .expect("test batch is valid")
}

/// Count `.vortex` files in `<data_path>/<table_id>/<current_snapshot_id>`.
async fn count_vortex_files(data_path: &PathBuf, table_id: &str, snapshot_id: &str) -> usize {
    let snapshot_dir = data_path.join(table_id).join(snapshot_id);
    let mut entries = match tokio::fs::read_dir(&snapshot_dir).await {
        Ok(e) => e,
        Err(_) => return 0,
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

async fn build_table(
    fixture: &common::TestFixture,
    name: &str,
    schema: Arc<Schema>,
    pk: Option<&str>,
    vortex_config: VortexConfig,
) -> (Arc<CayenneTableProvider>, SessionContext, String) {
    let on_conflict = pk.map(|pk_col| OnConflict::Upsert(ColumnReference::new(vec![pk_col.to_string()])));
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
    let table =
        CayenneTableProvider::create_table(catalog_arc, options, ctx.runtime_env())
            .await
            .expect("create_table");
    let table = Arc::new(table);
    let table_id = fixture.catalog.get_table(name).await.expect("get_table").table_id;
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
    // file. Without compaction we'd expect ~20 files. With inline compaction
    // firing after each write past the trigger threshold, file count drops
    // monotonically as small files get merged into larger ones.
    let batch_rows: i64 = 1500;
    for batch_idx in 0..20_i64 {
        let start = batch_idx * batch_rows;
        let batch = make_batch(&schema, start, batch_rows);
        common::insert_batch(&table, batch).await?;
    }

    // Read the current snapshot id off the provider — compactions advance it.
    let snapshot_id = fixture
        .catalog
        .get_table("compaction_files")
        .await?
        .current_snapshot_id;
    let file_count = count_vortex_files(&fixture.data_path, &table_id, &snapshot_id).await;

    assert!(
        file_count <= 6,
        "expected post-compaction file count <= 6, found {file_count} files in snapshot {snapshot_id}"
    );

    // Row count must be preserved end-to-end.
    let total = count_rows(&ctx, "compaction_files").await;
    assert_eq!(total, batch_rows * 20);

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
        let values: Vec<String> = ids.iter().map(|i| format!("first_{i}")).collect();
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
        let values: Vec<String> = ids.iter().map(|i| format!("second_{i}")).collect();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(values)),
            ],
        )?;
        common::insert_batch(&table, batch).await?;
    }

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

test_with_backends!(compaction_idempotent_when_no_candidates);
async fn compaction_idempotent_when_no_candidates(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = pk_schema();
    // Disable inline triggers via a high trigger_files count, so the first few
    // writes don't auto-compact and we can call compact explicitly.
    let mut config = VortexConfig::default();
    config.target_vortex_file_size_mb = 1;
    config.compaction_trigger_files = 1000;
    config.compaction_background_interval_ms = 0;

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

    let file_count =
        count_vortex_files(&fixture.data_path, &table_id, &snapshot_after_first).await;
    assert_eq!(file_count, 1, "snapshot should still hold the original file");

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
    let mut config = VortexConfig::default();
    config.target_vortex_file_size_mb = 1;
    config.compaction_trigger_files = 100;
    config.compaction_background_interval_ms = 0;

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
