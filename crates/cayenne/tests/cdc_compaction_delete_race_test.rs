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

//! Targeted reproducer for a suspected CDC convergence bug: a delete that lands
//! while a full-snapshot compaction rewrite is in flight can be lost, so the
//! deleted row reappears after compaction completes.
//!
//! ## The hypothesis (from the code audit)
//!
//! The full-snapshot rewrite path
//! (`CayenneTableProvider::rewrite_current_snapshot_for_compaction`, table.rs)
//! does roughly:
//!
//! ```text
//!   let stream = self.visible_file_stream_for_rewrite(&ctx).await?;  // (A) snapshot live rows + deletions
//!   ... encode the consolidated file (long-running) ...
//!   self.commit_snapshot_rewrite(&new_snapshot_id).await?;          // (B) DELETE FROM cayenne_delete_file ...
//!   { let _fence = self.listing_fence.write().await;
//!     self.update_current_snapshot_id(&new_snapshot_id);
//!     self.clear_all_deletion_caches();                             // (C) wipes the ENTIRE deletion index, unconditionally
//!   }
//! ```
//!
//! Compaction holds `compaction_lock`; deletes hold `write_lock`. These are
//! **different mutexes**, so a delete can execute concurrently with the rewrite.
//!
//! A delete that commits **after (A)** (so its row is still present in the
//! visible stream and therefore written into the new consolidated file) but is
//! then wiped at **(C)** — `clear_all_deletion_caches()` takes no sequence
//! fence, unlike `prune_deletion_index_at_or_below` — leaves the table with the
//! row physically present and no tombstone hiding it. The row is resurrected.
//!
//! ## What this test does
//!
//! It deliberately races a stream of single-key deletes against a background
//! compaction loop over a file-rich snapshot (so the rewrite window is wide),
//! then quiesces and asserts that every deleted key is gone. If the bug is
//! present, at least one deleted key reappears and the convergence assertion
//! fails. When the bug is fixed, the test passes.
//!
//! It is timing-dependent (it provokes a real race), so it makes several
//! attempts and uses a wide rewrite window to keep reproduction reliable; it is
//! a *reproducer*, complemented by the broader randomized harness in
//! `mutation_property_test.rs`. The attempt count is scalable via
//! `CAYENNE_PROPTEST_SCALE` (see `common::env_scale`), so CI can dial
//! reproduction depth up or down without code changes.

#![allow(clippy::expect_used)]
#![allow(clippy::clone_on_ref_ptr)]

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use cayenne::metadata::{CreateTableOptions, DeletionMode, VortexConfig};
use cayenne::{CayenneTableProvider, MetadataCatalog};
use common::{BackendType, TestFixture};
use datafusion::datasource::TableProvider;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::{col, lit};
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};

type TestResult<T> = Result<T, Box<dyn std::error::Error>>;
type Model = BTreeMap<i64, i64>;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

/// Aggressive compaction config. `inline_max_rows: 0` forces every write to a
/// snapshot file so the snapshot is file-rich and a full rewrite has real work
/// to do (a wide race window). Background scheduler disabled; we drive it.
///
/// `deletion_mode: Key` is pinned explicitly: the default `Auto` resolves to
/// `position` for PK tables (see `metadata.rs`), which would make this test a
/// duplicate of the position-mode variant below. We want this case to exercise
/// the key-delete sequence-fence compaction path described in the header docs.
fn config() -> VortexConfig {
    VortexConfig {
        deletion_mode: DeletionMode::Key,
        target_vortex_file_size_mb: 1,
        compaction_trigger_files: 4,
        compaction_background_interval_ms: 0,
        inline_max_rows: 0,
        ..VortexConfig::default()
    }
}

async fn create_table(
    fixture: &TestFixture,
    table_name: &str,
) -> TestResult<(Arc<CayenneTableProvider>, SessionContext)> {
    let table_options = CreateTableOptions {
        table_name: table_name.to_string(),
        schema: schema(),
        primary_key: vec!["id".to_string()],
        on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
            "id".to_string(),
        ]))),
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: config(),
    };
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let ctx = SessionContext::new();
    let table = Arc::new(
        CayenneTableProvider::create_table(catalog, table_options, ctx.runtime_env()).await?,
    );
    ctx.register_table(table_name, Arc::clone(&table) as Arc<dyn TableProvider>)?;
    Ok((table, ctx))
}

/// Insert `ids` (each value = id*10) as one snapshot file.
async fn insert_block(table: &Arc<CayenneTableProvider>, ids: Vec<i64>) -> TestResult<()> {
    let values: Vec<i64> = ids.iter().map(|k| k * 10).collect();
    let batch = RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(values)),
        ],
    )?;
    common::insert_batch(table.as_ref(), batch).await?;
    Ok(())
}

async fn delete_key(table: &Arc<CayenneTableProvider>, key: i64) -> TestResult<()> {
    let ctx = SessionContext::new();
    let plan = table
        .delete_from(&ctx.state(), vec![col("id").eq(lit(key))])
        .await?;
    datafusion_physical_plan::collect(plan, ctx.task_ctx()).await?;
    Ok(())
}

async fn read_rows(ctx: &SessionContext, table_name: &str) -> TestResult<Model> {
    let df = ctx
        .sql(&format!("SELECT id, value FROM {table_name} ORDER BY id"))
        .await?;
    let results = df.collect().await?;
    let mut model = Model::new();
    for batch in &results {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id column should be Int64");
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("value column should be Int64");
        for idx in 0..batch.num_rows() {
            model.insert(ids.value(idx), values.value(idx));
        }
    }
    Ok(model)
}

/// Run the race scenario once against a freshly created table.
async fn run_once(fixture: &TestFixture, table_name: &str) -> TestResult<()> {
    let (table, ctx) = create_table(fixture, table_name).await?;

    // File-rich snapshot: 120 files × 25 rows = 3000 rows. A full-snapshot
    // rewrite has to read all 120 files and re-encode a consolidated file,
    // giving a wide window during which a delete can interleave.
    let population: i64 = 3000;
    let block = 25;
    for start in (0..population).step_by(usize::try_from(block).expect("block size fits usize")) {
        let ids: Vec<i64> = (start..(start + block).min(population)).collect();
        insert_block(&table, ids).await?;
    }
    let mut model: Model = (0..population).map(|k| (k, k * 10)).collect();

    // Background compaction loop hammering the full-rewrite path.
    let stop = Arc::new(AtomicBool::new(false));
    let bg_table = Arc::clone(&table);
    let bg_stop = Arc::clone(&stop);
    let compactor = tokio::spawn(async move {
        while !bg_stop.load(Ordering::Relaxed) {
            let _ = bg_table.maybe_compact_small_files().await;
            tokio::task::yield_now().await;
        }
    });

    // Foreground: delete a spread of distinct keys. Spacing the deletes out
    // across the compaction loop maximizes the chance that at least one commits
    // inside a rewrite window (after the visible stream is captured, before the
    // deletion cache is cleared). Every deleted key MUST stay gone.
    let victims: Vec<i64> = (0..200).map(|i| i * 13 % population).collect();
    for &v in &victims {
        delete_key(&table, v).await?;
        model.remove(&v);
        // brief pause so deletes are interleaved with, not batched ahead of,
        // the rewrite passes.
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }

    stop.store(true, Ordering::Relaxed);
    compactor.await.expect("compaction task joins cleanly");
    // Settle: one final rewrite must not resurrect anything either.
    table.maybe_compact_small_files().await?;

    let live = read_rows(&ctx, table_name).await?;

    let resurrected: Vec<i64> = victims
        .iter()
        .copied()
        .filter(|v| live.contains_key(v))
        .collect();

    assert!(
        resurrected.is_empty(),
        "CONVERGENCE BUG REPRODUCED: {} deleted key(s) reappeared after \
         compaction: {:?}\n(this means a delete that landed during a \
         full-snapshot rewrite was lost when clear_all_deletion_caches() ran)",
        resurrected.len(),
        resurrected,
    );

    assert_eq!(
        live, model,
        "live state diverged from model after concurrent deletes + compaction"
    );
    Ok(())
}

async fn delete_during_full_rewrite_is_not_lost_impl(fixture: TestFixture) -> TestResult<()> {
    // A few independent attempts; a real timing race need only surface once to
    // prove the hazard is reachable. Scalable via `CAYENNE_PROPTEST_SCALE` for a
    // lighter per-PR pass or a deeper nightly run; floored at 1 so the race is
    // still exercised at least once.
    for attempt in 0..attempt_count() {
        run_once(&fixture, &format!("race_{attempt}")).await?;
    }
    Ok(())
}

#[expect(
    clippy::cast_sign_loss,
    clippy::cast_possible_truncation,
    reason = "common::env_scale() is always positive and the result is floored at 1.0 before casting"
)]
fn attempt_count() -> u64 {
    (4.0 * common::env_scale("CAYENNE_PROPTEST_SCALE"))
        .round()
        .max(1.0) as u64
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_during_full_rewrite_is_not_lost_sqlite() -> TestResult<()> {
    common::run_with_backend(
        BackendType::Sqlite,
        delete_during_full_rewrite_is_not_lost_impl,
    )
    .await
    .map_err(|e| -> Box<dyn std::error::Error> { e })
}

// ===========================================================================
// Position-mode variant (coverage)
// ===========================================================================
//
// `deletion_mode: position` records deletes as file-scoped row-position bitmaps
// (`cached_deleted_row_ids`) rather than a sequence-tagged key index. The same
// full-rewrite race applies, and it is arguably worse: position tombstones are
// keyed by data-file path, so once compaction swaps the source file away there
// is nothing to carry forward (the sequence-prune that could rescue key-based
// deletes is an explicit no-op for position deletes). The crate already guards
// the *protected-snapshot subset* compaction for position tables by holding the
// write lock across the rewrite (`serialize_position_deletes`), but the
// *full-snapshot* rewrite reached via `maybe_compact_small_files` does NOT — it
// holds neither the write lock nor the visibility lock.
//
// This test is primarily coverage for that gap.

/// Aggressive compaction + `deletion_mode: position`. Inlining fully disabled so
/// every write lands as a Vortex file (position deletes apply to files).
fn position_config() -> VortexConfig {
    VortexConfig {
        deletion_mode: DeletionMode::Position,
        target_vortex_file_size_mb: 1,
        compaction_trigger_files: 4,
        compaction_background_interval_ms: 0,
        inline_max_rows: 0,
        inline_max_bytes: 0,
        inline_max_buffer_bytes: 0,
        ..VortexConfig::default()
    }
}

async fn create_position_table(
    fixture: &TestFixture,
    table_name: &str,
) -> TestResult<(Arc<CayenneTableProvider>, SessionContext)> {
    let table_options = CreateTableOptions {
        table_name: table_name.to_string(),
        schema: schema(),
        primary_key: vec!["id".to_string()],
        on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
            "id".to_string(),
        ]))),
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: position_config(),
    };
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let ctx = SessionContext::new();
    let table = Arc::new(
        CayenneTableProvider::create_table(catalog, table_options, ctx.runtime_env()).await?,
    );
    ctx.register_table(table_name, Arc::clone(&table) as Arc<dyn TableProvider>)?;
    Ok((table, ctx))
}

async fn run_once_position(fixture: &TestFixture, table_name: &str) -> TestResult<()> {
    let (table, ctx) = create_position_table(fixture, table_name).await?;

    let population: i64 = 3000;
    let block = 25;
    for start in (0..population).step_by(usize::try_from(block).expect("block size fits usize")) {
        let ids: Vec<i64> = (start..(start + block).min(population)).collect();
        insert_block(&table, ids).await?;
    }
    // Upgrade keyset entries FileUnlocated -> FilePositioned so deletes tombstone
    // by file position (otherwise they fall back to key-based deletes).
    table.run_position_capture().await?;
    let mut model: Model = (0..population).map(|k| (k, k * 10)).collect();

    let stop = Arc::new(AtomicBool::new(false));
    let bg_table = Arc::clone(&table);
    let bg_stop = Arc::clone(&stop);
    let compactor = tokio::spawn(async move {
        while !bg_stop.load(Ordering::Relaxed) {
            let _ = bg_table.maybe_compact_small_files().await;
            tokio::task::yield_now().await;
        }
    });

    let victims: Vec<i64> = (0..200).map(|i| i * 13 % population).collect();
    for &v in &victims {
        delete_key(&table, v).await?;
        model.remove(&v);
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }

    stop.store(true, Ordering::Relaxed);
    compactor.await.expect("compaction task joins cleanly");
    table.maybe_compact_small_files().await?;

    let live = read_rows(&ctx, table_name).await?;
    let resurrected: Vec<i64> = victims
        .iter()
        .copied()
        .filter(|v| live.contains_key(v))
        .collect();

    assert!(
        resurrected.is_empty(),
        "CONVERGENCE BUG REPRODUCED (position mode): {} deleted key(s) reappeared \
         after compaction: {:?}",
        resurrected.len(),
        resurrected,
    );
    assert_eq!(live, model, "position-mode live state diverged from model");
    Ok(())
}

async fn position_delete_during_full_rewrite_is_not_lost_impl(
    fixture: TestFixture,
) -> TestResult<()> {
    for attempt in 0..attempt_count() {
        run_once_position(&fixture, &format!("pos_race_{attempt}")).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn position_delete_during_full_rewrite_is_not_lost_sqlite() -> TestResult<()> {
    common::run_with_backend(
        BackendType::Sqlite,
        position_delete_during_full_rewrite_is_not_lost_impl,
    )
    .await
    .map_err(|e| -> Box<dyn std::error::Error> { e })
}
