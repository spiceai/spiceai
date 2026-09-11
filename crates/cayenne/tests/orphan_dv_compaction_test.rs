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

//! Scheduling for the orphaned key-deletion-vector sweep (issue #13637).
//!
//! A key deletion vector with delete sequence `D` shadows only data with
//! sequence `< D`, so it is orphaned exactly when the surviving-sequence floor
//! reaches `D`. Publications raise that floor — a committed seq-prefix bake, a
//! committed protected-snapshot size-tier merge, a retention `DELETE` that
//! empties a protected snapshot — so each of them has to signal the sweep; a
//! crash between such a publication and its signal has to be repaired the next
//! time the provider opens; and the sweep still has to leave behind every vector
//! that a live row, warm or cold, still needs.
//!
//! These tests drive the compaction passes directly (the doc-hidden entry points
//! the background trigger calls) and then wait on the real background sweep via
//! `drain_in_flight_maintenance`, so they exercise production scheduling rather
//! than the deterministic `drain_orphan_dv_sweep` drain the collector's own tests
//! use.

#![allow(clippy::expect_used)]

mod common;

use arrow::array::{BinaryArray, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use cayenne::metadata::{CreateTableOptions, DeleteFile, DeletionMode, DeletionType, VortexConfig};
use cayenne::{CayenneContext, CayenneTableProvider, CayenneTableProviderBuilder, MetadataCatalog};
use common::TestFixture;
use datafusion::datasource::TableProvider;
use datafusion::prelude::*;
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};
use std::path::{Path, PathBuf};
use std::sync::Arc;

type TestResult = Result<(), Box<dyn std::error::Error>>;

/// The production sweep threshold (`ORPHANED_DV_CLEANUP_MIN_FILES`, crate-private).
/// A scheduled sweep reclaims nothing until this many orphans have accumulated.
const SWEEP_MIN_FILES: usize = 20;

/// A file/protected-snapshot count no test reaches, so no automatic compaction
/// trigger fires and each test drives the floor-advancing pass itself.
const NO_AUTOMATIC_COMPACTION: usize = 1_000_000;

test_with_backends!(bake_publication_schedules_orphan_dv_cleanup_impl);
test_with_backends!(protected_subset_merge_schedules_orphan_dv_cleanup_impl);
test_with_backends!(scheduled_cleanup_keeps_a_deletion_vector_still_needed_impl);
test_with_backends!(provider_open_replays_an_orphan_backlog_left_by_a_crash_impl);
test_with_backends!(scheduled_cleanup_waits_for_the_orphan_threshold_impl);
test_with_backends!(scheduled_cleanup_keeps_a_deletion_vector_the_cold_tier_needs_impl);

// ============================================================================
// Tests
// ============================================================================

/// A committed seq-prefix bake raises the surviving-sequence floor and leaves
/// every DV it orphaned on disk and in `cayenne_delete_file`, so it must signal
/// the sweep that reclaims them.
async fn bake_publication_schedules_orphan_dv_cleanup_impl(fixture: TestFixture) -> TestResult {
    let table_name = "orphan_dv_bake";
    let ctx = SessionContext::new();
    let table = create_key_upsert_table(&fixture, table_name, ctx.runtime_env()).await?;
    let base_dir = fixture.data_path.join(table_name);

    // One key, upserted repeatedly: every upsert but the first writes a key DV
    // that shadows the previous copy, and lands its own protected snapshot.
    upsert_key(&fixture, &table, 1, 0..=SWEEP_MIN_FILES + 5).await?;
    quiesce(&table).await?;

    let before = key_dv_count(&fixture, &table).await;
    assert!(
        before > SWEEP_MIN_FILES,
        "precondition: more than the sweep threshold of key DVs exist, got {before}"
    );
    assert_eq!(
        count_arrow_files(&base_dir),
        before,
        "every catalog key-DV row has its .arrow file on disk before the bake"
    );

    assert!(
        table.bake_seq_prefix_protected_snapshots().await?,
        "the seq-prefix bake must commit with this many protected snapshots"
    );
    table.drain_in_flight_maintenance().await?;

    let after = key_dv_count(&fixture, &table).await;
    assert!(
        after < before,
        "the bake must schedule the orphaned-DV sweep: {before} key DVs before, {after} after"
    );
    assert_eq!(
        count_arrow_files(&base_dir),
        after,
        "reclaimed .arrow files must be unlinked, not just dropped from the catalog"
    );
    assert_eq!(
        read_ids(&ctx, &table, table_name).await?,
        vec![1],
        "the surviving row is unchanged by the cleanup"
    );

    Ok(())
}

/// The same obligation for the protected-snapshot size-tier merge — the other
/// primary source of orphaned key DVs. Covered separately from the bake because
/// each publishes through its own commit path.
async fn protected_subset_merge_schedules_orphan_dv_cleanup_impl(
    fixture: TestFixture,
) -> TestResult {
    let table_name = "orphan_dv_subset";
    let ctx = SessionContext::new();
    let table = create_key_upsert_table(&fixture, table_name, ctx.runtime_env()).await?;
    let base_dir = fixture.data_path.join(table_name);

    upsert_key(&fixture, &table, 1, 0..=SWEEP_MIN_FILES + 5).await?;
    quiesce(&table).await?;

    let before = key_dv_count(&fixture, &table).await;
    assert!(
        before > SWEEP_MIN_FILES,
        "precondition: more than the sweep threshold of key DVs exist, got {before}"
    );

    let table = reopen_with_size_tier_merge_armed(&fixture, table_name, ctx.runtime_env()).await?;
    // Settle the reopen's own startup pass before the merge, so what is reclaimed
    // below is attributable to the merge's signal and not to a startup worker
    // still in flight.
    table.drain_in_flight_maintenance().await?;
    assert_eq!(
        key_dv_count(&fixture, &table).await,
        before,
        "the floor has not moved yet, so opening the provider reclaims nothing"
    );

    assert!(
        table.compact_protected_snapshots_subset(usize::MAX).await?,
        "the size-tier merge must commit with this many protected snapshots"
    );
    table.drain_in_flight_maintenance().await?;

    let after = key_dv_count(&fixture, &table).await;
    assert!(
        after < before,
        "the size-tier merge must schedule the orphaned-DV sweep: {before} key DVs before, \
         {after} after"
    );
    assert_eq!(
        count_arrow_files(&base_dir),
        after,
        "reclaimed .arrow files must be unlinked, not just dropped from the catalog"
    );
    assert_eq!(
        read_ids(&ctx, &table, table_name).await?,
        vec![1],
        "the surviving row is unchanged by the cleanup"
    );

    Ok(())
}

/// Scheduling the sweep more often must not make it delete more. A DV whose
/// delete sequence is ABOVE the surviving-sequence floor still shadows a live
/// row: it has to survive the sweep, its `.arrow` file has to stay readable, and
/// the table has to return each key exactly once — including after a reopen,
/// which re-materializes the deletion index from those very files.
async fn scheduled_cleanup_keeps_a_deletion_vector_still_needed_impl(
    fixture: TestFixture,
) -> TestResult {
    let table_name = "orphan_dv_needed";
    let ctx = SessionContext::new();
    let table = create_key_upsert_table(&fixture, table_name, ctx.runtime_env()).await?;
    let base_dir = fixture.data_path.join(table_name);

    upsert_key(&fixture, &table, 1, 0..=SWEEP_MIN_FILES + 5).await?;
    quiesce(&table).await?;
    let before = key_dv_count(&fixture, &table).await;
    assert!(
        before > SWEEP_MIN_FILES,
        "precondition: more than the sweep threshold of key DVs exist, got {before}"
    );

    // Bound the merge to the OLDEST inputs, leaving the two newest protected
    // snapshots unmerged with their own thresholds. The floor lands on the older
    // of those, below the newest DVs — which still shadow rows in the snapshots
    // the merge did not touch.
    let table = reopen_with_size_tier_merge_armed(&fixture, table_name, ctx.runtime_env()).await?;
    // Settle the reopen's own startup pass before the merge, so what is reclaimed
    // below is attributable to the merge's signal and not to a startup worker
    // still in flight.
    table.drain_in_flight_maintenance().await?;
    assert_eq!(
        key_dv_count(&fixture, &table).await,
        before,
        "the floor has not moved yet, so opening the provider reclaims nothing"
    );

    assert!(
        table.compact_protected_snapshots_subset(before - 2).await?,
        "the bounded size-tier merge must commit"
    );
    table.drain_in_flight_maintenance().await?;

    let after = key_dv_count(&fixture, &table).await;
    assert!(
        after < before,
        "orphaned DVs must be reclaimed: {before} before, {after} after"
    );
    assert!(
        after > 0,
        "a DV above the surviving-sequence floor must be RETAINED, not swept"
    );
    assert_eq!(
        count_arrow_files(&base_dir),
        after,
        "the retained DV keeps its .arrow file; only the reclaimed ones are unlinked"
    );
    assert_eq!(
        read_ids(&ctx, &table, table_name).await?,
        vec![1],
        "the key is returned exactly once after cleanup"
    );

    // A reopen reloads the deletion index from the retained `.arrow` files and
    // errors if one the floor still needs went missing.
    let reopened = reopen(&fixture, table_name, ctx.runtime_env()).await?;
    assert_eq!(
        read_ids(&ctx, &reopened, table_name).await?,
        vec![1],
        "the key is returned exactly once after reopening over the swept state"
    );

    Ok(())
}

/// A publication and the in-memory signal that follows it are not atomic, so a
/// crash in that window loses the edge for good: the orphans stay eligible and
/// an idle table never asks for them again. Opening the provider must replay the
/// cleanup.
///
/// The durable post-crash state is written directly — orphan-eligible
/// `cayenne_delete_file` rows and their `.arrow` files, with no live provider
/// that could have signalled — which is exactly what the crashed process left
/// behind, and is also the shape of a backlog that accumulated before this
/// scheduling existed at all.
async fn provider_open_replays_an_orphan_backlog_left_by_a_crash_impl(
    fixture: TestFixture,
) -> TestResult {
    let table_name = "orphan_dv_startup";
    let ctx = SessionContext::new();
    let table = create_key_upsert_table(&fixture, table_name, ctx.runtime_env()).await?;
    let base_dir = fixture.data_path.join(table_name);

    insert_row(&table, 1, 100).await?;
    quiesce(&table).await?;

    // Sequence 0 is at or below any surviving-sequence floor, so these are
    // unconditionally orphan-eligible.
    let backlog = SWEEP_MIN_FILES + 1;
    let seeded = seed_orphan_delete_files(&fixture, &table, backlog, 0).await?;
    assert_eq!(
        key_dv_count(&fixture, &table).await,
        backlog,
        "precondition: the crashed process left a backlog above the sweep threshold"
    );

    let reopened = reopen(&fixture, table_name, ctx.runtime_env()).await?;
    reopened.drain_in_flight_maintenance().await?;

    assert_eq!(
        key_dv_count(&fixture, &table).await,
        0,
        "opening the provider must replay the cleanup the crash lost"
    );
    for path in &seeded {
        assert!(
            !path.exists(),
            "startup replay must unlink the orphaned DV file at {}",
            path.display()
        );
    }
    assert_eq!(
        count_arrow_files(&base_dir),
        0,
        "no orphaned .arrow file survives the startup replay"
    );
    assert_eq!(
        read_ids(&ctx, &reopened, table_name).await?,
        vec![1],
        "the row set is unchanged by the startup replay"
    );

    Ok(())
}

/// The sweep is throttled: a backlog below the threshold is left alone, and is
/// reclaimed once it crosses. Signalling the sweep from every floor-advancing
/// publication must not turn it into a per-commit unlink storm. Each opening of
/// the provider below is the signal.
async fn scheduled_cleanup_waits_for_the_orphan_threshold_impl(fixture: TestFixture) -> TestResult {
    let table_name = "orphan_dv_threshold";
    let ctx = SessionContext::new();
    let table = create_key_upsert_table(&fixture, table_name, ctx.runtime_env()).await?;

    insert_row(&table, 1, 100).await?;
    quiesce(&table).await?;

    let below = SWEEP_MIN_FILES - 1;
    seed_orphan_delete_files(&fixture, &table, below, 0).await?;

    let reopened = reopen(&fixture, table_name, ctx.runtime_env()).await?;
    reopened.drain_in_flight_maintenance().await?;
    assert_eq!(
        key_dv_count(&fixture, &table).await,
        below,
        "a backlog of {below} is below the sweep threshold and must be left alone"
    );

    // One more orphan reaches the threshold.
    seed_orphan_delete_files(&fixture, &reopened, 1, 0).await?;
    let reopened = reopen(&fixture, table_name, ctx.runtime_env()).await?;
    reopened.drain_in_flight_maintenance().await?;
    assert_eq!(
        key_dv_count(&fixture, &table).await,
        0,
        "at the threshold the whole backlog is reclaimed"
    );
    assert_eq!(
        read_ids(&ctx, &reopened, table_name).await?,
        vec![1],
        "the row set is unchanged by the cleanup"
    );

    Ok(())
}

/// RESURRECTION GUARD. The cold tier is a live branch of the scan that key DVs
/// apply to, and promotion moves the OLDEST rows there — below every warm
/// sequence. A surviving-sequence floor derived from the warm snapshots alone
/// therefore sits ABOVE cold-resident rows, and a DV that still hides a
/// superseded cold row looks orphaned. Deleting it durably resurrects that row on
/// the next load.
async fn scheduled_cleanup_keeps_a_deletion_vector_the_cold_tier_needs_impl(
    fixture: TestFixture,
) -> TestResult {
    let table_name = "orphan_dv_cold";
    let ctx = SessionContext::new();
    let cold_dir = fixture.temp_dir.path().join("cold_store");
    std::fs::create_dir_all(&cold_dir)?;
    let table = create_cold_tier_table(&fixture, table_name, &cold_dir, ctx.runtime_env()).await?;

    for id in 0..4 {
        insert_row(&table, id, id * 10).await?;
    }
    quiesce(&table).await?;
    assert!(
        table.promote_warm_to_cold().await?,
        "the rows must graduate to the cold tier"
    );
    quiesce(&table).await?;

    let cold_min = cold_tier_lower_bound(&fixture, &table).await;

    // Above the cold manifest's lower bound, so this DV still shadows
    // cold-resident rows. The promotion emptied the warm side, so nothing else
    // bounds the floor below it: a warm-only floor is `i64::MAX` here.
    let backlog = SWEEP_MIN_FILES + 1;
    let seeded = seed_orphan_delete_files(&fixture, &table, backlog, cold_min + 1).await?;

    let reopened = reopen(&fixture, table_name, ctx.runtime_env()).await?;
    reopened.drain_in_flight_maintenance().await?;

    assert_eq!(
        key_dv_count(&fixture, &table).await,
        backlog,
        "a DV above the cold tier's lowest row sequence still hides cold rows and must survive"
    );
    for path in &seeded {
        assert!(
            path.exists(),
            "the retained DV keeps its file at {}",
            path.display()
        );
    }
    assert_eq!(
        read_ids(&ctx, &reopened, table_name).await?,
        vec![0, 1, 2, 3],
        "the cold-resident rows are unchanged"
    );

    Ok(())
}

// ============================================================================
// Helpers
// ============================================================================

fn table_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

/// A file-mode (`inline_max_rows: 0`) primary-key table in KEY deletion mode:
/// every conflicting insert writes a key deletion vector and its own protected
/// snapshot, which is the configuration that accumulates orphaned DVs.
async fn create_key_upsert_table(
    fixture: &TestFixture,
    table_name: &str,
    runtime_env: Arc<datafusion::execution::runtime_env::RuntimeEnv>,
) -> Result<Arc<CayenneTableProvider>, Box<dyn std::error::Error>> {
    let table_dir = fixture.data_path.join(table_name);
    std::fs::create_dir_all(&table_dir)?;

    let options = CreateTableOptions {
        table_name: table_name.to_string(),
        schema: table_schema(),
        primary_key: vec!["id".to_string()],
        on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
            "id".to_string(),
        ]))),
        base_path: table_dir.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: parked_config(),
    };

    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    Ok(Arc::new(
        CayenneTableProviderBuilder::new(catalog, runtime_env)
            .create(options)
            .await?,
    ))
}

/// Key deletion mode with every automatic compaction trigger parked, so the test
/// owns when the surviving-sequence floor moves. At their defaults the
/// write-driven passes fold the protected snapshots — and clear the very DV rows
/// under test — before the test's own compaction call runs.
fn parked_config() -> VortexConfig {
    VortexConfig {
        inline_max_rows: 0,
        deletion_mode: DeletionMode::Key,
        compaction_trigger_files: NO_AUTOMATIC_COMPACTION,
        compaction_trigger_protected_snapshots: NO_AUTOMATIC_COMPACTION,
        compaction_trigger_snapshot_age_ms: 0,
        compaction_background_interval_ms: 0,
        ..VortexConfig::default()
    }
}

/// A key-mode primary-key table whose warm rows graduate to a local cold store
/// on the first promotion.
async fn create_cold_tier_table(
    fixture: &TestFixture,
    table_name: &str,
    cold_dir: &Path,
    runtime_env: Arc<datafusion::execution::runtime_env::RuntimeEnv>,
) -> Result<Arc<CayenneTableProvider>, Box<dyn std::error::Error>> {
    let table_dir = fixture.data_path.join(table_name);
    std::fs::create_dir_all(&table_dir)?;

    let options = CreateTableOptions {
        table_name: table_name.to_string(),
        schema: table_schema(),
        primary_key: vec!["id".to_string()],
        on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
            "id".to_string(),
        ]))),
        base_path: table_dir.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: VortexConfig {
            cold_tier_location: Some(format!("file://{}", cold_dir.to_string_lossy())),
            cold_tier_warm_max_files: 1,
            cold_target_file_size_mb: 16,
            ..parked_config()
        },
    };

    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    Ok(Arc::new(
        CayenneTableProviderBuilder::new(catalog, runtime_env)
            .create(options)
            .await?,
    ))
}

/// The lowest commit sequence the cold manifest claims for any promoted file.
async fn cold_tier_lower_bound(fixture: &TestFixture, table: &CayenneTableProvider) -> i64 {
    let files = fixture
        .catalog
        .list_cold_tier_files(&table.metadata().table_id)
        .await
        .expect("read cold manifest");
    assert!(
        !files.is_empty(),
        "precondition: the promotion recorded at least one cold file"
    );
    files
        .iter()
        .map(|f| f.min_sequence)
        .min()
        .expect("non-empty cold manifest has a minimum")
}

async fn reopen(
    fixture: &TestFixture,
    table_name: &str,
    runtime_env: Arc<datafusion::execution::runtime_env::RuntimeEnv>,
) -> Result<Arc<CayenneTableProvider>, Box<dyn std::error::Error>> {
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    Ok(Arc::new(
        CayenneTableProviderBuilder::new(catalog, runtime_env)
            .open(table_name)
            .await?,
    ))
}

/// Reopen with the protected-snapshot size-tier merge armed at its default
/// threshold. `compaction_trigger_protected_snapshots` is BOTH the automatic
/// trigger and the merge's own minimum-runs-per-tier, so a table configured to
/// never self-compact can also never merge; arming it only on a provider that
/// takes no writes lets the test run the merge exactly once, when it asks.
async fn reopen_with_size_tier_merge_armed(
    fixture: &TestFixture,
    table_name: &str,
    runtime_env: Arc<datafusion::execution::runtime_env::RuntimeEnv>,
) -> Result<Arc<CayenneTableProvider>, Box<dyn std::error::Error>> {
    let config = VortexConfig {
        compaction_trigger_protected_snapshots: VortexConfig::default()
            .compaction_trigger_protected_snapshots,
        ..parked_config()
    };
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    Ok(Arc::new(
        CayenneTableProviderBuilder::new(catalog, Arc::clone(&runtime_env))
            .with_context(CayenneContext::new(&config, runtime_env, table_name))
            .open(table_name)
            .await?,
    ))
}

async fn insert_row(
    table: &CayenneTableProvider,
    id: i64,
    value: i64,
) -> Result<(), Box<dyn std::error::Error>> {
    let batch = RecordBatch::try_new(
        table_schema(),
        vec![
            Arc::new(Int64Array::from(vec![id])),
            Arc::new(Int64Array::from(vec![value])),
        ],
    )?;
    common::insert_batch(table, batch).await?;
    Ok(())
}

/// Write `id` once per value in `values`; every write after the first conflicts
/// and so produces one key deletion vector.
async fn upsert_key(
    fixture: &TestFixture,
    table: &CayenneTableProvider,
    id: i64,
    values: std::ops::RangeInclusive<usize>,
) -> Result<(), Box<dyn std::error::Error>> {
    for value in values {
        insert_row(table, id, i64::try_from(value).expect("value fits i64")).await?;
        common::poll_inlined_data_count_zero(&fixture.catalog, &table.metadata().table_id).await?;
    }
    Ok(())
}

/// Settle every detached maintenance pass the writes queued, so the compaction
/// call that follows sees a stable live set and takes the compaction lock.
async fn quiesce(table: &CayenneTableProvider) -> TestResult {
    table.flush_pending_maintenance().await?;
    table.drain_in_flight_maintenance().await?;
    Ok(())
}

async fn key_dv_count(fixture: &TestFixture, table: &CayenneTableProvider) -> usize {
    fixture
        .catalog
        .get_table_delete_files(&table.metadata().table_id)
        .await
        .expect("read delete files")
        .iter()
        .filter(|df| df.source_data_file_path.is_none())
        .count()
}

/// Record `count` orphan-eligible key deletion vectors at `sequence`, each with
/// its `.arrow` file present — the durable state a process leaves behind when it
/// dies after a floor-advancing publication but before signalling the sweep.
/// Returns the file paths.
async fn seed_orphan_delete_files(
    fixture: &TestFixture,
    table: &CayenneTableProvider,
    count: usize,
    sequence: i64,
) -> Result<Vec<PathBuf>, Box<dyn std::error::Error>> {
    let table_id = table.metadata().table_id.clone();
    let dir = fixture
        .data_path
        .join(table.metadata().table_name.clone())
        .join(table.current_snapshot_id())
        .join("deletions");
    std::fs::create_dir_all(&dir)?;

    let mut paths = Vec::with_capacity(count);
    for _ in 0..count {
        let id = uuid::Uuid::now_v7().to_string();
        let path = dir.join(format!("delete_{id}.arrow"));
        write_empty_key_deletion_vector(&path)?;
        fixture
            .catalog
            .add_delete_file(DeleteFile {
                delete_file_id: id,
                table_id: table_id.clone(),
                source_data_file_path: None,
                path: path.to_string_lossy().to_string(),
                path_is_relative: false,
                format: "arrow_ipc".to_string(),
                delete_count: 0,
                file_size_bytes: 0,
                deletion_type: DeletionType::KeyBased,
                sequence_number: sequence,
                reinsert_sequence: None,
            })
            .await?;
        paths.push(path);
    }
    Ok(paths)
}

/// Write a real, loadable key-based deletion vector holding no keys: the loader
/// identifies the file as key-based from its `Binary` first column and reads no
/// tombstones out of it, so a seeded backlog changes the query result of nothing.
fn write_empty_key_deletion_vector(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "row_key",
        DataType::Binary,
        false,
    )]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(BinaryArray::from(Vec::<&[u8]>::new()))],
    )?;
    let file = std::fs::File::create(path)?;
    let mut writer = arrow_ipc::writer::FileWriter::try_new(file, &schema)?;
    writer.write(&batch)?;
    writer.finish()?;
    Ok(())
}

/// Count `.arrow` deletion-vector files physically present anywhere under `root`.
fn count_arrow_files(root: &Path) -> usize {
    fn walk(dir: &Path, count: &mut usize) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.filter_map(std::result::Result::ok) {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, count);
            } else if path.extension().is_some_and(|ext| ext == "arrow") {
                *count += 1;
            }
        }
    }
    let mut count = 0;
    walk(root, &mut count);
    count
}

async fn read_ids(
    ctx: &SessionContext,
    table: &Arc<CayenneTableProvider>,
    table_name: &str,
) -> Result<Vec<i64>, Box<dyn std::error::Error>> {
    let _ = ctx.deregister_table(table_name);
    ctx.register_table(table_name, Arc::clone(table) as Arc<dyn TableProvider>)?;
    let batches = ctx
        .sql(&format!("SELECT id FROM {table_name} ORDER BY id"))
        .await?
        .collect()
        .await?;
    let mut ids = Vec::new();
    for batch in &batches {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id column is Int64");
        ids.extend(column.values().iter().copied());
    }
    Ok(ids)
}
