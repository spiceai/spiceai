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

//! Integration tests for the cold object-store tier (storage-cascade bottom
//! tier): whole-table promotion to a (local `file://`) cold store, cross-tier
//! scan correctness, and the key-delete-after-promotion invariant.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use cayenne::metadata::{CdcDurability, CreateTableOptions, DeletionMode, VortexConfig};
use cayenne::{
    CayenneCatalog, CayenneTableProvider, CayenneTableProviderBuilder, MetadataCatalog,
    SlotAdvancer,
};
use datafusion::datasource::TableProvider;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::prelude::*;
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};

type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

test_with_backends!(test_cold_tier_promotion_cross_tier_scan_and_delete_impl);

async fn row_count(ctx: &SessionContext, table: &str) -> TestResult<i64> {
    let results = ctx
        .sql(&format!("SELECT COUNT(*) AS c FROM {table}"))
        .await?
        .collect()
        .await?;
    Ok(results
        .first()
        .and_then(|b| b.column(0).as_any().downcast_ref::<Int64Array>())
        .and_then(|a| a.values().first())
        .copied()
        .unwrap_or(0))
}

async fn collect_pairs(ctx: &SessionContext, sql: &str) -> TestResult<Vec<(i64, i64)>> {
    let batches = ctx.sql(sql).await?.collect().await?;
    let mut rows = Vec::new();
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id column Int64");
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("value column Int64");
        for row in 0..batch.num_rows() {
            rows.push((ids.value(row), values.value(row)));
        }
    }
    rows.sort_unstable();
    Ok(rows)
}

async fn delete_id(table: &Arc<CayenneTableProvider>, id: i64) -> TestResult<u64> {
    let ctx = SessionContext::new();
    let plan = table
        .delete_from(&ctx.state(), vec![col("id").eq(lit(id))])
        .await?;
    let results = datafusion::physical_plan::collect(plan, ctx.task_ctx()).await?;
    Ok(results
        .first()
        .and_then(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<arrow::array::UInt64Array>()
        })
        .and_then(|a| a.values().first())
        .copied()
        .unwrap_or(0))
}

async fn test_cold_tier_promotion_cross_tier_scan_and_delete_impl(
    fixture: common::TestFixture,
) -> TestResult<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));

    // Local `file://` cold tier — no object-store config needed (the default
    // local store resolves it).
    let cold_dir = fixture.temp_dir.path().join("cold");
    std::fs::create_dir_all(&cold_dir)?;
    let cold_url = format!("file://{}", cold_dir.to_string_lossy());

    let table_options = CreateTableOptions {
        table_name: "cold_t".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["id".to_string()],
        on_conflict: None,
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: VortexConfig {
            // Cold tier on the local fs, clustered by `id`, triggered by ANY
            // warm file so the test is deterministic.
            cold_tier_location: Some(cold_url),
            cold_clustering_columns: vec!["id".to_string()],
            cold_tier_warm_max_files: 1,
            cold_target_file_size_mb: 16,
            deletion_mode: DeletionMode::Key,
            ..VortexConfig::default()
        },
    };

    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let ctx = SessionContext::new();
    let table = Arc::new(
        CayenneTableProvider::create_table(catalog, table_options, ctx.runtime_env()).await?,
    );
    ctx.register_table("cold_t", Arc::clone(&table) as Arc<dyn TableProvider>)?;

    // Insert 200 rows (value = id * 2) across two batches.
    for range in [0i64..100, 100..200] {
        let ids: Vec<i64> = range.collect();
        let values: Vec<i64> = ids.iter().map(|i| i * 2).collect();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(Int64Array::from(values)),
            ],
        )?;
        common::insert_batch(table.as_ref(), batch).await?;
    }

    // Flush the in-RAM/inline tiers to durable warm Vortex files so the
    // promotion trigger (which reads the warm file set) fires.
    let _ = table.checkpoint_inlined_data().await;
    let _ = table.checkpoint_mem_tier().await;

    // Graduate the warm tier to the cold object store (Z-order clustered).
    let promoted = table.promote_warm_to_cold().await?;
    assert!(
        promoted,
        "promotion should fire with cold_tier_warm_max_files = 1"
    );

    // Cold files are registered in the metastore manifest with the full row set.
    let cold = fixture
        .catalog
        .list_cold_tier_files(table.table_id())
        .await?;
    assert!(
        !cold.is_empty(),
        "expected cold-tier files registered after promotion"
    );
    let cold_rows: i64 = cold.iter().map(|f| f.row_count).sum();
    assert_eq!(cold_rows, 200, "all 200 rows graduated to cold");
    assert!(
        cold.iter().all(|f| !f.statistics_blob.is_empty()),
        "each cold file carries a footer statistics blob for listing-time pruning"
    );

    // The physical cold files exist on the local cold store.
    let physical_cold_files = count_vortex_files(&cold_dir);
    assert!(
        physical_cold_files >= 1,
        "expected at least one physical .vortex file on the cold store, got {physical_cold_files}"
    );

    // Physical layout is grouped under `{sanitized_table_name}-{table_id}/data/`:
    // the name prefix makes a shared datalake location navigable, and the UUIDv7
    // suffix keeps the prefix collision-free across tables/instances.
    let expected_segment = format!("cold_t-{}", table.table_id());
    assert!(
        cold_dir.join(&expected_segment).join("data").is_dir(),
        "expected cold objects under '{expected_segment}/data/'; cold dir entries: {:?}",
        std::fs::read_dir(&cold_dir)
            .map(|d| d.flatten().map(|e| e.file_name()).collect::<Vec<_>>())
    );

    // Cross-tier scan: warm is now an empty snapshot, so returning all rows
    // proves the cold branch is read + unioned correctly.
    assert_eq!(
        row_count(&ctx, "cold_t").await?,
        200,
        "cross-tier scan returns all promoted rows from the cold tier"
    );
    assert_eq!(
        collect_pairs(&ctx, "SELECT id, value FROM cold_t WHERE id = 42").await?,
        vec![(42, 84)],
        "point lookup over the cold tier returns the right row"
    );
    assert_eq!(
        collect_pairs(&ctx, "SELECT id, value FROM cold_t ORDER BY id LIMIT 3").await?,
        vec![(0, 0), (1, 2), (2, 4)],
        "ordered cross-tier scan with a limit returns correct rows"
    );

    // Key-delete-after-promotion: a delete must hide a row that now lives ONLY
    // in the cold tier (the cold branch applies the shared key-delete filter).
    let deleted = delete_id(&table, 42).await?;
    eprintln!("[cold_tier_test] DELETE id=42 reported rows-affected = {deleted}");
    // The data-correctness invariant: the row is hidden and the live count drops,
    // regardless of the reported rows-affected count (which is a separate concern).
    assert!(
        collect_pairs(&ctx, "SELECT id, value FROM cold_t WHERE id = 42")
            .await?
            .is_empty(),
        "a delete after promotion hides the cold-resident row (Ignore-filter invariant)"
    );
    assert_eq!(
        row_count(&ctx, "cold_t").await?,
        199,
        "exactly one row removed across the cold tier"
    );

    // Insert more warm rows, then promote AGAIN. The tombstone for id=42
    // dirties the (single) prior cold file, so this promotion rewrites it
    // together with the warm delta; the commit's replace-then-register must
    // prevent gen-1 rows from being double-counted alongside gen-2.
    let batch2 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![200i64, 201, 202])),
            Arc::new(Int64Array::from(vec![400i64, 402, 404])),
        ],
    )?;
    common::insert_batch(table.as_ref(), batch2).await?;
    let _ = table.checkpoint_inlined_data().await;
    let _ = table.checkpoint_mem_tier().await;
    assert!(
        table.promote_warm_to_cold().await?,
        "second promotion should fire"
    );

    // 199 (post-delete) + 3 new = 202, with NO gen-1 duplication.
    assert_eq!(
        row_count(&ctx, "cold_t").await?,
        202,
        "repeated whole-table promotion must not double-count the prior cold generation"
    );
    assert!(
        collect_pairs(&ctx, "SELECT id, value FROM cold_t WHERE id = 42")
            .await?
            .is_empty(),
        "the delete stays applied across a second promotion"
    );
    let cold2 = fixture
        .catalog
        .list_cold_tier_files(table.table_id())
        .await?;
    let regraduated_rows: i64 = cold2.iter().map(|f| f.row_count).sum();
    assert_eq!(
        regraduated_rows, 202,
        "cold manifest holds exactly the live row set after replace-all promotion"
    );

    Ok(())
}

/// Recursively count `.vortex` files under `dir`.
fn count_vortex_files(dir: &std::path::Path) -> usize {
    let mut count = 0;
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                count += count_vortex_files(&path);
            } else if path.extension().and_then(|e| e.to_str()) == Some("vortex") {
                count += 1;
            }
        }
    }
    count
}

test_with_backends!(test_cold_tier_carry_forward_promotion_impl);

/// Carry-forward (incremental) promotion: a promotion rewrites only the warm
/// data plus the cold files a tombstone may touch; every other cold file is
/// carried forward by manifest reference — same `file_url`, its object never
/// re-read or re-written.
async fn test_cold_tier_carry_forward_promotion_impl(
    fixture: common::TestFixture,
) -> TestResult<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let cold_dir = fixture.temp_dir.path().join("cold");
    std::fs::create_dir_all(&cold_dir)?;
    let cold_url = format!("file://{}", cold_dir.to_string_lossy());

    let table_options = CreateTableOptions {
        table_name: "cf_t".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["id".to_string()],
        on_conflict: None,
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: VortexConfig {
            cold_tier_location: Some(cold_url),
            cold_clustering_columns: vec!["id".to_string()],
            cold_tier_warm_max_files: 1,
            cold_target_file_size_mb: 16,
            deletion_mode: DeletionMode::Key,
            ..VortexConfig::default()
        },
    };

    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let ctx = SessionContext::new();
    let table = Arc::new(
        CayenneTableProvider::create_table(catalog, table_options, ctx.runtime_env()).await?,
    );
    ctx.register_table("cf_t", Arc::clone(&table) as Arc<dyn TableProvider>)?;

    let insert_range = |range: std::ops::Range<i64>| {
        let ids: Vec<i64> = range.collect();
        let values: Vec<i64> = ids.iter().map(|i| i * 2).collect();
        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(Int64Array::from(values)),
            ],
        )
    };

    // Generation 1: ids 0..100.
    common::insert_batch(table.as_ref(), insert_range(0..100)?).await?;
    let _ = table.checkpoint_inlined_data().await;
    let _ = table.checkpoint_mem_tier().await;
    assert!(table.promote_warm_to_cold().await?, "promotion 1 fires");
    let gen1: Vec<String> = fixture
        .catalog
        .list_cold_tier_files(table.table_id())
        .await?
        .into_iter()
        .map(|f| f.file_url)
        .collect();
    assert!(!gen1.is_empty(), "generation 1 registered");

    // Generation 2: ids 100..200. NO tombstones exist, so promotion 2 must
    // carry every generation-1 file forward VERBATIM (same file_url) and add
    // new files for the warm data only.
    common::insert_batch(table.as_ref(), insert_range(100..200)?).await?;
    let _ = table.checkpoint_inlined_data().await;
    let _ = table.checkpoint_mem_tier().await;
    assert!(table.promote_warm_to_cold().await?, "promotion 2 fires");
    let after2 = fixture
        .catalog
        .list_cold_tier_files(table.table_id())
        .await?;
    let after2_urls: Vec<&String> = after2.iter().map(|f| &f.file_url).collect();
    for url in &gen1 {
        assert!(
            after2_urls.contains(&url),
            "tombstone-free promotion carries generation-1 file forward: {url}"
        );
    }
    assert!(
        after2.len() > gen1.len(),
        "promotion 2 adds new files for the warm delta"
    );
    assert_eq!(row_count(&ctx, "cf_t").await?, 200, "all rows visible");

    // Delete an id that lives in generation 1 (0..100), then promote again
    // with fresh warm data. The tombstone dirties generation-1 files (their id
    // rectangles contain 5); generation-2 files (100..200) are provably clean
    // and must survive with their exact file_urls, while dirty files are
    // rewritten (their old urls leave the manifest).
    let deleted = delete_id(&table, 5).await?;
    eprintln!("[carry_forward_test] DELETE id=5 rows-affected = {deleted}");
    common::insert_batch(table.as_ref(), insert_range(200..203)?).await?;
    let _ = table.checkpoint_inlined_data().await;
    let _ = table.checkpoint_mem_tier().await;
    assert!(table.promote_warm_to_cold().await?, "promotion 3 fires");

    let after3 = fixture
        .catalog
        .list_cold_tier_files(table.table_id())
        .await?;
    let after3_urls: Vec<&String> = after3.iter().map(|f| &f.file_url).collect();
    let gen2_only: Vec<&String> = after2
        .iter()
        .map(|f| &f.file_url)
        .filter(|u| !gen1.contains(u))
        .collect();
    for url in &gen2_only {
        assert!(
            after3_urls.contains(url),
            "clean generation-2 file carried through the dirty rewrite: {url}"
        );
    }
    // A promotion writes MULTIPLE partition files, so generation 1 is several
    // files covering id sub-ranges. The id=5 tombstone dirties only the file
    // whose rectangle contains 5 — that one must be rewritten (url dropped);
    // its clean siblings must be carried. Both sides firing proves the
    // classification is file-granular WITHIN a generation.
    let gen1_dropped = gen1.iter().filter(|u| !after3_urls.contains(u)).count();
    let gen1_carried = gen1.len() - gen1_dropped;
    assert!(
        gen1_dropped >= 1,
        "the generation-1 file containing the tombstoned id must be rewritten (none dropped of {})",
        gen1.len()
    );
    assert!(
        gen1.len() == 1 || gen1_carried >= 1,
        "clean generation-1 sibling files must be carried, not rewritten (all {} dropped)",
        gen1.len()
    );

    // Correctness across the carry-forward rewrite: the tombstoned row is
    // physically gone, everything else is intact, no double counting.
    assert_eq!(
        row_count(&ctx, "cf_t").await?,
        202,
        "200 - 1 deleted + 3 new rows"
    );
    assert!(
        collect_pairs(&ctx, "SELECT id, value FROM cf_t WHERE id = 5")
            .await?
            .is_empty(),
        "tombstoned row physically dropped by the dirty rewrite"
    );
    assert_eq!(
        collect_pairs(&ctx, "SELECT id, value FROM cf_t WHERE id = 150").await?,
        vec![(150, 300)],
        "carried generation-2 row still readable"
    );
    let manifest_rows: i64 = after3.iter().map(|f| f.row_count).sum();
    assert_eq!(
        manifest_rows, 202,
        "manifest row counts match the live row set"
    );

    Ok(())
}

/// Build the standard two-column (id, value) cold-tier table options used by
/// the tests below: `file://` cold location, clustered by `id`, promotion
/// triggered by ANY warm file, key-mode deletes. `on_conflict` selects append
/// (`None`) or PK-upsert semantics.
fn cold_table_options_with_conflict(
    fixture: &common::TestFixture,
    table_name: &str,
    schema: &Arc<Schema>,
    cold_dir: &std::path::Path,
    gc_interval_ms: u64,
    on_conflict: Option<OnConflict>,
) -> CreateTableOptions {
    CreateTableOptions {
        table_name: table_name.to_string(),
        schema: Arc::clone(schema),
        primary_key: vec!["id".to_string()],
        on_conflict,
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: VortexConfig {
            cold_tier_location: Some(format!("file://{}", cold_dir.to_string_lossy())),
            cold_clustering_columns: vec!["id".to_string()],
            cold_tier_warm_max_files: 1,
            cold_target_file_size_mb: 16,
            cold_tier_gc_interval_ms: gc_interval_ms,
            deletion_mode: DeletionMode::Key,
            ..VortexConfig::default()
        },
    }
}

fn cold_table_options(
    fixture: &common::TestFixture,
    table_name: &str,
    schema: &Arc<Schema>,
    cold_dir: &std::path::Path,
    gc_interval_ms: u64,
) -> CreateTableOptions {
    cold_table_options_with_conflict(fixture, table_name, schema, cold_dir, gc_interval_ms, None)
}

/// Insert `range` as (id, id * 2) rows.
async fn insert_id_range(
    table: &CayenneTableProvider,
    schema: &Arc<Schema>,
    range: std::ops::Range<i64>,
) -> TestResult<()> {
    let ids: Vec<i64> = range.collect();
    let values: Vec<i64> = ids.iter().map(|i| i * 2).collect();
    let batch = RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(values)),
        ],
    )?;
    common::insert_batch(table, batch).await?;
    Ok(())
}

/// Flush the in-RAM/inline tiers to durable warm Vortex files so the
/// promotion trigger (which reads the warm file set) fires.
async fn flush_warm(table: &CayenneTableProvider) {
    let _ = table.checkpoint_inlined_data().await;
    let _ = table.checkpoint_mem_tier().await;
}

// ============================================================================
// Concurrent scan vs promotion (double-count regression)
// ============================================================================

/// Concurrent `COUNT(*)` hammer: tasks that scan `table` in a tight loop until
/// stopped, recording every observed count.
struct ScanHammer {
    stop: Arc<AtomicBool>,
    handles: Vec<tokio::task::JoinHandle<Result<Vec<i64>, String>>>,
}

fn spawn_scan_hammer(ctx: &SessionContext, table: &'static str, tasks: usize) -> ScanHammer {
    let stop = Arc::new(AtomicBool::new(false));
    let handles = (0..tasks)
        .map(|_| {
            let ctx = ctx.clone();
            let stop = Arc::clone(&stop);
            tokio::spawn(async move {
                let mut observed = Vec::new();
                while !stop.load(Ordering::Relaxed) {
                    let batches = ctx
                        .sql(&format!("SELECT COUNT(*) FROM {table}"))
                        .await
                        .map_err(|e| e.to_string())?
                        .collect()
                        .await
                        .map_err(|e| e.to_string())?;
                    let count = batches
                        .first()
                        .and_then(|b| b.column(0).as_any().downcast_ref::<Int64Array>())
                        .and_then(|a| a.values().first())
                        .copied()
                        .unwrap_or(-1);
                    observed.push(count);
                    // Yield so the promotion task interleaves between scans on a
                    // single-threaded test runtime.
                    tokio::task::yield_now().await;
                }
                Ok(observed)
            })
        })
        .collect();
    ScanHammer { stop, handles }
}

impl ScanHammer {
    /// Stop the hammer and assert every observed count equals `expected`.
    async fn stop_and_assert_all(self, expected: i64, phase: &str) -> TestResult<()> {
        self.stop.store(true, Ordering::Relaxed);
        let mut observations = 0usize;
        for handle in self.handles {
            let observed = handle.await?.map_err(Box::<dyn std::error::Error>::from)?;
            for count in &observed {
                assert_eq!(
                    *count, expected,
                    "{phase}: a concurrent scan observed {count} rows (expected {expected}) — the cold manifest commit and the warm snapshot flip must publish atomically w.r.t. scans"
                );
            }
            observations += observed.len();
        }
        assert!(
            observations > 0,
            "{phase}: the scan hammer must complete at least one scan"
        );
        Ok(())
    }
}

test_with_backends!(test_cold_tier_concurrent_scan_during_promotion_impl);

/// Double-count regression: a cold promotion has TWO visibility publication points — the
/// metastore cold-manifest commit (the cold scan branch lists files straight
/// from the metastore) and the in-memory warm snapshot flip. Before the fix
/// they published at different times, so a scan racing the gap paired the OLD
/// warm snapshot with the NEW cold manifest and counted the promoted rows
/// TWICE. Hammer `COUNT(*)` from concurrent tasks across three promotions
/// (fresh, dirty-rewrite, carry-forward); every observation must equal the
/// live row count.
async fn test_cold_tier_concurrent_scan_during_promotion_impl(
    fixture: common::TestFixture,
) -> TestResult<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let cold_dir = fixture.temp_dir.path().join("cold");
    std::fs::create_dir_all(&cold_dir)?;

    let options = cold_table_options(&fixture, "race_t", &schema, &cold_dir, 300_000);
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let ctx = SessionContext::new();
    let table =
        Arc::new(CayenneTableProvider::create_table(catalog, options, ctx.runtime_env()).await?);
    ctx.register_table("race_t", Arc::clone(&table) as Arc<dyn TableProvider>)?;

    // Promotion 1: fresh graduation of 400 rows.
    insert_id_range(&table, &schema, 0..400).await?;
    flush_warm(&table).await;
    let hammer = spawn_scan_hammer(&ctx, "race_t", 4);
    assert!(table.promote_warm_to_cold().await?, "promotion 1 fires");
    hammer
        .stop_and_assert_all(400, "promotion 1 (fresh)")
        .await?;

    // Promotion 2: a tombstone dirties the prior cold generation, so this one
    // rewrites cold files (replace-then-register) while scans run.
    delete_id(&table, 7).await?;
    insert_id_range(&table, &schema, 400..403).await?;
    flush_warm(&table).await;
    let hammer = spawn_scan_hammer(&ctx, "race_t", 4);
    assert!(table.promote_warm_to_cold().await?, "promotion 2 fires");
    hammer
        .stop_and_assert_all(402, "promotion 2 (dirty rewrite)")
        .await?;

    // Promotion 3: tombstone-free carry-forward plus a warm delta.
    insert_id_range(&table, &schema, 500..600).await?;
    flush_warm(&table).await;
    let hammer = spawn_scan_hammer(&ctx, "race_t", 4);
    assert!(table.promote_warm_to_cold().await?, "promotion 3 fires");
    hammer
        .stop_and_assert_all(502, "promotion 3 (carry-forward)")
        .await?;

    Ok(())
}

test_with_backends!(test_cold_promotion_couples_manifest_and_snapshot_impl);

/// A scan captures the cold manifest together with the warm snapshot id under one
/// `listing_fence.read()`, and the scan-view cache keys that whole bundle on the
/// snapshot id. Both rest on ONE metastore invariant: a promotion rewrites
/// `cayenne_cold_tier_file` and repoints `cayenne_table.current_snapshot_id` in
/// the SAME transaction, so the manifest cannot move without the cache key moving
/// with it.
///
/// A cold-manifest write that left the snapshot id alone would let the cache keep
/// serving a bundle whose manifest predates its own key, and the cold rows that
/// write published would stay invisible to scans until something else flipped the
/// warm snapshot. Pin the coupling directly, on both the fresh-graduation and the
/// dirty-rewrite commit paths.
async fn test_cold_promotion_couples_manifest_and_snapshot_impl(
    fixture: common::TestFixture,
) -> TestResult<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let cold_dir = fixture.temp_dir.path().join("cold");
    std::fs::create_dir_all(&cold_dir)?;

    let options = cold_table_options(&fixture, "couple_t", &schema, &cold_dir, 300_000);
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let ctx = SessionContext::new();
    let table =
        Arc::new(CayenneTableProvider::create_table(catalog, options, ctx.runtime_env()).await?);
    ctx.register_table("couple_t", Arc::clone(&table) as Arc<dyn TableProvider>)?;

    insert_id_range(&table, &schema, 0..200).await?;
    flush_warm(&table).await;
    let before = fixture
        .catalog
        .get_table("couple_t")
        .await?
        .current_snapshot_id;
    assert!(
        fixture
            .catalog
            .list_cold_tier_files(table.table_id())
            .await?
            .is_empty(),
        "no cold files exist before the first promotion"
    );

    // Fresh graduation: the manifest gains rows, so the snapshot id must move.
    assert!(table.promote_warm_to_cold().await?, "promotion 1 fires");
    let after_fresh = fixture
        .catalog
        .get_table("couple_t")
        .await?
        .current_snapshot_id;
    assert!(
        !fixture
            .catalog
            .list_cold_tier_files(table.table_id())
            .await?
            .is_empty(),
        "the promotion registered cold files"
    );
    assert_ne!(
        after_fresh, before,
        "a promotion that writes the cold manifest must repoint current_snapshot_id \
         in the same commit — the scan-view cache key is what pairs the two"
    );

    // Dirty rewrite: a tombstone forces the prior cold generation to be replaced,
    // which rewrites the manifest again — and must move the id again.
    delete_id(&table, 7).await?;
    insert_id_range(&table, &schema, 200..203).await?;
    flush_warm(&table).await;
    assert!(table.promote_warm_to_cold().await?, "promotion 2 fires");
    let after_dirty = fixture
        .catalog
        .get_table("couple_t")
        .await?
        .current_snapshot_id;
    assert_ne!(
        after_dirty, after_fresh,
        "the dirty-rewrite commit must repoint current_snapshot_id too"
    );
    assert_eq!(
        row_count(&ctx, "couple_t").await?,
        202,
        "the two promotions must leave every live row visible exactly once"
    );

    Ok(())
}

// ============================================================================
// Restart: reopen a table that has a cold manifest
// ============================================================================

test_with_backends!(test_cold_tier_restart_reopen_impl);

/// Restart durability for the cold tier: reopen the table from a FRESH catalog
/// connection (full metastore restart, not just a new provider) after a
/// promotion and a post-promotion delete, then verify cross-tier reads, a new
/// delete against a cold-resident key, and a further promotion all work from
/// the persisted state alone (cold manifest, persisted `cold_tier_location`,
/// keyset rebuilt from the cold PK blooms).
async fn test_cold_tier_restart_reopen_impl(fixture: common::TestFixture) -> TestResult<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let cold_dir = fixture.temp_dir.path().join("cold");
    std::fs::create_dir_all(&cold_dir)?;

    // Session 1: create, load, promote, delete a cold-resident key.
    let table_id = {
        let options = cold_table_options(&fixture, "restart_t", &schema, &cold_dir, 300_000);
        let catalog: Arc<dyn MetadataCatalog> =
            Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
        let ctx = SessionContext::new();
        let table = Arc::new(
            CayenneTableProvider::create_table(catalog, options, ctx.runtime_env()).await?,
        );
        ctx.register_table("restart_t", Arc::clone(&table) as Arc<dyn TableProvider>)?;

        insert_id_range(&table, &schema, 0..200).await?;
        flush_warm(&table).await;
        assert!(table.promote_warm_to_cold().await?, "promotion fires");
        delete_id(&table, 42).await?;
        assert_eq!(row_count(&ctx, "restart_t").await?, 199);
        table.table_id().to_string()
        // Session 1's provider, context, and background tasks drop here.
    };

    // Session 2: a NEW catalog connection over the same metastore file — the
    // provider must rebuild everything from persisted state.
    let catalog2 = Arc::new(CayenneCatalog::new(fixture.connection_string())?);
    catalog2.init().await?;
    let ctx2 = SessionContext::new();
    let reopened = Arc::new(
        CayenneTableProviderBuilder::new(
            Arc::clone(&catalog2) as Arc<dyn MetadataCatalog>,
            ctx2.runtime_env(),
        )
        .open("restart_t")
        .await?,
    );
    assert_eq!(
        reopened.table_id(),
        table_id,
        "reopen resolves the same table"
    );
    ctx2.register_table("restart_t", Arc::clone(&reopened) as Arc<dyn TableProvider>)?;

    // Cross-tier reads from the persisted cold manifest.
    assert_eq!(
        row_count(&ctx2, "restart_t").await?,
        199,
        "cold-resident rows and the pre-restart delete survive a restart"
    );
    assert_eq!(
        collect_pairs(&ctx2, "SELECT id, value FROM restart_t WHERE id = 41").await?,
        vec![(41, 82)],
        "point lookup over the cold tier works after restart"
    );
    assert!(
        collect_pairs(&ctx2, "SELECT id, value FROM restart_t WHERE id = 42")
            .await?
            .is_empty(),
        "the pre-restart tombstone stays applied after restart"
    );

    // A NEW delete against a cold-resident key exercises the rebuilt
    // key-existence path (`ColdPkExistence` / keyset from the cold manifest).
    delete_id(&reopened, 7).await?;
    assert!(
        collect_pairs(&ctx2, "SELECT id, value FROM restart_t WHERE id = 7")
            .await?
            .is_empty(),
        "a delete issued after restart hides the cold-resident row"
    );
    assert_eq!(row_count(&ctx2, "restart_t").await?, 198);

    // A further promotion from the reopened provider: the id=7 tombstone
    // dirties the prior generation, so this exercises classification against
    // the reloaded manifest + blooms end-to-end.
    insert_id_range(&reopened, &schema, 300..303).await?;
    flush_warm(&reopened).await;
    assert!(
        reopened.promote_warm_to_cold().await?,
        "promotion after restart fires"
    );
    assert_eq!(
        row_count(&ctx2, "restart_t").await?,
        201,
        "198 surviving + 3 new rows after the post-restart promotion"
    );
    let manifest_rows: i64 = catalog2
        .list_cold_tier_files(reopened.table_id())
        .await?
        .iter()
        .map(|f| f.row_count)
        .sum();
    assert_eq!(
        manifest_rows, 201,
        "cold manifest matches the live row set after the post-restart promotion"
    );

    Ok(())
}

// ============================================================================
// End-to-end physical GC
// ============================================================================

/// Absolute filesystem path of a `file://` manifest URL.
fn file_url_to_path(url: &str) -> std::path::PathBuf {
    std::path::PathBuf::from(url.trim_start_matches("file://"))
}

test_with_backends!(test_cold_tier_gc_end_to_end_impl);

/// End-to-end mark-and-sweep over the physical cold store: an orphan `.vortex`
/// object is deleted only after being observed orphaned for a full grace
/// interval (mark on one tick, sweep on a later one); manifest-referenced
/// files and non-`.vortex` objects are never touched; a superseded generation
/// (dirty rewrite) is physically reclaimed the same way.
async fn test_cold_tier_gc_end_to_end_impl(fixture: common::TestFixture) -> TestResult<()> {
    // Short GC interval: it doubles as the orphan grace, which is exactly the
    // time-domain behavior under test.
    const GRACE_MS: u64 = 150;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let cold_dir = fixture.temp_dir.path().join("cold");
    std::fs::create_dir_all(&cold_dir)?;

    let options = cold_table_options(&fixture, "gc_t", &schema, &cold_dir, GRACE_MS);
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let ctx = SessionContext::new();
    let table =
        Arc::new(CayenneTableProvider::create_table(catalog, options, ctx.runtime_env()).await?);
    ctx.register_table("gc_t", Arc::clone(&table) as Arc<dyn TableProvider>)?;

    insert_id_range(&table, &schema, 0..200).await?;
    flush_warm(&table).await;
    assert!(table.promote_warm_to_cold().await?, "promotion 1 fires");

    let gen1_paths: Vec<std::path::PathBuf> = fixture
        .catalog
        .list_cold_tier_files(table.table_id())
        .await?
        .iter()
        .map(|f| file_url_to_path(&f.file_url))
        .collect();
    assert!(!gen1_paths.is_empty(), "generation 1 registered");
    for path in &gen1_paths {
        assert!(
            path.is_file(),
            "manifest file exists on disk: {}",
            path.display()
        );
    }

    // Plant an orphan .vortex object (as a crash between write and commit
    // would leave) and a non-.vortex sidecar under the table's data prefix.
    let data_dir = cold_dir
        .join(format!("gc_t-{}", table.table_id()))
        .join("data");
    let orphan = data_dir.join("zz-orphan").join("orphan.vortex");
    std::fs::create_dir_all(orphan.parent().expect("orphan parent"))?;
    std::fs::write(&orphan, b"not a real vortex file")?;
    let sidecar = data_dir.join("notes.txt");
    std::fs::write(&sidecar, b"operator note - GC must ignore me")?;

    // Tick 1 MARKS the orphan (first observation) but must not delete it —
    // the grace guarantees an in-flight scan a full interval to finish.
    table.run_cold_tier_gc_tick().await;
    assert!(
        orphan.is_file(),
        "an orphan is only marked on first observation, never deleted immediately"
    );

    // Let the orphan age past the grace, then sweep. The sleep IS the subject
    // here (the grace is a time-domain contract), so a fixed wait is correct.
    tokio::time::sleep(std::time::Duration::from_millis(GRACE_MS * 2)).await;
    table.run_cold_tier_gc_tick().await;
    assert!(
        !orphan.exists(),
        "an orphan aged past the grace is physically deleted"
    );
    assert!(
        sidecar.is_file(),
        "non-.vortex objects under the cold prefix are never touched"
    );
    for path in &gen1_paths {
        assert!(
            path.is_file(),
            "manifest-referenced file survives GC: {}",
            path.display()
        );
    }

    // Supersede generation 1: a tombstone dirties it, so promotion 2 rewrites
    // the dirty file(s); their old objects become orphans for GC.
    delete_id(&table, 5).await?;
    insert_id_range(&table, &schema, 200..203).await?;
    flush_warm(&table).await;
    assert!(table.promote_warm_to_cold().await?, "promotion 2 fires");

    let live_paths: Vec<std::path::PathBuf> = fixture
        .catalog
        .list_cold_tier_files(table.table_id())
        .await?
        .iter()
        .map(|f| file_url_to_path(&f.file_url))
        .collect();
    let superseded: Vec<&std::path::PathBuf> = gen1_paths
        .iter()
        .filter(|p| !live_paths.contains(p))
        .collect();
    assert!(
        !superseded.is_empty(),
        "the dirty rewrite must supersede at least one generation-1 file"
    );

    // Mark, age past the grace, sweep.
    table.run_cold_tier_gc_tick().await;
    tokio::time::sleep(std::time::Duration::from_millis(GRACE_MS * 2)).await;
    table.run_cold_tier_gc_tick().await;

    for path in &superseded {
        assert!(
            !path.exists(),
            "superseded generation-1 file reclaimed by GC: {}",
            path.display()
        );
    }
    for path in &live_paths {
        assert!(
            path.is_file(),
            "current-generation file survives GC: {}",
            path.display()
        );
    }
    // Query correctness is unaffected throughout.
    assert_eq!(row_count(&ctx, "gc_t").await?, 202, "199 + 3 new rows");

    Ok(())
}

// ============================================================================
// Upsert against cold-resident keys
// ============================================================================

test_with_backends!(test_cold_tier_upsert_after_promotion_impl);

/// Post-promotion upserts against COLD-resident keys: the updated key's old
/// version lives only in the cold tier, so the upsert's conflict deletion must
/// hide it there (rebuilt keyset / `ColdPkExistence`) — the row count must stay
/// stable and the new value must win. Reproduces the CH-benCH convergence
/// failure where every PROMOTED table over-counted by ≈ its update count
/// (stale cold versions left visible alongside the new warm versions).
async fn test_cold_tier_upsert_after_promotion_impl(
    fixture: common::TestFixture,
) -> TestResult<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let cold_dir = fixture.temp_dir.path().join("cold");
    std::fs::create_dir_all(&cold_dir)?;

    let options = cold_table_options_with_conflict(
        &fixture,
        "upsert_t",
        &schema,
        &cold_dir,
        300_000,
        Some(OnConflict::Upsert(ColumnReference::new(vec![
            "id".to_string(),
        ]))),
    );
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let ctx = SessionContext::new();
    let table =
        Arc::new(CayenneTableProvider::create_table(catalog, options, ctx.runtime_env()).await?);
    ctx.register_table("upsert_t", Arc::clone(&table) as Arc<dyn TableProvider>)?;

    // Load 1000 rows (value = id * 2) and graduate them to the cold tier.
    insert_id_range(&table, &schema, 0..1000).await?;
    flush_warm(&table).await;
    assert!(table.promote_warm_to_cold().await?, "promotion fires");
    assert_eq!(row_count(&ctx, "upsert_t").await?, 1000);

    // Upsert 500 EXISTING keys with new values (value = id * 10). Their old
    // versions are cold-resident; each must be hidden, not duplicated.
    let ids: Vec<i64> = (0..500).collect();
    let values: Vec<i64> = ids.iter().map(|i| i * 10).collect();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(values)),
        ],
    )?;
    common::insert_batch(table.as_ref(), batch).await?;

    assert_eq!(
        row_count(&ctx, "upsert_t").await?,
        1000,
        "upserting cold-resident keys must not duplicate them (old cold versions hidden)"
    );
    assert_eq!(
        collect_pairs(&ctx, "SELECT id, value FROM upsert_t WHERE id = 7").await?,
        vec![(7, 70)],
        "the upserted value wins over the cold-resident version"
    );
    assert_eq!(
        collect_pairs(&ctx, "SELECT id, value FROM upsert_t WHERE id = 700").await?,
        vec![(700, 1400)],
        "non-upserted cold rows keep their original value"
    );

    // The invariants must survive checkpointing and a second promotion (the
    // dirty rewrite physically drops the superseded cold versions).
    flush_warm(&table).await;
    assert_eq!(
        row_count(&ctx, "upsert_t").await?,
        1000,
        "count stable after checkpointing the upsert delta"
    );
    assert!(
        table.promote_warm_to_cold().await?,
        "second promotion fires"
    );
    assert_eq!(
        row_count(&ctx, "upsert_t").await?,
        1000,
        "count stable after the dirty-rewrite promotion"
    );
    assert_eq!(
        collect_pairs(&ctx, "SELECT id, value FROM upsert_t WHERE id = 7").await?,
        vec![(7, 70)],
        "upserted value survives the rewrite"
    );
    let manifest_rows: i64 = fixture
        .catalog
        .list_cold_tier_files(table.table_id())
        .await?
        .iter()
        .map(|f| f.row_count)
        .sum();
    assert_eq!(
        manifest_rows, 1000,
        "cold manifest holds exactly the live set"
    );

    Ok(())
}

/// Arms `cdc_durability: memory` deferral in tests (the runtime installs a real
/// advancer from the first replayable committer).
struct NoopSlotAdvancer;
#[async_trait::async_trait]
impl SlotAdvancer for NoopSlotAdvancer {
    async fn on_checkpoint_durable(&self, _durable_epoch: u64) {}
}

fn batch_to_stream(batch: RecordBatch) -> SendableRecordBatchStream {
    let schema = batch.schema();
    Box::pin(RecordBatchStreamAdapter::new(
        schema,
        futures::stream::iter([Ok(batch)]),
    ))
}

/// CDC-apply `rows` through the in-memory tier (`write_cdc_append_stream`) —
/// the production CDC upsert path for `cdc_durability: memory` tables.
async fn cdc_upsert(
    table: &Arc<CayenneTableProvider>,
    schema: &Arc<Schema>,
    rows: &[(i64, i64)],
) -> TestResult<()> {
    let ids: Vec<i64> = rows.iter().map(|(k, _)| *k).collect();
    let values: Vec<i64> = rows.iter().map(|(_, v)| *v).collect();
    let batch = RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(values)),
        ],
    )?;
    let ctx = SessionContext::new();
    let write = table
        .write_cdc_append_stream(batch_to_stream(batch), &ctx.task_ctx())
        .await?;
    if write.has_pending_finalize() {
        write.finish().await?;
    }
    Ok(())
}

test_with_backends!(test_cold_tier_cdc_memory_upsert_after_promotion_impl);

/// Run-6 reproduction path: `cdc_durability: memory`, SHARDED in-memory CDC
/// applies (`write_cdc_append_stream`), upserts against COLD-resident keys
/// after a promotion. The CH-benCH convergence failure showed every promoted
/// table over-counting by ≈ its update count — each first-update-per-key
/// leaving the stale cold version visible next to the new warm version.
async fn test_cold_tier_cdc_memory_upsert_after_promotion_impl(
    fixture: common::TestFixture,
) -> TestResult<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let cold_dir = fixture.temp_dir.path().join("cold");
    std::fs::create_dir_all(&cold_dir)?;

    let mut options = cold_table_options_with_conflict(
        &fixture,
        "cdc_mem_t",
        &schema,
        &cold_dir,
        300_000,
        Some(OnConflict::Upsert(ColumnReference::new(vec![
            "id".to_string(),
        ]))),
    );
    // Mirror the benchmark profile: in-memory CDC tier, N>1 PK-hash shards.
    options.vortex_config.cdc_durability = CdcDurability::Memory;
    options.vortex_config.cdc_mem_tier_shards = 4;

    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let ctx = SessionContext::new();
    let table =
        Arc::new(CayenneTableProvider::create_table(catalog, options, ctx.runtime_env()).await?);
    assert!(
        table.is_cdc_memory_mode(),
        "test profile must arm the in-memory CDC tier"
    );
    table.install_slot_advancer(Arc::new(NoopSlotAdvancer));
    ctx.register_table("cdc_mem_t", Arc::clone(&table) as Arc<dyn TableProvider>)?;

    // Load 1000 rows through the CDC path, flush, and promote to cold.
    let initial: Vec<(i64, i64)> = (0..1000).map(|i| (i, i * 2)).collect();
    cdc_upsert(&table, &schema, &initial).await?;
    flush_warm(&table).await;
    assert!(table.promote_warm_to_cold().await?, "promotion fires");
    assert_eq!(row_count(&ctx, "cdc_mem_t").await?, 1000);

    // CDC-update 500 cold-resident keys. Each must supersede its cold version.
    let updates: Vec<(i64, i64)> = (0..500).map(|i| (i, i * 10)).collect();
    cdc_upsert(&table, &schema, &updates).await?;

    assert_eq!(
        row_count(&ctx, "cdc_mem_t").await?,
        1000,
        "in-memory CDC upserts of cold-resident keys must not duplicate them"
    );
    assert_eq!(
        collect_pairs(&ctx, "SELECT id, value FROM cdc_mem_t WHERE id = 7").await?,
        vec![(7, 70)],
        "the CDC-updated value wins over the cold-resident version"
    );

    // Stability across checkpoint + second promotion (dirty rewrite).
    flush_warm(&table).await;
    assert_eq!(
        row_count(&ctx, "cdc_mem_t").await?,
        1000,
        "count stable after checkpointing the CDC update delta"
    );
    assert!(
        table.promote_warm_to_cold().await?,
        "second promotion fires"
    );
    assert_eq!(
        row_count(&ctx, "cdc_mem_t").await?,
        1000,
        "count stable after the dirty-rewrite promotion"
    );
    assert_eq!(
        collect_pairs(&ctx, "SELECT id, value FROM cdc_mem_t WHERE id = 7").await?,
        vec![(7, 70)],
        "updated value survives the rewrite"
    );

    Ok(())
}

test_with_backends!(test_cold_tier_bake_preserves_cold_masking_tombstones_impl);

/// THE CH-benCH `-cold` convergence bug: the seq-prefix bake rewrites WARM
/// protected snapshots, physically applying tombstones, then prunes them from
/// the deletion index — but it never rewrites COLD objects, so a pruned
/// tombstone that was masking a superseded cold-resident key silently
/// resurrects the stale cold row (observed as promoted tables over-counting
/// by ≈ their update count). The prune cutoff must be capped at the cold
/// manifest's max sequence so cold-masking tombstones survive until a
/// promotion physically applies them.
async fn test_cold_tier_bake_preserves_cold_masking_tombstones_impl(
    fixture: common::TestFixture,
) -> TestResult<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let cold_dir = fixture.temp_dir.path().join("cold");
    std::fs::create_dir_all(&cold_dir)?;

    let options = cold_table_options_with_conflict(
        &fixture,
        "bake_t",
        &schema,
        &cold_dir,
        300_000,
        Some(OnConflict::Upsert(ColumnReference::new(vec![
            "id".to_string(),
        ]))),
    );
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let ctx = SessionContext::new();
    let table =
        Arc::new(CayenneTableProvider::create_table(catalog, options, ctx.runtime_env()).await?);
    ctx.register_table("bake_t", Arc::clone(&table) as Arc<dyn TableProvider>)?;

    // 1000 rows graduated to the cold tier.
    insert_id_range(&table, &schema, 0..1000).await?;
    flush_warm(&table).await;
    assert!(table.promote_warm_to_cold().await?, "promotion fires");
    assert_eq!(row_count(&ctx, "bake_t").await?, 1000);

    // Rounds of upserts against cold-resident keys, each flushed into its own
    // protected snapshot, until the bake's minimum input set exists. Each
    // round updates a DISJOINT key range (as a real update stream spreads
    // over keys over time), so the oldest rounds' cold versions are masked
    // ONLY by their own tombstones — exactly the ones the bake prunes.
    let rounds: i64 = 6;
    for round in 0..rounds {
        let ids: Vec<i64> = (round * 100..(round + 1) * 100).collect();
        let values: Vec<i64> = ids.iter().map(|i| i * 100 + round + 1).collect();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(Int64Array::from(values)),
            ],
        )?;
        common::insert_batch(table.as_ref(), batch).await?;
        flush_warm(&table).await;
    }
    assert_eq!(
        row_count(&ctx, "bake_t").await?,
        1000,
        "pre-bake: upserted cold-resident keys are masked, not duplicated"
    );

    // Complete detached post-write maintenance before the bake: the bake
    // `try_lock`s the compaction lock and DECLINES (returns false) if a
    // maintenance pass from the last flush is still holding it
    table.flush_pending_maintenance().await?;
    table.drain_in_flight_maintenance().await?;

    // Run the REAL seq-prefix bake (the production pass that pruned the
    // cold-masking tombstones before the fix).
    let baked = table.bake_seq_prefix_protected_snapshots().await?;
    assert!(baked, "the bake must fire with 6 protected snapshots");

    assert_eq!(
        row_count(&ctx, "bake_t").await?,
        1000,
        "post-bake: cold-masking tombstones survive the prune — stale cold versions must not resurrect"
    );
    // Key 7 was updated in round 1 — the OLDEST round, squarely inside the
    // baked prefix (keep=3 of 6): its cold version must stay hidden.
    assert_eq!(
        collect_pairs(&ctx, "SELECT id, value FROM bake_t WHERE id = 7").await?,
        vec![(7, 701)],
        "a baked-prefix upserted value wins after the bake (cold version stays hidden)"
    );
    assert_eq!(
        collect_pairs(&ctx, "SELECT id, value FROM bake_t WHERE id = 900").await?,
        vec![(900, 1800)],
        "an un-upserted cold row keeps its original value after the bake"
    );

    // A subsequent promotion physically applies the retained tombstones and
    // clears the index; the count must hold through it.
    flush_warm(&table).await;
    assert!(
        table.promote_warm_to_cold().await?,
        "post-bake promotion fires"
    );
    assert_eq!(
        row_count(&ctx, "bake_t").await?,
        1000,
        "count stable after the dirty-rewrite promotion applies the retained tombstones"
    );

    Ok(())
}

test_with_backends!(test_cold_tier_cdc_upserts_concurrent_with_promotion_impl);

/// The CH-benCH shape proper: CDC upserts of existing keys keep flowing WHILE
/// the promotion runs (they serialize on the write lock and land immediately
/// after the fenced publish — against a freshly cleared keyset whose keys are
/// now cold-resident). Every update must supersede its cold version: the row
/// count must remain exactly the distinct-key count throughout.
async fn test_cold_tier_cdc_upserts_concurrent_with_promotion_impl(
    fixture: common::TestFixture,
) -> TestResult<()> {
    const KEYS: i64 = 2000;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let cold_dir = fixture.temp_dir.path().join("cold");
    std::fs::create_dir_all(&cold_dir)?;

    let mut options = cold_table_options_with_conflict(
        &fixture,
        "cdc_race_t",
        &schema,
        &cold_dir,
        300_000,
        Some(OnConflict::Upsert(ColumnReference::new(vec![
            "id".to_string(),
        ]))),
    );
    options.vortex_config.cdc_durability = CdcDurability::Memory;
    options.vortex_config.cdc_mem_tier_shards = 4;

    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let ctx = SessionContext::new();
    let table =
        Arc::new(CayenneTableProvider::create_table(catalog, options, ctx.runtime_env()).await?);
    table.install_slot_advancer(Arc::new(NoopSlotAdvancer));
    ctx.register_table("cdc_race_t", Arc::clone(&table) as Arc<dyn TableProvider>)?;

    let initial: Vec<(i64, i64)> = (0..KEYS).map(|i| (i, i * 2)).collect();
    cdc_upsert(&table, &schema, &initial).await?;
    flush_warm(&table).await;

    // Promotion on its own task; CDC updates pump concurrently. Each round
    // updates every key with a new generation value, exactly like TPC-C
    // updates hammering warehouse rows while the graduation runs.
    let promo_table = Arc::clone(&table);
    let promotion = tokio::spawn(async move { promo_table.promote_warm_to_cold().await });
    let mut generation: i64 = 0;
    while !promotion.is_finished() {
        generation += 1;
        let updates: Vec<(i64, i64)> = (0..KEYS).map(|i| (i, i * 100 + generation)).collect();
        cdc_upsert(&table, &schema, &updates).await?;
        tokio::task::yield_now().await;
    }
    assert!(
        promotion.await??,
        "promotion fires with the warm tier over threshold"
    );
    // A few more rounds strictly AFTER the publish (freshly cleared keyset,
    // keys now cold-resident).
    for _ in 0..3 {
        generation += 1;
        let updates: Vec<(i64, i64)> = (0..KEYS).map(|i| (i, i * 100 + generation)).collect();
        cdc_upsert(&table, &schema, &updates).await?;
    }

    assert_eq!(
        row_count(&ctx, "cdc_race_t").await?,
        KEYS,
        "updates racing a promotion must never duplicate cold-resident keys (generation {generation})"
    );

    // And the invariant must hold durably: checkpoint + second promotion.
    flush_warm(&table).await;
    assert_eq!(row_count(&ctx, "cdc_race_t").await?, KEYS);
    assert!(
        table.promote_warm_to_cold().await?,
        "second promotion fires"
    );
    assert_eq!(
        row_count(&ctx, "cdc_race_t").await?,
        KEYS,
        "count stable after the dirty-rewrite promotion"
    );
    assert_eq!(
        collect_pairs(&ctx, "SELECT id, value FROM cdc_race_t WHERE id = 7").await?,
        vec![(7, 700 + generation)],
        "the last generation's value wins"
    );

    Ok(())
}

// ============================================================================
// Promotion vs pipelined Stage-B finalize
// ============================================================================

/// Build the standard cold-tier table options plus the exact routing that
/// forces `write_cdc_append_stream` onto the staged pipelined branch —
/// non-partitioned, PK + upsert, `cdc_durability: file`, `inline_max_rows: 0`.
/// On that shape Stage-A stages the batch durably and returns a `CdcWrite`
/// whose `finish()` is the real Stage-B publish.
fn staged_pipelined_table_options(
    fixture: &common::TestFixture,
    table_name: &str,
    schema: &Arc<Schema>,
    cold_dir: &std::path::Path,
) -> CreateTableOptions {
    let mut options = cold_table_options_with_conflict(
        fixture,
        table_name,
        schema,
        cold_dir,
        300_000,
        Some(OnConflict::Upsert(ColumnReference::new(vec![
            "id".to_string(),
        ]))),
    );
    options.vortex_config.cdc_durability = CdcDurability::File;
    options.vortex_config.inline_max_rows = 0;
    options
}

/// (id, id * 2) rows for `range` as a single batch.
fn id_range_batch(schema: &Arc<Schema>, range: std::ops::Range<i64>) -> TestResult<RecordBatch> {
    let ids: Vec<i64> = range.collect();
    let values: Vec<i64> = ids.iter().map(|i| i * 2).collect();
    Ok(RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(values)),
        ],
    )?)
}

test_with_backends!(test_cold_tier_promotion_preserves_pending_stage_b_impl);

/// A pipelined Stage-A-committed / Stage-B-pending write must survive a
/// concurrent cold-tier promotion.
async fn test_cold_tier_promotion_preserves_pending_stage_b_impl(
    fixture: common::TestFixture,
) -> TestResult<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let cold_dir = fixture.temp_dir.path().join("cold");
    tokio::fs::create_dir_all(&cold_dir).await?;

    let options = staged_pipelined_table_options(&fixture, "stage_b_t", &schema, &cold_dir);
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let ctx = SessionContext::new();
    let table =
        Arc::new(CayenneTableProvider::create_table(catalog, options, ctx.runtime_env()).await?);
    ctx.register_table("stage_b_t", Arc::clone(&table) as Arc<dyn TableProvider>)?;

    // 1. Seed + settle so the warm tier has a durable file (promotion fires).
    insert_id_range(&table, &schema, 0..100).await?;
    flush_warm(&table).await;

    // 2. Stage-A: staged pipelined write, Stage-B held open.
    let write = table
        .write_cdc_append_stream(
            batch_to_stream(id_range_batch(&schema, 100..110)?),
            &ctx.task_ctx(),
        )
        .await?;
    // Load-bearing precondition: a future routing change (inlining default-on,
    // a new fast path) could silently divert the batch to a completed write and
    // the test would pass while testing nothing.
    assert!(
        write.has_pending_finalize(),
        "repro precondition: batch must take the staged pipelined path \
         (non-partitioned, file durability, inline_max_rows: 0)"
    );

    // 3. Promotion starts with Stage-B pending. It must drain the staged
    //    publish before capturing the visible set, so `finish()` is spawned
    //    (it completes during the drain — Stage-B needs only the visibility
    //    lock + listing fence, which promotion does not hold at that point).
    let finish_task = tokio::spawn(write.finish());
    let promoted = table.promote_warm_to_cold().await?;
    assert!(promoted, "promotion fires with a durable warm file");

    // 4. Stage-B publish completed (either before the promotion's capture or
    //    during its drain — never lost).
    finish_task.await??;

    // 5. Rows 100..110 must be visible now AND after reopen from a fresh
    //    catalog connection (the loss manifests at restart).
    assert_eq!(
        row_count(&ctx, "stage_b_t").await?,
        110,
        "a Stage-B publish concurrent with a promotion must not lose the staged rows"
    );
    assert_eq!(
        collect_pairs(&ctx, "SELECT id, value FROM stage_b_t WHERE id >= 100").await?,
        (100..110).map(|i| (i, i * 2)).collect::<Vec<_>>(),
        "the staged batch is visible after its Stage-B publish"
    );

    let catalog2 = Arc::new(CayenneCatalog::new(fixture.connection_string())?);
    catalog2.init().await?;
    let ctx2 = SessionContext::new();
    let reopened = Arc::new(
        CayenneTableProviderBuilder::new(
            Arc::clone(&catalog2) as Arc<dyn MetadataCatalog>,
            ctx2.runtime_env(),
        )
        .open("stage_b_t")
        .await?,
    );
    ctx2.register_table("stage_b_t", Arc::clone(&reopened) as Arc<dyn TableProvider>)?;
    assert_eq!(
        row_count(&ctx2, "stage_b_t").await?,
        110,
        "the staged batch survives a restart (source slot was acked at Stage-A — \
         losing it here is silent, unrecoverable loss)"
    );
    assert_eq!(
        collect_pairs(&ctx2, "SELECT id, value FROM stage_b_t WHERE id >= 100").await?,
        (100..110).map(|i| (i, i * 2)).collect::<Vec<_>>(),
        "the staged batch's rows are intact after restart"
    );

    Ok(())
}

test_with_backends!(test_cold_tier_promotion_racing_stage_b_finalize_impl);

/// Concurrent variant: `finish()` (Stage-B publish) and the promotion run
/// concurrently (`tokio::join!`), repeated so the publish lands at different
/// points inside the promotion — a cheap deterministic sweep of the window.
async fn test_cold_tier_promotion_racing_stage_b_finalize_impl(
    fixture: common::TestFixture,
) -> TestResult<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let cold_dir = fixture.temp_dir.path().join("cold");
    tokio::fs::create_dir_all(&cold_dir).await?;

    let options = staged_pipelined_table_options(&fixture, "stage_b_race_t", &schema, &cold_dir);
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let ctx = SessionContext::new();
    let table =
        Arc::new(CayenneTableProvider::create_table(catalog, options, ctx.runtime_env()).await?);
    ctx.register_table(
        "stage_b_race_t",
        Arc::clone(&table) as Arc<dyn TableProvider>,
    )?;

    let mut expected: i64 = 0;
    for iter in 0..10i64 {
        // Fresh warm data each round so the promotion trigger fires again.
        let base = iter * 1000;
        insert_id_range(&table, &schema, base..base + 50).await?;
        flush_warm(&table).await;
        expected += 50;

        let write = table
            .write_cdc_append_stream(
                batch_to_stream(id_range_batch(&schema, base + 500..base + 510)?),
                &ctx.task_ctx(),
            )
            .await?;
        assert!(
            write.has_pending_finalize(),
            "repro precondition (iteration {iter}): batch must take the staged pipelined path"
        );
        expected += 10;

        let (finished, promoted) = tokio::join!(write.finish(), table.promote_warm_to_cold());
        finished?;
        assert!(promoted?, "promotion fires on iteration {iter}");

        assert_eq!(
            row_count(&ctx, "stage_b_race_t").await?,
            expected,
            "iteration {iter}: a Stage-B publish racing the promotion must not lose rows"
        );
    }

    // The loss manifests at restart (the source slot was acked at Stage-A).
    let catalog2 = Arc::new(CayenneCatalog::new(fixture.connection_string())?);
    catalog2.init().await?;
    let ctx2 = SessionContext::new();
    let reopened = Arc::new(
        CayenneTableProviderBuilder::new(
            Arc::clone(&catalog2) as Arc<dyn MetadataCatalog>,
            ctx2.runtime_env(),
        )
        .open("stage_b_race_t")
        .await?,
    );
    ctx2.register_table(
        "stage_b_race_t",
        Arc::clone(&reopened) as Arc<dyn TableProvider>,
    )?;
    assert_eq!(
        row_count(&ctx2, "stage_b_race_t").await?,
        expected,
        "every staged batch survives a restart"
    );

    Ok(())
}

test_with_backends!(test_cold_tier_bounded_zorder_multi_run_promotion_impl);

/// Promotion with `cold_clustering_run_size_mb: Some(1)` — small enough that
/// the Z-order sort splits into several byte-bounded runs (the inserted raw
/// bytes alone exceed 3 run caps, and the sorted stream is the *augmented*
/// batches, which are strictly larger). Verifies the bounded sort is invisible
/// to correctness: the commit succeeds, every inserted row lands in the cold
/// manifest exactly once (row conservation across runs), and cross-tier scans
/// return the exact row set.
async fn test_cold_tier_bounded_zorder_multi_run_promotion_impl(
    fixture: common::TestFixture,
) -> TestResult<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let cold_dir = fixture.temp_dir.path().join("cold");
    std::fs::create_dir_all(&cold_dir)?;
    let cold_url = format!("file://{}", cold_dir.to_string_lossy());

    let run_size_mb = 1usize;
    let table_options = CreateTableOptions {
        table_name: "cold_runs_t".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["id".to_string()],
        on_conflict: None,
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: VortexConfig {
            cold_tier_location: Some(cold_url),
            // Multi-column key exercises real bit-interleaving across runs.
            cold_clustering_columns: vec!["id".to_string(), "value".to_string()],
            cold_tier_warm_max_files: 1,
            cold_target_file_size_mb: 16,
            cold_clustering_run_size_mb: Some(run_size_mb),
            deletion_mode: DeletionMode::Key,
            ..VortexConfig::default()
        },
    };

    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let ctx = SessionContext::new();
    let table = Arc::new(
        CayenneTableProvider::create_table(catalog, table_options, ctx.runtime_env()).await?,
    );
    ctx.register_table("cold_runs_t", Arc::clone(&table) as Arc<dyn TableProvider>)?;

    // 240k rows (value = id * 2) in 8 batches — enough raw bytes to force
    // multiple 1 MB sort runs by construction.
    let total_rows: i64 = 240_000;
    let batch_rows: i64 = 30_000;
    let mut raw_bytes = 0usize;
    for chunk_start in (0..total_rows).step_by(usize::try_from(batch_rows).expect("fits usize")) {
        let ids: Vec<i64> = (chunk_start..chunk_start + batch_rows).collect();
        let values: Vec<i64> = ids.iter().map(|i| i * 2).collect();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(Int64Array::from(values)),
            ],
        )?;
        raw_bytes += batch.get_array_memory_size();
        common::insert_batch(table.as_ref(), batch).await?;
    }
    assert!(
        raw_bytes > 3 * run_size_mb * 1024 * 1024,
        "test must feed enough input to force >= 3 sort runs (raw input {raw_bytes} bytes)"
    );

    let _ = table.checkpoint_inlined_data().await;
    let _ = table.checkpoint_mem_tier().await;
    assert!(
        table.promote_warm_to_cold().await?,
        "promotion should fire with cold_tier_warm_max_files = 1"
    );

    // Row conservation through the bounded multi-run sort: the manifest holds
    // every inserted row exactly once.
    let cold = fixture
        .catalog
        .list_cold_tier_files(table.table_id())
        .await?;
    assert!(!cold.is_empty(), "cold files registered after promotion");
    let cold_rows: i64 = cold.iter().map(|f| f.row_count).sum();
    assert_eq!(
        cold_rows, total_rows,
        "all rows graduated to cold exactly once despite multiple sort runs"
    );

    // Cross-tier scan correctness over the multi-run layout.
    assert_eq!(row_count(&ctx, "cold_runs_t").await?, total_rows);
    assert_eq!(
        collect_pairs(&ctx, "SELECT id, value FROM cold_runs_t WHERE id = 54321").await?,
        vec![(54_321, 108_642)],
        "point lookup lands on the right row"
    );
    assert_eq!(
        collect_pairs(
            &ctx,
            "SELECT id, value FROM cold_runs_t ORDER BY id DESC LIMIT 2"
        )
        .await?,
        // `collect_pairs` sorts ascending for deterministic comparison.
        vec![(239_998, 479_996), (239_999, 479_998)],
        "ordered scan over run-overlapping files returns correct rows"
    );

    Ok(())
}
