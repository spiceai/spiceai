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
use cayenne::metadata::{CreateTableOptions, DeletionMode, VortexConfig};
use cayenne::{CayenneCatalog, CayenneTableProvider, CayenneTableProviderBuilder, MetadataCatalog};
use datafusion::datasource::TableProvider;
use datafusion::prelude::*;

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
/// triggered by ANY warm file, key-mode deletes.
fn cold_table_options(
    fixture: &common::TestFixture,
    table_name: &str,
    schema: &Arc<Schema>,
    cold_dir: &std::path::Path,
    gc_interval_ms: u64,
) -> CreateTableOptions {
    CreateTableOptions {
        table_name: table_name.to_string(),
        schema: Arc::clone(schema),
        primary_key: vec!["id".to_string()],
        on_conflict: None,
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
// Concurrent scan vs promotion (F1 regression)
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

/// F1 regression: a cold promotion has TWO visibility publication points — the
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
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let cold_dir = fixture.temp_dir.path().join("cold");
    std::fs::create_dir_all(&cold_dir)?;

    // Short GC interval: it doubles as the orphan grace, which is exactly the
    // time-domain behavior under test.
    const GRACE_MS: u64 = 150;
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
        assert!(path.is_file(), "manifest file exists on disk: {path:?}");
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
            "manifest-referenced file survives GC: {path:?}"
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
            "superseded generation-1 file reclaimed by GC: {path:?}"
        );
    }
    for path in &live_paths {
        assert!(
            path.is_file(),
            "current-generation file survives GC: {path:?}"
        );
    }
    // Query correctness is unaffected throughout.
    assert_eq!(row_count(&ctx, "gc_t").await?, 202, "199 + 3 new rows");

    Ok(())
}
