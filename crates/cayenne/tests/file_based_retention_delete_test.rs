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

//! Integration tests for file-based retention deletion.
//!
//! When a Cayenne table uses **position-based deletion** (no primary key) with
//! **time-based retention**, the `delete_from` path
//! prefers whole-file deletion over per-row deletion vectors.
//!
//! These tests verify that:
//! 1. Files whose `max(retention_col) < threshold` are physically deleted.
//! 2. Files with live data are preserved.
//! 3. Queries after deletion return correct results.
//! 4. The listing table is refreshed and subsequent scans see the updated state.

#![allow(clippy::expect_used)]

mod common;

use arrow::array::{Int64Array, RecordBatch, TimestampMicrosecondArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use cayenne::metadata::CreateTableOptions;
use cayenne::{
    CayenneTableProvider, CayenneTableProviderBuilder, MetadataCatalog, STAGING_DIR_NAME,
    TimeRetentionFilterBuilder,
};
use common::TestFixture;
use datafusion::datasource::TableProvider;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::prelude::*;
use datafusion_common::ScalarValue;
use datafusion_execution::cache::{TableScopedPath, cache_manager::CachedFileList};
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};
use object_store::ObjectMeta;
use std::sync::Arc;

type TestResult = Result<(), Box<dyn std::error::Error>>;

test_with_backends!(test_file_based_retention_deletes_expired_files_impl);
test_with_backends!(test_file_based_retention_no_eligible_files_impl);
test_with_backends!(test_file_based_retention_deletes_all_files_impl);
test_with_backends!(test_file_based_retention_mixed_file_not_deleted_impl);

// PK-based file retention tests
test_with_backends!(test_pk_file_based_retention_main_table_only_impl);

// List-files cache maintenance tests
test_with_backends!(test_cache_delta_applied_after_append_impl);
test_with_backends!(test_file_based_retention_targeted_cache_invalidation_impl);

/// Test: File-based retention physically deletes files that are fully expired.
///
/// Setup (3-second retention, position-based / no PK):
///   - file 1: `event_time` = now           → fresh (kept)
///   - file 2: `event_time` = now - 2s      → within retention (kept)
///   - file 3: `event_time` = now - 10s     → expired (deleted)
///
/// Steps:
/// 1. Insert 3 batches (separate Vortex files).
/// 2. Verify 3 `.vortex` files exist on disk.
/// 3. Call `delete_from` with `event_time < cutoff` (cutoff = now - 3s).
/// 4. Verify only 2 `.vortex` files remain.
/// 5. Verify count(*) = 2 and ids = [1, 2].
async fn test_file_based_retention_deletes_expired_files_impl(fixture: TestFixture) -> TestResult {
    let retention_seconds = 3;
    let table_name = "file_ret_delete";
    let ctx = SessionContext::new();
    let table =
        create_retention_table(&fixture, table_name, retention_seconds, ctx.runtime_env()).await?;

    // Insert each row as a separate batch → separate Vortex file.
    let now_us = chrono::Utc::now().timestamp_micros();
    insert_row(&table, 1, now_us).await?; // fresh
    insert_row(&table, 2, now_us - 2_000_000).await?; // 2s ago — within retention
    insert_row(&table, 3, now_us - 10_000_000).await?; // 10s ago — expired

    let dir = table_id_dir(&fixture, &table, table_name);
    assert_eq!(
        count_vortex_files(&dir),
        3,
        "Expected 3 Vortex files after 3 inserts"
    );

    // Execute file-based delete — should remove only the expired file
    let deleted = execute_delete(&table, retention_delete_filter(retention_seconds)).await?;
    assert_eq!(deleted, 1, "Should delete 1 row (the expired file)");

    assert_eq!(
        count_vortex_files(&dir),
        2,
        "Expected 2 Vortex files after deletion"
    );

    // Verify count(*) and individual rows
    assert_table_contents(
        &ctx,
        &table,
        table_name,
        &[1, 2],
        "After deleting expired file",
    )
    .await?;

    Ok(())
}

/// Test: No files are deleted when all data is within retention.
///
/// Setup (60-second retention, position-based / no PK):
///   - file 1: `event_time` = now           → fresh
///   - file 2: `event_time` = now - 5s      → within retention
///
/// Verify: `delete_from` returns 0 deleted rows, files are untouched,
/// and count(*) = 2 with ids [1, 2].
async fn test_file_based_retention_no_eligible_files_impl(fixture: TestFixture) -> TestResult {
    let retention_seconds = 60;
    let table_name = "file_ret_no_delete";
    let ctx = SessionContext::new();
    let table =
        create_retention_table(&fixture, table_name, retention_seconds, ctx.runtime_env()).await?;

    let now_us = chrono::Utc::now().timestamp_micros();
    insert_row(&table, 1, now_us).await?;
    insert_row(&table, 2, now_us - 5_000_000).await?;

    let dir = table_id_dir(&fixture, &table, table_name);
    assert_eq!(count_vortex_files(&dir), 2, "Expected 2 Vortex files");

    let deleted = execute_delete(&table, retention_delete_filter(retention_seconds)).await?;
    assert_eq!(deleted, 0, "No files should be deleted");

    assert_eq!(
        count_vortex_files(&dir),
        2,
        "All Vortex files should still exist"
    );

    assert_table_contents(
        &ctx,
        &table,
        table_name,
        &[1, 2],
        "No rows should be removed",
    )
    .await?;

    Ok(())
}

/// Test: All files are deleted when everything is expired.
///
/// Setup (1-second retention, position-based / no PK):
///   - file 1: `event_time` = now - 10s    → expired
///   - file 2: `event_time` = now - 20s    → expired
///
/// After deletion:
/// - 0 Vortex files remain.
/// - count(*) = 0 and no ids returned.
async fn test_file_based_retention_deletes_all_files_impl(fixture: TestFixture) -> TestResult {
    let retention_seconds = 1;
    let table_name = "file_ret_all_delete";
    let ctx = SessionContext::new();
    let table =
        create_retention_table(&fixture, table_name, retention_seconds, ctx.runtime_env()).await?;

    let now_us = chrono::Utc::now().timestamp_micros();
    insert_row(&table, 1, now_us - 10_000_000).await?; // 10s ago — expired
    insert_row(&table, 2, now_us - 20_000_000).await?; // 20s ago — expired

    let dir = table_id_dir(&fixture, &table, table_name);
    assert_eq!(count_vortex_files(&dir), 2, "Expected 2 Vortex files");

    let deleted = execute_delete(&table, retention_delete_filter(retention_seconds)).await?;
    assert_eq!(deleted, 2, "Should delete 2 rows (both files)");

    assert_eq!(
        count_vortex_files(&dir),
        0,
        "All Vortex files should be deleted"
    );

    assert_table_contents(&ctx, &table, table_name, &[], "No rows should remain").await?;

    Ok(())
}

/// Test: A file containing both expired and non-expired rows is NOT deleted.
///
/// Setup (3-second retention, position-based / no PK):
///   - file 1: mixed rows — id=1 (now), id=2 (10s ago expired)  → kept (max = now)
///   - file 2: expired only — id=3 (10s ago)                    → deleted
///
/// After deletion:
/// - File 1 is preserved (1 live + 1 expired row remain in the same file).
/// - File 2 is deleted.
/// - At query time, the scan-time retention filter hides the expired row (id=2),
///   so only the fresh row (id=1) is visible.
async fn test_file_based_retention_mixed_file_not_deleted_impl(fixture: TestFixture) -> TestResult {
    let retention_seconds = 3;
    let table_name = "file_ret_mixed";
    let ctx = SessionContext::new();
    let table =
        create_retention_table(&fixture, table_name, retention_seconds, ctx.runtime_env()).await?;

    let now_us = chrono::Utc::now().timestamp_micros();

    // File 1: mixed — one fresh row, one expired row in the same batch/file.
    insert_rows(
        &table,
        &[
            (1, now_us),              // fresh
            (2, now_us - 10_000_000), // 10s ago — expired
        ],
    )
    .await?;

    // File 2: fully expired.
    insert_row(&table, 3, now_us - 10_000_000).await?; // 10s ago

    let dir = table_id_dir(&fixture, &table, table_name);
    assert_eq!(
        count_vortex_files(&dir),
        2,
        "Expected 2 Vortex files after 2 inserts"
    );

    // Execute file-based delete
    let deleted = execute_delete(&table, retention_delete_filter(retention_seconds)).await?;
    assert_eq!(deleted, 1, "Should delete 1 row (file 2 only)");

    // File 1 must still exist — it has max(event_time) = now, above the cutoff.
    assert_eq!(
        count_vortex_files(&dir),
        1,
        "Mixed file must be preserved; only the fully-expired file is deleted"
    );

    // Scan-time retention filter hides the expired row (id=2) inside the mixed file,
    // so only the fresh row (id=1) is visible.
    assert_table_contents(
        &ctx,
        &table,
        table_name,
        &[1],
        "Only fresh row visible after deletion",
    )
    .await?;

    Ok(())
}

/// The append publish path makes newly committed files visible to the next scan
/// via a **targeted, incremental** update of the list-files cache: it
/// delta-applies the new file's metadata onto the cached snapshot-directory
/// listing rather than evicting it, and leaves unrelated cache entries untouched.
///
/// Steps:
/// 1. Create a table, insert a row, then query to populate the list-files cache.
/// 2. Capture the table's specific cache key and verify it lists exactly 1 file.
/// 3. Add a cache entry for a separate, unrelated table path.
/// 4. Insert another row → the append publish delta-applies the new file onto
///    the table's cached listing (incremental update, not eviction).
/// 5. Assert: the table's cache entry survives and now lists 2 files.
/// 6. Assert: the unrelated table's cache entry is still present (targeted).
/// 7. Query still returns correct data (the freshly added file is visible).
async fn test_cache_delta_applied_after_append_impl(fixture: TestFixture) -> TestResult {
    let retention_seconds = 60;
    let table_name = "cache_inv_append";
    let ctx = SessionContext::new();
    let runtime_env = ctx.runtime_env();
    let table = create_retention_table(
        &fixture,
        table_name,
        retention_seconds,
        Arc::clone(&runtime_env),
    )
    .await?;

    // 1. Insert first row
    let now_us = chrono::Utc::now().timestamp_micros();
    insert_row(&table, 1, now_us).await?;

    // 2. Query to populate the list-files cache (scan → list_with_cache → cache miss → populate)
    assert_table_contents(&ctx, &table, table_name, &[1], "After first insert").await?;

    // 3. Capture the table's specific cache key from the populated cache
    let cache = runtime_env
        .cache_manager
        .get_list_files_cache()
        .expect("list files cache should be enabled");
    let entries = cache.list_entries();
    assert_eq!(
        entries.len(),
        1,
        "Expected exactly one cache entry after first query"
    );
    let table_cache_key = entries
        .keys()
        .next()
        .expect("cache should have one entry")
        .clone();
    let cached_files_before = cache
        .get(&table_cache_key)
        .expect("table's cache entry should exist after query");
    assert_eq!(
        cached_files_before.len(),
        1,
        "Cache should list exactly the 1 file written by the first insert"
    );

    // 4. Add a cache entry for a separate, unrelated table path
    let other_table_key = TableScopedPath {
        table: None,
        path: object_store::path::Path::from("unrelated/table/path"),
    };
    cache.put(&other_table_key, dummy_cache_value());
    assert_eq!(
        cache.len(),
        2,
        "Expected exactly two cache entries after adding unrelated entry"
    );
    assert!(
        cache.contains_key(&other_table_key),
        "Unrelated table entry should be in cache"
    );

    // 5. Insert another row → the append publish delta-applies the new file's
    //    metadata onto the table's cached listing (incremental update).
    insert_row(&table, 2, now_us - 1_000_000).await?;

    // 6. The table's cache entry survives and now lists both files — the new file
    //    was delta-applied onto the existing listing, not evicted.
    let cached_files_after = cache
        .get(&table_cache_key)
        .expect("table's cache entry must survive an append (delta-applied, not evicted)");
    assert_eq!(
        cached_files_after.len(),
        2,
        "Append must delta-apply the new file onto the cached listing (2 files now visible)"
    );

    // 7. Unrelated entry is untouched (targeted update, not a blanket cache.clear())
    assert!(
        cache.contains_key(&other_table_key),
        "Unrelated table entry must survive the targeted cache update"
    );
    assert_eq!(
        cache.len(),
        2,
        "Both the table's (updated) entry and the unrelated entry must remain"
    );

    // 8. Query returns correct data — proves the delta-applied file is visible.
    assert_table_contents(&ctx, &table, table_name, &[1, 2], "After second insert").await?;

    Ok(())
}

/// File-based retention `delete_from` invalidates only snapshot URLs
/// where files were actually deleted, preserving unrelated cache entries.
///
/// Steps:
/// 1. Create a table with retention, insert 3 rows (1 fresh, 1 within retention, 1 expired).
/// 2. Query to populate the cache, capture the table's specific cache key.
/// 3. Add a cache entry for a separate, unrelated table path.
/// 4. Execute file-based retention delete (physically removes the expired file).
/// 5. Assert: the table's cache key is gone (targeted invalidation after delete).
/// 6. Assert: the unrelated table entry survives (not cleared by `cache.clear()`).
/// 7. Query returns correct 2 rows — proves fresh listing was used after invalidation.
/// 8. Verify the query repopulated the table's cache entry (both entries present).
/// 9. Execute a no-op delete (all remaining files within retention, 0 rows deleted).
/// 10. Assert: both cache entries survive — no invalidation when nothing is deleted.
async fn test_file_based_retention_targeted_cache_invalidation_impl(
    fixture: TestFixture,
) -> TestResult {
    let retention_seconds = 3;
    let table_name = "cache_inv_delete";
    let ctx = SessionContext::new();
    let runtime_env = ctx.runtime_env();
    let table = create_retention_table(
        &fixture,
        table_name,
        retention_seconds,
        Arc::clone(&runtime_env),
    )
    .await?;

    // Insert 3 rows as separate files
    let now_us = chrono::Utc::now().timestamp_micros();
    insert_row(&table, 1, now_us).await?; // fresh
    insert_row(&table, 2, now_us - 2_000_000).await?; // 2s ago — within retention
    insert_row(&table, 3, now_us - 10_000_000).await?; // 10s ago — expired

    // Query to populate the list-files cache.
    // Retention filter applied at read time → returns only 2 non-expired rows.
    assert_table_contents(&ctx, &table, table_name, &[1, 2], "Before delete").await?;

    // Capture the table's specific cache key
    let cache = runtime_env
        .cache_manager
        .get_list_files_cache()
        .expect("list files cache should be enabled");
    let entries = cache.list_entries();
    assert_eq!(
        entries.len(),
        1,
        "Expected exactly one cache entry after query"
    );
    let table_cache_key = entries
        .keys()
        .next()
        .expect("cache should have one entry")
        .clone();
    assert!(
        cache.contains_key(&table_cache_key),
        "Table's cache entry should exist after query"
    );

    // Add a cache entry for a separate, unrelated table path
    let other_table_key = TableScopedPath {
        table: None,
        path: object_store::path::Path::from("other/table/snapshot"),
    };
    cache.put(&other_table_key, dummy_cache_value());
    assert!(
        cache.contains_key(&other_table_key),
        "Unrelated table entry must be in cache before delete"
    );
    assert_eq!(cache.len(), 2, "Expected 2 cache entries before delete");

    // Verify 3 vortex files on disk (including the expired one)
    let dir = table_id_dir(&fixture, &table, table_name);
    assert_eq!(
        count_vortex_files(&dir),
        3,
        "Expected 3 Vortex files before file-based delete"
    );

    // Execute file-based retention delete — physically removes the expired file
    let deleted = execute_delete(&table, retention_delete_filter(retention_seconds)).await?;
    assert_eq!(deleted, 1, "Should delete 1 expired row");

    // Verify the expired file was physically removed
    assert_eq!(
        count_vortex_files(&dir),
        2,
        "Expected 2 Vortex files after deletion"
    );

    // Verify targeted invalidation:
    // a) The table's specific cache entry was invalidated by delete_from
    assert!(
        !cache.contains_key(&table_cache_key),
        "Table's cache entry must be invalidated after file-based delete"
    );

    // b) The unrelated table entry must still exist — not cleared by a blanket cache.clear()
    assert!(
        cache.contains_key(&other_table_key),
        "Unrelated table entry must survive targeted cache invalidation after file-based delete"
    );
    assert_eq!(
        cache.len(),
        1,
        "Only the unrelated entry should remain in cache"
    );

    // c) Query returns correct data — fresh listing was used (stale entry was evicted)
    assert_table_contents(
        &ctx,
        &table,
        table_name,
        &[1, 2],
        "After file-based retention delete with cache invalidation",
    )
    .await?;

    // d) The query in (c) repopulated the table's cache entry; verify both entries are back.
    assert!(
        cache.contains_key(&table_cache_key),
        "Table's cache entry should be repopulated after query"
    );

    assert_eq!(
        cache.len(),
        2,
        "Expected 2 cache entries after query repopulated the table's entry"
    );

    // e) No-op delete: all remaining files are within retention, so nothing is removed.
    //    Cache entries must survive because no files were actually deleted.
    let deleted = execute_delete(&table, retention_delete_filter(retention_seconds)).await?;
    assert_eq!(
        deleted, 0,
        "No rows should be deleted — all files are within retention"
    );
    assert!(
        cache.contains_key(&table_cache_key),
        "Table's cache entry must survive a no-op delete"
    );
    assert!(
        cache.contains_key(&other_table_key),
        "Unrelated table entry must survive a no-op delete"
    );

    Ok(())
}

// =============================================================================
// PK-Based File Retention Tests
// =============================================================================

/// Test: File-based retention works for Int64 PK tables without upserts.
///
/// This is the simplest PK scenario — identical to position-based behavior
/// but using the `Int64Pk` deletion strategy. No protected snapshots exist
/// because no upserts have been performed.
///
/// Setup (3-second retention, Int64 PK, `on_conflict`: None):
///   - file 1: `event_time` = now           → fresh (kept)
///   - file 2: `event_time` = now - 2s      → within retention (kept)
///   - file 3: `event_time` = now - 10s     → expired (deleted)
///
/// After deletion: 2 files remain, count(*) = 2, ids = [1, 2].
async fn test_pk_file_based_retention_main_table_only_impl(fixture: TestFixture) -> TestResult {
    let retention_seconds = 3;
    let table_name = "pk_file_ret_main";
    let ctx = SessionContext::new();
    let table = create_pk_retention_table(
        &fixture,
        table_name,
        retention_seconds,
        false,
        ctx.runtime_env(),
    )
    .await?;

    let now_us = chrono::Utc::now().timestamp_micros();
    insert_row(&table, 1, now_us).await?; // fresh
    insert_row(&table, 2, now_us - 2_000_000).await?; // 2s ago — within retention
    insert_row(&table, 3, now_us - 10_000_000).await?; // 10s ago — expired

    let dir = table_id_dir(&fixture, &table, table_name);
    assert_eq!(
        count_vortex_files(&dir),
        3,
        "Expected 3 Vortex files after 3 inserts"
    );

    let deleted = execute_delete(&table, retention_delete_filter(retention_seconds)).await?;
    assert_eq!(deleted, 1, "Should delete 1 row (the expired file)");

    assert_eq!(
        count_vortex_files(&dir),
        2,
        "Expected 2 Vortex files after deletion"
    );

    assert_table_contents(
        &ctx,
        &table,
        table_name,
        &[1, 2],
        "After deleting expired file",
    )
    .await?;

    Ok(())
}

// =============================================================================
// Helper Functions
// =============================================================================

/// Standard schema used by all tests: `(id: Int64, event_time: Timestamp(us, UTC))`.
fn retention_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
    ]))
}

/// Create a position-based (no PK) table with time-based retention.
async fn create_retention_table(
    fixture: &TestFixture,
    table_name: &str,
    retention_seconds: u64,
    runtime_env: Arc<RuntimeEnv>,
) -> Result<Arc<CayenneTableProvider>, Box<dyn std::error::Error>> {
    let table_dir = fixture.data_path.join(table_name);
    std::fs::create_dir_all(&table_dir)?;

    let schema = retention_schema();

    // Single-row inserts must materialize as physical Vortex files because
    // these tests assert on per-file retention semantics
    // (`count_vortex_files`, file-based delete cache, etc.). Disable
    // write-entry inlining so each `insert_batch` produces a real file.
    let vortex_config = cayenne::metadata::VortexConfig {
        inline_max_rows: 0,
        ..cayenne::metadata::VortexConfig::default()
    };

    let table_options = CreateTableOptions {
        table_name: table_name.to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec![], // No PK → position-based → file-based deletes preferred
        on_conflict: None,
        base_path: table_dir.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config,
    };

    let retention_builder =
        TimeRetentionFilterBuilder::try_new("event_time", retention_seconds, &schema)
            .expect("to create retention builder");

    let catalog_arc = Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    Ok(Arc::new(
        CayenneTableProviderBuilder::new(catalog_arc, runtime_env)
            .with_time_retention_filter_builder(retention_builder)
            .create(table_options)
            .await?,
    ))
}

/// Create an Int64 PK table with time-based retention.
///
/// When `with_upsert` is true, configures `on_conflict: Upsert` so that
/// subsequent inserts create protected snapshots.
async fn create_pk_retention_table(
    fixture: &TestFixture,
    table_name: &str,
    retention_seconds: u64,
    with_upsert: bool,
    runtime_env: Arc<RuntimeEnv>,
) -> Result<Arc<CayenneTableProvider>, Box<dyn std::error::Error>> {
    let table_dir = fixture.data_path.join(table_name);
    std::fs::create_dir_all(&table_dir)?;

    let schema = retention_schema();

    let on_conflict = if with_upsert {
        Some(OnConflict::Upsert(ColumnReference::new(vec![
            "id".to_string(),
        ])))
    } else {
        None
    };

    let vortex_config = cayenne::metadata::VortexConfig {
        inline_max_rows: 0,
        ..cayenne::metadata::VortexConfig::default()
    };

    let table_options = CreateTableOptions {
        table_name: table_name.to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["id".to_string()],
        on_conflict,
        base_path: table_dir.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config,
    };

    let retention_builder =
        TimeRetentionFilterBuilder::try_new("event_time", retention_seconds, &schema)
            .expect("to create retention builder");

    let catalog_arc = Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    Ok(Arc::new(
        CayenneTableProviderBuilder::new(catalog_arc, runtime_env)
            .with_time_retention_filter_builder(retention_builder)
            .create(table_options)
            .await?,
    ))
}

/// Insert multiple rows as a single batch → single Vortex file.
async fn insert_rows(
    table: &CayenneTableProvider,
    rows: &[(i64, i64)],
) -> Result<(), Box<dyn std::error::Error>> {
    let (ids, timestamps): (Vec<_>, Vec<_>) = rows.iter().copied().unzip();
    let batch = RecordBatch::try_new(
        retention_schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(TimestampMicrosecondArray::from(timestamps).with_timezone("UTC")),
        ],
    )?;
    let expected = u64::try_from(rows.len()).expect("len fits u64");
    let inserted = common::insert_batch(table, batch).await?;
    assert_eq!(inserted, expected, "Should insert {expected} rows");
    Ok(())
}

/// Insert a single row `(id, event_time)` as its own batch → own Vortex file.
async fn insert_row(
    table: &CayenneTableProvider,
    id: i64,
    event_time_us: i64,
) -> Result<(), Box<dyn std::error::Error>> {
    let batch = RecordBatch::try_new(
        retention_schema(),
        vec![
            Arc::new(Int64Array::from(vec![id])),
            Arc::new(TimestampMicrosecondArray::from(vec![event_time_us]).with_timezone("UTC")),
        ],
    )?;
    let inserted = common::insert_batch(table, batch).await?;
    assert_eq!(inserted, 1, "Should insert 1 row for id={id}");
    Ok(())
}

/// Build a delete filter: `event_time < now() - retention_seconds`.
///
/// This mirrors what the runtime's retention check task produces.
fn retention_delete_filter(retention_seconds: u64) -> Expr {
    let cutoff_us = chrono::Utc::now().timestamp_micros()
        - i64::try_from(retention_seconds).expect("retention seconds fits i64") * 1_000_000;
    col("event_time").lt(lit(ScalarValue::TimestampMicrosecond(
        Some(cutoff_us),
        Some("UTC".into()),
    )))
}

/// Execute `delete_from` on the table and return the reported deleted-row count.
async fn execute_delete(
    table: &CayenneTableProvider,
    filter: Expr,
) -> Result<u64, Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    let plan = table.delete_from(&ctx.state(), vec![filter]).await?;
    let results = datafusion::physical_plan::collect(plan, ctx.task_ctx()).await?;
    Ok(results
        .first()
        .and_then(|b| b.column(0).as_any().downcast_ref::<UInt64Array>())
        .and_then(|a| a.values().first().copied())
        .unwrap_or(0))
}

/// Query `SELECT count(*) FROM <table>` and return the count.
async fn query_count(
    ctx: &SessionContext,
    table: &Arc<CayenneTableProvider>,
    table_name: &str,
) -> Result<i64, Box<dyn std::error::Error>> {
    let _ = ctx.register_table(table_name, Arc::clone(table) as Arc<dyn TableProvider>);
    let df = ctx
        .sql(&format!("SELECT count(*) AS cnt FROM {table_name}"))
        .await?;
    let batches = df.collect().await?;
    Ok(batches
        .first()
        .and_then(|b| b.column(0).as_any().downcast_ref::<Int64Array>())
        .and_then(|a| a.values().first().copied())
        .unwrap_or(0))
}

/// Query ids and count, assert both match expectations.
async fn assert_table_contents(
    ctx: &SessionContext,
    table: &Arc<CayenneTableProvider>,
    table_name: &str,
    expected_ids: &[i64],
    msg: &str,
) -> TestResult {
    let count = query_count(ctx, table, table_name).await?;
    assert_eq!(
        count,
        i64::try_from(expected_ids.len()).expect("len fits i64"),
        "{msg}: count(*) mismatch"
    );

    let _ = ctx.register_table(table_name, Arc::clone(table) as Arc<dyn TableProvider>);
    let df = ctx
        .sql(&format!("SELECT id FROM {table_name} ORDER BY id"))
        .await?;
    let batches = df.collect().await?;
    let mut ids = Vec::new();
    for batch in &batches {
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id column");
        for i in 0..col.len() {
            ids.push(col.value(i));
        }
    }
    assert_eq!(ids, expected_ids, "{msg}: id mismatch");
    Ok(())
}

/// Count `.vortex` data files under the table's snapshot directory.
///
/// Directory structure: `[data_path]/[table_id]/[snapshot_id]/`
fn count_vortex_files(table_dir: &std::path::Path) -> usize {
    let Ok(entries) = std::fs::read_dir(table_dir) else {
        return 0;
    };
    let mut count = 0;
    for entry in entries.filter_map(std::result::Result::ok) {
        let path = entry.path();
        if path.is_dir()
            && path.file_name().is_none_or(|n| n != STAGING_DIR_NAME)
            && let Ok(snapshot_entries) = std::fs::read_dir(&path)
        {
            for file_entry in snapshot_entries.filter_map(std::result::Result::ok) {
                if file_entry
                    .path()
                    .extension()
                    .is_some_and(|ext| ext == "vortex")
                {
                    count += 1;
                }
            }
        }
    }
    count
}

/// Resolve the on-disk directory containing snapshot data for a table.
fn table_id_dir(
    fixture: &TestFixture,
    table: &CayenneTableProvider,
    table_name: &str,
) -> std::path::PathBuf {
    let meta = table.metadata();
    fixture.data_path.join(table_name).join(&meta.table_id)
}

/// Creates a dummy non-empty cache value. The cache rejects empty vecs internally.
fn dummy_cache_value() -> CachedFileList {
    CachedFileList::new(vec![ObjectMeta {
        location: object_store::path::Path::from("dummy/file.parquet"),
        last_modified: chrono::Utc::now(),
        size: 42,
        e_tag: None,
        version: None,
    }])
}
