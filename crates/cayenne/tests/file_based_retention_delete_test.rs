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
//! **time-based retention**, the [`DeletionTableProvider::delete_from`] path
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
    CayenneTableProvider, CayenneTableProviderBuilder, MetadataCatalog, TimeRetentionFilterBuilder,
};
use common::TestFixture;
use data_components::delete::DeletionTableProvider;
use datafusion::datasource::TableProvider;
use datafusion::prelude::*;
use datafusion_common::ScalarValue;
use std::sync::Arc;

type TestResult = Result<(), Box<dyn std::error::Error>>;

test_with_backends!(test_file_based_retention_deletes_expired_files_impl);
test_with_backends!(test_file_based_retention_no_eligible_files_impl);
test_with_backends!(test_file_based_retention_deletes_all_files_impl);
test_with_backends!(test_file_based_retention_mixed_file_not_deleted_impl);

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
    let table = create_retention_table(&fixture, table_name, retention_seconds).await?;

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
    assert_table_contents(&table, table_name, &[1, 2], "After deleting expired file").await?;

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
    let table = create_retention_table(&fixture, table_name, retention_seconds).await?;

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

    assert_table_contents(&table, table_name, &[1, 2], "No rows should be removed").await?;

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
    let table = create_retention_table(&fixture, table_name, retention_seconds).await?;

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

    assert_table_contents(&table, table_name, &[], "No rows should remain").await?;

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
    let table = create_retention_table(&fixture, table_name, retention_seconds).await?;

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
        &table,
        table_name,
        &[1],
        "Only fresh row visible after deletion",
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
) -> Result<Arc<CayenneTableProvider>, Box<dyn std::error::Error>> {
    let table_dir = fixture.data_path.join(table_name);
    std::fs::create_dir_all(&table_dir)?;

    let schema = retention_schema();

    let table_options = CreateTableOptions {
        table_name: table_name.to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec![], // No PK → position-based → file-based deletes preferred
        on_conflict: None,
        base_path: table_dir.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    let retention_builder =
        TimeRetentionFilterBuilder::try_new("event_time", retention_seconds, &schema)
            .expect("to create retention builder");

    let catalog_arc = Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    Ok(Arc::new(
        CayenneTableProviderBuilder::new(catalog_arc)
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
    let plan = table.delete_from(&ctx.state(), &[filter]).await?;
    let results = datafusion::physical_plan::collect(plan, ctx.task_ctx()).await?;
    Ok(results
        .first()
        .and_then(|b| b.column(0).as_any().downcast_ref::<UInt64Array>())
        .and_then(|a| a.values().first().copied())
        .unwrap_or(0))
}

/// Query `SELECT count(*) FROM <table>` and return the count.
async fn query_count(
    table: &Arc<CayenneTableProvider>,
    table_name: &str,
) -> Result<i64, Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    ctx.register_table(table_name, Arc::clone(table) as Arc<dyn TableProvider>)?;
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
    table: &Arc<CayenneTableProvider>,
    table_name: &str,
    expected_ids: &[i64],
    msg: &str,
) -> TestResult {
    let count = query_count(table, table_name).await?;
    assert_eq!(
        count,
        i64::try_from(expected_ids.len()).expect("len fits i64"),
        "{msg}: count(*) mismatch"
    );

    let ctx = SessionContext::new();
    ctx.register_table(table_name, Arc::clone(table) as Arc<dyn TableProvider>)?;
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
        if path.is_dir() {
            if let Ok(snapshot_entries) = std::fs::read_dir(&path) {
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
    }
    count
}

/// Resolve the on-disk directory containing snapshot data for a table.
fn table_id_dir(
    fixture: &TestFixture,
    table: &CayenneTableProvider,
    table_name: &str,
) -> std::path::PathBuf {
    fixture
        .data_path
        .join(table_name)
        .join(table.metadata().table_id.to_string())
}
