/*
Copyright 2025 The Spice.ai OSS Authors

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

//! Comprehensive edge case tests for position-based deletion vectors.
//!
//! Position-based deletion is the most fragile deletion strategy because it relies on
//! consistent row ordering across scans and deletions. These tests validate:
//!
//! 1. **Empty deletion set**: Deleting with no matches
//! 2. **All rows deleted**: Full table deletion and subsequent inserts
//! 3. **Idempotent deletion**: Deleting already-deleted rows
//! 4. **Sequential deletes**: Multiple delete operations in sequence
//! 5. **Projection with deletions**: Querying subset of columns
//! 6. **Multi-file scenarios**: Deleting from different Parquet files
//! 7. **Persistence after full delete**: Table reopening with all rows deleted
//! 8. **Large batch with interleaved deletes**: Stress testing row ID tracking
//!
//! ## Merge-Insert Compaction
//!
//! When inserting new data into a table with pending position-based deletions,
//! Cayenne performs a "merge-insert with compaction":
//! 1. Reads existing data with the deletion filter applied (removing deleted rows)
//! 2. Unions with the new data
//! 3. Writes everything to a new snapshot
//! 4. Clears the deletion vectors
//!
//! This prevents row ID collisions that would otherwise occur when new files
//! are added after deletions.

#![allow(clippy::expect_used)]

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use cayenne::{
    metadata::CreateTableOptions, CayenneCatalog, CayenneTableProvider,
    CayenneTableProviderBuilder, MetadataCatalog,
};
use data_components::delete::DeletionTableProvider;
use datafusion::datasource::TableProvider;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::*;
use std::sync::Arc;
use tempfile::TempDir;

type TestResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

// =============================================================================
// Helper Functions
// =============================================================================

async fn insert_batch(table: &Arc<CayenneTableProvider>, batch: RecordBatch) -> TestResult<u64> {
    let schema = batch.schema();
    let stream = futures::stream::once(async { Ok(batch) });
    let boxed_stream: datafusion_execution::SendableRecordBatchStream =
        Box::pin(datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(schema, stream));
    table.insert(boxed_stream).await.map_err(Into::into)
}

async fn delete_records(table: &Arc<CayenneTableProvider>, filter: Expr) -> TestResult<u64> {
    let ctx = SessionContext::new();
    let plan = table.delete_from(&ctx.state(), &[filter]).await?;
    let results = datafusion_physical_plan::collect(plan, ctx.task_ctx()).await?;
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

async fn get_row_count(ctx: &SessionContext, table_name: &str) -> TestResult<i64> {
    let df = ctx
        .sql(&format!("SELECT COUNT(*) as count FROM {table_name}"))
        .await?;
    let results = df.collect().await?;
    Ok(results
        .first()
        .and_then(|b| b.column(0).as_any().downcast_ref::<Int64Array>())
        .and_then(|a| a.values().first())
        .copied()
        .unwrap_or(0))
}

fn create_no_pk_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("category", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
        Field::new("label", DataType::Utf8, false),
    ]))
}

async fn setup_no_pk_table(
    data_dir: &TempDir,
    metadata_dir: &TempDir,
    table_name: &str,
) -> TestResult<(Arc<CayenneTableProvider>, SessionContext, Arc<Schema>)> {
    let catalog = Arc::new(CayenneCatalog::new(format!(
        "sqlite://{}/test.db",
        metadata_dir.path().display()
    ))?);
    catalog.init().await?;

    let schema = create_no_pk_schema();

    let table_options = CreateTableOptions {
        table_name: table_name.to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec![], // No primary key - position-based deletion
        base_path: data_dir.path().to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    let table = Arc::new(CayenneTableProvider::create_table(catalog, table_options).await?);
    let ctx = SessionContext::new();
    ctx.register_table(table_name, Arc::clone(&table) as Arc<dyn TableProvider>)?;

    Ok((table, ctx, schema))
}

// =============================================================================
// Edge Case 1: Empty deletion set (no matching rows)
// =============================================================================

/// Test: Delete with filter that matches no rows.
/// Should return 0 deleted and leave table unchanged.
#[tokio::test]
async fn test_position_based_delete_no_matches() -> TestResult<()> {
    let data_dir = TempDir::new()?;
    let metadata_dir = TempDir::new()?;

    let (table, ctx, schema) = setup_no_pk_table(&data_dir, &metadata_dir, "no_match_test").await?;

    // Insert data
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["A", "B", "C"])),
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["x", "y", "z"])),
        ],
    )?;
    insert_batch(&table, batch).await?;

    assert_eq!(get_row_count(&ctx, "no_match_test").await?, 3);

    // Delete with non-matching filter
    let deleted = delete_records(&table, col("category").eq(lit("X"))).await?;
    assert_eq!(deleted, 0, "Should delete 0 rows with non-matching filter");

    // Verify all rows still present
    assert_eq!(
        get_row_count(&ctx, "no_match_test").await?,
        3,
        "Should still have 3 rows"
    );

    Ok(())
}

// =============================================================================
// Edge Case 2: Delete all rows
// =============================================================================

/// Test: Delete all rows from a table.
/// Subsequent queries should return 0 rows.
#[tokio::test]
async fn test_position_based_delete_all_rows() -> TestResult<()> {
    let data_dir = TempDir::new()?;
    let metadata_dir = TempDir::new()?;

    let (table, ctx, schema) =
        setup_no_pk_table(&data_dir, &metadata_dir, "delete_all_test").await?;

    // Insert data
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["A", "A", "A", "A"])),
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            Arc::new(StringArray::from(vec!["w", "x", "y", "z"])),
        ],
    )?;
    insert_batch(&table, batch).await?;

    assert_eq!(get_row_count(&ctx, "delete_all_test").await?, 4);

    // Delete all rows
    let deleted = delete_records(&table, col("category").eq(lit("A"))).await?;
    assert_eq!(deleted, 4, "Should delete all 4 rows");

    // Verify no rows remain
    assert_eq!(
        get_row_count(&ctx, "delete_all_test").await?,
        0,
        "Should have 0 rows after deleting all"
    );

    // Verify SELECT returns empty
    let df = ctx.sql("SELECT * FROM delete_all_test").await?;
    let results = df.collect().await?;
    let total_rows: usize = results.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 0, "SELECT should return no rows");

    Ok(())
}

/// Test: Insert new data after deleting all rows.
///
/// **KNOWN LIMITATION**: Position-based deletion vectors store global row IDs that are
/// computed based on file ordering during the deletion scan. When new files are inserted,
/// the row IDs for the new data may overlap with the deleted row IDs from the old data,
/// causing the wrong rows to be filtered out.
///
/// This test documents this limitation. The proper fix requires either:
/// 1. Using file-scoped row IDs: `(data_file_id, position_within_file)`
/// 2. Using the `row_id_start` field from the catalog to compute global row IDs
/// 3. Compacting the table after deletions before inserting new data
/// This test verifies that inserting new data after a delete-all operation works correctly.
///
/// With the merge-insert compaction fix, when inserting into a table with pending
/// position-based deletions, the table is first compacted (deletions applied),
/// then new data is appended to a fresh snapshot. This prevents row ID collisions.
#[tokio::test]
async fn test_position_based_insert_after_delete_all() -> TestResult<()> {
    let data_dir = TempDir::new()?;
    let metadata_dir = TempDir::new()?;

    let (table, ctx, schema) =
        setup_no_pk_table(&data_dir, &metadata_dir, "insert_after_delete").await?;

    // Insert initial data
    let batch1 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["A", "A"])),
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["old1", "old2"])),
        ],
    )?;
    insert_batch(&table, batch1).await?;

    // Delete all
    let deleted = delete_records(&table, col("category").eq(lit("A"))).await?;
    assert_eq!(deleted, 2);
    assert_eq!(get_row_count(&ctx, "insert_after_delete").await?, 0);

    // Insert new data
    let batch2 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["B", "B", "B"])),
            Arc::new(Int64Array::from(vec![10, 20, 30])),
            Arc::new(StringArray::from(vec!["new1", "new2", "new3"])),
        ],
    )?;
    insert_batch(&table, batch2).await?;

    // New data should be visible
    assert_eq!(
        get_row_count(&ctx, "insert_after_delete").await?,
        3,
        "Should have 3 new rows"
    );

    // Verify the new labels
    let df = ctx
        .sql("SELECT label FROM insert_after_delete ORDER BY value")
        .await?;
    let results = df.collect().await?;
    let labels: Vec<String> = results
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("label column")
                .iter()
                .filter_map(|s| s.map(String::from))
        })
        .collect();
    assert_eq!(labels, vec!["new1", "new2", "new3"]);

    Ok(())
}

// =============================================================================
// Edge Case 3: Idempotent deletion (delete already-deleted rows)
// =============================================================================

/// Test: Attempting to delete already-deleted rows.
/// Should be idempotent and return 0 deleted.
#[tokio::test]
async fn test_position_based_idempotent_delete() -> TestResult<()> {
    let data_dir = TempDir::new()?;
    let metadata_dir = TempDir::new()?;

    let (table, ctx, schema) =
        setup_no_pk_table(&data_dir, &metadata_dir, "idempotent_test").await?;

    // Insert data
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["A", "B", "A", "B"])),
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            Arc::new(StringArray::from(vec!["a1", "b1", "a2", "b2"])),
        ],
    )?;
    insert_batch(&table, batch).await?;

    // First delete
    let deleted1 = delete_records(&table, col("category").eq(lit("A"))).await?;
    assert_eq!(deleted1, 2, "First delete should remove 2 rows");
    assert_eq!(get_row_count(&ctx, "idempotent_test").await?, 2);

    // Second delete with same filter - should be idempotent
    let deleted2 = delete_records(&table, col("category").eq(lit("A"))).await?;
    assert_eq!(
        deleted2, 0,
        "Second delete should return 0 (already deleted)"
    );
    assert_eq!(
        get_row_count(&ctx, "idempotent_test").await?,
        2,
        "Still should have 2 rows"
    );

    // Verify correct rows remain
    let df = ctx
        .sql("SELECT category FROM idempotent_test ORDER BY value")
        .await?;
    let results = df.collect().await?;
    let categories: Vec<String> = results
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("category column")
                .iter()
                .filter_map(|s| s.map(String::from))
        })
        .collect();
    assert_eq!(categories, vec!["B", "B"]);

    Ok(())
}

// =============================================================================
// Edge Case 4: Sequential deletes
// =============================================================================

/// Test: Multiple delete operations in sequence.
/// Each delete should correctly track cumulative deletions.
#[tokio::test]
async fn test_position_based_sequential_deletes() -> TestResult<()> {
    let data_dir = TempDir::new()?;
    let metadata_dir = TempDir::new()?;

    let (table, ctx, schema) =
        setup_no_pk_table(&data_dir, &metadata_dir, "sequential_test").await?;

    // Insert data with 3 categories
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["A", "B", "C", "A", "B", "C"])),
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6])),
            Arc::new(StringArray::from(vec!["a1", "b1", "c1", "a2", "b2", "c2"])),
        ],
    )?;
    insert_batch(&table, batch).await?;

    assert_eq!(get_row_count(&ctx, "sequential_test").await?, 6);

    // Delete A's (2 rows)
    let deleted_a = delete_records(&table, col("category").eq(lit("A"))).await?;
    assert_eq!(deleted_a, 2);
    assert_eq!(get_row_count(&ctx, "sequential_test").await?, 4);

    // Delete B's (2 rows)
    let deleted_b = delete_records(&table, col("category").eq(lit("B"))).await?;
    assert_eq!(deleted_b, 2);
    assert_eq!(get_row_count(&ctx, "sequential_test").await?, 2);

    // Delete C's (2 rows)
    let deleted_c = delete_records(&table, col("category").eq(lit("C"))).await?;
    assert_eq!(deleted_c, 2);
    assert_eq!(get_row_count(&ctx, "sequential_test").await?, 0);

    Ok(())
}

// =============================================================================
// Edge Case 5: Projection with deletions
// =============================================================================

/// Test: Query with projection (subset of columns) after deletion.
/// Deletion should work correctly even when not all columns are queried.
#[tokio::test]
async fn test_position_based_projection_after_delete() -> TestResult<()> {
    let data_dir = TempDir::new()?;
    let metadata_dir = TempDir::new()?;

    let (table, ctx, schema) =
        setup_no_pk_table(&data_dir, &metadata_dir, "projection_test").await?;

    // Insert data
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["A", "B", "A", "B"])),
            Arc::new(Int64Array::from(vec![10, 20, 30, 40])),
            Arc::new(StringArray::from(vec!["w", "x", "y", "z"])),
        ],
    )?;
    insert_batch(&table, batch).await?;

    // Delete A rows
    delete_records(&table, col("category").eq(lit("A"))).await?;

    // Query only 'value' column (not 'category' used in filter)
    let df = ctx
        .sql("SELECT value FROM projection_test ORDER BY value")
        .await?;
    let results = df.collect().await?;
    let values: Vec<i64> = results
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("value column")
                .values()
                .iter()
                .copied()
        })
        .collect();
    assert_eq!(values, vec![20, 40], "Only B values should remain");

    // Query only 'label' column
    let df = ctx
        .sql("SELECT label FROM projection_test ORDER BY label")
        .await?;
    let results = df.collect().await?;
    let labels: Vec<String> = results
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("label column")
                .iter()
                .filter_map(|s| s.map(String::from))
        })
        .collect();
    assert_eq!(labels, vec!["x", "z"], "Only B labels should remain");

    Ok(())
}

// =============================================================================
// Edge Case 6: Multi-file deletions
// =============================================================================

/// Test: Deleting rows that span multiple Parquet files.
/// Each insert creates a new file; deletion must track positions across files.
#[tokio::test]
async fn test_position_based_multi_file_deletion() -> TestResult<()> {
    let data_dir = TempDir::new()?;
    let metadata_dir = TempDir::new()?;

    let (table, ctx, schema) =
        setup_no_pk_table(&data_dir, &metadata_dir, "multi_file_test").await?;

    // Insert 3 separate batches (3 files)
    for file_num in 0..3i64 {
        let offset = file_num * 3;
        let labels: Vec<String> = vec![
            format!("f{file_num}_a"),
            format!("f{file_num}_b"),
            format!("f{file_num}_c"),
        ];
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["X", "Y", "Z"])),
                Arc::new(Int64Array::from(vec![offset + 1, offset + 2, offset + 3])),
                Arc::new(StringArray::from(
                    labels.iter().map(String::as_str).collect::<Vec<_>>(),
                )),
            ],
        )?;
        insert_batch(&table, batch).await?;
    }

    assert_eq!(get_row_count(&ctx, "multi_file_test").await?, 9);

    // Delete all 'Y' rows (1 per file = 3 total, values: 2, 5, 8)
    let deleted = delete_records(&table, col("category").eq(lit("Y"))).await?;
    assert_eq!(deleted, 3, "Should delete 3 'Y' rows across 3 files");

    assert_eq!(get_row_count(&ctx, "multi_file_test").await?, 6);

    // Verify remaining values
    let df = ctx
        .sql("SELECT value FROM multi_file_test ORDER BY value")
        .await?;
    let results = df.collect().await?;
    let values: Vec<i64> = results
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("value column")
                .values()
                .iter()
                .copied()
        })
        .collect();
    // Should have 1, 3, 4, 6, 7, 9 (missing 2, 5, 8)
    assert_eq!(values, vec![1, 3, 4, 6, 7, 9]);

    Ok(())
}

// =============================================================================
// Edge Case 7: Persistence after full delete
// =============================================================================

/// Test: Table persistence after deleting all rows.
/// Reopening the table should show 0 rows.
#[tokio::test]
async fn test_position_based_persistence_after_full_delete() -> TestResult<()> {
    let data_dir = TempDir::new()?;
    let metadata_dir = TempDir::new()?;
    let db_path = format!("sqlite://{}/test.db", metadata_dir.path().display());

    let schema = create_no_pk_schema();

    // Phase 1: Insert and delete all
    {
        let catalog = Arc::new(CayenneCatalog::new(&db_path)?);
        catalog.init().await?;

        let table_options = CreateTableOptions {
            table_name: "full_delete_persist".to_string(),
            schema: Arc::clone(&schema),
            primary_key: vec![],
            base_path: data_dir.path().to_string_lossy().to_string(),
            partition_column: None,
            vortex_config: cayenne::metadata::VortexConfig::default(),
        };

        let table = Arc::new(CayenneTableProvider::create_table(catalog, table_options).await?);
        let ctx = SessionContext::new();
        ctx.register_table(
            "full_delete_persist",
            Arc::clone(&table) as Arc<dyn TableProvider>,
        )?;

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["X", "X", "X"])),
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )?;
        insert_batch(&table, batch).await?;

        // Delete all
        let deleted = delete_records(&table, col("category").eq(lit("X"))).await?;
        assert_eq!(deleted, 3);
        assert_eq!(get_row_count(&ctx, "full_delete_persist").await?, 0);
    }

    // Phase 2: Reopen and verify 0 rows
    {
        let catalog = Arc::new(CayenneCatalog::new(&db_path)?);
        catalog.init().await?;

        let table = Arc::new(
            CayenneTableProviderBuilder::new(Arc::clone(&catalog) as Arc<dyn MetadataCatalog>)
                .open("full_delete_persist")
                .await?,
        );

        let ctx = SessionContext::new();
        ctx.register_table(
            "full_delete_persist",
            Arc::clone(&table) as Arc<dyn TableProvider>,
        )?;

        assert_eq!(
            get_row_count(&ctx, "full_delete_persist").await?,
            0,
            "Should still have 0 rows after reopening"
        );
    }

    Ok(())
}

// =============================================================================
// Edge Case 8: Large batch with interleaved deletes
// =============================================================================

/// Test: Large dataset with multiple interleaved inserts and deletes.
/// Stress tests the row ID tracking mechanism.
#[tokio::test]
async fn test_position_based_stress_interleaved() -> TestResult<()> {
    let data_dir = TempDir::new()?;
    let metadata_dir = TempDir::new()?;

    let (table, ctx, schema) = setup_no_pk_table(&data_dir, &metadata_dir, "stress_test").await?;

    // Insert batch 1: 100 rows with categories A, B, C, D cycling
    let categories1: Vec<&str> = (0..100)
        .map(|i| match i % 4 {
            0 => "A",
            1 => "B",
            2 => "C",
            _ => "D",
        })
        .collect();
    let batch1 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(categories1)),
            Arc::new(Int64Array::from((1..=100).collect::<Vec<_>>())),
            Arc::new(StringArray::from(
                (1..=100).map(|i| format!("label_{i}")).collect::<Vec<_>>(),
            )),
        ],
    )?;
    insert_batch(&table, batch1).await?;

    assert_eq!(get_row_count(&ctx, "stress_test").await?, 100);

    // Delete all A's (25 rows)
    let deleted_a = delete_records(&table, col("category").eq(lit("A"))).await?;
    assert_eq!(deleted_a, 25);
    assert_eq!(get_row_count(&ctx, "stress_test").await?, 75);

    // Insert batch 2: 50 more rows
    let categories2: Vec<&str> = (0..50)
        .map(|i| match i % 4 {
            0 => "A",
            1 => "B",
            2 => "C",
            _ => "D",
        })
        .collect();
    let batch2 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(categories2)),
            Arc::new(Int64Array::from((101..=150).collect::<Vec<_>>())),
            Arc::new(StringArray::from(
                (101..=150)
                    .map(|i| format!("label_{i}"))
                    .collect::<Vec<_>>(),
            )),
        ],
    )?;
    insert_batch(&table, batch2).await?;

    // Now have 75 (original - A's) + 50 = 125 rows
    assert_eq!(get_row_count(&ctx, "stress_test").await?, 125);

    // Delete all B's (25 from batch1 + ~13 from batch2 = ~38)
    // Batch1 has 25 B's (at positions 1, 5, 9, 13... every 4th starting from 1)
    // Batch2 has 12-13 B's
    let deleted_b = delete_records(&table, col("category").eq(lit("B"))).await?;
    // Batch1: 100/4 = 25 B's, Batch2: 50/4 = 12-13 B's
    // After compaction, all B's should be found and deleted
    assert!(
        (36..=38).contains(&deleted_b),
        "Should delete ~37 B rows, got {deleted_b}"
    );

    // Verify count makes sense
    let remaining = get_row_count(&ctx, "stress_test").await?;
    assert_eq!(
        remaining,
        125 - i64::try_from(deleted_b).expect("deleted_b fits in i64")
    );

    // Verify no A's or B's remain
    // Note: A's from batch2 DO remain because:
    // 1. We deleted A's from batch1
    // 2. When inserting batch2, merge_insert_with_compaction was triggered
    // 3. This compacted the table (applied A deletions), then added batch2 (including new A's)
    // 4. We then deleted B's (from both batches)
    // So A's from batch2 + C's and D's from both batches remain
    let df = ctx
        .sql("SELECT DISTINCT category FROM stress_test ORDER BY category")
        .await?;
    let results = df.collect().await?;
    let categories: Vec<String> = results
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("category")
                .iter()
                .filter_map(|s| s.map(String::from))
        })
        .collect();
    assert_eq!(
        categories,
        vec!["A", "C", "D"],
        "A's from batch2 plus C's and D's from both batches should remain"
    );

    Ok(())
}

// =============================================================================
// Edge Case 9: Empty table delete
// =============================================================================

/// Test: Deleting from an empty table.
/// Should return 0 and not error.
#[tokio::test]
async fn test_position_based_delete_from_empty() -> TestResult<()> {
    let data_dir = TempDir::new()?;
    let metadata_dir = TempDir::new()?;

    let (table, ctx, _schema) = setup_no_pk_table(&data_dir, &metadata_dir, "empty_table").await?;

    // Table is empty
    assert_eq!(get_row_count(&ctx, "empty_table").await?, 0);

    // Try to delete from empty table
    let deleted = delete_records(&table, col("category").eq(lit("A"))).await?;
    assert_eq!(deleted, 0, "Deleting from empty table should return 0");

    // Table should still be empty
    assert_eq!(get_row_count(&ctx, "empty_table").await?, 0);

    Ok(())
}

// =============================================================================
// Edge Case 10: Complex filter conditions
// =============================================================================

/// Test: Delete with complex filter (AND, OR conditions).
#[tokio::test]
async fn test_position_based_complex_filter() -> TestResult<()> {
    let data_dir = TempDir::new()?;
    let metadata_dir = TempDir::new()?;

    let (table, ctx, schema) =
        setup_no_pk_table(&data_dir, &metadata_dir, "complex_filter").await?;

    // Insert data
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["A", "B", "A", "B", "A", "B"])),
            Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50, 60])),
            Arc::new(StringArray::from(vec!["x", "x", "y", "y", "z", "z"])),
        ],
    )?;
    insert_batch(&table, batch).await?;

    assert_eq!(get_row_count(&ctx, "complex_filter").await?, 6);

    // Delete: (category = 'A' AND value > 20) OR (label = 'z')
    // Matches: A/30/y (value>20), A/50/z (value>20 + z), B/60/z (z)
    let filter = col("category")
        .eq(lit("A"))
        .and(col("value").gt(lit(20i64)))
        .or(col("label").eq(lit("z")));
    let deleted = delete_records(&table, filter).await?;
    assert_eq!(deleted, 3, "Should delete 3 rows matching complex filter");

    assert_eq!(get_row_count(&ctx, "complex_filter").await?, 3);

    // Verify remaining rows
    let df = ctx
        .sql("SELECT value FROM complex_filter ORDER BY value")
        .await?;
    let results = df.collect().await?;
    let values: Vec<i64> = results
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("value")
                .values()
                .iter()
                .copied()
        })
        .collect();
    assert_eq!(values, vec![10, 20, 40], "A/10, B/20, B/40 should remain");

    Ok(())
}

// =============================================================================
// Edge Case 11: Partial batch deletion
// =============================================================================

/// Test: Delete that affects only part of a single batch.
/// Ensures row IDs are correctly tracked within a single file.
#[tokio::test]
async fn test_position_based_partial_batch() -> TestResult<()> {
    let data_dir = TempDir::new()?;
    let metadata_dir = TempDir::new()?;

    let (table, ctx, schema) = setup_no_pk_table(&data_dir, &metadata_dir, "partial_batch").await?;

    // Insert single batch with 10 rows
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec![
                "A", "B", "C", "A", "B", "C", "A", "B", "C", "A",
            ])),
            Arc::new(Int64Array::from((1..=10).collect::<Vec<_>>())),
            Arc::new(StringArray::from(
                (1..=10).map(|i| format!("L{i}")).collect::<Vec<_>>(),
            )),
        ],
    )?;
    insert_batch(&table, batch).await?;

    // Delete just the middle rows (values 4, 5, 6)
    let filter = col("value").gt(lit(3i64)).and(col("value").lt(lit(7i64)));
    let deleted = delete_records(&table, filter).await?;
    assert_eq!(deleted, 3, "Should delete 3 middle rows");

    assert_eq!(get_row_count(&ctx, "partial_batch").await?, 7);

    // Verify the gap
    let df = ctx
        .sql("SELECT value FROM partial_batch ORDER BY value")
        .await?;
    let results = df.collect().await?;
    let values: Vec<i64> = results
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("value")
                .values()
                .iter()
                .copied()
        })
        .collect();
    assert_eq!(
        values,
        vec![1, 2, 3, 7, 8, 9, 10],
        "Values 4,5,6 should be deleted"
    );

    Ok(())
}

// =============================================================================
// Edge Case 12: Delete first and last rows
// =============================================================================

/// Test: Delete first and last rows specifically.
/// Tests boundary conditions in the row ID tracking.
#[tokio::test]
async fn test_position_based_boundary_rows() -> TestResult<()> {
    let data_dir = TempDir::new()?;
    let metadata_dir = TempDir::new()?;

    let (table, ctx, schema) = setup_no_pk_table(&data_dir, &metadata_dir, "boundary_test").await?;

    // Insert 5 rows
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec![
                "FIRST", "MID", "MID", "MID", "LAST",
            ])),
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])),
        ],
    )?;
    insert_batch(&table, batch).await?;

    // Delete first row
    let deleted1 = delete_records(&table, col("category").eq(lit("FIRST"))).await?;
    assert_eq!(deleted1, 1);

    // Delete last row
    let deleted2 = delete_records(&table, col("category").eq(lit("LAST"))).await?;
    assert_eq!(deleted2, 1);

    // Should have 3 rows remaining
    assert_eq!(get_row_count(&ctx, "boundary_test").await?, 3);

    // Verify middle rows remain
    let df = ctx
        .sql("SELECT value FROM boundary_test ORDER BY value")
        .await?;
    let results = df.collect().await?;
    let values: Vec<i64> = results
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("value")
                .values()
                .iter()
                .copied()
        })
        .collect();
    assert_eq!(values, vec![2, 3, 4], "Only middle values should remain");

    Ok(())
}
