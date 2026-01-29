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

//! Comprehensive edge case tests for Int64 primary key deletion strategy.
//!
//! Int64 PK deletion uses optimized `HashSet<i64>` lookup for efficient deletion
//! tracking. These tests validate:
//!
//! 1. **Basic operations**: Insert, delete, query
//! 2. **Empty deletion set**: Deleting with no matches
//! 3. **All rows deleted**: Full table deletion and subsequent inserts
//! 4. **Idempotent deletion**: Deleting already-deleted rows
//! 5. **Sequential deletes**: Multiple delete operations in sequence
//! 6. **Projection without PK**: Querying only non-PK columns
//! 7. **Multi-file scenarios**: Deleting from different data files
//! 8. **Persistence after delete**: Table reopening with pending deletions
//! 9. **Boundary values**: Int64 min/max values
//! 10. **Large scale stress test**: Many rows with interleaved operations

#![allow(clippy::expect_used)]

mod common;

use arrow::array::{Int64Array, RecordBatch, StringArray};

use arrow::datatypes::{DataType, Field, Schema};

use cayenne::{
    metadata::CreateTableOptions, CayenneTableProvider, CayenneTableProviderBuilder,
    MetadataCatalog,
};

use common::TestFixture;

use data_components::delete::DeletionTableProvider;

use datafusion::datasource::TableProvider;

use datafusion::execution::context::SessionContext;

use datafusion::prelude::*;

use std::sync::Arc;

type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

// =============================================================================
// Helper Functions
// =============================================================================

fn create_int64_pk_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

async fn setup_int64_pk_table(
    fixture: &TestFixture,
    table_name: &str,
) -> TestResult<(Arc<CayenneTableProvider>, SessionContext, Arc<Schema>)> {
    let schema = create_int64_pk_schema();

    let table_options = CreateTableOptions {
        table_name: table_name.to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["id".to_string()],
        on_conflict: None, // Int64 PK strategy
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let table = Arc::new(CayenneTableProvider::create_table(catalog, table_options).await?);
    let ctx = SessionContext::new();
    ctx.register_table(table_name, Arc::clone(&table) as Arc<dyn TableProvider>)?;

    Ok((table, ctx, schema))
}

async fn insert_batch(table: &Arc<CayenneTableProvider>, batch: RecordBatch) -> TestResult<u64> {
    common::insert_batch(table.as_ref(), batch)
        .await
        .map_err(Into::into)
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

async fn get_ids(ctx: &SessionContext, table_name: &str) -> TestResult<Vec<i64>> {
    let df = ctx
        .sql(&format!("SELECT id FROM {table_name} ORDER BY id"))
        .await?;
    let results = df.collect().await?;
    let ids: Vec<i64> = results
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("id column")
                .values()
                .iter()
                .copied()
        })
        .collect();
    Ok(ids)
}

// =============================================================================
// Edge Case 1: Empty deletion set (no matching rows)
// =============================================================================

async fn test_int64_pk_delete_no_matches_impl(fixture: TestFixture) -> TestResult<()> {
    let (table, ctx, schema) = setup_int64_pk_table(&fixture, "no_match_test").await?;

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["A", "B", "C"])),
            Arc::new(Int64Array::from(vec![100, 200, 300])),
        ],
    )?;
    insert_batch(&table, batch).await?;

    assert_eq!(get_row_count(&ctx, "no_match_test").await?, 3);

    // Delete with non-matching id
    let deleted = delete_records(&table, col("id").eq(lit(999i64))).await?;
    assert_eq!(deleted, 0, "Should delete 0 rows with non-matching id");

    assert_eq!(get_row_count(&ctx, "no_match_test").await?, 3);

    Ok(())
}

test_with_backends!(test_int64_pk_delete_no_matches_impl);

// =============================================================================
// Edge Case 2: Delete all rows
// =============================================================================

async fn test_int64_pk_delete_all_rows_impl(fixture: TestFixture) -> TestResult<()> {
    let (table, ctx, schema) = setup_int64_pk_table(&fixture, "delete_all_test").await?;

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            Arc::new(StringArray::from(vec!["A", "B", "C", "D"])),
            Arc::new(Int64Array::from(vec![100, 200, 300, 400])),
        ],
    )?;
    insert_batch(&table, batch).await?;

    assert_eq!(get_row_count(&ctx, "delete_all_test").await?, 4);

    // Delete all rows one by one
    for id in 1..=4 {
        delete_records(&table, col("id").eq(lit(id))).await?;
    }

    assert_eq!(get_row_count(&ctx, "delete_all_test").await?, 0);

    // Insert new rows after full deletion
    let new_batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![5, 6])),
            Arc::new(StringArray::from(vec!["E", "F"])),
            Arc::new(Int64Array::from(vec![500, 600])),
        ],
    )?;
    insert_batch(&table, new_batch).await?;

    assert_eq!(get_row_count(&ctx, "delete_all_test").await?, 2);
    assert_eq!(get_ids(&ctx, "delete_all_test").await?, vec![5, 6]);

    Ok(())
}

test_with_backends!(test_int64_pk_delete_all_rows_impl);

// =============================================================================
// Edge Case 3: Idempotent deletion
// =============================================================================

async fn test_int64_pk_idempotent_delete_impl(fixture: TestFixture) -> TestResult<()> {
    let (table, ctx, schema) = setup_int64_pk_table(&fixture, "idempotent_test").await?;

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["A", "B", "C"])),
            Arc::new(Int64Array::from(vec![100, 200, 300])),
        ],
    )?;
    insert_batch(&table, batch).await?;

    // Delete id=2
    let deleted1 = delete_records(&table, col("id").eq(lit(2i64))).await?;
    assert_eq!(deleted1, 1);

    // Try to delete id=2 again (already deleted)
    let deleted2 = delete_records(&table, col("id").eq(lit(2i64))).await?;
    assert_eq!(deleted2, 0, "Deleting already-deleted row should return 0");

    assert_eq!(get_row_count(&ctx, "idempotent_test").await?, 2);
    assert_eq!(get_ids(&ctx, "idempotent_test").await?, vec![1, 3]);

    Ok(())
}

test_with_backends!(test_int64_pk_idempotent_delete_impl);

// =============================================================================
// Edge Case 4: Sequential deletes
// =============================================================================

async fn test_int64_pk_sequential_deletes_impl(fixture: TestFixture) -> TestResult<()> {
    let (table, ctx, schema) = setup_int64_pk_table(&fixture, "sequential_test").await?;

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from((1..=10).collect::<Vec<_>>())),
            Arc::new(StringArray::from(vec![
                "A", "B", "C", "D", "E", "F", "G", "H", "I", "J",
            ])),
            Arc::new(Int64Array::from(
                (100..=1000).step_by(100).collect::<Vec<_>>(),
            )),
        ],
    )?;
    insert_batch(&table, batch).await?;

    // Delete in sequence: 3, 5, 7, 9
    for id in [3, 5, 7, 9] {
        let deleted = delete_records(&table, col("id").eq(lit(id))).await?;
        assert_eq!(deleted, 1, "Should delete exactly 1 row");
    }

    assert_eq!(get_row_count(&ctx, "sequential_test").await?, 6);
    assert_eq!(
        get_ids(&ctx, "sequential_test").await?,
        vec![1, 2, 4, 6, 8, 10]
    );

    Ok(())
}

test_with_backends!(test_int64_pk_sequential_deletes_impl);

// =============================================================================
// Edge Case 5: Projection without PK column
// =============================================================================

async fn test_int64_pk_projection_after_delete_impl(fixture: TestFixture) -> TestResult<()> {
    let (table, ctx, schema) = setup_int64_pk_table(&fixture, "projection_test").await?;

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec!["A", "B", "C", "D", "E"])),
            Arc::new(Int64Array::from(vec![100, 200, 300, 400, 500])),
        ],
    )?;
    insert_batch(&table, batch).await?;

    // Delete id=2 and id=4
    delete_records(&table, col("id").eq(lit(2i64))).await?;
    delete_records(&table, col("id").eq(lit(4i64))).await?;

    // Query only non-PK columns - should still filter deleted rows correctly
    let df = ctx
        .sql("SELECT name, value FROM projection_test ORDER BY value")
        .await?;
    let results = df.collect().await?;

    let names: Vec<String> = results
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("name column")
                .iter()
                .filter_map(|s| s.map(String::from))
        })
        .collect();

    assert_eq!(names, vec!["A", "C", "E"]);

    Ok(())
}

test_with_backends!(test_int64_pk_projection_after_delete_impl);

// =============================================================================
// Edge Case 6: Multi-file deletion
// =============================================================================

async fn test_int64_pk_multi_file_deletion_impl(fixture: TestFixture) -> TestResult<()> {
    let (table, ctx, schema) = setup_int64_pk_table(&fixture, "multi_file_test").await?;

    // Insert batch 1
    let batch1 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["A", "B", "C"])),
            Arc::new(Int64Array::from(vec![100, 200, 300])),
        ],
    )?;
    insert_batch(&table, batch1).await?;

    // Insert batch 2 (creates second file)
    let batch2 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![4, 5, 6])),
            Arc::new(StringArray::from(vec!["D", "E", "F"])),
            Arc::new(Int64Array::from(vec![400, 500, 600])),
        ],
    )?;
    insert_batch(&table, batch2).await?;

    assert_eq!(get_row_count(&ctx, "multi_file_test").await?, 6);

    // Delete from both files
    delete_records(&table, col("id").eq(lit(2i64))).await?; // from file 1
    delete_records(&table, col("id").eq(lit(5i64))).await?; // from file 2

    assert_eq!(get_row_count(&ctx, "multi_file_test").await?, 4);
    assert_eq!(get_ids(&ctx, "multi_file_test").await?, vec![1, 3, 4, 6]);

    Ok(())
}

test_with_backends!(test_int64_pk_multi_file_deletion_impl);

// =============================================================================
// Edge Case 7: Boundary values
// =============================================================================

async fn test_int64_pk_boundary_values_impl(fixture: TestFixture) -> TestResult<()> {
    let (table, ctx, schema) = setup_int64_pk_table(&fixture, "boundary_test").await?;

    // Test with boundary Int64 values
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![i64::MIN, -1, 0, 1, i64::MAX])),
            Arc::new(StringArray::from(vec!["min", "neg", "zero", "pos", "max"])),
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
        ],
    )?;
    insert_batch(&table, batch).await?;

    assert_eq!(get_row_count(&ctx, "boundary_test").await?, 5);

    // Delete boundary values
    delete_records(&table, col("id").eq(lit(i64::MIN))).await?;
    delete_records(&table, col("id").eq(lit(i64::MAX))).await?;

    assert_eq!(get_row_count(&ctx, "boundary_test").await?, 3);

    let ids = get_ids(&ctx, "boundary_test").await?;
    assert_eq!(ids, vec![-1, 0, 1]);

    Ok(())
}

test_with_backends!(test_int64_pk_boundary_values_impl);

// =============================================================================
// Edge Case 8: Delete from empty table
// =============================================================================

async fn test_int64_pk_delete_from_empty_impl(fixture: TestFixture) -> TestResult<()> {
    let (table, ctx, _schema) = setup_int64_pk_table(&fixture, "empty_test").await?;

    assert_eq!(get_row_count(&ctx, "empty_test").await?, 0);

    let deleted = delete_records(&table, col("id").eq(lit(1i64))).await?;
    assert_eq!(deleted, 0, "Deleting from empty table should return 0");

    assert_eq!(get_row_count(&ctx, "empty_test").await?, 0);

    Ok(())
}

test_with_backends!(test_int64_pk_delete_from_empty_impl);

// =============================================================================
// Edge Case 9: Persistence after full delete
// =============================================================================

async fn test_int64_pk_persistence_after_full_delete_impl(fixture: TestFixture) -> TestResult<()> {
    let schema = create_int64_pk_schema();
    let table_name = "persistence_test";

    // Create and populate table
    let table_options = CreateTableOptions {
        table_name: table_name.to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["id".to_string()],
        on_conflict: None,
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let table = Arc::new(CayenneTableProvider::create_table(catalog, table_options).await?);

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["A", "B", "C"])),
            Arc::new(Int64Array::from(vec![100, 200, 300])),
        ],
    )?;
    insert_batch(&table, batch).await?;

    // Delete all rows
    delete_records(&table, col("id").eq(lit(1i64))).await?;
    delete_records(&table, col("id").eq(lit(2i64))).await?;
    delete_records(&table, col("id").eq(lit(3i64))).await?;

    // Reopen the table
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let table2 = Arc::new(
        CayenneTableProviderBuilder::new(catalog)
            .open(table_name)
            .await?,
    );

    let ctx = SessionContext::new();
    ctx.register_table(table_name, Arc::clone(&table2) as Arc<dyn TableProvider>)?;

    // Verify deletions persisted
    assert_eq!(get_row_count(&ctx, table_name).await?, 0);

    Ok(())
}

test_with_backends!(test_int64_pk_persistence_after_full_delete_impl);

// =============================================================================
// Edge Case 10: Complex filter conditions
// =============================================================================

async fn test_int64_pk_complex_filter_impl(fixture: TestFixture) -> TestResult<()> {
    let (table, ctx, schema) = setup_int64_pk_table(&fixture, "complex_filter_test").await?;

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from((1..=20).collect::<Vec<_>>())),
            Arc::new(StringArray::from(
                (1..=20)
                    .map(|i| if i % 2 == 0 { "even" } else { "odd" })
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                (1..=20).map(|i| i * 10).collect::<Vec<_>>(),
            )),
        ],
    )?;
    insert_batch(&table, batch).await?;

    assert_eq!(get_row_count(&ctx, "complex_filter_test").await?, 20);

    // Delete with complex filter: id > 5 AND id < 15 AND name = 'even'
    let filter = col("id")
        .gt(lit(5i64))
        .and(col("id").lt(lit(15i64)))
        .and(col("name").eq(lit("even")));
    let deleted = delete_records(&table, filter).await?;

    // Should delete ids 6, 8, 10, 12, 14
    assert_eq!(deleted, 5);
    assert_eq!(get_row_count(&ctx, "complex_filter_test").await?, 15);

    Ok(())
}

test_with_backends!(test_int64_pk_complex_filter_impl);

// =============================================================================
// Edge Case 11: Stress test with interleaved inserts/deletes
// =============================================================================

async fn test_int64_pk_stress_interleaved_impl(fixture: TestFixture) -> TestResult<()> {
    let (table, ctx, schema) = setup_int64_pk_table(&fixture, "stress_test").await?;

    // Insert batch 1: ids 1-100
    let batch1 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from((1..=100).collect::<Vec<_>>())),
            Arc::new(StringArray::from(
                (1..=100).map(|i| format!("name_{i}")).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                (1..=100).map(|i| i * 10).collect::<Vec<_>>(),
            )),
        ],
    )?;
    insert_batch(&table, batch1).await?;

    assert_eq!(get_row_count(&ctx, "stress_test").await?, 100);

    // Delete all even ids (50 rows)
    let deleted_even = delete_records(&table, (col("id") % lit(2i64)).eq(lit(0i64))).await?;
    assert_eq!(deleted_even, 50);
    assert_eq!(get_row_count(&ctx, "stress_test").await?, 50);

    // Insert batch 2: ids 101-150
    let batch2 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from((101..=150).collect::<Vec<_>>())),
            Arc::new(StringArray::from(
                (101..=150).map(|i| format!("name_{i}")).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                (101..=150).map(|i| i * 10).collect::<Vec<_>>(),
            )),
        ],
    )?;
    insert_batch(&table, batch2).await?;

    // Now have 50 odd + 50 new = 100 rows
    assert_eq!(get_row_count(&ctx, "stress_test").await?, 100);

    // Delete all ids > 125
    let deleted_high = delete_records(&table, col("id").gt(lit(125i64))).await?;
    assert_eq!(deleted_high, 25);
    assert_eq!(get_row_count(&ctx, "stress_test").await?, 75);

    Ok(())
}

test_with_backends!(test_int64_pk_stress_interleaved_impl);

// =============================================================================
// Edge Case 12: Partial batch deletion
// =============================================================================

async fn test_int64_pk_partial_batch_impl(fixture: TestFixture) -> TestResult<()> {
    let (table, ctx, schema) = setup_int64_pk_table(&fixture, "partial_batch_test").await?;

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10])),
            Arc::new(StringArray::from(vec![
                "A", "B", "C", "D", "E", "F", "G", "H", "I", "J",
            ])),
            Arc::new(Int64Array::from(
                (100..=1000).step_by(100).collect::<Vec<_>>(),
            )),
        ],
    )?;
    insert_batch(&table, batch).await?;

    // Delete first 3 rows
    delete_records(&table, col("id").lt_eq(lit(3i64))).await?;
    assert_eq!(get_row_count(&ctx, "partial_batch_test").await?, 7);

    // Delete last 3 rows
    delete_records(&table, col("id").gt_eq(lit(8i64))).await?;
    assert_eq!(get_row_count(&ctx, "partial_batch_test").await?, 4);

    let ids = get_ids(&ctx, "partial_batch_test").await?;
    assert_eq!(ids, vec![4, 5, 6, 7]);

    Ok(())
}

test_with_backends!(test_int64_pk_partial_batch_impl);

// =============================================================================
// Edge Case 13: Insert after delete (reusing PKs) - UPSERT BEHAVIOR
// With the compaction-on-upsert fix, inserting a row with a previously deleted PK
// will trigger mini-compaction to apply deletions before adding new data.
// This ensures the new row is visible (proper upsert semantics).
// =============================================================================

async fn test_int64_pk_insert_after_delete_same_pk_impl(fixture: TestFixture) -> TestResult<()> {
    let (table, ctx, schema) = setup_int64_pk_table(&fixture, "reuse_pk_test").await?;

    // Initial insert
    let batch1 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["old1", "old2", "old3"])),
            Arc::new(Int64Array::from(vec![100, 200, 300])),
        ],
    )?;
    insert_batch(&table, batch1).await?;

    // Delete id=2
    delete_records(&table, col("id").eq(lit(2i64))).await?;

    // Insert new row with same PK (id=2 with different data)
    // With the fix, this triggers mini-compaction and the new row IS visible
    let batch2 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![2])),
            Arc::new(StringArray::from(vec!["new2"])),
            Arc::new(Int64Array::from(vec![999])),
        ],
    )?;
    insert_batch(&table, batch2).await?;

    // The new row with pk=2 should be visible after upsert
    assert_eq!(
        get_row_count(&ctx, "reuse_pk_test").await?,
        3,
        "Upserted row with reused PK should be visible after mini-compaction"
    );

    // Insert with a new PK to verify inserts still work
    let batch3 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![4])),
            Arc::new(StringArray::from(vec!["new4"])),
            Arc::new(Int64Array::from(vec![400])),
        ],
    )?;
    insert_batch(&table, batch3).await?;

    assert_eq!(
        get_row_count(&ctx, "reuse_pk_test").await?,
        4,
        "New row with fresh PK should appear"
    );

    // Verify the data: should have ids 1, 2, 3, 4 with 2 being the new version
    let ids = get_ids(&ctx, "reuse_pk_test").await?;
    assert_eq!(
        ids,
        vec![1, 2, 3, 4],
        "Should have all 4 ids including upserted id=2"
    );

    // Verify id=2 has the new value (999)
    let df = ctx
        .sql("SELECT value FROM reuse_pk_test WHERE id = 2")
        .await?;
    let results = df.collect().await?;
    let value = results
        .first()
        .and_then(|b| b.column(0).as_any().downcast_ref::<Int64Array>())
        .and_then(|a| a.values().first())
        .copied();
    assert_eq!(value, Some(999), "Upserted row should have new value");

    Ok(())
}

test_with_backends!(test_int64_pk_insert_after_delete_same_pk_impl);
