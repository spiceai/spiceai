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

//! Critical integration tests for deletion vectors.
//!
//! These tests verify:
//! 1. Key-based deletion works for tables WITH primary keys
//! 2. Position-based deletion works for tables WITHOUT primary keys  
//! 3. Deletion vectors persist correctly across table reopens
//! 4. Projections that exclude PK columns still filter deleted rows
//! 5. Deletion works correctly with composite primary keys

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

/// Helper to insert data using a record batch stream
async fn insert_batch(
    table_provider: &Arc<CayenneTableProvider>,
    batch: RecordBatch,
) -> TestResult<u64> {
    let schema = batch.schema();
    let stream = futures::stream::once(async { Ok(batch) });
    let boxed_stream: datafusion_execution::SendableRecordBatchStream =
        Box::pin(datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(schema, stream));

    table_provider
        .insert(boxed_stream)
        .await
        .map_err(Into::into)
}

/// Helper to delete records matching a filter
async fn delete_records(
    table_provider: &Arc<CayenneTableProvider>,
    filter: Expr,
) -> TestResult<u64> {
    let ctx = SessionContext::new();
    let plan = table_provider.delete_from(&ctx.state(), &[filter]).await?;

    let results = datafusion_physical_plan::collect(plan, ctx.task_ctx()).await?;

    Ok(results
        .first()
        .and_then(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::UInt64Array>()
        })
        .and_then(|array| array.values().first())
        .copied()
        .unwrap_or(0))
}

/// Helper to get row count from a table
async fn get_row_count(ctx: &SessionContext, table_name: &str) -> TestResult<i64> {
    let df = ctx
        .sql(&format!("SELECT COUNT(*) as count FROM {table_name}"))
        .await?;
    let results = df.collect().await?;
    Ok(results
        .first()
        .and_then(|batch| batch.column(0).as_any().downcast_ref::<Int64Array>())
        .and_then(|array| array.values().first())
        .copied()
        .unwrap_or(0))
}

// ============================================================================
// Test 1: Key-based deletion with projection excluding PK columns
// ============================================================================

/// Tests that deletion works when querying only non-PK columns.
/// This is critical because the key-based deletion filter needs PK columns
/// but the user query might not request them.
#[tokio::test]
async fn test_deletion_with_projection_excluding_pk() -> TestResult<()> {
    let data_dir = TempDir::new()?;
    let metadata_dir = TempDir::new()?;

    let catalog = Arc::new(CayenneCatalog::new(format!(
        "sqlite://{}/test.db",
        metadata_dir.path().display()
    ))?);
    catalog.init().await?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let table_options = CreateTableOptions {
        table_name: "pk_projection_test".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["id".to_string()],
        on_conflict: None,
        base_path: data_dir.path().to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    let table = Arc::new(CayenneTableProvider::create_table(catalog, table_options).await?);

    let ctx = SessionContext::new();
    ctx.register_table(
        "pk_projection_test",
        Arc::clone(&table) as Arc<dyn TableProvider>,
    )?;

    // Insert data
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec![
                "Alice", "Bob", "Charlie", "David", "Eve",
            ])),
            Arc::new(Int64Array::from(vec![100, 200, 300, 400, 500])),
        ],
    )?;
    insert_batch(&table, batch).await?;

    // Delete id=3
    delete_records(&table, col("id").eq(lit(3i64))).await?;

    // Query ONLY non-PK columns (name, value) - PK column 'id' is excluded
    // This tests that the projection extension logic works correctly
    let df = ctx
        .sql("SELECT name, value FROM pk_projection_test ORDER BY value")
        .await?;
    let results = df.collect().await?;

    // Collect all names to verify Charlie (id=3) is filtered out
    let names: Vec<String> = results
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("name column")
                .iter()
                .filter_map(|s| s.map(String::from))
        })
        .collect();

    assert_eq!(names.len(), 4, "Should have 4 rows after deletion");
    assert!(
        !names.contains(&"Charlie".to_string()),
        "Charlie should be filtered out after deletion, but got: {names:?}"
    );
    assert_eq!(
        names,
        vec!["Alice", "Bob", "David", "Eve"],
        "Names should match expected order"
    );

    Ok(())
}

// ============================================================================
// Test 2: Deletion vectors persist across table reopens
// ============================================================================

/// Tests that deletion vectors are correctly persisted and work after
/// closing and reopening the table.
#[tokio::test]
async fn test_deletion_persists_after_reopen() -> TestResult<()> {
    let data_dir = TempDir::new()?;
    let metadata_dir = TempDir::new()?;
    let db_path = format!("sqlite://{}/test.db", metadata_dir.path().display());

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    // Phase 1: Create table, insert data, delete some rows
    {
        let catalog = Arc::new(CayenneCatalog::new(&db_path)?);
        catalog.init().await?;

        let table_options = CreateTableOptions {
            table_name: "persist_test".to_string(),
            schema: Arc::clone(&schema),
            primary_key: vec!["id".to_string()],
            on_conflict: None,
            base_path: data_dir.path().to_string_lossy().to_string(),
            partition_column: None,
            vortex_config: cayenne::metadata::VortexConfig::default(),
        };

        let table = Arc::new(CayenneTableProvider::create_table(catalog, table_options).await?);

        // Insert data
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(StringArray::from(vec![
                    "Alice", "Bob", "Charlie", "David", "Eve",
                ])),
            ],
        )?;
        insert_batch(&table, batch).await?;

        // Delete id=2 and id=4
        delete_records(&table, col("id").eq(lit(2i64)).or(col("id").eq(lit(4i64)))).await?;

        let ctx = SessionContext::new();
        ctx.register_table("persist_test", Arc::clone(&table) as Arc<dyn TableProvider>)?;

        // Verify deletions work before close
        let count = get_row_count(&ctx, "persist_test").await?;
        assert_eq!(count, 3, "Should have 3 rows before close");
    }
    // Table is dropped here

    // Phase 2: Reopen table and verify deletions still work
    {
        let catalog = Arc::new(CayenneCatalog::new(&db_path)?);
        catalog.init().await?;

        // Reopen the table using builder pattern
        let table = Arc::new(
            CayenneTableProviderBuilder::new(catalog)
                .open("persist_test")
                .await?,
        );

        let ctx = SessionContext::new();
        ctx.register_table("persist_test", Arc::clone(&table) as Arc<dyn TableProvider>)?;

        // Verify row count
        let count = get_row_count(&ctx, "persist_test").await?;
        assert_eq!(
            count, 3,
            "Should still have 3 rows after reopening table (got {count})"
        );

        // Verify specific deleted rows are gone
        let df = ctx
            .sql("SELECT id FROM persist_test WHERE id IN (2, 4)")
            .await?;
        let results = df.collect().await?;
        let matching_rows: usize = results.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(
            matching_rows, 0,
            "Rows with id=2 and id=4 should not be visible"
        );

        // Verify remaining rows
        let df = ctx.sql("SELECT id FROM persist_test ORDER BY id").await?;
        let results = df.collect().await?;
        let ids: Vec<i64> = results
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("id column")
                    .values()
                    .iter()
                    .copied()
            })
            .collect();
        assert_eq!(ids, vec![1, 3, 5], "Remaining ids should be [1, 3, 5]");
    }

    Ok(())
}

// ============================================================================
// Test 3: Position-based deletion for tables WITHOUT primary key
// ============================================================================

/// Tests that position-based deletion works correctly for tables without PK.
#[tokio::test]
async fn test_position_based_deletion_no_pk() -> TestResult<()> {
    let data_dir = TempDir::new()?;
    let metadata_dir = TempDir::new()?;

    let catalog = Arc::new(CayenneCatalog::new(format!(
        "sqlite://{}/test.db",
        metadata_dir.path().display()
    ))?);
    catalog.init().await?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("category", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let table_options = CreateTableOptions {
        table_name: "no_pk_test".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec![],
        on_conflict: None, // NO primary key
        base_path: data_dir.path().to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    let table = Arc::new(CayenneTableProvider::create_table(catalog, table_options).await?);

    let ctx = SessionContext::new();
    ctx.register_table("no_pk_test", Arc::clone(&table) as Arc<dyn TableProvider>)?;

    // Insert data with duplicates (valid since no PK)
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["A", "B", "A", "C", "B"])),
            Arc::new(Int64Array::from(vec![100, 200, 300, 400, 500])),
        ],
    )?;
    insert_batch(&table, batch).await?;

    // Delete all rows with category='A'
    let deleted = delete_records(&table, col("category").eq(lit("A"))).await?;
    assert_eq!(deleted, 2, "Should delete 2 rows with category='A'");

    // Verify count
    let count = get_row_count(&ctx, "no_pk_test").await?;
    assert_eq!(count, 3, "Should have 3 rows after deletion");

    // Verify no 'A' categories remain
    let df = ctx
        .sql("SELECT category FROM no_pk_test WHERE category = 'A'")
        .await?;
    let results = df.collect().await?;
    let matching_rows: usize = results.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(matching_rows, 0, "No rows with category='A' should remain");

    Ok(())
}

// ============================================================================
// Test 4: Composite primary key deletion
// ============================================================================

/// Tests key-based deletion with a composite (multi-column) primary key.
#[tokio::test]
async fn test_composite_primary_key_deletion() -> TestResult<()> {
    let data_dir = TempDir::new()?;
    let metadata_dir = TempDir::new()?;

    let catalog = Arc::new(CayenneCatalog::new(format!(
        "sqlite://{}/test.db",
        metadata_dir.path().display()
    ))?);
    catalog.init().await?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("region", DataType::Utf8, false),
        Field::new("year", DataType::Int64, false),
        Field::new("revenue", DataType::Int64, false),
    ]));

    let table_options = CreateTableOptions {
        table_name: "composite_pk_test".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["region".to_string(), "year".to_string()],
        on_conflict: None, // Composite PK
        base_path: data_dir.path().to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    let table = Arc::new(CayenneTableProvider::create_table(catalog, table_options).await?);

    let ctx = SessionContext::new();
    ctx.register_table(
        "composite_pk_test",
        Arc::clone(&table) as Arc<dyn TableProvider>,
    )?;

    // Insert data
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["US", "US", "EU", "EU", "APAC"])),
            Arc::new(Int64Array::from(vec![2023, 2024, 2023, 2024, 2024])),
            Arc::new(Int64Array::from(vec![1000, 1100, 800, 850, 600])),
        ],
    )?;
    insert_batch(&table, batch).await?;

    // Delete specific composite key: (US, 2023)
    let deleted = delete_records(
        &table,
        col("region")
            .eq(lit("US"))
            .and(col("year").eq(lit(2023i64))),
    )
    .await?;
    assert_eq!(deleted, 1, "Should delete 1 row (US, 2023)");

    // Verify count
    let count = get_row_count(&ctx, "composite_pk_test").await?;
    assert_eq!(count, 4, "Should have 4 rows after deletion");

    // Verify deleted row is gone
    let df = ctx
        .sql("SELECT region, year FROM composite_pk_test WHERE region = 'US' AND year = 2023")
        .await?;
    let results = df.collect().await?;
    let matching_rows: usize = results.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(matching_rows, 0, "(US, 2023) should not be visible");

    // Verify (US, 2024) still exists
    let df = ctx
        .sql("SELECT revenue FROM composite_pk_test WHERE region = 'US' AND year = 2024")
        .await?;
    let results = df.collect().await?;
    let revenue: i64 = results
        .first()
        .and_then(|batch| batch.column(0).as_any().downcast_ref::<Int64Array>())
        .and_then(|array| array.values().first())
        .copied()
        .unwrap_or(0);
    assert_eq!(revenue, 1100, "(US, 2024) should still have revenue 1100");

    Ok(())
}

// ============================================================================
// Test 5: Delete then insert different key
// ============================================================================

/// Tests that deleting a row doesn't affect inserting rows with different keys.
///
/// NOTE: Inserting a row with the SAME key as a deleted row is a known limitation
/// of the current key-based deletion approach. The deletion vector will filter out
/// both the old and new row until compaction removes the stale deletion entry.
/// This test verifies that different keys are unaffected.
#[tokio::test]
async fn test_delete_then_insert_different_key() -> TestResult<()> {
    let data_dir = TempDir::new()?;
    let metadata_dir = TempDir::new()?;

    let catalog = Arc::new(CayenneCatalog::new(format!(
        "sqlite://{}/test.db",
        metadata_dir.path().display()
    ))?);
    catalog.init().await?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("version", DataType::Int64, false),
    ]));

    let table_options = CreateTableOptions {
        table_name: "delete_insert_test".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["id".to_string()],
        on_conflict: None,
        base_path: data_dir.path().to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    let table = Arc::new(CayenneTableProvider::create_table(catalog, table_options).await?);

    let ctx = SessionContext::new();
    ctx.register_table(
        "delete_insert_test",
        Arc::clone(&table) as Arc<dyn TableProvider>,
    )?;

    // Insert initial data
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
            Arc::new(Int64Array::from(vec![1, 1, 1])),
        ],
    )?;
    insert_batch(&table, batch).await?;

    // Delete id=2
    delete_records(&table, col("id").eq(lit(2i64))).await?;

    // Insert a NEW row with DIFFERENT id (id=4)
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![4])),
            Arc::new(StringArray::from(vec!["David"])),
            Arc::new(Int64Array::from(vec![1])),
        ],
    )?;
    insert_batch(&table, batch).await?;

    // Query for id=4 - should exist since it's a different key
    let df = ctx
        .sql("SELECT name FROM delete_insert_test WHERE id = 4")
        .await?;
    let results = df.collect().await?;

    let row_count: usize = results.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(row_count, 1, "Should have exactly 1 row for id=4");

    let name = results
        .first()
        .and_then(|batch| batch.column(0).as_any().downcast_ref::<StringArray>())
        .and_then(|array| array.value(0).into())
        .unwrap_or("");
    assert_eq!(name, "David", "Should see the new row David");

    // Total count should be 3 (1 + 3 - 1 = 3: Alice, Charlie, David)
    let count = get_row_count(&ctx, "delete_insert_test").await?;
    assert_eq!(count, 3, "Should have 3 total rows");

    // Verify deleted row is still gone
    let df = ctx
        .sql("SELECT id FROM delete_insert_test WHERE id = 2")
        .await?;
    let results = df.collect().await?;
    let matching_rows: usize = results.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(matching_rows, 0, "Row with id=2 should still be deleted");

    Ok(())
}

// ============================================================================
// Test 6: Multiple deletes accumulate correctly
// ============================================================================

/// Tests that multiple delete operations accumulate correctly.
#[tokio::test]
async fn test_multiple_delete_operations() -> TestResult<()> {
    let data_dir = TempDir::new()?;
    let metadata_dir = TempDir::new()?;

    let catalog = Arc::new(CayenneCatalog::new(format!(
        "sqlite://{}/test.db",
        metadata_dir.path().display()
    ))?);
    catalog.init().await?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let table_options = CreateTableOptions {
        table_name: "multi_delete_test".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["id".to_string()],
        on_conflict: None,
        base_path: data_dir.path().to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    let table = Arc::new(CayenneTableProvider::create_table(catalog, table_options).await?);

    let ctx = SessionContext::new();
    ctx.register_table(
        "multi_delete_test",
        Arc::clone(&table) as Arc<dyn TableProvider>,
    )?;

    // Insert 10 rows
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10])),
            Arc::new(Int64Array::from(vec![
                100, 200, 300, 400, 500, 600, 700, 800, 900, 1000,
            ])),
        ],
    )?;
    insert_batch(&table, batch).await?;

    // Delete in multiple operations
    delete_records(&table, col("id").eq(lit(1i64))).await?;
    delete_records(&table, col("id").eq(lit(3i64))).await?;
    delete_records(&table, col("id").eq(lit(5i64))).await?;
    delete_records(&table, col("id").eq(lit(7i64))).await?;
    delete_records(&table, col("id").eq(lit(9i64))).await?;

    // Should have 5 rows remaining (even ids)
    let count = get_row_count(&ctx, "multi_delete_test").await?;
    assert_eq!(count, 5, "Should have 5 rows after 5 deletions");

    // Verify remaining ids are even
    let df = ctx
        .sql("SELECT id FROM multi_delete_test ORDER BY id")
        .await?;
    let results = df.collect().await?;
    let ids: Vec<i64> = results
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("id column")
                .values()
                .iter()
                .copied()
        })
        .collect();
    assert_eq!(ids, vec![2, 4, 6, 8, 10], "Only even ids should remain");

    Ok(())
}

// ============================================================================
// Test 7: Large-scale deletion test
// ============================================================================

/// Tests deletion with a larger dataset to verify performance doesn't degrade.
#[tokio::test]
async fn test_large_scale_deletion() -> TestResult<()> {
    const TOTAL_ROWS: i64 = 1000;
    const BATCH_SIZE: i64 = 100;

    let data_dir = TempDir::new()?;
    let metadata_dir = TempDir::new()?;

    let catalog = Arc::new(CayenneCatalog::new(format!(
        "sqlite://{}/test.db",
        metadata_dir.path().display()
    ))?);
    catalog.init().await?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("category", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let table_options = CreateTableOptions {
        table_name: "large_scale_test".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["id".to_string()],
        on_conflict: None,
        base_path: data_dir.path().to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    let table = Arc::new(CayenneTableProvider::create_table(catalog, table_options).await?);

    let ctx = SessionContext::new();
    ctx.register_table(
        "large_scale_test",
        Arc::clone(&table) as Arc<dyn TableProvider>,
    )?;

    // Insert 1000 rows in batches
    for batch_num in 0..(TOTAL_ROWS / BATCH_SIZE) {
        let start_id = batch_num * BATCH_SIZE + 1;
        let ids: Vec<i64> = (start_id..(start_id + BATCH_SIZE)).collect();
        let categories: Vec<i64> = ids.iter().map(|id| id % 10).collect(); // 10 categories
        let values: Vec<i64> = ids.iter().map(|id| id * 100).collect();

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(Int64Array::from(categories)),
                Arc::new(Int64Array::from(values)),
            ],
        )?;
        insert_batch(&table, batch).await?;
    }

    // Verify initial count
    let count = get_row_count(&ctx, "large_scale_test").await?;
    assert_eq!(count, TOTAL_ROWS, "Should have {TOTAL_ROWS} rows initially");

    // Delete all rows in category 0 (100 rows: 10, 20, 30, ..., 1000)
    let deleted = delete_records(&table, col("category").eq(lit(0i64))).await?;
    assert_eq!(deleted, 100, "Should delete 100 rows in category 0");

    // Verify count after deletion
    let count = get_row_count(&ctx, "large_scale_test").await?;
    assert_eq!(
        count,
        TOTAL_ROWS - 100,
        "Should have {} rows after deleting category 0",
        TOTAL_ROWS - 100
    );

    // Verify no category 0 rows remain
    let df = ctx
        .sql("SELECT COUNT(*) as cnt FROM large_scale_test WHERE category = 0")
        .await?;
    let results = df.collect().await?;
    let cat0_count = results
        .first()
        .and_then(|batch| batch.column(0).as_any().downcast_ref::<Int64Array>())
        .and_then(|array| array.values().first())
        .copied()
        .unwrap_or(-1);
    assert_eq!(cat0_count, 0, "No category 0 rows should remain");

    Ok(())
}
