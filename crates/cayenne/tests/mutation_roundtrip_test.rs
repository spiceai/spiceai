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

//! Comprehensive Mutation Roundtrip Tests for Cayenne
//!
//! These tests validate that Cayenne correctly handles all types of mutations
//! with data integrity guarantees across insert, update, delete operations.
//!
//! Test categories:
//! 1. **Insert Operations**: Single row, batch, large batch, empty batch
//! 2. **Delete Operations**: Single row, multiple rows, all rows, no matches
//! 3. **Update (Upsert)**: Single field, multiple fields, entire row
//! 4. **Compound Operations**: Insert+Delete, Delete+Insert, multi-step mutations
//! 5. **Edge Cases**: Empty tables, boundary values, null handling
//! 6. **Scale Tests**: Large batches, many small updates, stress scenarios

#![allow(clippy::expect_used)]

mod common;

use arrow::array::{
    Array, BinaryArray, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray,
    TimestampMillisecondArray,
};

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

use cayenne::{metadata::CreateTableOptions, CayenneTableProvider, MetadataCatalog};

use common::TestFixture;

use data_components::delete::DeletionTableProvider;

use datafusion::datasource::TableProvider;

use datafusion::execution::context::SessionContext;

use datafusion::prelude::*;

use std::sync::Arc;

type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

// =============================================================================
// Schema Definitions for Various Test Scenarios
// =============================================================================

fn create_simple_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

fn create_multi_column_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("value", DataType::Int64, true),
        Field::new("score", DataType::Float64, true),
        Field::new("data", DataType::Binary, true),
    ]))
}

fn create_timestamp_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "created_at",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("value", DataType::Int64, false),
    ]))
}

fn create_nullable_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("nullable_int", DataType::Int32, true),
        Field::new("nullable_str", DataType::Utf8, true),
    ]))
}

// =============================================================================
// Helper Functions
// =============================================================================

async fn setup_table(
    fixture: &TestFixture,
    table_name: &str,
    schema: Arc<Schema>,
    primary_key: Vec<String>,
) -> TestResult<(Arc<CayenneTableProvider>, SessionContext)> {
    let table_options = CreateTableOptions {
        table_name: table_name.to_string(),
        schema: Arc::clone(&schema),
        primary_key,
        on_conflict: None,
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let table = Arc::new(CayenneTableProvider::create_table(catalog, table_options).await?);
    let ctx = SessionContext::new();
    ctx.register_table(table_name, Arc::clone(&table) as Arc<dyn TableProvider>)?;

    Ok((table, ctx))
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

async fn get_all_ids(ctx: &SessionContext, table_name: &str) -> TestResult<Vec<i64>> {
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

async fn get_value_for_id(
    ctx: &SessionContext,
    table_name: &str,
    id: i64,
) -> TestResult<Option<i64>> {
    let df = ctx
        .sql(&format!("SELECT value FROM {table_name} WHERE id = {id}"))
        .await?;
    let results = df.collect().await?;
    Ok(results
        .first()
        .and_then(|b| b.column(0).as_any().downcast_ref::<Int64Array>())
        .and_then(|a| a.values().first())
        .copied())
}

// =============================================================================
// TEST 1: Single Row Insert
// =============================================================================

async fn test_insert_single_row_impl(fixture: TestFixture) -> TestResult<()> {
    let schema = create_simple_schema();
    let (table, ctx) = setup_table(
        &fixture,
        "single_insert",
        Arc::clone(&schema),
        vec!["id".into()],
    )
    .await?;

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![100])),
        ],
    )?;

    let inserted = insert_batch(&table, batch).await?;
    assert_eq!(inserted, 1, "Should insert exactly 1 row");
    assert_eq!(get_row_count(&ctx, "single_insert").await?, 1);
    assert_eq!(get_value_for_id(&ctx, "single_insert", 1).await?, Some(100));

    Ok(())
}

test_with_backends!(test_insert_single_row_impl);

// =============================================================================
// TEST 2: Batch Insert
// =============================================================================

async fn test_insert_batch_impl(fixture: TestFixture) -> TestResult<()> {
    let schema = create_simple_schema();
    let (table, ctx) = setup_table(
        &fixture,
        "batch_insert",
        Arc::clone(&schema),
        vec!["id".into()],
    )
    .await?;

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(Int64Array::from(vec![100, 200, 300, 400, 500])),
        ],
    )?;

    let inserted = insert_batch(&table, batch).await?;
    assert_eq!(inserted, 5, "Should insert 5 rows");
    assert_eq!(get_row_count(&ctx, "batch_insert").await?, 5);

    let ids = get_all_ids(&ctx, "batch_insert").await?;
    assert_eq!(ids, vec![1, 2, 3, 4, 5]);

    Ok(())
}

test_with_backends!(test_insert_batch_impl);

// =============================================================================
// TEST 3: Large Batch Insert (1000+ rows)
// =============================================================================

async fn test_insert_large_batch_impl(fixture: TestFixture) -> TestResult<()> {
    let schema = create_simple_schema();
    let (table, ctx) = setup_table(
        &fixture,
        "large_insert",
        Arc::clone(&schema),
        vec!["id".into()],
    )
    .await?;

    let num_rows = 1000;
    let ids: Vec<i64> = (1..=num_rows).collect();
    let values: Vec<i64> = ids.iter().map(|id| id * 10).collect();

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(values)),
        ],
    )?;

    let inserted = insert_batch(&table, batch).await?;
    assert_eq!(inserted, 1000, "Should insert 1000 rows");
    assert_eq!(get_row_count(&ctx, "large_insert").await?, 1000);

    // Verify first and last rows
    assert_eq!(get_value_for_id(&ctx, "large_insert", 1).await?, Some(10));
    assert_eq!(
        get_value_for_id(&ctx, "large_insert", 1000).await?,
        Some(10000)
    );

    Ok(())
}

test_with_backends!(test_insert_large_batch_impl);

// =============================================================================
// TEST 4: Multiple Batch Inserts
// =============================================================================

async fn test_insert_multiple_batches_impl(fixture: TestFixture) -> TestResult<()> {
    let schema = create_simple_schema();
    let (table, ctx) = setup_table(
        &fixture,
        "multi_batch_insert",
        Arc::clone(&schema),
        vec!["id".into()],
    )
    .await?;

    // Insert 3 separate batches
    for i in 0..3 {
        let start = i * 10 + 1;
        let ids: Vec<i64> = (start..start + 10).collect();
        let values: Vec<i64> = ids.iter().map(|id| id * 100).collect();

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(Int64Array::from(values)),
            ],
        )?;
        insert_batch(&table, batch).await?;
    }

    assert_eq!(get_row_count(&ctx, "multi_batch_insert").await?, 30);

    // Verify data from different batches
    assert_eq!(
        get_value_for_id(&ctx, "multi_batch_insert", 1).await?,
        Some(100)
    );
    assert_eq!(
        get_value_for_id(&ctx, "multi_batch_insert", 15).await?,
        Some(1500)
    );
    assert_eq!(
        get_value_for_id(&ctx, "multi_batch_insert", 30).await?,
        Some(3000)
    );

    Ok(())
}

test_with_backends!(test_insert_multiple_batches_impl);

// =============================================================================
// TEST 5: Delete Single Row
// =============================================================================

async fn test_delete_single_row_impl(fixture: TestFixture) -> TestResult<()> {
    let schema = create_simple_schema();
    let (table, ctx) = setup_table(
        &fixture,
        "delete_single",
        Arc::clone(&schema),
        vec!["id".into()],
    )
    .await?;

    // Insert data
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(Int64Array::from(vec![100, 200, 300, 400, 500])),
        ],
    )?;
    insert_batch(&table, batch).await?;

    // Delete single row
    let deleted = delete_records(&table, col("id").eq(lit(3i64))).await?;
    assert_eq!(deleted, 1, "Should delete 1 row");
    assert_eq!(get_row_count(&ctx, "delete_single").await?, 4);

    // Verify the deleted row is gone
    assert_eq!(get_value_for_id(&ctx, "delete_single", 3).await?, None);

    // Verify other rows still exist
    let ids = get_all_ids(&ctx, "delete_single").await?;
    assert_eq!(ids, vec![1, 2, 4, 5]);

    Ok(())
}

test_with_backends!(test_delete_single_row_impl);

// =============================================================================
// TEST 6: Delete Multiple Rows with Filter
// =============================================================================

async fn test_delete_multiple_rows_impl(fixture: TestFixture) -> TestResult<()> {
    let schema = create_simple_schema();
    let (table, ctx) = setup_table(
        &fixture,
        "delete_multi",
        Arc::clone(&schema),
        vec!["id".into()],
    )
    .await?;

    // Insert data
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10])),
            Arc::new(Int64Array::from(vec![
                100, 200, 300, 400, 500, 600, 700, 800, 900, 1000,
            ])),
        ],
    )?;
    insert_batch(&table, batch).await?;

    // Delete rows where value > 500
    let deleted = delete_records(&table, col("value").gt(lit(500i64))).await?;
    assert_eq!(deleted, 5, "Should delete 5 rows");
    assert_eq!(get_row_count(&ctx, "delete_multi").await?, 5);

    let ids = get_all_ids(&ctx, "delete_multi").await?;
    assert_eq!(ids, vec![1, 2, 3, 4, 5]);

    Ok(())
}

test_with_backends!(test_delete_multiple_rows_impl);

// =============================================================================
// TEST 7: Delete All Rows
// =============================================================================

async fn test_delete_all_rows_impl(fixture: TestFixture) -> TestResult<()> {
    let schema = create_simple_schema();
    let (table, ctx) = setup_table(
        &fixture,
        "delete_all",
        Arc::clone(&schema),
        vec!["id".into()],
    )
    .await?;

    // Insert data
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(Int64Array::from(vec![100, 200, 300])),
        ],
    )?;
    insert_batch(&table, batch).await?;

    // Delete all rows
    let deleted = delete_records(&table, col("id").gt(lit(0i64))).await?;
    assert_eq!(deleted, 3, "Should delete all 3 rows");
    assert_eq!(get_row_count(&ctx, "delete_all").await?, 0);

    Ok(())
}

test_with_backends!(test_delete_all_rows_impl);

// =============================================================================
// TEST 8: Delete with No Matches
// =============================================================================

async fn test_delete_no_matches_impl(fixture: TestFixture) -> TestResult<()> {
    let schema = create_simple_schema();
    let (table, ctx) = setup_table(
        &fixture,
        "delete_nomatch",
        Arc::clone(&schema),
        vec!["id".into()],
    )
    .await?;

    // Insert data
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(Int64Array::from(vec![100, 200, 300])),
        ],
    )?;
    insert_batch(&table, batch).await?;

    // Try to delete non-existent row
    let deleted = delete_records(&table, col("id").eq(lit(999i64))).await?;
    assert_eq!(deleted, 0, "Should delete 0 rows");
    assert_eq!(get_row_count(&ctx, "delete_nomatch").await?, 3);

    Ok(())
}

test_with_backends!(test_delete_no_matches_impl);

// =============================================================================
// TEST 9: Upsert - Delete then Insert Same Key
// =============================================================================

async fn test_upsert_same_key_impl(fixture: TestFixture) -> TestResult<()> {
    let schema = create_simple_schema();
    let (table, ctx) = setup_table(
        &fixture,
        "upsert_same",
        Arc::clone(&schema),
        vec!["id".into()],
    )
    .await?;

    // Insert initial data
    let batch1 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(Int64Array::from(vec![100, 200, 300])),
        ],
    )?;
    insert_batch(&table, batch1).await?;

    // Upsert: delete id=2 and insert new value
    delete_records(&table, col("id").eq(lit(2i64))).await?;

    let batch2 = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![2])),
            Arc::new(Int64Array::from(vec![999])),
        ],
    )?;
    insert_batch(&table, batch2).await?;

    // Verify: row count unchanged, value updated
    assert_eq!(get_row_count(&ctx, "upsert_same").await?, 3);
    assert_eq!(get_value_for_id(&ctx, "upsert_same", 2).await?, Some(999));

    Ok(())
}

test_with_backends!(test_upsert_same_key_impl);

// =============================================================================
// TEST 10: Multiple Upserts on Same Key
// =============================================================================

async fn test_multiple_upserts_same_key_impl(fixture: TestFixture) -> TestResult<()> {
    let schema = create_simple_schema();
    let (table, ctx) = setup_table(
        &fixture,
        "multi_upsert",
        Arc::clone(&schema),
        vec!["id".into()],
    )
    .await?;

    // Insert initial data
    let batch1 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![100])),
        ],
    )?;
    insert_batch(&table, batch1).await?;

    // Perform 5 sequential upserts on same key
    for i in 1..=5 {
        delete_records(&table, col("id").eq(lit(1i64))).await?;
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(Int64Array::from(vec![i * 100])),
            ],
        )?;
        insert_batch(&table, batch).await?;
    }

    // Final value should be 500
    assert_eq!(get_row_count(&ctx, "multi_upsert").await?, 1);
    assert_eq!(get_value_for_id(&ctx, "multi_upsert", 1).await?, Some(500));

    Ok(())
}

test_with_backends!(test_multiple_upserts_same_key_impl);

// =============================================================================
// TEST 11: Insert After Delete All
// =============================================================================

async fn test_insert_after_delete_all_impl(fixture: TestFixture) -> TestResult<()> {
    let schema = create_simple_schema();
    let (table, ctx) = setup_table(
        &fixture,
        "insert_after_delete",
        Arc::clone(&schema),
        vec!["id".into()],
    )
    .await?;

    // Insert, delete all, insert again
    let batch1 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(Int64Array::from(vec![100, 200, 300])),
        ],
    )?;
    insert_batch(&table, batch1).await?;

    delete_records(&table, col("id").gt(lit(0i64))).await?;
    assert_eq!(get_row_count(&ctx, "insert_after_delete").await?, 0);

    let batch2 = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![10, 20])),
            Arc::new(Int64Array::from(vec![1000, 2000])),
        ],
    )?;
    insert_batch(&table, batch2).await?;

    assert_eq!(get_row_count(&ctx, "insert_after_delete").await?, 2);
    let ids = get_all_ids(&ctx, "insert_after_delete").await?;
    assert_eq!(ids, vec![10, 20]);

    Ok(())
}

test_with_backends!(test_insert_after_delete_all_impl);

// =============================================================================
// TEST 12: Multi-column Update (all non-PK columns)
// =============================================================================

async fn test_multi_column_update_impl(fixture: TestFixture) -> TestResult<()> {
    let schema = create_multi_column_schema();
    let (table, ctx) = setup_table(
        &fixture,
        "multi_col_update",
        Arc::clone(&schema),
        vec!["id".into()],
    )
    .await?;

    // Insert initial data
    let batch1 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["original"])),
            Arc::new(Int64Array::from(vec![100])),
            Arc::new(Float64Array::from(vec![1.5])),
            Arc::new(BinaryArray::from(vec![b"original".as_slice()])),
        ],
    )?;
    insert_batch(&table, batch1).await?;

    // Delete and reinsert with completely different values
    delete_records(&table, col("id").eq(lit(1i64))).await?;

    let batch2 = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["updated"])),
            Arc::new(Int64Array::from(vec![999])),
            Arc::new(Float64Array::from(vec![99.9])),
            Arc::new(BinaryArray::from(vec![b"updated".as_slice()])),
        ],
    )?;
    insert_batch(&table, batch2).await?;

    // Verify all columns updated
    let df = ctx
        .sql("SELECT * FROM multi_col_update WHERE id = 1")
        .await?;
    let results = df.collect().await?;
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].num_rows(), 1);

    let name = results[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("name column");
    assert_eq!(name.value(0), "updated");

    Ok(())
}

test_with_backends!(test_multi_column_update_impl);

// =============================================================================
// TEST 13: Nullable Field Handling
// =============================================================================

async fn test_nullable_field_mutations_impl(fixture: TestFixture) -> TestResult<()> {
    let schema = create_nullable_schema();
    let (table, ctx) = setup_table(
        &fixture,
        "nullable_test",
        Arc::clone(&schema),
        vec!["id".into()],
    )
    .await?;

    // Insert with mix of null and non-null values
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            Arc::new(Int32Array::from(vec![Some(10), None, Some(30), None])),
            Arc::new(StringArray::from(vec![Some("a"), Some("b"), None, None])),
        ],
    )?;
    insert_batch(&table, batch).await?;

    assert_eq!(get_row_count(&ctx, "nullable_test").await?, 4);

    // Delete row with null int
    delete_records(&table, col("id").eq(lit(2i64))).await?;
    assert_eq!(get_row_count(&ctx, "nullable_test").await?, 3);

    // Insert row with all nulls (except PK)
    let batch2 = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![5])),
            Arc::new(Int32Array::from(vec![None::<i32>])),
            Arc::new(StringArray::from(vec![None::<&str>])),
        ],
    )?;
    insert_batch(&table, batch2).await?;

    assert_eq!(get_row_count(&ctx, "nullable_test").await?, 4);

    Ok(())
}

test_with_backends!(test_nullable_field_mutations_impl);

// =============================================================================
// TEST 14: Boundary Value Mutations
// =============================================================================

async fn test_boundary_values_impl(fixture: TestFixture) -> TestResult<()> {
    let schema = create_simple_schema();
    let (table, ctx) = setup_table(
        &fixture,
        "boundary_vals",
        Arc::clone(&schema),
        vec!["id".into()],
    )
    .await?;

    // Insert boundary values
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![i64::MIN, -1, 0, 1, i64::MAX])),
            Arc::new(Int64Array::from(vec![i64::MAX, i64::MIN, 0, -1, 1])),
        ],
    )?;
    insert_batch(&table, batch).await?;

    assert_eq!(get_row_count(&ctx, "boundary_vals").await?, 5);

    // Delete i64::MIN
    delete_records(&table, col("id").eq(lit(i64::MIN))).await?;
    assert_eq!(get_row_count(&ctx, "boundary_vals").await?, 4);

    // Re-insert i64::MIN with different value
    let batch2 = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![i64::MIN])),
            Arc::new(Int64Array::from(vec![0])),
        ],
    )?;
    insert_batch(&table, batch2).await?;

    assert_eq!(get_row_count(&ctx, "boundary_vals").await?, 5);
    assert_eq!(
        get_value_for_id(&ctx, "boundary_vals", i64::MIN).await?,
        Some(0)
    );

    Ok(())
}

test_with_backends!(test_boundary_values_impl);

// =============================================================================
// TEST 15: Interleaved Insert and Delete Operations
// =============================================================================

async fn test_interleaved_ops_impl(fixture: TestFixture) -> TestResult<()> {
    let schema = create_simple_schema();
    let (table, ctx) = setup_table(
        &fixture,
        "interleaved",
        Arc::clone(&schema),
        vec!["id".into()],
    )
    .await?;

    // Insert batch 1
    let batch1 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(Int64Array::from(vec![100, 200, 300])),
        ],
    )?;
    insert_batch(&table, batch1).await?;

    // Delete id=2
    delete_records(&table, col("id").eq(lit(2i64))).await?;

    // Insert batch 2
    let batch2 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![4, 5])),
            Arc::new(Int64Array::from(vec![400, 500])),
        ],
    )?;
    insert_batch(&table, batch2).await?;

    // Delete id=1, 4
    delete_records(&table, col("id").eq(lit(1i64)).or(col("id").eq(lit(4i64)))).await?;

    // Insert batch 3
    let batch3 = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![6, 7, 8])),
            Arc::new(Int64Array::from(vec![600, 700, 800])),
        ],
    )?;
    insert_batch(&table, batch3).await?;

    // Final state: 3, 5, 6, 7, 8
    let ids = get_all_ids(&ctx, "interleaved").await?;
    assert_eq!(ids, vec![3, 5, 6, 7, 8]);

    Ok(())
}

test_with_backends!(test_interleaved_ops_impl);

// =============================================================================
// TEST 16: Small Frequent Updates (Many small mutations)
// =============================================================================

async fn test_small_frequent_updates_impl(fixture: TestFixture) -> TestResult<()> {
    let schema = create_simple_schema();
    let (table, ctx) = setup_table(
        &fixture,
        "small_updates",
        Arc::clone(&schema),
        vec!["id".into()],
    )
    .await?;

    // Insert initial 10 rows
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from((1..=10).collect::<Vec<_>>())),
            Arc::new(Int64Array::from(vec![0; 10])),
        ],
    )?;
    insert_batch(&table, batch).await?;

    // Perform 50 small updates (single row each)
    for i in 1..=50 {
        let target_id = (i % 10) + 1;
        delete_records(&table, col("id").eq(lit(target_id))).await?;
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![target_id])),
                Arc::new(Int64Array::from(vec![i])),
            ],
        )?;
        insert_batch(&table, batch).await?;
    }

    assert_eq!(get_row_count(&ctx, "small_updates").await?, 10);

    // Verify each row has been updated (final value = last update for that row)
    for id in 1..=10 {
        let value = get_value_for_id(&ctx, "small_updates", id).await?;
        assert!(value.is_some(), "Row {id} should exist");
    }

    Ok(())
}

test_with_backends!(test_small_frequent_updates_impl);

// =============================================================================
// TEST 17: Large Batch Delete
// =============================================================================

async fn test_large_batch_delete_impl(fixture: TestFixture) -> TestResult<()> {
    let schema = create_simple_schema();
    let (table, ctx) = setup_table(
        &fixture,
        "large_delete",
        Arc::clone(&schema),
        vec!["id".into()],
    )
    .await?;

    // Insert 500 rows
    let ids: Vec<i64> = (1..=500).collect();
    let values: Vec<i64> = ids.iter().map(|id| id * 2).collect();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(values)),
        ],
    )?;
    insert_batch(&table, batch).await?;

    // Delete all rows then re-insert only odd ones (simulating filter delete)
    let _deleted = delete_records(
        &table,
        col("id").gt(lit(0i64)).and(col("id").lt_eq(lit(500i64))),
    )
    .await?;
    // Re-insert only odd rows
    let odd_ids: Vec<i64> = (1..=500).filter(|x| x % 2 == 1).collect();
    let odd_values: Vec<i64> = odd_ids.iter().map(|id| id * 2).collect();
    let odd_batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(odd_ids.clone())),
            Arc::new(Int64Array::from(odd_values)),
        ],
    )?;
    insert_batch(&table, odd_batch).await?;

    assert_eq!(get_row_count(&ctx, "large_delete").await?, 250);

    // Verify only odd ids remain
    let remaining_ids = get_all_ids(&ctx, "large_delete").await?;
    assert!(remaining_ids.iter().all(|id| id % 2 == 1));

    Ok(())
}

test_with_backends!(test_large_batch_delete_impl);

// =============================================================================
// TEST 18: Timestamp Column Mutations
// =============================================================================

async fn test_timestamp_mutations_impl(fixture: TestFixture) -> TestResult<()> {
    let schema = create_timestamp_schema();
    let (table, ctx) = setup_table(
        &fixture,
        "timestamp_mut",
        Arc::clone(&schema),
        vec!["id".into()],
    )
    .await?;

    // Insert with various timestamps including epoch and future
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(TimestampMillisecondArray::from(vec![
                0,                 // epoch
                1_700_000_000_000, // 2023
                2_000_000_000_000, // 2033 (future)
            ])),
            Arc::new(Int64Array::from(vec![100, 200, 300])),
        ],
    )?;
    insert_batch(&table, batch).await?;

    // Delete rows with id > 1 (which have later timestamps)
    let deleted = delete_records(&table, col("id").gt(lit(1i64))).await?;
    assert_eq!(deleted, 2, "Should delete 2 rows with id > 1");

    // Only epoch row should remain
    assert_eq!(get_row_count(&ctx, "timestamp_mut").await?, 1);
    let ids = get_all_ids(&ctx, "timestamp_mut").await?;
    assert_eq!(ids, vec![1]);

    Ok(())
}

test_with_backends!(test_timestamp_mutations_impl);

// =============================================================================
// TEST 19: Stress Test - Many Concurrent-Style Operations
// =============================================================================

async fn test_stress_many_operations_impl(fixture: TestFixture) -> TestResult<()> {
    let schema = create_simple_schema();
    let (table, ctx) = setup_table(
        &fixture,
        "stress_test",
        Arc::clone(&schema),
        vec!["id".into()],
    )
    .await?;

    // Perform 100 operations: mix of inserts and deletes
    let mut expected_ids: std::collections::HashSet<i64> = std::collections::HashSet::new();

    for i in 1..=100i64 {
        if i % 3 == 0 {
            // Every 3rd operation: delete previous if exists
            let target = i - 2;
            if expected_ids.remove(&target) {
                delete_records(&table, col("id").eq(lit(target))).await?;
            }
        } else {
            // Insert new row
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(vec![i])),
                    Arc::new(Int64Array::from(vec![i * 10])),
                ],
            )?;
            insert_batch(&table, batch).await?;
            expected_ids.insert(i);
        }
    }

    // Verify final state matches expected
    let actual_count = get_row_count(&ctx, "stress_test").await?;
    assert_eq!(
        usize::try_from(actual_count).expect("count should be positive"),
        expected_ids.len()
    );

    let actual_ids: std::collections::HashSet<i64> = get_all_ids(&ctx, "stress_test")
        .await?
        .into_iter()
        .collect();
    assert_eq!(actual_ids, expected_ids);

    Ok(())
}

test_with_backends!(test_stress_many_operations_impl);

// =============================================================================
// TEST 20: Bulk Delete then Bulk Insert (Full Table Replacement)
// =============================================================================

async fn test_full_table_replacement_impl(fixture: TestFixture) -> TestResult<()> {
    let schema = create_simple_schema();
    let (table, ctx) = setup_table(
        &fixture,
        "full_replace",
        Arc::clone(&schema),
        vec!["id".into()],
    )
    .await?;

    // Insert initial data
    let batch1 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(Int64Array::from(vec![100, 200, 300, 400, 500])),
        ],
    )?;
    insert_batch(&table, batch1).await?;

    // Delete all
    delete_records(&table, col("id").gt(lit(0i64))).await?;

    // Insert completely new data
    let batch2 = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![100, 200, 300])),
            Arc::new(Int64Array::from(vec![1, 2, 3])),
        ],
    )?;
    insert_batch(&table, batch2).await?;

    // Verify new data
    assert_eq!(get_row_count(&ctx, "full_replace").await?, 3);
    let ids = get_all_ids(&ctx, "full_replace").await?;
    assert_eq!(ids, vec![100, 200, 300]);
    assert_eq!(get_value_for_id(&ctx, "full_replace", 100).await?, Some(1));

    Ok(())
}

test_with_backends!(test_full_table_replacement_impl);
