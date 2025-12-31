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

//! Comprehensive edge case tests for RowConverter-based (key-based) deletion strategy.
//!
//! RowConverter-based deletion uses `arrow_row::RowConverter` to convert composite
//! or non-integer primary keys to byte representations for efficient lookup.
//! This strategy is used for:
//! - Composite primary keys (multiple columns)
//! - Non-integer primary keys (String, etc.)
//!
//! These tests validate:
//! 1. **String PK**: Single-column String primary key
//! 2. **Composite PK**: Multi-column primary keys
//! 3. **Empty deletion set**: Deleting with no matches
//! 4. **All rows deleted**: Full table deletion and subsequent inserts
//! 5. **Idempotent deletion**: Deleting already-deleted rows
//! 6. **Sequential deletes**: Multiple delete operations in sequence
//! 7. **Projection without PK**: Querying only non-PK columns
//! 8. **Multi-file scenarios**: Deleting from different data files
//! 9. **Special characters**: Unicode, empty strings, whitespace
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

fn create_string_pk_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("code", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

fn create_composite_pk_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("region", DataType::Utf8, false),
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

async fn setup_string_pk_table(
    fixture: &TestFixture,
    table_name: &str,
) -> TestResult<(Arc<CayenneTableProvider>, SessionContext, Arc<Schema>)> {
    let schema = create_string_pk_schema();

    let table_options = CreateTableOptions {
        table_name: table_name.to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["code".to_string()], // String PK -> RowConverter strategy
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

async fn setup_composite_pk_table(
    fixture: &TestFixture,
    table_name: &str,
) -> TestResult<(Arc<CayenneTableProvider>, SessionContext, Arc<Schema>)> {
    let schema = create_composite_pk_schema();

    let table_options = CreateTableOptions {
        table_name: table_name.to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["region".to_string(), "id".to_string()], // Composite PK -> RowConverter strategy
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

async fn get_codes(ctx: &SessionContext, table_name: &str) -> TestResult<Vec<String>> {
    let df = ctx
        .sql(&format!("SELECT code FROM {table_name} ORDER BY code"))
        .await?;
    let results = df.collect().await?;
    let codes: Vec<String> = results
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("code column")
                .iter()
                .filter_map(|s| s.map(String::from))
        })
        .collect();
    Ok(codes)
}

// =============================================================================
// STRING PK TESTS
// =============================================================================

// Edge Case 1: String PK - basic deletion
async fn test_string_pk_basic_deletion_impl(fixture: TestFixture) -> TestResult<()> {
    let (table, ctx, schema) = setup_string_pk_table(&fixture, "string_pk_basic").await?;

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["A", "B", "C", "D", "E"])),
            Arc::new(StringArray::from(vec![
                "alpha", "bravo", "charlie", "delta", "echo",
            ])),
            Arc::new(Int64Array::from(vec![100, 200, 300, 400, 500])),
        ],
    )?;
    insert_batch(&table, batch).await?;

    assert_eq!(get_row_count(&ctx, "string_pk_basic").await?, 5);

    // Delete code='B' and code='D'
    delete_records(&table, col("code").eq(lit("B"))).await?;
    delete_records(&table, col("code").eq(lit("D"))).await?;

    assert_eq!(get_row_count(&ctx, "string_pk_basic").await?, 3);
    assert_eq!(
        get_codes(&ctx, "string_pk_basic").await?,
        vec!["A", "C", "E"]
    );

    Ok(())
}

test_with_backends!(test_string_pk_basic_deletion_impl);

// Edge Case 2: String PK - delete with no matches
async fn test_string_pk_delete_no_matches_impl(fixture: TestFixture) -> TestResult<()> {
    let (table, ctx, schema) = setup_string_pk_table(&fixture, "string_pk_no_match").await?;

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["X", "Y", "Z"])),
            Arc::new(StringArray::from(vec!["xray", "yankee", "zulu"])),
            Arc::new(Int64Array::from(vec![1, 2, 3])),
        ],
    )?;
    insert_batch(&table, batch).await?;

    let deleted = delete_records(&table, col("code").eq(lit("NONEXISTENT"))).await?;
    assert_eq!(deleted, 0);
    assert_eq!(get_row_count(&ctx, "string_pk_no_match").await?, 3);

    Ok(())
}

test_with_backends!(test_string_pk_delete_no_matches_impl);

// Edge Case 3: String PK - delete all rows
async fn test_string_pk_delete_all_rows_impl(fixture: TestFixture) -> TestResult<()> {
    let (table, ctx, schema) = setup_string_pk_table(&fixture, "string_pk_delete_all").await?;

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["P", "Q", "R"])),
            Arc::new(StringArray::from(vec!["papa", "quebec", "romeo"])),
            Arc::new(Int64Array::from(vec![10, 20, 30])),
        ],
    )?;
    insert_batch(&table, batch).await?;

    // Delete all
    for code in ["P", "Q", "R"] {
        delete_records(&table, col("code").eq(lit(code))).await?;
    }

    assert_eq!(get_row_count(&ctx, "string_pk_delete_all").await?, 0);

    // Insert new rows
    let new_batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["S", "T"])),
            Arc::new(StringArray::from(vec!["sierra", "tango"])),
            Arc::new(Int64Array::from(vec![40, 50])),
        ],
    )?;
    insert_batch(&table, new_batch).await?;

    assert_eq!(get_row_count(&ctx, "string_pk_delete_all").await?, 2);

    Ok(())
}

test_with_backends!(test_string_pk_delete_all_rows_impl);

// Edge Case 4: String PK - idempotent deletion
async fn test_string_pk_idempotent_delete_impl(fixture: TestFixture) -> TestResult<()> {
    let (table, ctx, schema) = setup_string_pk_table(&fixture, "string_pk_idempotent").await?;

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["M", "N", "O"])),
            Arc::new(StringArray::from(vec!["mike", "november", "oscar"])),
            Arc::new(Int64Array::from(vec![1, 2, 3])),
        ],
    )?;
    insert_batch(&table, batch).await?;

    // Delete N
    let deleted1 = delete_records(&table, col("code").eq(lit("N"))).await?;
    assert_eq!(deleted1, 1);

    // Try to delete N again
    let deleted2 = delete_records(&table, col("code").eq(lit("N"))).await?;
    assert_eq!(deleted2, 0);

    assert_eq!(get_row_count(&ctx, "string_pk_idempotent").await?, 2);

    Ok(())
}

test_with_backends!(test_string_pk_idempotent_delete_impl);

// Edge Case 5: String PK - special characters and Unicode
async fn test_string_pk_special_characters_impl(fixture: TestFixture) -> TestResult<()> {
    let (table, ctx, schema) = setup_string_pk_table(&fixture, "string_pk_special").await?;

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![
                "hello",
                "世界",      // Chinese
                "🚀",        // Emoji
                "café",      // Accented
                "tab\there", // Tab character
            ])),
            Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])),
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
        ],
    )?;
    insert_batch(&table, batch).await?;

    assert_eq!(get_row_count(&ctx, "string_pk_special").await?, 5);

    // Delete Unicode values
    delete_records(&table, col("code").eq(lit("世界"))).await?;
    delete_records(&table, col("code").eq(lit("🚀"))).await?;

    assert_eq!(get_row_count(&ctx, "string_pk_special").await?, 3);

    let remaining = get_codes(&ctx, "string_pk_special").await?;
    assert!(!remaining.contains(&"世界".to_string()));
    assert!(!remaining.contains(&"🚀".to_string()));

    Ok(())
}

test_with_backends!(test_string_pk_special_characters_impl);

// Edge Case 6: String PK - projection without PK column
async fn test_string_pk_projection_after_delete_impl(fixture: TestFixture) -> TestResult<()> {
    let (table, ctx, schema) = setup_string_pk_table(&fixture, "string_pk_projection").await?;

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["K1", "K2", "K3", "K4"])),
            Arc::new(StringArray::from(vec!["name1", "name2", "name3", "name4"])),
            Arc::new(Int64Array::from(vec![100, 200, 300, 400])),
        ],
    )?;
    insert_batch(&table, batch).await?;

    delete_records(&table, col("code").eq(lit("K2"))).await?;
    delete_records(&table, col("code").eq(lit("K4"))).await?;

    // Query only non-PK columns
    let df = ctx
        .sql("SELECT name, value FROM string_pk_projection ORDER BY value")
        .await?;
    let results = df.collect().await?;

    let values: Vec<i64> = results
        .iter()
        .flat_map(|b| {
            b.column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("value column")
                .values()
                .iter()
                .copied()
        })
        .collect();

    assert_eq!(values, vec![100, 300]);

    Ok(())
}

test_with_backends!(test_string_pk_projection_after_delete_impl);

// Edge Case 7: String PK - multi-file deletion
async fn test_string_pk_multi_file_deletion_impl(fixture: TestFixture) -> TestResult<()> {
    let (table, ctx, schema) = setup_string_pk_table(&fixture, "string_pk_multi_file").await?;

    // Insert batch 1
    let batch1 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["F1", "F2", "F3"])),
            Arc::new(StringArray::from(vec!["file1_a", "file1_b", "file1_c"])),
            Arc::new(Int64Array::from(vec![1, 2, 3])),
        ],
    )?;
    insert_batch(&table, batch1).await?;

    // Insert batch 2
    let batch2 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["F4", "F5", "F6"])),
            Arc::new(StringArray::from(vec!["file2_a", "file2_b", "file2_c"])),
            Arc::new(Int64Array::from(vec![4, 5, 6])),
        ],
    )?;
    insert_batch(&table, batch2).await?;

    assert_eq!(get_row_count(&ctx, "string_pk_multi_file").await?, 6);

    // Delete from both files
    delete_records(&table, col("code").eq(lit("F2"))).await?;
    delete_records(&table, col("code").eq(lit("F5"))).await?;

    assert_eq!(get_row_count(&ctx, "string_pk_multi_file").await?, 4);

    Ok(())
}

test_with_backends!(test_string_pk_multi_file_deletion_impl);

// =============================================================================
// COMPOSITE PK TESTS
// =============================================================================

// Edge Case 8: Composite PK - basic deletion
async fn test_composite_pk_basic_deletion_impl(fixture: TestFixture) -> TestResult<()> {
    let (table, ctx, schema) = setup_composite_pk_table(&fixture, "composite_pk_basic").await?;

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["US", "US", "EU", "EU"])),
            Arc::new(Int64Array::from(vec![1, 2, 1, 2])),
            Arc::new(StringArray::from(vec!["us_1", "us_2", "eu_1", "eu_2"])),
            Arc::new(Int64Array::from(vec![100, 200, 300, 400])),
        ],
    )?;
    insert_batch(&table, batch).await?;

    assert_eq!(get_row_count(&ctx, "composite_pk_basic").await?, 4);

    // Delete (region='US', id=1)
    let filter = col("region").eq(lit("US")).and(col("id").eq(lit(1i64)));
    let deleted = delete_records(&table, filter).await?;
    assert_eq!(deleted, 1);

    assert_eq!(get_row_count(&ctx, "composite_pk_basic").await?, 3);

    // Verify the right row was deleted
    let df = ctx
        .sql("SELECT name FROM composite_pk_basic WHERE region = 'US' AND id = 1")
        .await?;
    let results = df.collect().await?;
    assert!(results.is_empty() || results[0].num_rows() == 0);

    Ok(())
}

test_with_backends!(test_composite_pk_basic_deletion_impl);

// Edge Case 9: Composite PK - delete by partial key (multiple rows)
async fn test_composite_pk_partial_key_deletion_impl(fixture: TestFixture) -> TestResult<()> {
    let (table, ctx, schema) = setup_composite_pk_table(&fixture, "composite_pk_partial").await?;

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["US", "US", "US", "EU", "EU"])),
            Arc::new(Int64Array::from(vec![1, 2, 3, 1, 2])),
            Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])),
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
        ],
    )?;
    insert_batch(&table, batch).await?;

    assert_eq!(get_row_count(&ctx, "composite_pk_partial").await?, 5);

    // Delete all rows where region='US' (should delete 3 rows)
    let deleted = delete_records(&table, col("region").eq(lit("US"))).await?;
    assert_eq!(deleted, 3);

    assert_eq!(get_row_count(&ctx, "composite_pk_partial").await?, 2);

    // Verify only EU rows remain
    let df = ctx
        .sql("SELECT DISTINCT region FROM composite_pk_partial")
        .await?;
    let results = df.collect().await?;
    let regions: Vec<String> = results
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("region")
                .iter()
                .filter_map(|s| s.map(String::from))
        })
        .collect();
    assert_eq!(regions, vec!["EU"]);

    Ok(())
}

test_with_backends!(test_composite_pk_partial_key_deletion_impl);

// Edge Case 10: Composite PK - delete non-existent key
async fn test_composite_pk_delete_nonexistent_impl(fixture: TestFixture) -> TestResult<()> {
    let (table, ctx, schema) =
        setup_composite_pk_table(&fixture, "composite_pk_nonexistent").await?;

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["US", "EU"])),
            Arc::new(Int64Array::from(vec![1, 1])),
            Arc::new(StringArray::from(vec!["a", "b"])),
            Arc::new(Int64Array::from(vec![1, 2])),
        ],
    )?;
    insert_batch(&table, batch).await?;

    // Try to delete non-existent composite key
    let filter = col("region").eq(lit("ASIA")).and(col("id").eq(lit(1i64)));
    let deleted = delete_records(&table, filter).await?;
    assert_eq!(deleted, 0);

    assert_eq!(get_row_count(&ctx, "composite_pk_nonexistent").await?, 2);

    Ok(())
}

test_with_backends!(test_composite_pk_delete_nonexistent_impl);

// Edge Case 11: Composite PK - idempotent deletion
async fn test_composite_pk_idempotent_delete_impl(fixture: TestFixture) -> TestResult<()> {
    let (table, ctx, schema) =
        setup_composite_pk_table(&fixture, "composite_pk_idempotent").await?;

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["R1", "R2"])),
            Arc::new(Int64Array::from(vec![1, 1])),
            Arc::new(StringArray::from(vec!["a", "b"])),
            Arc::new(Int64Array::from(vec![10, 20])),
        ],
    )?;
    insert_batch(&table, batch).await?;

    let filter = col("region").eq(lit("R1")).and(col("id").eq(lit(1i64)));

    let deleted1 = delete_records(&table, filter.clone()).await?;
    assert_eq!(deleted1, 1);

    let deleted2 = delete_records(&table, filter).await?;
    assert_eq!(deleted2, 0);

    assert_eq!(get_row_count(&ctx, "composite_pk_idempotent").await?, 1);

    Ok(())
}

test_with_backends!(test_composite_pk_idempotent_delete_impl);

// Edge Case 12: Composite PK - multi-file deletion
async fn test_composite_pk_multi_file_deletion_impl(fixture: TestFixture) -> TestResult<()> {
    let (table, ctx, schema) =
        setup_composite_pk_table(&fixture, "composite_pk_multi_file").await?;

    // Batch 1
    let batch1 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["A", "A"])),
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["a1", "a2"])),
            Arc::new(Int64Array::from(vec![1, 2])),
        ],
    )?;
    insert_batch(&table, batch1).await?;

    // Batch 2
    let batch2 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["B", "B"])),
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["b1", "b2"])),
            Arc::new(Int64Array::from(vec![3, 4])),
        ],
    )?;
    insert_batch(&table, batch2).await?;

    assert_eq!(get_row_count(&ctx, "composite_pk_multi_file").await?, 4);

    // Delete from each batch
    let filter1 = col("region").eq(lit("A")).and(col("id").eq(lit(1i64)));
    delete_records(&table, filter1).await?;

    let filter2 = col("region").eq(lit("B")).and(col("id").eq(lit(2i64)));
    delete_records(&table, filter2).await?;

    assert_eq!(get_row_count(&ctx, "composite_pk_multi_file").await?, 2);

    Ok(())
}

test_with_backends!(test_composite_pk_multi_file_deletion_impl);

// Edge Case 13: Composite PK - stress test with interleaved operations
async fn test_composite_pk_stress_interleaved_impl(fixture: TestFixture) -> TestResult<()> {
    let (table, ctx, schema) = setup_composite_pk_table(&fixture, "composite_pk_stress").await?;

    // Insert batch 1: regions A and B with ids 1-10
    let regions1: Vec<&str> = (0..20).map(|i| if i < 10 { "A" } else { "B" }).collect();
    let ids1: Vec<i64> = (0..20).map(|i| (i % 10) + 1).collect();
    let names1: Vec<String> = (0..20).map(|i| format!("name_{i}")).collect();
    let values1: Vec<i64> = (0..20).map(|i| i * 10).collect();

    let batch1 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(regions1)),
            Arc::new(Int64Array::from(ids1)),
            Arc::new(StringArray::from(names1)),
            Arc::new(Int64Array::from(values1)),
        ],
    )?;
    insert_batch(&table, batch1).await?;

    assert_eq!(get_row_count(&ctx, "composite_pk_stress").await?, 20);

    // Delete all rows from region A with even ids
    let deleted = delete_records(
        &table,
        col("region")
            .eq(lit("A"))
            .and((col("id") % lit(2i64)).eq(lit(0i64))),
    )
    .await?;
    assert_eq!(deleted, 5); // A: 2, 4, 6, 8, 10

    assert_eq!(get_row_count(&ctx, "composite_pk_stress").await?, 15);

    // Insert batch 2
    let batch2 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["C", "C", "C", "C", "C"])),
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec!["c1", "c2", "c3", "c4", "c5"])),
            Arc::new(Int64Array::from(vec![100, 200, 300, 400, 500])),
        ],
    )?;
    insert_batch(&table, batch2).await?;

    assert_eq!(get_row_count(&ctx, "composite_pk_stress").await?, 20);

    // Delete all region C
    let deleted_c = delete_records(&table, col("region").eq(lit("C"))).await?;
    assert_eq!(deleted_c, 5);

    assert_eq!(get_row_count(&ctx, "composite_pk_stress").await?, 15);

    Ok(())
}

test_with_backends!(test_composite_pk_stress_interleaved_impl);

// Edge Case 14: Composite PK - persistence after delete
async fn test_composite_pk_persistence_after_delete_impl(fixture: TestFixture) -> TestResult<()> {
    let schema = create_composite_pk_schema();
    let table_name = "composite_pk_persistence";

    let table_options = CreateTableOptions {
        table_name: table_name.to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["region".to_string(), "id".to_string()],
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let table =
        Arc::new(CayenneTableProvider::create_table(Arc::clone(&catalog), table_options).await?);

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["X", "X", "Y"])),
            Arc::new(Int64Array::from(vec![1, 2, 1])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
            Arc::new(Int64Array::from(vec![1, 2, 3])),
        ],
    )?;
    insert_batch(&table, batch).await?;

    // Delete (X, 2)
    let filter = col("region").eq(lit("X")).and(col("id").eq(lit(2i64)));
    delete_records(&table, filter).await?;

    // Reopen table
    let table2 = Arc::new(
        CayenneTableProviderBuilder::new(catalog)
            .open(table_name)
            .await?,
    );

    let ctx = SessionContext::new();
    ctx.register_table(table_name, Arc::clone(&table2) as Arc<dyn TableProvider>)?;

    assert_eq!(get_row_count(&ctx, table_name).await?, 2);

    Ok(())
}

test_with_backends!(test_composite_pk_persistence_after_delete_impl);

// Edge Case 15: String PK - insert after delete (reusing PK) - UPSERT BEHAVIOR
// With the compaction-on-upsert fix, inserting a row with a previously deleted PK
// will trigger mini-compaction to apply deletions before adding new data.
// This ensures the new row is visible (proper upsert semantics).
async fn test_string_pk_insert_after_delete_same_pk_impl(fixture: TestFixture) -> TestResult<()> {
    let (table, ctx, schema) = setup_string_pk_table(&fixture, "string_pk_reuse").await?;

    let batch1 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["KEY1", "KEY2", "KEY3"])),
            Arc::new(StringArray::from(vec!["old1", "old2", "old3"])),
            Arc::new(Int64Array::from(vec![100, 200, 300])),
        ],
    )?;
    insert_batch(&table, batch1).await?;

    // Delete KEY2
    delete_records(&table, col("code").eq(lit("KEY2"))).await?;

    // Insert new row with same PK (KEY2 with different data)
    // With the fix, this triggers mini-compaction and the new row IS visible
    let batch2 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["KEY2"])),
            Arc::new(StringArray::from(vec!["new2"])),
            Arc::new(Int64Array::from(vec![999])),
        ],
    )?;
    insert_batch(&table, batch2).await?;

    // The new row with pk=KEY2 should be visible after upsert
    assert_eq!(
        get_row_count(&ctx, "string_pk_reuse").await?,
        3,
        "Upserted row with reused PK should be visible after mini-compaction"
    );

    // Insert with a new PK to verify inserts still work
    let batch3 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["KEY4"])),
            Arc::new(StringArray::from(vec!["new4"])),
            Arc::new(Int64Array::from(vec![400])),
        ],
    )?;
    insert_batch(&table, batch3).await?;

    assert_eq!(
        get_row_count(&ctx, "string_pk_reuse").await?,
        4,
        "New row with fresh PK should appear"
    );

    // Verify the data: should have KEY1, KEY2, KEY3, KEY4 with KEY2 being the new version
    let df = ctx
        .sql("SELECT code FROM string_pk_reuse ORDER BY code")
        .await?;
    let results = df.collect().await?;
    let codes: Vec<String> = results
        .first()
        .and_then(|b| b.column(0).as_any().downcast_ref::<StringArray>())
        .map(|a| a.iter().filter_map(|v| v.map(String::from)).collect())
        .unwrap_or_default();

    assert_eq!(
        codes,
        vec!["KEY1", "KEY2", "KEY3", "KEY4"],
        "Should have all 4 keys including upserted KEY2"
    );

    // Verify KEY2 has the new value (999)
    let df = ctx
        .sql("SELECT value FROM string_pk_reuse WHERE code = 'KEY2'")
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

test_with_backends!(test_string_pk_insert_after_delete_same_pk_impl);
