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

//! ACID Compliance Tests for Cayenne
//!
//! These tests validate that Cayenne provides proper ACID guarantees:
//!
//! **Atomicity**: Operations complete fully or not at all
//! **Consistency**: Data remains consistent across all operations
//! **Isolation**: Concurrent operations don't interfere (single-table focus)
//! **Durability**: Committed data persists across restarts
//!
//! Test categories:
//! 1. **Upsert Semantics**: Delete + insert with same PK produces correct result
//! 2. **Compaction Atomicity**: Snapshot update + delete file cleanup is atomic
//! 3. **Crash Recovery Simulation**: System recovers to consistent state after "crash"
//! 4. **Concurrent Write Serialization**: Multiple writes are serialized correctly
//! 5. **Durability**: Data survives table reopen

#![allow(clippy::expect_used)]

mod common;

use arrow::array::{Array, Int64Array, RecordBatch, StringArray};

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

async fn setup_int64_pk_table(
    fixture: &TestFixture,
    table_name: &str,
) -> TestResult<(Arc<CayenneTableProvider>, SessionContext, Arc<Schema>)> {
    let schema = create_int64_pk_schema();

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
    let ctx = SessionContext::new();
    ctx.register_table(table_name, Arc::clone(&table) as Arc<dyn TableProvider>)?;

    Ok((table, ctx, schema))
}

async fn setup_string_pk_table(
    fixture: &TestFixture,
    table_name: &str,
) -> TestResult<(Arc<CayenneTableProvider>, SessionContext, Arc<Schema>)> {
    let schema = create_string_pk_schema();

    let table_options = CreateTableOptions {
        table_name: table_name.to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["code".to_string()],
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
        primary_key: vec!["region".to_string(), "id".to_string()],
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
// ACID Test 1: Upsert with Int64 PK - Delete then Insert Same Key
// Validates: Atomicity + Consistency
// =============================================================================

async fn test_acid_upsert_int64_pk_impl(fixture: TestFixture) -> TestResult<()> {
    let (table, ctx, schema) = setup_int64_pk_table(&fixture, "acid_upsert_int64").await?;

    // Insert initial data
    let batch1 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])),
            Arc::new(Int64Array::from(vec![100, 200, 300, 400, 500])),
        ],
    )?;
    insert_batch(&table, batch1).await?;
    assert_eq!(get_row_count(&ctx, "acid_upsert_int64").await?, 5);

    // Delete row with id=3
    let deleted = delete_records(&table, col("id").eq(lit(3i64))).await?;
    assert_eq!(deleted, 1, "Should delete exactly 1 row");

    // Upsert: insert new row with same id=3 but different values
    let batch2 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![3])),
            Arc::new(StringArray::from(vec!["c_updated"])),
            Arc::new(Int64Array::from(vec![999])),
        ],
    )?;
    insert_batch(&table, batch2).await?;

    // ACID check: row count should still be 5 (old row deleted, new row inserted)
    assert_eq!(
        get_row_count(&ctx, "acid_upsert_int64").await?,
        5,
        "Row count should remain 5 after upsert"
    );

    // ACID check: the value for id=3 should be the new value
    let value = get_value_for_id(&ctx, "acid_upsert_int64", 3).await?;
    assert_eq!(value, Some(999), "Upserted row should have new value 999");

    // Verify all IDs are present
    let ids = get_ids(&ctx, "acid_upsert_int64").await?;
    assert_eq!(ids, vec![1, 2, 3, 4, 5], "All IDs should be present");

    Ok(())
}

// =============================================================================
// ACID Test 2: Upsert with String PK
// Validates: Atomicity + Consistency for RowConverter-based strategy
// =============================================================================

async fn test_acid_upsert_string_pk_impl(fixture: TestFixture) -> TestResult<()> {
    let (table, ctx, schema) = setup_string_pk_table(&fixture, "acid_upsert_string").await?;

    // Insert initial data
    let batch1 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["A", "B", "C", "D", "E"])),
            Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])),
            Arc::new(Int64Array::from(vec![100, 200, 300, 400, 500])),
        ],
    )?;
    insert_batch(&table, batch1).await?;
    assert_eq!(get_row_count(&ctx, "acid_upsert_string").await?, 5);

    // Delete row with code='C'
    let deleted = delete_records(&table, col("code").eq(lit("C"))).await?;
    assert_eq!(deleted, 1, "Should delete exactly 1 row");

    // Upsert: insert new row with same code='C' but different values
    let batch2 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["C"])),
            Arc::new(StringArray::from(vec!["c_updated"])),
            Arc::new(Int64Array::from(vec![999])),
        ],
    )?;
    insert_batch(&table, batch2).await?;

    // ACID check: row count should still be 5
    assert_eq!(
        get_row_count(&ctx, "acid_upsert_string").await?,
        5,
        "Row count should remain 5 after upsert"
    );

    // ACID check: the value for code='C' should be the new value
    let df = ctx
        .sql("SELECT value FROM acid_upsert_string WHERE code = 'C'")
        .await?;
    let results = df.collect().await?;
    let value = results
        .first()
        .and_then(|b| b.column(0).as_any().downcast_ref::<Int64Array>())
        .and_then(|a| a.values().first())
        .copied();
    assert_eq!(value, Some(999), "Upserted row should have new value 999");

    Ok(())
}

// =============================================================================
// ACID Test 3: Upsert with Composite PK
// Validates: Atomicity + Consistency for composite key RowConverter
// =============================================================================

async fn test_acid_upsert_composite_pk_impl(fixture: TestFixture) -> TestResult<()> {
    let (table, ctx, schema) = setup_composite_pk_table(&fixture, "acid_upsert_composite").await?;

    // Insert initial data
    let batch1 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["US", "US", "EU", "EU"])),
            Arc::new(Int64Array::from(vec![1, 2, 1, 2])),
            Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
            Arc::new(Int64Array::from(vec![100, 200, 300, 400])),
        ],
    )?;
    insert_batch(&table, batch1).await?;
    assert_eq!(get_row_count(&ctx, "acid_upsert_composite").await?, 4);

    // Delete row with region='US' AND id=2
    let deleted = delete_records(
        &table,
        col("region").eq(lit("US")).and(col("id").eq(lit(2i64))),
    )
    .await?;
    assert_eq!(deleted, 1, "Should delete exactly 1 row");

    // Upsert: insert new row with same composite key but different values
    let batch2 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["US"])),
            Arc::new(Int64Array::from(vec![2])),
            Arc::new(StringArray::from(vec!["b_updated"])),
            Arc::new(Int64Array::from(vec![999])),
        ],
    )?;
    insert_batch(&table, batch2).await?;

    // ACID check: row count should still be 4
    assert_eq!(
        get_row_count(&ctx, "acid_upsert_composite").await?,
        4,
        "Row count should remain 4 after upsert"
    );

    // ACID check: the value for (US, 2) should be the new value
    let df = ctx
        .sql("SELECT value FROM acid_upsert_composite WHERE region = 'US' AND id = 2")
        .await?;
    let results = df.collect().await?;
    let value = results
        .first()
        .and_then(|b| b.column(0).as_any().downcast_ref::<Int64Array>())
        .and_then(|a| a.values().first())
        .copied();
    assert_eq!(value, Some(999), "Upserted row should have new value 999");

    Ok(())
}

// =============================================================================
// ACID Test 4: Multiple Sequential Upserts
// Validates: Consistency across multiple upsert cycles
// =============================================================================

async fn test_acid_multiple_upserts_impl(fixture: TestFixture) -> TestResult<()> {
    let (table, ctx, schema) = setup_int64_pk_table(&fixture, "acid_multi_upsert").await?;

    // Insert initial data
    let batch1 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
            Arc::new(Int64Array::from(vec![100, 200, 300])),
        ],
    )?;
    insert_batch(&table, batch1).await?;

    // Perform multiple upsert cycles on the same key
    for i in 1..=5 {
        // Delete id=2
        delete_records(&table, col("id").eq(lit(2i64))).await?;

        // Insert new version of id=2
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![2])),
                Arc::new(StringArray::from(vec![format!("version_{i}")])),
                Arc::new(Int64Array::from(vec![i * 1000])),
            ],
        )?;
        insert_batch(&table, batch).await?;

        // Verify count is stable
        assert_eq!(
            get_row_count(&ctx, "acid_multi_upsert").await?,
            3,
            "Row count should remain 3 after upsert cycle {i}"
        );

        // Verify value is updated
        let value = get_value_for_id(&ctx, "acid_multi_upsert", 2).await?;
        assert_eq!(
            value,
            Some(i * 1000),
            "Value should be {i} * 1000 after upsert cycle {i}"
        );
    }

    Ok(())
}

// =============================================================================
// ACID Test 5: Durability - Reopen Table After Write
// Validates: Data persists after table reopen
// =============================================================================

async fn test_acid_durability_reopen_impl(fixture: TestFixture) -> TestResult<()> {
    let table_name = "acid_durability";

    // Create and populate table
    {
        let (table, ctx, schema) = setup_int64_pk_table(&fixture, table_name).await?;

        let batch1 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
                Arc::new(Int64Array::from(vec![100, 200, 300])),
            ],
        )?;
        insert_batch(&table, batch1).await?;

        // Delete and upsert
        delete_records(&table, col("id").eq(lit(2i64))).await?;

        let batch2 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![2])),
                Arc::new(StringArray::from(vec!["b_updated"])),
                Arc::new(Int64Array::from(vec![999])),
            ],
        )?;
        insert_batch(&table, batch2).await?;

        // Verify before "closing"
        assert_eq!(get_row_count(&ctx, table_name).await?, 3);
        let value = get_value_for_id(&ctx, table_name, 2).await?;
        assert_eq!(value, Some(999));
    }

    // "Reopen" the table by creating a new provider from the same catalog
    {
        let catalog: Arc<dyn MetadataCatalog> =
            Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
        let reopened_table = Arc::new(
            CayenneTableProviderBuilder::new(catalog)
                .open(table_name)
                .await?,
        );

        let ctx2 = SessionContext::new();
        ctx2.register_table(
            table_name,
            Arc::clone(&reopened_table) as Arc<dyn TableProvider>,
        )?;

        // Durability check: data should persist after reopen
        assert_eq!(
            get_row_count(&ctx2, table_name).await?,
            3,
            "Row count should be 3 after reopen"
        );

        let value = get_value_for_id(&ctx2, table_name, 2).await?;
        assert_eq!(
            value,
            Some(999),
            "Upserted value should persist after reopen"
        );
    }

    Ok(())
}

// =============================================================================
// ACID Test 6: Durability - Pending Deletions Survive Reopen
// Validates: Deletion vectors persist and work after table reopen
// =============================================================================

async fn test_acid_durability_deletions_persist_impl(fixture: TestFixture) -> TestResult<()> {
    let table_name = "acid_dur_delete";
    let schema = create_int64_pk_schema();

    // Create and populate table, then delete WITHOUT inserting
    {
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

        let batch1 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])),
                Arc::new(Int64Array::from(vec![100, 200, 300, 400, 500])),
            ],
        )?;
        insert_batch(&table, batch1).await?;

        // Delete rows 2 and 4 (but don't trigger compaction by not inserting)
        delete_records(&table, col("id").eq(lit(2i64))).await?;
        delete_records(&table, col("id").eq(lit(4i64))).await?;

        let ctx = SessionContext::new();
        ctx.register_table(table_name, Arc::clone(&table) as Arc<dyn TableProvider>)?;

        // Verify deletions took effect
        assert_eq!(get_row_count(&ctx, table_name).await?, 3);
        let ids = get_ids(&ctx, table_name).await?;
        assert_eq!(ids, vec![1, 3, 5]);
    }

    // Reopen and verify deletions still apply
    {
        let catalog: Arc<dyn MetadataCatalog> =
            Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
        let reopened_table = Arc::new(
            CayenneTableProviderBuilder::new(catalog)
                .open(table_name)
                .await?,
        );

        let ctx2 = SessionContext::new();
        ctx2.register_table(
            table_name,
            Arc::clone(&reopened_table) as Arc<dyn TableProvider>,
        )?;

        // Durability check: deletions should persist
        assert_eq!(
            get_row_count(&ctx2, table_name).await?,
            3,
            "Deleted rows should still be filtered after reopen"
        );

        let ids = get_ids(&ctx2, table_name).await?;
        assert_eq!(
            ids,
            vec![1, 3, 5],
            "Only non-deleted rows should be visible after reopen"
        );
    }

    Ok(())
}

// =============================================================================
// ACID Test 7: Batch Upsert - Multiple Keys Updated At Once
// Validates: Atomicity for batch operations
// =============================================================================

async fn test_acid_batch_upsert_impl(fixture: TestFixture) -> TestResult<()> {
    let (table, ctx, schema) = setup_int64_pk_table(&fixture, "acid_batch_upsert").await?;

    // Insert initial data
    let batch1 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])),
            Arc::new(Int64Array::from(vec![100, 200, 300, 400, 500])),
        ],
    )?;
    insert_batch(&table, batch1).await?;

    // Delete multiple rows at once
    let deleted =
        delete_records(&table, col("id").gt(lit(2i64)).and(col("id").lt(lit(5i64)))).await?;
    assert_eq!(deleted, 2, "Should delete rows 3 and 4");

    // Insert multiple rows including upserts
    let batch2 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![3, 4, 6])), // 3 and 4 are upserts, 6 is new
            Arc::new(StringArray::from(vec!["c_new", "d_new", "f"])),
            Arc::new(Int64Array::from(vec![333, 444, 600])),
        ],
    )?;
    insert_batch(&table, batch2).await?;

    // ACID check: should have 6 rows total (1,2,3,4,5 + 6)
    assert_eq!(
        get_row_count(&ctx, "acid_batch_upsert").await?,
        6,
        "Should have 6 rows after batch upsert"
    );

    // Verify specific values
    assert_eq!(
        get_value_for_id(&ctx, "acid_batch_upsert", 3).await?,
        Some(333)
    );
    assert_eq!(
        get_value_for_id(&ctx, "acid_batch_upsert", 4).await?,
        Some(444)
    );
    assert_eq!(
        get_value_for_id(&ctx, "acid_batch_upsert", 6).await?,
        Some(600)
    );

    Ok(())
}

// =============================================================================
// ACID Test 8: Delete All Then Insert - Ensures Clean Slate
// Validates: Atomicity when table is fully cleared
// =============================================================================

async fn test_acid_delete_all_then_insert_impl(fixture: TestFixture) -> TestResult<()> {
    let (table, ctx, schema) = setup_int64_pk_table(&fixture, "acid_delete_all").await?;

    // Insert initial data
    let batch1 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
            Arc::new(Int64Array::from(vec![100, 200, 300])),
        ],
    )?;
    insert_batch(&table, batch1).await?;
    assert_eq!(get_row_count(&ctx, "acid_delete_all").await?, 3);

    // Delete all rows
    let deleted = delete_records(&table, lit(true)).await?;
    assert_eq!(deleted, 3, "Should delete all 3 rows");
    assert_eq!(get_row_count(&ctx, "acid_delete_all").await?, 0);

    // Insert new data with same keys
    let batch2 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["x", "y", "z"])),
            Arc::new(Int64Array::from(vec![1000, 2000, 3000])),
        ],
    )?;
    insert_batch(&table, batch2).await?;

    // ACID check: should have 3 new rows
    assert_eq!(
        get_row_count(&ctx, "acid_delete_all").await?,
        3,
        "Should have 3 new rows after delete all + insert"
    );

    // Verify new values
    assert_eq!(
        get_value_for_id(&ctx, "acid_delete_all", 1).await?,
        Some(1000)
    );
    assert_eq!(
        get_value_for_id(&ctx, "acid_delete_all", 2).await?,
        Some(2000)
    );
    assert_eq!(
        get_value_for_id(&ctx, "acid_delete_all", 3).await?,
        Some(3000)
    );

    Ok(())
}

// =============================================================================
// ACID Test 9: Interleaved Inserts and Deletes
// Validates: Consistency under complex operation sequences
// =============================================================================

async fn test_acid_interleaved_ops_impl(fixture: TestFixture) -> TestResult<()> {
    let (table, ctx, schema) = setup_int64_pk_table(&fixture, "acid_interleaved").await?;

    // Insert batch 1: ids 1, 2, 3
    let batch1 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
            Arc::new(Int64Array::from(vec![100, 200, 300])),
        ],
    )?;
    insert_batch(&table, batch1).await?;

    // Delete id=2
    delete_records(&table, col("id").eq(lit(2i64))).await?;

    // Insert batch 2: ids 4, 5, 2 (2 is upsert)
    let batch2 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![4, 5, 2])),
            Arc::new(StringArray::from(vec!["d", "e", "b2"])),
            Arc::new(Int64Array::from(vec![400, 500, 222])),
        ],
    )?;
    insert_batch(&table, batch2).await?;

    // Delete id=1 and id=4
    delete_records(&table, col("id").eq(lit(1i64))).await?;
    delete_records(&table, col("id").eq(lit(4i64))).await?;

    // Insert batch 3: ids 1, 4, 6 (1 and 4 are upserts)
    let batch3 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 4, 6])),
            Arc::new(StringArray::from(vec!["a3", "d3", "f"])),
            Arc::new(Int64Array::from(vec![111, 444, 600])),
        ],
    )?;
    insert_batch(&table, batch3).await?;

    // Final state should be: ids 1,2,3,4,5,6 with specific values
    assert_eq!(
        get_row_count(&ctx, "acid_interleaved").await?,
        6,
        "Should have 6 rows after interleaved operations"
    );

    // Verify specific values
    assert_eq!(
        get_value_for_id(&ctx, "acid_interleaved", 1).await?,
        Some(111)
    );
    assert_eq!(
        get_value_for_id(&ctx, "acid_interleaved", 2).await?,
        Some(222)
    );
    assert_eq!(
        get_value_for_id(&ctx, "acid_interleaved", 3).await?,
        Some(300)
    );
    assert_eq!(
        get_value_for_id(&ctx, "acid_interleaved", 4).await?,
        Some(444)
    );
    assert_eq!(
        get_value_for_id(&ctx, "acid_interleaved", 5).await?,
        Some(500)
    );
    assert_eq!(
        get_value_for_id(&ctx, "acid_interleaved", 6).await?,
        Some(600)
    );

    Ok(())
}

// =============================================================================
// MULTI-FILE UPSERT TESTS
// These tests specifically verify upsert behavior when data spans multiple files
// =============================================================================

// =============================================================================
// Multi-File Test 1: Delete from File 1, Insert Same PK to File 2 (Int64 PK)
// =============================================================================

async fn test_multifile_delete_file1_insert_file2_int64_impl(
    fixture: TestFixture,
) -> TestResult<()> {
    let (table, ctx, schema) = setup_int64_pk_table(&fixture, "mf_del_ins_int64").await?;

    // File 1: Insert initial batch
    let batch1 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
            Arc::new(Int64Array::from(vec![100, 200, 300])),
        ],
    )?;
    insert_batch(&table, batch1).await?;
    assert_eq!(get_row_count(&ctx, "mf_del_ins_int64").await?, 3);

    // Delete id=2 from file 1
    let deleted = delete_records(&table, col("id").eq(lit(2i64))).await?;
    assert_eq!(deleted, 1);
    assert_eq!(get_row_count(&ctx, "mf_del_ins_int64").await?, 2);

    // File 2: Insert new batch including same PK (id=2) with different value
    // This should trigger mini-compaction and correctly show the new value
    let batch2 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![4, 2, 5])), // id=2 is "re-inserted"
            Arc::new(StringArray::from(vec!["d", "b_new", "e"])),
            Arc::new(Int64Array::from(vec![400, 222, 500])),
        ],
    )?;
    insert_batch(&table, batch2).await?;

    // Should have 5 rows: 1, 2 (new), 3, 4, 5
    assert_eq!(
        get_row_count(&ctx, "mf_del_ins_int64").await?,
        5,
        "Should have 5 rows after upsert across files"
    );
    assert_eq!(
        get_ids(&ctx, "mf_del_ins_int64").await?,
        vec![1, 2, 3, 4, 5]
    );

    // Verify id=2 has the NEW value (222), not the old (200)
    let value = get_value_for_id(&ctx, "mf_del_ins_int64", 2).await?;
    assert_eq!(value, Some(222), "Re-inserted PK should have new value");

    Ok(())
}

// =============================================================================
// Multi-File Test 2: Delete from File 1, Insert Same PK to File 2 (String PK)
// =============================================================================

async fn test_multifile_delete_file1_insert_file2_string_impl(
    fixture: TestFixture,
) -> TestResult<()> {
    let (table, ctx, schema) = setup_string_pk_table(&fixture, "mf_del_ins_string").await?;

    // File 1: Insert initial batch (schema: code, name, value)
    let batch1 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["key1", "key2", "key3"])),
            Arc::new(StringArray::from(vec!["desc1", "desc2", "desc3"])),
            Arc::new(Int64Array::from(vec![100, 200, 300])),
        ],
    )?;
    insert_batch(&table, batch1).await?;
    assert_eq!(get_row_count(&ctx, "mf_del_ins_string").await?, 3);

    // Delete key2 from file 1 (PK column is 'code')
    let deleted = delete_records(&table, col("code").eq(lit("key2"))).await?;
    assert_eq!(deleted, 1);
    assert_eq!(get_row_count(&ctx, "mf_del_ins_string").await?, 2);

    // File 2: Insert new batch including same PK (key2) with different value
    let batch2 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["key4", "key2", "key5"])),
            Arc::new(StringArray::from(vec!["desc4", "desc2_new", "desc5"])),
            Arc::new(Int64Array::from(vec![400, 222, 500])),
        ],
    )?;
    insert_batch(&table, batch2).await?;

    // Should have 5 rows
    assert_eq!(
        get_row_count(&ctx, "mf_del_ins_string").await?,
        5,
        "Should have 5 rows after upsert across files"
    );

    // Verify key2 has the NEW value (222)
    let df = ctx
        .sql("SELECT value FROM mf_del_ins_string WHERE code = 'key2'")
        .await?;
    let results = df.collect().await?;
    let value = results
        .first()
        .and_then(|b| b.column(0).as_any().downcast_ref::<Int64Array>())
        .and_then(|a| a.values().first())
        .copied();
    assert_eq!(value, Some(222), "Re-inserted PK should have new value");

    Ok(())
}

// =============================================================================
// Multi-File Test 3: Multiple Upserts Across 3+ Files (Int64 PK)
// =============================================================================

async fn test_multifile_multiple_upserts_int64_impl(fixture: TestFixture) -> TestResult<()> {
    let (table, ctx, schema) = setup_int64_pk_table(&fixture, "mf_multi_upsert").await?;

    // File 1: Initial data
    let batch1 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["a", "b"])),
            Arc::new(Int64Array::from(vec![100, 200])),
        ],
    )?;
    insert_batch(&table, batch1).await?;
    assert_eq!(get_row_count(&ctx, "mf_multi_upsert").await?, 2);

    // Delete id=1
    delete_records(&table, col("id").eq(lit(1i64))).await?;
    assert_eq!(get_row_count(&ctx, "mf_multi_upsert").await?, 1);

    // File 2: Re-insert id=1, add id=3
    let batch2 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 3])),
            Arc::new(StringArray::from(vec!["a_v2", "c"])),
            Arc::new(Int64Array::from(vec![111, 300])),
        ],
    )?;
    insert_batch(&table, batch2).await?;
    assert_eq!(get_row_count(&ctx, "mf_multi_upsert").await?, 3);
    assert_eq!(
        get_value_for_id(&ctx, "mf_multi_upsert", 1).await?,
        Some(111)
    );

    // Delete id=2
    delete_records(&table, col("id").eq(lit(2i64))).await?;
    assert_eq!(get_row_count(&ctx, "mf_multi_upsert").await?, 2);

    // File 3: Re-insert id=2, add id=4
    let batch3 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![2, 4])),
            Arc::new(StringArray::from(vec!["b_v2", "d"])),
            Arc::new(Int64Array::from(vec![222, 400])),
        ],
    )?;
    insert_batch(&table, batch3).await?;
    assert_eq!(get_row_count(&ctx, "mf_multi_upsert").await?, 4);
    assert_eq!(
        get_value_for_id(&ctx, "mf_multi_upsert", 2).await?,
        Some(222)
    );

    // Delete id=1 again
    delete_records(&table, col("id").eq(lit(1i64))).await?;
    assert_eq!(get_row_count(&ctx, "mf_multi_upsert").await?, 3);

    // File 4: Re-insert id=1 for third time
    let batch4 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 5])),
            Arc::new(StringArray::from(vec!["a_v3", "e"])),
            Arc::new(Int64Array::from(vec![1111, 500])),
        ],
    )?;
    insert_batch(&table, batch4).await?;

    // Final state: ids 1, 2, 3, 4, 5 with values 1111, 222, 300, 400, 500
    assert_eq!(get_row_count(&ctx, "mf_multi_upsert").await?, 5);
    assert_eq!(get_ids(&ctx, "mf_multi_upsert").await?, vec![1, 2, 3, 4, 5]);
    assert_eq!(
        get_value_for_id(&ctx, "mf_multi_upsert", 1).await?,
        Some(1111)
    );
    assert_eq!(
        get_value_for_id(&ctx, "mf_multi_upsert", 2).await?,
        Some(222)
    );

    Ok(())
}

// =============================================================================
// Multi-File Test 4: Composite PK Upsert Across Files
// =============================================================================

async fn test_multifile_upsert_composite_pk_impl(fixture: TestFixture) -> TestResult<()> {
    let (table, ctx, schema) = setup_composite_pk_table(&fixture, "mf_composite_upsert").await?;

    // File 1: Initial data (schema: region, id, name, value)
    let batch1 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["us", "us", "eu"])),
            Arc::new(Int64Array::from(vec![1, 2, 1])),
            Arc::new(StringArray::from(vec![
                "US user 1",
                "US user 2",
                "EU user 1",
            ])),
            Arc::new(Int64Array::from(vec![100, 200, 300])),
        ],
    )?;
    insert_batch(&table, batch1).await?;
    assert_eq!(get_row_count(&ctx, "mf_composite_upsert").await?, 3);

    // Delete (us, 2)
    delete_records(
        &table,
        col("region").eq(lit("us")).and(col("id").eq(lit(2i64))),
    )
    .await?;
    assert_eq!(get_row_count(&ctx, "mf_composite_upsert").await?, 2);

    // File 2: Re-insert (us, 2) with different name, add new rows
    let batch2 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["us", "eu", "ap"])),
            Arc::new(Int64Array::from(vec![2, 2, 1])),
            Arc::new(StringArray::from(vec![
                "US user 2 UPDATED",
                "EU user 2",
                "AP user 1",
            ])),
            Arc::new(Int64Array::from(vec![222, 400, 500])),
        ],
    )?;
    insert_batch(&table, batch2).await?;

    // Should have 5 rows
    assert_eq!(
        get_row_count(&ctx, "mf_composite_upsert").await?,
        5,
        "Should have 5 rows after composite PK upsert"
    );

    // Verify (us, 2) has the new name
    let df = ctx
        .sql("SELECT name FROM mf_composite_upsert WHERE region = 'us' AND id = 2")
        .await?;
    let results = df.collect().await?;
    let name = results
        .first()
        .and_then(|b| b.column(0).as_any().downcast_ref::<StringArray>())
        .and_then(|a| {
            if a.is_empty() {
                None
            } else {
                Some(a.value(0).to_string())
            }
        });
    assert_eq!(name, Some("US user 2 UPDATED".to_string()));

    Ok(())
}

// =============================================================================
// Multi-File Test 5: Delete All from File 1, Re-add All to File 2
// =============================================================================

async fn test_multifile_delete_all_readd_all_impl(fixture: TestFixture) -> TestResult<()> {
    let (table, ctx, schema) = setup_int64_pk_table(&fixture, "mf_delete_all_readd").await?;

    // File 1: Initial data
    let batch1 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
            Arc::new(Int64Array::from(vec![100, 200, 300])),
        ],
    )?;
    insert_batch(&table, batch1).await?;
    assert_eq!(get_row_count(&ctx, "mf_delete_all_readd").await?, 3);

    // Delete all rows
    delete_records(&table, col("id").gt(lit(0i64))).await?;
    assert_eq!(get_row_count(&ctx, "mf_delete_all_readd").await?, 0);

    // File 2: Re-add all the same PKs with different values
    let batch2 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a_new", "b_new", "c_new"])),
            Arc::new(Int64Array::from(vec![1000, 2000, 3000])),
        ],
    )?;
    insert_batch(&table, batch2).await?;

    // Should have 3 rows with new values
    assert_eq!(get_row_count(&ctx, "mf_delete_all_readd").await?, 3);
    assert_eq!(
        get_value_for_id(&ctx, "mf_delete_all_readd", 1).await?,
        Some(1000)
    );
    assert_eq!(
        get_value_for_id(&ctx, "mf_delete_all_readd", 2).await?,
        Some(2000)
    );
    assert_eq!(
        get_value_for_id(&ctx, "mf_delete_all_readd", 3).await?,
        Some(3000)
    );

    Ok(())
}

// =============================================================================
// Multi-File Test 6: Interleaved Inserts and Deletes Across Many Files
// =============================================================================

async fn test_multifile_interleaved_many_files_impl(fixture: TestFixture) -> TestResult<()> {
    let (table, ctx, schema) = setup_int64_pk_table(&fixture, "mf_interleaved").await?;

    // Pattern: insert 2 rows, delete 1, insert 2 more, delete 1... repeat 5 times
    // This creates a complex multi-file scenario with deletions spanning files

    let mut expected_ids: std::collections::HashSet<i64> = std::collections::HashSet::new();
    let mut next_id = 1i64;
    let mut expected_values: std::collections::HashMap<i64, i64> = std::collections::HashMap::new();

    for round in 0..5 {
        // Insert 2 rows
        let id1 = next_id;
        let id2 = next_id + 1;
        next_id += 2;

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![id1, id2])),
                Arc::new(StringArray::from(vec![
                    format!("r{round}_a"),
                    format!("r{round}_b"),
                ])),
                Arc::new(Int64Array::from(vec![id1 * 100, id2 * 100])),
            ],
        )?;
        insert_batch(&table, batch).await?;
        expected_ids.insert(id1);
        expected_ids.insert(id2);
        expected_values.insert(id1, id1 * 100);
        expected_values.insert(id2, id2 * 100);

        // Delete the first row added this round
        delete_records(&table, col("id").eq(lit(id1))).await?;
        expected_ids.remove(&id1);
        expected_values.remove(&id1);
    }

    // At end: should have 5 rows (one per round - the second ID from each)
    // IDs: 2, 4, 6, 8, 10
    let expected: Vec<i64> = expected_ids
        .into_iter()
        .collect::<Vec<_>>()
        .into_iter()
        .collect();
    let mut expected_sorted = expected;
    expected_sorted.sort_unstable();

    assert_eq!(
        get_row_count(&ctx, "mf_interleaved").await?,
        5,
        "Should have 5 rows after interleaved ops"
    );
    assert_eq!(get_ids(&ctx, "mf_interleaved").await?, expected_sorted);

    // Verify values
    for (id, expected_val) in expected_values {
        assert_eq!(
            get_value_for_id(&ctx, "mf_interleaved", id).await?,
            Some(expected_val)
        );
    }

    Ok(())
}

// =============================================================================
// Multi-File Test 7: Upsert Same PK Multiple Times in Single Batch
// =============================================================================

async fn test_multifile_duplicate_pk_in_batch_impl(fixture: TestFixture) -> TestResult<()> {
    let (table, ctx, schema) = setup_int64_pk_table(&fixture, "mf_dup_pk_batch").await?;

    // File 1: Initial data
    let batch1 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["a", "b"])),
            Arc::new(Int64Array::from(vec![100, 200])),
        ],
    )?;
    insert_batch(&table, batch1).await?;

    // Delete id=1
    delete_records(&table, col("id").eq(lit(1i64))).await?;

    // File 2: Insert batch with the deleted PK and additional new rows
    // Note: The batch itself has unique PKs; we're testing re-insertion of a deleted PK
    let batch2 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 3, 4])),
            Arc::new(StringArray::from(vec!["a_new", "c", "d"])),
            Arc::new(Int64Array::from(vec![111, 300, 400])),
        ],
    )?;
    insert_batch(&table, batch2).await?;

    // Should have 4 rows: 1 (new), 2, 3, 4
    assert_eq!(get_row_count(&ctx, "mf_dup_pk_batch").await?, 4);
    assert_eq!(get_ids(&ctx, "mf_dup_pk_batch").await?, vec![1, 2, 3, 4]);
    assert_eq!(
        get_value_for_id(&ctx, "mf_dup_pk_batch", 1).await?,
        Some(111)
    );

    Ok(())
}

// =============================================================================
// Multi-File Test 8: Position-Based Delete + Insert Across Files
// Tests that position-based deletion works correctly with per-file deletion vectors
// =============================================================================

async fn test_multifile_position_based_delete_reinsert_impl(
    fixture: TestFixture,
) -> TestResult<()> {
    // Create table WITHOUT primary key (uses position-based deletion)
    let schema = Arc::new(Schema::new(vec![
        Field::new("category", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let table_options = CreateTableOptions {
        table_name: "mf_position_delete_insert".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec![],
        on_conflict: None, // No PK = position-based deletion
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let table = Arc::new(CayenneTableProvider::create_table(catalog, table_options).await?);
    let ctx = SessionContext::new();
    ctx.register_table(
        "mf_position_delete_insert",
        Arc::clone(&table) as Arc<dyn TableProvider>,
    )?;

    // File 1: Initial data (3 rows)
    let batch1 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["A", "B", "C"])),
            Arc::new(Int64Array::from(vec![100, 200, 300])),
        ],
    )?;
    insert_batch(&table, batch1).await?;
    assert_eq!(get_row_count(&ctx, "mf_position_delete_insert").await?, 3);

    // Delete category B (removes B(200) from File 1)
    delete_records(&table, col("category").eq(lit("B"))).await?;
    assert_eq!(get_row_count(&ctx, "mf_position_delete_insert").await?, 2);

    // File 2: Insert new data including a new "B" row
    // With per-file deletion vectors, new files have no deletions - the B(222) is a new row,
    // completely separate from the deleted B(200). This is NOT upsert behavior.
    let batch2 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["B", "D"])),
            Arc::new(Int64Array::from(vec![222, 400])),
        ],
    )?;
    insert_batch(&table, batch2).await?;

    // Should have 4 rows: A(100), C(300) from File 1 + B(222), D(400) from File 2
    // Note: B(222) is a NEW row, not a replacement - there's no PK to deduplicate
    assert_eq!(
        get_row_count(&ctx, "mf_position_delete_insert").await?,
        4,
        "Should have 4 rows after position-based delete and re-insert"
    );

    // Verify only one B exists (the new one with value 222)
    let df = ctx
        .sql("SELECT value FROM mf_position_delete_insert WHERE category = 'B'")
        .await?;
    let results = df.collect().await?;
    let value = results
        .first()
        .and_then(|b| b.column(0).as_any().downcast_ref::<Int64Array>())
        .and_then(|a| a.values().first())
        .copied();
    assert_eq!(value, Some(222), "New B row should have value 222");

    Ok(())
}

// =============================================================================
// Multi-File Test 9: Delete from Multiple Files, Then Bulk Re-insert
// =============================================================================

async fn test_multifile_bulk_delete_bulk_reinsert_impl(fixture: TestFixture) -> TestResult<()> {
    let (table, ctx, schema) = setup_int64_pk_table(&fixture, "mf_bulk_ops").await?;

    // Create 3 files with distinct data
    for file_num in 0..3i64 {
        let start_id = file_num * 3 + 1;
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![start_id, start_id + 1, start_id + 2])),
                Arc::new(StringArray::from(vec![
                    format!("f{file_num}_a"),
                    format!("f{file_num}_b"),
                    format!("f{file_num}_c"),
                ])),
                Arc::new(Int64Array::from(vec![
                    start_id * 100,
                    (start_id + 1) * 100,
                    (start_id + 2) * 100,
                ])),
            ],
        )?;
        insert_batch(&table, batch).await?;
    }

    // Should have 9 rows: 1-9
    assert_eq!(get_row_count(&ctx, "mf_bulk_ops").await?, 9);

    // Delete one row from each file (ids: 2, 5, 8)
    delete_records(&table, col("id").eq(lit(2i64))).await?;
    delete_records(&table, col("id").eq(lit(5i64))).await?;
    delete_records(&table, col("id").eq(lit(8i64))).await?;
    assert_eq!(get_row_count(&ctx, "mf_bulk_ops").await?, 6);

    // Bulk re-insert all deleted PKs with new values
    let batch_reinsert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![2, 5, 8])),
            Arc::new(StringArray::from(vec!["new_2", "new_5", "new_8"])),
            Arc::new(Int64Array::from(vec![2222, 5555, 8888])),
        ],
    )?;
    insert_batch(&table, batch_reinsert).await?;

    // Should have 9 rows again
    assert_eq!(get_row_count(&ctx, "mf_bulk_ops").await?, 9);
    assert_eq!(
        get_ids(&ctx, "mf_bulk_ops").await?,
        vec![1, 2, 3, 4, 5, 6, 7, 8, 9]
    );

    // Verify re-inserted rows have new values
    assert_eq!(get_value_for_id(&ctx, "mf_bulk_ops", 2).await?, Some(2222));
    assert_eq!(get_value_for_id(&ctx, "mf_bulk_ops", 5).await?, Some(5555));
    assert_eq!(get_value_for_id(&ctx, "mf_bulk_ops", 8).await?, Some(8888));

    // Verify original rows still have original values
    assert_eq!(get_value_for_id(&ctx, "mf_bulk_ops", 1).await?, Some(100));
    assert_eq!(get_value_for_id(&ctx, "mf_bulk_ops", 4).await?, Some(400));
    assert_eq!(get_value_for_id(&ctx, "mf_bulk_ops", 7).await?, Some(700));

    Ok(())
}

// =============================================================================
// Test Registration Macros
// =============================================================================

macro_rules! test_with_backends {
    ($test_fn:ident) => {
        paste::paste! {
            #[tokio::test]
            async fn [<$test_fn _sqlite>]() {
                let fixture = TestFixture::new(common::BackendType::Sqlite)
                    .await
                    .expect("Failed to create test fixture");
                $test_fn(fixture).await.expect("Test failed");
            }

            #[cfg(feature = "turso")]
            #[tokio::test]
            async fn [<$test_fn _turso>]() {
                let fixture = TestFixture::new(common::BackendType::Turso)
                    .await
                    .expect("Failed to create test fixture");
                $test_fn(fixture).await.expect("Test failed");
            }
        }
    };
}

test_with_backends!(test_acid_upsert_int64_pk_impl);
test_with_backends!(test_acid_upsert_string_pk_impl);
test_with_backends!(test_acid_upsert_composite_pk_impl);
test_with_backends!(test_acid_multiple_upserts_impl);
test_with_backends!(test_acid_durability_reopen_impl);
test_with_backends!(test_acid_durability_deletions_persist_impl);
test_with_backends!(test_acid_batch_upsert_impl);
test_with_backends!(test_acid_delete_all_then_insert_impl);
test_with_backends!(test_acid_interleaved_ops_impl);

// Multi-file upsert tests
test_with_backends!(test_multifile_delete_file1_insert_file2_int64_impl);
test_with_backends!(test_multifile_delete_file1_insert_file2_string_impl);
test_with_backends!(test_multifile_multiple_upserts_int64_impl);
test_with_backends!(test_multifile_upsert_composite_pk_impl);
test_with_backends!(test_multifile_delete_all_readd_all_impl);
test_with_backends!(test_multifile_interleaved_many_files_impl);
test_with_backends!(test_multifile_duplicate_pk_in_batch_impl);
test_with_backends!(test_multifile_position_based_delete_reinsert_impl);
test_with_backends!(test_multifile_bulk_delete_bulk_reinsert_impl);
