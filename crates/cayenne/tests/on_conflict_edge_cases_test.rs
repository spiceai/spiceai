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

#![allow(clippy::expect_used)]
#![allow(clippy::clone_on_ref_ptr)]

//! Edge case tests for delete and on-conflict handling in Cayenne.
//!
//! These tests cover:
//! - Delete followed by insert of the same key (DELETE + INSERT pattern, not upsert)
//! - On-conflict upsert with single batch conflict
//! - `DoNothing` vs `Upsert` behavior
//! - On-conflict with composite primary keys
//! - String primary key upserts
//! - Empty batch handling

mod common;

use std::sync::Arc;

use arrow::array::Int64Array;

use arrow::datatypes::{DataType, Field, Schema};

use arrow::record_batch::RecordBatch;

use cayenne::metadata::CreateTableOptions;

use cayenne::{CayenneTableProvider, MetadataCatalog};

use data_components::delete::DeletionTableProvider;

use datafusion::prelude::{col, lit, Expr, SessionContext};

use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};

/// Helper to insert a batch into a table
async fn insert_batch(
    table: &Arc<CayenneTableProvider>,
    batch: RecordBatch,
) -> Result<u64, Box<dyn std::error::Error>> {
    common::insert_batch(table.as_ref(), batch)
        .await
        .map_err(Into::into)
}

/// Helper to delete records from a table using the `DeletionTableProvider` API
async fn delete_records(
    table: &Arc<CayenneTableProvider>,
    filter: Expr,
) -> Result<u64, Box<dyn std::error::Error>> {
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

// =============================================================================
// Test 1: Delete then Insert DIFFERENT Key (DELETE + INSERT pattern)
// Tests that DELETE doesn't affect subsequent INSERTs of different keys
// =============================================================================
test_with_backends!(test_delete_then_insert_different_key_impl);

async fn test_delete_then_insert_different_key_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let table_options = CreateTableOptions {
        table_name: "delete_insert".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["id".to_string()],
        on_conflict: None, // No on-conflict config
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    let catalog_arc: Arc<dyn MetadataCatalog> = fixture.catalog.clone();
    let table = CayenneTableProvider::create_table(catalog_arc, table_options).await?;
    let table = Arc::new(table);

    let ctx = SessionContext::new();
    ctx.register_table(
        "delete_insert",
        Arc::clone(&table) as Arc<dyn datafusion::datasource::TableProvider>,
    )?;

    // Insert initial data
    ctx.sql(
        "INSERT INTO delete_insert VALUES (1, 'Alice', 100), (2, 'Bob', 200), (3, 'Carol', 300)",
    )
    .await?
    .collect()
    .await?;

    // Delete row with id=2 using proper API
    let deleted = delete_records(&table, col("id").eq(lit(2i64))).await?;
    assert_eq!(deleted, 1, "Should delete exactly 1 row");

    // Insert with DIFFERENT id=4 - should succeed
    ctx.sql("INSERT INTO delete_insert VALUES (4, 'Dave', 400)")
        .await?
        .collect()
        .await?;

    let results = ctx
        .sql("SELECT id, name, value FROM delete_insert ORDER BY id")
        .await?
        .collect()
        .await?;

    assert_eq!(results.len(), 1);
    let batch = &results[0];
    assert_eq!(
        batch.num_rows(),
        3,
        "Should have 3 rows (deleted 1, inserted 1)"
    );

    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("id column");

    // Should have ids 1, 3, 4 (id=2 was deleted)
    assert_eq!(ids.value(0), 1);
    assert_eq!(ids.value(1), 3);
    assert_eq!(ids.value(2), 4);

    Ok(())
}

// =============================================================================
// Test 2: Single Upsert Operation
// =============================================================================
test_with_backends!(test_single_upsert_impl);

async fn test_single_upsert_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let table_options = CreateTableOptions {
        table_name: "single_upsert".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["id".to_string()],
        on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
            "id".to_string()
        ]))),
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    let catalog_arc: Arc<dyn MetadataCatalog> = fixture.catalog.clone();
    let table = CayenneTableProvider::create_table(catalog_arc, table_options).await?;
    let table = Arc::new(table);

    let ctx = SessionContext::new();
    ctx.register_table(
        "single_upsert",
        Arc::clone(&table) as Arc<dyn datafusion::datasource::TableProvider>,
    )?;

    // Initial insert
    ctx.sql("INSERT INTO single_upsert VALUES (1, 100), (2, 200)")
        .await?
        .collect()
        .await?;

    // Single upsert of existing key
    ctx.sql("INSERT INTO single_upsert VALUES (1, 999)")
        .await?
        .collect()
        .await?;

    let results = ctx
        .sql("SELECT id, value FROM single_upsert ORDER BY id")
        .await?
        .collect()
        .await?;

    assert_eq!(results[0].num_rows(), 2, "Should have 2 rows after upsert");

    let values = results[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("value column");

    assert_eq!(values.value(0), 999, "id=1 should have upserted value 999");
    assert_eq!(values.value(1), 200, "id=2 should remain 200");

    Ok(())
}

// =============================================================================
// Test 3: DoNothing Conflict Behavior (Drop Conflicts)
// =============================================================================
test_with_backends!(test_do_nothing_drops_conflicts_impl);

async fn test_do_nothing_drops_conflicts_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let table_options = CreateTableOptions {
        table_name: "do_nothing".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["id".to_string()],
        on_conflict: Some(OnConflict::DoNothing(ColumnReference::new(vec![
            "id".to_string()
        ]))),
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    let catalog_arc: Arc<dyn MetadataCatalog> = fixture.catalog.clone();
    let table = CayenneTableProvider::create_table(catalog_arc, table_options).await?;
    let table = Arc::new(table);

    let ctx = SessionContext::new();
    ctx.register_table(
        "do_nothing",
        Arc::clone(&table) as Arc<dyn datafusion::datasource::TableProvider>,
    )?;

    // Insert initial data
    ctx.sql("INSERT INTO do_nothing VALUES (1, 100), (2, 200)")
        .await?
        .collect()
        .await?;

    // Try to insert conflicting row - should be dropped (DoNothing)
    ctx.sql("INSERT INTO do_nothing VALUES (1, 999), (3, 300)")
        .await?
        .collect()
        .await?;

    let results = ctx
        .sql("SELECT id, value FROM do_nothing ORDER BY id")
        .await?
        .collect()
        .await?;

    assert_eq!(
        results[0].num_rows(),
        3,
        "Should have 3 rows (id=1 conflict dropped, id=3 inserted)"
    );

    let values = results[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("value column");

    // id=1 should retain original value (conflict dropped)
    assert_eq!(
        values.value(0),
        100,
        "id=1 should retain original value 100"
    );
    assert_eq!(values.value(1), 200, "id=2 should be 200");
    assert_eq!(values.value(2), 300, "id=3 should be 300");

    Ok(())
}

// =============================================================================
// Test 4: On-Conflict with Composite Primary Key
// =============================================================================
test_with_backends!(test_upsert_composite_pk_impl);

async fn test_upsert_composite_pk_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("region", DataType::Utf8, false),
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let table_options = CreateTableOptions {
        table_name: "composite_upsert".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["region".to_string(), "id".to_string()],
        on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
            "region".to_string(),
            "id".to_string(),
        ]))),
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    let catalog_arc: Arc<dyn MetadataCatalog> = fixture.catalog.clone();
    let table = CayenneTableProvider::create_table(catalog_arc, table_options).await?;
    let table = Arc::new(table);

    let ctx = SessionContext::new();
    ctx.register_table(
        "composite_upsert",
        Arc::clone(&table) as Arc<dyn datafusion::datasource::TableProvider>,
    )?;

    // Insert initial data
    ctx.sql("INSERT INTO composite_upsert VALUES ('US', 1, 100), ('EU', 1, 200), ('US', 2, 300)")
        .await?
        .collect()
        .await?;

    // Upsert ('US', 1) - should update
    ctx.sql("INSERT INTO composite_upsert VALUES ('US', 1, 999)")
        .await?
        .collect()
        .await?;

    let results = ctx
        .sql("SELECT region, id, value FROM composite_upsert ORDER BY region, id")
        .await?
        .collect()
        .await?;

    assert_eq!(results[0].num_rows(), 3);

    let values = results[0]
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("value column");

    // ('EU', 1) should be 200, ('US', 1) should be 999, ('US', 2) should be 300
    assert_eq!(values.value(0), 200, "EU,1 should be 200");
    assert_eq!(values.value(1), 999, "US,1 should be upserted to 999");
    assert_eq!(values.value(2), 300, "US,2 should be 300");

    Ok(())
}

// =============================================================================
// Test 5: Upsert with Large Batch
// =============================================================================
test_with_backends!(test_upsert_large_batch_impl);

async fn test_upsert_large_batch_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let table_options = CreateTableOptions {
        table_name: "large_batch".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["id".to_string()],
        on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
            "id".to_string()
        ]))),
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    let catalog_arc: Arc<dyn MetadataCatalog> = fixture.catalog.clone();
    let table = CayenneTableProvider::create_table(catalog_arc, table_options).await?;
    let table = Arc::new(table);

    let ctx = SessionContext::new();
    ctx.register_table(
        "large_batch",
        Arc::clone(&table) as Arc<dyn datafusion::datasource::TableProvider>,
    )?;

    // Insert 100 rows using SQL (builds VALUES clause)
    let mut values: Vec<String> = Vec::with_capacity(100);
    for i in 1..=100 {
        values.push(format!("({}, {})", i, i * 10));
    }
    let insert_sql = format!("INSERT INTO large_batch VALUES {}", values.join(", "));
    ctx.sql(&insert_sql).await?.collect().await?;

    // Verify count
    let results = ctx
        .sql("SELECT COUNT(*) FROM large_batch")
        .await?
        .collect()
        .await?;
    let count = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count")
        .value(0);
    assert_eq!(count, 100, "Should have 100 rows");

    // Upsert first 50 rows with new values using SQL
    let mut upsert_values: Vec<String> = Vec::with_capacity(50);
    for i in 1..=50 {
        upsert_values.push(format!("({}, {})", i, i * 100));
    }
    let upsert_sql = format!(
        "INSERT INTO large_batch VALUES {}",
        upsert_values.join(", ")
    );
    ctx.sql(&upsert_sql).await?.collect().await?;

    // Verify first row was upserted
    let results = ctx
        .sql("SELECT value FROM large_batch WHERE id = 1")
        .await?
        .collect()
        .await?;
    let value = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("value")
        .value(0);
    assert_eq!(value, 100, "id=1 should have value 100 after upsert");

    // Verify row 51 was not upserted (retained original value)
    let results = ctx
        .sql("SELECT value FROM large_batch WHERE id = 51")
        .await?
        .collect()
        .await?;
    let value = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("value")
        .value(0);
    assert_eq!(value, 510, "id=51 should retain original value 510");

    Ok(())
}

// =============================================================================
// Test 6: Delete Non-Existent Then Insert
// =============================================================================
test_with_backends!(test_delete_nonexistent_then_insert_impl);

async fn test_delete_nonexistent_then_insert_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let table_options = CreateTableOptions {
        table_name: "delete_nonexistent".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["id".to_string()],
        on_conflict: None, // No on-conflict config
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    let catalog_arc: Arc<dyn MetadataCatalog> = fixture.catalog.clone();
    let table = CayenneTableProvider::create_table(catalog_arc, table_options).await?;
    let table = Arc::new(table);

    let ctx = SessionContext::new();
    ctx.register_table(
        "delete_nonexistent",
        Arc::clone(&table) as Arc<dyn datafusion::datasource::TableProvider>,
    )?;

    // Insert initial data
    ctx.sql("INSERT INTO delete_nonexistent VALUES (1, 'Alice')")
        .await?
        .collect()
        .await?;

    // Delete non-existent row (should be no-op)
    let deleted = delete_records(&table, col("id").eq(lit(999i64))).await?;
    assert_eq!(deleted, 0, "Should delete 0 rows (non-existent)");

    // Insert with id=999 - should succeed
    ctx.sql("INSERT INTO delete_nonexistent VALUES (999, 'New')")
        .await?
        .collect()
        .await?;

    let results = ctx
        .sql("SELECT COUNT(*) FROM delete_nonexistent")
        .await?
        .collect()
        .await?;
    let count = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count")
        .value(0);
    assert_eq!(count, 2, "Should have 2 rows");

    Ok(())
}

// =============================================================================
// Test 7: String Primary Key Upsert
// =============================================================================
test_with_backends!(test_string_pk_upsert_impl);

async fn test_string_pk_upsert_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("code", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let table_options = CreateTableOptions {
        table_name: "string_pk".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["code".to_string()],
        on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
            "code".to_string()
        ]))),
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    let catalog_arc: Arc<dyn MetadataCatalog> = fixture.catalog.clone();
    let table = CayenneTableProvider::create_table(catalog_arc, table_options).await?;
    let table = Arc::new(table);

    let ctx = SessionContext::new();
    ctx.register_table(
        "string_pk",
        Arc::clone(&table) as Arc<dyn datafusion::datasource::TableProvider>,
    )?;

    // Insert initial data
    ctx.sql("INSERT INTO string_pk VALUES ('ABC', 100), ('DEF', 200)")
        .await?
        .collect()
        .await?;

    // Upsert with same key
    ctx.sql("INSERT INTO string_pk VALUES ('ABC', 999)")
        .await?
        .collect()
        .await?;

    let results = ctx
        .sql("SELECT code, value FROM string_pk ORDER BY code")
        .await?
        .collect()
        .await?;

    assert_eq!(results[0].num_rows(), 2);

    let values = results[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("value column");

    assert_eq!(values.value(0), 999, "ABC should be upserted to 999");
    assert_eq!(values.value(1), 200, "DEF should be 200");

    Ok(())
}

// =============================================================================
// Test 8: Delete All Then Insert New Keys
// =============================================================================
test_with_backends!(test_delete_all_then_insert_new_keys_impl);

async fn test_delete_all_then_insert_new_keys_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let table_options = CreateTableOptions {
        table_name: "delete_all_insert".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["id".to_string()],
        on_conflict: None, // No on-conflict - pure DELETE + INSERT
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    let catalog_arc: Arc<dyn MetadataCatalog> = fixture.catalog.clone();
    let table = CayenneTableProvider::create_table(catalog_arc, table_options).await?;
    let table = Arc::new(table);

    let ctx = SessionContext::new();
    ctx.register_table(
        "delete_all_insert",
        Arc::clone(&table) as Arc<dyn datafusion::datasource::TableProvider>,
    )?;

    // Insert initial data
    ctx.sql("INSERT INTO delete_all_insert VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')")
        .await?
        .collect()
        .await?;

    // Delete all rows
    let deleted = delete_records(&table, col("id").gt(lit(0i64))).await?;
    assert_eq!(deleted, 3, "Should delete 3 rows");

    // Verify empty
    let results = ctx
        .sql("SELECT COUNT(*) FROM delete_all_insert")
        .await?
        .collect()
        .await?;
    let count = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count")
        .value(0);
    assert_eq!(count, 0, "Table should be empty after delete all");

    // Insert with NEW keys (different from deleted ones) - should work
    ctx.sql("INSERT INTO delete_all_insert VALUES (10, 'New_Alice'), (20, 'New_Bob')")
        .await?
        .collect()
        .await?;

    let results = ctx
        .sql("SELECT id, name FROM delete_all_insert ORDER BY id")
        .await?
        .collect()
        .await?;

    assert_eq!(results[0].num_rows(), 2);
    let ids = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("id column");

    assert_eq!(ids.value(0), 10);
    assert_eq!(ids.value(1), 20);

    Ok(())
}

// =============================================================================
// Test 9: Mixed Non-Conflicting and Conflicting Keys in Single Batch
// =============================================================================
test_with_backends!(test_mixed_conflict_batch_impl);

async fn test_mixed_conflict_batch_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let table_options = CreateTableOptions {
        table_name: "mixed_batch".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["id".to_string()],
        on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
            "id".to_string()
        ]))),
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    let catalog_arc: Arc<dyn MetadataCatalog> = fixture.catalog.clone();
    let table = CayenneTableProvider::create_table(catalog_arc, table_options).await?;
    let table = Arc::new(table);

    let ctx = SessionContext::new();
    ctx.register_table(
        "mixed_batch",
        Arc::clone(&table) as Arc<dyn datafusion::datasource::TableProvider>,
    )?;

    // Insert initial data: ids 1, 2, 3
    ctx.sql("INSERT INTO mixed_batch VALUES (1, 100), (2, 200), (3, 300)")
        .await?
        .collect()
        .await?;

    // Insert batch with:
    // - id=1 (conflict, should upsert to 111)
    // - id=4 (new, should insert)
    // - id=2 (conflict, should upsert to 222)
    // - id=5 (new, should insert)
    ctx.sql("INSERT INTO mixed_batch VALUES (1, 111), (4, 400), (2, 222), (5, 500)")
        .await?
        .collect()
        .await?;

    let results = ctx
        .sql("SELECT id, value FROM mixed_batch ORDER BY id")
        .await?
        .collect()
        .await?;

    assert_eq!(results[0].num_rows(), 5, "Should have 5 rows total");

    let ids = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("id");
    let values = results[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("value");

    // Verify values
    assert_eq!(ids.value(0), 1);
    assert_eq!(values.value(0), 111, "id=1 should be upserted to 111");

    assert_eq!(ids.value(1), 2);
    assert_eq!(values.value(1), 222, "id=2 should be upserted to 222");

    assert_eq!(ids.value(2), 3);
    assert_eq!(values.value(2), 300, "id=3 should remain 300");

    assert_eq!(ids.value(3), 4);
    assert_eq!(values.value(3), 400, "id=4 should be inserted as 400");

    assert_eq!(ids.value(4), 5);
    assert_eq!(values.value(4), 500, "id=5 should be inserted as 500");

    Ok(())
}

// =============================================================================
// Test 10: Empty Batch Insert (no-op)
// =============================================================================
test_with_backends!(test_empty_batch_impl);

async fn test_empty_batch_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let table_options = CreateTableOptions {
        table_name: "empty_batch".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["id".to_string()],
        on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
            "id".to_string()
        ]))),
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    let catalog_arc: Arc<dyn MetadataCatalog> = fixture.catalog.clone();
    let table = CayenneTableProvider::create_table(catalog_arc, table_options).await?;
    let table = Arc::new(table);

    let ctx = SessionContext::new();
    ctx.register_table(
        "empty_batch",
        Arc::clone(&table) as Arc<dyn datafusion::datasource::TableProvider>,
    )?;

    // Insert initial data
    ctx.sql("INSERT INTO empty_batch VALUES (1, 'Alice')")
        .await?
        .collect()
        .await?;

    // Insert empty batch
    let empty_batch = RecordBatch::new_empty(Arc::clone(&schema));
    let _ = insert_batch(&table, empty_batch).await;

    // Data should be unchanged
    let results = ctx
        .sql("SELECT COUNT(*) FROM empty_batch")
        .await?
        .collect()
        .await?;
    let count = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count")
        .value(0);
    assert_eq!(count, 1, "Row count should still be 1");

    Ok(())
}
