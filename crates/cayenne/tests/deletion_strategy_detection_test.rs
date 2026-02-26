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

//! Integration tests for proper detection of deletion vector strategies.
//!
//! Cayenne supports three deletion strategies:
//! 1. `Int64Pk`: Single-column Int64 primary key using `HashSet<i64>` - most efficient
//! 2. `RowConverterBased` (key-based): Composite or non-integer primary keys using `RowConverter`
//! 3. `PositionBased`: Tables without primary keys using `RoaringBitmap`
//!
//! These tests verify that the correct strategy is selected based on table schema.

#![allow(clippy::expect_used)]

mod common;

use arrow::array::{Int32Array, Int64Array, RecordBatch, StringArray};

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

fn get_catalog(fixture: &TestFixture) -> Arc<dyn MetadataCatalog> {
    Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>
}

async fn insert_batch(table: &Arc<CayenneTableProvider>, batch: RecordBatch) -> TestResult<u64> {
    common::insert_batch(table.as_ref(), batch)
        .await
        .map_err(Into::into)
}

// =============================================================================
// Test Strategy Detection by Schema Configuration
// =============================================================================

/// Test that `Int64Pk` strategy is selected for single Int64 primary key.
async fn test_detects_int64_pk_strategy_impl(fixture: TestFixture) -> TestResult<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false), // Single Int64 PK
        Field::new("value", DataType::Utf8, false),
    ]));

    let table_options = CreateTableOptions {
        table_name: "int64_pk_table".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["id".to_string()],
        on_conflict: None, // Single Int64 PK -> Int64Pk strategy
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    let table =
        Arc::new(CayenneTableProvider::create_table(get_catalog(&fixture), table_options).await?);

    // Insert data
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])),
        ],
    )?;
    insert_batch(&table, batch).await?;

    // Delete and verify it works correctly (Int64Pk strategy)
    let ctx = SessionContext::new();
    let filter = col("id").eq(lit(2i64));
    let plan = table.delete_from(&ctx.state(), &[filter]).await?;
    let results = datafusion_physical_plan::collect(plan, ctx.task_ctx()).await?;
    let deleted = results
        .first()
        .and_then(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<arrow::array::UInt64Array>()
        })
        .and_then(|a| a.values().first())
        .copied()
        .unwrap_or(0);

    assert_eq!(
        deleted, 1,
        "Should delete exactly 1 row with Int64Pk strategy"
    );

    // Verify remaining data
    ctx.register_table(
        "int64_pk_table",
        Arc::clone(&table) as Arc<dyn TableProvider>,
    )?;
    let df = ctx
        .sql("SELECT COUNT(*) as cnt FROM int64_pk_table")
        .await?;
    let results = df.collect().await?;
    let count = results
        .first()
        .and_then(|b| b.column(0).as_any().downcast_ref::<Int64Array>())
        .and_then(|a| a.values().first())
        .copied()
        .unwrap_or(0);

    assert_eq!(count, 4);

    Ok(())
}

test_with_backends!(test_detects_int64_pk_strategy_impl);

/// Test that `RowConverter`-based strategy is selected for String primary key.
async fn test_detects_rowconverter_strategy_for_string_pk_impl(
    fixture: TestFixture,
) -> TestResult<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("code", DataType::Utf8, false), // String PK
        Field::new("value", DataType::Int64, false),
    ]));

    let table_options = CreateTableOptions {
        table_name: "string_pk_table".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["code".to_string()],
        on_conflict: None, // String PK -> RowConverter strategy
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    let table =
        Arc::new(CayenneTableProvider::create_table(get_catalog(&fixture), table_options).await?);

    // Insert data
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["A", "B", "C", "D"])),
            Arc::new(Int64Array::from(vec![100, 200, 300, 400])),
        ],
    )?;
    insert_batch(&table, batch).await?;

    // Delete and verify
    let ctx = SessionContext::new();
    let filter = col("code").eq(lit("B"));
    let plan = table.delete_from(&ctx.state(), &[filter]).await?;
    let results = datafusion_physical_plan::collect(plan, ctx.task_ctx()).await?;
    let deleted = results
        .first()
        .and_then(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<arrow::array::UInt64Array>()
        })
        .and_then(|a| a.values().first())
        .copied()
        .unwrap_or(0);

    assert_eq!(
        deleted, 1,
        "Should delete exactly 1 row with RowConverter strategy"
    );

    ctx.register_table(
        "string_pk_table",
        Arc::clone(&table) as Arc<dyn TableProvider>,
    )?;
    let df = ctx
        .sql("SELECT COUNT(*) as cnt FROM string_pk_table")
        .await?;
    let results = df.collect().await?;
    let count = results
        .first()
        .and_then(|b| b.column(0).as_any().downcast_ref::<Int64Array>())
        .and_then(|a| a.values().first())
        .copied()
        .unwrap_or(0);

    assert_eq!(count, 3);

    Ok(())
}

test_with_backends!(test_detects_rowconverter_strategy_for_string_pk_impl);

/// Test that `RowConverter`-based strategy is selected for composite primary key.
async fn test_detects_rowconverter_strategy_for_composite_pk_impl(
    fixture: TestFixture,
) -> TestResult<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("region", DataType::Utf8, false),
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let table_options = CreateTableOptions {
        table_name: "composite_pk_table".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["region".to_string(), "id".to_string()],
        on_conflict: None, // Composite PK -> RowConverter
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    let table =
        Arc::new(CayenneTableProvider::create_table(get_catalog(&fixture), table_options).await?);

    // Insert data
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["US", "US", "EU", "EU"])),
            Arc::new(Int64Array::from(vec![1, 2, 1, 2])),
            Arc::new(Int64Array::from(vec![100, 200, 300, 400])),
        ],
    )?;
    insert_batch(&table, batch).await?;

    // Delete with composite key
    let ctx = SessionContext::new();
    let filter = col("region").eq(lit("US")).and(col("id").eq(lit(1i64)));
    let plan = table.delete_from(&ctx.state(), &[filter]).await?;
    let results = datafusion_physical_plan::collect(plan, ctx.task_ctx()).await?;
    let deleted = results
        .first()
        .and_then(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<arrow::array::UInt64Array>()
        })
        .and_then(|a| a.values().first())
        .copied()
        .unwrap_or(0);

    assert_eq!(
        deleted, 1,
        "Should delete exactly 1 row with composite key RowConverter strategy"
    );

    ctx.register_table(
        "composite_pk_table",
        Arc::clone(&table) as Arc<dyn TableProvider>,
    )?;
    let df = ctx
        .sql("SELECT COUNT(*) as cnt FROM composite_pk_table")
        .await?;
    let results = df.collect().await?;
    let count = results
        .first()
        .and_then(|b| b.column(0).as_any().downcast_ref::<Int64Array>())
        .and_then(|a| a.values().first())
        .copied()
        .unwrap_or(0);

    assert_eq!(count, 3);

    Ok(())
}

test_with_backends!(test_detects_rowconverter_strategy_for_composite_pk_impl);

/// Test that `PositionBased` strategy is selected for tables without primary key.
async fn test_detects_position_based_strategy_impl(fixture: TestFixture) -> TestResult<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let table_options = CreateTableOptions {
        table_name: "no_pk_table".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec![],
        on_conflict: None, // No PK -> PositionBased strategy
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    let table =
        Arc::new(CayenneTableProvider::create_table(get_catalog(&fixture), table_options).await?);

    // Insert data
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])),
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
        ],
    )?;
    insert_batch(&table, batch).await?;

    // Delete by value (not PK)
    let ctx = SessionContext::new();
    let filter = col("value").eq(lit(3i64));
    let plan = table.delete_from(&ctx.state(), &[filter]).await?;
    let results = datafusion_physical_plan::collect(plan, ctx.task_ctx()).await?;
    let deleted = results
        .first()
        .and_then(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<arrow::array::UInt64Array>()
        })
        .and_then(|a| a.values().first())
        .copied()
        .unwrap_or(0);

    assert_eq!(
        deleted, 1,
        "Should delete exactly 1 row with PositionBased strategy"
    );

    ctx.register_table("no_pk_table", Arc::clone(&table) as Arc<dyn TableProvider>)?;
    let df = ctx.sql("SELECT COUNT(*) as cnt FROM no_pk_table").await?;
    let results = df.collect().await?;
    let count = results
        .first()
        .and_then(|b| b.column(0).as_any().downcast_ref::<Int64Array>())
        .and_then(|a| a.values().first())
        .copied()
        .unwrap_or(0);

    assert_eq!(count, 4);

    Ok(())
}

test_with_backends!(test_detects_position_based_strategy_impl);

/// Test that Int32 primary key uses `RowConverter` strategy (not `Int64Pk`).
async fn test_int32_pk_uses_rowconverter_impl(fixture: TestFixture) -> TestResult<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false), // Int32, not Int64
        Field::new("value", DataType::Utf8, false),
    ]));

    let table_options = CreateTableOptions {
        table_name: "int32_pk_table".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["id".to_string()],
        on_conflict: None, // Int32 PK -> RowConverter (not Int64Pk)
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    let table =
        Arc::new(CayenneTableProvider::create_table(get_catalog(&fixture), table_options).await?);

    // Insert data
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ],
    )?;
    insert_batch(&table, batch).await?;

    // Delete and verify
    let ctx = SessionContext::new();
    let filter = col("id").eq(lit(2i32));
    let plan = table.delete_from(&ctx.state(), &[filter]).await?;
    let results = datafusion_physical_plan::collect(plan, ctx.task_ctx()).await?;
    let deleted = results
        .first()
        .and_then(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<arrow::array::UInt64Array>()
        })
        .and_then(|a| a.values().first())
        .copied()
        .unwrap_or(0);

    assert_eq!(
        deleted, 1,
        "Should delete exactly 1 row (Int32 PK uses RowConverter strategy)"
    );

    ctx.register_table(
        "int32_pk_table",
        Arc::clone(&table) as Arc<dyn TableProvider>,
    )?;
    let df = ctx
        .sql("SELECT COUNT(*) as cnt FROM int32_pk_table")
        .await?;
    let results = df.collect().await?;
    let count = results
        .first()
        .and_then(|b| b.column(0).as_any().downcast_ref::<Int64Array>())
        .and_then(|a| a.values().first())
        .copied()
        .unwrap_or(0);

    assert_eq!(count, 2);

    Ok(())
}

test_with_backends!(test_int32_pk_uses_rowconverter_impl);

// =============================================================================
// Test Strategy Persistence and Reopen
// =============================================================================

/// Verify that the strategy is correctly determined on table reopen.
async fn test_strategy_persists_on_reopen_int64pk_impl(fixture: TestFixture) -> TestResult<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Utf8, false),
    ]));

    let table_options = CreateTableOptions {
        table_name: "persist_int64pk".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["id".to_string()],
        on_conflict: None,
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    let table =
        Arc::new(CayenneTableProvider::create_table(get_catalog(&fixture), table_options).await?);

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])),
        ],
    )?;
    insert_batch(&table, batch).await?;

    // Delete a row
    let ctx = SessionContext::new();
    let filter = col("id").eq(lit(3i64));
    let plan = table.delete_from(&ctx.state(), &[filter]).await?;
    datafusion_physical_plan::collect(plan, ctx.task_ctx()).await?;

    // Reopen table
    let table2 = Arc::new(
        CayenneTableProviderBuilder::new(get_catalog(&fixture))
            .open("persist_int64pk")
            .await?,
    );

    // Delete another row with reopened table
    let ctx2 = SessionContext::new();
    let filter2 = col("id").eq(lit(5i64));
    let plan2 = table2.delete_from(&ctx2.state(), &[filter2]).await?;
    datafusion_physical_plan::collect(plan2, ctx2.task_ctx()).await?;

    // Verify count
    ctx2.register_table(
        "persist_int64pk",
        Arc::clone(&table2) as Arc<dyn TableProvider>,
    )?;
    let df = ctx2
        .sql("SELECT COUNT(*) as cnt FROM persist_int64pk")
        .await?;
    let results = df.collect().await?;
    let count = results
        .first()
        .and_then(|b| b.column(0).as_any().downcast_ref::<Int64Array>())
        .and_then(|a| a.values().first())
        .copied()
        .unwrap_or(0);

    assert_eq!(count, 3, "Should have 3 rows remaining after reopening");

    Ok(())
}

test_with_backends!(test_strategy_persists_on_reopen_int64pk_impl);

/// Verify that `PositionBased` strategy persists on table reopen.
async fn test_strategy_persists_on_reopen_position_based_impl(
    fixture: TestFixture,
) -> TestResult<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let table_options = CreateTableOptions {
        table_name: "persist_position".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec![],
        on_conflict: None,
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    let table =
        Arc::new(CayenneTableProvider::create_table(get_catalog(&fixture), table_options).await?);

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
            Arc::new(Int64Array::from(vec![1, 2, 3])),
        ],
    )?;
    insert_batch(&table, batch).await?;

    // Delete
    let ctx = SessionContext::new();
    let filter = col("value").eq(lit(2i64));
    let plan = table.delete_from(&ctx.state(), &[filter]).await?;
    datafusion_physical_plan::collect(plan, ctx.task_ctx()).await?;

    // Reopen and delete more
    let table2 = Arc::new(
        CayenneTableProviderBuilder::new(get_catalog(&fixture))
            .open("persist_position")
            .await?,
    );

    let ctx2 = SessionContext::new();
    let filter2 = col("value").eq(lit(1i64));
    let plan2 = table2.delete_from(&ctx2.state(), &[filter2]).await?;
    datafusion_physical_plan::collect(plan2, ctx2.task_ctx()).await?;

    ctx2.register_table(
        "persist_position",
        Arc::clone(&table2) as Arc<dyn TableProvider>,
    )?;
    let df = ctx2
        .sql("SELECT COUNT(*) as cnt FROM persist_position")
        .await?;
    let results = df.collect().await?;
    let count = results
        .first()
        .and_then(|b| b.column(0).as_any().downcast_ref::<Int64Array>())
        .and_then(|a| a.values().first())
        .copied()
        .unwrap_or(0);

    assert_eq!(count, 1, "Should have 1 row remaining after reopening");

    Ok(())
}

test_with_backends!(test_strategy_persists_on_reopen_position_based_impl);

// =============================================================================
// Test Cross-Strategy Scenarios
// =============================================================================

/// Test multiple tables with different strategies in the same session.
async fn test_multiple_strategies_same_session_impl(fixture: TestFixture) -> TestResult<()> {
    let ctx = SessionContext::new();

    // Table 1: Int64Pk strategy
    let schema1 = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("data", DataType::Utf8, false),
    ]));
    let table1 = Arc::new(
        CayenneTableProvider::create_table(
            get_catalog(&fixture),
            CreateTableOptions {
                table_name: "t1_int64".to_string(),
                schema: Arc::clone(&schema1),
                primary_key: vec!["id".to_string()],
                on_conflict: None,
                base_path: fixture.data_path.to_string_lossy().to_string(),
                partition_column: None,
                vortex_config: cayenne::metadata::VortexConfig::default(),
            },
        )
        .await?,
    );
    let batch1 = RecordBatch::try_new(
        Arc::clone(&schema1),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["a", "b"])),
        ],
    )?;
    insert_batch(&table1, batch1).await?;

    // Table 2: String PK strategy
    let schema2 = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let table2 = Arc::new(
        CayenneTableProvider::create_table(
            get_catalog(&fixture),
            CreateTableOptions {
                table_name: "t2_string".to_string(),
                schema: Arc::clone(&schema2),
                primary_key: vec!["key".to_string()],
                on_conflict: None,
                base_path: fixture.data_path.to_string_lossy().to_string(),
                partition_column: None,
                vortex_config: cayenne::metadata::VortexConfig::default(),
            },
        )
        .await?,
    );
    let batch2 = RecordBatch::try_new(
        Arc::clone(&schema2),
        vec![
            Arc::new(StringArray::from(vec!["X", "Y"])),
            Arc::new(Int64Array::from(vec![10, 20])),
        ],
    )?;
    insert_batch(&table2, batch2).await?;

    // Table 3: PositionBased strategy
    let schema3 = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("amount", DataType::Int64, false),
    ]));
    let table3 = Arc::new(
        CayenneTableProvider::create_table(
            get_catalog(&fixture),
            CreateTableOptions {
                table_name: "t3_no_pk".to_string(),
                schema: Arc::clone(&schema3),
                primary_key: vec![],
                on_conflict: None,
                base_path: fixture.data_path.to_string_lossy().to_string(),
                partition_column: None,
                vortex_config: cayenne::metadata::VortexConfig::default(),
            },
        )
        .await?,
    );
    let batch3 = RecordBatch::try_new(
        Arc::clone(&schema3),
        vec![
            Arc::new(StringArray::from(vec!["foo", "bar"])),
            Arc::new(Int64Array::from(vec![100, 200])),
        ],
    )?;
    insert_batch(&table3, batch3).await?;

    // Delete from each table
    let plan1 = table1
        .delete_from(&ctx.state(), &[col("id").eq(lit(1i64))])
        .await?;
    datafusion_physical_plan::collect(plan1, ctx.task_ctx()).await?;

    let plan2 = table2
        .delete_from(&ctx.state(), &[col("key").eq(lit("X"))])
        .await?;
    datafusion_physical_plan::collect(plan2, ctx.task_ctx()).await?;

    let plan3 = table3
        .delete_from(&ctx.state(), &[col("amount").eq(lit(100i64))])
        .await?;
    datafusion_physical_plan::collect(plan3, ctx.task_ctx()).await?;

    // Verify each table
    ctx.register_table("t1_int64", Arc::clone(&table1) as Arc<dyn TableProvider>)?;
    ctx.register_table("t2_string", Arc::clone(&table2) as Arc<dyn TableProvider>)?;
    ctx.register_table("t3_no_pk", Arc::clone(&table3) as Arc<dyn TableProvider>)?;

    for table_name in ["t1_int64", "t2_string", "t3_no_pk"] {
        let df = ctx
            .sql(&format!("SELECT COUNT(*) as cnt FROM {table_name}"))
            .await?;
        let results = df.collect().await?;
        let count = results
            .first()
            .and_then(|b| b.column(0).as_any().downcast_ref::<Int64Array>())
            .and_then(|a| a.values().first())
            .copied()
            .unwrap_or(0);
        assert_eq!(count, 1, "Table {table_name} should have 1 row");
    }

    Ok(())
}

test_with_backends!(test_multiple_strategies_same_session_impl);
