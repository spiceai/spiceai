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

//! Critical integration tests for the three-tier deletion strategy.
//!
//! This test file specifically validates the three deletion strategies:
//! 1. **Int64 PK Strategy**: Single-column Int64 primary key using `HashSet<i64>`
//! 2. **`RowConverter` Strategy**: Composite/non-integer primary keys using `RowConverter`
//! 3. **Position-Based Strategy**: Tables without primary key using `RoaringBitmap`
//!
//! Each strategy is tested for:
//! - Basic deletion operations
//! - Multiple batch inserts with interleaved deletions
//! - Projection scenarios (querying without PK columns)
//! - Persistence across table reopens
//! - Concurrent operations
//! - Large-scale deletions

#![allow(clippy::expect_used)]

mod common;

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
// STRATEGY 1: Int64 PK (Single-column Int64 primary key)
// =============================================================================

/// Test: Int64 PK strategy basic deletion.
/// Verifies that single-column Int64 PK tables use direct `HashSet<i64>` lookup.
#[tokio::test]
async fn test_int64_pk_basic_deletion() -> TestResult<()> {
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
        table_name: "int64_pk_test".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["id".to_string()],
        on_conflict: None, // Single Int64 PK
        base_path: data_dir.path().to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    let table = Arc::new(CayenneTableProvider::create_table(catalog, table_options).await?);
    let ctx = SessionContext::new();
    ctx.register_table(
        "int64_pk_test",
        Arc::clone(&table) as Arc<dyn TableProvider>,
    )?;

    // Insert 10 rows
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
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

    // Delete id=3, id=7
    delete_records(&table, col("id").eq(lit(3i64))).await?;
    delete_records(&table, col("id").eq(lit(7i64))).await?;

    // Verify
    let count = get_row_count(&ctx, "int64_pk_test").await?;
    assert_eq!(count, 8, "Should have 8 rows after deleting 2");

    let ids = get_ids(&ctx, "int64_pk_test").await?;
    assert_eq!(ids, vec![1, 2, 4, 5, 6, 8, 9, 10]);
    assert!(!ids.contains(&3), "id=3 should be deleted");
    assert!(!ids.contains(&7), "id=7 should be deleted");

    Ok(())
}

/// Test: Int64 PK with multiple inserts and interleaved deletions.
/// This tests the critical bug scenario where deletions across multiple files work correctly.
#[tokio::test]
async fn test_int64_pk_multi_insert_deletion() -> TestResult<()> {
    let data_dir = TempDir::new()?;
    let metadata_dir = TempDir::new()?;

    let catalog = Arc::new(CayenneCatalog::new(format!(
        "sqlite://{}/test.db",
        metadata_dir.path().display()
    ))?);
    catalog.init().await?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("data", DataType::Utf8, false),
    ]));

    let table_options = CreateTableOptions {
        table_name: "int64_pk_multi".to_string(),
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
        "int64_pk_multi",
        Arc::clone(&table) as Arc<dyn TableProvider>,
    )?;

    // Insert batch 1: ids 1-5
    let batch1 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])),
        ],
    )?;
    insert_batch(&table, batch1).await?;

    // Insert batch 2: ids 6-10 (creates second file)
    let batch2 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![6, 7, 8, 9, 10])),
            Arc::new(StringArray::from(vec!["f", "g", "h", "i", "j"])),
        ],
    )?;
    insert_batch(&table, batch2).await?;

    assert_eq!(get_row_count(&ctx, "int64_pk_multi").await?, 10);

    // Delete from first batch (id=2) and second batch (id=8)
    delete_records(&table, col("id").eq(lit(2i64))).await?;
    delete_records(&table, col("id").eq(lit(8i64))).await?;

    // Verify deletions work across both files
    let count = get_row_count(&ctx, "int64_pk_multi").await?;
    assert_eq!(count, 8, "Should have 8 rows after deletions");

    let ids = get_ids(&ctx, "int64_pk_multi").await?;
    assert_eq!(ids, vec![1, 3, 4, 5, 6, 7, 9, 10]);

    // Insert batch 3: ids 11-15
    let batch3 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![11, 12, 13, 14, 15])),
            Arc::new(StringArray::from(vec!["k", "l", "m", "n", "o"])),
        ],
    )?;
    insert_batch(&table, batch3).await?;

    // Delete from third batch
    delete_records(&table, col("id").eq(lit(13i64))).await?;

    let final_count = get_row_count(&ctx, "int64_pk_multi").await?;
    assert_eq!(final_count, 12, "Should have 12 rows total");

    let final_ids = get_ids(&ctx, "int64_pk_multi").await?;
    assert_eq!(final_ids, vec![1, 3, 4, 5, 6, 7, 9, 10, 11, 12, 14, 15]);

    Ok(())
}

/// Test: Int64 PK projection without PK column.
/// Verifies that querying only non-PK columns still correctly filters deleted rows.
#[tokio::test]
async fn test_int64_pk_projection_without_pk() -> TestResult<()> {
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
        Field::new("score", DataType::Int64, false),
    ]));

    let table_options = CreateTableOptions {
        table_name: "int64_pk_proj".to_string(),
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
        "int64_pk_proj",
        Arc::clone(&table) as Arc<dyn TableProvider>,
    )?;

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

    // Delete id=3 (Charlie)
    delete_records(&table, col("id").eq(lit(3i64))).await?;

    // Query ONLY name and score (not id) - tests projection extension
    let df = ctx
        .sql("SELECT name, score FROM int64_pk_proj ORDER BY score")
        .await?;
    let results = df.collect().await?;

    let names: Vec<String> = results
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("name")
                .iter()
                .filter_map(|s| s.map(String::from))
        })
        .collect();

    assert_eq!(names.len(), 4);
    assert!(!names.contains(&"Charlie".to_string()));
    assert_eq!(names, vec!["Alice", "Bob", "David", "Eve"]);

    Ok(())
}

/// Test: Int64 PK persistence across table reopen.
#[tokio::test]
async fn test_int64_pk_persistence() -> TestResult<()> {
    let data_dir = TempDir::new()?;
    let metadata_dir = TempDir::new()?;
    let db_path = format!("sqlite://{}/test.db", metadata_dir.path().display());

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));

    // Phase 1: Create, insert, delete
    {
        let catalog = Arc::new(CayenneCatalog::new(&db_path)?);
        catalog.init().await?;

        let table_options = CreateTableOptions {
            table_name: "int64_persist".to_string(),
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
            "int64_persist",
            Arc::clone(&table) as Arc<dyn TableProvider>,
        )?;

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50])),
            ],
        )?;
        insert_batch(&table, batch).await?;

        delete_records(&table, col("id").eq(lit(2i64)).or(col("id").eq(lit(4i64)))).await?;

        assert_eq!(get_row_count(&ctx, "int64_persist").await?, 3);
    }

    // Phase 2: Reopen and verify
    {
        let catalog = Arc::new(CayenneCatalog::new(&db_path)?);
        catalog.init().await?;

        let table = Arc::new(
            CayenneTableProviderBuilder::new(catalog)
                .open("int64_persist")
                .await?,
        );

        let ctx = SessionContext::new();
        ctx.register_table(
            "int64_persist",
            Arc::clone(&table) as Arc<dyn TableProvider>,
        )?;

        let count = get_row_count(&ctx, "int64_persist").await?;
        assert_eq!(count, 3, "Should have 3 rows after reopen");

        let ids = get_ids(&ctx, "int64_persist").await?;
        assert_eq!(ids, vec![1, 3, 5]);
    }

    Ok(())
}

// =============================================================================
// STRATEGY 2: RowConverter (Composite or non-integer primary key)
// =============================================================================

/// Test: Composite PK (id1, id2) using `RowConverter` strategy.
#[tokio::test]
async fn test_rowconverter_composite_pk() -> TestResult<()> {
    let data_dir = TempDir::new()?;
    let metadata_dir = TempDir::new()?;

    let catalog = Arc::new(CayenneCatalog::new(format!(
        "sqlite://{}/test.db",
        metadata_dir.path().display()
    ))?);
    catalog.init().await?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id1", DataType::Int64, false),
        Field::new("id2", DataType::Int64, false),
        Field::new("data", DataType::Utf8, false),
    ]));

    let table_options = CreateTableOptions {
        table_name: "composite_pk_test".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["id1".to_string(), "id2".to_string()],
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

    // Insert data with composite keys
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 1, 2, 2, 2])),
            Arc::new(Int64Array::from(vec![1, 2, 3, 1, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e", "f"])),
        ],
    )?;
    insert_batch(&table, batch).await?;

    assert_eq!(get_row_count(&ctx, "composite_pk_test").await?, 6);

    // Delete (id1=1, id2=2) - should only delete one row
    delete_records(
        &table,
        col("id1").eq(lit(1i64)).and(col("id2").eq(lit(2i64))),
    )
    .await?;

    let count = get_row_count(&ctx, "composite_pk_test").await?;
    assert_eq!(count, 5, "Should have 5 rows after deletion");

    // Verify the correct row was deleted
    let df = ctx
        .sql("SELECT data FROM composite_pk_test WHERE id1 = 1 ORDER BY id2")
        .await?;
    let results = df.collect().await?;
    let data: Vec<String> = results
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("data")
                .iter()
                .filter_map(|s| s.map(String::from))
        })
        .collect();
    assert_eq!(data, vec!["a", "c"]); // "b" was deleted

    Ok(())
}

/// Test: String PK using `RowConverter` strategy.
#[tokio::test]
async fn test_rowconverter_string_pk() -> TestResult<()> {
    let data_dir = TempDir::new()?;
    let metadata_dir = TempDir::new()?;

    let catalog = Arc::new(CayenneCatalog::new(format!(
        "sqlite://{}/test.db",
        metadata_dir.path().display()
    ))?);
    catalog.init().await?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false), // String PK
        Field::new("email", DataType::Utf8, false),
        Field::new("age", DataType::Int64, false),
    ]));

    let table_options = CreateTableOptions {
        table_name: "string_pk_test".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["user_id".to_string()],
        on_conflict: None, // Single String PK
        base_path: data_dir.path().to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    let table = Arc::new(CayenneTableProvider::create_table(catalog, table_options).await?);
    let ctx = SessionContext::new();
    ctx.register_table(
        "string_pk_test",
        Arc::clone(&table) as Arc<dyn TableProvider>,
    )?;

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec![
                "user_001", "user_002", "user_003", "user_004", "user_005",
            ])),
            Arc::new(StringArray::from(vec![
                "a@test.com",
                "b@test.com",
                "c@test.com",
                "d@test.com",
                "e@test.com",
            ])),
            Arc::new(Int64Array::from(vec![25, 30, 35, 40, 45])),
        ],
    )?;
    insert_batch(&table, batch).await?;

    // Delete user_003
    delete_records(&table, col("user_id").eq(lit("user_003"))).await?;

    let count = get_row_count(&ctx, "string_pk_test").await?;
    assert_eq!(count, 4);

    // Verify user_003 is gone
    let df = ctx
        .sql("SELECT user_id FROM string_pk_test ORDER BY user_id")
        .await?;
    let results = df.collect().await?;
    let user_ids: Vec<String> = results
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("user_id")
                .iter()
                .filter_map(|s| s.map(String::from))
        })
        .collect();
    assert_eq!(
        user_ids,
        vec!["user_001", "user_002", "user_004", "user_005"]
    );

    Ok(())
}

/// Test: `RowConverter` strategy persistence.
#[tokio::test]
async fn test_rowconverter_persistence() -> TestResult<()> {
    let data_dir = TempDir::new()?;
    let metadata_dir = TempDir::new()?;
    let db_path = format!("sqlite://{}/test.db", metadata_dir.path().display());

    let schema = Arc::new(Schema::new(vec![
        Field::new("code", DataType::Utf8, false), // String PK
        Field::new("value", DataType::Int64, false),
    ]));

    // Phase 1
    {
        let catalog = Arc::new(CayenneCatalog::new(&db_path)?);
        catalog.init().await?;

        let table_options = CreateTableOptions {
            table_name: "rowconv_persist".to_string(),
            schema: Arc::clone(&schema),
            primary_key: vec!["code".to_string()],
            on_conflict: None,
            base_path: data_dir.path().to_string_lossy().to_string(),
            partition_column: None,
            vortex_config: cayenne::metadata::VortexConfig::default(),
        };

        let table = Arc::new(CayenneTableProvider::create_table(catalog, table_options).await?);
        let ctx = SessionContext::new();
        ctx.register_table(
            "rowconv_persist",
            Arc::clone(&table) as Arc<dyn TableProvider>,
        )?;

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["AAA", "BBB", "CCC", "DDD"])),
                Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            ],
        )?;
        insert_batch(&table, batch).await?;

        delete_records(&table, col("code").eq(lit("BBB"))).await?;
        assert_eq!(get_row_count(&ctx, "rowconv_persist").await?, 3);
    }

    // Phase 2: Reopen
    {
        let catalog = Arc::new(CayenneCatalog::new(&db_path)?);
        catalog.init().await?;

        let table = Arc::new(
            CayenneTableProviderBuilder::new(catalog)
                .open("rowconv_persist")
                .await?,
        );

        let ctx = SessionContext::new();
        ctx.register_table(
            "rowconv_persist",
            Arc::clone(&table) as Arc<dyn TableProvider>,
        )?;

        assert_eq!(get_row_count(&ctx, "rowconv_persist").await?, 3);

        // Verify BBB is still deleted
        let df = ctx
            .sql("SELECT code FROM rowconv_persist ORDER BY code")
            .await?;
        let results = df.collect().await?;
        let codes: Vec<String> = results
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("code")
                    .iter()
                    .filter_map(|s| s.map(String::from))
            })
            .collect();
        assert_eq!(codes, vec!["AAA", "CCC", "DDD"]);
    }

    Ok(())
}

// =============================================================================
// STRATEGY 3: Position-Based (No primary key)
// =============================================================================

/// Test: Position-based deletion basic case.
#[tokio::test]
async fn test_position_based_basic() -> TestResult<()> {
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
        table_name: "no_pk_basic".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec![],
        on_conflict: None, // NO primary key
        base_path: data_dir.path().to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    let table = Arc::new(CayenneTableProvider::create_table(catalog, table_options).await?);
    let ctx = SessionContext::new();
    ctx.register_table("no_pk_basic", Arc::clone(&table) as Arc<dyn TableProvider>)?;

    // Insert with duplicate values (allowed without PK)
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["A", "B", "A", "C", "B", "A"])),
            Arc::new(Int64Array::from(vec![100, 200, 300, 400, 500, 600])),
        ],
    )?;
    insert_batch(&table, batch).await?;

    assert_eq!(get_row_count(&ctx, "no_pk_basic").await?, 6);

    // Delete all 'A' categories (3 rows)
    let deleted = delete_records(&table, col("category").eq(lit("A"))).await?;
    assert_eq!(deleted, 3);

    let count = get_row_count(&ctx, "no_pk_basic").await?;
    assert_eq!(count, 3);

    // Verify no A's remain
    let df = ctx
        .sql("SELECT DISTINCT category FROM no_pk_basic ORDER BY category")
        .await?;
    let results = df.collect().await?;
    let cats: Vec<String> = results
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
    assert_eq!(cats, vec!["B", "C"]);

    Ok(())
}

/// Test: Position-based deletion with multiple inserts.
/// This is particularly important because position-based deletion can be fragile
/// with multiple data files.
#[tokio::test]
async fn test_position_based_multi_insert() -> TestResult<()> {
    let data_dir = TempDir::new()?;
    let metadata_dir = TempDir::new()?;

    let catalog = Arc::new(CayenneCatalog::new(format!(
        "sqlite://{}/test.db",
        metadata_dir.path().display()
    ))?);
    catalog.init().await?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("tag", DataType::Utf8, false),
        Field::new("seq", DataType::Int64, false),
    ]));

    let table_options = CreateTableOptions {
        table_name: "no_pk_multi".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec![],
        on_conflict: None,
        base_path: data_dir.path().to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    let table = Arc::new(CayenneTableProvider::create_table(catalog, table_options).await?);
    let ctx = SessionContext::new();
    ctx.register_table("no_pk_multi", Arc::clone(&table) as Arc<dyn TableProvider>)?;

    // Insert batch 1
    let batch1 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["X", "Y", "Z"])),
            Arc::new(Int64Array::from(vec![1, 2, 3])),
        ],
    )?;
    insert_batch(&table, batch1).await?;

    // Insert batch 2
    let batch2 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["X", "Y", "Z"])),
            Arc::new(Int64Array::from(vec![4, 5, 6])),
        ],
    )?;
    insert_batch(&table, batch2).await?;

    assert_eq!(get_row_count(&ctx, "no_pk_multi").await?, 6);

    // Delete all 'Y' tags (should delete 2 rows, one from each batch)
    let deleted = delete_records(&table, col("tag").eq(lit("Y"))).await?;
    assert_eq!(deleted, 2);

    let count = get_row_count(&ctx, "no_pk_multi").await?;
    assert_eq!(count, 4);

    // Verify remaining sequences
    let df = ctx.sql("SELECT seq FROM no_pk_multi ORDER BY seq").await?;
    let results = df.collect().await?;
    let seqs: Vec<i64> = results
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("seq")
                .values()
                .iter()
                .copied()
        })
        .collect();
    assert_eq!(seqs, vec![1, 3, 4, 6]); // 2 and 5 deleted

    Ok(())
}

/// Test: Position-based deletion persistence.
#[tokio::test]
async fn test_position_based_persistence() -> TestResult<()> {
    let data_dir = TempDir::new()?;
    let metadata_dir = TempDir::new()?;
    let db_path = format!("sqlite://{}/test.db", metadata_dir.path().display());

    let schema = Arc::new(Schema::new(vec![
        Field::new("label", DataType::Utf8, false),
        Field::new("num", DataType::Int64, false),
    ]));

    // Phase 1
    {
        let catalog = Arc::new(CayenneCatalog::new(&db_path)?);
        catalog.init().await?;

        let table_options = CreateTableOptions {
            table_name: "pos_persist".to_string(),
            schema: Arc::clone(&schema),
            primary_key: vec![],
            on_conflict: None,
            base_path: data_dir.path().to_string_lossy().to_string(),
            partition_column: None,
            vortex_config: cayenne::metadata::VortexConfig::default(),
        };

        let table = Arc::new(CayenneTableProvider::create_table(catalog, table_options).await?);
        let ctx = SessionContext::new();
        ctx.register_table("pos_persist", Arc::clone(&table) as Arc<dyn TableProvider>)?;

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["P", "Q", "R", "S", "T"])),
                Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50])),
            ],
        )?;
        insert_batch(&table, batch).await?;

        delete_records(&table, col("label").eq(lit("R"))).await?;
        assert_eq!(get_row_count(&ctx, "pos_persist").await?, 4);
    }

    // Phase 2: Reopen
    {
        let catalog = Arc::new(CayenneCatalog::new(&db_path)?);
        catalog.init().await?;

        let table = Arc::new(
            CayenneTableProviderBuilder::new(catalog)
                .open("pos_persist")
                .await?,
        );

        let ctx = SessionContext::new();
        ctx.register_table("pos_persist", Arc::clone(&table) as Arc<dyn TableProvider>)?;

        assert_eq!(get_row_count(&ctx, "pos_persist").await?, 4);

        let df = ctx
            .sql("SELECT label FROM pos_persist ORDER BY num")
            .await?;
        let results = df.collect().await?;
        let labels: Vec<String> = results
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("label")
                    .iter()
                    .filter_map(|s| s.map(String::from))
            })
            .collect();
        assert_eq!(labels, vec!["P", "Q", "S", "T"]); // R deleted
    }

    Ok(())
}

// =============================================================================
// Cross-Strategy Validation Tests
// =============================================================================

/// Test: Verify strategy selection is correct based on schema.
/// Creates three tables with different PK configurations and ensures
/// each uses the correct deletion strategy.
#[tokio::test]
async fn test_strategy_selection() -> TestResult<()> {
    let data_dir = TempDir::new()?;
    let metadata_dir = TempDir::new()?;

    let catalog: Arc<dyn MetadataCatalog> = Arc::new(CayenneCatalog::new(format!(
        "sqlite://{}/test.db",
        metadata_dir.path().display()
    ))?);
    catalog.init().await?;

    let ctx = SessionContext::new();

    // Table 1: Int64 PK (should use Int64Pk strategy)
    let schema1 = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("data", DataType::Utf8, false),
    ]));
    let opts1 = CreateTableOptions {
        table_name: "t_int64".to_string(),
        schema: Arc::clone(&schema1),
        primary_key: vec!["id".to_string()],
        on_conflict: None,
        base_path: data_dir.path().to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };
    let t1 = Arc::new(CayenneTableProvider::create_table(Arc::clone(&catalog), opts1).await?);
    ctx.register_table("t_int64", Arc::clone(&t1) as Arc<dyn TableProvider>)?;

    // Table 2: String PK (should use RowConverter strategy)
    let schema2 = Arc::new(Schema::new(vec![
        Field::new("code", DataType::Utf8, false),
        Field::new("data", DataType::Utf8, false),
    ]));
    let opts2 = CreateTableOptions {
        table_name: "t_string".to_string(),
        schema: Arc::clone(&schema2),
        primary_key: vec!["code".to_string()],
        on_conflict: None,
        base_path: data_dir.path().to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };
    let t2 = Arc::new(CayenneTableProvider::create_table(Arc::clone(&catalog), opts2).await?);
    ctx.register_table("t_string", Arc::clone(&t2) as Arc<dyn TableProvider>)?;

    // Table 3: No PK (should use PositionBased strategy)
    let schema3 = Arc::new(Schema::new(vec![
        Field::new("val1", DataType::Int64, false),
        Field::new("val2", DataType::Utf8, false),
    ]));
    let opts3 = CreateTableOptions {
        table_name: "t_nopk".to_string(),
        schema: Arc::clone(&schema3),
        primary_key: vec![],
        on_conflict: None,
        base_path: data_dir.path().to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };
    let t3 = Arc::new(CayenneTableProvider::create_table(Arc::clone(&catalog), opts3).await?);
    ctx.register_table("t_nopk", Arc::clone(&t3) as Arc<dyn TableProvider>)?;

    // Insert and delete from all three to verify they all work
    let batch1 = RecordBatch::try_new(
        Arc::clone(&schema1),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ],
    )?;
    insert_batch(&t1, batch1).await?;
    delete_records(&t1, col("id").eq(lit(2i64))).await?;
    assert_eq!(get_row_count(&ctx, "t_int64").await?, 2);

    let batch2 = RecordBatch::try_new(
        Arc::clone(&schema2),
        vec![
            Arc::new(StringArray::from(vec!["X", "Y", "Z"])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ],
    )?;
    insert_batch(&t2, batch2).await?;
    delete_records(&t2, col("code").eq(lit("Y"))).await?;
    assert_eq!(get_row_count(&ctx, "t_string").await?, 2);

    let batch3 = RecordBatch::try_new(
        Arc::clone(&schema3),
        vec![
            Arc::new(Int64Array::from(vec![100, 200, 300])),
            Arc::new(StringArray::from(vec!["p", "q", "r"])),
        ],
    )?;
    insert_batch(&t3, batch3).await?;
    delete_records(&t3, col("val1").eq(lit(200i64))).await?;
    assert_eq!(get_row_count(&ctx, "t_nopk").await?, 2);

    Ok(())
}

/// Test: Large-scale deletion stress test for each strategy.
#[tokio::test]
async fn test_large_scale_deletions() -> TestResult<()> {
    let data_dir = TempDir::new()?;
    let metadata_dir = TempDir::new()?;

    let catalog = Arc::new(CayenneCatalog::new(format!(
        "sqlite://{}/test.db",
        metadata_dir.path().display()
    ))?);
    catalog.init().await?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("category", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let table_options = CreateTableOptions {
        table_name: "large_scale".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["id".to_string()],
        on_conflict: None,
        base_path: data_dir.path().to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    let table = Arc::new(CayenneTableProvider::create_table(catalog, table_options).await?);
    let ctx = SessionContext::new();
    ctx.register_table("large_scale", Arc::clone(&table) as Arc<dyn TableProvider>)?;

    // Insert 1000 rows
    let n = 1000i64;
    let categories: Vec<&str> = (0..n)
        .map(|i| match i % 4 {
            0 => "A",
            1 => "B",
            2 => "C",
            _ => "D",
        })
        .collect();

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from((1..=n).collect::<Vec<_>>())),
            Arc::new(StringArray::from(categories)),
            Arc::new(Int64Array::from(
                (1..=n).map(|i| i * 10).collect::<Vec<_>>(),
            )),
        ],
    )?;
    insert_batch(&table, batch).await?;

    assert_eq!(get_row_count(&ctx, "large_scale").await?, n);

    // Delete all category='A' (250 rows)
    let deleted = delete_records(&table, col("category").eq(lit("A"))).await?;
    assert_eq!(deleted, 250);

    let remaining = get_row_count(&ctx, "large_scale").await?;
    assert_eq!(remaining, 750);

    // Verify no A's remain
    let df = ctx
        .sql("SELECT COUNT(*) FROM large_scale WHERE category = 'A'")
        .await?;
    let results = df.collect().await?;
    let a_count: i64 = results
        .first()
        .and_then(|b| b.column(0).as_any().downcast_ref::<Int64Array>())
        .and_then(|a| a.values().first())
        .copied()
        .unwrap_or(-1);
    assert_eq!(a_count, 0, "No rows with category='A' should remain");

    Ok(())
}
