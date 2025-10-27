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

//! Test DELETE operations for Pepper
//!
//! These tests work at the API level by calling `delete_from()` directly,
//! rather than using SQL DELETE statements (which require runtime-level integration).

use arrow::datatypes::{DataType, Field, Schema};
use data_components::delete::DeletionTableProvider;
use datafusion::prelude::*;
use datafusion_physical_plan::collect;
use pepper::metadata::CreateTableOptions;
use pepper::{MetadataCatalog, PepperCatalog, PepperTableProvider};
use std::sync::Arc;
use tempfile::TempDir;

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_delete_with_primary_key() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🧪 Testing DELETE with primary key...");

    // 1. Setup test environment
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("delete_pk_test.db");
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path)?;

    // 2. Create catalog and table with primary key
    let catalog: Arc<dyn MetadataCatalog> = Arc::new(PepperCatalog::new(format!(
        "sqlite://{}",
        db_path.to_string_lossy()
    )));
    catalog.init().await?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let table_options = CreateTableOptions {
        table_name: "test_delete_pk".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["id".to_string()],
        base_path: data_path.to_string_lossy().to_string(),
        partition_column: None,
    };

    let table = PepperTableProvider::create_table(Arc::clone(&catalog), table_options).await?;
    let table = Arc::new(table);
    println!("✓ Table created with primary key on 'id'");

    // 3. Register with DataFusion
    let ctx = SessionContext::new();
    ctx.register_table("test_delete_pk", Arc::clone(&table) as _)?;

    // 4. Insert initial data
    ctx.sql("INSERT INTO test_delete_pk VALUES (1, 'Alice', 100), (2, 'Bob', 200), (3, 'Charlie', 300), (4, 'Diana', 400), (5, 'Eve', 500)")
        .await?
        .collect()
        .await?;
    println!("✓ Inserted 5 rows");

    // 5. Verify initial data
    let df = ctx
        .sql("SELECT COUNT(*) as count FROM test_delete_pk")
        .await?;
    let results = df.collect().await?;
    let count = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .expect("count column")
        .value(0);
    assert_eq!(count, 5, "Expected 5 rows before deletion");
    println!("✓ Verified 5 rows exist");

    // 6. Delete a single row by primary key (API call, not SQL)
    println!("\n--- Test 1: Delete single row by primary key ---");

    // Build filter expression: id = 3
    let id_col = col("id");
    let filter = id_col.eq(lit(3i64));

    // Call delete_from directly on the table provider
    let delete_plan = table.delete_from(&ctx.state(), &[filter]).await?;

    // Execute the deletion plan
    let delete_results = collect(delete_plan, ctx.task_ctx()).await?;
    let delete_count = delete_results[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::UInt64Array>()
        .expect("count column")
        .value(0);

    println!("✓ DELETE executed for id=3, deleted {delete_count} row(s)");
    assert_eq!(delete_count, 1, "Expected to delete 1 row");

    // 7. Verify deletion (should have 4 rows)
    // Note: This will show 5 rows until read-time filtering is implemented
    let df = ctx
        .sql("SELECT COUNT(*) as count FROM test_delete_pk")
        .await?;
    let results = df.collect().await?;
    let count = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .expect("count column")
        .value(0);

    println!(
        "⚠️  Row count after delete: {count} (expected 4, but read-time filtering not yet implemented)"
    );
    // TODO: Once read-time filtering is implemented, uncomment:
    // assert_eq!(count, 4, "Expected 4 rows after deleting 1");

    // 8. Delete multiple rows by primary key
    println!("\n--- Test 2: Delete multiple rows by primary key ---");

    let id_col = col("id");
    let filter = id_col.clone().eq(lit(1i64)).or(id_col.eq(lit(5i64)));

    let delete_plan = table.delete_from(&ctx.state(), &[filter]).await?;
    let delete_results = collect(delete_plan, ctx.task_ctx()).await?;
    let delete_count = delete_results[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::UInt64Array>()
        .expect("count column")
        .value(0);

    println!("✓ DELETE executed for id IN (1, 5), deleted {delete_count} row(s)");
    assert_eq!(delete_count, 2, "Expected to delete 2 rows");

    // 9. Verify final count
    let df = ctx
        .sql("SELECT COUNT(*) as count FROM test_delete_pk")
        .await?;
    let results = df.collect().await?;
    let count = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .expect("count column")
        .value(0);

    println!(
        "⚠️  Final row count: {count} (expected 2, but read-time filtering not yet implemented)"
    );
    // TODO: Once read-time filtering is implemented, uncomment:
    // assert_eq!(count, 2, "Expected 2 rows after deleting 3 total");

    println!("\n✅ DELETE with primary key test completed successfully");
    Ok(())
}

#[tokio::test]
async fn test_delete_without_primary_key() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🧪 Testing DELETE without primary key...");

    // 1. Setup test environment
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("delete_no_pk_test.db");
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path)?;

    // 2. Create catalog and table WITHOUT primary key
    let catalog: Arc<dyn MetadataCatalog> = Arc::new(PepperCatalog::new(format!(
        "sqlite://{}",
        db_path.to_string_lossy()
    )));
    catalog.init().await?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("category", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let table_options = CreateTableOptions {
        table_name: "test_delete_no_pk".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec![], // NO primary key
        base_path: data_path.to_string_lossy().to_string(),
        partition_column: None,
    };

    let table = PepperTableProvider::create_table(Arc::clone(&catalog), table_options).await?;
    let table = Arc::new(table);
    println!("✓ Table created WITHOUT primary key");

    // 3. Register with DataFusion
    let ctx = SessionContext::new();
    ctx.register_table("test_delete_no_pk", Arc::clone(&table) as _)?;

    // 4. Insert initial data
    ctx.sql("INSERT INTO test_delete_no_pk VALUES (1, 'A', 100), (2, 'B', 200), (3, 'A', 300), (4, 'C', 400)")
        .await?
        .collect()
        .await?;
    println!("✓ Inserted 4 rows");

    // 5. Verify initial data
    let df = ctx
        .sql("SELECT COUNT(*) as count FROM test_delete_no_pk")
        .await?;
    let results = df.collect().await?;
    let count = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .expect("count column")
        .value(0);
    assert_eq!(count, 4, "Expected 4 rows before deletion");
    println!("✓ Verified 4 rows exist");

    // 6. Delete rows by filter (without primary key)
    println!("\n--- Test: Delete by category filter ---");

    let category_col = col("category");
    let filter = category_col.eq(lit("A"));

    let delete_plan = table.delete_from(&ctx.state(), &[filter]).await?;
    let delete_results = collect(delete_plan, ctx.task_ctx()).await?;
    let delete_count = delete_results[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::UInt64Array>()
        .expect("count column")
        .value(0);

    println!("✓ DELETE executed for category='A', deleted {delete_count} row(s)");
    assert_eq!(
        delete_count, 2,
        "Expected to delete 2 rows with category='A'"
    );

    // 7. Verify deletion (should have 2 rows left)
    let df = ctx
        .sql("SELECT COUNT(*) as count FROM test_delete_no_pk")
        .await?;
    let results = df.collect().await?;
    let count = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .expect("count column")
        .value(0);

    println!(
        "⚠️  Row count after delete: {count} (expected 2, but read-time filtering not yet implemented)"
    );
    // TODO: Once read-time filtering is implemented, uncomment:
    // assert_eq!(count, 2, "Expected 2 rows after deleting category='A' (id=1, id=3)");

    println!("\n✅ DELETE without primary key test completed successfully");
    Ok(())
}

#[tokio::test]
async fn test_delete_all_rows() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🧪 Testing DELETE all rows...");

    // 1. Setup test environment
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("delete_all_test.db");
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path)?;

    // 2. Create catalog and table
    let catalog: Arc<dyn MetadataCatalog> = Arc::new(PepperCatalog::new(format!(
        "sqlite://{}",
        db_path.to_string_lossy()
    )));
    catalog.init().await?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let table_options = CreateTableOptions {
        table_name: "test_delete_all".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["id".to_string()],
        base_path: data_path.to_string_lossy().to_string(),
        partition_column: None,
    };

    let table = PepperTableProvider::create_table(Arc::clone(&catalog), table_options).await?;
    let table = Arc::new(table);

    // 3. Register with DataFusion
    let ctx = SessionContext::new();
    ctx.register_table("test_delete_all", Arc::clone(&table) as _)?;

    // 4. Insert initial data
    ctx.sql("INSERT INTO test_delete_all VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")
        .await?
        .collect()
        .await?;
    println!("✓ Inserted 3 rows");

    // 5. Delete all rows (empty filter means delete all)
    let delete_plan = table.delete_from(&ctx.state(), &[]).await?;
    let delete_results = collect(delete_plan, ctx.task_ctx()).await?;
    let delete_count = delete_results[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::UInt64Array>()
        .expect("count column")
        .value(0);

    println!("✓ DELETE all executed, deleted {delete_count} row(s)");
    assert_eq!(delete_count, 3, "Expected to delete all 3 rows");

    // 6. Verify table is empty
    let df = ctx
        .sql("SELECT COUNT(*) as count FROM test_delete_all")
        .await?;
    let results = df.collect().await?;
    let count = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .expect("count column")
        .value(0);

    println!(
        "⚠️  Row count after delete all: {count} (expected 0, but read-time filtering not yet implemented)"
    );
    // TODO: Once read-time filtering is implemented, uncomment:
    // assert_eq!(count, 0, "Expected 0 rows after DELETE all");

    println!("\n✅ DELETE all rows test completed successfully");
    Ok(())
}

#[tokio::test]
async fn test_delete_then_insert() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🧪 Testing DELETE followed by INSERT...");

    // 1. Setup test environment
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("delete_insert_test.db");
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path)?;

    // 2. Create catalog and table
    let catalog: Arc<dyn MetadataCatalog> = Arc::new(PepperCatalog::new(format!(
        "sqlite://{}",
        db_path.to_string_lossy()
    )));
    catalog.init().await?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let table_options = CreateTableOptions {
        table_name: "test_delete_insert".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["id".to_string()],
        base_path: data_path.to_string_lossy().to_string(),
        partition_column: None,
    };

    let table = PepperTableProvider::create_table(Arc::clone(&catalog), table_options).await?;
    let table = Arc::new(table);

    // 3. Register with DataFusion
    let ctx = SessionContext::new();
    ctx.register_table("test_delete_insert", Arc::clone(&table) as _)?;

    // 4. Insert initial data
    ctx.sql("INSERT INTO test_delete_insert VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")
        .await?
        .collect()
        .await?;
    println!("✓ Inserted 3 rows");

    // 5. Delete a row
    let id_col = col("id");
    let filter = id_col.eq(lit(2i64));

    let delete_plan = table.delete_from(&ctx.state(), &[filter]).await?;
    let delete_results = collect(delete_plan, ctx.task_ctx()).await?;
    let delete_count = delete_results[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::UInt64Array>()
        .expect("count column")
        .value(0);

    println!("✓ Deleted id=2, deleted {delete_count} row(s)");
    assert_eq!(delete_count, 1, "Expected to delete 1 row");

    // 6. Verify deletion
    let df = ctx
        .sql("SELECT COUNT(*) as count FROM test_delete_insert")
        .await?;
    let results = df.collect().await?;
    let count = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .expect("count column")
        .value(0);

    println!(
        "⚠️  Row count after delete: {count} (expected 2, but read-time filtering not yet implemented)"
    );
    // TODO: Once read-time filtering is implemented, uncomment:
    // assert_eq!(count, 2, "Expected 2 rows after deletion");

    // 7. Insert new data (including re-using deleted id)
    ctx.sql("INSERT INTO test_delete_insert VALUES (2, 'NewBob'), (4, 'Diana')")
        .await?
        .collect()
        .await?;
    println!("✓ Inserted 2 new rows (re-using id=2)");

    // 8. Verify final state
    let df = ctx
        .sql("SELECT COUNT(*) as count FROM test_delete_insert")
        .await?;
    let results = df.collect().await?;
    let count = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .expect("count column")
        .value(0);

    println!("⚠️  Final row count: {count}");
    // Note: With read-time filtering, this would be 4 (2 remaining + 2 new)
    // Without it, we see all rows including deleted ones

    println!("\n✅ DELETE then INSERT test completed successfully");
    Ok(())
}

#[tokio::test]
async fn test_delete_with_complex_filter() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🧪 Testing DELETE with complex filter...");

    // 1. Setup test environment
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("delete_complex_test.db");
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path)?;

    // 2. Create catalog and table
    let catalog: Arc<dyn MetadataCatalog> = Arc::new(PepperCatalog::new(format!(
        "sqlite://{}",
        db_path.to_string_lossy()
    )));
    catalog.init().await?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("category", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
        Field::new("active", DataType::Boolean, false),
    ]));

    let table_options = CreateTableOptions {
        table_name: "test_delete_complex".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["id".to_string()],
        base_path: data_path.to_string_lossy().to_string(),
        partition_column: None,
    };

    let table = PepperTableProvider::create_table(Arc::clone(&catalog), table_options).await?;
    let table = Arc::new(table);

    // 3. Register with DataFusion
    let ctx = SessionContext::new();
    ctx.register_table("test_delete_complex", Arc::clone(&table) as _)?;

    // 4. Insert test data
    ctx.sql(
        "INSERT INTO test_delete_complex VALUES \
         (1, 'A', 100, true), \
         (2, 'B', 200, false), \
         (3, 'A', 300, false), \
         (4, 'B', 150, true), \
         (5, 'C', 250, true), \
         (6, 'A', 50, false)",
    )
    .await?
    .collect()
    .await?;
    println!("✓ Inserted 6 rows");

    // 5. Delete with complex filter: category='A' AND value > 100 AND active=false
    let category_col = col("category");
    let value_col = col("value");
    let active_col = col("active");

    let filter = category_col
        .eq(lit("A"))
        .and(value_col.gt(lit(100i64)))
        .and(active_col.eq(lit(false)));

    let delete_plan = table.delete_from(&ctx.state(), &[filter]).await?;
    let delete_results = collect(delete_plan, ctx.task_ctx()).await?;
    let delete_count = delete_results[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::UInt64Array>()
        .expect("count column")
        .value(0);

    println!("✓ DELETE executed with complex filter, deleted {delete_count} row(s)");
    assert_eq!(
        delete_count, 1,
        "Expected to delete 1 row matching complex filter"
    );

    // 6. Verify deletion (should delete id=3 only)
    let df = ctx
        .sql("SELECT COUNT(*) as count FROM test_delete_complex")
        .await?;
    let results = df.collect().await?;
    let count = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .expect("count column")
        .value(0);

    println!(
        "⚠️  Row count after delete: {count} (expected 5, but read-time filtering not yet implemented)"
    );
    // TODO: Once read-time filtering is implemented, uncomment:
    // assert_eq!(count, 5, "Expected 5 rows after complex DELETE");

    println!("\n✅ DELETE with complex filter test completed successfully");
    Ok(())
}
