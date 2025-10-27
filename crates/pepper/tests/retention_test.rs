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

//! Test `retention_sql` for Pepper
//!
//! These tests verify that Pepper can delete data based on `retention_sql` expressions.

use arrow::datatypes::{DataType, Field, Schema};
use data_components::delete::DeletionTableProvider;
use datafusion::prelude::*;
use datafusion_physical_plan::collect;
use pepper::metadata::CreateTableOptions;
use pepper::{MetadataCatalog, PepperCatalog, PepperTableProvider};
use std::convert::TryFrom;
use std::sync::Arc;
use tempfile::TempDir;

/// Test that retention_sql-style DELETE expressions work with Pepper.
#[tokio::test]
async fn test_retention_sql_basic() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🧪 Testing retention SQL basic functionality...");

    // 1. Setup test environment
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("retention_test.db");
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
        Field::new("active", DataType::Boolean, false),
        Field::new("timestamp", DataType::Int64, false), // Unix timestamp
    ]));

    let table_options = CreateTableOptions {
        table_name: "test_retention".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["id".to_string()],
        base_path: data_path.to_string_lossy().to_string(),
    };

    let table = PepperTableProvider::create_table(Arc::clone(&catalog), table_options).await?;
    let table = Arc::new(table);
    println!("✓ Table created");

    // 3. Register with DataFusion
    let ctx = SessionContext::new();
    ctx.register_table("test_retention", Arc::clone(&table) as _)?;

    // 4. Insert test data (some active, some inactive)
    let now = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs(),
    )?;
    let old_timestamp = now - (30 * 24 * 60 * 60); // 30 days ago
    let recent_timestamp = now - (5 * 24 * 60 * 60); // 5 days ago

    ctx.sql(&format!(
        "INSERT INTO test_retention VALUES \
         (1, 'Alice', true, {recent_timestamp}), \
         (2, 'Bob', false, {old_timestamp}), \
         (3, 'Charlie', true, {recent_timestamp}), \
         (4, 'Diana', false, {old_timestamp}), \
         (5, 'Eve', true, {recent_timestamp})"
    ))
    .await?
    .collect()
    .await?;
    println!("✓ Inserted 5 rows");

    // 5. Verify initial count
    let df = ctx
        .sql("SELECT COUNT(*) as count FROM test_retention")
        .await?;
    let results = df.collect().await?;
    let count = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .expect("count column")
        .value(0);
    assert_eq!(count, 5, "Expected 5 rows initially");
    println!("✓ Verified 5 rows exist");

    // 6. Delete inactive records (simulating retention_sql: "DELETE FROM test_retention WHERE active = false")
    println!("\n--- Test: Retention SQL to delete inactive records ---");
    let active_col = col("active");
    let filter = active_col.eq(lit(false));

    let delete_plan = table.delete_from(&ctx.state(), &[filter]).await?;
    let delete_results = collect(delete_plan, ctx.task_ctx()).await?;
    let delete_count = delete_results[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::UInt64Array>()
        .expect("count column")
        .value(0);

    println!("✓ Deleted {delete_count} inactive record(s)");
    assert_eq!(delete_count, 2, "Expected to delete 2 inactive records");

    println!("\n✅ Retention SQL basic test completed successfully");
    Ok(())
}

/// Test time-based retention (delete old records).
#[tokio::test]
async fn test_retention_sql_time_based() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🧪 Testing time-based retention SQL...");

    // 1. Setup test environment
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("retention_time_test.db");
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
        Field::new("data", DataType::Utf8, false),
        Field::new("created_at", DataType::Int64, false), // Unix timestamp in seconds
    ]));

    let table_options = CreateTableOptions {
        table_name: "test_time_retention".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["id".to_string()],
        base_path: data_path.to_string_lossy().to_string(),
    };

    let table = PepperTableProvider::create_table(Arc::clone(&catalog), table_options).await?;
    let table = Arc::new(table);
    println!("✓ Table created");

    // 3. Register with DataFusion
    let ctx = SessionContext::new();
    ctx.register_table("test_time_retention", Arc::clone(&table) as _)?;

    // 4. Insert test data with different timestamps
    let now = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs(),
    )?;

    let very_old = now - (60 * 24 * 60 * 60); // 60 days ago
    let old = now - (20 * 24 * 60 * 60); // 20 days ago
    let recent = now - (5 * 24 * 60 * 60); // 5 days ago

    ctx.sql(&format!(
        "INSERT INTO test_time_retention VALUES \
         (1, 'very_old', {very_old}), \
         (2, 'old', {old}), \
         (3, 'recent', {recent}), \
         (4, 'old2', {old}), \
         (5, 'recent2', {recent})"
    ))
    .await?
    .collect()
    .await?;
    println!("✓ Inserted 5 rows with different timestamps");

    // 5. Delete records older than 15 days (simulating retention period)
    println!("\n--- Test: Delete records older than 15 days ---");
    let cutoff_timestamp = now - (15 * 24 * 60 * 60);
    let created_at_col = col("created_at");
    let filter = created_at_col.lt(lit(cutoff_timestamp));

    let delete_plan = table.delete_from(&ctx.state(), &[filter]).await?;
    let delete_results = collect(delete_plan, ctx.task_ctx()).await?;
    let delete_count = delete_results[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::UInt64Array>()
        .expect("count column")
        .value(0);

    println!("✓ Deleted {delete_count} old record(s)");
    assert_eq!(
        delete_count, 3,
        "Expected to delete 3 old records (very_old, old, old2)"
    );

    println!("\n✅ Time-based retention test completed successfully");
    Ok(())
}

/// Test complex retention SQL with multiple conditions.
#[tokio::test]
async fn test_retention_sql_complex() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🧪 Testing complex retention SQL...");

    // 1. Setup test environment
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("retention_complex_test.db");
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
        Field::new("status", DataType::Utf8, false),
        Field::new("archived", DataType::Boolean, false),
        Field::new("score", DataType::Int64, false),
    ]));

    let table_options = CreateTableOptions {
        table_name: "test_complex_retention".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["id".to_string()],
        base_path: data_path.to_string_lossy().to_string(),
    };

    let table = PepperTableProvider::create_table(Arc::clone(&catalog), table_options).await?;
    let table = Arc::new(table);
    println!("✓ Table created");

    // 3. Register with DataFusion
    let ctx = SessionContext::new();
    ctx.register_table("test_complex_retention", Arc::clone(&table) as _)?;

    // 4. Insert test data
    ctx.sql(
        "INSERT INTO test_complex_retention VALUES \
         (1, 'active', false, 100), \
         (2, 'inactive', true, 50), \
         (3, 'active', true, 80), \
         (4, 'inactive', false, 30), \
         (5, 'active', false, 90), \
         (6, 'inactive', true, 20)",
    )
    .await?
    .collect()
    .await?;
    println!("✓ Inserted 6 rows");

    // 5. Delete records that are (archived = true AND score < 60) OR status = 'inactive'
    // This simulates: DELETE FROM table WHERE (archived = true AND score < 60) OR status = 'inactive'
    println!("\n--- Test: Complex retention condition ---");

    let status_col = col("status");
    let archived_col = col("archived");
    let score_col = col("score");

    let condition1 = archived_col.eq(lit(true)).and(score_col.lt(lit(60i64)));
    let condition2 = status_col.eq(lit("inactive"));
    let filter = condition1.or(condition2);

    let delete_plan = table.delete_from(&ctx.state(), &[filter]).await?;
    let delete_results = collect(delete_plan, ctx.task_ctx()).await?;
    let delete_count = delete_results[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::UInt64Array>()
        .expect("count column")
        .value(0);

    println!("✓ Deleted {delete_count} record(s)");
    // Should delete: id=2 (inactive+archived), id=4 (inactive), id=6 (inactive+archived+low_score)
    assert_eq!(
        delete_count, 3,
        "Expected to delete 3 records matching complex conditions"
    );

    println!("\n✅ Complex retention SQL test completed successfully");
    Ok(())
}
