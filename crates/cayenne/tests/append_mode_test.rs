/*
Copyright 2026 The Spice.ai OSS Authors

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

//! Tests for Cayenne data accelerator append mode functionality.
//!
//! These tests verify that the Cayenne accelerator correctly handles append operations:
//! - Initial data insertion
//! - Subsequent appends preserve existing data
//! - Data integrity across multiple append operations
//! - Primary key constraints with upsert behavior
//! - Time column-based append filtering

#![allow(clippy::expect_used)]

mod common;

use arrow::array::{Array, Int64Array, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use cayenne::metadata::CreateTableOptions;
use cayenne::{CayenneCatalog, CayenneTableProvider, MetadataCatalog};
use datafusion::prelude::*;
use datafusion_table_providers::util::column_reference::ColumnReference;
use datafusion_table_providers::util::on_conflict::OnConflict;
use std::sync::Arc;
use tempfile::TempDir;

/// Test basic append mode functionality.
/// Verifies that multiple inserts append data without overwriting.
#[tokio::test]
async fn test_cayenne_append_mode_basic() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🧪 Testing Cayenne append mode basic functionality...");

    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("append_test.db");
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path)?;

    // Create catalog and table
    let catalog: Arc<dyn MetadataCatalog> = Arc::new(CayenneCatalog::new(format!(
        "sqlite://{}",
        db_path.to_string_lossy()
    ))?);
    catalog.init().await?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let table_options = CreateTableOptions {
        table_name: "append_test".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec![],
        on_conflict: None,
        base_path: data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    let table = CayenneTableProvider::create_table(Arc::clone(&catalog), table_options).await?;
    println!("✓ Table created");

    let ctx = SessionContext::new();
    ctx.register_table("append_test", Arc::new(table))?;

    // === ROUND 1: Initial insert ===
    println!("\n--- Round 1: Initial insert ---");
    ctx.sql("INSERT INTO append_test VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")
        .await?
        .collect()
        .await?;
    println!("✓ First batch inserted (3 rows)");

    let df = ctx.sql("SELECT COUNT(*) as cnt FROM append_test").await?;
    let results = df.collect().await?;
    let total_rows: usize = results.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 1);
    println!("✓ Count query returned 1 batch");

    let df = ctx.sql("SELECT * FROM append_test ORDER BY id").await?;
    let results = df.collect().await?;
    let total_rows: usize = results.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 3, "Expected 3 rows after first insert");
    println!("✓ Query returned {total_rows} rows after first insert");

    // === ROUND 2: Append more data ===
    println!("\n--- Round 2: Append more data ---");
    ctx.sql("INSERT INTO append_test VALUES (4, 'David'), (5, 'Eve')")
        .await?
        .collect()
        .await?;
    println!("✓ Second batch inserted (2 rows)");

    let df = ctx.sql("SELECT * FROM append_test ORDER BY id").await?;
    let results = df.collect().await?;
    let total_rows: usize = results.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(
        total_rows, 5,
        "Expected 5 rows after append (3 + 2), but got {total_rows}"
    );
    println!("✓ Query returned {total_rows} rows after append");

    // Verify data content
    let mut all_ids = Vec::new();
    let mut all_names = Vec::new();
    for batch in &results {
        let id_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Expected Int64Array for id column");
        let name_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Expected StringArray for name column");

        for i in 0..batch.num_rows() {
            all_ids.push(id_array.value(i));
            all_names.push(name_array.value(i).to_string());
        }
    }

    assert_eq!(all_ids, vec![1, 2, 3, 4, 5]);
    assert_eq!(all_names, vec!["Alice", "Bob", "Charlie", "David", "Eve"]);
    println!("✓ Data content verified");

    // === ROUND 3: Another append ===
    println!("\n--- Round 3: Another append ---");
    ctx.sql("INSERT INTO append_test VALUES (6, 'Frank')")
        .await?
        .collect()
        .await?;
    println!("✓ Third batch inserted (1 row)");

    let df = ctx.sql("SELECT COUNT(*) as cnt FROM append_test").await?;
    let results = df.collect().await?;
    let count_array = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .expect("Expected Int64Array");
    assert_eq!(count_array.value(0), 6, "Expected 6 total rows");
    println!("✓ Total row count is 6");

    println!("\n✅ Cayenne append mode basic test passed!");
    Ok(())
}

/// Test append mode with primary key and upsert behavior.
/// Verifies that duplicate primary keys trigger upsert (update existing row).
#[tokio::test]
async fn test_cayenne_append_mode_with_primary_key_upsert() -> Result<(), Box<dyn std::error::Error>>
{
    println!("\n🧪 Testing Cayenne append mode with primary key upsert...");

    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("append_pk_upsert_test.db");
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path)?;

    let catalog: Arc<dyn MetadataCatalog> = Arc::new(CayenneCatalog::new(format!(
        "sqlite://{}",
        db_path.to_string_lossy()
    ))?);
    catalog.init().await?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int64, true),
    ]));

    let table_options = CreateTableOptions {
        table_name: "upsert_test".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["id".to_string()],
        on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
            "id".to_string()
        ]))),
        base_path: data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    let table = CayenneTableProvider::create_table(Arc::clone(&catalog), table_options).await?;
    println!("✓ Table with primary key created");

    let ctx = SessionContext::new();
    ctx.register_table("upsert_test", Arc::new(table))?;

    // Initial insert
    ctx.sql(
        "INSERT INTO upsert_test VALUES (1, 'Alice', 100), (2, 'Bob', 200), (3, 'Charlie', 300)",
    )
    .await?
    .collect()
    .await?;
    println!("✓ Initial data inserted (3 rows)");

    let df = ctx.sql("SELECT * FROM upsert_test ORDER BY id").await?;
    let results = df.collect().await?;
    let total_rows: usize = results.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 3);
    println!("✓ Initial row count verified");

    // Upsert with duplicate key (id=2 should update)
    ctx.sql("INSERT INTO upsert_test VALUES (2, 'Bobby', 250), (4, 'David', 400)")
        .await?
        .collect()
        .await?;
    println!("✓ Upsert executed (1 update + 1 insert)");

    let df = ctx.sql("SELECT * FROM upsert_test ORDER BY id").await?;
    let results = df.collect().await?;
    let total_rows: usize = results.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(
        total_rows, 4,
        "Expected 4 rows after upsert (3 original + 1 new, 1 updated)"
    );
    println!("✓ Row count after upsert: {total_rows}");

    // Verify the upserted row has new values
    let df = ctx
        .sql("SELECT name, value FROM upsert_test WHERE id = 2")
        .await?;
    let results = df.collect().await?;
    assert_eq!(results.len(), 1);

    let name_array = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("Expected StringArray");
    let value_array = results[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Expected Int64Array");

    assert_eq!(name_array.value(0), "Bobby");
    assert_eq!(value_array.value(0), 250);
    println!("✓ Upserted row has correct updated values");

    println!("\n✅ Cayenne append mode with primary key upsert test passed!");
    Ok(())
}

/// Test append mode with primary key and drop behavior.
/// Verifies that duplicate primary keys are dropped (ignored).
#[tokio::test]
async fn test_cayenne_append_mode_with_primary_key_drop() -> Result<(), Box<dyn std::error::Error>>
{
    println!("\n🧪 Testing Cayenne append mode with primary key drop...");

    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("append_pk_drop_test.db");
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path)?;

    let catalog: Arc<dyn MetadataCatalog> = Arc::new(CayenneCatalog::new(format!(
        "sqlite://{}",
        db_path.to_string_lossy()
    ))?);
    catalog.init().await?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let table_options = CreateTableOptions {
        table_name: "drop_test".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["id".to_string()],
        on_conflict: Some(OnConflict::DoNothingAll),
        base_path: data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    let table = CayenneTableProvider::create_table(Arc::clone(&catalog), table_options).await?;
    println!("✓ Table with drop behavior created");

    let ctx = SessionContext::new();
    ctx.register_table("drop_test", Arc::new(table))?;

    // Initial insert
    ctx.sql("INSERT INTO drop_test VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")
        .await?
        .collect()
        .await?;
    println!("✓ Initial data inserted (3 rows)");

    // Insert with duplicate key (id=2 should be dropped)
    ctx.sql("INSERT INTO drop_test VALUES (2, 'Bobby'), (4, 'David')")
        .await?
        .collect()
        .await?;
    println!("✓ Insert with duplicate executed");

    let df = ctx.sql("SELECT * FROM drop_test ORDER BY id").await?;
    let results = df.collect().await?;
    let total_rows: usize = results.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(
        total_rows, 4,
        "Expected 4 rows (3 original + 1 new, duplicate dropped)"
    );
    println!("✓ Row count after insert with drop: {total_rows}");

    // Verify the original row is preserved
    let df = ctx.sql("SELECT name FROM drop_test WHERE id = 2").await?;
    let results = df.collect().await?;
    let name_array = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("Expected StringArray");
    assert_eq!(
        name_array.value(0),
        "Bob",
        "Original row should be preserved"
    );
    println!("✓ Original row preserved (drop behavior verified)");

    println!("\n✅ Cayenne append mode with primary key drop test passed!");
    Ok(())
}

/// Test append mode with time column for incremental refresh.
/// Verifies that append works correctly with timestamp columns.
#[tokio::test]
async fn test_cayenne_append_mode_with_time_column() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🧪 Testing Cayenne append mode with time column...");

    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("append_time_test.db");
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path)?;

    let catalog: Arc<dyn MetadataCatalog> = Arc::new(CayenneCatalog::new(format!(
        "sqlite://{}",
        db_path.to_string_lossy()
    ))?);
    catalog.init().await?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("event", DataType::Utf8, false),
        Field::new(
            "created_at",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
    ]));

    let table_options = CreateTableOptions {
        table_name: "time_test".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec![],
        on_conflict: None,
        base_path: data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    let table = CayenneTableProvider::create_table(Arc::clone(&catalog), table_options).await?;
    println!("✓ Table with timestamp column created");

    let ctx = SessionContext::new();
    ctx.register_table("time_test", Arc::new(table))?;

    // Insert events with timestamps
    ctx.sql(
        "INSERT INTO time_test VALUES \
         (1, 'login', TIMESTAMP '2024-01-01 10:00:00'), \
         (2, 'purchase', TIMESTAMP '2024-01-01 11:00:00'), \
         (3, 'logout', TIMESTAMP '2024-01-01 12:00:00')",
    )
    .await?
    .collect()
    .await?;
    println!("✓ Initial events inserted");

    let df = ctx.sql("SELECT * FROM time_test ORDER BY id").await?;
    let results = df.collect().await?;
    let total_rows: usize = results.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 3);
    println!("✓ Initial row count verified");

    // Append more events (simulating incremental refresh)
    ctx.sql(
        "INSERT INTO time_test VALUES \
         (4, 'login', TIMESTAMP '2024-01-02 09:00:00'), \
         (5, 'checkout', TIMESTAMP '2024-01-02 10:00:00')",
    )
    .await?
    .collect()
    .await?;
    println!("✓ New events appended");

    let df = ctx.sql("SELECT COUNT(*) as cnt FROM time_test").await?;
    let results = df.collect().await?;
    let count_array = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Expected Int64Array");
    assert_eq!(count_array.value(0), 5);
    println!("✓ Total row count is 5");

    // Query by time range (simulating time-based filtering)
    let df = ctx
        .sql("SELECT * FROM time_test WHERE created_at >= TIMESTAMP '2024-01-02 00:00:00' ORDER BY id")
        .await?;
    let results = df.collect().await?;
    let total_rows: usize = results.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 2, "Expected 2 rows from 2024-01-02");
    println!("✓ Time-based filtering works correctly");

    // Verify timestamps are preserved
    let timestamp_array = results[0]
        .column(2)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .expect("Expected TimestampMicrosecondArray");
    assert!(!timestamp_array.is_null(0));
    println!("✓ Timestamp values preserved");

    println!("\n✅ Cayenne append mode with time column test passed!");
    Ok(())
}

/// Test append mode data integrity across multiple sessions.
/// Verifies that data persists and is accessible from fresh table providers.
#[tokio::test]
async fn test_cayenne_append_mode_persistence() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🧪 Testing Cayenne append mode persistence...");

    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("append_persist_test.db");
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path)?;

    let connection_string = format!("sqlite://{}", db_path.to_string_lossy());

    // Session 1: Create and populate table
    {
        let catalog: Arc<dyn MetadataCatalog> =
            Arc::new(CayenneCatalog::new(connection_string.clone())?);
        catalog.init().await?;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("data", DataType::Utf8, false),
        ]));

        let table_options = CreateTableOptions {
            table_name: "persist_test".to_string(),
            schema: Arc::clone(&schema),
            primary_key: vec![],
            on_conflict: None,
            base_path: data_path.to_string_lossy().to_string(),
            partition_column: None,
            vortex_config: cayenne::metadata::VortexConfig::default(),
        };

        let table = CayenneTableProvider::create_table(Arc::clone(&catalog), table_options).await?;

        let ctx = SessionContext::new();
        ctx.register_table("persist_test", Arc::new(table))?;

        ctx.sql("INSERT INTO persist_test VALUES (1, 'first'), (2, 'second')")
            .await?
            .collect()
            .await?;
        println!("✓ Session 1: Initial data inserted");
    }

    // Session 2: Reconnect and append more data
    {
        let catalog: Arc<dyn MetadataCatalog> =
            Arc::new(CayenneCatalog::new(connection_string.clone())?);
        // Note: no init() needed for existing database

        let table = CayenneTableProvider::new("persist_test", catalog).await?;

        let ctx = SessionContext::new();
        ctx.register_table("persist_test", Arc::new(table))?;

        // Verify existing data
        let df = ctx.sql("SELECT COUNT(*) as cnt FROM persist_test").await?;
        let results = df.collect().await?;
        let count_array = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Expected Int64Array");
        assert_eq!(count_array.value(0), 2);
        println!("✓ Session 2: Existing data verified");

        // Append more data
        ctx.sql("INSERT INTO persist_test VALUES (3, 'third')")
            .await?
            .collect()
            .await?;
        println!("✓ Session 2: New data appended");
    }

    // Session 3: Verify all data persisted
    {
        let catalog: Arc<dyn MetadataCatalog> =
            Arc::new(CayenneCatalog::new(connection_string.clone())?);

        let table = CayenneTableProvider::new("persist_test", catalog).await?;

        let ctx = SessionContext::new();
        ctx.register_table("persist_test", Arc::new(table))?;

        let df = ctx.sql("SELECT * FROM persist_test ORDER BY id").await?;
        let results = df.collect().await?;
        let total_rows: usize = results.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total_rows, 3, "All 3 rows should persist across sessions");
        println!("✓ Session 3: All data persisted ({total_rows} rows)");

        // Verify data content
        let mut all_ids = Vec::new();
        for batch in &results {
            let id_array = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("Expected Int64Array");
            for i in 0..batch.num_rows() {
                all_ids.push(id_array.value(i));
            }
        }
        assert_eq!(all_ids, vec![1, 2, 3]);
        println!("✓ Data content verified");
    }

    println!("\n✅ Cayenne append mode persistence test passed!");
    Ok(())
}

/// Test append mode with large batches.
/// Verifies that append handles larger data volumes correctly.
#[tokio::test]
async fn test_cayenne_append_mode_large_batch() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🧪 Testing Cayenne append mode with large batches...");

    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("append_large_test.db");
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path)?;

    let catalog: Arc<dyn MetadataCatalog> = Arc::new(CayenneCatalog::new(format!(
        "sqlite://{}",
        db_path.to_string_lossy()
    ))?);
    catalog.init().await?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let table_options = CreateTableOptions {
        table_name: "large_test".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec![],
        on_conflict: None,
        base_path: data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    let table = CayenneTableProvider::create_table(Arc::clone(&catalog), table_options).await?;
    println!("✓ Table created");

    let ctx = SessionContext::new();
    ctx.register_table("large_test", Arc::new(table))?;

    // Insert first large batch (500 rows)
    let batch_size = 500;
    let values1: Vec<String> = (0..batch_size)
        .map(|i| format!("({}, {})", i, i * 10))
        .collect();
    let insert_sql = format!("INSERT INTO large_test VALUES {}", values1.join(", "));
    ctx.sql(&insert_sql).await?.collect().await?;
    println!("✓ First batch inserted ({batch_size} rows)");

    let df = ctx.sql("SELECT COUNT(*) as cnt FROM large_test").await?;
    let results = df.collect().await?;
    let count_array = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Expected Int64Array");
    assert_eq!(count_array.value(0), batch_size as i64);
    println!("✓ First batch count verified");

    // Insert second large batch (500 more rows)
    let values2: Vec<String> = (batch_size..batch_size * 2)
        .map(|i| format!("({}, {})", i, i * 10))
        .collect();
    let insert_sql = format!("INSERT INTO large_test VALUES {}", values2.join(", "));
    ctx.sql(&insert_sql).await?.collect().await?;
    println!("✓ Second batch appended ({batch_size} rows)");

    let df = ctx.sql("SELECT COUNT(*) as cnt FROM large_test").await?;
    let results = df.collect().await?;
    let count_array = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Expected Int64Array");
    assert_eq!(count_array.value(0), (batch_size * 2) as i64);
    println!("✓ Total row count: {}", batch_size * 2);

    // Verify data range
    let df = ctx
        .sql("SELECT MIN(id) as min_id, MAX(id) as max_id FROM large_test")
        .await?;
    let results = df.collect().await?;
    let min_array = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Expected Int64Array");
    let max_array = results[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Expected Int64Array");
    assert_eq!(min_array.value(0), 0);
    assert_eq!(max_array.value(0), (batch_size * 2 - 1) as i64);
    println!("✓ Data range verified (0 to {})", batch_size * 2 - 1);

    println!("\n✅ Cayenne append mode large batch test passed!");
    Ok(())
}
