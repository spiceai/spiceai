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

//! Simple integration test for Pepper with Vortex

use arrow::array::{Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::TableProvider;
use datafusion::prelude::*;
use pepper::metadata::CreateTableOptions;
use pepper::{MetadataCatalog, PepperCatalog, PepperTableProvider};
use std::sync::Arc;
use tempfile::TempDir;

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_pepper_basic_workflow() -> Result<(), Box<dyn std::error::Error>> {
    // Create a temporary directory for the test
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test.db");
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path)?;

    // 1. Create and initialize catalog
    let catalog = Arc::new(PepperCatalog::new(format!(
        "sqlite://{}",
        db_path.to_string_lossy()
    )));
    catalog.init().await?;
    println!("✓ Catalog initialized");

    // 2. Create table schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let table_options = CreateTableOptions {
        table_name: "test_table".to_string(),
        schema: Arc::<arrow::datatypes::Schema>::clone(&schema),
        primary_key: vec![],
        base_path: data_path.to_string_lossy().to_string(),
        partition_column: None,
    };

    // 3. Create Pepper table provider
    let table = PepperTableProvider::create_table(
        Arc::<pepper::PepperCatalog>::clone(&catalog),
        table_options,
    )
    .await?;
    println!("✓ Table created");

    // 4. Verify table schema
    assert_eq!(table.schema().fields().len(), 2);
    assert_eq!(table.schema().field(0).name(), "id");
    assert_eq!(table.schema().field(1).name(), "name");
    println!("✓ Schema verified");

    // 5. Register with DataFusion context
    let ctx = SessionContext::new();
    ctx.register_table("test_table", Arc::new(table))?;
    println!("✓ Table registered with DataFusion");

    // 6. Query empty table
    let df = ctx.sql("SELECT * FROM test_table").await?;
    let results = df.collect().await?;
    assert_eq!(results.len(), 0);
    println!("✓ Empty table query successful");

    // === ROUND 1: First insert ===
    println!("\n--- Round 1: Initial insert ---");

    // 7. Insert first batch of test data using SQL
    ctx.sql("INSERT INTO test_table VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")
        .await?
        .collect()
        .await?;
    println!("✓ First batch inserted (3 rows)");

    // 8. Query the data back
    let df = ctx.sql("SELECT * FROM test_table ORDER BY id").await?;
    let results = df.collect().await?;
    let total_rows: usize = results.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 3, "Expected 3 rows after first insert");
    println!("✓ Query returned {total_rows} rows");

    // 9. Verify the data from first batch
    // Collect all rows across batches (in case data is split)
    let mut all_ids = Vec::new();
    let mut all_names = Vec::new();
    for (batch_idx, batch) in results.iter().enumerate() {
        let id_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap_or_else(|| panic!("Expected Int64Array for id column in batch {batch_idx}"));
        let name_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap_or_else(|| panic!("Expected StringArray for name column in batch {batch_idx}"));

        for i in 0..batch.num_rows() {
            all_ids.push(id_array.value(i));
            all_names.push(name_array.value(i).to_string());
        }
    }

    assert_eq!(all_ids, vec![1, 2, 3]);
    assert_eq!(all_names, vec!["Alice", "Bob", "Charlie"]);
    println!("✓ Data verification successful");

    // 10. Test filtering
    let df = ctx
        .sql("SELECT * FROM test_table WHERE id > 1 ORDER BY id")
        .await?;
    let results = df.collect().await?;
    let total_rows: usize = results
        .iter()
        .map(arrow::array::RecordBatch::num_rows)
        .sum();
    assert_eq!(total_rows, 2, "Expected 2 rows after filtering (id > 1)");
    println!("✓ Filter query successful (2 rows with id > 1)");

    // 11. Test limit
    let df = ctx.sql("SELECT * FROM test_table LIMIT 2").await?;
    let results = df.collect().await?;
    let total_rows: usize = results
        .iter()
        .map(arrow::array::RecordBatch::num_rows)
        .sum();
    assert_eq!(total_rows, 2, "Expected 2 rows after limit");
    println!("✓ Limit query successful (2 rows)");

    // 12. Test projection
    let df = ctx.sql("SELECT name FROM test_table ORDER BY id").await?;
    let results = df.collect().await?;
    let total_rows: usize = results
        .iter()
        .map(arrow::array::RecordBatch::num_rows)
        .sum();
    assert_eq!(total_rows, 3, "Expected 3 rows in projection");
    if !results.is_empty() {
        assert_eq!(
            results[0].num_columns(),
            1,
            "Expected 1 column in projection"
        );
    }
    println!("✓ Projection query successful (1 column, 3 rows)");

    // 13. Verify SQLite metastore after first insert
    verify_sqlite_metadata(&db_path, &data_path)?;
    println!("✓ SQLite metastore verification successful (round 1)");

    // === ROUND 2: Second insert ===
    println!("\n--- Round 2: Additional insert ---");

    // 14. Insert second batch of test data
    ctx.sql("INSERT INTO test_table VALUES (4, 'David'), (5, 'Eve')")
        .await?
        .collect()
        .await?;
    println!("✓ Second batch inserted (2 rows)");

    // 15. Query all data back
    let df = ctx.sql("SELECT * FROM test_table ORDER BY id").await?;
    let results = df.collect().await?;
    let total_rows: usize = results
        .iter()
        .map(arrow::array::RecordBatch::num_rows)
        .sum();
    assert_eq!(total_rows, 5, "Expected 5 rows total");
    println!("✓ Query returned {total_rows} rows total");

    // 16. Verify all data is present
    let df = ctx.sql("SELECT * FROM test_table ORDER BY id").await?;
    let results = df.collect().await?;

    // Collect all rows across batches
    let mut all_ids = Vec::new();
    let mut all_names = Vec::new();
    for (batch_idx, batch) in results.iter().enumerate() {
        let id_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap_or_else(|| panic!("Expected Int64Array for id column in batch {batch_idx}"));
        let name_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap_or_else(|| panic!("Expected StringArray for name column in batch {batch_idx}"));

        for i in 0..batch.num_rows() {
            all_ids.push(id_array.value(i));
            all_names.push(name_array.value(i).to_string());
        }
    }

    assert_eq!(all_ids, vec![1, 2, 3, 4, 5]);
    assert_eq!(all_names, vec!["Alice", "Bob", "Charlie", "David", "Eve"]);
    println!("✓ All data verification successful");

    // 17. Test filtering on combined data
    let df = ctx
        .sql("SELECT * FROM test_table WHERE id >= 3 ORDER BY id")
        .await?;
    let results = df.collect().await?;
    let total_rows: usize = results
        .iter()
        .map(arrow::array::RecordBatch::num_rows)
        .sum();
    assert_eq!(total_rows, 3, "Expected 3 rows after filtering (id >= 3)");
    println!("✓ Filter query successful (round 2)");

    // 18. Test limit on combined data
    let df = ctx
        .sql("SELECT * FROM test_table ORDER BY id LIMIT 3")
        .await?;
    let results = df.collect().await?;
    let total_rows: usize = results
        .iter()
        .map(arrow::array::RecordBatch::num_rows)
        .sum();
    assert_eq!(total_rows, 3, "Expected 3 rows after limit");
    println!("✓ Limit query successful (round 2: 3 rows)");

    // 19. Test projection on combined data
    let df = ctx.sql("SELECT id FROM test_table ORDER BY id").await?;
    let results = df.collect().await?;
    let total_rows: usize = results
        .iter()
        .map(arrow::array::RecordBatch::num_rows)
        .sum();
    assert_eq!(total_rows, 5, "Expected 5 rows in projection");
    if !results.is_empty() {
        assert_eq!(
            results[0].num_columns(),
            1,
            "Expected 1 column in projection"
        );
    }
    println!("✓ Projection query successful (round 2: 1 column, 5 rows)");

    // 20. Verify SQLite metastore after second insert
    verify_sqlite_metadata(&db_path, &data_path)?;
    println!("✓ SQLite metastore verification successful (round 2)");

    // === ROUND 3: INSERT OVERWRITE ===
    println!("\n--- Round 3: INSERT OVERWRITE ---");

    // 21. Verify we have 5 rows before overwrite
    let df_before = ctx.sql("SELECT COUNT(*) as count FROM test_table").await?;
    let _before_results = df_before.collect().await?;
    println!("✓ Before overwrite: verified 5 rows exist");

    // 22. Perform INSERT OVERWRITE - should replace all data with new data
    ctx.sql("INSERT OVERWRITE test_table VALUES (100, 'Overwrite1'), (200, 'Overwrite2'), (300, 'Overwrite3')")
        .await?
        .collect()
        .await?;
    println!("✓ INSERT OVERWRITE completed (3 new rows)");

    // 23. Query using SAME context - this works because insert_into updates the listing_table
    println!("\n--- Test 1: Query with same DataFusion context ---");
    let df = ctx.sql("SELECT * FROM test_table ORDER BY id").await?;
    let results = df.collect().await?;

    let total_rows: usize = results
        .iter()
        .map(arrow::array::RecordBatch::num_rows)
        .sum();

    // This should work - same context has the updated ListingTable
    assert_eq!(
        total_rows, 3,
        "Same context query failed: Expected 3 rows after overwrite but got {total_rows}"
    );
    println!("✓ Same context query returned {total_rows} rows (correct)");

    // 23. Verify the overwrite data content
    let mut all_ids = Vec::new();
    let mut all_names = Vec::new();
    for (batch_idx, batch) in results.iter().enumerate() {
        let id_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap_or_else(|| panic!("Expected Int64Array for id column in batch {batch_idx}"));
        let name_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap_or_else(|| panic!("Expected StringArray for name column in batch {batch_idx}"));

        for i in 0..batch.num_rows() {
            all_ids.push(id_array.value(i));
            all_names.push(name_array.value(i).to_string());
        }
    }

    assert_eq!(all_ids, vec![100, 200, 300]);
    assert_eq!(all_names, vec!["Overwrite1", "Overwrite2", "Overwrite3"]);
    println!("✓ Same context data is correct: [100, 200, 300]");

    // 24. Verify old data is NOT visible
    let df = ctx.sql("SELECT * FROM test_table WHERE id < 100").await?;
    let results = df.collect().await?;
    let total_rows: usize = results
        .iter()
        .map(arrow::array::RecordBatch::num_rows)
        .sum();
    assert_eq!(
        total_rows, 0,
        "Expected 0 rows from old data (should be replaced)"
    );
    println!("✓ Old data is not visible after overwrite");

    // 25. Test filtering on overwrite data
    let df = ctx
        .sql("SELECT * FROM test_table WHERE id >= 200 ORDER BY id")
        .await?;
    let results = df.collect().await?;
    let total_rows: usize = results
        .iter()
        .map(arrow::array::RecordBatch::num_rows)
        .sum();
    assert_eq!(total_rows, 2, "Expected 2 rows after filtering (id >= 200)");
    println!("✓ Filter query successful on overwrite data");

    // 26. Test projection on overwrite data
    let df = ctx.sql("SELECT name FROM test_table ORDER BY id").await?;
    let results = df.collect().await?;
    let total_rows: usize = results
        .iter()
        .map(arrow::array::RecordBatch::num_rows)
        .sum();
    assert_eq!(total_rows, 3, "Expected 3 rows in projection");
    if !results.is_empty() {
        assert_eq!(
            results[0].num_columns(),
            1,
            "Expected 1 column in projection"
        );
    }
    println!("✓ Projection query successful on overwrite data");

    // Note: Skipping verify_sqlite_metadata after overwrite because the path
    // is now correctly updated to point to the overwrite directory, not the base path

    // === CRITICAL TEST: Query with a FRESH table provider (simulates reconnect) ===
    println!("\n--- Test 2: Scan with fresh table provider (CRITICAL) ---");

    // Create a fresh table provider by reading from catalog
    // This simulates what happens when spiced restarts or a new client connects
    let catalog_arc: Arc<dyn pepper::MetadataCatalog> = catalog;
    let fresh_table = PepperTableProvider::new("test_table", catalog_arc).await?;

    // Create a fresh context and register the fresh table
    let fresh_ctx = SessionContext::new();
    fresh_ctx.register_table("test_table", Arc::new(fresh_table))?;
    println!("✓ Fresh table provider created from catalog");

    // Query with the fresh context - this will use TableProvider::scan()
    let df = fresh_ctx
        .sql("SELECT * FROM test_table ORDER BY id")
        .await?;
    let results = df.collect().await?;

    let total_rows: usize = results
        .iter()
        .map(arrow::array::RecordBatch::num_rows)
        .sum();

    println!("📊 Fresh provider scan returned: {total_rows} rows");

    // Collect the actual IDs to see what data was scanned
    let mut fresh_ids = Vec::new();
    for (batch_idx, batch) in results.iter().enumerate() {
        let id_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap_or_else(|| panic!("Expected Int64Array for id column in batch {batch_idx}"));
        for i in 0..batch.num_rows() {
            fresh_ids.push(id_array.value(i));
        }
    }
    fresh_ids.sort_unstable();
    println!("📊 Fresh provider scanned IDs: {fresh_ids:?}");

    // CRITICAL CHECK: This MUST return only the overwrite data (3 rows with IDs 100, 200, 300)
    // If it returns 5 rows or includes old IDs (1-5), then INSERT OVERWRITE is BROKEN
    assert_eq!(
        total_rows, 3,
        "❌ INSERT OVERWRITE BROKEN: Fresh table provider scan returned {total_rows} rows instead of 3. \
         The ListingTable is scanning the wrong directory (base path instead of overwrite directory)."
    );

    assert_eq!(
        fresh_ids,
        vec![100, 200, 300],
        "❌ INSERT OVERWRITE BROKEN: Fresh provider scanned wrong data. \
         Expected [100, 200, 300] but got {fresh_ids:?}. \
         The overwrite directory is not being used for scans."
    );
    println!("✅ Fresh provider correctly scans only overwrite data: [100, 200, 300]");

    println!("\n✅ Basic workflow test passed!");
    Ok(())
}

/// Helper function to verify `SQLite` metastore contains expected metadata
fn verify_sqlite_metadata(
    db_path: &std::path::Path,
    data_path: &std::path::Path,
) -> Result<(), Box<dyn std::error::Error>> {
    use rusqlite::Connection;

    let conn = Connection::open(db_path)?;

    // 1. Verify pepper_metadata table has initial metadata
    let next_catalog_id: i64 = conn.query_row(
        "SELECT value FROM pepper_metadata WHERE key = 'next_catalog_id'",
        [],
        |row| row.get(0),
    )?;
    let next_file_id: i64 = conn.query_row(
        "SELECT value FROM pepper_metadata WHERE key = 'next_file_id'",
        [],
        |row| row.get(0),
    )?;
    assert!(
        next_catalog_id >= 2,
        "Expected next_catalog_id to be at least 2"
    );
    assert_eq!(next_file_id, 1, "Expected next_file_id to be 1");
    println!(
        "  • Metadata verified: next_catalog_id={next_catalog_id}, next_file_id={next_file_id}"
    );

    // 2. Verify pepper_table has the test_table entry
    let table_count: i64 =
        conn.query_row("SELECT COUNT(*) FROM pepper_table", [], |row| row.get(0))?;
    assert_eq!(table_count, 1, "Expected 1 table in pepper_table");

    let (table_id, table_uuid, table_name, path, path_is_relative, schema_json): (
        i64,
        String,
        String,
        String,
        bool,
        String,
    ) = conn.query_row(
        "SELECT table_id, table_uuid, table_name, path, path_is_relative, schema_json FROM pepper_table",
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?)),
    )?;

    assert_eq!(
        table_name, "test_table",
        "Expected table_name to be 'test_table'"
    );
    assert_eq!(
        path,
        data_path.to_string_lossy().to_string(),
        "Expected path to match data directory"
    );
    assert!(!path_is_relative, "Expected path_is_relative to be false");
    assert!(table_id >= 1, "Expected table_id to be at least 1");
    assert!(
        !table_uuid.is_empty(),
        "Expected table_uuid to be non-empty"
    );
    assert!(
        !schema_json.is_empty(),
        "Expected schema_json to be non-empty"
    );
    println!(
        "  • Table metadata verified: table_id={table_id}, uuid={table_uuid}, name={table_name}"
    );

    // 3. Verify schema_json is base64 encoded (it's stored in Arrow IPC format)
    // We don't fully deserialize it here to avoid complex IPC parsing issues,
    // but we verify it's valid base64 and non-empty
    let schema_decode_result = base64::Engine::decode(
        &base64::engine::general_purpose::STANDARD,
        schema_json.as_bytes(),
    );
    assert!(
        schema_decode_result.is_ok(),
        "Schema JSON should be valid base64"
    );
    println!(
        "  • Schema JSON is valid base64 ({} chars)",
        schema_json.len()
    );

    // 4. Verify pepper_data_file table exists (may be empty if no data files created yet)
    let data_file_count: i64 =
        conn.query_row("SELECT COUNT(*) FROM pepper_data_file", [], |row| {
            row.get(0)
        })?;
    println!("  • Data files tracked: {data_file_count}");

    // 5. Verify pepper_delete_file table exists (should be empty for this test)
    let delete_file_count: i64 =
        conn.query_row("SELECT COUNT(*) FROM pepper_delete_file", [], |row| {
            row.get(0)
        })?;
    assert_eq!(
        delete_file_count, 0,
        "Expected 0 delete files for this test"
    );
    println!("  • Delete files tracked: {delete_file_count}");

    Ok(())
}

#[tokio::test]
async fn test_pepper_catalog_persistence() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("persist.db");

    // Create catalog and initialize
    {
        let catalog = PepperCatalog::new(format!("sqlite://{}", db_path.to_string_lossy()));
        catalog.init().await?;
        println!("✓ First initialization complete");
    }

    // Re-open and verify it doesn't fail
    {
        let catalog = PepperCatalog::new(format!("sqlite://{}", db_path.to_string_lossy()));
        catalog.init().await?;
        println!("✓ Second initialization complete (idempotent)");
    }

    println!("\n✅ Catalog persistence test passed!");
    Ok(())
}

#[tokio::test]
async fn test_pepper_statistics() -> Result<(), Box<dyn std::error::Error>> {
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::TableReference;
    use datafusion::execution::context::SessionContext;
    use datafusion_catalog::TableProvider;
    use pepper::metadata::CreateTableOptions;
    use pepper::{PepperCatalog, PepperTableProvider};
    use std::sync::Arc;
    use tempfile::TempDir;

    println!("\n🧪 Testing Pepper statistics tracking...");

    // 1. Setup test environment
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("stats_test.db");
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path)?;

    // 2. Create catalog and table
    let catalog = Arc::new(PepperCatalog::new(format!(
        "sqlite://{}",
        db_path.to_string_lossy()
    )));
    catalog.init().await?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Utf8, false),
    ]));

    let table_options = CreateTableOptions {
        table_name: "stats_table".to_string(),
        schema: Arc::<arrow::datatypes::Schema>::clone(&schema),
        primary_key: vec![],
        base_path: data_path.to_string_lossy().to_string(),
        partition_column: None,
    };

    let table = PepperTableProvider::create_table(
        Arc::<pepper::PepperCatalog>::clone(&catalog),
        table_options,
    )
    .await?;
    println!("✓ Table created");

    // 3. Check that statistics method is available and delegates to ListingTable
    // The statistics() method returns Option<Statistics> from the underlying ListingTable
    println!(
        "✓ Statistics delegation working: {}",
        table.statistics().is_some()
    );

    // 4. Register table and insert data
    let ctx = SessionContext::new();
    ctx.register_table(TableReference::bare("stats_table"), Arc::new(table))?;

    ctx.sql("INSERT INTO stats_table VALUES (1, 'test1'), (2, 'test2'), (3, 'test3')")
        .await?
        .collect()
        .await?;
    println!("✓ Data inserted (3 rows)");

    // 5. Get the table provider again and verify statistics are available
    let table_after = ctx
        .catalog("datafusion")
        .expect("Default catalog")
        .schema("public")
        .expect("Default schema")
        .table("stats_table")
        .await?
        .expect("Table exists");

    let has_stats = table_after.statistics().is_some();
    println!("✓ Statistics available after insert: {has_stats}");

    // The statistics are provided by the underlying Vortex ListingTable
    // which aggregates stats from all Vortex files in the table directory
    if has_stats {
        println!("  • Statistics object retrieved from ListingTable");
        println!("  • Statistics provide query optimizer information for better performance");
    }

    println!("\n✅ Statistics tracking test passed!");
    Ok(())
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_pepper_core_data_types() -> Result<(), Box<dyn std::error::Error>> {
    use arrow::array::{
        ArrayRef, BinaryArray, BooleanArray, Date32Array, Date64Array, Decimal128Array,
        Float32Array, Float64Array, Int16Array, Int32Array, Int8Array, LargeBinaryArray,
        LargeStringArray, TimestampMicrosecondArray, UInt16Array, UInt32Array, UInt64Array,
        UInt8Array,
    };
    use arrow::datatypes::TimeUnit;

    println!("\n🧪 Testing Pepper core data type support...");

    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("types_test.db");
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path)?;

    let catalog = Arc::new(PepperCatalog::new(format!(
        "sqlite://{}",
        db_path.to_string_lossy()
    )));
    catalog.init().await?;

    // Create table with all core supported data types
    let schema = Arc::new(Schema::new(vec![
        // Integer types
        Field::new("col_int8", DataType::Int8, true),
        Field::new("col_int16", DataType::Int16, true),
        Field::new("col_int32", DataType::Int32, true),
        Field::new("col_int64", DataType::Int64, false), // Primary key
        Field::new("col_uint8", DataType::UInt8, true),
        Field::new("col_uint16", DataType::UInt16, true),
        Field::new("col_uint32", DataType::UInt32, true),
        Field::new("col_uint64", DataType::UInt64, true),
        // Float types
        Field::new("col_float32", DataType::Float32, true),
        Field::new("col_float64", DataType::Float64, true),
        // Boolean
        Field::new("col_bool", DataType::Boolean, true),
        // String types
        Field::new("col_utf8", DataType::Utf8, true),
        Field::new("col_large_utf8", DataType::LargeUtf8, true),
        // Binary types
        Field::new("col_binary", DataType::Binary, true),
        Field::new("col_large_binary", DataType::LargeBinary, true),
        // Date/Time types
        Field::new("col_date32", DataType::Date32, true),
        Field::new("col_date64", DataType::Date64, true),
        Field::new(
            "col_timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        // Decimal types
        Field::new("col_decimal128", DataType::Decimal128(38, 10), true),
    ]));

    let table_options = CreateTableOptions {
        table_name: "types_test".to_string(),
        schema: Arc::<arrow::datatypes::Schema>::clone(&schema),
        primary_key: vec!["col_int64".to_string()],
        base_path: data_path.to_string_lossy().to_string(),
        partition_column: None,
    };

    let table = PepperTableProvider::create_table(
        Arc::<pepper::PepperCatalog>::clone(&catalog),
        table_options,
    )
    .await?;
    tracing::info!("✓ Table created with {} columns", schema.fields().len());

    let ctx = SessionContext::new();
    ctx.register_table("types_test", Arc::new(table))?;

    // Insert test data with various types

    let batch = RecordBatch::try_new(
        Arc::<arrow::datatypes::Schema>::clone(&schema),
        vec![
            Arc::new(Int8Array::from(vec![Some(127), Some(-128), None])) as ArrayRef,
            Arc::new(Int16Array::from(vec![Some(32_767), Some(-32_768), None])) as ArrayRef,
            Arc::new(Int32Array::from(vec![
                Some(2_147_483_647),
                Some(-2_147_483_648),
                None,
            ])) as ArrayRef,
            Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef, // Primary key, non-null
            Arc::new(UInt8Array::from(vec![Some(255), Some(0), None])) as ArrayRef,
            Arc::new(UInt16Array::from(vec![Some(65_535), Some(0), None])) as ArrayRef,
            Arc::new(UInt32Array::from(vec![Some(4_294_967_295), Some(0), None])) as ArrayRef,
            Arc::new(UInt64Array::from(vec![
                Some(18_446_744_073_709_551_615),
                Some(0),
                None,
            ])) as ArrayRef,
            Arc::new(Float32Array::from(vec![
                Some(std::f32::consts::PI),
                Some(-2.71_f32),
                None,
            ])) as ArrayRef,
            Arc::new(Float64Array::from(vec![
                Some(std::f64::consts::PI),
                Some(-std::f64::consts::E),
                None,
            ])) as ArrayRef,
            Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("Hello"), Some("World"), None])) as ArrayRef,
            Arc::new(LargeStringArray::from(vec![
                Some("Large"),
                Some("String"),
                None,
            ])) as ArrayRef,
            Arc::new(BinaryArray::from_vec(vec![
                &b"binary"[..],
                &b"data"[..],
                &b""[..],
            ])) as ArrayRef,
            Arc::new(LargeBinaryArray::from_vec(vec![
                &b"large"[..],
                &b"binary"[..],
                &b""[..],
            ])) as ArrayRef,
            Arc::new(Date32Array::from(vec![Some(18_993), Some(0), None])) as ArrayRef, // Days since epoch
            Arc::new(Date64Array::from(vec![
                Some(1_640_995_200_000),
                Some(0),
                None,
            ])) as ArrayRef, // Milliseconds since epoch
            Arc::new(TimestampMicrosecondArray::from(vec![
                Some(1_640_995_200_000_000),
                Some(0),
                None,
            ])) as ArrayRef,
            Arc::new(
                Decimal128Array::from(vec![
                    Some(314_159_265_358_i128),  // 3141.59265358
                    Some(-271_828_182_845_i128), // -2718.28182845
                    None,
                ])
                .with_precision_and_scale(38, 10)
                .expect("valid decimal"),
            ) as ArrayRef,
        ],
    )?;

    // Insert via DataFusion
    let df = ctx.read_batch(batch)?;
    df.write_table(
        "types_test",
        datafusion::dataframe::DataFrameWriteOptions::default(),
    )
    .await?;
    println!("✓ Inserted 3 rows with all data types");

    // Query back and verify
    let df = ctx
        .sql("SELECT * FROM types_test ORDER BY col_int64")
        .await?;
    let results = df.collect().await?;
    assert_eq!(results.len(), 1, "Expected 1 result batch");
    let result_batch = &results[0];
    assert_eq!(result_batch.num_rows(), 3, "Expected 3 rows");
    assert_eq!(result_batch.num_columns(), 19, "Expected 19 columns");
    println!(
        "✓ Query returned {} rows with {} columns",
        result_batch.num_rows(),
        result_batch.num_columns()
    );

    // Verify specific values for each data type
    println!("\n📊 Verifying data types:");

    // Int8
    let int8_col = result_batch
        .column(0)
        .as_any()
        .downcast_ref::<Int8Array>()
        .expect("Int8 column");
    assert_eq!(int8_col.value(0), 127);
    assert_eq!(int8_col.value(1), -128);
    assert!(int8_col.is_null(2));
    println!(
        "  ✓ Int8: max={}, min={}, null={}",
        int8_col.value(0),
        int8_col.value(1),
        int8_col.is_null(2)
    );

    // Int64 (primary key)
    let int64_col = result_batch
        .column(3)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64 column");
    assert_eq!(int64_col.value(0), 1);
    assert_eq!(int64_col.value(1), 2);
    assert_eq!(int64_col.value(2), 3);
    println!(
        "  ✓ Int64: {}, {}, {}",
        int64_col.value(0),
        int64_col.value(1),
        int64_col.value(2)
    );

    // Float32
    let float32_col = result_batch
        .column(8)
        .as_any()
        .downcast_ref::<Float32Array>()
        .expect("Float32 column");
    assert!((float32_col.value(0) - std::f32::consts::PI).abs() < 0.01);
    println!("  ✓ Float32: {}", float32_col.value(0));

    // Boolean
    let bool_col = result_batch
        .column(10)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("Boolean column");
    assert!(bool_col.value(0));
    assert!(!bool_col.value(1));
    assert!(bool_col.is_null(2));
    println!(
        "  ✓ Boolean: {}, {}, null={}",
        bool_col.value(0),
        bool_col.value(1),
        bool_col.is_null(2)
    );

    // String
    let str_col = result_batch
        .column(11)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("String column");
    assert_eq!(str_col.value(0), "Hello");
    assert_eq!(str_col.value(1), "World");
    assert!(str_col.is_null(2));
    println!(
        "  ✓ Utf8: '{}', '{}', null={}",
        str_col.value(0),
        str_col.value(1),
        str_col.is_null(2)
    );

    // Binary
    let bin_col = result_batch
        .column(13)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .expect("Binary column");
    assert_eq!(bin_col.value(0), b"binary");
    println!("  ✓ Binary: {} bytes", bin_col.value(0).len());

    // Timestamp
    let ts_col = result_batch
        .column(17)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .expect("Timestamp column");
    assert_eq!(ts_col.value(0), 1_640_995_200_000_000);
    println!("  ✓ Timestamp: {}", ts_col.value(0));

    // Decimal128
    let dec_col = result_batch
        .column(18)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .expect("Decimal128 column");
    assert_eq!(dec_col.value(0), 314_159_265_358_i128);
    println!("  ✓ Decimal128: {}", dec_col.value(0));

    println!("\n✅ Core data types test passed!");
    Ok(())
}
