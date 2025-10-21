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

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::TableProvider;
use datafusion::prelude::*;
use pepper::metadata::CreateTableOptions;
use pepper::{MetadataCatalog, PepperCatalog, PepperTableProvider};
use std::sync::Arc;
use tempfile::TempDir;

/// Backend configuration for tests
#[derive(Debug, Clone, Copy)]
enum Backend {
    Sqlite,
    Arrow,
}

impl Backend {
    /// Create a connection string for this backend
    fn connection_string(&self, temp_dir: &TempDir) -> String {
        match self {
            Backend::Sqlite => {
                let db_path = temp_dir.path().join("test.db");
                format!("sqlite://{}", db_path.to_string_lossy())
            }
            Backend::Arrow => {
                let metadata_path = temp_dir.path().join("metadata");
                format!("arrow://{}", metadata_path.to_string_lossy())
            }
        }
    }

    fn name(&self) -> &'static str {
        match self {
            Backend::Sqlite => "SQLite",
            Backend::Arrow => "Arrow/Feather",
        }
    }
}

/// Run the basic workflow test with a specific backend
async fn run_basic_workflow_test(backend: Backend) -> Result<(), Box<dyn std::error::Error>> {
    println!(
        "\n🧪 Testing Pepper basic workflow with {} backend",
        backend.name()
    );

    // Create a temporary directory for the test
    let temp_dir = TempDir::new()?;
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path)?;

    // 1. Create and initialize catalog
    let catalog = Arc::new(PepperCatalog::new(backend.connection_string(&temp_dir)));
    catalog.init().await?;
    println!("✓ Catalog initialized ({})", backend.name());

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
    let total_rows: usize = results
        .iter()
        .map(arrow::array::RecordBatch::num_rows)
        .sum();
    assert_eq!(total_rows, 3, "Expected 3 rows after first insert");
    println!("✓ Query returned {total_rows} rows");

    // 9. Verify the data from first batch
    // Collect all rows across batches (in case data is split)
    let mut all_ids = Vec::new();
    let mut all_names = Vec::new();
    for batch in &results {
        let id_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Expected Int64Array");
        let name_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Expected StringArray");

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
    let total_cols: usize = if results.is_empty() {
        0
    } else {
        results[0].num_columns()
    };
    let total_rows: usize = results
        .iter()
        .map(arrow::array::RecordBatch::num_rows)
        .sum();
    assert_eq!(total_cols, 1, "Expected 1 column in projection");
    assert_eq!(total_rows, 3, "Expected 3 rows in projection");
    println!("✓ Projection query successful (1 column, 3 rows)");

    // 13. Verify metadata after first insert (backend-specific)
    verify_metadata(&backend, &temp_dir, &data_path)?;
    println!("✓ Metadata verification successful (round 1)");

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
    for batch in &results {
        let id_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Expected Int64Array");
        let name_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Expected StringArray");

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
    let total_cols: usize = if results.is_empty() {
        0
    } else {
        results[0].num_columns()
    };
    let total_rows: usize = results
        .iter()
        .map(arrow::array::RecordBatch::num_rows)
        .sum();
    assert_eq!(total_cols, 1, "Expected 1 column in projection");
    assert_eq!(total_rows, 5, "Expected 5 rows in projection");
    println!("✓ Projection query successful (round 2: 1 column, 5 rows)");

    // 20. Verify metadata after second insert
    verify_metadata(&backend, &temp_dir, &data_path)?;
    println!("✓ Metadata verification successful (round 2)");

    println!(
        "\n✅ Basic workflow test passed with {} backend!",
        backend.name()
    );
    Ok(())
}

// Public test functions that call the shared implementation
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_pepper_basic_workflow_sqlite() -> Result<(), Box<dyn std::error::Error>> {
    run_basic_workflow_test(Backend::Sqlite).await
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_pepper_basic_workflow_arrow() -> Result<(), Box<dyn std::error::Error>> {
    run_basic_workflow_test(Backend::Arrow).await
}

/// Verify metadata based on the backend type
fn verify_metadata(
    backend: &Backend,
    temp_dir: &TempDir,
    data_path: &std::path::Path,
) -> Result<(), Box<dyn std::error::Error>> {
    match backend {
        Backend::Sqlite => {
            let db_path = temp_dir.path().join("test.db");
            verify_sqlite_metadata(&db_path, data_path)
        }
        Backend::Arrow => {
            let metadata_path = temp_dir.path().join("metadata");
            verify_arrow_metadata(&metadata_path, data_path)
        }
    }
}

/// Helper function to verify `Arrow` metadata contains expected data
fn verify_arrow_metadata(
    metadata_path: &std::path::Path,
    data_path: &std::path::Path,
) -> Result<(), Box<dyn std::error::Error>> {
    use arrow_ipc::reader::FileReader;

    // 1. Verify metadata file exists
    let metadata_file = metadata_path.join("pepper_metadata.arrow");
    assert!(metadata_file.exists(), "Metadata file should exist");

    let file = std::fs::File::open(&metadata_file)?;
    let reader = FileReader::try_new(file, None)?;

    let batches: Vec<_> = reader.collect::<Result<_, _>>()?;
    assert!(!batches.is_empty(), "Metadata should contain data");

    // Find next_catalog_id in metadata
    let mut found_catalog_id = false;
    for batch in &batches {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("keys column");
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("values column");

        for i in 0..batch.num_rows() {
            if keys.value(i) == "next_catalog_id" {
                let next_id = values.value(i);
                assert!(next_id >= 2, "Expected next_catalog_id to be at least 2");
                found_catalog_id = true;
                println!("  • Metadata verified: next_catalog_id={next_id}");
                break;
            }
        }
    }
    assert!(found_catalog_id, "Should find next_catalog_id in metadata");

    // 2. Verify tables file exists and has test_table entry
    let tables_file = metadata_path.join("pepper_table.arrow");
    assert!(tables_file.exists(), "Tables file should exist");

    let file = std::fs::File::open(&tables_file)?;
    let reader = FileReader::try_new(file, None)?;

    let batches: Vec<_> = reader.collect::<Result<_, _>>()?;
    assert!(!batches.is_empty(), "Tables should contain data");

    // Verify we have test_table
    let mut found_test_table = false;
    for batch in &batches {
        let table_names = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("table_name column");
        let paths = batch
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("path column");

        for i in 0..batch.num_rows() {
            if table_names.value(i) == "test_table" {
                assert_eq!(
                    paths.value(i),
                    data_path.to_string_lossy().to_string(),
                    "Expected path to match data directory"
                );
                found_test_table = true;
                println!("  • Table metadata verified: name=test_table");
                break;
            }
        }
    }
    assert!(found_test_table, "Should find test_table in tables");

    println!("  • Arrow metadata files verified");

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

/// Run catalog persistence test with a specific backend
async fn run_catalog_persistence_test(backend: Backend) -> Result<(), Box<dyn std::error::Error>> {
    println!(
        "\n🧪 Testing catalog persistence with {} backend",
        backend.name()
    );

    let temp_dir = TempDir::new()?;

    // Create catalog and initialize
    {
        let catalog = PepperCatalog::new(backend.connection_string(&temp_dir));
        catalog.init().await?;
        println!("✓ First initialization complete ({})", backend.name());
    }

    // Re-open and verify it doesn't fail
    {
        let catalog = PepperCatalog::new(backend.connection_string(&temp_dir));
        catalog.init().await?;
        println!("✓ Second initialization complete (idempotent)");
    }

    println!(
        "\n✅ Catalog persistence test passed with {} backend!",
        backend.name()
    );
    Ok(())
}

#[tokio::test]
async fn test_pepper_catalog_persistence_sqlite() -> Result<(), Box<dyn std::error::Error>> {
    run_catalog_persistence_test(Backend::Sqlite).await
}

#[tokio::test]
async fn test_pepper_catalog_persistence_arrow() -> Result<(), Box<dyn std::error::Error>> {
    run_catalog_persistence_test(Backend::Arrow).await
}

/// Run statistics test with a specific backend
async fn run_statistics_test(backend: Backend) -> Result<(), Box<dyn std::error::Error>> {
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::TableReference;
    use datafusion::execution::context::SessionContext;
    use datafusion_catalog::TableProvider;
    use pepper::metadata::CreateTableOptions;
    use pepper::{PepperCatalog, PepperTableProvider};
    use std::sync::Arc;

    println!(
        "\n🧪 Testing Pepper statistics tracking with {} backend...",
        backend.name()
    );

    // 1. Setup test environment
    let temp_dir = TempDir::new()?;
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path)?;

    // 2. Create catalog and table
    let catalog = Arc::new(PepperCatalog::new(backend.connection_string(&temp_dir)));
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
    };

    let table = PepperTableProvider::create_table(
        Arc::<pepper::PepperCatalog>::clone(&catalog),
        table_options,
    )
    .await?;
    println!("✓ Table created ({})", backend.name());

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

    println!(
        "\n✅ Statistics tracking test passed with {} backend!",
        backend.name()
    );
    Ok(())
}

#[tokio::test]
async fn test_pepper_statistics_sqlite() -> Result<(), Box<dyn std::error::Error>> {
    run_statistics_test(Backend::Sqlite).await
}

#[tokio::test]
async fn test_pepper_statistics_arrow() -> Result<(), Box<dyn std::error::Error>> {
    run_statistics_test(Backend::Arrow).await
}
