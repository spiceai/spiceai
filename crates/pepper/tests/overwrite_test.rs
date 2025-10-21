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

//! Test INSERT OVERWRITE functionality for Pepper

use arrow::datatypes::{DataType, Field, Schema};
use datafusion::prelude::*;
use pepper::metadata::CreateTableOptions;
use pepper::{MetadataCatalog, PepperCatalog, PepperTableProvider};
use std::sync::Arc;
use tempfile::TempDir;

#[tokio::test]
async fn test_insert_overwrite() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🧪 Testing INSERT OVERWRITE functionality...");

    // 1. Setup test environment
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("overwrite_test.db");
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
        Field::new("value", DataType::Utf8, false),
    ]));

    let table_options = CreateTableOptions {
        table_name: "test_overwrite".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec![],
        base_path: data_path.to_string_lossy().to_string(),
    };

    let table = PepperTableProvider::create_table(Arc::clone(&catalog), table_options).await?;
    println!("✓ Table created");

    // 3. Register with DataFusion context
    let ctx = SessionContext::new();
    ctx.register_table("test_overwrite", Arc::new(table))?;
    println!("✓ Table registered with DataFusion");

    // 4. Initial insert
    ctx.sql("INSERT INTO test_overwrite VALUES (1, 'first'), (2, 'second'), (3, 'third')")
        .await?
        .collect()
        .await?;
    println!("✓ Initial data inserted (3 rows)");

    // 5. Verify initial data
    let df = ctx.sql("SELECT * FROM test_overwrite ORDER BY id").await?;
    let results = df.collect().await?;
    let total_rows: usize = results
        .iter()
        .map(arrow::array::RecordBatch::num_rows)
        .sum();
    assert_eq!(total_rows, 3, "Expected 3 rows after initial insert");
    println!("✓ Initial data verified (3 rows)");

    // 6. Perform INSERT OVERWRITE - this should create a new subdirectory
    println!("\n--- Performing INSERT OVERWRITE ---");

    // Check how many subdirectories exist before overwrite
    let entries_before: Vec<_> = std::fs::read_dir(&data_path)?
        .filter_map(std::result::Result::ok)
        .filter(|e| e.path().is_dir())
        .collect();
    println!(
        "✓ Subdirectories before overwrite: {}",
        entries_before.len()
    );

    ctx.sql("INSERT OVERWRITE test_overwrite VALUES (10, 'new_first'), (20, 'new_second')")
        .await?
        .collect()
        .await?;
    println!("✓ INSERT OVERWRITE completed (2 new rows)");

    // 7. Check that a new subdirectory was created
    let entries_after: Vec<_> = std::fs::read_dir(&data_path)?
        .filter_map(std::result::Result::ok)
        .filter(|e| e.path().is_dir())
        .collect();
    println!("✓ Subdirectories after overwrite: {}", entries_after.len());

    // We should have at least one more directory (the overwrite directory)
    assert!(
        entries_after.len() > entries_before.len(),
        "Expected new subdirectory to be created for overwrite"
    );
    println!("✓ New subdirectory created for overwrite");

    // 8. Verify overwrite replaced the data
    // After overwrite, the provider's listing_table should now point to the new
    // overwrite directory, so queries should only see the new data (2 rows)
    let df = ctx.sql("SELECT * FROM test_overwrite ORDER BY id").await?;
    let results = df.collect().await?;
    let total_rows: usize = results
        .iter()
        .map(arrow::array::RecordBatch::num_rows)
        .sum();

    assert_eq!(
        total_rows, 2,
        "Expected 2 rows after overwrite (old data replaced)"
    );
    println!("✓ Query returned {total_rows} rows after overwrite (old data replaced)");

    // Verify we can see the new data
    let df = ctx
        .sql("SELECT * FROM test_overwrite WHERE id >= 10 ORDER BY id")
        .await?;
    let results = df.collect().await?;
    let new_data_rows: usize = results
        .iter()
        .map(arrow::array::RecordBatch::num_rows)
        .sum();
    assert_eq!(
        new_data_rows, 2,
        "Expected to find 2 new rows with id >= 10"
    );
    println!("✓ New overwrite data is accessible");

    // 9. Verify subdirectory naming
    let overwrite_dirs: Vec<_> = std::fs::read_dir(&data_path)?
        .filter_map(std::result::Result::ok)
        .filter(|e| e.path().is_dir() && e.file_name().to_string_lossy().starts_with("overwrite_"))
        .collect();
    assert!(
        !overwrite_dirs.is_empty(),
        "Expected at least one directory starting with 'overwrite_'"
    );
    println!("✓ Overwrite directory has correct naming (overwrite_*)");

    println!("\n✅ INSERT OVERWRITE test passed!");
    println!(
        "Note: Full overwrite semantics (hiding old data) will require catalog metadata updates"
    );
    Ok(())
}
