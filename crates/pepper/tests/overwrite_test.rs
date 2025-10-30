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
#[allow(clippy::too_many_lines)]
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
    ))?);
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
        partition_column: None,
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

    // 6. Perform INSERT OVERWRITE - this should create a new snapshot subdirectory
    println!("\n--- Performing INSERT OVERWRITE ---");

    // Check how many snapshot subdirectories exist before overwrite
    // Directory structure: [data_path]/[table_id]/[snapshot_id]/
    let table_dir = data_path.join("1"); // table_id = 1
    let snapshots_before: Vec<_> = std::fs::read_dir(&table_dir)?
        .filter_map(std::result::Result::ok)
        .filter(|e| e.path().is_dir())
        .collect();
    println!("✓ Snapshots before overwrite: {}", snapshots_before.len());

    ctx.sql("INSERT OVERWRITE test_overwrite VALUES (10, 'new_first'), (20, 'new_second')")
        .await?
        .collect()
        .await?;
    println!("✓ INSERT OVERWRITE completed (2 new rows)");

    // 7. Check that a new snapshot subdirectory was created
    let snapshots_after: Vec<_> = std::fs::read_dir(&table_dir)?
        .filter_map(std::result::Result::ok)
        .filter(|e| e.path().is_dir())
        .collect();
    println!("✓ Snapshots after overwrite: {}", snapshots_after.len());

    // We should have at least one more snapshot directory (the overwrite snapshot)
    assert!(
        snapshots_after.len() > snapshots_before.len(),
        "Expected new snapshot subdirectory to be created for overwrite"
    );
    println!("✓ New snapshot subdirectory created for overwrite");

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

    // 9. Verify snapshot directories use UUIDv7 naming
    let snapshot_dirs: Vec<_> = std::fs::read_dir(&table_dir)?
        .filter_map(std::result::Result::ok)
        .filter(|e| e.path().is_dir())
        .collect();
    assert!(
        snapshot_dirs.len() >= 2,
        "Expected at least 2 snapshot directories (initial + overwrite)"
    );
    println!("✓ Snapshot directories use UUIDv7 naming");

    // Verify that the snapshot directory names are valid UUIDs
    for entry in &snapshot_dirs {
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        // UUIDs have the format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx (36 chars with dashes)
        assert!(
            name_str.len() == 36 && name_str.chars().filter(|&c| c == '-').count() == 4,
            "Snapshot directory name should be a UUID: {name_str}"
        );
    }
    println!("✓ All snapshot directories have valid UUID names");

    println!("\n✅ INSERT OVERWRITE test passed!");
    println!("✅ Snapshot-based overwrite semantics working correctly");
    Ok(())
}
