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

//! Test that verifies `partition_by` works correctly with the new chunking implementation

mod common;

use arrow::array::{Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use cayenne::metadata::{CreateTableOptions, VortexConfig};
use cayenne::CayenneTableProvider;
use datafusion::prelude::*;
use std::sync::Arc;

// Generate test variants for each backend
test_with_backends!(test_partitioned_table_with_chunking_impl);

async fn test_partitioned_table_with_chunking_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let catalog = &fixture.catalog;
    let data_path = &fixture.data_path;

    println!("✓ Catalog initialized");

    // Create schema with partition column
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("category", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let table_options = CreateTableOptions {
        table_name: "partitioned_table".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec![],
        base_path: data_path.to_string_lossy().to_string(),
        partition_column: Some("category".to_string()),
        vortex_config: VortexConfig {
            target_vortex_file_size_mb: 1, // 1 MB chunks to test chunking with small data
            ..VortexConfig::default()
        },
    };

    // Create Cayenne table provider
    let table = CayenneTableProvider::create_table(
        Arc::<cayenne::CayenneCatalog>::clone(catalog),
        table_options,
    )
    .await?;
    println!("✓ Partitioned table created");

    // Register with DataFusion
    let ctx = SessionContext::new();
    ctx.register_table("partitioned_table", Arc::new(table))?;

    // Insert data into multiple partitions
    // This will test that chunking works within each partition
    println!("\n--- Inserting data into partitions ---");

    // Insert data for partition "A" - enough to potentially create multiple chunks
    for batch_num in 0..5 {
        let start_id = batch_num * 1000;
        let ids: Vec<i64> = (start_id..start_id + 1000).collect();
        let categories: Vec<&str> = vec!["A"; 1000];
        let values: Vec<i64> = (start_id..start_id + 1000).collect();

        ctx.sql(&format!(
            "INSERT INTO partitioned_table SELECT * FROM (VALUES {})",
            ids.iter()
                .zip(categories.iter())
                .zip(values.iter())
                .map(|((id, cat), val)| format!("({id}, '{cat}', {val})"))
                .collect::<Vec<_>>()
                .join(", ")
        ))
        .await?
        .collect()
        .await?;
    }
    println!("✓ Inserted 5000 rows into partition A");

    // Insert data for partition "B"
    for batch_num in 0..5 {
        let start_id = batch_num * 1000 + 10000; // Offset to avoid ID collision
        let ids: Vec<i64> = (start_id..start_id + 1000).collect();
        let categories: Vec<&str> = vec!["B"; 1000];
        let values: Vec<i64> = (start_id..start_id + 1000).collect();

        ctx.sql(&format!(
            "INSERT INTO partitioned_table SELECT * FROM (VALUES {})",
            ids.iter()
                .zip(categories.iter())
                .zip(values.iter())
                .map(|((id, cat), val)| format!("({id}, '{cat}', {val})"))
                .collect::<Vec<_>>()
                .join(", ")
        ))
        .await?
        .collect()
        .await?;
    }
    println!("✓ Inserted 5000 rows into partition B");

    // Insert data for partition "C"
    for batch_num in 0..3 {
        let start_id = batch_num * 1000 + 20000; // Offset to avoid ID collision
        let ids: Vec<i64> = (start_id..start_id + 1000).collect();
        let categories: Vec<&str> = vec!["C"; 1000];
        let values: Vec<i64> = (start_id..start_id + 1000).collect();

        ctx.sql(&format!(
            "INSERT INTO partitioned_table SELECT * FROM (VALUES {})",
            ids.iter()
                .zip(categories.iter())
                .zip(values.iter())
                .map(|((id, cat), val)| format!("({id}, '{cat}', {val})"))
                .collect::<Vec<_>>()
                .join(", ")
        ))
        .await?
        .collect()
        .await?;
    }
    println!("✓ Inserted 3000 rows into partition C");

    // Query to verify all data is present
    println!("\n--- Querying partitioned data ---");

    // Total count
    let df = ctx
        .sql("SELECT COUNT(*) as total FROM partitioned_table")
        .await?;
    let results = df.collect().await?;
    let count_array = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Count should be Int64");
    let total_count = count_array.value(0);
    assert_eq!(total_count, 13000, "Expected 13000 total rows");
    println!("✓ Total rows: {total_count}");

    // Count by partition
    let df = ctx
        .sql("SELECT category, COUNT(*) as count FROM partitioned_table GROUP BY category ORDER BY category")
        .await?;
    let results = df.collect().await?;
    assert_eq!(results.len(), 1, "Expected one result batch");

    let category_array = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("Category should be String");
    let count_array = results[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Count should be Int64");

    assert_eq!(category_array.value(0), "A");
    assert_eq!(
        count_array.value(0),
        5000,
        "Partition A should have 5000 rows"
    );
    println!("✓ Partition A: {} rows", count_array.value(0));

    assert_eq!(category_array.value(1), "B");
    assert_eq!(
        count_array.value(1),
        5000,
        "Partition B should have 5000 rows"
    );
    println!("✓ Partition B: {} rows", count_array.value(1));

    assert_eq!(category_array.value(2), "C");
    assert_eq!(
        count_array.value(2),
        3000,
        "Partition C should have 3000 rows"
    );
    println!("✓ Partition C: {} rows", count_array.value(2));

    // Query specific partition
    let df = ctx
        .sql("SELECT COUNT(*) as count FROM partitioned_table WHERE category = 'A'")
        .await?;
    let results = df.collect().await?;
    let count_array = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Count should be Int64");
    assert_eq!(
        count_array.value(0),
        5000,
        "Partition A should have 5000 rows"
    );
    println!("✓ Query for partition A returned correct count");

    println!("\n--- Verifying partition data integrity ---");

    // Verify data integrity - read back some specific records from each partition
    let df = ctx
        .sql("SELECT id, category, value FROM partitioned_table WHERE id IN (0, 10000, 20000) ORDER BY id")
        .await?;
    let results = df.collect().await?;
    assert_eq!(results.len(), 1, "Should get one batch");

    let id_array = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("ID should be Int64");
    let category_array = results[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("Category should be String");

    assert_eq!(id_array.len(), 3, "Should have 3 records");
    assert_eq!(id_array.value(0), 0);
    assert_eq!(category_array.value(0), "A");
    assert_eq!(id_array.value(1), 10000);
    assert_eq!(category_array.value(1), "B");
    assert_eq!(id_array.value(2), 20000);
    assert_eq!(category_array.value(2), "C");

    println!("✓ Data integrity verified - specific records match expected values");

    println!("\n✓ All partition + chunking tests passed!");

    Ok(())
}

// Generate test variants for second test
test_with_backends!(test_partitioned_table_with_large_chunks_impl);

async fn test_partitioned_table_with_large_chunks_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    // Test with larger chunk size to ensure single file per partition works
    let catalog = &fixture.catalog;
    let data_path = &fixture.data_path;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let table_options = CreateTableOptions {
        table_name: "large_chunk_table".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec![],
        base_path: data_path.to_string_lossy().to_string(),
        partition_column: Some("region".to_string()),
        vortex_config: VortexConfig {
            target_vortex_file_size_mb: 512, // Large chunk size
            ..VortexConfig::default()
        },
    };

    let table = CayenneTableProvider::create_table(
        Arc::<cayenne::CayenneCatalog>::clone(catalog),
        table_options,
    )
    .await?;

    let ctx = SessionContext::new();
    ctx.register_table("large_chunk_table", Arc::new(table))?;

    // Insert small amount of data (should all fit in one chunk per partition)
    ctx.sql("INSERT INTO large_chunk_table VALUES (1, 'US', 100), (2, 'EU', 200), (3, 'US', 300), (4, 'APAC', 400)")
        .await?
        .collect()
        .await?;

    let df = ctx
        .sql("SELECT COUNT(*) as total FROM large_chunk_table")
        .await?;
    let results = df.collect().await?;
    let count_array = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Count should be Int64");

    assert_eq!(count_array.value(0), 4, "Expected 4 total rows");
    println!(
        "✓ Large chunk test passed with {} rows",
        count_array.value(0)
    );

    Ok(())
}

// Generate test variants for timestamp partitioning test
test_with_backends!(test_timestamp_partition_with_date_part_impl);

async fn test_timestamp_partition_with_date_part_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    // Test partitioning by month extracted from timestamp
    // Note: At the Cayenne library level, we test simple column-based partitioning.
    // The runtime layer handles partition_by expressions like date_part(month, event_time).
    // This test simulates what the runtime would do: compute the partition value and
    // include it as a column for Cayenne to partition on.
    let catalog = &fixture.catalog;
    let data_path = &fixture.data_path;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "event_time",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("value", DataType::Int64, false),
        Field::new("month", DataType::Utf8, false), // Partition column (runtime would compute this from event_time)
    ]));

    let table_options = CreateTableOptions {
        table_name: "timestamp_partitioned_table".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec![],
        base_path: data_path.to_string_lossy().to_string(),
        partition_column: Some("month".to_string()), // Simple column partitioning
        vortex_config: VortexConfig {
            target_vortex_file_size_mb: 1, // Small chunks to test chunking
            ..VortexConfig::default()
        },
    };

    let table = CayenneTableProvider::create_table(
        Arc::<cayenne::CayenneCatalog>::clone(catalog),
        table_options,
    )
    .await?;
    println!("✓ Timestamp-partitioned table created");

    let ctx = SessionContext::new();
    ctx.register_table("timestamp_partitioned_table", Arc::new(table))?;

    // Insert data across multiple months (Jan, Feb, Mar 2024)
    // The runtime would use date_part(month, event_time) to compute the month value
    // Here we simulate that by pre-computing it in the test
    println!("\n--- Inserting timestamped data ---");

    // January 2024 data (month="2024-01")
    for i in 0..1000 {
        let timestamp_ms = 1_704_067_200_000_i64 + (i * 3_600_000); // Jan 1, 2024 00:00:00 UTC + i hours
        let month = "2024-01";
        let value = i;

        ctx.sql(&format!(
            "INSERT INTO timestamp_partitioned_table VALUES ({i}, {timestamp_ms}, {value}, '{month}')",
        ))
        .await?
        .collect()
        .await?;
    }
    println!("✓ Inserted 1000 rows for January (month=2024-01)");

    // February 2024 data (month="2024-02")
    for i in 1000..2000 {
        let timestamp_ms = 1_706_745_600_000_i64 + ((i - 1000) * 3_600_000); // Feb 1, 2024 00:00:00 UTC
        let month = "2024-02";
        let value = i;

        ctx.sql(&format!(
            "INSERT INTO timestamp_partitioned_table VALUES ({i}, {timestamp_ms}, {value}, '{month}')",
        ))
        .await?
        .collect()
        .await?;
    }
    println!("✓ Inserted 1000 rows for February (month=2024-02)");

    // March 2024 data (month="2024-03")
    for i in 2000..2500 {
        let timestamp_ms = 1_709_251_200_000_i64 + ((i - 2000) * 3_600_000); // Mar 1, 2024 00:00:00 UTC
        let month = "2024-03";
        let value = i;

        ctx.sql(&format!(
            "INSERT INTO timestamp_partitioned_table VALUES ({i}, {timestamp_ms}, {value}, '{month}')",
        ))
        .await?
        .collect()
        .await?;
    }
    println!("✓ Inserted 500 rows for March (month=2024-03)");

    println!("\n--- Querying partitioned timestamp data ---");

    // Verify total count
    let df = ctx
        .sql("SELECT COUNT(*) as total FROM timestamp_partitioned_table")
        .await?;
    let results = df.collect().await?;
    let count_array = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Count should be Int64");
    assert_eq!(count_array.value(0), 2500, "Expected 2500 total rows");
    println!("✓ Total rows: {}", count_array.value(0));

    // Verify January partition (month='2024-01')
    let df = ctx
        .sql("SELECT COUNT(*) as count FROM timestamp_partitioned_table WHERE month = '2024-01'")
        .await?;
    let results = df.collect().await?;
    let count_array = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Count should be Int64");
    assert_eq!(count_array.value(0), 1000, "January should have 1000 rows");
    println!("✓ January (month=2024-01): {} rows", count_array.value(0));

    // Verify February partition (month='2024-02')
    let df = ctx
        .sql("SELECT COUNT(*) as count FROM timestamp_partitioned_table WHERE month = '2024-02'")
        .await?;
    let results = df.collect().await?;
    let count_array = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Count should be Int64");
    assert_eq!(count_array.value(0), 1000, "February should have 1000 rows");
    println!("✓ February (month=2024-02): {} rows", count_array.value(0));

    // Verify March partition (month='2024-03')
    let df = ctx
        .sql("SELECT COUNT(*) as count FROM timestamp_partitioned_table WHERE month = '2024-03'")
        .await?;
    let results = df.collect().await?;
    let count_array = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Count should be Int64");
    assert_eq!(count_array.value(0), 500, "March should have 500 rows");
    println!("✓ March (month=2024-03): {} rows", count_array.value(0));

    // Verify data integrity - check specific records from each partition
    let df = ctx
        .sql("SELECT id, month, value FROM timestamp_partitioned_table WHERE id IN (0, 1000, 2000) ORDER BY id")
        .await?;
    let results = df.collect().await?;
    assert_eq!(results.len(), 1, "Should get one batch");

    let id_array = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("ID should be Int64");
    let month_array = results[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("Month should be String");

    assert_eq!(id_array.len(), 3, "Should have 3 records");
    assert_eq!(id_array.value(0), 0);
    assert_eq!(month_array.value(0), "2024-01");
    assert_eq!(id_array.value(1), 1000);
    assert_eq!(month_array.value(1), "2024-02");
    assert_eq!(id_array.value(2), 2000);
    assert_eq!(month_array.value(2), "2024-03");

    println!("✓ Data integrity verified across timestamp partitions");

    println!("\n✓ Timestamp partition + chunking test passed!");

    Ok(())
}
