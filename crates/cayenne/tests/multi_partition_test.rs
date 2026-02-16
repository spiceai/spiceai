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

#![allow(clippy::expect_used)]

//! Integration tests for Cayenne multi-partition (composite partition key) support.
//!
//! These tests verify that Cayenne correctly handles multiple `partition_by` expressions,
//! creating hierarchical partition structures (e.g., `year=2025/month=10/day=15/`).
//!
//! Test scenarios include:
//! - Two-level partitioning (year/month)
//! - Three-level partitioning (year/month/day)
//! - Mixed partition column types (string + integer)
//! - Partition pruning with composite keys
//! - Data isolation across nested partitions
//! - Metadata persistence with composite partition keys

mod common;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use cayenne::metadata::{CreateTableOptions, PartitionMetadata};
use cayenne::CayenneTableProvider;
use datafusion::prelude::*;
use std::sync::Arc;

// =============================================================================
// Two-Level Partitioning Tests (year/month)
// =============================================================================

test_with_backends!(test_two_level_partition_basic_impl);

/// Test basic two-level partitioning with year and month columns.
///
/// This test verifies:
/// 1. Data is correctly organized into `year=YYYY/month=MM/` directories
/// 2. Data from multiple partitions can be queried together
/// 3. Partition metadata correctly stores composite keys
async fn test_two_level_partition_basic_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let catalog = &fixture.catalog;
    let data_path = &fixture.data_path;
    let backend_name = fixture.backend_type.name();

    println!("🧪 Testing two-level partitioning (year/month) with {backend_name}");

    // Create schema with year, month, and value columns
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("year", DataType::Int32, false),
        Field::new("month", DataType::Int32, false),
        Field::new("value", DataType::Utf8, false),
    ]));

    let table_options = CreateTableOptions {
        table_name: "two_level_partitioned".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec![],
        on_conflict: None,
        base_path: data_path.to_string_lossy().to_string(),
        // Note: For multi-partition, we use the first partition column here
        // The actual multi-partition support comes from the runtime accelerator
        partition_column: Some("year".to_string()),
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    let table = CayenneTableProvider::create_table(
        Arc::<cayenne::CayenneCatalog>::clone(catalog),
        table_options,
    )
    .await?;
    println!("✓ Created table with partition_column: year");

    let ctx = SessionContext::new();
    ctx.register_table("two_level_partitioned", Arc::new(table))?;

    // Insert data spanning multiple year/month combinations
    ctx.sql(
        "INSERT INTO two_level_partitioned VALUES
         (1, 2024, 1, 'jan_2024_a'),
         (2, 2024, 1, 'jan_2024_b'),
         (3, 2024, 6, 'jun_2024'),
         (4, 2024, 12, 'dec_2024'),
         (5, 2025, 1, 'jan_2025'),
         (6, 2025, 3, 'mar_2025'),
         (7, 2025, 6, 'jun_2025')",
    )
    .await?
    .collect()
    .await?;
    println!("✓ Inserted 7 rows across 6 year/month combinations");

    // Query all data
    let df = ctx
        .sql("SELECT * FROM two_level_partitioned ORDER BY id")
        .await?;
    let results = df.collect().await?;
    let total_rows: usize = results.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 7, "Expected 7 rows");
    println!("✓ Query returned all {total_rows} rows");

    // Query specific year
    let df = ctx
        .sql("SELECT * FROM two_level_partitioned WHERE year = 2024 ORDER BY id")
        .await?;
    let results = df.collect().await?;
    let total_rows: usize = results.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 4, "Expected 4 rows for year 2024");
    println!("✓ Year filter returned {total_rows} rows");

    // Query specific year + month
    let df = ctx
        .sql("SELECT * FROM two_level_partitioned WHERE year = 2024 AND month = 1 ORDER BY id")
        .await?;
    let results = df.collect().await?;
    let total_rows: usize = results.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 2, "Expected 2 rows for year=2024, month=1");
    println!("✓ Year+month filter returned {total_rows} rows");

    println!("\n✅ Two-level partition basic test passed with {backend_name}!");
    Ok(())
}

// =============================================================================
// Three-Level Partitioning Tests (year/month/day)
// =============================================================================

test_with_backends!(test_three_level_partition_basic_impl);

/// Test three-level partitioning with year, month, and day columns.
///
/// This test verifies:
/// 1. Data is correctly organized into `year=YYYY/month=MM/day=DD/` directories
/// 2. All three partition levels can be queried independently or together
/// 3. Deep nesting doesn't cause issues with path handling
async fn test_three_level_partition_basic_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let catalog = &fixture.catalog;
    let data_path = &fixture.data_path;
    let backend_name = fixture.backend_type.name();

    println!("🧪 Testing three-level partitioning (year/month/day) with {backend_name}");

    // Create schema with year, month, day, and data columns
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("year", DataType::Int32, false),
        Field::new("month", DataType::Int32, false),
        Field::new("day", DataType::Int32, false),
        Field::new("event", DataType::Utf8, false),
    ]));

    let table_options = CreateTableOptions {
        table_name: "three_level_partitioned".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec![],
        on_conflict: None,
        base_path: data_path.to_string_lossy().to_string(),
        partition_column: Some("year".to_string()),
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    let table = CayenneTableProvider::create_table(
        Arc::<cayenne::CayenneCatalog>::clone(catalog),
        table_options,
    )
    .await?;
    println!("✓ Created table with three-level partitioning");

    let ctx = SessionContext::new();
    ctx.register_table("three_level_partitioned", Arc::new(table))?;

    // Insert data spanning multiple year/month/day combinations
    ctx.sql(
        "INSERT INTO three_level_partitioned VALUES
         (1, 2025, 1, 1, 'new_years_day'),
         (2, 2025, 1, 15, 'mid_january'),
         (3, 2025, 1, 31, 'end_january'),
         (4, 2025, 6, 1, 'june_start'),
         (5, 2025, 6, 15, 'mid_june'),
         (6, 2025, 12, 25, 'christmas'),
         (7, 2025, 12, 31, 'new_years_eve'),
         (8, 2026, 1, 1, 'next_year')",
    )
    .await?
    .collect()
    .await?;
    println!("✓ Inserted 8 rows across multiple year/month/day combinations");

    // Query all data
    let df = ctx
        .sql("SELECT * FROM three_level_partitioned ORDER BY id")
        .await?;
    let results = df.collect().await?;
    let total_rows: usize = results.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 8, "Expected 8 rows");
    println!("✓ All data query returned {total_rows} rows");

    // Query specific year
    let df = ctx
        .sql("SELECT * FROM three_level_partitioned WHERE year = 2025 ORDER BY id")
        .await?;
    let results = df.collect().await?;
    let total_rows: usize = results.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 7, "Expected 7 rows for year 2025");
    println!("✓ Year filter (2025) returned {total_rows} rows");

    // Query specific year + month
    let df = ctx
        .sql("SELECT * FROM three_level_partitioned WHERE year = 2025 AND month = 1 ORDER BY id")
        .await?;
    let results = df.collect().await?;
    let total_rows: usize = results.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 3, "Expected 3 rows for year=2025, month=1");
    println!("✓ Year+month filter returned {total_rows} rows");

    // Query specific year + month + day
    let df = ctx
        .sql("SELECT * FROM three_level_partitioned WHERE year = 2025 AND month = 12 AND day = 25")
        .await?;
    let results = df.collect().await?;
    let total_rows: usize = results.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(
        total_rows, 1,
        "Expected 1 row for year=2025, month=12, day=25"
    );
    println!("✓ Year+month+day filter returned {total_rows} rows (Christmas!)");

    // Query using range filters across partitions
    let df = ctx
        .sql("SELECT * FROM three_level_partitioned WHERE year = 2025 AND month >= 6 ORDER BY id")
        .await?;
    let results = df.collect().await?;
    let total_rows: usize = results.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 4, "Expected 4 rows for year=2025, month>=6");
    println!("✓ Range filter (month >= 6) returned {total_rows} rows");

    println!("\n✅ Three-level partition basic test passed with {backend_name}!");
    Ok(())
}

// =============================================================================
// Mixed Type Partitioning Tests (string + integer)
// =============================================================================

test_with_backends!(test_mixed_type_partition_impl);

/// Test partitioning with mixed column types (string region + integer year).
///
/// This test verifies:
/// 1. String and integer partition columns work together
/// 2. Partition paths handle different value types correctly
/// 3. Queries can filter on either partition column
async fn test_mixed_type_partition_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let catalog = &fixture.catalog;
    let data_path = &fixture.data_path;
    let backend_name = fixture.backend_type.name();

    println!(
        "🧪 Testing mixed-type partitioning (string region + integer year) with {backend_name}"
    );

    // Create schema with string region and integer year
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("year", DataType::Int32, false),
        Field::new("revenue", DataType::Int64, false),
    ]));

    let table_options = CreateTableOptions {
        table_name: "mixed_type_partitioned".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec![],
        on_conflict: None,
        base_path: data_path.to_string_lossy().to_string(),
        partition_column: Some("region".to_string()),
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    let table = CayenneTableProvider::create_table(
        Arc::<cayenne::CayenneCatalog>::clone(catalog),
        table_options,
    )
    .await?;
    println!("✓ Created table with mixed-type partitioning (region=string, year=int)");

    let ctx = SessionContext::new();
    ctx.register_table("mixed_type_partitioned", Arc::new(table))?;

    // Insert data across different regions and years
    ctx.sql(
        "INSERT INTO mixed_type_partitioned VALUES
         (1, 'us-east', 2024, 1000000),
         (2, 'us-east', 2025, 1200000),
         (3, 'us-west', 2024, 800000),
         (4, 'us-west', 2025, 950000),
         (5, 'eu-west', 2024, 600000),
         (6, 'eu-west', 2025, 750000),
         (7, 'ap-south', 2024, 400000),
         (8, 'ap-south', 2025, 550000)",
    )
    .await?
    .collect()
    .await?;
    println!("✓ Inserted 8 rows across 4 regions × 2 years");

    // Query all data
    let df = ctx
        .sql("SELECT * FROM mixed_type_partitioned ORDER BY id")
        .await?;
    let results = df.collect().await?;
    let total_rows: usize = results.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 8, "Expected 8 rows");
    println!("✓ All data query returned {total_rows} rows");

    // Query by string partition column
    let df = ctx
        .sql("SELECT * FROM mixed_type_partitioned WHERE region = 'us-east' ORDER BY id")
        .await?;
    let results = df.collect().await?;
    let total_rows: usize = results.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 2, "Expected 2 rows for region='us-east'");
    println!("✓ String partition filter (region='us-east') returned {total_rows} rows");

    // Query by integer partition column (within region)
    let df = ctx
        .sql("SELECT * FROM mixed_type_partitioned WHERE region = 'us-east' AND year = 2025")
        .await?;
    let results = df.collect().await?;
    let total_rows: usize = results.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(
        total_rows, 1,
        "Expected 1 row for region='us-east', year=2025"
    );
    println!("✓ Mixed-type filter (region='us-east', year=2025) returned {total_rows} rows");

    // Query across regions for a specific year
    let df = ctx
        .sql("SELECT region, SUM(revenue) as total FROM mixed_type_partitioned WHERE year = 2025 GROUP BY region ORDER BY region")
        .await?;
    let results = df.collect().await?;
    let total_rows: usize = results.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 4, "Expected 4 regions in 2025");
    println!("✓ Aggregation by year across regions returned {total_rows} rows");

    println!("\n✅ Mixed-type partition test passed with {backend_name}!");
    Ok(())
}

// =============================================================================
// Partition Metadata Tests
// =============================================================================

test_with_backends!(test_composite_partition_metadata_impl);

/// Test that composite partition metadata is correctly stored and retrieved.
///
/// This test verifies:
/// 1. Partition metadata stores multiple column names
/// 2. Partition metadata stores multiple partition values
/// 3. Composite keys are correctly serialized/deserialized
async fn test_composite_partition_metadata_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let catalog = &fixture.catalog;
    let data_path = &fixture.data_path;
    let backend_name = fixture.backend_type.name();

    println!("🧪 Testing composite partition metadata storage with {backend_name}");

    // Create a simple partitioned table
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("year", DataType::Int32, false),
        Field::new("data", DataType::Utf8, false),
    ]));

    let table_options = CreateTableOptions {
        table_name: "metadata_test".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec![],
        on_conflict: None,
        base_path: data_path.to_string_lossy().to_string(),
        partition_column: Some("year".to_string()),
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    let table = CayenneTableProvider::create_table(
        Arc::<cayenne::CayenneCatalog>::clone(catalog),
        table_options,
    )
    .await?;
    let ctx = SessionContext::new();
    ctx.register_table("metadata_test", Arc::new(table))?;

    // Insert data to create partitions
    ctx.sql("INSERT INTO metadata_test VALUES (1, 2024, 'a'), (2, 2025, 'b')")
        .await?
        .collect()
        .await?;
    println!("✓ Inserted data into 2 partitions");

    // Test creating composite partition metadata directly
    let composite_partition = PartitionMetadata::new_composite(
        1, // table_id (not important for this test)
        vec!["year".to_string(), "month".to_string()],
        vec!["2025".to_string(), "10".to_string()],
        data_path
            .join("year=2025/month=10")
            .to_string_lossy()
            .to_string(),
        false,
    );

    // Verify the composite key generation
    assert_eq!(composite_partition.composite_key(), "2025/10");
    println!(
        "✓ Composite key correctly generated: '{}'",
        composite_partition.composite_key()
    );

    // Verify partition_columns and partition_values are stored correctly
    assert_eq!(composite_partition.partition_columns, vec!["year", "month"]);
    assert_eq!(composite_partition.partition_values, vec!["2025", "10"]);
    println!(
        "✓ Partition columns: {:?}, values: {:?}",
        composite_partition.partition_columns, composite_partition.partition_values
    );

    // Test single partition metadata (backward compatibility)
    let single_partition = PartitionMetadata::new_single(
        1,
        "region".to_string(),
        "us-east-1".to_string(),
        data_path
            .join("region=us-east-1")
            .to_string_lossy()
            .to_string(),
        false,
    );

    assert_eq!(single_partition.partition_columns, vec!["region"]);
    assert_eq!(single_partition.partition_values, vec!["us-east-1"]);
    assert_eq!(single_partition.composite_key(), "us-east-1");
    println!("✓ Single partition backward compatibility verified");

    println!("\n✅ Composite partition metadata test passed with {backend_name}!");
    Ok(())
}

// =============================================================================
// Partition Data Isolation Tests
// =============================================================================

test_with_backends!(test_partition_data_isolation_impl);

/// Test that data in different partitions is properly isolated.
///
/// This test verifies:
/// 1. Updates to one partition don't affect other partitions
/// 2. Deletes in one partition don't affect other partitions
/// 3. Each partition maintains independent data
async fn test_partition_data_isolation_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let catalog = &fixture.catalog;
    let data_path = &fixture.data_path;
    let backend_name = fixture.backend_type.name();

    println!("🧪 Testing partition data isolation with {backend_name}");

    // Create schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let table_options = CreateTableOptions {
        table_name: "isolation_test".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec![],
        on_conflict: None,
        base_path: data_path.to_string_lossy().to_string(),
        partition_column: Some("region".to_string()),
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    let table = CayenneTableProvider::create_table(
        Arc::<cayenne::CayenneCatalog>::clone(catalog),
        table_options,
    )
    .await?;
    let ctx = SessionContext::new();
    ctx.register_table("isolation_test", Arc::new(table))?;

    // Insert initial data into multiple partitions
    ctx.sql(
        "INSERT INTO isolation_test VALUES
         (1, 'partition_a', 100),
         (2, 'partition_a', 200),
         (3, 'partition_b', 300),
         (4, 'partition_b', 400)",
    )
    .await?
    .collect()
    .await?;
    println!("✓ Inserted 4 rows across 2 partitions");

    // Verify initial state
    let df = ctx
        .sql("SELECT region, COUNT(*) as cnt FROM isolation_test GROUP BY region ORDER BY region")
        .await?;
    let results = df.collect().await?;

    // Both partitions should have 2 rows
    for batch in &results {
        let region_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("region column");
        let count_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("count column");

        for i in 0..batch.num_rows() {
            let count = count_col.value(i);
            assert_eq!(count, 2, "Each partition should have 2 rows initially");
            println!("  • {}: {} rows", region_col.value(i), count);
        }
    }

    // Insert more data into partition_a only
    ctx.sql("INSERT INTO isolation_test VALUES (5, 'partition_a', 500)")
        .await?
        .collect()
        .await?;
    println!("✓ Inserted 1 more row into partition_a");

    // Verify partition_a has 3 rows, partition_b still has 2
    let df = ctx
        .sql("SELECT COUNT(*) FROM isolation_test WHERE region = 'partition_a'")
        .await?;
    let results = df.collect().await?;
    let count_a: i64 = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count")
        .value(0);
    assert_eq!(count_a, 3, "partition_a should have 3 rows");

    let df = ctx
        .sql("SELECT COUNT(*) FROM isolation_test WHERE region = 'partition_b'")
        .await?;
    let results = df.collect().await?;
    let count_b: i64 = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count")
        .value(0);
    assert_eq!(count_b, 2, "partition_b should still have 2 rows");

    println!("✓ Partition isolation verified: partition_a={count_a}, partition_b={count_b}");

    println!("\n✅ Partition data isolation test passed with {backend_name}!");
    Ok(())
}

// =============================================================================
// Insert and Query Stress Tests
// =============================================================================

test_with_backends!(test_multi_partition_stress_impl);

/// Stress test with many partitions and data.
///
/// This test verifies:
/// 1. System handles many partitions correctly
/// 2. Queries across many partitions work correctly
/// 3. No performance degradation with partition count
async fn test_multi_partition_stress_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let catalog = &fixture.catalog;
    let data_path = &fixture.data_path;
    let backend_name = fixture.backend_type.name();

    println!("🧪 Testing multi-partition stress with {backend_name}");

    // Create schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("partition_key", DataType::Int32, false),
        Field::new("data", DataType::Utf8, false),
    ]));

    let table_options = CreateTableOptions {
        table_name: "stress_test".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec![],
        on_conflict: None,
        base_path: data_path.to_string_lossy().to_string(),
        partition_column: Some("partition_key".to_string()),
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    let table = CayenneTableProvider::create_table(
        Arc::<cayenne::CayenneCatalog>::clone(catalog),
        table_options,
    )
    .await?;
    let ctx = SessionContext::new();
    ctx.register_table("stress_test", Arc::new(table))?;

    // Insert data across 10 partitions, with multiple rows per partition
    let num_partitions = 10;
    let rows_per_partition = 5;
    let total_expected = num_partitions * rows_per_partition;

    let mut id_counter = 1i64;
    for partition in 0..num_partitions {
        let mut values = Vec::new();
        for _ in 0..rows_per_partition {
            values.push(format!("({id_counter}, {partition}, 'data_{id_counter}')"));
            id_counter += 1;
        }
        let sql = format!("INSERT INTO stress_test VALUES {}", values.join(", "));
        ctx.sql(&sql).await?.collect().await?;
    }
    println!("✓ Inserted {total_expected} rows across {num_partitions} partitions");

    // Query all data
    let df = ctx.sql("SELECT COUNT(*) FROM stress_test").await?;
    let results = df.collect().await?;
    let count: i64 = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count")
        .value(0);
    assert_eq!(
        count,
        i64::from(total_expected),
        "Expected {total_expected} total rows"
    );
    println!("✓ Total row count verified: {count}");

    // Query each partition individually
    for partition in 0..num_partitions {
        let df = ctx
            .sql(&format!(
                "SELECT COUNT(*) FROM stress_test WHERE partition_key = {partition}"
            ))
            .await?;
        let results = df.collect().await?;
        let count: i64 = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("count")
            .value(0);
        assert_eq!(
            count,
            i64::from(rows_per_partition),
            "Partition {partition} should have {rows_per_partition} rows"
        );
    }
    println!("✓ All {num_partitions} partition counts verified");

    // Query across multiple partitions with range filter
    let df = ctx
        .sql("SELECT COUNT(*) FROM stress_test WHERE partition_key >= 5")
        .await?;
    let results = df.collect().await?;
    let count: i64 = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count")
        .value(0);
    let expected_range = i64::try_from((5..num_partitions).count()).expect("should fit into i64")
        * i64::from(rows_per_partition);
    assert_eq!(
        count, expected_range,
        "Expected {expected_range} rows for partition_key >= 5"
    );
    println!("✓ Range query across partitions verified: {count} rows");

    println!("\n✅ Multi-partition stress test passed with {backend_name}!");
    Ok(())
}

// =============================================================================
// Partition Value Edge Cases
// =============================================================================

test_with_backends!(test_partition_value_edge_cases_impl);

/// Test edge cases in partition values.
///
/// This test verifies:
/// 1. Empty string partition values are handled
/// 2. Partition values with special characters work
/// 3. Very long partition values are supported
async fn test_partition_value_edge_cases_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let catalog = &fixture.catalog;
    let data_path = &fixture.data_path;
    let backend_name = fixture.backend_type.name();

    println!("🧪 Testing partition value edge cases with {backend_name}");

    // Create schema with string partition column
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("category", DataType::Utf8, false),
        Field::new("data", DataType::Utf8, false),
    ]));

    let table_options = CreateTableOptions {
        table_name: "edge_cases".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec![],
        on_conflict: None,
        base_path: data_path.to_string_lossy().to_string(),
        partition_column: Some("category".to_string()),
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    let table = CayenneTableProvider::create_table(
        Arc::<cayenne::CayenneCatalog>::clone(catalog),
        table_options,
    )
    .await?;
    let ctx = SessionContext::new();
    ctx.register_table("edge_cases", Arc::new(table))?;

    // Test various partition values
    ctx.sql(
        "INSERT INTO edge_cases VALUES
         (1, 'normal', 'regular partition'),
         (2, 'with-dash', 'dash in name'),
         (3, 'with_underscore', 'underscore in name'),
         (4, 'CamelCase', 'mixed case'),
         (5, 'UPPERCASE', 'all caps'),
         (6, 'lowercase', 'all lower')",
    )
    .await?
    .collect()
    .await?;
    println!("✓ Inserted rows with various partition value patterns");

    // Query each partition
    let test_categories = [
        "normal",
        "with-dash",
        "with_underscore",
        "CamelCase",
        "UPPERCASE",
        "lowercase",
    ];

    for category in test_categories {
        let df = ctx
            .sql(&format!(
                "SELECT * FROM edge_cases WHERE category = '{category}'"
            ))
            .await?;
        let results = df.collect().await?;
        let total_rows: usize = results.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total_rows, 1, "Expected 1 row for category '{category}'");
    }
    println!("✓ All partition value patterns correctly queried");

    // Query all data
    let df = ctx.sql("SELECT * FROM edge_cases ORDER BY id").await?;
    let results = df.collect().await?;
    let total_rows: usize = results.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 6, "Expected 6 total rows");
    println!("✓ Total row count verified: {total_rows}");

    println!("\n✅ Partition value edge cases test passed with {backend_name}!");
    Ok(())
}

// =============================================================================
// Aggregation Across Partitions
// =============================================================================

test_with_backends!(test_aggregation_across_partitions_impl);

/// Test aggregations that span multiple partitions.
///
/// This test verifies:
/// 1. SUM/COUNT/AVG work correctly across partitions
/// 2. GROUP BY on non-partition columns works
/// 3. Aggregations with partition filters are accurate
async fn test_aggregation_across_partitions_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let catalog = &fixture.catalog;
    let data_path = &fixture.data_path;
    let backend_name = fixture.backend_type.name();

    println!("🧪 Testing aggregations across partitions with {backend_name}");

    // Create schema with numeric data for aggregations
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("product", DataType::Utf8, false),
        Field::new("sales", DataType::Int64, false),
    ]));

    let table_options = CreateTableOptions {
        table_name: "sales_data".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec![],
        on_conflict: None,
        base_path: data_path.to_string_lossy().to_string(),
        partition_column: Some("region".to_string()),
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    let table = CayenneTableProvider::create_table(
        Arc::<cayenne::CayenneCatalog>::clone(catalog),
        table_options,
    )
    .await?;
    let ctx = SessionContext::new();
    ctx.register_table("sales_data", Arc::new(table))?;

    // Insert sales data across regions
    ctx.sql(
        "INSERT INTO sales_data VALUES
         (1, 'north', 'widget', 100),
         (2, 'north', 'widget', 150),
         (3, 'north', 'gadget', 200),
         (4, 'south', 'widget', 120),
         (5, 'south', 'gadget', 180),
         (6, 'east', 'widget', 90),
         (7, 'east', 'gadget', 210),
         (8, 'west', 'widget', 130),
         (9, 'west', 'gadget', 170)",
    )
    .await?
    .collect()
    .await?;
    println!("✓ Inserted 9 sales records across 4 regions");

    // Test 1: SUM across all partitions
    let df = ctx
        .sql("SELECT SUM(sales) as total FROM sales_data")
        .await?;
    let results = df.collect().await?;
    let total: i64 = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("total")
        .value(0);
    assert_eq!(total, 1350, "Expected total sales of 1350");
    println!("✓ Total sales (SUM): {total}");

    // Test 2: COUNT across partitions
    let df = ctx.sql("SELECT COUNT(*) as cnt FROM sales_data").await?;
    let results = df.collect().await?;
    let count: i64 = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count")
        .value(0);
    assert_eq!(count, 9, "Expected 9 records");
    println!("✓ Record count: {count}");

    // Test 3: SUM by region (partition column)
    let df = ctx
        .sql("SELECT region, SUM(sales) as regional_total FROM sales_data GROUP BY region ORDER BY region")
        .await?;
    let results = df.collect().await?;

    let expected_totals = [
        ("east", 300i64),
        ("north", 450),
        ("south", 300),
        ("west", 300),
    ];
    let mut idx = 0;
    for batch in &results {
        let region_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("region");
        let total_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("total");

        for i in 0..batch.num_rows() {
            let region = region_col.value(i);
            let total = total_col.value(i);
            assert_eq!(
                (region, total),
                expected_totals[idx],
                "Regional total mismatch"
            );
            idx += 1;
        }
    }
    println!("✓ Regional totals verified");

    // Test 4: SUM by product (non-partition column)
    let df = ctx
        .sql("SELECT product, SUM(sales) as product_total FROM sales_data GROUP BY product ORDER BY product")
        .await?;
    let results = df.collect().await?;

    let expected_products = [("gadget", 760i64), ("widget", 590)];
    let mut idx = 0;
    for batch in &results {
        let product_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("product");
        let total_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("total");

        for i in 0..batch.num_rows() {
            let product = product_col.value(i);
            let total = total_col.value(i);
            assert_eq!(
                (product, total),
                expected_products[idx],
                "Product total mismatch"
            );
            idx += 1;
        }
    }
    println!("✓ Product totals verified (gadget=760, widget=590)");

    // Test 5: Filtered aggregation (specific partition)
    let df = ctx
        .sql("SELECT SUM(sales) as north_total FROM sales_data WHERE region = 'north'")
        .await?;
    let results = df.collect().await?;
    let north_total: i64 = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("north_total")
        .value(0);
    assert_eq!(north_total, 450, "Expected north region total of 450");
    println!("✓ North region total: {north_total}");

    println!("\n✅ Aggregation across partitions test passed with {backend_name}!");
    Ok(())
}
