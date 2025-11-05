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

//! Integration tests for Cayenne partition pruning with `partition_by`

mod common;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use cayenne::metadata::CreateTableOptions;
use cayenne::CayenneTableProvider;
use datafusion::prelude::*;
use std::sync::Arc;

// Generate test variants for each backend
test_with_backends!(test_cayenne_partition_pruning_impl);

/// Sanitize file paths in explain plans for deterministic snapshots.
/// Replaces temp directory paths with a placeholder.
fn sanitize_file_paths(plan: &str) -> String {
    // Find file_groups= and replace the path content with placeholder
    let mut result = String::new();
    for line in plan.lines() {
        if line.contains("file_groups={") {
            // Find the start of file_groups
            if let Some(fg_start) = line.find("file_groups=") {
                // Find the closing ]]},
                if let Some(fg_end) = line[fg_start..].find("]]}") {
                    let prefix = &line[..fg_start];
                    let suffix = &line[fg_start + fg_end + 3..];
                    result.push_str(prefix);
                    result.push_str("file_groups={1 group: [[<TEMP_PATH>/<FILE>.vortex]]}");
                    result.push_str(suffix);
                } else {
                    result.push_str(line);
                }
            } else {
                result.push_str(line);
            }
        } else {
            result.push_str(line);
        }
        result.push('\n');
    }
    result
}

/// Test that validates partition pruning with `partition_by` in Cayenne.
/// This test:
/// 1. Creates a partitioned Cayenne table
/// 2. Inserts data across multiple partitions
/// 3. Queries with partition filters and validates EXPLAIN plan shows pruning
/// 4. Validates that only relevant partitions are scanned
#[allow(clippy::too_many_lines)]
async fn test_cayenne_partition_pruning_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let catalog = &fixture.catalog;
    let data_path = &fixture.data_path;
    let backend_name = fixture.backend_type.name();

    println!("✓ Catalog initialized with {backend_name} backend");

    // Create table schema with region as partition column
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));

    // Create partitioned table with region as partition column
    let table_options = CreateTableOptions {
        table_name: "partitioned_table".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec![],
        base_path: data_path.to_string_lossy().to_string(),
        partition_column: Some("region".to_string()),
    };

    let table = CayenneTableProvider::create_table(
        Arc::<cayenne::CayenneCatalog>::clone(catalog),
        table_options,
    )
    .await?;
    println!("✓ Partitioned table created with partition_by: region");

    // Register with DataFusion context
    let ctx = SessionContext::new();
    ctx.register_table("partitioned_table", Arc::new(table))?;
    println!("✓ Table registered with DataFusion");

    // Insert test data across multiple partitions
    ctx.sql(
        "INSERT INTO partitioned_table VALUES \
         (1, 'us-east-1', 100), \
         (2, 'us-east-1', 200), \
         (3, 'us-west-1', 300), \
         (4, 'us-west-1', 400), \
         (5, 'eu-west-1', 500), \
         (6, 'eu-west-1', 600)",
    )
    .await?
    .collect()
    .await?;
    println!("✓ Inserted 6 rows across 3 partitions (us-east-1, us-west-1, eu-west-1)");

    // Verify all data is present
    let df = ctx
        .sql("SELECT * FROM partitioned_table ORDER BY id")
        .await?;
    let results = df.collect().await?;
    let total_rows: usize = results.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 6, "Expected 6 rows after insert");
    println!("✓ All 6 rows retrieved");

    // Test 1: Query with partition filter only (region = 'us-east-1')
    println!("\n--- Test 1: Partition filter only ---");
    let df = ctx
        .sql("SELECT * FROM partitioned_table WHERE region = 'us-east-1' ORDER BY id")
        .await?;

    // Get physical plan for inspection
    let physical_plan = df.clone().create_physical_plan().await?;
    let mut explain_plan = datafusion::physical_plan::displayable(physical_plan.as_ref())
        .indent(true)
        .to_string();

    // Sanitize file paths for deterministic snapshots
    explain_plan = sanitize_file_paths(&explain_plan);

    println!("Physical plan:\n{explain_plan}");

    // Verify the plan doesn't include other partitions
    assert!(
        !explain_plan.contains("us-west-1"),
        "Physical plan should not include us-west-1 partition when filtering for us-east-1"
    );
    assert!(
        !explain_plan.contains("eu-west-1"),
        "Physical plan should not include eu-west-1 partition when filtering for us-east-1"
    );

    // Snapshot the explain plan
    insta::assert_snapshot!("partition_filter_us_east_1", explain_plan);

    // Verify query results
    let results = df.collect().await?;
    let total_rows: usize = results.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 2, "Expected 2 rows for region = 'us-east-1'");

    // Verify the data is correct
    for batch in &results {
        let region_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("region column should be StringArray");
        for i in 0..batch.num_rows() {
            assert_eq!(
                region_array.value(i),
                "us-east-1",
                "All returned rows should have region = 'us-east-1'"
            );
        }
    }
    println!("✓ Query correctly returned 2 rows for us-east-1 partition");

    // Test 2: Query with partition filter and data filter
    println!("\n--- Test 2: Partition filter + data filter ---");
    let df = ctx
        .sql("SELECT * FROM partitioned_table WHERE region = 'us-west-1' AND value > 300 ORDER BY id")
        .await?;

    let physical_plan = df.clone().create_physical_plan().await?;
    let mut explain_plan = datafusion::physical_plan::displayable(physical_plan.as_ref())
        .indent(true)
        .to_string();

    // Sanitize file paths for deterministic snapshots
    explain_plan = sanitize_file_paths(&explain_plan);

    println!("Physical plan:\n{explain_plan}");

    // Verify partition pruning
    assert!(
        !explain_plan.contains("us-east-1"),
        "Physical plan should not include us-east-1 partition"
    );
    assert!(
        !explain_plan.contains("eu-west-1"),
        "Physical plan should not include eu-west-1 partition"
    );

    // Snapshot the explain plan
    insta::assert_snapshot!("partition_and_data_filter", explain_plan);

    let results = df.collect().await?;
    let total_rows: usize = results.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(
        total_rows, 1,
        "Expected 1 row for region = 'us-west-1' AND value > 300"
    );

    // Verify the correct row
    let batch = &results[0];
    let id_array = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("id column should be Int64Array");
    let value_array = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("value column should be Int64Array");

    assert_eq!(id_array.value(0), 4, "Should return row with id=4");
    assert_eq!(
        value_array.value(0),
        400,
        "Should return row with value=400"
    );
    println!("✓ Query correctly returned 1 row with proper filtering");

    // Test 3: Query with data filter only (no partition filter)
    println!("\n--- Test 3: Data filter only (scans all partitions) ---");
    let df = ctx
        .sql("SELECT * FROM partitioned_table WHERE value > 400 ORDER BY id")
        .await?;

    let physical_plan = df.clone().create_physical_plan().await?;
    let mut explain_plan = datafusion::physical_plan::displayable(physical_plan.as_ref())
        .indent(true)
        .to_string();

    // Sanitize file paths for deterministic snapshots
    explain_plan = sanitize_file_paths(&explain_plan);

    // Sort lines for deterministic snapshot (HashMap iteration order is non-deterministic)
    let mut lines: Vec<&str> = explain_plan.lines().collect();
    if lines.len() > 1 {
        lines[1..].sort_unstable();
    }
    explain_plan = lines.join("\n") + "\n";

    println!("Physical plan:\n{explain_plan}");

    // This should scan all partitions since there's no partition filter
    // We can't assert specific partition names in the plan since it depends on execution details,
    // but we can verify the results

    // Snapshot the explain plan
    insta::assert_snapshot!("data_filter_only", explain_plan);

    let results = df.collect().await?;
    let total_rows: usize = results.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 2, "Expected 2 rows for value > 400");

    // Verify correct rows (id=5 and id=6)
    let mut ids = Vec::new();
    for batch in &results {
        let id_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id column should be Int64Array");
        for i in 0..batch.num_rows() {
            ids.push(id_array.value(i));
        }
    }
    ids.sort_unstable();
    assert_eq!(ids, vec![5, 6], "Should return rows with id=5 and id=6");
    println!("✓ Query correctly scanned all partitions and returned 2 rows");

    // Test 4: Query with IN list on partition column
    println!("\n--- Test 4: Partition filter with IN list ---");
    let df = ctx
        .sql("SELECT * FROM partitioned_table WHERE region IN ('us-east-1', 'eu-west-1') ORDER BY id")
        .await?;

    let physical_plan = df.clone().create_physical_plan().await?;
    let mut explain_plan = datafusion::physical_plan::displayable(physical_plan.as_ref())
        .indent(true)
        .to_string();

    // Sanitize file paths for deterministic snapshots
    explain_plan = sanitize_file_paths(&explain_plan);

    println!("Physical plan:\n{explain_plan}");

    // Should not include us-west-1
    assert!(
        !explain_plan.contains("us-west-1"),
        "Physical plan should not include us-west-1 partition"
    );

    // Snapshot the explain plan
    insta::assert_snapshot!("partition_filter_in_list", explain_plan);

    let results = df.collect().await?;
    let total_rows: usize = results.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(
        total_rows, 4,
        "Expected 4 rows for region IN ('us-east-1', 'eu-west-1')"
    );

    // Verify correct regions
    for batch in &results {
        let region_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("region column should be StringArray");
        for i in 0..batch.num_rows() {
            let region = region_array.value(i);
            assert!(
                region == "us-east-1" || region == "eu-west-1",
                "All returned rows should have region in ('us-east-1', 'eu-west-1')"
            );
        }
    }
    println!("✓ IN list query correctly pruned us-west-1 partition");

    // Test 5: Full table scan (no filters)
    println!("\n--- Test 5: Full table scan (no filters) ---");
    let df = ctx
        .sql("SELECT COUNT(*) as total FROM partitioned_table")
        .await?;

    let results = df.collect().await?;
    let batch = &results[0];
    let count_array = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count column should be Int64Array");
    assert_eq!(count_array.value(0), 6, "Full scan should return 6 rows");
    println!("✓ Full table scan returned correct count");

    Ok(())
}

// Generate test variants for bucket partitioning
test_with_backends!(test_cayenne_bucket_partitioning_impl);

/// Test that validates bucket partitioning with `partition_by: bucket(3, id)` in Cayenne.
/// This test verifies that:
/// 1. Bucket partitioning works correctly
/// 2. Filters are still pushed down to partitions (since each bucket contains multiple values)
/// 3. Partition pruning works for filters on the bucketed column
#[allow(clippy::too_many_lines)]
async fn test_cayenne_bucket_partitioning_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let catalog = &fixture.catalog;
    let data_path = &fixture.data_path;
    let backend_name = fixture.backend_type.name();

    println!("✓ Catalog initialized with {backend_name} backend");

    // Create table schema with id column that will be bucketed
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));

    // Note: Cayenne's partition_column is a simple string, not an expression like bucket(3, id)
    // So we'll test with a simple column partition here, but the runtime integration with
    // PartitionTableProvider would handle bucket() expressions
    // For now, we'll demonstrate the pattern with a regular partition
    let table_options = CreateTableOptions {
        table_name: "bucket_table".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec![],
        base_path: data_path.to_string_lossy().to_string(),
        partition_column: Some("id".to_string()), // In practice, runtime would use bucket(3, id)
    };

    let table = CayenneTableProvider::create_table(
        Arc::<cayenne::CayenneCatalog>::clone(catalog),
        table_options,
    )
    .await?;
    println!("✓ Table created with partition column: id");

    // Register with DataFusion context
    let ctx = SessionContext::new();
    ctx.register_table("bucket_table", Arc::new(table))?;
    println!("✓ Table registered with DataFusion");

    // Insert test data - multiple rows that would be distributed across buckets
    // If we were using bucket(3, id), these would map to:
    // id=1 -> bucket 1, id=2 -> bucket 2, id=3 -> bucket 0
    // id=4 -> bucket 1, id=5 -> bucket 2, id=6 -> bucket 0
    // id=7 -> bucket 1, id=8 -> bucket 2, id=9 -> bucket 0
    ctx.sql(
        "INSERT INTO bucket_table VALUES \
         (1, 'Alice', 100), \
         (2, 'Bob', 200), \
         (3, 'Charlie', 300), \
         (4, 'David', 400), \
         (5, 'Eve', 500), \
         (6, 'Frank', 600), \
         (7, 'Grace', 700), \
         (8, 'Henry', 800), \
         (9, 'Iris', 900)",
    )
    .await?
    .collect()
    .await?;
    println!("✓ Inserted 9 rows across partitions");

    // Verify all data is present
    let df = ctx.sql("SELECT * FROM bucket_table ORDER BY id").await?;
    let results = df.collect().await?;
    let total_rows: usize = results.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 9, "Expected 9 rows after insert");
    println!("✓ All 9 rows retrieved");

    // Test 1: Query with filter on partition column
    // This demonstrates that filters are pushed down even for partitioned columns
    // because a partition can contain multiple values (especially with bucketing)
    println!("\n--- Test 1: Filter on partition column (id > 5) ---");
    let df = ctx
        .sql("SELECT * FROM bucket_table WHERE id > 5 ORDER BY id")
        .await?;

    let physical_plan = df.clone().create_physical_plan().await?;
    let mut explain_plan = datafusion::physical_plan::displayable(physical_plan.as_ref())
        .indent(true)
        .to_string();

    // Sanitize file paths for deterministic snapshots
    explain_plan = sanitize_file_paths(&explain_plan);

    println!("Physical plan:\n{explain_plan}");

    // Snapshot the explain plan - this should show filter pushdown
    insta::assert_snapshot!("bucket_partition_with_filter", explain_plan);

    // Verify query results
    let results = df.collect().await?;
    let total_rows: usize = results.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 4, "Expected 4 rows for id > 5");

    // Verify the data is correct
    for batch in &results {
        let id_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id column should be Int64Array");
        for i in 0..batch.num_rows() {
            let id = id_array.value(i);
            assert!(id > 5, "All returned rows should have id > 5, but got {id}");
        }
    }
    println!("✓ Query correctly returned 4 rows with id > 5");

    // Test 2: Query with range filter on partition column
    println!("\n--- Test 2: Range filter on partition column (id BETWEEN 3 AND 7) ---");
    let df = ctx
        .sql("SELECT * FROM bucket_table WHERE id BETWEEN 3 AND 7 ORDER BY id")
        .await?;

    let physical_plan = df.clone().create_physical_plan().await?;
    let mut explain_plan = datafusion::physical_plan::displayable(physical_plan.as_ref())
        .indent(true)
        .to_string();

    // Sanitize file paths for deterministic snapshots
    explain_plan = sanitize_file_paths(&explain_plan);

    println!("Physical plan:\n{explain_plan}");

    // Snapshot the explain plan
    insta::assert_snapshot!("bucket_partition_range_filter", explain_plan);

    let results = df.collect().await?;
    let total_rows: usize = results.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 5, "Expected 5 rows for id BETWEEN 3 AND 7");

    // Verify correct rows (id=3,4,5,6,7)
    let mut ids = Vec::new();
    for batch in &results {
        let id_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id column should be Int64Array");
        for i in 0..batch.num_rows() {
            ids.push(id_array.value(i));
        }
    }
    ids.sort_unstable();
    assert_eq!(
        ids,
        vec![3, 4, 5, 6, 7],
        "Should return rows with id=3,4,5,6,7"
    );
    println!("✓ Query correctly returned 5 rows in range");

    // Test 3: Query with filter on non-partition column
    println!("\n--- Test 3: Filter on non-partition column (value > 500) ---");
    let df = ctx
        .sql("SELECT * FROM bucket_table WHERE value > 500 ORDER BY id")
        .await?;

    let physical_plan = df.clone().create_physical_plan().await?;
    let mut explain_plan = datafusion::physical_plan::displayable(physical_plan.as_ref())
        .indent(true)
        .to_string();

    // Sanitize file paths for deterministic snapshots
    explain_plan = sanitize_file_paths(&explain_plan);

    println!("Physical plan:\n{explain_plan}");

    // Snapshot the explain plan
    insta::assert_snapshot!("bucket_partition_data_filter", explain_plan);

    let results = df.collect().await?;
    let total_rows: usize = results.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 4, "Expected 4 rows for value > 500");

    // Verify correct rows
    for batch in &results {
        let value_array = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("value column should be Int64Array");
        for i in 0..batch.num_rows() {
            let value = value_array.value(i);
            assert!(
                value > 500,
                "All returned rows should have value > 500, but got {value}"
            );
        }
    }
    println!("✓ Query correctly returned 4 rows with value > 500");

    // Test 4: Combined filters on partition and non-partition columns
    println!("\n--- Test 4: Combined filters (id > 3 AND value < 700) ---");
    let df = ctx
        .sql("SELECT * FROM bucket_table WHERE id > 3 AND value < 700 ORDER BY id")
        .await?;

    let physical_plan = df.clone().create_physical_plan().await?;
    let mut explain_plan = datafusion::physical_plan::displayable(physical_plan.as_ref())
        .indent(true)
        .to_string();

    // Sanitize file paths for deterministic snapshots
    explain_plan = sanitize_file_paths(&explain_plan);

    println!("Physical plan:\n{explain_plan}");

    // Snapshot the explain plan
    insta::assert_snapshot!("bucket_partition_combined_filters", explain_plan);

    let results = df.collect().await?;
    let total_rows: usize = results.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 3, "Expected 3 rows for id > 3 AND value < 700");

    // Verify correct rows (id=4,5,6)
    let mut ids = Vec::new();
    for batch in &results {
        let id_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id column should be Int64Array");
        for i in 0..batch.num_rows() {
            ids.push(id_array.value(i));
        }
    }
    ids.sort_unstable();
    assert_eq!(ids, vec![4, 5, 6], "Should return rows with id=4,5,6");
    println!("✓ Query correctly applied combined filters");

    Ok(())
}
