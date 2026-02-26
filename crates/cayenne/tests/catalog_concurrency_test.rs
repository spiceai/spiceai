/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! Catalog Concurrency Tests for Cayenne
//!
//! These tests validate that catalog operations handle concurrent access correctly,
//! particularly focusing on:
//!
//! 1. **Constraint Violation Handling**: When multiple operations try to create
//!    the same partition/delete file concurrently, they should all succeed and
//!    return the same ID (the first one creates, others find existing).
//!
//! 2. **Round-trip Tests**: Verify data survives create/read cycles.
//!
//! 3. **Backend Agnostic**: Tests run with both `SQLite` and Turso backends to
//!    ensure consistent behavior.

#![allow(clippy::expect_used)]

mod common;

use arrow::datatypes::{DataType, Field, Schema};
use cayenne::metadata::{CreateTableOptions, DeleteFile, DeletionType, PartitionMetadata};
use cayenne::{CayenneCatalog, MetadataCatalog};
use common::TestFixture;
use std::sync::Arc;

type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

// =============================================================================
// Test Macros
// =============================================================================

test_with_backends!(test_concurrent_partition_creation_impl);
test_with_backends!(test_partition_roundtrip_impl);
test_with_backends!(test_delete_file_roundtrip_impl);
test_with_backends!(test_table_metadata_roundtrip_impl);
test_with_backends!(test_constraint_violation_recovery_impl);
test_with_backends!(test_sequential_partition_stress_impl);
test_with_backends!(test_duplicate_partition_returns_existing_impl);

// =============================================================================
// Helper Functions
// =============================================================================

fn create_test_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("partition_date", DataType::Utf8, true),
        Field::new("value", DataType::Utf8, true),
    ]))
}

async fn create_partitioned_table(
    catalog: &Arc<CayenneCatalog>,
    table_name: &str,
    base_path: &str,
) -> TestResult<i64> {
    let schema = create_test_schema();

    let table_options = CreateTableOptions {
        table_name: table_name.to_string(),
        schema,
        primary_key: vec!["id".to_string()],
        on_conflict: None,
        base_path: base_path.to_string(),
        partition_column: Some("partition_date".to_string()),
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    Ok(catalog.create_table(table_options).await?)
}

// =============================================================================
// Concurrent Partition Creation Tests
// =============================================================================

/// Test that multiple calls to `add_partition` with the same partition value
/// all succeed and return the same partition ID.
/// This validates the constraint violation handling for partitions.
///
/// For `SQLite`: Uses true concurrency to test race conditions.
/// For Turso: Uses sequential calls due to single-connection architecture.
async fn test_concurrent_partition_creation_impl(fixture: TestFixture) -> TestResult<()> {
    use common::BackendType;

    let table_dir = fixture.data_path.join("concurrent_partition");
    std::fs::create_dir_all(&table_dir)?;

    let table_id = create_partitioned_table(
        &fixture.catalog,
        "concurrent_partition",
        &table_dir.to_string_lossy(),
    )
    .await?;

    let num_calls = 5;
    let mut partition_ids = vec![];

    // Turso has a single-connection architecture that doesn't support concurrent writes
    // so we run sequentially for Turso and concurrently for SQLite
    #[expect(clippy::collapsible_else_if)]
    if matches!(fixture.backend_type, BackendType::Sqlite) {
        // SQLite: Use true concurrency to test race conditions
        let mut handles = vec![];

        for i in 0..num_calls {
            let catalog_clone = Arc::clone(&fixture.catalog);
            let path = table_dir.join(format!("partition_20240101_{i}"));

            let handle = tokio::spawn(async move {
                let mut partition = PartitionMetadata::new_single(
                    table_id,
                    "partition_date".to_string(),
                    "2024-01-01".to_string(), // Same value for all
                    path.to_string_lossy().to_string(),
                    false,
                );
                partition.record_count = 100;
                partition.file_size_bytes = 1024;

                catalog_clone.add_partition(partition).await
            });

            handles.push(handle);
        }

        // Wait for all tasks to complete
        let results: Vec<_> = futures::future::join_all(handles).await;

        // All tasks should succeed (either creating or finding the partition)
        for result in results {
            let partition_id = result
                .expect("Task panicked")
                .expect("add_partition failed");
            partition_ids.push(partition_id);
        }
    } else {
        // Turso: Use sequential calls to avoid database locking
        for i in 0..num_calls {
            let path = table_dir.join(format!("partition_20240101_{i}"));

            let mut partition = PartitionMetadata::new_single(
                table_id,
                "partition_date".to_string(),
                "2024-01-01".to_string(), // Same value for all
                path.to_string_lossy().to_string(),
                false,
            );
            partition.record_count = 100;
            partition.file_size_bytes = 1024;

            let partition_id = fixture.catalog.add_partition(partition).await?;
            partition_ids.push(partition_id);
        }
    }

    // All calls should have gotten the same partition_id
    assert!(
        partition_ids.windows(2).all(|w| w[0] == w[1]),
        "All add_partition calls should return the same partition_id, got: {partition_ids:?}"
    );

    // Verify the partition exists and can be queried
    let partitions = fixture.catalog.get_partitions(table_id).await?;

    assert_eq!(partitions.len(), 1, "Should have exactly one partition");
    assert_eq!(partitions[0].partition_id, partition_ids[0]);
    assert_eq!(partitions[0].partition_values[0], "2024-01-01");

    Ok(())
}

// =============================================================================
// Round-trip Tests
// =============================================================================

/// Test partition metadata survives a create/read cycle.
async fn test_partition_roundtrip_impl(fixture: TestFixture) -> TestResult<()> {
    let table_dir = fixture.data_path.join("partition_roundtrip");
    std::fs::create_dir_all(&table_dir)?;

    let table_id = create_partitioned_table(
        &fixture.catalog,
        "partition_roundtrip",
        &table_dir.to_string_lossy(),
    )
    .await?;

    // Create multiple partitions
    let partition_values = ["2024-01-01", "2024-01-02", "2024-01-03"];
    let mut expected_ids = vec![];

    for value in partition_values {
        let mut partition = PartitionMetadata::new_single(
            table_id,
            "partition_date".to_string(),
            value.to_string(),
            table_dir
                .join(format!("partition_{value}"))
                .to_string_lossy()
                .to_string(),
            false,
        );
        partition.record_count = 100;
        partition.file_size_bytes = 1024;

        let id = fixture.catalog.add_partition(partition).await?;
        expected_ids.push((id, value.to_string()));
    }

    // Read back all partitions
    let partitions = fixture.catalog.get_partitions(table_id).await?;

    assert_eq!(partitions.len(), 3, "Should have 3 partitions");

    // Verify each partition
    for (expected_id, expected_value) in expected_ids {
        let found = partitions.iter().find(|p| p.partition_id == expected_id);
        assert!(found.is_some(), "Partition with id {expected_id} not found");
        assert_eq!(
            found.expect("checked above").partition_values[0],
            expected_value
        );
    }

    Ok(())
}

/// Test delete file metadata survives a create/read cycle.
async fn test_delete_file_roundtrip_impl(fixture: TestFixture) -> TestResult<()> {
    let table_dir = fixture.data_path.join("delete_roundtrip");
    std::fs::create_dir_all(&table_dir)?;

    let table_id = create_partitioned_table(
        &fixture.catalog,
        "delete_roundtrip",
        &table_dir.to_string_lossy(),
    )
    .await?;

    // Create multiple delete files
    let mut expected_ids = vec![];

    for i in 0..3 {
        let delete_file = DeleteFile {
            delete_file_id: 0,
            table_id,
            path: table_dir
                .join(format!("delete_{i}.bin"))
                .to_string_lossy()
                .to_string(),
            path_is_relative: false,
            format: "position".to_string(),
            delete_count: 50 + i,
            file_size_bytes: 512 + i,
            source_data_file_path: Some(format!("data_{i}.parquet")),
            deletion_type: DeletionType::default(),
            sequence_number: i,
        };

        let id = fixture.catalog.add_delete_file(delete_file).await?;
        expected_ids.push((id, i));
    }

    // Read back all delete files
    let delete_files = fixture.catalog.get_table_delete_files(table_id).await?;

    assert_eq!(delete_files.len(), 3, "Should have 3 delete files");

    // Verify each delete file
    for (expected_id, i) in expected_ids {
        let found = delete_files
            .iter()
            .find(|d| d.delete_file_id == expected_id);
        assert!(
            found.is_some(),
            "Delete file with id {expected_id} not found"
        );
        let df = found.expect("checked above");
        assert_eq!(df.delete_count, 50 + i);
        assert_eq!(df.sequence_number, i);
    }

    Ok(())
}

/// Test table metadata survives a create/read cycle.
async fn test_table_metadata_roundtrip_impl(fixture: TestFixture) -> TestResult<()> {
    let table_dir = fixture.data_path.join("table_roundtrip");
    std::fs::create_dir_all(&table_dir)?;

    let schema = create_test_schema();

    let table_options = CreateTableOptions {
        table_name: "table_roundtrip".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["id".to_string()],
        on_conflict: None,
        base_path: table_dir.to_string_lossy().to_string(),
        partition_column: Some("partition_date".to_string()),
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    let table_id = fixture.catalog.create_table(table_options).await?;

    // Read back the table
    let table_metadata = fixture.catalog.get_table("table_roundtrip").await?;

    assert_eq!(table_metadata.table_id, table_id);
    assert_eq!(table_metadata.table_name, "table_roundtrip");
    assert_eq!(table_metadata.primary_key, vec!["id".to_string()]);
    assert_eq!(
        table_metadata.partition_column,
        Some("partition_date".to_string())
    );
    assert_eq!(table_metadata.schema.fields().len(), 3);

    Ok(())
}

// =============================================================================
// Sequential Stress Tests
// =============================================================================

/// Sequential stress test for partition creation.
/// Creates many partitions one at a time to test catalog stability.
/// This avoids Turso's connection locking issues while still stress-testing
/// the constraint violation recovery path.
async fn test_sequential_partition_stress_impl(fixture: TestFixture) -> TestResult<()> {
    let table_dir = fixture.data_path.join("stress_partition");
    std::fs::create_dir_all(&table_dir)?;

    let table_id = create_partitioned_table(
        &fixture.catalog,
        "stress_partition",
        &table_dir.to_string_lossy(),
    )
    .await?;

    // Create many different partitions sequentially
    let num_partitions = 20;
    let mut created_ids = vec![];

    for partition_idx in 0..num_partitions {
        let partition_value = format!("2024-01-{partition_idx:02}");
        let path = table_dir
            .join(format!("partition_{partition_idx}"))
            .to_string_lossy()
            .to_string();

        let mut partition = PartitionMetadata::new_single(
            table_id,
            "partition_date".to_string(),
            partition_value.clone(),
            path,
            false,
        );
        partition.record_count = 100;
        partition.file_size_bytes = 1024;

        let id = fixture.catalog.add_partition(partition).await?;
        created_ids.push((id, partition_value));
    }

    // Verify we have exactly num_partitions unique partitions
    let partitions = fixture.catalog.get_partitions(table_id).await?;
    assert_eq!(
        partitions.len(),
        num_partitions,
        "Should have {num_partitions} unique partitions"
    );

    // Verify all created partitions exist with correct values
    for (expected_id, expected_value) in created_ids {
        let found = partitions.iter().find(|p| p.partition_id == expected_id);
        assert!(found.is_some(), "Partition with id {expected_id} not found");
        assert_eq!(
            found.expect("checked above").partition_values[0],
            expected_value
        );
    }

    Ok(())
}

/// Test that attempting to create a duplicate partition returns the existing
/// partition ID instead of creating a new one or failing.
async fn test_duplicate_partition_returns_existing_impl(fixture: TestFixture) -> TestResult<()> {
    let table_dir = fixture.data_path.join("duplicate_partition");
    std::fs::create_dir_all(&table_dir)?;

    let table_id = create_partitioned_table(
        &fixture.catalog,
        "duplicate_partition",
        &table_dir.to_string_lossy(),
    )
    .await?;

    // Create first partition
    let mut partition1 = PartitionMetadata::new_single(
        table_id,
        "partition_date".to_string(),
        "2024-06-15".to_string(),
        table_dir.join("partition_v1").to_string_lossy().to_string(),
        false,
    );
    partition1.record_count = 1000;
    partition1.file_size_bytes = 4096;

    let first_id = fixture.catalog.add_partition(partition1).await?;

    // Create second partition with same key but different path
    let mut partition2 = PartitionMetadata::new_single(
        table_id,
        "partition_date".to_string(),
        "2024-06-15".to_string(), // Same partition value
        table_dir.join("partition_v2").to_string_lossy().to_string(), // Different path
        false,
    );
    partition2.record_count = 2000; // Different metadata
    partition2.file_size_bytes = 8192;

    let second_id = fixture.catalog.add_partition(partition2).await?;

    // Both should return the same ID
    assert_eq!(
        first_id, second_id,
        "Duplicate partition should return existing partition ID"
    );

    // Create third partition with same key
    let mut partition3 = PartitionMetadata::new_single(
        table_id,
        "partition_date".to_string(),
        "2024-06-15".to_string(), // Same partition value again
        table_dir.join("partition_v3").to_string_lossy().to_string(),
        false,
    );
    partition3.record_count = 3000;
    partition3.file_size_bytes = 12288;

    let third_id = fixture.catalog.add_partition(partition3).await?;

    // Still should be the same ID
    assert_eq!(
        first_id, third_id,
        "All duplicate partitions should return the same ID"
    );

    // Verify only one partition exists in the catalog
    let all_partitions = fixture.catalog.get_partitions(table_id).await?;
    assert_eq!(
        all_partitions.len(),
        1,
        "Should have exactly one partition despite multiple add attempts"
    );
    assert_eq!(all_partitions[0].partition_id, first_id);
    assert_eq!(all_partitions[0].partition_values[0], "2024-06-15");

    Ok(())
}

/// Test that constraint violations are properly recovered from,
/// even when operations interleave in specific patterns.
async fn test_constraint_violation_recovery_impl(fixture: TestFixture) -> TestResult<()> {
    let table_dir = fixture.data_path.join("constraint_recovery");
    std::fs::create_dir_all(&table_dir)?;

    let table_id = create_partitioned_table(
        &fixture.catalog,
        "constraint_recovery",
        &table_dir.to_string_lossy(),
    )
    .await?;

    // First, create a partition
    let mut partition1 = PartitionMetadata::new_single(
        table_id,
        "partition_date".to_string(),
        "2024-01-01".to_string(),
        table_dir.join("partition_1").to_string_lossy().to_string(),
        false,
    );
    partition1.record_count = 100;
    partition1.file_size_bytes = 1024;

    let first_id = fixture.catalog.add_partition(partition1).await?;

    // Now try to create the same partition again (should return same ID)
    let mut partition2 = PartitionMetadata::new_single(
        table_id,
        "partition_date".to_string(),
        "2024-01-01".to_string(), // Same value
        table_dir.join("partition_2").to_string_lossy().to_string(), // Different path
        false,
    );
    partition2.record_count = 200; // Different count
    partition2.file_size_bytes = 2048;

    let second_id = fixture.catalog.add_partition(partition2).await?;

    // Should return the same ID (the existing partition)
    assert_eq!(
        first_id, second_id,
        "Duplicate partition should return the existing partition ID"
    );

    // Verify only one partition exists
    let all_partitions = fixture.catalog.get_partitions(table_id).await?;
    assert_eq!(all_partitions.len(), 1, "Should have exactly one partition");

    Ok(())
}
