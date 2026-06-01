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

//! Tests for table-level statistics in the Cayenne metastore.
//!
//! Statistics are stored as serialized Vortex `FileStatistics` blobs
//! containing per-column min, max, and null count. The row in
//! `cayenne_table_statistics` is keyed by `table_id` and upserted on every
//! write, so it currently captures the accumulator from the most recent
//! write (last-write-wins) rather than an aggregate across every file.

mod common;

use arrow::datatypes::{DataType, Field, Schema};
use cayenne::MetadataCatalog;
use cayenne::metadata::{CreateTableOptions, TableStatistics};
use std::sync::Arc;

// Generate test variants for each backend
test_with_backends!(test_table_statistics_crud);
test_with_backends!(test_stats_cleared_on_drop_table);

/// Test basic CRUD operations on `cayenne_table_statistics`.
async fn test_table_statistics_crud(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let catalog = &fixture.catalog;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));

    let table_id = catalog
        .create_table(CreateTableOptions {
            table_name: "stats_test".to_string(),
            schema: Arc::clone(&schema),
            primary_key: vec![],
            on_conflict: None,
            base_path: fixture.data_path.to_string_lossy().to_string(),
            partition_column: None,
            vortex_config: cayenne::metadata::VortexConfig::default(),
        })
        .await?;

    // Initially no stats
    let stats = catalog.get_table_statistics(&table_id).await?;
    assert!(stats.is_none(), "Expected no stats initially");

    // Upsert stats with a dummy blob and NDV sketch blob
    let dummy_blob = vec![1, 2, 3, 4];
    let dummy_ndv = vec![9, 8, 7, 6, 5];
    let table_stats = TableStatistics {
        table_id: table_id.clone(),
        statistics_blob: dummy_blob.clone(),
        num_rows: 100,
        ndv_sketches: Some(dummy_ndv.clone()),
    };
    catalog.upsert_table_statistics(&table_stats).await?;

    // Read back
    let stats = catalog
        .get_table_statistics(&table_id)
        .await?
        .expect("stats should exist");
    assert_eq!(stats.statistics_blob, dummy_blob);
    assert_eq!(stats.num_rows, 100);
    assert_eq!(stats.ndv_sketches, Some(dummy_ndv));

    // Upsert updates existing stats, including clearing the NDV sketches to NULL.
    let updated = TableStatistics {
        table_id: table_id.clone(),
        statistics_blob: vec![5, 6, 7, 8],
        num_rows: 200,
        ndv_sketches: None,
    };
    catalog.upsert_table_statistics(&updated).await?;

    let stats = catalog
        .get_table_statistics(&table_id)
        .await?
        .expect("stats should exist");
    assert_eq!(stats.statistics_blob, vec![5, 6, 7, 8]);
    assert_eq!(stats.num_rows, 200);
    assert_eq!(stats.ndv_sketches, None);

    // Clear
    catalog.clear_table_statistics(&table_id).await?;
    let stats = catalog.get_table_statistics(&table_id).await?;
    assert!(stats.is_none());

    Ok(())
}

/// Test that stats are cleaned up when a table is dropped.
async fn test_stats_cleared_on_drop_table(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let catalog = &fixture.catalog;

    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

    let table_id = catalog
        .create_table(CreateTableOptions {
            table_name: "drop_stats_test".to_string(),
            schema,
            primary_key: vec![],
            on_conflict: None,
            base_path: fixture.data_path.to_string_lossy().to_string(),
            partition_column: None,
            vortex_config: cayenne::metadata::VortexConfig::default(),
        })
        .await?;

    // Add some stats
    catalog
        .upsert_table_statistics(&TableStatistics {
            table_id: table_id.clone(),
            statistics_blob: vec![1, 2, 3],
            num_rows: 100,
            ndv_sketches: None,
        })
        .await?;

    // Verify stats exist
    assert!(catalog.get_table_statistics(&table_id).await?.is_some());

    // Drop table
    let dropped = catalog.drop_table("drop_stats_test").await?;
    assert!(dropped);

    // Stats should be gone
    let stats = catalog.get_table_statistics(&table_id).await?;
    assert!(
        stats.is_none(),
        "Table statistics should be cleared after drop_table"
    );

    Ok(())
}
