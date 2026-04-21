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

//! Tests for table-level aggregate statistics in the Cayenne metastore.
//!
//! Statistics are stored as serialized Vortex `FileStatistics` blobs,
//! containing per-column stats (min, max, null count, etc.).

mod common;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use cayenne::metadata::{CreateTableOptions, TableStatistics};
use cayenne::{CayenneTableProvider, MetadataCatalog};
use datafusion::prelude::SessionContext;
use std::sync::Arc;

// Generate test variants for each backend
test_with_backends!(test_table_statistics_crud);
test_with_backends!(test_stats_persisted_after_insert);
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

    // Upsert stats with a dummy blob
    let dummy_blob = vec![1, 2, 3, 4];
    let table_stats = TableStatistics {
        table_id: table_id.clone(),
        statistics_blob: dummy_blob.clone(),
        num_rows: 100,
    };
    catalog.upsert_table_statistics(&table_stats).await?;

    // Read back
    let stats = catalog
        .get_table_statistics(&table_id)
        .await?
        .expect("stats should exist");
    assert_eq!(stats.statistics_blob, dummy_blob);
    assert_eq!(stats.num_rows, 100);

    // Upsert updates existing stats
    let updated = TableStatistics {
        table_id: table_id.clone(),
        statistics_blob: vec![5, 6, 7, 8],
        num_rows: 200,
    };
    catalog.upsert_table_statistics(&updated).await?;

    let stats = catalog
        .get_table_statistics(&table_id)
        .await?
        .expect("stats should exist");
    assert_eq!(stats.statistics_blob, vec![5, 6, 7, 8]);
    assert_eq!(stats.num_rows, 200);

    // Clear
    catalog.clear_table_statistics(&table_id).await?;
    let stats = catalog.get_table_statistics(&table_id).await?;
    assert!(stats.is_none());

    Ok(())
}

/// Test that table-level stats are persisted after inserting data.
async fn test_stats_persisted_after_insert(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let catalog = &fixture.catalog;
    let data_path = &fixture.data_path;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));

    let ctx = SessionContext::new();
    let table_name = "stats_insert_test";
    let table = CayenneTableProvider::create_table(
        Arc::clone(catalog) as Arc<dyn MetadataCatalog>,
        CreateTableOptions {
            table_name: table_name.to_string(),
            schema: Arc::clone(&schema),
            primary_key: vec![],
            on_conflict: None,
            base_path: data_path.to_string_lossy().to_string(),
            partition_column: None,
            vortex_config: cayenne::metadata::VortexConfig::default(),
        },
        ctx.runtime_env(),
    )
    .await?;

    let table_id = catalog.get_table(table_name).await?.table_id;

    // Insert data
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec![
                Some("Alice"),
                Some("Bob"),
                None,
                Some("Diana"),
                Some("Eve"),
            ])),
        ],
    )?;

    common::insert_batch(&table, batch).await?;

    // Verify stats were persisted
    let stats = catalog.get_table_statistics(&table_id).await?;
    assert!(
        stats.is_some(),
        "Expected table statistics to be persisted after insert"
    );

    let stats = stats.expect("table statistics should exist");
    assert_eq!(stats.num_rows, 5, "Expected 5 rows");
    assert!(
        !stats.statistics_blob.is_empty(),
        "Expected non-empty statistics blob"
    );

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
