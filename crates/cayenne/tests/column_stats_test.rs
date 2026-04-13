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

//! Tests for column-level and file-level statistics in the Cayenne metastore.

mod common;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use cayenne::metadata::{ColumnStats, CreateTableOptions, FileColumnStats};
use cayenne::{CayenneTableProvider, MetadataCatalog};
use datafusion::prelude::SessionContext;
use std::sync::Arc;

// Generate test variants for each backend
test_with_backends!(test_column_stats_crud);
test_with_backends!(test_file_column_stats_crud);
test_with_backends!(test_stats_persisted_after_insert);
test_with_backends!(test_stats_cleared_on_drop_table);

/// Test basic CRUD operations on cayenne_column_stats.
async fn test_column_stats_crud(
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
            schema,
            primary_key: vec![],
            on_conflict: None,
            base_path: fixture.data_path.to_string_lossy().to_string(),
            partition_column: None,
            vortex_config: cayenne::metadata::VortexConfig::default(),
        })
        .await?;

    // Initially no stats
    let stats = catalog.get_column_stats(&table_id).await?;
    assert!(stats.is_empty(), "Expected no stats initially");

    // Upsert stats
    let column_stats = vec![
        ColumnStats {
            table_id: table_id.clone(),
            column_name: "id".to_string(),
            min_value: Some("1".to_string()),
            max_value: Some("100".to_string()),
            null_count: Some(0),
            row_count: Some(100),
        },
        ColumnStats {
            table_id: table_id.clone(),
            column_name: "name".to_string(),
            min_value: Some("Alice".to_string()),
            max_value: Some("Zoe".to_string()),
            null_count: Some(5),
            row_count: Some(100),
        },
    ];
    catalog.upsert_column_stats(&column_stats).await?;

    // Read back
    let stats = catalog.get_column_stats(&table_id).await?;
    assert_eq!(stats.len(), 2);
    assert_eq!(stats[0].column_name, "id");
    assert_eq!(stats[0].min_value.as_deref(), Some("1"));
    assert_eq!(stats[0].max_value.as_deref(), Some("100"));
    assert_eq!(stats[0].null_count, Some(0));
    assert_eq!(stats[0].row_count, Some(100));
    assert_eq!(stats[1].column_name, "name");
    assert_eq!(stats[1].null_count, Some(5));

    // Upsert updates existing stats
    let updated = vec![ColumnStats {
        table_id: table_id.clone(),
        column_name: "id".to_string(),
        min_value: Some("1".to_string()),
        max_value: Some("200".to_string()),
        null_count: Some(0),
        row_count: Some(200),
    }];
    catalog.upsert_column_stats(&updated).await?;

    let stats = catalog.get_column_stats(&table_id).await?;
    assert_eq!(stats.len(), 2); // still 2 columns
    let id_stats = stats.iter().find(|s| s.column_name == "id").unwrap();
    assert_eq!(id_stats.max_value.as_deref(), Some("200"));
    assert_eq!(id_stats.row_count, Some(200));

    // Clear
    catalog.clear_column_stats(&table_id).await?;
    let stats = catalog.get_column_stats(&table_id).await?;
    assert!(stats.is_empty());

    Ok(())
}

/// Test basic CRUD operations on cayenne_file_column_stats.
async fn test_file_column_stats_crud(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let catalog = &fixture.catalog;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, true),
    ]));

    let table_id = catalog
        .create_table(CreateTableOptions {
            table_name: "file_stats_test".to_string(),
            schema,
            primary_key: vec![],
            on_conflict: None,
            base_path: fixture.data_path.to_string_lossy().to_string(),
            partition_column: None,
            vortex_config: cayenne::metadata::VortexConfig::default(),
        })
        .await?;

    // Upsert file stats for two files
    let file_stats = vec![
        FileColumnStats {
            table_id: table_id.clone(),
            file_path: "file_001.vortex".to_string(),
            column_name: "id".to_string(),
            min_value: Some("1".to_string()),
            max_value: Some("50".to_string()),
            null_count: Some(0),
            row_count: Some(50),
        },
        FileColumnStats {
            table_id: table_id.clone(),
            file_path: "file_001.vortex".to_string(),
            column_name: "value".to_string(),
            min_value: Some("10".to_string()),
            max_value: Some("999".to_string()),
            null_count: Some(3),
            row_count: Some(50),
        },
        FileColumnStats {
            table_id: table_id.clone(),
            file_path: "file_002.vortex".to_string(),
            column_name: "id".to_string(),
            min_value: Some("51".to_string()),
            max_value: Some("100".to_string()),
            null_count: Some(0),
            row_count: Some(50),
        },
    ];
    catalog.upsert_file_column_stats(&file_stats).await?;

    // Get all file stats
    let all_stats = catalog.get_file_column_stats(&table_id).await?;
    assert_eq!(all_stats.len(), 3);

    // Get stats for specific file
    let file1_stats = catalog
        .get_file_column_stats_for_file(&table_id, "file_001.vortex")
        .await?;
    assert_eq!(file1_stats.len(), 2);
    assert_eq!(file1_stats[0].file_path, "file_001.vortex");

    // Remove stats for one file
    catalog
        .remove_file_column_stats(&table_id, &["file_001.vortex".to_string()])
        .await?;
    let remaining = catalog.get_file_column_stats(&table_id).await?;
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].file_path, "file_002.vortex");

    // Clear all
    catalog.clear_file_column_stats(&table_id).await?;
    let empty = catalog.get_file_column_stats(&table_id).await?;
    assert!(empty.is_empty());

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
    let stats = catalog.get_column_stats(&table_id).await?;
    assert!(
        !stats.is_empty(),
        "Expected column stats to be persisted after insert"
    );

    // Find the id column stats
    let id_stats = stats.iter().find(|s| s.column_name == "id");
    assert!(
        id_stats.is_some(),
        "Expected stats for 'id' column to exist"
    );

    // Verify row count is populated
    let id_stats = id_stats.unwrap();
    assert!(
        id_stats.row_count.is_some(),
        "Expected row_count to be populated"
    );

    if let Some(row_count) = id_stats.row_count {
        assert_eq!(row_count, 5, "Expected 5 rows");
    }

    Ok(())
}

/// Test that stats are cleaned up when a table is dropped.
async fn test_stats_cleared_on_drop_table(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let catalog = &fixture.catalog;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
    ]));

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
        .upsert_column_stats(&[ColumnStats {
            table_id: table_id.clone(),
            column_name: "id".to_string(),
            min_value: Some("1".to_string()),
            max_value: Some("100".to_string()),
            null_count: Some(0),
            row_count: Some(100),
        }])
        .await?;
    catalog
        .upsert_file_column_stats(&[FileColumnStats {
            table_id: table_id.clone(),
            file_path: "file.vortex".to_string(),
            column_name: "id".to_string(),
            min_value: Some("1".to_string()),
            max_value: Some("100".to_string()),
            null_count: Some(0),
            row_count: Some(100),
        }])
        .await?;

    // Verify stats exist
    assert!(!catalog.get_column_stats(&table_id).await?.is_empty());
    assert!(!catalog.get_file_column_stats(&table_id).await?.is_empty());

    // Drop table
    let dropped = catalog.drop_table("drop_stats_test").await?;
    assert!(dropped);

    // Stats should be gone
    let column_stats = catalog.get_column_stats(&table_id).await?;
    assert!(
        column_stats.is_empty(),
        "Column stats should be cleared after drop_table"
    );
    let file_stats = catalog.get_file_column_stats(&table_id).await?;
    assert!(
        file_stats.is_empty(),
        "File column stats should be cleared after drop_table"
    );

    Ok(())
}
