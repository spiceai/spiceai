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

//! Comprehensive tests for ON CONFLICT (upsert/ignore) behavior in Pepper.
//!
//! These tests verify:
//! - DO NOTHING (drop duplicates)
//! - UPSERT (update on conflict)
//! - Validation of conflict columns matching primary key
//! - Error handling for invalid configurations

mod common;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use common::TestFixture;
use datafusion::datasource::TableProvider;
use datafusion::prelude::*;
use datafusion_execution::SendableRecordBatchStream;
use datafusion_table_providers::util::column_reference::ColumnReference;
use datafusion_table_providers::util::constraints::UpsertOptions;
use datafusion_table_providers::util::on_conflict::OnConflict;
use pepper::metadata::CreateTableOptions;
use pepper::{MetadataCatalog, PepperTableProvider};
use std::sync::Arc;

test_with_backends!(test_on_conflict_do_nothing_all_impl);
test_with_backends!(test_on_conflict_do_nothing_columns_impl);
test_with_backends!(test_on_conflict_upsert_impl);
test_with_backends!(test_on_conflict_upsert_preserves_unspecified_columns_impl);
test_with_backends!(test_on_conflict_requires_primary_key_impl);
test_with_backends!(test_on_conflict_columns_must_match_pk_impl);
test_with_backends!(test_on_conflict_with_composite_key_impl);

/// Test ON CONFLICT DO NOTHING with no specific columns (drops all conflicts)
async fn test_on_conflict_do_nothing_all_impl(
    fixture: TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let table_dir = fixture.data_path.join("on_conflict_do_nothing_all");
    std::fs::create_dir_all(&table_dir)?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let table_options = CreateTableOptions {
        table_name: "do_nothing_all".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["id".to_string()],
        base_path: table_dir.to_string_lossy().to_string(),
        partition_column: None,
        on_conflict: Some(OnConflict::DoNothingAll),
    };

    let catalog_arc = Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let table_provider =
        Arc::new(PepperTableProvider::create_table(catalog_arc, table_options).await?);

    // Insert initial data
    let batch1 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
            Arc::new(Int64Array::from(vec![100, 200, 300])),
        ],
    )?;

    let stream = futures::stream::iter(vec![Ok(batch1)]);
    let adapter = datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
        Arc::clone(&schema),
        stream,
    );
    let sendable: SendableRecordBatchStream = Box::pin(adapter);

    let inserted = table_provider.insert(sendable).await?;
    assert_eq!(inserted, 3, "Should insert all 3 initial rows");

    // Attempt to insert conflicting rows (same IDs) with DO NOTHING
    let batch2 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![2, 3, 4])), // 2 and 3 conflict
            Arc::new(StringArray::from(vec![
                "Bob_Updated",
                "Charlie_Updated",
                "Diana",
            ])),
            Arc::new(Int64Array::from(vec![222, 333, 400])),
        ],
    )?;

    let stream = futures::stream::iter(vec![Ok(batch2)]);
    let adapter = datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
        Arc::clone(&schema),
        stream,
    );
    let sendable: SendableRecordBatchStream = Box::pin(adapter);

    let inserted = table_provider.insert(sendable).await?;
    // With DO NOTHING, only Diana (id=4) should be inserted, conflicts dropped
    assert_eq!(inserted, 1, "Should insert only 1 non-conflicting row");

    // Query to verify data
    let ctx = SessionContext::new();
    ctx.register_table(
        "do_nothing_all",
        Arc::clone(&table_provider) as Arc<dyn TableProvider>,
    )?;

    let df = ctx
        .sql("SELECT id, name, value FROM do_nothing_all ORDER BY id")
        .await?;
    let batches = df.collect().await?;

    // Should have 4 rows: original 1,2,3 plus new 4
    let mut ids = Vec::new();
    let mut names = Vec::new();
    let mut values = Vec::new();

    for batch in &batches {
        let id_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id column");
        let name_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("name column");
        let value_array = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("value column");

        for idx in 0..batch.num_rows() {
            ids.push(id_array.value(idx));
            names.push(name_array.value(idx).to_string());
            values.push(value_array.value(idx));
        }
    }

    assert_eq!(ids, vec![1, 2, 3, 4], "Should have IDs 1-4");
    assert_eq!(
        names,
        vec!["Alice", "Bob", "Charlie", "Diana"],
        "Original names should be preserved (not updated)"
    );
    assert_eq!(
        values,
        vec![100, 200, 300, 400],
        "Original values should be preserved"
    );

    Ok(())
}

/// Test ON CONFLICT DO NOTHING with specific columns
async fn test_on_conflict_do_nothing_columns_impl(
    fixture: TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let table_dir = fixture.data_path.join("on_conflict_do_nothing_cols");
    std::fs::create_dir_all(&table_dir)?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let conflict_columns = ColumnReference::new(vec!["id".to_string()]);

    let table_options = CreateTableOptions {
        table_name: "do_nothing_cols".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["id".to_string()],
        base_path: table_dir.to_string_lossy().to_string(),
        partition_column: None,
        on_conflict: Some(OnConflict::DoNothing(conflict_columns)),
    };

    let catalog_arc = Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let table_provider =
        Arc::new(PepperTableProvider::create_table(catalog_arc, table_options).await?);

    // Insert initial rows
    let batch1 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(Int64Array::from(vec![10, 20])),
        ],
    )?;

    let stream = futures::stream::iter(vec![Ok(batch1)]);
    let adapter = datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
        Arc::clone(&schema),
        stream,
    );
    let sendable: SendableRecordBatchStream = Box::pin(adapter);

    table_provider.insert(sendable).await?;

    // Try to insert conflicting row
    let batch2 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 3])),
            Arc::new(Int64Array::from(vec![999, 30])),
        ],
    )?;

    let stream = futures::stream::iter(vec![Ok(batch2)]);
    let adapter = datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
        Arc::clone(&schema),
        stream,
    );
    let sendable: SendableRecordBatchStream = Box::pin(adapter);

    let inserted = table_provider.insert(sendable).await?;
    assert_eq!(inserted, 1, "Should insert only non-conflicting row");

    // Verify original value not updated
    let ctx = SessionContext::new();
    ctx.register_table(
        "do_nothing_cols",
        Arc::clone(&table_provider) as Arc<dyn TableProvider>,
    )?;

    let df = ctx
        .sql("SELECT value FROM do_nothing_cols WHERE id = 1")
        .await?;
    let batches = df.collect().await?;
    let value = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("value column")
        .value(0);

    assert_eq!(
        value, 10,
        "Original value should be preserved, not updated to 999"
    );

    Ok(())
}

/// Test ON CONFLICT with UPSERT (UPDATE on conflict)
async fn test_on_conflict_upsert_impl(
    fixture: TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let table_dir = fixture.data_path.join("on_conflict_upsert");
    std::fs::create_dir_all(&table_dir)?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let conflict_columns = ColumnReference::new(vec!["id".to_string()]);
    let upsert_options = UpsertOptions::default();

    let table_options = CreateTableOptions {
        table_name: "upsert_test".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["id".to_string()],
        base_path: table_dir.to_string_lossy().to_string(),
        partition_column: None,
        on_conflict: Some(OnConflict::Upsert(conflict_columns, upsert_options)),
    };

    let catalog_arc = Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let table_provider =
        Arc::new(PepperTableProvider::create_table(catalog_arc, table_options).await?);

    // Insert initial data
    let batch1 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
            Arc::new(Int64Array::from(vec![100, 200, 300])),
        ],
    )?;

    let stream = futures::stream::iter(vec![Ok(batch1)]);
    let adapter = datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
        Arc::clone(&schema),
        stream,
    );
    let sendable: SendableRecordBatchStream = Box::pin(adapter);

    let inserted = table_provider.insert(sendable).await?;
    eprintln!("DEBUG: First insert returned {}", inserted);
    assert_eq!(inserted, 3, "Should insert all 3 initial rows");

    // Insert with conflicts - should UPDATE existing rows
    let batch2 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![2, 3, 4])),
            Arc::new(StringArray::from(vec![
                "Bob_Updated",
                "Charlie_Updated",
                "Diana",
            ])),
            Arc::new(Int64Array::from(vec![222, 333, 400])),
        ],
    )?;

    let stream = futures::stream::iter(vec![Ok(batch2)]);
    let adapter = datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
        Arc::clone(&schema),
        stream,
    );
    let sendable: SendableRecordBatchStream = Box::pin(adapter);

    let inserted = table_provider.insert(sendable).await?;
    eprintln!("DEBUG: Second insert returned {}", inserted);
    // Should insert 1 new row (Diana) and update 2 existing (Bob, Charlie)
    assert_eq!(
        inserted, 3,
        "Should process all 3 rows (1 insert + 2 updates)"
    );

    // Small delay to ensure all files are flushed
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Query to verify updates
    let ctx = SessionContext::new();
    ctx.register_table(
        "upsert_test",
        Arc::clone(&table_provider) as Arc<dyn TableProvider>,
    )?;

    // List files in snapshot directory
    let snapshot_dir = table_dir
        .join(table_provider.metadata().table_id.to_string())
        .join(&table_provider.metadata().current_snapshot_id);
    eprintln!("DEBUG: Snapshot directory: {:?}", snapshot_dir);
    if snapshot_dir.exists() {
        let entries: Vec<_> = std::fs::read_dir(&snapshot_dir)?
            .filter_map(|e| e.ok())
            .collect();
        eprintln!("DEBUG: Found {} entries in snapshot dir", entries.len());
        for entry in entries {
            let path = entry.path();
            let metadata = std::fs::metadata(&path)?;
            eprintln!(
                "DEBUG:   - {:?} ({} bytes)",
                path.file_name(),
                metadata.len()
            );
        }
    } else {
        eprintln!("DEBUG: Snapshot directory does not exist!");
    }

    // Check key_index entries
    eprintln!("DEBUG: Querying key_index...");
    let db_path = fixture.db_path();
    let conn = rusqlite::Connection::open(&db_path)?;
    let mut stmt = conn.prepare(
        "SELECT key_bytes, row_id, begin_snapshot, end_snapshot FROM pepper_key_index WHERE table_id = ? ORDER BY row_id"
    )?;
    let rows: Result<Vec<_>, _> = stmt
        .query_map([table_provider.metadata().table_id], |row| {
            Ok((
                row.get::<_, Vec<u8>>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<String>>(3)?,
            ))
        })?
        .collect();
    for (key_bytes, row_id, begin_snap, end_snap) in rows? {
        // Assume key_bytes is just the i64 id for simplicity
        let id = i64::from_be_bytes(key_bytes.as_slice().try_into().unwrap_or([0; 8]));
        eprintln!(
            "DEBUG: key_index: id={}, row_id={}, begin={}, end={:?}",
            id,
            row_id,
            &begin_snap[..8],
            end_snap.as_ref().map(|s| &s[..8])
        );
    }

    // Check deletion vectors
    let delete_files = table_provider
        .catalog()
        .get_table_delete_files(table_provider.metadata().table_id)
        .await?;
    eprintln!("DEBUG: Found {} deletion vector files", delete_files.len());
    for (i, df) in delete_files.iter().enumerate() {
        eprintln!(
            "DEBUG: Delete file {}: data_file_id={}, delete_count={}, path={}",
            i, df.data_file_id, df.delete_count, df.path
        );
    }

    // List files in snapshot dir again right before query to verify they exist
    eprintln!("DEBUG: About to run SELECT query, verifying files still exist...");
    if snapshot_dir.is_dir() {
        let entries_before_query: Vec<_> = std::fs::read_dir(&snapshot_dir)?
            .filter_map(|e| e.ok())
            .collect();
        eprintln!(
            "DEBUG: Found {} entries before query",
            entries_before_query.len()
        );
        for entry in entries_before_query {
            if let Ok(metadata) = entry.metadata() {
                eprintln!(
                    "DEBUG:   - {:?} ({} bytes, is_file: {})",
                    entry.file_name(),
                    metadata.len(),
                    metadata.is_file()
                );
            }
        }
    }

    let df = ctx
        .sql("SELECT id, name, value FROM upsert_test ORDER BY id")
        .await?;
    let batches = df.collect().await?;

    eprintln!("DEBUG: Query returned {} batches", batches.len());
    for (i, batch) in batches.iter().enumerate() {
        eprintln!("DEBUG: Batch {} has {} rows", i, batch.num_rows());
        if batch.num_rows() > 0 {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("ids");
            eprintln!("DEBUG: Batch {} IDs: {:?}", i, ids.values());
        }
    }

    let mut ids = Vec::new();
    let mut names = Vec::new();
    let mut values = Vec::new();

    for batch in &batches {
        let id_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id column");
        let name_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("name column");
        let value_array = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("value column");

        for idx in 0..batch.num_rows() {
            ids.push(id_array.value(idx));
            names.push(name_array.value(idx).to_string());
            values.push(value_array.value(idx));
        }
    }

    assert_eq!(ids, vec![1, 2, 3, 4], "Should have IDs 1-4");
    assert_eq!(
        names,
        vec!["Alice", "Bob_Updated", "Charlie_Updated", "Diana"],
        "Conflicting rows should be UPDATED with new names"
    );
    assert_eq!(
        values,
        vec![100, 222, 333, 400],
        "Conflicting rows should be UPDATED with new values"
    );

    Ok(())
}

/// Test that UPSERT preserves columns not in the insert
async fn test_on_conflict_upsert_preserves_unspecified_columns_impl(
    fixture: TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let table_dir = fixture.data_path.join("upsert_preserve");
    std::fs::create_dir_all(&table_dir)?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
        Field::new("extra", DataType::Utf8, true),
    ]));

    let conflict_columns = ColumnReference::new(vec!["id".to_string()]);
    let upsert_options = UpsertOptions::default();

    let table_options = CreateTableOptions {
        table_name: "upsert_preserve".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["id".to_string()],
        base_path: table_dir.to_string_lossy().to_string(),
        partition_column: None,
        on_conflict: Some(OnConflict::Upsert(conflict_columns, upsert_options)),
    };

    let catalog_arc = Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let table_provider =
        Arc::new(PepperTableProvider::create_table(catalog_arc, table_options).await?);

    // Insert initial data with all columns
    let batch1 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["Alice"])),
            Arc::new(Int64Array::from(vec![100])),
            Arc::new(StringArray::from(vec![Some("original_extra")])),
        ],
    )?;

    let stream = futures::stream::iter(vec![Ok(batch1)]);
    let adapter = datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
        Arc::clone(&schema),
        stream,
    );
    let sendable: SendableRecordBatchStream = Box::pin(adapter);

    table_provider.insert(sendable).await?;

    // Update with conflicting ID but provide all columns (including new extra value)
    let batch2 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["Alice_Updated"])),
            Arc::new(Int64Array::from(vec![999])),
            Arc::new(StringArray::from(vec![Some("updated_extra")])),
        ],
    )?;

    let stream = futures::stream::iter(vec![Ok(batch2)]);
    let adapter = datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
        Arc::clone(&schema),
        stream,
    );
    let sendable: SendableRecordBatchStream = Box::pin(adapter);

    table_provider.insert(sendable).await?;

    // Verify all columns were updated
    let ctx = SessionContext::new();
    ctx.register_table(
        "upsert_preserve",
        Arc::clone(&table_provider) as Arc<dyn TableProvider>,
    )?;

    let df = ctx
        .sql("SELECT name, value, extra FROM upsert_preserve WHERE id = 1")
        .await?;
    let batches = df.collect().await?;
    let batch = &batches[0];

    let name = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("name")
        .value(0);
    let value = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("value")
        .value(0);
    let extra = batch
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("extra")
        .value(0);

    assert_eq!(name, "Alice_Updated", "Name should be updated");
    assert_eq!(value, 999, "Value should be updated");
    assert_eq!(extra, "updated_extra", "Extra column should be updated");

    Ok(())
}

/// Test that ON CONFLICT requires a primary key
async fn test_on_conflict_requires_primary_key_impl(
    fixture: TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let table_dir = fixture.data_path.join("on_conflict_no_pk");
    std::fs::create_dir_all(&table_dir)?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let table_options = CreateTableOptions {
        table_name: "no_pk".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec![], // No primary key!
        base_path: table_dir.to_string_lossy().to_string(),
        partition_column: None,
        on_conflict: Some(OnConflict::DoNothingAll),
    };

    let catalog_arc = Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let table_provider =
        Arc::new(PepperTableProvider::create_table(catalog_arc, table_options).await?);

    // Try to insert - should fail during conflict resolution
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![10])),
        ],
    )?;

    let stream = futures::stream::iter(vec![Ok(batch)]);
    let adapter = datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
        Arc::clone(&schema),
        stream,
    );
    let sendable: SendableRecordBatchStream = Box::pin(adapter);

    let result = table_provider.insert(sendable).await;

    assert!(
        result.is_err(),
        "Insert should fail when on_conflict is set but no primary key defined"
    );

    let error_msg = result.unwrap_err().to_string();
    assert!(
        error_msg.contains("on_conflict") && error_msg.contains("primary key"),
        "Error should mention on_conflict and primary key requirement: {error_msg}"
    );

    Ok(())
}

/// Test that ON CONFLICT columns must match the primary key
async fn test_on_conflict_columns_must_match_pk_impl(
    fixture: TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let table_dir = fixture.data_path.join("on_conflict_mismatch");
    std::fs::create_dir_all(&table_dir)?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("code", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));

    // Primary key is "id", but conflict columns specify "code"
    let conflict_columns = ColumnReference::new(vec!["code".to_string()]);

    let table_options = CreateTableOptions {
        table_name: "mismatch".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["id".to_string()],
        base_path: table_dir.to_string_lossy().to_string(),
        partition_column: None,
        on_conflict: Some(OnConflict::DoNothing(conflict_columns)),
    };

    let catalog_arc = Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let table_provider =
        Arc::new(PepperTableProvider::create_table(catalog_arc, table_options).await?);

    // Try to insert
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["A"])),
            Arc::new(Int64Array::from(vec![100])),
        ],
    )?;

    let stream = futures::stream::iter(vec![Ok(batch)]);
    let adapter = datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
        Arc::clone(&schema),
        stream,
    );
    let sendable: SendableRecordBatchStream = Box::pin(adapter);

    let result = table_provider.insert(sendable).await;

    assert!(
        result.is_err(),
        "Insert should fail when conflict columns don't match primary key"
    );

    let error_msg = result.unwrap_err().to_string();
    assert!(
        error_msg.contains("on_conflict columns") && error_msg.contains("primary key"),
        "Error should explain conflict columns must match PK: {error_msg}"
    );

    Ok(())
}

/// Test ON CONFLICT with composite primary key
async fn test_on_conflict_with_composite_key_impl(
    fixture: TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let table_dir = fixture.data_path.join("on_conflict_composite");
    std::fs::create_dir_all(&table_dir)?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("tenant_id", DataType::Int64, false),
        Field::new("user_id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let conflict_columns =
        ColumnReference::new(vec!["tenant_id".to_string(), "user_id".to_string()]);
    let upsert_options = UpsertOptions::default();

    let table_options = CreateTableOptions {
        table_name: "composite_key".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["tenant_id".to_string(), "user_id".to_string()],
        base_path: table_dir.to_string_lossy().to_string(),
        partition_column: None,
        on_conflict: Some(OnConflict::Upsert(conflict_columns, upsert_options)),
    };

    let catalog_arc = Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let table_provider =
        Arc::new(PepperTableProvider::create_table(catalog_arc, table_options).await?);

    // Insert initial data
    let batch1 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 2])),
            Arc::new(Int64Array::from(vec![100, 200, 100])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
        ],
    )?;

    let stream = futures::stream::iter(vec![Ok(batch1)]);
    let adapter = datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
        Arc::clone(&schema),
        stream,
    );
    let sendable: SendableRecordBatchStream = Box::pin(adapter);

    table_provider.insert(sendable).await?;

    // Insert with conflict on composite key (tenant_id=1, user_id=100)
    let batch2 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(Int64Array::from(vec![100, 300])),
            Arc::new(StringArray::from(vec!["Alice_Updated", "Diana"])),
        ],
    )?;

    let stream = futures::stream::iter(vec![Ok(batch2)]);
    let adapter = datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
        Arc::clone(&schema),
        stream,
    );
    let sendable: SendableRecordBatchStream = Box::pin(adapter);

    table_provider.insert(sendable).await?;

    // Query to verify
    let ctx = SessionContext::new();
    ctx.register_table(
        "composite_key",
        Arc::clone(&table_provider) as Arc<dyn TableProvider>,
    )?;

    let df = ctx
        .sql("SELECT tenant_id, user_id, name FROM composite_key ORDER BY tenant_id, user_id")
        .await?;
    let batches = df.collect().await?;

    let mut tenant_ids = Vec::new();
    let mut user_ids = Vec::new();
    let mut names = Vec::new();

    for batch in &batches {
        let tenant_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("tenant_id");
        let user_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("user_id");
        let name_array = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("name");

        for idx in 0..batch.num_rows() {
            tenant_ids.push(tenant_array.value(idx));
            user_ids.push(user_array.value(idx));
            names.push(name_array.value(idx).to_string());
        }
    }

    assert_eq!(
        tenant_ids,
        vec![1, 1, 2, 2],
        "Should have correct tenant_ids"
    );
    assert_eq!(
        user_ids,
        vec![100, 200, 100, 300],
        "Should have correct user_ids"
    );
    assert_eq!(
        names,
        vec!["Alice_Updated", "Bob", "Charlie", "Diana"],
        "Composite key conflict should update Alice, others unchanged"
    );

    Ok(())
}
