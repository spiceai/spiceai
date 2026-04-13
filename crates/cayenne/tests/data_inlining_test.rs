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

//! Tests for data inlining: small batches stored in the metastore as Arrow IPC blobs.

mod common;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use cayenne::metadata::CreateTableOptions;
use cayenne::{CayenneTableProvider, MetadataCatalog};
use datafusion::datasource::TableProvider;
use datafusion::prelude::*;
use std::sync::Arc;

test_with_backends!(test_inlined_data_crud);
test_with_backends!(test_small_insert_inlined);
test_with_backends!(test_inlined_data_visible_in_scan);

/// Test basic CRUD for inlined data via the catalog API.
async fn test_inlined_data_crud(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let catalog = &fixture.catalog;

    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

    let table_id = catalog
        .create_table(CreateTableOptions {
            table_name: "inline_crud_test".to_string(),
            schema: Arc::clone(&schema),
            primary_key: vec![],
            on_conflict: None,
            base_path: fixture.data_path.to_string_lossy().to_string(),
            partition_column: None,
            vortex_config: cayenne::metadata::VortexConfig::default(),
        })
        .await?;

    // Initially no inlined data
    let data = catalog.get_inlined_data(&table_id).await?;
    assert!(data.is_empty());
    assert_eq!(catalog.get_inlined_data_count(&table_id).await?, 0);

    // Add inlined data
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    )?;
    let mut ipc_buf = Vec::new();
    {
        let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut ipc_buf, &schema)?;
        writer.write(&batch)?;
        writer.finish()?;
    }

    let id = catalog
        .add_inlined_data(cayenne::InlinedData {
            inlined_id: String::new(),
            table_id: table_id.clone(),
            partition_key: None,
            data_ipc: ipc_buf,
            record_count: 3,
            sequence_number: 0,
            created_at: String::new(),
        })
        .await?;
    assert!(!id.is_empty());

    // Verify count
    assert_eq!(catalog.get_inlined_data_count(&table_id).await?, 3);

    // Read back
    let data = catalog.get_inlined_data(&table_id).await?;
    assert_eq!(data.len(), 1);
    assert_eq!(data[0].record_count, 3);

    // Clear
    catalog.clear_inlined_data(&table_id).await?;
    assert_eq!(catalog.get_inlined_data_count(&table_id).await?, 0);

    Ok(())
}

/// Test that a small insert (< 1024 rows) gets inlined in the metastore.
async fn test_small_insert_inlined(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let catalog = &fixture.catalog;
    let data_path = &fixture.data_path;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));

    let ctx = SessionContext::new();
    let table = CayenneTableProvider::create_table(
        Arc::clone(catalog) as Arc<dyn MetadataCatalog>,
        CreateTableOptions {
            table_name: "small_inline_test".to_string(),
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

    let table_id = catalog.get_table("small_inline_test").await?.table_id;

    // Insert a small batch (5 rows — well under 1024 threshold)
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])),
        ],
    )?;

    common::insert_batch(&table, batch).await?;

    // Verify data was inlined in the metastore
    let inlined_count = catalog.get_inlined_data_count(&table_id).await?;
    assert_eq!(
        inlined_count, 5,
        "Expected 5 rows to be inlined in the metastore"
    );

    Ok(())
}

/// Test that inlined data is visible when scanning the table.
async fn test_inlined_data_visible_in_scan(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let catalog = &fixture.catalog;
    let data_path = &fixture.data_path;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let ctx = SessionContext::new();
    let table = CayenneTableProvider::create_table(
        Arc::clone(catalog) as Arc<dyn MetadataCatalog>,
        CreateTableOptions {
            table_name: "scan_inline_test".to_string(),
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

    // Insert small batch (should be inlined)
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![10, 20, 30])),
            Arc::new(Int64Array::from(vec![100, 200, 300])),
        ],
    )?;
    common::insert_batch(&table, batch).await?;

    // Query should see inlined data
    let table_arc: Arc<dyn TableProvider> = Arc::new(table);
    ctx.register_table("scan_inline_test", Arc::clone(&table_arc))?;

    let df = ctx
        .sql("SELECT * FROM scan_inline_test ORDER BY id")
        .await?;
    let results = df.collect().await?;
    let total_rows: usize = results.iter().map(RecordBatch::num_rows).sum();

    assert_eq!(total_rows, 3, "Expected 3 rows from inlined data scan");

    // Concatenate all result batches to avoid flaky assertions when the planner
    // splits results across multiple RecordBatches.
    let combined =
        arrow::compute::concat_batches(&results[0].schema(), &results).expect("concat batches");
    let id_col = combined
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("id column");
    assert_eq!(id_col.value(0), 10);
    assert_eq!(id_col.value(1), 20);
    assert_eq!(id_col.value(2), 30);

    Ok(())
}
