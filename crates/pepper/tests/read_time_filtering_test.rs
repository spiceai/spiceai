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

//! Tests for read-time filtering based on retention configuration.
//!
//! These tests verify that deletion vector filtering is only applied
//! when retention_sql is configured, avoiding performance overhead
//! for full/append refreshes where data is known to be complete.

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use data_components::delete::DeletionTableProvider;
use datafusion::datasource::TableProvider;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::*;
use pepper::{metadata::CreateTableOptions, MetadataCatalog, PepperCatalog, PepperTableProvider};
use std::sync::Arc;
use tempfile::TempDir;

async fn setup_test_table(
    retention_enabled: bool,
) -> (Arc<PepperTableProvider>, SessionContext, TempDir, TempDir) {
    // Create temporary directories for data and metadata
    let data_dir = TempDir::new().expect("Failed to create temp data directory");
    let metadata_dir = TempDir::new().expect("Failed to create temp metadata directory");

    // Create catalog
    let catalog = Arc::new(PepperCatalog::new(format!(
        "sqlite://{}/test.db",
        metadata_dir.path().display()
    )));
    catalog.init().await.expect("Failed to initialize catalog");

    // Create schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));

    // Create table options
    let table_options = CreateTableOptions {
        table_name: "test_table".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["id".to_string()],
        base_path: data_dir.path().to_string_lossy().to_string(),
    };

    // Create table provider
    let mut table_provider = PepperTableProvider::create_table(catalog, table_options)
        .await
        .expect("Failed to create table");

    // Configure retention filtering
    if retention_enabled {
        table_provider = table_provider.with_retention_enabled(true);
    }

    let table_provider = Arc::new(table_provider);

    // Create session context for queries
    let ctx = SessionContext::new();
    ctx.register_table(
        "test_table",
        Arc::clone(&table_provider) as Arc<dyn TableProvider>,
    )
    .expect("Failed to register table");

    (table_provider, ctx, data_dir, metadata_dir)
}

async fn insert_test_data(table_provider: &Arc<PepperTableProvider>) -> u64 {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec![
                "Alice", "Bob", "Charlie", "David", "Eve",
            ])),
            Arc::new(Int64Array::from(vec![100, 200, 300, 400, 500])),
        ],
    )
    .expect("Failed to create record batch");

    let stream = futures::stream::once(async { Ok(batch) });
    let boxed_stream: datafusion_execution::SendableRecordBatchStream = Box::pin(
        datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            stream,
        ),
    );

    table_provider
        .insert(boxed_stream)
        .await
        .expect("Failed to insert data")
}

async fn delete_records(table_provider: &Arc<PepperTableProvider>, filter: Expr) -> u64 {
    let ctx = SessionContext::new();
    let plan = table_provider
        .delete_from(&ctx.state(), &[filter])
        .await
        .expect("Failed to create delete plan");

    let results = datafusion_physical_plan::collect(plan, ctx.task_ctx())
        .await
        .expect("Failed to execute delete");

    results
        .first()
        .and_then(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::UInt64Array>()
        })
        .and_then(|array| array.values().first())
        .copied()
        .unwrap_or(0)
}

#[tokio::test]
async fn test_scan_without_retention_does_not_check_deletion_vectors() {
    // Setup table WITHOUT retention enabled
    let (table_provider, ctx, _data_dir, _metadata_dir) = setup_test_table(false).await;

    // Insert data
    let inserted = insert_test_data(&table_provider).await;
    assert_eq!(inserted, 5, "Should insert 5 rows");

    // Delete some records
    let filter = col("id").lt_eq(lit(2i64));
    let deleted = delete_records(&table_provider, filter).await;
    assert_eq!(deleted, 2, "Should delete 2 rows (id 1 and 2)");

    // Query the table - deletion vectors should NOT be applied
    // since retention is disabled (performance optimization)
    let df = ctx
        .sql("SELECT COUNT(*) as count FROM test_table")
        .await
        .expect("Failed to query");

    let results = df.collect().await.expect("Failed to collect results");
    let count = results
        .first()
        .and_then(|batch| batch.column(0).as_any().downcast_ref::<Int64Array>())
        .and_then(|array| array.values().first())
        .copied()
        .unwrap_or(0);

    // Should still see all 5 rows because read-time filtering is disabled
    assert_eq!(
        count, 5,
        "Without retention enabled, deleted rows should still be visible"
    );
}

#[tokio::test]
async fn test_scan_with_retention_checks_deletion_vectors() {
    // Setup table WITH retention enabled
    let (table_provider, ctx, _data_dir, _metadata_dir) = setup_test_table(true).await;

    // Insert data
    let inserted = insert_test_data(&table_provider).await;
    assert_eq!(inserted, 5, "Should insert 5 rows");

    // Delete some records
    let filter = col("id").lt_eq(lit(2i64));
    let deleted = delete_records(&table_provider, filter).await;
    assert_eq!(deleted, 2, "Should delete 2 rows (id 1 and 2)");

    // Query the table - deletion vectors SHOULD be checked and applied
    let df = ctx
        .sql("SELECT * FROM test_table")
        .await
        .expect("Failed to query");

    let results = df.collect().await.expect("Failed to collect results");

    // Count total rows across all batches
    let total_rows: usize = results.iter().map(|batch| batch.num_rows()).sum();

    // With retention enabled and deletion vectors applied, we should see 3 rows
    // (we deleted rows with id <= 2, leaving rows 3, 4, 5)
    assert_eq!(
        total_rows, 3,
        "With retention enabled, deleted rows should be filtered out (expected 3, got {})",
        total_rows
    );
}

#[tokio::test]
async fn test_retention_flag_can_be_toggled() {
    // Create table without retention
    let data_dir = TempDir::new().expect("Failed to create temp data directory");
    let metadata_dir = TempDir::new().expect("Failed to create temp metadata directory");

    let catalog = Arc::new(PepperCatalog::new(format!(
        "sqlite://{}/test.db",
        metadata_dir.path().display()
    )));
    catalog.init().await.expect("Failed to initialize catalog");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let table_options = CreateTableOptions {
        table_name: "test_table".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec![],
        base_path: data_dir.path().to_string_lossy().to_string(),
    };

    let table_provider = PepperTableProvider::create_table(catalog, table_options)
        .await
        .expect("Failed to create table");

    // Initially false
    assert!(
        !table_provider.is_retention_enabled(),
        "Retention should be disabled by default"
    );

    // Enable retention
    let table_provider = table_provider.with_retention_enabled(true);
    assert!(
        table_provider.is_retention_enabled(),
        "Retention should be enabled after calling with_retention_enabled(true)"
    );

    // Disable retention
    let table_provider = table_provider.with_retention_enabled(false);
    assert!(
        !table_provider.is_retention_enabled(),
        "Retention should be disabled after calling with_retention_enabled(false)"
    );
}

#[tokio::test]
async fn test_get_table_delete_files_works() {
    // Setup table with retention enabled
    let (table_provider, _ctx, _data_dir, _metadata_dir) = setup_test_table(true).await;

    // Insert data
    let inserted = insert_test_data(&table_provider).await;
    assert_eq!(inserted, 5, "Should insert 5 rows");

    // Verify no deletion files initially
    let delete_files = table_provider
        .catalog()
        .get_table_delete_files(table_provider.metadata().table_id)
        .await
        .expect("Failed to get delete files");
    assert_eq!(
        delete_files.len(),
        0,
        "Should have no delete files initially"
    );

    // Delete some records
    let filter = col("id").lt_eq(lit(2i64));
    let deleted = delete_records(&table_provider, filter).await;
    assert_eq!(deleted, 2, "Should delete 2 rows");

    // Verify deletion file was registered
    let delete_files = table_provider
        .catalog()
        .get_table_delete_files(table_provider.metadata().table_id)
        .await
        .expect("Failed to get delete files");
    assert_eq!(
        delete_files.len(),
        1,
        "Should have 1 delete file after deletion"
    );
    assert_eq!(
        delete_files[0].delete_count, 2,
        "Delete file should track 2 deleted rows"
    );
}
