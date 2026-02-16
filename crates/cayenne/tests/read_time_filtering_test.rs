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
//! These tests verify that deletion vector filtering is applied during reads.

mod common;

use arrow::array::{Int64Array, RecordBatch, StringArray};

use arrow::datatypes::{DataType, Field, Schema};

use cayenne::{
    metadata::CreateTableOptions, CayenneCatalog, CayenneTableProvider, MetadataCatalog,
};

use data_components::delete::DeletionTableProvider;

use datafusion::datasource::TableProvider;

use datafusion::execution::context::SessionContext;

use datafusion::prelude::*;

use std::sync::Arc;

use tempfile::TempDir;

type TestResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

async fn setup_test_table(
) -> TestResult<(Arc<CayenneTableProvider>, SessionContext, TempDir, TempDir)> {
    // Create temporary directories for data and metadata
    let data_dir = TempDir::new()?;
    let metadata_dir = TempDir::new()?;

    // Create catalog
    let catalog = Arc::new(CayenneCatalog::new(format!(
        "sqlite://{}/test.db",
        metadata_dir.path().display()
    ))?);
    catalog.init().await?;

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
        on_conflict: None,
        base_path: data_dir.path().to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    // Create table provider
    let table_provider =
        Arc::new(CayenneTableProvider::create_table(catalog, table_options).await?);

    // Create session context for queries
    let ctx = SessionContext::new();
    ctx.register_table(
        "test_table",
        Arc::clone(&table_provider) as Arc<dyn TableProvider>,
    )?;

    Ok((table_provider, ctx, data_dir, metadata_dir))
}

async fn insert_test_data(table_provider: &Arc<CayenneTableProvider>) -> TestResult<u64> {
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
    )?;

    common::insert_batch(table_provider.as_ref(), batch)
        .await
        .map_err(Into::into)
}

async fn delete_records(
    table_provider: &Arc<CayenneTableProvider>,
    filter: Expr,
) -> TestResult<u64> {
    let ctx = SessionContext::new();
    let plan = table_provider.delete_from(&ctx.state(), &[filter]).await?;

    let results = datafusion_physical_plan::collect(plan, ctx.task_ctx()).await?;

    Ok(results
        .first()
        .and_then(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::UInt64Array>()
        })
        .and_then(|array| array.values().first())
        .copied()
        .unwrap_or(0))
}

#[tokio::test]
async fn test_scan_filters_deleted_rows_via_count() -> TestResult<()> {
    let (table_provider, ctx, _data_dir, _metadata_dir) = setup_test_table().await?;

    // Insert data
    let inserted = insert_test_data(&table_provider).await?;
    assert_eq!(inserted, 5, "Should insert 5 rows");

    // Delete some records
    let filter = col("id").lt_eq(lit(2i64));
    let deleted = delete_records(&table_provider, filter).await?;
    assert_eq!(deleted, 2, "Should delete 2 rows (id 1 and 2)");

    // Query the table - deletion vectors should be applied
    let df = ctx.sql("SELECT COUNT(*) as count FROM test_table").await?;

    let results = df.collect().await?;
    let count = results
        .first()
        .and_then(|batch| batch.column(0).as_any().downcast_ref::<Int64Array>())
        .and_then(|array| array.values().first())
        .copied()
        .unwrap_or(0);

    // Should see only 3 rows because read-time filtering removes deleted rows
    assert_eq!(
        count, 3,
        "With deletion vectors applied, only non-deleted rows should be visible"
    );

    Ok(())
}

#[tokio::test]
async fn test_scan_filters_deleted_rows() -> TestResult<()> {
    let (table_provider, ctx, _data_dir, _metadata_dir) = setup_test_table().await?;

    // Insert data
    let inserted = insert_test_data(&table_provider).await?;
    assert_eq!(inserted, 5, "Should insert 5 rows");

    // Delete some records
    let filter = col("id").lt_eq(lit(2i64));
    let deleted = delete_records(&table_provider, filter).await?;
    assert_eq!(deleted, 2, "Should delete 2 rows (id 1 and 2)");

    // Query the table - deletion vectors SHOULD be checked and applied
    let df = ctx.sql("SELECT * FROM test_table").await?;

    let results = df.collect().await?;

    // Count total rows across all batches
    let total_rows: usize = results
        .iter()
        .map(arrow::array::RecordBatch::num_rows)
        .sum();

    // With deletion vectors applied, we should see 3 rows (rows with id <= 2 are deleted)
    assert_eq!(
        total_rows, 3,
        "Deletion vectors should filter out deleted rows (expected 3, got {total_rows})"
    );

    Ok(())
}

#[tokio::test]
async fn test_get_table_delete_files_works() -> TestResult<()> {
    let (table_provider, _ctx, _data_dir, _metadata_dir) = setup_test_table().await?;

    // Insert data
    let inserted = insert_test_data(&table_provider).await?;
    assert_eq!(inserted, 5, "Should insert 5 rows");

    // Verify no deletion files initially
    let delete_files = table_provider
        .catalog()
        .get_table_delete_files(table_provider.metadata().table_id)
        .await?;
    assert_eq!(
        delete_files.len(),
        0,
        "Should have no delete files initially"
    );

    // Delete some records
    let filter = col("id").lt_eq(lit(2i64));
    let deleted = delete_records(&table_provider, filter).await?;
    assert_eq!(deleted, 2, "Should delete 2 rows");

    // Verify deletion file was registered
    let delete_files = table_provider
        .catalog()
        .get_table_delete_files(table_provider.metadata().table_id)
        .await?;
    assert_eq!(
        delete_files.len(),
        1,
        "Should have 1 delete file after deletion"
    );
    assert_eq!(
        delete_files[0].delete_count, 2,
        "Delete file should track 2 deleted rows"
    );

    Ok(())
}
