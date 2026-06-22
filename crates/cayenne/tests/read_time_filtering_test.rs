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
#![expect(clippy::expect_used, reason = "integration-test helpers")]

//! Tests for read-time filtering based on retention configuration.
//!
//! These tests verify that deletion vector filtering is applied during reads.

mod common;

use arrow::array::{Int64Array, RecordBatch, StringArray};

use arrow::datatypes::{DataType, Field, Schema};

use cayenne::{
    CayenneCatalog, CayenneTableProvider, MetadataCatalog, metadata::CreateTableOptions,
};

use datafusion::datasource::TableProvider;

use datafusion::execution::context::SessionContext;

use datafusion::prelude::*;

use std::sync::Arc;

use tempfile::TempDir;

type TestResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

async fn setup_test_table()
-> TestResult<(Arc<CayenneTableProvider>, SessionContext, TempDir, TempDir)> {
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

    // Create session context and table provider
    let ctx = SessionContext::new();
    let table_provider = Arc::new(
        CayenneTableProvider::create_table(catalog, table_options, ctx.runtime_env()).await?,
    );
    ctx.register_table(
        "test_table",
        Arc::clone(&table_provider) as Arc<dyn TableProvider>,
    )?;

    Ok((table_provider, ctx, data_dir, metadata_dir))
}

/// Insert enough rows to exceed the inline threshold (1024) so the write goes
/// through the file-backed path, keeping these tests exercising delete-file
/// mechanics rather than inline mutations.
const TEST_ROW_COUNT: usize = 1025;

async fn insert_test_data(table_provider: &Arc<CayenneTableProvider>) -> TestResult<u64> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let ids: Vec<i64> = (1..=i64::try_from(TEST_ROW_COUNT).expect("fits")).collect();
    let names: Vec<String> = (0..TEST_ROW_COUNT).map(|i| format!("user{i}")).collect();
    let values: Vec<i64> = (0..TEST_ROW_COUNT)
        .map(|i| i64::try_from((i + 1) * 100).expect("fits"))
        .collect();

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
            Arc::new(Int64Array::from(values)),
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
    let plan = table_provider
        .delete_from(&ctx.state(), vec![filter])
        .await?;

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
    let row_count = u64::try_from(TEST_ROW_COUNT).expect("fits");
    assert_eq!(inserted, row_count, "Should insert TEST_ROW_COUNT rows");

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

    let expected = i64::try_from(TEST_ROW_COUNT - 2).expect("fits");
    assert_eq!(
        count, expected,
        "With deletion vectors applied, only non-deleted rows should be visible"
    );

    Ok(())
}

#[tokio::test]
async fn test_scan_filters_deleted_rows() -> TestResult<()> {
    let (table_provider, ctx, _data_dir, _metadata_dir) = setup_test_table().await?;

    // Insert data
    let inserted = insert_test_data(&table_provider).await?;
    let row_count = u64::try_from(TEST_ROW_COUNT).expect("fits");
    assert_eq!(inserted, row_count, "Should insert TEST_ROW_COUNT rows");

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

    let expected = TEST_ROW_COUNT - 2;
    // With deletion vectors applied, deleted rows (id <= 2) should be filtered out
    assert_eq!(
        total_rows, expected,
        "Deletion vectors should filter out deleted rows (expected {expected}, got {total_rows})"
    );

    Ok(())
}

#[tokio::test]
async fn test_get_table_delete_files_works() -> TestResult<()> {
    let (table_provider, _ctx, _data_dir, _metadata_dir) = setup_test_table().await?;

    // Insert data
    let row_count = u64::try_from(TEST_ROW_COUNT).expect("fits");
    let inserted = insert_test_data(&table_provider).await?;
    assert_eq!(inserted, row_count, "Should insert TEST_ROW_COUNT rows");

    // Verify no deletion files initially
    let delete_files = table_provider
        .catalog()
        .get_table_delete_files(&table_provider.metadata().table_id)
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
        .get_table_delete_files(&table_provider.metadata().table_id)
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
