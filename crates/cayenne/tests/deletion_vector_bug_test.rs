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

//! Tests for deletion vectors with multiple inserts (appends).
//!
//! This test exposes a bug where deletion vectors don't work correctly
//! when data is inserted in multiple batches (creating multiple files).
//! The issue is that `DeletionFilterStream` uses a per-partition row offset
//! that starts at 0, but deletion vectors store global row IDs.

#![allow(clippy::expect_used)]

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
    let data_dir = TempDir::new()?;
    let metadata_dir = TempDir::new()?;

    let catalog = Arc::new(CayenneCatalog::new(format!(
        "sqlite://{}/test.db",
        metadata_dir.path().display()
    ))?);
    catalog.init().await?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let table_options = CreateTableOptions {
        table_name: "test_table".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["id".to_string()],
        base_path: data_dir.path().to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    let table_provider =
        Arc::new(CayenneTableProvider::create_table(catalog, table_options).await?);

    let ctx = SessionContext::new();
    ctx.register_table(
        "test_table",
        Arc::clone(&table_provider) as Arc<dyn TableProvider>,
    )?;

    Ok((table_provider, ctx, data_dir, metadata_dir))
}

async fn insert_batch(
    table_provider: &Arc<CayenneTableProvider>,
    ids: Vec<i64>,
    names: Vec<&str>,
    values: Vec<i64>,
) -> TestResult<u64> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
            Arc::new(Int64Array::from(values)),
        ],
    )?;

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

/// Tests that deletion vectors work correctly when data is inserted in multiple batches.
///
/// This test verifies that:
/// 1. Data can be inserted in multiple batches (creating multiple files)
/// 2. Deletion by filter correctly identifies and marks rows for deletion
/// 3. Subsequent reads correctly filter out deleted rows regardless of which file they're in
///
/// The deletion vector system uses position-based row IDs, so it's critical that:
/// - The delete operation and read operations see rows in the same order
/// - The `DeletionFilterExec` coalesces partitions to ensure consistent ordering
#[tokio::test]
async fn test_deletion_vectors_with_multiple_inserts() -> TestResult<()> {
    let (table_provider, ctx, _data_dir, _metadata_dir) = setup_test_table().await?;

    // First insert: 5 rows with ids 1-5
    let inserted1 = insert_batch(
        &table_provider,
        vec![1, 2, 3, 4, 5],
        vec!["Alice", "Bob", "Charlie", "David", "Eve"],
        vec![100, 200, 300, 400, 500],
    )
    .await?;
    assert_eq!(inserted1, 5, "First insert should have 5 rows");

    // Second insert: 5 more rows with ids 6-10
    let inserted2 = insert_batch(
        &table_provider,
        vec![6, 7, 8, 9, 10],
        vec!["Frank", "Grace", "Henry", "Ivy", "Jack"],
        vec![600, 700, 800, 900, 1000],
    )
    .await?;
    assert_eq!(inserted2, 5, "Second insert should have 5 rows");

    // Verify we have 10 rows total
    let df = ctx.sql("SELECT COUNT(*) as count FROM test_table").await?;
    let results = df.collect().await?;
    let total_before = results
        .first()
        .and_then(|batch| batch.column(0).as_any().downcast_ref::<Int64Array>())
        .and_then(|array| array.values().first())
        .copied()
        .unwrap_or(0);
    assert_eq!(total_before, 10, "Should have 10 rows before deletion");

    // Delete row with id=7 (from the second insert)
    let filter = col("id").eq(lit(7i64));
    let deleted = delete_records(&table_provider, filter).await?;
    assert_eq!(deleted, 1, "Should delete 1 row (id=7)");

    // Query to verify deletion worked
    let df = ctx.sql("SELECT COUNT(*) as count FROM test_table").await?;
    let results = df.collect().await?;
    let total_after = results
        .first()
        .and_then(|batch| batch.column(0).as_any().downcast_ref::<Int64Array>())
        .and_then(|array| array.values().first())
        .copied()
        .unwrap_or(0);

    // BUG: With multiple inserts, deletion vectors don't work correctly
    // This assertion should pass but may fail if the bug exists
    assert_eq!(
        total_after, 9,
        "Should have 9 rows after deleting id=7 (got {total_after})"
    );

    // Verify the specific row is gone
    let df = ctx.sql("SELECT id FROM test_table WHERE id = 7").await?;
    let results = df.collect().await?;
    let matching_rows: usize = results.iter().map(RecordBatch::num_rows).sum();

    assert_eq!(
        matching_rows, 0,
        "Row with id=7 should not be visible after deletion (got {matching_rows} rows)"
    );

    Ok(())
}

/// Test deletion of rows from the first batch when there are multiple files.
#[tokio::test]
async fn test_deletion_vectors_delete_from_first_batch() -> TestResult<()> {
    let (table_provider, ctx, _data_dir, _metadata_dir) = setup_test_table().await?;

    // First insert: 5 rows with ids 1-5
    insert_batch(
        &table_provider,
        vec![1, 2, 3, 4, 5],
        vec!["Alice", "Bob", "Charlie", "David", "Eve"],
        vec![100, 200, 300, 400, 500],
    )
    .await?;

    // Second insert: 5 more rows with ids 6-10
    insert_batch(
        &table_provider,
        vec![6, 7, 8, 9, 10],
        vec!["Frank", "Grace", "Henry", "Ivy", "Jack"],
        vec![600, 700, 800, 900, 1000],
    )
    .await?;

    // Delete row with id=2 (from the first insert)
    let filter = col("id").eq(lit(2i64));
    let deleted = delete_records(&table_provider, filter).await?;
    assert_eq!(deleted, 1, "Should delete 1 row (id=2)");

    // Query to verify deletion worked
    let df = ctx.sql("SELECT COUNT(*) as count FROM test_table").await?;
    let results = df.collect().await?;
    let total_after = results
        .first()
        .and_then(|batch| batch.column(0).as_any().downcast_ref::<Int64Array>())
        .and_then(|array| array.values().first())
        .copied()
        .unwrap_or(0);

    assert_eq!(
        total_after, 9,
        "Should have 9 rows after deleting id=2 (got {total_after})"
    );

    // Verify the specific row is gone
    let df = ctx.sql("SELECT id FROM test_table WHERE id = 2").await?;
    let results = df.collect().await?;
    let matching_rows: usize = results.iter().map(RecordBatch::num_rows).sum();

    assert_eq!(
        matching_rows, 0,
        "Row with id=2 should not be visible after deletion (got {matching_rows} rows)"
    );

    Ok(())
}

/// Test deletion of multiple rows spanning both batches.
#[tokio::test]
async fn test_deletion_vectors_delete_from_multiple_batches() -> TestResult<()> {
    let (table_provider, ctx, _data_dir, _metadata_dir) = setup_test_table().await?;

    // First insert: 5 rows with ids 1-5
    insert_batch(
        &table_provider,
        vec![1, 2, 3, 4, 5],
        vec!["Alice", "Bob", "Charlie", "David", "Eve"],
        vec![100, 200, 300, 400, 500],
    )
    .await?;

    // Second insert: 5 more rows with ids 6-10
    insert_batch(
        &table_provider,
        vec![6, 7, 8, 9, 10],
        vec!["Frank", "Grace", "Henry", "Ivy", "Jack"],
        vec![600, 700, 800, 900, 1000],
    )
    .await?;

    // Delete rows with id=3 and id=8 (one from each batch)
    let filter = col("id").eq(lit(3i64)).or(col("id").eq(lit(8i64)));
    let deleted = delete_records(&table_provider, filter).await?;
    assert_eq!(deleted, 2, "Should delete 2 rows (id=3 and id=8)");

    // Query to verify deletion worked
    let df = ctx.sql("SELECT COUNT(*) as count FROM test_table").await?;
    let results = df.collect().await?;
    let total_after = results
        .first()
        .and_then(|batch| batch.column(0).as_any().downcast_ref::<Int64Array>())
        .and_then(|array| array.values().first())
        .copied()
        .unwrap_or(0);

    assert_eq!(
        total_after, 8,
        "Should have 8 rows after deleting id=3 and id=8 (got {total_after})"
    );

    // Verify specific rows are gone
    let df = ctx
        .sql("SELECT id FROM test_table WHERE id IN (3, 8)")
        .await?;
    let results = df.collect().await?;
    let matching_rows: usize = results.iter().map(RecordBatch::num_rows).sum();

    assert_eq!(
        matching_rows, 0,
        "Rows with id=3 and id=8 should not be visible after deletion (got {matching_rows} rows)"
    );

    Ok(())
}
