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

//! Tests for scanning protected snapshots with column projections.
//!
//! Validates that queries with column projections work correctly when protected
//! snapshots exist and newer deletions need to be applied to them.
//!
//! The key scenario is when a projection reorders columns differently from the
//! original schema. For example, with schema (id, name, value) and a projection
//! SELECT value, id, the batch has value at index 0 and id at index 1. The
//! deletion filter must use the adjusted PK indices in the projection, not the
//! original schema indices.

#![allow(clippy::expect_used)]

mod common;

use arrow::array::{Array, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use cayenne::{metadata::CreateTableOptions, CayenneTableProvider, MetadataCatalog};
use common::TestFixture;
use data_components::delete::DeletionTableProvider;
use datafusion::datasource::TableProvider;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::*;
use std::sync::Arc;

type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

/// Create a schema with Int32 PK to trigger RowConverter-based deletion strategy.
/// Schema: id (Int32 PK), name (Utf8), value (Int64)
fn create_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

async fn setup_table(
    fixture: &TestFixture,
    table_name: &str,
) -> TestResult<(Arc<CayenneTableProvider>, SessionContext, Arc<Schema>)> {
    let schema = create_schema();

    let table_options = CreateTableOptions {
        table_name: table_name.to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["id".to_string()],
        on_conflict: None,
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let table = Arc::new(CayenneTableProvider::create_table(catalog, table_options).await?);
    let ctx = SessionContext::new();
    ctx.register_table(table_name, Arc::clone(&table) as Arc<dyn TableProvider>)?;

    Ok((table, ctx, schema))
}

async fn insert_batch(table: &Arc<CayenneTableProvider>, batch: RecordBatch) -> TestResult<u64> {
    common::insert_batch(table.as_ref(), batch)
        .await
        .map_err(Into::into)
}

async fn delete_records(table: &Arc<CayenneTableProvider>, filter: Expr) -> TestResult<u64> {
    let ctx = SessionContext::new();
    let plan = table.delete_from(&ctx.state(), &[filter]).await?;
    let results = datafusion_physical_plan::collect(plan, ctx.task_ctx()).await?;
    Ok(results
        .first()
        .and_then(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<arrow::array::UInt64Array>()
        })
        .and_then(|a| a.values().first())
        .copied()
        .unwrap_or(0))
}

// =============================================================================
// Test: Protected snapshot scan with projection that excludes PK column
// =============================================================================
//
// Validates that queries with column projections work correctly when protected
// snapshots exist and newer deletions need to be applied to them.
async fn test_protected_snapshot_projection_reorder_impl(fixture: TestFixture) -> TestResult<()> {
    let (table, ctx, schema) = setup_table(&fixture, "projection_reorder").await?;

    // Step 1: Insert initial data (ids 1, 2, 3)
    let batch1 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["alpha", "bravo", "charlie"])),
            Arc::new(Int64Array::from(vec![100, 200, 300])),
        ],
    )?;
    insert_batch(&table, batch1).await?;

    // Step 2: Delete id=2 to create pending deletions (deletion seq=1)
    let deleted = delete_records(&table, col("id").eq(lit(2_i32))).await?;
    assert_eq!(deleted, 1, "Should delete 1 row");

    // Step 3: Insert new data (ids 4, 5) - creates protected snapshot with max_delete_seq=1
    let batch2 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![4, 5])),
            Arc::new(StringArray::from(vec!["delta", "echo"])),
            Arc::new(Int64Array::from(vec![400, 500])),
        ],
    )?;
    insert_batch(&table, batch2).await?;

    // Step 4: Delete id=4 AFTER the protected snapshot was created (deletion seq=2)
    // This deletion has seq > max_delete_seq_at_creation, so it WILL be applied
    // to the protected snapshot during scan. This exercises the partial deletion
    // filter code path with column projection.
    let deleted2 = delete_records(&table, col("id").eq(lit(4_i32))).await?;
    assert_eq!(deleted2, 1, "Should delete 1 row");

    // Step 5: Query with projection that EXCLUDES the PK column (id)
    // The system must add id internally for deletion filtering, then strip it
    let df = ctx
        .sql("SELECT name, value FROM projection_reorder ORDER BY value")
        .await?;

    let results = df.collect().await?;

    // Verify we got the correct results
    // Expected: rows for ids 1, 3, 5 (id=2 deleted before snapshot, id=4 deleted after snapshot)
    let total_rows: usize = results.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(
        total_rows, 3,
        "Should have 3 rows (for ids 1, 3, 5 - not id 2 or 4 which were deleted)"
    );

    // Verify the name column values are correct (name is at index 0 in projection)
    let mut names = Vec::new();
    for batch in &results {
        let name_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("name column should be StringArray");
        for i in 0..name_col.len() {
            names.push(name_col.value(i).to_string());
        }
    }
    // Ordered by value: 100 (alpha), 300 (charlie), 500 (echo)
    assert_eq!(
        names,
        vec!["alpha", "charlie", "echo"],
        "Should have names alpha, charlie, echo in order by value"
    );

    Ok(())
}

test_with_backends!(test_protected_snapshot_projection_reorder_impl);
