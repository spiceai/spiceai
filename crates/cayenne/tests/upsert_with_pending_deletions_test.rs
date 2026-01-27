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

//! Tests for upsert behavior when there are pending deletions from prior upserts.

#![allow(clippy::expect_used)]

mod common;

use arrow::array::{Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use cayenne::{metadata::CreateTableOptions, CayenneTableProvider, MetadataCatalog};
use common::TestFixture;
use datafusion::datasource::TableProvider;
use datafusion::execution::context::SessionContext;
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};
use std::sync::Arc;

type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

// =============================================================================
// Helper Functions
// =============================================================================

fn create_schema_with_timestamp() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
        Field::new(
            "updated_at",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
    ]))
}

async fn setup_upsert_table(
    fixture: &TestFixture,
    table_name: &str,
) -> TestResult<(Arc<CayenneTableProvider>, SessionContext, Arc<Schema>)> {
    let schema = create_schema_with_timestamp();

    let table_options = CreateTableOptions {
        table_name: table_name.to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["id".to_string()],
        on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
            "id".to_string()
        ]))),
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

async fn get_row_count(ctx: &SessionContext, table_name: &str) -> TestResult<usize> {
    let df = ctx.sql(&format!("SELECT * FROM {table_name}")).await?;
    let results = df.collect().await?;
    Ok(results.iter().map(RecordBatch::num_rows).sum())
}

async fn get_ids(ctx: &SessionContext, table_name: &str) -> TestResult<Vec<i64>> {
    let df = ctx
        .sql(&format!("SELECT id FROM {table_name} ORDER BY id"))
        .await?;
    let results = df.collect().await?;
    let mut ids = Vec::new();
    for batch in &results {
        let id_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id column should be Int64Array");
        for i in 0..id_col.len() {
            ids.push(id_col.value(i));
        }
    }
    Ok(ids)
}

// =============================================================================
// Test: Consecutive upserts should not create duplicate PKs
// =============================================================================
//
// Verifies that multiple consecutive upserts with the same PKs correctly replace
// rows rather than creating duplicates, even when pending deletions exist from
// prior upsert operations.

async fn test_consecutive_upserts_no_duplicates_impl(fixture: TestFixture) -> TestResult<()> {
    let (table, ctx, schema) = setup_upsert_table(&fixture, "consecutive_upsert").await?;

    // First insert: ids 1, 2, 3 with January timestamps
    let batch1 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["alpha", "beta", "gamma"])),
            Arc::new(Int64Array::from(vec![100, 200, 300])),
            Arc::new(TimestampMicrosecondArray::from(vec![
                1_706_000_000_000_000_i64, // Jan 2024
                1_706_000_000_000_000_i64,
                1_706_000_000_000_000_i64,
            ])),
        ],
    )?;
    insert_batch(&table, batch1).await?;

    // Verify: 3 rows
    assert_eq!(
        get_row_count(&ctx, "consecutive_upsert").await?,
        3,
        "Initial insert should have 3 rows"
    );

    // Second insert: same ids with February timestamps (UPSERT)
    // This creates pending deletions for ids 1, 2, 3
    let batch2 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["alpha", "beta", "gamma"])),
            Arc::new(Int64Array::from(vec![100, 200, 300])),
            Arc::new(TimestampMicrosecondArray::from(vec![
                1_709_000_000_000_000_i64, // Feb 2024
                1_709_000_000_000_000_i64,
                1_709_000_000_000_000_i64,
            ])),
        ],
    )?;
    insert_batch(&table, batch2).await?;

    // Verify: still 3 rows (upsert replaced, not duplicated)
    assert_eq!(
        get_row_count(&ctx, "consecutive_upsert").await?,
        3,
        "After first upsert should have 3 rows (not 6)"
    );

    // Third insert: same ids AGAIN with March timestamps (UPSERT when pending deletions exist)
    // This exercises the path where pending deletions exist from the prior upsert,
    // and the incoming data also needs upsert validation.
    let batch3 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["alpha", "beta", "gamma"])),
            Arc::new(Int64Array::from(vec![100, 200, 300])),
            Arc::new(TimestampMicrosecondArray::from(vec![
                1_711_000_000_000_000_i64, // Mar 2024
                1_711_000_000_000_000_i64,
                1_711_000_000_000_000_i64,
            ])),
        ],
    )?;
    insert_batch(&table, batch3).await?;

    // KEY ASSERTION: Upsert with pending deletions should correctly replace rows
    assert_eq!(
        get_row_count(&ctx, "consecutive_upsert").await?,
        3,
        "After upsert with pending deletions should still have 3 rows (no duplicates)"
    );

    // Verify unique IDs
    let ids = get_ids(&ctx, "consecutive_upsert").await?;
    assert_eq!(ids, vec![1, 2, 3], "Should have exactly ids 1, 2, 3");

    Ok(())
}

test_with_backends!(test_consecutive_upserts_no_duplicates_impl);

// =============================================================================
// Test: Upsert with mix of new and existing PKs when pending deletions exist
// =============================================================================

async fn test_upsert_mixed_pks_with_pending_deletions_impl(fixture: TestFixture) -> TestResult<()> {
    let (table, ctx, schema) = setup_upsert_table(&fixture, "mixed_upsert").await?;

    // First insert: ids 1, 2, 3
    let batch1 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
            Arc::new(Int64Array::from(vec![100, 200, 300])),
            Arc::new(TimestampMicrosecondArray::from(vec![
                1_706_000_000_000_000_i64,
                1_706_000_000_000_000_i64,
                1_706_000_000_000_000_i64,
            ])),
        ],
    )?;
    insert_batch(&table, batch1).await?;
    assert_eq!(get_row_count(&ctx, "mixed_upsert").await?, 3);

    // Second insert: id 1 (upsert) + id 4 (new) - creates pending deletions for id 1
    let batch2 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 4])),
            Arc::new(StringArray::from(vec!["a_updated", "d"])),
            Arc::new(Int64Array::from(vec![150, 400])),
            Arc::new(TimestampMicrosecondArray::from(vec![
                1_709_000_000_000_000_i64,
                1_709_000_000_000_000_i64,
            ])),
        ],
    )?;
    insert_batch(&table, batch2).await?;
    assert_eq!(
        get_row_count(&ctx, "mixed_upsert").await?,
        4,
        "After upsert id=1 and new id=4, should have 4 rows"
    );

    // Third insert: id 2 (upsert) + id 5 (new) - pending deletions exist from id 1
    // This triggers the has_pending_deletions path
    let batch3 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![2, 5])),
            Arc::new(StringArray::from(vec!["b_updated", "e"])),
            Arc::new(Int64Array::from(vec![250, 500])),
            Arc::new(TimestampMicrosecondArray::from(vec![
                1_711_000_000_000_000_i64,
                1_711_000_000_000_000_i64,
            ])),
        ],
    )?;
    insert_batch(&table, batch3).await?;

    // Should have 5 unique rows: ids 1, 2, 3, 4, 5
    assert_eq!(
        get_row_count(&ctx, "mixed_upsert").await?,
        5,
        "After mixed upsert with pending deletions should have 5 rows"
    );

    let ids = get_ids(&ctx, "mixed_upsert").await?;
    assert_eq!(ids, vec![1, 2, 3, 4, 5], "Should have unique ids 1-5");

    // Verify the updated values
    let df = ctx
        .sql("SELECT id, value FROM mixed_upsert ORDER BY id")
        .await?;
    let results = df.collect().await?;
    let batch = &results[0];
    let values = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("value column");

    // id 1 -> 150 (updated), id 2 -> 250 (updated), id 3 -> 300 (original),
    // id 4 -> 400 (new), id 5 -> 500 (new)
    assert_eq!(values.value(0), 150, "id=1 should have updated value 150");
    assert_eq!(values.value(1), 250, "id=2 should have updated value 250");
    assert_eq!(values.value(2), 300, "id=3 should have original value 300");
    assert_eq!(values.value(3), 400, "id=4 should have new value 400");
    assert_eq!(values.value(4), 500, "id=5 should have new value 500");

    Ok(())
}

test_with_backends!(test_upsert_mixed_pks_with_pending_deletions_impl);

// =============================================================================
// Test: Multiple consecutive upsert cycles
// =============================================================================

async fn test_many_consecutive_upsert_cycles_impl(fixture: TestFixture) -> TestResult<()> {
    let (table, ctx, schema) = setup_upsert_table(&fixture, "many_cycles").await?;

    // Initial insert
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["a", "b"])),
            Arc::new(Int64Array::from(vec![100, 200])),
            Arc::new(TimestampMicrosecondArray::from(vec![
                1_700_000_000_000_000_i64,
                1_700_000_000_000_000_i64,
            ])),
        ],
    )?;
    insert_batch(&table, batch).await?;
    assert_eq!(get_row_count(&ctx, "many_cycles").await?, 2);

    // Run 5 upsert cycles - each should maintain exactly 2 rows
    for cycle in 1..=5 {
        let ts = 1_700_000_000_000_000_i64 + (cycle * 1_000_000_000_000_i64);
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![
                    format!("a_v{cycle}"),
                    format!("b_v{cycle}"),
                ])),
                Arc::new(Int64Array::from(vec![100 + cycle, 200 + cycle])),
                Arc::new(TimestampMicrosecondArray::from(vec![ts, ts])),
            ],
        )?;
        insert_batch(&table, batch).await?;

        let count = get_row_count(&ctx, "many_cycles").await?;
        assert_eq!(
            count, 2,
            "After upsert cycle {cycle}, should still have 2 rows, got {count}"
        );
    }

    // Final verification
    let ids = get_ids(&ctx, "many_cycles").await?;
    assert_eq!(ids, vec![1, 2]);

    // Verify final values (cycle 5: 105, 205)
    let df = ctx.sql("SELECT value FROM many_cycles ORDER BY id").await?;
    let results = df.collect().await?;
    let values = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("value column");
    assert_eq!(
        values.value(0),
        105,
        "id=1 should have value 105 after cycle 5"
    );
    assert_eq!(
        values.value(1),
        205,
        "id=2 should have value 205 after cycle 5"
    );

    Ok(())
}

test_with_backends!(test_many_consecutive_upsert_cycles_impl);
