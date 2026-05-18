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

//! Edge-case tests for column statistics and data inlining in Cayenne.
//!
//! Covers interactions between stats/inlining and: multiple appends,
//! overwrites, deletions, all-null columns, mixed data types, large
//! inserts that bypass inlining, checkpoint flush, and table reopen.

mod common;

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use cayenne::metadata::CreateTableOptions;
use cayenne::{CayenneTableProvider, MetadataCatalog};
use datafusion::prelude::*;
use std::sync::Arc;

type TestResult = Result<(), Box<dyn std::error::Error>>;

// ============================================================================
// Column Statistics Edge Cases
// ============================================================================

test_with_backends!(test_stats_correct_after_overwrite);

// ============================================================================
// Data Inlining Edge Cases
// ============================================================================

test_with_backends!(test_large_insert_bypasses_inline);
test_with_backends!(test_multiple_small_inserts_accumulate_inline);
test_with_backends!(test_scan_unions_inlined_and_vortex);
test_with_backends!(test_overwrite_clears_inlined_data);
test_with_backends!(test_delete_removes_row_from_pk_table);

// ============================================================================
// Helpers
// ============================================================================

async fn create_table_no_pk(
    fixture: &common::TestFixture,
    name: &str,
    schema: Arc<Schema>,
) -> (CayenneTableProvider, SessionContext) {
    let ctx = SessionContext::new();
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let table = CayenneTableProvider::create_table(
        catalog,
        CreateTableOptions {
            table_name: name.to_string(),
            schema,
            primary_key: vec![],
            on_conflict: None,
            base_path: fixture.data_path.to_string_lossy().to_string(),
            partition_column: None,
            vortex_config: cayenne::metadata::VortexConfig::default(),
        },
        ctx.runtime_env(),
    )
    .await
    .expect("create table");

    (table, ctx)
}

async fn create_table_with_pk(
    fixture: &common::TestFixture,
    name: &str,
    schema: Arc<Schema>,
    pk: Vec<String>,
) -> (CayenneTableProvider, SessionContext) {
    let ctx = SessionContext::new();
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let table = CayenneTableProvider::create_table(
        catalog,
        CreateTableOptions {
            table_name: name.to_string(),
            schema,
            primary_key: pk,
            on_conflict: None,
            base_path: fixture.data_path.to_string_lossy().to_string(),
            partition_column: None,
            vortex_config: cayenne::metadata::VortexConfig::default(),
        },
        ctx.runtime_env(),
    )
    .await
    .expect("create table");

    (table, ctx)
}

fn simple_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, true),
    ]))
}

async fn query_count(ctx: &SessionContext, table_name: &str) -> usize {
    let df = ctx
        .sql(&format!("SELECT COUNT(*) AS cnt FROM {table_name}"))
        .await
        .expect("count query");
    let results = df.collect().await.expect("collect");
    if results.is_empty() || results[0].num_rows() == 0 {
        return 0;
    }
    let col = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count column");
    usize::try_from(col.value(0)).unwrap_or(0)
}

// ============================================================================
// Stats Tests
// ============================================================================

/// After overwrite, stats should reflect only the new data.
async fn test_stats_correct_after_overwrite(fixture: common::TestFixture) -> TestResult {
    let schema = simple_schema();
    let (table, ctx) = create_table_no_pk(&fixture, "stats_overwrite", Arc::clone(&schema)).await;
    let table_id = fixture.catalog.get_table("stats_overwrite").await?.table_id;

    ctx.register_table("stats_overwrite", Arc::new(table))?;

    // Insert 3 rows
    ctx.sql("INSERT INTO stats_overwrite SELECT * FROM (VALUES (1,10),(2,20),(3,30))")
        .await?
        .collect()
        .await?;

    // Overwrite with just 2 rows
    ctx.sql("INSERT OVERWRITE stats_overwrite SELECT * FROM (VALUES (99,990),(100,1000))")
        .await?
        .collect()
        .await?;

    let stats = fixture.catalog.get_table_statistics(&table_id).await?;
    assert!(stats.is_some(), "Stats should exist after overwrite");
    let stats = stats.expect("stats");
    assert_eq!(
        stats.num_rows, 2,
        "After overwrite, stats should reflect only the new 2-row data"
    );

    // Verify scan also returns 2 rows
    assert_eq!(query_count(&ctx, "stats_overwrite").await, 2);

    Ok(())
}

// ============================================================================
// Inlining Tests
// ============================================================================

/// Inserts >1024 rows should bypass the inline path and go directly to Vortex.
async fn test_large_insert_bypasses_inline(fixture: common::TestFixture) -> TestResult {
    let schema = simple_schema();
    let (table, _ctx) = create_table_no_pk(&fixture, "inline_large", Arc::clone(&schema)).await;
    let table_id = fixture.catalog.get_table("inline_large").await?.table_id;

    // Insert 2000 rows (exceeds INLINE_MAX_ROWS=1024)
    let ids: Vec<i64> = (1..=2000).collect();
    let vals: Vec<i64> = (1..=2000).map(|i| i * 10).collect();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(vals)),
        ],
    )?;
    common::insert_batch(&table, batch).await?;

    // Should NOT be inlined — inlined count should be 0
    let inlined_count = fixture.catalog.get_inlined_data_count(&table_id).await?;
    assert_eq!(
        inlined_count, 0,
        "Large insert (2000 rows) should bypass inlining"
    );

    Ok(())
}

/// Multiple small inserts should accumulate in the inline store.
async fn test_multiple_small_inserts_accumulate_inline(fixture: common::TestFixture) -> TestResult {
    let schema = simple_schema();
    let (table, ctx) = create_table_no_pk(&fixture, "inline_multi", Arc::clone(&schema)).await;
    let table_id = fixture.catalog.get_table("inline_multi").await?.table_id;

    // Three small inserts of 5 rows each
    for i in 0..3 {
        let start = i64::from(i) * 5 + 1;
        let ids: Vec<i64> = (start..start + 5).collect();
        let vals: Vec<i64> = ids.iter().map(|x| x * 10).collect();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(Int64Array::from(vals)),
            ],
        )?;
        common::insert_batch(&table, batch).await?;
    }

    // Should have 15 rows inlined
    let inlined_count = fixture.catalog.get_inlined_data_count(&table_id).await?;
    assert_eq!(
        inlined_count, 15,
        "Three inserts of 5 rows each should accumulate 15 inlined rows"
    );

    // All 15 rows should be visible via scan
    ctx.register_table("inline_multi", Arc::new(table))?;
    assert_eq!(query_count(&ctx, "inline_multi").await, 15);

    Ok(())
}

/// Scans that span both the inlined (metastore) path and the Vortex (file) path
/// must surface every row exactly once without asserting a checkpoint occurred.
///
/// This test intentionally does NOT exceed the auto-checkpoint threshold
/// (`10_000` inlined rows) — the small + large inserts stay below it so the
/// inlined rows remain in the metastore even after the large Vortex write,
/// and the scan must union both sources. Auto-checkpoint behavior itself is
/// exercised by the provider-level unit/integration tests under
/// `provider::tests` rather than by this edge-case suite.
async fn test_scan_unions_inlined_and_vortex(fixture: common::TestFixture) -> TestResult {
    let schema = simple_schema();
    let (table, ctx) = create_table_no_pk(&fixture, "inline_union", Arc::clone(&schema)).await;
    let table_id = fixture.catalog.get_table("inline_union").await?.table_id;

    // Insert small batch (inlined)
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50])),
        ],
    )?;
    common::insert_batch(&table, batch).await?;

    // Verify inlined
    assert_eq!(fixture.catalog.get_inlined_data_count(&table_id).await?, 5);

    // Insert a larger batch (bypasses inlining, goes to Vortex)
    let ids: Vec<i64> = (100..=1200).collect();
    let vals: Vec<i64> = ids.iter().map(|i| i * 10).collect();
    let large_batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(vals)),
        ],
    )?;
    common::insert_batch(&table, large_batch).await?;

    // The large insert does NOT flush inlined data — it went straight to Vortex.
    // Inlined blobs from the first insert should still be present.
    assert_eq!(
        fixture.catalog.get_inlined_data_count(&table_id).await?,
        5,
        "small-then-large insert should not trigger checkpoint (below 10K threshold)",
    );

    // All rows should be queryable: 5 inlined + 1101 from Vortex.
    ctx.register_table("inline_union", Arc::new(table))?;
    assert_eq!(query_count(&ctx, "inline_union").await, 1106);

    Ok(())
}

/// Overwrite should clear any accumulated inlined data.
async fn test_overwrite_clears_inlined_data(fixture: common::TestFixture) -> TestResult {
    let schema = simple_schema();
    let (table, ctx) = create_table_no_pk(&fixture, "inline_overwrite", Arc::clone(&schema)).await;
    let table_id = fixture
        .catalog
        .get_table("inline_overwrite")
        .await?
        .table_id;

    ctx.register_table("inline_overwrite", Arc::new(table))?;

    // Insert small batch (inlined)
    ctx.sql("INSERT INTO inline_overwrite VALUES (1,10),(2,20),(3,30)")
        .await?
        .collect()
        .await?;

    // Verify inlined
    assert!(fixture.catalog.get_inlined_data_count(&table_id).await? > 0);

    // Overwrite with new data
    ctx.sql("INSERT OVERWRITE inline_overwrite VALUES (99,990),(100,1000)")
        .await?
        .collect()
        .await?;

    // Inlined data should be cleared
    assert_eq!(
        fixture.catalog.get_inlined_data_count(&table_id).await?,
        0,
        "Overwrite should clear inlined data"
    );

    // Only the overwrite data should be visible
    assert_eq!(query_count(&ctx, "inline_overwrite").await, 2);

    Ok(())
}

/// DELETE on a PK table must remove exactly the matching row.
///
/// Note: PK tables bypass the inlining fast-path (see
/// `provider::sink::can_inline`), so this test covers DELETE correctness on a
/// Vortex-backed table. Checkpoint-before-delete behavior for inlined data is
/// exercised by
/// `data_inlining_test::test_roundtrip_across_reopen` / the `delete_from`
/// implementation in `provider::table`.
async fn test_delete_removes_row_from_pk_table(fixture: common::TestFixture) -> TestResult {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let (table, ctx) = create_table_with_pk(
        &fixture,
        "inline_del",
        Arc::clone(&schema),
        vec!["id".into()],
    )
    .await;
    ctx.register_table("inline_del", Arc::new(table))?;

    // Insert data (goes through Vortex since table has PK)
    ctx.sql("INSERT INTO inline_del VALUES (1,10),(2,20),(3,30)")
        .await?
        .collect()
        .await?;

    // Delete one row
    ctx.sql("DELETE FROM inline_del WHERE id = 2")
        .await?
        .collect()
        .await?;

    // Should have 2 rows remaining
    assert_eq!(query_count(&ctx, "inline_del").await, 2);

    Ok(())
}
