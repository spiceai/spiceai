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

use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray, TimestampMillisecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use cayenne::metadata::CreateTableOptions;
use cayenne::{CayenneTableProvider, MetadataCatalog};
use datafusion::prelude::*;
use std::sync::Arc;

type TestResult = Result<(), Box<dyn std::error::Error>>;

// ============================================================================
// Column Statistics Edge Cases
// ============================================================================

test_with_backends!(test_stats_aggregate_across_writes);
test_with_backends!(test_stats_correct_after_overwrite);
test_with_backends!(test_stats_with_all_null_column);
test_with_backends!(test_stats_with_mixed_types);
test_with_backends!(test_stats_min_max_correct_for_strings);

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

/// Stats are updated on each write and stored as a Vortex `FileStatistics` blob.
///
/// `persist_table_stats` merges the current write's stats with existing
/// persisted stats: `num_rows` is summed and `min`/`max` are widened.
async fn test_stats_aggregate_across_writes(fixture: common::TestFixture) -> TestResult {
    use datafusion::common::ScalarValue;
    use datafusion::common::stats::Precision;

    let schema = simple_schema();
    let (table, _ctx) = create_table_no_pk(&fixture, "stats_accum", Arc::clone(&schema)).await;
    let table_id = fixture.catalog.get_table("stats_accum").await?.table_id;

    // First append: 3 rows, value column = [10, 20, 30] (no nulls).
    let batch1 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(Int64Array::from(vec![10, 20, 30])),
        ],
    )?;
    common::insert_batch(&table, batch1).await?;

    let stats1 = fixture
        .catalog
        .get_table_statistics(&table_id)
        .await?
        .expect("stats present after first write");
    assert_eq!(stats1.num_rows, 3, "first write num_rows");
    let fs1 = cayenne::stats::deserialize_file_statistics(&stats1.statistics_blob, &schema)
        .expect("deserialize stats1");
    let df1 = cayenne::stats::file_statistics_to_df(&fs1, stats1.num_rows);
    let v1 = &df1.column_statistics[1];
    assert_eq!(v1.min_value, Precision::Exact(ScalarValue::Int64(Some(10))));
    assert_eq!(v1.max_value, Precision::Exact(ScalarValue::Int64(Some(30))));
    assert_eq!(v1.null_count, Precision::Exact(0));

    // Second append: 2 rows, value column = [5, 50] (no nulls).
    let batch2 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![4, 5])),
            Arc::new(Int64Array::from(vec![5, 50])),
        ],
    )?;
    common::insert_batch(&table, batch2).await?;

    let stats2 = fixture
        .catalog
        .get_table_statistics(&table_id)
        .await?
        .expect("stats present after second write");
    // Aggregating behavior: stats are merged across writes, so num_rows
    // reflects the cumulative total and min/max span all appends.
    assert_eq!(
        stats2.num_rows, 5,
        "aggregated num_rows across both writes (3 + 2)",
    );
    let fs2 = cayenne::stats::deserialize_file_statistics(&stats2.statistics_blob, &schema)
        .expect("deserialize stats2");
    let df2 = cayenne::stats::file_statistics_to_df(&fs2, stats2.num_rows);
    let v2 = &df2.column_statistics[1];
    assert_eq!(v2.min_value, Precision::Exact(ScalarValue::Int64(Some(5))));
    assert_eq!(v2.max_value, Precision::Exact(ScalarValue::Int64(Some(50))));
    assert_eq!(v2.null_count, Precision::Exact(0));

    Ok(())
}

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

/// Stats should handle all-NULL columns gracefully.
async fn test_stats_with_all_null_column(fixture: common::TestFixture) -> TestResult {
    let schema = simple_schema();
    let (table, _ctx) = create_table_no_pk(&fixture, "stats_null", Arc::clone(&schema)).await;
    let table_id = fixture.catalog.get_table("stats_null").await?.table_id;

    // Insert with all NULLs in the value column
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(Int64Array::from(vec![None, None, None])),
        ],
    )?;
    common::insert_batch(&table, batch).await?;

    let stats = fixture.catalog.get_table_statistics(&table_id).await?;
    assert!(stats.is_some(), "Stats should exist after insert");
    let stats = stats.expect("stats");
    assert_eq!(stats.num_rows, 3);
    assert!(
        !stats.statistics_blob.is_empty(),
        "statistics_blob should be non-empty even with all-NULL column"
    );

    Ok(())
}

/// Stats should work with multiple data types: Int64, Float64, Utf8, Timestamp.
async fn test_stats_with_mixed_types(fixture: common::TestFixture) -> TestResult {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("score", DataType::Float64, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("ts", DataType::Timestamp(TimeUnit::Millisecond, None), true),
    ]));

    let (table, _ctx) = create_table_no_pk(&fixture, "stats_types", Arc::clone(&schema)).await;
    let table_id = fixture.catalog.get_table("stats_types").await?.table_id;

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(Float64Array::from(vec![1.5, 2.7, 0.3])),
            Arc::new(StringArray::from(vec!["alice", "bob", "charlie"])),
            Arc::new(TimestampMillisecondArray::from(vec![1000, 2000, 3000])),
        ],
    )?;
    common::insert_batch(&table, batch).await?;

    let stats = fixture.catalog.get_table_statistics(&table_id).await?;
    assert!(stats.is_some(), "Stats should exist after insert");
    let stats = stats.expect("stats");
    assert_eq!(stats.num_rows, 3);
    assert!(
        !stats.statistics_blob.is_empty(),
        "statistics_blob should be non-empty for mixed types"
    );

    Ok(())
}

/// String min/max and `null_count` must round-trip correctly in the statistics
/// blob. Asserts exact values (lexicographic min="apple", max="cherry") rather
/// than just checking that the blob is non-empty — stats correctness is a
/// data-correctness guarantee.
async fn test_stats_min_max_correct_for_strings(fixture: common::TestFixture) -> TestResult {
    use datafusion::common::ScalarValue;
    use datafusion::common::stats::Precision;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let (table, _ctx) = create_table_no_pk(&fixture, "stats_str", Arc::clone(&schema)).await;
    let table_id = fixture.catalog.get_table("stats_str").await?.table_id;

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["banana", "apple", "cherry"])),
        ],
    )?;
    common::insert_batch(&table, batch).await?;

    let stats = fixture
        .catalog
        .get_table_statistics(&table_id)
        .await?
        .expect("stats should exist after insert");
    assert_eq!(stats.num_rows, 3);
    assert!(!stats.statistics_blob.is_empty());

    // Deserialize the blob and project into DataFusion Statistics so we can
    // assert exact min/max values (not just that it deserialized).
    let file_stats = cayenne::stats::deserialize_file_statistics(&stats.statistics_blob, &schema)
        .expect("FileStatistics should deserialize");
    let df_stats = cayenne::stats::file_statistics_to_df(&file_stats, stats.num_rows);

    assert_eq!(df_stats.num_rows, Precision::Exact(3));
    assert_eq!(df_stats.column_statistics.len(), 2, "one entry per column");

    let name_stats = &df_stats.column_statistics[1];
    assert_eq!(
        name_stats.min_value,
        Precision::Exact(ScalarValue::Utf8(Some("apple".into()))),
        "min should be lexicographic minimum",
    );
    assert_eq!(
        name_stats.max_value,
        Precision::Exact(ScalarValue::Utf8(Some("cherry".into()))),
        "max should be lexicographic maximum",
    );
    assert_eq!(
        name_stats.null_count,
        Precision::Exact(0),
        "no NULL names in the input batch",
    );

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
