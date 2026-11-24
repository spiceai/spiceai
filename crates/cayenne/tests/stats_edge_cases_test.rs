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

use crate::common;

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use cayenne::metadata::CreateTableOptions;
use cayenne::{CayenneTableProvider, MetadataCatalog};
use datafusion::common::stats::Precision;
use datafusion::prelude::*;
use std::sync::Arc;

type TestResult = Result<(), Box<dyn std::error::Error>>;

// ============================================================================
// Column Statistics Edge Cases
// ============================================================================

test_on_data_store!(test_stats_correct_after_overwrite);
test_on_data_store!(test_stats_correct_after_full_rewrite_with_a_queued_delta);

// ============================================================================
// Data Inlining Edge Cases
// ============================================================================

test_on_data_store!(test_large_insert_bypasses_inline);
test_on_data_store!(test_multiple_small_inserts_accumulate_inline);
test_on_data_store!(test_scan_unions_inlined_and_vortex);
test_on_data_store!(test_overwrite_clears_inlined_data);
test_on_data_store!(test_delete_removes_row_from_pk_table);

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
    let table = fixture
        .data_store
        .create(
            catalog,
            ctx.runtime_env(),
            CreateTableOptions {
                table_name: name.to_string(),
                schema,
                primary_key: vec![],
                on_conflict: None,
                base_path: fixture.data_store.base_path(name),
                partition_column: None,
                vortex_config: cayenne::metadata::VortexConfig::default(),
            },
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
    let table = fixture
        .data_store
        .create(
            catalog,
            ctx.runtime_env(),
            CreateTableOptions {
                table_name: name.to_string(),
                schema,
                primary_key: pk,
                on_conflict: None,
                base_path: fixture.data_store.base_path(name),
                partition_column: None,
                vortex_config: cayenne::metadata::VortexConfig::default(),
            },
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

/// After overwrite, stats should reflect only the new data, including once the
/// post-write maintenance queued by the replaced writes has drained. Covers an
/// overwrite that lands in the inline tier and one written to data files.
///
/// The insert's statistics reach the metastore on a debounced maintenance pass,
/// so whether that pass runs before, during or after the overwrite depends on
/// timing; on a fast run it usually comes after. The crate's unit tests
/// `overwrite_discards_*` hold the pass and the publish at fixed points.
async fn test_stats_correct_after_overwrite(fixture: common::TestFixture) -> TestResult {
    let schema = simple_schema();
    let (table, ctx) = create_table_no_pk(&fixture, "stats_overwrite", Arc::clone(&schema)).await;
    let handle = table.clone_for_write_operations();
    let table_id = fixture.catalog.get_table("stats_overwrite").await?.table_id;

    ctx.register_table("stats_overwrite", Arc::new(table))?;

    // Rows resident in the inline tier keep the served count inexact.
    let overwrites = [
        (
            "INSERT OVERWRITE stats_overwrite SELECT * FROM (VALUES (99,990),(100,1000))",
            2,
            Precision::Inexact(2),
        ),
        // Above the inline row cap, so the replacement is written to data files.
        (
            "INSERT OVERWRITE stats_overwrite \
             SELECT v AS id, v AS value FROM (SELECT UNNEST(range(0, 2000)) AS v)",
            2000,
            Precision::Exact(2000),
        ),
    ];
    for (overwrite, expected_rows, expected_served) in overwrites {
        ctx.sql("INSERT INTO stats_overwrite SELECT * FROM (VALUES (1,10),(2,20),(3,30))")
            .await?
            .collect()
            .await?;
        ctx.sql(overwrite).await?.collect().await?;

        let stats = fixture
            .catalog
            .get_table_statistics(&table_id)
            .await?
            .expect("stats after overwrite");
        assert_eq!(
            stats.num_rows, expected_rows,
            "After overwrite, stats should reflect only the new {expected_rows}-row data"
        );

        handle.drain_in_flight_maintenance().await?;
        let stats = fixture
            .catalog
            .get_table_statistics(&table_id)
            .await?
            .expect("stats after maintenance");
        assert_eq!(
            (stats.num_rows, stats.num_rows_exact),
            (expected_rows, true),
            "maintenance queued before the overwrite must not add the replaced rows"
        );
        let optimizer = handle
            .optimizer_table_statistics()
            .expect("optimizer statistics");
        assert_eq!(
            optimizer.num_rows, expected_served,
            "the table statistics the planner reads count only the new rows"
        );

        assert_eq!(
            query_count(&ctx, "stats_overwrite").await,
            usize::try_from(expected_rows)?
        );
    }

    Ok(())
}

/// A full rewrite counts the rows it read, which include those of a write whose
/// statistics are still queued for maintenance; once maintenance runs, that
/// write must not be counted a second time.
async fn test_stats_correct_after_full_rewrite_with_a_queued_delta(
    fixture: common::TestFixture,
) -> TestResult {
    const TABLE: &str = "stats_rewrite_queued_delta";
    let (table, ctx) = create_table_no_pk(&fixture, TABLE, simple_schema()).await;
    let handle = table.clone_for_write_operations();
    let table_id = fixture.catalog.get_table(TABLE).await?.table_id;
    ctx.register_table(TABLE, Arc::new(table))?;

    // Both writes are above the inline row cap, so they are written to data files.
    ctx.sql(&format!(
        "INSERT INTO {TABLE} SELECT v AS id, v AS value FROM (SELECT UNNEST(range(0, 2000)) AS v)"
    ))
    .await?
    .collect()
    .await?;
    handle.drain_in_flight_maintenance().await?;
    let held = handle.hold_post_write_maintenance().await;
    ctx.sql(&format!(
        "INSERT INTO {TABLE} SELECT v AS id, v AS value FROM (SELECT UNNEST(range(2000, 3500)) AS v)"
    ))
    .await?
    .collect()
    .await?;
    assert!(
        handle.force_full_compaction().await?,
        "the full rewrite commits"
    );
    drop(held);
    handle.drain_in_flight_maintenance().await?;

    let stats = fixture
        .catalog
        .get_table_statistics(&table_id)
        .await?
        .expect("stats after the rewrite");
    assert_eq!(
        (stats.num_rows, stats.num_rows_exact),
        (3500, true),
        "the rewrite's count already includes the second write"
    );
    let optimizer = handle
        .optimizer_table_statistics()
        .expect("optimizer statistics");
    assert_eq!(optimizer.num_rows, Precision::Exact(3500));
    assert_eq!(query_count(&ctx, TABLE).await, 3500);

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

    // The overwrite clears the previous inline corpus and — being small enough
    // to be admitted — writes its own rows back as ONE entry in the same
    // transaction, so the tier now holds exactly the overwritten rows and none
    // of the pre-overwrite ones. Note `get_inlined_data_count` sums
    // `record_count`: it counts inlined ROWS, not entries.
    let inlined_rows = fixture.catalog.get_inlined_data_count(&table_id).await?;
    assert_eq!(
        inlined_rows, 2,
        "an admitted overwrite replaces the inlined corpus with exactly its own rows"
    );
    assert_eq!(
        fixture.catalog.get_inlined_data(&table_id).await?.len(),
        1,
        "the replacement is a single entry"
    );

    // Only the overwrite data is visible, whichever tier the rows landed in.
    assert_eq!(query_count(&ctx, "inline_overwrite").await, 2);

    // The load-bearing property for a table refreshed on a schedule: repeated
    // overwrites must REPLACE, never accumulate. Without the clear in
    // `commit_overwrite_in_txn` this grows one entry per refresh forever.
    for _ in 0..4 {
        ctx.sql("INSERT OVERWRITE inline_overwrite VALUES (99,990),(100,1000)")
            .await?
            .collect()
            .await?;
    }
    assert_eq!(
        fixture.catalog.get_inlined_data(&table_id).await?.len(),
        1,
        "five overwrites must still leave exactly one inline entry"
    );
    assert_eq!(
        fixture.catalog.get_inlined_data_count(&table_id).await?,
        2,
        "and exactly the last overwrite's rows"
    );
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
