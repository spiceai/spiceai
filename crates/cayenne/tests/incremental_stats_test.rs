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
#![allow(clippy::clone_on_ref_ptr)]
#![allow(clippy::cast_precision_loss)]

//! End-to-end tests for incrementally-maintained executor statistics: live
//! `num_rows` nets supersedes under upsert (not the sum of inserts), and
//! per-integer-column NDV (distinct-count) sketches are populated and reasonably
//! accurate — both surfaced via `CayenneTableProvider::optimizer_table_statistics()`
//! and DataFusion's `TableProvider::statistics()` optimizer metadata hook.

mod common;

use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use cayenne::metadata::{CreateTableOptions, VortexConfig};
use cayenne::{CayenneTableProvider, MetadataCatalog};

use datafusion::common::stats::Precision;
use datafusion::prelude::SessionContext;
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};

test_with_backends!(test_live_num_rows_and_ndv_under_upsert);

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("val", DataType::Int64, true),
    ]))
}

/// Build a batch with `id` in `[start, start+count)` and `val = id * mult`.
fn batch(start: i64, count: i64, mult: i64) -> RecordBatch {
    let ids: Vec<i64> = (start..start + count).collect();
    let vals: Vec<i64> = ids.iter().map(|i| i * mult).collect();
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(vals)),
        ],
    )
    .expect("batch")
}

fn distinct_count(stats: &datafusion::common::Statistics, col: usize) -> Option<usize> {
    match stats.column_statistics[col].distinct_count {
        Precision::Exact(n) | Precision::Inexact(n) => Some(n),
        Precision::Absent => None,
    }
}

fn table_provider_statistics(table: &CayenneTableProvider) -> datafusion::common::Statistics {
    datafusion::datasource::TableProvider::statistics(table)
        .expect("table provider statistics present")
}

fn assert_within(actual: usize, expected: usize, rel: f64, what: &str) {
    let diff = (actual as f64 - expected as f64).abs();
    assert!(
        diff <= rel * expected as f64,
        "{what}: {actual} not within {}% of {expected}",
        rel * 100.0
    );
}

async fn count_star(ctx: &SessionContext, table: &str) -> usize {
    let results = ctx
        .sql(&format!("SELECT COUNT(*) AS c FROM {table}"))
        .await
        .expect("count")
        .collect()
        .await
        .expect("collect");
    let col = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count col");
    usize::try_from(col.value(0)).unwrap_or(0)
}

async fn test_live_num_rows_and_ndv_under_upsert(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let table = CayenneTableProvider::create_table(
        catalog,
        CreateTableOptions {
            table_name: "inc_stats".to_string(),
            schema: schema(),
            primary_key: vec!["id".to_string()],
            on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
                "id".to_string(),
            ]))),
            base_path: fixture.data_path.to_string_lossy().to_string(),
            partition_column: None,
            vortex_config: VortexConfig::default(),
        },
        ctx.runtime_env(),
    )
    .await?;
    let table = Arc::new(table);
    ctx.register_table(
        "inc_stats",
        Arc::clone(&table) as Arc<dyn datafusion::datasource::TableProvider>,
    )?;

    // 1. Insert 2000 distinct ids -> num_rows == 2000, NDV(id) ~ 2000.
    common::insert_batch(&table, batch(1, 2000, 10)).await?;
    table.flush_pending_maintenance().await?;

    let stats = table
        .optimizer_table_statistics()
        .expect("stats present after insert");
    let table_provider_stats = table_provider_statistics(table.as_ref());
    assert_eq!(
        stats.num_rows.get_value().copied(),
        Some(2000),
        "after first insert, live num_rows should be 2000"
    );
    assert_eq!(
        table_provider_stats.num_rows.get_value().copied(),
        Some(2000),
        "TableProvider::statistics should expose live num_rows after insert"
    );
    assert_within(
        distinct_count(&stats, 0).expect("id NDV"),
        2000,
        0.10,
        "id NDV after insert",
    );
    assert_within(
        distinct_count(&table_provider_stats, 0).expect("id TableProvider NDV"),
        2000,
        0.10,
        "id NDV from TableProvider::statistics after insert",
    );
    assert_within(
        distinct_count(&stats, 1).expect("val NDV"),
        2000,
        0.10,
        "val NDV after insert",
    );

    // 2. Upsert ids 1..=1000 (supersede 1000 existing rows). Live count must
    //    stay 2000 — inserted (1000) - superseded (1000) = 0 net — NOT 3000.
    common::insert_batch(&table, batch(1, 1000, 100)).await?;
    table.flush_pending_maintenance().await?;

    assert_eq!(
        count_star(&ctx, "inc_stats").await,
        2000,
        "COUNT(*) after upsert"
    );
    let stats = table
        .optimizer_table_statistics()
        .expect("stats after upsert");
    let table_provider_stats = table_provider_statistics(table.as_ref());
    assert_eq!(
        stats.num_rows.get_value().copied(),
        Some(2000),
        "live num_rows must net supersedes (stay 2000, not grow to 3000)"
    );
    assert_eq!(
        table_provider_stats.num_rows.get_value().copied(),
        Some(2000),
        "TableProvider::statistics live num_rows must net supersedes"
    );
    // No new distinct ids were introduced by the upsert.
    assert_within(
        distinct_count(&stats, 0).expect("id NDV"),
        2000,
        0.10,
        "id NDV after upsert",
    );

    // 3. Insert 1000 brand-new ids -> num_rows == 3000, NDV(id) ~ 3000.
    common::insert_batch(&table, batch(2001, 1000, 10)).await?;
    table.flush_pending_maintenance().await?;

    assert_eq!(
        count_star(&ctx, "inc_stats").await,
        3000,
        "COUNT(*) after new insert"
    );
    let stats = table
        .optimizer_table_statistics()
        .expect("stats after new insert");
    let table_provider_stats = table_provider_statistics(table.as_ref());
    assert_eq!(
        stats.num_rows.get_value().copied(),
        Some(3000),
        "live num_rows after inserting 1000 new ids"
    );
    assert_eq!(
        table_provider_stats.num_rows.get_value().copied(),
        Some(3000),
        "TableProvider::statistics should expose live num_rows after new insert"
    );
    assert_within(
        distinct_count(&stats, 0).expect("id NDV"),
        3000,
        0.10,
        "id NDV after new insert",
    );
    assert_within(
        distinct_count(&table_provider_stats, 0).expect("id TableProvider NDV"),
        3000,
        0.10,
        "id NDV from TableProvider::statistics after new insert",
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// Regression tests for inline num_rows double-counting (40a6ffa3dd)
// ---------------------------------------------------------------------------
//
// `add_inlined_rows_to_statistics` in `cached_table_statistics_for_optimizer`
// folded `inlined_row_count` into the optimizer's `num_rows`, but `num_rows`
// was already updated via `live_rows_delta` through `schedule_post_write_maintenance`
// on the inline write path — double-counting every inline row.
//
// Test A: inline upsert superseding file-backed rows — `live_rows_delta` is 0
//         but `inlined_row_count` grew, so `num_rows` overcounted.
// Test B: fresh inline insert (no supersedes) — `live_rows_delta` is N and
//         `inlined_row_count` also grew by N, so `num_rows` overcounted by N.

test_with_backends!(test_inline_upsert_superseding_file_rows);
test_with_backends!(test_fresh_inline_after_staged_no_double_count);

/// Regression test A: an inline upsert that supersedes file-backed rows
/// must not inflate `num_rows` — `live_rows_delta` nets the supersedes.
async fn test_inline_upsert_superseding_file_rows(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let table = CayenneTableProvider::create_table(
        catalog,
        CreateTableOptions {
            table_name: "upsert_file_supersede".to_string(),
            schema: schema(),
            primary_key: vec!["id".to_string()],
            on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
                "id".to_string(),
            ]))),
            base_path: fixture.data_path.to_string_lossy().to_string(),
            partition_column: None,
            vortex_config: VortexConfig::default(),
        },
        ctx.runtime_env(),
    )
    .await?;
    let table = Arc::new(table);
    ctx.register_table(
        "upsert_file_supersede",
        Arc::clone(&table) as Arc<dyn datafusion::datasource::TableProvider>,
    )?;

    // 1. Insert 2000 rows — exceeds DEFAULT_INLINE_MAX_ROWS (1024) → staged path.
    common::insert_batch(&table, batch(1, 2000, 10)).await?;
    table.flush_pending_maintenance().await?;

    let stats = table
        .optimizer_table_statistics()
        .expect("stats after staged insert");
    assert_eq!(stats.num_rows.get_value().copied(), Some(2000));

    // 2. Upsert 500 rows (ids 1..=500) — fits inline (500 ≤ 1024),
    //    supersedes 500 FILE-BACKED rows. Net delta = 0.
    common::insert_batch(&table, batch(1, 500, 100)).await?;
    table.flush_pending_maintenance().await?;

    assert_eq!(
        count_star(&ctx, "upsert_file_supersede").await,
        2000,
        "COUNT(*) must stay 2000 after upsert"
    );
    let stats = table
        .optimizer_table_statistics()
        .expect("stats after inline upsert");
    assert_eq!(
        stats.num_rows.get_value().copied(),
        Some(2000),
        "num_rows must stay 2000 — inline upsert supersedes must be netted"
    );

    Ok(())
}

/// Regression test B: a fresh inline insert (no supersedes) after a
/// staged insert must not double-count the new rows.
async fn test_fresh_inline_after_staged_no_double_count(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let table = CayenneTableProvider::create_table(
        catalog,
        CreateTableOptions {
            table_name: "fresh_inline_no_dbl".to_string(),
            schema: schema(),
            primary_key: vec!["id".to_string()],
            on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
                "id".to_string(),
            ]))),
            base_path: fixture.data_path.to_string_lossy().to_string(),
            partition_column: None,
            vortex_config: VortexConfig::default(),
        },
        ctx.runtime_env(),
    )
    .await?;
    let table = Arc::new(table);
    ctx.register_table(
        "fresh_inline_no_dbl",
        Arc::clone(&table) as Arc<dyn datafusion::datasource::TableProvider>,
    )?;

    // 1. Insert 2000 rows — staged path.
    common::insert_batch(&table, batch(1, 2000, 10)).await?;
    table.flush_pending_maintenance().await?;

    let stats = table
        .optimizer_table_statistics()
        .expect("stats after staged insert");
    assert_eq!(stats.num_rows.get_value().copied(), Some(2000));

    // 2. Insert 200 brand-new rows (ids 2001..=2200) — fits inline (200 ≤ 1024),
    //    no supersedes. num_rows must be 2200, not 2400.
    common::insert_batch(&table, batch(2001, 200, 10)).await?;
    table.flush_pending_maintenance().await?;

    assert_eq!(
        count_star(&ctx, "fresh_inline_no_dbl").await,
        2200,
        "COUNT(*) must be 2200"
    );
    let stats = table
        .optimizer_table_statistics()
        .expect("stats after fresh inline insert");
    assert_eq!(
        stats.num_rows.get_value().copied(),
        Some(2200),
        "num_rows must be 2200 — fresh inline insert must not be double-counted"
    );

    Ok(())
}
