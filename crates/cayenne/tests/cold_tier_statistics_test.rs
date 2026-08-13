/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! Table statistics must survive a warm→datalake promotion.
//!
//! Promotion moves every live row to the cold store and leaves the warm snapshot
//! empty. The maintained row count is what lets `COUNT(*)` be answered from
//! metadata instead of a scan, so a count that drifts across that move is a wrong
//! answer, not a bad plan. These tests pin `TableProvider::statistics` to the
//! count a real scan returns, on both sides of a promotion.

mod common;

use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use cayenne::metadata::{CreateTableOptions, DeletionMode, VortexConfig};
use cayenne::{CayenneTableProvider, MetadataCatalog};
use datafusion::datasource::TableProvider;
use datafusion::prelude::*;
use datafusion_common::stats::Precision;

type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

const TABLE: &str = "stats_t";

test_with_backends!(test_cold_tier_statistics_survive_promotion_impl);

/// Rows a real scan returns.
///
/// Projects a column and sums the batches rather than issuing `COUNT(*)`: an
/// unfiltered count can be folded from the maintained statistics, which are the
/// value under test, so a count would let one drifted number confirm itself.
async fn scan_row_count(ctx: &SessionContext) -> TestResult<i64> {
    let batches = ctx
        .sql(&format!("SELECT id FROM {TABLE}"))
        .await?
        .collect()
        .await?;
    let mut rows = 0;
    for batch in &batches {
        rows += i64::try_from(batch.num_rows())?;
    }
    Ok(rows)
}

/// Flush everything the count depends on: RAM/inline tiers into durable warm
/// files, then the debounced maintenance pass that persists the row delta.
async fn settle(table: &CayenneTableProvider) -> TestResult<()> {
    let _ = table.checkpoint_inlined_data().await;
    let _ = table.checkpoint_mem_tier().await;
    table.flush_pending_maintenance().await?;
    Ok(())
}

/// Assert a real scan returns `expected` rows and the maintained statistics
/// report the same number as `Exact`.
///
/// The precision is asserted, not just the value: only `Exact` lets `COUNT(*)` be
/// answered from metadata, so a promotion that demoted a correct count to
/// `Inexact` would silently cost that optimization. An `Exact` count may also be
/// substituted into a result, so a wrong value here is a wrong answer.
async fn assert_count_agrees(
    table: &CayenneTableProvider,
    ctx: &SessionContext,
    expected: usize,
    phase: &str,
) -> TestResult<()> {
    assert_eq!(
        scan_row_count(ctx).await?,
        i64::try_from(expected)?,
        "scanned rows ({phase})"
    );

    let maintained = table
        .statistics()
        .unwrap_or_else(|| panic!("maintained statistics must be populated ({phase})"))
        .num_rows;
    assert_eq!(
        maintained,
        Precision::Exact(expected),
        "maintained statistics ({phase})"
    );
    Ok(())
}

async fn delete_id(table: &CayenneTableProvider, id: i64) -> TestResult<()> {
    let ctx = SessionContext::new();
    let plan = table
        .delete_from(&ctx.state(), vec![col("id").eq(lit(id))])
        .await?;
    datafusion::physical_plan::collect(plan, ctx.task_ctx()).await?;
    Ok(())
}

async fn insert_range(table: &CayenneTableProvider, ids: std::ops::Range<i64>) -> TestResult<()> {
    let ids: Vec<i64> = ids.collect();
    let values: Vec<i64> = ids.iter().map(|i| i * 2).collect();
    let batch = RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(values)),
        ],
    )?;
    common::insert_batch(table, batch).await?;
    Ok(())
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

/// Table with the datalake tier on a local `file://` store, triggered by any
/// warm file so promotions are deterministic.
async fn create_table(
    fixture: &common::TestFixture,
    ctx: &SessionContext,
) -> TestResult<Arc<CayenneTableProvider>> {
    let cold_dir = fixture.temp_dir.path().join("cold");
    std::fs::create_dir_all(&cold_dir)?;

    let options = CreateTableOptions {
        table_name: TABLE.to_string(),
        schema: schema(),
        primary_key: vec!["id".to_string()],
        on_conflict: None,
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: VortexConfig {
            cold_tier_location: Some(format!("file://{}", cold_dir.to_string_lossy())),
            cold_clustering_columns: vec!["id".to_string()],
            cold_tier_warm_max_files: 1,
            deletion_mode: DeletionMode::Key,
            ..VortexConfig::default()
        },
    };

    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let table =
        Arc::new(CayenneTableProvider::create_table(catalog, options, ctx.runtime_env()).await?);
    ctx.register_table(TABLE, Arc::clone(&table) as Arc<dyn TableProvider>)?;
    Ok(table)
}

async fn test_cold_tier_statistics_survive_promotion_impl(
    fixture: common::TestFixture,
) -> TestResult<()> {
    let ctx = SessionContext::new();
    let table = create_table(&fixture, &ctx).await?;

    // Baseline: 100 warm rows, nothing promoted yet.
    insert_range(&table, 0..100).await?;
    settle(&table).await?;
    assert_count_agrees(&table, &ctx, 100, "warm only").await?;

    // Promotion moves all 100 rows warm→cold. The rows changed tier, not
    // existence, so the count must be conserved.
    assert!(
        table.promote_warm_to_cold().await?,
        "promotion should fire with cold_tier_warm_max_files = 1"
    );
    table.flush_pending_maintenance().await?;
    assert_count_agrees(&table, &ctx, 100, "after promotion").await?;

    // A write after promotion straddles the tiers: 100 cold + 50 warm. The count
    // must cover both.
    insert_range(&table, 100..150).await?;
    settle(&table).await?;
    assert_count_agrees(&table, &ctx, 150, "cold + warm delta").await?;

    // A second promotion rewrites the cold generation. The count must stay 150,
    // never the sum of both generations.
    assert!(
        table.promote_warm_to_cold().await?,
        "second promotion should fire"
    );
    table.flush_pending_maintenance().await?;
    assert_count_agrees(&table, &ctx, 150, "after second promotion").await?;

    // The cold manifest is the physical record behind that count, so it must
    // agree too.
    let cold_rows: i64 = fixture
        .catalog
        .list_cold_tier_files(table.table_id())
        .await?
        .iter()
        .map(|f| f.row_count)
        .sum();
    assert_eq!(
        cold_rows, 150,
        "cold manifest row counts sum to the live row set"
    );

    Ok(())
}

test_with_backends!(test_cold_tier_statistics_follow_a_folded_delete_impl);

/// Deleting a datalake-resident row must decrement the maintained count once the
/// promotion that folds the tombstone has run.
///
/// Regression test for #12846: promotion applied every tombstone physically and
/// cleared the deletion index — dropping the `has_pending_deletions()` mask that
/// was the only thing keeping the stale count off the `Exact` path — without
/// re-baselining the count, so the table reported `Exact(110)` while the cold
/// manifest correctly held 109 rows.
async fn test_cold_tier_statistics_follow_a_folded_delete_impl(
    fixture: common::TestFixture,
) -> TestResult<()> {
    let ctx = SessionContext::new();
    let table = create_table(&fixture, &ctx).await?;

    insert_range(&table, 0..100).await?;
    settle(&table).await?;
    assert!(table.promote_warm_to_cold().await?, "promotion should fire");
    table.flush_pending_maintenance().await?;

    // id=42 now lives only in the datalake. The delete hides it immediately.
    delete_id(&table, 42).await?;
    assert_eq!(
        scan_row_count(&ctx).await?,
        99,
        "the delete hides the datalake-resident row immediately"
    );

    // A promotion needs warm data to graduate, so the next batch is what carries
    // the tombstone into the datalake: 99 live + 10 new = 109.
    insert_range(&table, 100..110).await?;
    settle(&table).await?;
    assert!(
        table.promote_warm_to_cold().await?,
        "the promotion folding the tombstone should fire"
    );
    table.flush_pending_maintenance().await?;

    let cold_rows: i64 = fixture
        .catalog
        .list_cold_tier_files(table.table_id())
        .await?
        .iter()
        .map(|f| f.row_count)
        .sum();
    assert_eq!(
        cold_rows, 109,
        "the cold manifest drops the physically-removed row"
    );
    assert_count_agrees(&table, &ctx, 109, "after the tombstone was folded").await?;

    Ok(())
}

test_with_backends!(test_a_delete_taints_the_maintained_count_exactness_impl);

/// A standalone `DELETE` must taint the maintained count's exactness durably.
///
/// The second half of #12846: nothing re-derives the count on the delete path, so
/// `has_pending_deletions()` masking it to `Inexact` is the *only* thing keeping
/// the stale value off the `Exact` path — and every tombstone fold drops that
/// mask. Persisting the taint means a fold that does not re-baseline (and a
/// restart, which reloads the flag) cannot serve the stale count `Exact`; a full
/// rewrite still restores exactness with its own authoritative count.
async fn test_a_delete_taints_the_maintained_count_exactness_impl(
    fixture: common::TestFixture,
) -> TestResult<()> {
    let ctx = SessionContext::new();
    let table = create_table(&fixture, &ctx).await?;

    insert_range(&table, 0..200).await?;
    settle(&table).await?;
    assert_count_agrees(&table, &ctx, 200, "before the delete").await?;

    // A delete that matches nothing removes no rows, so the count still
    // describes the live set and stays exact.
    delete_id(&table, 12_846).await?;
    assert_count_agrees(&table, &ctx, 200, "after a delete matching no row").await?;

    delete_id(&table, 42).await?;
    settle(&table).await?;
    assert_eq!(
        scan_row_count(&ctx).await?,
        199,
        "the delete hides the row immediately"
    );
    assert!(
        !table
            .statistics()
            .expect("maintained statistics must be populated after a delete")
            .num_rows
            .is_exact()
            .unwrap_or(false),
        "the maintained count still counts the deleted row, so it must not be served Exact"
    );

    // Durable, not just cached: the flag is what a reopen reloads, and an
    // `Exact` flag over a stale count is the wrong answer this guards.
    let persisted = fixture
        .catalog
        .get_table_statistics(table.table_id())
        .await?
        .expect("statistics row must be persisted");
    assert!(
        !persisted.num_rows_exact,
        "the persisted exactness flag must be tainted, got num_rows={} num_rows_exact=true",
        persisted.num_rows
    );

    Ok(())
}
