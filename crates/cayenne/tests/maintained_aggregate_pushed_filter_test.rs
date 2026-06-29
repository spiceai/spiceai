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

//! P0 regression test: a maintained aggregate view declared WITHOUT a filter must
//! NOT be used to answer a query that carries a `WHERE`.
//!
//! Broken on trunk: `maintained_aggregate_source` (`provider/optimizer_rules.rs`,
//! the `CayenneAccelerationExec` branch) returned the maintained registry with
//! `filter = None` and NO `has_pushed_filter()` guard. The physical `FilterPushdown`
//! pass pushes a Vortex-convertible predicate INTO the scan and REMOVES the
//! `FilterExec` above it, so the rewriter reaches the bare scan, matches an
//! UNFILTERED view, and serves the WHOLE-TABLE aggregate — silently dropping the
//! `WHERE` and returning wrong results. The sibling `CayenneStatsAggregateRewriter`
//! already guards this with `if scan.has_pushed_filter() { decline }`; the fix
//! mirrors that guard in the maintained-aggregate rewrite.
//!
//! Airtight design (a no-op test is worthless, so each gate is asserted):
//! - Gate A (freshness): the UNFILTERED query IS served by `MaintainedAggregateExec`
//!   — proves the registry is populated/fresh and the rewrite machinery works.
//! - Gate B (trigger): the FILTERED query's plan shows the predicate pushed onto
//!   the Vortex file source (`predicate: ...` on the `DataSourceExec`), proving the
//!   pushed-filter branch is actually reached. (A surviving `FilterExec` on the
//!   empty inline/delta branch of the base+delta union is expected and irrelevant —
//!   what matters is that a file source carries the predicate.) If this fails the
//!   bug isn't exercised and the test fails loudly rather than passing vacuously.
//! - Gate C (regression): the FILTERED result equals the correct FILTERED totals.
//!   On trunk the unfiltered view is served → wrong totals → FAILS; after the
//!   guard the query declines the view → scan+filter+aggregate → correct → PASSES.
//! - Gate D (fix direction): after the guard the filtered query no longer uses the
//!   maintained view.
//!
//! `inline_max_rows: 0` forces every insert into a Vortex FILE (not the inline
//! memtable), so the pushed predicate lands on a file source and
//! `plan_has_pushed_filter` (which inspects file-scan sources) returns true.

#![allow(clippy::expect_used)]

mod common;

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use cayenne::maintained_aggregate::{
    MaintainedAggregateExpr, MaintainedAggregateFunction, MaintainedAggregateSpec,
};
use cayenne::metadata::{CreateTableOptions, VortexConfig};
use cayenne::optimizer_rules::CayenneMaintainedAggregateRewriter;
use cayenne::{CayenneTableProvider, CayenneTableProviderBuilder, MetadataCatalog};
use common::TestFixture;
use datafusion::datasource::TableProvider;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_plan::{collect, displayable};
use datafusion::prelude::{SessionContext, col, lit};
use datafusion_table_providers::util::column_reference::ColumnReference;
use datafusion_table_providers::util::on_conflict::OnConflict;

type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

const TABLE: &str = "maintained_pushed_filter";
const TABLE_DEL: &str = "maintained_pushed_filter_del";
/// Rows whose `v` clears the filter threshold. With the data below, the correct
/// FILTERED totals are k10 = 200, k20 = 300; the (wrong) UNFILTERED totals the
/// bug serves are k10 = 205, k20 = 350.
const FILTER_THRESHOLD: i64 = 100;

fn table_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false), // PK -> key-based deletion / upsert
        Field::new("k", DataType::Int64, false),  // GROUP BY key
        Field::new("v", DataType::Int64, false),  // summed AND filtered
    ]))
}

/// SUM(v) GROUP BY k with `filter: None` — the unfiltered view the bug wrongly
/// serves for a filtered query.
fn unfiltered_sum_v_by_k() -> MaintainedAggregateSpec {
    MaintainedAggregateSpec {
        group_by: vec!["k".to_string()],
        aggregates: vec![MaintainedAggregateExpr {
            function: MaintainedAggregateFunction::Sum,
            column: Some("v".to_string()),
        }],
        filter: None,
    }
}

/// A `SessionContext` whose physical optimizer has `DataFusion`'s defaults (which
/// include `FilterPushdown`, running first) PLUS the Cayenne maintained-aggregate
/// rewrite appended after them — exactly the production ordering that triggers the
/// bug (`FilterPushdown` removes the `FilterExec`, then the rewrite sees the bare scan).
fn cayenne_ctx() -> SessionContext {
    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_physical_optimizer_rule(Arc::new(CayenneMaintainedAggregateRewriter::new()))
        .build();
    SessionContext::new_with_state(state)
}

async fn plan_string(ctx: &SessionContext, sql: &str) -> TestResult<String> {
    let plan = ctx.sql(sql).await?.create_physical_plan().await?;
    Ok(displayable(plan.as_ref()).indent(true).to_string())
}

/// Collect `(k, SUM(v))` rows, sorted, so assertions are order-independent.
async fn rows_k_sum(ctx: &SessionContext, sql: &str) -> TestResult<Vec<(i64, i64)>> {
    let batches = ctx.sql(sql).await?.collect().await?;
    let mut out = Vec::new();
    for batch in &batches {
        let k = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("group column is Int64");
        let sum = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("sum column is Int64");
        // Bounded by the batch row count.
        for row in 0..batch.num_rows() {
            out.push((k.value(row), sum.value(row)));
        }
    }
    out.sort_unstable();
    Ok(out)
}

async fn maintained_aggregate_pushed_filter_impl(fixture: TestFixture) -> TestResult<()> {
    let ctx = cayenne_ctx();
    let catalog: Arc<dyn MetadataCatalog> = Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;

    // 1) Create an upsert (Int64-PK) table, FILE-backed (`inline_max_rows: 0`), so
    //    the pushed predicate lands on a file source.
    let options = CreateTableOptions {
        table_name: TABLE.to_string(),
        schema: table_schema(),
        primary_key: vec!["id".to_string()],
        on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec!["id".to_string()]))),
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: VortexConfig {
            inline_max_rows: 0,
            ..VortexConfig::default()
        },
    };
    let table = Arc::new(
        CayenneTableProvider::create_table(Arc::clone(&catalog), options, ctx.runtime_env()).await?,
    );

    // 2) Insert two groups, each with one row below and one at/above the threshold.
    //    Unfiltered totals: k10 = 5+200 = 205, k20 = 50+300 = 350.
    //    Correct filtered (v >= 100) totals: k10 = 200, k20 = 300.
    let batch = RecordBatch::try_new(
        table_schema(),
        vec![
            Arc::new(Int64Array::from(vec![1_i64, 2, 3, 4])),
            Arc::new(Int64Array::from(vec![10_i64, 10, 20, 20])),
            Arc::new(Int64Array::from(vec![5_i64, 200, 50, 300])),
        ],
    )?;
    let inserted = common::insert_batch(table.as_ref(), batch).await?;
    assert_eq!(inserted, 4, "all four rows must be written");

    let table_id = catalog.get_table(TABLE).await?.table_id;
    assert_eq!(
        catalog.get_inlined_data_count(&table_id).await?,
        0,
        "data must be file-backed (inline_max_rows=0) so the predicate is pushed onto a file source"
    );
    drop(table);

    // 3) Re-open WITH the unfiltered maintained view. The open-time rebuild scans
    //    the committed files and populates the registry Fresh at epoch 0 (the
    //    deterministic way to feed it — plain `insert_into` does not).
    let reopened = CayenneTableProviderBuilder::new(catalog, ctx.runtime_env())
        .with_maintained_aggregates(vec![unfiltered_sum_v_by_k()])
        .open(TABLE)
        .await?;
    ctx.register_table(TABLE, Arc::new(reopened) as Arc<dyn TableProvider>)?;

    // Gate A — the UNFILTERED query is served by the maintained view (freshness).
    let unfiltered_sql = format!("SELECT k, SUM(v) FROM {TABLE} GROUP BY k");
    let unfiltered_plan = plan_string(&ctx, &unfiltered_sql).await?;
    assert!(
        unfiltered_plan.contains("MaintainedAggregateExec"),
        "Gate A: the unfiltered query must be served by the maintained view (registry fresh). Plan:\n{unfiltered_plan}"
    );

    // Gate B — the FILTERED query's predicate is pushed onto the Vortex file source
    // (`predicate:` on the DataSourceExec), so the pushed-filter branch is reached.
    // (The base+delta union keeps a FilterExec on the empty inline branch — expected
    // and irrelevant; what matters is a file source carrying the predicate.) Fails
    // loudly if not exercised.
    let filtered_sql =
        format!("SELECT k, SUM(v) FROM {TABLE} WHERE v >= {FILTER_THRESHOLD} GROUP BY k");
    let filtered_plan = plan_string(&ctx, &filtered_sql).await?;
    assert!(
        filtered_plan.contains("predicate:"),
        "Gate B: the predicate must be pushed onto the Vortex file source so the bug is exercised. Plan:\n{filtered_plan}"
    );

    // Gate C — THE REGRESSION: the filtered result must be the correct filtered totals.
    // On trunk the unfiltered view is served -> (10,205),(20,350) -> FAILS.
    // After the guard the view is declined -> scan+filter+aggregate -> (10,200),(20,300) -> PASSES.
    let got = rows_k_sum(&ctx, &format!("{filtered_sql} ORDER BY k")).await?;
    assert_eq!(
        got,
        vec![(10, 200), (20, 300)],
        "Gate C: filtered query returned wrong totals — an unfiltered maintained view served a \
         filtered query (the WHERE was silently dropped). has_pushed_filter() guard missing in \
         maintained_aggregate_source."
    );

    // Gate D — after the fix the filtered query falls back to scan+aggregate.
    assert!(
        !filtered_plan.contains("MaintainedAggregateExec"),
        "Gate D: after the guard the filtered query must NOT use the maintained view. Plan:\n{filtered_plan}"
    );

    Ok(())
}

test_with_backends!(maintained_aggregate_pushed_filter_impl);

/// Finding-1 variant: the table carries a pending key-deletion tombstone, so the
/// scan is wrapped in a deletion-filter exec and the query predicate is pushed onto
/// the file source BELOW it. A shallow `has_pushed_filter` (identity-preserving
/// whitelist) stops above the deletion exec and misses the predicate — so the bug
/// stays open on exactly the merge-on-read CDC tables maintained views target. This
/// asserts the DEEP walk closes it (Gate C) AND that a deletion filter alone (no
/// query predicate) does NOT over-decline the view (Gate A).
async fn maintained_aggregate_pushed_filter_with_deletes_impl(fixture: TestFixture) -> TestResult<()> {
    let ctx = cayenne_ctx();
    let catalog: Arc<dyn MetadataCatalog> = Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;

    let options = CreateTableOptions {
        table_name: TABLE_DEL.to_string(),
        schema: table_schema(),
        primary_key: vec!["id".to_string()],
        on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec!["id".to_string()]))),
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: VortexConfig {
            inline_max_rows: 0,
            ..VortexConfig::default()
        },
    };
    let table = Arc::new(
        CayenneTableProvider::create_table(Arc::clone(&catalog), options, ctx.runtime_env()).await?,
    );

    // Same four rows + a fifth (id=5, k=10, v=999) that we DELETE, so the table
    // carries a pending key-tombstone (the merge-on-read shape) at scan time.
    let batch = RecordBatch::try_new(
        table_schema(),
        vec![
            Arc::new(Int64Array::from(vec![1_i64, 2, 3, 4, 5])),
            Arc::new(Int64Array::from(vec![10_i64, 10, 20, 20, 10])),
            Arc::new(Int64Array::from(vec![5_i64, 200, 50, 300, 999])),
        ],
    )?;
    let inserted = common::insert_batch(table.as_ref(), batch).await?;
    assert_eq!(inserted, 5, "all five rows must be written");

    // Delete id=5 → a pending key-deletion tombstone.
    let delete_ctx = SessionContext::new();
    let delete_plan = table
        .delete_from(&delete_ctx.state(), vec![col("id").eq(lit(5_i64))])
        .await?;
    let _ = collect(delete_plan, delete_ctx.task_ctx()).await?;
    drop(table);

    // Re-open with the unfiltered view; the rebuild reads post-delete visible state
    // and the deletion index carries the tombstone, so scans wrap a deletion filter.
    let reopened = CayenneTableProviderBuilder::new(catalog, ctx.runtime_env())
        .with_maintained_aggregates(vec![unfiltered_sum_v_by_k()])
        .open(TABLE_DEL)
        .await?;
    ctx.register_table(TABLE_DEL, Arc::new(reopened) as Arc<dyn TableProvider>)?;

    // Gate A — an unfiltered query (no pushed predicate) must STILL serve from the
    // view: the deletion filter alone must not trip the guard (no over-decline).
    let unfiltered_sql = format!("SELECT k, SUM(v) FROM {TABLE_DEL} GROUP BY k");
    let unfiltered_plan = plan_string(&ctx, &unfiltered_sql).await?;
    assert!(
        unfiltered_plan.contains("MaintainedAggregateExec"),
        "Gate A: unfiltered query must serve from the view even with deletes present (no over-decline). Plan:\n{unfiltered_plan}"
    );

    let filtered_sql =
        format!("SELECT k, SUM(v) FROM {TABLE_DEL} WHERE v >= {FILTER_THRESHOLD} GROUP BY k");
    let filtered_plan = plan_string(&ctx, &filtered_sql).await?;

    // Precondition — the scan really carries a deletion-filter exec (so this test
    // exercises the deep-walk path: a predicate pushed BELOW it).
    assert!(
        filtered_plan.contains("DeletionFilterExec"),
        "precondition: the scan must carry a deletion-filter exec (pending tombstone). Plan:\n{filtered_plan}"
    );

    // Gate B — the predicate is pushed onto the file source (`predicate:` on the
    // DataSourceExec) which sits BELOW the deletion exec, so the shallow walk that
    // stops at the deletion exec would miss it. (A FilterExec survives on the empty
    // inline branch of the union — irrelevant here.)
    assert!(
        filtered_plan.contains("predicate:"),
        "Gate B: the predicate must be pushed onto the file source below the deletion exec. Plan:\n{filtered_plan}"
    );

    // Gate C — THE FINDING-1 REGRESSION: with a shallow guard the deletion exec hides
    // the pushed predicate, the unfiltered view is served, and the result is the
    // wrong (10,205),(20,350); the deep walk declines → scan+filter+aggregate →
    // (10,200),(20,300).
    let got = rows_k_sum(&ctx, &format!("{filtered_sql} ORDER BY k")).await?;
    assert_eq!(
        got,
        vec![(10, 200), (20, 300)],
        "Gate C: filtered query over a merge-on-read table returned wrong totals — the deep \
         pushed-filter walk must detect a predicate pushed below the deletion-filter exec."
    );

    // Gate D — the filtered query declined the view.
    assert!(
        !filtered_plan.contains("MaintainedAggregateExec"),
        "Gate D: filtered query must fall back to scan+aggregate. Plan:\n{filtered_plan}"
    );

    Ok(())
}

test_with_backends!(maintained_aggregate_pushed_filter_with_deletes_impl);
