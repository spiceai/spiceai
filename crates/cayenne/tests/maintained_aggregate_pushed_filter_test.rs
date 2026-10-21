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

use crate::common;

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
const TABLE_SERVED: &str = "maintained_pushed_filter_served";
const TABLE_CHBENCH: &str = "order_line";
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

/// SUM(v) GROUP BY k over the rows with `v >= FILTER_THRESHOLD` — the view a
/// query with that `WHERE` is answered from.
fn filtered_sum_v_by_k() -> MaintainedAggregateSpec {
    let schema = table_schema();
    let filter = datafusion::physical_expr::expressions::binary(
        datafusion::physical_expr::expressions::col("v", schema.as_ref())
            .expect("v is a table column"),
        datafusion::logical_expr::Operator::GtEq,
        datafusion::physical_expr::expressions::lit(FILTER_THRESHOLD),
        schema.as_ref(),
    )
    .expect("v >= threshold is a valid predicate");
    MaintainedAggregateSpec {
        filter: Some(filter),
        ..unfiltered_sum_v_by_k()
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
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;

    // 1) Create an upsert (Int64-PK) table, FILE-backed (`inline_max_rows: 0`), so
    //    the pushed predicate lands on a file source.
    let options = CreateTableOptions {
        table_name: TABLE.to_string(),
        schema: table_schema(),
        primary_key: vec!["id".to_string()],
        on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
            "id".to_string(),
        ]))),
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: VortexConfig {
            inline_max_rows: 0,
            ..VortexConfig::default()
        },
    };
    let table = Arc::new(
        CayenneTableProvider::create_table(Arc::clone(&catalog), options, ctx.runtime_env())
            .await?,
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
async fn maintained_aggregate_pushed_filter_with_deletes_impl(
    fixture: TestFixture,
) -> TestResult<()> {
    let ctx = cayenne_ctx();
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;

    let options = CreateTableOptions {
        table_name: TABLE_DEL.to_string(),
        schema: table_schema(),
        primary_key: vec!["id".to_string()],
        on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
            "id".to_string(),
        ]))),
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: VortexConfig {
            inline_max_rows: 0,
            ..VortexConfig::default()
        },
    };
    let table = Arc::new(
        CayenneTableProvider::create_table(Arc::clone(&catalog), options, ctx.runtime_env())
            .await?,
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

/// The other half of the pushed-filter contract: a view declared WITH the
/// query's filter must answer it even though physical `FilterPushdown` moved the
/// `WHERE` into the scan and removed the `FilterExec` above it — the shape every
/// filtered query takes against a file-backed table, CH-benCH q1/q6 included.
/// The table carries a pending key-tombstone, so the predicate also sits below a
/// deletion-filter exec, as on a merge-on-read CDC table.
///
/// - Gate A (shape): the predicate is pushed onto the file source below the
///   deletion exec, so the served path is the pushed one.
/// - Gate B: the filtered query is served by `MaintainedAggregateExec`.
/// - Gate C: the served totals are the correct filtered totals, without the
///   deleted row.
/// - Gate D: a query with a different predicate is not served from the view and
///   still returns its own correct totals.
async fn maintained_aggregate_filtered_view_serves_pushed_filter_impl(
    fixture: TestFixture,
) -> TestResult<()> {
    let ctx = cayenne_ctx();
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;

    let options = CreateTableOptions {
        table_name: TABLE_SERVED.to_string(),
        schema: table_schema(),
        primary_key: vec!["id".to_string()],
        on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
            "id".to_string(),
        ]))),
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: VortexConfig {
            inline_max_rows: 0,
            ..VortexConfig::default()
        },
    };
    let table = Arc::new(
        CayenneTableProvider::create_table(Arc::clone(&catalog), options, ctx.runtime_env())
            .await?,
    );

    // Filtered (v >= 100) totals: k10 = 200, k20 = 300. The deleted id=5 row
    // (v = 999) would add 999 to k10 if it were still counted.
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
    let delete_ctx = SessionContext::new();
    let delete_plan = table
        .delete_from(&delete_ctx.state(), vec![col("id").eq(lit(5_i64))])
        .await?;
    let _ = collect(delete_plan, delete_ctx.task_ctx()).await?;
    drop(table);

    let reopened = Arc::new(
        CayenneTableProviderBuilder::new(catalog, ctx.runtime_env())
            .with_maintained_aggregates(vec![filtered_sum_v_by_k()])
            .open(TABLE_SERVED)
            .await?,
    ) as Arc<dyn TableProvider>;
    ctx.register_table(TABLE_SERVED, Arc::clone(&reopened))?;

    let filtered_sql =
        format!("SELECT k, SUM(v) FROM {TABLE_SERVED} WHERE v >= {FILTER_THRESHOLD} GROUP BY k");

    // Gate A — without the rewrite, the WHERE is pushed into the scan: onto the
    // file source below the deletion exec, with no `FilterExec` left above the
    // scan. Serving replaces the scan, so the shape is read from a plan built
    // with `DataFusion`'s rules alone.
    let plain =
        SessionContext::new_with_state(SessionStateBuilder::new().with_default_features().build());
    plain.register_table(TABLE_SERVED, reopened)?;
    let unserved_plan = plan_string(&plain, &filtered_sql).await?;
    let operators: Vec<&str> = unserved_plan.lines().map(str::trim_start).collect();
    let scan_at = operators
        .iter()
        .position(|operator| operator.starts_with("CayenneAccelerationExec"))
        .expect("the unserved plan scans the Cayenne table");
    assert!(
        unserved_plan.contains("predicate:") && unserved_plan.contains("DeletionFilterExec"),
        "Gate A: the predicate must be pushed onto the file source below the deletion exec. Plan:\n{unserved_plan}"
    );
    assert!(
        !operators[..scan_at]
            .iter()
            .any(|operator| operator.starts_with("FilterExec:")),
        "Gate A: no FilterExec may remain above the scan, or this does not exercise the pushed shape. Plan:\n{unserved_plan}"
    );

    let filtered_plan = plan_string(&ctx, &filtered_sql).await?;

    // Gate B — the filtered query is served from the filtered view.
    assert!(
        filtered_plan.contains("MaintainedAggregateExec"),
        "Gate B: a query whose WHERE matches the view's filter must be served from the view even when the WHERE was pushed into the scan. Plan:\n{filtered_plan}"
    );

    // Gate C — the served totals are the correct filtered totals.
    let got = rows_k_sum(&ctx, &format!("{filtered_sql} ORDER BY k")).await?;
    assert_eq!(
        got,
        vec![(10, 200), (20, 300)],
        "Gate C: the filtered view served wrong totals"
    );

    // Gate D — another predicate is not the view's and runs the real aggregate.
    let other_sql = format!("SELECT k, SUM(v) FROM {TABLE_SERVED} WHERE v >= 50 GROUP BY k");
    let other_plan = plan_string(&ctx, &other_sql).await?;
    assert!(
        !other_plan.contains("MaintainedAggregateExec"),
        "Gate D: a query with a different predicate must not be served from the view. Plan:\n{other_plan}"
    );
    let other = rows_k_sum(&ctx, &format!("{other_sql} ORDER BY k")).await?;
    assert_eq!(
        other,
        vec![(10, 200), (20, 350)],
        "Gate D: the query the view declined returned wrong totals"
    );

    Ok(())
}

test_with_backends!(maintained_aggregate_filtered_view_serves_pushed_filter_impl);

/// A maintained filter as the Cayenne accelerator builds it from `filter_sql`:
/// parsed against the table schema, coerced and folded the way the planner
/// folds a query's `WHERE`, then planned.
fn filter_from_sql(
    sql: &str,
    schema: &Arc<Schema>,
) -> Arc<dyn datafusion::physical_expr::PhysicalExpr> {
    use datafusion::common::ToDFSchema;
    let df_schema = schema
        .as_ref()
        .clone()
        .to_dfschema()
        .expect("schema converts");
    let context = util::session_state::session_context();
    let logical = context
        .parse_sql_expr(sql, &df_schema)
        .expect("filter parses");
    let logical = util::expr::coerce_and_simplify_exprs([logical], schema)
        .expect("filter folds")
        .pop()
        .expect("one filter");
    context
        .create_physical_expr(logical, &df_schema)
        .expect("filter plans")
}

/// The scheduled CH-benCH pods declare q1 and q6 as maintained views on
/// `order_line`, with the queries' own predicates as `filter_sql`. Each query,
/// with its `WHERE` pushed into the scan, must be answered by its view, and
/// the answer must equal the base-table scan's.
async fn chbench_q1_and_q6_are_served_by_their_views_impl(fixture: TestFixture) -> TestResult<()> {
    use arrow::array::{Decimal128Array, Int32Array, TimestampMicrosecondArray};
    use arrow::datatypes::TimeUnit;

    let ctx = cayenne_ctx();
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let schema = Arc::new(Schema::new(vec![
        Field::new("ol_w_id", DataType::Int32, false),
        Field::new("ol_number", DataType::Int32, false),
        Field::new(
            "ol_delivery_d",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new("ol_quantity", DataType::Int32, false),
        Field::new("ol_amount", DataType::Decimal128(6, 2), false),
    ]));
    let options = CreateTableOptions {
        table_name: TABLE_CHBENCH.to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["ol_w_id".to_string(), "ol_number".to_string()],
        on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
            "ol_w_id".to_string(),
            "ol_number".to_string(),
        ]))),
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: VortexConfig {
            inline_max_rows: 0,
            ..VortexConfig::default()
        },
    };
    let table = Arc::new(
        CayenneTableProvider::create_table(Arc::clone(&catalog), options, ctx.runtime_env())
            .await?,
    );
    // 2008-01-01 (delivered) and NULL (undelivered) delivery dates.
    let delivered = Some(1_199_145_600_000_000_i64);
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![1, 1, 1, 2, 2])),
            Arc::new(Int32Array::from(vec![1, 2, 3, 1, 2])),
            Arc::new(TimestampMicrosecondArray::from(vec![
                delivered, delivered, None, delivered, delivered,
            ])),
            Arc::new(Int32Array::from(vec![5, 3, 7, 0, 9])),
            Arc::new(
                Decimal128Array::from(vec![1_050_i128, 2_000, 333, 10, 99_999])
                    .with_precision_and_scale(6, 2)?,
            ),
        ],
    )?;
    let inserted = common::insert_batch(table.as_ref(), batch).await?;
    assert_eq!(inserted, 5, "all five rows must be written");
    drop(table);

    let q1_view = MaintainedAggregateSpec {
        group_by: vec!["ol_number".to_string()],
        aggregates: vec![
            MaintainedAggregateExpr {
                function: MaintainedAggregateFunction::Sum,
                column: Some("ol_quantity".to_string()),
            },
            MaintainedAggregateExpr {
                function: MaintainedAggregateFunction::Sum,
                column: Some("ol_amount".to_string()),
            },
            MaintainedAggregateExpr {
                function: MaintainedAggregateFunction::Avg,
                column: Some("ol_quantity".to_string()),
            },
            MaintainedAggregateExpr {
                function: MaintainedAggregateFunction::Avg,
                column: Some("ol_amount".to_string()),
            },
            MaintainedAggregateExpr {
                function: MaintainedAggregateFunction::Count,
                column: None,
            },
        ],
        filter: Some(filter_from_sql(
            "ol_delivery_d > '2007-01-02 00:00:00.000000'",
            &schema,
        )),
    };
    let q6_view = MaintainedAggregateSpec {
        group_by: vec![],
        aggregates: vec![MaintainedAggregateExpr {
            function: MaintainedAggregateFunction::Sum,
            column: Some("ol_amount".to_string()),
        }],
        filter: Some(filter_from_sql(
            "ol_delivery_d >= '1997-01-01 00:00:00' AND ol_delivery_d < '2030-01-01 00:00:00' AND ol_quantity BETWEEN 1 AND 100000",
            &schema,
        )),
    };
    let reopened = Arc::new(
        CayenneTableProviderBuilder::new(catalog, ctx.runtime_env())
            .with_maintained_aggregates(vec![q1_view, q6_view])
            .open(TABLE_CHBENCH)
            .await?,
    ) as Arc<dyn TableProvider>;
    ctx.register_table(TABLE_CHBENCH, Arc::clone(&reopened))?;
    let scan_only =
        SessionContext::new_with_state(SessionStateBuilder::new().with_default_features().build());
    scan_only.register_table(TABLE_CHBENCH, reopened)?;

    let q1 = "SELECT ol_number, sum(ol_quantity) as sum_qty, sum(ol_amount) as sum_amount, avg(ol_quantity) as avg_qty, avg(ol_amount) as avg_amount, count(*) as count_order FROM order_line WHERE ol_delivery_d > '2007-01-02 00:00:00.000000' GROUP BY ol_number ORDER BY ol_number";
    let q6 = "SELECT sum(ol_amount) AS revenue FROM order_line WHERE ol_delivery_d >= '1997-01-01 00:00:00' AND ol_delivery_d < '2030-01-01 00:00:00' AND ol_quantity BETWEEN 1 AND 100000";
    for (name, sql) in [("q1", q1), ("q6", q6)] {
        let plan = plan_string(&ctx, sql).await?;
        assert!(
            plan.contains("MaintainedAggregateExec"),
            "CH-benCH {name} must be served by its maintained view. Plan:\n{plan}"
        );
        let served = ctx.sql(sql).await?.collect().await?;
        let scanned = scan_only.sql(sql).await?.collect().await?;
        assert_eq!(
            arrow::util::pretty::pretty_format_batches(&served)?.to_string(),
            arrow::util::pretty::pretty_format_batches(&scanned)?.to_string(),
            "CH-benCH {name} served from its view must equal the base-table scan"
        );
    }
    Ok(())
}

test_with_backends!(chbench_q1_and_q6_are_served_by_their_views_impl);
