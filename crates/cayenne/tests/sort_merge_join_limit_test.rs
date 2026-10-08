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

//! A `LIMIT` must survive `CayenneAntiJoinSortMergeRewriter`.
//!
//! `LimitPushdown` folds a `LIMIT` above a hash join into the join's `fetch`
//! and drops the limit node when nothing above the join merges partitions, so
//! on a single-partition join output the join's `fetch` is the only limit in
//! the plan. `SortMergeJoinExec` has no `fetch`; a rewrite that does not carry
//! it over returns every joined row of a `LIMIT n` query.
//!
//! Every query runs in a session with the rewriter and in one without it. One
//! partition makes the join output a single partition, and the memory gate is
//! set low enough that every build side here counts as oversized.

mod common;

use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::{Float64Array, Int32Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::util::display::array_value_to_string;
use cayenne::metadata::{CreateTableOptions, VortexConfig};
use cayenne::optimizer_rules::{CayenneAntiJoinSortMergeRewriter, CayenneOptimizerConfig};
use cayenne::{CayenneTableProvider, MetadataCatalog};
use common::TestFixture;
use datafusion::datasource::TableProvider;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_plan::displayable;
use datafusion::prelude::{SessionConfig, SessionContext};

type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

const CUSTOMERS: i32 = 300;
const ORDERS: i32 = 3_000;

/// Each query, the same query without its `LIMIT`/`OFFSET` (whose rows the
/// limited answer must be drawn from), and the number of rows the limited
/// query returns.
const CASES: &[(&str, &str, usize)] = &[
    (
        "SELECT c_name, o_id FROM customer JOIN orders ON c_id = o_c_id LIMIT 7",
        "SELECT c_name, o_id FROM customer JOIN orders ON c_id = o_c_id",
        7,
    ),
    (
        "SELECT c_name, o_id FROM customer JOIN orders ON c_id = o_c_id LIMIT 5 OFFSET 2",
        "SELECT c_name, o_id FROM customer JOIN orders ON c_id = o_c_id",
        5,
    ),
    // An aggregated build side of a full outer join is rewritten whatever its
    // size, and through the coalescing path.
    (
        "SELECT x.k, o_id FROM \
           (SELECT c_nation AS k, count(*) AS n FROM customer GROUP BY c_nation) x \
         FULL JOIN orders ON x.k = o_id LIMIT 3",
        "SELECT x.k, o_id FROM \
           (SELECT c_nation AS k, count(*) AS n FROM customer GROUP BY c_nation) x \
         FULL JOIN orders ON x.k = o_id",
        3,
    ),
    (
        "SELECT o_id FROM orders WHERE o_c_id IN (SELECT c_id FROM customer) LIMIT 4",
        "SELECT o_id FROM orders WHERE o_c_id IN (SELECT c_id FROM customer)",
        4,
    ),
];

async fn file_backed_table(
    fixture: &TestFixture,
    name: &str,
    batch: RecordBatch,
) -> TestResult<Arc<CayenneTableProvider>> {
    let schema: SchemaRef = batch.schema();
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let options = CreateTableOptions {
        table_name: name.to_string(),
        schema,
        primary_key: Vec::new(),
        on_conflict: None,
        base_path: fixture.data_path.join(name).to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: VortexConfig {
            inline_max_rows: 0,
            compaction_background_interval_ms: 3_600_000,
            ..VortexConfig::default()
        },
    };
    let ctx = SessionContext::new();
    let table =
        Arc::new(CayenneTableProvider::create_table(catalog, options, ctx.runtime_env()).await?);
    common::insert_batch(table.as_ref(), batch).await?;
    Ok(table)
}

fn customer_batch() -> TestResult<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("c_id", DataType::Int32, false),
        Field::new("c_nation", DataType::Int32, false),
        Field::new("c_name", DataType::Utf8, false),
    ]));
    let ids: Vec<i32> = (1..=CUSTOMERS).collect();
    let nations: Vec<i32> = ids.iter().map(|id| id % 25).collect();
    let names: Vec<String> = ids.iter().map(|id| format!("customer-{id}")).collect();
    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(Int32Array::from(nations)),
            Arc::new(StringArray::from(names)),
        ],
    )?)
}

fn orders_batch() -> TestResult<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("o_id", DataType::Int32, false),
        Field::new("o_c_id", DataType::Int32, false),
        Field::new("o_total", DataType::Float64, false),
    ]));
    let ids: Vec<i32> = (1..=ORDERS).collect();
    let customers: Vec<i32> = ids.iter().map(|id| id % CUSTOMERS + 1).collect();
    let totals: Vec<f64> = ids.iter().map(|id| f64::from(id % 1_000) / 8.0).collect();
    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(Int32Array::from(customers)),
            Arc::new(Float64Array::from(totals)),
        ],
    )?)
}

/// One partition and a memory gate every build side here exceeds, with or
/// without the rewriter.
fn session(with_rewriter: bool) -> SessionContext {
    let mut cayenne = CayenneOptimizerConfig::default();
    cayenne.sort_merge_memory_pool_bytes = Some(1_024);
    let config = SessionConfig::new()
        .with_target_partitions(1)
        .with_option_extension(cayenne);
    let mut builder = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features();
    if with_rewriter {
        builder =
            builder.with_physical_optimizer_rule(Arc::new(CayenneAntiJoinSortMergeRewriter::new()));
    }
    SessionContext::new_with_state(builder.build())
}

async fn rows(ctx: &SessionContext, sql: &str) -> TestResult<Vec<String>> {
    let batches = ctx.sql(sql).await?.collect().await?;
    let mut rows = Vec::new();
    for batch in &batches {
        for row in 0..batch.num_rows() {
            let mut rendered = Vec::with_capacity(batch.num_columns());
            for column in batch.columns() {
                rendered.push(array_value_to_string(column, row)?);
            }
            rows.push(rendered.join(","));
        }
    }
    Ok(rows)
}

async fn plan_of(ctx: &SessionContext, sql: &str) -> TestResult<String> {
    let plan = ctx.sql(sql).await?.create_physical_plan().await?;
    Ok(displayable(plan.as_ref()).indent(true).to_string())
}

async fn a_limit_survives_the_sort_merge_rewrite_impl(fixture: TestFixture) -> TestResult<()> {
    let customer = file_backed_table(&fixture, "customer", customer_batch()?).await?;
    let orders = file_backed_table(&fixture, "orders", orders_batch()?).await?;

    let with_rewriter = session(true);
    let reference = session(false);
    for ctx in [&with_rewriter, &reference] {
        ctx.register_table("customer", Arc::clone(&customer) as Arc<dyn TableProvider>)?;
        ctx.register_table("orders", Arc::clone(&orders) as Arc<dyn TableProvider>)?;
    }

    for &(sql, unlimited_sql, expected) in CASES {
        // The rewrite must be in play for the comparison to mean anything.
        let plan = plan_of(&with_rewriter, sql).await?;
        assert!(
            plan.contains("SortMergeJoin"),
            "precondition: the rewriter must turn this join into a sort-merge join: {sql}\n{plan}"
        );

        let all_rows: HashSet<String> =
            rows(&reference, unlimited_sql).await?.into_iter().collect();
        assert!(
            all_rows.len() > expected,
            "precondition: {unlimited_sql} must return more rows than the limit, got {}",
            all_rows.len()
        );
        assert_eq!(
            rows(&reference, sql).await?.len(),
            expected,
            "precondition: without the rewriter {sql} returns {expected} rows"
        );

        let got = rows(&with_rewriter, sql).await?;
        assert_eq!(
            got.len(),
            expected,
            "the sort-merge rewrite changed how many rows {sql} returns:\n{plan}"
        );
        for row in &got {
            assert!(
                all_rows.contains(row),
                "{sql} returned {row}, which the query without its LIMIT does not:\n{plan}"
            );
        }
    }
    Ok(())
}

test_with_backends!(a_limit_survives_the_sort_merge_rewrite_impl);
