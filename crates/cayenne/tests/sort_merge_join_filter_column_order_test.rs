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

//! Regression test for #14235: CH-benCHmark q17 failed on Cayenne with
//! `column types must match schema types, expected Int32 but found Float64`.
//!
//! When `JoinSelection` swaps a hash join's inputs it swaps the side of every
//! join-filter column but keeps their order, so the filter can read a right
//! column before a left one (`CAST(ol_quantity@0 AS Float64) < a@1`, where
//! `ol_quantity` is on the right). `HashJoinExec` builds the filter batch in
//! `column_indices` order. `SortMergeJoinExec` builds it with every left
//! column first, so once `CayenneAntiJoinSortMergeRewriter` carries that
//! filter onto a sort-merge join the batch no longer matches the filter's
//! schema: a query fails when the misplaced columns differ in type, and
//! returns wrong rows when they do not.
//!
//! Every query runs in a session with the rewriter and in one without it, and
//! the rows must match. The memory gate is set low enough that the join counts
//! as oversized, and one partition gives both join inputs the same partition
//! count, which the rewrite requires.

use crate::common;

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

const ITEMS: i32 = 300;
const ORDER_LINES: i32 = 6_000;

/// CH-benCHmark q17, whose join filter compares an `Int32` from `order_line`
/// with the `Float64` average from the derived table.
const Q17: &str = "SELECT sum(ol_amount) / 2.0 AS avg_yearly \
    FROM order_line, \
      (SELECT i_id, avg(ol_quantity) AS a FROM item, order_line \
       WHERE i_data LIKE '%b' AND ol_i_id = i_id GROUP BY i_id) t \
    WHERE ol_i_id = t.i_id AND ol_quantity < t.a";

/// q17 with `max` in place of `avg`, so both sides of the filter are `Int32`.
/// Misplaced columns of one type raise no error: the filter compares
/// `a < ol_quantity` instead of `ol_quantity < a`, and the answer is wrong.
const Q17_SAME_TYPE: &str = "SELECT count(*) AS lines, sum(ol_amount) AS amount \
    FROM order_line, \
      (SELECT i_id, max(ol_quantity) AS a FROM item, order_line \
       WHERE i_data LIKE '%b' AND ol_i_id = i_id GROUP BY i_id) t \
    WHERE ol_i_id = t.i_id AND ol_quantity < t.a";

/// The same derived table outer-joined with the comparison in the `ON` clause,
/// so the filter also decides which `order_line` rows keep a NULL-extended
/// match.
const Q17_LEFT_JOIN: &str = "SELECT count(*) AS lines, count(t.i_id) AS matched, \
      sum(ol_amount) AS amount \
    FROM order_line LEFT JOIN \
      (SELECT i_id, avg(ol_quantity) AS a FROM item, order_line \
       WHERE i_data LIKE '%b' AND ol_i_id = i_id GROUP BY i_id) t \
    ON ol_i_id = t.i_id AND ol_quantity < t.a";

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

fn item_batch() -> TestResult<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("i_id", DataType::Int32, false),
        Field::new("i_data", DataType::Utf8, false),
    ]));
    let ids: Vec<i32> = (1..=ITEMS).collect();
    let data: Vec<String> = ids
        .iter()
        .map(|id| format!("data-{id}-{}", if id % 3 == 0 { 'a' } else { 'b' }))
        .collect();
    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(StringArray::from(data)),
        ],
    )?)
}

fn order_line_batch() -> TestResult<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("ol_i_id", DataType::Int32, false),
        Field::new("ol_quantity", DataType::Int32, false),
        Field::new("ol_amount", DataType::Float64, false),
    ]));
    let rows: Vec<i32> = (0..ORDER_LINES).collect();
    let item_ids: Vec<i32> = rows.iter().map(|row| row % ITEMS + 1).collect();
    let quantities: Vec<i32> = rows.iter().map(|row| row % 7 + 1).collect();
    let amounts: Vec<f64> = rows.iter().map(|row| f64::from(row % 100) / 4.0).collect();
    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(item_ids)),
            Arc::new(Int32Array::from(quantities)),
            Arc::new(Float64Array::from(amounts)),
        ],
    )?)
}

/// One partition and a memory gate every build side here exceeds, with or
/// without the rewriter. Join reordering stays on: the swap it performs is
/// what puts a right column first in the join filter.
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

async fn sorted_rows(ctx: &SessionContext, sql: &str) -> TestResult<Vec<String>> {
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
    rows.sort_unstable();
    Ok(rows)
}

async fn plan_of(ctx: &SessionContext, sql: &str) -> TestResult<String> {
    let plan = ctx.sql(sql).await?.create_physical_plan().await?;
    Ok(displayable(plan.as_ref()).indent(true).to_string())
}

async fn a_swapped_join_filter_survives_the_sort_merge_rewrite_impl(
    fixture: TestFixture,
) -> TestResult<()> {
    let item = file_backed_table(&fixture, "item", item_batch()?).await?;
    let order_line = file_backed_table(&fixture, "order_line", order_line_batch()?).await?;

    let with_rewriter = session(true);
    let reference = session(false);
    for ctx in [&with_rewriter, &reference] {
        ctx.register_table("item", Arc::clone(&item) as Arc<dyn TableProvider>)?;
        ctx.register_table(
            "order_line",
            Arc::clone(&order_line) as Arc<dyn TableProvider>,
        )?;
    }

    for sql in [Q17_SAME_TYPE, Q17_LEFT_JOIN, Q17] {
        // The rewrite must be in play for the comparison to mean anything.
        let plan = plan_of(&with_rewriter, sql).await?;
        assert!(
            plan.contains("SortMergeJoin") && plan.contains("filter="),
            "precondition: the rewriter must turn this filtered join into a sort-merge join: {sql}\n{plan}"
        );

        let reference_rows = sorted_rows(&reference, sql).await?;
        assert_eq!(
            reference_rows.len(),
            1,
            "precondition: {sql} returns one row: {reference_rows:?}"
        );
        let got = sorted_rows(&with_rewriter, sql)
            .await
            .unwrap_or_else(|err| {
                panic!("{sql} failed under the sort-merge rewrite: {err}\n{plan}")
            });
        assert_eq!(
            got, reference_rows,
            "the sort-merge rewrite changed the answer to {sql}:\n{plan}"
        );
    }
    Ok(())
}

test_with_backends!(a_swapped_join_filter_survives_the_sort_merge_rewrite_impl);
