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

//! `x NOT IN (subquery)` plans as a null-aware anti join: a NULL among the
//! subquery's values makes every `NOT IN` unknown, so no row qualifies, and a NULL
//! `x` never qualifies. `CayenneAntiJoinSortMergeRewriter` replaces an oversized
//! hash join with a sort-merge join, which has no null-aware mode, so it must leave
//! a null-aware join as it is.
//!
//! Every query runs in a session with the rewriter and in one without it, and the
//! rows must match. The memory gate is set low enough that every join here counts
//! as oversized, and one partition gives both join inputs the same partition
//! count, which the rewrite requires; that is the plan a deployment sized to one
//! CPU gets. A null-aware join always builds from the outer table; with
//! `join_reordering` off the plain anti join the test compares against builds from
//! it too, instead of being swapped to build from the two-row subquery.

#![allow(clippy::expect_used)]

use crate::common;

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
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

/// Rows in the outer table, so its build side is far past the memory gate below.
const OUTER_ROWS: i64 = 10_000;

async fn file_backed_table(
    fixture: &TestFixture,
    name: &str,
    column: &str,
    values: Vec<Option<i64>>,
) -> TestResult<Arc<CayenneTableProvider>> {
    let schema = Arc::new(Schema::new(vec![Field::new(column, DataType::Int64, true)]));
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let options = CreateTableOptions {
        table_name: name.to_string(),
        schema: Arc::clone(&schema),
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
    common::insert_batch(
        table.as_ref(),
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values))])?,
    )
    .await?;
    Ok(table)
}

/// One partition and a memory gate every build side here exceeds, with or without
/// the rewriter.
fn session(with_rewriter: bool) -> SessionContext {
    let mut cayenne = CayenneOptimizerConfig::default();
    cayenne.sort_merge_memory_pool_bytes = Some(1_024);
    let mut config = SessionConfig::new()
        .with_target_partitions(1)
        .with_option_extension(cayenne);
    config.options_mut().optimizer.join_reordering = false;
    // The rows below depend only on the join. A join dynamic filter pushed into the
    // subquery's scan drops its NULL before a hash join sees it
    // (apache/datafusion#23103, guarded in `datafusion_dynamic_filter_backports_test`),
    // which would make the reference wrong too.
    config
        .options_mut()
        .optimizer
        .enable_join_dynamic_filter_pushdown = false;
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

async fn not_in_keeps_null_aware_semantics_under_the_sort_merge_rewrite_impl(
    fixture: TestFixture,
) -> TestResult<()> {
    let mut outer: Vec<Option<i64>> = (1..=OUTER_ROWS).map(Some).collect();
    outer.push(None);
    let outer = file_backed_table(&fixture, "o", "id", outer).await?;
    let values_with_null =
        file_backed_table(&fixture, "i_null", "eid", vec![Some(5), None]).await?;
    let values = file_backed_table(&fixture, "i", "eid", vec![Some(5)]).await?;

    let with_rewriter = session(true);
    let reference = session(false);
    for ctx in [&with_rewriter, &reference] {
        ctx.register_table("o", Arc::clone(&outer) as Arc<dyn TableProvider>)?;
        ctx.register_table(
            "i_null",
            Arc::clone(&values_with_null) as Arc<dyn TableProvider>,
        )?;
        ctx.register_table("i", Arc::clone(&values) as Arc<dyn TableProvider>)?;
    }

    // The rewrite must fire where it is sound, or the comparisons below could
    // pass without it ever being in play: `NOT EXISTS` is a plain anti join.
    let not_exists =
        "SELECT COUNT(*) FROM o WHERE NOT EXISTS (SELECT 1 FROM i_null WHERE i_null.eid = o.id)";
    assert!(
        plan_of(&with_rewriter, not_exists)
            .await?
            .contains("SortMergeJoin"),
        "precondition: the rewriter must turn a plain anti join this large into a sort-merge join"
    );

    let cases = [
        // A NULL among the values: no row qualifies.
        (
            "SELECT COUNT(*) FROM o WHERE id NOT IN (SELECT eid FROM i_null)",
            "0",
        ),
        // No NULL among the values: every non-NULL id but 5 qualifies, and the
        // NULL id does not.
        (
            "SELECT COUNT(*) FROM o WHERE id NOT IN (SELECT eid FROM i)",
            "9999",
        ),
        // The plain anti join keeps the NULL id: it has no match.
        (not_exists, "10000"),
    ];
    let mut wrong = Vec::new();
    for (sql, expected) in cases {
        let got = sorted_rows(&with_rewriter, sql).await?;
        let reference_rows = sorted_rows(&reference, sql).await?;
        assert_eq!(
            reference_rows,
            vec![expected.to_string()],
            "precondition: the reference session must answer {sql} correctly"
        );
        if got != reference_rows {
            wrong.push(format!(
                "{sql}: got {got:?}, expected {reference_rows:?}\n{}",
                plan_of(&with_rewriter, sql).await?
            ));
        }
    }
    assert!(
        wrong.is_empty(),
        "the sort-merge rewrite changed the rows a NOT IN selects: {wrong:#?}"
    );
    Ok(())
}

test_with_backends!(not_in_keeps_null_aware_semantics_under_the_sort_merge_rewrite_impl);
