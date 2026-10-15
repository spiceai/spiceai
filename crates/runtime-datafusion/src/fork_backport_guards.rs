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

//! Guards for upstream `DataFusion` wrong-results fixes that the `spiceai-54` fork
//! carries as backports (see `docs/dev/fork_patches.md`). Each test runs the
//! fix's upstream regression query in a session built from the configuration
//! every Spice session starts from, and fails if the backport is lost when the
//! fork is re-cut.

use std::path::Path;

use arrow::util::display::array_value_to_string;
use datafusion::prelude::{SessionConfig, SessionContext};

use crate::session_config::get_df_default_config;

/// A session built from the configuration every Spice session starts from.
fn spice_session(configure: impl FnOnce(&mut SessionConfig)) -> SessionContext {
    let mut config = get_df_default_config();
    configure(&mut config);
    SessionContext::new_with_config(config)
}

async fn run_statements(ctx: &SessionContext, statements: &[&str]) {
    for statement in statements {
        ctx.sql(statement)
            .await
            .expect("setup statement plans")
            .collect()
            .await
            .expect("setup statement runs");
    }
}

/// The rows `sql` returns, in order, rendered as `a,b,…` with NULL spelled out,
/// or the error it fails with.
async fn answer(ctx: &SessionContext, sql: &str) -> Result<Vec<String>, String> {
    let batches = ctx
        .sql(sql)
        .await
        .map_err(|error| error.to_string())?
        .collect()
        .await
        .map_err(|error| error.to_string())?;
    let mut rows = Vec::new();
    for batch in &batches {
        for row in 0..batch.num_rows() {
            let mut rendered = Vec::with_capacity(batch.num_columns());
            for column in batch.columns() {
                rendered.push(if column.is_null(row) {
                    "NULL".to_string()
                } else {
                    array_value_to_string(column, row).map_err(|error| error.to_string())?
                });
            }
            rows.push(rendered.join(","));
        }
    }
    Ok(rows)
}

/// [`answer`], sorted, for a query whose row order SQL does not fix.
async fn sorted_answer(ctx: &SessionContext, sql: &str) -> Result<Vec<String>, String> {
    let mut rows = answer(ctx, sql).await?;
    rows.sort_unstable();
    Ok(rows)
}

fn sorted(rows: &[&str]) -> Vec<String> {
    let mut rows: Vec<String> = rows.iter().map(ToString::to_string).collect();
    rows.sort_unstable();
    rows
}

fn owned(rows: &[&str]) -> Vec<String> {
    rows.iter().map(ToString::to_string).collect()
}

/// apache/datafusion#22810. `NOT IN (subquery)` is a null-aware anti join, and
/// `SortMergeJoinExec` has no null-aware mode; with `prefer_hash_join` off the
/// planner picked it anyway, and the NULL in the subquery stopped excluding rows.
#[tokio::test(flavor = "multi_thread")]
async fn not_in_stays_a_hash_join_when_sort_merge_joins_are_preferred() {
    let ctx = spice_session(|config| {
        config.options_mut().optimizer.prefer_hash_join = false;
        config.options_mut().execution.target_partitions = 4;
    });
    run_statements(
        &ctx,
        &[
            "CREATE TABLE nia_left(x INT) AS VALUES (1), (2), (3), (4)",
            "CREATE TABLE nia_right(y INT) AS VALUES (2), (NULL)",
        ],
    )
    .await;
    assert_eq!(
        answer(
            &ctx,
            "SELECT x FROM nia_left WHERE x NOT IN (SELECT y FROM nia_right) ORDER BY x"
        )
        .await,
        Ok(Vec::new()),
        "a NULL among the subquery's values leaves no row selected"
    );
}

/// apache/datafusion#23684. The `TopK` aggregation over `max(y)` dropped the
/// groups whose maximum is NULL.
#[tokio::test(flavor = "multi_thread")]
async fn topk_aggregation_keeps_groups_whose_max_is_null() {
    let ctx = spice_session(|_| {});
    run_statements(
        &ctx,
        &["CREATE TABLE topk_mixed(s VARCHAR, y BIGINT) AS VALUES ('n1', CAST(NULL AS BIGINT)), ('n2', CAST(NULL AS BIGINT)), ('v1', 10), ('v2', 20), ('v3', 30)"],
    )
    .await;
    let top = |order: &str| {
        format!(
            "SELECT max_y FROM (SELECT s, max(y) AS max_y FROM topk_mixed GROUP BY s) ORDER BY max_y {order} LIMIT 4"
        )
    };
    assert_eq!(
        answer(&ctx, &top("DESC NULLS LAST")).await,
        Ok(owned(&["30", "20", "10", "NULL"]))
    );
    assert_eq!(
        answer(&ctx, &top("DESC NULLS FIRST")).await,
        Ok(owned(&["NULL", "NULL", "30", "20"]))
    );
}

/// apache/datafusion#24247. The simplifier folded `log(a, 1)`, `log(a, a)`,
/// `power(a, 0)` and their kin to constants, which is wrong where `a` is NULL.
#[tokio::test(flavor = "multi_thread")]
async fn log_and_power_simplification_keeps_null() {
    let ctx = spice_session(|_| {});
    assert_eq!(
        answer(
            &ctx,
            "SELECT log(a, 1) IS NULL, log(a, a) IS NULL, log(a, power(a, b)) IS NULL, power(a, 0) IS NULL, power(a, log(a, b)) IS NULL FROM (VALUES (CAST(NULL AS DOUBLE), CAST(2.0 AS DOUBLE))) AS t(a, b)"
        )
        .await,
        Ok(owned(&["true,true,true,true,true"]))
    );
}

/// apache/datafusion#24248. The simplifier folded `(a XOR b) XOR a` to `b` and
/// `a XOR a` to `0`, which is wrong where `a` is NULL.
#[tokio::test(flavor = "multi_thread")]
async fn xor_simplification_keeps_null() {
    let ctx = spice_session(|_| {});
    assert_eq!(
        sorted_answer(
            &ctx,
            "SELECT (a # b) # a AS l, a # (b # a) AS r, a # a AS s FROM (VALUES (CAST(NULL AS INT), 7), (3, 7)) AS t(a, b)"
        )
        .await,
        Ok(sorted(&["7,7,0", "NULL,NULL,NULL"]))
    );
}

/// apache/datafusion#24380. The simplifier folded `col ~ '.*'` to `true`, which
/// is wrong where `col` is NULL.
#[tokio::test(flavor = "multi_thread")]
async fn regex_match_all_simplification_keeps_null() {
    let ctx = spice_session(|_| {});
    assert_eq!(
        answer(
            &ctx,
            "WITH vals(id, col) AS (VALUES (1, 'foo'), (2, ''), (3, CAST(NULL AS VARCHAR))) SELECT col, col ~ '.*' FROM vals ORDER BY id"
        )
        .await,
        Ok(owned(&["foo,true", ",true", "NULL,NULL"]))
    );
}

/// apache/datafusion#24686. Merging nested projections that each redefine `i`
/// substituted the wrong layer, so six `i + 1` layers added three.
#[tokio::test(flavor = "multi_thread")]
async fn nested_projections_keep_every_layer() {
    let ctx = spice_session(|_| {});
    run_statements(&ctx, &["CREATE TABLE np(i INT) AS VALUES (3), (4), (5)"]).await;
    assert_eq!(
        sorted_answer(
            &ctx,
            "SELECT i FROM (SELECT i + 1 AS i FROM (SELECT i + 1 AS i FROM (SELECT i + 1 AS i FROM (SELECT i + 1 AS i FROM (SELECT i + 1 AS i FROM (SELECT i + 1 AS i FROM np))))))"
        )
        .await,
        Ok(sorted(&["9", "10", "11"]))
    );
}

/// apache/datafusion#24958. A sort pushed below a `GlobalLimitExec` ignored its
/// `skip`, so `LIMIT 4 OFFSET 3` returned one row. The inner limit has no order,
/// so which four rows it keeps is not fixed; how many it keeps is.
#[tokio::test(flavor = "multi_thread")]
async fn a_sort_over_limit_offset_keeps_every_row() {
    let ctx = spice_session(|_| {});
    run_statements(
        &ctx,
        &["CREATE TABLE t1(a INT) AS VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)"],
    )
    .await;
    let rows = answer(
        &ctx,
        "SELECT * FROM (SELECT a FROM t1 LIMIT 4 OFFSET 3) ORDER BY a",
    )
    .await;
    assert_eq!(rows.map(|rows| rows.len()), Ok(4));
}

/// apache/datafusion#24997. `COUNT` with `ORDER BY` counted only its first
/// argument, and grouped it panicked.
#[tokio::test(flavor = "multi_thread")]
async fn count_with_order_by_counts_every_argument() {
    let ctx = spice_session(|_| {});
    run_statements(
        &ctx,
        &["CREATE TABLE count_order_by_t(a INT, b INT, c INT) AS VALUES (1, NULL, 10), (2, 20, 10), (3, 30, 20), (NULL, 40, 20)"],
    )
    .await;
    assert_eq!(
        answer(&ctx, "SELECT COUNT(a, c ORDER BY b) FROM count_order_by_t").await,
        Ok(owned(&["3"]))
    );
    assert_eq!(
        sorted_answer(
            &ctx,
            "SELECT c, COUNT(a ORDER BY b) FROM count_order_by_t GROUP BY c"
        )
        .await,
        Ok(sorted(&["10,2", "20,1"]))
    );
}

/// apache/datafusion#25348. `3 NOT IN (subquery)` compares a constant, and its
/// plan ignored a NULL among the subquery's values.
#[tokio::test(flavor = "multi_thread")]
async fn constant_not_in_sees_the_subquerys_null() {
    let ctx = spice_session(|_| {});
    run_statements(
        &ctx,
        &[
            "CREATE TABLE naconst_t1(id INT) AS VALUES (1), (2)",
            "CREATE TABLE naconst_t2(id INT) AS VALUES (1), (NULL)",
        ],
    )
    .await;
    assert_eq!(
        answer(
            &ctx,
            "SELECT id FROM naconst_t1 WHERE 3 NOT IN (SELECT id FROM naconst_t2) ORDER BY id"
        )
        .await,
        Ok(Vec::new())
    );
}

/// A correlated `NOT IN (subquery)` in a `WHERE` clause is planned as the
/// `NOT EXISTS` it equals there. The fork's null-aware hash join takes a single
/// key and checks the subquery's NULLs over every row, so with a correlation it
/// applied the NULL rules to the correlation key, missed that the correlation
/// excludes a NULL, or failed to plan. The expected rows are `SQLite`'s.
#[tokio::test(flavor = "multi_thread")]
async fn correlated_not_in_is_answered_as_not_exists() {
    let ctx = spice_session(|_| {});
    run_statements(
        &ctx,
        &[
            "CREATE TABLE nacorr_t1(id INT, g INT) AS VALUES (1, 1), (2, 2), (3, NULL), (NULL, 1)",
            "CREATE TABLE nacorr_t2(id INT, g INT) AS VALUES (1, 1), (NULL, 2), (4, NULL), (2, 3)",
        ],
    )
    .await;
    let cases: [(&str, &[&str]); 4] = [
        (
            "SELECT id FROM nacorr_t1 WHERE 3 NOT IN \
             (SELECT nacorr_t2.id FROM nacorr_t2 WHERE nacorr_t2.g = nacorr_t1.g)",
            &["1", "3", "NULL"],
        ),
        (
            "SELECT id FROM nacorr_t1 WHERE nacorr_t1.id NOT IN \
             (SELECT nacorr_t2.id FROM nacorr_t2 WHERE nacorr_t2.g = nacorr_t1.g)",
            &["3"],
        ),
        (
            "SELECT id FROM nacorr_t1 WHERE 3 NOT IN \
             (SELECT nacorr_t2.id FROM nacorr_t2 WHERE nacorr_t2.g > nacorr_t1.g)",
            &["2", "3"],
        ),
        (
            "SELECT id FROM nacorr_t1 WHERE nacorr_t1.id NOT IN \
             (SELECT nacorr_t2.id FROM nacorr_t2 WHERE nacorr_t2.g > nacorr_t1.g)",
            &["3"],
        ),
    ];
    let mut answers = Vec::with_capacity(cases.len());
    for (sql, _) in cases {
        answers.push((sql, sorted_answer(&ctx, sql).await));
    }
    let expected: Vec<_> = cases
        .iter()
        .map(|(sql, rows)| (*sql, Ok(sorted(rows))))
        .collect();
    assert_eq!(answers, expected);
}

/// apache/datafusion#24516. A scalar subquery that returns no rows is NULL, but its
/// nullability came from its projected field, so `IS NULL` over one whose field is
/// non-nullable was folded to `false`.
#[tokio::test(flavor = "multi_thread")]
async fn an_empty_scalar_subquery_is_null() {
    let ctx = spice_session(|_| {});
    run_statements(
        &ctx,
        &[
            "CREATE TABLE essq_t(id INT) AS VALUES (1), (2)",
            "CREATE TABLE essq_u(z INT NOT NULL)",
        ],
    )
    .await;
    assert_eq!(
        (
            answer(&ctx, "SELECT (SELECT 1 WHERE FALSE) IS NULL").await,
            sorted_answer(
                &ctx,
                "SELECT id FROM essq_t WHERE (SELECT z FROM essq_u) IS NULL"
            )
            .await,
        ),
        (Ok(owned(&["true"])), Ok(sorted(&["1", "2"])))
    );
}

/// apache/datafusion#23429, which #24516 is written against. `x IN (subquery)` is
/// NULL when `x` matches no row and the subquery holds a NULL, but its nullability
/// came from `x` alone, and the simplifier folds on it (`A = A` to `true`).
#[tokio::test(flavor = "multi_thread")]
async fn an_in_subquery_over_a_nullable_column_is_nullable() {
    let ctx = spice_session(|_| {});
    run_statements(
        &ctx,
        &[
            "CREATE TABLE innl_s(c INT NOT NULL) AS VALUES (1), (2)",
            "CREATE TABLE innl_t(a INT) AS VALUES (2), (NULL)",
        ],
    )
    .await;
    let plan = ctx
        .sql("SELECT c IN (SELECT a FROM innl_t) AS in_t FROM innl_s")
        .await
        .expect("the query plans");
    assert!(
        plan.schema()
            .field_with_unqualified_name("in_t")
            .expect("the IN column")
            .is_nullable(),
        "an IN over a subquery that can hold a NULL is nullable"
    );
}

/// apache/datafusion#25227. Parquet column statistics were cast along with the
/// column, so `MAX(CAST(a AS INT))` over the strings `'1'`, `'100'`, `'2'` was
/// answered from the string maximum `'2'`.
#[tokio::test(flavor = "multi_thread")]
async fn a_cast_aggregate_is_not_answered_from_uncast_statistics() {
    let dir = tempfile::tempdir().expect("temporary directory");
    let path = dir.path().join("cast_strings.parquet");
    let ctx = spice_session(|_| {});
    write_parquet(
        &ctx,
        "SELECT * FROM (VALUES ('1'), ('100'), ('2')) AS t(a)",
        &path,
    )
    .await;
    run_statements(
        &ctx,
        &[&format!(
            "CREATE EXTERNAL TABLE cast_stats_strings STORED AS PARQUET LOCATION '{}'",
            path.display()
        )],
    )
    .await;
    assert_eq!(
        answer(
            &ctx,
            "SELECT MIN(CAST(a AS INT)), MAX(CAST(a AS INT)) FROM cast_stats_strings"
        )
        .await,
        Ok(owned(&["1,100"]))
    );
}

async fn write_parquet(ctx: &SessionContext, select: &str, path: &Path) {
    run_statements(
        ctx,
        &[&format!(
            "COPY ({select}) TO '{}' STORED AS PARQUET",
            path.display()
        )],
    )
    .await;
}

/// spiceai/datafusion#252. `FilterPushdown` gives a hash join its dynamic filter
/// after `JoinSelection` has chosen the build side, and `HashJoinExec::swap_inputs`
/// rejects a join that has one. A plan that is optimized twice (a table provider
/// that returns an optimized sub-plan from `scan`, as `vector_search` does) runs
/// `JoinSelection` again on such a join, so the join must keep its order.
#[test]
fn join_selection_keeps_the_order_of_a_join_with_a_dynamic_filter() {
    use arrow::array::{Int32Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::config::ConfigOptions;
    use datafusion::common::{JoinType, NullEquality};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::physical_expr::PhysicalExpr;
    use datafusion::physical_expr::expressions::{Column, DynamicFilterPhysicalExpr, lit};
    use datafusion::physical_optimizer::PhysicalOptimizerRule;
    use datafusion::physical_optimizer::join_selection::JoinSelection;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
    use std::sync::Arc;

    let source = |name: &str, rows: i32| -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from_iter_values(0..rows))],
        )
        .expect("batch matches its schema");
        MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None)
            .expect("memory source builds")
    };
    // The build (left) side is bigger, so statistics alone would swap the inputs.
    let (big, small) = (source("big_col", 10_000), source("small_col", 10));
    let probe_key: Arc<dyn PhysicalExpr> = Arc::new(Column::new("small_col", 0));
    let join = |dynamic_filter: Option<Arc<DynamicFilterPhysicalExpr>>| {
        let join = HashJoinExec::try_new(
            Arc::clone(&big),
            Arc::clone(&small),
            vec![(Arc::new(Column::new("big_col", 0)), Arc::clone(&probe_key))],
            None,
            &JoinType::Inner,
            None,
            PartitionMode::CollectLeft,
            NullEquality::NullEqualsNothing,
            false,
        )
        .expect("join builds");
        let join = match dynamic_filter {
            Some(filter) => join
                .with_dynamic_filter_expr(filter)
                .expect("filter matches the probe side"),
            None => join,
        };
        Arc::new(join) as Arc<dyn ExecutionPlan>
    };
    let select = |plan| {
        JoinSelection::new()
            .optimize(plan, &ConfigOptions::new())
            .expect("join selection succeeds")
    };

    // Without a dynamic filter the bigger build side is swapped behind a projection.
    assert!(
        select(join(None)).downcast_ref::<HashJoinExec>().is_none(),
        "the join should swap when it has no dynamic filter"
    );

    let filter = Arc::new(DynamicFilterPhysicalExpr::new(
        vec![Arc::clone(&probe_key)],
        lit(true),
    ));
    let kept = select(join(Some(filter)));
    let kept = kept
        .downcast_ref::<HashJoinExec>()
        .expect("a join with a dynamic filter keeps its inputs");
    assert_eq!(kept.left().schema().field(0).name(), "big_col");
    assert_eq!(kept.dynamic_expressions_produced().len(), 1);
}
