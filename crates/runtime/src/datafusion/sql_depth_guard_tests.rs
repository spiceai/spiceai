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

//! [`super::sql_depth_guard`] on the runtime's real planner, on the worker
//! stack size `spiced` runs queries on.

use std::sync::Arc;

use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::sql::parser::DFParser;
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion::sql::sqlparser::parser::ParserError;

use super::sql_depth_guard::{
    self, MAX_QUERY_NESTING_DEPTH, MAX_SQL_DATA_TYPE_DEPTH, MAX_SQL_EXPR_DEPTH,
    MAX_UNPROTECTED_EXPR_DEPTH,
};

/// The deepest expression or set-operation tree the vendored `sqlparser`
/// builds (`MAX_AST_DEPTH` in `vendor/sqlparser/src/parser/mod.rs`).
const MAX_PARSED_TREE_DEPTH: usize = 2048;

/// `n` terms of `+1` after `head`: an expression tree `n` deep.
fn plus_chain(head: &str, n: usize) -> String {
    format!("{head}{}", "+1".repeat(n - 1))
}

/// Runs `task` on a Tokio worker with the default 2 MiB stack `spiced`'s
/// query runtimes run on, so a stack overflow aborts the test process instead
/// of passing. The test harness's own threads are larger (`RUST_MIN_STACK`).
async fn on_default_worker_stack<T: Send + 'static>(
    task: impl Future<Output = T> + Send + 'static,
) -> T {
    let workers = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .thread_stack_size(2 * 1024 * 1024)
        .enable_all()
        .build()
        .expect("worker runtime builds");
    let result = workers.spawn(task).await.expect("worker task completes");
    workers.shutdown_background();
    result
}

/// Plans `sql` through the runtime's planner on the default worker stack, so
/// an over-deep expression the guard lets through aborts the test process.
async fn plan_on_default_worker_stack(sql: String) -> DFResult<()> {
    let runtime = crate::Runtime::builder().build().await;
    let df = Arc::clone(&runtime.df);
    on_default_worker_stack(async move {
        let session = df.ctx.state();
        df.create_logical_plan(&session, &sql, None)
            .await
            .map(|_| ())
    })
    .await
}

fn is_parser_depth_error(err: &DataFusionError) -> bool {
    matches!(
        err.find_root(),
        DataFusionError::SQL(parser_error, _)
            if matches!(**parser_error, ParserError::RecursionLimitExceeded)
    )
}

/// Regression test: a flat `+` chain parses without deep recursion, so the
/// guard rejected it, but dropping the rejected tree recursed once per term
/// and overflowed the worker stack, aborting the process. The parser now
/// refuses to build it, on every path that parses query text: the query
/// planner, the table-function placeholder check that runs before it, and
/// the check a Flight statement update runs first.
#[tokio::test]
async fn over_deep_chains_are_parse_errors_on_the_worker_stack() {
    let runtime = crate::Runtime::builder().build().await;
    for terms in [31_000, 100_000] {
        let sql = format!("SELECT {}", plus_chain("1", terms));

        let err = plan_on_default_worker_stack(sql.clone())
            .await
            .expect_err("an over-deep chain does not plan");
        assert!(is_parser_depth_error(&err), "{terms} terms: {err}");

        let df = Arc::clone(&runtime.df);
        let query = sql.clone();
        let has_placeholders =
            on_default_worker_stack(async move { df.has_table_function_placeholders(&query) })
                .await;
        assert!(!has_placeholders, "{terms} terms");

        let err = on_default_worker_stack(async move {
            sql_depth_guard::ensure_sql_within_depth_limits(&sql)
        })
        .await
        .expect_err("an over-deep chain is refused");
        assert!(is_parser_depth_error(&err), "{terms} terms: {err}");
    }
}

/// The deepest expression the parser builds is refused by the guard, and
/// dropped, on the worker stack, after the table-function placeholder check
/// has walked it there too; one level deeper is a parse error.
#[tokio::test]
async fn chains_at_the_parser_bound_are_refused_on_the_worker_stack() {
    let at_the_bound = format!("SELECT {}", plus_chain("1", MAX_PARSED_TREE_DEPTH));

    let runtime = crate::Runtime::builder().build().await;
    let df = Arc::clone(&runtime.df);
    let query = at_the_bound.clone();
    let has_placeholders =
        on_default_worker_stack(async move { df.has_table_function_placeholders(&query) }).await;
    assert!(!has_placeholders);

    let err = plan_on_default_worker_stack(at_the_bound)
        .await
        .expect_err("a chain the parser builds is refused by the guard");
    assert_eq!(
        err.strip_backtrace(),
        format!(
            "Error during planning: Query rejected: a selected, grouped, ordered, or VALUES \
             expression is nested more than {MAX_UNPROTECTED_EXPR_DEPTH} levels deep. Simplify \
             the expression (for example, compute part of a long chain of +, || or AND in a \
             subquery)."
        )
    );

    let err = plan_on_default_worker_stack(format!(
        "SELECT {}",
        plus_chain("1", MAX_PARSED_TREE_DEPTH + 1)
    ))
    .await
    .expect_err("a chain past the parser bound is refused");
    assert!(is_parser_depth_error(&err), "{err}");
}

/// A chain of set operations is built in a loop like an operator chain, and
/// its tree is bounded the same way: the deepest one the parser builds is
/// planned and dropped on the worker stack, and one level deeper is a parse
/// error.
#[tokio::test]
async fn set_operation_chains_are_bounded_by_the_parser() {
    for (terms, past_the_bound) in [
        (MAX_PARSED_TREE_DEPTH, false),
        (MAX_PARSED_TREE_DEPTH + 1, true),
        (31_000, true),
    ] {
        let sql = vec!["SELECT 1"; terms].join(" UNION ALL ");
        let parsed = on_default_worker_stack(async move {
            DFParser::parse_sql_with_dialect(&sql, &PostgreSqlDialect {}).map(|_| ())
        })
        .await;
        assert_eq!(
            parsed.as_ref().is_err_and(is_parser_depth_error),
            past_the_bound,
            "{terms} UNION ALL terms: {parsed:?}"
        );
        assert_eq!(parsed.is_ok(), !past_the_bound, "{terms} UNION ALL terms");
    }

    for operator in ["UNION ALL", "UNION"] {
        let chain = |terms| vec!["SELECT 1 AS x"; terms].join(&format!(" {operator} "));
        plan_on_default_worker_stack(chain(MAX_PARSED_TREE_DEPTH))
            .await
            .unwrap_or_else(|e| panic!("a {operator} chain at the bound must plan: {e}"));
        let err = plan_on_default_worker_stack(chain(MAX_PARSED_TREE_DEPTH + 1))
            .await
            .expect_err("a chain past the bound does not plan");
        assert!(is_parser_depth_error(&err), "{operator}: {err}");
    }
}

/// Regression test: a `SELECT`, `ORDER BY`, or `VALUES` chain under the
/// general cap overflowed the worker stack while `DataFusion` rendered its
/// column name, aborting the process. It is now a planning error.
#[tokio::test]
async fn deep_named_chains_are_rejected_on_the_worker_stack() {
    let expected = format!(
        "Error during planning: Query rejected: a selected, grouped, ordered, or VALUES \
         expression is nested more than {MAX_UNPROTECTED_EXPR_DEPTH} levels deep. Simplify the \
         expression (for example, compute part of a long chain of +, || or AND in a subquery)."
    );
    for sql in [
        format!("SELECT {}", plus_chain("1", MAX_UNPROTECTED_EXPR_DEPTH + 1)),
        format!(
            "SELECT id FROM (VALUES (1)) t(id) ORDER BY {}",
            plus_chain("id", MAX_UNPROTECTED_EXPR_DEPTH + 1)
        ),
        format!(
            "SELECT * FROM (VALUES ({})) v(x)",
            plus_chain("1", MAX_UNPROTECTED_EXPR_DEPTH + 1)
        ),
    ] {
        let err = plan_on_default_worker_stack(sql)
            .await
            .expect_err("a deep named chain is rejected");
        assert_eq!(err.strip_backtrace(), expected);
    }
}

/// Chains at the named cap, and a predicate chain at the general cap, still
/// plan on the worker stack.
#[tokio::test]
async fn chains_at_the_caps_plan_on_the_worker_stack() {
    let or_terms = vec!["id = 1"; MAX_SQL_EXPR_DEPTH - 1].join(" OR ");
    for sql in [
        format!("SELECT {}", plus_chain("1", MAX_UNPROTECTED_EXPR_DEPTH)),
        format!(
            "SELECT id FROM (VALUES (1)) t(id) ORDER BY {}",
            plus_chain("id", MAX_UNPROTECTED_EXPR_DEPTH)
        ),
        format!("SELECT id FROM (VALUES (1)) t(id) WHERE {or_terms}"),
    ] {
        plan_on_default_worker_stack(sql.clone())
            .await
            .unwrap_or_else(|e| panic!("{sql:.60}… must plan: {e}"));
    }
}

/// `n` CTEs, each reading the one before it, then a query over the last:
/// queries nested `n + 1` deep.
fn cte_chain(n: usize) -> String {
    let mut ctes = vec!["c0 AS (SELECT 1 AS x)".to_string()];
    ctes.extend((1..n).map(|i| format!("c{i} AS (SELECT x FROM c{})", i - 1)));
    format!("WITH {} SELECT x FROM c{}", ctes.join(", "), n - 1)
}

/// A CTE chain at the query nesting cap plans on the worker stack, and one
/// level more is a planning error.
#[tokio::test]
async fn cte_chains_are_bounded_on_the_worker_stack() {
    plan_on_default_worker_stack(cte_chain(MAX_QUERY_NESTING_DEPTH - 1))
        .await
        .unwrap_or_else(|e| panic!("a CTE chain at the cap must plan: {e}"));
    let err = plan_on_default_worker_stack(cte_chain(MAX_QUERY_NESTING_DEPTH))
        .await
        .expect_err("a CTE chain over the cap is rejected");
    assert!(
        err.strip_backtrace().contains(&format!(
            "queries are nested more than {MAX_QUERY_NESTING_DEPTH} levels deep"
        )),
        "unexpected error: {err}"
    );
}

/// Regression test: a view's SQL was planned at load without the depth guard,
/// so a `WHERE` chain deeper than the planner handles overflowed the stack it
/// was planned on, and a deeply nested data type overflowed the stack it was
/// parsed on. Both views are now refused at load with the guard's error, and
/// the runtime keeps serving its datasets.
#[tokio::test]
async fn an_over_deep_view_is_refused_at_load() {
    let dir = tempfile::tempdir().expect("temporary directory");
    let csv = dir.path().join("events.csv");
    tokio::fs::write(&csv, "id\n1\n2\n3\n")
        .await
        .expect("source file is written");

    let mut view = spicepod::component::view::View::new("deep_view".to_string());
    view.sql = Some(format!(
        "SELECT id FROM depth_source WHERE {}",
        vec!["id > 0"; 2000].join(" AND ")
    ));
    let mut type_view = spicepod::component::view::View::new("deep_type_view".to_string());
    type_view.sql = Some(format!(
        "SELECT id FROM depth_source WHERE CAST(NULL AS {}INT{}) IS NULL",
        "ARRAY<".repeat(10_000),
        ">".repeat(10_000)
    ));
    let app = app::AppBuilder::new("view_depth")
        .with_dataset(spicepod::component::dataset::Dataset::new(
            format!("file:{}", csv.display()),
            "depth_source",
        ))
        .with_view(view)
        .with_view(type_view)
        .build();
    let rt = Arc::new(crate::Runtime::builder().with_app(app).build().await);
    tokio::time::timeout(
        std::time::Duration::from_mins(1),
        Arc::clone(&rt).load_components(),
    )
    .await
    .expect("components load");

    let view_status = |view: &str| {
        rt.status()
            .get_view_statuses()
            .into_iter()
            .find_map(|(name, status)| (name.table() == view).then_some(status))
    };
    for (view, expected) in [
        (
            "deep_view",
            format!(
                "Failed to attach view: Unable to create view: Error during planning: Query \
                 rejected: an expression is nested more than {MAX_SQL_EXPR_DEPTH} levels deep. \
                 Simplify the query (for example, replace a long chain of OR/AND conditions \
                 with an IN list or a join)."
            ),
        ),
        (
            "deep_type_view",
            format!(
                "Error during planning: Query rejected: a data type is nested more than \
                 {MAX_SQL_DATA_TYPE_DEPTH} levels deep. Simplify the type."
            ),
        ),
    ] {
        let mut status = view_status(view);
        for _ in 0..600 {
            if status.as_ref().is_some_and(|s| s.error_message().is_some()) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            status = view_status(view);
        }
        assert_eq!(
            status,
            Some(crate::status::ComponentStatus::error_with_message(expected)),
            "{view}"
        );
    }

    assert!(rt.status().is_ready_allowing_unhealthy());
    let rows = rt
        .df
        .ctx
        .sql("SELECT count(*) FROM depth_source")
        .await
        .expect("query plans")
        .collect()
        .await
        .expect("query runs");
    assert_eq!(
        datafusion::arrow::util::pretty::pretty_format_batches(&rows)
            .expect("batches format")
            .to_string(),
        "+----------+\n| count(*) |\n+----------+\n| 3        |\n+----------+"
    );
}
