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

//! `IS [NOT] DISTINCT FROM` binds tighter than `AND` and `OR`.
//!
//! sqlparser 0.62.0 parsed the right operand with the lowest precedence, so
//! `a IS DISTINCT FROM b OR c` became `a IS DISTINCT FROM (b OR c)` and
//! returned wrong rows. The fix is the vendored sqlparser in `vendor/sqlparser`
//! (apache/datafusion-sqlparser-rs#2436). Every expected value below is what
//! PostgreSQL 17.5 returns for the same statement.

#![expect(
    clippy::expect_used,
    reason = "test helpers fail the test on unexpected parse or query errors"
)]

use arrow::util::display::array_value_to_string;
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{BinaryOperator, Expr, SelectItem, SetExpr, Statement};
use datafusion::sql::sqlparser::dialect::{Dialect, GenericDialect, PostgreSqlDialect};
use datafusion::sql::sqlparser::parser::Parser;
use runtime_datafusion::session_config::get_df_default_config;

/// Statements and the single value PostgreSQL returns for each.
const CASES: &[(&str, &str)] = &[
    // IS DISTINCT FROM followed by OR / AND, boolean operands.
    ("SELECT true IS DISTINCT FROM true OR true", "true"),
    ("SELECT true IS DISTINCT FROM false AND false", "false"),
    // IS NOT DISTINCT FROM followed by OR / AND, boolean operands.
    ("SELECT true IS NOT DISTINCT FROM false OR true", "true"),
    ("SELECT true IS NOT DISTINCT FROM true AND false", "false"),
    // Integer operands. These failed to plan with "Cannot infer common
    // argument type for logical boolean operation Int64 OR Boolean".
    (
        "SELECT 1 IS DISTINCT FROM 2 OR 3 IS DISTINCT FROM 3",
        "true",
    ),
    ("SELECT 1 IS DISTINCT FROM 1 OR 2 = 2", "true"),
    ("SELECT 1 IS NOT DISTINCT FROM 1 AND 2 = 3", "false"),
    (
        "SELECT 1 IS NOT DISTINCT FROM 2 OR 3 IS NOT DISTINCT FROM 3",
        "true",
    ),
    ("SELECT NULL IS DISTINCT FROM 1 OR false", "true"),
    // Filters: the trailing disjunct / conjunct must not be folded away.
    (
        "SELECT COUNT(*) FROM (VALUES (1,'a'),(2,'b'),(3,'a')) t(x,y) WHERE (x > 1) IS DISTINCT FROM true OR y = 'a'",
        "2",
    ),
    (
        "SELECT COUNT(*) FROM (VALUES (1,'a'),(2,'b'),(3,'a')) t(x,y) WHERE x IS DISTINCT FROM 2 AND y = 'a'",
        "2",
    ),
    (
        "SELECT COUNT(*) FROM (VALUES (1,'a'),(2,'b'),(3,'a')) t(x,y) WHERE x IS NOT DISTINCT FROM 2 OR y = 'a'",
        "3",
    ),
    (
        "SELECT COUNT(*) FROM (VALUES (1,'a'),(2,'b'),(3,'a')) t(x,y) WHERE x IS NOT DISTINCT FROM 3 AND y = 'a'",
        "1",
    ),
    // Parenthesized and tighter-binding forms are unchanged.
    ("SELECT (true IS DISTINCT FROM true) OR true", "true"),
    ("SELECT true IS DISTINCT FROM (true OR true)", "false"),
    ("SELECT 1 IS DISTINCT FROM 1 + 1", "true"),
    ("SELECT NOT 1 IS DISTINCT FROM 1", "true"),
];

async fn single_value(ctx: &SessionContext, sql: &str) -> String {
    let batches = ctx
        .sql(sql)
        .await
        .unwrap_or_else(|e| panic!("plan `{sql}`: {e}"))
        .collect()
        .await
        .unwrap_or_else(|e| panic!("run `{sql}`: {e}"));
    let values: Vec<String> = batches
        .iter()
        .flat_map(|b| {
            (0..b.num_rows())
                .map(move |row| array_value_to_string(b.column(0), row).expect("render value"))
        })
        .collect();
    assert_eq!(values.len(), 1, "`{sql}` returned {values:?}");
    values.into_iter().next().expect("one value")
}

#[tokio::test]
async fn is_distinct_from_returns_the_postgres_answer() {
    // Spice's session config, which parses with the PostgreSQL dialect.
    let spice = SessionContext::new_with_config(get_df_default_config());
    // DataFusion's default (generic dialect).
    let generic = SessionContext::new();
    let mut failures = Vec::new();
    for (name, ctx) in [("spice", &spice), ("generic", &generic)] {
        for &(sql, expected) in CASES {
            let got = single_value(ctx, sql).await;
            if got != expected {
                failures.push(format!(
                    "[{name}] `{sql}`: got {got}, PostgreSQL {expected}"
                ));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

fn select_expr(dialect: &dyn Dialect, sql: &str) -> Expr {
    let statements = Parser::parse_sql(dialect, sql).expect("parse");
    let Statement::Query(query) = &statements[0] else {
        panic!("not a query: {sql}");
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        panic!("not a select: {sql}");
    };
    match &select.projection[0] {
        SelectItem::UnnamedExpr(expr) => expr.clone(),
        other => panic!("unexpected projection {other:?}"),
    }
}

#[test]
fn and_or_after_is_distinct_from_is_the_root_of_the_expression() {
    let cases = [
        ("SELECT a IS DISTINCT FROM b OR c", BinaryOperator::Or, true),
        (
            "SELECT a IS DISTINCT FROM b AND c",
            BinaryOperator::And,
            true,
        ),
        (
            "SELECT a IS NOT DISTINCT FROM b OR c",
            BinaryOperator::Or,
            false,
        ),
        (
            "SELECT a IS NOT DISTINCT FROM b AND c",
            BinaryOperator::And,
            false,
        ),
        (
            "SELECT 1 IS DISTINCT FROM 2 OR 3 = 3",
            BinaryOperator::Or,
            true,
        ),
        (
            "SELECT 1 IS NOT DISTINCT FROM 2 AND 3 = 3",
            BinaryOperator::And,
            false,
        ),
    ];
    for dialect in [&PostgreSqlDialect {} as &dyn Dialect, &GenericDialect {}] {
        for (sql, op, distinct) in &cases {
            let expr = select_expr(dialect, sql);
            let Expr::BinaryOp { left, op: got, .. } = &expr else {
                panic!("{dialect:?}: `{sql}` parsed as {expr}, expected a {op} at the root");
            };
            assert_eq!(got, op, "{dialect:?}: `{sql}` parsed as {expr}");
            let left_ok = if *distinct {
                matches!(left.as_ref(), Expr::IsDistinctFrom(..))
            } else {
                matches!(left.as_ref(), Expr::IsNotDistinctFrom(..))
            };
            assert!(left_ok, "{dialect:?}: `{sql}` parsed as {expr}");
        }

        // Operators that bind tighter than IS stay in the right operand.
        for sql in [
            "SELECT a IS DISTINCT FROM b + 1",
            "SELECT a IS DISTINCT FROM b = c",
            "SELECT a IS DISTINCT FROM (b OR c)",
        ] {
            let expr = select_expr(dialect, sql);
            assert!(
                matches!(expr, Expr::IsDistinctFrom(..)),
                "{dialect:?}: `{sql}` parsed as {expr}"
            );
        }
    }
}
