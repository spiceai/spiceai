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

//! A minus sign before an operand that starts with a minus sign is written with a space, because
//! `--` starts a comment. The fix is in the vendored sqlparser (`vendor/sqlparser`,
//! `impl Display for Expr`, `Expr::UnaryOp`).

#![expect(
    clippy::expect_used,
    reason = "test helpers fail the test on unexpected plan or parse errors"
)]

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::MemTable;
use datafusion::logical_expr::Expr as LogicalExpr;
use datafusion::prelude::{SessionContext, col, lit};
use datafusion::sql::sqlparser::ast::{
    BinaryOperator, Expr, SetExpr, Statement, UnaryOperator, Value, ValueWithSpan,
};
use datafusion::sql::sqlparser::dialect::{Dialect, GenericDialect, PostgreSqlDialect};
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::unparser::Unparser;
use datafusion::sql::unparser::dialect as unparser_dialect;

fn parse(dialect: &dyn Dialect, sql: &str) -> Vec<Statement> {
    Parser::parse_sql(dialect, sql).unwrap_or_else(|e| panic!("{sql} does not parse: {e}"))
}

#[test]
fn nested_minus_signs_read_back_as_written() {
    for sql in [
        "SELECT - -x",
        "SELECT - - -x",
        "SELECT - -1",
        "SELECT -(-x)",
        "SELECT 1 - -x",
        "SELECT -x",
    ] {
        for dialect in [&GenericDialect {} as &dyn Dialect, &PostgreSqlDialect {}] {
            let statements = parse(dialect, sql);
            let written = statements[0].to_string();
            assert_eq!(
                parse(dialect, &written),
                statements,
                "{sql} written as {written}"
            );
        }
    }
}

/// The number of minus signs in front of `expr` and the operand they apply to.
fn minus_signs(mut expr: &Expr) -> (usize, &Expr) {
    let mut count = 0;
    loop {
        match expr {
            Expr::Nested(inner) => expr = inner,
            Expr::UnaryOp {
                op: UnaryOperator::Minus,
                expr: inner,
            } => {
                count += 1;
                expr = inner;
            }
            _ => return (count, expr),
        }
    }
}

/// The two sides of the first `WHERE <left> > <right>` in `sql`. `GenericDialect` reads the
/// double-quoted and backtick names the unparsers write and, like PostgreSQL and SQLite, reads
/// `--` as the start of a comment.
fn compared_sides(sql: &str) -> (Expr, Expr) {
    let statements = parse(&GenericDialect {}, sql);
    let [Statement::Query(query)] = statements.as_slice() else {
        panic!("{sql} is not one query");
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        panic!("{sql} is not a select");
    };
    let mut selection = select.selection.as_ref().expect("a WHERE clause");
    while let Expr::Nested(inner) = selection {
        selection = inner;
    }
    match selection {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Gt,
            right,
        } => (left.as_ref().clone(), right.as_ref().clone()),
        other => panic!("{sql}: unexpected WHERE {other:?}"),
    }
}

fn negative(expr: LogicalExpr) -> LogicalExpr {
    LogicalExpr::Negative(Box::new(expr))
}

#[tokio::test]
async fn federated_sql_keeps_a_double_negation() {
    let ctx = SessionContext::new();
    let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true)]));
    ctx.register_table(
        "t",
        Arc::new(MemTable::try_new(schema, vec![vec![]]).expect("memtable")),
    )
    .expect("register");
    let plan = ctx
        .table("t")
        .await
        .expect("table")
        .filter(negative(negative(col("n"))).gt(negative(lit(-5_i64))))
        .expect("filter")
        .into_unoptimized_plan();
    let dialects: [(&str, &dyn unparser_dialect::Dialect); 3] = [
        ("postgres", &unparser_dialect::PostgreSqlDialect {}),
        ("sqlite", &unparser_dialect::SqliteDialect {}),
        ("default", &unparser_dialect::DefaultDialect {}),
    ];
    for (name, dialect) in dialects {
        let sql = Unparser::new(dialect)
            .plan_to_sql(&plan)
            .expect("unparse")
            .to_string();
        let (left, right) = compared_sides(&sql);
        let (count, operand) = minus_signs(&left);
        assert_eq!(count, 2, "{name}: {sql}");
        assert!(
            matches!(operand, Expr::CompoundIdentifier(parts)
                if parts.last().is_some_and(|ident| ident.value == "n")),
            "{name}: {sql}"
        );
        let (count, operand) = minus_signs(&right);
        assert_eq!(count, 2, "{name}: {sql}");
        assert!(
            matches!(
                operand,
                Expr::Value(ValueWithSpan {
                    value: Value::Number(number, _),
                    ..
                }) if number == "5"
            ),
            "{name}: {sql}"
        );
    }
}
