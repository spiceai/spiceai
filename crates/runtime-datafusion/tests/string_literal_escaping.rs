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

//! A string literal writes every quote character in its value twice, so an engine that reads a
//! backslash as a plain character (PostgreSQL, SQLite) reads the same value back. The fix is in
//! the vendored sqlparser (`vendor/sqlparser`, `impl Display for EscapeQuotedString`).

#![expect(
    clippy::expect_used,
    reason = "test helpers fail the test on unexpected plan or parse errors"
)]

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::MemTable;
use datafusion::prelude::{SessionContext, col, lit};
use datafusion::sql::sqlparser::ast::{
    BinaryOperator, Expr, SetExpr, Statement, Value, ValueWithSpan,
};
use datafusion::sql::sqlparser::dialect::{GenericDialect, PostgreSqlDialect};
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer};
use datafusion::sql::unparser::Unparser;
use datafusion::sql::unparser::dialect as unparser_dialect;

/// Values that hold a quote next to a backslash or to another quote, and a trailing backslash.
const VALUES: [&str; 9] = [
    "it's", "a\\b", "a\\'b", "a\\'", "a''b", "''", "'", "a\\", "a\\\\'b",
];

#[test]
fn a_string_literal_reads_back_as_its_value() {
    for value in VALUES {
        let sql = Value::SingleQuotedString(value.to_string()).to_string();
        let tokens = Tokenizer::new(&PostgreSqlDialect {}, &sql)
            .tokenize()
            .expect("tokenize");
        assert_eq!(
            tokens,
            vec![Token::SingleQuotedString(value.to_string())],
            "{value:?} written as {sql}"
        );
    }
}

/// The right side of the first `WHERE <column> = <literal>` in `sql`. `GenericDialect` reads the
/// double-quoted and backtick names the unparsers write and, like PostgreSQL and SQLite, reads a
/// backslash in a string literal as a plain character.
fn compared_value(sql: &str) -> String {
    let statements = Parser::parse_sql(&GenericDialect {}, sql)
        .unwrap_or_else(|e| panic!("{sql} does not parse: {e}"));
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
            op: BinaryOperator::Eq,
            right,
            ..
        } => match right.as_ref() {
            Expr::Value(ValueWithSpan {
                value: Value::SingleQuotedString(value),
                ..
            }) => value.clone(),
            other => panic!("{sql}: unexpected right side {other:?}"),
        },
        other => panic!("{sql}: unexpected WHERE {other:?}"),
    }
}

#[tokio::test]
async fn federated_sql_keeps_string_values() {
    let ctx = SessionContext::new();
    let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, true)]));
    ctx.register_table(
        "t",
        Arc::new(MemTable::try_new(schema, vec![vec![]]).expect("memtable")),
    )
    .expect("register");
    let dialects: [(&str, &dyn unparser_dialect::Dialect); 3] = [
        ("postgres", &unparser_dialect::PostgreSqlDialect {}),
        ("sqlite", &unparser_dialect::SqliteDialect {}),
        ("default", &unparser_dialect::DefaultDialect {}),
    ];
    for value in VALUES {
        let plan = ctx
            .table("t")
            .await
            .expect("table")
            .filter(col("s").eq(lit(value)))
            .expect("filter")
            .into_unoptimized_plan();
        for (name, dialect) in dialects {
            let sql = Unparser::new(dialect)
                .plan_to_sql(&plan)
                .expect("unparse")
                .to_string();
            assert_eq!(compared_value(&sql), value, "{name}: {sql}");
        }
    }
}
