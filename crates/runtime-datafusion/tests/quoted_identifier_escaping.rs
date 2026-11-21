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

//! A quoted identifier writes every quote character in its name twice, so the engine that
//! receives the SQL reads the same name back. The fix is in the vendored sqlparser
//! (`vendor/sqlparser`, `impl Display for Ident`).

#![expect(
    clippy::expect_used,
    reason = "test helpers fail the test on unexpected plan or parse errors"
)]

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{Expr, Ident, SelectItem, SetExpr, Statement};
use datafusion::sql::sqlparser::dialect::{
    Dialect, GenericDialect, MySqlDialect, PostgreSqlDialect,
};
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer};
use datafusion::sql::unparser::Unparser;
use datafusion::sql::unparser::dialect as unparser_dialect;

/// Names that hold the quote character `q` next to a backslash or to another quote character.
fn names_with(q: char) -> Vec<String> {
    vec![
        format!("a\\{q}b"),
        format!("a\\{q}"),
        format!("x{q}{q}y"),
        format!("{q}"),
        format!("{q}a{q}"),
        "a\\".to_string(),
    ]
}

#[test]
fn a_quoted_identifier_reads_back_as_its_name() {
    for q in ['"', '`', '\''] {
        for name in names_with(q) {
            let sql = Ident::with_quote(q, name.clone()).to_string();
            let tokens = Tokenizer::new(&GenericDialect {}, &sql)
                .tokenize()
                .expect("tokenize");
            let read_back = match tokens.as_slice() {
                [Token::Word(word)] => word.value.clone(),
                [Token::SingleQuotedString(value)] => value.clone(),
                other => panic!("{name:?} written as {sql} reads back as {other:?}"),
            };
            assert_eq!(read_back, name, "{name:?} written as {sql}");
        }
    }
}

fn table_with_column(name: &str) -> Arc<MemTable> {
    let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Int32, true)]));
    Arc::new(MemTable::try_new(schema, vec![vec![]]).expect("memtable"))
}

/// Unparses `sql` with `dialect`, parses the result with `parser_dialect`, and returns the alias
/// (or the last part of the column name) of the first projection item.
async fn first_output_name(
    sql: &str,
    column: &str,
    dialect: &dyn unparser_dialect::Dialect,
    parser_dialect: &dyn Dialect,
) -> String {
    let ctx = SessionContext::new();
    ctx.register_table("t", table_with_column(column))
        .expect("register");
    let plan = ctx.state().create_logical_plan(sql).await.expect("plan");
    let unparsed = Unparser::new(dialect)
        .plan_to_sql(&plan)
        .expect("unparse")
        .to_string();
    let statements = Parser::parse_sql(parser_dialect, &unparsed)
        .unwrap_or_else(|e| panic!("{unparsed} does not parse: {e}"));
    let [Statement::Query(query)] = statements.as_slice() else {
        panic!("{unparsed} is not one query");
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        panic!("{unparsed} is not a select");
    };
    match &select.projection[0] {
        SelectItem::ExprWithAlias { alias, .. } => alias.value.clone(),
        SelectItem::UnnamedExpr(Expr::Identifier(ident)) => ident.value.clone(),
        SelectItem::UnnamedExpr(Expr::CompoundIdentifier(idents)) => {
            idents.last().expect("compound identifier").value.clone()
        }
        other => panic!("{unparsed}: unexpected projection {other:?}"),
    }
}

#[tokio::test]
async fn federated_sql_keeps_aliases_and_column_names() {
    let postgres = unparser_dialect::PostgreSqlDialect {};
    let mysql = unparser_dialect::MySqlDialect {};
    for name in names_with('"') {
        let quoted = Ident::with_quote('"', name.clone()).to_string();
        let alias = first_output_name(
            &format!("SELECT id AS {quoted} FROM t"),
            "id",
            &postgres,
            &PostgreSqlDialect {},
        )
        .await;
        assert_eq!(alias, name, "Postgres alias");
        let column = first_output_name(
            &format!("SELECT {quoted} FROM t"),
            &name,
            &postgres,
            &PostgreSqlDialect {},
        )
        .await;
        assert_eq!(column, name, "Postgres column");
    }
    for name in names_with('`') {
        let quoted = Ident::with_quote('"', name.clone()).to_string();
        let alias = first_output_name(
            &format!("SELECT id AS {quoted} FROM t"),
            "id",
            &mysql,
            &MySqlDialect {},
        )
        .await;
        assert_eq!(alias, name, "MySQL alias");
    }
}
