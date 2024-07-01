/*
Copyright 2024 The Spice.ai OSS Authors

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

use datafusion::sql::parser::{DFParser, Statement};
use datafusion::sql::sqlparser::ast::SetExpr;
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion::sql::{sqlparser, TableReference};
use snafu::prelude::*;
use sqlparser::ast::Statement as SQLStatement;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to parse the refresh SQL: {source}"))]
    UnableToParseSql {
        source: sqlparser::parser::ParserError,
    },

    #[snafu(display(
        "Expected a single SQL statement for the refresh SQL, found {num_statements}"
    ))]
    ExpectedSingleSqlStatement { num_statements: usize },

    #[snafu(display("Expected a SQL query starting with SELECT * FROM {expected_table}"))]
    InvalidSqlStatement { expected_table: TableReference },

    #[snafu(display("Missing expected SQL statement - this is a bug in Spice.ai"))]
    MissingStatement,
}

pub fn validate_refresh_sql(expected_table: TableReference, refresh_sql: &str) -> Result<()> {
    let mut statements = DFParser::parse_sql_with_dialect(refresh_sql, &PostgreSqlDialect {})
        .context(UnableToParseSqlSnafu)?;
    if statements.len() != 1 {
        ExpectedSingleSqlStatementSnafu {
            num_statements: statements.len(),
        }
        .fail()?;
    }

    let statement = statements.pop_front().context(MissingStatementSnafu)?;
    match statement {
        Statement::Statement(statement) => match statement.as_ref() {
            SQLStatement::Query(query) => match query.body.as_ref() {
                SetExpr::Select(select) => {
                    ensure!(
                        select.projection.len() == 1,
                        InvalidSqlStatementSnafu { expected_table }
                    );
                    ensure!(
                        matches!(
                            select.projection[0],
                            sqlparser::ast::SelectItem::Wildcard(_)
                        ),
                        InvalidSqlStatementSnafu { expected_table }
                    );
                    ensure!(
                        select.from.len() == 1,
                        InvalidSqlStatementSnafu { expected_table }
                    );

                    match &select.from[0].relation {
                        sqlparser::ast::TableFactor::Table {
                            name,
                            alias: _,
                            args: _,
                            with_hints: _,
                            version: _,
                            partitions: _,
                        } => {
                            let table_name_with_schema = name
                                .0
                                .iter()
                                .map(|x| x.value.as_str())
                                .collect::<Vec<_>>()
                                .join(".");
                            ensure!(
                                TableReference::parse_str(&table_name_with_schema)
                                    == expected_table,
                                InvalidSqlStatementSnafu { expected_table }
                            );
                        }
                        _ => {
                            InvalidSqlStatementSnafu { expected_table }.fail()?;
                        }
                    }

                    Ok(())
                }
                _ => InvalidSqlStatementSnafu { expected_table }.fail()?,
            },
            _ => InvalidSqlStatementSnafu { expected_table }.fail()?,
        },
        _ => InvalidSqlStatementSnafu { expected_table }.fail()?,
    }
}
