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
use datafusion::sql::sqlparser;
use datafusion::sql::sqlparser::ast::SetExpr;
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
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

    #[snafu(display("The refresh SQL query is invalid: {message}"))]
    InvalidSqlStatement { message: String },

    #[snafu(display("Missing expected SQL statement - this is a bug in Spice.ai"))]
    MissingStatement,
}

#[allow(clippy::module_name_repetitions)]
pub fn validate_refresh_sql(refresh_sql: &str) -> Result<()> {
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
                    if select.projection.len() != 1 {
                        InvalidSqlStatementSnafu {
                            message: "Expected a SQL query starting with SELECT *".to_string(),
                        }
                        .fail()?;
                    }

                    #[allow(clippy::match_on_vec_items)]
                    match select.projection[0] {
                        sqlparser::ast::SelectItem::Wildcard(_) => Ok(()),
                        _ => InvalidSqlStatementSnafu {
                            message: "Expected a SQL query starting with SELECT *".to_string(),
                        }
                        .fail()?,
                    }
                }
                _ => InvalidSqlStatementSnafu {
                    message: "Expected a SQL query starting with SELECT *".to_string(),
                }
                .fail()?,
            },
            _ => InvalidSqlStatementSnafu {
                message: "Expected a SQL query starting with SELECT *".to_string(),
            }
            .fail()?,
        },
        _ => InvalidSqlStatementSnafu {
            message: "Expected a SQL query starting with SELECT *".to_string(),
        }
        .fail()?,
    }
}
