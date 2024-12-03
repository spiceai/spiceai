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

use std::sync::Arc;

use datafusion::arrow::datatypes::Schema;
use datafusion::sql::parser::{DFParser, Statement};
use datafusion::sql::sqlparser::ast::{Expr, GroupByExpr, SelectItem, SetExpr};
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion::sql::{sqlparser, TableReference};
use itertools::Itertools;
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

    #[snafu(display("Expected a SQL query starting with SELECT <columns> FROM {expected_table}"))]
    InvalidSqlStatement { expected_table: TableReference },

    #[snafu(display(
        "Only column references are allowed in the SELECT clause of the refresh SQL, custom expressions and aliases are not supported.\nChange the SQL to only use columns references, i.e. SELECT col1, col2, col3 FROM {expected_table}"
    ))]
    OnlyColumnReferences { expected_table: TableReference },

    #[snafu(display(
        "The column '{column}' is not present in the source table '{expected_table}', valid columns are: {valid_columns}\nRewrite the SQL to only select columns that exist in the source table."
    ))]
    ColumnNotFoundInSource {
        column: Arc<str>,
        valid_columns: Arc<str>,
        expected_table: TableReference,
    },

    #[snafu(display("Missing expected SQL statement - this is a bug in Spice.ai"))]
    MissingStatement,
}

#[allow(clippy::too_many_lines)]
pub fn validate_refresh_sql(
    expected_table: TableReference,
    refresh_sql: &str,
    source_schema: Arc<Schema>,
) -> Result<Arc<Schema>> {
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
                    let refresh_schema = validate_select_columns(
                        &select.projection,
                        source_schema,
                        &expected_table,
                    )?;
                    ensure!(
                        select.from.len() == 1,
                        InvalidSqlStatementSnafu { expected_table }
                    );
                    ensure!(
                        select.cluster_by.is_empty(),
                        InvalidSqlStatementSnafu { expected_table }
                    );
                    ensure!(
                        select.connect_by.is_none(),
                        InvalidSqlStatementSnafu { expected_table }
                    );
                    ensure!(
                        select.distinct.is_none(),
                        InvalidSqlStatementSnafu { expected_table }
                    );
                    ensure!(
                        select.distribute_by.is_empty(),
                        InvalidSqlStatementSnafu { expected_table }
                    );
                    match &select.group_by {
                        GroupByExpr::All(modifiers) => {
                            ensure!(
                                modifiers.is_empty(),
                                InvalidSqlStatementSnafu { expected_table }
                            );
                        }
                        GroupByExpr::Expressions(exprs, modifiers) => {
                            ensure!(
                                exprs.is_empty(),
                                InvalidSqlStatementSnafu { expected_table }
                            );
                            ensure!(
                                modifiers.is_empty(),
                                InvalidSqlStatementSnafu { expected_table }
                            );
                        }
                    }
                    ensure!(
                        select.having.is_none(),
                        InvalidSqlStatementSnafu { expected_table }
                    );
                    ensure!(
                        select.into.is_none(),
                        InvalidSqlStatementSnafu { expected_table }
                    );
                    ensure!(
                        select.lateral_views.is_empty(),
                        InvalidSqlStatementSnafu { expected_table }
                    );
                    ensure!(
                        select.named_window.is_empty(),
                        InvalidSqlStatementSnafu { expected_table }
                    );
                    ensure!(
                        select.prewhere.is_none(),
                        InvalidSqlStatementSnafu { expected_table }
                    );
                    ensure!(
                        select.qualify.is_none(),
                        InvalidSqlStatementSnafu { expected_table }
                    );
                    ensure!(
                        select.sort_by.is_empty(),
                        InvalidSqlStatementSnafu { expected_table }
                    );
                    ensure!(
                        select.top.is_none(),
                        InvalidSqlStatementSnafu { expected_table }
                    );
                    ensure!(
                        select.value_table_mode.is_none(),
                        InvalidSqlStatementSnafu { expected_table }
                    );

                    match &select.from[0].relation {
                        sqlparser::ast::TableFactor::Table { name, .. } => {
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

                    Ok(refresh_schema)
                }
                _ => InvalidSqlStatementSnafu { expected_table }.fail()?,
            },
            _ => InvalidSqlStatementSnafu { expected_table }.fail()?,
        },
        _ => InvalidSqlStatementSnafu { expected_table }.fail()?,
    }
}

#[allow(clippy::too_many_lines)]
fn validate_select_columns(
    select: &Vec<SelectItem>,
    source_schema: Arc<Schema>,
    expected_table: &TableReference,
) -> Result<Arc<Schema>> {
    // Wildcard will select all columns
    if select.len() == 1 && matches!(select[0], SelectItem::Wildcard(_)) {
        return Ok(source_schema);
    }

    let mut fields = vec![];
    for select_item in select {
        match select_item {
            SelectItem::UnnamedExpr(expr) => match expr {
                Expr::Identifier(ident) => {
                    let column_name = ident.value.as_str();
                    let Ok(field) = source_schema.field_with_name(column_name) else {
                        return ColumnNotFoundInSourceSnafu {
                            column: Arc::from(column_name),
                            valid_columns: Arc::from(
                                source_schema.fields().iter().map(|f| f.name()).join(", "),
                            ),
                            expected_table: expected_table.clone(),
                        }
                        .fail();
                    };
                    fields.push(field.clone());
                }
                _ => {
                    return OnlyColumnReferencesSnafu {
                        expected_table: expected_table.clone(),
                    }
                    .fail();
                }
            },
            SelectItem::ExprWithAlias { .. }
            | SelectItem::QualifiedWildcard(..)
            | SelectItem::Wildcard(..) => {
                return OnlyColumnReferencesSnafu {
                    expected_table: expected_table.clone(),
                }
                .fail();
            }
        }
    }

    Ok(Arc::new(Schema::new(fields)))
}
