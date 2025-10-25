/*
Copyright 2024-2025 The Spice.ai OSS Authors

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
use datafusion::common::DFSchema;
use datafusion::error::DataFusionError;
use datafusion::prelude::{Expr, SessionContext};
use datafusion::sql::parser::{DFParser, Statement};
use datafusion::sql::sqlparser::ast::{Delete, Expr as SQLExpr};
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion::sql::{TableReference, sqlparser};
use snafu::prelude::*;
use sqlparser::ast::Statement as SQLStatement;
use tokio::runtime::Handle;

use crate::datafusion::builder::get_df_default_config;
use runtime_object_store::registry::default_runtime_env;
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "The provided Retention SQL could not be parsed. {source} Check the SQL for syntax errors."
    ))]
    UnableToParseSql { source: DataFusionError },

    #[snafu(display(
        "Expected a single SQL statement for the retention SQL, found {num_statements}. Rewrite the SQL to only contain a single DELETE FROM statement."
    ))]
    ExpectedSingleSqlStatement { num_statements: usize },

    #[snafu(display("Expected a SQL query starting with DELETE FROM {expected_table}"))]
    InvalidSqlStatement { expected_table: TableReference },

    #[snafu(display(
        "DELETE statement must have a WHERE clause for retention SQL. Rewrite the SQL to include a WHERE clause, i.e. DELETE FROM {expected_table} WHERE column = 'value'"
    ))]
    MissingWhereClause { expected_table: TableReference },

    #[snafu(display(
        "Only DELETE statements are allowed in retention SQL. Rewrite the SQL to use DELETE FROM {expected_table} WHERE <condition>"
    ))]
    OnlyDeleteStatements { expected_table: TableReference },

    #[snafu(display(
        "The table '{table_name}' in the retention SQL does not match the expected table '{expected_table}'. Rewrite the SQL to use the correct table name."
    ))]
    TableMismatch {
        table_name: String,
        expected_table: TableReference,
    },

    #[snafu(display("Missing expected SQL statement - this is a bug in Spice.ai"))]
    MissingStatement,

    #[snafu(display("Failed to convert Arrow schema to DataFusion schema: {source}"))]
    SchemaConversion { source: DataFusionError },

    #[snafu(display("Failed to parse SQL expression '{expression}': {source}"))]
    ExpressionParsing {
        expression: String,
        source: Box<DataFusionError>,
    },
}

#[allow(clippy::result_large_err)]
pub fn parse_retention_sql(
    expected_table: &TableReference,
    retention_sql: &str,
    schema: Arc<Schema>,
) -> Result<Expr> {
    let mut statements = DFParser::parse_sql_with_dialect(retention_sql, &PostgreSqlDialect {})
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
            SQLStatement::Delete(Delete {
                from, selection, ..
            }) => {
                // Validate the table name matches
                validate_table_name(from, expected_table)?;

                // Extract and return the WHERE clause
                match selection {
                    Some(where_expr) => to_df_logical_expr(where_expr, schema),
                    None => MissingWhereClauseSnafu {
                        expected_table: expected_table.clone(),
                    }
                    .fail(),
                }
            }
            _ => OnlyDeleteStatementsSnafu {
                expected_table: expected_table.clone(),
            }
            .fail(),
        },
        _ => OnlyDeleteStatementsSnafu {
            expected_table: expected_table.clone(),
        }
        .fail(),
    }
}

#[allow(clippy::result_large_err)]
fn validate_table_name(
    from: &sqlparser::ast::FromTable,
    expected_table: &TableReference,
) -> Result<()> {
    let sqlparser::ast::FromTable::WithFromKeyword(from_tables) = from else {
        return InvalidSqlStatementSnafu {
            expected_table: expected_table.clone(),
        }
        .fail();
    };

    if from_tables.len() != 1 {
        return InvalidSqlStatementSnafu {
            expected_table: expected_table.clone(),
        }
        .fail();
    }

    match &from_tables[0].relation {
        sqlparser::ast::TableFactor::Table { name, .. } => {
            let table_name_with_schema = name
                .0
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(".");

            let table_ref = TableReference::parse_str(&table_name_with_schema);
            ensure!(
                table_ref == *expected_table,
                TableMismatchSnafu {
                    table_name: table_name_with_schema,
                    expected_table: expected_table.clone(),
                }
            );
        }
        _ => {
            InvalidSqlStatementSnafu {
                expected_table: expected_table.clone(),
            }
            .fail()?;
        }
    }

    Ok(())
}

#[allow(clippy::result_large_err)]
fn to_df_logical_expr(sql_expr: &SQLExpr, schema: Arc<Schema>) -> Result<Expr> {
    let df_schema = DFSchema::try_from(schema).context(SchemaConversionSnafu)?;

    let ctx = SessionContext::new_with_config_rt(
        get_df_default_config(),
        default_runtime_env(Handle::current()),
    );

    // To convert SQLExpr to DataFusion Expr, we need SqlToRel, which requires a ContextProvider.
    // SessionContextProvider used by DataFusion is not exposed publicly, so we provide the filter as a string
    // and let DataFusion handle the parsing and conversion instead of implementing our own ContextProvider.
    let expr_string = format!("{sql_expr}");
    ctx.state()
        .create_logical_expr(&expr_string, &df_schema)
        .map_err(Box::new)
        .context(ExpressionParsingSnafu {
            expression: expr_string,
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    fn create_test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("deleted", DataType::Boolean, true),
            Field::new("created_at", DataType::Utf8, true),
        ]))
    }

    #[tokio::test]
    async fn test_valid_delete_statement() -> Result<()> {
        let schema = create_test_schema();
        let table = TableReference::parse_str("test_table");
        let sql = "DELETE FROM test_table WHERE deleted = true";

        let result = parse_retention_sql(&table, sql, schema)?;
        // The result should be the WHERE clause expression
        assert!(matches!(result, Expr::BinaryExpr { .. }));
        Ok(())
    }

    #[tokio::test]
    async fn test_missing_where_clause() {
        let schema = create_test_schema();
        let table = TableReference::parse_str("test_table");
        let sql = "DELETE FROM test_table";

        let result = parse_retention_sql(&table, sql, schema);
        assert!(matches!(result, Err(Error::MissingWhereClause { .. })));
    }

    #[tokio::test]
    async fn test_wrong_table_name() {
        let schema = create_test_schema();
        let table = TableReference::parse_str("test_table");
        let sql = "DELETE FROM wrong_table WHERE deleted = true";

        let result = parse_retention_sql(&table, sql, schema);
        assert!(matches!(result, Err(Error::TableMismatch { .. })));
    }

    #[tokio::test]
    async fn test_select_statement_not_allowed() {
        let schema = create_test_schema();
        let table = TableReference::parse_str("test_table");
        let sql = "SELECT * FROM test_table WHERE deleted = true";

        let result = parse_retention_sql(&table, sql, schema);
        assert!(matches!(result, Err(Error::OnlyDeleteStatements { .. })));
    }

    #[tokio::test]
    async fn test_multiple_statements() {
        let schema = create_test_schema();
        let table = TableReference::parse_str("test_table");
        let sql =
            "DELETE FROM test_table WHERE deleted = true; DELETE FROM test_table WHERE old = true";

        let result = parse_retention_sql(&table, sql, schema);
        assert!(matches!(
            result,
            Err(Error::ExpectedSingleSqlStatement { .. })
        ));
    }

    #[tokio::test]
    async fn test_complex_where_clause() -> Result<()> {
        let schema = create_test_schema();
        let table = TableReference::parse_str("test_table");
        let sql = "DELETE FROM test_table WHERE deleted = true OR created_at < NOW() - INTERVAL '10 days'";

        let result = parse_retention_sql(&table, sql, schema)?;
        assert!(matches!(result, Expr::BinaryExpr { .. }));
        Ok(())
    }

    #[tokio::test]
    async fn test_qualified_table_name() -> Result<()> {
        let schema = create_test_schema();
        let table = TableReference::parse_str("schema.test_table");
        let sql = "DELETE FROM schema.test_table WHERE deleted = true";

        let result = parse_retention_sql(&table, sql, schema)?;
        assert!(matches!(result, Expr::BinaryExpr { .. }));
        Ok(())
    }

    #[tokio::test]
    async fn test_case_sensitive_table_and_column_names() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ID", DataType::Int64, false),
            Field::new("Deleted", DataType::Boolean, true),
            Field::new("Created_At", DataType::Utf8, true),
        ]));
        let table = TableReference::parse_str("Test_Table");
        let sql = "DELETE FROM Test_Table WHERE Deleted = true";

        let result = parse_retention_sql(&table, sql, schema)?;
        assert!(matches!(result, Expr::BinaryExpr { .. }));
        Ok(())
    }

    #[tokio::test]
    async fn test_quoted_table_and_column_names() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("is deleted", DataType::Boolean, true),
            Field::new("created at", DataType::Utf8, true),
        ]));
        let table = TableReference::parse_str("\"Test Table\"");
        let sql = "DELETE FROM \"Test Table\" WHERE \"is deleted\" = true";

        let result = parse_retention_sql(&table, sql, schema)?;
        assert!(matches!(result, Expr::BinaryExpr { .. }));
        Ok(())
    }

    #[tokio::test]
    async fn test_nonexistent_column() {
        let schema = create_test_schema();
        let table = TableReference::parse_str("test_table");
        let sql = "DELETE FROM test_table WHERE nonexistent_column = true";

        let result = parse_retention_sql(&table, sql, schema);
        assert!(matches!(result, Err(Error::ExpressionParsing { .. })));
    }
}
