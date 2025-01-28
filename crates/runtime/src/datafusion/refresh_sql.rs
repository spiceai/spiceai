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

use arrow_schema::SchemaRef;
use arrow_tools::schema::schema_meta_get_computed_columns;
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
    #[snafu(display(
        "The provided Refresh SQL could not be parsed.\n{source}\nCheck the SQL for syntax errors."
    ))]
    UnableToParseSql {
        source: sqlparser::parser::ParserError,
    },

    #[snafu(display(
        "Expected a single SQL statement for the refresh SQL, found {num_statements}.\nRewrite the SQL to only contain a single SELECT statement."
    ))]
    ExpectedSingleSqlStatement { num_statements: usize },

    #[snafu(display("Expected a SQL query starting with SELECT <columns> FROM {expected_table}"))]
    InvalidSqlStatement { expected_table: TableReference },

    #[snafu(display("Unexpected '{expr}' in the Refresh SQL statement.\nRewrite the SQL to only perform WHERE filters, i.e. SELECT col1, col2, col3 FROM {expected_table} WHERE col1 = 'foo'"))]
    UnexpectedExpression {
        expr: &'static str,
        expected_table: TableReference,
    },

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

macro_rules! ensure_no_expr {
    ($condition:expr, $expr_name:expr, $expected_table:expr) => {
        ensure!(
            $condition,
            UnexpectedExpressionSnafu {
                expr: $expr_name,
                expected_table: $expected_table.clone(),
            }
        );
    };
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
            SQLStatement::Query(query) => {
                ensure_no_expr!(query.fetch.is_none(), "FETCH", expected_table);
                ensure_no_expr!(query.offset.is_none(), "OFFSET", expected_table);
                ensure_no_expr!(query.with.is_none(), "WITH", expected_table);
                ensure_no_expr!(query.order_by.is_none(), "ORDER BY", expected_table);
                ensure_no_expr!(query.for_clause.is_none(), "FOR", expected_table);
                ensure_no_expr!(query.limit_by.is_empty(), "LIMIT BY", expected_table);
                ensure_no_expr!(query.format_clause.is_none(), "FORMAT", expected_table);
                ensure_no_expr!(query.settings.is_none(), "SETTINGS", expected_table);

                match query.body.as_ref() {
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

                        ensure_no_expr!(select.cluster_by.is_empty(), "CLUSTER BY", expected_table);
                        ensure_no_expr!(select.connect_by.is_none(), "CONNECT BY", expected_table);
                        ensure_no_expr!(select.distinct.is_none(), "DISTINCT", expected_table);
                        ensure_no_expr!(
                            select.distribute_by.is_empty(),
                            "DISTRIBUTE BY",
                            expected_table
                        );

                        match &select.group_by {
                            GroupByExpr::All(modifiers) => {
                                ensure_no_expr!(modifiers.is_empty(), "GROUP BY", expected_table);
                            }
                            GroupByExpr::Expressions(exprs, modifiers) => {
                                ensure_no_expr!(exprs.is_empty(), "GROUP BY", expected_table);
                                ensure_no_expr!(modifiers.is_empty(), "GROUP BY", expected_table);
                            }
                        }

                        ensure_no_expr!(select.having.is_none(), "HAVING", expected_table);
                        ensure_no_expr!(select.into.is_none(), "INTO", expected_table);
                        ensure_no_expr!(
                            select.lateral_views.is_empty(),
                            "LATERAL VIEW",
                            expected_table
                        );
                        ensure_no_expr!(select.named_window.is_empty(), "WINDOW", expected_table);
                        ensure_no_expr!(select.prewhere.is_none(), "PREWHERE", expected_table);
                        ensure_no_expr!(select.qualify.is_none(), "QUALIFY", expected_table);
                        ensure_no_expr!(select.sort_by.is_empty(), "SORT BY", expected_table);
                        ensure_no_expr!(select.top.is_none(), "TOP", expected_table);
                        ensure_no_expr!(
                            select.value_table_mode.is_none(),
                            "AS VALUE",
                            expected_table
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
                }
            }
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

    // If the refresh SQL defines a subset of columns to fetch, computed columns (e.g., embeddings)
    // are not included automatically. We verify their presence in the source schema and add them manually if needed.
    fields = include_computed_columns(&fields, &source_schema);

    Ok(Arc::new(Schema::new(fields)))
}

/// Checks the source schema for associated computed columns (e.g., embeddings)
/// and adds any missing computed fields to the target schema if they are found.
fn include_computed_columns(
    fields: &[arrow_schema::Field],
    source_schema: &SchemaRef,
) -> Vec<arrow_schema::Field> {
    let mut extended_fields = fields.to_owned();
    for field in fields {
        if let Some(computed_cols) = schema_meta_get_computed_columns(source_schema, field.name()) {
            for computed_col in computed_cols {
                // Add field only if it does not exist in target schema
                if !extended_fields
                    .iter()
                    .any(|f| f.name() == computed_col.name())
                {
                    extended_fields.push((*computed_col).clone());
                }
            }
        }
    }

    extended_fields
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    fn create_test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, true),
        ]))
    }

    fn create_test_schema_with_enmbeddings() -> Arc<Schema> {
        let mut schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, true),
            Field::new(
                "name_embedding",
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::FixedSizeList(
                        Arc::new(Field::new("item", DataType::Float32, false)),
                        1536,
                    ),
                    false,
                ))),
                false,
            ),
            Field::new(
                "name_offset",
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::FixedSizeList(
                        Arc::new(Field::new("item", DataType::Int32, false)),
                        2,
                    ),
                    false,
                ))),
                false,
            ),
        ]);

        // mark `name_embedding` and `name_offset` as computed columns for `name`
        let mut computed_columns_meta = std::collections::HashMap::new();
        computed_columns_meta.insert(
            "name".to_string(),
            vec!["name_embedding".to_string(), "name_offset".to_string()],
        );
        arrow_tools::schema::set_computed_columns_meta(&mut schema, &computed_columns_meta);

        Arc::new(schema)
    }

    #[test]
    fn test_valid_select_all() -> Result<()> {
        let schema = create_test_schema();
        let table = TableReference::parse_str("test_table");
        let sql = "SELECT * FROM test_table";

        let result = validate_refresh_sql(table, sql, Arc::clone(&schema))?;
        assert_eq!(result.fields().len(), 3);
        Ok(())
    }

    #[test]
    fn test_valid_select_columns() -> Result<()> {
        let schema = create_test_schema();
        let table = TableReference::parse_str("test_table");
        let sql = "SELECT id, name FROM test_table";

        let result = validate_refresh_sql(table, sql, Arc::clone(&schema))?;
        assert_eq!(result.fields().len(), 2);
        assert_eq!(result.field(0).name(), "id");
        assert_eq!(result.field(1).name(), "name");
        Ok(())
    }

    #[test]
    fn test_invalid_column() {
        let schema = create_test_schema();
        let table = TableReference::parse_str("test_table");
        let sql = "SELECT id, invalid_column FROM test_table";

        let result = validate_refresh_sql(table, sql, Arc::clone(&schema));
        assert!(matches!(result, Err(Error::ColumnNotFoundInSource { .. })));
    }

    #[test]
    fn test_invalid_table() {
        let schema = create_test_schema();
        let table = TableReference::parse_str("test_table");
        let sql = "SELECT id FROM wrong_table";

        let result = validate_refresh_sql(table, sql, Arc::clone(&schema));
        assert!(matches!(result, Err(Error::InvalidSqlStatement { .. })));
    }

    #[test]
    fn test_invalid_expression() {
        let schema = create_test_schema();
        let table = TableReference::parse_str("test_table");
        let sql = "SELECT id + 1 FROM test_table";

        let result = validate_refresh_sql(table, sql, Arc::clone(&schema));
        assert!(matches!(result, Err(Error::OnlyColumnReferences { .. })));
    }

    #[test]
    fn test_invalid_alias() {
        let schema = create_test_schema();
        let table = TableReference::parse_str("test_table");
        let sql = "SELECT id as user_id FROM test_table";

        let result = validate_refresh_sql(table, sql, Arc::clone(&schema));
        assert!(matches!(result, Err(Error::OnlyColumnReferences { .. })));
    }

    #[test]
    fn test_invalid_group_by() {
        let schema = create_test_schema();
        let table = TableReference::parse_str("test_table");
        let sql = "SELECT id FROM test_table GROUP BY id";

        let result = validate_refresh_sql(table, sql, Arc::clone(&schema));
        assert!(matches!(result, Err(Error::UnexpectedExpression { .. })));
    }

    #[test]
    fn test_multiple_statements() {
        let schema = create_test_schema();
        let table = TableReference::parse_str("test_table");
        let sql = "SELECT id FROM test_table; SELECT name FROM test_table";

        let result = validate_refresh_sql(table, sql, Arc::clone(&schema));
        assert!(matches!(
            result,
            Err(Error::ExpectedSingleSqlStatement { .. })
        ));
    }

    #[test]
    fn test_valid_select_columns_with_embeddings() -> Result<()> {
        let schema = create_test_schema_with_enmbeddings();
        let table = TableReference::parse_str("test_table");
        let sql = "SELECT id, name FROM test_table";

        let result = validate_refresh_sql(table, sql, Arc::clone(&schema))?;
        assert_eq!(result.fields().len(), 4);
        assert_eq!(result.field(0).name(), "id");
        assert_eq!(result.field(1).name(), "name");
        assert_eq!(result.field(2).name(), "name_embedding");
        assert_eq!(result.field(3).name(), "name_offset");
        Ok(())
    }
}
