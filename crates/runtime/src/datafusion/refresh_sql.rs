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

use crate::datafusion::error::format_datafusion_error;
use arrow_schema::SchemaRef;
use arrow_tools::schema::schema_meta_get_computed_columns;
use datafusion::arrow::datatypes::Schema;
use datafusion::error::DataFusionError;
use datafusion::sql::parser::{DFParser, Statement};
use datafusion::sql::sqlparser::ast::{Expr, GroupByExpr, Ident, SelectItem, SetExpr};
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion::sql::{TableReference, sqlparser};
use datafusion_expr::sqlparser::ast::LimitClause;
use itertools::Itertools;
use snafu::prelude::*;
use sqlparser::ast::Statement as SQLStatement;

use crate::accelerated_table::refresh::{RefreshSQL, RefreshSQLColumns};

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "The provided Refresh SQL could not be parsed. {} Check the SQL for syntax errors.",
        format_datafusion_error(source)
    ))]
    UnableToParseSql { source: DataFusionError },

    #[snafu(display(
        "Expected a single SQL statement for the refresh SQL, found {num_statements}. Rewrite the SQL to only contain a single SELECT statement."
    ))]
    ExpectedSingleSqlStatement { num_statements: usize },

    #[snafu(display(
        "Expected a SQL query starting with SELECT <columns> FROM {expected_table}. {issue}"
    ))]
    InvalidSqlStatement {
        expected_table: TableReference,
        issue: String,
    },

    #[snafu(display(
        "Unexpected '{expr}' in the Refresh SQL statement. Rewrite the SQL to only perform WHERE filters, i.e. SELECT col1, col2, col3 FROM {expected_table} WHERE col1 = 'foo'"
    ))]
    UnexpectedExpression {
        expr: &'static str,
        expected_table: TableReference,
    },

    #[snafu(display(
        "Only column references are allowed in the SELECT clause of the refresh SQL, custom expressions and aliases are not supported. Change the SQL to only use columns references, i.e. SELECT col1, col2, col3 FROM {expected_table}"
    ))]
    OnlyColumnReferences { expected_table: TableReference },

    #[snafu(display(
        "The column '{column}' is not present in the source table '{expected_table}', valid columns are: {valid_columns} Rewrite the SQL to only select columns that exist in the source table."
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

/// Parse and validate a refresh SQL string, returning both a structured `RefreshSQL` and the
/// validated schema. This replaces the old `validate_refresh_sql` which only returned a schema.
pub fn parse_refresh_sql(
    expected_table: TableReference,
    refresh_sql: &str,
    source_schema: Arc<Schema>,
) -> Result<(RefreshSQL, Arc<Schema>)> {
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
                ensure_no_expr!(query.with.is_none(), "WITH", expected_table);
                ensure_no_expr!(query.order_by.is_none(), "ORDER BY", expected_table);
                ensure_no_expr!(query.for_clause.is_none(), "FOR", expected_table);

                let limit_value = parse_limit(query.limit_clause.as_ref(), &expected_table)?;

                ensure_no_expr!(query.format_clause.is_none(), "FORMAT", expected_table);
                ensure_no_expr!(query.settings.is_none(), "SETTINGS", expected_table);

                match query.body.as_ref() {
                    SetExpr::Select(select) => {
                        let (columns, refresh_schema) = validate_and_extract_columns(
                            &select.projection,
                            source_schema,
                            &expected_table,
                        )?;
                        ensure!(
                            select.from.len() == 1,
                            InvalidSqlStatementSnafu {
                                expected_table,
                                issue: format!(
                                    "A single FROM clause with table reference is expected, found {}",
                                    select.from.len()
                                )
                            }
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
                                    .map(ToString::to_string)
                                    .collect::<Vec<_>>()
                                    .join(".");
                                if TableReference::parse_str(&table_name_with_schema)
                                    != expected_table
                                {
                                    return InvalidSqlStatementSnafu {
                                        expected_table: expected_table.clone(),
                                        issue: format!(
                                            "Table name in refresh_sql should be {expected_table}, is {name}",
                                        ),
                                    }
                                    .fail();
                                }
                            }
                            _ => {
                                return InvalidSqlStatementSnafu {
                                    expected_table,
                                    issue:
                                        "No FROM clause with table reference found in SQL statement"
                                            .to_string(),
                                }
                                .fail();
                            }
                        }

                        // Extract user WHERE filters, split on top-level AND
                        let user_filters = select
                            .selection
                            .as_ref()
                            .map(|expr| split_conjunction(expr.clone()))
                            .unwrap_or_default();

                        let refresh_sql =
                            RefreshSQL::new(expected_table, columns, user_filters, limit_value);

                        Ok((refresh_sql, refresh_schema))
                    }
                    _ => InvalidSqlStatementSnafu {
                        expected_table,
                        issue: format!(
                            "Expected a basic Select SQL query statement, found {}",
                            query.body
                        ),
                    }
                    .fail()?,
                }
            }
            _ => InvalidSqlStatementSnafu {
                expected_table,
                issue: format!("Expected a Select SQL query statement, found {statement}"),
            }
            .fail()?,
        },
        _ => InvalidSqlStatementSnafu {
            expected_table,
            issue: format!("Expected a SQL Statement, found {statement}"),
        }
        .fail()?,
    }
}

/// Extract and validate the LIMIT clause, ensuring it is a simple non-negative integer literal if present, and that no unsupported features like OFFSET or LIMIT BY are used.
fn parse_limit(limit: Option<&LimitClause>, tbl: &TableReference) -> Result<Option<usize>> {
    let (limit, offset, limit_by) = match limit {
        Some(LimitClause::LimitOffset {
            limit,
            offset,
            limit_by,
        }) => (limit, offset, limit_by),
        None => return Ok(None), // No LIMIT clause specified, treated as no limit
        _ => {
            return UnexpectedExpressionSnafu {
                expr: "unsupported LIMIT clause; expected LIMIT <non-negative integer literal>",
                expected_table: tbl.clone(),
            }
            .fail();
        }
    };

    ensure_no_expr!(limit_by.is_empty(), "LIMIT BY", tbl.clone());
    ensure_no_expr!(offset.is_none(), "OFFSET", tbl.clone());

    match &limit {
        Some(Expr::Value(v)) => v
            .to_string()
            .parse::<usize>()
            .map_err(|_| Error::UnexpectedExpression {
                expr: "non-negative integer literal LIMIT",
                expected_table: tbl.clone(),
            })
            .map(Some),
        None => Ok(None), // No LIMIT value specified, treated as no limit
        _ => UnexpectedExpressionSnafu {
            expr: "non-negative integer literal LIMIT",
            expected_table: tbl.clone(),
        }
        .fail(),
    }
}

/// Validate select columns and extract both the `RefreshSQLColumns` and the resulting schema.
fn validate_and_extract_columns(
    select: &Vec<SelectItem>,
    source_schema: Arc<Schema>,
    expected_table: &TableReference,
) -> Result<(RefreshSQLColumns, Arc<Schema>)> {
    // Wildcard will select all columns
    if select.len() == 1 && matches!(select[0], SelectItem::Wildcard(_)) {
        return Ok((RefreshSQLColumns::All, source_schema));
    }

    let mut fields = vec![];
    let mut column_idents = vec![];
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
                    column_idents.push(ident.clone());
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
    let (fields, column_idents) = include_computed_columns(&fields, &column_idents, &source_schema);

    Ok((
        RefreshSQLColumns::Named(column_idents),
        Arc::new(Schema::new(fields)),
    ))
}

/// Split a WHERE expression on top-level AND into individual predicates.
fn split_conjunction(expr: Expr) -> Vec<Expr> {
    match expr {
        Expr::BinaryOp {
            left,
            op: sqlparser::ast::BinaryOperator::And,
            right,
        } => {
            let mut parts = split_conjunction(*left);
            parts.extend(split_conjunction(*right));
            parts
        }
        other => vec![other],
    }
}

/// Checks the source schema for associated computed columns (e.g., embeddings)
/// and adds any missing computed fields to the target schema if they are found.
fn include_computed_columns(
    fields: &[arrow_schema::Field],
    column_idents: &[Ident],
    source_schema: &SchemaRef,
) -> (Vec<arrow_schema::Field>, Vec<Ident>) {
    let mut extended_fields = fields.to_owned();
    let mut extended_idents = column_idents.to_owned();
    for field in fields {
        if let Some(computed_cols) = schema_meta_get_computed_columns(source_schema, field.name()) {
            for computed_col in computed_cols {
                // Add field only if it does not exist in target schema
                if !extended_fields
                    .iter()
                    .any(|f| f.name() == computed_col.name())
                {
                    extended_fields.push((*computed_col).clone());
                    extended_idents.push(Ident::new(computed_col.name().clone()));
                }
            }
        }
    }

    (extended_fields, extended_idents)
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

        let (refresh_sql, result_schema) = parse_refresh_sql(table, sql, Arc::clone(&schema))?;
        assert_eq!(result_schema.fields().len(), 3);
        assert!(matches!(refresh_sql.columns(), RefreshSQLColumns::All));
        assert_eq!(refresh_sql.to_sql(), "SELECT * FROM test_table");
        Ok(())
    }

    #[test]
    fn test_valid_select_columns() -> Result<()> {
        let schema = create_test_schema();
        let table = TableReference::parse_str("test_table");
        let sql = "SELECT id, name FROM test_table";

        let (refresh_sql, result_schema) = parse_refresh_sql(table, sql, Arc::clone(&schema))?;
        assert_eq!(result_schema.fields().len(), 2);
        assert_eq!(result_schema.field(0).name(), "id");
        assert_eq!(result_schema.field(1).name(), "name");
        assert!(matches!(refresh_sql.columns(), RefreshSQLColumns::Named(_)));
        assert_eq!(refresh_sql.to_sql(), "SELECT id, name FROM test_table");
        Ok(())
    }

    #[test]
    fn test_invalid_column() {
        let schema = create_test_schema();
        let table = TableReference::parse_str("test_table");
        let sql = "SELECT id, invalid_column FROM test_table";

        let result = parse_refresh_sql(table, sql, Arc::clone(&schema));
        assert!(matches!(result, Err(Error::ColumnNotFoundInSource { .. })));
    }

    #[test]
    fn test_invalid_table() {
        let schema = create_test_schema();
        let table = TableReference::parse_str("test_table");
        let sql = "SELECT id FROM wrong_table";

        let result = parse_refresh_sql(table, sql, Arc::clone(&schema));
        assert!(matches!(result, Err(Error::InvalidSqlStatement { .. })));
    }

    #[test]
    fn test_invalid_expression() {
        let schema = create_test_schema();
        let table = TableReference::parse_str("test_table");
        let sql = "SELECT id + 1 FROM test_table";

        let result = parse_refresh_sql(table, sql, Arc::clone(&schema));
        assert!(matches!(result, Err(Error::OnlyColumnReferences { .. })));
    }

    #[test]
    fn test_invalid_alias() {
        let schema = create_test_schema();
        let table = TableReference::parse_str("test_table");
        let sql = "SELECT id as user_id FROM test_table";

        let result = parse_refresh_sql(table, sql, Arc::clone(&schema));
        assert!(matches!(result, Err(Error::OnlyColumnReferences { .. })));
    }

    #[test]
    fn test_invalid_group_by() {
        let schema = create_test_schema();
        let table = TableReference::parse_str("test_table");
        let sql = "SELECT id FROM test_table GROUP BY id";

        let result = parse_refresh_sql(table, sql, Arc::clone(&schema));
        assert!(matches!(result, Err(Error::UnexpectedExpression { .. })));
    }

    #[test]
    fn test_multiple_statements() {
        let schema = create_test_schema();
        let table = TableReference::parse_str("test_table");
        let sql = "SELECT id FROM test_table; SELECT name FROM test_table";

        let result = parse_refresh_sql(table, sql, Arc::clone(&schema));
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

        let (refresh_sql, result_schema) = parse_refresh_sql(table, sql, Arc::clone(&schema))?;
        assert_eq!(result_schema.fields().len(), 4);
        assert_eq!(result_schema.field(0).name(), "id");
        assert_eq!(result_schema.field(1).name(), "name");
        assert_eq!(result_schema.field(2).name(), "name_embedding");
        assert_eq!(result_schema.field(3).name(), "name_offset");
        assert_eq!(
            refresh_sql.to_sql(),
            "SELECT id, name, name_embedding, name_offset FROM test_table"
        );
        Ok(())
    }

    #[test]
    fn test_valid_where_with_scalar_function() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("organization_id", DataType::Utf8, false),
            Field::new("value", DataType::Float64, true),
        ]));
        let table = TableReference::parse_str("test_table");

        // bucket() in a WHERE clause should be accepted by refresh SQL validation.
        let sql = "SELECT * FROM test_table WHERE bucket(50, organization_id) = 0";
        let (refresh_sql, result_schema) =
            parse_refresh_sql(table.clone(), sql, Arc::clone(&schema))?;
        assert_eq!(result_schema.fields().len(), 3);
        assert_eq!(
            refresh_sql.to_sql(),
            "SELECT * FROM test_table WHERE bucket(50, organization_id) = 0"
        );

        // Multiple OR'd bucket() filters (as generated by partition assignment).
        let sql = "SELECT * FROM test_table WHERE bucket(50, organization_id) = 0 OR bucket(50, organization_id) = 1";
        let (refresh_sql, result_schema) = parse_refresh_sql(table, sql, Arc::clone(&schema))?;
        assert_eq!(result_schema.fields().len(), 3);
        // The OR expression stays as a single filter since OR is not top-level AND
        assert_eq!(
            refresh_sql.to_sql(),
            "SELECT * FROM test_table WHERE bucket(50, organization_id) = 0 OR bucket(50, organization_id) = 1"
        );

        Ok(())
    }

    #[test]
    fn test_where_with_and() -> Result<()> {
        let schema = create_test_schema();
        let table = TableReference::parse_str("test_table");
        let sql = "SELECT * FROM test_table WHERE id > 10 AND name = 'foo'";

        let (refresh_sql, _) = parse_refresh_sql(table, sql, Arc::clone(&schema))?;
        // Should split on top-level AND into 2 user_filters
        let reconstructed = refresh_sql.to_sql();
        assert!(reconstructed.contains("WHERE"));
        assert!(reconstructed.contains("id > 10"));
        assert!(reconstructed.contains("name = 'foo'"));
        Ok(())
    }

    #[test]
    fn test_with_limit() -> Result<()> {
        let schema = create_test_schema();
        let table = TableReference::parse_str("test_table");
        let sql = "SELECT * FROM test_table LIMIT 100";

        let (refresh_sql, _) = parse_refresh_sql(table, sql, Arc::clone(&schema))?;
        assert!(refresh_sql.to_sql().contains("LIMIT 100"));
        Ok(())
    }

    #[test]
    fn test_split_conjunction() {
        use sqlparser::ast::{BinaryOperator, Value};

        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Value(Value::Number("1".to_string(), false).into())),
            op: BinaryOperator::And,
            right: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Value(Value::Number("2".to_string(), false).into())),
                op: BinaryOperator::And,
                right: Box::new(Expr::Value(Value::Number("3".to_string(), false).into())),
            }),
        };

        let parts = split_conjunction(expr);
        assert_eq!(parts.len(), 3);
    }

    #[test]
    fn test_quoted_identifiers_preserved() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("MyCol", DataType::Int64, false),
            Field::new("another col", DataType::Utf8, false),
        ]));
        let table = TableReference::parse_str("test_table");
        let sql = r#"SELECT "MyCol", "another col" FROM test_table"#;

        let (refresh_sql, result_schema) = parse_refresh_sql(table, sql, Arc::clone(&schema))?;
        assert_eq!(result_schema.fields().len(), 2);
        // to_sql() should preserve the double-quoting
        let reconstructed = refresh_sql.to_sql();
        assert!(
            reconstructed.contains(r#""MyCol""#),
            "Expected quoted identifier in: {reconstructed}"
        );
        assert!(
            reconstructed.contains(r#""another col""#),
            "Expected quoted identifier in: {reconstructed}"
        );
        Ok(())
    }
}
