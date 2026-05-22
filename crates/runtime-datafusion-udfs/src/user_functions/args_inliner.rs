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

//! Inline literal table-function arguments into body SQL before planning.
//!
//! When all table-function arguments are literals (enforced by
//! [`super::sql::literal_arg`]), we can replace `(SELECT col FROM args)`
//! scalar subqueries with the literal value directly.  This makes the
//! literal visible to the logical planner so that filter pushdown (e.g.
//! HTTP connector's `request_path`) works correctly.

use std::collections::HashMap;
use std::ops::ControlFlow;

use arrow::datatypes::Schema;
use datafusion::scalar::ScalarValue;
use datafusion::sql::{
    parser::DFParser,
    sqlparser::{
        ast::{self, VisitMut, VisitorMut},
        dialect::PostgreSqlDialect,
    },
};

use super::sql::SQL_TABLE_ARGS_TABLE_NAME;

/// Convert a [`ScalarValue`] to a `sqlparser` AST expression.
///
/// Returns `None` for types that cannot be cleanly represented as a SQL
/// literal — the caller should fall back to the `args` MemTable path.
fn scalar_value_to_ast_expr(value: &ScalarValue) -> Option<ast::Expr> {
    let val = match value {
        ScalarValue::Null => ast::Value::Null,
        ScalarValue::Boolean(Some(b)) => ast::Value::Boolean(*b),
        ScalarValue::Int8(Some(n)) => ast::Value::Number(n.to_string(), false),
        ScalarValue::Int16(Some(n)) => ast::Value::Number(n.to_string(), false),
        ScalarValue::Int32(Some(n)) => ast::Value::Number(n.to_string(), false),
        ScalarValue::Int64(Some(n)) => ast::Value::Number(n.to_string(), false),
        ScalarValue::UInt8(Some(n)) => ast::Value::Number(n.to_string(), false),
        ScalarValue::UInt16(Some(n)) => ast::Value::Number(n.to_string(), false),
        ScalarValue::UInt32(Some(n)) => ast::Value::Number(n.to_string(), false),
        ScalarValue::UInt64(Some(n)) => ast::Value::Number(n.to_string(), false),
        ScalarValue::Float32(Some(f)) => ast::Value::Number(f.to_string(), false),
        ScalarValue::Float64(Some(f)) => ast::Value::Number(f.to_string(), false),
        ScalarValue::Utf8(Some(s))
        | ScalarValue::LargeUtf8(Some(s))
        | ScalarValue::Utf8View(Some(s)) => ast::Value::SingleQuotedString(s.clone()),
        // For any other type (Option<None> variants, timestamps, decimals,
        // etc.) we cannot produce a safe literal — fall back.
        _ => return None,
    };
    Some(ast::Expr::Value(val.into()))
}

/// Visitor that replaces `(SELECT expr FROM args)` scalar subqueries with
/// the args' literal values inlined into `expr`.
struct ArgsInliner {
    /// Column name (lower-cased) → literal AST expression.
    arg_map: HashMap<String, ast::Expr>,
    /// Set to `true` when an unsupported pattern is encountered, signalling
    /// the caller to fall back to the MemTable path.
    failed: bool,
}

impl ArgsInliner {
    /// Return `true` if `table` in a `FROM` clause refers to the `args`
    /// virtual table (single, unqualified identifier, case-insensitive).
    fn is_args_table(table: &ast::TableFactor) -> bool {
        if let ast::TableFactor::Table { name, .. } = table {
            let parts: Vec<_> = name
                .0
                .iter()
                .filter_map(|p| match p {
                    ast::ObjectNamePart::Identifier(ident) => Some(ident.value.as_str()),
                    _ => None,
                })
                .collect();
            parts.len() == 1 && parts[0].eq_ignore_ascii_case(SQL_TABLE_ARGS_TABLE_NAME)
        } else {
            false
        }
    }

    /// Check if a `FROM` clause consists of exactly the `args` table with
    /// no joins.
    fn is_single_args_from(from: &[ast::TableWithJoins]) -> bool {
        from.len() == 1 && from[0].joins.is_empty() && Self::is_args_table(&from[0].relation)
    }

    /// Replace bare column identifiers that match an arg name with the
    /// corresponding literal value.  Returns `true` if any replacement
    /// was made.
    fn inline_columns_in_expr(&self, expr: &mut ast::Expr) -> bool {
        let mut replaced = false;
        match expr {
            // Unqualified identifier: `col`
            ast::Expr::Identifier(ident) => {
                if let Some(replacement) = self.arg_map.get(&ident.value.to_ascii_lowercase()) {
                    *expr = replacement.clone();
                    replaced = true;
                }
            }
            // Qualified identifier: `args.col`
            ast::Expr::CompoundIdentifier(parts) => {
                if parts.len() == 2
                    && parts[0]
                        .value
                        .eq_ignore_ascii_case(SQL_TABLE_ARGS_TABLE_NAME)
                {
                    if let Some(replacement) =
                        self.arg_map.get(&parts[1].value.to_ascii_lowercase())
                    {
                        *expr = replacement.clone();
                        replaced = true;
                    }
                }
            }
            _ => {}
        }
        replaced
    }

    /// Recursively walk an AST expression and replace all arg column
    /// references with their literal values.
    fn inline_columns_recursive(&self, expr: &mut ast::Expr) {
        // First try a direct replacement at this node.
        if self.inline_columns_in_expr(expr) {
            return; // replaced the whole node
        }
        // Otherwise recurse into children.
        match expr {
            ast::Expr::Function(func) => {
                if let ast::FunctionArguments::List(arg_list) = &mut func.args {
                    for arg in &mut arg_list.args {
                        if let ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e))
                        | ast::FunctionArg::Named {
                            arg: ast::FunctionArgExpr::Expr(e),
                            ..
                        } = arg
                        {
                            self.inline_columns_recursive(e);
                        }
                    }
                }
            }
            ast::Expr::BinaryOp { left, right, .. } => {
                self.inline_columns_recursive(left);
                self.inline_columns_recursive(right);
            }
            ast::Expr::UnaryOp { expr: inner, .. } => {
                self.inline_columns_recursive(inner);
            }
            ast::Expr::Nested(inner) => {
                self.inline_columns_recursive(inner);
            }
            ast::Expr::Cast { expr: inner, .. } => {
                self.inline_columns_recursive(inner);
            }
            ast::Expr::Case {
                operand,
                conditions,
                else_result,
                ..
            } => {
                if let Some(op) = operand {
                    self.inline_columns_recursive(op);
                }
                for cw in conditions {
                    self.inline_columns_recursive(&mut cw.condition);
                    self.inline_columns_recursive(&mut cw.result);
                }
                if let Some(e) = else_result {
                    self.inline_columns_recursive(e);
                }
            }
            ast::Expr::IsFalse(e)
            | ast::Expr::IsTrue(e)
            | ast::Expr::IsNull(e)
            | ast::Expr::IsNotNull(e) => {
                self.inline_columns_recursive(e);
            }
            _ => {
                // For any expression type we don't explicitly recurse
                // into, leave it as-is.  The outer `post_visit_expr`
                // will still handle `Subquery` nodes at the top level.
            }
        }
    }
}

impl VisitorMut for ArgsInliner {
    type Break = ();

    fn post_visit_expr(&mut self, expr: &mut ast::Expr) -> ControlFlow<Self::Break> {
        if self.failed {
            return ControlFlow::Continue(());
        }

        // Match `(SELECT expr FROM args)` scalar subqueries.
        let ast::Expr::Subquery(query) = expr else {
            return ControlFlow::Continue(());
        };

        // Only handle simple `SELECT expr FROM args` — no CTEs, no LIMIT,
        // no GROUP BY, no HAVING, no DISTINCT, no WINDOW.
        let ast::SetExpr::Select(select) = query.body.as_ref() else {
            return ControlFlow::Continue(());
        };

        if !Self::is_single_args_from(&select.from) {
            return ControlFlow::Continue(());
        }

        // Only a single projection item.
        if select.projection.len() != 1 {
            self.failed = true;
            return ControlFlow::Continue(());
        }

        // Guard against complex subqueries we can't safely rewrite.
        if select.group_by != ast::GroupByExpr::Expressions(vec![], vec![])
            || select.having.is_some()
            || select.distinct.is_some()
            || !select.sort_by.is_empty()
            || query.limit_clause.is_some()
        {
            return ControlFlow::Continue(());
        }

        // Extract the single SELECT expression.
        let proj = &select.projection[0];
        let mut select_expr = match proj {
            ast::SelectItem::UnnamedExpr(e) => e.clone(),
            ast::SelectItem::ExprWithAlias { expr: e, .. } => e.clone(),
            _ => {
                // Wildcard or qualified wildcard — can't inline.
                self.failed = true;
                return ControlFlow::Continue(());
            }
        };

        // Inline all arg column references in the expression.
        self.inline_columns_recursive(&mut select_expr);

        // Replace the entire subquery with the rewritten expression.
        *expr = select_expr;

        ControlFlow::Continue(())
    }
}

/// Attempt to inline literal scalar args into the body SQL by rewriting
/// `(SELECT expr FROM args)` scalar subqueries.
///
/// Returns `Some(rewritten_sql)` on success, or `None` if any pattern
/// could not be safely inlined (the caller should use the MemTable path).
pub(super) fn inline_args_into_body(
    body: &str,
    schema: &Schema,
    values: &[ScalarValue],
) -> Option<String> {
    if schema.fields().is_empty() {
        return None;
    }

    // Build the arg_map: column_name -> AST literal.
    let mut arg_map = HashMap::with_capacity(schema.fields().len());
    for (field, value) in schema.fields().iter().zip(values) {
        let ast_expr = scalar_value_to_ast_expr(value)?;
        arg_map.insert(field.name().to_ascii_lowercase(), ast_expr);
    }

    // Parse the body SQL.
    let statements = DFParser::parse_sql_with_dialect(body, &PostgreSqlDialect {}).ok()?;
    if statements.len() != 1 {
        return None;
    }

    let mut statement = match statements.into_iter().next()? {
        datafusion::sql::parser::Statement::Statement(s) => *s,
        _ => return None,
    };

    let mut inliner = ArgsInliner {
        arg_map,
        failed: false,
    };
    let _ = statement.visit(&mut inliner);

    if inliner.failed {
        return None;
    }

    Some(statement.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Field as ArrowField;

    fn utf8_schema(names: &[&str]) -> Schema {
        Schema::new(
            names
                .iter()
                .map(|n| ArrowField::new(*n, arrow::datatypes::DataType::Utf8, true))
                .collect::<Vec<_>>(),
        )
    }

    #[test]
    fn inline_args_replaces_scalar_subquery() {
        let schema = utf8_schema(&["username"]);
        let values = vec![ScalarValue::Utf8(Some("pg".into()))];
        let body =
            "SELECT content FROM raw_users WHERE request_path = (SELECT username FROM args)";
        let result = inline_args_into_body(body, &schema, &values).expect("should inline");
        assert!(
            result.contains("'pg'"),
            "Expected inlined literal 'pg' in: {result}"
        );
        assert!(
            !result.contains("FROM args"),
            "Should not contain FROM args after inlining: {result}"
        );
    }

    #[test]
    fn inline_args_replaces_expression_over_args() {
        let schema = utf8_schema(&["username"]);
        let values = vec![ScalarValue::Utf8(Some("pg".into()))];
        let body = "SELECT content FROM raw_users WHERE request_path = (SELECT concat('/users/', username) FROM args)";
        let result = inline_args_into_body(body, &schema, &values).expect("should inline");
        assert!(
            result.contains("'pg'"),
            "Expected inlined literal 'pg' in: {result}"
        );
        assert!(
            !result.contains("FROM args"),
            "Should not contain FROM args: {result}"
        );
        assert!(
            result.to_lowercase().contains("concat"),
            "Should still contain concat call: {result}"
        );
    }

    #[test]
    fn inline_args_handles_qualified_reference() {
        let schema = utf8_schema(&["username"]);
        let values = vec![ScalarValue::Utf8(Some("pg".into()))];
        let body = "SELECT content FROM raw_users WHERE request_path = (SELECT args.username FROM args)";
        let result = inline_args_into_body(body, &schema, &values).expect("should inline");
        assert!(
            result.contains("'pg'"),
            "Expected inlined literal: {result}"
        );
        assert!(
            !result.contains("FROM args"),
            "Should not contain FROM args: {result}"
        );
    }

    #[test]
    fn inline_args_handles_numeric_types() {
        let schema = Schema::new(vec![ArrowField::new(
            "x",
            arrow::datatypes::DataType::Int64,
            true,
        )]);
        let values = vec![ScalarValue::Int64(Some(42))];
        let body = "SELECT x AS value, x * 2 AS doubled FROM t WHERE id = (SELECT x FROM args)";
        let result = inline_args_into_body(body, &schema, &values).expect("should inline");
        assert!(
            result.contains("42"),
            "Expected inlined literal 42 in: {result}"
        );
        assert!(
            !result.contains("FROM args"),
            "Should not contain FROM args: {result}"
        );
    }

    #[test]
    fn inline_args_handles_boolean_and_null() {
        let schema = Schema::new(vec![ArrowField::new(
            "flag",
            arrow::datatypes::DataType::Boolean,
            true,
        )]);
        let values = vec![ScalarValue::Boolean(Some(true))];
        let body = "SELECT * FROM t WHERE active = (SELECT flag FROM args)";
        let result = inline_args_into_body(body, &schema, &values).expect("should inline");
        assert!(
            result.to_lowercase().contains("true"),
            "Expected inlined boolean: {result}"
        );

        let null_values = vec![ScalarValue::Null];
        let schema_null = utf8_schema(&["val"]);
        let body_null = "SELECT * FROM t WHERE col = (SELECT val FROM args)";
        let result_null =
            inline_args_into_body(body_null, &schema_null, &null_values).expect("should inline");
        assert!(
            result_null.contains("NULL"),
            "Expected inlined NULL: {result_null}"
        );
    }

    #[test]
    fn inline_args_escapes_strings_safely() {
        let schema = utf8_schema(&["name"]);
        // Value with single quotes and semicolons — must not cause SQL injection.
        let values = vec![ScalarValue::Utf8(Some("O'Reilly; DROP TABLE".into()))];
        let body = "SELECT * FROM t WHERE name = (SELECT name FROM args)";
        let result = inline_args_into_body(body, &schema, &values).expect("should inline");
        // sqlparser escapes single quotes by doubling them.
        assert!(
            result.contains("O''Reilly"),
            "Expected escaped quote in: {result}"
        );
        assert!(
            !result.contains("FROM args"),
            "Should not contain FROM args: {result}"
        );
    }

    #[test]
    fn inline_args_returns_none_for_empty_schema() {
        let schema = Schema::empty();
        let values = vec![];
        let body = "SELECT * FROM t";
        assert!(
            inline_args_into_body(body, &schema, &values).is_none(),
            "Should return None for empty schema"
        );
    }

    #[test]
    fn inline_args_no_subquery_returns_original() {
        // Body that uses `FROM args` directly (not a scalar subquery) —
        // the inliner doesn't touch it but still returns the SQL.
        let schema = utf8_schema(&["x"]);
        let values = vec![ScalarValue::Utf8(Some("val".into()))];
        let body = "SELECT x AS value FROM args";
        let result = inline_args_into_body(body, &schema, &values);
        // No scalar subquery to inline, so the SQL is returned as-is
        // (modulo re-serialization by sqlparser).
        assert!(result.is_some(), "Should still return Some for valid SQL");
    }

    #[test]
    fn inline_args_multiple_subqueries() {
        let schema = Schema::new(vec![
            ArrowField::new("a", arrow::datatypes::DataType::Utf8, true),
            ArrowField::new("b", arrow::datatypes::DataType::Int64, true),
        ]);
        let values = vec![
            ScalarValue::Utf8(Some("hello".into())),
            ScalarValue::Int64(Some(99)),
        ];
        let body =
            "SELECT * FROM t WHERE name = (SELECT a FROM args) AND id = (SELECT b FROM args)";
        let result = inline_args_into_body(body, &schema, &values).expect("should inline");
        assert!(
            result.contains("'hello'"),
            "Expected inlined 'hello': {result}"
        );
        assert!(result.contains("99"), "Expected inlined 99: {result}");
        assert!(
            !result.contains("FROM args"),
            "Should not contain FROM args: {result}"
        );
    }
}
