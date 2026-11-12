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

//! Inline SQL table-function arguments used as arguments of search UDTFs
//! (`vector_search`, `text_search`) before `DataFusion` plans the function
//! body.
//!
//! Search UDTFs inspect their arguments while the table function itself is
//! being constructed during planning (`runtime-search::rrf::parse_search_args`,
//! `runtime-search::full_text_udtf`, `runtime::embeddings::udtf`), and only
//! accept literals there. All three also require the query in the *second
//! positional* slot — none of them consult a named `query` argument. A
//! SQL-tier function's own arguments, by contrast, are only replaced with
//! literal values in [`super::args_inliner::inline_args_into_plan`], which
//! runs on the logical plan *after* planning. That's too late for these
//! UDTFs: `text_search(docs, q, limit => k)` would see `k` as a column.
//!
//! This module closes the gap by rewriting the parsed SQL text. For every
//! search UDTF call, an identifier that names a function argument is
//! replaced with that argument's value as a literal when it is:
//!
//! - the query, positional or `query => ...` (string values only); it is
//!   always normalized into the second positional slot;
//! - any other named argument (`limit => k`, `include_score => s`,
//!   `rank_weight => w`, ...) except `column => ...`; it stays named;
//! - a positional argument after the column slot (`limit`, then
//!   `include_score`).
//!
//! The table slot, the positional column slot and `column => ...` are never
//! rewritten, even if a function argument shares the identifier's name, as
//! they name a table or column. A `NULL` argument value is written as SQL
//! `NULL`, so the UDTF reports it rather than seeing an unknown column.

use std::collections::HashMap;
use std::ops::ControlFlow;

use arrow::datatypes::Schema;
use datafusion::common::Result as DataFusionResult;
use datafusion::scalar::ScalarValue;
use datafusion::sql::{
    parser::DFParser,
    sqlparser::{
        ast,
        ast::{VisitMut, VisitorMut},
        dialect::Dialect,
    },
};

pub(crate) fn inline_search_query_args(
    body: &str,
    dialect: &dyn Dialect,
    schema: &Schema,
    values: &[ScalarValue],
) -> DataFusionResult<String> {
    let mut statements = DFParser::parse_sql_with_dialect(body, dialect)?;
    let arg_values: HashMap<String, ScalarValue> = schema
        .fields()
        .iter()
        .zip(values)
        .map(|(field, value)| (field.name().to_ascii_lowercase(), value.clone()))
        .collect();

    let mut rewriter = SearchQueryArgRewriter {
        values: &arg_values,
    };
    for statement in &mut statements {
        if let datafusion::sql::parser::Statement::Statement(statement) = statement {
            let _: ControlFlow<()> = statement.visit(&mut rewriter);
        }
    }
    Ok(statements
        .into_iter()
        .map(|statement| statement.to_string())
        .collect::<Vec<_>>()
        .join("; "))
}

struct SearchQueryArgRewriter<'a> {
    values: &'a HashMap<String, ScalarValue>,
}

impl VisitorMut for SearchQueryArgRewriter<'_> {
    type Break = ();

    // A search UDTF nested inside another call — e.g. `vector_search(...)`
    // as an argument to `rrf(...)` — parses as an `Expr::Function`.
    fn pre_visit_expr(&mut self, expr: &mut ast::Expr) -> ControlFlow<Self::Break> {
        let ast::Expr::Function(function) = expr else {
            return ControlFlow::Continue(());
        };
        if !is_search_udtf(&function.name.to_string()) {
            return ControlFlow::Continue(());
        }
        let ast::FunctionArguments::List(arguments) = &mut function.args else {
            return ControlFlow::Continue(());
        };
        inline_search_arguments(&mut arguments.args, self.values);
        ControlFlow::Continue(())
    }

    // A search UDTF used directly as a `FROM` source — e.g.
    // `FROM vector_search(docs, q)` — parses as a `TableFactor::Table`
    // rather than an `Expr::Function`, so it needs its own hook.
    fn pre_visit_table_factor(
        &mut self,
        table_factor: &mut ast::TableFactor,
    ) -> ControlFlow<Self::Break> {
        let ast::TableFactor::Table {
            name,
            args: Some(table_args),
            ..
        } = table_factor
        else {
            return ControlFlow::Continue(());
        };
        if !is_search_udtf(&name.to_string()) {
            return ControlFlow::Continue(());
        }
        inline_search_arguments(&mut table_args.args, self.values);
        ControlFlow::Continue(())
    }
}

fn is_search_udtf(function_name: &str) -> bool {
    function_name.rsplit('.').next().is_some_and(|name| {
        name.eq_ignore_ascii_case("vector_search") || name.eq_ignore_ascii_case("text_search")
    })
}

fn is_named(arg: &ast::FunctionArg) -> bool {
    matches!(
        arg,
        ast::FunctionArg::Named { .. } | ast::FunctionArg::ExprNamed { .. }
    )
}

/// The `name => value` key of a named argument, if that key is a plain
/// identifier (rather than a computed expression).
fn named_key(arg: &ast::FunctionArg) -> Option<&str> {
    match arg {
        ast::FunctionArg::Named { name, .. } => Some(name.value.as_str()),
        ast::FunctionArg::ExprNamed {
            name: ast::Expr::Identifier(identifier),
            ..
        } => Some(identifier.value.as_str()),
        _ => None,
    }
}

fn arg_expr_mut(arg: &mut ast::FunctionArg) -> &mut ast::FunctionArgExpr {
    match arg {
        ast::FunctionArg::Named { arg, .. }
        | ast::FunctionArg::ExprNamed { arg, .. }
        | ast::FunctionArg::Unnamed(arg) => arg,
    }
}

/// A function-argument value that can be written back into SQL text as a
/// literal, so the planner produces the same typed `Expr::Literal` a user
/// would get by writing the value inline.
#[derive(Debug, Clone, PartialEq)]
enum SqlLiteral {
    Null,
    Boolean(bool),
    Integer(i128),
    /// Always finite; see [`SqlLiteral::from_scalar`].
    Float(f64),
    String(String),
}

impl SqlLiteral {
    /// `None` for values with no plain SQL literal form (lists, structs,
    /// timestamps, non-finite floats, ...). Such identifiers are left as
    /// written, and the UDTF reports them during planning.
    fn from_scalar(value: &ScalarValue) -> Option<Self> {
        if value.is_null() {
            return Some(Self::Null);
        }
        match value {
            ScalarValue::Boolean(Some(v)) => Some(Self::Boolean(*v)),
            ScalarValue::Int8(Some(v)) => Some(Self::Integer(i128::from(*v))),
            ScalarValue::Int16(Some(v)) => Some(Self::Integer(i128::from(*v))),
            ScalarValue::Int32(Some(v)) => Some(Self::Integer(i128::from(*v))),
            ScalarValue::Int64(Some(v)) => Some(Self::Integer(i128::from(*v))),
            ScalarValue::UInt8(Some(v)) => Some(Self::Integer(i128::from(*v))),
            ScalarValue::UInt16(Some(v)) => Some(Self::Integer(i128::from(*v))),
            ScalarValue::UInt32(Some(v)) => Some(Self::Integer(i128::from(*v))),
            ScalarValue::UInt64(Some(v)) => Some(Self::Integer(i128::from(*v))),
            ScalarValue::Float32(Some(v)) => Self::finite_float(f64::from(*v)),
            ScalarValue::Float64(Some(v)) => Self::finite_float(*v),
            ScalarValue::Utf8(Some(v))
            | ScalarValue::LargeUtf8(Some(v))
            | ScalarValue::Utf8View(Some(v)) => Some(Self::String(v.clone())),
            _ => None,
        }
    }

    fn finite_float(value: f64) -> Option<Self> {
        value.is_finite().then_some(Self::Float(value))
    }

    fn into_arg_expr(self) -> ast::FunctionArgExpr {
        let value = match self {
            Self::Null => ast::Value::Null,
            Self::Boolean(v) => ast::Value::Boolean(v),
            Self::Integer(v) => ast::Value::Number(v.to_string(), false),
            Self::Float(v) => {
                // `f64` display never uses an exponent, but drops the
                // fraction of whole numbers. Keep a `.0` so the value is
                // planned as a float, not an integer.
                let mut text = v.to_string();
                if !text.contains('.') {
                    text.push_str(".0");
                }
                ast::Value::Number(text, false)
            }
            Self::String(v) => ast::Value::SingleQuotedString(v),
        };
        ast::FunctionArgExpr::Expr(ast::Expr::Value(ast::ValueWithSpan::from(value)))
    }
}

/// If `expr` is a bare identifier bound to a function argument, returns
/// that argument's value as a literal.
fn resolve_identifier(
    expr: &ast::FunctionArgExpr,
    values: &HashMap<String, ScalarValue>,
) -> Option<SqlLiteral> {
    let ast::FunctionArgExpr::Expr(ast::Expr::Identifier(identifier)) = expr else {
        return None;
    };
    SqlLiteral::from_scalar(values.get(&identifier.value.to_ascii_lowercase())?)
}

/// Like [`resolve_identifier`], but only for string values: the only kind
/// a search query can be.
fn resolve_identifier_literal(
    expr: &ast::FunctionArgExpr,
    values: &HashMap<String, ScalarValue>,
) -> Option<ast::FunctionArgExpr> {
    match resolve_identifier(expr, values)? {
        literal @ SqlLiteral::String(_) => Some(literal.into_arg_expr()),
        _ => None,
    }
}

/// Where a positional argument sits in the search UDTF signature
/// `(table, query, [column, limit, include_score])`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PositionalSlot {
    Table,
    Query,
    Column,
    /// `limit` or `include_score`: always a scalar value.
    Scalar,
}

impl PositionalSlot {
    /// `query_is_named` is true when the query is still passed as
    /// `query => ...`, so it does not take a positional slot.
    fn at(index: usize, query_is_named: bool) -> Self {
        match (index, query_is_named) {
            (0, _) => Self::Table,
            (1, false) => Self::Query,
            (1, true) | (2, false) => Self::Column,
            _ => Self::Scalar,
        }
    }
}

fn is_named_key(arg: &ast::FunctionArg, key: &str) -> bool {
    named_key(arg).is_some_and(|name| name.eq_ignore_ascii_case(key))
}

/// Inline function arguments into one search UDTF call; see the module
/// docs for which argument positions are rewritten.
fn inline_search_arguments(
    args: &mut Vec<ast::FunctionArg>,
    values: &HashMap<String, ScalarValue>,
) {
    normalize_query_argument(args, values);

    let query_is_named = args.iter().any(|arg| is_named_key(arg, "query"));
    let mut positional_index = 0;
    for arg in args.iter_mut() {
        let rewrite = if is_named(arg) {
            // A key that is not a plain identifier, or a `query`/`column`
            // key, is left as written.
            named_key(arg).is_some_and(|name| {
                !name.eq_ignore_ascii_case("query") && !name.eq_ignore_ascii_case("column")
            })
        } else {
            let slot = PositionalSlot::at(positional_index, query_is_named);
            positional_index += 1;
            slot == PositionalSlot::Scalar
        };
        if !rewrite {
            continue;
        }
        let expr = arg_expr_mut(arg);
        if let Some(literal) = resolve_identifier(expr, values) {
            *expr = literal.into_arg_expr();
        }
    }
}

/// Ensure the search UDTF's query argument ends up as a literal in the
/// second *positional* slot, since that's the only place every search UDTF
/// parser looks for it. A `query => q` named argument is moved into that
/// slot rather than merely having its value inlined in place, since it
/// would otherwise still be rejected as an unrecognized named argument.
fn normalize_query_argument(
    args: &mut Vec<ast::FunctionArg>,
    values: &HashMap<String, ScalarValue>,
) {
    let Some(table_idx) = args.iter().position(|arg| !is_named(arg)) else {
        // No positional table argument — the query position can't be
        // resolved by convention, so leave the call as written.
        return;
    };

    let mut positional_count = 0;
    let second_positional_idx = args.iter().position(|arg| {
        if is_named(arg) {
            return false;
        }
        let is_second = positional_count == 1;
        positional_count += 1;
        is_second
    });

    if let Some(idx) = second_positional_idx {
        let expr = arg_expr_mut(&mut args[idx]);
        if let Some(literal) = resolve_identifier_literal(expr, values) {
            *expr = literal;
        }
        return;
    }

    let Some(query_idx) = args.iter().position(|arg| is_named_key(arg, "query")) else {
        return;
    };
    let Some(literal) = resolve_identifier_literal(arg_expr_mut(&mut args[query_idx]), values)
    else {
        return;
    };

    args.remove(query_idx);
    let insert_at = if query_idx < table_idx {
        table_idx
    } else {
        table_idx + 1
    };
    args.insert(insert_at, ast::FunctionArg::Unnamed(literal));
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field};
    use datafusion::sql::sqlparser::dialect::GenericDialect;

    use super::*;

    /// `text` deliberately shares its name with a column of `docs`.
    fn schema() -> Schema {
        Schema::new(vec![
            Field::new("q", DataType::Utf8, true),
            Field::new("k", DataType::Int64, true),
            Field::new("text", DataType::Utf8, true),
            Field::new("s", DataType::Boolean, true),
            Field::new("w", DataType::Float64, true),
            Field::new("n", DataType::Int64, true),
            Field::new("u", DataType::UInt64, true),
        ])
    }

    fn values() -> Vec<ScalarValue> {
        vec![
            ScalarValue::Utf8(Some("hybrid search".to_string())),
            ScalarValue::Int64(Some(5)),
            ScalarValue::Utf8(Some("not a column".to_string())),
            ScalarValue::Boolean(Some(false)),
            ScalarValue::Float64(Some(2.0)),
            ScalarValue::Int64(None),
            ScalarValue::UInt64(Some(7)),
        ]
    }

    fn rewrite(body: &str) -> String {
        inline_search_query_args(body, &GenericDialect {}, &schema(), &values())
            .unwrap_or_else(|e| panic!("rewrite of {body:?} failed: {e}"))
    }

    /// Table-driven over the call shapes search UDTFs must accept: plain
    /// positional, named `query => q` before/after other named args, and
    /// both `vector_search`/`text_search` inside `rrf`. Every case must end
    /// up with the query argument as a literal in positional slot two,
    /// since that's the only place any search UDTF parser looks for it.
    #[test]
    fn normalizes_every_search_udtf_query_argument_shape() {
        let cases = [
            (
                "SELECT * FROM vector_search(docs, q)",
                "vector_search(docs, 'hybrid search')",
            ),
            (
                "SELECT * FROM vector_search(docs, query => q)",
                "vector_search(docs, 'hybrid search')",
            ),
            (
                "SELECT * FROM vector_search(docs, limit => 10, query => q)",
                "vector_search(docs, 'hybrid search', limit => 10)",
            ),
            (
                "SELECT * FROM rrf(vector_search(docs, q), text_search(docs, query => q))",
                "rrf(vector_search(docs, 'hybrid search'), text_search(docs, 'hybrid search'))",
            ),
            (
                "SELECT * FROM search.vector_search(docs, query => q)",
                "search.vector_search(docs, 'hybrid search')",
            ),
        ];

        for (body, expected_fragment) in cases {
            let rewritten =
                inline_search_query_args(body, &GenericDialect {}, &schema(), &values())
                    .unwrap_or_else(|e| panic!("rewrite of {body:?} failed: {e}"));
            assert!(
                rewritten.contains(expected_fragment),
                "expected {rewritten:?} to contain {expected_fragment:?}"
            );
        }
    }

    /// Table-driven over the non-query argument shapes: named and positional
    /// `limit`/`include_score`, other named options, nesting in `rrf`, and
    /// every literal kind. Each function argument must become a literal in
    /// the same position, since search UDTFs only accept literals there.
    #[test]
    fn inlines_scalar_arguments_of_every_search_udtf_shape() {
        let cases = [
            (
                "SELECT * FROM text_search(docs, q, limit => k)",
                "text_search(docs, 'hybrid search', limit => 5)",
            ),
            (
                "SELECT * FROM text_search(docs, query => q, limit => k)",
                "text_search(docs, 'hybrid search', limit => 5)",
            ),
            (
                "SELECT * FROM text_search(docs, q, text, k)",
                "text_search(docs, 'hybrid search', text, 5)",
            ),
            (
                "SELECT * FROM text_search(docs, q, text, k, s)",
                "text_search(docs, 'hybrid search', text, 5, false)",
            ),
            (
                "SELECT * FROM text_search(docs, q, include_score => s)",
                "text_search(docs, 'hybrid search', include_score => false)",
            ),
            (
                "SELECT * FROM vector_search(docs, q, limit => k)",
                "vector_search(docs, 'hybrid search', limit => 5)",
            ),
            (
                "SELECT * FROM vector_search(docs, q, text, u)",
                "vector_search(docs, 'hybrid search', text, 7)",
            ),
            (
                "SELECT * FROM rrf(vector_search(docs, q, limit => k), text_search(docs, q, text, k, rank_weight => w))",
                "rrf(vector_search(docs, 'hybrid search', limit => 5), text_search(docs, 'hybrid search', text, 5, rank_weight => 2.0))",
            ),
            (
                "SELECT * FROM text_search(docs, q, limit => n)",
                "text_search(docs, 'hybrid search', limit => NULL)",
            ),
        ];

        for (body, expected_fragment) in cases {
            let rewritten = rewrite(body);
            assert!(
                rewritten.contains(expected_fragment),
                "expected {rewritten:?} to contain {expected_fragment:?}"
            );
        }
    }

    /// The table slot, the positional column slot and `column => ...` name
    /// a table or column, so a function argument with the same name must
    /// not replace them.
    #[test]
    fn leaves_table_and_column_arguments_untouched() {
        let cases = [
            (
                "SELECT * FROM text_search(docs, q, text, k)",
                "(docs, 'hybrid search', text, 5)",
            ),
            (
                "SELECT * FROM text_search(docs, q, column => text, limit => k)",
                "text_search(docs, 'hybrid search', column => text, limit => 5)",
            ),
            (
                "SELECT * FROM text_search(text, q)",
                "text_search(text, 'hybrid search')",
            ),
            (
                "SELECT * FROM vector_search(docs, q, text)",
                "vector_search(docs, 'hybrid search', text)",
            ),
        ];

        for (body, expected_fragment) in cases {
            let rewritten = rewrite(body);
            assert!(
                rewritten.contains(expected_fragment),
                "expected {rewritten:?} to contain {expected_fragment:?}"
            );
        }
    }

    #[test]
    fn leaves_unknown_identifiers_and_non_search_arguments_untouched() {
        let rewritten =
            rewrite("SELECT k FROM text_search(docs, q, limit => other) WHERE _score > w");
        assert!(rewritten.contains("text_search(docs, 'hybrid search', limit => other)"));
        assert!(rewritten.contains("SELECT k FROM"));
        assert!(rewritten.contains("_score > w"));
    }

    #[test]
    fn sql_literal_maps_scalars_to_typed_literals() {
        assert_eq!(
            SqlLiteral::from_scalar(&ScalarValue::UInt64(Some(u64::MAX))),
            Some(SqlLiteral::Integer(i128::from(u64::MAX)))
        );
        assert_eq!(
            SqlLiteral::from_scalar(&ScalarValue::Utf8View(Some("x".to_string()))),
            Some(SqlLiteral::String("x".to_string()))
        );
        assert_eq!(
            SqlLiteral::from_scalar(&ScalarValue::Boolean(None)),
            Some(SqlLiteral::Null)
        );
        assert_eq!(
            SqlLiteral::from_scalar(&ScalarValue::Float64(Some(f64::NAN))),
            None
        );
        assert_eq!(SqlLiteral::Float(0.5).into_arg_expr().to_string(), "0.5");
        assert_eq!(SqlLiteral::Integer(-3).into_arg_expr().to_string(), "-3");
    }

    #[test]
    fn leaves_non_identifier_query_arguments_untouched() {
        let rewritten = inline_search_query_args(
            "SELECT * FROM vector_search(docs, 'literal already')",
            &GenericDialect {},
            &schema(),
            &values(),
        )
        .expect("rewrites without error");
        assert!(rewritten.contains("vector_search(docs, 'literal already')"));
    }

    #[test]
    fn leaves_unrelated_functions_untouched() {
        let rewritten = inline_search_query_args(
            "SELECT * FROM some_other_udtf(docs, q)",
            &GenericDialect {},
            &schema(),
            &values(),
        )
        .expect("rewrites without error");
        assert!(rewritten.contains("some_other_udtf(docs, q)"));
    }
}
