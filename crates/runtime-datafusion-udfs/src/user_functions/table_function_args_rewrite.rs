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

//! Inline a SQL table function's scalar arguments into the arguments of
//! the table functions its body calls, before `DataFusion` plans the body.
//!
//! A table function reads its arguments while it is constructed during
//! planning (for example `text_search`, `vector_search`, `rrf`, `rerank`,
//! `flatten_json`), and most accept only literals there. A SQL-tier
//! function's own arguments, by contrast, are only replaced with literal
//! values in [`super::args_inliner::inline_args_into_plan`], which runs on
//! the logical plan *after* planning. That's too late for table functions:
//! `text_search(docs, q, limit => k)` would see `k` as a column.
//!
//! This module closes the gap by rewriting the parsed SQL text. In every
//! table function call — a `FROM` source, and any call nested in its
//! arguments, such as `text_search(...)` inside `rrf(...)` — an argument in
//! any position, positional or `name => value`, is replaced with a literal
//! when it is a bare identifier `k` or the qualified `args.k`, and `k` names
//! one of the function's scalar arguments:
//!
//! ```text
//! text_search(corpus, query,    limit => k)   -- f('cancer', 5)
//! text_search(corpus, 'cancer', limit => 5)
//! ```
//!
//! The function argument always wins over a table or column with the same
//! name: in a function with an argument `text`, `text_search(corpus, q,
//! text)` passes the argument's value, not the `text` column. Only an
//! identifier that names a table-typed argument (`signature.tables`) is
//! never replaced, since it is registered as a table. A `NULL` argument
//! value is written as SQL `NULL`, so the table function reports it rather
//! than seeing an unknown column. Scalar function calls outside table
//! function arguments are left for `inline_args_into_plan`.
//!
//! Search UDTFs also require the query in the *second positional* slot and
//! do not read a named `query` argument, so a string `query => ...` on
//! `text_search` or `vector_search` is moved into that slot.

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

use super::sql::SQL_TABLE_ARGS_TABLE_NAME;

/// Rewrite `body` so table function calls in it receive the function's
/// scalar arguments (`schema`, `values`) as literals. `table_arg_names`
/// are the function's table-typed arguments, which are never replaced.
pub(crate) fn inline_table_function_args(
    body: &str,
    dialect: &dyn Dialect,
    schema: &Schema,
    values: &[ScalarValue],
    table_arg_names: &[&str],
) -> DataFusionResult<String> {
    let mut statements = DFParser::parse_sql_with_dialect(body, dialect)?;
    let scope = ArgScope::new(schema, values, table_arg_names);
    let mut rewriter = TableFunctionArgRewriter { scope: &scope };
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

/// The scalar function arguments a body can refer to, by lowercase name.
struct ArgScope {
    values: HashMap<String, ScalarValue>,
}

impl ArgScope {
    fn new(schema: &Schema, values: &[ScalarValue], table_arg_names: &[&str]) -> Self {
        let values = schema
            .fields()
            .iter()
            .zip(values)
            .map(|(field, value)| (field.name().to_ascii_lowercase(), value.clone()))
            .filter(|(name, _)| {
                !table_arg_names
                    .iter()
                    .any(|table| table.eq_ignore_ascii_case(name))
            })
            .collect();
        Self { values }
    }

    /// The literal for `expr` if it is `k` or `args.k` and `k` names a
    /// scalar function argument.
    fn resolve(&self, expr: &ast::Expr) -> Option<SqlLiteral> {
        let name = match expr {
            ast::Expr::Identifier(identifier) => &identifier.value,
            ast::Expr::CompoundIdentifier(parts) => match parts.as_slice() {
                [table, column] if table.value.eq_ignore_ascii_case(SQL_TABLE_ARGS_TABLE_NAME) => {
                    &column.value
                }
                _ => return None,
            },
            _ => return None,
        };
        SqlLiteral::from_scalar(self.values.get(&name.to_ascii_lowercase())?)
    }
}

struct TableFunctionArgRewriter<'a> {
    scope: &'a ArgScope,
}

impl VisitorMut for TableFunctionArgRewriter<'_> {
    type Break = ();

    // A table function used as a `FROM` source parses as a table factor:
    // `FROM f(...)` as `TableFactor::Table` with arguments, and
    // `LATERAL f(...)` as `TableFactor::Function`. Calls nested in its
    // arguments are `Expr::Function`s, handled by `inline_call_args`.
    fn pre_visit_table_factor(
        &mut self,
        table_factor: &mut ast::TableFactor,
    ) -> ControlFlow<Self::Break> {
        match table_factor {
            ast::TableFactor::Table {
                name,
                args: Some(table_args),
                ..
            } => inline_call_args(name, &mut table_args.args, self.scope),
            ast::TableFactor::Function { name, args, .. } => {
                inline_call_args(name, args, self.scope);
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

/// Inline function arguments into every argument of one table function
/// call, then into any function call nested in those arguments.
fn inline_call_args(name: &ast::ObjectName, args: &mut Vec<ast::FunctionArg>, scope: &ArgScope) {
    for arg in args.iter_mut() {
        if let ast::FunctionArgExpr::Expr(expr) = arg_expr_mut(arg) {
            inline_arg_expr(expr, scope);
        }
    }
    if is_search_udtf(&name.to_string()) {
        move_named_query_to_second_slot(args);
    }
}

fn inline_arg_expr(expr: &mut ast::Expr, scope: &ArgScope) {
    if let Some(literal) = scope.resolve(expr) {
        *expr = literal.into_expr();
        return;
    }
    if let ast::Expr::Function(function) = expr
        && let ast::FunctionArguments::List(list) = &mut function.args
    {
        inline_call_args(&function.name, &mut list.args, scope);
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

fn into_arg_expr(arg: ast::FunctionArg) -> ast::FunctionArgExpr {
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

    fn into_expr(self) -> ast::Expr {
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
        ast::Expr::Value(ast::ValueWithSpan::from(value))
    }
}

/// Search UDTF parsers read the query only from the second *positional*
/// slot. Move a `query => '...'` string argument into that slot when it is
/// empty, since the parsers would otherwise reject it as an unrecognized
/// named argument.
fn move_named_query_to_second_slot(args: &mut Vec<ast::FunctionArg>) {
    let mut positional = args
        .iter()
        .enumerate()
        .filter(|(_, arg)| !is_named(arg))
        .map(|(idx, _)| idx);
    let Some(table_idx) = positional.next() else {
        // No positional table argument — the query position can't be
        // resolved by convention, so leave the call as written.
        return;
    };
    if positional.next().is_some() {
        return;
    }

    let Some(query_idx) = args
        .iter()
        .position(|arg| named_key(arg).is_some_and(|name| name.eq_ignore_ascii_case("query")))
    else {
        return;
    };
    if !matches!(
        arg_expr_mut(&mut args[query_idx]),
        ast::FunctionArgExpr::Expr(ast::Expr::Value(ast::ValueWithSpan {
            value: ast::Value::SingleQuotedString(_),
            ..
        }))
    ) {
        return;
    }

    let query = into_arg_expr(args.remove(query_idx));
    let insert_at = if query_idx < table_idx {
        table_idx
    } else {
        table_idx + 1
    };
    args.insert(insert_at, ast::FunctionArg::Unnamed(query));
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field};
    use datafusion::sql::sqlparser::dialect::GenericDialect;

    use super::*;

    /// `text` deliberately shares its name with a column of `docs`, and
    /// `corpus` with a table-typed argument.
    fn schema() -> Schema {
        Schema::new(vec![
            Field::new("q", DataType::Utf8, true),
            Field::new("k", DataType::Int64, true),
            Field::new("text", DataType::Utf8, true),
            Field::new("s", DataType::Boolean, true),
            Field::new("w", DataType::Float64, true),
            Field::new("n", DataType::Int64, true),
            Field::new("u", DataType::UInt64, true),
            Field::new("corpus", DataType::Utf8, true),
        ])
    }

    fn values() -> Vec<ScalarValue> {
        vec![
            ScalarValue::Utf8(Some("hybrid search".to_string())),
            ScalarValue::Int64(Some(5)),
            ScalarValue::Utf8(Some("body".to_string())),
            ScalarValue::Boolean(Some(false)),
            ScalarValue::Float64(Some(2.0)),
            ScalarValue::Int64(None),
            ScalarValue::UInt64(Some(7)),
            ScalarValue::Utf8(Some("not a table".to_string())),
        ]
    }

    fn rewrite(body: &str) -> String {
        inline_table_function_args(body, &GenericDialect {}, &schema(), &values(), &["corpus"])
            .unwrap_or_else(|e| panic!("rewrite of {body:?} failed: {e}"))
    }

    fn assert_rewrites(cases: &[(&str, &str)]) {
        for (body, expected_fragment) in cases {
            let rewritten = rewrite(body);
            assert!(
                rewritten.contains(expected_fragment),
                "expected {rewritten:?} to contain {expected_fragment:?}"
            );
        }
    }

    /// Table-driven over the call shapes search UDTFs must accept: plain
    /// positional, named `query => q` before/after other named args, and
    /// both `vector_search`/`text_search` inside `rrf`. Every case must end
    /// up with the query argument as a literal in positional slot two,
    /// since that's the only place any search UDTF parser looks for it.
    #[test]
    fn normalizes_every_search_udtf_query_argument_shape() {
        assert_rewrites(&[
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
                "SELECT * FROM search.vector_search(docs, query => args.q)",
                "search.vector_search(docs, 'hybrid search')",
            ),
        ]);
    }

    /// Table-driven over table function calls of every kind. Each function
    /// argument must become a literal in the same position, since table
    /// functions read their arguments while they are planned.
    #[test]
    fn inlines_function_arguments_into_every_table_function_call() {
        assert_rewrites(&[
            // Search UDTFs, named and positional, including the 3rd slot.
            (
                "SELECT * FROM text_search(docs, q, limit => k)",
                "text_search(docs, 'hybrid search', limit => 5)",
            ),
            (
                "SELECT * FROM text_search(docs, query => q, limit => k)",
                "text_search(docs, 'hybrid search', limit => 5)",
            ),
            (
                "SELECT * FROM text_search(docs, q, k)",
                "text_search(docs, 'hybrid search', 5)",
            ),
            (
                "SELECT * FROM text_search(docs, q, body, k, s)",
                "text_search(docs, 'hybrid search', body, 5, false)",
            ),
            (
                "SELECT * FROM vector_search(docs, q, limit => u, include_score => s)",
                "vector_search(docs, 'hybrid search', limit => 7, include_score => false)",
            ),
            // Nested search calls in `rrf`, with `rrf` options.
            (
                "SELECT * FROM rrf(text_search(docs, q, limit => k), vector_search(docs, q, rank_weight => w), k => k)",
                "rrf(text_search(docs, 'hybrid search', limit => 5), vector_search(docs, 'hybrid search', rank_weight => 2.0), k => 5)",
            ),
            // `rerank` over a nested search call, with named options.
            (
                "SELECT * FROM rerank(vector_search(docs, q), document => body, query => q, limit => k)",
                "rerank(vector_search(docs, 'hybrid search'), document => body, query => 'hybrid search', limit => 5)",
            ),
            // Any other table function, whatever its name.
            (
                "SELECT * FROM flatten_json(q, max_depth => k)",
                "flatten_json('hybrid search', max_depth => 5)",
            ),
            (
                "SELECT * FROM my_decision(q, threshold => w, strict => s)",
                "my_decision('hybrid search', threshold => 2.0, strict => false)",
            ),
            (
                "SELECT * FROM t CROSS JOIN LATERAL json_tree(q, max_rows => k)",
                "json_tree('hybrid search', max_rows => 5)",
            ),
            // Qualified `args.k`, and a scalar call nested in an argument.
            (
                "SELECT * FROM text_search(docs, args.q, limit => args.k)",
                "text_search(docs, 'hybrid search', limit => 5)",
            ),
            (
                "SELECT * FROM text_search(docs, q, limit => abs(k))",
                "text_search(docs, 'hybrid search', limit => abs(5))",
            ),
            // A NULL argument is passed as SQL NULL.
            (
                "SELECT * FROM text_search(docs, q, limit => n)",
                "text_search(docs, 'hybrid search', limit => NULL)",
            ),
        ]);
    }

    /// The function argument wins over a column or table with the same
    /// name, in every slot.
    #[test]
    fn function_argument_wins_over_a_column_or_table_with_the_same_name() {
        assert_rewrites(&[
            (
                "SELECT * FROM text_search(docs, q, text)",
                "text_search(docs, 'hybrid search', 'body')",
            ),
            (
                "SELECT * FROM text_search(docs, q, column => text, limit => k)",
                "text_search(docs, 'hybrid search', column => 'body', limit => 5)",
            ),
            (
                "SELECT * FROM text_search(text, q)",
                "text_search('body', 'hybrid search')",
            ),
        ]);
    }

    /// A table-typed argument is registered as a table, so its name is
    /// never replaced, even if a scalar argument has the same name.
    #[test]
    fn leaves_table_argument_identifiers_untouched() {
        assert_rewrites(&[
            (
                "SELECT * FROM text_search(corpus, q, limit => k)",
                "text_search(corpus, 'hybrid search', limit => 5)",
            ),
            (
                "SELECT * FROM rerank(corpus, document => body, query => q)",
                "rerank(corpus, document => body, query => 'hybrid search')",
            ),
        ]);
    }

    #[test]
    fn leaves_unknown_identifiers_and_non_table_function_calls_untouched() {
        let rewritten = rewrite(
            "SELECT k, upper(q) FROM text_search(docs, q, limit => other) WHERE _score > w AND x.k = 1",
        );
        assert!(rewritten.contains("text_search(docs, 'hybrid search', limit => other)"));
        assert!(rewritten.contains("SELECT k, upper(q) FROM"));
        assert!(rewritten.contains("_score > w AND x.k = 1"));
    }

    #[test]
    fn leaves_non_identifier_arguments_untouched() {
        assert_rewrites(&[
            (
                "SELECT * FROM vector_search(docs, 'literal already')",
                "vector_search(docs, 'literal already')",
            ),
            (
                "SELECT * FROM some_other_udtf(docs, x.q, k + 1)",
                "some_other_udtf(docs, x.q, k + 1)",
            ),
        ]);
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
        assert_eq!(SqlLiteral::Float(0.5).into_expr().to_string(), "0.5");
        assert_eq!(SqlLiteral::Integer(-3).into_expr().to_string(), "-3");
    }
}
