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

//! Inline SQL table-function arguments used as the query text of search
//! UDTFs (`vector_search`, `text_search`) before `DataFusion` plans the
//! function body.
//!
//! Search UDTFs inspect their query argument while the table function
//! itself is being constructed during planning
//! (`runtime-search::rrf::parse_search_args`,
//! `runtime-search::full_text_udtf`, `runtime::embeddings::udtf`), and all
//! three require that argument in the *second positional* slot — none of
//! them consult a named `query` argument. A SQL-tier function's own
//! arguments, by contrast, are only replaced with literal values in
//! [`super::args_inliner::inline_args_into_plan`], which runs on the
//! logical plan *after* planning. That's too late for these UDTFs.
//!
//! This module closes the gap by rewriting the parsed SQL text: any
//! argument in the search UDTF's query position — whether already
//! positional or passed as `query => ...` — that resolves to a known
//! string-valued function argument is normalized into the second
//! positional slot as a literal, before the body is planned.

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
        normalize_query_argument(&mut arguments.args, self.values);
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
        normalize_query_argument(&mut table_args.args, self.values);
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

/// If `expr` is an identifier bound to a known `Utf8` argument value,
/// returns the literal `FunctionArgExpr` it should be replaced with.
fn resolve_identifier_literal(
    expr: &ast::FunctionArgExpr,
    values: &HashMap<String, ScalarValue>,
) -> Option<ast::FunctionArgExpr> {
    let ast::FunctionArgExpr::Expr(ast::Expr::Identifier(identifier)) = expr else {
        return None;
    };
    let ScalarValue::Utf8(Some(query)) = values.get(&identifier.value.to_ascii_lowercase())? else {
        return None;
    };
    Some(ast::FunctionArgExpr::Expr(ast::Expr::Value(
        ast::ValueWithSpan::from(ast::Value::SingleQuotedString(query.clone())),
    )))
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

    let Some(query_idx) = args
        .iter()
        .position(|arg| named_key(arg).is_some_and(|name| name.eq_ignore_ascii_case("query")))
    else {
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

    fn schema() -> Schema {
        Schema::new(vec![Field::new("q", DataType::Utf8, true)])
    }

    fn values() -> Vec<ScalarValue> {
        vec![ScalarValue::Utf8(Some("hybrid search".to_string()))]
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
