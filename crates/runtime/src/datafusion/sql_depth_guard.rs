/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! A pre-planning guard that rejects pathologically deep SQL before it reaches
//! `DataFusion`'s recursive logical planner.
//!
//! `sqlparser`'s own `recursion_limit` bounds only nested-descent grammar
//! (parentheses, subqueries). A *flat* binary-operator chain such as
//! `a OR b OR c OR …` parses left-deep with near-constant parser recursion but
//! builds an N-deep expression tree, which `DataFusion` then walks recursively
//! during planning, type coercion, and simplification with no depth cap. A
//! few kilobytes of such SQL overflows the worker-thread stack and aborts the
//! whole process (`fatal runtime error: stack overflow`) — an unauthenticated
//! crash-class denial of service. A separate recursive-descent path, the Arrow
//! type-string parser reached through `arrow_cast(x, '…')`, overflows the same
//! way on a deeply nested type such as `List(List(… Int64 …))`.
//!
//! This guard measures depth *iteratively, with an explicit stack and no
//! recursion*, so the check itself cannot overflow, and rejects an over-deep
//! statement as a normal per-request error (HTTP 400 / Flight `InvalidArgument`,
//! the same mapping `DataFusionError::Plan` already receives) before planning.

use std::ops::ControlFlow;

use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::sql::parser::{CopyToSource, DFParser, Statement as DFStatement};
use datafusion::sql::sqlparser::ast::{
    Array, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Statement as SQLStatement, Value,
    Visit, Visitor,
};
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;

/// Maximum nesting depth of a SQL expression tree accepted before planning.
///
/// A flat binary chain of `N` terms builds a tree roughly `N` deep. In a debug
/// build on a default ~2 MiB Tokio worker stack, `DataFusion`'s recursive
/// planning begins to overflow at a tree depth of roughly 750 (empirically an
/// `OR` chain of ~800 terms aborts the process while ~700 returns normally).
/// Release builds have more headroom, but the debug threshold is the binding
/// one because the guard and the planner run on the same worker stack. 512 sits
/// comfortably below that floor while still admitting any realistic query —
/// legitimate expressions almost never nest beyond a few dozen levels, and an
/// `IN` list or `VALUES` clause is wide, not deep, so it is unaffected.
pub(crate) const MAX_SQL_EXPR_DEPTH: usize = 512;

/// Maximum parenthesis-nesting depth accepted in an Arrow type string (the
/// literal second argument of `arrow_cast`).
///
/// The Arrow type-string parser (`arrow-schema`) is recursive-descent with no
/// depth cap, so `arrow_cast(NULL, 'List(List(… Int64 …))')` overflows the
/// stack at a nesting of roughly 200 in a debug build. 64 is far below that and
/// far above any real nested type (a handful of `List`/`Struct`/`Map` levels).
pub(crate) const MAX_ARROW_TYPE_DEPTH: usize = 64;

/// Reject `statement` if any of its expression trees, or any `arrow_cast` type
/// string it carries, is nested more deeply than the fixed limits above.
///
/// Handles the `sqlparser` statement, the `EXPLAIN` wrapper, and a query
/// nested in `COPY (query) TO`. `COPY <table> TO`, `CREATE EXTERNAL TABLE`,
/// and `RESET` carry no user expression tree that reaches the recursive planner.
pub(crate) fn ensure_statement_within_depth_limits(statement: &DFStatement) -> DFResult<()> {
    match statement {
        DFStatement::Statement(sql) => ensure_sql_statement_within_depth_limits(sql),
        DFStatement::Explain(explain) => ensure_statement_within_depth_limits(&explain.statement),
        DFStatement::CopyTo(copy) => match &copy.source {
            CopyToSource::Query(query) => {
                let mut collector = DepthCollector { result: Ok(()) };
                let _ = query.visit(&mut collector);
                collector.result
            }
            CopyToSource::Relation(_) => Ok(()),
        },
        DFStatement::CreateExternalTable(_) | DFStatement::Reset(_) => Ok(()),
    }
}

/// Parse `sql` (same dialect as the rest of the runtime) and apply
/// [`ensure_statement_within_depth_limits`] to every statement.
///
/// For entry points that only hold the SQL text. Parsing is iterative in
/// `sqlparser` for flat chains and does not overflow; only the subsequent
/// recursive planning does, which this guards.
pub(crate) fn ensure_sql_within_depth_limits(sql: &str) -> DFResult<()> {
    let statements = DFParser::parse_sql_with_dialect(sql, &PostgreSqlDialect {})?;
    for statement in &statements {
        ensure_statement_within_depth_limits(statement)?;
    }
    Ok(())
}

fn ensure_sql_statement_within_depth_limits(statement: &SQLStatement) -> DFResult<()> {
    // `sqlparser`'s visitor finds every top-level expression tree; its own
    // recursion is bounded by the parser's `recursion_limit` (nested-descent
    // grammar only). The unbounded flat chains live *inside* a single
    // expression, measured iteratively by `check_expr_depth`, which runs on an
    // expression tree's root before the visitor descends into it — so the
    // visitor never recurses into an over-deep tree.
    let mut collector = DepthCollector { result: Ok(()) };
    let _ = statement.visit(&mut collector);
    collector.result
}

struct DepthCollector {
    result: DFResult<()>,
}

impl Visitor for DepthCollector {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
        if let Err(e) = check_expr_depth(expr) {
            self.result = Err(e);
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    }
}

/// Iteratively measure the nesting depth of one expression tree, and the
/// parenthesis depth of any `arrow_cast` type string within it, using an
/// explicit work stack so the measurement itself never recurses.
fn check_expr_depth(root: &Expr) -> DFResult<()> {
    let mut stack: Vec<(&Expr, usize)> = vec![(root, 1)];
    let mut children: Vec<&Expr> = Vec::new();
    while let Some((expr, depth)) = stack.pop() {
        if depth > MAX_SQL_EXPR_DEPTH {
            return Err(too_deep_expr_error());
        }
        check_arrow_cast_type_depth(expr)?;
        children.clear();
        push_child_exprs(expr, &mut children);
        for child in children.drain(..) {
            stack.push((child, depth + 1));
        }
    }
    Ok(())
}

fn too_deep_expr_error() -> DataFusionError {
    DataFusionError::Plan(format!(
        "Query rejected: an expression is nested more than {MAX_SQL_EXPR_DEPTH} levels deep. \
         Simplify the query (for example, replace a long chain of OR/AND conditions with an IN list \
         or a join)."
    ))
}

fn too_deep_arrow_type_error(function: &str) -> DataFusionError {
    DataFusionError::Plan(format!(
        "Query rejected: an '{function}' type string is nested more than {MAX_ARROW_TYPE_DEPTH} \
         levels deep. Simplify the target type."
    ))
}

/// If `expr` is an `arrow_cast` or `arrow_try_cast` call, bound the
/// parenthesis-nesting depth of its type argument.
///
/// The type is the argument named `datatype`, or the second argument when
/// none is named (`arrow_cast(expression, datatype)`). A string in the value
/// being cast is not a type and is not measured here.
fn check_arrow_cast_type_depth(expr: &Expr) -> DFResult<()> {
    let Expr::Function(function) = expr else {
        return Ok(());
    };
    let Some(name) = function
        .name
        .0
        .last()
        .and_then(|part| part.as_ident())
        .map(|ident| ident.value.as_str())
        .filter(|name| {
            name.eq_ignore_ascii_case("arrow_cast") || name.eq_ignore_ascii_case("arrow_try_cast")
        })
    else {
        return Ok(());
    };

    let FunctionArguments::List(list) = &function.args else {
        return Ok(());
    };
    let Some(type_expr) = arrow_cast_type_expr(&list.args) else {
        return Ok(());
    };
    let Some(type_text) = string_literal_text(type_expr) else {
        return Ok(());
    };
    if max_paren_depth(type_text) > MAX_ARROW_TYPE_DEPTH {
        return Err(too_deep_arrow_type_error(name));
    }
    Ok(())
}

/// The text of a string literal, after grouping parentheses around it are removed.
///
/// `arrow_cast(x, ('Int64'))` parses the type argument as [`Expr::Nested`], not
/// as a bare literal. Those parentheses are grouping, so the string inside is
/// still the type the Arrow parser will recurse through. Escaped (`E'…'`) and
/// dollar-quoted (`$$…$$`) literals are included because planning turns them
/// into the same UTF-8 type string as a single-quoted literal.
fn string_literal_text(expr: &Expr) -> Option<&str> {
    let mut current = expr;
    while let Expr::Nested(inner) = current {
        current = inner.as_ref();
    }
    let Expr::Value(value) = current else {
        return None;
    };
    match &value.value {
        Value::SingleQuotedString(text)
        | Value::DoubleQuotedString(text)
        | Value::EscapedStringLiteral(text) => Some(text.as_str()),
        Value::DollarQuotedString(text) => Some(text.value.as_str()),
        _ => None,
    }
}

/// The type argument of `arrow_cast(expression, datatype)`.
fn arrow_cast_type_expr(args: &[FunctionArg]) -> Option<&Expr> {
    let named = args.iter().find_map(|arg| match arg {
        FunctionArg::Named {
            name,
            arg: FunctionArgExpr::Expr(expr),
            ..
        } if name.value.eq_ignore_ascii_case("datatype") => Some(expr),
        FunctionArg::ExprNamed {
            name,
            arg: FunctionArgExpr::Expr(expr),
            ..
        } if expr_is_ident(name, "datatype") => Some(expr),
        _ => None,
    });
    named.or_else(|| {
        let arg = args.get(1)?;
        match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))
            | FunctionArg::Named {
                arg: FunctionArgExpr::Expr(expr),
                ..
            }
            | FunctionArg::ExprNamed {
                arg: FunctionArgExpr::Expr(expr),
                ..
            } => Some(expr),
            _ => None,
        }
    })
}

fn expr_is_ident(expr: &Expr, name: &str) -> bool {
    match expr {
        Expr::Identifier(ident) => ident.value.eq_ignore_ascii_case(name),
        Expr::CompoundIdentifier(parts) => parts
            .last()
            .is_some_and(|ident| ident.value.eq_ignore_ascii_case(name)),
        _ => false,
    }
}

/// Maximum run of unmatched `(` in `s` — a cheap upper bound on the nesting the
/// Arrow type parser would recurse through.
fn max_paren_depth(s: &str) -> usize {
    let mut depth = 0usize;
    let mut max = 0usize;
    for c in s.bytes() {
        match c {
            b'(' => {
                depth += 1;
                max = max.max(depth);
            }
            b')' => depth = depth.saturating_sub(1),
            _ => {}
        }
    }
    max
}

/// Push the direct child expressions of `expr` onto `out`.
///
/// Covers every `sqlparser` [`Expr`] variant that holds nested expressions; the
/// unbounded attack shapes (binary/unary chains, nested casts, `CASE`, function
/// arguments, collections) are enumerated explicitly. Variants that hold a
/// subquery are deliberately not descended here — subquery nesting is already
/// bounded by the parser's `recursion_limit`, and a subquery's own expressions
/// are reached by the statement-level visitor.
fn push_child_exprs<'a>(expr: &'a Expr, out: &mut Vec<&'a Expr>) {
    match expr {
        Expr::BinaryOp { left, right, .. }
        | Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right) => {
            out.push(left.as_ref());
            out.push(right.as_ref());
        }
        Expr::UnaryOp { expr, .. }
        | Expr::Cast { expr, .. }
        | Expr::Nested(expr)
        | Expr::Collate { expr, .. }
        | Expr::IsFalse(expr)
        | Expr::IsNotFalse(expr)
        | Expr::IsTrue(expr)
        | Expr::IsNotTrue(expr)
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::IsUnknown(expr)
        | Expr::IsNotUnknown(expr) => out.push(expr.as_ref()),
        Expr::Between {
            expr, low, high, ..
        } => {
            out.push(expr.as_ref());
            out.push(low.as_ref());
            out.push(high.as_ref());
        }
        Expr::Like { expr, pattern, .. }
        | Expr::ILike { expr, pattern, .. }
        | Expr::SimilarTo { expr, pattern, .. }
        | Expr::RLike { expr, pattern, .. } => {
            out.push(expr.as_ref());
            out.push(pattern.as_ref());
        }
        Expr::InList { expr, list, .. } => {
            out.push(expr.as_ref());
            out.extend(list.iter());
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(op) = operand {
                out.push(op.as_ref());
            }
            for when in conditions {
                out.push(&when.condition);
                out.push(&when.result);
            }
            if let Some(e) = else_result {
                out.push(e.as_ref());
            }
        }
        Expr::Tuple(exprs) | Expr::Array(Array { elem: exprs, .. }) => out.extend(exprs.iter()),
        Expr::Function(function) => {
            if let FunctionArguments::List(list) = &function.args {
                for arg in &list.args {
                    match arg {
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(e))
                        | FunctionArg::Named {
                            arg: FunctionArgExpr::Expr(e),
                            ..
                        }
                        | FunctionArg::ExprNamed {
                            arg: FunctionArgExpr::Expr(e),
                            ..
                        } => out.push(e),
                        _ => {}
                    }
                }
            }
            if let Some(filter) = &function.filter {
                out.push(filter.as_ref());
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(sql: &str) -> DFStatement {
        DFParser::parse_sql_with_dialect(sql, &PostgreSqlDialect {})
            .expect("test SQL parses")
            .pop_front()
            .expect("one statement")
    }

    fn depth_check(sql: &str) -> DFResult<()> {
        ensure_statement_within_depth_limits(&parse(sql))
    }

    /// `SELECT 1 WHERE a OR a OR …` has no table and no parentheses. The
    /// parser consumes the chain iteratively, so this is the shape that
    /// reaches planning with an N-deep tree. Over the cap it is rejected.
    #[test]
    fn rejects_flat_or_chain_without_from() {
        let terms = vec!["id = 1"; MAX_SQL_EXPR_DEPTH + 500];
        let sql = format!("SELECT 1 WHERE {}", terms.join(" OR "));
        let err = depth_check(&sql).expect_err("flat OR chain is rejected");
        assert!(
            err.to_string().contains("nested more than"),
            "unexpected error: {err}"
        );
    }

    /// A flat OR chain far over the cap is rejected (not crashed), with the
    /// expression-depth message.
    #[test]
    fn rejects_deep_or_chain() {
        let terms = vec!["1 = 1"; MAX_SQL_EXPR_DEPTH + 500];
        let sql = format!("SELECT * FROM t WHERE {}", terms.join(" OR "));
        let err = depth_check(&sql).expect_err("deep OR chain is rejected");
        assert!(
            err.to_string().contains("nested more than"),
            "unexpected error: {err}"
        );
    }

    /// A flat AND chain far over the cap is rejected the same way.
    #[test]
    fn rejects_deep_and_chain() {
        let terms = vec!["1 = 1"; MAX_SQL_EXPR_DEPTH + 500];
        let sql = format!("SELECT * FROM t WHERE {}", terms.join(" AND "));
        let _ = depth_check(&sql).expect_err("deep AND chain is rejected");
    }

    /// A moderately long chain under the cap still passes the guard.
    #[test]
    fn allows_moderate_chain() {
        let terms = vec!["1 = 1"; 300];
        let sql = format!("SELECT * FROM t WHERE {}", terms.join(" OR "));
        depth_check(&sql).expect("a 300-term chain is under the cap");
    }

    /// A deeply nested `arrow_cast` type string is rejected.
    #[test]
    fn rejects_deep_arrow_cast_type() {
        let open = "List(".repeat(MAX_ARROW_TYPE_DEPTH + 50);
        let close = ")".repeat(MAX_ARROW_TYPE_DEPTH + 50);
        let sql = format!("SELECT arrow_cast(NULL, '{open}Int64{close}')");
        let err = depth_check(&sql).expect_err("deep arrow_cast type is rejected");
        assert!(
            err.to_string().contains("arrow_cast"),
            "unexpected error: {err}"
        );
    }

    /// A deep `arrow_cast` type string wrapped in `count(*)` is still rejected
    /// (the type string is reached wherever the call appears).
    #[test]
    fn rejects_deep_arrow_cast_type_in_aggregate() {
        let open = "List(".repeat(MAX_ARROW_TYPE_DEPTH + 50);
        let close = ")".repeat(MAX_ARROW_TYPE_DEPTH + 50);
        let sql = format!("SELECT count(arrow_cast(NULL, '{open}Int64{close}'))");
        let _ = depth_check(&sql).expect_err("deep arrow_cast type is rejected");
    }

    /// Parentheses in the value being cast are not a type string.
    #[test]
    fn allows_parenthesized_arrow_cast_value() {
        let open = "(".repeat(MAX_ARROW_TYPE_DEPTH + 5);
        let close = ")".repeat(MAX_ARROW_TYPE_DEPTH + 5);
        let sql = format!("SELECT arrow_cast('{open}hello{close}', 'Utf8')");
        depth_check(&sql).expect("parentheses in the cast value are allowed");
    }

    /// A deep type passed by the `datatype` name is still the type argument.
    #[test]
    fn rejects_named_arrow_cast_type() {
        let open = "List(".repeat(MAX_ARROW_TYPE_DEPTH + 50);
        let close = ")".repeat(MAX_ARROW_TYPE_DEPTH + 50);
        let sql = format!("SELECT arrow_cast(a, datatype => '{open}Int64{close}') FROM t");
        let err = depth_check(&sql).expect_err("named datatype is checked");
        assert!(
            err.to_string().contains("arrow_cast"),
            "unexpected error: {err}"
        );
    }

    /// `arrow_try_cast` parses the same type string.
    #[test]
    fn rejects_deep_arrow_try_cast_type() {
        let open = "List(".repeat(MAX_ARROW_TYPE_DEPTH + 50);
        let close = ")".repeat(MAX_ARROW_TYPE_DEPTH + 50);
        let sql = format!("SELECT arrow_try_cast(NULL, '{open}Int64{close}')");
        let err = depth_check(&sql).expect_err("deep arrow_try_cast type is rejected");
        assert!(
            err.to_string().contains("arrow_try_cast"),
            "unexpected error: {err}"
        );
    }

    /// Parentheses around the type literal are grouping. The string inside is
    /// still the type argument, for both cast functions.
    #[test]
    fn rejects_parenthesized_deep_arrow_cast_type() {
        let open = "List(".repeat(MAX_ARROW_TYPE_DEPTH + 50);
        let close = ")".repeat(MAX_ARROW_TYPE_DEPTH + 50);
        let sql = format!("SELECT arrow_cast(NULL, ('{open}Int64{close}'))");
        let err = depth_check(&sql).expect_err("parenthesized deep arrow_cast type is rejected");
        assert!(
            err.to_string().contains("arrow_cast"),
            "unexpected error: {err}"
        );

        let sql = format!("SELECT arrow_try_cast(NULL, (('{open}Int64{close}')))");
        let err =
            depth_check(&sql).expect_err("parenthesized deep arrow_try_cast type is rejected");
        assert!(
            err.to_string().contains("arrow_try_cast"),
            "unexpected error: {err}"
        );
    }

    /// A shallow parenthesized type is still a normal cast.
    #[test]
    fn allows_parenthesized_shallow_arrow_cast_type() {
        depth_check("SELECT arrow_cast(NULL, ('Int64'))")
            .expect("a shallow parenthesized type is allowed");
        depth_check("SELECT arrow_try_cast(NULL, (('Utf8')))")
            .expect("a shallow double-parenthesized type is allowed");
    }

    /// Escaped and dollar-quoted literals are still the type string. `DataFusion`
    /// plans both as UTF-8 literals and then parses them as Arrow types.
    #[test]
    fn rejects_deep_escaped_and_dollar_quoted_arrow_cast_type() {
        let open = "List(".repeat(MAX_ARROW_TYPE_DEPTH + 50);
        let close = ")".repeat(MAX_ARROW_TYPE_DEPTH + 50);
        let escaped = format!("SELECT arrow_cast(NULL, E'{open}Int64{close}')");
        let err = depth_check(&escaped).expect_err("escaped deep arrow_cast type is rejected");
        assert!(
            err.to_string().contains("arrow_cast"),
            "unexpected error: {err}"
        );

        let dollar = format!("SELECT arrow_try_cast(NULL, $${open}Int64{close}$$)");
        let err =
            depth_check(&dollar).expect_err("dollar-quoted deep arrow_try_cast type is rejected");
        assert!(
            err.to_string().contains("arrow_try_cast"),
            "unexpected error: {err}"
        );
    }

    /// Shallow escaped and dollar-quoted types are normal casts.
    #[test]
    fn allows_shallow_escaped_and_dollar_quoted_arrow_cast_type() {
        depth_check("SELECT arrow_cast(NULL, E'Int64')")
            .expect("a shallow escaped type is allowed");
        depth_check("SELECT arrow_try_cast(NULL, $$Utf8$$)")
            .expect("a shallow dollar-quoted type is allowed");
    }

    /// A query wrapped in `COPY (...) TO` is planned, so it is depth-checked.
    #[test]
    fn rejects_deep_or_chain_inside_copy_to() {
        let terms = vec!["1 = 1"; MAX_SQL_EXPR_DEPTH + 500];
        let sql = format!(
            "COPY (SELECT * FROM t WHERE {}) TO 'out.parquet'",
            terms.join(" OR ")
        );
        let err = depth_check(&sql).expect_err("COPY query is depth-checked");
        assert!(
            err.to_string().contains("nested more than"),
            "unexpected error: {err}"
        );
    }

    /// A shallow `COPY` query, and `COPY` of a table, pass.
    #[test]
    fn allows_shallow_copy_to() {
        depth_check("COPY (SELECT * FROM t WHERE a > 1) TO 'out.parquet'")
            .expect("a shallow COPY query passes");
        depth_check("COPY t TO 'out.parquet'").expect("COPY of a table passes");
    }

    /// A shallow, normal `arrow_cast` passes.
    #[test]
    fn allows_normal_arrow_cast() {
        depth_check("SELECT arrow_cast(a, 'List(Int64)') FROM t")
            .expect("a shallow arrow_cast passes");
    }

    /// A wide (not deep) `IN` list is unaffected by the depth cap.
    #[test]
    fn allows_wide_in_list() {
        let items: Vec<String> = (0..5000).map(|i| i.to_string()).collect();
        let sql = format!("SELECT * FROM t WHERE x IN ({})", items.join(", "));
        depth_check(&sql).expect("a wide IN list is not deep");
    }

    /// An ordinary query passes.
    #[test]
    fn allows_normal_query() {
        depth_check("SELECT a, b FROM t WHERE a > 1 AND b < 2 ORDER BY a LIMIT 10")
            .expect("a normal query passes");
    }

    #[test]
    fn max_paren_depth_counts_nesting() {
        assert_eq!(max_paren_depth(""), 0);
        assert_eq!(max_paren_depth("Int64"), 0);
        assert_eq!(max_paren_depth("List(Int64)"), 1);
        assert_eq!(max_paren_depth("List(List(Int64))"), 2);
        assert_eq!(max_paren_depth("a(b)c(d(e))"), 2);
    }
}
