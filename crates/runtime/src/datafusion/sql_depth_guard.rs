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
use datafusion::sql::parser::{DFParser, Statement as DFStatement};
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
/// Handles the `sqlparser` statement and the `EXPLAIN` wrapper; the other
/// `DataFusion` parser extensions (`CREATE EXTERNAL TABLE`, `COPY TO`, `RESET`)
/// carry no user expression tree that reaches the recursive planner.
pub(crate) fn ensure_statement_within_depth_limits(statement: &DFStatement) -> DFResult<()> {
    match statement {
        DFStatement::Statement(sql) => ensure_sql_statement_within_depth_limits(sql),
        DFStatement::Explain(explain) => ensure_statement_within_depth_limits(&explain.statement),
        DFStatement::CreateExternalTable(_) | DFStatement::CopyTo(_) | DFStatement::Reset(_) => {
            Ok(())
        }
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

fn too_deep_arrow_type_error() -> DataFusionError {
    DataFusionError::Plan(format!(
        "Query rejected: an 'arrow_cast' type string is nested more than {MAX_ARROW_TYPE_DEPTH} \
         levels deep. Simplify the target type."
    ))
}

/// If `expr` is an `arrow_cast(…, '<type string>')` call, bound the
/// parenthesis-nesting depth of each string-literal argument.
fn check_arrow_cast_type_depth(expr: &Expr) -> DFResult<()> {
    let Expr::Function(function) = expr else {
        return Ok(());
    };
    let is_arrow_cast = function
        .name
        .0
        .last()
        .and_then(|part| part.as_ident())
        .is_some_and(|ident| ident.value.eq_ignore_ascii_case("arrow_cast"));
    if !is_arrow_cast {
        return Ok(());
    }

    let FunctionArguments::List(list) = &function.args else {
        return Ok(());
    };
    for arg in &list.args {
        let arg_expr = match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(e))
            | FunctionArg::Named {
                arg: FunctionArgExpr::Expr(e),
                ..
            }
            | FunctionArg::ExprNamed {
                arg: FunctionArgExpr::Expr(e),
                ..
            } => e,
            _ => continue,
        };
        if let Expr::Value(value) = arg_expr
            && let Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) = &value.value
            && max_paren_depth(s) > MAX_ARROW_TYPE_DEPTH
        {
            return Err(too_deep_arrow_type_error());
        }
    }
    Ok(())
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
