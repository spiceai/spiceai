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

//! Inline literal table-function arguments into a [`LogicalPlan`].
//!
//! SQL table functions expose their scalar arguments via a one-row `args`
//! `MemTable`. Because the `MemTable` is only populated at execution time,
//! the planner sees column references rather than literals, which prevents
//! filter pushdown for connectors that need concrete values (e.g. the HTTP
//! connector's `request_path`).
//!
//! Since all scalar args are guaranteed to be literals (enforced by
//! [`super::sql::literal_arg`]), this module walks the *unoptimized*
//! [`LogicalPlan`] produced by `ctx.sql(body)` and replaces every
//! `Expr::Column` that references the `args` table with the corresponding
//! `Expr::Literal`.  The optimizer then sees constants and can fold /
//! push down as usual.
//!
//! This follows the same pattern as `DataFusion`'s own
//! [`LogicalPlan::replace_params_with_values`], which replaces
//! `Expr::Placeholder` with `Expr::Literal`.

use std::collections::HashMap;

use arrow::datatypes::Schema;
use datafusion::{
    common::{
        Column, Result as DataFusionResult,
        tree_node::{Transformed, TreeNode},
    },
    logical_expr::{LogicalPlan, Projection, TableScan, expr::Alias},
    prelude::Expr,
    scalar::ScalarValue,
};

use super::sql::SQL_TABLE_ARGS_TABLE_NAME;

/// Returns `true` if `table_name` refers to the `args` table.
fn is_args_table_ref(table_name: &datafusion::sql::TableReference) -> bool {
    table_name
        .table()
        .eq_ignore_ascii_case(SQL_TABLE_ARGS_TABLE_NAME)
}

/// If `plan` is `Projection([single_expr], TableScan("args"))` and
/// `single_expr` is a literal, return that literal.  This detects the
/// pattern left behind after column→literal replacement inside scalar
/// subqueries.
fn try_extract_literal_from_args_subquery(plan: &LogicalPlan) -> Option<Expr> {
    if let LogicalPlan::Projection(Projection { expr, input, .. }) = plan
        // Single projected expression over a TableScan on `args`.
        && let [expr] = expr.as_slice()
        && let LogicalPlan::TableScan(TableScan { table_name, .. }) = input.as_ref()
        && is_args_table_ref(table_name)
    {
        let inner = match expr {
            Expr::Alias(Alias { expr, .. }) => expr,
            other => other,
        };
        if matches!(inner, Expr::Literal(..)) {
            return Some(inner.clone());
        }
    }
    None
}

/// Recursively collapse `Expr::ScalarSubquery` nodes whose inner plan
/// is `Projection([literal], TableScan("args"))`.
///
/// `DataFusion`'s `Expr::transform_up` explicitly skips `ScalarSubquery`
/// children, so we must walk the expression tree manually to find and
/// replace them.
fn collapse_args_subqueries(expr: Expr) -> Expr {
    match expr {
        Expr::ScalarSubquery(ref subquery) => {
            if let Some(literal) = try_extract_literal_from_args_subquery(&subquery.subquery) {
                literal
            } else {
                expr
            }
        }
        // Recurse into expression types that can contain ScalarSubquery.
        Expr::BinaryExpr(mut bin) => {
            *bin.left = collapse_args_subqueries(*bin.left);
            *bin.right = collapse_args_subqueries(*bin.right);
            Expr::BinaryExpr(bin)
        }
        Expr::Not(inner) => Expr::Not(Box::new(collapse_args_subqueries(*inner))),
        Expr::IsNotNull(inner) => Expr::IsNotNull(Box::new(collapse_args_subqueries(*inner))),
        Expr::IsNull(inner) => Expr::IsNull(Box::new(collapse_args_subqueries(*inner))),
        Expr::IsTrue(inner) => Expr::IsTrue(Box::new(collapse_args_subqueries(*inner))),
        Expr::IsFalse(inner) => Expr::IsFalse(Box::new(collapse_args_subqueries(*inner))),
        Expr::Negative(inner) => Expr::Negative(Box::new(collapse_args_subqueries(*inner))),
        Expr::Cast(mut cast) => {
            *cast.expr = collapse_args_subqueries(*cast.expr);
            Expr::Cast(cast)
        }
        Expr::TryCast(mut cast) => {
            *cast.expr = collapse_args_subqueries(*cast.expr);
            Expr::TryCast(cast)
        }
        Expr::Alias(mut alias) => {
            *alias.expr = collapse_args_subqueries(*alias.expr);
            Expr::Alias(alias)
        }
        Expr::ScalarFunction(mut func) => {
            func.args = func
                .args
                .into_iter()
                .map(collapse_args_subqueries)
                .collect();
            Expr::ScalarFunction(func)
        }
        Expr::Case(mut case) => {
            case.expr = case.expr.map(|o| Box::new(collapse_args_subqueries(*o)));
            case.when_then_expr = case
                .when_then_expr
                .into_iter()
                .map(|(w, t)| {
                    (
                        Box::new(collapse_args_subqueries(*w)),
                        Box::new(collapse_args_subqueries(*t)),
                    )
                })
                .collect();
            case.else_expr = case
                .else_expr
                .map(|e| Box::new(collapse_args_subqueries(*e)));
            Expr::Case(case)
        }
        // For any other expression type, return as-is.
        other => other,
    }
}

/// Walk an unoptimized [`LogicalPlan`] and replace every `Expr::Column`
/// referencing the `args` table with the corresponding `Expr::Literal`.
///
/// Also collapses `Expr::ScalarSubquery` nodes that, after column
/// replacement, reduce to a single literal projected from `args`.
///
/// This operates on the plan produced *before* optimization, so the
/// optimizer's filter-pushdown passes see concrete literal values instead
/// of column references to a `MemTable`.
pub(super) fn inline_args_into_plan(
    plan: LogicalPlan,
    schema: &Schema,
    values: &[ScalarValue],
) -> DataFusionResult<LogicalPlan> {
    if schema.fields().is_empty() {
        return Ok(plan);
    }

    let arg_map: HashMap<String, ScalarValue> = schema
        .fields()
        .iter()
        .zip(values)
        .map(|(field, value)| (field.name().to_ascii_lowercase(), value.clone()))
        .collect();

    // Pass 1: replace `Expr::Column` refs to `args` with literals inside
    // all plans (including subquery plans).
    let plan = plan
        .transform_up_with_subqueries(|plan| {
            plan.map_expressions(|expr| {
                expr.transform_up(|e| {
                    if let Expr::Column(Column {
                        ref relation,
                        ref name,
                        ..
                    }) = e
                    {
                        let key = name.to_ascii_lowercase();
                        let should_replace = match relation {
                            Some(r) => is_args_table_ref(r) && arg_map.contains_key(&key),
                            None => arg_map.contains_key(&key),
                        };
                        if should_replace && let Some(value) = arg_map.get(&key) {
                            return Ok(Transformed::yes(Expr::Literal(value.clone(), None)));
                        }
                    }
                    Ok(Transformed::no(e))
                })
            })
        })?
        .data;

    // Pass 2: collapse `Expr::ScalarSubquery` nodes whose inner plan is
    // now `Projection([literal], TableScan("args"))`.  This turns the
    // subquery into a bare literal so the optimizer can push it down.
    //
    // We use a manual expression walk because DataFusion's
    // `Expr::transform_up` explicitly skips `ScalarSubquery` children.
    plan.transform_up_with_subqueries(|plan| {
        plan.map_expressions(|expr| {
            let collapsed = collapse_args_subqueries(expr.clone());
            if collapsed == expr {
                Ok(Transformed::no(expr))
            } else {
                Ok(Transformed::yes(collapsed))
            }
        })
    })
    .map(|res| res.data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Field as ArrowField;
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;
    use std::sync::Arc;

    /// Register a one-row `args` `MemTable` and plan the body SQL, returning
    /// the unoptimized plan.
    async fn plan_body(body: &str, schema: &Schema, values: &[ScalarValue]) -> LogicalPlan {
        let ctx = SessionContext::new();
        let schema_ref = Arc::new(schema.clone());

        // Build the one-row args MemTable.
        let arrays: Vec<_> = values
            .iter()
            .map(|v| v.to_array().expect("to_array"))
            .collect();
        let batch = arrow::record_batch::RecordBatch::try_new(Arc::clone(&schema_ref), arrays)
            .expect("batch");
        let table = MemTable::try_new(schema_ref, vec![vec![batch]]).expect("memtable");
        ctx.register_table("args", Arc::new(table))
            .expect("register");

        // Also register a dummy `raw_users` table so body SQL can reference it.
        let users_schema = Arc::new(arrow::datatypes::Schema::new(vec![
            ArrowField::new("content", arrow::datatypes::DataType::Utf8, true),
            ArrowField::new("request_path", arrow::datatypes::DataType::Utf8, true),
        ]));
        let users_table = MemTable::try_new(users_schema, vec![vec![]]).expect("users memtable");
        ctx.register_table("raw_users", Arc::new(users_table))
            .expect("register users");

        // Also register a dummy `t` table.
        let t_schema = Arc::new(arrow::datatypes::Schema::new(vec![
            ArrowField::new("id", arrow::datatypes::DataType::Int64, true),
            ArrowField::new("name", arrow::datatypes::DataType::Utf8, true),
            ArrowField::new("active", arrow::datatypes::DataType::Boolean, true),
            ArrowField::new("col", arrow::datatypes::DataType::Utf8, true),
        ]));
        let t_table = MemTable::try_new(t_schema, vec![vec![]]).expect("t memtable");
        ctx.register_table("t", Arc::new(t_table))
            .expect("register t");

        ctx.sql(body)
            .await
            .expect("plan body")
            .into_unoptimized_plan()
    }

    fn utf8_schema(names: &[&str]) -> Schema {
        Schema::new(
            names
                .iter()
                .map(|n| ArrowField::new(*n, arrow::datatypes::DataType::Utf8, true))
                .collect::<Vec<_>>(),
        )
    }

    /// Format the plan to a string for assertion.
    fn plan_str(plan: &LogicalPlan) -> String {
        format!("{plan}")
    }

    #[tokio::test]
    async fn inline_replaces_scalar_subquery() {
        let schema = utf8_schema(&["username"]);
        let values = vec![ScalarValue::Utf8(Some("pg".into()))];
        let body = "SELECT content FROM raw_users WHERE request_path = (SELECT username FROM args)";
        let plan = plan_body(body, &schema, &values).await;
        let rewritten = inline_args_into_plan(plan, &schema, &values).expect("rewrite");
        let s = plan_str(&rewritten);
        assert!(
            s.contains("Utf8(\"pg\")"),
            "Expected inlined literal in plan: {s}"
        );
    }

    #[tokio::test]
    async fn inline_replaces_expression_over_args() {
        let schema = utf8_schema(&["username"]);
        let values = vec![ScalarValue::Utf8(Some("pg".into()))];
        let body = "SELECT content FROM raw_users WHERE request_path = (SELECT concat('/users/', username) FROM args)";
        let plan = plan_body(body, &schema, &values).await;
        let rewritten = inline_args_into_plan(plan, &schema, &values).expect("rewrite");
        let s = plan_str(&rewritten);
        assert!(
            s.contains("Utf8(\"pg\")"),
            "Expected inlined literal in plan: {s}"
        );
        assert!(s.contains("concat"), "Should still contain concat: {s}");
    }

    #[tokio::test]
    async fn inline_replaces_from_args_direct() {
        // Body uses `FROM args` directly — columns should still be replaced.
        let schema = Schema::new(vec![ArrowField::new(
            "x",
            arrow::datatypes::DataType::Int64,
            true,
        )]);
        let values = vec![ScalarValue::Int64(Some(42))];
        let body = "SELECT x AS value, x * 2 AS doubled FROM args";
        let plan = plan_body(body, &schema, &values).await;
        let rewritten = inline_args_into_plan(plan, &schema, &values).expect("rewrite");
        let s = plan_str(&rewritten);
        assert!(
            s.contains("Int64(42)"),
            "Expected inlined literal 42 in plan: {s}"
        );
    }

    #[tokio::test]
    async fn inline_handles_multiple_args() {
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
        let plan = plan_body(body, &schema, &values).await;
        let rewritten = inline_args_into_plan(plan, &schema, &values).expect("rewrite");
        let s = plan_str(&rewritten);
        assert!(
            s.contains("Utf8(\"hello\")"),
            "Expected inlined 'hello': {s}"
        );
        assert!(s.contains("Int64(99)"), "Expected inlined 99: {s}");
    }

    #[tokio::test]
    async fn inline_empty_schema_is_noop() {
        let schema = Schema::empty();
        let values: Vec<ScalarValue> = vec![];
        // With an empty schema, just plan against `t` directly (no args table).
        let ctx = SessionContext::new();
        let t_schema = Arc::new(Schema::new(vec![ArrowField::new(
            "id",
            arrow::datatypes::DataType::Int64,
            true,
        )]));
        let t_table = MemTable::try_new(t_schema, vec![vec![]]).expect("t memtable");
        ctx.register_table("t", Arc::new(t_table))
            .expect("register t");
        let plan = ctx
            .sql("SELECT * FROM t")
            .await
            .expect("plan")
            .into_unoptimized_plan();
        let original = plan_str(&plan);
        let rewritten = inline_args_into_plan(plan, &schema, &values).expect("rewrite");
        assert_eq!(original, plan_str(&rewritten));
    }

    #[tokio::test]
    async fn inline_handles_boolean_and_null() {
        let schema = Schema::new(vec![ArrowField::new(
            "flag",
            arrow::datatypes::DataType::Boolean,
            true,
        )]);
        let values = vec![ScalarValue::Boolean(Some(true))];
        let body = "SELECT * FROM t WHERE active = (SELECT flag FROM args)";
        let plan = plan_body(body, &schema, &values).await;
        let rewritten = inline_args_into_plan(plan, &schema, &values).expect("rewrite");
        let s = plan_str(&rewritten);
        assert!(s.contains("Boolean(true)"), "Expected inlined boolean: {s}");
    }
}
