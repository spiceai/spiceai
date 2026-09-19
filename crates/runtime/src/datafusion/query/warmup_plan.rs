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

//! Turn a logical plan into a warmup template: the query *shape* with
//! equality-filter values replaced by placeholders, plus the columns those
//! placeholders stand for.
//!
//! Two queries that differ only in `col = <value>` hash to the same template
//! (`WHERE id = 1` and `WHERE id = 2` are one plan). Warmup fills each
//! placeholder from `SELECT DISTINCT` of that column in the dataset.

use std::sync::Arc;

use arrow::datatypes::Field;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::logical_expr::{BinaryExpr, Expr, LogicalPlan, Operator, expr::Placeholder};
use serde::{Deserialize, Serialize};

/// A query shape that can be replayed with fresh key values from the dataset.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct WarmupTemplate {
    /// SQL with `$1`, `$2`, … standing in for equality-filter values.
    pub sql: String,
    /// One entry per placeholder, in `$1`… order: the column whose distinct
    /// values should be bound.
    pub bindings: Vec<WarmupBinding>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct WarmupBinding {
    pub table: String,
    pub column: String,
}

/// Identity of a template so the first 10 distinct *plans* are kept, not the
/// first 10 SQL strings with different literals.
#[must_use]
pub(super) fn template_id(template: &WarmupTemplate) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::hash::DefaultHasher::new();
    template.sql.hash(&mut hasher);
    for binding in &template.bindings {
        binding.table.hash(&mut hasher);
        binding.column.hash(&mut hasher);
    }
    hasher.finish()
}

/// Parameterize equality filters (`col = literal` / `col = $n`) and unparse
/// the plan. Returns `None` when the plan cannot be expressed as warmup SQL.
pub(super) fn template_from_plan(plan: &LogicalPlan) -> Option<WarmupTemplate> {
    let mut bindings = Vec::new();
    let rewritten = plan
        .clone()
        .transform_up(|node| {
            let LogicalPlan::Filter(filter) = node else {
                return Ok(Transformed::no(node));
            };
            let predicate = parameterize_predicate(filter.predicate, &mut bindings);
            let new_filter =
                datafusion::logical_expr::logical_plan::Filter::try_new(predicate, filter.input)?;
            Ok(Transformed::yes(LogicalPlan::Filter(new_filter)))
        })
        .ok()?
        .data;

    fill_missing_tables(plan, &mut bindings);

    let sql = datafusion::sql::unparser::plan_to_sql(&rewritten)
        .ok()?
        .to_string();
    Some(WarmupTemplate { sql, bindings })
}

fn fill_missing_tables(plan: &LogicalPlan, bindings: &mut [WarmupBinding]) {
    if !bindings.iter().any(|binding| binding.table.is_empty()) {
        return;
    }
    let tables = cache::get_logical_plan_input_tables(plan);
    if tables.len() != 1 {
        return;
    }
    let Some(table) = tables.iter().next() else {
        return;
    };
    let name = table.to_string();
    for binding in bindings.iter_mut() {
        if binding.table.is_empty() {
            binding.table.clone_from(&name);
        }
    }
}

fn parameterize_predicate(expr: Expr, bindings: &mut Vec<WarmupBinding>) -> Expr {
    let fallback = expr.clone();
    expr.transform_up(|e| {
        let Some((column, value_on_right)) = equality_column(&e) else {
            return Ok(Transformed::no(e));
        };
        let placeholder_id = format!("${}", bindings.len() + 1);
        let table = column
            .relation
            .as_ref()
            .map_or_else(String::new, ToString::to_string);
        let data_type = match &e {
            Expr::BinaryExpr(BinaryExpr { left, right, .. }) => {
                if value_on_right {
                    literal_or_placeholder_type(right)
                } else {
                    literal_or_placeholder_type(left)
                }
            }
            _ => None,
        };
        bindings.push(WarmupBinding {
            table,
            column: column.name.clone(),
        });
        Ok(Transformed::yes(Expr::Column(column).eq(
            Expr::Placeholder(Placeholder {
                id: placeholder_id,
                field: data_type.map(|dt| Arc::new(Field::new("param", dt, true))),
            }),
        )))
    })
    .map_or(fallback, |t| t.data)
}

fn equality_column(expr: &Expr) -> Option<(datafusion::common::Column, bool)> {
    let Expr::BinaryExpr(BinaryExpr {
        left,
        op: Operator::Eq,
        right,
    }) = expr
    else {
        return None;
    };
    match (left.as_ref(), right.as_ref()) {
        (Expr::Column(col), v) if is_literal_or_placeholder(v) => Some((col.clone(), true)),
        (v, Expr::Column(col)) if is_literal_or_placeholder(v) => Some((col.clone(), false)),
        _ => None,
    }
}

fn is_literal_or_placeholder(expr: &Expr) -> bool {
    matches!(expr, Expr::Literal(_, _) | Expr::Placeholder(_))
}

fn literal_or_placeholder_type(expr: &Expr) -> Option<datafusion::arrow::datatypes::DataType> {
    match expr {
        Expr::Literal(scalar, _) => Some(scalar.data_type()),
        Expr::Placeholder(Placeholder { field, .. }) => {
            field.as_ref().map(|f| f.data_type().clone())
        }
        _ => None,
    }
}

/// `SELECT DISTINCT` SQL that yields one row per unique combination of a
/// template's bound columns. `None` when the template has no variables (run
/// the template SQL as-is) or the bindings span more than one table (the
/// distinct keys would not be a real join combination).
#[must_use]
pub(super) fn distinct_keys_sql(template: &WarmupTemplate) -> Option<String> {
    if template.bindings.is_empty() {
        return None;
    }
    let table = template.bindings[0].table.as_str();
    if table.is_empty() {
        return None;
    }
    if template.bindings.iter().any(|b| b.table != table) {
        return None;
    }
    let columns = template
        .bindings
        .iter()
        .map(|b| quote_ident(&b.column))
        .collect::<Vec<_>>()
        .join(", ");
    let quoted_table = table
        .split('.')
        .map(quote_ident)
        .collect::<Vec<_>>()
        .join(".");
    Some(format!("SELECT DISTINCT {columns} FROM {quoted_table}"))
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;

    async fn plan_of(sql: &str) -> LogicalPlan {
        let ctx = SessionContext::new();
        ctx.sql("CREATE TABLE orders (id INT, status VARCHAR)")
            .await
            .expect("create")
            .collect()
            .await
            .expect("collect create");
        ctx.sql(sql).await.expect("sql").logical_plan().clone()
    }

    #[tokio::test]
    async fn equality_literals_are_the_same_plan_shape() {
        let a = template_from_plan(&plan_of("SELECT id FROM orders WHERE id = 1").await)
            .expect("template");
        let b = template_from_plan(&plan_of("SELECT id FROM orders WHERE id = 2").await)
            .expect("template");
        assert_eq!(template_id(&a), template_id(&b));
        assert_eq!(a.bindings.len(), 1);
        assert_eq!(a.bindings[0].column, "id");
        assert!(
            distinct_keys_sql(&a).expect("distinct").contains("id"),
            "warmup should DISTINCT the bound column"
        );
    }

    #[tokio::test]
    async fn a_query_with_no_equality_filter_has_no_bindings() {
        let t =
            template_from_plan(&plan_of("SELECT count(*) FROM orders").await).expect("template");
        assert!(t.bindings.is_empty());
        assert!(distinct_keys_sql(&t).is_none());
    }

    #[tokio::test]
    async fn two_equality_filters_become_two_placeholders() {
        let t = template_from_plan(
            &plan_of("SELECT id FROM orders WHERE id = 1 AND status = 'open'").await,
        )
        .expect("template");
        assert_eq!(t.bindings.len(), 2);
        let cols: Vec<&str> = t.bindings.iter().map(|b| b.column.as_str()).collect();
        assert!(cols.contains(&"id"));
        assert!(cols.contains(&"status"));
    }
}
