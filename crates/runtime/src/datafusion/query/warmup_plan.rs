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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::datatypes::Field;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::logical_expr::{BinaryExpr, Expr, LogicalPlan, Operator, expr::Placeholder};
use datafusion::sql::TableReference;
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
    let mut disjunctive_equality = false;
    let rewritten = plan
        .clone()
        .transform_up(|node| {
            let LogicalPlan::Filter(filter) = node else {
                return Ok(Transformed::no(node));
            };
            if equality_under_disjunction(&filter.predicate) {
                disjunctive_equality = true;
            }
            let predicate = parameterize_predicate(filter.predicate, &mut bindings);
            let new_filter =
                datafusion::logical_expr::logical_plan::Filter::try_new(predicate, filter.input)?;
            Ok(Transformed::yes(LogicalPlan::Filter(new_filter)))
        })
        .ok()?
        .data;

    finalize_binding_tables(plan, &mut bindings);

    // Row-wise DISTINCT cannot reconstruct OR parameter tuples (e.g. `id = 1 OR
    // id = 2` needs `(1, 2)`, but `SELECT DISTINCT id, id` only yields diagonals).
    // Clear table names so `template_can_warm` rejects via the empty-table path.
    if disjunctive_equality {
        for binding in &mut bindings {
            binding.table.clear();
        }
    }

    let sql = datafusion::sql::unparser::plan_to_sql(&rewritten)
        .ok()?
        .to_string();
    Some(WarmupTemplate { sql, bindings })
}

/// Resolve `FROM orders AS o` bindings to the input table, fill a missing
/// name when the plan has one table, and drop names that are not a real
/// input table (an alias over a subquery is not something DISTINCT can scan).
fn finalize_binding_tables(plan: &LogicalPlan, bindings: &mut [WarmupBinding]) {
    resolve_alias_tables(plan, bindings);
    fill_missing_tables(plan, bindings);
    drop_unresolved_alias_tables(plan, bindings);
}

fn resolve_alias_tables(plan: &LogicalPlan, bindings: &mut [WarmupBinding]) {
    let aliases = alias_to_input_table(plan);
    for binding in bindings.iter_mut() {
        if let Some(table) = alias_target(&aliases, &binding.table) {
            binding.table.clone_from(table);
        }
    }
}

fn alias_target<'a>(aliases: &'a HashMap<String, String>, name: &str) -> Option<&'a String> {
    aliases.get(name).or_else(|| {
        let parsed = TableReference::parse_str(name);
        aliases.get(parsed.table())
    })
}

fn alias_to_input_table(plan: &LogicalPlan) -> HashMap<String, String> {
    let mut map = HashMap::new();
    let _ = plan.apply(|node| {
        if let LogicalPlan::SubqueryAlias(alias) = node
            && let Some(table) = underlying_table_scan(alias.input.as_ref())
        {
            map.insert(alias.alias.to_string(), table.clone());
            map.insert(alias.alias.table().to_string(), table.clone());
            map.insert(quote_table_reference(&alias.alias), table);
        }
        Ok(TreeNodeRecursion::Continue)
    });
    map
}

/// Follow nested aliases down to a `TableScan`. A subquery or projection in
/// between is not a table we can `SELECT DISTINCT` from.
fn underlying_table_scan(plan: &LogicalPlan) -> Option<String> {
    match plan {
        LogicalPlan::TableScan(scan) => Some(quote_table_reference(&scan.table_name)),
        LogicalPlan::SubqueryAlias(alias) => underlying_table_scan(alias.input.as_ref()),
        _ => None,
    }
}

fn drop_unresolved_alias_tables(plan: &LogicalPlan, bindings: &mut [WarmupBinding]) {
    let inputs = cache::get_logical_plan_input_tables(plan);
    for binding in bindings.iter_mut() {
        if !binding.table.is_empty() && !is_input_table(&binding.table, &inputs) {
            binding.table.clear();
        }
    }
}

fn is_input_table(name: &str, inputs: &HashSet<TableReference>) -> bool {
    let parsed = TableReference::parse_str(name);
    inputs.iter().any(|table| {
        quote_table_reference(table) == name
            || table.to_string() == name
            || table.table() == name
            || table_refs_match(table, &parsed)
    })
}

fn table_refs_match(left: &TableReference, right: &TableReference) -> bool {
    left.table() == right.table()
        && (left.schema().is_none() || right.schema().is_none() || left.schema() == right.schema())
        && (left.catalog().is_none()
            || right.catalog().is_none()
            || left.catalog() == right.catalog())
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
    let name = quote_table_reference(table);
    for binding in bindings.iter_mut() {
        if binding.table.is_empty() {
            binding.table.clone_from(&name);
        }
    }
}

/// True when an equality-to-literal sits beneath `OR`. Row-wise DISTINCT cannot
/// replay those parameter combinations, so such templates must not be recorded.
fn equality_under_disjunction(expr: &Expr) -> bool {
    fn walk(expr: &Expr, under_or: bool) -> bool {
        if equality_column(expr).is_some() {
            return under_or;
        }
        let next_under_or = under_or
            || matches!(
                expr,
                Expr::BinaryExpr(BinaryExpr {
                    op: Operator::Or,
                    ..
                })
            );
        let mut found = false;
        let _ = expr.apply_children(|child| {
            if walk(child, next_under_or) {
                found = true;
            }
            // `apply_children` only visits direct children; `walk` recurses.
            Ok(TreeNodeRecursion::Continue)
        });
        found
    }
    walk(expr, false)
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
            .map_or_else(String::new, quote_table_reference);
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

/// Whether this template can be replayed after a refresh.
///
/// No bindings: run the template SQL as-is. Bindings on one named table: fill
/// placeholders from `SELECT DISTINCT`. Bindings that span tables, have an
/// empty table name, repeat the same column (including equalities under `OR`),
/// or come from a disjunctive predicate cannot produce the parameter tuples
/// that match recorded cache keys, so they are not recorded or replayed.
#[must_use]
pub(super) fn template_can_warm(template: &WarmupTemplate) -> bool {
    template.bindings.is_empty() || distinct_keys_sql(template).is_some()
}

/// Upper bound on `SELECT DISTINCT` rows pulled for one template. Caps both
/// the key query and the number of replay executions when LRU eviction keeps
/// `size` below `max_size`.
pub(super) const MAX_WARMUP_DISTINCT_KEYS: usize = 1024;

/// `SELECT DISTINCT` SQL that yields one row per unique combination of a
/// template's bound columns. `None` when the template has no variables (run
/// the template SQL as-is), the bindings span more than one table (the
/// distinct keys would not be a real join combination), a binding has an empty
/// table name, or the same column is bound more than once (row-wise DISTINCT
/// cannot reconstruct disjunctive parameter tuples such as `(1, 2)` for
/// `id = 1 OR id = 2`).
///
/// The `FROM` clause quotes each `TableReference` part so `spice.public.orders`
/// stays catalog-qualified and a single identifier such as `users.v1` is not
/// split into schema and table.
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
    let mut seen_columns = HashSet::new();
    for binding in &template.bindings {
        if !seen_columns.insert(binding.column.as_str()) {
            // Repeated columns ⇒ `SELECT DISTINCT id, id` only yields diagonals.
            return None;
        }
    }
    let columns = template
        .bindings
        .iter()
        .map(|b| quote_ident(&b.column))
        .collect::<Vec<_>>()
        .join(", ");
    let quoted_table = quote_table_reference(&TableReference::parse_str(table));
    Some(format!(
        "SELECT DISTINCT {columns} FROM {quoted_table} LIMIT {MAX_WARMUP_DISTINCT_KEYS}"
    ))
}

/// Quote each `TableReference` part. Display-flattening then splitting on `.`
/// would turn a single identifier `users.v1` into schema `users` / table `v1`.
fn quote_table_reference(table: &TableReference) -> String {
    match (table.catalog(), table.schema()) {
        (Some(catalog), Some(schema)) => format!(
            "{}.{}.{}",
            quote_ident(catalog),
            quote_ident(schema),
            quote_ident(table.table())
        ),
        (None, Some(schema)) => {
            format!("{}.{}", quote_ident(schema), quote_ident(table.table()))
        }
        _ => quote_ident(table.table()),
    }
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
        let distinct = distinct_keys_sql(&a).expect("distinct");
        assert!(
            distinct.contains("id"),
            "warmup should DISTINCT the bound column"
        );
        assert!(
            distinct.contains(&format!("LIMIT {MAX_WARMUP_DISTINCT_KEYS}")),
            "DISTINCT keys must be bounded so a high-cardinality column cannot materialize unbounded rows"
        );
        assert!(template_can_warm(&a));
    }

    #[tokio::test]
    async fn a_query_with_no_equality_filter_has_no_bindings() {
        let t =
            template_from_plan(&plan_of("SELECT count(*) FROM orders").await).expect("template");
        assert!(t.bindings.is_empty());
        assert!(distinct_keys_sql(&t).is_none());
        assert!(
            template_can_warm(&t),
            "no-variable templates run as-is at warmup"
        );
    }

    #[tokio::test]
    async fn aliased_single_table_query_resolves_binding_to_input_table() {
        let t = template_from_plan(&plan_of("SELECT id FROM orders AS o WHERE o.id = 1").await)
            .expect("template");
        assert_eq!(t.bindings.len(), 1, "got {t:?}");
        assert_eq!(
            t.bindings[0].table, r#""orders""#,
            "binding must name the input table, not the alias, got {t:?}"
        );
        assert_eq!(t.bindings[0].column, "id");
        let distinct = distinct_keys_sql(&t).expect("distinct");
        assert!(
            distinct.contains("\"orders\""),
            "DISTINCT must read the input table, got {distinct}"
        );
        assert!(
            !distinct.contains("\"o\""),
            "DISTINCT must not use the SQL alias as a table name, got {distinct}"
        );
        assert!(
            template_can_warm(&t),
            "an aliased single-table query must be warmable"
        );

        let ctx = SessionContext::new();
        ctx.sql("CREATE TABLE orders (id INT, status VARCHAR)")
            .await
            .expect("create")
            .collect()
            .await
            .expect("collect create");
        ctx.sql(&distinct)
            .await
            .expect("DISTINCT must execute against the input table, not the alias")
            .collect()
            .await
            .expect("collect distinct");
    }

    #[tokio::test]
    async fn subquery_alias_that_is_not_a_table_is_not_warmable() {
        let t = template_from_plan(
            &plan_of("SELECT id FROM (SELECT id FROM orders) AS o WHERE o.id = 1").await,
        )
        .expect("template");
        assert!(
            !template_can_warm(&t),
            "an alias over a subquery is not a table we can DISTINCT from, got {t:?}"
        );
        assert!(
            distinct_keys_sql(&t).is_none(),
            "unresolved aliases must not produce DISTINCT SQL against the alias, got {t:?}"
        );
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
        assert!(
            template_can_warm(&t),
            "single-table bindings must be warmable"
        );
    }

    async fn plan_of_join(sql: &str) -> LogicalPlan {
        let ctx = SessionContext::new();
        ctx.sql("CREATE TABLE orders (id INT, customer_id INT)")
            .await
            .expect("create orders")
            .collect()
            .await
            .expect("collect create orders");
        ctx.sql("CREATE TABLE customers (id INT, name VARCHAR)")
            .await
            .expect("create customers")
            .collect()
            .await
            .expect("collect create customers");
        ctx.sql(sql).await.expect("sql").logical_plan().clone()
    }

    #[tokio::test]
    async fn join_filters_on_two_tables_are_not_warmable() {
        let t = template_from_plan(
            &plan_of_join(
                "SELECT orders.id FROM orders JOIN customers ON orders.customer_id = customers.id \
                 WHERE orders.id = 1 AND customers.name = 'acme'",
            )
            .await,
        )
        .expect("template");
        assert!(
            t.bindings.len() >= 2,
            "both equality filters must become bindings, got {t:?}"
        );
        let tables: std::collections::HashSet<&str> =
            t.bindings.iter().map(|b| b.table.as_str()).collect();
        assert!(
            tables.len() > 1 || t.bindings.iter().any(|b| b.table.is_empty()),
            "join bindings must span tables or leave a table name empty, got {t:?}"
        );
        assert!(
            distinct_keys_sql(&t).is_none(),
            "multi-table bindings must not produce DISTINCT SQL"
        );
        assert!(
            !template_can_warm(&t),
            "multi-table bindings cannot be warmed without a join DISTINCT"
        );
    }

    #[test]
    fn empty_table_name_on_bindings_is_not_warmable() {
        let t = WarmupTemplate {
            sql: "SELECT id FROM orders WHERE id = $1".to_string(),
            bindings: vec![WarmupBinding {
                table: String::new(),
                column: "id".to_string(),
            }],
        };
        assert!(distinct_keys_sql(&t).is_none());
        assert!(!template_can_warm(&t));
    }

    #[test]
    fn catalog_qualified_binding_quotes_each_table_part() {
        let t = WarmupTemplate {
            sql: "SELECT id FROM spice.public.orders WHERE id = $1".to_string(),
            bindings: vec![WarmupBinding {
                table: "spice.public.orders".to_string(),
                column: "id".to_string(),
            }],
        };
        assert_eq!(
            distinct_keys_sql(&t).as_deref(),
            Some(r#"SELECT DISTINCT "id" FROM "spice"."public"."orders" LIMIT 1024"#)
        );
    }

    #[tokio::test]
    async fn dotted_table_identifier_is_not_split_into_schema_and_table() {
        let ctx = SessionContext::new();
        ctx.sql(r#"CREATE TABLE "users.v1" (id INT, status VARCHAR)"#)
            .await
            .expect("create dotted table")
            .collect()
            .await
            .expect("collect create");
        let plan = ctx
            .sql(r#"SELECT id FROM "users.v1" WHERE id = 1"#)
            .await
            .expect("sql")
            .logical_plan()
            .clone();
        let t = template_from_plan(&plan).expect("template");
        assert_eq!(
            t.bindings.len(),
            1,
            "expected one binding for the dotted table, got {t:?}"
        );
        assert_eq!(
            t.bindings[0].table, r#""users.v1""#,
            "binding must keep the dotted identifier as one table, got {t:?}"
        );
        let distinct = distinct_keys_sql(&t).expect("distinct");
        assert!(
            distinct.contains(r#""users.v1""#),
            "DISTINCT must quote the dotted identifier as one table, got {distinct}"
        );
        assert!(
            !distinct.contains(r#""users"."v1""#),
            "DISTINCT must not split a dotted identifier into schema.table, got {distinct}"
        );
        ctx.sql(&distinct)
            .await
            .expect("DISTINCT must execute against the dotted table identifier")
            .collect()
            .await
            .expect("collect distinct");
    }

    #[tokio::test]
    async fn aliased_dotted_table_resolves_to_quoted_identifier() {
        let ctx = SessionContext::new();
        ctx.sql(r#"CREATE TABLE "users.v1" (id INT, status VARCHAR)"#)
            .await
            .expect("create dotted table")
            .collect()
            .await
            .expect("collect create");
        let plan = ctx
            .sql(r#"SELECT id FROM "users.v1" AS u WHERE u.id = 1"#)
            .await
            .expect("sql")
            .logical_plan()
            .clone();
        let t = template_from_plan(&plan).expect("template");
        assert_eq!(
            t.bindings[0].table, r#""users.v1""#,
            "alias must resolve to the dotted input table, got {t:?}"
        );
        let distinct = distinct_keys_sql(&t).expect("distinct");
        assert!(
            distinct.contains(r#""users.v1""#),
            "DISTINCT must read the dotted input table, got {distinct}"
        );
        assert!(
            !distinct.contains(r#""u""#),
            "DISTINCT must not use the SQL alias as a table name, got {distinct}"
        );
        ctx.sql(&distinct)
            .await
            .expect("DISTINCT must execute against the dotted table, not the alias")
            .collect()
            .await
            .expect("collect distinct");
    }

    #[tokio::test]
    async fn disjunctive_equality_on_same_column_is_not_warmable() {
        // `id = 1 OR id = 2` parameterizes to two `id` bindings. Row-wise
        // `SELECT DISTINCT id, id` only yields diagonals like `(1,1)`, never the
        // observed tuple `(1,2)`, so the template cannot warm the cache key that
        // recorded it and must not consume a catalog slot.
        let t = template_from_plan(&plan_of("SELECT id FROM orders WHERE id = 1 OR id = 2").await)
            .expect("template");
        assert!(
            t.bindings.len() >= 2,
            "both OR equalities must become bindings, got {t:?}"
        );
        assert!(
            distinct_keys_sql(&t).is_none(),
            "disjunctive/repeated-column bindings must not produce DISTINCT SQL, got {t:?}"
        );
        assert!(
            !template_can_warm(&t),
            "OR equalities must not be recorded for warmup, got {t:?}"
        );
    }

    #[tokio::test]
    async fn disjunctive_equality_on_different_columns_is_not_warmable() {
        let t = template_from_plan(
            &plan_of("SELECT id FROM orders WHERE id = 1 OR status = 'open'").await,
        )
        .expect("template");
        assert!(
            t.bindings.len() >= 2,
            "both OR equalities must become bindings, got {t:?}"
        );
        assert!(
            distinct_keys_sql(&t).is_none(),
            "disjunctive bindings must not produce DISTINCT SQL, got {t:?}"
        );
        assert!(
            !template_can_warm(&t),
            "OR equalities on different columns must not be recorded, got {t:?}"
        );
    }

    #[test]
    fn repeated_binding_columns_are_not_warmable() {
        let t = WarmupTemplate {
            sql: "SELECT id FROM orders WHERE id = $1 OR id = $2".to_string(),
            bindings: vec![
                WarmupBinding {
                    table: r#""orders""#.to_string(),
                    column: "id".to_string(),
                },
                WarmupBinding {
                    table: r#""orders""#.to_string(),
                    column: "id".to_string(),
                },
            ],
        };
        assert!(distinct_keys_sql(&t).is_none());
        assert!(!template_can_warm(&t));
    }
}
