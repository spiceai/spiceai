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

use arrow::datatypes::{Field, Schema};
use datafusion::{
    common::Column,
    sql::{
        ResolvedTableReference, TableReference,
        sqlparser::{ast::Statement, dialect::PostgreSqlDialect, parser::Parser},
    },
};
use reqwest::Client;
use serde_json::Value;
use std::{collections::HashSet, sync::Arc};

// Fetches the logical plan by running `EXPLAIN FORMAT PGJSON <sql>` against `v1/sql`.
//
// Returns the logical plan in PGJSON format.
pub async fn logical_plan(
    http_client: Client,
    http_base_url: &str,
    sql: &str,
) -> Result<Value, anyhow::Error> {
    let url = format!("{http_base_url}/v1/sql");

    let response = http_client
        .post(&url)
        .body(format!("EXPLAIN FORMAT PGJSON {sql}"))
        .header("Content-Type", "text/plain")
        .send()
        .await?;

    let json: Vec<Value> = response.json().await?;
    let plan_str = json
        .first()
        .and_then(|v| v.get("plan"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("Failed to extract plan from response"))?;

    let plan: Vec<Value> = serde_json::from_str(plan_str)?;
    let root_plan = plan
        .first()
        .and_then(|v| v.get("Plan"))
        .ok_or_else(|| anyhow::anyhow!("Failed to extract Plan from response"))?;

    Ok(root_plan.clone())
}

/// Attempts to parse the SQL and extract table references and projected columns.
///
/// Will parse multiple statements, but only parse SELECT query statements.
pub fn attempt_parse_table_and_projection(
    sql: &str,
) -> Result<(HashSet<ResolvedTableReference>, HashSet<Column>), anyhow::Error> {
    use datafusion::sql::sqlparser::ast::With;

    let mut tables = HashSet::new();
    let mut projections = HashSet::new();
    let mut cte_names = HashSet::new();

    let statements = Parser::parse_sql(&PostgreSqlDialect {}, sql)?;
    let queries: Vec<_> = statements
        .iter()
        .filter_map(|s| match s {
            Statement::Query(q) => Some(q.as_ref()),
            _ => None,
        })
        .collect();

    for q in queries {
        // Extract CTE names so we don't count them as external tables
        if let Some(With { cte_tables, .. }) = &q.with {
            for cte in cte_tables {
                cte_names.insert(cte.alias.name.value.clone());
                // Always extract tables from CTE subqueries
                extract_tables_from_set_expr(&cte.query.body, &cte_names, &mut tables);
            }
        }

        extract_tables_from_set_expr(&q.body, &cte_names, &mut tables);
        extract_projections_from_set_expr(&q.body, &mut projections);
    }

    Ok((tables, projections))
}

fn extract_tables_from_set_expr(
    expr: &datafusion::sql::sqlparser::ast::SetExpr,
    cte_names: &HashSet<String>,
    tables: &mut HashSet<ResolvedTableReference>,
) {
    use datafusion::sql::sqlparser::ast::SetExpr;

    match expr {
        SetExpr::Select(select) => {
            for from in &select.from {
                extract_table_from_factor(&from.relation, cte_names, tables);
                for join in &from.joins {
                    extract_table_from_factor(&join.relation, cte_names, tables);
                }
            }
        }
        SetExpr::SetOperation { left, right, .. } => {
            extract_tables_from_set_expr(left, cte_names, tables);
            extract_tables_from_set_expr(right, cte_names, tables);
        }
        SetExpr::Query(q) => {
            extract_tables_from_set_expr(&q.body, cte_names, tables);
        }
        SetExpr::Values(_)
        | SetExpr::Insert(_)
        | SetExpr::Update(_)
        | SetExpr::Table(_)
        | SetExpr::Delete(_) => {}
    }
}

fn extract_table_from_factor(
    factor: &datafusion::sql::sqlparser::ast::TableFactor,
    cte_names: &HashSet<String>,
    tables: &mut HashSet<ResolvedTableReference>,
) {
    use datafusion::sql::sqlparser::ast::{ObjectNamePart, TableFactor};

    match factor {
        TableFactor::Table { name, .. } => {
            // ObjectName is a Vec<ObjectNamePart>, extract identifier values
            let parts: Vec<&str> = name
                .0
                .iter()
                .filter_map(|part| match part {
                    ObjectNamePart::Identifier(ident) => Some(ident.value.as_str()),
                    ObjectNamePart::Function(_) => None,
                })
                .collect();

            let table_ref = match parts.as_slice() {
                [table] => TableReference::bare(*table),
                [schema, table] => TableReference::partial(*schema, *table),
                [catalog, schema, table] => TableReference::full(*catalog, *schema, *table),
                _ => {
                    // For names with more parts, join and use parse_str as fallback
                    let full_name = parts.join(".");
                    TableReference::parse_str(&full_name)
                }
            };

            let resolved = table_ref.resolve("spice", "public");
            // Skip if it's a CTE (check against the table part only)
            let table_name_for_cte = parts.last().copied().unwrap_or_default();
            if !cte_names.contains(table_name_for_cte) && !cte_names.contains(&name.to_string()) {
                tables.insert(resolved);
            }
        }
        TableFactor::Derived { subquery, .. } => {
            extract_tables_from_set_expr(&subquery.body, cte_names, tables);
        }
        TableFactor::TableFunction { .. }
        | TableFactor::Function { .. }
        | TableFactor::UNNEST { .. }
        | TableFactor::JsonTable { .. }
        | TableFactor::OpenJsonTable { .. }
        | TableFactor::NestedJoin { .. }
        | TableFactor::Pivot { .. }
        | TableFactor::Unpivot { .. }
        | TableFactor::MatchRecognize { .. }
        | TableFactor::XmlTable { .. } => {}
    }
}

fn extract_projections_from_set_expr(
    expr: &datafusion::sql::sqlparser::ast::SetExpr,
    projections: &mut HashSet<Column>,
) {
    use datafusion::sql::sqlparser::ast::{ObjectNamePart, SelectItem, SetExpr, TableFactor};
    use std::collections::HashMap;

    match expr {
        SetExpr::Select(select) => {
            // Build alias-to-table mapping from FROM clause
            let mut alias_map: HashMap<String, ResolvedTableReference> = HashMap::new();

            for from in &select.from {
                extract_alias_from_table_factor(&from.relation, &mut alias_map);
                for join in &from.joins {
                    extract_alias_from_table_factor(&join.relation, &mut alias_map);
                }
            }

            // Extract the default table reference from FROM clause for unqualified columns
            let default_table: Option<ResolvedTableReference> = if select.from.len() == 1 {
                let from = &select.from[0];
                // Only use default if there are no joins (single table)
                if from.joins.is_empty() {
                    if let TableFactor::Table { name, .. } = &from.relation {
                        let parts: Vec<&str> = name
                            .0
                            .iter()
                            .filter_map(|part| match part {
                                ObjectNamePart::Identifier(ident) => Some(ident.value.as_str()),
                                ObjectNamePart::Function(_) => None,
                            })
                            .collect();
                        let table_ref = match parts.as_slice() {
                            [table] => TableReference::bare(*table),
                            [schema, table] => TableReference::partial(*schema, *table),
                            [catalog, schema, table] => {
                                TableReference::full(*catalog, *schema, *table)
                            }
                            _ => {
                                let full_name = parts.join(".");
                                TableReference::parse_str(&full_name)
                            }
                        };
                        Some(table_ref.resolve("spice", "public"))
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            };

            for item in &select.projection {
                match item {
                    SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
                        extract_columns_from_expr(
                            e,
                            projections,
                            default_table.as_ref(),
                            &alias_map,
                        );
                    }
                    SelectItem::QualifiedWildcard(name, _) => {
                        // e.g., table.* - we record this as a partial match indicator
                        let name_str = name.to_string();
                        let tbl = alias_map.get(&name_str).cloned().unwrap_or_else(|| {
                            TableReference::parse_str(&name_str).resolve("spice", "public")
                        });
                        projections.insert(Column::new(
                            Some(TableReference::Full {
                                catalog: Arc::clone(&tbl.catalog),
                                schema: Arc::clone(&tbl.schema),
                                table: Arc::clone(&tbl.table),
                            }),
                            "*",
                        ));
                    }
                    SelectItem::Wildcard(_) => {
                        // SELECT * - we can't determine specific columns without schema
                        projections.insert(Column::new(None::<TableReference>, "*"));
                    }
                }
            }
        }
        SetExpr::SetOperation { left, right, .. } => {
            extract_projections_from_set_expr(left, projections);
            extract_projections_from_set_expr(right, projections);
        }
        SetExpr::Query(q) => {
            extract_projections_from_set_expr(&q.body, projections);
        }
        SetExpr::Values(_)
        | SetExpr::Insert(_)
        | SetExpr::Update(_)
        | SetExpr::Table(_)
        | SetExpr::Delete(_) => {}
    }
}

/// Extracts alias-to-table mappings from a table factor.
fn extract_alias_from_table_factor(
    factor: &datafusion::sql::sqlparser::ast::TableFactor,
    alias_map: &mut std::collections::HashMap<String, ResolvedTableReference>,
) {
    use datafusion::sql::sqlparser::ast::{ObjectNamePart, TableFactor};

    if let TableFactor::Table { name, alias, .. } = factor {
        let parts: Vec<&str> = name
            .0
            .iter()
            .filter_map(|part| match part {
                ObjectNamePart::Identifier(ident) => Some(ident.value.as_str()),
                ObjectNamePart::Function(_) => None,
            })
            .collect();

        let table_ref = match parts.as_slice() {
            [table] => TableReference::bare(*table),
            [schema, table] => TableReference::partial(*schema, *table),
            [catalog, schema, table] => TableReference::full(*catalog, *schema, *table),
            _ => {
                let full_name = parts.join(".");
                TableReference::parse_str(&full_name)
            }
        };

        let resolved = table_ref.resolve("spice", "public");

        if let Some(alias) = alias {
            alias_map.insert(alias.name.value.clone(), resolved);
        }
    }
}

fn extract_columns_from_expr(
    expr: &datafusion::sql::sqlparser::ast::Expr,
    projections: &mut HashSet<Column>,
    default_table: Option<&ResolvedTableReference>,
    alias_map: &std::collections::HashMap<String, ResolvedTableReference>,
) {
    use datafusion::sql::sqlparser::ast::{Expr, FunctionArg, FunctionArgExpr, FunctionArguments};

    match expr {
        Expr::Identifier(ident) => {
            // Use the default table if available for unqualified columns
            let relation = default_table.map(|t| TableReference::Full {
                catalog: Arc::clone(&t.catalog),
                schema: Arc::clone(&t.schema),
                table: Arc::clone(&t.table),
            });
            projections.insert(Column::new(relation, &ident.value));
        }
        Expr::CompoundIdentifier(idents) => {
            if idents.len() >= 2 {
                // Last part is the column name, rest is the table reference
                let col_name = &idents[idents.len() - 1].value;
                let table_parts: Vec<_> = idents[..idents.len() - 1]
                    .iter()
                    .map(|i| &i.value)
                    .collect();
                let table_ref_str = table_parts
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>()
                    .join(".");

                // Check if the first part is an alias
                let tbl = if table_parts.len() == 1 {
                    if let Some(resolved) = alias_map.get(table_parts[0]) {
                        resolved.clone()
                    } else {
                        TableReference::parse_str(&table_ref_str).resolve("spice", "public")
                    }
                } else {
                    TableReference::parse_str(&table_ref_str).resolve("spice", "public")
                };

                projections.insert(Column::new(
                    Some(TableReference::Full {
                        catalog: Arc::clone(&tbl.catalog),
                        schema: Arc::clone(&tbl.schema),
                        table: Arc::clone(&tbl.table),
                    }),
                    col_name,
                ));
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            extract_columns_from_expr(left, projections, default_table, alias_map);
            extract_columns_from_expr(right, projections, default_table, alias_map);
        }
        Expr::UnaryOp { expr: e, .. } | Expr::Nested(e) | Expr::Cast { expr: e, .. } => {
            extract_columns_from_expr(e, projections, default_table, alias_map);
        }
        Expr::Function(f) => {
            if let FunctionArguments::List(arg_list) = &f.args {
                for arg in &arg_list.args {
                    if let FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) = arg {
                        extract_columns_from_expr(e, projections, default_table, alias_map);
                    }
                }
            }
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(op) = operand {
                extract_columns_from_expr(op, projections, default_table, alias_map);
            }
            for case_when in conditions {
                extract_columns_from_expr(
                    &case_when.condition,
                    projections,
                    default_table,
                    alias_map,
                );
                extract_columns_from_expr(&case_when.result, projections, default_table, alias_map);
            }
            if let Some(e) = else_result {
                extract_columns_from_expr(e, projections, default_table, alias_map);
            }
        }
        Expr::Subquery(q) => {
            // Subqueries have their own scope, don't pass default_table or alias_map
            extract_projections_from_set_expr(&q.body, projections);
        }
        Expr::InSubquery {
            expr: e, subquery, ..
        } => {
            extract_columns_from_expr(e, projections, default_table, alias_map);
            // Subqueries have their own scope
            extract_projections_from_set_expr(&subquery.body, projections);
        }
        // Many other expr variants exist; we handle the common ones
        _ => {}
    }
}

pub fn extract_tables_and_projection(
    logical_plan: &Value,
) -> (HashSet<ResolvedTableReference>, HashSet<Column>) {
    let mut tables = HashSet::new();
    let mut projections = HashSet::new();

    extract_table_scans(logical_plan, &mut tables, &mut projections);

    (tables, projections)
}

fn extract_table_scans(
    plan: &Value,
    tables: &mut HashSet<ResolvedTableReference>,
    projections: &mut HashSet<Column>,
) {
    if let Some(node_type) = plan.get("Node Type").and_then(Value::as_str)
        && node_type == "TableScan"
    {
        let mut relation_opt: Option<ResolvedTableReference> = None;
        if let Some(relation_name) = plan.get("Relation Name").and_then(Value::as_str) {
            let tbl = TableReference::parse_str(relation_name).resolve("spice", "public");
            tables.insert(tbl.clone());
            relation_opt = Some(tbl);
        }
        if let Some(output) = plan.get("Output").and_then(Value::as_array) {
            for col in output {
                if let Some(col_str) = col.as_str() {
                    projections.insert(Column::new(
                        relation_opt.as_ref().map(|t| TableReference::Full {
                            catalog: Arc::clone(&t.catalog),
                            schema: Arc::clone(&t.schema),
                            table: Arc::clone(&t.table),
                        }),
                        col_str,
                    ));
                }
            }
        }
    }

    if let Some(plans) = plan.get("Plans").and_then(Value::as_array) {
        for sub_plan in plans {
            extract_table_scans(sub_plan, tables, projections);
        }
    }
}

/// Finds the SQL query's schema by running the SQL against `v1/sql` endpoint.
///
/// Attempts to use more efficient SQL with equivalent outputs.
pub async fn sql_schema(
    http_client: Client,
    http_base_url: &str,
    sql: &str,
) -> Result<Schema, anyhow::Error> {
    let url = format!("{http_base_url}/v1/sql");

    let response = http_client
        .post(&url)
        .body(format!(
            "SELECT * FROM ({}) LIMIT 1",
            sql.strip_suffix(";").unwrap_or(sql)
        ))
        .header("Content-Type", "text/plain")
        .header("Accept", "application/vnd.spiceai.nsql.v1+json")
        .send()
        .await?;

    let mut json: Value = response.json().await?;
    let Some(schema) = json.get_mut("schema") else {
        return Err(anyhow::anyhow!("Failed to extract schema from response"));
    };
    let Some(f) = schema.get_mut("fields") else {
        return Err(anyhow::anyhow!("Failed to extract fields from schema"));
    };
    let fields: Vec<Field> = serde_json::from_value(f.take())
        .map_err(|e| anyhow::anyhow!("Failed to deserialize fields from schema: {e}"))?;
    Ok(Schema::new(fields))
}

#[cfg(test)]
mod tests {
    use super::*;
    use insta::assert_snapshot;

    /// Formats the parse result for snapshot testing.
    fn format_result(
        tables: &HashSet<ResolvedTableReference>,
        projections: &HashSet<Column>,
    ) -> String {
        let mut tables_vec: Vec<_> = tables.iter().map(ToString::to_string).collect();
        tables_vec.sort();

        let mut proj_vec: Vec<_> = projections.iter().map(ToString::to_string).collect();
        proj_vec.sort();

        format!(
            "Tables:\n{}\n\nProjections:\n{}",
            if tables_vec.is_empty() {
                "  (none)".to_string()
            } else {
                tables_vec
                    .iter()
                    .map(|t| format!("  {t}"))
                    .collect::<Vec<_>>()
                    .join("\n")
            },
            if proj_vec.is_empty() {
                "  (none)".to_string()
            } else {
                proj_vec
                    .iter()
                    .map(|p| format!("  {p}"))
                    .collect::<Vec<_>>()
                    .join("\n")
            }
        )
    }

    #[test]
    fn simple_select() {
        let (tables, projections) =
            attempt_parse_table_and_projection("SELECT id, name FROM users")
                .expect("Failed to parse SQL");
        assert_snapshot!(format_result(&tables, &projections), @r"
        Tables:
          spice.public.users

        Projections:
          spice.public.users.id
          spice.public.users.name
        ");
    }

    #[test]
    fn select_star() {
        let (tables, projections) = attempt_parse_table_and_projection("SELECT * FROM orders")
            .expect("Failed to parse SQL");
        assert_snapshot!(format_result(&tables, &projections), @r#"
        Tables:
          spice.public.orders

        Projections:
          *
        "#);
    }

    #[test]
    fn join_query() {
        let (tables, projections) = attempt_parse_table_and_projection(
            "SELECT u.id, u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id",
        )
        .expect("Failed to parse SQL");
        assert_snapshot!(format_result(&tables, &projections), @r#"
        Tables:
          spice.public.orders
          spice.public.users

        Projections:
          spice.public.orders.total
          spice.public.users.id
          spice.public.users.name
        "#);
    }

    #[test]
    fn subquery() {
        let (tables, projections) = attempt_parse_table_and_projection(
            "SELECT * FROM (SELECT id, amount FROM transactions WHERE amount > 100) AS t",
        )
        .expect("Failed to parse SQL");
        assert_snapshot!(format_result(&tables, &projections), @r"
        Tables:
          spice.public.transactions

        Projections:
          *
        ");
    }

    #[test]
    fn cte_query() {
        let (tables, projections) = attempt_parse_table_and_projection(
            "WITH active_users AS (SELECT id, name FROM users WHERE active = true) SELECT * FROM active_users",
        )
        .expect("Failed to parse SQL");
        assert_snapshot!(format_result(&tables, &projections), @r#"
        Tables:
          spice.public.users

        Projections:
          *
        "#);
    }

    #[test]
    fn union_query() {
        let (tables, projections) = attempt_parse_table_and_projection(
            "SELECT id, name FROM customers UNION SELECT id, name FROM vendors",
        )
        .expect("Failed to parse SQL");
        assert_snapshot!(format_result(&tables, &projections), @r"
        Tables:
          spice.public.customers
          spice.public.vendors

        Projections:
          spice.public.customers.id
          spice.public.customers.name
          spice.public.vendors.id
          spice.public.vendors.name
        ");
    }

    #[test]
    fn qualified_columns() {
        let (tables, projections) = attempt_parse_table_and_projection(
            "SELECT customers.id, customers.name, orders.total FROM customers, orders",
        )
        .expect("Failed to parse SQL");
        assert_snapshot!(format_result(&tables, &projections), @r#"
        Tables:
          spice.public.customers
          spice.public.orders

        Projections:
          spice.public.customers.id
          spice.public.customers.name
          spice.public.orders.total
        "#);
    }

    #[test]
    fn function_in_select() {
        let (tables, projections) =
            attempt_parse_table_and_projection("SELECT COUNT(id), SUM(amount) FROM transactions")
                .expect("Failed to parse SQL");
        assert_snapshot!(format_result(&tables, &projections), @r"
        Tables:
          spice.public.transactions

        Projections:
          spice.public.transactions.amount
          spice.public.transactions.id
        ");
    }

    #[test]
    fn case_expression() {
        let (tables, projections) = attempt_parse_table_and_projection(
            "SELECT id, CASE WHEN status = 1 THEN 'active' ELSE 'inactive' END FROM users",
        )
        .expect("Failed to parse SQL");
        assert_snapshot!(format_result(&tables, &projections), @r"
        Tables:
          spice.public.users

        Projections:
          spice.public.users.id
          spice.public.users.status
        ");
    }

    #[test]
    fn qualified_wildcard() {
        let (tables, projections) = attempt_parse_table_and_projection(
            "SELECT users.*, orders.id FROM users JOIN orders ON users.id = orders.user_id",
        )
        .expect("Failed to parse SQL");
        assert_snapshot!(format_result(&tables, &projections), @r"
        Tables:
          spice.public.orders
          spice.public.users

        Projections:
          spice.public.orders.id
          spice.public.users.*.*
        ");
    }

    #[test]
    fn multiple_joins() {
        let (tables, projections) = attempt_parse_table_and_projection(
            "SELECT a.id, b.name, c.value FROM table_a a LEFT JOIN table_b b ON a.id = b.a_id INNER JOIN table_c c ON b.id = c.b_id",
        )
        .expect("Failed to parse SQL");
        assert_snapshot!(format_result(&tables, &projections), @r#"
        Tables:
          spice.public.table_a
          spice.public.table_b
          spice.public.table_c

        Projections:
          spice.public.table_a.id
          spice.public.table_b.name
          spice.public.table_c.value
        "#);
    }

    #[test]
    fn join_with_alias_resolution() {
        // Test that table aliases (T1, T2) are resolved to actual table names in projections
        let (tables, projections) = attempt_parse_table_and_projection(
            "SELECT T2.School FROM satscores AS T1 INNER JOIN schools AS T2 ON T1.cds = T2.CDSCode WHERE T2.Magnet = 1 AND T1.NumTstTakr > 500",
        )
        .expect("Failed to parse SQL");
        assert_snapshot!(format_result(&tables, &projections), @r"
        Tables:
          spice.public.satscores
          spice.public.schools

        Projections:
          spice.public.schools.School
        ");
    }

    #[test]
    fn nested_subquery() {
        let (tables, projections) = attempt_parse_table_and_projection(
            "SELECT id FROM users WHERE id IN (SELECT user_id FROM orders WHERE total > (SELECT AVG(total) FROM orders))",
        )
        .expect("Failed to parse SQL");
        assert_snapshot!(format_result(&tables, &projections), @r"
        Tables:
          spice.public.users

        Projections:
          spice.public.users.id
        ");
    }

    #[test]
    fn schema_qualified_table() {
        let (tables, projections) =
            attempt_parse_table_and_projection("SELECT id, name FROM myschema.users")
                .expect("Failed to parse SQL");
        assert_snapshot!(format_result(&tables, &projections), @r"
        Tables:
          spice.myschema.users

        Projections:
          spice.myschema.users.id
          spice.myschema.users.name
        ");
    }

    #[test]
    fn cast_expression() {
        let (tables, projections) = attempt_parse_table_and_projection(
            "SELECT CAST(id AS VARCHAR), amount::numeric FROM transactions",
        )
        .expect("Failed to parse SQL");
        assert_snapshot!(format_result(&tables, &projections), @r"
        Tables:
          spice.public.transactions

        Projections:
          spice.public.transactions.amount
          spice.public.transactions.id
        ");
    }

    #[test]
    fn alias_expression() {
        let (tables, projections) = attempt_parse_table_and_projection(
            "SELECT id AS user_id, name AS user_name FROM users",
        )
        .expect("Failed to parse SQL");
        assert_snapshot!(format_result(&tables, &projections), @r"
        Tables:
          spice.public.users

        Projections:
          spice.public.users.id
          spice.public.users.name
        ");
    }

    #[test]
    fn quoted_identifiers() {
        // Note: "spice.public.frpm" is a single quoted identifier, so the dots are part of the
        // table name itself, not schema separators. When resolved, it becomes spice.public."spice.public.frpm"
        let (tables, projections) = attempt_parse_table_and_projection(
            r#"SELECT MAX("Free Meal Count (K-12)" / "Enrollment (K-12)") AS highest_eligible_free_rate FROM "spice.public.frpm" WHERE "County Name" = 'Alameda'"#,
        )
        .expect("Failed to parse SQL");
        assert_snapshot!(format_result(&tables, &projections), @r"
        Tables:
          spice.public.spice.public.frpm

        Projections:
          spice.public.spice.public.frpm.Enrollment (K-12)
          spice.public.spice.public.frpm.Free Meal Count (K-12)
        ");
    }

    #[test]
    fn properly_qualified_quoted_table() {
        // With proper schema qualification: "spice"."public"."frpm" - each part is separate
        let (tables, projections) =
            attempt_parse_table_and_projection(r#"SELECT "col1" FROM "spice"."public"."frpm""#)
                .expect("Failed to parse SQL");
        assert_snapshot!(format_result(&tables, &projections), @r"
        Tables:
          spice.public.frpm

        Projections:
          spice.public.frpm.col1
        ");
    }

    #[test]
    fn properly_extra_text() {
        let _ = attempt_parse_table_and_projection(r#"Looking at the schema and previous errors, I need to provide only the raw SQL without any explanations, markdown, or extra text. The errors indicate the SQL parser is failing because of the additional content.

            Based on the tables:
            - `frpm` has `"FRPM Count (K-12)"` column
            - `schools` has `"MailStreet"` column (unabbreviated mailing street address)
            - Join on `"CDSCode"`

            SELECT "MailStreet" FROM schools JOIN frpm ON schools."CDSCode" = frpm."CDSCode" WHERE frpm."FRPM Count (K-12)" IS NOT NULL ORDER BY frpm."FRPM Count (K-12)" DESC LIMIT 1"#).expect_err(
                "Invalid SQL should return an error"
            );
    }
}
