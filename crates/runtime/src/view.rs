/*
Copyright 2024-2025 The Spice.ai OSS Authors

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
use crate::{component::view::View, search::full_text::table::add_full_text_search_to_table};
use ::datafusion::sql::{TableReference, parser, sqlparser::ast};
use datafusion::{
    catalog::TableProvider, datasource::ViewTable, error::Result, prelude::SessionContext,
};
use std::{collections::HashSet, sync::Arc};

pub(crate) fn get_dependent_table_names(statement: &parser::Statement) -> Vec<TableReference> {
    let mut table_names = Vec::new();
    let mut cte_names = HashSet::new();

    if let parser::Statement::Statement(statement) = statement.clone()
        && let ast::Statement::Query(statement) = *statement
    {
        // Collect names of CTEs
        if let Some(with) = statement.with {
            for table in with.cte_tables {
                cte_names.insert(TableReference::bare(table.alias.name.to_string()));
                let cte_table_names = get_dependent_table_names(&parser::Statement::Statement(
                    Box::new(ast::Statement::Query(table.query)),
                ));
                // Extend table_names with names found in CTEs if they reference actual tables
                table_names.extend(cte_table_names);
            }
        }
        // Extract table names from the main query
        table_names.extend(extract_tables_from_set_expr(&statement.body, &cte_names));
    }

    // Filter out CTEs and temporary views (aliases of subqueries)
    table_names
        .into_iter()
        .filter(|name| !cte_names.contains(name))
        .collect()
}

fn extract_tables_from_set_expr(
    expr: &ast::SetExpr,
    cte_names: &HashSet<TableReference>,
) -> Vec<TableReference> {
    match expr {
        ast::SetExpr::Select(select_statement) => {
            let mut table_names = vec![];
            for from in &select_statement.from {
                let mut relations = vec![from.relation.clone()];
                for join in &from.joins {
                    relations.push(join.relation.clone());
                }

                for relation in relations {
                    match relation {
                        ast::TableFactor::Table { name, .. } => {
                            let table_ref = name.to_string().into();
                            if !cte_names.contains(&table_ref) {
                                table_names.push(table_ref);
                            }
                        }
                        ast::TableFactor::Derived { subquery, .. } => {
                            table_names.extend(get_dependent_table_names(
                                &parser::Statement::Statement(Box::new(ast::Statement::Query(
                                    subquery,
                                ))),
                            ));
                        }
                        _ => {}
                    }
                }
            }
            table_names
        }
        ast::SetExpr::SetOperation { left, right, .. } => {
            let mut table_names = extract_tables_from_set_expr(left, cte_names);
            table_names.extend(extract_tables_from_set_expr(right, cte_names));
            table_names
        }
        _ => vec![],
    }
}

pub(crate) async fn prepare_view(
    ctx: &SessionContext,
    statement: &parser::Statement,
    view: &Arc<View>,
) -> Result<Arc<dyn TableProvider>> {
    let plan = ctx.state().statement_to_plan(statement.clone()).await?;
    let view_table = ViewTable::new(plan, Some(view.sql.to_string()));

    if view.has_full_text_column() {
        let idx = add_full_text_search_to_table(Arc::new(view_table), &view.columns, &view.name)?;
        Ok(Arc::new(idx) as Arc<dyn TableProvider>)
    } else {
        Ok(Arc::new(view_table) as Arc<dyn TableProvider>)
    }
}

#[cfg(test)]
mod tests {
    use datafusion::sql::{parser::DFParser, sqlparser::dialect::PostgreSqlDialect};

    use super::*;

    #[tokio::test]
    async fn test_get_dependent_table_names_with_simple_query() {
        let sql = r"
            SELECT a, b FROM employees limit 10;
        ";

        let actual_table_names = extract_table_names_from_sql(sql);

        let expected_table_names: HashSet<_> = vec![TableReference::bare("employees".to_string())]
            .into_iter()
            .collect();

        assert_eq!(expected_table_names, actual_table_names);
    }

    #[tokio::test]
    async fn test_get_dependent_table_names_with_schema() {
        let sql = r"
            SELECT a, b FROM dbo.employees limit 10;
        ";

        let actual_table_names = extract_table_names_from_sql(sql);

        let expected_table_names: HashSet<TableReference> =
            vec!["dbo.employees".into()].into_iter().collect();

        assert_eq!(expected_table_names, actual_table_names);
    }

    #[tokio::test]
    async fn test_get_dependent_table_names_with_joins() {
        let sql = r"
            SELECT e.name, d.department_name
            FROM employees e
            JOIN departments d ON e.department_id = d.id
        ";

        let actual_table_names = extract_table_names_from_sql(sql);

        let expected_table_names: HashSet<TableReference> =
            vec!["employees".into(), "departments".into()]
                .into_iter()
                .collect();

        assert_eq!(expected_table_names, actual_table_names);
    }

    #[tokio::test]
    async fn test_get_dependent_table_names_with_cte_and_join() {
        let sql = r"
            WITH tmp AS (
                SELECT * FROM t1
            )
            SELECT tmp.id, t2.name
            FROM tmp
            JOIN t2 ON tmp.id = t2.id;
        ";

        let actual_table_names = extract_table_names_from_sql(sql);

        let expected_table_names: HashSet<TableReference> =
            vec!["t1".into(), "t2".into()].into_iter().collect();

        assert_eq!(expected_table_names, actual_table_names);
    }

    #[tokio::test]
    async fn test_get_dependent_table_names_with_cte_and_union() {
        let sql = r"
            WITH all_sales AS (
                SELECT sales FROM s3_source
                UNION ALL
                SELECT fare_amount + tip_amount AS sales FROM dremio_source
            )
            SELECT SUM(sales) AS total_sales,
                   COUNT(*) AS total_transactions,
                   MAX(sales) AS max_sale,
                   AVG(sales) AS avg_sale
            FROM all_sales;
        ";

        let actual_table_names = extract_table_names_from_sql(sql);

        let expected_table_names: HashSet<TableReference> =
            vec!["s3_source".into(), "dremio_source".into()]
                .into_iter()
                .collect();

        assert_eq!(expected_table_names, actual_table_names);
    }

    #[tokio::test]
    async fn test_get_dependent_table_names_with_nested_subqueries() {
        let sql = r"
            SELECT * FROM (
                SELECT * FROM (
                    SELECT * FROM orders
                ) AS subquery1
            ) AS subquery2
        ";

        let actual_table_names = extract_table_names_from_sql(sql);

        let expected_table_names: HashSet<TableReference> =
            vec!["orders".into()].into_iter().collect();

        assert_eq!(expected_table_names, actual_table_names);
    }

    fn extract_table_names_from_sql(sql: &str) -> HashSet<TableReference> {
        let statements =
            DFParser::parse_sql_with_dialect(sql, &PostgreSqlDialect {}).expect("to parse sql");
        assert_eq!(statements.len(), 1);

        let table_names = get_dependent_table_names(&statements[0]);
        table_names.into_iter().collect()
    }

    #[tokio::test]
    async fn test_get_dependent_table_names_with_cte_and_multiple_queries() {
        let sql = r"
            WITH cte1 AS (
                SELECT * FROM table1
            ), cte2 AS (
                SELECT * FROM table2
            )
            SELECT * FROM cte1
            UNION ALL
            SELECT * FROM cte2
            UNION
            SELECT * FROM table3
        ";

        let actual_table_names = extract_table_names_from_sql(sql);

        let expected_table_names: HashSet<TableReference> =
            vec!["table1".into(), "table2".into(), "table3".into()]
                .into_iter()
                .collect();

        assert_eq!(expected_table_names, actual_table_names);
    }

    #[tokio::test]
    async fn test_get_dependent_table_names_with_set_operations() {
        let sql = r"
            SELECT * FROM table1
            UNION
            SELECT * FROM table2
            INTERSECT
            SELECT * FROM table3
        ";

        let actual_table_names = extract_table_names_from_sql(sql);

        let expected_table_names: HashSet<TableReference> =
            vec!["table1".into(), "table2".into(), "table3".into()]
                .into_iter()
                .collect();

        assert_eq!(expected_table_names, actual_table_names);
    }
}
