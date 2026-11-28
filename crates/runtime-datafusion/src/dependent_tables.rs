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
//! Discovery of the tables that a SQL statement depends on.

use datafusion::common::TableReference;
use datafusion::sql::{
    parser,
    sqlparser::ast::{self, Visit, Visitor},
};
use std::collections::HashSet;
use std::ops::ControlFlow;

/// Returns the tables a SQL statement reads from. CTE names are excluded, and the
/// tables inside CTEs, derived tables, joins, and set operations are included.
#[must_use]
pub fn get_dependent_table_names(statement: &parser::Statement) -> Vec<TableReference> {
    match statement {
        parser::Statement::Statement(statement) => match statement.as_ref() {
            ast::Statement::Query(query) => get_dependent_table_names_of_query(query),
            _ => Vec::new(),
        },
        _ => Vec::new(),
    }
}

/// Walks the query by reference: cloning it would recurse once per level of
/// every expression tree inside it, as deep as the parser allows.
fn get_dependent_table_names_of_query(query: &ast::Query) -> Vec<TableReference> {
    let mut table_names = Vec::new();
    let mut cte_names = HashSet::new();

    // Collect names of CTEs
    if let Some(with) = &query.with {
        for table in &with.cte_tables {
            cte_names.insert(TableReference::bare(table.alias.name.to_string()));
            // Extend table_names with names found in CTEs if they reference actual tables
            table_names.extend(get_dependent_table_names_of_query(&table.query));
        }
    }
    // Extract table names from the main query
    table_names.extend(extract_tables_from_set_expr(&query.body, &cte_names));

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
                let relations = std::iter::once(&from.relation)
                    .chain(from.joins.iter().map(|join| &join.relation));

                for relation in relations {
                    match relation {
                        // A table function, e.g. `text_search(docs, 'fox')`, is not a
                        // table. See `table_function_arguments` for the tables it reads.
                        ast::TableFactor::Table { args: Some(_), .. } => {}
                        ast::TableFactor::Table { name, .. } => {
                            let table_ref = name.to_string().into();
                            if !cte_names.contains(&table_ref) {
                                table_names.push(table_ref);
                            }
                        }
                        ast::TableFactor::Derived { subquery, .. } => {
                            table_names.extend(get_dependent_table_names_of_query(subquery));
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

/// Returns the names passed as arguments to the statement's table-function calls,
/// including calls in subqueries, `LATERAL` calls, and calls nested in another
/// call's arguments, such as the searches `rrf` fuses. A name may be a table, like
/// the `docs` of `text_search(docs, 'fox')`, or a column, like the `q.term` of
/// `LATERAL text_search(docs, q.term)`: the caller tells them apart by the tables
/// it knows.
#[must_use]
pub fn table_function_arguments(statement: &parser::Statement) -> Vec<TableReference> {
    let parser::Statement::Statement(statement) = statement else {
        return vec![];
    };
    let mut arguments = TableFunctionArguments::default();
    let _ = statement.visit(&mut arguments);
    arguments.0
}

#[derive(Default)]
struct TableFunctionArguments(Vec<TableReference>);

impl Visitor for TableFunctionArguments {
    type Break = ();

    fn pre_visit_table_factor(&mut self, table_factor: &ast::TableFactor) -> ControlFlow<()> {
        match table_factor {
            ast::TableFactor::Table {
                args: Some(args), ..
            } => self.extend(&args.args),
            ast::TableFactor::Function { args, .. } => self.extend(args),
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

impl TableFunctionArguments {
    fn extend(&mut self, args: &[ast::FunctionArg]) {
        for arg in args {
            let (ast::FunctionArg::Unnamed(arg)
            | ast::FunctionArg::Named { arg, .. }
            | ast::FunctionArg::ExprNamed { arg, .. }) = arg;
            let ast::FunctionArgExpr::Expr(expr) = arg else {
                continue;
            };
            match expr {
                ast::Expr::Identifier(ident) => self.0.push(ident.to_string().into()),
                ast::Expr::CompoundIdentifier(parts) => self.0.push(
                    parts
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join(".")
                        .into(),
                ),
                ast::Expr::Function(call) => {
                    if let ast::FunctionArguments::List(list) = &call.args {
                        self.extend(&list.args);
                    }
                }
                _ => {}
            }
        }
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

    #[tokio::test]
    async fn test_get_dependent_table_names_with_table_functions() {
        let cases: [(&str, &[&str]); 3] = [
            ("SELECT id FROM text_search(docs, 'fox')", &[]),
            (
                "SELECT q.term, s.id FROM (VALUES ('fox'), ('dog')) AS q(term)
                 CROSS JOIN LATERAL text_search(docs, q.term) s",
                &[],
            ),
            (
                "SELECT t.n, s.value FROM t
                 LEFT JOIN LATERAL generate_series(1, t.n) s ON true",
                &["t"],
            ),
        ];

        for (sql, expected) in cases {
            let expected: HashSet<TableReference> = expected.iter().map(|&t| t.into()).collect();
            assert_eq!(expected, extract_table_names_from_sql(sql), "{sql}");
        }
    }

    #[tokio::test]
    async fn test_table_function_arguments() {
        let cases: [(&str, &[&str]); 6] = [
            ("SELECT id FROM text_search(docs, 'fox')", &["docs"]),
            (
                "SELECT * FROM vector_search(public.docs, 'fox', limit => 5)",
                &["public.docs"],
            ),
            (
                "SELECT * FROM rrf(text_search(docs, 'fox'), vector_search(qs, 'fox'))",
                &["docs", "qs"],
            ),
            (
                "SELECT q.term, s.id FROM (VALUES ('fox'), ('dog')) AS q(term)
                 CROSS JOIN LATERAL text_search(docs, q.term) s",
                &["docs", "q.term"],
            ),
            (
                "SELECT * FROM t WHERE id IN (SELECT id FROM text_search(docs, 'fox'))",
                &["docs"],
            ),
            ("SELECT a FROM t JOIN u ON t.id = u.id", &[]),
        ];

        for (sql, expected) in cases {
            let statements =
                DFParser::parse_sql_with_dialect(sql, &PostgreSqlDialect {}).expect("to parse sql");
            let expected: Vec<TableReference> = expected.iter().map(|&t| t.into()).collect();
            assert_eq!(expected, table_function_arguments(&statements[0]), "{sql}");
        }
    }
}
