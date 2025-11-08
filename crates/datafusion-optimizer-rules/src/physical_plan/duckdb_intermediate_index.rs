use crate::common::plan_node_key::PlanNodeKey;
use crate::common::search_visitor::SearchVisitor;
use crate::concrete;
use datafusion::common::Result;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::sqlparser::ast::{CteAsMaterialized, ObjectName, Query};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::sql::sqlparser::ast::helpers::attached_token::AttachedToken;
use datafusion::sql::sqlparser::ast::{
    BinaryOperator, Cte, Expr, Ident, ObjectNamePart, Select, SelectItem, SetExpr, Statement,
    TableAlias, TableFactor, TableWithJoins, Value, ValueWithSpan, With, visit_expressions,
    visit_expressions_mut, visit_relations,
};
use datafusion::sql::sqlparser::dialect::DuckDbDialect;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::sqlparser::tokenizer::Span;
use datafusion_table_providers::duckdb::sql_table::DuckSqlExec;
use datafusion_table_providers::sql::db_connection_pool::dbconnection::duckdbconn::DuckDBParameter;
use datafusion_table_providers::util::column_reference::ColumnReference;
use datafusion_table_providers::util::indexes::IndexType;
use duckdb::DuckdbConnectionManager;
use r2d2::PooledConnection;
use std::collections::HashSet;
use std::fmt::Debug;
use std::ops::ControlFlow;
use std::sync::Arc;

static DIALECT: DuckDbDialect = DuckDbDialect {};
const CTE_NAME: &str = "_intermediate_materialize";

pub struct DuckDBIntermediateIndexMaterializationOptimizer {}

#[derive(Debug, Clone, PartialEq)]
struct SelectionWithIdents {
    expr: Expr,
    references: HashSet<String>,
}

impl SelectionWithIdents {
    pub fn from(expr: &Expr) -> Self {
        let mut references = HashSet::new();

        let _ = visit_expressions(expr, |e| {
            if let Expr::Identifier(id) = e {
                references.insert(id.value.clone());
            }

            ControlFlow::<()>::Continue(())
        });

        Self {
            expr: expr.clone(),
            references,
        }
    }
}

impl DuckDBIntermediateIndexMaterializationOptimizer {
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(DuckDBIntermediateIndexMaterializationOptimizer {})
    }

    /// Walk the `Expr` collecting all AND bin-ops
    fn collect_conjunctive_filters(expr: &Expr) -> Vec<SelectionWithIdents> {
        let mut selections = vec![];

        let _ = visit_expressions(expr, |e| {
            let Expr::BinaryOp { op, .. } = e else {
                return ControlFlow::<()>::Continue(());
            };

            match op {
                BinaryOperator::And => ControlFlow::<()>::Continue(()),
                BinaryOperator::Or | BinaryOperator::Xor => ControlFlow::<()>::Break(()),
                _ => {
                    selections.push(SelectionWithIdents::from(e));
                    ControlFlow::<()>::Continue(())
                }
            }
        });

        selections
    }

    /// Given the SELECT component of a statement and bound `DuckDB` indexes, attempt to build a
    /// materialized CTE with filters _only_ on index columns
    fn build_cte(
        select: &Select,
        indexes: &[(ColumnReference, IndexType)],
    ) -> Option<(Cte, Vec<SelectionWithIdents>)> {
        // There must be a `WHERE` otherwise we cannot apply the optimization
        let selection = select.selection.as_ref()?;

        // Collect all `AND` filters and assy a list of idents referenced in them.
        let filters = Self::collect_conjunctive_filters(selection);
        let all_filter_idents = filters
            .iter()
            .flat_map(|swi| swi.references.clone())
            .collect::<HashSet<_>>();

        // Find the first index we can bind (we can only bind one)
        let bindable_index = indexes.iter().find_map(|(cr, _)| {
            if cr.columns.iter().all(|c| all_filter_idents.contains(c)) {
                Some(cr.columns.iter().cloned().collect::<HashSet<_>>())
            } else {
                None
            }
        })?;

        // This query is already optimal
        if bindable_index == all_filter_idents {
            return None;
        }

        // Match filters to the index idents. An index may be satisfied by more than one filter.
        let cte_filters = filters
            .into_iter()
            .filter(|f| f.references.iter().all(|cr| bindable_index.contains(cr)))
            .collect::<Vec<_>>();

        // It may be possible for an expr to reference many columns, so a binding can be satisfied
        // by one or more exprs
        let cte_columns = cte_filters
            .iter()
            .flat_map(|swi| swi.references.iter())
            .cloned()
            .collect::<HashSet<_>>();

        // TODO: it may be possible to rewrite variants where this is true
        if cte_columns != bindable_index {
            return None;
        }

        // This is the selection expression for the CTE
        let cte_selection = cte_filters
            .iter()
            .map(|swi| swi.expr.clone())
            .reduce(|a, b| Expr::BinaryOp {
                left: Box::new(a),
                right: Box::new(b),
                op: BinaryOperator::And,
            })
            .or_else(|| cte_filters.last().map(|f| f.expr.clone()));

        // Copy the input select overriding `WHERE`, build the CTE
        let mut cte_select = select.clone();
        cte_select.selection = cte_selection;

        // Outer query filters may reference columns not in the projection, so we need to pass
        // them along
        let remaining_filter_columns: HashSet<_> = all_filter_idents
            .difference(&cte_columns)
            .cloned()
            .collect();

        // But not for SELECT *
        let has_wildcard = cte_select
            .projection
            .iter()
            .any(|item| matches!(item, SelectItem::Wildcard(_)));

        if !has_wildcard && !remaining_filter_columns.is_empty() {
            let mut projected_columns = HashSet::new();
            for item in &cte_select.projection {
                let _ = visit_expressions(item, |e| {
                    if let Expr::Identifier(id) = e {
                        projected_columns.insert(id.value.clone());
                    }
                    ControlFlow::<()>::Continue(())
                });
            }

            let mut missing_columns: Vec<_> = remaining_filter_columns
                .difference(&projected_columns)
                .cloned()
                .collect();
            missing_columns.sort();

            for col in missing_columns {
                cte_select
                    .projection
                    .push(SelectItem::UnnamedExpr(Expr::Identifier(Ident::new(&col))));
            }
        }

        let table_alias = TableAlias {
            name: Ident::new(CTE_NAME),
            columns: vec![],
        };

        let cte_query = Query {
            with: None,
            body: Box::new(SetExpr::Select(Box::new(cte_select))),
            order_by: None,
            limit_clause: None,
            fetch: None,
            locks: vec![],
            for_clause: None,
            settings: None,
            format_clause: None,
            pipe_operators: vec![],
        };

        let cte = Cte {
            alias: table_alias,
            query: Box::new(cte_query),
            from: None,
            materialized: Some(CteAsMaterialized::Materialized),
            closing_paren_token: AttachedToken::empty(),
        };

        Some((cte, cte_filters))
    }

    pub(crate) fn rewrite_statement(
        statement: &Statement,
        indexes: &[(ColumnReference, IndexType)],
    ) -> Option<Statement> {
        let mut relation_count: usize = 0;
        let _ = visit_relations(statement, |_| {
            relation_count += 1;
            ControlFlow::<()>::Continue(())
        });

        if relation_count > 1 {
            return None;
        }

        // Unfurl the AST to the SetExpr node
        let Statement::Query(query) = statement else {
            return None;
        };

        let SetExpr::Select(select) = query.body.as_ref() else {
            return None;
        };

        // Bind index filters, build CTE
        let (index_cte, bound_filters) = Self::build_cte(select.as_ref(), indexes)?;

        let mut outer_selections = select.selection.clone()?;

        // Rewrite any predicates used in the filter with no-op truthy value
        let exprs_to_noop = bound_filters
            .into_iter()
            .map(|f| f.expr)
            .collect::<HashSet<_>>();

        let _ = visit_expressions_mut(&mut outer_selections, |e| {
            if exprs_to_noop.contains(e) {
                *e = Expr::Value(ValueWithSpan {
                    value: Value::Boolean(true),
                    span: Span::empty(),
                });
            }

            ControlFlow::<()>::Continue(())
        });

        // Build the new select
        let mut new_select = select.as_ref().clone();

        // From should point to our intermediate materialized CTE
        new_select.from = vec![TableWithJoins {
            relation: TableFactor::Table {
                name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new(CTE_NAME))]),
                alias: None,
                args: None,
                with_hints: vec![],
                version: None,
                with_ordinality: false,
                partitions: vec![],
                json_path: None,
                sample: None,
                index_hints: vec![],
            },
            joins: vec![],
        }];

        // The selection now has all predicates except for those bound to the intermediate CTE
        new_select.selection = Some(outer_selections);

        // Build the new query, with all the new pieces
        let mut new_query = query.as_ref().clone();
        new_query.body = Box::new(SetExpr::Select(Box::new(new_select)));
        new_query.with = Some(With {
            with_token: AttachedToken::empty(),
            recursive: false,
            cte_tables: vec![index_cte],
        });

        Some(Statement::Query(Box::new(new_query)))
    }
}

impl Debug for DuckDBIntermediateIndexMaterializationOptimizer {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "DuckDBIntermediateIndexMaterializationOptimizer")
    }
}

type ConcreteDuckSqlExec = DuckSqlExec<PooledConnection<DuckdbConnectionManager>, DuckDBParameter>;

impl PhysicalOptimizerRule for DuckDBIntermediateIndexMaterializationOptimizer {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Find DuckSqlExec
        let Some(exec) = SearchVisitor::first_concrete_down::<ConcreteDuckSqlExec>(&plan)? else {
            return Ok(plan);
        };

        let Some(duck_exec) = concrete!(exec, ConcreteDuckSqlExec) else {
            return Ok(plan);
        };

        // Get its SQL + statement
        let sql = duck_exec.base_sql().map_err(|e| {
            DataFusionError::Execution(format!("Unable to generate DuckDB SQL: {e}"))
        })?;

        let Some(statement) = Parser::parse_sql(&DIALECT, sql.as_str())?.first().cloned() else {
            return Ok(plan);
        };

        let Some(new_statement) = Self::rewrite_statement(&statement, duck_exec.indexes()) else {
            return Ok(plan);
        };

        let old_exec_key = PlanNodeKey::from(exec.as_ref());

        // Finally, replace the old DuckSqlExec with the optimized one
        let transformed = plan.transform_down(|node| {
            let node_key = PlanNodeKey::from(node.as_ref());

            if node_key == old_exec_key {
                let new_exec = duck_exec
                    .clone()
                    .with_optimized_sql(format!("{new_statement}"));

                Ok(Transformed::yes(Arc::new(new_exec)))
            } else {
                Ok(Transformed::no(node))
            }
        });

        transformed.map(|t| t.data)
    }

    fn name(&self) -> &'static str {
        "DuckDBIntermediateIndexMaterialization"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_table_providers::util::column_reference::ColumnReference;
    use datafusion_table_providers::util::indexes::IndexType;

    fn parse_statement(sql: &str) -> Statement {
        Parser::parse_sql(&DIALECT, sql)
            .expect("Failed to parse SQL")
            .into_iter()
            .next()
            .expect("No statement found")
    }

    fn make_index(columns: &[&str]) -> (ColumnReference, IndexType) {
        (
            ColumnReference {
                columns: columns.iter().map(|s| (*s).to_string()).collect(),
            },
            IndexType::Enabled,
        )
    }

    #[test]
    #[allow(clippy::type_complexity)]
    fn test_rewrite_statement() {
        let test_cases: Vec<(&str, Vec<(ColumnReference, IndexType)>, Option<&str>)> = vec![
            // core query we want to optimize
            (
                "SELECT * FROM foo WHERE a = 1 AND b = 2 AND c = 3",
                vec![make_index(&["a", "b"])],
                Some(
                    "WITH _intermediate_materialize AS MATERIALIZED (SELECT * FROM foo WHERE a = 1 AND b = 2) SELECT * FROM _intermediate_materialize WHERE true AND true AND c = 3",
                ),
            ),
            // all filters covered by index - no rewrite
            (
                "SELECT * FROM foo WHERE a = 1 AND b = 2",
                vec![make_index(&["a", "b"])],
                None,
            ),
            // all filters covered, but subquery - no rewrite
            (
                "SELECT * FROM (SELECT * FROM foo) AS t WHERE a = 1 AND b = 2",
                vec![make_index(&["a", "b"])],
                None,
            ),
            // no filters
            ("SELECT * FROM foo", vec![make_index(&["a", "b"])], None),
            // c is not an indexed column
            (
                "SELECT * FROM foo WHERE a = 1 AND c = 3",
                vec![make_index(&["a", "b"])],
                None,
            ),
            // multiple filters on same column
            (
                "SELECT * FROM foo WHERE a = 1 AND a > 0 AND b = 2 AND c = 3",
                vec![make_index(&["a", "b"])],
                Some(
                    "WITH _intermediate_materialize AS MATERIALIZED (SELECT * FROM foo WHERE a = 1 AND a > 0 AND b = 2) SELECT * FROM _intermediate_materialize WHERE true AND true AND true AND c = 3",
                ),
            ),
            // single column index
            (
                "SELECT * FROM foo WHERE a = 1 AND b = 2",
                vec![make_index(&["a"])],
                Some(
                    "WITH _intermediate_materialize AS MATERIALIZED (SELECT * FROM foo WHERE a = 1) SELECT * FROM _intermediate_materialize WHERE true AND b = 2",
                ),
            ),
            // multiple indexes but only one is bindable
            (
                "SELECT * FROM foo WHERE a = 1 AND b = 2 AND c = 3",
                vec![make_index(&["a", "b"]), make_index(&["c", "d"])],
                Some(
                    "WITH _intermediate_materialize AS MATERIALIZED (SELECT * FROM foo WHERE a = 1 AND b = 2) SELECT * FROM _intermediate_materialize WHERE true AND true AND c = 3",
                ),
            ),
            // not bindable
            (
                "SELECT * FROM foo WHERE z = 1",
                vec![make_index(&["a", "b"])],
                None,
            ),
            // more than one relation (no joins)
            (
                "SELECT * FROM foo JOIN bar ON foo.id = bar.id WHERE a = 1 AND b = 2",
                vec![make_index(&["a", "b"])],
                None,
            ),
            // projection filters on (a,b,c), cte filters on (a,b) but needs c for outer
            (
                "SELECT d FROM foo WHERE a = 1 AND b = 2 AND c = 3",
                vec![make_index(&["a", "b"])],
                Some(
                    "WITH _intermediate_materialize AS MATERIALIZED (SELECT d, c FROM foo WHERE a = 1 AND b = 2) SELECT d FROM _intermediate_materialize WHERE true AND true AND c = 3",
                ),
            ),
            // ensure order by is preserved
            (
                "SELECT * FROM foo WHERE a = 1 AND b = 2 AND c = 3 ORDER BY d",
                vec![make_index(&["a", "b"])],
                Some(
                    "WITH _intermediate_materialize AS MATERIALIZED (SELECT * FROM foo WHERE a = 1 AND b = 2) SELECT * FROM _intermediate_materialize WHERE true AND true AND c = 3 ORDER BY d",
                ),
            ),
        ];

        test_cases.into_iter().enumerate().for_each(
            |(i, (input_sql, indexes, expected_pattern))| {
                let input_stmt = parse_statement(input_sql);
                let result = DuckDBIntermediateIndexMaterializationOptimizer::rewrite_statement(
                    &input_stmt,
                    &indexes,
                );

                assert_eq!(
                    expected_pattern.map(String::from),
                    result.map(|s| format!("{s}")),
                    "Query {i} must be rewritten correctly"
                );
            },
        );
    }
}
