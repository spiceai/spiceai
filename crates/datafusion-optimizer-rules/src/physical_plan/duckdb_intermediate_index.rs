use crate::common::plan_node_key::PlanNodeKey;
use crate::common::search_visitor::SearchVisitor;
use crate::concrete;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::Result;
use datafusion::config::ConfigOptions;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::sqlparser::ast::{CteAsMaterialized, ObjectName, Query};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::sql::sqlparser::ast::helpers::attached_token::AttachedToken;
use datafusion::sql::sqlparser::ast::{visit_expressions, visit_expressions_mut, BinaryOperator, Cte, Expr, Ident, ObjectNamePart, Select, SetExpr, Statement, TableAlias, TableFactor, TableWithJoins, Value, ValueWithSpan, With};
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

pub struct DuckDBIntermediateIndexMaterializationOptimizer {}

struct SelectionWithIdents {
    expr: Expr,
    references: HashSet<String>
}

impl SelectionWithIdents {
    pub fn from(expr: &Expr) -> Self {
        let mut references = HashSet::new();

        let _ = visit_expressions(expr, |e| {
            if let Expr::Identifier(id) = e {
                references.insert(id.value.clone());
            };

            ControlFlow::<()>::Continue(())
        });

        Self { expr: expr.clone(), references }
    }
}

impl DuckDBIntermediateIndexMaterializationOptimizer {
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

    /// Given the SELECT component of a statement and bound DuckDB indexes, attempt to build a
    /// materialized CTE with filters _only_ on index columns
    fn build_cte(select: &Box<Select>, indexes: &Vec<(ColumnReference, IndexType)>) -> Option<(Cte, Vec<SelectionWithIdents>)> {
        // There must be a `WHERE` otherwise we cannot apply the optimization
        let selection = select.selection.as_ref()?;

        // Collect all `AND` filters and assy a list of idents referenced in them.
        let filters = Self::collect_conjunctive_filters(selection);
        let all_filter_idents = filters
            .iter()
            .flat_map(|swi| swi.references.clone())
            .collect::<HashSet<_>>();

        // Find the first index we can bind (we can only bind one)
        let bindable_index = indexes.iter().filter_map(|(cr, _)| {
            if cr.columns.iter().all(|c| all_filter_idents.contains(c)) {
                Some(cr.columns.iter().cloned().collect::<HashSet<_>>())
            } else {
                None
            }
        }).next()?;

        // Match filters to the index idents. An index may be satisifed by more than one filter.
        let cte_filters = filters
            .into_iter()
            .filter(|f| {
                f.references.iter().all(|cr| all_filter_idents.contains(cr))
            })
            .collect::<Vec<_>>();

        // TODO: it may be possible to rewrite variants where this is true
        if cte_filters.len() != bindable_index.len() {
            return None;
        }

        // This is the selection expression for the CTE
        let cte_selection = cte_filters
            .iter()
            .map(|swi| swi.expr.clone())
            .reduce(|a, b| {
                Expr::BinaryOp {
                    left: Box::new(a),
                    right: Box::new(b),
                    op: BinaryOperator::And,
                }
            })
            .or_else(|| cte_filters.last().map(|f| f.expr.clone()));

        // Copy the input select overriding `WHERE`, build the CTE
        let mut cte_select = select.clone();
        cte_select.selection = cte_selection;

        let table_alias = TableAlias {
            name: Ident::new("_intermediate_materialize"),
            columns: vec![]
        };

        let cte_query = Query {
            with: None,
            body: Box::new(SetExpr::Select(cte_select)),
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
            closing_paren_token: AttachedToken::empty()
        };

        Some((cte, cte_filters))
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

        // Unfurl the AST to the SetExpr node
        let Statement::Query(query) = statement else {
            return Ok(plan);
        };

        let SetExpr::Select(select) = query.body.as_ref() else {
            return Ok(plan);
        };

        // Bind index filters, build CTE
        let Some((index_cte, bound_filters)) = Self::build_cte(&select, duck_exec.indexes()) else {
            return Ok(plan);
        };

        let Some(mut outer_selections) = select.selection.clone() else {
            return Ok(plan);
        };

        // Rewrite any predicates used in the filter with no-op truthy value
        let exprs_to_noop = bound_filters
            .into_iter()
            .map(|f| f.expr)
            .collect::<HashSet<_>>();

        let _ = visit_expressions_mut(&mut outer_selections, |e| {
            if exprs_to_noop.contains(e) {
                *e = Expr::Value(ValueWithSpan {
                    value: Value::Boolean(true),
                    span: Span::empty()
                });
            }

            ControlFlow::<()>::Continue(())
        });

        // Build the new select
        let mut new_select = select.as_ref().clone();

        // From should point to our intermediate materialized CTE
        new_select.from = vec![TableWithJoins {
            relation: TableFactor::Table {
                name: ObjectName(vec![ObjectNamePart::Identifier(
                    Ident::new("_intermediate_materialize")
                )]),
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

        let new_statement = Statement::Query(Box::new(new_query));
        let old_exec_key = PlanNodeKey::from(exec.as_ref());

        // Finally, replace the old DuckSqlExec with the optimized one
        let transformed = plan.transform_down(|node| {
            let node_key = PlanNodeKey::from(node.as_ref());

            if node_key == old_exec_key {
                let new_exec = duck_exec.clone()
                    .with_optimized_sql(format!("{}", new_statement));

                Ok(Transformed::yes(Arc::new(new_exec)))
            } else {
                Ok(Transformed::no(node))
            }
        });

        transformed.map(|t| t.data)
    }

    fn name(&self) -> &str {
        "DuckDBIntermediateIndexMaterialization"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use crate::physical_plan::duckdb_intermediate_index::DIALECT;
    use datafusion::logical_expr::sqlparser::parser::Parser;

    #[test]
    fn unparse_stuff() {
        let parsed = Parser::parse_sql(&DIALECT, "select * from foo where a = 1 and b = 2 and c = 3 and d = 4 and e = 5").unwrap();
        println!("{:#?}", parsed);
    }
}