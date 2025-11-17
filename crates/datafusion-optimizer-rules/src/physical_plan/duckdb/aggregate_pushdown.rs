use crate::common::plan_node_key::PlanNodeKey;
use crate::common::search_visitor::SearchVisitor;
use crate::concrete;
use crate::physical_plan::duckdb::{ConcreteDuckSqlExec, PARSER_DIALECT};
use datafusion::common::{plan_err, DataFusionError, Result};
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::sqlparser::ast::{visit_relations, Expr, Function, ObjectName, SetExpr, Statement};
use datafusion::physical_expr::{expressions, ScalarFunctionExpr};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr};
use datafusion::sql::sqlparser::ast::{FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, GroupByExpr, Ident, SelectItem};
use datafusion::sql::unparser;
use datafusion::sql::unparser::Unparser;
use std::collections::HashMap;
use std::ops::ControlFlow;
use std::sync::Arc;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::physical_expr::expressions::Column;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion_expr::{col, lit, Literal};

#[derive(Debug)]
pub struct DuckDBAggregatePushdownOptimizer {}

impl DuckDBAggregatePushdownOptimizer {
    pub fn new() -> Arc<Self> {
        Arc::new(DuckDBAggregatePushdownOptimizer {})
    }

    pub(crate) fn rewrite_statement(
        statement: &Statement,
        physical_aggregate: &AggregateExec,
    ) -> Result<Option<Statement>> {
        let mut relation_count: usize = 0;
        let _ = visit_relations(statement, |_| {
            relation_count += 1;
            ControlFlow::<()>::Continue(())
        });

        if relation_count > 1 {
            return Ok(None);
        }

        // Unfurl the AST to the SetExpr node
        let Statement::Query(query) = statement else {
            return Ok(None);
        };

        let SetExpr::Select(select) = query.body.as_ref() else {
            return Ok(None);
        };

        let phys_agg_exprs = physical_aggregate
            .aggr_expr()
            .iter()
            .flat_map(|e| {
                e.expressions().into_iter()
            })
            .collect::<Vec<_>>();

        let unparser_dialect = unparser::dialect::DuckDBDialect::new();
        let unparser = Unparser::new(&unparser_dialect);
        let ast_project_exprs = phys_agg_exprs
            .iter()
            .map(|e| Self::physical_expr_to_sql_expr(&unparser, e))
            .collect::<Result<Vec<_>>>()?;

        let ast_gby_exprs = physical_aggregate
            .group_expr()
            .expr()
            .iter()
            .filter_map(|(exp, alias)| {
                if let Some(column) = concrete!(exp, Column) {
                    Some(col(column.name()).alias(alias))
                } else {
                    None
                }
            })
            .map(|logical| unparser.expr_to_sql(&logical))
            .collect::<Result<Vec<_>>>()?;

        let mut new_select = select.clone();
        new_select.projection.extend(ast_project_exprs);
        new_select.group_by = GroupByExpr::Expressions(ast_gby_exprs, vec![]);

        let mut new_query = query.clone();

        new_query.body = Box::new(SetExpr::Select(new_select));

        Ok(Some(Statement::Query(new_query)))
    }

    /// Use the unparser to take scalar function expressions from the phys agg and convert them to AST
    fn physical_expr_to_sql_expr(unparser: &Unparser, expr: &Arc<dyn PhysicalExpr>) -> Result<SelectItem> {
        let Some(sfe) = concrete!(expr, ScalarFunctionExpr) else {
            return plan_err!("Unsupported physical expression {expr:?}")
        };

        let args = sfe
            .args()
            .iter()
            .filter_map(|a| {
                if let Some(literal) = concrete!(a, expressions::Literal) {
                    Some(literal.value().lit())
                } else if let Some(column) = concrete!(a, expressions::Column) {
                    Some(col(column.name()))
                } else {
                    None
                }
            })
            .map(|logical| {
                unparser.expr_to_sql(&logical).map(|sql| {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(sql))
                })
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(SelectItem::UnnamedExpr(
            Expr::Function(Function {
                name: ObjectName::from(vec![Ident::new(sfe.name())]),
                uses_odbc_syntax: false,
                parameters: FunctionArguments::List(
                    FunctionArgumentList {
                        duplicate_treatment: None,
                        args,
                        clauses: vec![]
                    }
                ),
                args: FunctionArguments::None,
                filter: None,
                null_treatment: None,
                over: None,
                within_group: vec![],
            })
        ))
    }

    /// Collect final aggregates from the plan, and ensure that each final agg subtree is disjoint
    fn find_eligible_aggs(plan: &Arc<dyn ExecutionPlan>) -> Result<Vec<Arc<dyn ExecutionPlan>>> {
        let output_aggs = SearchVisitor::default()
            .down(|p| {
                let concrete = concrete!(p, AggregateExec)?;
                match concrete.mode() {
                    AggregateMode::Final
                    | AggregateMode::FinalPartitioned
                    | AggregateMode::Single
                    | AggregateMode::SinglePartitioned => Some(Arc::clone(p)),
                    _ => None,
                }
            })
            .find(&plan)?;

        let all_subtrees = output_aggs
            .iter()
            .map(SearchVisitor::vec_down)
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

        let mut counts = all_subtrees
            .iter()
            .map(|p| (PlanNodeKey::from(p.as_ref()), 0)).collect::<HashMap<PlanNodeKey, usize>>();

        for node in all_subtrees {
            let key = PlanNodeKey::from(node.as_ref());
            let new_count = counts[&key] + 1;
            counts.insert(key, new_count);
        }

        if counts.values().all(|c| *c == 1) {
            Ok(output_aggs)
        } else {
            Ok(vec![])
        }
    }

    fn process_agg_tree(plan: &Arc<dyn ExecutionPlan>) -> Result<Option<HashMap<PlanNodeKey, Option<Arc<dyn ExecutionPlan>>>>> {
        let Some(agg) = concrete!(plan, AggregateExec) else {
            return Ok(None)
        };

        let flat = SearchVisitor::default()
            .down(|p| {
                if p.children().len() <= 1 {
                    Some(Arc::clone(p))
                } else {
                    None
                }
            })
            .find(plan)?;


        // Must end in a DuckSqlExec, or cannot optimize
        let Some(duck_exec) = flat
            .last()
            .and_then(|p| concrete!(p, ConcreteDuckSqlExec)) else {
            return Ok(None)
        };

        let sql = duck_exec.base_sql().map_err(|e| {
            DataFusionError::Execution(format!("Unable to generate DuckDB SQL: {e}"))
        })?;

        let Some(statement) = Parser::parse_sql(&PARSER_DIALECT, sql.as_str())?.first().cloned() else {
            return Ok(None);
        };

        let Some(rewritten) = Self::rewrite_statement(&statement, agg)? else {
            return Ok(None);
        };

        // Where None = delete
        let mut rewrites: HashMap<PlanNodeKey, Option<Arc<dyn ExecutionPlan>>> = HashMap::new();
        for node in &flat {
            if concrete!(node, AggregateExec).is_some() {
                rewrites.insert(node.as_ref().into(), None);
            } else if concrete!(node, ConcreteDuckSqlExec).is_some() {
                let new_duck_exec = duck_exec.clone().with_optimized_sql(
                    format!("{rewritten}")
                );
                rewrites.insert(node.as_ref().into(), Some(Arc::new(new_duck_exec)));
            }
        }

        Ok(Some(rewrites))
    }
}

impl PhysicalOptimizerRule for DuckDBAggregatePushdownOptimizer {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let plan_aggs = Self::find_eligible_aggs(&plan)?;

        // There are no aggregates in this plan
        if plan_aggs.is_empty() {
            return Ok(plan);
        }

        let all_rewrites = plan_aggs
            .iter()
            .map(Self::process_agg_tree)
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .flat_map(|m| m.into_iter())
            .collect::<HashMap<_, _>>();

        let transformed = plan.transform_down(|p| {
            match all_rewrites.get(&p.as_ref().into()) {
                Some(None) => {
                    match p.children().first() {
                        Some(child) => Ok(Transformed::yes(Arc::clone(child))),
                        _ => Ok(Transformed::no(p))
                    }
                },
                Some(Some(new)) => {
                    Ok(Transformed::yes(Arc::clone(new)))
                },
                None => Ok(Transformed::no(p))
            }
        })?;

        Ok(transformed.data)
    }

    fn name(&self) -> &str {
        "DuckDBAggregatePushdown"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

mod tests {
    use crate::physical_plan::duckdb::PARSER_DIALECT;
    use datafusion::logical_expr::sqlparser::parser::Parser;

    #[test]
    fn test_rewrite_agg() {
        let statement = Parser::parse_sql(&PARSER_DIALECT, "SELECT \"Data1\", \"Data2\", count(*) FROM tdata group by Data1").unwrap().first().cloned().unwrap();

        println!("{:#?}", statement);
    }
}