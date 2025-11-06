use std::any;
use std::collections::HashSet;
use crate::common::search_visitor::SearchVisitor;
use crate::concrete;
use datafusion::common::Result;
use datafusion::config::ConfigOptions;
use datafusion::error::DataFusionError;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::sql::sqlparser::dialect::{Dialect, DuckDbDialect};
use datafusion::sql::sqlparser::parser::Parser;
use datafusion_table_providers::duckdb::sql_table::DuckSqlExec;
use datafusion_table_providers::sql::db_connection_pool::dbconnection::duckdbconn::DuckDBParameter;
use duckdb::DuckdbConnectionManager;
use r2d2::PooledConnection;
use std::fmt::Debug;
use std::ops::ControlFlow;
use std::sync::Arc;
use datafusion::logical_expr::sqlparser::ast::{CteAsMaterialized, ObjectName, Query};
use datafusion::prelude::SessionContext;
use datafusion::sql::planner::SqlToRel;
use datafusion::sql::sqlparser::ast::{visit_expressions, visit_statements, BinaryOperator, Cte, Expr, Ident, ObjectNamePart, SelectItem, SetExpr, Statement, TableAlias, TableFactor, TableWithJoins, With};
use datafusion::sql::sqlparser::ast::Expr::Identifier;
use datafusion::sql::sqlparser::ast::helpers::attached_token::AttachedToken;
use datafusion::sql::unparser::ast::SelectBuilder;

static DIALECT: DuckDbDialect = DuckDbDialect {};

pub struct DuckDBIntermediateIndexMaterializationOptimizer {
    ctx: Arc<SessionContext>,
}

impl DuckDBIntermediateIndexMaterializationOptimizer {
    pub fn new(ctx: Arc<SessionContext>) -> Arc<Self> {
        Arc::new(DuckDBIntermediateIndexMaterializationOptimizer {
            ctx
        })
    }
    fn collect_conjunctive(expr: &Expr, exprs: &mut Vec<Expr>) {
        if let Expr::BinaryOp { left, right, op: BinaryOperator::And } = expr {
            Self::collect_conjunctive(left, exprs);
            Self::collect_conjunctive(right, exprs)
        } else {
            exprs.push(expr.clone());
        }
    }
}

impl Debug for DuckDBIntermediateIndexMaterializationOptimizer {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "DuckDBIntermediateIndexMaterializationOptimizer")
    }
}

// TODO: why does everything need to know about the pool?
type ConcreteDuckSqlExec = DuckSqlExec<PooledConnection<DuckdbConnectionManager>, DuckDBParameter>;

impl PhysicalOptimizerRule for DuckDBIntermediateIndexMaterializationOptimizer {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Find DuckSqlExec
        let Some(exec) = SearchVisitor::first_concrete_down::<ConcreteDuckSqlExec>(&plan)? else {
            return Ok(plan);
        };

        let Some(duck_exec) = concrete!(exec, ConcreteDuckSqlExec) else {
            return Ok(plan);
        };

        // Get its SQL + statement
        let sql = duck_exec.base_exec.sql().map_err(|e| {
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

        let Some(selection) = select.selection.as_ref() else {
            return Ok(plan);
        };

        // Find all the AND filters and the columns they reference
        let mut and_filters = vec![];
        let mut and_filter_refs = vec![];
        Self::collect_conjunctive(selection, &mut and_filters);

        for filter in &and_filters {
            let mut references = vec![];

            let _ = visit_expressions(filter, |e| {
                if let Expr::Identifier(id) = e {
                    references.push(id.value.clone());
                };

                ControlFlow::<()>::Continue(())
            });

            and_filter_refs.push(references);
        }

        let filter_colset = and_filter_refs.iter().flatten().cloned().collect::<HashSet<_>>();

        // Find an index we can bind
        let Some(bindable_index) = duck_exec.indexes.iter().filter_map(|(cr, _)| {
            if cr.columns.iter().all(|c| filter_colset.contains(c)) {
                Some(cr.columns.clone())
            } else {
                None
            }
        }).next() else {
            return Ok(plan);
        };

        let bindable_index_colset = bindable_index.iter().cloned().collect::<HashSet<_>>();

        let mut cte_selections = vec![];
        for (i, refs) in and_filter_refs.iter().enumerate() {
            if refs.iter().any(|c| bindable_index_colset.contains(c)) {
                cte_selections.push(and_filters[i].clone());
            }
        }

        let cte_selection = cte_selections.clone().into_iter().reduce(|a, b| {
            Expr::BinaryOp {
                left: Box::new(a),
                right: Box::new(b),
                op: BinaryOperator::And,
            }
        }).or(cte_selections.first().cloned());

        let mut cte_select = SelectBuilder::default()
            .projection(select.projection.clone())
            .selection(cte_selection)
            .build()?;

        cte_select.from = select.from.clone();

        let ta = TableAlias {
            name: Ident::new("_intermediate_materialize"),
            columns: vec![]
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
            alias: ta,
            query: Box::new(cte_query),
            from: None,
            materialized: Some(CteAsMaterialized::Materialized),
            closing_paren_token: AttachedToken::empty()
        };

        let mut new_select = select.as_ref().clone();
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

        let mut new_query = query.as_ref().clone();
        new_query.body = Box::new(SetExpr::Select(Box::new(new_select)));
        new_query.with = Some(With {
            with_token: AttachedToken::empty(),
            recursive: false,
            cte_tables: vec![cte],
        });

        let new_statement = Statement::Query(Box::new(new_query));

        let new_exec = duck_exec.clone()
            .with_optimized_sql(format!("{}", new_statement));

        Ok(Arc::new(new_exec))
    }

    fn name(&self) -> &str {
        "DuckDBIntermediateIndexMaterialization"
    }

    fn schema_check(&self) -> bool {
        true
    }
}
