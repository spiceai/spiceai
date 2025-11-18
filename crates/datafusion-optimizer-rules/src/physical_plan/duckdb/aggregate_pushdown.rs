use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{exec_err, Result};
use datafusion::config::ConfigOptions;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PhysicalExpr, PlanProperties};
use datafusion_expr::{Literal, LogicalPlan, UserDefinedLogicalNodeCore};
use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;
use datafusion::sql::unparser::dialect::DuckDBDialect;
use datafusion::sql::unparser::Unparser;
use crate::common::search_visitor::SearchVisitor;
use crate::concrete;
use crate::physical_plan::duckdb::ConcreteDuckSqlExec;

#[derive(Debug)]
pub struct DuckDBAggregatePushdownMarkerExec {
    logical_plan: LogicalPlan,
    input: Arc<dyn ExecutionPlan>,
}

impl DuckDBAggregatePushdownMarkerExec {
    pub fn new(logical_plan: LogicalPlan, input: Arc<dyn ExecutionPlan>) -> Arc<Self> {
        Arc::new(DuckDBAggregatePushdownMarkerExec { logical_plan, input })
    }
}

impl ExecutionPlan for DuckDBAggregatePushdownMarkerExec {
    fn name(&self) -> &str {
        "DuckDBAggregatePushdownMarkerExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        self.input.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> {
        assert_eq!(children.len(), 1, "DuckDBAggregatePushdownNode is unary");
        Ok(Self::new(
            self.logical_plan.clone(),
            Arc::clone(&children[0]),
        ))
    }

    fn execute(&self, _partition: usize, _context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        exec_err!("DuckDBAggregatePushdownNode must be rewritten, never executed. This is a bug.")
    }
}

impl DisplayAs for DuckDBAggregatePushdownMarkerExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "DuckDBAggregatePushdownMarkerExec")
    }
}


#[derive(Debug)]
pub struct DuckDBAggregatePushdownRewriter {}

impl DuckDBAggregatePushdownRewriter {
    pub fn new() -> Arc<Self> {
        Arc::new(DuckDBAggregatePushdownRewriter {})
    }
}

impl PhysicalOptimizerRule for DuckDBAggregatePushdownRewriter {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let dialect = DuckDBDialect::new();
        let unparser = Unparser::new(&dialect);

        let maybe_new_plan = plan.transform_down(|p| {
            let Some(marker) = concrete!(p, DuckDBAggregatePushdownMarkerExec) else {
                return Ok(Transformed::no(p))
            };

            let Some(maybe_duck_exec) = SearchVisitor::first_concrete_down::<ConcreteDuckSqlExec>(&p)? else {
                return Ok(Transformed::no(p))
            };

            let Some(duck_exec) = concrete!(maybe_duck_exec, ConcreteDuckSqlExec) else {
                return Ok(Transformed::no(p));
            };

            let optimized_sql = unparser.plan_to_sql(&marker.logical_plan)?;

            println!("optimized SQL: {}", optimized_sql);

            let rewritten = duck_exec
                .clone()
                .with_optimized_sql(optimized_sql.to_string());

            println!("rewritten: {:?}", rewritten.schema());

            Ok(Transformed::new(Arc::new(rewritten), true, TreeNodeRecursion::Jump))
        });

        maybe_new_plan.map(|t| t.data)
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

    }
}