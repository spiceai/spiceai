use crate::common::search_visitor::SearchVisitor;
use crate::concrete;
use crate::physical_plan::duckdb::ConcreteDuckSqlExec;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Result, Statistics, exec_err, plan_err};
use datafusion::config::ConfigOptions;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{Distribution, OrderingRequirements, PhysicalExpr};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::execution_plan::{CardinalityEffect, InvariantLevel};
use datafusion::physical_plan::filter_pushdown::{
    ChildPushdownResult, FilterDescription, FilterPushdownPhase, FilterPushdownPropagation,
};
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion::sql::unparser::Unparser;
use datafusion::sql::unparser::dialect::DuckDBDialect;
use datafusion_expr::LogicalPlan;
use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;
use datafusion_federation::sql::MultiPartTableReference::TableReference;
use datafusion_table_providers::duckdb::sql_table::SimpleDuckSqlExec;
use datafusion_table_providers::sql::sql_provider_datafusion::SqlExec;

/// Physical planning counterpart to logical_plan::duckdb planners.
/// Looks for physical plan marker nodes and rewrites them with a `DuckSqlExec` that satisfies the whole plan subtree.
#[derive(Debug)]
pub struct DuckDBLogicalPlanPushdownMarkerExec {
    logical_plan: LogicalPlan,
    input: Arc<dyn ExecutionPlan>,
}

impl DuckDBLogicalPlanPushdownMarkerExec {
    pub fn new(logical_plan: LogicalPlan, input: Arc<dyn ExecutionPlan>) -> Arc<Self> {
        Arc::new(DuckDBLogicalPlanPushdownMarkerExec {
            logical_plan,
            input,
        })
    }

    fn name() -> &'static str {
        "DuckDBAggregatePushdownMarkerExec"
    }
}

#[deny(clippy::missing_trait_methods)]
impl ExecutionPlan for DuckDBLogicalPlanPushdownMarkerExec {
    fn name(&self) -> &'static str {
        Self::name()
    }

    fn static_name() -> &'static str
    where
        Self: Sized,
    {
        Self::name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn properties(&self) -> &PlanProperties {
        self.input.properties()
    }

    fn check_invariants(&self, _check: InvariantLevel) -> Result<()> {
        Ok(())
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        self.input.required_input_distribution()
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        self.input.required_input_ordering()
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        self.input.maintains_input_order()
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![true]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return plan_err!(
                "DuckDBAggregatePushdownMarkerExec is unary, but has more than one input"
            );
        }

        Ok(Self::new(
            self.logical_plan.clone(),
            Arc::clone(&children[0]),
        ))
    }

    fn reset_state(self: Arc<Self>) -> Result<Arc<dyn ExecutionPlan>> {
        let reset_input = Arc::clone(&self.input).reset_state()?;
        Ok(Self::new(self.logical_plan.clone(), reset_input))
    }

    fn repartitioned(
        &self,
        target_partitions: usize,
        config: &ConfigOptions,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        if let Some(repartitioned) = self.input.repartitioned(target_partitions, config)? {
            Ok(Some(Self::new(self.logical_plan.clone(), repartitioned)))
        } else {
            Ok(None)
        }
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        exec_err!("DuckDBAggregatePushdownNode must be rewritten, never executed. This is a bug.")
    }

    fn metrics(&self) -> Option<MetricsSet> {
        self.input.metrics()
    }

    // Deprecated, but need to allow because `missing_trait_methods` complains otherwise
    #[allow(deprecated)]
    fn statistics(&self) -> Result<Statistics> {
        self.input.statistics()
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        self.input.partition_statistics(partition)
    }

    fn supports_limit_pushdown(&self) -> bool {
        self.input.supports_limit_pushdown()
    }

    fn with_fetch(&self, _limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        None
    }

    // LIMIT is serialized as a part of the SQL string
    fn fetch(&self) -> Option<usize> {
        None
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        self.input.cardinality_effect()
    }

    fn try_swapping_with_projection(
        &self,
        projection: &ProjectionExec,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        if let Some(swapped) = self.input.try_swapping_with_projection(projection)? {
            Ok(Some(Self::new(self.logical_plan.clone(), swapped)))
        } else {
            Ok(None)
        }
    }

    fn gather_filters_for_pushdown(
        &self,
        phase: FilterPushdownPhase,
        parent_filters: Vec<Arc<dyn PhysicalExpr>>,
        config: &ConfigOptions,
    ) -> Result<FilterDescription> {
        self.input
            .gather_filters_for_pushdown(phase, parent_filters, config)
    }

    fn handle_child_pushdown_result(
        &self,
        _phase: FilterPushdownPhase,
        child_pushdown_result: ChildPushdownResult,
        _config: &ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
        Ok(FilterPushdownPropagation::if_all(child_pushdown_result))
    }

    fn with_new_state(&self, _state: Arc<dyn Any + Send + Sync>) -> Option<Arc<dyn ExecutionPlan>> {
        None
    }
}

impl DisplayAs for DuckDBLogicalPlanPushdownMarkerExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "DuckDBAggregatePushdownMarkerExec")
    }
}

#[derive(Debug)]
pub struct DuckDBLogicalPlanPushdownPhysicalRewriter {}

impl DuckDBLogicalPlanPushdownPhysicalRewriter {
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(DuckDBLogicalPlanPushdownPhysicalRewriter {})
    }
}

impl PhysicalOptimizerRule for DuckDBLogicalPlanPushdownPhysicalRewriter {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let dialect = DuckDBDialect::new();
        let unparser = Unparser::new(&dialect);

        let maybe_new_plan = plan.clone().transform_down(|p| {
            let Some(marker) = concrete!(p, DuckDBLogicalPlanPushdownMarkerExec) else {
                return Ok(Transformed::no(p));
            };

            println!("found marker");

            let Some(maybe_duck_exec) =
                SearchVisitor::first_concrete_down::<ConcreteDuckSqlExec>(&marker.input)?
            else {
                return exec_err!("DuckDBAggregatePushdownMarkerExec was found with no DuckSqlExec child. This is a bug.")
            };

            let Some(duck_exec) = concrete!(maybe_duck_exec, ConcreteDuckSqlExec) else {
                return exec_err!("Cannot cast DuckSqlExec for rewriting. This is a bug.")
            };

            println!("attempt unparse {}", marker.logical_plan.display_indent());
            let optimized_sql = unparser.plan_to_sql(&marker.logical_plan)?;
            let logical_plan_schema = Arc::clone(marker.logical_plan.schema().inner());

            println!("unparse ok");

            let new_exec = SimpleDuckSqlExec::new(
                duck_exec.get_pool(),
                optimized_sql.to_string(),
                logical_plan_schema,
                duck_exec.properties().clone(),
            );

            Ok(Transformed::new(
                Arc::new(new_exec),
                true,
                TreeNodeRecursion::Jump,
            ))
        })?;

        if maybe_new_plan.transformed {
            Ok(maybe_new_plan.data)
        } else {
            Ok(plan)
        }
    }

    fn name(&self) -> &'static str {
        "DuckDBAggregatePushdown"
    }

    fn schema_check(&self) -> bool {
        false
    }
}
