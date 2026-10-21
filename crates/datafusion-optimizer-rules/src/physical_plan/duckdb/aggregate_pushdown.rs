use crate::common::search_visitor::SearchVisitor;
use crate::concrete;
use crate::physical_plan::duckdb::ConcreteDuckSqlExec;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Result, Statistics, exec_err};
use datafusion::config::ConfigOptions;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{
    Distribution, OrderingRequirements, PhysicalExpr, PhysicalSortExpr,
};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::execution_plan::{CardinalityEffect, InvariantLevel};
use datafusion::physical_plan::filter_pushdown::{
    ChildPushdownResult, FilterDescription, FilterPushdownPhase, FilterPushdownPropagation,
};
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{
    ChildStats, DisplayAs, DisplayFormatType, ExecutionPlan, InputDistributionRequirements,
    PlanProperties, ReplaceChildrenOptions, SortOrderPushdownResult, StatisticsArgs,
    StatisticsContext,
};
use datafusion::sql::unparser::Unparser;
use datafusion::sql::unparser::dialect::DuckDBDialect;
use datafusion_expr::LogicalPlan;
use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;

/// Physical planning counterpart to `DuckDBAggregateLogicalPushdown`.
/// Looks for physical plan marker nodes and rewrites them with a `DuckSqlExec` that satisfies the whole plan subtree.
#[derive(Debug)]
pub struct DuckDBAggregatePushdownMarkerExec {
    logical_plan: LogicalPlan,
    input: Arc<dyn ExecutionPlan>,
}

impl DuckDBAggregatePushdownMarkerExec {
    pub fn new(logical_plan: LogicalPlan, input: Arc<dyn ExecutionPlan>) -> Arc<Self> {
        Arc::new(DuckDBAggregatePushdownMarkerExec {
            logical_plan,
            input,
        })
    }

    fn name() -> &'static str {
        "DuckDBAggregatePushdownMarkerExec"
    }

    fn with_input(&self, children: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> {
        let [input]: [Arc<dyn ExecutionPlan>; 1] = children.try_into().map_err(|_| {
            datafusion::error::DataFusionError::Plan(
                "DuckDBAggregatePushdownMarkerExec is unary, but has more than one input"
                    .to_string(),
            )
        })?;
        Ok(Self::new(self.logical_plan.clone(), input))
    }
}

#[deny(clippy::missing_trait_methods)]
impl ExecutionPlan for DuckDBAggregatePushdownMarkerExec {
    fn downcast_delegate(&self) -> Option<&dyn ExecutionPlan> {
        None
    }

    fn with_preserve_order(&self, _preserve_order: bool) -> Option<Arc<dyn ExecutionPlan>> {
        None
    }

    fn name(&self) -> &'static str {
        Self::name()
    }

    fn static_name() -> &'static str
    where
        Self: Sized,
    {
        Self::name()
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }

    fn check_invariants(&self, _check: InvariantLevel) -> Result<()> {
        Ok(())
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::UnspecifiedDistribution; self.children().len()]
    }

    fn input_distribution_requirements(&self) -> InputDistributionRequirements {
        InputDistributionRequirements::new(vec![
            Distribution::UnspecifiedDistribution;
            self.children().len()
        ])
    }

    /// A marker that is rewritten before execution; it owns no dynamic filters.
    fn dynamic_expressions_produced(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        Vec::new()
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        self.input.required_input_ordering()
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        self.input.maintains_input_order()
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(
            &Arc<dyn datafusion::physical_plan::PhysicalExpr>,
        ) -> datafusion::error::Result<
            datafusion::common::tree_node::TreeNodeRecursion,
        >,
    ) -> datafusion::error::Result<datafusion::common::tree_node::TreeNodeRecursion> {
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        _options: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // `properties()` is read from the input, so there is nothing to keep or recompute.
        self.with_input(children)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.with_input(children)
    }

    fn with_new_children_and_same_properties(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.with_input(children)
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

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>> {
        StatisticsContext::new().compute(
            self.input.as_ref(),
            &StatisticsArgs::new().with_partition(partition),
        )
    }

    fn child_stats_requests(&self, partition: Option<usize>) -> Vec<ChildStats> {
        vec![ChildStats::At(partition)]
    }

    /// The marker stands in for its input subtree, so it reports the input's statistics.
    fn statistics_from_inputs(
        &self,
        input_stats: &[Arc<Statistics>],
        _args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        let Some(stats) = input_stats.first() else {
            return exec_err!("DuckDBAggregatePushdownMarkerExec requires exactly one input");
        };
        Ok(Arc::clone(stats))
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

    fn try_pushdown_sort(
        &self,
        _order: &[PhysicalSortExpr],
    ) -> Result<SortOrderPushdownResult<Arc<dyn ExecutionPlan>>> {
        Ok(SortOrderPushdownResult::Unsupported)
    }

    fn try_swapping_with_projection(
        &self,
        projection: &ProjectionExec,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        // Implementations of `try_swapping_with_projection` read the projection's
        // input as the node being swapped with (a `ProjectionExec` collapses the
        // chain starting there, `CoalescePartitionsExec` rebuilds from its child),
        // so the projection is re-rooted on the input before it is handed down.
        // This node's schema is its input's, so the expressions carry over.
        let projection = ProjectionExec::try_new_with_schema_metadata(
            projection.expr().iter().cloned(),
            Arc::clone(&self.input),
            projection.schema().as_ref(),
        )?;
        if let Some(swapped) = self.input.try_swapping_with_projection(&projection)? {
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

    /// Not serializable: the marker must be rewritten into a `DuckSqlExec` before execution.
    fn try_to_proto(
        &self,
        _ctx: &datafusion::physical_plan::proto::ExecutionPlanEncodeCtx<'_>,
    ) -> Result<Option<datafusion_proto::protobuf::PhysicalPlanNode>> {
        Ok(None)
    }
}

impl DisplayAs for DuckDBAggregatePushdownMarkerExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "DuckDBAggregatePushdownMarkerExec")
    }
}

#[derive(Debug)]
pub struct DuckDBAggregatePushdownRewriter {}

impl DuckDBAggregatePushdownRewriter {
    #[must_use]
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
                return Ok(Transformed::no(p));
            };

            let Some(maybe_duck_exec) =
                SearchVisitor::first_concrete_down::<ConcreteDuckSqlExec>(&p)?
            else {
                return exec_err!("DuckDBAggregatePushdownMarkerExec was found with no DuckSqlExec child. This is a bug.")
            };

            let Some(duck_exec) = concrete!(maybe_duck_exec, ConcreteDuckSqlExec) else {
                return exec_err!("Cannot cast DuckSqlExec for rewriting. This is a bug.")
            };

            let optimized_sql = unparser.plan_to_sql(&marker.logical_plan)?;
            let logical_plan_schema = Arc::clone(marker.logical_plan.schema().inner());

            let rewritten = duck_exec
                .clone()
                .with_optimized_sql(optimized_sql.to_string(), Some(logical_plan_schema));

            Ok(Transformed::new(
                Arc::new(rewritten),
                true,
                TreeNodeRecursion::Jump,
            ))
        });

        maybe_new_plan.map(|t| t.data)
    }

    fn name(&self) -> &'static str {
        "DuckDBAggregatePushdown"
    }

    fn schema_check(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::logical_expr::LogicalPlanBuilder;
    use datafusion::physical_expr::expressions::col;
    use datafusion::physical_optimizer::projection_pushdown::ProjectionPushdown;
    use datafusion::physical_plan::displayable;

    /// A projection above the marker, over a projection the marker holds, must
    /// collapse beneath the marker. Delegated without re-rooting, the inner
    /// `ProjectionExec` hands the projection back still above the marker, and
    /// the projection pushdown nests markers on every step without bound.
    #[test]
    fn projection_over_marked_projection_collapses_beneath_the_marker() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let scan = MemorySourceConfig::try_new_exec(&[vec![]], Arc::clone(&schema), None)
            .expect("memory scan should be created");
        let id = col("id", &schema).expect("id column should exist");
        let inner = Arc::new(
            ProjectionExec::try_new(
                vec![(Arc::clone(&id), "a".to_string()), (id, "b".to_string())],
                scan,
            )
            .expect("inner projection should be created"),
        );
        let logical_plan = LogicalPlanBuilder::empty(false)
            .build()
            .expect("empty logical plan should build");
        let marker: Arc<dyn ExecutionPlan> =
            DuckDBAggregatePushdownMarkerExec::new(logical_plan, inner);
        let marker_schema = marker.schema();
        let outer = ProjectionExec::try_new(
            vec![
                (col("b", &marker_schema).expect("b column"), "x".to_string()),
                (col("a", &marker_schema).expect("a column"), "y".to_string()),
            ],
            Arc::clone(&marker),
        )
        .expect("outer projection should be created");

        let swapped = marker
            .try_swapping_with_projection(&outer)
            .expect("projection swap should be attempted")
            .expect("the projections should collapse");
        let child = swapped.children()[0];
        assert!(
            swapped.is::<DuckDBAggregatePushdownMarkerExec>()
                && child.is::<ProjectionExec>()
                && !child.children()[0].is::<DuckDBAggregatePushdownMarkerExec>(),
            "expected one projection beneath the marker, got:\n{}",
            displayable(swapped.as_ref()).indent(true)
        );
        assert_eq!(swapped.schema(), outer.schema());

        let optimized = ProjectionPushdown::new()
            .optimize(Arc::new(outer), &ConfigOptions::default())
            .expect("projection pushdown should succeed");
        let rendered = displayable(optimized.as_ref()).indent(true).to_string();
        assert_eq!(
            rendered
                .matches("DuckDBAggregatePushdownMarkerExec")
                .count(),
            1,
            "projection pushdown must not nest markers:\n{rendered}"
        );
    }
}
