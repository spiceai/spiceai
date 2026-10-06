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

//! A serializable execution plan for UDTFs (User-Defined Table Functions).
//!
//! This execution plan wraps a UDTF invocation and can be serialized/deserialized
//! for distributed query execution. When executed on a remote node, the UDTF is
//! re-invoked with the stored arguments to produce results.

use arrow_schema::SchemaRef;
use datafusion::common::{Result, Statistics};
use datafusion::config::ConfigOptions;
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, OrderingRequirements};
use datafusion::physical_plan::execution_plan::{
    CardinalityEffect, InvariantLevel, check_default_invariants,
};
use datafusion::physical_plan::filter_pushdown::{
    ChildPushdownResult, FilterDescription, FilterPushdownPhase, FilterPushdownPropagation,
};
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{
    ChildStats, DisplayAs, DisplayFormatType, Distribution, EmptyRecordBatchStream, ExecutionPlan,
    ExecutionPlanProperties, InputDistributionRequirements, Partitioning, PhysicalExpr,
    PlanProperties, ReplaceChildrenOptions, SortOrderPushdownResult, StatisticsArgs,
    StatisticsContext, expressions::PhysicalSortExpr,
};
use runtime_proto::UdtfArgs;
use std::any::Any;
use std::fmt;
use std::sync::Arc;

/// An execution plan that wraps a UDTF invocation.
///
/// This plan stores the UDTF arguments and the inner execution plan produced by
/// the UDTF. The arguments enable serialization for distributed execution - when
/// deserialized on a remote executor, the UDTF can be re-invoked to produce the
/// same results.
///
/// The inner plan is the actual execution plan produced by `TableProvider::scan()`
/// on the UDTF's result table.
#[derive(Debug)]
pub struct UdtfExec {
    /// The UDTF arguments (serializable via protobuf).
    args: UdtfArgs,
    /// The inner execution plan from the UDTF's `TableProvider`.
    inner: Arc<dyn ExecutionPlan>,
    /// Cached plan properties.
    properties: Arc<PlanProperties>,
}

impl UdtfExec {
    /// Creates a new `UdtfExec` wrapping the given inner plan with UDTF arguments.
    #[must_use]
    pub fn new(args: UdtfArgs, inner: Arc<dyn ExecutionPlan>) -> Self {
        let schema = inner.schema();
        let eq_properties = EquivalenceProperties::new(schema);
        let emission_type = inner.pipeline_behavior();
        let boundedness = inner.boundedness();
        let properties = Arc::new(PlanProperties::new(
            eq_properties,
            inner.output_partitioning().clone(),
            emission_type,
            boundedness,
        ));

        Self {
            args,
            inner,
            properties,
        }
    }

    /// Creates a placeholder `UdtfExec` for deserialization.
    ///
    /// This is used when decoding a serialized plan - the inner plan will be
    /// reconstructed by re-invoking the UDTF.
    ///
    /// # Note
    /// This method is currently unused but reserved for future serialization enhancements.
    #[must_use]
    pub fn placeholder(args: UdtfArgs, schema: SchemaRef) -> Self {
        let eq_properties = EquivalenceProperties::new(Arc::clone(&schema));
        let properties = Arc::new(PlanProperties::new(
            eq_properties,
            Partitioning::UnknownPartitioning(1),
            datafusion::physical_plan::execution_plan::EmissionType::Final,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        ));

        // Create a placeholder inner plan - this will be replaced when the
        // plan is actually executed after reconstruction
        let placeholder_inner = Arc::new(PlaceholderExec::new(schema));

        Self {
            args,
            inner: placeholder_inner,
            properties,
        }
    }

    /// Returns the UDTF arguments.
    #[must_use]
    pub fn args(&self) -> &UdtfArgs {
        &self.args
    }

    /// Returns the inner execution plan.
    #[must_use]
    pub fn inner(&self) -> &Arc<dyn ExecutionPlan> {
        &self.inner
    }
}

impl UdtfExec {
    fn with_inner(&self, children: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> {
        let [inner]: [Arc<dyn ExecutionPlan>; 1] = children.try_into().map_err(|_| {
            DataFusionError::Execution("UdtfExec expects exactly one child".to_string())
        })?;
        Ok(Arc::new(Self::new(self.args.clone(), inner)))
    }
}

impl DisplayAs for UdtfExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(f, "UdtfExec")
            }
        }
    }
}

#[deny(clippy::missing_trait_methods)]
impl ExecutionPlan for UdtfExec {
    fn downcast_delegate(&self) -> Option<&dyn ExecutionPlan> {
        None
    }

    fn with_preserve_order(&self, _preserve_order: bool) -> Option<Arc<dyn ExecutionPlan>> {
        None
    }

    fn name(&self) -> &'static str {
        "UdtfExec"
    }

    fn static_name() -> &'static str
    where
        Self: Sized,
    {
        "UdtfExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn check_invariants(&self, check: InvariantLevel) -> Result<()> {
        check_default_invariants(self, check)
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::UnspecifiedDistribution]
    }

    fn input_distribution_requirements(&self) -> InputDistributionRequirements {
        InputDistributionRequirements::new(vec![Distribution::UnspecifiedDistribution])
    }

    fn dynamic_expressions_produced(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        Vec::new()
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        vec![None]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
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
        // Return inner as a child so optimizers can traverse it
        vec![&self.inner]
    }

    /// Always rebuilds through `new`, which derives the properties from the inner
    /// plan; that is correct whichever mode is requested.
    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        _options: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.with_inner(children)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.with_inner(children)
    }

    fn with_new_children_and_same_properties(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.with_inner(children)
    }

    fn reset_state(self: Arc<Self>) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn repartitioned(
        &self,
        _target_partitions: usize,
        _config: &ConfigOptions,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        Ok(None)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // Delegate execution to the inner plan
        self.inner.execute(partition, context)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        self.inner.metrics()
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>> {
        StatisticsContext::new().compute(
            self.inner.as_ref(),
            &StatisticsArgs::new().with_partition(partition),
        )
    }

    fn child_stats_requests(&self, partition: Option<usize>) -> Vec<ChildStats> {
        vec![ChildStats::At(partition)]
    }

    /// Execution delegates to the inner plan, so its statistics are this node's.
    fn statistics_from_inputs(
        &self,
        input_stats: &[Arc<Statistics>],
        _args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        input_stats.first().map(Arc::clone).ok_or_else(|| {
            DataFusionError::Execution("UdtfExec expects exactly one child".to_string())
        })
    }

    fn supports_limit_pushdown(&self) -> bool {
        false
    }

    fn with_fetch(&self, _limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        None
    }

    fn fetch(&self) -> Option<usize> {
        None
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }

    fn try_swapping_with_projection(
        &self,
        _projection: &ProjectionExec,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        Ok(None)
    }

    fn gather_filters_for_pushdown(
        &self,
        _phase: FilterPushdownPhase,
        parent_filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> Result<FilterDescription> {
        // UDTFs don't support filter pushdown - mark all filters as unsupported
        Ok(FilterDescription::all_unsupported(
            &parent_filters,
            &self.children(),
        ))
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

    fn try_pushdown_sort(
        &self,
        _order: &[PhysicalSortExpr],
    ) -> Result<SortOrderPushdownResult<Arc<dyn ExecutionPlan>>> {
        Ok(SortOrderPushdownResult::Unsupported)
    }

    /// `None` defers to Spice's physical extension codec, which serializes this
    /// node as its UDTF arguments and re-invokes the UDTF on the remote side.
    fn try_to_proto(
        &self,
        _ctx: &datafusion::physical_plan::proto::ExecutionPlanEncodeCtx<'_>,
    ) -> Result<Option<datafusion_proto::protobuf::PhysicalPlanNode>> {
        Ok(None)
    }
}

/// A placeholder execution plan used during deserialization.
///
/// This plan should never actually be executed - it exists only as a placeholder
/// until the real inner plan is reconstructed by re-invoking the UDTF.
#[derive(Debug)]
struct PlaceholderExec {
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl PlaceholderExec {
    fn new(schema: SchemaRef) -> Self {
        let eq_properties = EquivalenceProperties::new(Arc::clone(&schema));
        let properties = Arc::new(PlanProperties::new(
            eq_properties,
            Partitioning::UnknownPartitioning(1),
            datafusion::physical_plan::execution_plan::EmissionType::Final,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        ));
        Self { schema, properties }
    }
}

impl PlaceholderExec {
    fn with_no_children(
        self: Arc<Self>,
        children: &[Arc<dyn ExecutionPlan>],
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Execution(
                "PlaceholderExec expects no children".to_string(),
            ))
        }
    }
}

impl DisplayAs for PlaceholderExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "PlaceholderExec")
    }
}

#[deny(clippy::missing_trait_methods)]
impl ExecutionPlan for PlaceholderExec {
    fn downcast_delegate(&self) -> Option<&dyn ExecutionPlan> {
        None
    }

    fn with_preserve_order(&self, _preserve_order: bool) -> Option<Arc<dyn ExecutionPlan>> {
        None
    }

    fn name(&self) -> &'static str {
        "PlaceholderExec"
    }

    fn static_name() -> &'static str
    where
        Self: Sized,
    {
        "PlaceholderExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn check_invariants(&self, check: InvariantLevel) -> Result<()> {
        check_default_invariants(self, check)
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![]
    }

    /// A leaf: no children, so no distribution requirements.
    fn input_distribution_requirements(&self) -> InputDistributionRequirements {
        InputDistributionRequirements::new(vec![])
    }

    fn dynamic_expressions_produced(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        Vec::new()
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        vec![]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![]
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
        vec![]
    }

    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        _options: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.with_no_children(&children)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.with_no_children(&children)
    }

    fn with_new_children_and_same_properties(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.with_no_children(&children)
    }

    fn reset_state(self: Arc<Self>) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn repartitioned(
        &self,
        _target_partitions: usize,
        _config: &ConfigOptions,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        Ok(None)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // Return an empty stream - this should never be called in practice
        // as the placeholder should be replaced before execution
        Ok(Box::pin(EmptyRecordBatchStream::new(Arc::clone(
            &self.schema,
        ))))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    fn partition_statistics(&self, _partition: Option<usize>) -> Result<Arc<Statistics>> {
        Ok(Arc::new(Statistics::new_unknown(&self.schema)))
    }

    /// A leaf: there are no children whose statistics to request.
    fn child_stats_requests(&self, _partition: Option<usize>) -> Vec<ChildStats> {
        Vec::new()
    }

    fn statistics_from_inputs(
        &self,
        _input_stats: &[Arc<Statistics>],
        _args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        Ok(Arc::new(Statistics::new_unknown(&self.schema)))
    }

    fn supports_limit_pushdown(&self) -> bool {
        false
    }

    fn with_fetch(&self, _limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        None
    }

    fn fetch(&self) -> Option<usize> {
        None
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }

    fn try_swapping_with_projection(
        &self,
        _projection: &ProjectionExec,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        Ok(None)
    }

    fn gather_filters_for_pushdown(
        &self,
        _phase: FilterPushdownPhase,
        parent_filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> Result<FilterDescription> {
        Ok(FilterDescription::all_unsupported(
            &parent_filters,
            &self.children(),
        ))
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

    fn try_pushdown_sort(
        &self,
        _order: &[PhysicalSortExpr],
    ) -> Result<SortOrderPushdownResult<Arc<dyn ExecutionPlan>>> {
        Ok(SortOrderPushdownResult::Unsupported)
    }

    /// Not serializable: a deserialization stand-in that must be replaced before execution.
    fn try_to_proto(
        &self,
        _ctx: &datafusion::physical_plan::proto::ExecutionPlanEncodeCtx<'_>,
    ) -> Result<Option<datafusion_proto::protobuf::PhysicalPlanNode>> {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::physical_plan::empty::EmptyExec;
    use runtime_proto::ListUdfsArgs;
    use runtime_proto::udtf_args::Args;

    fn test_udtf_args() -> UdtfArgs {
        UdtfArgs {
            args: Some(Args::ListUdfs(ListUdfsArgs {})),
        }
    }

    fn test_inner() -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        Arc::new(EmptyExec::new(schema))
    }

    /// Regression test for issue #10951.
    ///
    /// `check_default_invariants` (invoked by optimizer rules like `EnforceSorting`
    /// when `UdtfExec` is composed under another plan, e.g. `rrf(text_search(...),
    /// vector_search(...))`) asserts that `maintains_input_order`,
    /// `required_input_ordering`, `required_input_distribution`, and
    /// `benefits_from_input_partitioning` each return a Vec with one entry per
    /// child. `UdtfExec` reports one child (the inner plan), so each of these
    /// must return a single-element Vec.
    #[test]
    fn invariant_vec_lengths_match_children_count() {
        let exec = UdtfExec::new(test_udtf_args(), test_inner());
        let children_len = exec.children().len();
        assert_eq!(children_len, 1);
        assert_eq!(exec.maintains_input_order().len(), children_len);
        assert_eq!(exec.required_input_ordering().len(), children_len);
        assert_eq!(
            exec.input_distribution_requirements()
                .into_per_child()
                .len(),
            children_len
        );
        assert_eq!(exec.benefits_from_input_partitioning().len(), children_len);
    }

    #[test]
    fn check_default_invariants_passes() {
        let exec = UdtfExec::new(test_udtf_args(), test_inner());
        exec.check_invariants(InvariantLevel::Always)
            .expect("default invariants should pass for UdtfExec");
        exec.check_invariants(InvariantLevel::Executable)
            .expect("default invariants should pass for UdtfExec");
    }

    /// `UdtfExec` executes its inner plan, so it must report the inner plan's
    /// statistics through `StatisticsContext`, the path `DataFusion` 55's optimizer uses.
    #[test]
    fn statistics_are_the_inner_plans() {
        use datafusion::arrow::array::Int32Array;
        use datafusion::arrow::record_batch::RecordBatch;
        use datafusion::common::stats::Precision;
        use datafusion::datasource::memory::MemorySourceConfig;

        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .expect("valid batch");
        let inner = MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None)
            .expect("valid memory exec");
        let exec = UdtfExec::new(test_udtf_args(), inner);

        let stats = StatisticsContext::new()
            .compute(&exec, &StatisticsArgs::new())
            .expect("statistics");
        assert_eq!(stats.num_rows, Precision::Exact(3));
    }
}
