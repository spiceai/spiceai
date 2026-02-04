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
    DisplayAs, DisplayFormatType, Distribution, EmptyRecordBatchStream, ExecutionPlan,
    ExecutionPlanProperties, Partitioning, PhysicalExpr, PlanProperties,
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
    properties: PlanProperties,
}

impl UdtfExec {
    /// Creates a new `UdtfExec` wrapping the given inner plan with UDTF arguments.
    #[must_use]
    pub fn new(args: UdtfArgs, inner: Arc<dyn ExecutionPlan>) -> Self {
        let schema = inner.schema();
        let eq_properties = EquivalenceProperties::new(schema);
        let emission_type = inner.pipeline_behavior();
        let boundedness = inner.boundedness();
        let properties = PlanProperties::new(
            eq_properties,
            inner.output_partitioning().clone(),
            emission_type,
            boundedness,
        );

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
        let properties = PlanProperties::new(
            eq_properties,
            Partitioning::UnknownPartitioning(1),
            datafusion::physical_plan::execution_plan::EmissionType::Final,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        );

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
    fn name(&self) -> &'static str {
        "UdtfExec"
    }

    fn static_name() -> &'static str
    where
        Self: Sized,
    {
        "UdtfExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn check_invariants(&self, check: InvariantLevel) -> Result<()> {
        check_default_invariants(self, check)
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![]
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

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        // Return inner as a child so optimizers can traverse it
        vec![&self.inner]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() == 1 {
            Ok(Arc::new(Self::new(
                self.args.clone(),
                Arc::clone(&children[0]),
            )))
        } else {
            Err(DataFusionError::Execution(
                "UdtfExec expects exactly one child".to_string(),
            ))
        }
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

    fn statistics(&self) -> Result<Statistics> {
        #[expect(deprecated)]
        self.inner.statistics()
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        self.inner.partition_statistics(partition)
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
}

/// A placeholder execution plan used during deserialization.
///
/// This plan should never actually be executed - it exists only as a placeholder
/// until the real inner plan is reconstructed by re-invoking the UDTF.
#[derive(Debug)]
struct PlaceholderExec {
    schema: SchemaRef,
    properties: PlanProperties,
}

impl PlaceholderExec {
    fn new(schema: SchemaRef) -> Self {
        let eq_properties = EquivalenceProperties::new(Arc::clone(&schema));
        let properties = PlanProperties::new(
            eq_properties,
            Partitioning::UnknownPartitioning(1),
            datafusion::physical_plan::execution_plan::EmissionType::Final,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        );
        Self { schema, properties }
    }
}

impl DisplayAs for PlaceholderExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "PlaceholderExec")
    }
}

#[deny(clippy::missing_trait_methods)]
impl ExecutionPlan for PlaceholderExec {
    fn name(&self) -> &'static str {
        "PlaceholderExec"
    }

    fn static_name() -> &'static str
    where
        Self: Sized,
    {
        "PlaceholderExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
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

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        vec![]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Execution(
                "PlaceholderExec expects no children".to_string(),
            ))
        }
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

    fn statistics(&self) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&self.schema))
    }

    fn partition_statistics(&self, _partition: Option<usize>) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&self.schema))
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
}
