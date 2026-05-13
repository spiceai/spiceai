/*
Copyright 2025 The Spice.ai OSS Authors

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

use std::{any::Any, sync::Arc};

use arrow_schema::SchemaRef;
use datafusion::config::ConfigOptions;
use datafusion::error::Result;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_common::{DataFusionError, Statistics};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_expr::{Distribution, OrderingRequirements, PhysicalExpr};
use futures::TryStreamExt;

use datafusion_physical_expr::Partitioning;
use datafusion_physical_plan::{
    DisplayAs, ExecutionPlan, PlanProperties, SortOrderPushdownResult,
    execution_plan::{CardinalityEffect, InvariantLevel, check_default_invariants},
    expressions::PhysicalSortExpr,
    filter_pushdown::{
        ChildPushdownResult, FilterDescription, FilterPushdownPhase, FilterPushdownPropagation,
    },
    metrics::MetricsSet,
    projection::ProjectionExec,
    repartition::RepartitionExec,
};

/// Wrapper for Cayenne acceleration execution plans.
/// This is used to identify Cayenne-specific table scans from within the physical plan, once references to the table is lost from the logical plan.
#[derive(Debug)]
pub struct CayenneAccelerationExec {
    inner: Arc<dyn ExecutionPlan>,
}

impl CayenneAccelerationExec {
    /// Creates a new `CayenneAccelerationExec` wrapping the given execution plan.
    #[must_use]
    pub fn new(inner: Arc<dyn ExecutionPlan>) -> Self {
        Self { inner }
    }
}

pub(crate) fn round_robin_repartition_if_needed(
    plan: Arc<dyn ExecutionPlan>,
    target_partitions: usize,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    let current_partitions = plan.properties().output_partitioning().partition_count();
    if target_partitions <= 1
        || current_partitions >= target_partitions
        || plan.properties().output_ordering().is_some()
    {
        return Ok(None);
    }

    Ok(Some(Arc::new(RepartitionExec::try_new(
        plan,
        Partitioning::RoundRobinBatch(target_partitions),
    )?)))
}

impl DisplayAs for CayenneAccelerationExec {
    fn fmt_as(
        &self,
        _t: datafusion_physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "CayenneAccelerationExec")
    }
}

#[deny(clippy::missing_trait_methods)]
impl ExecutionPlan for CayenneAccelerationExec {
    fn name(&self) -> &'static str {
        "CayenneAccelerationExec"
    }

    fn static_name() -> &'static str
    where
        Self: Sized,
    {
        "CayenneAccelerationExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(self.properties().eq_properties.schema())
    }

    fn properties(&self) -> &PlanProperties {
        self.inner.properties()
    }

    fn check_invariants(&self, check: InvariantLevel) -> Result<()> {
        check_default_invariants(self, check)
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::UnspecifiedDistribution; self.children().len()]
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        vec![None; self.children().len()]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true; self.children().len()]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![self.inner.properties().output_ordering().is_none()]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.inner]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::External(
                super::Error::InvalidChildrenCount {
                    children_count: children.len(),
                }
                .into(),
            ));
        }

        let Some(input) = children.into_iter().next() else {
            unreachable!("should have one input");
        };
        Ok(Arc::new(CayenneAccelerationExec::new(input)))
    }

    fn reset_state(self: Arc<Self>) -> Result<Arc<dyn ExecutionPlan>> {
        let children = self.children().into_iter().cloned().collect();
        self.with_new_children(children)
    }

    fn repartitioned(
        &self,
        target_partitions: usize,
        config: &ConfigOptions,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let repartitioned = self.inner.repartitioned(target_partitions, config)?;
        Ok(repartitioned
            .map(|plan| Arc::new(CayenneAccelerationExec::new(plan)) as Arc<dyn ExecutionPlan>))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        let stream = self.inner.execute(partition, context)?;
        let schema = stream.schema();
        let mapped = stream.map_err(|e| {
            let msg = e.to_string();
            if msg.contains("Too many open files") {
                // Extract the file path from messages like "Unable to open file /path/to/file.vortex: Too many ..."
                let file_path = msg
                    .find("Unable to open file ")
                    .and_then(|start| {
                        let after = &msg[start + "Unable to open file ".len()..];
                        after.find(':').map(|end| &after[..end])
                    })
                    .unwrap_or("unknown");
                DataFusionError::External(Box::new(super::Error::TooManyOpenFiles {
                    file_path: file_path.to_string(),
                }))
            } else {
                e
            }
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, mapped)))
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

    // Allow optimizer to push limits through to inputs
    fn supports_limit_pushdown(&self) -> bool {
        self.inner.supports_limit_pushdown()
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        self.inner
            .with_fetch(limit)
            .map(|plan| Arc::new(CayenneAccelerationExec::new(plan)) as Arc<dyn ExecutionPlan>)
    }

    fn fetch(&self) -> Option<usize> {
        self.inner.fetch()
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
        FilterDescription::from_children(parent_filters, &self.children())
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
        order: &[PhysicalSortExpr],
    ) -> Result<SortOrderPushdownResult<Arc<dyn ExecutionPlan>>> {
        let result = self.inner.try_pushdown_sort(order)?;
        Ok(result
            .map(|plan| Arc::new(CayenneAccelerationExec::new(plan)) as Arc<dyn ExecutionPlan>))
    }
}

pub(crate) trait IsCayenneAccelerationExec {
    /// Returns true if the execution plan is a `CayenneAccelerationExec`
    fn is_cayenne_acceleration_exec(&self) -> bool;
}

impl IsCayenneAccelerationExec for Arc<dyn ExecutionPlan> {
    fn is_cayenne_acceleration_exec(&self) -> bool {
        self.as_any()
            .downcast_ref::<CayenneAccelerationExec>()
            .is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::record_batch::RecordBatch;
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::datasource::memory::MemorySourceConfig;

    fn one_partition_plan() -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3]))],
        )
        .expect("test batch should be valid");

        MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None)
            .expect("memory exec should be created")
    }

    #[test]
    fn repartitions_unordered_plan_to_target_partitions() {
        let plan = one_partition_plan();
        let repartitioned_plan = round_robin_repartition_if_needed(plan, 4)
            .expect("repartition check should succeed")
            .expect("plan should be repartitioned");

        assert_eq!(
            repartitioned_plan
                .properties()
                .output_partitioning()
                .partition_count(),
            4
        );
        assert!(
            repartitioned_plan
                .as_any()
                .downcast_ref::<RepartitionExec>()
                .is_some()
        );
    }

    #[test]
    fn cayenne_exec_benefits_from_unordered_input_partitioning() {
        let exec = CayenneAccelerationExec::new(one_partition_plan());

        assert_eq!(exec.benefits_from_input_partitioning(), vec![true]);
    }
}
