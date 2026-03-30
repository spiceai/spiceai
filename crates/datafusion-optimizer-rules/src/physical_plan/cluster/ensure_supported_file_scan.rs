/*
Copyright 2025-2026 The Spice.ai OSS Authors

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

use crate::concrete;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion, TreeNodeVisitor};
use datafusion::common::{Result, plan_err};
use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_datasource::file_scan_config::FileScanConfig;
use datafusion_datasource::memory::MemorySourceConfig;
use datafusion_datasource::source::DataSourceExec;
use std::sync::Arc;

/// Name of the `UdtfExec` execution plan.
///
/// We use name-based detection to avoid circular dependencies between
/// `datafusion-optimizer-rules` and `runtime` crates.
const UDTF_EXEC_NAME: &str = "UdtfExec";

/// An optimizer to sanity check `DataSourceExec` encapsulate the kinds of plans
/// we can distribute.
///
/// This optimizer validates that all `DataSourceExec` nodes in the plan use
/// file-based or remote data sources that can be serialized for distributed
/// execution. In-memory sources (`MemorySourceConfig`) cannot be distributed
/// unless they are part of a UDTF execution (wrapped in `UdtfExec`), which
/// will be reconstructed on remote executors.
#[derive(Debug, Clone)]
pub struct EnsureSupportedFileScan {}

impl EnsureSupportedFileScan {
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(EnsureSupportedFileScan {})
    }

    fn name() -> &'static str {
        "EnsureSerializableFileScanOptimizer"
    }

    fn validate(plan: &Arc<dyn ExecutionPlan>) -> Result<()> {
        let Some(data_source_exec) = concrete!(plan, DataSourceExec) else {
            return plan_err!(
                "{} only operates on DataSourceExec. This is a bug.",
                Self::name()
            );
        };

        if concrete!(data_source_exec.data_source(), MemorySourceConfig).is_some() {
            return plan_err!(
                "{}: DataSourceExec with MemorySourceConfig cannot be distributed. Use file-based or remote data sources instead.",
                Self::name()
            );
        }

        if concrete!(data_source_exec.data_source(), FileScanConfig).is_none() {
            return plan_err!(
                "{}: does not support {} scans",
                Self::name(),
                std::any::type_name_of_val(data_source_exec.data_source().as_ref())
            );
        }

        Ok(())
    }
}

impl PhysicalOptimizerRule for EnsureSupportedFileScan {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Use a custom visitor that skips children of UdtfExec nodes
        let mut visitor = DataSourceExecValidator::default();
        plan.visit(&mut visitor)?;
        visitor.result?;

        Ok(plan)
    }

    fn name(&self) -> &str {
        Self::name()
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// A visitor that collects `DataSourceExec` nodes for validation, skipping
/// those that are children of `UdtfExec` nodes.
struct DataSourceExecValidator {
    /// Tracks how deep we are inside `UdtfExec` nodes (can be nested).
    udtf_depth: usize,
    /// Accumulated validation result.
    result: Result<()>,
}

impl Default for DataSourceExecValidator {
    fn default() -> Self {
        Self {
            udtf_depth: 0,
            result: Ok(()),
        }
    }
}

impl TreeNodeVisitor<'_> for DataSourceExecValidator {
    type Node = Arc<dyn ExecutionPlan>;

    fn f_down(&mut self, node: &Self::Node) -> Result<TreeNodeRecursion> {
        // Check if we're entering a UdtfExec
        if node.name() == UDTF_EXEC_NAME {
            self.udtf_depth += 1;
        }

        // Only validate DataSourceExec nodes that are NOT inside a UdtfExec
        if self.udtf_depth == 0
            && concrete!(node, DataSourceExec).is_some()
            && let Err(e) = EnsureSupportedFileScan::validate(node)
        {
            self.result = Err(e);
            return Ok(TreeNodeRecursion::Stop);
        }

        Ok(TreeNodeRecursion::Continue)
    }

    fn f_up(&mut self, node: &Self::Node) -> Result<TreeNodeRecursion> {
        // Check if we're leaving a UdtfExec
        if node.name() == UDTF_EXEC_NAME {
            self.udtf_depth = self.udtf_depth.saturating_sub(1);
        }

        Ok(TreeNodeRecursion::Continue)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::common::Statistics;
    use datafusion::config::ConfigOptions;
    use datafusion::execution::{SendableRecordBatchStream, TaskContext};
    use datafusion::physical_expr::{EquivalenceProperties, OrderingRequirements};
    use datafusion::physical_plan::execution_plan::InvariantLevel;
    use datafusion::physical_plan::metrics::MetricsSet;
    use datafusion::physical_plan::projection::ProjectionExec;
    use datafusion::physical_plan::{
        DisplayAs, DisplayFormatType, Distribution, ExecutionPlanProperties, PlanProperties,
    };
    use std::any::Any;
    use std::fmt;

    /// A mock execution plan that pretends to be `UdtfExec` for testing purposes.
    #[derive(Debug)]
    struct MockUdtfExec {
        inner: Arc<dyn ExecutionPlan>,
        properties: PlanProperties,
    }

    impl MockUdtfExec {
        fn new(inner: Arc<dyn ExecutionPlan>) -> Self {
            let schema = inner.schema();
            let eq_properties = EquivalenceProperties::new(schema);
            let properties = PlanProperties::new(
                eq_properties,
                inner.output_partitioning().clone(),
                inner.pipeline_behavior(),
                inner.boundedness(),
            );
            Self { inner, properties }
        }
    }

    impl DisplayAs for MockUdtfExec {
        fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "UdtfExec")
        }
    }

    impl ExecutionPlan for MockUdtfExec {
        fn name(&self) -> &'static str {
            "UdtfExec" // Matches UDTF_EXEC_NAME
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

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![&self.inner]
        }

        fn with_new_children(
            self: Arc<Self>,
            children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            Ok(Arc::new(MockUdtfExec::new(Arc::clone(&children[0]))))
        }

        fn execute(
            &self,
            partition: usize,
            context: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            self.inner.execute(partition, context)
        }

        fn statistics(&self) -> Result<Statistics> {
            #[expect(deprecated)]
            self.inner.statistics()
        }

        fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
            self.inner.partition_statistics(partition)
        }

        fn reset_state(self: Arc<Self>) -> Result<Arc<dyn ExecutionPlan>> {
            Ok(self)
        }

        fn check_invariants(&self, level: InvariantLevel) -> Result<()> {
            datafusion::physical_plan::execution_plan::check_default_invariants(self, level)
        }

        fn schema(&self) -> SchemaRef {
            self.inner.schema()
        }

        fn required_input_distribution(&self) -> Vec<Distribution> {
            vec![]
        }

        fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
            vec![]
        }

        fn maintains_input_order(&self) -> Vec<bool> {
            vec![true]
        }

        fn benefits_from_input_partitioning(&self) -> Vec<bool> {
            vec![false]
        }

        fn repartitioned(
            &self,
            _target_partitions: usize,
            _config: &ConfigOptions,
        ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
            Ok(None)
        }

        fn metrics(&self) -> Option<MetricsSet> {
            None
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

        fn cardinality_effect(
            &self,
        ) -> datafusion::physical_plan::execution_plan::CardinalityEffect {
            datafusion::physical_plan::execution_plan::CardinalityEffect::Equal
        }

        fn try_swapping_with_projection(
            &self,
            _projection: &ProjectionExec,
        ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
            Ok(None)
        }
    }

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]))
    }

    #[test]
    fn test_memory_source_fails_without_udtf_wrapper() {
        let schema = test_schema();
        let batch = RecordBatch::new_empty(Arc::clone(&schema));
        let memory_source =
            MemorySourceConfig::try_new(&[vec![batch]], schema, None).expect("memory source");
        let data_source_exec = Arc::new(DataSourceExec::new(Arc::new(memory_source)));

        let optimizer = EnsureSupportedFileScan::new();
        let config = ConfigOptions::default();

        let result = optimizer.optimize(data_source_exec, &config);
        assert!(result.is_err());
        assert!(
            result
                .expect_err("expected error for MemorySourceConfig")
                .to_string()
                .contains("MemorySourceConfig cannot be distributed")
        );
    }

    #[test]
    fn test_memory_source_succeeds_with_udtf_wrapper() {
        let schema = test_schema();
        let batch = RecordBatch::new_empty(Arc::clone(&schema));
        let memory_source =
            MemorySourceConfig::try_new(&[vec![batch]], schema, None).expect("memory source");
        let data_source_exec = Arc::new(DataSourceExec::new(Arc::new(memory_source)));

        // Wrap in MockUdtfExec
        let udtf_exec = Arc::new(MockUdtfExec::new(data_source_exec));

        let optimizer = EnsureSupportedFileScan::new();
        let config = ConfigOptions::default();

        let result = optimizer.optimize(udtf_exec, &config);
        assert!(result.is_ok(), "Expected success when wrapped in UdtfExec");
    }
}
