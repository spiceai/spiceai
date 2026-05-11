/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

//! Display-only `DataSource` wrapper that surfaces the pushed-down `limit` in
//! the `tree` `EXPLAIN` format.
//!
//! Upstream DataFusion's `FileScanConfig::fmt_as` includes `limit=N` for the
//! `Default`/`Verbose` formats but omits it for `TreeRender`, so a `DataSourceExec`
//! that received a fetch limit looks identical to one without when rendered as a
//! tree. [`LimitDisplayDataSource`] wraps an existing `DataSource`, delegates every
//! behavior to the inner source, and appends `limit=N` to the tree output when
//! the inner source's `fetch()` is set.
//!
//! Wrapping is applied by [`DataSourceTreeDisplayOptimizer`], a physical
//! optimizer rule that runs after limit pushdown. The wrapper makes
//! [`DataSource::as_any`] return the inner source so existing downcasts (e.g. to
//! `FileScanConfig`) keep working transparently.

use std::any::Any;
use std::fmt::{self, Formatter};
use std::sync::Arc;

use datafusion::common::Result;
use datafusion::common::Statistics;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, LexOrdering};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::SortOrderPushdownResult;
use datafusion::physical_plan::execution_plan::SchedulingType;
use datafusion::physical_plan::expressions::PhysicalSortExpr;
use datafusion::physical_plan::filter_pushdown::FilterPushdownPropagation;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::projection::ProjectionExprs;
use datafusion::physical_plan::{DisplayFormatType, ExecutionPlan, Partitioning, PhysicalExpr};
use datafusion_datasource::source::{DataSource, DataSourceExec};

/// Transparent `DataSource` wrapper that adds `limit=N` to the `TreeRender`
/// output when the inner source reports a fetch limit.
#[derive(Debug)]
pub struct LimitDisplayDataSource {
    inner: Arc<dyn DataSource>,
}

impl LimitDisplayDataSource {
    #[must_use]
    pub fn new(inner: Arc<dyn DataSource>) -> Self {
        Self { inner }
    }

    fn wrap(inner: Arc<dyn DataSource>) -> Arc<dyn DataSource> {
        Arc::new(Self::new(inner)) as Arc<dyn DataSource>
    }
}

impl DataSource for LimitDisplayDataSource {
    fn open(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.inner.open(partition, context)
    }

    // Pass through so downstream downcasts (e.g. to `FileScanConfig`) keep working
    // even though this wrapper sits between `DataSourceExec` and the real source.
    fn as_any(&self) -> &dyn Any {
        self.inner.as_any()
    }

    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        self.inner.fmt_as(t, f)?;
        if matches!(t, DisplayFormatType::TreeRender)
            && let Some(limit) = self.inner.fetch()
        {
            writeln!(f, "limit={limit}")?;
        }
        Ok(())
    }

    fn repartitioned(
        &self,
        target_partitions: usize,
        repartition_file_min_size: usize,
        output_ordering: Option<LexOrdering>,
    ) -> Result<Option<Arc<dyn DataSource>>> {
        Ok(self
            .inner
            .repartitioned(target_partitions, repartition_file_min_size, output_ordering)?
            .map(Self::wrap))
    }

    fn output_partitioning(&self) -> Partitioning {
        self.inner.output_partitioning()
    }

    fn eq_properties(&self) -> EquivalenceProperties {
        self.inner.eq_properties()
    }

    fn scheduling_type(&self) -> SchedulingType {
        self.inner.scheduling_type()
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        self.inner.partition_statistics(partition)
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn DataSource>> {
        self.inner.with_fetch(limit).map(Self::wrap)
    }

    fn fetch(&self) -> Option<usize> {
        self.inner.fetch()
    }

    fn metrics(&self) -> ExecutionPlanMetricsSet {
        self.inner.metrics()
    }

    fn try_swapping_with_projection(
        &self,
        projection: &ProjectionExprs,
    ) -> Result<Option<Arc<dyn DataSource>>> {
        Ok(self
            .inner
            .try_swapping_with_projection(projection)?
            .map(Self::wrap))
    }

    fn try_pushdown_filters(
        &self,
        filters: Vec<Arc<dyn PhysicalExpr>>,
        config: &ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn DataSource>>> {
        let mut propagation = self.inner.try_pushdown_filters(filters, config)?;
        if let Some(updated) = propagation.updated_node.take() {
            propagation.updated_node = Some(Self::wrap(updated));
        }
        Ok(propagation)
    }

    fn try_pushdown_sort(
        &self,
        order: &[PhysicalSortExpr],
    ) -> Result<SortOrderPushdownResult<Arc<dyn DataSource>>> {
        Ok(self.inner.try_pushdown_sort(order)?.map(Self::wrap))
    }
}

/// Physical optimizer rule that wraps each `DataSourceExec`'s `DataSource` with
/// [`LimitDisplayDataSource`] when a fetch limit has been pushed down. The wrap
/// is purely cosmetic — it does not alter execution, statistics, or plan
/// properties — and must run after all DataFusion rules that mutate the data
/// source (in particular, limit pushdown).
#[derive(Debug, Default)]
pub struct DataSourceTreeDisplayOptimizer;

impl DataSourceTreeDisplayOptimizer {
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl PhysicalOptimizerRule for DataSourceTreeDisplayOptimizer {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> std::result::Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        plan.transform_down(|plan| {
            let Some(data_source_exec) = plan.as_any().downcast_ref::<DataSourceExec>() else {
                return Ok(Transformed::no(plan));
            };

            // Only wrap when there is a fetch limit to surface, so plans without
            // limits stay identical to upstream DataFusion.
            if data_source_exec.data_source().fetch().is_none() {
                return Ok(Transformed::no(plan));
            }

            let wrapped = LimitDisplayDataSource::wrap(Arc::clone(data_source_exec.data_source()));
            let new_exec = data_source_exec.clone().with_data_source(wrapped);
            Ok(Transformed::yes(Arc::new(new_exec)))
        })
        .data()
    }

    fn name(&self) -> &str {
        "DataSourceTreeDisplayOptimizer"
    }

    fn schema_check(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use arrow::record_batch::RecordBatch;
    use datafusion::common::Result;
    use datafusion::config::ConfigOptions;
    use datafusion::physical_optimizer::PhysicalOptimizerRule;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::physical_plan::displayable;
    use datafusion_datasource::memory::MemorySourceConfig;
    use datafusion_datasource::source::{DataSource, DataSourceExec};

    use super::DataSourceTreeDisplayOptimizer;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
    }

    fn test_batch(schema: &SchemaRef) -> Result<RecordBatch> {
        Ok(RecordBatch::try_new(
            Arc::clone(schema),
            vec![Arc::new(Int64Array::from_iter_values(0i64..10))],
        )?)
    }

    fn data_source_exec_with_fetch(fetch: Option<usize>) -> Result<Arc<dyn ExecutionPlan>> {
        let schema = test_schema();
        let batch = test_batch(&schema)?;
        let source: Arc<dyn DataSource> =
            Arc::new(MemorySourceConfig::try_new(&[vec![batch]], schema, None)?);
        let source = if let Some(limit) = fetch {
            source
                .with_fetch(Some(limit))
                .expect("MemorySourceConfig supports fetch")
        } else {
            source
        };
        Ok(Arc::new(DataSourceExec::new(source)))
    }

    #[test]
    fn tree_render_shows_limit_when_fetch_set() -> Result<()> {
        let plan = data_source_exec_with_fetch(Some(2))?;

        let baseline = displayable(plan.as_ref()).tree_render().to_string();
        assert!(
            !baseline.contains("limit"),
            "baseline tree should not yet contain limit, got:\n{baseline}"
        );

        let optimizer = DataSourceTreeDisplayOptimizer::new();
        let optimized = optimizer.optimize(Arc::clone(&plan), &ConfigOptions::new())?;

        let rendered = displayable(optimized.as_ref()).tree_render().to_string();
        assert!(
            rendered.contains("limit"),
            "expected wrapped tree to contain `limit`, got:\n{rendered}"
        );

        Ok(())
    }

    #[test]
    fn tree_render_unchanged_without_fetch() -> Result<()> {
        let plan = data_source_exec_with_fetch(None)?;

        let before = displayable(plan.as_ref()).tree_render().to_string();
        let optimizer = DataSourceTreeDisplayOptimizer::new();
        let optimized = optimizer.optimize(Arc::clone(&plan), &ConfigOptions::new())?;
        let after = displayable(optimized.as_ref()).tree_render().to_string();

        assert_eq!(before, after);
        Ok(())
    }

    #[test]
    fn name_preserved_after_wrapping() -> Result<()> {
        // The wrapper has to keep the `DataSourceExec` name so existing
        // snapshot tests and EXPLAIN output stay stable; only the tree
        // contents gain a `limit` line.
        let plan = data_source_exec_with_fetch(Some(5))?;

        let optimizer = DataSourceTreeDisplayOptimizer::new();
        let optimized = optimizer.optimize(plan, &ConfigOptions::new())?;

        let rendered = displayable(optimized.as_ref()).tree_render().to_string();
        assert!(
            rendered.contains("DataSourceExec"),
            "tree output should retain `DataSourceExec` node label, got:\n{rendered}"
        );
        Ok(())
    }
}
