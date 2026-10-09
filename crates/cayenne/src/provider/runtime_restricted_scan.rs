/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! A file scan whose file list is narrowed when it starts, to the files a
//! hash join's completed dynamic filter can match.
//!
//! The probe side learns its keys after the physical plan is built. This node
//! waits for the completed dynamic filter, resolves its index selection, and
//! removes covered files that cannot contain candidates before constructing
//! the executable scan. Uncovered files remain in the scan. Every execution
//! partition shares the same narrowed plan and its file queue; partition
//! count and ordering remain those advertised by the original plan.

use std::fmt;
use std::sync::Arc;

use datafusion::config::ConfigOptions;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::SortOrderPushdownResult;
use datafusion::physical_plan::execution_plan::CardinalityEffect;
use datafusion::physical_plan::expressions::PhysicalSortExpr;
use datafusion::physical_plan::filter_pushdown::{
    ChildPushdownResult, FilterDescription, FilterPushdownPhase, FilterPushdownPropagation,
};
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    ChildStats, ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan,
    PlanProperties, ReplaceChildrenOptions, SendableRecordBatchStream, StatisticsArgs,
    StatisticsContext,
};
use datafusion_common::{Result, Statistics, tree_node::TreeNodeRecursion};
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
use datafusion_datasource::source::DataSourceExec;
use datafusion_physical_expr::PhysicalExpr;
use futures::{StreamExt, TryStreamExt};
use parking_lot::Mutex;

use super::lookup_index::{DynamicLookupAccessPlanProvider, RuntimeLookupSelection};

struct RestrictedScan {
    selection: Arc<RuntimeLookupSelection>,
    plan: Arc<dyn ExecutionPlan>,
}

/// See the module documentation.
pub(crate) struct RuntimeRestrictedScanExec {
    input: Arc<dyn ExecutionPlan>,
    provider: Arc<DynamicLookupAccessPlanProvider>,
    /// The narrowed scan, built once per resolved selection and shared by
    /// every partition.
    restricted: Arc<Mutex<Option<RestrictedScan>>>,
}

impl RuntimeRestrictedScanExec {
    pub(crate) fn new(
        input: Arc<dyn ExecutionPlan>,
        provider: Arc<DynamicLookupAccessPlanProvider>,
    ) -> Self {
        Self {
            input,
            provider,
            restricted: Arc::default(),
        }
    }

    fn with_child(&self, children: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> {
        let [input]: [Arc<dyn ExecutionPlan>; 1] = children.try_into().map_err(|_| {
            datafusion_common::DataFusionError::Internal(
                "RuntimeRestrictedScanExec needs one child".to_string(),
            )
        })?;
        Ok(Arc::new(Self::new(input, Arc::clone(&self.provider))))
    }

    /// The scan to run: `input` narrowed to the files the selection may
    /// match, or `input` itself when the index cannot answer the filter.
    async fn scan(
        input: Arc<dyn ExecutionPlan>,
        provider: Arc<DynamicLookupAccessPlanProvider>,
        restricted: Arc<Mutex<Option<RestrictedScan>>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let Some(config) = file_scan(&input) else {
            return Ok(input);
        };
        let predicate: Option<Arc<dyn PhysicalExpr>> = config.file_source().filter();
        let Some(selection) = provider.selection(predicate.as_ref()).await else {
            return Ok(input);
        };
        // Every partition must run the same narrowed scan: its partitions
        // share one queue of files, so a second scan built for another
        // partition would read the same files again.
        let mut restricted = restricted.lock();
        match restricted.as_ref() {
            Some(cached) if Arc::ptr_eq(&cached.selection, &selection) => {
                Ok(Arc::clone(&cached.plan))
            }
            _ => {
                let plan = narrowed(&input, &selection)?;
                *restricted = Some(RestrictedScan {
                    selection,
                    plan: Arc::clone(&plan),
                });
                Ok(plan)
            }
        }
    }
}

/// The file scan at the bottom of `plan`'s chain of single-child nodes (the
/// runtime wraps a scan, for one, to count the bytes it reads).
fn file_scan(plan: &Arc<dyn ExecutionPlan>) -> Option<&FileScanConfig> {
    if let Some(scan) = plan.downcast_ref::<DataSourceExec>() {
        return scan.data_source().downcast_ref::<FileScanConfig>();
    }
    match plan.children().as_slice() {
        [child] => file_scan(child),
        _ => None,
    }
}

/// `plan` with the file scan at the bottom of its chain narrowed to the files
/// `selection` may match, every node above it rebuilt over the narrowed one.
fn narrowed(
    plan: &Arc<dyn ExecutionPlan>,
    selection: &RuntimeLookupSelection,
) -> Result<Arc<dyn ExecutionPlan>> {
    if let Some(config) = plan
        .downcast_ref::<DataSourceExec>()
        .and_then(|scan| scan.data_source().downcast_ref::<FileScanConfig>())
    {
        let groups: Vec<FileGroup> = config
            .file_groups
            .iter()
            .map(|group| {
                FileGroup::new(
                    group
                        .iter()
                        .filter(|file| selection.may_hold(file.object_meta.location.as_ref()))
                        .cloned()
                        .collect(),
                )
            })
            .collect();
        return Ok(DataSourceExec::from_data_source(
            FileScanConfigBuilder::from(config.clone())
                .with_file_groups(groups)
                .build(),
        ));
    }
    let child = match plan.children().as_slice() {
        [child] => narrowed(child, selection)?,
        _ => return Ok(Arc::clone(plan)),
    };
    Arc::clone(plan).replace_children(
        vec![child],
        ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
    )
}

impl fmt::Debug for RuntimeRestrictedScanExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RuntimeRestrictedScanExec")
            .finish_non_exhaustive()
    }
}

impl DisplayAs for RuntimeRestrictedScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "RuntimeRestrictedScanExec")
    }
}

impl ExecutionPlan for RuntimeRestrictedScanExec {
    fn name(&self) -> &'static str {
        "RuntimeRestrictedScanExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        // The predicate belongs to the child scan, which is visited separately.
        Ok(TreeNodeRecursion::Continue)
    }

    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        _options: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Properties are read directly from the child in both replacement modes.
        self.with_child(children)
    }

    fn with_new_children_and_same_properties(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.with_child(children)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.with_child(children)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let schema = self.schema();
        let input = Arc::clone(&self.input);
        let provider = Arc::clone(&self.provider);
        let restricted = Arc::clone(&self.restricted);
        let stream = futures::stream::once(async move {
            let plan = Self::scan(input, provider, restricted).await?;
            plan.execute(partition, context)
        })
        .try_flatten()
        .boxed();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        match self.restricted.lock().as_ref() {
            Some(cached) => cached.plan.metrics(),
            None => self.input.metrics(),
        }
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>> {
        StatisticsContext::new().compute(self, &StatisticsArgs::new().with_partition(partition))
    }

    fn child_stats_requests(&self, partition: Option<usize>) -> Vec<ChildStats> {
        vec![ChildStats::At(partition)]
    }

    fn statistics_from_inputs(
        &self,
        input_stats: &[Arc<Statistics>],
        _args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        let [child_stats] = input_stats else {
            return Err(datafusion_common::DataFusionError::Internal(
                "RuntimeRestrictedScanExec needs one child's statistics".to_string(),
            ));
        };
        Ok(Arc::clone(child_stats))
    }

    fn supports_limit_pushdown(&self) -> bool {
        self.input.supports_limit_pushdown()
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        self.input.with_fetch(limit).map(|input| {
            Arc::new(Self::new(input, Arc::clone(&self.provider))) as Arc<dyn ExecutionPlan>
        })
    }

    fn fetch(&self) -> Option<usize> {
        self.input.fetch()
    }

    fn try_pushdown_sort(
        &self,
        order: &[PhysicalSortExpr],
    ) -> Result<SortOrderPushdownResult<Arc<dyn ExecutionPlan>>> {
        let result = self.input.try_pushdown_sort(order)?;
        Ok(result.map(|input| {
            Arc::new(Self::new(input, Arc::clone(&self.provider))) as Arc<dyn ExecutionPlan>
        }))
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::LowerEqual
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
}
