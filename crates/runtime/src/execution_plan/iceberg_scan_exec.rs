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

//! A serializable leaf execution plan for distributed Iceberg scans.
//!
//! An `IcebergTableScan` holds a live, non-serializable `Table`, and its planned
//! `FileScanTask`s carry fields the iceberg crate intentionally refuses to
//! serialize. So instead of shipping the plan, this leaf carries a *recipe* — the
//! `DataFusion` [`TableReference`] of the registered Iceberg provider plus the
//! scan's projection, filters, and limit — which the physical codec serializes.
//!
//! It has two modes ([`ScanSource`]):
//! - **Planned** (scheduler / single-node): wraps the concrete scan the provider
//!   produced, so physical planning sees real schema and partitioning.
//! - **Deferred** (remote executor, after decode): holds the registered provider
//!   resolved synchronously from the recipe; the actual `TableProvider::scan` is
//!   replayed lazily inside [`execute`](IcebergScanExec::execute) — in proper
//!   async context — so no catalog I/O happens during synchronous plan
//!   deserialization and no blocking bridge is needed.
//!
//! It is a *leaf* node: [`children`](IcebergScanExec::children) returns an empty
//! list so the non-serializable scan is never handed to Ballista's codec.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use datafusion::catalog::TableProvider;
use datafusion::common::{Result, Statistics, TableReference};
use datafusion::config::ConfigOptions;
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::{EquivalenceProperties, OrderingRequirements, Partitioning};
use datafusion::physical_plan::execution_plan::{
    Boundedness, CardinalityEffect, EmissionType, InvariantLevel, check_default_invariants,
};
use datafusion::physical_plan::filter_pushdown::{
    ChildPushdownResult, FilterDescription, FilterPushdownPhase, FilterPushdownPropagation,
};
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, PhysicalExpr, PlanProperties,
    SortOrderPushdownResult, expressions::PhysicalSortExpr,
};
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::TryStreamExt;
use iceberg_datafusion::physical_plan::IcebergTableScan;
use runtime_datafusion::config::cluster_config::SpiceClusterConfig;
use tokio::sync::OnceCell;

/// Returns `true` when `config` belongs to a distributed (Ballista) session.
///
/// [`SpiceClusterConfig`] is installed on the session config only when the
/// runtime is acting as a cluster scheduler/executor, so its presence is a
/// reliable signal that a query will be planned and shipped across nodes. In a
/// single-node session it is absent, so Iceberg scans are left untouched.
#[must_use]
pub fn session_is_distributed(config: &SessionConfig) -> bool {
    config
        .options()
        .extensions
        .get::<SpiceClusterConfig>()
        .is_some()
}

/// How an [`IcebergScanExec`] produces its rows.
#[derive(Debug)]
enum ScanSource {
    /// Planning-time: the concrete scan the provider produced (scheduler side and
    /// single-node execution). Execution delegates straight to it.
    Planned(Arc<dyn ExecutionPlan>),
    /// Decode-time on a remote executor: the registered Iceberg provider, resolved
    /// synchronously from the recipe. The scan is re-derived lazily on the first
    /// `execute()` and memoized in `scan`, so it is planned exactly once per
    /// instance (not once per partition) — avoiding redundant `plan_files()` work
    /// and keeping every partition of this instance on a single snapshot/fileset.
    /// No catalog I/O runs during synchronous plan deserialization.
    Deferred {
        provider: Arc<dyn TableProvider>,
        scan: Arc<OnceCell<Arc<dyn ExecutionPlan>>>,
    },
}

/// A leaf execution plan that represents a distributed Iceberg scan. See the
/// module docs for the planning-vs-deferred modes.
#[derive(Debug)]
pub struct IcebergScanExec {
    /// `DataFusion` reference of the registered Iceberg table, used by the codec
    /// to resolve the provider (and reuse its catalog) on the executor.
    table_ref: TableReference,
    /// Output schema (projected). Cached so `schema()` is sync in both modes.
    schema: SchemaRef,
    /// Column projection passed to `TableProvider::scan`, replayed on the executor.
    projection: Option<Vec<usize>>,
    /// Pushed-down filters passed to `TableProvider::scan`, replayed on the
    /// executor so it reproduces the same file pruning / partition count.
    filters: Vec<Expr>,
    /// Row limit passed to `TableProvider::scan`.
    limit: Option<usize>,
    /// Cached plan properties (schema + partitioning).
    properties: Arc<PlanProperties>,
    /// Where rows come from — see [`ScanSource`].
    source: ScanSource,
}

impl IcebergScanExec {
    /// Planning-time constructor: wraps the concrete `inner` scan (an
    /// `IcebergTableScan`) the provider produced, carrying the scan arguments the
    /// codec needs to serialize a recipe.
    #[must_use]
    pub fn new(
        table_ref: TableReference,
        inner: Arc<dyn ExecutionPlan>,
        projection: Option<Vec<usize>>,
        filters: Vec<Expr>,
        limit: Option<usize>,
    ) -> Self {
        let schema = inner.schema();
        let properties = Arc::clone(inner.properties());
        Self {
            table_ref,
            schema,
            projection,
            filters,
            limit,
            properties,
            source: ScanSource::Planned(inner),
        }
    }

    /// Decode-time constructor: builds a deferred scan over the registered
    /// `provider`. `schema` and `partitioning` are reconstructed from the recipe
    /// so `properties()` is correct before the (lazy) scan runs. The provider must
    /// be the concrete Iceberg provider (e.g. `cluster.inner()`), not the cluster
    /// wrapper, so replaying its `scan()` yields the bare scan without re-wrapping.
    #[must_use]
    pub fn new_deferred(
        table_ref: TableReference,
        provider: Arc<dyn TableProvider>,
        projection: Option<Vec<usize>>,
        filters: Vec<Expr>,
        limit: Option<usize>,
        schema: SchemaRef,
        partitioning: Partitioning,
    ) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            partitioning,
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            table_ref,
            schema,
            projection,
            filters,
            limit,
            properties,
            source: ScanSource::Deferred {
                provider,
                scan: Arc::new(OnceCell::new()),
            },
        }
    }

    /// The `DataFusion` reference of the registered Iceberg table.
    #[must_use]
    pub fn table_ref(&self) -> &TableReference {
        &self.table_ref
    }

    /// The scan's column projection.
    #[must_use]
    pub fn projection(&self) -> Option<&Vec<usize>> {
        self.projection.as_ref()
    }

    /// The scan's pushed-down filters.
    #[must_use]
    pub fn filters(&self) -> &[Expr] {
        &self.filters
    }

    /// The scan's row limit.
    #[must_use]
    pub fn limit(&self) -> Option<usize> {
        self.limit
    }

    /// The snapshot to pin for this scan, derived from the wrapped scan at plan
    /// time: the scan's explicit snapshot if any, else the table's current
    /// snapshot. The codec serializes this so every executor task plans against
    /// the same snapshot — giving one consistent snapshot across all partitions
    /// of a distributed query even under concurrent commits. Returns `None` for a
    /// deferred node (its provider already carries the pin) or if the wrapped plan
    /// isn't an `IcebergTableScan`.
    #[must_use]
    pub fn snapshot_id(&self) -> Option<i64> {
        match &self.source {
            ScanSource::Planned(inner) => inner.downcast_ref::<IcebergTableScan>().and_then(|s| {
                s.snapshot_id()
                    .or_else(|| s.table().metadata().current_snapshot_id())
            }),
            ScanSource::Deferred { .. } => None,
        }
    }
}

impl DisplayAs for IcebergScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "IcebergScanExec table_ref=[{}]", self.table_ref)?;
        match &self.source {
            ScanSource::Planned(inner) => {
                write!(f, " ")?;
                inner.fmt_as(t, f)
            }
            ScanSource::Deferred { .. } => {
                let projection = self.projection.as_ref().map_or_else(String::new, |p| {
                    p.iter().map(usize::to_string).collect::<Vec<_>>().join(",")
                });
                write!(f, " deferred projection:[{projection}]")
            }
        }
    }
}

#[deny(clippy::missing_trait_methods)]
impl ExecutionPlan for IcebergScanExec {
    fn downcast_delegate(&self) -> Option<&dyn ExecutionPlan> {
        None
    }

    fn with_preserve_order(&self, _preserve_order: bool) -> Option<Arc<dyn ExecutionPlan>> {
        None
    }

    fn name(&self) -> &'static str {
        "IcebergScanExec"
    }

    fn static_name() -> &'static str
    where
        Self: Sized,
    {
        "IcebergScanExec"
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
        // Leaf node: the inner Iceberg scan is reconstructed from the serialized
        // recipe, never traversed or serialized as a child.
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
                "IcebergScanExec expects no children".to_string(),
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
        match &self.source {
            ScanSource::Planned(inner) => inner.execute(partition, context),
            ScanSource::Deferred { provider, scan } => {
                let provider = Arc::clone(provider);
                let scan = Arc::clone(scan);
                let projection = self.projection.clone();
                let filters = self.filters.clone();
                let limit = self.limit;
                let schema = self.schema();

                // Re-derive the scan lazily, in async context, and memoize it: the
                // first partition to execute plans it (via plan_files); every other
                // partition of this instance reuses the same plan, so they all read
                // one consistent snapshot/fileset and re-planning happens once. A
                // SessionState carrying the per-job config (notably
                // `target_partitions`) makes the rebuilt scan bucket the way the
                // scheduler planned.
                let fut = async move {
                    let plan = scan
                        .get_or_try_init(|| async {
                            let session =
                                SessionContext::new_with_config(context.session_config().clone())
                                    .state();
                            provider
                                .scan(&session, projection.as_ref(), &filters, limit)
                                .await
                        })
                        .await?;
                    plan.execute(partition, context)
                };
                let stream = futures::stream::once(fut).try_flatten();
                Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
            }
        }
    }

    fn metrics(&self) -> Option<MetricsSet> {
        match &self.source {
            ScanSource::Planned(inner) => inner.metrics(),
            ScanSource::Deferred { .. } => None,
        }
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>> {
        match &self.source {
            ScanSource::Planned(inner) => inner.partition_statistics(partition),
            ScanSource::Deferred { .. } => Ok(Arc::new(Statistics::new_unknown(&self.schema))),
        }
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
}
