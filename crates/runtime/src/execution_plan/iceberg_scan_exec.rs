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
//! An [`IcebergTableScan`] holds a live, non-serializable `Table`, and its
//! planned `FileScanTask`s carry fields the iceberg crate intentionally refuses
//! to serialize. So instead of shipping the plan, this leaf wraps it and carries
//! the `DataFusion` [`TableReference`] of the registered Iceberg provider plus
//! the scan's projection, filters, and limit. The physical codec serializes that
//! recipe; on the executor it resolves the same registered provider and replays
//! `TableProvider::scan` to re-derive an equivalent, identically-bucketed scan,
//! reusing the provider's catalog (so no secrets cross the wire).
//!
//! It is a *leaf* node: [`children`](IcebergScanExec::children) returns an empty
//! list so the non-serializable inner scan is never handed to Ballista's codec.
//! All execution is delegated to the inner [`IcebergTableScan`].

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use datafusion::common::{Result, Statistics, TableReference};
use datafusion::config::ConfigOptions;
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::OrderingRequirements;
use datafusion::physical_plan::execution_plan::{
    CardinalityEffect, InvariantLevel, check_default_invariants,
};
use datafusion::physical_plan::filter_pushdown::{
    ChildPushdownResult, FilterDescription, FilterPushdownPhase, FilterPushdownPropagation,
};
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, PhysicalExpr, PlanProperties,
    SortOrderPushdownResult, expressions::PhysicalSortExpr,
};
use datafusion::prelude::SessionConfig;
use runtime_datafusion::config::cluster_config::SpiceClusterConfig;

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

/// A leaf execution plan that wraps an [`IcebergTableScan`] for distributed
/// execution.
///
/// It carries the [`TableReference`] of the registered Iceberg provider plus the
/// scan's projection, filters, and limit. The physical codec serializes this
/// recipe; the executor resolves the registered provider and calls its `scan()`
/// with the same arguments to re-derive an equivalent (identically bucketed)
/// scan — sidestepping the iceberg `FileScanTask` fields that are intentionally
/// non-serializable, while reusing the provider's catalog (no secrets on the
/// wire). Execution is delegated to `inner`.
#[derive(Debug)]
pub struct IcebergScanExec {
    /// `DataFusion` reference of the registered Iceberg table, used by the codec
    /// to resolve the provider (and reuse its catalog) on the executor.
    table_ref: TableReference,
    /// The wrapped Iceberg scan (an `IcebergTableScan`). Held privately and never
    /// exposed as a child, so Ballista never tries to serialize it directly.
    inner: Arc<dyn ExecutionPlan>,
    /// Column projection passed to `TableProvider::scan`, replayed on the executor.
    projection: Option<Vec<usize>>,
    /// Pushed-down filters passed to `TableProvider::scan`, replayed on the
    /// executor so it reproduces the same file pruning / partition count.
    filters: Vec<Expr>,
    /// Row limit passed to `TableProvider::scan`.
    limit: Option<usize>,
}

impl IcebergScanExec {
    /// Wraps `inner` (an `IcebergTableScan`) with the table reference and the
    /// scan arguments needed to reconstruct an equivalent scan remotely.
    #[must_use]
    pub fn new(
        table_ref: TableReference,
        inner: Arc<dyn ExecutionPlan>,
        projection: Option<Vec<usize>>,
        filters: Vec<Expr>,
        limit: Option<usize>,
    ) -> Self {
        Self {
            table_ref,
            inner,
            projection,
            filters,
            limit,
        }
    }

    /// The `DataFusion` reference of the registered Iceberg table.
    #[must_use]
    pub fn table_ref(&self) -> &TableReference {
        &self.table_ref
    }

    /// The wrapped Iceberg scan (an `IcebergTableScan`).
    #[must_use]
    pub fn inner(&self) -> &Arc<dyn ExecutionPlan> {
        &self.inner
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
}

impl DisplayAs for IcebergScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "IcebergScanExec table_ref=[{}] ", self.table_ref)?;
        self.inner.fmt_as(t, f)
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
        self.inner.properties()
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
        self.inner.execute(partition, context)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        self.inner.metrics()
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>> {
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
