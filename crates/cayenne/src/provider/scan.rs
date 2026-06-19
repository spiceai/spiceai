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

use std::{
    any::Any,
    collections::{BTreeSet, HashMap},
    sync::{Arc, OnceLock},
};

use crate::maintained_aggregate::MaintainedAggregateRegistry;
use arrow_schema::SchemaRef;
use datafusion::config::ConfigOptions;
use datafusion::error::Result;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_common::{DataFusionError, Statistics, stats::Precision};
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_scan_config::FileScanConfig;
use datafusion_datasource::source::DataSourceExec;
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_expr::expressions::{Column, DynamicFilterPhysicalExpr};
use datafusion_physical_expr::{Distribution, OrderingRequirements, PhysicalExpr};
use futures::{StreamExt, TryStreamExt};
use parking_lot::Mutex;

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
    union::UnionExec,
};

/// Keeps a scan's snapshot directories alive for the FULL lifetime of the scan —
/// plan-build AND execution. Increments a per-snapshot in-flight-scan ref-count on
/// creation and decrements it on `Drop` (when the scan's `ExecutionPlan` and all its
/// output streams have been dropped). Snapshot GC/cleanup skips any dir with a live
/// ref, so a long-running query (e.g. a 139s analytical scan) cannot have its Vortex
/// segment files deleted out from under it by a concurrent compaction. This replaces
/// the brittle time-based grace, which a query slower than the grace would outlive.
///
/// Correctness rests on the listing-flip invariant: a scan captures its snapshot ids
/// under `listing_fence.read()`, and a compaction's flip takes `listing_fence.write()`,
/// so after the flip no NEW scan can reference the superseded snapshot — its ref-count
/// only decreases, and cleanup that observes count 0 can delete it safely.
#[derive(Debug)]
pub(crate) struct SnapshotScanRef {
    refs: Arc<Mutex<HashMap<String, usize>>>,
    snapshot_ids: Vec<String>,
}

impl SnapshotScanRef {
    /// Increment the in-flight-scan ref-count for each snapshot id and return a
    /// guard that decrements them on drop.
    pub(crate) fn new(
        refs: Arc<Mutex<HashMap<String, usize>>>,
        snapshot_ids: Vec<String>,
    ) -> Arc<Self> {
        {
            let mut map = refs.lock();
            for id in &snapshot_ids {
                *map.entry(id.clone()).or_insert(0) += 1;
            }
        }
        Arc::new(Self { refs, snapshot_ids })
    }
}

impl Drop for SnapshotScanRef {
    fn drop(&mut self) {
        let mut map = self.refs.lock();
        for id in &self.snapshot_ids {
            if let Some(count) = map.get_mut(id) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    map.remove(id);
                }
            }
        }
    }
}

/// Wrapper for Cayenne acceleration execution plans.
/// This is used to identify Cayenne-specific table scans from within the physical plan, once references to the table is lost from the logical plan.
#[derive(Debug)]
pub struct CayenneAccelerationExec {
    inner: Arc<dyn ExecutionPlan>,
    scan_identity: OnceLock<Option<Arc<ScanIdentity>>>,
    /// In-flight-scan ref-count guard for the snapshot dirs this scan reads. Held
    /// for the plan's lifetime AND injected into each output stream by `execute`,
    /// so the snapshots stay GC-protected until execution completes. `None` for the
    /// inner per-snapshot wrappers; set on the outermost wrapper `scan()` returns.
    /// MUST be carried through every plan-rewriting method (`with_new_children`,
    /// `with_fetch`, `try_swapping_with_projection`, `reset_state`) or a concurrent
    /// compaction could GC a snapshot mid-execution.
    scan_guard: Option<Arc<SnapshotScanRef>>,
    maintained_aggregates: Option<Arc<MaintainedAggregateRegistry>>,
    maintained_aggregate_epoch: u64,
    /// Column-statistics overlay sourced from the table's maintained optimizer
    /// aggregate (live min/max + integer NDV), aligned to the inner plan's
    /// output schema. Consumed in [`Self::partition_statistics`] to refill
    /// column stats the Cayenne base+delta `UnionExec` drops to
    /// `Precision::Absent` via `DataFusion`'s generic `col_stats_union`
    optimizer_column_overlay: Option<Arc<Statistics>>,
}

impl CayenneAccelerationExec {
    /// Creates a new `CayenneAccelerationExec` wrapping the given execution plan.
    #[must_use]
    pub fn new(inner: Arc<dyn ExecutionPlan>) -> Self {
        Self {
            inner,
            scan_identity: OnceLock::new(),
            scan_guard: None,
            maintained_aggregates: None,
            maintained_aggregate_epoch: 0,
            optimizer_column_overlay: None,
        }
    }

    /// As [`Self::new`], but carries an in-flight-scan ref-count guard so the
    /// snapshot dirs this scan reads are not GC'd until execution completes.
    #[must_use]
    pub(crate) fn with_guard(inner: Arc<dyn ExecutionPlan>, guard: Arc<SnapshotScanRef>) -> Self {
        Self {
            inner,
            scan_identity: OnceLock::new(),
            scan_guard: Some(guard),
            maintained_aggregates: None,
            maintained_aggregate_epoch: 0,
            optimizer_column_overlay: None,
        }
    }

    /// Creates a new `CayenneAccelerationExec` carrying maintained aggregate
    /// state captured at the table scan visibility epoch.
    #[must_use]
    pub fn new_with_maintained_aggregates(
        inner: Arc<dyn ExecutionPlan>,
        maintained_aggregates: Arc<MaintainedAggregateRegistry>,
        maintained_aggregate_epoch: u64,
    ) -> Self {
        Self {
            inner,
            scan_identity: OnceLock::new(),
            scan_guard: None,
            maintained_aggregates: Some(maintained_aggregates),
            maintained_aggregate_epoch,
            optimizer_column_overlay: None,
        }
    }

    /// As [`Self::new_with_maintained_aggregates`], but also carries an in-flight-scan
    /// ref-count guard. Used for the outermost wrapper `scan()` returns when the table
    /// has maintained aggregates, so the result carries BOTH the GC-protection guard
    /// and the captured aggregate state.
    #[must_use]
    pub(crate) fn with_guard_and_maintained_aggregates(
        inner: Arc<dyn ExecutionPlan>,
        guard: Arc<SnapshotScanRef>,
        maintained_aggregates: Arc<MaintainedAggregateRegistry>,
        maintained_aggregate_epoch: u64,
    ) -> Self {
        Self {
            inner,
            scan_identity: OnceLock::new(),
            scan_guard: Some(guard),
            maintained_aggregates: Some(maintained_aggregates),
            maintained_aggregate_epoch,
            optimizer_column_overlay: None,
        }
    }

    /// Attaches a column-statistics overlay sourced from the table's maintained
    /// optimizer aggregate (live min/max + integer NDV). At
    /// [`Self::partition_statistics`] this refills only the columns the Cayenne
    /// base+delta `UnionExec` wiped to `Precision::Absent`, restoring the
    /// join-key signal `JoinSelection` needs without overriding any surviving
    /// child statistic. A `None` overlay (cold aggregate) is a no-op.
    #[must_use]
    pub(crate) fn with_optimizer_column_overlay(
        mut self,
        overlay: Option<Arc<Statistics>>,
    ) -> Self {
        self.optimizer_column_overlay = overlay;
        self
    }

    /// Returns the maintained aggregate registry and scan epoch captured for
    /// this table scan, if aggregate maintenance is enabled for the table.
    #[must_use]
    pub(crate) fn maintained_aggregates(&self) -> Option<(&Arc<MaintainedAggregateRegistry>, u64)> {
        self.maintained_aggregates
            .as_ref()
            .map(|registry| (registry, self.maintained_aggregate_epoch))
    }

    /// Rewrap `inner`, preserving this node's carry-through state — the in-flight
    /// scan guard AND any maintained-aggregate registry — so both survive the
    /// optimizer transforms applied by the plan-rewriting trait methods
    /// (`with_new_children`, `with_fetch`, `try_swapping_with_projection`).
    fn wrap_rewritten_child(&self, inner: Arc<dyn ExecutionPlan>) -> Self {
        // The output schema is stable across child rewrites (projection/limit
        // pushdown), so the optimizer column overlay stays aligned and valid.
        Self {
            inner,
            scan_identity: OnceLock::new(),
            scan_guard: self.scan_guard.clone(),
            maintained_aggregates: self.maintained_aggregates.clone(),
            maintained_aggregate_epoch: self.maintained_aggregate_epoch,
            optimizer_column_overlay: self.optimizer_column_overlay.clone(),
        }
    }

    /// Returns a stable identity for the underlying scan source, derived from
    /// the `FileScanConfig`'s `object_store_url` plus the sorted set of file
    /// paths backing the inner `DataSourceExec`.
    ///
    /// Two `CayenneAccelerationExec` nodes that scan the same set of physical
    /// files return the same identity, which is the precondition for sharing a
    /// runtime dynamic filter across them (see the cross-scan filter sharing
    /// workstream documented in `crates/cayenne/src/optimizer_rules.rs`).
    ///
    /// Returns `None` if the inner plan does not contain a `DataSourceExec`
    /// whose `DataSource` is a `FileScanConfig` with at least one file. Mixed
    /// inlined-data scans use a `UnionExec`; their in-memory branch is ignored
    /// and the identity is derived from the file-backed branch. The identity
    /// intentionally ignores ordering of files within partitions and projection
    /// differences — it is purely a per-table fingerprint.
    ///
    /// The `object_store_url` is required to disambiguate two stores that
    /// happen to contain the same relative paths (e.g. two different S3
    /// buckets both with `part-000.vortex`). Without it the identity would
    /// silently collide when paths are stored as relative locations.
    #[must_use]
    pub(crate) fn scan_identity(&self) -> Option<Arc<ScanIdentity>> {
        self.scan_identity
            .get_or_init(|| compute_scan_identity(&self.inner))
            .as_ref()
            .map(Arc::clone)
    }

    /// Returns the dynamic filters currently pushed into this Cayenne scan.
    ///
    /// These filters originate from `DataFusion`'s hash-join dynamic-filter
    /// pass. They are safe to share only when an optimizer has proven the target
    /// scan is equi-joined on every referenced column.
    #[must_use]
    pub(crate) fn dynamic_filters(&self) -> Vec<ScanDynamicFilter> {
        let mut filters = Vec::new();
        for file_scan_config in file_scan_configs(&self.inner) {
            if let Some(filter) = file_scan_config.file_source().filter() {
                collect_dynamic_filters(&filter, &mut filters);
            }
        }
        filters
    }

    /// Push additional dynamic filters into the underlying file source.
    ///
    /// Returns `Ok(None)` when the scan source declined all filters or the inner
    /// plan is not a simple file scan.
    ///
    /// # Errors
    ///
    /// Returns an error when rebuilding the underlying `DataSourceExec` with the
    /// additional filters fails.
    pub(crate) fn with_additional_dynamic_filters(
        &self,
        filters: &[Arc<dyn PhysicalExpr>],
        config: &ConfigOptions,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(inner) =
            push_dynamic_filters_to_data_source(Arc::clone(&self.inner), filters, config)?
        else {
            return Ok(None);
        };

        Ok(Some(Arc::new(self.wrap_rewritten_child(inner))))
    }
}

/// Refills only `Precision::Absent` column stats in `child` from `overlay`,
/// restoring the join-key `min/max` + `distinct_count` (NDV) that the Cayenne
/// base+delta `UnionExec` drops (a stat-less/empty delta branch makes
/// `col_stats_union` return `Absent`, collapsing `estimate_inner_join_cardinality`
/// to `min(L, R)`). Present child stats — filter-aware `num_rows`/`null_count`
/// and any surviving column stat — are preserved; a column-count mismatch is a
/// defensive no-op.
///
/// `UnionExec` can't fix this itself: an `Absent` branch means *unknown*, not
/// *empty*, so it must drop min/max, and NDV isn't additive across branches of
/// unknown overlap. The repair belongs here because only the Cayenne scan owns
/// the out-of-band metadata the union can't see — the incrementally-maintained
/// per-table aggregate (live min/max + HLL-derived integer NDV over base+delta)
/// — which it applies as an additive overlay, not a stats replacement.
fn restore_absent_column_statistics(mut child: Statistics, overlay: &Statistics) -> Statistics {
    if child.column_statistics.len() != overlay.column_statistics.len() {
        return child;
    }
    for (col, src) in child
        .column_statistics
        .iter_mut()
        .zip(overlay.column_statistics.iter())
    {
        if matches!(col.min_value, Precision::Absent) {
            col.min_value = src.min_value.clone();
        }
        if matches!(col.max_value, Precision::Absent) {
            col.max_value = src.max_value.clone();
        }
        if matches!(col.distinct_count, Precision::Absent) {
            col.distinct_count = src.distinct_count;
        }
    }
    child
}

fn compute_scan_identity(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<ScanIdentity>> {
    let file_scan_configs = file_scan_configs(plan);
    let first_file_scan_config = file_scan_configs.first()?;
    let object_store_url = first_file_scan_config.object_store_url.as_str();
    if file_scan_configs
        .iter()
        .any(|file_scan_config| file_scan_config.object_store_url.as_str() != object_store_url)
    {
        return None;
    }

    let mut paths: Vec<String> = file_scan_configs
        .iter()
        .flat_map(|file_scan_config| file_scan_config.file_groups.iter())
        .flat_map(datafusion_datasource::file_groups::FileGroup::iter)
        .map(|pf| pf.object_meta.location.to_string())
        .collect();

    if paths.is_empty() {
        return None;
    }

    paths.sort();
    paths.dedup();
    Some(Arc::new(ScanIdentity {
        object_store_url: Arc::from(object_store_url),
        paths: Arc::from(paths),
    }))
}

/// Stable identifier for a Cayenne scan source, derived from the
/// `FileScanConfig`'s `object_store_url` plus the sorted set of file paths
/// backing the underlying `DataSourceExec`.
///
/// Equality and hashing are content-based on both the `object_store_url` and
/// the path set, so two `CayenneAccelerationExec` instances over the same
/// logical table compare equal regardless of projection, partitioning, or
/// wrapper-plan differences — and two scans over different stores that happen
/// to share a relative path (e.g. two S3 buckets each with `part-000.vortex`)
/// do *not* collide. The path set is reference-counted so copying a scan
/// identity during optimizer rewrites does not clone every file path.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct ScanIdentity {
    object_store_url: Arc<str>,
    paths: Arc<[String]>,
}

/// A dynamic filter currently attached to a Cayenne scan, plus the scan-local
/// column names the filter references.
#[derive(Clone)]
pub(crate) struct ScanDynamicFilter {
    filter: Arc<dyn PhysicalExpr>,
    columns: BTreeSet<String>,
}

impl ScanDynamicFilter {
    /// Returns the shared dynamic filter expression.
    #[must_use]
    pub(crate) fn filter(&self) -> &Arc<dyn PhysicalExpr> {
        &self.filter
    }

    /// Returns the scan-local column names referenced by this filter.
    #[must_use]
    pub(crate) fn columns(&self) -> &BTreeSet<String> {
        &self.columns
    }
}

fn file_scan_configs(plan: &Arc<dyn ExecutionPlan>) -> Vec<&FileScanConfig> {
    let mut configs = Vec::new();
    collect_file_scan_configs(plan, &mut configs);
    configs
}

/// Counts file-backed scan sources (snapshot generations) and the total files
/// across them in `plan`, returning `(snapshots_scanned, files_scanned)`.
///
/// Unlike [`file_scan_configs`], this walks the WHOLE subtree and does not stop at
/// non-identity-preserving wrappers: for read-amplification reporting we want every
/// `DataSourceExec` that touches disk regardless of the per-snapshot deletion filter,
/// sort, or other operator sitting above it.
fn count_file_scan_sources(plan: &Arc<dyn ExecutionPlan>) -> (usize, usize) {
    let mut snapshots = 0;
    let mut files = 0;
    accumulate_file_scan_sources(plan, &mut snapshots, &mut files);
    (snapshots, files)
}

fn accumulate_file_scan_sources(
    plan: &Arc<dyn ExecutionPlan>,
    snapshots: &mut usize,
    files: &mut usize,
) {
    if let Some(data_source_exec) = plan.downcast_ref::<DataSourceExec>() {
        if let Some(file_scan_config) = data_source_exec
            .data_source()
            .downcast_ref::<FileScanConfig>()
        {
            *snapshots += 1;
            *files += file_scan_config
                .file_groups
                .iter()
                .map(FileGroup::len)
                .sum::<usize>();
        }
        return;
    }
    for child in plan.children() {
        accumulate_file_scan_sources(child, snapshots, files);
    }
}

/// Walks `plan` looking for underlying file-backed `DataSourceExec` nodes,
/// descending only through a whitelist of operators that are known to preserve
/// scan identity, plus `UnionExec` for mixed file + inlined-memory scans.
///
/// Cayenne plans typically wrap the data source in transparent or
/// near-transparent operators: `ProjectionExec`, `RepartitionExec`,
/// `CoalesceBatchesExec`, `CoalescePartitionsExec`, plus the runtime's
/// `BytesProcessedExec` / `SchemaCastScanExec` and the cayenne-internal
/// `InexactStatsExec`. Any one of those may sit between
/// `CayenneAccelerationExec` and the `DataSourceExec`.
///
/// Cayenne tables with inlined rows add a `UnionExec` whose file-backed branch
/// should still participate in dynamic-filter sharing. Non-file children such
/// as `MemoryExec` are ignored; they stay unfiltered because inline batches are
/// intentionally small.
///
/// Anything else with a single child (e.g. `FilterExec`, `SortExec`,
/// `LimitExec`, an unfamiliar custom node) is *not* identity-preserving for
/// our purposes — it may change cardinality, ordering, or the file-set
/// semantics the identity relies on. Collecting no file scans is safer than
/// misattributing identity: the worst that happens is dynamic-filter sharing is
/// conservatively disabled.
fn collect_file_scan_configs<'a>(
    plan: &'a Arc<dyn ExecutionPlan>,
    configs: &mut Vec<&'a FileScanConfig>,
) {
    if let Some(data_source_exec) = plan.downcast_ref::<DataSourceExec>() {
        if let Some(file_scan_config) = data_source_exec
            .data_source()
            .downcast_ref::<FileScanConfig>()
        {
            configs.push(file_scan_config);
        }
        return;
    }

    if plan.downcast_ref::<UnionExec>().is_some() {
        for child in plan.children() {
            collect_file_scan_configs(child, configs);
        }
        return;
    }

    if !is_identity_preserving_wrapper(plan) {
        return;
    }

    let children = plan.children();
    if children.len() != 1 {
        return;
    }

    collect_file_scan_configs(children[0], configs);
}

/// Returns `true` if `plan` is a known transparent / near-transparent wrapper
/// that preserves the underlying scan's identity (same file set, same logical
/// rows, just resharded / renamed / instrumented).
///
/// The check is by-type for the wrappers we have in-scope, and by `name()` for
/// the ones that live in other crates or are crate-private. Adding a new
/// wrapper requires touching this function explicitly — that's intentional;
/// it stops a future operator from silently being treated as transparent.
#[expect(deprecated)]
fn is_identity_preserving_wrapper(plan: &Arc<dyn ExecutionPlan>) -> bool {
    if plan.downcast_ref::<ProjectionExec>().is_some()
        || plan.downcast_ref::<RepartitionExec>().is_some()
        || plan
            .downcast_ref::<datafusion_physical_plan::coalesce_batches::CoalesceBatchesExec>()
            .is_some()
        || plan
            .downcast_ref::<datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec>()
            .is_some()
        || plan.downcast_ref::<CayenneAccelerationExec>().is_some()
    {
        return true;
    }

    // Cross-crate / crate-private wrappers we can't downcast to without
    // pulling in their concrete types: match by the stable `name()` string.
    matches!(
        plan.name(),
        "BytesProcessedExec" | "SchemaCastScanExec" | "InexactStatsExec"
    )
}

fn collect_dynamic_filters(expr: &Arc<dyn PhysicalExpr>, filters: &mut Vec<ScanDynamicFilter>) {
    if let Some(dynamic_filter) = expr.downcast_ref::<DynamicFilterPhysicalExpr>() {
        if let Some(columns) = dynamic_filter_column_names(dynamic_filter) {
            filters.push(ScanDynamicFilter {
                filter: Arc::clone(expr),
                columns,
            });
        }
        return;
    }

    for child in expr.children() {
        collect_dynamic_filters(child, filters);
    }
}

fn dynamic_filter_column_names(
    dynamic_filter: &DynamicFilterPhysicalExpr,
) -> Option<BTreeSet<String>> {
    let mut columns = BTreeSet::new();
    for child in dynamic_filter.children() {
        let column = child.downcast_ref::<Column>()?;
        columns.insert(column.name().to_string());
    }

    if columns.is_empty() {
        None
    } else {
        Some(columns)
    }
}

fn push_dynamic_filters_to_data_source(
    plan: Arc<dyn ExecutionPlan>,
    filters: &[Arc<dyn PhysicalExpr>],
    optimizer_config: &ConfigOptions,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    if filters.is_empty() {
        return Ok(None);
    }

    if let Some(data_source_exec) = plan.downcast_ref::<DataSourceExec>()
        && let Some(file_scan_config) = data_source_exec
            .data_source()
            .downcast_ref::<FileScanConfig>()
    {
        let filters = filters.iter().map(Arc::clone).collect();
        let propagation = file_scan_config
            .file_source()
            .try_pushdown_filters(filters, optimizer_config)?;

        let Some(updated_source) = propagation.updated_node else {
            return Ok(None);
        };

        let mut updated_config = file_scan_config.clone();
        updated_config.file_source = updated_source;
        let updated_exec = data_source_exec
            .clone()
            .with_data_source(Arc::new(updated_config));
        return Ok(Some(Arc::new(updated_exec)));
    }

    let children = plan
        .children()
        .into_iter()
        .map(Arc::clone)
        .collect::<Vec<_>>();
    if children.is_empty() {
        return Ok(None);
    }

    let is_union = plan.downcast_ref::<UnionExec>().is_some();
    if !is_union && !is_identity_preserving_wrapper(&plan) {
        return Ok(None);
    }
    if !is_union && children.len() != 1 {
        return Ok(None);
    }

    let mut changed = false;
    let mut new_children = Vec::with_capacity(children.len());
    for child in children {
        match push_dynamic_filters_to_data_source(Arc::clone(&child), filters, optimizer_config)? {
            Some(updated_child) => {
                changed = true;
                new_children.push(updated_child);
            }
            None => new_children.push(child),
        }
    }

    if !changed {
        return Ok(None);
    }

    plan.with_new_children(new_children).map(Some)
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
        // Surface read amplification: how many file-backed snapshots this scan unions
        // (base + un-compacted protected snapshots) and the total Vortex files across
        // them. Each query re-scans and merge-filters every generation, so a rising
        // `snapshots_scanned` is the signal that compaction is behind. This is the
        // structural read-tax that otherwise had to be hand-counted from the plan tree.
        //
        // NB: a full subtree walk, NOT `file_scan_configs` — the latter intentionally
        // stops at non-identity-preserving nodes (the per-snapshot deletion filter is
        // one), which would undercount the snapshots to zero on a real scan plan.
        let (snapshots_scanned, files_scanned) = count_file_scan_sources(&self.inner);
        write!(
            f,
            "CayenneAccelerationExec: snapshots_scanned={snapshots_scanned}, files_scanned={files_scanned}"
        )
    }
}

#[deny(clippy::missing_trait_methods)]
impl ExecutionPlan for CayenneAccelerationExec {
    fn downcast_delegate(&self) -> Option<&dyn ExecutionPlan> {
        None
    }

    fn with_preserve_order(&self, _preserve_order: bool) -> Option<Arc<dyn ExecutionPlan>> {
        None
    }

    fn name(&self) -> &'static str {
        "CayenneAccelerationExec"
    }

    fn static_name() -> &'static str
    where
        Self: Sized,
    {
        "CayenneAccelerationExec"
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(self.properties().eq_properties.schema())
    }

    fn properties(&self) -> &Arc<PlanProperties> {
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
        vec![false]
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
        Ok(Arc::new(self.wrap_rewritten_child(input)))
    }

    fn reset_state(self: Arc<Self>) -> Result<Arc<dyn ExecutionPlan>> {
        let children = self.children().into_iter().cloned().collect();
        self.with_new_children(children)
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
        // Hold the in-flight-scan guard for the stream's lifetime so the snapshot
        // dirs this scan reads are not GC'd mid-execution. The closure is a no-op
        // per batch; it drops (releasing the ref-count) when the stream is fully
        // consumed or dropped. `None` on the inner per-snapshot wrappers.
        let scan_guard = self.scan_guard.clone();
        let mapped = mapped.map(move |item| {
            let _hold = &scan_guard;
            item
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, mapped)))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        self.inner.metrics()
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>> {
        let child_stats = self.inner.partition_statistics(partition)?;
        // The overlay is a per-table (global) aggregate: its min/max/NDV
        // describe the whole table, not any single partition. Only the
        // table-wide aggregate stats (`partition == None`) may be refilled from
        // it. Per-partition stats (`partition == Some(_)`) must pass through
        // unchanged — filling them from the global aggregate would violate
        // `partition_statistics(Some(_))` semantics and mislead partition-level
        // pruning/optimization.
        let Some(overlay) = self
            .optimizer_column_overlay
            .as_ref()
            .filter(|_| partition.is_none())
        else {
            return Ok(child_stats);
        };

        Ok(Arc::new(restore_absent_column_statistics(
            child_stats.as_ref().clone(),
            overlay,
        )))
    }
    }

    // Allow optimizer to push limits through to inputs
    fn supports_limit_pushdown(&self) -> bool {
        self.inner.supports_limit_pushdown()
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        self.inner
            .with_fetch(limit)
            .map(|plan| Arc::new(self.wrap_rewritten_child(plan)) as Arc<dyn ExecutionPlan>)
    }

    fn fetch(&self) -> Option<usize> {
        self.inner.fetch()
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }

    fn try_swapping_with_projection(
        &self,
        projection: &ProjectionExec,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        self.inner
            .try_swapping_with_projection(projection)
            .map(|plan| {
                plan.map(|plan| Arc::new(self.wrap_rewritten_child(plan)) as Arc<dyn ExecutionPlan>)
            })
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
        Ok(result.map(|plan| Arc::new(self.wrap_rewritten_child(plan)) as Arc<dyn ExecutionPlan>))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::record_batch::RecordBatch;
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::physical_plan::expressions::col;

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
                .downcast_ref::<RepartitionExec>()
                .is_some()
        );
    }

    #[test]
    fn cayenne_exec_does_not_request_input_repartitioning() {
        let exec = CayenneAccelerationExec::new(one_partition_plan());

        assert_eq!(exec.benefits_from_input_partitioning(), vec![false]);
        assert!(
            exec.repartitioned(4, &ConfigOptions::default())
                .expect("repartition check should succeed")
                .is_none()
        );
    }

    #[test]
    fn cayenne_exec_delegates_projection_swapping_to_inner_plan() {
        let plan = one_partition_plan();
        let projection_expr = col("id", &plan.schema()).expect("id column should exist");
        let projection =
            ProjectionExec::try_new(vec![(projection_expr, "id".to_string())], Arc::clone(&plan))
                .expect("projection exec should be created");
        let exec = CayenneAccelerationExec::new(plan);

        let swapped = exec
            .try_swapping_with_projection(&projection)
            .expect("projection swap should be attempted")
            .expect("inner plan should support projection swapping");

        assert!(
            swapped.downcast_ref::<CayenneAccelerationExec>().is_some(),
            "projection-swapped Cayenne plan should stay wrapped for optimizer identification"
        );
    }

    #[test]
    fn scan_identity_returns_none_for_non_file_data_source() {
        // MemorySourceConfig is not a FileScanConfig, so scan_identity must
        // return None rather than misattributing identity.
        let exec = CayenneAccelerationExec::new(one_partition_plan());
        assert!(exec.scan_identity().is_none());
    }

    /// Frozen/clean partition: a scan with no deletion filter (the wrapper is
    /// applied directly over the file scan) must surface the inner plan's
    /// `Exact` per-partition statistics unchanged — this is the join-side
    /// selection / pruning signal that the deletion-filter path deliberately
    /// relaxes to `Inexact` only when deletions are present.
    #[test]
    fn cayenne_exec_passes_through_exact_partition_statistics() {
        use datafusion_common::stats::Precision;

        let exec = CayenneAccelerationExec::new(one_partition_plan());
        let stats = exec
            .partition_statistics(Some(0))
            .expect("partition statistics should be available");
        assert_eq!(
            stats.num_rows,
            Precision::Exact(3),
            "clean scan must keep the inner plan's exact row count"
        );
        // Aggregate over all partitions must likewise stay exact.
        let agg = exec
            .partition_statistics(None)
            .expect("aggregate statistics should be available");
        assert_eq!(agg.num_rows, Precision::Exact(3));
    }

    #[test]
    fn scan_identity_returns_none_when_inner_wraps_unknown_multi_child_plan() {
        // A plan with multiple children (e.g. a join) cannot have a single
        // unambiguous scan identity; find_data_source_exec must bail.
        let left = one_partition_plan();
        let right = one_partition_plan();
        let schema = left.schema();
        let projection_expr = col("id", &schema).expect("id column should exist");

        // Construct a 2-child wrapper via UnionExec to exercise the
        // `children.len() != 1` early return without depending on join wiring.
        let union = datafusion::physical_plan::union::UnionExec::try_new(vec![left, right])
            .expect("union exec should be created");

        // Wrap in a projection so the top isn't a DataSourceExec.
        let projection = ProjectionExec::try_new(vec![(projection_expr, "id".to_string())], union)
            .expect("projection exec should be created");
        let exec = CayenneAccelerationExec::new(Arc::new(projection));
        assert!(exec.scan_identity().is_none());
    }

    #[test]
    fn scan_identity_equality_and_hashing_are_path_based() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let a = ScanIdentity {
            object_store_url: Arc::from("s3://bucket/"),
            paths: Arc::from(vec!["a.parquet".to_string(), "b.parquet".to_string()]),
        };
        let b = ScanIdentity {
            object_store_url: Arc::from("s3://bucket/"),
            paths: Arc::from(vec!["a.parquet".to_string(), "b.parquet".to_string()]),
        };
        let c = ScanIdentity {
            object_store_url: Arc::from("s3://bucket/"),
            paths: Arc::from(vec!["a.parquet".to_string()]),
        };

        assert_eq!(a, b, "same path set must compare equal");
        assert_ne!(a, c, "different path sets must not compare equal");

        let mut ha = DefaultHasher::new();
        a.hash(&mut ha);
        let mut hb = DefaultHasher::new();
        b.hash(&mut hb);
        // Verify Hash compiles and is content-based (we don't assert exact
        // equality of finish() between distinct hashers, but both use the
        // same content; the trait must be derivable from the inner fields).
        let _ = (ha.finish(), hb.finish());

        assert_eq!(a.object_store_url.as_ref(), "s3://bucket/");
        assert_eq!(a.paths.as_ref(), &["a.parquet", "b.parquet"]);
    }

    #[test]
    fn scan_identity_does_not_collide_across_object_stores() {
        // Same relative paths across two different stores must produce
        // distinct identities — otherwise cross-scan dynamic filters could
        // mistakenly share state across unrelated tables.
        let bucket_a = ScanIdentity {
            object_store_url: Arc::from("s3://bucket-a/"),
            paths: Arc::from(vec!["part-000.vortex".to_string()]),
        };
        let bucket_b = ScanIdentity {
            object_store_url: Arc::from("s3://bucket-b/"),
            paths: Arc::from(vec!["part-000.vortex".to_string()]),
        };
        assert_ne!(bucket_a, bucket_b);
    }

    /// The base+delta `UnionExec` wipes a join key's min/max + distinct_count to
    /// `Precision::Absent` (an empty delta branch poisons `col_stats_union`).
    /// With an optimizer overlay attached, the wrapper must refill exactly those
    /// Absent fields while preserving the child's (filter-aware) `num_rows`.
    #[test]
    fn overlay_refills_union_wiped_join_key_statistics() {
        use datafusion_common::{ColumnStatistics, ScalarValue};
        use datafusion_physical_plan::empty::EmptyExec;

        let memory = one_partition_plan();
        let schema = memory.schema();
        let empty = Arc::new(EmptyExec::new(Arc::clone(&schema))) as Arc<dyn ExecutionPlan>;
        let union: Arc<dyn ExecutionPlan> =
            UnionExec::try_new(vec![memory, empty]).expect("union exec should be created");

        // Sanity: the union poisons min/max + distinct_count to Absent.
        let poisoned = union
            .partition_statistics(None)
            .expect("union statistics should be available");
        assert!(matches!(
            poisoned.column_statistics[0].min_value,
            Precision::Absent
        ));
        assert!(matches!(
            poisoned.column_statistics[0].max_value,
            Precision::Absent
        ));
        assert!(matches!(
            poisoned.column_statistics[0].distinct_count,
            Precision::Absent
        ));

        let overlay = Arc::new(Statistics {
            num_rows: Precision::Absent,
            total_byte_size: Precision::Absent,
            column_statistics: vec![ColumnStatistics {
                null_count: Precision::Absent,
                max_value: Precision::Inexact(ScalarValue::Int64(Some(9999))),
                min_value: Precision::Inexact(ScalarValue::Int64(Some(1))),
                sum_value: Precision::Absent,
                distinct_count: Precision::Inexact(42),
                byte_size: Precision::Absent,
            }],
        });

        // Without an overlay: poisoned stats pass through unchanged.
        let plain = CayenneAccelerationExec::new(Arc::clone(&union));
        let plain_stats = plain
            .partition_statistics(None)
            .expect("statistics should be available");
        assert!(matches!(
            plain_stats.column_statistics[0].min_value,
            Precision::Absent
        ));
        assert!(matches!(
            plain_stats.column_statistics[0].distinct_count,
            Precision::Absent
        ));

        // With an overlay: the Absent join-key fields are refilled, num_rows kept.
        let restored_exec = CayenneAccelerationExec::new(Arc::clone(&union))
            .with_optimizer_column_overlay(Some(overlay));
        let restored = restored_exec
            .partition_statistics(None)
            .expect("statistics should be available");
        let col = &restored.column_statistics[0];
        assert_eq!(
            col.min_value,
            Precision::Inexact(ScalarValue::Int64(Some(1)))
        );
        assert_eq!(
            col.max_value,
            Precision::Inexact(ScalarValue::Int64(Some(9999)))
        );
        assert_eq!(col.distinct_count, Precision::Inexact(42));
        assert_eq!(
            restored.num_rows, poisoned.num_rows,
            "overlay must not override the child's filter-aware num_rows"
        );

        // The overlay is a per-table (global) aggregate, so it must NOT be
        // applied to per-partition stats: `partition_statistics(Some(_))` must
        // return the child's partition stats untouched.
        let per_partition = restored_exec
            .partition_statistics(Some(0))
            .expect("per-partition statistics should be available");
        let child_partition = union
            .partition_statistics(Some(0))
            .expect("child per-partition statistics should be available");
        assert_eq!(
            per_partition.column_statistics[0].min_value,
            child_partition.column_statistics[0].min_value,
            "overlay must not leak into per-partition min_value"
        );
        assert_eq!(
            per_partition.column_statistics[0].max_value,
            child_partition.column_statistics[0].max_value,
            "overlay must not leak into per-partition max_value"
        );
        assert_eq!(
            per_partition.column_statistics[0].distinct_count,
            child_partition.column_statistics[0].distinct_count,
            "overlay must not leak into per-partition distinct_count"
        );
    }

    /// The restore must never override a statistic the child already provides —
    /// only `Precision::Absent` fields are filled from the overlay.
    #[test]
    fn restore_preserves_present_child_statistics() {
        use datafusion_common::{ColumnStatistics, ScalarValue};

        let child = Statistics {
            num_rows: Precision::Exact(100),
            total_byte_size: Precision::Absent,
            column_statistics: vec![ColumnStatistics {
                null_count: Precision::Exact(0),
                max_value: Precision::Exact(ScalarValue::Int64(Some(7))),
                min_value: Precision::Absent,
                sum_value: Precision::Absent,
                distinct_count: Precision::Absent,
                byte_size: Precision::Absent,
            }],
        };
        let overlay = Statistics {
            num_rows: Precision::Absent,
            total_byte_size: Precision::Absent,
            column_statistics: vec![ColumnStatistics {
                null_count: Precision::Absent,
                max_value: Precision::Inexact(ScalarValue::Int64(Some(999))),
                min_value: Precision::Inexact(ScalarValue::Int64(Some(-5))),
                sum_value: Precision::Absent,
                distinct_count: Precision::Inexact(50),
                byte_size: Precision::Absent,
            }],
        };

        let restored = restore_absent_column_statistics(child, &overlay);
        let col = &restored.column_statistics[0];
        // Present max_value must be kept (not overwritten by the overlay).
        assert_eq!(col.max_value, Precision::Exact(ScalarValue::Int64(Some(7))));
        // Absent min_value / distinct_count are filled from the overlay.
        assert_eq!(
            col.min_value,
            Precision::Inexact(ScalarValue::Int64(Some(-5)))
        );
        assert_eq!(col.distinct_count, Precision::Inexact(50));
        assert_eq!(restored.num_rows, Precision::Exact(100));
    }
}
