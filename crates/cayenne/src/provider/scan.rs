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
use datafusion_execution::memory_pool::{
    MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation,
};
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
    /// The dataset this scan reads, carried only so a query-pool refusal can
    /// name it (see [`scan_memory_refusal`]). `None` on plans built outside the
    /// table provider's `scan()`, which costs the message a name and nothing
    /// else — no execution behaviour reads this.
    table_name: Option<Arc<str>>,
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
            table_name: None,
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
            table_name: None,
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
            table_name: None,
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
            table_name: None,
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

    /// Records the dataset this scan reads, so a query-pool refusal can name it
    /// rather than leaving an operator to work out which of their datasets ran
    /// out of memory. Diagnostic only — nothing on the execution path reads it.
    #[must_use]
    pub(crate) fn with_table_name(mut self, table_name: impl Into<Arc<str>>) -> Self {
        self.table_name = Some(table_name.into());
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
            table_name: self.table_name.clone(),
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

    /// Returns `true` if any underlying file source carries a pushed-down
    /// filter — a static predicate or a dynamic join filter.
    ///
    /// Whole-table statistics (`num_rows`, per-column `sum`/`min`/`max`) describe
    /// the *unfiltered* file, so a stats-based aggregate fold
    /// ([`crate::stats_aggregate`]) must decline whenever a filter is present:
    /// the metadata cannot answer an aggregate restricted to a row subset.
    #[must_use]
    pub(crate) fn has_pushed_filter(&self) -> bool {
        plan_has_pushed_filter(&self.inner)
    }

    /// Like [`Self::has_pushed_filter`] but detects a predicate pushed onto a file
    /// source ANYWHERE in the wrapped plan — including below a deletion-filter exec
    /// on a merge-on-read table (which [`Self::has_pushed_filter`]'s shallow walk
    /// stops above). The maintained-aggregate rewrite's soundness guard uses this:
    /// a maintained view answers the unfiltered relation, so it must decline when a
    /// query predicate has narrowed the scan — even when a pending-tombstone
    /// deletion-filter exec sits between the scan wrapper and the source.
    #[must_use]
    pub(crate) fn has_pushed_filter_deep(&self) -> bool {
        plan_has_pushed_filter_deep(&self.inner)
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

/// Refills the `Precision::Absent` `num_rows` and per-column `distinct_count`
/// (NDV) in `child` from `overlay`, restoring the table size and join-key NDV
/// that the Cayenne base+delta `UnionExec` drops. A stat-less branch (e.g. a
/// `collect_stats=false` scan during pending deletions) makes the union return
/// `num_rows = Absent` — the `(Absent, _) => Absent` rule discards the known
/// per-branch counts — and makes `col_stats_union` drop NDV.
///
/// `UnionExec` can't fix this itself: an `Absent` branch means *unknown*, not
/// *empty*, so it must drop the NDV, which isn't additive across branches of
/// unknown overlap. The repair belongs here because only the Cayenne scan owns
/// the out-of-band metadata the union can't see — the incrementally-maintained
/// per-table aggregate (HLL-derived integer NDV over base+delta).
///
/// Deliberately does NOT restore min/max. A range predicate against an exact
/// bound — an append-refresh `ts > watermark` (watermark == the column max) or
/// a retention `ts < cutoff` — makes `DataFusion`'s interval analysis build an
/// empty interval `[max+1, max]`; casting it back trips `Interval::try_new`'s
/// `lower <= upper` assertion (`Err`), failing the query. Build-side selection
/// reads only `total_byte_size`/`num_rows`, and join-cardinality estimation
/// prefers NDV when present, so the restored NDV is what carries the estimate —
/// min/max are not needed here and only re-introduce the assertion hazard.
///
/// The refilled NDV is capped at the child's `num_rows`: a column cannot have
/// more distinct values than it has rows. The overlay NDV is the whole-table HLL
/// aggregate, which only ever GROWS — it is never shrunk on a delete, an
/// upsert-supersede, or a retention drop, and is not filter-aware — so on a
/// churned or selectively-filtered scan it can exceed the child's live row count.
/// Without the cap that never-shrink over-count would inflate the per-key NDV and
/// mis-drive `estimate_inner_join_cardinality`.
fn restore_absent_column_statistics(mut child: Statistics, overlay: &Statistics) -> Statistics {
    if child.column_statistics.len() != overlay.column_statistics.len() {
        return child;
    }
    // Backfill an Absent num_rows from the overlay's maintained whole-table count
    if matches!(child.num_rows, Precision::Absent)
        && matches!(overlay.num_rows, Precision::Exact(n) | Precision::Inexact(n) if n > 0)
    {
        child.num_rows = overlay.num_rows;
    }
    let child_num_rows = child.num_rows;
    for (col, src) in child
        .column_statistics
        .iter_mut()
        .zip(overlay.column_statistics.iter())
    {
        if matches!(col.distinct_count, Precision::Absent) {
            col.distinct_count = cap_distinct_count_at_rows(src.distinct_count, child_num_rows);
        }
    }
    child
}

/// Clamp an NDV (`distinct_count`) to a row count — a column cannot have more
/// distinct values than it has rows. Returns the NDV unchanged when either side
/// is unknown or it is already within bounds; a clamped value is `Inexact`
/// (it is a derived bound, not a measured count).
fn cap_distinct_count_at_rows(
    distinct_count: Precision<usize>,
    num_rows: Precision<usize>,
) -> Precision<usize> {
    match (distinct_count.get_value(), num_rows.get_value()) {
        (Some(&ndv), Some(&rows)) if ndv > rows => Precision::Inexact(rows),
        _ => distinct_count,
    }
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

/// Whether any file source reachable from `plan` carries a pushed-down filter (a
/// static predicate or a dynamic join filter). Reachability is exactly what
/// [`file_scan_configs`] walks — it descends only identity-preserving wrappers
/// and `UnionExec` (the Cayenne base+delta shape), not arbitrary intermediate
/// operators — so a filter buried under a non-passthrough node is not detected.
/// That matches the intended callers, whose `FileScanConfig`s sit directly under
/// such wrappers: the file-metadata `num_rows` describes the *unfiltered* file,
/// so a consumer reasoning about a scan's live row count — e.g. the deletion
/// filter's delete-aware `num_rows`, which subtracts a whole-table deletion count
/// — must not treat a filtered scan's (subset) count as the whole-table count.
/// Reused by [`CayenneAccelerationExec::has_pushed_filter`] and the
/// deletion-filter execs.
pub(crate) fn plan_has_pushed_filter(plan: &Arc<dyn ExecutionPlan>) -> bool {
    file_scan_configs(plan)
        .iter()
        .any(|config| config.file_source().filter().is_some())
}

/// Like [`plan_has_pushed_filter`] but walks the ENTIRE subtree (every descendant,
/// not just the identity-preserving whitelist), so a query predicate pushed onto a
/// file source BELOW a non-passthrough operator is still detected. The critical
/// case is a merge-on-read table with pending tombstones: `scan()` wraps the Vortex
/// `DataSourceExec` in a deletion-filter exec (which is NOT identity-preserving, so
/// [`plan_has_pushed_filter`] stops above it), and a Vortex-convertible `WHERE` is
/// pushed THROUGH that exec onto the source. The aggregate-rewrite soundness guard
/// must see that predicate — otherwise a maintained / whole-file aggregate silently
/// serves the unfiltered relation for a filtered query. Over-detection is sound for
/// that guard: it only ever causes a decline (the real scan+aggregate runs).
/// Distinct from [`plan_has_pushed_filter`], which is intentionally shallow because
/// the deletion-filter exec's delete-aware `num_rows` math must NOT see a filtered
/// (subset) count as a whole-table count.
pub(crate) fn plan_has_pushed_filter_deep(plan: &Arc<dyn ExecutionPlan>) -> bool {
    if let Some(data_source_exec) = plan.downcast_ref::<DataSourceExec>() {
        return data_source_exec
            .data_source()
            .downcast_ref::<FileScanConfig>()
            .is_some_and(|config| config.file_source().filter().is_some());
    }
    for child in plan.children() {
        if plan_has_pushed_filter_deep(child) {
            return true;
        }
    }
    false
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

/// Whether executing `plan` decodes any file at all — i.e. whether it reaches a
/// file-backed source that has files to read.
///
/// Gates the scan's memory accounting. A Cayenne scan unions its file branches
/// with `MemorySourceConfig` branches for the durable inline corpus and the
/// in-RAM CDC tier, and those memory branches yield batches that are already
/// resident and already accounted: `mem_tier_budget` mirrors the tier's bytes
/// into this same query pool under the `cayenne:mem_tier` consumer. Charging
/// them again reserves for memory no scan allocated, which can refuse a query
/// on bytes the pool has already been told about.
///
/// So a scan with nothing file-backed to decode is not charged at all. That is
/// not a corner case: a `mode: memory` table keeps all of its data in the RAM
/// mem-tier, and `mode` defaults to `memory`. A plan that mixes the two still
/// charges both; see the type docs on [`MemoryAccountedScanStream`].
fn plan_materializes_files(plan: &Arc<dyn ExecutionPlan>) -> bool {
    let (_snapshots, files) = count_file_scan_sources(plan);
    files > 0
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

    if plan.is::<UnionExec>() {
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
    if plan.is::<ProjectionExec>()
        || plan.is::<RepartitionExec>()
        || plan.is::<datafusion_physical_plan::coalesce_batches::CoalesceBatchesExec>()
        || plan.is::<datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec>()
        || plan.is::<CayenneAccelerationExec>()
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

    let is_union = plan.is::<UnionExec>();
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
        // Cloned rather than moved: the memory-accounted wrapper below needs the
        // pool from this same context. `Arc::clone` is a refcount bump.
        let stream = self.inner.execute(partition, Arc::clone(&context))?;
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
        // Charge this scan's canonicalized batches against the query pool. Two
        // conditions gate it:
        //
        // - Only the outermost wrapper accounts (`scan_guard.is_some()`): the
        //   inner per-snapshot wrappers feed into this same stream, and
        //   registering a consumer at every layer would count one batch once per
        //   layer.
        // - Only a plan that decodes files accounts: a purely memory-backed scan
        //   yields already-resident bytes that `cayenne:mem_tier` has already
        //   mirrored into this pool (see `plan_materializes_files`).
        if self.scan_guard.is_some() && plan_materializes_files(&self.inner) {
            let accounted = MemoryAccountedScanStream::new(
                Box::pin(RecordBatchStreamAdapter::new(Arc::clone(&schema), mapped)),
                schema,
                self.table_name.clone(),
                partition,
                &context,
            );
            return Ok(Box::pin(accounted));
        }
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
            Arc::unwrap_or_clone(child_stats),
            overlay,
        )))
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

/// Charges a scan's materialized batches against the `DataFusion` memory pool.
///
/// Reading Cayenne means canonicalizing Vortex's compressed encodings (`RunEnd`,
/// Constant, dictionary) into flat Arrow, and that expansion is the single
/// largest allocator in the process at scale: a SF-1000 heap profile put ~50 GiB
/// under `vortex_buffer::BufferMut::with_capacity_preferred_aligned`, reached
/// through `to_arrow_struct`, `canonical::execute`, `to_arrow_primitive`,
/// `runend_decode_primitive` and friends.
///
/// None of it was accounted. `DataFusion`'s operators reserve for what they hold
/// and Cayenne's `memory_account` covers long-lived resident state (the PK keyset
/// and deletion indexes, explicitly *outside* query execution), but the
/// materialization in between reserved from nothing. So the query pool could sit
/// at its limit and spill while the scan beneath it kept allocating, and
/// `runtime.query.memory_limit` did not bound the process: measured peak RSS
/// tracked the cgroup cap (95.8 GiB at 96G, 109.8 GiB at 110G) and was unmoved by
/// concurrency or by the tuning mode.
///
/// The charge is taken **before** each poll and held across it, then settled to
/// the measured size once the batch exists. Charging afterwards would be a
/// detector rather than a bound: the decode has already run, so a refusal could
/// only reject the *next* batch, never the one that exceeded the budget. Holding
/// it across a `Pending` matters for the same reason — that is the window the
/// buffers are being expanded in.
///
/// The reservation then covers the batch **in flight** until the next poll,
/// rather than its whole downstream lifetime, which this stream cannot observe.
/// Whatever an operator above retains, it reserves for itself.
///
/// Over budget returns `ResourcesExhausted` before the decode runs, so a scan
/// that cannot fit fails without allocating. That is a deliberate behaviour
/// change: a query that used to drift toward an OOM kill now errors — so the
/// refusal has to say which dataset it was, what it needed, and which of the
/// two remedies applies (raise the limit, or run against less concurrent work).
/// [`scan_memory_refusal`] builds that message and logs it.
///
/// # What this does NOT bound
///
/// Accounting attaches to the outermost wrapper only (`scan_guard.is_some()`),
/// so it charges one estimate per output partition. Under a base+delta plan the
/// inner per-snapshot wrappers and the Vortex `DataSourceExec` beneath them can
/// decode CONCURRENTLY while the outer stream holds a single charge, which
/// under-counts by roughly the number of concurrently decoding children. The
/// pre-poll charge means that window is no longer *unaccounted*, but it is not
/// accurately accounted either.
///
/// Bounding it properly means charging at each materializing leaf, or gating
/// concurrent decodes behind a shared budget. Both are larger changes than this,
/// and neither should be inferred from the presence of this type: a plan whose
/// memory is dominated by fan-out beneath the outermost scan is still able to
/// exceed `runtime.query.memory_limit`.
///
/// In the other direction, a mixed plan over-counts its memory branches. The
/// gate that installs this stream is whole-plan ([`plan_materializes_files`]),
/// so a union that decodes files AND serves the RAM tier accounts for both, and
/// each in-flight mem-tier batch is charged here as well as by
/// `cayenne:mem_tier`. The overlap is one in-flight batch per partition against
/// the tier's whole resident size, so it is small next to the mirror it doubles
/// — but it is an over-charge, and an over-charge fails a query that would have
/// fit. Charging at the materializing leaf removes it, along with the
/// under-count above; that is the same follow-up, not a second one.
struct MemoryAccountedScanStream<S> {
    inner: S,
    schema: SchemaRef,
    /// Always `Some` in practice — `MemoryConsumer::register` is infallible, so
    /// there is no unaccounted path. `Option` only so the accounting can be
    /// skipped wholesale in a future caller (or a test) without threading a
    /// second flag through the poll loop.
    reservation: Option<MemoryReservation>,
    /// What to charge BEFORE a poll, since the batch's real size is unknowable
    /// until the decode that allocates it has already run. Adapted upward to the
    /// largest batch this stream has produced.
    ///
    /// A running max, not the last size: under-reserving is the failure this
    /// exists to prevent, and batch widths vary run to run, so the estimate
    /// converges upward and stays there. Over-reserving costs pool headroom;
    /// under-reserving costs the guarantee.
    estimate: usize,
    /// True while the reservation covers an in-progress decode rather than a
    /// batch already handed downstream. Keeps the charge in place across a
    /// `Pending`, which is exactly when Vortex is expanding buffers.
    decode_charged: bool,
    /// The dataset being scanned, and which of its output partitions this
    /// stream is. Diagnostic only: a refusal that cannot name the dataset
    /// leaves an operator to guess which one exhausted the pool.
    table_name: Option<Arc<str>>,
    partition: usize,
    /// The pool this stream reserves from, kept so a refusal can read back the
    /// limit and what else is holding it. `MemoryReservation` does not expose
    /// its pool, and those two numbers are what separate "this scan can never
    /// fit" from "this scan cannot fit right now".
    pool: Arc<dyn MemoryPool>,
}

/// Explains a query-pool refusal in terms an operator can act on, and logs it.
///
/// The pool's own error reports the reservation name and the byte counts, but
/// not which dataset asked for them and not what to change. The two ways a scan
/// can be refused also want opposite advice, and the pool's limit is what
/// separates them:
///
/// - `needed > limit` — the batch does not fit even an empty pool, so waiting or
///   shedding concurrent queries changes nothing; the limit itself has to move,
///   or the query has to read narrower batches.
/// - `needed <= limit` — it fits on its own but not beside what is reserved right
///   now, so draining concurrent work is a real remedy.
///
/// The pool counters are read after the failure, so they are a snapshot of a
/// moving value rather than the exact state at the moment of refusal — close
/// enough to tell an operator which of the two situations they are in, which is
/// all they are used for.
///
/// Call this only once the caller's own reservation has been released, so
/// `pool.reserved()` is exactly what OTHER work holds. Both call sites free
/// before returning the error, which is also what makes `needed` the batch's
/// whole size rather than a delta over a charge still standing.
fn scan_memory_refusal(
    table_name: Option<&str>,
    partition: usize,
    needed: usize,
    pool: &Arc<dyn MemoryPool>,
    source: &DataFusionError,
) -> DataFusionError {
    // What a retry against a quieter runtime would get back.
    let others = pool.reserved();
    let needed_h = util::human_readable_bytes(needed);
    let others_h = util::human_readable_bytes(others);

    let detail = match pool.memory_limit() {
        MemoryLimit::Finite(limit) if needed > limit => {
            let limit_h = util::human_readable_bytes(limit);
            format!(
                "Reading one batch of it needs {needed_h}, more than the entire {limit_h} query memory pool, so this scan cannot run at this limit however idle the runtime is. \
                 Raise runtime.query.memory_limit above {needed_h}, or read narrower batches by selecting fewer columns or filtering more selectively."
            )
        }
        MemoryLimit::Finite(limit) => {
            let limit_h = util::human_readable_bytes(limit);
            let available_h = util::human_readable_bytes(limit.saturating_sub(others));
            format!(
                "Reading one batch of it needs {needed_h}, which fits the {limit_h} query memory pool on its own but not alongside the {others_h} other queries are holding right now ({available_h} free). \
                 Retry when the runtime is less busy, lower the query concurrency, or raise runtime.query.memory_limit."
            )
        }
        MemoryLimit::Infinite | MemoryLimit::Unknown => format!(
            "Reading one batch of it needs {needed_h}, and the query memory pool refused it with {others_h} held by other queries running now. \
             Retry when the runtime is less busy, lower the query concurrency, or raise runtime.query.memory_limit."
        ),
    };

    let message = match table_name {
        Some(name) => format!(
            "Failed to scan dataset {name} (cayenne): Out of query memory. {detail} For details, visit: https://spiceai.org/docs/reference/memory"
        ),
        None => format!(
            "Failed to scan a Cayenne-accelerated dataset: Out of query memory. {detail} For details, visit: https://spiceai.org/docs/reference/memory"
        ),
    };

    // The error reaches whoever ran the query; this reaches whoever operates the
    // runtime, who is the one who can act on the limit. `source` carries the
    // pool's own byte-level accounting and stays out of the user-facing text.
    tracing::warn!(
        table = table_name.unwrap_or("unknown"),
        partition,
        needed_bytes = needed,
        held_by_others_bytes = others,
        error = %source,
        "{message}"
    );

    DataFusionError::ResourcesExhausted(message)
}

/// First-poll charge, before any batch has been measured.
///
/// Deliberately small. The charge has to be paid before the first batch's size
/// can be known, so an estimate that is too large refuses scans that would have
/// fit — a pool sized for a handful of narrow batches should not be rejected
/// because the guess was megabytes. It is equally deliberately not zero: a zero
/// first charge would reopen, for one batch per partition, exactly the hole this
/// type exists to close.
///
/// So the exposure is bounded and explicit: until the running max converges
/// (from the second batch on), a scan can decode one batch per partition against
/// this charge rather than its true size. At 1 MiB x 20 partitions that is ~20
/// MiB of slack, against the tens of GiB this bounds in steady state.
const INITIAL_BATCH_ESTIMATE_BYTES: usize = 1024 * 1024;

impl<S> MemoryAccountedScanStream<S> {
    fn new(
        inner: S,
        schema: SchemaRef,
        table_name: Option<Arc<str>>,
        partition: usize,
        context: &Arc<TaskContext>,
    ) -> Self {
        let consumer_name = match table_name.as_deref() {
            Some(table) => format!("cayenne_scan[{table}, partition={partition}]"),
            None => format!("cayenne_scan[partition={partition}]"),
        };
        let pool = Arc::clone(context.memory_pool());
        // Infallible: `register` hands back a zero-sized reservation and the
        // pool only refuses later, at `try_grow`.
        let reservation = Some(MemoryConsumer::new(consumer_name).register(&pool));
        Self {
            inner,
            schema,
            reservation,
            estimate: INITIAL_BATCH_ESTIMATE_BYTES,
            decode_charged: false,
            table_name,
            partition,
            pool,
        }
    }
}

impl<S> futures::Stream for MemoryAccountedScanStream<S>
where
    S: futures::Stream<Item = Result<arrow::record_batch::RecordBatch>> + Unpin,
{
    type Item = Result<arrow::record_batch::RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        // Charge BEFORE polling. The inner poll is where Vortex canonicalizes
        // into Arrow, so charging after it would mean the allocation has already
        // happened and a refusal could only reject the NEXT batch, never the one
        // that broke the budget — a detector, not a bound.
        if !self.decode_charged {
            // The previous batch has been handed downstream; whatever holds it
            // now reserves for itself, so keeping its bytes here would
            // double-count them.
            let estimate = self.estimate;
            let mut refused = None;
            if let Some(reservation) = self.reservation.as_mut() {
                reservation.free();
                if let Err(e) = reservation.try_grow(estimate) {
                    // Refused before the decode runs, so this returns without
                    // having allocated the batch. `free()` above means this
                    // stream now holds nothing, so everything reserved belongs
                    // to other work.
                    refused = Some(e);
                }
            }
            if let Some(e) = refused {
                return std::task::Poll::Ready(Some(Err(scan_memory_refusal(
                    self.table_name.as_deref(),
                    self.partition,
                    estimate,
                    &self.pool,
                    &e,
                ))));
            }
            self.decode_charged = true;
        }

        let polled = std::pin::Pin::new(&mut self.inner).poll_next(cx);
        match &polled {
            // Decode still in progress: hold the charge across it. This is the
            // window the buffers are actually being expanded in.
            std::task::Poll::Pending => {}
            std::task::Poll::Ready(Some(Ok(batch))) => {
                // `get_array_memory_size` counts the buffers this batch actually
                // holds — the expanded Arrow form, not the compressed on-disk
                // size. Settle the estimate to the truth now that it is known.
                let actual = batch.get_array_memory_size();
                self.estimate = self.estimate.max(actual);
                let mut refused = None;
                if let Some(reservation) = self.reservation.as_mut()
                    && let Err(e) = reservation.try_resize(actual)
                {
                    // Settling failed, so this batch is dropped with the error.
                    // Release its charge and clear the flag: leaving the flag set
                    // would make the next poll skip both the free and the
                    // pre-charge, so a stream that is polled again after an error
                    // would decode against a reservation held for a batch that no
                    // longer exists. Most consumers abort on first error, but the
                    // accounting must not depend on that. Freeing first is also
                    // what lets the refusal below attribute the whole remaining
                    // reservation to other work.
                    reservation.free();
                    self.decode_charged = false;
                    refused = Some(e);
                }
                if let Some(e) = refused {
                    return std::task::Poll::Ready(Some(Err(scan_memory_refusal(
                        self.table_name.as_deref(),
                        self.partition,
                        actual,
                        &self.pool,
                        &e,
                    ))));
                }
                // The charge now covers the in-flight batch; the next poll
                // releases it and re-charges for the following decode.
                self.decode_charged = false;
            }
            std::task::Poll::Ready(Some(Err(_)) | None) => {
                if let Some(reservation) = self.reservation.as_mut() {
                    reservation.free();
                }
                self.decode_charged = false;
            }
        }
        polled
    }
}

impl<S> datafusion::physical_plan::RecordBatchStream for MemoryAccountedScanStream<S>
where
    S: futures::Stream<Item = Result<arrow::record_batch::RecordBatch>> + Unpin,
{
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
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

    /// A scan must charge its canonicalized batches to the query pool, and must
    /// fail rather than exceed it.
    ///
    /// Before this, `runtime.query.memory_limit` did not bound the process:
    /// `DataFusion` operators reserved for what they held and Cayenne's
    /// `memory_account` covered long-lived resident state, but the Vortex ->
    /// Arrow materialization between them reserved from nothing. At SF-1000 that
    /// was ~50 GiB of the heap, and peak RSS tracked the cgroup cap rather than
    /// the configured limit.
    #[tokio::test]
    async fn a_scan_over_its_pool_budget_fails_instead_of_allocating() {
        use datafusion_execution::memory_pool::GreedyMemoryPool;
        use datafusion_execution::runtime_env::RuntimeEnvBuilder;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        // Comfortably smaller than one batch's Arrow footprint, so the very
        // first `try_grow` is refused.
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::new(GreedyMemoryPool::new(64)))
            .build_arc()
            .expect("runtime env");
        let context = Arc::new(TaskContext::default().with_runtime(runtime));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(
                (0..4096_i64).collect::<Vec<_>>(),
            ))],
        )
        .expect("batch");
        let inner = futures::stream::iter(vec![Ok(batch)]);

        let mut stream = MemoryAccountedScanStream::new(
            Box::pin(inner),
            Arc::clone(&schema),
            Some(Arc::from("test_table")),
            0,
            &context,
        );

        let first = stream.next().await.expect("one item");
        let err = first.expect_err("a batch larger than the pool must be refused");
        assert!(
            err.to_string().contains("Resources exhausted"),
            "expected a pool refusal, got: {err}"
        );
    }

    /// A failed settle must not leave the charge stuck.
    ///
    /// `try_resize` failing means the batch is dropped with the error. If the
    /// `decode_charged` flag stayed set, the next poll would skip BOTH the free
    /// and the pre-charge, so the following decode would run against a
    /// reservation still held for a batch that no longer exists — accounting
    /// drift in the direction that under-charges. Most consumers abort on the
    /// first error, but the accounting must not rely on that.
    #[tokio::test]
    async fn a_failed_settle_releases_the_charge_and_recharges_next_poll() {
        use datafusion_execution::memory_pool::GreedyMemoryPool;
        use datafusion_execution::runtime_env::RuntimeEnvBuilder;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let wide = || {
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int64Array::from(
                    (0..262_144_i64).collect::<Vec<_>>(),
                ))],
            )
            .expect("batch")
        };
        // Pool fits the 1 MiB pre-charge but not the settled size of this batch,
        // so `try_grow` succeeds and `try_resize` is what fails.
        let batch_bytes = wide().get_array_memory_size();
        assert!(
            batch_bytes > INITIAL_BATCH_ESTIMATE_BYTES,
            "the batch must settle larger than the pre-charge for this to exercise try_resize"
        );
        let pool: Arc<dyn datafusion_execution::memory_pool::MemoryPool> = Arc::new(
            GreedyMemoryPool::new(INITIAL_BATCH_ESTIMATE_BYTES + (batch_bytes / 2)),
        );
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::clone(&pool))
            .build_arc()
            .expect("runtime env");
        let context = Arc::new(TaskContext::default().with_runtime(runtime));

        let inner = futures::stream::iter(vec![Ok(wide()), Ok(wide())]);
        let mut stream = MemoryAccountedScanStream::new(
            Box::pin(inner),
            Arc::clone(&schema),
            Some(Arc::from("test_table")),
            0,
            &context,
        );

        let first = stream.next().await.expect("one item");
        assert!(
            first.is_err(),
            "a batch larger than the pool must fail to settle"
        );
        assert!(
            !stream.decode_charged,
            "a failed settle must clear the charge flag, or the next poll skips its pre-charge"
        );
        assert_eq!(
            pool.reserved(),
            0,
            "a failed settle must release the charge for the batch it dropped"
        );
    }

    /// The charge must land BEFORE the decode, not after it.
    ///
    /// Charging afterwards makes the guard a detector rather than a bound: the
    /// batch has already been materialized, so a refusal can only reject the
    /// next one. This asserts the inner stream is never polled when the pool
    /// cannot fit the charge — i.e. that no allocation happened.
    #[tokio::test]
    async fn an_over_budget_scan_is_refused_without_polling_the_decode() {
        use datafusion_execution::memory_pool::GreedyMemoryPool;
        use datafusion_execution::runtime_env::RuntimeEnvBuilder;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let polls = Arc::new(AtomicUsize::new(0));

        // A stream that records every poll. If the charge is taken first and
        // refused, this must never be polled at all.
        let counted = {
            let polls = Arc::clone(&polls);
            let schema = Arc::clone(&schema);
            futures::stream::poll_fn(move |_cx| {
                polls.fetch_add(1, Ordering::SeqCst);
                let batch = RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![Arc::new(Int64Array::from(vec![1_i64]))],
                )
                .expect("batch");
                std::task::Poll::Ready(Some(Ok(batch)))
            })
        };

        // Smaller than INITIAL_BATCH_ESTIMATE_BYTES, so the pre-poll charge is
        // refused on the very first poll.
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::new(GreedyMemoryPool::new(1024)))
            .build_arc()
            .expect("runtime env");
        let context = Arc::new(TaskContext::default().with_runtime(runtime));

        let mut stream = MemoryAccountedScanStream::new(
            Box::pin(counted),
            Arc::clone(&schema),
            Some(Arc::from("test_table")),
            0,
            &context,
        );

        let err = stream
            .next()
            .await
            .expect("one item")
            .expect_err("the pre-poll charge must be refused");
        assert!(
            err.to_string().contains("Resources exhausted"),
            "expected a pool refusal, got: {err}"
        );
        assert_eq!(
            polls.load(Ordering::SeqCst),
            0,
            "the decode must not run when the pool refused the charge - charging \
             after the poll would mean the batch was already materialized"
        );
    }

    /// The reservation covers the batch in flight only. Holding every batch for
    /// the stream's lifetime would double-count against whichever operator above
    /// now owns it, and would make a long scan look like a leak.
    #[tokio::test]
    async fn a_scan_releases_each_batch_before_taking_the_next() {
        use datafusion_execution::memory_pool::GreedyMemoryPool;
        use datafusion_execution::runtime_env::RuntimeEnvBuilder;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = || {
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int64Array::from(
                    (0..1024_i64).collect::<Vec<_>>(),
                ))],
            )
            .expect("batch")
        };
        let one_batch_bytes = batch().get_array_memory_size();

        // Sized for the pre-poll charge plus a batch, and no more. Ten batches
        // are streamed through it: an in-flight-only reservation fits, a
        // cumulative one is refused partway through. The pre-charge is the floor
        // here, not the batch size — these batches are far smaller than it.
        let pool: Arc<dyn datafusion_execution::memory_pool::MemoryPool> = Arc::new(
            GreedyMemoryPool::new(INITIAL_BATCH_ESTIMATE_BYTES + one_batch_bytes),
        );
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::clone(&pool))
            .build_arc()
            .expect("runtime env");
        let context = Arc::new(TaskContext::default().with_runtime(runtime));

        let inner = futures::stream::iter((0..10).map(|_| Ok(batch())));
        let mut stream = MemoryAccountedScanStream::new(
            Box::pin(inner),
            Arc::clone(&schema),
            Some(Arc::from("test_table")),
            0,
            &context,
        );

        let mut seen = 0;
        while let Some(item) = stream.next().await {
            item.expect("an in-flight-only reservation fits a pool sized for two batches");
            seen += 1;
        }
        assert_eq!(seen, 10, "every batch should stream through");

        drop(stream);
        assert_eq!(
            pool.reserved(),
            0,
            "dropping the stream must return its bytes to the pool"
        );
    }

    /// A purely memory-backed scan must not be charged at all.
    ///
    /// The RAM CDC tier and the durable inline corpus reach a scan as
    /// `MemorySourceConfig` branches, and their batches are already resident:
    /// `mem_tier_budget` mirrors the tier's bytes into this same query pool under
    /// the `cayenne:mem_tier` consumer. Charging them here as well reserves for
    /// memory that no scan allocated — and unlike the under-count beneath the
    /// outermost wrapper, an over-charge refuses a query that would have fit.
    ///
    /// The pool here is smaller than the pre-poll charge, so any accounting at
    /// all fails the scan.
    #[tokio::test]
    async fn a_memory_only_scan_is_not_charged_to_the_query_pool() {
        use datafusion_execution::memory_pool::GreedyMemoryPool;
        use datafusion_execution::runtime_env::RuntimeEnvBuilder;

        const POOL_BYTES: usize = 1024;
        const {
            assert!(
                INITIAL_BATCH_ESTIMATE_BYTES > POOL_BYTES,
                "the pool must be smaller than the pre-poll charge for this to prove anything"
            );
        }
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(POOL_BYTES));
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::clone(&pool))
            .build_arc()
            .expect("runtime env");
        let context = Arc::new(TaskContext::default().with_runtime(runtime));

        // `with_guard` marks this the outermost wrapper, so only the file-backed
        // gate can hold the accounting off.
        let guard = SnapshotScanRef::new(Arc::new(Mutex::new(HashMap::new())), Vec::new());
        let exec = CayenneAccelerationExec::with_guard(one_partition_plan(), guard);

        let mut stream = exec
            .execute(0, context)
            .expect("a memory-only scan should execute");
        let mut rows = 0;
        while let Some(item) = stream.next().await {
            rows += item
                .expect("a memory-only scan must not be refused by the pool")
                .num_rows();
        }

        assert_eq!(rows, 3, "every row should stream through");
        assert_eq!(
            pool.reserved(),
            0,
            "a memory-only scan must not reserve - cayenne:mem_tier already mirrors those bytes"
        );
    }

    /// A refusal must name the dataset and tell the operator which remedy
    /// applies. "Resources exhausted" alone leaves them with neither: not which
    /// of their datasets ran out, and not whether raising
    /// `runtime.query.memory_limit` or shedding concurrent queries is the fix.
    ///
    /// This is the batch-does-not-fit-at-all case: nothing else holds the pool,
    /// so no amount of waiting helps and the message must not suggest it.
    #[tokio::test]
    async fn a_batch_larger_than_the_whole_pool_says_the_limit_must_move() {
        use datafusion_execution::memory_pool::GreedyMemoryPool;
        use datafusion_execution::runtime_env::RuntimeEnvBuilder;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024));
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::clone(&pool))
            .build_arc()
            .expect("runtime env");
        let context = Arc::new(TaskContext::default().with_runtime(runtime));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(
                (0..4096_i64).collect::<Vec<_>>(),
            ))],
        )
        .expect("batch");
        let mut stream = MemoryAccountedScanStream::new(
            Box::pin(futures::stream::iter(vec![Ok(batch)])),
            Arc::clone(&schema),
            Some(Arc::from("orders")),
            3,
            &context,
        );

        let err = stream
            .next()
            .await
            .expect("one item")
            .expect_err("a batch larger than the pool must be refused")
            .to_string();

        assert!(
            err.contains("dataset orders"),
            "the refusal must name the dataset that ran out, got: {err}"
        );
        assert!(
            err.contains("runtime.query.memory_limit"),
            "the refusal must name the knob that fixes it, got: {err}"
        );
        assert!(
            err.contains("however idle the runtime is"),
            "a batch bigger than the whole pool must not be blamed on concurrent queries, got: {err}"
        );
        assert!(
            !err.contains("Retry when the runtime is less busy"),
            "retrying cannot help when the batch exceeds the whole pool, got: {err}"
        );
    }

    /// The other refusal: the batch would fit an empty pool, so what is missing
    /// is not headroom in the config but headroom right now. Waiting or reducing
    /// concurrency IS the fix here, and the message must say so — the same
    /// `ResourcesExhausted` with the opposite remedy.
    #[tokio::test]
    async fn a_batch_that_fits_alone_blames_the_concurrent_queries() {
        use datafusion_execution::memory_pool::GreedyMemoryPool;
        use datafusion_execution::runtime_env::RuntimeEnvBuilder;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        // Twice the pre-poll charge, so the estimate fits the empty pool...
        let pool: Arc<dyn MemoryPool> =
            Arc::new(GreedyMemoryPool::new(INITIAL_BATCH_ESTIMATE_BYTES * 2));
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::clone(&pool))
            .build_arc()
            .expect("runtime env");
        let context = Arc::new(TaskContext::default().with_runtime(runtime));

        // ...but another consumer holds all but a sliver of it, so the scan's
        // charge is refused for lack of room rather than lack of limit.
        let squatter = MemoryConsumer::new("another_query").register(&pool);
        squatter
            .try_grow(INITIAL_BATCH_ESTIMATE_BYTES + (INITIAL_BATCH_ESTIMATE_BYTES / 2))
            .expect("the squatter must fit, or this tests the wrong branch");

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1_i64]))],
        )
        .expect("batch");
        let mut stream = MemoryAccountedScanStream::new(
            Box::pin(futures::stream::iter(vec![Ok(batch)])),
            Arc::clone(&schema),
            Some(Arc::from("orders")),
            3,
            &context,
        );

        let err = stream
            .next()
            .await
            .expect("one item")
            .expect_err("a full pool must refuse the charge")
            .to_string();

        assert!(
            err.contains("dataset orders"),
            "the refusal must name the dataset that ran out, got: {err}"
        );
        assert!(
            err.contains("Retry when the runtime is less busy"),
            "a batch that fits the pool alone must point at the concurrent load, got: {err}"
        );
        assert!(
            !err.contains("however idle the runtime is"),
            "this batch does fit an idle runtime, so the message must not claim otherwise: {err}"
        );
    }

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
        assert!(repartitioned_plan.is::<RepartitionExec>());
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
            swapped.is::<CayenneAccelerationExec>(),
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

    /// The base+delta `UnionExec` wipes a join key's `distinct_count` to
    /// `Precision::Absent` (an empty delta branch poisons `col_stats_union`).
    /// With an optimizer overlay attached, the wrapper refills the Absent NDV
    /// while preserving the child's (filter-aware) `num_rows`; min/max are
    /// intentionally left Absent (they trip `DataFusion`'s empty-interval assertion
    /// on range filters and aren't needed by build-side selection / cardinality).
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
                // <= the 3-row union count, so the row-count cap is a no-op here;
                // the cap has its own tests (`overlay_refilled_ndv_*`).
                distinct_count: Precision::Inexact(2),
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

        // With an overlay: the Absent NDV is refilled, num_rows kept. min/max
        // are NOT restored (they trip DataFusion's empty-interval assertion on a
        // `col > max` range filter and aren't needed downstream).
        let restored_exec = CayenneAccelerationExec::new(Arc::clone(&union))
            .with_optimizer_column_overlay(Some(overlay));
        let restored = restored_exec
            .partition_statistics(None)
            .expect("statistics should be available");
        let col = &restored.column_statistics[0];
        assert!(matches!(col.min_value, Precision::Absent));
        assert!(matches!(col.max_value, Precision::Absent));
        assert_eq!(col.distinct_count, Precision::Inexact(2));
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
        // min/max are never restored (only NDV); the Absent min stays Absent.
        assert!(matches!(col.min_value, Precision::Absent));
        // Absent distinct_count is filled from the overlay.
        assert_eq!(col.distinct_count, Precision::Inexact(50));
        assert_eq!(restored.num_rows, Precision::Exact(100));
    }

    /// The base+delta `UnionExec` collapses `num_rows` to `Absent` when a branch
    /// is stat-less (e.g. `collect_stats=false` during pending deletions). The
    /// overlay's maintained whole-table count backfills it.
    #[test]
    fn restore_backfills_absent_num_rows_from_overlay() {
        use datafusion_common::ColumnStatistics;

        let child = Statistics {
            num_rows: Precision::Absent, // union collapsed the per-branch counts
            total_byte_size: Precision::Absent,
            column_statistics: vec![ColumnStatistics::new_unknown()],
        };
        let overlay = Statistics {
            num_rows: Precision::Inexact(21_420_000), // maintained table count
            total_byte_size: Precision::Absent,
            column_statistics: vec![ColumnStatistics::new_unknown()],
        };

        let restored = restore_absent_column_statistics(child, &overlay);
        assert_eq!(restored.num_rows, Precision::Inexact(21_420_000));
    }

    /// A non-positive overlay count (cold/un-seeded aggregate, or the window
    /// before the first checkpoint seeds a `cdc_durability: memory` table) carries
    /// no information and must NOT be restored: doing so mis-sizes the join and,
    /// via the NDV cap, would zero every refilled `distinct_count`. The child stays
    /// Absent — better than reporting an invalid 0.
    #[test]
    fn restore_does_not_backfill_num_rows_from_zero_overlay() {
        use datafusion_common::ColumnStatistics;

        let child = Statistics {
            num_rows: Precision::Absent,
            total_byte_size: Precision::Absent,
            column_statistics: vec![ColumnStatistics {
                null_count: Precision::Absent,
                max_value: Precision::Absent,
                min_value: Precision::Absent,
                sum_value: Precision::Absent,
                distinct_count: Precision::Absent,
                byte_size: Precision::Absent,
            }],
        };
        let overlay = Statistics {
            num_rows: Precision::Inexact(0), // un-seeded maintained count
            total_byte_size: Precision::Absent,
            column_statistics: vec![ColumnStatistics {
                null_count: Precision::Absent,
                max_value: Precision::Absent,
                min_value: Precision::Absent,
                sum_value: Precision::Absent,
                distinct_count: Precision::Inexact(98_421), // good HLL NDV
                byte_size: Precision::Absent,
            }],
        };

        let restored = restore_absent_column_statistics(child, &overlay);
        // num_rows left Absent (not restored to 0)...
        assert_eq!(restored.num_rows, Precision::Absent);
        // ...so the NDV cap does not collapse the refilled distinct_count to 0:
        // with an Absent row count the cap is a no-op and the good NDV survives.
        assert_eq!(
            restored.column_statistics[0].distinct_count,
            Precision::Inexact(98_421),
        );
    }

    /// A present (filter-aware) child `num_rows` must win over the whole-table
    /// overlay count — the overlay is only a fallback for an `Absent` count.
    #[test]
    fn restore_preserves_present_num_rows_over_overlay() {
        use datafusion_common::ColumnStatistics;

        let child = Statistics {
            num_rows: Precision::Inexact(500),
            total_byte_size: Precision::Absent,
            column_statistics: vec![ColumnStatistics::new_unknown()],
        };
        let overlay = Statistics {
            num_rows: Precision::Inexact(21_420_000),
            total_byte_size: Precision::Absent,
            column_statistics: vec![ColumnStatistics::new_unknown()],
        };

        let restored = restore_absent_column_statistics(child, &overlay);
        assert_eq!(restored.num_rows, Precision::Inexact(500));
    }

    /// The refilled NDV is capped at the child's `num_rows`: a column cannot have
    /// more distinct values than it has rows. The whole-table overlay NDV is an
    /// HLL union that only grows (never shrinks on delete/upsert-supersede), so it
    /// can exceed the live row count; the cap keeps it from inflating the join
    /// cardinality estimate.
    #[test]
    fn overlay_refilled_ndv_is_capped_at_num_rows() {
        use datafusion_common::ColumnStatistics;

        let child = Statistics {
            num_rows: Precision::Exact(3),
            total_byte_size: Precision::Absent,
            column_statistics: vec![ColumnStatistics {
                null_count: Precision::Absent,
                max_value: Precision::Absent,
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
                max_value: Precision::Absent,
                min_value: Precision::Absent,
                sum_value: Precision::Absent,
                // Over-counts the live 3 rows (HLL never-shrink drift).
                distinct_count: Precision::Inexact(42),
                byte_size: Precision::Absent,
            }],
        };

        let restored = restore_absent_column_statistics(child, &overlay);
        // Capped at num_rows (3), not the inflated overlay NDV (42).
        assert_eq!(
            restored.column_statistics[0].distinct_count,
            Precision::Inexact(3)
        );
    }

    /// The NDV cap clamps only when the NDV strictly exceeds the row count, and is
    /// a no-op when either side is unknown.
    #[test]
    fn cap_distinct_count_at_rows_clamps_only_when_exceeding() {
        // Exceeds rows -> clamped to rows (Inexact).
        assert_eq!(
            cap_distinct_count_at_rows(Precision::Inexact(42), Precision::Exact(3)),
            Precision::Inexact(3)
        );
        // Within bounds -> unchanged.
        assert_eq!(
            cap_distinct_count_at_rows(Precision::Inexact(2), Precision::Exact(3)),
            Precision::Inexact(2)
        );
        // Equal -> unchanged.
        assert_eq!(
            cap_distinct_count_at_rows(Precision::Exact(3), Precision::Exact(3)),
            Precision::Exact(3)
        );
        // Unknown NDV or unknown rows -> unchanged.
        assert_eq!(
            cap_distinct_count_at_rows(Precision::Absent, Precision::Exact(3)),
            Precision::Absent
        );
        assert_eq!(
            cap_distinct_count_at_rows(Precision::Inexact(5), Precision::Absent),
            Precision::Inexact(5)
        );
    }
}
