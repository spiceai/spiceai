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

//! Physical optimizer rules for Cayenne execution plans.
//!
//! # No-spill build-side memory strategy for wide multi-way joins
//!
//! `DataFusion`'s `HashJoinExec` build side is non-spillable. Under the runtime
//! memory pool (`GreedyMemoryPool` wrapped in `TrackConsumersPool`), wide
//! multi-way joins with correlated semi/anti subplans can exhaust the
//! `HashJoinInput[N]` reservations because each build-side hash table
//! independently materializes its full keyspace.
//!
//! The optimizer strategy is layered so each rule handles the part `DataFusion`
//! cannot currently spill or infer on its own:
//!
//! 1. **Logical predicate propagation.**
//!    [`crate::logical_optimizer::CayennePropagateFilterAcrossEquiJoinKeys`]
//!    introduces explicit `InSubquery` filters for equi-join keys when the
//!    selective predicate is on a non-key column. `DataFusion`'s stock
//!    `infer_join_predicates` only fires when the predicate already references
//!    a join key (`WHERE n_nationkey = 5` → `WHERE s_nationkey = 5`). When the
//!    selective filter is on a non-key dimension column, the Cayenne rule
//!    exposes the dimension-to-fact cardinality bound before `push_down_filter`
//!    plants it into scans.
//!
//! 2. **Cross-scan dynamic filter sharing.** When a join's
//!    `Arc<DynamicFilterPhysicalExpr>` is pushed into one
//!    `CayenneAccelerationExec`, [`CayenneDynamicFilterSharing`] installs the
//!    same `Arc` on sibling `CayenneAccelerationExec`s backed by the same
//!    underlying table and equi-joined column set. The shared `Arc` carries the
//!    same `Arc<RwLock<Inner>>` state, so all sibling scans observe the exact
//!    filter values as soon as the producing join accumulates them. Applies to
//!    `Inner`, `LeftSemi`, and `RightSemi` parent joins (anti joins are
//!    excluded — their semantics require the *absence* of a match, so sharing
//!    the filter would drop rows the anti-join is supposed to preserve).
//!
//! 3. **Oversized hash-join sort-merge rewrite.** Any `HashJoinExec` build side
//!    that is too big for the pool leaves a large non-spillable
//!    `HashJoinInput[N]` reservation behind, and dynamic-filter pushdown only
//!    shrinks the probe side, not that build-side table.
//!    [`CayenneAntiJoinSortMergeRewriter`] rewrites such joins to
//!    `SortMergeJoinExec` with explicit spillable `SortExec` inputs. When a
//!    query memory pool is wired through config (the runtime always does this),
//!    it covers any join type sort-merge supports — inner, outer, and semi/anti
//!    — from any source, gated on the estimated build side exceeding its share
//!    of the pool (the smaller of an absolute fraction and an even split across
//!    all hash joins in the plan). With no pool configured it falls back to the
//!    original conservative scope: same-source semi/anti joins above a 10M-row
//!    exact build-side threshold. Joins carrying an embedded projection are left
//!    alone and fall back to the `runtime.query.prefer_hash_join` knob.
//!
//! The ordinary inner-join probe side is handled by `DataFusion` 53's *native*
//! hash-join dynamic-filter pushdown. For inner joins (the only shape
//! `DataFusion` pushes join-derived dynamic filters through),
//! `HashJoinExec::gather_filters_for_pushdown` plants an
//! `Arc<DynamicFilterPhysicalExpr>` into the right-side scan during the
//! filter-pushdown phase, and `SharedBuildAccumulator` populates it at
//! execute-time with a combined predicate: min/max **bounds** (for
//! statistics-based row-group/file/segment skipping) *and* a **membership**
//! check — an `InList` for small build sides (within
//! `datafusion.execution.hash_join_inlist_pushdown_max_size`, which the Spice
//! runtime session builder sizes from `runtime.query.memory_limit` per
//! partition) or a hash-table lookup for larger ones. This natively supersedes
//! the previous forked
//! `ExactLeftAccumulator` seam (exact `InList` with min/max + bloom fallback),
//! so no Cayenne-specific accumulator swap is required. The
//! `CayenneAccelerationExec` scan already accepts the pushed filter via its
//! `gather_filters_for_pushdown`/`handle_child_pushdown_result` hooks, and
//! [`CayenneDynamicFilterSharing`] then fans it out to equi-joined same-source
//! sibling scans.
//!
//! ## Audit notes
//!
//! * **Cayenne table statistics are `Exact` at the physical-plan boundary.**
//!   The chain `CayenneTableProvider::statistics`
//!   → [`crate::stats::file_statistics_to_df`] returns
//!   `Precision::Exact(num_rows)` whenever the persisted `i64` row count is
//!   non-negative. Per-file `Statistics` are also `Exact` because
//!   `VortexFormat::infer_stats` reads `row_count` from the file footer, and
//!   `SessionConfig::default().collect_statistics()` is `true`, so
//!   `ListingTable::do_collect_statistics` is exercised for every scan.
//!   `CayenneAccelerationExec::partition_statistics` simply delegates to the
//!   inner `DataSourceExec`, so the value reaches `JoinSelection`. Representative
//!   explain plans confirm `should_swap_join_order` picks the smaller side as
//!   build at every level, so poor behavior on wide joins is *not* attributable
//!   to fuzzy stats — the logical optimizer must also avoid preserving SQL
//!   `FROM`-order cross joins when the parent join predicates can be evaluated
//!   inside a selective branch first.
//!
//! * **Build-side projections are minimal.** Every `CayenneAccelerationExec`
//!   in the snapshot terminates in a `DataSourceExec` whose `projection=[...]`
//!   lists only the join keys and the columns referenced above the join.
//!   `DataFusion`'s stock projection pushdown already prunes wider scans down to
//!   `[s_suppkey, s_name, s_nationkey]`, `[o_orderkey, o_orderstatus]`,
//!   `[l_orderkey, l_suppkey]`, etc. No additional `ProjectionExec` insertion
//!   above the build side is required.
//!
//! With these layers active, wide join and semi/anti-join workloads can stay on
//! spillable or pruned execution paths more often.

use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, IntervalUnit, SchemaRef};
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{JoinType, NullEquality, extensions_options};
use datafusion::config::{ConfigExtension, ConfigOptions};
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::joins::{HashJoinExec, SortMergeJoinExec};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion_common::stats::Precision;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_plan::repartition::RepartitionExec;
use runtime_datafusion::execution_plan::schema_cast::SchemaCastScanExec;
use runtime_datafusion::extension::bytes_processed::BytesProcessedExec;
use runtime_datafusion::join_accumulator::{
    DEFAULT_MAXIMUM_SHARED_INLIST_MEMORY_BYTES, ExactLeftAccumulator,
};
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use crate::maintained_aggregate::{
    MaintainedAggregateExec, MaintainedAggregateRegistry, aggregate_shape_is_maintainable,
};
use crate::provider::CayenneAccelerationExec;
use crate::provider::delete::{Int64PkDeletionFilterExec, KeyBasedDeletionFilterExec};
use crate::provider::scan::{ScanDynamicFilter, ScanIdentity};

/// Optimizer rule that rewrites `HashJoinExec` nodes to use `ExactLeftAccumulator`
/// when the probe side is a `CayenneAccelerationExec`.
///
/// Opt-in: this rule is only registered when the runtime
/// `cayenne_optimizer_rules.exact_join_filter` flag is enabled. By default the
/// ordinary inner-join probe filter is handled by `DataFusion` 53's native
/// hash-join dynamic-filter pushdown (whose `InList` budget is raised in the
/// runtime session builder's `configure_hash_join_memory_limits`).
#[derive(Default)]
pub struct CayenneJoinRewriter;

impl CayenneJoinRewriter {
    /// Create a new `CayenneJoinRewriter` optimizer rule.
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

/// Shares already-pushed hash-join dynamic filters between same-source Cayenne
/// scans when the current hash join proves the relevant columns are equi-joined.
#[derive(Default)]
pub struct CayenneDynamicFilterSharing;

impl CayenneDynamicFilterSharing {
    /// Create a new `CayenneDynamicFilterSharing` optimizer rule.
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

/// Rewrites exact aggregate queries over Cayenne scans to a table-maintained
/// aggregate batch when the scan and registry epochs match.
#[derive(Default)]
pub struct CayenneMaintainedAggregateRewriter;

impl CayenneMaintainedAggregateRewriter {
    /// Create a new `CayenneMaintainedAggregateRewriter` optimizer rule.
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl std::fmt::Debug for CayenneMaintainedAggregateRewriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CayenneMaintainedAggregateRewriter")
            .finish()
    }
}

impl std::fmt::Debug for CayenneDynamicFilterSharing {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CayenneDynamicFilterSharing").finish()
    }
}

/// Rewrites large Cayenne hash joins to sort-merge joins when the build side is
/// large enough to risk exhausting the query memory pool.
///
/// `DataFusion`'s `HashJoinExec` always materializes its left input as a
/// non-spillable build-side hash table, regardless of join type, so a build
/// side too big for the pool fails the query outright. When a query memory pool
/// is wired through config (the runtime always does this), any join type that
/// sort-merge supports — inner, left/right/full outer, and semi/anti — whose
/// estimated build side would not fit its share of the pool is rewritten to a
/// `SortMergeJoinExec` with spillable `SortExec` inputs. Smaller joins, and
/// (when no pool is configured) everything but same-source semi/anti joins, are
/// left as hash joins because that is usually the faster plan. Joins that carry
/// an embedded output projection are also left alone — `HashJoinExec` exposes
/// no accessor to reconstruct the projection onto a sort-merge join — and fall
/// back to the deterministic `runtime.query.prefer_hash_join` knob.
#[derive(Default)]
pub struct CayenneAntiJoinSortMergeRewriter;

/// Only rewrite same-source joins whose LEFT (build) input has
/// `Precision::Exact` row count exceeding this threshold. Below it, the
/// in-memory hash table is usually faster than two explicit sort buffers.
const ANTI_JOIN_SORT_MERGE_MIN_EXACT_ROWS: usize = 10_000_000;
const ANTI_JOIN_SORT_MERGE_MEMORY_POOL_FRACTION: f64 = 0.125;
const EXACT_JOIN_FILTER_MIN_PROBE_ROWS: usize = 100_000;
const EXACT_JOIN_FILTER_MIN_PROBE_TO_BUILD_RATIO: usize = 10;

extensions_options! {
    /// Cayenne optimizer configuration.
    pub struct CayenneOptimizerConfig {
        /// Minimum exact LEFT/build-side row count before considering the same-source hash-join to sort-merge rewrite.
        pub sort_merge_min_rows: usize, default = ANTI_JOIN_SORT_MERGE_MIN_EXACT_ROWS

        /// Fraction of the query memory pool that the estimated hash-join build side must exceed before rewriting to sort-merge. Set to 0 to disable the memory gate.
        pub sort_merge_memory_pool_fraction: f64, default = ANTI_JOIN_SORT_MERGE_MEMORY_POOL_FRACTION

        /// Effective query memory pool size in bytes. Runtime wiring sets this from `runtime.query.memory_limit`; direct DataFusion users can leave it unset to use the row-count gate only.
        pub sort_merge_memory_pool_bytes: Option<usize>, default = None

        /// Maximum estimated LEFT/build-side join-key bytes before preserving DataFusion's default hash-join accumulator instead of using Cayenne's exact in-list accumulator.
        pub exact_join_filter_max_bytes: usize, default = DEFAULT_MAXIMUM_SHARED_INLIST_MEMORY_BYTES

        /// Minimum known RIGHT/probe-side row count before using Cayenne's exact in-list accumulator.
        pub exact_join_filter_min_probe_rows: usize, default = EXACT_JOIN_FILTER_MIN_PROBE_ROWS

        /// Minimum known RIGHT/probe-side to LEFT/build-side row-count ratio before using Cayenne's exact in-list accumulator. Set to 0 to disable the ratio gate.
        pub exact_join_filter_min_probe_to_build_ratio: usize, default = EXACT_JOIN_FILTER_MIN_PROBE_TO_BUILD_RATIO
    }
}

impl ConfigExtension for CayenneOptimizerConfig {
    const PREFIX: &'static str = "cayenne";
}

impl CayenneAntiJoinSortMergeRewriter {
    /// Create a new `CayenneAntiJoinSortMergeRewriter` optimizer rule.
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl std::fmt::Debug for CayenneAntiJoinSortMergeRewriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CayenneAntiJoinSortMergeRewriter").finish()
    }
}

impl PhysicalOptimizerRule for CayenneAntiJoinSortMergeRewriter {
    fn name(&self) -> &'static str {
        "CayenneAntiJoinSortMergeRewriter"
    }

    fn schema_check(&self) -> bool {
        false
    }

    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        // The fair-share memory gate divides the pool across every hash join in
        // the plan, so count them once up front (the original plan's join count
        // is the concurrency pressure we are budgeting against).
        let hash_join_count = count_hash_joins(&plan);
        plan.transform_down(|node| {
            let Some(hash_join) = node.as_any().downcast_ref::<HashJoinExec>() else {
                return Ok(Transformed::no(node));
            };

            let Some(sort_merge_join) =
                try_rewrite_oversized_join(hash_join, config, hash_join_count)?
            else {
                return Ok(Transformed::no(node));
            };

            Ok(Transformed::yes(sort_merge_join))
        })
        .data()
    }
}

impl PhysicalOptimizerRule for CayenneDynamicFilterSharing {
    fn name(&self) -> &'static str {
        "CayenneDynamicFilterSharing"
    }

    fn schema_check(&self) -> bool {
        false
    }

    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        plan.transform_down(|node| {
            let Some(hash_join) = node.as_any().downcast_ref::<HashJoinExec>() else {
                return Ok(Transformed::no(node));
            };

            let (left_additions, right_additions) = filter_additions_for_join(hash_join);
            if left_additions.is_empty() && right_additions.is_empty() {
                return Ok(Transformed::no(node));
            }

            let (left, left_changed) =
                apply_filter_additions(Arc::clone(hash_join.left()), &left_additions, config)?;
            let (right, right_changed) =
                apply_filter_additions(Arc::clone(hash_join.right()), &right_additions, config)?;

            if !left_changed && !right_changed {
                return Ok(Transformed::no(node));
            }

            let new_node = node.with_new_children(vec![left, right])?;
            Ok(Transformed::yes(new_node))
        })
        .data()
    }
}

impl PhysicalOptimizerRule for CayenneMaintainedAggregateRewriter {
    fn name(&self) -> &'static str {
        "CayenneMaintainedAggregateRewriter"
    }

    fn schema_check(&self) -> bool {
        false
    }

    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        plan.transform_down(|node| {
            let Some(aggregate) = node.as_any().downcast_ref::<AggregateExec>() else {
                return Ok(Transformed::no(node));
            };
            let Some((source, scan_epoch, query_aggregate)) =
                maintained_aggregate_source_for_aggregate(aggregate)
            else {
                return Ok(Transformed::no(node));
            };
            let Some(batch) =
                source.batch_for_aggregate_with_output(query_aggregate, aggregate, scan_epoch)?
            else {
                return Ok(Transformed::no(node));
            };

            Ok(Transformed::yes(
                Arc::new(MaintainedAggregateExec::try_new(batch)?) as Arc<dyn ExecutionPlan>,
            ))
        })
        .data()
    }
}

fn maintained_aggregate_source_for_aggregate(
    aggregate: &AggregateExec,
) -> Option<(&Arc<MaintainedAggregateRegistry>, u64, &AggregateExec)> {
    match aggregate.mode() {
        AggregateMode::Single | AggregateMode::SinglePartitioned => {
            maintained_aggregate_source(aggregate.input())
                .map(|(source, scan_epoch)| (source, scan_epoch, aggregate))
        }
        AggregateMode::Final | AggregateMode::FinalPartitioned => {
            let partial = maintained_aggregate_partial_input(aggregate.input())?;
            if !is_simple_partial_aggregate(partial) {
                return None;
            }
            maintained_aggregate_source(partial.input())
                .map(|(source, scan_epoch)| (source, scan_epoch, partial))
        }
        AggregateMode::Partial | AggregateMode::PartialReduce => None,
    }
}

#[expect(deprecated)]
fn maintained_aggregate_partial_input(plan: &Arc<dyn ExecutionPlan>) -> Option<&AggregateExec> {
    if let Some(aggregate) = plan.as_any().downcast_ref::<AggregateExec>() {
        return Some(aggregate);
    }

    let any = plan.as_any();
    if any.downcast_ref::<RepartitionExec>().is_none()
        && any
            .downcast_ref::<datafusion_physical_plan::coalesce_batches::CoalesceBatchesExec>()
            .is_none()
        && any
            .downcast_ref::<datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec>()
            .is_none()
    {
        return None;
    }

    let children = plan.children();
    if children.len() != 1 {
        return None;
    }
    maintained_aggregate_partial_input(children[0])
}

fn is_simple_partial_aggregate(aggregate: &AggregateExec) -> bool {
    matches!(aggregate.mode(), AggregateMode::Partial) && aggregate_shape_is_maintainable(aggregate)
}

#[expect(deprecated)]
fn maintained_aggregate_source(
    plan: &Arc<dyn ExecutionPlan>,
) -> Option<(&Arc<MaintainedAggregateRegistry>, u64)> {
    if let Some(cayenne_scan) = plan.as_any().downcast_ref::<CayenneAccelerationExec>() {
        return cayenne_scan.maintained_aggregates();
    }

    let any = plan.as_any();
    if any.downcast_ref::<RepartitionExec>().is_none()
        && any
            .downcast_ref::<datafusion_physical_plan::coalesce_batches::CoalesceBatchesExec>()
            .is_none()
        && any.downcast_ref::<SchemaCastScanExec>().is_none()
        && any
            .downcast_ref::<datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec>()
            .is_none()
    {
        return None;
    }

    let children = plan.children();
    if children.len() != 1 {
        return None;
    }
    maintained_aggregate_source(children[0])
}

#[derive(Clone)]
struct CayenneScanSummary {
    identity: Arc<ScanIdentity>,
    columns: BTreeSet<String>,
    schema_fields: Vec<(String, DataType)>,
    dynamic_filters: Vec<ScanDynamicFilter>,
}

#[derive(Clone)]
struct FilterAddition {
    identity: Arc<ScanIdentity>,
    schema_fields: Vec<(String, DataType)>,
    filter: Arc<dyn PhysicalExpr>,
}

fn filter_additions_for_join(
    hash_join: &HashJoinExec,
) -> (Vec<FilterAddition>, Vec<FilterAddition>) {
    // `Inner`, `LeftSemi`, and `RightSemi` all preserve the equi-key domain:
    // a dynamic filter built from one side is also a valid filter for an
    // equi-joined same-source scan on the other side. `LeftAnti`/`RightAnti`
    // do not — their output requires the absence of a match, so propagating
    // the filter would drop rows that should be retained.
    if !matches!(
        *hash_join.join_type(),
        JoinType::Inner | JoinType::LeftSemi | JoinType::RightSemi,
    ) {
        return (Vec::new(), Vec::new());
    }
    if hash_join.null_equality() != NullEquality::NullEqualsNothing {
        return (Vec::new(), Vec::new());
    }

    let left_scans = collect_cayenne_scans(hash_join.left());
    let right_scans = collect_cayenne_scans(hash_join.right());
    if left_scans.is_empty() || right_scans.is_empty() {
        return (Vec::new(), Vec::new());
    }
    let right_scans_by_identity = scans_by_identity(&right_scans);

    let mut pair_columns: HashMap<(usize, usize), BTreeSet<String>> = HashMap::new();
    for (left_key, right_key) in hash_join.on() {
        let Some(left_column) = physical_column_name(left_key) else {
            continue;
        };
        let Some(right_column) = physical_column_name(right_key) else {
            continue;
        };

        if left_column != right_column {
            continue;
        }

        let matching_pairs = same_source_pairs_for_column(
            &left_scans,
            &right_scans,
            &right_scans_by_identity,
            left_column,
            right_column,
        );
        let [(left_index, right_index)] = matching_pairs.as_slice() else {
            continue;
        };
        if left_scans[*left_index].schema_fields != right_scans[*right_index].schema_fields {
            continue;
        }

        pair_columns
            .entry((*left_index, *right_index))
            .or_default()
            .insert(left_column.to_string());
    }

    let mut left_additions = Vec::new();
    let mut right_additions = Vec::new();

    for ((left_index, right_index), shared_columns) in pair_columns {
        let left_scan = &left_scans[left_index];
        let right_scan = &right_scans[right_index];

        for filter in &left_scan.dynamic_filters {
            if filter.columns().is_subset(&shared_columns) {
                push_filter_addition(
                    &mut right_additions,
                    Arc::clone(&right_scan.identity),
                    right_scan.schema_fields.clone(),
                    Arc::clone(filter.filter()),
                );
            }
        }

        for filter in &right_scan.dynamic_filters {
            if filter.columns().is_subset(&shared_columns) {
                push_filter_addition(
                    &mut left_additions,
                    Arc::clone(&left_scan.identity),
                    left_scan.schema_fields.clone(),
                    Arc::clone(filter.filter()),
                );
            }
        }
    }

    (left_additions, right_additions)
}

fn try_rewrite_oversized_join(
    hash_join: &HashJoinExec,
    config: &ConfigOptions,
    hash_join_count: usize,
) -> Result<Option<Arc<dyn ExecutionPlan>>, DataFusionError> {
    // `SortMergeJoinExec` supports these join types with spillable, explicitly
    // sorted inputs. Inner and outer joins are included here (unlike the legacy
    // path below) because a large inner/outer build side is just as
    // non-spillable as a semi/anti one — `HashJoinExec`'s dynamic-filter
    // pushdown shrinks the *probe* side, not the build-side hash table that
    // actually exhausts the pool.
    if !matches!(
        hash_join.join_type(),
        JoinType::Inner
            | JoinType::Left
            | JoinType::Right
            | JoinType::Full
            | JoinType::LeftSemi
            | JoinType::RightSemi
            | JoinType::LeftAnti
            | JoinType::RightAnti,
    ) {
        return Ok(None);
    }

    // Sorted-merge inputs rely on the default null-comparison semantics.
    if hash_join.null_equality() != NullEquality::NullEqualsNothing {
        return Ok(None);
    }

    // `SortMergeJoinExec` carries no embedded output projection and
    // `HashJoinExec` exposes no accessor to read one back, so a projected join
    // cannot be rewritten without changing the output schema; leave it to the
    // deterministic `runtime.query.prefer_hash_join` knob.
    if hash_join.contains_projection() || hash_join.on().is_empty() {
        return Ok(None);
    }

    let optimizer_config = cayenne_optimizer_config(config);
    let memory_gate_bytes = sort_merge_memory_gate_bytes(&optimizer_config);

    let should_rewrite = if let Some(gate_bytes) = memory_gate_bytes {
        // General memory-gated path — the live path in production, where the
        // runtime always wires a pool. Any supported join type, from any source,
        // is eligible when its estimated build side would not fit its share of
        // the pool. Build-side row counts may be inexact here: a build side that
        // is itself a join result rarely carries exact statistics, and an
        // inexact estimate is enough to choose spilling over an OOM.
        if !join_touches_cayenne(hash_join) {
            return Ok(None);
        }
        let Some(build_row_count) = build_input_row_estimate(hash_join) else {
            return Ok(None);
        };
        let Some(estimated_build_bytes) =
            build_side_memory_estimate(hash_join.left().as_ref(), build_row_count)
        else {
            return Ok(None);
        };

        // Per-join budget: the smaller of the absolute pool fraction and an even
        // share of the pool across every hash join in the plan. A wide query
        // such as TPC-DS q78 keeps many build sides alive at once, each below
        // the absolute fraction yet summing past the pool; the fair-share term
        // catches that, while a lone large join still gets the full fraction.
        let fair_share = optimizer_config
            .sort_merge_memory_pool_bytes
            .map_or(gate_bytes, |pool_bytes| pool_bytes / hash_join_count.max(1));
        let effective_gate = gate_bytes.min(fair_share);
        let fire = estimated_build_bytes > effective_gate;

        tracing::debug!(
            join_type = ?hash_join.join_type(),
            build_row_count,
            estimated_build_bytes,
            gate_bytes,
            fair_share,
            effective_gate,
            hash_join_count,
            fire,
            "Evaluated Cayenne oversized-join memory gate"
        );
        fire
    } else {
        // Legacy row-count fallback for direct `DataFusion` users with no memory
        // pool wired through config. Preserve the original conservative scope:
        // same-source semi/anti joins with an exact, large build side.
        if !matches!(
            hash_join.join_type(),
            JoinType::LeftAnti | JoinType::RightAnti | JoinType::LeftSemi | JoinType::RightSemi,
        ) {
            return Ok(None);
        }
        if !has_single_same_source_pair_for_all_join_keys(hash_join) {
            return Ok(None);
        }
        let Some(build_row_count) = spillable_rewrite_build_input_exact_rows(hash_join) else {
            return Ok(None);
        };
        build_row_count > optimizer_config.sort_merge_min_rows
    };

    if !should_rewrite {
        return Ok(None);
    }

    let sort_options = vec![SortOptions::default(); hash_join.on().len()];
    let Some(left_ordering) = join_key_ordering(
        hash_join
            .on()
            .iter()
            .map(|(left_key, _)| Arc::clone(left_key)),
        &sort_options,
    ) else {
        return Ok(None);
    };
    let Some(right_ordering) = join_key_ordering(
        hash_join
            .on()
            .iter()
            .map(|(_, right_key)| Arc::clone(right_key)),
        &sort_options,
    ) else {
        return Ok(None);
    };

    let left: Arc<dyn ExecutionPlan> =
        Arc::new(SortExec::new(left_ordering, Arc::clone(hash_join.left())));
    let right: Arc<dyn ExecutionPlan> =
        Arc::new(SortExec::new(right_ordering, Arc::clone(hash_join.right())));

    let join = SortMergeJoinExec::try_new(
        left,
        right,
        hash_join.on().to_vec(),
        hash_join.filter().cloned(),
        *hash_join.join_type(),
        sort_options,
        hash_join.null_equality(),
    )?;

    tracing::debug!(
        join_type = ?hash_join.join_type(),
        "Replaced large Cayenne HashJoinExec with spillable SortMergeJoinExec"
    );

    Ok(Some(Arc::new(join)))
}

/// Build-side (LEFT input) row count for the spillable rewrite, accepting an
/// inexact estimate. Returns `None` only when statistics are entirely absent.
/// Deep build sides (a join result feeding another join) rarely have exact
/// statistics, so requiring `Precision::Exact` would skip exactly the wide
/// multi-way joins this rewrite targets.
fn build_input_row_estimate(hash_join: &HashJoinExec) -> Option<usize> {
    match hash_join.left().partition_statistics(None).ok()?.num_rows {
        Precision::Exact(row_count) | Precision::Inexact(row_count) => Some(row_count),
        Precision::Absent => None,
    }
}

/// Whether either side of the join reads Cayenne-accelerated data. Keeps the
/// memory-gated rewrite scoped to Cayenne query plans without the restrictive
/// same-source join-key pairing required by the legacy semi/anti path.
fn join_touches_cayenne(hash_join: &HashJoinExec) -> bool {
    !collect_cayenne_scans(hash_join.left()).is_empty()
        || !collect_cayenne_scans(hash_join.right()).is_empty()
}

/// Count the `HashJoinExec` nodes in a plan. Used to size each join's fair
/// share of the query memory pool: many concurrent build sides, each within the
/// absolute fraction, can still sum past the pool.
fn count_hash_joins(plan: &Arc<dyn ExecutionPlan>) -> usize {
    let mut count = 0;
    count_hash_joins_inner(plan, &mut count);
    count
}

fn count_hash_joins_inner(plan: &Arc<dyn ExecutionPlan>, count: &mut usize) {
    if plan.as_any().downcast_ref::<HashJoinExec>().is_some() {
        *count += 1;
    }
    for child in plan.children() {
        count_hash_joins_inner(child, count);
    }
}

fn cayenne_optimizer_config(config: &ConfigOptions) -> CayenneOptimizerConfig {
    config
        .extensions
        .get::<CayenneOptimizerConfig>()
        .cloned()
        .unwrap_or_default()
}

fn sort_merge_memory_gate_bytes(config: &CayenneOptimizerConfig) -> Option<usize> {
    let fraction = config.sort_merge_memory_pool_fraction;
    if !fraction.is_finite() || fraction <= 0.0 {
        return None;
    }

    config
        .sort_merge_memory_pool_bytes
        .map(|pool_bytes| fractional_bytes(pool_bytes, fraction))
}

#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    reason = "DataFusion config exposes this memory gate as a fraction; saturating conversion is used for byte thresholds"
)]
fn fractional_bytes(bytes: usize, fraction: f64) -> usize {
    let scaled = bytes as f64 * fraction;
    if !scaled.is_finite() || scaled >= usize::MAX as f64 {
        usize::MAX
    } else if scaled <= 0.0 {
        0
    } else {
        scaled as usize
    }
}

fn build_side_memory_estimate(plan: &dyn ExecutionPlan, build_rows: usize) -> Option<usize> {
    let row_width = plan
        .schema()
        .fields()
        .iter()
        .try_fold(0_usize, |acc, field| {
            Some(acc.saturating_add(estimated_arrow_width(field.data_type())?))
        })?;

    Some(row_width.saturating_mul(build_rows))
}

fn estimated_arrow_width(data_type: &DataType) -> Option<usize> {
    match data_type {
        DataType::Null => Some(0),
        DataType::Boolean | DataType::Int8 | DataType::UInt8 => Some(1),
        DataType::Int16 | DataType::UInt16 | DataType::Float16 => Some(2),
        DataType::Int32
        | DataType::UInt32
        | DataType::Float32
        | DataType::Date32
        | DataType::Time32(_)
        | DataType::Interval(IntervalUnit::YearMonth)
        | DataType::Decimal32(_, _) => Some(4),
        DataType::Int64
        | DataType::UInt64
        | DataType::Float64
        | DataType::Timestamp(_, _)
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Duration(_)
        | DataType::Decimal64(_, _)
        | DataType::Interval(IntervalUnit::DayTime) => Some(8),
        DataType::Interval(IntervalUnit::MonthDayNano) | DataType::Decimal128(_, _) => Some(16),
        DataType::Decimal256(_, _) => Some(32),
        DataType::FixedSizeBinary(size) => usize::try_from(*size).ok(),
        DataType::Dictionary(_, value_type) => estimated_arrow_width(value_type)
            .map(|width| width.saturating_add(std::mem::size_of::<u64>())),
        DataType::FixedSizeList(field, length) => {
            let length = usize::try_from(*length).ok()?;
            estimated_arrow_width(field.data_type()).map(|width| width.saturating_mul(length))
        }
        DataType::Struct(fields) => fields.iter().try_fold(0_usize, |acc, field| {
            Some(acc.saturating_add(estimated_arrow_width(field.data_type())?))
        }),
        DataType::RunEndEncoded(_, value_field) => estimated_arrow_width(value_field.data_type())
            .map(|width| width.saturating_add(std::mem::size_of::<u64>())),
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::List(_)
        | DataType::ListView(_)
        | DataType::LargeList(_)
        | DataType::LargeListView(_)
        | DataType::Map(_, _)
        | DataType::Union(_, _) => Some(64),
    }
}

fn spillable_rewrite_build_input_exact_rows(hash_join: &HashJoinExec) -> Option<usize> {
    // `HashJoinExec` materializes the LEFT input as the (non-spillable) build
    // hash table regardless of join type.
    let build_input = hash_join.left();

    match build_input.partition_statistics(None).ok()?.num_rows {
        Precision::Exact(row_count) => Some(row_count),
        Precision::Inexact(_) | Precision::Absent => None,
    }
}

fn exact_join_filter_build_key_bytes(
    hash_join: &HashJoinExec,
    build_row_count: usize,
    max_build_bytes: usize,
) -> Option<usize> {
    let build_schema = hash_join.left().schema();
    let mut estimated_build_bytes = 0_usize;

    for (left_key, _) in hash_join.on() {
        let data_type = left_key.data_type(build_schema.as_ref()).ok()?;
        if !supports_exact_join_filter_fallback(&data_type) {
            return None;
        }

        let key_width = estimated_arrow_width(&data_type)?;
        estimated_build_bytes =
            estimated_build_bytes.saturating_add(build_row_count.saturating_mul(key_width));
        if estimated_build_bytes > max_build_bytes {
            break;
        }
    }

    Some(estimated_build_bytes)
}

fn exact_join_filter_probe_rows(hash_join: &HashJoinExec) -> Option<usize> {
    match hash_join.right().partition_statistics(None).ok()?.num_rows {
        Precision::Exact(row_count) | Precision::Inexact(row_count) => Some(row_count),
        Precision::Absent => None,
    }
}

fn supports_exact_join_filter_fallback(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal32(_, _)
            | DataType::Decimal64(_, _)
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
            | DataType::Date32
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Timestamp(_, _)
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
    )
}

fn should_rewrite_with_exact_accumulator(hash_join: &HashJoinExec, config: &ConfigOptions) -> bool {
    if *hash_join.join_type() != JoinType::Inner {
        tracing::debug!(
            join_type = ?hash_join.join_type(),
            "Keeping HashJoinExec default accumulator because DataFusion only pushes join dynamic filters through inner joins"
        );
        return false;
    }

    let optimizer_config = cayenne_optimizer_config(config);
    let max_build_bytes = optimizer_config.exact_join_filter_max_bytes;
    let Some(build_row_count) = spillable_rewrite_build_input_exact_rows(hash_join) else {
        tracing::debug!(
            "Keeping HashJoinExec default accumulator because exact build-side row statistics are unavailable"
        );
        return false;
    };

    let Some(probe_row_count) = exact_join_filter_probe_rows(hash_join) else {
        tracing::debug!(
            "Keeping HashJoinExec default accumulator because probe-side row statistics are unavailable"
        );
        return false;
    };

    if probe_row_count < optimizer_config.exact_join_filter_min_probe_rows {
        tracing::debug!(
            probe_row_count,
            min_probe_rows = optimizer_config.exact_join_filter_min_probe_rows,
            "Keeping HashJoinExec default accumulator because the Cayenne probe side is too small for exact join-filter collection to pay off"
        );
        return false;
    }

    let min_probe_to_build_ratio = optimizer_config.exact_join_filter_min_probe_to_build_ratio;
    if build_row_count > 0
        && min_probe_to_build_ratio > 0
        && probe_row_count < build_row_count.saturating_mul(min_probe_to_build_ratio)
    {
        tracing::debug!(
            build_row_count,
            probe_row_count,
            min_probe_to_build_ratio,
            "Keeping HashJoinExec default accumulator because the Cayenne probe side is not much larger than the build-side key domain"
        );
        return false;
    }

    let Some(estimated_build_bytes) =
        exact_join_filter_build_key_bytes(hash_join, build_row_count, max_build_bytes)
    else {
        tracing::debug!(
            "Keeping HashJoinExec default accumulator because fallback-compatible build-side join-key types are unavailable"
        );
        return false;
    };

    if estimated_build_bytes > max_build_bytes {
        tracing::debug!(
            build_row_count,
            estimated_build_bytes,
            max_build_bytes,
            "Keeping HashJoinExec default accumulator because estimated exact join-filter memory exceeds the configured budget"
        );
        return false;
    }

    true
}

fn join_key_ordering(
    keys: impl Iterator<Item = Arc<dyn PhysicalExpr>>,
    sort_options: &[SortOptions],
) -> Option<LexOrdering> {
    let sort_exprs = keys
        .zip(sort_options.iter().copied())
        .map(|(expr, options)| PhysicalSortExpr { expr, options })
        .collect::<Vec<_>>();

    LexOrdering::new(sort_exprs)
}

fn has_single_same_source_pair_for_all_join_keys(hash_join: &HashJoinExec) -> bool {
    let left_scans = collect_cayenne_scans(hash_join.left());
    let right_scans = collect_cayenne_scans(hash_join.right());
    if left_scans.is_empty() || right_scans.is_empty() {
        return false;
    }
    let right_scans_by_identity = scans_by_identity(&right_scans);

    let mut matched_pair = None;
    for (left_key, right_key) in hash_join.on() {
        let Some(left_column) = physical_column_name(left_key) else {
            return false;
        };
        let Some(right_column) = physical_column_name(right_key) else {
            return false;
        };

        if left_column != right_column {
            return false;
        }

        let pairs = same_source_pairs_for_column(
            &left_scans,
            &right_scans,
            &right_scans_by_identity,
            left_column,
            right_column,
        );
        let [(left_index, right_index)] = pairs.as_slice() else {
            return false;
        };
        let pair = (*left_index, *right_index);

        if matched_pair.is_some_and(|previous_pair| previous_pair != pair) {
            return false;
        }
        matched_pair = Some(pair);
    }

    matched_pair.is_some()
}

fn collect_cayenne_scans(plan: &Arc<dyn ExecutionPlan>) -> Vec<CayenneScanSummary> {
    let mut scans = Vec::new();
    collect_cayenne_scans_inner(plan, &mut scans);
    scans
}

fn collect_cayenne_scans_inner(plan: &Arc<dyn ExecutionPlan>, scans: &mut Vec<CayenneScanSummary>) {
    if let Some(cayenne) = plan.as_any().downcast_ref::<CayenneAccelerationExec>()
        && let Some(identity) = cayenne.scan_identity()
    {
        let schema_fields = plan_schema_fields(&cayenne.schema());
        let columns = schema_fields.iter().map(|(name, _)| name.clone()).collect();
        scans.push(CayenneScanSummary {
            identity,
            columns,
            schema_fields,
            dynamic_filters: cayenne.dynamic_filters(),
        });
        return;
    }

    for child in plan.children() {
        collect_cayenne_scans_inner(child, scans);
    }
}

fn physical_column_name(expr: &Arc<dyn PhysicalExpr>) -> Option<&str> {
    expr.as_any().downcast_ref::<Column>().map(Column::name)
}

fn scans_by_identity(scans: &[CayenneScanSummary]) -> HashMap<Arc<ScanIdentity>, Vec<usize>> {
    let mut by_identity: HashMap<Arc<ScanIdentity>, Vec<usize>> = HashMap::new();
    for (index, scan) in scans.iter().enumerate() {
        by_identity
            .entry(Arc::clone(&scan.identity))
            .or_default()
            .push(index);
    }
    by_identity
}

fn same_source_pairs_for_column(
    left_scans: &[CayenneScanSummary],
    right_scans: &[CayenneScanSummary],
    right_scans_by_identity: &HashMap<Arc<ScanIdentity>, Vec<usize>>,
    left_column: &str,
    right_column: &str,
) -> Vec<(usize, usize)> {
    let mut pairs = Vec::new();

    for (left_index, left_scan) in left_scans.iter().enumerate() {
        if !left_scan.columns.contains(left_column) {
            continue;
        }

        let Some(right_indices) = right_scans_by_identity.get(&left_scan.identity) else {
            continue;
        };

        for &right_index in right_indices {
            if right_scans[right_index].columns.contains(right_column) {
                pairs.push((left_index, right_index));
            }
        }
    }

    pairs
}

fn push_filter_addition(
    additions: &mut Vec<FilterAddition>,
    identity: Arc<ScanIdentity>,
    schema_fields: Vec<(String, DataType)>,
    filter: Arc<dyn PhysicalExpr>,
) {
    if additions.iter().any(|addition| {
        addition.identity == identity
            && addition.schema_fields == schema_fields
            && Arc::ptr_eq(&addition.filter, &filter)
    }) {
        return;
    }

    additions.push(FilterAddition {
        identity,
        schema_fields,
        filter,
    });
}

fn plan_schema_fields(schema: &SchemaRef) -> Vec<(String, DataType)> {
    schema
        .fields()
        .iter()
        .map(|field| (field.name().clone(), field.data_type().clone()))
        .collect()
}

fn apply_filter_additions(
    plan: Arc<dyn ExecutionPlan>,
    additions: &[FilterAddition],
    config: &ConfigOptions,
) -> Result<(Arc<dyn ExecutionPlan>, bool), DataFusionError> {
    if additions.is_empty() {
        return Ok((plan, false));
    }

    if let Some(cayenne) = plan.as_any().downcast_ref::<CayenneAccelerationExec>() {
        let Some(identity) = cayenne.scan_identity() else {
            return Ok((plan, false));
        };
        let schema_fields = plan_schema_fields(&cayenne.schema());
        let existing = cayenne.dynamic_filters();
        let filters = additions
            .iter()
            .filter(|addition| addition.identity == identity)
            .filter(|addition| addition.schema_fields == schema_fields)
            .filter(|addition| {
                !existing
                    .iter()
                    .any(|filter| Arc::ptr_eq(filter.filter(), &addition.filter))
            })
            .map(|addition| Arc::clone(&addition.filter))
            .collect::<Vec<_>>();

        let Some(new_plan) = cayenne.with_additional_dynamic_filters(&filters, config)? else {
            return Ok((plan, false));
        };

        return Ok((new_plan, true));
    }

    let children = plan
        .children()
        .into_iter()
        .map(Arc::clone)
        .collect::<Vec<_>>();
    if children.is_empty() {
        return Ok((plan, false));
    }

    let mut changed = false;
    let mut new_children = Vec::with_capacity(children.len());
    for child in children {
        let (new_child, child_changed) = apply_filter_additions(child, additions, config)?;
        changed |= child_changed;
        new_children.push(new_child);
    }

    if !changed {
        return Ok((plan, false));
    }

    plan.with_new_children(new_children)
        .map(|plan| (plan, true))
}

impl std::fmt::Debug for CayenneJoinRewriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CayenneJoinRewriter").finish()
    }
}

/// Flatten transparent nodes (like `ProjectionExec` that just pass through columns)
/// to find the underlying plan node.
// `CoalesceBatchesExec` is deprecated in DF53 (superseded by arrow-rs
// `BatchCoalescer`) but the physical planner still emits it, so we keep seeing
// through it here — mirrors `provider::scan::is_identity_preserving_wrapper`.
#[expect(deprecated)]
fn flatten_transparent_nodes(plan: &Arc<dyn ExecutionPlan>) -> &Arc<dyn ExecutionPlan> {
    // ProjectionExec is transparent if it just passes through columns
    if let Some(projection) = plan.as_any().downcast_ref::<ProjectionExec>() {
        return flatten_transparent_nodes(projection.input());
    }

    if let Some(bytes_processed_exec) = plan.as_any().downcast_ref::<BytesProcessedExec>() {
        let children = bytes_processed_exec.children();
        let Some(input) = children.first() else {
            return plan;
        };

        return flatten_transparent_nodes(input);
    }

    if let Some(repartitioned) = plan.as_any().downcast_ref::<RepartitionExec>() {
        return flatten_transparent_nodes(repartitioned.input());
    }

    if let Some(coalesce) =
        plan.as_any()
            .downcast_ref::<datafusion_physical_plan::coalesce_batches::CoalesceBatchesExec>()
    {
        return flatten_transparent_nodes(coalesce.input());
    }

    // Deletion-filter execs sit directly above the Cayenne scan whenever
    // key-deletes are pending. They preserve the child's schema and
    // partitioning (they only remove deleted rows), so for the purpose of
    // identifying a Cayenne-backed scan on a join build/probe side they are
    // transparent — see through them so the dynamic-filter join rewrite still
    // fires on tables undergoing CDC deletes.
    if let Some(int64_delete) = plan.as_any().downcast_ref::<Int64PkDeletionFilterExec>() {
        let children = int64_delete.children();
        let Some(input) = children.first() else {
            return plan;
        };

        return flatten_transparent_nodes(input);
    }

    if let Some(key_delete) = plan.as_any().downcast_ref::<KeyBasedDeletionFilterExec>() {
        let children = key_delete.children();
        let Some(input) = children.first() else {
            return plan;
        };

        return flatten_transparent_nodes(input);
    }

    if let Some(schema_cast_scan) = plan.as_any().downcast_ref::<SchemaCastScanExec>() {
        let children = schema_cast_scan.children();
        let Some(input) = children.first() else {
            return plan;
        };

        return flatten_transparent_nodes(input);
    }

    plan
}

fn hash_join_build_side_is_cayenne(join: &HashJoinExec) -> bool {
    let build_side = flatten_transparent_nodes(join.left());

    if build_side
        .as_any()
        .downcast_ref::<CayenneAccelerationExec>()
        .is_some()
    {
        true
    } else if let Some(nested_join) = build_side.as_any().downcast_ref::<HashJoinExec>() {
        // Recursively check the build side of the nested join
        hash_join_build_side_is_cayenne(nested_join)
    } else {
        false
    }
}

/// Check if the probe side of the first input `HashJoinExec` is either `CayenneAccelerationExec` or another `HashJoinExec`.
///
/// For nested hash joins, the build side of the join must also be a `CayenneAccelerationExec` as the dynamic filter from this `HashJoinExec` will push into the build side of the next join.
///
/// This handles nested join patterns like:
/// ```text
///      HashJoinExec (top)
///         | - DataSourceExec (build)
///         | - HashJoinExec (probe/nested)
///               | - DataSourceExec (build of nested)
///               | - DataSourceExec (probe of nested)
/// ```
fn is_cayenne_backed_join(hash_join: &HashJoinExec) -> bool {
    // Check the probe side first (right child)
    let probe_side = flatten_transparent_nodes(hash_join.right());

    if probe_side
        .as_any()
        .downcast_ref::<CayenneAccelerationExec>()
        .is_some()
    {
        return true;
    }

    // If probe side is another `HashJoinExec`, check the build side of the nested join is Cayenne
    if let Some(nested_join) = probe_side.as_any().downcast_ref::<HashJoinExec>() {
        // The nested join's build side must also be Cayenne
        return hash_join_build_side_is_cayenne(nested_join);
    }

    // Unknown node type on probe side - not Cayenne-backed
    false
}

impl PhysicalOptimizerRule for CayenneJoinRewriter {
    fn name(&self) -> &'static str {
        "CayenneJoinRewriter"
    }

    fn schema_check(&self) -> bool {
        false
    }

    fn optimize(
        &self,
        plan: std::sync::Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        // For each `HashJoinExec`, determine if probe side is a `CayenneAccelerationExec` with a Cayenne accelerator
        // If so, that `HashJoinExec` can be replaced with one which uses a `ExactLeftAccumulator` so we can push down exact dynamic filter bounds into Cayenne
        // The build side is irrelevant for the collection, as we only push the filter down to the probe side
        //
        // This can become more complex for plans like:
        //      `HashJoinExec`
        //         | - `CayenneAccelerationExec`
        //         | - `HashJoinExec`
        //               | - `CayenneAccelerationExec`
        //               | - `CayenneAccelerationExec`
        //
        // In this scenario, the "build side" is the very first `CayenneAccelerationExec` - the probe side becomes the remaining `HashJoinExec`, which includes the other 2 `CayenneAccelerationExec`s.
        // The dynamic filter from the top `CayenneAccelerationExec` will push down into the build side of the second `HashJoinExec`.
        // After that, the dynamic filter from the second `HashJoinExec` will push down into its probe side `CayenneAccelerationExec` - sourced from its own build-side dynamic filter.
        //
        // Therefore, after we encounter a `HashJoinExec` we need to continue traversing down the build side of any subsequent `HashJoinExec`s to ensure it is a `CayenneAccelerationExec`.

        plan.transform_down(|node| {
            let Some(hash_join) = node.as_any().downcast_ref::<HashJoinExec>() else {
                return Ok(Transformed::no(node));
            };

            if *hash_join.join_type() != JoinType::Inner {
                return Ok(Transformed::no(node));
            }

            if hash_join.null_equality() != NullEquality::NullEqualsNothing {
                return Ok(Transformed::no(node));
            }

            if !is_cayenne_backed_join(hash_join) {
                return Ok(Transformed::no(node));
            }

            if !should_rewrite_with_exact_accumulator(hash_join, config) {
                return Ok(Transformed::no(node));
            }

            tracing::debug!(
                "Replacing HashJoinExec with ExactLeftAccumulator for Cayenne acceleration"
            );

            let new_join = hash_join.recreate_with_accumulator::<ExactLeftAccumulator>();

            Ok(Transformed::yes(Arc::new(new_join)))
        })
        .data()
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ANTI_JOIN_SORT_MERGE_MIN_EXACT_ROWS, CayenneAntiJoinSortMergeRewriter,
        CayenneDynamicFilterSharing, CayenneMaintainedAggregateRewriter, CayenneOptimizerConfig,
        FilterAddition, apply_filter_additions, plan_schema_fields,
    };
    use crate::maintained_aggregate::{
        MaintainedAggregateExec, MaintainedAggregateExpr, MaintainedAggregateFunction,
        MaintainedAggregateRegistry, MaintainedAggregateSpec,
    };
    use crate::provider::CayenneAccelerationExec;
    use crate::provider::scan::ScanDynamicFilter;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::common::{JoinType, NullEquality};
    use datafusion::config::ConfigOptions;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::physical_expr::aggregate::AggregateExprBuilder;
    use datafusion::physical_optimizer::PhysicalOptimizerRule;
    use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
    use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode, SortMergeJoinExec};
    use datafusion::physical_plan::sorts::sort::SortExec;
    use datafusion::physical_plan::union::UnionExec;
    use datafusion::physical_plan::{ExecutionPlan, displayable};
    use datafusion_common::stats::Precision;
    use datafusion_common::{DataFusionError, Result as DFResult, Statistics};
    use datafusion_datasource::file::FileSource;
    use datafusion_datasource::file_groups::FileGroup;
    use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
    use datafusion_datasource::file_stream::FileOpener;
    use datafusion_datasource::source::DataSourceExec;
    use datafusion_datasource::{PartitionedFile, TableSchema};
    use datafusion_functions_aggregate::count::count_udaf;
    use datafusion_physical_expr::expressions::{DynamicFilterPhysicalExpr, col, lit};
    use datafusion_physical_expr::projection::ProjectionExprs;
    use datafusion_physical_expr::{PhysicalExpr, conjunction};
    use datafusion_physical_plan::DisplayFormatType;
    use datafusion_physical_plan::filter_pushdown::{FilterPushdownPropagation, PushedDown};
    use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
    use object_store::ObjectMeta;
    use object_store::ObjectStore;
    use object_store::path::Path;
    use std::any::Any;
    use std::sync::Arc;

    fn maintained_aggregate_test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("value", DataType::Int64, true),
        ]))
    }

    fn maintained_aggregate_test_batch() -> RecordBatch {
        RecordBatch::try_new(
            maintained_aggregate_test_schema(),
            vec![
                Arc::new(StringArray::from(vec![Some("a"), Some("a"), Some("b")])),
                Arc::new(Int64Array::from(vec![Some(1), Some(2), Some(3)])),
            ],
        )
        .expect("test batch should be valid")
    }

    fn maintained_count_aggregate(
        input: Arc<dyn ExecutionPlan>,
        schema: Arc<Schema>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let group_by =
            PhysicalGroupBy::new_single(vec![(col("name", schema.as_ref())?, "name".to_string())]);
        let aggregate_expr = AggregateExprBuilder::new(count_udaf(), vec![lit(1_i8)])
            .schema(Arc::clone(&schema))
            .alias("count(*)".to_string())
            .build()?;
        Ok(Arc::new(AggregateExec::try_new(
            AggregateMode::Single,
            group_by,
            vec![Arc::new(aggregate_expr)],
            vec![None],
            input,
            schema,
        )?))
    }

    #[test]
    fn maintained_aggregate_rewriter_replaces_fresh_matching_aggregate() -> DFResult<()> {
        let schema = maintained_aggregate_test_schema();
        let batch = maintained_aggregate_test_batch();
        let registry = Arc::new(MaintainedAggregateRegistry::try_new(
            &[MaintainedAggregateSpec {
                group_by: vec!["name".to_string()],
                aggregates: vec![MaintainedAggregateExpr {
                    function: MaintainedAggregateFunction::Count,
                    column: None,
                }],
            }],
            &schema,
        )?);
        registry.apply_insert_batches(1, std::slice::from_ref(&batch))?;
        let memory = MemorySourceConfig::try_new_exec(&[vec![batch]], Arc::clone(&schema), None)?;
        let cayenne_scan = Arc::new(CayenneAccelerationExec::new_with_maintained_aggregates(
            memory, registry, 1,
        )) as Arc<dyn ExecutionPlan>;
        let aggregate = maintained_count_aggregate(cayenne_scan, schema)?;

        let optimized = CayenneMaintainedAggregateRewriter::new()
            .optimize(aggregate, &ConfigOptions::default())?;

        assert!(
            optimized
                .as_any()
                .downcast_ref::<MaintainedAggregateExec>()
                .is_some()
        );
        Ok(())
    }

    #[test]
    fn maintained_aggregate_rewriter_preserves_stale_epoch_aggregate() -> DFResult<()> {
        let schema = maintained_aggregate_test_schema();
        let batch = maintained_aggregate_test_batch();
        let registry = Arc::new(MaintainedAggregateRegistry::try_new(
            &[MaintainedAggregateSpec {
                group_by: vec!["name".to_string()],
                aggregates: vec![MaintainedAggregateExpr {
                    function: MaintainedAggregateFunction::Count,
                    column: None,
                }],
            }],
            &schema,
        )?);
        registry.apply_insert_batches(1, std::slice::from_ref(&batch))?;
        registry.mark_stale(2);
        let memory = MemorySourceConfig::try_new_exec(&[vec![batch]], Arc::clone(&schema), None)?;
        let cayenne_scan = Arc::new(CayenneAccelerationExec::new_with_maintained_aggregates(
            memory, registry, 2,
        )) as Arc<dyn ExecutionPlan>;
        let aggregate = maintained_count_aggregate(cayenne_scan, schema)?;

        let optimized = CayenneMaintainedAggregateRewriter::new()
            .optimize(Arc::clone(&aggregate), &ConfigOptions::default())?;

        assert!(
            optimized.as_any().downcast_ref::<AggregateExec>().is_some(),
            "stale maintained aggregate state must not rewrite"
        );
        Ok(())
    }

    #[derive(Clone)]
    struct TestFileSource {
        table_schema: TableSchema,
        filter: Option<Arc<dyn PhysicalExpr>>,
        metrics: ExecutionPlanMetricsSet,
    }

    impl TestFileSource {
        fn new(table_schema: TableSchema, filter: Option<Arc<dyn PhysicalExpr>>) -> Self {
            Self {
                table_schema,
                filter,
                metrics: ExecutionPlanMetricsSet::new(),
            }
        }
    }

    impl FileSource for TestFileSource {
        fn create_file_opener(
            &self,
            _object_store: Arc<dyn ObjectStore>,
            _base_config: &datafusion_datasource::file_scan_config::FileScanConfig,
            _partition: usize,
        ) -> DFResult<Arc<dyn FileOpener>> {
            Err(DataFusionError::NotImplemented(
                "test source cannot open files".to_string(),
            ))
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn table_schema(&self) -> &TableSchema {
            &self.table_schema
        }

        fn with_batch_size(&self, _batch_size: usize) -> Arc<dyn FileSource> {
            Arc::new(self.clone())
        }

        fn filter(&self) -> Option<Arc<dyn PhysicalExpr>> {
            self.filter.clone()
        }

        fn projection(&self) -> Option<&ProjectionExprs> {
            None
        }

        fn metrics(&self) -> &ExecutionPlanMetricsSet {
            &self.metrics
        }

        fn file_type(&self) -> &'static str {
            "test"
        }

        fn fmt_extra(
            &self,
            _t: DisplayFormatType,
            _f: &mut std::fmt::Formatter<'_>,
        ) -> std::fmt::Result {
            Ok(())
        }

        fn try_pushdown_filters(
            &self,
            filters: Vec<Arc<dyn PhysicalExpr>>,
            _config: &ConfigOptions,
        ) -> DFResult<FilterPushdownPropagation<Arc<dyn FileSource>>> {
            let filter_count = filters.len();
            let filter = match &self.filter {
                Some(existing) => Some(conjunction(
                    std::iter::once(Arc::clone(existing)).chain(filters),
                )),
                None => Some(conjunction(filters)),
            };
            let source = Self {
                table_schema: self.table_schema.clone(),
                filter,
                metrics: ExecutionPlanMetricsSet::new(),
            };

            Ok(FilterPushdownPropagation::with_parent_pushdown_result(vec![
                PushedDown::Yes;
                filter_count
            ])
            .with_updated_node(Arc::new(source)))
        }
    }

    fn file_exec(
        schema: &Arc<Schema>,
        path: &str,
        filter: Option<Arc<dyn PhysicalExpr>>,
    ) -> Arc<dyn ExecutionPlan> {
        file_exec_with_statistics(schema, path, filter, Statistics::new_unknown(schema))
    }

    fn file_exec_with_statistics(
        schema: &Arc<Schema>,
        path: &str,
        filter: Option<Arc<dyn PhysicalExpr>>,
        statistics: Statistics,
    ) -> Arc<dyn ExecutionPlan> {
        let table_schema = TableSchema::new(Arc::clone(schema), Vec::new());
        let source = Arc::new(TestFileSource::new(table_schema, filter));
        let file = PartitionedFile::from(ObjectMeta {
            location: Path::from(path),
            last_modified: chrono::DateTime::UNIX_EPOCH,
            size: 1_024,
            e_tag: None,
            version: None,
        });
        let config = FileScanConfigBuilder::new(
            ObjectStoreUrl::parse("file:///").expect("object store url should parse"),
            source,
        )
        .with_file_group(FileGroup::new(vec![file]))
        .with_statistics(statistics)
        .build();

        DataSourceExec::from_data_source(config) as Arc<dyn ExecutionPlan>
    }

    fn cayenne_file_exec(
        schema: &Arc<Schema>,
        path: &str,
        filter: Option<Arc<dyn PhysicalExpr>>,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(CayenneAccelerationExec::new(file_exec(
            schema, path, filter,
        )))
    }

    fn inlined_exec(schema: &Arc<Schema>) -> Arc<dyn ExecutionPlan> {
        MemorySourceConfig::try_new_exec(&[vec![]], Arc::clone(schema), None)
            .expect("inlined memory exec should be valid")
    }

    fn cayenne_file_with_inlined_exec(
        schema: &Arc<Schema>,
        path: &str,
        filter: Option<Arc<dyn PhysicalExpr>>,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(CayenneAccelerationExec::new(
            UnionExec::try_new(vec![file_exec(schema, path, filter), inlined_exec(schema)])
                .expect("mixed file and inlined union should be valid"),
        ))
    }

    fn cayenne_file_exec_with_num_rows(
        schema: &Arc<Schema>,
        path: &str,
        row_count: Precision<usize>,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(CayenneAccelerationExec::new(file_exec_with_statistics(
            schema,
            path,
            None,
            Statistics::new_unknown(schema).with_num_rows(row_count),
        )))
    }

    fn large_exact_cayenne_file_exec(schema: &Arc<Schema>, path: &str) -> Arc<dyn ExecutionPlan> {
        cayenne_file_exec_with_num_rows(
            schema,
            path,
            Precision::Exact(ANTI_JOIN_SORT_MERGE_MIN_EXACT_ROWS + 1),
        )
    }

    fn order_line_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Int64, false),
            Field::new("warehouse_id", DataType::Int64, false),
            Field::new("line_number", DataType::Int64, false),
        ]))
    }

    fn reordered_order_line_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("warehouse_id", DataType::Int64, false),
            Field::new("order_id", DataType::Int64, false),
            Field::new("line_number", DataType::Int64, false),
        ]))
    }

    fn order_line_schema_with_different_non_key_type() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Int64, false),
            Field::new("warehouse_id", DataType::Int64, false),
            Field::new("line_number", DataType::UInt64, false),
        ]))
    }

    fn dynamic_filter_for(column_name: &str, schema: &Schema) -> Arc<dyn PhysicalExpr> {
        Arc::new(DynamicFilterPhysicalExpr::new(
            vec![col(column_name, schema).expect("filter column should exist")],
            lit(true),
        ))
    }

    fn hash_join(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        left_column: &str,
        right_column: &str,
    ) -> HashJoinExec {
        hash_join_with_null_equality(
            left,
            right,
            left_column,
            right_column,
            NullEquality::NullEqualsNothing,
        )
    }

    fn hash_join_with_null_equality(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        left_column: &str,
        right_column: &str,
        null_equality: NullEquality,
    ) -> HashJoinExec {
        hash_join_with_join_type(
            left,
            right,
            left_column,
            right_column,
            JoinType::Inner,
            null_equality,
        )
    }

    fn hash_join_with_join_type(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        left_column: &str,
        right_column: &str,
        join_type: JoinType,
        null_equality: NullEquality,
    ) -> HashJoinExec {
        hash_join_with_join_type_on(
            left,
            right,
            &[(left_column, right_column)],
            join_type,
            null_equality,
        )
    }

    fn hash_join_with_join_type_on(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        columns: &[(&str, &str)],
        join_type: JoinType,
        null_equality: NullEquality,
    ) -> HashJoinExec {
        let on = columns
            .iter()
            .map(|(left_column, right_column)| {
                let left_key =
                    col(left_column, &left.schema()).expect("left join key should exist");
                let right_key =
                    col(right_column, &right.schema()).expect("right join key should exist");
                (left_key, right_key)
            })
            .collect();

        HashJoinExec::try_new(
            left,
            right,
            on,
            None,
            &join_type,
            None,
            PartitionMode::Partitioned,
            null_equality,
            false,
        )
        .expect("hash join should be valid")
    }

    fn optimize_filter_sharing(plan: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        CayenneDynamicFilterSharing::new()
            .optimize(plan, &ConfigOptions::default())
            .expect("filter sharing optimizer should succeed")
    }

    fn optimize_anti_join_sort_merge(plan: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        optimize_anti_join_sort_merge_with_config(plan, &ConfigOptions::default())
    }

    fn optimize_anti_join_sort_merge_with_config(
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Arc<dyn ExecutionPlan> {
        CayenneAntiJoinSortMergeRewriter::new()
            .optimize(plan, config)
            .expect("anti join sort-merge optimizer should succeed")
    }

    fn config_with_cayenne_optimizer(
        sort_merge_min_rows: Option<usize>,
        sort_merge_memory_pool_fraction: Option<f64>,
        sort_merge_memory_pool_bytes: Option<usize>,
    ) -> ConfigOptions {
        let mut config = ConfigOptions::default();
        let mut cayenne_config = CayenneOptimizerConfig::default();
        if let Some(sort_merge_min_rows) = sort_merge_min_rows {
            cayenne_config.sort_merge_min_rows = sort_merge_min_rows;
        }
        if let Some(sort_merge_memory_pool_fraction) = sort_merge_memory_pool_fraction {
            cayenne_config.sort_merge_memory_pool_fraction = sort_merge_memory_pool_fraction;
        }
        cayenne_config.sort_merge_memory_pool_bytes = sort_merge_memory_pool_bytes;
        config.extensions.insert(cayenne_config);
        config
    }

    #[test]
    fn shares_dynamic_filter_across_same_source_equi_joined_cayenne_scans() {
        let schema = order_line_schema();
        let source_filter = dynamic_filter_for("order_id", &schema);
        let left = cayenne_file_exec(
            &schema,
            "order_line.vortex",
            Some(Arc::clone(&source_filter)),
        );
        let right = cayenne_file_exec(&schema, "order_line.vortex", None);
        let join = Arc::new(hash_join(left, right, "order_id", "order_id"));

        let optimized = optimize_filter_sharing(join);
        let join = optimized
            .as_any()
            .downcast_ref::<HashJoinExec>()
            .expect("optimized plan should remain a hash join");
        let right = join
            .right()
            .as_any()
            .downcast_ref::<CayenneAccelerationExec>()
            .expect("right side should remain Cayenne");
        let filters = right.dynamic_filters();

        assert_eq!(1, filters.len());
        assert!(Arc::ptr_eq(filters[0].filter(), &source_filter));
    }

    #[test]
    fn shares_dynamic_filter_with_vortex_branch_of_mixed_inlined_scan() {
        let schema = order_line_schema();
        let source_filter = dynamic_filter_for("order_id", &schema);
        let left = cayenne_file_with_inlined_exec(
            &schema,
            "order_line.vortex",
            Some(Arc::clone(&source_filter)),
        );
        let right = cayenne_file_with_inlined_exec(&schema, "order_line.vortex", None);
        let join = Arc::new(hash_join(left, right, "order_id", "order_id"));

        let optimized = optimize_filter_sharing(join);
        let join = optimized
            .as_any()
            .downcast_ref::<HashJoinExec>()
            .expect("optimized plan should remain a hash join");
        let right = join
            .right()
            .as_any()
            .downcast_ref::<CayenneAccelerationExec>()
            .expect("right side should remain Cayenne");
        let filters = right.dynamic_filters();

        assert_eq!(1, filters.len());
        assert!(Arc::ptr_eq(filters[0].filter(), &source_filter));
    }

    #[test]
    fn does_not_share_dynamic_filter_when_join_does_not_cover_filter_columns() {
        let schema = order_line_schema();
        let source_filter = dynamic_filter_for("line_number", &schema);
        let left = cayenne_file_exec(
            &schema,
            "order_line.vortex",
            Some(Arc::clone(&source_filter)),
        );
        let right = cayenne_file_exec(&schema, "order_line.vortex", None);
        let join = Arc::new(hash_join(left, right, "order_id", "order_id"));

        let optimized = optimize_filter_sharing(join);
        let join = optimized
            .as_any()
            .downcast_ref::<HashJoinExec>()
            .expect("optimized plan should remain a hash join");
        let right = join
            .right()
            .as_any()
            .downcast_ref::<CayenneAccelerationExec>()
            .expect("right side should remain Cayenne");

        assert!(right.dynamic_filters().is_empty());
    }

    #[test]
    fn does_not_share_dynamic_filter_across_different_projection_order() {
        let left_schema = order_line_schema();
        let right_schema = reordered_order_line_schema();
        let source_filter = dynamic_filter_for("order_id", &left_schema);
        let left = cayenne_file_exec(
            &left_schema,
            "order_line.vortex",
            Some(Arc::clone(&source_filter)),
        );
        let right = cayenne_file_exec(&right_schema, "order_line.vortex", None);
        let join = Arc::new(hash_join(left, right, "order_id", "order_id"));

        let optimized = optimize_filter_sharing(join);
        let join = optimized
            .as_any()
            .downcast_ref::<HashJoinExec>()
            .expect("optimized plan should remain a hash join");
        let right = join
            .right()
            .as_any()
            .downcast_ref::<CayenneAccelerationExec>()
            .expect("right side should remain Cayenne");

        assert!(right.dynamic_filters().is_empty());
    }

    #[test]
    fn does_not_share_dynamic_filter_across_different_schema_types() {
        let left_schema = order_line_schema();
        let right_schema = order_line_schema_with_different_non_key_type();
        let source_filter = dynamic_filter_for("order_id", &left_schema);
        let left = cayenne_file_exec(
            &left_schema,
            "order_line.vortex",
            Some(Arc::clone(&source_filter)),
        );
        let right = cayenne_file_exec(&right_schema, "order_line.vortex", None);
        let join = Arc::new(hash_join(left, right, "order_id", "order_id"));

        let optimized = optimize_filter_sharing(join);
        let join = optimized
            .as_any()
            .downcast_ref::<HashJoinExec>()
            .expect("optimized plan should remain a hash join");
        let right = join
            .right()
            .as_any()
            .downcast_ref::<CayenneAccelerationExec>()
            .expect("right side should remain Cayenne");

        assert!(right.dynamic_filters().is_empty());
    }

    #[test]
    fn does_not_apply_filter_addition_to_same_identity_different_projection_order() {
        // `apply_filter_additions` must not push a filter into a scan whose
        // schema fields don't match the source scan exactly (different column
        // ordering / types means the filter's column-by-position indices
        // would refer to wrong columns).
        let source_schema = order_line_schema();
        let target_schema = reordered_order_line_schema();
        let source_filter = dynamic_filter_for("order_id", &source_schema);
        let source = CayenneAccelerationExec::new(file_exec(
            &source_schema,
            "order_line.vortex",
            Some(Arc::clone(&source_filter)),
        ));
        let addition = FilterAddition {
            identity: source
                .scan_identity()
                .expect("source scan should have file identity"),
            schema_fields: plan_schema_fields(&source.schema()),
            filter: Arc::clone(&source_filter),
        };
        let target = cayenne_file_exec(&target_schema, "order_line.vortex", None);

        let (optimized, changed) =
            apply_filter_additions(Arc::clone(&target), &[addition], &ConfigOptions::default())
                .expect("filter addition should be evaluated");

        assert!(!changed);
        let target = optimized
            .as_any()
            .downcast_ref::<CayenneAccelerationExec>()
            .expect("target should remain Cayenne");
        assert!(target.dynamic_filters().is_empty());
    }

    #[test]
    fn does_not_share_dynamic_filter_for_anti_join() {
        // `*Anti` joins must not receive a shared dynamic filter: their
        // output requires the *absence* of a match, so filtering the probe
        // side would drop rows that the anti-join should preserve.
        let schema = order_line_schema();
        let source_filter = dynamic_filter_for("order_id", &schema);
        let left = cayenne_file_exec(
            &schema,
            "order_line.vortex",
            Some(Arc::clone(&source_filter)),
        );
        let right = cayenne_file_exec(&schema, "order_line.vortex", None);
        let join = Arc::new(hash_join_with_join_type(
            left,
            right,
            "order_id",
            "order_id",
            JoinType::RightAnti,
            NullEquality::NullEqualsNothing,
        ));

        let optimized = optimize_filter_sharing(join);
        let join = optimized
            .as_any()
            .downcast_ref::<HashJoinExec>()
            .expect("optimized plan should remain a hash join");
        let right = join
            .right()
            .as_any()
            .downcast_ref::<CayenneAccelerationExec>()
            .expect("right side should remain Cayenne");

        assert!(right.dynamic_filters().is_empty());
    }

    #[test]
    fn does_not_share_dynamic_filter_for_null_equal_inner_join() {
        let schema = order_line_schema();
        let source_filter = dynamic_filter_for("order_id", &schema);
        let left = cayenne_file_exec(
            &schema,
            "order_line.vortex",
            Some(Arc::clone(&source_filter)),
        );
        let right = cayenne_file_exec(&schema, "order_line.vortex", None);
        let join = Arc::new(hash_join_with_null_equality(
            left,
            right,
            "order_id",
            "order_id",
            NullEquality::NullEqualsNull,
        ));

        let optimized = optimize_filter_sharing(join);
        let join = optimized
            .as_any()
            .downcast_ref::<HashJoinExec>()
            .expect("optimized plan should remain a hash join");
        let right = join
            .right()
            .as_any()
            .downcast_ref::<CayenneAccelerationExec>()
            .expect("right side should remain Cayenne");

        assert!(right.dynamic_filters().is_empty());
    }

    #[test]
    fn shares_dynamic_filter_for_left_semi_join() {
        // `LeftSemi` preserves the equi-key domain: a dynamic filter built
        // from the left side is also valid on a same-source equi-joined
        // right scan, since the semi join's output is a subset of the left.
        let schema = order_line_schema();
        let source_filter = dynamic_filter_for("order_id", &schema);
        let left = cayenne_file_exec(
            &schema,
            "order_line.vortex",
            Some(Arc::clone(&source_filter)),
        );
        let right = cayenne_file_exec(&schema, "order_line.vortex", None);
        let join = Arc::new(hash_join_with_join_type(
            left,
            right,
            "order_id",
            "order_id",
            JoinType::LeftSemi,
            NullEquality::NullEqualsNothing,
        ));

        let optimized = optimize_filter_sharing(join);
        let join = optimized
            .as_any()
            .downcast_ref::<HashJoinExec>()
            .expect("optimized plan should remain a hash join");
        let right = join
            .right()
            .as_any()
            .downcast_ref::<CayenneAccelerationExec>()
            .expect("right side should remain Cayenne");
        let filters = right.dynamic_filters();

        assert_eq!(
            1,
            filters.len(),
            "semi join should propagate same-source filter"
        );
        assert!(Arc::ptr_eq(filters[0].filter(), &source_filter));
    }

    #[test]
    fn rewrites_same_source_left_semi_hash_join_to_sort_merge() {
        // Same memory concern as `LeftAnti`: `HashJoinExec` materializes the
        // LEFT input as a non-spillable hash table, and a large same-source
        // semi-join build side risks OOM.
        let schema = order_line_schema();
        let left = large_exact_cayenne_file_exec(&schema, "order_line.vortex");
        let right = large_exact_cayenne_file_exec(&schema, "order_line.vortex");
        let join = Arc::new(hash_join_with_join_type(
            left,
            right,
            "order_id",
            "order_id",
            JoinType::LeftSemi,
            NullEquality::NullEqualsNothing,
        ));

        let optimized = optimize_anti_join_sort_merge(join);
        assert!(
            optimized
                .as_any()
                .downcast_ref::<SortMergeJoinExec>()
                .is_some(),
            "same-source Cayenne LeftSemi join should use sort-merge join"
        );
    }

    #[test]
    fn rewrites_same_source_left_anti_hash_join_to_sort_merge() {
        let schema = order_line_schema();
        let left = large_exact_cayenne_file_exec(&schema, "order_line.vortex");
        let right = large_exact_cayenne_file_exec(&schema, "order_line.vortex");
        let join = Arc::new(hash_join_with_join_type(
            left,
            right,
            "order_id",
            "order_id",
            JoinType::LeftAnti,
            NullEquality::NullEqualsNothing,
        ));

        let optimized = optimize_anti_join_sort_merge(join);
        let sort_merge = optimized
            .as_any()
            .downcast_ref::<SortMergeJoinExec>()
            .expect("same-source Cayenne anti join should use sort-merge join");

        assert_eq!(JoinType::LeftAnti, sort_merge.join_type());
        assert!(
            sort_merge
                .left()
                .as_any()
                .downcast_ref::<SortExec>()
                .is_some(),
            "left anti-join input should be explicitly sorted"
        );
        assert!(
            sort_merge
                .right()
                .as_any()
                .downcast_ref::<SortExec>()
                .is_some(),
            "right anti-join input should be explicitly sorted"
        );
    }

    #[test]
    fn leaves_same_source_inner_hash_join_unchanged_even_when_build_side_is_large() {
        let schema = order_line_schema();
        let left = large_exact_cayenne_file_exec(&schema, "order_line.vortex");
        let right = large_exact_cayenne_file_exec(&schema, "order_line.vortex");
        let join = Arc::new(hash_join_with_join_type(
            left,
            right,
            "order_id",
            "order_id",
            JoinType::Inner,
            NullEquality::NullEqualsNothing,
        ));

        let optimized = optimize_anti_join_sort_merge(join);

        assert!(
            optimized.as_any().downcast_ref::<HashJoinExec>().is_some(),
            "inner joins should stay as hash joins unless a more targeted rule proves a win"
        );
    }

    #[test]
    fn leaves_same_source_left_hash_join_unchanged_even_when_build_side_is_large() {
        let schema = order_line_schema();
        let left = large_exact_cayenne_file_exec(&schema, "order_line.vortex");
        let right = large_exact_cayenne_file_exec(&schema, "order_line.vortex");
        let join = Arc::new(hash_join_with_join_type(
            left,
            right,
            "order_id",
            "order_id",
            JoinType::Left,
            NullEquality::NullEqualsNothing,
        ));

        let optimized = optimize_anti_join_sort_merge(join);

        assert!(
            optimized.as_any().downcast_ref::<HashJoinExec>().is_some(),
            "outer joins should stay as hash joins unless a more targeted rule proves a win"
        );
    }

    #[test]
    fn rewrites_same_source_multi_key_left_anti_hash_join_to_sort_merge() {
        let schema = order_line_schema();
        let left = large_exact_cayenne_file_exec(&schema, "order_line.vortex");
        let right = large_exact_cayenne_file_exec(&schema, "order_line.vortex");
        let join = Arc::new(hash_join_with_join_type_on(
            left,
            right,
            &[("order_id", "order_id"), ("warehouse_id", "warehouse_id")],
            JoinType::LeftAnti,
            NullEquality::NullEqualsNothing,
        ));

        let optimized = optimize_anti_join_sort_merge(join);

        assert!(
            optimized
                .as_any()
                .downcast_ref::<SortMergeJoinExec>()
                .is_some(),
            "multi-key same-source Cayenne anti join should use sort-merge join"
        );
    }

    #[test]
    fn leaves_unrelated_left_anti_hash_join_unchanged() {
        let schema = order_line_schema();
        let left = large_exact_cayenne_file_exec(&schema, "order_line.vortex");
        let right = large_exact_cayenne_file_exec(&schema, "other_order_line.vortex");
        let join = Arc::new(hash_join_with_join_type(
            left,
            right,
            "order_id",
            "order_id",
            JoinType::LeftAnti,
            NullEquality::NullEqualsNothing,
        ));

        let optimized = optimize_anti_join_sort_merge(join);

        assert!(
            optimized.as_any().downcast_ref::<HashJoinExec>().is_some(),
            "anti joins over unrelated sources should stay as hash joins"
        );
    }

    #[test]
    fn leaves_unrelated_inner_hash_join_unchanged() {
        let schema = order_line_schema();
        let left = large_exact_cayenne_file_exec(&schema, "order_line.vortex");
        let right = large_exact_cayenne_file_exec(&schema, "other_order_line.vortex");
        let join = Arc::new(hash_join_with_join_type(
            left,
            right,
            "order_id",
            "order_id",
            JoinType::Inner,
            NullEquality::NullEqualsNothing,
        ));

        let optimized = optimize_anti_join_sort_merge(join);

        assert!(
            optimized.as_any().downcast_ref::<HashJoinExec>().is_some(),
            "inner joins over unrelated sources should stay as hash joins"
        );
    }

    #[test]
    fn leaves_exact_small_same_source_left_anti_hash_join_unchanged() {
        let schema = order_line_schema();
        let left = cayenne_file_exec_with_num_rows(
            &schema,
            "order_line.vortex",
            Precision::Exact(ANTI_JOIN_SORT_MERGE_MIN_EXACT_ROWS),
        );
        let right = large_exact_cayenne_file_exec(&schema, "order_line.vortex");
        let join = Arc::new(hash_join_with_join_type(
            left,
            right,
            "order_id",
            "order_id",
            JoinType::LeftAnti,
            NullEquality::NullEqualsNothing,
        ));

        let optimized = optimize_anti_join_sort_merge(join);

        assert!(
            optimized.as_any().downcast_ref::<HashJoinExec>().is_some(),
            "same-source anti joins at or below the large-input threshold should stay as hash joins"
        );
    }

    #[test]
    fn leaves_null_equal_same_source_left_anti_hash_join_unchanged() {
        let schema = order_line_schema();
        let left = large_exact_cayenne_file_exec(&schema, "order_line.vortex");
        let right = large_exact_cayenne_file_exec(&schema, "order_line.vortex");
        let join = Arc::new(hash_join_with_join_type(
            left,
            right,
            "order_id",
            "order_id",
            JoinType::LeftAnti,
            NullEquality::NullEqualsNull,
        ));

        let optimized = optimize_anti_join_sort_merge(join);

        assert!(
            optimized.as_any().downcast_ref::<HashJoinExec>().is_some(),
            "null-equal anti joins should stay as hash joins"
        );
    }

    #[test]
    fn leaves_same_source_left_anti_hash_join_when_configured_min_rows_is_higher() {
        let schema = order_line_schema();
        let left = large_exact_cayenne_file_exec(&schema, "order_line.vortex");
        let right = large_exact_cayenne_file_exec(&schema, "order_line.vortex");
        let join = Arc::new(hash_join_with_join_type(
            left,
            right,
            "order_id",
            "order_id",
            JoinType::LeftAnti,
            NullEquality::NullEqualsNothing,
        ));
        let config = config_with_cayenne_optimizer(
            Some(ANTI_JOIN_SORT_MERGE_MIN_EXACT_ROWS + 2),
            None,
            None,
        );

        let optimized = optimize_anti_join_sort_merge_with_config(join, &config);

        assert!(
            optimized.as_any().downcast_ref::<HashJoinExec>().is_some(),
            "configured min-row threshold should keep smaller build sides as hash joins"
        );
    }

    /// Wide-build regression: low row count but wide projection produces a
    /// build big enough to OOM `HashJoinExec`'s non-spillable hash table. The byte gate
    /// must catch this case even though the row count is below
    /// `sort_merge_min_rows`.
    #[test]
    fn rewrites_low_row_count_wide_build_when_byte_estimate_exceeds_memory_gate() {
        let schema = order_line_schema();
        // 200K rows × ~24 bytes/row ≈ 4.8 MB, plus inflated overhead from
        // `build_side_memory_estimate`'s hash-table overhead factor. Well below
        // the 10M row threshold but well above the 64 KB byte gate below.
        let small_rows = Precision::Exact(200_000);
        let left = cayenne_file_exec_with_num_rows(&schema, "order_line.vortex", small_rows);
        let right = cayenne_file_exec_with_num_rows(&schema, "order_line.vortex", small_rows);
        let join = Arc::new(hash_join_with_join_type(
            left,
            right,
            "order_id",
            "order_id",
            JoinType::LeftAnti,
            NullEquality::NullEqualsNothing,
        ));
        // 64 KB pool × 0.125 = 8 KB byte gate. The 200K-row build is enormously
        // above that, so the rewrite should fire despite row count below 10M.
        let config = config_with_cayenne_optimizer(None, Some(0.125), Some(64 * 1024));

        let optimized = optimize_anti_join_sort_merge_with_config(join, &config);

        assert!(
            optimized
                .as_any()
                .downcast_ref::<SortMergeJoinExec>()
                .is_some(),
            "low-row-count + wide-row build exceeding the byte gate should be rewritten to sort-merge"
        );
    }

    #[test]
    fn leaves_same_source_left_anti_hash_join_when_build_estimate_fits_memory_gate() {
        let schema = order_line_schema();
        let left = large_exact_cayenne_file_exec(&schema, "order_line.vortex");
        let right = large_exact_cayenne_file_exec(&schema, "order_line.vortex");
        let join = Arc::new(hash_join_with_join_type(
            left,
            right,
            "order_id",
            "order_id",
            JoinType::LeftAnti,
            NullEquality::NullEqualsNothing,
        ));
        let config = config_with_cayenne_optimizer(None, Some(0.125), Some(4 * 1024 * 1024 * 1024));

        let optimized = optimize_anti_join_sort_merge_with_config(join, &config);

        assert!(
            optimized.as_any().downcast_ref::<HashJoinExec>().is_some(),
            "estimated build side within the configured memory fraction should stay a hash join"
        );
    }

    #[test]
    fn rewrites_same_source_left_anti_hash_join_when_build_estimate_exceeds_memory_gate() {
        let schema = order_line_schema();
        let left = large_exact_cayenne_file_exec(&schema, "order_line.vortex");
        let right = large_exact_cayenne_file_exec(&schema, "order_line.vortex");
        let join = Arc::new(hash_join_with_join_type(
            left,
            right,
            "order_id",
            "order_id",
            JoinType::LeftAnti,
            NullEquality::NullEqualsNothing,
        ));
        let config = config_with_cayenne_optimizer(None, Some(0.125), Some(64 * 1024 * 1024));

        let optimized = optimize_anti_join_sort_merge_with_config(join, &config);

        assert!(
            optimized
                .as_any()
                .downcast_ref::<SortMergeJoinExec>()
                .is_some(),
            "estimated build side above the configured memory fraction should use sort-merge"
        );
    }

    #[test]
    fn leaves_inexact_same_source_left_anti_hash_join_unchanged() {
        let schema = order_line_schema();
        let left = cayenne_file_exec_with_num_rows(
            &schema,
            "order_line.vortex",
            Precision::Inexact(ANTI_JOIN_SORT_MERGE_MIN_EXACT_ROWS + 1),
        );
        let right = large_exact_cayenne_file_exec(&schema, "order_line.vortex");
        let join = Arc::new(hash_join_with_join_type(
            left,
            right,
            "order_id",
            "order_id",
            JoinType::LeftAnti,
            NullEquality::NullEqualsNothing,
        ));

        let optimized = optimize_anti_join_sort_merge(join);

        assert!(
            optimized.as_any().downcast_ref::<HashJoinExec>().is_some(),
            "same-source anti joins with inexact preserved-side stats should stay as hash joins"
        );
    }

    #[test]
    fn leaves_unknown_same_source_left_anti_hash_join_unchanged() {
        let schema = order_line_schema();
        let left = cayenne_file_exec(&schema, "order_line.vortex", None);
        let right = large_exact_cayenne_file_exec(&schema, "order_line.vortex");
        let join = Arc::new(hash_join_with_join_type(
            left,
            right,
            "order_id",
            "order_id",
            JoinType::LeftAnti,
            NullEquality::NullEqualsNothing,
        ));

        let optimized = optimize_anti_join_sort_merge(join);

        assert!(
            optimized.as_any().downcast_ref::<HashJoinExec>().is_some(),
            "same-source anti joins with unknown preserved-side stats should stay as hash joins"
        );
    }

    #[test]
    fn rewrites_right_anti_hash_join_when_build_side_stats_are_exact_large() {
        let schema = order_line_schema();
        let left = large_exact_cayenne_file_exec(&schema, "order_line.vortex");
        let right = cayenne_file_exec(&schema, "order_line.vortex", None);
        let join = Arc::new(hash_join_with_join_type(
            left,
            right,
            "order_id",
            "order_id",
            JoinType::RightAnti,
            NullEquality::NullEqualsNothing,
        ));

        let optimized = optimize_anti_join_sort_merge(join);

        assert!(
            optimized
                .as_any()
                .downcast_ref::<SortMergeJoinExec>()
                .is_some(),
            "RightAnti should gate on the left build side, not the right preserved side"
        );
    }

    #[test]
    fn leaves_right_anti_hash_join_when_build_side_stats_are_unknown() {
        let schema = order_line_schema();
        let left = cayenne_file_exec(&schema, "order_line.vortex", None);
        let right = large_exact_cayenne_file_exec(&schema, "order_line.vortex");
        let join = Arc::new(hash_join_with_join_type(
            left,
            right,
            "order_id",
            "order_id",
            JoinType::RightAnti,
            NullEquality::NullEqualsNothing,
        ));

        let optimized = optimize_anti_join_sort_merge(join);

        assert!(
            optimized.as_any().downcast_ref::<HashJoinExec>().is_some(),
            "RightAnti should stay hash join when the left build side has unknown stats"
        );
    }

    /// Recursively find the first `CayenneAccelerationExec` in a plan tree.
    /// Collects the dynamic filters attached to the first `CayenneAccelerationExec`
    /// found anywhere in `plan` (depth-first). Returns an empty vec if there is no
    /// Cayenne scan or it carries no dynamic filters.
    fn cayenne_scan_dynamic_filters(plan: &Arc<dyn ExecutionPlan>) -> Vec<ScanDynamicFilter> {
        if let Some(cayenne) = plan.as_any().downcast_ref::<CayenneAccelerationExec>() {
            return cayenne.dynamic_filters();
        }
        for child in plan.children() {
            let filters = cayenne_scan_dynamic_filters(child);
            if !filters.is_empty()
                || child
                    .as_any()
                    .downcast_ref::<CayenneAccelerationExec>()
                    .is_some()
            {
                return filters;
            }
        }
        Vec::new()
    }

    /// Runs `DataFusion`'s Post-phase physical filter pushdown — the phase that
    /// plants hash-join dynamic filters into probe-side scans.
    fn push_down_filters(
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Arc<dyn ExecutionPlan> {
        use datafusion::physical_optimizer::filter_pushdown::FilterPushdown;
        FilterPushdown::new_post_optimization()
            .optimize(plan, config)
            .expect("post-optimization filter pushdown should succeed")
    }

    /// Regression test for the DF53 *native* hash-join dynamic-filter pushdown
    /// that replaced the forked `ExactLeftAccumulator` accumulator seam.
    ///
    /// For an inner join whose probe (right) side is a `CayenneAccelerationExec`,
    /// `DataFusion`'s Post-phase `FilterPushdown` must plant a
    /// `DynamicFilterPhysicalExpr` into the Cayenne scan's underlying file
    /// source. This is the effect the old `CayenneJoinRewriter` rule used to
    /// secure via a custom accumulator; the native path now provides it (with a
    /// min/max bounds + InList/hash-table membership filter) with no
    /// Cayenne-specific physical rule.
    #[test]
    fn native_inner_join_plants_dynamic_filter_into_cayenne_probe_scan() {
        let schema = order_line_schema();
        let build = file_exec(&schema, "build.vortex", None);
        let probe = cayenne_file_exec(&schema, "probe.vortex", None);
        let join: Arc<dyn ExecutionPlan> =
            Arc::new(hash_join(build, probe, "order_id", "order_id"));

        // Default config keeps `optimizer.enable_join_dynamic_filter_pushdown`
        // (and the umbrella `enable_dynamic_filter_pushdown`) enabled.
        let config = ConfigOptions::default();
        assert!(
            config.optimizer.enable_join_dynamic_filter_pushdown,
            "native join dynamic-filter pushdown is expected to default on"
        );

        let optimized = push_down_filters(join, &config);

        let filters = cayenne_scan_dynamic_filters(&optimized);

        assert!(
            !filters.is_empty(),
            "DataFusion's native inner-join dynamic filter should reach the Cayenne probe scan; \
             got plan: {}",
            displayable(optimized.as_ref()).indent(true)
        );
        assert!(
            filters.iter().any(|f| f.columns().contains("order_id")),
            "the planted dynamic filter should reference the equi-join key column"
        );
    }

    /// Companion to the inner-join regression: `DataFusion` only pushes
    /// join-derived dynamic filters through **inner** joins
    /// (`HashJoinExec::allow_join_dynamic_filter_pushdown`). A semi join must
    /// therefore *not* plant a dynamic filter into the Cayenne scan — the OOM
    /// mitigation for semi-join shapes (e.g. CH-benCH Q18) is handled by the
    /// separate logical `CayennePushDownSemiJoin` rule and by
    /// `CayenneAntiJoinSortMergeRewriter`, not by this pushdown path.
    #[test]
    fn native_semi_join_does_not_plant_dynamic_filter_into_cayenne_probe_scan() {
        let schema = order_line_schema();
        let build = file_exec(&schema, "build.vortex", None);
        let probe = cayenne_file_exec(&schema, "probe.vortex", None);
        let join: Arc<dyn ExecutionPlan> = Arc::new(hash_join_with_join_type(
            build,
            probe,
            "order_id",
            "order_id",
            JoinType::RightSemi,
            NullEquality::NullEqualsNothing,
        ));

        let optimized = push_down_filters(join, &ConfigOptions::default());

        assert!(
            cayenne_scan_dynamic_filters(&optimized).is_empty(),
            "DataFusion does not push join dynamic filters through semi joins"
        );
    }

    /// q78-style: a large *inner* hash join whose non-spillable build side would
    /// exhaust the pool is rewritten to a spillable sort-merge join. The legacy
    /// semi/anti gate skipped inner joins entirely.
    #[test]
    fn rewrites_large_inner_hash_join_under_memory_gate() {
        let schema = order_line_schema();
        let left = large_exact_cayenne_file_exec(&schema, "order_line.vortex");
        let right = large_exact_cayenne_file_exec(&schema, "store_sales.vortex");
        let join = Arc::new(hash_join_with_join_type(
            left,
            right,
            "order_id",
            "order_id",
            JoinType::Inner,
            NullEquality::NullEqualsNothing,
        ));
        // 64 MiB pool × 0.125 = 8 MiB gate; the ~240 MB build is far above it.
        let config = config_with_cayenne_optimizer(None, Some(0.125), Some(64 * 1024 * 1024));

        let optimized = optimize_anti_join_sort_merge_with_config(join, &config);

        assert!(
            optimized
                .as_any()
                .downcast_ref::<SortMergeJoinExec>()
                .is_some(),
            "a large inner hash join over the memory gate should become a sort-merge join"
        );
    }

    /// q97-style: a large *full outer* join is rewritten too.
    #[test]
    fn rewrites_large_full_outer_hash_join_under_memory_gate() {
        let schema = order_line_schema();
        let left = large_exact_cayenne_file_exec(&schema, "store_sales.vortex");
        let right = large_exact_cayenne_file_exec(&schema, "catalog_sales.vortex");
        let join = Arc::new(hash_join_with_join_type(
            left,
            right,
            "order_id",
            "order_id",
            JoinType::Full,
            NullEquality::NullEqualsNothing,
        ));
        let config = config_with_cayenne_optimizer(None, Some(0.125), Some(64 * 1024 * 1024));

        let optimized = optimize_anti_join_sort_merge_with_config(join, &config);

        assert!(
            optimized
                .as_any()
                .downcast_ref::<SortMergeJoinExec>()
                .is_some(),
            "a large full-outer hash join over the memory gate should become a sort-merge join"
        );
    }

    /// The build side of a deep join is rarely `Precision::Exact`; the memory
    /// gate must still fire on an inexact estimate (the legacy path requires
    /// exact rows and would skip this — the key q78 enabler).
    #[test]
    fn rewrites_inexact_build_inner_hash_join_under_memory_gate() {
        let schema = order_line_schema();
        let left = cayenne_file_exec_with_num_rows(
            &schema,
            "order_line.vortex",
            Precision::Inexact(ANTI_JOIN_SORT_MERGE_MIN_EXACT_ROWS + 1),
        );
        let right = large_exact_cayenne_file_exec(&schema, "store_sales.vortex");
        let join = Arc::new(hash_join_with_join_type(
            left,
            right,
            "order_id",
            "order_id",
            JoinType::Inner,
            NullEquality::NullEqualsNothing,
        ));
        let config = config_with_cayenne_optimizer(None, Some(0.125), Some(64 * 1024 * 1024));

        let optimized = optimize_anti_join_sort_merge_with_config(join, &config);

        assert!(
            optimized
                .as_any()
                .downcast_ref::<SortMergeJoinExec>()
                .is_some(),
            "an inexact-but-large inner build side should still be rewritten under the memory gate"
        );
    }

    /// The memory-gated path no longer requires both sides to share a Cayenne
    /// source; cross-table joins (the common analytical case) are eligible.
    #[test]
    fn rewrites_unrelated_inner_hash_join_under_memory_gate() {
        let schema = order_line_schema();
        let left = large_exact_cayenne_file_exec(&schema, "order_line.vortex");
        let right = large_exact_cayenne_file_exec(&schema, "other_order_line.vortex");
        let join = Arc::new(hash_join_with_join_type(
            left,
            right,
            "order_id",
            "order_id",
            JoinType::Inner,
            NullEquality::NullEqualsNothing,
        ));
        let config = config_with_cayenne_optimizer(None, Some(0.125), Some(64 * 1024 * 1024));

        let optimized = optimize_anti_join_sort_merge_with_config(join, &config);

        assert!(
            optimized
                .as_any()
                .downcast_ref::<SortMergeJoinExec>()
                .is_some(),
            "different-source inner joins are eligible under the memory gate"
        );
    }

    /// Stays scoped to Cayenne plans: a large join with no Cayenne scan on
    /// either side is left to the `prefer_hash_join` knob, not this rewriter.
    #[test]
    fn leaves_non_cayenne_inner_hash_join_under_memory_gate_unchanged() {
        let schema = order_line_schema();
        let big = || {
            file_exec_with_statistics(
                &schema,
                "external.parquet",
                None,
                Statistics::new_unknown(&schema)
                    .with_num_rows(Precision::Exact(ANTI_JOIN_SORT_MERGE_MIN_EXACT_ROWS + 1)),
            )
        };
        let join = Arc::new(hash_join_with_join_type(
            big(),
            big(),
            "order_id",
            "order_id",
            JoinType::Inner,
            NullEquality::NullEqualsNothing,
        ));
        let config = config_with_cayenne_optimizer(None, Some(0.125), Some(64 * 1024 * 1024));

        let optimized = optimize_anti_join_sort_merge_with_config(join, &config);

        assert!(
            optimized.as_any().downcast_ref::<HashJoinExec>().is_some(),
            "non-Cayenne joins are left to the prefer_hash_join knob, not the Cayenne rewriter"
        );
    }

    /// A single inner join whose build side fits the absolute pool fraction
    /// stays a hash join...
    #[test]
    fn leaves_single_inner_hash_join_within_pool_fraction() {
        let schema = order_line_schema();
        let join = Arc::new(hash_join_with_join_type(
            large_exact_cayenne_file_exec(&schema, "a.vortex"),
            large_exact_cayenne_file_exec(&schema, "b.vortex"),
            "order_id",
            "order_id",
            JoinType::Inner,
            NullEquality::NullEqualsNothing,
        ));
        // ~240 MB build < 0.9 × 400 MiB ≈ 360 MiB gate, and below the full pool.
        let config = config_with_cayenne_optimizer(None, Some(0.9), Some(400 * 1024 * 1024));

        let optimized = optimize_anti_join_sort_merge_with_config(join, &config);

        assert!(
            optimized.as_any().downcast_ref::<HashJoinExec>().is_some(),
            "a lone inner join within the pool fraction should stay a hash join"
        );
    }

    /// ...but two such joins, each within the absolute fraction, exceed their
    /// fair share of the pool and are both rewritten (the q78 sum-of-build-sides
    /// failure mode that a single-join gate misses).
    #[test]
    fn rewrites_concurrent_inner_hash_joins_exceeding_fair_share() {
        let schema = order_line_schema();
        let make_join = |a: &str, b: &str| {
            Arc::new(hash_join_with_join_type(
                large_exact_cayenne_file_exec(&schema, a),
                large_exact_cayenne_file_exec(&schema, b),
                "order_id",
                "order_id",
                JoinType::Inner,
                NullEquality::NullEqualsNothing,
            )) as Arc<dyn ExecutionPlan>
        };
        let plan = UnionExec::try_new(vec![
            make_join("a.vortex", "b.vortex"),
            make_join("c.vortex", "d.vortex"),
        ])
        .expect("union of two same-schema joins should be valid");
        // Same 0.9 × 400 MiB config: each ~240 MB build is under the ~360 MiB
        // absolute gate but over its 200 MiB fair share (pool / 2 joins).
        let config = config_with_cayenne_optimizer(None, Some(0.9), Some(400 * 1024 * 1024));

        let optimized = optimize_anti_join_sort_merge_with_config(plan, &config);

        let union = optimized
            .as_any()
            .downcast_ref::<UnionExec>()
            .expect("top node should remain a union");
        assert_eq!(union.children().len(), 2, "union should keep both joins");
        for child in union.children() {
            assert!(
                child
                    .as_any()
                    .downcast_ref::<SortMergeJoinExec>()
                    .is_some(),
                "each concurrent inner join should be rewritten to sort-merge under fair-share"
            );
        }
    }
}
