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
//! # No-spill build-side memory strategy (q21 / chbench multi-way joins)
//!
//! `DataFusion`'s `HashJoinExec` build side is non-spillable. Under the runtime
//! memory pool (`GreedyMemoryPool` wrapped in `TrackConsumersPool`), wide chbench
//! shapes such as q21 (a 5-way join feeding a correlated `NOT EXISTS` self-join
//! over `order_line`) exhaust the `HashJoinInput[N]` reservations because each
//! build-side hash table independently materializes its full keyspace.
//!
//! The q21 fix is layered so each optimizer rule handles the part `DataFusion`
//! cannot currently spill or infer on its own:
//!
//! 1. **Logical predicate propagation.**
//!    [`crate::logical_optimizer::CayennePropagateFilterAcrossEquiJoinKeys`]
//!    introduces explicit `InSubquery` filters for equi-join keys when the
//!    selective predicate is on a non-key column. `DataFusion`'s stock
//!    `infer_join_predicates` only fires when the predicate already references
//!    a join key (`WHERE n_nationkey = 5` → `WHERE s_nationkey = 5`). For q21
//!    the filter is `n_name = 'CHINA'`, so the Cayenne rule exposes the
//!    `nation → supplier → stock/order_line` cardinality bound before
//!    `push_down_filter` plants it into scans.
//!
//! 2. **Cross-scan dynamic filter sharing.** When a join's
//!    `Arc<DynamicFilterPhysicalExpr>` is pushed into one
//!    `CayenneAccelerationExec`, [`CayenneDynamicFilterSharing`] installs the
//!    same `Arc` on sibling `CayenneAccelerationExec`s backed by the same
//!    underlying table and equi-joined column set. The shared `Arc` carries the
//!    same `Arc<RwLock<Inner>>` state, so all sibling scans observe the exact
//!    filter values as soon as the producing join accumulates them.
//!
//! 3. **Same-source anti-join sort-merge rewrite.** `DataFusion` does not create
//!    dynamic filters for anti joins, and q21's `NOT EXISTS` self-join can leave
//!    large `HashJoinInput[N]` reservations behind. [`CayenneAntiJoinSortMergeRewriter`]
//!    rewrites same-source Cayenne `LeftAnti` / `RightAnti` `HashJoinExec`
//!    nodes to `SortMergeJoinExec` with explicit spillable `SortExec` inputs,
//!    preserving anti-join semantics without materializing a full non-spillable
//!    build hash table.
//!
//! [`CayenneJoinRewriter`] still handles the ordinary inner-join probe side by
//! swapping the default in-list accumulator for [`ExactLeftAccumulator`], which
//! produces a precise dynamic filter (or falls back to `RangeBounds` +
//! `BloomFilter`) that `DataFusion`'s filter-pushdown phase plants into the
//! right-side `CayenneAccelerationExec`'s `FileSource`.
//!
//! ## Audit notes (verified 2026-05-14 against the q21 explain snapshot at
//! `crates/test-framework/src/snapshot/snapshots/explain/test_framework__snapshot__file[parquet]-cayenne[file]-indexes_tpch_q21_explain.snap`)
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
//!   inner `DataSourceExec`, so the value reaches `JoinSelection`. The q21
//!   explain plan confirms `should_swap_join_order` picks the smaller side as
//!   build at every level (nation/supplier on the LEFT, lineitem on the
//!   RIGHT), so the residual OOM is *not* attributable to fuzzy stats — it is
//!   the **logical** join order locking in the SQL `FROM` order and applying
//!   the nation filter last.
//!
//! * **Build-side projections are minimal.** Every `CayenneAccelerationExec`
//!   in the snapshot terminates in a `DataSourceExec` whose `projection=[...]`
//!   lists only the join keys and the columns referenced above the join.
//!   `DataFusion`'s stock projection pushdown already prunes wider scans down to
//!   `[s_suppkey, s_name, s_nationkey]`, `[o_orderkey, o_orderstatus]`,
//!   `[l_orderkey, l_suppkey]`, etc. No additional `ProjectionExec` insertion
//!   above the build side is required.
//!
//! With these layers active, q21 is included in
//! `test_framework::queries::get_chbench_test_queries`.

use arrow::compute::SortOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{JoinType, NullEquality};
use datafusion::config::ConfigOptions;
use datafusion::error::DataFusionError;
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::joins::{HashJoinExec, SortMergeJoinExec};
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::{error::Result, physical_plan::projection::ProjectionExec};
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion_physical_plan::repartition::RepartitionExec;
use runtime_datafusion::execution_plan::schema_cast::SchemaCastScanExec;
use runtime_datafusion::extension::bytes_processed::BytesProcessedExec;
use runtime_datafusion::join_accumulator::ExactLeftAccumulator;
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use crate::provider::CayenneAccelerationExec;
use crate::provider::scan::{IsCayenneAccelerationExec, ScanDynamicFilter, ScanIdentity};

/// Optimizer rule that rewrites `HashJoinExec` nodes to use `ExactLeftAccumulator`
/// when the probe side is a `CayenneAccelerationExec`.
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

impl std::fmt::Debug for CayenneDynamicFilterSharing {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CayenneDynamicFilterSharing").finish()
    }
}

/// Rewrites same-source Cayenne anti joins from hash join to sort-merge join.
///
/// `DataFusion`'s `HashJoinExec` always materializes its left input as the
/// non-spillable build side. For q21's correlated `NOT EXISTS` self-join, that
/// preserved side can be a large multi-way `order_line` result. Sort-merge join
/// keeps anti-join semantics without allocating a full hash table for that side.
#[derive(Default)]
pub struct CayenneAntiJoinSortMergeRewriter;

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
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        plan.transform_down(|node| {
            let Some(hash_join) = node.as_any().downcast_ref::<HashJoinExec>() else {
                return Ok(Transformed::no(node));
            };

            let Some(sort_merge_join) = try_rewrite_same_source_anti_join(hash_join)? else {
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

#[derive(Clone)]
struct CayenneScanSummary {
    identity: ScanIdentity,
    columns: BTreeSet<String>,
    dynamic_filters: Vec<ScanDynamicFilter>,
}

#[derive(Clone)]
struct FilterAddition {
    identity: ScanIdentity,
    filter: Arc<dyn PhysicalExpr>,
}

fn filter_additions_for_join(
    hash_join: &HashJoinExec,
) -> (Vec<FilterAddition>, Vec<FilterAddition>) {
    let left_scans = collect_cayenne_scans(hash_join.left());
    let right_scans = collect_cayenne_scans(hash_join.right());
    if left_scans.is_empty() || right_scans.is_empty() {
        return (Vec::new(), Vec::new());
    }

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

        let matching_pairs =
            same_source_pairs_for_column(&left_scans, &right_scans, left_column, right_column);
        let [(left_index, right_index)] = matching_pairs.as_slice() else {
            continue;
        };

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
                    right_scan.identity.clone(),
                    Arc::clone(filter.filter()),
                );
            }
        }

        for filter in &right_scan.dynamic_filters {
            if filter.columns().is_subset(&shared_columns) {
                push_filter_addition(
                    &mut left_additions,
                    left_scan.identity.clone(),
                    Arc::clone(filter.filter()),
                );
            }
        }
    }

    (left_additions, right_additions)
}

fn try_rewrite_same_source_anti_join(
    hash_join: &HashJoinExec,
) -> Result<Option<Arc<dyn ExecutionPlan>>, DataFusionError> {
    if !matches!(
        hash_join.join_type(),
        JoinType::LeftAnti | JoinType::RightAnti
    ) {
        return Ok(None);
    }

    if hash_join.contains_projection() || hash_join.on().is_empty() {
        return Ok(None);
    }

    if !has_single_same_source_pair_for_all_join_keys(hash_join) {
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
        "Replacing same-source Cayenne anti HashJoinExec with SortMergeJoinExec"
    );

    Ok(Some(Arc::new(join)))
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

        let pairs =
            same_source_pairs_for_column(&left_scans, &right_scans, left_column, right_column);
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
        let columns = cayenne
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect();
        scans.push(CayenneScanSummary {
            identity,
            columns,
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

fn same_source_pairs_for_column(
    left_scans: &[CayenneScanSummary],
    right_scans: &[CayenneScanSummary],
    left_column: &str,
    right_column: &str,
) -> Vec<(usize, usize)> {
    let mut pairs = Vec::new();

    for (left_index, left_scan) in left_scans.iter().enumerate() {
        if !left_scan.columns.contains(left_column) {
            continue;
        }

        for (right_index, right_scan) in right_scans.iter().enumerate() {
            if left_scan.identity == right_scan.identity
                && right_scan.columns.contains(right_column)
            {
                pairs.push((left_index, right_index));
            }
        }
    }

    pairs
}

fn push_filter_addition(
    additions: &mut Vec<FilterAddition>,
    identity: ScanIdentity,
    filter: Arc<dyn PhysicalExpr>,
) {
    if additions
        .iter()
        .any(|addition| addition.identity == identity && Arc::ptr_eq(&addition.filter, &filter))
    {
        return;
    }

    additions.push(FilterAddition { identity, filter });
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
        let filters = additions
            .iter()
            .filter(|addition| addition.identity == identity)
            .filter(|addition| !cayenne.has_dynamic_filter(&addition.filter))
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

    if let Some(coalesce) = plan.as_any().downcast_ref::<CoalesceBatchesExec>() {
        return flatten_transparent_nodes(coalesce.input());
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

    if build_side.is_cayenne_acceleration_exec() {
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
        _config: &ConfigOptions,
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

            if !is_cayenne_backed_join(hash_join) {
                return Ok(Transformed::no(node));
            }

            if hash_join.null_equality() != NullEquality::NullEqualsNothing {
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
        CayenneAntiJoinSortMergeRewriter, CayenneDynamicFilterSharing, CayenneJoinRewriter,
    };
    use crate::provider::CayenneAccelerationExec;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::{JoinType, NullEquality};
    use datafusion::config::ConfigOptions;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::physical_optimizer::PhysicalOptimizerRule;
    use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode, SortMergeJoinExec};
    use datafusion::physical_plan::projection::ProjectionExec;
    use datafusion::physical_plan::sorts::sort::SortExec;
    use datafusion::physical_plan::{ExecutionPlan, displayable};
    use datafusion_common::{DataFusionError, Result as DFResult};
    use datafusion_datasource::file::FileSource;
    use datafusion_datasource::file_groups::FileGroup;
    use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
    use datafusion_datasource::file_stream::FileOpener;
    use datafusion_datasource::source::DataSourceExec;
    use datafusion_datasource::{PartitionedFile, TableSchema};
    use datafusion_physical_expr::expressions::{DynamicFilterPhysicalExpr, col, lit};
    use datafusion_physical_expr::projection::ProjectionExprs;
    use datafusion_physical_expr::{PhysicalExpr, conjunction};
    use datafusion_physical_plan::DisplayFormatType;
    use datafusion_physical_plan::filter_pushdown::{FilterPushdownPropagation, PushedDown};
    use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
    use object_store::ObjectMeta;
    use object_store::ObjectStore;
    use object_store::path::Path;
    use runtime_datafusion::join_accumulator::ExactLeftAccumulator;
    use std::any::Any;
    use std::sync::Arc;

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

    fn memory_exec(column_name: &str) -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            column_name,
            DataType::Int32,
            false,
        )]));
        MemorySourceConfig::try_new_exec(&[vec![]], schema, None)
            .expect("memory exec should be valid")
    }

    fn file_exec(
        schema: &Arc<Schema>,
        path: &str,
        filter: Option<Arc<dyn PhysicalExpr>>,
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

    fn order_line_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Int64, false),
            Field::new("warehouse_id", DataType::Int64, false),
            Field::new("line_number", DataType::Int64, false),
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
        )
        .expect("hash join should be valid")
    }

    fn join_with_right(right: Arc<dyn ExecutionPlan>) -> HashJoinExec {
        hash_join(memory_exec("left_id"), right, "left_id", "right_id")
    }

    fn optimize(plan: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        CayenneJoinRewriter::new()
            .optimize(plan, &ConfigOptions::default())
            .expect("optimizer should succeed")
    }

    fn optimize_filter_sharing(plan: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        CayenneDynamicFilterSharing::new()
            .optimize(plan, &ConfigOptions::default())
            .expect("filter sharing optimizer should succeed")
    }

    fn optimize_anti_join_sort_merge(plan: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        CayenneAntiJoinSortMergeRewriter::new()
            .optimize(plan, &ConfigOptions::default())
            .expect("anti join sort-merge optimizer should succeed")
    }

    fn plan_snapshot(plan: &Arc<dyn ExecutionPlan>) -> String {
        displayable(plan.as_ref()).indent(true).to_string()
    }

    #[test]
    fn rewrites_hash_join_with_cayenne_probe_side() {
        let right = Arc::new(CayenneAccelerationExec::new(memory_exec("right_id")));
        let join = Arc::new(join_with_right(right));

        let optimized = optimize(join);

        assert!(
            optimized
                .as_any()
                .downcast_ref::<HashJoinExec<ExactLeftAccumulator>>()
                .is_some(),
            "Cayenne-backed joins should use ExactLeftAccumulator"
        );
    }

    #[test]
    fn leaves_hash_join_without_cayenne_probe_side_unchanged() {
        let right = memory_exec("right_id");
        let join = Arc::new(join_with_right(right));

        let optimized = optimize(join);

        assert!(
            optimized.as_any().downcast_ref::<HashJoinExec>().is_some(),
            "Non-Cayenne joins should keep the default accumulator"
        );
    }

    #[test]
    fn leaves_null_equal_hash_join_unchanged() {
        let left = memory_exec("left_id");
        let right = Arc::new(CayenneAccelerationExec::new(memory_exec("right_id")));
        let join = Arc::new(hash_join_with_null_equality(
            left,
            right,
            "left_id",
            "right_id",
            NullEquality::NullEqualsNull,
        ));

        let optimized = optimize(join);

        assert!(
            optimized.as_any().downcast_ref::<HashJoinExec>().is_some(),
            "Null-equal joins should keep the default accumulator to preserve probe NULL matches"
        );
    }

    #[test]
    fn rewrites_hash_join_through_transparent_projection() {
        let right_input = Arc::new(CayenneAccelerationExec::new(memory_exec("right_id")));
        let right_schema = right_input.schema();
        let right = Arc::new(
            ProjectionExec::try_new(
                vec![(
                    col("right_id", &right_schema).expect("projection column should exist"),
                    "right_id".to_string(),
                )],
                right_input,
            )
            .expect("projection should be valid"),
        );
        let join = Arc::new(join_with_right(right));

        let optimized = optimize(join);

        assert!(
            optimized
                .as_any()
                .downcast_ref::<HashJoinExec<ExactLeftAccumulator>>()
                .is_some(),
            "Transparent wrappers over Cayenne scans should still use ExactLeftAccumulator"
        );
    }

    #[test]
    fn rewrites_nested_cayenne_probe_join_chain() {
        let nested_left = Arc::new(CayenneAccelerationExec::new(memory_exec("nested_left_id")));
        let nested_right = Arc::new(CayenneAccelerationExec::new(memory_exec("nested_right_id")));
        let nested_join = Arc::new(hash_join(
            nested_left,
            nested_right,
            "nested_left_id",
            "nested_right_id",
        ));
        let top_join = Arc::new(hash_join(
            memory_exec("top_id"),
            nested_join,
            "top_id",
            "nested_left_id",
        ));

        let optimized = optimize(top_join);
        let snapshot = plan_snapshot(&optimized);

        assert_eq!(
            2,
            snapshot.matches("accumulator=ExactLeftAccumulator").count(),
            "The top join and nested Cayenne probe join should both use ExactLeftAccumulator"
        );
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
    fn rewrites_same_source_left_anti_hash_join_to_sort_merge() {
        let schema = order_line_schema();
        let left = cayenne_file_exec(&schema, "order_line.vortex", None);
        let right = cayenne_file_exec(&schema, "order_line.vortex", None);
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
    fn rewrites_same_source_multi_key_left_anti_hash_join_to_sort_merge() {
        let schema = order_line_schema();
        let left = cayenne_file_exec(&schema, "order_line.vortex", None);
        let right = cayenne_file_exec(&schema, "order_line.vortex", None);
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
        let left = cayenne_file_exec(&schema, "order_line.vortex", None);
        let right = cayenne_file_exec(&schema, "other_order_line.vortex", None);
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
    fn snapshots_cayenne_probe_join_explain_plan() {
        let right = Arc::new(CayenneAccelerationExec::new(memory_exec("right_id")));
        let join = Arc::new(join_with_right(right));

        let optimized = optimize(join);

        insta::assert_snapshot!(
            "cayenne_probe_join_uses_exact_accumulator_explain",
            plan_snapshot(&optimized)
        );
    }

    #[test]
    fn snapshots_nested_cayenne_probe_join_explain_plan() {
        let nested_left = Arc::new(CayenneAccelerationExec::new(memory_exec("nested_left_id")));
        let nested_right = Arc::new(CayenneAccelerationExec::new(memory_exec("nested_right_id")));
        let nested_join = Arc::new(hash_join(
            nested_left,
            nested_right,
            "nested_left_id",
            "nested_right_id",
        ));
        let top_join = Arc::new(hash_join(
            memory_exec("top_id"),
            nested_join,
            "top_id",
            "nested_left_id",
        ));

        let optimized = optimize(top_join);

        insta::assert_snapshot!(
            "nested_cayenne_probe_join_uses_exact_accumulator_explain",
            plan_snapshot(&optimized)
        );
    }
}
