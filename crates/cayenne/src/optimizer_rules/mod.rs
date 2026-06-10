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
//! 3. **Same-source large semi/anti sort-merge rewrite.** `DataFusion` does
//!    not create dynamic filters for anti joins, and semi/anti joins with a
//!    large same-source LEFT input can leave a large non-spillable
//!    `HashJoinInput[N]` reservation behind.
//!    [`CayenneAntiJoinSortMergeRewriter`] rewrites only same-source Cayenne
//!    semi/anti `HashJoinExec` nodes to `SortMergeJoinExec` with explicit
//!    spillable `SortExec` inputs when the exact build-side row count exceeds
//!    `cayenne.sort_merge_min_rows` (default
//!    [`ANTI_JOIN_SORT_MERGE_MIN_EXACT_ROWS`], 10M). When the query memory-pool
//!    size is wired through `cayenne.sort_merge_memory_pool_bytes` and a
//!    build-side byte estimate is available, a memory gate of
//!    `sort_merge_memory_pool_fraction` (default 1/8) of the pool decides
//!    instead of the row gate. Ordinary inner/outer joins stay with
//!    `HashJoinExec` unless another optimizer rule supplies a more targeted
//!    win.
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
//! `datafusion.optimizer.hash_join_inlist_pushdown_max_size`, which the Spice
//! runtime session builder caps at `runtime.query.memory_limit` divided across
//! target partitions, never raising the `DataFusion` default) or a hash-table
//! lookup for larger ones. This natively supersedes the previous forked
//! `ExactLeftAccumulator` seam (exact `InList` with min/max + bloom fallback),
//! so no Cayenne-specific accumulator swap is required by default (the opt-in
//! [`CayenneJoinRewriter`] physical rule restores that seam when enabled). The
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

use crate::provider::CayenneAccelerationExec;
use crate::provider::delete::{Int64PkDeletionFilterExec, KeyBasedDeletionFilterExec};
use crate::provider::scan::{ScanDynamicFilter, ScanIdentity};

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

mod anti_join_sort_merge;
mod dynamic_filter_sharing;
mod join_rewriter;
mod utils;

pub use anti_join_sort_merge::CayenneAntiJoinSortMergeRewriter;
pub use dynamic_filter_sharing::CayenneDynamicFilterSharing;
pub use join_rewriter::CayenneJoinRewriter;

#[expect(unused_imports)]
use anti_join_sort_merge::{
    build_side_memory_estimate, fractional_bytes, has_single_same_source_pair_for_all_join_keys,
    join_key_ordering, sort_merge_memory_gate_bytes, try_rewrite_large_same_source_join,
};
#[expect(unused_imports)]
use dynamic_filter_sharing::{
    FilterAddition, apply_filter_additions, filter_additions_for_join, push_filter_addition,
};
#[expect(unused_imports)]
use join_rewriter::{
    exact_join_filter_build_key_bytes, exact_join_filter_probe_rows,
    hash_join_build_side_is_cayenne, is_cayenne_backed_join, should_rewrite_with_exact_accumulator,
    supports_exact_join_filter_fallback,
};
#[expect(unused_imports)]
use utils::{
    CayenneScanSummary, cayenne_optimizer_config, collect_cayenne_scans,
    collect_cayenne_scans_inner, estimated_arrow_width, flatten_transparent_nodes,
    physical_column_name, plan_schema_fields, same_source_pairs_for_column, scans_by_identity,
    spillable_rewrite_build_input_exact_rows,
};

#[cfg(test)]
mod tests;
