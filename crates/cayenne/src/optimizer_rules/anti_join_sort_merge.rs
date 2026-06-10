//! [`CayenneAntiJoinSortMergeRewriter`]: a [`PhysicalOptimizerRule`] that
//! rewrites same-source Cayenne `LeftSemi`/`RightSemi`/`LeftAnti`/`RightAnti`
//! `HashJoinExec` nodes into `SortExec` + `SortMergeJoinExec` so the build
//! side becomes spillable. Fires only when every join key maps to a single
//! same-source Cayenne scan pair and the LEFT (build) input has an exact row
//! count above `cayenne.sort_merge_min_rows` (default
//! [`ANTI_JOIN_SORT_MERGE_MIN_EXACT_ROWS`], 10M); when the memory gate is
//! configured and a build-side byte estimate exists, that byte gate
//! ([`ANTI_JOIN_SORT_MERGE_MEMORY_POOL_FRACTION`] of the pool) decides instead
//! of the row gate. Joins with projections, expression keys, or non-default
//! NULL equality are left alone.

use super::{
    Arc, CayenneOptimizerConfig, ConfigOptions, DataFusionError, ExecutionPlan, HashJoinExec,
    JoinType, LexOrdering, NullEquality, PhysicalExpr, PhysicalOptimizerRule, PhysicalSortExpr,
    Result, SortExec, SortMergeJoinExec, SortOptions, Transformed, TransformedResult, TreeNode,
    cayenne_optimizer_config, collect_cayenne_scans, estimated_arrow_width, physical_column_name,
    same_source_pairs_for_column, scans_by_identity, spillable_rewrite_build_input_exact_rows,
};

/// Rewrites same-source large Cayenne semi/anti joins from hash join to
/// sort-merge join when the build side is large enough to risk OOM.
///
/// `DataFusion`'s `HashJoinExec` always materializes its left input as the
/// non-spillable build side regardless of join type. For wide semi/anti-join
/// decorrelations, that build side can be a large multi-way result. Sort-merge
/// preserves those semi/anti semantics while keeping the build side spillable;
/// ordinary inner/outer joins are left alone because their hash join can still
/// be the faster plan.
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
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        plan.transform_down(|node| {
            let Some(hash_join) = node.as_any().downcast_ref::<HashJoinExec>() else {
                return Ok(Transformed::no(node));
            };

            let Some(sort_merge_join) = try_rewrite_large_same_source_join(hash_join, config)?
            else {
                return Ok(Transformed::no(node));
            };

            Ok(Transformed::yes(sort_merge_join))
        })
        .data()
    }
}

pub(super) fn try_rewrite_large_same_source_join(
    hash_join: &HashJoinExec,
    config: &ConfigOptions,
) -> Result<Option<Arc<dyn ExecutionPlan>>, DataFusionError> {
    // Semi/anti joins are the clear-win target: `HashJoinExec` builds the LEFT
    // input into a non-spillable hash table, while these joins do not have the
    // same dynamic-filter fallback as ordinary inner joins.
    if !matches!(
        hash_join.join_type(),
        JoinType::LeftAnti | JoinType::RightAnti | JoinType::LeftSemi | JoinType::RightSemi,
    ) {
        return Ok(None);
    }

    if hash_join.null_equality() != NullEquality::NullEqualsNothing {
        return Ok(None);
    }

    if hash_join.contains_projection() || hash_join.on().is_empty() {
        return Ok(None);
    }

    if !has_single_same_source_pair_for_all_join_keys(hash_join) {
        return Ok(None);
    }

    let Some(build_row_count) = spillable_rewrite_build_input_exact_rows(hash_join) else {
        return Ok(None);
    };
    let optimizer_config = cayenne_optimizer_config(config);
    let row_count_threshold = optimizer_config.sort_merge_min_rows;
    let row_gate_passes = build_row_count > row_count_threshold;
    let memory_gate_bytes = sort_merge_memory_gate_bytes(&optimizer_config);

    // When a memory gate is configured, it's the *primary* signal — the row gate
    // becomes irrelevant unless the byte estimate is unavailable. This lets the
    // rule catch wide-row builds whose row count is well below the row
    // threshold but whose materialised hash
    // table would still exhaust the memory pool. When the gate is *inactive*
    // (no memory pool wired through config — direct DataFusion users), fall back
    // to the row-count threshold alone.
    let estimated_build_bytes = match memory_gate_bytes {
        Some(_) => build_side_memory_estimate(hash_join.left().as_ref(), build_row_count),
        None => None,
    };
    let should_rewrite = match (memory_gate_bytes, estimated_build_bytes) {
        // Memory gate active + byte estimate available — byte gate alone decides.
        (Some(gate_bytes), Some(bytes)) => bytes > gate_bytes,
        // Memory gate active but no byte estimate, or no gate configured — fall back to row gate.
        (Some(_), None) | (None, _) => row_gate_passes,
    };

    if !should_rewrite {
        tracing::debug!(
            join_type = ?hash_join.join_type(),
            build_row_count,
            row_count_threshold,
            estimated_build_bytes,
            memory_gate_bytes,
            "Keeping same-source Cayenne HashJoinExec because neither row nor byte gate fires"
        );
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
        build_row_count,
        row_count_threshold,
        estimated_build_bytes,
        memory_gate_bytes,
        "Replacing large same-source Cayenne HashJoinExec with SortMergeJoinExec"
    );

    Ok(Some(Arc::new(join)))
}

pub(super) fn sort_merge_memory_gate_bytes(config: &CayenneOptimizerConfig) -> Option<usize> {
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
pub(super) fn fractional_bytes(bytes: usize, fraction: f64) -> usize {
    let scaled = bytes as f64 * fraction;
    if !scaled.is_finite() || scaled >= usize::MAX as f64 {
        usize::MAX
    } else if scaled <= 0.0 {
        0
    } else {
        scaled as usize
    }
}

pub(super) fn build_side_memory_estimate(
    plan: &dyn ExecutionPlan,
    build_rows: usize,
) -> Option<usize> {
    let row_width = plan
        .schema()
        .fields()
        .iter()
        .try_fold(0_usize, |acc, field| {
            Some(acc.saturating_add(estimated_arrow_width(field.data_type())?))
        })?;

    Some(row_width.saturating_mul(build_rows))
}

pub(super) fn join_key_ordering(
    keys: impl Iterator<Item = Arc<dyn PhysicalExpr>>,
    sort_options: &[SortOptions],
) -> Option<LexOrdering> {
    let sort_exprs = keys
        .zip(sort_options.iter().copied())
        .map(|(expr, options)| PhysicalSortExpr { expr, options })
        .collect::<Vec<_>>();

    LexOrdering::new(sort_exprs)
}

pub(super) fn has_single_same_source_pair_for_all_join_keys(hash_join: &HashJoinExec) -> bool {
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
