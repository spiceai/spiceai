//! [`CayenneJoinRewriter`]: an opt-in [`PhysicalOptimizerRule`] that swaps the
//! accumulator of inner, Cayenne-probe-backed `HashJoinExec` nodes to the
//! forked `ExactLeftAccumulator`, restoring the exact in-list dynamic-filter
//! seam that `DataFusion` 53's native hash-join pushdown otherwise supersedes.
//! Registered only when the `exact_join_filter` token is named in the runtime
//! `cayenne_optimizer_rules` param. Bails without exact build-side row
//! statistics or known probe-side statistics, below the probe-size gates
//! (`exact_join_filter_min_probe_rows`, default 100k rows;
//! `exact_join_filter_min_probe_to_build_ratio`, default 10x), and when the
//! estimated build-key bytes exceed `exact_join_filter_max_bytes`.

use super::{
    Arc, CayenneAccelerationExec, ConfigOptions, DataFusionError, DataType, ExactLeftAccumulator,
    ExecutionPlan, HashJoinExec, JoinType, NullEquality, PhysicalExpr, PhysicalOptimizerRule,
    Precision, Result, Transformed, TransformedResult, TreeNode, cayenne_optimizer_config,
    estimated_arrow_width, flatten_transparent_nodes, spillable_rewrite_build_input_exact_rows,
};

/// Optimizer rule that rewrites `HashJoinExec` nodes to use `ExactLeftAccumulator`
/// when the probe side (behind transparent wrappers) is a
/// `CayenneAccelerationExec`, or a nested `HashJoinExec` whose own build side
/// is Cayenne-backed.
///
/// Opt-in: this rule is only registered when the `exact_join_filter` token is
/// set in the runtime `cayenne_optimizer_rules` param. By default the
/// ordinary inner-join probe filter is handled by `DataFusion` 53's native
/// hash-join dynamic-filter pushdown (whose `InList` budget is capped in the
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

pub(super) fn exact_join_filter_build_key_bytes(
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

pub(super) fn exact_join_filter_probe_rows(hash_join: &HashJoinExec) -> Option<usize> {
    match hash_join.right().partition_statistics(None).ok()?.num_rows {
        Precision::Exact(row_count) | Precision::Inexact(row_count) => Some(row_count),
        Precision::Absent => None,
    }
}

pub(super) fn supports_exact_join_filter_fallback(data_type: &DataType) -> bool {
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

pub(super) fn should_rewrite_with_exact_accumulator(
    hash_join: &HashJoinExec,
    config: &ConfigOptions,
) -> bool {
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

impl std::fmt::Debug for CayenneJoinRewriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CayenneJoinRewriter").finish()
    }
}

pub(super) fn hash_join_build_side_is_cayenne(join: &HashJoinExec) -> bool {
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
pub(super) fn is_cayenne_backed_join(hash_join: &HashJoinExec) -> bool {
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
