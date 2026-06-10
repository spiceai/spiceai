//! [`CayennePropagateFilterAcrossEquiJoinKeys`]: a logical [`OptimizerRule`]
//! (applied [`ApplyOrder::TopDown`]) that, for `Inner`/`LeftSemi`/`RightSemi`
//! joins with column-vs-column equi-keys, wraps the Cayenne-backed side
//! opposite a selectively filtered dim-like side with
//! `Filter(key IN (SELECT key FROM dim_subtree))`, exposing the dim filter's
//! key-domain bound to the fact scan before join ordering.
//!
//! Bails unless the providing side is dim-like (at most
//! [`MAX_DIM_LIKE_TABLE_SCANS`] scans), the key survives any
//! `Aggregate`/`Distinct`, and statistics prove the receiving side has at
//! least [`MIN_FACT_ROWS_FOR_PROPAGATION`] rows and is at least
//! [`MIN_FACT_TO_DIM_KEY_DOMAIN_RATIO`]x the dim key domain (missing stats
//! means no-op). See the parent module docs for the full design.

use super::{
    ApplyOrder, Arc, BTreeSet, CayenneTableProvider, Column, DataFusionError, DefaultTableSource,
    EquiKey, Expr, Join, JoinType, LogicalPlan, NullEquality, OptimizerConfig, OptimizerRule,
    Result, TableProvider, TableProviderPredicate, TableSource, TableSourcePredicate, Transformed,
    analyze_logical_side, build_key_projection_subquery, column_expr, columns_match,
    contains_cayenne_table_scan_with_column, key_preserved_through_summaries,
    matching_equijoin_keys, right_side_carries_propagation_marker, skip_propagation_by_cardinality,
    wrap_with_in_subquery_filter_expr,
};

/// Logical optimizer rule that, for each `Inner`, `LeftSemi`, or `RightSemi`
/// join with default SQL NULL equality and a simple column equi-key
/// `(left.a = right.b)`, introduces
/// `Filter(other_side.key IN (SELECT this_side.key FROM this_side_subtree))`
/// on the Cayenne-backed side opposite a non-key filter.
///
/// See the module-level docs for the full design and selective join-domain motivation.
pub struct CayennePropagateFilterAcrossEquiJoinKeys {
    is_cayenne_table_source: TableSourcePredicate,
}

impl Default for CayennePropagateFilterAcrossEquiJoinKeys {
    fn default() -> Self {
        Self::new()
    }
}

impl CayennePropagateFilterAcrossEquiJoinKeys {
    /// Create a new instance of the rule.
    #[must_use]
    pub fn new() -> Self {
        Self::new_with_table_provider_predicate(|provider| {
            provider.as_any().is::<CayenneTableProvider>()
        })
    }

    /// Create a new instance with a caller-provided table-provider predicate.
    ///
    /// Runtime registration uses this to recognize `AcceleratedTable`s whose
    /// inner accelerator is Cayenne, while this crate's default stays scoped to
    /// direct [`CayenneTableProvider`] scans.
    #[must_use]
    pub fn new_with_table_provider_predicate(
        is_cayenne_table_provider: impl Fn(&dyn TableProvider) -> bool + Send + Sync + 'static,
    ) -> Self {
        let is_cayenne_table_provider: TableProviderPredicate = Arc::new(is_cayenne_table_provider);
        Self::new_with_table_source_predicate(move |source| {
            source
                .as_any()
                .downcast_ref::<DefaultTableSource>()
                .is_some_and(|source| is_cayenne_table_provider(source.table_provider.as_ref()))
        })
    }

    /// Create a new instance with a caller-provided table-source predicate.
    #[must_use]
    pub fn new_with_table_source_predicate(
        is_cayenne_table_source: impl Fn(&dyn TableSource) -> bool + Send + Sync + 'static,
    ) -> Self {
        Self {
            is_cayenne_table_source: Arc::new(is_cayenne_table_source),
        }
    }
}

impl std::fmt::Debug for CayennePropagateFilterAcrossEquiJoinKeys {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CayennePropagateFilterAcrossEquiJoinKeys")
            .finish()
    }
}

impl OptimizerRule for CayennePropagateFilterAcrossEquiJoinKeys {
    fn name(&self) -> &'static str {
        "cayenne_propagate_filter_across_equi_join_keys"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        // TopDown: process higher joins first so propagation seeds reach
        // nested joins on the next pass.
        Some(ApplyOrder::TopDown)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>, DataFusionError> {
        let join = match plan {
            LogicalPlan::Join(j) => j,
            other => return Ok(Transformed::no(other)),
        };
        if !matches!(
            join.join_type,
            JoinType::Inner | JoinType::LeftSemi | JoinType::RightSemi,
        ) {
            return Ok(Transformed::no(LogicalPlan::Join(join)));
        }
        if join.null_equality != NullEquality::NullEqualsNothing {
            return Ok(Transformed::no(LogicalPlan::Join(join)));
        }
        if matches!(join.join_type, JoinType::LeftSemi)
            && right_side_carries_propagation_marker(&join.right)
        {
            return Ok(Transformed::no(LogicalPlan::Join(join)));
        }
        let equijoin_keys = matching_equijoin_keys(&join);
        if equijoin_keys.is_empty() {
            return Ok(Transformed::no(LogicalPlan::Join(join)));
        }

        let mut left_analysis = analyze_logical_side(&join.left);
        let mut right_analysis = analyze_logical_side(&join.right);
        let mut new_left: Arc<LogicalPlan> = Arc::clone(&join.left);
        let mut new_right: Arc<LogicalPlan> = Arc::clone(&join.right);
        let mut changed = false;

        for EquiKey { left, right } in &equijoin_keys {
            // Propagate the LEFT-side filtered key domain → the RIGHT side.
            if contains_cayenne_table_scan_with_column(
                &join.right,
                right,
                &self.is_cayenne_table_source,
            ) && left_analysis.is_dim_like
                && left_analysis.has_selective_non_key_filter(left)
                && key_preserved_through_summaries(&join.left, left)
                && !skip_propagation_by_cardinality(&join.left, &join.right, left)
                && !right_analysis.has_propagated_filter_target(&column_expr(right))
            {
                let subquery_plan = build_key_projection_subquery(
                    Arc::clone(&join.left),
                    left,
                    config.alias_generator(),
                )?;
                let target = column_expr(right);
                let wrapped = wrap_with_in_subquery_filter_expr(
                    Arc::clone(&new_right),
                    &target,
                    subquery_plan,
                )?;
                new_right = Arc::new(wrapped);
                right_analysis.add_propagated_filter_target(&target);
                changed = true;
            }

            // Propagate the RIGHT-side filtered key domain → the LEFT side.
            if contains_cayenne_table_scan_with_column(
                &join.left,
                left,
                &self.is_cayenne_table_source,
            ) && right_analysis.is_dim_like
                && right_analysis.has_selective_non_key_filter(right)
                && key_preserved_through_summaries(&join.right, right)
                && !skip_propagation_by_cardinality(&join.right, &join.left, right)
                && !left_analysis.has_propagated_filter_target(&column_expr(left))
            {
                let subquery_plan = build_key_projection_subquery(
                    Arc::clone(&join.right),
                    right,
                    config.alias_generator(),
                )?;
                let target = column_expr(left);
                let wrapped = wrap_with_in_subquery_filter_expr(
                    Arc::clone(&new_left),
                    &target,
                    subquery_plan,
                )?;
                new_left = Arc::new(wrapped);
                left_analysis.add_propagated_filter_target(&target);
                changed = true;
            }
        }

        if !changed {
            return Ok(Transformed::no(LogicalPlan::Join(join)));
        }

        let new_join = Join::try_new(
            new_left,
            new_right,
            join.on,
            join.filter,
            join.join_type,
            join.join_constraint,
            join.null_equality,
            join.null_aware,
        )?;

        Ok(Transformed::yes(LogicalPlan::Join(new_join)))
    }
}

#[derive(Default)]
pub(super) struct SideAnalysis {
    pub(super) is_dim_like: bool,
    pub(super) selective_filter_columns: Vec<Column>,
    /// Targets of already-propagated `InSubquery` filters on this side, keyed
    /// by the `Display` form of the target expression. Used for cycle
    /// prevention — the same target should not be wrapped twice.
    pub(super) propagated_filter_targets: BTreeSet<String>,
}

impl SideAnalysis {
    fn has_selective_non_key_filter(&self, key: &Column) -> bool {
        self.selective_filter_columns
            .iter()
            .any(|column| !columns_match(column, key))
    }

    fn has_propagated_filter_target(&self, target: &Expr) -> bool {
        self.propagated_filter_targets.contains(&target.to_string())
    }

    fn add_propagated_filter_target(&mut self, target: &Expr) {
        self.propagated_filter_targets.insert(target.to_string());
    }
}
