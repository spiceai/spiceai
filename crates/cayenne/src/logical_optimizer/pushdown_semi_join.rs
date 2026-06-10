//! [`CayennePushDownSemiJoin`]: a logical [`OptimizerRule`]
//! ([`ApplyOrder::TopDown`]) that takes a `LeftSemi`/`RightSemi` equi-join
//! with no residual filter and re-plants it directly above the Cayenne
//! `TableScan` sourcing its kept-side key columns, descending through inner
//! joins and key-preserving `Projection`/`Filter` wrappers, so the semi-join
//! prunes the base scan before the multi-way joins above it build their
//! non-spillable hash tables. Bails on expression keys, keys split across
//! join sides, non-Cayenne landing scans, and scans provably smaller than
//! [`MIN_SEMI_JOIN_PUSHDOWN_SCAN_ROWS`].

use super::{
    ApplyOrder, Arc, CayenneTableProvider, Column, DataFusionError, DefaultTableSource, Expr,
    Filter, Join, JoinConstraint, JoinType, LogicalPlan, NullEquality, OptimizerConfig,
    OptimizerRule, Projection, Result, TableProvider, TableProviderPredicate, TableSource,
    TableSourcePredicate, Transformed, TreeNode, TreeNodeRecursion, column_expr,
    table_scan_has_column, table_scan_upper_bound_rows,
};

/// Minimum upper-bound row count of the landing scan before a semi-join is
/// pushed down onto it. Below this the pushed-down semi-join can't recoup its
/// overhead, so the join is left where `DataFusion` placed it. Scans whose row
/// count is unknown are allowed through — we only skip when we can *prove* the
/// scan is small.
pub(super) const MIN_SEMI_JOIN_PUSHDOWN_SCAN_ROWS: usize = 100_000;

/// Logical optimizer rule that pushes a `LeftSemi`/`RightSemi` join down through
/// inner joins (and identity-preserving `Projection`/`Filter` wrappers) so it
/// prunes the base Cayenne table scan that sources its join key *before* the
/// expensive multi-way joins build their non-spillable hash tables.
///
/// TPC-H Q18 is the motivating shape: `o_orderkey IN (SELECT l_orderkey FROM
/// lineitem GROUP BY l_orderkey HAVING sum(l_quantity) > 300)` decorrelates into
/// a `LeftSemi` join at the very top of the plan, so the full
/// `customer ⋈ orders ⋈ lineitem` join is materialised and only *then* filtered
/// by the handful of qualifying orderkeys — leaving a multi-GB non-spillable
/// `HashJoinInput` build behind. Pushing the semi-join down to the `orders` scan
/// prunes orders to those orderkeys first, collapsing the downstream build from
/// billions of rows to thousands (and avoiding the OOM without paying the
/// sort-merge tax).
///
/// Soundness rests on the reordering law `(R ⋈ T) ⋉ₖ S ≡ (R ⋉ₖ S) ⋈ T`, valid
/// whenever every semi-join key column `k` is sourced solely from `R`. The rule
/// therefore only descends through:
///
///   * `Inner` joins with default SQL NULL equality, into the *single* side that
///     carries every key column (never an outer/anti join — its row-preservation
///     or null-padding could change which kept rows survive);
///   * identity-preserving `Projection`/`Filter` wrappers that still expose every
///     key column (a projection that recomputes a key drops the qualified column
///     from the child schema, which ends the descent — the key-transform guard).
///
/// It only *lands* on a Cayenne `TableScan` carrying every key column. If no
/// such scan is reachable the plan is left untouched, so the rule never makes a
/// non-pushable shape worse.
pub struct CayennePushDownSemiJoin {
    is_cayenne_table_source: TableSourcePredicate,
}

impl Default for CayennePushDownSemiJoin {
    fn default() -> Self {
        Self::new()
    }
}

impl CayennePushDownSemiJoin {
    /// Create a new instance of the rule.
    #[must_use]
    pub fn new() -> Self {
        Self::new_with_table_provider_predicate(|provider| {
            provider.as_any().is::<CayenneTableProvider>()
        })
    }

    /// Create a new instance with a caller-provided table-provider predicate.
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

    /// Recursively plant `LeftSemi(<cayenne scan>, filter_side)` as deep as
    /// possible inside `node`, rebuilding the structure above it. Returns `None`
    /// when no eligible Cayenne scan carrying every `kept_keys` column is
    /// reachable through inner joins / identity wrappers.
    fn push_to_cayenne_scan(
        &self,
        node: &LogicalPlan,
        filter_side: &Arc<LogicalPlan>,
        key_pairs: &[(Column, Column)],
        kept_keys: &[Column],
    ) -> Result<Option<LogicalPlan>, DataFusionError> {
        match node {
            // Landing: a Cayenne scan carrying every kept-side key column.
            LogicalPlan::TableScan(scan)
                if (self.is_cayenne_table_source)(scan.source.as_ref())
                    && kept_keys.iter().all(|key| table_scan_has_column(scan, key)) =>
            {
                if !scan_large_enough_for_semi_join_pushdown(scan) {
                    return Ok(None);
                }
                Ok(Some(build_landed_semi_join(
                    node.clone(),
                    filter_side,
                    key_pairs,
                )?))
            }
            // Inner join: descend into the single side that carries all keys.
            LogicalPlan::Join(inner)
                if inner.join_type == JoinType::Inner
                    && inner.null_equality == NullEquality::NullEqualsNothing =>
            {
                let left_has = kept_keys
                    .iter()
                    .all(|key| schema_has_column(inner.left.schema(), key));
                let right_has = kept_keys
                    .iter()
                    .all(|key| schema_has_column(inner.right.schema(), key));
                match (left_has, right_has) {
                    (true, false) => {
                        let Some(new_left) = self.push_to_cayenne_scan(
                            &inner.left,
                            filter_side,
                            key_pairs,
                            kept_keys,
                        )?
                        else {
                            return Ok(None);
                        };
                        Ok(Some(rebuild_inner_join(
                            inner,
                            Arc::new(new_left),
                            Arc::clone(&inner.right),
                        )?))
                    }
                    (false, true) => {
                        let Some(new_right) = self.push_to_cayenne_scan(
                            &inner.right,
                            filter_side,
                            key_pairs,
                            kept_keys,
                        )?
                        else {
                            return Ok(None);
                        };
                        Ok(Some(rebuild_inner_join(
                            inner,
                            Arc::clone(&inner.left),
                            Arc::new(new_right),
                        )?))
                    }
                    // Keys split across both sides (or neither side carries them
                    // all): there is no single side to push into.
                    _ => Ok(None),
                }
            }
            // Identity wrapper: descend if the child still exposes every key.
            LogicalPlan::Projection(projection)
                if kept_keys
                    .iter()
                    .all(|key| schema_has_column(projection.input.schema(), key)) =>
            {
                let Some(new_input) = self.push_to_cayenne_scan(
                    &projection.input,
                    filter_side,
                    key_pairs,
                    kept_keys,
                )?
                else {
                    return Ok(None);
                };
                Ok(Some(LogicalPlan::Projection(Projection::try_new(
                    projection.expr.clone(),
                    Arc::new(new_input),
                )?)))
            }
            LogicalPlan::Filter(filter)
                if kept_keys
                    .iter()
                    .all(|key| schema_has_column(filter.input.schema(), key)) =>
            {
                let Some(new_input) =
                    self.push_to_cayenne_scan(&filter.input, filter_side, key_pairs, kept_keys)?
                else {
                    return Ok(None);
                };
                Ok(Some(LogicalPlan::Filter(Filter::try_new(
                    filter.predicate.clone(),
                    Arc::new(new_input),
                )?)))
            }
            _ => Ok(None),
        }
    }
}

impl std::fmt::Debug for CayennePushDownSemiJoin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CayennePushDownSemiJoin").finish()
    }
}

impl OptimizerRule for CayennePushDownSemiJoin {
    fn name(&self) -> &'static str {
        "cayenne_push_down_semi_join"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        // TopDown: handle the outermost semi-join first, planting it deep. The
        // resulting scan-anchored `LeftSemi` is rejected on re-entry (its kept
        // side is a bare `TableScan`), so the rewrite is idempotent.
        Some(ApplyOrder::TopDown)
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>, DataFusionError> {
        let LogicalPlan::Join(join) = plan else {
            return Ok(Transformed::no(plan));
        };
        // Only plain `LeftSemi`/`RightSemi` equi-joins with default NULL equality
        // and no residual non-equi filter — anything else changes the kept-row
        // set in ways the reordering law does not cover.
        if !matches!(join.join_type, JoinType::LeftSemi | JoinType::RightSemi)
            || join.null_equality != NullEquality::NullEqualsNothing
            || join.filter.is_some()
            || join.on.is_empty()
        {
            return Ok(Transformed::no(LogicalPlan::Join(join)));
        }

        let kept_on_left = matches!(join.join_type, JoinType::LeftSemi);
        let (kept_side, filter_side) = if kept_on_left {
            (&join.left, &join.right)
        } else {
            (&join.right, &join.left)
        };

        // Nothing to do unless the kept side has structure to descend through —
        // a bare scan is already where the semi-join belongs (this is also what
        // makes the rewrite idempotent).
        if !matches!(
            kept_side.as_ref(),
            LogicalPlan::Join(_) | LogicalPlan::Projection(_) | LogicalPlan::Filter(_)
        ) {
            return Ok(Transformed::no(LogicalPlan::Join(join)));
        }

        // Column-vs-column equi-keys only, oriented `(kept_column, filter_column)`.
        // Bail on any expression key — we cannot trace its provenance to a scan.
        let mut key_pairs: Vec<(Column, Column)> = Vec::with_capacity(join.on.len());
        for (left_expr, right_expr) in &join.on {
            let (kept_expr, filter_expr) = if kept_on_left {
                (left_expr, right_expr)
            } else {
                (right_expr, left_expr)
            };
            let (Expr::Column(kept_column), Expr::Column(filter_column)) = (kept_expr, filter_expr)
            else {
                return Ok(Transformed::no(LogicalPlan::Join(join)));
            };
            key_pairs.push((kept_column.clone(), filter_column.clone()));
        }
        let kept_keys: Vec<Column> = key_pairs.iter().map(|(kept, _)| kept.clone()).collect();

        match self.push_to_cayenne_scan(kept_side, filter_side, &key_pairs, &kept_keys)? {
            Some(rewritten) => Ok(Transformed::yes(rewritten)),
            None => Ok(Transformed::no(LogicalPlan::Join(join))),
        }
    }
}

/// Build the scan-anchored `LeftSemi` join planted by [`CayennePushDownSemiJoin`].
/// The kept (scan) side is always on the left, so the join keeps scan rows that
/// match the filter side — the same membership the original outer semi-join
/// computed, just evaluated before the expensive joins.
pub(super) fn build_landed_semi_join(
    scan: LogicalPlan,
    filter_side: &Arc<LogicalPlan>,
    key_pairs: &[(Column, Column)],
) -> Result<LogicalPlan, DataFusionError> {
    let on = key_pairs
        .iter()
        .map(|(kept, filter)| (column_expr(kept), column_expr(filter)))
        .collect();
    Ok(LogicalPlan::Join(Join::try_new(
        Arc::new(scan),
        Arc::clone(filter_side),
        on,
        None,
        JoinType::LeftSemi,
        JoinConstraint::On,
        NullEquality::NullEqualsNothing,
        false,
    )?))
}

pub(super) fn rebuild_inner_join(
    inner: &Join,
    new_left: Arc<LogicalPlan>,
    new_right: Arc<LogicalPlan>,
) -> Result<LogicalPlan, DataFusionError> {
    Ok(LogicalPlan::Join(Join::try_new(
        new_left,
        new_right,
        inner.on.clone(),
        inner.filter.clone(),
        inner.join_type,
        inner.join_constraint,
        inner.null_equality,
        inner.null_aware,
    )?))
}

pub(super) fn schema_has_column(schema: &datafusion::common::DFSchemaRef, column: &Column) -> bool {
    schema.has_column(column)
        || schema
            .qualified_field_with_unqualified_name(&column.name)
            .is_ok()
}

pub(super) fn scan_large_enough_for_semi_join_pushdown(
    scan: &datafusion::logical_expr::TableScan,
) -> bool {
    match table_scan_upper_bound_rows(scan) {
        Some(rows) => rows >= MIN_SEMI_JOIN_PUSHDOWN_SCAN_ROWS,
        None => true,
    }
}

pub(super) fn is_single_cayenne_table_scan_input(
    plan: &LogicalPlan,
    is_cayenne_table_source: &TableSourcePredicate,
) -> bool {
    let mut total_scans = 0_usize;
    let mut cayenne_scans = 0_usize;
    let _ = plan.apply(|node| {
        if let LogicalPlan::TableScan(scan) = node {
            total_scans = total_scans.saturating_add(1);
            if is_cayenne_table_source(scan.source.as_ref()) {
                cayenne_scans = cayenne_scans.saturating_add(1);
            }
            if total_scans > 1 {
                return Ok(TreeNodeRecursion::Stop);
            }
        }

        Ok(TreeNodeRecursion::Continue)
    });
    total_scans == 1 && cayenne_scans == 1
}
