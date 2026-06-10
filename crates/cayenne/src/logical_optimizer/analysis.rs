//! Shared plan-shape analysis helpers for the logical rules — primarily
//! [`CayennePropagateFilterAcrossEquiJoinKeys`]: dim-like subtree detection
//! ([`MAX_DIM_LIKE_TABLE_SCANS`]), key-domain preservation through
//! `Aggregate`/`Distinct`, statistics-based cardinality gates
//! ([`MIN_FACT_ROWS_FOR_PROPAGATION`], [`MIN_FACT_TO_DIM_KEY_DOMAIN_RATIO`]),
//! propagated-filter marker detection for cycle prevention, and construction
//! of the `IN (SELECT key ...)` subquery itself. No optimizer rule lives in
//! this file; these helpers are consumed by the sibling rule modules.

use super::{
    Arc, BTreeSet, Column, DefaultTableSource, Expr, ExprSchemable, Filter, InSubquery, Join,
    JoinType, LogicalPlan, NullEquality, Operator, PROPAGATED_FILTER_ALIAS_PREFIX, Projection,
    Result, SideAnalysis, Spans, Subquery, SubqueryAlias, TableReference, TableSourcePredicate,
    TreeNode, TreeNodeRecursion,
};

pub(super) fn contains_cayenne_table_scan(
    plan: &LogicalPlan,
    is_cayenne_table_source: &TableSourcePredicate,
) -> bool {
    let mut found = false;
    let _ = plan.apply(|node| {
        if let LogicalPlan::TableScan(scan) = node
            && is_cayenne_table_source(scan.source.as_ref())
        {
            found = true;
            return Ok(TreeNodeRecursion::Stop);
        }

        Ok(TreeNodeRecursion::Continue)
    });
    found
}

pub(super) fn column_expr(column: &Column) -> Expr {
    Expr::Column(column.clone())
}

pub(super) fn analyze_logical_side(plan: &LogicalPlan) -> SideAnalysis {
    let mut analysis = SideAnalysis {
        is_dim_like: is_dim_like_subtree(plan),
        ..SideAnalysis::default()
    };

    let _ = plan.apply(|node| {
        if let LogicalPlan::Filter(filter) = node {
            collect_selective_filter_columns(
                &filter.predicate,
                &mut analysis.selective_filter_columns,
            );
            collect_propagated_filter_targets(
                &filter.predicate,
                &mut analysis.propagated_filter_targets,
            );
        }
        // Post-decorrelation cycle detection: `decorrelate_predicate_subquery`
        // rewrites our propagated `InSubquery` into a `LeftSemi` join with the
        // marker `SubqueryAlias` as its right child. Without this branch the
        // rule's cycle guard misses the marker (it only walked Filter
        // predicates), and the optimizer would re-propagate on every iteration
        // until hitting `max_passes`, stacking redundant `LeftSemi` layers.
        if let LogicalPlan::Join(join) = node
            && matches!(join.join_type, JoinType::LeftSemi)
            && right_side_carries_propagation_marker(&join.right)
        {
            for (left_expr, _) in &join.on {
                analysis
                    .propagated_filter_targets
                    .insert(left_expr.to_string());
            }
        }

        Ok(TreeNodeRecursion::Continue)
    });

    analysis
}

pub(super) fn contains_cayenne_table_scan_with_column(
    plan: &LogicalPlan,
    target_column: &Column,
    is_cayenne_table_source: &TableSourcePredicate,
) -> bool {
    let mut found = false;
    let _ = plan.apply(|node| {
        if let LogicalPlan::TableScan(scan) = node
            && is_cayenne_table_source(scan.source.as_ref())
            && table_scan_has_column(scan, target_column)
        {
            found = true;
            return Ok(TreeNodeRecursion::Stop);
        }

        Ok(TreeNodeRecursion::Continue)
    });
    found
}

pub(super) fn table_scan_has_column(
    scan: &datafusion::logical_expr::TableScan,
    column: &Column,
) -> bool {
    scan.projected_schema.has_column(column)
        || scan
            .projected_schema
            .qualified_field_with_unqualified_name(&column.name)
            .is_ok()
}

/// Returns `true` if `plan` is — possibly behind a chain of `Projection` or
/// `SubqueryAlias` wrappers added by later optimizer rules — a `SubqueryAlias`
/// whose name starts with [`PROPAGATED_FILTER_ALIAS_PREFIX`].
pub(super) fn right_side_carries_propagation_marker(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::SubqueryAlias(alias) => {
            if alias
                .alias
                .table()
                .starts_with(PROPAGATED_FILTER_ALIAS_PREFIX)
            {
                return true;
            }
            right_side_carries_propagation_marker(&alias.input)
        }
        LogicalPlan::Projection(p) => right_side_carries_propagation_marker(&p.input),
        _ => false,
    }
}

/// A column-vs-column equi-join key from `Join::on`.
pub(super) struct EquiKey {
    pub(super) left: Column,
    pub(super) right: Column,
}

/// Return the column-vs-column equi-join keys from `join.on` whose data types
/// match. Drops expression keys and pairs whose types differ (the `IN` subquery
/// would need an implicit cast we don't insert here).
pub(super) fn matching_equijoin_keys(join: &Join) -> Vec<EquiKey> {
    join.on
        .iter()
        .filter_map(|(left, right)| {
            if !join_key_types_match(left, right, &join.left, &join.right) {
                return None;
            }

            match (left, right) {
                (Expr::Column(l), Expr::Column(r)) => Some(EquiKey {
                    left: l.clone(),
                    right: r.clone(),
                }),
                _ => None,
            }
        })
        .collect()
}

pub(super) fn join_key_types_match(
    left: &Expr,
    right: &Expr,
    left_plan: &LogicalPlan,
    right_plan: &LogicalPlan,
) -> bool {
    let Ok(left_type) = left.get_type(left_plan.schema()) else {
        return false;
    };
    let Ok(right_type) = right.get_type(right_plan.schema()) else {
        return false;
    };

    left_type == right_type
}

/// Maximum number of `TableScan` leaves allowed inside a dim-like subtree.
///
/// Chosen to cover common dimension snowflakes
/// (`region ⋈ nation ⋈ supplier`, three leaves) without admitting arbitrarily
/// large dim joins whose re-execution under an `InSubquery` would be expensive.
pub(super) const MAX_DIM_LIKE_TABLE_SCANS: usize = 3;

/// Skip propagation when the receiving fact subtree's known upper-bound row
/// count is below this threshold. Below it there isn't enough probe
/// cardinality for the filter to recoup the propagation overhead.
pub(super) const MIN_FACT_ROWS_FOR_PROPAGATION: usize = 100_000;

/// Skip propagation unless the receiving fact subtree is at least this many
/// times larger than the dim side's propagated join-key domain.
pub(super) const MIN_FACT_TO_DIM_KEY_DOMAIN_RATIO: usize = 10;

/// Returns `true` if `plan` is a "dim-like" subtree — a small snowflake of
/// dimensions composed of at most [`MAX_DIM_LIKE_TABLE_SCANS`] `TableScan`s
/// connected through identity-preserving operators (`Projection`,
/// `SubqueryAlias`, `Filter`, `Limit`), inner equi-joins with default SQL
/// NULL equality, `Aggregate`, or `Distinct`.
///
/// The conservatism here keeps the duplicated subquery cheap: `DataFusion` will
/// execute it independently of the outer join, so we only fire on subtrees
/// where re-running the scan(s) + filter(s) is cheap. Unions, windows, sorts,
/// and any non-inner / null-equal join terminate the walk.
///
/// `Aggregate` and `Distinct` are *structurally* allowed here, but the rule's
/// caller must additionally verify the join key is preserved through any
/// aggregations via [`key_preserved_through_summaries`] — an aggregate that
/// does not group by the key does not preserve its domain and cannot be the
/// source of a propagated subquery on that key.
pub(super) fn is_dim_like_subtree(plan: &LogicalPlan) -> bool {
    count_dim_like_table_scans(plan).is_some_and(|n| n <= MAX_DIM_LIKE_TABLE_SCANS)
}

pub(super) fn count_dim_like_table_scans(plan: &LogicalPlan) -> Option<usize> {
    match plan {
        LogicalPlan::TableScan(_) => Some(1),
        LogicalPlan::Projection(p) => count_dim_like_table_scans(&p.input),
        LogicalPlan::SubqueryAlias(a) => count_dim_like_table_scans(&a.input),
        LogicalPlan::Filter(f) => count_dim_like_table_scans(&f.input),
        LogicalPlan::Limit(l) => count_dim_like_table_scans(&l.input),
        LogicalPlan::Aggregate(a) => count_dim_like_table_scans(&a.input),
        LogicalPlan::Distinct(d) => count_dim_like_table_scans(distinct_input(d)),
        LogicalPlan::Join(j)
            if j.join_type == JoinType::Inner
                && j.null_equality == NullEquality::NullEqualsNothing =>
        {
            let l = count_dim_like_table_scans(&j.left)?;
            let r = count_dim_like_table_scans(&j.right)?;
            Some(l + r)
        }
        _ => None,
    }
}

/// Returns the single input plan of a `Distinct` regardless of variant.
pub(super) fn distinct_input(distinct: &datafusion::logical_expr::Distinct) -> &LogicalPlan {
    use datafusion::logical_expr::Distinct;
    match distinct {
        Distinct::All(input) => input,
        Distinct::On(on) => &on.input,
    }
}

/// Sum of known upper-bound row counts of all `TableScan`s reachable from
/// `plan`. Returns `None` if any reachable `TableScan` is missing stats — the
/// caller falls back to the structural gates in that case.
///
/// The walk follows every `LogicalPlan` child (not just dim-like wrappers) so
/// fact-side subtrees with joins, aggregates, etc. are summed too. The result
/// is a loose *upper bound* — filter selectivity isn't accounted for — which
/// is the right direction for the "skip if known small" gate (a true upper
/// bound below the threshold guarantees the subtree is actually small).
pub(super) fn subtree_upper_bound_rows(plan: &LogicalPlan) -> Option<usize> {
    let mut total: usize = 0;
    let mut any_unknown = false;
    let _ = plan.apply(|node| {
        if let LogicalPlan::TableScan(scan) = node {
            if let Some(n) = table_scan_upper_bound_rows(scan) {
                total = total.saturating_add(n);
            } else {
                any_unknown = true;
                return Ok(TreeNodeRecursion::Stop);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    });
    if any_unknown { None } else { Some(total) }
}

pub(super) fn table_scan_upper_bound_rows(
    scan: &datafusion::logical_expr::TableScan,
) -> Option<usize> {
    use datafusion::common::stats::Precision;

    scan.source
        .as_any()
        .downcast_ref::<DefaultTableSource>()
        .and_then(|default| default.table_provider.statistics())
        .and_then(|stats| match stats.num_rows {
            Precision::Exact(n) | Precision::Inexact(n) => Some(n),
            Precision::Absent => None,
        })
}

pub(super) fn key_for_input_schema(input: &LogicalPlan, key: &Column) -> Option<Column> {
    input
        .schema()
        .qualified_field_with_unqualified_name(&key.name)
        .ok()
        .map(|(qualifier, field)| Column::new(qualifier.cloned(), field.name().clone()))
}

/// Upper bound for the number of rows that can contribute values for `key`.
///
/// This intentionally tracks the key's source domain instead of summing every
/// scan under the dim side. For q17-like aggregates, the filtered side may
/// include a large fact scan, but the propagated key domain is still bounded by
/// the grouped dimension key (for example `item.i_id`).
pub(super) fn key_domain_upper_bound_rows(plan: &LogicalPlan, key: &Column) -> Option<usize> {
    if !plan.schema().has_column(key) {
        return None;
    }

    match plan {
        LogicalPlan::TableScan(scan) => table_scan_upper_bound_rows(scan),
        LogicalPlan::Filter(filter) => key_domain_upper_bound_rows(&filter.input, key),
        LogicalPlan::Limit(limit) => key_domain_upper_bound_rows(&limit.input, key),
        LogicalPlan::Projection(projection) => {
            let index = projection.schema.maybe_index_of_column(key)?;
            let expr = projection.expr.get(index)?;
            key_domain_upper_bound_rows_for_expr(&projection.input, expr)
        }
        LogicalPlan::SubqueryAlias(alias) => key_for_input_schema(&alias.input, key)
            .and_then(|input_key| key_domain_upper_bound_rows(&alias.input, &input_key)),
        LogicalPlan::Aggregate(aggregate) => {
            let key_in_group = aggregate
                .group_expr
                .iter()
                .any(|expr| matches!(expr, Expr::Column(column) if column == key));
            if key_in_group {
                key_domain_upper_bound_rows(&aggregate.input, key)
            } else {
                None
            }
        }
        LogicalPlan::Distinct(distinct) => {
            use datafusion::logical_expr::Distinct;
            let key_kept = match distinct {
                Distinct::All(_) => true,
                Distinct::On(on) => on
                    .on_expr
                    .iter()
                    .any(|expr| matches!(expr, Expr::Column(column) if column == key)),
            };
            if key_kept {
                key_domain_upper_bound_rows(distinct_input(distinct), key)
            } else {
                None
            }
        }
        LogicalPlan::Join(join)
            if join.join_type == JoinType::Inner
                && join.null_equality == NullEquality::NullEqualsNothing =>
        {
            let left_rows = if join.left.schema().has_column(key) {
                key_domain_upper_bound_rows(&join.left, key)
            } else {
                None
            };
            let right_rows = if join.right.schema().has_column(key) {
                key_domain_upper_bound_rows(&join.right, key)
            } else {
                None
            };
            match (left_rows, right_rows) {
                (Some(left), Some(right)) => Some(left.min(right)),
                (Some(rows), None) | (None, Some(rows)) => Some(rows),
                (None, None) => None,
            }
        }
        _ => None,
    }
}

pub(super) fn key_domain_upper_bound_rows_for_expr(
    input: &LogicalPlan,
    expr: &Expr,
) -> Option<usize> {
    match expr {
        Expr::Column(column) => key_domain_upper_bound_rows(input, column),
        Expr::Alias(alias) => key_domain_upper_bound_rows_for_expr(input, &alias.expr),
        _ => None,
    }
}

/// `true` when propagation should be skipped based on cardinality.
///
/// Propagation only pays when the receiving side is known large and the
/// filtered key domain is known small. Missing statistics are therefore a
/// skip, not a fallback to shape-only heuristics.
pub(super) fn skip_propagation_by_cardinality(
    dim_side: &LogicalPlan,
    fact_side: &LogicalPlan,
    dim_key: &Column,
) -> bool {
    let dim_key_domain_rows = key_domain_upper_bound_rows(dim_side, dim_key);

    tracing::debug!(
        dim_key_domain_rows = ?dim_key_domain_rows,
        "CayennePropagateFilterAcrossEquiJoinKeys: dim-side key-domain cardinality"
    );

    let fact_rows = subtree_upper_bound_rows(fact_side);

    tracing::debug!(
        fact_rows = ?fact_rows,
        "CayennePropagateFilterAcrossEquiJoinKeys: fact-side cardinality"
    );

    let Some(fact_rows) = fact_rows else {
        return true;
    };
    if fact_rows < MIN_FACT_ROWS_FOR_PROPAGATION {
        return true;
    }

    let Some(dim_key_domain_rows) = dim_key_domain_rows else {
        return true;
    };

    if dim_key_domain_rows == 0 {
        return false;
    }

    if fact_rows < dim_key_domain_rows.saturating_mul(MIN_FACT_TO_DIM_KEY_DOMAIN_RATIO) {
        return true;
    }

    false
}

/// Returns `true` if `key` retains its scan-level domain through every
/// `Aggregate` / `Distinct` reachable in `plan`.
///
/// * `Aggregate` preserves a column's domain only when it appears in
///   `group_expr` as a plain `Expr::Column` reference.
/// * `Distinct::All` preserves every projected column (deduplication keeps
///   value identity).
/// * `Distinct::On(distinct_on)` preserves only the columns named in its `on`
///   list; for safety we conservatively require the key to appear there.
///
/// The walk follows only identity-preserving operators plus inner equi-joins —
/// the same shape `is_dim_like_subtree` accepts. Anything outside that vocab
/// (`Sort`, `Window`, etc.) is conservatively rejected by returning `false`.
pub(super) fn key_preserved_through_summaries(plan: &LogicalPlan, key: &Column) -> bool {
    fn walk(plan: &LogicalPlan, key: &Column) -> bool {
        match plan {
            LogicalPlan::TableScan(_) => plan.schema().has_column(key),
            LogicalPlan::Projection(p) => plan.schema().has_column(key) && walk(&p.input, key),
            LogicalPlan::SubqueryAlias(a) => {
                let relation_matches_alias = match key.relation.as_ref() {
                    Some(relation) => relation == &a.alias,
                    None => true,
                };
                relation_matches_alias
                    && key_for_input_schema(&a.input, key)
                        .is_some_and(|input_key| walk(&a.input, &input_key))
            }
            LogicalPlan::Filter(f) => plan.schema().has_column(key) && walk(&f.input, key),
            LogicalPlan::Limit(l) => plan.schema().has_column(key) && walk(&l.input, key),
            LogicalPlan::Aggregate(a) => {
                let key_in_group = a
                    .group_expr
                    .iter()
                    .any(|expr| matches!(expr, Expr::Column(column) if column == key));
                key_in_group && plan.schema().has_column(key) && walk(&a.input, key)
            }
            LogicalPlan::Distinct(distinct) => {
                use datafusion::logical_expr::Distinct;
                let key_kept = match distinct {
                    Distinct::All(_) => true,
                    Distinct::On(on) => on
                        .on_expr
                        .iter()
                        .any(|expr| matches!(expr, Expr::Column(column) if column == key)),
                };
                key_kept && plan.schema().has_column(key) && walk(distinct_input(distinct), key)
            }
            LogicalPlan::Join(j)
                if j.join_type == JoinType::Inner
                    && j.null_equality == NullEquality::NullEqualsNothing =>
            {
                plan.schema().has_column(key) && (walk(&j.left, key) || walk(&j.right, key))
            }
            _ => false,
        }
    }

    walk(plan, key)
}

pub(super) fn collect_selective_filter_columns(expr: &Expr, columns: &mut Vec<Column>) {
    match expr {
        Expr::BinaryExpr(binary) if binary.op == Operator::And => {
            collect_selective_filter_columns(&binary.left, columns);
            collect_selective_filter_columns(&binary.right, columns);
        }
        Expr::BinaryExpr(binary) if comparison_operator_is_selective(binary.op) => {
            collect_literal_comparison_columns(&binary.left, &binary.right, columns);
            collect_literal_comparison_columns(&binary.right, &binary.left, columns);
        }
        Expr::Between(between)
            if !between.negated
                && expr_is_literal_like(&between.low)
                && expr_is_literal_like(&between.high) =>
        {
            collect_columns_from_expr(&between.expr, columns);
        }
        Expr::InList(in_list)
            if !in_list.negated
                && !in_list.list.is_empty()
                && in_list.list.iter().all(expr_is_literal_like) =>
        {
            collect_columns_from_expr(&in_list.expr, columns);
        }
        Expr::Like(like) if !like.negated && expr_is_literal_like(&like.pattern) => {
            collect_columns_from_expr(&like.expr, columns);
        }
        _ => {}
    }
}

pub(super) fn comparison_operator_is_selective(op: Operator) -> bool {
    matches!(
        op,
        Operator::Eq | Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq
    )
}

pub(super) fn collect_literal_comparison_columns(
    expr: &Expr,
    other: &Expr,
    columns: &mut Vec<Column>,
) {
    if expr_is_literal_like(other) {
        collect_columns_from_expr(expr, columns);
    }
}

pub(super) fn expr_is_literal_like(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(_, _) => true,
        Expr::Cast(cast) => expr_is_literal_like(&cast.expr),
        Expr::TryCast(cast) => expr_is_literal_like(&cast.expr),
        _ => false,
    }
}

pub(super) fn collect_columns_from_expr(expr: &Expr, columns: &mut Vec<Column>) {
    let _ = expr.apply(|e| {
        if let Expr::Column(column) = e
            && !columns
                .iter()
                .any(|existing| columns_match(existing, column))
        {
            columns.push(column.clone());
        }

        Ok(TreeNodeRecursion::Continue)
    });
}

pub(super) fn columns_match(left: &Column, right: &Column) -> bool {
    left == right
        || (left.name == right.name && (left.relation.is_none() || right.relation.is_none()))
}

pub(super) fn collect_propagated_filter_targets(expr: &Expr, targets: &mut BTreeSet<String>) {
    let _ = expr.apply(|e| {
        if let Expr::InSubquery(InSubquery {
            expr: target_expr,
            subquery,
            ..
        }) = e
            && let LogicalPlan::SubqueryAlias(alias) = subquery.subquery.as_ref()
            && alias
                .alias
                .table()
                .starts_with(PROPAGATED_FILTER_ALIAS_PREFIX)
        {
            targets.insert(target_expr.to_string());
            return Ok(TreeNodeRecursion::Jump);
        }

        Ok(TreeNodeRecursion::Continue)
    });
}

/// Returns `true` if `plan` already contains a propagated-filter marker.
#[cfg(test)]
pub(super) fn subtree_has_propagated_filter(plan: &LogicalPlan) -> bool {
    let mut found = false;
    let _ = plan.apply(|node| {
        if let LogicalPlan::SubqueryAlias(alias) = node
            && alias
                .alias
                .table()
                .starts_with(PROPAGATED_FILTER_ALIAS_PREFIX)
        {
            found = true;
            return Ok(TreeNodeRecursion::Stop);
        }
        if let LogicalPlan::Filter(f) = node
            && expr_has_propagated_filter(&f.predicate)
        {
            found = true;
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    });
    found
}

/// Returns `true` if `expr` contains an [`InSubquery`] whose inner plan
/// starts with a [`SubqueryAlias`] named with
/// [`PROPAGATED_FILTER_ALIAS_PREFIX`].
#[must_use]
#[cfg(test)]
pub(super) fn expr_has_propagated_filter(expr: &Expr) -> bool {
    let mut found = false;
    let _ = expr.apply(|e| {
        if let Expr::InSubquery(InSubquery { subquery, .. }) = e
            && let LogicalPlan::SubqueryAlias(alias) = subquery.subquery.as_ref()
            && alias
                .alias
                .table()
                .starts_with(PROPAGATED_FILTER_ALIAS_PREFIX)
        {
            found = true;
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    });
    found
}

/// Build a `SubqueryAlias(__cayenne_xclos__N, Projection([key_col], subtree))`
/// suitable for use as the inner plan of a [`Subquery`] referenced by an
/// [`InSubquery`] expression.
///
/// The alias name uses [`PROPAGATED_FILTER_ALIAS_PREFIX`] plus a unique id
/// from [`OptimizerConfig::alias_generator`], so each invocation produces a
/// distinct marker. The marker doubles as the cycle-detection sentinel
/// scanned by [`analyze_logical_side`].
pub(super) fn build_key_projection_subquery(
    subtree: Arc<LogicalPlan>,
    key_col: &Column,
    alias_gen: &Arc<datafusion::common::alias::AliasGenerator>,
) -> Result<LogicalPlan> {
    let key_expr = Expr::Column(key_col.clone());
    let projection = LogicalPlan::Projection(Projection::try_new(vec![key_expr], subtree)?);
    let alias_name = alias_gen.next(PROPAGATED_FILTER_ALIAS_PREFIX);
    let aliased = SubqueryAlias::try_new(Arc::new(projection), TableReference::bare(alias_name))?;
    Ok(LogicalPlan::SubqueryAlias(aliased))
}

/// Wrap `input` with `Filter(target IN (subquery))` using the `subquery_plan`
/// (which must already be a `SubqueryAlias` named with
/// [`PROPAGATED_FILTER_ALIAS_PREFIX`]) as the right-hand side.
pub(super) fn wrap_with_in_subquery_filter_expr(
    input: Arc<LogicalPlan>,
    target: &Expr,
    subquery_plan: LogicalPlan,
) -> Result<LogicalPlan> {
    let predicate = Expr::InSubquery(InSubquery::new(
        Box::new(target.clone()),
        Subquery {
            subquery: Arc::new(subquery_plan),
            outer_ref_columns: vec![],
            spans: Spans::default(),
        },
        false,
    ));
    let filter = Filter::try_new(predicate, input)?;
    Ok(LogicalPlan::Filter(filter))
}
