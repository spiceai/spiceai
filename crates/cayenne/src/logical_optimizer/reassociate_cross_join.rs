//! [`CayenneReassociateCrossJoin`]: a logical [`OptimizerRule`]
//! ([`ApplyOrder::BottomUp`]) that matches an inner join whose left input is a
//! pure cross join — `(A CROSS B) JOIN C` — and rewrites it to
//! `A JOIN (B JOIN C)`, moving the B-C equi-keys and B/C-only filter
//! conjuncts inward while A-C keys and remaining conjuncts stay on the outer
//! join. Bails when no B-C equi-key exists, when an `on` key pairs the inputs
//! any other way (or has unknown/volatile column provenance), or when neither
//! B nor C contains a Cayenne table scan.

use super::{
    ApplyOrder, Arc, CayenneTableProvider, DataFusionError, DefaultTableSource, Expr, Join,
    JoinConstraint, JoinType, LogicalPlan, NullEquality, OptimizerConfig, OptimizerRule, Result,
    TableProvider, TableProviderPredicate, TableSource, TableSourcePredicate, Transformed,
    TreeNode, TreeNodeRecursion, conjunction, contains_cayenne_table_scan, split_conjunction_owned,
};

/// Logical optimizer rule that reassociates a left-deep inner join when the
/// SQL `FROM` order leaves an early cross join in front of a later selective
/// join.
///
/// A typical shape is `(A CROSS B) JOIN C`, where the parent join predicates
/// only reference `B` and `C`. This rule rewrites that to `A CROSS (B JOIN C)`
/// while preserving the final output schema order. If the parent join also
/// contains predicates involving `A`, only the `B`/`C` predicates move inward
/// and the `A` predicates remain on the outer join.
pub struct CayenneReassociateCrossJoin {
    is_cayenne_table_source: TableSourcePredicate,
}

impl Default for CayenneReassociateCrossJoin {
    fn default() -> Self {
        Self::new()
    }
}

impl CayenneReassociateCrossJoin {
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

impl std::fmt::Debug for CayenneReassociateCrossJoin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CayenneReassociateCrossJoin").finish()
    }
}

impl OptimizerRule for CayenneReassociateCrossJoin {
    fn name(&self) -> &'static str {
        "cayenne_reassociate_cross_join"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        // BottomUp lets a single optimizer pass grow `A CROSS (B JOIN C)` into
        // `A JOIN (B JOIN C JOIN D)` as later joins expose more B/D predicates.
        Some(ApplyOrder::BottomUp)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>, DataFusionError> {
        let LogicalPlan::Join(join) = plan else {
            return Ok(Transformed::no(plan));
        };

        reassociate_cross_join(join, &self.is_cayenne_table_source)
    }
}

pub(super) fn reassociate_cross_join(
    join: Join,
    is_cayenne_table_source: &TableSourcePredicate,
) -> Result<Transformed<LogicalPlan>, DataFusionError> {
    if !is_reassociable_inner_join(&join) {
        return Ok(Transformed::no(LogicalPlan::Join(join)));
    }
    let LogicalPlan::Join(cross_join) = join.left.as_ref() else {
        return Ok(Transformed::no(LogicalPlan::Join(join)));
    };
    if !is_pure_inner_cross_join(cross_join) {
        return Ok(Transformed::no(LogicalPlan::Join(join)));
    }
    if !contains_cayenne_table_scan(&cross_join.right, is_cayenne_table_source)
        && !contains_cayenne_table_scan(&join.right, is_cayenne_table_source)
    {
        return Ok(Transformed::no(LogicalPlan::Join(join)));
    }

    let cross_left = cross_join.left.as_ref();
    let cross_right = cross_join.right.as_ref();
    let join_right = join.right.as_ref();

    let mut inner_on = Vec::new();
    let mut outer_on = Vec::new();
    for (left, right) in &join.on {
        let Some(left_refs) = expr_input_refs(left, cross_left, cross_right, join_right) else {
            return Ok(Transformed::no(LogicalPlan::Join(join)));
        };
        let Some(right_refs) = expr_input_refs(right, cross_left, cross_right, join_right) else {
            return Ok(Transformed::no(LogicalPlan::Join(join)));
        };

        if left_refs.only_cross_right() && right_refs.only_join_right() {
            inner_on.push((left.clone(), right.clone()));
        } else if left_refs.only_cross_left() && right_refs.only_join_right() {
            outer_on.push((left.clone(), right.clone()));
        } else {
            return Ok(Transformed::no(LogicalPlan::Join(join)));
        }
    }

    if inner_on.is_empty() {
        return Ok(Transformed::no(LogicalPlan::Join(join)));
    }

    let mut inner_filters = Vec::new();
    let mut outer_filters = Vec::new();
    if let Some(filter) = join.filter.clone() {
        for conjunct in split_conjunction_owned(filter) {
            let Some(refs) = expr_input_refs(&conjunct, cross_left, cross_right, join_right) else {
                return Ok(Transformed::no(LogicalPlan::Join(join)));
            };

            if refs.only_cross_right_and_join_right() {
                inner_filters.push(conjunct);
            } else {
                outer_filters.push(conjunct);
            }
        }
    }

    let inner_join = LogicalPlan::Join(Join::try_new(
        Arc::clone(&cross_join.right),
        Arc::clone(&join.right),
        inner_on,
        conjunction(inner_filters),
        JoinType::Inner,
        JoinConstraint::On,
        NullEquality::NullEqualsNothing,
        false,
    )?);

    let outer_join = Join::try_new(
        Arc::clone(&cross_join.left),
        Arc::new(inner_join),
        outer_on,
        conjunction(outer_filters),
        JoinType::Inner,
        JoinConstraint::On,
        NullEquality::NullEqualsNothing,
        false,
    )?;

    Ok(Transformed::yes(LogicalPlan::Join(outer_join)))
}

pub(super) fn is_reassociable_inner_join(join: &Join) -> bool {
    join.join_type == JoinType::Inner
        && join.join_constraint == JoinConstraint::On
        && join.null_equality == NullEquality::NullEqualsNothing
}

pub(super) fn is_pure_inner_cross_join(join: &Join) -> bool {
    is_reassociable_inner_join(join) && join.on.is_empty() && join.filter.is_none()
}

#[derive(Default)]
pub(super) struct JoinInputRefs {
    cross_left: bool,
    cross_right: bool,
    join_right: bool,
}

impl JoinInputRefs {
    fn only_cross_left(&self) -> bool {
        self.cross_left && !self.cross_right && !self.join_right
    }

    fn only_cross_right(&self) -> bool {
        !self.cross_left && self.cross_right && !self.join_right
    }

    fn only_join_right(&self) -> bool {
        !self.cross_left && !self.cross_right && self.join_right
    }

    fn only_cross_right_and_join_right(&self) -> bool {
        !self.cross_left && self.cross_right && self.join_right
    }
}

pub(super) fn expr_input_refs(
    expr: &Expr,
    cross_left: &LogicalPlan,
    cross_right: &LogicalPlan,
    join_right: &LogicalPlan,
) -> Option<JoinInputRefs> {
    if expr.is_volatile() {
        return None;
    }

    let mut refs = JoinInputRefs::default();
    let mut unknown = false;
    let _ = expr.apply(|node| {
        match node {
            Expr::Column(column) => {
                match (
                    cross_left.schema().has_column(column),
                    cross_right.schema().has_column(column),
                    join_right.schema().has_column(column),
                ) {
                    (true, false, false) => refs.cross_left = true,
                    (false, true, false) => refs.cross_right = true,
                    (false, false, true) => refs.join_right = true,
                    _ => {
                        unknown = true;
                        return Ok(TreeNodeRecursion::Stop);
                    }
                }
            }
            Expr::OuterReferenceColumn(_, _) => {
                unknown = true;
                return Ok(TreeNodeRecursion::Stop);
            }
            _ => {}
        }

        Ok(TreeNodeRecursion::Continue)
    });

    if unknown { None } else { Some(refs) }
}
