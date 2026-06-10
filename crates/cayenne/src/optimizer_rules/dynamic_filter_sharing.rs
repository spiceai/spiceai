//! [`CayenneDynamicFilterSharing`]: a [`PhysicalOptimizerRule`] that walks
//! `HashJoinExec` nodes (`Inner`/`LeftSemi`/`RightSemi`, default SQL NULL
//! equality) whose two sides contain `CayenneAccelerationExec` scans over the
//! same underlying table, and installs each dynamic filter already pushed
//! into one scan onto the equi-joined sibling scan — sharing the same
//! `Arc<dyn PhysicalExpr>` so both scans observe updates simultaneously.
//! Bails for anti joins, differently named key columns, ambiguous matches
//! (more than one same-source scan pair for a key), mismatched scan schemas,
//! and filters referencing columns outside the proven equi-joined set.

use super::{
    Arc, BTreeSet, CayenneAccelerationExec, ConfigOptions, DataFusionError, DataType,
    ExecutionPlan, HashJoinExec, HashMap, JoinType, NullEquality, PhysicalExpr,
    PhysicalOptimizerRule, Result, ScanIdentity, Transformed, TransformedResult, TreeNode,
    collect_cayenne_scans, physical_column_name, plan_schema_fields, same_source_pairs_for_column,
    scans_by_identity,
};

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
pub(super) struct FilterAddition {
    pub(super) identity: Arc<ScanIdentity>,
    pub(super) schema_fields: Vec<(String, DataType)>,
    pub(super) filter: Arc<dyn PhysicalExpr>,
}

pub(super) fn filter_additions_for_join(
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

pub(super) fn push_filter_addition(
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

pub(super) fn apply_filter_additions(
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
