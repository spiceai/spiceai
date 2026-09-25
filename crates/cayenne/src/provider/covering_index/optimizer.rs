/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! Physical planning for bounded covering-index joins.

use std::{collections::HashSet, fmt, sync::Arc};

use arrow_schema::Schema;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::{ExecutionPlan, joins::HashJoinExec};
use datafusion_common::{DataFusionError, JoinSide, NullEquality, Result};
use datafusion_expr::{JoinType, Operator};
use datafusion_physical_expr::{
    PhysicalExprRef,
    expressions::{BinaryExpr, Column},
};
use datafusion_physical_plan::{
    coalesce_partitions::CoalescePartitionsExec,
    filter::FilterExec,
    joins::utils::{ColumnIndex, JoinFilter},
    projection::ProjectionExec,
    repartition::RepartitionExec,
};
use runtime_datafusion::execution_plan::schema_cast::SchemaCastScanExec;

use super::{
    CayenneIndexJoinExec, CayenneIndexScanExec, CoveringIndexCapability, IndexDefinition,
    IndexJoinMapping,
};
use crate::provider::CayenneAccelerationExec;

const MAX_OUTER_CANDIDATES: usize = 2_048;
const MAX_CANDIDATE_PAIRS: usize = 65_536;
const MIN_INNER_SELECTIVITY_DENOMINATOR: usize = 2_048;

/// Replaces a bounded `HashJoinExec` edge with a one-child covering index join.
#[derive(Default)]
pub struct CayenneIndexJoinRewriter;

impl CayenneIndexJoinRewriter {
    /// Creates the rule.
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl fmt::Debug for CayenneIndexJoinRewriter {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("CayenneIndexJoinRewriter").finish()
    }
}

impl PhysicalOptimizerRule for CayenneIndexJoinRewriter {
    fn name(&self) -> &'static str {
        "CayenneIndexJoinRewriter"
    }

    fn schema_check(&self) -> bool {
        false
    }

    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|node| {
            let Some(join) = node.downcast_ref::<HashJoinExec>() else {
                return Ok(Transformed::no(node));
            };
            let Some(replacement) = rewrite_join(join)? else {
                return Ok(Transformed::no(node));
            };
            Ok(Transformed::yes(replacement))
        })
        .data()
    }
}

#[derive(Clone)]
struct CapturedInner {
    capability: CoveringIndexCapability,
    filters: Vec<PhysicalExprRef>,
}

struct Candidate {
    outer: Arc<dyn ExecutionPlan>,
    inner: CapturedInner,
    definition: IndexDefinition,
    outer_keys: Vec<PhysicalExprRef>,
    mapping: IndexJoinMapping,
    residual_filters: Vec<JoinFilter>,
    outer_bound: usize,
    candidate_pairs: usize,
    output_bound: usize,
}

fn rewrite_join(join: &HashJoinExec) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    let join_type = join.join_type();
    if !matches!(
        *join_type,
        JoinType::Inner | JoinType::Left | JoinType::Right
    ) || join.null_equality() != NullEquality::NullEqualsNothing
        || join.on().is_empty()
    {
        return Ok(None);
    }

    let mut candidates = Vec::new();
    let orientations: &[bool] = match join_type {
        JoinType::Inner => &[true, false],
        JoinType::Left => &[true],
        // DataFusion can commute a logical LEFT into a physical RIGHT before
        // this rule runs; preserving that physical join type preserves output.
        JoinType::Right => &[false],
        _ => return Ok(None),
    };
    for &outer_is_left in orientations {
        let (outer, inner_plan) = if outer_is_left {
            (Arc::clone(join.left()), Arc::clone(join.right()))
        } else {
            (Arc::clone(join.right()), Arc::clone(join.left()))
        };
        let Some(outer_bound) = outer_candidate_upper_bound(&outer) else {
            continue;
        };
        if outer_bound > MAX_OUTER_CANDIDATES {
            continue;
        }
        let Some(inner) = captured_inner(&inner_plan) else {
            continue;
        };
        for access in inner.capability.accesses() {
            let Some((outer_keys, residual_filters)) =
                indexed_key_mapping(join, &inner.capability, access.definition(), outer_is_left)
            else {
                continue;
            };
            let max_multiplicity = access
                .view()
                .catalog()
                .runs()
                .iter()
                .try_fold(0usize, |total, run| {
                    total.checked_add(run.max_duplicate_key_count())
                });
            let inner_rows = access
                .view()
                .catalog()
                .sources()
                .values()
                .try_fold(0usize, |total, source| {
                    total.checked_add(source.row_count())
                });
            let (Some(max_multiplicity), Some(inner_rows)) = (max_multiplicity, inner_rows) else {
                continue;
            };
            let Some(candidate_pairs) = outer_bound.checked_mul(max_multiplicity) else {
                continue;
            };
            if candidate_pairs > MAX_CANDIDATE_PAIRS
                || candidate_pairs
                    > MIN_INNER_SELECTIVITY_DENOMINATOR.max(inner_rows.saturating_div(8))
            {
                continue;
            }
            candidates.push(Candidate {
                outer: Arc::clone(&outer),
                inner: inner.clone(),
                definition: access.definition().clone(),
                outer_keys,
                mapping: IndexJoinMapping::new(outer_is_left),
                residual_filters,
                outer_bound,
                candidate_pairs,
                output_bound: if matches!(*join_type, JoinType::Left | JoinType::Right) {
                    outer_bound.max(candidate_pairs)
                } else {
                    candidate_pairs
                },
            });
        }
    }

    candidates.sort_by_key(|candidate| {
        (
            candidate.candidate_pairs,
            candidate.outer_bound,
            !candidate.mapping.outer_is_left(),
        )
    });
    let Some(candidate) = candidates.into_iter().next() else {
        return Ok(None);
    };
    let replacement = CayenneIndexJoinExec::try_new_with_inner_filters(
        candidate.outer,
        candidate.inner.capability,
        &candidate.definition,
        &candidate.outer_keys,
        candidate.mapping,
        join.projection
            .as_ref()
            .map(|projection| projection.to_vec()),
        join.filter().cloned(),
        &candidate.inner.filters,
        &candidate.residual_filters,
        Some(candidate.output_bound),
        *join_type,
        join.null_equality(),
    )?;
    if replacement.schema().as_ref() != join.schema().as_ref() {
        return Ok(None);
    }
    Ok(Some(Arc::new(replacement)))
}

fn outer_candidate_upper_bound(plan: &Arc<dyn ExecutionPlan>) -> Option<usize> {
    if let Some(scan) = plan.downcast_ref::<CayenneIndexScanExec>() {
        return Some(scan.raw_candidate_upper_bound());
    }
    if let Some(join) = plan.downcast_ref::<CayenneIndexJoinExec>() {
        return join.raw_output_candidate_upper_bound();
    }
    if plan.is::<CayenneAccelerationExec>()
        || plan.is::<FilterExec>()
        || plan.is::<RepartitionExec>()
        || plan.is::<CoalescePartitionsExec>()
    {
        return only_child(plan).and_then(outer_candidate_upper_bound);
    }
    if let Some(projection) = plan.downcast_ref::<ProjectionExec>() {
        projection_is_bare_columns(projection).then(|| ())?;
        return only_child(plan).and_then(outer_candidate_upper_bound);
    }
    if plan.is::<SchemaCastScanExec>() && schema_cast_is_identity(plan) {
        return only_child(plan).and_then(outer_candidate_upper_bound);
    }
    None
}

fn captured_inner(plan: &Arc<dyn ExecutionPlan>) -> Option<CapturedInner> {
    if let Some(scan) = plan.downcast_ref::<CayenneAccelerationExec>() {
        let capability = scan.covering_index()?.clone();
        return capability
            .matches_output_schema(&scan.schema())
            .then_some(CapturedInner {
                capability,
                filters: Vec::new(),
            });
    }
    if let Some(filter) = plan.downcast_ref::<FilterExec>() {
        let mut captured = captured_inner(filter.input())?;
        captured
            .capability
            .matches_output_schema(&filter.input().schema())
            .then_some(())?;
        captured.filters.push(Arc::clone(filter.predicate()));
        return Some(captured);
    }
    if let Some(projection) = plan.downcast_ref::<ProjectionExec>() {
        let mut captured = captured_inner(projection.input())?;
        captured.capability = captured.capability.project_through(projection)?;
        captured
            .capability
            .matches_output_schema(&projection.schema())
            .then_some(captured)
    } else if plan.is::<RepartitionExec>()
        || plan.is::<CoalescePartitionsExec>()
        || (plan.is::<SchemaCastScanExec>() && schema_cast_is_identity(plan))
    {
        only_child(plan).and_then(captured_inner)
    } else {
        None
    }
}

fn indexed_key_mapping(
    join: &HashJoinExec,
    capability: &CoveringIndexCapability,
    definition: &IndexDefinition,
    inner_is_right: bool,
) -> Option<(Vec<PhysicalExprRef>, Vec<JoinFilter>)> {
    let mut used = HashSet::new();
    let mut outer_keys = Vec::with_capacity(definition.columns().len());
    for indexed in definition.columns() {
        let (position, outer) =
            join.on()
                .iter()
                .enumerate()
                .find_map(|(position, (left, right))| {
                    if used.contains(&position) {
                        return None;
                    }
                    let (outer, inner) = if inner_is_right {
                        (left, right)
                    } else {
                        (right, left)
                    };
                    let column = inner.downcast_ref::<Column>()?;
                    (capability.output_columns().get(column.index())
                        == Some(&indexed.schema_index())
                        && outer.downcast_ref::<Column>().is_some())
                    .then(|| (position, Arc::clone(outer)))
                })?;
        used.insert(position);
        outer_keys.push(outer);
    }

    let residual_filters = join
        .on()
        .iter()
        .enumerate()
        .filter(|(position, _)| !used.contains(position))
        .map(|(_, (left, right))| {
            equality_filter(left, right, join.left().schema(), join.right().schema())
        })
        .collect::<Option<Vec<_>>>()?;
    Some((outer_keys, residual_filters))
}

fn equality_filter(
    left: &PhysicalExprRef,
    right: &PhysicalExprRef,
    left_schema: arrow_schema::SchemaRef,
    right_schema: arrow_schema::SchemaRef,
) -> Option<JoinFilter> {
    let left = left.downcast_ref::<Column>()?;
    let right = right.downcast_ref::<Column>()?;
    let left_field = left_schema.fields().get(left.index())?;
    let right_field = right_schema.fields().get(right.index())?;
    let schema = Arc::new(Schema::new(vec![
        Arc::clone(left_field),
        Arc::clone(right_field),
    ]));
    Some(JoinFilter::new(
        Arc::new(BinaryExpr::new(
            Arc::new(Column::new(left_field.name(), 0)),
            Operator::Eq,
            Arc::new(Column::new(right_field.name(), 1)),
        )),
        vec![
            ColumnIndex {
                index: left.index(),
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: right.index(),
                side: JoinSide::Right,
            },
        ],
        schema,
    ))
}

fn only_child(plan: &Arc<dyn ExecutionPlan>) -> Option<&Arc<dyn ExecutionPlan>> {
    let children = plan.children();
    (children.len() == 1).then(|| children[0])
}

fn projection_is_bare_columns(projection: &ProjectionExec) -> bool {
    projection
        .expr()
        .iter()
        .enumerate()
        .all(|(output_index, expr)| {
            expr.expr.downcast_ref::<Column>().is_some_and(|column| {
                projection.input().schema().field(column.index())
                    == projection.schema().field(output_index)
            })
        })
}

fn schema_cast_is_identity(plan: &Arc<dyn ExecutionPlan>) -> bool {
    let Some(input) = only_child(plan) else {
        return false;
    };
    let input_schema = input.schema();
    let output_schema = plan.schema();
    // `SchemaCastScanExec` may replace schema or field metadata without
    // changing Arrow values. Metadata is not consumed by index key mapping or
    // execution; position, name, type, and nullability are the value contract.
    input_schema.fields().len() == output_schema.fields().len()
        && input_schema
            .fields()
            .iter()
            .zip(output_schema.fields())
            .all(|(input, output)| {
                input.name() == output.name()
                    && input.data_type() == output.data_type()
                    && input.is_nullable() == output.is_nullable()
            })
}
