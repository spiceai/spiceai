use crate::concrete;
use crate::datafusion::cluster::common::plan_node_key::PlanNodeKey;
use crate::datafusion::cluster::physical_plan::common::search_visitor::SearchVisitor;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Result, exec_err};
use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion_datasource::source::DataSourceExec;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug)]
pub struct UnionProjectionPushdown {}

impl UnionProjectionPushdown {
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(UnionProjectionPushdown {})
    }

    fn find_eligible_union(projection: &ProjectionExec) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        // Collect unary children until reaching a `UnionExec`.
        let mut stop = false;
        let children = SearchVisitor::default()
            .down(move |p| {
                if p.children().len() == 1 && !stop {
                    Some(Arc::clone(p))
                } else if concrete!(p, UnionExec).is_some() && !stop {
                    stop = true;
                    Some(Arc::clone(p))
                } else {
                    None
                }
            })
            .find(projection.input())?;

        // The last collected node must be a `UnionExec`, or we cannot apply the optimization
        let Some(union_exec) = children.last().and_then(|p| concrete!(p, UnionExec)) else {
            return Ok(None);
        };

        // The input schema of the projection must match the output schema of the union
        if union_exec.inputs().is_empty() || union_exec.schema() != projection.input().schema() {
            return Ok(None);
        }

        // All `UnionExec` inputs must also be unary chains that end with `DataSourceExec` leaves
        // that have the same schema, without any intermediate projections
        let data_source_exec_leaves = union_exec
            .inputs()
            .iter()
            .filter_map(|p_child| {
                SearchVisitor::default()
                    .down(move |p| {
                        if p.children().len() == 1 && concrete!(p, ProjectionExec).is_none() {
                            Some(Arc::clone(p))
                        } else if concrete!(p, DataSourceExec).is_some() {
                            Some(Arc::clone(p))
                        } else {
                            None
                        }
                    })
                    .find(p_child)
                    .ok()
                    .and_then(|nodes| nodes.into_iter().last())
            })
            .collect::<Vec<_>>();

        // The union inputs should represent the same number of DataSourceExec instances
        if data_source_exec_leaves.len() != union_exec.inputs().len() {
            return Ok(None);
        }

        Ok(children.last().cloned())
    }
}

impl PhysicalOptimizerRule for UnionProjectionPushdown {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut replacements: HashMap<PlanNodeKey, Arc<dyn ExecutionPlan>> = HashMap::new();

        let optimized = plan
            .transform_down(|p| {
                // We might need to rewrite this node
                if let Some(replacement) = replacements.remove(&p.as_ref().into()) {
                    return Ok(Transformed::yes(replacement));
                }

                // Only operate on `ProjectionExec`
                let Some(projection) = concrete!(p, ProjectionExec) else {
                    return Ok(Transformed::no(p));
                };

                // Find an eligible union to push down to
                let Some(union_exec) = Self::find_eligible_union(projection)? else {
                    return Ok(Transformed::no(p));
                };

                let projection_expr = projection.expr().to_vec();

                // Take the projection and apply it on top of the union inputs. Notably, this means
                // above the shuffles of `expand_file_scan`
                for leaf in union_exec.children() {
                    let leaf_key: PlanNodeKey = leaf.as_ref().into();

                    let projection = Arc::new(ProjectionExec::try_new(
                        projection_expr.clone(),
                        Arc::clone(leaf),
                    )?);

                    let maybe_repartition =
                        SearchVisitor::first_concrete_down::<RepartitionExec>(leaf)?;

                    let wrapped: Arc<dyn ExecutionPlan> = if let Some(repartition) =
                        maybe_repartition
                            .as_ref()
                            .and_then(|p| concrete!(p, RepartitionExec))
                    {
                        Arc::new(RepartitionExec::try_new(
                            projection,
                            repartition.partitioning().clone(),
                        )?)
                    } else {
                        projection
                    };

                    replacements.insert(leaf_key, wrapped);
                }

                Ok(Transformed::yes(Arc::clone(projection.input())))
            })?
            .data;

        if replacements.is_empty() {
            Ok(optimized)
        } else {
            exec_err!(
                "{}: Failed to bind all plan replacements. This is a bug.",
                self.name()
            )
        }
    }

    fn name(&self) -> &'static str {
        "InvertUnionProjectionOptimizer"
    }

    fn schema_check(&self) -> bool {
        true
    }
}
