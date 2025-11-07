use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Result, exec_err};
use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion_datasource::source::DataSourceExec;
use datafusion_optimizer_rules::common::plan_node_key::PlanNodeKey;
use datafusion_optimizer_rules::common::search_visitor::SearchVisitor;
use datafusion_optimizer_rules::concrete;
use std::collections::HashMap;
use std::sync::Arc;

/// This looks for any `ProjectionExec` atop `UnionExec` and attempts to push it down into
/// the inputs of the union
#[derive(Debug)]
pub struct UnionProjectionPushdownOptimizer {}

impl UnionProjectionPushdownOptimizer {
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(UnionProjectionPushdownOptimizer {})
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
                        if (p.children().len() == 1 && concrete!(p, ProjectionExec).is_none())
                            || concrete!(p, DataSourceExec).is_some()
                        {
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

impl PhysicalOptimizerRule for UnionProjectionPushdownOptimizer {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut replacements: HashMap<PlanNodeKey, Arc<dyn ExecutionPlan>> = HashMap::new();

        let pruned = Arc::clone(&plan)
            .transform_down(|p| {
                // Only operate on `ProjectionExec`
                let Some(projection) = concrete!(p, ProjectionExec) else {
                    return Ok(Transformed::no(p));
                };

                // Find an eligible union to push down to
                let Some(union_exec) = Self::find_eligible_union(projection)? else {
                    return Ok(Transformed::no(p));
                };

                let projection_expr = projection.expr().to_vec();

                // Take the projection and apply it on top of the union inputs. Specifically, on top
                // of the repartition exec emitted by `DistributeFileScanOptimizer`
                for leaf in union_exec.children() {
                    let leaf_key: PlanNodeKey = leaf.as_ref().into();

                    // Decorate the projection atop the union input
                    let projection = Arc::new(ProjectionExec::try_new(
                        projection_expr.clone(),
                        Arc::clone(leaf),
                    )?);

                    // Find the downstream repartition or coalesce
                    let maybe_repartition =
                        SearchVisitor::first_concrete_down::<RepartitionExec>(leaf)?;

                    let maybe_coalesce =
                        SearchVisitor::first_concrete_down::<CoalescePartitionsExec>(leaf)?;

                    let rewrite_leaf: Arc<dyn ExecutionPlan> =
                        if let Some(repartition) = maybe_repartition {
                            Arc::new(RepartitionExec::try_new(
                                projection,
                                repartition.output_partitioning().clone(),
                            )?)
                        } else if maybe_coalesce.is_some() {
                            Arc::new(CoalescePartitionsExec::new(projection))
                        } else {
                            projection
                        };

                    replacements.insert(leaf_key, rewrite_leaf);
                }

                Ok(Transformed::yes(Arc::clone(projection.input())))
            })?
            .data;

        // If we can push down, this will be populated
        if replacements.is_empty() {
            return Ok(plan);
        }

        // Rewrite projection-pruned plan with replacements
        let optimized = pruned
            .transform_down(|p| {
                if let Some(replacement) = replacements.remove(&p.as_ref().into()) {
                    Ok(Transformed::yes(replacement))
                } else {
                    Ok(Transformed::no(p))
                }
            })?
            .data;

        // If there are any leftover replacements, something is wrong
        if replacements.is_empty() {
            Ok(optimized)
        } else {
            exec_err!(
                "{}: Failed to bind all plan replacements. Report this bug on GitHub: https://github.com/spiceai/spiceai/issues",
                self.name()
            )
        }
    }

    fn name(&self) -> &'static str {
        "UnionProjectionPushdownOptimizer"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datafusion::cluster::physical_plan::optimizer::distribute_file_scan::DistributeFileScanOptimizer;
    use crate::datafusion::cluster::physical_plan::optimizer::distribute_file_scan::tests::create_partitioned_file;
    use crate::datafusion::cluster::physical_plan::optimizer::distribute_file_scan::tests::{
        DEFAULT_CONFIG_OPTIONS, create_data_source_exec,
    };
    use datafusion::physical_expr::expressions::col;
    use datafusion::physical_optimizer::optimizer::PhysicalOptimizer;
    use std::sync::LazyLock;

    static OPTIMIZER: LazyLock<PhysicalOptimizer> = LazyLock::new(|| {
        PhysicalOptimizer::with_rules(vec![
            DistributeFileScanOptimizer::new(),
            UnionProjectionPushdownOptimizer::new(),
        ])
    });

    fn optimize(plan: &Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        OPTIMIZER.rules.iter().fold(Arc::clone(plan), |acc, rule| {
            rule.optimize(acc, &DEFAULT_CONFIG_OPTIONS)
                .expect("Must optimize")
        })
    }

    #[tokio::test]
    async fn test_projection_pushdown() {
        let files = vec![
            create_partitioned_file("file:///file4.parquet", 256_000_000, None),
            create_partitioned_file("file:///file5.parquet", 256_000_000, None),
        ];

        let data_source_exec = create_data_source_exec(files);
        let projection_exec = ProjectionExec::try_new(
            vec![(
                col("id", data_source_exec.schema().as_ref()).expect("Must bind expr"),
                "foo".to_string(),
            )],
            data_source_exec,
        )
        .expect("Must make projection_exec");
        let plan: Arc<dyn ExecutionPlan> = Arc::new(projection_exec);

        // We start with 1 projection
        assert_eq!(
            SearchVisitor::collect_concrete_down::<ProjectionExec>(&plan)
                .expect("Must collect")
                .len(),
            1
        );

        let optimized = optimize(&plan);

        let data_source_exec_leaves =
            SearchVisitor::collect_concrete_down::<DataSourceExec>(&optimized)
                .expect("Must collect")
                .len();

        // Make sure we have enough leaves to test with
        assert!(data_source_exec_leaves > 1);

        // The number of projections must match the number of DataSourceExec leaves
        assert_eq!(
            SearchVisitor::collect_concrete_down::<ProjectionExec>(&optimized)
                .expect("Must collect")
                .len(),
            data_source_exec_leaves
        );
    }
}
