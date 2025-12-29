/*
Copyright 2025 The Spice.ai OSS Authors

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

use crate::common::plan_node_key::PlanNodeKey;
use crate::common::search_visitor::SearchVisitor;
use crate::concrete;
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
        // The union inputs should represent the same number of DataSourceExec instances
        if union_exec
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
            .count()
            != union_exec.inputs().len()
        {
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
                let expected_schema = projection.input().schema();

                // Take the projection and apply it on top of the union inputs. Specifically, on top
                // of the repartition exec emitted by `DistributeFileScanOptimizer`
                for leaf in union_exec.children() {
                    // Verify the leaf schema matches the projection's input schema
                    // to ensure the column indices in projection_expr are valid.
                    // Without this check, we could get "project index N out of bounds" errors
                    // when the leaf has fewer columns than the projection expressions expect.
                    if leaf.schema() != expected_schema {
                        tracing::debug!(
                            expected_fields = expected_schema.fields().len(),
                            leaf_fields = leaf.schema().fields().len(),
                            "Skipping projection pushdown: union child schema differs from projection input schema"
                        );
                        return Ok(Transformed::no(p));
                    }

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

    use arrow::datatypes::{DataType, Field, Schema};
    use chrono::DateTime;
    use datafusion::datasource::physical_plan::ArrowSource;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::physical_expr::expressions::col;
    use datafusion::physical_optimizer::optimizer::PhysicalOptimizer;
    use datafusion_datasource::file_groups::FileGroup;
    use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
    use datafusion_datasource::{FileRange, PartitionedFile};
    use object_store::{ObjectMeta, path::Path};

    use std::sync::LazyLock;

    #[must_use]
    pub fn create_partitioned_file(
        path: &str,
        size: u64,
        range: Option<FileRange>,
    ) -> PartitionedFile {
        PartitionedFile {
            object_meta: ObjectMeta {
                location: Path::from(path),
                last_modified: DateTime::default(),
                size,
                e_tag: None,
                version: None,
            },
            partition_values: vec![],
            range,
            statistics: None,
            extensions: None,
            metadata_size_hint: None,
        }
    }
    fn file_scan_config_builder() -> FileScanConfigBuilder {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        FileScanConfigBuilder::new(
            ObjectStoreUrl::parse("file://tmp/").expect("Must parse dummy URL"),
            schema,
            Arc::new(ArrowSource::default()),
        )
    }

    #[must_use]
    pub fn create_data_source_exec(files: Vec<PartitionedFile>) -> Arc<dyn ExecutionPlan> {
        let fsc = file_scan_config_builder()
            .with_file_group(FileGroup::new(files))
            .build();

        DataSourceExec::from_data_source(fsc)
    }

    static OPTIMIZER: LazyLock<PhysicalOptimizer> = LazyLock::new(|| {
        PhysicalOptimizer::with_rules(vec![UnionProjectionPushdownOptimizer::new()])
    });

    fn optimize(plan: &Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        OPTIMIZER.rules.iter().fold(Arc::clone(plan), |acc, rule| {
            rule.optimize(acc, &ConfigOptions::default())
                .expect("Must optimize")
        })
    }

    #[ignore = "See #8313"]
    #[test]
    fn test_projection_pushdown() {
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

    /// Tests that the optimizer validates child schemas before pushing down projections.
    ///
    /// This test verifies that when the optimizer encounters a valid plan structure
    /// (`Projection` -> intermediate nodes -> `Union` -> `DataSources`), it correctly
    /// validates that the union children's schemas match the expected schema before
    /// applying the projection pushdown optimization.
    ///
    /// This is a defensive check to prevent "project index N out of bounds" errors
    /// that could occur if projection expressions (with column indices bound to one
    /// schema) are applied to plan nodes with different schemas.
    #[test]
    fn test_projection_pushdown_validates_schemas() {
        // Create a schema for the test
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ]));

        // Create DataSourceExec with the schema
        let fsc1 = FileScanConfigBuilder::new(
            ObjectStoreUrl::parse("file://tmp/").expect("Must parse dummy URL"),
            Arc::clone(&schema),
            Arc::new(ArrowSource::default()),
        )
        .with_file_group(FileGroup::new(vec![create_partitioned_file(
            "file:///file1.parquet",
            100,
            None,
        )]))
        .build();
        let data_source1: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(fsc1);

        let fsc2 = FileScanConfigBuilder::new(
            ObjectStoreUrl::parse("file://tmp/").expect("Must parse dummy URL"),
            Arc::clone(&schema),
            Arc::new(ArrowSource::default()),
        )
        .with_file_group(FileGroup::new(vec![create_partitioned_file(
            "file:///file2.parquet",
            100,
            None,
        )]))
        .build();
        let data_source2: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(fsc2);

        // Create a UnionExec
        let union_exec: Arc<dyn ExecutionPlan> =
            Arc::new(UnionExec::new(vec![data_source1, data_source2]));

        // Verify all schemas match
        assert_eq!(union_exec.schema().fields().len(), 3);
        for child in union_exec.children() {
            assert_eq!(
                child.schema(),
                union_exec.schema(),
                "Union child schema should match union schema"
            );
        }

        // Create projection expressions
        let projection_expr = vec![
            (
                col("id", union_exec.schema().as_ref()).expect("Must bind expr"),
                "id".to_string(),
            ),
            (
                col("value", union_exec.schema().as_ref()).expect("Must bind expr"),
                "value".to_string(),
            ),
        ];

        // Create projection on top of union
        let projection_exec =
            ProjectionExec::try_new(projection_expr, union_exec).expect("Must make projection");
        let plan: Arc<dyn ExecutionPlan> = Arc::new(projection_exec);

        // Run the optimizer - it should succeed without errors
        let result = OPTIMIZER.rules[0].optimize(Arc::clone(&plan), &ConfigOptions::default());
        assert!(result.is_ok(), "Optimization should not error");

        let optimized = result.expect("Must optimize");

        // The plan should still be valid (either optimized or unchanged)
        let projections = SearchVisitor::collect_concrete_down::<ProjectionExec>(&optimized)
            .expect("Must collect");
        assert!(
            !projections.is_empty(),
            "Plan should contain at least one projection"
        );
    }
}
