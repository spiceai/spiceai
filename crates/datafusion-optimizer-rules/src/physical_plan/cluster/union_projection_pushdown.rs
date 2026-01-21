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
use crate::concrete;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Result, exec_err};
use datafusion::config::ConfigOptions;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::union::UnionExec;
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

    fn is_safe_unary_node(node: &Arc<dyn ExecutionPlan>) -> bool {
        concrete!(node, RepartitionExec).is_some()
            || concrete!(node, CoalescePartitionsExec).is_some()
    }

    /// Walk down from `start` through only safe unary nodes until we find a node of type `T`.
    /// Returns `None` if we encounter a non-safe node before finding `T`.
    fn find_through_safe_unary<T: ExecutionPlan + 'static>(
        start: &Arc<dyn ExecutionPlan>,
    ) -> Option<Arc<dyn ExecutionPlan>> {
        let mut current = Arc::clone(start);
        loop {
            if concrete!(&current, T).is_some() {
                return Some(current);
            } else if current.children().len() == 1 && Self::is_safe_unary_node(&current) {
                current = Arc::clone(current.children()[0]);
            } else {
                // Not safe unary and not target type - cannot proceed
                return None;
            }
        }
    }

    fn find_eligible_union(projection: &ProjectionExec) -> Option<Arc<dyn ExecutionPlan>> {
        // Walk down from projection input through only safe unary nodes to find UnionExec
        let union_node = Self::find_through_safe_unary::<UnionExec>(projection.input())?;

        let union_exec = concrete!(&union_node, UnionExec)?;

        // The input schema of the projection must match the output schema of the union
        if union_exec.inputs().is_empty() || union_exec.schema() != projection.input().schema() {
            return None;
        }

        // All `UnionExec` inputs must be unary chains of safe nodes ending with `DataSourceExec`.
        // We must walk through ONLY safe unary nodes - if we hit any other node type, reject.
        for union_child in union_exec.inputs() {
            Self::find_through_safe_unary::<DataSourceExec>(union_child)?;
        }

        Some(union_node)
    }
}

impl PhysicalOptimizerRule for UnionProjectionPushdownOptimizer {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Arc::clone(&plan)
            .transform_down(|p| {
                // Only operate on `ProjectionExec`
                let Some(projection) = concrete!(p, ProjectionExec) else {
                    return Ok(Transformed::no(p));
                };

                if projection.expr().is_empty() {
                    return Ok(Transformed::no(p));
                }

                // Find an eligible union to push down to
                let Some(union_exec) = Self::find_eligible_union(projection) else {
                    return Ok(Transformed::no(p));
                };

                let mut replacements: HashMap<PlanNodeKey, Arc<dyn ExecutionPlan>> =
                    HashMap::new();
                let projection_key: PlanNodeKey = p.as_ref().into();
                let projection_expr = Arc::new(projection.expr().to_vec());
                let expected_schema = projection.input().schema();
                let expected_fields = || {
                    expected_schema
                        .fields()
                        .iter()
                        .enumerate()
                        .map(|(idx, field)| format!("{idx}:{}", field.name()))
                        .collect::<Vec<_>>()
                };
                let projection_columns = || {
                    projection_expr
                        .iter()
                        .map(|projection_expr| {
                            if let Some(col) =
                                projection_expr.expr.as_any().downcast_ref::<Column>()
                            {
                                format!(
                                    "{}:{} as {}",
                                    col.index(),
                                    col.name(),
                                    projection_expr.alias
                                )
                            } else {
                                format!("{:?} as {}", projection_expr.expr, projection_expr.alias)
                            }
                        })
                        .collect::<Vec<_>>()
                };

                // Take the projection and apply it on top of the union inputs. Specifically, on top
                // of the repartition exec emitted by `DistributeFileScanOptimizer`
                let union_children = union_exec.children().len();
                for (leaf_idx, leaf) in union_exec.children().iter().enumerate() {
                    let leaf_key: PlanNodeKey = leaf.as_ref().into();
                    // Verify the leaf schema matches the projection's input schema
                    // to ensure the column indices in projection_expr are valid.
                    // Without this check, we could get "project index N out of bounds" errors
                    // when the leaf has fewer columns than the projection expressions expect.
                    if leaf.schema() != expected_schema {
                        let expected_fields = expected_fields();
                        let projection_columns = projection_columns();
                        let leaf_fields = leaf
                            .schema()
                            .fields()
                            .iter()
                            .enumerate()
                            .map(|(idx, field)| format!("{idx}:{}", field.name()))
                            .collect::<Vec<_>>();
                        tracing::debug!(
                            expected_fields = expected_schema.fields().len(),
                            leaf_fields = leaf.schema().fields().len(),
                            projection_key = ?projection_key,
                            leaf_key = ?leaf_key,
                            leaf_index = leaf_idx,
                            union_children = union_exec.children().len(),
                            projection_columns = %projection_columns.join(","),
                            expected_field_names = %expected_fields.join(","),
                            leaf_field_names = %leaf_fields.join(","),
                            "Skipping projection pushdown: union child schema differs from projection input schema"
                        );
                        return Ok(Transformed::no(p));
                    }

                    if tracing::enabled!(tracing::Level::TRACE) {
                        let expected_fields = expected_fields();
                        let projection_columns = projection_columns();
                        let leaf_fields = leaf
                            .schema()
                            .fields()
                            .iter()
                            .enumerate()
                            .map(|(idx, field)| format!("{idx}:{}", field.name()))
                            .collect::<Vec<_>>();
                        tracing::trace!(
                            projection_key = ?projection_key,
                            leaf_key = ?leaf_key,
                            leaf_index = leaf_idx,
                            union_children = union_exec.children().len(),
                            projection_columns = %projection_columns.join(","),
                            expected_field_names = %expected_fields.join(","),
                            leaf_field_names = %leaf_fields.join(","),
                            "Union projection pushdown: projection input and leaf schema fields"
                        );
                    }

                    // Decorate the projection atop the union input
                    let projection_expr_for_exec = if leaf_idx + 1 == union_children {
                        Arc::unwrap_or_clone(Arc::clone(&projection_expr))
                    } else {
                        projection_expr.as_ref().clone()
                    };

                    // Build the rewritten leaf by inserting projection at the correct place.
                    // If the leaf starts with Repartition or Coalesce, we need to:
                    // 1. Extract the inner subtree (repartition/coalesce's child)
                    // 2. Create projection around the inner subtree
                    // 3. Wrap with a new repartition/coalesce
                    // This avoids duplicating the repartition/coalesce nodes.
                    let rewrite_leaf: Arc<dyn ExecutionPlan> =
                        if let Some(repartition) = concrete!(leaf, RepartitionExec) {
                            // Get repartition's input and wrap it with projection
                            let repartition_children = repartition.children();
                            let repartition_input = repartition_children
                                .first()
                                .ok_or_else(|| {
                                    datafusion::common::DataFusionError::Internal(
                                        "RepartitionExec has no children".to_string(),
                                    )
                                })?;
                            let inner_projection =
                                match ProjectionExec::try_new(
                                    projection_expr_for_exec,
                                    Arc::clone(repartition_input),
                                ) {
                                    Ok(proj) => Arc::new(proj),
                                    Err(err) => {
                                        tracing::debug!(
                                            error = %err,
                                            "Skipping projection pushdown: failed to build projection for repartition child"
                                        );
                                        return Ok(Transformed::no(p));
                                    }
                                };
                            Arc::new(RepartitionExec::try_new(
                                inner_projection,
                                repartition.partitioning().clone(),
                            )?)
                        } else if let Some(coalesce) = concrete!(leaf, CoalescePartitionsExec) {
                            // Get coalesce's input and wrap it with projection
                            let coalesce_children = coalesce.children();
                            let coalesce_input = coalesce_children.first().ok_or_else(|| {
                                datafusion::common::DataFusionError::Internal(
                                    "CoalescePartitionsExec has no children".to_string(),
                                )
                            })?;
                            let inner_projection =
                                match ProjectionExec::try_new(
                                    projection_expr_for_exec,
                                    Arc::clone(coalesce_input),
                                ) {
                                    Ok(proj) => Arc::new(proj),
                                    Err(err) => {
                                        tracing::debug!(
                                            error = %err,
                                            "Skipping projection pushdown: failed to build projection for coalesce child"
                                        );
                                        return Ok(Transformed::no(p));
                                    }
                                };
                            Arc::new(CoalescePartitionsExec::new(inner_projection))
                        } else {
                            // No repartition or coalesce - wrap leaf directly with projection
                            match ProjectionExec::try_new(projection_expr_for_exec, Arc::clone(leaf))
                            {
                                Ok(projection) => Arc::new(projection),
                                Err(err) => {
                                    let expected_fields = expected_fields();
                                    let projection_columns = projection_columns();
                                    let leaf_fields = leaf
                                        .schema()
                                        .fields()
                                        .iter()
                                        .enumerate()
                                        .map(|(idx, field)| format!("{idx}:{}", field.name()))
                                        .collect::<Vec<_>>();
                                    tracing::debug!(
                                        expected_fields = expected_schema.fields().len(),
                                        leaf_fields = leaf.schema().fields().len(),
                                        projection_key = ?projection_key,
                                        leaf_key = ?leaf_key,
                                        leaf_index = leaf_idx,
                                        union_children = union_exec.children().len(),
                                        projection_columns = %projection_columns.join(","),
                                        expected_field_names = %expected_fields.join(","),
                                        leaf_field_names = %leaf_fields.join(","),
                                        projection_expr = ?projection_expr.as_ref(),
                                        error = %err,
                                        "Skipping projection pushdown: failed to build projection for union child"
                                    );
                                    return Ok(Transformed::no(p));
                                }
                            }
                        };

                    replacements.insert(leaf_key, rewrite_leaf);
                }

                let mut remaining_replacements = replacements;
                let rewritten_input = Arc::clone(projection.input())
                    .transform_down(|node| {
                        if let Some(replacement) =
                            remaining_replacements.remove(&node.as_ref().into())
                        {
                            Ok(Transformed::yes(replacement))
                        } else {
                            Ok(Transformed::no(node))
                        }
                    })?
                    .data;

                if remaining_replacements.is_empty() {
                    Ok(Transformed::yes(rewritten_input))
                } else {
                    exec_err!(
                        "{}: Failed to bind all plan replacements. Report this bug on GitHub: https://github.com/spiceai/spiceai/issues",
                        self.name()
                    )
                }
            })
            .map(|transformed| transformed.data)
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

    use crate::common::search_visitor::SearchVisitor;
    use arrow::datatypes::{DataType, Field, Schema};
    use chrono::DateTime;
    use datafusion::datasource::physical_plan::ArrowSource;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::BinaryExpr;
    use datafusion::physical_expr::expressions::col;
    use datafusion::physical_expr::expressions::lit;
    use datafusion::physical_optimizer::optimizer::PhysicalOptimizer;
    use datafusion::physical_plan::filter::FilterExec;
    use datafusion::physical_plan::projection::ProjectionExpr;
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

    #[test]
    fn test_projection_pushdown() {
        let files_1 = vec![create_partitioned_file(
            "file:///file4.parquet",
            256_000_000,
            None,
        )];
        let files_2 = vec![create_partitioned_file(
            "file:///file5.parquet",
            256_000_000,
            None,
        )];

        let data_source_1 = create_data_source_exec(files_1);
        let data_source_2 = create_data_source_exec(files_2);
        let union_exec: Arc<dyn ExecutionPlan> =
            UnionExec::try_new(vec![data_source_1, data_source_2]).expect("create union");
        let projection_exec = ProjectionExec::try_new(
            vec![(
                col("id", union_exec.schema().as_ref()).expect("Must bind expr"),
                "foo".to_string(),
            )],
            union_exec,
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

    /// Ensures we do not push projections past filters that require columns
    /// not present in the projection.
    #[test]
    fn test_projection_pushdown_skips_filter_chain() {
        let files_1 = vec![create_partitioned_file(
            "file:///file4.parquet",
            256_000_000,
            None,
        )];
        let files_2 = vec![create_partitioned_file(
            "file:///file5.parquet",
            256_000_000,
            None,
        )];

        let data_source_1 = create_data_source_exec(files_1);
        let data_source_2 = create_data_source_exec(files_2);
        let union_exec: Arc<dyn ExecutionPlan> =
            UnionExec::try_new(vec![data_source_1, data_source_2]).expect("create union");

        let filter_expr = Arc::new(BinaryExpr::new(
            col("id", union_exec.schema().as_ref()).expect("Must bind expr"),
            Operator::Gt,
            lit(1i64),
        ));
        let filter_exec = Arc::new(FilterExec::try_new(filter_expr, union_exec).expect("filter"));

        let projection_exec = ProjectionExec::try_new(
            vec![(
                col("name", filter_exec.schema().as_ref()).expect("Must bind expr"),
                "name".to_string(),
            )],
            filter_exec,
        )
        .expect("Must make projection_exec");
        let plan: Arc<dyn ExecutionPlan> = Arc::new(projection_exec);

        let optimized = optimize(&plan);

        let projection_count = SearchVisitor::collect_concrete_down::<ProjectionExec>(&optimized)
            .expect("Must collect")
            .len();
        assert_eq!(projection_count, 1);
    }

    #[test]
    fn test_projection_pushdown_skips_empty_projection() {
        let files_1 = vec![create_partitioned_file(
            "file:///file4.parquet",
            256_000_000,
            None,
        )];
        let files_2 = vec![create_partitioned_file(
            "file:///file5.parquet",
            256_000_000,
            None,
        )];

        let data_source_1 = create_data_source_exec(files_1);
        let data_source_2 = create_data_source_exec(files_2);
        let union_exec: Arc<dyn ExecutionPlan> =
            UnionExec::try_new(vec![data_source_1, data_source_2]).expect("create union");

        let projection_exec = ProjectionExec::try_new(Vec::<ProjectionExpr>::new(), union_exec)
            .expect("Must make projection_exec");
        let plan: Arc<dyn ExecutionPlan> = Arc::new(projection_exec);

        let optimized = optimize(&plan);

        let projection_count = SearchVisitor::collect_concrete_down::<ProjectionExec>(&optimized)
            .expect("Must collect")
            .len();
        assert_eq!(projection_count, 1);
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
            UnionExec::try_new(vec![data_source1, data_source2]).expect("create union");

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

    #[test]
    fn test_projection_pushdown_parent_projection_rebuild_schema_mismatch() {
        use datafusion::physical_plan::union::UnionExec;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let data_source1 = {
            let fsc = FileScanConfigBuilder::new(
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
            DataSourceExec::from_data_source(fsc)
        };

        let data_source2 = {
            let fsc = FileScanConfigBuilder::new(
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
            DataSourceExec::from_data_source(fsc)
        };

        let union_exec: Arc<dyn ExecutionPlan> =
            UnionExec::try_new(vec![data_source1, data_source2]).expect("create union");

        // Child projection outputs 3 columns from a 2-column input schema.
        let child_projection = ProjectionExec::try_new(
            vec![
                (
                    col("id", union_exec.schema().as_ref()).expect("Must bind expr"),
                    "id".to_string(),
                ),
                (
                    col("id", union_exec.schema().as_ref()).expect("Must bind expr"),
                    "id_dup".to_string(),
                ),
                (
                    col("name", union_exec.schema().as_ref()).expect("Must bind expr"),
                    "name".to_string(),
                ),
            ],
            union_exec,
        )
        .expect("Must make child projection");

        // Parent projection refers to the third output column (index 2).
        let parent_projection = ProjectionExec::try_new(
            vec![(
                col("name", child_projection.schema().as_ref()).expect("Must bind expr"),
                "name".to_string(),
            )],
            Arc::new(child_projection),
        )
        .expect("Must make parent projection");

        let result =
            OPTIMIZER.rules[0].optimize(Arc::new(parent_projection), &ConfigOptions::default());

        assert!(
            result.is_ok(),
            "Optimization should avoid rebuilding ancestors against an intermediate schema"
        );
    }

    /// Regression test for Issue #2: When a union child has both `RepartitionExec` and
    /// `CoalescePartitionsExec` (e.g., Repartition -> Coalesce -> `DataSource`), the optimizer
    /// must preserve all intermediate nodes, not just the topmost one found.
    ///
    /// This test creates a structure:
    /// Projection -> Union -> [Repartition -> Coalesce -> `DataSource`, Repartition -> Coalesce -> `DataSource`]
    ///
    /// After optimization, the `CoalescePartitionsExec` nodes must still be present in the plan.
    #[test]
    fn test_projection_pushdown_preserves_nested_repartition_coalesce() {
        use datafusion::physical_plan::Partitioning;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        // Create two branches: Repartition -> Coalesce -> DataSource
        let data_source1 = {
            let fsc = FileScanConfigBuilder::new(
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
            DataSourceExec::from_data_source(fsc)
        };
        let coalesce1: Arc<dyn ExecutionPlan> = Arc::new(CoalescePartitionsExec::new(data_source1));
        let repartition1: Arc<dyn ExecutionPlan> = Arc::new(
            RepartitionExec::try_new(coalesce1, Partitioning::RoundRobinBatch(4))
                .expect("Must create repartition"),
        );

        let data_source2 = {
            let fsc = FileScanConfigBuilder::new(
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
            DataSourceExec::from_data_source(fsc)
        };
        let coalesce2: Arc<dyn ExecutionPlan> = Arc::new(CoalescePartitionsExec::new(data_source2));
        let repartition2: Arc<dyn ExecutionPlan> = Arc::new(
            RepartitionExec::try_new(coalesce2, Partitioning::RoundRobinBatch(4))
                .expect("Must create repartition"),
        );

        let union_exec: Arc<dyn ExecutionPlan> =
            UnionExec::try_new(vec![repartition1, repartition2]).expect("create union");

        let projection_exec = ProjectionExec::try_new(
            vec![(
                col("id", union_exec.schema().as_ref()).expect("Must bind expr"),
                "id".to_string(),
            )],
            union_exec,
        )
        .expect("Must make projection");

        let plan: Arc<dyn ExecutionPlan> = Arc::new(projection_exec);

        // Count CoalescePartitionsExec before optimization
        let coalesce_count_before =
            SearchVisitor::collect_concrete_down::<CoalescePartitionsExec>(&plan)
                .expect("Must collect")
                .len();
        assert_eq!(
            coalesce_count_before, 2,
            "Should have 2 CoalescePartitionsExec before optimization"
        );

        let optimized = optimize(&plan);

        // Verify CoalescePartitionsExec nodes are preserved after optimization
        let coalesce_count_after =
            SearchVisitor::collect_concrete_down::<CoalescePartitionsExec>(&optimized)
                .expect("Must collect")
                .len();
        assert_eq!(
            coalesce_count_after, 2,
            "CoalescePartitionsExec nodes must be preserved after projection pushdown. \
             The optimizer should not discard intermediate nodes when pushing projections down."
        );

        // Also verify the projections were pushed down
        let projection_count = SearchVisitor::collect_concrete_down::<ProjectionExec>(&optimized)
            .expect("Must collect")
            .len();
        assert_eq!(
            projection_count, 2,
            "Projections should be pushed down to each branch"
        );
    }

    /// Regression test for Issue #5: The optimizer's `find_eligible_union` validates that
    /// union children end with `DataSourceExec`, but the replacement logic may incorrectly
    /// discard intermediate nodes that are not `RepartitionExec` or `CoalescePartitionsExec`.
    ///
    /// This test should NOT apply the optimization because there's a `FilterExec` in the chain,
    /// which is not in the "safe unary nodes" list. The test verifies the optimizer correctly
    /// skips this case rather than incorrectly applying the optimization and discarding nodes.
    #[test]
    fn test_projection_pushdown_skips_unsafe_intermediate_nodes() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        // Create: DataSource -> Filter -> Repartition (child of union)
        // The filter is NOT in the safe unary nodes list
        let data_source1 = {
            let fsc = FileScanConfigBuilder::new(
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
            DataSourceExec::from_data_source(fsc)
        };

        // Filter that uses the id column
        let filter_expr1 = Arc::new(BinaryExpr::new(
            col("id", data_source1.schema().as_ref()).expect("Must bind expr"),
            Operator::Gt,
            lit(10i64),
        ));
        let filter1: Arc<dyn ExecutionPlan> =
            Arc::new(FilterExec::try_new(filter_expr1, data_source1).expect("Must create filter"));

        let data_source2 = {
            let fsc = FileScanConfigBuilder::new(
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
            DataSourceExec::from_data_source(fsc)
        };

        let filter_expr2 = Arc::new(BinaryExpr::new(
            col("id", data_source2.schema().as_ref()).expect("Must bind expr"),
            Operator::Gt,
            lit(10i64),
        ));
        let filter2: Arc<dyn ExecutionPlan> =
            Arc::new(FilterExec::try_new(filter_expr2, data_source2).expect("Must create filter"));

        let union_exec: Arc<dyn ExecutionPlan> =
            UnionExec::try_new(vec![filter1, filter2]).expect("create union");

        let projection_exec = ProjectionExec::try_new(
            vec![(
                col("id", union_exec.schema().as_ref()).expect("Must bind expr"),
                "id".to_string(),
            )],
            union_exec,
        )
        .expect("Must make projection");

        let plan: Arc<dyn ExecutionPlan> = Arc::new(projection_exec);

        // Count filters before optimization
        let filter_count_before = SearchVisitor::collect_concrete_down::<FilterExec>(&plan)
            .expect("Must collect")
            .len();
        assert_eq!(
            filter_count_before, 2,
            "Should have 2 FilterExec before optimization"
        );

        let optimized = optimize(&plan);

        // Verify FilterExec nodes are preserved - the optimizer should NOT apply
        // the optimization since FilterExec is not a safe unary node
        let filter_count_after = SearchVisitor::collect_concrete_down::<FilterExec>(&optimized)
            .expect("Must collect")
            .len();
        assert_eq!(
            filter_count_after, 2,
            "FilterExec nodes must be preserved. The optimizer should skip pushdown \
             when union children contain unsafe intermediate nodes like FilterExec."
        );

        // The projection should NOT be pushed down (should remain as 1)
        let projection_count = SearchVisitor::collect_concrete_down::<ProjectionExec>(&optimized)
            .expect("Must collect")
            .len();
        assert_eq!(
            projection_count, 1,
            "Projection should NOT be pushed down when union children contain unsafe nodes"
        );
    }

    /// Regression test for Issue #6: The `transform_down` replacement should use
    /// `Transformed::complete()` to prevent traversal into replacement subtrees.
    ///
    /// This test creates a scenario where replacement nodes could potentially
    /// match keys in `remaining_replacements` if traversal continues into them.
    /// While currently the keys are unique per-leaf, using `complete()` is more robust.
    ///
    /// This test validates that after optimization, the plan structure is correct
    /// and no unintended replacements occur within the newly inserted subtrees.
    #[test]
    fn test_projection_pushdown_replacement_stops_traversal() {
        use datafusion::physical_plan::Partitioning;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        // Create a more complex structure with nested repartitions
        let data_source1 = {
            let fsc = FileScanConfigBuilder::new(
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
            DataSourceExec::from_data_source(fsc)
        };
        let repartition1: Arc<dyn ExecutionPlan> = Arc::new(
            RepartitionExec::try_new(data_source1, Partitioning::RoundRobinBatch(4))
                .expect("Must create repartition"),
        );

        let data_source2 = {
            let fsc = FileScanConfigBuilder::new(
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
            DataSourceExec::from_data_source(fsc)
        };
        let repartition2: Arc<dyn ExecutionPlan> = Arc::new(
            RepartitionExec::try_new(data_source2, Partitioning::RoundRobinBatch(4))
                .expect("Must create repartition"),
        );

        let union_exec: Arc<dyn ExecutionPlan> =
            UnionExec::try_new(vec![repartition1, repartition2]).expect("create union");

        let projection_exec = ProjectionExec::try_new(
            vec![(
                col("id", union_exec.schema().as_ref()).expect("Must bind expr"),
                "id".to_string(),
            )],
            union_exec,
        )
        .expect("Must make projection");

        let plan: Arc<dyn ExecutionPlan> = Arc::new(projection_exec);

        let result = OPTIMIZER.rules[0].optimize(Arc::clone(&plan), &ConfigOptions::default());
        assert!(result.is_ok(), "Optimization should succeed");

        let optimized = result.expect("Must optimize");

        // Verify the structure is correct: projections should be inside repartitions
        let repartition_count = SearchVisitor::collect_concrete_down::<RepartitionExec>(&optimized)
            .expect("Must collect")
            .len();
        assert_eq!(repartition_count, 2, "Should have 2 RepartitionExec nodes");

        let projection_count = SearchVisitor::collect_concrete_down::<ProjectionExec>(&optimized)
            .expect("Must collect")
            .len();
        assert_eq!(
            projection_count, 2,
            "Should have 2 ProjectionExec nodes pushed down"
        );

        // Verify projections are children of repartitions (proper structure)
        let repartition_nodes = SearchVisitor::collect_concrete_down::<RepartitionExec>(&optimized)
            .expect("Must collect");
        for repartition in repartition_nodes {
            let children = repartition.children();
            assert_eq!(children.len(), 1, "Repartition should have one child");
            assert!(
                concrete!(children[0], ProjectionExec).is_some(),
                "Repartition's child should be ProjectionExec after optimization"
            );
        }
    }
}
