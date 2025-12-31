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
use crate::concrete;
use crate::config::cluster_config::SpiceClusterConfig;
use datafusion::common::stats::Precision;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{DataFusionError, Statistics};
use datafusion::common::{Result, exec_err};
use datafusion::config::ConfigOptions;
use datafusion::physical_expr::Partitioning;
use datafusion::physical_expr::expressions::col;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
use datafusion_datasource::source::{DataSource, DataSourceExec};
use datafusion_datasource::{PartitionedFile, compute_all_files_statistics};
use itertools::Itertools;
use std::cmp::max;
use std::sync::Arc;

/// This takes one large logical `FileScanConfig` and breaks up its file groups into
/// individual scans that are then UNION'd together at the top. Each of the new scans
/// has a `CoalescePartitionsExec` at the top of its plan to signal to the Ballista
/// distributed planner that it can break the plan into a new stage at that point in time.
///
/// The vanilla distributed plan looks like this. This would run as single task on a single node:
/// ```text
/// DataSourceExec: file_groups={20 groups ...]}, file_type=parquet
/// ```
///
/// The new distributed plan, that runs as many tasks, across several nodes:
/// ```text
/// =========ResolvedStage[stage_id=1.0, partitions=1]=========
/// ShuffleWriterExec: partitioning:None
///   EnsureRuntimeDependencyExec: RuntimeDependencySpec {...}
///     DataSourceExec: file_groups={1 group: [[wiki_a.parquet:0..43660370]]}, file_type=parquet
///
/// =========ResolvedStage[stage_id=2.0, partitions=1]=========
/// ShuffleWriterExec: partitioning:None
///   EnsureRuntimeDependencyExec: RuntimeDependencySpec {...}
///     DataSourceExec: file_groups={1 group: [[wiki_a.parquet:43660370..87320740]]}, file_type=parquet
/// ```
///
/// If a `DataSourceExec` has a limit pushed down, then it is not split, but may be repartitioned
/// for projections above it.
#[derive(Debug)]
pub struct DistributeFileScanOptimizer {}

impl DistributeFileScanOptimizer {
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(DistributeFileScanOptimizer {})
    }
}

impl DistributeFileScanOptimizer {
    /// Bytes read is either the whole file or some part of it
    // `cast_sign_loss` is OK because file offsets are always non-negative
    #[expect(clippy::cast_sign_loss)]
    fn read_size(pf: &PartitionedFile) -> u64 {
        if let Some(range) = pf.range.as_ref() {
            (range.end - range.start) as u64
        } else {
            pf.object_meta.size
        }
    }

    /// Emit file groups that are constrained by the target byte size
    fn groups_by_byte_size(
        files: impl IntoIterator<Item = PartitionedFile>,
        partition_byte_size: u64,
    ) -> Vec<FileGroup> {
        let mut groups: Vec<Vec<PartitionedFile>> = vec![vec![]];
        let mut current_group_size: u64 = 0;
        files.into_iter().for_each(|f| {
            let size = Self::read_size(&f);

            if (current_group_size + size) >= partition_byte_size {
                groups.push(vec![]);
                current_group_size = 0;
            }

            current_group_size += size;

            let Some(last_group) = groups.last_mut() else {
                unreachable!("There must be at least one group")
            };

            last_group.push(f);
        });
        groups
            .into_iter()
            .filter_map(|files| {
                if files.is_empty() {
                    None
                } else {
                    Some(FileGroup::new(files))
                }
            })
            .collect()
    }

    /// Group `FileGroup`s into stages based on task level parallelism and optional user-defined
    /// max stages configuration
    fn groups_to_stages(
        groups: Vec<FileGroup>,
        task_partitions: usize,
        max_stages: Option<usize>,
    ) -> Vec<Vec<FileGroup>> {
        let stage_size = task_partitions * 2;
        let max_stages = max_stages.unwrap_or(usize::MAX);

        let stage_size = if (groups.len() / stage_size) > max_stages {
            groups.len() / max_stages
        } else if (groups.len() / stage_size) < 2 {
            max(groups.len() / 2, 1)
        } else {
            stage_size
        };

        groups
            .into_iter()
            .chunks(stage_size)
            .into_iter()
            .map(Iterator::collect)
            .collect()
    }

    /// Repartitions a single `FileScanConfig` into many file groups. Bails out if the read is
    /// smaller than the configured `file_group_size_bytes`
    fn scan_to_stages(
        file_scan_config: &FileScanConfig,
        config: &ConfigOptions,
    ) -> Result<Option<Vec<Vec<FileGroup>>>> {
        let Some(spice_config) = config.extensions.get::<SpiceClusterConfig>() else {
            return exec_err!(
                "SpiceClusterConfig not bound. Did you forget `.with_option_extension(Arc::new(SpiceClusterConfig::default()))`?"
            );
        };

        let file_group_byte_size = spice_config.execution.file_group_size_bytes;
        let task_partitions = config.execution.target_partitions;

        // Get all the partitioned files
        let partitioned_files = file_scan_config
            .file_groups
            .iter()
            .flat_map(|fg| fg.iter().cloned())
            .map(|mut pf| {
                let mut stats = Statistics::new_unknown(file_scan_config.file_schema.as_ref());

                // We can deduce the byte size from the read range
                stats.total_byte_size =
                    Precision::Exact(usize::try_from(Self::read_size(&pf)).map_err(|_| {
                        DataFusionError::Execution("Cannot cast usize".to_string())
                    })?);
                pf.statistics = Some(Arc::new(stats));
                Ok(pf)
            })
            .collect::<Result<Vec<_>>>()?;

        let read_size: u64 = partitioned_files.iter().map(Self::read_size).sum();

        // If less than our byte size bucket, skip that step and let `groups_to_stages` distribute
        let file_groups = if read_size <= file_group_byte_size {
            vec![FileGroup::new(partitioned_files)]
        } else {
            Self::groups_by_byte_size(partitioned_files, file_group_byte_size)
        };

        let stages = Self::groups_to_stages(
            file_groups,
            task_partitions,
            spice_config.execution.file_scan_expand_max_stages,
        );

        // `UnionExec` requires at least 2 inputs, some reads are too small
        if stages.len() < 2 {
            return Ok(None);
        }

        Ok(Some(stages))
    }

    ///  Ballista's `DistributedPlanner` only makes stages if it can detect these nodes in a plan:
    /// - `CoalescePartitionsExec`
    /// - `RepartitionExec` (hash only)
    /// - `SortPreservingMergeExec`
    ///
    /// This tries to:
    /// - Preserve input partitioning if compatible (hash partitioning)
    ///   - In the case that there are fewer partitions than task parallelism, shard to task parallelism
    /// - Repartition based on a 'best guess' column value
    /// - `CoalescePartitionsExec` if cannot do the above (slow)
    fn with_stage_repartition(
        exec: Arc<dyn ExecutionPlan>,
        task_partitions: usize,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let schema = exec.schema();

        // Try to guess a partitioning column
        let partition_column = schema
            .fields
            .iter()
            .find(|f| {
                let name = f.name().to_lowercase();
                name == "id" || name == "key" || name.ends_with("_id")
            })
            .or_else(|| schema.fields.first());

        let partitioning = match (exec.output_partitioning(), partition_column) {
            // Preserve input partitioning if compatible with stage split
            (Partitioning::Hash(exprs, count), _) => {
                Partitioning::Hash(exprs.clone(), max(task_partitions, *count))
            }
            // Try to guess alternate partitioning criteria
            (_, Some(partition_column)) => Partitioning::Hash(
                vec![col(partition_column.name(), schema.as_ref())?],
                task_partitions,
            ),
            // Fallback
            _ => Partitioning::RoundRobinBatch(task_partitions),
        };

        let new_exec: Arc<dyn ExecutionPlan> = match partitioning {
            hash @ Partitioning::Hash(..) => Arc::new(RepartitionExec::try_new(exec, hash)?),
            _ => Arc::new(CoalescePartitionsExec::new(exec)),
        };

        Ok(new_exec)
    }

    fn stage_to_new_file_scan(
        original_file_scan: &FileScanConfig,
        stage: Vec<FileGroup>,
        task_partitions: usize,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let (stage_with_stats, agg_stats) = compute_all_files_statistics(
            stage,
            Arc::clone(&original_file_scan.file_schema),
            true,
            true,
        )?;

        // Copy all existing attributes including projection, excluding file groups as they are potentially
        // expensive to clone for large scans
        let new_scan = FileScanConfigBuilder::new(
            original_file_scan.object_store_url.clone(),
            Arc::clone(&original_file_scan.file_schema),
            Arc::clone(&original_file_scan.file_source),
        )
        .with_batch_size(original_file_scan.batch_size)
        .with_constraints(original_file_scan.constraints.clone())
        .with_expr_adapter(original_file_scan.expr_adapter_factory.clone())
        .with_file_compression_type(original_file_scan.file_compression_type)
        .with_file_groups(stage_with_stats)
        .with_limit(original_file_scan.limit)
        .with_metadata_cols(original_file_scan.metadata_cols.clone())
        .with_object_versioning_type(original_file_scan.object_versioning_type.clone())
        .with_output_ordering(original_file_scan.output_ordering.clone())
        .with_projection(original_file_scan.projection.clone())
        .with_statistics(agg_stats)
        .with_table_partition_cols(
            original_file_scan
                .table_partition_cols
                .iter()
                .map(|field| field.as_ref().clone())
                .collect(),
        )
        .build();

        // Propagate source partitioning
        let new_scan_partitioning = new_scan.output_partitioning();
        let new_data_source_exec =
            DataSourceExec::new(Arc::new(new_scan)).with_partitioning(new_scan_partitioning);

        Self::with_stage_repartition(Arc::new(new_data_source_exec), task_partitions)
    }
}

impl PhysicalOptimizerRule for DistributeFileScanOptimizer {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let Some(spice_config) = config.extensions.get::<SpiceClusterConfig>() else {
            return exec_err!(
                "SpiceClusterConfig not bound. Did you forget `.with_option_extension(Arc::new(SpiceClusterConfig::default()))`?"
            );
        };

        let transformed = plan.transform_up(|p| {
            let maybe_file_scan = concrete!(p, DataSourceExec)
                .and_then(|d| concrete!(d.data_source(), FileScanConfig));

            let Some(file_scan_config) = maybe_file_scan else {
                return Ok(Transformed::no(p));
            };

            // Only repartition sufficiently large LIMIT scans
            // TODO: statistics + check upstream projections for transforms
            match file_scan_config.limit {
                Some(limit)
                    if limit as u64 >= spice_config.execution.file_scan_min_repartition_limit =>
                {
                    return Ok(Transformed::yes(Self::with_stage_repartition(
                        p,
                        config.execution.target_partitions,
                    )?));
                }
                Some(_) => return Ok(Transformed::no(p)),
                None => {}
            }

            let Some(new_stages) = Self::scan_to_stages(file_scan_config, config)? else {
                return Ok(Transformed::no(p));
            };

            let exploded_scans = new_stages
                .into_iter()
                .map(|stage| {
                    Self::stage_to_new_file_scan(
                        file_scan_config,
                        stage,
                        config.execution.target_partitions,
                    )
                })
                .collect::<Result<Vec<_>>>()?;

            Ok(Transformed::yes(Arc::new(UnionExec::new(exploded_scans))))
        })?;

        Ok(transformed.data)
    }

    fn name(&self) -> &'static str {
        "DistributeFileScanOptimizer"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use chrono::DateTime;
    use datafusion::datasource::physical_plan::ArrowSource;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion_datasource::FileRange;
    use datafusion_optimizer_rules::common::{
        plan_node_key::PlanNodeKey, search_visitor::SearchVisitor,
    };
    use object_store::ObjectMeta;
    use object_store::path::Path;
    use std::sync::LazyLock;

    pub static DEFAULT_CONFIG_OPTIONS: LazyLock<ConfigOptions> = LazyLock::new(|| {
        let mut config = ConfigOptions::default();
        config.extensions.insert(SpiceClusterConfig::default());
        config
    });

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

    #[test]
    fn test_respects_byte_size_setting() {
        let optimizer = DistributeFileScanOptimizer::new();

        // Default = 128MB per filegroup
        let files = vec![
            // Group 1
            create_partitioned_file("file:///file1.parquet", 50_000_000, None),
            create_partitioned_file("file:///file2.parquet", 50_000_000, None),
            // Group 2 (trips `file_group_size_bytes`, is new group)
            create_partitioned_file("file:///file3.parquet", 28_000_001, None),
            // Group 3 (too big, new group)
            create_partitioned_file("file:///file4.parquet", 256_000_000, None),
            // Group 4 (too big, new group)
            create_partitioned_file("file:///file5.parquet", 256_000_000, None),
        ];

        let plan = create_data_source_exec(files);

        let optimized_plan = optimizer
            .optimize(plan, &DEFAULT_CONFIG_OPTIONS)
            .expect("Must optimize");

        let file_groups_after_optimize =
            SearchVisitor::collect_concrete_down::<DataSourceExec>(&optimized_plan)
                .expect("Must search plan")
                .into_iter()
                .filter_map(|d| {
                    concrete!(d, DataSourceExec)
                        .and_then(|ds| ds.data_source().as_any().downcast_ref::<FileScanConfig>())
                        .map(|fsc| fsc.file_groups.clone())
                })
                .flatten()
                .collect::<Vec<_>>();

        // There should be 4 groups with 5 total files
        assert_eq!(file_groups_after_optimize.len(), 4);
        assert_eq!(
            file_groups_after_optimize
                .into_iter()
                .map(|g| g.len())
                .sum::<usize>(),
            5
        );
    }

    #[test]
    fn test_respects_max_stages() {
        let optimizer = DistributeFileScanOptimizer::new();

        let files = (0..10000)
            .map(|i| {
                create_partitioned_file(format!("file:///{i}.parquet").as_str(), 128_000_000, None)
            })
            .collect::<Vec<_>>();

        let plan = create_data_source_exec(files);

        let mut config_with_max_stages = DEFAULT_CONFIG_OPTIONS.clone();
        // Set target_partitions to ensure deterministic test behavior
        // With target_partitions=25, stage_size=50, we get exactly 200 stages from 10000 files
        config_with_max_stages
            .set("datafusion.execution.target_partitions", "25")
            .expect("Must set target_partitions");
        config_with_max_stages
            .set("spice.execution.file_scan_expand_max_stages", "200")
            .expect("Must set config");

        let optimized_plan = optimizer
            .optimize(plan, &config_with_max_stages)
            .expect("Must optimize");

        let data_source_execs =
            SearchVisitor::collect_concrete_down::<DataSourceExec>(&optimized_plan)
                .expect("Must search plan");

        // There should be 200 max DataSourceExec stages
        assert_eq!(data_source_execs.len(), 200);
    }

    #[test]
    fn test_small_read_bail_out() {
        let optimizer = DistributeFileScanOptimizer::new();

        let plan = create_data_source_exec(vec![create_partitioned_file(
            "file:///file1.parquet",
            5_000_000,
            None,
        )]);
        let plan_key: PlanNodeKey = plan.as_ref().into();

        let optimized_plan = optimizer
            .optimize(plan, &DEFAULT_CONFIG_OPTIONS)
            .expect("Must optimize");
        let optimized_plan_key: PlanNodeKey = optimized_plan.as_ref().into();

        assert_eq!(plan_key, optimized_plan_key);
    }

    #[test]
    fn test_statistics_recomputed_correctly() {
        let optimizer = DistributeFileScanOptimizer::new();

        // This scan will get split into two (>128M)
        let files = vec![
            create_partitioned_file("file:///file1.parquet", 256_000_000, None),
            create_partitioned_file("file:///file2.parquet", 256_000_000, None),
        ];

        let plan = create_data_source_exec(files);

        let optimized_plan = optimizer
            .optimize(plan, &DEFAULT_CONFIG_OPTIONS)
            .expect("Must optimize");

        let data_source_execs =
            SearchVisitor::collect_concrete_down::<DataSourceExec>(&optimized_plan)
                .expect("Must search plan");

        assert_eq!(
            data_source_execs.len(),
            2,
            "Must have two DataSourceExec nodes after rewrite"
        );

        for exec in &data_source_execs {
            if let Some(file_scan) = concrete!(exec, DataSourceExec)
                .and_then(|ds| ds.data_source().as_any().downcast_ref::<FileScanConfig>())
            {
                let stats = file_scan
                    .file_source
                    .statistics()
                    .expect("Must have statistics");
                assert_eq!(
                    stats.total_byte_size.get_value(),
                    Some(256_000_000_usize).as_ref()
                );
            }
        }
    }

    pub mod cluster {
        use super::*;

        use datafusion::physical_expr::expressions::col;
        use datafusion::physical_optimizer::optimizer::PhysicalOptimizer;
        use datafusion::physical_plan::ExecutionPlan;
        use datafusion::physical_plan::projection::ProjectionExec;
        use datafusion_datasource::source::DataSourceExec;
        use datafusion_optimizer_rules::common::search_visitor::SearchVisitor;
        use datafusion_optimizer_rules::physical_plan::cluster::{
            ensure_supported_file_scan::EnsureSupportedFileScan,
            union_projection_pushdown::UnionProjectionPushdownOptimizer,
        };

        use std::sync::{Arc, LazyLock};

        use crate::optimizer_rule::distribute_file_scan::DistributeFileScanOptimizer;
        use crate::optimizer_rule::distribute_file_scan::tests::DEFAULT_CONFIG_OPTIONS;

        static OPTIMIZER: LazyLock<PhysicalOptimizer> = LazyLock::new(|| {
            let mut rules = PhysicalOptimizer::new().rules;
            rules.extend([
                EnsureSupportedFileScan::new(),
                DistributeFileScanOptimizer::new(),
                UnionProjectionPushdownOptimizer::new(),
            ]
                as [Arc<dyn datafusion::physical_optimizer::PhysicalOptimizerRule + Send + Sync>;
                    3]);
            PhysicalOptimizer::with_rules(rules)
        });

        fn optimize(plan: &Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
            OPTIMIZER.rules.iter().fold(Arc::clone(plan), |acc, rule| {
                rule.optimize(acc, &DEFAULT_CONFIG_OPTIONS)
                    .expect("Must optimize")
            })
        }

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
    }
}
