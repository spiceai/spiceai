use crate::concrete;
use crate::datafusion::DataFusion;
use crate::datafusion::cluster::common::datafusion_scheduler_ext::DataFusionSchedulerExtensions;
use crate::datafusion::cluster::config::SpiceClusterConfig;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Result, exec_err};
use datafusion::config::ConfigOptions;
use datafusion::physical_expr::Partitioning;
use datafusion::physical_expr::expressions::col;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion_datasource::PartitionedFile;
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
use datafusion_datasource::source::DataSourceExec;
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
/// Limits are handled by the default physical pushdown mechanism and are currently
/// replicated per scan.
#[derive(Debug)]
pub struct ExpandFileScanOptimizer {
    df: Arc<DataFusion>,
}

impl ExpandFileScanOptimizer {
    #[must_use]
    pub fn new(df: Arc<DataFusion>) -> Arc<Self> {
        Arc::new(ExpandFileScanOptimizer { df })
    }
}

impl ExpandFileScanOptimizer {
    fn read_size(pf: &PartitionedFile) -> u64 {
        if let Some(range) = pf.range.as_ref() {
            (range.end - range.start) as u64
        } else {
            pf.object_meta.size
        }
    }

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
            groups.last_mut().expect("Must get current group").push(f);
        });
        groups.into_iter().map(FileGroup::new).collect()
    }

    fn groups_to_stages(
        &self,
        groups: Vec<FileGroup>,
        desired_stages: usize,
        task_parallelism: usize,
    ) -> Result<Vec<Vec<FileGroup>>> {
        // Large reads: bucket into desired shuffle count
        let stage_size: usize = if groups.len() > desired_stages {
            let at_desired_stages = groups.len() / desired_stages;
            max(at_desired_stages, task_parallelism)
        }
        // Smaller reads: split work up amongst executors
        else {
            groups.len() / max(self.df.executors()?.len(), 2)
        };

        Ok(groups
            .into_iter()
            .chunks(stage_size)
            .into_iter()
            .map(Iterator::collect)
            .collect())
    }

    fn scan_to_stages(
        &self,
        file_scan_config: &FileScanConfig,
        config: &ConfigOptions,
    ) -> Result<Option<Vec<Vec<FileGroup>>>> {
        let Some(spice_config) = config.extensions.get::<SpiceClusterConfig>() else {
            return exec_err!(
                "SpiceClusterConfig not bound. Did you forget `.with_option_extension(Arc::new(SpiceClusterConfig::default()))`?"
            );
        };

        let file_group_byte_size = spice_config.execution.file_group_size_bytes;
        let desired_stages = spice_config.execution.file_scan_expand_stages;

        let task_partitions = config.execution.target_partitions;
        let partition_byte_size = file_group_byte_size / task_partitions as u64;

        // Get all the partitioned files
        let partitioned_files = file_scan_config
            .file_groups
            .iter()
            .flat_map(|fg| fg.clone().into_inner())
            .collect::<Vec<_>>();

        let read_size: u64 = partitioned_files.iter().map(Self::read_size).sum();
        let core_count = self.df.executors()?.len() * task_partitions;

        // If we don't hit the byte size trigger, splat the task out to all cores
        let file_groups = if read_size <= file_group_byte_size {
            let group_size = max(partitioned_files.len() / core_count, 1);
            partitioned_files
                .into_iter()
                .chunks(group_size)
                .into_iter()
                .map(|g| FileGroup::new(g.collect()))
                .collect()
        } else {
            Self::groups_by_byte_size(partitioned_files, partition_byte_size)
        };

        let stages = self.groups_to_stages(file_groups, desired_stages, task_partitions)?;

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
    /// So we insert the best kind of stage for the given input schema
    fn with_stage_repartition(
        exec: Arc<dyn ExecutionPlan>,
        task_partitions: usize,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let schema = exec.schema();
        let partition_column = schema
            .fields
            .iter()
            .filter(|f| {
                let name = f.name().to_lowercase();
                name == "id" || name == "key" || name.ends_with("_id")
            })
            .next()
            .or_else(|| schema.fields.first());

        // TODO check underlying partitioning
        // TODO directly emit unresolved shuffle?
        let partitioning: Arc<dyn ExecutionPlan> = if let Some(partition_column) = partition_column
        {
            let re = RepartitionExec::try_new(
                exec,
                Partitioning::Hash(
                    vec![col(partition_column.name(), schema.as_ref())?],
                    task_partitions,
                ),
            )?;
            Arc::new(re)
        } else {
            Arc::new(CoalescePartitionsExec::new(exec))
        };

        Ok(partitioning)
    }
}

impl PhysicalOptimizerRule for ExpandFileScanOptimizer {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let transformed = plan.transform_up(|plan| {
            let maybe_file_scan = concrete!(plan, DataSourceExec)
                .and_then(|d| concrete!(d.data_source(), FileScanConfig));

            let Some(file_scan_config) = maybe_file_scan else {
                return Ok(Transformed::no(plan));
            };

            let Some(new_stages) = self.scan_to_stages(file_scan_config, config)? else {
                return Ok(Transformed::no(plan));
            };

            let exploded_scans = new_stages
                .into_iter()
                .map(|stage| {
                    // Copy all existing attributes including projection, but override file groups
                    let new_scan = FileScanConfigBuilder::from(file_scan_config.clone())
                        .with_file_groups(stage)
                        .build();
                    let new_data_source_exec: Arc<dyn ExecutionPlan> =
                        DataSourceExec::from_data_source(new_scan);

                    Self::with_stage_repartition(
                        new_data_source_exec,
                        config.execution.target_partitions,
                    )
                })
                .collect::<Result<Vec<_>>>()?;

            Ok(Transformed::yes(Arc::new(UnionExec::new(exploded_scans))))
        })?;

        Ok(transformed.data)
    }

    fn name(&self) -> &'static str {
        "ExpandFileScanOptimizer"
    }

    fn schema_check(&self) -> bool {
        true
    }
}
