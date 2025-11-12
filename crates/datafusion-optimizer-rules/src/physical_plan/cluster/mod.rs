use crate::physical_plan::cluster::distribute_file_scan::DistributeFileScanOptimizer;
use crate::physical_plan::cluster::ensure_serializable_file_scan::EnsureSerializableFileScanOptimizer;
use crate::physical_plan::cluster::union_projection_pushdown::UnionProjectionPushdownOptimizer;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::optimizer::PhysicalOptimizer;
use datafusion::physical_planner::DefaultPhysicalPlanner;
use std::sync::Arc;

pub mod distribute_file_scan;
pub mod ensure_serializable_file_scan;
pub mod union_projection_pushdown;

pub fn datafusion_and_cluster_physical_optimizers()
-> Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> {
    let mut rules = PhysicalOptimizer::new().rules;
    rules.extend(cluster_physical_optimizers());
    rules
}

pub fn cluster_physical_optimizers() -> Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> {
    vec![
        EnsureSerializableFileScanOptimizer::new(),
        DistributeFileScanOptimizer::new(),
        UnionProjectionPushdownOptimizer::new(),
    ]
}
