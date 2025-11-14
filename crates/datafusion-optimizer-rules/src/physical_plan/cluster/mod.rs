use crate::physical_plan::cluster::distribute_file_scan::DistributeFileScanOptimizer;
use crate::physical_plan::cluster::ensure_supported_file_scan::EnsureSupportedFileScan;
use crate::physical_plan::cluster::union_projection_pushdown::UnionProjectionPushdownOptimizer;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::optimizer::PhysicalOptimizer;
use std::sync::Arc;

pub mod distribute_file_scan;
pub mod ensure_supported_file_scan;
pub mod union_projection_pushdown;

#[must_use]
pub fn datafusion_and_cluster_physical_optimizers()
-> Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> {
    let mut rules = PhysicalOptimizer::new().rules;
    rules.extend(cluster_physical_optimizers());
    rules
}

#[must_use]
pub fn cluster_physical_optimizers() -> Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> {
    vec![
        EnsureSupportedFileScan::new(),
        DistributeFileScanOptimizer::new(),
        UnionProjectionPushdownOptimizer::new(),
    ]
}
