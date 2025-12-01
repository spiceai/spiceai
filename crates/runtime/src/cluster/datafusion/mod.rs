use std::sync::Arc;

use datafusion::physical_optimizer::{PhysicalOptimizerRule, optimizer::PhysicalOptimizer};
use datafusion_optimizer_rules::physical_plan::cluster::{
    ensure_supported_file_scan::EnsureSupportedFileScan,
    union_projection_pushdown::UnionProjectionPushdownOptimizer,
};
use runtime_datafusion::optimizer_rule::distribute_file_scan::DistributeFileScanOptimizer;

pub mod codec;
pub mod datafusion_scheduler_ext;

#[must_use]
pub fn datafusion_and_cluster_physical_optimizers()
-> Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> {
    let mut rules = PhysicalOptimizer::new().rules;
    rules.extend(cluster_physical_optimizers());
    rules
}

#[must_use]
fn cluster_physical_optimizers() -> Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> {
    vec![
        EnsureSupportedFileScan::new(),
        DistributeFileScanOptimizer::new(),
        UnionProjectionPushdownOptimizer::new(),
    ]
}
