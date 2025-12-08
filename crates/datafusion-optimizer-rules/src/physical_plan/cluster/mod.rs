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
