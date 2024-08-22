/*
Copyright 2024 The Spice.ai OSS Authors

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

use std::sync::Arc;

use datafusion::{
    common::tree_node::{Transformed, TreeNode},
    config::ConfigOptions,
    error::Result,
    logical_expr::{Extension, LogicalPlan},
    optimizer::AnalyzerRule,
};
use datafusion_federation::FederatedPlanNode;

use super::telemetry::BytesScannedNode;

#[derive(Default)]
pub struct SpiceExtensionAnalyzerRule {}

impl AnalyzerRule for SpiceExtensionAnalyzerRule {
    // Walk over the plan and perform any replacements required by Spice extensions.
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        let transformed_plan = plan.transform_up(|plan| match plan {
            LogicalPlan::TableScan(table_scan) => {
                let bytes_scanned = BytesScannedNode::new(LogicalPlan::TableScan(table_scan));
                let ext_node = Extension {
                    node: Arc::new(bytes_scanned),
                };
                Ok(Transformed::yes(LogicalPlan::Extension(ext_node)))
            }
            LogicalPlan::Extension(extension) => {
                let plan_node = extension.node.as_any().downcast_ref::<FederatedPlanNode>();

                if plan_node.is_some() {
                    let bytes_scanned =
                        BytesScannedNode::new(LogicalPlan::Extension(extension.clone()));
                    let ext_node = Extension {
                        node: Arc::new(bytes_scanned),
                    };
                    Ok(Transformed::yes(LogicalPlan::Extension(ext_node)))
                } else {
                    Ok(Transformed::no(LogicalPlan::Extension(extension)))
                }
            }
            _ => Ok(Transformed::no(plan)),
        })?;
        Ok(transformed_plan.data)
    }

    /// A human readable name for this optimizer rule
    fn name(&self) -> &str {
        "spice_extension_analyzer_rule"
    }
}

impl SpiceExtensionAnalyzerRule {
    pub fn new() -> Self {
        Self::default()
    }
}
