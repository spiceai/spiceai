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

use std::{collections::HashSet, fmt::Debug, sync::Arc};

use datafusion::{
    common::tree_node::{Transformed, TreeNode},
    config::ConfigOptions,
    datasource::DefaultTableSource,
    error::DataFusionError,
    logical_expr::{Extension, LogicalPlan, TableScan},
    optimizer::AnalyzerRule,
};
use runtime_datafusion_index::{Index, analyzer::IndexTableScanNode};
use search::index::{VectorScanTableProvider, derived_columns_from_vector_index};

/// An [`AnalyzerRule`] that
pub struct AvoidDerivedVectorColumnOnIndexRule {}

impl Debug for AvoidDerivedVectorColumnOnIndexRule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AvoidDerivedVectorColumnOnIndexRule")
            .finish()
    }
}

impl AvoidDerivedVectorColumnOnIndexRule {
    /// For [`Index`] in [`IndexTableScanNode`], find all derived columns of [`VectorIndex`]s.
    fn derived_vector_index_columns(indexes: &[Arc<dyn Index + Send + Sync>]) -> Vec<String> {
        indexes
            .iter()
            .filter_map(derived_columns_from_vector_index)
            .flatten()
            .collect()
    }

    fn avoid_derived_vector_columns(
        derived: &[&String],
        index_scan: &IndexTableScanNode,
        table_scan: &TableScan,
    ) -> Result<LogicalPlan, DataFusionError> {
        let mut proj = match table_scan.projection.as_ref() {
            None => (0..table_scan.projected_schema.fields().len()).collect(),
            Some(p) => p.clone(),
        };
        let derived = derived
            .iter()
            .filter_map(|&d| {
                table_scan
                    .projected_schema
                    .index_of_column_by_name(None, d.as_str())
            })
            .collect::<HashSet<usize>>();
        proj.retain(|p| !derived.contains(p));

        let tbl_scan = TableScan::try_new(
            table_scan.table_name.clone(),
            Arc::clone(&table_scan.source),
            Some(proj),
            table_scan.filters.clone(),
            table_scan.fetch,
        )?;

        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(IndexTableScanNode::new(
                LogicalPlan::TableScan(tbl_scan),
                index_scan.indexes().to_vec(),
            )),
        }))
    }
}

impl AnalyzerRule for AvoidDerivedVectorColumnOnIndexRule {
    fn name(&self) -> &str {
        "avoid_derived_vector_columns_on_index"
    }

    fn analyze(
        &self,
        plan: LogicalPlan,
        _config: &ConfigOptions,
    ) -> Result<LogicalPlan, DataFusionError> {
        Ok(plan
            .transform_up(|plan| {
                let LogicalPlan::Extension(ref ext) = plan else {
                    return Ok(Transformed::no(plan));
                };
                let Some(index_scan) = ext.node.as_any().downcast_ref::<IndexTableScanNode>()
                else {
                    return Ok(Transformed::no(plan));
                };

                let LogicalPlan::TableScan(table_scan) = index_scan.input() else {
                    return Ok(Transformed::no(plan));
                };

                // Check it is DefaultTableSource(VectorScanTableProvider)
                let Some(default_table_source) = table_scan
                    .source
                    .as_any()
                    .downcast_ref::<DefaultTableSource>()
                else {
                    return Ok(Transformed::no(plan));
                };
                if default_table_source
                    .table_provider
                    .as_any()
                    .downcast_ref::<VectorScanTableProvider>()
                    .is_none()
                {
                    return Ok(Transformed::no(plan));
                };

                let derived_columns = Self::derived_vector_index_columns(index_scan.indexes());
                if derived_columns.is_empty() {
                    return Ok(Transformed::no(plan));
                };

                let projected_derived_columns: Vec<&String> = derived_columns
                    .iter()
                    .filter(|&c| {
                        table_scan
                            .projected_schema
                            .has_column_with_unqualified_name(c)
                    })
                    .collect();

                if projected_derived_columns.is_empty() {
                    return Ok(Transformed::no(plan));
                }

                return Ok(Transformed::yes(Self::avoid_derived_vector_columns(
                    &projected_derived_columns,
                    index_scan,
                    table_scan,
                )?));
            })?
            .data)
    }
}
