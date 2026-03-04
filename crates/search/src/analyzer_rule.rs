/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

//! Analyzer rule that expands [`SearchQueryProvider`] table scans into their full logical plan
//! equivalents (index scan → optional join with base table → sort by score → project).
//!
//! By expanding at the analysis phase, DataFusion's optimizer (predicate pushdown, projection
//! pruning, join ordering, etc.) can act on the full search plan rather than on an opaque
//! [`TableProvider`] whose structure is only revealed during physical planning.

use std::sync::Arc;

use datafusion::{
    common::tree_node::{Transformed, TreeNode},
    config::ConfigOptions,
    datasource::DefaultTableSource,
    error::Result as DFResult,
    logical_expr::{Extension, LogicalPlan},
    optimizer::AnalyzerRule,
};

use crate::{provider::SearchQueryProvider, telemetry_node::SearchTelemetryNode};

/// Analyzer rule that replaces `TableScan(SearchQueryProvider)` nodes with the expanded
/// logical plan produced by [`SearchQueryProvider::to_logical_plan`].
pub struct SearchQueryAnalyzerRule;

impl std::fmt::Debug for SearchQueryAnalyzerRule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SearchQueryAnalyzerRule").finish()
    }
}

impl AnalyzerRule for SearchQueryAnalyzerRule {
    fn name(&self) -> &'static str {
        "search_query_expand"
    }

    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> DFResult<LogicalPlan> {
        plan.transform(|node| {
            let LogicalPlan::TableScan(ref scan) = node else {
                return Ok(Transformed::no(node));
            };

            let Some(default_source) = scan.source.as_any().downcast_ref::<DefaultTableSource>()
            else {
                return Ok(Transformed::no(node));
            };

            let Some(provider) = default_source
                .table_provider
                .as_any()
                .downcast_ref::<SearchQueryProvider>()
            else {
                return Ok(Transformed::no(node));
            };

            let expanded =
                provider.to_logical_plan(scan.projection.as_ref(), &scan.filters, scan.fetch)?;

            let plan = if let Some(callback) = &provider.scan_callback {
                LogicalPlan::Extension(Extension {
                    node: Arc::new(SearchTelemetryNode::new(expanded, Arc::clone(callback))),
                })
            } else {
                expanded
            };

            Ok(Transformed::yes(plan))
        })
        .map(|t| t.data)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};
    use datafusion::{
        config::ConfigOptions,
        datasource::DefaultTableSource,
        logical_expr::{LogicalPlan, LogicalPlanBuilder},
        optimizer::AnalyzerRule,
    };

    use crate::analyzer_rule::SearchQueryAnalyzerRule;

    #[test]
    fn rule_is_noop_for_non_search_scans() {
        let rule = SearchQueryAnalyzerRule;
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

        // Create a plain in-memory table provider
        let provider = Arc::new(
            datafusion::datasource::MemTable::try_new(Arc::clone(&schema), vec![])
                .expect("MemTable creation should not fail"),
        );

        let source = Arc::new(DefaultTableSource::new(provider));
        let plan = LogicalPlanBuilder::scan("t", source, None)
            .expect("scan should not fail")
            .build()
            .expect("build should not fail");

        let result = rule
            .analyze(plan, &ConfigOptions::default())
            .expect("analyze should not fail");

        // The plan should be structurally unchanged (still a TableScan, not expanded).
        assert!(
            matches!(result, LogicalPlan::TableScan(_)),
            "Expected TableScan to be unchanged, got: {result:?}"
        );
    }
}
