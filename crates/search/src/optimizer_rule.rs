/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! Optimizer rule that expands [`SearchQueryProvider`] table scans into their full logical plan
//! equivalents (index scan → optional join with base table → sort by score → project).
//!
//! By expanding during the optimization phase (after `PushDownFilter` has run), the `TableScan`
//! already carries any pushed-down filters in `scan.filters`. These filters are then forwarded
//! into [`SearchQueryProvider::to_logical_plan`], which passes them to the underlying search
//! index (e.g. `S3VectorsQueryExec`), preserving filter pushdown behavior.
//!
//! This was previously an analyzer rule, but analyzer rules run *before* `PushDownFilter`,
//! meaning `scan.filters` was always empty at expansion time. Moving to an optimizer rule
//! fixes the filter pushdown regression for search queries.

use std::sync::Arc;

use datafusion::{
    common::tree_node::Transformed,
    datasource::DefaultTableSource,
    error::Result as DFResult,
    logical_expr::{Extension, LogicalPlan, LogicalPlanBuilder},
    optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule},
};

use crate::{provider::SearchQueryProvider, telemetry_node::SearchTelemetryNode};

/// Optimizer rule that replaces `TableScan(SearchQueryProvider)` nodes with the expanded
/// logical plan produced by [`SearchQueryProvider::to_logical_plan`].
///
/// This rule runs after DataFusion's built-in `PushDownFilter` optimizer rule, so
/// `TableScan.filters` is already populated with any pushed-down predicates.
pub struct SearchQueryOptimizerRule;

impl std::fmt::Debug for SearchQueryOptimizerRule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SearchQueryOptimizerRule").finish()
    }
}

impl OptimizerRule for SearchQueryOptimizerRule {
    fn name(&self) -> &'static str {
        "search_query_expand"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> DFResult<Transformed<LogicalPlan>> {
        let LogicalPlan::TableScan(ref scan) = plan else {
            return Ok(Transformed::no(plan));
        };

        let Some(default_source) = scan.source.as_any().downcast_ref::<DefaultTableSource>()
        else {
            return Ok(Transformed::no(plan));
        };

        let Some(provider) = default_source
            .table_provider
            .as_any()
            .downcast_ref::<SearchQueryProvider>()
        else {
            return Ok(Transformed::no(plan));
        };

        let expanded =
            provider.to_logical_plan(scan.projection.as_ref(), &scan.filters, scan.fetch)?;

        // Wrap the expanded plan with the original table name as an alias so that parent
        // operations (Sort, Projection, etc.) can still reference columns using the original
        // table name. This makes the expansion transparent to the rest of the query.
        let aliased = LogicalPlanBuilder::new_from_arc(Arc::new(expanded))
            .alias(scan.table_name.to_string())?
            .build()?;

        let plan = if let Some(callback) = &provider.scan_callback {
            LogicalPlan::Extension(Extension {
                node: Arc::new(SearchTelemetryNode::new(aliased, Arc::clone(callback))),
            })
        } else {
            aliased
        };

        Ok(Transformed::yes(plan))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::record_batch::RecordBatch;
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::{
        datasource::DefaultTableSource,
        logical_expr::{LogicalPlan, LogicalPlanBuilder},
        optimizer::{OptimizerContext, OptimizerRule},
    };

    use crate::optimizer_rule::SearchQueryOptimizerRule;

    #[test]
    fn rule_is_noop_for_non_search_scans() {
        let rule = SearchQueryOptimizerRule;
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

        // Create a plain in-memory table provider with one empty partition
        let batch = RecordBatch::new_empty(Arc::clone(&schema));
        let provider = Arc::new(
            datafusion::datasource::MemTable::try_new(Arc::clone(&schema), vec![vec![batch]])
                .expect("MemTable creation should not fail"),
        );

        let source = Arc::new(DefaultTableSource::new(provider));
        let plan = LogicalPlanBuilder::scan("t", source, None)
            .expect("scan should not fail")
            .build()
            .expect("build should not fail");

        let config = OptimizerContext::new();
        let result = rule
            .rewrite(plan, &config)
            .expect("rewrite should not fail");

        // The plan should be structurally unchanged (still a TableScan, not expanded).
        assert!(
            matches!(result.data, LogicalPlan::TableScan(_)),
            "Expected TableScan to be unchanged, got: {:?}",
            result.data
        );
        assert!(
            !result.transformed,
            "Expected no transformation for non-search scan"
        );
    }
}
