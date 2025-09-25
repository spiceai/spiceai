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

use std::{collections::HashSet, sync::Arc};

use datafusion::{
    common::{
        Column, JoinConstraint, JoinType,
        tree_node::{Transformed, TreeNode},
    },
    config::ConfigOptions,
    error::DataFusionError,
    logical_expr::{Filter, Join, LogicalPlan, Projection},
    optimizer::AnalyzerRule,
};

#[derive(Debug)]
pub struct RedundantJoinAnalyzerRule {}

impl RedundantJoinAnalyzerRule {
    fn prune_left_join(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>, DataFusionError> {
        // Look for a `Projection` wrapping `Join`
        let LogicalPlan::Projection(Projection { expr, input, .. }) = &plan else {
            return Ok(Transformed::no(plan));
        };

        let LogicalPlan::Join(Join {
            left,
            filter,
            join_type: JoinType::Left,
            join_constraint: JoinConstraint::On,
            ..
        }) = Arc::unwrap_or_clone(Arc::clone(input))
        else {
            return Ok(Transformed::no(plan));
        };

        // Check if LHS of Join has all columns referenced in projection
        let left_columns_vec = left.schema().columns();
        let left_columns = left_columns_vec.iter().collect::<HashSet<&Column>>();

        let expr_cols = expr
            .iter()
            .flat_map(|e| e.column_refs())
            .collect::<HashSet<&Column>>();

        if !expr_cols.is_subset(&left_columns) {
            return Ok(Transformed::no(plan));
        }

        // Check if LHS of Join has all columns referenced in filter.
        if let Some(ref filter) = filter
            && !filter.column_refs().is_subset(&left_columns)
        {
            // filters are applied to columns in RHS. Cannot reduce.
            return Ok(Transformed::no(plan));
        }

        // We can now prune JOIN and just return LHS
        // Apply filter directly to LHS.
        let input: Arc<LogicalPlan> = if let Some(f) = filter {
            LogicalPlan::Filter(Filter::try_new(f, left)?).into()
        } else {
            left
        };

        Ok(Transformed::yes(LogicalPlan::Projection(
            Projection::try_new(expr.clone(), input)?,
        )))
    }
}

impl AnalyzerRule for RedundantJoinAnalyzerRule {
    fn analyze(
        &self,
        plan: LogicalPlan,
        _config: &ConfigOptions,
    ) -> Result<LogicalPlan, DataFusionError> {
        let plan = plan.transform_down(Self::prune_left_join)?.data;
        Ok(plan)
    }

    /// A human readable name for this analyzer rule
    fn name(&self) -> &str {
        "RedundantJoinAnalyzerRule"
    }
}
