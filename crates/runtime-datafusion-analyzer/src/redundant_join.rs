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

use std::{any::Any, cmp::Ordering, collections::HashSet, fmt, hash::Hasher, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    common::{
        Column, DFSchemaRef, Dependency, FunctionalDependence, JoinConstraint, JoinType,
        tree_node::{Transformed, TreeNode},
    },
    config::ConfigOptions,
    error::DataFusionError,
    execution::SessionState,
    logical_expr::{
        Extension, Filter, InvariantLevel, Join, LogicalPlan, Projection, UserDefinedLogicalNode,
        UserDefinedLogicalNodeCore,
    },
    optimizer::AnalyzerRule,
    physical_plan::ExecutionPlan,
    physical_planner::{ExtensionPlanner, PhysicalPlanner},
    prelude::Expr,
};

#[derive(Debug)]
pub struct RedundantJoinAnalyzerRule {}

impl RedundantJoinAnalyzerRule {
    fn prune_left_join(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>, DataFusionError> {
        tracing::warn!("planning={plan:?}\n");
        // Look for a `Projection` wrapping `Join`
        let LogicalPlan::Projection(Projection {
            expr,
            input,
            schema: proj_schema,
            ..
        }) = &plan
        else {
            tracing::warn!("out @Projection ");
            return Ok(Transformed::no(plan));
        };

        let LogicalPlan::Extension(Extension { node }) = Arc::unwrap_or_clone(Arc::clone(&input))
        else {
            tracing::warn!("out @Extension ");
            return Ok(Transformed::no(plan));
        };

        tracing::warn!("finding a DistinctJoinColumns?");
        let Some(DistinctJoinColumns {
            input: LogicalPlan::Join(join),
            left: distinct_left,
            right: distinct_right,
        }) = node.as_any().downcast_ref::<DistinctJoinColumns>().cloned()
        else {
            tracing::warn!("out @ DistinctJoinColumns ");
            return Ok(Transformed::no(plan));
        };

        tracing::warn!("Found meself a DistinctJoinColumns");

        let (left_on, right_on): (Vec<Expr>, Vec<Expr>) = join.on.clone().into_iter().unzip();

        tracing::warn!("left_on={left_on:?}, right_on={right_on:?}");
        // Ensure `on` is distinct.
        let left_ok = Self::ensure_expr_are_column_superset(left_on, distinct_left);
        let right_ok = Self::ensure_expr_are_column_superset(right_on, distinct_right);

        if !(left_ok && right_ok) {
            tracing::warn!("left_ok={left_ok:?}, right_ok={right_ok:?}");
            return Ok(Transformed::no(plan));
        };

        // Check if LHS of Join has all columns referenced in projection
        if !Self::child_is_sufficient(&join.left, expr.as_slice(), &join.filter) {
            tracing::warn!("child_is_sufficient");
            return Ok(Transformed::no(plan));
        }

        // We can now prune JOIN and just return LHS
        // Apply filter directly to LHS.
        let input: Arc<LogicalPlan> = if let Some(f) = join.filter {
            LogicalPlan::Filter(Filter::try_new(f, join.left)?).into()
        } else {
            join.left
        };

        Ok(Transformed::yes(LogicalPlan::Projection(
            Projection::try_new(expr.clone(), input)?,
        )))
    }

    pub fn ensure_expr_are_column_superset(expr: Vec<Expr>, cols: Vec<Column>) -> bool {
        let expr_columns: HashSet<Column> = expr
            .iter()
            .filter_map(|e| match e {
                Expr::Column(c) => Some(c.clone()),
                _ => None,
            })
            .collect();

        expr_columns.is_superset(&cols.into_iter().collect::<HashSet<_>>())
    }

    // Returns true if the child has all sufficient columns to satisfy the `projection` and to handle all `filters`.
    pub fn child_is_sufficient(
        child: &Arc<LogicalPlan>,
        projection: &[Expr],
        filters: &Option<Expr>,
    ) -> bool {
        let child_columns_vec = child.schema().columns();
        let child_columns = child_columns_vec.iter().collect::<HashSet<&Column>>();

        let expr_cols = projection
            .iter()
            .flat_map(|e| e.column_refs())
            .collect::<HashSet<&Column>>();

        if !expr_cols.is_subset(&child_columns) {
            tracing::warn!(
                "child_is_sufficient expr_cols={expr_cols:?}. child_columns={child_columns:?}"
            );
            return false;
        }

        if let Some(f) = &filters {
            tracing::warn!(
                "child_is_sufficient f.column_refs()={:?}. child_columns={child_columns:?}",
                f.column_refs()
            );
        }

        filters
            .as_ref()
            .is_none_or(|f| f.column_refs().is_subset(&child_columns))
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

/// For [`AnalyzerRule`]s, guarantees that the left and right side of a [`LogicalPlan::Join`] are distinct rows for a given set of columns.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub struct DistinctJoinColumns {
    input: LogicalPlan,
    left: Vec<Column>,
    right: Vec<Column>,
}

/// Construct a [`DistinctJoinColumns`] from a [`Join`] assuming that the [`Join::on`] expressions are both 1. columns, and 2. defined distinctness.
impl From<Join> for DistinctJoinColumns {
    fn from(value: Join) -> Self {
        let (left, right): (Vec<_>, Vec<_>) = value
            .on
            .iter()
            .filter_map(|(a, b)| match (a, b) {
                (Expr::Column(l), Expr::Column(r)) => Some((l.clone(), r.clone())),
                _ => None,
            })
            .unzip();

        DistinctJoinColumns {
            input: LogicalPlan::Join(value),
            left,
            right,
        }
    }
}

impl UserDefinedLogicalNodeCore for DistinctJoinColumns {
    fn name(&self) -> &str {
        "DistinctJoinColumns"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        Vec::new()
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DistinctJoinColumns")
    }

    fn necessary_children_exprs(&self, output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        // Since the input & output schema is the same, output columns require their corresponding index in the input columns.
        Some(vec![output_columns.to_vec()])
    }

    fn prevent_predicate_push_down_columns(&self) -> HashSet<String> {
        // Allow filters for all columns to be pushed down
        HashSet::new()
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> Result<Self, DataFusionError> {
        assert_eq!(inputs.len(), 1, "should have one input");
        assert_eq!(exprs.len(), 0, "should have no expressions");
        let Some(input) = inputs.into_iter().next() else {
            panic!("should have one input");
        };

        Ok(Self {
            input,
            left: self.left.clone(),
            right: self.right.clone(),
        })
    }

    fn check_invariants(
        &self,
        check: InvariantLevel,
        plan: &LogicalPlan,
    ) -> Result<(), DataFusionError> {
        Ok(())
    }

    fn supports_limit_pushdown(&self) -> bool {
        true
    }
}

#[derive(Default)]
pub struct DistinctJoinColumnsExtensionPlanner {}

impl DistinctJoinColumnsExtensionPlanner {
    #[must_use]
    pub fn new() -> Self {
        DistinctJoinColumnsExtensionPlanner {}
    }
}

#[async_trait]
impl ExtensionPlanner for DistinctJoinColumnsExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>, DataFusionError> {
        let distinct_join_columns = node.as_any().downcast_ref::<DistinctJoinColumns>();
        if distinct_join_columns.is_some() {
            assert_eq!(logical_inputs.len(), 1, "should have 1 input");
            assert_eq!(physical_inputs.len(), 1, "should have 1 input");
            let physical_input = &physical_inputs[0];

            return Ok(Some(Arc::clone(&physical_input)));
        }

        Ok(None)
    }
}
