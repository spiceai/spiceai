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

use datafusion::{
    common::{
        Column, DFSchemaRef, Dependency, FunctionalDependence, JoinConstraint, JoinType,
        tree_node::{Transformed, TreeNode},
    },
    config::ConfigOptions,
    error::DataFusionError,
    logical_expr::{
        Extension, Filter, InvariantLevel, Join, LogicalPlan, Projection, UserDefinedLogicalNode,
    },
    optimizer::AnalyzerRule,
    prelude::Expr,
};

#[derive(Debug)]
pub struct RedundantJoinAnalyzerRule {}

impl RedundantJoinAnalyzerRule {
    fn prune_left_join(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>, DataFusionError> {
        // Look for a `Projection` wrapping `Join`
        let LogicalPlan::Projection(Projection {
            expr,
            input,
            schema: proj_schema,
            ..
        }) = &plan
        else {
            return Ok(Transformed::no(plan));
        };

        let LogicalPlan::Extension(Extension { node }) = Arc::unwrap_or_clone(Arc::clone(&input))
        else {
            return Ok(Transformed::no(plan));
        };

        let Some(DistinctJoinColumns {
            input: LogicalPlan::Join(join),
            left: distinct_left,
            right: distinct_right,
        }) = node.as_any().downcast_ref::<DistinctJoinColumns>().cloned()
        else {
            return Ok(Transformed::no(plan));
        };

        let (left_on, right_on): (Vec<Expr>, Vec<Expr>) = join.on.clone().into_iter().unzip();

        // Ensure `on` is distinct.
        let left_ok = Self::ensure_expr_are_column_superset(left_on, distinct_left);
        let right_ok = Self::ensure_expr_are_column_superset(right_on, distinct_right);

        if !(left_ok && right_ok) {
            return Ok(Transformed::no(LogicalPlan::Projection(
                Projection::try_new_with_schema(
                    expr.clone(),
                    LogicalPlan::Join(join).into(),
                    proj_schema.clone(),
                )?,
            )));
        };

        // Check if LHS of Join has all columns referenced in projection
        if !Self::child_is_sufficient(&join.left, expr.as_slice(), &join.filter) {
            return Ok(Transformed::no(LogicalPlan::Projection(
                Projection::try_new_with_schema(
                    expr.clone(),
                    LogicalPlan::Join(join).into(),
                    Arc::clone(&proj_schema),
                )?,
            )));
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
            return false;
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
#[derive(Debug, Clone)]
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

impl UserDefinedLogicalNode for DistinctJoinColumns {
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Return the plan's name.
    fn name(&self) -> &str {
        "DistinctJoinColumns"
    }

    /// Return the logical plan's inputs.
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    /// Return the output schema of this logical plan node.
    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn check_invariants(
        &self,
        check: InvariantLevel,
        plan: &LogicalPlan,
    ) -> Result<(), DataFusionError> {
        Ok(())
    }

    fn expressions(&self) -> Vec<Expr> {
        Vec::new()
    }

    /// Write a single line, human readable string to `f` for use in explain plan.
    ///
    /// For example: `TopK: k=10`
    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DistinctJoinColumns")
    }

    /// Create a new `UserDefinedLogicalNode` with the specified children
    /// and expressions. This function is used during optimization
    /// when the plan is being rewritten and a new instance of the
    /// `UserDefinedLogicalNode` must be created.
    ///
    /// Note that exprs and inputs are in the same order as the result
    /// of self.inputs and self.exprs.
    ///
    /// So, `self.with_exprs_and_inputs(exprs, ..).expressions() == exprs
    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> Result<Arc<dyn UserDefinedLogicalNode>, DataFusionError> {
        let Some(LogicalPlan::Join(j)) = inputs.first() else {
            return Err(DataFusionError::Internal(format!(
                "expect a single Join input to {}",
                self.name()
            )));
        };

        Ok(Arc::new(Self {
            input: LogicalPlan::Join(j.clone()),
            left: self.left.clone(),
            right: self.right.clone(),
        }))
    }

    fn necessary_children_exprs(&self, _output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        None
    }

    fn dyn_hash(&self, state: &mut dyn Hasher) {}

    fn dyn_eq(&self, other: &dyn UserDefinedLogicalNode) -> bool {
        match other.as_any().downcast_ref::<Self>() {
            Some(Self { input, .. }) => *input == self.input,
            None => false,
        }
    }
    fn dyn_ord(&self, other: &dyn UserDefinedLogicalNode) -> Option<Ordering> {
        other
            .as_any()
            .downcast_ref::<Self>()
            .and_then(|other| self.input.partial_cmp(&other.input))
    }

    fn supports_limit_pushdown(&self) -> bool {
        true
    }
}
