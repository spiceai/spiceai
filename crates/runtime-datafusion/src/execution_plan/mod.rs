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

use datafusion::catalog::{Session, TableProvider};
use datafusion::common::ToDFSchema;
use datafusion::error::Result;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
use datafusion::physical_expr::create_physical_expr;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion::physical_planner::DefaultPhysicalPlanner;
use std::sync::Arc;

pub mod fallback_on_zero_results;
pub mod schema_cast;
pub mod slice;
pub mod tee;

#[derive(Clone)]
pub struct TableScanParams {
    state: Arc<SessionState>,
    projection: Option<Vec<usize>>,
    filters: Vec<Expr>,
    limit: Option<usize>,
}

impl TableScanParams {
    /// # Panics
    ///
    /// Will panic if the `state` cannot be downcast to `SessionState`.
    /// This isn't possible with the current version of `DataFusion` (v41).
    #[must_use]
    pub fn new(
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Self {
        let Some(session_state) = state.as_any().downcast_ref::<SessionState>() else {
            panic!("Failed to downcast Session to SessionState");
        };
        Self {
            state: Arc::new(session_state.clone()),
            projection: projection.cloned(),
            filters: filters.to_vec(),
            limit,
        }
    }

    /// Builds a scan with residual filters and optimizes it into one output partition.
    ///
    /// Use this for scans created during execution, outside the query's physical
    /// optimization pass. `filters_to_reapply` contains only predicates that the
    /// provider does not enforce exactly, including any caller-isolation predicate.
    ///
    /// # Errors
    ///
    /// Returns an error if scanning, residual filtering or physical optimization fails.
    pub async fn scan_and_optimize(
        &self,
        provider: &dyn TableProvider,
        filters_to_reapply: &[Expr],
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let input = provider
            .scan(
                self.state.as_ref(),
                self.projection.as_ref(),
                &self.filters,
                // A provider cannot safely truncate rows before residual filtering.
                self.limit.filter(|_| filters_to_reapply.is_empty()),
            )
            .await?;
        let input = wrap_with_filter(input, self.state.as_ref(), filters_to_reapply)?;
        optimize_single_partition_plan(input, &self.state)
    }

    /// Scan filters plus `extra`, preserving projection and limit.
    #[must_use]
    pub fn with_additional_filters(&self, extra: &[Expr]) -> Self {
        let mut cloned = self.clone();
        cloned.filters.extend(extra.iter().cloned());
        cloned
    }

    /// Drop the caller's projection so residual filters can see every column.
    #[must_use]
    pub fn without_projection(&self) -> Self {
        let mut cloned = self.clone();
        cloned.projection = None;
        cloned
    }
}

/// Applies the caller's physical optimizer rules to a late-built subtree and
/// coalesces every resulting partition into its single output stream.
fn optimize_single_partition_plan(
    plan: Arc<dyn ExecutionPlan>,
    session_state: &SessionState,
) -> Result<Arc<dyn ExecutionPlan>> {
    let mut optimized =
        DefaultPhysicalPlanner::default().optimize_physical_plan(plan, session_state, |_, _| {})?;

    if optimized.output_partitioning().partition_count() > 1 {
        optimized = Arc::new(CoalescePartitionsExec::new(optimized));
    }

    Ok(optimized)
}

/// Wraps an input `ExecutionPlan` with a `FilterExec` for the given filters.
///
/// This is useful when a `TableProvider` does not fully support filter pushdown
/// (i.e., returns `Inexact` or `Unsupported` for some filters). The caller should
/// pass only the filters that need to be re-applied after scanning.
///
/// If `filters` is empty, the input plan is returned unchanged.
///
/// # Errors
///
/// Returns an error if the filter expression cannot be created or applied.
pub fn wrap_with_filter(
    input: Arc<dyn ExecutionPlan>,
    state: &dyn Session,
    filters: &[Expr],
) -> Result<Arc<dyn ExecutionPlan>> {
    let Some(session_state) = state.as_any().downcast_ref::<SessionState>() else {
        return Err(datafusion::error::DataFusionError::Internal(
            "Failed to downcast Session to SessionState".to_string(),
        ));
    };

    let Some(joined_filters) = filters.iter().cloned().reduce(|left, right| {
        Expr::BinaryExpr(BinaryExpr::new(
            Box::new(left),
            Operator::And,
            Box::new(right),
        ))
    }) else {
        tracing::trace!("No filters to apply to input plan");
        return Ok(input);
    };

    let input_schema = input.schema();
    let input_dfschema = Arc::clone(&input_schema).to_dfschema()?;

    tracing::trace!("Wrapping execution plan with FilterExec for: {joined_filters}");

    let physical_expr = create_physical_expr(
        &joined_filters,
        &input_dfschema,
        session_state.execution_props(),
        &datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext::default(),
    )?;

    let filtered_input = FilterExec::try_new(physical_expr, input)?;

    Ok(Arc::new(filtered_input))
}

/// Filters an input `ExecutionPlan` using the filters in `TableScanParams`.
pub(crate) fn filter_plan(
    input: Arc<dyn ExecutionPlan>,
    scan_params: &TableScanParams,
) -> Result<Arc<dyn ExecutionPlan>> {
    let Some(joined_filters) = scan_params.filters.iter().cloned().reduce(|left, right| {
        Expr::BinaryExpr(BinaryExpr::new(
            Box::new(left),
            Operator::And,
            Box::new(right),
        ))
    }) else {
        tracing::trace!("No filters to apply to input plan");
        return Ok(input);
    };
    let input_schema = input.schema();
    let input_dfschema = Arc::clone(&input_schema).to_dfschema()?;

    tracing::trace!("Creating physical expression for filter: {joined_filters}");

    let physical_expr = create_physical_expr(
        &joined_filters,
        &input_dfschema,
        scan_params.state.execution_props(),
        &datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext::default(),
    )?;

    let filtered_input = FilterExec::try_new(physical_expr, input)?;

    Ok(Arc::new(filtered_input))
}
