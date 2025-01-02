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

use datafusion::catalog::Session;
use datafusion::common::ToDFSchema;
use datafusion::error::Result;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
use datafusion::physical_expr::create_physical_expr;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

pub mod fallback_on_zero_results;
pub mod schema_cast;
pub mod slice;
pub mod tee;

#[derive(Clone)]
pub struct TableScanParams {
    state: SessionState,
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
            state: session_state.clone(),
            projection: projection.cloned(),
            filters: filters.to_vec(),
            limit,
        }
    }
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
    )?;

    let filtered_input = FilterExec::try_new(physical_expr, input)?;

    Ok(Arc::new(filtered_input))
}
