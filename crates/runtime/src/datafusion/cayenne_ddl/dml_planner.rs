/*
Copyright 2026, Spice AI, Inc.

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

//! Distributed Cayenne DML handler plus logical-plan extraction helpers.
//!
//! The runtime emits generic `datafusion_dml::DmlExtensionNode` values for
//! distributed Cayenne DML. This module provides:
//!
//! - [`DistributedCayenneDmlHandler`] — the catalog-specific handler embedded in
//!   those nodes.
//! - [`extract_filters`] and [`extract_update_assignments`] — helpers used while
//!   rewriting `DataFusion` DML plans into generic extension nodes.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::SessionState;
use datafusion::logical_expr::{Expr, LogicalPlan, dml::InsertOp};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::sql::unparser::expr_to_sql;
use datafusion_dml::{CatalogDmlHandler, DeleteParams, InsertParams, MergeParams, UpdateParams};
use datafusion_expr::utils::conjunction;

use super::physical_plans::{
    DistributedCayenneDeleteExec, DistributedCayenneInsertExec, DistributedCayenneMergeExec,
    DistributedCayenneUpdateExec,
};
use crate::cluster::ExecutorRegistry;

/// Walk the input plan to find the topmost `Filter` and return predicate
/// expressions.
///
/// Used by distributed DELETE/UPDATE planning. Empty means no `WHERE` clause.
pub fn extract_filters(plan: &LogicalPlan) -> DFResult<Vec<Expr>> {
    match plan {
        LogicalPlan::Filter(filter) => Ok(vec![filter.predicate.clone()]),
        LogicalPlan::Projection(proj) => extract_filters(&proj.input),
        _ => Ok(Vec::new()),
    }
}

/// Extract `(column_name, value_expr)` assignment pairs from an UPDATE input
/// plan.
///
/// The UPDATE input is a `Projection` over a (possibly filtered) `TableScan`.
/// Each projection expression is either:
/// - `col AS col` — identity (unchanged column), skipped.
/// - `<expr> AS col` — SET assignment, included.
pub fn extract_update_assignments(
    plan: &LogicalPlan,
    table_name: &datafusion::sql::TableReference,
) -> DFResult<Vec<(String, Expr)>> {
    let LogicalPlan::Projection(proj) = plan else {
        return Ok(Vec::new());
    };

    let mut assignments = Vec::new();
    for expr in &proj.expr {
        let Expr::Alias(alias) = expr else {
            continue;
        };
        let col_name = &alias.name;

        if let Expr::Column(col) = alias.expr.as_ref()
            && col.name == *col_name
            && col.relation.as_ref().is_none_or(|r| *r == *table_name)
        {
            continue;
        }

        assignments.push((col_name.clone(), alias.expr.as_ref().clone()));
    }
    Ok(assignments)
}

fn filters_to_sql(filters: &[Expr]) -> DFResult<Option<String>> {
    if filters.is_empty() {
        return Ok(None);
    }

    let Some(predicate) = conjunction(filters.to_vec()) else {
        return Ok(None);
    };

    let ast = expr_to_sql(&predicate)?;
    Ok(Some(ast.to_string()))
}

fn assignments_to_sql(assignments: &[(String, Expr)]) -> DFResult<Vec<(String, String)>> {
    assignments
        .iter()
        .map(|(col, expr)| {
            let ast = expr_to_sql(expr)?;
            Ok((col.clone(), ast.to_string()))
        })
        .collect()
}

/// Catalog DML handler for distributed Cayenne execution.
#[derive(Debug)]
pub struct DistributedCayenneDmlHandler {
    executor_registry: Arc<ExecutorRegistry>,
    io_runtime: Option<tokio::runtime::Handle>,
}

impl DistributedCayenneDmlHandler {
    #[must_use]
    pub fn new(
        executor_registry: Arc<ExecutorRegistry>,
        io_runtime: Option<tokio::runtime::Handle>,
    ) -> Self {
        Self {
            executor_registry,
            io_runtime,
        }
    }
}

#[async_trait]
impl CatalogDmlHandler for DistributedCayenneDmlHandler {
    fn name(&self) -> &'static str {
        "cayenne_distributed"
    }

    async fn delete_exec(
        &self,
        params: DeleteParams,
        physical_inputs: Vec<Arc<dyn ExecutionPlan>>,
        _session_state: &SessionState,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let [input] = physical_inputs.as_slice() else {
            return Err(DataFusionError::Internal(
                "Distributed DELETE requires exactly one physical input".to_string(),
            ));
        };

        let filter_sql = filters_to_sql(&params.filters)?;

        Ok(Arc::new(DistributedCayenneDeleteExec::new(
            params.table_name,
            Arc::clone(&self.executor_registry),
            filter_sql,
            Arc::clone(input),
        )))
    }

    async fn update_exec(
        &self,
        params: UpdateParams,
        physical_inputs: Vec<Arc<dyn ExecutionPlan>>,
        _session_state: &SessionState,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let [input] = physical_inputs.as_slice() else {
            return Err(DataFusionError::Internal(
                "Distributed UPDATE requires exactly one physical input".to_string(),
            ));
        };

        if params.assignments.is_empty() {
            return Err(DataFusionError::Execution(format!(
                "UPDATE on '{}' has no SET assignments",
                params.table_name
            )));
        }

        let filter_sql = filters_to_sql(&params.filters)?;
        let assignments_sql = assignments_to_sql(&params.assignments)?;

        Ok(Arc::new(DistributedCayenneUpdateExec::new(
            params.table_name,
            Arc::clone(&self.executor_registry),
            filter_sql,
            assignments_sql,
            Arc::clone(input),
        )))
    }

    async fn insert_exec(
        &self,
        params: InsertParams,
        physical_inputs: Vec<Arc<dyn ExecutionPlan>>,
        session_state: &SessionState,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if params.insert_op != InsertOp::Append {
            return Err(DataFusionError::Plan(format!(
                "Unsupported distributed insert op: {}",
                params.insert_op
            )));
        }

        let [input] = physical_inputs.as_slice() else {
            return Err(DataFusionError::Internal(
                "Distributed INSERT requires exactly one physical input".to_string(),
            ));
        };

        let io_runtime = self.io_runtime.clone().ok_or_else(|| {
            DataFusionError::Internal(
                "DistributedCayenneInsertExec requires an IO runtime handle".to_string(),
            )
        })?;

        let ctx = Arc::new(datafusion::prelude::SessionContext::new_with_state(
            session_state.clone(),
        ));

        Ok(Arc::new(DistributedCayenneInsertExec::new(
            params.table_name,
            Arc::clone(&self.executor_registry),
            ctx,
            io_runtime,
            Arc::clone(input),
        )))
    }

    async fn merge_exec(
        &self,
        params: MergeParams,
        _physical_inputs: Vec<Arc<dyn ExecutionPlan>>,
        session_state: &SessionState,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let original_sql = params.original_sql.ok_or_else(|| {
            DataFusionError::Plan(
                "Distributed MERGE requires original SQL text for forwarding".to_string(),
            )
        })?;

        let ctx = Arc::new(datafusion::prelude::SessionContext::new_with_state(
            session_state.clone(),
        ));

        Ok(Arc::new(DistributedCayenneMergeExec::new(
            params.target_table,
            params.source_table,
            params.on_keys,
            original_sql,
            Arc::clone(&self.executor_registry),
            ctx,
        )))
    }
}
