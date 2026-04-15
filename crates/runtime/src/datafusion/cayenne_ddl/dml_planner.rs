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

//! Extension planners for Cayenne DML nodes.
//!
//! DDL nodes are handled by `datafusion_ddl::DdlExtensionPlanner`.
//! Local MERGE is handled by `cayenne::ddl::CayenneDmlExtensionPlanner`.
//!
//! This module provides:
//!
//! - [`DistributedCayenneDmlExtensionPlanner`] — handles the four distributed
//!   DML nodes (DELETE, UPDATE, INSERT, MERGE) that forward operations to
//!   executor nodes. Only registered when an [`ExecutorRegistry`] is present.
//!
//! The handler layer follows the optional-overlay contract from
//! `datafusion-dml`: handlers override only operations that need custom
//! behavior and inherit trait defaults for standard `DataFusion` execution.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::SessionState;
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNode, dml::InsertOp};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use datafusion::sql::unparser::expr_to_sql;
use datafusion_dml::{CatalogDmlHandler, DeleteParams, InsertParams, MergeParams, UpdateParams};
use datafusion_expr::utils::conjunction;

use super::logical_nodes::{
    DistributedCayenneDeleteNode, DistributedCayenneInsertNode, DistributedCayenneMergeNode,
    DistributedCayenneUpdateNode,
};
use super::physical_plans::{
    DistributedCayenneDeleteExec, DistributedCayenneInsertExec, DistributedCayenneMergeExec,
    DistributedCayenneUpdateExec,
};
use crate::cluster::executor_registry::ExecutorRegistry;

// ── DML extraction helpers ───────────────────────────────────────────────────

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

        // Skip identity projections (unchanged columns).
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

// ── DistributedCayenneDmlHandler ──────────────────────────────────────────────

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
        let Some(input) = physical_inputs.first() else {
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
        let Some(input) = physical_inputs.first() else {
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

        let Some(input) = physical_inputs.first() else {
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

// ── DistributedCayenneDmlExtensionPlanner ─────────────────────────────────────

/// Extension planner for distributed Cayenne DML nodes.
///
/// Handles DELETE, UPDATE, INSERT, and MERGE nodes that forward operations
/// to executor nodes. Only registered when an [`ExecutorRegistry`] is present.
#[derive(Debug)]
pub struct DistributedCayenneDmlExtensionPlanner {
    handler: Arc<DistributedCayenneDmlHandler>,
}

impl DistributedCayenneDmlExtensionPlanner {
    #[must_use]
    pub fn new(
        executor_registry: Arc<ExecutorRegistry>,
        io_runtime: Option<tokio::runtime::Handle>,
    ) -> Self {
        Self {
            handler: Arc::new(DistributedCayenneDmlHandler::new(
                executor_registry,
                io_runtime,
            )),
        }
    }
}

#[async_trait]
impl ExtensionPlanner for DistributedCayenneDmlExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
        if let Some(delete) = node.as_any().downcast_ref::<DistributedCayenneDeleteNode>() {
            return self
                .handler
                .delete_exec(
                    DeleteParams {
                        table_name: delete.table_name.clone(),
                        filters: delete.filters.clone(),
                    },
                    physical_inputs.to_vec(),
                    session_state,
                )
                .await
                .map(Some);
        }

        if let Some(update) = node.as_any().downcast_ref::<DistributedCayenneUpdateNode>() {
            return self
                .handler
                .update_exec(
                    UpdateParams {
                        table_name: update.table_name.clone(),
                        filters: update.filters.clone(),
                        assignments: update.assignments.clone(),
                    },
                    physical_inputs.to_vec(),
                    session_state,
                )
                .await
                .map(Some);
        }

        if let Some(insert) = node.as_any().downcast_ref::<DistributedCayenneInsertNode>() {
            return self
                .handler
                .insert_exec(
                    InsertParams {
                        table_name: insert.table_name.clone(),
                        insert_op: insert.insert_op,
                    },
                    physical_inputs.to_vec(),
                    session_state,
                )
                .await
                .map(Some);
        }

        if let Some(merge) = node.as_any().downcast_ref::<DistributedCayenneMergeNode>() {
            return self
                .handler
                .merge_exec(
                    MergeParams {
                        target_table: merge.target_table.clone(),
                        source_table: merge.source_table.clone(),
                        target_qualifier: merge.target_qualifier.clone(),
                        source_qualifier: merge.source_qualifier.clone(),
                        on_keys: merge.on_keys.clone(),
                        // Distributed execution forwards original SQL directly.
                        assignments: Vec::new(),
                        original_sql: Some(merge.original_sql.clone()),
                    },
                    physical_inputs.to_vec(),
                    session_state,
                )
                .await
                .map(Some);
        }

        Ok(None)
    }
}
