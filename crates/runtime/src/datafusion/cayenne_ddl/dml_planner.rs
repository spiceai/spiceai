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

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};

use super::logical_nodes::{
    DistributedCayenneDeleteNode, DistributedCayenneInsertNode, DistributedCayenneMergeNode,
    DistributedCayenneUpdateNode,
};
use super::physical_plans::{
    DistributedCayenneDeleteExec, DistributedCayenneInsertExec, DistributedCayenneMergeExec,
    DistributedCayenneUpdateExec,
};
use crate::cluster::executor_registry::ExecutorRegistry;
// ── DML SQL extraction helpers ────────────────────────────────────────────────

/// Walk the input plan to find the topmost `Filter` and convert its predicate to SQL text.
///
/// Used by the distributed DELETE and UPDATE planners to reconstruct the WHERE clause
/// that will be forwarded verbatim to executor nodes.
pub fn extract_filter_sql(plan: &LogicalPlan) -> DFResult<Option<String>> {
    use datafusion::sql::unparser::expr_to_sql;
    match plan {
        LogicalPlan::Filter(filter) => {
            let ast = expr_to_sql(&filter.predicate)?;
            Ok(Some(ast.to_string()))
        }
        LogicalPlan::Projection(proj) => extract_filter_sql(&proj.input),
        _ => Ok(None),
    }
}

/// Extract `(column_name, value_sql)` assignment pairs from an UPDATE input plan.
///
/// The UPDATE input is a `Projection` over a (possibly filtered) `TableScan`.
/// Each projection expression is either:
/// - `col AS col` — identity (unchanged column), skipped.
/// - `<expr> AS col` — SET assignment, included.
pub fn extract_update_assignments(
    plan: &LogicalPlan,
    table_name: &datafusion::sql::TableReference,
) -> DFResult<Vec<(String, String)>> {
    use datafusion::prelude::Expr;
    use datafusion::sql::unparser::expr_to_sql;

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

        let ast = expr_to_sql(alias.expr.as_ref())?;
        assignments.push((col_name.clone(), ast.to_string()));
    }
    Ok(assignments)
}

// ── DistributedCayenneDmlExtensionPlanner ─────────────────────────────────────

/// Extension planner for distributed Cayenne DML nodes.
///
/// Handles DELETE, UPDATE, INSERT, and MERGE nodes that forward operations
/// to executor nodes. Only registered when an [`ExecutorRegistry`] is present.
#[derive(Debug)]
pub struct DistributedCayenneDmlExtensionPlanner {
    executor_registry: Arc<ExecutorRegistry>,
    io_runtime: Option<tokio::runtime::Handle>,
}

impl DistributedCayenneDmlExtensionPlanner {
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
impl ExtensionPlanner for DistributedCayenneDmlExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &datafusion::execution::SessionState,
    ) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
        if let Some(delete) = node.as_any().downcast_ref::<DistributedCayenneDeleteNode>() {
            let input = physical_inputs.first().ok_or_else(|| {
                datafusion::error::DataFusionError::Internal(
                    "DistributedCayenneDeleteNode requires exactly one physical input".to_string(),
                )
            })?;
            return Ok(Some(Arc::new(DistributedCayenneDeleteExec::new(
                delete.table_name.clone(),
                Arc::clone(&self.executor_registry),
                delete.filter_sql.clone(),
                Arc::clone(input),
            ))));
        }

        if let Some(update) = node.as_any().downcast_ref::<DistributedCayenneUpdateNode>() {
            let input = physical_inputs.first().ok_or_else(|| {
                datafusion::error::DataFusionError::Internal(
                    "DistributedCayenneUpdateNode requires exactly one physical input".to_string(),
                )
            })?;
            return Ok(Some(Arc::new(DistributedCayenneUpdateExec::new(
                update.table_name.clone(),
                Arc::clone(&self.executor_registry),
                update.filter_sql.clone(),
                update.assignments_sql.clone(),
                Arc::clone(input),
            ))));
        }

        if let Some(insert) = node.as_any().downcast_ref::<DistributedCayenneInsertNode>() {
            let input = physical_inputs.first().ok_or_else(|| {
                datafusion::error::DataFusionError::Internal(
                    "DistributedCayenneInsertNode requires exactly one physical input".to_string(),
                )
            })?;
            let io_runtime = self.io_runtime.clone().ok_or_else(|| {
                datafusion::error::DataFusionError::Internal(
                    "DistributedCayenneInsertExec requires an IO runtime handle".to_string(),
                )
            })?;
            let ctx = Arc::new(datafusion::prelude::SessionContext::new_with_state(
                session_state.clone(),
            ));
            return Ok(Some(Arc::new(DistributedCayenneInsertExec::new(
                insert.table_name.clone(),
                Arc::clone(&self.executor_registry),
                ctx,
                io_runtime,
                Arc::clone(input),
            ))));
        }

        if let Some(merge) = node.as_any().downcast_ref::<DistributedCayenneMergeNode>() {
            let ctx = Arc::new(datafusion::prelude::SessionContext::new_with_state(
                session_state.clone(),
            ));
            return Ok(Some(Arc::new(DistributedCayenneMergeExec::new(
                merge.target_table.clone(),
                merge.source_table.clone(),
                merge.on_keys.clone(),
                merge.original_sql.clone(),
                Arc::clone(&self.executor_registry),
                ctx,
            ))));
        }

        Ok(None)
    }
}
