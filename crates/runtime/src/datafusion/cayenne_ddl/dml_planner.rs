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

//! Extension planner for distributed Cayenne DML nodes and local MERGE.
//!
//! DDL nodes are handled by `datafusion_ddl::DdlExtensionPlanner`.
//! This planner handles:
//! - [`DistributedCayenneDeleteNode`] → [`DistributedCayenneDeleteExec`]
//! - [`DistributedCayenneUpdateNode`] → [`DistributedCayenneUpdateExec`]
//! - [`DistributedCayenneInsertNode`] → [`DistributedCayenneInsertExec`]
//! - [`DistributedCayenneMergeNode`] → [`DistributedCayenneMergeExec`]
//! - [`CayenneMergeNode`] → [`CayenneMergeExec`] (local delete+insert merge)

use std::sync::Arc;

use async_trait::async_trait;
use cayenne::ddl::physical_plans::CayenneMergeExec;
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
use crate::datafusion::planner::logical_nodes::CayenneMergeNode;

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
use crate::datafusion::{SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA};

/// Extension planner for distributed Cayenne DML and local MERGE.
#[derive(Debug)]
pub struct CayenneDmlExtensionPlanner {
    executor_registry: Arc<ExecutorRegistry>,
    io_runtime: Option<tokio::runtime::Handle>,
}

impl CayenneDmlExtensionPlanner {
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
impl ExtensionPlanner for CayenneDmlExtensionPlanner {
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
                self.executor_registry.clone(),
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
                self.executor_registry.clone(),
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
                self.executor_registry.clone(),
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
                self.executor_registry.clone(),
                ctx,
            ))));
        }

        if let Some(merge) = node.as_any().downcast_ref::<CayenneMergeNode>() {
            return plan_local_merge(merge, session_state).await;
        }

        Ok(None)
    }
}

/// Build the physical plan for a local (single-node) `CayenneMergeNode`.
async fn plan_local_merge(
    merge: &CayenneMergeNode,
    session_state: &datafusion::execution::SessionState,
) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
    use std::collections::HashMap;

    use datafusion::datasource::provider_as_source;
    use datafusion::logical_expr::LogicalPlanBuilder;
    use datafusion::prelude::{Column, Expr, JoinType, col};

    async fn resolve_table(
        session_state: &datafusion::execution::SessionState,
        table_ref: &datafusion::sql::TableReference,
    ) -> DFResult<Arc<dyn datafusion::datasource::TableProvider>> {
        let catalog_name = table_ref.catalog().unwrap_or(SPICE_DEFAULT_CATALOG);
        let schema_name = table_ref.schema().unwrap_or(SPICE_DEFAULT_SCHEMA);
        let table_name = table_ref.table();
        let catalog = session_state
            .catalog_list()
            .catalog(catalog_name)
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Plan(format!(
                    "Catalog '{catalog_name}' not found"
                ))
            })?;
        let schema = catalog.schema(schema_name).ok_or_else(|| {
            datafusion::error::DataFusionError::Plan(format!("Schema '{schema_name}' not found"))
        })?;
        schema
            .table(table_name)
            .await
            .map_err(|e| {
                datafusion::error::DataFusionError::Plan(format!(
                    "Failed to resolve table '{table_ref}': {e}"
                ))
            })?
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Plan(format!("Table '{table_ref}' not found"))
            })
    }

    let target_provider = resolve_table(session_state, &merge.target_table).await?;
    let source_provider = resolve_table(session_state, &merge.source_table).await?;

    let target_qualifier = merge.target_qualifier.as_str();
    let source_qualifier = merge.source_qualifier.as_str();

    let target_scan = LogicalPlanBuilder::scan(
        target_qualifier,
        provider_as_source(Arc::clone(&target_provider)),
        None,
    )?
    .build()?;
    let source_scan = LogicalPlanBuilder::scan(
        source_qualifier,
        provider_as_source(Arc::clone(&source_provider)),
        None,
    )?
    .build()?;

    let (left_keys, right_keys): (Vec<Column>, Vec<Column>) = merge
        .on_keys
        .iter()
        .map(|(t, s)| {
            (
                Column::new(Some(target_qualifier.to_string()), t),
                Column::new(Some(source_qualifier.to_string()), s),
            )
        })
        .unzip();
    let target_key_columns: Vec<String> = merge.on_keys.iter().map(|(t, _)| t.clone()).collect();

    let joined = LogicalPlanBuilder::from(target_scan)
        .join(source_scan, JoinType::Inner, (left_keys, right_keys), None)?
        .build()?;

    let assign_map: HashMap<&str, &str> = merge
        .assignments
        .iter()
        .map(|(c, e)| (c.as_str(), e.as_str()))
        .collect();
    let target_schema = target_provider.schema();
    let target_field_names: std::collections::HashSet<&str> = target_schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();

    for (col_name, _) in &merge.assignments {
        if !target_field_names.contains(col_name.as_str()) {
            return Err(datafusion::error::DataFusionError::Plan(format!(
                "MERGE SET column '{col_name}' does not exist in target table"
            )));
        }
    }

    let joined_schema = joined.schema();
    let project_exprs: Vec<Expr> = target_schema
        .fields()
        .iter()
        .map(|field| {
            let col_name = field.name();
            let expr = if let Some(value_sql) = assign_map.get(col_name.as_str()) {
                session_state.create_logical_expr(value_sql, joined_schema)?
            } else {
                col(Column::new(Some(target_qualifier.to_string()), col_name))
            };
            Ok(expr.alias(col_name))
        })
        .collect::<DFResult<Vec<_>>>()?;

    let projected = LogicalPlanBuilder::from(joined)
        .project(project_exprs)?
        .build()?;
    let join_physical = session_state.create_physical_plan(&projected).await?;

    Ok(Some(Arc::new(CayenneMergeExec::new(
        join_physical,
        target_provider,
        session_state.clone(),
        target_key_columns,
    ))))
}
