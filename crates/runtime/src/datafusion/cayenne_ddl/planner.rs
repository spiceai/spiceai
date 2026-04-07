/*
Copyright 2026 The Spice.ai OSS Authors

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

//! Extension planner that converts Cayenne DDL logical nodes into
//! physical execution plans.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::CatalogProviderList;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};

use super::logical_nodes::{
    CayenneCreateSchemaNode, CayenneCreateTableNode, CayenneDropTableNode,
    DistributedCayenneDeleteNode, DistributedCayenneInsertNode, DistributedCayenneMergeNode,
    DistributedCayenneUpdateNode,
};
use super::physical_plans::{
    CayenneCreateSchemaExec, CayenneCreateTableExecBuilder, CayenneDropTableExec,
    DistributedCayenneDeleteExec, DistributedCayenneInsertExec, DistributedCayenneMergeExec,
    DistributedCayenneUpdateExec,
};
use crate::cluster::executor_registry::ExecutorRegistry;

/// Extension planner for Cayenne DDL operations.
///
/// When an [`ExecutorRegistry`] is provided (scheduler mode), the physical
/// plans will forward DDL statements to executor nodes after local execution.
#[derive(Debug)]
pub struct CayenneDdlExtensionPlanner {
    executor_registry: Option<Arc<ExecutorRegistry>>,
    io_runtime: Option<tokio::runtime::Handle>,
}

impl CayenneDdlExtensionPlanner {
    #[must_use]
    pub fn new(
        executor_registry: Option<Arc<ExecutorRegistry>>,
        io_runtime: Option<tokio::runtime::Handle>,
    ) -> Self {
        Self {
            executor_registry,
            io_runtime,
        }
    }
}

impl Default for CayenneDdlExtensionPlanner {
    fn default() -> Self {
        Self::new(None, None)
    }
}

#[async_trait]
impl ExtensionPlanner for CayenneDdlExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        _physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &datafusion::execution::SessionState,
    ) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
        let catalog_list = Arc::<dyn CatalogProviderList>::clone(session_state.catalog_list());

        if let Some(create) = node.as_any().downcast_ref::<CayenneCreateTableNode>() {
            return Ok(Some(Arc::new(
                CayenneCreateTableExecBuilder::new(
                    create.table_name.clone(),
                    Arc::clone(&create.arrow_schema),
                    create.df_catalog_name.clone(),
                    create.df_schema_name.clone(),
                    create.primary_key.clone(),
                    catalog_list,
                )
                .if_not_exists(create.if_not_exists)
                .executor_registry(self.executor_registry.clone())
                .partition_expr(create.partition_expr.clone())
                .partition_expr_sql(create.partition_expr_sql.clone())
                .like_source_table(create.like_source_table.clone())
                .ctx(Some(Arc::new(
                    datafusion::prelude::SessionContext::new_with_state(session_state.clone()),
                )))
                .build(),
            )));
        }

        if let Some(create_schema) = node.as_any().downcast_ref::<CayenneCreateSchemaNode>() {
            return Ok(Some(Arc::new(CayenneCreateSchemaExec::new(
                create_schema.schema_name.clone(),
                create_schema.if_not_exists,
                create_schema.df_catalog_name.clone(),
                catalog_list,
            ))));
        }

        if let Some(drop) = node.as_any().downcast_ref::<CayenneDropTableNode>() {
            return Ok(Some(Arc::new(CayenneDropTableExec::new(
                drop.table_name.clone(),
                drop.if_exists,
                drop.df_catalog_name.clone(),
                drop.df_schema_name.clone(),
                catalog_list,
                self.executor_registry.clone(),
            ))));
        }

        if let Some(delete) = node.as_any().downcast_ref::<DistributedCayenneDeleteNode>() {
            let input = _physical_inputs.first().ok_or_else(|| {
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
            let input = _physical_inputs.first().ok_or_else(|| {
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
            let input = _physical_inputs.first().ok_or_else(|| {
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

        if let Some(merge) =
            node.as_any()
                .downcast_ref::<crate::datafusion::planner::logical_nodes::CayenneMergeNode>()
        {
            return plan_merge_extension(merge, session_state).await;
        }

        Ok(None)
    }
}

/// Build the physical plan for a `CayenneMergeNode`.
///
/// Constructs a `DataFusion` INNER JOIN between target and source tables,
/// projects the SET assignment expressions over the joined schema, and
/// wraps the result in a [`CayenneMergeExec`] that executes delete + insert.
async fn plan_merge_extension(
    merge: &crate::datafusion::planner::logical_nodes::CayenneMergeNode,
    session_state: &datafusion::execution::SessionState,
) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
    use std::collections::HashMap;

    use datafusion::datasource::provider_as_source;
    use datafusion::logical_expr::LogicalPlanBuilder;
    use datafusion::prelude::{Column, Expr, JoinType, col};

    use crate::datafusion::planner::physical_execs::CayenneMergeExec;
    use crate::datafusion::{SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA};

    // Resolve target and source table providers.
    async fn resolve_table(
        session_state: &datafusion::execution::SessionState,
        table_ref: &datafusion::sql::TableReference,
    ) -> DFResult<Arc<dyn datafusion::datasource::TableProvider>> {
        let catalog_name = table_ref.catalog().unwrap_or(SPICE_DEFAULT_CATALOG);
        let schema_name = table_ref.schema().unwrap_or(SPICE_DEFAULT_SCHEMA);
        let table_name = table_ref.table();

        let catalog_list = session_state.catalog_list();
        let catalog = catalog_list.catalog(catalog_name).ok_or_else(|| {
            datafusion::error::DataFusionError::Plan(format!("Catalog '{catalog_name}' not found"))
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

    // Use the qualifiers stored on the node — these are the alias (if
    // provided) or table name. Assignment value SQL references these
    // qualifiers, so the scan must match.
    let target_qualifier = merge.target_qualifier.as_str();
    let source_qualifier = merge.source_qualifier.as_str();

    // Build table scans.
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

    // Build INNER JOIN from pre-normalized key pairs.
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

    // Build projection: for each target column, use the assignment expression
    // or keep the original target column value.
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

    // Validate all assignment targets exist in the target schema.
    for (col_name, _) in &merge.assignments {
        if !target_field_names.contains(col_name.as_str()) {
            return Err(datafusion::error::DataFusionError::Plan(format!(
                "MERGE UPDATE SET target column '{col_name}' does not exist in target table"
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

    // Convert the logical plan to a physical plan.
    let join_physical = session_state.create_physical_plan(&projected).await?;

    Ok(Some(Arc::new(CayenneMergeExec::new(
        join_physical,
        target_provider,
        session_state.clone(),
        target_key_columns,
    ))))
}
