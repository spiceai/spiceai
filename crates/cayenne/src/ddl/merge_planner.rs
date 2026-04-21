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

//! [`CayenneDmlExtensionPlanner`] — stateless extension planner for local
//! (single-node) Cayenne MERGE.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::datasource::provider_as_source;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::LogicalPlanBuilder;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use datafusion::prelude::{Column, Expr, JoinType, col};
use datafusion_common::ResolvedTableReference;
use datafusion_dml::{CatalogDmlHandler, MergeParams};

use super::logical_nodes::CayenneMergeNode;
use super::physical_plans::CayenneMergeExec;
use crate::provider::position_tracking::{
    CayennePositionTrackingTable, POSITION_FILE_PATH_COLUMN, POSITION_ROW_IDX_COLUMN,
    is_position_based_cayenne,
};

/// Catalog DML handler for local (single-node) Cayenne operations.
///
/// This handler intentionally overlays only `MERGE`. Other DML operations
/// use the default `CatalogDmlHandler` implementations, which delegate to
/// standard `DataFusion` DML machinery.
#[derive(Debug)]
pub struct CayenneDmlHandler {
    default_catalog: &'static str,
    default_schema: &'static str,
}

impl CayenneDmlHandler {
    /// Creates a new local Cayenne DML handler using the provided default
    /// catalog and schema for unresolved table references.
    #[must_use]
    pub fn new(default_catalog: &'static str, default_schema: &'static str) -> Self {
        Self {
            default_catalog,
            default_schema,
        }
    }
}

#[async_trait]
impl CatalogDmlHandler for CayenneDmlHandler {
    fn name(&self) -> &'static str {
        "cayenne"
    }

    async fn merge_exec(
        &self,
        params: MergeParams,
        physical_inputs: Vec<Arc<dyn ExecutionPlan>>,
        session_state: &datafusion::execution::SessionState,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let [join_physical] = physical_inputs.as_slice() else {
            return Err(DataFusionError::Internal(
                "Local MERGE requires exactly one physical input".to_string(),
            ));
        };

        let target_provider = resolve_table(
            session_state,
            &params.target_table,
            self.default_catalog,
            self.default_schema,
        )
        .await
        .ok_or(DataFusionError::Plan(format!(
            "Table {} not found",
            params.target_table
        )))?;

        let target_key_columns: Vec<String> = params
            .on_keys
            .iter()
            .map(|(target, _)| target.clone())
            .collect();

        Ok(Arc::new(CayenneMergeExec::new(
            Arc::clone(join_physical),
            target_provider,
            session_state.clone(),
            target_key_columns,
        )))
    }
}

/// Stateless extension planner for local (single-node) Cayenne MERGE.
///
/// Handles [`CayenneMergeNode`] by building the joined/projection physical
/// input and delegating final execution-plan construction to
/// [`CayenneDmlHandler`].
#[derive(Debug)]
pub struct CayenneDmlExtensionPlanner {
    handler: Arc<CayenneDmlHandler>,
}

impl CayenneDmlExtensionPlanner {
    /// Creates a [`CayenneDmlExtensionPlanner`] that assumes [`TableReference`] that are not fully
    /// resolved have `default_catalog` and `default_schema`.
    #[must_use]
    pub fn new(default_catalog: &'static str, default_schema: &'static str) -> Self {
        Self {
            handler: Arc::new(CayenneDmlHandler::new(default_catalog, default_schema)),
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
        _physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &datafusion::execution::SessionState,
    ) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
        let Some(merge) = node.as_any().downcast_ref::<CayenneMergeNode>() else {
            return Ok(None);
        };

        let (params, join_physical) =
            build_local_merge_input(merge, session_state, self.handler.as_ref()).await?;

        self.handler
            .merge_exec(params, vec![join_physical], session_state)
            .await
            .map(Some)
    }
}

/// Build the joined+projected physical input and typed params for a local
/// [`CayenneMergeNode`].
async fn build_local_merge_input(
    merge: &CayenneMergeNode,
    session_state: &datafusion::execution::SessionState,
    handler: &CayenneDmlHandler,
) -> DFResult<(MergeParams, Arc<dyn ExecutionPlan>)> {
    let target_provider = resolve_table(
        session_state,
        &merge.target_table,
        handler.default_catalog,
        handler.default_schema,
    )
    .await
    .ok_or(DataFusionError::Plan(format!(
        "Table {} not found",
        merge.target_table
    )))?;
    let source_provider = resolve_table(
        session_state,
        &merge.source_table,
        handler.default_catalog,
        handler.default_schema,
    )
    .await
    .ok_or(DataFusionError::Plan(format!(
        "Table {} not found",
        merge.source_table
    )))?;

    let target_qualifier = merge.target_qualifier.as_str();
    let source_qualifier = merge.source_qualifier.as_str();

    let track_positions = is_position_based_cayenne(&target_provider).await;
    let scan_provider: Arc<dyn datafusion::datasource::TableProvider> = if track_positions {
        Arc::new(CayennePositionTrackingTable::try_new(Arc::clone(
            &target_provider,
        ))?)
    } else {
        Arc::clone(&target_provider)
    };

    let target_scan =
        LogicalPlanBuilder::scan(target_qualifier, provider_as_source(scan_provider), None)?
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
        .map(|(target, source)| {
            (
                Column::new(Some(target_qualifier.to_string()), target),
                Column::new(Some(source_qualifier.to_string()), source),
            )
        })
        .unzip();

    let joined = LogicalPlanBuilder::from(target_scan)
        .join(source_scan, JoinType::Inner, (left_keys, right_keys), None)?
        .build()?;

    let target_schema = target_provider.schema();
    let target_field_names: std::collections::HashSet<&str> = target_schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();

    for (column_name, _) in &merge.assignments {
        if !target_field_names.contains(column_name.as_str()) {
            return Err(DataFusionError::Plan(format!(
                "MERGE SET column '{column_name}' does not exist in target table"
            )));
        }
    }

    let joined_schema = joined.schema();
    let assignments: Vec<(String, Expr)> = merge
        .assignments
        .iter()
        .map(|(column, value_sql)| {
            let expr = session_state.create_logical_expr(value_sql, joined_schema)?;
            Ok((column.clone(), expr))
        })
        .collect::<DFResult<Vec<_>>>()?;

    let assignment_map: HashMap<&str, &Expr> = assignments
        .iter()
        .map(|(column, expr)| (column.as_str(), expr))
        .collect();

    let mut project_exprs: Vec<Expr> = target_schema
        .fields()
        .iter()
        .map(|field| {
            let col_name = field.name();
            let expr = if let Some(assignment) = assignment_map.get(col_name.as_str()) {
                (*assignment).clone()
            } else {
                col(Column::new(Some(target_qualifier.to_string()), col_name))
            };
            Ok(expr.alias(col_name))
        })
        .collect::<DFResult<Vec<_>>>()?;

    if track_positions {
        project_exprs.push(
            col(Column::new(
                Some(target_qualifier.to_string()),
                POSITION_FILE_PATH_COLUMN,
            ))
            .alias(POSITION_FILE_PATH_COLUMN),
        );
        project_exprs.push(
            col(Column::new(
                Some(target_qualifier.to_string()),
                POSITION_ROW_IDX_COLUMN,
            ))
            .alias(POSITION_ROW_IDX_COLUMN),
        );
    }

    let projected = LogicalPlanBuilder::from(joined)
        .project(project_exprs)?
        .build()?;

    let join_physical = session_state.create_physical_plan(&projected).await?;

    let params = MergeParams {
        target_table: merge.target_table.clone(),
        source_table: merge.source_table.clone(),
        target_qualifier: merge.target_qualifier.clone(),
        source_qualifier: merge.source_qualifier.clone(),
        on_keys: merge.on_keys.clone(),
        assignments,
        original_sql: None,
    };

    Ok((params, join_physical))
}

async fn resolve_table(
    session_state: &datafusion::execution::SessionState,
    table_ref: &datafusion::sql::TableReference,
    default_catalog: &str,
    default_schema: &str,
) -> Option<Arc<dyn datafusion::datasource::TableProvider>> {
    let ResolvedTableReference {
        catalog,
        schema,
        table,
    } = table_ref.clone().resolve(default_catalog, default_schema);
    let catalog = session_state.catalog_list().catalog(&catalog)?;
    let schema = catalog.schema(&schema)?;
    schema.table(&table).await.ok()?
}
