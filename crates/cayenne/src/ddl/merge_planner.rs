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

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use datafusion_common::{DataFusionError, ResolvedTableReference};
use std::collections::HashMap;

use datafusion::datasource::provider_as_source;
use datafusion::logical_expr::LogicalPlanBuilder;
use datafusion::prelude::{Column, Expr, JoinType, col};

use super::logical_nodes::CayenneMergeNode;
use super::physical_plans::CayenneMergeExec;

/// Stateless extension planner for local (single-node) Cayenne MERGE.
///
/// Handles [`CayenneMergeNode`] → [`CayenneMergeExec`]. Always registered;
/// does not require a distributed executor registry.
#[derive(Debug)]
pub struct CayenneDmlExtensionPlanner {
    default_catalog: &'static str,
    default_schema: &'static str,
}

impl CayenneDmlExtensionPlanner {
    /// Creates a [`CayenneDmlExtensionPlanner`] that assumes [`TableReference`] that are not fully
    /// resolved have `default_catalog` and `default_schema`.
    #[must_use]
    pub fn new(default_catalog: &'static str, default_schema: &'static str) -> Self {
        Self {
            default_catalog,
            default_schema,
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
        if let Some(merge) = node.as_any().downcast_ref::<CayenneMergeNode>() {
            return plan_local_merge(
                merge,
                session_state,
                self.default_catalog,
                self.default_schema,
            )
            .await;
        }
        Ok(None)
    }
}

/// Build the physical plan for a local (single-node) [`CayenneMergeNode`].
async fn plan_local_merge(
    merge: &CayenneMergeNode,
    session_state: &datafusion::execution::SessionState,
    default_catalog: &str,
    default_schema: &str,
) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
    let target_provider = resolve_table(
        session_state,
        &merge.target_table,
        default_catalog,
        default_schema,
    )
    .await
    .ok_or(DataFusionError::Plan(format!(
        "Table {} not found",
        merge.target_table
    )))?;
    let source_provider = resolve_table(
        session_state,
        &merge.source_table,
        default_catalog,
        default_schema,
    )
    .await
    .ok_or(DataFusionError::Plan(format!(
        "Table {} not found",
        merge.source_table
    )))?;

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
