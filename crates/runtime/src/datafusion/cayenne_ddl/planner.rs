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

use super::logical_nodes::{CayenneCreateSchemaNode, CayenneCreateTableNode, CayenneDropTableNode};
use super::physical_plans::{
    CayenneCreateSchemaExec, CayenneCreateTableExecBuilder, CayenneDropTableExec,
};
use crate::cluster::executor_registry::ExecutorRegistry;

/// Extension planner for Cayenne DDL operations.
///
/// When an [`ExecutorRegistry`] is provided (scheduler mode), the physical
/// plans will forward DDL statements to executor nodes after local execution.
#[derive(Debug)]
pub struct CayenneDdlExtensionPlanner {
    executor_registry: Option<Arc<ExecutorRegistry>>,
}

impl CayenneDdlExtensionPlanner {
    #[must_use]
    pub fn new(executor_registry: Option<Arc<ExecutorRegistry>>) -> Self {
        Self { executor_registry }
    }
}

impl Default for CayenneDdlExtensionPlanner {
    fn default() -> Self {
        Self::new(None)
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

        Ok(None)
    }
}
