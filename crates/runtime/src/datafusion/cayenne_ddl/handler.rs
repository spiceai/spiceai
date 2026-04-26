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

//! [`DistributedCayenneDdlHandler`] — distributed Cayenne DDL handler for the runtime.
//!
//! Extends the single-node [`CayenneDdlHandler`] with:
//! - Detection of `ComposedCatalogProvider`-wrapped Cayenne catalogs.
//! - Broadcast physical plans that forward DDL/DML to executor nodes after
//!   performing the local operation.

use std::sync::Arc;

use datafusion::catalog::CatalogProviderList;
use datafusion::error::Result as DFResult;
use datafusion::execution::SessionState;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use datafusion_ddl::{CatalogDdlHandler, CreateSchemaParams, CreateTableParams, DropTableParams};

use super::get_cayenne_provider;
use super::physical_plans::{
    DistributedCayenneCreateSchemaExec, DistributedCayenneCreateTableExec,
    DistributedCayenneDropTableExec,
};
use crate::cluster::ExecutorRegistry;
use cayenne::ddl::operations;

/// Distributed Cayenne DDL handler.
///
/// Handles both direct `CayenneCatalogProvider` and the runtime's
/// `ComposedCatalogProvider` wrapper.  Produces broadcast physical plans that
/// call the same `cayenne::ddl::operations` functions as the single-node handler
/// and then forward DDL to all connected executor nodes.
#[derive(Debug)]
pub struct DistributedCayenneDdlHandler {
    pub executor_registry: Arc<ExecutorRegistry>,
}

impl DistributedCayenneDdlHandler {
    #[must_use]
    pub fn new(executor_registry: Arc<ExecutorRegistry>) -> Self {
        Self { executor_registry }
    }
}

impl CatalogDdlHandler for DistributedCayenneDdlHandler {
    fn name(&self) -> &'static str {
        "cayenne_distributed"
    }

    fn is_target_catalog(
        &self,
        catalog_name: &str,
        catalog_list: &Arc<dyn CatalogProviderList>,
    ) -> bool {
        catalog_list
            .catalog(catalog_name)
            .is_some_and(|c| get_cayenne_provider(c.as_ref()).is_some())
    }

    fn create_table_exec(
        &self,
        params: CreateTableParams,
        catalog_list: Arc<dyn CatalogProviderList>,
        session_state: &SessionState,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let table_ref = format!(
            "{}.{}.{}",
            params.catalog_name, params.schema_name, params.table_name
        );
        let Some(partition_expr_sql) = params.extension.partition_by else {
            return Err(datafusion::error::DataFusionError::Plan(format!(
                "Failed to create table {table_ref} (cayenne): PARTITION BY is required in distributed mode"
            )));
        };

        Ok(Arc::new(DistributedCayenneCreateTableExec::new(
            operations::CreateTableParams {
                table_name: params.table_name,
                schema_name: params.schema_name,
                catalog_name: params.catalog_name,
                arrow_schema: params.arrow_schema,
                primary_key: params.primary_key,
                partition_expr_sql: Some(partition_expr_sql.to_string()),
                if_not_exists: params.if_not_exists,
                like_source_table: params.like_source_table,
                ctx: Some(Arc::new(SessionContext::new_with_state(
                    session_state.clone(),
                ))),
            },
            catalog_list,
            Arc::clone(&self.executor_registry),
            Arc::clone(session_state.runtime_env()),
        )))
    }

    fn drop_table_exec(
        &self,
        params: DropTableParams,
        catalog_list: Arc<dyn CatalogProviderList>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(DistributedCayenneDropTableExec::new(
            params.table_name,
            params.if_exists,
            params.catalog_name,
            params.schema_name,
            catalog_list,
            Arc::clone(&self.executor_registry),
        )))
    }

    fn create_schema_exec(
        &self,
        params: CreateSchemaParams,
        catalog_list: Arc<dyn CatalogProviderList>,
        _session_state: &SessionState,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(DistributedCayenneCreateSchemaExec::new(
            params.schema_name,
            params.if_not_exists,
            params.catalog_name,
            catalog_list,
            Arc::clone(&self.executor_registry),
        )))
    }
}
