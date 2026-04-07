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
//! - A [`DdlLog`] that records every successfully applied DDL statement so that
//!   late-joining executors can replay the full log via `GetAppDefinition`.

use std::sync::Arc;

use cayenne::ddl::physical_plans::CayenneCreateSchemaExec;
use datafusion::catalog::CatalogProviderList;
use datafusion::error::Result as DFResult;
use datafusion::execution::SessionState;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use datafusion_ddl::{CatalogDdlHandler, CreateSchemaParams, CreateTableParams, DropTableParams};

use super::get_cayenne_provider;
use super::physical_plans::{CayenneCreateTableExec, CayenneDropTableExec};
use crate::cluster::executor_registry::ExecutorRegistry;
use crate::cluster::DdlLog;
use cayenne::ddl::operations;

/// Distributed Cayenne DDL handler.
///
/// Handles both direct `CayenneCatalogProvider` and the runtime's
/// `ComposedCatalogProvider` wrapper.  Produces broadcast physical plans that
/// call the same `cayenne::ddl::operations` functions as the single-node handler
/// and then forward DDL to all connected executor nodes.
///
/// When `ddl_log` is `Some` (scheduler mode), each physical plan appends the
/// canonical DDL SQL to the log after successful execution so that executors
/// joining after the fact can replay the full history via `GetAppDefinition`.
#[derive(Debug)]
pub struct DistributedCayenneDdlHandler {
    pub executor_registry: Option<Arc<ExecutorRegistry>>,
    pub io_runtime: Option<tokio::runtime::Handle>,
    pub ddl_log: Option<Arc<DdlLog>>,
}

impl DistributedCayenneDdlHandler {
    #[must_use]
    pub fn new(
        executor_registry: Option<Arc<ExecutorRegistry>>,
        io_runtime: Option<tokio::runtime::Handle>,
        ddl_log: Option<Arc<DdlLog>>,
    ) -> Self {
        Self {
            executor_registry,
            io_runtime,
            ddl_log,
        }
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
            .map(|c| get_cayenne_provider(c.as_ref()).is_some())
            .unwrap_or(false)
    }

    fn create_table_exec(
        &self,
        params: CreateTableParams,
        catalog_list: Arc<dyn CatalogProviderList>,
        session_state: &SessionState,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(CayenneCreateTableExec::new(
            operations::CreateTableParams {
                table_name: params.table_name,
                schema_name: params.schema_name,
                catalog_name: params.catalog_name,
                arrow_schema: params.arrow_schema,
                primary_key: params.primary_key,
                partition_expr_sql: params.extension.partition_by.map(|e| e.to_string()),
                if_not_exists: params.if_not_exists,
                like_source_table: params.like_source_table,
                ctx: Some(Arc::new(SessionContext::new_with_state(
                    session_state.clone(),
                ))),
            },
            catalog_list,
            self.executor_registry.clone(),
            self.ddl_log.clone(),
            Arc::clone(session_state.runtime_env()),
        )))
    }

    fn drop_table_exec(
        &self,
        params: DropTableParams,
        catalog_list: Arc<dyn CatalogProviderList>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(CayenneDropTableExec::new(
            params.table_name,
            params.if_exists,
            params.catalog_name,
            params.schema_name,
            catalog_list,
            self.executor_registry.clone(),
            self.ddl_log.clone(),
        )))
    }

    fn create_schema_exec(
        &self,
        params: CreateSchemaParams,
        catalog_list: Arc<dyn CatalogProviderList>,
        _session_state: &SessionState,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // Schema creation is identical in single-node and broadcast mode.
        // Pass the DDL log so the exec can record the CREATE SCHEMA statement.
        Ok(Arc::new(CayenneCreateSchemaExec::new(
            params.schema_name,
            params.if_not_exists,
            params.catalog_name,
            catalog_list,
            self.ddl_log.clone(),
        )))
    }
}
