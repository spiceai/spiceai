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

//! [`CayenneDdlHandler`] — single-node [`CatalogDdlHandler`] implementation for Cayenne.
//!
//! Produces [`CayenneCreateTableExec`], [`CayenneDropTableExec`], and
//! [`CayenneCreateSchemaExec`] physical plans.  No executor forwarding or
//! distributed partition metadata — those concerns belong in the runtime's
//! `DistributedCayenneDdlHandler`.

use std::sync::Arc;

use datafusion::catalog::CatalogProviderList;
use datafusion::error::Result as DFResult;
use datafusion::execution::SessionState;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use datafusion_ddl::{CatalogDdlHandler, CreateSchemaParams, CreateTableParams, DropTableParams};

use crate::catalog_provider::CayenneCatalogProvider;
use crate::ddl::operations;
use crate::ddl::physical_plans::{
    CayenneCreateSchemaExec, CayenneCreateTableExec, CayenneDropTableExec,
};

/// Handle DDL for Cayenne Catalog (via using `datafusion_ddl` machinery).
#[derive(Debug, Default)]
pub struct CayenneDdlHandler;

impl CatalogDdlHandler for CayenneDdlHandler {
    fn name(&self) -> &'static str {
        "cayenne"
    }

    fn is_target_catalog(
        &self,
        catalog_name: &str,
        catalog_list: &Arc<dyn CatalogProviderList>,
    ) -> bool {
        catalog_list.catalog(catalog_name).is_some_and(|c| {
            c.as_any()
                .downcast_ref::<CayenneCatalogProvider>()
                .is_some()
        })
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
        )))
    }

    fn create_schema_exec(
        &self,
        params: CreateSchemaParams,
        catalog_list: Arc<dyn CatalogProviderList>,
        _session_state: &SessionState,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(CayenneCreateSchemaExec::new(
            params.schema_name,
            params.if_not_exists,
            params.catalog_name,
            catalog_list,
        )))
    }
}
