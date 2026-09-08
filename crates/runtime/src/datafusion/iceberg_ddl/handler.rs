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

//! [`IcebergDdlHandler`] — [`CatalogDdlHandler`] implementation for Iceberg catalogs.

use std::sync::{Arc, Weak};

use data_components::iceberg::provider::IcebergCatalogProvider;
use datafusion::catalog::CatalogProviderList;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::SessionState;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_ddl::{CatalogDdlHandler, CreateSchemaParams, CreateTableParams, DropTableParams};

use super::SharedDataFusionRef;
use super::composed_catalog_to_iceberg;
use super::physical_plans::{
    IcebergCreateSchemaExec, IcebergCreateTableExec, IcebergDropTableExec,
};

/// Iceberg DDL handler.
///
/// Detects Iceberg catalogs (direct or via `ComposedCatalogProvider`).
/// Holds a lazily-initialised [`SharedDataFusionRef`] for registering accelerated
/// tables after creation — populated by [`DataFusion::set_self_ref`] after the
/// `DataFusion` struct is wrapped in an `Arc`.
#[derive(Debug, Clone)]
pub struct IcebergDdlHandler {
    datafusion_ref: SharedDataFusionRef,
}

impl IcebergDdlHandler {
    #[must_use]
    pub fn new(datafusion_ref: SharedDataFusionRef) -> Self {
        Self { datafusion_ref }
    }

    fn datafusion_weak(&self) -> Weak<crate::datafusion::DataFusion> {
        self.datafusion_ref.get().cloned().unwrap_or_default()
    }

    fn get_iceberg_catalog(
        catalog_name: &str,
        catalog_list: &Arc<dyn CatalogProviderList>,
    ) -> Option<Arc<dyn iceberg::Catalog>> {
        let df_catalog = catalog_list.catalog(catalog_name)?;
        if let Some(p) = df_catalog.downcast_ref::<IcebergCatalogProvider>() {
            return Some(Arc::clone(p.catalog()));
        }
        composed_catalog_to_iceberg(df_catalog.as_ref())
    }
}

impl CatalogDdlHandler for IcebergDdlHandler {
    fn name(&self) -> &'static str {
        "iceberg"
    }

    fn is_target_catalog(
        &self,
        catalog_name: &str,
        catalog_list: &Arc<dyn CatalogProviderList>,
    ) -> bool {
        Self::get_iceberg_catalog(catalog_name, catalog_list).is_some()
    }

    fn create_table_exec(
        &self,
        params: CreateTableParams,
        catalog_list: Arc<dyn CatalogProviderList>,
        _session_state: &SessionState,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let iceberg_catalog = Self::get_iceberg_catalog(&params.catalog_name, &catalog_list)
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "Catalog '{}' is not an Iceberg catalog",
                    params.catalog_name
                ))
            })?;
        let namespace = iceberg::NamespaceIdent::new(params.schema_name.clone());
        let dataset_options = params.extension.dataset.clone();
        let acceleration = params.extension.acceleration.clone();
        let partition_expr_sql = params.extension.partition_by.map(|e| e.to_string());

        Ok(Arc::new(IcebergCreateTableExec::new(
            iceberg_catalog,
            namespace,
            params.table_name,
            params.arrow_schema,
            params.if_not_exists,
            params.or_replace,
            params.catalog_name,
            params.schema_name,
            catalog_list,
            acceleration,
            dataset_options,
            partition_expr_sql,
            self.datafusion_weak(),
        )))
    }

    fn drop_table_exec(
        &self,
        params: DropTableParams,
        catalog_list: Arc<dyn CatalogProviderList>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let iceberg_catalog = Self::get_iceberg_catalog(&params.catalog_name, &catalog_list)
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "Catalog '{}' is not an Iceberg catalog",
                    params.catalog_name
                ))
            })?;
        let namespace = iceberg::NamespaceIdent::new(params.schema_name.clone());
        Ok(Arc::new(IcebergDropTableExec::new(
            iceberg_catalog,
            namespace,
            params.table_name,
            params.if_exists,
            params.catalog_name,
            params.schema_name,
            catalog_list,
            self.datafusion_weak(),
        )))
    }

    fn create_schema_exec(
        &self,
        params: CreateSchemaParams,
        catalog_list: Arc<dyn CatalogProviderList>,
        _session_state: &SessionState,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let iceberg_catalog = Self::get_iceberg_catalog(&params.catalog_name, &catalog_list)
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "Catalog '{}' is not an Iceberg catalog",
                    params.catalog_name
                ))
            })?;
        let namespace = iceberg::NamespaceIdent::new(params.schema_name.clone());
        Ok(Arc::new(IcebergCreateSchemaExec::new(
            iceberg_catalog,
            namespace,
            params.if_not_exists,
            params.catalog_name,
            params.schema_name,
            catalog_list,
            self.datafusion_weak(),
        )))
    }
}
