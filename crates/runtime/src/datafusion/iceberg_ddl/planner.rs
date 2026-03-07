/*
Copyright 2024-2025, Spice AI, Inc.

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

//! Extension planner that converts Iceberg DDL logical nodes into
//! physical execution plans.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::CatalogProviderList;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};

use super::SharedDataFusionRef;
use super::logical_nodes::{IcebergCreateTableNode, IcebergDropTableNode};
use super::physical_plans::{IcebergCreateTableExec, IcebergDropTableExec};

/// Extension planner for Iceberg DDL operations.
///
/// Holds a [`SharedDataFusionRef`] — a lazily-initialized weak reference to the
/// `DataFusion` instance — so that physical plans can access the runtime for
/// accelerated table creation.
#[derive(Debug)]
pub struct IcebergDdlExtensionPlanner {
    datafusion_ref: SharedDataFusionRef,
}

impl IcebergDdlExtensionPlanner {
    #[must_use]
    pub fn new(datafusion_ref: SharedDataFusionRef) -> Self {
        Self { datafusion_ref }
    }
}

#[async_trait]
impl ExtensionPlanner for IcebergDdlExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        _physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &datafusion::execution::SessionState,
    ) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
        let catalog_list = Arc::<dyn CatalogProviderList>::clone(session_state.catalog_list());

        // Resolve the weak DataFusion reference for physical plans
        let datafusion_weak = self.datafusion_ref.get().cloned().unwrap_or_default();

        if let Some(create) = node.as_any().downcast_ref::<IcebergCreateTableNode>() {
            return Ok(Some(Arc::new(IcebergCreateTableExec::new(
                Arc::clone(&create.catalog),
                create.namespace.clone(),
                create.table_name.clone(),
                Arc::clone(&create.arrow_schema),
                create.if_not_exists,
                create.or_replace,
                create.df_catalog_name.clone(),
                create.df_schema_name.clone(),
                catalog_list,
                create.acceleration.clone(),
                create.dataset_options.clone(),
                datafusion_weak,
            ))));
        }

        if let Some(drop) = node.as_any().downcast_ref::<IcebergDropTableNode>() {
            return Ok(Some(Arc::new(IcebergDropTableExec::new(
                Arc::clone(&drop.catalog),
                drop.namespace.clone(),
                drop.table_name.clone(),
                drop.if_exists,
                drop.df_catalog_name.clone(),
                drop.df_schema_name.clone(),
                catalog_list,
                datafusion_weak,
            ))));
        }

        Ok(None)
    }
}
