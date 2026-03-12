/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

//! Analyzer rule that intercepts DDL plans (`CREATE TABLE` / `DROP TABLE`)
//! targeting Iceberg-backed DDL-enabled catalogs and rewrites them into
//! custom [`LogicalPlan::Extension`] nodes.

use std::collections::HashSet;
use std::fmt;
use std::sync::{Arc, RwLock, Weak};

use datafusion::catalog::CatalogProviderList;
use datafusion::config::ConfigOptions;
use datafusion::error::DataFusionError;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::DdlStatement;
use datafusion::logical_expr::{Extension, LogicalPlan};
use datafusion::optimizer::AnalyzerRule;

use data_components::iceberg::provider::IcebergCatalogProvider;

use super::acceleration_options::{DatasetOptions, SharedDdlExtensionStore};
use super::composed_catalog_to_iceberg;
use super::logical_nodes::{IcebergCreateTableNode, IcebergDropTableNode};
use crate::datafusion::{SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA};

/// Analyzer rule that rewrites DDL targeting Iceberg catalogs into
/// custom extension nodes that perform async Iceberg catalog operations.
///
/// Uses `Weak` references to avoid reference cycles: the `SessionContext`
/// owns the analyzer rules, so holding `Arc` refs back to the session's
/// catalog list or to `DataFusion`'s `ddl_enabled_catalogs` would create
/// a cycle that prevents cleanup.
pub struct IcebergDdlAnalyzerRule {
    /// Weak reference to the catalog list for catalog resolution.
    catalog_list: Weak<dyn CatalogProviderList>,
    /// Weak reference to the set of DDL-enabled catalog names.
    ddl_enabled_catalogs: Weak<RwLock<HashSet<String>>>,
    /// Shared store for DDL extensions extracted from `CREATE TABLE` statements.
    ddl_options: SharedDdlExtensionStore,
}

impl fmt::Debug for IcebergDdlAnalyzerRule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IcebergDdlAnalyzerRule")
            .finish_non_exhaustive()
    }
}

impl IcebergDdlAnalyzerRule {
    #[must_use]
    pub fn new(
        catalog_list: &Arc<dyn CatalogProviderList>,
        ddl_enabled_catalogs: &Arc<RwLock<HashSet<String>>>,
        ddl_options: SharedDdlExtensionStore,
    ) -> Self {
        Self {
            catalog_list: Arc::downgrade(catalog_list),
            ddl_enabled_catalogs: Arc::downgrade(ddl_enabled_catalogs),
            ddl_options,
        }
    }

    fn is_ddl_enabled(&self, catalog_name: &str) -> bool {
        self.ddl_enabled_catalogs
            .upgrade()
            .and_then(|catalogs| catalogs.read().ok().map(|set| set.contains(catalog_name)))
            .unwrap_or(false)
    }

    /// Try to get the Iceberg catalog from a `DataFusion` catalog name.
    /// Handles both direct `IcebergCatalogProvider` and `ComposedCatalogProvider`
    /// wrapping an `IcebergCatalogProvider`.
    fn get_iceberg_catalog(&self, catalog_name: &str) -> Option<Arc<dyn iceberg::Catalog>> {
        let catalog_list = self.catalog_list.upgrade()?;
        let df_catalog = catalog_list.catalog(catalog_name)?;

        // Try direct downcast first
        if let Some(iceberg_provider) = df_catalog.as_any().downcast_ref::<IcebergCatalogProvider>()
        {
            return Some(Arc::clone(iceberg_provider.catalog()));
        }

        // Try via ComposedCatalogProvider
        composed_catalog_to_iceberg(df_catalog.as_ref())
    }
}

impl AnalyzerRule for IcebergDdlAnalyzerRule {
    fn name(&self) -> &'static str {
        "iceberg_ddl_rewrite"
    }

    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> DFResult<LogicalPlan> {
        match &plan {
            LogicalPlan::Ddl(DdlStatement::CreateMemoryTable(create)) => {
                let catalog_name = create
                    .name
                    .catalog()
                    .unwrap_or(SPICE_DEFAULT_CATALOG)
                    .to_string();

                if !self.is_ddl_enabled(&catalog_name) {
                    return Ok(plan);
                }

                let Some(iceberg_catalog) = self.get_iceberg_catalog(&catalog_name) else {
                    return Ok(plan);
                };

                let schema_name = create
                    .name
                    .schema()
                    .unwrap_or(SPICE_DEFAULT_SCHEMA)
                    .to_string();
                let table_name = create.name.table().to_string();
                let acceleration_key = create.name.to_string();

                // Extract the Arrow schema from the logical plan's input
                let arrow_schema = Arc::new(create.input.schema().inner().as_ref().clone());

                let namespace = iceberg::NamespaceIdent::new(schema_name.clone());

                // Look up DDL extensions from the store (consumed on use)
                let (acceleration, dataset_options) = {
                    let mut store = self.ddl_options.write().map_err(|e| {
                        DataFusionError::Execution(format!(
                            "Failed to acquire DDL extension store lock: {e}"
                        ))
                    })?;
                    match store.remove(&acceleration_key) {
                        Some(ext) => (ext.acceleration, ext.dataset),
                        None => (None, DatasetOptions::default()),
                    }
                };

                let node = IcebergCreateTableNode::new(
                    iceberg_catalog,
                    namespace,
                    table_name,
                    arrow_schema,
                    create.if_not_exists,
                    create.or_replace,
                    catalog_name,
                    schema_name,
                    acceleration,
                    dataset_options,
                );

                Ok(LogicalPlan::Extension(Extension {
                    node: Arc::new(node),
                }))
            }
            LogicalPlan::Ddl(DdlStatement::DropTable(drop)) => {
                let catalog_name = drop
                    .name
                    .catalog()
                    .unwrap_or(SPICE_DEFAULT_CATALOG)
                    .to_string();

                if !self.is_ddl_enabled(&catalog_name) {
                    return Ok(plan);
                }

                let Some(iceberg_catalog) = self.get_iceberg_catalog(&catalog_name) else {
                    return Ok(plan);
                };

                let schema_name = drop
                    .name
                    .schema()
                    .unwrap_or(SPICE_DEFAULT_SCHEMA)
                    .to_string();
                let table_name = drop.name.table().to_string();

                let namespace = iceberg::NamespaceIdent::new(schema_name.clone());

                let node = IcebergDropTableNode::new(
                    iceberg_catalog,
                    namespace,
                    table_name,
                    drop.if_exists,
                    catalog_name,
                    schema_name,
                );

                Ok(LogicalPlan::Extension(Extension {
                    node: Arc::new(node),
                }))
            }
            _ => Ok(plan),
        }
    }
}
