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

//! Iceberg DDL support: analyzer rule, logical nodes, extension planner,
//! and physical execution plans for `CREATE TABLE` / `DROP TABLE` on
//! Iceberg-backed catalogs.

pub mod analyzer_rule;
pub mod logical_nodes;
pub mod physical_plans;
pub mod planner;

use std::sync::Arc;

use data_components::iceberg::provider::IcebergCatalogProvider;
use datafusion::catalog::CatalogProvider;

use super::composed_catalog::ComposedCatalogProvider;

/// Try to extract the Iceberg catalog from a `CatalogProvider`.
/// Handles both direct `IcebergCatalogProvider` and `ComposedCatalogProvider`
/// wrapping an `IcebergCatalogProvider`.
pub fn composed_catalog_to_iceberg(
    provider: &dyn CatalogProvider,
) -> Option<Arc<dyn iceberg::Catalog>> {
    // Try direct downcast
    if let Some(iceberg) = provider.as_any().downcast_ref::<IcebergCatalogProvider>() {
        return Some(Arc::clone(iceberg.catalog()));
    }
    // Try via ComposedCatalogProvider
    if let Some(composed) = provider.as_any().downcast_ref::<ComposedCatalogProvider>()
        && let Some(iceberg) = composed
            .external()
            .as_any()
            .downcast_ref::<IcebergCatalogProvider>()
    {
        return Some(Arc::clone(iceberg.catalog()));
    }
    None
}
