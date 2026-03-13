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

//! Cayenne DDL support: analyzer rule, logical nodes, extension planner,
//! and physical execution plans for `CREATE TABLE` / `DROP TABLE` / `CREATE SCHEMA` on
//! Cayenne-backed DDL-enabled catalogs.
//!
//! Reuses the shared DDL infrastructure from [`super::ddl`].

pub mod analyzer_rule;
pub mod logical_nodes;
pub mod physical_plans;
pub mod planner;

use datafusion::catalog::CatalogProvider;

use super::composed_catalog::ComposedCatalogProvider;
use crate::catalogconnector::PartitionAwareCatalog;
use crate::catalogconnector::cayenne::provider::CayenneCatalogProvider;

/// Check whether the given catalog provider is a Cayenne-backed catalog.
pub fn is_cayenne_catalog(provider: &dyn CatalogProvider) -> bool {
    if provider
        .as_any()
        .downcast_ref::<CayenneCatalogProvider>()
        .is_some()
    {
        return true;
    }
    if let Some(composed) = provider.as_any().downcast_ref::<ComposedCatalogProvider>() {
        return composed
            .external()
            .as_any()
            .downcast_ref::<CayenneCatalogProvider>()
            .is_some();
    }
    false
}

/// Extract the [`CayenneCatalogProvider`] reference from a `CatalogProvider`.
///
/// Handles both direct `CayenneCatalogProvider` and `ComposedCatalogProvider`
/// wrapping a `CayenneCatalogProvider`.
pub fn get_cayenne_provider(provider: &dyn CatalogProvider) -> Option<&CayenneCatalogProvider> {
    if let Some(cayenne) = provider.as_any().downcast_ref::<CayenneCatalogProvider>() {
        return Some(cayenne);
    }
    if let Some(composed) = provider.as_any().downcast_ref::<ComposedCatalogProvider>() {
        return composed
            .external()
            .as_any()
            .downcast_ref::<CayenneCatalogProvider>();
    }
    None
}

/// If the catalog provider implements [`PartitionAwareCatalog`], return a trait reference.
///
/// Handles both direct providers and `ComposedCatalogProvider` wrappers.
pub fn as_partition_aware(provider: &dyn CatalogProvider) -> Option<&dyn PartitionAwareCatalog> {
    if let Some(cayenne) = provider.as_any().downcast_ref::<CayenneCatalogProvider>() {
        return Some(cayenne);
    }
    if let Some(composed) = provider.as_any().downcast_ref::<ComposedCatalogProvider>() {
        if let Some(cayenne) = composed
            .external()
            .as_any()
            .downcast_ref::<CayenneCatalogProvider>()
        {
            return Some(cayenne);
        }
    }
    None
}
