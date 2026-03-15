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

use ::data_components::delete::DeletionTableProviderAdapter;
use cayenne::CayenneTableProvider;
use datafusion::catalog::{CatalogProvider, TableProvider};
use runtime_table_partition::provider::PartitionTableProvider;
use vortex::session::SessionVar;

use super::composed_catalog::ComposedCatalogProvider;
use crate::catalogconnector::cayenne::provider::CayenneCatalogProvider;
use crate::catalogconnector::{PartitionAwareCatalog, RefreshingCatalogProvider};
use crate::dataaccelerator::cayenne::CayennePartitionCreator;

/// Check whether the given catalog provider is a Cayenne-backed catalog.
pub fn is_cayenne_catalog(provider: &dyn CatalogProvider) -> bool {
    get_cayenne_provider(provider).is_some()
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
    if let Some(refreshing) = provider
        .as_any()
        .downcast_ref::<RefreshingCatalogProvider>()
    {
        return refreshing
            .inner_catalog()
            .as_any()
            .downcast_ref::<CayenneCatalogProvider>();
    }
    None
}

/// If the catalog provider is Cayenne-backed and implements [`PartitionAwareCatalog`],
/// return a trait reference.
///
/// Handles both direct [`CayenneCatalogProvider`] providers and
/// [`ComposedCatalogProvider`] wrappers whose external provider is a
/// [`CayenneCatalogProvider`].
pub fn as_partition_aware(provider: &dyn CatalogProvider) -> Option<&dyn PartitionAwareCatalog> {
    let cayenne_catalog = get_cayenne_provider(provider)?;
    Some(cayenne_catalog as &dyn PartitionAwareCatalog)
}

#[expect(dead_code)]
pub fn is_cayenne_table(provider: &dyn TableProvider) -> bool {
    if provider.as_any().is::<CayenneTableProvider>() {
        return true;
    };
    if let Some(deletion_adapter) = provider
        .as_any()
        .downcast_ref::<DeletionTableProviderAdapter>()
    {
        let source = deletion_adapter.source();
        return is_cayenne_table(source.as_ref());
    }
    let Some(partition_table_provider) = provider.as_any().downcast_ref::<PartitionTableProvider>()
    else {
        return false;
    };
    // This isn't working.
    partition_table_provider
        .creator()
        .as_any()
        .downcast_ref::<CayennePartitionCreator>()
        .is_some()
}
