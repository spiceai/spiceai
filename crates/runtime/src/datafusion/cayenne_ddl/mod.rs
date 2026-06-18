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

//! Broadcast (distributed) Cayenne DDL and DML support for the runtime.
//!
//! DDL: [`DistributedCayenneDdlHandler`] implements [`CatalogDdlHandler`] and is
//! paired with `datafusion_ddl::DdlAnalyzerRule` + `DdlExtensionPlanner`.
//!
//! DML: [`DistributedCayenneDmlHandler`] is embedded in generic
//! `datafusion_dml::DmlExtensionNode` values and executed by the shared
//! `datafusion_dml::DmlExtensionPlanner`.
//!
//! This overlay is optional by operation: handlers can override only the
//! operations they need while inheriting default `DataFusion` DML behavior
//! for the rest.

pub mod dml_planner;
pub mod handler;
pub mod physical_plans;

pub use dml_planner::{DistributedCayenneDmlHandler, extract_filters, extract_update_assignments};
pub use handler::DistributedCayenneDdlHandler;

use datafusion::catalog::CatalogProvider;

use cayenne::catalog_provider::CayenneCatalogProvider;

use super::composed_catalog::ComposedCatalogProvider;
use crate::catalogconnector::PartitionAwareCatalog;

/// Returns `true` if `provider` is Cayenne-backed — handles both a direct
/// [`CayenneCatalogProvider`] and the runtime's [`ComposedCatalogProvider`] wrapper.
pub fn is_cayenne_catalog(provider: &dyn CatalogProvider) -> bool {
    get_cayenne_provider(provider).is_some()
}

/// Extract the [`CayenneCatalogProvider`] reference, handling both direct and
/// `ComposedCatalogProvider`-wrapped cases.
pub fn get_cayenne_provider(provider: &dyn CatalogProvider) -> Option<&CayenneCatalogProvider> {
    if let Some(cayenne) = provider.downcast_ref::<CayenneCatalogProvider>() {
        return Some(cayenne);
    }
    if let Some(composed) = provider.downcast_ref::<ComposedCatalogProvider>() {
        return composed.external().downcast_ref::<CayenneCatalogProvider>();
    }
    None
}

/// Return a [`PartitionAwareCatalog`] reference if the provider is Cayenne-backed.
pub fn as_partition_aware(provider: &dyn CatalogProvider) -> Option<&dyn PartitionAwareCatalog> {
    let cayenne_catalog = get_cayenne_provider(provider)?;
    Some(cayenne_catalog as &dyn PartitionAwareCatalog)
}
