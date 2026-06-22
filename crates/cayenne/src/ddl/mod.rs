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

//! Single-node Cayenne DDL plus local MERGE DML support.
//!
//! Usable without the runtime crate. Pair with `datafusion_ddl::DdlAnalyzerRule`
//! and `datafusion_ddl::DdlExtensionPlanner` for DDL, and with
//! `datafusion_dml::DmlExtensionPlanner` for any emitted generic DML extension
//! nodes.

pub mod handler;
pub mod merge_planner;
pub mod operations;
pub mod physical_plans;

pub use handler::CayenneDdlHandler;
pub use merge_planner::{CayenneDmlHandler, LocalMergePlanInput, build_local_merge_plan_input};
pub use physical_plans::CayenneMergeExec;

use data_components::RefreshingCatalogProvider;
use datafusion::catalog::CatalogProvider;
use runtime_datafusion::composed_catalog::ComposedCatalogProvider;

use crate::catalog_provider::CayenneCatalogProvider;

/// Returns `true` if `provider` is Cayenne-backed, peeling the transparent
/// [`RefreshingCatalogProvider`] and [`ComposedCatalogProvider`] wrappers.
#[must_use]
pub fn is_cayenne_catalog(provider: &dyn CatalogProvider) -> bool {
    get_cayenne_provider(provider).is_some()
}

/// Extract the [`CayenneCatalogProvider`] reference, peeling the transparent
/// [`RefreshingCatalogProvider`] and [`ComposedCatalogProvider`] wrappers in any
/// nesting order.
///
/// `DataFusion` 54 removed `CatalogProvider::as_any`, which those wrappers used to
/// delegate to their inner provider so that `downcast_ref::<CayenneCatalogProvider>()`
/// transparently saw through them. The `Any`-based `downcast_ref` that replaced it
/// only resolves to the wrapper's own type, so the wrappers must be peeled
/// explicitly.
#[must_use]
pub fn get_cayenne_provider(provider: &dyn CatalogProvider) -> Option<&CayenneCatalogProvider> {
    if let Some(cayenne) = provider.downcast_ref::<CayenneCatalogProvider>() {
        return Some(cayenne);
    }
    if let Some(refreshing) = provider.downcast_ref::<RefreshingCatalogProvider>() {
        return get_cayenne_provider(refreshing.inner_catalog());
    }
    if let Some(composed) = provider.downcast_ref::<ComposedCatalogProvider>() {
        return get_cayenne_provider(composed.external().as_ref());
    }
    None
}
