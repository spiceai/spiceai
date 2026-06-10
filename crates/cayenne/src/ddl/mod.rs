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
//!
//! # Supported operations
//!
//! The `datafusion_ddl` analyzer intercepts exactly three DDL statement kinds
//! for Cayenne catalogs, handled here by [`CayenneDdlHandler`]:
//!
//! - `CREATE TABLE` (with `IF NOT EXISTS`, `PARTITION BY <expr>`, and
//!   `CREATE TABLE … (LIKE <table>)`)
//! - `DROP TABLE` (with `IF EXISTS`)
//! - `CREATE SCHEMA` (with `IF NOT EXISTS`)
//!
//! Other DDL (`ALTER TABLE`, `DROP SCHEMA`, views, …) is not intercepted and
//! falls through to `DataFusion`'s default handling. On the DML side,
//! [`CayenneDmlHandler`] overlays only `MERGE` (see [`merge_planner`] for the
//! supported statement shape); `INSERT`/`UPDATE`/`DELETE` use the standard
//! `TableProvider` paths.

pub mod handler;
pub mod merge_planner;
pub mod operations;
pub mod physical_plans;

pub use handler::CayenneDdlHandler;
pub use merge_planner::{CayenneDmlHandler, LocalMergePlanInput, build_local_merge_plan_input};
pub use physical_plans::CayenneMergeExec;

use datafusion::catalog::CatalogProvider;

use crate::catalog_provider::CayenneCatalogProvider;

/// Returns `true` if `provider` is a direct [`CayenneCatalogProvider`].
///
/// The runtime extends this check to also cover `ComposedCatalogProvider`.
#[must_use]
pub fn is_cayenne_catalog(provider: &dyn CatalogProvider) -> bool {
    provider
        .as_any()
        .downcast_ref::<CayenneCatalogProvider>()
        .is_some()
}

/// Try to extract a [`CayenneCatalogProvider`] reference via direct downcast.
///
/// Returns `None` for wrapped providers — use the runtime's `get_cayenne_provider`
/// to also handle `ComposedCatalogProvider`.
#[must_use]
pub fn get_cayenne_provider(provider: &dyn CatalogProvider) -> Option<&CayenneCatalogProvider> {
    provider.as_any().downcast_ref::<CayenneCatalogProvider>()
}
