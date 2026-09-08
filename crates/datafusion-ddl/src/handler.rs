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

//! The [`CatalogDdlHandler`] trait and its associated parameter types.
//!
//! Implementors know how to turn parsed DDL parameters into [`ExecutionPlan`]s.
//! They know nothing about `LogicalPlan` rewriting, `UserDefinedLogicalNode`,
//! or the `AnalyzerRule` / `ExtensionPlanner` pipeline — that is all handled
//! by [`DdlAnalyzerRule`] and [`DdlExtensionPlanner`] in [`super::analyzer`].

use std::fmt;
use std::sync::Arc;

use arrow::datatypes::Schema;
use datafusion::catalog::CatalogProviderList;
use datafusion::error::Result as DFResult;
use datafusion::execution::SessionState;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::sql::TableReference;

use crate::CreateTableStatementExtension;

// ── Parameter types ───────────────────────────────────────────────────────────

/// Parameters for [`CatalogDdlHandler::create_table_exec`].
///
/// Populated by [`DdlAnalyzerRule`] from the intercepted `CREATE TABLE` logical
/// plan and the [`crate::DdlExtensionStore`].
#[derive(Debug, Clone)]
pub struct CreateTableParams {
    pub catalog_name: String,
    pub schema_name: String,
    pub table_name: String,
    pub arrow_schema: Arc<Schema>,
    pub primary_key: Vec<String>,
    /// DDL extensions extracted from `WITH (...)` and `PARTITION BY` clauses.
    /// Handlers use whichever sub-fields they care about.
    pub extension: CreateTableStatementExtension,
    pub if_not_exists: bool,
    pub or_replace: bool,
    /// Source table for `CREATE TABLE ... (LIKE ...)`.
    /// `None` for plain `CREATE TABLE`.
    pub like_source_table: Option<TableReference>,
}

/// Parameters for [`CatalogDdlHandler::drop_table_exec`].
#[derive(Debug, Clone)]
pub struct DropTableParams {
    pub catalog_name: String,
    pub schema_name: String,
    pub table_name: String,
    pub if_exists: bool,
}

/// Parameters for [`CatalogDdlHandler::create_schema_exec`].
#[derive(Debug, Clone)]
pub struct CreateSchemaParams {
    pub catalog_name: String,
    pub schema_name: String,
    pub if_not_exists: bool,
}

// ── Trait ─────────────────────────────────────────────────────────────────────

/// Catalog-specific DDL handler.
///
/// Implementations know how to create [`ExecutionPlan`]s for
/// `CREATE TABLE` / `DROP TABLE` / `CREATE SCHEMA` on a particular catalog
/// backend (Cayenne, Iceberg, …).
///
/// Handlers are paired with [`DdlAnalyzerRule`] (one per handler) and a shared
/// stateless [`DdlExtensionPlanner`]. They do **not** need to know about
/// `LogicalPlan` rewriting, `UserDefinedLogicalNode`, `AnalyzerRule`, or
/// `ExtensionPlanner` — all of that is in `datafusion-ddl`.
pub trait CatalogDdlHandler: fmt::Debug + Send + Sync {
    /// Short identifier used for diagnostics (e.g. `"cayenne"`, `"iceberg"`).
    fn name(&self) -> &'static str;

    /// Returns `true` if this handler should intercept DDL for `catalog_name`.
    ///
    /// Called after the DDL-enabled check passes. Implementors inspect the
    /// `catalog_list` to determine whether the named catalog is backed by
    /// their storage engine.
    fn is_target_catalog(
        &self,
        catalog_name: &str,
        catalog_list: &Arc<dyn CatalogProviderList>,
    ) -> bool;

    /// Produce an [`ExecutionPlan`] that creates the described table.
    ///
    /// The returned plan is executed as-is — implementors should do all async
    /// work (metadata registration, schema creation, etc.) inside `execute()`.
    ///
    /// `session_state` is provided for access to `runtime_env()`,
    /// `SessionContext` construction, and SQL expression parsing.
    /// # Errors
    ///
    /// Returns an error if the table cannot be created (e.g. catalog not found,
    /// schema mismatch, or underlying storage failure).
    fn create_table_exec(
        &self,
        params: CreateTableParams,
        catalog_list: Arc<dyn CatalogProviderList>,
        session_state: &SessionState,
    ) -> DFResult<Arc<dyn ExecutionPlan>>;

    /// Produce an [`ExecutionPlan`] that drops the described table.
    ///
    /// # Errors
    ///
    /// Returns an error if the table cannot be dropped (e.g. catalog not found
    /// or underlying storage failure).
    fn drop_table_exec(
        &self,
        params: DropTableParams,
        catalog_list: Arc<dyn CatalogProviderList>,
    ) -> DFResult<Arc<dyn ExecutionPlan>>;

    /// Produce an [`ExecutionPlan`] that creates the described schema.
    ///
    /// # Errors
    ///
    /// Returns an error if the schema cannot be created (e.g. catalog not found
    /// or underlying storage failure).
    fn create_schema_exec(
        &self,
        params: CreateSchemaParams,
        catalog_list: Arc<dyn CatalogProviderList>,
        session_state: &SessionState,
    ) -> DFResult<Arc<dyn ExecutionPlan>>;
}
