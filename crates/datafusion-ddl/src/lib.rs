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

//! Shared `DataFusion` DDL infrastructure for Spice catalog integrations.
//!
//! ## Core abstractions
//!
//! - [`CatalogDdlHandler`] — catalog-specific trait: knows only about `ExecutionPlan`s.
//! - [`DdlAnalyzerRule`] — generic `AnalyzerRule`: intercepts DDL plans, extracts
//!   params, stores them in a [`DdlExtensionNode`].  One instance per catalog type.
//! - [`DdlExtensionPlanner`] — stateless `ExtensionPlanner`: dispatches any
//!   [`DdlExtensionNode`] to the handler embedded in that node.  One instance serves
//!   all catalog types registered in the same session.
//!
//! ## Shared utilities
//!
//! - [`arrow_datatype_to_sql`] — Arrow → SQL type mapping for DDL forwarding.
//! - [`DdlExtensionStore`] / [`SharedDdlExtensionStore`] / [`new_shared_store`] — thread-safe
//!   store populated by the SQL pre-processor and consumed by [`DdlAnalyzerRule`].
//! - Option parsing ([`parse_ddl_table_options`] etc.) and generic helpers
//!   ([`ddl_output_schema`], [`parse_qualified_schema_name`], …).
//!
//! ## Constants
//!
//! - [`DEFAULT_CATALOG`] / [`DEFAULT_SCHEMA`] — Spice-level defaults used when a
//!   `CREATE TABLE` or `CREATE SCHEMA` statement omits the catalog/schema name.

pub mod analyzer;
pub mod arrow_type;
pub mod ddl_log;
pub mod extension_store;
pub mod handler;
pub mod helpers;

pub use analyzer::{DdlAnalyzerRule, DdlExtensionNode, DdlExtensionPlanner, DdlNodeOp};
pub use arrow_type::arrow_datatype_to_sql;
pub use ddl_log::{DdlLog, Error as DdlLogError, InMemoryDdlLog};
pub use extension_store::{
    CreateTableStatementExtension, DatasetOptions, DdlExtensionStore, SharedDdlExtensionStore,
    new_shared_store, parse_acceleration_options, parse_dataset_options, parse_ddl_table_options,
};
pub use handler::{CatalogDdlHandler, CreateSchemaParams, CreateTableParams, DropTableParams};
pub use helpers::{
    ddl_output_schema, extract_primary_key_columns, has_ddl_extensions, is_ddl_enabled,
    parse_qualified_schema_name,
};
