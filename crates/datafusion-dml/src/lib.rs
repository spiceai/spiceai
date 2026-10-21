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

//! Shared `DataFusion` DML infrastructure for Spice catalog integrations.
//!
//! ## Core abstractions
//!
//! - [`CatalogDmlHandler`] — catalog-specific trait: maps DML params to
//!   [`ExecutionPlan`]s.
//! - [`DmlExtensionNode`] — single logical node type carrying operation params
//!   plus the selected handler.
//! - [`DmlExtensionPlanner`] — stateless `ExtensionPlanner` that dispatches
//!   any [`DmlExtensionNode`] to the handler embedded in that node.
//!
//! This mirrors the `datafusion-ddl` abstraction, but for DML operations
//! (`DELETE`, `UPDATE`, `INSERT`, `MERGE`).
//!
//! The handler API is an optional overlay over default `DataFusion` DML:
//! handlers may override only the operations they need, while relying on trait
//! defaults for standard behavior.
//!
//! The API follows core `DataFusion` DML conventions:
//! - typed predicates/assignments (`Expr`) rather than SQL strings,
//! - explicit insert semantics via [`datafusion::logical_expr::dml::InsertOp`],
//! - standard DML output schema (`count: UInt64`).

pub mod handler;
pub mod helpers;
pub mod node;
pub mod planner;

pub use handler::{CatalogDmlHandler, DeleteParams, InsertParams, MergeParams, UpdateParams};
pub use helpers::dml_count_output_schema;
pub use node::{DmlExtensionNode, DmlNodeOp};
pub use planner::DmlExtensionPlanner;
