/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! The [`QueryEngine`] trait abstracts the read-path, query-execution, and
//! data-write surface of a query engine.
//!
//! Consumers that only need to run queries, inspect table metadata, or write
//! records can depend on `Arc<dyn QueryEngine>` instead of the concrete
//! `DataFusion` struct, breaking the coupling to the `runtime` crate.

use std::fmt::Debug;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use arrow_schema::Schema;
use async_trait::async_trait;
use datafusion::common::ParamValues;
use datafusion::datasource::TableProvider;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::prelude::SessionContext;
use datafusion::sql::TableReference;

use crate::allowlist::ResolvedTableAwareAllowlist;

/// Errors returned by [`QueryEngine`] methods.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Options for a SQL query executed via [`QueryEngine::execute_query`].
pub struct QueryRequest {
    /// The SQL text to execute.
    pub sql: String,

    /// When `true`, reject any DDL, DML, COPY, or statement nodes.
    pub read_only: bool,

    /// Bind parameters (`$1`, `$2`, ...) for the query.
    pub parameters: Option<ParamValues>,

    /// Restrict which tables the query may reference.
    pub table_allowlist: Option<ResolvedTableAwareAllowlist>,
}

impl QueryRequest {
    /// Create a new query request with the given SQL.
    #[must_use]
    pub fn new(sql: impl Into<String>) -> Self {
        Self {
            sql: sql.into(),
            read_only: false,
            parameters: None,
            table_allowlist: None,
        }
    }

    /// Mark this query as read-only, rejecting any write operations.
    #[must_use]
    pub fn read_only(mut self, read_only: bool) -> Self {
        self.read_only = read_only;
        self
    }

    /// Set bind parameters for the query.
    #[must_use]
    pub fn parameters(mut self, params: ParamValues) -> Self {
        self.parameters = Some(params);
        self
    }

    /// Restrict the query to only reference tables in the allowlist.
    #[must_use]
    pub fn allow_tables(mut self, allowlist: ResolvedTableAwareAllowlist) -> Self {
        self.table_allowlist = Some(allowlist);
        self
    }
}

/// Trait for components that provide query execution capabilities.
///
/// This trait captures the read-path, query execution, and data write surface
/// of the query engine, allowing consumers to depend on `Arc<dyn QueryEngine>`
/// rather than the concrete `DataFusion` struct from the `runtime` crate.
#[async_trait]
pub trait QueryEngine: Send + Sync + Debug {
    // --- Session access ---

    /// Returns the underlying [`SessionContext`].
    fn session_context(&self) -> &Arc<SessionContext>;

    // --- Table metadata ---

    /// Async table lookup by reference.
    async fn get_table(&self, table_ref: &TableReference) -> Option<Arc<dyn TableProvider>>;

    /// Sync table lookup. Works for tables in `SpiceSchemaProvider`-backed
    /// schemas where the lookup is an in-memory `DashMap` read.
    fn get_table_sync(&self, table_ref: &TableReference) -> Option<Arc<dyn TableProvider>>;

    /// Check if a table exists.
    fn table_exists(&self, table_ref: &TableReference) -> bool;

    /// Get the Arrow schema for a dataset.
    async fn get_arrow_schema(&self, table_ref: TableReference) -> Result<Schema, BoxError>;

    /// Get user-visible table names (excludes internal schemas like `runtime`).
    fn get_user_table_names(&self) -> Vec<TableReference>;

    /// Get all public table names as fully-qualified strings.
    fn get_public_table_names(&self) -> Result<Vec<String>, BoxError>;

    // --- Access control ---

    /// Check if a specific table is writable.
    fn is_writable(&self, table_ref: &TableReference) -> bool;

    /// Check if the catalog containing a table reference is writable.
    fn is_path_catalog_writable(&self, table_ref: &TableReference) -> bool;

    // --- Query execution ---

    /// Execute a SQL query and return the result stream.
    ///
    /// This is the primary query execution entry point. The implementation
    /// handles planning, validation, caching, and telemetry internally.
    async fn execute_query(
        &self,
        request: QueryRequest,
    ) -> Result<SendableRecordBatchStream, BoxError>;

    // --- Data write ---

    /// Write record batches to a table (append).
    async fn write_data(
        &self,
        table_ref: &TableReference,
        data: Vec<RecordBatch>,
    ) -> Result<(), BoxError>;
}
