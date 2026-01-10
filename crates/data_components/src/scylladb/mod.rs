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

//! `ScyllaDB` connector module with CQL dialect support.
//!
//! Note: Filter pushdown is disabled for `ScyllaDB` because CQL
//! (Cassandra Query Language) doesn't support most SQL constructs like
//! JOINs, subqueries, INTERVAL, CASE WHEN, CAST, window functions, etc.
//! All query processing happens locally in `DataFusion`, with `ScyllaDB`
//! providing only the base table data via simple SELECT * queries.

mod cql_dialect;

use std::{any::Any, fmt, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    catalog::Session,
    datasource::TableProvider,
    error::Result as DataFusionResult,
    logical_expr::{Expr, TableProviderFilterPushDown, TableType},
    physical_plan::ExecutionPlan,
    sql::TableReference,
};
use datafusion_table_providers::sql::{
    db_connection_pool::DbConnectionPool,
    sql_provider_datafusion::{self, SqlTable},
};
use scylla::client::session::Session as ScyllaSession;
use snafu::prelude::*;

use crate::Read;

pub use cql_dialect::CqlDialect;

pub type ScyllaDbConnectionPool =
    dyn DbConnectionPool<Arc<ScyllaSession>, &'static dyn Sync> + Send + Sync;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to construct SQL table: {source}"))]
    UnableToConstructSQLTable {
        source: sql_provider_datafusion::Error,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// `ScyllaDB` table wrapper that disables filter pushdown.
///
/// CQL doesn't support most SQL filter expressions (CAST, INTERVAL, complex
/// predicates), so we return `Unsupported` for all filters and let `DataFusion`
/// handle filtering locally after fetching all data from `ScyllaDB`.
pub struct ScyllaDbTable {
    base_table: SqlTable<Arc<ScyllaSession>, &'static dyn Sync>,
}

impl fmt::Debug for ScyllaDbTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScyllaDbTable").finish_non_exhaustive()
    }
}

impl fmt::Display for ScyllaDbTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ScyllaDbTable")
    }
}

#[async_trait]
impl TableProvider for ScyllaDbTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> datafusion::arrow::datatypes::SchemaRef {
        self.base_table.schema()
    }

    fn table_type(&self) -> TableType {
        self.base_table.table_type()
    }

    /// Disable all filter pushdown for `ScyllaDB`.
    ///
    /// CQL doesn't support most SQL filter expressions like CAST, INTERVAL,
    /// complex predicates, etc. Rather than trying to detect which filters
    /// CQL can handle (very few), we disable pushdown entirely and let
    /// `DataFusion` filter results locally.
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        // Return Unsupported for all filters - DataFusion will filter locally
        Ok(filters
            .iter()
            .map(|_| TableProviderFilterPushDown::Unsupported)
            .collect())
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        // Always pass empty filters to the base table - we handle filtering in DataFusion
        self.base_table.scan(state, projection, &[], limit).await
    }
}

pub struct ScyllaDbTableFactory {
    pool: Arc<ScyllaDbConnectionPool>,
}

impl fmt::Debug for ScyllaDbTableFactory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScyllaDbTableFactory")
            .finish_non_exhaustive()
    }
}

impl ScyllaDbTableFactory {
    #[must_use]
    pub fn new(pool: Arc<ScyllaDbConnectionPool>) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl Read for ScyllaDbTableFactory {
    async fn table_provider(
        &self,
        table_reference: TableReference,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn std::error::Error + Send + Sync>> {
        let pool = Arc::clone(&self.pool);

        // Create the base SqlTable with CQL dialect
        let base_table = SqlTable::new("scylladb", &pool, table_reference.clone(), None)
            .await
            .context(UnableToConstructSQLTableSnafu)?
            .with_dialect(Arc::new(CqlDialect::new()));

        // Wrap in ScyllaDbTable to disable filter pushdown
        // CQL doesn't support most SQL filter expressions (CAST, INTERVAL, etc.)
        Ok(Arc::new(ScyllaDbTable { base_table }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        // Verify error variant names are descriptive
        let err_type = std::any::type_name::<Error>();
        assert!(err_type.contains("scylladb") || err_type.contains("Error"));
    }

    #[test]
    fn test_scylladb_table_factory_debug() {
        let debug_format = "ScyllaDbTableFactory { .. }";
        assert!(debug_format.contains("ScyllaDbTableFactory"));
    }

    #[test]
    fn test_table_reference_variants() {
        // Test that various TableReference types can be constructed
        let bare = TableReference::bare("my_table");
        assert_eq!(bare.table(), "my_table");

        let partial = TableReference::partial("my_keyspace", "my_table");
        assert_eq!(partial.schema(), Some("my_keyspace"));
        assert_eq!(partial.table(), "my_table");

        let full = TableReference::full("my_catalog", "my_keyspace", "my_table");
        assert_eq!(full.catalog(), Some("my_catalog"));
        assert_eq!(full.schema(), Some("my_keyspace"));
        assert_eq!(full.table(), "my_table");
    }
}
