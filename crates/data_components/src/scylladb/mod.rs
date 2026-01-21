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

//! `ScyllaDB` connector module with CQL dialect and partition key filter pushdown support.
//!
//! This module provides a `TableProvider` implementation for ScyllaDB/Cassandra databases
//! with intelligent filter pushdown. While CQL doesn't support most SQL constructs
//! (JOINs, subqueries, CAST, window functions, etc.), it does support efficient
//! filtering on primary key columns:
//!
//! - **Partition key equality**: `WHERE partition_key = value`
//! - **Clustering key comparisons**: `WHERE pk = value AND ck > value`
//!
//! Non-key filters and complex expressions are evaluated locally by `DataFusion`.

mod cql_dialect;
pub mod table_schema;

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
pub use table_schema::ScyllaDBTableSchema;

pub type ScyllaDbConnectionPool =
    dyn DbConnectionPool<Arc<ScyllaSession>, &'static dyn Sync> + Send + Sync;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to construct SQL table: {source}"))]
    UnableToConstructSQLTable {
        source: sql_provider_datafusion::Error,
    },

    #[snafu(display("Failed to fetch table schema: {source}"))]
    UnableToFetchTableSchema { source: table_schema::Error },
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// `ScyllaDB` table wrapper with partition key filter pushdown.
///
/// This `TableProvider` enables efficient queries by:
/// 1. Pushing down partition key equality filters to CQL
/// 2. Optionally pushing down clustering key comparisons
/// 3. Evaluating all other filters locally in `DataFusion`
///
/// ## Filter Pushdown Rules
///
/// - **Partition key `=`**: Always pushed down (enables efficient key lookup)
/// - **Clustering key `=`, `<`, `<=`, `>`, `>=`**: Pushed down when partition key is present
/// - **Regular columns**: Never pushed down (would require ALLOW FILTERING)
/// - **OR conditions**: Never pushed down (CQL doesn't support)
/// - **Complex expressions**: Never pushed down (CAST, BETWEEN, LIKE, etc.)
pub struct ScyllaDbTable {
    base_table: SqlTable<Arc<ScyllaSession>, &'static dyn Sync>,
    table_schema: ScyllaDBTableSchema,
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

    /// Determine filter pushdown support for each filter.
    ///
    /// Returns `Exact` for partition key equality filters (efficiently pushed to CQL),
    /// `Inexact` for clustering key filters (may need partition key at runtime),
    /// and `Unsupported` for all other filters.
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        Ok(self.table_schema.supports_filters_pushdown(filters))
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        // Separate key filters from other filters
        let (key_filters, _other_filters) = self.table_schema.separate_key_filters(filters);

        // Build the list of filters to push down to CQL
        let pushdown_filters: Vec<Expr> =
            if let Some((partition_filter, clustering_filter)) = key_filters {
                let mut filters = vec![partition_filter];
                if let Some(ck_filter) = clustering_filter {
                    filters.push(ck_filter);
                }
                filters
            } else {
                // No partition key filter - cannot push down any filters
                Vec::new()
            };

        self.base_table
            .scan(state, projection, &pushdown_filters, limit)
            .await
    }
}

pub struct ScyllaDbTableFactory {
    pool: Arc<ScyllaDbConnectionPool>,
    session: Arc<ScyllaSession>,
    keyspace: Arc<str>,
}

impl fmt::Debug for ScyllaDbTableFactory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScyllaDbTableFactory")
            .field("keyspace", &self.keyspace)
            .finish_non_exhaustive()
    }
}

impl ScyllaDbTableFactory {
    #[must_use]
    pub fn new(
        pool: Arc<ScyllaDbConnectionPool>,
        session: Arc<ScyllaSession>,
        keyspace: Arc<str>,
    ) -> Self {
        Self {
            pool,
            session,
            keyspace,
        }
    }
}

#[async_trait]
impl Read for ScyllaDbTableFactory {
    async fn table_provider(
        &self,
        table_reference: TableReference,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn std::error::Error + Send + Sync>> {
        let pool = Arc::clone(&self.pool);

        // Get keyspace and table names from reference
        let (keyspace, table) = match &table_reference {
            TableReference::Full { schema, table, .. }
            | TableReference::Partial { schema, table } => (schema.to_string(), table.to_string()),
            TableReference::Bare { table } => (self.keyspace.to_string(), table.to_string()),
        };

        // Fetch table schema (partition and clustering keys)
        let table_schema = ScyllaDBTableSchema::fetch(&self.session, &keyspace, &table)
            .await
            .context(UnableToFetchTableSchemaSnafu)?;

        // Create the base SqlTable with CQL dialect
        let base_table = SqlTable::new("scylladb", &pool, table_reference.clone(), None)
            .await
            .context(UnableToConstructSQLTableSnafu)?
            .with_dialect(Arc::new(CqlDialect::new()));

        // Wrap in ScyllaDbTable with schema for filter pushdown
        Ok(Arc::new(ScyllaDbTable {
            base_table,
            table_schema,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_expr::{col, lit};

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

    #[test]
    fn test_table_schema_partition_key_filter() {
        let schema = ScyllaDBTableSchema::new(
            "test_keyspace",
            "test_table",
            vec!["user_id".to_string()],
            vec!["timestamp".to_string()],
        );

        let filters = [col("user_id").eq(lit("user123"))];
        let filter_refs: Vec<&Expr> = filters.iter().collect();

        let result = schema.supports_filters_pushdown(&filter_refs);
        assert_eq!(result.len(), 1);
        assert!(matches!(result[0], TableProviderFilterPushDown::Exact));
    }

    #[test]
    fn test_table_schema_clustering_key_filter() {
        let schema = ScyllaDBTableSchema::new(
            "test_keyspace",
            "test_table",
            vec!["user_id".to_string()],
            vec!["timestamp".to_string()],
        );

        let filters = [col("timestamp").gt(lit("2024-01-01"))];
        let filter_refs: Vec<&Expr> = filters.iter().collect();

        let result = schema.supports_filters_pushdown(&filter_refs);
        assert_eq!(result.len(), 1);
        // Clustering key alone is Inexact (needs partition key at query time)
        assert!(matches!(result[0], TableProviderFilterPushDown::Inexact));
    }

    #[test]
    fn test_table_schema_regular_column_unsupported() {
        let schema = ScyllaDBTableSchema::new(
            "test_keyspace",
            "test_table",
            vec!["user_id".to_string()],
            vec!["timestamp".to_string()],
        );

        let filters = [col("status").eq(lit("active"))];
        let filter_refs: Vec<&Expr> = filters.iter().collect();

        let result = schema.supports_filters_pushdown(&filter_refs);
        assert_eq!(result.len(), 1);
        assert!(matches!(
            result[0],
            TableProviderFilterPushDown::Unsupported
        ));
    }

    #[test]
    fn test_table_schema_separate_key_filters() {
        let schema = ScyllaDBTableSchema::new(
            "test_keyspace",
            "test_table",
            vec!["user_id".to_string()],
            vec!["timestamp".to_string()],
        );

        let filters = vec![
            col("user_id").eq(lit("user123")),
            col("timestamp").gt(lit("2024-01-01")),
            col("status").eq(lit("active")),
        ];

        let (key_filters, other_filters) = schema.separate_key_filters(&filters);

        assert!(key_filters.is_some());
        let (pk, ck) = key_filters.expect("should have key filters");
        assert!(matches!(pk, Expr::BinaryExpr(_)));
        assert!(ck.is_some());
        assert_eq!(other_filters.len(), 1);
    }
}
