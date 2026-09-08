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

pub mod conn;
mod cql_dialect;
pub mod pool;
pub mod table_schema;

use std::{fmt, sync::Arc};

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

use data_components::Read;

pub use cql_dialect::CqlDialect;
pub use table_schema::ScyllaDBTableSchema;

pub type ScyllaDbConnectionPool =
    dyn DbConnectionPool<Arc<ScyllaSession>, &'static dyn Sync> + Send + Sync;

type ScyllaDbSqlTable = SqlTable<Arc<ScyllaSession>, &'static dyn Sync>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to create ScyllaDB SQL table: {source}"))]
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
    base_table: ScyllaDbSqlTable,
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

/// Applies the CQL dialect and declines the pushdowns CQL cannot serve.
///
/// Both pushdowns happen in the physical optimizer, below
/// `TableProvider::supports_filters_pushdown`, so declining them at the logical level is
/// not enough:
///
/// - a physical `FilterExec` predicate on a non-key column needs `ALLOW FILTERING`;
/// - `ORDER BY` is rejected outright unless the partition key is restricted by `=` or
///   `IN`, so a sort absorbed into the scan makes the whole statement invalid.
///
/// Sorting and non-key filtering are performed by `DataFusion` instead.
fn apply_cql_pushdown_limits(table: ScyllaDbSqlTable) -> ScyllaDbSqlTable {
    table
        .with_dialect(Arc::new(CqlDialect::new()))
        .with_allow_physical_filter_pushdown(false)
        .with_allow_physical_sort_pushdown(false)
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

        // Create the base SqlTable, configured for what CQL can serve
        let base_table = apply_cql_pushdown_limits(
            SqlTable::new("scylladb", &pool, table_reference.clone(), None)
                .await
                .context(UnableToConstructSQLTableSnafu)?,
        );

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
    use arrow::{
        compute::SortOptions,
        datatypes::{DataType, Field, Schema},
    };
    use datafusion::{
        logical_expr::{col, lit},
        physical_plan::{
            SortOrderPushdownResult, displayable,
            expressions::{Column, PhysicalSortExpr},
        },
        prelude::SessionContext,
    };
    use datafusion_table_providers::sql::db_connection_pool::{
        JoinPushDown, dbconnection::DbConnection,
    };

    /// A pool that builds plans but never connects: the sort-pushdown tests inspect the
    /// SQL a scan *would* issue, and never execute it.
    struct MockPool;

    #[async_trait]
    impl DbConnectionPool<Arc<ScyllaSession>, &'static dyn Sync> for MockPool {
        async fn connect(
            &self,
        ) -> std::result::Result<
            Box<dyn DbConnection<Arc<ScyllaSession>, &'static dyn Sync>>,
            Box<dyn std::error::Error + Send + Sync>,
        > {
            Err("MockPool never connects".into())
        }

        fn join_push_down(&self) -> JoinPushDown {
            JoinPushDown::Disallow
        }
    }

    /// One projected column plus the column `tpch_simple_q3` sorts on.
    fn lineitem_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("l_comment", DataType::Utf8, true),
            Field::new("l_linenumber", DataType::Int32, true),
        ]))
    }

    fn base_table() -> ScyllaDbSqlTable {
        let pool: Arc<ScyllaDbConnectionPool> = Arc::new(MockPool);
        SqlTable::new_with_schema(
            "scylladb",
            &pool,
            lineitem_schema(),
            TableReference::partial("tpch", "lineitem"),
            None,
        )
    }

    fn scylladb_table(table: ScyllaDbSqlTable) -> ScyllaDbTable {
        ScyllaDbTable {
            base_table: table,
            table_schema: ScyllaDBTableSchema::new(
                "tpch",
                "lineitem",
                vec!["l_orderkey".to_string()],
                vec!["l_linenumber".to_string()],
            ),
        }
    }

    /// `order by l_linenumber desc` — the clause `ScyllaDB` rejected in #10775.
    fn order_by_linenumber_desc() -> Vec<PhysicalSortExpr> {
        vec![PhysicalSortExpr::new(
            Arc::new(Column::new("l_linenumber", 1)),
            SortOptions {
                descending: true,
                nulls_first: true,
            },
        )]
    }

    /// `simple_q3`'s scan: no limit, because the query's `fetch` belongs to the sort above
    /// it and cannot be pushed below one.
    async fn scan(table: &ScyllaDbTable) -> Arc<dyn ExecutionPlan> {
        let state = SessionContext::new().state();
        table
            .scan(&state, None, &[], None)
            .await
            .expect("ScyllaDbTable should plan a scan")
    }

    #[tokio::test]
    async fn scylladb_scan_declines_physical_sort_pushdown() {
        let table = scylladb_table(apply_cql_pushdown_limits(base_table()));
        let plan = scan(&table).await;

        let pushdown = plan
            .try_pushdown_sort(&order_by_linenumber_desc())
            .expect("sort pushdown should be answered, not error");

        assert!(
            matches!(pushdown, SortOrderPushdownResult::Unsupported),
            "CQL cannot ORDER BY an unrestricted partition key, so the scan must leave the sort to DataFusion"
        );
    }

    /// Positive control for `scylladb_scan_declines_physical_sort_pushdown`: with the
    /// opt-out removed, the same fixture reaches the gate and does absorb the sort. Without
    /// this, an `Unsupported` for some unrelated reason would pass that test vacuously.
    #[tokio::test]
    async fn a_default_sql_table_absorbs_the_sort_into_its_query() {
        let table = scylladb_table(base_table().with_dialect(Arc::new(CqlDialect::new())));
        let plan = scan(&table).await;

        let pushdown = plan
            .try_pushdown_sort(&order_by_linenumber_desc())
            .expect("sort pushdown should be answered, not error");

        let SortOrderPushdownResult::Exact { inner } = pushdown else {
            panic!("a SqlExec that allows sort pushdown should absorb the sort exactly");
        };
        assert!(
            displayable(inner.as_ref())
                .indent(false)
                .to_string()
                .contains("ORDER BY"),
            "the absorbed sort should show up as ORDER BY in the pushed query"
        );
    }

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
