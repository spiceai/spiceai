/*
Copyright 2026 The Spice.ai OSS Authors

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

//! `ScyllaDB`/Cassandra table schema utilities.
//!
//! This module provides functionality to query and store table metadata,
//! including partition keys and clustering keys, from `ScyllaDB`'s system tables.

use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use scylla::client::session::Session;
use snafu::prelude::*;
use std::sync::Arc;

use crate::key_filter;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to query table schema: {source}"))]
    QueryError {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Table '{keyspace}.{table}' not found or has no columns"))]
    TableNotFound { keyspace: String, table: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Encapsulates `ScyllaDB` table schema with partition and clustering key information.
///
/// This struct stores the primary key structure of a `ScyllaDB`/Cassandra table,
/// enabling efficient filter pushdown by identifying which filters can be
/// used in CQL WHERE clauses.
#[derive(Debug, Clone)]
pub struct ScyllaDBTableSchema {
    keyspace: Arc<str>,
    table: Arc<str>,
    /// Partition key columns in order. Multiple columns for composite partition keys.
    partition_keys: Vec<String>,
    /// Clustering key columns in order. Defines row ordering within a partition.
    clustering_keys: Vec<String>,
}

impl ScyllaDBTableSchema {
    /// Creates a new `ScyllaDBTableSchema` with the given key columns.
    #[must_use]
    pub fn new(
        keyspace: impl Into<Arc<str>>,
        table: impl Into<Arc<str>>,
        partition_keys: Vec<String>,
        clustering_keys: Vec<String>,
    ) -> Self {
        Self {
            keyspace: keyspace.into(),
            table: table.into(),
            partition_keys,
            clustering_keys,
        }
    }

    /// Fetches table schema from `ScyllaDB` system tables.
    ///
    /// Queries `system_schema.columns` to retrieve partition and clustering key columns.
    ///
    /// # Arguments
    ///
    /// * `session` - The `ScyllaDB` session to query
    /// * `keyspace` - The keyspace name
    /// * `table` - The table name
    ///
    /// # Returns
    ///
    /// A `ScyllaDBTableSchema` with partition and clustering key information.
    pub async fn fetch(session: &Session, keyspace: &str, table: &str) -> Result<Self, Error> {
        // Escape single quotes to prevent CQL injection
        let escaped_keyspace = keyspace.replace('\'', "''");
        let escaped_table = table.replace('\'', "''");

        // Query system_schema.columns for column kinds and positions
        // kind: 'partition_key', 'clustering', 'regular', 'static'
        // position: order within the key (for composite keys)
        let query = format!(
            "SELECT column_name, kind, position FROM system_schema.columns \
             WHERE keyspace_name = '{escaped_keyspace}' AND table_name = '{escaped_table}'"
        );

        let result = session
            .query_unpaged(query.as_str(), &[])
            .await
            .map_err(|e| Error::QueryError {
                source: Box::new(e),
            })?;

        let rows = result.into_rows_result().map_err(|e| Error::QueryError {
            source: Box::new(e),
        })?;

        let mut partition_keys: Vec<(i32, String)> = Vec::new();
        let mut clustering_keys: Vec<(i32, String)> = Vec::new();

        for row in rows
            .rows::<(String, String, i32)>()
            .map_err(|e| Error::QueryError {
                source: Box::new(e),
            })?
        {
            let (column_name, kind, position) = row.map_err(|e| Error::QueryError {
                source: Box::new(e),
            })?;

            match kind.as_str() {
                "partition_key" => partition_keys.push((position, column_name)),
                "clustering" => clustering_keys.push((position, column_name)),
                _ => {} // Ignore regular and static columns
            }
        }

        // Validate that we found at least one partition key
        ensure!(
            !partition_keys.is_empty(),
            TableNotFoundSnafu {
                keyspace: keyspace.to_string(),
                table: table.to_string(),
            }
        );

        // Sort by position to get correct key order
        partition_keys.sort_by_key(|(pos, _)| *pos);
        clustering_keys.sort_by_key(|(pos, _)| *pos);

        let partition_keys: Vec<String> =
            partition_keys.into_iter().map(|(_, name)| name).collect();
        let clustering_keys: Vec<String> =
            clustering_keys.into_iter().map(|(_, name)| name).collect();

        Ok(Self::new(keyspace, table, partition_keys, clustering_keys))
    }

    /// Returns the keyspace name.
    #[must_use]
    pub fn keyspace(&self) -> &str {
        &self.keyspace
    }

    /// Returns the table name.
    #[must_use]
    pub fn table(&self) -> &str {
        &self.table
    }

    /// Returns the partition key columns in order.
    #[must_use]
    pub fn partition_keys(&self) -> &[String] {
        &self.partition_keys
    }

    /// Returns the clustering key columns in order.
    #[must_use]
    pub fn clustering_keys(&self) -> &[String] {
        &self.clustering_keys
    }

    /// Returns the first (or only) partition key column.
    ///
    /// For composite partition keys, this returns only the first column.
    /// Use `partition_keys()` to get all partition key columns.
    #[must_use]
    pub fn partition_key(&self) -> Option<&str> {
        self.partition_keys.first().map(String::as_str)
    }

    /// Returns the first clustering key column, if any.
    #[must_use]
    pub fn clustering_key(&self) -> Option<&str> {
        self.clustering_keys.first().map(String::as_str)
    }

    /// Determines filter pushdown support for each filter.
    ///
    /// # CQL Filter Pushdown Rules
    ///
    /// ScyllaDB/Cassandra has strict rules about which filters can be pushed down:
    ///
    /// 1. **Partition key equality is required** for efficient queries
    /// 2. **Clustering key** can use equality or range comparisons (after partition key)
    /// 3. **Regular columns** cannot be filtered without ALLOW FILTERING (unsafe)
    /// 4. **OR conditions** are not supported in CQL
    ///
    /// This implementation only pushes down filters on the first partition key column
    /// (for simplicity) and optionally the first clustering key column.
    #[must_use]
    pub fn supports_filters_pushdown(&self, filters: &[&Expr]) -> Vec<TableProviderFilterPushDown> {
        let partition_key = self.partition_key();
        let clustering_key = self.clustering_key();

        filters
            .iter()
            .map(|&expr| {
                // Only support simple partition key (not composite) for now
                if let Some(pk) = partition_key
                    && let Some(key_filter) =
                        key_filter::try_extract_key_filter(expr, pk, clustering_key)
                {
                    // Check for OR conditions which CQL doesn't support
                    if key_filter::contains_or(expr) {
                        return TableProviderFilterPushDown::Unsupported;
                    }

                    match key_filter {
                        key_filter::KeyFilter::Partition(_) => {
                            return TableProviderFilterPushDown::Exact;
                        }
                        key_filter::KeyFilter::Sort(_) => {
                            // Sort key can only be pushed if partition key is also present
                            // This is checked at query time, mark as Inexact to keep filter
                            return TableProviderFilterPushDown::Inexact;
                        }
                    }
                }
                TableProviderFilterPushDown::Unsupported
            })
            .collect()
    }

    /// Separates filters into key filters (pushable) and other filters.
    ///
    /// Returns `(partition_filter, clustering_filter, other_filters)` if a valid
    /// partition key equality filter is found, otherwise returns `None` and all
    /// filters in the other list.
    #[must_use]
    pub fn separate_key_filters(
        &self,
        filters: &[Expr],
    ) -> (Option<(Expr, Option<Expr>)>, Vec<Expr>) {
        let Some(pk) = self.partition_key() else {
            return (None, filters.to_vec());
        };

        let ck = self.clustering_key();

        match key_filter::try_match_index(filters, pk, ck) {
            Some((partition, sort, others)) => (Some((partition, sort)), others),
            None => (None, filters.to_vec()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_expr::{col, lit};

    fn create_test_schema() -> ScyllaDBTableSchema {
        ScyllaDBTableSchema::new(
            "test_keyspace",
            "test_table",
            vec!["user_id".to_string()],
            vec!["timestamp".to_string()],
        )
    }

    #[test]
    fn test_schema_getters() {
        let schema = create_test_schema();

        assert_eq!(schema.keyspace(), "test_keyspace");
        assert_eq!(schema.table(), "test_table");
        assert_eq!(schema.partition_keys(), &["user_id".to_string()]);
        assert_eq!(schema.clustering_keys(), &["timestamp".to_string()]);
        assert_eq!(schema.partition_key(), Some("user_id"));
        assert_eq!(schema.clustering_key(), Some("timestamp"));
    }

    #[test]
    fn test_partition_key_filter_supported() {
        let schema = create_test_schema();
        let filters = [col("user_id").eq(lit("user123"))];
        let filter_refs: Vec<&Expr> = filters.iter().collect();

        let result = schema.supports_filters_pushdown(&filter_refs);
        assert_eq!(result.len(), 1);
        assert!(matches!(result[0], TableProviderFilterPushDown::Exact));
    }

    #[test]
    fn test_clustering_key_filter_inexact() {
        let schema = create_test_schema();
        let filters = [col("timestamp").gt(lit("2024-01-01"))];
        let filter_refs: Vec<&Expr> = filters.iter().collect();

        let result = schema.supports_filters_pushdown(&filter_refs);
        assert_eq!(result.len(), 1);
        // Clustering key alone is Inexact (needs partition key at query time)
        assert!(matches!(result[0], TableProviderFilterPushDown::Inexact));
    }

    #[test]
    fn test_regular_column_unsupported() {
        let schema = create_test_schema();
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
    fn test_separate_key_filters() {
        let schema = create_test_schema();
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

    #[test]
    fn test_composite_partition_key() {
        let schema = ScyllaDBTableSchema::new(
            "test_keyspace",
            "test_table",
            vec!["tenant_id".to_string(), "user_id".to_string()],
            vec!["timestamp".to_string()],
        );

        // Only first partition key is used for now
        assert_eq!(schema.partition_key(), Some("tenant_id"));
        assert_eq!(schema.partition_keys().len(), 2);
    }

    #[test]
    fn test_no_clustering_key() {
        let schema = ScyllaDBTableSchema::new(
            "test_keyspace",
            "test_table",
            vec!["id".to_string()],
            vec![],
        );

        assert_eq!(schema.clustering_key(), None);
        assert!(schema.clustering_keys().is_empty());
    }
}
