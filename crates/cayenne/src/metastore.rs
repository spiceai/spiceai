/*
Copyright 2025 The Spice.ai OSS Authors

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

//! Metastore backend abstraction for Cayenne catalog storage.
//!
//! This module provides a trait-based abstraction over different database backends
//! that can be used to store Cayenne metadata. This allows swapping between `SQLite`,
//! Turso, or other storage implementations.

pub mod sqlite;

#[cfg(feature = "turso")]
pub mod turso;

use std::fmt::Display;

use super::catalog::CatalogResult;
use async_trait::async_trait;

/// Expected column definitions for a metadata table.
///
/// Used by [`validate_existing_schema`] to compare the actual schema of an existing
/// metadata table against the expected schema. Only column names are checked — types
/// and constraints are not compared because `SQLite`/`libSQL` type affinity makes
/// exact type matching unreliable and columns may be added with `ALTER TABLE`.
#[derive(Debug, Clone)]
pub struct ExpectedTable {
    /// The table name (e.g., `"cayenne_table"`).
    pub name: &'static str,
    /// The ordered list of expected column names.
    pub columns: &'static [&'static str],
}

/// Expected schema definitions for all Cayenne metadata tables.
///
/// These must be kept in sync with the DDL constants in `sqlite.rs` and `turso.rs`.
/// When the schema changes, update both the DDL constants **and** these definitions.
pub const EXPECTED_TABLES: &[ExpectedTable] = &[
    ExpectedTable {
        name: "cayenne_table",
        columns: &[
            "table_id",
            "table_uuid",
            "table_name",
            "path",
            "path_is_relative",
            "schema_json",
            "primary_key_json",
            "on_conflict_json",
            "current_snapshot_id",
            "partition_column",
            "vortex_config_json",
            "current_sequence_number",
        ],
    },
    ExpectedTable {
        name: "cayenne_delete_file",
        columns: &[
            "delete_file_id",
            "table_id",
            "path",
            "path_is_relative",
            "format",
            "delete_count",
            "file_size_bytes",
            "source_data_file_path",
            "sequence_number",
        ],
    },
    ExpectedTable {
        name: "cayenne_partition",
        columns: &[
            "partition_id",
            "table_id",
            "partition_columns_json",
            "partition_values_json",
            "partition_key",
            "path",
            "path_is_relative",
            "record_count",
            "file_size_bytes",
        ],
    },
    ExpectedTable {
        name: "cayenne_insert_record",
        columns: &[
            "insert_record_id",
            "table_id",
            "pk_bytes",
            "sequence_number",
        ],
    },
    ExpectedTable {
        name: "cayenne_snapshot_sequence",
        columns: &["table_id", "snapshot_id", "sequence_number"],
    },
];

/// Validate the existing metadata table schemas against the expected definitions.
///
/// Compares the actual column names of each metadata table against
/// [`EXPECTED_TABLES`]. If any table has a different set of columns (missing,
/// extra, or reordered), returns a [`CatalogError::SchemaMismatch`] that tells
/// the user to clear their acceleration data.
///
/// `actual_columns_fn` is an async callback that returns the ordered list of
/// column names for a given table. It should return an empty `Vec` if the table
/// does not yet exist (the table will be created by the DDL that runs before
/// validation).
///
/// # Errors
///
/// Returns [`CatalogError::SchemaMismatch`] when the existing schema does not
/// match the expected schema.
pub async fn validate_existing_schema<F, Fut>(actual_columns_fn: F) -> CatalogResult<()>
where
    F: Fn(&'static str) -> Fut,
    Fut: std::future::Future<Output = CatalogResult<Vec<String>>>,
{
    for expected in EXPECTED_TABLES {
        let actual_columns = actual_columns_fn(expected.name).await?;

        // If the table has no columns it was freshly created — nothing to validate.
        if actual_columns.is_empty() {
            continue;
        }

        let expected_columns: Vec<&str> = expected.columns.to_vec();
        let actual_refs: Vec<&str> = actual_columns.iter().map(String::as_str).collect();

        if expected_columns != actual_refs {
            tracing::debug!(
                "Cayenne schema mismatch for '{}': expected columns [{}], found [{}]",
                expected.name,
                expected_columns.join(", "),
                actual_refs.join(", ")
            );
            return Err(super::catalog::CatalogError::SchemaMismatch {
                table: expected.name.to_string(),
            });
        }
    }

    Ok(())
}

/// Parameters for querying a single row from the database.
#[derive(Debug)]
pub struct QueryRowParams<'a> {
    /// SQL query to execute
    pub sql: &'a str,
    /// Parameters to bind to the query
    pub params: Vec<MetastoreValue>,
}

/// Parameters for executing a SQL statement that modifies data.
#[derive(Debug)]
pub struct ExecuteParams<'a> {
    /// SQL statement to execute
    pub sql: &'a str,
    /// Parameters to bind to the statement
    pub params: Vec<MetastoreValue>,
}

/// Parameters for querying multiple rows from the database.
#[derive(Debug)]
pub struct QueryParams<'a> {
    /// SQL query to execute
    pub sql: &'a str,
    /// Parameters to bind to the query
    pub params: Vec<MetastoreValue>,
}

/// A value that can be stored in or retrieved from the metastore.
#[derive(Debug, Clone)]
pub enum MetastoreValue {
    /// Integer value
    Integer(i64),
    /// Text value
    Text(String),
    /// Boolean value
    Bool(bool),
    /// Blob (binary) value
    Blob(Vec<u8>),
    /// Null value
    Null,
}

impl Display for MetastoreValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MetastoreValue::Integer(v) => write!(f, "integer {v}"),
            MetastoreValue::Text(v) => write!(f, "text '{v}'"),
            MetastoreValue::Bool(v) => write!(f, "bool {v}"),
            MetastoreValue::Blob(v) => write!(f, "blob ({} bytes)", v.len()),
            MetastoreValue::Null => write!(f, "NULL"),
        }
    }
}

impl From<i64> for MetastoreValue {
    fn from(v: i64) -> Self {
        Self::Integer(v)
    }
}

impl From<String> for MetastoreValue {
    fn from(v: String) -> Self {
        Self::Text(v)
    }
}

impl From<&str> for MetastoreValue {
    fn from(v: &str) -> Self {
        Self::Text(v.to_string())
    }
}

impl From<bool> for MetastoreValue {
    fn from(v: bool) -> Self {
        Self::Bool(v)
    }
}

impl<T: Into<MetastoreValue>> From<Option<T>> for MetastoreValue {
    fn from(v: Option<T>) -> Self {
        match v {
            Some(inner) => inner.into(),
            None => Self::Null,
        }
    }
}

/// A row returned from a query.
pub trait MetastoreRow: Send {
    /// Get an i64 value from the row by column index.
    ///
    /// # Errors
    ///
    /// Returns an error if the column index is out of bounds or if the value
    /// cannot be converted to i64.
    fn get_i64(&self, index: usize) -> CatalogResult<i64>;

    /// Get a String value from the row by column index.
    ///
    /// # Errors
    ///
    /// Returns an error if the column index is out of bounds or if the value
    /// cannot be converted to String.
    fn get_string(&self, index: usize) -> CatalogResult<String>;

    /// Get a bool value from the row by column index.
    ///
    /// # Errors
    ///
    /// Returns an error if the column index is out of bounds or if the value
    /// cannot be converted to bool.
    fn get_bool(&self, index: usize) -> CatalogResult<bool>;

    /// Get a blob (binary) value from the row by column index.
    ///
    /// # Errors
    ///
    /// Returns an error if the column index is out of bounds or if the value
    /// cannot be converted to a byte array.
    fn get_blob(&self, index: usize) -> CatalogResult<Vec<u8>>;

    /// Get an optional i64 value from the row by column index.
    ///
    /// # Errors
    ///
    /// Returns an error if the column index is out of bounds.
    fn get_optional_i64(&self, index: usize) -> CatalogResult<Option<i64>>;

    /// Get an optional String value from the row by column index.
    ///
    /// # Errors
    ///
    /// Returns an error if the column index is out of bounds.
    fn get_optional_string(&self, index: usize) -> CatalogResult<Option<String>>;
}

/// Trait for types that can be extracted from a metastore row.
pub trait MetastoreGetValue: Sized {
    /// Extract this type from a metastore value.
    ///
    /// # Errors
    ///
    /// Returns an error if the value cannot be converted to this type.
    fn from_value(value: &MetastoreValue) -> CatalogResult<Self>;
}

impl MetastoreGetValue for i64 {
    fn from_value(value: &MetastoreValue) -> CatalogResult<Self> {
        match value {
            MetastoreValue::Integer(v) => Ok(*v),
            _ => Err(super::catalog::CatalogError::Database {
                message: format!("Expected integer value, found {value}"),
            }),
        }
    }
}

impl MetastoreGetValue for String {
    fn from_value(value: &MetastoreValue) -> CatalogResult<Self> {
        match value {
            MetastoreValue::Text(v) => Ok(v.clone()),
            _ => Err(super::catalog::CatalogError::Database {
                message: format!("Expected text value, found {value}"),
            }),
        }
    }
}

impl MetastoreGetValue for bool {
    fn from_value(value: &MetastoreValue) -> CatalogResult<Self> {
        match value {
            MetastoreValue::Bool(v) => Ok(*v),
            MetastoreValue::Integer(v) => Ok(*v != 0),
            _ => Err(super::catalog::CatalogError::Database {
                message: format!("Expected boolean value, found {value}"),
            }),
        }
    }
}

impl MetastoreGetValue for Vec<u8> {
    fn from_value(value: &MetastoreValue) -> CatalogResult<Self> {
        match value {
            MetastoreValue::Blob(v) => Ok(v.clone()),
            _ => Err(super::catalog::CatalogError::Database {
                message: format!("Expected blob value, found {value}"),
            }),
        }
    }
}

impl<T: MetastoreGetValue> MetastoreGetValue for Option<T> {
    fn from_value(value: &MetastoreValue) -> CatalogResult<Self> {
        match value {
            MetastoreValue::Null => Ok(None),
            _ => Ok(Some(T::from_value(value)?)),
        }
    }
}

// Transaction support is backend-specific and cannot be expressed as a trait object
// due to generic methods. Each backend should provide its own concrete transaction type.
//
// For example:
// - SqliteMetastore provides SqliteTransaction
// - TursoMetastore provides TursoTransaction
//
// These concrete types should follow the RAII pattern:
// - Automatically rollback on drop unless explicitly committed
// - Provide execute(), query_row(), query() methods matching the MetastoreBackend API
// - Provide commit() and rollback() methods
/// The transaction must be explicitly committed via `commit()`, otherwise it will
/// automatically rollback when dropped.
#[async_trait]
pub trait MetastoreTransaction: Send + Sync {
    /// Execute a SQL statement that modifies data within the transaction.
    ///
    /// # Errors
    ///
    /// Returns an error if the statement cannot be executed.
    async fn execute(&self, params: ExecuteParams<'_>) -> CatalogResult<()>;

    /// Query a single row from the database within the transaction.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails or returns no rows.
    async fn query_row<F, T>(&self, params: QueryRowParams<'_>, f: F) -> CatalogResult<T>
    where
        F: FnOnce(&dyn MetastoreRow) -> CatalogResult<T> + Send + 'static,
        T: Send + 'static;

    /// Query multiple rows from the database within the transaction.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    async fn query<F, T>(&self, params: QueryParams<'_>, f: F) -> CatalogResult<Vec<T>>
    where
        F: Fn(&dyn MetastoreRow) -> CatalogResult<T> + Send + 'static,
        T: Send + 'static;

    /// Commit the transaction.
    ///
    /// After calling this, the transaction guard will not rollback on drop.
    ///
    /// # Errors
    ///
    /// Returns an error if the transaction cannot be committed.
    async fn commit(self) -> CatalogResult<()>;

    /// Explicitly rollback the transaction.
    ///
    /// This is optional as the transaction will automatically rollback on drop.
    ///
    /// # Errors
    ///
    /// Returns an error if the transaction cannot be rolled back.
    async fn rollback(self) -> CatalogResult<()>;
}

/// Trait for metastore backend implementations.
///
/// This trait abstracts the database layer for the Cayenne catalog, allowing
/// different storage backends (`SQLite`, Turso, etc.) to be used interchangeably.
#[async_trait]
pub trait MetastoreBackend: Send + Sync {
    /// Initialize the metastore schema (create tables if they don't exist).
    ///
    /// # Errors
    ///
    /// Returns an error if the schema cannot be initialized.
    async fn init_schema(&self) -> CatalogResult<()>;

    /// Execute a SQL statement that modifies data (INSERT, UPDATE, DELETE).
    ///
    /// # Errors
    ///
    /// Returns an error if the statement cannot be executed.
    async fn execute(&self, params: ExecuteParams<'_>) -> CatalogResult<()>;

    /// Execute a batch of SQL statements (separated by semicolons).
    ///
    /// # Errors
    ///
    /// Returns an error if any statement in the batch fails.
    async fn execute_batch(&self, sql: &str) -> CatalogResult<()>;

    /// Query a single row from the database.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails or returns no rows.
    async fn query_row<F, T>(&self, params: QueryRowParams<'_>, f: F) -> CatalogResult<T>
    where
        F: FnOnce(&dyn MetastoreRow) -> CatalogResult<T> + Send + 'static,
        T: Send + 'static;

    /// Query multiple rows from the database.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    async fn query<F, T>(&self, params: QueryParams<'_>, f: F) -> CatalogResult<Vec<T>>
    where
        F: Fn(&dyn MetastoreRow) -> CatalogResult<T> + Send + 'static,
        T: Send + 'static;

    /// Shutdown the metastore, performing any necessary cleanup.
    ///
    /// # Errors
    ///
    /// Returns an error if cleanup fails.
    async fn shutdown(&self) -> CatalogResult<()>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::CatalogError;

    #[tokio::test]
    async fn test_validate_existing_schema_matching_columns() {
        // When actual columns exactly match expected columns, validation passes.
        let result = validate_existing_schema(|table_name| async move {
            let expected = EXPECTED_TABLES
                .iter()
                .find(|t| t.name == table_name)
                .expect("table should exist in EXPECTED_TABLES");
            Ok(expected.columns.iter().map(|c| (*c).to_string()).collect())
        })
        .await;

        assert!(result.is_ok(), "Matching schema should pass validation");
    }

    #[tokio::test]
    async fn test_validate_existing_schema_empty_table_skipped() {
        // When a table returns no columns (freshly created), it should be skipped.
        let result = validate_existing_schema(|_table_name| async move {
            Ok(Vec::new()) // Simulate a table that doesn't exist yet
        })
        .await;

        assert!(
            result.is_ok(),
            "Empty (new) tables should be skipped during validation"
        );
    }

    #[tokio::test]
    async fn test_validate_existing_schema_extra_column_fails() {
        // When a table has an extra column not in the expected schema, validation fails.
        let result = validate_existing_schema(|table_name| async move {
            let expected = EXPECTED_TABLES
                .iter()
                .find(|t| t.name == table_name)
                .expect("table should exist in EXPECTED_TABLES");
            let mut cols: Vec<String> = expected.columns.iter().map(|c| (*c).to_string()).collect();
            cols.push("unexpected_new_column".to_string());
            Ok(cols)
        })
        .await;

        assert!(result.is_err(), "Extra column should fail validation");
        let err = result.expect_err("should be SchemaMismatch");
        assert!(
            matches!(err, CatalogError::SchemaMismatch { .. }),
            "Error should be SchemaMismatch, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_validate_existing_schema_missing_column_fails() {
        // When a table is missing a column from the expected schema, validation fails.
        let result = validate_existing_schema(|table_name| async move {
            let expected = EXPECTED_TABLES
                .iter()
                .find(|t| t.name == table_name)
                .expect("table should exist in EXPECTED_TABLES");
            // Return all columns except the last one
            let cols: Vec<String> = expected.columns[..expected.columns.len() - 1]
                .iter()
                .map(|c| (*c).to_string())
                .collect();
            Ok(cols)
        })
        .await;

        assert!(result.is_err(), "Missing column should fail validation");
        let err = result.expect_err("should be SchemaMismatch");
        assert!(
            matches!(err, CatalogError::SchemaMismatch { .. }),
            "Error should be SchemaMismatch, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_validate_existing_schema_reordered_columns_fails() {
        // When columns are in a different order, validation fails.
        let result = validate_existing_schema(|table_name| async move {
            let expected = EXPECTED_TABLES
                .iter()
                .find(|t| t.name == table_name)
                .expect("table should exist in EXPECTED_TABLES");
            let mut cols: Vec<String> = expected.columns.iter().map(|c| (*c).to_string()).collect();
            // Swap first two columns if there are at least 2
            if cols.len() >= 2 {
                cols.swap(0, 1);
            }
            Ok(cols)
        })
        .await;

        assert!(result.is_err(), "Reordered columns should fail validation");
        let err = result.expect_err("should be SchemaMismatch");
        assert!(
            matches!(err, CatalogError::SchemaMismatch { .. }),
            "Error should be SchemaMismatch, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_schema_mismatch_error_message_is_actionable() {
        // The error message should tell users what to do.
        let err = CatalogError::SchemaMismatch {
            table: "cayenne_table".to_string(),
        };
        let msg = err.to_string();
        assert!(
            msg.contains("clear your acceleration data"),
            "Error message should tell the user to clear data: {msg}"
        );
        assert!(
            msg.contains("cayenne_table"),
            "Error message should name the mismatched table: {msg}"
        );
    }
}
