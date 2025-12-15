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
    /// Null value
    Null,
}

impl Display for MetastoreValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MetastoreValue::Integer(v) => write!(f, "integer {v}"),
            MetastoreValue::Text(v) => write!(f, "text '{v}'"),
            MetastoreValue::Bool(v) => write!(f, "bool {v}"),
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
