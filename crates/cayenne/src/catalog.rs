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

//! Trait abstraction for metadata catalog operations.
//!
//! This trait defines the interface for managing table metadata
//! and file references. It can be implemented by different RDBMS backends
//! (`SQLite`, `PostgreSQL`, etc.).

use super::metadata::{CreateTableOptions, DeleteFile, PartitionMetadata, TableMetadata};
use async_trait::async_trait;
use snafu::Snafu;
use std::sync::Arc;

/// Error type for catalog operations.
#[expect(missing_docs)]
#[derive(Debug, Snafu)]
pub enum CatalogError {
    /// Database error
    #[snafu(display("Database error: {message}"))]
    Database {
        /// Error message
        message: String,
    },

    /// Table not found
    #[snafu(display("Table not found: {table_name}"))]
    TableNotFound {
        /// Name of the table that was not found
        table_name: String,
    },

    /// Table already exists
    #[snafu(display("Table already exists: {table_name}"))]
    TableAlreadyExists {
        /// Name of the table that already exists
        table_name: String,
    },

    /// Invalid operation
    #[snafu(display("Invalid operation: {message} {source}"))]
    InvalidOperation {
        /// Description of the invalid operation
        message: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// IO error
    #[snafu(display("IO error: {source}"))]
    Io {
        /// The underlying IO error
        source: std::io::Error,
    },

    /// `SQLite` error
    #[snafu(transparent)]
    Sqlite {
        /// The underlying `SQLite` error
        source: rusqlite::Error,
    },

    /// Task join error
    #[snafu(transparent)]
    TaskJoin {
        /// The underlying task join error
        source: tokio::task::JoinError,
    },

    /// IO error (from `std::io::Error`)
    #[snafu(transparent)]
    IoError {
        /// The underlying IO error  
        source: std::io::Error,
    },

    /// Lock poisoning error
    #[snafu(display("Lock poisoned during {operation}: a thread panicked while holding this lock. This indicates an internal error that requires restarting the runtime."))]
    LockPoisoned {
        /// The operation that failed due to lock poisoning
        operation: String,
    },

    #[snafu(display("Invalid database path: {path}"))]
    InvalidDatabasePath { path: String },

    #[snafu(display("The function '{function}' is not implemented"))]
    NotImplemented { function: String },

    #[snafu(display(
        "Deletion vectors require non-negative row IDs, found negative values: {row_ids}"
    ))]
    NegativeRowId { row_ids: String },

    #[snafu(display("Failed to get catalog table. {source}"))]
    FailedToGetTable { source: Box<CatalogError> },

    #[snafu(display("Failed to get current snapshot. {source}"))]
    FailedToGetCurrentSnapshot { source: Box<CatalogError> },

    #[snafu(display("Failed to set current snapshot. {source}"))]
    FailedToSetCurrentSnapshot { source: Box<CatalogError> },

    #[snafu(display("Failed to create catalog table. {source}"))]
    FailedToCreateTable { source: Box<CatalogError> },

    #[snafu(display("Failed to add delete file. {source}"))]
    FailedToAddDeleteFile { source: Box<CatalogError> },

    #[snafu(display("Failed to get delete files for table. {source}"))]
    FailedToGetTableDeleteFiles { source: Box<CatalogError> },

    #[snafu(display("Failed to add partition. {source}"))]
    FailedToAddPartition { source: Box<CatalogError> },

    #[snafu(display("Failed to get partitions. {source}"))]
    FailedToGetPartitions { source: Box<CatalogError> },

    #[snafu(display("Failed to get partition. {source}"))]
    FailedToGetPartition { source: Box<CatalogError> },

    #[snafu(display(
        "Multiple partitions found for table ID {table_id} and partition value '{partition_value}'"
    ))]
    InvalidPartitionCount {
        table_id: i64,
        partition_value: String,
    },

    #[snafu(display("Failed to update partition stats. {source}"))]
    FailedToUpdatePartitionStats { source: Box<CatalogError> },

    #[snafu(display("Failed to get partition stats. {source}"))]
    FailedToGetPartitionStats { source: Box<CatalogError> },

    #[snafu(display("Failed to get partition data files. {source}"))]
    FailedToGetPartitionDataFiles { source: Box<CatalogError> },

    #[snafu(display(
        "Turso backend requested but 'turso' feature is not enabled. Enable with --features turso"
    ))]
    TursoNotEnabled,
}

/// Result type for catalog operations.
pub type CatalogResult<T> = std::result::Result<T, CatalogError>;

// Transaction support is currently not exposed at the catalog level.
// Each catalog implementation can use backend-specific transactions internally
// to ensure atomicity of operations.
//
// Future work: Expose catalog-level transactions when needed.

/// Trait for metadata catalog operations.
///
/// This trait provides the core operations needed to manage a Cayenne catalog,
/// including table creation and file tracking.
#[async_trait]
pub trait MetadataCatalog: Send + Sync {
    /// Initialize the catalog, creating necessary tables if they don't exist.
    async fn init(&self) -> CatalogResult<()>;

    /// Create a new table.
    async fn create_table(&self, options: CreateTableOptions) -> CatalogResult<i64>;

    /// Get table metadata by name.
    async fn get_table(&self, table_name: &str) -> CatalogResult<TableMetadata>;

    /// Set the current snapshot ID for a table (`UUIDv7` string).
    async fn set_current_snapshot(&self, table_id: i64, snapshot_id: &str) -> CatalogResult<()>;

    /// Add a delete file (deletion vector) for a data file.
    ///
    /// Tracks a deletion vector file that marks rows as deleted in a specific
    /// virtual file (`ListingTable`).
    async fn add_delete_file(&self, delete_file: DeleteFile) -> CatalogResult<i64>;

    /// Get all active delete files for a table (across all virtual files).
    async fn get_table_delete_files(&self, table_id: i64) -> CatalogResult<Vec<DeleteFile>>;

    /// Add a partition to a table.
    async fn add_partition(&self, partition: PartitionMetadata) -> CatalogResult<i64>;

    /// Get all partitions for a table.
    async fn get_partitions(&self, table_id: i64) -> CatalogResult<Vec<PartitionMetadata>>;

    /// Shutdown the catalog, performing any necessary cleanup (e.g., WAL checkpoint, optimize).
    /// Default implementation does nothing.
    async fn shutdown(&self) -> CatalogResult<()> {
        Ok(())
    }
}

/// Factory trait for creating catalog instances.
pub trait CatalogFactory: Send + Sync {
    /// Create a new catalog instance.
    ///
    /// # Errors
    ///
    /// Returns an error if the catalog cannot be created.
    fn create(&self, connection_string: &str) -> CatalogResult<Arc<dyn MetadataCatalog>>;
}
