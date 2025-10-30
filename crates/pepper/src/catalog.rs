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

use super::metadata::{
    CreateTableOptions, DataFile, DeleteFile, PartitionMetadata, PartitionStats, TableMetadata,
    TableStats,
};
use async_trait::async_trait;
use snafu::Snafu;
use std::sync::Arc;

/// Error type for catalog operations.
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
    #[snafu(display("Invalid operation: {message}"))]
    InvalidOperation {
        /// Description of the invalid operation
        message: String,
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
}

/// Result type for catalog operations.
pub type CatalogResult<T> = std::result::Result<T, CatalogError>;

/// Transaction guard for catalog operations that automatically rolls back on drop unless explicitly committed.
///
/// This follows the RAII pattern used by rusqlite and other database libraries.
#[async_trait]
// Transaction support is currently not exposed at the catalog level.
// Each catalog implementation can use backend-specific transactions internally
// to ensure atomicity of operations.
//
// Future work: Expose catalog-level transactions when needed.

/// Trait for metadata catalog operations.
///
/// This trait provides the core operations needed to manage a Pepper catalog,
/// including table creation and file tracking.
#[async_trait]
pub trait MetadataCatalog: Send + Sync {
    /// Initialize the catalog, creating necessary tables if they don't exist.
    async fn init(&self) -> CatalogResult<()>;

    /// Create a new table.
    async fn create_table(&self, options: CreateTableOptions) -> CatalogResult<i64>;

    /// Get table metadata by name.
    async fn get_table(&self, table_name: &str) -> CatalogResult<TableMetadata>;

    /// Get table metadata by ID.
    async fn get_table_by_id(&self, table_id: i64) -> CatalogResult<TableMetadata>;

    /// Get the current snapshot ID for a table (`UUIDv7` string).
    /// All tables have a snapshot (created on table initialization).
    async fn get_current_snapshot(&self, table_id: i64) -> CatalogResult<String>;

    /// Set the current snapshot ID for a table (`UUIDv7` string).
    async fn set_current_snapshot(&self, table_id: i64, snapshot_id: &str) -> CatalogResult<()>;

    /// List all active tables.
    async fn list_tables(&self) -> CatalogResult<Vec<TableMetadata>>;

    /// Drop a table.
    async fn drop_table(&self, table_name: &str) -> CatalogResult<()>;

    /// Add a data file (virtual file/`ListingTable`) to a table.
    ///
    /// Creates metadata for a new virtual file. The `data_file.path` should point
    /// to a unique directory where the `ListingTable`'s Vortex files will be stored.
    async fn add_data_file(&self, data_file: DataFile) -> CatalogResult<i64>;

    /// Get all active data files (virtual files/`ListingTables`) for a table.
    ///
    /// Returns metadata for all virtual files that make up this table. Each `DataFile`
    /// represents a separate `ListingTable` at its own directory.
    async fn get_data_files(&self, table_id: i64) -> CatalogResult<Vec<DataFile>>;

    /// Add a delete file (deletion vector) for a data file.
    ///
    /// Tracks a deletion vector file that marks rows as deleted in a specific
    /// virtual file (`ListingTable`).
    async fn add_delete_file(&self, delete_file: DeleteFile) -> CatalogResult<i64>;

    /// Get all active delete files for a specific data file (virtual file).
    async fn get_delete_files(&self, data_file_id: i64) -> CatalogResult<Vec<DeleteFile>>;

    /// Get all active delete files for a table (across all virtual files).
    async fn get_table_delete_files(&self, table_id: i64) -> CatalogResult<Vec<DeleteFile>>;

    /// Get statistics for a table.
    async fn get_table_stats(&self, table_id: i64) -> CatalogResult<TableStats>;

    /// Add a partition to a table.
    async fn add_partition(&self, partition: PartitionMetadata) -> CatalogResult<i64>;

    /// Get all partitions for a table.
    async fn get_partitions(&self, table_id: i64) -> CatalogResult<Vec<PartitionMetadata>>;

    /// Get a specific partition by table ID and partition value.
    async fn get_partition(
        &self,
        table_id: i64,
        partition_value: &str,
    ) -> CatalogResult<Option<PartitionMetadata>>;

    /// Update partition statistics (record count and file size).
    async fn update_partition_stats(
        &self,
        partition_id: i64,
        record_count: i64,
        file_size_bytes: i64,
    ) -> CatalogResult<()>;

    /// Get partition statistics.
    async fn get_partition_stats(&self, partition_id: i64) -> CatalogResult<PartitionStats>;

    /// Get data files belonging to a specific partition.
    async fn get_partition_data_files(&self, partition_id: i64) -> CatalogResult<Vec<DataFile>>;

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
