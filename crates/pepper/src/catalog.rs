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

use super::metadata::{CreateTableOptions, DataFile, DeleteFile, TableMetadata, TableStats};
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

    /// Update the data path for a table (used for INSERT OVERWRITE).
    async fn update_table_path(&self, table_id: i64, new_path: &str) -> CatalogResult<()>;

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

    /// Begin a transaction.
    async fn begin_transaction(&self) -> CatalogResult<()>;

    /// Commit a transaction.
    async fn commit_transaction(&self) -> CatalogResult<()>;

    /// Rollback a transaction.
    async fn rollback_transaction(&self) -> CatalogResult<()>;
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
