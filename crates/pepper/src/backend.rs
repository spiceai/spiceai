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

//! Backend trait for Pepper catalog implementations.
//!
//! This module defines the internal trait that different storage backends
//! (SQLite, Arrow/Feather) must implement.

use super::catalog::CatalogResult;
use super::metadata::{CreateTableOptions, DataFile, DeleteFile, TableMetadata, TableStats};
use async_trait::async_trait;

/// Internal trait for catalog backend implementations.
///
/// This trait is implemented by specific storage backends (SQLite, Arrow/Feather)
/// and provides the actual storage operations.
#[async_trait]
pub(crate) trait CatalogBackend: Send + Sync {
    /// Initialize the backend, creating necessary storage structures.
    async fn init(&self) -> CatalogResult<()>;

    /// Create a new table.
    async fn create_table(&self, options: CreateTableOptions) -> CatalogResult<i64>;

    /// Get table metadata by name.
    async fn get_table(&self, table_name: &str) -> CatalogResult<TableMetadata>;

    /// Get table metadata by ID.
    async fn get_table_by_id(&self, table_id: i64) -> CatalogResult<TableMetadata>;

    /// List all active tables.
    async fn list_tables(&self) -> CatalogResult<Vec<TableMetadata>>;

    /// Drop a table.
    async fn drop_table(&self, table_name: &str) -> CatalogResult<()>;

    /// Add a data file to a table.
    async fn add_data_file(&self, data_file: DataFile) -> CatalogResult<i64>;

    /// Get all active data files for a table.
    async fn get_data_files(&self, table_id: i64) -> CatalogResult<Vec<DataFile>>;

    /// Add a delete file for a data file.
    async fn add_delete_file(&self, delete_file: DeleteFile) -> CatalogResult<i64>;

    /// Get all active delete files for a specific data file.
    async fn get_delete_files(&self, data_file_id: i64) -> CatalogResult<Vec<DeleteFile>>;

    /// Get all active delete files for a table.
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
