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

//! Configurable metadata catalog for Pepper.
//!
//! Supports multiple backends: SQLite and Arrow/Feather.

use super::arrow_backend::ArrowBackend;
use super::backend::CatalogBackend;
use super::catalog::{CatalogResult, MetadataCatalog};
use super::metadata::{CreateTableOptions, DataFile, DeleteFile, TableMetadata, TableStats};
use super::sqlite_backend::SqliteBackend;
use async_trait::async_trait;
use std::sync::Arc;

/// Metadata catalog for Pepper with configurable backend.
///
/// The catalog manages metadata for tables and their "virtual files". In Pepper,
/// a "file" is not a single physical file, but rather a Vortex `ListingTable` at a
/// unique directory. The catalog tracks:
/// - Tables and their schemas
/// - `DataFile` entries (metadata for each `ListingTable`/virtual file)
/// - `DeleteFile` entries (deletion vectors for each virtual file)
///
/// Operations on files (read, append, delete, stats) are delegated to the
/// corresponding Vortex `ListingTable` provider.
///
/// # Connection Strings
///
/// - `sqlite://path/to/database.db` - SQLite backend
/// - `arrow://path/to/directory` - Arrow/Feather backend
pub struct PepperCatalog {
    backend: Arc<dyn CatalogBackend>,
}

impl PepperCatalog {
    /// Create a new Pepper catalog with the specified backend.
    ///
    /// # Arguments
    ///
    /// * `connection_string` - Connection string specifying the backend type and location:
    ///   - `sqlite://path/to/database.db` - SQLite backend
    ///   - `arrow://path/to/directory` - Arrow/Feather backend (stores metadata as Arrow IPC files)
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let catalog = PepperCatalog::new("sqlite:///data/pepper.db");
    /// let catalog = PepperCatalog::new("arrow:///data/pepper_metadata");
    /// ```
    pub fn new(connection_string: impl Into<String>) -> Self {
        let conn_str = connection_string.into();
        let backend: Arc<dyn CatalogBackend> = Self::create_backend(&conn_str);
        Self { backend }
    }

    /// Create the appropriate backend based on the connection string.
    fn create_backend(connection_string: &str) -> Arc<dyn CatalogBackend> {
        if let Some(path) = connection_string.strip_prefix("sqlite://") {
            Arc::new(SqliteBackend::new(path))
        } else if let Some(path) = connection_string.strip_prefix("arrow://") {
            Arc::new(ArrowBackend::new(path))
        } else {
            // Default to SQLite for backward compatibility
            Arc::new(SqliteBackend::new(connection_string))
        }
    }
}

#[async_trait]
impl MetadataCatalog for PepperCatalog {
    async fn init(&self) -> CatalogResult<()> {
        self.backend.init().await
    }

    async fn create_table(&self, options: CreateTableOptions) -> CatalogResult<i64> {
        self.backend.create_table(options).await
    }

    async fn get_table(&self, table_name: &str) -> CatalogResult<TableMetadata> {
        self.backend.get_table(table_name).await
    }

    async fn get_table_by_id(&self, table_id: i64) -> CatalogResult<TableMetadata> {
        self.backend.get_table_by_id(table_id).await
    }

    async fn list_tables(&self) -> CatalogResult<Vec<TableMetadata>> {
        self.backend.list_tables().await
    }

    async fn drop_table(&self, table_name: &str) -> CatalogResult<()> {
        self.backend.drop_table(table_name).await
    }

    async fn add_data_file(&self, data_file: DataFile) -> CatalogResult<i64> {
        self.backend.add_data_file(data_file).await
    }

    async fn get_data_files(&self, table_id: i64) -> CatalogResult<Vec<DataFile>> {
        self.backend.get_data_files(table_id).await
    }

    async fn add_delete_file(&self, delete_file: DeleteFile) -> CatalogResult<i64> {
        self.backend.add_delete_file(delete_file).await
    }

    async fn get_delete_files(&self, data_file_id: i64) -> CatalogResult<Vec<DeleteFile>> {
        self.backend.get_delete_files(data_file_id).await
    }

    async fn get_table_delete_files(&self, table_id: i64) -> CatalogResult<Vec<DeleteFile>> {
        self.backend.get_table_delete_files(table_id).await
    }

    async fn get_table_stats(&self, table_id: i64) -> CatalogResult<TableStats> {
        self.backend.get_table_stats(table_id).await
    }

    async fn begin_transaction(&self) -> CatalogResult<()> {
        self.backend.begin_transaction().await
    }

    async fn commit_transaction(&self) -> CatalogResult<()> {
        self.backend.commit_transaction().await
    }

    async fn rollback_transaction(&self) -> CatalogResult<()> {
        self.backend.rollback_transaction().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_catalog_creation_sqlite() {
        let _catalog = PepperCatalog::new("sqlite://./test.db");
        // Tests will be added once implementation is complete
    }

    #[tokio::test]
    async fn test_catalog_creation_arrow() {
        let _catalog = PepperCatalog::new("arrow://./test_arrow_metadata");
        // Tests will be added once implementation is complete
    }
}
