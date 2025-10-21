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

//! `SQLite` implementation of the metadata catalog for Pepper.

use super::catalog::{CatalogError, CatalogResult, MetadataCatalog};
use super::metadata::{CreateTableOptions, DataFile, DeleteFile, TableMetadata, TableStats};
use async_trait::async_trait;
use std::path::Path;
use std::sync::Arc;

/// `SQLite`-based metadata catalog for Pepper.
///
/// The catalog manages metadata for tables and their "virtual files". In Pepper,
/// a "file" is not a single physical file, but rather a Vortex `ListingTable` at a
/// unique directory. The `SQLite` database tracks:
/// - Tables and their schemas
/// - `DataFile` entries (metadata for each `ListingTable`/virtual file)
/// - `DeleteFile` entries (deletion vectors for each virtual file)
///
/// Operations on files (read, append, delete, stats) are delegated to the
/// corresponding Vortex `ListingTable` provider.
pub struct PepperCatalog {
    connection_string: String,
    // Using rusqlite with tokio requires careful handling
    // For now, we'll use a simple approach with a mutex
    _marker: std::marker::PhantomData<()>,
}

impl PepperCatalog {
    /// Create a new Pepper catalog.
    pub fn new(connection_string: impl Into<String>) -> Self {
        Self {
            connection_string: connection_string.into(),
            _marker: std::marker::PhantomData,
        }
    }

    /// Get the database file path from the connection string.
    fn db_path(&self) -> &str {
        self.connection_string
            .strip_prefix("sqlite://")
            .unwrap_or(&self.connection_string)
    }

    /// Generate a unique directory path for a new virtual file (`ListingTable`).
    ///
    /// Returns a relative path like `file_000001/` that will be combined with
    /// the table's base path to create the full `ListingTable` directory.
    #[allow(dead_code)]
    fn generate_file_path(file_id: i64) -> String {
        format!("file_{file_id:06}/")
    }

    /// Schema for the `pepper_metadata` table that tracks next IDs.
    const METADATA_TABLE_DDL: &'static str = r"
        CREATE TABLE IF NOT EXISTS pepper_metadata (
            key TEXT PRIMARY KEY,
            value BIGINT NOT NULL
        )
    ";

    /// Schema for the `pepper_table` table.
    const TABLE_TABLE_DDL: &'static str = r"
        CREATE TABLE IF NOT EXISTS pepper_table (
            table_id BIGINT PRIMARY KEY,
            table_uuid TEXT NOT NULL,
            table_name TEXT NOT NULL,
            path TEXT NOT NULL,
            path_is_relative BOOLEAN NOT NULL,
            schema_json TEXT NOT NULL,
            primary_key_json TEXT
        )
    ";

    /// Schema for the `pepper_data_file` table.
    const DATA_FILE_TABLE_DDL: &'static str = r"
        CREATE TABLE IF NOT EXISTS pepper_data_file (
            data_file_id BIGINT PRIMARY KEY,
            table_id BIGINT NOT NULL,
            file_order BIGINT NOT NULL,
            path TEXT NOT NULL,
            path_is_relative BOOLEAN NOT NULL,
            file_format TEXT NOT NULL,
            record_count BIGINT NOT NULL,
            file_size_bytes BIGINT NOT NULL,
            row_id_start BIGINT NOT NULL
        )
    ";

    /// Schema for the `pepper_delete_file` table.
    const DELETE_FILE_TABLE_DDL: &'static str = r"
        CREATE TABLE IF NOT EXISTS pepper_delete_file (
            delete_file_id BIGINT PRIMARY KEY,
            table_id BIGINT NOT NULL,
            data_file_id BIGINT NOT NULL,
            path TEXT NOT NULL,
            path_is_relative BOOLEAN NOT NULL,
            format TEXT NOT NULL,
            delete_count BIGINT NOT NULL,
            file_size_bytes BIGINT NOT NULL
        )
    ";

    /// Initialize metadata tables.
    fn initialize_schema(conn: &rusqlite::Connection) -> CatalogResult<()> {
        // Create tables in a transaction
        conn.execute_batch(&format!(
            "{}; {}; {}; {};",
            Self::METADATA_TABLE_DDL,
            Self::TABLE_TABLE_DDL,
            Self::DATA_FILE_TABLE_DDL,
            Self::DELETE_FILE_TABLE_DDL
        ))?;

        // Initialize metadata with next IDs if not exists
        conn.execute(
            "INSERT OR IGNORE INTO pepper_metadata (key, value) VALUES ('next_catalog_id', 1)",
            [],
        )?;
        conn.execute(
            "INSERT OR IGNORE INTO pepper_metadata (key, value) VALUES ('next_file_id', 1)",
            [],
        )?;

        Ok(())
    }
}

#[async_trait]
impl MetadataCatalog for PepperCatalog {
    async fn init(&self) -> CatalogResult<()> {
        // Create database file if it doesn't exist
        let db_path = self.db_path();
        let db_dir = Path::new(db_path)
            .parent()
            .ok_or_else(|| CatalogError::InvalidOperation {
                message: "Invalid database path".to_string(),
            })?;

        if !db_dir.exists() {
            tokio::fs::create_dir_all(db_dir).await?;
        }

        // Open connection and initialize schema with write permissions
        let db_path_owned = db_path.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = rusqlite::Connection::open_with_flags(
                &db_path_owned,
                rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                    | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
            )?;
            Self::initialize_schema(&conn)?;
            Ok::<(), CatalogError>(())
        })
        .await??;

        Ok(())
    }

    async fn create_table(&self, options: CreateTableOptions) -> CatalogResult<i64> {
        let db_path_owned = self.db_path().to_string();
        let table_name = options.table_name.clone();
        let base_path = options.base_path.clone();

        // Serialize schema using Arrow IPC format (supports all Arrow types)
        let schema_json = {
            use arrow_ipc::writer::IpcWriteOptions;
            let write_options = IpcWriteOptions::default();
            let arrow_flight::IpcMessage(schema_bytes) =
                arrow_flight::SchemaAsIpc::new(options.schema.as_ref(), &write_options)
                    .try_into()
                    .map_err(
                        |e: arrow_schema::ArrowError| CatalogError::InvalidOperation {
                            message: format!("Failed to serialize schema: {e}"),
                        },
                    )?;

            // Convert to base64 for storage in TEXT column
            base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                schema_bytes.as_ref(),
            )
        };

        let primary_key_json = if options.primary_key.is_empty() {
            None
        } else {
            Some(serde_json::to_string(&options.primary_key).map_err(|e| {
                CatalogError::InvalidOperation {
                    message: format!("Failed to serialize primary key: {e}"),
                }
            })?)
        };

        tokio::task::spawn_blocking(move || {
            let conn = rusqlite::Connection::open_with_flags(
                &db_path_owned,
                rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                    | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
            )?;

            // Start transaction
            conn.execute("BEGIN TRANSACTION", [])?;

            // Get next catalog ID (for table_id)
            let next_catalog_id: i64 = conn.query_row(
                "SELECT value FROM pepper_metadata WHERE key = 'next_catalog_id'",
                [],
                |row| row.get(0),
            )?;

            let table_id = next_catalog_id;

            // Generate table UUID
            let table_uuid = uuid::Uuid::now_v7().to_string();

            // Insert table metadata
            conn.execute(
                r"
                INSERT INTO pepper_table (
                    table_id, table_uuid,
                    table_name, path, path_is_relative, schema_json, primary_key_json
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                ",
                rusqlite::params![
                    table_id,
                    table_uuid,
                    table_name,
                    base_path,
                    false, // path_is_relative - using absolute paths for now
                    schema_json,
                    primary_key_json,
                ],
            )?;

            // Update next_catalog_id in metadata
            conn.execute(
                "UPDATE pepper_metadata SET value = ?1 WHERE key = 'next_catalog_id'",
                [next_catalog_id + 1],
            )?;

            // Commit transaction
            conn.execute("COMMIT", [])?;

            Ok::<i64, CatalogError>(table_id)
        })
        .await?
    }

    async fn get_table(&self, table_name: &str) -> CatalogResult<TableMetadata> {
        let db_path_owned = self.db_path().to_string();
        let table_name_owned = table_name.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = rusqlite::Connection::open_with_flags(
                &db_path_owned,
                rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            )?;

            // Query for the table
            let mut stmt = conn.prepare(
                r"
                SELECT table_id, table_uuid,
                       table_name, path, path_is_relative, schema_json, primary_key_json
                FROM pepper_table
                WHERE table_name = ?1
                LIMIT 1
                ",
            )?;

            let table_metadata = stmt
                .query_row([&table_name_owned], |row| {
                    let table_id: i64 = row.get(0)?;
                    let table_uuid: String = row.get(1)?;
                    let table_name: String = row.get(2)?;
                    let path: String = row.get(3)?;
                    let _path_is_relative: bool = row.get(4)?;
                    let schema_json: String = row.get(5)?;
                    let primary_key_json: Option<String> = row.get(6)?;

                    // Deserialize schema using Arrow IPC format
                    let schema = {
                        use base64::Engine;
                        use bytes::Bytes;

                        let schema_bytes = base64::engine::general_purpose::STANDARD
                            .decode(&schema_json)
                            .map_err(|_| rusqlite::Error::InvalidQuery)?;

                        let ipc_message = arrow_flight::IpcMessage(Bytes::from(schema_bytes));
                        arrow_schema::Schema::try_from(ipc_message)
                            .map_err(|_| rusqlite::Error::InvalidQuery)?
                    };

                    let schema = Arc::new(schema);

                    // Parse primary key
                    let primary_key = if let Some(pk_json) = primary_key_json {
                        serde_json::from_str(&pk_json).unwrap_or_default()
                    } else {
                        vec![]
                    };

                    Ok(TableMetadata {
                        table_id,
                        table_uuid,
                        table_name,
                        path,
                        path_is_relative: _path_is_relative,
                        schema,
                        primary_key,
                    })
                })
                .map_err(|e| match e {
                    rusqlite::Error::QueryReturnedNoRows => CatalogError::TableNotFound {
                        table_name: table_name_owned.clone(),
                    },
                    e => CatalogError::from(e),
                })?;

            Ok::<TableMetadata, CatalogError>(table_metadata)
        })
        .await?
    }

    async fn get_table_by_id(&self, table_id: i64) -> CatalogResult<TableMetadata> {
        // Implementation would query pepper_table by ID
        Err(CatalogError::TableNotFound {
            table_name: format!("id:{table_id}"),
        })
    }

    async fn list_tables(&self) -> CatalogResult<Vec<TableMetadata>> {
        // Implementation would query all active tables
        Ok(vec![])
    }

    async fn drop_table(&self, _table_name: &str) -> CatalogResult<()> {
        // Implementation would delete table from catalog
        Err(CatalogError::InvalidOperation {
            message: "Not yet implemented".to_string(),
        })
    }

    async fn add_data_file(&self, _data_file: DataFile) -> CatalogResult<i64> {
        // Implementation would insert into pepper_data_file
        Err(CatalogError::InvalidOperation {
            message: "Not yet implemented".to_string(),
        })
    }

    async fn get_data_files(&self, _table_id: i64) -> CatalogResult<Vec<DataFile>> {
        // Implementation would query active data files for table
        Ok(vec![])
    }

    async fn add_delete_file(&self, _delete_file: DeleteFile) -> CatalogResult<i64> {
        // Implementation would insert into pepper_delete_file
        Err(CatalogError::InvalidOperation {
            message: "Not yet implemented".to_string(),
        })
    }

    async fn get_delete_files(&self, _data_file_id: i64) -> CatalogResult<Vec<DeleteFile>> {
        // Implementation would query delete files for specific data file
        Ok(vec![])
    }

    async fn get_table_delete_files(&self, _table_id: i64) -> CatalogResult<Vec<DeleteFile>> {
        // Implementation would query all delete files for table
        Ok(vec![])
    }

    async fn get_table_stats(&self, _table_id: i64) -> CatalogResult<TableStats> {
        // Implementation would aggregate stats from data and delete files
        Ok(TableStats::default())
    }

    async fn begin_transaction(&self) -> CatalogResult<()> {
        // Implementation would begin SQLite transaction
        Ok(())
    }

    async fn commit_transaction(&self) -> CatalogResult<()> {
        // Implementation would commit SQLite transaction
        Ok(())
    }

    async fn rollback_transaction(&self) -> CatalogResult<()> {
        // Implementation would rollback SQLite transaction
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_catalog_creation() {
        let _catalog = PepperCatalog::new("sqlite://./test.db");
        // Tests will be added once implementation is complete
    }
}
