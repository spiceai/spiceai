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
use super::metadata::{
    CreateTableOptions, DataFile, DeleteFile, PartitionMetadata, PartitionStats, TableMetadata,
    TableStats,
};
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
///
/// ## Concurrency Model
///
/// The catalog uses `SQLite` with WAL (Write-Ahead Logging) mode which allows:
/// - Multiple concurrent readers
/// - One writer at a time (serialized by `SQLite` itself)
///
/// Each async operation opens a new connection with proper configuration (WAL mode,
/// busy timeout, etc.). `SQLite`'s internal locking with the 5-second busy timeout
/// handles write serialization automatically, eliminating the need for application-level
/// locks.
pub struct PepperCatalog {
    connection_string: String,
}

impl PepperCatalog {
    /// Create a new Pepper catalog.
    pub fn new(connection_string: impl Into<String>) -> Self {
        Self {
            connection_string: connection_string.into(),
        }
    }

    /// Get the database file path from the connection string.
    fn db_path(&self) -> &str {
        self.connection_string
            .strip_prefix("sqlite://")
            .unwrap_or(&self.connection_string)
    }

    /// Open a `SQLite` connection configured for concurrent access.
    ///
    /// Applies performance optimizations based on `SQLite` best practices:
    /// - WAL mode for non-blocking reads/writes
    /// - Busy timeout to reduce lock contention errors
    /// - NORMAL synchronous mode (safe with WAL)
    /// - Memory cache and temp storage for performance
    /// - Foreign keys enabled
    fn open_connection(db_path: &str, read_only: bool) -> CatalogResult<rusqlite::Connection> {
        let flags = if read_only {
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
        } else {
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE
        };

        let conn = rusqlite::Connection::open_with_flags(db_path, flags)?;

        // Enable WAL mode for better concurrent access (allows multiple readers with one writer)
        if !read_only {
            conn.pragma_update(None, "journal_mode", "WAL")?;
        }

        // SQLite will wait 5 seconds to obtain a lock before returning SQLITE_BUSY errors
        conn.busy_timeout(std::time::Duration::from_secs(5))?;

        // NORMAL synchronous mode is safe with WAL and more performant than FULL
        conn.pragma_update(None, "synchronous", "NORMAL")?;

        // 32MB cache size (negative number means kilobytes)
        conn.pragma_update(None, "cache_size", -32000)?;

        // Enable foreign keys (disabled by default for historical reasons)
        conn.pragma_update(None, "foreign_keys", true)?;

        // Store temporary tables in memory for better performance
        conn.pragma_update(None, "temp_store", "memory")?;

        Ok(conn)
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
            primary_key_json TEXT,
            current_snapshot_id TEXT NOT NULL DEFAULT '',
            partition_column TEXT
        )
    ";

    /// Schema for the `pepper_data_file` table.
    const DATA_FILE_TABLE_DDL: &'static str = r"
        CREATE TABLE IF NOT EXISTS pepper_data_file (
            data_file_id BIGINT PRIMARY KEY,
            table_id BIGINT NOT NULL,
            partition_id BIGINT,
            file_order BIGINT NOT NULL,
            path TEXT NOT NULL,
            path_is_relative BOOLEAN NOT NULL,
            file_format TEXT NOT NULL,
            record_count BIGINT NOT NULL,
            file_size_bytes BIGINT NOT NULL,
            row_id_start BIGINT NOT NULL,
            FOREIGN KEY(partition_id) REFERENCES pepper_partition(partition_id) ON DELETE SET NULL
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

    /// Schema for the `pepper_partition` table.
    const PARTITION_TABLE_DDL: &'static str = r"
        CREATE TABLE IF NOT EXISTS pepper_partition (
            partition_id BIGINT PRIMARY KEY,
            table_id BIGINT NOT NULL,
            partition_column TEXT NOT NULL,
            partition_value TEXT NOT NULL,
            path TEXT NOT NULL,
            path_is_relative BOOLEAN NOT NULL,
            record_count BIGINT NOT NULL DEFAULT 0,
            file_size_bytes BIGINT NOT NULL DEFAULT 0,
            UNIQUE(table_id, partition_value)
        )
    ";

    /// Initialize metadata tables.
    fn initialize_schema(conn: &rusqlite::Connection) -> CatalogResult<()> {
        // Create tables in a transaction
        conn.execute_batch(&format!(
            "{}; {}; {}; {}; {};",
            Self::METADATA_TABLE_DDL,
            Self::TABLE_TABLE_DDL,
            Self::DATA_FILE_TABLE_DDL,
            Self::DELETE_FILE_TABLE_DDL,
            Self::PARTITION_TABLE_DDL
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
        conn.execute(
            "INSERT OR IGNORE INTO pepper_metadata (key, value) VALUES ('next_partition_id', 1)",
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

        // Initialize schema using connection with WAL mode
        let db_path_owned = self.db_path().to_string();
        tokio::task::spawn_blocking(move || {
            let conn = Self::open_connection(&db_path_owned, false)?;
            Self::initialize_schema(&conn)?;
            Ok::<(), CatalogError>(())
        })
        .await??;

        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    async fn create_table(&self, options: CreateTableOptions) -> CatalogResult<i64> {
        /// Result of attempting to create a table in the catalog
        enum CreateTableResult {
            /// Table was created successfully with the given snapshot ID
            Created {
                table_id: i64,
                snapshot_id: String,
                base_path: String,
            },
            /// Table already existed with the given ID
            AlreadyExists { table_id: i64 },
        }

        let table_name = options.table_name.clone();
        let base_path = options.base_path.clone();
        let db_path_owned = self.db_path().to_string();

        // Check if table already exists first (read-only check)
        let db_path_for_check = db_path_owned.clone();
        let table_name_check = table_name.clone();
        let existing_table_id: Option<i64> = tokio::task::spawn_blocking(move || {
            let conn = Self::open_connection(&db_path_for_check, true)?;
            match conn.query_row(
                "SELECT table_id FROM pepper_table WHERE table_name = ?1",
                [&table_name_check],
                |row| row.get(0),
            ) {
                Ok(id) => Ok(Some(id)),
                Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
                Err(e) => Err(CatalogError::from(e)),
            }
        })
        .await??;

        if let Some(table_id) = existing_table_id {
            // Table already exists, return its ID
            return Ok(table_id);
        }

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

        let partition_column = options.partition_column.clone();

        let create_result = tokio::task::spawn_blocking(move || {
            let conn = Self::open_connection(&db_path_owned, false)?;

            // Start transaction with IMMEDIATE to acquire write lock upfront
            conn.execute("BEGIN IMMEDIATE TRANSACTION", [])?;

            // Double-check if table was created by another thread while we were preparing
            let existing: Result<i64, rusqlite::Error> = conn.query_row(
                "SELECT table_id FROM pepper_table WHERE table_name = ?1",
                [&table_name],
                |row| row.get(0),
            );

            match existing {
                Ok(id) => {
                    // Another thread created it, return that ID
                    conn.execute("COMMIT", [])?;
                    Ok::<CreateTableResult, CatalogError>(CreateTableResult::AlreadyExists {
                        table_id: id,
                    })
                }
                Err(rusqlite::Error::QueryReturnedNoRows) => {
                    // Proceed with creation
                    // Get next catalog ID (for table_id)
                    let next_catalog_id: i64 = conn.query_row(
                        "SELECT value FROM pepper_metadata WHERE key = 'next_catalog_id'",
                        [],
                        |row| row.get(0),
                    )?;

                    let table_id = next_catalog_id;

                    // Generate table UUID
                    let table_uuid = uuid::Uuid::now_v7().to_string();

                    // Generate initial snapshot UUID
                    let initial_snapshot_id = uuid::Uuid::now_v7().to_string();

                    // Insert table metadata with initial snapshot
                    conn.execute(
                        r"
                        INSERT INTO pepper_table (
                            table_id, table_uuid,
                            table_name, path, path_is_relative, schema_json, primary_key_json,
                            current_snapshot_id, partition_column
                        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
                        ",
                        rusqlite::params![
                            table_id,
                            table_uuid,
                            table_name,
                            base_path,
                            false, // path_is_relative - using absolute paths for now
                            schema_json,
                            primary_key_json,
                            initial_snapshot_id, // All tables start with an initial snapshot
                            partition_column,
                        ],
                    )?;

                    // Update next_catalog_id in metadata
                    conn.execute(
                        "UPDATE pepper_metadata SET value = ?1 WHERE key = 'next_catalog_id'",
                        [next_catalog_id + 1],
                    )?;

                    // Commit transaction
                    conn.execute("COMMIT", [])?;

                    Ok::<CreateTableResult, CatalogError>(CreateTableResult::Created {
                        table_id,
                        snapshot_id: initial_snapshot_id,
                        base_path,
                    })
                }
                Err(e) => Err(CatalogError::from(e)),
            }
        })
        .await??;

        // Handle the result - only create snapshot directory if table was newly created
        match create_result {
            CreateTableResult::Created {
                table_id,
                snapshot_id,
                base_path,
            } => {
                // Create the initial snapshot directory
                // Directory structure: [base_path]/[table_id]/[snapshot_id]/
                let snapshot_dir = std::path::PathBuf::from(&base_path)
                    .join(table_id.to_string())
                    .join(&snapshot_id);

                tokio::fs::create_dir_all(&snapshot_dir)
                    .await
                    .map_err(|e| CatalogError::Io { source: e })?;

                Ok(table_id)
            }
            CreateTableResult::AlreadyExists { table_id } => {
                // Table already exists, no need to create snapshot directory
                Ok(table_id)
            }
        }
    }

    async fn get_table(&self, table_name: &str) -> CatalogResult<TableMetadata> {
        let db_path_owned = self.db_path().to_string();
        let table_name_owned = table_name.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = Self::open_connection(&db_path_owned, true)?;

            // Query for the table
            let mut stmt = conn.prepare(
                r"
                SELECT table_id, table_uuid,
                       table_name, path, path_is_relative, schema_json, primary_key_json,
                       current_snapshot_id, partition_column
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
                    let current_snapshot_id: String = row.get(7)?;
                    let partition_column: Option<String> = row.get(8)?;

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
                        current_snapshot_id,
                        partition_column,
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

    async fn get_current_snapshot(&self, table_id: i64) -> CatalogResult<String> {
        let db_path_owned = self.db_path().to_string();

        tokio::task::spawn_blocking(move || {
            let conn = Self::open_connection(&db_path_owned, true)?;

            let snapshot_id: String = conn.query_row(
                "SELECT current_snapshot_id FROM pepper_table WHERE table_id = ?1",
                [table_id],
                |row| row.get(0),
            )?;

            Ok::<String, CatalogError>(snapshot_id)
        })
        .await?
    }

    async fn set_current_snapshot(&self, table_id: i64, snapshot_id: &str) -> CatalogResult<()> {
        let db_path_owned = self.db_path().to_string();
        let snapshot_id_owned = snapshot_id.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = Self::open_connection(&db_path_owned, false)?;

            conn.execute(
                "UPDATE pepper_table SET current_snapshot_id = ?1 WHERE table_id = ?2",
                rusqlite::params![snapshot_id_owned, table_id],
            )?;

            Ok::<(), CatalogError>(())
        })
        .await?
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

    async fn add_partition(&self, partition: PartitionMetadata) -> CatalogResult<i64> {
        let db_path_owned = self.db_path().to_string();

        tokio::task::spawn_blocking(move || {
            let conn = Self::open_connection(&db_path_owned, false)?;

            // Start transaction with IMMEDIATE to acquire write lock upfront
            conn.execute("BEGIN IMMEDIATE TRANSACTION", [])?;

            // Check if partition already exists
            let existing_partition: Result<i64, rusqlite::Error> = conn.query_row(
                "SELECT partition_id FROM pepper_partition WHERE table_id = ?1 AND partition_value = ?2",
                rusqlite::params![partition.table_id, partition.partition_value],
                |row| row.get(0),
            );

            let partition_id = match existing_partition {
                Ok(id) => {
                    // Partition already exists, return its ID
                    conn.execute("COMMIT", [])?;
                    id
                }
                Err(rusqlite::Error::QueryReturnedNoRows) => {
                    // Partition doesn't exist, create it
                    // Get next partition ID
                    let next_partition_id: i64 = conn.query_row(
                        "SELECT value FROM pepper_metadata WHERE key = 'next_partition_id'",
                        [],
                        |row| row.get(0),
                    )?;

                    let partition_id = next_partition_id;

                    // Insert partition metadata
                    conn.execute(
                        r"
                        INSERT INTO pepper_partition (
                            partition_id, table_id, partition_column, partition_value, path, path_is_relative, record_count, file_size_bytes
                        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                        ",
                        rusqlite::params![
                            partition_id,
                            partition.table_id,
                            partition.partition_column,
                            partition.partition_value,
                            partition.path,
                            partition.path_is_relative,
                            partition.record_count,
                            partition.file_size_bytes,
                        ],
                    )?;

                    // Update next_partition_id in metadata
                    conn.execute(
                        "UPDATE pepper_metadata SET value = ?1 WHERE key = 'next_partition_id'",
                        [next_partition_id + 1],
                    )?;

                    // Commit transaction
                    conn.execute("COMMIT", [])?;

                    partition_id
                }
                Err(e) => {
                    // Other error, propagate it
                    return Err(CatalogError::from(e));
                }
            };

            Ok::<i64, CatalogError>(partition_id)
        })
        .await?
    }

    async fn get_partitions(&self, table_id: i64) -> CatalogResult<Vec<PartitionMetadata>> {
        let db_path_owned = self.db_path().to_string();

        tokio::task::spawn_blocking(move || {
            let conn = Self::open_connection(&db_path_owned, true)?;

            let mut stmt = conn.prepare(
                r"
                SELECT partition_id, table_id, partition_column, partition_value, path, path_is_relative, record_count, file_size_bytes
                FROM pepper_partition
                WHERE table_id = ?1
                ORDER BY partition_id
                ",
            )?;

            let partitions = stmt
                .query_map([table_id], |row| {
                    Ok(PartitionMetadata {
                        partition_id: row.get(0)?,
                        table_id: row.get(1)?,
                        partition_column: row.get(2)?,
                        partition_value: row.get(3)?,
                        path: row.get(4)?,
                        path_is_relative: row.get(5)?,
                        record_count: row.get(6)?,
                        file_size_bytes: row.get(7)?,
                    })
                })?
                .collect::<Result<Vec<_>, _>>()?;

            Ok::<Vec<PartitionMetadata>, CatalogError>(partitions)
        })
        .await?
    }

    async fn get_partition(
        &self,
        table_id: i64,
        partition_value: &str,
    ) -> CatalogResult<Option<PartitionMetadata>> {
        let db_path_owned = self.db_path().to_string();
        let partition_value_owned = partition_value.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = Self::open_connection(&db_path_owned, true)?;

            let mut stmt = conn.prepare(
                r"
                SELECT partition_id, table_id, partition_column, partition_value, path, path_is_relative, record_count, file_size_bytes
                FROM pepper_partition
                WHERE table_id = ?1 AND partition_value = ?2
                LIMIT 1
                ",
            )?;

            match stmt.query_row(rusqlite::params![table_id, partition_value_owned], |row| {
                Ok(PartitionMetadata {
                    partition_id: row.get(0)?,
                    table_id: row.get(1)?,
                    partition_column: row.get(2)?,
                    partition_value: row.get(3)?,
                    path: row.get(4)?,
                    path_is_relative: row.get(5)?,
                    record_count: row.get(6)?,
                    file_size_bytes: row.get(7)?,
                })
            }) {
                Ok(partition) => Ok(Some(partition)),
                Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
                Err(e) => Err(CatalogError::from(e)),
            }
        })
        .await?
    }

    async fn update_partition_stats(
        &self,
        partition_id: i64,
        record_count: i64,
        file_size_bytes: i64,
    ) -> CatalogResult<()> {
        let db_path_owned = self.db_path().to_string();

        tokio::task::spawn_blocking(move || {
            let conn = Self::open_connection(&db_path_owned, false)?;

            conn.execute(
                r"
                UPDATE pepper_partition 
                SET record_count = ?1, file_size_bytes = ?2
                WHERE partition_id = ?3
                ",
                rusqlite::params![record_count, file_size_bytes, partition_id],
            )?;

            Ok::<(), CatalogError>(())
        })
        .await?
    }

    async fn get_partition_stats(&self, partition_id: i64) -> CatalogResult<PartitionStats> {
        let db_path_owned = self.db_path().to_string();

        tokio::task::spawn_blocking(move || {
            let conn = Self::open_connection(&db_path_owned, true)?;

            let (record_count, file_size_bytes): (i64, i64) = conn.query_row(
                r"
                SELECT record_count, file_size_bytes
                FROM pepper_partition
                WHERE partition_id = ?1
                ",
                [partition_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;

            Ok::<PartitionStats, CatalogError>(PartitionStats {
                record_count,
                file_size_bytes,
            })
        })
        .await?
    }

    async fn get_partition_data_files(&self, partition_id: i64) -> CatalogResult<Vec<DataFile>> {
        let db_path_owned = self.db_path().to_string();

        tokio::task::spawn_blocking(move || {
            let conn = Self::open_connection(&db_path_owned, true)?;

            let mut stmt = conn.prepare(
                r"
                SELECT data_file_id, table_id, partition_id, file_order, path, path_is_relative,
                       file_format, record_count, file_size_bytes, row_id_start
                FROM pepper_data_file
                WHERE partition_id = ?1
                ORDER BY file_order
                ",
            )?;

            let files = stmt
                .query_map([partition_id], |row| {
                    Ok(DataFile {
                        data_file_id: row.get(0)?,
                        table_id: row.get(1)?,
                        partition_id: row.get(2)?,
                        file_order: row.get(3)?,
                        path: row.get(4)?,
                        path_is_relative: row.get(5)?,
                        file_format: row.get(6)?,
                        record_count: row.get(7)?,
                        file_size_bytes: row.get(8)?,
                        row_id_start: row.get(9)?,
                    })
                })?
                .collect::<Result<Vec<_>, _>>()?;

            Ok::<Vec<DataFile>, CatalogError>(files)
        })
        .await?
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

    async fn shutdown(&self) -> CatalogResult<()> {
        let db_path_owned = self.db_path().to_string();

        tokio::task::spawn_blocking(move || {
            let conn = rusqlite::Connection::open(&db_path_owned)?;

            // Check if WAL mode is enabled
            let journal_mode: String =
                conn.query_row("PRAGMA journal_mode", [], |row| row.get(0))?;

            if journal_mode.eq_ignore_ascii_case("wal") {
                tracing::info!("Truncating Pepper catalog WAL log");
                // Truncate the WAL log to persist changes and reduce file size
                conn.execute("PRAGMA wal_checkpoint(TRUNCATE)", [])?;
            }

            // Run optimize to improve query performance for future connections
            tracing::info!("Running optimize on Pepper catalog");
            conn.execute("PRAGMA optimize", [])?;

            Ok::<(), CatalogError>(())
        })
        .await
        .map_err(|e| CatalogError::InvalidOperation {
            message: format!("Failed to join shutdown task: {e}"),
        })??;

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
