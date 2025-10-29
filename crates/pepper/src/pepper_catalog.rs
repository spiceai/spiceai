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

//! Metadata catalog implementation for Pepper.

use super::catalog::{CatalogError, CatalogResult, MetadataCatalog};
use super::metadata::{
    CreateTableOptions, DataFile, DeleteFile, PartitionMetadata, PartitionStats, TableMetadata,
    TableStats,
};
use super::metastore::sqlite::SqliteMetastore;
#[cfg(feature = "turso")]
use super::metastore::turso::TursoMetastore;
use super::metastore::{
    ExecuteParams, MetastoreBackend, MetastoreRow, MetastoreValue, QueryParams, QueryRowParams,
};
use async_trait::async_trait;
use std::path::Path;
use std::sync::Arc;

/// Metastore backend enum to support different implementations.
enum MetastoreImpl {
    Sqlite(SqliteMetastore),
    #[cfg(feature = "turso")]
    Turso(TursoMetastore),
}

/// Metadata catalog for Pepper with pluggable metastore backends.
///
/// The catalog manages metadata for tables and their "virtual files". In Pepper,
/// a "file" is not a single physical file, but rather a Vortex `ListingTable` at a
/// unique directory. The metastore database tracks:
/// - Tables and their schemas
/// - `DataFile` entries (metadata for each `ListingTable`/virtual file)
/// - `DeleteFile` entries (deletion vectors for each virtual file)
///
/// Operations on files (read, append, delete, stats) are delegated to the
/// corresponding Vortex `ListingTable` provider.
///
/// ## Concurrency Model
///
/// The catalog uses a metastore backend (SQLite or Turso) with WAL mode which allows:
/// - Multiple concurrent readers
/// - One writer at a time (serialized by the backend)
///
/// The backend handles locking and concurrency automatically.
pub struct PepperCatalog {
    connection_string: String,
    metastore: MetastoreImpl,
}

impl PepperCatalog {
    /// Create a new Pepper catalog with the appropriate metastore backend.
    ///
    /// The connection string determines which backend to use:
    /// - `sqlite://path` - SQLite backend
    /// - `libsql://path` - Turso backend (requires `turso` feature)
    pub fn new(connection_string: impl Into<String>) -> Self {
        let connection_string = connection_string.into();
        let metastore = if connection_string.starts_with("libsql://") {
            #[cfg(feature = "turso")]
            {
                MetastoreImpl::Turso(TursoMetastore::new(&connection_string))
            }
            #[cfg(not(feature = "turso"))]
            {
                panic!("Turso backend requested but 'turso' feature is not enabled. Enable with --features turso");
            }
        } else {
            MetastoreImpl::Sqlite(SqliteMetastore::new(&connection_string))
        };

        Self {
            connection_string,
            metastore,
        }
    }

    /// Get the database file path from the connection string.
    fn db_path(&self) -> &str {
        self.connection_string
            .strip_prefix("sqlite://")
            .or_else(|| self.connection_string.strip_prefix("libsql://"))
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

    /// Perform catalog shutdown maintenance tasks.
    ///
    /// Runs a WAL checkpoint and `PRAGMA optimize` to ensure the catalog is in
    /// a clean state before shutdown, preventing large WAL files from lingering
    /// between runs.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError`] if the catalog cannot be opened or if the
    /// maintenance pragma statements fail to execute.
    pub async fn shutdown(&self) -> CatalogResult<()> {
        // Only SQLite supports WAL checkpoint and optimize pragmas
        // Turso handles optimization automatically
        match &self.metastore {
            MetastoreImpl::Sqlite(_) => {
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
                    message: format!("Catalog shutdown task panicked: {e}"),
                })??;
            }
            #[cfg(feature = "turso")]
            MetastoreImpl::Turso(_) => {
                // Turso handles optimization automatically, no action needed
                tracing::debug!("Turso backend handles optimization automatically");
            }
        }

        Ok(())
    }
}

impl PepperCatalog {
    /// Helper to query a single row from metastore, working with both SQLite and Turso
    async fn query_row_helper<F, T>(&self, params: QueryRowParams<'_>, f: F) -> CatalogResult<T>
    where
        F: FnOnce(&dyn MetastoreRow) -> CatalogResult<T> + Send + 'static + Copy,
        T: Send + 'static,
    {
        match &self.metastore {
            MetastoreImpl::Sqlite(m) => m.query_row(params, f).await,
            #[cfg(feature = "turso")]
            MetastoreImpl::Turso(m) => m.query_row(params, f).await,
        }
    }

    /// Helper to execute a statement on metastore, working with both SQLite and Turso
    async fn execute_helper(&self, params: ExecuteParams<'_>) -> CatalogResult<()> {
        match &self.metastore {
            MetastoreImpl::Sqlite(m) => m.execute(params).await,
            #[cfg(feature = "turso")]
            MetastoreImpl::Turso(m) => m.execute(params).await,
        }
    }

    /// Helper to query multiple rows from metastore, working with both SQLite and Turso
    async fn query_helper<F, T>(&self, params: QueryParams<'_>, f: F) -> CatalogResult<Vec<T>>
    where
        F: Fn(&dyn MetastoreRow) -> CatalogResult<T> + Send + 'static,
        T: Send + 'static,
    {
        match &self.metastore {
            MetastoreImpl::Sqlite(m) => m.query(params, f).await,
            #[cfg(feature = "turso")]
            MetastoreImpl::Turso(m) => m.query(params, f).await,
        }
    }
}

#[async_trait]
impl MetadataCatalog for PepperCatalog {
    async fn init(&self) -> CatalogResult<()> {
        // Create database directory if it doesn't exist
        let db_path = self.db_path();
        let db_dir = Path::new(db_path)
            .parent()
            .ok_or_else(|| CatalogError::InvalidOperation {
                message: "Invalid database path".to_string(),
            })?;

        if !db_dir.exists() {
            tokio::fs::create_dir_all(db_dir).await?;
        }

        // Initialize schema using the appropriate metastore backend
        match &self.metastore {
            MetastoreImpl::Sqlite(metastore) => metastore.init_schema().await?,
            #[cfg(feature = "turso")]
            MetastoreImpl::Turso(metastore) => metastore.init_schema().await?,
        }

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

        // Check if table already exists first (read-only check)
        let existing_table_id: Option<i64> = self
            .query_row_helper(
                QueryRowParams {
                    sql: "SELECT table_id FROM pepper_table WHERE table_name = ?1",
                    params: vec![MetastoreValue::Text(table_name.clone())],
                },
                |row| row.get_i64(0),
            )
            .await
            .ok();

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

        // Double-check if table was created by another thread while we were preparing
        let double_check = self
            .query_row_helper(
                QueryRowParams {
                    sql: "SELECT table_id FROM pepper_table WHERE table_name = ?1",
                    params: vec![MetastoreValue::Text(table_name.clone())],
                },
                |row| row.get_i64(0),
            )
            .await;

        let create_result: CreateTableResult = match double_check {
            Ok(id) => CreateTableResult::AlreadyExists { table_id: id },
            Err(_) => {
                // Get next catalog ID (for table_id)
                let next_catalog_id: i64 = self
                    .query_row_helper(
                        QueryRowParams {
                            sql: "SELECT value FROM pepper_metadata WHERE key = 'next_catalog_id'",
                            params: vec![],
                        },
                        |row| row.get_i64(0),
                    )
                    .await?;

                let table_id = next_catalog_id;

                // Generate table UUID
                let table_uuid = uuid::Uuid::now_v7().to_string();

                // Generate initial snapshot UUID
                let initial_snapshot_id = uuid::Uuid::now_v7().to_string();

                // Insert table metadata with initial snapshot
                self.execute_helper(ExecuteParams {
                    sql: r"
                        INSERT INTO pepper_table (
                            table_id, table_uuid,
                            table_name, path, path_is_relative, schema_json, primary_key_json,
                            current_snapshot_id, partition_column
                        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
                    ",
                    params: vec![
                        MetastoreValue::Integer(table_id),
                        MetastoreValue::Text(table_uuid),
                        MetastoreValue::Text(table_name.clone()),
                        MetastoreValue::Text(base_path.clone()),
                        MetastoreValue::Bool(false), // path_is_relative
                        MetastoreValue::Text(schema_json),
                        primary_key_json.map_or(MetastoreValue::Null, MetastoreValue::Text),
                        MetastoreValue::Text(initial_snapshot_id.clone()),
                        partition_column.map_or(MetastoreValue::Null, MetastoreValue::Text),
                    ],
                })
                .await?;

                // Update next_catalog_id in metadata
                self.execute_helper(ExecuteParams {
                    sql: "UPDATE pepper_metadata SET value = ?1 WHERE key = 'next_catalog_id'",
                    params: vec![MetastoreValue::Integer(next_catalog_id + 1)],
                })
                .await?;

                CreateTableResult::Created {
                    table_id,
                    snapshot_id: initial_snapshot_id,
                    base_path: base_path.clone(),
                }
            }
        };

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
                    .map_err(|e| CatalogError::Io { source: e})?;

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

    async fn add_delete_file(&self, delete_file: DeleteFile) -> CatalogResult<i64> {
        let db_path_owned = self.db_path().to_string();

        let result = tokio::task::spawn_blocking(move || {
            let conn = Self::open_connection(&db_path_owned, false)?;

            // Begin transaction
            conn.execute("BEGIN TRANSACTION", [])?;

            // Get next delete_file_id
            let next_delete_file_id: i64 = conn.query_row(
                "SELECT COALESCE(MAX(delete_file_id), 0) + 1 FROM pepper_delete_file",
                [],
                |row| row.get(0),
            )?;

            let delete_file_id = next_delete_file_id;

            // Insert delete file record
            conn.execute(
                r"
                INSERT INTO pepper_delete_file (
                    delete_file_id, table_id, data_file_id, path, path_is_relative,
                    format, delete_count, file_size_bytes
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                ",
                rusqlite::params![
                    delete_file_id,
                    delete_file.table_id,
                    delete_file.data_file_id,
                    delete_file.path,
                    delete_file.path_is_relative,
                    delete_file.format,
                    delete_file.delete_count,
                    delete_file.file_size_bytes,
                ],
            )?;

            // Commit transaction
            conn.execute("COMMIT", [])?;

            Ok::<i64, CatalogError>(delete_file_id)
        })
        .await;

        let delete_file_id = Self::handle_blocking_result(result, "Delete file registration")?;

        Ok(delete_file_id)
    }

    async fn get_delete_files(&self, _data_file_id: i64) -> CatalogResult<Vec<DeleteFile>> {
        // Implementation would query delete files for specific data file
        Ok(vec![])
    }

    async fn get_table_delete_files(&self, table_id: i64) -> CatalogResult<Vec<DeleteFile>> {
        let db_path_owned = self.db_path().to_string();

        let blocking_result = tokio::task::spawn_blocking(move || {
            let conn = Self::open_connection(&db_path_owned, true)?;

            let mut stmt = conn.prepare(
                "SELECT delete_file_id, table_id, data_file_id, path, path_is_relative, 
                        format, delete_count, file_size_bytes 
                 FROM pepper_delete_file 
                 WHERE table_id = ?1",
            )?;

            let delete_files = stmt
                .query_map([table_id], |row| {
                    Ok(DeleteFile {
                        delete_file_id: row.get(0)?,
                        table_id: row.get(1)?,
                        data_file_id: row.get(2)?,
                        path: row.get(3)?,
                        path_is_relative: row.get(4)?,
                        format: row.get(5)?,
                        delete_count: row.get(6)?,
                        file_size_bytes: row.get(7)?,
                    })
                })?
                .collect::<Result<Vec<_>, _>>()?;

            Ok::<_, CatalogError>(delete_files)
        })
        .await?;

        blocking_result
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

        let result = tokio::task::spawn_blocking(move || {
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
        .await;

        Self::handle_blocking_result(result, "Catalog shutdown")?;

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
