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

//! Metadata catalog implementation for Cayenne.

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

/// Metadata catalog for Cayenne with pluggable metastore backends.
///
/// The catalog manages metadata for tables and their "virtual files". In Cayenne,
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
/// The catalog uses a metastore backend (`SQLite` or Turso) with WAL mode which allows:
/// - Multiple concurrent readers
/// - One writer at a time (serialized by the backend)
///
/// The backend handles locking and concurrency automatically.
pub struct CayenneCatalog {
    connection_string: String,
    metastore: MetastoreImpl,
}

impl CayenneCatalog {
    /// Create a new Cayenne catalog with the appropriate metastore backend.
    ///
    /// The connection string determines which backend to use:
    /// - `sqlite://path` - `SQLite` backend
    /// - `libsql://path` - Turso backend (requires `turso` feature)
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError::InvalidOperation`] if the `libsql://` scheme is used
    /// but the `turso` feature is not enabled.
    pub fn new(connection_string: impl Into<String>) -> CatalogResult<Self> {
        let connection_string = connection_string.into();
        let metastore = if connection_string.starts_with("libsql://") {
            #[cfg(feature = "turso")]
            {
                MetastoreImpl::Turso(TursoMetastore::new(&connection_string))
            }
            #[cfg(not(feature = "turso"))]
            {
                return Err(CatalogError::InvalidOperation {
                    message: "Turso backend requested but 'turso' feature is not enabled. Enable with --features turso".to_string(),
                });
            }
        } else {
            MetastoreImpl::Sqlite(SqliteMetastore::new(&connection_string))
        };

        Ok(Self {
            connection_string,
            metastore,
        })
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
                        tracing::info!("Truncating Cayenne catalog WAL log");
                        // Truncate the WAL log to persist changes and reduce file size
                        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)", [])?;
                    }

                    // Run optimize to improve query performance for future connections
                    tracing::info!("Running optimize on Cayenne catalog");
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

impl CayenneCatalog {
    /// Helper to query a single row from metastore, working with both `SQLite` and Turso
    async fn query_row_helper<F, T>(&self, params: QueryRowParams<'_>, f: F) -> CatalogResult<T>
    where
        F: FnOnce(&dyn MetastoreRow) -> CatalogResult<T> + Send + 'static,
        T: Send + 'static,
    {
        match &self.metastore {
            MetastoreImpl::Sqlite(m) => m.query_row(params, f).await,
            #[cfg(feature = "turso")]
            MetastoreImpl::Turso(m) => m.query_row(params, f).await,
        }
    }

    /// Helper to execute a statement on metastore, working with both `SQLite` and Turso
    async fn execute_helper(&self, params: ExecuteParams<'_>) -> CatalogResult<()> {
        match &self.metastore {
            MetastoreImpl::Sqlite(m) => m.execute(params).await,
            #[cfg(feature = "turso")]
            MetastoreImpl::Turso(m) => m.execute(params).await,
        }
    }

    /// Helper to query multiple rows from metastore, working with both `SQLite` and Turso
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
impl MetadataCatalog for CayenneCatalog {
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
                    sql: "SELECT table_id FROM cayenne_table WHERE table_name = ?1",
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
                    sql: "SELECT table_id FROM cayenne_table WHERE table_name = ?1",
                    params: vec![MetastoreValue::Text(table_name.clone())],
                },
                |row| row.get_i64(0),
            )
            .await;

        let create_result: CreateTableResult = if let Ok(id) = double_check {
            CreateTableResult::AlreadyExists { table_id: id }
        } else {
            // Get next catalog ID (for table_id)
            let next_catalog_id: i64 = self
                .query_row_helper(
                    QueryRowParams {
                        sql: "SELECT value FROM cayenne_metadata WHERE key = 'next_catalog_id'",
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

            // Serialize Vortex config to JSON
            let vortex_config_json =
                serde_json::to_string(&options.vortex_config).map_err(|e| {
                    CatalogError::InvalidOperation {
                        message: format!("Failed to serialize vortex config: {e}"),
                    }
                })?;

            // Insert table metadata with initial snapshot
            // Handle race condition where another thread creates the table concurrently
            let insert_result = self
                .execute_helper(ExecuteParams {
                    sql: r"
                    INSERT INTO cayenne_table (
                        table_id, table_uuid,
                        table_name, path, path_is_relative, schema_json, primary_key_json,
                        current_snapshot_id, partition_column, vortex_config_json
                    ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
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
                        MetastoreValue::Text(vortex_config_json),
                    ],
                })
                .await;

            // Check if insert failed due to constraint violation (race condition)
            if let Err(CatalogError::Sqlite {
                source: rusqlite::Error::SqliteFailure(err, _),
            }) = &insert_result
            {
                if err.code == rusqlite::ErrorCode::ConstraintViolation {
                    // Race condition - another thread created the table with same table_id
                    // Fetch the existing table_id by table_name
                    return self
                        .query_row_helper(
                            QueryRowParams {
                                sql: "SELECT table_id FROM cayenne_table WHERE table_name = ?1",
                                params: vec![MetastoreValue::Text(table_name.clone())],
                            },
                            |row| row.get_i64(0),
                        )
                        .await;
                }
            }

            // Propagate any other errors
            insert_result?;

            // Update next_catalog_id in metadata
            self.execute_helper(ExecuteParams {
                sql: "UPDATE cayenne_metadata SET value = ?1 WHERE key = 'next_catalog_id'",
                params: vec![MetastoreValue::Integer(next_catalog_id + 1)],
            })
            .await?;

            CreateTableResult::Created {
                table_id,
                snapshot_id: initial_snapshot_id,
                base_path: base_path.clone(),
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
        let table_name_owned = table_name.to_string();

        self.query_row_helper(
            QueryRowParams {
                sql: r"
                    SELECT table_id, table_uuid,
                           table_name, path, path_is_relative, schema_json, primary_key_json,
                           current_snapshot_id, partition_column, vortex_config_json
                    FROM cayenne_table
                    WHERE table_name = ?1
                    LIMIT 1
                ",
                params: vec![MetastoreValue::Text(table_name_owned.clone())],
            },
            |row| {
                let table_id = row.get_i64(0)?;
                let table_uuid = row.get_string(1)?;
                let table_name = row.get_string(2)?;
                let path = row.get_string(3)?;
                let path_is_relative = row.get_bool(4)?;
                let schema_json = row.get_string(5)?;
                let primary_key_json = row.get_optional_string(6)?;
                let current_snapshot_id = row.get_string(7)?;
                let partition_column = row.get_optional_string(8)?;
                let vortex_config_json = row.get_optional_string(9)?;

                // Deserialize schema using Arrow IPC format
                let schema = {
                    use base64::Engine;
                    use bytes::Bytes;

                    let schema_bytes = base64::engine::general_purpose::STANDARD
                        .decode(&schema_json)
                        .map_err(|_| CatalogError::InvalidOperation {
                            message: "Failed to decode schema from base64".to_string(),
                        })?;

                    let ipc_message = arrow_flight::IpcMessage(Bytes::from(schema_bytes));
                    arrow_schema::Schema::try_from(ipc_message).map_err(|_| {
                        CatalogError::InvalidOperation {
                            message: "Failed to deserialize schema from IPC".to_string(),
                        }
                    })?
                };

                let schema = Arc::new(schema);

                // Parse primary key
                let primary_key = if let Some(pk_json) = primary_key_json {
                    serde_json::from_str(&pk_json).map_err(|e| CatalogError::InvalidOperation {
                        message: format!("Failed to deserialize primary key: {e}"),
                    })?
                } else {
                    vec![]
                };

                // Parse vortex config
                let vortex_config = if let Some(config_json) = vortex_config_json {
                    serde_json::from_str(&config_json).map_err(|e| {
                        CatalogError::InvalidOperation {
                            message: format!("Failed to deserialize vortex config: {e}"),
                        }
                    })?
                } else {
                    super::metadata::VortexConfig::default()
                };

                Ok(TableMetadata {
                    table_id,
                    table_uuid,
                    table_name,
                    path,
                    path_is_relative,
                    schema,
                    primary_key,
                    current_snapshot_id,
                    partition_column,
                    vortex_config,
                })
            },
        )
        .await
        .map_err(|_| CatalogError::TableNotFound {
            table_name: table_name_owned,
        })
    }

    async fn get_table_by_id(&self, table_id: i64) -> CatalogResult<TableMetadata> {
        // Implementation would query cayenne_table by ID
        Err(CatalogError::TableNotFound {
            table_name: format!("id:{table_id}"),
        })
    }

    async fn get_current_snapshot(&self, table_id: i64) -> CatalogResult<String> {
        self.query_row_helper(
            QueryRowParams {
                sql: "SELECT current_snapshot_id FROM cayenne_table WHERE table_id = ?1",
                params: vec![MetastoreValue::Integer(table_id)],
            },
            |row| row.get_string(0),
        )
        .await
    }

    async fn set_current_snapshot(&self, table_id: i64, snapshot_id: &str) -> CatalogResult<()> {
        self.execute_helper(ExecuteParams {
            sql: "UPDATE cayenne_table SET current_snapshot_id = ?1 WHERE table_id = ?2",
            params: vec![
                MetastoreValue::Text(snapshot_id.to_string()),
                MetastoreValue::Integer(table_id),
            ],
        })
        .await
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
        // Implementation would insert into cayenne_data_file
        Err(CatalogError::InvalidOperation {
            message: "Not yet implemented".to_string(),
        })
    }

    async fn get_data_files(&self, _table_id: i64) -> CatalogResult<Vec<DataFile>> {
        // Implementation would query active data files for table
        Ok(vec![])
    }

    async fn add_delete_file(&self, delete_file: DeleteFile) -> CatalogResult<i64> {
        // Retry loop to handle concurrent inserts - max 10 attempts
        for attempt in 0..10 {
            // Get next delete_file_id
            let next_delete_file_id: i64 = self
                .query_row_helper(
                    QueryRowParams {
                        sql: "SELECT COALESCE(MAX(delete_file_id), 0) + 1 FROM cayenne_delete_file",
                        params: vec![],
                    },
                    |row| row.get_i64(0),
                )
                .await?;

            let delete_file_id = next_delete_file_id;

            // Insert delete file record
            // Handle race condition where another thread creates a delete file concurrently
            let insert_result = self
                .execute_helper(ExecuteParams {
                    sql: r"
                INSERT INTO cayenne_delete_file (
                    delete_file_id, table_id, data_file_id, path, path_is_relative,
                    format, delete_count, file_size_bytes
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
            ",
                    params: vec![
                        MetastoreValue::Integer(delete_file_id),
                        MetastoreValue::Integer(delete_file.table_id),
                        MetastoreValue::Integer(delete_file.data_file_id),
                        MetastoreValue::Text(delete_file.path.clone()),
                        MetastoreValue::Bool(delete_file.path_is_relative),
                        MetastoreValue::Text(delete_file.format.clone()),
                        MetastoreValue::Integer(delete_file.delete_count),
                        MetastoreValue::Integer(delete_file.file_size_bytes),
                    ],
                })
                .await;

            // Check if insert succeeded or failed due to constraint violation
            match insert_result {
                Ok(()) => return Ok(delete_file_id),
                Err(CatalogError::Sqlite {
                    source: rusqlite::Error::SqliteFailure(err, _),
                }) if err.code == rusqlite::ErrorCode::ConstraintViolation => {
                    // Race condition - another thread used the same delete_file_id
                    // Retry on next iteration (unless this was the last attempt)
                    if attempt == 9 {
                        // Last attempt failed, return the error
                        return Err(CatalogError::Sqlite {
                            source: rusqlite::Error::SqliteFailure(err, None),
                        });
                    }
                    // Small delay before retry to reduce contention
                    tokio::time::sleep(tokio::time::Duration::from_micros(100)).await;
                    // Otherwise, loop continues to next iteration
                }
                Err(e) => return Err(e),
            }
        }

        // This should never be reached due to the loop logic
        unreachable!("Retry loop should either return or error");
    }

    async fn get_delete_files(&self, _data_file_id: i64) -> CatalogResult<Vec<DeleteFile>> {
        // Implementation would query delete files for specific data file
        Ok(vec![])
    }

    async fn get_table_delete_files(&self, table_id: i64) -> CatalogResult<Vec<DeleteFile>> {
        self.query_helper(
            QueryParams {
                sql: "SELECT delete_file_id, table_id, data_file_id, path, path_is_relative, 
                        format, delete_count, file_size_bytes 
                 FROM cayenne_delete_file 
                 WHERE table_id = ?1",
                params: vec![MetastoreValue::Integer(table_id)],
            },
            |row| {
                Ok(DeleteFile {
                    delete_file_id: row.get_i64(0)?,
                    table_id: row.get_i64(1)?,
                    data_file_id: row.get_i64(2)?,
                    path: row.get_string(3)?,
                    path_is_relative: row.get_bool(4)?,
                    format: row.get_string(5)?,
                    delete_count: row.get_i64(6)?,
                    file_size_bytes: row.get_i64(7)?,
                })
            },
        )
        .await
    }

    async fn get_table_stats(&self, _table_id: i64) -> CatalogResult<TableStats> {
        // Implementation would aggregate stats from data and delete files
        Ok(TableStats::default())
    }

    async fn add_partition(&self, partition: PartitionMetadata) -> CatalogResult<i64> {
        // Check if partition already exists
        let existing_partition = self
            .query_row_helper(
                QueryRowParams {
                    sql: "SELECT partition_id FROM cayenne_partition WHERE table_id = ?1 AND partition_value = ?2",
                    params: vec![
                        MetastoreValue::Integer(partition.table_id),
                        MetastoreValue::Text(partition.partition_value.clone()),
                    ],
                },
                |row| row.get_i64(0),
            )
            .await;

        if let Ok(id) = existing_partition {
            // Partition already exists, return its ID
            return Ok(id);
        }

        // Partition doesn't exist, create it
        // Get next partition ID
        let next_partition_id: i64 = self
            .query_row_helper(
                QueryRowParams {
                    sql: "SELECT value FROM cayenne_metadata WHERE key = 'next_partition_id'",
                    params: vec![],
                },
                |row| row.get_i64(0),
            )
            .await?;

        let partition_id = next_partition_id;

        // Insert partition metadata
        // Handle race condition where another thread creates the partition concurrently
        let insert_result = self
            .execute_helper(ExecuteParams {
                sql: r"
                INSERT INTO cayenne_partition (
                    partition_id, table_id, partition_column, partition_value, path, path_is_relative, record_count, file_size_bytes
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
            ",
                params: vec![
                    MetastoreValue::Integer(partition_id),
                    MetastoreValue::Integer(partition.table_id),
                    MetastoreValue::Text(partition.partition_column.clone()),
                    MetastoreValue::Text(partition.partition_value.clone()),
                    MetastoreValue::Text(partition.path.clone()),
                    MetastoreValue::Bool(partition.path_is_relative),
                    MetastoreValue::Integer(partition.record_count),
                    MetastoreValue::Integer(partition.file_size_bytes),
                ],
            })
            .await;

        // Check if insert failed due to constraint violation (race condition)
        if let Err(CatalogError::Sqlite {
            source: rusqlite::Error::SqliteFailure(err, _),
        }) = &insert_result
        {
            if err.code == rusqlite::ErrorCode::ConstraintViolation {
                // Race condition - another thread created the partition
                // Fetch the existing partition_id by table_id and partition_value
                return self
                    .query_row_helper(
                        QueryRowParams {
                            sql: "SELECT partition_id FROM cayenne_partition WHERE table_id = ?1 AND partition_value = ?2",
                            params: vec![
                                MetastoreValue::Integer(partition.table_id),
                                MetastoreValue::Text(partition.partition_value),
                            ],
                        },
                        |row| row.get_i64(0),
                    )
                    .await;
            }
        }

        // Propagate any other errors
        insert_result?;

        // Update next_partition_id in metadata
        self.execute_helper(ExecuteParams {
            sql: "UPDATE cayenne_metadata SET value = ?1 WHERE key = 'next_partition_id'",
            params: vec![MetastoreValue::Integer(next_partition_id + 1)],
        })
        .await?;

        Ok(partition_id)
    }

    async fn get_partitions(&self, table_id: i64) -> CatalogResult<Vec<PartitionMetadata>> {
        self.query_helper(
            QueryParams {
                sql: r"
                    SELECT partition_id, table_id, partition_column, partition_value, path, path_is_relative, record_count, file_size_bytes
                    FROM cayenne_partition
                    WHERE table_id = ?1
                    ORDER BY partition_id
                ",
                params: vec![MetastoreValue::Integer(table_id)],
            },
            |row| {
                Ok(PartitionMetadata {
                    partition_id: row.get_i64(0)?,
                    table_id: row.get_i64(1)?,
                    partition_column: row.get_string(2)?,
                    partition_value: row.get_string(3)?,
                    path: row.get_string(4)?,
                    path_is_relative: row.get_bool(5)?,
                    record_count: row.get_i64(6)?,
                    file_size_bytes: row.get_i64(7)?,
                })
            },
        )
        .await
    }

    async fn get_partition(
        &self,
        table_id: i64,
        partition_value: &str,
    ) -> CatalogResult<Option<PartitionMetadata>> {
        let result = self
            .query_row_helper(
                QueryRowParams {
                    sql: r"
                        SELECT partition_id, table_id, partition_column, partition_value, path, path_is_relative, record_count, file_size_bytes
                        FROM cayenne_partition
                        WHERE table_id = ?1 AND partition_value = ?2
                        LIMIT 1
                    ",
                    params: vec![
                        MetastoreValue::Integer(table_id),
                        MetastoreValue::Text(partition_value.to_string()),
                    ],
                },
                |row| {
                    Ok(PartitionMetadata {
                        partition_id: row.get_i64(0)?,
                        table_id: row.get_i64(1)?,
                        partition_column: row.get_string(2)?,
                        partition_value: row.get_string(3)?,
                        path: row.get_string(4)?,
                        path_is_relative: row.get_bool(5)?,
                        record_count: row.get_i64(6)?,
                        file_size_bytes: row.get_i64(7)?,
                    })
                },
            )
            .await;

        match result {
            Ok(partition) => Ok(Some(partition)),
            Err(_) => Ok(None),
        }
    }

    async fn update_partition_stats(
        &self,
        partition_id: i64,
        record_count: i64,
        file_size_bytes: i64,
    ) -> CatalogResult<()> {
        self.execute_helper(ExecuteParams {
            sql: r"
                UPDATE cayenne_partition 
                SET record_count = ?1, file_size_bytes = ?2
                WHERE partition_id = ?3
            ",
            params: vec![
                MetastoreValue::Integer(record_count),
                MetastoreValue::Integer(file_size_bytes),
                MetastoreValue::Integer(partition_id),
            ],
        })
        .await
    }

    async fn get_partition_stats(&self, partition_id: i64) -> CatalogResult<PartitionStats> {
        self.query_row_helper(
            QueryRowParams {
                sql: r"
                    SELECT record_count, file_size_bytes
                    FROM cayenne_partition
                    WHERE partition_id = ?1
                ",
                params: vec![MetastoreValue::Integer(partition_id)],
            },
            |row| {
                Ok(PartitionStats {
                    record_count: row.get_i64(0)?,
                    file_size_bytes: row.get_i64(1)?,
                })
            },
        )
        .await
    }

    async fn get_partition_data_files(&self, partition_id: i64) -> CatalogResult<Vec<DataFile>> {
        self.query_helper(
            QueryParams {
                sql: r"
                    SELECT data_file_id, table_id, partition_id, file_order, path, path_is_relative,
                           file_format, record_count, file_size_bytes, row_id_start
                    FROM cayenne_data_file
                    WHERE partition_id = ?1
                    ORDER BY file_order
                ",
                params: vec![MetastoreValue::Integer(partition_id)],
            },
            |row| {
                Ok(DataFile {
                    data_file_id: row.get_i64(0)?,
                    table_id: row.get_i64(1)?,
                    partition_id: row.get_optional_i64(2)?,
                    file_order: row.get_i64(3)?,
                    path: row.get_string(4)?,
                    path_is_relative: row.get_bool(5)?,
                    file_format: row.get_string(6)?,
                    record_count: row.get_i64(7)?,
                    file_size_bytes: row.get_i64(8)?,
                    row_id_start: row.get_i64(9)?,
                })
            },
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_catalog_creation() {
        let _catalog = CayenneCatalog::new("sqlite://./test.db").expect("Failed to create catalog");
        // Tests will be added once implementation is complete
    }

    #[tokio::test]
    async fn test_concurrent_table_creation() {
        // Create a unique test database to avoid conflicts with other tests
        let test_db = format!("sqlite://./.test_concurrent_{}.db", uuid::Uuid::now_v7());
        let catalog = Arc::new(CayenneCatalog::new(&test_db).expect("Failed to create catalog"));

        // Initialize the catalog
        catalog.init().await.expect("Failed to initialize catalog");

        // Create test schema
        let schema = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("id", arrow_schema::DataType::Int64, false),
            arrow_schema::Field::new("name", arrow_schema::DataType::Utf8, true),
        ]));

        let table_name = "test_concurrent_table";
        let base_path = "/tmp/cayenne_test";

        // Spawn multiple tasks that all try to create the same table concurrently
        let mut handles = vec![];
        for _ in 0..10 {
            let catalog_clone = Arc::clone(&catalog);
            let schema_clone = Arc::clone(&schema);
            let table_name = table_name.to_string();
            let base_path = base_path.to_string();

            let handle = tokio::spawn(async move {
                let options = CreateTableOptions {
                    table_name: table_name.clone(),
                    schema: schema_clone,
                    primary_key: vec![],
                    base_path,
                    partition_column: None,
                    vortex_config: crate::metadata::VortexConfig::default(),
                };

                catalog_clone.create_table(options).await
            });

            handles.push(handle);
        }

        // Wait for all tasks to complete
        let results: Vec<_> = futures::future::join_all(handles).await;

        // All tasks should succeed (either creating or finding the table)
        let mut table_ids = vec![];
        for result in results {
            let table_id = result.expect("Task panicked").expect("create_table failed");
            table_ids.push(table_id);
        }

        // All tasks should have gotten the same table_id
        assert!(
            table_ids.windows(2).all(|w| w[0] == w[1]),
            "All concurrent create_table calls should return the same table_id"
        );

        // Verify the table exists and can be queried
        let table_metadata = catalog
            .get_table(table_name)
            .await
            .expect("Failed to get table metadata");

        assert_eq!(table_metadata.table_name, table_name);
        assert_eq!(table_metadata.table_id, table_ids[0]);

        // Cleanup test database
        let db_path = test_db.strip_prefix("sqlite://").unwrap_or(&test_db);
        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(format!("{db_path}-shm"));
        let _ = std::fs::remove_file(format!("{db_path}-wal"));
    }

    #[tokio::test]
    async fn test_concurrent_partition_creation() {
        // Create a unique test database to avoid conflicts with other tests
        let test_db = format!(
            "sqlite://./.test_concurrent_partition_{}.db",
            uuid::Uuid::now_v7()
        );
        let catalog = Arc::new(CayenneCatalog::new(&test_db).expect("Failed to create catalog"));

        // Initialize the catalog
        catalog.init().await.expect("Failed to initialize catalog");

        // Create a test table first
        let schema = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("id", arrow_schema::DataType::Int64, false),
            arrow_schema::Field::new("date", arrow_schema::DataType::Utf8, true),
        ]));

        let table_options = CreateTableOptions {
            table_name: "test_table".to_string(),
            schema,
            primary_key: vec![],
            base_path: "/tmp/cayenne_test_partition".to_string(),
            partition_column: Some("date".to_string()),
            vortex_config: crate::metadata::VortexConfig::default(),
        };

        let table_id = catalog
            .create_table(table_options)
            .await
            .expect("Failed to create table");

        // Spawn multiple tasks that all try to create the same partition concurrently
        let mut handles = vec![];
        for _ in 0..10 {
            let catalog_clone = Arc::clone(&catalog);

            let handle = tokio::spawn(async move {
                let partition = PartitionMetadata {
                    partition_id: 0, // Will be assigned by catalog
                    table_id,
                    partition_column: "date".to_string(),
                    partition_value: "2024-01-01".to_string(),
                    path: "/tmp/cayenne_test_partition/partition_20240101".to_string(),
                    path_is_relative: false,
                    record_count: 100,
                    file_size_bytes: 1024,
                };

                catalog_clone.add_partition(partition).await
            });

            handles.push(handle);
        }

        // Wait for all tasks to complete
        let results: Vec<_> = futures::future::join_all(handles).await;

        // All tasks should succeed (either creating or finding the partition)
        let mut partition_ids = vec![];
        for result in results {
            let partition_id = result
                .expect("Task panicked")
                .expect("add_partition failed");
            partition_ids.push(partition_id);
        }

        // All tasks should have gotten the same partition_id
        assert!(
            partition_ids.windows(2).all(|w| w[0] == w[1]),
            "All concurrent add_partition calls should return the same partition_id"
        );

        // Verify the partition exists and can be queried
        let partitions = catalog
            .get_partitions(table_id)
            .await
            .expect("Failed to get partitions");

        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0].partition_id, partition_ids[0]);
        assert_eq!(partitions[0].partition_value, "2024-01-01");

        // Cleanup test database
        let db_path = test_db.strip_prefix("sqlite://").unwrap_or(&test_db);
        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(format!("{db_path}-shm"));
        let _ = std::fs::remove_file(format!("{db_path}-wal"));
    }

    #[tokio::test]
    async fn test_concurrent_delete_file_creation() {
        // Create a unique test database to avoid conflicts with other tests
        let test_db = format!(
            "sqlite://./.test_concurrent_delete_file_{}.db",
            uuid::Uuid::now_v7()
        );
        let catalog = Arc::new(CayenneCatalog::new(&test_db).expect("Failed to create catalog"));

        // Initialize the catalog
        catalog.init().await.expect("Failed to initialize catalog");

        let table_id = 1;
        let data_file_id = 1;

        // Spawn multiple tasks that all try to create delete files concurrently
        let mut handles = vec![];
        for i in 0..10 {
            let catalog_clone = Arc::clone(&catalog);

            let handle = tokio::spawn(async move {
                let delete_file = DeleteFile {
                    delete_file_id: 0, // Will be assigned by catalog
                    table_id,
                    data_file_id,
                    path: format!("/tmp/delete_file_{i}.parquet"),
                    path_is_relative: false,
                    format: "parquet".to_string(),
                    delete_count: 10,
                    file_size_bytes: 512,
                };

                catalog_clone.add_delete_file(delete_file).await
            });

            handles.push(handle);
        }

        // Wait for all tasks to complete
        let results: Vec<_> = futures::future::join_all(handles).await;

        // All tasks should succeed with unique delete_file_ids
        let mut delete_file_ids = vec![];
        for result in results {
            let delete_file_id = result
                .expect("Task panicked")
                .expect("add_delete_file failed");
            delete_file_ids.push(delete_file_id);
        }

        // All delete_file_ids should be unique (unlike tables/partitions which are idempotent)
        let unique_ids: std::collections::HashSet<_> = delete_file_ids.iter().collect();
        assert_eq!(
            unique_ids.len(),
            delete_file_ids.len(),
            "All concurrent add_delete_file calls should return unique delete_file_ids"
        );

        // Verify all delete files were created
        let delete_files = catalog
            .get_table_delete_files(table_id)
            .await
            .expect("Failed to get delete files");

        assert_eq!(delete_files.len(), 10);

        // Cleanup test database
        let db_path = test_db.strip_prefix("sqlite://").unwrap_or(&test_db);
        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(format!("{db_path}-shm"));
        let _ = std::fs::remove_file(format!("{db_path}-wal"));
    }
}
