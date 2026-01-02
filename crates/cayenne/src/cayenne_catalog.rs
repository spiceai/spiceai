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
use super::metadata::{CreateTableOptions, DeleteFile, PartitionMetadata, TableMetadata};
use super::metastore::sqlite::SqliteMetastore;
#[cfg(feature = "turso")]
use super::metastore::turso::TursoMetastore;
use super::metastore::{
    ExecuteParams, MetastoreBackend, MetastoreRow, MetastoreValue, QueryParams, QueryRowParams,
};
use async_trait::async_trait;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

/// Metastore backend enum to support different implementations.
#[derive(Debug)]
pub(crate) enum MetastoreImpl {
    Sqlite(SqliteMetastore),
    #[cfg(feature = "turso")]
    Turso(TursoMetastore),
}

impl MetastoreImpl {
    /// Helper to query a single row from metastore, working with both `SQLite` and Turso
    pub(crate) async fn query_row_helper<F, T>(
        &self,
        params: QueryRowParams<'_>,
        f: F,
    ) -> CatalogResult<T>
    where
        F: FnOnce(&dyn MetastoreRow) -> CatalogResult<T> + Send + 'static,
        T: Send + 'static,
    {
        match self {
            MetastoreImpl::Sqlite(m) => m.query_row(params, f).await,
            #[cfg(feature = "turso")]
            MetastoreImpl::Turso(m) => m.query_row(params, f).await,
        }
    }

    /// Helper to execute a statement on metastore, working with both `SQLite` and Turso
    pub(crate) async fn execute_helper(&self, params: ExecuteParams<'_>) -> CatalogResult<()> {
        match self {
            MetastoreImpl::Sqlite(m) => m.execute(params).await,
            #[cfg(feature = "turso")]
            MetastoreImpl::Turso(m) => m.execute(params).await,
        }
    }

    /// Helper to execute a batch of SQL statements atomically.
    ///
    /// For `SQLite`, this runs all statements in a single transaction.
    /// The entire batch succeeds or fails as a unit.
    pub(crate) async fn execute_batch_helper(&self, sql: &str) -> CatalogResult<()> {
        match self {
            MetastoreImpl::Sqlite(m) => m.execute_batch(sql).await,
            #[cfg(feature = "turso")]
            MetastoreImpl::Turso(m) => m.execute_batch(sql).await,
        }
    }

    /// Helper to query multiple rows from metastore, working with both `SQLite` and Turso
    pub(crate) async fn query_helper<F, T>(
        &self,
        params: QueryParams<'_>,
        f: F,
    ) -> CatalogResult<Vec<T>>
    where
        F: Fn(&dyn MetastoreRow) -> CatalogResult<T> + Send + 'static,
        T: Send + 'static,
    {
        match self {
            MetastoreImpl::Sqlite(m) => m.query(params, f).await,
            #[cfg(feature = "turso")]
            MetastoreImpl::Turso(m) => m.query(params, f).await,
        }
    }
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
                return Err(CatalogError::TursoNotEnabled);
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
                    message: "Catalog shutdown task panicked.".to_string(),
                    source: Box::new(e),
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

#[async_trait]
impl MetadataCatalog for CayenneCatalog {
    async fn init(&self) -> CatalogResult<()> {
        // Create database directory if it doesn't exist
        let db_path = self.db_path();
        let db_dir =
            Path::new(db_path)
                .parent()
                .ok_or_else(|| CatalogError::InvalidDatabasePath {
                    path: db_path.to_string(),
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

    async fn create_table(&self, options: CreateTableOptions) -> CatalogResult<i64> {
        let table_name = options.table_name.clone();
        let base_path = options.base_path.clone();

        // Check if table already exists first (read-only check)
        let existing_table_id: Option<i64> = self
            .metastore
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
                            message: "Failed to serialize schema.".to_string(),
                            source: Box::new(e),
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
                    message: "Failed to serialize primary key.".to_string(),
                    source: Box::new(e),
                }
            })?)
        };

        let partition_column = options.partition_column.clone();

        // Generate table UUID
        let table_uuid = uuid::Uuid::now_v7().to_string();

        // Generate initial snapshot UUID
        let initial_snapshot_id = uuid::Uuid::now_v7().to_string();

        // Serialize Vortex config to JSON
        let vortex_config_json = serde_json::to_string(&options.vortex_config).map_err(|e| {
            CatalogError::InvalidOperation {
                message: "Failed to serialize vortex config.".to_string(),
                source: Box::new(e),
            }
        })?;

        // Insert table metadata with initial snapshot
        self.metastore
            .execute_helper(ExecuteParams {
                sql: r"
                    INSERT INTO cayenne_table (
                        table_uuid, table_name, path, path_is_relative, schema_json, primary_key_json,
                        current_snapshot_id, partition_column, vortex_config_json
                    ) VALUES (
                     ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9
                    )
                ",
                params: vec![
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
            .await?;

        // Retrieve the assigned table ID
        let table_id: i64 = self
            .metastore
            .query_row_helper(
                QueryRowParams {
                    sql: "SELECT table_id FROM cayenne_table WHERE table_name = ?1",
                    params: vec![MetastoreValue::Text(table_name.clone())],
                },
                |row| row.get_i64(0),
            )
            .await?;

        // Create the initial snapshot directory
        // Directory structure: [base_path]/[table_id]/[snapshot_id]/
        // Create the initial snapshot directory (only for local paths)
        // Directory structure: [base_path]/[table_id]/[snapshot_id]/
        // For S3 paths, directories are virtual and created when files are written
        if !base_path.starts_with("s3://") {
            let snapshot_dir = std::path::PathBuf::from(&base_path)
                .join(table_id.to_string())
                .join(&initial_snapshot_id);

            tokio::fs::create_dir_all(&snapshot_dir)
                .await
                .map_err(|e| CatalogError::Io { source: e })?;
        }

        Ok(table_id)
    }

    async fn get_table(&self, table_name: &str) -> CatalogResult<TableMetadata> {
        let table_name_owned = table_name.to_string();

        self.metastore
            .query_row_helper(
                QueryRowParams {
                    sql: r"
                    SELECT table_id, table_uuid,
                           table_name, path, path_is_relative, schema_json, primary_key_json,
                           current_snapshot_id, partition_column, vortex_config_json,
                           current_sequence_number
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
                    let current_sequence_number = row.get_optional_i64(10)?.unwrap_or(0);

                    // Deserialize schema using Arrow IPC format
                    let schema = {
                        use base64::Engine;
                        use bytes::Bytes;

                        let schema_bytes = base64::engine::general_purpose::STANDARD
                            .decode(&schema_json)
                            .map_err(|e| CatalogError::InvalidOperation {
                                message: "Failed to decode schema from base64".to_string(),
                                source: Box::new(e),
                            })?;

                        let ipc_message = arrow_flight::IpcMessage(Bytes::from(schema_bytes));
                        arrow_schema::Schema::try_from(ipc_message).map_err(|e| {
                            CatalogError::InvalidOperation {
                                message: "Failed to deserialize schema from IPC".to_string(),
                                source: Box::new(e),
                            }
                        })?
                    };

                    let schema = Arc::new(schema);

                    // Parse primary key
                    let primary_key = if let Some(pk_json) = primary_key_json {
                        serde_json::from_str(&pk_json).map_err(|e| {
                            CatalogError::InvalidOperation {
                                message: "Failed to deserialize primary key".to_string(),
                                source: Box::new(e),
                            }
                        })?
                    } else {
                        vec![]
                    };

                    // Parse vortex config
                    let vortex_config = if let Some(config_json) = vortex_config_json {
                        serde_json::from_str(&config_json).map_err(|e| {
                            CatalogError::InvalidOperation {
                                message: "Failed to deserialize vortex config.".to_string(),
                                source: Box::new(e),
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
                        current_sequence_number,
                    })
                },
            )
            .await
            .map_err(|e| CatalogError::FailedToGetTable {
                source: Box::new(e),
            })
    }

    async fn set_current_snapshot(&self, table_id: i64, snapshot_id: &str) -> CatalogResult<()> {
        self.metastore
            .execute_helper(ExecuteParams {
                sql: "UPDATE cayenne_table SET current_snapshot_id = ?1 WHERE table_id = ?2",
                params: vec![
                    MetastoreValue::Text(snapshot_id.to_string()),
                    MetastoreValue::Integer(table_id),
                ],
            })
            .await
            .map_err(|e| CatalogError::FailedToSetCurrentSnapshot {
                source: Box::new(e),
            })
    }

    async fn add_delete_file(&self, delete_file: DeleteFile) -> CatalogResult<i64> {
        // Insert delete file record
        let insert_result = self
            .metastore
            .execute_helper(ExecuteParams {
                sql: r"
                INSERT INTO cayenne_delete_file (
                    table_id, path, path_is_relative,
                    format, delete_count, file_size_bytes, source_data_file_path, sequence_number
                ) VALUES (
                    ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8
                )
            ",
                params: vec![
                    MetastoreValue::Integer(delete_file.table_id),
                    MetastoreValue::Text(delete_file.path.clone()),
                    MetastoreValue::Bool(delete_file.path_is_relative),
                    MetastoreValue::Text(delete_file.format.clone()),
                    MetastoreValue::Integer(delete_file.delete_count),
                    MetastoreValue::Integer(delete_file.file_size_bytes),
                    delete_file
                        .source_data_file_path
                        .clone()
                        .map_or(MetastoreValue::Null, MetastoreValue::Text),
                    MetastoreValue::Integer(delete_file.sequence_number),
                ],
            })
            .await;

        match insert_result {
            Err(CatalogError::Sqlite {
                source: rusqlite::Error::SqliteFailure(err, _),
            }) if err.code == rusqlite::ErrorCode::ConstraintViolation => {
                // Another concurrent operation inserted the same delete file
                // Retrieve the existing delete_file_id by falling through
            }
            Err(e) => {
                return Err(CatalogError::FailedToAddDeleteFile {
                    source: Box::new(e),
                })
            }
            Ok(()) => {}
        }

        // Retrieve the assigned delete_file_id
        let delete_file_id: i64 = self
            .metastore
            .query_row_helper(
                QueryRowParams {
                    sql: r"
                    SELECT delete_file_id
                    FROM cayenne_delete_file
                    WHERE table_id = ?1 AND path = ?2
                    ORDER BY delete_file_id DESC
                    LIMIT 1
                ",
                    params: vec![
                        MetastoreValue::Integer(delete_file.table_id),
                        MetastoreValue::Text(delete_file.path.clone()),
                    ],
                },
                |row| row.get_i64(0),
            )
            .await
            .map_err(|e| CatalogError::FailedToAddDeleteFile {
                source: Box::new(e),
            })?;

        Ok(delete_file_id)
    }

    async fn get_table_delete_files(&self, table_id: i64) -> CatalogResult<Vec<DeleteFile>> {
        self.metastore
            .query_helper(
                QueryParams {
                    sql: "SELECT delete_file_id, table_id, path, path_is_relative, 
                        format, delete_count, file_size_bytes, source_data_file_path, sequence_number 
                 FROM cayenne_delete_file 
                 WHERE table_id = ?1",
                    params: vec![MetastoreValue::Integer(table_id)],
                },
                |row| {
                    Ok(DeleteFile {
                        delete_file_id: row.get_i64(0)?,
                        table_id: row.get_i64(1)?,
                        source_data_file_path: row.get_optional_string(7)?,
                        path: row.get_string(2)?,
                        path_is_relative: row.get_bool(3)?,
                        format: row.get_string(4)?,
                        delete_count: row.get_i64(5)?,
                        file_size_bytes: row.get_i64(6)?,
                        // The actual deletion type is determined when reading the file
                        // based on the schema (row_id = position-based, row_key = key-based)
                        deletion_type: crate::metadata::DeletionType::default(),
                        sequence_number: row.get_optional_i64(8)?.unwrap_or(0),
                    })
                },
            )
            .await
            .map_err(|e| CatalogError::FailedToGetTableDeleteFiles {
                source: Box::new(e),
            })
    }

    async fn clear_delete_files(&self, table_id: i64) -> CatalogResult<()> {
        self.metastore
            .execute_helper(ExecuteParams {
                sql: "DELETE FROM cayenne_delete_file WHERE table_id = ?1",
                params: vec![MetastoreValue::Integer(table_id)],
            })
            .await
            .map_err(|e| CatalogError::FailedToGetTableDeleteFiles {
                source: Box::new(e),
            })?;
        Ok(())
    }

    async fn increment_sequence_number(&self, table_id: i64) -> CatalogResult<i64> {
        // Atomically increment and return the new sequence number
        self.metastore
            .execute_helper(ExecuteParams {
                sql: "UPDATE cayenne_table SET current_sequence_number = current_sequence_number + 1 WHERE table_id = ?1",
                params: vec![MetastoreValue::Integer(table_id)],
            })
            .await
            .map_err(|e| CatalogError::InvalidOperation {
                message: "Failed to increment sequence number".to_string(),
                source: Box::new(e),
            })?;

        // Retrieve the new sequence number
        self.get_sequence_number(table_id).await
    }

    async fn get_sequence_number(&self, table_id: i64) -> CatalogResult<i64> {
        self.metastore
            .query_row_helper(
                QueryRowParams {
                    sql: "SELECT current_sequence_number FROM cayenne_table WHERE table_id = ?1",
                    params: vec![MetastoreValue::Integer(table_id)],
                },
                |row| row.get_i64(0),
            )
            .await
            .map_err(|e| CatalogError::InvalidOperation {
                message: "Failed to get sequence number".to_string(),
                source: Box::new(e),
            })
    }

    async fn add_insert_record(
        &self,
        table_id: i64,
        pk_bytes: Vec<u8>,
        sequence_number: i64,
    ) -> CatalogResult<()> {
        // Use INSERT OR REPLACE to update sequence if PK already exists
        self.metastore
            .execute_helper(ExecuteParams {
                sql: "INSERT OR REPLACE INTO cayenne_insert_record (table_id, pk_bytes, sequence_number) VALUES (?1, ?2, ?3)",
                params: vec![
                    MetastoreValue::Integer(table_id),
                    MetastoreValue::Blob(pk_bytes),
                    MetastoreValue::Integer(sequence_number),
                ],
            })
            .await
            .map_err(|e| CatalogError::InvalidOperation {
                message: "Failed to add insert record entry".to_string(),
                source: Box::new(e),
            })?;
        Ok(())
    }

    async fn add_insert_records_batch(
        &self,
        table_id: i64,
        pk_bytes_list: Vec<Vec<u8>>,
        sequence_number: i64,
    ) -> CatalogResult<()> {
        if pk_bytes_list.is_empty() {
            return Ok(());
        }

        // Build a batch insert with all PKs
        // Using INSERT OR REPLACE to update sequence if PK already exists
        let mut values_parts = Vec::with_capacity(pk_bytes_list.len());
        let mut params = Vec::with_capacity(pk_bytes_list.len() * 3);

        for (i, pk_bytes) in pk_bytes_list.into_iter().enumerate() {
            let base = i * 3 + 1; // SQLite params are 1-indexed
            values_parts.push(format!("(?{}, ?{}, ?{})", base, base + 1, base + 2));
            params.push(MetastoreValue::Integer(table_id));
            params.push(MetastoreValue::Blob(pk_bytes));
            params.push(MetastoreValue::Integer(sequence_number));
        }

        let sql = format!(
            "INSERT OR REPLACE INTO cayenne_insert_record (table_id, pk_bytes, sequence_number) VALUES {}",
            values_parts.join(", ")
        );

        self.metastore
            .execute_helper(ExecuteParams { sql: &sql, params })
            .await
            .map_err(|e| CatalogError::InvalidOperation {
                message: "Failed to add insert record entries in batch".to_string(),
                source: Box::new(e),
            })?;
        Ok(())
    }

    async fn get_insert_records(
        &self,
        table_id: i64,
    ) -> CatalogResult<std::collections::HashMap<Box<[u8]>, i64>> {
        let results: Vec<(Vec<u8>, i64)> = self
            .metastore
            .query_helper(
                QueryParams {
                    sql: "SELECT pk_bytes, sequence_number FROM cayenne_insert_record WHERE table_id = ?1",
                    params: vec![MetastoreValue::Integer(table_id)],
                },
                |row| {
                    let pk_bytes = row.get_blob(0)?;
                    let sequence_number = row.get_i64(1)?;
                    Ok((pk_bytes, sequence_number))
                },
            )
            .await
            .map_err(|e| CatalogError::InvalidOperation {
                message: "Failed to get insert records".to_string(),
                source: Box::new(e),
            })?;

        Ok(results
            .into_iter()
            .map(|(pk, seq)| (pk.into_boxed_slice(), seq))
            .collect())
    }

    async fn clear_insert_records(&self, table_id: i64) -> CatalogResult<()> {
        self.metastore
            .execute_helper(ExecuteParams {
                sql: "DELETE FROM cayenne_insert_record WHERE table_id = ?1",
                params: vec![MetastoreValue::Integer(table_id)],
            })
            .await
            .map_err(|e| CatalogError::InvalidOperation {
                message: "Failed to clear insert records".to_string(),
                source: Box::new(e),
            })?;
        Ok(())
    }

    async fn set_snapshot_sequence(
        &self,
        table_id: i64,
        snapshot_id: &str,
        sequence_number: i64,
    ) -> CatalogResult<()> {
        self.metastore
            .execute_helper(ExecuteParams {
                sql: "INSERT OR REPLACE INTO cayenne_snapshot_sequence (table_id, snapshot_id, sequence_number) VALUES (?1, ?2, ?3)",
                params: vec![
                    MetastoreValue::Integer(table_id),
                    MetastoreValue::Text(snapshot_id.to_string()),
                    MetastoreValue::Integer(sequence_number),
                ],
            })
            .await
            .map_err(|e| CatalogError::InvalidOperation {
                message: "Failed to set snapshot sequence".to_string(),
                source: Box::new(e),
            })?;
        Ok(())
    }

    async fn get_snapshot_sequence(
        &self,
        table_id: i64,
        snapshot_id: &str,
    ) -> CatalogResult<Option<i64>> {
        let results: Vec<i64> = self
            .metastore
            .query_helper(
                QueryParams {
                    sql: "SELECT sequence_number FROM cayenne_snapshot_sequence WHERE table_id = ?1 AND snapshot_id = ?2",
                    params: vec![
                        MetastoreValue::Integer(table_id),
                        MetastoreValue::Text(snapshot_id.to_string()),
                    ],
                },
                |row| row.get_i64(0),
            )
            .await
            .map_err(|e| CatalogError::InvalidOperation {
                message: "Failed to get snapshot sequence".to_string(),
                source: Box::new(e),
            })?;

        Ok(results.into_iter().next())
    }

    async fn get_all_snapshot_sequences(
        &self,
        table_id: i64,
    ) -> CatalogResult<HashMap<String, i64>> {
        let results: Vec<(String, i64)> = self
            .metastore
            .query_helper(
                QueryParams {
                    sql: "SELECT snapshot_id, sequence_number FROM cayenne_snapshot_sequence WHERE table_id = ?1",
                    params: vec![MetastoreValue::Integer(table_id)],
                },
                |row| {
                    let snapshot_id = row.get_string(0)?;
                    let seq = row.get_i64(1)?;
                    Ok((snapshot_id, seq))
                },
            )
            .await
            .map_err(|e| CatalogError::InvalidOperation {
                message: "Failed to get all snapshot sequences".to_string(),
                source: Box::new(e),
            })?;

        Ok(results.into_iter().collect())
    }

    async fn clear_snapshot_sequence(&self, table_id: i64, snapshot_id: &str) -> CatalogResult<()> {
        self.metastore
            .execute_helper(ExecuteParams {
                sql: "DELETE FROM cayenne_snapshot_sequence WHERE table_id = ?1 AND snapshot_id = ?2",
                params: vec![
                    MetastoreValue::Integer(table_id),
                    MetastoreValue::Text(snapshot_id.to_string()),
                ],
            })
            .await
            .map_err(|e| CatalogError::InvalidOperation {
                message: format!("Failed to clear snapshot sequence for {snapshot_id}"),
                source: Box::new(e),
            })
    }

    async fn commit_compaction(&self, table_id: i64, new_snapshot_id: &str) -> CatalogResult<()> {
        // Execute all operations atomically using a transaction batch.
        // SQLite's execute_batch runs all statements in a single transaction,
        // ensuring atomicity: either all succeed or none takes effect.
        //
        // Order matters for crash safety:
        // 1. Clear delete files first - they reference the old snapshot's data
        // 2. Clear insert records - they correspond to the cleared delete files
        // 3. Update snapshot pointer - commits the new snapshot as active
        //
        // If interrupted between these, the old snapshot remains active with
        // no delete files, which is safe (just loses the pending deletions,
        // but data is not corrupted).
        let batch_sql = format!(
            "BEGIN TRANSACTION; \
             DELETE FROM cayenne_delete_file WHERE table_id = {table_id}; \
             DELETE FROM cayenne_insert_record WHERE table_id = {table_id}; \
             UPDATE cayenne_table SET current_snapshot_id = '{new_snapshot_id}' WHERE table_id = {table_id}; \
             COMMIT;"
        );

        self.metastore
            .execute_batch_helper(&batch_sql)
            .await
            .map_err(|e| CatalogError::FailedToSetCurrentSnapshot {
                source: Box::new(e),
            })?;

        Ok(())
    }

    async fn add_partition(&self, partition: PartitionMetadata) -> CatalogResult<i64> {
        // Check if partition already exists
        let existing_partition = self.metastore.query_row_helper(
                QueryRowParams {
                    sql: "SELECT partition_id FROM cayenne_partition WHERE table_id = ?1 AND partition_column = ?2 AND partition_value = ?3",
                    params: vec![
                        MetastoreValue::Integer(partition.table_id),
                        MetastoreValue::Text(partition.partition_column.clone()),
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

        // Insert partition metadata
        let insert_result = self.metastore.execute_helper(ExecuteParams {
                sql: r"
                INSERT INTO cayenne_partition (
                    table_id, partition_column, partition_value, path, path_is_relative, record_count, file_size_bytes
                ) VALUES (
                    ?1, ?2, ?3, ?4, ?5, ?6, ?7
                )",
                params: vec![
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

        match insert_result {
            Err(CatalogError::Sqlite {
                source: rusqlite::Error::SqliteFailure(err, _),
            }) if err.code == rusqlite::ErrorCode::ConstraintViolation => {
                // Another concurrent operation inserted the same partition
                // Retrieve the existing partition ID by falling through
            }
            Err(e) => {
                return Err(CatalogError::FailedToAddPartition {
                    source: Box::new(e),
                })
            }
            Ok(()) => {}
        }

        // Retrieve the assigned partition ID
        let partition_id: i64 = self.metastore
            .query_row_helper(
                QueryRowParams {
                    sql: "SELECT partition_id FROM cayenne_partition WHERE table_id = ?1 AND partition_column = ?2 AND partition_value = ?3",
                    params: vec![
                        MetastoreValue::Integer(partition.table_id),
                        MetastoreValue::Text(partition.partition_column.clone()),
                        MetastoreValue::Text(partition.partition_value.clone()),
                    ],
                },
                |row| row.get_i64(0),
            )
            .await
            .map_err(|e| CatalogError::FailedToAddPartition {
                source: Box::new(e),
            })?;

        Ok(partition_id)
    }

    async fn get_partitions(&self, table_id: i64) -> CatalogResult<Vec<PartitionMetadata>> {
        self.metastore.query_helper(
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
        .await.map_err(|e| CatalogError::FailedToGetPartitions {
            source: Box::new(e),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::DeletionType;
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

        // Insert the required table entry for the foreign key constraint
        catalog
            .metastore
            .execute_helper(ExecuteParams {
                sql: r"
                INSERT INTO cayenne_table (
                    table_uuid, table_name, path, path_is_relative, schema_json, primary_key_json,
                    current_snapshot_id, partition_column, vortex_config_json
                ) VALUES (
                 ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9
                )
            ",
                params: vec![
                    MetastoreValue::Text(uuid::Uuid::now_v7().to_string()),
                    MetastoreValue::Text("test_table".to_string()),
                    MetastoreValue::Text("/tmp/cayenne_test".to_string()),
                    MetastoreValue::Bool(false), // path_is_relative
                    MetastoreValue::Text("{}".to_string()), // empty schema
                    MetastoreValue::Null,        // primary_key_json
                    MetastoreValue::Text(uuid::Uuid::now_v7().to_string()), // current_snapshot_id
                    MetastoreValue::Null,        // partition_column
                    MetastoreValue::Text("{}".to_string()), // empty vortex_config_json
                ],
            })
            .await
            .expect("Failed to insert test table");

        // Spawn multiple tasks that all try to create delete files concurrently
        let mut handles = vec![];
        for i in 0..10 {
            let catalog_clone = Arc::clone(&catalog);

            let handle = tokio::spawn(async move {
                let delete_file = DeleteFile {
                    delete_file_id: 0, // Will be assigned by catalog
                    table_id,
                    source_data_file_path: None,
                    path: format!("/tmp/delete_file_{i}.parquet"),
                    path_is_relative: false,
                    format: "parquet".to_string(),
                    delete_count: 10,
                    file_size_bytes: 512,
                    deletion_type: DeletionType::default(),
                    sequence_number: 1, // Test sequence number
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
