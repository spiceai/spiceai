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

use super::catalog::{CatalogError, CatalogResult, MetadataCatalog, SnapshotSequenceCommit};
use super::metadata::{
    CreateTableOptions, DeleteFile, InlinedData, InlinedDataStats, InlinedDelete,
    PartitionMetadata, PkConflictDetection, TableMetadata, TableStatistics,
};
use super::metastore::sqlite::SqliteMetastore;
#[cfg(feature = "turso")]
use super::metastore::turso::TursoMetastore;
use super::metastore::{
    ExecuteParams, MetastoreBackend, MetastoreGetValue, MetastoreRow, MetastoreTransaction,
    MetastoreValue, QueryParams, QueryRowParams,
};
use async_trait::async_trait;
use datafusion_table_providers::util::on_conflict::OnConflict;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use turso_shared::{
    DEFAULT_CONCURRENT_WRITE_MAX_ATTEMPTS, is_retryable_write_conflict_message, retry_backoff_delay,
};

struct ExistingDeleteFileRecord {
    delete_file_id: String,
    path_is_relative: bool,
    format: String,
    delete_count: i64,
    file_size_bytes: i64,
    source_data_file_path: Option<String>,
    sequence_number: i64,
}

fn metastore_value_at(values: &[MetastoreValue], index: usize) -> CatalogResult<&MetastoreValue> {
    values.get(index).ok_or_else(|| CatalogError::Database {
        message: format!("Expected metastore value at index {index}"),
    })
}

fn existing_delete_file_record_from_values(
    values: &[MetastoreValue],
) -> CatalogResult<ExistingDeleteFileRecord> {
    Ok(ExistingDeleteFileRecord {
        delete_file_id: String::from_value(metastore_value_at(values, 0)?)?,
        path_is_relative: bool::from_value(metastore_value_at(values, 1)?)?,
        format: String::from_value(metastore_value_at(values, 2)?)?,
        delete_count: i64::from_value(metastore_value_at(values, 3)?)?,
        file_size_bytes: i64::from_value(metastore_value_at(values, 4)?)?,
        source_data_file_path: Option::<String>::from_value(metastore_value_at(values, 5)?)?,
        sequence_number: Option::<i64>::from_value(metastore_value_at(values, 6)?)?.unwrap_or(0),
    })
}

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

    /// Helper to execute a transactional batch on metastore, working with both `SQLite` and Turso
    pub(crate) async fn execute_transaction_batch_helper(&self, sql: &str) -> CatalogResult<()> {
        match self {
            MetastoreImpl::Sqlite(m) => m.execute_transaction_batch(sql).await,
            #[cfg(feature = "turso")]
            MetastoreImpl::Turso(m) => m.execute_transaction_batch(sql).await,
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

    /// Shutdown the metastore, performing any necessary cleanup.
    pub(crate) async fn shutdown(&self) -> CatalogResult<()> {
        match self {
            MetastoreImpl::Sqlite(m) => m.shutdown().await,
            #[cfg(feature = "turso")]
            MetastoreImpl::Turso(m) => m.shutdown().await,
        }
    }

    /// Begin a transaction on the underlying metastore.
    ///
    /// Each backend sends the appropriate BEGIN statement (e.g. `BEGIN TRANSACTION`
    /// for `SQLite`, `BEGIN CONCURRENT` for Turso). The returned transaction object
    /// holds exclusive access to the connection.
    pub(crate) async fn begin_transaction(
        &self,
    ) -> CatalogResult<Box<dyn super::metastore::MetastoreTransaction>> {
        match self {
            MetastoreImpl::Sqlite(m) => m.begin_transaction().await,
            #[cfg(feature = "turso")]
            MetastoreImpl::Turso(m) => m.begin_transaction().await,
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
        self.metastore.shutdown().await
    }

    /// Open a transaction on the underlying metastore.
    ///
    /// Each backend sends the appropriate BEGIN statement (e.g. `BEGIN IMMEDIATE`
    /// for `SQLite`, `BEGIN CONCURRENT` for Turso). The returned handle owns
    /// exclusive access to the connection until `commit` or `rollback` is
    /// called, or the handle is dropped (which auto-rolls-back).
    ///
    /// Used by the cross-partition coordinator (issue #10125) to batch every
    /// partition's [`Self::commit_compaction_in_txn`] call inside a single
    /// transaction. Single-partition callers should prefer the higher-level
    /// [`MetadataCatalog::commit_compaction`].
    ///
    /// # Errors
    ///
    /// Returns an error if the backend cannot begin a transaction (e.g.
    /// connection failure, busy timeout).
    pub async fn begin_transaction(&self) -> CatalogResult<Box<dyn MetastoreTransaction>> {
        self.metastore.begin_transaction().await
    }

    async fn existing_delete_file_record(
        &self,
        table_id: &str,
        path: &str,
    ) -> CatalogResult<Option<ExistingDeleteFileRecord>> {
        let records = self
            .metastore
            .query_helper(
                QueryParams {
                    sql: r"
                    SELECT delete_file_id, path_is_relative, format, delete_count,
                           file_size_bytes, source_data_file_path, sequence_number
                    FROM cayenne_delete_file
                    WHERE table_id = ?1 AND path = ?2
                    ORDER BY delete_file_id DESC
                    LIMIT 1
                ",
                    params: vec![
                        MetastoreValue::Text(table_id.to_string()),
                        MetastoreValue::Text(path.to_string()),
                    ],
                },
                |row| {
                    Ok(ExistingDeleteFileRecord {
                        delete_file_id: row.get_string(0)?,
                        path_is_relative: row.get_bool(1)?,
                        format: row.get_string(2)?,
                        delete_count: row.get_i64(3)?,
                        file_size_bytes: row.get_i64(4)?,
                        source_data_file_path: row.get_optional_string(5)?,
                        sequence_number: row.get_optional_i64(6)?.unwrap_or(0),
                    })
                },
            )
            .await?;

        Ok(records.into_iter().next())
    }

    async fn validate_existing_delete_file_if_present_in_transaction(
        tx: &dyn MetastoreTransaction,
        delete_file: &DeleteFile,
    ) -> CatalogResult<()> {
        // The failing `ON CONFLICT DO UPDATE` path uses SQLite's default ABORT
        // conflict mode: the statement is rolled back, but the transaction stays
        // open for this validation read. Turso is expected to preserve the same
        // SQLite-compatible transaction behavior.
        let count_values = tx
            .query_row_values(QueryRowParams {
                sql: r"
                    SELECT COUNT(*)
                    FROM cayenne_delete_file
                    WHERE table_id = ?1 AND path = ?2
                ",
                params: vec![
                    MetastoreValue::Text(delete_file.table_id.clone()),
                    MetastoreValue::Text(delete_file.path.clone()),
                ],
            })
            .await?;
        let existing_count = i64::from_value(metastore_value_at(&count_values, 0)?)?;
        if existing_count == 0 {
            return Ok(());
        }

        let record_values = tx
            .query_row_values(QueryRowParams {
                sql: r"
                    SELECT delete_file_id, path_is_relative, format, delete_count,
                           file_size_bytes, source_data_file_path, sequence_number
                    FROM cayenne_delete_file
                    WHERE table_id = ?1 AND path = ?2
                    ORDER BY delete_file_id DESC
                    LIMIT 1
                ",
                params: vec![
                    MetastoreValue::Text(delete_file.table_id.clone()),
                    MetastoreValue::Text(delete_file.path.clone()),
                ],
            })
            .await?;
        let existing_record = existing_delete_file_record_from_values(&record_values)?;
        validate_existing_delete_file_record(delete_file, &existing_record)
    }

    /// Apply a compaction commit's catalog mutations inside the caller's
    /// `MetastoreTransaction`, without opening a new transaction.
    ///
    /// This is the building block for cross-partition atomic commits
    /// (issue #10125): the coordinator opens one transaction via
    /// [`Self::begin_transaction`], calls this method for every participating
    /// partition, then commits the transaction once. Either every partition's
    /// snapshot pointer advances or none do.
    ///
    /// The mutations and their order match
    /// [`MetadataCatalog::commit_compaction`]:
    ///
    /// 1. `DELETE FROM cayenne_delete_file       WHERE table_id = ?`
    /// 2. `DELETE FROM cayenne_insert_record     WHERE table_id = ?`
    /// 3. `DELETE FROM cayenne_snapshot_sequence WHERE table_id = ?`
    /// 4. `UPDATE cayenne_table SET current_snapshot_id = ? WHERE table_id = ?`
    ///
    /// Caller owns transaction lifecycle: `commit` and retry-on-conflict are
    /// the coordinator's responsibility. This method does not retry — a
    /// `SQLITE_BUSY` / write-conflict on the borrowed transaction is surfaced
    /// to the caller so it can roll back and retry the entire cross-partition
    /// batch.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError::InvalidOperationNoSource`] if either UUID is
    /// malformed (validated to prevent SQL injection — both values are
    /// interpolated into the batch SQL).
    /// Returns [`CatalogError::FailedToSetCurrentSnapshot`] if the
    /// `execute_batch` call against the borrowed transaction fails.
    pub async fn commit_compaction_in_txn(
        &self,
        txn: &mut dyn MetastoreTransaction,
        table_id: &str,
        new_snapshot_id: &str,
    ) -> CatalogResult<()> {
        // Validate that IDs are well-formed UUIDs to prevent SQL injection.
        // Both values are generated internally via uuid::Uuid::now_v7(), but
        // we enforce the invariant here since they are interpolated into
        // batch SQL.
        for (name, value) in [("table_id", table_id), ("new_snapshot_id", new_snapshot_id)] {
            if uuid::Uuid::parse_str(value).is_err() {
                return Err(CatalogError::InvalidOperationNoSource {
                    message: format!("{name} is not a valid UUID: {value}"),
                });
            }
        }

        let table_id_literal = sql_text_literal(table_id);
        let new_snapshot_id_literal = sql_text_literal(new_snapshot_id);
        let batch_sql = format!(
            "DELETE FROM cayenne_delete_file WHERE table_id = {table_id_literal}; \
             DELETE FROM cayenne_insert_record WHERE table_id = {table_id_literal}; \
             DELETE FROM cayenne_snapshot_sequence WHERE table_id = {table_id_literal}; \
             UPDATE cayenne_table SET current_snapshot_id = {new_snapshot_id_literal} WHERE table_id = {table_id_literal};"
        );

        txn.execute_batch(&batch_sql)
            .await
            .map_err(|e| CatalogError::FailedToSetCurrentSnapshot {
                source: Box::new(e),
            })
    }

    /// Apply an overwrite commit's catalog mutations inside the caller's
    /// `MetastoreTransaction`, without opening a new transaction.
    ///
    /// Like [`Self::commit_compaction_in_txn`], this is the building block for
    /// cross-partition atomic commits; the coordinator opens one transaction,
    /// calls this method per participating partition, then commits.
    ///
    /// Differs from `commit_compaction_in_txn` in that overwrite REPLACES all
    /// of a table's contents, so anything keyed on the old snapshot must be
    /// dropped atomically with the pointer flip:
    ///
    /// 1. `DELETE FROM cayenne_delete_file       WHERE table_id = ?`
    /// 2. `DELETE FROM cayenne_insert_record     WHERE table_id = ?`
    /// 3. `DELETE FROM cayenne_snapshot_sequence WHERE table_id = ?`
    /// 4. `DELETE FROM cayenne_inlined_data      WHERE table_id = ?`
    /// 5. `DELETE FROM cayenne_inlined_delete    WHERE table_id = ?`
    /// 6. `DELETE FROM cayenne_table_statistics  WHERE table_id = ?`
    /// 7. `DELETE FROM cayenne_pk_index           WHERE table_id = ?`
    /// 8. `UPDATE cayenne_table SET current_snapshot_id = ? WHERE table_id = ?`
    ///
    /// Without (4)-(6) in the same transaction, a crash between the pointer
    /// flip and the (separate, post-commit) clears in `PreparedOverwrite::finish`
    /// would leave the catalog pointing at the new snapshot while inlined
    /// rows from the old snapshot continued to surface in scans (which UNION
    /// the listing table with inlined data) and stale table stats biased
    /// the query planner.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError::InvalidOperationNoSource`] if either UUID is
    /// malformed.
    /// Returns [`CatalogError::FailedToSetCurrentSnapshot`] if the
    /// `execute_batch` call against the borrowed transaction fails.
    pub async fn commit_overwrite_in_txn(
        &self,
        txn: &mut dyn MetastoreTransaction,
        table_id: &str,
        new_snapshot_id: &str,
    ) -> CatalogResult<()> {
        for (name, value) in [("table_id", table_id), ("new_snapshot_id", new_snapshot_id)] {
            if uuid::Uuid::parse_str(value).is_err() {
                return Err(CatalogError::InvalidOperationNoSource {
                    message: format!("{name} is not a valid UUID: {value}"),
                });
            }
        }

        let table_id_literal = sql_text_literal(table_id);
        let new_snapshot_id_literal = sql_text_literal(new_snapshot_id);
        let batch_sql = format!(
            "DELETE FROM cayenne_delete_file WHERE table_id = {table_id_literal}; \
             DELETE FROM cayenne_insert_record WHERE table_id = {table_id_literal}; \
             DELETE FROM cayenne_snapshot_sequence WHERE table_id = {table_id_literal}; \
             DELETE FROM cayenne_inlined_data WHERE table_id = {table_id_literal}; \
             DELETE FROM cayenne_inlined_delete WHERE table_id = {table_id_literal}; \
             DELETE FROM cayenne_table_statistics WHERE table_id = {table_id_literal}; \
             DELETE FROM cayenne_pk_index WHERE table_id = {table_id_literal}; \
             UPDATE cayenne_table SET current_snapshot_id = {new_snapshot_id_literal} WHERE table_id = {table_id_literal};"
        );

        txn.execute_batch(&batch_sql)
            .await
            .map_err(|e| CatalogError::FailedToSetCurrentSnapshot {
                source: Box::new(e),
            })
    }

    async fn validate_existing_table_configuration(
        &self,
        table_name: &str,
        options: &CreateTableOptions,
    ) -> CatalogResult<TableMetadata> {
        match self.get_table(table_name).await {
            Ok(stored_metadata) => {
                log_runtime_footer_cache_drift(table_name, &stored_metadata, options);

                if configuration_matches(&stored_metadata, options) {
                    return Ok(stored_metadata);
                }

                log_configuration_differences(table_name, &stored_metadata, options);

                Err(CatalogError::ChangedConfiguration {
                    table_name: table_name.to_string(),
                })
            }
            Err(e) => Err(CatalogError::InvalidMetadata {
                table_name: table_name.to_string(),
                source: Box::new(e),
            }),
        }
    }

    /// Execute a single parameterized `INSERT OR REPLACE` for a chunk of insert
    /// records that fits within `SQLite`'s parameter limit.
    async fn insert_records_chunk(
        &self,
        table_id: &str,
        pk_bytes_list: Vec<Vec<u8>>,
        sequence_number: i64,
    ) -> CatalogResult<()> {
        let (sql, params) =
            Self::build_insert_records_chunk_sql(table_id, &pk_bytes_list, sequence_number);

        self.metastore
            .execute_helper(ExecuteParams { sql: &sql, params })
            .await
            .map_err(|e| CatalogError::InvalidOperation {
                message: "Failed to add insert record entries in batch".to_string(),
                source: Box::new(e),
            })?;
        Ok(())
    }

    /// Build the SQL and parameters for a single chunk of insert records.
    fn build_insert_records_chunk_sql(
        table_id: &str,
        pk_bytes_list: &[Vec<u8>],
        sequence_number: i64,
    ) -> (String, Vec<MetastoreValue>) {
        use std::fmt::Write as _;

        const PREFIX: &str = "INSERT OR REPLACE INTO cayenne_insert_record \
             (insert_record_id, table_id, pk_bytes, sequence_number) VALUES ";
        // Each "(?N, ?N, ?N, ?N)" row is ≤ 32 bytes for the placeholder counts we hit.
        let mut sql = String::with_capacity(PREFIX.len() + pk_bytes_list.len() * 32);
        sql.push_str(PREFIX);
        let mut params = Vec::with_capacity(pk_bytes_list.len() * 4);

        for (i, pk_bytes) in pk_bytes_list.iter().enumerate() {
            let base = i * 4 + 1; // SQLite params are 1-indexed
            if i > 0 {
                sql.push_str(", ");
            }
            // `write!` into a `String` is infallible.
            let _ = write!(
                sql,
                "(?{}, ?{}, ?{}, ?{})",
                base,
                base + 1,
                base + 2,
                base + 3
            );
            params.push(MetastoreValue::Text(uuid::Uuid::now_v7().to_string()));
            params.push(MetastoreValue::Text(table_id.to_string()));
            params.push(MetastoreValue::Blob(pk_bytes.clone()));
            params.push(MetastoreValue::Integer(sequence_number));
        }

        (sql, params)
    }

    /// Build a multi-VALUES `INSERT ... ON CONFLICT(table_id, path) DO UPDATE`
    /// for a chunk of delete-file rows. Each row uses 9 parameters; the
    /// per-row `ON CONFLICT` clause references `excluded` (the single
    /// conflicting row), so the idempotency check is the same as the
    /// single-row form previously emitted in `commit_on_conflict_deletions`.
    fn build_insert_delete_files_chunk_sql(
        delete_files: &[DeleteFile],
    ) -> (String, Vec<MetastoreValue>) {
        use std::fmt::Write as _;

        const PARAMS_PER_ROW: usize = 9;
        const PREFIX: &str = "INSERT INTO cayenne_delete_file (\
                 delete_file_id, table_id, path, path_is_relative, \
                 format, delete_count, file_size_bytes, source_data_file_path, sequence_number\
             ) VALUES ";
        const SUFFIX: &str = " \
             ON CONFLICT(table_id, path) DO UPDATE SET \
                 path = CASE \
                     WHEN cayenne_delete_file.path_is_relative = excluded.path_is_relative \
                         AND cayenne_delete_file.format = excluded.format \
                         AND cayenne_delete_file.delete_count = excluded.delete_count \
                         AND cayenne_delete_file.file_size_bytes = excluded.file_size_bytes \
                         AND cayenne_delete_file.source_data_file_path IS excluded.source_data_file_path \
                         AND cayenne_delete_file.sequence_number = excluded.sequence_number \
                     THEN cayenne_delete_file.path \
                     ELSE NULL \
                 END";
        // Each "(?N, ?N, ?N, ?N, ?N, ?N, ?N, ?N, ?N)" row averages ~64 bytes.
        let mut sql = String::with_capacity(PREFIX.len() + SUFFIX.len() + delete_files.len() * 64);
        sql.push_str(PREFIX);
        let mut params = Vec::with_capacity(delete_files.len() * PARAMS_PER_ROW);

        for (i, delete_file) in delete_files.iter().enumerate() {
            let base = i * PARAMS_PER_ROW + 1; // 1-indexed
            if i > 0 {
                sql.push_str(", ");
            }
            let _ = write!(
                sql,
                "(?{}, ?{}, ?{}, ?{}, ?{}, ?{}, ?{}, ?{}, ?{})",
                base,
                base + 1,
                base + 2,
                base + 3,
                base + 4,
                base + 5,
                base + 6,
                base + 7,
                base + 8,
            );
            params.push(MetastoreValue::Text(uuid::Uuid::now_v7().to_string()));
            params.push(MetastoreValue::Text(delete_file.table_id.clone()));
            params.push(MetastoreValue::Text(delete_file.path.clone()));
            params.push(MetastoreValue::Bool(delete_file.path_is_relative));
            params.push(MetastoreValue::Text(delete_file.format.clone()));
            params.push(MetastoreValue::Integer(delete_file.delete_count));
            params.push(MetastoreValue::Integer(delete_file.file_size_bytes));
            params.push(
                delete_file
                    .source_data_file_path
                    .clone()
                    .map_or(MetastoreValue::Null, MetastoreValue::Text),
            );
            params.push(MetastoreValue::Integer(delete_file.sequence_number));
        }

        sql.push_str(SUFFIX);
        (sql, params)
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

            // Best-effort sync of the parent directory so the db_dir entry
            // itself is durable on local FS before we proceed to create the
            // catalog DB file and initialize its schema.
            //
            // We keep this best-effort (with warning on failure) rather than
            // fatal because:
            // - Catalog DB directory creation is a one-time initialization
            //   event (not a hot write path).
            // - It is immediately followed by DB file creation and schema
            //   initialization, which provide strong content durability.
            // - The parent directory is frequently a stable, operator-
            //   managed volume root (e.g., K8s PersistentVolume) where
            //   directory entry durability is already handled at a higher
            //   level.
            //
            // This is still the right thing to do for consistency with the
            // uniform durability contract used for all per-table mutable
            // data paths, and it gives operators a clear warning if
            // something unusual happens on a fresh deployment.
            if let Some(parent) = db_dir.parent() {
                let parent = parent.to_path_buf();
                if let Err(e) = tokio::task::spawn_blocking(move || {
                    std::fs::File::open(&parent).and_then(|f| f.sync_all())
                })
                .await
                {
                    tracing::warn!(
                        "Failed to sync parent of catalog DB directory {} (subsequent DB writes will still be durable; directory entry may not survive crash): {e}",
                        db_dir.display()
                    );
                }
            }
        }

        // Initialize schema using the appropriate metastore backend
        match &self.metastore {
            MetastoreImpl::Sqlite(metastore) => metastore.init_schema().await?,
            #[cfg(feature = "turso")]
            MetastoreImpl::Turso(metastore) => metastore.init_schema().await?,
        }

        Ok(())
    }

    async fn list_table_names(&self) -> CatalogResult<Vec<String>> {
        self.metastore
            .query_helper(
                QueryParams {
                    sql: "SELECT table_name FROM cayenne_table ORDER BY table_name",
                    params: vec![],
                },
                |row| row.get_string(0),
            )
            .await
    }

    async fn create_table(&self, options: CreateTableOptions) -> CatalogResult<String> {
        let table_name = options.table_name.clone();
        let base_path = options.base_path.clone();

        validate_create_table_options(&options)?;

        // Check if table already exists first (read-only check)
        let existing_table_id: Option<String> = self
            .metastore
            .query_row_helper(
                QueryRowParams {
                    sql: "SELECT table_id FROM cayenne_table WHERE table_name = ?1",
                    params: vec![MetastoreValue::Text(table_name.clone())],
                },
                |row| row.get_string(0),
            )
            .await
            .ok();

        if let Some(ref existing_id) = existing_table_id {
            return match self
                .validate_existing_table_configuration(table_name.as_str(), &options)
                .await
            {
                Ok(stored_metadata) => Ok(stored_metadata.table_id),
                Err(CatalogError::ChangedConfiguration { .. }) => {
                    // Fall back to stored config — the warning was already logged by
                    // validate_existing_table_configuration.
                    Ok(existing_id.clone())
                }
                Err(e) => Err(e),
            };
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
        let on_conflict_json = options.on_conflict.as_ref().map(ToString::to_string);

        let partition_column = options.partition_column.clone();

        // Generate table ID (UUIDv7)
        let table_id = uuid::Uuid::now_v7().to_string();

        // Generate initial snapshot UUID
        let initial_snapshot_id = uuid::Uuid::now_v7().to_string();

        // Create the initial snapshot directory *before* inserting the table
        // row into the metastore. This ensures the directory entry is durable
        // (with parent sync of the table root) before the catalog "commits"
        // the existence of a table pointing at this snapshot_id. This is the
        // final piece of the uniform local-FS durability contract (snapshot
        // dirs, _partitioned_wal/, deletions/, and now initial table creation).
        // Matches the contract we enforce everywhere else in the write path.
        if !base_path.starts_with("s3://") {
            let table_root = std::path::PathBuf::from(&base_path).join(&table_id);
            let snapshot_dir = table_root.join(&initial_snapshot_id);

            if !snapshot_dir.exists() {
                tokio::fs::create_dir_all(&snapshot_dir)
                    .await
                    .map_err(|e| CatalogError::Io { source: e })?;

                // Sync the table root (parent of the new snapshot dir) so the
                // subdir entry is durable on local FS. Best-effort on the sync
                // itself (creation failure is already fatal above); this is
                // the same pattern used for the first _partitioned_wal/ and
                // first deletions/ subdirs.
                let table_root_for_sync = table_root.clone();
                let _ = tokio::task::spawn_blocking(move || {
                    let _ = std::fs::File::open(&table_root_for_sync).and_then(|f| f.sync_all());
                })
                .await;
            }
        }

        // Serialize Vortex config to JSON
        let vortex_config_json = serde_json::to_string(&options.vortex_config).map_err(|e| {
            CatalogError::InvalidOperation {
                message: "Failed to serialize vortex config.".to_string(),
                source: Box::new(e),
            }
        })?;

        // Insert table metadata with initial snapshot
        let insert_result = self
            .metastore
            .execute_helper(ExecuteParams {
                sql: r"
                    INSERT INTO cayenne_table (
                        table_id, table_name, path, path_is_relative, schema_json, primary_key_json,
                        on_conflict_json, current_snapshot_id, partition_column, vortex_config_json
                    ) VALUES (
                     ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10
                    )
                ",
                params: vec![
                    MetastoreValue::Text(table_id.clone()),
                    MetastoreValue::Text(table_name.clone()),
                    MetastoreValue::Text(base_path.clone()),
                    MetastoreValue::Bool(false), // path_is_relative
                    MetastoreValue::Text(schema_json),
                    primary_key_json.map_or(MetastoreValue::Null, MetastoreValue::Text),
                    on_conflict_json.map_or(MetastoreValue::Null, MetastoreValue::Text),
                    MetastoreValue::Text(initial_snapshot_id.clone()),
                    partition_column.map_or(MetastoreValue::Null, MetastoreValue::Text),
                    MetastoreValue::Text(vortex_config_json),
                ],
            })
            .await;

        match insert_result {
            Ok(()) => {}
            Err(CatalogError::ConstraintViolation { .. }) => {
                match self
                    .validate_existing_table_configuration(table_name.as_str(), &options)
                    .await
                {
                    Ok(existing_table) => {
                        ensure_snapshot_directory_exists(&existing_table).await?;
                        return Ok(existing_table.table_id);
                    }
                    Err(CatalogError::ChangedConfiguration { .. }) => {
                        // Fall back to stored config — the warning was already logged.
                        let stored = self.get_table(&table_name).await?;
                        ensure_snapshot_directory_exists(&stored).await?;
                        return Ok(stored.table_id);
                    }
                    Err(e) => return Err(e),
                }
            }
            Err(e) => return Err(e),
        }

        // The initial snapshot directory was already created (with parent
        // sync) before the metastore INSERT, so the catalog row now points
        // at a durable directory. Nothing more to do here for local FS.

        Ok(table_id)
    }

    async fn get_table(&self, table_name: &str) -> CatalogResult<TableMetadata> {
        let table_name_owned = table_name.to_string();

        let results = self.metastore
            .query_helper(
                QueryParams {
                    sql: r"
                    SELECT table_id,
                           table_name, path, path_is_relative, schema_json, primary_key_json,
                           on_conflict_json, current_snapshot_id, partition_column, vortex_config_json,
                           current_sequence_number
                    FROM cayenne_table
                    WHERE table_name = ?1
                    LIMIT 1
                ",
                    params: vec![MetastoreValue::Text(table_name_owned.clone())],
                },
                |row| {
                    let table_id = row.get_string(0)?;
                    let table_name = row.get_string(1)?;
                    let path = row.get_string(2)?;
                    let path_is_relative = row.get_bool(3)?;
                    let schema_json = row.get_string(4)?;
                    let primary_key_json = row.get_optional_string(5)?;
                    let on_conflict_json = row.get_optional_string(6)?;
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

                    let on_conflict = if let Some(oc_str) = on_conflict_json {
                        Some(
                            datafusion_table_providers::util::on_conflict::OnConflict::try_from(
                                oc_str.as_str(),
                            )
                            .map_err(|e| CatalogError::InvalidOperation {
                                message: "Failed to deserialize on_conflict".to_string(),
                                source: Box::new(e),
                            })?,
                        )
                    } else {
                        None
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
                        table_name,
                        path,
                        path_is_relative,
                        schema,
                        primary_key,
                        on_conflict,
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
            })?;

        results
            .into_iter()
            .next()
            .ok_or(CatalogError::TableNotFound {
                table_name: table_name_owned,
            })
    }

    async fn set_current_snapshot(&self, table_id: &str, snapshot_id: &str) -> CatalogResult<()> {
        self.metastore
            .execute_helper(ExecuteParams {
                sql: "UPDATE cayenne_table SET current_snapshot_id = ?1 WHERE table_id = ?2",
                params: vec![
                    MetastoreValue::Text(snapshot_id.to_string()),
                    MetastoreValue::Text(table_id.to_string()),
                ],
            })
            .await
            .map_err(|e| CatalogError::FailedToSetCurrentSnapshot {
                source: Box::new(e),
            })
    }

    async fn add_delete_file(&self, delete_file: DeleteFile) -> CatalogResult<String> {
        // Generate delete file ID (UUIDv7)
        let delete_file_id = uuid::Uuid::now_v7().to_string();

        // Insert delete file record
        let insert_result = self
            .metastore
            .execute_helper(ExecuteParams {
                sql: r"
                INSERT INTO cayenne_delete_file (
                    delete_file_id, table_id, path, path_is_relative,
                    format, delete_count, file_size_bytes, source_data_file_path, sequence_number
                ) VALUES (
                    ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9
                )
            ",
                params: vec![
                    MetastoreValue::Text(delete_file_id.clone()),
                    MetastoreValue::Text(delete_file.table_id.clone()),
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
            Ok(()) => Ok(delete_file_id),
            Err(CatalogError::ConstraintViolation { message })
                if is_delete_file_unique_constraint_violation_message(&message) =>
            {
                // Another concurrent operation inserted first — only treat this as idempotent
                // when the existing row matches the incoming delete-file metadata.
                let existing_record = self
                    .existing_delete_file_record(&delete_file.table_id, &delete_file.path)
                    .await
                    .map_err(|e| CatalogError::FailedToAddDeleteFile {
                        source: Box::new(e),
                    })?
                    .ok_or_else(|| CatalogError::ConstraintViolation {
                        message: format!(
                            "Delete file path '{}' for table '{}' hit a unique constraint but the existing row could not be found",
                            delete_file.path, delete_file.table_id
                        ),
                    })?;

                validate_existing_delete_file_record(&delete_file, &existing_record).map_err(
                    |e| CatalogError::FailedToAddDeleteFile {
                        source: Box::new(e),
                    },
                )?;

                Ok(existing_record.delete_file_id)
            }
            Err(e) => Err(CatalogError::FailedToAddDeleteFile {
                source: Box::new(e),
            }),
        }
    }

    async fn get_table_delete_files(&self, table_id: &str) -> CatalogResult<Vec<DeleteFile>> {
        self.metastore
            .query_helper(
                QueryParams {
                    sql: "SELECT delete_file_id, table_id, path, path_is_relative, 
                        format, delete_count, file_size_bytes, source_data_file_path, sequence_number 
                 FROM cayenne_delete_file 
                 WHERE table_id = ?1",
                    params: vec![MetastoreValue::Text(table_id.to_string())],
                },
                |row| {
                    Ok(DeleteFile {
                        delete_file_id: row.get_string(0)?,
                        table_id: row.get_string(1)?,
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

    async fn remove_delete_files(
        &self,
        table_id: &str,
        delete_file_ids: &[String],
    ) -> CatalogResult<()> {
        use std::fmt::Write as _;

        const PREFIX: &str =
            "DELETE FROM cayenne_delete_file WHERE table_id = ?1 AND delete_file_id IN (";

        if delete_file_ids.is_empty() {
            return Ok(());
        }
        let mut sql = String::with_capacity(PREFIX.len() + delete_file_ids.len() * 6 + 1);
        sql.push_str(PREFIX);
        for idx in 0..delete_file_ids.len() {
            if idx > 0 {
                sql.push_str(", ");
            }
            let _ = write!(sql, "?{}", idx + 2);
        }
        sql.push(')');

        let mut params = Vec::with_capacity(delete_file_ids.len() + 1);
        params.push(MetastoreValue::Text(table_id.to_string()));
        for id in delete_file_ids {
            params.push(MetastoreValue::Text(id.clone()));
        }

        self.metastore
            .execute_helper(ExecuteParams { sql: &sql, params })
            .await
    }

    async fn clear_delete_files(&self, table_id: &str) -> CatalogResult<()> {
        self.metastore
            .execute_helper(ExecuteParams {
                sql: "DELETE FROM cayenne_delete_file WHERE table_id = ?1",
                params: vec![MetastoreValue::Text(table_id.to_string())],
            })
            .await
            .map_err(|e| CatalogError::FailedToGetTableDeleteFiles {
                source: Box::new(e),
            })?;
        Ok(())
    }

    async fn increment_sequence_number(&self, table_id: &str) -> CatalogResult<i64> {
        self.reserve_sequence_numbers(table_id, 1).await
    }

    async fn reserve_sequence_numbers(&self, table_id: &str, count: u32) -> CatalogResult<i64> {
        if count == 0 {
            return Err(CatalogError::InvalidOperationNoSource {
                message: "reserve_sequence_numbers called with count=0".to_string(),
            });
        }
        let delta = i64::from(count);
        let max_attempts = DEFAULT_CONCURRENT_WRITE_MAX_ATTEMPTS;

        for attempt in 1..=max_attempts {
            let tx = match self.metastore.begin_transaction().await {
                Ok(tx) => tx,
                Err(e) => {
                    if retry_on_metastore_write_conflict(
                        &e,
                        attempt,
                        max_attempts,
                        "begin sequence reservation transaction",
                    )
                    .await
                    {
                        continue;
                    }
                    return Err(CatalogError::InvalidOperation {
                        message: format!(
                            "Failed to begin transaction reserving {count} sequence numbers"
                        ),
                        source: Box::new(e),
                    });
                }
            };

            let row_values = match tx
                .query_row_values(QueryRowParams {
                    sql: "UPDATE cayenne_table SET current_sequence_number = current_sequence_number + ?2 WHERE table_id = ?1 RETURNING current_sequence_number",
                    params: vec![
                        MetastoreValue::Text(table_id.to_string()),
                        MetastoreValue::Integer(delta),
                    ],
                })
                .await
            {
                Ok(row_values) => row_values,
                Err(e) => {
                    if should_retry_metastore_write_conflict(&e, attempt, max_attempts) {
                        drop(tx);
                        sleep_before_metastore_write_retry(
                            attempt,
                            max_attempts,
                            "reserve sequence number block",
                        )
                        .await;
                        continue;
                    }
                    if is_query_returned_no_rows(&e) {
                        return Err(CatalogError::InvalidOperationNoSource {
                            message: format!(
                                "Cannot reserve {count} sequence numbers for table_id '{table_id}': table row does not exist"
                            ),
                        });
                    }
                    return Err(CatalogError::InvalidOperation {
                        message: format!("Failed to reserve {count} sequence numbers"),
                        source: Box::new(e),
                    });
                }
            };
            let Some(new_high_value) = row_values.first() else {
                return Err(CatalogError::InvalidOperationNoSource {
                    message: "Failed to read reserved sequence high-water mark: query returned no columns"
                        .to_string(),
                });
            };
            let new_high =
                i64::from_value(new_high_value).map_err(|e| CatalogError::InvalidOperation {
                    message: "Failed to parse reserved sequence high-water mark".to_string(),
                    source: Box::new(e),
                })?;

            match tx.commit().await {
                Ok(()) => {
                    // The reserved block is [new_high - delta + 1, new_high]
                    return Ok(new_high - delta + 1);
                }
                Err(e) => {
                    if retry_on_metastore_write_conflict(
                        &e,
                        attempt,
                        max_attempts,
                        "commit sequence reservation transaction",
                    )
                    .await
                    {
                        continue;
                    }
                    return Err(CatalogError::InvalidOperation {
                        message: format!(
                            "Failed to commit reservation of {count} sequence numbers"
                        ),
                        source: Box::new(e),
                    });
                }
            }
        }

        Err(CatalogError::InvalidOperationNoSource {
            message: format!(
                "reserve_sequence_numbers exhausted {max_attempts} retry attempts after retryable write conflicts"
            ),
        })
    }

    async fn get_sequence_number(&self, table_id: &str) -> CatalogResult<i64> {
        self.metastore
            .query_row_helper(
                QueryRowParams {
                    sql: "SELECT current_sequence_number FROM cayenne_table WHERE table_id = ?1",
                    params: vec![MetastoreValue::Text(table_id.to_string())],
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
        table_id: &str,
        pk_bytes: Vec<u8>,
        sequence_number: i64,
    ) -> CatalogResult<()> {
        let insert_record_id = uuid::Uuid::now_v7().to_string();
        // Use INSERT OR REPLACE to update sequence if PK already exists
        self.metastore
            .execute_helper(ExecuteParams {
                sql: "INSERT OR REPLACE INTO cayenne_insert_record (insert_record_id, table_id, pk_bytes, sequence_number) VALUES (?1, ?2, ?3, ?4)",
                params: vec![
                    MetastoreValue::Text(insert_record_id),
                    MetastoreValue::Text(table_id.to_string()),
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
        table_id: &str,
        pk_bytes_list: Vec<Vec<u8>>,
        sequence_number: i64,
    ) -> CatalogResult<()> {
        // SQLite has a compile-time limit of SQLITE_MAX_VARIABLE_NUMBER (default
        // 32 766) parameters per prepared statement.  Each row needs 4 params
        // (insert_record_id, table_id, pk_bytes, sequence_number), so we chunk
        // the list to stay within the limit.
        //
        // All chunks are wrapped in a single transaction so the operation is
        // atomic: either every chunk is applied or none is.
        const PARAMS_PER_ROW: usize = 4;
        const MAX_PARAMS: usize = 32_000; // conservative cap below the 32 766 default
        const MAX_ROWS_PER_CHUNK: usize = MAX_PARAMS / PARAMS_PER_ROW;

        if pk_bytes_list.is_empty() {
            return Ok(());
        }

        // Single chunk that fits within the parameter limit — no transaction
        // overhead needed.
        if pk_bytes_list.len() <= MAX_ROWS_PER_CHUNK {
            return self
                .insert_records_chunk(table_id, pk_bytes_list, sequence_number)
                .await;
        }

        // Multiple chunks required — use a proper transaction for atomicity.
        // The transaction holds exclusive access to the connection, preventing
        // concurrent operations from interleaving BEGIN/COMMIT boundaries.
        let tx = self.metastore.begin_transaction().await.map_err(|e| {
            CatalogError::InvalidOperation {
                message: "Failed to begin transaction for batch insert records".to_string(),
                source: Box::new(e),
            }
        })?;

        for chunk in pk_bytes_list.chunks(MAX_ROWS_PER_CHUNK) {
            let (sql, params) =
                Self::build_insert_records_chunk_sql(table_id, chunk, sequence_number);
            if let Err(e) = tx.execute(ExecuteParams { sql: &sql, params }).await {
                // Transaction auto-rolls-back on drop.
                return Err(CatalogError::InvalidOperation {
                    message: "Failed to add insert record entries in batch".to_string(),
                    source: Box::new(e),
                });
            }
        }

        tx.commit()
            .await
            .map_err(|e| CatalogError::InvalidOperation {
                message: "Failed to commit transaction for batch insert records".to_string(),
                source: Box::new(e),
            })?;

        Ok(())
    }

    async fn get_insert_records(
        &self,
        table_id: &str,
    ) -> CatalogResult<std::collections::HashMap<Box<[u8]>, i64>> {
        let results: Vec<(Vec<u8>, i64)> = self
            .metastore
            .query_helper(
                QueryParams {
                    sql: "SELECT pk_bytes, sequence_number FROM cayenne_insert_record WHERE table_id = ?1",
                    params: vec![MetastoreValue::Text(table_id.to_string())],
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

        // Pre-size the map so the load path skips bucket reallocations as the
        // insert-record set grows; `collect()` starts from capacity 0 and grows
        // by doubling.
        let mut map = std::collections::HashMap::<Box<[u8]>, i64>::with_capacity(results.len());
        for (pk, seq) in results {
            map.insert(pk.into_boxed_slice(), seq);
        }
        Ok(map)
    }

    async fn clear_insert_records(&self, table_id: &str) -> CatalogResult<()> {
        self.metastore
            .execute_helper(ExecuteParams {
                sql: "DELETE FROM cayenne_insert_record WHERE table_id = ?1",
                params: vec![MetastoreValue::Text(table_id.to_string())],
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
        table_id: &str,
        snapshot_id: &str,
        sequence_number: i64,
    ) -> CatalogResult<()> {
        self.metastore
            .execute_helper(ExecuteParams {
                sql: "INSERT OR REPLACE INTO cayenne_snapshot_sequence (table_id, snapshot_id, sequence_number) VALUES (?1, ?2, ?3)",
                params: vec![
                    MetastoreValue::Text(table_id.to_string()),
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
        table_id: &str,
        snapshot_id: &str,
    ) -> CatalogResult<Option<i64>> {
        let results: Vec<i64> = self
            .metastore
            .query_helper(
                QueryParams {
                    sql: "SELECT sequence_number FROM cayenne_snapshot_sequence WHERE table_id = ?1 AND snapshot_id = ?2",
                    params: vec![
                        MetastoreValue::Text(table_id.to_string()),
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
        table_id: &str,
    ) -> CatalogResult<HashMap<String, i64>> {
        let results: Vec<(String, i64)> = self
            .metastore
            .query_helper(
                QueryParams {
                    sql: "SELECT snapshot_id, sequence_number FROM cayenne_snapshot_sequence WHERE table_id = ?1",
                    params: vec![MetastoreValue::Text(table_id.to_string())],
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

    async fn clear_snapshot_sequence(
        &self,
        table_id: &str,
        snapshot_id: &str,
    ) -> CatalogResult<()> {
        self.metastore
            .execute_helper(ExecuteParams {
                sql: "DELETE FROM cayenne_snapshot_sequence WHERE table_id = ?1 AND snapshot_id = ?2",
                params: vec![
                    MetastoreValue::Text(table_id.to_string()),
                    MetastoreValue::Text(snapshot_id.to_string()),
                ],
            })
            .await
            .map_err(|e| CatalogError::InvalidOperation {
                message: format!("Failed to clear snapshot sequence for {snapshot_id}"),
                source: Box::new(e),
            })
    }

    async fn commit_compaction(&self, table_id: &str, new_snapshot_id: &str) -> CatalogResult<()> {
        // Execute all operations atomically using a proper transaction.
        //
        // Order matters for crash safety (enforced by `commit_compaction_in_txn`):
        // 1. Clear delete files first - they reference the old snapshot's data
        // 2. Clear insert records - they correspond to the cleared delete files
        // 3. Clear snapshot sequences - protected snapshots are no longer needed
        //    after compaction since all data is merged into the new snapshot
        // 4. Update snapshot pointer - commits the new snapshot as active
        //
        // Devil's advocate (to be really sure): one could worry that clearing the
        // delete files *before* advancing the snapshot pointer opens a window where
        // a concurrent query on the old snapshot would lose its deletion vectors.
        // This is prevented by the `listing_fence` + `protected_snapshots` mechanism
        // (queries that started on the old snapshot hold a protected entry, so the
        // old snapshot directory is not cleaned until they finish, and they captured
        // the delete files at scan start time).
        //
        // If the process crashes anywhere in the batch or before the background
        // cleanup runs, the worst observable state is "old snapshot still current,
        // but its delete files are gone from the catalog". This means any deletions
        // that were pending at compaction time are lost (the rows that should have
        // been deleted are still visible until the next successful compaction),
        // but **no deleted row is ever resurrected after it was once successfully
        // deleted in a prior snapshot**, and no data file is ever lost. This is an
        // acceptable "at-least-once deletion" anomaly for a best-effort compaction
        // system, and is the documented tradeoff.
        //
        // The new snapshot is always written + fsynced *before* this catalog
        // transaction is even attempted, so a crash before the pointer move leaves
        // an orphaned (but harmless) new snapshot directory.
        //
        // The transaction may fail with SQLITE_BUSY/SQLITE_LOCKED conflicts at
        // commit time (especially with Turso's BEGIN CONCURRENT). Retry a few
        // times with backoff.
        let max_attempts = DEFAULT_CONCURRENT_WRITE_MAX_ATTEMPTS;
        if max_attempts == 0 {
            return Err(CatalogError::InvalidOperationNoSource {
                message: "commit_compaction requires at least one attempt".to_string(),
            });
        }

        for attempt in 1..=max_attempts {
            let mut tx = self.begin_transaction().await.map_err(|e| {
                CatalogError::FailedToSetCurrentSnapshot {
                    source: Box::new(e),
                }
            })?;

            match self
                .commit_compaction_in_txn(&mut *tx, table_id, new_snapshot_id)
                .await
            {
                Ok(()) => match tx.commit().await {
                    Ok(()) => return Ok(()),
                    Err(e) if attempt < max_attempts && is_retryable_write_conflict(&e) => {
                        let delay = retry_backoff_delay(attempt);
                        tracing::debug!(
                            attempt,
                            max_attempts,
                            ?delay,
                            "Retrying compaction transaction after commit conflict"
                        );
                        tokio::time::sleep(delay).await;
                    }
                    Err(e) => {
                        return Err(CatalogError::FailedToSetCurrentSnapshot {
                            source: Box::new(e),
                        });
                    }
                },
                Err(e) => {
                    // Transaction auto-rolls-back on drop.
                    return Err(e);
                }
            }
        }

        Err(CatalogError::InvalidOperationNoSource {
            message: format!(
                "commit_compaction exhausted {max_attempts} attempts without success or a terminal error"
            ),
        })
    }

    async fn commit_overwrite(&self, table_id: &str, new_snapshot_id: &str) -> CatalogResult<()> {
        // Same retry-on-conflict shape as commit_compaction; the only
        // additional work happens inside the transaction via
        // commit_overwrite_in_txn below.
        let max_attempts = DEFAULT_CONCURRENT_WRITE_MAX_ATTEMPTS;
        if max_attempts == 0 {
            return Err(CatalogError::InvalidOperationNoSource {
                message: "commit_overwrite requires at least one attempt".to_string(),
            });
        }

        for attempt in 1..=max_attempts {
            let mut tx = self.begin_transaction().await.map_err(|e| {
                CatalogError::FailedToSetCurrentSnapshot {
                    source: Box::new(e),
                }
            })?;

            match self
                .commit_overwrite_in_txn(&mut *tx, table_id, new_snapshot_id)
                .await
            {
                Ok(()) => match tx.commit().await {
                    Ok(()) => return Ok(()),
                    Err(e) if attempt < max_attempts && is_retryable_write_conflict(&e) => {
                        let delay = retry_backoff_delay(attempt);
                        tracing::debug!(
                            attempt,
                            max_attempts,
                            ?delay,
                            "Retrying overwrite transaction after commit conflict"
                        );
                        tokio::time::sleep(delay).await;
                    }
                    Err(e) => {
                        return Err(CatalogError::FailedToSetCurrentSnapshot {
                            source: Box::new(e),
                        });
                    }
                },
                Err(e) => {
                    return Err(e);
                }
            }
        }

        Err(CatalogError::InvalidOperationNoSource {
            message: format!(
                "commit_overwrite exhausted {max_attempts} attempts without success or a terminal error"
            ),
        })
    }

    async fn add_partition(&self, partition: PartitionMetadata) -> CatalogResult<String> {
        // Validate partition metadata invariants before persisting
        // Without this, invalid metadata could cause incorrect partition lookups at query time
        if partition.partition_columns.is_empty() {
            return Err(CatalogError::InvalidPartitionMetadata {
                message: "partition_columns cannot be empty".to_string(),
            });
        }
        if partition.partition_values.is_empty() {
            return Err(CatalogError::InvalidPartitionMetadata {
                message: "partition_values cannot be empty".to_string(),
            });
        }
        if partition.partition_columns.len() != partition.partition_values.len() {
            return Err(CatalogError::InvalidPartitionMetadata {
                message: format!(
                    "partition_columns count ({}) does not match partition_values count ({})",
                    partition.partition_columns.len(),
                    partition.partition_values.len()
                ),
            });
        }

        // Serialize partition columns and values as JSON arrays for storage
        let columns_json = serde_json::to_string(&partition.partition_columns).map_err(|e| {
            CatalogError::Database {
                message: format!("Failed to serialize partition columns: {e}"),
            }
        })?;
        let values_json = serde_json::to_string(&partition.partition_values).map_err(|e| {
            CatalogError::Database {
                message: format!("Failed to serialize partition values: {e}"),
            }
        })?;
        let partition_key = partition.composite_key();

        // Check if partition already exists using the composite key
        let existing_partition = self
            .metastore
            .query_row_helper(
                QueryRowParams {
                    sql: "SELECT partition_id FROM cayenne_partition WHERE table_id = ?1 AND partition_key = ?2",
                    params: vec![
                        MetastoreValue::Text(partition.table_id.clone()),
                        MetastoreValue::Text(partition_key.clone()),
                    ],
                },
                |row| row.get_string(0),
            )
            .await;

        if let Ok(id) = existing_partition {
            // Partition already exists, return its ID
            return Ok(id);
        }

        let partition_id = uuid::Uuid::now_v7().to_string();

        // Insert partition metadata with composite key support
        let insert_result = self
            .metastore
            .execute_helper(ExecuteParams {
                sql: r"
                INSERT INTO cayenne_partition (
                    partition_id, table_id, partition_columns_json, partition_values_json, partition_key, path, path_is_relative, record_count, file_size_bytes
                ) VALUES (
                    ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9
                )",
                params: vec![
                    MetastoreValue::Text(partition_id.clone()),
                    MetastoreValue::Text(partition.table_id.clone()),
                    MetastoreValue::Text(columns_json.clone()),
                    MetastoreValue::Text(values_json.clone()),
                    MetastoreValue::Text(partition_key.clone()),
                    MetastoreValue::Text(partition.path.clone()),
                    MetastoreValue::Bool(partition.path_is_relative),
                    MetastoreValue::Integer(partition.record_count),
                    MetastoreValue::Integer(partition.file_size_bytes),
                ],
            })
            .await;

        match insert_result {
            Ok(()) => return Ok(partition_id),
            Err(CatalogError::ConstraintViolation { message })
                if is_partition_unique_constraint_violation_message(&message) => {}
            Err(e) => {
                return Err(CatalogError::FailedToAddPartition {
                    source: Box::new(e),
                });
            }
        }

        // Another concurrent operation inserted first — retrieve existing partition ID
        let existing_id: String = self
            .metastore
            .query_row_helper(
                QueryRowParams {
                    sql: "SELECT partition_id FROM cayenne_partition WHERE table_id = ?1 AND partition_key = ?2",
                    params: vec![
                        MetastoreValue::Text(partition.table_id),
                        MetastoreValue::Text(partition_key),
                    ],
                },
                |row| row.get_string(0),
            )
            .await
            .map_err(|e| CatalogError::FailedToAddPartition {
                source: Box::new(e),
            })?;

        Ok(existing_id)
    }

    async fn get_partitions(&self, table_id: &str) -> CatalogResult<Vec<PartitionMetadata>> {
        self.metastore
            .query_helper(
                QueryParams {
                    sql: r"
                    SELECT partition_id, table_id, partition_columns_json, partition_values_json, path, path_is_relative, record_count, file_size_bytes
                    FROM cayenne_partition
                    WHERE table_id = ?1
                    ORDER BY partition_id
                ",
                    params: vec![MetastoreValue::Text(table_id.to_string())],
                },
                |row| {
                    let columns_json = row.get_string(2)?;
                    let values_json = row.get_string(3)?;

                    let partition_columns: Vec<String> =
                        serde_json::from_str(&columns_json).map_err(|e| CatalogError::Database {
                            message: format!("Failed to deserialize partition columns: {e}"),
                        })?;
                    let partition_values: Vec<String> =
                        serde_json::from_str(&values_json).map_err(|e| CatalogError::Database {
                            message: format!("Failed to deserialize partition values: {e}"),
                        })?;

                    Ok(PartitionMetadata {
                        partition_id: row.get_string(0)?,
                        table_id: row.get_string(1)?,
                        partition_columns,
                        partition_values,
                        path: row.get_string(4)?,
                        path_is_relative: row.get_bool(5)?,
                        record_count: row.get_i64(6)?,
                        file_size_bytes: row.get_i64(7)?,
                    })
                },
            )
            .await
            .map_err(|e| CatalogError::FailedToGetPartitions {
                source: Box::new(e),
            })
    }

    async fn upsert_table_statistics(&self, stats: &TableStatistics) -> CatalogResult<()> {
        self.metastore
            .execute_helper(ExecuteParams {
                sql: "INSERT OR REPLACE INTO cayenne_table_statistics \
                      (table_id, statistics_blob, num_rows, ndv_sketches) \
                      VALUES (?1, ?2, ?3, ?4)",
                params: vec![
                    MetastoreValue::Text(stats.table_id.clone()),
                    MetastoreValue::Blob(stats.statistics_blob.clone()),
                    MetastoreValue::Integer(stats.num_rows),
                    stats
                        .ndv_sketches
                        .clone()
                        .map_or(MetastoreValue::Null, MetastoreValue::Blob),
                ],
            })
            .await
    }

    async fn get_table_statistics(&self, table_id: &str) -> CatalogResult<Option<TableStatistics>> {
        let results = self
            .metastore
            .query_helper(
                QueryParams {
                    sql: r"
                    SELECT table_id, statistics_blob, num_rows, ndv_sketches
                    FROM cayenne_table_statistics
                    WHERE table_id = ?1
                    ",
                    params: vec![MetastoreValue::Text(table_id.to_string())],
                },
                |row| {
                    Ok(TableStatistics {
                        table_id: row.get_string(0)?,
                        statistics_blob: row.get_blob(1)?,
                        num_rows: row.get_i64(2)?,
                        ndv_sketches: row.get_optional_blob(3)?,
                    })
                },
            )
            .await?;
        Ok(results.into_iter().next())
    }

    async fn clear_table_statistics(&self, table_id: &str) -> CatalogResult<()> {
        self.metastore
            .execute_helper(ExecuteParams {
                sql: "DELETE FROM cayenne_table_statistics WHERE table_id = ?1",
                params: vec![MetastoreValue::Text(table_id.to_string())],
            })
            .await
    }

    async fn upsert_pk_index(
        &self,
        table_id: &str,
        snapshot_id: &str,
        index_blob: &[u8],
    ) -> CatalogResult<()> {
        self.metastore
            .execute_helper(ExecuteParams {
                sql: "INSERT OR REPLACE INTO cayenne_pk_index \
                      (table_id, snapshot_id, index_blob) \
                      VALUES (?1, ?2, ?3)",
                params: vec![
                    MetastoreValue::Text(table_id.to_string()),
                    MetastoreValue::Text(snapshot_id.to_string()),
                    MetastoreValue::Blob(index_blob.to_vec()),
                ],
            })
            .await
    }

    async fn get_pk_index(&self, table_id: &str) -> CatalogResult<Option<(String, Vec<u8>)>> {
        let results = self
            .metastore
            .query_helper(
                QueryParams {
                    sql: r"
                    SELECT snapshot_id, index_blob
                    FROM cayenne_pk_index
                    WHERE table_id = ?1
                    ",
                    params: vec![MetastoreValue::Text(table_id.to_string())],
                },
                |row| Ok((row.get_string(0)?, row.get_blob(1)?)),
            )
            .await?;
        Ok(results.into_iter().next())
    }

    async fn clear_pk_index(&self, table_id: &str) -> CatalogResult<()> {
        self.metastore
            .execute_helper(ExecuteParams {
                sql: "DELETE FROM cayenne_pk_index WHERE table_id = ?1",
                params: vec![MetastoreValue::Text(table_id.to_string())],
            })
            .await
    }

    async fn add_inlined_data(&self, data: InlinedData) -> CatalogResult<String> {
        let inlined_id = if data.inlined_id.is_empty() {
            uuid::Uuid::now_v7().to_string()
        } else {
            data.inlined_id
        };
        self.metastore
            .execute_helper(ExecuteParams {
                sql: r"
                INSERT INTO cayenne_inlined_data
                    (inlined_id, table_id, partition_key, data_ipc, record_count, sequence_number)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                ",
                params: vec![
                    MetastoreValue::Text(inlined_id.clone()),
                    MetastoreValue::Text(data.table_id),
                    data.partition_key.into(),
                    MetastoreValue::Blob(data.data_ipc),
                    MetastoreValue::Integer(data.record_count),
                    MetastoreValue::Integer(data.sequence_number),
                ],
            })
            .await?;
        Ok(inlined_id)
    }

    async fn get_inlined_data(&self, table_id: &str) -> CatalogResult<Vec<InlinedData>> {
        self.metastore
            .query_helper(
                QueryParams {
                    sql: r"
                    SELECT inlined_id, table_id, partition_key, data_ipc, record_count, sequence_number, created_at
                    FROM cayenne_inlined_data
                    WHERE table_id = ?1
                    ORDER BY sequence_number
                    ",
                    params: vec![MetastoreValue::Text(table_id.to_string())],
                },
                |row| {
                    Ok(InlinedData {
                        inlined_id: row.get_string(0)?,
                        table_id: row.get_string(1)?,
                        partition_key: row.get_optional_string(2)?,
                        data_ipc: row.get_blob(3)?,
                        record_count: row.get_i64(4)?,
                        sequence_number: row.get_i64(5)?,
                        created_at: row.get_string(6)?,
                    })
                },
            )
            .await
    }

    async fn get_inlined_data_for_partition(
        &self,
        table_id: &str,
        partition_key: &str,
    ) -> CatalogResult<Vec<InlinedData>> {
        self.metastore
            .query_helper(
                QueryParams {
                    sql: r"
                    SELECT inlined_id, table_id, partition_key, data_ipc, record_count, sequence_number, created_at
                    FROM cayenne_inlined_data
                    WHERE table_id = ?1 AND partition_key = ?2
                    ORDER BY sequence_number
                    ",
                    params: vec![
                        MetastoreValue::Text(table_id.to_string()),
                        MetastoreValue::Text(partition_key.to_string()),
                    ],
                },
                |row| {
                    Ok(InlinedData {
                        inlined_id: row.get_string(0)?,
                        table_id: row.get_string(1)?,
                        partition_key: row.get_optional_string(2)?,
                        data_ipc: row.get_blob(3)?,
                        record_count: row.get_i64(4)?,
                        sequence_number: row.get_i64(5)?,
                        created_at: row.get_string(6)?,
                    })
                },
            )
            .await
    }

    async fn get_inlined_data_count(&self, table_id: &str) -> CatalogResult<i64> {
        self.metastore
            .query_row_helper(
                QueryRowParams {
                    sql: "SELECT COALESCE(SUM(record_count), 0) FROM cayenne_inlined_data WHERE table_id = ?1",
                    params: vec![MetastoreValue::Text(table_id.to_string())],
                },
                |row| row.get_i64(0),
            )
            .await
    }

    async fn get_inlined_data_stats(&self, table_id: &str) -> CatalogResult<InlinedDataStats> {
        self.metastore
            .query_row_helper(
                QueryRowParams {
                    sql: r"
                    SELECT
                        COALESCE(SUM(record_count), 0),
                        COUNT(*),
                        COALESCE(SUM(LENGTH(data_ipc)), 0)
                    FROM cayenne_inlined_data
                    WHERE table_id = ?1
                    ",
                    params: vec![MetastoreValue::Text(table_id.to_string())],
                },
                |row| {
                    Ok(InlinedDataStats {
                        record_count: row.get_i64(0)?,
                        entry_count: row.get_i64(1)?,
                        ipc_bytes: row.get_i64(2)?,
                    })
                },
            )
            .await
    }

    async fn clear_inlined_data(&self, table_id: &str) -> CatalogResult<()> {
        self.metastore
            .execute_helper(ExecuteParams {
                sql: "DELETE FROM cayenne_inlined_data WHERE table_id = ?1",
                params: vec![MetastoreValue::Text(table_id.to_string())],
            })
            .await
    }

    async fn clear_inlined_data_and_deletes(&self, table_id: &str) -> CatalogResult<()> {
        let table_id_literal = sql_text_literal(table_id);
        let batch_sql = format!(
            "DELETE FROM cayenne_inlined_data WHERE table_id = {table_id_literal}; \
             DELETE FROM cayenne_inlined_delete WHERE table_id = {table_id_literal};"
        );

        self.metastore
            .execute_transaction_batch_helper(&batch_sql)
            .await
    }

    async fn add_inlined_delete(&self, delete: InlinedDelete) -> CatalogResult<String> {
        let inlined_id = if delete.inlined_id.is_empty() {
            uuid::Uuid::now_v7().to_string()
        } else {
            delete.inlined_id
        };
        self.metastore
            .execute_helper(ExecuteParams {
                sql: r"
                INSERT INTO cayenne_inlined_delete
                    (inlined_id, table_id, delete_ipc, delete_count, sequence_number)
                VALUES (?1, ?2, ?3, ?4, ?5)
                ",
                params: vec![
                    MetastoreValue::Text(inlined_id.clone()),
                    MetastoreValue::Text(delete.table_id),
                    MetastoreValue::Blob(delete.delete_ipc),
                    MetastoreValue::Integer(delete.delete_count),
                    MetastoreValue::Integer(delete.sequence_number),
                ],
            })
            .await?;
        Ok(inlined_id)
    }

    async fn commit_inlined_mutation(
        &self,
        table_id: &str,
        updated_data: Vec<InlinedData>,
        deleted_inlined_ids: Vec<String>,
        data: Vec<InlinedData>,
    ) -> CatalogResult<()> {
        if updated_data.is_empty() && deleted_inlined_ids.is_empty() && data.is_empty() {
            return Ok(());
        }

        for updated in &updated_data {
            if updated.table_id != table_id {
                return Err(CatalogError::InvalidOperationNoSource {
                    message: format!(
                        "Inline data table_id '{}' does not match commit table_id '{table_id}'",
                        updated.table_id
                    ),
                });
            }
            if updated.inlined_id.is_empty() {
                return Err(CatalogError::InvalidOperationNoSource {
                    message: "Updated inline data rows must include an inlined_id".to_string(),
                });
            }
        }
        for data_entry in &data {
            if data_entry.table_id != table_id {
                return Err(CatalogError::InvalidOperationNoSource {
                    message: format!(
                        "Inline data table_id '{}' does not match commit table_id '{table_id}'",
                        data_entry.table_id
                    ),
                });
            }
        }

        let sequence_increment = i64::from(!data.is_empty());
        let max_attempts = DEFAULT_CONCURRENT_WRITE_MAX_ATTEMPTS;
        if max_attempts == 0 {
            return Err(CatalogError::InvalidOperationNoSource {
                message: "commit_inlined_mutation requires at least one attempt".to_string(),
            });
        }

        for attempt in 1..=max_attempts {
            let tx = self.metastore.begin_transaction().await.map_err(|e| {
                CatalogError::InvalidOperation {
                    message: "Failed to begin inline mutation transaction".to_string(),
                    source: Box::new(e),
                }
            })?;

            if sequence_increment > 0 {
                tx.execute(ExecuteParams {
                    sql: "UPDATE cayenne_table SET current_sequence_number = current_sequence_number + ?1 WHERE table_id = ?2",
                    params: vec![
                        MetastoreValue::Integer(sequence_increment),
                        MetastoreValue::Text(table_id.to_string()),
                    ],
                })
                .await
                .map_err(|e| CatalogError::InvalidOperation {
                    message: "Failed to execute inline mutation transaction".to_string(),
                    source: Box::new(e),
                })?;
            }

            for updated in &updated_data {
                tx.execute(ExecuteParams {
                    sql: r"
                    UPDATE cayenne_inlined_data
                    SET data_ipc = ?1, record_count = ?2
                    WHERE table_id = ?3 AND inlined_id = ?4
                    ",
                    params: vec![
                        MetastoreValue::Blob(updated.data_ipc.clone()),
                        MetastoreValue::Integer(updated.record_count),
                        MetastoreValue::Text(table_id.to_string()),
                        MetastoreValue::Text(updated.inlined_id.clone()),
                    ],
                })
                .await
                .map_err(|e| CatalogError::InvalidOperation {
                    message: "Failed to execute inline mutation transaction".to_string(),
                    source: Box::new(e),
                })?;
            }

            for inlined_id in &deleted_inlined_ids {
                tx.execute(ExecuteParams {
                    sql: "DELETE FROM cayenne_inlined_data WHERE table_id = ?1 AND inlined_id = ?2",
                    params: vec![
                        MetastoreValue::Text(table_id.to_string()),
                        MetastoreValue::Text(inlined_id.clone()),
                    ],
                })
                .await
                .map_err(|e| CatalogError::InvalidOperation {
                    message: "Failed to execute inline mutation transaction".to_string(),
                    source: Box::new(e),
                })?;
            }

            for data_entry in &data {
                let inlined_id = if data_entry.inlined_id.is_empty() {
                    uuid::Uuid::now_v7().to_string()
                } else {
                    data_entry.inlined_id.clone()
                };
                tx.execute(ExecuteParams {
                    sql: r"
                    INSERT INTO cayenne_inlined_data
                        (inlined_id, table_id, partition_key, data_ipc, record_count, sequence_number)
                    VALUES (?1, ?2, ?3, ?4, ?5, (SELECT current_sequence_number FROM cayenne_table WHERE table_id = ?2))
                    ",
                    params: vec![
                        MetastoreValue::Text(inlined_id),
                        MetastoreValue::Text(table_id.to_string()),
                        data_entry.partition_key.clone().into(),
                        MetastoreValue::Blob(data_entry.data_ipc.clone()),
                        MetastoreValue::Integer(data_entry.record_count),
                    ],
                })
                .await
                .map_err(|e| CatalogError::InvalidOperation {
                    message: "Failed to execute inline mutation transaction".to_string(),
                    source: Box::new(e),
                })?;
            }

            match tx.commit().await {
                Ok(()) => return Ok(()),
                Err(e) if attempt < max_attempts && is_retryable_write_conflict(&e) => {
                    let delay = retry_backoff_delay(attempt);
                    tracing::debug!(
                        attempt,
                        max_attempts,
                        ?delay,
                        "Retrying inline mutation transaction after commit conflict"
                    );
                    tokio::time::sleep(delay).await;
                }
                Err(e) => {
                    return Err(CatalogError::InvalidOperation {
                        message: "Failed to commit inline mutation transaction".to_string(),
                        source: Box::new(e),
                    });
                }
            }
        }

        Err(CatalogError::InvalidOperationNoSource {
            message: format!(
                "commit_inlined_mutation exhausted {max_attempts} attempts without success or a terminal error"
            ),
        })
    }

    async fn commit_on_conflict_deletions(
        &self,
        delete_files: Vec<DeleteFile>,
        table_id: &str,
        insert_pk_bytes_list: Vec<Vec<u8>>,
        insert_sequence: i64,
        snapshot_sequence: Option<SnapshotSequenceCommit>,
    ) -> CatalogResult<()> {
        // SQLite param limit chunking (mirrors add_insert_records_batch).
        const PARAMS_PER_ROW: usize = 4;
        const MAX_PARAMS: usize = 32_000;
        const MAX_ROWS_PER_CHUNK: usize = MAX_PARAMS / PARAMS_PER_ROW;

        // Delete-file rows use 9 params each; keep the same budget.
        const DELETE_FILE_PARAMS_PER_ROW: usize = 9;
        const MAX_DELETE_FILE_ROWS_PER_CHUNK: usize = MAX_PARAMS / DELETE_FILE_PARAMS_PER_ROW;

        // Atomic replacement for the legacy `add_delete_file × N` +
        // `add_insert_records_batch` sequence in `apply_on_conflict_deletions`.
        // See `crates/cayenne/benches/apply_on_conflict_rpc_ceiling.rs` for the
        // before-numbers and the atomicity tradeoff.
        // The caller now uses `reserve_sequence_numbers(2)` (one round-trip for
        // the delete+insert pair) before entering this transaction; the txn
        // itself only does the durable catalog writes for the DeleteFiles and
        // InsertRecords.
        if delete_files.is_empty() && insert_pk_bytes_list.is_empty() && snapshot_sequence.is_none()
        {
            return Ok(());
        }

        // Validate every delete_file belongs to this table_id up front so a
        // mismatch can't half-apply via the txn. Duplicate path metadata is
        // checked by the INSERT/ON CONFLICT guard inside the transaction and
        // re-read only on error to produce the descriptive validation message.
        for delete_file in &delete_files {
            if delete_file.table_id != table_id {
                return Err(CatalogError::InvalidOperationNoSource {
                    message: format!(
                        "Delete-file table_id '{}' does not match commit table_id '{table_id}'",
                        delete_file.table_id
                    ),
                });
            }
            if !insert_pk_bytes_list.is_empty() && insert_sequence <= delete_file.sequence_number {
                return Err(CatalogError::InvalidOperationNoSource {
                    message: format!(
                        "Insert sequence {insert_sequence} must be greater than delete-file sequence {} for on-conflict replacement rows",
                        delete_file.sequence_number
                    ),
                });
            }
        }

        let max_attempts = DEFAULT_CONCURRENT_WRITE_MAX_ATTEMPTS;

        'attempts: for attempt in 1..=max_attempts {
            let tx = match self.metastore.begin_transaction().await {
                Ok(tx) => tx,
                Err(e) => {
                    if retry_on_metastore_write_conflict(
                        &e,
                        attempt,
                        max_attempts,
                        "begin on-conflict deletion transaction",
                    )
                    .await
                    {
                        continue 'attempts;
                    }
                    return Err(CatalogError::InvalidOperation {
                        message: "Failed to begin on-conflict deletion transaction".to_string(),
                        source: Box::new(e),
                    });
                }
            };

            // INSERT delete_file rows in batched multi-VALUES chunks. The
            // per-row `ON CONFLICT(table_id, path) DO UPDATE SET path = CASE
            // ... END` clause keeps each row's idempotency check scoped to its
            // own `excluded` values, identical to the previous one-INSERT-per-
            // row form. A duplicate `(table_id, path)` whose metadata does not
            // match the existing row trips the NOT NULL guard on `path`; on
            // that error path we fall back to per-row INSERTs inside the same
            // txn to pinpoint the offending delete file for the descriptive
            // validation error.
            for chunk in delete_files.chunks(MAX_DELETE_FILE_ROWS_PER_CHUNK) {
                let (sql, params) = Self::build_insert_delete_files_chunk_sql(chunk);
                let res = tx.execute(ExecuteParams { sql: &sql, params }).await;
                if let Err(e) = res {
                    if should_retry_metastore_write_conflict(&e, attempt, max_attempts) {
                        drop(tx);
                        sleep_before_metastore_write_retry(
                            attempt,
                            max_attempts,
                            "insert delete file chunk inside on-conflict transaction",
                        )
                        .await;
                        continue 'attempts;
                    }

                    for delete_file in chunk {
                        let (sql, params) = Self::build_insert_delete_files_chunk_sql(
                            std::slice::from_ref(delete_file),
                        );
                        let res = tx.execute(ExecuteParams { sql: &sql, params }).await;
                        if let Err(e) = res {
                            if should_retry_metastore_write_conflict(&e, attempt, max_attempts) {
                                drop(tx);
                                sleep_before_metastore_write_retry(
                                    attempt,
                                    max_attempts,
                                    "insert delete file inside on-conflict transaction",
                                )
                                .await;
                                continue 'attempts;
                            }
                            let validation_result =
                                Self::validate_existing_delete_file_if_present_in_transaction(
                                    tx.as_ref(),
                                    delete_file,
                                )
                                .await;
                            drop(tx);
                            if let Err(validation_error) = validation_result {
                                return Err(CatalogError::InvalidOperation {
                                    message:
                                        "Delete-file metadata conflicts with an existing row inside on-conflict transaction"
                                            .to_string(),
                                    source: Box::new(validation_error),
                                });
                            }
                            return Err(CatalogError::InvalidOperation {
                                message:
                                    "Failed to insert delete file inside on-conflict transaction"
                                        .to_string(),
                                source: Box::new(e),
                            });
                        }
                    }
                }
            }

            // Chunked INSERTs for the insert_record rows.
            for chunk in insert_pk_bytes_list.chunks(MAX_ROWS_PER_CHUNK) {
                let (sql, params) =
                    Self::build_insert_records_chunk_sql(table_id, chunk, insert_sequence);
                if let Err(e) = tx.execute(ExecuteParams { sql: &sql, params }).await {
                    if should_retry_metastore_write_conflict(&e, attempt, max_attempts) {
                        drop(tx);
                        sleep_before_metastore_write_retry(
                            attempt,
                            max_attempts,
                            "insert insert-record chunk inside on-conflict transaction",
                        )
                        .await;
                        continue 'attempts;
                    }
                    drop(tx);
                    return Err(CatalogError::InvalidOperation {
                        message:
                            "Failed to insert insert-record chunk inside on-conflict transaction"
                                .to_string(),
                        source: Box::new(e),
                    });
                }
            }

            if let Some(snapshot_sequence) = &snapshot_sequence {
                if let Err(e) = tx
                    .execute(ExecuteParams {
                        sql: "INSERT OR REPLACE INTO cayenne_snapshot_sequence (table_id, snapshot_id, sequence_number) VALUES (?1, ?2, ?3)",
                        params: vec![
                            MetastoreValue::Text(table_id.to_string()),
                            MetastoreValue::Text(snapshot_sequence.snapshot_id.clone()),
                            MetastoreValue::Integer(snapshot_sequence.sequence_number),
                        ],
                    })
                    .await
                {
                    if should_retry_metastore_write_conflict(&e, attempt, max_attempts) {
                        drop(tx);
                        sleep_before_metastore_write_retry(
                            attempt,
                            max_attempts,
                            "insert snapshot sequence inside on-conflict transaction",
                        )
                        .await;
                        continue 'attempts;
                    }
                    drop(tx);
                    return Err(CatalogError::InvalidOperation {
                        message:
                            "Failed to insert snapshot sequence inside on-conflict transaction"
                                .to_string(),
                        source: Box::new(e),
                    });
                }
            }

            match tx.commit().await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    if retry_on_metastore_write_conflict(
                        &e,
                        attempt,
                        max_attempts,
                        "commit on-conflict deletion transaction",
                    )
                    .await
                    {
                        continue 'attempts;
                    }
                    return Err(CatalogError::InvalidOperation {
                        message: "Failed to commit on-conflict deletion transaction".to_string(),
                        source: Box::new(e),
                    });
                }
            }
        }

        Err(CatalogError::InvalidOperationNoSource {
            message: format!(
                "commit_on_conflict_deletions exhausted {max_attempts} retry attempts after retryable write conflicts"
            ),
        })
    }

    async fn get_inlined_deletes(&self, table_id: &str) -> CatalogResult<Vec<InlinedDelete>> {
        self.metastore
            .query_helper(
                QueryParams {
                    sql: r"
                    SELECT inlined_id, table_id, delete_ipc, delete_count, sequence_number, created_at
                    FROM cayenne_inlined_delete
                    WHERE table_id = ?1
                    ORDER BY sequence_number
                    ",
                    params: vec![MetastoreValue::Text(table_id.to_string())],
                },
                |row| {
                    Ok(InlinedDelete {
                        inlined_id: row.get_string(0)?,
                        table_id: row.get_string(1)?,
                        delete_ipc: row.get_blob(2)?,
                        delete_count: row.get_i64(3)?,
                        sequence_number: row.get_i64(4)?,
                        created_at: row.get_string(5)?,
                    })
                },
            )
            .await
    }

    async fn clear_inlined_deletes(&self, table_id: &str) -> CatalogResult<()> {
        self.metastore
            .execute_helper(ExecuteParams {
                sql: "DELETE FROM cayenne_inlined_delete WHERE table_id = ?1",
                params: vec![MetastoreValue::Text(table_id.to_string())],
            })
            .await
    }

    async fn drop_table(&self, table_name: &str) -> CatalogResult<bool> {
        // First check if the table exists and get its ID
        let table_id: Option<String> = self
            .metastore
            .query_row_helper(
                QueryRowParams {
                    sql: "SELECT table_id FROM cayenne_table WHERE table_name = ?1",
                    params: vec![MetastoreValue::Text(table_name.to_string())],
                },
                |row| row.get_string(0),
            )
            .await
            .ok();

        let Some(table_id) = table_id else {
            return Ok(false); // Table doesn't exist
        };

        // Delete all related metadata in order (respect foreign key constraints)
        // 1. Delete insert records
        self.metastore
            .execute_helper(ExecuteParams {
                sql: "DELETE FROM cayenne_insert_record WHERE table_id = ?1",
                params: vec![MetastoreValue::Text(table_id.clone())],
            })
            .await
            .map_err(|e| CatalogError::InvalidOperation {
                message: "Failed to delete insert records.".to_string(),
                source: Box::new(e),
            })?;

        // 2. Delete snapshot sequences
        self.metastore
            .execute_helper(ExecuteParams {
                sql: "DELETE FROM cayenne_snapshot_sequence WHERE table_id = ?1",
                params: vec![MetastoreValue::Text(table_id.clone())],
            })
            .await
            .map_err(|e| CatalogError::InvalidOperation {
                message: "Failed to delete snapshot sequences.".to_string(),
                source: Box::new(e),
            })?;

        // 3. Delete delete files
        self.metastore
            .execute_helper(ExecuteParams {
                sql: "DELETE FROM cayenne_delete_file WHERE table_id = ?1",
                params: vec![MetastoreValue::Text(table_id.clone())],
            })
            .await
            .map_err(|e| CatalogError::InvalidOperation {
                message: "Failed to delete delete files.".to_string(),
                source: Box::new(e),
            })?;

        // 4. Delete partitions
        self.metastore
            .execute_helper(ExecuteParams {
                sql: "DELETE FROM cayenne_partition WHERE table_id = ?1",
                params: vec![MetastoreValue::Text(table_id.clone())],
            })
            .await
            .map_err(|e| CatalogError::InvalidOperation {
                message: "Failed to delete partitions.".to_string(),
                source: Box::new(e),
            })?;

        // 5. Delete table statistics
        self.metastore
            .execute_helper(ExecuteParams {
                sql: "DELETE FROM cayenne_table_statistics WHERE table_id = ?1",
                params: vec![MetastoreValue::Text(table_id.clone())],
            })
            .await
            .map_err(|e| CatalogError::InvalidOperation {
                message: "Failed to delete table statistics.".to_string(),
                source: Box::new(e),
            })?;

        // 6. Delete inlined data
        self.metastore
            .execute_helper(ExecuteParams {
                sql: "DELETE FROM cayenne_inlined_data WHERE table_id = ?1",
                params: vec![MetastoreValue::Text(table_id.clone())],
            })
            .await
            .map_err(|e| CatalogError::InvalidOperation {
                message: "Failed to delete inlined data.".to_string(),
                source: Box::new(e),
            })?;

        // 7. Delete inlined deletes
        self.metastore
            .execute_helper(ExecuteParams {
                sql: "DELETE FROM cayenne_inlined_delete WHERE table_id = ?1",
                params: vec![MetastoreValue::Text(table_id.clone())],
            })
            .await
            .map_err(|e| CatalogError::InvalidOperation {
                message: "Failed to delete inlined deletes.".to_string(),
                source: Box::new(e),
            })?;

        // 8. Finally delete the table itself
        self.metastore
            .execute_helper(ExecuteParams {
                sql: "DELETE FROM cayenne_table WHERE table_id = ?1",
                params: vec![MetastoreValue::Text(table_id)],
            })
            .await
            .map_err(|e| CatalogError::InvalidOperation {
                message: "Failed to delete table.".to_string(),
                source: Box::new(e),
            })?;

        Ok(true)
    }

    async fn export_dataset_slice(
        &self,
        dataset_name: &str,
        data_dir_anchor: &std::path::Path,
    ) -> CatalogResult<crate::metastore::snapshot::DatasetMetastoreSlice> {
        match &self.metastore {
            MetastoreImpl::Sqlite(m) => {
                crate::metastore::snapshot::export_dataset(m, dataset_name, data_dir_anchor).await
            }
            #[cfg(feature = "turso")]
            MetastoreImpl::Turso(m) => {
                crate::metastore::snapshot::export_dataset(m, dataset_name, data_dir_anchor).await
            }
        }
    }

    async fn import_dataset_slice(
        &self,
        slice: &crate::metastore::snapshot::DatasetMetastoreSlice,
        data_dir_anchor: &std::path::Path,
    ) -> CatalogResult<()> {
        match &self.metastore {
            MetastoreImpl::Sqlite(m) => {
                crate::metastore::snapshot::import_dataset(m, slice, data_dir_anchor).await
            }
            #[cfg(feature = "turso")]
            MetastoreImpl::Turso(m) => {
                crate::metastore::snapshot::import_dataset(m, slice, data_dir_anchor).await
            }
        }
    }
}

/// Returns `true` if the given catalog error looks like a transient write
/// conflict (`SQLITE_BUSY`, `SQLITE_LOCKED`, or the equivalent Turso
/// `BEGIN CONCURRENT` write-conflict at commit time).
///
/// Used by `commit_compaction` / `commit_compaction_in_txn` to drive their
/// internal retry loops, and by the cross-partition coordinator
/// (`CayennePartitionedInsertStrategy`, issue #10125) to retry batched
/// transactions on transient failures.
#[must_use]
pub fn is_retryable_write_conflict(error: &CatalogError) -> bool {
    match error {
        CatalogError::Database { message } => is_retryable_write_conflict_message(message),
        CatalogError::InvalidOperation { source, .. } => {
            source
                .downcast_ref::<CatalogError>()
                .is_some_and(is_retryable_write_conflict)
                || source
                    .downcast_ref::<rusqlite::Error>()
                    .is_some_and(is_retryable_sqlite_error)
        }
        CatalogError::Sqlite { source } => is_retryable_sqlite_error(source),
        _ => false,
    }
}

fn is_retryable_sqlite_error(error: &rusqlite::Error) -> bool {
    matches!(
        error,
        rusqlite::Error::SqliteFailure(err, _)
            if matches!(
                err.code,
                rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked
            )
    )
}

fn is_query_returned_no_rows(error: &CatalogError) -> bool {
    match error {
        CatalogError::Database { message } => message.contains("Query returned no rows"),
        CatalogError::InvalidOperation { source, .. } => source
            .downcast_ref::<CatalogError>()
            .is_some_and(is_query_returned_no_rows),
        CatalogError::Sqlite {
            source: rusqlite::Error::QueryReturnedNoRows,
        } => true,
        _ => false,
    }
}

async fn retry_on_metastore_write_conflict(
    error: &CatalogError,
    attempt: u32,
    max_attempts: u32,
    operation: &'static str,
) -> bool {
    if !should_retry_metastore_write_conflict(error, attempt, max_attempts) {
        return false;
    }

    sleep_before_metastore_write_retry(attempt, max_attempts, operation).await;
    true
}

fn should_retry_metastore_write_conflict(
    error: &CatalogError,
    attempt: u32,
    max_attempts: u32,
) -> bool {
    attempt < max_attempts && is_retryable_write_conflict(error)
}

async fn sleep_before_metastore_write_retry(
    attempt: u32,
    max_attempts: u32,
    operation: &'static str,
) {
    let delay = retry_backoff_delay(attempt);
    tracing::debug!(
        attempt,
        max_attempts,
        ?delay,
        operation,
        "Retrying metastore transaction after retryable write conflict"
    );
    tokio::time::sleep(delay).await;
}

fn validate_existing_delete_file_record(
    incoming: &DeleteFile,
    existing: &ExistingDeleteFileRecord,
) -> CatalogResult<()> {
    let mut mismatched_fields = Vec::new();

    if existing.path_is_relative != incoming.path_is_relative {
        mismatched_fields.push("path_is_relative");
    }
    if existing.format != incoming.format {
        mismatched_fields.push("format");
    }
    if existing.delete_count != incoming.delete_count {
        mismatched_fields.push("delete_count");
    }
    if existing.file_size_bytes != incoming.file_size_bytes {
        mismatched_fields.push("file_size_bytes");
    }
    if existing.source_data_file_path != incoming.source_data_file_path {
        mismatched_fields.push("source_data_file_path");
    }
    if existing.sequence_number != incoming.sequence_number {
        mismatched_fields.push("sequence_number");
    }

    if mismatched_fields.is_empty() {
        return Ok(());
    }

    Err(CatalogError::ConstraintViolation {
        message: format!(
            "Delete file path '{}' for table '{}' already exists as '{}' with conflicting metadata in fields: {}",
            incoming.path,
            incoming.table_id,
            existing.delete_file_id,
            mismatched_fields.join(", ")
        ),
    })
}

fn is_delete_file_unique_constraint_violation_message(message: &str) -> bool {
    constraint_violation_message_contains_all(
        message,
        &["unique", "cayenne_delete_file", "table_id", "path"],
    ) || constraint_violation_message_contains_all(message, &["idx_cayenne_delete_file_table_path"])
}

fn is_partition_unique_constraint_violation_message(message: &str) -> bool {
    constraint_violation_message_contains_all(
        message,
        &["unique", "cayenne_partition", "table_id", "partition_key"],
    )
}

fn constraint_violation_message_contains_all(message: &str, required_parts: &[&str]) -> bool {
    let normalized = message.to_ascii_lowercase();
    required_parts.iter().all(|part| normalized.contains(part))
}

fn sql_text_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

async fn ensure_snapshot_directory_exists(table: &TableMetadata) -> CatalogResult<()> {
    if table.path.starts_with("s3://") {
        return Ok(());
    }

    let table_root = std::path::PathBuf::from(&table.path).join(&table.table_id);
    let snapshot_dir = table_root.join(&table.current_snapshot_id);

    match tokio::fs::metadata(&snapshot_dir).await {
        Ok(metadata) if metadata.is_dir() => return Ok(()),
        Ok(_) => {
            return Err(CatalogError::Io {
                source: std::io::Error::new(
                    std::io::ErrorKind::AlreadyExists,
                    format!(
                        "snapshot path '{}' exists but is not a directory",
                        snapshot_dir.display()
                    ),
                ),
            });
        }
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => {}
        Err(source) => return Err(CatalogError::Io { source }),
    }

    tokio::fs::create_dir_all(&snapshot_dir)
        .await
        .map_err(|source| CatalogError::Io { source })?;

    // Sync parent (table root) for the same durability reason as the
    // initial creation path above and all other new subdir creations.
    let table_root_for_sync = table_root;
    let _ = tokio::task::spawn_blocking(move || {
        let _ = std::fs::File::open(&table_root_for_sync).and_then(|f| f.sync_all());
    })
    .await;

    Ok(())
}

/// Checks if the existing stored configuration matches the new [`CreateTableOptions`].
///
/// Returns `true` if the configuration matches (no recreation needed).
/// Only compares data-affecting fields; runtime tuning parameters like cache sizes
/// and write/upload concurrency are excluded since they don't affect data correctness.
fn configuration_matches(stored: &TableMetadata, options: &CreateTableOptions) -> bool {
    // Compare primary keys
    if stored.primary_key != options.primary_key {
        return false;
    }

    // Compare on-conflict behavior via string representation
    let stored_oc = stored.on_conflict.as_ref().map(ToString::to_string);
    let new_oc = options.on_conflict.as_ref().map(ToString::to_string);
    if stored_oc != new_oc {
        return false;
    }

    // Compare partition column
    if stored.partition_column != options.partition_column {
        return false;
    }

    // Compare Arrow schema
    if stored.schema.as_ref() != options.schema.as_ref() {
        return false;
    }

    // Compare data-affecting Vortex config fields
    if stored.vortex_config.sort_columns != options.vortex_config.sort_columns {
        return false;
    }
    if stored.vortex_config.compression_strategy != options.vortex_config.compression_strategy {
        return false;
    }

    // Compare base path (path change means data is in a different location)
    if stored.path != options.base_path {
        return false;
    }

    true
}

fn validate_create_table_options(options: &CreateTableOptions) -> CatalogResult<()> {
    if matches!(
        options.vortex_config.pk_conflict_detection,
        PkConflictDetection::None
    ) && matches!(options.on_conflict, Some(OnConflict::Upsert(_)))
    {
        return Err(CatalogError::InvalidOperationNoSource {
            message: format!(
                "cayenne_pk_conflict_detection=none cannot be combined with on_conflict=upsert on table {}: upsert requires conflict detection. Either remove on_conflict or set pk_conflict_detection=auto.",
                options.table_name
            ),
        });
    }

    Ok(())
}

fn log_runtime_footer_cache_drift(
    table_name: &str,
    stored: &TableMetadata,
    options: &CreateTableOptions,
) {
    if let (Some(stored_footer_cache_mb), Some(configured_footer_cache_mb)) = (
        stored.vortex_config.footer_cache_mb,
        options.vortex_config.footer_cache_mb,
    ) && stored_footer_cache_mb != configured_footer_cache_mb
    {
        tracing::warn!(
            table = table_name,
            stored_footer_cache_mb,
            configured_footer_cache_mb,
            "Cayenne table was registered with a different runtime.params.cayenne_footer_cache_mb than the value stored in the metastore; using the current runtime value"
        );
    }
}

/// Logs a warning describing exactly which configuration fields differ between the
/// stored table metadata and the newly requested [`CreateTableOptions`].
///
/// Called when [`validate_existing_table_configuration`] detects a mismatch so the
/// user can see *what* changed and how to resolve it.
fn log_configuration_differences(
    table_name: &str,
    stored: &TableMetadata,
    options: &CreateTableOptions,
) {
    let mut differences = Vec::new();

    if stored.primary_key != options.primary_key {
        differences.push(format!(
            "primary_key: {:?} -> {:?}",
            stored.primary_key, options.primary_key
        ));
    }

    let stored_oc = stored.on_conflict.as_ref().map(ToString::to_string);
    let new_oc = options.on_conflict.as_ref().map(ToString::to_string);
    if stored_oc != new_oc {
        differences.push(format!(
            "on_conflict: {} -> {}",
            stored_oc.as_deref().unwrap_or("none"),
            new_oc.as_deref().unwrap_or("none"),
        ));
    }

    if stored.partition_column != options.partition_column {
        differences.push(format!(
            "partition_column: {:?} -> {:?}",
            stored.partition_column, options.partition_column
        ));
    }

    if stored.schema.as_ref() != options.schema.as_ref() {
        differences.push("schema: <changed>".to_string());
    }

    if stored.vortex_config.sort_columns != options.vortex_config.sort_columns {
        differences.push(format!(
            "sort_columns: {:?} -> {:?}",
            stored.vortex_config.sort_columns, options.vortex_config.sort_columns
        ));
    }

    if stored.vortex_config.compression_strategy != options.vortex_config.compression_strategy {
        differences.push(format!(
            "compression_strategy: {:?} -> {:?}",
            stored.vortex_config.compression_strategy, options.vortex_config.compression_strategy
        ));
    }

    if stored.path != options.base_path {
        differences.push(format!(
            "base_path: {:?} -> {:?}",
            stored.path, options.base_path
        ));
    }

    tracing::warn!(
        table = table_name,
        "Configuration for table '{table_name}' has changed but the existing acceleration was not recreated. \
         Changed fields: [{}]. \
         The acceleration will continue using the previously stored configuration. \
         To apply the new configuration, delete the existing acceleration and restart.",
        differences.join(", ")
    );
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
                    on_conflict: None,
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
            on_conflict: None,
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
            let table_id = table_id.clone();

            let handle = tokio::spawn(async move {
                let partition = PartitionMetadata {
                    partition_id: String::new(), // Will be assigned by catalog
                    table_id,
                    partition_columns: vec!["date".to_string()],
                    partition_values: vec!["2024-01-01".to_string()],
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
            .get_partitions(&table_id)
            .await
            .expect("Failed to get partitions");

        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0].partition_id, partition_ids[0]);
        assert_eq!(partitions[0].partition_values, vec!["2024-01-01"]);

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

        // Create a table via the catalog API to get a valid table_id
        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "id",
            arrow_schema::DataType::Int64,
            false,
        )]));
        let table_options = CreateTableOptions {
            table_name: "test_table".to_string(),
            schema,
            primary_key: vec![],
            on_conflict: None,
            base_path: "/tmp/cayenne_test".to_string(),
            partition_column: None,
            vortex_config: crate::metadata::VortexConfig::default(),
        };
        let table_id = catalog
            .create_table(table_options)
            .await
            .expect("Failed to create table");

        // Spawn multiple tasks that all try to create delete files concurrently
        let mut handles = vec![];
        for i in 0..10 {
            let catalog_clone = Arc::clone(&catalog);
            let table_id = table_id.clone();

            let handle = tokio::spawn(async move {
                let delete_file = DeleteFile {
                    delete_file_id: String::new(), // Will be assigned by catalog
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
            .get_table_delete_files(&table_id)
            .await
            .expect("Failed to get delete files");

        assert_eq!(delete_files.len(), 10);

        // Cleanup test database
        let db_path = test_db.strip_prefix("sqlite://").unwrap_or(&test_db);
        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(format!("{db_path}-shm"));
        let _ = std::fs::remove_file(format!("{db_path}-wal"));
    }

    #[tokio::test]
    async fn test_concurrent_delete_file_creation_is_idempotent_for_same_path() {
        let test_db = format!(
            "sqlite://./.test_concurrent_delete_file_same_path_{}.db",
            uuid::Uuid::now_v7()
        );
        let catalog = Arc::new(CayenneCatalog::new(&test_db).expect("Failed to create catalog"));

        catalog.init().await.expect("Failed to initialize catalog");

        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "id",
            arrow_schema::DataType::Int64,
            false,
        )]));
        let table_options = CreateTableOptions {
            table_name: "test_table_same_path".to_string(),
            schema,
            primary_key: vec![],
            on_conflict: None,
            base_path: "/tmp/cayenne_test".to_string(),
            partition_column: None,
            vortex_config: crate::metadata::VortexConfig::default(),
        };
        let table_id = catalog
            .create_table(table_options)
            .await
            .expect("Failed to create table");

        let mut handles = vec![];
        for _ in 0..10 {
            let catalog_clone = Arc::clone(&catalog);
            let table_id = table_id.clone();

            let handle = tokio::spawn(async move {
                let delete_file = DeleteFile {
                    delete_file_id: String::new(),
                    table_id,
                    source_data_file_path: None,
                    path: "/tmp/delete_file_same_path.parquet".to_string(),
                    path_is_relative: false,
                    format: "parquet".to_string(),
                    delete_count: 10,
                    file_size_bytes: 512,
                    deletion_type: DeletionType::default(),
                    sequence_number: 1,
                };

                catalog_clone.add_delete_file(delete_file).await
            });

            handles.push(handle);
        }

        let results: Vec<_> = futures::future::join_all(handles).await;

        let mut delete_file_ids = vec![];
        for result in results {
            let delete_file_id = result
                .expect("Task panicked")
                .expect("add_delete_file failed");
            delete_file_ids.push(delete_file_id);
        }

        let unique_ids: std::collections::HashSet<_> = delete_file_ids.iter().collect();
        assert_eq!(
            unique_ids.len(),
            1,
            "All concurrent add_delete_file calls for the same path should return the same delete_file_id"
        );

        let delete_files = catalog
            .get_table_delete_files(&table_id)
            .await
            .expect("Failed to get delete files");

        assert_eq!(delete_files.len(), 1);
        assert_eq!(delete_files[0].path, "/tmp/delete_file_same_path.parquet");

        let db_path = test_db.strip_prefix("sqlite://").unwrap_or(&test_db);
        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(format!("{db_path}-shm"));
        let _ = std::fs::remove_file(format!("{db_path}-wal"));
    }

    #[tokio::test]
    async fn test_same_delete_file_path_rejects_conflicting_metadata() {
        let test_db = format!(
            "sqlite://./.test_conflicting_delete_file_same_path_{}.db",
            uuid::Uuid::now_v7()
        );
        let catalog = Arc::new(CayenneCatalog::new(&test_db).expect("Failed to create catalog"));

        catalog.init().await.expect("Failed to initialize catalog");

        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "id",
            arrow_schema::DataType::Int64,
            false,
        )]));
        let table_options = CreateTableOptions {
            table_name: "test_table_conflicting_same_path".to_string(),
            schema,
            primary_key: vec![],
            on_conflict: None,
            base_path: "/tmp/cayenne_test".to_string(),
            partition_column: None,
            vortex_config: crate::metadata::VortexConfig::default(),
        };
        let table_id = catalog
            .create_table(table_options)
            .await
            .expect("Failed to create table");

        let delete_file = DeleteFile {
            delete_file_id: String::new(),
            table_id: table_id.clone(),
            source_data_file_path: Some("/tmp/source.parquet".to_string()),
            path: "/tmp/delete_file_same_path_conflict.parquet".to_string(),
            path_is_relative: false,
            format: "parquet".to_string(),
            delete_count: 10,
            file_size_bytes: 512,
            deletion_type: DeletionType::default(),
            sequence_number: 1,
        };

        let first_id = catalog
            .add_delete_file(delete_file.clone())
            .await
            .expect("initial add_delete_file should succeed");

        let mut conflicting_delete_file = delete_file;
        conflicting_delete_file.file_size_bytes = 1024;

        let err = catalog
            .add_delete_file(conflicting_delete_file)
            .await
            .expect_err("conflicting same-path metadata should be rejected");

        match err {
            CatalogError::FailedToAddDeleteFile { source } => match *source {
                CatalogError::ConstraintViolation { message } => {
                    assert!(
                        message.contains("file_size_bytes"),
                        "expected file_size_bytes mismatch in error, got: {message}"
                    );
                }
                other => panic!("expected nested ConstraintViolation, got: {other}"),
            },
            other => panic!("expected FailedToAddDeleteFile, got: {other}"),
        }

        let delete_files = catalog
            .get_table_delete_files(&table_id)
            .await
            .expect("Failed to get delete files");

        assert_eq!(delete_files.len(), 1);
        assert_eq!(delete_files[0].delete_file_id, first_id);
        assert_eq!(delete_files[0].file_size_bytes, 512);

        let db_path = test_db.strip_prefix("sqlite://").unwrap_or(&test_db);
        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(format!("{db_path}-shm"));
        let _ = std::fs::remove_file(format!("{db_path}-wal"));
    }

    #[tokio::test]
    async fn test_commit_on_conflict_deletions_is_idempotent_for_same_delete_file() {
        let test_db = format!(
            "sqlite://./.test_on_conflict_delete_file_idempotent_{}.db",
            uuid::Uuid::now_v7()
        );
        let catalog = CayenneCatalog::new(&test_db).expect("Failed to create catalog");

        catalog.init().await.expect("Failed to initialize catalog");

        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "id",
            arrow_schema::DataType::Int64,
            false,
        )]));
        let table_options = CreateTableOptions {
            table_name: "test_table_on_conflict_same_path".to_string(),
            schema,
            primary_key: vec![],
            on_conflict: None,
            base_path: "/tmp/cayenne_test".to_string(),
            partition_column: None,
            vortex_config: crate::metadata::VortexConfig::default(),
        };
        let table_id = catalog
            .create_table(table_options)
            .await
            .expect("Failed to create table");

        let delete_file = DeleteFile {
            delete_file_id: String::new(),
            table_id: table_id.clone(),
            source_data_file_path: Some("/tmp/source.parquet".to_string()),
            path: "/tmp/on_conflict_delete_file_same_path.parquet".to_string(),
            path_is_relative: false,
            format: "parquet".to_string(),
            delete_count: 10,
            file_size_bytes: 512,
            deletion_type: DeletionType::default(),
            sequence_number: 1,
        };

        catalog
            .commit_on_conflict_deletions(
                vec![delete_file.clone()],
                &table_id,
                vec![vec![1_u8]],
                2,
                None,
            )
            .await
            .expect("initial on-conflict deletion commit should succeed");
        catalog
            .commit_on_conflict_deletions(vec![delete_file], &table_id, vec![vec![1_u8]], 2, None)
            .await
            .expect("replayed on-conflict deletion commit should be idempotent");

        let delete_files = catalog
            .get_table_delete_files(&table_id)
            .await
            .expect("Failed to get delete files");
        assert_eq!(delete_files.len(), 1);
        assert_eq!(delete_files[0].file_size_bytes, 512);

        let insert_records = catalog
            .get_insert_records(&table_id)
            .await
            .expect("Failed to get insert records");
        assert_eq!(insert_records.get([1_u8].as_slice()), Some(&2));

        let db_path = test_db.strip_prefix("sqlite://").unwrap_or(&test_db);
        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(format!("{db_path}-shm"));
        let _ = std::fs::remove_file(format!("{db_path}-wal"));
    }

    #[tokio::test]
    async fn test_commit_on_conflict_deletions_rejects_conflicting_delete_file_metadata() {
        let test_db = format!(
            "sqlite://./.test_on_conflict_delete_file_conflict_{}.db",
            uuid::Uuid::now_v7()
        );
        let catalog = CayenneCatalog::new(&test_db).expect("Failed to create catalog");

        catalog.init().await.expect("Failed to initialize catalog");

        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "id",
            arrow_schema::DataType::Int64,
            false,
        )]));
        let table_options = CreateTableOptions {
            table_name: "test_table_on_conflict_conflict".to_string(),
            schema,
            primary_key: vec![],
            on_conflict: None,
            base_path: "/tmp/cayenne_test".to_string(),
            partition_column: None,
            vortex_config: crate::metadata::VortexConfig::default(),
        };
        let table_id = catalog
            .create_table(table_options)
            .await
            .expect("Failed to create table");

        let delete_file = DeleteFile {
            delete_file_id: String::new(),
            table_id: table_id.clone(),
            source_data_file_path: Some("/tmp/source.parquet".to_string()),
            path: "/tmp/on_conflict_delete_file_conflict.parquet".to_string(),
            path_is_relative: false,
            format: "parquet".to_string(),
            delete_count: 10,
            file_size_bytes: 512,
            deletion_type: DeletionType::default(),
            sequence_number: 1,
        };

        catalog
            .commit_on_conflict_deletions(
                vec![delete_file.clone()],
                &table_id,
                vec![vec![1_u8]],
                2,
                None,
            )
            .await
            .expect("initial on-conflict deletion commit should succeed");

        let mut conflicting_delete_file = delete_file;
        conflicting_delete_file.file_size_bytes = 1024;

        let err = catalog
            .commit_on_conflict_deletions(
                vec![conflicting_delete_file],
                &table_id,
                vec![vec![2_u8]],
                3,
                None,
            )
            .await
            .expect_err("conflicting delete-file metadata should be rejected");

        match err {
            CatalogError::InvalidOperation { message, source } => {
                assert!(
                    message.contains("Delete-file metadata conflicts"),
                    "expected descriptive on-conflict conflict message, got: {message}"
                );
                match source.downcast_ref::<CatalogError>() {
                    Some(CatalogError::ConstraintViolation { message }) => {
                        assert!(
                            message.contains("file_size_bytes"),
                            "expected file_size_bytes mismatch in error, got: {message}"
                        );
                    }
                    Some(other) => panic!("expected nested ConstraintViolation, got: {other}"),
                    None => panic!("expected nested CatalogError, got: {source}"),
                }
            }
            other => panic!("expected InvalidOperation, got: {other}"),
        }

        let delete_files = catalog
            .get_table_delete_files(&table_id)
            .await
            .expect("Failed to get delete files");
        assert_eq!(delete_files.len(), 1);
        assert_eq!(delete_files[0].file_size_bytes, 512);

        let insert_records = catalog
            .get_insert_records(&table_id)
            .await
            .expect("Failed to get insert records");
        assert_eq!(insert_records.get([1_u8].as_slice()), Some(&2));
        assert!(!insert_records.contains_key([2_u8].as_slice()));

        let db_path = test_db.strip_prefix("sqlite://").unwrap_or(&test_db);
        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(format!("{db_path}-shm"));
        let _ = std::fs::remove_file(format!("{db_path}-wal"));
    }

    #[tokio::test]
    async fn test_commit_on_conflict_deletions_batches_multiple_delete_files() {
        // Exercises the batched multi-VALUES INSERT path: multiple distinct
        // delete files committed in a single transaction must all be visible
        // afterward and produce a single row per (table_id, path).
        let test_db = format!(
            "sqlite://./.test_on_conflict_delete_file_batched_{}.db",
            uuid::Uuid::now_v7()
        );
        let catalog = CayenneCatalog::new(&test_db).expect("Failed to create catalog");

        catalog.init().await.expect("Failed to initialize catalog");

        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "id",
            arrow_schema::DataType::Int64,
            false,
        )]));
        let table_options = CreateTableOptions {
            table_name: "test_table_on_conflict_batched".to_string(),
            schema,
            primary_key: vec![],
            on_conflict: None,
            base_path: "/tmp/cayenne_test".to_string(),
            partition_column: None,
            vortex_config: crate::metadata::VortexConfig::default(),
        };
        let table_id = catalog
            .create_table(table_options)
            .await
            .expect("Failed to create table");

        let make_delete_file = |idx: usize| DeleteFile {
            delete_file_id: String::new(),
            table_id: table_id.clone(),
            source_data_file_path: Some(format!("/tmp/source_{idx}.parquet")),
            path: format!("/tmp/on_conflict_delete_file_batched_{idx}.parquet"),
            path_is_relative: false,
            format: "parquet".to_string(),
            delete_count: 10,
            file_size_bytes: 512,
            deletion_type: DeletionType::default(),
            sequence_number: 1,
        };

        let delete_files: Vec<DeleteFile> = (0..5).map(make_delete_file).collect();
        let insert_pks: Vec<Vec<u8>> = (0..5_u8).map(|i| vec![i]).collect();

        catalog
            .commit_on_conflict_deletions(delete_files.clone(), &table_id, insert_pks, 2, None)
            .await
            .expect("batched on-conflict deletion commit should succeed");

        let stored = catalog
            .get_table_delete_files(&table_id)
            .await
            .expect("Failed to get delete files");
        assert_eq!(stored.len(), 5);
        let stored_paths: std::collections::HashSet<&str> =
            stored.iter().map(|d| d.path.as_str()).collect();
        for expected in &delete_files {
            assert!(
                stored_paths.contains(expected.path.as_str()),
                "missing delete file path: {}",
                expected.path
            );
        }

        // Replay should be idempotent across the whole batch.
        catalog
            .commit_on_conflict_deletions(delete_files, &table_id, vec![vec![0_u8]], 2, None)
            .await
            .expect("replayed batched on-conflict deletion commit should be idempotent");
        let stored = catalog
            .get_table_delete_files(&table_id)
            .await
            .expect("Failed to get delete files after replay");
        assert_eq!(stored.len(), 5);

        let db_path = test_db.strip_prefix("sqlite://").unwrap_or(&test_db);
        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(format!("{db_path}-shm"));
        let _ = std::fs::remove_file(format!("{db_path}-wal"));
    }

    #[tokio::test]
    async fn test_concurrent_sequence_reservations_do_not_overlap() {
        const TASK_COUNT: usize = 16;
        const BLOCK_SIZE: u32 = 2;

        let test_db = format!(
            "sqlite://./.test_sequence_reservation_concurrency_{}.db",
            uuid::Uuid::now_v7()
        );
        let catalog = Arc::new(CayenneCatalog::new(&test_db).expect("Failed to create catalog"));

        catalog.init().await.expect("Failed to initialize catalog");

        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "id",
            arrow_schema::DataType::Int64,
            false,
        )]));
        let table_options = CreateTableOptions {
            table_name: "test_sequence_reservation_concurrency".to_string(),
            schema,
            primary_key: vec![],
            on_conflict: None,
            base_path: "/tmp/cayenne_test".to_string(),
            partition_column: None,
            vortex_config: crate::metadata::VortexConfig::default(),
        };
        let table_id = catalog
            .create_table(table_options)
            .await
            .expect("Failed to create table");

        let mut tasks = Vec::with_capacity(TASK_COUNT);
        for _ in 0..TASK_COUNT {
            let catalog = Arc::clone(&catalog);
            let table_id = table_id.clone();
            tasks.push(tokio::spawn(async move {
                catalog
                    .reserve_sequence_numbers(&table_id, BLOCK_SIZE)
                    .await
                    .expect("sequence reservation should succeed")
            }));
        }

        let block_size_usize = usize::try_from(BLOCK_SIZE).expect("BLOCK_SIZE fits in usize");
        let mut reserved_sequences = Vec::with_capacity(TASK_COUNT * block_size_usize);
        for task in tasks {
            let block_start = task.await.expect("reservation task should join");
            for offset in 0..BLOCK_SIZE {
                reserved_sequences.push(block_start + i64::from(offset));
            }
        }

        reserved_sequences.sort_unstable();
        assert_eq!(reserved_sequences.first().copied(), Some(1));
        assert_eq!(
            reserved_sequences.last().copied(),
            Some(
                i64::try_from(TASK_COUNT).expect("TASK_COUNT fits in i64") * i64::from(BLOCK_SIZE)
            )
        );
        for (expected, actual) in (1_i64..).zip(&reserved_sequences) {
            assert_eq!(*actual, expected);
        }

        let final_sequence = catalog
            .get_sequence_number(&table_id)
            .await
            .expect("Failed to get final sequence number");
        assert_eq!(
            final_sequence,
            i64::try_from(TASK_COUNT).expect("TASK_COUNT fits in i64") * i64::from(BLOCK_SIZE)
        );

        let db_path = test_db.strip_prefix("sqlite://").unwrap_or(&test_db);
        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(format!("{db_path}-shm"));
        let _ = std::fs::remove_file(format!("{db_path}-wal"));
    }

    #[tokio::test]
    async fn test_reserve_sequence_numbers_missing_table_errors() {
        let test_db = format!(
            "sqlite://./.test_sequence_reservation_missing_table_{}.db",
            uuid::Uuid::now_v7()
        );
        let catalog = CayenneCatalog::new(&test_db).expect("Failed to create catalog");

        catalog.init().await.expect("Failed to initialize catalog");

        let err = catalog
            .reserve_sequence_numbers("missing_table", 2)
            .await
            .expect_err("missing table sequence reservation should fail");

        match err {
            CatalogError::InvalidOperationNoSource { message } => assert!(
                message.contains("table row does not exist"),
                "expected missing-table error, got: {message}"
            ),
            other => panic!("expected InvalidOperationNoSource, got: {other}"),
        }

        let db_path = test_db.strip_prefix("sqlite://").unwrap_or(&test_db);
        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(format!("{db_path}-shm"));
        let _ = std::fs::remove_file(format!("{db_path}-wal"));
    }

    /// Test that shutdown properly flushes WAL and data persists across catalog restarts.
    #[tokio::test]
    async fn test_shutdown_wal_checkpoint_and_reload() {
        // Create a unique test database
        let test_db = format!(
            "sqlite://./.test_shutdown_reload_{}.db",
            uuid::Uuid::now_v7()
        );
        let db_path = test_db.strip_prefix("sqlite://").expect("test db path");

        // Create test schema
        let schema = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("id", arrow_schema::DataType::Int64, false),
            arrow_schema::Field::new("name", arrow_schema::DataType::Utf8, true),
        ]));

        let table_name = "test_shutdown_table";
        let base_path = "/tmp/cayenne_shutdown_test";

        // Phase 1: Create catalog, add data, shutdown
        let table_id;
        {
            let catalog = CayenneCatalog::new(&test_db).expect("Failed to create catalog");
            catalog.init().await.expect("Failed to initialize catalog");

            // Create a table
            let options = CreateTableOptions {
                table_name: table_name.to_string(),
                schema: Arc::clone(&schema),
                primary_key: vec!["id".to_string()],
                on_conflict: None,
                base_path: base_path.to_string(),
                partition_column: Some("name".to_string()),
                vortex_config: crate::metadata::VortexConfig::default(),
            };

            table_id = catalog
                .create_table(options)
                .await
                .expect("Failed to create table");

            // Add a partition
            let partition = PartitionMetadata {
                partition_id: String::new(),
                table_id: table_id.clone(),
                partition_columns: vec!["name".to_string()],
                partition_values: vec!["test_value".to_string()],
                path: format!("{base_path}/partition_test"),
                path_is_relative: false,
                record_count: 100,
                file_size_bytes: 2048,
            };
            catalog
                .add_partition(partition)
                .await
                .expect("Failed to add partition");

            // Add a delete file
            let delete_file = DeleteFile {
                delete_file_id: String::new(),
                table_id: table_id.clone(),
                source_data_file_path: None,
                path: format!("{base_path}/delete_file.parquet"),
                path_is_relative: false,
                format: "parquet".to_string(),
                delete_count: 5,
                file_size_bytes: 256,
                deletion_type: DeletionType::default(),
                sequence_number: 1,
            };
            catalog
                .add_delete_file(delete_file)
                .await
                .expect("Failed to add delete file");

            // Increment sequence number
            let seq = catalog
                .increment_sequence_number(&table_id)
                .await
                .expect("Failed to increment sequence");
            assert_eq!(seq, 1);

            // Perform graceful shutdown - this checkpoints the WAL
            catalog
                .shutdown()
                .await
                .expect("Failed to shutdown catalog");

            // Catalog goes out of scope here, connection is dropped
        }

        // Phase 2: Reopen catalog and verify all data persisted correctly
        {
            let catalog = CayenneCatalog::new(&test_db).expect("Failed to reopen catalog");
            catalog
                .init()
                .await
                .expect("Failed to reinitialize catalog");

            // Verify table exists with correct metadata
            let table = catalog
                .get_table(table_name)
                .await
                .expect("Table should exist after restart");

            assert_eq!(table.table_id, table_id);
            assert_eq!(table.table_name, table_name);
            assert_eq!(table.primary_key, vec!["id".to_string()]);
            assert_eq!(table.partition_column, Some("name".to_string()));
            assert_eq!(table.current_sequence_number, 1);

            // Verify partition persisted
            let partitions = catalog
                .get_partitions(&table_id)
                .await
                .expect("Failed to get partitions");
            assert_eq!(partitions.len(), 1);
            assert_eq!(partitions[0].partition_values, vec!["test_value"]);
            assert_eq!(partitions[0].record_count, 100);

            // Verify delete file persisted
            let delete_files = catalog
                .get_table_delete_files(&table_id)
                .await
                .expect("Failed to get delete files");
            assert_eq!(delete_files.len(), 1);
            assert_eq!(delete_files[0].delete_count, 5);
            assert_eq!(delete_files[0].sequence_number, 1);

            // Verify sequence number persisted
            let seq = catalog
                .get_sequence_number(&table_id)
                .await
                .expect("Failed to get sequence number");
            assert_eq!(seq, 1);
        }

        // Cleanup
        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(format!("{db_path}-shm"));
        let _ = std::fs::remove_file(format!("{db_path}-wal"));
    }

    /// Test multiple shutdown/reload cycles to ensure repeated restarts maintain integrity.
    #[tokio::test]
    async fn test_multiple_shutdown_reload_cycles() {
        let test_db = format!(
            "sqlite://./.test_multi_shutdown_{}.db",
            uuid::Uuid::now_v7()
        );
        let db_path = test_db.strip_prefix("sqlite://").expect("test db path");

        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "id",
            arrow_schema::DataType::Int64,
            false,
        )]));

        let table_name = "cycle_test_table";
        let base_path = "/tmp/cayenne_cycle_test";

        // Cycle 1: Create table
        let table_id;
        {
            let catalog = CayenneCatalog::new(&test_db).expect("Failed to create catalog");
            catalog.init().await.expect("Failed to init");

            let options = CreateTableOptions {
                table_name: table_name.to_string(),
                schema: Arc::clone(&schema),
                primary_key: vec![],
                on_conflict: None,
                base_path: base_path.to_string(),
                partition_column: None,
                vortex_config: crate::metadata::VortexConfig::default(),
            };

            table_id = catalog
                .create_table(options)
                .await
                .expect("Failed to create table");
            catalog.shutdown().await.expect("Shutdown failed");
        }

        // Cycle 2: Add delete files
        {
            let catalog = CayenneCatalog::new(&test_db).expect("Failed to reopen");
            catalog.init().await.expect("Failed to init");

            for i in 0..5 {
                let delete_file = DeleteFile {
                    delete_file_id: String::new(),
                    table_id: table_id.clone(),
                    source_data_file_path: None,
                    path: format!("{base_path}/delete_{i}.parquet"),
                    path_is_relative: false,
                    format: "parquet".to_string(),
                    delete_count: i + 1,
                    file_size_bytes: 100,
                    deletion_type: DeletionType::default(),
                    sequence_number: i + 1,
                };
                catalog
                    .add_delete_file(delete_file)
                    .await
                    .expect("Failed to add delete file");
            }

            catalog.shutdown().await.expect("Shutdown failed");
        }

        // Cycle 3: Verify and modify
        {
            let catalog = CayenneCatalog::new(&test_db).expect("Failed to reopen");
            catalog.init().await.expect("Failed to init");

            let delete_files = catalog
                .get_table_delete_files(&table_id)
                .await
                .expect("Failed to get delete files");
            assert_eq!(delete_files.len(), 5, "All 5 delete files should persist");

            // Increment sequence number multiple times
            for _ in 0..3 {
                catalog
                    .increment_sequence_number(&table_id)
                    .await
                    .expect("Failed to increment");
            }

            catalog.shutdown().await.expect("Shutdown failed");
        }

        // Cycle 4: Final verification
        {
            let catalog = CayenneCatalog::new(&test_db).expect("Failed to reopen");
            catalog.init().await.expect("Failed to init");

            let table = catalog
                .get_table(table_name)
                .await
                .expect("Table should exist");
            assert_eq!(
                table.current_sequence_number, 3,
                "Sequence number should be 3 after 3 increments"
            );

            let delete_files = catalog
                .get_table_delete_files(&table_id)
                .await
                .expect("Failed to get delete files");
            assert_eq!(delete_files.len(), 5);

            // Verify delete file sequence numbers
            let mut seq_nums: Vec<i64> = delete_files.iter().map(|f| f.sequence_number).collect();
            seq_nums.sort_unstable();
            assert_eq!(seq_nums, vec![1, 2, 3, 4, 5]);

            catalog.shutdown().await.expect("Shutdown failed");
        }

        // Cleanup
        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(format!("{db_path}-shm"));
        let _ = std::fs::remove_file(format!("{db_path}-wal"));
    }

    /// Test that data persists even without explicit shutdown (WAL should still be readable).
    #[tokio::test]
    async fn test_data_persists_without_explicit_shutdown() {
        let test_db = format!("sqlite://./.test_no_shutdown_{}.db", uuid::Uuid::now_v7());
        let db_path = test_db.strip_prefix("sqlite://").expect("test db path");

        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "id",
            arrow_schema::DataType::Int64,
            false,
        )]));

        let table_name = "no_shutdown_table";

        // Create and populate without explicit shutdown
        let table_id;
        {
            let catalog = CayenneCatalog::new(&test_db).expect("Failed to create catalog");
            catalog.init().await.expect("Failed to init");

            let options = CreateTableOptions {
                table_name: table_name.to_string(),
                schema,
                primary_key: vec![],
                on_conflict: None,
                base_path: "/tmp/no_shutdown_test".to_string(),
                partition_column: None,
                vortex_config: crate::metadata::VortexConfig::default(),
            };

            table_id = catalog
                .create_table(options)
                .await
                .expect("Failed to create table");

            // Add some data
            catalog
                .increment_sequence_number(&table_id)
                .await
                .expect("Failed to increment");
            catalog
                .increment_sequence_number(&table_id)
                .await
                .expect("Failed to increment");

            // NO explicit shutdown - catalog just drops
        }

        // Reopen and verify data is still accessible (SQLite WAL recovery)
        {
            let catalog = CayenneCatalog::new(&test_db).expect("Failed to reopen");
            catalog.init().await.expect("Failed to init");

            let table = catalog
                .get_table(table_name)
                .await
                .expect("Table should exist");
            assert_eq!(table.table_id, table_id);
            assert_eq!(table.current_sequence_number, 2, "Sequence should be 2");

            // Now do proper shutdown
            catalog.shutdown().await.expect("Shutdown failed");
        }

        // Cleanup
        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(format!("{db_path}-shm"));
        let _ = std::fs::remove_file(format!("{db_path}-wal"));
    }

    /// Test insert records persist across shutdown/reload.
    #[tokio::test]
    async fn test_insert_records_persist_across_restart() {
        let test_db = format!(
            "sqlite://./.test_insert_records_{}.db",
            uuid::Uuid::now_v7()
        );
        let db_path = test_db.strip_prefix("sqlite://").expect("test db path");

        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "id",
            arrow_schema::DataType::Int64,
            false,
        )]));

        // Create and add insert records
        let table_id;
        {
            let catalog = CayenneCatalog::new(&test_db).expect("Failed to create catalog");
            catalog.init().await.expect("Failed to init");

            let options = CreateTableOptions {
                table_name: "insert_record_test".to_string(),
                schema,
                primary_key: vec!["id".to_string()],
                on_conflict: None,
                base_path: "/tmp/insert_record_test".to_string(),
                partition_column: None,
                vortex_config: crate::metadata::VortexConfig::default(),
            };

            table_id = catalog
                .create_table(options)
                .await
                .expect("Failed to create table");

            // Add individual insert records
            catalog
                .add_insert_record(&table_id, vec![1, 2, 3, 4], 1)
                .await
                .expect("Failed to add insert record");
            catalog
                .add_insert_record(&table_id, vec![5, 6, 7, 8], 2)
                .await
                .expect("Failed to add insert record");

            // Add batch insert records
            catalog
                .add_insert_records_batch(
                    &table_id,
                    vec![vec![9, 10], vec![11, 12], vec![13, 14]],
                    3,
                )
                .await
                .expect("Failed to add batch insert records");

            catalog.shutdown().await.expect("Shutdown failed");
        }

        // Reopen and verify
        {
            let catalog = CayenneCatalog::new(&test_db).expect("Failed to reopen");
            catalog.init().await.expect("Failed to init");

            let records = catalog
                .get_insert_records(&table_id)
                .await
                .expect("Failed to get insert records");

            assert_eq!(records.len(), 5, "Should have 5 insert records");

            // Verify specific records by converting to Box<[u8]> for lookup
            let key1: Box<[u8]> = vec![1u8, 2, 3, 4].into_boxed_slice();
            let key2: Box<[u8]> = vec![5u8, 6, 7, 8].into_boxed_slice();
            let key3: Box<[u8]> = vec![9u8, 10].into_boxed_slice();
            let key4: Box<[u8]> = vec![11u8, 12].into_boxed_slice();
            let key5: Box<[u8]> = vec![13u8, 14].into_boxed_slice();

            assert_eq!(records.get(&key1), Some(&1i64));
            assert_eq!(records.get(&key2), Some(&2i64));
            assert_eq!(records.get(&key3), Some(&3i64));
            assert_eq!(records.get(&key4), Some(&3i64));
            assert_eq!(records.get(&key5), Some(&3i64));

            catalog.shutdown().await.expect("Shutdown failed");
        }

        // Cleanup
        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(format!("{db_path}-shm"));
        let _ = std::fs::remove_file(format!("{db_path}-wal"));
    }

    /// Test snapshot sequences persist across restart.
    #[tokio::test]
    async fn test_snapshot_sequences_persist_across_restart() {
        let test_db = format!("sqlite://./.test_snapshot_seq_{}.db", uuid::Uuid::now_v7());
        let db_path = test_db.strip_prefix("sqlite://").expect("test db path");

        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "id",
            arrow_schema::DataType::Int64,
            false,
        )]));

        let snapshot_1 = uuid::Uuid::now_v7().to_string();
        let snapshot_2 = uuid::Uuid::now_v7().to_string();
        let snapshot_3 = uuid::Uuid::now_v7().to_string();

        // Create and set snapshot sequences
        let table_id;
        {
            let catalog = CayenneCatalog::new(&test_db).expect("Failed to create catalog");
            catalog.init().await.expect("Failed to init");

            let options = CreateTableOptions {
                table_name: "snapshot_seq_test".to_string(),
                schema,
                primary_key: vec![],
                on_conflict: None,
                base_path: "/tmp/snapshot_seq_test".to_string(),
                partition_column: None,
                vortex_config: crate::metadata::VortexConfig::default(),
            };

            table_id = catalog
                .create_table(options)
                .await
                .expect("Failed to create table");

            catalog
                .set_snapshot_sequence(&table_id, &snapshot_1, 10)
                .await
                .expect("Failed to set snapshot seq");
            catalog
                .set_snapshot_sequence(&table_id, &snapshot_2, 20)
                .await
                .expect("Failed to set snapshot seq");
            catalog
                .set_snapshot_sequence(&table_id, &snapshot_3, 30)
                .await
                .expect("Failed to set snapshot seq");

            catalog.shutdown().await.expect("Shutdown failed");
        }

        // Reopen and verify
        {
            let catalog = CayenneCatalog::new(&test_db).expect("Failed to reopen");
            catalog.init().await.expect("Failed to init");

            let seq_1 = catalog
                .get_snapshot_sequence(&table_id, &snapshot_1)
                .await
                .expect("Failed to get seq");
            let seq_2 = catalog
                .get_snapshot_sequence(&table_id, &snapshot_2)
                .await
                .expect("Failed to get seq");
            let seq_3 = catalog
                .get_snapshot_sequence(&table_id, &snapshot_3)
                .await
                .expect("Failed to get seq");

            assert_eq!(seq_1, Some(10));
            assert_eq!(seq_2, Some(20));
            assert_eq!(seq_3, Some(30));

            let all_seqs = catalog
                .get_all_snapshot_sequences(&table_id)
                .await
                .expect("Failed to get all seqs");
            assert_eq!(all_seqs.len(), 3);

            catalog.shutdown().await.expect("Shutdown failed");
        }

        // Cleanup
        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(format!("{db_path}-shm"));
        let _ = std::fs::remove_file(format!("{db_path}-wal"));
    }

    #[tokio::test]
    async fn test_create_table_falls_back_on_config_change() {
        let test_db = format!("sqlite://./.test_config_change_{}.db", uuid::Uuid::now_v7());
        let catalog = CayenneCatalog::new(&test_db).expect("Failed to create catalog");
        catalog.init().await.expect("Failed to initialize catalog");

        let schema = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("id", arrow_schema::DataType::Int64, false),
            arrow_schema::Field::new("name", arrow_schema::DataType::Utf8, true),
        ]));

        // Create initial table with no primary key
        let options = CreateTableOptions {
            table_name: "test_table".to_string(),
            schema: Arc::clone(&schema),
            primary_key: vec![],
            on_conflict: None,
            base_path: "/tmp/cayenne_config_test".to_string(),
            partition_column: None,
            vortex_config: crate::metadata::VortexConfig::default(),
        };
        let table_id_1 = catalog
            .create_table(options)
            .await
            .expect("Failed to create table");

        // Verify table was created
        let metadata = catalog
            .get_table("test_table")
            .await
            .expect("Failed to get table");
        assert!(metadata.primary_key.is_empty());
        assert_eq!(metadata.table_id, table_id_1);

        // Now try to create with a primary key change — should fall back to stored config
        // (a warning is logged, but create_table succeeds with the original table_id)
        let options_changed = CreateTableOptions {
            table_name: "test_table".to_string(),
            schema: Arc::clone(&schema),
            primary_key: vec!["id".to_string()],
            on_conflict: None,
            base_path: "/tmp/cayenne_config_test".to_string(),
            partition_column: None,
            vortex_config: crate::metadata::VortexConfig::default(),
        };
        let table_id_2 = catalog
            .create_table(options_changed)
            .await
            .expect("Config change should fall back gracefully");
        assert_eq!(
            table_id_1, table_id_2,
            "Should return the original table_id when config changes"
        );

        // Original table should still be intact with original config
        let metadata = catalog
            .get_table("test_table")
            .await
            .expect("Failed to get table");
        assert!(metadata.primary_key.is_empty());
        assert_eq!(metadata.table_id, table_id_1);

        // Recreate with the SAME config — should return the same table_id
        let options_same = CreateTableOptions {
            table_name: "test_table".to_string(),
            schema: Arc::clone(&schema),
            primary_key: vec![],
            on_conflict: None,
            base_path: "/tmp/cayenne_config_test".to_string(),
            partition_column: None,
            vortex_config: crate::metadata::VortexConfig::default(),
        };
        let table_id_2 = catalog
            .create_table(options_same)
            .await
            .expect("Failed to create table with same config");

        // Should reuse the existing table
        assert_eq!(table_id_1, table_id_2);

        // Cleanup
        let db_path = test_db.strip_prefix("sqlite://").unwrap_or(&test_db);
        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(format!("{db_path}-shm"));
        let _ = std::fs::remove_file(format!("{db_path}-wal"));
    }

    #[tokio::test]
    async fn test_create_table_falls_back_on_sort_columns_change() {
        let test_db = format!("sqlite://./.test_sort_change_{}.db", uuid::Uuid::now_v7());
        let catalog = CayenneCatalog::new(&test_db).expect("Failed to create catalog");
        catalog.init().await.expect("Failed to initialize catalog");

        let schema = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("id", arrow_schema::DataType::Int64, false),
            arrow_schema::Field::new("ts", arrow_schema::DataType::Int64, false),
        ]));

        // Create table with no sort columns
        let options = CreateTableOptions {
            table_name: "sorted_table".to_string(),
            schema: Arc::clone(&schema),
            primary_key: vec![],
            on_conflict: None,
            base_path: "/tmp/cayenne_sort_test".to_string(),
            partition_column: None,
            vortex_config: crate::metadata::VortexConfig::default(),
        };
        let table_id_1 = catalog
            .create_table(options)
            .await
            .expect("Failed to create table");

        // Add sort columns — should fall back to stored config with a warning
        let vortex_config = crate::metadata::VortexConfig {
            sort_columns: vec!["ts".to_string()],
            ..Default::default()
        };
        let options_sorted = CreateTableOptions {
            table_name: "sorted_table".to_string(),
            schema: Arc::clone(&schema),
            primary_key: vec![],
            on_conflict: None,
            base_path: "/tmp/cayenne_sort_test".to_string(),
            partition_column: None,
            vortex_config,
        };
        let table_id_2 = catalog
            .create_table(options_sorted)
            .await
            .expect("Sort column change should fall back gracefully");
        assert_eq!(
            table_id_1, table_id_2,
            "Should return the original table_id when sort columns change"
        );

        // Cleanup
        let db_path = test_db.strip_prefix("sqlite://").unwrap_or(&test_db);
        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(format!("{db_path}-shm"));
        let _ = std::fs::remove_file(format!("{db_path}-wal"));
    }

    #[tokio::test]
    async fn test_create_table_cache_change_does_not_recreate() {
        let test_db = format!("sqlite://./.test_cache_change_{}.db", uuid::Uuid::now_v7());
        let catalog = CayenneCatalog::new(&test_db).expect("Failed to create catalog");
        catalog.init().await.expect("Failed to initialize catalog");

        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "id",
            arrow_schema::DataType::Int64,
            false,
        )]));

        // Create table with default cache settings
        let options = CreateTableOptions {
            table_name: "cache_table".to_string(),
            schema: Arc::clone(&schema),
            primary_key: vec![],
            on_conflict: None,
            base_path: "/tmp/cayenne_cache_test".to_string(),
            partition_column: None,
            vortex_config: crate::metadata::VortexConfig::default(),
        };
        let table_id_1 = catalog
            .create_table(options)
            .await
            .expect("Failed to create table");

        // Change only cache sizes (non-data-affecting) — should NOT trigger recreation
        let vortex_config = crate::metadata::VortexConfig {
            footer_cache_mb: Some(512),
            segment_cache_mb: 1024,
            upload_concurrency: 8,
            write_concurrency: Some(16),
            target_vortex_file_size_mb: 512,
            ..Default::default()
        };
        let options_cache_changed = CreateTableOptions {
            table_name: "cache_table".to_string(),
            schema: Arc::clone(&schema),
            primary_key: vec![],
            on_conflict: None,
            base_path: "/tmp/cayenne_cache_test".to_string(),
            partition_column: None,
            vortex_config,
        };
        let table_id_2 = catalog
            .create_table(options_cache_changed)
            .await
            .expect("Failed to create table with cache change");

        // Should reuse the same table (cache changes don't affect data)
        assert_eq!(table_id_1, table_id_2);

        // Cleanup
        let db_path = test_db.strip_prefix("sqlite://").unwrap_or(&test_db);
        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(format!("{db_path}-shm"));
        let _ = std::fs::remove_file(format!("{db_path}-wal"));
    }

    /// Test that `commit_compaction` clears delete files, insert records, and
    /// snapshot sequences, and updates the active snapshot pointer.
    #[tokio::test]
    async fn test_commit_compaction_clears_metadata() {
        let test_db = format!(
            "sqlite://./.test_commit_compaction_{}.db",
            uuid::Uuid::now_v7()
        );
        let catalog = CayenneCatalog::new(&test_db).expect("Failed to create catalog");
        catalog.init().await.expect("Failed to initialize catalog");

        // Create a table.
        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "id",
            arrow_schema::DataType::Int64,
            false,
        )]));
        let table_id = catalog
            .create_table(CreateTableOptions {
                table_name: "compaction_test".to_string(),
                schema,
                primary_key: vec![],
                on_conflict: None,
                base_path: "/tmp/cayenne_compaction_test".to_string(),
                partition_column: None,
                vortex_config: crate::metadata::VortexConfig::default(),
            })
            .await
            .expect("Failed to create table");

        // Add a delete file so there is something to clear.
        let delete_file = DeleteFile {
            delete_file_id: String::new(),
            table_id: table_id.clone(),
            source_data_file_path: None,
            path: "/tmp/delete.parquet".to_string(),
            path_is_relative: false,
            format: "parquet".to_string(),
            delete_count: 5,
            file_size_bytes: 256,
            deletion_type: DeletionType::default(),
            sequence_number: 1,
        };
        catalog
            .add_delete_file(delete_file)
            .await
            .expect("Failed to add delete file");

        // Verify delete file exists before compaction.
        let before = catalog
            .get_table_delete_files(&table_id)
            .await
            .expect("Failed to get delete files");
        assert_eq!(before.len(), 1, "Expected 1 delete file before compaction");

        // Commit compaction with a new snapshot ID.
        let new_snapshot_id = uuid::Uuid::now_v7().to_string();
        catalog
            .commit_compaction(&table_id, &new_snapshot_id)
            .await
            .expect("commit_compaction failed");

        // Verify delete files were cleared.
        let after = catalog
            .get_table_delete_files(&table_id)
            .await
            .expect("Failed to get delete files after compaction");
        assert!(
            after.is_empty(),
            "Delete files should be cleared after compaction"
        );

        // Verify the snapshot pointer was updated.
        let table = catalog
            .get_table("compaction_test")
            .await
            .expect("Failed to get table after compaction");
        assert_eq!(
            table.current_snapshot_id, new_snapshot_id,
            "Snapshot pointer should be updated after compaction"
        );

        // Cleanup.
        let db_path = test_db.strip_prefix("sqlite://").unwrap_or(&test_db);
        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(format!("{db_path}-shm"));
        let _ = std::fs::remove_file(format!("{db_path}-wal"));
    }

    /// Test that `commit_compaction` rejects non-UUID identifiers.
    #[tokio::test]
    async fn test_commit_compaction_rejects_invalid_uuid() {
        let test_db = format!(
            "sqlite://./.test_compaction_invalid_{}.db",
            uuid::Uuid::now_v7()
        );
        let catalog = CayenneCatalog::new(&test_db).expect("Failed to create catalog");
        catalog.init().await.expect("Failed to initialize catalog");

        let valid_uuid = uuid::Uuid::now_v7().to_string();

        // Invalid table_id should fail.
        let result = catalog
            .commit_compaction("'; DROP TABLE cayenne_table;--", &valid_uuid)
            .await;
        assert!(result.is_err(), "Should reject non-UUID table_id");

        // Invalid new_snapshot_id should fail.
        let result = catalog.commit_compaction(&valid_uuid, "not-a-uuid").await;
        assert!(result.is_err(), "Should reject non-UUID new_snapshot_id");

        // Cleanup.
        let db_path = test_db.strip_prefix("sqlite://").unwrap_or(&test_db);
        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(format!("{db_path}-shm"));
        let _ = std::fs::remove_file(format!("{db_path}-wal"));
    }

    /// Helper used by the `commit_compaction_in_txn` tests: create a table and
    /// attach a delete file to it so the `in_txn` variant has metadata to clear
    /// and a snapshot pointer to advance.
    async fn setup_table_with_delete_file(
        catalog: &CayenneCatalog,
        table_name: &str,
        base_path: &str,
    ) -> String {
        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "id",
            arrow_schema::DataType::Int64,
            false,
        )]));
        let table_id = catalog
            .create_table(CreateTableOptions {
                table_name: table_name.to_string(),
                schema,
                primary_key: vec![],
                on_conflict: None,
                base_path: base_path.to_string(),
                partition_column: None,
                vortex_config: crate::metadata::VortexConfig::default(),
            })
            .await
            .expect("Failed to create table");

        let delete_file = DeleteFile {
            delete_file_id: String::new(),
            table_id: table_id.clone(),
            source_data_file_path: None,
            path: format!("/tmp/delete_{table_name}.parquet"),
            path_is_relative: false,
            format: "parquet".to_string(),
            delete_count: 5,
            file_size_bytes: 256,
            deletion_type: DeletionType::default(),
            sequence_number: 1,
        };
        catalog
            .add_delete_file(delete_file)
            .await
            .expect("Failed to add delete file");

        table_id
    }

    #[tokio::test]
    async fn test_clear_inlined_data_and_deletes_clears_both_tables() {
        let test_db = format!(
            "sqlite://./.test_clear_inline_metadata_{}.db",
            uuid::Uuid::now_v7()
        );
        let catalog = CayenneCatalog::new(&test_db).expect("Failed to create catalog");
        catalog.init().await.expect("Failed to initialize catalog");

        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "id",
            arrow_schema::DataType::Int64,
            false,
        )]));
        let table_id = catalog
            .create_table(CreateTableOptions {
                table_name: "clear_inline_metadata".to_string(),
                schema,
                primary_key: vec![],
                on_conflict: None,
                base_path: "/tmp/clear_inline_metadata".to_string(),
                partition_column: None,
                vortex_config: crate::metadata::VortexConfig::default(),
            })
            .await
            .expect("Failed to create table");

        catalog
            .add_inlined_data(InlinedData {
                inlined_id: String::new(),
                table_id: table_id.clone(),
                partition_key: None,
                data_ipc: vec![1, 2, 3],
                record_count: 3,
                sequence_number: 1,
                created_at: String::new(),
            })
            .await
            .expect("Failed to add inlined data");
        catalog
            .add_inlined_delete(InlinedDelete {
                inlined_id: String::new(),
                table_id: table_id.clone(),
                delete_ipc: vec![4, 5, 6],
                delete_count: 2,
                sequence_number: 2,
                created_at: String::new(),
            })
            .await
            .expect("Failed to add inlined delete");

        assert_eq!(
            catalog
                .get_inlined_data_count(&table_id)
                .await
                .expect("Failed to get inlined data count"),
            3
        );
        assert_eq!(
            catalog
                .get_inlined_deletes(&table_id)
                .await
                .expect("Failed to get inlined deletes")
                .len(),
            1
        );

        catalog
            .clear_inlined_data_and_deletes(&table_id)
            .await
            .expect("Failed to clear inline metadata");

        assert_eq!(
            catalog
                .get_inlined_data_count(&table_id)
                .await
                .expect("Failed to get inlined data count after clear"),
            0
        );
        assert!(
            catalog
                .get_inlined_deletes(&table_id)
                .await
                .expect("Failed to get inlined deletes after clear")
                .is_empty()
        );

        let db_path = test_db.strip_prefix("sqlite://").unwrap_or(&test_db);
        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(format!("{db_path}-shm"));
        let _ = std::fs::remove_file(format!("{db_path}-wal"));
    }

    /// Issue #10125 — `commit_compaction_in_txn` applied to a single partition
    /// inside an explicit transaction is observably equivalent to the legacy
    /// `commit_compaction`: snapshot pointer advances, delete files cleared.
    #[tokio::test]
    async fn test_commit_compaction_in_txn_single_partition_parity() {
        let test_db = format!("sqlite://./.test_in_txn_parity_{}.db", uuid::Uuid::now_v7());
        let catalog = CayenneCatalog::new(&test_db).expect("Failed to create catalog");
        catalog.init().await.expect("Failed to initialize catalog");

        let table_id =
            setup_table_with_delete_file(&catalog, "in_txn_parity", "/tmp/in_txn_parity").await;

        // Sanity: delete file exists before the in_txn call.
        let before = catalog
            .get_table_delete_files(&table_id)
            .await
            .expect("Failed to get delete files");
        assert_eq!(before.len(), 1, "Expected 1 delete file before commit");

        let new_snapshot_id = uuid::Uuid::now_v7().to_string();

        // Caller-owned transaction: open, apply in_txn variant, commit.
        let mut tx = catalog
            .begin_transaction()
            .await
            .expect("Failed to begin transaction");
        catalog
            .commit_compaction_in_txn(&mut *tx, &table_id, &new_snapshot_id)
            .await
            .expect("commit_compaction_in_txn failed");
        tx.commit()
            .await
            .expect("Failed to commit caller transaction");

        // Delete files cleared.
        let after = catalog
            .get_table_delete_files(&table_id)
            .await
            .expect("Failed to get delete files after commit");
        assert!(
            after.is_empty(),
            "Delete files should be cleared after commit_compaction_in_txn"
        );

        // Snapshot pointer advanced.
        let table = catalog
            .get_table("in_txn_parity")
            .await
            .expect("Failed to get table after commit");
        assert_eq!(table.current_snapshot_id, new_snapshot_id);

        // Cleanup.
        let db_path = test_db.strip_prefix("sqlite://").unwrap_or(&test_db);
        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(format!("{db_path}-shm"));
        let _ = std::fs::remove_file(format!("{db_path}-wal"));
    }

    /// Issue #10125 — two `commit_compaction_in_txn` calls inside one
    /// transaction commit atomically: after `tx.commit()`, both partitions'
    /// pointers have advanced together.
    #[tokio::test]
    async fn test_commit_compaction_in_txn_cross_partition_atomicity() {
        let test_db = format!(
            "sqlite://./.test_in_txn_cross_atomic_{}.db",
            uuid::Uuid::now_v7()
        );
        let catalog = CayenneCatalog::new(&test_db).expect("Failed to create catalog");
        catalog.init().await.expect("Failed to initialize catalog");

        // Two "partitions": independent tables, treated as a single atomic
        // commit unit by the (future) cross-partition coordinator.
        let table_a = setup_table_with_delete_file(&catalog, "partition_a", "/tmp/p_a").await;
        let table_b = setup_table_with_delete_file(&catalog, "partition_b", "/tmp/p_b").await;

        let snap_a = uuid::Uuid::now_v7().to_string();
        let snap_b = uuid::Uuid::now_v7().to_string();

        let mut tx = catalog
            .begin_transaction()
            .await
            .expect("Failed to begin transaction");
        catalog
            .commit_compaction_in_txn(&mut *tx, &table_a, &snap_a)
            .await
            .expect("partition A in_txn failed");
        catalog
            .commit_compaction_in_txn(&mut *tx, &table_b, &snap_b)
            .await
            .expect("partition B in_txn failed");
        tx.commit().await.expect("Failed to commit transaction");

        // Both partitions advanced after the single tx.commit().
        let a = catalog.get_table("partition_a").await.expect("get a");
        let b = catalog.get_table("partition_b").await.expect("get b");
        assert_eq!(a.current_snapshot_id, snap_a);
        assert_eq!(b.current_snapshot_id, snap_b);

        // Cleanup.
        let db_path = test_db.strip_prefix("sqlite://").unwrap_or(&test_db);
        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(format!("{db_path}-shm"));
        let _ = std::fs::remove_file(format!("{db_path}-wal"));
    }

    /// Issue #10125 — dropping the transaction without committing rolls back
    /// every `commit_compaction_in_txn` call applied inside it. The catalog
    /// is left exactly as it was before the transaction opened.
    #[tokio::test]
    async fn test_commit_compaction_in_txn_rolls_back_on_drop() {
        let test_db = format!(
            "sqlite://./.test_in_txn_rollback_{}.db",
            uuid::Uuid::now_v7()
        );
        let catalog = CayenneCatalog::new(&test_db).expect("Failed to create catalog");
        catalog.init().await.expect("Failed to initialize catalog");

        let table_id =
            setup_table_with_delete_file(&catalog, "in_txn_rollback", "/tmp/in_txn_rb").await;

        // Capture pre-commit state.
        let before = catalog.get_table("in_txn_rollback").await.expect("get");
        let original_snapshot_id = before.current_snapshot_id.clone();

        let attempted_snapshot_id = uuid::Uuid::now_v7().to_string();

        {
            let mut tx = catalog
                .begin_transaction()
                .await
                .expect("Failed to begin transaction");
            catalog
                .commit_compaction_in_txn(&mut *tx, &table_id, &attempted_snapshot_id)
                .await
                .expect("in_txn variant succeeded inside tx");
            // Drop tx without committing — auto-rollback.
        }

        // The pointer must NOT have advanced.
        let after = catalog.get_table("in_txn_rollback").await.expect("get");
        assert_eq!(
            after.current_snapshot_id, original_snapshot_id,
            "Dropping the transaction must roll back commit_compaction_in_txn"
        );

        // The delete file must STILL exist.
        let delete_files = catalog
            .get_table_delete_files(&table_id)
            .await
            .expect("get delete files");
        assert_eq!(
            delete_files.len(),
            1,
            "Delete files must still exist after a rolled-back commit_compaction_in_txn"
        );

        // Cleanup.
        let db_path = test_db.strip_prefix("sqlite://").unwrap_or(&test_db);
        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(format!("{db_path}-shm"));
        let _ = std::fs::remove_file(format!("{db_path}-wal"));
    }

    /// Issue #10125 — `commit_compaction_in_txn` rejects non-UUID identifiers
    /// before touching the borrowed transaction. The error path leaves the
    /// catalog and the transaction untouched.
    #[tokio::test]
    async fn test_commit_compaction_in_txn_rejects_invalid_uuid() {
        let test_db = format!(
            "sqlite://./.test_in_txn_invalid_uuid_{}.db",
            uuid::Uuid::now_v7()
        );
        let catalog = CayenneCatalog::new(&test_db).expect("Failed to create catalog");
        catalog.init().await.expect("Failed to initialize catalog");

        let valid_uuid = uuid::Uuid::now_v7().to_string();

        let mut tx = catalog
            .begin_transaction()
            .await
            .expect("Failed to begin transaction");

        // Invalid table_id should fail.
        let result = catalog
            .commit_compaction_in_txn(&mut *tx, "'; DROP TABLE cayenne_table;--", &valid_uuid)
            .await;
        assert!(result.is_err(), "Should reject non-UUID table_id");

        // Invalid new_snapshot_id should fail.
        let result = catalog
            .commit_compaction_in_txn(&mut *tx, &valid_uuid, "not-a-uuid")
            .await;
        assert!(result.is_err(), "Should reject non-UUID new_snapshot_id");

        // The borrowed transaction is still usable for a subsequent valid call
        // (we never rolled back; the error path is purely validation, no SQL
        // was sent).
        drop(tx);

        // Cleanup.
        let db_path = test_db.strip_prefix("sqlite://").unwrap_or(&test_db);
        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(format!("{db_path}-shm"));
        let _ = std::fs::remove_file(format!("{db_path}-wal"));
    }

    #[test]
    fn test_sql_text_literal_escapes_single_quotes() {
        assert_eq!(sql_text_literal("abc'def"), "'abc''def'");
    }

    #[test]
    fn test_delete_file_unique_constraint_violation_message_matches_expected_conflicts() {
        let messages = [
            "UNIQUE constraint failed: cayenne_delete_file.table_id, cayenne_delete_file.path",
            "constraint failed: idx_cayenne_delete_file_table_path",
        ];

        for message in messages {
            assert!(is_delete_file_unique_constraint_violation_message(message));
        }
    }

    #[test]
    fn test_delete_file_unique_constraint_violation_message_rejects_unrelated_constraints() {
        let messages = [
            "FOREIGN KEY constraint failed",
            "UNIQUE constraint failed: cayenne_table.table_name",
        ];

        for message in messages {
            assert!(!is_delete_file_unique_constraint_violation_message(message));
        }
    }

    #[test]
    fn test_partition_unique_constraint_violation_message_matches_expected_conflicts() {
        let message =
            "UNIQUE constraint failed: cayenne_partition.table_id, cayenne_partition.partition_key";
        assert!(is_partition_unique_constraint_violation_message(message));
    }

    #[test]
    fn test_partition_unique_constraint_violation_message_rejects_unrelated_constraints() {
        let messages = [
            "FOREIGN KEY constraint failed",
            "UNIQUE constraint failed: cayenne_delete_file.table_id, cayenne_delete_file.path",
        ];

        for message in messages {
            assert!(!is_partition_unique_constraint_violation_message(message));
        }
    }

    /// Helper to create a [`TableMetadata`] for unit tests.
    fn make_test_metadata(
        primary_key: Vec<String>,
        on_conflict: Option<datafusion_table_providers::util::on_conflict::OnConflict>,
        partition_column: Option<String>,
        path: &str,
        vortex_config: crate::metadata::VortexConfig,
        schema: arrow_schema::SchemaRef,
    ) -> TableMetadata {
        TableMetadata {
            table_id: "test-id".to_string(),
            table_name: "test_table".to_string(),
            path: path.to_string(),
            path_is_relative: false,
            schema,
            primary_key,
            on_conflict,
            current_snapshot_id: "snap-1".to_string(),
            partition_column,
            vortex_config,
            current_sequence_number: 0,
        }
    }

    #[test]
    fn test_configuration_matches_identical() {
        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "id",
            arrow_schema::DataType::Int64,
            false,
        )]));
        let stored = make_test_metadata(
            vec!["id".to_string()],
            None,
            None,
            "/tmp/test",
            crate::metadata::VortexConfig::default(),
            Arc::clone(&schema),
        );
        let options = CreateTableOptions {
            table_name: "test_table".to_string(),
            schema,
            primary_key: vec!["id".to_string()],
            on_conflict: None,
            base_path: "/tmp/test".to_string(),
            partition_column: None,
            vortex_config: crate::metadata::VortexConfig::default(),
        };
        assert!(
            configuration_matches(&stored, &options),
            "Identical configurations should match"
        );
    }

    #[test]
    fn test_configuration_matches_primary_key_differs() {
        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "id",
            arrow_schema::DataType::Int64,
            false,
        )]));
        let stored = make_test_metadata(
            vec![],
            None,
            None,
            "/tmp/test",
            crate::metadata::VortexConfig::default(),
            Arc::clone(&schema),
        );
        let options = CreateTableOptions {
            table_name: "test_table".to_string(),
            schema,
            primary_key: vec!["id".to_string()],
            on_conflict: None,
            base_path: "/tmp/test".to_string(),
            partition_column: None,
            vortex_config: crate::metadata::VortexConfig::default(),
        };
        assert!(
            !configuration_matches(&stored, &options),
            "Different primary_key should not match"
        );
    }

    #[test]
    fn test_configuration_matches_on_conflict_differs() {
        use datafusion_table_providers::util::on_conflict::OnConflict;
        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "id",
            arrow_schema::DataType::Int64,
            false,
        )]));
        let stored = make_test_metadata(
            vec!["id".to_string()],
            None,
            None,
            "/tmp/test",
            crate::metadata::VortexConfig::default(),
            Arc::clone(&schema),
        );
        let options = CreateTableOptions {
            table_name: "test_table".to_string(),
            schema,
            primary_key: vec!["id".to_string()],
            on_conflict: Some(OnConflict::DoNothingAll),
            base_path: "/tmp/test".to_string(),
            partition_column: None,
            vortex_config: crate::metadata::VortexConfig::default(),
        };
        assert!(
            !configuration_matches(&stored, &options),
            "Different on_conflict should not match"
        );
    }

    #[test]
    fn test_configuration_matches_sort_columns_differ() {
        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "id",
            arrow_schema::DataType::Int64,
            false,
        )]));
        let stored = make_test_metadata(
            vec![],
            None,
            None,
            "/tmp/test",
            crate::metadata::VortexConfig::default(),
            Arc::clone(&schema),
        );
        let changed_vortex = crate::metadata::VortexConfig {
            sort_columns: vec!["id".to_string()],
            ..Default::default()
        };
        let options = CreateTableOptions {
            table_name: "test_table".to_string(),
            schema,
            primary_key: vec![],
            on_conflict: None,
            base_path: "/tmp/test".to_string(),
            partition_column: None,
            vortex_config: changed_vortex,
        };
        assert!(
            !configuration_matches(&stored, &options),
            "Different sort_columns should not match"
        );
    }

    #[test]
    fn test_configuration_matches_base_path_differs() {
        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "id",
            arrow_schema::DataType::Int64,
            false,
        )]));
        let stored = make_test_metadata(
            vec![],
            None,
            None,
            "/tmp/old_path",
            crate::metadata::VortexConfig::default(),
            Arc::clone(&schema),
        );
        let options = CreateTableOptions {
            table_name: "test_table".to_string(),
            schema,
            primary_key: vec![],
            on_conflict: None,
            base_path: "/tmp/new_path".to_string(),
            partition_column: None,
            vortex_config: crate::metadata::VortexConfig::default(),
        };
        assert!(
            !configuration_matches(&stored, &options),
            "Different base_path should not match"
        );
    }

    #[test]
    fn test_log_configuration_differences_primary_key_change() {
        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "id",
            arrow_schema::DataType::Int64,
            false,
        )]));
        let stored = make_test_metadata(
            vec![],
            None,
            None,
            "/tmp/test",
            crate::metadata::VortexConfig::default(),
            Arc::clone(&schema),
        );
        let options = CreateTableOptions {
            table_name: "test_table".to_string(),
            schema,
            primary_key: vec!["id".to_string()],
            on_conflict: None,
            base_path: "/tmp/test".to_string(),
            partition_column: None,
            vortex_config: crate::metadata::VortexConfig::default(),
        };
        // Should not panic; exercises the logging path for primary_key change.
        log_configuration_differences("test_table", &stored, &options);
    }

    #[test]
    fn test_log_configuration_differences_on_conflict_change() {
        use datafusion_table_providers::util::on_conflict::OnConflict;
        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "id",
            arrow_schema::DataType::Int64,
            false,
        )]));
        let stored = make_test_metadata(
            vec!["id".to_string()],
            None,
            None,
            "/tmp/test",
            crate::metadata::VortexConfig::default(),
            Arc::clone(&schema),
        );
        let options = CreateTableOptions {
            table_name: "test_table".to_string(),
            schema,
            primary_key: vec!["id".to_string()],
            on_conflict: Some(OnConflict::DoNothingAll),
            base_path: "/tmp/test".to_string(),
            partition_column: None,
            vortex_config: crate::metadata::VortexConfig::default(),
        };
        // Should not panic; exercises the logging path for on_conflict change.
        log_configuration_differences("test_table", &stored, &options);
    }

    #[test]
    fn test_log_configuration_differences_multiple_fields() {
        use datafusion_table_providers::util::on_conflict::OnConflict;
        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "id",
            arrow_schema::DataType::Int64,
            false,
        )]));
        let stored = make_test_metadata(
            vec![],
            None,
            None,
            "/tmp/old",
            crate::metadata::VortexConfig::default(),
            Arc::clone(&schema),
        );
        let changed_vortex = crate::metadata::VortexConfig {
            sort_columns: vec!["id".to_string()],
            ..Default::default()
        };
        let options = CreateTableOptions {
            table_name: "test_table".to_string(),
            schema,
            primary_key: vec!["id".to_string()],
            on_conflict: Some(OnConflict::DoNothingAll),
            base_path: "/tmp/new".to_string(),
            partition_column: Some("region".to_string()),
            vortex_config: changed_vortex,
        };
        // Should not panic; exercises the logging path when many fields change at once.
        log_configuration_differences("test_table", &stored, &options);
    }

    #[tokio::test]
    async fn test_create_table_on_conflict_change_falls_back() {
        use datafusion_table_providers::util::on_conflict::OnConflict;
        let test_db = format!(
            "sqlite://./.test_on_conflict_change_{}.db",
            uuid::Uuid::now_v7()
        );
        let catalog = CayenneCatalog::new(&test_db).expect("Failed to create catalog");
        catalog.init().await.expect("Failed to initialize catalog");

        let schema = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("id", arrow_schema::DataType::Int64, false),
            arrow_schema::Field::new("name", arrow_schema::DataType::Utf8, true),
        ]));

        // Create table without on_conflict
        let options = CreateTableOptions {
            table_name: "oc_table".to_string(),
            schema: Arc::clone(&schema),
            primary_key: vec!["id".to_string()],
            on_conflict: None,
            base_path: "/tmp/cayenne_oc_test".to_string(),
            partition_column: None,
            vortex_config: crate::metadata::VortexConfig::default(),
        };
        let table_id_1 = catalog
            .create_table(options)
            .await
            .expect("Failed to create table");

        // Now try to add on_conflict — should fall back gracefully
        let options_changed = CreateTableOptions {
            table_name: "oc_table".to_string(),
            schema: Arc::clone(&schema),
            primary_key: vec!["id".to_string()],
            on_conflict: Some(OnConflict::DoNothingAll),
            base_path: "/tmp/cayenne_oc_test".to_string(),
            partition_column: None,
            vortex_config: crate::metadata::VortexConfig::default(),
        };
        let table_id_2 = catalog
            .create_table(options_changed)
            .await
            .expect("on_conflict change should fall back gracefully");
        assert_eq!(
            table_id_1, table_id_2,
            "Should return the original table_id when on_conflict changes"
        );

        // Stored metadata should still have original config (no on_conflict)
        let metadata = catalog
            .get_table("oc_table")
            .await
            .expect("Failed to get table");
        assert!(
            metadata.on_conflict.is_none(),
            "Stored on_conflict should remain None"
        );

        // Cleanup
        let db_path = test_db.strip_prefix("sqlite://").unwrap_or(&test_db);
        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(format!("{db_path}-shm"));
        let _ = std::fs::remove_file(format!("{db_path}-wal"));
    }

    #[tokio::test]
    async fn test_validate_existing_table_configuration_returns_error_on_mismatch() {
        let test_db = format!(
            "sqlite://./.test_validate_mismatch_{}.db",
            uuid::Uuid::now_v7()
        );
        let catalog = CayenneCatalog::new(&test_db).expect("Failed to create catalog");
        catalog.init().await.expect("Failed to initialize catalog");

        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "id",
            arrow_schema::DataType::Int64,
            false,
        )]));

        // Create table with no primary key
        let options = CreateTableOptions {
            table_name: "validate_table".to_string(),
            schema: Arc::clone(&schema),
            primary_key: vec![],
            on_conflict: None,
            base_path: "/tmp/cayenne_validate_test".to_string(),
            partition_column: None,
            vortex_config: crate::metadata::VortexConfig::default(),
        };
        catalog
            .create_table(options)
            .await
            .expect("Failed to create table");

        // validate_existing_table_configuration should return ChangedConfiguration
        let changed_options = CreateTableOptions {
            table_name: "validate_table".to_string(),
            schema: Arc::clone(&schema),
            primary_key: vec!["id".to_string()],
            on_conflict: None,
            base_path: "/tmp/cayenne_validate_test".to_string(),
            partition_column: None,
            vortex_config: crate::metadata::VortexConfig::default(),
        };
        let result = catalog
            .validate_existing_table_configuration("validate_table", &changed_options)
            .await;
        assert!(
            matches!(&result, Err(CatalogError::ChangedConfiguration { .. })),
            "Expected ChangedConfiguration error from validate, got: {result:?}"
        );

        // validate_existing_table_configuration should return Ok when config matches
        let same_options = CreateTableOptions {
            table_name: "validate_table".to_string(),
            schema,
            primary_key: vec![],
            on_conflict: None,
            base_path: "/tmp/cayenne_validate_test".to_string(),
            partition_column: None,
            vortex_config: crate::metadata::VortexConfig::default(),
        };
        let result = catalog
            .validate_existing_table_configuration("validate_table", &same_options)
            .await;
        assert!(
            result.is_ok(),
            "Expected Ok when config matches, got: {result:?}"
        );

        // Cleanup
        let db_path = test_db.strip_prefix("sqlite://").unwrap_or(&test_db);
        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(format!("{db_path}-shm"));
        let _ = std::fs::remove_file(format!("{db_path}-wal"));
    }

    #[tokio::test]
    async fn test_validate_existing_table_configuration_allows_configured_footer_cache_drift() {
        let test_db = format!(
            "sqlite://./.test_footer_cache_validate_{}.db",
            uuid::Uuid::now_v7()
        );
        let catalog = CayenneCatalog::new(&test_db).expect("Failed to create catalog");
        catalog.init().await.expect("Failed to initialize catalog");

        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "id",
            arrow_schema::DataType::Int64,
            false,
        )]));

        let options = CreateTableOptions {
            table_name: "footer_cache_validate_table".to_string(),
            schema: Arc::clone(&schema),
            primary_key: vec![],
            on_conflict: None,
            base_path: "/tmp/cayenne_footer_cache_validate_test".to_string(),
            partition_column: None,
            vortex_config: crate::metadata::VortexConfig {
                footer_cache_mb: Some(128),
                ..Default::default()
            },
        };
        catalog
            .create_table(options)
            .await
            .expect("Failed to create table");

        let changed_options = CreateTableOptions {
            table_name: "footer_cache_validate_table".to_string(),
            schema: Arc::clone(&schema),
            primary_key: vec![],
            on_conflict: None,
            base_path: "/tmp/cayenne_footer_cache_validate_test".to_string(),
            partition_column: None,
            vortex_config: crate::metadata::VortexConfig {
                footer_cache_mb: Some(256),
                ..Default::default()
            },
        };
        let result = catalog
            .validate_existing_table_configuration("footer_cache_validate_table", &changed_options)
            .await;
        assert!(
            result.is_ok(),
            "Expected Ok for footer cache runtime tuning drift, got: {result:?}"
        );

        let db_path = test_db.strip_prefix("sqlite://").unwrap_or(&test_db);
        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(format!("{db_path}-shm"));
        let _ = std::fs::remove_file(format!("{db_path}-wal"));
    }
}
