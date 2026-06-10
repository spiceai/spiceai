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
//!
//! [`CayenneCatalog`] is the production implementation of
//! [`MetadataCatalog`](crate::catalog::MetadataCatalog), backed by a
//! crate-private `MetastoreImpl` enum that dispatches to either the
//! embedded `SQLite` backend (`sqlite://`) or the Turso/libSQL backend
//! (`libsql://`, `turso` feature).
//!
//! Module layout: this file holds the [`CayenneCatalog`] type, its
//! constructor, and the `*_in_txn` building blocks used by the
//! cross-partition coordinator; the `MetadataCatalog` trait methods live
//! in `metadata_catalog_impl`, the backend-dispatch enum in
//! `metastore_impl`, and retry/validation/SQL-literal helpers in `util`.

use super::catalog::{CatalogError, CatalogResult, MetadataCatalog, SnapshotSequenceCommit};
use super::metadata::{
    CreateTableOptions, DeleteFile, InlinedData, InlinedDataStats, InlinedDelete,
    PartitionMetadata, PkConflictDetection, SnapshotFileStatistics, TableMetadata, TableStatistics,
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
    /// Returns [`CatalogError::TursoNotEnabled`] if the `libsql://` scheme is
    /// used but the `turso` feature is not enabled.
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
        // `cayenne_insert_record.table_id` is a raw-UUID-bytes BLOB, so it must
        // be matched with a BLOB literal, not the TEXT literal the other tables
        // use (a TEXT literal never equals a BLOB in SQLite).
        let insert_record_table_id_literal = insert_record_table_id_blob_literal(table_id);
        let new_snapshot_id_literal = sql_text_literal(new_snapshot_id);
        let batch_sql = format!(
            "DELETE FROM cayenne_delete_file WHERE table_id = {table_id_literal}; \
             DELETE FROM cayenne_insert_record WHERE table_id = {insert_record_table_id_literal}; \
             DELETE FROM cayenne_snapshot_sequence WHERE table_id = {table_id_literal}; \
             UPDATE cayenne_table SET current_snapshot_id = {new_snapshot_id_literal} WHERE table_id = {table_id_literal};"
        );

        txn.execute_batch(&batch_sql)
            .await
            .map_err(|e| CatalogError::FailedToSetCurrentSnapshot {
                source: Box::new(e),
            })
    }

    /// CAS-validate and swap a subset of protected-snapshot sequence rows
    /// inside the caller's `MetastoreTransaction`, without opening a new one.
    ///
    /// Returns `Ok(false)` (and makes no mutation) if any input snapshot is no
    /// longer present in `cayenne_snapshot_sequence` — the caller should roll
    /// back / discard the rewritten output. An empty `old_snapshot_ids` list
    /// also returns `Ok(false)` without mutating.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError::InvalidOperationNoSource`] if any UUID is
    /// malformed, or propagates the metastore error if a statement fails.
    pub async fn swap_protected_snapshots_in_txn(
        &self,
        txn: &mut dyn MetastoreTransaction,
        table_id: &str,
        old_snapshot_ids: &[String],
        new_snapshot_id: &str,
        new_sequence_number: i64,
    ) -> CatalogResult<bool> {
        // Validate UUIDs to keep the interpolated batch SQL injection-free.
        // All ids are generated internally via `uuid::Uuid::now_v7()`, but we
        // enforce the invariant here since they are interpolated as SQL literals.
        if uuid::Uuid::parse_str(table_id).is_err() {
            return Err(CatalogError::InvalidOperationNoSource {
                message: format!("table_id is not a valid UUID: {table_id}"),
            });
        }
        if uuid::Uuid::parse_str(new_snapshot_id).is_err() {
            return Err(CatalogError::InvalidOperationNoSource {
                message: format!("new_snapshot_id is not a valid UUID: {new_snapshot_id}"),
            });
        }
        for id in old_snapshot_ids {
            if uuid::Uuid::parse_str(id).is_err() {
                return Err(CatalogError::InvalidOperationNoSource {
                    message: format!("old_snapshot_id is not a valid UUID: {id}"),
                });
            }
        }

        if old_snapshot_ids.is_empty() {
            return Ok(false);
        }

        let table_id_literal = sql_text_literal(table_id);
        let id_list = old_snapshot_ids
            .iter()
            .map(|id| sql_text_literal(id))
            .collect::<Vec<_>>()
            .join(", ");

        // CAS guard: every input snapshot must still be active. If a concurrent
        // compaction already consumed one of them, abort without mutating.
        let count_sql = format!(
            "SELECT COUNT(*) FROM cayenne_snapshot_sequence \
             WHERE table_id = {table_id_literal} AND snapshot_id IN ({id_list})"
        );
        let count_values = txn
            .query_row_values(QueryRowParams {
                sql: &count_sql,
                params: vec![],
            })
            .await?;
        let existing = i64::from_value(metastore_value_at(&count_values, 0)?)?;
        if existing != i64::try_from(old_snapshot_ids.len()).unwrap_or(i64::MAX) {
            return Ok(false);
        }

        let new_snapshot_id_literal = sql_text_literal(new_snapshot_id);
        let batch_sql = format!(
            "DELETE FROM cayenne_snapshot_sequence \
                WHERE table_id = {table_id_literal} AND snapshot_id IN ({id_list}); \
             INSERT OR REPLACE INTO cayenne_snapshot_sequence \
                (table_id, snapshot_id, sequence_number) \
                VALUES ({table_id_literal}, {new_snapshot_id_literal}, {new_sequence_number});"
        );
        txn.execute_batch(&batch_sql).await?;
        Ok(true)
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
    /// 1. `DELETE FROM cayenne_delete_file              WHERE table_id = ?`
    /// 2. `DELETE FROM cayenne_insert_record            WHERE table_id = ?`
    /// 3. `DELETE FROM cayenne_snapshot_sequence        WHERE table_id = ?`
    /// 4. `DELETE FROM cayenne_inlined_data             WHERE table_id = ?`
    /// 5. `DELETE FROM cayenne_inlined_delete           WHERE table_id = ?`
    /// 6. `DELETE FROM cayenne_table_statistics         WHERE table_id = ?`
    /// 7. `DELETE FROM cayenne_snapshot_file_statistics WHERE table_id = ?`
    /// 8. `DELETE FROM cayenne_pk_index                 WHERE table_id = ?`
    /// 9. `UPDATE cayenne_table SET current_snapshot_id = ? WHERE table_id = ?`
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
        // `cayenne_insert_record.table_id` is a raw-UUID-bytes BLOB, so it must
        // be matched with a BLOB literal, not the TEXT literal the other tables
        // use (a TEXT literal never equals a BLOB in SQLite).
        let insert_record_table_id_literal = insert_record_table_id_blob_literal(table_id);
        let new_snapshot_id_literal = sql_text_literal(new_snapshot_id);
        let batch_sql = format!(
            "DELETE FROM cayenne_delete_file WHERE table_id = {table_id_literal}; \
             DELETE FROM cayenne_insert_record WHERE table_id = {insert_record_table_id_literal}; \
             DELETE FROM cayenne_snapshot_sequence WHERE table_id = {table_id_literal}; \
             DELETE FROM cayenne_inlined_data WHERE table_id = {table_id_literal}; \
             DELETE FROM cayenne_inlined_delete WHERE table_id = {table_id_literal}; \
             DELETE FROM cayenne_table_statistics WHERE table_id = {table_id_literal}; \
             DELETE FROM cayenne_snapshot_file_statistics WHERE table_id = {table_id_literal}; \
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
             (table_id, pk_bytes, sequence_number) VALUES ";
        // Each "(?N, ?N, ?N)" row is ≤ 28 bytes for the placeholder counts we hit.
        let mut sql = String::with_capacity(PREFIX.len() + pk_bytes_list.len() * 28);
        sql.push_str(PREFIX);
        let mut params = Vec::with_capacity(pk_bytes_list.len() * 3);

        for (i, pk_bytes) in pk_bytes_list.iter().enumerate() {
            let base = i * 3 + 1; // SQLite params are 1-indexed
            if i > 0 {
                sql.push_str(", ");
            }
            // `write!` into a `String` is infallible.
            let _ = write!(sql, "(?{}, ?{}, ?{})", base, base + 1, base + 2);
            params.push(insert_record_table_id_value(table_id));
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

    /// Build a single batched `UPDATE … SET published = 1 WHERE table_id = ?
    /// AND inlined_id IN (?, ?, …)` for a chunk of deferred tombstone flips
    /// (cycle-8 TASK D4).
    ///
    /// Exactly equivalent to running one `UPDATE … WHERE inlined_id = ?` per id
    /// (each flip is an idempotent set-to-1; the order of the rows updated is
    /// irrelevant since every match lands the same constant), but collapses N
    /// per-row writer round-trips into one statement — shrinking both the
    /// statement count and the WAL frame churn the folded Stage-A txn holds the
    /// writer across. `table_id` binds once as `?1`; each id is `?2..`.
    fn build_flip_published_chunk_sql(
        table_id: &str,
        inlined_ids: &[String],
    ) -> (String, Vec<MetastoreValue>) {
        use std::fmt::Write as _;

        const PREFIX: &str = "UPDATE cayenne_inlined_delete SET published = 1 \
             WHERE table_id = ?1 AND inlined_id IN (";
        let mut sql = String::with_capacity(PREFIX.len() + inlined_ids.len() * 6 + 1);
        sql.push_str(PREFIX);
        let mut params = Vec::with_capacity(inlined_ids.len() + 1);
        params.push(MetastoreValue::Text(table_id.to_string()));

        for (i, inlined_id) in inlined_ids.iter().enumerate() {
            // `table_id` took ?1, so ids start at ?2.
            let placeholder = i + 2;
            if i > 0 {
                sql.push_str(", ");
            }
            let _ = write!(sql, "?{placeholder}");
            params.push(MetastoreValue::Text(inlined_id.clone()));
        }
        sql.push(')');

        (sql, params)
    }
}

mod metadata_catalog_impl;
mod metastore_impl;
mod util;

pub(crate) use metastore_impl::MetastoreImpl;
pub use util::is_retryable_write_conflict;

use metastore_impl::{
    ExistingDeleteFileRecord, existing_delete_file_record_from_values, metastore_value_at,
};
use util::{
    configuration_matches, ensure_snapshot_directory_exists, insert_record_table_id_blob_literal,
    insert_record_table_id_value, is_delete_file_unique_constraint_violation_message,
    is_partition_unique_constraint_violation_message, is_query_returned_no_rows,
    log_configuration_differences, log_runtime_footer_cache_drift,
    retry_on_metastore_write_conflict, should_retry_metastore_write_conflict,
    sleep_before_metastore_write_retry, sql_text_literal, validate_create_table_options,
    validate_existing_delete_file_record,
};

#[cfg(test)]
mod tests;
