//! The [`MetadataCatalog`] trait implementation for [`CayenneCatalog`].
//!
//! Every method here translates a catalog operation into parameterized SQL
//! against the `cayenne_*` metastore tables via [`MetastoreImpl`]. Behavior
//! contracts (atomicity, retryability, sequence ordering) are documented on
//! the trait in `crate::catalog`; this module adds the backend specifics:
//! `SQLite` parameter-limit chunking, retry-with-backoff loops around
//! multi-statement transactions, and idempotent handling of concurrent
//! insert races. Reusable SQL builders live on `CayenneCatalog` in `mod.rs`;
//! shared helpers in `util`.

use super::{
    Arc, CatalogError, CatalogResult, CayenneCatalog, CreateTableOptions,
    DEFAULT_CONCURRENT_WRITE_MAX_ATTEMPTS, DeleteFile, ExecuteParams, HashMap, InlinedData,
    InlinedDataStats, InlinedDelete, MetadataCatalog, MetastoreBackend, MetastoreImpl,
    MetastoreValue, PartitionMetadata, Path, QueryParams, QueryRowParams, SnapshotFileStatistics,
    SnapshotSequenceCommit, TableMetadata, TableStatistics, async_trait,
    ensure_snapshot_directory_exists, insert_record_table_id_value,
    is_delete_file_unique_constraint_violation_message,
    is_partition_unique_constraint_violation_message, is_query_returned_no_rows,
    is_retryable_write_conflict, retry_backoff_delay, retry_on_metastore_write_conflict,
    should_retry_metastore_write_conflict, sleep_before_metastore_write_retry, sql_text_literal,
    validate_create_table_options, validate_existing_delete_file_record,
};

#[async_trait]
impl MetadataCatalog for CayenneCatalog {
    // ───── Table lifecycle ─────
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

    /// cycle-5 TASK 2b: drain the WAL off the hot path with a non-blocking
    /// PASSIVE checkpoint (delegated to the backend).
    async fn checkpoint_wal(&self) -> CatalogResult<()> {
        self.metastore.checkpoint_wal().await
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
                        crate::metadata::VortexConfig::default()
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

    // ───── Snapshots ─────
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

    // ───── Delete files & deletion vectors ─────
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

    // ───── Sequence numbers ─────
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

        // De-`BEGIN IMMEDIATE` the reservation (cycle-4 lever (e)). The reservation
        // is a SINGLE read-modify-write statement that touches only the table's
        // control row:
        //   `UPDATE cayenne_table SET current_sequence_number =
        //        current_sequence_number + ?2 WHERE table_id = ?1
        //    RETURNING current_sequence_number`
        // Previously this ran inside an explicit `begin_transaction()` whose
        // `BEGIN IMMEDIATE` (sqlite.rs:952) grabs the per-table WAL *reserved write
        // lock* and HOLDS it across BEGIN → UPDATE → COMMIT. On a heavy-upsert
        // table that lock is contended by the same table's spawned Stage-B
        // finalize and by the next batch's Stage-A folded transaction, so the
        // reservation busy-waits for the whole alternation window (measured 74 ms
        // as `stage_seq_reserve`).
        //
        // SQLite executes a bare AUTOCOMMIT `UPDATE … RETURNING` as one atomic
        // statement: it takes the write lock, performs the read-modify-write, and
        // RELEASES the lock the instant the statement completes — no held
        // transaction window. Two concurrent autocommit reservations are still
        // serialized by the single WAL writer and each observes the other's
        // committed increment (the read-modify-write is atomic WITHIN the
        // statement, identical isolation to wrapping it in `BEGIN IMMEDIATE`).
        // So this is PROVABLY EQUIVALENT under concurrency while removing one full
        // reserved-lock acquire/hold cycle per staged batch. The same retry-on-
        // `SQLITE_BUSY` loop is preserved (a single autocommit statement can still
        // return `SQLITE_BUSY` if it cannot acquire the writer within
        // `busy_timeout`). Turso routes `query_row` the same single-statement way.
        for attempt in 1..=max_attempts {
            let new_high = match self
                .metastore
                .query_row_helper(
                    QueryRowParams {
                        sql: "UPDATE cayenne_table SET current_sequence_number = current_sequence_number + ?2 WHERE table_id = ?1 RETURNING current_sequence_number",
                        params: vec![
                            MetastoreValue::Text(table_id.to_string()),
                            MetastoreValue::Integer(delta),
                        ],
                    },
                    |row| row.get_i64(0),
                )
                .await
            {
                Ok(new_high) => new_high,
                Err(e) => {
                    if should_retry_metastore_write_conflict(&e, attempt, max_attempts) {
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

            // The reserved block is [new_high - delta + 1, new_high].
            return Ok(new_high - delta + 1);
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

    // ───── Insert records ─────
    async fn add_insert_record(
        &self,
        table_id: &str,
        pk_bytes: Vec<u8>,
        sequence_number: i64,
    ) -> CatalogResult<()> {
        // Use INSERT OR REPLACE to update sequence if the (table_id, pk_bytes)
        // composite PK already exists.
        self.metastore
            .execute_helper(ExecuteParams {
                sql: "INSERT OR REPLACE INTO cayenne_insert_record (table_id, pk_bytes, sequence_number) VALUES (?1, ?2, ?3)",
                params: vec![
                    insert_record_table_id_value(table_id),
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
        // 32 766) parameters per prepared statement.  Each row needs 3 params
        // (table_id, pk_bytes, sequence_number), so we chunk the list to stay
        // within the limit.
        //
        // All chunks are wrapped in a single transaction so the operation is
        // atomic: either every chunk is applied or none is.
        const PARAMS_PER_ROW: usize = 3;
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
                    params: vec![insert_record_table_id_value(table_id)],
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
                params: vec![insert_record_table_id_value(table_id)],
            })
            .await
            .map_err(|e| CatalogError::InvalidOperation {
                message: "Failed to clear insert records".to_string(),
                source: Box::new(e),
            })?;
        Ok(())
    }

    // ───── Snapshot sequences & commits ─────
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

    async fn swap_protected_snapshots(
        &self,
        table_id: &str,
        old_snapshot_ids: &[String],
        new_snapshot_id: &str,
        new_sequence_number: i64,
    ) -> CatalogResult<bool> {
        let max_attempts = DEFAULT_CONCURRENT_WRITE_MAX_ATTEMPTS;
        if max_attempts == 0 {
            return Err(CatalogError::InvalidOperationNoSource {
                message: "swap_protected_snapshots requires at least one attempt".to_string(),
            });
        }

        for attempt in 1..=max_attempts {
            let mut tx = self.begin_transaction().await.map_err(|e| {
                CatalogError::FailedToSetCurrentSnapshot {
                    source: Box::new(e),
                }
            })?;

            match self
                .swap_protected_snapshots_in_txn(
                    &mut *tx,
                    table_id,
                    old_snapshot_ids,
                    new_snapshot_id,
                    new_sequence_number,
                )
                .await
            {
                // CAS guard failed — an input snapshot is no longer active.
                // The transaction made no changes; nothing to commit.
                Ok(false) => return Ok(false),
                Ok(true) => match tx.commit().await {
                    Ok(()) => return Ok(true),
                    Err(e) if attempt < max_attempts && is_retryable_write_conflict(&e) => {
                        let delay = retry_backoff_delay(attempt);
                        tracing::debug!(
                            attempt,
                            max_attempts,
                            ?delay,
                            "Retrying protected-snapshot swap after commit conflict"
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
                "swap_protected_snapshots exhausted {max_attempts} attempts without success or a terminal error"
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

    // ───── Partitions ─────
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

    // ───── Statistics ─────
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

    async fn upsert_snapshot_file_statistics(
        &self,
        stats: &SnapshotFileStatistics,
    ) -> CatalogResult<()> {
        self.metastore
            .execute_helper(ExecuteParams {
                sql: "INSERT OR REPLACE INTO cayenne_snapshot_file_statistics \
                      (table_id, snapshot_id, file_path, file_size_bytes, num_rows, statistics_blob) \
                      VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params: vec![
                    MetastoreValue::Text(stats.table_id.clone()),
                    MetastoreValue::Text(stats.snapshot_id.clone()),
                    MetastoreValue::Text(stats.file_path.clone()),
                    MetastoreValue::Integer(stats.file_size_bytes),
                    MetastoreValue::Integer(stats.num_rows),
                    MetastoreValue::Blob(stats.statistics_blob.clone()),
                ],
            })
            .await
    }

    async fn get_snapshot_file_statistics(
        &self,
        table_id: &str,
        snapshot_id: &str,
        file_path: &str,
    ) -> CatalogResult<Option<SnapshotFileStatistics>> {
        let results = self
            .metastore
            .query_helper(
                QueryParams {
                    sql: r"
                    SELECT table_id, snapshot_id, file_path, file_size_bytes, num_rows, statistics_blob
                    FROM cayenne_snapshot_file_statistics
                    WHERE table_id = ?1 AND snapshot_id = ?2 AND file_path = ?3
                    ",
                    params: vec![
                        MetastoreValue::Text(table_id.to_string()),
                        MetastoreValue::Text(snapshot_id.to_string()),
                        MetastoreValue::Text(file_path.to_string()),
                    ],
                },
                |row| {
                    Ok(SnapshotFileStatistics {
                        table_id: row.get_string(0)?,
                        snapshot_id: row.get_string(1)?,
                        file_path: row.get_string(2)?,
                        file_size_bytes: row.get_i64(3)?,
                        num_rows: row.get_i64(4)?,
                        statistics_blob: row.get_blob(5)?,
                    })
                },
            )
            .await?;
        Ok(results.into_iter().next())
    }

    async fn clear_snapshot_file_statistics_except(
        &self,
        table_id: &str,
        snapshot_id: &str,
    ) -> CatalogResult<()> {
        self.metastore
            .execute_helper(ExecuteParams {
                sql: "DELETE FROM cayenne_snapshot_file_statistics \
                      WHERE table_id = ?1 AND snapshot_id != ?2",
                params: vec![
                    MetastoreValue::Text(table_id.to_string()),
                    MetastoreValue::Text(snapshot_id.to_string()),
                ],
            })
            .await
    }

    async fn clear_snapshot_file_statistics(&self, table_id: &str) -> CatalogResult<()> {
        self.metastore
            .execute_helper(ExecuteParams {
                sql: "DELETE FROM cayenne_snapshot_file_statistics WHERE table_id = ?1",
                params: vec![MetastoreValue::Text(table_id.to_string())],
            })
            .await
    }

    // ───── PK index ─────
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

    // ───── Inlined data ─────
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

    async fn get_inlined_data_above_sequence(
        &self,
        table_id: &str,
        after_sequence: i64,
    ) -> CatalogResult<Vec<InlinedData>> {
        // Incremental inline-cache read path: same projection/order as
        // `get_inlined_data` but pushes the `sequence_number > ?2` predicate into
        // SQL so an append-only cache refresh ships ONLY the freshly committed
        // rows, never the whole corpus's `data_ipc` blobs. Exactly equivalent to
        // `get_inlined_data` filtered to `sequence_number > after_sequence`.
        self.metastore
            .query_helper(
                QueryParams {
                    sql: r"
                    SELECT inlined_id, table_id, partition_key, data_ipc, record_count, sequence_number, created_at
                    FROM cayenne_inlined_data
                    WHERE table_id = ?1 AND sequence_number > ?2
                    ORDER BY sequence_number
                    ",
                    params: vec![
                        MetastoreValue::Text(table_id.to_string()),
                        MetastoreValue::Integer(after_sequence),
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

    // ───── Inlined deletes & mutations ─────
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
                    (inlined_id, table_id, delete_ipc, delete_count, sequence_number, published)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                ",
                params: vec![
                    MetastoreValue::Text(inlined_id.clone()),
                    MetastoreValue::Text(delete.table_id),
                    MetastoreValue::Blob(delete.delete_ipc),
                    MetastoreValue::Integer(delete.delete_count),
                    MetastoreValue::Integer(delete.sequence_number),
                    MetastoreValue::Integer(i64::from(delete.published)),
                ],
            })
            .await?;
        Ok(inlined_id)
    }

    async fn mark_inlined_delete_published(
        &self,
        table_id: &str,
        inlined_id: &str,
    ) -> CatalogResult<()> {
        self.metastore
            .execute_helper(ExecuteParams {
                sql: "UPDATE cayenne_inlined_delete SET published = 1 \
                      WHERE table_id = ?1 AND inlined_id = ?2",
                params: vec![
                    MetastoreValue::Text(table_id.to_string()),
                    MetastoreValue::Text(inlined_id.to_string()),
                ],
            })
            .await
    }

    async fn mark_inlined_delete_unpublished(
        &self,
        table_id: &str,
        inlined_id: &str,
    ) -> CatalogResult<()> {
        self.metastore
            .execute_helper(ExecuteParams {
                sql: "UPDATE cayenne_inlined_delete SET published = 0 \
                      WHERE table_id = ?1 AND inlined_id = ?2",
                params: vec![
                    MetastoreValue::Text(table_id.to_string()),
                    MetastoreValue::Text(inlined_id.to_string()),
                ],
            })
            .await
    }

    async fn publish_orphan_inlined_deletes(&self, table_id: &str) -> CatalogResult<u64> {
        // Count the unpublished tombstones first (the execute helper returns no
        // affected-row count). The `published = 0` partial index
        // (`idx_cayenne_inlined_delete_unpublished`) lets both the COUNT and the
        // UPDATE seek straight to the in-flight rows instead of scanning every
        // tombstone for the table under the `(table_id, sequence_number)` index.
        let pending: i64 = self
            .metastore
            .query_row_helper(
                QueryRowParams {
                    sql: "SELECT COUNT(*) FROM cayenne_inlined_delete \
                          WHERE table_id = ?1 AND published = 0",
                    params: vec![MetastoreValue::Text(table_id.to_string())],
                },
                |row| row.get_i64(0),
            )
            .await?;

        if pending > 0 {
            self.metastore
                .execute_helper(ExecuteParams {
                    sql: "UPDATE cayenne_inlined_delete SET published = 1 \
                          WHERE table_id = ?1 AND published = 0",
                    params: vec![MetastoreValue::Text(table_id.to_string())],
                })
                .await?;
        }

        Ok(u64::try_from(pending).unwrap_or(0))
    }

    async fn commit_inlined_mutation(
        &self,
        table_id: &str,
        updated_data: Vec<InlinedData>,
        deleted_inlined_ids: Vec<String>,
        data: Vec<InlinedData>,
        assigned_sequence: i64,
    ) -> CatalogResult<Option<i64>> {
        if updated_data.is_empty() && deleted_inlined_ids.is_empty() && data.is_empty() {
            return Ok(None);
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

        // The appended rows are stamped with the caller-supplied
        // `assigned_sequence` (lever B2). `None` when no rows are appended, so
        // the caller does not advance its visibility watermark for a no-op.
        let appended_sequence = (!data.is_empty()).then_some(assigned_sequence);
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

            // Lever B2: NO counter mutation here. Allocation moved to the
            // in-memory `SeqAllocator` on the provider; the DB high-water is kept
            // at-or-ahead by the allocator's reserve-ahead refill, so the
            // appended row is stamped directly from `assigned_sequence` below
            // (a bound parameter) instead of bumping + reading back the counter.

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
                    VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                    ",
                    params: vec![
                        MetastoreValue::Text(inlined_id),
                        MetastoreValue::Text(table_id.to_string()),
                        data_entry.partition_key.clone().into(),
                        MetastoreValue::Blob(data_entry.data_ipc.clone()),
                        MetastoreValue::Integer(data_entry.record_count),
                        // Lever B2: stamp the caller-allocated sequence directly,
                        // replacing the prior correlated subquery read of the DB
                        // counter (which no longer moves inside this txn).
                        MetastoreValue::Integer(assigned_sequence),
                    ],
                })
                .await
                .map_err(|e| CatalogError::InvalidOperation {
                    message: "Failed to execute inline mutation transaction".to_string(),
                    source: Box::new(e),
                })?;
            }

            match tx.commit().await {
                Ok(()) => return Ok(appended_sequence),
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

            if let Some(snapshot_sequence) = &snapshot_sequence
                && let Err(e) = tx
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
                    message: "Failed to insert snapshot sequence inside on-conflict transaction"
                        .to_string(),
                    source: Box::new(e),
                });
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

    async fn commit_on_conflict_deletions_with_tombstone(
        &self,
        delete_files: Vec<DeleteFile>,
        table_id: &str,
        insert_pk_bytes_list: Vec<Vec<u8>>,
        insert_sequence: i64,
        snapshot_sequence: Option<SnapshotSequenceCommit>,
        inline_tombstone: Option<InlinedDelete>,
        pending_durable_flips: &[String],
    ) -> CatalogResult<Option<String>> {
        // Single-transaction Stage-A fold (cycle-3 metastore lever): identical
        // statement set + order to `commit_on_conflict_deletions`, with the
        // inline tombstone INSERT (Option D, `published = false`) appended as the
        // last statement BEFORE commit. The two writes previously acquired the
        // process-wide SQLite writer twice (one txn for the deletion metadata,
        // one for the tombstone); folding them into one BEGIN..COMMIT removes a
        // full writer round-trip per staged upsert batch on heavy-upsert tables.
        //
        // The retry-on-conflict control flow (`drop(tx); sleep; continue
        // 'attempts`) is preserved verbatim from `commit_on_conflict_deletions`
        // because a `SQLITE_BUSY`/`SQLITE_LOCKED` mid-transaction requires
        // re-opening a fresh transaction — it cannot be factored out without
        // losing the re-`begin_transaction` semantics.

        // SQLite param limit chunking (mirrors commit_on_conflict_deletions).
        const PARAMS_PER_ROW: usize = 4;
        const MAX_PARAMS: usize = 32_000;
        const MAX_ROWS_PER_CHUNK: usize = MAX_PARAMS / PARAMS_PER_ROW;

        const DELETE_FILE_PARAMS_PER_ROW: usize = 9;
        const MAX_DELETE_FILE_ROWS_PER_CHUNK: usize = MAX_PARAMS / DELETE_FILE_PARAMS_PER_ROW;

        // cycle-8 TASK D4: deferred-flip UPDATE chunking. The batched UPDATE binds
        // one shared `table_id` plus one `inlined_id` per flip, so a chunk of K
        // flips uses `1 + K` params. Cap K so the bind count stays under
        // `MAX_PARAMS` (the `- 1` reserves the slot for `table_id`).
        const MAX_FLIP_ROWS_PER_CHUNK: usize = MAX_PARAMS - 1;

        // Fast path: nothing to commit AND no tombstone AND no deferred flips —
        // identical short-circuit to `commit_on_conflict_deletions`. A batch with
        // ONLY deferred flips (b1★ drain riding an otherwise-empty staged commit)
        // still falls through so the flip UPDATEs run.
        if delete_files.is_empty()
            && insert_pk_bytes_list.is_empty()
            && snapshot_sequence.is_none()
            && inline_tombstone.is_none()
            && pending_durable_flips.is_empty()
        {
            return Ok(None);
        }

        // Generate the tombstone id up front (outside the retry loop) so a retry
        // re-INSERTs the SAME id rather than minting a new one each attempt —
        // the whole transaction rolls back on conflict, so only the committed
        // attempt's id ever lands, and the caller gets a stable id to flip.
        let tombstone_with_id = inline_tombstone.map(|mut tombstone| {
            if tombstone.inlined_id.is_empty() {
                tombstone.inlined_id = uuid::Uuid::now_v7().to_string();
            }
            tombstone
        });
        let inlined_id = tombstone_with_id
            .as_ref()
            .map(|tombstone| tombstone.inlined_id.clone());

        // Same up-front validation as `commit_on_conflict_deletions`.
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
                        "begin on-conflict deletion (with tombstone) transaction",
                    )
                    .await
                    {
                        continue 'attempts;
                    }
                    return Err(CatalogError::InvalidOperation {
                        message:
                            "Failed to begin on-conflict deletion (with tombstone) transaction"
                                .to_string(),
                        source: Box::new(e),
                    });
                }
            };

            for chunk in delete_files.chunks(MAX_DELETE_FILE_ROWS_PER_CHUNK) {
                let (sql, params) = Self::build_insert_delete_files_chunk_sql(chunk);
                let res = tx.execute(ExecuteParams { sql: &sql, params }).await;
                if let Err(e) = res {
                    if should_retry_metastore_write_conflict(&e, attempt, max_attempts) {
                        drop(tx);
                        sleep_before_metastore_write_retry(
                            attempt,
                            max_attempts,
                            "insert delete file chunk inside on-conflict (with tombstone) transaction",
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
                                    "insert delete file inside on-conflict (with tombstone) transaction",
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
                                        "Delete-file metadata conflicts with an existing row inside on-conflict (with tombstone) transaction"
                                            .to_string(),
                                    source: Box::new(validation_error),
                                });
                            }
                            return Err(CatalogError::InvalidOperation {
                                message:
                                    "Failed to insert delete file inside on-conflict (with tombstone) transaction"
                                        .to_string(),
                                source: Box::new(e),
                            });
                        }
                    }
                }
            }

            for chunk in insert_pk_bytes_list.chunks(MAX_ROWS_PER_CHUNK) {
                let (sql, params) =
                    Self::build_insert_records_chunk_sql(table_id, chunk, insert_sequence);
                if let Err(e) = tx.execute(ExecuteParams { sql: &sql, params }).await {
                    if should_retry_metastore_write_conflict(&e, attempt, max_attempts) {
                        drop(tx);
                        sleep_before_metastore_write_retry(
                            attempt,
                            max_attempts,
                            "insert insert-record chunk inside on-conflict (with tombstone) transaction",
                        )
                        .await;
                        continue 'attempts;
                    }
                    drop(tx);
                    return Err(CatalogError::InvalidOperation {
                        message:
                            "Failed to insert insert-record chunk inside on-conflict (with tombstone) transaction"
                                .to_string(),
                        source: Box::new(e),
                    });
                }
            }

            if let Some(snapshot_sequence) = &snapshot_sequence
                && let Err(e) = tx
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
                        "insert snapshot sequence inside on-conflict (with tombstone) transaction",
                    )
                    .await;
                    continue 'attempts;
                }
                drop(tx);
                return Err(CatalogError::InvalidOperation {
                    message: "Failed to insert snapshot sequence inside on-conflict (with tombstone) transaction"
                        .to_string(),
                    source: Box::new(e),
                });
            }

            // Append the inline tombstone INSERT as the LAST statement before
            // commit — same SQL/params as `add_inlined_delete`, with the id
            // pre-generated above so retries are idempotent. Preserves the
            // pre-fold ordering (the tombstone was written immediately after
            // `commit_on_conflict_deletions` returned) while making the two
            // writes one atomic, single-writer-acquisition transaction.
            if let Some(tombstone) = &tombstone_with_id
                && let Err(e) = tx
                    .execute(ExecuteParams {
                        sql: r"
                        INSERT INTO cayenne_inlined_delete
                            (inlined_id, table_id, delete_ipc, delete_count, sequence_number, published)
                        VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                        ",
                        params: vec![
                            MetastoreValue::Text(tombstone.inlined_id.clone()),
                            MetastoreValue::Text(tombstone.table_id.clone()),
                            MetastoreValue::Blob(tombstone.delete_ipc.clone()),
                            MetastoreValue::Integer(tombstone.delete_count),
                            MetastoreValue::Integer(tombstone.sequence_number),
                            MetastoreValue::Integer(i64::from(tombstone.published)),
                        ],
                    })
                    .await
            {
                if should_retry_metastore_write_conflict(&e, attempt, max_attempts) {
                    drop(tx);
                    sleep_before_metastore_write_retry(
                        attempt,
                        max_attempts,
                        "insert inline tombstone inside on-conflict (with tombstone) transaction",
                    )
                    .await;
                    continue 'attempts;
                }
                drop(tx);
                return Err(CatalogError::InvalidOperation {
                    message: "Failed to insert inline tombstone inside on-conflict (with tombstone) transaction"
                        .to_string(),
                    source: Box::new(e),
                });
            }

            // b1★: apply any DEFERRED durable `published = 1` flips from previously-
            // finalized tombstones as the LAST statements before commit, so they
            // ride THIS batch's single `BEGIN IMMEDIATE` acquisition (no extra
            // writer round-trip). cycle-8 TASK D4: BATCHED into one
            // `… WHERE inlined_id IN (…)` per MAX_PARAMS chunk (mirrors the
            // delete-file/insert-record chunking) instead of one round-trip per
            // id, so a large drained-flip batch holds the writer across far fewer
            // statements. Still idempotent (set-to-1) and a no-op for rows already
            // flipped or cleared by a checkpoint; the IN-list is order-independent.
            let mut flip_retry = false;
            for chunk in pending_durable_flips.chunks(MAX_FLIP_ROWS_PER_CHUNK) {
                let (sql, params) = Self::build_flip_published_chunk_sql(table_id, chunk);
                let res = tx.execute(ExecuteParams { sql: &sql, params }).await;
                if let Err(e) = res {
                    if should_retry_metastore_write_conflict(&e, attempt, max_attempts) {
                        flip_retry = true;
                        break;
                    }
                    drop(tx);
                    return Err(CatalogError::InvalidOperation {
                        message: "Failed to apply deferred tombstone flip chunk inside on-conflict (with tombstone) transaction"
                            .to_string(),
                        source: Box::new(e),
                    });
                }
            }
            if flip_retry {
                drop(tx);
                sleep_before_metastore_write_retry(
                    attempt,
                    max_attempts,
                    "apply deferred tombstone flip chunk inside on-conflict (with tombstone) transaction",
                )
                .await;
                continue 'attempts;
            }

            match tx.commit().await {
                Ok(()) => return Ok(inlined_id),
                Err(e) => {
                    if retry_on_metastore_write_conflict(
                        &e,
                        attempt,
                        max_attempts,
                        "commit on-conflict deletion (with tombstone) transaction",
                    )
                    .await
                    {
                        continue 'attempts;
                    }
                    return Err(CatalogError::InvalidOperation {
                        message:
                            "Failed to commit on-conflict deletion (with tombstone) transaction"
                                .to_string(),
                        source: Box::new(e),
                    });
                }
            }
        }

        Err(CatalogError::InvalidOperationNoSource {
            message: format!(
                "commit_on_conflict_deletions_with_tombstone exhausted {max_attempts} retry attempts after retryable write conflicts"
            ),
        })
    }

    async fn get_inlined_deletes(&self, table_id: &str) -> CatalogResult<Vec<InlinedDelete>> {
        self.metastore
            .query_helper(
                QueryParams {
                    sql: r"
                    SELECT inlined_id, table_id, delete_ipc, delete_count, sequence_number, created_at, published
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
                        published: row.get_bool(6)?,
                    })
                },
            )
            .await
    }

    async fn get_published_inlined_deletes(
        &self,
        table_id: &str,
    ) -> CatalogResult<Vec<InlinedDelete>> {
        // Hot read path: same shape as `get_inlined_deletes` but pushes the
        // `published = 1` gate into SQL so unpublished tombstones' expensive
        // `delete_ipc` blobs are never materialised/shipped only to be skipped in
        // memory. `published = 1` is exactly the complement of the Rust
        // `!delete.published` skip, so the no-transient-PK-vanish gate is
        // preserved. Seeks via the complement of the
        // `idx_cayenne_inlined_delete_unpublished` partial index.
        self.metastore
            .query_helper(
                QueryParams {
                    sql: r"
                    SELECT inlined_id, table_id, delete_ipc, delete_count, sequence_number, created_at, published
                    FROM cayenne_inlined_delete
                    WHERE table_id = ?1 AND published = 1
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
                        published: row.get_bool(6)?,
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

    // ───── Table drop & dataset import/export ─────
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

        // Delete all related metadata in order. `cayenne_insert_record` no
        // longer has a foreign key (its `table_id` is a raw-bytes BLOB; see
        // `insert_record_table_id_value`), so it must be cleared explicitly
        // here rather than via ON DELETE CASCADE.
        // 1. Delete insert records
        self.metastore
            .execute_helper(ExecuteParams {
                sql: "DELETE FROM cayenne_insert_record WHERE table_id = ?1",
                params: vec![insert_record_table_id_value(&table_id)],
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

        // 5b. Delete per-file snapshot statistics
        self.metastore
            .execute_helper(ExecuteParams {
                sql: "DELETE FROM cayenne_snapshot_file_statistics WHERE table_id = ?1",
                params: vec![MetastoreValue::Text(table_id.clone())],
            })
            .await
            .map_err(|e| CatalogError::InvalidOperation {
                message: "Failed to delete snapshot file statistics.".to_string(),
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
