//! Free-function helpers for the [`CayenneCatalog`] implementation:
//! retryable-write-conflict classification and retry/backoff plumbing,
//! delete-file and create-table validation, configuration-drift logging,
//! SQL literal/value encoding for the interpolated batch paths, and the
//! snapshot-directory durability helper. Everything here is `pub(super)`
//! except [`is_retryable_write_conflict`], which is re-exported at the
//! crate root for external retry loops.

use super::{
    CatalogError, CatalogResult, CreateTableOptions, DeleteFile, ExistingDeleteFileRecord,
    MetastoreValue, OnConflict, PkConflictDetection, TableMetadata,
    is_retryable_write_conflict_message, retry_backoff_delay,
};

/// Returns `true` if the given catalog error looks like a transient write
/// conflict (`SQLITE_BUSY`, `SQLITE_LOCKED`, or the equivalent Turso
/// `BEGIN CONCURRENT` write-conflict at commit time).
///
/// Drives the internal retry loops of the catalog's transactional commit
/// methods (`commit_compaction`, `commit_overwrite`,
/// `swap_protected_snapshots`, `commit_inlined_mutation`, the on-conflict
/// commits, and `reserve_sequence_numbers`), and is re-exported as
/// `cayenne::is_retryable_write_conflict` for the cross-partition
/// coordinator (`CayennePartitionedInsertStrategy` in the runtime crate,
/// issue #10125) to retry batched `*_in_txn` transactions — the `*_in_txn`
/// building blocks themselves do NOT retry.
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

pub(super) fn is_retryable_sqlite_error(error: &rusqlite::Error) -> bool {
    matches!(
        error,
        rusqlite::Error::SqliteFailure(err, _)
            if matches!(
                err.code,
                rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked
            )
    )
}

pub(super) fn is_query_returned_no_rows(error: &CatalogError) -> bool {
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

pub(super) async fn retry_on_metastore_write_conflict(
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

pub(super) fn should_retry_metastore_write_conflict(
    error: &CatalogError,
    attempt: u32,
    max_attempts: u32,
) -> bool {
    attempt < max_attempts && is_retryable_write_conflict(error)
}

pub(super) async fn sleep_before_metastore_write_retry(
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

pub(super) fn validate_existing_delete_file_record(
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

pub(super) fn is_delete_file_unique_constraint_violation_message(message: &str) -> bool {
    constraint_violation_message_contains_all(
        message,
        &["unique", "cayenne_delete_file", "table_id", "path"],
    ) || constraint_violation_message_contains_all(message, &["idx_cayenne_delete_file_table_path"])
}

pub(super) fn is_partition_unique_constraint_violation_message(message: &str) -> bool {
    constraint_violation_message_contains_all(
        message,
        &["unique", "cayenne_partition", "table_id", "partition_key"],
    )
}

pub(super) fn constraint_violation_message_contains_all(
    message: &str,
    required_parts: &[&str],
) -> bool {
    let normalized = message.to_ascii_lowercase();
    required_parts.iter().all(|part| normalized.contains(part))
}

pub(super) fn sql_text_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

/// Encode a `table_id` UUID string as the compact `BLOB` key value bound into
/// `cayenne_insert_record` (the 16 raw UUID bytes, not the 36-char text). See
/// [`crate::metastore::table_id_to_key_bytes`] for the encoding contract and
/// why this cuts WAL volume on hot upsert bursts.
pub(super) fn insert_record_table_id_value(table_id: &str) -> MetastoreValue {
    MetastoreValue::Blob(crate::metastore::table_id_to_key_bytes(table_id))
}

/// SQL `BLOB` literal (`x'<hex>'`) of the `cayenne_insert_record` `table_id`
/// key for the batch paths that interpolate it (`commit_compaction_in_txn`,
/// `commit_overwrite_in_txn`) rather than binding a parameter. The bytes are
/// the same raw-UUID encoding as [`insert_record_table_id_value`]; the callers
/// already validate `table_id` is a well-formed UUID before interpolating.
pub(super) fn insert_record_table_id_blob_literal(table_id: &str) -> String {
    use std::fmt::Write as _;
    let bytes = crate::metastore::table_id_to_key_bytes(table_id);
    let mut hex = String::with_capacity(bytes.len() * 2 + 3);
    hex.push_str("x'");
    for b in bytes {
        // Writing to a String is infallible.
        let _ = write!(hex, "{b:02x}");
    }
    hex.push('\'');
    hex
}

/// Ensure the table's current snapshot directory exists on local FS (no-op for
/// `s3://` paths), creating it and best-effort fsyncing the table root if
/// missing. Called on the `create_table` already-exists path so a table row
/// whose snapshot directory has gone missing (e.g. the data directory was
/// removed out-of-band) is healed before the existing `table_id` is returned.
/// Fails with [`CatalogError::Io`] if the snapshot path exists but is not a
/// directory.
pub(super) async fn ensure_snapshot_directory_exists(table: &TableMetadata) -> CatalogResult<()> {
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
pub(super) fn configuration_matches(stored: &TableMetadata, options: &CreateTableOptions) -> bool {
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

pub(super) fn validate_create_table_options(options: &CreateTableOptions) -> CatalogResult<()> {
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

pub(super) fn log_runtime_footer_cache_drift(
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
pub(super) fn log_configuration_differences(
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

    // Note: `delta_encoding` is deliberately NOT part of the data-compatibility
    // gate (`is_data_compatible`): every level emits standard Vortex encodings
    // readable by the same scan, so a level change applies to future writes
    // only and must not force a table re-create. Surface it in the change log.
    if stored.vortex_config.delta_encoding != options.vortex_config.delta_encoding {
        differences.push(format!(
            "delta_encoding: {} -> {} (write-time only; existing data unaffected)",
            stored.vortex_config.delta_encoding, options.vortex_config.delta_encoding
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
