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

//! Turso implementation of the metastore backend.

use super::{
    ExecuteParams, MetastoreBackend, MetastoreRow, MetastoreTransaction, MetastoreValue,
    QueryParams, QueryRowParams, duplicate_delete_file_index_error_message,
};
use crate::catalog::{CatalogError, CatalogResult};
use async_trait::async_trait;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{fmt::Debug, path::Path};
use tokio::sync::{Mutex, OnceCell, OwnedMutexGuard};
use turso::{Builder, Connection, Value as TursoValue};
use turso_shared::JOURNAL_MODE_SQL_LITERAL;

const DELETE_FILE_TABLE_UNIQUE_INDEX_DDL: &str = "CREATE UNIQUE INDEX IF NOT EXISTS idx_cayenne_delete_file_table_path ON cayenne_delete_file(table_id, path)";

/// Round-robin connection pool for the [`TursoMetastore`].
///
/// Turso/libSQL uses `BEGIN CONCURRENT` which leverages MVCC for true parallel
/// writes — unlike plain `SQLite` WAL, multiple writers can proceed without
/// engine-level serialization. A larger pool (K = 16) therefore translates
/// directly to higher throughput for concurrent CDC table commits.
struct TursoConnectionPool {
    conns: Vec<Arc<Mutex<Connection>>>,
    next: AtomicUsize,
}

impl TursoConnectionPool {
    /// Acquire a connection using round-robin with try-first heuristic.
    async fn acquire(&self) -> OwnedMutexGuard<Connection> {
        let n = self.conns.len();
        let start = self.next.fetch_add(1, Ordering::Relaxed) % n;
        for i in 0..n {
            let idx = (start + i) % n;
            if let Ok(guard) = Arc::clone(&self.conns[idx]).try_lock_owned() {
                return guard;
            }
        }
        Arc::clone(&self.conns[start]).lock_owned().await
    }
}

/// Turso-based metastore backend with a K = 16 connection pool.
///
/// Leverages Turso/libSQL's `BEGIN CONCURRENT` MVCC to allow true parallel
/// writes without engine-level serialization.
pub struct TursoMetastore {
    /// Round-robin pool of 16 independent connections.
    pool: OnceCell<Arc<TursoConnectionPool>>,
    connection_string: String,
}

impl Debug for TursoMetastore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TursoMetastore")
            .field("connection_string", &self.connection_string)
            .finish_non_exhaustive()
    }
}

impl TursoMetastore {
    /// Create a new Turso metastore.
    pub fn new(connection_string: impl Into<String>) -> Self {
        Self {
            pool: OnceCell::new(),
            connection_string: connection_string.into(),
        }
    }

    /// Get the database file path from the connection string.
    fn db_path(&self) -> &str {
        self.connection_string
            .strip_prefix("libsql://")
            .unwrap_or(&self.connection_string)
    }

    /// Open and configure a single Turso connection.
    async fn open_connection(&self) -> CatalogResult<Connection> {
        let db_path = self.db_path();

        let db_dir =
            Path::new(db_path)
                .parent()
                .ok_or_else(|| CatalogError::InvalidDatabasePath {
                    path: db_path.to_string(),
                })?;

        if !db_dir.exists() {
            tokio::fs::create_dir_all(db_dir).await?;

            // Best-effort parent directory sync (defense-in-depth with the sync
            // already performed in CayenneCatalog::init).
            if let Some(parent) = db_dir.parent() {
                let parent_for_sync = parent.to_path_buf();
                let parent_display = parent_for_sync.display().to_string();
                let db_dir_display = db_dir.display().to_string();
                match tokio::task::spawn_blocking(move || {
                    std::fs::File::open(&parent_for_sync).and_then(|f| f.sync_all())
                })
                .await
                {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => tracing::warn!(
                        "Failed to sync parent directory {parent_display} after creating Turso catalog DB directory {db_dir_display} (subsequent DB writes will still be durable): {error}"
                    ),
                    Err(error) => tracing::warn!(
                        "Failed to join Turso catalog DB parent directory sync task for {parent_display}: {error}"
                    ),
                }
            }
        }

        let db = Builder::new_local(db_path)
            .build()
            .await
            .map_err(|e| CatalogError::Database {
                message: format!("Failed to open Turso database: {e}"),
            })?;

        let conn = db.connect().map_err(|e| CatalogError::Database {
            message: format!("Failed to connect to Turso database: {e}"),
        })?;

        // Set busy timeout to wait for locks instead of immediately returning SQLITE_BUSY.
        conn.busy_timeout(std::time::Duration::from_secs(5))
            .map_err(|e| CatalogError::Database {
                message: format!("Failed to set busy timeout: {e}"),
            })?;

        // BEGIN CONCURRENT requires MVCC journal mode for concurrent writers.
        conn.pragma_update("journal_mode", JOURNAL_MODE_SQL_LITERAL)
            .await
            .map_err(|e| CatalogError::Database {
                message: format!("Failed to set journal mode: {e}"),
            })?;

        conn.execute("PRAGMA foreign_keys = ON", ())
            .await
            .map_err(|e| CatalogError::Database {
                message: format!("Failed to enable foreign keys: {e}"),
            })?;

        // NORMAL synchronous mode: safe with MVCC, more performant than FULL
        conn.execute("PRAGMA synchronous = NORMAL", ())
            .await
            .map_err(|e| CatalogError::Database {
                message: format!("Failed to set synchronous mode: {e}"),
            })?;

        // 32MB cache size (negative value = kilobytes in SQLite/libSQL)
        conn.execute("PRAGMA cache_size = -32768", ())
            .await
            .map_err(|e| CatalogError::Database {
                message: format!("Failed to set cache size: {e}"),
            })?;

        Ok(conn)
    }

    /// Return the connection pool, initialising K = 16 connections lazily on first call.
    ///
    /// All connections share the same database file. `BEGIN CONCURRENT` leverages
    /// Turso/libSQL MVCC so concurrent writers do not serialize at the engine level.
    async fn pool(&self) -> CatalogResult<&Arc<TursoConnectionPool>> {
        self.pool
            .get_or_try_init(|| async {
                let mut conns = Vec::with_capacity(16);
                for _ in 0..16 {
                    conns.push(Arc::new(Mutex::new(self.open_connection().await?)));
                }
                Ok(Arc::new(TursoConnectionPool {
                    conns,
                    next: AtomicUsize::new(0),
                }))
            })
            .await
    }

    /// Schema for the `cayenne_table` table.
    const TABLE_TABLE_DDL: &'static str = r"
        CREATE TABLE IF NOT EXISTS cayenne_table (
            table_id TEXT PRIMARY KEY,
            table_name TEXT NOT NULL,
            path TEXT NOT NULL,
            path_is_relative BOOLEAN NOT NULL,
            schema_json TEXT NOT NULL,
            primary_key_json TEXT,
            on_conflict_json TEXT,
            current_snapshot_id TEXT NOT NULL DEFAULT '',
            partition_column TEXT,
            vortex_config_json TEXT,
            current_sequence_number BIGINT NOT NULL DEFAULT 0
        )
    ";

    const TABLE_NAME_UNIQUE_INDEX_DDL: &'static str = r"
        CREATE UNIQUE INDEX IF NOT EXISTS idx_cayenne_table_name_unique
        ON cayenne_table(table_name)
    ";

    /// Schema for the `cayenne_delete_file` table.
    const DELETE_FILE_TABLE_DDL: &'static str = r"
        CREATE TABLE IF NOT EXISTS cayenne_delete_file (
            delete_file_id TEXT PRIMARY KEY,
            table_id TEXT NOT NULL,
            path TEXT NOT NULL,
            path_is_relative BOOLEAN NOT NULL,
            format TEXT NOT NULL,
            delete_count BIGINT NOT NULL,
            file_size_bytes BIGINT NOT NULL,
            source_data_file_path TEXT,
            sequence_number BIGINT NOT NULL DEFAULT 0,
            FOREIGN KEY (table_id) REFERENCES cayenne_table(table_id) ON DELETE CASCADE
        )
    ";

    /// Schema for the `cayenne_partition` table.
    ///
    /// Supports composite partition keys by storing column names and values as JSON arrays.
    /// The `partition_key` column stores a unique composite key (slash-separated values)
    /// for efficient lookups and uniqueness constraints.
    const PARTITION_TABLE_DDL: &'static str = r"
        CREATE TABLE IF NOT EXISTS cayenne_partition (
            partition_id TEXT PRIMARY KEY,
            table_id TEXT NOT NULL,
            partition_columns_json TEXT NOT NULL,
            partition_values_json TEXT NOT NULL,
            partition_key TEXT NOT NULL,
            path TEXT NOT NULL,
            path_is_relative BOOLEAN NOT NULL,
            record_count BIGINT NOT NULL DEFAULT 0,
            file_size_bytes BIGINT NOT NULL DEFAULT 0,
            FOREIGN KEY (table_id) REFERENCES cayenne_table(table_id) ON DELETE CASCADE,
            UNIQUE(table_id, partition_key)
        )
    ";

    /// Schema for the `cayenne_insert_record` table.
    ///
    /// Insert records track PKs that were re-inserted after being deleted.
    /// Each record stores the sequence number when the insert occurred.
    /// Combined with the delete's sequence number, this enables ordering:
    /// - If `insert_sequence` > `delete_sequence` for a PK, the row is visible
    /// - If `delete_sequence` > `insert_sequence`, the row is filtered out
    ///
    /// Keyed directly on `(table_id, pk_bytes)` as a composite primary key. The
    /// only access paths are `WHERE table_id = ?` (leading-prefix scan) and the
    /// `INSERT OR REPLACE` upsert on `(table_id, pk_bytes)`; both are served by
    /// the composite PK. The previous never-read `insert_record_id` UUID
    /// `TEXT PRIMARY KEY` (plus a redundant `UNIQUE(table_id, pk_bytes)`) is
    /// dropped (see `init_schema` for the legacy-schema migration).
    ///
    /// Unlike the SQLite backend, this table is NOT declared `WITHOUT ROWID`.
    /// `WITHOUT ROWID` is not currently supported by Turso under the `mvcc`
    /// journal mode that the metastore relies on for `BEGIN CONCURRENT`
    /// parallel writers (Turso rejects it with "WITHOUT ROWID tables are not
    /// supported in MVCC mode").
    const INSERT_RECORD_TABLE_DDL: &'static str = r"
        CREATE TABLE IF NOT EXISTS cayenne_insert_record (
            table_id TEXT NOT NULL,
            pk_bytes BLOB NOT NULL,
            sequence_number BIGINT NOT NULL,
            FOREIGN KEY (table_id) REFERENCES cayenne_table(table_id) ON DELETE CASCADE,
            PRIMARY KEY (table_id, pk_bytes)
        )
    ";

    /// Schema for the `cayenne_snapshot_sequence` table.
    ///
    /// Tracks the sequence number for each snapshot. This enables Iceberg-style
    /// sequence ordering: a deletion only applies to snapshots with `sequence_number`
    /// <= the delete file's `sequence_number`.
    const SNAPSHOT_SEQUENCE_TABLE_DDL: &'static str = r"
        CREATE TABLE IF NOT EXISTS cayenne_snapshot_sequence (
            table_id TEXT NOT NULL,
            snapshot_id TEXT NOT NULL,
            sequence_number BIGINT NOT NULL,
            FOREIGN KEY (table_id) REFERENCES cayenne_table(table_id) ON DELETE CASCADE,
            PRIMARY KEY (table_id, snapshot_id)
        )
    ";

    /// Schema for the `cayenne_table_statistics` table.
    const TABLE_STATISTICS_DDL: &'static str = r"
        CREATE TABLE IF NOT EXISTS cayenne_table_statistics (
            table_id TEXT NOT NULL PRIMARY KEY,
            statistics_blob BLOB NOT NULL,
            num_rows BIGINT NOT NULL DEFAULT 0,
            ndv_sketches BLOB,
            FOREIGN KEY (table_id) REFERENCES cayenne_table(table_id) ON DELETE CASCADE
        )
    ";

    /// Schema for the `cayenne_pk_index` table. Mirrors the `SQLite` definition;
    /// captured in metastore snapshots via `EXPECTED_TABLES`.
    const PK_INDEX_TABLE_DDL: &'static str = r"
        CREATE TABLE IF NOT EXISTS cayenne_pk_index (
            table_id TEXT NOT NULL PRIMARY KEY,
            snapshot_id TEXT NOT NULL,
            index_blob BLOB NOT NULL,
            FOREIGN KEY (table_id) REFERENCES cayenne_table(table_id) ON DELETE CASCADE
        )
    ";

    const INLINED_DATA_TABLE_DDL: &'static str = r"
        CREATE TABLE IF NOT EXISTS cayenne_inlined_data (
            inlined_id TEXT PRIMARY KEY,
            table_id TEXT NOT NULL,
            partition_key TEXT,
            data_ipc BLOB NOT NULL,
            record_count BIGINT NOT NULL,
            sequence_number BIGINT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
            FOREIGN KEY (table_id) REFERENCES cayenne_table(table_id) ON DELETE CASCADE
        )
    ";

    const INLINED_DELETE_TABLE_DDL: &'static str = r"
        CREATE TABLE IF NOT EXISTS cayenne_inlined_delete (
            inlined_id TEXT PRIMARY KEY,
            table_id TEXT NOT NULL,
            delete_ipc BLOB NOT NULL,
            delete_count BIGINT NOT NULL,
            sequence_number BIGINT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
            published INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY (table_id) REFERENCES cayenne_table(table_id) ON DELETE CASCADE
        )
    ";

    const INLINED_DATA_INDEX_DDL: &'static str = "CREATE INDEX IF NOT EXISTS idx_cayenne_inlined_data_table_seq ON cayenne_inlined_data(table_id, sequence_number)";
    const INLINED_DELETE_INDEX_DDL: &'static str = "CREATE INDEX IF NOT EXISTS idx_cayenne_inlined_delete_table_seq ON cayenne_inlined_delete(table_id, sequence_number)";
    /// Partial index over the unpublished tombstones (Option D). The only other
    /// `cayenne_inlined_delete` index is `(table_id, sequence_number)`, which a
    /// `WHERE table_id = ? AND published = 0` predicate cannot seek — it has to
    /// scan every tombstone for the table. This partial index covers exactly the
    /// in-flight `published = 0` rows (a tiny set; finalize flips them to 1), so
    /// `publish_orphan_inlined_deletes`' COUNT/UPDATE seek straight to them. Its
    /// complement also accelerates the hot read path's
    /// `WHERE table_id = ? AND published = 1` (`get_published_inlined_deletes`).
    /// Turso/libSQL (`turso_core` 0.6.1) supports partial indexes: the index
    /// translator binds the `WHERE` predicate and skips non-matching rows during
    /// index population, matching `SQLite` partial-index semantics.
    const INLINED_DELETE_UNPUBLISHED_INDEX_DDL: &'static str = "CREATE INDEX IF NOT EXISTS idx_cayenne_inlined_delete_unpublished ON cayenne_inlined_delete(table_id) WHERE published = 0";
}

/// Borrowed view of a `turso::Row` implementing `MetastoreRow`. Typed
/// accessors take ownership of the value returned by `get_value` directly,
/// avoiding the per-row `Vec<MetastoreValue>` materialization plus the
/// redundant Text/Blob clone the conversion-then-extract path paid. See
/// `metastore_operations::query_batch` (`Turso` vs `SQLite` gap).
struct TursoRow<'a> {
    inner: &'a turso::Row,
}

impl TursoRow<'_> {
    fn raw_value(&self, index: usize) -> CatalogResult<TursoValue> {
        let count = self.inner.column_count();
        if index >= count {
            return Err(CatalogError::Database {
                message: format!("Column index {index} out of bounds (column_count={count})"),
            });
        }
        self.inner.get_value(index).map_err(convert_turso_error)
    }
}

impl MetastoreRow for TursoRow<'_> {
    fn get_value(&self, index: usize) -> CatalogResult<MetastoreValue> {
        Ok(convert_turso_value(self.raw_value(index)?))
    }

    fn get_i64(&self, index: usize) -> CatalogResult<i64> {
        match self.raw_value(index)? {
            TursoValue::Integer(i) => Ok(i),
            other => Err(CatalogError::Database {
                message: format!("Expected integer at index {index}, found {other:?}"),
            }),
        }
    }

    fn get_string(&self, index: usize) -> CatalogResult<String> {
        match self.raw_value(index)? {
            TursoValue::Text(s) => Ok(s),
            other => Err(CatalogError::Database {
                message: format!("Expected text at index {index}, found {other:?}"),
            }),
        }
    }

    fn get_bool(&self, index: usize) -> CatalogResult<bool> {
        match self.raw_value(index)? {
            TursoValue::Integer(i) => Ok(i != 0),
            other => Err(CatalogError::Database {
                message: format!(
                    "Expected boolean (Integer 0/1) at index {index}, found {other:?}"
                ),
            }),
        }
    }

    fn get_blob(&self, index: usize) -> CatalogResult<Vec<u8>> {
        match self.raw_value(index)? {
            TursoValue::Blob(b) => Ok(b),
            other => Err(CatalogError::Database {
                message: format!("Expected blob at index {index}, found {other:?}"),
            }),
        }
    }

    fn get_optional_i64(&self, index: usize) -> CatalogResult<Option<i64>> {
        // Real is treated as Null because the metastore schema never stores Real values;
        // this mirrors the historic `convert_turso_value` mapping.
        match self.raw_value(index)? {
            TursoValue::Integer(i) => Ok(Some(i)),
            TursoValue::Null | TursoValue::Real(_) => Ok(None),
            other => Err(CatalogError::Database {
                message: format!("Expected integer or null at index {index}, found {other:?}"),
            }),
        }
    }

    fn get_optional_string(&self, index: usize) -> CatalogResult<Option<String>> {
        match self.raw_value(index)? {
            TursoValue::Text(s) => Ok(Some(s)),
            TursoValue::Null | TursoValue::Real(_) => Ok(None),
            other => Err(CatalogError::Database {
                message: format!("Expected text or null at index {index}, found {other:?}"),
            }),
        }
    }

    fn get_optional_blob(&self, index: usize) -> CatalogResult<Option<Vec<u8>>> {
        match self.raw_value(index)? {
            TursoValue::Blob(b) => Ok(Some(b)),
            TursoValue::Null | TursoValue::Real(_) => Ok(None),
            other => Err(CatalogError::Database {
                message: format!("Expected blob or null at index {index}, found {other:?}"),
            }),
        }
    }
}

/// Convert Turso Value to `MetastoreValue`, consuming the source so the
/// Text/Blob payload moves without an extra heap copy.
fn convert_turso_value(value: TursoValue) -> MetastoreValue {
    match value {
        TursoValue::Null => MetastoreValue::Null,
        TursoValue::Integer(i) => MetastoreValue::Integer(i),
        TursoValue::Real(_) => {
            // We don't use real numbers in metadata
            MetastoreValue::Null
        }
        TursoValue::Text(t) => MetastoreValue::Text(t),
        TursoValue::Blob(b) => MetastoreValue::Blob(b),
    }
}

/// Convert `MetastoreValue` to Turso Value, consuming the source so Text/Blob
/// payloads move without an extra heap copy.
fn to_turso_value(value: MetastoreValue) -> TursoValue {
    match value {
        MetastoreValue::Integer(i) => TursoValue::Integer(i),
        MetastoreValue::Text(s) => TursoValue::Text(s),
        MetastoreValue::Bool(b) => TursoValue::Integer(i64::from(b)),
        MetastoreValue::Blob(b) => TursoValue::Blob(b),
        MetastoreValue::Null => TursoValue::Null,
    }
}

/// Convert Turso errors to `CatalogError`, distinguishing constraint violations.
fn convert_turso_error(e: turso::Error) -> CatalogError {
    match e {
        // turso exposes a dedicated Constraint variant for constraint violations
        turso::Error::Constraint(ref msg) => CatalogError::ConstraintViolation {
            message: msg.clone(),
        },
        other => CatalogError::Database {
            message: format!("Failed to execute statement: {other}"),
        },
    }
}

#[async_trait]
impl MetastoreBackend for TursoMetastore {
    async fn init_schema(&self) -> CatalogResult<()> {
        let conn = self.pool().await?.acquire().await;

        // Create tables
        let schema_sql = format!(
            "{}; {}; {}; {}; {}; {}; {}; {}; {}; {};",
            Self::TABLE_TABLE_DDL,
            Self::TABLE_NAME_UNIQUE_INDEX_DDL,
            Self::DELETE_FILE_TABLE_DDL,
            Self::PARTITION_TABLE_DDL,
            Self::INSERT_RECORD_TABLE_DDL,
            Self::SNAPSHOT_SEQUENCE_TABLE_DDL,
            Self::TABLE_STATISTICS_DDL,
            Self::INLINED_DATA_TABLE_DDL,
            Self::INLINED_DELETE_TABLE_DDL,
            Self::PK_INDEX_TABLE_DDL
        );

        conn.execute_batch(&schema_sql)
            .await
            .map_err(|e| CatalogError::Database {
                message: format!("Failed to initialize schema: {e}"),
            })?;

        conn.execute(DELETE_FILE_TABLE_UNIQUE_INDEX_DDL, ())
            .await
            .map_err(|e| CatalogError::Database {
                message: duplicate_delete_file_index_error_message("Turso/libSQL", e),
            })?;

        conn.execute(Self::INLINED_DATA_INDEX_DDL, ())
            .await
            .map_err(|e| CatalogError::Database {
                message: format!("Failed to create inlined_data index: {e}"),
            })?;
        conn.execute(Self::INLINED_DELETE_INDEX_DDL, ())
            .await
            .map_err(|e| CatalogError::Database {
                message: format!("Failed to create inlined_delete index: {e}"),
            })?;
        // Attempt to backfill newly added columns for existing deployments. Errors are ignored
        // because the column may already exist (libSQL doesn't support IF NOT EXISTS for ALTER).
        let _ = conn
            .execute(
                "ALTER TABLE cayenne_table ADD COLUMN on_conflict_json TEXT",
                (),
            )
            .await;
        let _ = conn
            .execute(
                "ALTER TABLE cayenne_table_statistics ADD COLUMN ndv_sketches BLOB",
                (),
            )
            .await;

        // Per-tombstone activation flag for `cayenne_inlined_delete`. When the
        // ALTER actually adds the column (Ok), every existing row took the
        // default (0); backfill legacy rows to 1 because they were always active
        // under the pre-flag semantics (leaving them 0 would resurrect the old
        // inline copies they hide). On a fresh DB / later startups the column
        // already exists, the ALTER errors, and the backfill is skipped so a
        // legitimately in-flight `published = 0` tombstone is never re-activated.
        if conn
            .execute(
                "ALTER TABLE cayenne_inlined_delete ADD COLUMN published INTEGER NOT NULL DEFAULT 0",
                (),
            )
            .await
            .is_ok()
        {
            conn.execute("UPDATE cayenne_inlined_delete SET published = 1", ())
                .await
                .map_err(|e| CatalogError::Database {
                    message: format!("Failed to backfill inlined-delete published flag: {e}"),
                })?;
        }

        // Migrate a legacy `cayenne_insert_record` (UUID `insert_record_id` TEXT
        // PRIMARY KEY + redundant `UNIQUE(table_id, pk_bytes)`) to the composite-PK
        // schema. The `CREATE TABLE IF NOT EXISTS` above leaves a pre-existing
        // table untouched, so detect the old layout here and copy its rows
        // forward. The table is ephemeral (cleared at every checkpoint and
        // recoverable from the snapshot), but we copy the
        // (table_id, pk_bytes, sequence_number) rows forward to preserve any
        // in-flight pre-checkpoint re-insert sequences across the upgrade.
        let mut ir_cols = conn
            .query("PRAGMA table_info('cayenne_insert_record')", ())
            .await
            .map_err(|e| CatalogError::Database {
                message: format!("Failed to read cayenne_insert_record schema for migration: {e}"),
            })?;
        let mut has_legacy_uuid_column = false;
        loop {
            match ir_cols.next().await {
                // Column name is at index 1 of PRAGMA table_info.
                Ok(Some(row)) => {
                    if let Ok(turso::Value::Text(name)) = row.get_value(1)
                        && name == "insert_record_id"
                    {
                        has_legacy_uuid_column = true;
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    return Err(CatalogError::Database {
                        message: format!(
                            "Failed to read cayenne_insert_record schema for migration: {e}"
                        ),
                    });
                }
            }
        }
        drop(ir_cols);
        if has_legacy_uuid_column {
            // NOTE: not `WITHOUT ROWID` here, intentionally. Turso does not
            // currently support `WITHOUT ROWID` tables under the `mvcc` journal
            // mode this metastore uses for `BEGIN CONCURRENT` (see
            // `INSERT_RECORD_TABLE_DDL`).
            conn.execute_batch(
                "CREATE TABLE cayenne_insert_record_new (
                    table_id TEXT NOT NULL,
                    pk_bytes BLOB NOT NULL,
                    sequence_number BIGINT NOT NULL,
                    FOREIGN KEY (table_id) REFERENCES cayenne_table(table_id) ON DELETE CASCADE,
                    PRIMARY KEY (table_id, pk_bytes)
                );
                INSERT OR REPLACE INTO cayenne_insert_record_new (table_id, pk_bytes, sequence_number)
                    SELECT table_id, pk_bytes, sequence_number FROM cayenne_insert_record;
                DROP TABLE cayenne_insert_record;
                ALTER TABLE cayenne_insert_record_new RENAME TO cayenne_insert_record;",
            )
            .await
            .map_err(|e| CatalogError::Database {
                message: format!("Failed to migrate cayenne_insert_record to composite PK: {e}"),
            })?;
        }

        // Must run AFTER the `published` column migration above: the partial index
        // predicate (`WHERE published = 0`) references the column, so creating it
        // first on a pre-flag metastore would fail and abort `init_schema`.
        conn.execute(Self::INLINED_DELETE_UNPUBLISHED_INDEX_DDL, ())
            .await
            .map_err(|e| CatalogError::Database {
                message: format!("Failed to create inlined_delete unpublished index: {e}"),
            })?;

        // Validate that existing tables match the expected schema.
        // This catches incompatible metadata databases from previous versions.
        for expected in super::EXPECTED_TABLES {
            let mut rows = conn
                .query(&format!("PRAGMA table_info('{}')", expected.name), ())
                .await
                .map_err(|e| CatalogError::Database {
                    message: format!("Failed to read table schema for validation: {e}"),
                })?;

            let mut actual_columns = Vec::new();
            loop {
                match rows.next().await {
                    Ok(Some(row)) => {
                        // PRAGMA table_info columns: cid, name, type, notnull, dflt_value, pk
                        // Column name is at index 1.
                        if let Ok(turso::Value::Text(name)) = row.get_value(1) {
                            actual_columns.push(name);
                        }
                    }
                    Ok(None) => break,
                    Err(e) => {
                        return Err(CatalogError::Database {
                            message: format!("Failed to read table schema for validation: {e}"),
                        });
                    }
                }
            }

            // Skip validation for freshly created tables (no columns = table didn't exist before).
            if actual_columns.is_empty() {
                continue;
            }

            let expected_columns: Vec<&str> = expected.columns.to_vec();
            let actual_refs: Vec<&str> = actual_columns.iter().map(String::as_str).collect();

            if expected_columns != actual_refs {
                tracing::debug!(
                    "Cayenne schema mismatch for '{}': expected columns [{}], found [{}]",
                    expected.name,
                    expected_columns.join(", "),
                    actual_refs.join(", ")
                );
                return Err(CatalogError::SchemaMismatch {
                    table: expected.name.to_string(),
                });
            }
        }

        Ok(())
    }

    async fn execute(&self, params: ExecuteParams<'_>) -> CatalogResult<()> {
        let conn = self.pool().await?.acquire().await;

        let turso_params: Vec<TursoValue> = params.params.into_iter().map(to_turso_value).collect();

        let mut stmt = conn
            .prepare_cached(params.sql)
            .await
            .map_err(convert_turso_error)?;
        stmt.execute(turso_params)
            .await
            .map_err(convert_turso_error)?;

        Ok(())
    }

    async fn execute_batch(&self, sql: &str) -> CatalogResult<()> {
        let conn = self.pool().await?.acquire().await;

        conn.execute_batch(sql)
            .await
            .map_err(|e| CatalogError::Database {
                message: format!("Failed to execute batch: {e}"),
            })?;

        Ok(())
    }

    async fn execute_transaction_batch(&self, sql: &str) -> CatalogResult<()> {
        let conn = self.pool().await?.acquire().await;
        let batch_sql = format!("BEGIN CONCURRENT; {sql}; COMMIT;");

        if let Err(e) = conn.execute_batch(&batch_sql).await {
            let _ = conn.execute("ROLLBACK", ()).await;
            return Err(CatalogError::Database {
                message: format!("Failed to execute transaction batch: {e}"),
            });
        }

        Ok(())
    }

    async fn query_row<F, T>(&self, params: QueryRowParams<'_>, f: F) -> CatalogResult<T>
    where
        F: FnOnce(&dyn MetastoreRow) -> CatalogResult<T> + Send + 'static,
        T: Send + 'static,
    {
        let conn = self.pool().await?.acquire().await;

        let turso_params: Vec<TursoValue> = params.params.into_iter().map(to_turso_value).collect();

        let mut stmt =
            conn.prepare_cached(params.sql)
                .await
                .map_err(|e| CatalogError::Database {
                    message: format!("Failed to query row: {e}"),
                })?;
        let mut rows = stmt
            .query(turso_params)
            .await
            .map_err(|e| CatalogError::Database {
                message: format!("Failed to query row: {e}"),
            })?;

        let row = rows.next().await.map_err(|e| CatalogError::Database {
            message: format!("Failed to fetch row: {e}"),
        })?;

        let row = row.ok_or_else(|| CatalogError::Database {
            message: "Query returned no rows".to_string(),
        })?;

        let turso_row = TursoRow { inner: &row };
        f(&turso_row)
    }

    async fn query<F, T>(&self, params: QueryParams<'_>, f: F) -> CatalogResult<Vec<T>>
    where
        F: Fn(&dyn MetastoreRow) -> CatalogResult<T> + Send + 'static,
        T: Send + 'static,
    {
        let conn = self.pool().await?.acquire().await;

        let turso_params: Vec<TursoValue> = params.params.into_iter().map(to_turso_value).collect();

        let mut stmt =
            conn.prepare_cached(params.sql)
                .await
                .map_err(|e| CatalogError::Database {
                    message: format!("Failed to query rows: {e}"),
                })?;
        let mut rows = stmt
            .query(turso_params)
            .await
            .map_err(|e| CatalogError::Database {
                message: format!("Failed to query rows: {e}"),
            })?;

        let mut results = Vec::new();

        loop {
            match rows.next().await {
                Ok(Some(row)) => {
                    let turso_row = TursoRow { inner: &row };
                    results.push(f(&turso_row)?);
                }
                Ok(None) => break,
                Err(e) => {
                    return Err(CatalogError::Database {
                        message: format!("Failed to fetch row: {e}"),
                    });
                }
            }
        }

        Ok(results)
    }

    async fn begin_transaction(&self) -> CatalogResult<Box<dyn MetastoreTransaction>> {
        let guard = self.pool().await?.acquire().await;

        // Defensively clear any leftover transaction state before BEGIN. A
        // prior `TursoTransaction` whose `Drop` fired-and-forgot a ROLLBACK
        // via `tokio::spawn` can lose the rollback under runtime shutdown,
        // returning the connection to the pool inside `BEGIN CONCURRENT`.
        // Issuing ROLLBACK is idempotent on a clean connection (it errors
        // with "no transaction active"); we ignore that case.
        let _ = guard.execute("ROLLBACK", ()).await;

        guard
            .execute("BEGIN CONCURRENT", ())
            .await
            .map_err(|e| CatalogError::Database {
                message: format!("Failed to begin concurrent transaction: {e}"),
            })?;

        Ok(Box::new(TursoTransaction { conn: Some(guard) }))
    }

    async fn shutdown(&self) -> CatalogResult<()> {
        // Turso handles cleanup automatically
        tracing::info!("Shutting down Turso metastore");
        Ok(())
    }
}

/// A transaction on a Turso metastore connection.
///
/// Holds an [`OwnedMutexGuard`] on the underlying connection, ensuring
/// exclusive access for the lifetime of the transaction.
///
/// If neither [`commit`](MetastoreTransaction::commit) nor
/// [`rollback`](MetastoreTransaction::rollback) is called, the transaction
/// is automatically rolled back on drop via a best-effort `ROLLBACK`.
pub struct TursoTransaction {
    /// Exclusive lock on a pool connection. `None` after commit/rollback.
    conn: Option<OwnedMutexGuard<Connection>>,
}

impl Drop for TursoTransaction {
    fn drop(&mut self) {
        if let Some(guard) = self.conn.take() {
            tokio::spawn(async move {
                tracing::debug!(
                    "TursoTransaction dropped without explicit commit or rollback; \
                     attempting auto-rollback"
                );
                if let Err(err) = guard.execute("ROLLBACK", ()).await {
                    tracing::error!("Failed to auto-rollback TursoTransaction on drop: {err}");
                }
                // `guard` is dropped here, releasing the pool slot.
            });
        }
    }
}

#[async_trait]
impl MetastoreTransaction for TursoTransaction {
    async fn execute(&self, params: ExecuteParams<'_>) -> CatalogResult<()> {
        let conn = self.conn.as_ref().ok_or_else(|| CatalogError::Database {
            message: "Transaction already completed".to_string(),
        })?;

        let turso_params: Vec<TursoValue> = params.params.into_iter().map(to_turso_value).collect();

        let mut stmt = conn
            .prepare_cached(params.sql)
            .await
            .map_err(convert_turso_error)?;
        stmt.execute(turso_params)
            .await
            .map_err(convert_turso_error)?;

        Ok(())
    }

    async fn query_row_values(
        &self,
        params: QueryRowParams<'_>,
    ) -> CatalogResult<Vec<MetastoreValue>> {
        let conn = self.conn.as_ref().ok_or_else(|| CatalogError::Database {
            message: "Transaction already completed".to_string(),
        })?;
        let turso_params: Vec<TursoValue> = params.params.into_iter().map(to_turso_value).collect();

        let mut stmt = conn
            .prepare_cached(params.sql)
            .await
            .map_err(convert_turso_error)?;
        let mut rows = stmt
            .query(turso_params)
            .await
            .map_err(convert_turso_error)?;

        let row = rows.next().await.map_err(convert_turso_error)?;
        let row = row.ok_or_else(|| CatalogError::Database {
            message: "Query returned no rows".to_string(),
        })?;

        (0..row.column_count())
            .map(|i| {
                row.get_value(i)
                    .map(convert_turso_value)
                    .map_err(convert_turso_error)
            })
            .collect::<CatalogResult<_>>()
    }

    async fn execute_batch(&self, sql: &str) -> CatalogResult<()> {
        let conn = self.conn.as_ref().ok_or_else(|| CatalogError::Database {
            message: "Transaction already completed".to_string(),
        })?;

        conn.execute_batch(sql)
            .await
            .map_err(|e| CatalogError::Database {
                message: format!("Failed to execute batch in transaction: {e}"),
            })?;

        Ok(())
    }

    async fn commit(mut self: Box<Self>) -> CatalogResult<()> {
        let conn = self.conn.take().ok_or_else(|| CatalogError::Database {
            message: "Transaction already completed".to_string(),
        })?;

        if let Err(e) = conn.execute("COMMIT", ()).await {
            // Best-effort rollback to leave the connection in a clean state.
            let _ = conn.execute("ROLLBACK", ()).await;
            return Err(CatalogError::Database {
                message: format!("Failed to commit transaction: {e}"),
            });
        }

        Ok(())
    }

    async fn rollback(mut self: Box<Self>) -> CatalogResult<()> {
        let conn = self.conn.take().ok_or_else(|| CatalogError::Database {
            message: "Transaction already completed".to_string(),
        })?;

        conn.execute("ROLLBACK", ())
            .await
            .map_err(|e| CatalogError::Database {
                message: format!("Failed to rollback transaction: {e}"),
            })?;

        Ok(())
    }
}
