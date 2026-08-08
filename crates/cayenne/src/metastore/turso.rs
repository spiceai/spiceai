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
//!
//! libSQL/Turso backend (gated on the `turso` feature). Unlike `SQLite`'s single writer,
//! it uses `BEGIN CONCURRENT` MVCC writers that run in parallel and serialize at commit
//! time only on actual conflicts, behind a fixed `K = 16` connection pool.

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
    ///
    /// The connection is handed out only once it is confirmed to be in autocommit, so a
    /// caller never inherits a transaction a previous holder left open — see
    /// [`Self::clear_open_transaction`]. A slot that cannot be cleared is reported as an
    /// error rather than handed over: the borrower's statement would otherwise read that
    /// transaction's snapshot, which is the failure this pool guard exists to prevent.
    async fn acquire(&self) -> CatalogResult<OwnedMutexGuard<Connection>> {
        let guard = self.lock_next_free().await;
        Self::clear_open_transaction(&guard).await?;
        Ok(guard)
    }

    /// Take the lock on the first free connection, else wait on the round-robin pick.
    async fn lock_next_free(&self) -> OwnedMutexGuard<Connection> {
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

    /// Roll back a transaction a previous holder left open on `conn`.
    ///
    /// A connection comes back to the pool inside `BEGIN CONCURRENT` whenever its holder
    /// could not finish the transaction: [`TursoTransaction`]'s `Drop` hands the `ROLLBACK`
    /// to a spawned task, which is dropped without running if the runtime is shutting down,
    /// and `commit`/`rollback` release the connection even when the statement they issue
    /// fails. Under `BEGIN CONCURRENT` the transaction holds an MVCC snapshot, so the next
    /// statement on that connection reads the state the abandoned transaction saw rather
    /// than the committed database — a reader can miss rows another connection has already
    /// committed. `SqliteMetastore` needs no equivalent because `BEGIN IMMEDIATE` takes no
    /// snapshot.
    ///
    /// Clearing on acquire covers every borrower, transactional or not, in one place.
    async fn clear_open_transaction(conn: &Connection) -> CatalogResult<()> {
        // Connection-local state, so a clean connection — the overwhelmingly common case —
        // pays no round trip here. Only a definite autocommit skips the rollback: if the
        // state cannot be read, issuing one is the safe direction, and it is harmless on a
        // connection that has no transaction open.
        if matches!(conn.is_autocommit(), Ok(true)) {
            return Ok(());
        }

        conn.execute("ROLLBACK", ())
            .await
            .map_err(|err| CatalogError::Database {
                message: format!(
                    "Failed to roll back a transaction left open on a pooled Turso connection: {err}"
                ),
            })?;

        Self::require_autocommit(conn.is_autocommit())
    }

    /// Decide whether a connection may be handed to a borrower from the autocommit state
    /// read after [`Self::clear_open_transaction`] cleaned it up.
    ///
    /// The rollback reporting success is not on its own proof that the transaction ended,
    /// and an unreadable state is a refusal rather than a pass. Both alternatives hand the
    /// borrower the MVCC snapshot the cleanup exists to discard, and a stale read is the
    /// worse half of the pair: a failed acquire propagates to a caller that can retry or
    /// give up, whereas a silent stale read makes the cold-tier GC delete a live file.
    fn require_autocommit(state: turso::Result<bool>) -> CatalogResult<()> {
        match state {
            Ok(true) => Ok(()),
            Ok(false) => Err(CatalogError::Database {
                message:
                    "A pooled Turso connection is still in a transaction after rolling it back"
                        .to_string(),
            }),
            Err(err) => Err(CatalogError::Database {
                message: format!(
                    "Cannot confirm a pooled Turso connection is out of its transaction: {err}"
                ),
            }),
        }
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
            reinsert_sequence BIGINT,
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
    /// `table_id` stores the 16 raw bytes of the UUID (`BLOB`), not the 36-char
    /// text — a pure re-encoding (via `cayenne_catalog::insert_record_table_id_value`
    /// / `metastore::table_id_to_key_bytes`) that cuts the WAL/journal volume of
    /// hot upsert bursts; see the `SQLite` `INSERT_RECORD_TABLE_DDL` doc for the
    /// full rationale and correctness contract. The catalog builders bind the
    /// same `MetastoreValue::Blob` regardless of backend, so this column shape
    /// must match `SQLite`.
    ///
    /// The `cayenne_table(table_id)` foreign key is dropped: under
    /// `PRAGMA foreign_keys = ON` a `BLOB` child value can never satisfy the
    /// `TEXT` parent key. The cascade was belt-and-suspenders (the table is
    /// cleared at every checkpoint/overwrite, `drop_table` deletes it
    /// explicitly, and `snapshot::import_dataset` now does too).
    ///
    /// Unlike the `SQLite` backend, this table is NOT declared `WITHOUT ROWID`.
    /// `WITHOUT ROWID` is not currently supported by Turso under the `mvcc`
    /// journal mode that the metastore relies on for `BEGIN CONCURRENT`
    /// parallel writers (Turso rejects it with "WITHOUT ROWID tables are not
    /// supported in MVCC mode").
    const INSERT_RECORD_TABLE_DDL: &'static str = r"
        CREATE TABLE IF NOT EXISTS cayenne_insert_record (
            table_id BLOB NOT NULL,
            pk_bytes BLOB NOT NULL,
            sequence_number BIGINT NOT NULL,
            PRIMARY KEY (table_id, pk_bytes)
        )
    ";

    /// Schema for the `cayenne_pending_write_back` table (durable federated
    /// write-back, #11838). See the `SQLite` `PENDING_WRITE_BACK_TABLE_DDL` doc
    /// for semantics. Plain rowid table (Turso rejects `WITHOUT ROWID` in MVCC).
    const PENDING_WRITE_BACK_TABLE_DDL: &'static str = r"
        CREATE TABLE IF NOT EXISTS cayenne_pending_write_back (
            table_id BLOB NOT NULL,
            pk_bytes BLOB NOT NULL,
            sequence_number BIGINT NOT NULL,
            first_marked_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
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
            num_rows_exact INTEGER NOT NULL DEFAULT 1,
            FOREIGN KEY (table_id) REFERENCES cayenne_table(table_id) ON DELETE CASCADE
        )
    ";

    const SNAPSHOT_FILE_STATISTICS_TABLE_DDL: &'static str = r"
        CREATE TABLE IF NOT EXISTS cayenne_snapshot_file_statistics (
            table_id TEXT NOT NULL,
            snapshot_id TEXT NOT NULL,
            file_path TEXT NOT NULL,
            file_size_bytes BIGINT NOT NULL,
            num_rows BIGINT NOT NULL DEFAULT 0,
            statistics_blob BLOB NOT NULL,
            FOREIGN KEY (table_id) REFERENCES cayenne_table(table_id) ON DELETE CASCADE,
            PRIMARY KEY (table_id, snapshot_id, file_path)
        )
    ";

    /// Authoritative per-snapshot data-file manifest. Mirrors the `SQLite`
    /// `SNAPSHOT_FILE_TABLE_DDL`: the complete file set per snapshot (not the
    /// best-effort `cayenne_snapshot_file_statistics` cache), with each file's
    /// commit-seq range for seq-prefix compaction baking.
    const SNAPSHOT_FILE_TABLE_DDL: &'static str = r"
        CREATE TABLE IF NOT EXISTS cayenne_snapshot_file (
            table_id TEXT NOT NULL,
            snapshot_id TEXT NOT NULL,
            file_path TEXT NOT NULL,
            row_count BIGINT NOT NULL DEFAULT 0,
            file_size_bytes BIGINT NOT NULL DEFAULT 0,
            min_sequence BIGINT NOT NULL DEFAULT 0,
            max_sequence BIGINT NOT NULL DEFAULT 0,
            digest TEXT,
            FOREIGN KEY (table_id) REFERENCES cayenne_table(table_id) ON DELETE CASCADE,
            PRIMARY KEY (table_id, snapshot_id, file_path)
        )
    ";

    /// Cold-tier object-store manifest. Mirrors the `SQLite`
    /// `COLD_TIER_FILE_TABLE_DDL`: table-scoped, append-only, stats blob inline.
    /// Plain-rowid and pragma-free for Turso/libSQL portability.
    const COLD_TIER_FILE_TABLE_DDL: &'static str = r"
        CREATE TABLE IF NOT EXISTS cayenne_cold_tier_file (
            table_id TEXT NOT NULL,
            file_url TEXT NOT NULL,
            row_count BIGINT NOT NULL DEFAULT 0,
            file_size_bytes BIGINT NOT NULL DEFAULT 0,
            min_sequence BIGINT NOT NULL DEFAULT 0,
            max_sequence BIGINT NOT NULL DEFAULT 0,
            statistics_blob BLOB NOT NULL,
            pk_bloom_blob BLOB,
            FOREIGN KEY (table_id) REFERENCES cayenne_table(table_id) ON DELETE CASCADE,
            PRIMARY KEY (table_id, file_url)
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
        let conn = self.pool().await?.acquire().await?;

        // Refuse to open a catalog written by a newer, incompatible Spice build
        // BEFORE running any migration against it (a fresh/legacy DB reads 0).
        let mut version_rows =
            conn.query("PRAGMA user_version", ())
                .await
                .map_err(|e| CatalogError::Database {
                    message: format!("Failed to read metastore schema version: {e}"),
                })?;
        let stored_version = match version_rows.next().await {
            Ok(Some(row)) => match row.get_value(0) {
                Ok(TursoValue::Integer(v)) => v,
                _ => 0,
            },
            _ => 0,
        };
        drop(version_rows);
        super::ensure_supported_schema_version(stored_version)?;

        // Create tables
        let schema_sql = format!(
            "{}; {}; {}; {}; {}; {}; {}; {}; {}; {}; {}; {}; {}; {};",
            Self::TABLE_TABLE_DDL,
            Self::TABLE_NAME_UNIQUE_INDEX_DDL,
            Self::DELETE_FILE_TABLE_DDL,
            Self::PARTITION_TABLE_DDL,
            Self::INSERT_RECORD_TABLE_DDL,
            Self::PENDING_WRITE_BACK_TABLE_DDL,
            Self::SNAPSHOT_SEQUENCE_TABLE_DDL,
            Self::TABLE_STATISTICS_DDL,
            Self::SNAPSHOT_FILE_STATISTICS_TABLE_DDL,
            Self::SNAPSHOT_FILE_TABLE_DDL,
            Self::COLD_TIER_FILE_TABLE_DDL,
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
        // Whether the maintained `num_rows` is a provably-exact live count. Legacy
        // rows predate the mem-tier drift fix; DEFAULT 1 trusts their count once
        // (the next mem-tier checkpoint delta taints a drifted one to 0; only a
        // full-rewrite `Set` restores exactness). See the sqlite note.
        let _ = conn
            .execute(
                "ALTER TABLE cayenne_table_statistics ADD COLUMN num_rows_exact INTEGER NOT NULL DEFAULT 1",
                (),
            )
            .await;
        // Metadata-only publish: per-commit reinsert sequence on delete-file rows;
        // NULL on legacy rows falls back to cayenne_insert_record. Forward-upgrade
        // safe; downgrade requires a catalog rebuild (see the sqlite backfill note).
        // The `user_version` gate at the top / stamp at the bottom of this fn turns
        // an unsafe downgrade into a loud failure instead of silent row loss.
        let _ = conn
            .execute(
                "ALTER TABLE cayenne_delete_file ADD COLUMN reinsert_sequence BIGINT",
                (),
            )
            .await;

        // End-to-end data-file integrity digest (opt-in
        // `cayenne_integrity_checksums`). NULL on legacy rows / feature-off rows
        // → verification skipped; forward- and downgrade-safe. Appended last to
        // match the CREATE TABLE and EXPECTED_TABLES column order.
        let _ = conn
            .execute(
                "ALTER TABLE cayenne_snapshot_file ADD COLUMN digest TEXT",
                (),
            )
            .await;

        // Per-cold-file PK existence bloom. NULL (legacy / non-upsert /
        // over-cap) makes the keyset rebuild fall back to the exact cold
        // scan, so the column is forward- and downgrade-safe. Appended
        // last to match CREATE TABLE and EXPECTED_TABLES column order.
        let _ = conn
            .execute(
                "ALTER TABLE cayenne_cold_tier_file ADD COLUMN pk_bloom_blob BLOB",
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

        // Migrate a legacy `cayenne_insert_record` to the current shape: a
        // composite PK `(table_id, pk_bytes)` whose `table_id` is the 16 raw
        // UUID bytes (`BLOB`) and which carries no foreign key (see
        // `INSERT_RECORD_TABLE_DDL`). Two legacy layouts predate this and both
        // store `table_id` as `TEXT`: the pre-composite UUID `insert_record_id`
        // TEXT PK + redundant `UNIQUE(table_id, pk_bytes)`, and the composite-PK
        // TEXT-`table_id` shape with a `cayenne_table(table_id)` foreign key.
        // Both are detected by a single check — the declared type of the
        // `table_id` column is not `BLOB` — and migrated identically: read the
        // legacy rows out, re-encode each TEXT `table_id` to the raw-bytes key
        // via `table_id_to_key_bytes` (the same function the write path uses),
        // recreate the table in the new shape, and re-insert.
        //
        // The table is ephemeral (cleared at every checkpoint and recoverable
        // from the snapshot), but the copy-forward preserves in-flight
        // pre-checkpoint re-insert sequences across the upgrade at trivial cost.
        let mut ir_cols = conn
            .query("PRAGMA table_info('cayenne_insert_record')", ())
            .await
            .map_err(|e| CatalogError::Database {
                message: format!("Failed to read cayenne_insert_record schema for migration: {e}"),
            })?;
        let mut table_id_is_blob = false;
        loop {
            match ir_cols.next().await {
                // PRAGMA table_info: name at index 1, declared type at index 2.
                Ok(Some(row)) => {
                    if let (Ok(turso::Value::Text(name)), Ok(turso::Value::Text(col_type))) =
                        (row.get_value(1), row.get_value(2))
                        && name == "table_id"
                        && col_type.eq_ignore_ascii_case("BLOB")
                    {
                        table_id_is_blob = true;
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
        if !table_id_is_blob {
            // Read the legacy rows out (TEXT `table_id`) before recreating.
            let mut legacy_rows: Vec<(String, Vec<u8>, i64)> = Vec::new();
            let mut rows = conn
                .query(
                    "SELECT table_id, pk_bytes, sequence_number FROM cayenne_insert_record",
                    (),
                )
                .await
                .map_err(|e| CatalogError::Database {
                    message: format!("Failed to read legacy cayenne_insert_record rows: {e}"),
                })?;
            loop {
                match rows.next().await {
                    Ok(Some(row)) => {
                        let Ok(turso::Value::Text(table_id)) = row.get_value(0) else {
                            return Err(CatalogError::Database {
                                message: "legacy cayenne_insert_record.table_id is not TEXT"
                                    .to_string(),
                            });
                        };
                        let Ok(turso::Value::Blob(pk_bytes)) = row.get_value(1) else {
                            return Err(CatalogError::Database {
                                message: "legacy cayenne_insert_record.pk_bytes is not BLOB"
                                    .to_string(),
                            });
                        };
                        let Ok(turso::Value::Integer(sequence_number)) = row.get_value(2) else {
                            return Err(CatalogError::Database {
                                message:
                                    "legacy cayenne_insert_record.sequence_number is not INTEGER"
                                        .to_string(),
                            });
                        };
                        legacy_rows.push((table_id, pk_bytes, sequence_number));
                    }
                    Ok(None) => break,
                    Err(e) => {
                        return Err(CatalogError::Database {
                            message: format!(
                                "Failed to read legacy cayenne_insert_record rows: {e}"
                            ),
                        });
                    }
                }
            }
            drop(rows);

            // NOTE: not `WITHOUT ROWID` here, intentionally. Turso does not
            // currently support `WITHOUT ROWID` tables under the `mvcc` journal
            // mode this metastore uses for `BEGIN CONCURRENT` (see
            // `INSERT_RECORD_TABLE_DDL`).
            conn.execute_batch(
                "DROP TABLE cayenne_insert_record;
                CREATE TABLE cayenne_insert_record (
                    table_id BLOB NOT NULL,
                    pk_bytes BLOB NOT NULL,
                    sequence_number BIGINT NOT NULL,
                    PRIMARY KEY (table_id, pk_bytes)
                );",
            )
            .await
            .map_err(|e| CatalogError::Database {
                message: format!("Failed to migrate cayenne_insert_record to composite PK: {e}"),
            })?;

            for (table_id, pk_bytes, sequence_number) in legacy_rows {
                let params: Vec<turso::Value> = vec![
                    turso::Value::Blob(crate::metastore::table_id_to_key_bytes(&table_id)),
                    turso::Value::Blob(pk_bytes),
                    turso::Value::Integer(sequence_number),
                ];
                conn.execute(
                    "INSERT OR REPLACE INTO cayenne_insert_record \
                     (table_id, pk_bytes, sequence_number) VALUES (?1, ?2, ?3)",
                    params,
                )
                .await
                .map_err(|e| CatalogError::Database {
                    message: format!("Failed to re-insert migrated cayenne_insert_record row: {e}"),
                })?;
            }
        }

        // Must run AFTER the `published` column migration above: the partial index
        // predicate (`WHERE published = 0`) references the column, so creating it
        // first on a pre-flag metastore would fail and abort `init_schema`.
        conn.execute(Self::INLINED_DELETE_UNPUBLISHED_INDEX_DDL, ())
            .await
            .map_err(|e| CatalogError::Database {
                message: format!("Failed to create inlined_delete unpublished index: {e}"),
            })?;

        // Stamp the current schema version now that all migrations have succeeded,
        // so a later downgrade to a build with a lower max version fails loudly at
        // the gate above instead of returning silently wrong results.
        conn.execute(
            &format!(
                "PRAGMA user_version = {}",
                super::CAYENNE_METASTORE_SCHEMA_VERSION
            ),
            (),
        )
        .await
        .map_err(|e| CatalogError::Database {
            message: format!("Failed to stamp metastore schema version: {e}"),
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
        let conn = self.pool().await?.acquire().await?;

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
        let conn = self.pool().await?.acquire().await?;

        conn.execute_batch(sql)
            .await
            .map_err(|e| CatalogError::Database {
                message: format!("Failed to execute batch: {e}"),
            })?;

        Ok(())
    }

    async fn execute_transaction_batch(&self, sql: &str) -> CatalogResult<()> {
        let conn = self.pool().await?.acquire().await?;
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
        let conn = self.pool().await?.acquire().await?;

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
        let conn = self.pool().await?.acquire().await?;

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
        // `acquire` hands out a connection in autocommit, so no leftover transaction
        // state can precede this `BEGIN`.
        let guard = self.pool().await?.acquire().await?;

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
            let rollback = async move {
                tracing::debug!(
                    "TursoTransaction dropped without explicit commit or rollback; \
                     attempting auto-rollback"
                );
                if let Err(err) = guard.execute("ROLLBACK", ()).await {
                    tracing::error!("Failed to auto-rollback TursoTransaction on drop: {err}");
                }
                // `guard` is dropped here, releasing the pool slot.
            };
            if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                runtime.spawn(rollback);
            } else {
                // No ambient Tokio runtime (the transaction was dropped from a
                // non-Tokio thread). `turso`'s connection I/O is Tokio-based, so
                // `futures::executor::block_on` would run it without a reactor
                // and could panic or stall. Build a small current-thread Tokio
                // runtime to drive the best-effort rollback, logging if even
                // the runtime cannot be created.
                std::thread::spawn(move || {
                    match tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                    {
                        Ok(rt) => rt.block_on(rollback),
                        Err(err) => tracing::error!(
                            "Failed to build fallback Tokio runtime to auto-rollback TursoTransaction on drop: {err}"
                        ),
                    }
                });
            }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_metastore() -> (tempfile::TempDir, TursoMetastore) {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("cayenne_test.db");
        let metastore = TursoMetastore::new(format!("libsql://{}", db_path.display()));
        (dir, metastore)
    }

    /// Count the rows of `t` over `conn`, which reads whatever snapshot `conn` is on.
    async fn count_rows(conn: &Connection) -> i64 {
        let mut stmt = conn
            .prepare_cached("SELECT COUNT(*) FROM t")
            .await
            .expect("prepare count");
        let mut rows = stmt.query(()).await.expect("run count");
        let row = rows
            .next()
            .await
            .expect("fetch count row")
            .expect("count returns a row");
        match row.get_value(0).expect("read count") {
            TursoValue::Integer(n) => n,
            other => panic!("COUNT(*) should be an integer, got {other:?}"),
        }
    }

    /// Leave slot `idx` inside an open `BEGIN CONCURRENT`, holding an MVCC snapshot, the
    /// way a holder that could not finish its transaction does. The read pins the snapshot
    /// before the guard is released back to the pool.
    async fn leak_open_transaction(pool: &Arc<TursoConnectionPool>, idx: usize) {
        let guard = Arc::clone(&pool.conns[idx]).lock_owned().await;
        guard
            .execute("BEGIN CONCURRENT", ())
            .await
            .expect("begin concurrent");
        count_rows(&guard).await;
        drop(guard);
    }

    /// A connection returned to the pool mid-transaction must not serve its stale MVCC
    /// snapshot to the next borrower. This is the read that loses rows in the cold-tier GC
    /// path: the GC root lists live files off a pooled connection, and a snapshot taken
    /// before a promotion committed omits the files the promotion just published, so GC
    /// deletes a file the manifest still references.
    #[tokio::test]
    async fn a_connection_left_in_a_transaction_does_not_serve_a_stale_snapshot() {
        let (_dir, metastore) = temp_metastore();
        metastore
            .execute_batch("CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY)")
            .await
            .expect("create table");

        let pool = Arc::clone(metastore.pool().await.expect("pool"));

        // Slot 0 abandons a transaction, pinning a snapshot of the empty table.
        leak_open_transaction(&pool, 0).await;

        // Another connection commits a row. Slot 0's abandoned snapshot predates it.
        let writer = Arc::clone(&pool.conns[1]).lock_owned().await;
        writer
            .execute("INSERT INTO t (id) VALUES (1)", ())
            .await
            .expect("insert committed row");
        drop(writer);

        // Slot 0, taken through the pool, must read the committed database.
        let guard = Arc::clone(&pool.conns[0]).lock_owned().await;
        TursoConnectionPool::clear_open_transaction(&guard)
            .await
            .expect("clear the abandoned transaction");

        assert_eq!(
            count_rows(&guard).await,
            1,
            "a pooled connection should read committed rows, not the snapshot of a \
             transaction its previous holder abandoned"
        );
    }

    /// The invariant `acquire` relies on: whatever state a borrower returned the
    /// connection in, the next one gets it in autocommit.
    #[tokio::test]
    async fn clearing_an_abandoned_transaction_restores_autocommit() {
        let (_dir, metastore) = temp_metastore();
        metastore
            .execute_batch("CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY)")
            .await
            .expect("create table");

        let pool = Arc::clone(metastore.pool().await.expect("pool"));
        leak_open_transaction(&pool, 0).await;

        let guard = Arc::clone(&pool.conns[0]).lock_owned().await;
        assert!(
            !guard.is_autocommit().expect("read autocommit state"),
            "the leak should have left slot 0 inside a transaction"
        );

        TursoConnectionPool::clear_open_transaction(&guard)
            .await
            .expect("clear the abandoned transaction");

        assert!(
            guard.is_autocommit().expect("read autocommit state"),
            "clearing should return the connection to autocommit"
        );
    }

    /// Clearing must be inert on a connection that has no transaction open — that is the
    /// path every ordinary acquire takes.
    #[tokio::test]
    async fn clearing_a_clean_connection_leaves_it_usable() {
        let (_dir, metastore) = temp_metastore();
        metastore
            .execute_batch("CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY)")
            .await
            .expect("create table");

        let pool = Arc::clone(metastore.pool().await.expect("pool"));
        let guard = Arc::clone(&pool.conns[0]).lock_owned().await;

        TursoConnectionPool::clear_open_transaction(&guard)
            .await
            .expect("clearing a clean connection should succeed");

        assert!(
            guard.is_autocommit().expect("read autocommit state"),
            "a clean connection should still be in autocommit"
        );
        guard
            .execute("INSERT INTO t (id) VALUES (7)", ())
            .await
            .expect("a cleared clean connection should still accept writes");
        assert_eq!(count_rows(&guard).await, 1);
    }

    /// The same guarantee through the production entry point: `acquire` itself, not just
    /// the routine it calls, must never hand out a connection carrying a stale snapshot.
    /// Acquiring as many times as the pool is wide visits every slot, including the leaked
    /// one, because round-robin advances one slot per uncontended acquire.
    #[tokio::test]
    async fn acquire_never_hands_out_a_stale_snapshot() {
        let (_dir, metastore) = temp_metastore();
        metastore
            .execute_batch("CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY)")
            .await
            .expect("create table");

        let pool = Arc::clone(metastore.pool().await.expect("pool"));
        leak_open_transaction(&pool, 0).await;

        let writer = Arc::clone(&pool.conns[1]).lock_owned().await;
        writer
            .execute("INSERT INTO t (id) VALUES (1)", ())
            .await
            .expect("insert committed row");
        drop(writer);

        for _ in 0..pool.conns.len() {
            let guard = pool.acquire().await.expect("acquire a pooled connection");
            assert!(
                guard.is_autocommit().expect("read autocommit state"),
                "acquire should hand out a connection in autocommit"
            );
            assert_eq!(
                count_rows(&guard).await,
                1,
                "every acquired connection should read the committed row"
            );
        }
    }

    /// The decision `acquire` makes once cleanup has run. Only a connection read as being
    /// in autocommit may be handed over; a connection still in its transaction, or one whose
    /// state cannot be read at all, is refused. Handing either one to a borrower would serve
    /// the stale snapshot the cleanup exists to discard, so the guard has to fail closed.
    #[test]
    fn a_connection_not_confirmed_in_autocommit_is_refused() {
        TursoConnectionPool::require_autocommit(Ok(true))
            .expect("a connection confirmed in autocommit is fit to hand out");

        let Err(still_open) = TursoConnectionPool::require_autocommit(Ok(false)) else {
            panic!("a connection still inside a transaction must not be handed out")
        };
        assert!(
            still_open.to_string().contains("still in a transaction"),
            "the error should name the transaction that survived the rollback, got: {still_open}"
        );

        let Err(unreadable) = TursoConnectionPool::require_autocommit(Err(turso::Error::Error(
            "connection is closed".to_string(),
        ))) else {
            panic!("a connection whose autocommit state cannot be read must not be handed out")
        };
        assert!(
            unreadable.to_string().contains("Cannot confirm"),
            "the error should say the state could not be confirmed, got: {unreadable}"
        );
    }
}
