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

//! `SQLite` implementation of the metastore backend.
//!
//! Uses `tokio-rusqlite` for a persistent connection managed by a background thread,
//! avoiding the overhead of opening a new connection for each operation.

use super::{
    ExecuteParams, MetastoreBackend, MetastoreGetValue, MetastoreRow, MetastoreTransaction,
    MetastoreValue, QueryParams, QueryRowParams, duplicate_delete_file_index_error_message,
};
use crate::catalog::{CatalogError, CatalogResult};
use async_trait::async_trait;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::{Mutex, OnceCell, OwnedMutexGuard};

const DELETE_FILE_TABLE_UNIQUE_INDEX_DDL: &str = "CREATE UNIQUE INDEX IF NOT EXISTS idx_cayenne_delete_file_table_path ON cayenne_delete_file(table_id, path)";
const SQLITE_PRAGMA_RETRY_DELAYS_MS: &[u64] = &[10, 25, 50, 100, 200];

/// `auto_vacuum` mode for the metastore DB.
///
/// A high-update upsert table (e.g. `district`) frees pages as it supersedes
/// rows; with the default `None` those pages stay on the freelist and are reused,
/// so the file plateaus at its high-water mark rather than growing unboundedly.
/// The page cache already keeps the *live* working set resident, so the freelist
/// pages are never read and reclaiming them is a disk-footprint concern, not a
/// latency one — and every reclaiming mode taxes the write path (see the
/// variants), so reclamation is opt-in and `None` is the default.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SqliteAutoVacuum {
    /// `SQLite` default: freed pages stay on the freelist and are reused for new
    /// inserts; the file plateaus. No write-path overhead. Recommended.
    #[default]
    None,
    /// Freed pages are tracked for reclamation by an explicit
    /// `PRAGMA incremental_vacuum`, which holds the write lock while it relocates
    /// and truncates — so reclamation must be driven off the hot path, never
    /// inside a CDC burst.
    Incremental,
    /// Reclaim each commit's freed pages as part of that commit: the file stays
    /// small continuously, at the cost of page-relocation work on *every*
    /// metastore write (trades write latency for disk footprint).
    Full,
}

impl SqliteAutoVacuum {
    /// Pragma argument, or `None` for the `SQLite` default (skip the redundant set).
    fn pragma_value(self) -> Option<&'static str> {
        match self {
            SqliteAutoVacuum::None => None,
            SqliteAutoVacuum::Incremental => Some("INCREMENTAL"),
            SqliteAutoVacuum::Full => Some("FULL"),
        }
    }
}

/// Tunable `SQLite` pragmas for the Cayenne metastore.
///
/// The defaults match what was previously hardcoded. The runtime overrides them
/// once at startup from `runtime.params` (`cayenne_metastore_*`) via
/// [`set_sqlite_metastore_config`], so a deployment sizes the page cache and
/// memory map to its host instead of inheriting a fixed large-host assumption.
/// Sized for the host because the cache is per-connection × pool size: a 256 MiB
/// cache on a 32-slot pool can reserve gigabytes for a single hot table's DB.
#[derive(Debug, Clone, Copy)]
pub struct SqliteMetastoreConfig {
    /// `cache_size` page cache in MiB (applied as `-<KiB>`).
    pub cache_size_mb: usize,
    /// `mmap_size` in bytes.
    pub mmap_size_bytes: i64,
    /// `busy_timeout` in milliseconds.
    pub busy_timeout_ms: u64,
    /// `wal_autocheckpoint` threshold in pages.
    pub wal_autocheckpoint_pages: u32,
    /// `auto_vacuum` mode. Takes effect only on a fresh DB (an existing DB needs
    /// a full VACUUM to change it). Defaults to [`SqliteAutoVacuum::None`].
    pub auto_vacuum: SqliteAutoVacuum,
}

impl Default for SqliteMetastoreConfig {
    fn default() -> Self {
        Self {
            cache_size_mb: 256,
            mmap_size_bytes: 1_073_741_824, // 1 GiB
            busy_timeout_ms: 30_000,
            wal_autocheckpoint_pages: 10_000,
            auto_vacuum: SqliteAutoVacuum::None,
        }
    }
}

/// Process-wide `SQLite` metastore pragma config. Connections opened after a call
/// to [`set_sqlite_metastore_config`] use the new values; unset → the defaults.
static SQLITE_METASTORE_CONFIG: std::sync::LazyLock<std::sync::RwLock<SqliteMetastoreConfig>> =
    std::sync::LazyLock::new(|| std::sync::RwLock::new(SqliteMetastoreConfig::default()));

/// Install the process-wide `SQLite` metastore pragma config. Called once at
/// startup by the runtime; later calls replace it (tests). A poisoned lock is
/// ignored — a metastore that keeps its prior/default pragmas is far better than
/// a panic on the catalog setup path.
pub fn set_sqlite_metastore_config(config: SqliteMetastoreConfig) {
    if let Ok(mut guard) = SQLITE_METASTORE_CONFIG.write() {
        *guard = config;
    }
}

fn sqlite_metastore_config() -> SqliteMetastoreConfig {
    SQLITE_METASTORE_CONFIG
        .read()
        .map(|cfg| *cfg)
        .unwrap_or_default()
}

fn is_sqlite_lock_error(error: &tokio_rusqlite::Error<rusqlite::Error>) -> bool {
    matches!(
        error,
        tokio_rusqlite::Error::Error(rusqlite::Error::SqliteFailure(err, _))
            if matches!(
                err.code,
                rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked
            )
    )
}

async fn configure_sqlite_connection(
    conn: &tokio_rusqlite::Connection,
) -> Result<(), tokio_rusqlite::Error<rusqlite::Error>> {
    // Resolve the tunable pragmas once. Defaults and rationale live on
    // `SqliteMetastoreConfig`; the runtime overrides them via
    // `set_sqlite_metastore_config` from `runtime.params`.
    let cfg = sqlite_metastore_config();
    let cache_size_kib =
        i64::try_from(cfg.cache_size_mb.saturating_mul(1024)).unwrap_or(262_144);
    let mut retry_delays = SQLITE_PRAGMA_RETRY_DELAYS_MS.iter();
    loop {
        let result = conn
            .call(move |conn| {
                conn.busy_timeout(std::time::Duration::from_millis(cfg.busy_timeout_ms))?;
                // auto_vacuum must be set before the first table is created (it is
                // a no-op on an existing DB without a full VACUUM). NONE is the
                // SQLite default, so skip the pragma entirely for it.
                if let Some(mode) = cfg.auto_vacuum.pragma_value() {
                    conn.pragma_update(None, "auto_vacuum", mode)?;
                }
                conn.pragma_update(None, "journal_mode", "WAL")?;
                conn.pragma_update(None, "synchronous", "NORMAL")?;
                conn.pragma_update(None, "cache_size", -cache_size_kib)?;
                conn.pragma_update(None, "foreign_keys", true)?;
                conn.pragma_update(None, "temp_store", "memory")?;
                conn.pragma_update(None, "mmap_size", cfg.mmap_size_bytes)?;
                conn.pragma_update(None, "wal_autocheckpoint", cfg.wal_autocheckpoint_pages)?;

                Ok::<_, rusqlite::Error>(())
            })
            .await;

        match result {
            Ok(()) => return Ok(()),
            Err(error) if is_sqlite_lock_error(&error) => {
                let Some(delay_ms) = retry_delays.next() else {
                    return Err(error);
                };
                tokio::time::sleep(std::time::Duration::from_millis(*delay_ms)).await;
            }
            Err(error) => return Err(error),
        }
    }
}

/// Round-robin connection pool for the [`SqliteMetastore`].
///
/// `SQLite` WAL mode allows concurrent readers and serializes writers at the
/// engine level. Having K independent connections means N concurrent callers
/// spread across K slots: for N ≤ K every caller finds a free slot immediately;
/// for N > K callers share proportionally, reducing the per-table wait from
/// O(N·RTT) to O(⌈N/K⌉·RTT).
///
/// Pool size is `min(available_parallelism, 32)` (minimum 2). If
/// `available_parallelism()` fails (rare — e.g. seccomp-restricted
/// environments), `K` falls back to 4. `SQLite` WAL mode allows many
/// concurrent readers per database file (read-only operations don't take
/// the WAL write lock), so a larger pool lifts the read-side concurrency
/// ceiling for metadata-heavy workloads — e.g. 64-core deployments running
/// concurrent scans against many tables, where every scan pays one or more
/// metastore reads (table metadata, snapshot file lists, deletion-vector
/// loads, stats lookups). Writes still serialize at the WAL layer
/// regardless of pool size; this is fine because writes are
/// O(commits-per-second) while reads are
/// O(queries-per-second × per-query-metadata-fanout).
struct SqliteConnectionPool {
    conns: Vec<Arc<Mutex<tokio_rusqlite::Connection>>>,
    next: AtomicUsize,
}

impl SqliteConnectionPool {
    /// Acquire a connection using round-robin with try-first heuristic.
    ///
    /// Tries each slot starting from the round-robin index; returns the first
    /// slot that is immediately free (`try_lock_owned` succeeds). Falls back to
    /// `lock_owned().await` on the starting slot if all slots appear busy.
    async fn acquire(&self) -> OwnedMutexGuard<tokio_rusqlite::Connection> {
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

/// `SQLite`-based metastore backend with a persistent connection pool.
///
/// Maintains K independent `tokio-rusqlite` connections to eliminate the
/// single-mutex serialization bottleneck that capped cross-table CDC
/// throughput at one commit per RTT regardless of table count.
pub struct SqliteMetastore {
    connection_string: String,
    /// Round-robin pool of K independent connections shared across all
    /// operations (reads, writes, and transactions).
    ///
    /// K = `min(available_parallelism, 32)` (or 4 if
    /// `available_parallelism()` fails, with a minimum of 2 — see the
    /// [`SqliteConnectionPool`] doc comment for the rationale). Lazily
    /// initialised on first use. `begin_transaction` holds an
    /// [`OwnedMutexGuard`] on one pool slot for the full transaction
    /// lifetime.
    pool: OnceCell<Arc<SqliteConnectionPool>>,
}

/// Convert a `tokio_rusqlite::Error` to a `CatalogError`, distinguishing constraint violations.
fn convert_tokio_rusqlite_error(
    e: tokio_rusqlite::Error<rusqlite::Error>,
    context: &str,
) -> CatalogError {
    match e {
        tokio_rusqlite::Error::Error(rusqlite::Error::SqliteFailure(err, msg))
            if err.code == rusqlite::ErrorCode::ConstraintViolation =>
        {
            CatalogError::ConstraintViolation {
                message: msg.unwrap_or_else(|| "Constraint violation".to_string()),
            }
        }
        tokio_rusqlite::Error::Error(sqlite_err) => CatalogError::Sqlite { source: sqlite_err },
        other => CatalogError::Database {
            message: format!("{context}: {other}"),
        },
    }
}

impl std::fmt::Debug for SqliteMetastore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqliteMetastore")
            .field("connection_string", &self.connection_string)
            .finish_non_exhaustive()
    }
}

impl SqliteMetastore {
    /// Create a new `SQLite` metastore.
    #[must_use]
    pub fn new(connection_string: impl Into<String>) -> Self {
        Self {
            connection_string: connection_string.into(),
            pool: OnceCell::new(),
        }
    }

    /// Get the database file path from the connection string.
    fn db_path(&self) -> &str {
        self.connection_string
            .strip_prefix("sqlite://")
            .unwrap_or(&self.connection_string)
    }

    /// Open a configured `SQLite` connection.
    ///
    /// The connection is configured with performance optimizations:
    /// - WAL mode for non-blocking reads/writes
    /// - Busy timeout to reduce lock contention errors
    /// - NORMAL synchronous mode (safe with WAL)
    /// - Memory cache and temp storage for performance
    /// - Foreign keys enabled
    ///
    async fn open_connection(&self) -> CatalogResult<tokio_rusqlite::Connection> {
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
                        "Failed to sync parent directory {parent_display} after creating SQLite catalog DB directory {db_dir_display} (subsequent DB writes will still be durable): {error}"
                    ),
                    Err(error) => tracing::warn!(
                        "Failed to join SQLite catalog DB parent directory sync task for {parent_display}: {error}"
                    ),
                }
            }
        }

        let conn = tokio_rusqlite::Connection::open(db_path)
            .await
            .map_err(|e| CatalogError::Database {
                message: format!("Failed to open SQLite database: {e}"),
            })?;

        configure_sqlite_connection(&conn).await.map_err(
            |e: tokio_rusqlite::Error<rusqlite::Error>| CatalogError::Database {
                message: format!("Failed to configure SQLite pragmas: {e}"),
            },
        )?;

        Ok(conn)
    }

    /// Return the connection pool, initialising it lazily on first call.
    ///
    /// Opens K = `min(available_parallelism, 32)` connections once and reuses
    /// them for the lifetime of the metastore. If `available_parallelism()`
    /// fails (rare — e.g. seccomp-restricted environments), K falls back to
    /// 4. K is then clamped to a minimum of 2 so single-core systems still
    /// have one slot reserved for read-while-write. All operations draw from
    /// the same pool; `begin_transaction` holds an [`OwnedMutexGuard`] on
    /// the acquired slot for the full transaction lifetime.
    async fn pool(&self) -> CatalogResult<&Arc<SqliteConnectionPool>> {
        self.pool
            .get_or_try_init(|| async {
                let k = std::thread::available_parallelism()
                    .map_or(4, |n| n.get().min(32))
                    .max(2);
                let mut conns = Vec::with_capacity(k);
                for _ in 0..k {
                    conns.push(Arc::new(Mutex::new(self.open_connection().await?)));
                }
                Ok(Arc::new(SqliteConnectionPool {
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
    const INSERT_RECORD_TABLE_DDL: &'static str = r"
        CREATE TABLE IF NOT EXISTS cayenne_insert_record (
            insert_record_id TEXT PRIMARY KEY,
            table_id TEXT NOT NULL,
            pk_bytes BLOB NOT NULL,
            sequence_number BIGINT NOT NULL,
            FOREIGN KEY (table_id) REFERENCES cayenne_table(table_id) ON DELETE CASCADE,
            UNIQUE(table_id, pk_bytes)
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
    ///
    /// Stores a single row per table holding a serialized Vortex `FileStatistics`
    /// flatbuffer blob (min, max, null count), a live `num_rows` count, and an
    /// optional `ndv_sketches` blob of per-column `HyperLogLog` sketches. The row is
    /// upserted on every write and merged into the running per-table aggregate.
    /// Consumers must treat these values as optimization hints.
    const TABLE_STATISTICS_DDL: &'static str = r"
        CREATE TABLE IF NOT EXISTS cayenne_table_statistics (
            table_id TEXT NOT NULL PRIMARY KEY,
            statistics_blob BLOB NOT NULL,
            num_rows BIGINT NOT NULL DEFAULT 0,
            ndv_sketches BLOB,
            FOREIGN KEY (table_id) REFERENCES cayenne_table(table_id) ON DELETE CASCADE
        )
    ";

    /// Schema for the `cayenne_pk_index` table.
    ///
    /// One row per table holding the serialized primary-key existence bloom
    /// checkpoint (see `provider::table`), tagged with the snapshot id it covers.
    /// Lets restart / snapshot-bootstrap skip the full-table keyset rebuild;
    /// captured in metastore snapshots via `EXPECTED_TABLES`.
    const PK_INDEX_TABLE_DDL: &'static str = r"
        CREATE TABLE IF NOT EXISTS cayenne_pk_index (
            table_id TEXT NOT NULL PRIMARY KEY,
            snapshot_id TEXT NOT NULL,
            index_blob BLOB NOT NULL,
            FOREIGN KEY (table_id) REFERENCES cayenne_table(table_id) ON DELETE CASCADE
        )
    ";

    /// Schema for the `cayenne_inlined_data` table.
    ///
    /// Stores small batches of insert data as Arrow IPC blobs directly in the
    /// metastore, avoiding the overhead of creating individual Vortex files for
    /// each small write. A `CHECKPOINT` operation flushes accumulated inline data
    /// to consolidated Vortex files.
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

    /// Schema for the `cayenne_inlined_delete` table.
    ///
    /// Stores small batches of delete identifiers directly in the metastore.
    /// Flushed to deletion vector files during checkpoint.
    const INLINED_DELETE_TABLE_DDL: &'static str = r"
        CREATE TABLE IF NOT EXISTS cayenne_inlined_delete (
            inlined_id TEXT PRIMARY KEY,
            table_id TEXT NOT NULL,
            delete_ipc BLOB NOT NULL,
            delete_count BIGINT NOT NULL,
            sequence_number BIGINT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
            FOREIGN KEY (table_id) REFERENCES cayenne_table(table_id) ON DELETE CASCADE
        )
    ";

    const INLINED_DATA_INDEX_DDL: &'static str = "CREATE INDEX IF NOT EXISTS idx_cayenne_inlined_data_table_seq ON cayenne_inlined_data(table_id, sequence_number)";
    const INLINED_DELETE_INDEX_DDL: &'static str = "CREATE INDEX IF NOT EXISTS idx_cayenne_inlined_delete_table_seq ON cayenne_inlined_delete(table_id, sequence_number)";
}

/// `SQLite` row wrapper implementing `MetastoreRow`.
struct SqliteRow {
    values: Vec<MetastoreValue>,
}

impl MetastoreRow for SqliteRow {
    fn get_value(&self, index: usize) -> CatalogResult<MetastoreValue> {
        self.values
            .get(index)
            .cloned()
            .ok_or_else(|| CatalogError::Database {
                message: format!("Column index {index} out of bounds"),
            })
    }

    fn get_i64(&self, index: usize) -> CatalogResult<i64> {
        let value = self
            .values
            .get(index)
            .ok_or_else(|| CatalogError::Database {
                message: format!("Column index {index} out of bounds"),
            })?;
        i64::from_value(value)
    }

    fn get_string(&self, index: usize) -> CatalogResult<String> {
        let value = self
            .values
            .get(index)
            .ok_or_else(|| CatalogError::Database {
                message: format!("Column index {index} out of bounds"),
            })?;
        String::from_value(value)
    }

    fn get_bool(&self, index: usize) -> CatalogResult<bool> {
        let value = self
            .values
            .get(index)
            .ok_or_else(|| CatalogError::Database {
                message: format!("Column index {index} out of bounds"),
            })?;
        bool::from_value(value)
    }

    fn get_blob(&self, index: usize) -> CatalogResult<Vec<u8>> {
        let value = self
            .values
            .get(index)
            .ok_or_else(|| CatalogError::Database {
                message: format!("Column index {index} out of bounds"),
            })?;
        Vec::<u8>::from_value(value)
    }

    fn get_optional_i64(&self, index: usize) -> CatalogResult<Option<i64>> {
        let value = self
            .values
            .get(index)
            .ok_or_else(|| CatalogError::Database {
                message: format!("Column index {index} out of bounds"),
            })?;
        Option::<i64>::from_value(value)
    }

    fn get_optional_string(&self, index: usize) -> CatalogResult<Option<String>> {
        let value = self
            .values
            .get(index)
            .ok_or_else(|| CatalogError::Database {
                message: format!("Column index {index} out of bounds"),
            })?;
        Option::<String>::from_value(value)
    }

    fn get_optional_blob(&self, index: usize) -> CatalogResult<Option<Vec<u8>>> {
        let value = self
            .values
            .get(index)
            .ok_or_else(|| CatalogError::Database {
                message: format!("Column index {index} out of bounds"),
            })?;
        Option::<Vec<u8>>::from_value(value)
    }
}

/// Convert `rusqlite::Value` to `MetastoreValue`.
fn convert_sqlite_value(value: rusqlite::types::ValueRef<'_>) -> MetastoreValue {
    match value {
        rusqlite::types::ValueRef::Null => MetastoreValue::Null,
        rusqlite::types::ValueRef::Integer(i) => MetastoreValue::Integer(i),
        rusqlite::types::ValueRef::Real(_) => {
            // We don't use real numbers in metadata, treat as error
            MetastoreValue::Null
        }
        rusqlite::types::ValueRef::Text(t) => {
            // `into_owned()` on a `Cow::Owned` (invalid UTF-8 fallback) keeps the
            // already-allocated String. `.to_string()` would clone it again.
            MetastoreValue::Text(String::from_utf8_lossy(t).into_owned())
        }
        rusqlite::types::ValueRef::Blob(b) => MetastoreValue::Blob(b.to_vec()),
    }
}

/// Convert `MetastoreValue` to a `rusqlite::types::Value`, consuming the
/// source so Text/Blob payloads move without an extra heap copy.
fn to_sqlite_value(value: MetastoreValue) -> rusqlite::types::Value {
    match value {
        MetastoreValue::Integer(i) => rusqlite::types::Value::Integer(i),
        MetastoreValue::Text(s) => rusqlite::types::Value::Text(s),
        MetastoreValue::Bool(b) => rusqlite::types::Value::Integer(i64::from(b)),
        MetastoreValue::Blob(b) => rusqlite::types::Value::Blob(b),
        MetastoreValue::Null => rusqlite::types::Value::Null,
    }
}

#[async_trait]
impl MetastoreBackend for SqliteMetastore {
    async fn init_schema(&self) -> CatalogResult<()> {
        let guard = self.pool().await?.acquire().await;

        guard
            .call(|conn| {
                // Create tables in a transaction
                conn.execute_batch(&format!(
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
                ))?;

                // Backfill new columns for existing deployments (SQLite doesn't support IF NOT EXISTS for ALTER TABLE until v3.35)
                // Ignore errors when the column already exists to keep init idempotent.
                let _ = conn.execute(
                    "ALTER TABLE cayenne_table ADD COLUMN on_conflict_json TEXT",
                    [],
                );
                let _ = conn.execute(
                    "ALTER TABLE cayenne_table_statistics ADD COLUMN ndv_sketches BLOB",
                    [],
                );

                Ok::<_, rusqlite::Error>(())
            })
            .await
            .map_err(
                |e: tokio_rusqlite::Error<rusqlite::Error>| CatalogError::Database {
                    message: format!("Failed to initialize schema: {e}"),
                },
            )?;

        guard
            .call(|conn| {
                conn.execute(DELETE_FILE_TABLE_UNIQUE_INDEX_DDL, [])?;
                conn.execute(Self::INLINED_DATA_INDEX_DDL, [])?;
                conn.execute(Self::INLINED_DELETE_INDEX_DDL, [])?;
                Ok::<_, rusqlite::Error>(())
            })
            .await
            .map_err(
                |e: tokio_rusqlite::Error<rusqlite::Error>| CatalogError::Database {
                    message: duplicate_delete_file_index_error_message("SQLite", e),
                },
            )?;

        // Validate that existing tables match the expected schema.
        // This catches incompatible metadata databases from previous versions.
        // Drop the guard before validation — the callback acquires it per-table.
        drop(guard);
        let pool_ref = Arc::clone(self.pool().await?);
        super::validate_existing_schema(|table_name| {
            let pool = Arc::clone(&pool_ref);
            async move {
                let g = pool.acquire().await;
                g.call(move |conn| {
                    let mut stmt = conn.prepare(&format!("PRAGMA table_info('{table_name}')"))?;
                    let columns: Vec<String> = stmt
                        .query_map([], |row| row.get::<_, String>(1))?
                        .collect::<Result<Vec<_>, _>>()?;
                    Ok::<Vec<String>, rusqlite::Error>(columns)
                })
                .await
                .map_err(
                    |e: tokio_rusqlite::Error<rusqlite::Error>| CatalogError::Database {
                        message: format!("Failed to read table schema for validation: {e}"),
                    },
                )
            }
        })
        .await?;

        Ok(())
    }

    async fn execute(&self, params: ExecuteParams<'_>) -> CatalogResult<()> {
        let guard = self.pool().await?.acquire().await;
        let sql = params.sql.to_string();
        let param_values: Vec<rusqlite::types::Value> =
            params.params.into_iter().map(to_sqlite_value).collect();

        guard
            .call(move |conn| {
                let params_refs: Vec<&dyn rusqlite::ToSql> = param_values
                    .iter()
                    .map(|v| v as &dyn rusqlite::ToSql)
                    .collect();
                conn.prepare_cached(&sql)?.execute(params_refs.as_slice())?;
                Ok::<_, rusqlite::Error>(())
            })
            .await
            .map_err(|e| convert_tokio_rusqlite_error(e, "Failed to execute statement"))?;

        Ok(())
    }

    async fn execute_batch(&self, sql: &str) -> CatalogResult<()> {
        let guard = self.pool().await?.acquire().await;
        let sql_owned = sql.to_string();

        guard
            .call(move |conn| {
                conn.execute_batch(&sql_owned)?;
                Ok::<_, rusqlite::Error>(())
            })
            .await
            .map_err(
                |e: tokio_rusqlite::Error<rusqlite::Error>| CatalogError::Database {
                    message: format!("Failed to execute batch: {e}"),
                },
            )?;

        Ok(())
    }

    async fn execute_transaction_batch(&self, sql: &str) -> CatalogResult<()> {
        let guard = self.pool().await?.acquire().await;
        let batch_sql = format!("BEGIN TRANSACTION; {sql}; COMMIT;");

        guard
            .call(move |conn| {
                conn.execute_batch(&batch_sql).inspect_err(|_| {
                    let _ = conn.execute_batch("ROLLBACK");
                })?;
                Ok::<_, rusqlite::Error>(())
            })
            .await
            .map_err(
                |e: tokio_rusqlite::Error<rusqlite::Error>| CatalogError::Database {
                    message: format!("Failed to execute transaction batch: {e}"),
                },
            )?;

        Ok(())
    }

    async fn query_row<F, T>(&self, params: QueryRowParams<'_>, f: F) -> CatalogResult<T>
    where
        F: FnOnce(&dyn MetastoreRow) -> CatalogResult<T> + Send + 'static,
        T: Send + 'static,
    {
        let guard = self.pool().await?.acquire().await;
        let sql = params.sql.to_string();
        let param_values: Vec<rusqlite::types::Value> =
            params.params.into_iter().map(to_sqlite_value).collect();

        // Execute query and extract row values inside the closure
        let row_values = guard
            .call(move |conn| {
                let params_refs: Vec<&dyn rusqlite::ToSql> = param_values
                    .iter()
                    .map(|v| v as &dyn rusqlite::ToSql)
                    .collect();

                conn.prepare_cached(&sql)?
                    .query_row(params_refs.as_slice(), |row| {
                        let column_count = row.as_ref().column_count();
                        let mut values = Vec::with_capacity(column_count);

                        for i in 0..column_count {
                            let value = row.get_ref(i)?;
                            values.push(convert_sqlite_value(value));
                        }

                        Ok(values)
                    })
            })
            .await
            .map_err(
                |e: tokio_rusqlite::Error<rusqlite::Error>| CatalogError::Database {
                    message: format!("Failed to query row: {e}"),
                },
            )?;

        // Apply the callback outside the rusqlite closure to preserve CatalogError
        let sqlite_row = SqliteRow { values: row_values };
        f(&sqlite_row)
    }

    async fn query<F, T>(&self, params: QueryParams<'_>, f: F) -> CatalogResult<Vec<T>>
    where
        F: Fn(&dyn MetastoreRow) -> CatalogResult<T> + Send + 'static,
        T: Send + 'static,
    {
        let guard = self.pool().await?.acquire().await;
        let sql = params.sql.to_string();
        let param_values: Vec<rusqlite::types::Value> =
            params.params.into_iter().map(to_sqlite_value).collect();

        // Execute query and collect all row values inside the closure
        let all_row_values = guard
            .call(move |conn| {
                let params_refs: Vec<&dyn rusqlite::ToSql> = param_values
                    .iter()
                    .map(|v| v as &dyn rusqlite::ToSql)
                    .collect();

                let mut stmt = conn.prepare_cached(&sql)?;
                let rows = stmt.query_map(params_refs.as_slice(), |row| {
                    let column_count = row.as_ref().column_count();
                    let mut values = Vec::with_capacity(column_count);

                    for i in 0..column_count {
                        let value = row.get_ref(i)?;
                        values.push(convert_sqlite_value(value));
                    }

                    Ok(values)
                })?;

                let mut collected_rows = Vec::new();
                for row_result in rows {
                    collected_rows.push(row_result?);
                }

                Ok::<Vec<Vec<MetastoreValue>>, rusqlite::Error>(collected_rows)
            })
            .await
            .map_err(
                |e: tokio_rusqlite::Error<rusqlite::Error>| CatalogError::Database {
                    message: format!("Failed to query rows: {e}"),
                },
            )?;

        // Apply the callback outside the rusqlite closure to preserve CatalogError
        let mut results = Vec::with_capacity(all_row_values.len());
        for row_values in all_row_values {
            let sqlite_row = SqliteRow { values: row_values };
            results.push(f(&sqlite_row)?);
        }

        Ok(results)
    }

    async fn begin_transaction(&self) -> CatalogResult<Box<dyn MetastoreTransaction>> {
        let guard = self.pool().await?.acquire().await;

        // Defensively clear any leftover transaction state before BEGIN. A
        // prior `SqliteTransaction` whose `Drop` fired-and-forgot a ROLLBACK
        // via `tokio::spawn` can lose the rollback under runtime shutdown,
        // returning the connection to the pool inside an open transaction.
        // SQLite's `autocommit` flag tells us if a txn is pending; rolling
        // back only when needed avoids the noisy "no transaction is active"
        // error on clean connections.
        guard
            .call(|conn| {
                if !conn.is_autocommit() {
                    let _ = conn.execute_batch("ROLLBACK");
                }
                // Metastore transactions are write transactions. Acquiring the
                // reserved lock up front lets SQLite's busy timeout serialize
                // contending writers instead of failing later while upgrading a
                // deferred transaction after reads have already run.
                conn.execute_batch("BEGIN IMMEDIATE")?;
                Ok::<_, rusqlite::Error>(())
            })
            .await
            .map_err(
                |e: tokio_rusqlite::Error<rusqlite::Error>| CatalogError::Database {
                    message: format!("Failed to begin transaction: {e}"),
                },
            )?;

        Ok(Box::new(SqliteTransaction { conn: Some(guard) }))
    }

    async fn shutdown(&self) -> CatalogResult<()> {
        // WAL checkpoint and optimize on the first connection only.
        // Multiple concurrent checkpoints on the same WAL would conflict;
        // a single checkpoint covers the shared file.
        if let Some(pool) = self.pool.get()
            && let Some(conn) = pool.conns.first()
        {
            let guard = conn.lock().await;
            guard
                .call(|conn| {
                    // Check if WAL mode is enabled
                    let journal_mode: String =
                        conn.query_row("PRAGMA journal_mode", [], |row| row.get(0))?;

                    if journal_mode.eq_ignore_ascii_case("wal") {
                        tracing::info!("Truncating Cayenne catalog WAL log");
                        // Truncate the WAL log to persist changes and reduce file size
                        // wal_checkpoint returns results (busy, log, checkpointed), so we use query_row
                        let _: (i32, i32, i32) =
                            conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
                                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                            })?;
                    }

                    // Run optimize to improve query performance for future connections
                    // PRAGMA optimize may return rows indicating what was optimized
                    tracing::info!("Running optimize on Cayenne catalog");
                    let mut stmt = conn.prepare("PRAGMA optimize")?;
                    let mut rows = stmt.query([])?;
                    while rows.next()?.is_some() {} // Consume all results to ensure PRAGMA completes

                    Ok::<_, rusqlite::Error>(())
                })
                .await
                .map_err(
                    |e: tokio_rusqlite::Error<rusqlite::Error>| CatalogError::Database {
                        message: format!("Failed to shutdown catalog: {e}"),
                    },
                )?;
            // Note: We intentionally do not explicitly close the connections here.
            // Closing pool connections while other pool slots remain open would be
            // inconsistent; instead we rely on normal drop semantics to clean up
            // the background connections when the metastore is dropped.
        }

        Ok(())
    }
}

/// A transaction on a `SQLite` metastore connection.
///
/// Holds an [`OwnedMutexGuard`] on the underlying connection, ensuring
/// exclusive access for the lifetime of the transaction. The guard is
/// released when the transaction is committed, rolled back, or dropped.
///
/// If neither [`commit`](MetastoreTransaction::commit) nor
/// [`rollback`](MetastoreTransaction::rollback) is called, the transaction
/// is automatically rolled back on drop via a best-effort `ROLLBACK`.
pub struct SqliteTransaction {
    /// Exclusive lock on the connection. `None` after commit/rollback.
    conn: Option<OwnedMutexGuard<tokio_rusqlite::Connection>>,
}

impl Drop for SqliteTransaction {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            // Best-effort rollback — fire and forget since we're in drop.
            // tokio_rusqlite::Connection::call sends a closure to the bg
            // thread; it will execute even after this Drop returns.
            // We spawn a task to await the future properly.
            tokio::spawn(async move {
                let _ = conn
                    .call(|conn| {
                        let _ = conn.execute_batch("ROLLBACK");
                        Ok::<_, rusqlite::Error>(())
                    })
                    .await;
            });
        }
    }
}

#[async_trait]
impl MetastoreTransaction for SqliteTransaction {
    async fn execute(&self, params: ExecuteParams<'_>) -> CatalogResult<()> {
        let conn = self.conn.as_ref().ok_or_else(|| CatalogError::Database {
            message: "Transaction already completed".to_string(),
        })?;
        let sql = params.sql.to_string();
        let param_values: Vec<rusqlite::types::Value> =
            params.params.into_iter().map(to_sqlite_value).collect();

        conn.call(move |conn| {
            let params_refs: Vec<&dyn rusqlite::ToSql> = param_values
                .iter()
                .map(|v| v as &dyn rusqlite::ToSql)
                .collect();
            conn.prepare_cached(&sql)?.execute(params_refs.as_slice())?;
            Ok::<_, rusqlite::Error>(())
        })
        .await
        .map_err(|e| {
            convert_tokio_rusqlite_error(e, "Failed to execute statement in transaction")
        })?;

        Ok(())
    }

    async fn query_row_values(
        &self,
        params: QueryRowParams<'_>,
    ) -> CatalogResult<Vec<MetastoreValue>> {
        let conn = self.conn.as_ref().ok_or_else(|| CatalogError::Database {
            message: "Transaction already completed".to_string(),
        })?;
        let sql = params.sql.to_string();
        let param_values: Vec<rusqlite::types::Value> =
            params.params.into_iter().map(to_sqlite_value).collect();

        conn.call(move |conn| {
            let params_refs: Vec<&dyn rusqlite::ToSql> = param_values
                .iter()
                .map(|v| v as &dyn rusqlite::ToSql)
                .collect();

            conn.prepare_cached(&sql)?
                .query_row(params_refs.as_slice(), |row| {
                    let column_count = row.as_ref().column_count();
                    let mut values = Vec::with_capacity(column_count);

                    for i in 0..column_count {
                        let value = row.get_ref(i)?;
                        values.push(convert_sqlite_value(value));
                    }

                    Ok(values)
                })
        })
        .await
        .map_err(
            |e: tokio_rusqlite::Error<rusqlite::Error>| CatalogError::Database {
                message: format!("Failed to query row in transaction: {e}"),
            },
        )
    }

    async fn execute_batch(&self, sql: &str) -> CatalogResult<()> {
        let conn = self.conn.as_ref().ok_or_else(|| CatalogError::Database {
            message: "Transaction already completed".to_string(),
        })?;
        let sql_owned = sql.to_string();

        conn.call(move |conn| {
            conn.execute_batch(&sql_owned)?;
            Ok::<_, rusqlite::Error>(())
        })
        .await
        .map_err(
            |e: tokio_rusqlite::Error<rusqlite::Error>| CatalogError::Database {
                message: format!("Failed to execute batch in transaction: {e}"),
            },
        )?;

        Ok(())
    }

    async fn commit(mut self: Box<Self>) -> CatalogResult<()> {
        let conn = self.conn.take().ok_or_else(|| CatalogError::Database {
            message: "Transaction already completed".to_string(),
        })?;

        let commit_result = conn
            .call(|conn| {
                conn.execute_batch("COMMIT")?;
                Ok::<_, rusqlite::Error>(())
            })
            .await;

        match commit_result {
            Ok(()) => Ok(()),
            Err(e) => {
                // Best-effort rollback to leave the connection in a clean state.
                let _ = conn
                    .call(|conn| {
                        let _ = conn.execute_batch("ROLLBACK");
                        Ok::<_, rusqlite::Error>(())
                    })
                    .await;

                Err(CatalogError::Database {
                    message: format!("Failed to commit transaction: {e}"),
                })
            }
        }
    }

    async fn rollback(mut self: Box<Self>) -> CatalogResult<()> {
        let conn = self.conn.take().ok_or_else(|| CatalogError::Database {
            message: "Transaction already completed".to_string(),
        })?;

        conn.call(|conn| {
            conn.execute_batch("ROLLBACK")?;
            Ok::<_, rusqlite::Error>(())
        })
        .await
        .map_err(
            |e: tokio_rusqlite::Error<rusqlite::Error>| CatalogError::Database {
                message: format!("Failed to rollback transaction: {e}"),
            },
        )?;

        Ok(())
    }
}
