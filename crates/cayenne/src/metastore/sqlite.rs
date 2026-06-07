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

/// Default WAL-size cap (bytes) for [`SqliteMetastoreConfig::wal_truncate_threshold_bytes`]
/// — the size above which the background maintenance-tick checkpoint escalates
/// from PASSIVE to TRUNCATE (cycle-8 TASK A2). See that field for the rationale.
///
/// 160 MiB (cycle-10, bracketed by measurement). The sweep at SF-100 @10K txn/s:
/// at 512 MiB the WAL reached ~370 MB between drains and every writer acquisition
/// paid the large-WAL overhead (`writer_held` ~219 ms, `wait` 323 ms), each
/// TRUNCATE costing ~1.9 s (it drained half a gigabyte); at 48 MiB the TRUNCATEs
/// fired so often their brief writer-lock made writes hostile (`writer_held` rose
/// to ~307 ms even though the WAL stayed small). 160 MiB sits between the brackets
/// — TRUNCATEs ~3× rarer than at 48 MiB while the file stays modest for readers —
/// and the hot COMMIT path still never checkpoints (the A2 invariant).
const DEFAULT_WAL_TRUNCATE_THRESHOLD_BYTES: u64 = 160 * 1024 * 1024;
// cycle-10 CORRECTION to the 48 MiB rationale above: benchmarked 48 MiB showed the
// large-WAL acquisition tax was NOT the held-time driver (WAL max 369->77 MB yet
// writer_held ROSE 219->307 ms) — the frequent TRUNCATEs each briefly block
// writers, so a tiny cap is read-friendly but write-hostile (QPH 4,261->6,445
// while CDC lag regressed). 160 MiB sits between the measured brackets:
// TRUNCATEs ~3x rarer than 48 MiB, WAL stays modest for readers.

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
    /// `wal_autocheckpoint` threshold in pages. `0` DISABLES `SQLite`'s inline
    /// auto-checkpoint entirely (per the `SQLite` docs, a threshold of 0 turns
    /// auto-checkpointing off).
    ///
    /// # WAL-drain contract (cycle-8 TASK A2)
    ///
    /// The default is `0` — the inline auto-checkpoint is OFF, so a checkpoint
    /// (and its blocking main-DB fsync) can NEVER fire from inside a hot CDC
    /// COMMIT's WAL-write-locked window. This eliminates the invisible inline
    /// autocheckpoint tax that dominated `writer_held`: a multi-MB tombstone
    /// payload per txn was tripping the page threshold constantly, folding a
    /// full checkpoint fsync into the hot COMMIT with no Rust call site for our
    /// metrics to see.
    ///
    /// With the inline checkpoint off, the WAL is drained EXCLUSIVELY off the hot
    /// path by [`SqliteMetastore::checkpoint_wal`], which runs on a DEDICATED
    /// connection (never a pool writer slot) on the background maintenance tick
    /// (`MetadataCatalog::checkpoint_wal`, debounced ~100 ms). That checkpoint is
    /// PASSIVE by default (never blocks writers, never waits for readers) and
    /// ESCALATES to TRUNCATE only when the sampled WAL size exceeds
    /// [`Self::wal_truncate_threshold_bytes`] — a TRUNCATE briefly blocks writers,
    /// so it is gated behind that size cap and runs ONLY on the maintenance tick, never
    /// on the hot write path. PASSIVE alone never truncates the WAL file under a
    /// continuous writer (it cannot reclaim frames past the reader/writer mark),
    /// so without the size-triggered TRUNCATE the `-wal` file would plateau at its
    /// high-water mark; the TRUNCATE escalation reclaims it.
    ///
    /// Why the WAL cannot grow unbounded with the inline checkpoint off: the
    /// dedicated-connection checkpoint copies committed frames into the main DB
    /// every maintenance tick (which fires whenever a write schedules
    /// maintenance — i.e. continuously under CDC load), and the size-triggered
    /// TRUNCATE caps the file. A non-zero value here may be set via
    /// `cayenne_metastore_wal_autocheckpoint_pages` to RE-ENABLE the inline
    /// backstop (e.g. if the maintenance tick is disabled), but that re-introduces
    /// the inline-COMMIT fsync tax this default exists to remove.
    pub wal_autocheckpoint_pages: u32,
    /// WAL-size cap in bytes above which the background maintenance-tick
    /// checkpoint escalates from PASSIVE to TRUNCATE (cycle-8 TASK A2).
    ///
    /// With the inline auto-checkpoint disabled (`wal_autocheckpoint_pages = 0`)
    /// a PASSIVE checkpoint copies committed frames into the main DB but, under a
    /// continuous writer, never truncates the `-wal` file — it plateaus at its
    /// high-water mark. A TRUNCATE reclaims the file but briefly takes the WAL
    /// write lock, so it is gated behind this cap and runs ONLY on the background
    /// tick, NEVER on the hot write path. Defaults to
    /// [`DEFAULT_WAL_TRUNCATE_THRESHOLD_BYTES`] (160 MiB — bracketed by
    /// measurement; see that const's rationale): TRUNCATEs are infrequent enough
    /// not to tax writers, yet the file stays bounded if a tick lags. `0` makes
    /// EVERY background checkpoint a TRUNCATE (used by tests for determinism).
    pub wal_truncate_threshold_bytes: u64,
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
            // cycle-8 TASK A2: DISABLE the inline auto-checkpoint (0 = off). The
            // arc: cycle-5 raised it to 100_000 (~400 MB) to push the inline
            // checkpoint off the hot path, cycle-6 lowered it to 32_000 (~128 MB)
            // to bound a measured wal-index-walk tax; cycle-8 MEASURED that even
            // at 32_000 a multi-MB tombstone payload per txn trips the threshold
            // constantly, so a checkpoint fsync still landed INSIDE the hot
            // COMMIT (the dominant, metrics-invisible component of writer_held).
            // 0 removes that tax entirely: the WAL is drained exclusively by the
            // dedicated-connection background checkpoint (PASSIVE, escalating to
            // TRUNCATE past `wal_truncate_threshold_bytes`) on the maintenance
            // tick. See the field doc for the full drain contract.
            wal_autocheckpoint_pages: 0,
            wal_truncate_threshold_bytes: DEFAULT_WAL_TRUNCATE_THRESHOLD_BYTES,
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
    let cache_size_kib = i64::try_from(cfg.cache_size_mb.saturating_mul(1024)).unwrap_or(262_144);
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
    /// Connection used ONLY by [`SqliteMetastore::checkpoint_wal`] (cycle-8 TASK
    /// A2). The background maintenance-tick checkpoint runs here so it never
    /// contends a `conns` slot — a writer that finds every `conns` slot busy
    /// falls back to `lock_owned()` on `conns[0]`, so reusing `conns[0]` for the
    /// checkpoint could (rarely) serialize a hot writer behind it. A dedicated
    /// connection guarantees the off-hot-path drain stays off the hot path even
    /// under full pool saturation. It still targets the same shared `-wal` file,
    /// so a single checkpoint here covers the catalog's tables.
    checkpoint_conn: Arc<Mutex<tokio_rusqlite::Connection>>,
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
                // cycle-8 TASK A2: dedicated checkpoint connection (see the field
                // doc). One extra connection per metastore DB, used only by the
                // background WAL drain so it never lands on a `conns` slot a hot
                // writer could fall back to.
                let checkpoint_conn = Arc::new(Mutex::new(self.open_connection().await?));
                Ok(Arc::new(SqliteConnectionPool {
                    conns,
                    next: AtomicUsize::new(0),
                    checkpoint_conn,
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
    /// The table is keyed directly on `(table_id, pk_bytes)` as a
    /// `WITHOUT ROWID` composite primary key. The only access paths are
    /// `WHERE table_id = ?` (a leading-prefix scan, e.g.
    /// `get_insert_records` / `clear_insert_records`) and the
    /// `INSERT OR REPLACE` upsert keyed on `(table_id, pk_bytes)`; both are
    /// served by the composite PK. The previous `insert_record_id` UUID
    /// `TEXT PRIMARY KEY` was never read, filtered, or joined — it added a
    /// second B-tree and a 36-byte text alloc per row for no benefit, so it
    /// is dropped (see `init_schema` for the legacy-schema migration).
    const INSERT_RECORD_TABLE_DDL: &'static str = r"
        CREATE TABLE IF NOT EXISTS cayenne_insert_record (
            table_id TEXT NOT NULL,
            pk_bytes BLOB NOT NULL,
            sequence_number BIGINT NOT NULL,
            FOREIGN KEY (table_id) REFERENCES cayenne_table(table_id) ON DELETE CASCADE,
            PRIMARY KEY (table_id, pk_bytes)
        ) WITHOUT ROWID
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
    const INLINED_DELETE_UNPUBLISHED_INDEX_DDL: &'static str = "CREATE INDEX IF NOT EXISTS idx_cayenne_inlined_delete_unpublished ON cayenne_inlined_delete(table_id) WHERE published = 0";
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

                // Per-tombstone activation flag for `cayenne_inlined_delete`. The
                // ALTER sets every existing row to the column default (0). Rows
                // that predate this flag were ALWAYS active under the old
                // semantics (no `published` gate), so when the ALTER actually
                // adds the column (Ok), backfill those legacy rows to 1 — leaving
                // them at 0 would make them inert and resurrect the old inline
                // copies they hide. On a fresh DB the column already exists in the
                // CREATE TABLE above, the ALTER errors (Err), and the backfill is
                // skipped (the table is empty anyway). On every later startup the
                // ALTER errors too, so the backfill never re-activates a
                // legitimately in-flight `published = 0` tombstone.
                if conn
                    .execute(
                        "ALTER TABLE cayenne_inlined_delete ADD COLUMN published INTEGER NOT NULL DEFAULT 0",
                        [],
                    )
                    .is_ok()
                {
                    conn.execute("UPDATE cayenne_inlined_delete SET published = 1", [])?;
                }

                // Migrate a legacy `cayenne_insert_record` (UUID `insert_record_id`
                // TEXT PRIMARY KEY + redundant `UNIQUE(table_id, pk_bytes)`) to the
                // `WITHOUT ROWID` composite-PK schema. The `CREATE TABLE IF NOT
                // EXISTS` above leaves a pre-existing table untouched, so detect the
                // old layout here and copy its rows forward into the new shape.
                //
                // The table is ephemeral (fully cleared at every checkpoint via
                // commit_compaction / commit_overwrite, and recoverable from the
                // snapshot), so dropping it would be safe — but we copy the
                // (table_id, pk_bytes, sequence_number) rows forward anyway to
                // preserve any in-flight pre-checkpoint re-insert sequences across
                // the upgrade at trivial cost. Runs inside the schema-init
                // transaction so the swap is atomic.
                let has_legacy_uuid_column = conn
                    .prepare("PRAGMA table_info('cayenne_insert_record')")?
                    .query_map([], |row| row.get::<_, String>(1))?
                    .collect::<Result<Vec<String>, _>>()?
                    .iter()
                    .any(|name| name == "insert_record_id");
                if has_legacy_uuid_column {
                    conn.execute_batch(
                        "CREATE TABLE cayenne_insert_record_new (
                            table_id TEXT NOT NULL,
                            pk_bytes BLOB NOT NULL,
                            sequence_number BIGINT NOT NULL,
                            FOREIGN KEY (table_id) REFERENCES cayenne_table(table_id) ON DELETE CASCADE,
                            PRIMARY KEY (table_id, pk_bytes)
                        ) WITHOUT ROWID;
                        INSERT OR REPLACE INTO cayenne_insert_record_new (table_id, pk_bytes, sequence_number)
                            SELECT table_id, pk_bytes, sequence_number FROM cayenne_insert_record;
                        DROP TABLE cayenne_insert_record;
                        ALTER TABLE cayenne_insert_record_new RENAME TO cayenne_insert_record;",
                    )?;
                }

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
                conn.execute(Self::INLINED_DELETE_UNPUBLISHED_INDEX_DDL, [])?;
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
        // METRIC 1: a bare autocommit write statement. Wait = pool-slot acquire
        // (the WAL writer lock is taken implicitly by the statement itself); held
        // = the statement's run. Labeled `txn="other"` — this generic path cannot
        // cheaply know the originating catalog stage.
        let wait_start = std::time::Instant::now();
        let guard = self.pool().await?.acquire().await;
        telemetry::track_cayenne_metastore_writer_wait(
            wait_start.elapsed(),
            &[telemetry::KeyValue::new("txn", "other")],
        );
        let sql = params.sql.to_string();
        let param_values: Vec<rusqlite::types::Value> =
            params.params.into_iter().map(to_sqlite_value).collect();

        let held_start = std::time::Instant::now();
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
        telemetry::track_cayenne_metastore_writer_held(
            held_start.elapsed(),
            &[telemetry::KeyValue::new("txn", "other")],
        );

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
        // METRIC 1 (writer wait): wall-clock from the start of acquisition through
        // a held BEGIN IMMEDIATE. This is the WAL-serialized writer queueing cost —
        // the pool-slot lock plus SQLite's reserved-lock acquire (the busy-timeout
        // wait when another writer holds the lock). No `txn` stage label here: the
        // generic backend `begin_transaction` cannot cheaply know which catalog
        // stage opened it without threading a parameter through every call site,
        // so it records `"other"`.
        let wait_start = std::time::Instant::now();
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
        telemetry::track_cayenne_metastore_writer_wait(
            wait_start.elapsed(),
            &[telemetry::KeyValue::new("txn", "other")],
        );

        // METRIC 1 (writer held): the reserved write lock is held from this BEGIN
        // until commit/rollback/drop. Stamp the start so `SqliteTransaction` can
        // record the hold duration when it releases the guard.
        Ok(Box::new(SqliteTransaction {
            conn: Some(guard),
            held_start: std::time::Instant::now(),
        }))
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

    async fn checkpoint_wal(&self) -> CatalogResult<()> {
        // cycle-8 TASK A2: the SOLE WAL drain. With the inline auto-checkpoint
        // disabled (`wal_autocheckpoint_pages = 0`) no checkpoint ever fires from
        // a hot CDC COMMIT; this background-tick checkpoint is now the only thing
        // that copies committed frames into the main DB. It runs on a DEDICATED
        // connection (never a `conns` writer slot — see the field doc) so it can
        // never serialize a hot writer.
        //
        // Mode: PASSIVE by default (never blocks writers, never waits for
        // readers; a busy WAL just leaves frames for the next tick). A PASSIVE
        // checkpoint under a continuous writer copies frames but never TRUNCATEs
        // the `-wal` file, so the file plateaus at its high-water mark. We
        // ESCALATE to TRUNCATE only when the sampled size exceeds the configured
        // `wal_truncate_threshold_bytes` — TRUNCATE briefly takes the WAL write
        // lock, which is acceptable on this off-hot-path background tick (and
        // bounds the file) but would be unacceptable on the hot path, which is
        // exactly why the inline auto-checkpoint is off.
        let Some(pool) = self.pool.get() else {
            return Ok(());
        };
        let conn = &pool.checkpoint_conn;
        let guard = conn.lock().await;

        // Sample the -wal size BEFORE the checkpoint to pick the mode (cheap
        // stat()). Past the cap we TRUNCATE to reclaim the file; otherwise
        // PASSIVE keeps writers unblocked. The pre-checkpoint sample is the size
        // the truncate decision must be based on (the post-checkpoint sample
        // below is the resulting drained size for the gauge).
        let wal_bytes_before = self.read_wal_bytes().await;
        let truncate = wal_bytes_before > sqlite_metastore_config().wal_truncate_threshold_bytes;
        let mode_label = if truncate {
            "truncate_background"
        } else {
            "passive_background"
        };

        // METRIC 2 (checkpoint duration): time the checkpoint with the chosen
        // background mode (this IS the off-hot-path background drain).
        let checkpoint_start = std::time::Instant::now();
        guard
            .call(move |conn| {
                let journal_mode: String =
                    conn.query_row("PRAGMA journal_mode", [], |row| row.get(0))?;
                if journal_mode.eq_ignore_ascii_case("wal") {
                    // wal_checkpoint returns (busy, log, checkpointed). TRUNCATE
                    // reclaims the file once frames are copied; PASSIVE leaves the
                    // file in place for reuse. A TRUNCATE that finds the WAL busy
                    // returns busy=1 and does partial work — never an error, so
                    // the next tick retries (the cap re-trips).
                    let pragma = if truncate {
                        "PRAGMA wal_checkpoint(TRUNCATE)"
                    } else {
                        "PRAGMA wal_checkpoint(PASSIVE)"
                    };
                    let _: (i32, i32, i32) = conn.query_row(pragma, [], |row| {
                        Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                    })?;
                }
                Ok::<_, rusqlite::Error>(())
            })
            .await
            .map_err(
                |e: tokio_rusqlite::Error<rusqlite::Error>| CatalogError::Database {
                    message: format!("Failed to checkpoint catalog WAL: {e}"),
                },
            )?;
        telemetry::track_cayenne_metastore_checkpoint(
            checkpoint_start.elapsed(),
            &[telemetry::KeyValue::new("mode", mode_label)],
        );
        // METRIC 2 (WAL bytes): sample the -wal file size right after the
        // checkpoint copied as many frames as it could. A cheap stat(); a missing
        // file (just truncated) reports 0.
        self.sample_wal_bytes().await;
        Ok(())
    }
}

impl SqliteMetastore {
    /// Read the current `-wal` file size in bytes (cheap `stat()`), without
    /// recording it. Best-effort: a missing or unreadable file reports 0 (the WAL
    /// was truncated or not yet created).
    ///
    /// `tokio::fs::metadata` (not `std::fs`): this runs on the async maintenance
    /// tick, so a blocking stat would stall a Tokio worker thread (PR #11206
    /// review).
    async fn read_wal_bytes(&self) -> u64 {
        let wal_path = format!("{}-wal", self.db_path());
        tokio::fs::metadata(&wal_path).await.map_or(0, |m| m.len())
    }

    /// Sample the current `-wal` file size and publish it to the METRIC 2
    /// `cayenne_metastore_wal_bytes` gauge.
    async fn sample_wal_bytes(&self) {
        let bytes = self.read_wal_bytes().await;
        telemetry::track_cayenne_metastore_wal_bytes(bytes, &[]);
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
    /// When the reserved write lock was acquired (BEGIN IMMEDIATE returned), used
    /// to record METRIC 1 `cayenne_metastore_writer_held_ms` on
    /// commit/rollback/drop.
    held_start: std::time::Instant,
}

impl Drop for SqliteTransaction {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            // A drop without an explicit commit/rollback still held the write
            // lock for this long (it rolls back below).
            telemetry::track_cayenne_metastore_writer_held(
                self.held_start.elapsed(),
                &[telemetry::KeyValue::new("txn", "other")],
            );
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

        // METRIC 1 (writer held): record AFTER the write lock is actually
        // released. On success that is when COMMIT returns (the BEGIN IMMEDIATE
        // lock is held through COMMIT's fsync, so a contending writer blocks
        // until then). On a failed COMMIT the lock persists until the
        // best-effort ROLLBACK below completes, so that path records after the
        // rollback instead — recording any earlier under-reports the hold
        // window the next writer queues behind (PR #11206 review).
        match commit_result {
            Ok(()) => {
                telemetry::track_cayenne_metastore_writer_held(
                    self.held_start.elapsed(),
                    &[telemetry::KeyValue::new("txn", "other")],
                );
                Ok(())
            }
            Err(e) => {
                // Best-effort rollback to leave the connection in a clean state.
                let _ = conn
                    .call(|conn| {
                        let _ = conn.execute_batch("ROLLBACK");
                        Ok::<_, rusqlite::Error>(())
                    })
                    .await;

                telemetry::track_cayenne_metastore_writer_held(
                    self.held_start.elapsed(),
                    &[telemetry::KeyValue::new("txn", "other")],
                );

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

        let rollback_result = conn
            .call(|conn| {
                conn.execute_batch("ROLLBACK")?;
                Ok::<_, rusqlite::Error>(())
            })
            .await;

        // METRIC 1 (writer held): record AFTER ROLLBACK — the write lock is held
        // through the rollback statement, so include its duration (PR #11206).
        telemetry::track_cayenne_metastore_writer_held(
            self.held_start.elapsed(),
            &[telemetry::KeyValue::new("txn", "other")],
        );

        rollback_result.map_err(|e: tokio_rusqlite::Error<rusqlite::Error>| {
            CatalogError::Database {
                message: format!("Failed to rollback transaction: {e}"),
            }
        })?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metastore::{ExecuteParams, MetastoreValue, QueryParams, QueryRowParams};

    /// `SQLITE_METASTORE_CONFIG` is process-wide and read at connection-open
    /// time (and, for the truncate threshold, at `checkpoint_wal` time). The
    /// cycle-8 TASK A2 tests below mutate it, so they serialize through this lock
    /// and each sets the exact config it needs while holding it — preventing a
    /// parallel test from observing another's override. A `tokio` Mutex (not
    /// `std`) so the guard can be held across the `.await`s in the test body
    /// (the writes are tiny) without the held-guard-across-await lint.
    static CONFIG_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    fn temp_metastore() -> (tempfile::TempDir, SqliteMetastore) {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("cayenne_test.db");
        let metastore = SqliteMetastore::new(format!("sqlite://{}", db_path.display()));
        (dir, metastore)
    }

    /// Create a tiny table and append `n` rows each carrying a ~`blob_kib` KiB
    /// blob, growing the WAL. With `wal_autocheckpoint = 0` (TASK A2 default) the
    /// engine never drains it inline, so the `-wal` file accumulates every frame.
    async fn grow_wal(metastore: &SqliteMetastore, n: usize, blob_kib: usize) {
        metastore
            .execute_batch("CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, payload BLOB)")
            .await
            .expect("create table");
        let blob = vec![0xABu8; blob_kib * 1024];
        for i in 0..i64::try_from(n).unwrap_or(i64::MAX) {
            metastore
                .execute(ExecuteParams {
                    sql: "INSERT INTO t (id, payload) VALUES (?1, ?2)",
                    params: vec![
                        MetastoreValue::Integer(i),
                        MetastoreValue::Blob(blob.clone()),
                    ],
                })
                .await
                .expect("insert row");
        }
    }

    /// TASK A2: a connection opened under the default config has the inline WAL
    /// auto-checkpoint DISABLED (`PRAGMA wal_autocheckpoint` returns 0), so a
    /// checkpoint can never fire inside a hot COMMIT.
    #[tokio::test]
    async fn test_wal_autocheckpoint_disabled_by_default() {
        let _guard = CONFIG_LOCK.lock().await;
        set_sqlite_metastore_config(SqliteMetastoreConfig::default());

        let (_dir, metastore) = temp_metastore();
        metastore.init_schema().await.expect("init schema");

        // Read the live pragma off an actual pooled connection (round-trips
        // through the same open path a writer uses).
        let pool = metastore.pool().await.expect("pool");
        let guard = pool.conns[0].lock().await;
        let autocheckpoint: i64 = guard
            .call(|conn| conn.query_row("PRAGMA wal_autocheckpoint", [], |row| row.get(0)))
            .await
            .expect("read pragma");
        assert_eq!(
            autocheckpoint, 0,
            "inline WAL auto-checkpoint must be disabled (0) by default so no checkpoint fsync lands inside a hot CDC COMMIT"
        );

        // The dedicated checkpoint connection must also have it disabled.
        let cp_guard = pool.checkpoint_conn.lock().await;
        let cp_autocheckpoint: i64 = cp_guard
            .call(|conn| conn.query_row("PRAGMA wal_autocheckpoint", [], |row| row.get(0)))
            .await
            .expect("read pragma on checkpoint conn");
        assert_eq!(cp_autocheckpoint, 0);
    }

    /// TASK A2: with the inline checkpoint off, the background `checkpoint_wal`
    /// is the SOLE drain. Force the TRUNCATE escalation (threshold = 0) and
    /// assert it reclaims a grown `-wal` file — proving the off-hot-path drain
    /// keeps the WAL bounded without the inline backstop.
    #[tokio::test]
    async fn test_background_checkpoint_truncates_grown_wal() {
        let _guard = CONFIG_LOCK.lock().await;
        // threshold = 0 → every background checkpoint escalates to TRUNCATE.
        set_sqlite_metastore_config(SqliteMetastoreConfig {
            wal_truncate_threshold_bytes: 0,
            ..SqliteMetastoreConfig::default()
        });

        let (_dir, metastore) = temp_metastore();
        metastore.init_schema().await.expect("init schema");

        // Grow the WAL: ~64 rows × 64 KiB ≈ 4 MiB of frames that the disabled
        // inline auto-checkpoint never drained.
        grow_wal(&metastore, 64, 64).await;
        let wal_before = metastore.read_wal_bytes().await;
        assert!(
            wal_before > 1024 * 1024,
            "expected the WAL to accumulate (no inline checkpoint); got {wal_before} bytes"
        );

        metastore.checkpoint_wal().await.expect("checkpoint");

        let wal_after = metastore.read_wal_bytes().await;
        assert!(
            wal_after < wal_before,
            "background checkpoint must drain the WAL: before={wal_before} after={wal_after}"
        );
        // TRUNCATE reclaims the file outright in a quiescent DB (no other writer
        // holds frames), so it should collapse to ~0.
        assert!(
            wal_after <= 64 * 1024,
            "TRUNCATE-mode background checkpoint should reclaim the -wal file; after={wal_after} bytes"
        );

        // The data survived the drain (frames were copied into the main DB before
        // truncation) — read it back through a fresh query.
        let count: i64 = metastore
            .query_row(
                QueryRowParams {
                    sql: "SELECT COUNT(*) FROM t",
                    params: vec![],
                },
                |row| row.get_i64(0),
            )
            .await
            .expect("count rows");
        assert_eq!(count, 64, "all rows must be durable after the WAL drain");

        // Restore the default so the non-zero threshold does not leak to other
        // crate tests that open metastores concurrently with the lock released.
        set_sqlite_metastore_config(SqliteMetastoreConfig::default());
    }

    /// TASK A2: the default PASSIVE background checkpoint (WAL well under the
    /// 512 MiB cap) drains the accumulated frames into the main DB without
    /// requiring TRUNCATE. After it runs, an independent TRUNCATE finds nothing
    /// left to copy and reclaims the file — proving PASSIVE fully checkpointed.
    #[tokio::test]
    async fn test_background_passive_checkpoint_drains_into_main_db() {
        let _guard = CONFIG_LOCK.lock().await;
        set_sqlite_metastore_config(SqliteMetastoreConfig::default());

        let (_dir, metastore) = temp_metastore();
        metastore.init_schema().await.expect("init schema");

        grow_wal(&metastore, 32, 64).await;
        let wal_before = metastore.read_wal_bytes().await;
        assert!(wal_before > 0, "WAL should hold frames before the drain");

        // Default threshold (512 MiB) ⇒ PASSIVE (our ~2 MiB WAL is far below it).
        metastore
            .checkpoint_wal()
            .await
            .expect("passive checkpoint");

        // PASSIVE copies frames into the main DB but does not truncate the file;
        // prove the copy happened by checking an independent TRUNCATE on the
        // dedicated conn now reports `log == checkpointed` (everything already in
        // the main DB) and the file collapses to ~0.
        let pool = metastore.pool().await.expect("pool");
        let guard = pool.checkpoint_conn.lock().await;
        let (busy, log, checkpointed): (i64, i64, i64) = guard
            .call(|conn| {
                conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                })
            })
            .await
            .expect("follow-up truncate");
        drop(guard);
        assert_eq!(
            busy, 0,
            "no writer should be blocking the follow-up checkpoint"
        );
        assert_eq!(
            log, checkpointed,
            "the prior PASSIVE checkpoint should have copied every frame into the main DB (log={log}, checkpointed={checkpointed})"
        );

        let wal_after = metastore.read_wal_bytes().await;
        assert!(
            wal_after <= 64 * 1024,
            "WAL should be reclaimed after a full drain; after={wal_after} bytes"
        );
    }

    // ------------------------------------------------------------------
    // cycle-11: `cayenne_insert_record` WITHOUT ROWID composite-PK schema.
    // ------------------------------------------------------------------

    /// Read the ordered column names of a table off a live pooled connection.
    async fn table_columns(metastore: &SqliteMetastore, table: &str) -> Vec<String> {
        let table = table.to_string();
        let pool = metastore.pool().await.expect("pool");
        let guard = pool.conns[0].lock().await;
        guard
            .call(move |conn| {
                let mut stmt = conn.prepare(&format!("PRAGMA table_info('{table}')"))?;
                let cols: Vec<String> = stmt
                    .query_map([], |row| row.get::<_, String>(1))?
                    .collect::<Result<Vec<_>, _>>()?;
                Ok::<Vec<String>, rusqlite::Error>(cols)
            })
            .await
            .expect("table_info")
    }

    /// A fresh `cayenne_insert_record` has exactly the 3 composite-PK columns
    /// (no `insert_record_id`) and is a `WITHOUT ROWID` table.
    #[tokio::test]
    async fn test_insert_record_schema_is_without_rowid_composite_pk() {
        let _guard = CONFIG_LOCK.lock().await;
        set_sqlite_metastore_config(SqliteMetastoreConfig::default());
        let (_dir, metastore) = temp_metastore();
        metastore.init_schema().await.expect("init schema");

        let cols = table_columns(&metastore, "cayenne_insert_record").await;
        assert_eq!(
            cols,
            vec!["table_id", "pk_bytes", "sequence_number"],
            "the never-read insert_record_id UUID column must be gone"
        );

        // WITHOUT ROWID tables have no implicit `rowid`; selecting it errors.
        let pool = metastore.pool().await.expect("pool");
        let g = pool.conns[0].lock().await;
        let has_rowid = g
            .call(|conn| {
                Ok::<bool, rusqlite::Error>(
                    conn.query_row(
                        "SELECT rowid FROM cayenne_insert_record LIMIT 1",
                        [],
                        |_| Ok(()),
                    )
                    .is_ok(),
                )
            })
            .await
            .expect("rowid probe");
        assert!(
            !has_rowid,
            "cayenne_insert_record must be WITHOUT ROWID (no implicit rowid column)"
        );
    }

    /// INSERT OR REPLACE on a duplicate `(table_id, pk_bytes)` updates the
    /// sequence in place and keeps exactly one row (the composite PK is the
    /// conflict target the catalog relies on).
    #[tokio::test]
    async fn test_insert_record_duplicate_pk_upserts_sequence() {
        let _guard = CONFIG_LOCK.lock().await;
        set_sqlite_metastore_config(SqliteMetastoreConfig::default());
        let (_dir, metastore) = temp_metastore();
        metastore.init_schema().await.expect("init schema");

        // Parent row so the FK is satisfied.
        let table_id = uuid::Uuid::now_v7().to_string();
        metastore
            .execute_batch(&format!(
                "INSERT INTO cayenne_table (table_id, table_name, path, path_is_relative, schema_json, primary_key_json, current_snapshot_id) \
                 VALUES ('{table_id}', 'dup_pk_t', '/tmp', 1, '{{}}', '[]', '{table_id}')"
            ))
            .await
            .expect("seed parent");

        for seq in [11_i64, 42] {
            // Second iteration upserts the SAME (table_id, pk_bytes) → REPLACE.
            metastore
                .execute(ExecuteParams {
                    sql: "INSERT OR REPLACE INTO cayenne_insert_record (table_id, pk_bytes, sequence_number) VALUES (?1, ?2, ?3)",
                    params: vec![
                        MetastoreValue::Text(table_id.clone()),
                        MetastoreValue::Blob(b"pk-dup".to_vec()),
                        MetastoreValue::Integer(seq),
                    ],
                })
                .await
                .expect("upsert insert record");
        }

        let (count, seq): (i64, i64) = {
            let table_id = table_id.clone();
            let pool = metastore.pool().await.expect("pool");
            let g = pool.conns[0].lock().await;
            g.call(move |conn| {
                conn.query_row(
                    "SELECT COUNT(*), MAX(sequence_number) FROM cayenne_insert_record WHERE table_id = ?1",
                    [&table_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
            })
            .await
            .expect("read back")
        };
        assert_eq!(
            count, 1,
            "duplicate (table_id, pk_bytes) must collapse to one row"
        );
        assert_eq!(seq, 42, "the later upsert's sequence must win");
    }

    /// `DELETE FROM cayenne_insert_record WHERE table_id = ?` (the checkpoint
    /// clear) empties only the target table's rows — still served by the
    /// leading-prefix of the composite PK.
    #[tokio::test]
    async fn test_insert_record_checkpoint_clear_by_table_id() {
        let _guard = CONFIG_LOCK.lock().await;
        set_sqlite_metastore_config(SqliteMetastoreConfig::default());
        let (_dir, metastore) = temp_metastore();
        metastore.init_schema().await.expect("init schema");

        let table_id = uuid::Uuid::now_v7().to_string();
        metastore
            .execute_batch(&format!(
                "INSERT INTO cayenne_table (table_id, table_name, path, path_is_relative, schema_json, primary_key_json, current_snapshot_id) \
                 VALUES ('{table_id}', 'clear_t', '/tmp', 1, '{{}}', '[]', '{table_id}'); \
                 INSERT INTO cayenne_insert_record (table_id, pk_bytes, sequence_number) VALUES ('{table_id}', x'01', 1); \
                 INSERT INTO cayenne_insert_record (table_id, pk_bytes, sequence_number) VALUES ('{table_id}', x'02', 2);"
            ))
            .await
            .expect("seed rows");

        metastore
            .execute(ExecuteParams {
                sql: "DELETE FROM cayenne_insert_record WHERE table_id = ?1",
                params: vec![MetastoreValue::Text(table_id.clone())],
            })
            .await
            .expect("checkpoint clear");

        let remaining: i64 = metastore
            .query_row(
                QueryRowParams {
                    sql: "SELECT COUNT(*) FROM cayenne_insert_record WHERE table_id = ?1",
                    params: vec![MetastoreValue::Text(table_id)],
                },
                |row| row.get_i64(0),
            )
            .await
            .expect("count after clear");
        assert_eq!(
            remaining, 0,
            "checkpoint clear must empty the table's insert records"
        );
    }

    /// A DB created with the legacy schema (UUID `insert_record_id` TEXT PRIMARY
    /// KEY + redundant `UNIQUE(table_id, pk_bytes)`) and carrying rows is
    /// migrated by `init_schema` to the `WITHOUT ROWID` composite-PK layout,
    /// copying the `(table_id, pk_bytes, sequence_number)` rows forward.
    #[tokio::test]
    async fn test_insert_record_legacy_schema_migrates_with_rows_present() {
        let _guard = CONFIG_LOCK.lock().await;
        set_sqlite_metastore_config(SqliteMetastoreConfig::default());
        let (_dir, metastore) = temp_metastore();

        // Build a realistic "old deployment": init the current full schema for
        // every OTHER table, then DOWNGRADE only cayenne_insert_record back to
        // the legacy UUID-PK shape and seed a parent row + two insert records.
        // Re-running init_schema must then detect & migrate just this table
        // (and leave every other table matching EXPECTED_TABLES).
        metastore.init_schema().await.expect("baseline init schema");
        let table_id = uuid::Uuid::now_v7().to_string();
        metastore
            .execute_batch(&format!(
                "DROP TABLE cayenne_insert_record; \
                 CREATE TABLE cayenne_insert_record (\
                    insert_record_id TEXT PRIMARY KEY, table_id TEXT NOT NULL, \
                    pk_bytes BLOB NOT NULL, sequence_number BIGINT NOT NULL, \
                    FOREIGN KEY (table_id) REFERENCES cayenne_table(table_id) ON DELETE CASCADE, \
                    UNIQUE(table_id, pk_bytes)); \
                 INSERT INTO cayenne_table (table_id, table_name, path, path_is_relative, schema_json, primary_key_json, current_snapshot_id) \
                    VALUES ('{table_id}', 'legacy_t', '/tmp', 1, '{{}}', '[]', '{table_id}'); \
                 INSERT INTO cayenne_insert_record (insert_record_id, table_id, pk_bytes, sequence_number) \
                    VALUES ('{u1}', '{table_id}', x'0a', 7); \
                 INSERT INTO cayenne_insert_record (insert_record_id, table_id, pk_bytes, sequence_number) \
                    VALUES ('{u2}', '{table_id}', x'0b', 9);",
                u1 = uuid::Uuid::now_v7(),
                u2 = uuid::Uuid::now_v7(),
            ))
            .await
            .expect("downgrade cayenne_insert_record to legacy schema + seed rows");

        // Sanity: the legacy column is present pre-migration.
        let before = table_columns(&metastore, "cayenne_insert_record").await;
        assert!(
            before.contains(&"insert_record_id".to_string()),
            "precondition: legacy UUID column present, got {before:?}"
        );

        metastore.init_schema().await.expect("init schema migrates");

        // Post-migration: the new 3-column WITHOUT ROWID schema and both rows
        // (with their original sequences) survived the copy-forward.
        let after = table_columns(&metastore, "cayenne_insert_record").await;
        assert_eq!(
            after,
            vec!["table_id", "pk_bytes", "sequence_number"],
            "legacy table must be migrated to the composite-PK schema"
        );

        let rows: Vec<(Vec<u8>, i64)> = metastore
            .query(
                QueryParams {
                    sql: "SELECT pk_bytes, sequence_number FROM cayenne_insert_record WHERE table_id = ?1 ORDER BY pk_bytes",
                    params: vec![MetastoreValue::Text(table_id)],
                },
                |row| Ok((row.get_blob(0)?, row.get_i64(1)?)),
            )
            .await
            .expect("read migrated rows");
        assert_eq!(
            rows,
            vec![(vec![0x0a_u8], 7_i64), (vec![0x0b_u8], 9_i64)],
            "the (table_id, pk_bytes, sequence_number) rows must be copied forward"
        );

        // The migrated DB also passes the EXPECTED_TABLES validation that
        // init_schema runs at the end (no SchemaMismatch was returned above),
        // and re-running init_schema is idempotent (the column is now gone).
        metastore
            .init_schema()
            .await
            .expect("second init_schema is a no-op");
    }
}
