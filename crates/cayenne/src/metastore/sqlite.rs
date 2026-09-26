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
//! Uses `tokio-rusqlite`. Holds a round-robin pool of persistent connections
//! (`K = min(cpu_budget().cores(), 32)`, floor 2) plus a dedicated checkpoint
//! connection, each managed by a background thread — avoiding the overhead of opening a
//! new connection per operation and lifting read-side concurrency for metadata-heavy scans.

use super::{
    ExecuteParams, MetastoreBackend, MetastoreGetValue, MetastoreRow, MetastoreTransaction,
    MetastoreValue, QueryParams, QueryRowParams, duplicate_delete_file_index_error_message,
};
use crate::catalog::{CatalogError, CatalogResult};
use async_trait::async_trait;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use tokio::sync::{Mutex, OnceCell, OwnedMutexGuard};

/// `PRAGMA auto_vacuum` integer values (0 = NONE, 1 = FULL, 2 = INCREMENTAL).
const SQLITE_AUTO_VACUUM_NONE: i64 = 0;
const SQLITE_AUTO_VACUUM_INCREMENTAL: i64 = 2;

/// Read the database's live `auto_vacuum` mode. Fixed at file creation — the
/// configured mode only applies to a fresh DB — so a single probe is enough for
/// the process lifetime of a given file.
fn read_auto_vacuum_mode(conn: &mut rusqlite::Connection) -> Result<i64, rusqlite::Error> {
    conn.query_row("PRAGMA auto_vacuum", [], |row| row.get(0))
}

/// Checkpoint the WAL with `pragma` when the database is in WAL mode.
/// `wal_checkpoint` returns (busy, log, checkpointed). TRUNCATE reclaims the
/// file once frames are copied; PASSIVE leaves the file in place for reuse. A
/// TRUNCATE that finds the WAL busy returns busy=1 and does partial work —
/// never an error, so the next tick retries (the cap re-trips).
fn checkpoint_wal_file(
    conn: &mut rusqlite::Connection,
    pragma: &str,
) -> Result<(), rusqlite::Error> {
    let journal_mode: String = conn.query_row("PRAGMA journal_mode", [], |row| row.get(0))?;
    if journal_mode.eq_ignore_ascii_case("wal") {
        let _: (i32, i32, i32) = conn.query_row(pragma, [], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })?;
    }
    Ok(())
}

/// How long a background TRUNCATE checkpoint waits for readers still on an
/// older snapshot. A TRUNCATE holds the write lock while it waits, so every
/// write queued behind it on the writer connection waits too. One that gives up
/// has still copied what it could (`busy=1`), and the next maintenance tick
/// tries again.
const TRUNCATE_CHECKPOINT_BUSY_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(100);

/// A background TRUNCATE checkpoint on the writer connection that waits at most
/// [`TRUNCATE_CHECKPOINT_BUSY_TIMEOUT`] for readers, then restores the
/// connection's configured busy timeout.
fn truncate_wal(conn: &mut rusqlite::Connection) -> Result<(), rusqlite::Error> {
    conn.busy_timeout(TRUNCATE_CHECKPOINT_BUSY_TIMEOUT)?;
    let checkpointed = checkpoint_wal_file(conn, "PRAGMA wal_checkpoint(TRUNCATE)");
    conn.busy_timeout(std::time::Duration::from_millis(
        sqlite_metastore_config().busy_timeout_ms,
    ))?;
    checkpointed
}

/// Boundedly reclaim freelist pages on a connection already known to be in
/// INCREMENTAL auto-vacuum mode.
///
/// Returns pages reclaimed (0 when there is not yet a full slice to reclaim).
/// Holds the write lock only for the duration of `PRAGMA incremental_vacuum(N)`,
/// and only when there is actually work to do.
fn reclaim_freelist_pages(
    conn: &mut rusqlite::Connection,
    max_pages: u32,
) -> Result<u64, rusqlite::Error> {
    // Cheap counter read; skip the write lock entirely unless enough pages are
    // waiting to be worth taking it. The caller is the per-table maintenance tick
    // (~100 ms under load, and every table of a catalog shares this one
    // database), so reclaiming whenever any single page is free would take the
    // write lock on essentially every tick and turn each relocated page into a
    // WAL frame the following checkpoint must copy back. A high-update table's
    // freelist churns — pages are freed and immediately reused — so holding out
    // for a batch leaves at most a slice unreclaimed while converging just as
    // fast on the case that matters: the large freelist a bulk delete leaves.
    //
    // Capped by `max_pages` so raising the per-tick cap to drain a big freelist
    // faster cannot raise the bar for starting at all.
    let floor = i64::from(max_pages.min(DEFAULT_INCREMENTAL_VACUUM_PAGES)).max(1);
    let free_before: i64 = conn.query_row("PRAGMA freelist_count", [], |row| row.get(0))?;
    if free_before < floor {
        return Ok(0);
    }
    // Bounded: reclaims at most `max_pages`, leaving the rest for later ticks
    // rather than holding the write lock proportional to the whole freelist.
    conn.execute_batch(&format!("PRAGMA incremental_vacuum({max_pages})"))?;
    let free_after: i64 = conn.query_row("PRAGMA freelist_count", [], |row| row.get(0))?;
    Ok(u64::try_from((free_before - free_after).max(0)).unwrap_or(0))
}

const DELETE_FILE_TABLE_UNIQUE_INDEX_DDL: &str = "CREATE UNIQUE INDEX IF NOT EXISTS idx_cayenne_delete_file_table_path ON cayenne_delete_file(table_id, path)";
const SQLITE_PRAGMA_RETRY_DELAYS_MS: &[u64] = &[10, 25, 50, 100, 200];

/// Rows per `tokio_rusqlite` call in [`SqliteTransaction::execute_many`]: a
/// manifest rewrite pays a round trip per this many files rather than per file,
/// and a cancelled batch runs at most this many further rows under the write
/// lock before the transaction rolls back.
const EXECUTE_MANY_ROWS_PER_CALL: usize = 1_024;

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

/// Freelist pages reclaimed per maintenance tick under INCREMENTAL auto-vacuum.
/// 256 pages is 1 MiB at the 4 KiB default page size — small enough that the
/// write-lock hold stays imperceptible next to the checkpoint that follows it in
/// the same pass, large enough to converge on a growing freelist.
const DEFAULT_INCREMENTAL_VACUUM_PAGES: u32 = 256;
// cycle-10 CORRECTION to the 48 MiB rationale above: benchmarked 48 MiB showed the
// large-WAL acquisition tax was NOT the held-time driver (WAL max 369->77 MB yet
// writer_held ROSE 219->307 ms) — the frequent TRUNCATEs each briefly block
// writers, so a tiny cap is read-friendly but write-hostile (QPH 4,261->6,445
// while CDC lag regressed). 160 MiB sits between the measured brackets:
// TRUNCATEs ~3x rarer than 48 MiB, WAL stays modest for readers.

/// `auto_vacuum` mode for the metastore DB.
///
/// A high-update upsert table (e.g. `district`) frees pages as it supersedes
/// rows. Under `None` those pages stay on the freelist and are reused, so the
/// file plateaus at its high-water mark — but that mark is set by the largest
/// transient the metastore ever held, and an operator reading `du` cannot tell a
/// plateau from a leak. `Incremental` is the default so the space comes back:
/// the mode itself does no vacuuming (it only maintains the pointer map that
/// makes reclamation possible), and the reclaiming
/// `PRAGMA incremental_vacuum(N)` runs bounded, off the hot path, on the
/// maintenance tick. `None` remains available for a deployment that wants the
/// pointer-map overhead off its write path and is content with the plateau.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SqliteAutoVacuum {
    /// `SQLite`'s own default: no pointer map, and so no write-path overhead at
    /// all.
    None,
    /// Freed pages are tracked for reclamation by an explicit
    /// `PRAGMA incremental_vacuum`, which holds the write lock while it relocates
    /// and truncates — so reclamation must be driven off the hot path, never
    /// inside a CDC burst.
    #[default]
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
    /// `auto_vacuum` mode. Takes effect only on a fresh DB: moving an existing
    /// one off NONE takes `PRAGMA auto_vacuum = <mode>` FOLLOWED by a full
    /// `VACUUM` — the pragma alone is a no-op there, and a bare `VACUUM`
    /// preserves whatever mode the file already has. So a metastore created
    /// before this defaulted to [`SqliteAutoVacuum::Incremental`] keeps its old
    /// mode, and the driver gates on the file's real mode rather than on this
    /// setting.
    pub auto_vacuum: SqliteAutoVacuum,
    /// Freelist pages reclaimed per maintenance tick when the database is in
    /// INCREMENTAL auto-vacuum mode. Ignored in every other mode.
    ///
    /// `PRAGMA incremental_vacuum(N)` relocates pages to the end of the file and
    /// truncates, holding the write lock for the duration — so the cap is what
    /// makes it safe to run at all. At the 4 KiB default page size the default
    /// reclaims 1 MiB per tick; with the maintenance debounce at ~100 ms under
    /// load that converges at roughly 10 MiB/s while each individual pause stays
    /// short. Raise it to drain a large freelist faster at the cost of longer
    /// write-lock holds; `0` disables reclamation without changing the DB mode.
    pub incremental_vacuum_pages: u32,
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
            auto_vacuum: SqliteAutoVacuum::Incremental,
            incremental_vacuum_pages: DEFAULT_INCREMENTAL_VACUUM_PAGES,
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

/// Returns true if the connection string targets an in-memory `SQLite` database
/// (Cayenne memory mode). Memory mode always uses the memdb VFS
/// (`file:<name>?vfs=memdb`) — the only form under which multiple pooled
/// connections share ONE in-memory database. (`:memory:` / `mode=memory` would
/// instead give each pooled connection its own private database.)
///
/// In-memory databases have no backing file, so the pool skips parent-directory
/// creation and uses the `MEMORY` rollback journal (WAL is unsupported).
///
/// `pub(crate)` so [`crate::cayenne_catalog::CayenneCatalog::init`] applies the
/// SAME guard as [`SqliteMetastore::open_connection`] — the two must agree on
/// what counts as in-memory, or one path creates a stray directory the other
/// skips (the memory-mode `file:` directory bug, #11922).
pub(crate) fn is_memory_db_path(db_path: &str) -> bool {
    db_path.contains("vfs=memdb")
}

async fn configure_sqlite_connection(
    conn: &tokio_rusqlite::Connection,
    in_memory: bool,
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
                if in_memory {
                    // In-memory databases don't support WAL — use the MEMORY
                    // rollback journal and set synchronous OFF (no backing file to
                    // fsync). mmap_size / wal_autocheckpoint are meaningless without
                    // a file and are skipped below.
                    conn.pragma_update(None, "journal_mode", "MEMORY")?;
                    conn.pragma_update(None, "synchronous", "OFF")?;
                } else {
                    conn.pragma_update(None, "journal_mode", "WAL")?;
                    conn.pragma_update(None, "synchronous", "NORMAL")?;
                }
                conn.pragma_update(None, "cache_size", -cache_size_kib)?;
                conn.pragma_update(None, "foreign_keys", true)?;
                conn.pragma_update(None, "temp_store", "memory")?;
                if !in_memory {
                    conn.pragma_update(None, "mmap_size", cfg.mmap_size_bytes)?;
                    conn.pragma_update(None, "wal_autocheckpoint", cfg.wal_autocheckpoint_pages)?;
                }

                Ok::<_, rusqlite::Error>(())
            })
            .await;

        match result {
            Ok(()) => return Ok(()),
            Err(error) if is_sqlite_lock_error(&error) => {
                let Some(delay_ms) = retry_delays.next() else {
                    return Err(error);
                };
                // Equal jitter (shared with the write-conflict retry) so simultaneous
                // connection-setup retriers don't all wake on the same boundary.
                let jittered =
                    turso_shared::apply_equal_jitter(std::time::Duration::from_millis(*delay_ms));
                tokio::time::sleep(jittered).await;
            }
            Err(error) => return Err(error),
        }
    }
}

/// The connection every in-process write to one metastore file runs on, one at
/// a time, in the order the writes arrive.
///
/// `SQLite` admits one writer at a time and parks the rest in its busy handler,
/// which retries on a sleep backoff of up to 100 ms and grants no order: a
/// writer that re-takes the lock back to back can starve the others until they
/// fail with `database is locked`, and under a few dozen writers every write's
/// tail is the backoff rather than the work. A writer parked there also holds a
/// pooled connection while it sleeps, so enough of them leave none for a read.
///
/// Instead every write, whether an autocommit statement or a transaction from
/// `BEGIN IMMEDIATE` through `COMMIT` or `ROLLBACK`, is queued on this one
/// connection, and its thread runs them in arrival order. Consecutive writes run
/// back to back, with no handoff between their callers; the write lock is free
/// the moment a write commits; and a queued write holds no pooled connection. A
/// transaction is a [`Session`], which occupies the thread from its `BEGIN` to
/// its end while the writes queued behind it wait. `SQLite`'s busy handler only
/// arbitrates between processes.
///
/// Reads never queue here: in WAL mode they neither wait for nor block a writer.
struct Writer {
    conn: tokio_rusqlite::Connection,
}

/// Writer connections by metastore file, shared by every [`SqliteMetastore`]
/// open on that file in this process. Each file has its own slot, held while
/// its writer connection opens, so a file whose open waits out another
/// process's lock holds up only metastores on that same file. Weak, so a
/// file's writer connection closes once no metastore is open on it and no
/// write or session queued there is left to run.
type WriterSlot = Arc<Mutex<std::sync::Weak<Writer>>>;
static WRITERS: std::sync::LazyLock<
    parking_lot::Mutex<std::collections::HashMap<WriterKey, WriterSlot>>,
> = std::sync::LazyLock::new(|| parking_lot::Mutex::new(std::collections::HashMap::new()));

/// The database a writer connection is for.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum WriterKey {
    /// A memory-mode URI, which already names its database.
    Memory(String),
    /// A file, by its canonical path, kept as a path: a path need not be UTF-8,
    /// and converting it to a string loses the bytes that tell two files apart.
    File(PathBuf),
}

/// Names a metastore file so every path to it shares a writer connection: its
/// canonical path, with every symlink resolved, the file's own included, so a
/// symlink to a metastore file shares its target's writer. A file that does
/// not exist yet has no path to resolve, and is named by its canonical parent
/// directory joined with its file name.
async fn writer_key(db_path: &str) -> WriterKey {
    if is_memory_db_path(db_path) {
        return WriterKey::Memory(db_path.to_string());
    }
    if let Ok(file) = tokio::fs::canonicalize(db_path).await {
        return WriterKey::File(file);
    }
    let path = Path::new(db_path);
    let (Some(parent), Some(name)) = (path.parent(), path.file_name()) else {
        return WriterKey::File(path.to_path_buf());
    };
    let parent = if parent.as_os_str().is_empty() {
        Path::new(".")
    } else {
        parent
    };
    WriterKey::File(
        tokio::fs::canonicalize(parent)
            .await
            .map_or_else(|_| path.to_path_buf(), |dir| dir.join(name)),
    )
}

/// What a write gets when its turn does not come within the busy timeout: the
/// `SQLITE_BUSY` a contended write gets from `SQLite` itself, in the form a
/// connection call returns it. Each write path maps it with the same closure
/// as its own call's error, so a write that waits out its turn fails with
/// exactly the error, text and retryable conflict (`is_retryable_write_conflict`)
/// alike, that it gets when `SQLite`'s busy handler times out.
fn writer_timeout() -> tokio_rusqlite::Error<rusqlite::Error> {
    tokio_rusqlite::Error::Error(rusqlite::Error::SqliteFailure(
        rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_BUSY),
        Some("database is locked".to_string()),
    ))
}

/// A queued write's claim on its turn on the [`Writer`]. The writer's thread
/// claims it when the write's turn comes; a caller that stops waiting first,
/// because its wait ran out or its future was dropped, withdraws it, and a
/// withdrawn write never runs. So a caller that got `database is locked` can
/// retry without the write having happened behind its back.
struct Turn(Arc<AtomicU8>);

impl Turn {
    const WAITING: u8 = 0;
    const CLAIMED: u8 = 1;
    const WITHDRAWN: u8 = 2;

    fn new() -> Self {
        Self(Arc::new(AtomicU8::new(Self::WAITING)))
    }

    /// The handle the writer's thread claims the turn through.
    fn claimant(&self) -> Arc<AtomicU8> {
        Arc::clone(&self.0)
    }

    /// On the writer's thread, when the write's turn comes: whether it runs.
    fn claim(state: &AtomicU8) -> bool {
        state
            .compare_exchange(
                Self::WAITING,
                Self::CLAIMED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    /// By the caller: whether the write was withdrawn before its turn came. A
    /// claimed write runs to its end, so its caller has to wait for it.
    fn withdraw(&self) -> bool {
        self.0
            .compare_exchange(
                Self::WAITING,
                Self::WITHDRAWN,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }
}

impl Drop for Turn {
    fn drop(&mut self) {
        // A caller that goes away before its write's turn withdraws it; once
        // the write has been claimed this does nothing.
        self.withdraw();
    }
}

/// A statement a [`Session`] runs on the writer's thread, and whether the
/// session ends with it.
type SessionJob = Box<dyn FnOnce(&mut rusqlite::Connection) -> SessionStep + Send>;

enum SessionStep {
    Continue,
    End,
    /// The session ends with its `abort`: the caller of its last statement
    /// went away before the statement ran.
    Abort,
}

/// Every write on the writer connection ends its own transaction, so the next
/// one should never start inside one. Should one be left open, by a `ROLLBACK`
/// that failed, roll it back before the write runs; if that fails too, the
/// write fails with its error rather than run inside a transaction nothing
/// will commit.
fn end_leftover_transaction(conn: &mut rusqlite::Connection) -> Result<(), rusqlite::Error> {
    if conn.is_autocommit() {
        return Ok(());
    }
    conn.execute_batch("ROLLBACK")
}

/// A transaction's start on the writer connection. `IMMEDIATE` takes the write
/// lock up front, so a transaction contending with another process waits in
/// `SQLite`'s busy handler instead of failing later, while upgrading a deferred
/// transaction after its reads have run.
fn begin_immediate(conn: &mut rusqlite::Connection) -> Result<(), rusqlite::Error> {
    conn.execute_batch("BEGIN IMMEDIATE")
}

/// A transaction's end when it is dropped without a commit or rollback, or
/// when its commit's caller went away before the `COMMIT` ran: roll it back,
/// then record how long it held the write lock. That is known only here, on
/// the writer's thread, once any statement still running and the rollback
/// are done.
fn roll_back(conn: &mut rusqlite::Connection, began: std::time::Instant) {
    let _ = conn.execute_batch("ROLLBACK");
    telemetry::cayenne::track_metastore_writer_held(
        began.elapsed(),
        &[telemetry::KeyValue::new("txn", "other")],
    );
}

impl Writer {
    /// How long a write waits for its turn: the busy timeout a writer already
    /// had waiting on `SQLite`.
    fn busy_timeout() -> std::time::Duration {
        std::time::Duration::from_millis(sqlite_metastore_config().busy_timeout_ms)
    }

    /// Run `job` once every write queued before it has run. `Ok(None)` means
    /// its turn did not come within the busy timeout, and it never runs.
    async fn try_run<F, R>(
        self: &Arc<Self>,
        job: F,
    ) -> Result<Option<R>, tokio_rusqlite::Error<rusqlite::Error>>
    where
        F: FnOnce(&mut rusqlite::Connection) -> Result<R, rusqlite::Error> + Send + 'static,
        R: Send + 'static,
    {
        let turn = Turn::new();
        let claimant = turn.claimant();
        // A started write runs to its end even when its caller stops waiting,
        // so the write holds the writer until then: the file's writer
        // connection stays registered, and a metastore opened on the file
        // meanwhile queues behind the write instead of opening a second one.
        let writer = Arc::clone(self);
        let call = self.conn.call(move |conn| {
            let ran = if Turn::claim(&claimant) {
                end_leftover_transaction(conn)
                    .and_then(|()| job(conn))
                    .map(Some)
            } else {
                Ok(None)
            };
            drop(writer);
            ran
        });
        tokio::pin!(call);
        match tokio::time::timeout(Self::busy_timeout(), &mut call).await {
            Ok(result) => result,
            Err(_) if turn.withdraw() => Ok(None),
            // Its turn came as the wait ran out: it is running, so wait for it.
            Err(_) => call.await,
        }
    }

    /// [`Self::try_run`], failing with [`writer_timeout`] when the write's
    /// turn does not come within the busy timeout.
    async fn run<F, R>(
        self: &Arc<Self>,
        job: F,
    ) -> Result<R, tokio_rusqlite::Error<rusqlite::Error>>
    where
        F: FnOnce(&mut rusqlite::Connection) -> Result<R, rusqlite::Error> + Send + 'static,
        R: Send + 'static,
    {
        self.try_run(job).await?.ok_or_else(writer_timeout)
    }

    /// Take the writer connection for statements its caller issues one at a
    /// time, such as a transaction or schema setup, in its turn like any write
    /// and waiting at most the busy timeout for it. `start` runs first (a
    /// transaction's `BEGIN IMMEDIATE`). The session then runs each statement
    /// sent to it, in order, until [`Session::finish`] or until it is dropped,
    /// when `abort` runs (a transaction's `ROLLBACK`) with the instant `start`
    /// returned.
    async fn session(
        self: &Arc<Self>,
        start: fn(&mut rusqlite::Connection) -> Result<(), rusqlite::Error>,
        abort: fn(&mut rusqlite::Connection, std::time::Instant),
    ) -> Result<Session, tokio_rusqlite::Error<rusqlite::Error>> {
        let turn = Turn::new();
        let claimant = turn.claimant();
        let (started_tx, mut started) =
            tokio::sync::oneshot::channel::<Result<(), rusqlite::Error>>();
        let (jobs, next_job) = std::sync::mpsc::channel::<SessionJob>();
        let conn = self.conn.clone();
        // The session runs on the writer's thread until it ends, however long
        // its caller takes; this task only queues it there. It holds the writer
        // throughout, `abort` included, so the file's writer connection stays
        // registered past the metastore that began the session and past a
        // caller that drops it, and a metastore opened on the file meanwhile
        // queues its writes on this connection instead of opening a second one.
        let writer = Arc::clone(self);
        tokio::spawn(async move {
            let _ = conn
                .call_raw(move |conn| {
                    run_session(conn, &claimant, start, abort, started_tx, &next_job);
                    drop(writer);
                })
                .await;
        });
        let started = match tokio::time::timeout(Self::busy_timeout(), &mut started).await {
            Ok(started) => started,
            Err(_) if turn.withdraw() => return Err(writer_timeout()),
            // Its turn came as the wait ran out: it is starting, so wait for it.
            Err(_) => started.await,
        };
        match started {
            Ok(Ok(())) => Ok(Session { jobs }),
            Ok(Err(e)) => Err(tokio_rusqlite::Error::Error(e)),
            Err(_) => Err(tokio_rusqlite::Error::ConnectionClosed),
        }
    }
}

/// A [`Writer::session`] on the writer's thread once its turn comes: `start`,
/// then each statement its caller sends until one ends the session, and
/// `abort` when it ends any other way.
fn run_session(
    conn: &mut rusqlite::Connection,
    claimant: &AtomicU8,
    start: fn(&mut rusqlite::Connection) -> Result<(), rusqlite::Error>,
    abort: fn(&mut rusqlite::Connection, std::time::Instant),
    started_tx: tokio::sync::oneshot::Sender<Result<(), rusqlite::Error>>,
    next_job: &std::sync::mpsc::Receiver<SessionJob>,
) {
    if !Turn::claim(claimant) {
        return;
    }
    if let Err(e) = end_leftover_transaction(conn).and_then(|()| start(conn)) {
        let _ = started_tx.send(Err(e));
        return;
    }
    let began = std::time::Instant::now();
    if started_tx.send(Ok(())).is_err() {
        // Its caller went away as its turn came.
        abort(conn, began);
        return;
    }
    while let Ok(job) = next_job.recv() {
        match job(conn) {
            SessionStep::Continue => {}
            SessionStep::End => return,
            SessionStep::Abort => break,
        }
    }
    // Dropped without being finished, or finished by a caller that went away
    // before its last statement ran.
    abort(conn, began);
}

/// A caller's hold on the [`Writer`] for statements it issues one at a time.
/// Dropping it ends the session with the `abort` it was started with.
struct Session {
    jobs: std::sync::mpsc::Sender<SessionJob>,
}

impl Session {
    /// Run `function` in the session, as [`tokio_rusqlite::Connection::call`]
    /// runs it on a connection.
    async fn call<F, R, E>(&self, function: F) -> Result<R, tokio_rusqlite::Error<E>>
    where
        F: FnOnce(&mut rusqlite::Connection) -> Result<R, E> + Send + 'static,
        R: Send + 'static,
        E: Send + 'static,
    {
        self.send(function, SessionStep::Continue).await
    }

    /// Run `function`, a `COMMIT` or `ROLLBACK`, as the session's last
    /// statement, and end the session.
    async fn finish<F, R, E>(self, function: F) -> Result<R, tokio_rusqlite::Error<E>>
    where
        F: FnOnce(&mut rusqlite::Connection) -> Result<R, E> + Send + 'static,
        R: Send + 'static,
        E: Send + 'static,
    {
        self.send(function, SessionStep::End).await
    }

    async fn send<F, R, E>(
        &self,
        function: F,
        step: SessionStep,
    ) -> Result<R, tokio_rusqlite::Error<E>>
    where
        F: FnOnce(&mut rusqlite::Connection) -> Result<R, E> + Send + 'static,
        R: Send + 'static,
        E: Send + 'static,
    {
        let (result_tx, result) = tokio::sync::oneshot::channel();
        self.jobs
            .send(Box::new(move |conn: &mut rusqlite::Connection| {
                // A last statement, a transaction's COMMIT, whose caller has
                // gone away does not run: the session ends with its abort (a
                // ROLLBACK) instead, so a transaction whose commit was cancelled
                // before it began never commits behind its caller's back. Other
                // statements run regardless, since skipping one in a
                // transaction that goes on could commit part of it.
                if matches!(step, SessionStep::End) && result_tx.is_closed() {
                    return SessionStep::Abort;
                }
                let _ = result_tx.send(function(conn));
                step
            }))
            .map_err(|_| tokio_rusqlite::Error::ConnectionClosed)?;
        result
            .await
            .map_err(|_| tokio_rusqlite::Error::ConnectionClosed)?
            .map_err(tokio_rusqlite::Error::Error)
    }
}

/// Whether a statement issued through a query method is an `INSERT`, `UPDATE`,
/// `DELETE` or `REPLACE`, with or without a `WITH` clause before it, which
/// sends it straight to the [`Writer`] to wait its turn from the moment it
/// arrives: `reserve_sequence_numbers` runs `UPDATE … RETURNING` through
/// `query_row`. Any other statement is prepared on a pooled read connection
/// first, and handed to the writer if `SQLite` reports that it writes (DDL,
/// say); such a write joins the writer's queue only once a read connection has
/// prepared it. A read stays on the pool, since queueing it on the writer would
/// make a read nested inside a transaction wait for the transaction it is part
/// of.
fn statement_writes(sql: &str) -> bool {
    statement_keyword(sql).is_some_and(|keyword| {
        ["INSERT", "UPDATE", "DELETE", "REPLACE"]
            .iter()
            .any(|write| keyword.eq_ignore_ascii_case(write))
    })
}

/// The keyword that says what a statement does: its first word, or, when that
/// is `WITH`, the first word after the clause's common table expressions, so
/// `WITH a AS (…), b(x) AS MATERIALIZED (…) UPDATE …` gives `UPDATE`. `None`
/// when the text does not follow `SQLite`'s grammar for the clause.
fn statement_keyword(sql: &str) -> Option<&str> {
    let mut tokens = SqlTokens(sql);
    let SqlToken::Word(first) = tokens.next()? else {
        return None;
    };
    if !first.eq_ignore_ascii_case("WITH") {
        return Some(first);
    }
    let mut token = tokens.next()?;
    if token.is_keyword("RECURSIVE") {
        token = tokens.next()?;
    }
    // Each common table expression is
    // `name [(column, …)] AS [[NOT] MATERIALIZED] (select)`.
    loop {
        if !matches!(token, SqlToken::Word(_) | SqlToken::Quoted) {
            return None;
        }
        token = tokens.next()?;
        if token == SqlToken::Open {
            tokens.skip_group()?;
            token = tokens.next()?;
        }
        if !token.is_keyword("AS") {
            return None;
        }
        token = tokens.next()?;
        if token.is_keyword("NOT") {
            token = tokens.next()?;
            if !token.is_keyword("MATERIALIZED") {
                return None;
            }
            token = tokens.next()?;
        } else if token.is_keyword("MATERIALIZED") {
            token = tokens.next()?;
        }
        if token != SqlToken::Open {
            return None;
        }
        tokens.skip_group()?;
        match tokens.next()? {
            SqlToken::Comma => token = tokens.next()?,
            SqlToken::Word(keyword) => return Some(keyword),
            _ => return None,
        }
    }
}

/// A token of `SQLite`'s SQL, as far as [`statement_keyword`] tells them apart.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SqlToken<'a> {
    /// A keyword or an unquoted identifier.
    Word(&'a str),
    /// A string literal or a quoted identifier.
    Quoted,
    Open,
    Close,
    Comma,
    /// Any other character: an operator, a digit, a parameter's sigil.
    Other,
}

impl SqlToken<'_> {
    fn is_keyword(self, keyword: &str) -> bool {
        matches!(self, SqlToken::Word(word) if word.eq_ignore_ascii_case(keyword))
    }
}

/// The tokens of a statement's text, skipping whitespace and comments the way
/// `SQLite` does.
struct SqlTokens<'a>(&'a str);

impl SqlTokens<'_> {
    /// Skip past the `)` that closes the group whose `(` was just read.
    fn skip_group(&mut self) -> Option<()> {
        let mut depth = 1_usize;
        while depth > 0 {
            match self.next()? {
                SqlToken::Open => depth += 1,
                SqlToken::Close => depth -= 1,
                _ => {}
            }
        }
        Some(())
    }
}

impl<'a> Iterator for SqlTokens<'a> {
    type Item = SqlToken<'a>;

    fn next(&mut self) -> Option<SqlToken<'a>> {
        loop {
            self.0 = self.0.trim_start_matches(|c: char| c.is_ascii_whitespace());
            if let Some(rest) = self.0.strip_prefix("--") {
                self.0 = rest.split_once('\n').map_or("", |(_, rest)| rest);
            } else if let Some(rest) = self.0.strip_prefix("/*") {
                // An unterminated comment runs to the end of the text.
                self.0 = rest.split_once("*/").map_or("", |(_, rest)| rest);
            } else {
                break;
            }
        }
        let first = self.0.chars().next()?;
        let (token, len) = match first {
            '(' => (SqlToken::Open, 1),
            ')' => (SqlToken::Close, 1),
            ',' => (SqlToken::Comma, 1),
            '\'' | '"' | '`' => (SqlToken::Quoted, quoted_len(self.0, first)?),
            '[' => (SqlToken::Quoted, self.0.find(']')? + 1),
            c if is_identifier_char(c) && !c.is_ascii_digit() && c != '$' => {
                let len = self
                    .0
                    .find(|c: char| !is_identifier_char(c))
                    .unwrap_or(self.0.len());
                (SqlToken::Word(&self.0[..len]), len)
            }
            c => (SqlToken::Other, c.len_utf8()),
        };
        self.0 = &self.0[len..];
        Some(token)
    }
}

/// A character `SQLite` allows in an unquoted identifier: an ASCII letter or
/// digit, `_`, `$`, or any character outside ASCII.
fn is_identifier_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_' || c == '$' || !c.is_ascii()
}

/// The length of the string or quoted identifier at the start of `text`,
/// through the closing `quote`; a doubled quote inside it is an escaped one.
/// `None` when it is never closed.
fn quoted_len(text: &str, quote: char) -> Option<usize> {
    let mut chars = text.char_indices().skip(1);
    while let Some((i, c)) = chars.next() {
        if c == quote {
            if text[i + 1..].starts_with(quote) {
                chars.next();
            } else {
                return Some(i + 1);
            }
        }
    }
    None
}

/// A statement issued through a query method, as its read connection found it:
/// run there, or a write, handed back with its SQL and parameters to run on
/// the [`Writer`].
enum ReadAttempt<T> {
    Ran(T),
    Writes(String, Vec<rusqlite::types::Value>),
}

fn row_values(row: &rusqlite::Row<'_>) -> Result<Vec<MetastoreValue>, rusqlite::Error> {
    let column_count = row.as_ref().column_count();
    let mut values = Vec::with_capacity(column_count);
    for i in 0..column_count {
        values.push(convert_sqlite_value(row.get_ref(i)?));
    }
    Ok(values)
}

/// The values of the one row `sql` returns.
fn fetch_row(
    conn: &mut rusqlite::Connection,
    sql: &str,
    params: &[rusqlite::types::Value],
) -> Result<Vec<MetastoreValue>, rusqlite::Error> {
    conn.prepare_cached(sql)?
        .query_row(rusqlite::params_from_iter(params), row_values)
}

/// The values of every row `sql` returns.
fn fetch_rows(
    conn: &mut rusqlite::Connection,
    sql: &str,
    params: &[rusqlite::types::Value],
) -> Result<Vec<Vec<MetastoreValue>>, rusqlite::Error> {
    conn.prepare_cached(sql)?
        .query_map(rusqlite::params_from_iter(params), row_values)?
        .collect()
}

/// Round-robin connection pool for the [`SqliteMetastore`]'s reads, beside the
/// [`Writer`] every write runs on.
///
/// `SQLite` WAL mode allows concurrent readers and serializes writers at the
/// engine level. Having K independent connections means N concurrent readers
/// spread across K slots: for N ≤ K every reader finds a free slot immediately;
/// for N > K readers share proportionally, reducing the per-table wait from
/// O(N·RTT) to O(⌈N/K⌉·RTT).
///
/// Pool size is `min(cpu_budget().cores(), 32)` (minimum 2) — the runtime's
/// CPU entitlement, not the host's core count, since each connection carries
/// its own mmap and page cache. `SQLite` WAL mode allows many
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
    /// Connection used ONLY by [`SqliteMetastore::checkpoint_wal`]'s PASSIVE
    /// drain (cycle-8 TASK A2). The background maintenance-tick checkpoint runs
    /// here so it never contends a `conns` slot a read is waiting for, even under
    /// full pool saturation; a PASSIVE checkpoint takes no write lock, so it does
    /// not queue on the [`Writer`] either. It still targets the same shared
    /// `-wal` file, so a single checkpoint here covers the catalog's tables.
    checkpoint_conn: Arc<Mutex<tokio_rusqlite::Connection>>,
    /// This file's [`Writer`], shared with every other metastore open on it.
    writer: Arc<Writer>,
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
    /// Round-robin pool of K independent connections for reads, and the
    /// [`Writer`] every write runs on.
    ///
    /// K = `min(cpu_budget().cores(), 32)`, with a minimum of 2 — see the
    /// [`SqliteConnectionPool`] doc comment for the rationale. Lazily
    /// initialised on first use. A transaction runs on the writer connection
    /// for its full lifetime.
    pool: OnceCell<Arc<SqliteConnectionPool>>,
    /// Whether this DB file's *actual* `auto_vacuum` mode is INCREMENTAL.
    ///
    /// The mode is fixed at file creation (a config flip over an existing file
    /// is a no-op without a full VACUUM), so one probe is enough for the
    /// process lifetime. Cached so the maintenance tick does not re-issue
    /// `PRAGMA auto_vacuum` (or queue on the writer connection at all, when the
    /// answer is no) on every pass.
    db_auto_vacuum_is_incremental: OnceLock<bool>,
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
            db_auto_vacuum_is_incremental: OnceLock::new(),
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
        let in_memory = is_memory_db_path(db_path);

        // In-memory databases (Cayenne memory mode) have no backing file, so
        // there is no parent directory to create or fsync — skip straight to the
        // open. File-mode DBs create and sync the parent dir as before.
        if !in_memory {
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
        }

        // In-memory DSNs are URI filenames (e.g. `file:...?vfs=memdb`) that let
        // every pooled connection attach to the SAME shared database;
        // SQLITE_OPEN_URI is required to interpret them. It is part of
        // OpenFlags::default(), but we pass flags explicitly so the URI
        // dependency is not silently lost if that default ever changes. File
        // mode keeps the plain open.
        let open_result = if in_memory {
            tokio_rusqlite::Connection::open_with_flags(
                db_path.to_string(),
                rusqlite::OpenFlags::default() | rusqlite::OpenFlags::SQLITE_OPEN_URI,
            )
            .await
        } else {
            tokio_rusqlite::Connection::open(db_path).await
        };
        let conn = open_result.map_err(|e| CatalogError::Database {
            message: format!("Failed to open SQLite database: {e}"),
        })?;

        configure_sqlite_connection(&conn, in_memory)
            .await
            .map_err(
                |e: tokio_rusqlite::Error<rusqlite::Error>| CatalogError::Database {
                    message: format!("Failed to configure SQLite pragmas: {e}"),
                },
            )?;

        Ok(conn)
    }

    /// The writer connection shared by every metastore open on this one's
    /// file, opened by the first of them.
    async fn shared_writer(&self) -> CatalogResult<Arc<Writer>> {
        let key = writer_key(self.db_path()).await;
        let slot = {
            let mut writers = WRITERS.lock();
            // A slot nobody is opening through, whose writer connection has
            // closed, can go.
            writers.retain(|_, slot| {
                Arc::strong_count(slot) > 1
                    || !matches!(slot.try_lock(), Ok(writer) if writer.strong_count() == 0)
            });
            Arc::clone(writers.entry(key).or_default())
        };
        // Held across the open, so metastores opening one file at once share
        // one writer connection.
        let mut writer = slot.lock().await;
        if let Some(open) = writer.upgrade() {
            return Ok(open);
        }
        let opened = Arc::new(Writer {
            conn: self.open_connection().await?,
        });
        *writer = Arc::downgrade(&opened);
        Ok(opened)
    }

    /// Return the connection pool, initialising it lazily on first call.
    ///
    /// Opens K = `min(cpu_budget().cores(), 32)` read connections once and
    /// reuses them for the lifetime of the metastore, beside the shared writer
    /// connection. K is clamped to a minimum of 2.
    async fn pool(&self) -> CatalogResult<&Arc<SqliteConnectionPool>> {
        self.pool
            .get_or_try_init(|| async {
                let k = cpu_budget::cpu_budget().metastore_pool_connections();
                let mut conns = Vec::with_capacity(k);
                for _ in 0..k {
                    conns.push(Arc::new(Mutex::new(self.open_connection().await?)));
                }
                // cycle-8 TASK A2: dedicated checkpoint connection (see the field
                // doc). One extra connection per metastore DB, used only by the
                // background WAL drain so it never lands on a `conns` slot a
                // read is waiting for.
                let checkpoint_conn = Arc::new(Mutex::new(self.open_connection().await?));
                // After the opens, so a file-mode database exists and its
                // canonical path names the writer.
                let writer = self.shared_writer().await?;
                Ok(Arc::new(SqliteConnectionPool {
                    conns,
                    next: AtomicUsize::new(0),
                    checkpoint_conn,
                    writer,
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
    /// The table is keyed directly on `(table_id, pk_bytes)` as a
    /// `WITHOUT ROWID` composite primary key. The only access paths are
    /// `WHERE table_id = ?` (a leading-prefix scan, e.g.
    /// `get_insert_records` / `clear_insert_records`) and the
    /// `INSERT OR REPLACE` upsert keyed on `(table_id, pk_bytes)`; both are
    /// served by the composite PK. The previous `insert_record_id` UUID
    /// `TEXT PRIMARY KEY` was never read, filtered, or joined — it added a
    /// second B-tree and a 36-byte text alloc per row for no benefit, so it
    /// is dropped (see `init_schema` for the legacy-schema migration).
    ///
    /// `table_id` is stored as the **16 raw bytes of the table's UUID**
    /// (`BLOB`), not the 36-char hyphenated text. It is the leading field of
    /// the clustered `WITHOUT ROWID` key and is identical for every row of a
    /// burst, so the 20-byte/row text→raw-bytes shrink removes ~37% of the WAL
    /// frames a hot upsert burst writes (it both narrows each cell and packs
    /// more rows per B-tree leaf). The value is a pure re-encoding of the same
    /// constant — `cayenne_catalog::table_id_blob` translates the `table_id`
    /// `&str` once per call at every access path, so the `WHERE table_id = ?`
    /// prefix scan and the `(table_id, pk_bytes)` upsert conflict target are
    /// preserved 1:1; the reader (`get_insert_records`) only ever returns
    /// `pk_bytes` + `sequence_number` and never the key beyond the filter.
    ///
    /// The `FOREIGN KEY (table_id) → cayenne_table(table_id)` is intentionally
    /// **dropped**: under `PRAGMA foreign_keys = ON`, `SQLite` never equates a
    /// `BLOB` child value to the `TEXT`-affinity parent key, so the FK could
    /// not be satisfied by the raw-bytes encoding. The cascade it provided was
    /// belt-and-suspenders — `drop_table` already deletes the insert-records
    /// explicitly before the parent row, and `metastore::snapshot::import_dataset`
    /// now clears them explicitly too (it previously leaned on the cascade).
    /// The table is also fully cleared at every checkpoint/overwrite.
    const INSERT_RECORD_TABLE_DDL: &'static str = r"
        CREATE TABLE IF NOT EXISTS cayenne_insert_record (
            table_id BLOB NOT NULL,
            pk_bytes BLOB NOT NULL,
            sequence_number BIGINT NOT NULL,
            PRIMARY KEY (table_id, pk_bytes)
        ) WITHOUT ROWID
    ";

    /// Schema for the `cayenne_pending_write_back` table (durable federated
    /// write-back, #11838).
    ///
    /// Tracks the primary keys that a durable-write-back table has committed to
    /// the accelerator but not yet reconciled to the federated source. One row
    /// per undelivered key; the delivery worker claims rows, reconciles the
    /// key's current committed value to the source, then clears the row.
    ///
    /// `table_id` is the 16 raw UUID bytes (as in `cayenne_insert_record`);
    /// `pk_bytes` is the `RowConverter` `OwnedRow` encoding of the full primary
    /// key (bit-identical to the keyset/footprint `pk_digest` input) so the
    /// worker can rebuild both the accelerator point-scan filter and the source
    /// key. `sequence_number` is the table commit sequence that last dirtied the
    /// key; the marker upsert is **monotone** (keeps `MAX`) and the worker's
    /// compare-and-clear is `<= claimed_seq`, so a newer commit landing during
    /// delivery leaves the marker above the claimed sequence and the stale clear
    /// no-ops. `first_marked_at` is the oldest-undelivered timestamp (lag metric)
    /// and is never overwritten on re-mark.
    ///
    /// Unlike `cayenne_insert_record`, this table is **never** cleared at
    /// checkpoint/overwrite (that would drop acked-but-undelivered writes);
    /// `drop_table` deletes its rows explicitly.
    const PENDING_WRITE_BACK_TABLE_DDL: &'static str = r"
        CREATE TABLE IF NOT EXISTS cayenne_pending_write_back (
            table_id BLOB NOT NULL,
            pk_bytes BLOB NOT NULL,
            sequence_number BIGINT NOT NULL,
            first_marked_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
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
            num_rows_exact INTEGER NOT NULL DEFAULT 1,
            FOREIGN KEY (table_id) REFERENCES cayenne_table(table_id) ON DELETE CASCADE
        )
    ";

    /// Per-file footer statistics for listing-time pruning without re-reading
    /// every object on each scan. One row per `(table_id, snapshot_id, file_path)`.
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

    /// Authoritative per-snapshot data-file manifest (manifest snapshot model,
    /// phase 1). One row per `(table_id, snapshot_id, file_path)` for EVERY data
    /// file in the snapshot — unlike `cayenne_snapshot_file_statistics`, which is
    /// a best-effort pruning cache, this is the complete, authoritative file set
    /// (the future replacement for directory listing as the scan's file source).
    /// `min_sequence`/`max_sequence` carry the file's commit-seq range so
    /// compaction can bake a seq-prefix (`max_sequence <= T`) and reference the
    /// un-baked files in place. Populated atomically with every append/compaction
    /// write; rows are scoped to a snapshot so a new snapshot can reference an
    /// existing file by inserting a row pointing at the same path (no copy).
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

    /// Cold-tier object-store manifest (storage-cascade bottom tier). One row
    /// per Vortex file promoted to the cold object store. Table-scoped (no
    /// `snapshot_id`) and append-only: a promoted file is referenced only here,
    /// never from `cayenne_snapshot_file`. `file_url` is the absolute
    /// object-store URL (the cold location may differ from the warm table path).
    /// `statistics_blob` is the file's serialized Vortex `FileStatistics`
    /// (NOT NULL — always captured at promotion) so the scan prunes cold files
    /// at listing time without re-reading any footer. Captured in metastore
    /// snapshots via `EXPECTED_TABLES`.
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

    /// Index for the durable-write-back claim (#11838). The delivery worker pages
    /// markers in commit order with a `(sequence_number, pk_bytes)` keyset cursor,
    /// which the table's own `(table_id, pk_bytes)` primary key cannot serve: it
    /// would sort every one of the table's markers on each claim. Ordered to match
    /// the cursor, and covering, so a claim seeks to the resume point and reads
    /// only its page.
    const PENDING_WRITE_BACK_INDEX_DDL: &'static str = "CREATE INDEX IF NOT EXISTS idx_cayenne_pending_write_back_table_seq ON cayenne_pending_write_back(table_id, sequence_number, pk_bytes)";

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
        // Schema creation and the migrations below write, so they run on the
        // writer connection as one session, in its turn like any write. A write
        // that waits out its turn reads as the busy timeout the schema
        // creation's first write would have hit.
        let schema_error = |e: tokio_rusqlite::Error<rusqlite::Error>| CatalogError::Database {
            message: format!("Failed to initialize schema: {e}"),
        };
        let pool = self.pool().await?;
        let session = pool
            .writer
            .session(|_| Ok(()), |_, _| {})
            .await
            .map_err(schema_error)?;

        // Refuse to open a catalog written by a newer, incompatible Spice build
        // BEFORE running any migration against it (a fresh/legacy DB reads 0).
        let stored_version = session
            .call(|conn| conn.query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0)))
            .await
            .map_err(
                |e: tokio_rusqlite::Error<rusqlite::Error>| CatalogError::Database {
                    message: format!("Failed to read metastore schema version: {e}"),
                },
            )?;
        super::ensure_supported_schema_version(stored_version)?;

        session
            .call(|conn| {
                // Create tables in a transaction
                conn.execute_batch(&format!(
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
                // Whether the maintained `num_rows` is a provably-exact live count.
                // Legacy rows predate the mem-tier drift fix; DEFAULT 1 trusts their
                // count once (the next mem-tier checkpoint delta taints a drifted
                // one to 0, after which only a full-rewrite `Set` restores exactness).
                let _ = conn.execute(
                    "ALTER TABLE cayenne_table_statistics ADD COLUMN num_rows_exact INTEGER NOT NULL DEFAULT 1",
                    [],
                );

                // Metadata-only publish: per-commit reinsert sequence on delete-file
                // rows replaces the per-key cayenne_insert_record chunks. NULL on
                // legacy rows → the merge-on-read load falls back to
                // cayenne_insert_record, so adding the column is forward-upgrade safe.
                // (DOWNGRADE is NOT safe: an older binary on a catalog with this
                // column reads an empty insert-record table for new commits and would
                // drop the re-inserts — rebuild the catalog before downgrading. The
                // `user_version` gate at the top/bottom of this fn — bumped to
                // CAYENNE_METASTORE_SCHEMA_VERSION here — turns that into a loud
                // failure on any downgrade to a build with a lower max version.)
                let _ = conn.execute(
                    "ALTER TABLE cayenne_delete_file ADD COLUMN reinsert_sequence BIGINT",
                    [],
                );

                // End-to-end data-file integrity digest (opt-in
                // `cayenne_integrity_checksums`). NULL on legacy rows and on rows
                // written with the feature off → verification is skipped for
                // those files, so adding the column is forward- and
                // downgrade-safe (an older binary simply ignores the extra
                // column). Appended last to match the CREATE TABLE and
                // EXPECTED_TABLES column order.
                let _ = conn.execute(
                    "ALTER TABLE cayenne_snapshot_file ADD COLUMN digest TEXT",
                    [],
                );

                // Per-cold-file PK existence bloom. NULL (legacy / non-upsert /
                // over-cap) makes the keyset rebuild fall back to the exact cold
                // scan, so the column is forward- and downgrade-safe. Appended
                // last to match CREATE TABLE and EXPECTED_TABLES column order.
                let _ = conn.execute(
                    "ALTER TABLE cayenne_cold_tier_file ADD COLUMN pk_bloom_blob BLOB",
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

                // Migrate a legacy `cayenne_insert_record` to the current shape:
                // a `WITHOUT ROWID` composite PK `(table_id, pk_bytes)` whose
                // `table_id` is the 16 raw UUID bytes (`BLOB`) and which carries
                // no foreign key (see `INSERT_RECORD_TABLE_DDL`). Two legacy
                // layouts predate this and both store `table_id` as `TEXT`:
                //   (a) the pre-WITHOUT-ROWID UUID `insert_record_id` TEXT PK +
                //       redundant `UNIQUE(table_id, pk_bytes)`; and
                //   (b) the WITHOUT-ROWID composite PK with a TEXT `table_id`
                //       and a `cayenne_table(table_id)` FOREIGN KEY.
                // Both are detected by a single check — the declared type of the
                // `table_id` column is not `BLOB` — and migrated identically.
                //
                // The `CREATE TABLE IF NOT EXISTS` above leaves a pre-existing
                // table untouched, so we recreate it here in the new shape and
                // copy its rows forward, re-encoding each TEXT `table_id` to the
                // raw-bytes key via `table_id_to_key_bytes` (the same function the
                // write path uses, guaranteeing the migrated key matches what the
                // reader/upsert produce). The table is ephemeral (cleared at every
                // checkpoint via commit_compaction / commit_overwrite and
                // recoverable from the snapshot), so the copy-forward only
                // preserves in-flight pre-checkpoint re-insert sequences across
                // the upgrade — but it does so at trivial cost and keeps the
                // upgrade lossless. Runs inside the schema-init transaction so the
                // swap is atomic.
                let table_id_is_blob = conn
                    .prepare("PRAGMA table_info('cayenne_insert_record')")?
                    .query_map([], |row| {
                        Ok((row.get::<_, String>(1)?, row.get::<_, String>(2)?))
                    })?
                    .collect::<Result<Vec<(String, String)>, _>>()?
                    .iter()
                    .any(|(name, col_type)| {
                        name == "table_id" && col_type.eq_ignore_ascii_case("BLOB")
                    });
                if !table_id_is_blob {
                    // Read the legacy rows out (TEXT `table_id`), re-encode the
                    // `table_id` in Rust, then re-insert into the new BLOB table.
                    let legacy_rows: Vec<(String, Vec<u8>, i64)> = conn
                        .prepare(
                            "SELECT table_id, pk_bytes, sequence_number FROM cayenne_insert_record",
                        )?
                        .query_map([], |row| {
                            Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?, row.get::<_, i64>(2)?))
                        })?
                        .collect::<Result<Vec<_>, _>>()?;

                    conn.execute_batch(
                        "DROP TABLE cayenne_insert_record;
                        CREATE TABLE cayenne_insert_record (
                            table_id BLOB NOT NULL,
                            pk_bytes BLOB NOT NULL,
                            sequence_number BIGINT NOT NULL,
                            PRIMARY KEY (table_id, pk_bytes)
                        ) WITHOUT ROWID;",
                    )?;

                    if !legacy_rows.is_empty() {
                        let mut stmt = conn.prepare(
                            "INSERT OR REPLACE INTO cayenne_insert_record \
                             (table_id, pk_bytes, sequence_number) VALUES (?1, ?2, ?3)",
                        )?;
                        for (table_id_text, pk_bytes, sequence_number) in legacy_rows {
                            stmt.execute(rusqlite::params![
                                crate::metastore::table_id_to_key_bytes(&table_id_text),
                                pk_bytes,
                                sequence_number,
                            ])?;
                        }
                    }
                }

                Ok::<_, rusqlite::Error>(())
            })
            .await
            .map_err(schema_error)?;

        session
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

        // Kept out of the block above: that one reports every failure as duplicate
        // `cayenne_delete_file` paths, with remediation against that table.
        session
            .call(|conn| conn.execute(Self::PENDING_WRITE_BACK_INDEX_DDL, []))
            .await
            .map_err(
                |e: tokio_rusqlite::Error<rusqlite::Error>| CatalogError::Database {
                    message: format!("Failed to create pending write-back index: {e}"),
                },
            )?;

        // Stamp the current schema version now that all migrations have succeeded,
        // so a later downgrade to a build with a lower max version fails loudly at
        // the gate above instead of returning silently wrong results.
        session
            .call(|conn| {
                conn.pragma_update(
                    None,
                    "user_version",
                    super::CAYENNE_METASTORE_SCHEMA_VERSION,
                )
            })
            .await
            .map_err(
                |e: tokio_rusqlite::Error<rusqlite::Error>| CatalogError::Database {
                    message: format!("Failed to stamp metastore schema version: {e}"),
                },
            )?;

        // Validate that existing tables match the expected schema.
        // This catches incompatible metadata databases from previous versions.
        // End the session before validation, which only reads, on pooled
        // connections.
        drop(session);
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
        // METRIC 1: a bare autocommit write statement. Wait = until the
        // statement is handed to a connection (the writer connection, which a
        // bare write never waits to be handed to); held = the statement's run
        // from there, including its wait for its turn, since the WAL writer
        // lock is taken by the statement itself. Labeled `txn="other"` — this
        // generic path cannot cheaply know the originating catalog stage.
        let wait_start = std::time::Instant::now();
        let pool = self.pool().await?;
        telemetry::cayenne::track_metastore_writer_wait(
            wait_start.elapsed(),
            &[telemetry::KeyValue::new("txn", "other")],
        );
        let sql = params.sql.to_string();
        let param_values: Vec<rusqlite::types::Value> =
            params.params.into_iter().map(to_sqlite_value).collect();

        let held_start = std::time::Instant::now();
        pool.writer
            .run(move |conn| {
                let params_refs: Vec<&dyn rusqlite::ToSql> = param_values
                    .iter()
                    .map(|v| v as &dyn rusqlite::ToSql)
                    .collect();
                conn.prepare_cached(&sql)?.execute(params_refs.as_slice())?;
                Ok(())
            })
            .await
            .map_err(|e| convert_tokio_rusqlite_error(e, "Failed to execute statement"))?;
        telemetry::cayenne::track_metastore_writer_held(
            held_start.elapsed(),
            &[telemetry::KeyValue::new("txn", "other")],
        );

        Ok(())
    }

    async fn execute_batch(&self, sql: &str) -> CatalogResult<()> {
        let pool = self.pool().await?;
        let sql_owned = sql.to_string();

        pool.writer
            .run(move |conn| conn.execute_batch(&sql_owned))
            .await
            .map_err(
                |e: tokio_rusqlite::Error<rusqlite::Error>| CatalogError::Database {
                    message: format!("Failed to execute batch: {e}"),
                },
            )
    }

    async fn execute_transaction_batch(&self, sql: &str) -> CatalogResult<()> {
        let pool = self.pool().await?;
        let batch_sql = format!("BEGIN TRANSACTION; {sql}; COMMIT;");

        pool.writer
            .run(move |conn| {
                conn.execute_batch(&batch_sql).inspect_err(|_| {
                    let _ = conn.execute_batch("ROLLBACK");
                })
            })
            .await
            .map_err(
                |e: tokio_rusqlite::Error<rusqlite::Error>| CatalogError::Database {
                    message: format!("Failed to execute transaction batch: {e}"),
                },
            )
    }

    async fn query_row<F, T>(&self, params: QueryRowParams<'_>, f: F) -> CatalogResult<T>
    where
        F: FnOnce(&dyn MetastoreRow) -> CatalogResult<T> + Send + 'static,
        T: Send + 'static,
    {
        let query_error = |e: tokio_rusqlite::Error<rusqlite::Error>| CatalogError::Database {
            message: format!("Failed to query row: {e}"),
        };
        let pool = self.pool().await?;
        let sql = params.sql.to_string();
        let param_values: Vec<rusqlite::types::Value> =
            params.params.into_iter().map(to_sqlite_value).collect();

        // A write runs on the writer connection in its turn; a read on a pooled
        // connection. See `statement_writes`.
        let row_values = if statement_writes(&sql) {
            pool.writer
                .run(move |conn| fetch_row(conn, &sql, &param_values))
                .await
        } else {
            let attempt = pool
                .acquire()
                .await
                .call(move |conn| {
                    if !conn.prepare_cached(&sql)?.readonly() {
                        return Ok(ReadAttempt::Writes(sql, param_values));
                    }
                    fetch_row(conn, &sql, &param_values).map(ReadAttempt::Ran)
                })
                .await;
            match attempt {
                Ok(ReadAttempt::Ran(values)) => Ok(values),
                Ok(ReadAttempt::Writes(sql, param_values)) => {
                    pool.writer
                        .run(move |conn| fetch_row(conn, &sql, &param_values))
                        .await
                }
                Err(e) => Err(e),
            }
        }
        .map_err(query_error)?;

        // Apply the callback outside the rusqlite closure to preserve CatalogError
        let sqlite_row = SqliteRow { values: row_values };
        f(&sqlite_row)
    }

    async fn query<F, T>(&self, params: QueryParams<'_>, f: F) -> CatalogResult<Vec<T>>
    where
        F: Fn(&dyn MetastoreRow) -> CatalogResult<T> + Send + 'static,
        T: Send + 'static,
    {
        let query_error = |e: tokio_rusqlite::Error<rusqlite::Error>| CatalogError::Database {
            message: format!("Failed to query rows: {e}"),
        };
        let pool = self.pool().await?;
        let sql = params.sql.to_string();
        let param_values: Vec<rusqlite::types::Value> =
            params.params.into_iter().map(to_sqlite_value).collect();

        // A write runs on the writer connection in its turn; a read on a pooled
        // connection. See `statement_writes`.
        let all_row_values = if statement_writes(&sql) {
            pool.writer
                .run(move |conn| fetch_rows(conn, &sql, &param_values))
                .await
        } else {
            let attempt = pool
                .acquire()
                .await
                .call(move |conn| {
                    if !conn.prepare_cached(&sql)?.readonly() {
                        return Ok(ReadAttempt::Writes(sql, param_values));
                    }
                    fetch_rows(conn, &sql, &param_values).map(ReadAttempt::Ran)
                })
                .await;
            match attempt {
                Ok(ReadAttempt::Ran(rows)) => Ok(rows),
                Ok(ReadAttempt::Writes(sql, param_values)) => {
                    pool.writer
                        .run(move |conn| fetch_rows(conn, &sql, &param_values))
                        .await
                }
                Err(e) => Err(e),
            }
        }
        .map_err(query_error)?;

        // Apply the callback outside the rusqlite closure to preserve CatalogError
        let mut results = Vec::with_capacity(all_row_values.len());
        for row_values in all_row_values {
            let sqlite_row = SqliteRow { values: row_values };
            results.push(f(&sqlite_row)?);
        }

        Ok(results)
    }

    async fn begin_transaction(&self) -> CatalogResult<Box<dyn MetastoreTransaction>> {
        // METRIC 1 (writer wait): wall-clock from the call until BEGIN IMMEDIATE
        // has returned on the writer connection — the wait for its turn there
        // plus SQLite's reserved-lock acquire (the busy-timeout wait when another
        // process holds the lock). No `txn` stage label here: the generic
        // backend `begin_transaction` cannot cheaply know which catalog stage
        // opened it without threading a parameter through every call site, so it
        // records `"other"`.
        let wait_start = std::time::Instant::now();
        let pool = self.pool().await?;
        let session = pool
            .writer
            .session(begin_immediate, roll_back)
            .await
            .map_err(
                |e: tokio_rusqlite::Error<rusqlite::Error>| CatalogError::Database {
                    message: format!("Failed to begin transaction: {e}"),
                },
            )?;
        telemetry::cayenne::track_metastore_writer_wait(
            wait_start.elapsed(),
            &[telemetry::KeyValue::new("txn", "other")],
        );

        // METRIC 1 (writer held): the reserved write lock is held from this BEGIN
        // until commit/rollback/drop. Stamp the start so `SqliteTransaction` can
        // record the hold duration when it ends.
        Ok(Box::new(SqliteTransaction {
            session: Some(session),
            held_start: std::time::Instant::now(),
        }))
    }

    async fn shutdown(&self) -> CatalogResult<()> {
        // WAL checkpoint and optimize on the writer connection, in its turn like
        // any write: the TRUNCATE checkpoint and `PRAGMA optimize` write, and a
        // single checkpoint covers the shared file.
        if let Some(pool) = self.pool.get() {
            pool.writer
                .run(|conn| {
                    // Check if WAL mode is enabled
                    let journal_mode: String =
                        conn.query_row("PRAGMA journal_mode", [], |row| row.get(0))?;

                    if journal_mode.eq_ignore_ascii_case("wal") {
                        tracing::debug!("Truncating Cayenne catalog WAL log");
                        // Truncate the WAL log to persist changes and reduce file size
                        // wal_checkpoint returns results (busy, log, checkpointed), so we use query_row
                        let _: (i32, i32, i32) =
                            conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
                                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                            })?;
                    }

                    // Run optimize to improve query performance for future connections
                    // PRAGMA optimize may return rows indicating what was optimized
                    tracing::debug!("Running optimize on Cayenne catalog");
                    let mut stmt = conn.prepare("PRAGMA optimize")?;
                    let mut rows = stmt.query([])?;
                    while rows.next()?.is_some() {} // Consume all results to ensure PRAGMA completes

                    Ok(())
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
        // that copies committed frames into the main DB. The PASSIVE drain runs
        // on a DEDICATED connection (never a `conns` slot — see the field doc),
        // so it never delays a read.
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

        // Sample the -wal size BEFORE the checkpoint to pick the mode (cheap
        // stat()). Past the cap we TRUNCATE to reclaim the file; otherwise
        // PASSIVE keeps writers unblocked. The pre-checkpoint sample is the size
        // the truncate decision must be based on (the post-checkpoint sample
        // below is the resulting drained size for the gauge).
        let wal_bytes_before = self.read_wal_bytes().await;
        let truncate_due =
            wal_bytes_before > sqlite_metastore_config().wal_truncate_threshold_bytes;
        let mode_label = if truncate_due {
            "truncate_background"
        } else {
            "passive_background"
        };
        let checkpoint_error = |e: tokio_rusqlite::Error<rusqlite::Error>| CatalogError::Database {
            message: format!("Failed to checkpoint catalog WAL: {e}"),
        };

        // METRIC 2 (checkpoint duration): time the checkpoint with the chosen
        // background mode (this IS the off-hot-path background drain), including
        // any wait for the write lock.
        let checkpoint_start = std::time::Instant::now();
        // TRUNCATE holds the write lock while it waits out readers and resets the
        // file, so it runs on the writer connection in its turn like a write, and
        // waits only briefly for readers (see `TRUNCATE_CHECKPOINT_BUSY_TIMEOUT`).
        // If its turn does not come within the busy timeout, this tick drains
        // PASSIVE instead — the partial drain a TRUNCATE that finds the WAL busy
        // does — and the next tick tries again.
        let truncated = truncate_due
            && pool
                .writer
                .try_run(truncate_wal)
                .await
                .map_err(checkpoint_error)?
                .is_some();
        if !truncated {
            let guard = pool.checkpoint_conn.lock().await;
            guard
                .call(|conn| checkpoint_wal_file(conn, "PRAGMA wal_checkpoint(PASSIVE)"))
                .await
                .map_err(checkpoint_error)?;
        }
        telemetry::cayenne::track_metastore_checkpoint(
            checkpoint_start.elapsed(),
            &[telemetry::KeyValue::new("mode", mode_label)],
        );
        // METRIC 2 (WAL bytes): sample the -wal file size right after the
        // checkpoint copied as many frames as it could. A cheap stat(); a missing
        // file (just truncated) reports 0.
        self.sample_file_footprint().await;
        Ok(())
    }

    async fn incremental_vacuum(&self) -> CatalogResult<u64> {
        // `PRAGMA incremental_vacuum` takes the write lock while it relocates
        // pages, so it runs on the writer connection in its turn like any write.
        //
        // Ordering note for the caller: this belongs BEFORE the checkpoint in a
        // maintenance pass. In WAL mode the relocation is written as WAL frames
        // and the main DB file only shrinks when a checkpoint copies them back,
        // so vacuuming after the checkpoint would defer the actual truncation by
        // a whole tick.
        let cfg = sqlite_metastore_config();
        // Skip the writer connection when reclamation is not configured.
        // The DB's *actual* mode is still gated below (and cached) — config only
        // takes effect on a fresh file, so a later flip to Incremental must not
        // pretend an existing NONE/FULL database is reclaimable.
        if cfg.auto_vacuum != SqliteAutoVacuum::Incremental || cfg.incremental_vacuum_pages == 0 {
            return Ok(0);
        }
        // Cached live-mode probe: once we know the file is not INCREMENTAL, skip
        // the writer connection entirely on every subsequent tick. The mode is
        // fixed at file creation, so the answer never changes for this handle.
        if matches!(self.db_auto_vacuum_is_incremental.get(), Some(false)) {
            return Ok(0);
        }
        let max_pages = cfg.incremental_vacuum_pages;
        let Some(pool) = self.pool.get() else {
            return Ok(0);
        };

        let start = std::time::Instant::now();
        let map_err = |e: tokio_rusqlite::Error<rusqlite::Error>| CatalogError::Database {
            message: format!("Failed to incrementally vacuum the catalog: {e}"),
        };
        // Once the live mode is known INCREMENTAL, each tick is a single
        // freelist reclaim call. The first tick also probes the mode in that
        // same call so we never pay two round-trips to the connection thread.
        let reclaimed = if self.db_auto_vacuum_is_incremental.get() == Some(&true) {
            pool.writer
                .run(move |conn| reclaim_freelist_pages(conn, max_pages))
                .await
                .map_err(map_err)?
        } else {
            let (mode, reclaimed) = pool
                .writer
                .run(move |conn| {
                    // 0 = NONE, 1 = FULL, 2 = INCREMENTAL. FULL already reclaims
                    // at commit time, so it needs nothing here either. The mode
                    // itself is returned, not just "is it INCREMENTAL", so the
                    // caller can tell a FULL database (reclaiming, fine) from a
                    // NONE one (never reclaiming, worth saying so).
                    let mode = read_auto_vacuum_mode(conn)?;
                    if mode != SQLITE_AUTO_VACUUM_INCREMENTAL {
                        return Ok((mode, 0));
                    }
                    let n = reclaim_freelist_pages(conn, max_pages)?;
                    Ok((mode, n))
                })
                .await
                .map_err(map_err)?;
            let is_incremental = mode == SQLITE_AUTO_VACUUM_INCREMENTAL;
            // Racing first probes may both try to set; the value is deterministic
            // for a given file so a failed set is fine.
            // One-shot: this branch runs only until the mode is cached, so the
            // warning fires at most once per metastore file per process. Without
            // it a database created under the old `none` default is silently
            // never reclaimed, and the only symptom is a `.db` that never shrinks.
            // FULL is excluded — it reclaims on every commit, so it needs neither
            // this driver nor a migration.
            if self
                .db_auto_vacuum_is_incremental
                .set(is_incremental)
                .is_ok()
                && mode == SQLITE_AUTO_VACUUM_NONE
            {
                tracing::warn!(
                    "Metastore '{}' was created with `auto_vacuum` disabled, so freed pages are reused but never returned to the filesystem and the file stays at its high-water size. SQLite fixes this mode at file creation: to adopt the `incremental` default, stop the runtime and run `PRAGMA auto_vacuum = INCREMENTAL; VACUUM;` against the file, then restart — a `VACUUM` on its own keeps the file on `none`. See: https://spiceai.org/docs/components/data-accelerators/cayenne",
                    self.db_path()
                );
            }
            if !is_incremental {
                return Ok(0);
            }
            reclaimed
        };

        if reclaimed > 0 {
            telemetry::cayenne::track_metastore_incremental_vacuum(start.elapsed(), reclaimed);
            tracing::debug!(reclaimed_pages = reclaimed, "Metastore incremental vacuum");
        }
        Ok(reclaimed)
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

    /// Sample the metastore's on-disk footprint — the database file and its
    /// `-wal` — and publish both gauges.
    ///
    /// Two `stat()` calls on a background tick. The WAL half was already
    /// exported; the database half is the larger and slower-moving of the two,
    /// and without it the total metadata footprint could not be read off
    /// `/metrics` at all.
    ///
    /// Both carry a `catalog` label. The metastore is per-dataset and its file
    /// is always named `cayenne.db`, so an unlabelled gauge has every dataset's
    /// sample overwriting the others on one series — which is what the WAL gauge
    /// did before this.
    async fn sample_file_footprint(&self) {
        // Both stats in ONE `spawn_blocking`. `tokio::fs` dispatches each call to
        // the blocking pool individually, and this runs on the post-write
        // maintenance loop's ~100 ms debounce — so two `tokio::fs::metadata`
        // calls would be two task hops per pass under sustained CDC, for two
        // numbers a scrape reads once a second.
        let db_path = self.db_path().to_string();
        let wal_path = format!("{db_path}-wal");
        let Ok((db_bytes, wal_bytes)) =
            tokio::task::spawn_blocking(move || measure_file_footprint(&db_path, &wal_path)).await
        else {
            return;
        };

        let dimensions = [telemetry::KeyValue::new(
            "catalog",
            self.db_path().to_string(),
        )];
        if let Some(wal_bytes) = wal_bytes {
            telemetry::cayenne::track_metastore_wal_bytes(wal_bytes, &dimensions);
        }
        if let Some(db_bytes) = db_bytes {
            telemetry::cayenne::track_metastore_db_bytes(db_bytes, &dimensions);
        }
    }
}

/// `stat` the metastore file and its `-wal`, as `(database bytes, WAL bytes)`.
///
/// `None` means NOT MEASURED, and the caller leaves that gauge at its previous
/// value. Publishing a failed `stat` as `0` would say the metastore shrank to
/// nothing — a louder and more wrong statement than saying nothing at all, and
/// the same rule the table footprint sample follows when its query fails.
///
/// A missing `-wal` is the one real zero: the WAL is created on the first write
/// and removed on a clean close, so its absence means no WAL bytes rather than a
/// measurement that failed. `NotFound` on the database itself is NOT the same
/// statement — an open `SQLite` database stays live and allocated after its
/// pathname is unlinked, so its size is unknown, not zero.
fn measure_file_footprint(db_path: &str, wal_path: &str) -> (Option<u64>, Option<u64>) {
    let db = std::fs::metadata(db_path).map(|m| m.len()).ok();
    let wal = match std::fs::metadata(wal_path) {
        Ok(metadata) => Some(metadata.len()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Some(0),
        Err(_) => None,
    };
    (db, wal)
}

/// A transaction on the metastore's [`Writer`] connection: a [`Session`] from
/// `BEGIN IMMEDIATE` until [`commit`](MetastoreTransaction::commit) or
/// [`rollback`](MetastoreTransaction::rollback), while the writes queued behind
/// it wait. Dropped without either, it is rolled back on the writer's thread as
/// its session ends. METRIC 1 `cayenne_metastore_writer_held_ms` is recorded on
/// the writer's thread on every path, once the write lock is released.
pub struct SqliteTransaction {
    /// The transaction's session. `None` after commit/rollback.
    session: Option<Session>,
    /// When the reserved write lock was acquired (BEGIN IMMEDIATE returned),
    /// for the hold commit and rollback record.
    held_start: std::time::Instant,
}

impl SqliteTransaction {
    fn session(&self) -> CatalogResult<&Session> {
        self.session.as_ref().ok_or_else(|| CatalogError::Database {
            message: "Transaction already completed".to_string(),
        })
    }

    fn take_session(&mut self) -> CatalogResult<Session> {
        self.session.take().ok_or_else(|| CatalogError::Database {
            message: "Transaction already completed".to_string(),
        })
    }
}

#[async_trait]
impl MetastoreTransaction for SqliteTransaction {
    async fn execute(&self, params: ExecuteParams<'_>) -> CatalogResult<()> {
        let session = self.session()?;
        let sql = params.sql.to_string();
        let param_values: Vec<rusqlite::types::Value> =
            params.params.into_iter().map(to_sqlite_value).collect();

        session
            .call(move |conn| {
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

    async fn execute_many(&self, sql: &str, params: Vec<Vec<MetastoreValue>>) -> CatalogResult<()> {
        let session = self.session()?;
        let sql: Arc<str> = Arc::from(sql);

        // One `call` per chunk of rows: the statement is prepared once and each
        // row is a step on the writer's thread, not a channel round trip. A
        // `call` runs to completion even if its caller stops waiting, so the
        // chunk bounds what a cancelled batch still executes under the write
        // lock before the transaction's `Drop` can roll it back.
        let mut rows = params.into_iter().peekable();
        while rows.peek().is_some() {
            let chunk: Vec<Vec<rusqlite::types::Value>> = rows
                .by_ref()
                .take(EXECUTE_MANY_ROWS_PER_CALL)
                .map(|row| row.into_iter().map(to_sqlite_value).collect())
                .collect();
            let sql = Arc::clone(&sql);
            session
                .call(move |conn| {
                    let mut stmt = conn.prepare_cached(&sql)?;
                    for row in &chunk {
                        stmt.execute(rusqlite::params_from_iter(row))?;
                    }
                    Ok::<_, rusqlite::Error>(())
                })
                .await
                .map_err(|e| {
                    convert_tokio_rusqlite_error(e, "Failed to execute statement in transaction")
                })?;
        }

        Ok(())
    }

    async fn query_values(
        &self,
        params: QueryParams<'_>,
    ) -> CatalogResult<Vec<Vec<MetastoreValue>>> {
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

            let mut stmt = conn.prepare_cached(&sql)?;
            let rows = stmt.query_map(params_refs.as_slice(), |row| {
                let column_count = row.as_ref().column_count();
                let mut values = Vec::with_capacity(column_count);
                for i in 0..column_count {
                    values.push(convert_sqlite_value(row.get_ref(i)?));
                }
                Ok(values)
            })?;
            rows.collect::<std::result::Result<Vec<_>, _>>()
        })
        .await
        .map_err(|e| convert_tokio_rusqlite_error(e, "Failed to query rows in transaction"))
    }

    async fn query_row_values(
        &self,
        params: QueryRowParams<'_>,
    ) -> CatalogResult<Vec<MetastoreValue>> {
        let session = self.session()?;
        let sql = params.sql.to_string();
        let param_values: Vec<rusqlite::types::Value> =
            params.params.into_iter().map(to_sqlite_value).collect();

        session
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
                    message: format!("Failed to query row in transaction: {e}"),
                },
            )
    }

    async fn execute_batch(&self, sql: &str) -> CatalogResult<()> {
        let session = self.session()?;
        let sql_owned = sql.to_string();

        session
            .call(move |conn| {
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
        let session = self.take_session()?;

        // COMMIT, and on a failed COMMIT the best-effort ROLLBACK that leaves the
        // writer connection clean, run as the session's last statement: the
        // next write starts only once both are done.
        let held_start = self.held_start;
        let commit_result = session
            .finish(move |conn| {
                let committed = conn.execute_batch("COMMIT").inspect_err(|_| {
                    let _ = conn.execute_batch("ROLLBACK");
                });
                // METRIC 1 (writer held): record AFTER the write lock is actually
                // released, on the writer's thread: when COMMIT returns (the
                // BEGIN IMMEDIATE lock is held through COMMIT's fsync, so a
                // contending writer blocks until then), or on a failed COMMIT
                // once its ROLLBACK has run. Recording any earlier under-reports
                // the hold window the next writer queues behind (PR #11206
                // review).
                telemetry::cayenne::track_metastore_writer_held(
                    held_start.elapsed(),
                    &[telemetry::KeyValue::new("txn", "other")],
                );
                committed
            })
            .await;

        commit_result.map_err(
            |e: tokio_rusqlite::Error<rusqlite::Error>| CatalogError::Database {
                message: format!("Failed to commit transaction: {e}"),
            },
        )
    }

    async fn rollback(mut self: Box<Self>) -> CatalogResult<()> {
        let session = self.take_session()?;

        let held_start = self.held_start;
        let rollback_result = session
            .finish(move |conn| {
                let rolled_back = conn.execute_batch("ROLLBACK");
                // METRIC 1 (writer held): record AFTER ROLLBACK, on the writer's
                // thread — the write lock is held through the rollback
                // statement, so include its duration (PR #11206).
                telemetry::cayenne::track_metastore_writer_held(
                    held_start.elapsed(),
                    &[telemetry::KeyValue::new("txn", "other")],
                );
                rolled_back
            })
            .await;

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
    use std::fmt::Write;

    /// `SQLITE_METASTORE_CONFIG` is process-wide and read at connection-open
    /// time (and, for the truncate threshold, at `checkpoint_wal` time). The
    /// cycle-8 TASK A2 tests below mutate it, so they serialize through this lock
    /// and each sets the exact config it needs while holding it. NOTE: this only
    /// serializes the tests WITHIN this module — it does NOT prevent a cayenne
    /// test in another module of the same test binary from observing the global
    /// override while one of these holds the lock. That is acceptable because the
    /// other suites don't assert on this config; if one ever did, it would need
    /// its own coordination. A `tokio` Mutex (not `std`) so the guard can be held
    /// across the `.await`s in the test body (the writes are tiny) without the
    /// held-guard-across-await lint.
    static CONFIG_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    fn temp_metastore() -> (tempfile::TempDir, SqliteMetastore) {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("cayenne_test.db");
        let metastore = SqliteMetastore::new(format!("sqlite://{}", db_path.display()));
        (dir, metastore)
    }

    /// Build an in-memory (memdb) metastore for memory-mode tests. Each caller
    /// MUST pass a unique `name` — memdb shares one in-memory database per name
    /// across the whole process, so a shared name would leak state between tests.
    fn in_memory_metastore(name: &str) -> SqliteMetastore {
        SqliteMetastore::new(format!("sqlite://file:/{name}?vfs=memdb"))
    }

    async fn count_rows(tx: &dyn MetastoreTransaction, sql: &str) -> i64 {
        let value = tx
            .query_row_values(QueryRowParams {
                sql,
                params: vec![],
            })
            .await
            .expect("count query")
            .into_iter()
            .next()
            .expect("one column");
        let MetastoreValue::Integer(count) = value else {
            panic!("COUNT(*) returned {value:?}");
        };
        count
    }

    /// `execute_many` runs its statement once per entry, in order, across chunk
    /// boundaries, and the first entry that fails stops the batch with that
    /// entry's error — what a loop of `execute` calls does, so the caller's
    /// rollback leaves nothing behind.
    #[tokio::test]
    async fn test_execute_many_runs_every_entry_and_stops_at_the_first_failure() {
        const INSERT: &str = "INSERT INTO t (id, label) VALUES (?1, ?2)";
        fn row(id: i64) -> Vec<MetastoreValue> {
            vec![
                MetastoreValue::Integer(id),
                MetastoreValue::Text(format!("row-{id}")),
            ]
        }
        let chunk = i64::try_from(EXECUTE_MANY_ROWS_PER_CALL).expect("chunk size fits i64");
        let (_dir, metastore) = temp_metastore();
        metastore
            .execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, label TEXT NOT NULL)")
            .await
            .expect("create table");

        // Two full chunks and a partial third.
        let total = chunk * 2 + 17;
        let tx = metastore.begin_transaction().await.expect("begin");
        tx.execute_many(INSERT, Vec::new())
            .await
            .expect("an empty batch is a no-op");
        tx.execute_many(INSERT, (0..total).map(row).collect())
            .await
            .expect("insert batch");
        tx.commit().await.expect("commit");

        let tx = metastore.begin_transaction().await.expect("begin");
        assert_eq!(
            count_rows(tx.as_ref(), "SELECT COUNT(*) FROM t").await,
            total,
            "every entry of every chunk must run"
        );
        // A full first chunk of new keys, then a duplicate at the start of the
        // second chunk, then more new keys the batch must never reach.
        let first_new = total;
        let mut batch: Vec<Vec<MetastoreValue>> = (first_new..first_new + chunk).map(row).collect();
        batch.push(row(0));
        batch.extend((first_new + chunk..first_new + chunk + 5).map(row));
        let result = tx.execute_many(INSERT, batch).await;
        assert!(
            matches!(result, Err(CatalogError::ConstraintViolation { .. })),
            "the failing entry's constraint violation must surface: {result:?}"
        );
        assert_eq!(
            count_rows(
                tx.as_ref(),
                &format!("SELECT COUNT(*) FROM t WHERE id >= {first_new}")
            )
            .await,
            chunk,
            "entries before the failure stay applied until the caller rolls back, and none after it run"
        );
        tx.rollback().await.expect("rollback");

        let tx = metastore.begin_transaction().await.expect("begin");
        assert_eq!(
            count_rows(tx.as_ref(), "SELECT COUNT(*) FROM t").await,
            total,
            "rolling back drops the partially applied batch"
        );
        tx.rollback().await.expect("rollback");
    }

    /// Writers queued behind a held write lock are granted it in the order they
    /// arrived. `SQLite`'s busy handler grants no order — each waiter wakes on its
    /// own backoff and whoever retries first wins — so without the writer
    /// connection this order is effectively random.
    #[tokio::test]
    async fn test_writers_are_granted_the_write_lock_in_arrival_order() {
        const WRITERS: i64 = 16;
        let (_dir, metastore) = temp_metastore();
        let metastore = Arc::new(metastore);
        metastore
            .execute_batch(
                "CREATE TABLE t (seq INTEGER PRIMARY KEY AUTOINCREMENT, writer INTEGER NOT NULL)",
            )
            .await
            .expect("create table");

        let holder = metastore.begin_transaction().await.expect("begin");
        let mut writers = Vec::new();
        for writer in 0..WRITERS {
            let metastore = Arc::clone(&metastore);
            writers.push(tokio::spawn(async move {
                metastore
                    .execute(ExecuteParams {
                        sql: "INSERT INTO t (writer) VALUES (?1)",
                        params: vec![MetastoreValue::Integer(writer)],
                    })
                    .await
            }));
            // Arrival order is what is under test: each writer must be queued
            // before the next one starts, and reaching the queue takes
            // microseconds.
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        holder.commit().await.expect("commit the holder");
        for writer in writers {
            writer
                .await
                .expect("writer task")
                .expect("a queued writer must get the lock, not time out");
        }

        let order: Vec<i64> = metastore
            .query(
                QueryParams {
                    sql: "SELECT writer FROM t ORDER BY seq",
                    params: vec![],
                },
                |row| row.get_i64(0),
            )
            .await
            .expect("read commit order");
        assert_eq!(
            order,
            (0..WRITERS).collect::<Vec<_>>(),
            "writers must commit in the order they queued for the write lock"
        );
    }

    /// The error each write path returns while another writer holds the write
    /// lock, as `(path, message, retryable)`.
    async fn contended_write_errors(
        metastore: &SqliteMetastore,
    ) -> Vec<(&'static str, String, bool)> {
        let describe = |path: &'static str, error: CatalogError| {
            let retryable = crate::cayenne_catalog::is_retryable_write_conflict(&error);
            (path, error.to_string(), retryable)
        };
        let returning = "UPDATE t SET n = n + 1 WHERE id = 1 RETURNING n";
        vec![
            describe(
                "execute",
                metastore
                    .execute(ExecuteParams {
                        sql: "INSERT INTO t (id, n) VALUES (2, 0)",
                        params: vec![],
                    })
                    .await
                    .expect_err("an autocommit write must not get the held lock"),
            ),
            describe(
                "execute_batch",
                metastore
                    .execute_batch("INSERT INTO t (id, n) VALUES (3, 0)")
                    .await
                    .expect_err("a batch must not get the held lock"),
            ),
            describe(
                "execute_transaction_batch",
                metastore
                    .execute_transaction_batch("INSERT INTO t (id, n) VALUES (4, 0)")
                    .await
                    .expect_err("a transaction batch must not get the held lock"),
            ),
            describe(
                "begin_transaction",
                metastore
                    .begin_transaction()
                    .await
                    .err()
                    .expect("a transaction must not begin while the lock is held"),
            ),
            describe(
                "query_row",
                metastore
                    .query_row(
                        QueryRowParams {
                            sql: returning,
                            params: vec![],
                        },
                        |row| row.get_i64(0),
                    )
                    .await
                    .expect_err("a write issued through query_row must not get the held lock"),
            ),
            describe(
                "query",
                metastore
                    .query(
                        QueryParams {
                            sql: returning,
                            params: vec![],
                        },
                        |row| row.get_i64(0),
                    )
                    .await
                    .expect_err("a write issued through query must not get the held lock"),
            ),
        ]
    }

    /// A write that waits out its turn on the writer connection gets exactly the
    /// error it gets when `SQLite`'s busy handler times out on a lock held by
    /// another process, on every write path, including an `UPDATE … RETURNING`
    /// issued as a query: the same message and the same retryable `database is
    /// locked`. A read runs regardless of who holds the writer connection.
    #[tokio::test]
    async fn test_a_write_that_waits_out_its_turn_reads_like_a_busy_timeout() {
        let _guard = CONFIG_LOCK.lock().await;
        set_sqlite_metastore_config(SqliteMetastoreConfig {
            busy_timeout_ms: 200,
            ..SqliteMetastoreConfig::default()
        });
        let (_dir, metastore) = temp_metastore();
        metastore
            .execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER NOT NULL)")
            .await
            .expect("create table");
        metastore
            .execute(ExecuteParams {
                sql: "INSERT INTO t (id, n) VALUES (1, 0)",
                params: vec![],
            })
            .await
            .expect("seed row");

        // Another process holds the write lock: every write gets its turn on the
        // writer connection, and SQLite's busy handler times out.
        let other_process =
            rusqlite::Connection::open(metastore.db_path()).expect("open a second connection");
        other_process
            .execute_batch("BEGIN IMMEDIATE")
            .expect("take the write lock from another connection");
        let busy_timeouts = contended_write_errors(&metastore).await;
        other_process
            .execute_batch("ROLLBACK")
            .expect("release the write lock");

        // An in-process transaction holds the writer connection: every write's
        // turn fails to come in time.
        let holder = metastore.begin_transaction().await.expect("begin");
        let turn_timeouts = contended_write_errors(&metastore).await;
        let n = metastore
            .query_row(
                QueryRowParams {
                    sql: "SELECT n FROM t WHERE id = 1",
                    params: vec![],
                },
                |row| row.get_i64(0),
            )
            .await
            .expect("a read must not wait for the writer connection");
        holder.rollback().await.expect("rollback the holder");
        set_sqlite_metastore_config(SqliteMetastoreConfig::default());

        assert_eq!(
            turn_timeouts, busy_timeouts,
            "a write that waits out its turn must fail exactly like SQLite's busy timeout"
        );
        for (path, message, retryable) in &turn_timeouts {
            assert!(
                *retryable && message.contains("database is locked"),
                "{path}: a writer that waited out the busy timeout must see a retryable database is locked: {message}"
            );
        }
        assert_eq!(n, 0, "no contended write may have applied");
    }

    /// Every metastore open on one file shares its writer connection, however
    /// the path is spelled, so two catalogs on the same file queue their writes
    /// together.
    #[tokio::test]
    async fn test_metastores_on_one_file_share_the_writer_connection() {
        let dir = tempfile::tempdir().expect("tempdir");
        let direct = SqliteMetastore::new(format!(
            "sqlite://{}",
            dir.path().join("cayenne.db").display()
        ));
        let dotted = SqliteMetastore::new(format!(
            "sqlite://{}",
            dir.path().join(".").join("cayenne.db").display()
        ));
        let other = SqliteMetastore::new(format!(
            "sqlite://{}",
            dir.path().join("other.db").display()
        ));
        let direct_writer = Arc::clone(&direct.pool().await.expect("pool").writer);
        let dotted_writer = Arc::clone(&dotted.pool().await.expect("pool").writer);
        let other_writer = Arc::clone(&other.pool().await.expect("pool").writer);
        assert!(
            Arc::ptr_eq(&direct_writer, &dotted_writer),
            "two spellings of one metastore file must share its writer connection"
        );
        assert!(
            !Arc::ptr_eq(&direct_writer, &other_writer),
            "different metastore files must not share a writer connection"
        );
    }

    /// How long the last session aborted by `record_abort` had held the writer
    /// connection, in microseconds.
    static ABORTED_HOLD_MICROS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

    fn record_abort(conn: &mut rusqlite::Connection, began: std::time::Instant) {
        roll_back(conn, began);
        let held = u64::try_from(began.elapsed().as_micros()).unwrap_or(u64::MAX);
        ABORTED_HOLD_MICROS.store(held, std::sync::atomic::Ordering::SeqCst);
    }

    /// A dropped transaction's hold is measured once its rollback has run on
    /// the writer's thread, not when its caller let go: a statement still
    /// running there keeps the write lock until it ends.
    #[tokio::test]
    async fn test_a_dropped_transaction_is_timed_until_its_rollback() {
        // How long the statement still running when the transaction is dropped
        // takes; time is what is under test.
        const IN_FLIGHT: std::time::Duration = std::time::Duration::from_millis(200);
        let (_dir, metastore) = temp_metastore();
        let pool = metastore.pool().await.expect("pool");
        let session = pool
            .writer
            .session(begin_immediate, record_abort)
            .await
            .expect("begin");
        let began = std::time::Instant::now();
        let (running_tx, running) = tokio::sync::oneshot::channel();
        session
            .jobs
            .send(Box::new(move |_: &mut rusqlite::Connection| {
                let _ = running_tx.send(());
                std::thread::sleep(IN_FLIGHT);
                SessionStep::Continue
            }))
            .expect("queue a statement");
        running.await.expect("the statement started");
        drop(session);
        let dropped_after = began.elapsed();

        // The writer takes its next write only once the dropped session ends.
        metastore
            .execute_batch("SELECT 1")
            .await
            .expect("a write after the dropped transaction");
        let held = std::time::Duration::from_micros(
            ABORTED_HOLD_MICROS.load(std::sync::atomic::Ordering::SeqCst),
        );
        assert!(
            held >= IN_FLIGHT,
            "the dropped transaction held the lock through its {IN_FLIGHT:?} statement but was timed at {held:?} (its caller let go after {dropped_after:?})"
        );
    }

    /// A write that does not start with a write keyword, here a CTE-prefixed
    /// `UPDATE … RETURNING` issued through `query_row`, still waits its turn on
    /// the writer connection, so it runs before a write queued after it.
    #[tokio::test]
    async fn test_a_cte_write_through_a_query_waits_its_turn_on_the_writer() {
        let _guard = CONFIG_LOCK.lock().await;
        set_sqlite_metastore_config(SqliteMetastoreConfig::default());
        let (_dir, metastore) = temp_metastore();
        let metastore = Arc::new(metastore);
        metastore
            .execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER NOT NULL)")
            .await
            .expect("create table");
        metastore
            .execute(ExecuteParams {
                sql: "INSERT INTO t (id, n) VALUES (1, 1)",
                params: vec![],
            })
            .await
            .expect("seed row");

        let holder = metastore.begin_transaction().await.expect("begin");
        let multiply = {
            let metastore = Arc::clone(&metastore);
            tokio::spawn(async move {
                metastore
                    .query_row(
                        QueryRowParams {
                            sql: "WITH factor(v) AS (VALUES (10)) UPDATE t SET n = n * (SELECT v FROM factor) WHERE id = 1 RETURNING n",
                            params: vec![],
                        },
                        |row| row.get_i64(0),
                    )
                    .await
            })
        };
        // Arrival order is what is under test: the CTE write must be queued
        // before the next write, and reaching the queue takes microseconds.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let add = {
            let metastore = Arc::clone(&metastore);
            tokio::spawn(async move {
                metastore
                    .execute(ExecuteParams {
                        sql: "UPDATE t SET n = n + 1 WHERE id = 1",
                        params: vec![],
                    })
                    .await
            })
        };
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        holder.commit().await.expect("commit the holder");
        multiply
            .await
            .expect("CTE write task")
            .expect("the CTE write");
        add.await
            .expect("write task")
            .expect("the write queued after it");

        let n = metastore
            .query_row(
                QueryRowParams {
                    sql: "SELECT n FROM t WHERE id = 1",
                    params: vec![],
                },
                |row| row.get_i64(0),
            )
            .await
            .expect("read n");
        assert_eq!(
            n, 11,
            "the CTE write must run in its turn on the writer, before the write queued after it (1 × 10 + 1)"
        );
    }

    /// The query methods' keyword check calls a statement a write exactly when
    /// `SQLite` says it writes, however its `WITH` clause, comments and quoting
    /// are spelled, and leaves any text it cannot follow to `SQLite`'s check.
    #[test]
    fn test_statement_writes_agrees_with_sqlite() {
        let conn = rusqlite::Connection::open_in_memory().expect("open in-memory SQLite");
        conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER)")
            .expect("create table");
        for sql in [
            "SELECT n FROM t",
            "  select n from t",
            "VALUES (1)",
            "UPDATE t SET n = 1",
            "insert into t (id, n) values (1, 1)",
            "REPLACE INTO t (id, n) VALUES (1, 1)",
            "DELETE FROM t",
            "-- a comment ) (\nUPDATE t SET n = 1",
            "/* a comment */ SELECT 1",
            "WITH f(v) AS (VALUES (10)) UPDATE t SET n = n * (SELECT v FROM f) WHERE id = 1 RETURNING n",
            "WITH RECURSIVE r(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM r WHERE i < 3) INSERT INTO t (id, n) SELECT i, i FROM r",
            "WITH a AS MATERIALIZED (SELECT 1 AS x), b AS NOT MATERIALIZED (SELECT 2 AS y) DELETE FROM t WHERE id IN (SELECT x FROM a UNION SELECT y FROM b)",
            "WITH a AS (SELECT ')' AS p /* ) */ -- )\n FROM t) SELECT p FROM a",
            "WITH \"a)\" AS (SELECT 1 AS x) REPLACE INTO t (id, n) SELECT x, x FROM \"a)\"",
            "WITH [a b] AS (SELECT 'it''s' AS x) SELECT x FROM [a b]",
            "WITH replace AS (SELECT 1 AS x) SELECT x FROM replace",
            "WITH faktör(v) AS (VALUES (2)) UPDATE t SET n = n * (SELECT v FROM faktör)",
            "WITH a AS (SELECT 1 AS x) SELECT x FROM a",
        ] {
            let sqlite_writes = !conn
                .prepare(sql)
                .unwrap_or_else(|e| panic!("SQLite must accept {sql:?}: {e}"))
                .readonly();
            assert_eq!(statement_writes(sql), sqlite_writes, "{sql:?}");
        }
        for sql in [
            "WITH a AS (SELECT 'unterminated",
            "WITH a (SELECT 1) UPDATE t SET n = 1",
            "WITH",
            "",
        ] {
            assert!(
                !statement_writes(sql),
                "{sql:?} does not follow the grammar, so it is left to SQLite's check"
            );
        }
    }

    /// A CTE-prefixed write issued through a query method is recognized by the
    /// statement its `WITH` clause leads into, so it queues on the writer
    /// connection the moment it arrives: it waits for no read connection, and a
    /// write that arrives after it cannot run first.
    #[tokio::test]
    async fn test_a_cte_write_through_a_query_does_not_wait_for_a_read_connection() {
        let _guard = CONFIG_LOCK.lock().await;
        set_sqlite_metastore_config(SqliteMetastoreConfig::default());
        let (_dir, metastore) = temp_metastore();
        let metastore = Arc::new(metastore);
        metastore
            .execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER NOT NULL)")
            .await
            .expect("create table");
        metastore
            .execute(ExecuteParams {
                sql: "INSERT INTO t (id, n) VALUES (1, 1)",
                params: vec![],
            })
            .await
            .expect("seed row");

        // Every read connection taken, as by a burst of reads.
        let pool = metastore.pool().await.expect("pool");
        let mut reads = Vec::with_capacity(pool.conns.len());
        for conn in &pool.conns {
            reads.push(Arc::clone(conn).lock_owned().await);
        }
        let multiply = {
            let metastore = Arc::clone(&metastore);
            tokio::spawn(async move {
                metastore
                    .query_row(
                        QueryRowParams {
                            sql: "WITH factor(v) AS (VALUES (10)) UPDATE t SET n = n * (SELECT v FROM factor) WHERE id = 1 RETURNING n",
                            params: vec![],
                        },
                        |row| row.get_i64(0),
                    )
                    .await
            })
        };
        // Arrival order is what is under test: the CTE write arrives first.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        metastore
            .execute(ExecuteParams {
                sql: "UPDATE t SET n = n + 1 WHERE id = 1",
                params: vec![],
            })
            .await
            .expect("the write that arrived after the CTE write");
        drop(reads);
        multiply
            .await
            .expect("CTE write task")
            .expect("the CTE write");

        let n = metastore
            .query_row(
                QueryRowParams {
                    sql: "SELECT n FROM t WHERE id = 1",
                    params: vec![],
                },
                |row| row.get_i64(0),
            )
            .await
            .expect("read n");
        assert_eq!(
            n, 11,
            "the CTE write must run before the write that arrived after it: 1 × 10 + 1, not (1 + 1) × 10"
        );
    }

    /// A write the keyword check leaves to `SQLite`, here a `CREATE TABLE`
    /// issued through `query`, still runs on the writer connection once a read
    /// connection has prepared it, so it runs before a write queued after it.
    #[tokio::test]
    async fn test_a_write_only_sqlite_recognizes_waits_its_turn_on_the_writer() {
        let _guard = CONFIG_LOCK.lock().await;
        set_sqlite_metastore_config(SqliteMetastoreConfig::default());
        let (_dir, metastore) = temp_metastore();
        let metastore = Arc::new(metastore);

        let holder = metastore.begin_transaction().await.expect("begin");
        let create = {
            let metastore = Arc::clone(&metastore);
            tokio::spawn(async move {
                metastore
                    .query(
                        QueryParams {
                            sql: "CREATE TABLE later (id INTEGER PRIMARY KEY)",
                            params: vec![],
                        },
                        |_| Ok(()),
                    )
                    .await
            })
        };
        // Arrival order is what is under test: the CREATE TABLE must be queued
        // before the INSERT, and reaching the queue takes well under this.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let insert = {
            let metastore = Arc::clone(&metastore);
            tokio::spawn(async move {
                metastore
                    .execute(ExecuteParams {
                        sql: "INSERT INTO later (id) VALUES (1)",
                        params: vec![],
                    })
                    .await
            })
        };
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        holder.commit().await.expect("commit the holder");
        create
            .await
            .expect("CREATE TABLE task")
            .expect("the CREATE TABLE");
        insert
            .await
            .expect("INSERT task")
            .expect("the INSERT queued after the CREATE TABLE must find its table");
    }

    /// A transaction keeps its writer connection registered after the metastore
    /// that began it is dropped, so the next metastore opened on the file is
    /// handed that same connection rather than a second one.
    #[tokio::test]
    async fn test_a_transaction_keeps_its_writer_registered_past_its_metastore() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("cayenne.db");
        let first = SqliteMetastore::new(format!("sqlite://{}", path.display()));
        let writer = Arc::clone(&first.pool().await.expect("pool").writer);
        let session = writer
            .session(begin_immediate, roll_back)
            .await
            .expect("begin");
        let registered = Arc::downgrade(&writer);
        drop(writer);
        drop(first);

        let key = writer_key(path.to_str().expect("utf-8 path")).await;
        let slot = Arc::clone(WRITERS.lock().get(&key).expect("the file's writer slot"));
        let handed_out = slot.lock().await.upgrade();
        let registered = registered
            .upgrade()
            .expect("an open transaction must keep its writer connection alive");
        assert!(
            handed_out.is_some_and(|writer| Arc::ptr_eq(&writer, &registered)),
            "the file's slot must still hand out the open transaction's writer connection"
        );
        drop(session);
    }

    /// The writer connection a file's slot hands out now, if it is still open.
    async fn registered_writer(path: &std::path::Path) -> Option<Arc<Writer>> {
        let key = writer_key(path.to_str().expect("utf-8 path")).await;
        let slot = Arc::clone(WRITERS.lock().get(&key).expect("the file's writer slot"));
        slot.lock().await.upgrade()
    }

    /// A write whose caller stops waiting once it has started runs to its end,
    /// and keeps its writer connection registered until then, even past the
    /// last metastore open on the file: a metastore opened meanwhile is handed
    /// that connection rather than a second one.
    #[tokio::test]
    async fn test_a_started_write_keeps_its_writer_registered_past_its_caller() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("cayenne.db");
        let first = SqliteMetastore::new(format!("sqlite://{}", path.display()));
        let writer = Arc::clone(&first.pool().await.expect("pool").writer);
        let registered = Arc::downgrade(&writer);
        let (running_tx, running) = tokio::sync::oneshot::channel();
        let (release, released) = std::sync::mpsc::channel::<()>();
        let write = tokio::spawn(async move {
            writer
                .run(move |conn| {
                    let _ = running_tx.send(());
                    // Still running once its caller and the metastore are gone.
                    let _ = released.recv();
                    conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY)")
                })
                .await
        });
        running.await.expect("the write starts");
        write.abort();
        assert!(
            write.await.is_err_and(|e| e.is_cancelled()),
            "the write's caller must have stopped waiting"
        );
        drop(first);

        let handed_out = registered_writer(&path).await;
        let still_registered = registered.upgrade();
        release.send(()).expect("the write is still running");
        let still_registered =
            still_registered.expect("a started write must keep its writer connection alive");
        assert!(
            handed_out.is_some_and(|writer| Arc::ptr_eq(&writer, &still_registered)),
            "the file's slot must still hand out the running write's writer connection"
        );
    }

    /// A transaction dropped while one of its statements runs is rolled back on
    /// the writer connection once the statement ends, and keeps the connection
    /// registered until that rollback has run, even past the last metastore
    /// open on the file.
    #[tokio::test]
    async fn test_a_dropped_transaction_keeps_its_writer_registered_until_its_rollback() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("cayenne.db");
        let first = SqliteMetastore::new(format!("sqlite://{}", path.display()));
        let writer = Arc::clone(&first.pool().await.expect("pool").writer);
        let registered = Arc::downgrade(&writer);
        let session = writer
            .session(begin_immediate, roll_back)
            .await
            .expect("begin");
        drop(writer);
        let (running_tx, running) = tokio::sync::oneshot::channel();
        let (release, released) = std::sync::mpsc::channel::<()>();
        let statement = tokio::spawn(async move {
            session
                .call(move |_conn| {
                    let _ = running_tx.send(());
                    // Still running once the transaction and the metastore are
                    // gone, so the rollback is still to come.
                    let _ = released.recv();
                    Ok::<_, rusqlite::Error>(())
                })
                .await
        });
        running.await.expect("the statement starts");
        statement.abort();
        assert!(
            statement.await.is_err_and(|e| e.is_cancelled()),
            "the transaction must have been dropped"
        );
        drop(first);

        let handed_out = registered_writer(&path).await;
        let still_registered = registered.upgrade();
        release.send(()).expect("the statement is still running");
        let still_registered = still_registered.expect(
            "a dropped transaction must keep its writer connection alive until its rollback",
        );
        assert!(
            handed_out.is_some_and(|writer| Arc::ptr_eq(&writer, &still_registered)),
            "the file's slot must still hand out the dropped transaction's writer connection"
        );
    }

    /// A symlink to a metastore file shares its target's writer connection, so
    /// writes through either path are ordered together.
    #[cfg(unix)]
    #[tokio::test]
    async fn test_a_symlink_to_a_metastore_file_shares_its_writer_connection() {
        let dir = tempfile::tempdir().expect("tempdir");
        let target_path = dir.path().join("cayenne.db");
        let target = SqliteMetastore::new(format!("sqlite://{}", target_path.display()));
        let target_writer = Arc::clone(&target.pool().await.expect("pool").writer);
        let alias_path = dir.path().join("alias.db");
        std::os::unix::fs::symlink(&target_path, &alias_path).expect("symlink the metastore file");
        let alias = SqliteMetastore::new(format!("sqlite://{}", alias_path.display()));
        let alias_writer = Arc::clone(&alias.pool().await.expect("pool").writer);
        assert!(
            Arc::ptr_eq(&target_writer, &alias_writer),
            "a symlink to a metastore file must share its target's writer connection"
        );
    }

    /// Two files whose canonical paths differ only in bytes that are not UTF-8
    /// are two databases, so they get two writer keys. As strings the paths
    /// read alike, which would hand the second file's writes to the first
    /// file's writer connection.
    #[cfg(unix)]
    #[test]
    fn test_writer_keys_keep_paths_that_are_not_utf8_apart() {
        use std::os::unix::ffi::OsStrExt;
        let first = PathBuf::from(std::ffi::OsStr::from_bytes(b"/metastore/\xff.db"));
        let second = PathBuf::from(std::ffi::OsStr::from_bytes(b"/metastore/\xfe.db"));
        assert_eq!(
            first.to_string_lossy(),
            second.to_string_lossy(),
            "the two paths must read alike as strings for this test to mean anything"
        );
        assert_ne!(
            WriterKey::File(first),
            WriterKey::File(second),
            "two files must never share a writer key"
        );
    }

    /// A transaction dropped without commit or rollback is rolled back on the
    /// writer connection as its session ends, and the next writer proceeds.
    #[tokio::test]
    async fn test_a_dropped_transaction_releases_the_writer_connection() {
        let (_dir, metastore) = temp_metastore();
        metastore
            .execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            .await
            .expect("create table");
        let abandoned = metastore.begin_transaction().await.expect("begin");
        abandoned
            .execute(ExecuteParams {
                sql: "INSERT INTO t (id) VALUES (1)",
                params: vec![],
            })
            .await
            .expect("write inside the abandoned transaction");
        drop(abandoned);

        let next = metastore
            .begin_transaction()
            .await
            .expect("the next writer must get the lock once the dropped one rolls back");
        let rows = next
            .query_row_values(QueryRowParams {
                sql: "SELECT COUNT(*) FROM t",
                params: vec![],
            })
            .await
            .expect("count");
        assert!(
            matches!(rows.first(), Some(MetastoreValue::Integer(0))),
            "the dropped transaction's write must have been rolled back: {rows:?}"
        );
        next.rollback().await.expect("rollback");
    }

    async fn insert_id(metastore: &SqliteMetastore, id: i64) -> CatalogResult<()> {
        metastore
            .execute(ExecuteParams {
                sql: "INSERT INTO t (id) VALUES (?1)",
                params: vec![MetastoreValue::Integer(id)],
            })
            .await
    }

    async fn ids(metastore: &SqliteMetastore) -> Vec<i64> {
        metastore
            .query(
                QueryParams {
                    sql: "SELECT id FROM t ORDER BY id",
                    params: vec![],
                },
                |row| row.get_i64(0),
            )
            .await
            .expect("read ids")
    }

    /// A write whose turn does not come within the busy timeout fails with the
    /// retryable `database is locked` and never runs, even once the writer
    /// connection frees up, so a caller that retries it cannot apply it twice.
    #[tokio::test]
    async fn test_a_write_that_waits_out_its_turn_never_runs() {
        let _guard = CONFIG_LOCK.lock().await;
        set_sqlite_metastore_config(SqliteMetastoreConfig {
            busy_timeout_ms: 200,
            ..SqliteMetastoreConfig::default()
        });
        let (_dir, metastore) = temp_metastore();
        metastore
            .execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            .await
            .expect("create table");

        let holder = metastore.begin_transaction().await.expect("begin");
        let error = insert_id(&metastore, 1)
            .await
            .expect_err("the write must not get its turn while a transaction holds the writer");
        assert!(
            crate::cayenne_catalog::is_retryable_write_conflict(&error),
            "a write that waited out its turn must fail retryably: {error}"
        );
        holder.rollback().await.expect("rollback the holder");
        // Queued after the timed-out write, so once it has run the writer
        // connection has reached, and skipped, the timed-out one.
        insert_id(&metastore, 2)
            .await
            .expect("a write after the holder ends");
        set_sqlite_metastore_config(SqliteMetastoreConfig::default());

        assert_eq!(
            ids(&metastore).await,
            vec![2],
            "the write that waited out its turn must never have run"
        );
    }

    /// A write whose caller stops waiting before its turn, its future dropped,
    /// never runs.
    #[tokio::test]
    async fn test_a_cancelled_write_never_runs() {
        let _guard = CONFIG_LOCK.lock().await;
        set_sqlite_metastore_config(SqliteMetastoreConfig::default());
        let (_dir, metastore) = temp_metastore();
        metastore
            .execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            .await
            .expect("create table");

        let holder = metastore.begin_transaction().await.expect("begin");
        let cancelled = tokio::time::timeout(
            std::time::Duration::from_millis(50),
            insert_id(&metastore, 1),
        )
        .await;
        assert!(
            cancelled.is_err(),
            "the write must still have been waiting for its turn when its caller left"
        );
        holder.rollback().await.expect("rollback the holder");
        insert_id(&metastore, 2)
            .await
            .expect("a write after the holder ends");

        assert_eq!(
            ids(&metastore).await,
            vec![2],
            "a write whose caller went away before its turn must never run"
        );
    }

    /// A transaction whose commit is cancelled while the COMMIT is still queued
    /// on the writer connection is rolled back, not committed behind its
    /// caller's back.
    #[tokio::test]
    async fn test_a_commit_cancelled_before_it_runs_rolls_back() {
        // A statement the writer is still running when the COMMIT is queued
        // behind it.
        const SLOW_COUNT: &str = "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 2000000) SELECT count(*) FROM c";
        let _guard = CONFIG_LOCK.lock().await;
        set_sqlite_metastore_config(SqliteMetastoreConfig::default());
        let (_dir, metastore) = temp_metastore();
        metastore
            .execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            .await
            .expect("create table");

        let pool = metastore.pool().await.expect("pool");
        let session = pool
            .writer
            .session(begin_immediate, roll_back)
            .await
            .expect("begin");
        session
            .call(|conn| {
                conn.execute("INSERT INTO t (id) VALUES (1)", [])
                    .map(|_| ())
            })
            .await
            .expect("write inside the transaction");
        let (slow_started, slow_running) = tokio::sync::oneshot::channel();
        session
            .jobs
            .send(Box::new(move |conn: &mut rusqlite::Connection| {
                let _ = slow_started.send(());
                let _ = conn.query_row(SLOW_COUNT, [], |row| row.get::<_, i64>(0));
                SessionStep::Continue
            }))
            .expect("queue a slow statement");
        slow_running.await.expect("the slow statement started");

        let cancelled = tokio::time::timeout(
            std::time::Duration::from_millis(20),
            session.finish(|conn| conn.execute_batch("COMMIT")),
        )
        .await;
        assert!(
            cancelled.is_err(),
            "the COMMIT must still have been queued when its caller left"
        );
        insert_id(&metastore, 2)
            .await
            .expect("a write after the transaction ends");

        assert_eq!(
            ids(&metastore).await,
            vec![2],
            "a transaction whose commit was cancelled before it ran must roll back"
        );
    }

    /// Opening one file's writer connection never waits for another file's:
    /// a file whose open is stuck behind another process's lock holds up only
    /// metastores on that same file.
    #[tokio::test]
    async fn test_a_stuck_writer_open_holds_up_only_its_own_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let stuck = dir.path().join("stuck.db");
        let key = writer_key(stuck.to_str().expect("utf-8 path")).await;
        // Hold the stuck file's slot, as an open waiting out a lock would.
        let slot = Arc::clone(WRITERS.lock().entry(key).or_default());
        let _opening = slot.lock().await;

        let other = SqliteMetastore::new(format!(
            "sqlite://{}",
            dir.path().join("other.db").display()
        ));
        tokio::time::timeout(std::time::Duration::from_secs(5), other.pool())
            .await
            .expect("another file's writer connection must not wait for a stuck open")
            .expect("pool");
    }

    /// A TRUNCATE checkpoint waits only briefly for a reader still on an older
    /// snapshot. It holds the write lock while it waits, so the write queued
    /// behind it would otherwise wait out the whole read, for up to the busy
    /// timeout.
    #[tokio::test]
    async fn test_a_truncate_waiting_on_a_reader_holds_writes_only_briefly() {
        // How long the reader holds its snapshot: the duration the write behind
        // the TRUNCATE must not have to wait out.
        const READ_HOLD: std::time::Duration = std::time::Duration::from_secs(2);
        let _guard = CONFIG_LOCK.lock().await;
        set_sqlite_metastore_config(SqliteMetastoreConfig::default());
        let (_dir, metastore) = temp_metastore();
        metastore
            .execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            .await
            .expect("create table");
        insert_id(&metastore, 1).await.expect("seed row");

        let path = metastore.db_path().to_string();
        let (snapshot_taken, reader_ready) = tokio::sync::oneshot::channel();
        let reader = std::thread::spawn(move || {
            let conn = rusqlite::Connection::open(path).expect("open the reader");
            conn.execute_batch("BEGIN").expect("begin the read");
            conn.query_row("SELECT COUNT(*) FROM t", [], |row| row.get::<_, i64>(0))
                .expect("read");
            snapshot_taken.send(()).expect("signal the snapshot");
            // Time is what is under test: the reader keeps its snapshot open.
            std::thread::sleep(READ_HOLD);
            conn.execute_batch("COMMIT").expect("end the read");
        });
        reader_ready.await.expect("the reader took its snapshot");
        insert_id(&metastore, 2)
            .await
            .expect("a write past the reader's snapshot");

        // Polled in order, so the TRUNCATE is queued on the writer before the
        // write.
        let pool = metastore.pool().await.expect("pool");
        let started = std::time::Instant::now();
        let (truncate, write) = tokio::join!(pool.writer.try_run(truncate_wal), async {
            insert_id(&metastore, 3).await.map(|()| started.elapsed())
        });
        tokio::task::spawn_blocking(move || reader.join())
            .await
            .expect("join the reader")
            .expect("reader thread");

        assert!(
            truncate.expect("truncate checkpoint").is_some(),
            "the TRUNCATE must have had its turn"
        );
        let waited = write.expect("the write behind the TRUNCATE");
        assert!(
            waited < READ_HOLD / 2,
            "the write behind a TRUNCATE waited {waited:?}, as long as the reader held its snapshot"
        );
    }

    /// Writes waiting for their turn hold no pooled connection, so reads run
    /// while more writes are queued than the pool has connections.
    #[tokio::test]
    async fn test_queued_writes_leave_the_pool_to_reads() {
        let _guard = CONFIG_LOCK.lock().await;
        set_sqlite_metastore_config(SqliteMetastoreConfig::default());
        let (_dir, metastore) = temp_metastore();
        let metastore = Arc::new(metastore);
        metastore
            .execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            .await
            .expect("create table");
        let queued_writes = 2 * metastore.pool().await.expect("pool").conns.len();

        let holder = metastore.begin_transaction().await.expect("begin");
        let writes: Vec<_> = (0..queued_writes)
            .map(|id| {
                let metastore = Arc::clone(&metastore);
                let id = i64::try_from(id).expect("write id fits in i64");
                tokio::spawn(async move { insert_id(&metastore, id).await })
            })
            .collect();
        // On this current-thread runtime a yield lets every spawned write run up
        // to its wait for a turn before the reads below start.
        tokio::task::yield_now().await;
        for _ in 0..queued_writes {
            let read = tokio::time::timeout(std::time::Duration::from_secs(5), ids(&metastore))
                .await
                .expect("a read must not wait behind queued writes");
            assert!(
                read.is_empty(),
                "no queued write may have run yet: {read:?}"
            );
        }
        assert!(
            writes.iter().all(|write| !write.is_finished()),
            "every write must still be queued behind the transaction"
        );

        holder.rollback().await.expect("rollback the holder");
        for write in writes {
            write
                .await
                .expect("write task")
                .expect("a queued write must run once the transaction ends");
        }
        assert_eq!(ids(&metastore).await.len(), queued_writes);
    }

    /// Cayenne memory mode: an in-memory (memdb) metastore must be usable and,
    /// crucially, SHARED across every pooled connection — a table created via one
    /// pooled connection must be visible when a read round-robins to another.
    /// This is the core memdb-sharing invariant; a private `:memory:` per
    /// connection would fail the cross-connection read.
    #[tokio::test]
    async fn test_in_memory_metastore_shared_across_pool() {
        let _guard = CONFIG_LOCK.lock().await;
        set_sqlite_metastore_config(SqliteMetastoreConfig::default());

        let metastore = in_memory_metastore("cayenne-mem-shared-pool");
        assert!(is_memory_db_path(metastore.db_path()));

        // Create the table via a pooled connection.
        metastore
            .execute_batch("CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY)")
            .await
            .expect("create table on a pooled connection");

        // Every pooled connection must report the MEMORY journal (WAL is
        // unsupported in-memory) — read it off a real pooled connection.
        let pool = metastore.pool().await.expect("pool");
        let journal_mode: String = pool.conns[0]
            .lock()
            .await
            .call(|conn| conn.query_row("PRAGMA journal_mode", [], |row| row.get(0)))
            .await
            .expect("read journal_mode");
        assert_eq!(
            journal_mode.to_lowercase(),
            "memory",
            "in-memory metastore must use the MEMORY journal, got {journal_mode}"
        );

        // Insert enough rows that the round-robin pool spreads writes across
        // multiple connections, then read the full count back — proving all
        // connections share ONE database (memdb), not a private DB each.
        for i in 0..64i64 {
            metastore
                .execute(ExecuteParams {
                    sql: "INSERT INTO t (id) VALUES (?1)",
                    params: vec![MetastoreValue::Integer(i)],
                })
                .await
                .expect("insert row");
        }
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
        assert_eq!(
            count, 64,
            "all inserts must be visible across the shared in-memory pool"
        );
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
    /// The whole point of the driver: under INCREMENTAL the freelist is actually
    /// returned to the filesystem, and the main DB file shrinks.
    #[tokio::test]
    async fn test_incremental_vacuum_reclaims_freelist_and_shrinks_the_file() {
        let _guard = CONFIG_LOCK.lock().await;
        set_sqlite_metastore_config(SqliteMetastoreConfig {
            // `auto_vacuum` is deliberately left at its default, so this doubles
            // as the proof that a metastore configured by nobody reclaims.
            // TRUNCATE every checkpoint so the file size is deterministic here.
            wal_truncate_threshold_bytes: 0,
            // Big enough to drain this freelist in one pass.
            incremental_vacuum_pages: 100_000,
            ..SqliteMetastoreConfig::default()
        });

        let (_dir, metastore) = temp_metastore();
        metastore.init_schema().await.expect("init schema");
        grow_wal(&metastore, 64, 64).await;
        metastore.checkpoint_wal().await.expect("checkpoint");

        let size_before = std::fs::metadata(metastore.db_path())
            .expect("db file")
            .len();

        // Free a large run of pages, then drain the WAL so the deletion itself is
        // resident in the main DB and its pages are on the freelist.
        metastore
            .execute_batch("DELETE FROM t")
            .await
            .expect("delete rows");
        metastore.checkpoint_wal().await.expect("checkpoint");

        let reclaimed = metastore.incremental_vacuum().await.expect("vacuum");
        assert!(
            reclaimed > 0,
            "INCREMENTAL mode must reclaim the freed pages; reclaimed={reclaimed}"
        );
        // The relocation lands in the WAL — the file only shrinks once a
        // checkpoint copies it back, which is why the caller vacuums BEFORE the
        // checkpoint in a maintenance pass.
        metastore.checkpoint_wal().await.expect("checkpoint");

        let size_after = std::fs::metadata(metastore.db_path())
            .expect("db file")
            .len();
        assert!(
            size_after < size_before,
            "vacuum + checkpoint must shrink the DB file: before={size_before} after={size_after}"
        );
    }

    /// Freed metastore pages are returned to the filesystem out of the box. The
    /// reclaim itself is proved above; this pins the default that reaches it, so
    /// flipping it back cannot pass unnoticed.
    #[test]
    fn test_auto_vacuum_defaults_to_incremental() {
        assert_eq!(
            SqliteMetastoreConfig::default().auto_vacuum,
            SqliteAutoVacuum::Incremental
        );
    }

    /// `None` opts back out: freed pages are reused and the file plateaus, which
    /// costs the write path nothing — not even the pointer map.
    #[tokio::test]
    async fn test_incremental_vacuum_is_a_noop_when_auto_vacuum_is_none() {
        let _guard = CONFIG_LOCK.lock().await;
        set_sqlite_metastore_config(SqliteMetastoreConfig {
            auto_vacuum: SqliteAutoVacuum::None,
            ..SqliteMetastoreConfig::default()
        });

        let (_dir, metastore) = temp_metastore();
        metastore.init_schema().await.expect("init schema");
        grow_wal(&metastore, 16, 64).await;
        metastore.checkpoint_wal().await.expect("checkpoint");
        metastore
            .execute_batch("DELETE FROM t")
            .await
            .expect("delete rows");
        metastore.checkpoint_wal().await.expect("checkpoint");

        assert_eq!(
            metastore.incremental_vacuum().await.expect("vacuum"),
            0,
            "auto_vacuum NONE keeps freed pages on the freelist for reuse"
        );
    }

    /// A database created in NONE mode does not become reclaimable just because
    /// the config later says INCREMENTAL — `auto_vacuum` only takes effect on a
    /// fresh file. This is the shape every metastore created before INCREMENTAL
    /// became the default has, so the driver gates on the DB's real mode rather
    /// than take the write lock every tick forever on such a database.
    #[tokio::test]
    async fn test_incremental_vacuum_gates_on_the_databases_actual_mode() {
        let (_dir, metastore) = {
            let _guard = CONFIG_LOCK.lock().await;
            set_sqlite_metastore_config(SqliteMetastoreConfig {
                auto_vacuum: SqliteAutoVacuum::None,
                ..SqliteMetastoreConfig::default()
            });
            let (dir, metastore) = temp_metastore();
            metastore.init_schema().await.expect("init schema");
            grow_wal(&metastore, 16, 64).await;
            metastore
                .execute_batch("DELETE FROM t")
                .await
                .expect("delete rows");
            metastore.checkpoint_wal().await.expect("checkpoint");
            (dir, metastore)
        };

        let _guard = CONFIG_LOCK.lock().await;
        set_sqlite_metastore_config(SqliteMetastoreConfig {
            auto_vacuum: SqliteAutoVacuum::Incremental,
            ..SqliteMetastoreConfig::default()
        });
        assert_eq!(
            metastore.incremental_vacuum().await.expect("vacuum"),
            0,
            "the file was created NONE; flipping the config must not pretend otherwise"
        );
    }

    /// `0` turns reclamation off without changing the database's mode.
    #[tokio::test]
    async fn test_incremental_vacuum_pages_zero_disables_the_driver() {
        let _guard = CONFIG_LOCK.lock().await;
        set_sqlite_metastore_config(SqliteMetastoreConfig {
            auto_vacuum: SqliteAutoVacuum::Incremental,
            incremental_vacuum_pages: 0,
            ..SqliteMetastoreConfig::default()
        });

        let (_dir, metastore) = temp_metastore();
        metastore.init_schema().await.expect("init schema");
        grow_wal(&metastore, 16, 64).await;
        metastore
            .execute_batch("DELETE FROM t")
            .await
            .expect("delete rows");
        metastore.checkpoint_wal().await.expect("checkpoint");

        assert_eq!(
            metastore.incremental_vacuum().await.expect("vacuum"),
            0,
            "incremental_vacuum_pages = 0 must reclaim nothing"
        );
    }

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
    /// 160 MiB cap, `DEFAULT_WAL_TRUNCATE_THRESHOLD_BYTES`) drains the accumulated
    /// frames into the main DB without requiring TRUNCATE. After it runs, an
    /// independent TRUNCATE finds nothing left to copy and reclaims the file —
    /// proving PASSIVE fully checkpointed.
    #[tokio::test]
    async fn test_background_passive_checkpoint_drains_into_main_db() {
        let _guard = CONFIG_LOCK.lock().await;
        set_sqlite_metastore_config(SqliteMetastoreConfig::default());

        let (_dir, metastore) = temp_metastore();
        metastore.init_schema().await.expect("init schema");

        grow_wal(&metastore, 32, 64).await;
        let wal_before = metastore.read_wal_bytes().await;
        assert!(wal_before > 0, "WAL should hold frames before the drain");

        // Default threshold (160 MiB) ⇒ PASSIVE (our ~2 MiB WAL is far below it).
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
    // cycle-12: `table_id` stored as the raw-UUID-bytes BLOB (no FK).
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

    /// Read the declared type of one column off a live pooled connection.
    async fn column_type(metastore: &SqliteMetastore, table: &str, column: &str) -> String {
        let table = table.to_string();
        let column = column.to_string();
        let pool = metastore.pool().await.expect("pool");
        let guard = pool.conns[0].lock().await;
        guard
            .call(move |conn| {
                conn.query_row(
                    &format!("SELECT type FROM pragma_table_info('{table}') WHERE name = ?1"),
                    [&column],
                    |row| row.get::<_, String>(0),
                )
            })
            .await
            .expect("column type")
    }

    /// A fresh `cayenne_insert_record` has exactly the 3 composite-PK columns
    /// (no `insert_record_id`), is a `WITHOUT ROWID` table, stores `table_id`
    /// as a `BLOB`, and carries no foreign key.
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

        // `table_id` is the raw-UUID-bytes BLOB (cycle-12 WAL-volume lever).
        assert_eq!(
            column_type(&metastore, "cayenne_insert_record", "table_id").await,
            "BLOB",
            "table_id must be declared BLOB (16 raw UUID bytes, not 36-char text)"
        );

        // The foreign key was dropped (a BLOB child cannot satisfy the TEXT
        // parent key under foreign_keys=ON); foreign_key_list must be empty.
        let fk_count: i64 = {
            let pool = metastore.pool().await.expect("pool");
            let g = pool.conns[0].lock().await;
            g.call(|conn| {
                conn.query_row(
                    "SELECT COUNT(*) FROM pragma_foreign_key_list('cayenne_insert_record')",
                    [],
                    |row| row.get(0),
                )
            })
            .await
            .expect("fk list")
        };
        assert_eq!(
            fk_count, 0,
            "cayenne_insert_record must carry no foreign key (BLOB table_id)"
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
    /// conflict target the catalog relies on). The `table_id` is bound as the
    /// raw-UUID-bytes BLOB, matching the production write path.
    #[tokio::test]
    async fn test_insert_record_duplicate_pk_upserts_sequence() {
        let _guard = CONFIG_LOCK.lock().await;
        set_sqlite_metastore_config(SqliteMetastoreConfig::default());
        let (_dir, metastore) = temp_metastore();
        metastore.init_schema().await.expect("init schema");

        // No FK to satisfy any more, but a realistic UUID table_id is used so
        // the raw-bytes encoding is exercised.
        let table_id = uuid::Uuid::now_v7().to_string();
        let table_id_blob = crate::metastore::table_id_to_key_bytes(&table_id);

        for seq in [11_i64, 42] {
            // Second iteration upserts the SAME (table_id, pk_bytes) → REPLACE.
            metastore
                .execute(ExecuteParams {
                    sql: "INSERT OR REPLACE INTO cayenne_insert_record (table_id, pk_bytes, sequence_number) VALUES (?1, ?2, ?3)",
                    params: vec![
                        MetastoreValue::Blob(table_id_blob.clone()),
                        MetastoreValue::Blob(b"pk-dup".to_vec()),
                        MetastoreValue::Integer(seq),
                    ],
                })
                .await
                .expect("upsert insert record");
        }

        let (count, seq): (i64, i64) = {
            let table_id_blob = table_id_blob.clone();
            let pool = metastore.pool().await.expect("pool");
            let g = pool.conns[0].lock().await;
            g.call(move |conn| {
                conn.query_row(
                    "SELECT COUNT(*), MAX(sequence_number) FROM cayenne_insert_record WHERE table_id = ?1",
                    [&table_id_blob],
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
    /// leading-prefix of the composite PK with the BLOB `table_id`. Rows seeded
    /// via a BLOB hex literal (mirroring the `commit_*_in_txn` batch path) and
    /// a second table's row stays untouched.
    #[tokio::test]
    async fn test_insert_record_checkpoint_clear_by_table_id() {
        let _guard = CONFIG_LOCK.lock().await;
        set_sqlite_metastore_config(SqliteMetastoreConfig::default());
        let (_dir, metastore) = temp_metastore();
        metastore.init_schema().await.expect("init schema");

        let table_id = uuid::Uuid::now_v7().to_string();
        let other_table_id = uuid::Uuid::now_v7().to_string();
        let table_id_blob = crate::metastore::table_id_to_key_bytes(&table_id);
        // Build the `x'..'` BLOB literal of the raw UUID bytes.
        let blob_lit = |id: &str| {
            let bytes = crate::metastore::table_id_to_key_bytes(id);
            let hex = bytes.iter().fold(String::new(), |mut acc, b| {
                let _ = write!(acc, "{b:02x}");
                acc
            });
            format!("x'{hex}'")
        };
        let tid_lit = blob_lit(&table_id);
        let other_lit = blob_lit(&other_table_id);
        metastore
            .execute_batch(&format!(
                "INSERT INTO cayenne_insert_record (table_id, pk_bytes, sequence_number) VALUES ({tid_lit}, x'01', 1); \
                 INSERT INTO cayenne_insert_record (table_id, pk_bytes, sequence_number) VALUES ({tid_lit}, x'02', 2); \
                 INSERT INTO cayenne_insert_record (table_id, pk_bytes, sequence_number) VALUES ({other_lit}, x'01', 9);"
            ))
            .await
            .expect("seed rows");

        metastore
            .execute(ExecuteParams {
                sql: "DELETE FROM cayenne_insert_record WHERE table_id = ?1",
                params: vec![MetastoreValue::Blob(table_id_blob.clone())],
            })
            .await
            .expect("checkpoint clear");

        let remaining: i64 = metastore
            .query_row(
                QueryRowParams {
                    sql: "SELECT COUNT(*) FROM cayenne_insert_record WHERE table_id = ?1",
                    params: vec![MetastoreValue::Blob(table_id_blob)],
                },
                |row| row.get_i64(0),
            )
            .await
            .expect("count after clear");
        assert_eq!(
            remaining, 0,
            "checkpoint clear must empty the table's insert records"
        );

        let other_remaining: i64 = metastore
            .query_row(
                QueryRowParams {
                    sql: "SELECT COUNT(*) FROM cayenne_insert_record WHERE table_id = ?1",
                    params: vec![MetastoreValue::Blob(
                        crate::metastore::table_id_to_key_bytes(&other_table_id),
                    )],
                },
                |row| row.get_i64(0),
            )
            .await
            .expect("count other after clear");
        assert_eq!(
            other_remaining, 1,
            "clearing one table_id must not touch another table's insert records"
        );
    }

    /// Drive the legacy→current `cayenne_insert_record` migration for a given
    /// legacy DDL (which stores `table_id` as TEXT). Asserts the migrated table
    /// is the BLOB-`table_id` WITHOUT-ROWID composite-PK shape with no FK, and
    /// that the two seeded rows survive with their `table_id` re-encoded to the
    /// raw UUID bytes (so the BLOB-keyed reader/clear find them).
    async fn assert_legacy_insert_record_migrates(legacy_create_sql: &str, with_uuid_pk: bool) {
        let _guard = CONFIG_LOCK.lock().await;
        set_sqlite_metastore_config(SqliteMetastoreConfig::default());
        let (_dir, metastore) = temp_metastore();

        // Build a realistic "old deployment": init the current full schema for
        // every OTHER table, then DOWNGRADE only cayenne_insert_record back to
        // a legacy TEXT-`table_id` shape and seed a parent row + two insert
        // records. Re-running init_schema must then detect & migrate just this
        // table (leaving every other table matching EXPECTED_TABLES).
        metastore.init_schema().await.expect("baseline init schema");
        let table_id = uuid::Uuid::now_v7().to_string();

        let insert_rows = if with_uuid_pk {
            format!(
                "INSERT INTO cayenne_insert_record (insert_record_id, table_id, pk_bytes, sequence_number) \
                    VALUES ('{u1}', '{table_id}', x'0a', 7); \
                 INSERT INTO cayenne_insert_record (insert_record_id, table_id, pk_bytes, sequence_number) \
                    VALUES ('{u2}', '{table_id}', x'0b', 9);",
                u1 = uuid::Uuid::now_v7(),
                u2 = uuid::Uuid::now_v7(),
            )
        } else {
            format!(
                "INSERT INTO cayenne_insert_record (table_id, pk_bytes, sequence_number) \
                    VALUES ('{table_id}', x'0a', 7); \
                 INSERT INTO cayenne_insert_record (table_id, pk_bytes, sequence_number) \
                    VALUES ('{table_id}', x'0b', 9);"
            )
        };

        metastore
            .execute_batch(&format!(
                "DROP TABLE cayenne_insert_record; \
                 {legacy_create_sql} \
                 INSERT INTO cayenne_table (table_id, table_name, path, path_is_relative, schema_json, primary_key_json, current_snapshot_id) \
                    VALUES ('{table_id}', 'legacy_t', '/tmp', 1, '{{}}', '[]', '{table_id}'); \
                 {insert_rows}"
            ))
            .await
            .expect("downgrade cayenne_insert_record to legacy schema + seed rows");

        // Sanity: the legacy `table_id` column is TEXT pre-migration.
        assert_eq!(
            column_type(&metastore, "cayenne_insert_record", "table_id").await,
            "TEXT",
            "precondition: legacy table_id is TEXT"
        );

        metastore.init_schema().await.expect("init schema migrates");

        // Post-migration: the new 3-column schema with a BLOB table_id.
        let after = table_columns(&metastore, "cayenne_insert_record").await;
        assert_eq!(
            after,
            vec!["table_id", "pk_bytes", "sequence_number"],
            "legacy table must be migrated to the composite-PK schema"
        );
        assert_eq!(
            column_type(&metastore, "cayenne_insert_record", "table_id").await,
            "BLOB",
            "migrated table_id must be BLOB"
        );

        // The rows survived AND are now keyed by the raw-UUID-bytes BLOB — i.e.
        // a BLOB-encoded lookup (what the production reader does) finds them.
        let rows: Vec<(Vec<u8>, i64)> = metastore
            .query(
                QueryParams {
                    sql: "SELECT pk_bytes, sequence_number FROM cayenne_insert_record WHERE table_id = ?1 ORDER BY pk_bytes",
                    params: vec![MetastoreValue::Blob(crate::metastore::table_id_to_key_bytes(&table_id))],
                },
                |row| Ok((row.get_blob(0)?, row.get_i64(1)?)),
            )
            .await
            .expect("read migrated rows");
        assert_eq!(
            rows,
            vec![(vec![0x0a_u8], 7_i64), (vec![0x0b_u8], 9_i64)],
            "the (table_id, pk_bytes, sequence_number) rows must be copied forward and re-encoded"
        );

        // Re-running init_schema is idempotent (table_id is already BLOB).
        metastore
            .init_schema()
            .await
            .expect("second init_schema is a no-op");
        assert_eq!(
            column_type(&metastore, "cayenne_insert_record", "table_id").await,
            "BLOB",
            "re-running init_schema must not re-migrate (already BLOB)"
        );
    }

    /// Pre-cycle-11 legacy shape: UUID `insert_record_id` TEXT PRIMARY KEY +
    /// redundant `UNIQUE(table_id, pk_bytes)` + TEXT `table_id` + FK.
    #[tokio::test]
    async fn test_insert_record_legacy_uuid_pk_schema_migrates_with_rows_present() {
        assert_legacy_insert_record_migrates(
            "CREATE TABLE cayenne_insert_record (\
                insert_record_id TEXT PRIMARY KEY, table_id TEXT NOT NULL, \
                pk_bytes BLOB NOT NULL, sequence_number BIGINT NOT NULL, \
                FOREIGN KEY (table_id) REFERENCES cayenne_table(table_id) ON DELETE CASCADE, \
                UNIQUE(table_id, pk_bytes));",
            true,
        )
        .await;
    }

    /// cycle-11 legacy shape: WITHOUT ROWID composite PK with a TEXT `table_id`
    /// and a `cayenne_table(table_id)` foreign key (the shape this lever
    /// replaces). The TEXT→BLOB re-encode must still fire.
    #[tokio::test]
    async fn test_insert_record_cycle11_text_schema_migrates_with_rows_present() {
        assert_legacy_insert_record_migrates(
            "CREATE TABLE cayenne_insert_record (\
                table_id TEXT NOT NULL, pk_bytes BLOB NOT NULL, sequence_number BIGINT NOT NULL, \
                FOREIGN KEY (table_id) REFERENCES cayenne_table(table_id) ON DELETE CASCADE, \
                PRIMARY KEY (table_id, pk_bytes)) WITHOUT ROWID;",
            false,
        )
        .await;
    }

    async fn read_user_version(metastore: &SqliteMetastore) -> i64 {
        metastore
            .query_row(
                QueryRowParams {
                    sql: "PRAGMA user_version",
                    params: vec![],
                },
                |row| row.get_i64(0),
            )
            .await
            .expect("read user_version")
    }

    /// A fresh catalog is stamped with the current schema version, and re-running
    /// `init_schema` on it is idempotent (no downgrade, no error).
    #[tokio::test]
    async fn test_init_schema_stamps_and_preserves_user_version() {
        let (_dir, metastore) = temp_metastore();
        metastore.init_schema().await.expect("first init schema");
        assert_eq!(
            read_user_version(&metastore).await,
            crate::metastore::CAYENNE_METASTORE_SCHEMA_VERSION,
            "fresh catalog must be stamped with the current schema version"
        );

        metastore
            .init_schema()
            .await
            .expect("re-running init schema is idempotent");
        assert_eq!(
            read_user_version(&metastore).await,
            crate::metastore::CAYENNE_METASTORE_SCHEMA_VERSION,
            "re-init must leave the stamp at the current version"
        );
    }

    /// A legacy catalog (`user_version` 0, written before this gate existed) opens
    /// cleanly and is migrated forward to the current stamp.
    #[tokio::test]
    async fn test_init_schema_upgrades_legacy_zero_version() {
        let (_dir, metastore) = temp_metastore();
        metastore.init_schema().await.expect("init schema");
        // Simulate a pre-gate catalog by resetting the stamp to 0.
        metastore
            .execute(ExecuteParams {
                sql: "PRAGMA user_version = 0",
                params: vec![],
            })
            .await
            .expect("reset user_version to legacy 0");
        assert_eq!(read_user_version(&metastore).await, 0, "precondition");

        metastore
            .init_schema()
            .await
            .expect("legacy (v0) catalog must open and migrate");
        assert_eq!(
            read_user_version(&metastore).await,
            crate::metastore::CAYENNE_METASTORE_SCHEMA_VERSION,
            "legacy catalog must be re-stamped to the current version"
        );
    }

    /// A catalog stamped by a NEWER build must be refused loudly rather than
    /// opened and read with silently-wrong (dropped-row) results (#11291).
    #[tokio::test]
    async fn test_init_schema_rejects_newer_user_version() {
        let (_dir, metastore) = temp_metastore();
        metastore.init_schema().await.expect("first init schema");
        // Simulate a catalog written by a future, incompatible build.
        let newer = crate::metastore::CAYENNE_METASTORE_SCHEMA_VERSION + 1;
        metastore
            .execute(ExecuteParams {
                sql: &format!("PRAGMA user_version = {newer}"),
                params: vec![],
            })
            .await
            .expect("bump user_version to a future version");

        let err = metastore
            .init_schema()
            .await
            .expect_err("a newer catalog must be rejected");
        match err {
            CatalogError::IncompatibleSchemaVersion { found, supported } => {
                assert_eq!(found, newer);
                assert_eq!(
                    supported,
                    crate::metastore::CAYENNE_METASTORE_SCHEMA_VERSION
                );
            }
            other => panic!("expected IncompatibleSchemaVersion, got: {other}"),
        }

        // The rejected open must NOT have mutated the stamp (no silent downgrade).
        assert_eq!(
            read_user_version(&metastore).await,
            newer,
            "a refused open must leave the newer stamp untouched"
        );
    }

    /// A `stat` that fails must leave its gauge alone, because the alternative
    /// reading — zero — says the metastore shrank to nothing. The exception is a
    /// missing `-wal`, which is a real zero: the WAL is created on the first
    /// write and removed on a clean close.
    ///
    /// The database's own absence is deliberately NOT that exception. An open
    /// `SQLite` database stays live and allocated after its pathname is unlinked,
    /// so a failed `stat` on it means "unknown", never "empty".
    #[test]
    fn a_failed_stat_is_not_a_zero_byte_metastore() {
        let dir = tempfile::tempdir().expect("temp dir");
        let db = dir.path().join("cayenne.db");
        let wal = dir.path().join("cayenne.db-wal");
        std::fs::write(&db, vec![0_u8; 4096]).expect("write the database file");
        std::fs::write(&wal, vec![0_u8; 512]).expect("write the WAL file");

        let db_path = db.to_string_lossy().to_string();
        let wal_path = wal.to_string_lossy().to_string();

        assert_eq!(
            measure_file_footprint(&db_path, &wal_path),
            (Some(4096), Some(512)),
            "both files present: both measured"
        );

        std::fs::remove_file(&wal).expect("remove the WAL file");
        assert_eq!(
            measure_file_footprint(&db_path, &wal_path),
            (Some(4096), Some(0)),
            "a checkpointed-away WAL holds zero bytes, which is a measurement"
        );

        std::fs::remove_file(&db).expect("unlink the database file");
        assert_eq!(
            measure_file_footprint(&db_path, &wal_path),
            (None, Some(0)),
            "an unlinked database is unknown, not empty: an open handle keeps its pages allocated"
        );
    }
}
