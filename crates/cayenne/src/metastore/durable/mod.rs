/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! A catalog whose authoritative copy is in object storage (`mode: datalake`).
//!
//! The local `SQLite` file is a replica: every catalog transaction is
//! captured as a row-level change set ([`changes`]), made durable in object
//! storage ([`log`]), and only then committed locally. A transaction whose
//! change set object storage did not accept rolls back, so the local file
//! never holds a state object storage cannot reproduce, and every write the
//! catalog acknowledges survives the loss of the local disk. On open,
//! [`recover`] brings the local file to the latest durable state: it keeps a
//! file that is already a prefix of it, and otherwise downloads the newest
//! image, then applies the change sets that follow.

pub mod changes;
pub mod log;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;
use snafu::{ResultExt, Snafu};
use uuid::Uuid;

pub use log::CatalogLocation;

pub(crate) use changes::POSITION_TABLE;
use changes::{Capture, ChangeSet, Layout};
use log::DurableLog;

/// Why a durable catalog could not be read, written or recovered.
#[derive(Debug, Snafu)]
#[expect(missing_docs, reason = "each variant's display message documents it")]
pub enum Error {
    #[snafu(transparent)]
    Log {
        #[snafu(source(from(log::Error, Box::new)))]
        source: Box<log::Error>,
    },

    #[snafu(transparent)]
    Changes {
        #[snafu(source(from(changes::Error, Box::new)))]
        source: Box<changes::Error>,
    },

    #[snafu(display("Failed to update the local catalog replica '{}': {source}", path.display()))]
    Local {
        path: PathBuf,
        source: rusqlite::Error,
    },

    #[snafu(display("Failed to write the local catalog replica '{}': {source}", path.display()))]
    LocalIo {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Failed to run a local catalog task: {source}"))]
    Join { source: tokio::task::JoinError },

    #[snafu(display(
        "Another Spice instance created the catalog in {location} while this one was creating it. Only one instance may write a `mode: datalake` table; restart this instance once the other has stopped."
    ))]
    CreatedElsewhere { location: String },

    #[snafu(display(
        "A catalog change was made durable in {location} but could not be committed to the local replica, so this process stopped writing to the table. Restart Spice to recover it. Cause: {cause}"
    ))]
    ReplicaBehind { location: String, cause: String },
}

/// The result of a durable catalog operation.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// The durable position a local replica holds, from its position table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Position {
    pub catalog_id: Uuid,
    pub sequence: u64,
}

fn local(path: &Path) -> impl FnOnce(rusqlite::Error) -> Error + '_ {
    move |source| Error::Local {
        path: path.to_path_buf(),
        source,
    }
}

fn sidecars(db: &Path) -> [PathBuf; 2] {
    ["-wal", "-shm"].map(|suffix| {
        let mut name = db.as_os_str().to_owned();
        name.push(suffix);
        PathBuf::from(name)
    })
}

/// Create the position table if needed and record `position` in it.
pub(crate) fn write_position(
    conn: &rusqlite::Connection,
    position: Position,
) -> rusqlite::Result<()> {
    conn.execute_batch(&format!(
        "CREATE TABLE IF NOT EXISTS {POSITION_TABLE} (id INTEGER PRIMARY KEY CHECK (id = 1), catalog_id TEXT NOT NULL, sequence INTEGER NOT NULL)"
    ))?;
    conn.execute(
        &format!(
            "INSERT OR REPLACE INTO {POSITION_TABLE} (id, catalog_id, sequence) VALUES (1, ?1, ?2)"
        ),
        rusqlite::params![
            position.catalog_id.to_string(),
            i64::try_from(position.sequence).unwrap_or(i64::MAX)
        ],
    )?;
    Ok(())
}

/// The statement that records `sequence` as the position, inside the
/// transaction that brings the replica to it.
pub(crate) fn set_sequence(conn: &rusqlite::Connection, sequence: u64) -> rusqlite::Result<()> {
    let sequence = i64::try_from(sequence)
        .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(1, i64::MAX))?;
    let changed = conn.execute(
        &format!("UPDATE {POSITION_TABLE} SET sequence = ?1 WHERE id = 1"),
        [sequence],
    )?;
    if changed == 1 {
        Ok(())
    } else {
        Err(rusqlite::Error::QueryReturnedNoRows)
    }
}

/// The position recorded in the catalog file at `db`, if it is a replica.
pub(crate) fn read_position(db: &Path) -> Result<Option<Position>> {
    if !db.exists() {
        return Ok(None);
    }
    let conn = rusqlite::Connection::open(db).map_err(local(db))?;
    read_position_from(&conn).map_err(local(db))
}

fn read_position_from(conn: &rusqlite::Connection) -> rusqlite::Result<Option<Position>> {
    let exists: bool = conn.query_row(
        "SELECT EXISTS (SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = ?1)",
        [POSITION_TABLE],
        |row| row.get(0),
    )?;
    if !exists {
        return Ok(None);
    }
    let row: Option<(String, i64)> = conn
        .query_row(
            &format!("SELECT catalog_id, sequence FROM {POSITION_TABLE} WHERE id = 1"),
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map(Some)
        .or_else(|e| match e {
            rusqlite::Error::QueryReturnedNoRows => Ok(None),
            e => Err(e),
        })?;
    Ok(row.and_then(|(id, sequence)| {
        Some(Position {
            catalog_id: Uuid::parse_str(&id).ok()?,
            sequence: u64::try_from(sequence).ok()?,
        })
    }))
}

/// Remove the catalog file at `db` and its WAL sidecars.
pub(crate) fn remove_replica(db: &Path) -> Result<()> {
    for path in std::iter::once(db.to_path_buf()).chain(sidecars(db)) {
        match std::fs::remove_file(&path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(source) => return Err(Error::LocalIo { path, source }),
        }
    }
    Ok(())
}

/// Replace the catalog file at `db` with `image`, durably.
fn install_image(db: &Path, image: &[u8]) -> Result<()> {
    let io = |source| Error::LocalIo {
        path: db.to_path_buf(),
        source,
    };
    if let Some(dir) = db.parent() {
        std::fs::create_dir_all(dir).map_err(io)?;
    }
    let mut staged = db.as_os_str().to_owned();
    staged.push(".download");
    let staged = PathBuf::from(staged);
    {
        use std::io::Write;
        let mut file = std::fs::File::create(&staged).map_err(io)?;
        file.write_all(image).map_err(io)?;
        file.sync_all().map_err(io)?;
    }
    remove_replica(db)?;
    std::fs::rename(&staged, db).map_err(io)?;
    if let Some(dir) = db.parent() {
        std::fs::File::open(dir)
            .and_then(|d| d.sync_all())
            .map_err(io)?;
    }
    Ok(())
}

/// Apply change sets in order to the catalog file at `db`, each in its own
/// transaction that also advances the recorded position.
fn apply_logs(db: &Path, logs: &[(u64, Bytes)]) -> Result<()> {
    if logs.is_empty() {
        return Ok(());
    }
    let conn = rusqlite::Connection::open(db).map_err(local(db))?;
    // A change set already holds every row a cascade changed.
    conn.pragma_update(None, "foreign_keys", "OFF")
        .map_err(local(db))?;
    let layout = Layout::load(&conn)?;
    for (sequence, bytes) in logs {
        let changes = ChangeSet::decode(bytes)?;
        conn.execute_batch("BEGIN IMMEDIATE").map_err(local(db))?;
        let applied = changes::apply(&conn, &layout, &changes).and_then(|()| {
            set_sequence(&conn, *sequence).map_err(|source| changes::Error::Replay {
                table: POSITION_TABLE.to_string(),
                source,
            })
        });
        match applied {
            Ok(()) => conn.execute_batch("COMMIT").map_err(local(db))?,
            Err(error) => {
                let _ = conn.execute_batch("ROLLBACK");
                return Err(error.into());
            }
        }
    }
    Ok(())
}

/// The result of bringing a local replica to the latest durable state.
pub(crate) enum Recovered {
    /// The location holds no catalog yet; the local file was removed.
    Empty(CatalogLocation),
    /// The local file now holds the latest durable state.
    Current(Arc<DurableLog>),
}

/// Bring the catalog file at `db` to the latest state durable at `location`.
///
/// # Errors
///
/// Returns an error when object storage cannot be read, or holds a catalog
/// this build cannot read, or the local file cannot be written.
pub(crate) async fn recover(location: CatalogLocation, db: PathBuf) -> Result<Recovered> {
    let Some(log) = DurableLog::open(location.clone()).await? else {
        let path = db.clone();
        tokio::task::spawn_blocking(move || remove_replica(&path))
            .await
            .context(JoinSnafu)??;
        return Ok(Recovered::Empty(location));
    };
    let path = db.clone();
    let position = tokio::task::spawn_blocking(move || read_position(&path))
        .await
        .context(JoinSnafu)??;
    let local = position
        .filter(|p| p.catalog_id == log.catalog_id())
        .map(|p| p.sequence);
    let recovery = log.recovery(local).await?;
    let image = match &recovery.image {
        Some((image, _)) => Some(log.location().get_immutable(image).await?),
        None => None,
    };
    let mut logs = Vec::with_capacity(recovery.logs.len());
    for pending in &recovery.logs {
        logs.push((pending.sequence, log.fetch(pending).await?));
    }
    let restored = recovery.image.as_ref().map(|(_, sequence)| *sequence);
    let applied = logs.len();
    let path = db.clone();
    tokio::task::spawn_blocking(move || {
        if let Some(image) = image {
            install_image(&path, &image)?;
        }
        apply_logs(&path, &logs)
    })
    .await
    .context(JoinSnafu)??;
    if restored.is_some() || applied > 0 {
        tracing::info!(
            "Restored the catalog from {} into '{}' ({}{applied} later changes applied).",
            location.url(),
            db.display(),
            restored.map_or_else(String::new, |s| format!("image at version {s}, ")),
        );
    }
    Ok(Recovered::Current(Arc::new(log)))
}

/// A consistent image of the catalog file at `db` and the position it holds,
/// written by `VACUUM INTO` on `conn`, a connection to that file.
pub(crate) fn image(conn: &rusqlite::Connection, db: &Path) -> Result<(Bytes, Position)> {
    let mut target = db.as_os_str().to_owned();
    target.push(format!(".image-{}", Uuid::now_v7()));
    let target = PathBuf::from(target);
    let result = (|| {
        conn.execute("VACUUM INTO ?1", [target.to_string_lossy().as_ref()])
            .map_err(local(db))?;
        let copy = rusqlite::Connection::open(&target).map_err(local(&target))?;
        let position = read_position_from(&copy)
            .map_err(local(&target))?
            .ok_or_else(|| Error::Local {
                path: target.clone(),
                source: rusqlite::Error::QueryReturnedNoRows,
            })?;
        drop(copy);
        let bytes = std::fs::read(&target).map_err(|source| Error::LocalIo {
            path: target.clone(),
            source,
        })?;
        Ok((Bytes::from(bytes), position))
    })();
    let _ = std::fs::remove_file(&target);
    result
}

/// Create the catalog at `location` from the local file `conn` opens, which
/// becomes its state at sequence 0.
///
/// # Errors
///
/// Returns [`Error::CreatedElsewhere`] when another process created it first.
pub(crate) async fn create(
    location: CatalogLocation,
    image: Bytes,
    catalog_id: Uuid,
) -> Result<Arc<DurableLog>> {
    let url = location.url().to_string();
    DurableLog::create(location, image, catalog_id)
        .await?
        .map(Arc::new)
        .ok_or(Error::CreatedElsewhere { location: url })
}

/// Durability attached to a metastore's writer connection.
pub(crate) struct Durability {
    log: Arc<DurableLog>,
    capture: Arc<Capture>,
    rotating: AtomicBool,
    /// Set when the local replica fell behind object storage; no later write
    /// may commit, since it would be computed from a stale state.
    halted: parking_lot::Mutex<Option<String>>,
}

impl std::fmt::Debug for Durability {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Durability")
            .field("log", &self.log)
            .finish_non_exhaustive()
    }
}

impl Durability {
    /// Install change capture on `conn`, the writer connection.
    pub(crate) fn install(conn: &rusqlite::Connection, log: Arc<DurableLog>) -> Result<Self> {
        let layout = Layout::load(conn)?;
        Ok(Self {
            log,
            capture: Capture::install(conn, layout)?,
            rotating: AtomicBool::new(false),
            halted: parking_lot::Mutex::new(None),
        })
    }

    pub(crate) fn log(&self) -> &Arc<DurableLog> {
        &self.log
    }

    /// Whether every catalog table on `conn`, the writer connection, still has
    /// its changes recorded.
    pub(crate) fn covers(&self, conn: &rusqlite::Connection) -> rusqlite::Result<bool> {
        self.capture.covers(conn)
    }

    /// Start recording a new transaction's changes.
    pub(crate) fn begin(&self) {
        self.capture.clear();
    }

    /// Make the recorded transaction durable, returning the sequence the local
    /// commit must record, or `None` when it changed nothing.
    ///
    /// # Errors
    ///
    /// Returns an error when the changes could not be recorded or made durable;
    /// the local transaction must then roll back.
    pub(crate) async fn publish(&self, schema_covered: bool) -> Result<Option<u64>> {
        if !schema_covered {
            self.capture.clear();
            return Err(changes::Error::SchemaChanged.into());
        }
        if let Some(cause) = self.halted.lock().clone() {
            return ReplicaBehindSnafu {
                location: self.log.location().url(),
                cause,
            }
            .fail();
        }
        let changes = self.capture.take()?;
        if changes.is_empty() {
            return Ok(None);
        }
        let count = changes.len();
        let bytes = changes.encode()?;
        let sequence = self.log.publish(bytes).await?;
        tracing::trace!(sequence, count, "Committed a catalog change set");
        Ok(Some(sequence))
    }

    /// Record that a durable change could not be committed locally.
    pub(crate) fn halt(&self, cause: impl std::fmt::Display) -> Error {
        let cause = cause.to_string();
        *self.halted.lock() = Some(cause.clone());
        let error = Error::ReplicaBehind {
            location: self.log.location().url().to_string(),
            cause,
        };
        tracing::error!("{error}");
        error
    }

    /// Claim the right to run a rotation, when one is due and none runs.
    pub(crate) async fn claim_rotation(&self) -> bool {
        self.log.rotation_due().await && !self.rotating.swap(true, Ordering::AcqRel)
    }

    /// Release a claim taken by [`Self::claim_rotation`].
    pub(crate) fn release_rotation(&self) {
        self.rotating.store(false, Ordering::Release);
    }

    /// Wait, up to `limit`, for a rotation in progress to finish.
    pub(crate) async fn wait_for_rotation(&self, limit: std::time::Duration) {
        let deadline = tokio::time::Instant::now() + limit;
        while self.rotating.load(Ordering::Acquire) && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }
}
