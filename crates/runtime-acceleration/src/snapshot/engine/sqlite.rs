/*
Copyright 2026 The Spice.ai OSS Authors
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

//! `SQLite`-specific snapshot engine implementation.
//!
//! `SQLite` accelerator databases run in WAL (write-ahead log) journal mode.
//! In WAL mode, writes are buffered into a `<db>-wal` sidecar file and only
//! periodically checkpointed back into the main `.sqlite` file. A naive
//! `fs::copy` of just the main file therefore captures only the durable
//! pages (often just the 4 KB header on a freshly-written DB) and loses
//! every uncheckpointed write.
//!
//! The `SqliteSnapshotEngine` addresses this by:
//!   1. **`checkpoint_live`** — under the accelerator write lock, opens a
//!      short-lived rusqlite connection to the live DB and runs a full
//!      `wal_checkpoint(TRUNCATE)`. This is safe under WAL mode (multiple
//!      connections are supported) and the existing accelerator write lock
//!      held by the caller guarantees no other writers race us.
//!   2. **`prepare_for_upload`** — defensively switches the *copied* file
//!      to `journal_mode=DELETE` so the uploaded snapshot has no `-wal`
//!      sidecar at all and is fully self-contained.
//!   3. **`prepare_file_restore`** — right before a download is renamed over
//!      the live file, moves the live database's `-wal`/`-shm`/`-journal`
//!      aside, so no connection that opens the path once the restored file is
//!      in place can apply them to it. A journal beside the file records the
//!      database's identity first. **`abort_file_restore`** moves the sidecars
//!      back when that rename does not replace the file.
//!      [`recover_interrupted_sqlite_restore`] does the same after a crash,
//!      or deletes the parked sidecars when the rename did replace the file.
//!   4. **`finalize_file_snapshot`** — after the rename, deletes the sidecars
//!      that were set aside and any that a connection to the replaced database
//!      created in between.

use async_trait::async_trait;
use parking_lot::Mutex;
use snafu::prelude::*;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;
use tokio::sync::Notify;

use super::SnapshotEngine;

#[derive(Debug, Snafu)]
pub enum SqliteSnapshotError {
    #[snafu(display("Failed to open SQLite for snapshot preparation: {path:?}"))]
    Connect {
        path: PathBuf,
        source: rusqlite::Error,
    },
    #[snafu(display(
        "Failed to checkpoint SQLite WAL for dataset '{dataset}' at {path:?}: {source}"
    ))]
    Checkpoint {
        dataset: String,
        path: PathBuf,
        source: rusqlite::Error,
    },
    #[snafu(display(
        "Incomplete WAL checkpoint for dataset '{dataset}' at {path:?}: \
         busy={busy}, log_frames={log_frames}, checkpointed_frames={checkpointed_frames}. \
         Another connection is holding the WAL or not all frames were flushed; \
         snapshotting now would lose data."
    ))]
    CheckpointIncomplete {
        dataset: String,
        path: PathBuf,
        busy: i64,
        log_frames: i64,
        checkpointed_frames: i64,
    },
    #[snafu(display(
        "Failed to switch SQLite copy to journal_mode=DELETE for dataset '{dataset}' at {path:?}: {source}"
    ))]
    JournalMode {
        dataset: String,
        path: PathBuf,
        source: rusqlite::Error,
    },
    #[snafu(display(
        "Failed to remove the stale SQLite sidecar {path:?} after restoring the snapshot of dataset '{dataset}': {source}"
    ))]
    RemoveSidecar {
        dataset: String,
        path: PathBuf,
        source: std::io::Error,
    },
    #[snafu(display(
        "Failed to move aside the SQLite sidecar {path:?} before restoring the snapshot of dataset '{dataset}': {source}"
    ))]
    ParkSidecar {
        dataset: String,
        path: PathBuf,
        source: std::io::Error,
    },
    #[snafu(display(
        "Failed to put back the SQLite sidecar {path:?} after the snapshot restore of dataset '{dataset}' did not replace the database: {source}"
    ))]
    RestoreSidecar {
        dataset: String,
        path: PathBuf,
        source: std::io::Error,
    },
    #[snafu(display(
        "Failed to read the snapshot restore journal {path:?} for dataset '{dataset}': {source}"
    ))]
    ReadRestoreJournal {
        dataset: String,
        path: PathBuf,
        source: std::io::Error,
    },
    #[snafu(display(
        "Failed to write the snapshot restore journal {path:?} for dataset '{dataset}': {source}"
    ))]
    WriteRestoreJournal {
        dataset: String,
        path: PathBuf,
        source: std::io::Error,
    },
    #[snafu(display(
        "The snapshot restore journal {path:?} for dataset '{dataset}' is invalid, so the parked SQLite sidecars were left in place"
    ))]
    InvalidRestoreJournal { dataset: String, path: PathBuf },
    #[snafu(display(
        "The snapshot restore journal {path:?} for dataset '{dataset}' is present but the database file is not, so the parked SQLite sidecars were left in place"
    ))]
    RestoreJournalWithoutDatabase { dataset: String, path: PathBuf },
    #[snafu(display(
        "SQLite snapshot preparation task failed unexpectedly for dataset '{dataset}'"
    ))]
    JoinError {
        dataset: String,
        source: tokio::task::JoinError,
    },
}

pub struct SqliteSnapshotEngine;

impl SqliteSnapshotEngine {
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl Default for SqliteSnapshotEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl SnapshotEngine for SqliteSnapshotEngine {
    async fn checkpoint_live(
        &self,
        live_path: &Path,
        dataset_name: &str,
    ) -> Result<(), super::SnapshotEngineError> {
        let live_path = live_path.to_path_buf();
        let dataset = dataset_name.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = rusqlite::Connection::open(&live_path).context(ConnectSnafu {
                path: live_path.clone(),
            })?;
            // wal_checkpoint(TRUNCATE) forces all WAL frames into the main
            // database file and truncates the WAL to zero length. This is the
            // strongest available checkpoint short of switching journal mode.
            //
            // The pragma returns one row `(busy, log, checkpointed)`:
            //   * `busy != 0` means another connection (e.g. a stuck
            //     read-transaction) is holding the WAL and the truncation
            //     could not complete.
            //   * `checkpointed < log` means not every frame was flushed.
            //
            // Either case would let post-checkpoint writes leak past the copy
            // we're about to take, defeating the whole point of the hook. We
            // surface them as `Checkpoint` errors so the caller can either
            // retry or fall back rather than silently snapshot a corrupted
            // database.
            let (busy, log_frames, checkpointed_frames): (i64, i64, i64) = conn
                .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                })
                .context(CheckpointSnafu {
                    dataset: dataset.clone(),
                    path: live_path.clone(),
                })?;
            if busy != 0 || checkpointed_frames < log_frames {
                return Err(SqliteSnapshotError::CheckpointIncomplete {
                    dataset: dataset.clone(),
                    path: live_path,
                    busy,
                    log_frames,
                    checkpointed_frames,
                });
            }
            Ok::<(), SqliteSnapshotError>(())
        })
        .await
        .context(JoinSnafu {
            dataset: dataset_name.to_string(),
        })
        .map_err(|e| super::SnapshotEngineError::Sqlite { source: e })?
        .map_err(|e| super::SnapshotEngineError::Sqlite { source: e })
    }

    async fn prepare_for_upload(
        &self,
        source_path: &Path,
        dataset_name: &str,
    ) -> Result<PathBuf, super::SnapshotEngineError> {
        // The caller has already done `fs::copy(live_db, source_path)` after
        // `checkpoint_live` flushed the WAL. The copy should already be
        // self-contained, but as defense-in-depth we switch the copy to
        // `journal_mode=DELETE` to guarantee no `-wal`/`-shm` sidecars exist
        // adjacent to the file we're about to upload.
        let copy_path = source_path.to_path_buf();
        let dataset = dataset_name.to_string();
        let path = tokio::task::spawn_blocking(move || {
            let conn = rusqlite::Connection::open(&copy_path).context(ConnectSnafu {
                path: copy_path.clone(),
            })?;
            // Switching to DELETE mode forces a final checkpoint and removes
            // any -wal/-shm files. If the copy never had a WAL (because the
            // live checkpoint already truncated it), this is a no-op.
            conn.query_row("PRAGMA journal_mode=DELETE", [], |_row| Ok(()))
                .context(JournalModeSnafu {
                    dataset,
                    path: copy_path.clone(),
                })?;
            Ok::<PathBuf, SqliteSnapshotError>(copy_path)
        })
        .await
        .context(JoinSnafu {
            dataset: dataset_name.to_string(),
        })
        .map_err(|e| super::SnapshotEngineError::Sqlite { source: e })?
        .map_err(|e| super::SnapshotEngineError::Sqlite { source: e })?;
        Ok(path)
    }

    fn supports_compaction(&self) -> bool {
        false
    }

    async fn prepare_file_restore(
        &self,
        live_path: &Path,
        dataset_name: &str,
    ) -> Result<(), super::SnapshotEngineError> {
        // Finish an earlier attempt that crashed after parking sidecars before
        // this one records a new journal and parks again.
        recover_interrupted_sqlite_restore(live_path, dataset_name).await?;
        match tokio::fs::metadata(live_path).await {
            Ok(_) => write_restore_journal(live_path, dataset_name).await?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(source) => {
                return Err(journal_error(
                    dataset_name,
                    live_path.to_path_buf(),
                    source,
                    true,
                ));
            }
        }
        // The rename replaces the database file but not the sidecars kept
        // beside it, so they are moved aside first. A connection opening the
        // path once the restored file is in place would otherwise take the
        // stale `-wal` as the restored database's and apply it, and a
        // checkpoint would then write the old pages into the restored file.
        // Moving them, rather than deleting them, lets `abort_file_restore`
        // put them back when the rename does not replace the file. Connections
        // already open on the live file hold their own handles to the moved
        // sidecars, so reads in flight are unaffected.
        if let Err(error) = park_sidecars(live_path, dataset_name).await {
            // Parking rolled its own partial move back. Drop the journal so
            // the next open does not treat this attempt as interrupted.
            let _ = remove_restore_journal(live_path, dataset_name).await;
            return Err(error);
        }
        Ok(())
    }

    async fn abort_file_restore(
        &self,
        live_path: &Path,
        dataset_name: &str,
    ) -> Result<(), super::SnapshotEngineError> {
        unpark_sidecars(live_path, dataset_name).await?;
        remove_restore_journal(live_path, dataset_name).await
    }

    async fn finalize_file_snapshot(
        &self,
        restored_path: &Path,
        dataset_name: &str,
    ) -> Result<(), super::SnapshotEngineError> {
        // Drop the parked originals before removing anything a connection
        // created at the live names. A later abort must not be able to put
        // the replaced database's log beside the restored file.
        discard_parked_sidecars(restored_path, dataset_name).await?;
        // A connection to the replaced database opened after the sidecars were
        // moved aside may have created new ones at the live names.
        remove_sidecars(restored_path, dataset_name).await?;
        remove_restore_journal(restored_path, dataset_name).await
    }
}

const SIDECAR_SUFFIXES: [&str; 3] = ["-wal", "-shm", "-journal"];
/// Not a suffix `SQLite` looks up, so a connection that opens the database
/// while a restore is in progress cannot apply the parked log.
const PARKED_SIDECAR_MARK: &str = ".spice-aside";

fn parked_sidecar_path(database: &Path, suffix: &str) -> PathBuf {
    let mut path = sidecar_path(database, suffix).into_os_string();
    path.push(PARKED_SIDECAR_MARK);
    PathBuf::from(path)
}

async fn park_sidecars(
    database: &Path,
    dataset_name: &str,
) -> Result<(), super::SnapshotEngineError> {
    for suffix in SIDECAR_SUFFIXES {
        let from = sidecar_path(database, suffix);
        let to = parked_sidecar_path(database, suffix);
        match rename_over(&from, &to).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(source) => {
                unpark_sidecars(database, dataset_name).await?;
                return Err(super::SnapshotEngineError::Sqlite {
                    source: SqliteSnapshotError::ParkSidecar {
                        dataset: dataset_name.to_string(),
                        path: from,
                        source,
                    },
                });
            }
        }
    }
    Ok(())
}

async fn unpark_sidecars(
    database: &Path,
    dataset_name: &str,
) -> Result<(), super::SnapshotEngineError> {
    for suffix in SIDECAR_SUFFIXES {
        let from = parked_sidecar_path(database, suffix);
        let to = sidecar_path(database, suffix);
        match rename_over(&from, &to).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(source) => {
                return Err(super::SnapshotEngineError::Sqlite {
                    source: SqliteSnapshotError::RestoreSidecar {
                        dataset: dataset_name.to_string(),
                        path: to,
                        source,
                    },
                });
            }
        }
    }
    Ok(())
}

async fn discard_parked_sidecars(
    database: &Path,
    dataset_name: &str,
) -> Result<(), super::SnapshotEngineError> {
    for suffix in SIDECAR_SUFFIXES {
        let path = parked_sidecar_path(database, suffix);
        match tokio::fs::remove_file(&path).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(source) => {
                return Err(super::SnapshotEngineError::Sqlite {
                    source: SqliteSnapshotError::RemoveSidecar {
                        dataset: dataset_name.to_string(),
                        path,
                        source,
                    },
                });
            }
        }
    }
    Ok(())
}

/// Renames `from` onto `to`. On Windows, replacing an existing file can fail
/// with [`std::io::ErrorKind::AlreadyExists`]; remove that file and retry once.
async fn rename_over(from: &Path, to: &Path) -> std::io::Result<()> {
    match tokio::fs::rename(from, to).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            tokio::fs::remove_file(to).await?;
            tokio::fs::rename(from, to).await
        }
        Err(error) => Err(error),
    }
}

fn restore_journal_path(database: &Path) -> PathBuf {
    let mut path = database.as_os_str().to_owned();
    path.push(".spice-restore");
    PathBuf::from(path)
}

/// Identity of `database` before a restore renames another file over it.
/// A crash after the sidecars are parked compares this with the file that is
/// there now: the same file means the rename did not happen.
async fn file_identity(path: &Path) -> std::io::Result<String> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let meta = tokio::fs::metadata(path).await?;
        Ok(format!("unix {} {}", meta.dev(), meta.ino()))
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        let meta = tokio::fs::metadata(path).await?;
        let volume = meta.volume_serial_number().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "volume serial number unavailable",
            )
        })?;
        let index = meta.file_index().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::Unsupported, "file index unavailable")
        })?;
        Ok(format!("windows {volume} {index}"))
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = path;
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "no file identity on this platform",
        ))
    }
}

fn journal_error(
    dataset_name: &str,
    path: PathBuf,
    source: std::io::Error,
    write: bool,
) -> super::SnapshotEngineError {
    let dataset = dataset_name.to_string();
    super::SnapshotEngineError::Sqlite {
        source: if write {
            SqliteSnapshotError::WriteRestoreJournal {
                dataset,
                path,
                source,
            }
        } else {
            SqliteSnapshotError::ReadRestoreJournal {
                dataset,
                path,
                source,
            }
        },
    }
}

async fn write_restore_journal(
    database: &Path,
    dataset_name: &str,
) -> Result<(), super::SnapshotEngineError> {
    let identity = file_identity(database)
        .await
        .map_err(|source| journal_error(dataset_name, database.to_path_buf(), source, true))?;
    let journal = restore_journal_path(database);
    let mut temporary = journal.as_os_str().to_owned();
    temporary.push(".tmp");
    let temporary = PathBuf::from(temporary);
    tokio::fs::write(&temporary, identity.as_bytes())
        .await
        .map_err(|source| journal_error(dataset_name, temporary.clone(), source, true))?;
    rename_over(&temporary, &journal)
        .await
        .map_err(|source| journal_error(dataset_name, journal, source, true))?;
    Ok(())
}

struct RestoreJournal {
    identity: String,
    aside_name: Option<String>,
}

fn parse_restore_journal(text: &str) -> Option<RestoreJournal> {
    let mut lines = text.lines();
    let identity = lines.next()?.trim();
    if identity.is_empty() || !(identity.starts_with("unix ") || identity.starts_with("windows ")) {
        return None;
    }
    let mut aside_name = None;
    for line in lines {
        let Some(name) = line.strip_prefix("aside ") else {
            continue;
        };
        let name = name.trim();
        if name.is_empty()
            || name.contains('/')
            || name.contains('\\')
            || name == "."
            || name == ".."
        {
            return None;
        }
        aside_name = Some(name.to_string());
    }
    Some(RestoreJournal {
        identity: identity.to_string(),
        aside_name,
    })
}

/// Records the file name the live database will be moved to before a
/// Windows replace installs the download. Recovery renames that file back
/// when the live path is missing.
pub(crate) async fn record_sqlite_restore_aside(
    database: &Path,
    aside: &Path,
    dataset_name: &str,
) -> Result<(), super::SnapshotEngineError> {
    let journal = restore_journal_path(database);
    let text = tokio::fs::read_to_string(&journal)
        .await
        .map_err(|source| journal_error(dataset_name, journal.clone(), source, false))?;
    let Some(parsed) = parse_restore_journal(&text) else {
        return Err(super::SnapshotEngineError::Sqlite {
            source: SqliteSnapshotError::InvalidRestoreJournal {
                dataset: dataset_name.to_string(),
                path: journal,
            },
        });
    };
    let Some(name) = aside.file_name().and_then(|name| name.to_str()) else {
        return Err(super::SnapshotEngineError::Sqlite {
            source: SqliteSnapshotError::InvalidRestoreJournal {
                dataset: dataset_name.to_string(),
                path: aside.to_path_buf(),
            },
        });
    };
    let body = format!("{}\naside {name}\n", parsed.identity);
    let mut temporary = restore_journal_path(database).into_os_string();
    temporary.push(".tmp");
    let temporary = PathBuf::from(temporary);
    tokio::fs::write(&temporary, body.as_bytes())
        .await
        .map_err(|source| journal_error(dataset_name, temporary.clone(), source, true))?;
    // `rename` replaces an existing file on Unix and does not on Windows.
    // The journal is created once and then rewritten when the aside file is
    // named, so the second rename has to replace the first.
    rename_over(&temporary, &journal)
        .await
        .map_err(|source| journal_error(dataset_name, journal, source, true))?;
    Ok(())
}

async fn remove_restore_journal(
    database: &Path,
    dataset_name: &str,
) -> Result<(), super::SnapshotEngineError> {
    let journal = restore_journal_path(database);
    match tokio::fs::remove_file(&journal).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(journal_error(dataset_name, journal, source, false)),
    }
}

fn restore_key(path: &Path) -> PathBuf {
    std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf())
}

/// Restores whose journal belongs to this process. A pool open during one of
/// them must not treat that journal as a crash and move the WAL back.
static ACTIVE_SQLITE_RESTORES: LazyLock<Mutex<HashSet<PathBuf>>> =
    LazyLock::new(|| Mutex::new(HashSet::new()));
static SQLITE_RESTORE_FINISHED: Notify = Notify::const_new();

/// Held from before a restore parks `SQLite` sidecars until the attempt has
/// put them back or deleted them.
#[must_use = "dropping the guard marks the restore finished"]
pub(crate) struct ActiveSqliteRestore {
    path: PathBuf,
}

impl Drop for ActiveSqliteRestore {
    fn drop(&mut self) {
        ACTIVE_SQLITE_RESTORES.lock().remove(&self.path);
        SQLITE_RESTORE_FINISHED.notify_waiters();
    }
}

/// Waits until this process is not between parking a `SQLite` database's
/// sidecars and putting them back or deleting them.
///
/// A connection opened in that interval can create a new `-wal` for the file
/// the rename just installed. The cleanup that follows deletes that `-wal`,
/// and the next connection then fails with a disk I/O error.
pub async fn wait_for_sqlite_restore(path: &Path) {
    let key = restore_key(path);
    loop {
        // Subscribe before the check so a finish that lands between them is
        // not missed.
        let notified = SQLITE_RESTORE_FINISHED.notified();
        tokio::pin!(notified);
        if !ACTIVE_SQLITE_RESTORES.lock().contains(&key) {
            return;
        }
        notified.await;
    }
}

pub(crate) fn begin_sqlite_restore(path: &Path) -> ActiveSqliteRestore {
    let path = restore_key(path);
    ACTIVE_SQLITE_RESTORES.lock().insert(path.clone());
    ActiveSqliteRestore { path }
}

/// Puts parked `SQLite` sidecars back, or deletes them, after a restore was
/// interrupted before it could finish.
///
/// The journal stores the identity of the database file from before the
/// rename. The same file means the replacement never landed, so the parked
/// `-wal`/`-shm`/`-journal` are moved back. A different file means the
/// replacement landed, so those sidecars are deleted rather than applied to it.
/// No journal means there is nothing to finish. A restore this process still
/// has in progress is left alone.
///
/// Call this before opening the database. A connection opened while the log is
/// still parked does not see the rows that exist only in that log.
///
/// # Errors
///
/// Returns an error when the journal cannot be read, the journal text is not a
/// file identity, the database file is missing while a journal is present, the
/// current file's identity cannot be read, or the parked sidecars cannot be
/// moved back or deleted.
pub async fn recover_interrupted_sqlite_restore(
    database: &Path,
    dataset_name: &str,
) -> Result<(), super::SnapshotEngineError> {
    // This process still owns the journal. The attempt that parked the WAL
    // will put it back, or delete it, itself. Treating the journal as a crash
    // here moves the WAL back beside the file the rename is about to replace.
    if ACTIVE_SQLITE_RESTORES
        .lock()
        .contains(&restore_key(database))
    {
        return Ok(());
    }
    // `rename_over` deletes the installed journal before renaming the
    // temporary file onto it. A crash in between leaves only the temporary
    // file, which already holds the identity and the aside name.
    let journal = restore_journal_path(database);
    let mut temporary = journal.as_os_str().to_owned();
    temporary.push(".tmp");
    let temporary = PathBuf::from(temporary);
    let text = match tokio::fs::read_to_string(&journal).await {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            match tokio::fs::read_to_string(&temporary).await {
                Ok(text) => text,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
                Err(source) => {
                    return Err(journal_error(dataset_name, temporary, source, false));
                }
            }
        }
        Err(source) => return Err(journal_error(dataset_name, journal, source, false)),
    };
    let Some(parsed) = parse_restore_journal(&text) else {
        return Err(super::SnapshotEngineError::Sqlite {
            source: SqliteSnapshotError::InvalidRestoreJournal {
                dataset: dataset_name.to_string(),
                path: journal,
            },
        });
    };
    match tokio::fs::metadata(database).await {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let Some(name) = parsed.aside_name.as_deref() else {
                return Err(super::SnapshotEngineError::Sqlite {
                    source: SqliteSnapshotError::RestoreJournalWithoutDatabase {
                        dataset: dataset_name.to_string(),
                        path: journal,
                    },
                });
            };
            let aside = database
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .join(name);
            match tokio::fs::rename(&aside, database).await {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    return Err(super::SnapshotEngineError::Sqlite {
                        source: SqliteSnapshotError::RestoreJournalWithoutDatabase {
                            dataset: dataset_name.to_string(),
                            path: journal,
                        },
                    });
                }
                Err(source) => {
                    return Err(journal_error(dataset_name, aside, source, false));
                }
            }
        }
        Err(source) => {
            return Err(journal_error(
                dataset_name,
                database.to_path_buf(),
                source,
                false,
            ));
        }
    }
    let current = file_identity(database)
        .await
        .map_err(|source| journal_error(dataset_name, database.to_path_buf(), source, false))?;
    if current == parsed.identity {
        unpark_sidecars(database, dataset_name).await?;
    } else {
        discard_parked_sidecars(database, dataset_name).await?;
        if let Some(name) = parsed.aside_name.as_deref() {
            let aside = database
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .join(name);
            if aside != database {
                match tokio::fs::remove_file(&aside).await {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(source) => {
                        return Err(journal_error(dataset_name, aside, source, false));
                    }
                }
            }
        }
    }
    remove_restore_journal(database, dataset_name).await?;
    match tokio::fs::remove_file(&temporary).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(journal_error(dataset_name, temporary, source, false)),
    }
}

async fn remove_sidecars(
    database: &Path,
    dataset_name: &str,
) -> Result<(), super::SnapshotEngineError> {
    for suffix in SIDECAR_SUFFIXES {
        let sidecar = sidecar_path(database, suffix);
        match tokio::fs::remove_file(&sidecar).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(source) => {
                return Err(super::SnapshotEngineError::Sqlite {
                    source: SqliteSnapshotError::RemoveSidecar {
                        dataset: dataset_name.to_string(),
                        path: sidecar,
                        source,
                    },
                });
            }
        }
    }
    Ok(())
}

/// `SQLite` names a database's sidecars by appending to the database path.
fn sidecar_path(database: &Path, suffix: &str) -> PathBuf {
    let mut path = database.as_os_str().to_owned();
    path.push(suffix);
    PathBuf::from(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;
    use tempfile::TempDir;

    fn create_wal_mode_db_with_rows(tmp: &TempDir, rows: &[(i64, &str)]) -> PathBuf {
        let db_path = tmp.path().join("live.sqlite");
        let conn = Connection::open(&db_path).expect("open");
        // Force WAL mode and write rows; do NOT checkpoint, so the main
        // file remains nearly-empty and the data lives only in the WAL.
        conn.query_row("PRAGMA journal_mode=WAL", [], |_| Ok(()))
            .expect("wal");
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT)", [])
            .expect("create");
        for (id, name) in rows {
            conn.execute("INSERT INTO t(id, name) VALUES (?1, ?2)", (id, name))
                .expect("insert");
        }
        // Drop without explicit checkpoint -- keep WAL state intact.
        drop(conn);
        db_path
    }

    fn count_rows(path: &Path) -> i64 {
        let conn = Connection::open(path).expect("open verify");
        conn.query_row("SELECT COUNT(*) FROM t", [], |row| row.get(0))
            .expect("count")
    }

    /// Restores a three-row snapshot over a live WAL-mode database whose
    /// connection is still open with an unflushed WAL, the state an
    /// accelerator's first reload replaces. Returns the restored path and the
    /// open connection to the replaced database.
    fn restore_over_live_wal_database(tmp: &TempDir) -> (PathBuf, Connection) {
        let live_path = tmp.path().join("orders.sqlite");
        let live = Connection::open(&live_path).expect("open live");
        live.query_row("PRAGMA journal_mode=WAL", [], |_| Ok(()))
            .expect("wal");
        live.execute_batch(
            "PRAGMA wal_autocheckpoint=0; CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT);",
        )
        .expect("create");
        assert!(sidecar_path(&live_path, "-wal").exists());

        let download = tmp.path().join("download.sqlite");
        let snapshot = Connection::open(&download).expect("open download");
        snapshot
            .execute_batch(
                "CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT);
                 INSERT INTO t(id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c');",
            )
            .expect("write snapshot");
        drop(snapshot);

        std::fs::rename(&download, &live_path).expect("restore");
        (live_path, live)
    }

    /// A live WAL-mode database with an unflushed WAL and its connection
    /// still open, and a downloaded three-row snapshot beside it: the state
    /// an accelerator's first reload replaces. The caller does the rename.
    fn live_wal_database_and_download(tmp: &TempDir) -> (PathBuf, PathBuf, Connection) {
        let live_path = tmp.path().join("orders.sqlite");
        let live = Connection::open(&live_path).expect("open live");
        live.query_row("PRAGMA journal_mode=WAL", [], |_| Ok(()))
            .expect("wal");
        live.execute_batch(
            "PRAGMA wal_autocheckpoint=0; CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT);",
        )
        .expect("create");
        assert!(sidecar_path(&live_path, "-wal").exists());

        let download = tmp.path().join("download.sqlite");
        let snapshot = Connection::open(&download).expect("open download");
        snapshot
            .execute_batch(
                "CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT);
                 INSERT INTO t(id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c');",
            )
            .expect("write snapshot");
        drop(snapshot);
        (live_path, download, live)
    }

    fn count_rows_in_wal_mode(path: &Path) -> i64 {
        let conn = Connection::open(path).expect("open fresh");
        conn.query_row("PRAGMA journal_mode=WAL", [], |_| Ok(()))
            .expect("wal");
        conn.query_row("SELECT COUNT(*) FROM t", [], |row| row.get(0))
            .expect("count")
    }

    #[tokio::test]
    async fn a_restore_over_a_live_wal_database_keeps_its_rows() {
        // Control: without the hook, the restored rows are lost to the stale
        // WAL, so this scenario reproduces what the hook exists to prevent.
        let unfixed = TempDir::new().expect("tmp");
        let (path, _live) = restore_over_live_wal_database(&unfixed);
        assert_eq!(count_rows_in_wal_mode(&path), 0);

        let tmp = TempDir::new().expect("tmp");
        let (path, live) = restore_over_live_wal_database(&tmp);
        SqliteSnapshotEngine::new()
            .finalize_file_snapshot(&path, "orders")
            .await
            .expect("finalize");
        assert!(!sidecar_path(&path, "-wal").exists());
        assert!(!sidecar_path(&path, "-shm").exists());
        assert_eq!(count_rows_in_wal_mode(&path), 3);

        // Closing the replaced database's connection afterwards changes nothing.
        drop(live);
        assert_eq!(count_rows_in_wal_mode(&path), 3);
    }

    #[tokio::test]
    async fn a_connection_opening_the_path_during_a_restore_reads_the_restored_rows() {
        let engine = SqliteSnapshotEngine::new();

        // Control: with the sidecars removed only after the rename, a
        // connection that opens the path in between applies the stale WAL and
        // the restored rows are lost for good.
        let unfixed = TempDir::new().expect("tmp");
        let (live_path, download, live) = live_wal_database_and_download(&unfixed);
        std::fs::rename(&download, &live_path).expect("restore");
        assert_eq!(count_rows_in_wal_mode(&live_path), 0);
        engine
            .finalize_file_snapshot(&live_path, "orders")
            .await
            .expect("finalize");
        assert_eq!(count_rows_in_wal_mode(&live_path), 0);
        drop(live);

        let tmp = TempDir::new().expect("tmp");
        let (live_path, download, live) = live_wal_database_and_download(&tmp);
        engine
            .prepare_file_restore(&live_path, "orders")
            .await
            .expect("prepare");
        std::fs::rename(&download, &live_path).expect("restore");
        assert_eq!(count_rows_in_wal_mode(&live_path), 3);
        engine
            .finalize_file_snapshot(&live_path, "orders")
            .await
            .expect("finalize");
        assert_eq!(count_rows_in_wal_mode(&live_path), 3);

        // Closing the replaced database's connection afterwards changes nothing.
        drop(live);
        assert_eq!(count_rows_in_wal_mode(&live_path), 3);
    }

    /// Opens a fresh connection. Returns the row count, or the error `SQLite`
    /// raises when the database's WAL was removed out from under it.
    fn fresh_row_count(path: &Path) -> Result<i64, rusqlite::Error> {
        let conn = Connection::open(path)?;
        conn.query_row("PRAGMA journal_mode=WAL", [], |_| Ok(()))?;
        conn.query_row("SELECT COUNT(*) FROM t", [], |row| row.get(0))
    }

    #[tokio::test]
    async fn a_failed_rename_after_prepare_file_restore_keeps_the_live_rows() {
        let tmp = TempDir::new().expect("tmp");
        let live_path = tmp.path().join("orders.sqlite");
        let live = Connection::open(&live_path).expect("open live");
        live.query_row("PRAGMA journal_mode=WAL", [], |_| Ok(()))
            .expect("wal");
        live.execute_batch(
            "PRAGMA wal_autocheckpoint=0;
             CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT);
             INSERT INTO t(id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c');",
        )
        .expect("write live rows into the wal");
        assert!(sidecar_path(&live_path, "-wal").exists());
        assert_eq!(fresh_row_count(&live_path).expect("read before prepare"), 3);

        let engine = SqliteSnapshotEngine::new();
        let missing = tmp.path().join("missing-download.sqlite");
        let error =
            crate::snapshot::replace_downloaded_file(&engine, &missing, &live_path, "orders")
                .await
                .expect_err("a missing download cannot replace the live database");
        assert!(
            matches!(
                error,
                crate::snapshot::SnapshotDownloadError::WriteLocal { .. }
            ),
            "the rename failure is reported once the wal is back: {error}"
        );
        // The original connection is still open, which is the pool's state when
        // a restore fails. A connection opened now must still read the rows.
        assert_eq!(
            fresh_row_count(&live_path).expect("read while the original connection is open"),
            3
        );
        assert!(
            sidecar_path(&live_path, "-wal").exists(),
            "the live wal is back beside the database"
        );
        assert!(
            !parked_sidecar_path(&live_path, "-wal").exists(),
            "the parked copy was moved back, not left aside"
        );
        drop(live);
        assert_eq!(fresh_row_count(&live_path).expect("read after close"), 3);
        assert!(
            !restore_journal_path(&live_path).exists(),
            "a finished attempt does not leave a restore journal"
        );
    }

    #[tokio::test]
    async fn an_interrupted_restore_before_the_rename_is_recovered_on_reopen() {
        let tmp = TempDir::new().expect("tmp");
        let live_path = tmp.path().join("orders.sqlite");
        let live = Connection::open(&live_path).expect("open live");
        live.query_row("PRAGMA journal_mode=WAL", [], |_| Ok(()))
            .expect("wal");
        live.execute_batch(
            "PRAGMA wal_autocheckpoint=0;
             CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT);
             INSERT INTO t(id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c');",
        )
        .expect("write live rows into the wal");
        let engine = SqliteSnapshotEngine::new();
        engine
            .prepare_file_restore(&live_path, "orders")
            .await
            .expect("park the wal");
        // A crash does not run `SQLite`'s clean shutdown, which would checkpoint
        // the still-open log back into the main file. Leak the connection so
        // this reopen is that crash, not a checkpoint.
        std::mem::forget(live);
        let interrupted = fresh_row_count(&live_path).expect_err("parked wal is not readable");
        assert!(
            interrupted.to_string().contains("no such table")
                || interrupted.to_string().contains("disk I/O error"),
            "interrupted reopen: {interrupted}"
        );

        recover_interrupted_sqlite_restore(&live_path, "orders")
            .await
            .expect("recover");
        assert_eq!(fresh_row_count(&live_path).expect("recovered"), 3);
        assert!(sidecar_path(&live_path, "-wal").exists());
        assert!(!parked_sidecar_path(&live_path, "-wal").exists());
        assert!(!restore_journal_path(&live_path).exists());
    }

    #[tokio::test]
    async fn an_interrupted_windows_swap_restores_the_database_from_the_aside_file() {
        let tmp = TempDir::new().expect("tmp");
        let live_path = tmp.path().join("orders.sqlite");
        let live = Connection::open(&live_path).expect("open live");
        live.query_row("PRAGMA journal_mode=WAL", [], |_| Ok(()))
            .expect("wal");
        live.execute_batch(
            "PRAGMA wal_autocheckpoint=0;
             CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT);
             INSERT INTO t(id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c');",
        )
        .expect("write live rows into the wal");
        let engine = SqliteSnapshotEngine::new();
        engine
            .prepare_file_restore(&live_path, "orders")
            .await
            .expect("park the wal");
        let aside = live_path.with_extension(format!("old.{}", std::process::id()));
        record_sqlite_restore_aside(&live_path, &aside, "orders")
            .await
            .expect("record aside");
        std::fs::rename(&live_path, &aside).expect("move the live database aside");
        assert!(!live_path.exists());
        std::mem::forget(live);

        recover_interrupted_sqlite_restore(&live_path, "orders")
            .await
            .expect("restore the aside file");
        assert_eq!(fresh_row_count(&live_path).expect("rows are back"), 3);
        assert!(live_path.exists());
        assert!(!aside.exists());
        assert!(!restore_journal_path(&live_path).exists());
    }

    #[tokio::test]
    async fn recovery_reads_a_journal_left_in_the_temporary_file() {
        let tmp = TempDir::new().expect("tmp");
        let live_path = tmp.path().join("orders.sqlite");
        let live = Connection::open(&live_path).expect("open live");
        live.query_row("PRAGMA journal_mode=WAL", [], |_| Ok(()))
            .expect("wal");
        live.execute_batch(
            "PRAGMA wal_autocheckpoint=0;
             CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT);
             INSERT INTO t(id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c');",
        )
        .expect("write live rows into the wal");
        let engine = SqliteSnapshotEngine::new();
        engine
            .prepare_file_restore(&live_path, "orders")
            .await
            .expect("park the wal");
        let aside = live_path.with_extension(format!("old.{}", std::process::id()));
        record_sqlite_restore_aside(&live_path, &aside, "orders")
            .await
            .expect("record aside");
        let journal = restore_journal_path(&live_path);
        let text = std::fs::read_to_string(&journal).expect("read journal");
        let mut temporary = journal.as_os_str().to_owned();
        temporary.push(".tmp");
        let temporary = PathBuf::from(temporary);
        std::fs::write(&temporary, text).expect("write the temporary journal");
        std::fs::remove_file(&journal).expect("journal deleted before the rename finished");
        std::fs::rename(&live_path, &aside).expect("move the live database aside");
        std::mem::forget(live);

        recover_interrupted_sqlite_restore(&live_path, "orders")
            .await
            .expect("recover from the temporary journal");
        assert_eq!(fresh_row_count(&live_path).expect("rows are back"), 3);
        assert!(!temporary.exists());
        assert!(!journal.exists());
    }

    #[tokio::test]
    async fn a_pool_open_waits_until_the_restore_finishes() {
        let tmp = TempDir::new().expect("tmp");
        let live_path = tmp.path().join("orders.sqlite");
        std::fs::write(&live_path, b"sqlite").expect("write");
        let active = begin_sqlite_restore(&live_path);
        let wait_path = live_path.clone();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let waiting = tokio::spawn(async move {
            let _ = started_tx.send(());
            wait_for_sqlite_restore(&wait_path).await;
        });
        started_rx.await.expect("wait task started");
        tokio::task::yield_now().await;
        assert!(
            !waiting.is_finished(),
            "a pool open must wait while the restore holds the database"
        );
        drop(active);
        tokio::time::timeout(std::time::Duration::from_secs(2), waiting)
            .await
            .expect("pool open was not released when the restore finished")
            .expect("wait task");
    }

    #[tokio::test]
    async fn a_pool_open_during_restore_does_not_put_the_wal_back() {
        let tmp = TempDir::new().expect("tmp");
        let (live_path, download, live) = live_wal_database_and_download(&tmp);
        // `replace_downloaded_file` holds this for the whole attempt.
        let _active = begin_sqlite_restore(&live_path);
        let engine = SqliteSnapshotEngine::new();
        engine
            .prepare_file_restore(&live_path, "orders")
            .await
            .expect("park the wal");
        // `get_shared_pool` recovers before it opens the file. During a live
        // restore that recovery must not move the parked WAL back.
        recover_interrupted_sqlite_restore(&live_path, "orders")
            .await
            .expect("recover");
        std::fs::rename(&download, &live_path).expect("replacement landed");
        drop(live);
        assert_eq!(count_rows_in_wal_mode(&live_path), 3);
    }

    #[tokio::test]
    async fn an_interrupted_restore_after_the_rename_discards_the_parked_wal() {
        let tmp = TempDir::new().expect("tmp");
        let (live_path, download, live) = live_wal_database_and_download(&tmp);
        let engine = SqliteSnapshotEngine::new();
        engine
            .prepare_file_restore(&live_path, "orders")
            .await
            .expect("park the wal");
        std::fs::rename(&download, &live_path).expect("replacement landed");
        // Same crash as the pre-rename case: no clean shutdown checkpoint.
        std::mem::forget(live);

        recover_interrupted_sqlite_restore(&live_path, "orders")
            .await
            .expect("discard parked wal");
        // The parked log belongs to the replaced file. Applying it would make
        // this read 0.
        assert_eq!(count_rows_in_wal_mode(&live_path), 3);
        assert!(!parked_sidecar_path(&live_path, "-wal").exists());
        assert!(!restore_journal_path(&live_path).exists());
    }

    #[tokio::test]
    async fn replace_downloaded_file_keeps_the_snapshot_rows_and_drops_the_parked_wal() {
        let tmp = TempDir::new().expect("tmp");
        let (live_path, download, live) = live_wal_database_and_download(&tmp);
        let engine = SqliteSnapshotEngine::new();
        crate::snapshot::replace_downloaded_file(&engine, &download, &live_path, "orders")
            .await
            .expect("replace");
        assert_eq!(count_rows_in_wal_mode(&live_path), 3);
        drop(live);
        assert_eq!(count_rows_in_wal_mode(&live_path), 3);
        for suffix in SIDECAR_SUFFIXES {
            assert!(
                !sidecar_path(&live_path, suffix).exists(),
                "{suffix} still beside the restored file"
            );
            assert!(
                !parked_sidecar_path(&live_path, suffix).exists(),
                "{suffix} still parked"
            );
        }
    }

    #[tokio::test]
    async fn finalize_without_sidecars_is_a_no_op() {
        let tmp = TempDir::new().expect("tmp");
        let path = tmp.path().join("fresh.sqlite");
        std::fs::write(&path, b"").expect("write");
        SqliteSnapshotEngine::new()
            .finalize_file_snapshot(&path, "orders")
            .await
            .expect("finalize with no sidecars");
        assert!(path.exists());
    }

    #[tokio::test]
    async fn checkpoint_live_then_copy_captures_all_rows() {
        let tmp = TempDir::new().expect("tmp");
        let rows = vec![(1, "a"), (2, "b"), (3, "c")];
        let live = create_wal_mode_db_with_rows(&tmp, &rows);

        let engine = SqliteSnapshotEngine::new();
        engine
            .checkpoint_live(&live, "ds")
            .await
            .expect("checkpoint live");

        let copy = tmp.path().join("copy.sqlite");
        std::fs::copy(&live, &copy).expect("copy");

        let final_path = engine
            .prepare_for_upload(&copy, "ds")
            .await
            .expect("prepare");

        // No -wal or -shm should exist next to the prepared file.
        assert!(!final_path.with_extension("sqlite-wal").exists());
        assert!(!final_path.with_extension("sqlite-shm").exists());

        assert_eq!(
            count_rows(&final_path),
            i64::try_from(rows.len()).expect("row count fits in i64")
        );
    }
}
