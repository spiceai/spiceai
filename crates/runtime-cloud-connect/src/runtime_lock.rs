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

//! The single-runtime-per-instance-directory lock.
//!
//! One instance directory is one runtime: its `.spice` directory holds one
//! enrolled identity, one deployed spicepod, and one delivered-secrets cache,
//! and its ports are bound once. A second `spiced` started in the same
//! directory would connect a second control stream under the same identity,
//! answer commands addressed to its sibling, and race it for every file under
//! `.spice` — so it has to refuse before it binds a listener or dials the
//! gateway, not discover the conflict from an "address already in use" after
//! the control plane has already seen two sessions.
//!
//! Ownership is the operating system's advisory file lock on
//! `<config-dir>/runtime.lock`, held open for the process's whole lifetime and
//! released by the kernel when the process exits — including a `SIGKILL` or a
//! crash, which is what makes a stale file harmless. The PID and start time
//! written *after* acquisition are diagnostics for the message a refused start
//! prints; they are never read to decide ownership, because a PID can be
//! recycled and a file can survive the process that wrote it.

use std::fs::File;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};

/// The lock file, relative to the config directory.
pub const RUNTIME_LOCK_FILE: &str = "runtime.lock";

/// Why a runtime could not take ownership of an instance directory.
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Failed to start the Spice runtime for {}: another runtime is already running in this \
         directory{owner}. One instance directory serves one runtime: a second would share the \
         enrolled identity, the deployed spicepod, and the ports this one is about to bind. Run \
         `spice connect status` to see what this directory is running, stop that runtime, or \
         start this one from another instance directory — `SPICE_CONFIG_DIR` moves the \
         per-instance state on its own. See: https://spiceai.org/docs",
        instance.display(),
    ))]
    AlreadyRunning {
        /// The instance directory, not the lock file: it is what an operator
        /// recognises and what the remedies are expressed in.
        instance: PathBuf,
        /// ` (pid 1234)` when the holder recorded itself, empty otherwise.
        /// Rendered by [`RuntimeLockOwner::describe`].
        owner: String,
    },

    #[snafu(display("Failed to prepare the runtime lock at {}: {source}", path.display()))]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// What the holder of the lock recorded about itself.
///
/// Diagnostics only. Its presence never implies the lock is held and its
/// absence never implies it is free — the file outlives the process that wrote
/// it, and a PID can belong to something else entirely by the time it is read.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeLockOwner {
    /// The process that held the lock when it wrote this.
    pub pid: u32,
    /// When that process took the lock, in Unix seconds. `None` when the host
    /// clock could not be read, which is not a reason to fail a start.
    #[serde(default)]
    pub acquired_unix: Option<u64>,
}

impl RuntimeLockOwner {
    /// The parenthesised clause a refusal names the holder with, or an empty
    /// string when nothing usable was recorded.
    #[must_use]
    fn describe(owner: Option<&Self>) -> String {
        match owner {
            Some(owner) => format!(" (pid {})", owner.pid),
            None => String::new(),
        }
    }
}

/// Exclusive ownership of one instance directory's runtime, held for as long
/// as this value lives.
///
/// Dropping it closes the descriptor, which is what releases the lock.
#[derive(Debug)]
pub struct RuntimeLock {
    path: PathBuf,
    /// Held open purely to hold the lock: the kernel releases it when this
    /// descriptor closes, whether that is a clean shutdown or a crash.
    _file: File,
}

impl RuntimeLock {
    /// Take exclusive ownership of `config_dir` for this process.
    ///
    /// Tries once and never waits: a caller that finds the directory owned has
    /// nothing to wait for — the holder keeps its lock for its whole lifetime.
    ///
    /// # Errors
    ///
    /// [`Error::AlreadyRunning`] when another live runtime owns the directory,
    /// and [`Error::Io`] when the lock file cannot be created or inspected.
    pub fn acquire(config_dir: &Path) -> Result<Self> {
        let path = config_dir.join(RUNTIME_LOCK_FILE);
        std::fs::create_dir_all(config_dir).context(IoSnafu {
            path: config_dir.to_path_buf(),
        })?;

        let file = open_lock_file(&path)?;

        if !fs4::fs_std::FileExt::try_lock_exclusive(&file)
            .context(IoSnafu { path: path.clone() })?
        {
            return Err(Error::AlreadyRunning {
                instance: instance_dir_of(config_dir),
                owner: RuntimeLockOwner::describe(read_owner(&path).as_ref()),
            });
        }

        // Only now: the contents describe a holder, and a process that never
        // held the lock must never have written them.
        record_owner(&file, &path)?;

        Ok(Self { path, _file: file })
    }

    /// The lock file this ownership is held on.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Open (creating if needed) the lock file as a regular, owner-only file.
///
/// The file type is checked through the opened descriptor rather than the path,
/// so a hostile FIFO or device swapped in at the path cannot block a privileged
/// runtime on `open` or be locked in place of the real file.
fn open_lock_file(path: &Path) -> Result<File> {
    #[cfg(unix)]
    use std::os::unix::fs::{OpenOptionsExt as _, PermissionsExt as _};

    let mut options = std::fs::OpenOptions::new();
    options.create(true).read(true).write(true).truncate(false);
    #[cfg(unix)]
    options
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);

    let file = options.open(path).context(IoSnafu {
        path: path.to_path_buf(),
    })?;

    #[cfg(unix)]
    {
        if !file
            .metadata()
            .context(IoSnafu {
                path: path.to_path_buf(),
            })?
            .is_file()
        {
            return Err(Error::Io {
                path: path.to_path_buf(),
                source: std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "the runtime lock must be a regular file",
                ),
            });
        }
        file.set_permissions(std::fs::Permissions::from_mode(0o600))
            .context(IoSnafu {
                path: path.to_path_buf(),
            })?;
    }

    Ok(file)
}

/// Replace the file's contents with this process's identification.
///
/// Written in place rather than through the usual atomic rename: the lock lives
/// on this descriptor, and a rename would put a *different* file at the path
/// for the next process to lock while this one held the old inode. A torn write
/// costs nothing — the contents are diagnostics, and an unparseable file simply
/// reports no owner.
fn record_owner(file: &File, path: &Path) -> Result<()> {
    use std::io::{Seek as _, SeekFrom, Write as _};

    let owner = RuntimeLockOwner {
        pid: std::process::id(),
        acquired_unix: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .ok()
            .map(|since| since.as_secs()),
    };
    let bytes = serde_json::to_vec(&owner).unwrap_or_default();

    let mut handle = file;
    handle.set_len(0).context(IoSnafu {
        path: path.to_path_buf(),
    })?;
    handle.seek(SeekFrom::Start(0)).context(IoSnafu {
        path: path.to_path_buf(),
    })?;
    handle.write_all(&bytes).context(IoSnafu {
        path: path.to_path_buf(),
    })?;
    handle.flush().context(IoSnafu {
        path: path.to_path_buf(),
    })?;
    Ok(())
}

/// Read the recorded holder, treating every failure as "nothing recorded":
/// this is the diagnostic half of a refusal that has already been decided.
fn read_owner(path: &Path) -> Option<RuntimeLockOwner> {
    let contents = std::fs::read(path).ok()?;
    serde_json::from_slice(&contents).ok()
}

/// The instance directory a config directory belongs to, for messages.
///
/// `SPICE_CONFIG_DIR` can point anywhere, so a config directory not named
/// `.spice` has no instance directory above it and names itself.
fn instance_dir_of(config_dir: &Path) -> PathBuf {
    if config_dir.file_name() == Some(std::ffi::OsStr::new(".spice"))
        && let Some(parent) = config_dir.parent().filter(|p| !p.as_os_str().is_empty())
    {
        return parent.to_path_buf();
    }
    config_dir.to_path_buf()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_second_runtime_is_refused_while_the_first_holds_the_lock() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let config_dir = dir.path().join(".spice");

        let first =
            RuntimeLock::acquire(&config_dir).expect("the first runtime owns the directory");

        let error = RuntimeLock::acquire(&config_dir)
            .expect_err("a second runtime must not take the same directory");
        assert!(matches!(error, Error::AlreadyRunning { .. }), "{error:?}");
        // The instance directory, not `.spice`, is what the operator recognises.
        assert!(
            error
                .to_string()
                .contains(&dir.path().display().to_string()),
            "{error}"
        );

        drop(first);
        // The lock is the kernel's, so releasing it frees the directory.
        RuntimeLock::acquire(&config_dir).expect("the directory is free once the holder exits");
    }

    #[test]
    fn stale_contents_alone_never_imply_ownership() {
        // The file survives a killed process. If its contents were read as
        // ownership, one crash would make a directory permanently unstartable.
        let dir = tempfile::tempdir().expect("create tempdir");
        let config_dir = dir.path().join(".spice");
        std::fs::create_dir_all(&config_dir).expect("create config dir");
        std::fs::write(
            config_dir.join(RUNTIME_LOCK_FILE),
            br#"{"pid":999999,"acquired_unix":1}"#,
        )
        .expect("write a stale lock file");

        let lock =
            RuntimeLock::acquire(&config_dir).expect("stale contents must not block a start");
        // And the holder replaces them with its own identification.
        let owner =
            read_owner(&config_dir.join(RUNTIME_LOCK_FILE)).expect("the holder records itself");
        assert_eq!(owner.pid, std::process::id());
        drop(lock);
    }

    #[test]
    fn nothing_is_recorded_before_the_lock_is_taken() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let config_dir = dir.path().join(".spice");
        let held = RuntimeLock::acquire(&config_dir).expect("hold the lock");

        let lock_path = config_dir.join(RUNTIME_LOCK_FILE);
        let owner = read_owner(&lock_path).expect("the holder recorded itself");
        assert_eq!(owner.pid, std::process::id());

        // A refused start must leave the holder's record intact: writing before
        // acquiring would let a process that owns nothing claim the directory.
        RuntimeLock::acquire(&config_dir).expect_err("refused");
        let after = read_owner(&lock_path).expect("still the holder");
        assert_eq!(after, owner);

        drop(held);
    }

    #[test]
    fn an_unparseable_lock_file_reports_no_owner_and_still_starts() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let config_dir = dir.path().join(".spice");
        std::fs::create_dir_all(&config_dir).expect("create config dir");
        std::fs::write(config_dir.join(RUNTIME_LOCK_FILE), b"not json").expect("write garbage");

        assert!(read_owner(&config_dir.join(RUNTIME_LOCK_FILE)).is_none());
        let lock = RuntimeLock::acquire(&config_dir).expect("garbage must not block a start");
        drop(lock);
    }

    #[test]
    fn the_message_names_the_instance_directory_for_a_custom_config_dir() {
        // SPICE_CONFIG_DIR can point anywhere; a directory not named `.spice`
        // has no instance directory above it to name.
        assert_eq!(
            instance_dir_of(Path::new("/var/lib/spice-state")),
            PathBuf::from("/var/lib/spice-state")
        );
        assert_eq!(
            instance_dir_of(Path::new("/srv/edge/.spice")),
            PathBuf::from("/srv/edge")
        );
    }
}
