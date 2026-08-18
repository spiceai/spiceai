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
//! On Unix, ownership is the operating system's advisory lock on the opened
//! config-directory inode. The retained descriptor keeps the claim stable when
//! `runtime.lock`, the directory, or one of its ancestors is renamed after
//! acquisition. The advisory lock on `<config-dir>/runtime.lock` is retained as
//! compatibility with runtimes released before the directory lease existed;
//! its PID and start time are diagnostics only. Windows prevents replacement of
//! the opened lock file through its share mode and holds that file lock for the
//! process's whole lifetime. In either case the kernel releases ownership on a
//! clean exit, `SIGKILL`, or crash, which is what makes stale contents harmless.

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

    #[snafu(display(
        "Failed to create the instance state directory {}: {source}",
        path.display()
    ))]
    DirectoryUnavailable {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Failed to claim the runtime lock at {}: {source}", path.display()))]
    Unusable {
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
    /// The directory inode is the authoritative Unix lease. A lock file can be
    /// unlinked while its descriptor is open, but another runtime resolving the
    /// same directory (including through a renamed path or symlink alias) still
    /// reaches this inode and is refused.
    #[cfg(unix)]
    _directory: File,
    /// Held open purely to hold the lock: the kernel releases it when this
    /// descriptor closes, whether that is a clean shutdown or a crash. On Unix
    /// this also excludes older runtimes that only know the file lease.
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
    /// [`Error::DirectoryUnavailable`] when the instance state directory cannot
    /// be created, and [`Error::Unusable`] when the lock file itself cannot be
    /// opened, checked, or locked. Every failure is authoritative: a runtime
    /// that cannot hold the lease must not start with this instance identity.
    pub fn acquire(config_dir: &Path) -> Result<Self> {
        let physical_config_dir =
            canonical_config_directory(config_dir).context(DirectoryUnavailableSnafu {
                path: config_dir.to_path_buf(),
            })?;
        std::fs::create_dir_all(&physical_config_dir).context(DirectoryUnavailableSnafu {
            path: physical_config_dir.clone(),
        })?;
        let physical_config_dir =
            std::fs::canonicalize(&physical_config_dir).context(DirectoryUnavailableSnafu {
                path: physical_config_dir,
            })?;
        let path = physical_config_dir.join(RUNTIME_LOCK_FILE);

        #[cfg(unix)]
        let directory = open_config_directory(&physical_config_dir)?;
        #[cfg(unix)]
        if !fs4::fs_std::FileExt::try_lock_exclusive(&directory)
            .context(UnusableSnafu { path: path.clone() })?
        {
            return Err(Error::AlreadyRunning {
                instance: instance_dir_of(config_dir),
                owner: RuntimeLockOwner::describe(read_owner_path(&path).as_ref()),
            });
        }

        #[cfg(unix)]
        let file = open_lock_file(&directory, &path)?;
        #[cfg(not(unix))]
        let file = open_lock_file(&path)?;

        if !fs4::fs_std::FileExt::try_lock_exclusive(&file)
            .context(UnusableSnafu { path: path.clone() })?
        {
            return Err(Error::AlreadyRunning {
                instance: instance_dir_of(config_dir),
                owner: RuntimeLockOwner::describe(read_owner(&file).as_ref()),
            });
        }

        // Only now: the contents describe a holder, and a process that never
        // held the lock must never have written them.
        //
        // A failure to write them is not a failure to acquire. The lock is the
        // kernel's and this process holds it; giving it up because a diagnostic
        // line could not be written would let a second runtime in, in exchange
        // for a message nothing depends on.
        if let Err(err) = record_owner(&file, &path) {
            tracing::warn!(
                "{err}. The instance directory is claimed regardless; only the diagnostic naming this process is missing."
            );
        }

        Ok(Self {
            path,
            #[cfg(unix)]
            _directory: directory,
            _file: file,
        })
    }

    /// The compatibility lock file that carries this holder's diagnostics.
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
#[cfg(unix)]
fn open_lock_file(directory: &File, path: &Path) -> Result<File> {
    use std::os::fd::{AsRawFd as _, FromRawFd as _};
    use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};

    let lock_name = c"runtime.lock";
    let fd = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            lock_name.as_ptr(),
            libc::O_RDWR | libc::O_CREAT | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK,
            0o600,
        )
    };
    if fd < 0 {
        return Err(Error::Unusable {
            path: path.to_path_buf(),
            source: std::io::Error::last_os_error(),
        });
    }
    let file = unsafe { File::from_raw_fd(fd) };
    let metadata = validate_regular_lock_file(&file, path)?;
    if metadata.nlink() != 1 {
        return Err(Error::Unusable {
            path: path.to_path_buf(),
            source: std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "the runtime lock must not be hard-linked",
            ),
        });
    }
    file.set_permissions(std::fs::Permissions::from_mode(0o600))
        .context(UnusableSnafu {
            path: path.to_path_buf(),
        })?;
    Ok(file)
}

#[cfg(not(unix))]
fn open_lock_file(path: &Path) -> Result<File> {
    let mut options = std::fs::OpenOptions::new();
    options.create(true).read(true).write(true).truncate(false);

    let file = options.open(path).context(UnusableSnafu {
        path: path.to_path_buf(),
    })?;

    validate_regular_lock_file(&file, path)?;

    Ok(file)
}

fn validate_regular_lock_file(file: &File, path: &Path) -> Result<std::fs::Metadata> {
    let metadata = file.metadata().context(UnusableSnafu {
        path: path.to_path_buf(),
    })?;
    if !metadata.is_file() {
        return Err(Error::Unusable {
            path: path.to_path_buf(),
            source: std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "the runtime lock must be a regular file",
            ),
        });
    }
    Ok(metadata)
}

/// Resolve aliases before taking the inode lease, while still allowing the
/// final `.spice` directory to be absent on first start.
fn canonical_config_directory(path: &Path) -> std::io::Result<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    let mut cursor = absolute.as_path();
    let mut missing = Vec::new();
    loop {
        match std::fs::symlink_metadata(cursor) {
            Ok(_) => {
                let mut resolved = std::fs::canonicalize(cursor)?;
                if !resolved.is_dir() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "the runtime config path must resolve to a directory",
                    ));
                }
                for component in missing.iter().rev() {
                    resolved.push(component);
                }
                return Ok(resolved);
            }
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
                let Some(name) = cursor.file_name() else {
                    return Err(source);
                };
                missing.push(name.to_os_string());
                let Some(parent) = cursor.parent() else {
                    return Err(source);
                };
                cursor = parent;
            }
            Err(source) => return Err(source),
        }
    }
}

/// Open the resolved directory without following a replacement at its final
/// entry. The open descriptor is the object locked and retained by the guard.
#[cfg(unix)]
fn open_config_directory(path: &Path) -> Result<File> {
    use std::ffi::CString;
    use std::os::fd::{AsRawFd as _, FromRawFd as _};
    use std::os::unix::ffi::OsStrExt as _;

    fn open_at(parent: &File, name: &std::ffi::OsStr) -> std::io::Result<File> {
        let name = CString::new(name.as_bytes()).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "runtime config paths cannot contain NUL bytes",
            )
        })?;
        let fd = unsafe {
            libc::openat(
                parent.as_raw_fd(),
                name.as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
            )
        };
        if fd < 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(unsafe { File::from_raw_fd(fd) })
    }

    let root_fd = unsafe {
        libc::open(
            c"/".as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC,
        )
    };
    if root_fd < 0 {
        return Err(Error::Unusable {
            path: path.to_path_buf(),
            source: std::io::Error::last_os_error(),
        });
    }
    let mut directory = unsafe { File::from_raw_fd(root_fd) };
    for component in path.components() {
        match component {
            std::path::Component::RootDir | std::path::Component::CurDir => {}
            std::path::Component::Normal(name) => {
                directory = open_at(&directory, name).context(UnusableSnafu {
                    path: path.to_path_buf(),
                })?;
            }
            std::path::Component::ParentDir | std::path::Component::Prefix(_) => {
                return Err(Error::Unusable {
                    path: path.to_path_buf(),
                    source: std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "the canonical runtime config path was not absolute",
                    ),
                });
            }
        }
    }
    Ok(directory)
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
    handle.set_len(0).context(UnusableSnafu {
        path: path.to_path_buf(),
    })?;
    handle.seek(SeekFrom::Start(0)).context(UnusableSnafu {
        path: path.to_path_buf(),
    })?;
    handle.write_all(&bytes).context(UnusableSnafu {
        path: path.to_path_buf(),
    })?;
    handle.flush().context(UnusableSnafu {
        path: path.to_path_buf(),
    })?;
    Ok(())
}

/// Read the recorded holder, treating every failure as "nothing recorded":
/// this is the diagnostic half of a refusal that has already been decided.
fn read_owner(file: &File) -> Option<RuntimeLockOwner> {
    use std::io::{Read as _, Seek as _, SeekFrom};

    const MAX_OWNER_BYTES: u64 = 4096;
    let mut handle = file;
    handle.seek(SeekFrom::Start(0)).ok()?;
    let mut contents = Vec::new();
    handle
        .take(MAX_OWNER_BYTES.saturating_add(1))
        .read_to_end(&mut contents)
        .ok()?;
    if u64::try_from(contents.len()).ok()? > MAX_OWNER_BYTES {
        return None;
    }
    serde_json::from_slice(&contents).ok()
}

fn read_owner_path(path: &Path) -> Option<RuntimeLockOwner> {
    read_owner(&File::open(path).ok()?)
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
        let owner = read_owner_path(&config_dir.join(RUNTIME_LOCK_FILE))
            .expect("the holder records itself");
        assert_eq!(owner.pid, std::process::id());
        drop(lock);
    }

    #[test]
    fn nothing_is_recorded_before_the_lock_is_taken() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let config_dir = dir.path().join(".spice");
        let held = RuntimeLock::acquire(&config_dir).expect("hold the lock");

        let lock_path = config_dir.join(RUNTIME_LOCK_FILE);
        let owner = read_owner_path(&lock_path).expect("the holder recorded itself");
        assert_eq!(owner.pid, std::process::id());

        // A refused start must leave the holder's record intact: writing before
        // acquiring would let a process that owns nothing claim the directory.
        RuntimeLock::acquire(&config_dir).expect_err("refused");
        let after = read_owner_path(&lock_path).expect("still the holder");
        assert_eq!(after, owner);

        drop(held);
    }

    #[cfg(unix)]
    #[test]
    fn replacing_the_diagnostic_file_does_not_replace_the_directory_lease() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let config_dir = dir.path().join(".spice");
        let held = RuntimeLock::acquire(&config_dir).expect("hold the directory lease");

        let lock_path = config_dir.join(RUNTIME_LOCK_FILE);
        std::fs::rename(&lock_path, config_dir.join("runtime.lock.displaced"))
            .expect("displace the diagnostic file");
        std::fs::write(&lock_path, b"replacement").expect("replace the diagnostic file");

        let error = RuntimeLock::acquire(&config_dir)
            .expect_err("a new file inode must not admit another runtime");
        assert!(matches!(error, Error::AlreadyRunning { .. }), "{error:?}");
        drop(held);
    }

    #[cfg(unix)]
    #[test]
    fn renaming_or_aliasing_the_config_directory_keeps_one_inode_lease() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let config_dir = dir.path().join(".spice");
        let held = RuntimeLock::acquire(&config_dir).expect("hold the directory lease");

        let moved = dir.path().join("moved-spice");
        std::fs::rename(&config_dir, &moved).expect("rename the config directory");
        let moved_error = RuntimeLock::acquire(&moved)
            .expect_err("the renamed directory is still the held inode");
        assert!(
            matches!(moved_error, Error::AlreadyRunning { .. }),
            "{moved_error:?}"
        );

        let alias = dir.path().join("spice-alias");
        std::os::unix::fs::symlink(&moved, &alias).expect("alias the config directory");
        let alias_error = RuntimeLock::acquire(&alias)
            .expect_err("a symlink alias must resolve to the held inode");
        assert!(
            matches!(alias_error, Error::AlreadyRunning { .. }),
            "{alias_error:?}"
        );
        drop(held);
    }

    #[test]
    fn an_unparseable_lock_file_reports_no_owner_and_still_starts() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let config_dir = dir.path().join(".spice");
        std::fs::create_dir_all(&config_dir).expect("create config dir");
        std::fs::write(config_dir.join(RUNTIME_LOCK_FILE), b"not json").expect("write garbage");

        assert!(read_owner_path(&config_dir.join(RUNTIME_LOCK_FILE)).is_none());
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
