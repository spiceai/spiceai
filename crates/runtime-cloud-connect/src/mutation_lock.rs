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

//! The mutation lock that serializes changes to one instance directory's Cloud
//! Connect state.
//!
//! `<config-dir>/connect.lock` is the common primitive later lifecycle layers
//! use to cover a complete state-changing operation, such as the interactive
//! enrollment/project transaction or a release. It is coarser and outer to
//! [`crate::EnrollmentTransactionLock`], which already serializes one enrollment
//! operation in this foundation layer: once a caller needs both, it takes this
//! lock first and the enrollment lock second, never the reverse.
//!
//! It lives here, below the CLI, so that the runtime can take the same lock:
//! it reaches the same state through the control stream, and two exclusion
//! primitives over one directory would exclude nothing.
//!
//! The lock is advisory and file-descriptor-scoped: the operating system drops
//! it when the holding process exits, so a crash cannot leave a directory
//! permanently locked. The file's contents (`pid=… action=…`) are diagnostics
//! for the waiter's error message, never authority — a lock is held because the
//! descriptor is locked, not because a file says so.

use std::fs::{File, TryLockError};
use std::io::{Read as _, Seek as _, Write as _};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use snafu::{ResultExt as _, Snafu};

/// File (relative to the config dir) holding the mutation lock.
pub const MUTATION_LOCK_FILE: &str = "connect.lock";

/// How long a caller waits for the directory before reporting who holds it.
pub const MUTATION_LOCK_TIMEOUT: Duration = Duration::from_secs(30);

const LOCK_RETRY_INTERVAL: Duration = Duration::from_millis(50);

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to access Cloud Connect state at {}: {source}", path.display()))]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display(
        "Cloud Connect state for this directory is already being changed{owner}. Wait for that command to finish, then retry."
    ))]
    LockTimeout { owner: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Held for the complete state-changing operation on one instance directory.
///
/// Dropping it releases the directory. Nothing about the lock is stored beyond
/// the open descriptor, so a holder that exits — cleanly or not — releases it.
#[derive(Debug)]
pub struct MutationLock {
    _file: File,
    config_dir: PathBuf,
    /// Retains the exact directory inode used to open the lock. Protected
    /// paths are rooted through this descriptor so a later pathname
    /// substitution cannot redirect an operation to another instance.
    #[cfg(unix)]
    directory: File,
}

impl MutationLock {
    /// Take the directory for `action`, waiting up to
    /// [`MUTATION_LOCK_TIMEOUT`].
    ///
    /// # Errors
    ///
    /// Returns [`Error::LockTimeout`] when another operation still holds the
    /// directory, naming the holder when its diagnostics are readable, and
    /// [`Error::Io`] when the lock file cannot be opened safely.
    pub async fn acquire(config_dir: &Path, action: &'static str) -> Result<Self> {
        Self::acquire_with_timeout(config_dir, action, MUTATION_LOCK_TIMEOUT).await
    }

    /// [`MutationLock::acquire`] with an explicit wait budget.
    ///
    /// # Errors
    ///
    /// As [`MutationLock::acquire`].
    pub async fn acquire_with_timeout(
        config_dir: &Path,
        action: &'static str,
        timeout: Duration,
    ) -> Result<Self> {
        let config_dir = config_dir.to_path_buf();
        let lock_path = config_dir.join(MUTATION_LOCK_FILE);
        // Blocking file I/O, and a wait that can last the whole timeout, so it
        // never occupies an async worker thread.
        tokio::task::spawn_blocking(move || acquire_lock(&config_dir, action, timeout))
            .await
            .map_err(|source| Error::Io {
                path: lock_path,
                source: std::io::Error::other(format!("lock task panicked: {source}")),
            })?
    }

    /// Return the canonical config directory protected by this lock after
    /// verifying that its pathname still names the retained directory inode.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the directory was replaced while the lock was
    /// held.
    pub fn config_dir(&self) -> Result<&Path> {
        self.ensure_directory_stable()?;
        Ok(&self.config_dir)
    }

    /// A path whose directory component resolves through the retained
    /// descriptor rather than by looking up the original pathname again.
    ///
    /// On Linux, `/proc/self/fd` keeps child lookups rooted at the inode this
    /// guard opened even if its canonical name is replaced. Other platforms use
    /// the verified canonical path because `/dev/fd` does not provide Linux's
    /// directory traversal semantics.
    ///
    /// # Errors
    ///
    /// Returns an I/O error when the retained descriptor cannot be resolved to
    /// the directory inode acquired with the lock.
    pub fn descriptor_relative_config_dir(&self) -> Result<PathBuf> {
        #[cfg(target_os = "linux")]
        {
            use std::os::fd::AsRawFd as _;
            use std::os::unix::fs::MetadataExt as _;

            let path = PathBuf::from(format!("/proc/self/fd/{}", self.directory.as_raw_fd()));

            let retained = self.directory.metadata().context(IoSnafu {
                path: self.config_dir.clone(),
            })?;
            let descriptor_path =
                std::fs::metadata(&path).context(IoSnafu { path: path.clone() })?;
            if retained.dev() != descriptor_path.dev() || retained.ino() != descriptor_path.ino() {
                return Err(Error::Io {
                    path,
                    source: std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "the retained Cloud Connect directory descriptor resolved to another inode",
                    ),
                });
            }
            Ok(path)
        }

        #[cfg(not(target_os = "linux"))]
        {
            self.ensure_directory_stable()?;
            Ok(self.config_dir.clone())
        }
    }

    /// Verify that the retained directory inode is still the one named by the
    /// canonical path before a caller derives a protected state path from it.
    ///
    /// # Errors
    ///
    /// Returns an I/O error when an ancestor or the directory itself was
    /// renamed/replaced while this lock was held.
    pub fn ensure_directory_stable(&self) -> Result<()> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt as _;

            let retained = self.directory.metadata().context(IoSnafu {
                path: self.config_dir.clone(),
            })?;
            let named = std::fs::metadata(&self.config_dir).context(IoSnafu {
                path: self.config_dir.clone(),
            })?;
            let final_entry = std::fs::symlink_metadata(&self.config_dir).context(IoSnafu {
                path: self.config_dir.clone(),
            })?;
            if final_entry.file_type().is_symlink()
                || retained.dev() != named.dev()
                || retained.ino() != named.ino()
            {
                return Err(Error::Io {
                    path: self.config_dir.clone(),
                    source: std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "the locked Cloud Connect config directory was renamed or replaced",
                    ),
                });
            }
        }
        Ok(())
    }
}

fn acquire_lock(config_dir: &Path, action: &str, timeout: Duration) -> Result<MutationLock> {
    let config_dir = canonical_config_directory(config_dir)?;
    validate_existing_directory_chain(&config_dir)?;
    #[cfg(unix)]
    if unsafe { libc::geteuid() } == 0
        && std::fs::symlink_metadata(&config_dir)
            .is_err_and(|source| source.kind() == std::io::ErrorKind::NotFound)
    {
        return Err(Error::Io {
            path: config_dir,
            source: std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "a privileged Cloud Connect mutation cannot create the config directory; enroll as the intended service account first",
            ),
        });
    }
    std::fs::create_dir_all(&config_dir).context(IoSnafu {
        path: config_dir.clone(),
    })?;
    validate_existing_directory_chain(&config_dir)?;
    let path = config_dir.join(MUTATION_LOCK_FILE);
    #[cfg(unix)]
    let (mut file, lock_directory) = open_unix_lock_file(&config_dir, &path)?;
    #[cfg(not(unix))]
    let mut options = std::fs::OpenOptions::new();
    #[cfg(not(unix))]
    options.create(true).read(true).write(true);
    #[cfg(not(unix))]
    let mut file = options
        .open(&path)
        .context(IoSnafu { path: path.clone() })?;
    let metadata = file.metadata().context(IoSnafu { path: path.clone() })?;
    if !metadata.is_file() {
        return Err(Error::Io {
            path,
            source: std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "the Cloud Connect mutation lock must be a regular file",
            ),
        });
    }
    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd as _;
        use std::os::unix::fs::MetadataExt as _;
        use std::os::unix::fs::PermissionsExt as _;
        if metadata.nlink() != 1 {
            return Err(Error::Io {
                path,
                source: std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "the Cloud Connect mutation lock must not be hard-linked",
                ),
            });
        }
        let directory_metadata = lock_directory
            .metadata()
            .context(IoSnafu { path: path.clone() })?;
        let effective_uid = unsafe { libc::geteuid() };
        if effective_uid != 0 && effective_uid != directory_metadata.uid() {
            return Err(Error::Io {
                path,
                source: std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "the Cloud Connect mutation lock must be acquired by the config-directory owner",
                ),
            });
        }
        if metadata.uid() != directory_metadata.uid() {
            if effective_uid != 0 {
                return Err(Error::Io {
                    path,
                    source: std::io::Error::new(
                        std::io::ErrorKind::PermissionDenied,
                        "the Cloud Connect mutation lock is not owned by the config-directory owner",
                    ),
                });
            }
            let result = unsafe {
                libc::fchown(
                    file.as_raw_fd(),
                    directory_metadata.uid(),
                    directory_metadata.gid(),
                )
            };
            if result != 0 {
                return Err(Error::Io {
                    path,
                    source: std::io::Error::last_os_error(),
                });
            }
        }
        file.set_permissions(std::fs::Permissions::from_mode(0o600))
            .context(IoSnafu { path: path.clone() })?;
    }
    let started = Instant::now();

    loop {
        match file.try_lock() {
            Ok(()) => break,
            Err(TryLockError::WouldBlock) if started.elapsed() < timeout => {
                std::thread::sleep(
                    LOCK_RETRY_INTERVAL.min(timeout.saturating_sub(started.elapsed())),
                );
            }
            Err(TryLockError::WouldBlock) => {
                let owner = lock_owner_suffix(&mut file);
                return Err(Error::LockTimeout { owner });
            }
            Err(TryLockError::Error(source)) => {
                return Err(Error::Io { path, source });
            }
        }
    }

    file.set_len(0).context(IoSnafu { path: path.clone() })?;
    file.rewind().context(IoSnafu { path: path.clone() })?;
    writeln!(file, "pid={} action={action}", std::process::id())
        .context(IoSnafu { path: path.clone() })?;
    file.sync_data().context(IoSnafu { path: path.clone() })?;
    #[cfg(unix)]
    lock_directory.sync_all().context(IoSnafu { path })?;
    #[cfg(not(unix))]
    crate::identity::sync_parent_directory(&path).context(IoSnafu { path })?;
    Ok(MutationLock {
        _file: file,
        config_dir,
        #[cfg(unix)]
        directory: lock_directory,
    })
}

/// Open the canonical directory one component at a time without following
/// symlinks, then create/open the lock relative to that held descriptor.
///
/// `O_NOFOLLOW` on the lock alone protects only its final name. Guarded
/// descriptor traversal makes an ancestor rename/symlink substitution either
/// irrelevant (the already-open directory remains authoritative) or an error,
/// including in a privileged installer.
#[cfg(unix)]
fn open_unix_lock_file(config_dir: &Path, path: &Path) -> Result<(File, File)> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt as _;
    use std::os::unix::io::{AsRawFd as _, FromRawFd as _};

    fn open_directory_at(parent: &File, name: &std::ffi::OsStr) -> std::io::Result<File> {
        let name = CString::new(name.as_bytes()).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Cloud Connect config paths cannot contain NUL bytes",
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

    let root = c"/";
    let root_fd = unsafe {
        libc::open(
            root.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC,
        )
    };
    if root_fd < 0 {
        return Err(Error::Io {
            path: config_dir.to_path_buf(),
            source: std::io::Error::last_os_error(),
        });
    }
    let mut directory = unsafe { File::from_raw_fd(root_fd) };
    for component in config_dir.components() {
        match component {
            std::path::Component::RootDir | std::path::Component::CurDir => {}
            std::path::Component::Normal(name) => {
                directory = open_directory_at(&directory, name).context(IoSnafu {
                    path: config_dir.to_path_buf(),
                })?;
            }
            std::path::Component::ParentDir | std::path::Component::Prefix(_) => {
                return Err(Error::Io {
                    path: config_dir.to_path_buf(),
                    source: std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "the canonical Cloud Connect config path was not absolute",
                    ),
                });
            }
        }
    }

    let lock = crate::lock_file::create_or_open_lock_at(&directory, c"connect.lock").map_err(
        |source| Error::Io {
            path: path.to_path_buf(),
            source,
        },
    )?;
    Ok((lock, directory))
}

/// Resolve every existing component of a config path before creating or
/// opening its mutation lock.
///
/// This deliberately accepts a symlink supplied as the instance directory or
/// by `SPICE_CONFIG_DIR`, but reduces it to the same physical directory as its
/// target. Missing tail components are appended only after the nearest
/// existing ancestor has been canonicalized, so the directory-chain checks
/// below never reject a legitimate symlinked ancestor or create a second lock
/// through a path alias.
fn canonical_config_directory(path: &Path) -> Result<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map(|cwd| cwd.join(path))
            .context(IoSnafu {
                path: path.to_path_buf(),
            })?
    };
    let mut cursor = absolute.as_path();
    let mut missing = Vec::new();
    loop {
        match std::fs::symlink_metadata(cursor) {
            Ok(_) => {
                let mut resolved = std::fs::canonicalize(cursor).context(IoSnafu {
                    path: cursor.to_path_buf(),
                })?;
                if !resolved.is_dir() {
                    return Err(Error::Io {
                        path: cursor.to_path_buf(),
                        source: std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "the Cloud Connect config path must resolve to a directory",
                        ),
                    });
                }
                for component in missing.iter().rev() {
                    resolved.push(component);
                }
                return Ok(resolved);
            }
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
                let Some(name) = cursor.file_name() else {
                    return Err(Error::Io {
                        path: absolute,
                        source,
                    });
                };
                missing.push(name.to_os_string());
                let Some(parent) = cursor.parent() else {
                    return Err(Error::Io {
                        path: absolute,
                        source,
                    });
                };
                cursor = parent;
            }
            Err(source) => {
                return Err(Error::Io {
                    path: cursor.to_path_buf(),
                    source,
                });
            }
        }
    }
}

fn validate_existing_directory_chain(path: &Path) -> Result<()> {
    let mut ancestors = path.ancestors().collect::<Vec<_>>();
    ancestors.reverse();
    for ancestor in ancestors {
        let metadata = match std::fs::symlink_metadata(ancestor) {
            Ok(metadata) => metadata,
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => continue,
            Err(source) => {
                return Err(Error::Io {
                    path: ancestor.to_path_buf(),
                    source,
                });
            }
        };
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(Error::Io {
                path: ancestor.to_path_buf(),
                source: std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "the Cloud Connect config path must contain only real directories",
                ),
            });
        }
    }
    Ok(())
}

/// The most of the lock file that is ever read to describe its holder.
///
/// The file belongs to whoever holds the lock, so formatting a timeout message
/// must not size an allocation from it. The bound is on the read itself and not
/// on the truncation that follows: reading the whole file first would let a large
/// payload cost memory before a single character was discarded.
const LOCK_DIAGNOSTICS_BYTES: u64 = 1024;

/// How much of the sanitized text reaches the message.
const LOCK_DIAGNOSTICS_CHARS: usize = 160;

fn lock_owner_suffix(file: &mut File) -> String {
    let owner = read_bounded_lock_diagnostics(file)
        .chars()
        .filter(|character| !character.is_control())
        .take(LOCK_DIAGNOSTICS_CHARS)
        .collect::<String>();
    if owner.is_empty() {
        return String::new();
    }
    format!(" ({owner})")
}

/// Read at most [`LOCK_DIAGNOSTICS_BYTES`] of `file` from the start.
///
/// Lossy rather than strict UTF-8 because these bytes are diagnostics: a lock
/// file someone filled with anything at all should still produce a message,
/// bounded and stripped, rather than none.
fn read_bounded_lock_diagnostics(file: &mut File) -> String {
    if file.rewind().is_err() {
        return String::new();
    }
    let mut bytes = Vec::new();
    if file
        .take(LOCK_DIAGNOSTICS_BYTES)
        .read_to_end(&mut bytes)
        .is_err()
    {
        return String::new();
    }
    String::from_utf8_lossy(&bytes).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The lock is the directory's exclusion: while one operation holds it, the
    /// next waits and then reports who has it; once it is dropped, the directory
    /// is free. Everything that changes Cloud Connect state depends on this, so
    /// it is asserted directly rather than through a caller.
    #[tokio::test]
    async fn one_holder_at_a_time_and_the_waiter_is_told_who() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let config_dir = dir.path().join(".spice");

        let held = MutationLock::acquire_with_timeout(
            &config_dir,
            "first-action",
            Duration::from_millis(50),
        )
        .await
        .expect("the first operation takes the directory");

        let error = MutationLock::acquire_with_timeout(
            &config_dir,
            "second-action",
            Duration::from_millis(50),
        )
        .await
        .expect_err("a second operation must not change the same directory at the same time");
        let rendered = error.to_string();
        assert!(
            matches!(error, Error::LockTimeout { .. }),
            "unexpected failure: {rendered}"
        );
        assert!(
            rendered.contains("first-action")
                && rendered.contains("pid=")
                && rendered.contains("already being changed"),
            "the waiter should be told which action holds the directory: {rendered}"
        );

        drop(held);
        MutationLock::acquire_with_timeout(&config_dir, "third-action", Duration::from_millis(500))
            .await
            .expect("a released directory is available again");
    }

    /// The lock file's contents are diagnostics, so an unreadable or hostile
    /// payload must not become the error message or block the acquisition.
    #[tokio::test]
    async fn lock_diagnostics_are_bounded_and_stripped_of_control_characters() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let config_dir = dir.path().join(".spice");
        std::fs::create_dir_all(&config_dir).expect("create config dir");
        std::fs::write(
            config_dir.join(MUTATION_LOCK_FILE),
            format!("pid=1 action={}\n\u{1b}[31m", "a".repeat(400)),
        )
        .expect("write hostile lock diagnostics");

        let held =
            MutationLock::acquire_with_timeout(&config_dir, "first", Duration::from_millis(50))
                .await
                .expect("a stale payload does not own the directory");
        let error =
            MutationLock::acquire_with_timeout(&config_dir, "second", Duration::from_millis(50))
                .await
                .expect_err("the directory is held");
        let rendered = error.to_string();
        assert!(
            !rendered.contains('\u{1b}'),
            "control characters must not reach the message: {rendered}"
        );
        assert!(
            rendered.len() < 400,
            "the diagnostics must stay bounded: {rendered}"
        );
        drop(held);
    }

    /// The lock file is opened without following links, so a link planted where
    /// it belongs cannot make the lock write through to another file.
    /// The bound is on the read. A lock file far larger than the message could
    /// ever carry must not be pulled into memory to describe its holder.
    #[test]
    fn lock_diagnostics_read_at_most_their_bound() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let path = dir.path().join("large.lock");
        let payload = "a".repeat(4 * 1024 * 1024);
        std::fs::write(&path, &payload).expect("write a large lock file");
        let mut file = std::fs::File::open(&path).expect("open the large lock file");

        let read = read_bounded_lock_diagnostics(&mut file);
        assert!(
            read.len() as u64 <= LOCK_DIAGNOSTICS_BYTES,
            "the read itself must be bounded, not only the message: {} bytes",
            read.len()
        );
        assert_eq!(
            lock_owner_suffix(&mut file).chars().count(),
            LOCK_DIAGNOSTICS_CHARS + 3,
            "the message carries the bounded text inside its parentheses"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_symlink_never_becomes_the_lock() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().expect("create tempdir");
        let target = dir.path().join("important.txt");
        std::fs::write(&target, "must remain unchanged").expect("write the target");
        let config_dir = dir.path().join(".spice");
        std::fs::create_dir_all(&config_dir).expect("create config dir");
        symlink(&target, config_dir.join(MUTATION_LOCK_FILE)).expect("plant the link");

        MutationLock::acquire_with_timeout(&config_dir, "connect", Duration::from_millis(50))
            .await
            .expect_err("a symlink must not become the mutation lock");
        assert_eq!(
            std::fs::read_to_string(&target).expect("read the target"),
            "must remain unchanged",
            "the lock must not write through a link"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_symlinked_config_ancestor_resolves_to_one_lock() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().expect("create tempdir");
        let physical = dir.path().join("physical");
        std::fs::create_dir_all(physical.join(".spice")).expect("create physical config dir");
        let alias = dir.path().join("alias");
        symlink(&physical, &alias).expect("create the ancestor symlink");

        let held = MutationLock::acquire_with_timeout(
            &physical.join(".spice"),
            "physical",
            Duration::from_millis(50),
        )
        .await
        .expect("take the directory through its physical path");
        let error = MutationLock::acquire_with_timeout(
            &alias.join(".spice"),
            "alias",
            Duration::from_millis(50),
        )
        .await
        .expect_err("an aliased path must reach the same lock, not a second one");
        assert!(matches!(error, Error::LockTimeout { .. }), "{error}");
        drop(held);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn destructive_paths_remain_on_the_locked_inode_after_path_substitution() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().expect("create tempdir");
        let config_dir = dir.path().join(".spice");
        let moved_config_dir = dir.path().join("moved-spice");
        let victim_dir = dir.path().join("another-instance");
        std::fs::create_dir_all(&config_dir).expect("create locked config dir");
        std::fs::create_dir_all(&victim_dir).expect("create victim config dir");
        std::fs::write(config_dir.join("identity.json"), "locked").expect("write locked identity");
        std::fs::write(victim_dir.join("identity.json"), "victim").expect("write victim identity");

        let held = MutationLock::acquire(&config_dir, "remove")
            .await
            .expect("lock the original config directory");
        let protected = held
            .descriptor_relative_config_dir()
            .expect("derive a descriptor-relative config path");

        std::fs::rename(&config_dir, &moved_config_dir).expect("rename the locked directory");
        symlink(&victim_dir, &config_dir).expect("substitute the old pathname");

        std::fs::remove_file(protected.join("identity.json"))
            .expect("remove state through the retained directory descriptor");
        assert!(
            !moved_config_dir.join("identity.json").exists(),
            "the locked instance is the deletion target"
        );
        assert_eq!(
            std::fs::read_to_string(victim_dir.join("identity.json"))
                .expect("read the untouched victim identity"),
            "victim",
            "replacing the old pathname must not redirect a privileged deletion"
        );
    }
}
