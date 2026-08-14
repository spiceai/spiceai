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
//! `<config-dir>/connect.lock` is held for a complete state-changing operation —
//! the enrollment and project transaction, a release — so two of them cannot
//! interleave their reads and writes of the identity, the enrollment draft, and
//! the journals. It is coarser and outer to
//! [`crate::EnrollmentTransactionLock`], which serializes one enrollment
//! operation: a caller takes this first and that second, never the reverse.
//!
//! It lives here, below the CLI, so that the runtime can take the same lock:
//! it reaches the same state through the control stream, and two exclusion
//! primitives over one directory would exclude nothing.
//!
//! What takes it today is the CLI — the `spice connect` transaction and
//! `spice connect remove`. The runtime's own mutation paths (the `--token`
//! bootstrap, and the command handlers that persist an app id or an attachment)
//! do not yet, so a runtime mutation can still interleave with a CLI
//! transaction. Moving them onto this lock is what this module exists for, and
//! it is deliberately not a change made from here: a control-stream handler that
//! blocks on a directory a long CLI transaction holds stalls the stream, so each
//! path has to adopt the lock with that in view. Until a path does, this module
//! is the one place the exclusion is defined rather than a second one.
//!
//! The lock is advisory and file-descriptor-scoped: the operating system drops
//! it when the holding process exits, so a crash cannot leave a directory
//! permanently locked. The file's contents (`pid=… action=…`) are diagnostics
//! for the waiter's error message, never authority — a lock is held because the
//! descriptor is locked, not because a file says so.

use std::fs::{File, OpenOptions, TryLockError};
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
}

fn acquire_lock(config_dir: &Path, action: &str, timeout: Duration) -> Result<MutationLock> {
    let config_dir = canonical_config_directory(config_dir)?;
    validate_existing_directory_chain(&config_dir)?;
    std::fs::create_dir_all(&config_dir).context(IoSnafu {
        path: config_dir.clone(),
    })?;
    validate_existing_directory_chain(&config_dir)?;
    let path = config_dir.join(MUTATION_LOCK_FILE);
    #[cfg(not(windows))]
    let mut options = OpenOptions::new();
    #[cfg(not(windows))]
    options.create(true).read(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options
            .mode(0o600)
            // Never follow an attacker-controlled repository symlink or block
            // on a FIFO/device before checking the descriptor below.
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    #[cfg(not(windows))]
    let mut file = options
        .open(&path)
        .context(IoSnafu { path: path.clone() })?;
    #[cfg(windows)]
    let mut file =
        open_windows_owner_only_lock_file(&path).context(IoSnafu { path: path.clone() })?;
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
        file.set_permissions(std::fs::Permissions::from_mode(0o600))
            .context(IoSnafu { path: path.clone() })?;
    }
    #[cfg(windows)]
    crate::identity::validate_windows_regular_single_link(&file)
        .context(IoSnafu { path: path.clone() })?;
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
    crate::identity::sync_parent_directory(&path).context(IoSnafu { path })?;
    Ok(MutationLock { _file: file })
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

#[cfg(windows)]
fn open_windows_owner_only_lock_file(path: &Path) -> std::io::Result<File> {
    use std::os::windows::ffi::OsStrExt as _;
    use std::os::windows::io::{FromRawHandle as _, RawHandle};
    use windows_sys::Win32::Foundation::{CloseHandle, INVALID_HANDLE_VALUE, LocalFree};
    use windows_sys::Win32::Security::Authorization::{
        ConvertStringSecurityDescriptorToSecurityDescriptorW, SDDL_REVISION_1, SE_FILE_OBJECT,
        SetSecurityInfo,
    };
    use windows_sys::Win32::Security::{
        DACL_SECURITY_INFORMATION, GetSecurityDescriptorDacl, PROTECTED_DACL_SECURITY_INFORMATION,
        SECURITY_ATTRIBUTES,
    };
    use windows_sys::Win32::Storage::FileSystem::{
        CreateFileW, FILE_ATTRIBUTE_NORMAL, FILE_FLAG_OPEN_REPARSE_POINT, FILE_GENERIC_READ,
        FILE_GENERIC_WRITE, FILE_SHARE_READ, FILE_SHARE_WRITE, OPEN_ALWAYS,
    };

    let path = path
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    // Protected DACL granting generic-all only to the file owner. Passing it
    // at creation avoids a window in which a newly-created repository-local
    // lock inherits broader directory permissions.
    let sddl = "D:P(A;;GA;;;OW)"
        .encode_utf16()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    let mut descriptor = std::ptr::null_mut();
    let converted = unsafe {
        ConvertStringSecurityDescriptorToSecurityDescriptorW(
            sddl.as_ptr(),
            SDDL_REVISION_1,
            &mut descriptor,
            std::ptr::null_mut(),
        )
    };
    if converted == 0 {
        return Err(std::io::Error::last_os_error());
    }
    let mut security = SECURITY_ATTRIBUTES {
        nLength: u32::try_from(std::mem::size_of::<SECURITY_ATTRIBUTES>()).unwrap_or(u32::MAX),
        lpSecurityDescriptor: descriptor,
        bInheritHandle: 0,
    };
    let handle = unsafe {
        CreateFileW(
            path.as_ptr(),
            FILE_GENERIC_READ | FILE_GENERIC_WRITE,
            FILE_SHARE_READ | FILE_SHARE_WRITE,
            &mut security,
            OPEN_ALWAYS,
            FILE_ATTRIBUTE_NORMAL | FILE_FLAG_OPEN_REPARSE_POINT,
            std::ptr::null_mut(),
        )
    };
    if handle == INVALID_HANDLE_VALUE {
        unsafe {
            let _ = LocalFree(descriptor.cast());
        }
        return Err(std::io::Error::last_os_error());
    }
    let mut dacl_present = 0;
    let mut dacl = std::ptr::null_mut();
    let mut dacl_defaulted = 0;
    let got_dacl = unsafe {
        GetSecurityDescriptorDacl(
            descriptor,
            &mut dacl_present,
            &mut dacl,
            &mut dacl_defaulted,
        )
    };
    if got_dacl == 0 || dacl_present == 0 {
        let source = if got_dacl == 0 {
            std::io::Error::last_os_error()
        } else {
            std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "owner-only lock descriptor did not contain a DACL",
            )
        };
        unsafe {
            let _ = LocalFree(descriptor.cast());
            let _ = CloseHandle(handle);
        }
        return Err(source);
    }
    let acl_result = unsafe {
        SetSecurityInfo(
            handle,
            SE_FILE_OBJECT,
            DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            dacl,
            std::ptr::null_mut(),
        )
    };
    unsafe {
        let _ = LocalFree(descriptor.cast());
    }
    if acl_result != 0 {
        unsafe {
            let _ = CloseHandle(handle);
        }
        return Err(std::io::Error::from_raw_os_error(
            i32::try_from(acl_result).unwrap_or(i32::MAX),
        ));
    }
    Ok(unsafe { File::from_raw_handle(handle as RawHandle) })
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
}
