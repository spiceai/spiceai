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

//! Opening the lock file that serializes access to a Cloud Connect directory.
//!
//! Every one of these locks is opened relative to a directory descriptor the
//! caller already holds and has verified, never by pathname, so that no ancestor
//! symlink substituted after the check can move the lock out from under it.

/// Attempts a create that reports the entry missing may take before the report
/// is believed.
///
/// One further look has always been enough: the entry is already present when
/// the false report arrives. The bound is generous anyway, because an extra look
/// costs one syscall while believing a false report costs a failed lock
/// acquisition.
#[cfg(unix)]
const MISSING_ENTRY_ATTEMPTS: u32 = 16;

/// Create `name` inside `directory`, or open it if it is already there, as the
/// carrier for an advisory lock.
///
/// `O_NOFOLLOW` refuses a symlink in the lock's place rather than following it,
/// and `O_NONBLOCK` keeps the open itself from waiting — the advisory lock is
/// what a caller waits on.
///
/// # Concurrent creation
///
/// `O_CREAT` without `O_EXCL` has two correct answers, that the entry was
/// created or that the existing one was opened, and callers racing on one name
/// is the situation a lock file exists for. Darwin hands a losing racer neither:
/// it reports `ENOENT` for a *relative* name whose entry another thread or
/// process is creating, where the same race resolved through an absolute
/// pathname returns the open file, and where adding `O_EXCL` correctly reports
/// `EEXIST`. The entry is present by the time the report arrives, so the answer
/// is to look again.
///
/// The retry is bounded because a parent directory that really has been removed
/// reports `ENOENT` here too, and that has to stay reportable rather than spin.
///
/// # Errors
///
/// Returns the underlying `openat` error. `ENOENT` survives
/// [`MISSING_ENTRY_ATTEMPTS`] looks only when the entry genuinely cannot be
/// created.
#[cfg(unix)]
pub(crate) fn create_or_open_lock_at(
    directory: &std::fs::File,
    name: &std::ffi::CStr,
) -> std::io::Result<std::fs::File> {
    use std::os::fd::{AsRawFd as _, FromRawFd as _};

    let mut attempts = 0;
    loop {
        let descriptor = unsafe {
            libc::openat(
                directory.as_raw_fd(),
                name.as_ptr(),
                libc::O_RDWR
                    | libc::O_CREAT
                    | libc::O_CLOEXEC
                    | libc::O_NOFOLLOW
                    | libc::O_NONBLOCK,
                0o600,
            )
        };
        if descriptor >= 0 {
            return Ok(unsafe { std::fs::File::from_raw_fd(descriptor) });
        }
        let error = std::io::Error::last_os_error();
        attempts += 1;
        if error.kind() != std::io::ErrorKind::NotFound || attempts >= MISSING_ENTRY_ATTEMPTS {
            return Err(error);
        }
        std::thread::yield_now();
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    /// Every caller racing to create one lock file gets a descriptor for it.
    /// Serializing concurrent access is the whole purpose, so a caller that
    /// merely arrived second must never be told the lock does not exist.
    #[test]
    fn concurrent_callers_all_open_the_same_lock() {
        let directory = tempfile::tempdir().expect("create tempdir");
        let handle = std::fs::File::open(directory.path()).expect("open the directory");
        let name = c"contended.lock";
        let start = std::sync::Barrier::new(16);

        let opened = std::thread::scope(|scope| {
            let workers: Vec<_> = (0..16)
                .map(|_| {
                    scope.spawn(|| {
                        start.wait();
                        create_or_open_lock_at(&handle, name)
                    })
                })
                .collect();
            workers
                .into_iter()
                .map(|worker| worker.join().expect("the worker finishes"))
                .collect::<Vec<_>>()
        });

        for result in &opened {
            assert!(
                result.is_ok(),
                "a caller that arrived second must still open the lock: {:?}",
                result.as_ref().err()
            );
        }
        assert!(
            directory.path().join("contended.lock").is_file(),
            "the contended lock must be left as a regular file"
        );
    }

    /// A directory that is gone cannot carry a lock, and the retry must not turn
    /// that into a spin or a different error.
    #[test]
    fn a_removed_directory_still_reports_the_missing_entry() {
        let directory = tempfile::tempdir().expect("create tempdir");
        let handle = std::fs::File::open(directory.path()).expect("open the directory");
        // The descriptor outlives the name, which is exactly the state a
        // released instance directory leaves behind.
        std::fs::remove_dir_all(directory.path()).expect("remove the directory");

        let error = create_or_open_lock_at(&handle, c"orphaned.lock")
            .expect_err("a removed directory cannot carry a lock");
        assert_eq!(error.kind(), std::io::ErrorKind::NotFound, "{error}");
    }
}
