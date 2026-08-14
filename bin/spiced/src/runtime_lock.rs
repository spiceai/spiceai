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

//! This process's claim on its instance directory.
//!
//! Taken before the `--token` bootstrap, before any listener binds, and before
//! the control stream dials, because each of those is a side effect a second
//! runtime in one directory must not have: it would redeem a key against an
//! identity its sibling already holds, fail a bind after the control plane has
//! seen two sessions, or answer commands addressed to the other process.
//!
//! The claim is [`runtime_cloud_connect::RuntimeLock`]; what this module adds
//! is the policy for a runtime that has to keep running.
//!
//! A directory owned by another live runtime is a refusal. So is a lock this
//! process cannot take for any *specific* reason — a lock file it may not open,
//! a symlink or device left in its place, a full disk — because every one of
//! those can coexist with a live runtime holding the real lock, and starting
//! anyway would quietly void the one-runtime guarantee exactly where it is
//! being attacked.
//!
//! The single exception is a read-only filesystem. Nothing there can take the
//! lock, and nothing there can change the instance state either, so the
//! exclusion is equally unavailable to every process and there is no conflict
//! to detect. A container image with a read-only root is an ordinary way to run
//! `spiced`, so that case degrades to a warning and the start proceeds
//! unclaimed rather than breaking a correct deployment.

use runtime_cloud_connect::{CloudConnectConfig, RuntimeLock};

/// This process's ownership of its instance directory, held until the process
/// exits.
///
/// The `None` case is a read-only filesystem, where the claim is unavailable to
/// every process alike — see the module docs.
#[derive(Debug)]
pub struct InstanceClaim {
    /// Held, never read: the claim lives exactly as long as this value does.
    _lock: Option<RuntimeLock>,
}

/// Claim this process's instance directory.
///
/// # Errors
///
/// Returns the message to print when the directory cannot be claimed: another
/// live runtime owns it, or the lock itself could not be taken for a reason
/// that leaves a conflict possible. The caller exits non-zero on it, having
/// bound nothing and connected nothing.
pub fn claim_instance_directory() -> Result<InstanceClaim, String> {
    claim(&CloudConnectConfig::default_config_dir())
}

fn claim(config_dir: &std::path::Path) -> Result<InstanceClaim, String> {
    match RuntimeLock::acquire(config_dir) {
        Ok(lock) => {
            tracing::debug!("Instance directory claimed ({})", lock.path().display());
            Ok(InstanceClaim { _lock: Some(lock) })
        }
        // Nothing on a read-only filesystem can hold the lock, or change the
        // state the lock protects, so there is no exclusion to lose.
        Err(err) if err.is_read_only_filesystem() => {
            tracing::warn!(
                "{err}. Starting anyway: on a read-only filesystem no runtime can take this lock \
                 and none can change this directory's state, so there is nothing for a second one \
                 to corrupt."
            );
            Ok(InstanceClaim { _lock: None })
        }
        // Everything else, including a lock file this process may not open and
        // anything that is not the regular file the lock must be: a live
        // runtime may well be holding the real lock, so starting would void the
        // guarantee precisely where something is interfering with it.
        Err(err) => Err(format!(
            "{err}. Refusing to start: a lock that cannot be taken cannot rule out a second \
             runtime in this directory. Fix the lock file's ownership, type, or permissions — or \
             point this runtime at another instance directory with SPICE_CONFIG_DIR. \
             See: https://spiceai.org/docs"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_second_runtime_in_one_directory_is_refused_with_actionable_guidance() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let config_dir = dir.path().join(".spice");

        let held = claim(&config_dir).expect("the first runtime claims the directory");
        let message = claim(&config_dir).expect_err("the second runtime must be refused");

        assert!(
            message.contains("another runtime is already running"),
            "{message}"
        );
        // The guidance has to be the canonical way out, not a description of
        // the lock file.
        assert!(message.contains("SPICE_CONFIG_DIR"), "{message}");
        assert!(message.contains("https://spiceai.org/docs"), "{message}");

        drop(held);
        claim(&config_dir).expect("the directory is free once the holder exits");
    }

    #[cfg(unix)]
    #[test]
    fn a_lock_this_process_may_not_open_fails_closed() {
        // Another user's owner-only lock file is what a live runtime under a
        // different account looks like from here. Starting anyway would put a
        // second runtime on that instance directory — the exact outcome the
        // lock exists to prevent — so this refuses instead of degrading.
        use std::os::unix::fs::PermissionsExt as _;

        let dir = tempfile::tempdir().expect("create tempdir");
        let config_dir = dir.path().join(".spice");
        std::fs::create_dir_all(&config_dir).expect("create the config dir");
        let lock_path = config_dir.join(runtime_cloud_connect::runtime_lock::RUNTIME_LOCK_FILE);
        std::fs::write(&lock_path, b"{}").expect("create the lock file");
        std::fs::set_permissions(&lock_path, std::fs::Permissions::from_mode(0o000))
            .expect("make the lock file unopenable");

        let claimed = claim(&config_dir);

        std::fs::set_permissions(&lock_path, std::fs::Permissions::from_mode(0o600))
            .expect("restore permissions so the tempdir can be cleaned up");

        // Root opens anything, so the refusal is only observable unprivileged;
        // the assertion below is skipped rather than inverted, because running
        // as root is not the case under test.
        if !is_root() {
            let message = claimed.expect_err("an unopenable lock must not be started past");
            assert!(message.contains("Refusing to start"), "{message}");
            assert!(message.contains("SPICE_CONFIG_DIR"), "{message}");
        }
    }

    #[cfg(unix)]
    #[test]
    fn a_symlinked_lock_file_fails_closed() {
        // A symlink in place of the lock is someone redirecting exclusion at
        // another file; the guarantee cannot be assumed to hold.
        let dir = tempfile::tempdir().expect("create tempdir");
        let config_dir = dir.path().join(".spice");
        std::fs::create_dir_all(&config_dir).expect("create the config dir");
        let elsewhere = dir.path().join("elsewhere.lock");
        std::fs::write(&elsewhere, b"{}").expect("create the redirect target");
        std::os::unix::fs::symlink(
            &elsewhere,
            config_dir.join(runtime_cloud_connect::runtime_lock::RUNTIME_LOCK_FILE),
        )
        .expect("redirect the lock path");

        let message = claim(&config_dir).expect_err("a redirected lock must not be started past");
        assert!(message.contains("Refusing to start"), "{message}");
    }

    #[cfg(unix)]
    fn is_root() -> bool {
        // SAFETY: `geteuid` reads the caller's effective uid and has no
        // preconditions, no failure mode, and no side effects.
        unsafe { libc::geteuid() == 0 }
    }
}
