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
//! is the policy for a runtime that has to keep running: a directory owned by
//! another live runtime is a refusal, while a lock file that cannot be created
//! at all is not. Read-only and ephemeral filesystems are ordinary places to
//! run `spiced` — a container image with no writable working directory, a
//! read-only root — and none of them contains a second runtime to collide
//! with. Refusing to start there would break deployments that are correct, to
//! prevent a conflict that cannot occur, so an unwritable directory degrades to
//! a warning and the start proceeds unclaimed.

use runtime_cloud_connect::{CloudConnectConfig, RuntimeLock};

/// This process's ownership of its instance directory, held until the process
/// exits.
///
/// The `None` case is a directory that could not be claimed for an I/O reason,
/// which is deliberately not fatal — see the module docs.
#[derive(Debug)]
pub struct InstanceClaim {
    /// Held, never read: the claim lives exactly as long as this value does.
    _lock: Option<RuntimeLock>,
}

/// Claim this process's instance directory.
///
/// # Errors
///
/// Returns the message to print when another live runtime already owns the
/// directory. The caller exits non-zero on it, having bound nothing and
/// connected nothing.
pub fn claim_instance_directory() -> Result<InstanceClaim, String> {
    claim(&CloudConnectConfig::default_config_dir())
}

fn claim(config_dir: &std::path::Path) -> Result<InstanceClaim, String> {
    match RuntimeLock::acquire(config_dir) {
        Ok(lock) => {
            tracing::debug!("Instance directory claimed ({})", lock.path().display());
            Ok(InstanceClaim { _lock: Some(lock) })
        }
        Err(err @ runtime_cloud_connect::runtime_lock::Error::AlreadyRunning { .. }) => {
            Err(err.to_string())
        }
        Err(err) => {
            tracing::warn!(
                "{err}. Starting anyway: this only leaves a second runtime in the same directory \
                 undetected, and nothing else depends on the lock."
            );
            Ok(InstanceClaim { _lock: None })
        }
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
    fn an_unwritable_config_directory_does_not_stop_the_runtime() {
        // A read-only working directory is an ordinary deployment and contains
        // no second runtime to collide with; refusing to start there would
        // break a correct deployment to prevent an impossible conflict.
        use std::os::unix::fs::PermissionsExt as _;

        let dir = tempfile::tempdir().expect("create tempdir");
        let readonly = dir.path().join("readonly");
        std::fs::create_dir(&readonly).expect("create the read-only parent");
        std::fs::set_permissions(&readonly, std::fs::Permissions::from_mode(0o500))
            .expect("make the parent read-only");

        let claimed = claim(&readonly.join(".spice"));

        std::fs::set_permissions(&readonly, std::fs::Permissions::from_mode(0o700))
            .expect("restore permissions so the tempdir can be cleaned up");
        claimed.expect("an unclaimable directory must not stop the runtime");
    }
}
