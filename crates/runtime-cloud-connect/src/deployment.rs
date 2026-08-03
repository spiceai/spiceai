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

//! Which deployment's spicepod this instance has applied.
//!
//! A deployment applies by persisting the spicepod and restarting the process,
//! so the `CommandResult` cannot be what tells the control plane the deployment
//! landed — the stream drops mid-apply. What the instance reports on its next
//! `Hello` is (`Hello.applied_deployment_version`), and this file is what
//! carries the answer across the restart.
//!
//! # Format
//!
//! One JSON document beside the cloud-managed spicepod it describes:
//!
//! ```json
//! {
//!   "format_version": 1,
//!   "deployment_version": 42,
//!   "applied_at_unix": 1754006400
//! }
//! ```
//!
//! `deployment_version` is null when the dispatch that wrote the spicepod
//! assigned no version. Null is not zero: a zero would let a deployment resolve
//! against a version nothing claimed.
//!
//! Nothing here is secret — the file records a version number and a timestamp —
//! but it is written through the same atomic, owner-only path as the identity so
//! a reader never sees it half-written.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};

/// File name (relative to the config dir) of the applied-deployment record.
pub const DEPLOYMENT_STATE_FILE: &str = "deployment-state.json";

/// The only format version this build writes, and the only one it reads.
pub const FORMAT_VERSION: u32 = 1;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to read the deployment record at {}: {source}", path.display()))]
    Read {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Failed to write the deployment record at {}: {source}", path.display()))]
    Write {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display(
        "The deployment record at {} is not valid JSON: {source}. Discarding it; this instance \
         reports no applied deployment until the next deployment rewrites it.",
        path.display()
    ))]
    Malformed {
        path: PathBuf,
        source: serde_json::Error,
    },

    #[snafu(display(
        "The deployment record at {} is format version {found}, but this runtime writes version \
         {FORMAT_VERSION}. Discarding it; this instance reports no applied deployment until the \
         next deployment rewrites it.",
        path.display()
    ))]
    UnsupportedVersion { path: PathBuf, found: u32 },

    #[snafu(display("Failed to encode the deployment record: {source}"))]
    Encode { source: serde_json::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// The deployment whose spicepod is persisted at
/// [`crate::config::CLOUD_MANAGED_SPICEPOD_FILE`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppliedDeployment {
    format_version: u32,
    /// `None` when the dispatch assigned no version — the instance applied the
    /// spicepod but cannot name a deployment for it.
    pub deployment_version: Option<u64>,
    /// When the record was written, for an operator reading the config dir.
    pub applied_at_unix: u64,
}

/// Where the record lives for `config_dir`.
#[must_use]
pub fn path(config_dir: &Path) -> PathBuf {
    config_dir.join(DEPLOYMENT_STATE_FILE)
}

/// Read the record, returning `Ok(None)` when the instance has never applied a
/// deployment.
///
/// # Errors
///
/// Returns [`Error::Read`], [`Error::Malformed`], or
/// [`Error::UnsupportedVersion`]. Every one is a discard rather than a crash for
/// the caller — see [`read_version`], which is the form most callers want.
pub fn read(config_dir: &Path) -> Result<Option<AppliedDeployment>> {
    let path = path(config_dir);
    let bytes = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => return Err(Error::Read { path, source }),
    };

    let record: AppliedDeployment =
        serde_json::from_slice(&bytes).context(MalformedSnafu { path: path.clone() })?;
    snafu::ensure!(
        record.format_version == FORMAT_VERSION,
        UnsupportedVersionSnafu {
            path,
            found: record.format_version,
        }
    );
    Ok(Some(record))
}

/// The applied deployment version, or `None` when there is no record, it cannot
/// be read, or the recorded deployment carried no version.
///
/// An unreadable record is a warning and a `None`, never a failure: reporting
/// "this instance cannot say" is always safe, while refusing to start over a
/// corrupt bookkeeping file would strand the instance.
#[must_use]
pub fn read_version(config_dir: &Path) -> Option<u64> {
    match read(config_dir) {
        Ok(record) => record.and_then(|record| record.deployment_version),
        Err(err) => {
            tracing::warn!("Spice Cloud Connect: {err}");
            None
        }
    }
}

/// Record `deployment_version` as the applied deployment.
///
/// `None` is a meaningful value, not a skip: it records that the persisted
/// spicepod came from a dispatch that named no version, which must clear a
/// version left by an earlier deployment rather than let the instance keep
/// claiming it.
///
/// # Errors
///
/// Returns [`Error::Write`] or [`Error::Encode`]. The caller has already
/// persisted the spicepod by this point, so a failure here means the instance
/// will come back serving the new configuration while reporting the old version
/// — say so in the command result rather than swallowing it.
pub fn write(config_dir: &Path, deployment_version: Option<u64>) -> Result<()> {
    let path = path(config_dir);
    let record = AppliedDeployment {
        format_version: FORMAT_VERSION,
        deployment_version,
        applied_at_unix: crate::heartbeat::now_unix(),
    };
    let bytes = serde_json::to_vec_pretty(&record).context(EncodeSnafu)?;
    crate::identity::atomic_write_owner_only(&path, &bytes).context(WriteSnafu { path })
}

/// Delete the record. A missing file is success.
///
/// # Errors
///
/// Returns [`Error::Write`] when the file exists but cannot be removed — a
/// released instance that keeps the record would report an applied deployment
/// for an app it is no longer part of.
pub fn remove(config_dir: &Path) -> Result<()> {
    let path = path(config_dir);
    match std::fs::remove_file(&path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(Error::Write { path, source }),
    }
}

/// [`remove`] for a caller on a Tokio worker: the Cloud Connect stream must not
/// block a runtime thread on filesystem I/O.
///
/// # Errors
///
/// As [`remove`].
pub async fn remove_async(config_dir: &Path) -> Result<()> {
    let path = path(config_dir);
    match tokio::fs::remove_file(&path).await {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(Error::Write { path, source }),
    }
}

/// Render a deployment version for a human-readable field (a log line, the
/// secrets-cache header). An absent version is the empty string, which is how
/// the cache header has always spelled "unknown".
#[must_use]
pub fn version_label(deployment_version: Option<u64>) -> String {
    deployment_version.map_or_else(String::new, |version| version.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch(tag: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("spice-deployment-{}-{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create scratch dir");
        dir
    }

    #[test]
    fn no_record_reports_no_version() {
        let dir = scratch("absent");
        assert_eq!(read(&dir).expect("absent record is not an error"), None);
        assert_eq!(read_version(&dir), None);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_version_round_trips() {
        let dir = scratch("round-trip");
        write(&dir, Some(42)).expect("write");
        let record = read(&dir).expect("read").expect("record present");
        assert_eq!(record.deployment_version, Some(42));
        assert_eq!(read_version(&dir), Some(42));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_versionless_deployment_clears_a_recorded_version() {
        // Reporting the previous version after a dispatch that assigned none
        // would resolve a new deployment against a version it never carried.
        let dir = scratch("clears");
        write(&dir, Some(7)).expect("write");
        write(&dir, None).expect("overwrite");
        assert_eq!(read_version(&dir), None);
        assert!(
            read(&dir).expect("read").is_some(),
            "the record still exists — the instance applied a deployment, it just has no version"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_corrupt_record_is_a_discard_not_a_crash() {
        let dir = scratch("corrupt");
        std::fs::write(path(&dir), b"{not json").expect("write corrupt record");
        assert!(matches!(read(&dir), Err(Error::Malformed { .. })));
        assert_eq!(
            read_version(&dir),
            None,
            "a corrupt record reports no applied deployment rather than failing the start"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn an_unknown_format_version_is_discarded() {
        let dir = scratch("format");
        std::fs::write(
            path(&dir),
            br#"{"format_version": 9999, "deployment_version": 3, "applied_at_unix": 1}"#,
        )
        .expect("write future record");
        assert!(matches!(read(&dir), Err(Error::UnsupportedVersion { .. })));
        assert_eq!(read_version(&dir), None);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn remove_is_idempotent() {
        let dir = scratch("remove");
        remove(&dir).expect("removing an absent record is success");
        write(&dir, Some(1)).expect("write");
        remove(&dir).expect("remove");
        assert_eq!(read_version(&dir), None);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn version_label_spells_an_absent_version_as_empty() {
        assert_eq!(version_label(Some(9)), "9");
        assert_eq!(version_label(None), "");
    }
}
