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

//! Per-directory serialization and the non-secret enrollment journal.

use std::fs::OpenOptions;
use std::io::{Read as _, Write as _};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use runtime_cloud_connect::EnrollmentDraft;
use runtime_cloud_connect::config::CloudConnectConfig;
use runtime_cloud_connect::identity::Identity;
use serde::{Deserialize, Serialize};
use snafu::{ResultExt as _, Snafu};

use super::project::ProjectMutation;

// Shared with the runtime, which removes these journals when the control plane
// releases the instance.
pub(super) const CONNECT_OPERATION_FILE: &str = CloudConnectConfig::CONNECT_OPERATION_FILE;
pub(super) const PROJECT_OPERATION_FILE: &str = CloudConnectConfig::PROJECT_OPERATION_FILE;

const CONNECT_OPERATION_SCHEMA_VERSION: u32 = 3;
const PROJECT_OPERATION_SCHEMA_VERSION: u32 = 3;
const MAX_JOURNAL_BYTES: u64 = 256 * 1024;

#[derive(Debug, Snafu)]
pub(super) enum Error {
    #[snafu(display("Failed to access Cloud Connect state at {}: {source}", path.display()))]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Failed to parse the Cloud Connect enrollment journal at {}: {source}", path.display()))]
    Parse {
        path: PathBuf,
        source: serde_json::Error,
    },

    #[snafu(display("Failed to serialize the Cloud Connect enrollment journal: {source}"))]
    Serialize { source: serde_json::Error },

    #[snafu(display(
        "Cloud Connect state for this directory is already being changed{owner}. Wait for that command to finish, then retry."
    ))]
    LockTimeout { owner: String },

    #[snafu(display("The Cloud Connect enrollment journal did not match this directory or identity. It was quarantined at {} and was not replayed.", journal.display()))]
    Quarantined { journal: PathBuf },

    #[snafu(display(
        "A retry-safe Cloud Connect enrollment is already pending for different parameters ({reason}). Retry with the original organization and endpoint, or run `spice connect remove --force --yes` to explicitly abandon it. The exact-replay state was preserved."
    ))]
    PendingRequestMismatch { reason: String },

    #[snafu(display(
        "A retry-safe Cloud Connect project assignment is already pending for different parameters ({reason}). Retry the original command or run `spice connect remove --force --yes` to explicitly abandon it. The exact-replay state was preserved."
    ))]
    PendingProjectMismatch { reason: String },

    #[snafu(display("The Cloud Connect enrollment journal at {} uses unsupported schema {found}; expected schema {expected}.", path.display()))]
    UnsupportedSchema {
        path: PathBuf,
        found: u32,
        expected: u32,
    },
}

pub(super) type Result<T, E = Error> = std::result::Result<T, E>;

/// The non-secret identity fields journal reconciliation compares against.
///
/// Reconciliation runs on a blocking task, so handing it the whole [`Identity`]
/// would copy the PEM-encoded certificate and both private keys onto the heap —
/// where nothing zeroizes them — to perform four string comparisons.
#[derive(Debug, Clone)]
pub(super) struct IdentityFacts {
    identifier: String,
    org_name: Option<String>,
    app_id: Option<String>,
    app_name: Option<String>,
    control_plane_endpoint: Option<String>,
}

impl From<&Identity> for IdentityFacts {
    fn from(identity: &Identity) -> Self {
        Self {
            identifier: identity.identifier.clone(),
            org_name: identity.org_name.clone(),
            app_id: identity.app_id.clone(),
            app_name: identity.app_name.clone(),
            control_plane_endpoint: identity.control_plane_endpoint.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(super) enum EnrollmentPhase {
    Prepared,
    Enrolled,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct ConnectOperation {
    pub schema_version: u32,
    pub directory: PathBuf,
    pub enrollment_operation_id: String,
    pub organization: String,
    pub endpoint: String,
    pub region: Option<String>,
    pub phase: EnrollmentPhase,
    pub instance_id: Option<String>,
}

/// How firmly an operation is tied to the organization recorded for it.
///
/// The two enrollment authorities differ here, and the journal has to match them
/// or it refuses a retry the enrollment itself would accept.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum OrganizationBinding {
    /// A login-session operation is bound to one organization for its lifetime:
    /// the session was issued for it, and another organization is another
    /// operation.
    Fixed,
    /// An enrollment key asserts the organization it expects, and Spice Cloud
    /// checks the assertion *before* consuming the key — so a mismatch leaves the
    /// key unspent and the operation replayable with the assertion corrected.
    Assertion,
}

impl ConnectOperation {
    pub(super) fn prepare(
        config_dir: &Path,
        directory: &Path,
        enrollment_operation_id: &str,
        organization: &str,
        binding: OrganizationBinding,
        endpoint: &str,
        region: Option<&str>,
    ) -> Result<Self> {
        if let Some(mut existing) = Self::load_optional(config_dir)? {
            let same_organization = existing.organization.eq_ignore_ascii_case(organization);
            if existing.schema_version == CONNECT_OPERATION_SCHEMA_VERSION
                && existing.directory == directory
                && existing.enrollment_operation_id == enrollment_operation_id
                && (same_organization || binding == OrganizationBinding::Assertion)
                && existing.endpoint == endpoint
                && region.is_none_or(|region| existing.region.as_deref() == Some(region))
                && existing.phase == EnrollmentPhase::Prepared
            {
                if !same_organization {
                    // The corrected assertion, replaying the same operation. It is
                    // recorded now so that the identity this run enrolls is checked
                    // against what was asked for, not against what was mistyped.
                    existing.organization = organization.to_string();
                    existing.store(config_dir)?;
                }
                return Ok(existing);
            }
            return Err(Error::PendingRequestMismatch {
                reason: format!(
                    "journal operation {}, organization {}, endpoint {}, region {}",
                    existing.enrollment_operation_id,
                    existing.organization,
                    existing.endpoint,
                    existing.region.as_deref().unwrap_or("unspecified")
                ),
            });
        }

        let operation = Self {
            schema_version: CONNECT_OPERATION_SCHEMA_VERSION,
            directory: directory.to_path_buf(),
            enrollment_operation_id: enrollment_operation_id.to_string(),
            organization: organization.to_string(),
            endpoint: endpoint.to_string(),
            region: region.map(ToString::to_string),
            phase: EnrollmentPhase::Prepared,
            instance_id: None,
        };
        operation.store(config_dir)?;
        Ok(operation)
    }

    pub(super) fn mark_enrolled(&mut self, config_dir: &Path, identity: &Identity) -> Result<()> {
        self.phase = EnrollmentPhase::Enrolled;
        self.instance_id = Some(identity.identifier.clone());
        if !self.organization.is_empty()
            && !identity
                .org_name
                .as_deref()
                .is_some_and(|org| org.eq_ignore_ascii_case(&self.organization))
        {
            return Err(Error::PendingRequestMismatch {
                reason: format!(
                    "the enrolled identity organization did not match requested organization {}",
                    self.organization
                ),
            });
        }
        self.store(config_dir)
    }

    pub(super) fn delete(config_dir: &Path) -> Result<()> {
        remove_if_exists(&config_dir.join(CONNECT_OPERATION_FILE))
    }

    pub(super) fn load_optional(config_dir: &Path) -> Result<Option<Self>> {
        let path = config_dir.join(CONNECT_OPERATION_FILE);
        let raw = match read_bounded_regular_file(&path, MAX_JOURNAL_BYTES) {
            Ok(raw) => String::from_utf8(raw).map_err(|source| Error::Io {
                path: path.clone(),
                source: std::io::Error::new(std::io::ErrorKind::InvalidData, source),
            })?,
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(source) => return Err(Error::Io { path, source }),
        };
        let operation =
            serde_json::from_str::<Self>(&raw).context(ParseSnafu { path: path.clone() })?;
        if operation.schema_version != CONNECT_OPERATION_SCHEMA_VERSION {
            return Err(Error::UnsupportedSchema {
                path,
                found: operation.schema_version,
                expected: CONNECT_OPERATION_SCHEMA_VERSION,
            });
        }
        Ok(Some(operation))
    }

    /// Reconcile a leftover enrollment journal before any new mutation.
    ///
    /// A matching durable identity makes the journal obsolete. Any positive
    /// mismatch is quarantined together with the provisional draft so it can
    /// never be replayed under this directory by accident.
    pub(super) fn reconcile(
        config_dir: &Path,
        directory: &Path,
        endpoint: &str,
        identity: Option<&IdentityFacts>,
    ) -> Result<()> {
        let Some(operation) = Self::load_optional(config_dir)? else {
            return Ok(());
        };
        let directory_matches = operation.directory == directory;
        if identity.is_none() && (!directory_matches || operation.endpoint != endpoint) {
            return Err(Error::PendingRequestMismatch {
                reason: format!(
                    "directory {}, endpoint {}",
                    operation.directory.display(),
                    operation.endpoint
                ),
            });
        }
        let identity_matches = identity.is_none_or(|identity| {
            operation
                .instance_id
                .as_deref()
                .is_none_or(|instance_id| instance_id == identity.identifier)
                && (operation.organization.is_empty()
                    || identity
                        .org_name
                        .as_deref()
                        .is_some_and(|org| org.eq_ignore_ascii_case(&operation.organization)))
                && identity.control_plane_endpoint.as_deref() == Some(operation.endpoint.as_str())
        });
        let missing_identity_after_enroll =
            identity.is_none() && operation.phase == EnrollmentPhase::Enrolled;

        if !directory_matches || !identity_matches || missing_identity_after_enroll {
            let path = config_dir.join(CONNECT_OPERATION_FILE);
            let draft = EnrollmentDraft::path_in(config_dir);
            // The replay material goes first. If either rename fails, the
            // mismatched journal remains as a fail-closed guard; moving the
            // journal first could strand a live draft that the next run pairs
            // with fresh local authority.
            if draft.exists() {
                let _ = quarantine(config_dir, &draft)?;
            }
            let journal = quarantine(config_dir, &path)?;
            return Err(Error::Quarantined { journal });
        }

        if identity.is_some() {
            // Identity promotion is authoritative, but a crash can leave the
            // draft (and both private keys it contains) beside that identity.
            // Retire it under the enrollment transaction before removing the
            // journal, otherwise every later run takes the existing-identity
            // path and the credential debris becomes permanent.
            EnrollmentDraft::delete(config_dir).map_err(|source| Error::Io {
                path: EnrollmentDraft::path_in(config_dir),
                source: std::io::Error::other(source),
            })?;
            Self::delete(config_dir)?;
        }
        Ok(())
    }

    fn store(&self, config_dir: &Path) -> Result<()> {
        let path = config_dir.join(CONNECT_OPERATION_FILE);
        let body = serde_json::to_vec_pretty(self).context(SerializeSnafu)?;
        atomic_write_owner_only(&path, &body)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct ProjectOperation {
    pub schema_version: u32,
    pub directory: PathBuf,
    pub endpoint: String,
    pub organization: String,
    pub request: ProjectMutation,
}

impl ProjectOperation {
    pub(super) fn prepare(
        config_dir: &Path,
        directory: &Path,
        endpoint: &str,
        organization: &str,
        request: ProjectMutation,
    ) -> Result<Self> {
        if let Some(existing) = Self::load_optional(config_dir)? {
            if existing.directory == directory
                && existing.endpoint == endpoint
                && existing.organization.eq_ignore_ascii_case(organization)
                && existing.request.instance_id == request.instance_id
                && existing.request.name == request.name
            {
                return Ok(existing);
            }
            return Err(Error::PendingProjectMismatch {
                reason: format!(
                    "organization {}, endpoint {}, instance {}, project {}",
                    existing.organization,
                    existing.endpoint,
                    existing.request.instance_id,
                    existing.request.name
                ),
            });
        }
        let operation = Self {
            schema_version: PROJECT_OPERATION_SCHEMA_VERSION,
            directory: directory.to_path_buf(),
            endpoint: endpoint.to_string(),
            organization: organization.to_string(),
            request,
        };
        operation.store(config_dir)?;
        Ok(operation)
    }

    pub(super) fn reconcile(
        config_dir: &Path,
        directory: &Path,
        endpoint: &str,
        identity: Option<&IdentityFacts>,
    ) -> Result<Option<Self>> {
        let Some(operation) = Self::load_optional(config_dir)? else {
            return Ok(None);
        };
        let matches_identity = identity.is_some_and(|identity| {
            identity.identifier == operation.request.instance_id
                && identity
                    .org_name
                    .as_deref()
                    .is_some_and(|org| org.eq_ignore_ascii_case(&operation.organization))
        });
        if operation.directory != directory || operation.endpoint != endpoint || !matches_identity {
            return Err(Error::PendingProjectMismatch {
                reason: format!(
                    "organization {}, endpoint {}, instance {}, project {}",
                    operation.organization,
                    operation.endpoint,
                    operation.request.instance_id,
                    operation.request.name
                ),
            });
        }
        let Some(identity) = identity else {
            return Err(Error::PendingProjectMismatch {
                reason: "the enrolled identity is missing".to_string(),
            });
        };
        if identity.app_id.is_some() {
            let matches_attachment = identity
                .org_name
                .as_deref()
                .is_some_and(|org| org.eq_ignore_ascii_case(&operation.organization))
                && identity.app_name.as_deref() == Some(operation.request.name.as_str());
            if matches_attachment {
                Self::delete(config_dir)?;
                return Ok(None);
            }
            return Err(Error::PendingProjectMismatch {
                reason: "the durable identity has a different project attachment".to_string(),
            });
        }
        Ok(Some(operation))
    }

    pub(super) fn load_optional(config_dir: &Path) -> Result<Option<Self>> {
        let path = config_dir.join(PROJECT_OPERATION_FILE);
        let raw = match read_bounded_regular_file(&path, MAX_JOURNAL_BYTES) {
            Ok(raw) => String::from_utf8(raw).map_err(|source| Error::Io {
                path: path.clone(),
                source: std::io::Error::new(std::io::ErrorKind::InvalidData, source),
            })?,
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(source) => return Err(Error::Io { path, source }),
        };
        let operation =
            serde_json::from_str::<Self>(&raw).context(ParseSnafu { path: path.clone() })?;
        if operation.schema_version != PROJECT_OPERATION_SCHEMA_VERSION {
            return Err(Error::UnsupportedSchema {
                path,
                found: operation.schema_version,
                expected: PROJECT_OPERATION_SCHEMA_VERSION,
            });
        }
        Ok(Some(operation))
    }

    pub(super) fn delete(config_dir: &Path) -> Result<()> {
        remove_if_exists(&config_dir.join(PROJECT_OPERATION_FILE))
    }

    fn store(&self, config_dir: &Path) -> Result<()> {
        let path = config_dir.join(PROJECT_OPERATION_FILE);
        let body = serde_json::to_vec_pretty(self).context(SerializeSnafu)?;
        atomic_write_owner_only(&path, &body)
    }
}

fn read_bounded_regular_file(path: &Path, max_bytes: u64) -> std::io::Result<Vec<u8>> {
    read_bounded_regular_file_with_metadata(path, max_bytes).map(|(bytes, _)| bytes)
}

pub(super) fn read_bounded_regular_file_with_metadata(
    path: &Path,
    max_bytes: u64,
) -> std::io::Result<(Vec<u8>, std::fs::Metadata)> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.custom_flags(nix::libc::O_NOFOLLOW | nix::libc::O_NONBLOCK);
    }
    let file = options.open(path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.len() > max_bytes {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "the Cloud Connect state file was not a bounded regular file",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        if metadata.nlink() != 1 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "the Cloud Connect state file must not be hard-linked",
            ));
        }
    }
    let mut bytes = Vec::with_capacity(usize::try_from(metadata.len()).unwrap_or(0));
    file.take(max_bytes.saturating_add(1))
        .read_to_end(&mut bytes)?;
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > max_bytes {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "the Cloud Connect state file exceeded its size limit",
        ));
    }
    Ok((bytes, metadata))
}

pub(super) fn atomic_write_owner_only(path: &Path, bytes: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).context(IoSnafu {
            path: parent.to_path_buf(),
        })?;
    }
    let candidate = path.with_file_name(format!(
        ".{}.{}.candidate",
        path.file_name()
            .and_then(std::ffi::OsStr::to_str)
            .unwrap_or(CONNECT_OPERATION_FILE),
        rand::random::<u64>()
    ));
    let mut options = OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    let mut file = options.open(&candidate).context(IoSnafu {
        path: candidate.clone(),
    })?;
    file.write_all(bytes).context(IoSnafu {
        path: candidate.clone(),
    })?;
    file.sync_all().context(IoSnafu {
        path: candidate.clone(),
    })?;
    drop(file);
    if let Err(source) = promote_candidate(&candidate, path) {
        let _ = std::fs::remove_file(&candidate);
        return Err(Error::Io {
            path: path.to_path_buf(),
            source,
        });
    }
    sync_parent_directory(path).context(IoSnafu {
        path: path.to_path_buf(),
    })
}

#[cfg(unix)]
fn promote_candidate(candidate: &Path, path: &Path) -> std::io::Result<()> {
    std::fs::rename(candidate, path)
}

#[cfg(not(unix))]
fn promote_candidate(candidate: &Path, path: &Path) -> std::io::Result<()> {
    std::fs::rename(candidate, path)
}

#[cfg(unix)]
fn sync_parent_directory(path: &Path) -> std::io::Result<()> {
    std::fs::File::open(path.parent().unwrap_or_else(|| Path::new(".")))?.sync_all()
}

#[cfg(not(unix))]
fn sync_parent_directory(_path: &Path) -> std::io::Result<()> {
    Ok(())
}

fn quarantine(config_dir: &Path, path: &Path) -> Result<PathBuf> {
    let epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let stem = path
        .file_stem()
        .and_then(std::ffi::OsStr::to_str)
        .unwrap_or("connect-state");
    let extension = path
        .extension()
        .and_then(std::ffi::OsStr::to_str)
        .unwrap_or("json");
    let quarantined = config_dir.join(format!(
        "{stem}.quarantine.{epoch}.{}.{extension}",
        std::process::id()
    ));
    std::fs::rename(path, &quarantined).context(IoSnafu {
        path: path.to_path_buf(),
    })?;
    // The quarantine is an authority transition: after success, the old live
    // name must not reappear after a crash and become replayable again.
    sync_parent(&quarantined)?;
    Ok(quarantined)
}

pub(super) fn remove_durable_file(path: &Path) -> Result<()> {
    remove_if_exists(path)
}

fn remove_if_exists(path: &Path) -> Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => sync_parent(path),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => sync_parent(path),
        Err(source) => Err(Error::Io {
            path: path.to_path_buf(),
            source,
        }),
    }
}

#[cfg(unix)]
fn sync_parent(path: &Path) -> Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    std::fs::File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| Error::Io {
            path: parent.to_path_buf(),
            source,
        })
}

#[cfg(not(unix))]
fn sync_parent(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity(instance: &str, org: &str) -> Identity {
        Identity {
            identifier: instance.to_string(),
            identity_cert_pem: "CERT".to_string(),
            private_key_pem: "KEY".to_string(),
            public_key_pem: "PUB".to_string(),
            ca_bundle_pem: String::new(),
            gateway_addr: "gateway.test:443".to_string(),
            not_after_unix: None,
            enc_private_key_pem: String::new(),
            enc_public_key_pem: String::new(),
            enc_previous_private_key_pem: String::new(),
            cache_key_b64: String::new(),
            app_id: None,
            org_name: Some(org.to_string()),
            app_name: None,
            monitor_url: None,
            new_project_url: None,
            control_plane_endpoint: Some("https://api.spice.ai".to_string()),
        }
    }

    #[test]
    fn enrolled_identity_retires_the_matching_enrollment_journal() {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(
            EnrollmentDraft::path_in(dir.path()),
            b"private enrollment material left after identity promotion",
        )
        .expect("write interrupted draft");
        let mut operation = ConnectOperation::prepare(
            dir.path(),
            dir.path(),
            "operation-1",
            "acme",
            OrganizationBinding::Fixed,
            "https://api.spice.ai",
            None,
        )
        .expect("prepare");
        operation
            .mark_enrolled(dir.path(), &identity("inst_1", "acme"))
            .expect("mark enrolled");
        ConnectOperation::reconcile(
            dir.path(),
            dir.path(),
            "https://api.spice.ai",
            Some(&IdentityFacts::from(&identity("inst_1", "acme"))),
        )
        .expect("reconcile");
        assert!(!dir.path().join(CONNECT_OPERATION_FILE).exists());
        assert!(
            !EnrollmentDraft::path_in(dir.path()).exists(),
            "the authoritative identity must retire its matching draft"
        );
    }

    #[test]
    fn mismatched_identity_is_quarantined_and_never_replayed() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut operation = ConnectOperation::prepare(
            dir.path(),
            dir.path(),
            "operation-1",
            "acme",
            OrganizationBinding::Fixed,
            "https://api.spice.ai",
            None,
        )
        .expect("prepare");
        operation
            .mark_enrolled(dir.path(), &identity("inst_1", "acme"))
            .expect("mark enrolled");
        let err = ConnectOperation::reconcile(
            dir.path(),
            dir.path(),
            "https://api.spice.ai",
            Some(&IdentityFacts::from(&identity("inst_other", "acme"))),
        )
        .expect_err("mismatch must quarantine");
        assert!(matches!(err, Error::Quarantined { .. }));
        assert!(!dir.path().join(CONNECT_OPERATION_FILE).exists());
        assert!(
            std::fs::read_dir(dir.path())
                .expect("read dir")
                .flatten()
                .any(|entry| entry.file_name().to_string_lossy().contains("quarantine"))
        );
    }

    #[test]
    fn identity_from_another_control_plane_cannot_retire_the_journal() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut operation = ConnectOperation::prepare(
            dir.path(),
            dir.path(),
            "operation-1",
            "acme",
            OrganizationBinding::Fixed,
            "https://control-a.example",
            None,
        )
        .expect("prepare");
        let mut enrolled = identity("inst_1", "acme");
        enrolled.control_plane_endpoint = Some("https://control-b.example".to_string());
        operation
            .mark_enrolled(dir.path(), &enrolled)
            .expect("record enrolled phase");

        let error = ConnectOperation::reconcile(
            dir.path(),
            dir.path(),
            "https://control-b.example",
            Some(&IdentityFacts::from(&enrolled)),
        )
        .expect_err("another control plane must not consume exact-replay state");

        assert!(matches!(error, Error::Quarantined { .. }));
        assert!(!dir.path().join(CONNECT_OPERATION_FILE).exists());
    }

    #[test]
    fn missing_identity_organization_cannot_retire_an_org_bound_journal() {
        let dir = tempfile::tempdir().expect("tempdir");
        ConnectOperation::prepare(
            dir.path(),
            dir.path(),
            "operation-1",
            "acme",
            OrganizationBinding::Fixed,
            "https://api.spice.ai",
            None,
        )
        .expect("prepare");
        let mut recovered = IdentityFacts::from(&identity("inst_1", "acme"));
        recovered.org_name = None;

        let error = ConnectOperation::reconcile(
            dir.path(),
            dir.path(),
            "https://api.spice.ai",
            Some(&recovered),
        )
        .expect_err("missing organization must not bypass journal authority");

        assert!(matches!(error, Error::Quarantined { .. }));
        assert!(!dir.path().join(CONNECT_OPERATION_FILE).exists());
    }

    /// An enrollment key asserts the organization it expects and Spice Cloud
    /// checks the assertion before consuming the key, so a mismatch leaves the
    /// operation replayable with the assertion corrected. The journal has to
    /// accept that correction, or the draft it preserved can never be finished.
    #[test]
    fn a_corrected_assertion_replays_the_same_token_operation() {
        let dir = tempfile::tempdir().expect("tempdir");
        ConnectOperation::prepare(
            dir.path(),
            dir.path(),
            "operation-1",
            "acme-typo",
            OrganizationBinding::Assertion,
            "https://api.spice.ai",
            None,
        )
        .expect("prepare the operation that asserted the wrong organization");

        let corrected = ConnectOperation::prepare(
            dir.path(),
            dir.path(),
            "operation-1",
            "acme",
            OrganizationBinding::Assertion,
            "https://api.spice.ai",
            None,
        )
        .expect("the corrected assertion replays the same operation");
        assert_eq!(
            corrected.enrollment_operation_id, "operation-1",
            "the operation is replayed, not replaced"
        );
        assert_eq!(
            corrected.organization, "acme",
            "and the journal records what was asked for"
        );

        // Persisted, so the identity this run enrolls is checked against the
        // corrected organization rather than the mistyped one.
        let reloaded = ConnectOperation::load_optional(dir.path())
            .expect("load")
            .expect("the journal is still there");
        assert_eq!(reloaded.organization, "acme");

        // A login-bound operation is not an assertion and does not move.
        let fixed = ConnectOperation::prepare(
            dir.path(),
            dir.path(),
            "operation-1",
            "globex",
            OrganizationBinding::Fixed,
            "https://api.spice.ai",
            None,
        )
        .expect_err("a session is issued for one organization");
        assert!(matches!(fixed, Error::PendingRequestMismatch { .. }));
    }

    #[test]
    fn changed_pending_request_preserves_exact_replay_state() {
        let dir = tempfile::tempdir().expect("tempdir");
        let draft = EnrollmentDraft::path_in(dir.path());
        std::fs::write(&draft, "exact replay material").expect("write draft sentinel");
        ConnectOperation::prepare(
            dir.path(),
            dir.path(),
            "operation-1",
            "acme",
            OrganizationBinding::Fixed,
            "https://control-a.example",
            None,
        )
        .expect("prepare");

        let endpoint_error =
            ConnectOperation::reconcile(dir.path(), dir.path(), "https://control-b.example", None)
                .expect_err("changed endpoint must fail closed");
        assert!(matches!(
            endpoint_error,
            Error::PendingRequestMismatch { .. }
        ));
        let org_error = ConnectOperation::prepare(
            dir.path(),
            dir.path(),
            "operation-1",
            "globex",
            OrganizationBinding::Fixed,
            "https://control-a.example",
            None,
        )
        .expect_err("changed organization must fail closed");
        assert!(matches!(org_error, Error::PendingRequestMismatch { .. }));
        assert_eq!(
            std::fs::read_to_string(draft).expect("draft remains"),
            "exact replay material"
        );
        let operation = ConnectOperation::load_optional(dir.path())
            .expect("load journal")
            .expect("journal remains");
        assert_eq!(operation.enrollment_operation_id, "operation-1");
        assert_eq!(operation.organization, "acme");
        assert_eq!(operation.endpoint, "https://control-a.example");
    }

    #[test]
    fn changed_pending_region_preserves_exact_replay_state() {
        let dir = tempfile::tempdir().expect("tempdir");
        ConnectOperation::prepare(
            dir.path(),
            dir.path(),
            "operation-1",
            "acme",
            OrganizationBinding::Fixed,
            "https://api.spice.ai",
            Some("us-east-1"),
        )
        .expect("prepare");
        let error = ConnectOperation::prepare(
            dir.path(),
            dir.path(),
            "operation-1",
            "acme",
            OrganizationBinding::Fixed,
            "https://api.spice.ai",
            Some("eu-west-1"),
        )
        .expect_err("changed region must fail closed");
        assert!(matches!(error, Error::PendingRequestMismatch { .. }));
        let operation = ConnectOperation::load_optional(dir.path())
            .expect("load operation")
            .expect("operation remains");
        assert_eq!(operation.region.as_deref(), Some("us-east-1"));
    }

    #[test]
    fn enrolled_organization_cannot_replace_the_requested_organization() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut operation = ConnectOperation::prepare(
            dir.path(),
            dir.path(),
            "operation-1",
            "acme",
            OrganizationBinding::Fixed,
            "https://api.spice.ai",
            None,
        )
        .expect("prepare");
        let error = operation
            .mark_enrolled(dir.path(), &identity("inst_1", "globex"))
            .expect_err("response org must not rewrite request authority");
        assert!(matches!(error, Error::PendingRequestMismatch { .. }));
        assert_eq!(operation.organization, "acme");
    }

    #[test]
    fn project_operation_persists_only_the_exact_request_for_server_replay() {
        let dir = tempfile::tempdir().expect("tempdir");
        let request = ProjectMutation {
            instance_id: "inst_1".to_string(),
            name: "retail".to_string(),
            cert_pem: "certificate".to_string(),
            pop_sig: "signature".to_string(),
        };
        let operation = ProjectOperation::prepare(
            dir.path(),
            dir.path(),
            "https://api.spice.ai",
            "acme",
            request.clone(),
        )
        .expect("prepare project operation");
        let recovered = ProjectOperation::prepare(
            dir.path(),
            dir.path(),
            "https://api.spice.ai",
            "acme",
            request,
        )
        .expect("recover exact operation");
        assert_eq!(recovered, operation);
    }

    #[cfg(unix)]
    #[test]
    fn journal_is_owner_only_and_contains_no_credentials() {
        use std::os::unix::fs::PermissionsExt as _;
        let dir = tempfile::tempdir().expect("tempdir");
        ConnectOperation::prepare(
            dir.path(),
            dir.path(),
            "operation-1",
            "acme",
            OrganizationBinding::Fixed,
            "https://api.spice.ai",
            None,
        )
        .expect("prepare");
        let path = dir.path().join(CONNECT_OPERATION_FILE);
        let mode = std::fs::metadata(&path)
            .expect("metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600);
        let raw = std::fs::read_to_string(path).expect("journal");
        assert!(!raw.contains("token"));
        assert!(!raw.contains("private_key"));
    }
}
