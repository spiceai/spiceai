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

//! Owner-only per-directory service manifest and deterministic service names.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use snafu::Snafu;

use super::model::{SCHEMA_VERSION, ServiceOwner, ServiceScope, ServiceSupervisor};

pub(crate) const MANIFEST_FILE: &str = "service.json";
const MAX_NAME_FRAGMENT: usize = 32;
const DIGEST_BYTES: usize = 8;

#[derive(Debug, Snafu)]
pub(crate) enum Error {
    #[snafu(display("Failed to inspect service manifest {}: {source}", path.display()))]
    Inspect {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Failed to read service manifest {}: {source}", path.display()))]
    Read {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Failed to parse service manifest {}: {source}", path.display()))]
    Parse {
        path: PathBuf,
        source: serde_json::Error,
    },

    #[snafu(display("Failed to write service manifest {}: {source}", path.display()))]
    Write {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Invalid service manifest {}: {reason}", path.display()))]
    Invalid { path: PathBuf, reason: String },

    #[snafu(display(
        "service_name_collision: Service name collision for {name}: {reason}. No service files were changed."
    ))]
    Collision { name: String, reason: String },
}

pub(crate) type Result<T, E = Error> = std::result::Result<T, E>;

/// Whether the manifest currently represents installed service assets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ManifestState {
    Installed,
    Uninstalled,
}

/// Sole discovery record for a service owned by one canonical directory.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ServiceManifest {
    pub(crate) schema_version: u32,
    pub(crate) directory: PathBuf,
    pub(crate) name: String,
    pub(crate) scope: ServiceScope,
    pub(crate) backend: ServiceSupervisor,
    pub(crate) owner: ServiceOwner,
    pub(crate) definition_path: PathBuf,
    pub(crate) runtime_path: PathBuf,
    pub(crate) log_source: String,
    pub(crate) runtime_sha256: String,
    pub(crate) runtime_version: String,
    pub(crate) health_url: Option<String>,
    pub(crate) state: ManifestState,
}

impl ServiceManifest {
    /// Validate identity fields before an install is allowed to mutate an
    /// existing definition with the deterministic name.
    pub(crate) fn ensure_same_service(&self, requested: &Self) -> Result<()> {
        let same_identity = self.directory == requested.directory
            && self.name == requested.name
            && self.scope == requested.scope
            && self.backend == requested.backend
            && self.owner == requested.owner;
        if same_identity {
            return Ok(());
        }
        Err(Error::Collision {
            name: requested.name.clone(),
            reason: format!(
                "the existing manifest belongs to directory {}, scope {:?}, backend {:?}, owner {} (uid {}), not directory {}, scope {:?}, backend {:?}, owner {} (uid {})",
                self.directory.display(),
                self.scope,
                self.backend,
                self.owner.name,
                self.owner.uid,
                requested.directory.display(),
                requested.scope,
                requested.backend,
                requested.owner.name,
                requested.owner.uid
            ),
        })
    }
}

/// Load and validate the manifest for exactly `directory`. Absence means no
/// service; malformed or untrusted state is an error and never triggers a
/// global supervisor scan.
pub(crate) fn load_validated(
    config_dir: &Path,
    directory: &Path,
) -> Result<Option<ServiceManifest>> {
    let path = config_dir.join(MANIFEST_FILE);
    let metadata = match std::fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => return Err(Error::Inspect { path, source }),
    };
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(Error::Invalid {
            path,
            reason: "the manifest must be a regular file, not a symlink or directory".to_string(),
        });
    }

    let bytes = std::fs::read(&path).map_err(|source| Error::Read {
        path: path.clone(),
        source,
    })?;
    let manifest =
        serde_json::from_slice::<ServiceManifest>(&bytes).map_err(|source| Error::Parse {
            path: path.clone(),
            source,
        })?;
    validate_file_metadata(&path, &metadata, &manifest)?;
    validate_manifest(&path, directory, &manifest)?;
    Ok(Some(manifest))
}

fn validate_manifest(path: &Path, directory: &Path, manifest: &ServiceManifest) -> Result<()> {
    let canonical = validate_manifest_fields(path, directory, manifest)?;
    validate_owned_directory(path, "instance directory", &canonical, manifest.owner.uid)?;
    if let Some(config_dir) = path.parent() {
        validate_owned_directory(
            path,
            "configuration directory",
            config_dir,
            manifest.owner.uid,
        )?;
    }
    for (label, owned_path) in [
        ("definition_path", &manifest.definition_path),
        ("runtime_path", &manifest.runtime_path),
    ] {
        validate_owned_asset(path, label, owned_path, manifest)?;
    }
    Ok(())
}

/// Validate the non-secret backend plan before it is allowed to mutate the
/// host. Native backends remain responsible for validating any pre-existing
/// definition with the planned name during their read-only planning phase.
pub(crate) fn validate_install_plan(
    config_dir: &Path,
    directory: &Path,
    manifest: &ServiceManifest,
) -> Result<()> {
    validate_manifest_fields(&config_dir.join(MANIFEST_FILE), directory, manifest).map(|_| ())
}

fn validate_manifest_fields(
    path: &Path,
    directory: &Path,
    manifest: &ServiceManifest,
) -> Result<PathBuf> {
    if manifest.schema_version != SCHEMA_VERSION {
        return Err(Error::Invalid {
            path: path.to_path_buf(),
            reason: format!(
                "unsupported schema_version {}; expected {SCHEMA_VERSION}",
                manifest.schema_version
            ),
        });
    }

    let canonical = std::fs::canonicalize(directory).map_err(|source| Error::Inspect {
        path: directory.to_path_buf(),
        source,
    })?;
    if canonical != manifest.directory {
        return Err(Error::Invalid {
            path: path.to_path_buf(),
            reason: format!(
                "directory is {}, but this invocation resolves to {}",
                manifest.directory.display(),
                canonical.display()
            ),
        });
    }

    let expected_name = service_name_for_dir(&canonical, manifest.backend)?;
    if manifest.name != expected_name {
        return Err(Error::Invalid {
            path: path.to_path_buf(),
            reason: format!(
                "service name is {}, but the canonical directory requires {expected_name}",
                manifest.name
            ),
        });
    }

    for (label, owned_path) in [
        ("definition_path", &manifest.definition_path),
        ("runtime_path", &manifest.runtime_path),
    ] {
        if !owned_path.is_absolute() {
            return Err(Error::Invalid {
                path: path.to_path_buf(),
                reason: format!("{label} must be absolute: {}", owned_path.display()),
            });
        }
    }

    if manifest.owner.name.is_empty() || manifest.owner.name.chars().any(char::is_control) {
        return Err(Error::Invalid {
            path: path.to_path_buf(),
            reason: "owner name must be non-empty and contain no control characters".to_string(),
        });
    }
    if manifest.log_source.is_empty() || manifest.log_source.chars().any(char::is_control) {
        return Err(Error::Invalid {
            path: path.to_path_buf(),
            reason: "log_source must be non-empty and contain no control characters".to_string(),
        });
    }
    if manifest.runtime_version.is_empty() || manifest.runtime_version.chars().any(char::is_control)
    {
        return Err(Error::Invalid {
            path: path.to_path_buf(),
            reason: "runtime_version must be non-empty and contain no control characters"
                .to_string(),
        });
    }
    if manifest.runtime_sha256.len() != 64
        || !manifest
            .runtime_sha256
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(Error::Invalid {
            path: path.to_path_buf(),
            reason: "runtime_sha256 must contain exactly 64 lowercase hexadecimal characters"
                .to_string(),
        });
    }
    if let Some(health_url) = manifest.health_url.as_deref() {
        validate_local_health_url(path, health_url)?;
    }
    Ok(canonical)
}

fn validate_local_health_url(manifest_path: &Path, health_url: &str) -> Result<()> {
    let url = reqwest::Url::parse(health_url).map_err(|source| Error::Invalid {
        path: manifest_path.to_path_buf(),
        reason: format!("health_url is invalid: {source}"),
    })?;
    let host = url.host_str().unwrap_or_default().trim_matches(['[', ']']);
    let is_loopback = host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|address| address.is_loopback());
    if !matches!(url.scheme(), "http" | "https")
        || !url.username().is_empty()
        || url.password().is_some()
        || !is_loopback
    {
        return Err(Error::Invalid {
            path: manifest_path.to_path_buf(),
            reason: "health_url must be an HTTP(S) loopback URL without credentials".to_string(),
        });
    }
    Ok(())
}

fn validate_owned_directory(
    manifest_path: &Path,
    label: &str,
    directory: &Path,
    expected_uid: u32,
) -> Result<()> {
    let metadata = std::fs::symlink_metadata(directory).map_err(|source| Error::Inspect {
        path: directory.to_path_buf(),
        source,
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(Error::Invalid {
            path: manifest_path.to_path_buf(),
            reason: format!(
                "{label} is not a real, non-symlink directory: {}",
                directory.display()
            ),
        });
    }
    validate_owned_directory_metadata(manifest_path, label, directory, &metadata, expected_uid)
}

#[cfg(unix)]
fn validate_owned_directory_metadata(
    manifest_path: &Path,
    label: &str,
    directory: &Path,
    metadata: &std::fs::Metadata,
    expected_uid: u32,
) -> Result<()> {
    use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};

    let mode = metadata.permissions().mode() & 0o777;
    if metadata.uid() != expected_uid || mode & 0o022 != 0 {
        return Err(Error::Invalid {
            path: manifest_path.to_path_buf(),
            reason: format!(
                "{label} {} has uid {} mode {mode:04o}; expected uid {expected_uid} and no group/world write bits",
                directory.display(),
                metadata.uid()
            ),
        });
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_owned_directory_metadata(
    _manifest_path: &Path,
    _label: &str,
    _directory: &Path,
    _metadata: &std::fs::Metadata,
    _expected_uid: u32,
) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn validate_file_metadata(
    path: &Path,
    metadata: &std::fs::Metadata,
    manifest: &ServiceManifest,
) -> Result<()> {
    use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};

    let mode = metadata.permissions().mode() & 0o777;
    if mode != 0o600 {
        return Err(Error::Invalid {
            path: path.to_path_buf(),
            reason: format!("mode is {mode:04o}; expected owner-only mode 0600"),
        });
    }
    if metadata.uid() != manifest.owner.uid {
        return Err(Error::Invalid {
            path: path.to_path_buf(),
            reason: format!(
                "file owner uid {} does not match manifest owner uid {}",
                metadata.uid(),
                manifest.owner.uid
            ),
        });
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_file_metadata(
    _path: &Path,
    _metadata: &std::fs::Metadata,
    _manifest: &ServiceManifest,
) -> Result<()> {
    Ok(())
}

fn validate_owned_asset(
    manifest_path: &Path,
    label: &str,
    owned_path: &Path,
    manifest: &ServiceManifest,
) -> Result<()> {
    let metadata = match std::fs::symlink_metadata(owned_path) {
        Ok(metadata) => metadata,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(source) => {
            return Err(Error::Inspect {
                path: owned_path.to_path_buf(),
                source,
            });
        }
    };
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(Error::Invalid {
            path: manifest_path.to_path_buf(),
            reason: format!(
                "{label} is not a regular, non-symlink file: {}",
                owned_path.display()
            ),
        });
    }
    validate_owned_asset_metadata(manifest_path, label, owned_path, &metadata, manifest)
}

#[cfg(unix)]
fn validate_owned_asset_metadata(
    manifest_path: &Path,
    label: &str,
    owned_path: &Path,
    metadata: &std::fs::Metadata,
    manifest: &ServiceManifest,
) -> Result<()> {
    use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};

    let expected_uid = match manifest.scope {
        ServiceScope::User => manifest.owner.uid,
        ServiceScope::System => 0,
    };
    let mode = metadata.permissions().mode() & 0o777;
    if metadata.uid() != expected_uid || mode & 0o022 != 0 {
        return Err(Error::Invalid {
            path: manifest_path.to_path_buf(),
            reason: format!(
                "{label} {} has uid {} mode {mode:04o}; expected uid {expected_uid} and no group/world write bits",
                owned_path.display(),
                metadata.uid()
            ),
        });
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_owned_asset_metadata(
    _manifest_path: &Path,
    _label: &str,
    _owned_path: &Path,
    _metadata: &std::fs::Metadata,
    _manifest: &ServiceManifest,
) -> Result<()> {
    Ok(())
}

/// Write a complete manifest atomically with owner-only permissions.
pub(crate) fn write(config_dir: &Path, manifest: &ServiceManifest) -> Result<()> {
    let path = config_dir.join(MANIFEST_FILE);
    let bytes = serde_json::to_vec_pretty(manifest).map_err(|source| Error::Parse {
        path: path.clone(),
        source,
    })?;
    write_0600(&path, &bytes).map_err(|source| Error::Write { path, source })
}

#[cfg(unix)]
fn write_0600(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write as _;
    use std::os::unix::fs::OpenOptionsExt as _;

    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(parent)?;
    let tmp = parent.join(".service.json.tmp");
    if let Err(error) = std::fs::remove_file(&tmp)
        && error.kind() != std::io::ErrorKind::NotFound
    {
        return Err(error);
    }
    let mut file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(&tmp)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    std::fs::rename(tmp, path)
}

#[cfg(not(unix))]
fn write_0600(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    std::fs::write(path, bytes)
}

/// Remove only the manifest for this directory after its exact backend has
/// confirmed uninstall.
pub(crate) fn remove(config_dir: &Path) -> Result<()> {
    let path = config_dir.join(MANIFEST_FILE);
    match std::fs::remove_file(&path) {
        Ok(()) => Ok(()),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(Error::Write { path, source }),
    }
}

/// Deterministic systemd unit name or launchd label for a canonical directory.
pub(crate) fn service_name_for_dir(
    canonical_directory: &Path,
    backend: ServiceSupervisor,
) -> Result<String> {
    let stem = name_stem_for_dir(canonical_directory)?;
    Ok(match backend {
        ServiceSupervisor::Systemd => format!("spiced-cloud-connect-{stem}.service"),
        ServiceSupervisor::Launchd => format!("ai.spice.cloud-connect.{stem}"),
    })
}

/// `<sanitized-final-component>-<16 lowercase hex>`, with a digest-only
/// fallback when the final component has no usable ASCII characters.
fn name_stem_for_dir(canonical_directory: &Path) -> Result<String> {
    let path = canonical_directory.to_str().ok_or_else(|| Error::Invalid {
        path: canonical_directory.to_path_buf(),
        reason: "the canonical directory is not valid UTF-8".to_string(),
    })?;
    let digest = Sha256::digest(path.as_bytes());
    let mut short = String::with_capacity(DIGEST_BYTES * 2);
    for byte in digest.iter().take(DIGEST_BYTES) {
        write!(short, "{byte:02x}").map_err(|_| Error::Invalid {
            path: canonical_directory.to_path_buf(),
            reason: "failed to format the service-name digest".to_string(),
        })?;
    }

    let fragment = canonical_directory
        .file_name()
        .and_then(|name| name.to_str())
        .map(sanitize_fragment)
        .filter(|fragment| !fragment.is_empty());
    Ok(fragment.map_or_else(|| short.clone(), |fragment| format!("{fragment}-{short}")))
}

fn sanitize_fragment(name: &str) -> String {
    let mut fragment = String::with_capacity(name.len().min(MAX_NAME_FRAGMENT));
    let mut pending_dash = false;
    for character in name.chars() {
        if character.is_ascii_alphanumeric() {
            if pending_dash && !fragment.is_empty() && fragment.len() < MAX_NAME_FRAGMENT {
                fragment.push('-');
            }
            pending_dash = false;
            if fragment.len() < MAX_NAME_FRAGMENT {
                fragment.push(character.to_ascii_lowercase());
            }
        } else {
            pending_dash = true;
        }
        if fragment.len() >= MAX_NAME_FRAGMENT {
            break;
        }
    }
    fragment.trim_matches('-').to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    fn manifest_for_test(directory: &Path) -> ServiceManifest {
        let canonical = std::fs::canonicalize(directory).expect("canonical test directory");
        ServiceManifest {
            schema_version: SCHEMA_VERSION,
            directory: canonical.clone(),
            name: service_name_for_dir(&canonical, ServiceSupervisor::Systemd)
                .expect("derive test service name"),
            scope: ServiceScope::User,
            backend: ServiceSupervisor::Systemd,
            owner: ServiceOwner {
                name: "test-owner".to_string(),
                uid: nix::unistd::Uid::effective().as_raw(),
            },
            definition_path: canonical.join("definition.service"),
            runtime_path: canonical.join("spiced"),
            log_source: "journald:test".to_string(),
            runtime_sha256: "a".repeat(64),
            runtime_version: "v2.2.0".to_string(),
            health_url: Some("http://127.0.0.1:8090/health".to_string()),
            state: ManifestState::Installed,
        }
    }

    #[test]
    fn deterministic_names_use_the_exact_fragment_and_digest_contract() {
        let directory = Path::new("/srv/edge-analytics");
        assert_eq!(
            service_name_for_dir(directory, ServiceSupervisor::Systemd)
                .expect("derive systemd name"),
            "spiced-cloud-connect-edge-analytics-59e8c853e76c15ba.service"
        );
        assert_eq!(
            service_name_for_dir(directory, ServiceSupervisor::Launchd)
                .expect("derive launchd label"),
            "ai.spice.cloud-connect.edge-analytics-59e8c853e76c15ba"
        );
    }

    #[test]
    fn fragment_collapses_runs_trims_and_stops_at_32_characters() {
        assert_eq!(sanitize_fragment("--My__Edge Site!!--"), "my-edge-site");
        assert_eq!(sanitize_fragment(&"A".repeat(80)), "a".repeat(32));
        assert!(sanitize_fragment("___").is_empty());
    }

    #[test]
    fn collision_checks_every_ownership_dimension() {
        let base = ServiceManifest {
            schema_version: SCHEMA_VERSION,
            directory: PathBuf::from("/srv/edge"),
            name: "spiced-cloud-connect-edge-deadbeefdeadbeef.service".to_string(),
            scope: ServiceScope::User,
            backend: ServiceSupervisor::Systemd,
            owner: ServiceOwner {
                name: "edge".to_string(),
                uid: 1000,
            },
            definition_path: PathBuf::from("/tmp/edge.service"),
            runtime_path: PathBuf::from("/tmp/spiced"),
            log_source: "journald".to_string(),
            runtime_sha256: "a".repeat(64),
            runtime_version: "v1".to_string(),
            health_url: Some("http://127.0.0.1:8090/health".to_string()),
            state: ManifestState::Installed,
        };
        base.ensure_same_service(&base)
            .expect("the same service identity is idempotent");

        let mut other = base.clone();
        other.owner.uid = 1001;
        let error = base
            .ensure_same_service(&other)
            .expect_err("different ownership must collide");
        assert!(matches!(error, Error::Collision { .. }));
    }

    #[cfg(unix)]
    #[test]
    fn validated_load_requires_owner_only_manifest_permissions() {
        use std::os::unix::fs::PermissionsExt as _;

        let directory = tempfile::TempDir::new().expect("create instance directory");
        let config = directory.path().join(".spice");
        let manifest = manifest_for_test(directory.path());
        write(&config, &manifest).expect("write valid manifest");
        assert_eq!(
            load_validated(&config, directory.path()).expect("load valid manifest"),
            Some(manifest)
        );

        std::fs::set_permissions(
            config.join(MANIFEST_FILE),
            std::fs::Permissions::from_mode(0o644),
        )
        .expect("broaden permissions");
        let error = load_validated(&config, directory.path())
            .expect_err("group/world-readable manifest must fail");
        assert!(error.to_string().contains("0600"), "{error}");
    }

    #[cfg(unix)]
    #[test]
    fn validated_load_rejects_manifest_and_asset_symlinks() {
        use std::os::unix::fs::symlink;

        let directory = tempfile::TempDir::new().expect("create instance directory");
        let config = directory.path().join(".spice");
        let manifest = manifest_for_test(directory.path());
        write(&config, &manifest).expect("write valid manifest");
        let real_manifest = config.join("real-service.json");
        std::fs::rename(config.join(MANIFEST_FILE), &real_manifest).expect("move real manifest");
        symlink(&real_manifest, config.join(MANIFEST_FILE)).expect("link manifest");
        let error =
            load_validated(&config, directory.path()).expect_err("manifest symlink must fail");
        assert!(error.to_string().contains("symlink"), "{error}");

        std::fs::remove_file(config.join(MANIFEST_FILE)).expect("remove manifest symlink");
        std::fs::rename(&real_manifest, config.join(MANIFEST_FILE)).expect("restore manifest");
        let real_definition = directory.path().join("real-definition.service");
        std::fs::write(&real_definition, "definition").expect("write definition");
        symlink(&real_definition, &manifest.definition_path).expect("link definition");
        let error =
            load_validated(&config, directory.path()).expect_err("definition symlink must fail");
        assert!(error.to_string().contains("definition_path"), "{error}");
    }

    #[cfg(unix)]
    #[test]
    fn validated_load_rejects_owner_and_directory_mismatches() {
        let directory = tempfile::TempDir::new().expect("create instance directory");
        let other = tempfile::TempDir::new().expect("create another directory");
        let config = directory.path().join(".spice");
        let mut manifest = manifest_for_test(directory.path());
        manifest.owner.uid = manifest.owner.uid.saturating_add(1);
        write(&config, &manifest).expect("write wrong-owner manifest");
        let error =
            load_validated(&config, directory.path()).expect_err("owner mismatch must fail");
        assert!(error.to_string().contains("owner uid"), "{error}");

        manifest.owner.uid = nix::unistd::Uid::effective().as_raw();
        write(&config, &manifest).expect("rewrite owner");
        let error =
            load_validated(&config, other.path()).expect_err("directory mismatch must fail");
        assert!(error.to_string().contains("invocation resolves"), "{error}");
    }
}
