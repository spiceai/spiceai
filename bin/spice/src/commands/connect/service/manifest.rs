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

//! `<config-dir>/service.json`: what this instance directory's service is.
//!
//! Every `spice connect service` action resolves the service it operates on
//! from the canonical instance directory and this file, and from nothing else.
//! There is no name argument and no host-wide scan, because both can name a
//! service belonging to a different instance — and a lifecycle command that
//! controls the wrong process is worse than one that refuses.
//!
//! The manifest is therefore a claim that has to be checked, not trusted: it
//! is written owner-only, and a load re-derives the service name from the
//! directory and refuses anything that does not match what the file says.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use super::backend::ServiceBackend;
use super::model::{LogSource, ServiceScope, Supervisor};
use crate::error::{Error, Result};

/// File name (relative to the resolved Spice config dir) of the manifest.
pub(crate) const SERVICE_MANIFEST_FILE: &str = "service.json";

/// Current manifest schema. A manifest from a newer CLI is refused rather
/// than partially understood: acting on half a description of a service is
/// how the wrong process gets stopped.
pub(crate) const MANIFEST_SCHEMA_VERSION: u32 = 1;

/// The account the installed runtime runs as.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ServiceOwner {
    pub(crate) uid: u32,
    pub(crate) gid: u32,
    /// The local account name, when the host has one for `uid`. Human status
    /// prints this; the numeric id is what the definition is written with.
    #[serde(default)]
    pub(crate) name: Option<String>,
}

impl ServiceOwner {
    /// How the owner is named in human output: the account name when the host
    /// knows one, and the numeric id otherwise.
    pub(crate) fn describe(&self) -> String {
        match &self.name {
            Some(name) => name.clone(),
            None => format!("uid {}", self.uid),
        }
    }
}

/// The complete description of the service installed for one instance
/// directory.
///
/// Field order is the JSON field order; the file is a Spice-owned record
/// rather than a published automation schema, but it is still versioned so a
/// downgrade fails loudly.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ServiceManifest {
    pub(crate) schema_version: u32,
    /// The canonical instance directory this service was installed for. The
    /// authoritative half of the identity check: a manifest that names a
    /// different directory belongs to a different instance.
    pub(crate) directory: PathBuf,
    /// The deterministic service name derived from `directory`.
    pub(crate) name: String,
    pub(crate) scope: ServiceScope,
    pub(crate) supervisor: Supervisor,
    pub(crate) owner: ServiceOwner,
    /// The unit file or plist.
    pub(crate) definition_path: PathBuf,
    /// The staged `spiced` the service executes.
    pub(crate) runtime_path: PathBuf,
    /// `None` when the definition names no log source this CLI can read.
    pub(crate) log_source: Option<LogSource>,
    /// SHA-256 (lowercase hex) of the staged runtime at install time, so an
    /// upgrade can tell whether the binary on the path the service executes is
    /// still the one that was installed.
    pub(crate) runtime_digest: String,
    /// The `spiced` version string recorded at install time.
    pub(crate) runtime_version: String,
    /// Where this instance answers a health probe.
    pub(crate) health_url: String,
}

impl ServiceManifest {
    /// Absolute path of the manifest for a resolved config dir.
    pub(crate) fn path_in(config_dir: &Path) -> PathBuf {
        config_dir.join(SERVICE_MANIFEST_FILE)
    }

    /// Read and validate the manifest for `instance_dir`.
    ///
    /// `Ok(None)` means there is no manifest — a directory that never had a
    /// service installed, or one whose manifest was lost while its definition
    /// is still installed (see [`super::adopt_installed_definition`]). An
    /// unreadable, malformed, or mismatched manifest is an error rather than a
    /// `None`: it describes *something*, and guessing what would be a lifecycle
    /// command aimed at an unverified target.
    ///
    /// # Errors
    ///
    /// Returns an error when the file cannot be read, does not parse, is not
    /// owner-only, or does not describe `instance_dir`.
    pub(crate) fn load(
        config_dir: &Path,
        instance_dir: &Path,
        backend: &dyn ServiceBackend,
    ) -> Result<Option<Self>> {
        let path = Self::path_in(config_dir);
        let metadata = match std::fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => {
                return Err(Error::CloudConnectIo {
                    message: format!("inspect the service manifest {}: {e}", path.display()),
                });
            }
        };
        ensure_owner_only(&path, &metadata)?;

        let bytes = std::fs::read(&path).map_err(|e| Error::CloudConnectIo {
            message: format!("read the service manifest {}: {e}", path.display()),
        })?;
        let manifest: Self =
            serde_json::from_slice(&bytes).map_err(|e| Error::InvalidArgument {
                message: format!(
                    "Failed to read the Spice Cloud Connect service manifest {}: {e}. \
                 Re-run `spice connect service install` to rewrite it, or delete the file to \
                 forget the installed service. See: https://spiceai.org/docs",
                    path.display()
                ),
            })?;

        manifest.validate(&path, instance_dir, backend)?;
        Ok(Some(manifest))
    }

    /// Check that this manifest describes the service for `instance_dir`.
    ///
    /// # Errors
    ///
    /// Returns an error when the schema is from another release, the recorded
    /// directory is not the one being resolved, the supervisor is not this
    /// host's, the name is not the one that directory derives, or a recorded
    /// path is not absolute.
    fn validate(
        &self,
        path: &Path,
        instance_dir: &Path,
        backend: &dyn ServiceBackend,
    ) -> Result<()> {
        let reject = |reason: String| {
            Err(Error::InvalidArgument {
                message: format!(
                    "Failed to resolve the Spice Cloud Connect service for {instance}: \
                     its manifest {manifest} {reason}. Re-run `spice connect service install` \
                     from this directory to rewrite it. See: https://spiceai.org/docs",
                    instance = instance_dir.display(),
                    manifest = path.display(),
                ),
            })
        };

        if self.schema_version != MANIFEST_SCHEMA_VERSION {
            return reject(format!(
                "is schema version {} and this CLI understands version {MANIFEST_SCHEMA_VERSION}",
                self.schema_version
            ));
        }
        if !self.directory.is_absolute() {
            return reject(format!(
                "records a relative instance directory ({}), so the service it names depends \
                 on where a command was run",
                self.directory.display()
            ));
        }
        if self.directory != instance_dir {
            return reject(format!(
                "describes the service for {} instead",
                self.directory.display()
            ));
        }
        if self.supervisor != backend.supervisor() {
            return reject(format!(
                "was written for {} and this host is managed by {}",
                self.supervisor,
                backend.supervisor()
            ));
        }
        let derived = backend.name_for_dir(instance_dir);
        if self.name != derived {
            return reject(format!(
                "names the service {} where this directory derives {derived}",
                self.name
            ));
        }
        // The definition path is what an uninstall deletes, so it is not enough
        // for it to look plausible: it has to be the exact path this back end
        // would write for this name in this scope. Otherwise a corrupted or
        // hand-edited manifest can pair the derived name with any absolute path
        // and have `uninstall` remove an unrelated file.
        let expected_definition = backend.definition_path(&derived, self.scope);
        if self.definition_path != expected_definition {
            return reject(format!(
                "records the definition {} where a {scope} {supervisor} service for this \
                 directory is {expected}",
                self.definition_path.display(),
                scope = self.scope,
                supervisor = self.supervisor,
                expected = expected_definition.display(),
            ));
        }
        for (label, recorded) in [
            ("definition", &self.definition_path),
            ("runtime", &self.runtime_path),
        ] {
            if !recorded.is_absolute() {
                return reject(format!(
                    "records a relative {label} path ({})",
                    recorded.display()
                ));
            }
        }
        Ok(())
    }

    /// Write the manifest into `config_dir`, owner-only and atomically.
    ///
    /// The rename is what keeps a concurrent read from seeing a half-written
    /// description of the service it is about to control.
    ///
    /// # Errors
    ///
    /// Returns an error when the manifest cannot be serialized or written.
    pub(crate) fn write(&self, config_dir: &Path) -> Result<()> {
        let path = Self::path_in(config_dir);
        let json = serde_json::to_vec_pretty(self).map_err(|e| Error::CloudConnectIo {
            message: format!("serialize the service manifest for {}: {e}", path.display()),
        })?;

        std::fs::create_dir_all(config_dir).map_err(|e| Error::CloudConnectIo {
            message: format!(
                "create the Spice config directory {}: {e}",
                config_dir.display()
            ),
        })?;

        let staging = path.with_extension("json.incoming");
        let _ = std::fs::remove_file(&staging);
        write_owner_only(&staging, &json)?;
        std::fs::rename(&staging, &path).map_err(|e| {
            let _ = std::fs::remove_file(&staging);
            Error::CloudConnectIo {
                message: format!("write the service manifest {}: {e}", path.display()),
            }
        })
    }

    /// Delete the manifest, if there is one.
    ///
    /// # Errors
    ///
    /// Returns an error when the file exists and cannot be removed — a
    /// manifest left behind would claim a service that is no longer installed.
    pub(crate) fn remove(config_dir: &Path) -> Result<()> {
        let path = Self::path_in(config_dir);
        match std::fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(Error::CloudConnectIo {
                message: format!("remove the service manifest {}: {e}", path.display()),
            }),
        }
    }
}

/// Refuse a manifest anyone but its owner can change.
///
/// Every lifecycle command reads this file to decide which service to control
/// and which binary that service runs. A group- or world-writable manifest, or
/// a symlink standing in for one, is a way to redirect that decision.
#[cfg(unix)]
fn ensure_owner_only(path: &Path, metadata: &std::fs::Metadata) -> Result<()> {
    use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};

    if metadata.file_type().is_symlink() {
        return Err(Error::InvalidArgument {
            message: format!(
                "Failed to read the Spice Cloud Connect service manifest {}: it is a symlink, \
                 so it cannot be trusted to describe this directory's service. Replace it with \
                 a real file by re-running `spice connect service install`. \
                 See: https://spiceai.org/docs",
                path.display()
            ),
        });
    }

    let mode = metadata.permissions().mode() & 0o7777;
    if mode & 0o077 != 0 {
        return Err(Error::InvalidArgument {
            message: format!(
                "Failed to read the Spice Cloud Connect service manifest {}: mode {mode:04o} \
                 lets accounts other than its owner read or change which service and which \
                 binary Spice controls. Restrict it (`chmod 600 {}`) and try again. \
                 See: https://spiceai.org/docs",
                path.display(),
                path.display()
            ),
        });
    }

    let owner = metadata.uid();
    let effective = nix::unistd::Uid::effective().as_raw();
    if owner != 0 && owner != effective {
        return Err(Error::InvalidArgument {
            message: format!(
                "Failed to read the Spice Cloud Connect service manifest {}: it is owned by uid \
                 {owner}, but this command runs as uid {effective}. Run it as the owning account, \
                 or re-run `spice connect service install` to reclaim the directory. \
                 See: https://spiceai.org/docs",
                path.display()
            ),
        });
    }
    Ok(())
}

#[cfg(not(unix))]
fn ensure_owner_only(_path: &Path, _metadata: &std::fs::Metadata) -> Result<()> {
    Ok(())
}

/// Create `path` with owner-only permissions from the first byte and write
/// `bytes` into it.
///
/// `create_new` rather than `create`: inheriting a file someone else left
/// behind would inherit its mode and its owner too.
#[cfg(unix)]
fn write_owner_only(path: &Path, bytes: &[u8]) -> Result<()> {
    use std::io::Write as _;
    use std::os::unix::fs::OpenOptionsExt as _;

    let io_error = |e: std::io::Error| Error::CloudConnectIo {
        message: format!("write the service manifest {}: {e}", path.display()),
    };
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .map_err(io_error)?;
    file.write_all(bytes).map_err(io_error)?;
    file.sync_all().map_err(io_error)
}

#[cfg(not(unix))]
fn write_owner_only(path: &Path, bytes: &[u8]) -> Result<()> {
    std::fs::write(path, bytes).map_err(|e| Error::CloudConnectIo {
        message: format!("write the service manifest {}: {e}", path.display()),
    })
}

#[cfg(test)]
mod tests {
    use super::super::backend::fake::FakeBackend;
    use super::*;

    fn manifest_for(backend: &FakeBackend, instance_dir: &Path) -> ServiceManifest {
        let scope = ServiceScope::System;
        let name = backend.name_for_dir(instance_dir);
        ServiceManifest {
            schema_version: MANIFEST_SCHEMA_VERSION,
            directory: instance_dir.to_path_buf(),
            name: name.clone(),
            scope,
            supervisor: backend.supervisor(),
            owner: ServiceOwner {
                uid: 1000,
                gid: 1000,
                name: Some("alice".to_string()),
            },
            definition_path: backend.definition_path(&name, scope),
            runtime_path: PathBuf::from("/usr/local/lib/spice/spiced"),
            log_source: backend.log_source(&name, scope),
            runtime_digest: "0".repeat(64),
            runtime_version: "v2.2.0".to_string(),
            health_url: "http://127.0.0.1:8090/health".to_string(),
        }
    }

    #[test]
    fn a_written_manifest_round_trips() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let fake = FakeBackend::new(dir.path());
        let instance_dir = dir.path().join("edge-1");
        let config_dir = instance_dir.join(".spice");
        let manifest = manifest_for(&fake, &instance_dir);

        manifest.write(&config_dir).expect("write manifest");
        let loaded = ServiceManifest::load(&config_dir, &instance_dir, &fake)
            .expect("load manifest")
            .expect("a manifest was written");
        assert_eq!(loaded, manifest);
    }

    #[test]
    fn an_absent_manifest_is_not_an_error() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let fake = FakeBackend::new(dir.path());
        assert_eq!(
            ServiceManifest::load(dir.path(), &dir.path().join("edge-1"), &fake).expect("load"),
            None
        );
    }

    #[test]
    fn a_manifest_for_another_directory_is_refused() {
        // The failure this check exists for: a copied instance directory whose
        // manifest still names the original's service. Acting on it would stop
        // a service belonging to a different instance.
        let dir = tempfile::tempdir().expect("create tempdir");
        let fake = FakeBackend::new(dir.path());
        let instance_dir = dir.path().join("edge-1");
        let config_dir = dir.path().join("edge-2").join(".spice");
        manifest_for(&fake, &instance_dir)
            .write(&config_dir)
            .expect("write manifest");

        let error = ServiceManifest::load(&config_dir, &dir.path().join("edge-2"), &fake)
            .expect_err("a manifest naming another directory must not resolve");
        assert!(
            error.to_string().contains("describes the service for"),
            "{error}"
        );
    }

    #[test]
    fn a_manifest_with_a_forged_name_is_refused() {
        // The name is derived from the directory, so a manifest that names
        // something else is naming a service this directory does not own.
        let dir = tempfile::tempdir().expect("create tempdir");
        let fake = FakeBackend::new(dir.path());
        let instance_dir = dir.path().join("edge-1");
        let config_dir = instance_dir.join(".spice");
        let mut manifest = manifest_for(&fake, &instance_dir);
        manifest.name = "spiced-cloud-connect-someone-else.service".to_string();
        manifest.write(&config_dir).expect("write manifest");

        let error = ServiceManifest::load(&config_dir, &instance_dir, &fake)
            .expect_err("a forged service name must not resolve");
        assert!(error.to_string().contains("derives"), "{error}");
    }

    #[test]
    fn a_manifest_written_for_another_supervisor_is_refused() {
        // An instance directory carried between a Linux and a macOS host has a
        // manifest naming a supervisor that is not there. Trusting its
        // definition path would point every command at a file this host's
        // supervisor knows nothing about.
        let dir = tempfile::tempdir().expect("create tempdir");
        let fake = FakeBackend::new(dir.path());
        let instance_dir = dir.path().join("edge-1");
        let config_dir = instance_dir.join(".spice");
        let mut manifest = manifest_for(&fake, &instance_dir);
        manifest.supervisor = Supervisor::Launchd;
        manifest.write(&config_dir).expect("write manifest");

        let error = ServiceManifest::load(&config_dir, &instance_dir, &fake)
            .expect_err("a manifest for another supervisor must not resolve");
        assert!(
            error.to_string().contains("this host is managed by"),
            "{error}"
        );
    }

    #[test]
    fn a_manifest_from_another_schema_version_is_refused() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let fake = FakeBackend::new(dir.path());
        let instance_dir = dir.path().join("edge-1");
        let config_dir = instance_dir.join(".spice");
        let mut manifest = manifest_for(&fake, &instance_dir);
        manifest.schema_version = MANIFEST_SCHEMA_VERSION + 1;
        manifest.write(&config_dir).expect("write manifest");

        let error = ServiceManifest::load(&config_dir, &instance_dir, &fake)
            .expect_err("a future schema must not be half-understood");
        assert!(error.to_string().contains("schema version"), "{error}");
    }

    #[test]
    fn a_relative_recorded_path_is_refused() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let fake = FakeBackend::new(dir.path());
        let instance_dir = dir.path().join("edge-1");
        let config_dir = instance_dir.join(".spice");
        let mut manifest = manifest_for(&fake, &instance_dir);
        manifest.runtime_path = PathBuf::from("relative/spiced");
        manifest.write(&config_dir).expect("write manifest");

        let error = ServiceManifest::load(&config_dir, &instance_dir, &fake)
            .expect_err("a relative runtime path must not resolve");
        assert!(
            error.to_string().contains("relative runtime path"),
            "{error}"
        );
    }

    #[test]
    fn a_manifest_pointing_at_an_unrelated_definition_is_refused() {
        // An uninstall deletes this path, so a manifest that pairs the derived
        // name with any absolute path could have Spice remove an unrelated file.
        let dir = tempfile::tempdir().expect("create tempdir");
        let fake = FakeBackend::new(dir.path());
        let instance_dir = dir.path().join("edge-1");
        let config_dir = instance_dir.join(".spice");
        let mut manifest = manifest_for(&fake, &instance_dir);
        manifest.definition_path = PathBuf::from("/etc/passwd");
        manifest.write(&config_dir).expect("write manifest");

        let error = ServiceManifest::load(&config_dir, &instance_dir, &fake)
            .expect_err("an unrelated definition path must not resolve");
        assert!(error.to_string().contains("/etc/passwd"), "{error}");
        assert!(
            error.to_string().contains("records the definition"),
            "{error}"
        );
    }

    #[test]
    fn a_manifest_recording_a_relative_instance_directory_is_refused() {
        // A relative instance directory makes the service's name depend on the
        // process working directory, so the same instance derives two names.
        let dir = tempfile::tempdir().expect("create tempdir");
        let fake = FakeBackend::new(dir.path());
        let instance_dir = PathBuf::from("edge-1");
        let config_dir = dir.path().join("edge-1").join(".spice");
        let mut manifest = manifest_for(&fake, &instance_dir);
        manifest.directory = instance_dir.clone();
        manifest.write(&config_dir).expect("write manifest");

        let error = ServiceManifest::load(&config_dir, &instance_dir, &fake)
            .expect_err("a relative instance directory must not resolve");
        assert!(
            error.to_string().contains("relative instance directory"),
            "{error}"
        );
    }

    #[test]
    fn a_malformed_manifest_is_an_error_rather_than_no_service() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let fake = FakeBackend::new(dir.path());
        let instance_dir = dir.path().join("edge-1");
        let config_dir = instance_dir.join(".spice");
        std::fs::create_dir_all(&config_dir).expect("create config dir");
        std::fs::write(ServiceManifest::path_in(&config_dir), "not json").expect("write");

        let error = ServiceManifest::load(&config_dir, &instance_dir, &fake)
            .expect_err("a manifest that describes something unreadable must be reported");
        assert!(error.to_string().contains("service manifest"), "{error}");
    }

    #[test]
    fn removing_an_absent_manifest_succeeds() {
        let dir = tempfile::tempdir().expect("create tempdir");
        ServiceManifest::remove(dir.path()).expect("idempotent removal");
    }

    #[cfg(unix)]
    #[test]
    fn a_group_readable_manifest_is_refused() {
        use std::os::unix::fs::PermissionsExt as _;

        let dir = tempfile::tempdir().expect("create tempdir");
        let fake = FakeBackend::new(dir.path());
        let instance_dir = dir.path().join("edge-1");
        let config_dir = instance_dir.join(".spice");
        manifest_for(&fake, &instance_dir)
            .write(&config_dir)
            .expect("write manifest");

        let path = ServiceManifest::path_in(&config_dir);
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644))
            .expect("widen the mode");
        let error = ServiceManifest::load(&config_dir, &instance_dir, &fake)
            .expect_err("a manifest others can change must not decide what Spice controls");
        assert!(error.to_string().contains("0644"), "{error}");
    }

    #[cfg(unix)]
    #[test]
    fn a_manifest_is_written_owner_only() {
        use std::os::unix::fs::PermissionsExt as _;

        let dir = tempfile::tempdir().expect("create tempdir");
        let fake = FakeBackend::new(dir.path());
        let instance_dir = dir.path().join("edge-1");
        let config_dir = instance_dir.join(".spice");
        manifest_for(&fake, &instance_dir)
            .write(&config_dir)
            .expect("write manifest");

        let mode = std::fs::metadata(ServiceManifest::path_in(&config_dir))
            .expect("stat manifest")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600, "mode {mode:04o}");
    }

    #[cfg(unix)]
    #[test]
    fn a_symlinked_manifest_is_refused() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let fake = FakeBackend::new(dir.path());
        let instance_dir = dir.path().join("edge-1");
        let config_dir = instance_dir.join(".spice");
        let elsewhere = dir.path().join("elsewhere.json");
        manifest_for(&fake, &instance_dir)
            .write(dir.path())
            .expect("write manifest");
        std::fs::rename(ServiceManifest::path_in(dir.path()), &elsewhere).expect("move it aside");
        std::fs::create_dir_all(&config_dir).expect("create config dir");
        std::os::unix::fs::symlink(&elsewhere, ServiceManifest::path_in(&config_dir))
            .expect("symlink the manifest");

        let error = ServiceManifest::load(&config_dir, &instance_dir, &fake)
            .expect_err("a symlinked manifest must not resolve a service");
        assert!(error.to_string().contains("symlink"), "{error}");
    }

    #[test]
    fn rewriting_a_manifest_replaces_it() {
        // Install is idempotent, so the second write must not trip over the
        // first one's file.
        let dir = tempfile::tempdir().expect("create tempdir");
        let fake = FakeBackend::new(dir.path());
        let instance_dir = dir.path().join("edge-1");
        let config_dir = instance_dir.join(".spice");
        let mut manifest = manifest_for(&fake, &instance_dir);
        manifest.write(&config_dir).expect("first write");
        manifest.runtime_version = "v2.3.0".to_string();
        manifest.write(&config_dir).expect("second write");

        let loaded = ServiceManifest::load(&config_dir, &instance_dir, &fake)
            .expect("load")
            .expect("a manifest is present");
        assert_eq!(loaded.runtime_version, "v2.3.0");
    }
}
