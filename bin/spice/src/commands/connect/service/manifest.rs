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

/// [`SERVICE_MANIFEST_FILE`] as a C string, for the descriptor-relative
/// syscalls. Spelled as a literal rather than built with `CString::new` so this
/// fixed production path cannot trip the workspace's denied `expect_used` lint;
/// `the_two_manifest_file_names_agree` keeps the two spellings in step.
const SERVICE_MANIFEST_FILE_C: &std::ffi::CStr = c"service.json";

/// Current manifest schema. A manifest from a newer CLI is refused rather
/// than partially understood: acting on half a description of a service is
/// how the wrong process gets stopped.
pub(crate) const MANIFEST_SCHEMA_VERSION: u32 = 1;

const MAX_SERVICE_MANIFEST_BYTES: u64 = 1024 * 1024;

/// The config directory a manifest operation applies to.
///
/// A [`runtime_cloud_connect::MutationLock`] holds its directory open for the
/// whole operation, and that descriptor is the one name for the directory its
/// owner cannot replace. Carrying it here is what lets an install or uninstall
/// resolve the instance by inode: a pathname re-resolved after the lock's
/// identity check can still name a *different* directory the same owner
/// created, and the ownership test alone cannot tell the two apart. Without a
/// lock there is nothing to pin, and the pathname is all a read has.
pub(crate) struct PinnedConfigDir {
    path: PathBuf,
    #[cfg(unix)]
    directory: Option<std::fs::File>,
}

impl PinnedConfigDir {
    /// A directory reached by pathname, for a caller holding no mutation lock.
    pub(crate) fn unlocked(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            #[cfg(unix)]
            directory: None,
        }
    }

    /// The directory a mutation lock retains, named by the descriptor it holds
    /// open. `path` is what messages show; every lookup goes through
    /// `directory`.
    #[cfg(unix)]
    pub(crate) fn locked(path: impl Into<PathBuf>, directory: std::fs::File) -> Self {
        Self {
            path: path.into(),
            directory: Some(directory),
        }
    }

    /// The directory a mutation lock holds, taking the descriptor it retains
    /// where the platform has one.
    ///
    /// # Errors
    ///
    /// Returns an error when the retained descriptor cannot be duplicated.
    pub(crate) fn for_lock(
        path: impl Into<PathBuf>,
        lock: &runtime_cloud_connect::MutationLock,
    ) -> Result<Self> {
        #[cfg(unix)]
        {
            let directory = lock
                .pinned_directory()
                .map_err(|source| Error::CloudConnectIo {
                    message: format!("pin the locked Cloud Connect config directory: {source}"),
                })?;
            Ok(Self::locked(path, directory))
        }
        #[cfg(not(unix))]
        {
            let _ = lock;
            Ok(Self::unlocked(path))
        }
    }

    /// What messages call this directory. When it is pinned this is only a
    /// name for the operator to read: the directory it resolves to is exactly
    /// what a locked operation must not trust, so every lookup goes through
    /// [`PinnedConfigDir::descriptor`] instead.
    fn display_path(&self) -> &Path {
        &self.path
    }

    /// The retained descriptor, when this directory is held by a lock.
    #[cfg(unix)]
    fn descriptor(&self) -> Option<&std::fs::File> {
        self.directory.as_ref()
    }
}

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
        config: &PinnedConfigDir,
        instance_dir: &Path,
        backend: &dyn ServiceBackend,
    ) -> Result<Option<Self>> {
        let path = Self::path_in(config.display_path());
        let (bytes, metadata) = match read_manifest_bytes(config, &path) {
            Ok(Some(contents)) => contents,
            Ok(None) => return Ok(None),
            Err(e) if is_symlink_loop(&e) => {
                return Err(Error::InvalidArgument {
                    message: format!(
                        "Failed to read the Spice Cloud Connect service manifest {}: it is a symlink. Remove it and re-run `spice connect service install`.",
                        path.display()
                    ),
                });
            }
            Err(e) => {
                return Err(Error::CloudConnectIo {
                    message: format!("read the service manifest {} safely: {e}", path.display()),
                });
            }
        };
        let manifest: Self =
            serde_json::from_slice(&bytes).map_err(|e| Error::InvalidArgument {
                message: format!(
                    "Failed to read the Spice Cloud Connect service manifest {}: {e}. \
                 Re-run `spice connect service install` to rewrite it, or delete the file to \
                 forget the installed service. See: https://spiceai.org/docs",
                    path.display()
                ),
            })?;
        ensure_owner_only(&path, &metadata, &manifest.owner)?;

        manifest.validate(&path, instance_dir, backend, config)?;
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
        config: &PinnedConfigDir,
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
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt as _;

            let config_dir = config.display_path();
            // A pinned directory is described by the descriptor the lock holds
            // open, so there is no name here to be a symlink or to be replaced
            // between this check and the operation that follows it.
            let (metadata, pinned) = match config.descriptor() {
                Some(directory) => (directory.metadata(), true),
                None => (std::fs::symlink_metadata(config_dir), false),
            };
            let metadata = metadata.map_err(|source| Error::CloudConnectIo {
                message: format!(
                    "inspect service manifest directory {}: {source}",
                    config_dir.display()
                ),
            })?;
            if (!pinned && metadata.file_type().is_symlink())
                || !metadata.is_dir()
                || metadata.uid() != self.owner.uid
            {
                return reject(format!(
                    "records owner uid {}, but its config directory {} is owned by uid {} or is not a real directory",
                    self.owner.uid,
                    config_dir.display(),
                    metadata.uid()
                ));
            }
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
    pub(crate) fn write(&self, config: &PinnedConfigDir) -> Result<()> {
        let config_dir = config.display_path();
        let path = Self::path_in(config_dir);
        let json = serde_json::to_vec_pretty(self).map_err(|e| Error::CloudConnectIo {
            message: format!("serialize the service manifest for {}: {e}", path.display()),
        })?;
        if u64::try_from(json.len()).unwrap_or(u64::MAX) > MAX_SERVICE_MANIFEST_BYTES {
            return Err(Error::InvalidArgument {
                message: format!(
                    "Refusing to write the Spice Cloud Connect service manifest {} because it exceeds the {MAX_SERVICE_MANIFEST_BYTES}-byte limit.",
                    path.display()
                ),
            });
        }

        #[cfg(unix)]
        {
            // A system-scope install writes as root into a directory owned by
            // the service account. Pin that directory first and perform every
            // subsequent operation relative to its descriptor. The owner can
            // rename the directory or replace its pathname with a symlink, but
            // neither can redirect this privileged write after validation.
            //
            // A pinned directory needs no creating — taking the lock created
            // it — and creating it by name here would build whatever the
            // pathname now points at, which is what the descriptor exists to
            // stop being trusted.
            if config.descriptor().is_none() && !nix::unistd::Uid::effective().is_root() {
                std::fs::create_dir_all(config_dir).map_err(|e| Error::CloudConnectIo {
                    message: format!(
                        "create the Spice config directory {}: {e}",
                        config_dir.display()
                    ),
                })?;
            }
            let directory = open_pinned_manifest_directory(config, &self.owner)?;
            write_manifest_in_directory(&directory, &path, &json, &self.owner)
        }

        #[cfg(not(unix))]
        {
            std::fs::create_dir_all(config_dir).map_err(|e| Error::CloudConnectIo {
                message: format!(
                    "create the Spice config directory {}: {e}",
                    config_dir.display()
                ),
            })?;

            let staging = path.with_extension("json.incoming");
            let _ = std::fs::remove_file(&staging);
            write_owner_only(&staging, &json, &self.owner)?;
            std::fs::rename(&staging, &path).map_err(|e| {
                let _ = std::fs::remove_file(&staging);
                Error::CloudConnectIo {
                    message: format!("write the service manifest {}: {e}", path.display()),
                }
            })?;
            sync_manifest_directory(config_dir).map_err(|e| Error::CloudConnectIo {
                message: format!(
                    "synchronize the service manifest directory {}: {e}",
                    config_dir.display()
                ),
            })
        }
    }

    /// Delete the manifest, if there is one.
    ///
    /// # Errors
    ///
    /// Returns an error when the file exists and cannot be removed — a
    /// manifest left behind would claim a service that is no longer installed.
    pub(crate) fn remove(&self, config: &PinnedConfigDir) -> Result<()> {
        let config_dir = config.display_path();
        let path = Self::path_in(config_dir);

        #[cfg(unix)]
        {
            // Pin, reopen, and revalidate the exact manifest before unlinking
            // it relative to the same descriptor. A service account may rename
            // its config directory while a root uninstall is in progress; no
            // path lookup after this point may follow that replacement.
            let directory = open_pinned_manifest_directory(config, &self.owner)?;
            remove_manifest_in_directory(&directory, &path, self)
        }

        #[cfg(not(unix))]
        {
            match std::fs::remove_file(&path) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => {
                    return Err(Error::CloudConnectIo {
                        message: format!("remove the service manifest {}: {e}", path.display()),
                    });
                }
            }
            match sync_manifest_directory(config_dir) {
                Ok(()) => Ok(()),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(e) => Err(Error::CloudConnectIo {
                    message: format!(
                        "synchronize the removed service manifest in {}: {e}",
                        config_dir.display()
                    ),
                }),
            }
        }
    }
}

/// Flush a non-Unix manifest directory as far as the platform permits.
#[cfg(not(unix))]
fn sync_manifest_directory(path: &Path) -> std::io::Result<()> {
    std::fs::File::open(path)?.sync_all()
}

#[cfg(unix)]
fn is_symlink_loop(error: &std::io::Error) -> bool {
    error.raw_os_error() == Some(nix::libc::ELOOP)
}

#[cfg(not(unix))]
fn is_symlink_loop(_error: &std::io::Error) -> bool {
    false
}

/// Refuse a manifest anyone but its owner can change.
///
/// Every lifecycle command reads this file to decide which service to control
/// and which binary that service runs. A group- or world-writable manifest, or
/// a symlink standing in for one, is a way to redirect that decision.
#[cfg(unix)]
fn ensure_owner_only(
    path: &Path,
    metadata: &std::fs::Metadata,
    intended_owner: &ServiceOwner,
) -> Result<()> {
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
    if owner != intended_owner.uid {
        return Err(Error::InvalidArgument {
            message: format!(
                "Failed to read the Spice Cloud Connect service manifest {}: it is owned by uid \
                 {owner}, but it records the intended operator as uid {}. Re-run \
                 `spice connect service install` from that operator account to rewrite it. \
                 See: https://spiceai.org/docs",
                path.display(),
                intended_owner.uid,
            ),
        });
    }
    Ok(())
}

#[cfg(not(unix))]
fn ensure_owner_only(
    _path: &Path,
    _metadata: &std::fs::Metadata,
    _intended_owner: &ServiceOwner,
) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn open_pinned_manifest_directory(
    config: &PinnedConfigDir,
    owner: &ServiceOwner,
) -> Result<std::fs::File> {
    use std::ffi::CString;
    use std::os::fd::{AsRawFd as _, FromRawFd as _};
    use std::os::unix::ffi::OsStrExt as _;
    use std::os::unix::fs::MetadataExt as _;

    let config_dir = config.display_path();
    let io_error = |e: std::io::Error| Error::CloudConnectIo {
        message: format!(
            "open the service manifest directory {} safely: {e}",
            config_dir.display()
        ),
    };
    let directory = if let Some(retained) = config.descriptor() {
        // The mutation lock already holds the protected directory open, and
        // that descriptor is the authority for which inode is protected.
        // Duplicating it keeps the identity this operation was authorized
        // against; resolving the pathname again would let the owner point it
        // at another directory they also own, which the ownership test below
        // cannot distinguish.
        retained.try_clone().map_err(io_error)?
    } else {
        let canonical = std::fs::canonicalize(config_dir).map_err(io_error)?;
        if !canonical.is_absolute() {
            return Err(Error::InvalidArgument {
                message: format!(
                    "Refusing to write a service manifest through non-absolute config directory {}.",
                    config_dir.display()
                ),
            });
        }

        let root = c"/";
        let root_fd = unsafe {
            nix::libc::open(
                root.as_ptr(),
                nix::libc::O_RDONLY | nix::libc::O_DIRECTORY | nix::libc::O_CLOEXEC,
            )
        };
        if root_fd < 0 {
            return Err(io_error(std::io::Error::last_os_error()));
        }
        let mut directory = unsafe { std::fs::File::from_raw_fd(root_fd) };
        for component in canonical.components() {
            match component {
                std::path::Component::RootDir | std::path::Component::CurDir => {}
                std::path::Component::Normal(name) => {
                    let name =
                        CString::new(name.as_bytes()).map_err(|_| Error::InvalidArgument {
                            message: format!(
                                "The Spice config directory {} contains a NUL byte.",
                                config_dir.display()
                            ),
                        })?;
                    let fd = unsafe {
                        nix::libc::openat(
                            directory.as_raw_fd(),
                            name.as_ptr(),
                            nix::libc::O_RDONLY
                                | nix::libc::O_DIRECTORY
                                | nix::libc::O_CLOEXEC
                                | nix::libc::O_NOFOLLOW,
                        )
                    };
                    if fd < 0 {
                        return Err(io_error(std::io::Error::last_os_error()));
                    }
                    directory = unsafe { std::fs::File::from_raw_fd(fd) };
                }
                std::path::Component::ParentDir | std::path::Component::Prefix(_) => {
                    return Err(Error::InvalidArgument {
                        message: format!(
                            "The canonical Spice config directory {} is not an absolute Unix path.",
                            canonical.display()
                        ),
                    });
                }
            }
        }
        directory
    };

    let metadata = directory.metadata().map_err(io_error)?;
    if !metadata.is_dir() || metadata.uid() != owner.uid {
        return Err(Error::InvalidArgument {
            message: format!(
                "Refusing to write the service manifest in {}: the pinned directory is owned by uid {}, not the service owner uid {}.",
                config_dir.display(),
                metadata.uid(),
                owner.uid,
            ),
        });
    }
    let effective = nix::unistd::Uid::effective().as_raw();
    if effective != 0 && effective != owner.uid {
        return Err(Error::InvalidArgument {
            message: format!(
                "Refusing to write a service manifest in {} for uid {} while running as uid {effective}.",
                config_dir.display(),
                owner.uid,
            ),
        });
    }
    Ok(directory)
}

/// Read `service.json` under `config`, applying the type, size, and link-count
/// bounds a manifest read applies however the directory was reached.
///
/// `Ok(None)` means there is no manifest. A pinned directory is read relative
/// to its retained descriptor, so the file that is read belongs to the
/// instance the lock was taken for and not to whatever the config directory's
/// pathname names by the time the read happens.
fn read_manifest_bytes(
    config: &PinnedConfigDir,
    path: &Path,
) -> std::io::Result<Option<(Vec<u8>, std::fs::Metadata)>> {
    #[cfg(unix)]
    if let Some(directory) = config.descriptor() {
        return read_manifest_in_directory(directory);
    }

    match super::super::state::read_bounded_regular_file_with_metadata(
        path,
        MAX_SERVICE_MANIFEST_BYTES,
    ) {
        Ok(contents) => Ok(Some(contents)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

/// Read `service.json` relative to an already-pinned directory descriptor.
///
/// No pathname lookup escapes the pinned config directory: only the final
/// component is named, and `O_NOFOLLOW` is what keeps that name from being a
/// symlink out of the directory.
#[cfg(unix)]
fn read_manifest_in_directory(
    directory: &std::fs::File,
) -> std::io::Result<Option<(Vec<u8>, std::fs::Metadata)>> {
    use std::io::Read as _;
    use std::os::fd::{AsRawFd as _, FromRawFd as _};
    use std::os::unix::fs::MetadataExt as _;

    let fd = unsafe {
        nix::libc::openat(
            directory.as_raw_fd(),
            SERVICE_MANIFEST_FILE_C.as_ptr(),
            nix::libc::O_RDONLY
                | nix::libc::O_CLOEXEC
                | nix::libc::O_NOFOLLOW
                | nix::libc::O_NONBLOCK,
        )
    };
    if fd < 0 {
        let error = std::io::Error::last_os_error();
        if error.kind() == std::io::ErrorKind::NotFound {
            return Ok(None);
        }
        return Err(error);
    }
    let mut file = unsafe { std::fs::File::from_raw_fd(fd) };
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.len() > MAX_SERVICE_MANIFEST_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "the Cloud Connect state file was not a bounded regular file",
        ));
    }
    if metadata.nlink() != 1 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "the Cloud Connect state file must not be hard-linked",
        ));
    }
    let mut bytes = Vec::with_capacity(usize::try_from(metadata.len()).unwrap_or(0));
    (&mut file)
        .take(MAX_SERVICE_MANIFEST_BYTES.saturating_add(1))
        .read_to_end(&mut bytes)?;
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > MAX_SERVICE_MANIFEST_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "the Cloud Connect state file exceeded its size limit",
        ));
    }
    Ok(Some((bytes, metadata)))
}

/// Create a unique owner-only staging inode and publish it relative to the
/// already-validated directory descriptor. No pathname lookup escapes the
/// pinned config directory.
#[cfg(unix)]
fn write_manifest_in_directory(
    directory: &std::fs::File,
    display_path: &Path,
    bytes: &[u8],
    owner: &ServiceOwner,
) -> Result<()> {
    use std::ffi::CString;
    use std::io::Write as _;
    use std::os::fd::{AsRawFd as _, FromRawFd as _};
    use std::os::unix::fs::MetadataExt as _;

    let io_error = |e: std::io::Error| Error::CloudConnectIo {
        message: format!("write the service manifest {}: {e}", display_path.display()),
    };
    let staging_name = format!(".{SERVICE_MANIFEST_FILE}.incoming-{}", uuid::Uuid::new_v4());
    let staging = CString::new(staging_name.as_bytes()).map_err(|_| Error::CloudConnectIo {
        message: "construct the service manifest staging name: generated name contains a NUL byte"
            .to_string(),
    })?;
    let destination = SERVICE_MANIFEST_FILE_C;
    let fd = unsafe {
        nix::libc::openat(
            directory.as_raw_fd(),
            staging.as_ptr(),
            nix::libc::O_WRONLY
                | nix::libc::O_CREAT
                | nix::libc::O_EXCL
                | nix::libc::O_CLOEXEC
                | nix::libc::O_NOFOLLOW,
            0o600,
        )
    };
    if fd < 0 {
        return Err(io_error(std::io::Error::last_os_error()));
    }
    let mut file = unsafe { std::fs::File::from_raw_fd(fd) };

    let publish = (|| -> std::io::Result<()> {
        let metadata = file.metadata()?;
        if !metadata.is_file() || metadata.nlink() != 1 {
            return Err(std::io::Error::other(
                "new service manifest staging inode is not a single-link regular file",
            ));
        }
        if nix::unistd::Uid::effective().is_root() {
            let changed = unsafe { nix::libc::fchown(file.as_raw_fd(), owner.uid, owner.gid) };
            if changed != 0 {
                return Err(std::io::Error::last_os_error());
            }
        }
        let restricted = unsafe { nix::libc::fchmod(file.as_raw_fd(), 0o600) };
        if restricted != 0 {
            return Err(std::io::Error::last_os_error());
        }
        file.write_all(bytes)?;
        file.sync_all()?;
        let renamed = unsafe {
            nix::libc::renameat(
                directory.as_raw_fd(),
                staging.as_ptr(),
                directory.as_raw_fd(),
                destination.as_ptr(),
            )
        };
        if renamed != 0 {
            return Err(std::io::Error::last_os_error());
        }
        directory.sync_all()
    })();

    if let Err(error) = publish {
        unsafe {
            nix::libc::unlinkat(directory.as_raw_fd(), staging.as_ptr(), 0);
        }
        return Err(io_error(error));
    }
    Ok(())
}

/// Revalidate and remove `service.json` relative to one pinned directory.
#[cfg(unix)]
fn remove_manifest_in_directory(
    directory: &std::fs::File,
    display_path: &Path,
    expected: &ServiceManifest,
) -> Result<()> {
    use std::os::fd::AsRawFd as _;
    use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};

    let io_error = |error: std::io::Error| Error::CloudConnectIo {
        message: format!(
            "remove the service manifest {} safely: {error}",
            display_path.display()
        ),
    };
    // The same bounded read the load path uses, so the type, link-count and
    // size limits an uninstall re-checks cannot drift from the ones a load
    // applies. An absent manifest is the idempotent case, not a failure.
    let Some((bytes, metadata)) = read_manifest_in_directory(directory).map_err(&io_error)? else {
        return directory.sync_all().map_err(io_error);
    };
    let mode = metadata.permissions().mode() & 0o7777;
    if metadata.uid() != expected.owner.uid || mode & 0o077 != 0 {
        return Err(Error::InvalidArgument {
            message: format!(
                "Refusing to remove the re-opened service manifest {} because its owner or permissions changed during uninstall.",
                display_path.display()
            ),
        });
    }
    let reopened: ServiceManifest = serde_json::from_slice(&bytes).map_err(|error| {
        Error::InvalidArgument {
            message: format!(
                "Refusing to remove the service manifest {} because it changed during uninstall: {error}",
                display_path.display()
            ),
        }
    })?;
    if &reopened != expected {
        return Err(Error::InvalidArgument {
            message: format!(
                "Refusing to remove the service manifest {} because it changed during uninstall.",
                display_path.display()
            ),
        });
    }
    let removed =
        unsafe { nix::libc::unlinkat(directory.as_raw_fd(), SERVICE_MANIFEST_FILE_C.as_ptr(), 0) };
    if removed != 0 {
        return Err(io_error(std::io::Error::last_os_error()));
    }
    directory.sync_all().map_err(io_error)
}

#[cfg(not(unix))]
fn write_owner_only(path: &Path, bytes: &[u8], _owner: &ServiceOwner) -> Result<()> {
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
        #[cfg(unix)]
        let owner = ServiceOwner {
            uid: nix::unistd::Uid::effective().as_raw(),
            gid: nix::unistd::Gid::effective().as_raw(),
            name: Some("alice".to_string()),
        };
        #[cfg(not(unix))]
        let owner = ServiceOwner {
            uid: 1000,
            gid: 1000,
            name: Some("alice".to_string()),
        };
        ServiceManifest {
            schema_version: MANIFEST_SCHEMA_VERSION,
            directory: instance_dir.to_path_buf(),
            name: name.clone(),
            scope,
            supervisor: backend.supervisor(),
            owner,
            definition_path: backend.definition_path(&name, scope),
            runtime_path: PathBuf::from("/usr/local/lib/spice/spiced"),
            log_source: backend.log_source(&name, scope),
            runtime_digest: "0".repeat(64),
            runtime_version: "v2.2.0".to_string(),
            health_url: "http://127.0.0.1:8090/health".to_string(),
        }
    }

    #[test]
    fn the_two_manifest_file_names_agree() {
        // The descriptor-relative syscalls name the manifest with the C
        // spelling, so a rename that missed it would leave them opening a file
        // nothing else writes.
        assert_eq!(
            SERVICE_MANIFEST_FILE_C.to_bytes(),
            SERVICE_MANIFEST_FILE.as_bytes()
        );
    }

    #[test]
    fn a_written_manifest_round_trips() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let fake = FakeBackend::new(dir.path());
        let instance_dir = dir.path().join("edge-1");
        let config_dir = instance_dir.join(".spice");
        let manifest = manifest_for(&fake, &instance_dir);

        manifest
            .write(&PinnedConfigDir::unlocked(&config_dir))
            .expect("write manifest");
        let loaded = ServiceManifest::load(
            &PinnedConfigDir::unlocked(&config_dir),
            &instance_dir,
            &fake,
        )
        .expect("load manifest")
        .expect("a manifest was written");
        assert_eq!(loaded, manifest);
    }

    #[test]
    fn an_absent_manifest_is_not_an_error() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let fake = FakeBackend::new(dir.path());
        assert_eq!(
            ServiceManifest::load(
                &PinnedConfigDir::unlocked(dir.path()),
                &dir.path().join("edge-1"),
                &fake
            )
            .expect("load"),
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
            .write(&PinnedConfigDir::unlocked(&config_dir))
            .expect("write manifest");

        let error = ServiceManifest::load(
            &PinnedConfigDir::unlocked(&config_dir),
            &dir.path().join("edge-2"),
            &fake,
        )
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
        manifest
            .write(&PinnedConfigDir::unlocked(&config_dir))
            .expect("write manifest");

        let error = ServiceManifest::load(
            &PinnedConfigDir::unlocked(&config_dir),
            &instance_dir,
            &fake,
        )
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
        manifest
            .write(&PinnedConfigDir::unlocked(&config_dir))
            .expect("write manifest");

        let error = ServiceManifest::load(
            &PinnedConfigDir::unlocked(&config_dir),
            &instance_dir,
            &fake,
        )
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
        manifest
            .write(&PinnedConfigDir::unlocked(&config_dir))
            .expect("write manifest");

        let error = ServiceManifest::load(
            &PinnedConfigDir::unlocked(&config_dir),
            &instance_dir,
            &fake,
        )
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
        manifest
            .write(&PinnedConfigDir::unlocked(&config_dir))
            .expect("write manifest");

        let error = ServiceManifest::load(
            &PinnedConfigDir::unlocked(&config_dir),
            &instance_dir,
            &fake,
        )
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
        manifest
            .write(&PinnedConfigDir::unlocked(&config_dir))
            .expect("write manifest");

        let error = ServiceManifest::load(
            &PinnedConfigDir::unlocked(&config_dir),
            &instance_dir,
            &fake,
        )
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
        manifest
            .write(&PinnedConfigDir::unlocked(&config_dir))
            .expect("write manifest");

        let error = ServiceManifest::load(
            &PinnedConfigDir::unlocked(&config_dir),
            &instance_dir,
            &fake,
        )
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

        let error = ServiceManifest::load(
            &PinnedConfigDir::unlocked(&config_dir),
            &instance_dir,
            &fake,
        )
        .expect_err("a manifest that describes something unreadable must be reported");
        assert!(error.to_string().contains("service manifest"), "{error}");
    }

    #[test]
    fn removing_an_absent_manifest_succeeds() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let fake = FakeBackend::new(dir.path());
        let instance_dir = dir.path().join("edge-1");
        manifest_for(&fake, &instance_dir)
            .remove(&PinnedConfigDir::unlocked(dir.path()))
            .expect("idempotent removal");
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
            .write(&PinnedConfigDir::unlocked(&config_dir))
            .expect("write manifest");

        let path = ServiceManifest::path_in(&config_dir);
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644))
            .expect("widen the mode");
        let error = ServiceManifest::load(
            &PinnedConfigDir::unlocked(&config_dir),
            &instance_dir,
            &fake,
        )
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
            .write(&PinnedConfigDir::unlocked(&config_dir))
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
    fn a_pinned_directory_cannot_be_redirected_before_manifest_publication() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let config_dir = dir.path().join("instance").join(".spice");
        let moved_config_dir = dir.path().join("pinned-config");
        let attacker_dir = dir.path().join("replacement");
        std::fs::create_dir_all(&config_dir).expect("create config dir");
        std::fs::create_dir_all(&attacker_dir).expect("create replacement dir");
        let owner = ServiceOwner {
            uid: nix::unistd::Uid::effective().as_raw(),
            gid: nix::unistd::Gid::effective().as_raw(),
            name: None,
        };
        let pinned =
            open_pinned_manifest_directory(&PinnedConfigDir::unlocked(&config_dir), &owner)
                .expect("pin original config dir");

        std::fs::rename(&config_dir, &moved_config_dir).expect("move original config dir");
        std::os::unix::fs::symlink(&attacker_dir, &config_dir)
            .expect("replace config path with symlink");
        write_manifest_in_directory(
            &pinned,
            &ServiceManifest::path_in(&config_dir),
            b"pinned directory",
            &owner,
        )
        .expect("publish through the pinned descriptor");

        assert_eq!(
            std::fs::read(moved_config_dir.join(SERVICE_MANIFEST_FILE))
                .expect("read manifest in original directory"),
            b"pinned directory"
        );
        assert!(
            !attacker_dir.join(SERVICE_MANIFEST_FILE).exists(),
            "the replacement pathname must not receive the privileged write"
        );
    }

    /// The pinned directory has to be the inode the lock retained even after
    /// the config directory's pathname names a *different* directory with the
    /// same owner — which is what a re-resolved pathname cannot distinguish,
    /// on every Unix rather than only where `/proc/self/fd` can be traversed.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_pinned_directory_is_the_locked_inode_after_a_same_owner_replacement() {
        use std::os::unix::fs::MetadataExt as _;

        let dir = tempfile::tempdir().expect("create tempdir");
        let config_dir = dir.path().join("instance").join(".spice");
        std::fs::create_dir_all(&config_dir).expect("create config dir");
        let lock = runtime_cloud_connect::MutationLock::acquire(&config_dir, "manifest-test")
            .await
            .expect("lock config dir");
        let locked = std::fs::metadata(&config_dir).expect("stat the locked config dir");
        let pinned = PinnedConfigDir::locked(
            config_dir.clone(),
            lock.pinned_directory()
                .expect("retain the locked directory"),
        );
        let owner = ServiceOwner {
            uid: nix::unistd::Uid::effective().as_raw(),
            gid: nix::unistd::Gid::effective().as_raw(),
            name: None,
        };

        let moved_config_dir = dir.path().join("instance").join("moved-spice");
        std::fs::rename(&config_dir, &moved_config_dir).expect("move the locked config dir");
        std::fs::create_dir_all(&config_dir).expect("create the replacement config dir");
        let replacement = std::fs::metadata(&config_dir).expect("stat the replacement");
        assert_ne!(
            (replacement.dev(), replacement.ino()),
            (locked.dev(), locked.ino()),
            "the replacement must be a different directory for this test to mean anything"
        );
        assert_eq!(
            replacement.uid(),
            owner.uid,
            "the replacement is owned by the same account, so ownership cannot discriminate"
        );

        let opened = open_pinned_manifest_directory(&pinned, &owner)
            .expect("open the pinned directory")
            .metadata()
            .expect("stat the pinned directory");

        assert_eq!(
            (opened.dev(), opened.ino()),
            (locked.dev(), locked.ino()),
            "a pinned manifest operation must resolve the directory the lock was taken for"
        );
    }

    /// The write and the read that an install performs both have to land in
    /// the locked instance's directory, not in whichever directory its
    /// pathname names by the time they run.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_locked_manifest_is_published_and_read_back_from_the_locked_directory() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let fake = FakeBackend::new(dir.path());
        let instance_dir = dir.path().join("edge-1");
        let config_dir = instance_dir.join(".spice");
        std::fs::create_dir_all(&config_dir).expect("create config dir");
        let manifest = manifest_for(&fake, &instance_dir);

        let lock = runtime_cloud_connect::MutationLock::acquire(&config_dir, "manifest-test")
            .await
            .expect("lock config dir");
        let pinned = PinnedConfigDir::locked(
            config_dir.clone(),
            lock.pinned_directory()
                .expect("retain the locked directory"),
        );

        let moved_config_dir = instance_dir.join("moved-spice");
        std::fs::rename(&config_dir, &moved_config_dir).expect("move the locked config dir");
        std::fs::create_dir_all(&config_dir).expect("create the replacement config dir");

        manifest.write(&pinned).expect("publish through the lock");

        assert!(
            !ServiceManifest::path_in(&config_dir).exists(),
            "the replacement pathname must not receive the manifest"
        );
        assert_eq!(
            ServiceManifest::load(&pinned, &instance_dir, &fake)
                .expect("read back through the lock"),
            Some(manifest),
            "the read must come from the locked directory"
        );
        assert!(
            ServiceManifest::path_in(&moved_config_dir).exists(),
            "the locked directory must hold the manifest"
        );
    }

    /// Nothing a locked operation does may depend on the config directory's
    /// pathname still resolving: the owner can move it out from under a root
    /// install or uninstall, and the descriptor the lock holds is what names
    /// the instance.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_locked_manifest_is_read_and_removed_after_its_pathname_is_gone() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let fake = FakeBackend::new(dir.path());
        let instance_dir = dir.path().join("edge-1");
        let config_dir = instance_dir.join(".spice");
        std::fs::create_dir_all(&config_dir).expect("create config dir");
        let manifest = manifest_for(&fake, &instance_dir);

        let lock = runtime_cloud_connect::MutationLock::acquire(&config_dir, "manifest-test")
            .await
            .expect("lock config dir");
        let pinned = PinnedConfigDir::locked(
            config_dir.clone(),
            lock.pinned_directory()
                .expect("retain the locked directory"),
        );
        manifest.write(&pinned).expect("publish through the lock");

        let moved_config_dir = instance_dir.join("moved-spice");
        std::fs::rename(&config_dir, &moved_config_dir).expect("move the locked config dir");
        assert!(
            !config_dir.exists(),
            "the config directory's pathname must name nothing for this test to mean anything"
        );

        assert_eq!(
            ServiceManifest::load(&pinned, &instance_dir, &fake).expect("read through the lock"),
            Some(manifest.clone()),
            "a locked read must not depend on the pathname"
        );
        manifest.remove(&pinned).expect("remove through the lock");
        assert!(
            !ServiceManifest::path_in(&moved_config_dir).exists(),
            "the manifest in the locked directory must be removed"
        );
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
            .write(&PinnedConfigDir::unlocked(dir.path()))
            .expect("write manifest");
        std::fs::rename(ServiceManifest::path_in(dir.path()), &elsewhere).expect("move it aside");
        std::fs::create_dir_all(&config_dir).expect("create config dir");
        std::os::unix::fs::symlink(&elsewhere, ServiceManifest::path_in(&config_dir))
            .expect("symlink the manifest");

        let error = ServiceManifest::load(
            &PinnedConfigDir::unlocked(&config_dir),
            &instance_dir,
            &fake,
        )
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
        manifest
            .write(&PinnedConfigDir::unlocked(&config_dir))
            .expect("first write");
        manifest.runtime_version = "v2.3.0".to_string();
        manifest
            .write(&PinnedConfigDir::unlocked(&config_dir))
            .expect("second write");

        let loaded = ServiceManifest::load(
            &PinnedConfigDir::unlocked(&config_dir),
            &instance_dir,
            &fake,
        )
        .expect("load")
        .expect("a manifest is present");
        assert_eq!(loaded.runtime_version, "v2.3.0");
    }
}
