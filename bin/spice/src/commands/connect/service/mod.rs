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

//! `spice connect service`: run `spiced` as a persistent service for an
//! enrolled instance directory, and manage its lifecycle.
//!
//! A deployment applies to the running instance and never ends its process, so
//! the supervisor is what keeps the instance up across the things that do end
//! it — a host reboot, an OOM kill, an operator's `systemctl restart`. Both
//! back ends therefore restart the runtime unconditionally — systemd with
//! `Restart=always`, launchd with `KeepAlive`.
//!
//! ## Support matrix
//!
//! Linux with systemd and macOS with launchd. Containers get the same
//! guarantee from their runtime's restart policy
//! (`docker run --restart unless-stopped`) and enroll directly with
//! `spiced --token`. Windows enrolls and runs `spiced` under the user's own
//! supervisor.
//!
//! ## One service per instance directory
//!
//! Two instances on one host must produce two independently-running services,
//! so the service name is derived deterministically from the *absolute*
//! instance directory (see [`name_stem_for_dir`]) and the directory is baked
//! into the definition as its working directory. A command run in one instance
//! directory can therefore never touch another instance's service.
//!
//! ## Resolution is by directory, never by name
//!
//! Every action resolves its target from the canonical instance directory plus
//! the per-directory manifest (`<config-dir>/service.json`). There is no name
//! argument and no host-wide scan: both can name a service belonging to a
//! different instance, and a lifecycle command aimed at the wrong process is a
//! worse outcome than one that refuses.

pub(crate) mod backend;
pub(crate) mod cli;
// Each back end is compiled only for the host it drives: its helpers and
// constants exist to serve one supervisor, and carrying them onto a platform
// that has no such supervisor would be dead code the compiler is right to
// flag. The naming, staging, manifest, and status code every platform shares
// lives here instead, and is covered on every target.
#[cfg(target_os = "macos")]
mod launchd;
pub(crate) mod manifest;
pub(crate) mod model;
#[cfg(target_os = "linux")]
mod systemd;

use std::path::{Path, PathBuf};

#[cfg(unix)]
use std::os::unix::fs::MetadataExt as _;

use sha2::{Digest as _, Sha256};

use crate::error::{Error, Result};

pub(crate) use backend::{InstallRequest, LogRequest, ServiceBackend, for_host as backend};
pub(crate) use manifest::{ServiceManifest, ServiceOwner};
pub(crate) use model::{ServiceScope, ServiceStatus};

/// Longest directory-name fragment carried into a service name, so a deeply-named
/// instance directory cannot produce an unwieldy name. The appended digest is
/// what makes the name unique; the fragment only makes it legible.
const MAX_NAME_FRAGMENT: usize = 32;

/// Hex digits of the path digest appended to every service name.
///
/// Eight bytes of SHA-256. The digest is the whole of the uniqueness
/// guarantee — two instance directories that share a basename are told apart
/// by nothing else — so it is sized for a collision to be unreachable rather
/// than merely unlikely on the hosts anyone has today.
const NAME_DIGEST_HEX: usize = 16;

/// Marks a booted systemd system. Present only when systemd is PID 1 and
/// running, which is exactly the condition `systemctl` needs.
const SYSTEMD_RUNTIME_MARKER: &str = "/run/systemd/system";

/// Root-owned directory the installed service's `spiced` is staged into.
///
/// Not under `/usr/local`: Homebrew owns that prefix on Intel Macs and makes it
/// writable by the installing user, which is exactly what the staged runtime
/// must not be. Not under `/Library/Application Support` either — the space in
/// that path turns every command this module prints for the operator into two
/// arguments.
#[cfg(target_os = "macos")]
const RUNTIME_STAGE_DIR: &str = "/Library/Spice";

/// Root-owned directory the installed service's `spiced` is staged into.
#[cfg(not(target_os = "macos"))]
const RUNTIME_STAGE_DIR: &str = "/usr/local/lib/spice";

/// File name of the staged runtime inside [`RUNTIME_STAGE_DIR`].
const STAGED_RUNTIME_FILE: &str = "spiced";

#[cfg(unix)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ServiceAccount {
    uid: u32,
    gid: u32,
}

/// Resolve the non-root account the installed service should run as.
///
/// `sudo` records the invoking uid/gid. For a direct root session, the owner of
/// the instance directory is the only safe local signal. A root-owned instance
/// with no sudo caller is ambiguous and is rejected instead of installing a
/// privileged service over user-controlled configuration.
#[cfg(unix)]
fn service_account(instance_dir: &Path) -> Result<ServiceAccount> {
    let caller_ids = (std::env::var_os("SUDO_UID"), std::env::var_os("SUDO_GID"));
    if caller_ids.0.is_some() || caller_ids.1.is_some() {
        let uid = caller_ids
            .0
            .and_then(|value| value.to_str().and_then(|value| value.parse::<u32>().ok()));
        let gid = caller_ids
            .1
            .and_then(|value| value.to_str().and_then(|value| value.parse::<u32>().ok()));
        if let (Some(uid), Some(gid)) = (uid, gid)
            && uid != 0
            && gid != 0
        {
            return Ok(ServiceAccount { uid, gid });
        }
        return Err(Error::InvalidArgument {
            message: "Failed to install the Spice Cloud Connect service: SUDO_UID and SUDO_GID must identify the non-root operator who will run spiced. Re-run from that account with `sudo spice connect service install`.".to_string(),
        });
    }

    let metadata = std::fs::metadata(instance_dir).map_err(|e| Error::CloudConnectIo {
        message: format!(
            "inspect instance directory {} to choose the service account: {e}",
            instance_dir.display()
        ),
    })?;
    if metadata.uid() != 0 && metadata.gid() != 0 {
        return Ok(ServiceAccount {
            uid: metadata.uid(),
            gid: metadata.gid(),
        });
    }

    Err(Error::InvalidArgument {
        message: format!(
            "Failed to install the Spice Cloud Connect service: {} is root-owned and no non-root sudo caller is available. Run the command from the intended operator account with `sudo spice connect service install`.",
            instance_dir.display()
        ),
    })
}

/// The local account name for a uid, when the host has one.
#[cfg(unix)]
fn account_name(uid: u32) -> Option<String> {
    nix::unistd::User::from_uid(nix::unistd::Uid::from_raw(uid))
        .ok()
        .flatten()
        .map(|user| user.name)
}

/// Give the service account access to the enrolled identity and managed state.
/// Symlinks are rejected so a root install cannot be steered into changing the
/// ownership of an unrelated target.
#[cfg(unix)]
fn provision_config_ownership(path: &Path, account: ServiceAccount) -> Result<()> {
    let metadata = std::fs::symlink_metadata(path).map_err(|e| Error::CloudConnectIo {
        message: format!("inspect Spice config directory {}: {e}", path.display()),
    })?;
    if metadata.file_type().is_symlink() {
        return Err(Error::InvalidArgument {
            message: format!(
                "Failed to install the Spice Cloud Connect service: config path {} is a symlink; use a real directory so ownership can be provisioned safely.",
                path.display()
            ),
        });
    }
    if metadata.is_dir() {
        for entry in std::fs::read_dir(path).map_err(|e| Error::CloudConnectIo {
            message: format!("read Spice config directory {}: {e}", path.display()),
        })? {
            let entry = entry.map_err(|e| Error::CloudConnectIo {
                message: format!(
                    "read an entry in Spice config directory {}: {e}",
                    path.display()
                ),
            })?;
            provision_config_ownership(&entry.path(), account)?;
        }
    }
    std::os::unix::fs::lchown(path, Some(account.uid), Some(account.gid)).map_err(|e| {
        Error::CloudConnectIo {
            message: format!(
                "set ownership of Spice Cloud Connect state {} to {}:{}: {e}",
                path.display(),
                account.uid,
                account.gid
            ),
        }
    })
}

/// The `spiced` the installed service runs.
fn staged_runtime_path() -> PathBuf {
    Path::new(RUNTIME_STAGE_DIR).join(STAGED_RUNTIME_FILE)
}

/// A service installed for one instance directory, as its back end describes
/// it immediately after installing it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct InstalledService {
    /// The name the supervisor knows this service by: a systemd unit name
    /// including its `.service` suffix, or a launchd job label.
    pub(crate) name: String,
    /// Absolute path of the service definition — the unit file or the plist.
    pub(crate) path: PathBuf,
    /// The instance directory baked in as the service's working directory.
    pub(crate) working_dir: PathBuf,
    /// The `spiced` binary the service runs — the root-owned staged copy.
    pub(crate) runtime: PathBuf,
}

/// Derive the per-instance part of this directory's service name.
///
/// The stem is `<fragment>-<digest>`: a legible fragment of at most
/// [`MAX_NAME_FRAGMENT`] characters from the directory name, plus the first
/// [`NAME_DIGEST_HEX`] hex digits of the SHA-256 of the absolute path. The
/// digest is what guarantees two directories never collide — including two
/// directories with the same basename (`/srv/a/edge` and `/srv/b/edge`) —
/// while the fragment keeps `systemctl status` and `launchctl print` output
/// recognisable.
///
/// `dir` must already be absolute (`CloudConnectConfig::resolve_config_dir`
/// absolutises it at enroll time): the same instance must always derive the
/// same name, and a relative path would derive a different one depending on
/// where the command ran.
fn name_stem_for_dir(dir: &Path) -> String {
    let digest = Sha256::digest(dir.as_os_str().as_encoded_bytes());
    let mut short = String::with_capacity(NAME_DIGEST_HEX);
    for byte in digest.iter().take(NAME_DIGEST_HEX / 2) {
        use std::fmt::Write as _;
        // Writing into a String is infallible; the Result exists only to
        // satisfy the `Write` trait.
        let _ = write!(short, "{byte:02x}");
    }

    let fragment = dir
        .file_name()
        .and_then(|name| name.to_str())
        .map(sanitize_fragment)
        .filter(|fragment| !fragment.is_empty());

    match fragment {
        Some(fragment) => format!("{fragment}-{short}"),
        // A directory with no usable name (`/`, or non-UTF-8) still installs;
        // the digest alone names it.
        None => short,
    }
}

/// Reduce a directory name to characters both supervisors accept in a service
/// name, collapsing runs of anything else to a single `-` and bounding the
/// length.
fn sanitize_fragment(name: &str) -> String {
    let mut out = String::with_capacity(name.len().min(MAX_NAME_FRAGMENT));
    let mut last_was_dash = false;
    for ch in name.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
            last_was_dash = false;
        } else if !last_was_dash && !out.is_empty() {
            out.push('-');
            last_was_dash = true;
        }
        if out.len() >= MAX_NAME_FRAGMENT {
            break;
        }
    }
    out.trim_matches('-').to_string()
}

/// Why a host cannot have a service installed on it.
///
/// Distinguished from a generic error so the caller can run the check before
/// touching any state: a failed preflight must change nothing.
#[derive(Debug)]
pub(crate) enum PreflightFailure {
    /// Neither Linux nor macOS — there is no supervisor to install into.
    UnsupportedPlatform,
    /// Linux, but systemd is not the running init.
    SystemdUnavailable,
    /// A user-scope service was asked for, and the back end does not install
    /// one yet.
    UserScopePending,
}

impl PreflightFailure {
    /// The operator-facing message, naming the fix and the alternative path.
    fn message(&self) -> String {
        match self {
            Self::UnsupportedPlatform => format!(
                "Failed to install the Spice Cloud Connect service: a service needs \
                 Linux with systemd or macOS with launchd (this host is {}). Run `spiced` from \
                 the enrolled directory under your own supervisor, or run Spice in a container \
                 with a restart policy (`docker run --restart unless-stopped`). \
                 See: https://spiceai.org/docs",
                std::env::consts::OS
            ),
            Self::SystemdUnavailable => format!(
                "Failed to install the Spice Cloud Connect service: systemd is not running on \
                 this host ({SYSTEMD_RUNTIME_MARKER} is absent). This is normal inside a \
                 container: run `spiced --token <enrollment-key>` under the container \
                 runtime's restart policy (`docker run --restart unless-stopped`) \
                 instead. See: https://spiceai.org/docs"
            ),
            Self::UserScopePending => format!(
                "Failed to install the Spice Cloud Connect service: a {} is not available in \
                 this release. Install a system service instead: \
                 `sudo spice connect service install`. \
                 See: https://spiceai.org/docs",
                if cfg!(target_os = "macos") {
                    "user LaunchAgent"
                } else {
                    "systemd user service"
                }
            ),
        }
    }
}

impl From<PreflightFailure> for Error {
    fn from(failure: PreflightFailure) -> Self {
        Error::InvalidArgument {
            message: failure.message(),
        }
    }
}

/// Check that this host can have a service installed, **before** anything
/// irreversible happens.
///
/// The platform check is here rather than in a back end because a host with no
/// supported supervisor has no back end to ask. `cfg!` rather than `#[cfg]`, so
/// the unsupported-platform branch is compiled — and its message kept
/// truthful — on every target.
///
/// # Errors
///
/// Returns the reason installation is impossible on this host.
fn preflight(
    backend: &dyn ServiceBackend,
    scope: ServiceScope,
) -> std::result::Result<(), PreflightFailure> {
    if !cfg!(any(target_os = "linux", target_os = "macos")) {
        return Err(PreflightFailure::UnsupportedPlatform);
    }
    backend.preflight(scope)
}

/// The service domain this invocation can install into.
///
/// Least privilege by default: an unprivileged invocation asks for a user
/// service, and only an explicitly elevated one installs host-wide.
pub(crate) fn scope_for_privilege() -> ServiceScope {
    if is_root() {
        ServiceScope::System
    } else {
        ServiceScope::User
    }
}

/// Resolve the service installed for `instance_dir`, from that directory and
/// nothing else.
///
/// `Ok(None)` means no service is installed for this directory. An error means
/// something claiming to be one could not be trusted — reported rather than
/// treated as absence, because "there is nothing here" would send `install`
/// on to write over it.
///
/// # Errors
///
/// Returns an error when the manifest exists but does not validate.
pub(crate) fn resolve(
    backend: &dyn ServiceBackend,
    instance_dir: &Path,
    config_dir: &Path,
) -> Result<Option<ServiceManifest>> {
    if let Some(manifest) = ServiceManifest::load(config_dir, instance_dir, backend)? {
        return Ok(Some(manifest));
    }
    Ok(adopt_installed_definition(backend, instance_dir))
}

/// Describe a service that is installed for `instance_dir` but has no
/// manifest, by reading the supervisor's own definition.
///
/// A directory can lose its manifest — deleted by hand, or restored from a
/// backup that skipped it — while the definition is still installed and the
/// service still running. Reading the definition is what keeps that service
/// reportable and removable instead of invisible. It is not a fallback search:
/// the name is still derived from the canonical directory, and the definition
/// is accepted only if it names that same directory as its working directory.
fn adopt_installed_definition(
    backend: &dyn ServiceBackend,
    instance_dir: &Path,
) -> Option<ServiceManifest> {
    let scope = ServiceScope::System;
    let installed = backend.find_installed(instance_dir, scope)?;
    let owner = installed_owner(instance_dir);
    Some(ServiceManifest {
        schema_version: manifest::MANIFEST_SCHEMA_VERSION,
        directory: instance_dir.to_path_buf(),
        name: installed.name.clone(),
        scope,
        supervisor: backend.supervisor(),
        owner,
        definition_path: installed.path,
        runtime_path: installed.runtime,
        log_source: backend.log_source(&installed.name, scope),
        // Not recoverable from the definition, and not computed here: hashing
        // the runtime on every `status` would read a gigabyte to answer a
        // question nobody asked. The next `service install` records it.
        runtime_digest: String::new(),
        runtime_version: String::new(),
        health_url: crate::context::DEFAULT_HTTP_ENDPOINT.to_string() + "/health",
    })
}

/// The account an adopted service runs as, taken from the instance directory
/// it was installed for — the same signal the installer uses when there is no
/// `sudo` caller to name.
#[cfg(unix)]
fn installed_owner(instance_dir: &Path) -> ServiceOwner {
    let (uid, gid) =
        std::fs::metadata(instance_dir).map_or((0, 0), |metadata| (metadata.uid(), metadata.gid()));
    ServiceOwner {
        uid,
        gid,
        name: account_name(uid),
    }
}

#[cfg(not(unix))]
fn installed_owner(_instance_dir: &Path) -> ServiceOwner {
    ServiceOwner {
        uid: 0,
        gid: 0,
        name: None,
    }
}

/// The complete service half of the status contract for one instance
/// directory.
///
/// Never fails: `status` has to render something, and a supervisor that could
/// not be asked is a state in the vocabulary rather than an error that
/// suppresses the rest of the report. The caller decides the exit code from
/// [`model::ServiceState::is_degraded`].
pub(crate) fn status(
    backend: &dyn ServiceBackend,
    instance_dir: &Path,
    config_dir: &Path,
) -> ServiceStatus {
    let manifest = match resolve(backend, instance_dir, config_dir) {
        Ok(Some(manifest)) => manifest,
        Ok(None) => return ServiceStatus::not_installed(),
        Err(err) => return ServiceStatus::unavailable(err.to_string()),
    };

    let observed = backend.observe(&manifest);
    ServiceStatus {
        installed: true,
        state: observed.state,
        scope: Some(manifest.scope),
        supervisor: Some(manifest.supervisor),
        starts: observed.starts,
        owner: Some(manifest.owner.describe()),
        name: Some(manifest.name.clone()),
        working_dir: Some(manifest.directory.clone()),
        definition_path: Some(manifest.definition_path.clone()),
        runtime_path: Some(manifest.runtime_path.clone()),
        log_source: manifest.log_source,
        diagnostic: observed.diagnostic,
        starts_action: observed.starts_action,
    }
}

/// Install (or reinstall) the service for `instance_dir` and record what was
/// installed in the per-directory manifest.
///
/// Idempotent, and the in-place upgrade path: re-running restages the current
/// `spiced` binary, rewrites the service definition, restarts the service, and
/// rewrites the manifest, leaving the enrolled identity untouched.
///
/// # Errors
///
/// Returns an error when the host cannot host a service, when the derived
/// service name is already taken by something that does not belong to this
/// directory, when the runtime cannot be staged, or when the supervisor
/// rejects the definition. Every one of those is checked or reported before
/// the manifest is written, so a failed install never leaves a manifest
/// describing a service that is not there.
pub(crate) fn install(
    backend: &dyn ServiceBackend,
    instance_dir: &Path,
    config_dir: &Path,
    spiced_path: &Path,
    runtime_version: &str,
    health_url: &str,
) -> Result<ServiceManifest> {
    let scope = scope_for_privilege();
    preflight(backend, scope)?;

    // Reading the existing state first is what makes the pre-existing-name
    // check meaningful: an invalid manifest, or a definition under this
    // directory's derived name that belongs to somewhere else, has to fail
    // before anything is written.
    let existing = resolve(backend, instance_dir, config_dir)?;
    if existing.is_none() {
        ensure_name_unclaimed(backend, instance_dir, scope)?;
    }

    let installed = backend.install(&InstallRequest {
        instance_dir,
        config_dir,
        spiced_path,
        scope,
    })?;

    let owner = install_owner(instance_dir)?;
    let manifest = ServiceManifest {
        schema_version: manifest::MANIFEST_SCHEMA_VERSION,
        directory: instance_dir.to_path_buf(),
        name: installed.name.clone(),
        scope,
        supervisor: backend.supervisor(),
        owner,
        definition_path: installed.path,
        runtime_path: installed.runtime.clone(),
        log_source: backend.log_source(&installed.name, scope),
        runtime_digest: file_digest(&installed.runtime).unwrap_or_default(),
        runtime_version: runtime_version.to_string(),
        health_url: health_url.to_string(),
    };
    manifest.write(config_dir)?;
    Ok(manifest)
}

/// The account the freshly installed service runs as.
#[cfg(unix)]
fn install_owner(instance_dir: &Path) -> Result<ServiceOwner> {
    let account = service_account(instance_dir)?;
    Ok(ServiceOwner {
        uid: account.uid,
        gid: account.gid,
        name: account_name(account.uid),
    })
}

#[cfg(not(unix))]
fn install_owner(instance_dir: &Path) -> Result<ServiceOwner> {
    Ok(installed_owner(instance_dir))
}

/// Refuse to install over a definition that already carries this directory's
/// derived name but does not describe this directory's service.
///
/// The derived name is a function of the path, so the only way this happens is
/// a definition written by hand, left behind by a directory that has since
/// moved, or belonging to another operator. Overwriting any of those would
/// take over a service this directory does not own, so it fails before the
/// installer touches anything.
fn ensure_name_unclaimed(
    backend: &dyn ServiceBackend,
    instance_dir: &Path,
    scope: ServiceScope,
) -> Result<()> {
    let name = backend.name_for_dir(instance_dir);
    let path = backend.definition_path(&name, scope);
    let Ok(metadata) = std::fs::symlink_metadata(&path) else {
        return Ok(());
    };

    let claim = if metadata.file_type().is_symlink() {
        Some("is a symlink".to_string())
    } else if backend.find_installed(instance_dir, scope).is_none() {
        Some("belongs to another instance directory".to_string())
    } else {
        definition_ownership_problem(&metadata, scope)
    };

    match claim {
        Some(reason) => Err(Error::InvalidArgument {
            message: format!(
                "Failed to install the Spice Cloud Connect service for {instance}: the service \
                 definition {definition} already exists and {reason}. Remove or repair it, then \
                 re-run `spice connect service install`. Nothing was changed. \
                 See: https://spiceai.org/docs",
                instance = instance_dir.display(),
                definition = path.display(),
            ),
        }),
        None => Ok(()),
    }
}

/// Whether an existing definition's ownership disqualifies it from being
/// rewritten in `scope`.
#[cfg(unix)]
fn definition_ownership_problem(
    metadata: &std::fs::Metadata,
    scope: ServiceScope,
) -> Option<String> {
    use std::os::unix::fs::PermissionsExt as _;

    let uid = metadata.uid();
    let mode = metadata.permissions().mode() & 0o7777;
    match scope {
        // A host-wide definition tells the supervisor what to run as whom. A
        // non-root owner, or write access for anyone but root, means someone
        // else already decides that.
        ServiceScope::System if uid != 0 || mode & 0o022 != 0 => Some(format!(
            "is owned by uid {uid} with mode {mode:04o}, so it is not a definition only root controls"
        )),
        ServiceScope::User if uid != nix::unistd::Uid::effective().as_raw() => {
            Some(format!("is owned by uid {uid} rather than by this account"))
        }
        _ => None,
    }
}

#[cfg(not(unix))]
fn definition_ownership_problem(
    _metadata: &std::fs::Metadata,
    _scope: ServiceScope,
) -> Option<String> {
    None
}

/// Stop and remove the service for `instance_dir`, and forget it.
///
/// Idempotent: a directory with no service succeeds and returns `None`. This
/// is the one primitive both `spice connect service uninstall` and
/// `spice connect remove` use, so they cannot diverge on which assets a
/// removal touches. What they do *not* share is identity: uninstall preserves
/// the Cloud identity, and `remove` is the command that releases it.
///
/// # Errors
///
/// Returns an error when the supervisor or the definition file cannot be
/// removed. The manifest is deleted only after the back end reports the
/// service gone, so a partial failure leaves a directory that still knows what
/// it is trying to remove.
pub(crate) fn uninstall(
    backend: &dyn ServiceBackend,
    instance_dir: &Path,
    config_dir: &Path,
) -> Result<Option<ServiceManifest>> {
    let Some(manifest) = resolve(backend, instance_dir, config_dir)? else {
        return Ok(None);
    };
    backend.uninstall(&manifest)?;
    ServiceManifest::remove(config_dir)?;
    Ok(Some(manifest))
}

/// SHA-256 (lowercase hex) of a file, or `None` when it cannot be read.
fn file_digest(path: &Path) -> Option<String> {
    let mut file = std::fs::File::open(path).ok()?;
    let mut hasher = Sha256::new();
    std::io::copy(&mut file, &mut hasher).ok()?;
    Some(format!("{:x}", hasher.finalize()))
}

/// `true` when this process runs with root's effective user id — the id that
/// governs writing a host-wide service definition and directing the
/// supervisor.
#[cfg(unix)]
fn is_root() -> bool {
    nix::unistd::Uid::effective().is_root()
}

#[cfg(not(unix))]
fn is_root() -> bool {
    false
}

/// Stage `source` as the root-owned `spiced` the installed service runs.
///
/// The runtime is commonly installed under an operator's `~/.spice/bin`.
/// Copying it into a root-owned directory (0755, and created by a root process)
/// prevents later replacement behind the supervisor's back and gives every
/// installed instance one stable executable.
///
/// Copying every install also makes the documented upgrade path work: re-running
/// the install restages whatever `spiced` the CLI now resolves. One host has one
/// staged runtime, shared by every instance's service — so a re-install in one
/// instance directory does change the binary the others run at their next
/// restart.
///
/// A debug `spiced` runs to about a gigabyte, so the copy is skipped when a
/// sidecar stamp shows the destination was staged from exactly this source file.
/// The stamp is required rather than comparing timestamps directly, because
/// `fs::copy` gives the destination a fresh modified time — a
/// same-length-and-mtime test would never match and would recopy every run,
/// while a "destination is newer" test could skip a genuine upgrade and leave
/// the service on the old runtime while the install output reported the new
/// version.
///
/// `verify_staged` is given the copy before it is published, and again when an
/// existing one is reused. The destination is the path every installed service
/// already executes, so a runtime that turns out to be unusable must never
/// reach it: publishing first and checking afterwards would take out the
/// instances that were working.
fn stage_runtime<F>(source: &Path, verify_staged: F) -> Result<PathBuf>
where
    F: Fn(&Path, &Path) -> Result<()> + Copy,
{
    let dest = staged_runtime_path();
    ensure_root_only_dir(Path::new(RUNTIME_STAGE_DIR))?;
    stage_runtime_at(source, &dest, verify_staged)?;
    Ok(dest)
}

/// [`stage_runtime`] against an explicit destination.
fn stage_runtime_at<F>(source: &Path, dest: &Path, verify_staged: F) -> Result<()>
where
    F: Fn(&Path, &Path) -> Result<()> + Copy,
{
    let stamp_path = dest.with_extension("stamp");
    let stamp = source_stamp(source);

    // A staged runtime that no longer works is restaged, not refused:
    // re-running the install is the documented way to put a good binary back on
    // the path every service executes, and short-circuiting the failure here
    // would make that the one thing it could not do.
    if let Some(ref stamp) = stamp
        && runtime_is_already_staged(dest, &stamp_path, stamp)
        && verify_staged(dest, source).is_ok()
    {
        return Ok(());
    }

    // A stale stamp must never outlive the binary it describes: drop it before
    // copying so an interrupted stage cannot leave a stamp claiming a runtime
    // that was never written.
    let _ = std::fs::remove_file(&stamp_path);

    publish_runtime(source, dest, verify_staged)?;

    // Written last, so it only ever describes a runtime that is actually in
    // place. A failure here costs one needless copy next time, nothing more.
    if let Some(stamp) = stamp {
        let _ = std::fs::write(&stamp_path, stamp);
    }

    Ok(())
}

/// Copy `source` onto `dest`, and put it there only once `verify_staged`
/// accepts the copy.
///
/// The copy goes to a sibling and is renamed, so a running service is never
/// left pointing at a half-written binary — `rename` on the same filesystem is
/// atomic, and an already-executing image keeps running from its open inode.
/// Verification happens on that sibling rather than on `dest`, because `dest`
/// is the binary every installed service re-executes when it restarts:
/// publishing first and checking afterwards would hand an unusable runtime to
/// the instances that were working.
fn publish_runtime<F>(source: &Path, dest: &Path, verify_staged: F) -> Result<()>
where
    F: Fn(&Path, &Path) -> Result<()> + Copy,
{
    let staging = dest.with_extension("incoming");
    let _ = std::fs::remove_file(&staging);
    if let Err(err) = copy_as_root_only_executable(source, &staging) {
        let _ = std::fs::remove_file(&staging);
        return Err(err);
    }

    if let Err(err) = verify_staged(&staging, source) {
        let _ = std::fs::remove_file(&staging);
        return Err(err);
    }

    std::fs::rename(&staging, dest).map_err(|e| Error::CloudConnectIo {
        message: format!(
            "install the staged Spice runtime at {}: {e}",
            dest.display()
        ),
    })
}

/// Create `dir` if absent and refuse to use it unless only root can change what
/// it holds.
///
/// Creating the directory is not enough on its own. Root publishes the shared
/// runtime here, so anyone who can write the directory — or rename any
/// directory on the path to it — can substitute what every installed service
/// runs. An existing `/usr/local/lib/spice` (or `/usr/local/lib`, or
/// `/usr/local`) that is group- or world-writable, or a symlink into somewhere
/// that is, hands that power to a local user, and `create_dir_all` on an
/// existing directory changes nothing about it.
///
/// So: create with an explicit `0755` (not the process umask, which could be 0),
/// then walk the whole path and require every component to be root-owned and
/// writable by nobody else. A component that fails is reported for the operator
/// to fix rather than silently chmod'ed — repermissioning someone's
/// `/usr/local` is not this command's call to make.
#[cfg(unix)]
fn ensure_root_only_dir(dir: &Path) -> Result<()> {
    use std::os::unix::fs::{DirBuilderExt as _, MetadataExt as _, PermissionsExt as _};

    // `recursive(true)` applies the mode to every component it creates and
    // treats an existing directory as success, which is what the checks below
    // are for.
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o755)
        .create(dir)
        .map_err(|e| Error::CloudConnectIo {
            message: format!("create {}: {e}", dir.display()),
        })?;

    // The leaf is checked without following symlinks: a symlinked directory
    // points the root-managed runtime staging somewhere this check cannot
    // vouch for, even if the target itself looks fine today.
    let leaf = std::fs::symlink_metadata(dir).map_err(|e| Error::CloudConnectIo {
        message: format!("inspect {}: {e}", dir.display()),
    })?;
    if leaf.file_type().is_symlink() {
        return Err(Error::InvalidArgument {
            message: format!(
                "Failed to install the Spice Cloud Connect service: {dir} is a symlink. \
                 The runtime staging is managed by root, so it must use a real, root-owned \
                 directory. Replace the symlink with a directory owned by root \
                 (`chown root {dir}`, `chmod 755 {dir}`) and re-run \
                 `sudo spice connect service install`.",
                dir = dir.display()
            ),
        });
    }

    for component in dir.ancestors() {
        // Ancestors are resolved (symlinks followed): a root-owned symlink
        // inside a directory only root can write is not a way in, and refusing
        // one would fail on distributions that symlink parts of /usr.
        let meta = std::fs::metadata(component).map_err(|e| Error::CloudConnectIo {
            message: format!("inspect {}: {e}", component.display()),
        })?;
        let mode = meta.permissions().mode();
        // 0o022: group-write and other-write. Either one lets a non-root user
        // rename the directory below it and present their own.
        if meta.uid() != 0 || mode & 0o022 != 0 {
            return Err(Error::InvalidArgument {
                message: format!(
                    "Failed to install the Spice Cloud Connect service: {component} is owned by uid {uid} with mode {mode:04o}, \
                     so a non-root user can change what the service executes as root. Restrict it \
                     (`sudo chown root {component}` and `sudo chmod go-w {component}`) and re-run \
                     `sudo spice connect service install`.",
                    component = component.display(),
                    uid = meta.uid(),
                    mode = mode & 0o7777,
                ),
            });
        }
    }

    Ok(())
}

#[cfg(not(unix))]
fn ensure_root_only_dir(dir: &Path) -> Result<()> {
    std::fs::create_dir_all(dir).map_err(|e| Error::CloudConnectIo {
        message: format!("create {}: {e}", dir.display()),
    })
}

/// Copy `source` to `dest`, creating `dest` as `0755` from the first byte.
///
/// `fs::copy` gives the destination the *source's* permissions, so staging a
/// runtime that happened to be group- or world-writable would publish a
/// writable file into the staging directory and only tighten it afterwards.
/// Directory permissions do not save it: they govern creating and renaming
/// entries, not writing to a file that already exists — so between the copy and
/// the chmod, any local user could overwrite the binary root is about to run.
/// Creating the file with the final mode closes that window, and `create_new`
/// refuses to inherit a leftover one.
///
/// Writing bytes into a fresh file rather than cloning the original also leaves
/// the extended attributes behind, which on macOS is what keeps a
/// `com.apple.quarantine` on the operator's download from reaching the copy
/// launchd has to execute.
#[cfg(unix)]
fn copy_as_root_only_executable(source: &Path, dest: &Path) -> Result<()> {
    use std::fs::OpenOptions;
    use std::os::unix::fs::OpenOptionsExt as _;

    let io_error = |e: std::io::Error| Error::CloudConnectIo {
        message: format!(
            "stage the Spice runtime from {} to {}: {e}",
            source.display(),
            dest.display()
        ),
    };

    let mut reader = std::fs::File::open(source).map_err(io_error)?;
    let mut writer = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o755)
        .open(dest)
        .map_err(io_error)?;
    std::io::copy(&mut reader, &mut writer).map_err(io_error)?;
    // The rename that follows publishes this file to the service; flush it first
    // so a crash cannot leave the service pointing at a truncated binary.
    writer.sync_all().map_err(io_error)?;
    Ok(())
}

#[cfg(not(unix))]
fn copy_as_root_only_executable(source: &Path, dest: &Path) -> Result<()> {
    std::fs::copy(source, dest)
        .map(|_| ())
        .map_err(|e| Error::CloudConnectIo {
            message: format!(
                "stage the Spice runtime from {} to {}: {e}",
                source.display(),
                dest.display()
            ),
        })
}

/// Identity of the source binary: length and modified time, which together
/// change on any rebuild. `None` when the source cannot be described, in which
/// case the caller always restages.
fn source_stamp(source: &Path) -> Option<String> {
    let meta = source.metadata().ok()?;
    let modified = meta
        .modified()
        .ok()?
        .duration_since(std::time::UNIX_EPOCH)
        .ok()?;
    Some(format!(
        "{} {} {}",
        meta.len(),
        modified.as_nanos(),
        source.display()
    ))
}

/// `true` when `dest` exists and `stamp_path` records that it was staged from
/// exactly this source. Anything else restages, so the check can only ever skip
/// work that has already been done.
fn runtime_is_already_staged(dest: &Path, stamp_path: &Path, stamp: &str) -> bool {
    if !dest.is_file() {
        return false;
    }
    std::fs::read_to_string(stamp_path).is_ok_and(|recorded| recorded == stamp)
}

#[cfg(test)]
mod tests {
    use super::backend::fake::FakeBackend;
    use super::model::{ServiceStarts, ServiceState};
    use super::*;

    /// The exact stems the naming algorithm must produce.
    ///
    /// Golden, not derived: the service name is the identity of an installed
    /// service, so a change here silently orphans every service already
    /// installed on every host. Changing these values is a migration, not a
    /// refactor.
    #[test]
    fn name_stem_algorithm_is_golden() {
        for (dir, expected) in [
            ("/opt/edge-1", "edge-1-2962fca679ae993a"),
            ("/srv/edge-analytics", "edge-analytics-59e8c853e76c15ba"),
            ("/srv/a/edge", "edge-ee40c5935182e518"),
            ("/srv/b/edge", "edge-80faba57766c7bb4"),
            // No usable directory name: the digest alone names it.
            ("/", "8a5edab282632443"),
        ] {
            assert_eq!(name_stem_for_dir(Path::new(dir)), expected, "{dir}");
        }

        // A 120-character directory name keeps exactly 32 characters of
        // fragment, then the 16-hex digest.
        let long = format!("/opt/{}", "a".repeat(120));
        assert_eq!(
            name_stem_for_dir(Path::new(&long)),
            format!("{}-0aca36d818e38f1d", "a".repeat(MAX_NAME_FRAGMENT))
        );
    }

    #[test]
    fn name_stem_digest_is_sixteen_hex_digits() {
        let stem = name_stem_for_dir(Path::new("/opt/edge-1"));
        let digest = stem.rsplit('-').next().expect("a stem always has a digest");
        assert_eq!(digest.len(), NAME_DIGEST_HEX, "{stem}");
        assert!(digest.chars().all(|ch| ch.is_ascii_hexdigit()), "{stem}");
    }

    #[test]
    fn name_stem_is_deterministic() {
        let dir = Path::new("/opt/edge-1");
        assert_eq!(name_stem_for_dir(dir), name_stem_for_dir(dir));
    }

    #[test]
    fn two_directories_yield_two_stems() {
        // The acceptance criterion: two installs in two directories must be
        // two independently-running services.
        let a = name_stem_for_dir(Path::new("/opt/edge-1"));
        let b = name_stem_for_dir(Path::new("/opt/edge-2"));
        assert_ne!(a, b);
    }

    #[test]
    fn same_basename_in_different_parents_does_not_collide() {
        // The legible fragment is identical here, so only the path digest keeps
        // these apart — an uninstall in one must never touch the other's
        // service.
        let a = name_stem_for_dir(Path::new("/srv/a/edge"));
        let b = name_stem_for_dir(Path::new("/srv/b/edge"));
        assert_ne!(a, b);
        assert!(a.starts_with("edge-"), "{a}");
        assert!(b.starts_with("edge-"), "{b}");
    }

    #[test]
    fn name_stem_sanitizes_and_bounds_the_fragment() {
        let stem = name_stem_for_dir(Path::new("/opt/My Edge_Site!!"));
        assert!(stem.starts_with("my-edge-site-"), "{stem}");

        // A long directory name is truncated, not carried whole.
        let long = name_stem_for_dir(Path::new(&format!("/opt/{}", "a".repeat(120))));
        assert!(
            long.len() <= MAX_NAME_FRAGMENT + 1 + NAME_DIGEST_HEX,
            "{long}"
        );
    }

    #[test]
    fn staged_runtime_is_not_under_a_user_home() {
        // The service runs it as root, so it must never be a path its operator
        // could later replace.
        let staged = staged_runtime_path();
        let staged = staged.to_string_lossy();
        assert!(!staged.contains("/home/"), "{staged}");
        assert!(!staged.contains("/Users/"), "{staged}");
    }

    #[test]
    fn staged_runtime_path_has_no_whitespace() {
        // Every remedy this module prints names this path, and a
        // `sudo chown root <path>` with a space in it is two arguments and the
        // wrong instruction. systemd's `ExecStart` splits on it too.
        let staged = staged_runtime_path();
        let staged = staged.to_string_lossy();
        assert!(
            !staged.contains(char::is_whitespace),
            "the staged runtime path must be usable verbatim in a command: {staged}"
        );
    }

    #[test]
    fn staging_skips_only_an_exact_restage_and_never_a_rebuild() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let source = dir.path().join("spiced");
        let dest = dir.path().join("staged");
        let stamp_path = dir.path().join("staged.stamp");

        std::fs::write(&source, b"runtime-v1").expect("write source");
        let stamp = source_stamp(&source).expect("stamp the source");

        // Nothing staged yet.
        assert!(!runtime_is_already_staged(&dest, &stamp_path, &stamp));

        // Staged, but before the stamp is written: still restages, so an
        // interrupted stage is retried rather than trusted.
        std::fs::copy(&source, &dest).expect("copy");
        assert!(!runtime_is_already_staged(&dest, &stamp_path, &stamp));

        // Binary in place and stamped from this exact source: skip the copy.
        std::fs::write(&stamp_path, &stamp).expect("write stamp");
        assert!(runtime_is_already_staged(&dest, &stamp_path, &stamp));

        // A rebuild changes the stamp, so an upgrade is never skipped — the
        // failure that would leave the service on an old runtime while the
        // install output reported the new version.
        std::fs::write(&source, b"runtime-v2").expect("rebuild source");
        let rebuilt = source_stamp(&source).expect("stamp the rebuild");
        assert_ne!(rebuilt, stamp, "a rebuild must produce a different stamp");
        assert!(!runtime_is_already_staged(&dest, &stamp_path, &rebuilt));

        // A missing binary with a leftover stamp also restages.
        std::fs::remove_file(&dest).expect("remove staged binary");
        assert!(!runtime_is_already_staged(&dest, &stamp_path, &stamp));
    }

    #[test]
    fn a_runtime_that_fails_verification_never_reaches_the_live_path() {
        // The destination is what every installed service re-executes when it
        // restarts, so a runtime that turns out to be unusable must not land on
        // it — a rejected upgrade in one instance directory would otherwise
        // take out every other instance on the host.
        let dir = tempfile::tempdir().expect("create tempdir");
        let source = dir.path().join("spiced");
        let dest = dir.path().join("staged");
        std::fs::write(&source, b"unusable").expect("write source");
        std::fs::write(&dest, b"the runtime already in service").expect("write dest");

        let rejected = publish_runtime(&source, &dest, |_, _| {
            Err(Error::InvalidArgument {
                message: "does not run".to_string(),
            })
        });
        assert!(
            rejected.is_err(),
            "a rejected runtime must fail the install"
        );
        assert_eq!(
            std::fs::read(&dest).expect("read dest"),
            b"the runtime already in service",
            "the live path must be untouched"
        );
        assert!(
            !dest.with_extension("incoming").exists(),
            "the rejected copy must be cleaned up"
        );

        // And an accepted one is published, so the check is not simply refusing
        // everything.
        publish_runtime(&source, &dest, |_, _| Ok(())).expect("publish");
        assert_eq!(std::fs::read(&dest).expect("read dest"), b"unusable");
    }

    /// A verifier standing in for the real one: it rejects a candidate that
    /// says it is broken, the way executing it would.
    fn reject_a_broken_runtime(candidate: &Path, _source: &Path) -> Result<()> {
        if std::fs::read(candidate).is_ok_and(|bytes| bytes == b"broken") {
            return Err(Error::InvalidArgument {
                message: "does not run".to_string(),
            });
        }
        Ok(())
    }

    #[test]
    fn a_staged_runtime_that_stopped_working_is_replaced_rather_than_refused() {
        // Re-running the install is the documented way to put a good binary
        // back on the path every service executes. Refusing on the strength of
        // the stamp would make that the one repair it could not perform.
        let dir = tempfile::tempdir().expect("create tempdir");
        let source = dir.path().join("spiced");
        let dest = dir.path().join("staged");
        std::fs::write(&source, b"working").expect("write source");

        stage_runtime_at(&source, &dest, reject_a_broken_runtime).expect("first stage");
        assert_eq!(std::fs::read(&dest).expect("read dest"), b"working");

        // The stamp still matches the source, so only the verifier can notice.
        std::fs::write(&dest, b"broken").expect("damage the staged runtime");
        stage_runtime_at(&source, &dest, reject_a_broken_runtime).expect("restage");
        assert_eq!(
            std::fs::read(&dest).expect("read dest"),
            b"working",
            "a staged runtime that fails verification must be restaged from source"
        );
    }

    #[test]
    fn a_staged_runtime_that_still_works_is_not_copied_again() {
        // The other half: a gigabyte is not recopied on every install.
        let dir = tempfile::tempdir().expect("create tempdir");
        let source = dir.path().join("spiced");
        let dest = dir.path().join("staged");
        std::fs::write(&source, b"working").expect("write source");

        stage_runtime_at(&source, &dest, reject_a_broken_runtime).expect("first stage");
        // Marked, not damaged: the verifier accepts it, so a second stage that
        // recopied would overwrite the mark.
        std::fs::write(&dest, b"reused").expect("mark the staged runtime");
        stage_runtime_at(&source, &dest, reject_a_broken_runtime).expect("second stage");
        assert_eq!(
            std::fs::read(&dest).expect("read dest"),
            b"reused",
            "an unchanged source must not be copied again"
        );
    }

    #[test]
    fn source_stamp_distinguishes_length_and_mtime() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let a = dir.path().join("a");
        let b = dir.path().join("b");
        std::fs::write(&a, b"same-length").expect("write a");
        std::fs::write(&b, b"same-length").expect("write b");

        let stamp_a = source_stamp(&a).expect("stamp a");
        // The path is part of the stamp, so two equal-length files never
        // masquerade as each other.
        assert_ne!(stamp_a, source_stamp(&b).expect("stamp b"));
        assert!(
            stamp_a.starts_with("11 "),
            "length leads the stamp: {stamp_a}"
        );
        assert_eq!(source_stamp(&dir.path().join("missing")), None);
    }

    #[test]
    fn file_digest_is_stable_and_absent_for_a_missing_file() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let path = dir.path().join("runtime");
        std::fs::write(&path, b"spiced").expect("write");
        assert_eq!(
            file_digest(&path).as_deref(),
            Some("677e903199916067c479594e77308560145a863b94342cc471f0043403c92d0e"),
            "the recorded runtime digest must be a plain SHA-256 of the staged file"
        );
        assert_eq!(file_digest(&dir.path().join("missing")), None);
    }

    #[test]
    fn copying_a_runtime_leaves_its_extended_attributes_behind() {
        // On macOS this is what keeps `com.apple.quarantine` off the copy
        // launchd executes: the stage writes bytes into a new file rather than
        // cloning the original.
        let dir = tempfile::tempdir().expect("create tempdir");
        let source = dir.path().join("spiced");
        let dest = dir.path().join("staged");
        std::fs::write(&source, b"runtime").expect("write source");

        let set = std::process::Command::new("xattr")
            .args(["-w", "com.apple.quarantine", "0083;00000000;Test;"])
            .arg(&source)
            .status();
        if !set.is_ok_and(|status| status.success()) {
            // No `xattr` (any non-macOS host): nothing to prove here.
            return;
        }

        copy_as_root_only_executable(&source, &dest).expect("stage the runtime");

        let read = std::process::Command::new("xattr")
            .args(["-p", "com.apple.quarantine"])
            .arg(&dest)
            .status()
            .expect("run xattr");
        assert!(
            !read.success(),
            "the staged copy must not carry com.apple.quarantine"
        );
    }

    #[test]
    fn preflight_failures_name_the_fix() {
        assert!(
            PreflightFailure::UserScopePending
                .message()
                .contains("sudo"),
            "the deferred user-scope failure must name the path that does work"
        );
        assert!(
            PreflightFailure::SystemdUnavailable
                .message()
                .contains("restart policy"),
            "the no-systemd failure must point containers at the --token flow"
        );
        assert!(
            PreflightFailure::SystemdUnavailable
                .message()
                .contains("spiced --token"),
            "containers enroll directly with the runtime, not this installer"
        );
        assert!(
            PreflightFailure::UnsupportedPlatform
                .message()
                .contains("spiced"),
            "an unsupported host must be told the supported way to run"
        );
        assert!(
            PreflightFailure::UnsupportedPlatform
                .message()
                .contains("launchd"),
            "macOS is supported, so the message must not read as Linux-only"
        );
    }

    #[test]
    fn a_directory_with_no_service_resolves_to_nothing() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let instance_dir = dir.path().join("edge-1");
        let config_dir = instance_dir.join(".spice");
        let fake = FakeBackend::new(dir.path());

        assert_eq!(
            resolve(&fake, &instance_dir, &config_dir).expect("resolve"),
            None
        );
        assert_eq!(
            status(&fake, &instance_dir, &config_dir),
            ServiceStatus::not_installed()
        );
    }

    #[test]
    fn install_records_what_it_installed_and_is_idempotent() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let instance_dir = dir.path().join("edge-1");
        std::fs::create_dir_all(&instance_dir).expect("create instance dir");
        let config_dir = instance_dir.join(".spice");
        let fake = FakeBackend::new(dir.path());

        let manifest = install(
            &fake,
            &instance_dir,
            &config_dir,
            Path::new("/usr/bin/spiced"),
            "v2.2.0",
            "http://127.0.0.1:8090/health",
        )
        .expect("install");
        assert_eq!(manifest.name, fake.name_for_dir(&instance_dir));
        assert_eq!(manifest.directory, instance_dir);
        assert_eq!(manifest.runtime_version, "v2.2.0");
        assert_eq!(
            ServiceManifest::load(&config_dir, &instance_dir, &fake)
                .expect("load")
                .as_ref(),
            Some(&manifest)
        );

        // Re-running is the upgrade path: it installs again over its own
        // service rather than refusing because the definition is there.
        let again = install(
            &fake,
            &instance_dir,
            &config_dir,
            Path::new("/usr/bin/spiced"),
            "v2.3.0",
            "http://127.0.0.1:8090/health",
        )
        .expect("reinstall");
        assert_eq!(again.runtime_version, "v2.3.0");
        assert_eq!(
            fake.calls(),
            vec!["install".to_string(), "install".to_string()]
        );
    }

    #[test]
    fn install_refuses_a_name_already_claimed_by_another_directory() {
        // The pre-existing-name check: a definition under this directory's
        // derived name that names somewhere else must fail before the
        // installer writes anything.
        let dir = tempfile::tempdir().expect("create tempdir");
        let instance_dir = dir.path().join("edge-1");
        std::fs::create_dir_all(&instance_dir).expect("create instance dir");
        let config_dir = instance_dir.join(".spice");
        let fake = FakeBackend::new(dir.path());
        fake.plant_foreign_definition(&instance_dir);

        let error = install(
            &fake,
            &instance_dir,
            &config_dir,
            Path::new("/usr/bin/spiced"),
            "v2.2.0",
            "http://127.0.0.1:8090/health",
        )
        .expect_err("a foreign definition under the derived name must not be taken over");
        assert!(
            error
                .to_string()
                .contains("belongs to another instance directory"),
            "{error}"
        );
        assert!(fake.calls().is_empty(), "nothing may be installed");
        assert!(
            !ServiceManifest::path_in(&config_dir).exists(),
            "no manifest may be written"
        );
    }

    #[test]
    fn a_service_installed_without_a_manifest_is_still_resolved() {
        // A directory that lost its manifest must not lose the service it
        // still has installed — resolution is still by canonical directory,
        // not by a host-wide search.
        let dir = tempfile::tempdir().expect("create tempdir");
        let instance_dir = dir.path().join("edge-1");
        std::fs::create_dir_all(&instance_dir).expect("create instance dir");
        let config_dir = instance_dir.join(".spice");
        let fake = FakeBackend::new(dir.path());
        fake.install(&InstallRequest {
            instance_dir: &instance_dir,
            config_dir: &config_dir,
            spiced_path: Path::new("/usr/bin/spiced"),
            scope: ServiceScope::System,
        })
        .expect("install without writing a manifest");

        let resolved = resolve(&fake, &instance_dir, &config_dir)
            .expect("resolve")
            .expect("the definition describes this directory's service");
        assert_eq!(resolved.name, fake.name_for_dir(&instance_dir));
        assert_eq!(resolved.directory, instance_dir);
        assert!(
            resolved.runtime_digest.is_empty(),
            "an adopted service records no digest rather than hashing a gigabyte"
        );
        assert_eq!(
            status(&fake, &instance_dir, &config_dir).state,
            ServiceState::Running
        );
    }

    #[test]
    fn a_definition_for_another_directory_is_not_adopted() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let instance_dir = dir.path().join("edge-1");
        let config_dir = instance_dir.join(".spice");
        let fake = FakeBackend::new(dir.path());
        fake.plant_foreign_definition(&instance_dir);

        assert_eq!(
            resolve(&fake, &instance_dir, &config_dir).expect("resolve"),
            None
        );
    }

    #[test]
    fn an_unreadable_manifest_reports_unavailable_rather_than_absent() {
        // "Nothing is installed here" would send `install` on to write over
        // whatever the unreadable manifest describes.
        let dir = tempfile::tempdir().expect("create tempdir");
        let instance_dir = dir.path().join("edge-1");
        let config_dir = instance_dir.join(".spice");
        std::fs::create_dir_all(&config_dir).expect("create config dir");
        std::fs::write(ServiceManifest::path_in(&config_dir), "{").expect("write manifest");

        let fake = FakeBackend::new(dir.path());
        let status = status(&fake, &instance_dir, &config_dir);
        assert_eq!(status.state, ServiceState::Unavailable);
        assert!(status.diagnostic.is_some());
        assert!(!status.installed);
    }

    #[test]
    fn status_reports_every_normalized_state_the_backend_observes() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let instance_dir = dir.path().join("edge-1");
        let config_dir = instance_dir.join(".spice");

        for state in [
            ServiceState::Starting,
            ServiceState::Running,
            ServiceState::Stopping,
            ServiceState::Stopped,
            ServiceState::Failed,
            ServiceState::Unavailable,
        ] {
            let fake = FakeBackend::in_state(dir.path(), state);
            write_manifest_for(&fake, &instance_dir, &config_dir);
            let status = status(&fake, &instance_dir, &config_dir);
            assert!(status.installed, "{state}");
            assert_eq!(status.state, state);
            assert_eq!(
                status.name.as_deref(),
                Some(fake.name_for_dir(&instance_dir).as_str())
            );
            assert_eq!(status.working_dir.as_deref(), Some(instance_dir.as_path()));
            assert!(status.definition_path.is_some(), "{state}");
            assert!(status.runtime_path.is_some(), "{state}");
            assert!(status.log_source.is_some(), "{state}");
        }
    }

    #[test]
    fn uninstall_is_idempotent_and_forgets_the_manifest() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let instance_dir = dir.path().join("edge-1");
        let config_dir = instance_dir.join(".spice");
        let fake = FakeBackend::new(dir.path());
        write_manifest_for(&fake, &instance_dir, &config_dir);

        let removed = uninstall(&fake, &instance_dir, &config_dir)
            .expect("uninstall")
            .expect("a service was installed");
        assert_eq!(removed.name, fake.name_for_dir(&instance_dir));
        assert!(!ServiceManifest::path_in(&config_dir).exists());
        assert_eq!(fake.calls(), vec!["uninstall".to_string()]);

        // Running it again changes nothing and does not call the back end.
        let fake = FakeBackend::new(dir.path());
        assert_eq!(
            uninstall(&fake, &instance_dir, &config_dir).expect("second uninstall"),
            None
        );
        assert!(fake.calls().is_empty());
    }

    #[test]
    fn a_failed_uninstall_keeps_the_manifest() {
        // The directory has to keep knowing what it is trying to remove:
        // forgetting the manifest while the service is still installed would
        // strand it.
        let dir = tempfile::tempdir().expect("create tempdir");
        let instance_dir = dir.path().join("edge-1");
        let config_dir = instance_dir.join(".spice");
        let fake = FakeBackend::failing(dir.path(), "uninstall", "the supervisor refused");
        write_manifest_for(&fake, &instance_dir, &config_dir);

        let error = uninstall(&fake, &instance_dir, &config_dir)
            .expect_err("a supervisor refusal must fail the uninstall");
        assert!(
            error.to_string().contains("the supervisor refused"),
            "{error}"
        );
        assert!(
            ServiceManifest::path_in(&config_dir).exists(),
            "the manifest must survive a failed uninstall"
        );
    }

    /// Install through the fake back end so the shared code around it — the
    /// manifest write, the owner, the log source — is exercised without a
    /// supervisor.
    fn write_manifest_for(backend: &FakeBackend, instance_dir: &Path, config_dir: &Path) {
        let name = backend.name_for_dir(instance_dir);
        let scope = ServiceScope::System;
        ServiceManifest {
            schema_version: manifest::MANIFEST_SCHEMA_VERSION,
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
        .write(config_dir)
        .expect("write manifest");
    }

    #[test]
    fn status_carries_the_backends_diagnostic_and_remediation() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let instance_dir = dir.path().join("edge-1");
        let config_dir = instance_dir.join(".spice");
        let mut fake = FakeBackend::in_state(dir.path(), ServiceState::Running);
        fake.observation.starts = ServiceStarts::LoginOnly;
        fake.observation.starts_action = Some("loginctl enable-linger alice".to_string());
        write_manifest_for(&fake, &instance_dir, &config_dir);

        let status = status(&fake, &instance_dir, &config_dir);
        assert_eq!(status.starts, ServiceStarts::LoginOnly);
        assert_eq!(
            status.starts_action.as_deref(),
            Some("loginctl enable-linger alice")
        );
    }
}
