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

//! `spice connect --install`: run `spiced --cloud-connect` as a persistent
//! system service.
//!
//! The supervisor is not a convenience here, it is the mechanism deployments
//! depend on: a deployment applies by having the runtime validate and persist
//! the new spicepod and then exit 0, which only becomes a deployment if
//! something relaunches it. Both back ends therefore restart the runtime
//! unconditionally — systemd with `Restart=always`, launchd with `KeepAlive`.
//!
//! ## Support matrix
//!
//! `--install` is Linux with systemd and macOS with launchd, and needs root on
//! both. Containers get the same guarantee from their runtime's restart policy
//! (`docker run --restart unless-stopped`) and use the env-var flow instead.
//! Windows enrolls and runs `spiced --cloud-connect` under the user's own
//! supervisor.
//!
//! macOS installs a `LaunchDaemon`, not a `LaunchAgent`: an agent runs only
//! while its user is logged in, which is not the guarantee `--install` makes.
//! A daemon starts at boot and survives logout. Root installs and manages its
//! definition, while the runtime itself runs as the non-root operator.
//!
//! ## One service per instance directory
//!
//! Two instances on one host must produce two independently-running services,
//! so the service name is derived deterministically from the *absolute*
//! instance directory (see [`name_stem_for_dir`]) and the directory is baked
//! into the definition as its working directory. A `--install` or `remove` in
//! one instance directory can therefore never touch another instance's service.
//!
//! Both back ends run `spiced` as the non-root operator who invoked `sudo` (or
//! owns the instance directory).

#[cfg(unix)]
#[cfg_attr(not(target_os = "macos"), expect(dead_code))]
mod launchd;
#[cfg(unix)]
#[cfg_attr(not(target_os = "linux"), expect(dead_code))]
mod systemd;

use std::path::{Path, PathBuf};

#[cfg(unix)]
use std::os::unix::fs::MetadataExt as _;

use sha2::{Digest as _, Sha256};

use crate::error::{Error, Result};

#[cfg(target_os = "linux")]
use systemd as backend;

#[cfg(target_os = "macos")]
use launchd as backend;

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
use unsupported as backend;

/// Longest directory-name fragment carried into a service name, so a deeply-named
/// instance directory cannot produce an unwieldy name. The appended digest is
/// what makes the name unique; the fragment only makes it legible.
const MAX_NAME_FRAGMENT: usize = 32;

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
            message: "Failed to install the Spice Cloud Connect service: SUDO_UID and SUDO_GID must identify the non-root operator who will run spiced. Re-run from that account with `sudo spice connect --install`.".to_string(),
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
            "Failed to install the Spice Cloud Connect service: {} is root-owned and no non-root sudo caller is available. Run the command from the intended operator account with `sudo spice connect --install`.",
            instance_dir.display()
        ),
    })
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

/// A service installed for one instance directory.
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
/// The stem is `<fragment>-<digest>`: a legible fragment from the directory
/// name plus the first 8 hex digits of the SHA-256 of the absolute path. The
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
    let mut short = String::with_capacity(8);
    for byte in digest.iter().take(4) {
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
/// Distinguished from a generic error so the caller can run the check *before*
/// the HTTPS enroll: a failed preflight must consume nothing, leaving the
/// adoption code valid.
#[derive(Debug)]
pub(crate) enum PreflightFailure {
    /// Neither Linux nor macOS — `--install` has no supervisor to install into.
    UnsupportedPlatform,
    /// Linux, but systemd is not the running init.
    SystemdUnavailable,
    /// Not running as root, so the service definition directory is not writable
    /// and the supervisor cannot be asked to manage the service.
    NotRoot,
}

impl PreflightFailure {
    /// The operator-facing message, naming the fix and the alternative path.
    fn message(&self) -> String {
        match self {
            Self::UnsupportedPlatform => format!(
                "Failed to install the Spice Cloud Connect service: --install requires \
                 Linux with systemd or macOS with launchd (this host is {}). Enroll without \
                 --install and run `spiced --cloud-connect` under your own supervisor, or run \
                 Spice in a container with a restart policy (`docker run --restart unless-stopped`). \
                 See: https://spiceai.org/docs",
                std::env::consts::OS
            ),
            Self::SystemdUnavailable => format!(
                "Failed to install the Spice Cloud Connect service: systemd is not running on \
                 this host ({SYSTEMD_RUNTIME_MARKER} is absent). This is normal inside a \
                 container: set SPICE_CONNECT_ADOPT_CODE and run `spiced --cloud-connect` under \
                 the container runtime's restart policy (`docker run --restart unless-stopped`) \
                 instead of --install. See: https://spiceai.org/docs"
            ),
            Self::NotRoot => format!(
                "Failed to install the Spice Cloud Connect service: installing a {} requires \
                 root. Re-run with sudo: `sudo spice connect --install`. \
                 See: https://spiceai.org/docs",
                if cfg!(target_os = "macos") {
                    "launchd daemon"
                } else {
                    "systemd unit"
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
/// Called ahead of the HTTPS enroll so a host with no supervisor or without
/// root fails with nothing installed and the adoption code still redeemable.
pub(crate) fn preflight() -> std::result::Result<(), PreflightFailure> {
    if !cfg!(any(target_os = "linux", target_os = "macos")) {
        return Err(PreflightFailure::UnsupportedPlatform);
    }
    backend::preflight()
}

/// Install (or reinstall) and start the service for `instance_dir`, preserving
/// the resolved `config_dir` in the service environment.
///
/// Idempotent, and the in-place upgrade path: re-running restages the current
/// `spiced` binary, rewrites the service definition, and restarts the service,
/// leaving the staged identity untouched. Returns the installed service.
///
/// `spiced_path` is where the CLI found the runtime; it is copied to a
/// root-owned location (see [`stage_runtime`]) rather than referenced in place.
///
/// # Errors
///
/// Returns an error when the runtime cannot be staged, the definition cannot be
/// written, or the supervisor rejects it. Since this runs *after* the enroll,
/// the messages say that the identity is already staged and how to resume.
pub(crate) fn install(
    instance_dir: &Path,
    config_dir: &Path,
    spiced_path: &Path,
) -> Result<InstalledService> {
    backend::install(instance_dir, config_dir, spiced_path)
}

/// Stop and delete the service for `instance_dir`.
///
/// Returns the service that was removed, or `None` when none was installed for
/// this directory.
///
/// # Errors
///
/// Returns an error when the definition cannot be deleted, or when the
/// supervisor is left holding a job that the deleted definition can no longer
/// account for.
pub(crate) fn uninstall(instance_dir: &Path) -> Result<Option<InstalledService>> {
    backend::uninstall(instance_dir)
}

/// The service installed for `instance_dir`, if any.
///
/// Matches on the derived name rather than by scanning working directories, so
/// the lookup is exact and cannot pick up another instance's service.
pub(crate) fn find_for_dir(instance_dir: &Path) -> Option<InstalledService> {
    backend::find_for_dir(instance_dir)
}

/// Every service this command installed on the host, sorted by name.
///
/// This is what lets `spice connect status` report against installed services
/// when the resolved directory holds no instance state, instead of printing a
/// misleading "not connected" for a host that is in fact connected from
/// somewhere else.
pub(crate) fn discover_all() -> Vec<InstalledService> {
    backend::discover_all()
}

/// The service's current state as the supervisor reports it, or `None` when it
/// cannot be determined.
pub(crate) fn is_active(name: &str) -> Option<String> {
    backend::is_active(name)
}

/// The commands an operator inspects and follows this service with, in the
/// vocabulary of the supervisor that owns it.
pub(crate) fn manage_hints(name: &str) -> Vec<String> {
    backend::manage_hints(name)
}

/// `true` when this process runs with root's effective user id — the id that
/// governs writing the service definition and directing the supervisor.
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
/// `--install` restages whatever `spiced` the CLI now resolves. One host has one
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
    // re-running `--install` is the documented way to put a good binary back on
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
                 `sudo spice connect --install`.",
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
                     `sudo spice connect --install`.",
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

/// Stubs for a host with no supported supervisor. [`preflight`] rejects such a
/// host before any of these can be reached; they exist so the CLI still builds
/// for it.
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
mod unsupported {
    use std::path::Path;

    use super::{InstalledService, PreflightFailure};
    use crate::error::Result;

    pub(super) fn preflight() -> std::result::Result<(), PreflightFailure> {
        Err(PreflightFailure::UnsupportedPlatform)
    }

    pub(super) fn install(
        _instance_dir: &Path,
        _config_dir: &Path,
        _spiced_path: &Path,
    ) -> Result<InstalledService> {
        Err(PreflightFailure::UnsupportedPlatform.into())
    }

    pub(super) fn uninstall(_instance_dir: &Path) -> Result<Option<InstalledService>> {
        Ok(None)
    }

    pub(super) fn find_for_dir(_instance_dir: &Path) -> Option<InstalledService> {
        None
    }

    pub(super) fn discover_all() -> Vec<InstalledService> {
        Vec::new()
    }

    pub(super) fn is_active(_name: &str) -> Option<String> {
        None
    }

    pub(super) fn manage_hints(_name: &str) -> Vec<String> {
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        // these apart — a `remove` in one must never touch the other's service.
        let a = name_stem_for_dir(Path::new("/srv/a/edge"));
        let b = name_stem_for_dir(Path::new("/srv/b/edge"));
        assert_ne!(a, b);
        assert!(a.starts_with("edge-"), "{a}");
        assert!(b.starts_with("edge-"), "{b}");
    }

    #[test]
    fn name_stem_sanitizes_and_bounds_the_fragment() {
        let stem = name_stem_for_dir(Path::new("/opt/My Edge_Site!!/"));
        assert!(stem.starts_with("my-edge-site-"), "{stem}");

        // A long directory name is truncated, not carried whole.
        let long = name_stem_for_dir(Path::new(&format!("/opt/{}", "a".repeat(120))));
        assert!(long.len() <= MAX_NAME_FRAGMENT + 9, "{long}");
    }

    #[test]
    fn name_stem_handles_a_directory_with_no_usable_name() {
        // Only the digest names it, and it is still 8 hex digits.
        let stem = name_stem_for_dir(Path::new("/"));
        assert_eq!(stem.len(), 8, "{stem}");
        assert!(stem.chars().all(|ch| ch.is_ascii_hexdigit()), "{stem}");
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
        let dir = std::env::temp_dir().join(format!("spice-stage-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("scratch dir");
        let source = dir.join("spiced");
        let dest = dir.join("staged");
        let stamp_path = dir.join("staged.stamp");

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

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_runtime_that_fails_verification_never_reaches_the_live_path() {
        // The destination is what every installed service re-executes when it
        // restarts, so a runtime that turns out to be unusable must not land on
        // it — a rejected upgrade in one instance directory would otherwise
        // take out every other instance on the host.
        let dir = std::env::temp_dir().join(format!("spice-publish-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("scratch dir");
        let source = dir.join("spiced");
        let dest = dir.join("staged");
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

        let _ = std::fs::remove_dir_all(&dir);
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
        // Re-running `--install` is the documented way to put a good binary
        // back on the path every service executes. Refusing on the strength of
        // the stamp would make that the one repair it could not perform.
        let dir = std::env::temp_dir().join(format!("spice-restage-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("scratch dir");
        let source = dir.join("spiced");
        let dest = dir.join("staged");
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

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_staged_runtime_that_still_works_is_not_copied_again() {
        // The other half: a gigabyte is not recopied on every `--install`.
        let dir = std::env::temp_dir().join(format!("spice-reuse-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("scratch dir");
        let source = dir.join("spiced");
        let dest = dir.join("staged");
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

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn source_stamp_distinguishes_length_and_mtime() {
        let dir = std::env::temp_dir().join(format!("spice-stamp-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("scratch dir");
        let a = dir.join("a");
        let b = dir.join("b");
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
        assert_eq!(source_stamp(&dir.join("missing")), None);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn copying_a_runtime_leaves_its_extended_attributes_behind() {
        // On macOS this is what keeps `com.apple.quarantine` off the copy
        // launchd executes: the stage writes bytes into a new file rather than
        // cloning the original.
        let dir = std::env::temp_dir().join(format!("spice-xattr-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("scratch dir");
        let source = dir.join("spiced");
        let dest = dir.join("staged");
        std::fs::write(&source, b"runtime").expect("write source");

        let set = std::process::Command::new("xattr")
            .args(["-w", "com.apple.quarantine", "0083;00000000;Test;"])
            .arg(&source)
            .status();
        if !set.is_ok_and(|status| status.success()) {
            // No `xattr` (any non-macOS host): nothing to prove here.
            let _ = std::fs::remove_dir_all(&dir);
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

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn preflight_failures_name_the_fix() {
        assert!(
            PreflightFailure::NotRoot.message().contains("sudo"),
            "the root failure must name sudo"
        );
        assert!(
            PreflightFailure::SystemdUnavailable
                .message()
                .contains("restart policy"),
            "the no-systemd failure must point containers at the env-var flow"
        );
        assert!(
            PreflightFailure::UnsupportedPlatform
                .message()
                .contains("spiced --cloud-connect"),
            "an unsupported host must be told the supported way to run"
        );
        assert!(
            PreflightFailure::UnsupportedPlatform
                .message()
                .contains("launchd"),
            "macOS is supported, so the message must not read as Linux-only"
        );
    }
}
