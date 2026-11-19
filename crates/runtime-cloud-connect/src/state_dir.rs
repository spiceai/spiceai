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

//! The directory that holds Cloud Connect state, and the rule for trusting it.
//!
//! Everything Cloud Connect keeps lives in `<config dir>/cloud-connect/`, a
//! directory Spice creates owner-only. The config directory itself (`.spice`,
//! or `SPICE_CONFIG_DIR`) may have any owner and mode, as a Kubernetes
//! `emptyDir` does: another account cannot create a directory owned by this
//! one, so the state directory stays trustworthy even when its parent is
//! shared. Cloud Connect state is Unix-only; elsewhere its owner-only writes
//! report `Unsupported`, so there is nothing for this rule to protect.

use std::path::{Path, PathBuf};
use std::sync::Once;

/// The state directory's name inside the config directory.
pub const STATE_DIR_NAME: &str = "cloud-connect";

const DOCS: &str = "https://spiceai.org/docs/cli/reference/cloud";

/// Every file Cloud Connect keeps, as named in the config directory before
/// the state directory existed. The first Spice command or start after an
/// upgrade moves them. Lock files are not moved: each lock is taken where it
/// lives, and the old mutation lock is the one an older CLI still takes.
pub const LEGACY_STATE_FILES: [&str; 14] = [
    crate::config::IDENTITY_FILE,
    crate::secret_cache::SECRET_CACHE_FILE,
    crate::config::CLOUD_MANAGED_SPICEPOD_FILE,
    crate::config::INCOMING_CLOUD_MANAGED_SPICEPOD_FILE,
    crate::config::PREVIOUS_CLOUD_MANAGED_SPICEPOD_FILE,
    crate::config::PREVIOUS_SECRET_CACHE_FILE,
    crate::config::INCOMING_SECRET_CACHE_FILE,
    crate::config::DEPLOYMENT_TRANSACTION_FILE,
    crate::config::DEPLOYMENT_TRANSACTION_INCOMING_FILE,
    crate::draft::ENROLLMENT_DRAFT_FILE,
    crate::config::CloudConnectConfig::ENDPOINT_OVERRIDE_FILE,
    crate::config::CloudConnectConfig::CONNECT_OPERATION_FILE,
    crate::config::CloudConnectConfig::PROJECT_OPERATION_FILE,
    crate::config::SERVICE_MANIFEST_FILE,
];

/// The state directory for a config directory.
#[must_use]
pub fn state_dir(config_dir: &Path) -> PathBuf {
    config_dir.join(STATE_DIR_NAME)
}

/// Why a directory holding Cloud Connect state cannot be trusted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Untrusted {
    /// Owned by `owner` and writable by other accounts: a root-run `spiced`
    /// over another account's directory, or a config directory that still
    /// holds state to move.
    WritableByOthers { dir: PathBuf, owner: u32 },
    /// Owned by `owner`, not by this account.
    OwnedByAnotherAccount { dir: PathBuf, owner: u32, me: u32 },
}

impl Untrusted {
    /// What is wrong and the command that fixes it, as the clause `spiced`
    /// prints after "Failed to start: ".
    #[must_use]
    pub fn message(&self) -> String {
        match self {
            Self::WritableByOthers { dir, owner } => format!(
                "the Spice Cloud Connect state directory '{dir}' is owned by user id {owner} and writable by other accounts, so its deployment and identity files could be replaced. Run `chmod go-w '{dir}'` as its owner, or set SPICE_CONFIG_DIR to a directory only this account can write. See: {DOCS}",
                dir = dir.display()
            ),
            Self::OwnedByAnotherAccount { dir, owner, me } => format!(
                "the Spice Cloud Connect state directory '{dir}' is owned by user id {owner}, not by this account (user id {me}), so that account could replace its deployment and identity files. Run `chown -R {me} '{dir}'` as root, or set SPICE_CONFIG_DIR to a directory this account owns. See: {DOCS}",
                dir = dir.display()
            ),
        }
    }
}

#[derive(Debug, snafu::Snafu)]
pub enum Error {
    #[snafu(display("{}", untrusted.message()))]
    Untrusted { untrusted: Untrusted },

    #[snafu(display(
        "could not use the Spice Cloud Connect state directory '{}': {source}",
        path.display()
    ))]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
}

impl Error {
    /// The message as a sentence of its own, for `spice cloud status`.
    #[must_use]
    pub fn sentence(&self) -> String {
        let message = self.to_string();
        let mut characters = message.chars();
        match characters.next() {
            Some(first) => first.to_uppercase().chain(characters).collect(),
            None => message,
        }
    }

    /// The trust failure, when that is what this is.
    #[must_use]
    pub fn untrusted(&self) -> Option<&Untrusted> {
        match self {
            Self::Untrusted { untrusted } => Some(untrusted),
            Self::Io { .. } => None,
        }
    }

    fn io(path: &Path) -> impl FnOnce(std::io::Error) -> Self + '_ {
        move |source| Self::Io {
            path: path.to_path_buf(),
            source,
        }
    }
}

/// Whether a state directory exists after [`check`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Presence {
    Absent,
    Present,
}

/// Apply the trust rule to a state directory, creating it owner-only when
/// `create` is set and it does not exist.
///
/// | State directory | Result |
/// | --- | --- |
/// | Absent | Created `0700` when `create`; otherwise [`Presence::Absent`]. |
/// | Owned by this account or root, not writable by group or other | Used. |
/// | Owned by this account, writable by group or other | `chmod go-w`, one INFO line, then used. |
/// | This process is root; owned by one other account, not writable by others | Used. |
/// | Anything else | [`Error::Untrusted`]. |
///
/// # Errors
///
/// [`Error::Untrusted`] when the directory cannot be trusted, and
/// [`Error::Io`] when it cannot be inspected, created or repaired, or is not
/// a real directory.
pub fn check(dir: &Path, create: bool) -> Result<Presence, Error> {
    #[cfg(unix)]
    {
        check_unix(dir, create)
    }

    #[cfg(not(unix))]
    {
        if dir.is_dir() {
            Ok(Presence::Present)
        } else if create {
            std::fs::create_dir_all(dir).map_err(Error::io(dir))?;
            Ok(Presence::Present)
        } else {
            Ok(Presence::Absent)
        }
    }
}

#[cfg(unix)]
fn check_unix(dir: &Path, create: bool) -> Result<Presence, Error> {
    use std::os::unix::fs::{
        DirBuilderExt as _, MetadataExt as _, OpenOptionsExt as _, PermissionsExt as _,
    };

    // SAFETY: `geteuid` has no preconditions and cannot fail.
    let me = unsafe { libc::geteuid() };
    let mut created_by_this_process = false;
    match std::fs::symlink_metadata(dir) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            return Err(Error::io(dir)(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "it is a symbolic link, and Spice keeps this state only in a real directory",
            )));
        }
        Ok(_) => {}
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
            if !create {
                return Ok(Presence::Absent);
            }
            if let Some(parent) = dir.parent() {
                std::fs::create_dir_all(parent).map_err(Error::io(parent))?;
            }
            match std::fs::DirBuilder::new().mode(0o700).create(dir) {
                Ok(()) => created_by_this_process = true,
                Err(source) if source.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(source) => return Err(Error::io(dir)(source)),
            }
        }
        Err(source) => return Err(Error::io(dir)(source)),
    }

    // Every decision below is made on this descriptor, so the directory
    // inspected is the one repaired.
    let directory = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(dir)
        .map_err(Error::io(dir))?;
    if created_by_this_process
        && me == 0
        && let Some(parent) = dir.parent()
    {
        // A privileged command creates it for the account that owns the
        // config directory, as it does the lock file, so that account's
        // runtime can keep writing it.
        let parent = std::fs::metadata(parent).map_err(Error::io(parent))?;
        std::os::unix::fs::fchown(&directory, Some(parent.uid()), Some(parent.gid()))
            .map_err(Error::io(dir))?;
    }
    let metadata = directory.metadata().map_err(Error::io(dir))?;
    if !metadata.is_dir() {
        return Err(Error::io(dir)(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "it is not a directory",
        )));
    }

    let owner = metadata.uid();
    let mode = metadata.mode() & 0o7777;
    let writable_by_others = mode & 0o022 != 0;
    if owner == me {
        if writable_by_others {
            let repaired = mode & !0o022;
            directory
                .set_permissions(std::fs::Permissions::from_mode(repaired))
                .map_err(Error::io(dir))?;
            log_repair_once(dir, mode, repaired);
        }
        return Ok(Presence::Present);
    }
    if !writable_by_others && (owner == 0 || me == 0) {
        return Ok(Presence::Present);
    }
    Err(Error::Untrusted {
        untrusted: if me == 0 {
            Untrusted::WritableByOthers {
                dir: dir.to_path_buf(),
                owner,
            }
        } else {
            Untrusted::OwnedByAnotherAccount {
                dir: dir.to_path_buf(),
                owner,
                me,
            }
        },
    })
}

/// The INFO line for a repaired mode.
#[must_use]
pub fn repair_message(dir: &Path, from: u32, to: u32) -> String {
    format!(
        "Spice Cloud Connect removed group and other write access from its state directory '{}' (mode {from:04o} → {to:04o}), so that only this account can add files there. See: {DOCS}",
        dir.display()
    )
}

fn log_repair_once(dir: &Path, from: u32, to: u32) {
    static REPAIRED: Once = Once::new();
    REPAIRED.call_once(|| {
        tracing::info!(target: "spiced::cloud_connect", "{}", repair_message(dir, from, to));
    });
}

/// Whether `config_dir` still holds Cloud Connect state written before the
/// state directory existed. Never-enrolled directories hold none.
#[must_use]
pub fn legacy_state_present(config_dir: &Path) -> bool {
    !legacy_state_names(config_dir).is_empty()
}

/// The legacy state entries in `config_dir`: the fixed names, and the
/// quarantined journals and drafts `spice cloud link` set aside for an
/// operator, which belong with the state they were set aside from.
fn legacy_state_names(config_dir: &Path) -> Vec<std::ffi::OsString> {
    let mut names: Vec<std::ffi::OsString> = LEGACY_STATE_FILES
        .iter()
        .filter(|name| std::fs::symlink_metadata(config_dir.join(name)).is_ok())
        .map(std::ffi::OsString::from)
        .collect();
    if let Ok(entries) = std::fs::read_dir(config_dir) {
        names.extend(
            entries
                .filter_map(std::result::Result::ok)
                .map(|entry| entry.file_name())
                .filter(|name| name.to_str().is_some_and(is_quarantined_state)),
        );
    }
    names
}

/// `{stem}.quarantine.{epoch}.{pid}.json`, as `spice cloud link` names a
/// journal or draft it sets aside.
fn is_quarantined_state(name: &str) -> bool {
    let stems = [
        crate::draft::ENROLLMENT_DRAFT_FILE,
        crate::config::CloudConnectConfig::CONNECT_OPERATION_FILE,
        crate::config::CloudConnectConfig::PROJECT_OPERATION_FILE,
    ]
    .map(|file| file.trim_end_matches(".json"));
    stems.iter().any(|stem| {
        name.strip_prefix(stem)
            .and_then(|rest| rest.strip_prefix(".quarantine."))
            .and_then(|rest| rest.strip_suffix(".json"))
            .is_some_and(|rest| {
                let mut parts = rest.split('.');
                matches!(
                    (parts.next(), parts.next(), parts.next()),
                    (Some(epoch), Some(pid), None)
                        if !epoch.is_empty()
                            && !pid.is_empty()
                            && epoch.bytes().all(|byte| byte.is_ascii_digit())
                            && pid.bytes().all(|byte| byte.is_ascii_digit())
                )
            })
    })
}

/// Move Cloud Connect state written before the state directory existed from
/// `config_dir` into it, under the mutation lock at both locations. The
/// caller holds the instance's runtime lock (`spiced` does for its whole
/// life); a CLI command uses [`migrate_legacy_state_when_idle`].
///
/// The config directory must not be owned by another account and writable
/// by others. Spice never changes its mode. Only files that pass the
/// per-file checks (a regular, singly-linked file owned by this account or
/// the config directory's owner, not a link) are moved; any other entry is
/// left where it is and never read. A file already in the state directory
/// wins. Returns how many files moved.
///
/// # Errors
///
/// [`Error::Untrusted`] when the config directory or the state directory
/// cannot be trusted, and [`Error::Io`] when a lock or a move fails.
pub async fn migrate_legacy_state(config_dir: &Path) -> Result<usize, Error> {
    if legacy_state_names(config_dir).is_empty() {
        return Ok(0);
    }
    check_config_dir(config_dir)?;
    let config_dir_owner = owner_of(config_dir)?;

    // A CLI that predates the state directory takes its lock here; holding
    // it too keeps such a CLI from changing a file mid-move. Only root and
    // the config directory's owner can take it, so when this account is
    // neither (a root-owned Kubernetes `emptyDir`), an older CLI run as this
    // account cannot take it either, and so changes nothing.
    let _legacy = if may_take_legacy_lock(config_dir_owner) {
        Some(
            crate::MutationLock::acquire_legacy(config_dir, "migrate")
                .await
                .map_err(lock_error(config_dir))?,
        )
    } else {
        None
    };
    let state = state_dir(config_dir);
    let _lock = crate::MutationLock::acquire(&state, "migrate")
        .await
        .map_err(lock_error(&state))?;

    let mut moved = 0;
    // Listed again under the locks: an older CLI may have changed them.
    for name in legacy_state_names(config_dir) {
        let from = config_dir.join(&name);
        let to = state.join(&name);
        if std::fs::symlink_metadata(&from).is_err()
            || std::fs::symlink_metadata(&to).is_ok()
            || !written_by_spice(&from, config_dir_owner)
        {
            continue;
        }
        std::fs::rename(&from, &to).map_err(Error::io(&from))?;
        // Checked again where only this account can change it: an entry
        // swapped in between the check and the move goes back, unused.
        if !written_by_spice(&to, config_dir_owner) {
            std::fs::rename(&to, &from).map_err(Error::io(&to))?;
            continue;
        }
        moved += 1;
    }
    if moved > 0 {
        crate::identity::sync_parent_directory(&state.join(crate::config::IDENTITY_FILE))
            .map_err(Error::io(&state))?;
        crate::identity::sync_parent_directory(&config_dir.join(crate::config::IDENTITY_FILE))
            .map_err(Error::io(config_dir))?;
    }
    Ok(moved)
}

/// [`migrate_legacy_state`] for a CLI command: moves nothing while a
/// runtime holds the instance directory, since an older runtime still reads
/// and writes the old location and a current one already moved its state
/// when it started.
///
/// # Errors
///
/// As [`migrate_legacy_state`].
pub async fn migrate_legacy_state_when_idle(config_dir: &Path) -> Result<usize, Error> {
    if !legacy_state_present(config_dir) {
        return Ok(0);
    }
    // A runtime creates its lock file before anything else, so without one
    // no runtime holds the directory. Not creating it here keeps a command
    // run as root from leaving a lock file the instance's own account
    // cannot open.
    if std::fs::symlink_metadata(config_dir.join(crate::runtime_lock::RUNTIME_LOCK_FILE)).is_err() {
        return migrate_legacy_state(config_dir).await;
    }
    let claim_dir = config_dir.to_path_buf();
    let claim = tokio::task::spawn_blocking(move || crate::RuntimeLock::acquire(&claim_dir))
        .await
        .map_err(|source| Error::Io {
            path: config_dir.to_path_buf(),
            source: std::io::Error::other(format!("runtime lock task panicked: {source}")),
        })?;
    let _runtime = match claim {
        Ok(lock) => lock,
        Err(crate::runtime_lock::Error::AlreadyRunning { .. }) => return Ok(0),
        Err(source) => {
            return Err(Error::Io {
                path: config_dir.to_path_buf(),
                source: std::io::Error::other(source.to_string()),
            });
        }
    };
    migrate_legacy_state(config_dir).await
}

fn lock_error(path: &Path) -> impl FnOnce(crate::mutation_lock::Error) -> Error + '_ {
    move |source| match source {
        crate::mutation_lock::Error::StateDirectory { source } => source,
        other => Error::Io {
            path: path.to_path_buf(),
            source: std::io::Error::other(other.to_string()),
        },
    }
}

/// The config directory may have any owner and mode, except owned by another
/// account and writable by others: then a file in it could have been put
/// there by that account rather than by Spice.
fn check_config_dir(config_dir: &Path) -> Result<(), Error> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        let metadata = std::fs::metadata(config_dir).map_err(Error::io(config_dir))?;
        // SAFETY: `geteuid` has no preconditions and cannot fail.
        let me = unsafe { libc::geteuid() };
        let owner = metadata.uid();
        if owner != me && owner != 0 && metadata.mode() & 0o022 != 0 {
            return Err(Error::Untrusted {
                untrusted: Untrusted::WritableByOthers {
                    dir: config_dir.to_path_buf(),
                    owner,
                },
            });
        }
    }
    #[cfg(not(unix))]
    let _ = config_dir;
    Ok(())
}

fn may_take_legacy_lock(config_dir_owner: u32) -> bool {
    #[cfg(unix)]
    {
        // SAFETY: `geteuid` has no preconditions and cannot fail.
        let me = unsafe { libc::geteuid() };
        me == 0 || me == config_dir_owner
    }
    #[cfg(not(unix))]
    {
        let _ = config_dir_owner;
        true
    }
}

fn owner_of(dir: &Path) -> Result<u32, Error> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        Ok(std::fs::metadata(dir).map_err(Error::io(dir))?.uid())
    }
    #[cfg(not(unix))]
    {
        let _ = dir;
        Ok(0)
    }
}

/// The per-file checks every state read applies: a regular, singly-linked
/// file, not a link, owned by this account or the directory's owner.
fn written_by_spice(path: &Path, directory_owner: u32) -> bool {
    let Ok(metadata) = std::fs::symlink_metadata(path) else {
        return false;
    };
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        // SAFETY: `geteuid` has no preconditions and cannot fail.
        let me = unsafe { libc::geteuid() };
        metadata.file_type().is_file()
            && metadata.nlink() == 1
            && (metadata.uid() == me || metadata.uid() == directory_owner)
    }
    #[cfg(not(unix))]
    {
        let _ = directory_owner;
        metadata.file_type().is_file()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The wording a user reads: both errors name the directory, its owner,
    /// and the command that fixes it.
    #[test]
    fn untrusted_messages_name_the_directory_and_the_fix() {
        assert_eq!(
            Untrusted::WritableByOthers {
                dir: PathBuf::from("/srv/app/.spice/cloud-connect"),
                owner: 1001,
            }
            .message(),
            "the Spice Cloud Connect state directory '/srv/app/.spice/cloud-connect' is owned by user id 1001 and writable by other accounts, so its deployment and identity files could be replaced. Run `chmod go-w '/srv/app/.spice/cloud-connect'` as its owner, or set SPICE_CONFIG_DIR to a directory only this account can write. See: https://spiceai.org/docs/cli/reference/cloud"
        );
        let another_account = Untrusted::OwnedByAnotherAccount {
            dir: PathBuf::from("/srv/app/.spice/cloud-connect"),
            owner: 1001,
            me: 1000,
        };
        assert_eq!(
            another_account.message(),
            "the Spice Cloud Connect state directory '/srv/app/.spice/cloud-connect' is owned by user id 1001, not by this account (user id 1000), so that account could replace its deployment and identity files. Run `chown -R 1000 '/srv/app/.spice/cloud-connect'` as root, or set SPICE_CONFIG_DIR to a directory this account owns. See: https://spiceai.org/docs/cli/reference/cloud"
        );
        assert!(
            Error::Untrusted {
                untrusted: another_account
            }
            .sentence()
            .starts_with("The Spice Cloud Connect state directory '/srv/app/.spice/cloud-connect' is owned by user id 1001, not by this account")
        );
        assert_eq!(
            repair_message(Path::new("/srv/app/.spice/cloud-connect"), 0o775, 0o755),
            "Spice Cloud Connect removed group and other write access from its state directory '/srv/app/.spice/cloud-connect' (mode 0775 → 0755), so that only this account can add files there. See: https://spiceai.org/docs/cli/reference/cloud"
        );
    }

    #[cfg(unix)]
    #[test]
    fn an_owned_state_directory_is_created_owner_only_and_repaired() {
        use std::os::unix::fs::PermissionsExt as _;

        let root = tempfile::tempdir().expect("tempdir");
        let dir = state_dir(&root.path().join(".spice"));
        let mode = |dir: &Path| std::fs::metadata(dir).expect("stat").permissions().mode() & 0o777;

        assert_eq!(check(&dir, false).expect("absent"), Presence::Absent);
        assert!(
            !root.path().join(".spice").exists(),
            "checking without `create` creates nothing"
        );

        assert_eq!(check(&dir, true).expect("create"), Presence::Present);
        assert_eq!(mode(&dir), 0o700);

        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o775)).expect("chmod");
        assert_eq!(check(&dir, false).expect("repair"), Presence::Present);
        assert_eq!(mode(&dir), 0o755);
    }

    #[cfg(unix)]
    #[test]
    fn a_symlinked_state_directory_is_refused() {
        let root = tempfile::tempdir().expect("tempdir");
        let elsewhere = root.path().join("elsewhere");
        std::fs::create_dir(&elsewhere).expect("create target");
        let config_dir = root.path().join(".spice");
        std::fs::create_dir(&config_dir).expect("create config dir");
        std::os::unix::fs::symlink(&elsewhere, state_dir(&config_dir)).expect("symlink");

        let error = check(&state_dir(&config_dir), true).expect_err("a link is refused");
        assert_eq!(
            error.to_string(),
            format!(
                "could not use the Spice Cloud Connect state directory '{}': it is a symbolic link, and Spice keeps this state only in a real directory",
                state_dir(&config_dir).display()
            )
        );
    }

    /// Files Spice wrote move; a link, a hard-linked file, and a file already
    /// in the state directory stay where they are.
    #[cfg(unix)]
    #[tokio::test]
    async fn legacy_state_moves_into_the_state_directory() {
        let root = tempfile::tempdir().expect("tempdir");
        let config_dir = root.path().join(".spice");
        std::fs::create_dir(&config_dir).expect("create config dir");
        std::fs::write(config_dir.join("identity.json"), b"{}").expect("write identity");
        std::fs::write(
            config_dir.join("spicepod-cloud-managed.yml"),
            b"version: v1",
        )
        .expect("write deployment");
        std::fs::write(root.path().join("target"), b"elsewhere").expect("write target");
        std::os::unix::fs::symlink(
            root.path().join("target"),
            config_dir.join("secrets-cache.json"),
        )
        .expect("symlink");
        std::fs::write(config_dir.join("cloud-endpoint"), b"https://a").expect("write");
        std::fs::hard_link(
            config_dir.join("cloud-endpoint"),
            root.path().join("second-name"),
        )
        .expect("hard link");
        std::fs::write(config_dir.join("service.json"), b"old").expect("write manifest");
        std::fs::write(
            config_dir.join("enrollment-draft.quarantine.1.2.json"),
            b"set aside",
        )
        .expect("write quarantined draft");
        std::fs::create_dir(state_dir(&config_dir)).expect("create state dir");
        std::fs::write(state_dir(&config_dir).join("service.json"), b"new").expect("write");

        assert_eq!(migrate_legacy_state(&config_dir).await.expect("migrate"), 3);

        let state = state_dir(&config_dir);
        assert_eq!(
            std::fs::read(state.join("identity.json")).expect("read"),
            b"{}"
        );
        assert_eq!(
            std::fs::read(state.join("spicepod-cloud-managed.yml")).expect("read"),
            b"version: v1"
        );
        assert!(!config_dir.join("identity.json").exists());
        assert_eq!(
            std::fs::read(state.join("enrollment-draft.quarantine.1.2.json")).expect("read"),
            b"set aside"
        );
        assert!(
            config_dir
                .join("secrets-cache.json")
                .symlink_metadata()
                .expect("link stays")
                .file_type()
                .is_symlink()
        );
        assert!(!state.join("secrets-cache.json").exists());
        assert!(config_dir.join("cloud-endpoint").exists());
        assert!(!state.join("cloud-endpoint").exists());
        assert_eq!(
            std::fs::read(state.join("service.json")).expect("read"),
            b"new"
        );
        assert_eq!(
            std::fs::read(config_dir.join("service.json")).expect("read"),
            b"old"
        );

        assert_eq!(
            migrate_legacy_state(&config_dir).await.expect("again"),
            0,
            "a second run moves nothing"
        );
    }

    #[test]
    fn quarantined_journals_and_drafts_are_state() {
        for name in [
            "enrollment-draft.quarantine.1714000000000.4242.json",
            "connect-operation.quarantine.1.2.json",
            "connect-project-operation.quarantine.1.2.json",
        ] {
            assert!(is_quarantined_state(name), "{name}");
        }
        for name in [
            "enrollment-draft.quarantine.notes.2.json",
            "enrollment-draft.quarantine.1.json",
            "enrollment-draft.quarantine.1.2.3.json",
            "enrollment-draft.quarantine..2.json",
            "identity.quarantine.1.2.json",
            "connect-operation.json",
        ] {
            assert!(!is_quarantined_state(name), "{name}");
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_never_enrolled_directory_is_left_alone() {
        let root = tempfile::tempdir().expect("tempdir");
        let config_dir = root.path().join(".spice");
        std::fs::create_dir(&config_dir).expect("create config dir");

        assert_eq!(migrate_legacy_state(&config_dir).await.expect("migrate"), 0);
        assert_eq!(
            std::fs::read_dir(&config_dir).expect("list").count(),
            0,
            "no state directory and no lock file"
        );
    }
}
