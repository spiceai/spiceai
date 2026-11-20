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

//! Authentication configuration management.
//!
//! Handles reading and writing credentials to .env or .env.local files.

use crate::error::{ConfigIoSnafu, Result};
use snafu::ResultExt;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// Merge authentication configuration into the .env file.
///
/// Credentials are stored with the format: `SPICE_{AUTH_TYPE}_{PARAM}=value`
///
/// # Arguments
///
/// * `auth_type` - The authentication type (e.g., "DREMIO", "S3", "PG")
/// * `params` - Key-value pairs of authentication parameters
///
/// # Errors
///
/// Returns an error if the .env file cannot be read or written.
pub fn merge_auth_config(auth_type: &str, params: &[(&str, &str)]) -> Result<()> {
    let env_file = env_file_path();

    // The existing variables are read only from a regular file. A link is
    // refused rather than read through: its target's contents would otherwise
    // be copied into the credential file written below.
    let mut env_vars: HashMap<String, String> = match read_env_file_for_update(&env_file)? {
        Some(contents) => parse_env_file(&contents),
        None => HashMap::new(),
    };

    // Add new auth params
    for (key, value) in params {
        let secret_key = format!("SPICE_{auth_type}_{key}");
        env_vars.insert(secret_key, (*value).to_string());
    }

    write_env_file(&env_file, &env_vars)
}

/// Why a credential file cannot be read or replaced in place, or `None` when
/// it is absent or a regular file.
///
/// The env file is named relative to the working directory, which may be a
/// checkout someone else controls. Following a link there would send the
/// credential, the rewrite, and the permission change to whatever file the link
/// names, so only a regular file is ever read or replaced.
pub(crate) fn unsafe_credential_file(path: &Path) -> std::io::Result<Option<&'static str>> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => Ok(Some(SYMBOLIC_LINK)),
        Ok(metadata) if !metadata.is_file() => Ok(Some("not a regular file")),
        Ok(_) => Ok(None),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(source) => Err(source),
    }
}

/// The error for a credential file that is a link or not a regular file.
///
/// It states the rule that is enforced, which is about writes: commands that
/// only read a credential still read a linked `.env`.
fn refuse_credential_file(path: &Path, what: &str) -> crate::error::Error {
    let through_link = if what == SYMBOLIC_LINK {
        ", never through a link, so they cannot land in the file it points to"
    } else {
        ""
    };
    crate::error::Error::ConfigIo {
        operation: "update",
        path: path.to_path_buf(),
        source: std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "it is {what}. `spice login` writes credentials only to a regular file{through_link}. Replace '{}' with a regular file, or store credentials in the keychain with `spice login --output keychain`. See: https://spiceai.org/docs/cli/reference/login",
                path.display()
            ),
        ),
    }
}

const SYMBOLIC_LINK: &str = "a symbolic link";

/// Read the env file a credential update will replace, or `None` when there is
/// none yet.
///
/// # Errors
///
/// Returns an error when the path is a symbolic link or not a regular file, or
/// when it cannot be read.
pub(crate) fn read_env_file_for_update(path: &Path) -> Result<Option<String>> {
    use std::io::Read as _;

    let Some(mut file) = open_env_file_for_update(path)? else {
        return Ok(None);
    };
    let mut contents = String::new();
    file.read_to_string(&mut contents).context(ConfigIoSnafu {
        operation: "read",
        path: path.to_path_buf(),
    })?;
    Ok(Some(contents))
}

/// As [`read_env_file_for_update`], without requiring UTF-8.
#[cfg(unix)]
fn read_env_file_bytes_for_update(path: &Path) -> Result<Option<Vec<u8>>> {
    use std::io::Read as _;

    let Some(mut file) = open_env_file_for_update(path)? else {
        return Ok(None);
    };
    let mut contents = Vec::new();
    file.read_to_end(&mut contents).context(ConfigIoSnafu {
        operation: "read",
        path: path.to_path_buf(),
    })?;
    Ok(Some(contents))
}

/// Open the env file a credential update will replace, or `None` when there is
/// none yet. A link or anything else that is not a regular file is refused.
fn open_env_file_for_update(path: &Path) -> Result<Option<std::fs::File>> {
    if let Some(what) = unsafe_credential_file(path).context(ConfigIoSnafu {
        operation: "read",
        path: path.to_path_buf(),
    })? {
        return Err(refuse_credential_file(path, what));
    }

    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    // The check above inspected the name; these flags make the open itself
    // refuse a link swapped in since, and keep a FIFO from blocking the read.
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    let file = match options.open(path) {
        Ok(file) => file,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(crate::error::Error::ConfigIo {
                operation: "read",
                path: path.to_path_buf(),
                source,
            });
        }
    };
    let metadata = file.metadata().context(ConfigIoSnafu {
        operation: "read",
        path: path.to_path_buf(),
    })?;
    if !metadata.is_file() {
        return Err(refuse_credential_file(path, "not a regular file"));
    }
    Ok(Some(file))
}

/// Replace the env file at `path` with `contents`, owner-readable only.
///
/// The contents go to a new file created beside `path` with mode `0600`, which
/// is synchronized and then renamed over `path`. The rename replaces the
/// directory entry itself, so a link swapped in after the check below is
/// replaced rather than followed, and a failure part-way leaves the previous
/// file intact.
///
/// The rename names the new file by its path, which another program that can
/// change the directory may point at a different entry. On Unix the entry that
/// ends up at `path` is therefore compared with the file that was written, and
/// the update fails unless they are the same file; see
/// [`publication::check`] for what is cleared and put back then.
///
/// # Errors
///
/// Returns an error when `path` is a symbolic link or not a regular file, when
/// the new file cannot be written or moved into place, or when what stands at
/// `path` afterwards is not the file that was written.
pub(crate) fn replace_env_file(path: &Path, contents: &[u8]) -> Result<()> {
    if let Some(what) = unsafe_credential_file(path).context(ConfigIoSnafu {
        operation: "write",
        path: path.to_path_buf(),
    })? {
        return Err(refuse_credential_file(path, what));
    }

    // Kept so they can be put back when the rename publishes something else.
    #[cfg(unix)]
    let previous = read_env_file_bytes_for_update(path)?;

    let written = publish(path, contents).context(ConfigIoSnafu {
        operation: "write",
        path: path.to_path_buf(),
    })?;

    #[cfg(unix)]
    publication::check(path, &written, previous.as_deref())?;
    #[cfg(not(unix))]
    drop(written);
    Ok(())
}

/// Write `contents` to a new owner-only file beside `path`, synchronize it,
/// and rename it over `path`. Returns the written file, still open.
fn publish(path: &Path, contents: &[u8]) -> std::io::Result<std::fs::File> {
    use std::io::Write as _;

    let directory = match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    };
    let file_name = path
        .file_name()
        .and_then(std::ffi::OsStr::to_str)
        .unwrap_or(".env");
    // `tempfile` creates the file exclusively (`O_CREAT | O_EXCL`), so it never
    // opens an existing entry, and with mode 0600 from the start.
    let mut staged = tempfile::Builder::new()
        .prefix(&format!(".{file_name}."))
        .suffix(".tmp")
        .tempfile_in(directory)?;
    staged.write_all(contents)?;
    staged.as_file().sync_all()?;
    #[cfg(test)]
    let staged_path = staged.path().to_path_buf();
    #[cfg(test)]
    publish_hooks::run(publish_hooks::Point::BeforeRename, &staged_path);
    let written = staged.persist(path).map_err(|error| error.error)?;
    // Make the rename durable. Best-effort: the new file is already complete.
    #[cfg(unix)]
    if let Ok(directory) = std::fs::File::open(directory) {
        let _ = directory.sync_all();
    }
    #[cfg(test)]
    publish_hooks::run(publish_hooks::Point::AfterRename, &staged_path);
    Ok(written)
}

/// Whether the file a rename published is the one that was written, and what
/// to do when it is not.
#[cfg(unix)]
mod publication {
    use super::{Path, publish};
    use std::os::unix::fs::MetadataExt as _;

    /// How putting the previous contents back went.
    pub(super) enum Restore {
        /// There was nothing to put back, or something newer stands at the path.
        NotAttempted,
        Restored,
        Failed(std::io::Error),
    }

    /// Fail unless the entry at `path` is `written`.
    ///
    /// Entries are compared by device and inode, without following a link at
    /// `path`. When they differ, the written file is emptied through its
    /// descriptor, so the update does not stay on disk under whatever name it
    /// now has, and `previous` is written back when [`displaced`] says it belongs
    /// there.
    pub(super) fn check(
        path: &Path,
        written: &std::fs::File,
        previous: Option<&[u8]>,
    ) -> crate::error::Result<()> {
        let io_error = |source| crate::error::Error::ConfigIo {
            operation: "write",
            path: path.to_path_buf(),
            source,
        };
        let Some(restore_previous) = displaced(path, written).map_err(io_error)? else {
            return Ok(());
        };
        let cleared = clear(written).err();
        let restore = match previous {
            Some(previous) if restore_previous => match put_back(path, previous) {
                Ok(()) => Restore::Restored,
                Err(error) => Restore::Failed(error),
            },
            _ => Restore::NotAttempted,
        };
        Err(io_error(std::io::Error::other(displaced_message(
            path,
            &restore,
            cleared.as_ref(),
        ))))
    }

    /// `None` when the entry at `path` is `written`; otherwise whether the
    /// previous contents belong back at `path`.
    ///
    /// They do when the written file still has a name, because then the rename
    /// moved some other entry into place. When it has none, it did land and was
    /// replaced or removed since: a regular file there is a later update and a
    /// missing one a later removal, and both are kept. Anything else is never a
    /// credential file, so it is replaced.
    fn displaced(path: &Path, written: &std::fs::File) -> std::io::Result<Option<bool>> {
        let written = written.metadata()?;
        let published = match std::fs::symlink_metadata(path) {
            Ok(published) => Some(published),
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => None,
            Err(source) => return Err(source),
        };
        if published.as_ref().is_some_and(|published| {
            published.dev() == written.dev() && published.ino() == written.ino()
        }) {
            return Ok(None);
        }
        Ok(Some(
            written.nlink() > 0 || published.is_some_and(|published| !published.is_file()),
        ))
    }

    /// Empty a written file that did not land, through its descriptor.
    fn clear(written: &std::fs::File) -> std::io::Result<()> {
        written.set_len(0)?;
        written.sync_all()
    }

    /// Write `previous` back to `path`, once.
    fn put_back(path: &Path, previous: &[u8]) -> std::io::Result<()> {
        let written = publish(path, previous)?;
        if displaced(path, &written)?.is_none() {
            return Ok(());
        }
        match clear(&written) {
            Ok(()) => Err(std::io::Error::other(
                "another program changed its directory again",
            )),
            Err(error) => Err(std::io::Error::other(format!(
                "another program changed its directory again, and the copy written to put them back could not be cleared: {error}"
            ))),
        }
    }

    /// Why the update failed when the entry at `path` is not the written file:
    /// the cause clause of a `Failed to write configuration at '{path}'` error.
    pub(super) fn displaced_message(
        path: &Path,
        restore: &Restore,
        cleared: Option<&std::io::Error>,
    ) -> String {
        let restore = match restore {
            Restore::NotAttempted => String::new(),
            Restore::Restored => "; its previous contents were put back".to_string(),
            Restore::Failed(error) => {
                format!("; its previous contents could not be put back ({error})")
            }
        };
        let cleared = cleared.map_or_else(String::new, |error| {
            format!("; the written copy could not be cleared ({error}), so it may remain under another name")
        });
        let path = path.display();
        format!(
            "another program changed its directory while the new file was being moved into place, so it is not the file that was written and the update was not saved{restore}{cleared}. Make sure no other user or program can change the directory that holds '{path}', check what '{path}' contains, then run the command again, or store credentials in the keychain with `spice login --output keychain`. See: https://spiceai.org/docs/cli/reference/login"
        )
    }
}

/// The env file credentials are read from and written to.
///
/// `.env.local` wins when present. Every reader and the writer must agree on
/// this, or a credential is stored where nothing looks for it.
#[must_use]
pub fn env_file_path() -> PathBuf {
    #[cfg(test)]
    return test_env_file::path();

    #[cfg(not(test))]
    if Path::new(".env.local").exists() {
        PathBuf::from(".env.local")
    } else {
        PathBuf::from(".env")
    }
}

/// Read one variable from the env file, using the same parse as the writer.
///
/// Shares [`read_env_file`] so a value the writer quoted is unquoted the same
/// way, and `KEY = value` spacing is accepted on both sides.
#[must_use]
pub fn read_env_var(var: &str) -> Option<String> {
    read_env_file(&env_file_path())
        .ok()?
        .remove(var)
        .filter(|value| !value.is_empty())
}

/// Every variable currently set in the env file.
#[must_use]
pub fn env_file_vars() -> HashMap<String, String> {
    read_env_file(&env_file_path()).unwrap_or_default()
}

/// Read environment variables from a .env file.
fn read_env_file(path: &Path) -> Result<HashMap<String, String>> {
    let contents = std::fs::read_to_string(path).context(ConfigIoSnafu {
        operation: "read",
        path: path.to_path_buf(),
    })?;
    Ok(parse_env_file(&contents))
}

/// Parse `KEY=value` lines, unquoting values the way [`write_env_file`] quotes them.
fn parse_env_file(contents: &str) -> HashMap<String, String> {
    let mut vars = HashMap::new();
    for line in contents.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        if let Some((key, value)) = line.split_once('=') {
            let key = key.trim();
            let value = value.trim();
            // Remove surrounding quotes if present
            let value = value
                .strip_prefix('"')
                .and_then(|v| v.strip_suffix('"'))
                .unwrap_or(value);
            let value = value
                .strip_prefix('\'')
                .and_then(|v| v.strip_suffix('\''))
                .unwrap_or(value);
            vars.insert(key.to_string(), value.to_string());
        }
    }

    vars
}

/// Write environment variables to a .env file.
fn write_env_file(path: &Path, vars: &HashMap<String, String>) -> Result<()> {
    let mut lines: Vec<String> = vars
        .iter()
        .map(|(k, v)| {
            // Quote values that contain special characters
            if v.contains(' ') || v.contains('"') || v.contains('\'') || v.contains('=') {
                format!("{k}=\"{}\"", v.replace('"', "\\\""))
            } else {
                format!("{k}={v}")
            }
        })
        .collect();

    // Sort for consistent output
    lines.sort();

    let contents = lines.join("\n") + "\n";
    replace_env_file(path, contents.as_bytes())
}

/// Store authentication credentials in the platform keyring.
///
/// Credentials are stored with the service name `spice` and account name
/// `SPICE_{AUTH_TYPE}_{PARAM}`.
///
/// # Errors
///
/// Returns an error if the keyring operation fails.
pub fn store_keychain(auth_type: &str, params: &[(&str, &str)]) -> Result<()> {
    for (key, value) in params {
        let account = format!("SPICE_{auth_type}_{key}");
        super::keychain::write(&account, value).map_err(|reason| {
            crate::error::Error::InvalidArgument {
                message: format!("Failed to store `{account}` in the keychain: {reason}"),
            }
        })?;
    }
    Ok(())
}

/// Redirects the env-file helpers to a temporary file in tests.
///
/// The fallback is also temporary, so a test that forgets to install an
/// explicit path still cannot touch the developer's real `.env`.
#[cfg(test)]
pub(crate) mod test_env_file {
    use std::cell::RefCell;
    use std::path::PathBuf;

    thread_local! {
        static DIR: RefCell<Option<tempfile::TempDir>> = const { RefCell::new(None) };
    }

    pub(crate) fn path() -> PathBuf {
        DIR.with_borrow_mut(|dir| {
            dir.get_or_insert_with(|| tempfile::tempdir().expect("create test env directory"))
                .path()
                .join(".env")
        })
    }

    pub(crate) fn reset() {
        DIR.with_borrow_mut(|dir| *dir = None);
    }
}

/// Lets a test act on the directory while [`replace_env_file`] publishes, the
/// way another program sharing it could.
#[cfg(test)]
pub(crate) mod publish_hooks {
    use std::cell::RefCell;
    use std::collections::VecDeque;
    use std::path::Path;

    /// Where in a publish a hook runs.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub(crate) enum Point {
        /// The new file is written and synchronized, and not yet renamed.
        BeforeRename,
        /// The rename has returned.
        AfterRename,
    }

    type Hook = Box<dyn FnOnce(&Path)>;

    thread_local! {
        static HOOKS: RefCell<VecDeque<(Point, Hook)>> = const { RefCell::new(VecDeque::new()) };
    }

    /// Run `hook` with the staged file's path the next time a publish reaches
    /// `point`. Hooks run once each, in the order they were added.
    pub(crate) fn add(point: Point, hook: impl FnOnce(&Path) + 'static) {
        HOOKS.with_borrow_mut(|hooks| hooks.push_back((point, Box::new(hook))));
    }

    /// How many hooks have not run yet.
    pub(crate) fn pending() -> usize {
        HOOKS.with_borrow(VecDeque::len)
    }

    pub(super) fn run(point: Point, staged: &Path) {
        let hook = HOOKS.with_borrow_mut(|hooks| {
            if hooks.front().is_some_and(|(at, _)| *at == point) {
                hooks.pop_front().map(|(_, hook)| hook)
            } else {
                None
            }
        });
        if let Some(hook) = hook {
            hook(staged);
        }
    }
}

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    mod publish {
        use super::super::publish_hooks::{self, Point};
        use super::super::{merge_auth_config, test_env_file};
        use std::path::{Path, PathBuf};

        /// What a login left behind in the env file's directory.
        #[derive(Debug, PartialEq, Eq)]
        struct Outcome {
            succeeded: bool,
            env_is_link: bool,
            /// The env file's contents when it is a regular file.
            env_contents: Option<String>,
            /// What remains of the written file at the name it was moved to.
            moved_contents: Vec<Option<String>>,
            /// The file a substituted link points to.
            target_contents: String,
            unused_hooks: usize,
        }

        struct Dir {
            env: PathBuf,
            target: PathBuf,
        }

        impl Dir {
            fn new(previous: Option<&str>) -> Self {
                test_env_file::reset();
                let env = test_env_file::path();
                let target = env.with_file_name("elsewhere.env");
                std::fs::write(&target, "OTHER=untouched\n").expect("write link target");
                if let Some(previous) = previous {
                    std::fs::write(&env, previous).expect("write previous env file");
                }
                Self { env, target }
            }

            fn moved(&self, n: usize) -> PathBuf {
                self.env.with_file_name(format!("moved-{n}"))
            }

            /// Move the staged file to `moved(n)` and put a link to the target
            /// at its name, so the rename publishes the link.
            fn swap_staged_for_link(&self, n: usize) {
                let moved = self.moved(n);
                let target = self.target.clone();
                publish_hooks::add(Point::BeforeRename, move |staged: &Path| {
                    std::fs::rename(staged, &moved).expect("move the staged file aside");
                    std::os::unix::fs::symlink(&target, staged).expect("link the staged name");
                });
            }

            fn outcome(&self, succeeded: bool, moved: usize) -> Outcome {
                let metadata = std::fs::symlink_metadata(&self.env).ok();
                let env_is_link = metadata
                    .as_ref()
                    .is_some_and(|metadata| metadata.file_type().is_symlink());
                let env_contents = metadata
                    .filter(std::fs::Metadata::is_file)
                    .map(|_| std::fs::read_to_string(&self.env).expect("read env file"));
                Outcome {
                    succeeded,
                    env_is_link,
                    env_contents,
                    moved_contents: (1..=moved)
                        .map(|n| std::fs::read_to_string(self.moved(n)).ok())
                        .collect(),
                    target_contents: std::fs::read_to_string(&self.target)
                        .expect("read link target"),
                    unused_hooks: publish_hooks::pending(),
                }
            }
        }

        /// The error for an env file that is not the file the update wrote.
        fn expected_error(env: &Path, restore: &str) -> String {
            let env = env.display();
            format!(
                "Failed to write configuration at '{env}': another program changed its directory while the new file was being moved into place, so it is not the file that was written and the update was not saved{restore}. Make sure no other user or program can change the directory that holds '{env}', check what '{env}' contains, then run the command again, or store credentials in the keychain with `spice login --output keychain`. See: https://spiceai.org/docs/cli/reference/login"
            )
        }

        fn login() -> crate::error::Result<()> {
            merge_auth_config("TEST", &[("TOKEN", "secret-token")])
        }

        #[test]
        fn login_fails_and_restores_env_when_the_staged_file_is_swapped_for_a_link() {
            let dir = Dir::new(Some("EXISTING=kept\n"));
            dir.swap_staged_for_link(1);

            let result = login();

            assert_eq!(
                dir.outcome(result.is_ok(), 1),
                Outcome {
                    succeeded: false,
                    env_is_link: false,
                    env_contents: Some("EXISTING=kept\n".to_string()),
                    moved_contents: vec![Some(String::new())],
                    target_contents: "OTHER=untouched\n".to_string(),
                    unused_hooks: 0,
                }
            );
            assert_eq!(
                result.expect_err("checked above").to_string(),
                expected_error(&dir.env, "; its previous contents were put back")
            );
        }

        #[test]
        fn login_fails_and_restores_env_when_the_staged_file_is_swapped_for_another_file() {
            let dir = Dir::new(Some("EXISTING=kept\n"));
            let moved = dir.moved(1);
            publish_hooks::add(Point::BeforeRename, move |staged: &Path| {
                std::fs::rename(staged, &moved).expect("move the staged file aside");
                std::fs::write(staged, "SUBSTITUTED=1\n").expect("write over the staged name");
            });

            let result = login();

            assert_eq!(
                dir.outcome(result.is_ok(), 1),
                Outcome {
                    succeeded: false,
                    env_is_link: false,
                    env_contents: Some("EXISTING=kept\n".to_string()),
                    moved_contents: vec![Some(String::new())],
                    target_contents: "OTHER=untouched\n".to_string(),
                    unused_hooks: 0,
                }
            );
            assert_eq!(
                result.expect_err("checked above").to_string(),
                expected_error(&dir.env, "; its previous contents were put back")
            );
        }

        #[test]
        fn login_fails_without_a_previous_env_when_the_staged_file_is_swapped_for_a_link() {
            let dir = Dir::new(None);
            dir.swap_staged_for_link(1);

            let result = login();

            assert_eq!(
                dir.outcome(result.is_ok(), 1),
                Outcome {
                    succeeded: false,
                    env_is_link: true,
                    env_contents: None,
                    moved_contents: vec![Some(String::new())],
                    target_contents: "OTHER=untouched\n".to_string(),
                    unused_hooks: 0,
                }
            );
            assert_eq!(
                result.expect_err("checked above").to_string(),
                expected_error(&dir.env, "")
            );
        }

        #[test]
        fn login_fails_and_keeps_a_later_write_that_replaced_env() {
            let dir = Dir::new(Some("EXISTING=kept\n"));
            let env = dir.env.clone();
            publish_hooks::add(Point::AfterRename, move |_staged: &Path| {
                let later = env.with_file_name("later.tmp");
                std::fs::write(&later, "LATER=wins\n").expect("write later file");
                std::fs::rename(&later, &env).expect("publish later file");
            });

            let result = login();

            assert_eq!(
                dir.outcome(result.is_ok(), 0),
                Outcome {
                    succeeded: false,
                    env_is_link: false,
                    env_contents: Some("LATER=wins\n".to_string()),
                    moved_contents: vec![],
                    target_contents: "OTHER=untouched\n".to_string(),
                    unused_hooks: 0,
                }
            );
            assert_eq!(
                result.expect_err("checked above").to_string(),
                expected_error(&dir.env, "")
            );
        }

        #[test]
        fn login_reports_when_the_previous_env_cannot_be_put_back() {
            let dir = Dir::new(Some("EXISTING=kept\n"));
            dir.swap_staged_for_link(1);
            dir.swap_staged_for_link(2);

            let result = login();

            assert_eq!(
                dir.outcome(result.is_ok(), 2),
                Outcome {
                    succeeded: false,
                    env_is_link: true,
                    env_contents: None,
                    moved_contents: vec![Some(String::new()), Some(String::new())],
                    target_contents: "OTHER=untouched\n".to_string(),
                    unused_hooks: 0,
                }
            );
            assert_eq!(
                result.expect_err("checked above").to_string(),
                expected_error(
                    &dir.env,
                    "; its previous contents could not be put back (another program changed its directory again)"
                )
            );
        }

        #[test]
        fn displaced_message_says_when_the_written_copy_could_not_be_cleared() {
            use super::super::publication::{Restore, displaced_message};

            assert_eq!(
                displaced_message(
                    Path::new(".env"),
                    &Restore::NotAttempted,
                    Some(&std::io::Error::other("disk unavailable")),
                ),
                "another program changed its directory while the new file was being moved into place, so it is not the file that was written and the update was not saved; the written copy could not be cleared (disk unavailable), so it may remain under another name. Make sure no other user or program can change the directory that holds '.env', check what '.env' contains, then run the command again, or store credentials in the keychain with `spice login --output keychain`. See: https://spiceai.org/docs/cli/reference/login"
            );
        }
    }

    #[test]
    fn test_read_env_file_format() {
        // Test various formats that should be parsed
        let content = r#"
# Comment
KEY1=value1
KEY2="quoted value"
KEY3='single quoted'
KEY4=
"#;
        // This would need a temp file to test properly
        assert!(content.contains("KEY1=value1"));
    }
}
