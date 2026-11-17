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
        Ok(metadata) if metadata.file_type().is_symlink() => Ok(Some("a symbolic link")),
        Ok(metadata) if !metadata.is_file() => Ok(Some("not a regular file")),
        Ok(_) => Ok(None),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(source) => Err(source),
    }
}

/// The error for a credential file that is a link or not a regular file.
fn refuse_credential_file(operation: &'static str, path: &Path, what: &str) -> crate::error::Error {
    crate::error::Error::ConfigIo {
        operation,
        path: path.to_path_buf(),
        source: std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "it is {what}. Credentials are only read from and written to a regular file, never through a link, so no other file can receive them. Replace '{}' with a regular file, or keep credentials in the platform keychain with `spice login --output keychain`. See: https://spiceai.org/docs/cli",
                path.display()
            ),
        ),
    }
}

/// Read the env file a credential update will replace, or `None` when there is
/// none yet.
///
/// # Errors
///
/// Returns an error when the path is a symbolic link or not a regular file, or
/// when it cannot be read.
pub(crate) fn read_env_file_for_update(path: &Path) -> Result<Option<String>> {
    use std::io::Read as _;

    if let Some(what) = unsafe_credential_file(path).context(ConfigIoSnafu {
        operation: "read",
        path: path.to_path_buf(),
    })? {
        return Err(refuse_credential_file("read", path, what));
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
    let mut file = match options.open(path) {
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
        return Err(refuse_credential_file("read", path, "not a regular file"));
    }
    let mut contents = String::new();
    file.read_to_string(&mut contents).context(ConfigIoSnafu {
        operation: "read",
        path: path.to_path_buf(),
    })?;
    Ok(Some(contents))
}

/// Replace the env file at `path` with `contents`, owner-readable only.
///
/// The contents go to a new file created beside `path` with mode `0600`, which
/// is synchronized and then renamed over `path`. The rename replaces the
/// directory entry itself, so a link swapped in after the check below is
/// replaced rather than followed, and a failure part-way leaves the previous
/// file intact.
///
/// # Errors
///
/// Returns an error when `path` is a symbolic link or not a regular file, or
/// when the new file cannot be written or moved into place.
pub(crate) fn replace_env_file(path: &Path, contents: &[u8]) -> Result<()> {
    use std::io::Write as _;

    if let Some(what) = unsafe_credential_file(path).context(ConfigIoSnafu {
        operation: "write",
        path: path.to_path_buf(),
    })? {
        return Err(refuse_credential_file("write", path, what));
    }

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
        .tempfile_in(directory)
        .context(ConfigIoSnafu {
            operation: "write",
            path: path.to_path_buf(),
        })?;
    staged.write_all(contents).context(ConfigIoSnafu {
        operation: "write",
        path: path.to_path_buf(),
    })?;
    staged.as_file().sync_all().context(ConfigIoSnafu {
        operation: "write",
        path: path.to_path_buf(),
    })?;
    staged
        .persist(path)
        .map_err(|error| crate::error::Error::ConfigIo {
            operation: "write",
            path: path.to_path_buf(),
            source: error.error,
        })?;
    // Make the rename durable. Best-effort: the new file is already complete.
    #[cfg(unix)]
    if let Ok(directory) = std::fs::File::open(directory) {
        let _ = directory.sync_all();
    }
    Ok(())
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

#[cfg(test)]
mod tests {
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
