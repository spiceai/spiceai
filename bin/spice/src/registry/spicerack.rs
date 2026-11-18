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

//! Spicerack.org registry for Spicepods.

use super::install_dir::{Entry, InstallDir};
use super::{Error, Result, ZipExtractionSnafu};
use snafu::ResultExt;
use std::collections::HashMap;
use std::io::Read;
use std::path::{Path, PathBuf};

/// Base URL for spicerack API.
fn get_spicerack_base_url() -> String {
    if let Ok(url) = std::env::var("SPICERACK_BASE_URL") {
        return url;
    }

    let version = env!("CARGO_PKG_VERSION");
    if version.ends_with("-dev") {
        "https://dev-api.spicerack.org/v1".to_string()
    } else {
        "https://api.spicerack.org/v1".to_string()
    }
}

/// How much of a registry error body to quote back to the user.
const MAX_QUOTED_MESSAGE_LEN: usize = 512;

/// Keys an error body may carry its human-readable text under.
const MESSAGE_KEYS: [&str; 3] = ["Message", "message", "error"];

/// Signatures a zip record can open with: local file header, end-of-central-directory (an empty
/// archive), and the spanned/split marker.
const ZIP_SIGNATURES: [&[u8]; 3] = [b"PK\x03\x04", b"PK\x05\x06", b"PK\x07\x08"];

/// Whether a body the zip reader rejected still claims to be an archive.
///
/// A download cut short keeps its signature, so it is a damaged archive rather than a message the
/// registry meant for the user - and reporting it as one would quote the raw bytes back as if the
/// server had said them.
fn claims_to_be_an_archive(body: &[u8]) -> bool {
    ZIP_SIGNATURES
        .iter()
        .any(|signature| body.starts_with(signature))
}

/// Pull the human-readable message out of a registry error body.
///
/// Spicerack reports errors as `{"Message": "...", "Code": 0, "Type": "error"}`; anything else
/// falls back to the raw body so the server's own words always reach the user.
fn registry_error_message(body: &str) -> String {
    let parsed = serde_json::from_str::<serde_json::Value>(body).unwrap_or_default();
    let message = MESSAGE_KEYS
        .iter()
        .find_map(|key| parsed.get(key).and_then(serde_json::Value::as_str))
        .unwrap_or(body)
        .trim();

    if message.is_empty() {
        return "(the registry sent an empty body)".to_string();
    }

    // Keep the message on one line, and bound it so a stray HTML error page can't flood the
    // terminal.
    let mut quoted: String = message
        .chars()
        .take(MAX_QUOTED_MESSAGE_LEN)
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect();
    if message.chars().nth(MAX_QUOTED_MESSAGE_LEN).is_some() {
        quoted.push('…');
    }
    quoted
}

/// Registry that fetches Spicepods from spicerack.org.
pub struct SpicerackRegistry;

impl SpicerackRegistry {
    pub async fn get_pod(
        &self,
        pod_full_path: &str,
        pods_dir: &Path,
        headers: &HashMap<String, String>,
        http_client: &reqwest::Client,
    ) -> Result<std::path::PathBuf> {
        self.get_pod_from(
            &get_spicerack_base_url(),
            pod_full_path,
            pods_dir,
            headers,
            http_client,
        )
        .await
    }

    async fn get_pod_from(
        &self,
        base_url: &str,
        pod_full_path: &str,
        pods_dir: &Path,
        headers: &HashMap<String, String>,
        http_client: &reqwest::Client,
    ) -> Result<std::path::PathBuf> {
        // Parse pod path and optional version (e.g., "spiceai/quickstart@v1.0")
        let (pod_path, pod_version) = if let Some(idx) = pod_full_path.find('@') {
            let (path, version) = pod_full_path.split_at(idx);
            (path, Some(&version[1..])) // Skip the '@'
        } else {
            (pod_full_path, None)
        };

        // Build URL
        let url = match pod_version {
            Some(version) => format!("{base_url}/spicepods/{pod_path}/{version}"),
            None => format!("{base_url}/spicepods/{pod_path}"),
        };

        // Make request
        let mut request = http_client.get(&url).header("Accept", "application/zip");

        for (key, value) in headers {
            request = request.header(key, value);
        }

        let response = request.send().await.map_err(|e| Error::FetchFailed {
            pod: pod_full_path.to_string(),
            message: e.to_string(),
        })?;

        // Check response status
        let status = response.status();
        if status.as_u16() == 404 {
            return Err(Error::NotFound {
                path: pod_full_path.to_string(),
            });
        }

        if !status.is_success() {
            return Err(Error::FetchFailed {
                pod: pod_full_path.to_string(),
                message: format!("HTTP {status}"),
            });
        }

        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or("unset")
            .to_string();

        let limits = ArchiveLimits::SPICEPOD;
        let downloaded = download_archive(response, pod_full_path, &limits).await?;

        // Unzipping is blocking file I/O and CPU work, so it runs off the async
        // worker threads.
        let pod = pod_full_path.to_string();
        let pod_path = pod_path.to_string();
        let pods_dir = pods_dir.to_path_buf();
        tokio::task::spawn_blocking(move || {
            install_archive(
                &downloaded,
                &RegistryResponse {
                    pod: &pod,
                    status: status.as_u16(),
                    content_type: &content_type,
                },
                &pods_dir,
                &pod_path,
                &limits,
            )
        })
        .await
        .map_err(|source| Error::Io {
            operation: "extract",
            path: pod_full_path.to_string(),
            source: std::io::Error::other(source),
        })?
    }
}

const MIB: u64 = 1024 * 1024;

/// How large a Spicepod archive from the registry may be.
///
/// A Spicepod package is a manifest and a few companion files, so every limit
/// is far above any real package. They are finite so that a registry response
/// cannot exhaust memory or fill the disk, however it is built.
#[derive(Clone, Copy, Debug)]
struct ArchiveLimits {
    /// Largest archive downloaded, whatever its `Content-Length` claims.
    archive_bytes: u64,
    /// Most entries one archive may hold.
    entries: usize,
    /// Largest total the entries may expand to. A few MiB of deflated zeros
    /// would otherwise expand to many GiB.
    expanded_bytes: u64,
}

impl ArchiveLimits {
    const SPICEPOD: Self = Self {
        archive_bytes: 256 * MIB,
        entries: 10_000,
        expanded_bytes: 1024 * MIB,
    };
}

/// Largest body that is read back to quote a registry error message.
const MAX_ERROR_BODY_BYTES: u64 = MIB;

/// The unit a limit is written in: the largest binary unit it is a whole
/// number of (`1 GiB`, `256 MiB`), so the limit itself never shows a fraction.
fn size_unit(limit: u64) -> (u64, &'static str) {
    const UNITS: [(u64, &str); 3] = [(1024 * MIB, "GiB"), (MIB, "MiB"), (1024, "KiB")];
    UNITS
        .into_iter()
        .find(|(size, _)| limit >= *size && limit.is_multiple_of(*size))
        .unwrap_or((1, "bytes"))
}

/// A limit as a user reads it: `1 GiB`, `256 MiB`, `2,560 bytes`.
fn describe_limit(limit: u64) -> String {
    let (size, unit) = size_unit(limit);
    format!("{} {unit}", with_thousands(u128::from(limit / size)))
}

/// A measured size in the unit of the limit it is compared with, to one
/// decimal place, so both sides of the sentence read alike: `3.0 GiB` against
/// `1 GiB`.
fn describe_size(bytes: u128, limit: u64) -> String {
    let (size, unit) = size_unit(limit);
    if size == 1 {
        return format!("{} {unit}", with_thousands(bytes));
    }
    let size = u128::from(size);
    let tenths = bytes.saturating_mul(10).saturating_add(size / 2) / size;
    format!("{}.{} {unit}", with_thousands(tenths / 10), tenths % 10)
}

/// `10000` as `10,000`.
fn with_thousands(value: u128) -> String {
    let digits = value.to_string();
    let mut grouped = String::with_capacity(digits.len() + digits.len() / 3);
    for (index, digit) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index).is_multiple_of(3) {
            grouped.push(',');
        }
        grouped.push(digit);
    }
    grouped
}

/// What the registry answered, for the errors an archive can produce.
struct RegistryResponse<'a> {
    pod: &'a str,
    status: u16,
    content_type: &'a str,
}

/// A response body saved to a temporary file.
struct DownloadedArchive {
    file: tempfile::NamedTempFile,
    len: u64,
}

fn archive_limit(pod: &str, reason: String) -> Error {
    Error::ArchiveLimitExceeded {
        pod: pod.to_string(),
        reason,
    }
}

/// Stream the response body to a temporary file, stopping as soon as it is
/// larger than the archive limit.
async fn download_archive(
    mut response: reqwest::Response,
    pod: &str,
    limits: &ArchiveLimits,
) -> Result<DownloadedArchive> {
    use tokio::io::AsyncWriteExt as _;

    if let Some(len) = response.content_length()
        && len > limits.archive_bytes
    {
        return Err(archive_limit(
            pod,
            format!(
                "is {}, more than the {} a Spicepod package may be",
                describe_size(u128::from(len), limits.archive_bytes),
                describe_limit(limits.archive_bytes)
            ),
        ));
    }

    let file = tempfile::NamedTempFile::new().map_err(|e| Error::Io {
        operation: "create temp file",
        path: "tempfile".to_string(),
        source: e,
    })?;
    let temp_path = file.path().display().to_string();
    let mut writer = tokio::fs::File::from_std(file.reopen().map_err(|e| Error::Io {
        operation: "open temp file",
        path: temp_path.clone(),
        source: e,
    })?);

    let mut len: u64 = 0;
    while let Some(chunk) = response.chunk().await.map_err(|e| Error::FetchFailed {
        pod: pod.to_string(),
        message: e.to_string(),
    })? {
        len = len.saturating_add(u64::try_from(chunk.len()).unwrap_or(u64::MAX));
        if len > limits.archive_bytes {
            // The rest is never read, so only the lower bound is known.
            return Err(archive_limit(
                pod,
                format!(
                    "is larger than the {} a Spicepod package may be",
                    describe_limit(limits.archive_bytes)
                ),
            ));
        }
        writer.write_all(&chunk).await.map_err(|e| Error::Io {
            operation: "write temp file",
            path: temp_path.clone(),
            source: e,
        })?;
    }
    writer.flush().await.map_err(|e| Error::Io {
        operation: "write temp file",
        path: temp_path,
        source: e,
    })?;

    Ok(DownloadedArchive { file, len })
}

/// Extract a downloaded archive and install it as `pods_dir/pod_path`.
///
/// The archive is checked and extracted into a new directory beside the
/// destination first, and only moved into place once every entry has been
/// written. An archive that breaks a limit or names an unsafe path therefore
/// installs nothing and leaves an existing install as it was.
fn install_archive(
    downloaded: &DownloadedArchive,
    response: &RegistryResponse<'_>,
    pods_dir: &Path,
    pod_path: &str,
    limits: &ArchiveLimits,
) -> Result<std::path::PathBuf> {
    let archive_path = downloaded.file.path();
    let file = std::fs::File::open(archive_path).map_err(|e| Error::Io {
        operation: "open temp file",
        path: archive_path.display().to_string(),
        source: e,
    })?;

    let mut archive = match zip::ZipArchive::new(file) {
        Ok(archive) => archive,
        Err(source) => return Err(unreadable_archive(downloaded, response, source)),
    };
    check_declared_size(&archive, response.pod, limits)?;

    let pods_dir_existed = std::fs::symlink_metadata(pods_dir).is_ok();
    let installed = open_pods_dir(pods_dir, response.pod).and_then(|pods| {
        let staged = stage_archive(&mut archive, &pods, response.pod, limits)?;
        promote(staged, &pods, pod_path, response.pod)
    });
    if installed.is_err() && !pods_dir_existed {
        // Only removes the directory if nothing else landed in it.
        let _ = std::fs::remove_dir(pods_dir);
    }
    installed
}

/// The error for an install path that is a symbolic link, naming it relative
/// to the project directory.
fn linked_install_path(pod: &str, pods: &InstallDir, path: &Path) -> Error {
    let project = pods.path().parent().unwrap_or(pods.path());
    Error::LinkedInstallPath {
        pod: pod.to_string(),
        path: path
            .strip_prefix(project)
            .unwrap_or(path)
            .display()
            .to_string(),
    }
}

/// Open `pods_dir`, creating it when it is missing. It is reached from the
/// project directory without following a link: one at `spicepods` would put
/// every package wherever it points.
fn open_pods_dir(pods_dir: &Path, pod: &str) -> Result<InstallDir> {
    let io_error = |source| Error::Io {
        operation: "create directory",
        path: pods_dir.display().to_string(),
        source,
    };
    let (Some(project), Some(name)) = (pods_dir.parent(), pods_dir.file_name()) else {
        return Err(io_error(std::io::Error::from(
            std::io::ErrorKind::InvalidInput,
        )));
    };
    std::fs::create_dir_all(project).map_err(io_error)?;
    let project = InstallDir::open(project).map_err(io_error)?;
    child_dir(&project, name, false).map_err(|failure| match failure {
        ChildDirFailure::Link => Error::LinkedInstallPath {
            pod: pod.to_string(),
            path: Path::new(name).display().to_string(),
        },
        ChildDirFailure::Io(source) => io_error(source),
    })
}

/// Why [`child_dir`] could not open a directory.
enum ChildDirFailure {
    /// The name is a symbolic link.
    Link,
    Io(std::io::Error),
}

/// Open the directory `name` in `parent`, creating it when it is missing.
fn child_dir(
    parent: &InstallDir,
    name: &std::ffi::OsStr,
    private: bool,
) -> std::result::Result<InstallDir, ChildDirFailure> {
    // A second look covers a directory created by someone else between the
    // first look and the create.
    for _ in 0..2 {
        match parent.entry(name).map_err(ChildDirFailure::Io)? {
            Entry::Directory(dir) => return Ok(dir),
            Entry::Link => return Err(ChildDirFailure::Link),
            Entry::Other => {
                return Err(ChildDirFailure::Io(std::io::Error::from(
                    std::io::ErrorKind::NotADirectory,
                )));
            }
            Entry::Missing => {
                parent
                    .create_dir(name, private)
                    .map_err(ChildDirFailure::Io)?;
            }
        }
    }
    Err(ChildDirFailure::Io(std::io::Error::from(
        std::io::ErrorKind::NotFound,
    )))
}

/// The normal components of `path`, or `None` when it has any other kind
/// (`..`, a root, a prefix).
fn normal_components(path: &Path) -> Option<Vec<&std::ffi::OsStr>> {
    path.components()
        .filter(|component| !matches!(component, std::path::Component::CurDir))
        .map(|component| match component {
            std::path::Component::Normal(name) => Some(name),
            _ => None,
        })
        .collect()
}

/// The error for a body the zip reader rejected.
///
/// The registry can answer a success status with an error body - a JSON payload under
/// `Content-Type: application/zip`. Reporting that as a corrupt archive throws the
/// server's own message away, so when the body is text the registry meant for the user,
/// quote it back instead. A body still carrying a zip signature is a damaged archive, not
/// a message, even when its bytes happen to be valid UTF-8. The zip reader decides what is
/// an archive, so a body it accepts is never diverted here.
fn unreadable_archive(
    downloaded: &DownloadedArchive,
    response: &RegistryResponse<'_>,
    source: zip::result::ZipError,
) -> Error {
    if downloaded.len <= MAX_ERROR_BODY_BYTES
        && let Ok(bytes) = std::fs::read(downloaded.file.path())
        && let Ok(text) = std::str::from_utf8(&bytes)
        && !claims_to_be_an_archive(&bytes)
    {
        return Error::NotAnArchive {
            pod: response.pod.to_string(),
            status: response.status,
            content_type: response.content_type.to_string(),
            body_len: bytes.len(),
            message: registry_error_message(text),
        };
    }
    Error::ZipExtraction { source }
}

/// Refuse an archive whose central directory already declares more entries or
/// expanded bytes than a Spicepod package may hold, before anything is written.
///
/// Declared sizes can lie, so [`stage_archive`] still counts every byte it
/// writes; this only turns away an honest oversized archive early.
fn check_declared_size<R: Read + std::io::Seek>(
    archive: &zip::ZipArchive<R>,
    pod: &str,
    limits: &ArchiveLimits,
) -> Result<()> {
    if archive.len() > limits.entries {
        return Err(archive_limit(
            pod,
            format!(
                "holds {} entries, more than the {} a Spicepod package may hold",
                with_thousands(u128::try_from(archive.len()).unwrap_or(u128::MAX)),
                with_thousands(u128::try_from(limits.entries).unwrap_or(u128::MAX))
            ),
        ));
    }
    if let Some(declared) = archive.decompressed_size()
        && declared > u128::from(limits.expanded_bytes)
    {
        return Err(archive_limit(
            pod,
            format!(
                "expands to {}, more than the {} a Spicepod package may expand to",
                describe_size(declared, limits.expanded_bytes),
                describe_limit(limits.expanded_bytes)
            ),
        ));
    }
    Ok(())
}

/// The error for archive entry `index`, whose name cannot be written inside
/// the Spicepod directory. The name is quoted on one line and bounded.
fn unsafe_archive_entry(
    pod: &str,
    index: usize,
    name: zip::result::ZipResult<std::borrow::Cow<'_, str>>,
) -> Error {
    Error::UnsafeArchiveEntry {
        pod: pod.to_string(),
        entry: name.map_or_else(
            |_| format!("#{index}"),
            |name| {
                name.chars()
                    .take(256)
                    .map(|c| if c.is_control() { ' ' } else { c })
                    .collect()
            },
        ),
    }
}

/// A directory inside `pods_dir` that a package is extracted into, removed
/// when dropped unless it was moved into place.
struct Staging {
    /// The directory it was created in, which it is removed from.
    pods: InstallDir,
    dir: InstallDir,
    name: std::ffi::OsString,
    moved: bool,
}

impl Staging {
    /// Create a new owner-only directory in `pods`.
    fn create(pods: &InstallDir) -> Result<Self> {
        let mut last_error = None;
        for _ in 0..8 {
            let name =
                std::ffi::OsString::from(format!(".spice-add-{}", uuid::Uuid::new_v4().simple()));
            match pods.create_dir(&name, true) {
                Ok(true) => {}
                Ok(false) => continue,
                Err(source) => {
                    last_error = Some(source);
                    continue;
                }
            }
            let parent = pods.try_clone().map_err(|source| Error::Io {
                operation: "create directory",
                path: pods.child_path(&name).display().to_string(),
                source,
            })?;
            return match pods.entry(&name) {
                Ok(Entry::Directory(dir)) => Ok(Self {
                    pods: parent,
                    dir,
                    name,
                    moved: false,
                }),
                Ok(_) => Err(Error::Io {
                    operation: "create directory",
                    path: pods.child_path(&name).display().to_string(),
                    source: std::io::Error::from(std::io::ErrorKind::NotADirectory),
                }),
                Err(source) => Err(Error::Io {
                    operation: "create directory",
                    path: pods.child_path(&name).display().to_string(),
                    source,
                }),
            };
        }
        Err(Error::Io {
            operation: "create directory",
            path: pods.path().display().to_string(),
            source: last_error
                .unwrap_or_else(|| std::io::Error::from(std::io::ErrorKind::AlreadyExists)),
        })
    }
}

impl Drop for Staging {
    fn drop(&mut self) {
        if !self.moved {
            // Relative to the directory it was created in, so a `spicepods`
            // path swapped for a link since cannot point the removal elsewhere.
            let _ = self.pods.remove_tree(&self.name);
        }
    }
}

/// Extract every entry into a new directory inside `pods`, streaming each one
/// to disk and stopping once the archive has expanded past its limit.
///
/// The directory is new, owner-only, and only this function writes to it, and
/// every entry is created relative to it, so no entry can be redirected by a
/// link.
fn stage_archive<R: Read + std::io::Seek>(
    archive: &mut zip::ZipArchive<R>,
    pods: &InstallDir,
    pod: &str,
    limits: &ArchiveLimits,
) -> Result<Staging> {
    let staging = Staging::create(pods)?;

    let mut remaining = limits.expanded_bytes;
    for i in 0..archive.len() {
        let mut entry = archive.by_index(i).context(ZipExtractionSnafu)?;

        // `enclosed_name` refuses a name that escapes the directory, and one
        // that cannot be a file name here at all (a NUL, a drive prefix, a
        // name this platform reserves). Either way the entry cannot be written
        // where the archive says, so the package is not installed in part.
        let Some(file_name) = entry.enclosed_name() else {
            return Err(unsafe_archive_entry(pod, i, entry.name()));
        };
        let Some(components) = normal_components(&file_name) else {
            return Err(unsafe_archive_entry(pod, i, entry.name()));
        };
        let dest_path = staging.dir.path().join(&file_name);

        let (parents, leaf) = if entry.is_dir() {
            (components.as_slice(), None)
        } else {
            match components.split_last() {
                Some((leaf, parents)) => (parents, Some(*leaf)),
                None => return Err(unsafe_archive_entry(pod, i, entry.name())),
            }
        };
        let mut dir = staging.dir.try_clone().map_err(|e| Error::Io {
            operation: "create directory",
            path: dest_path.display().to_string(),
            source: e,
        })?;
        for name in parents {
            dir = child_dir(&dir, name, false).map_err(|failure| Error::Io {
                operation: "create directory",
                path: dest_path.display().to_string(),
                source: match failure {
                    ChildDirFailure::Link => {
                        std::io::Error::from(std::io::ErrorKind::NotADirectory)
                    }
                    ChildDirFailure::Io(source) => source,
                },
            })?;
        }
        let Some(leaf) = leaf else {
            continue;
        };
        let mut outfile = dir.create_file(leaf).map_err(|e| Error::Io {
            operation: "create file",
            path: dest_path.display().to_string(),
            source: e,
        })?;

        // One byte past the budget is enough to know it was exceeded, and is
        // as far as the entry is ever expanded.
        let written = std::io::copy(
            &mut (&mut entry).take(remaining.saturating_add(1)),
            &mut outfile,
        )
        .map_err(|e| Error::Io {
            operation: "extract",
            path: file_name.display().to_string(),
            source: e,
        })?;
        if written > remaining {
            return Err(archive_limit(
                pod,
                format!(
                    "expands to more than the {} a Spicepod package may expand to",
                    describe_limit(limits.expanded_bytes)
                ),
            ));
        }
        remaining -= written;
    }

    Ok(staging)
}

/// Move a fully extracted Spicepod to `pod_path` inside `pods`, returning
/// where it was installed.
///
/// A first install renames the whole directory into place. Re-installing over
/// an existing directory keeps today's behavior of writing the package's files
/// over it, now only after the package has been extracted in full.
///
/// Nothing is written through a symbolic link. Every directory from `pods` to
/// the install, and every directory inside an existing install that the
/// package writes into, is opened without following a link, and each file is
/// moved relative to the directory it was opened as. All of them are checked
/// before anything is moved, so a link already there installs nothing; one
/// swapped in while the package is being moved fails the install at that
/// directory, and nothing is written through it.
fn promote(mut staged: Staging, pods: &InstallDir, pod_path: &str, pod: &str) -> Result<PathBuf> {
    let dest_path = pods.path().join(pod_path);
    let invalid = || Error::Io {
        operation: "install",
        path: dest_path.display().to_string(),
        source: std::io::Error::from(std::io::ErrorKind::InvalidInput),
    };
    let components = normal_components(Path::new(pod_path)).ok_or_else(invalid)?;
    let (leaf, parents) = components.split_last().ok_or_else(invalid)?;

    let mut parent = pods.try_clone().map_err(|e| Error::Io {
        operation: "create directory",
        path: pods.path().display().to_string(),
        source: e,
    })?;
    for name in parents {
        parent = child_dir(&parent, name, false).map_err(|failure| match failure {
            ChildDirFailure::Link => linked_install_path(pod, pods, &parent.child_path(name)),
            ChildDirFailure::Io(source) => Error::Io {
                operation: "create directory",
                path: parent.child_path(name).display().to_string(),
                source,
            },
        })?;
    }

    let inspect_error = |source| Error::Io {
        operation: "inspect",
        path: dest_path.display().to_string(),
        source,
    };
    match parent.entry(leaf).map_err(inspect_error)? {
        Entry::Missing => {
            pods.rename(&staged.name, &parent, leaf)
                .map_err(|e| Error::Io {
                    operation: "install",
                    path: dest_path.display().to_string(),
                    source: e,
                })?;
            staged.moved = true;
        }
        Entry::Link => return Err(linked_install_path(pod, pods, &parent.child_path(leaf))),
        Entry::Other => {
            return Err(Error::Io {
                operation: "install",
                path: dest_path.display().to_string(),
                source: std::io::Error::from(std::io::ErrorKind::NotADirectory),
            });
        }
        Entry::Directory(dest) => {
            if let Some(path) = first_linked_directory(&staged.dir, &dest)? {
                return Err(linked_install_path(pod, pods, &path));
            }
            move_entries(&staged.dir, &dest)?;
        }
    }
    Ok(dest_path)
}

/// The first directory under `to` that a move of `from`'s tree would write
/// into and that is a symbolic link, if any.
fn first_linked_directory(from: &InstallDir, to: &InstallDir) -> Result<Option<PathBuf>> {
    for (name, is_dir) in list(from)? {
        if !is_dir {
            continue;
        }
        match to.entry(&name).map_err(|e| Error::Io {
            operation: "inspect",
            path: to.child_path(&name).display().to_string(),
            source: e,
        })? {
            Entry::Link => return Ok(Some(to.child_path(&name))),
            Entry::Directory(target) => {
                let source = staged_dir(from, &name)?;
                if let Some(linked) = first_linked_directory(&source, &target)? {
                    return Ok(Some(linked));
                }
            }
            // Nothing below a missing directory exists yet, and a file where
            // the package has a directory fails the move itself.
            Entry::Missing | Entry::Other => {}
        }
    }
    Ok(None)
}

/// Move every file under `from` to the same relative path under `to`.
fn move_entries(from: &InstallDir, to: &InstallDir) -> Result<()> {
    for (name, is_dir) in list(from)? {
        let target_path = to.child_path(&name);
        if is_dir {
            let target = child_dir(to, &name, false).map_err(|failure| Error::Io {
                operation: "install",
                path: target_path.display().to_string(),
                source: match failure {
                    // Checked before anything was moved, so a link here was
                    // swapped in since; nothing is written through it.
                    ChildDirFailure::Link => {
                        std::io::Error::other("it became a symbolic link during the install")
                    }
                    ChildDirFailure::Io(source) => source,
                },
            })?;
            move_entries(&staged_dir(from, &name)?, &target)?;
        } else {
            from.rename(&name, to, &name).map_err(|e| Error::Io {
                operation: "install",
                path: target_path.display().to_string(),
                source: e,
            })?;
        }
    }
    Ok(())
}

/// The names in a directory of the extracted package.
fn list(dir: &InstallDir) -> Result<Vec<(std::ffi::OsString, bool)>> {
    dir.list().map_err(|e| Error::Io {
        operation: "read directory",
        path: dir.path().display().to_string(),
        source: e,
    })
}

/// The subdirectory `name` of the extracted package.
fn staged_dir(dir: &InstallDir, name: &std::ffi::OsStr) -> Result<InstallDir> {
    match dir.entry(name) {
        Ok(Entry::Directory(staged)) => Ok(staged),
        Ok(_) => Err(Error::Io {
            operation: "read directory",
            path: dir.child_path(name).display().to_string(),
            source: std::io::Error::from(std::io::ErrorKind::NotADirectory),
        }),
        Err(source) => Err(Error::Io {
            operation: "read directory",
            path: dir.child_path(name).display().to_string(),
            source,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Cursor, Write};
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// The exact 152-byte body `api.spicerack.org` answers `GET /v1/spicepods/spiceai/quickstart`
    /// with, under `HTTP 200` and `Content-Type: application/zip`.
    const REGISTRY_ERROR_BODY: &str = "{\"Message\":\"invalid path \\\"spiceaiquickstartv0.1.08bb188f7b4106571cdec9b33f44963b04f928f7f\\\": selected encoding not supported\",\"Code\":0,\"Type\":\"error\"}\n";

    /// The manifest [`zip_with_manifest`] packs.
    const MANIFEST: &str = "version: v1\nkind: Spicepod\nname: quickstart\n";

    /// Build a Spicepod archive holding a single manifest.
    fn zip_with_manifest() -> Vec<u8> {
        let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
        let options = zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Stored);
        writer
            .start_file("spicepod.yaml", options)
            .expect("start manifest entry");
        writer
            .write_all(MANIFEST.as_bytes())
            .expect("write manifest entry");
        writer.finish().expect("finish archive").into_inner()
    }

    /// Fetch `spiceai/quickstart` from a registry that answers `response`.
    ///
    /// The returned `TempDir` is the pods dir, and has to outlive the assertions that read it.
    async fn fetch_pod_from(
        response: ResponseTemplate,
    ) -> (Result<std::path::PathBuf, Error>, tempfile::TempDir) {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/spicepods/spiceai/quickstart"))
            .respond_with(response)
            .mount(&server)
            .await;

        let pods_dir = tempfile::tempdir().expect("create pods dir");
        let result = SpicerackRegistry
            .get_pod_from(
                &format!("{}/v1", server.uri()),
                "spiceai/quickstart",
                pods_dir.path(),
                &HashMap::new(),
                &reqwest::Client::new(),
            )
            .await;
        (result, pods_dir)
    }

    /// Fetch a body the registry serves as an archive, whatever it actually contains.
    async fn fetch_pod_serving(
        body: impl Into<Vec<u8>>,
    ) -> (Result<std::path::PathBuf, Error>, tempfile::TempDir) {
        fetch_pod_from(ResponseTemplate::new(200).set_body_raw(body.into(), "application/zip"))
            .await
    }

    #[test]
    fn registry_error_message_reads_the_json_message_field() {
        assert_eq!(
            registry_error_message(REGISTRY_ERROR_BODY),
            "invalid path \"spiceaiquickstartv0.1.08bb188f7b4106571cdec9b33f44963b04f928f7f\": selected encoding not supported"
        );
        assert_eq!(
            registry_error_message(r#"{"message":"lowercase"}"#),
            "lowercase"
        );
        assert_eq!(
            registry_error_message(r#"{"error":"other key"}"#),
            "other key"
        );
    }

    #[test]
    fn registry_error_message_falls_back_to_the_raw_body() {
        assert_eq!(
            registry_error_message("upstream timed out"),
            "upstream timed out"
        );
        assert_eq!(
            registry_error_message(r#"{"Code":0}"#),
            r#"{"Code":0}"#,
            "a JSON body with no message key"
        );
        assert_eq!(
            registry_error_message("   \n  "),
            "(the registry sent an empty body)"
        );
        assert_eq!(
            registry_error_message(""),
            "(the registry sent an empty body)"
        );
    }

    #[test]
    fn registry_error_message_stays_on_one_line_and_bounded() {
        assert_eq!(
            registry_error_message("first line\nsecond\tline"),
            "first line second line"
        );

        let long = registry_error_message(&"x".repeat(MAX_QUOTED_MESSAGE_LEN + 10));
        assert_eq!(long.chars().count(), MAX_QUOTED_MESSAGE_LEN + 1);
        assert!(long.ends_with('\u{2026}'));
    }

    /// Regression test for #12116: the registry's own message has to survive, and the CLI must
    /// not blame the archive for a server-side error.
    #[tokio::test]
    async fn json_error_under_a_success_status_reports_the_registry_message() {
        let (result, pods_dir) = fetch_pod_serving(REGISTRY_ERROR_BODY).await;

        let message = result
            .expect_err("a JSON error body is not a Spicepod archive")
            .to_string();
        assert!(
            message.contains("selected encoding not supported"),
            "the registry's message must reach the user: {message}"
        );
        assert!(
            message.contains("content-type: application/zip")
                && message.contains(&format!("{}-byte", REGISTRY_ERROR_BODY.len())),
            "the mismatch between the declared type and the body must be reported: {message}"
        );
        assert!(
            !message.contains("EOCD") && !message.contains("extract"),
            "a server-side error must not be reported as a corrupt archive: {message}"
        );
        assert_eq!(
            std::fs::read_dir(pods_dir.path())
                .expect("read pods dir")
                .count(),
            0,
            "nothing should be written for a failed fetch"
        );
    }

    #[tokio::test]
    async fn an_empty_body_under_a_success_status_is_reported_as_such() {
        let (result, _pods_dir) = fetch_pod_serving(Vec::new()).await;

        let message = result
            .expect_err("an empty body is not a Spicepod archive")
            .to_string();
        assert!(
            message.contains("0-byte") && message.contains("(the registry sent an empty body)"),
            "{message}"
        );
    }

    /// A body the zip reader cannot make sense of and that is not text stays a corrupt archive.
    #[tokio::test]
    async fn a_corrupt_binary_body_is_still_reported_as_a_bad_archive() {
        let (result, pods_dir) =
            fetch_pod_serving(b"PK\x03\x04\xff\xfe\x00 truncated".to_vec()).await;

        let err = result.expect_err("a truncated archive cannot be extracted");
        assert!(
            matches!(err, Error::ZipExtraction { .. }),
            "{err} should stay a ZipExtraction"
        );
        assert_eq!(
            std::fs::read_dir(pods_dir.path())
                .expect("read pods dir")
                .count(),
            0,
            "a failed extraction must not leave an empty pod directory behind"
        );
    }

    /// A download cut short can leave bytes that are valid UTF-8, and quoting those back as the
    /// registry's own words would invent a message the server never sent. The zip signature is
    /// what separates a damaged archive from an error body.
    #[tokio::test]
    async fn a_truncated_archive_whose_bytes_are_text_is_still_reported_as_a_bad_archive() {
        let body = b"PK\x03\x04 truncated but perfectly valid UTF-8".to_vec();
        assert!(
            std::str::from_utf8(&body).is_ok(),
            "the body must be valid UTF-8 for this test to exercise the signature check"
        );

        let (result, _pods_dir) = fetch_pod_serving(body).await;

        let err = result.expect_err("a truncated archive cannot be extracted");
        assert!(
            matches!(err, Error::ZipExtraction { .. }),
            "{err} should stay a ZipExtraction"
        );
    }

    /// The zip reader decides what is an archive, so a body it accepts extracts even when it
    /// carries leading data and therefore starts with no zip signature.
    #[tokio::test]
    async fn an_archive_is_extracted_with_or_without_leading_data() {
        let mut with_preamble = b"#!/bin/sh\n# self-extracting preamble\n".to_vec();
        with_preamble.extend_from_slice(&zip_with_manifest());

        for (label, body) in [
            ("a bare archive", zip_with_manifest()),
            ("an archive with leading data", with_preamble),
        ] {
            let (result, pods_dir) = fetch_pod_serving(body).await;

            let dest = result.expect(label);
            assert_eq!(dest, pods_dir.path().join("spiceai/quickstart"), "{label}");
            assert_eq!(
                std::fs::read_to_string(dest.join("spicepod.yaml")).expect("read manifest"),
                MANIFEST,
                "{label}"
            );
        }
    }

    /// Once an install directory has been opened, the package goes into that
    /// directory whatever happens to its path afterwards. A directory above it
    /// swapped for a link does not redirect the move, and a directory inside the
    /// install that becomes a link before the package reaches it fails the
    /// install without anything written through it.
    #[cfg(unix)]
    #[test]
    fn a_directory_swapped_for_a_link_during_an_install_is_never_written_through() {
        const README: &str = "written by the package\n";
        let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
        let options = zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Stored);
        writer
            .start_file("spicepod.yaml", options)
            .expect("start manifest entry");
        writer
            .write_all(MANIFEST.as_bytes())
            .expect("write manifest entry");
        writer
            .start_file("docs/README.md", options)
            .expect("start docs entry");
        writer
            .write_all(README.as_bytes())
            .expect("write docs entry");
        let body = writer.finish().expect("finish archive").into_inner();

        let project = tempfile::tempdir().expect("create the project");
        let outside = tempfile::tempdir().expect("create the outside directory");
        let pods_path = project.path().join("spicepods");
        let install = pods_path.join("evil").join("pod");
        let moved_away = pods_path.join("evil").join("pod.real");
        std::fs::create_dir_all(install.join("docs")).expect("create the earlier install");

        let pods = InstallDir::open(&pods_path).expect("open the pods directory");
        let open_install = || {
            let org = child_dir(&pods, std::ffi::OsStr::new("evil"), false)
                .unwrap_or_else(|_| panic!("open the org directory"));
            child_dir(&org, std::ffi::OsStr::new("pod"), false)
                .unwrap_or_else(|_| panic!("open the install directory"))
        };
        let stage = || {
            let mut archive =
                zip::ZipArchive::new(Cursor::new(body.clone())).expect("open archive");
            stage_archive(&mut archive, &pods, "evil/pod", &ArchiveLimits::SPICEPOD)
                .unwrap_or_else(|err| panic!("stage the package: {err}"))
        };
        let outside_entries = || {
            std::fs::read_dir(outside.path())
                .expect("list the outside directory")
                .count()
        };

        // The install's path leads outside once it has been opened.
        let dest = open_install();
        let staged = stage();
        std::fs::rename(&install, &moved_away).expect("move the install away");
        std::os::unix::fs::symlink(outside.path(), &install).expect("link its path outside");
        move_entries(&staged.dir, &dest)
            .unwrap_or_else(|err| panic!("move into the directory that was opened: {err}"));
        assert_eq!(
            std::fs::read_to_string(moved_away.join("docs").join("README.md"))
                .expect("read the installed file"),
            README
        );
        assert_eq!(outside_entries(), 0, "nothing may be written outside");

        // A directory inside the install becomes a link before it is reached.
        std::fs::remove_file(&install).expect("remove the link");
        std::fs::rename(&moved_away, &install).expect("put the install back");
        let dest = open_install();
        let staged = stage();
        let docs = install.join("docs");
        std::fs::remove_dir_all(&docs).expect("remove the docs directory");
        std::os::unix::fs::symlink(outside.path(), &docs).expect("link it outside");
        let err = move_entries(&staged.dir, &dest)
            .expect_err("a directory that became a link is not written through");
        assert_eq!(
            err.to_string(),
            format!(
                "Failed to install '{}': it became a symbolic link during the install",
                docs.display()
            )
        );
        assert_eq!(outside_entries(), 0, "nothing may be written outside");
    }

    /// A refused or failed install removes its staging directory from the
    /// directory it was created in, even when `spicepods/` has since been
    /// swapped for a link: the cleanup neither follows the link nor removes an
    /// entry of the same name wherever it points.
    #[cfg(unix)]
    #[test]
    fn a_staging_directory_is_removed_where_it_was_created() {
        let project = tempfile::tempdir().expect("create the project");
        let outside = tempfile::tempdir().expect("create the outside directory");
        let pods_path = project.path().join("spicepods");
        let moved_away = project.path().join("spicepods.real");
        std::fs::create_dir(&pods_path).expect("create the pods directory");

        let pods = InstallDir::open(&pods_path).expect("open the pods directory");
        let staged = Staging::create(&pods).expect("create the staging directory");
        let name = staged.name.clone();
        let file = staged
            .dir
            .create_file(std::ffi::OsStr::new("partial.yaml"))
            .expect("write into the staging directory");
        drop(file);

        std::fs::rename(&pods_path, &moved_away).expect("move the pods directory away");
        std::os::unix::fs::symlink(outside.path(), &pods_path).expect("link its path outside");
        let decoy = outside.path().join(&name);
        std::fs::create_dir(&decoy).expect("create an outside entry with the staging name");
        std::fs::write(decoy.join("keep.txt"), "not Spice's\n").expect("write the outside entry");

        drop(staged);

        assert_eq!(
            std::fs::read_to_string(decoy.join("keep.txt")).expect("the outside entry is kept"),
            "not Spice's\n"
        );
        assert!(
            !moved_away.join(&name).exists(),
            "the staging directory must be removed where it was created"
        );
    }

    /// The expansion budget counts the bytes actually written, across entries. A central
    /// directory can leave the total unknown (entries written with data descriptors), so the
    /// count is the bound that always holds - and an archive that crosses it leaves nothing
    /// behind in the pods directory.
    #[test]
    fn extraction_stops_at_the_expansion_budget_across_entries() {
        let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
        let options = zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Stored);
        for name in ["a.bin", "b.bin", "c.bin"] {
            writer.start_file(name, options).expect("start entry");
            writer.write_all(&[b'x'; 1024]).expect("write entry");
        }
        let body = writer.finish().expect("finish archive").into_inner();
        let mut archive = zip::ZipArchive::new(Cursor::new(body)).expect("open archive");

        let pods_dir = tempfile::tempdir().expect("create pods dir");
        let limits = ArchiveLimits {
            archive_bytes: MIB,
            entries: 10,
            expanded_bytes: 2560,
        };
        let pods = InstallDir::open(pods_dir.path()).expect("open pods dir");
        let err = stage_archive(&mut archive, &pods, "test/pod", &limits)
            .map(|_| ())
            .expect_err("3 KiB of entries cannot fit a 2560-byte budget");

        assert_eq!(
            err.to_string(),
            "Failed to install Spicepod 'test/pod' from spicerack.org: the archive expands to more than the 2,560 bytes a Spicepod package may expand to, so nothing was installed. Check the Spicepod name and retry, or install from a local path with `spice add <path>`. See: https://spiceai.org/docs/cli/reference/add"
        );
        assert_eq!(
            std::fs::read_dir(pods_dir.path())
                .expect("read pods dir")
                .count(),
            0,
            "the partly extracted staging directory must be removed"
        );
    }

    /// Both sides of a size comparison read in the limit's unit, to one decimal
    /// place, and counts carry thousands separators.
    #[test]
    fn sizes_are_described_in_the_unit_of_their_limit() {
        assert_eq!(describe_limit(1024 * MIB), "1 GiB");
        assert_eq!(describe_limit(256 * MIB), "256 MiB");
        assert_eq!(describe_limit(2560), "2,560 bytes");
        assert_eq!(describe_size(3_221_225_510, 1024 * MIB), "3.0 GiB");
        assert_eq!(describe_size(1_610_612_736, 1024 * MIB), "1.5 GiB");
        assert_eq!(
            describe_size(10 * 1024 * 1024 * 1024, 256 * MIB),
            "10,240.0 MiB"
        );
        assert_eq!(describe_size(3000, 2560), "3,000 bytes");
        assert_eq!(with_thousands(10_000), "10,000");
        assert_eq!(with_thousands(999), "999");
        assert_eq!(with_thousands(1_000_000), "1,000,000");
    }

    #[tokio::test]
    async fn a_missing_pod_is_still_reported_as_not_found() {
        let (result, _pods_dir) =
            fetch_pod_from(ResponseTemplate::new(404).set_body_string(REGISTRY_ERROR_BODY)).await;

        let err = result.expect_err("a 404 is not a Spicepod archive");
        assert!(
            matches!(err, Error::NotFound { .. }),
            "{err} should stay a NotFound"
        );
    }
}
