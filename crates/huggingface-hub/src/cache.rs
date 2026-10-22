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

//! The Hub's file cache, in the layout `huggingface_hub` writes and reads, so a model fetched
//! with `hf download` is reused and the `hf` CLI reads what Spice downloads:
//!
//! ```text
//! <cache>/
//!   models--<owner>--<name>/            (datasets--…, spaces--…)
//!     refs/<revision>                   the commit a branch or tag last resolved to
//!     blobs/<etag>                      a file's bytes, named by its git object id or SHA-256
//!     snapshots/<commit>/<path>         a relative symlink to the file's blob
//!   .locks/models--<owner>--<name>/<etag>.lock
//! ```
//!
//! A blob is written to `<etag>.incomplete` and renamed into place only once its checksum
//! matches, so a blob that exists is complete. Writers of one blob, in this process or another
//! (`hf download` included), take its lock file first.

use std::io;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::location::RepoId;

/// Directories the cache creates are `0755`, files `0644`, whatever the process umask.
#[cfg(unix)]
const DIR_MODE: u32 = 0o755;
#[cfg(unix)]
const FILE_MODE: u32 = 0o644;

/// How often a download waiting for another writer's lock on a blob tries again.
const LOCK_POLL: Duration = Duration::from_millis(250);

/// Tells apart the temporary files of this process's concurrent writes.
static TEMPORARY_FILES: AtomicU64 = AtomicU64::new(0);

/// The root of a Hub file cache.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HubCache {
    root: PathBuf,
}

/// A file in a cached snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CachedFile {
    /// The path from the repository root, `/`-separated.
    pub path: String,
    pub size: u64,
}

impl HubCache {
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// `<cache>/models--<owner>--<name>`.
    #[must_use]
    pub fn repo_dir(&self, repo: &RepoId) -> PathBuf {
        self.root.join(repo_folder_name(repo))
    }

    /// `<cache>/<repo>/snapshots/<commit>`.
    #[must_use]
    pub fn snapshot_dir(&self, repo: &RepoId, commit: &str) -> PathBuf {
        self.repo_dir(repo).join("snapshots").join(commit)
    }

    /// The commit `revision` last resolved to, as the cache recorded it; a commit names
    /// itself.
    ///
    /// # Errors
    ///
    /// Returns an error if the ref exists but cannot be read.
    pub async fn read_ref(&self, repo: &RepoId, revision: &str) -> io::Result<Option<String>> {
        if crate::client::is_commit_sha(revision) {
            return Ok(Some(revision.to_string()));
        }
        let Some(relative) = relative_path(revision) else {
            return Ok(None);
        };
        match tokio::fs::read_to_string(self.repo_dir(repo).join("refs").join(relative)).await {
            Ok(commit) => {
                let commit = commit.trim();
                Ok(crate::client::is_commit_sha(commit).then(|| commit.to_string()))
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// Records that `revision` resolved to `commit`. A commit is not recorded: it names
    /// itself.
    ///
    /// # Errors
    ///
    /// Returns an error if the ref cannot be written.
    pub async fn write_ref(&self, repo: &RepoId, revision: &str, commit: &str) -> io::Result<()> {
        if revision == commit {
            return Ok(());
        }
        let Some(relative) = relative_path(revision) else {
            return Ok(());
        };
        let path = self.repo_dir(repo).join("refs").join(relative);
        if tokio::fs::read_to_string(&path)
            .await
            .is_ok_and(|current| current.trim() == commit)
        {
            return Ok(());
        }
        write_atomically(&path, commit.as_bytes()).await
    }

    /// The files of `commit` already in the cache, with their sizes. A snapshot link whose
    /// blob is gone is skipped.
    ///
    /// # Errors
    ///
    /// Returns an error if the snapshot directory exists but cannot be read.
    pub async fn cached_files(&self, repo: &RepoId, commit: &str) -> io::Result<Vec<CachedFile>> {
        let root = self.snapshot_dir(repo, commit);
        let mut files = Vec::new();
        let mut pending = vec![root.clone()];
        while let Some(dir) = pending.pop() {
            let mut entries = match tokio::fs::read_dir(&dir).await {
                Ok(entries) => entries,
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error),
            };
            while let Some(entry) = entries.next_entry().await? {
                let path = entry.path();
                // `metadata` follows the snapshot's symlink to the blob.
                let Ok(metadata) = tokio::fs::metadata(&path).await else {
                    continue;
                };
                if metadata.is_dir() {
                    pending.push(path);
                } else if let Ok(relative) = path.strip_prefix(&root) {
                    let relative = relative
                        .components()
                        .map(|component| component.as_os_str().to_string_lossy())
                        .collect::<Vec<_>>()
                        .join("/");
                    files.push(CachedFile {
                        path: relative,
                        size: metadata.len(),
                    });
                }
            }
        }
        files.sort_by(|a, b| a.path.cmp(&b.path));
        Ok(files)
    }

    /// The cached file at `path` of `commit`, when it is in the cache and its blob is whole.
    pub(crate) async fn cached_file(
        &self,
        repo: &RepoId,
        commit: &str,
        path: &str,
        size: Option<u64>,
    ) -> Option<PathBuf> {
        let file = self.snapshot_dir(repo, commit).join(relative_path(path)?);
        let metadata = tokio::fs::metadata(&file).await.ok()?;
        (metadata.is_file() && size.is_none_or(|size| metadata.len() == size)).then_some(file)
    }

    /// `<cache>/<repo>/blobs/<etag>`.
    pub(crate) fn blob_path(&self, repo: &RepoId, etag: &str) -> PathBuf {
        self.repo_dir(repo).join("blobs").join(etag)
    }

    /// Links `path` of `commit` to the blob `etag`, replacing any link there: a relative
    /// symlink, as `huggingface_hub` makes, or a hard link or copy where symlinks are not
    /// supported.
    pub(crate) async fn link(
        &self,
        repo: &RepoId,
        commit: &str,
        path: &str,
        etag: &str,
    ) -> io::Result<PathBuf> {
        let relative = relative_path(path).ok_or_else(|| invalid_path(path))?;
        let file = self.snapshot_dir(repo, commit).join(&relative);
        if let Some(parent) = file.parent() {
            ensure_dir(parent).await?;
        }
        // `snapshots/<commit>/<path>` → `../../blobs/<etag>`, one more `..` per folder in
        // `path`.
        let depth = relative.components().count() + 1;
        let target = PathBuf::from("../".repeat(depth)).join("blobs").join(etag);
        let blob = self.blob_path(repo, etag);
        // The entry is made beside its place and renamed over it, so a concurrent repair or a
        // reader never finds the place empty.
        let temporary = temporary_sibling(&file);
        let made = match symlink(&target, &temporary).await {
            Ok(()) => Ok(()),
            Err(_) => match tokio::fs::hard_link(&blob, &temporary).await {
                Ok(()) => Ok(()),
                Err(_) => tokio::fs::copy(&blob, &temporary).await.map(|_| ()),
            },
        };
        let published = match made {
            Ok(()) => tokio::fs::rename(&temporary, &file).await,
            Err(error) => Err(error),
        };
        if published.is_err() {
            let _ = tokio::fs::remove_file(&temporary).await;
        }
        published.map(|()| file)
    }

    /// Takes the lock on writing the blob `etag`, waiting while another writer holds it.
    pub(crate) async fn lock_blob(&self, repo: &RepoId, etag: &str) -> io::Result<BlobLock> {
        let path = self
            .root
            .join(".locks")
            .join(repo_folder_name(repo))
            .join(format!("{etag}.lock"));
        if let Some(parent) = path.parent() {
            ensure_dir(parent).await?;
        }
        let mut waited = false;
        loop {
            let open_path = path.clone();
            let file = tokio::task::spawn_blocking(move || {
                std::fs::OpenOptions::new()
                    .create(true)
                    .truncate(false)
                    .write(true)
                    .open(open_path)
            })
            .await
            .map_err(io::Error::other)??;
            match file.try_lock() {
                Ok(()) => {
                    // Another writer may have removed the lock file between our open and our
                    // lock, as `huggingface_hub` does when it is done: a lock on the removed
                    // file excludes no one, so it is taken again on the file now at `path`.
                    if same_file(&file, &path).await {
                        return Ok(BlobLock { _file: file });
                    }
                }
                Err(std::fs::TryLockError::WouldBlock) => {
                    if !waited {
                        tracing::debug!(
                            "Waiting for another download of '{}' to finish before reading it",
                            path.display()
                        );
                        waited = true;
                    }
                    tokio::time::sleep(LOCK_POLL).await;
                }
                Err(std::fs::TryLockError::Error(error)) => return Err(error),
            }
        }
    }
}

/// An exclusive lock on writing one blob, released when dropped.
#[derive(Debug)]
pub(crate) struct BlobLock {
    _file: std::fs::File,
}

/// `models--<owner>--<name>`, `huggingface_hub`'s `repo_folder_name`.
#[must_use]
pub fn repo_folder_name(repo: &RepoId) -> String {
    format!(
        "{}--{}--{}",
        repo.kind().plural(),
        repo.owner(),
        repo.name()
    )
}

/// `path` as a relative path inside the cache, or `None` if it is empty, absolute, or has a
/// component that would leave its directory.
#[must_use]
pub fn relative_path(path: &str) -> Option<PathBuf> {
    if path.is_empty() || path.contains(['\\', '\0']) {
        return None;
    }
    let mut relative = PathBuf::new();
    for segment in path.split('/') {
        let component = Path::new(segment).components().next();
        let normal = matches!(component, Some(Component::Normal(name)) if name == segment);
        if !normal {
            return None;
        }
        relative.push(segment);
    }
    Some(relative)
}

/// Whether `etag` can name a blob: a git object id (40 hex digits) or a SHA-256 (64).
#[must_use]
pub fn is_valid_etag(etag: &str) -> bool {
    matches!(etag.len(), 40 | 64) && etag.bytes().all(|b| b.is_ascii_hexdigit())
}

fn invalid_path(path: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        format!("'{path}' is not a path inside the repository"),
    )
}

/// Creates `dir` and any missing parents, each `0755`.
pub(crate) async fn ensure_dir(dir: &Path) -> io::Result<()> {
    if tokio::fs::metadata(dir).await.is_ok_and(|m| m.is_dir()) {
        return Ok(());
    }
    let dir = dir.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let mut builder = std::fs::DirBuilder::new();
        builder.recursive(true);
        #[cfg(unix)]
        std::os::unix::fs::DirBuilderExt::mode(&mut builder, DIR_MODE);
        builder.create(&dir)?;
        set_mode(&dir, true)
    })
    .await
    .map_err(io::Error::other)?
}

/// Makes `path` `0644` (or `0755` for a directory), whatever the umask it was created under.
pub(crate) fn set_mode(path: &Path, dir: bool) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = if dir { DIR_MODE } else { FILE_MODE };
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
    }
    #[cfg(not(unix))]
    {
        let _ = (path, dir);
        Ok(())
    }
}

/// Writes `contents` to `path` through a temporary file renamed into place, so a reader never
/// sees part of it.
async fn write_atomically(path: &Path, contents: &[u8]) -> io::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| invalid_path(&path.display().to_string()))?;
    ensure_dir(parent).await?;
    let path = path.to_path_buf();
    let contents = contents.to_vec();
    tokio::task::spawn_blocking(move || {
        use std::io::Write;
        let temporary = temporary_sibling(&path);
        let written = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .and_then(|mut file| {
                file.write_all(&contents)?;
                file.sync_all()
            })
            .and_then(|()| set_mode(&temporary, false))
            .and_then(|()| std::fs::rename(&temporary, &path));
        if written.is_err() {
            let _ = std::fs::remove_file(&temporary);
        }
        written
    })
    .await
    .map_err(io::Error::other)?
}

/// A path beside `path` that no other write uses, in this process or another sharing the
/// cache.
fn temporary_sibling(path: &Path) -> PathBuf {
    let file_name = path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();
    let unique = TEMPORARY_FILES.fetch_add(1, Ordering::Relaxed);
    path.with_file_name(format!(".{file_name}.{}.{unique}.tmp", std::process::id()))
}

async fn symlink(target: &Path, link: &Path) -> io::Result<()> {
    let (target, link) = (target.to_path_buf(), link.to_path_buf());
    tokio::task::spawn_blocking(move || {
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(target, link)
        }
        #[cfg(windows)]
        {
            std::os::windows::fs::symlink_file(target, link)
        }
    })
    .await
    .map_err(io::Error::other)?
}

/// Whether the open `file` is the file now at `path`.
async fn same_file(file: &std::fs::File, path: &Path) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let (Ok(open), Ok(current)) = (file.metadata(), tokio::fs::metadata(path).await) else {
            return false;
        };
        open.dev() == current.dev() && open.ino() == current.ino()
    }
    #[cfg(not(unix))]
    {
        let _ = (file, path);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::location::RepoType;

    const COMMIT: &str = "1111111111111111111111111111111111111111";

    fn repo() -> RepoId {
        RepoId::new(RepoType::Model, "o", "m").expect("a valid repository")
    }

    /// Loads that record a revision at the same time each succeed, and the ref they leave
    /// holds one of their commits whole: never an empty or a torn one, which an offline load
    /// would then fail to read.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn concurrent_ref_writes_each_succeed_and_leave_a_whole_commit() {
        let dir = tempfile::tempdir().expect("a cache directory");
        let cache = HubCache::new(dir.path());
        let repo = repo();
        for round in 0..25_u32 {
            let commits: Vec<String> = (0..16_u32)
                .map(|writer| format!("{:040x}", round * 16 + writer + 1))
                .collect();
            let writes = commits.iter().map(|commit| {
                let (cache, repo, commit) = (cache.clone(), repo.clone(), commit.clone());
                tokio::spawn(async move { cache.write_ref(&repo, "main", &commit).await })
            });
            for (writer, result) in futures::future::join_all(writes)
                .await
                .into_iter()
                .enumerate()
            {
                result
                    .expect("the write ran")
                    .unwrap_or_else(|e| panic!("round {round}, writer {writer}: {e}"));
            }
            let recorded = cache.read_ref(&repo, "main").await.expect("the ref reads");
            assert!(
                recorded
                    .as_ref()
                    .is_some_and(|commit| commits.contains(commit)),
                "round {round}: the ref holds {recorded:?}"
            );
        }
    }

    /// Loads that repair the same snapshot entry at the same time each succeed, and the entry
    /// ends up as the blob.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn concurrent_repairs_of_a_snapshot_entry_each_succeed() {
        let dir = tempfile::tempdir().expect("a cache directory");
        let cache = HubCache::new(dir.path());
        let repo = repo();
        let etag = "a".repeat(64);
        let blob = cache.blob_path(&repo, &etag);
        std::fs::create_dir_all(blob.parent().expect("blobs have a folder"))
            .expect("creates the blobs folder");
        std::fs::write(&blob, b"weights").expect("writes the blob");
        let entry = cache.snapshot_dir(&repo, COMMIT).join("model.safetensors");
        std::fs::create_dir_all(entry.parent().expect("an entry has a folder"))
            .expect("creates the snapshot folder");
        for round in 0..25_u32 {
            // A stale entry: a link to a blob that is gone, which a load repairs.
            let _ = std::fs::remove_file(&entry);
            #[cfg(unix)]
            std::os::unix::fs::symlink("../../blobs/gone", &entry).expect("a stale link");
            #[cfg(windows)]
            std::fs::write(&entry, b"stale").expect("a stale entry");
            let repairs = (0..8).map(|_| {
                let (cache, repo, etag) = (cache.clone(), repo.clone(), etag.clone());
                tokio::spawn(
                    async move { cache.link(&repo, COMMIT, "model.safetensors", &etag).await },
                )
            });
            for (writer, result) in futures::future::join_all(repairs)
                .await
                .into_iter()
                .enumerate()
            {
                result
                    .expect("the repair ran")
                    .unwrap_or_else(|e| panic!("round {round}, writer {writer}: {e}"));
            }
            assert_eq!(
                std::fs::read(&entry).expect("the entry reads"),
                b"weights",
                "round {round}"
            );
        }
    }
}
