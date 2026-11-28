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

//! Downloads one file of a repository into the cache's blob store.
//!
//! The bytes go to `blobs/<etag>.incomplete` under the blob's lock. A download that stops part
//! way leaves that file behind, and the next one resumes after its last byte with a `Range`
//! request. The finished file must match the Hub's checksum — the SHA-256 of an LFS or Xet
//! file, the git object id of any other — before it is renamed to `blobs/<etag>`, so a blob
//! that exists is whole.
//!
//! Writing and hashing run on a blocking thread, fed through a bounded channel: hashing a
//! large model file takes seconds of CPU, which must not stall the async runtime.

use std::io::{self, Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use futures::StreamExt;
use object_store::GetRange;
use sha1::Sha1;
use sha2::{Digest, Sha256};
use snafu::prelude::*;

use crate::cache::{HubCache, file_options, is_valid_etag, set_mode};
use crate::client::{self, Hub, TreeEntry};
use crate::location::RepoId;

/// Chunks queued between the download and the thread writing it.
const WRITE_QUEUE: usize = 16;
/// The block the bytes already on disk are hashed in when a download resumes.
const HASH_BLOCK: usize = 1 << 20;

#[derive(Debug, Snafu)]
pub enum FetchError {
    #[snafu(display("{source}"))]
    Hub { source: client::Error },

    #[snafu(display("writing '{}' failed: {source}", path.display()))]
    Write { path: PathBuf, source: io::Error },

    #[snafu(display(
        "the downloaded file does not match the Hub's {algorithm} ({expected}), so it was discarded"
    ))]
    Checksum {
        algorithm: &'static str,
        expected: String,
    },

    #[snafu(display(
        "the download ended after {received} of the file's {size} bytes, so it was discarded"
    ))]
    Size { received: u64, size: u64 },

    #[snafu(display("the Hub's description of '{path}' has no valid content id"))]
    InvalidEntry { path: String },
}

impl FetchError {
    fn write(path: &Path) -> impl FnOnce(io::Error) -> Self + '_ {
        move |source| FetchError::Write {
            path: path.to_path_buf(),
            source,
        }
    }
}

/// What a finished file must hash to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Checksum {
    /// The SHA-256 of the content: an LFS or Xet file.
    Sha256(String),
    /// The git blob id, the SHA-1 of `blob <size>\0` and the content: any other file.
    GitSha1(String),
}

impl Checksum {
    /// The checksum the Hub's description of a file promises.
    #[must_use]
    pub fn of(entry: &TreeEntry) -> Self {
        match entry.lfs_oid() {
            Some(sha256) => Checksum::Sha256(sha256.to_ascii_lowercase()),
            None => Checksum::GitSha1(entry.git_oid().to_ascii_lowercase()),
        }
    }

    /// The name of the file's blob in the cache, as `huggingface_hub` names it.
    #[must_use]
    pub fn etag(&self) -> &str {
        match self {
            Checksum::Sha256(hash) | Checksum::GitSha1(hash) => hash,
        }
    }

    fn algorithm(&self) -> &'static str {
        match self {
            Checksum::Sha256(_) => "SHA-256",
            Checksum::GitSha1(_) => "git object id",
        }
    }

    fn hasher(&self, size: u64) -> Hasher {
        match self {
            Checksum::Sha256(_) => Hasher::Sha256(Sha256::new()),
            Checksum::GitSha1(_) => {
                let mut sha1 = Sha1::new();
                sha1.update(format!("blob {size}\0").as_bytes());
                Hasher::GitSha1(sha1)
            }
        }
    }
}

enum Hasher {
    Sha256(Sha256),
    GitSha1(Sha1),
}

impl Hasher {
    fn update(&mut self, bytes: &[u8]) {
        match self {
            Hasher::Sha256(hasher) => hasher.update(bytes),
            Hasher::GitSha1(hasher) => hasher.update(bytes),
        }
    }

    fn finish(self) -> String {
        match self {
            Hasher::Sha256(hasher) => hex::encode(hasher.finalize()),
            Hasher::GitSha1(hasher) => hex::encode(hasher.finalize()),
        }
    }
}

/// Bytes downloaded so far by a set of downloads, shared with whatever reports them.
#[derive(Debug, Default)]
pub struct Progress {
    received: AtomicU64,
}

impl Progress {
    #[must_use]
    pub fn received(&self) -> u64 {
        self.received.load(Ordering::Relaxed)
    }
}

/// One file's share of a [`Progress`], which it gives back when its download starts over.
struct FileProgress<'a> {
    total: &'a Progress,
    counted: u64,
}

impl FileProgress<'_> {
    fn add(&mut self, bytes: u64) {
        self.total.received.fetch_add(bytes, Ordering::Relaxed);
        self.counted += bytes;
    }

    fn restart(&mut self) {
        self.total
            .received
            .fetch_sub(self.counted, Ordering::Relaxed);
        self.counted = 0;
    }
}

/// One file to download into `incomplete`.
struct Job<'a> {
    hub: &'a Arc<Hub>,
    repo: &'a RepoId,
    commit: &'a str,
    entry: &'a TreeEntry,
    checksum: &'a Checksum,
    incomplete: &'a Path,
}

/// Downloads `entry` of `repo` at `commit` into the cache, unless its blob is already there,
/// and returns the blob's path. Resumed bytes, already on disk, are added to `progress` too.
///
/// # Errors
///
/// Returns an error if the Hub refuses or fails the download, the cache cannot be written, or
/// the downloaded bytes do not match the file's checksum.
pub async fn fetch_blob(
    hub: &Arc<Hub>,
    cache: &HubCache,
    repo: &RepoId,
    commit: &str,
    entry: &TreeEntry,
    progress: &Progress,
) -> Result<PathBuf, FetchError> {
    let checksum = Checksum::of(entry);
    let etag = checksum.etag().to_string();
    if !is_valid_etag(&etag) {
        // The Hub masks the content ids of a gated repository's files from an account without
        // access. Reading a byte of the file gets the Hub to say why.
        hub.read(repo, commit, &entry.path, Some(GetRange::Bounded(0..1)))
            .await
            .context(HubSnafu)?;
        return InvalidEntrySnafu {
            path: entry.path.clone(),
        }
        .fail();
    }
    let blob = cache.blob_path(repo, &etag);
    if is_whole(&blob, entry.size).await {
        return Ok(blob);
    }
    let blobs = blob
        .parent()
        .map_or_else(|| cache.root().to_path_buf(), Path::to_path_buf);
    cache
        .ensure_dir(&blobs)
        .await
        .map_err(FetchError::write(&blobs))?;
    let _lock = cache
        .lock_blob(repo, &etag)
        .await
        .map_err(FetchError::write(&blobs))?;
    // Another writer may have finished it while this one waited for the lock.
    if is_whole(&blob, entry.size).await {
        return Ok(blob);
    }

    let incomplete = blob.with_file_name(format!("{etag}.incomplete"));
    let job = Job {
        hub,
        repo,
        commit,
        entry,
        checksum: &checksum,
        incomplete: &incomplete,
    };
    let mut file_progress = FileProgress {
        total: progress,
        counted: 0,
    };
    match download(&job, &mut file_progress, true).await {
        Ok(()) => {}
        // A server that answered the resumed request with the whole file, or with other bytes
        // than asked for: start over from the first byte.
        Err(FetchError::Hub {
            source: client::Error::UnexpectedResponse { .. },
        }) if file_len(&incomplete).await > 0 => {
            tracing::debug!(
                "Restarting the download of '{}' of Hugging Face {} '{repo}' from its first byte: the Hub did not resume it",
                entry.path,
                repo.kind()
            );
            file_progress.restart();
            download(&job, &mut file_progress, false).await?;
        }
        Err(error) => return Err(error),
    }
    tokio::fs::rename(&incomplete, &blob)
        .await
        .map_err(FetchError::write(&blob))?;
    Ok(blob)
}

/// Downloads the job's file into its `incomplete` path, after the bytes already there when
/// `resume` is set, and checks the whole file against its checksum. A file that fails the
/// check is removed.
async fn download(
    job: &Job<'_>,
    progress: &mut FileProgress<'_>,
    resume: bool,
) -> Result<(), FetchError> {
    let Job {
        hub,
        repo,
        commit,
        entry,
        checksum,
        incomplete,
    } = *job;
    let size = entry.size;
    let (sender, mut receiver) = tokio::sync::mpsc::channel::<Bytes>(WRITE_QUEUE);
    let (start_sender, start) = tokio::sync::oneshot::channel::<u64>();
    let path = incomplete.to_path_buf();
    let mut hasher = checksum.hasher(size);
    // Opens the file, hashes what a resumed download already wrote, then appends what
    // arrives and hashes it as it goes.
    let writer = tokio::task::spawn_blocking(move || -> io::Result<(u64, String)> {
        let mut file = file_options()
            .create(true)
            .read(true)
            .append(true)
            .open(&path)?;
        let mut existing = file.metadata()?.len();
        if !resume || existing > size {
            file.set_len(0)?;
            existing = 0;
        }
        file.seek(io::SeekFrom::Start(0))?;
        let mut buffer = vec![0u8; HASH_BLOCK];
        let mut remaining = existing;
        while remaining > 0 {
            let take = usize::try_from(remaining.min(HASH_BLOCK as u64)).unwrap_or(HASH_BLOCK);
            file.read_exact(&mut buffer[..take])?;
            hasher.update(&buffer[..take]);
            remaining -= take as u64;
        }
        if start_sender.send(existing).is_err() {
            return Ok((existing, String::new()));
        }
        let mut written = existing;
        let mut file = io::BufWriter::with_capacity(HASH_BLOCK, file);
        while let Some(chunk) = receiver.blocking_recv() {
            file.write_all(&chunk)?;
            hasher.update(&chunk);
            written += chunk.len() as u64;
        }
        let file = file.into_inner().map_err(io::IntoInnerError::into_error)?;
        file.sync_all()?;
        set_mode(&path, false)?;
        Ok((written, hasher.finish()))
    });

    let streamed = async {
        let Ok(existing) = start.await else {
            return Ok(());
        };
        progress.add(existing);
        if existing == size {
            return Ok(());
        }
        let range = (existing > 0).then_some(GetRange::Offset(existing));
        let read = hub
            .read(repo, commit, &entry.path, range)
            .await
            .context(HubSnafu)?;
        let mut stream = read.stream;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.context(HubSnafu)?;
            let len = chunk.len() as u64;
            if sender.send(chunk).await.is_err() {
                // The writer stopped; its error is the one to report.
                break;
            }
            progress.add(len);
        }
        Ok(())
    }
    .await;
    drop(sender);
    let written = writer.await.map_err(|error| FetchError::Write {
        path: incomplete.to_path_buf(),
        source: io::Error::other(error),
    })?;
    streamed?;
    let (written, actual) = written.map_err(FetchError::write(incomplete))?;
    if written != size {
        // Too few bytes resume next time; too many cannot be right.
        if written > size {
            let _ = tokio::fs::remove_file(incomplete).await;
        }
        return SizeSnafu {
            received: written,
            size,
        }
        .fail();
    }
    if actual != checksum.etag() {
        let _ = tokio::fs::remove_file(incomplete).await;
        return ChecksumSnafu {
            algorithm: checksum.algorithm(),
            expected: checksum.etag().to_string(),
        }
        .fail();
    }
    Ok(())
}

/// Whether the blob at `path` is there with `size` bytes.
async fn is_whole(path: &Path, size: u64) -> bool {
    tokio::fs::metadata(path)
        .await
        .is_ok_and(|metadata| metadata.is_file() && metadata.len() == size)
}

async fn file_len(path: &Path) -> u64 {
    tokio::fs::metadata(path)
        .await
        .map_or(0, |metadata| metadata.len())
}
