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

//! A [`LocalFileSystem`] whose writes are durable before they return.
//!
//! [`LocalFileSystem`] (`object_store` 0.13) never syncs: a put or a multipart
//! completion survives a process crash but not a power loss. This wrapper syncs
//! the written file and then its parent directory, both with
//! [`std::fs::File::sync_all`] (`F_FULLFSYNC` on macOS), after every put,
//! multipart completion, copy and rename, so a caller may reference the object
//! from a commit as soon as the call returns. Reads, lists and deletes are
//! delegated unchanged.
//!
//! Directories a write creates are synced into their own parents too: before a
//! write the store records the nearest existing ancestor of the target, and after
//! it syncs every directory from the target's parent up to that ancestor. A copy
//! or rename only syncs the destination directory: the contents were synced when
//! they were written, and a local copy is a hard link to the same inode.

use std::fmt::{self, Display, Formatter};
use std::io;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{
    CopyOptions, Error as ObjectStoreError, GetOptions, GetResult, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
    UploadPart,
};

/// A [`LocalFileSystem`] rooted at `/` whose writes are durable when they return.
#[derive(Debug)]
pub struct DurableLocalFileSystem {
    inner: Arc<LocalFileSystem>,
}

impl Default for DurableLocalFileSystem {
    fn default() -> Self {
        Self::new()
    }
}

impl DurableLocalFileSystem {
    /// A store that maps object paths to absolute filesystem paths, like
    /// [`LocalFileSystem::new`].
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(LocalFileSystem::new()),
        }
    }

    fn path(&self, location: &Path) -> Result<PathBuf, ObjectStoreError> {
        self.inner.path_to_filesystem(location)
    }
}

impl Display for DurableLocalFileSystem {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "DurableLocalFileSystem")
    }
}

fn sync_error(source: io::Error) -> ObjectStoreError {
    ObjectStoreError::Generic {
        store: "DurableLocalFileSystem",
        source: Box::new(source),
    }
}

static SYNCED_WRITES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// The nearest ancestor of `path` that exists now; directories below it will be
/// created by the write and must be synced afterwards.
fn existing_ancestor(path: &std::path::Path) -> Option<PathBuf> {
    path.ancestors()
        .skip(1)
        .find(|p| p.is_dir())
        .map(std::path::Path::to_path_buf)
}

/// Sync every directory from `path`'s parent up to and including `stop`.
fn sync_dirs_up_to(path: &std::path::Path, stop: Option<&std::path::Path>) -> io::Result<()> {
    for dir in path.ancestors().skip(1) {
        std::fs::File::open(dir)?.sync_all()?;
        if stop.is_none_or(|s| dir == s) {
            break;
        }
    }
    Ok(())
}

/// Sync only the destination directory of a copy or rename.
async fn sync_parent_dir(path: PathBuf) -> Result<(), ObjectStoreError> {
    tokio::task::spawn_blocking(move || -> io::Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::File::open(parent)?.sync_all()?;
        }
        Ok(())
    })
    .await
    .map_err(|join| sync_error(io::Error::other(join)))?
    .map_err(sync_error)
}

/// Sync `path`'s contents, then every directory the write created, up to the
/// nearest ancestor that existed before it (`stop`).
async fn sync_new_file(path: PathBuf, stop: Option<PathBuf>) -> Result<(), ObjectStoreError> {
    let n = SYNCED_WRITES.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
    if n == 1 || n.is_multiple_of(100) {
        tracing::info!(
            synced_writes = n,
            "DurableLocalFileSystem synced {n} writes"
        );
    }
    tokio::task::spawn_blocking(move || -> io::Result<()> {
        std::fs::File::open(&path)?.sync_all()?;
        sync_dirs_up_to(&path, stop.as_deref())
    })
    .await
    .map_err(|join| sync_error(io::Error::other(join)))?
    .map_err(sync_error)
}

#[derive(Debug)]
struct DurableUpload {
    inner: Box<dyn MultipartUpload>,
    path: PathBuf,
    stop: Option<PathBuf>,
}

#[async_trait]
impl MultipartUpload for DurableUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        self.inner.put_part(data)
    }

    async fn complete(&mut self) -> Result<PutResult, ObjectStoreError> {
        let result = self.inner.complete().await?;
        sync_new_file(self.path.clone(), self.stop.clone()).await?;
        Ok(result)
    }

    async fn abort(&mut self) -> Result<(), ObjectStoreError> {
        self.inner.abort().await
    }
}

#[deny(clippy::missing_trait_methods)]
#[async_trait]
impl ObjectStore for DurableLocalFileSystem {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult, ObjectStoreError> {
        let path = self.path(location)?;
        let stop = existing_ancestor(&path);
        let result = self.inner.put_opts(location, payload, opts).await?;
        sync_new_file(path, stop).await?;
        Ok(result)
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>, ObjectStoreError> {
        let path = self.path(location)?;
        let stop = existing_ancestor(&path);
        let inner = self.inner.put_multipart_opts(location, opts).await?;
        Ok(Box::new(DurableUpload { inner, path, stop }))
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> Result<GetResult, ObjectStoreError> {
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> Result<Vec<Bytes>, ObjectStoreError> {
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path, ObjectStoreError>>,
    ) -> BoxStream<'static, Result<Path, ObjectStoreError>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'static, Result<ObjectMeta, ObjectStoreError>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta, ObjectStoreError>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> Result<ListResult, ObjectStoreError> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> Result<(), ObjectStoreError> {
        self.inner.copy_opts(from, to, options).await?;
        sync_parent_dir(self.path(to)?).await
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> Result<(), ObjectStoreError> {
        self.inner.rename_opts(from, to, options).await?;
        sync_parent_dir(self.path(to)?).await
    }
}
